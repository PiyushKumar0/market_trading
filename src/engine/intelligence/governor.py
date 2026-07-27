"""Token-budget governor (D6, §5.6).

No credit-balance API exists (D6), so this ledger IS the platform's only view of LLM spend: every SDK
result message's billed usage is priced at the D4 standard rates in ``config/agents.yaml``, appended to
``budget_ledger``, and the month-to-date total drives the degrade ladder. A tier change publishes on
``budget.state`` (§3.2.1) — the dashboard, the owner alert (R8), and the D6 mode-capability mapping all
read that single signal. Monthly reconciliation against the Anthropic console is a manual runbook step
(§10.1); drift there is expected and is why the ladder trips well below the credit.

Pace is measured over TRADING days (R6): $100 over a 21-trading-day month is $4.76/day, and counting
calendar days would declare a perfectly healthy month over-pace every weekend. "No burn on holidays" is
emergent — no calls fire — but the denominator must agree with that or the pace is nonsense.

Pay-as-you-go overflow (``llm.overflow_enabled``) is off by default (D6) and is NOT a lever here: DG4
trips at the credit regardless, and enabling overflow is an owner action outside this class.
"""

from __future__ import annotations

import sqlite3
from calendar import monthrange
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from engine.core.calendar import NSECalendar
from engine.core.clock import Clock
from engine.core.config import config_dir, load_yaml
from engine.core.db import transaction
from engine.core.enums import DegradeTier
from engine.core.eventbus import EventBus
from engine.core.log import get_logger
from engine.intelligence.events import TOPIC_BUDGET_STATE, BudgetStateChanged

_log = get_logger("engine.intelligence.governor")

_MICRO = Decimal(1_000_000)

# §11.2 cache pricing (D8): read = 0.1× the input rate, write = 1.25× the input rate. Both are
# multipliers ON THE INPUT RATE, never separate table entries — agents.yaml lists input/output only.
CACHE_READ_MULT = Decimal("0.1")
CACHE_WRITE_MULT = Decimal("1.25")

# §5.6 DG1 row: heartbeat 20→45 min, pre-screen cap 6→4/day. The BASE values are agents.yaml knobs
# (``heartbeat_min`` / ``prescreen_cap_per_day``); only the degraded targets are fixed by the ladder.
DEGRADED_HEARTBEAT_MIN = 45
DEGRADED_PRESCREEN_CAP = 4
# §5.4/§5.6 DG2 row: news-analyst batching coarsens from the normal 30-min cadence to 60 min.
NEWS_BATCH_CADENCE_MIN = 30
DEGRADED_NEWS_BATCH_CADENCE_MIN = 60

# §5.6 DG3 row: all intraday LLM calls off + news analyst off. The pre-open planner is intraday-class
# (it plans the session it can no longer afford to run); nightly/weekly keep running — they are the
# cheap-per-month agents that produce the learning the ladder exists to protect.
DG3_BLOCKED_AGENTS = frozenset({"intraday_analyst", "news_analyst", "preopen_planner"})

# Explicit rank: the ladder is ordered, and StrEnum's lexicographic comparison happening to agree is
# not something a consumer predicate should rest on (mirrors ``_MODE_RANK`` in engine.risk.mode).
_TIER_RANK = {
    DegradeTier.DG0: 0,
    DegradeTier.DG1: 1,
    DegradeTier.DG2: 2,
    DegradeTier.DG3: 3,
    DegradeTier.DG4: 4,
}


class TokenUsage(BaseModel):
    """Billed usage of one SDK call, as reported on its result message (D6)."""

    model_config = ConfigDict(frozen=True)

    in_tokens: int = Field(ge=0)
    out_tokens: int = Field(ge=0)
    cache_read: int = Field(default=0, ge=0)
    cache_write: int = Field(default=0, ge=0)


class InvokeDecision(BaseModel):
    """Whether an agent may make a call right now, and the tier the answer was computed under."""

    model_config = ConfigDict(frozen=True)

    allowed: bool
    tier: DegradeTier
    reason: str | None = None


def _pct(value: Any) -> Decimal:
    return Decimal(str(value)) / Decimal(100)


class BudgetGovernor:
    """Prices SDK usage into ``budget_ledger`` and derives the §5.6 degrade tier from it."""

    def __init__(
        self,
        conn: sqlite3.Connection,
        clock: Clock,
        calendar: NSECalendar,
        cfg: dict[str, Any],
        bus: EventBus | None = None,
    ) -> None:
        self._conn = conn
        self._clock = clock
        self._calendar = calendar
        self._cfg = cfg
        self._bus = bus
        llm = cfg.get("llm") or {}
        self._credit = Decimal(str(llm.get("monthly_credit_usd", 100)))
        self._pricing: dict[str, dict[str, Any]] = cfg.get("model_pricing_usd_per_mtok") or {}
        self._allocations = {
            agent: Decimal(str(usd)) for agent, usd in (cfg.get("budget_allocations_usd") or {}).items()
        }
        ladder = cfg.get("degrade_ladder") or {}
        dg1, dg2, dg3 = ladder.get("DG1") or {}, ladder.get("DG2") or {}, ladder.get("DG3") or {}
        self._dg1_pace = _pct(dg1.get("pro_rata_pct", 110))
        self._dg1_agent = _pct(dg1.get("agent_alloc_pct", 85))
        self._dg2_pace = _pct(dg2.get("pro_rata_pct", 125))
        self._dg2_global = _pct(dg2.get("global_alloc_pct", 85))
        self._dg3_global = _pct(dg3.get("global_alloc_pct", 95))
        # DG4 is a latch on the month it was raised, so a rollover clears it without owner action
        # (§5.6: "until month rollover or owner enables overflow").
        self._billing_error_month: str | None = None
        # Last tier PUBLISHED, not the tier itself: ``degrade_tier`` stays a pure function of the ledger
        # so nothing can desync it. This latch exists only so the rollover DG4→DG0 recovery still alerts.
        self._last_tier: DegradeTier | None = None

    @classmethod
    def from_config(
        cls,
        conn: sqlite3.Connection,
        clock: Clock,
        calendar: NSECalendar,
        bus: EventBus | None = None,
        cfg_dir: str | Path | None = None,
    ) -> BudgetGovernor:
        base = Path(cfg_dir) if cfg_dir is not None else config_dir()
        return cls(conn, clock, calendar, load_yaml(base / "agents.yaml"), bus=bus)

    # ----------------------------------------------------------------- pricing (D4 rates)
    def price(self, model: str, usage: TokenUsage) -> Decimal:
        """USD cost of one call. Decimal throughout — a float here silently corrupts the ledger."""
        rates = self._pricing.get(model)
        if rates is None:
            # Fail loud: an unpriced model would bill as $0 and blind the ladder for the whole month.
            raise ValueError(f"no D4 pricing for model {model!r} in agents.yaml model_pricing_usd_per_mtok")
        rate_in = Decimal(str(rates["input"]))
        rate_out = Decimal(str(rates["output"]))
        micro = (
            usage.in_tokens * rate_in
            + usage.out_tokens * rate_out
            + usage.cache_read * CACHE_READ_MULT * rate_in
            + usage.cache_write * CACHE_WRITE_MULT * rate_in
        )
        return micro / _MICRO

    # ----------------------------------------------------------------- ledger
    async def record(
        self,
        agent_id: str,
        model: str,
        usage: TokenUsage,
        at: datetime | None = None,
    ) -> DegradeTier:
        """Price + append one call, then recompute the tier; publish only if it CHANGED (R8)."""
        when = at or self._clock.now()
        month = _month_key(when)
        cost = self.price(model, usage)
        prior = self._last_tier if self._last_tier is not None else self.degrade_tier(month)
        with transaction(self._conn):
            self._conn.execute(
                """
                INSERT INTO budget_ledger
                    (agent_id, model, at, in_tokens, out_tokens, cache_read, cache_write, cost_usd, month)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    agent_id,
                    model,
                    when.isoformat(),
                    usage.in_tokens,
                    usage.out_tokens,
                    usage.cache_read,
                    usage.cache_write,
                    str(cost),          # TEXT column: decimal-as-string (§8.1), never a float
                    month,
                ),
            )
        tier = self.degrade_tier(month)
        _log.info(
            "budget_call",
            agent=agent_id,
            model=model,
            cost_usd=str(cost),
            month=month,
            tier=tier.value,
        )
        await self._publish_if_changed(prior, tier, month, when)
        return tier

    def note_billing_error(self, reason: str = "sdk_billing_error") -> None:
        """An SDK billing error trips DG4 on its own (§5.6): the ledger only ever sees calls that
        SUCCEEDED, so it can read healthy at the exact moment the credit ran out."""
        self._billing_error_month = _month_key(self._clock.now())
        _log.error("budget_billing_error", reason=reason, month=self._billing_error_month)

    async def raise_billing_error(self, reason: str = "sdk_billing_error") -> DegradeTier:
        """:meth:`note_billing_error` + publish the resulting tier change (R8 alerts on EVERY change)."""
        month = _month_key(self._clock.now())
        prior = self._last_tier if self._last_tier is not None else self.degrade_tier(month)
        self.note_billing_error(reason)
        tier = self.degrade_tier(month)
        await self._publish_if_changed(prior, tier, month, self._clock.now())
        return tier

    def month_spend(self, month: str | None = None) -> Decimal:
        return self._sum("SELECT cost_usd FROM budget_ledger WHERE month=?", (month or self._month(),))

    def agent_spend(self, agent_id: str, month: str | None = None) -> Decimal:
        return self._sum(
            "SELECT cost_usd FROM budget_ledger WHERE month=? AND agent_id=?",
            (month or self._month(), agent_id),
        )

    def _sum(self, sql: str, params: tuple[Any, ...]) -> Decimal:
        # Summed in Python, NOT by SQLite: SUM() on a TEXT column coerces to float and reintroduces the
        # binary-float error the decimal-as-string convention exists to keep out (§8.1).
        rows = self._conn.execute(sql, params).fetchall()
        return sum((Decimal(r["cost_usd"]) for r in rows), Decimal(0))

    # ----------------------------------------------------------------- pace (R6, trading days)
    def trading_days(self, month: str | None = None) -> tuple[int, int]:
        """``(elapsed_including_today, total)`` trading days in ``month`` (R6)."""
        m = month or self._month()
        year, mon = (int(part) for part in m.split("-"))
        today = self._clock.today()
        elapsed = total = 0
        for dom in range(1, monthrange(year, mon)[1] + 1):
            d = date(year, mon, dom)
            if not self._calendar.is_trading_day(d):
                continue
            total += 1
            if d <= today:
                elapsed += 1
        return elapsed, total

    def pro_rata_to_date(self, month: str | None = None) -> Decimal:
        """Credit the month SHOULD have consumed by the close of today (§11.2: $100/21 ≈ $4.76/day)."""
        elapsed, total = self.trading_days(month)
        if total == 0:
            return Decimal(0)  # no calendar for that year (R6) — pace is unmeasurable, never over-pace
        return self._credit * Decimal(elapsed) / Decimal(total)

    # ----------------------------------------------------------------- degrade ladder (§5.6/§11.2)
    def degrade_tier(self, month: str | None = None) -> DegradeTier:
        m = month or self._month()
        spend = self.month_spend(m)
        if self._billing_error_month == m or spend >= self._credit:
            return DegradeTier.DG4
        if spend >= self._dg3_global * self._credit:
            return DegradeTier.DG3
        pace = self.pro_rata_to_date(m)
        if spend > self._dg2_pace * pace or spend > self._dg2_global * self._credit:
            return DegradeTier.DG2
        if spend > self._dg1_pace * pace or self._any_agent_over_pct(m, self._dg1_agent):
            return DegradeTier.DG1
        return DegradeTier.DG0

    def _any_agent_over_pct(self, month: str, pct: Decimal) -> bool:
        return any(
            self.agent_spend(agent, month) > pct * alloc for agent, alloc in self._allocations.items()
        )

    # ----------------------------------------------------------------- call admission
    def can_invoke(self, agent_id: str, call_class: str = "schedule") -> InvokeDecision:
        """Whether ``agent_id`` may make a ``call_class`` call now (§5.6).

        The per-agent allocation is a hard month-long stop independent of the ladder: one runaway agent
        must not be able to spend the other agents' months. The reserve ($7) is owner-assignable via
        ``/budget`` (§5.6) — until that lands it is simply unspent.
        """
        tier = self.degrade_tier()
        alloc = self._allocations.get(agent_id)
        if alloc is not None and self.agent_spend(agent_id) >= alloc:
            return InvokeDecision(allowed=False, tier=tier, reason="agent_allocation_exhausted")
        if tier == DegradeTier.DG4:
            return InvokeDecision(allowed=False, tier=tier, reason="DG4_zero_sdk_calls")
        if tier == DegradeTier.DG3 and agent_id in DG3_BLOCKED_AGENTS:
            return InvokeDecision(allowed=False, tier=tier, reason="DG3_intraday_llm_off")
        if call_class == "heartbeat" and _TIER_RANK[tier] >= _TIER_RANK[DegradeTier.DG2]:
            return InvokeDecision(allowed=False, tier=tier, reason="DG2_heartbeats_off")
        return InvokeDecision(allowed=True, tier=tier)

    # ----------------------------------------------------------------- behaviour knobs (§5.6)
    def heartbeat_interval_min(self) -> int | None:
        """Intraday-analyst heartbeat cadence; ``None`` once heartbeats are off (DG2+)."""
        tier = self.degrade_tier()
        if _TIER_RANK[tier] >= _TIER_RANK[DegradeTier.DG2]:
            return None
        if tier == DegradeTier.DG1:
            return DEGRADED_HEARTBEAT_MIN
        return int(self._agent_cfg("intraday_analyst").get("heartbeat_min", 20))

    def prescreen_forward_cap(self) -> int:
        """Pre-screen candidates forwarded to the analyst per day (the scanners keep running — they are
        deterministic and free; only the forwarding costs tokens, §5.6/§3.2.5)."""
        tier = self.degrade_tier()
        if tier == DegradeTier.DG0:
            return int(self._agent_cfg("intraday_analyst").get("prescreen_cap_per_day", 6))
        return DEGRADED_PRESCREEN_CAP

    def news_batch_cadence_min(self) -> int:
        tier = self.degrade_tier()
        if _TIER_RANK[tier] >= _TIER_RANK[DegradeTier.DG2]:
            return DEGRADED_NEWS_BATCH_CADENCE_MIN
        return NEWS_BATCH_CADENCE_MIN

    def _agent_cfg(self, agent_id: str) -> dict[str, Any]:
        return (self._cfg.get("agents") or {}).get(agent_id) or {}

    # ----------------------------------------------------------------- internals
    def _month(self) -> str:
        return _month_key(self._clock.now())

    async def _publish_if_changed(
        self, old: DegradeTier, new: DegradeTier, month: str, at: datetime
    ) -> None:
        self._last_tier = new
        if new == old:
            return
        _log.warning("budget_tier_changed", old=old.value, new=new.value, month=month)
        if self._bus is not None:
            await self._bus.apublish(
                TOPIC_BUDGET_STATE,
                BudgetStateChanged(
                    old_tier=old, new_tier=new, month_spend_usd=self.month_spend(month), at=at
                ),
            )


def _month_key(at: datetime) -> str:
    """``budget_ledger.month`` key — IST month of the call (the credit resets on the billing month)."""
    return at.strftime("%Y-%m")
