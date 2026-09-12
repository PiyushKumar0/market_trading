"""Token-budget governor (D6, §5.6).

No credit-balance API exists (D6), so this ledger IS the platform's only view of LLM spend: every SDK
result message's billed usage is priced at the D4 standard rates in ``config/agents.yaml``, appended to
``budget_ledger``, and the window-to-date total drives the degrade ladder. A tier change publishes on
``budget.state`` (§3.2.1) — the dashboard, the owner alert (R8), and the D6 mode-capability mapping all
read that single signal. Reconciliation against the Anthropic console is a manual runbook step (§10.1);
drift there is expected and is why the ladder trips well below the credit.

The budget period is the SUBSCRIPTION's quota week, not the calendar month (owner-directed 2026-09-12):
the Agent SDK draws on a weekly quota that resets Thursday 14:00 IST, so a month-keyed ladder measured
the wrong thing in both directions — it let a five-week month look healthy while a single week blew the
quota, and it latched a per-agent trip for the rest of the month (news_analyst, 08-28 and 09-09, dropped
the forward cap 48→4 for three weeks). The anchor is ``llm.quota_window`` config; ``_window_key`` /
``window_bounds`` are its wall-clock arithmetic and are deliberately calendar-BLIND (a Thursday holiday
still rolls the quota — the NSE calendar only shapes the pace denominator).

Pace is measured over TRADING sessions (R6): counting calendar days would declare a perfectly healthy
week over-pace every weekend. "No burn on holidays" is emergent — no calls fire — but the denominator
must agree with that or the pace is nonsense. The two boundary Thursdays are HALF sessions: the morning
is billed to the window ending at 14:00, the afternoon to the one starting there.

Pay-as-you-go overflow (``llm.overflow_enabled``) is off by default (D6) and is NOT a lever here: DG4
trips at the credit regardless, and enabling overflow is an owner action outside this class.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Mapping
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, NamedTuple

from pydantic import BaseModel, ConfigDict, Field

from engine.core.calendar import NSECalendar
from engine.core.clock import IST, Clock
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

# §5.6 DG1 row: heartbeat 20→45 min. The BASE value is an agents.yaml knob (``heartbeat_min``); only
# the degraded target is fixed by the ladder.
DEGRADED_HEARTBEAT_MIN = 45
# §5.4/§5.6 DG2 row: news-analyst batching coarsens from the normal 30-min cadence to 60 min.
NEWS_BATCH_CADENCE_MIN = 30
DEGRADED_NEWS_BATCH_CADENCE_MIN = 60
# §5.6 DG1 row: the degraded pre-screen cap is a FRACTION of the configured base, not the old flat 4 —
# that constant was written when the base was 6 and silently became a 92% cut once the owner raised the
# base to 48 (2026-08-18). Owner-tunable as ``degrade_ladder.DG1.degraded_forward_cap_frac``.
DEFAULT_DEGRADED_FORWARD_CAP_FRAC = Decimal("0.67")

# §5.6 DG3 row: all intraday LLM calls off + news analyst off. The pre-open planner is intraday-class
# (it plans the session it can no longer afford to run); nightly/weekly keep running — they are the
# cheap-per-window agents that produce the learning the ladder exists to protect.
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

_WEEKDAY_NAMES = {
    "monday": 0, "tuesday": 1, "wednesday": 2, "thursday": 3,
    "friday": 4, "saturday": 5, "sunday": 6,
}
#: Observed subscription reset (owner 2026-09-12). Only a config file may move it; the defaults exist
#: so a config predating ``llm.quota_window`` still keys on the right week rather than on nothing.
DEFAULT_RESET_WEEKDAY = "thursday"
DEFAULT_RESET_TIME_IST = "14:00"

_HALF_SESSION = Decimal("0.5")


class WindowAnchor(NamedTuple):
    """Where the subscription's quota week starts: weekday (Mon=0, as ``date.weekday``) + IST time."""

    weekday: int
    at: time


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
        # Fail loud, never fall back: a config still carrying the retired ``monthly_credit_usd`` would
        # otherwise run the weekly ladder against a monthly number (or a default) and never trip.
        if "weekly_credit_usd" not in llm:
            raise ValueError(
                "config llm.weekly_credit_usd is missing — the governor moved to the subscription's "
                "weekly quota window on 2026-09-12 and `monthly_credit_usd` is retired; set "
                "llm.weekly_credit_usd (the governor refuses to guess a credit)"
            )
        self._credit = Decimal(str(llm["weekly_credit_usd"]))
        self._anchor = _parse_anchor(llm.get("quota_window"))
        self._pricing: dict[str, dict[str, Any]] = cfg.get("model_pricing_usd_per_mtok") or {}
        self._allocations = {
            agent: Decimal(str(usd)) for agent, usd in (cfg.get("budget_allocations_usd") or {}).items()
        }
        ladder = cfg.get("degrade_ladder") or {}
        dg1, dg2, dg3 = ladder.get("DG1") or {}, ladder.get("DG2") or {}, ladder.get("DG3") or {}
        self._dg1_pace = _pct(dg1.get("pro_rata_pct", 110))
        self._dg1_agent = _pct(dg1.get("agent_alloc_pct", 85))
        self._degraded_cap_frac = _parse_cap_frac(
            dg1.get("degraded_forward_cap_frac", DEFAULT_DEGRADED_FORWARD_CAP_FRAC)
        )
        self._dg2_pace = _pct(dg2.get("pro_rata_pct", 125))
        self._dg2_global = _pct(dg2.get("global_alloc_pct", 85))
        self._dg3_global = _pct(dg3.get("global_alloc_pct", 95))
        # DG4 is a latch on the WINDOW it was raised in, so the next Thursday 14:00 clears it without
        # owner action (§5.6: the quota itself is what refilled).
        self._billing_error_window: str | None = None
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
            # Fail loud: an unpriced model would bill as $0 and blind the ladder for the whole window.
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
        when = at if at is not None else self._clock.now()
        if when.tzinfo is None:
            # ``at`` is range-scanned as an ISO STRING (see :meth:`_window_iso`), which only sorts if
            # every row carries the same offset. A naive stamp would land outside every window.
            raise ValueError("BudgetGovernor.record: `at` must be tz-aware (budget_ledger.at is IST ISO-8601)")
        when = when.astimezone(IST)
        window = _window_key(when, self._anchor)
        cost = self.price(model, usage)
        prior = self._last_tier if self._last_tier is not None else self.degrade_tier(window)
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
                    _month_key(when),
                ),
            )
        tier = self.degrade_tier(window)
        _log.info(
            "budget_call",
            agent=agent_id,
            model=model,
            cost_usd=str(cost),
            window=window,
            tier=tier.value,
        )
        await self._publish_if_changed(prior, tier, window, when)
        return tier

    def note_billing_error(self, reason: str = "sdk_billing_error") -> None:
        """An SDK billing error trips DG4 on its own (§5.6): the ledger only ever sees calls that
        SUCCEEDED, so it can read healthy at the exact moment the quota ran out."""
        self._billing_error_window = self._window()
        _log.error("budget_billing_error", reason=reason, window=self._billing_error_window)

    async def raise_billing_error(self, reason: str = "sdk_billing_error") -> DegradeTier:
        """:meth:`note_billing_error` + publish the resulting tier change (R8 alerts on EVERY change)."""
        window = self._window()
        prior = self._last_tier if self._last_tier is not None else self.degrade_tier(window)
        self.note_billing_error(reason)
        tier = self.degrade_tier(window)
        await self._publish_if_changed(prior, tier, window, self._clock.now())
        return tier

    def credit(self) -> Decimal:
        """The WEEKLY credit ceiling (USD) — public for the owner surfaces (/budget, dashboard)."""
        return self._credit

    def allocations(self) -> dict[str, Decimal]:
        """Per-agent WEEKLY allocations (USD), a copy — public for the owner surfaces."""
        return dict(self._allocations)

    # ----------------------------------------------------------------- quota window (§5.6)
    def window_key(self, at: datetime | None = None) -> str:
        """Key of the quota window containing ``at`` (default: now) — the start Thursday's ISO date."""
        return _window_key(at if at is not None else self._clock.now(), self._anchor)

    def window_bounds(self, window: str | None = None) -> tuple[datetime, datetime]:
        """``[start, end)`` of ``window`` as aware IST datetimes — the inverse of :func:`_window_key`."""
        return _window_bounds(window or self._window(), self._anchor)

    def window_spend(self, window: str | None = None) -> Decimal:
        lo, hi = self._window_iso(window)
        return self._sum("SELECT cost_usd FROM budget_ledger WHERE at >= ? AND at < ?", (lo, hi))

    def agent_spend(self, agent_id: str, window: str | None = None) -> Decimal:
        lo, hi = self._window_iso(window)
        return self._sum(
            "SELECT cost_usd FROM budget_ledger WHERE at >= ? AND at < ? AND agent_id = ?",
            (lo, hi, agent_id),
        )

    def window_agents(self, window: str | None = None) -> list[str]:
        """Every ``agent_id`` that billed in ``window``, sorted — including ones with no allocation.

        The owner surfaces split the window total per agent off ``allocations()``; an agent that spent
        but was never budgeted (``sdk_smoke``, or a new agent added before its allocation) would then
        be invisible in the split while still counted in the headline, and the rows would not add up.
        """
        lo, hi = self._window_iso(window)
        rows = self._conn.execute(
            "SELECT DISTINCT agent_id FROM budget_ledger WHERE at >= ? AND at < ?", (lo, hi)
        ).fetchall()
        return sorted(r["agent_id"] for r in rows if r["agent_id"])

    def _window_iso(self, window: str | None) -> tuple[str, str]:
        """Half-open ISO bounds for the ``at`` range scan (idx_budget_at).

        ``at`` is written by :meth:`record` as ``Clock``-stamped IST ISO-8601, so every row carries the
        same ``+05:30`` offset and the stored form sorts lexicographically — the same convention
        ``idx_notifications_created_at`` rests on. ``>= start`` / ``< end`` therefore puts a call made
        exactly at a 14:00 reset in the NEW window, and a 13:59:59.9 call in the old one.
        """
        start, end = self.window_bounds(window)
        return start.isoformat(), end.isoformat()

    def _sum(self, sql: str, params: tuple[Any, ...]) -> Decimal:
        # Summed in Python, NOT by SQLite: SUM() on a TEXT column coerces to float and reintroduces the
        # binary-float error the decimal-as-string convention exists to keep out (§8.1).
        rows = self._conn.execute(sql, params).fetchall()
        return sum((Decimal(r["cost_usd"]) for r in rows), Decimal(0))

    # ----------------------------------------------------------------- pace (R6, trading sessions)
    def trading_sessions(self, window: str | None = None) -> tuple[Decimal, Decimal]:
        """``(elapsed_including_today, total)`` trading SESSIONS in ``window`` (R6).

        The two boundary Thursdays are half-sessions — the morning belongs to the window that ENDS at
        14:00, the afternoon to the one that starts there — so each contributes 0.5 when it is a
        trading day; the five days between contribute 1 each. Holidays contribute 0: the reset is
        wall-clock (a Thursday holiday still rolls the quota), only this denominator is calendar-aware.
        """
        start, end = self.window_bounds(window)
        first, last = start.date(), end.date()
        today = self._clock.today()
        elapsed = total = Decimal(0)
        d = first
        while d <= last:
            if self._calendar.is_trading_day(d):
                weight = _HALF_SESSION if d in (first, last) else Decimal(1)
                total += weight
                if d <= today:
                    elapsed += weight
            d += timedelta(days=1)
        return elapsed, total

    def pro_rata_to_date(self, window: str | None = None) -> Decimal | None:
        """Credit the window SHOULD have consumed by the close of today (R6), or ``None``.

        ``None`` is UNMEASURABLE — deliberately NOT ``Decimal(0)``, which reads as "nothing should
        have been spent yet" and is the same value a genuine zero pace would carry. Every consumer
        has to tell the two apart: ``spend > 1.25 × 0`` is "any spend at all", so a zero would trip
        DG2 on the window's first cent (:meth:`degrade_tier` skips both pace rungs on ``None``), and
        the owner surfaces would print ``$0.0000`` where the honest answer is "not yet measurable".

        Two states reach it:
          * ``total == 0`` — no calendar file for that year (R6: never over-pace on no data);
          * ``elapsed == 0`` — the window's sessions have not started. The reset is wall-clock, so a
            window whose reset Thursday is an NSE holiday OPENS at 14:00 on a zero-session day, and
            that evening's news sweep (19:00–22:00, not window-gated) bills before session one.
        """
        elapsed, total = self.trading_sessions(window)
        if total == 0 or elapsed == 0:
            return None
        return self._credit * elapsed / total

    # ----------------------------------------------------------------- degrade ladder (§5.6/§11.2)
    def degrade_tier(self, window: str | None = None) -> DegradeTier:
        """The PLATFORM tier for ``window`` (default: the live one).

        Per-agent spend is deliberately absent: one agent over its own share degrades ITS OWN cadence
        (:meth:`agent_degraded`) and is hard-stopped at 100% by :meth:`can_invoke`, but it no longer
        drags the whole platform down a rung (owner-directed 2026-09-12 — the 08-28/09-09 news_analyst
        trips cut the intraday forward cap for three weeks over spend that was inside the total).
        """
        w = window or self._window()
        spend = self.window_spend(w)
        if self._billing_error_window == w or spend >= self._credit:
            return DegradeTier.DG4
        if spend >= self._dg3_global * self._credit:
            return DegradeTier.DG3
        # An UNMEASURABLE pace (``None`` — see :meth:`pro_rata_to_date`) is skipped, never compared
        # against: ``spend > 1.25 × 0`` is ``any spend at all``, so the first cent of a window whose
        # reset Thursday is a holiday — or of any window with no calendar — would trip DG2 (R6:
        # never over-pace on an unmeasurable pace). The absolute rungs below still bind, so nothing
        # is weakened; the pace rungs simply wait for the first session to elapse. A non-positive
        # pace is folded into the same skip: a credit of 0 would otherwise re-open the same trap
        # through a measurable-but-zero denominator.
        pace = self.pro_rata_to_date(w)
        if pace is not None and pace <= 0:
            pace = None
        if (pace is not None and spend > self._dg2_pace * pace) or spend >= self._dg2_global * self._credit:
            return DegradeTier.DG2
        if pace is not None and spend > self._dg1_pace * pace:
            return DegradeTier.DG1
        return DegradeTier.DG0

    def agent_degraded(self, agent_id: str) -> bool:
        """``agent_id`` is over its own DG1 share of its weekly allocation (§5.6).

        The per-agent half of the old DG1 rule, decoupled from the platform tier: it throttles only the
        knobs that agent drives (its heartbeat / forward cap / batch cadence), never everyone else's.
        An agent with no allocation is never "degraded" — nothing was budgeted to exceed.
        """
        alloc = self._allocations.get(agent_id)
        if alloc is None:
            return False
        return self.agent_spend(agent_id) > self._dg1_agent * alloc

    # ----------------------------------------------------------------- call admission
    def can_invoke(self, agent_id: str, call_class: str = "schedule") -> InvokeDecision:
        """Whether ``agent_id`` may make a ``call_class`` call now (§5.6).

        The per-agent allocation is a hard window-long stop independent of the ladder: one runaway agent
        must not be able to spend the other agents' week. The reserve is owner-assignable via ``/budget``
        (§5.6) — until that lands it is simply unspent.
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
        """Intraday-analyst heartbeat cadence; ``None`` once heartbeats are off (DG2+).

        Slows to 45 min at DG1 **or** when the intraday analyst alone is over its share — the agent
        whose spend is the problem is the one whose cadence pays for it (§5.6, 2026-09-12).
        """
        tier = self.degrade_tier()
        if _TIER_RANK[tier] >= _TIER_RANK[DegradeTier.DG2]:
            return None
        if tier == DegradeTier.DG1 or self.agent_degraded("intraday_analyst"):
            return DEGRADED_HEARTBEAT_MIN
        return int(self._agent_cfg("intraday_analyst").get("heartbeat_min", 20))

    def prescreen_forward_cap(self) -> int:
        """Pre-screen candidates forwarded to the analyst per day (the scanners keep running — they are
        deterministic and free; only the forwarding costs tokens, §5.6/§3.2.5).

        Full base at DG0 with the intraday analyst inside its share; otherwise a PROPORTIONAL cut to
        ``floor(base × degraded_forward_cap_frac)``, floored at 1 so a degraded platform still forwards
        something. Proportional because the old flat 4 was written against a base of 6 and turned into a
        92% cut when the owner raised the base to 48 (2026-08-18); it ran the whole of 09-10 at 4/day.
        """
        base = int(self._agent_cfg("intraday_analyst").get("prescreen_cap_per_day", 6))
        if base <= 0:
            return base     # an owner-set 0 is "forward nothing"; the floor below must not re-open it
        if self.degrade_tier() == DegradeTier.DG0 and not self.agent_degraded("intraday_analyst"):
            return base
        return max(1, int(Decimal(base) * self._degraded_cap_frac))

    def news_batch_cadence_min(self) -> int:
        """News-analyst batch cadence: 60 min at DG2+ or when the news analyst is over its own share."""
        if _TIER_RANK[self.degrade_tier()] >= _TIER_RANK[DegradeTier.DG2] or self.agent_degraded(
            "news_analyst"
        ):
            return DEGRADED_NEWS_BATCH_CADENCE_MIN
        return NEWS_BATCH_CADENCE_MIN

    def _agent_cfg(self, agent_id: str) -> dict[str, Any]:
        return (self._cfg.get("agents") or {}).get(agent_id) or {}

    # ----------------------------------------------------------------- internals
    def _window(self) -> str:
        return _window_key(self._clock.now(), self._anchor)

    async def _publish_if_changed(
        self, old: DegradeTier, new: DegradeTier, window: str, at: datetime
    ) -> None:
        self._last_tier = new
        if new == old:
            return
        _log.warning("budget_tier_changed", old=old.value, new=new.value, window=window)
        if self._bus is not None:
            await self._bus.apublish(
                TOPIC_BUDGET_STATE,
                BudgetStateChanged(
                    old_tier=old,
                    new_tier=new,
                    window_key=window,
                    window_spend_usd=self.window_spend(window),
                    at=at,
                ),
            )


def _parse_anchor(raw: Mapping[str, Any] | None) -> WindowAnchor:
    """``llm.quota_window`` → :class:`WindowAnchor`. Fail loud — a mis-parsed anchor silently shifts
    every window boundary and the ladder measures a week that does not exist.

    ``reset_time_ist`` MUST be quoted in YAML: unquoted ``14:00`` resolves to the sexagesimal int 840
    under YAML 1.1, which this rejects rather than reading as a time.
    """
    cfg = dict(raw or {})
    name = str(cfg.get("reset_weekday", DEFAULT_RESET_WEEKDAY)).strip().lower()
    if name not in _WEEKDAY_NAMES:
        raise ValueError(f"llm.quota_window.reset_weekday {name!r} is not a weekday name")
    raw_time = cfg.get("reset_time_ist", DEFAULT_RESET_TIME_IST)
    try:
        hh, mm = (int(part) for part in str(raw_time).split(":"))
        at = time(hh, mm)
    except ValueError as exc:
        raise ValueError(
            f"llm.quota_window.reset_time_ist {raw_time!r} is not \"HH:MM\" IST (quote it in YAML)"
        ) from exc
    return WindowAnchor(_WEEKDAY_NAMES[name], at)


def _parse_cap_frac(raw: Any) -> Decimal:
    """``degrade_ladder.DG1.degraded_forward_cap_frac`` → a fraction in (0, 1].

    Fail loud like every other knob in this constructor: the neighbouring ``pro_rata_pct`` /
    ``agent_alloc_pct`` keys ARE percentages, so ``0.67`` typed as ``67`` is the likely edit — and a
    fraction > 1 would make the degrade ladder RAISE the forward cap above its DG0 base, spending more
    at every rung that exists to spend less. Nothing downstream re-checks the number.
    """
    try:
        frac = Decimal(str(raw))
    except ArithmeticError as exc:
        raise ValueError(
            f"degrade_ladder.DG1.degraded_forward_cap_frac {raw!r} is not a number"
        ) from exc
    if not Decimal(0) < frac <= Decimal(1):
        raise ValueError(
            f"degrade_ladder.DG1.degraded_forward_cap_frac {raw!r} is out of range — it is a FRACTION "
            "of the configured pre-screen cap in (0, 1], not a percentage like the pro_rata_pct / "
            "agent_alloc_pct keys beside it (0.67 keeps two thirds of the base)"
        )
    return frac


def _window_key(at: datetime, anchor: WindowAnchor) -> str:
    """ISO date of the anchor weekday whose reset time opened the quota window containing ``at``.

    Wall-clock and calendar-BLIND by design (§5.6): the subscription refills on Thursday 14:00 IST
    whether or not the NSE is open, so a holiday must not move the boundary. ``at`` is normalised to
    IST first — the key is an IST date and a UTC-stamped caller would otherwise key the wrong week.
    """
    if at.tzinfo is None:
        # The tz contract is enforced HERE, at the arithmetic, not once per caller: ``astimezone`` on a
        # naive value silently reads it as SYSTEM-local, so on any non-IST host (CI, a replay box, a
        # UTC container) the same instant would key a different week through ``window_key`` than
        # through ``record`` — which rejects it.
        raise ValueError("budget window arithmetic: `at` must be tz-aware (the quota window is IST)")
    ist = at.astimezone(IST)
    start_date = ist.date() - timedelta(days=(ist.weekday() - anchor.weekday) % 7)
    if start_date == ist.date() and ist.time() < anchor.at:
        start_date -= timedelta(days=7)     # reset-day MORNING still belongs to the window that ends
    return start_date.isoformat()


def _window_bounds(window: str, anchor: WindowAnchor) -> tuple[datetime, datetime]:
    """``[start, end)`` for a window key, as aware IST datetimes."""
    start_date = date.fromisoformat(window)
    if start_date.weekday() != anchor.weekday:
        # A key that is not on the anchor weekday came from somewhere other than ``_window_key`` and
        # would define a week no ledger row is ever assigned to — refuse instead of summing nothing.
        raise ValueError(
            f"budget window key {window!r} is a {start_date.strftime('%A')}; the quota window starts on "
            f"weekday {anchor.weekday} (Mon=0) at {anchor.at.isoformat('minutes')} IST"
        )
    return (
        datetime.combine(start_date, anchor.at, tzinfo=IST),
        datetime.combine(start_date + timedelta(days=7), anchor.at, tzinfo=IST),
    )


def _month_key(at: datetime) -> str:
    """``budget_ledger.month`` — HISTORY ONLY since 2026-09-12: still written on every row so the
    pre-weekly ledger keeps one consistent column. No reader is left — the ladder ranges on ``at``,
    ``ops/nightly_review.py`` slices ``at`` by day, and ``scripts/g2_evidence.py`` moved its C5 block
    onto the quota window on 2026-09-12 — so the column and ``idx_budget_month`` stay only as history;
    check those three before assuming a new query may key on it."""
    return at.strftime("%Y-%m")
