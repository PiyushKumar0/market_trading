"""Pre-open Planner job (§5.3, plan lines 1155-1163) — the 08:50 daily day-plan run.

Same shape as :class:`~engine.ops.news_scoring.NewsScoringJob` (§5.4): a thin deterministic shell
that decides WHETHER to call (governor), assembles every input in Python, calls the one Tier-1
agent, and persists the result. The plan is advisory only — it never originates a symbol, a level
or a trade; deterministic scanners and the risk gate keep doing that (§5.3 catalyst_focus note).

**Planner death never blocks anything (§2.7).** The scanner path is planner-independent: a blocked
governor, a harness failure, or thin/empty upstream tables all resolve to ``run() -> False`` and no
``day_plans`` row — never an exception. The intraday context assembler already renders an absent
plan as ``"no day plan"`` (``ContextAssembler._day_plan_text``), so a missing plan degrades the
day's intraday prompts, not the engine.

Deterministic inputs, all best-effort, each degrading independently (D7 fail-to-zero):

- **movers** — top 10 by ``|1-day return|`` across the PRIOR trading day's included universe, from
  ``bars_1d`` (prior trading day vs the one before it — never ``d`` itself, which has not opened
  yet). Marked ``unavailable`` when the two prior sessions' bars can't be joined at all: that is a
  foundational-data gap (EOD backfill never ran), not a legitimate "no movers" zero.
- **digest_lines** — market + top-5 sector sentiment rows from the latest ``sentiment_agg`` run.
  ``unavailable`` only when the digest has NEVER run (``latest_sentiment_as_of() is None``) — the
  same convention :meth:`ContextAssembler._catalyst_text` already uses for the per-symbol block.
- **watchlist_lines** / **earnings_today** — rendered rows for ``d``. An empty result here is a
  real, common zero (no catalysts graded today / no results today), so it renders as "none" via
  ``ContextAssembler._section``, never "unavailable" — mirrors the existing per-symbol watchlist
  convention ("none for this symbol today") rather than treating every empty query as an outage.
- **surveillance_changes** — today's ``universe_daily`` rows with ``exclusion_reasons``.
  ``unavailable`` only when the universe build never ran for ``d`` (zero rows at all); an empty
  EXCLUSION set with rows present is a real "no changes today" zero.
- **open positions** — the ``positions`` table, ``state='OPEN'``. Zero open positions is routine
  platform state, not an outage: renders "none", never "unavailable".
- **yesterday_review_summary** — latest ``nightly_reviews.payload["summary"]``, else "none" (the
  nightly reviewer is a later Phase-2 wave; an empty table is the normal state today).

**No pre-open gap-scan (decision).** The plan row (§5.3) still lists "gap scan vs prior close" as a
context input and :meth:`ContextAssembler.for_planner` still accepts a ``gap_lines`` parameter, but
A14 established that indicative pre-open prices are auction artifacts, not traded prices — there is
no trustworthy PRE-OPEN price to gap against. The real auction grounding lives in the planner's
system prompt (``engine.intelligence.agents.preopen.SYSTEM_PROMPT``), not in a number this job could
compute. ``gap_lines`` therefore always carries one explanatory line, never a silent "none" (which
would misleadingly read as "scanned, found nothing").
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Awaitable, Callable, Mapping
from datetime import date, timedelta
from decimal import Decimal
from typing import Any

from engine.core.calendar import NSECalendar
from engine.core.clock import Clock
from engine.core.db import transaction
from engine.core.log import get_logger
from engine.intelligence.agents import preopen
from engine.intelligence.context import ContextAssembler
from engine.intelligence.governor import BudgetGovernor
from engine.intelligence.harness import AgentDef, AgentHarness
from engine.intelligence.schemas import DayPlan
from engine.marketdata.store import MarketStore

_log = get_logger("engine.ops.preopen_planner")

#: agents.yaml / governor admission key — must agree with config/agents.yaml's preopen_planner
#: trigger and with the "schedule" call_class ContextAssembler.for_planner stamps on its context.
CALL_CLASS = "schedule"

_UNAVAILABLE = "unavailable"
MOVERS_TOP_N = 10
SECTOR_TOP_N = 5

_GAP_SCAN_LINE = (
    "not computed: no trustworthy pre-open price source (A14 - indicative pre-open prices are "
    "auction artifacts, not traded prices); the auction grounding lives in the system prompt"
)

#: (severity, message) — ops idiom (matches engine.ops.lifecycle / engine.ops.post_login).
AlertCallback = Callable[[str, str], Awaitable[None]]


class PreopenPlannerJob:
    """§5.3: assemble the pre-open context, call the planner agent, persist one ``day_plans`` row.

    Parameters
    ----------
    store / conn:
        DuckDB market data (movers, sentiment, watchlist, earnings, universe) and the SQLite state
        db (``positions``, ``nightly_reviews``, ``day_plans``) respectively.
    assembler / harness / agent_defs:
        The §3.2.6 context assembler, the sole SDK caller, and the loaded ``agents.yaml`` roster.
    governor:
        §5.6 admission — checked here, up front, so a blocked planner never even builds a context.
    clock / calendar:
        The single "now"/"today" and the trading-day/session facts (§3.2 — no naive datetimes).
    notify:
        Optional owner-alert callback for a failed run (§5.3 step 3). The scanner path never waits
        on it — a `None` notify simply means the failure is logged, not additionally alerted.
    """

    def __init__(
        self,
        store: MarketStore,
        conn: sqlite3.Connection,
        assembler: ContextAssembler,
        harness: AgentHarness,
        agent_defs: Mapping[str, AgentDef],
        governor: BudgetGovernor,
        clock: Clock,
        calendar: NSECalendar,
        *,
        notify: AlertCallback | None = None,
    ) -> None:
        agent_def = agent_defs.get(preopen.AGENT_ID)
        if agent_def is None:
            raise KeyError(
                f"agents.yaml has no enabled {preopen.AGENT_ID!r} definition — the §5.3 planner "
                "job cannot be wired to a parked agent"
            )
        self._store = store
        self._conn = conn
        self._assembler = assembler
        self._harness = harness
        self._def = agent_def
        self._governor = governor
        self._clock = clock
        self._calendar = calendar
        self._notify = notify

    # ------------------------------------------------------------------ run (§5.3)
    async def run(self, d: date) -> bool:
        """Produce and persist the day plan for ``d``. ``True`` iff a ``day_plans`` row was written.

        Every failure path (governor-blocked, harness failure) returns ``False`` — never raises —
        because the planner is advisory-only and its death must never block the scanner path (§2.7).
        """
        decision = self._governor.can_invoke(preopen.AGENT_ID, CALL_CLASS)
        if not decision.allowed:
            _log.warning(
                "preopen_planner_blocked", d=d.isoformat(), tier=decision.tier.value, reason=decision.reason
            )
            return False

        actx = self._assembler.for_planner(
            d,
            movers_lines=self._movers_lines(d),
            gap_lines=[_GAP_SCAN_LINE],
            digest_lines=self._digest_lines(),
            watchlist_lines=self._watchlist_lines(d),
            earnings_today=self._earnings_lines(d),
            surveillance_changes=self._surveillance_lines(d),
            open_positions_summary=self._positions_summary(),
            yesterday_review_summary=self._yesterday_review_summary(),
            platform_health=self._platform_health_line(d),
        )
        result = await self._harness.run_single_shot(
            self._def,
            actx,
            preopen.parse_output,
            json_schema=preopen.output_json_schema(),
            call_class=CALL_CLASS,
        )
        if not result.ok:
            _log.warning(
                "preopen_planner_failed", d=d.isoformat(), reason=result.reason, call_id=result.call_id
            )
            if self._notify is not None:
                await self._notify(
                    "error",
                    f"pre-open planner failed for {d.isoformat()}: {result.reason} "
                    f"({(result.detail or '')[:200]}) call_id={result.call_id}",
                )
            return False

        plan: DayPlan = result.payload
        self._persist(d, plan)
        _log.info("preopen_planner_ran", d=d.isoformat(), call_id=result.call_id, focus=len(plan.focus))
        return True

    # ------------------------------------------------------------------ movers (§5.3 "overnight movers")
    def _movers_lines(self, d: date) -> list[str]:
        """Top-|return| movers, prior trading day vs the one before it — never ``d`` (unopened)."""
        d1 = self._prior_trading_day(d)
        d2 = self._prior_trading_day(d1) if d1 is not None else None
        if d1 is None or d2 is None:
            return [_UNAVAILABLE]
        universe = {row["symbol"] for row in self._store.get_universe_daily(d1, included_only=True)}
        if not universe:
            return [_UNAVAILABLE]
        bars_d1 = {b.symbol: b for b in self._store.get_bars_1d_for_day(d1) if b.symbol in universe}
        bars_d2 = {b.symbol: b for b in self._store.get_bars_1d_for_day(d2)}
        movers: list[tuple[str, Decimal]] = []
        for symbol, bar1 in bars_d1.items():
            bar2 = bars_d2.get(symbol)
            if bar2 is None or bar2.close == 0:
                continue
            movers.append((symbol, (bar1.close - bar2.close) / bar2.close))
        if not movers:
            return [_UNAVAILABLE]
        movers.sort(key=lambda item: abs(item[1]), reverse=True)
        return [f"{symbol} {ret:+.2%}" for symbol, ret in movers[:MOVERS_TOP_N]]

    def _prior_trading_day(self, d: date) -> date | None:
        """The trading day strictly before ``d``, or ``None`` if the calendar horizon can't supply
        one within a bounded walk (R6 — no calendar, no trading)."""
        probe = d - timedelta(days=1)
        for _ in range(400):
            if self._calendar.is_trading_day(probe):
                return probe
            probe -= timedelta(days=1)
        return None

    # ------------------------------------------------------------------ catalyst digest (§2.7 step 5)
    def _digest_lines(self) -> list[str]:
        """Market + top-5 sector sentiment rows from the latest digest run; ``unavailable`` only when
        the digest has never run (mirrors ``ContextAssembler._catalyst_text``'s convention)."""
        as_of = self._store.latest_sentiment_as_of()
        if as_of is None:
            return [_UNAVAILABLE]
        rows = self._store.get_sentiment_agg(as_of)
        lines: list[str] = []
        market = next((r for r in rows if r.get("scope") == "market"), None)
        if market is not None:
            lines.append(f"market: {market['value']:+.3f}")
        sectors = sorted(
            (r for r in rows if r.get("scope") == "sector"),
            key=lambda r: abs(r["value"]),
            reverse=True,
        )
        lines.extend(f"sector {r['scope_key']}: {r['value']:+.3f}" for r in sectors[:SECTOR_TOP_N])
        return lines or [_UNAVAILABLE]

    # ------------------------------------------------------------------ watchlist (§2.7 step 5(ii))
    def _watchlist_lines(self, d: date) -> list[str]:
        """Graded watchlist rows for ``d``: grade, event_type, levels. An empty watchlist is a real
        "no catalysts today" zero, not an outage — renders "none" via ``ContextAssembler._section``."""
        lines: list[str] = []
        for row in self._store.get_catalyst_watchlist(d):
            levels = [
                f"{key}={row[key]}"
                for key in (
                    "confirm_trigger", "invalidation",
                    "stop_band_low", "stop_band_high", "target_band_low", "target_band_high",
                )
                if row.get(key) is not None
            ]
            line = f"{row.get('symbol')} {row.get('grade')} {row.get('event_type')}"
            if levels:
                line += " (" + ", ".join(levels) + ")"
            lines.append(line)
        return lines

    # ------------------------------------------------------------------ earnings today
    def _earnings_lines(self, d: date) -> list[str]:
        """Earnings-calendar rows for ``d``. No earnings today is a real zero, not an outage."""
        return [f"{row['symbol']} ({row.get('kind', 'results')})" for row in self._store.get_earnings_calendar(d, d)]

    # ------------------------------------------------------------------ surveillance changes (A8)
    def _surveillance_lines(self, d: date) -> list[str]:
        """Today's EXCHANGE surveillance exclusions from ``universe_daily``. ``unavailable`` only
        when the universe build never ran for ``d`` (zero rows); no flagged symbols is "none".

        Only ``surveillance_*`` reasons pass this filter (2026-07-30): platform bookkeeping reasons
        — ``watchlist_cap`` is our OWN top-N liquidity cap and applies to ~150 symbols every single
        day — read to the model like a mass exchange action ("mass surveillance sweep covering most
        Nifty 100/200") and poisoned the plan's warnings.
        """
        rows = self._store.get_universe_daily(d)
        if not rows:
            return [_UNAVAILABLE]
        lines: list[str] = []
        for row in rows:
            reasons = [r for r in (row.get("exclusion_reasons") or []) if str(r).startswith("surveillance_")]
            if reasons:
                lines.append(f"{row['symbol']}: {', '.join(reasons)}")
        return lines or ["none"]

    # ------------------------------------------------------------------ open positions + overnight risk
    def _positions_summary(self) -> str:
        """Open platform/recommended/external positions — whatever the ``positions`` table carries.
        Zero open positions is routine, not an outage: "none", never "unavailable"."""
        rows = self._conn.execute(
            "SELECT symbol, side, qty, avg_entry, stop, product FROM positions WHERE state='OPEN'"
        ).fetchall()
        if not rows:
            return "none"
        return "; ".join(
            f"{r['symbol']} {r['side']} {r['qty']} @ {r['avg_entry']} "
            f"({r['product'] or '?'}, stop {r['stop']})"
            for r in rows
        )

    # ------------------------------------------------------------------ yesterday's review (§5.5)
    def _yesterday_review_summary(self) -> str:
        """Latest ``nightly_reviews.payload["summary"]`` prefixed with ITS session date; "none" when
        absent. The date label matters (2026-07-30): an unlabeled post-mortem of an already-fixed
        incident was escalated by the planner into a present-tense platform outage."""
        row = self._conn.execute(
            "SELECT d, payload FROM nightly_reviews ORDER BY d DESC LIMIT 1"
        ).fetchone()
        if row is None:
            return "none"
        try:
            payload: Any = json.loads(row["payload"])
        except (TypeError, json.JSONDecodeError):
            return "none"
        summary = payload.get("summary") if isinstance(payload, dict) else None
        if not (isinstance(summary, str) and summary.strip()):
            return "none"
        return f"[review of the {row['d']} session] {summary}"

    def _platform_health_line(self, d: date) -> str:
        """Deterministic CURRENT harness health — the only operational-status source the planner
        prompt permits (2026-07-30: without it, history became a present-tense outage claim)."""
        row = self._conn.execute(
            "SELECT ok, at FROM agent_calls WHERE agent_id='sdk_smoke' ORDER BY at DESC LIMIT 1"
        ).fetchone()
        smoke = "never-run"
        if row is not None:
            smoke = f"{'PASS' if row['ok'] else 'FAIL'} at {row['at']}"
        counts = self._conn.execute(
            "SELECT COALESCE(SUM(ok), 0), COUNT(*) FROM agent_calls WHERE at >= ?",
            (d.isoformat(),),
        ).fetchone()
        ok_n, total = int(counts[0]), int(counts[1])
        return (
            f"LLM self-test {smoke}; agent calls today: {ok_n} ok / {total - ok_n} failed. "
            "If the self-test passes, the signal pipeline is operational regardless of what any "
            "historical review text says."
        )

    # ------------------------------------------------------------------ persistence (§2.6 run-latest)
    def _persist(self, d: date, plan: DayPlan) -> None:
        """``INSERT OR REPLACE`` — a re-run for the same ``d`` replaces the row (§2.6 idempotent
        run-latest), never duplicates it (``day_plans.d`` is the primary key)."""
        with transaction(self._conn):
            self._conn.execute(
                "INSERT OR REPLACE INTO day_plans (d, payload, created_at) VALUES (?, ?, ?)",
                (d.isoformat(), plan.model_dump_json(), self._clock.now().isoformat()),
            )
