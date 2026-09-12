"""Pre-open Planner job (§5.3, plan lines 1155-1163) — the 08:50 daily day-plan run.

Same shape as :class:`~engine.ops.news_scoring.NewsScoringJob` (§5.4): a thin deterministic shell
that decides WHETHER to call (governor), assembles every input in Python, calls the one Tier-1
agent, and persists the result. The plan is advisory only — it never originates a symbol, a level
or a trade; deterministic scanners and the risk gate keep doing that (§5.3 catalyst_focus note).

**Planner death never blocks anything (§2.7).** The scanner path is planner-independent: a blocked
governor, a harness failure, or thin/empty upstream tables all resolve to a non-``RAN``
:class:`~engine.ops.jobs.AdvisoryOutcome` and no ``day_plans`` row — never an exception. The
intraday context assembler already renders an absent plan as ``"no day plan"``
(``ContextAssembler._day_plan_text``), so a missing plan degrades the day's intraday prompts, not
the engine. WO-14 (c) is why the return is a tri-state rather than the original ``bool``: the
watermark must retry a harness failure and must NOT retry a governor block.

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
  platform state, not an outage: renders "none", never "unavailable". Since WO-D2 a position the
  §3.6 holdings journal has seen UNHELD on two consecutive observation days is excluded from this
  line and rendered in its own "SOLD OUTSIDE THE LEDGER" block instead — the plan must not reason
  about the overnight risk of a position the owner already sold (eleven plans did, 08-26 → 09-11).
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
from engine.intelligence.context import ContextAssembler, sentiment_rail_note
from engine.intelligence.governor import BudgetGovernor
from engine.intelligence.harness import AgentDef, AgentHarness
from engine.intelligence.schemas import DayPlan
from engine.marketdata.store import MarketStore
from engine.ops.holdings_reconcile import (
    MISSING_SESSIONS,
    MissingHolding,
    entry_rec_id,
    missing_holdings_observations,
    positions_missing_from_holdings,
)
from engine.ops.jobs import AdvisoryOutcome
from engine.strategy.scanners import brk20

_log = get_logger("engine.ops.preopen_planner")

#: agents.yaml / governor admission key — must agree with config/agents.yaml's preopen_planner
#: trigger and with the "schedule" call_class ContextAssembler.for_planner stamps on its context.
CALL_CLASS = "schedule"

_UNAVAILABLE = "unavailable"
MOVERS_TOP_N = 10
SECTOR_TOP_N = 5

#: Heading for the WO-D2 "sold outside the ledger" block. The whole positions summary is rendered as
#: ONE prompt item by the assembler, so this line is the only thing separating the two groups inside
#: it — it must read as a header AND state that what follows is not the overnight book, because the
#: failure it was written for is a planner folding those names back into its risk paragraph (eleven
#: consecutive day plans did, 08-26 → 09-11).
#:
#: It says "contradict", not "holds none of these": this block uses the journal's WIDE predicate
#: (``held < tracked``), so a partial exit the owner never reported lands here too, and each line
#: states the quantity the broker actually showed. Claiming zero over a line that says 3 would be the
#: same class of error in the other direction — which is also why the heading points the model at the
#: per-line broker quantity as the real exposure rather than telling it these names carry none. "The
#: tracked size is fiction" is true of every line here; "there is nothing there" is true only of the
#: ones whose line says 0, and a plan that writes off three genuinely-held shares as no exposure is
#: the same understatement of overnight risk this block exists to remove.
_SOLD_OUTSIDE_HEADING = (
    "SOLD OUTSIDE THE LEDGER - the broker's holdings CONTRADICT these tracked positions, so their "
    "tracked size is fiction and they are deliberately excluded from the open positions and "
    "overnight risk above. Never treat the tracked size as exposure: the only exposure that exists "
    "on these names is the quantity the broker actually showed, stated on each line (usually zero). "
    "The one action on them is to get them reported:"
)

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
    async def run(self, d: date) -> AdvisoryOutcome:
        """Produce and persist the day plan for ``d``. ``RAN`` iff a ``day_plans`` row was written.

        Never raises — the planner is advisory-only and its death must never block the scanner path
        (§2.7). WO-14 (c) splits the two non-``RAN`` cases the old ``bool`` conflated, because they
        want opposite watermark treatment: ``BLOCKED`` (the §5.6 governor declined) is a CORRECT
        outcome that must NOT be retried — retrying spends LLM budget re-asking a governor that is
        deliberately saying no — while ``FAILED`` (harness failure) is retryable, and its retry is
        governor-gated by the very check above. The composition-root wrapper translates.
        """
        decision = self._governor.can_invoke(preopen.AGENT_ID, CALL_CLASS)
        if not decision.allowed:
            _log.warning(
                "preopen_planner_blocked", d=d.isoformat(), tier=decision.tier.value, reason=decision.reason
            )
            return AdvisoryOutcome.BLOCKED

        actx = self._assembler.for_planner(
            d,
            movers_lines=self._movers_lines(d),
            breakout_lines=self._breakout_lines(d),
            gap_lines=[_GAP_SCAN_LINE],
            digest_lines=self._digest_lines(),
            watchlist_lines=self._watchlist_lines(d),
            earnings_today=self._earnings_lines(d),
            surveillance_changes=self._surveillance_lines(d),
            open_positions_summary=self._positions_summary(d),
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
            return AdvisoryOutcome.FAILED

        plan: DayPlan = result.payload
        self._persist(d, plan)
        _log.info("preopen_planner_ran", d=d.isoformat(), call_id=result.call_id, focus=len(plan.focus))
        return AdvisoryOutcome.RAN

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

    # ------------------------------------------------------------------ brk20 (§6.1 addendum, 2026-08-04)
    def _breakout_lines(self, d: date) -> list[str]:
        """Yesterday's 20d-high daily-close breakouts over the BATCH universe (brk20 rule).

        Advisory context only (§5.3: the planner never originates) — the ACTIONABLE candidates are
        admitted by the window-open sweep through the prescreen caps. Full-universe by design:
        watchlist_cap symbols are invisible to the per-bar scanners (the BPCL 2026-08-03 miss),
        and this section is exactly where the planner learns about them. 2026-09-01 (§3.2.4
        extended-leg addendum): widened from the eligible set to the BATCH universe — the morning
        plan now also surfaces criteria-passing non-index breakouts (the JINDALSAW/WELCORP class);
        those lines are advisory-only by construction since the gate never approves non-included
        symbols. Predicate centralized in the store method (was an inline duplicate).
        """
        rows = self._store.get_universe_daily(d)
        included = {r["symbol"] for r in rows if r["included"]}
        eligible = self._store.get_batch_universe_symbols(d)
        if not eligible:
            return [_UNAVAILABLE]
        yesterday = d - timedelta(days=1)
        histories: dict[str, list[brk20.DailyRow]] = {}
        for sym in eligible:
            frame = self._store.get_bars_1d_frame(sym, d - timedelta(days=70), yesterday)
            if len(frame):
                histories[sym] = [
                    brk20.DailyRow(high=float(h), close=float(c), volume=float(v), open=float(o))
                    for h, c, v, o in zip(
                        frame["high"], frame["close"], frame["volume"], frame["open"]
                    )
                ]
        ex_map: dict[str, list[date]] = {}
        for row in self._store.get_corp_actions(
            ex_from=d, ex_to=d + timedelta(days=int(brk20.DEFAULT_PARAMS["ex_skip_days"]))
        ):
            if row.get("ex_date") is not None:
                ex_map.setdefault(row["symbol"], []).append(row["ex_date"])
        # No WO-19 veto accumulator here: this path is ADVISORY context, and the actionable run of
        # the same rule is the window-open sweep, which logs the counts. A second set from a second
        # run of the rule over the same universe would double-count the day's vetoes in the log.
        cands = brk20.sweep_daily(histories, today=d, ex_dates_by_symbol=ex_map)
        # WO-4 semantics: entry = the broken 20d-high LEVEL, stop = 2*level - close(y), so
        # close(y) is recovered as 2*entry - stop; the summary names the close and the level.
        return [
            f"{c.symbol} closed {c.raw_levels.entry * 2 - c.raw_levels.stop} above its 20d high "
            f"{c.raw_levels.entry} "
            f"(score {c.score:.2f}; watchlisted: {'yes' if c.symbol in included else 'no'})"
            for c in cands
        ] or ["none"]

    # ------------------------------------------------------------------ catalyst digest (§2.7 step 5)
    def _digest_lines(self) -> list[str]:
        """Market + top-5 sector sentiment rows from the latest digest run; ``unavailable`` only when
        the digest has never run (mirrors ``ContextAssembler._catalyst_text``'s convention).

        A railed row carries the same measured note the analyst context renders
        (:func:`sentiment_rail_note`, 2026-09-04): this job used to print the bare ``-1.000``, the
        plan called it "pinned at its floor", and every intraday verdict on 2026-09-03 cited that
        risk-off frame — from raw −9.48 across 759 clusters, i.e. neutral flow."""
        as_of = self._store.latest_sentiment_as_of()
        if as_of is None:
            return [_UNAVAILABLE]
        rows = self._store.get_sentiment_agg(as_of)
        lines: list[str] = []
        market = next((r for r in rows if r.get("scope") == "market"), None)
        if market is not None:
            lines.append(f"market: {market['value']:+.3f}{sentiment_rail_note(market)}")
        sectors = sorted(
            (r for r in rows if r.get("scope") == "sector"),
            key=lambda r: abs(r["value"]),
            reverse=True,
        )
        lines.extend(
            f"sector {r['scope_key']}: {r['value']:+.3f}{sentiment_rail_note(r)}"
            for r in sectors[:SECTOR_TOP_N]
        )
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
    def _positions_summary(self, d: date) -> str:
        """Open positions for the overnight-risk line, MINUS the ones the broker no longer holds.

        Zero open positions is routine, not an outage: "none", never "unavailable".

        WO-D2 (2026-09-12). Two positions the owner sold outside the ledger on 08-26 were still
        rendered as open — and reasoned about as overnight risk — in eleven consecutive day plans.
        A position with two consecutive short broker observations (§3.6 journal,
        :func:`~engine.ops.holdings_reconcile.positions_missing_from_holdings`) is therefore pulled
        OUT of this line and re-stated underneath it as its own block, naming the exact ``/closed``
        reply that ends the ambiguity. The planner then reasons about the book the owner actually
        holds, and the one thing it is asked to do about the others is get them reported.

        Two reads of the same journal (the set, then the per-position streak/quantities the block
        prints) on a job that runs once a day — the alternative is re-deriving the platform's
        "missing" threshold here, where it would be free to drift away from the position-event path's.
        An unreadable journal (a database that has not taken migration 0013 yet, a locked file)
        degrades to the pre-WO-D2 rendering — every open position on the risk line — because the
        planner may never raise (§2.7: its death must not block the scanner path).
        """
        rows = self._conn.execute(
            "SELECT position_id, symbol, side, qty, avg_entry, stop, product "
            "FROM positions WHERE state='OPEN'"
        ).fetchall()
        if not rows:
            return "none"
        try:
            missing = positions_missing_from_holdings(self._conn, d)
            observations = missing_holdings_observations(self._conn, d) if missing else {}
        except sqlite3.Error as exc:
            _log.warning("preopen_holdings_journal_unreadable", d=d.isoformat(),
                         error_type=type(exc).__name__, error=str(exc)[:200])
            missing, observations = set(), {}
        held = [r for r in rows if str(r["position_id"]) not in missing]
        sold = [r for r in rows if str(r["position_id"]) in missing]
        summary = "; ".join(
            f"{r['symbol']} {r['side']} {r['qty']} @ {r['avg_entry']} "
            f"({r['product'] or '?'}, stop {r['stop']})"
            for r in held
        ) or "none"
        if not sold:
            return summary
        # The assembler renders this whole string as ONE prompt item ("open positions and overnight
        # risk: ...", intelligence/context.py). Without a heading of its own the sold block is just
        # unlabelled continuation lines UNDER that label, and a planner reading it can fold them back
        # into exactly the overnight-risk reasoning WO-D2 removes them from — the per-line marker
        # alone was carrying the distinction. The heading is the separator, and it states the
        # negative ("not open risk") before the model reaches the position lines.
        return "\n".join(
            [summary, _SOLD_OUTSIDE_HEADING, *(self._sold_outside_line(r, observations) for r in sold)]
        )

    def _sold_outside_line(self, row: Any, observations: Mapping[str, MissingHolding]) -> str:
        """One "sold outside the ledger" line: what the platform tracks, what the broker showed, and
        the literal reply that settles it — the same ``entry_rec_id`` the §3.6 alert names.

        ``held_qty`` is the quantity the LATEST observation actually saw, not a hardcoded zero: a
        partial exit reads short with a non-zero holding, and the plan must not assert a number the
        journal never recorded. A position that is in the missing set but (impossibly, absent a
        concurrent write between the two reads) has no observation falls back to the threshold that
        put it there.
        """
        position_id = str(row["position_id"])
        seen = observations.get(position_id)
        sessions = seen.sessions if seen is not None else MISSING_SESSIONS
        held_qty = seen.held_qty if seen is not None else 0
        rec_id = entry_rec_id(self._conn, position_id)
        reply = (
            f"/closed {rec_id} <price>" if rec_id
            else f"/closed <rec_id> <price> (no learning-ledger row for position {position_id})"
        )
        return (
            f"{row['symbol']} {row['side']} {row['qty']} @ {row['avg_entry']} "
            f"({row['product'] or '?'}) - SOLD OUTSIDE THE LEDGER (broker holdings show {held_qty} "
            f"on {sessions} sessions): reply {reply}"
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
