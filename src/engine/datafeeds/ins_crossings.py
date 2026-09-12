"""``ins_crossings`` — the EOD job that finds the day's NEW insider net-BUY crossings (§6.1 `ins`).

Owner-directed 2026-08-17, the first evidence-first origination leg. Runs date-keyed at 19:15 IST,
AFTER ``filings_pit_fresh`` (19:00) so the day's BSE fresh disclosures are already in
``insider_trades``, and persists each crossing as an ``ins_pending`` row (migration 0008) for the NEXT
session's window-open sweep to admit through ``SignalPreScreen.admit``.

Why the work is split EOD-compute / morning-admit rather than done in one place: filings arrive after
the close, so the event is knowable tonight; but a candidate may only enter the pipeline during a
session, through the §3.2.5 dedupe/caps spine. Journalling the crossing in between is what makes the
handoff survive an engine restart, a sleeping machine or a §2.6 catch-up replay — the same reason
``mom_rebalance_state`` (0006) and the prescreen day-slot journal (0004/0005) exist.

**The crossing rule is NOT implemented here.** It comes from
:func:`engine.datafeeds.filings_events.insider_net_buy`, which wraps
:func:`engine.datafeeds.insider_crossings.insider_cluster_events` verbatim — the SAME function the
WO-16 study runs (T+10 +0.7297% / T+20 +1.5797% net). One definition; a copy would drift, and a
drifted live rule would be trading a population the evidence never measured. This module's whole job
is to feed that function the right inputs and journal what comes back.

STARVATION VISIBILITY (plan §6.1, binding)
------------------------------------------
Live crossings are computable only from the **BSE fresh feed** (live since 2026-07-19, ~13-18
in-universe rows/day); the NSE PIT feed's ~70-day content embargo makes it historical-only, so the
live-reachable event population is NOT proven identical to the backtested one. Every run therefore
logs ``ins_crossings_run`` with the day's fresh-feed row counts alongside the crossings found:
**sustained zero-rows is starvation of the feed, not absence of signal**, and the two are
indistinguishable from a "0 crossings" line alone. That was the 2026-08-04 news-corpus lesson; it is
not being re-learned here.

Rows are not the only starvation shape (2026-09-12). The feed can deliver rows every day and still
cover a handful of ISSUERS: the 09-10 run saw 22 symbols with filings across a 480-name eligible
universe (4.6%) where the NSE corpus the edge was measured on carried 1,565. ``ins_crossings_run``
therefore also carries ``coverage_pct`` (:func:`coverage_percent`), and a run under
:data:`COVERAGE_WARN_PCT` raises ``ins_feed_coverage_low`` — the row-count warning above fires only
on ZERO rows and was silent through the whole episode. ``coverage_pct`` measures the 120-day CORPUS,
not today's feed (a PIT backfill of ~70-day-old rows can widen it with the live feed unchanged), so
both lines also carry ``fresh_symbols_in_universe``: the issuers the live BSE feed reached on ``d``.

This alarm does NOT own the feed's own resolution stage. Widening the corpus lifts ``coverage_pct``
above the floor and silences this line permanently, so a later collapse of
``filings_pit_fresh``'s scrip→symbol map would be invisible here: that job raises its own
``filings_pit_fresh_scrip_map_degraded`` warning and a latched owner alert (plan §2.8.5). Keep the
two alarms distinct — this one answers "how much of the universe has filings at all", that one
answers "can we still resolve the rows we fetched".

DELIBERATE APPROXIMATION, STATED
--------------------------------
:func:`~engine.datafeeds.insider_crossings.insider_cluster_events` starts ``armed=True`` at
``sessions[0]``, so its re-arm hysteresis is path-dependent from the first session it is given. The
study runs the full 3-year history; a live EOD job cannot re-derive three years nightly for every
symbol. This job runs a :data:`LOOKBACK_DAYS`-calendar-day window instead. A divergence from the
full-history study needs the trailing sum to sit at/above the threshold across the ENTIRE window
prefix (any single session below it re-arms both computations identically) — i.e. ~4 months of
uninterrupted ≥₹1cr trailing insider buying in one symbol. Accepted, documented, and the window is
the knob if it ever bites.
"""

from __future__ import annotations

import argparse
import asyncio
import sqlite3
from collections import defaultdict
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict

from engine.core.calendar import NSECalendar
from engine.core.clock import IST, Clock
from engine.core.log import get_logger
from engine.datafeeds.filings_events import insider_net_buy, row_source
from engine.datafeeds.filings_pit_fresh import BSE_SOURCE
from engine.marketdata.store import MarketStore

_log = get_logger("engine.datafeeds.ins_crossings")

#: Calendar-day history the crossing function is given per symbol. Must comfortably exceed the
#: 10-SESSION trailing window (that part needs ~3 weeks); the rest buys margin on the re-arm
#: path-dependence documented in the module docstring.
LOOKBACK_DAYS = 120

#: ISSUER-coverage floor, in percent of the eligible universe, under which the run warns
#: ``ins_feed_coverage_low``. 10% is not a measured optimum — it is the round number that sits an
#: order of magnitude above the 09-10 reading (22/480 = 4.6%) and well below the ~1,565-issuer NSE
#: corpus the edge was measured on, so it fires on today's starvation and would stop firing only on
#: a materially wider feed. Raise it when the feed is fixed, never to quieten the line.
COVERAGE_WARN_PCT = 10.0


class InsCrossingsResult(BaseModel):
    """One run's outcome (never an exception — this job is not entry-blocking, §2.8 rule iii).

    ``ok`` is the watermark verdict (``engine.ops.jobs._job_result_ok`` reads it): False only when the
    day could not be EVALUATED at all — no universe row for ``d``, or no daily bar for ``d`` on any
    candidate symbol — so a §2.6 catch-up pass retries it. Zero crossings on a day that WAS evaluated
    is a real answer, not a failure: ``ok=True``, ``crossings_found=0``.
    """

    model_config = ConfigDict(frozen=True)

    d: date
    ok: bool
    for_session: date | None = None
    universe_symbols: int = 0
    symbols_with_filings: int = 0
    fresh_rows_today: int = 0        # BSE fresh-feed insider rows broadcast on d (whole market)
    fresh_rows_in_universe: int = 0  # ...of those, rows on an ELIGIBLE symbol (the plan's ~13-18/day)
    crossings_found: int = 0
    rows_written: int = 0
    symbols_missing_bar: int = 0
    reason: str | None = None


class InsCrossingsJob:
    """§6.1 ``ins_crossings`` — date-keyed EOD crossing detection + ``ins_pending`` journalling.

    Idempotent per day: ``ins_pending``'s ``(for_session, symbol)`` primary key means a re-run
    (catch-up replay, the periodic missed-job sweep, a manual ``--once``) upserts the same row rather
    than queueing a duplicate candidate. A re-run deliberately does NOT resurrect a row the morning
    sweep already consumed — see :meth:`_persist`.
    """

    def __init__(
        self,
        store: MarketStore,
        conn: sqlite3.Connection,
        clock: Clock,
        calendar: NSECalendar,
        *,
        threshold_inr: float | int | Decimal,
    ) -> None:
        self._store = store
        self._conn = conn
        self._clock = clock
        self._calendar = calendar
        self._threshold = Decimal(str(threshold_inr))

    async def run(self, d: date) -> InsCrossingsResult:
        """Compute ``d``'s NEW crossings and journal them for the next session. Never raises (E5)."""
        try:
            return await asyncio.to_thread(self._run_sync, d)
        except Exception as exc:  # noqa: BLE001 - E5: a filings job never takes down the scheduler
            _log.exception("ins_crossings_failed", d=d.isoformat())
            return InsCrossingsResult(d=d, ok=False, reason=f"{type(exc).__name__}: {exc}")

    # ------------------------------------------------------------------ the run, on a worker thread
    def _run_sync(self, d: date) -> InsCrossingsResult:
        window_start = d - timedelta(days=LOOKBACK_DAYS)

        # --- ELIGIBLE universe: the FULL eligible set, exactly as brk20 reads it (§6.1: `ins` is a
        #     batch rule over the whole eligible universe). A watchlist-cap exclusion is the ONE
        #     exclusion that still leaves a symbol tradeable — it means "not in today's top-N focus
        #     list", not "ineligible" — and cap symbols have no 1m bars, which is precisely why a
        #     batch rule exists at all. (2026-09-01: predicate centralized in the store method —
        #     this was an inline duplicate; `ins` deliberately stays on the ELIGIBLE set, NOT the
        #     batch-universe extended set, because its registered edge was measured on index members
        #     and its candidates must be gate-approvable.)
        eligible = set(self._store.get_universe_eligible_symbols(d))
        if not eligible:
            _log.warning("ins_crossings_no_universe", d=d.isoformat())
            return InsCrossingsResult(d=d, ok=False, reason="no universe_daily rows for the run day")

        # --- one bulk read of the filings window, grouped by symbol. A symbol with no filing inside
        #     the window cannot produce a crossing inside it (the trailing sum is 10 sessions), so
        #     this is a lossless narrowing of ~200 universe symbols down to the handful that filed.
        filings = self._store.get_insider_trades(
            broadcast_from=datetime.combine(window_start, time(0, 0), tzinfo=IST),
            broadcast_to=datetime.combine(d, time(23, 59, 59), tzinfo=IST),
        )
        by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in filings:
            symbol = str(row.get("symbol") or "").upper()
            if symbol in eligible:
                by_symbol[symbol].append(row)

        # --- feed-health telemetry: what the BSE FRESH feed delivered for d (the only live-reachable
        #     source — the NSE PIT feed's ~70-day embargo makes it historical-only, WO-16).
        fresh_today = [
            row for row in filings
            if row_source(row) == BSE_SOURCE and _broadcast_date(row) == d
        ]
        fresh_in_universe = [
            row for row in fresh_today if str(row.get("symbol") or "").upper() in eligible
        ]
        # ISSUERS the live feed reached today, apart from the ROWS it delivered. `by_symbol` below
        # counts the 120-day CORPUS, which includes the ~70-day-embargoed NSE PIT rows: a PIT
        # backfill can widen `coverage_pct` past the alarm threshold while today's live feed still
        # reaches the same handful of issuers. Logging both keeps "the corpus is wide" readable
        # apart from "today's feed reached N issuers" even when the alarm has gone quiet.
        fresh_symbols_in_universe = len({
            str(row.get("symbol") or "").upper() for row in fresh_in_universe
        })

        pending: list[dict[str, Any]] = []
        missing_bar = 0
        for symbol in sorted(by_symbol):
            bars = self._store.get_bars_1d(symbol, window_start, d)
            sessions = [b.d for b in bars]
            if not sessions or sessions[-1] != d:
                # No completed bar for the run day (bhavcopy/daily_bars gap, or a symbol that did not
                # trade): a crossing AT d is not computable and must not be invented from an older
                # session's close. Counted and logged, never silently dropped.
                missing_bar += 1
                continue
            events = insider_net_buy(by_symbol[symbol], sessions, min_value_inr=self._threshold)
            reference_close = bars[-1].close
            for event in events:
                # Only crossings that happened TODAY are new. Earlier ones in the window were
                # journalled on their own run day (or predate the leg) — re-emitting them would
                # re-originate stale events every night.
                if event["event_session"] != d:
                    continue
                pending.append(
                    {
                        "symbol": symbol,
                        "crossing_session": d,
                        "trailing_value": Decimal(str(event["trailing_value"])),
                        "contributing_filings_n": int(event["contributing_filings_n"] or 0),
                        "reference_close": reference_close,
                    }
                )

        for_session = self._calendar.next_trading_day(d)
        written = self._persist(for_session, pending)

        result = InsCrossingsResult(
            d=d,
            ok=True,
            for_session=for_session,
            universe_symbols=len(eligible),
            symbols_with_filings=len(by_symbol),
            fresh_rows_today=len(fresh_today),
            fresh_rows_in_universe=len(fresh_in_universe),
            crossings_found=len(pending),
            rows_written=written,
            symbols_missing_bar=missing_bar,
        )
        # THE starvation-visibility line (plan §6.1): fresh-feed row counts next to crossings found.
        # Zero crossings with zero fresh rows = the feed is starved. Zero crossings with healthy fresh
        # rows = a genuinely quiet day. One log line has to be able to tell those apart.
        coverage_pct = coverage_percent(len(by_symbol), len(eligible))
        _log.info(
            "ins_crossings_run",
            d=d.isoformat(),
            for_session=for_session.isoformat(),
            universe=len(eligible),
            symbols_with_filings=len(by_symbol),
            coverage_pct=coverage_pct,
            fresh_rows_today=len(fresh_today),
            fresh_rows_in_universe=len(fresh_in_universe),
            fresh_symbols_in_universe=fresh_symbols_in_universe,
            crossings=len(pending),
            written=written,
            symbols_missing_bar=missing_bar,
            threshold_inr=str(self._threshold),
        )
        if not fresh_today:
            _log.warning("ins_crossings_fresh_feed_empty", d=d.isoformat())
        # The SECOND starvation shape, invisible until 2026-09-12: the feed delivers rows every day
        # (so `fresh_feed_empty` never fires) but from a handful of ISSUERS — the 09-10 run saw 22
        # symbols across a 480-name eligible universe, 4.6%, where the NSE corpus the edge was
        # measured on carried 1,565. A rule that can only see 4.6% of the universe is not quiet, it is
        # blind, and the two are indistinguishable from a crossings=0 line.
        # KNOWN LIMIT of the threshold metric: its numerator is the 120-day corpus, so a single NSE
        # PIT backfill (rows broadcast ~70 days ago, inside the window) can lift `coverage_pct` past
        # the threshold and silence this line while the LIVE feed still reaches ~22 issuers a day.
        # `fresh_symbols_in_universe` rides along so the warning — and the run line above, which is
        # what is left once the warning goes quiet — still names today's live issuer count.
        if coverage_pct < COVERAGE_WARN_PCT:
            _log.warning(
                "ins_feed_coverage_low", d=d.isoformat(),
                symbols_with_filings=len(by_symbol), universe=len(eligible),
                fresh_symbols_in_universe=fresh_symbols_in_universe,
                coverage_pct=coverage_pct, threshold_pct=COVERAGE_WARN_PCT,
            )
        return result

    def _persist(self, for_session: date, pending: list[dict[str, Any]]) -> int:
        """Upsert the day's crossings as ``ins_pending`` rows. Returns rows written.

        The ``ON CONFLICT`` clause is what makes a same-day re-run idempotent, and it deliberately
        leaves ``consumed`` ALONE: if the morning sweep already admitted a row, a later catch-up pass
        for the same trading day must not un-consume it and let the candidate be admitted twice. The
        crossing metadata is refreshed (a late-arriving filing can legitimately raise the trailing
        value) — that is data correction, not re-origination.
        """
        if not pending:
            return 0
        now = self._clock.now().isoformat()
        rows = [
            (
                for_session.isoformat(),
                row["symbol"],
                row["crossing_session"].isoformat(),
                str(row["trailing_value"]),
                row["contributing_filings_n"],
                str(row["reference_close"]),
                now,
            )
            for row in pending
        ]
        # Bare execute on the autocommit connection (``isolation_level=None``), matching the engine's
        # other journal writers. Deliberately NOT ``core.db.transaction()``: this runs on a worker
        # thread against the connection the loop thread also uses, and an explicit BEGIN could land
        # inside one another writer already opened. Per-row commits are fine here — the (for_session,
        # symbol) key makes a partially-written batch converge on the next run rather than duplicate.
        self._conn.executemany(
            """
            INSERT INTO ins_pending (
                for_session, symbol, crossing_session, trailing_value,
                contributing_filings_n, reference_close, consumed, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, 0, ?)
            ON CONFLICT (for_session, symbol) DO UPDATE SET
                crossing_session       = excluded.crossing_session,
                trailing_value         = excluded.trailing_value,
                contributing_filings_n = excluded.contributing_filings_n,
                reference_close        = excluded.reference_close
            """,
            rows,
        )
        return len(rows)


def coverage_percent(symbols_with_filings: int, universe: int) -> float:
    """Share of the eligible universe the filings feed reached, to one decimal (0.0 on no universe).

    ONE definition, used by the run log, the warning test and the ``--once`` print: two call sites
    rounding independently would let the logged number and the alarm threshold disagree at the edge.
    """
    if universe <= 0:
        return 0.0
    return round(100.0 * symbols_with_filings / universe, 1)


def _broadcast_date(row: dict[str, Any]) -> date | None:
    """The IST calendar date of an ``insider_trades`` row's broadcast stamp (None if unstamped)."""
    bdt = row.get("broadcast_dt")
    if bdt is None:
        return None
    if isinstance(bdt, datetime):
        return (bdt.astimezone(IST) if bdt.tzinfo is not None else bdt.replace(tzinfo=IST)).date()
    return None


# --------------------------------------------------------------------------- one-shot runner (--once)
async def _run_once(d: date | None = None) -> InsCrossingsResult:
    from engine.core.config import config_dir, load_settings
    from engine.core.db import connect

    settings = load_settings()
    clock = Clock()
    store = MarketStore.from_settings(settings, clock).open()
    conn = connect(settings.sqlite_path())
    calendar = NSECalendar(config_dir() / "calendar", clock, sqlite_conn=conn)
    run_day = d or clock.today()
    try:
        return await InsCrossingsJob(
            store, conn, clock, calendar, threshold_inr=settings.ins.threshold_inr
        ).run(run_day)
    finally:
        conn.close()
        store.close()


def main(argv: list[str] | None = None) -> int:
    from engine.core.log import configure_logging

    configure_logging()
    parser = argparse.ArgumentParser(description="§6.1 ins — EOD insider net-buy crossing detection.")
    parser.add_argument("--once", action="store_true", help="run one detection pass against the live store")
    parser.add_argument(
        "--date", default=None, type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
        help="run day YYYY-MM-DD (default: today IST)",
    )
    args = parser.parse_args(argv)
    if not args.once:
        parser.error("nothing to do -- pass --once to run one detection pass")
    result = asyncio.run(_run_once(args.date))
    # ASCII only (this prints to a Windows console that may be cp1252).
    print(
        f"ins_crossings d={result.d} ok={result.ok} for_session={result.for_session} "
        f"universe={result.universe_symbols} symbols_with_filings={result.symbols_with_filings} "
        f"coverage_pct={coverage_percent(result.symbols_with_filings, result.universe_symbols)} "
        f"fresh_rows_today={result.fresh_rows_today} (in_universe={result.fresh_rows_in_universe}) "
        f"crossings={result.crossings_found} written={result.rows_written} "
        f"missing_bar={result.symbols_missing_bar}"
    )
    if result.reason:
        print(f"  reason: {result.reason}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
