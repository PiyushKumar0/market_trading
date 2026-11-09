"""``ins_crossings`` — the EOD job that finds the day's NEW insider net-BUY crossings (the `ins` strategy).

Runs date-keyed at 19:15 IST, AFTER ``filings_pit_fresh`` (19:00) so the day's BSE fresh
disclosures are already in ``insider_trades``, and persists each crossing as an ``ins_pending`` row
for the NEXT session's window-open sweep to admit through ``SignalPreScreen.admit``. The split
(EOD compute, morning admit) exists because filings arrive after the close but a candidate may only
enter the pipeline during a session; the journal row makes the handoff survive a restart, a sleeping
machine or a catch-up replay.

**The crossing rule is NOT implemented here.** It comes from
:func:`engine.datafeeds.filings_events.insider_net_buy`, which wraps
:func:`engine.datafeeds.insider_crossings.insider_cluster_events` verbatim — the SAME function the
study runs. One definition; a copy would drift. This module feeds that function the right inputs and
journals what comes back.

STARVATION VISIBILITY (binding)
------------------------------------------
Live crossings come from two same-day feeds carrying the same filings: the **BSE fresh feed**
(~13-18 in-universe rows/day) and NSE's PIT V2.0 route (the old NSE route's ~70-day embargo made NSE
historical-only). :meth:`MarketStore.get_insider_trades` counts a trade both feeds carry once. Every
run logs ``ins_crossings_run`` with the day's fresh-feed row counts alongside the crossings found:
**sustained zero-rows is starvation of the feed, not absence of signal**, and the two are
indistinguishable from a "0 crossings" line alone.

Rows are not the only starvation shape: the feed can deliver rows every day yet cover a handful of
ISSUERS. ``ins_crossings_run`` therefore also carries ``coverage_pct`` (:func:`coverage_percent`), and
a run under :data:`COVERAGE_WARN_PCT` raises ``ins_feed_coverage_low`` (the zero-rows warning does not
fire in that case). ``coverage_pct`` measures the 120-day CORPUS, not today's feed (a backfill can
widen it with the live feeds unchanged), so both lines also carry ``fresh_symbols_in_universe``: the
issuers the live feeds reached on ``d``.

This alarm does NOT own the feed's own resolution stage: widening the corpus silences it, so a
collapse of ``filings_pit_fresh``'s scrip→symbol map would be invisible here. That job raises its own
``filings_pit_fresh_scrip_map_degraded`` warning and a latched owner alert. Keep the two alarms
distinct — this one answers "how much of the universe has filings at all", that one "can we still
resolve the rows we fetched".

DELIBERATE APPROXIMATION
------------------------
``insider_cluster_events`` starts ``armed=True`` at ``sessions[0]``, so its re-arm hysteresis is
path-dependent from the first session it is given. The study runs the full 3-year history; this job
runs a :data:`LOOKBACK_DAYS`-calendar-day window. A divergence needs the trailing sum to sit at/above
the threshold across the ENTIRE window prefix — ~4 months of uninterrupted ≥₹1cr trailing insider
buying in one symbol. Accepted; the window is the knob if it ever bites.
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
from engine.datafeeds.filings_events import insider_net_buy
from engine.marketdata.store import MarketStore

_log = get_logger("engine.datafeeds.ins_crossings")

#: Calendar-day history the crossing function is given per symbol. Must comfortably exceed the
#: 10-SESSION trailing window (~3 weeks); the rest buys margin on the re-arm path-dependence.
LOOKBACK_DAYS = 120

#: ISSUER-coverage floor, in percent of the eligible universe, under which the run warns
#: ``ins_feed_coverage_low``. A round number, not a measured optimum: well below the ~1,565-issuer
#: NSE corpus the edge was measured on. Raise it when the feed is fixed, never to quieten the line.
COVERAGE_WARN_PCT = 10.0


class InsCrossingsResult(BaseModel):
    """One run's outcome (never an exception — this job is not entry-blocking).

    ``ok`` is the watermark verdict (``engine.ops.jobs._job_result_ok``): False only when the day
    could not be EVALUATED — no universe row for ``d``, or no daily bar for ``d`` on any candidate —
    so a catch-up pass retries it. Zero crossings on an evaluated day is ``ok=True``.
    """

    model_config = ConfigDict(frozen=True)

    d: date
    ok: bool
    for_session: date | None = None
    universe_symbols: int = 0
    symbols_with_filings: int = 0
    fresh_rows_today: int = 0        # insider rows broadcast on d (both live feeds, constituents)
    fresh_rows_in_universe: int = 0  # ...of those, rows on an ELIGIBLE symbol (~13-18/day expected)
    crossings_found: int = 0
    rows_written: int = 0
    symbols_missing_bar: int = 0
    reason: str | None = None


class InsCrossingsJob:
    """``ins_crossings`` — date-keyed EOD crossing detection + ``ins_pending`` journalling.

    Idempotent per day: ``ins_pending``'s ``(for_session, symbol)`` key makes a re-run upsert the
    same row. A re-run does NOT resurrect a consumed row — see :meth:`_persist`.
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
        """Compute ``d``'s NEW crossings and journal them for the next session. Never raises."""
        try:
            return await asyncio.to_thread(self._run_sync, d)
        except Exception as exc:  # noqa: BLE001 - a filings job never takes down the scheduler
            _log.exception("ins_crossings_failed", d=d.isoformat())
            return InsCrossingsResult(d=d, ok=False, reason=f"{type(exc).__name__}: {exc}")

    # ------------------------------------------------------------------ the run, on a worker thread
    def _run_sync(self, d: date) -> InsCrossingsResult:
        window_start = d - timedelta(days=LOOKBACK_DAYS)

        # --- CORE-eligible universe (owner decision): the eligible set brk20 scans, restricted to core
        #     (NIFTY 500) members via get_core_eligible_symbols (`ins` is a batch rule). A watchlist-cap
        #     exclusion still leaves a symbol tradeable (cap symbols have no 1m bars, which is why a
        #     batch rule exists). `ins` stays on the ELIGIBLE set, NOT the batch-universe extended set:
        #     its edge was measured on index members, never microcaps, and candidates must be
        #     gate-approvable.
        eligible = set(self._store.get_core_eligible_symbols(d))
        if not eligible:
            _log.warning("ins_crossings_no_universe", d=d.isoformat())
            return InsCrossingsResult(d=d, ok=False, reason="no core-eligible universe rows for the run day (no build, or core list unavailable)")

        # --- one bulk read of the filings window, grouped by symbol; a symbol with no filing in the
        #     window cannot cross, so narrowing to those that filed is lossless.
        filings = self._store.get_insider_trades(
            broadcast_from=datetime.combine(window_start, time(0, 0), tzinfo=IST),
            broadcast_to=datetime.combine(d, time(23, 59, 59), tzinfo=IST),
        )
        by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in filings:
            symbol = str(row.get("symbol") or "").upper()
            if symbol in eligible:
                by_symbol[symbol].append(row)

        # --- feed-health telemetry: what the live (same-day) feeds delivered for d.
        fresh_today = [row for row in filings if _broadcast_date(row) == d]
        fresh_in_universe = [
            row for row in fresh_today if str(row.get("symbol") or "").upper() in eligible
        ]
        # ISSUERS reached today, apart from ROWS: `by_symbol` counts the 120-day CORPUS, which a
        # backfill can widen while today's feeds still reach the same few issuers.
        fresh_symbols_in_universe = len({
            str(row.get("symbol") or "").upper() for row in fresh_in_universe
        })

        pending: list[dict[str, Any]] = []
        missing_bar = 0
        for symbol in sorted(by_symbol):
            bars = self._store.get_bars_1d(symbol, window_start, d)
            sessions = [b.d for b in bars]
            if not sessions or sessions[-1] != d:
                # No completed bar for d: a crossing AT d must not be invented from an older
                # session's close. Counted, never silently dropped.
                missing_bar += 1
                continue
            events = insider_net_buy(by_symbol[symbol], sessions, min_value_inr=self._threshold)
            reference_close = bars[-1].close
            for event in events:
                # Only TODAY's crossings are new; earlier ones were journalled on their own run day.
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
        # Starvation-visibility line: zero crossings with zero fresh rows = starved feed; with healthy
        # rows = a quiet day.
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
        # Second starvation shape: rows every day (so `fresh_feed_empty` never fires) from a handful
        # of ISSUERS — blind, not quiet. KNOWN LIMIT: the numerator is the 120-day corpus, so an NSE
        # PIT backfill can lift `coverage_pct` past the threshold and silence this while the live
        # feed stays narrow; `fresh_symbols_in_universe` rides along to name today's issuer count.
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

        ``ON CONFLICT`` makes a same-day re-run idempotent and deliberately leaves ``consumed`` ALONE:
        a catch-up pass must not un-consume a row the morning sweep admitted. Crossing metadata is
        refreshed (a late filing can raise the trailing value) — data correction, not re-origination.
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
        # Bare execute on the autocommit connection, NOT ``core.db.transaction()``: this runs on a
        # worker thread against a connection the loop thread also uses, and an explicit BEGIN could
        # land inside another writer's. Per-row commits are fine: the key makes a partial batch converge.
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

    ONE definition for the run log, the warning and the ``--once`` print, so the logged number and the
    alarm threshold cannot disagree at the edge.
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
    # Same calendar as the engine (include_special=False): for_session never names a special session.
    calendar = NSECalendar(config_dir() / "calendar", clock, sqlite_conn=conn, include_special=False)
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
    parser = argparse.ArgumentParser(description="ins — EOD insider net-buy crossing detection.")
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
