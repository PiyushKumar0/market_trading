#!/usr/bin/env python
"""Full-market ``bars_1d`` history backfill from the NSE bhavcopy archive, plus corporate-action
history for the same window (owner one-off tool, worklog 2026-09-01 follow-up).

The daily bhavcopy job (``BhavcopyJob``, §4.4 job 6) only started covering the FULL market on
2026-07-13; before that, ``bars_1d`` outside the trading universe is sparse. This script fills the
gap so ``scripts/backtest_hi52.py`` can be re-run on a genuinely full-market population: it walks
every calendar date in ``[--from, --to]`` (default 2022-07-01..2026-07-12 — exactly the pre-coverage
window) and, for each trading day, fetches the day's bhavcopy from the NSE archive and persists it
through the SAME parse + persist path the live job uses.

Two archive URL formats, chosen by date (falling back to the other on a 404):

* **UDiFF** (current scheme, ``BHAVCOPY_URL_TEMPLATE`` in ``engine.datafeeds.bhavcopy``) — exists
  for 2024-07-05 onward, 404s for 2023/2022 dates.
* **LEGACY** (``.../content/historical/EQUITIES/<YYYY>/<MON>/cm<DD><MON><YYYY>bhav.csv.zip``) —
  exists up to 2024-07-05, 404s from 2024-07-08.

``_LEGACY_UDIFF_SWITCH_DATE`` (2024-07-08) decides which format is TRIED FIRST for a given date; the
other is the fallback on a 404. EVERY calendar date is attempted — NSE holds Saturday/Sunday sessions
(DR drills 2024-01-20/03-02/05-18, Muhurat 2023-11-12, Budget 2025-02-01) that a weekday filter
would silently drop, and a weekend costs only two 404s. A 404 on BOTH formats means a non-trading day
— checkpointed 'holiday', nothing written — but ``_MAX_HOLIDAY_STREAK`` consecutive such days is not
a calendar shape NSE produces (the longest closures run 4-5 days): it is an archive outage or a wrong
URL era, so the leg stops there, the streak's 'holiday' checkpoints are cleared and counted failed,
and the next run retries them. Any OTHER error fails the date (counted, no checkpoint) so the next
run retries it. Bars are persisted via ``BhavcopyJob._persist`` exactly as the live job does, so a
Kite-official row for (symbol, day) is NEVER overwritten (A11) — the archive row only fills a gap or
cross-checks (an adjusted Kite row vs a raw archive row shows up as a mismatch count, nothing more).

The corp-actions leg starts ``_CORP_ACTIONS_LOOKBACK_DAYS`` (400, the live hi52 frame window) BEFORE
``--from``: its consumer is a trailing 400-day lookback per signal date, so the earliest bars need
ex-dates from a year earlier. A window that parses to zero rows fails (no checkpoint) — NSE lists
hundreds of actions a month, so an empty window is a capped/failed response, not history.

The engine must be OFF while this runs: ``market.duckdb`` (DuckDB) has a single writer (§4.1), and
this script opens it directly (no engine IPC). ``--status`` is the one read-only exception — it opens
``state.db`` (SQLite) read-only and never touches DuckDB, so it is safe to run while the engine is live.

Resumable: each unit is checkpointed in ``state.db`` ``filings_backfill_checkpoints`` (migration
0002, shared with ``scripts/backfill_filings.py``) — ``feed='bhavcopy_archive'`` with
``unit=<ISO date>`` (``through_date`` = the same ISO date once ingested, or the literal ``'holiday'``
for a confirmed non-trading day; both count as done), and ``feed='corp_actions_archive'`` with
``unit=<from>..<to>`` per <=35-day window. A re-run skips every unit already checkpointed — re-fetching
would be safe anyway (all writes are idempotent upserts) but skipping saves ~1,000 requests on resume.

Rough runtime: ~1,000 trading sessions in the default window, at ~1-2 s each (one archive GET per
day, paced by ``--pace-s``, default 1.0 s) — call it 20-30 minutes for the bhavcopy leg alone. It is
safe to interrupt and resume across multiple evenings.

    python scripts/backfill_bhavcopy.py [--from YYYY-MM-DD] [--to YYYY-MM-DD] [--pace-s SECONDS]
                                         [--skip-corp-actions] [--skip-bhavcopy] [--config-dir DIR]
    python scripts/backfill_bhavcopy.py --status [--config-dir DIR]

Exit codes: 0 = ran clean (or --status); 1 = some dates/windows failed for non-setup reasons, or the
holiday-streak guard stopped the bars leg (re-run to resume from checkpoints); 2 = setup error
(``market.duckdb`` missing or locked, ``state.db`` unavailable, or ``--from`` after ``--to``).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sqlite3
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

_REPO_SRC = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
if _REPO_SRC not in sys.path:  # pragma: no cover - loose-script shim
    sys.path.insert(0, _REPO_SRC)

import httpx  # noqa: E402

import engine  # noqa: E402,F401  native import-order guard (sklearn before numba/vectorbt/cvxpy)
from engine.core.clock import Clock  # noqa: E402
from engine.core.config import load_settings  # noqa: E402
from engine.core.db import connect  # noqa: E402
from engine.core.log import configure_logging, get_logger  # noqa: E402
from engine.core.migrations import apply_migrations  # noqa: E402
from engine.core.nse_http import nse_get  # noqa: E402
from engine.datafeeds.bhavcopy import (  # noqa: E402
    BHAVCOPY_URL_TEMPLATE,
    BhavcopyJob,
    _unwrap_zip,
    parse_bhavcopy_csv,
)
from engine.datafeeds.corp_actions import NSE_CORP_ACTIONS_URL, parse_corp_actions  # noqa: E402
from engine.marketdata.store import MarketStore  # noqa: E402

_log = get_logger("scripts.backfill_bhavcopy")

#: state.db checkpoint feed names (filings_backfill_checkpoints, migration 0002 — shared table).
_FEED_BHAVCOPY = "bhavcopy_archive"
_FEED_CORP_ACTIONS = "corp_actions_archive"

#: UDiFF exists 2024-07-05 onward, 404s before; LEGACY exists up to 2024-07-05, 404s from 2024-07-08.
#: This switch decides which format is TRIED FIRST for a given date — the other is the 404 fallback.
_LEGACY_UDIFF_SWITCH_DATE = date(2024, 7, 8)

#: Corp-actions leg: <=35-day NSE windows (same shape as backfill_filings.py's _windows/_NSE_WINDOW_DAYS).
_CORP_ACTIONS_WINDOW_DAYS = 35
#: How far before --from the corp-actions leg starts: the consumer (hi52's unadjusted-history veto,
#: src/engine/ops/main.py hi52_start) looks back 400 calendar days from each signal date.
_CORP_ACTIONS_LOOKBACK_DAYS = 400

#: Consecutive 404-on-both dates that stop the bars leg (see the module docstring).
_MAX_HOLIDAY_STREAK = 7

#: Per-request timeouts (mirror BhavcopyJob's / CorpActionsJob's own defaults).
_BHAVCOPY_TIMEOUT_S = 30.0
_CORP_ACTIONS_TIMEOUT_S = 20.0

_DEFAULT_FROM = date(2022, 7, 1)
_DEFAULT_TO = date(2026, 7, 12)  # the day before the daily job's full-market coverage started


# --------------------------------------------------------------------------- URL selection
def _legacy_bhavcopy_url(d: date) -> str:
    mon = d.strftime("%b").upper()
    return (
        f"https://nsearchives.nseindia.com/content/historical/EQUITIES/{d.year}/{mon}/"
        f"cm{d.day:02d}{mon}{d.year}bhav.csv.zip"
    )


def _candidate_urls(d: date) -> list[str]:
    """Ordered [first, fallback] archive URLs for day ``d`` (LEGACY first before the switch date,
    UDiFF first on/after it)."""
    legacy = _legacy_bhavcopy_url(d)
    udiff = BHAVCOPY_URL_TEMPLATE.format(d=d)
    if d < _LEGACY_UDIFF_SWITCH_DATE:
        return [legacy, udiff]
    return [udiff, legacy]


# --------------------------------------------------------------------------- checkpoints (copied from
# scripts/backfill_filings.py:129-153 — private to that script, so duplicated here rather than imported)
def _windows(frm: date, to: date, span_days: int) -> list[tuple[date, date]]:
    """Ascending <=``span_days`` windows covering ``[frm, to]`` inclusive."""
    out: list[tuple[date, date]] = []
    cur = frm
    while cur <= to:
        end = min(cur + timedelta(days=span_days - 1), to)
        out.append((cur, end))
        cur = end + timedelta(days=1)
    return out


def _cp_done(conn: sqlite3.Connection, feed: str, unit: str) -> bool:
    row = conn.execute(
        "SELECT through_date FROM filings_backfill_checkpoints WHERE feed=? AND unit=?", (feed, unit)
    ).fetchone()
    return bool(row and row["through_date"])


def _cp_set(conn: sqlite3.Connection, feed: str, unit: str, through: str, now: str) -> None:
    conn.execute(
        "INSERT INTO filings_backfill_checkpoints (feed, unit, through_date, updated_at) "
        "VALUES (?,?,?,?) ON CONFLICT(feed, unit) DO UPDATE SET "
        "through_date=excluded.through_date, updated_at=excluded.updated_at",
        (feed, unit, through, now),
    )


def _cp_clear(conn: sqlite3.Connection, feed: str, units: list[str]) -> None:
    conn.executemany(
        "DELETE FROM filings_backfill_checkpoints WHERE feed=? AND unit=?", [(feed, u) for u in units]
    )


# --------------------------------------------------------------------------- bhavcopy leg
async def _try_fetch(http: httpx.AsyncClient, url: str, timeout: float) -> str:
    resp = await nse_get(http, url, timeout=timeout)
    return _unwrap_zip(resp.content)


async def _ingest_bhavcopy_date(
    conn, store, http, clock: Clock, job: BhavcopyJob, d: date, report: dict
) -> str:
    """Fetch+parse+persist one day: try the date-appropriate format first, fall back to the other on
    a 404. Returns 'ingested' | 'holiday' (404 on BOTH: checkpointed, nothing written) | 'failed'
    (any other error: counted, no checkpoint — the next run retries it)."""
    urls = _candidate_urls(d)
    try:
        text: str | None = None
        for i, url in enumerate(urls):
            try:
                text = await _try_fetch(http, url, _BHAVCOPY_TIMEOUT_S)
                break
            except httpx.HTTPStatusError as exc:
                is_404 = exc.response is not None and exc.response.status_code == 404
                if is_404 and i < len(urls) - 1:
                    continue  # try the fallback format
                raise
        bars = parse_bhavcopy_csv(text, d)
        if not bars:
            raise ValueError("bhavcopy parsed to zero EQ rows")  # a served file is never empty (E5)
        written, checked, mismatches = await store.arun(job._persist, d, bars)
    except Exception as exc:  # noqa: BLE001 - every failure is counted and left for the next run
        if isinstance(exc, httpx.HTTPStatusError) and exc.response is not None \
                and exc.response.status_code == 404:
            _cp_set(conn, _FEED_BHAVCOPY, d.isoformat(), "holiday", clock.now().isoformat())
            report["bhavcopy"]["holiday"] += 1
            return "holiday"
        report["bhavcopy"]["failed"] += 1
        report["bhavcopy"]["failed_dates"].append(d.isoformat())
        _log.warning("bhavcopy_archive_fetch_failed", d=d.isoformat(), urls=urls,
                     error=f"{type(exc).__name__}: {exc}")
        return "failed"

    _cp_set(conn, _FEED_BHAVCOPY, d.isoformat(), d.isoformat(), clock.now().isoformat())
    report["bhavcopy"]["ingested"] += 1
    report["bhavcopy"]["rows_written"] += written
    report["bhavcopy"]["rows_cross_checked"] += checked
    report["bhavcopy"]["rows_mismatched"] += len(mismatches)
    return "ingested"


async def _run_bhavcopy_leg(conn, store, http, clock: Clock, job: BhavcopyJob, frm: date, to: date,
                             pace_s: float, report: dict) -> None:
    """Ascending loop over EVERY calendar date (see the module docstring for why weekends are not
    skipped). Stops at ``_MAX_HOLIDAY_STREAK`` consecutive holidays: the streak's checkpoints are
    cleared and re-counted as failed so the next run retries them."""
    d = frm
    one_day = timedelta(days=1)
    streak: list[str] = []
    while d <= to:
        unit = d.isoformat()
        if _cp_done(conn, _FEED_BHAVCOPY, unit):
            report["bhavcopy"]["skipped"] += 1
            streak = []
        else:
            report["bhavcopy"]["attempted"] += 1
            outcome = await _ingest_bhavcopy_date(conn, store, http, clock, job, d, report)
            streak = streak + [unit] if outcome == "holiday" else []
            if len(streak) >= _MAX_HOLIDAY_STREAK:
                _cp_clear(conn, _FEED_BHAVCOPY, streak)
                report["bhavcopy"]["holiday"] -= len(streak)
                report["bhavcopy"]["failed"] += len(streak)
                report["bhavcopy"]["failed_dates"].extend(streak)
                report["bhavcopy"]["aborted_at"] = unit
                _log.error("bhavcopy_archive_holiday_streak", first=streak[0], last=unit,
                           streak=len(streak))
                return
            await asyncio.sleep(pace_s)
        d += one_day


# --------------------------------------------------------------------------- corp-actions leg
def _corp_actions_window_url(frm: date, to: date) -> str:
    return f"{NSE_CORP_ACTIONS_URL}&from_date={frm:%d-%m-%Y}&to_date={to:%d-%m-%Y}"


async def _run_corp_actions_leg(conn, store, http, clock: Clock, frm: date, to: date,
                                 pace_s: float, report: dict) -> None:
    """<=35-day NSE windows (checkpoint per window); persisted exactly as CorpActionsJob.run does."""
    for w_frm, w_to in _windows(frm, to, _CORP_ACTIONS_WINDOW_DAYS):
        unit = f"{w_frm.isoformat()}..{w_to.isoformat()}"
        if _cp_done(conn, _FEED_CORP_ACTIONS, unit):
            report["corp_actions"]["skipped"] += 1
            continue
        try:
            resp = await nse_get(http, _corp_actions_window_url(w_frm, w_to), timeout=_CORP_ACTIONS_TIMEOUT_S)
            rows = parse_corp_actions(json.loads(resp.content))
            if not rows:
                raise ValueError("corp-actions window parsed to zero rows")  # capped/failed, not history
            now = clock.now()
            stamped = [{**row, "recorded_at": now} for row in rows]
            written = await store.arun(store.upsert_corp_actions, stamped)
        except Exception as exc:  # noqa: BLE001 - record + continue; the checkpoint stays open to retry
            report["corp_actions"]["failed"] += 1
            report["corp_actions"]["failed_windows"].append(unit)
            _log.warning("corp_actions_archive_window_failed", unit=unit, error=f"{type(exc).__name__}: {exc}")
            await asyncio.sleep(pace_s)
            continue
        _cp_set(conn, _FEED_CORP_ACTIONS, unit, w_to.isoformat(), clock.now().isoformat())
        report["corp_actions"]["windows"] += 1
        report["corp_actions"]["rows_written"] += written
        report["corp_actions"]["rows_by_window"][unit] = len(rows)
        await asyncio.sleep(pace_s)


# --------------------------------------------------------------------------- report + status
def _new_report() -> dict:
    return {
        "bhavcopy": {
            "attempted": 0, "ingested": 0, "holiday": 0, "skipped": 0, "failed": 0,
            "rows_written": 0, "rows_cross_checked": 0, "rows_mismatched": 0,
            "failed_dates": [], "aborted_at": None,
        },
        "corp_actions": {
            "windows": 0, "skipped": 0, "failed": 0, "rows_written": 0,
            "failed_windows": [], "rows_by_window": {},
        },
    }


def _write_report(settings, args, report: dict) -> Path:
    out = settings.resolved_data_dir() / "reports" / "bhavcopy_archive_backfill.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(
        json.dumps(
            {
                "from": args.start.isoformat(),
                "to": args.end.isoformat(),
                "skip_corp_actions": args.skip_corp_actions,
                "pace_s": args.pace_s,
                "report": report,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return out


def _print_summary(report: dict, path: Path) -> None:
    b = report["bhavcopy"]
    c = report["corp_actions"]
    print("\n=== bhavcopy_archive backfill summary ===")
    print(
        f"  [bhavcopy] attempted={b['attempted']} ingested={b['ingested']} holiday={b['holiday']} "
        f"skipped={b['skipped']} failed={b['failed']} rows_written={b['rows_written']} "
        f"rows_cross_checked={b['rows_cross_checked']} rows_mismatched={b['rows_mismatched']}"
    )
    print(
        f"  [corp_actions] windows={c['windows']} skipped={c['skipped']} failed={c['failed']} "
        f"rows_written={c['rows_written']}"
    )
    if b["aborted_at"]:
        print(f"  [bhavcopy] STOPPED at {b['aborted_at']}: {_MAX_HOLIDAY_STREAK} consecutive 404-on-both "
              "dates - archive outage or wrong URL era; those dates are un-checkpointed, re-run to retry")
    print(f"report: {path}")


def _connect_ro(sqlite_path: Path) -> sqlite3.Connection | None:
    """Read-only SQLite connection, or None if state.db is missing/unreadable (mirrors
    scripts/backfill.py:224-231 — --status must never create or migrate state.db)."""
    uri = f"file:{Path(sqlite_path).as_posix()}?mode=ro"
    try:
        return sqlite3.connect(uri, uri=True)
    except sqlite3.Error:
        return None


def _run_status(settings) -> int:
    """Read-only checkpoint summary (safe while the engine is live: SQLite only, never DuckDB)."""
    print(f"backfill_bhavcopy status  (state.db: {settings.sqlite_path()})")
    conn = _connect_ro(settings.sqlite_path())
    if conn is None:
        print("state.db missing or unreadable - nothing has been backfilled yet")
        return 0
    try:
        for feed in (_FEED_BHAVCOPY, _FEED_CORP_ACTIONS):
            try:
                rows = conn.execute(
                    "SELECT unit, through_date FROM filings_backfill_checkpoints WHERE feed = ? ORDER BY unit",
                    (feed,),
                ).fetchall()
            except sqlite3.Error:
                print(f"[{feed}] no filings_backfill_checkpoints table yet - nothing backfilled")
                continue
            done = [r for r in rows if r[1]]
            holidays = sum(1 for r in done if r[1] == "holiday")
            print(f"[{feed}] done={len(done)} holidays={holidays}")
            if done:
                print(f"  first={done[0][0]}  last={done[-1][0]}")
    finally:
        conn.close()
    return 0


# --------------------------------------------------------------------------- orchestration
async def _execute(settings, clock: Clock, args) -> int:
    conn: sqlite3.Connection | None = None
    store: MarketStore | None = None
    report = _new_report()
    try:
        try:
            conn = connect(settings.sqlite_path())
            apply_migrations(conn)
        except Exception as exc:  # noqa: BLE001
            print(f"backfill_bhavcopy: could not open/migrate state.db ({exc})", file=sys.stderr)
            return 2
        # MarketStore.open() CREATES a missing file: a --config-dir typo must not run 30 minutes into
        # an empty store and exit 0.
        db_path = Path(settings.resolved_data_dir()) / "market.duckdb"
        if not db_path.exists():
            print(f"backfill_bhavcopy: {db_path} does not exist - wrong --config-dir?", file=sys.stderr)
            return 2
        try:
            store = MarketStore.from_settings(settings, clock).open()
        except Exception as exc:  # noqa: BLE001 - most likely the live-engine single-writer lock
            print(
                "backfill_bhavcopy: could not open market.duckdb - most likely locked by a running "
                f"engine (run offline / after hours) (underlying: {type(exc).__name__}: {exc})",
                file=sys.stderr,
            )
            return 2

        async with httpx.AsyncClient(follow_redirects=True) as http:
            if not args.skip_bhavcopy:
                job = BhavcopyJob(store, clock, http)
                print(f"backfill_bhavcopy: bhavcopy {args.start.isoformat()}..{args.end.isoformat()} ...")
                await _run_bhavcopy_leg(
                    conn, store, http, clock, job, args.start, args.end, args.pace_s, report
                )
            if not args.skip_corp_actions:
                ca_from = args.start - timedelta(days=_CORP_ACTIONS_LOOKBACK_DAYS)
                print(f"backfill_bhavcopy: corp-actions {ca_from.isoformat()}..{args.end.isoformat()} ...")
                await _run_corp_actions_leg(conn, store, http, clock, ca_from, args.end, args.pace_s, report)
    finally:
        if store is not None:
            store.close()
        if conn is not None:
            conn.close()

    path = _write_report(settings, args, report)
    _print_summary(report, path)
    failed = report["bhavcopy"]["failed"] + report["corp_actions"]["failed"]
    return 1 if failed else 0


# --------------------------------------------------------------------------- argparse + entrypoint
def _parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Full-market bars_1d + corp-actions history backfill from the NSE bhavcopy archive."
    )
    parser.add_argument("--from", dest="start", type=_parse_date, default=_DEFAULT_FROM, help="YYYY-MM-DD")
    parser.add_argument("--to", dest="end", type=_parse_date, default=_DEFAULT_TO, help="YYYY-MM-DD")
    parser.add_argument("--pace-s", type=float, default=1.0, help="seconds between archive requests")
    parser.add_argument("--skip-corp-actions", action="store_true", help="skip the corp-actions leg")
    parser.add_argument(
        "--skip-bhavcopy", action="store_true",
        help="skip the bars leg (corp-actions only: fills the live hi52 veto's lookback window)",
    )
    parser.add_argument(
        "--status", action="store_true",
        help="read-only checkpoint summary (SQLite only, never DuckDB) - safe while the engine is live",
    )
    parser.add_argument("--config-dir", default=None, help="config dir override (default: repo config/)")
    return parser


def main(argv: list[str] | None = None) -> int:
    configure_logging()
    args = _build_parser().parse_args(argv)
    settings = load_settings(args.config_dir)
    clock = Clock()

    if args.status:
        return _run_status(settings)

    if args.start > args.end:
        print(f"backfill_bhavcopy: --from ({args.start}) is after --to ({args.end})", file=sys.stderr)
        return 2

    return asyncio.run(_execute(settings, clock, args))


if __name__ == "__main__":
    raise SystemExit(main())
