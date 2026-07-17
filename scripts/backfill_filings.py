#!/usr/bin/env python
"""§2.8 corporate-filings initial-history-seed CLI (stage 1 — ingest + backfill only).

The one-off owner tool that seeds the ~3-year filings history the §2.8.4 event study (stage 2) will
backtest. It drives the SAME defensive parse functions the nightly ``filings_*`` jobs use, but owns
the windowing + checkpointing (exactly as ``scripts/backfill.py`` owns chunking while ``BackfillJob``
owns the fetch). Point-in-time throughout (§2.8 rule i): every row keyed on its exchange broadcast
timestamp, never the period label.

    python scripts/backfill_filings.py seed [--from YYYY-MM-DD] [--symbols CSV]
                                             [--skip-pit] [--skip-results] [--skip-shp]
                                             [--config-dir DIR]

Default ``--from`` is 3 years back; ``--to`` is always today. NSE endpoints (PIT, results,
event-calendar) are walked in ≤31-day windows; BSE SHP is a per-symbol quarter loop. Every request is
spaced ≥1.5 s (observed safe, §2.8). Resumable: each (feed, unit) checkpoint lives in ``state.db``
``filings_backfill_checkpoints`` (migration 0002), so a re-run skips completed windows/symbols —
re-fetching is safe anyway (all writes are idempotent upserts). Prints a summary and writes
``data/reports/filings_backfill_report.json``.

BSE caveat (§2.8): the SHP detail endpoint shapes are UNVERIFIED (see ``filings_shp`` module docstring);
the SHP leg is built defensively and degrades per-symbol without aborting the seed.

Exit codes: 0 = ran clean; 1 = some units failed for non-setup reasons (re-run to resume); 2 =
setup error (no universe resolved, ``market.duckdb``/``state.db`` unavailable).
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

import engine  # noqa: E402,F401  native import-order guard (sklearn before numba/vectorbt/cvxpy)
import httpx  # noqa: E402

from engine.core.bse_http import bse_get  # noqa: E402
from engine.core.clock import Clock  # noqa: E402
from engine.core.config import load_settings, repo_root  # noqa: E402
from engine.core.db import connect  # noqa: E402
from engine.core.log import configure_logging, get_logger  # noqa: E402
from engine.core.migrations import apply_migrations  # noqa: E402
from engine.core.nse_http import nse_get  # noqa: E402
from engine.datafeeds.earnings_calendar import event_calendar_range_url, parse_event_calendar  # noqa: E402
from engine.datafeeds.filings_pit import parse_pit, pit_url  # noqa: E402
from engine.datafeeds.filings_results import parse_results, results_url  # noqa: E402
from engine.datafeeds.filings_shp import (  # noqa: E402
    BSE_SHP_DETAIL_URL,
    BSE_SHP_QUARTER_INDEX_URL,
    parse_shp_detail,
    parse_shp_quarter_index,
)
from engine.datafeeds.isin_map import IsinMapJob  # noqa: E402
from engine.marketdata.store import MarketStore  # noqa: E402
from engine.universe.builder import parse_index_constituents_csv  # noqa: E402

_log = get_logger("scripts.backfill_filings")

#: ≥1.5 s between requests (§2.8, observed safe). NSE windows are ≤31 days apiece.
_PACE_S = 1.5
_NSE_WINDOW_DAYS = 31
_SEED_YEARS = 3


# --------------------------------------------------------------------------- argparse
def _parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="§2.8 corporate-filings history seed (stage 1 — ingest + backfill only)."
    )
    parser.add_argument("mode", choices=("seed",))
    parser.add_argument(
        "--from", dest="start", type=_parse_date, default=None,
        help=f"seed start (YYYY-MM-DD); default {_SEED_YEARS} years back",
    )
    parser.add_argument("--symbols", default=None, help="comma-separated universe override (uppercased)")
    parser.add_argument("--skip-pit", action="store_true", help="skip the insider-trades (PIT) leg")
    parser.add_argument("--skip-results", action="store_true", help="skip the results + event-calendar leg")
    parser.add_argument("--skip-shp", action="store_true", help="skip the SHP + pledge leg (BSE)")
    parser.add_argument(
        "--redo-shp", action="store_true",
        help="clear ONLY the shp feed's checkpoints first, forcing a full SHP re-fetch "
             "(used after the 2026-07-17 broadcast_dt parser fix; other feeds untouched)",
    )
    parser.add_argument("--config-dir", default=None, help="config dir override (default: repo config/)")
    return parser


def _cp_clear_feed(conn: sqlite3.Connection, feed: str) -> int:
    """Delete every checkpoint row for ``feed`` (the --redo-shp surface). Returns rows cleared."""
    cur = conn.execute("DELETE FROM filings_backfill_checkpoints WHERE feed=?", (feed,))
    conn.commit()
    return cur.rowcount


# --------------------------------------------------------------------------- universe resolution
def _resolve_universe(settings, symbols_arg: str | None) -> tuple[list[str], str]:
    """Resolve the working symbol set + source label (runtime cache → committed seed → --symbols)."""
    if symbols_arg:
        syms = sorted({s.strip().upper() for s in symbols_arg.split(",") if s.strip()})
        return syms, "--symbols override"
    cache = settings.resolved_data_dir() / "universe" / "nifty200_cached.csv"
    seed_rel = Path(settings.universe.nifty200_seed_path)
    seed = seed_rel if seed_rel.is_absolute() else repo_root() / seed_rel
    for path, label in ((cache, f"runtime cache {cache}"), (seed, f"committed seed {seed}")):
        try:
            if path.exists():
                syms = parse_index_constituents_csv(path.read_text(encoding="utf-8"))
                if syms:
                    return syms, label
        except (OSError, ValueError):
            _log.warning("filings_seed_universe_unreadable", path=str(path))
    return [], ""


# --------------------------------------------------------------------------- windows + checkpoints
def _windows(frm: date, to: date, span_days: int = _NSE_WINDOW_DAYS) -> list[tuple[date, date]]:
    """Ascending ≤``span_days`` windows covering ``[frm, to]`` inclusive."""
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


# --------------------------------------------------------------------------- per-feed seed legs
async def _seed_nse_windowed(
    conn, store, http, clock, feed: str, frm: date, to: date, summary: dict,
) -> None:
    """Walk PIT or results over ≤31-day windows (checkpoint per window). event-calendar rides the
    same window as the results leg (its historical board-meeting dates → earnings_calendar)."""
    for w_frm, w_to in _windows(frm, to):
        unit = f"{w_frm.isoformat()}..{w_to.isoformat()}"
        if _cp_done(conn, feed, unit):
            summary[feed]["skipped"] += 1
            continue
        try:
            if feed == "pit":
                resp = await nse_get(http, pit_url(w_frm, w_to), timeout=20.0)
                rows = parse_pit(json.loads(resp.content))
                written = await store.arun(store.upsert_insider_trades, rows)
            else:  # results (+ event calendar)
                resp = await nse_get(http, results_url(w_frm, w_to), timeout=20.0)
                rows = parse_results(json.loads(resp.content))
                written = await store.arun(store.upsert_results_filings, rows)
                await asyncio.sleep(_PACE_S)
                ev_resp = await nse_get(http, event_calendar_range_url(w_frm, w_to), timeout=20.0)
                ev_rows = parse_event_calendar(json.loads(ev_resp.content))
                now = clock.now()
                await store.arun(
                    store.upsert_earnings_calendar, [{**r, "recorded_at": now} for r in ev_rows]
                )
                summary[feed]["events"] += len(ev_rows)
        except Exception as exc:  # noqa: BLE001 - record + continue; the checkpoint stays open to retry
            summary[feed]["failed"] += 1
            _log.warning("filings_seed_window_failed", feed=feed, unit=unit, error=f"{type(exc).__name__}: {exc}")
            await asyncio.sleep(_PACE_S)
            continue
        _cp_set(conn, feed, unit, w_to.isoformat(), clock.now().isoformat())
        summary[feed]["written"] += written
        summary[feed]["windows"] += 1
        _log.info("filings_seed_window_done", feed=feed, unit=unit, written=written)
        await asyncio.sleep(_PACE_S)


async def _seed_shp(conn, store, http, clock, symbols: list[str], frm: date, to: date, summary: dict) -> None:
    """Per-symbol BSE SHP quarter loop (checkpoint per symbol). Requires ``symbol_isin`` scrip codes;
    a symbol without one is skipped-and-counted. UNVERIFIED BSE shape — degrades per-symbol (§2.8)."""
    isin_map = await store.asymbol_isin_map()
    for symbol in symbols:
        if _cp_done(conn, "shp", symbol):
            summary["shp"]["skipped"] += 1
            continue
        mapping = isin_map.get(symbol)
        code = str((mapping or {}).get("bse_scrip_code") or "").strip()
        if not code:
            summary["shp"]["skipped_no_scrip"] += 1
            continue
        try:
            idx_resp = await bse_get(http, BSE_SHP_QUARTER_INDEX_URL.format(code=code), timeout=20.0)
            quarter_index = parse_shp_quarter_index(json.loads(idx_resp.content))
            written = 0
            for entry in quarter_index:
                q_end = entry.get("qtr_end")
                if not isinstance(q_end, date) or not (frm <= q_end <= to):
                    continue
                await asyncio.sleep(_PACE_S)
                detail_resp = await bse_get(
                    http, BSE_SHP_DETAIL_URL.format(code=code, qtrid=entry["qtrid"]), timeout=20.0
                )
                rows = parse_shp_detail(
                    json.loads(detail_resp.content),
                    symbol=symbol, qtr_end=q_end, broadcast_dt=None, revised=False,
                )
                written += await store.arun(store.upsert_shp_quarterly, rows)
        except Exception as exc:  # noqa: BLE001 - degrade THIS symbol; the checkpoint stays open to retry
            summary["shp"]["failed"] += 1
            _log.warning("filings_seed_shp_failed", symbol=symbol, error=f"{type(exc).__name__}: {exc}")
            await asyncio.sleep(_PACE_S)
            continue
        _cp_set(conn, "shp", symbol, to.isoformat(), clock.now().isoformat())
        summary["shp"]["written"] += written
        summary["shp"]["symbols"] += 1
        await asyncio.sleep(_PACE_S)


# --------------------------------------------------------------------------- orchestration
def _new_summary() -> dict:
    return {
        "pit": {"windows": 0, "written": 0, "skipped": 0, "failed": 0},
        "results": {"windows": 0, "written": 0, "events": 0, "skipped": 0, "failed": 0},
        "shp": {"symbols": 0, "written": 0, "skipped": 0, "skipped_no_scrip": 0, "failed": 0},
    }


async def _execute(settings, clock: Clock, args, universe: list[str], source: str) -> int:
    frm = args.start or (clock.today() - timedelta(days=365 * _SEED_YEARS))
    to = clock.today()
    if frm > to:
        print(f"backfill_filings: --from ({frm}) is after today ({to})", file=sys.stderr)
        return 2

    conn: sqlite3.Connection | None = None
    store: MarketStore | None = None
    try:
        try:
            conn = connect(settings.sqlite_path())
            apply_migrations(conn)
        except Exception as exc:  # noqa: BLE001
            print(f"backfill_filings: could not open/migrate state.db ({exc})", file=sys.stderr)
            return 2
        try:
            store = MarketStore.from_settings(settings, clock).open()
        except Exception as exc:  # noqa: BLE001 - most likely the live-engine single-writer lock
            print(
                "backfill_filings: could not open market.duckdb — most likely locked by a running "
                f"engine (seed offline / after hours) ({exc})",
                file=sys.stderr,
            )
            return 2

        summary = _new_summary()
        async with httpx.AsyncClient(follow_redirects=True) as http:
            # SHP needs BSE scrip codes: build symbol_isin first (paces its own BSE calls, §2.8).
            if not args.skip_shp:
                print(f"backfill_filings: building symbol_isin for {len(universe)} symbols ...")
                isin_result = await IsinMapJob(settings, store, clock, http).run(universe)
                summary["isin"] = isin_result.model_dump(mode="json")
            if not args.skip_pit:
                # ASCII only in prints: Windows consoles may be cp1252 ('<=' not '≤').
                print(f"backfill_filings: PIT {frm}..{to} in <={_NSE_WINDOW_DAYS}d windows ...")
                await _seed_nse_windowed(conn, store, http, clock, "pit", frm, to, summary)
            if not args.skip_results:
                print(f"backfill_filings: results + event-calendar {frm}..{to} ...")
                await _seed_nse_windowed(conn, store, http, clock, "results", frm, to, summary)
            if not args.skip_shp:
                if args.redo_shp:
                    cleared = _cp_clear_feed(conn, "shp")
                    print(f"backfill_filings: --redo-shp cleared {cleared} shp checkpoints")
                print(f"backfill_filings: SHP per-symbol quarter loop ({len(universe)} symbols) ...")
                await _seed_shp(conn, store, http, clock, universe, frm, to, summary)
    finally:
        if store is not None:
            store.close()
        if conn is not None:
            conn.close()

    path = _write_report(settings, args, source, frm, to, summary)
    _print_summary(summary, path)
    failed = summary["pit"]["failed"] + summary["results"]["failed"] + summary["shp"]["failed"]
    return 1 if failed else 0


def _write_report(settings, args, source, frm, to, summary) -> Path:
    out = settings.resolved_data_dir() / "reports" / "filings_backfill_report.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(
        json.dumps(
            {
                "mode": args.mode,
                "from": frm.isoformat(),
                "to": to.isoformat(),
                "universe_source": source,
                "skip": {"pit": args.skip_pit, "results": args.skip_results, "shp": args.skip_shp},
                "summary": summary,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return out


def _print_summary(summary: dict, path: Path) -> None:
    print("\n=== filings backfill summary ===")
    for feed in ("pit", "results", "shp"):
        print(f"  [{feed}] " + "  ".join(f"{k}={v}" for k, v in summary[feed].items()))
    print(f"report: {path}")


# --------------------------------------------------------------------------- entrypoint
def main(argv: list[str] | None = None) -> int:
    configure_logging()
    args = _build_parser().parse_args(argv)
    settings = load_settings(args.config_dir)
    clock = Clock()

    universe, source = _resolve_universe(settings, args.symbols)
    if not universe:
        print(
            "backfill_filings: no universe resolved — pass --symbols, or ensure the runtime cache / "
            f"committed seed ({repo_root() / settings.universe.nifty200_seed_path}) exists",
            file=sys.stderr,
        )
        return 2
    print(f"universe: {source} ({len(universe)} symbols)")
    return asyncio.run(_execute(settings, clock, args, universe, source))


if __name__ == "__main__":
    raise SystemExit(main())
