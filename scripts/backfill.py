#!/usr/bin/env python
"""G1 initial-history-seed CLI (§3.1, §4.4 job 3, §2.6 step 4, A2) — drives ``BackfillJob``.

The one-off owner tool the RUNBOOK "Historical backfill procedure" calls "Seed the full history
once": it pulls official Kite candles for the trading universe through the SAME chunked, checkpointed,
resumable :class:`~engine.marketdata.backfill.BackfillJob` the engine's nightly ``daily_bars`` job
uses (A2 ≤3 req/s, paced inside ``KiteClient``). The initial minute history is a multi-evening job;
every re-run resumes exactly where it stopped because each ``(symbol, interval)`` checkpoint is
persisted in ``state.db`` ``backfill_checkpoints``. A11: Kite candles are already corp-action adjusted
and are written verbatim — no re-adjustment happens anywhere (that concern lives inside ``BackfillJob``).

    python scripts/backfill.py seed   [--daily-years N] [--minute-years N] [--skip-daily] [--skip-minute] \
                                      [--symbols CSV] [--config-dir DIR] [--dry-run] [--reset-checkpoints]
    python scripts/backfill.py run    --interval {minute,day} --from YYYY-MM-DD --to YYYY-MM-DD \
                                      [--symbols CSV] [--config-dir DIR] [--dry-run] [--reset-checkpoints]
    python scripts/backfill.py status [--config-dir DIR]

``seed`` backfills the universe: DAILY (universe + NIFTY 50 + India VIX, default
``data.backfill_daily_years``=2y) then MINUTE (universe only, default ``data.backfill_minute_years``=1y).
``run`` backfills exactly the given interval/range/symbols. ``status`` reports checkpoint completeness
from SQLite only (safe while the engine is live — it never opens the DuckDB store).

**Monotonic-checkpoint caveat:** a checkpoint only ever advances FORWARD, so extending history further
BACK than a previous run is skipped unless you pass ``--reset-checkpoints`` (which clears the relevant
checkpoints first). Re-fetching is safe — bar writes are idempotent upserts. For the same reason ``run``
refuses a ``--to`` beyond today: a checkpoint advanced past today would silently mask those days forever.

``--dry-run`` (seed/run) needs no Kite token and touches no network/store: it prints the universe source,
per-interval symbol counts, spans, chunk sizes, request estimates and minimum wall time — runnable tonight
before login. ``status`` is read-only too (it never creates or migrates ``state.db``).

Exit codes: 0 = ran clean (or dry-run/status); 1 = some spans failed for non-auth reasons (re-run to
resume from checkpoints); 2 = setup/auth/lock error — missing or stale token (including one that goes
stale mid-run), instrument-refresh failure, or ``market.duckdb``/``state.db`` unavailable.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import sqlite3
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

_REPO_SRC = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
if _REPO_SRC not in sys.path:  # pragma: no cover - loose-script shim
    sys.path.insert(0, _REPO_SRC)

import engine  # noqa: E402,F401  native import-order guard (sklearn before numba/vectorbt/cvxpy)
from engine.broker.instruments import InstrumentStore  # noqa: E402
from engine.broker.kite_client import KiteClient  # noqa: E402
from engine.broker.rate_limiter import RateLimiter  # noqa: E402
from engine.broker.session import SessionManager  # noqa: E402
from engine.core.clock import Clock  # noqa: E402
from engine.core.config import load_settings, repo_root  # noqa: E402
from engine.core.db import connect  # noqa: E402
from engine.core.log import configure_logging, get_logger  # noqa: E402
from engine.core.migrations import apply_migrations  # noqa: E402
from engine.core.secrets import KITE_ACCESS_TOKEN, KITE_API_KEY, Secrets  # noqa: E402
from engine.marketdata.backfill import (  # noqa: E402
    KITE_DAY_CHUNK_DAYS,
    KITE_MINUTE_CHUNK_DAYS,
    BackfillJob,
)
from engine.marketdata.store import MarketStore  # noqa: E402
from engine.universe.builder import parse_index_constituents_csv  # noqa: E402

_log = get_logger("scripts.backfill")

# INDEX_SYMBOL / VIX_SYMBOL are canonically DEFINED in engine.ops.main (lines 101-102) and nowhere
# else; importing that module would boot the whole composition graph, so the two names are duplicated
# here (kept in sync with engine.ops.main.INDEX_SYMBOL / VIX_SYMBOL). They are the bars_1d symbols the
# regime features read (§7.1 regime_data_ready): the seed's DAILY set includes them, the MINUTE set
# does not — matching the engine's own backfill_hook / daily_bars job.
INDEX_SYMBOL = "NIFTY 50"
VIX_SYMBOL = "INDIA VIX"


@dataclass(frozen=True)
class IntervalPlan:
    """One (interval, symbols, [start, end]) unit of work shared by seed/run/dry-run."""

    interval: str          # "day" | "minute"
    symbols: list[str]
    start: date
    end: date


# --------------------------------------------------------------------------- argparse
def _parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="G1 initial-history-seed CLI — drives BackfillJob (§3.1/§4.4 job 3/A2)."
    )
    parser.add_argument("mode", choices=("seed", "run", "status"))
    parser.add_argument(
        "--interval", choices=("minute", "day"), default=None, help="run mode: which interval to fetch"
    )
    parser.add_argument("--from", dest="start", type=_parse_date, default=None, help="run mode: YYYY-MM-DD")
    parser.add_argument("--to", dest="end", type=_parse_date, default=None, help="run mode: YYYY-MM-DD")
    parser.add_argument(
        "--daily-years", type=int, default=None,
        help="seed: daily-history span in years (default: settings.data.backfill_daily_years=2)",
    )
    parser.add_argument(
        "--minute-years", type=int, default=None,
        help="seed: minute-history span in years (default: settings.data.backfill_minute_years=1)",
    )
    parser.add_argument("--skip-daily", action="store_true", help="seed: skip the daily interval")
    parser.add_argument("--skip-minute", action="store_true", help="seed: skip the minute interval")
    parser.add_argument(
        "--symbols", default=None,
        help=(
            "comma-separated universe override (uppercased); scopes seed/run fetches AND the status "
            "report; no index symbols added in run mode"
        ),
    )
    parser.add_argument("--config-dir", default=None, help="config dir override (default: repo config/)")
    parser.add_argument(
        "--dry-run", action="store_true",
        help="seed/run: resolve universe + print request estimates only — no token, network or writes",
    )
    parser.add_argument(
        "--reset-checkpoints", action="store_true",
        help=(
            "clear (symbol, interval) checkpoints for the symbols about to run BEFORE running. The "
            "checkpoint is MONOTONIC, so extending history further BACK than a previous run requires "
            "this; re-fetching is safe because bar writes are idempotent upserts."
        ),
    )
    return parser


# --------------------------------------------------------------------------- universe resolution
def _dedup(items: list[str]) -> list[str]:
    """Order-preserving de-duplication (§ universe resolution: index symbols appended, deduped)."""
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        if item not in seen:
            seen.add(item)
            out.append(item)
    return out


def _resolve_universe(settings, symbols_arg: str | None) -> tuple[list[str], str]:
    """Resolve the working universe + a human-readable source label (shared by seed/run/status).

    Ladder: ``--symbols`` override → runtime cache ``<data>/universe/nifty200_cached.csv`` (the exact
    name ``UniverseBuilder`` writes, builder.py:173) → committed seed ``universe.nifty200_seed_path``.
    Returns ``([], "")`` if nothing resolves so the caller can print an actionable error and exit 2.
    """
    if symbols_arg:
        syms = _dedup([s.strip().upper() for s in symbols_arg.split(",") if s.strip()])
        return syms, "--symbols override"

    cache = settings.resolved_data_dir() / "universe" / "nifty200_cached.csv"
    if cache.exists():
        try:
            syms = parse_index_constituents_csv(cache.read_text(encoding="utf-8"))
            if syms:
                return syms, f"runtime cache {cache}"
        except (OSError, ValueError):
            _log.warning("backfill_cache_unreadable", path=str(cache))

    seed_rel = Path(settings.universe.nifty200_seed_path)
    seed = seed_rel if seed_rel.is_absolute() else repo_root() / seed_rel
    if seed.exists():
        try:
            syms = parse_index_constituents_csv(seed.read_text(encoding="utf-8"))
            if syms:
                return syms, f"committed seed {seed}"
        except (OSError, ValueError):
            _log.warning("backfill_seed_unreadable", path=str(seed))

    return [], ""


def _build_plans(mode: str, args, settings, clock: Clock, universe: list[str]) -> list[IntervalPlan]:
    """Turn the resolved universe + args into the interval plans to run (seed: day then minute)."""
    today = clock.today()
    plans: list[IntervalPlan] = []
    if mode == "seed":
        daily_years = args.daily_years if args.daily_years is not None else settings.data.backfill_daily_years
        minute_years = (
            args.minute_years if args.minute_years is not None else settings.data.backfill_minute_years
        )
        if not args.skip_daily:
            daily_symbols = _dedup([*universe, INDEX_SYMBOL, VIX_SYMBOL])
            plans.append(
                IntervalPlan("day", daily_symbols, today - timedelta(days=365 * daily_years), today)
            )
        if not args.skip_minute:
            plans.append(
                IntervalPlan("minute", list(universe), today - timedelta(days=365 * minute_years), today)
            )
    else:  # run — exactly the given interval/range/symbols; NO implicit index symbols
        plans.append(IntervalPlan(args.interval, list(universe), args.start, args.end))
    return plans


# --------------------------------------------------------------------------- estimates (dry-run)
def _chunk_days(settings, interval: str) -> int:
    """Per-request span in effect: the settings knob clamped to the pinned Kite cap (mirrors
    BackfillJob._chunk_days so the dry-run estimate matches what run() will actually do)."""
    cfg = settings.backfill
    if interval == "day":
        return max(1, min(int(cfg.day_chunk_days), KITE_DAY_CHUNK_DAYS))
    return max(1, min(int(cfg.minute_chunk_days), KITE_MINUTE_CHUNK_DAYS))


def _span_days(start: date, end: date) -> int:
    return (end - start).days + 1  # inclusive, matching BackfillJob's [start, end]


def _connect_ro(sqlite_path: Path) -> sqlite3.Connection | None:
    """Read-only SQLite connection, or None if state.db is missing/unreadable. Read-only keeps
    dry-run/status free of side effects — they must never create or migrate state.db."""
    uri = f"file:{Path(sqlite_path).as_posix()}?mode=ro"
    try:
        return sqlite3.connect(uri, uri=True)
    except sqlite3.Error:
        return None


def _count_existing_checkpoints(sqlite_path: Path, plan: IntervalPlan) -> int:
    """Read-only count of checkpoints a --reset would delete (dry-run preview; never creates/writes)."""
    if not plan.symbols:
        return 0
    conn = _connect_ro(sqlite_path)
    if conn is None:
        return 0
    try:
        placeholders = ",".join("?" for _ in plan.symbols)
        row = conn.execute(
            f"SELECT COUNT(*) FROM backfill_checkpoints WHERE interval = ? AND symbol IN ({placeholders})",
            (plan.interval, *plan.symbols),
        ).fetchone()
        return int(row[0]) if row else 0
    except sqlite3.Error:
        return 0
    finally:
        conn.close()


def _run_dry(settings, plans: list[IntervalPlan], args) -> int:
    print("DRY RUN — no secrets, no network, no store writes.")
    req_per_s = max(1, int(settings.backfill.req_per_s))
    total_requests = 0
    total_wall = 0.0
    for plan in plans:
        chunk = _chunk_days(settings, plan.interval)
        span = _span_days(plan.start, plan.end)
        per_symbol = math.ceil(span / chunk)
        requests = per_symbol * len(plan.symbols)
        wall = requests / req_per_s
        total_requests += requests
        total_wall += wall
        print(
            f"[{plan.interval}] symbols={len(plan.symbols)} "
            f"span={plan.start.isoformat()}..{plan.end.isoformat()} ({span}d) chunk={chunk}d  "
            f"est_requests={requests} (ceil({span}/{chunk})={per_symbol}/symbol)  "
            f"min_wall~{wall:.1f}s @{req_per_s} req/s"
        )
        if args.reset_checkpoints:
            existing = _count_existing_checkpoints(settings.sqlite_path(), plan)
            print(
                f"    reset-checkpoints (dry-run): would DELETE {existing} existing "
                f"{plan.interval} checkpoint row(s); none deleted"
            )
    print(f"TOTAL est_requests={total_requests}  min_wall~{total_wall:.1f}s ({total_wall / 60:.1f} min)")
    return 0


# --------------------------------------------------------------------------- status
def _print_symbol_list(items: list[str], limit: int = 50) -> None:
    if not items:
        return
    shown = items[:limit]
    suffix = f"  (+{len(items) - limit} more)" if len(items) > limit else ""
    print("    " + ", ".join(shown) + suffix)


def _run_status(settings, clock: Clock, symbols_arg: str | None) -> int:
    """G1 completeness evidence (safe while the engine is live): read-only SQLite, never DuckDB."""
    today = clock.today()
    print(f"backfill status @ {today.isoformat()}  (state.db: {settings.sqlite_path()})")

    rows: list[tuple[str, str, str | None]] = []
    conn = _connect_ro(settings.sqlite_path())
    if conn is None:
        print("state.db missing or unreadable — nothing has been backfilled yet")
    else:
        try:
            rows = [
                (r[0], r[1], r[2])
                for r in conn.execute("SELECT symbol, interval, through_date FROM backfill_checkpoints")
            ]
        except sqlite3.Error:  # schema not migrated yet — same meaning as an empty table
            print("state.db has no backfill_checkpoints table yet — nothing has been backfilled")
        finally:
            conn.close()

    by_interval: dict[str, dict[str, str | None]] = {}
    for symbol, interval, through in rows:
        by_interval.setdefault(interval, {})[symbol] = through

    universe, source = _resolve_universe(settings, symbols_arg)
    if universe:
        print(f"universe: {source} ({len(universe)} symbols)")
    else:
        print("universe: UNRESOLVED (no --symbols/cache/seed) — missing-symbol comparison skipped")

    for interval in sorted(set(by_interval) | {"day", "minute"}):
        cps = by_interval.get(interval, {})
        throughs = sorted(v for v in cps.values() if v)
        print(f"\n[{interval}] checkpointed symbols: {len(cps)}")
        if throughs:
            print(f"  through_date: min={throughs[0]} max={throughs[-1]}")
        else:
            print("  through_date: (none)")
        if not universe:
            continue
        missing = sorted(s for s in universe if s not in cps)
        print(f"  universe symbols with NO {interval} checkpoint: {len(missing)}")
        _print_symbol_list(missing)
        stale: list[str] = []
        for symbol in universe:
            value = cps.get(symbol)
            if not value:
                continue
            try:
                through = date.fromisoformat(value)
            except ValueError:
                continue
            if (today - through).days > 5:
                stale.append(f"{symbol}@{value}")
        print(
            f"  universe symbols >5 days behind today (staleness HINT, not a hard rule): {len(stale)}"
        )
        _print_symbol_list(sorted(stale))
    return 0


# --------------------------------------------------------------------------- fetch (seed/run)
def _token_smell(text: str) -> bool:
    """Heuristic: does this error text smell like a stale/invalid Kite token vs a transient error?
    Applied to exceptions at setup AND to failed-span error strings after a run (a token can go stale
    MID-run — Kite allows one active session, so a web/app login invalidates the script's token)."""
    low = text.lower()
    return (
        "tokenexception" in low
        or "invalid session" in low
        or "403" in low
        or "forbidden" in low
        or "api_key" in low
        or "api key" in low
        or "access token" in low
    )


def _looks_like_token_problem(exc: Exception) -> bool:
    return _token_smell(f"{type(exc).__name__}: {exc}")


def _reset_checkpoints(conn: sqlite3.Connection, plans: list[IntervalPlan]) -> int:
    """DELETE the (symbol, interval) checkpoints for every symbol about to run, per interval (A2)."""
    total = 0
    for plan in plans:
        if not plan.symbols:
            continue
        placeholders = ",".join("?" for _ in plan.symbols)
        cur = conn.execute(
            f"DELETE FROM backfill_checkpoints WHERE interval = ? AND symbol IN ({placeholders})",
            (plan.interval, *plan.symbols),
        )
        conn.commit()
        deleted = cur.rowcount if cur.rowcount is not None else 0
        print(f"reset-checkpoints: deleted {deleted} {plan.interval} checkpoint row(s)")
        total += deleted
    return total


def _print_report(report) -> None:
    print(
        f"  [{report.interval}] requested={len(report.requested)} "
        f"fetched_spans={len(report.fetched)} bars_written={report.bars_written} "
        f"failed_spans={len(report.failed)}"
    )
    for span in report.failed:
        print(f"    FAILED {span.symbol} {span.frm}..{span.to}: {span.error}")


def _write_json_summary(settings, args, source: str, results, started: datetime, finished: datetime) -> Path:
    out = settings.resolved_data_dir() / "reports" / "backfill_report.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "mode": args.mode,
        "args": {
            "interval": args.interval,
            "from": args.start.isoformat() if args.start else None,
            "to": args.end.isoformat() if args.end else None,
            "symbols": args.symbols,
            "daily_years": args.daily_years,
            "minute_years": args.minute_years,
            "skip_daily": args.skip_daily,
            "skip_minute": args.skip_minute,
            "reset_checkpoints": args.reset_checkpoints,
            "config_dir": args.config_dir,
        },
        "universe_source": source,
        "started_at": started.isoformat(),
        "finished_at": finished.isoformat(),
        "intervals": [
            {
                "interval": report.interval,
                "symbols": len(plan.symbols),
                "span": [plan.start.isoformat(), plan.end.isoformat()],
                "bars_written": report.bars_written,
                "failed": [
                    {"symbol": s.symbol, "frm": s.frm, "to": s.to, "error": s.error}
                    for s in report.failed
                ],
                "started_at": run_started.isoformat(),
                "finished_at": run_finished.isoformat(),
            }
            for plan, report, run_started, run_finished in results
        ],
    }
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return out


async def _execute(settings, clock: Clock, plans: list[IntervalPlan], source: str, args) -> int:
    """The async orchestration: auth + wiring → BackfillJob.run per plan → report. Only ever reached
    for a real (non-dry-run) seed/run — this is the only path that touches secrets/network/stores."""
    secrets = Secrets()
    if not secrets.has(KITE_API_KEY) or not secrets.has(KITE_ACCESS_TOKEN):
        print(
            "backfill: kite_api_key / kite_access_token missing from DPAPI secrets — run offline "
            "AFTER the daily Kite login so a token exists (§10.2)",
            file=sys.stderr,
        )
        return 2

    session = SessionManager(secrets, clock, redirect_path=settings.broker.kite_login_redirect_path)
    kc = session.kite_connect()
    if kc is None:
        print("backfill: no Kite api_key configured — cannot build a broker session", file=sys.stderr)
        return 2
    kite = KiteClient(kc, RateLimiter(clock), clock)

    instruments = InstrumentStore(clock)
    try:
        count = await instruments.refresh(kite)
        _log.info("backfill_instruments_refreshed", count=count)
    except Exception as exc:  # noqa: BLE001 - any refresh failure is a setup failure (exit 2)
        if _looks_like_token_problem(exc):
            print(
                "backfill: access token is stale — complete today's Kite login, then re-run "
                "(checkpoints resume automatically)",
                file=sys.stderr,
            )
        else:
            print(
                "backfill: instrument refresh failed — cannot resolve symbol tokens "
                f"(underlying: {type(exc).__name__}: {exc})",
                file=sys.stderr,
            )
        return 2

    try:
        store = MarketStore.from_settings(settings, clock).open()
    except Exception as exc:  # noqa: BLE001 - most likely the live-engine single-writer lock
        print(
            "backfill: could not open market.duckdb — most likely it is locked by a running engine "
            "(the runbook seeds offline: stop the engine or run after hours) "
            f"(underlying: {type(exc).__name__}: {exc})",
            file=sys.stderr,
        )
        return 2

    conn: sqlite3.Connection | None = None
    started_overall = clock.now()
    results: list[tuple[IntervalPlan, object, datetime, datetime]] = []
    try:
        try:
            conn = connect(settings.sqlite_path())
            apply_migrations(conn)
        except Exception as exc:  # noqa: BLE001 - same exit-2 contract as the DuckDB open above
            print(
                f"backfill: could not open/migrate state.db ({settings.sqlite_path()}) — is the "
                f"engine mid-write? (underlying: {type(exc).__name__}: {exc})",
                file=sys.stderr,
            )
            return 2
        if args.reset_checkpoints:
            _reset_checkpoints(conn, plans)
        job = BackfillJob(store, kite, clock, settings, conn, instruments.token_for_symbol)
        for plan in plans:
            print(
                f"backfill: running [{plan.interval}] {len(plan.symbols)} symbols "
                f"{plan.start.isoformat()}..{plan.end.isoformat()} ..."
            )
            run_started = clock.now()
            report = await job.run(plan.symbols, plan.interval, plan.start, plan.end)
            run_finished = clock.now()
            results.append((plan, report, run_started, run_finished))
            _print_report(report)
    finally:
        if conn is not None:
            conn.close()
        store.close()

    finished_overall = clock.now()
    path = _write_json_summary(settings, args, source, results, started_overall, finished_overall)
    print(f"backfill: wrote report {path}")

    failed_errors = [span.error or "" for _, report, _, _ in results for span in report.failed]
    if any(_token_smell(err) for err in failed_errors):
        print(
            "backfill: failures look like a stale Kite token (it can go stale MID-run if another "
            "session logs in) — complete today's Kite login, then re-run; completed symbols are "
            "checkpointed and will be skipped",
            file=sys.stderr,
        )
        return 2
    if failed_errors:
        print(
            "backfill: some spans failed — re-run scripts/backfill.py; it resumes from checkpoints",
            file=sys.stderr,
        )
        return 1
    return 0


# --------------------------------------------------------------------------- entrypoint
def main(argv: list[str] | None = None) -> int:
    configure_logging()
    parser = _build_parser()
    args = parser.parse_args(argv)
    settings = load_settings(args.config_dir)
    clock = Clock()

    if args.mode == "status":
        return _run_status(settings, clock, args.symbols)

    if args.mode == "run":
        if not args.interval:
            parser.error("run mode requires --interval {minute,day}")
        if args.start is None or args.end is None:
            parser.error("run mode requires --from and --to (YYYY-MM-DD)")
        if args.start > args.end:
            parser.error(f"--from ({args.start.isoformat()}) must be <= --to ({args.end.isoformat()})")
        if args.end > clock.today():
            parser.error(
                f"--to ({args.end.isoformat()}) is after today ({clock.today().isoformat()}): the "
                "monotonic checkpoint would advance past today and those days would be silently "
                "skipped by every later backfill (including the engine's nightly job)"
            )

    universe, source = _resolve_universe(settings, args.symbols)
    if not universe:
        print(
            "backfill: no universe resolved — pass --symbols, or ensure the runtime cache "
            f"({settings.resolved_data_dir() / 'universe' / 'nifty200_cached.csv'}) or the committed "
            f"seed ({repo_root() / settings.universe.nifty200_seed_path}) exists",
            file=sys.stderr,
        )
        return 2
    print(f"universe: {source} ({len(universe)} symbols)")

    plans = _build_plans(args.mode, args, settings, clock, universe)
    if not plans:
        print("nothing to do (both --skip-daily and --skip-minute set)")
        return 0

    if args.dry_run:
        return _run_dry(settings, plans, args)

    return asyncio.run(_execute(settings, clock, plans, source, args))


if __name__ == "__main__":
    raise SystemExit(main())
