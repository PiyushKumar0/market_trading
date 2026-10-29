#!/usr/bin/env python
"""Rewind ``state.db`` after ``market.duckdb`` was restored from a backup export (§10.5, §2.6).

The RUNBOOK "Restore market.duckdb from backup" step that follows the ``IMPORT DATABASE``. The export
``data/backups/market-<YYYYmmddTHHMMSS>`` holds DuckDB as of its stamp; ``state.db`` is NOT restored,
so its ``job_runs`` watermarks and ``backfill_checkpoints`` still claim the days after the stamp are
done, and the §2.6 catch-up would never refill them. This script rewinds exactly those two things.

    python scripts/rewind_after_market_restore.py (--export NAME_OR_PATH | --since YYYY-MM-DD)
                                                   [--apply [--engine-crashed]]
                                                   [--data-dir DIR] [--config-dir DIR]

**S** is the stamp's DATE (IST, the engine's ``clock.now()`` stamp), INCLUSIVE: an export may run
before that day's EOD jobs (``_market_backup`` only refuses 06:00-15:45 on a trading day,
``engine/ops/main.py:4652``), and replaying a day is idempotent. With ``--export``, a ``job_runs``
success for an EARLIER day that was recorded after the stamp (a catch-up that finished after the export
ran) pulls S back to that day: its writes are not in the export either. ``--since`` takes S verbatim.

Default is a DRY RUN that prints exactly what would change (read-only, safe while the engine runs).
``--apply`` takes ``<data>/engine.lock`` non-blocking (``InstanceLock``, the engine's own single-instance
lock: the engine holds it for its whole life and the kernel frees it on any process death) and holds it
until the transaction is done, so no engine can boot mid-rewind; a held lock is a refusal. It also
refuses unless ``engine_lifecycle.state`` is ``STOPPED``. ``--engine-crashed`` additionally accepts
``RUNNING`` / ``STOPPING``, the state a crash or power loss leaves behind: the lock just acquired is the
proof that no engine process is alive. ``engine_lifecycle`` is never written, so the next boot still
sees the crash and runs its crash-recovery path. It then re-reads the plan and applies it inside one
``BEGIN IMMEDIATE`` transaction, after writing a pre-rewind copy ``<state.db>.pre-rewind-<stamp>``
through the SQLite online-backup API (the undo: put it back with the engine stopped). A second run finds
nothing to change and writes no copy.

**job_runs: SUCCESS rows with run_for_date >= S are deleted for these date-keyed jobs** (output in
market.duckdb, recomputable from upstream on a replay). Deleting works because the catch-up scans
from the last success + 1 day (``CatchUpRunner._missed_days``, engine/ops/jobs.py:817-828). ``failed``
rows are kept: the scan already starts at the oldest one (``include_failed``), and deleting it would
drop its give-up streak clock and, for a job with no success before S, the retry itself. ``skipped``
(given-up) rows are kept: that day never wrote its data, and the restore does not change why.

* ``bhavcopy`` — ``BhavcopyJob.run(d)`` fetches the NSE archive for ``d`` and ``_persist`` writes
  ``bars_1d`` rows only where none exists (engine/datafeeds/bhavcopy.py:174-238).
* ``daily_bars`` — ``backfill.run([*watchlist_symbols(), INDEX_SYMBOL, VIX_SYMBOL], "day", d, d)``
  (engine/ops/main.py:1509-1513). Checkpointed, so it only refetches after the checkpoint rewind below.
* ``deals`` — ``DealsJob.run(d)``: bulk/block deals for ``d`` → ``upsert_flagged_instrument_days``
  (engine/datafeeds/deals.py:172-194).
* ``filings_pit`` — NSE PIT windows from the STORE's own watermark ``alatest_insider_broadcast`` to
  ``d`` (engine/datafeeds/filings_pit.py:487-513); the restored watermark widens the window by itself.
* ``filings_pit_fresh`` — BSE insider surfaces for ``[d-3, d]`` → ``upsert_insider_trades``
  (engine/datafeeds/filings_pit_fresh.py:500-551).
* ``filings_results`` — results from the store watermark ``alatest_results_broadcast`` to ``d`` plus
  the event calendar around ``d`` (engine/datafeeds/filings_results.py:343-382).
* ``bar_reconcile`` — its symbols are the ``bars_1m`` holders of ``d`` and ``reconcile_log`` is its
  per-(day, symbol) resume checkpoint (engine/marketdata/reconcile.py:5-10, 179-180, 312-322). For days
  after the export no bars exist, so the replay is a successful no-op (needs a valid Kite token, else
  the day records failed until login). Kept for day S: an export taken between 15:45 and the end of
  that day's 15:50 reconcile holds self-built bars no reconcile has logged, and the replay converts them.
* ``features_daily`` — ``FeatureEngine.daily_snapshot(d)`` needs ``universe_daily(d)``
  (engine/features/engine.py:374-377); days after the export have none and record success with a
  ``features_daily_skipped_empty_universe`` warning. Kept for day S: an export taken after S's 08:30
  universe build and before its 20:45 features run holds the universe, and S's features are recomputed.

**Not rewound:** ``ins_crossings`` writes ``ins_pending`` through the state.db connection
(engine/datafeeds/ins_crossings.py:310-312), which the restore did not touch; replaying days after the
export would fail on the missing universe rows (ins_crossings.py:164) and give up after
``GIVE_UP_AFTER_DAYS``. ``tick_compact`` compacts parquet files under ``paths.parquet_dir``, outside
market.duckdb (engine/ops/main.py:1530-1531). ``rec_outcomes`` writes ``rec_outcomes`` in state.db
(engine/ops/rec_outcomes.py:45). ``signal_outcomes`` writes ``signal_outcomes`` in state.db.
``weekly_summary`` would re-send the owner digest. ``nightly_review``
is an LLM job writing state.db (engine/ops/main.py:1409-1415). Run-latest and safety-critical jobs are
not per-day replays: ``universe_build`` and ``instruments`` only ever build TODAY (main.py:1249, 1225),
so their rows for days before today stay (nothing can rebuild a past day's snapshot).

**Today's snapshot: TODAY's (IST, ``Clock().today()``) SUCCESS rows for ``universe_build`` and
``instruments`` are deleted when the restored file lacks their output** — with ``--export``, rows
recorded after the stamp (an export from an earlier day, or from before today's 08:15/08:30 builds);
with ``--since``, every such row (S <= today: today's data is not in the file). Without this the
watermark tells the boot that today's ``universe_daily`` / ``instruments_daily`` exist, and the engine
runs on the export's older snapshot until the next morning. ``universe_build`` is RUN_LATEST:
``_missed_days`` then finds today due and the boot catch-up rebuilds it (before the date-keyed
``features_daily`` replay that reads it). ``instruments`` is SAFETY_CRITICAL: in session or pre-open
after 08:15 its governing run is today's (``CatchUpRunner._governing_day``), so the boot re-runs it,
and if that run fails (before a Kite login, say) the boot freezes entries
(``data_freshness:instruments``) until a later sweep succeeds. That is the intended fail-closed outcome:
today's snapshot really is missing. After the close the governing run is the next session's 08:15, so
the deleted row costs nothing and today's ``instruments_daily`` stays missing. ``failed`` / ``skipped``
rows are kept on the same rule as above. An export stamped after the row (taken after today's builds)
holds today's snapshot: the row is kept, since rebuilding after the close would rewrite the universe
the day traded on.

**backfill_checkpoints: every row (all symbols, both intervals, NIFTY 50 and INDIA VIX included) with
through_date >= S is set to S-1 (calendar day).** ``BackfillJob.run`` resumes at
``eff_start = max(start, checkpoint + timedelta(days=1))`` (engine/marketdata/backfill.py:188), so a
checkpoint of S-1 restarts every later fetch at S — the replayed ``daily_bars`` days and the boot regime
fill ``backfill.run([index_symbol, vix_symbol], "day", ...)`` (engine/ops/post_login.py:212). A
calendar S-1 needs no trading calendar and is never later than the trading-day one. ``minute``
checkpoints are rewound too, but no engine path reads them: the §2.6 minute fills (``warmup_gap``) pick
their range from store coverage over TODAY's session (post_login.py:232, main.py:1282), never from a
checkpoint, so the engine never refetches past minute bars. The rewind keeps the checkpoints truthful,
so a manual ``scripts/backfill.py run --interval minute`` or a later ``seed`` refetches the days after S
instead of skipping them as complete.

Exit codes: 0 = dry run printed, applied, or nothing to change; 2 = refused or setup error.
"""

from __future__ import annotations

import argparse
import os
import re
import sqlite3
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path, PureWindowsPath

_REPO_SRC = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
if _REPO_SRC not in sys.path:  # pragma: no cover - loose-script shim
    sys.path.insert(0, _REPO_SRC)

import engine  # noqa: E402,F401  native import-order guard (sklearn before numba/vectorbt/cvxpy)
from engine.core.clock import IST, Clock  # noqa: E402
from engine.core.config import load_settings, repo_root  # noqa: E402
from engine.ops.jobs import (  # noqa: E402
    JOB_BHAVCOPY,
    JOB_DAILY_BARS,
    JOB_DEALS,
    JOB_FEATURES,
    JOB_FILINGS_PIT,
    JOB_FILINGS_PIT_FRESH,
    JOB_FILINGS_RESULTS,
    JOB_INS_CROSSINGS,
    JOB_INSTRUMENTS,
    JOB_NIGHTLY_REVIEW,
    JOB_REC_OUTCOMES,
    JOB_RECONCILE,
    JOB_SIGNAL_OUTCOMES,
    JOB_TICK_COMPACT,
    JOB_UNIVERSE,
    JOB_WEEKLY_SUMMARY,
)
from engine.ops.single_instance import InstanceLock  # noqa: E402

#: Date-keyed jobs whose job_runs rows from S on are deleted (module docstring: one reason each).
REPLAY_JOB_IDS: tuple[str, ...] = (
    JOB_RECONCILE, JOB_BHAVCOPY, JOB_DAILY_BARS, JOB_DEALS, JOB_FEATURES,
    JOB_FILINGS_PIT, JOB_FILINGS_PIT_FRESH, JOB_FILINGS_RESULTS,
)

#: Jobs that only ever build TODAY's snapshot; only today's row is a rewind candidate (docstring).
TODAY_SNAPSHOT_JOB_IDS: tuple[str, ...] = (JOB_INSTRUMENTS, JOB_UNIVERSE)

#: ``engine_lifecycle.state`` values a crash or power loss leaves behind (accepted by --engine-crashed).
CRASHED_STATES: frozenset[str] = frozenset({"RUNNING", "STOPPING"})

#: Date-keyed jobs deliberately left alone, with the reason printed in every plan.
NOT_REWOUND: dict[str, str] = {
    JOB_INS_CROSSINGS: "writes ins_pending in state.db, which the restore did not touch",
    JOB_TICK_COMPACT: "compacts tick parquet files outside market.duckdb",
    JOB_REC_OUTCOMES: "writes rec_outcomes in state.db",
    JOB_WEEKLY_SUMMARY: "a replay would re-send the owner digest",
    JOB_NIGHTLY_REVIEW: "LLM review into state.db; a replay spends quota",
    JOB_SIGNAL_OUTCOMES: "writes signal_outcomes in state.db, which the restore did not touch",
}

#: ``CatchUpRunner(max_lookback_days=30)`` default; engine.ops.main passes none (pinned by a test).
CATCHUP_LOOKBACK_DAYS = 30

#: Kept in sync with engine.ops.main.INDEX_SYMBOL / VIX_SYMBOL (same duplication as scripts/backfill.py:
#: importing engine.ops.main would boot the composition graph). Only used to label the plan.
INDEX_SYMBOL = "NIFTY 50"
VIX_SYMBOL = "INDIA VIX"

#: engine.ops.main._MARKET_SNAPSHOT_PREFIX + _MARKET_SNAPSHOT_STAMP. ``fullmatch`` rejects ``*.tmp``.
_EXPORT_RE = re.compile(r"market-(\d{8}T\d{6})")
_STAMP_FMT = "%Y%m%dT%H%M%S"


# --------------------------------------------------------------------------- inputs
def parse_export_stamp(arg: str) -> datetime:
    """``market-<YYYYmmddTHHMMSS>`` (a bare name or a path, either separator) → the IST stamp."""
    name = PureWindowsPath(arg.strip()).name
    match = _EXPORT_RE.fullmatch(name)
    if match is None:
        raise ValueError(f"not a market export: {arg!r} (expected market-YYYYmmddTHHMMSS)")
    try:
        stamp = datetime.strptime(match.group(1), _STAMP_FMT)
    except ValueError as exc:
        raise ValueError(f"bad export stamp in {arg!r}: {exc}") from exc
    return stamp.replace(tzinfo=IST)


def _parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Rewind state.db watermarks/checkpoints after a market.duckdb restore (§10.5)."
    )
    src = parser.add_mutually_exclusive_group(required=True)
    src.add_argument("--export", default=None,
                     help="the restored export market-YYYYmmddTHHMMSS: a name in the backups dir, or a path")
    src.add_argument("--since", type=_parse_date, default=None,
                     help="S directly (YYYY-MM-DD, inclusive) instead of an export stamp")
    parser.add_argument("--apply", action="store_true",
                        help="execute (engine must be STOPPED); default is a dry run")
    parser.add_argument("--engine-crashed", action="store_true",
                        help="with --apply: also accept engine_lifecycle RUNNING/STOPPING left by a crash; "
                             "still refused while engine.lock is held")
    parser.add_argument("--data-dir", default=None,
                        help="data dir override (default: settings / MT_DATA_DIR, as the engine resolves it)")
    parser.add_argument("--config-dir", default=None, help="config dir override (default: repo config/)")
    return parser


def _resolve_paths(settings, data_dir_arg: str | None) -> tuple[Path, Path, Path]:
    """(data dir, state.db, backups dir) — the engine's own resolution, or rebased on ``--data-dir``
    exactly as ``Settings._abs`` rebases ``data/…`` paths on ``MT_DATA_DIR``."""
    if data_dir_arg is None:
        return settings.resolved_data_dir(), settings.sqlite_path(), settings.backups_dir()
    data_dir = Path(data_dir_arg)

    def rebase(rel: str) -> Path:
        p = Path(rel)
        if p.is_absolute():
            return p
        if rel.startswith("data/"):
            return data_dir / rel[len("data/"):]
        return repo_root() / rel

    return data_dir, rebase(settings.paths.sqlite), rebase(settings.paths.backups_dir)


def _find_export(arg: str, backups_dir: Path) -> Path | None:
    given = Path(arg.strip())
    if given.is_dir():
        return given
    under = backups_dir / PureWindowsPath(arg.strip()).name
    return under if under.is_dir() else None


# --------------------------------------------------------------------------- plan
@dataclass
class Plan:
    requested: date                                   # S from --export / --since
    since: date                                       # effective S (may be pulled back)
    pulled_back_by: list[tuple[str, str, str]] = field(default_factory=list)
    job_rows: list[tuple[str, str, str | None]] = field(default_factory=list)   # success: deleted
    kept_rows: list[tuple[str, str, str | None]] = field(default_factory=list)  # failed/skipped
    checkpoints: list[tuple[str, str, str]] = field(default_factory=list)
    orphans: list[str] = field(default_factory=list)  # jobs left with no success before S
    # TODAY_SNAPSHOT_JOB_IDS success rows for today: (job_id, run_for_date, last_success_at)
    today_rows: list[tuple[str, str, str | None]] = field(default_factory=list)       # deleted
    today_in_export: list[tuple[str, str, str | None]] = field(default_factory=list)  # kept

    @property
    def rewind_to(self) -> date:
        return self.since - timedelta(days=1)

    @property
    def empty(self) -> bool:
        return not self.job_rows and not self.checkpoints and not self.today_rows


def _parse_ts(value: str) -> datetime | None:
    try:
        ts = datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return None
    return ts.replace(tzinfo=IST) if ts.tzinfo is None else ts


def compute_plan(conn: sqlite3.Connection, requested: date, stamp: datetime | None,
                 today: date) -> Plan:
    """What the rewind changes, read from ``conn`` (no writes). ``today`` is the IST trading date."""
    ids = REPLAY_JOB_IDS
    marks = ",".join("?" for _ in ids)
    plan = Plan(requested=requested, since=requested)
    if stamp is not None:
        rows = conn.execute(
            f"SELECT job_id, run_for_date, last_success_at FROM job_runs WHERE job_id IN ({marks}) "
            "AND status='success' AND run_for_date < ? AND last_success_at IS NOT NULL "
            "ORDER BY run_for_date, job_id",
            (*ids, requested.isoformat()),
        ).fetchall()
        for job_id, run_for, at in rows:
            recorded = _parse_ts(at)
            try:
                day = date.fromisoformat(run_for)
            except ValueError:
                continue
            if recorded is not None and recorded > stamp:
                plan.pulled_back_by.append((job_id, run_for, at))
                plan.since = min(plan.since, day)
    since = plan.since.isoformat()
    rows = conn.execute(
        f"SELECT job_id, run_for_date, status FROM job_runs WHERE job_id IN ({marks}) "
        "AND run_for_date >= ? ORDER BY job_id, run_for_date",
        (*ids, since),
    ).fetchall()
    plan.job_rows = [(r[0], r[1], r[2]) for r in rows if r[2] == "success"]
    plan.kept_rows = [(r[0], r[1], r[2]) for r in rows if r[2] != "success"]
    plan.checkpoints = [
        (r[0], r[1], r[2]) for r in conn.execute(
            "SELECT symbol, interval, through_date FROM backfill_checkpoints WHERE through_date >= ? "
            "ORDER BY interval, through_date, symbol",
            (since,),
        )
    ]
    for job_id in sorted({j for j, _, _ in plan.job_rows}):
        earlier = conn.execute(
            "SELECT 1 FROM job_runs WHERE job_id=? AND status='success' AND run_for_date < ? LIMIT 1",
            (job_id, since),
        ).fetchone()
        if earlier is None:
            plan.orphans.append(job_id)
    snap = ",".join("?" for _ in TODAY_SNAPSHOT_JOB_IDS)
    for job_id, run_for, status, at in conn.execute(
        f"SELECT job_id, run_for_date, status, last_success_at FROM job_runs WHERE job_id IN ({snap}) "
        "AND run_for_date = ? ORDER BY job_id",
        (*TODAY_SNAPSHOT_JOB_IDS, today.isoformat()),
    ):
        recorded = _parse_ts(at)
        if status != "success":
            plan.kept_rows.append((job_id, run_for, status))
        elif stamp is not None and recorded is not None and recorded <= stamp:
            plan.today_in_export.append((job_id, run_for, at))
        else:  # after the stamp, --since, or an unreadable time: the file lacks it (fail closed)
            plan.today_rows.append((job_id, run_for, at))
    return plan


def _engine_state(conn: sqlite3.Connection) -> tuple[str | None, int | None]:
    row = conn.execute("SELECT state, pid FROM engine_lifecycle WHERE id=1").fetchone()
    return (row[0], row[1]) if row else (None, None)


# --------------------------------------------------------------------------- output
def _print_plan(plan: Plan, today: date, stamp: datetime | None, export_name: str | None) -> None:
    if stamp is not None:
        print(f"export: {export_name} (stamp {stamp:%Y-%m-%d %H:%M:%S} IST)")
    if plan.since < plan.requested:
        print(f"S = {plan.since.isoformat()} (pulled back from {plan.requested.isoformat()}: "
              f"{len(plan.pulled_back_by)} success row(s) for earlier days were recorded after the export)")
        for job_id, run_for, at in plan.pulled_back_by:
            print(f"    {job_id} {run_for} recorded {at}")
    else:
        print(f"S = {plan.since.isoformat()} (inclusive)")

    print(f"\njob_runs success rows to delete ({len(plan.job_rows)}):")
    by_job: dict[str, list[str]] = defaultdict(list)
    for job_id, run_for, _status in plan.job_rows:
        by_job[job_id].append(run_for)
    for job_id in REPLAY_JOB_IDS:
        if by_job.get(job_id):
            print(f"  {job_id}: {', '.join(by_job[job_id])}")
    if not plan.job_rows:
        print("  (none)")
    _print_kept([r for r in plan.kept_rows if r[0] not in TODAY_SNAPSHOT_JOB_IDS])
    for job_id, why in NOT_REWOUND.items():
        print(f"  not touched: {job_id} — {why}")

    print(f"\ntoday's ({today.isoformat()}) snapshot success rows to delete ({len(plan.today_rows)}):")
    for job_id, run_for, at in plan.today_rows:
        print(f"  {job_id} {run_for} (recorded {at}) — its output is not in the restored file;\n"
              "    the boot catch-up rebuilds it")
    if not plan.today_rows:
        print("  (none)")
    _print_kept([r for r in plan.kept_rows if r[0] in TODAY_SNAPSHOT_JOB_IDS])
    for job_id, run_for, at in plan.today_in_export:
        print(f"  kept: {job_id} {run_for} — recorded before the export ({at}): its snapshot is in it")
    if any(j == JOB_INSTRUMENTS for j, _, _ in plan.today_rows):
        print(f"  instruments is SAFETY_CRITICAL: in session the boot re-runs it, and entries stay\n"
              f"    FROZEN (data_freshness:{JOB_INSTRUMENTS}) until a run succeeds; that needs a Kite "
              "login.\n    After the close the next run is the next session's 08:15.")

    print(f"\nbackfill_checkpoints to rewind to {plan.rewind_to.isoformat()} ({len(plan.checkpoints)}):")
    groups: dict[tuple[str, str], list[str]] = defaultdict(list)
    for symbol, interval, through in plan.checkpoints:
        groups[(interval, through)].append(symbol)
    for (interval, through), symbols in groups.items():
        print(f"  {interval} {through} -> {plan.rewind_to.isoformat()} ({len(symbols)}): "
              f"{', '.join(sorted(symbols))}")
    if not plan.checkpoints:
        print("  (none)")
    regime = sorted({s for s, i, _ in plan.checkpoints if i == "day" and s in (INDEX_SYMBOL, VIX_SYMBOL)})
    if regime:
        print(f"  regime symbols included: {', '.join(regime)}")

    horizon = today - timedelta(days=CATCHUP_LOOKBACK_DAYS)
    if plan.since < horizon:
        print(f"\nWARNING: S is before {horizon.isoformat()}: the catch-up replays at most "
              f"{CATCHUP_LOOKBACK_DAYS} days back, so {plan.since.isoformat()}..{horizon.isoformat()} "
              "is never replayed. Refill those days by hand (scripts/backfill.py run --interval day, "
              "scripts/backfill_bhavcopy.py).")
    for job_id in plan.orphans:
        print(f"\nWARNING: {job_id} has no success before S, so the catch-up anchors at the engine's "
              "off-window start, not at S: it will not replay these days by itself.")
    _print_outlook(plan, today)


def _print_kept(rows: list[tuple[str, str, str | None]]) -> None:
    for job_id, run_for, status in rows:
        print(f"  kept: {job_id} {run_for} ({status}) — "
              + ("the catch-up already retries it" if status == "failed" else "given up; not retried"))


def _print_outlook(plan: Plan, today: date) -> None:
    s, s_1 = plan.since.isoformat(), plan.rewind_to.isoformat()
    yesterday = today - timedelta(days=1)
    minute_cmd = (
        f"scripts/backfill.py run --interval minute --from {s} --to {yesterday.isoformat()}"
        if plan.since <= yesterday else "(none to refetch: S is today)"
    )
    print(f"""
After --apply, the next boot recovers by itself (§2.6 catch-up, one run per trading day from {s}):
  - bars_1d: bhavcopy (every EQ symbol) and daily_bars (watchlist + {INDEX_SYMBOL} + {VIX_SYMBOL});
    the boot regime fill and the post-login re-trigger refetch {INDEX_SYMBOL} / {VIX_SYMBOL} from {s}
  - flagged_instrument_days (deals), insider_trades (filings_pit, filings_pit_fresh),
    results_filings + event calendar (filings_results)
  - day {s} only, where the export holds the inputs: official 1m bars + reconcile_log for symbols
    with self-built bars (bar_reconcile), features_daily (needs universe_daily for {s})
  daily_bars and bar_reconcile need a valid Kite session: boot after the day's login, or log in
  right after start.

Lost for days after the export (nothing rebuilds them):
  - bars_1m after the export: self-built, gap_backfilled and official. The engine's minute fills
    cover today's session only. Official candles can be refetched by hand:
    {minute_cmd}
    (minute checkpoints now stop at {s_1}). Raw ticks in the parquet dir survive the restore,
    but nothing rebuilds 1m bars from them.
  - universe_daily and instruments_daily for past days (only today's are rebuilt: by the boot
    catch-up when today's rows are deleted above, else at the next 08:30 / 08:15 fire), and so
    features_daily for every day after {s} (it needs that day's universe_daily)
  - news / news_clusters / sentiment_agg (LLM scores), catalyst_watchlist, feature_snapshots,
    reconcile_log drift evidence, bars_1m_no_trade
  - run-latest tables (corp_actions, sector_map, symbol_isin, shp_quarterly, ...) refresh only on
    their next scheduled run, within that job's own window (corp_actions: run day -7..+35)""")


# --------------------------------------------------------------------------- run
def _dry_run(state_db: Path, requested: date, stamp: datetime | None, export_name: str | None,
             today: date) -> int:
    conn = sqlite3.connect(f"file:{state_db.as_posix()}?mode=ro", uri=True)
    try:
        state, pid = _engine_state(conn)
        plan = compute_plan(conn, requested, stamp, today)
    finally:
        conn.close()
    print(f"DRY RUN — nothing is changed. state.db: {state_db}")
    print(f"engine_lifecycle.state = {state}")
    _print_plan(plan, today, stamp, export_name)
    if state != "STOPPED":
        print(f"\nNOTE: --apply would be refused: engine_lifecycle.state is {state} (pid {pid}).")
        if state in CRASHED_STATES:
            print("If the engine crashed (no mt-engine / engine process left), add --engine-crashed: it "
                  "applies only while engine.lock is free, and leaves engine_lifecycle as it is.")
    elif plan.empty:
        print("\nnothing to change.")
    else:
        print("\nRe-run with --apply (engine STOPPED) to execute this in one transaction.")
    return 0


def _apply(state_db: Path, lock_path: Path, requested: date, stamp: datetime | None,
           export_name: str | None, today: date, *, engine_crashed: bool = False) -> int:
    # Held until the transaction ends: an engine boot meanwhile is refused at its own acquire.
    lock = InstanceLock(lock_path)
    if not lock.acquire():
        print(f"refusing --apply: {lock_path} is held (pid {lock.holder_pid()}) — the engine is running. "
              "Stop mt-engine first.", file=sys.stderr)
        return 2
    accepted = {"STOPPED", *CRASHED_STATES} if engine_crashed else {"STOPPED"}
    try:
        conn = sqlite3.connect(str(state_db), isolation_level=None, timeout=5.0)
        try:
            state, pid = _engine_state(conn)
            if state not in accepted:
                hint = ("If it crashed and no engine process is left, re-run with --engine-crashed."
                        if state in CRASHED_STATES else "")
                print(f"refusing --apply: engine_lifecycle.state is {state} (pid {pid}), not STOPPED. "
                      "Stop mt-engine cleanly first. Never edit engine_lifecycle by hand: its "
                      f"state is what makes the next boot crash-recover. {hint}".rstrip(),
                      file=sys.stderr)
                return 2
            if state in CRASHED_STATES:
                print(f"engine_lifecycle.state is {state} (pid {pid}) and {lock_path} was free: treating "
                      "the engine as crashed. engine_lifecycle is left as it is for the next boot.")
            preview = compute_plan(conn, requested, stamp, today)
            print(f"APPLY — state.db: {state_db}")
            _print_plan(preview, today, stamp, export_name)
            if preview.empty:
                print("\nnothing to change.")
                return 0
            # Online-backup API: WAL-safe, unlike a file copy of state.db alone.
            copy = state_db.with_name(f"{state_db.name}.pre-rewind-{datetime.now(IST):{_STAMP_FMT}}")
            dest = sqlite3.connect(str(copy))
            try:
                conn.backup(dest)
            finally:
                dest.close()
            print(f"\npre-rewind copy: {copy}")
            conn.execute("BEGIN IMMEDIATE")
            try:
                plan = compute_plan(conn, requested, stamp, today)
                if _engine_state(conn) != (state, pid) or plan != preview:
                    raise RuntimeError("state.db changed between the preview and the write — rolled back")
                marks = ",".join("?" for _ in REPLAY_JOB_IDS)
                deleted = conn.execute(
                    f"DELETE FROM job_runs WHERE job_id IN ({marks}) AND run_for_date >= ? "
                    "AND status = 'success'",
                    (*REPLAY_JOB_IDS, plan.since.isoformat()),
                ).rowcount
                today_deleted = sum(
                    conn.execute(
                        "DELETE FROM job_runs WHERE job_id = ? AND run_for_date = ? AND status = 'success'",
                        (job_id, run_for),
                    ).rowcount
                    for job_id, run_for, _at in plan.today_rows
                )
                rewound = conn.execute(
                    "UPDATE backfill_checkpoints SET through_date = ? WHERE through_date >= ?",
                    (plan.rewind_to.isoformat(), plan.since.isoformat()),
                ).rowcount
                if (deleted != len(plan.job_rows) or today_deleted != len(plan.today_rows)
                        or rewound != len(plan.checkpoints)):
                    raise RuntimeError(
                        f"row counts drifted from the plan (deleted {deleted}/{len(plan.job_rows)}, "
                        f"today's snapshot rows {today_deleted}/{len(plan.today_rows)}, "
                        f"rewound {rewound}/{len(plan.checkpoints)}) — rolled back"
                    )
                conn.execute("COMMIT")
            except BaseException:
                if conn.in_transaction:
                    conn.execute("ROLLBACK")
                raise
        finally:
            conn.close()
    finally:
        lock.release()
    print(f"\napplied: deleted {deleted} job_runs row(s) and {today_deleted} of today's snapshot row(s), "
          f"rewound {rewound} checkpoint(s) to {plan.rewind_to.isoformat()}.")
    return 0


def main(argv: list[str] | None = None, *, today: date | None = None) -> int:
    args = _build_parser().parse_args(argv)
    today = today or Clock().today()
    settings = load_settings(args.config_dir)
    data_dir, state_db, backups_dir = _resolve_paths(settings, args.data_dir)

    stamp: datetime | None = None
    export_name: str | None = None
    if args.export is not None:
        try:
            stamp = parse_export_stamp(args.export)
        except ValueError as exc:
            print(f"rewind: {exc}", file=sys.stderr)
            return 2
        export = _find_export(args.export, backups_dir)
        if export is None:
            print(f"rewind: export {args.export!r} not found (as a path or under {backups_dir}); "
                  "pass --since YYYY-MM-DD to give S directly", file=sys.stderr)
            return 2
        export_name = export.name
        requested = stamp.date()
    else:
        requested = args.since
    if requested > today:
        print(f"rewind: S {requested.isoformat()} is after today {today.isoformat()}", file=sys.stderr)
        return 2
    if not state_db.is_file():
        print(f"rewind: state.db not found at {state_db}", file=sys.stderr)
        return 2

    try:
        if args.apply:
            return _apply(state_db, data_dir / "engine.lock", requested, stamp, export_name, today,
                          engine_crashed=args.engine_crashed)
        return _dry_run(state_db, requested, stamp, export_name, today)
    except (sqlite3.Error, RuntimeError) as exc:
        print(f"rewind: nothing changed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
