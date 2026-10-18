"""Composition-root wiring (§3.2.12, §10.1/§4.4): the integrator's ``engine.ops.main`` seams.

``engine.ops.main`` is the only module allowed to import everything; it is not unit-tested elsewhere
(it spawns uvicorn + the ticker child and idles on a stop signal). These tests lock the two pieces the
integrator owns that ARE pure enough to assert without booting the whole engine:

  * ``build_job_registry`` — the single §10.1/§4.4 job inventory feeding BOTH the live Scheduler and
    the startup CatchUpRunner (same registry, §2.6): every Phase-1 job present, at its pinned fire-time,
    in the right §2.6 step-5 class + dependency order, with the weekly Sunday sector-map fire-day.
  * ``_scheduled_runner`` — the live-scheduler wrapper that records the ``job_runs`` watermark on every
    fire (so the CatchUpRunner never re-runs a job the scheduler already ran today) and marks a failed
    run so catch-up retries it, never crashing the loop.

The wiring inside ``compose`` is asserted on a composed engine (``tests/_composition.py``: the real
object graph, never booted) through its registry, its scheduler jobs, constructor spies and the
recorded sweep legs (``_World``); ``_boot`` and ``_shutdown`` run against recording fake engines.
"""

from __future__ import annotations

import ast
import asyncio
import inspect
import json
import logging
import re
import threading
from contextlib import asynccontextmanager
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
import yaml
from kiteconnect.exceptions import TokenException

from engine.broker.instruments import InstrumentStore
from engine.core.calendar import NSECalendar
from engine.core.clock import IST, Clock
from engine.core.config import Settings, config_dir, load_settings
from engine.core.db import connect
from engine.core.enums import Actor, Mode, RiskState
from engine.core.migrations import apply_migrations
from engine.core.protected_store import IntegrityError, ProtectedStore
from engine.core.secrets import KITE_ACCESS_TOKEN
from engine.core.types import Bar, OwnerConfirmation, Tick, TradeWindow
from engine.datafeeds.sector_map import SectorMapJob, SectorMapResult
from engine.marketdata.store import DailyBar, MarketStore
from engine.marketdata.tick_compact import TickCompactionResult
from engine.notify.catalog import MessageKind
from engine.ops import main as opsmain
from engine.ops.jobs import (
    JOB_BHAVCOPY,
    JOB_EARNINGS,
    JOB_INSTRUMENTS,
    JOB_NIGHTLY_REVIEW,
    JOB_PREOPEN_PLANNER,
    JOB_SECTOR_MAP,
    JOB_UNIVERSE,
    AdvisoryOutcome,
    AdvisoryRun,
    CatchUpRunner,
    CatchUpScope,
    JobClass,
    JobRegistry,
    JobSpec,
)
from engine.ops.main import (
    _FULL_SWEEP_TRIGGERS,
    _PUBLICATION_LEG_ORDER,
    _SWEEP_IN_FLIGHT_REPLY,
    NO_EDGE_SHADOW_STRATEGIES,
    PHASE1_JOB_IDS,
    POST_ARM_JOB_IDS,
    CompactionLane,
    _arm_live_jobs,
    _arm_registry_jobs,
    _consume_ins_pending,
    _freeze_lift_skip_reason,
    _freeze_lift_sweep,
    _hi52_daily_leg,
    _ins_rows_to_consume,
    _publication_order,
    _read_ins_pending,
    _retest_active,
    _retest_republish,
    _roll_batch_ticks,
    _scheduled_runner,
    _single_flight_sweep,
    _strategy_expected_edge_pct,
    _sweep_window_active,
    _ticker_tokens,
    boot_contract_watchdog,
    build_job_registry,
    cancel_post_arm,
    hydrate_instruments_at_startup,
    seed_boot_snapshots,
    start_scheduler_and_fire_post_arm,
)
from engine.ops.pipeline import RecommendationPipeline
from engine.ops.scheduler import Scheduler
from engine.risk.events import TOPIC_RISK_STATE, RiskStateChanged
from engine.risk.limits import LimitsEngine
from engine.strategy.prescreen import SignalPreScreen
from engine.strategy.retest import DEFAULT_RETEST_SESSIONS, RestingLevelBook
from engine.strategy.scanners import brk20, cat, cat_reversal, hi52, ins
from engine.strategy.types import RawLevels, SignalCandidate
from tests._composition import composed_engine
from tests.conftest import FIXED_NOW, PINNED_CAPITAL
from tests.unit.test_instruments import NIFTY50_ROW, RELIANCE_ROW, FakeKite


async def _noop() -> None:
    return None


async def _noop_dated(_d: date) -> None:
    return None


def _all_noop_fns() -> dict:
    """A runner for every Phase-1 job id (DATE_KEYED ids need the dated signature)."""
    date_keyed = {
        opsmain.JOB_RECONCILE, opsmain.JOB_BHAVCOPY, opsmain.JOB_DAILY_BARS,
        opsmain.JOB_DEALS, opsmain.JOB_FEATURES,
        opsmain.JOB_FILINGS_PIT, opsmain.JOB_FILINGS_RESULTS,   # §2.8 date-keyed
        opsmain.JOB_FILINGS_PIT_FRESH, opsmain.JOB_INS_CROSSINGS,
        opsmain.JOB_TICK_COMPACT,                               # §4.3/WO-7 date-keyed
    }
    return {jid: (_noop_dated if jid in date_keyed else _noop) for jid in PHASE1_JOB_IDS}


@pytest.fixture
def calendar(clock):
    return NSECalendar(config_dir() / "calendar", clock, strict=False)


@pytest.fixture
def market_store(tmp_path, clock):
    s = MarketStore(tmp_path / "market.duckdb", tmp_path / "parquet", clock)
    s.open()
    yield s
    s.close()


# --------------------------------------------------------------------------- cold-start hydrate ladder (F2)
@pytest.mark.asyncio
async def test_startup_hydrates_from_persisted_snapshot(market_store, clock):
    """Restart after 08:15: the table has yesterday's dump, no Kite session needed → HYDRATE branch."""
    # Persist a snapshot the way the 08:15 job does (a separate populated store -> snapshot_rows -> upsert).
    seeded = InstrumentStore(clock)
    await seeded.refresh(FakeKite([RELIANCE_ROW, NIFTY50_ROW]))
    market_store.upsert_instruments_daily(seeded.snapshot_rows(clock.today()))

    instruments = InstrumentStore(clock)                     # fresh process: empty in-memory store
    assert instruments.is_empty is True
    branch = await hydrate_instruments_at_startup(
        instruments, market_store, kite=None, session_valid=False, clock=clock,
    )
    assert branch == "hydrated"
    assert instruments.hydrated is True
    assert instruments.token_for_symbol("RELIANCE") == 408065
    assert instruments.token_for_symbol("NIFTY 50") == 256265   # index seam rebuilt too


@pytest.mark.asyncio
async def test_startup_refreshes_live_when_table_empty_and_session_valid(market_store, clock):
    """Fresh install, table empty, valid session → REFRESH branch: live pull + persist for next boot."""
    instruments = InstrumentStore(clock)
    kite = FakeKite([RELIANCE_ROW, NIFTY50_ROW])
    branch = await hydrate_instruments_at_startup(
        instruments, market_store, kite=kite, session_valid=True, clock=clock,
    )
    assert branch == "startup_refresh"
    assert kite.calls == 1
    assert instruments.hydrated is False                     # a live refresh, not a hydrate
    assert instruments.token_for_symbol("RELIANCE") == 408065
    # And it persisted, so the NEXT restart can hydrate pre-login.
    latest = market_store.get_latest_instruments_daily()
    assert latest is not None
    d, stored = latest
    assert d == clock.today()
    assert any(r["tradingsymbol"] == "RELIANCE" for r in stored)


@pytest.mark.asyncio
async def test_startup_unavailable_when_no_snapshot_and_no_session(market_store, clock):
    """Neither a stored snapshot nor a valid session → UNAVAILABLE: token map stays empty (entries
    FROZEN), but with a named cause rather than a silent unknown_token storm."""
    instruments = InstrumentStore(clock)
    branch = await hydrate_instruments_at_startup(
        instruments, market_store, kite=None, session_valid=False, clock=clock,
    )
    assert branch == "unavailable"
    assert instruments.is_empty is True
    assert instruments.token_for_symbol("RELIANCE") is None


@pytest.mark.asyncio
async def test_degraded_snapshot_escalates_to_live_refresh(market_store, clock):
    """2026-07-21 torn-persist incident: a snapshot with ZERO index rows (a taskkill mid-upsert lost
    the index tail) cannot resolve the regime tokens. With a live session the ladder must ESCALATE:
    live refresh + persist, healing the stored day in place."""
    # Persist a DEGRADED snapshot: equities only, no index rows (what the torn 07-21 day looked like).
    degraded = InstrumentStore(clock)
    await degraded.refresh(FakeKite([RELIANCE_ROW]))
    market_store.upsert_instruments_daily(degraded.snapshot_rows(clock.today()))

    instruments = InstrumentStore(clock)
    kite = FakeKite([RELIANCE_ROW, NIFTY50_ROW])
    branch = await hydrate_instruments_at_startup(
        instruments, market_store, kite=kite, session_valid=True, clock=clock,
    )
    assert branch == "hydrated_degraded_refreshed"
    assert kite.calls == 1                                     # escalation actually refreshed live
    assert instruments.hydrated is False                       # store now holds a live dump
    assert instruments.token_for_symbol("NIFTY 50") == 256265  # regime token resolvable again
    # And the stored day is HEALED: the persisted snapshot now carries the index row, so the NEXT
    # cold boot hydrates it without escalating.
    healed = InstrumentStore(clock)
    branch2 = await hydrate_instruments_at_startup(
        healed, market_store, kite=None, session_valid=False, clock=clock,
    )
    assert branch2 == "hydrated"
    assert healed.token_for_symbol("NIFTY 50") == 256265


@pytest.mark.asyncio
async def test_degraded_snapshot_without_session_stays_hydrated_with_warning(market_store, clock, caplog):
    """Same degraded snapshot pre-login: no session to escalate with — the ladder keeps the hydrated
    (degraded) map and the post-login instruments step heals it on login."""
    degraded = InstrumentStore(clock)
    await degraded.refresh(FakeKite([RELIANCE_ROW]))
    market_store.upsert_instruments_daily(degraded.snapshot_rows(clock.today()))

    instruments = InstrumentStore(clock)
    with caplog.at_level(logging.WARNING, logger="engine.ops.post_login"):
        branch = await hydrate_instruments_at_startup(
            instruments, market_store, kite=None, session_valid=False, clock=clock,
        )
    assert branch == "hydrated"
    assert instruments.index_count == 0
    assert instruments.token_for_symbol("NIFTY 50") is None    # degraded until login refresh
    warned = [r for r in caplog.records if r.getMessage() == "instruments_hydrate_degraded"]
    assert len(warned) == 1 and warned[0].levelname == "WARNING"   # the operator's pre-login signal


@pytest.mark.asyncio
async def test_startup_skips_when_store_already_populated(market_store, clock):
    """A store already populated this process (refresh already ran) short-circuits to already_loaded —
    no redundant hydrate, no clobbering a live dump with a stale snapshot."""
    instruments = InstrumentStore(clock)
    await instruments.refresh(FakeKite([RELIANCE_ROW, NIFTY50_ROW]))
    branch = await hydrate_instruments_at_startup(
        instruments, market_store, kite=None, session_valid=False, clock=clock,
    )
    assert branch == "already_loaded"
    assert instruments.hydrated is False                     # untouched: still the live refresh


# --------------------------------------------------------------------------- registry structure


def test_registry_covers_every_phase1_job() -> None:
    # Phase-1 fns only ⇒ the Phase-2 specs (fns.get returns None) are skipped, not half-registered.
    reg = build_job_registry(load_settings(), _all_noop_fns())
    assert {s.job_id for s in reg.specs()} == set(PHASE1_JOB_IDS)
    assert len(reg) == len(PHASE1_JOB_IDS) == 24   # +4 §2.8 filings, +1 WO-7 tick compaction,
                                                   # +1 §6.1 ins_crossings (2026-08-17),
                                                   # +1 §2.8.4 results_line_items, +1 isin_map,
                                                   # +1 feed_freshness (2026-09-24),
                                                   # +1 claude_login_check (2026-10-11),
                                                   # +1 §10.5 market_backup (2026-10-11)


def test_registry_phase2_jobs_register_when_their_fns_exist(calendar) -> None:
    """§8.3 wiring: digest 08:35 → planner 08:50 (run-latest, after the news chain in catch-up
    order), time exit 09:30 run-latest ahead of the post-arm news chain, reco-expiry 15:45
    run-latest, rec outcomes 18:30 date-keyed after daily_bars, nightly review 21:00 date-keyed
    (§2.6 per missed day)."""
    fns = _all_noop_fns()
    fns[opsmain.JOB_CATALYST_DIGEST] = _noop
    fns[opsmain.JOB_PREOPEN_PLANNER] = _noop
    fns[opsmain.JOB_RECO_EXPIRE] = _noop
    fns[opsmain.JOB_TIME_EXIT_CHECK] = _noop
    fns[opsmain.JOB_REC_OUTCOMES] = _noop_dated
    fns[opsmain.JOB_WEEKLY_SUMMARY] = _noop_dated
    fns[opsmain.JOB_NIGHTLY_REVIEW] = _noop_dated
    by_id = {s.job_id: s for s in build_job_registry(load_settings(), fns, calendar=calendar).specs()}
    assert set(by_id) == set(PHASE1_JOB_IDS) | set(opsmain.PHASE2_JOB_IDS)

    expected = {
        opsmain.JOB_CATALYST_DIGEST: (JobClass.RUN_LATEST, time(8, 35), 25),
        opsmain.JOB_PREOPEN_PLANNER: (JobClass.RUN_LATEST, time(8, 50), 28),
        opsmain.JOB_TIME_EXIT_CHECK: (JobClass.RUN_LATEST, time(9, 30), 5),
        opsmain.JOB_RECO_EXPIRE:     (JobClass.RUN_LATEST, time(15, 45), 60),
        opsmain.JOB_REC_OUTCOMES:    (JobClass.DATE_KEYED, time(18, 30), 35),
        opsmain.JOB_WEEKLY_SUMMARY:  (JobClass.DATE_KEYED, time(19, 0), 36),
        opsmain.JOB_NIGHTLY_REVIEW:  (JobClass.DATE_KEYED, time(21, 0), 80),
    }
    for jid, (cls, at, order) in expected.items():
        assert (by_id[jid].job_class, by_id[jid].at, by_id[jid].order) == (cls, at, order), jid
    # Catch-up dependency order (§2.7 steps 4-6): news chain before digest before planner.
    news = by_id[opsmain.JOB_NEWS_CHAIN].order
    assert news < by_id[opsmain.JOB_CATALYST_DIGEST].order < by_id[opsmain.JOB_PREOPEN_PLANNER].order
    assert by_id[opsmain.JOB_TIME_EXIT_CHECK].order < news
    assert opsmain.JOB_TIME_EXIT_CHECK in POST_ARM_JOB_IDS
    # rec_outcomes scores on bhavcopy's and daily_bars' bars: after both, on the clock and in catch-up.
    for dep in (opsmain.JOB_BHAVCOPY, opsmain.JOB_DAILY_BARS):
        assert by_id[dep].at < by_id[opsmain.JOB_REC_OUTCOMES].at
        assert by_id[dep].order < by_id[opsmain.JOB_REC_OUTCOMES].order
    assert opsmain.JOB_REC_OUTCOMES in POST_ARM_JOB_IDS
    # weekly_summary waits for rec_outcomes: after it, on the clock and in catch-up.
    assert by_id[opsmain.JOB_REC_OUTCOMES].at < by_id[opsmain.JOB_WEEKLY_SUMMARY].at
    assert by_id[opsmain.JOB_REC_OUTCOMES].order < by_id[opsmain.JOB_WEEKLY_SUMMARY].order
    assert opsmain.JOB_WEEKLY_SUMMARY in POST_ARM_JOB_IDS


def test_registry_classes_and_fire_times_match_the_schedule() -> None:
    """§10.1: instruments 08:15, surveillance 08:20, universe 08:30, reconcile 15:50, bhavcopy 18:00,
    corp-actions 20:15, earnings 18:30, deals 20:30 — in their §2.6 step-5 classes."""
    settings = load_settings()
    by_id = {s.job_id: s for s in build_job_registry(settings, _all_noop_fns()).specs()}

    expected = {
        opsmain.JOB_INSTRUMENTS:  (JobClass.SAFETY_CRITICAL, time(8, 15)),
        opsmain.JOB_SURVEILLANCE: (JobClass.SAFETY_CRITICAL, time(8, 20)),
        opsmain.JOB_EARNINGS:     (JobClass.SAFETY_CRITICAL, time(18, 30)),
        opsmain.JOB_UNIVERSE:     (JobClass.RUN_LATEST,      time(8, 30)),
        opsmain.JOB_NEWS_CHAIN:   (JobClass.RUN_LATEST,      time(8, 25)),
        # corp_actions/deals moved past NSE's ~18:15-19:00 evening maintenance window (2026-07-24)
        opsmain.JOB_CORP_ACTIONS: (JobClass.RUN_LATEST,      time(20, 15)),
        opsmain.JOB_SECTOR_MAP:   (JobClass.RUN_LATEST,      time(8, 30)),
        opsmain.JOB_BACKUP:       (JobClass.RUN_LATEST,      time(21, 0)),
        opsmain.JOB_RECONCILE:    (JobClass.DATE_KEYED,      time(15, 50)),
        opsmain.JOB_BHAVCOPY:     (JobClass.DATE_KEYED,      time(18, 0)),
        opsmain.JOB_DAILY_BARS:   (JobClass.DATE_KEYED,      time(18, 5)),
        opsmain.JOB_DEALS:        (JobClass.DATE_KEYED,      time(20, 30)),
        # features 18:50→20:45 (2026-08-18): it reads day-d deals flags + corp actions, whose jobs
        # moved to 20:30/20:15 on 2026-07-24 — at 18:50 it read both before their daily writes.
        opsmain.JOB_FEATURES:     (JobClass.DATE_KEYED,      time(20, 45)),
        opsmain.JOB_FILINGS_PIT:       (JobClass.DATE_KEYED, time(18, 35)),
        opsmain.JOB_FILINGS_PIT_FRESH: (JobClass.DATE_KEYED, time(19, 0)),
        opsmain.JOB_FILINGS_RESULTS:   (JobClass.DATE_KEYED, time(18, 45)),
        # §6.1 `ins` (2026-08-17): AFTER filings_pit_fresh's 19:00 — it consumes that job's rows.
        opsmain.JOB_INS_CROSSINGS:     (JobClass.DATE_KEYED, time(19, 15)),
        opsmain.JOB_FILINGS_SHP:     (JobClass.RUN_LATEST,   time(18, 50)),
        opsmain.JOB_RESULTS_LINE_ITEMS: (JobClass.RUN_LATEST, time(19, 30)),
        opsmain.JOB_ISIN_MAP:          (JobClass.RUN_LATEST, time(18, 40)),
        opsmain.JOB_FEED_FRESHNESS:    (JobClass.RUN_LATEST, time(21, 30)),
        # Stored claude.ai login health: before the 08:25 pre-open chain. Overnight news scoring and
        # early hydration (~06:30) can hit a dead login first; their auth_failed alerts carry the hint.
        opsmain.JOB_CLAUDE_LOGIN:      (JobClass.RUN_LATEST, time(8, 10)),
        # WO-7 storage housekeeping: post-EOD, after the nightly review's 21:00 slot.
        opsmain.JOB_TICK_COMPACT:      (JobClass.DATE_KEYED, time(22, 30)),
        # §10.5 DuckDB export (2026-10-11): after the 21:00 SQLite backup, before compaction.
        opsmain.JOB_MARKET_BACKUP:     (JobClass.RUN_LATEST, time(22, 0)),
    }
    for jid, (cls, at) in expected.items():
        assert by_id[jid].job_class == cls, jid
        assert by_id[jid].at == at, jid
    assert opsmain.JOB_MARKET_BACKUP in PHASE1_JOB_IDS
    assert opsmain.JOB_MARKET_BACKUP not in POST_ARM_JOB_IDS

    # Write→read dependencies as RELATIONS, not just literals — the 2026-07-24 reschedule moved
    # deals/corp_actions past features' 18:50 and the literal-only table above stayed green while
    # features read pre-write data for a month (found 2026-08-18). A future reschedule must trip
    # these, not just edit two lines of the dict.
    assert by_id[opsmain.JOB_DEALS].at < by_id[opsmain.JOB_FEATURES].at            # flags → features
    assert by_id[opsmain.JOB_CORP_ACTIONS].at < by_id[opsmain.JOB_FEATURES].at     # ex-dates → features
    assert by_id[opsmain.JOB_DEALS].order < by_id[opsmain.JOB_FEATURES].order      # catch-up sequence
    # Filing listings → their XBRL line items; and the minutes-long fetch batch never runs in boot.
    assert by_id[opsmain.JOB_FILINGS_RESULTS].at < by_id[opsmain.JOB_RESULTS_LINE_ITEMS].at
    assert opsmain.JOB_RESULTS_LINE_ITEMS in opsmain.POST_ARM_JOB_IDS
    # The ISIN map feeds the shareholding job (which fetches mapped symbols only), on the clock and
    # in catch-up order; the census judges the EOD feeds after they land, and after a boot's catch-up.
    assert by_id[opsmain.JOB_ISIN_MAP].at < by_id[opsmain.JOB_FILINGS_SHP].at
    assert by_id[opsmain.JOB_ISIN_MAP].order < by_id[opsmain.JOB_FILINGS_SHP].order
    assert max(by_id[j].at for j in (opsmain.JOB_DEALS, opsmain.JOB_FEATURES, opsmain.JOB_CORP_ACTIONS)) \
        < by_id[opsmain.JOB_FEED_FRESHNESS].at
    assert opsmain.JOB_FEED_FRESHNESS in opsmain.POST_ARM_JOB_IDS
    assert by_id[opsmain.JOB_CLAUDE_LOGIN].at < by_id[opsmain.JOB_NEWS_CHAIN].at


def test_instruments_runs_before_surveillance_in_dependency_order() -> None:
    """A10/A8: instruments dump (tick sizes) must precede surveillance within the safety-critical class."""
    reg = build_job_registry(load_settings(), _all_noop_fns())
    safety = [s.job_id for s in reg.specs(JobClass.SAFETY_CRITICAL)]
    assert safety.index(JOB_INSTRUMENTS) < safety.index(opsmain.JOB_SURVEILLANCE)


def test_sector_map_fires_only_on_sunday() -> None:
    """§4.4 job 13: the sector/theme map refresh is a weekly Sunday cadence, not a trading-day job."""
    reg = build_job_registry(load_settings(), _all_noop_fns())
    sector = next(s for s in reg.specs() if s.job_id == JOB_SECTOR_MAP)
    assert sector.fire_day is not None
    assert sector.fire_day(date(2026, 7, 12)) is True    # Sunday
    assert sector.fire_day(date(2026, 7, 13)) is False   # Monday
    # Every other job leaves fire_day defaulted (the calendar trading-day guard applies).
    assert all(s.fire_day is None for s in reg.specs() if s.job_id != JOB_SECTOR_MAP)


def test_missing_job_fn_is_a_loud_wiring_error() -> None:
    fns = _all_noop_fns()
    del fns[JOB_EARNINGS]
    with pytest.raises(KeyError):
        build_job_registry(load_settings(), fns)


# --------------------------------------------------------------------------- composition-root closure seam


def _spec(engine: opsmain.Engine, job_id: str) -> JobSpec:
    return next(s for s in engine.registry.specs() if s.job_id == job_id)


def _spy(monkeypatch, name: str, built: dict[str, list], *, build=None) -> None:
    """Constructor spy on ``opsmain.<name>`` (``compose`` looks it up at call time): records
    ``(args, kwargs, obj)`` per construction and returns the real object, or ``build``'s."""
    real = getattr(opsmain, name)

    def construct(*args, **kwargs):
        obj = (build or real)(*args, **kwargs)
        built.setdefault(name, []).append((args, kwargs, obj))
        return obj

    monkeypatch.setattr(opsmain, name, construct)


async def test_bhavcopy_composition_root_closure_forwards_degraded_result(
    tmp_path, monkeypatch, migrated_db_template, instant_backoff
) -> None:
    """Composition-root regression (2026-08-13): ``engine.ops.main``'s ``job_bhavcopy`` closure MUST
    forward ``BhavcopyJob.run()``'s return value to ``spec.run`` — the 2026-08-12 live bug was exactly
    this closure discarding it (``await bhavcopy.run(d)`` with no ``return``), which meant the
    ``_job_result_ok`` watermark fix never saw the degraded result because ``spec.run`` was always
    ``None`` regardless of what the underlying job returned.

    Through the REAL pieces: the composed engine's own ``BhavcopyJob`` degraded by a failing network
    under its shared HTTP client, its registry's ``job_bhavcopy``, the real ``_scheduled_runner``."""
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("nse unreachable", request=request)

    async with composed_engine(tmp_path, monkeypatch, db_template=migrated_db_template,
                               transport=httpx.MockTransport(handler)) as c:
        e = c.engine
        await _scheduled_runner(_spec(e, opsmain.JOB_BHAVCOPY), e.catch_up, e.clock)()
        today = e.clock.today()
        row = e.conn.execute(
            "SELECT status FROM job_runs WHERE job_id=? AND run_for_date=?",
            (opsmain.JOB_BHAVCOPY, today.isoformat()),
        ).fetchone()
        assert row["status"] == "failed"                                # not the pre-fix "success"
        assert e.catch_up.was_run(opsmain.JOB_BHAVCOPY, today) is False


#: The 9 composition-root closures forwarded (2026-08-13) so an ok-bearing job result reaches
#: JobSpec.run instead of being discarded to None (the bhavcopy seam above, closed for 8 more jobs),
#: plus job_reconcile (2026-10-11: a token abort must sink the watermark, not record success).
#: wrapper -> (job id, the job class whose ``run`` it forwards, whether the job is date-keyed).
_FORWARDING_WRAPPERS: dict[str, tuple[str, type, bool]] = {
    "job_bhavcopy": (opsmain.JOB_BHAVCOPY, opsmain.BhavcopyJob, True),
    "job_earnings": (opsmain.JOB_EARNINGS, opsmain.EarningsCalendarJob, False),
    "job_corp_actions": (opsmain.JOB_CORP_ACTIONS, opsmain.CorpActionsJob, False),
    "job_sector_map": (opsmain.JOB_SECTOR_MAP, opsmain.SectorMapJob, False),
    "job_filings_shp": (opsmain.JOB_FILINGS_SHP, opsmain.FilingsShpJob, False),
    "job_deals": (opsmain.JOB_DEALS, opsmain.DealsJob, True),
    "job_filings_pit": (opsmain.JOB_FILINGS_PIT, opsmain.FilingsPitJob, True),
    "job_filings_pit_fresh": (opsmain.JOB_FILINGS_PIT_FRESH, opsmain.FilingsPitFreshJob, True),
    "job_filings_results": (opsmain.JOB_FILINGS_RESULTS, opsmain.FilingsResultsJob, True),
    "job_reconcile": (opsmain.JOB_RECONCILE, opsmain.ReconcileJob, True),
}


async def test_sector_map_wrapper_classifies_the_batch_universe(
    tmp_path, monkeypatch, migrated_db_template
) -> None:
    """O15 (2026-09-04): ``job_sector_map`` must feed sector_map the day's BATCH universe, not the
    tick watchlist. With the eligible set at NIFTY 500 and ``universe_max_watchlist`` still 200, a
    watchlist-scoped run leaves every capped/extended name unclassified — and §7.1 caps the
    UNCLASSIFIED bucket at 1 open position, so the widened swing legs would be gate-blocked."""
    received: list[tuple[date, list[str]]] = []

    async def run(self, d: date, *, universe_symbols=None) -> SectorMapResult:
        received.append((d, list(universe_symbols)))
        return SectorMapResult(as_of=d, ok=False, reason="recorded")

    monkeypatch.setattr(SectorMapJob, "run", run)
    async with composed_engine(tmp_path, monkeypatch, db_template=migrated_db_template) as c:
        today = c.engine.clock.today()
        c.engine.store.upsert_universe_daily([
            {"d": today, "symbol": "WATCHA", "included": True, "exclusion_reasons": None},
            {"d": today, "symbol": "WATCHB", "included": True, "exclusion_reasons": None},
            {"d": today, "symbol": "CAPPED", "included": False, "exclusion_reasons": ["watchlist_cap"]},
            {"d": today, "symbol": "EXTENDED", "included": False, "exclusion_reasons": ["not_in_index"]},
            {"d": today, "symbol": "ILLIQUID", "included": False, "exclusion_reasons": ["illiquid"]},
        ])
        spec = next(s for s in c.engine.registry.specs() if s.job_id == JOB_SECTOR_MAP)
        result = await spec.run()

    assert received == [(today, ["CAPPED", "EXTENDED", "WATCHA", "WATCHB"])]
    assert result.reason == "recorded"           # the wrapper forwards the job's verdict


@pytest.mark.parametrize("wrapper_name", _FORWARDING_WRAPPERS)
async def test_ok_bearing_wrapper_forwards_return_value(
    wrapper_name: str, tmp_path, monkeypatch, migrated_db_template
) -> None:
    """Composition-root regression (2026-08-13), swept over all ok-bearing closures: each MUST
    ``return await <job>.run(...)``, not a bare ``await`` that discards the result and always returns
    None to ``JobSpec.run`` — the exact seam ``test_bhavcopy_composition_root_closure_forwards_degraded_result``
    pins end-to-end for bhavcopy alone. Here the composed engine's registry entry is run against its
    job class's ``run`` returning a marker: the wrapper must hand back that very object."""
    job_id, job_class, date_keyed = _FORWARDING_WRAPPERS[wrapper_name]
    verdict = object()

    async def run(self, *args, **kwargs):
        return verdict

    monkeypatch.setattr(job_class, "run", run)
    # job_reconcile asks the broker only with a valid token (a known-dead one is answered locally).
    async with composed_engine(tmp_path, monkeypatch, db_template=migrated_db_template, kite=True,
                               secrets={KITE_ACCESS_TOKEN: "token"}) as c:
        spec = _spec(c.engine, job_id)
        args = (c.engine.clock.today(),) if date_keyed else ()
        assert await spec.run(*args) is verdict, f"{wrapper_name} does not forward its job's return value"


# --------------------------------------------------------------------------- reconcile retry (2026-10-11)
_RECONCILE_TOKENS = {"A": 1, "B": 2, "C": 3}


class _ReconcileKite:
    """Official 09:15 candle per call; raises TokenException once more than ``dead_after`` calls
    have been made (``None`` = healthy). ``calls`` records the instrument tokens fetched."""

    def __init__(self, dead_after: int | None = None) -> None:
        self.dead_after = dead_after
        self.calls: list[int] = []

    async def historical(self, token, frm, to, interval):
        self.calls.append(token)
        if self.dead_after is not None and len(self.calls) > self.dead_after:
            raise TokenException("Incorrect api_key or access_token")
        minute = datetime.combine(frm.date(), time(9, 15), tzinfo=IST)
        return [{"date": minute, "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0, "volume": 100}]


def _reconcile_world(monkeypatch, kite: _ReconcileKite):
    """The composed engine's own ``ReconcileJob`` on the broker stand-in and a three-symbol token map,
    with one 09:15 bar per symbol in its market store."""
    real = opsmain.ReconcileJob

    def build(store, _kite, clock, settings, **kwargs):
        return real(store, kite, clock, settings, **{**kwargs, "token_for_symbol": _RECONCILE_TOKENS.get})

    monkeypatch.setattr(opsmain, "ReconcileJob", build)

    def market(store) -> None:
        d = FIXED_NOW.date()
        store.insert_bars_1m([
            Bar(symbol=s, ts_minute=datetime.combine(d, time(9, 15), tzinfo=IST), open=Decimal("10"),
                high=Decimal("10"), low=Decimal("10"), close=Decimal("10"), volume=100, src="self")
            for s in _RECONCILE_TOKENS
        ])

    return market


def _reconcile_status(conn, d: date) -> str | None:
    row = conn.execute(
        "SELECT status FROM job_runs WHERE job_id=? AND run_for_date=?",
        (opsmain.JOB_RECONCILE, d.isoformat()),
    ).fetchone()
    return row["status"] if row else None


async def test_reconcile_composition_root_closure_records_failed_on_token_abort(
    tmp_path, monkeypatch, migrated_db_template
) -> None:
    """The live gap: job_reconcile discarded ReconcileJob.run's report, so a token abort recorded a
    SUCCESS watermark and the rest of the day was never reconciled. Through the composed engine's
    closure and registry and the real _scheduled_runner, the abort must record 'failed'; the next
    catch-up pass after re-login retries the day and fetches ONLY the symbols the abort left."""
    kite = _ReconcileKite(dead_after=1)
    market = _reconcile_world(monkeypatch, kite)
    async with composed_engine(tmp_path, monkeypatch, db_template=migrated_db_template, kite=True,
                               secrets={KITE_ACCESS_TOKEN: "token"}, market=market) as c:
        e = c.engine
        today = e.clock.today()
        await _scheduled_runner(_spec(e, opsmain.JOB_RECONCILE), CatchUpRunner(e.conn, e.clock, e.calendar),
                                e.clock)()
        assert _reconcile_status(e.conn, today) == "failed"
        assert [r["symbol"] for r in e.store.get_reconcile_log(today)] == ["A"]

        kite.dead_after, kite.calls = None, []
        after_eod = Clock(time_source=lambda: datetime.combine(today, time(16, 0), tzinfo=IST))
        sweep = CatchUpRunner(
            e.conn, after_eod, e.calendar, e.registry.select(lambda s: s.job_id == opsmain.JOB_RECONCILE),
        )
        result = await sweep.catch_up(scope=CatchUpScope.ALL)
        assert result.jobs_caught_up == [f"{opsmain.JOB_RECONCILE}:{today.isoformat()}"]
        assert kite.calls == [_RECONCILE_TOKENS["B"], _RECONCILE_TOKENS["C"]]
        assert _reconcile_status(e.conn, today) == "success"
        assert all(r["bars_compared"] == 1 for r in e.store.get_reconcile_log(today))


async def test_reconcile_dead_token_gate_no_broker_call_no_freeze_no_alert_storm(
    tmp_path, monkeypatch, migrated_db_template
) -> None:
    """A known-dead token: zero broker calls per sweep, the day stays 'failed' (DATE_KEYED never
    freezes), the CATCHUP_REPORT goes out once for the unchanged failure, and the first sweep after
    re-login reconciles the whole day."""
    kite = _ReconcileKite()
    market = _reconcile_world(monkeypatch, kite)
    async with composed_engine(tmp_path, monkeypatch, db_template=migrated_db_template, kite=True,
                               market=market) as c:     # no access token: the session knows it is dead
        e = c.engine
        today = e.clock.today()
        after_eod = Clock(time_source=lambda: datetime.combine(today, time(16, 0), tzinfo=IST))
        freezes: list[str] = []
        sent: list = []

        async def freeze(reason: str) -> None:
            freezes.append(reason)

        async def notify(msg) -> None:
            sent.append(msg)

        registry = e.registry.select(lambda s: s.job_id == opsmain.JOB_RECONCILE)
        sweep = CatchUpRunner(e.conn, after_eod, e.calendar, registry, freeze=freeze, notify=notify)
        for _ in range(2):
            result = await sweep.catch_up(scope=CatchUpScope.ALL)
            assert result.jobs_failed == [f"{opsmain.JOB_RECONCILE}:{today.isoformat()}"]
            assert result.frozen_reasons == []
        assert kite.calls == []
        assert freezes == []
        assert len(sent) == 1                                     # unchanged failure set is not re-sent
        assert _reconcile_status(e.conn, today) == "failed"
        assert e.store.get_reconcile_log(today) == []

        monkeypatch.setattr(e.session, "token_valid", lambda: True)      # the owner re-logs in
        result = await sweep.catch_up(scope=CatchUpScope.ALL)
        assert result.jobs_caught_up == [f"{opsmain.JOB_RECONCILE}:{today.isoformat()}"]
        assert kite.calls == list(_RECONCILE_TOKENS.values())
        assert _reconcile_status(e.conn, today) == "success"


# --------------------------------------------------------------------------- scheduled_runner watermark


@pytest.mark.asyncio
async def test_scheduled_runner_records_watermark_on_success(conn, clock, calendar) -> None:
    catch_up = CatchUpRunner(conn, clock, calendar)
    seen: list[date] = []

    async def run_it(d: date) -> None:
        seen.append(d)

    spec = JobSpec(JOB_BHAVCOPY, JobClass.DATE_KEYED, time(18, 0), run_it, order=1)
    await _scheduled_runner(spec, catch_up, clock)()

    assert seen == [clock.today()]                       # date-keyed runner got today's date
    assert catch_up.was_run(JOB_BHAVCOPY, clock.today())  # watermark recorded -> catch-up skips it


@pytest.mark.asyncio
async def test_scheduled_runner_marks_failure_without_crashing(conn, clock, calendar) -> None:
    catch_up = CatchUpRunner(conn, clock, calendar)

    async def boom() -> None:
        raise RuntimeError("job blew up")

    spec = JobSpec(JOB_UNIVERSE, JobClass.RUN_LATEST, time(8, 30), boom, order=1)
    await _scheduled_runner(spec, catch_up, clock)()      # must not raise into the scheduler loop

    # A failed run is NOT a success watermark, so the CatchUpRunner will retry it on next startup.
    assert catch_up.was_run(JOB_UNIVERSE, clock.today()) is False


class _OkResult:
    """A minimal stand-in for a job's ``ok``-bearing return (e.g. ``BhavcopyResult``)."""

    def __init__(self, ok: bool) -> None:
        self.ok = ok


@pytest.mark.asyncio
async def test_scheduled_runner_records_failed_status_on_notok_result(conn, clock, calendar) -> None:
    """2026-08-12 live bug: a job that degrades-without-raising (returns ``ok=False``) must sink the
    watermark exactly like an exception — this is the bhavcopy shape (BhavcopyJob.run never raises,
    E5). Before the fix this hit ``record_run`` with its default ``status='success'``."""
    catch_up = CatchUpRunner(conn, clock, calendar)

    async def run_it(d: date) -> _OkResult:
        return _OkResult(ok=False)

    spec = JobSpec(JOB_BHAVCOPY, JobClass.DATE_KEYED, time(18, 0), run_it, order=1)
    await _scheduled_runner(spec, catch_up, clock)()

    today = clock.today()
    row = conn.execute(
        "SELECT status, last_success_at FROM job_runs WHERE job_id=? AND run_for_date=?",
        (JOB_BHAVCOPY, today.isoformat()),
    ).fetchone()
    assert row["status"] == "failed"
    assert row["last_success_at"] is None
    assert catch_up.was_run(JOB_BHAVCOPY, today) is False


@pytest.mark.asyncio
async def test_scheduled_runner_records_success_for_ok_true_result(conn, clock, calendar) -> None:
    """A job returning an explicit ``ok=True`` result (not just ``None``) still records success."""
    catch_up = CatchUpRunner(conn, clock, calendar)

    async def run_it(d: date) -> _OkResult:
        return _OkResult(ok=True)

    spec = JobSpec(JOB_BHAVCOPY, JobClass.DATE_KEYED, time(18, 0), run_it, order=1)
    await _scheduled_runner(spec, catch_up, clock)()

    assert catch_up.was_run(JOB_BHAVCOPY, clock.today()) is True


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "result",
    [TickCompactionResult(skipped_in_flight=True), TickCompactionResult(stopped=True)],
    ids=["another_run_holds_the_lock", "stopped_at_the_window_or_shutdown"],
)
async def test_scheduled_runner_records_nothing_for_an_unfinished_run(
    conn, clock, calendar, result: TickCompactionResult
) -> None:
    """2026-09-29: every ``tick_compact`` watermark from 09-22 to 09-28 was a 22:30 fire that found
    a catch-up compaction in flight, returned ``ok=True`` and was recorded as success. An unfinished
    run records nothing, so the day stays missed and the next pass replays it."""
    catch_up = CatchUpRunner(conn, clock, calendar)

    async def run_it(d: date) -> TickCompactionResult:
        return result

    spec = JobSpec(opsmain.JOB_TICK_COMPACT, JobClass.DATE_KEYED, time(22, 30), run_it, order=90)
    await _scheduled_runner(spec, catch_up, clock)()

    assert conn.execute("SELECT count(*) FROM job_runs").fetchone()[0] == 0


# ---------------------------------------- scheduled fire vs today's watermark (§2.6 early hydration)
# Owner-directed 2026-09-09, after review REVERSED the first draft: the scheduled fire stays
# UNCONDITIONAL. A draft skipped any job already watermarked for today, and that pinned the 06:30
# digest/plan — and a universe built on a pre-08:00 instruments dump — for the whole day. Results and
# exchange filings published 07:00–09:00 IST are the largest catalyst class, so on a day the PC stays
# awake the 08:25/08:35/08:50 fires MUST run again (fresher digest and plan; the universe rebuilt
# behind the 08:15 instruments refresh). The watermark ``hydrate_ahead`` records guards the SLEEP case
# only — a PC that sleeps through the fire times and wakes at 09:30 finds the sweep satisfied and
# keeps the 06:30 run. A second pre-open plan message on an awake-PC early-login day is accepted.


@pytest.mark.asyncio
async def test_scheduled_fire_runs_even_when_today_is_already_watermarked(
    conn, clock, calendar
) -> None:
    ran: list[str] = []

    async def run_it() -> None:
        ran.append("planner")

    catch_up = CatchUpRunner(conn, clock, calendar)
    catch_up.record_run(JOB_PREOPEN_PLANNER, clock.today())      # e.g. an early-hydration pass ran it
    spec = JobSpec(JOB_PREOPEN_PLANNER, JobClass.RUN_LATEST, time(8, 50), run_it, order=28)

    await _scheduled_runner(spec, catch_up, clock)()

    assert ran == ["planner"]                                    # a fresher plan, not a silenced fire
    assert catch_up.was_run(JOB_PREOPEN_PLANNER, clock.today()) is True


# --------------------------------------------------------------------------- scheduler arming (same registry)


def test_same_registry_arms_every_job_on_the_live_scheduler(conn, clock, calendar) -> None:
    """§2.6/§10.1: the live Scheduler and the CatchUpRunner are fed the SAME JobRegistry — arming must
    register every Phase-1 job id on APScheduler (the sector map as a weekly Sunday cron, the rest as
    calendar-guarded trading-day jobs)."""
    settings = load_settings()
    reg = build_job_registry(settings, _all_noop_fns())
    catch_up = CatchUpRunner(conn, clock, calendar, reg)
    sched = Scheduler(clock, calendar)

    _arm_registry_jobs(sched, reg, catch_up, clock)

    armed = {j.id for j in sched._sched.get_jobs()}
    assert set(PHASE1_JOB_IDS) <= armed
    # The weekly sector-map job carries a day-of-week cron; the daily ones do not.
    sector_trigger = str(next(j for j in sched._sched.get_jobs() if j.id == JOB_SECTOR_MAP).trigger)
    assert "day_of_week='sun'" in sector_trigger


def test_pre_open_token_check_is_armed_at_0840(clock, calendar) -> None:
    """WO-21 (iii): the pre-open token probe is a calendar-guarded 08:40 daily fire, and it is
    deliberately NOT a registry job — a watermark would make a pre-open check catch-up-eligible,
    and a token check replayed at 14:00 answers a question nobody is asking."""
    from engine.ops.token_check import TokenCheckJob

    sched = Scheduler(clock, calendar)
    job = TokenCheckJob(kite=object(), clock=clock, calendar=calendar, notify=None)

    opsmain._arm_token_check(sched, job)

    armed = {j.id for j in sched._sched.get_jobs()}
    assert "token_check" in armed
    trigger = str(next(j for j in sched._sched.get_jobs() if j.id == "token_check").trigger)
    assert "hour='8'" in trigger and "minute='40'" in trigger
    assert "token_check" not in set(PHASE1_JOB_IDS) | set(POST_ARM_JOB_IDS)


def test_live_interval_jobs_are_armed(clock, calendar) -> None:
    """The always-on interval jobs (not calendar-gated): coarse bar finalization, health, per-feed news."""
    settings = load_settings()
    sched = Scheduler(clock, calendar)

    async def _resolve_news(_hs) -> None:
        return None

    _arm_live_jobs(sched, settings, bar_builder=None, health=None,
                   news_ingest=None, resolve_news=_resolve_news,
                   ticker=object(), calendar=calendar, clock=clock)

    armed = {j.id for j in sched._sched.get_jobs()}
    # One news_poll_<name> job per configured RSS feed, plus GDELT and the NSE announcements feed.
    assert {"bar_advance", "health_check", "feed_stats", "news_poll_gdelt"} <= armed
    assert {f"news_poll_{name}" for name in settings.news.feeds.rss} <= armed
    ann = settings.news.feeds.nse_announcements
    assert ann.enabled and "news_poll_nse_ann" in armed
    trigger = str(next(j for j in sched._sched.get_jobs() if j.id == "news_poll_nse_ann").trigger)
    assert str(timedelta(seconds=ann.poll_s)) in trigger   # its own cadence, not the RSS default


def test_holdings_reconcile_is_armed_hourly(clock, calendar) -> None:
    """§3.6 holdings reconcile (2026-09-07): an hourly in-session tick. The cadence is the point —
    the check answers "did the owner sell this?", which is worth asking a few times a session and
    pointless to ask every minute."""
    settings = load_settings()
    sched = Scheduler(clock, calendar)

    async def _resolve_news(_hs) -> None:
        return None

    async def _holdings_reconcile_tick() -> None:
        return None

    _arm_live_jobs(sched, settings, bar_builder=None, health=None,
                   news_ingest=None, resolve_news=_resolve_news,
                   ticker=object(), calendar=calendar, clock=clock,
                   holdings_reconcile_tick=_holdings_reconcile_tick)

    jobs = {j.id: j for j in sched._sched.get_jobs()}
    assert "holdings_reconcile" in jobs
    assert str(timedelta(seconds=3600)) in str(jobs["holdings_reconcile"].trigger)


def test_holdings_reconcile_is_not_armed_without_a_broker(clock, calendar) -> None:
    """No api_key ⇒ no KiteClient ⇒ nothing to reconcile against: the tick is simply absent (the
    same posture as every other optional interval job)."""
    settings = load_settings()
    sched = Scheduler(clock, calendar)

    async def _resolve_news(_hs) -> None:
        return None

    _arm_live_jobs(sched, settings, bar_builder=None, health=None,
                   news_ingest=None, resolve_news=_resolve_news,
                   ticker=object(), calendar=calendar, clock=clock)

    assert "holdings_reconcile" not in {j.id for j in sched._sched.get_jobs()}


@pytest.mark.parametrize(("keyword", "seconds"), [("protection_reminder_tick", 300), ("paper_tick", 30)])
@pytest.mark.parametrize("wired", [True, False])
def test_optional_interval_ticks_are_armed_at_their_cadence_only_when_wired(
    clock, calendar, keyword, seconds, wired
) -> None:
    sched = Scheduler(clock, calendar)

    async def _resolve_news(_hs) -> None:
        return None

    async def _tick() -> None:
        return None

    _arm_live_jobs(sched, load_settings(), bar_builder=None, health=None,
                   news_ingest=None, resolve_news=_resolve_news,
                   ticker=object(), calendar=calendar, clock=clock,
                   **{keyword: _tick if wired else None})

    jobs = {j.id: j for j in sched._sched.get_jobs()}
    assert (keyword in jobs) is wired
    if wired:
        assert str(timedelta(seconds=seconds)) in str(jobs[keyword].trigger)


async def test_paper_tick_is_wired_only_when_the_paper_runtime_was_built(
    tmp_path, monkeypatch, migrated_db_template
) -> None:
    """The subsystem is enabled both times; with the limit table unverifiable the paper runtime fails
    to build, and then no paper tick may be armed."""
    for registered in (True, False):
        async with composed_engine(tmp_path / str(registered), monkeypatch, db_template=migrated_db_template,
                                   paper=True, registered=registered) as c:
            job = c.engine.scheduler._sched.get_job("paper_tick")
            assert (c.engine.paper is not None) is registered
            assert (job is not None) is registered
            if registered:
                assert job.func == c.engine.paper.runtime.paper_tick


async def test_clusterer_and_digest_read_the_one_cat_envelope_row(
    tmp_path, monkeypatch, migrated_db_template
) -> None:
    """The clusterer window and the digest horizon are the SAME envelope row
    ``cat.max_event_age_days`` off the same boot ``envelope_state`` read (the settings key is gone)."""
    from engine.datafeeds.news_pipeline import load_cat_params

    def promoted(conn) -> None:      # a §6.4 promotion moved the row off its envelope.yaml default
        conn.execute("INSERT INTO envelope_state (parameter, value, bounds_sha256, set_by) "
                     "VALUES ('cat.max_event_age_days', '5', 'x', 'promotion')")

    built: dict[str, list] = {}
    _spy(monkeypatch, "HeadlineClusterer", built)
    _spy(monkeypatch, "CatalystDigestJob", built)
    async with composed_engine(tmp_path, monkeypatch, db_template=migrated_db_template, state_db=promoted):
        pass
    ((_, clusterer_kw, _),) = built["HeadlineClusterer"]
    ((_, digest_kw, _),) = built["CatalystDigestJob"]
    assert clusterer_kw["max_event_age_days"] == 5
    assert int(load_cat_params(digest_kw["envelope"])["max_event_age_days"]) == 5
    # No envelope_state row today ⇒ the envelope.yaml default, which equals the retired settings 2.
    assert int(load_cat_params(None)["max_event_age_days"]) == 2
    # A leftover settings.yaml key is ignored (CatCfg/Settings keep pydantic's extra="ignore").
    assert not hasattr(Settings.model_validate({"cat": {"max_event_age_days": 2}}).cat, "max_event_age_days")


def test_nse_announcements_job_is_not_armed_when_disabled(clock, calendar) -> None:
    """`enabled: false` is the owner's off switch — the poll job must not exist at all."""
    settings = load_settings()
    settings.news.feeds.nse_announcements.enabled = False
    sched = Scheduler(clock, calendar)

    async def _resolve_news(_hs) -> None:
        return None

    _arm_live_jobs(sched, settings, bar_builder=None, health=None,
                   news_ingest=None, resolve_news=_resolve_news,
                   ticker=object(), calendar=calendar, clock=clock)

    assert "news_poll_nse_ann" not in {j.id for j in sched._sched.get_jobs()}


# --------------------------------------------------------------------------- brk20 feature-snapshot mint
def _brk20_cand(symbol: str):
    from decimal import Decimal

    from engine.strategy.types import RawLevels, SignalCandidate

    return SignalCandidate(
        signal_id=f"sig-{symbol}", strategy_id="brk20", symbol=symbol, side="BUY", style="swing",
        raw_levels=RawLevels(entry=Decimal("100.00"), stop=Decimal("95.00"), target=Decimal("110.00")),
        score=0.6,
    )


def test_attach_feature_snapshots_mints_one_id_per_candidate() -> None:
    """2026-08-04 defect: batch-rule (brk20) candidates reached the analyst with
    features_snapshot_id: null, and intraday.py Rule 6 mandates no_action on a missing id — every
    brk20 candidate was structurally un-recommendable. The composition root must mint the same
    §4.3 feature link the per-bar ScanContext path mints."""
    from types import SimpleNamespace

    minted: list[str] = []

    class FakeFeatures:
        def intraday_snapshot(self, symbol):
            minted.append(symbol)
            return SimpleNamespace(features_snapshot_id=f"snap-{symbol}")

    cands = [_brk20_cand("BPCL"), _brk20_cand("COALINDIA")]
    out = opsmain._attach_feature_snapshots(FakeFeatures(), cands)

    assert [c.features_snapshot_id for c in out] == ["snap-BPCL", "snap-COALINDIA"]
    assert minted == ["BPCL", "COALINDIA"]          # one mint per admitted candidate
    assert [c.signal_id for c in out] == [c.signal_id for c in cands]  # everything else unchanged
    assert all(c.features_snapshot_id is None for c in cands)          # frozen inputs not mutated


def test_attach_feature_snapshots_degrades_to_none_never_raises() -> None:
    """Scan-path posture (§3.2.5): a FeatureEngine failure costs that candidate its feature link,
    never the sweep. Mixed batch: the healthy symbol still gets its id."""

    class FlakyFeatures:
        def intraday_snapshot(self, symbol):
            from types import SimpleNamespace

            if symbol == "BPCL":
                raise RuntimeError("boom")
            return SimpleNamespace(features_snapshot_id=f"snap-{symbol}")

    out = opsmain._attach_feature_snapshots(FlakyFeatures(), [_brk20_cand("BPCL"), _brk20_cand("COALINDIA")])
    assert [c.features_snapshot_id for c in out] == [None, "snap-COALINDIA"]


# --------------------------------------------------------------------------- brk20 RETEST re-arm (WO-R)
#: A retest tick's day — Thu 2026-06-18, the session after the 06-17 crossing used in test_retest.py.
RETEST_D = date(2026, 6, 18)


class _FakeRestingBook:
    """Records what the tick asked of the book; returns a canned ``due`` list."""

    def __init__(self, due: list | None = None) -> None:
        self._due = list(due or [])
        self.expired: list[date] = []
        self.due_calls: list[tuple] = []
        self.due_kwargs: list[dict] = []

    def expire(self, today: date) -> int:
        self.expired.append(today)
        return 0

    def due(self, ltp_fn, band_pct, today: date, **kwargs) -> list:
        self.due_calls.append((ltp_fn, band_pct, today))
        self.due_kwargs.append(kwargs)
        offers = kwargs.get("offers")
        for cand in self._due:
            if offers is not None:
                offers[cand.signal_id] = {"symbol": cand.symbol, "entry": "100.00",
                                          "ltp": "100.20", "dev": "0.20",
                                          "signal_d": "2026-06-17"}
        return list(self._due)


def _retest_kwargs(**overrides):
    """The tick's non-book arguments, defaulted to the ordinary in-window minute."""
    base = dict(
        active=True, sweep_ready=True,
        ltp_fn=lambda _s: Decimal("100.00"), tick_age_fn=lambda _s: 1.0,
        limits_fn=lambda: (2.0, 5.0), skip_fn=lambda _syms: {}, today=RETEST_D,
    )
    base.update(overrides)
    return base


class _FakeScreen:
    """``SignalPreScreen.admit``'s surface only. ``accept=None`` admits everything."""

    def __init__(self, accept: set[str] | None = None) -> None:
        self.calls: list[tuple] = []
        self._accept = accept

    def admit(self, cands, day: date, *, in_window: bool = True) -> list:
        self.calls.append((list(cands), day, in_window))
        return [c for c in cands if self._accept is None or c.symbol in self._accept]


def _snapshot_features(minted: list[str]):
    from types import SimpleNamespace

    class _F:
        def intraday_snapshot(self, symbol):
            minted.append(symbol)
            return SimpleNamespace(features_snapshot_id=f"snap-{symbol}")

    return _F()


def test_the_retest_rearm_does_nothing_at_all_outside_the_trade_window() -> None:
    """The re-arm rides the 60 s forward-drain pulse, which runs all day; the window verdict is the
    sweep's own `_sweep_window_active` (a live session, inside the owner trade window, in a mode that
    originates). Outside it NOTHING happens — not the expiry sweep, not the journal read, and above
    all not a `prescreen.admit` that would spend an unrefundable §3.2.5 day slot on a candidate the
    pipeline is guaranteed to drop as `signal_candidate_out_of_window`."""
    book, screen = _FakeRestingBook([_brk20_cand("AAA")]), _FakeScreen()
    minted: list[str] = []

    out = _retest_republish(book, screen, _snapshot_features(minted), **_retest_kwargs(active=False))

    assert out == []
    assert (book.expired, book.due_calls, screen.calls, minted) == ([], [], [], [])


def test_the_retest_rearm_waits_for_todays_in_window_sweep() -> None:
    """`sweep_ready` is "today's IN-WINDOW sweep has published". Until it has, the tick only
    EXPIRES. Two reasons, both load-bearing: (1) the pre-screen's `(symbol, strategy)` dedupe is
    first-come, so a level broken days ago landing ahead of the day's ranked admission would take the
    slot and today's fresh crossing of the same symbol would be dropped as a duplicate — and, being
    un-admitted, never journalled either, leaving the book on the stale geometry all window; (2) the
    skip screen reads the sweep's own eligible set, which is EMPTY before it has run."""
    book, screen = _FakeRestingBook([_brk20_cand("AAA")]), _FakeScreen()

    out = _retest_republish(book, screen, _snapshot_features([]),
                            **_retest_kwargs(sweep_ready=False))

    assert out == [] and screen.calls == [] and book.due_calls == []
    assert book.expired == [RETEST_D]     # …housekeeping still runs; it spends no slot


def test_the_retest_rearm_admits_through_the_same_prescreen_and_mints_after_it() -> None:
    """A re-publication is not a bypass: it faces the SAME `prescreen.admit` — day cap, per-strategy
    cap, same-day dedupe — as a fresh sweep candidate, with `in_window=True` stating the window
    verdict the pre-screen (Clock-free by design) cannot take itself.

    The §4.3 snapshot is minted AFTER that admit, the sweep's own discipline: a candidate the caps
    suppress must never spend a snapshot write, and a candidate with a null id is structurally
    un-recommendable (intraday.py Rule 6), so it cannot simply be left off either."""
    due = [_brk20_cand("AAA"), _brk20_cand("BBB")]
    book, screen = _FakeRestingBook(due), _FakeScreen(accept={"AAA"})
    minted: list[str] = []

    def ltp(_symbol: str) -> Decimal:    # identity matters below — the tick cache is passed THROUGH
        return Decimal("100.00")

    def age(_symbol: str) -> float:
        return 1.0

    def skip(_symbols):
        return {}

    out = _retest_republish(
        book, screen, _snapshot_features(minted),
        **_retest_kwargs(ltp_fn=ltp, tick_age_fn=age, skip_fn=skip),
    )

    assert [c.symbol for c in out] == ["AAA"]
    assert [c.features_snapshot_id for c in out] == ["snap-AAA"]
    assert minted == ["AAA"]                       # the suppressed candidate spent nothing
    assert book.expired == [RETEST_D]              # expire ran BEFORE the read
    assert book.due_calls == [(ltp, 2.0, RETEST_D)]   # the live tick cache + the live band, verbatim
    # …and the two NARROWINGS ride the same call: the gate's own staleness bound on the trigger
    # price, and the caller's not-offerable screen. Both from ONE limits load.
    kwargs = book.due_kwargs[0]
    assert (kwargs["tick_age_fn"], kwargs["max_tick_age_s"], kwargs["skip_fn"]) == (age, 5.0, skip)
    assert screen.calls == [(due, RETEST_D, True)]


def test_the_republication_line_counts_ADMITTED_candidates_only(caplog) -> None:
    """`brk20_retest_republished` is a PUBLICATION count. Emitted at offer time it would also count
    every candidate the day cap, the brk20 sub-cap or the same-day dedupe suppressed — the raw-vs-
    published ambiguity the WO-9 funnel exists to remove. The book logs `brk20_retest_offered`; this
    line is emitted here, per accepted candidate, off the book's `offers` detail."""
    due = [_brk20_cand("AAA"), _brk20_cand("BBB")]
    book, screen = _FakeRestingBook(due), _FakeScreen(accept={"AAA"})

    with caplog.at_level(logging.INFO, logger="engine.ops.main"):
        out = _retest_republish(book, screen, _snapshot_features([]), **_retest_kwargs())

    assert [c.symbol for c in out] == ["AAA"]
    lines = [r for r in caplog.records if r.getMessage() == "brk20_retest_republished"]
    assert len(lines) == 1
    detail = lines[0].__dict__          # core.log passes structured fields through `extra`
    assert detail["symbol"] == "AAA"
    assert detail["signal_d"] == "2026-06-17" and detail["dev"] == "0.20"


def test_an_unreadable_limits_table_re_offers_nothing(caplog) -> None:
    """The band is the only thing between this mechanism and re-publishing a level the §7.1 gate is
    certain to reject, so an unverifiable limits store fails to ZERO rather than to a constant (§2.4
    item 1: limits are read at the enforcement site or not used). The staleness bound rides the SAME
    load, so neither can be read against a different snapshot than the other. Expiry — pure
    housekeeping that reads no limit — still runs."""
    book, screen = _FakeRestingBook([_brk20_cand("AAA")]), _FakeScreen()

    def boom() -> tuple[float, float]:
        raise RuntimeError("limits.yaml hash mismatch")

    with caplog.at_level(logging.WARNING, logger="engine.ops.main"):
        out = _retest_republish(book, screen, _snapshot_features([]),
                                **_retest_kwargs(limits_fn=boom))

    assert out == []
    assert book.expired == [RETEST_D] and book.due_calls == [] and screen.calls == []
    assert "brk20_retest_band_unreadable" in caplog.text


def test_an_empty_book_costs_one_read_and_no_admission() -> None:
    """The ordinary minute: nothing is resting, or nothing is back in the band. One cheap SQL read,
    no admit call, no log noise — the cadence has to be free or it cannot be a cadence."""
    book, screen = _FakeRestingBook([]), _FakeScreen()
    out = _retest_republish(book, screen, _snapshot_features([]), **_retest_kwargs())
    assert out == [] and screen.calls == []
    assert len(book.due_calls) == 1 and book.expired == [RETEST_D]


# --------------------------------------------------------------------------- the not-offerable screen
def test_the_skip_screen_names_exactly_what_the_gate_would_hard_reject() -> None:
    """The mechanism the backtest measured books ONE trade per signal. A level already HELD, or
    carrying a pending entry recommendation, is rejected by `gate._rule_per_stock_exposure` as
    `already held or pending` — a HARD reason no shrink can cure — but only AFTER the §3.2.5 day slot
    and one of the day's analyst calls are spent. Two more narrowings keep the re-offer inside the
    population and the corporate-action safety brk20 originated under."""
    reasons = opsmain._retest_skip_reasons(
        ["HELD", "PENDING", "EXDATE", "UNADJ", "CHURNED", "FINE"],
        eligible={"HELD", "PENDING", "EXDATE", "UNADJ", "FINE"}, ex_skip={"EXDATE"},
        held={"HELD"}, pending={"PENDING"}, unadjusted={"UNADJ"},
    )
    assert set(reasons) == {"HELD", "PENDING", "EXDATE", "UNADJ", "CHURNED"}
    assert "per_stock_exposure" in reasons["HELD"]
    assert "pending" in reasons["PENDING"]
    assert "ex-date" in reasons["EXDATE"]
    # The BACKWARD half of the corporate-action veto (2026-09-12 review): an ex-date that passed
    # between the crossing and the retest leaves bars_1d in two units, exactly what the sweep's
    # `unadjusted_symbols` veto refuses on a fresh cross.
    assert "two units" in reasons["UNADJ"]
    assert "eligible universe" in reasons["CHURNED"]
    assert "FINE" not in reasons


def test_a_pending_entry_recommendation_is_read_the_way_the_gate_reads_it(conn, clock) -> None:
    """Same predicate as `GateContextBuilder._pending_entry_rec_symbols`, on the same shared
    `parse_valid_until`: an ACTIONED row, an EXPIRED one, an exit rec and an unparseable/absent
    `valid_until` are all NOT pending — negating the expiry test would flip the last of those."""
    now = clock.now()
    rows = [
        ("r1", {"kind": "entry", "instrument": "LIVE",
                "valid_until": (now + timedelta(hours=2)).isoformat()}, None),
        ("r2", {"kind": "entry", "instrument": "TAKEN",
                "valid_until": (now + timedelta(hours=2)).isoformat()}, "taken"),
        ("r3", {"kind": "entry", "instrument": "STALE",
                "valid_until": (now - timedelta(hours=2)).isoformat()}, None),
        ("r4", {"kind": "exit", "instrument": "EXITREC",
                "valid_until": (now + timedelta(hours=2)).isoformat()}, None),
        ("r5", {"kind": "entry", "instrument": "NOEXPIRY"}, None),
        ("r6", None, None),                       # a half-written row must not raise
    ]
    for rec_id, payload, action in rows:
        conn.execute(
            "INSERT INTO recommendations (rec_id, payload, human_action) VALUES (?, ?, ?)",
            (rec_id, None if payload is None else json.dumps(payload), action),
        )
    assert opsmain._pending_entry_rec_symbols(conn, now) == {"LIVE"}


# --------------------------------------------------------------------------- the composed sweep world
#: Today's universe: one included (watchlist) name, one capped NIFTY 500 name, one capped name outside
#: the core index — so eligible = all three, core = the first two, watchlist = the first.
_WATCH, _CORE, _WIDE = "WATCH", "CORE1", "WIDE1"
_TOKENS = {_WATCH: 1, _CORE: 2, _WIDE: 3, "RESTER": 4, "PAPONLY": 5,
           opsmain.INDEX_SYMBOL: 99, opsmain.VIX_SYMBOL: 98}
#: Captured at import: a second world in one test must wrap the real admit, not the first world's spy.
_REAL_ADMIT = SignalPreScreen.admit


class _World:
    """What a composed engine's scan sweeps did. Every batch leg is a recorder that emits one candidate
    per symbol it was handed, so its INPUT is what the sweep chose to give it; every
    ``SignalPreScreen.admit`` batch and every publication into the pipeline is recorded too."""

    def __init__(self) -> None:
        self.legs: dict[str, list[tuple[list[str], dict]]] = {}
        self.admits: list[tuple[list[tuple[str, str]], bool]] = []
        self.published: list[tuple[str, str]] = []
        #: ``(strategy, symbol)`` pairs the admit refuses, as a binding cap would.
        self.suppress: set[tuple[str, str]] = set()
        #: When set, each publication waits on it — a sweep held AFTER its publication mark.
        self.hold: asyncio.Event | None = None


def _patch_world(monkeypatch, world: _World) -> None:
    async def on_candidate(self, cand) -> None:
        world.published.append((cand.strategy_id, cand.symbol))
        if world.hold is not None:
            await world.hold.wait()

    monkeypatch.setattr(RecommendationPipeline, "on_signal_candidate", on_candidate)

    def leg(name: str, symbols_of, score: float = 0.5):
        def run(rows, **kwargs):
            symbols = symbols_of(rows)
            world.legs.setdefault(name, []).append((symbols, kwargs))
            return [_batch_cand(name, s, score) for s in symbols]
        return run

    def by_symbol(rows) -> list[str]:
        return [r.symbol for r in rows]

    monkeypatch.setattr(brk20, "sweep_daily", leg("brk20", sorted))
    monkeypatch.setattr(ins, "sweep_crossings", leg("ins", by_symbol))
    monkeypatch.setattr(cat, "sweep_watchlist", leg("cat", by_symbol))
    monkeypatch.setattr(cat_reversal, "sweep_watchlist", leg("cat_reversal", by_symbol))
    hi52_leg = leg("hi52", list, score=0.99)
    monkeypatch.setattr(opsmain, "_hi52_daily_leg",
                        lambda store, *, trigger, eligible, **_kw: hi52_leg(eligible, trigger=trigger))

    def cat_rows(store, today):
        rows = [dict(entry_id=f"e-{s}", symbol=s, grade="originating", direction="long",
                     event_age_sessions=0, materiality=0.8, reference_close=Decimal("100"))
                for s in (_CORE, _WIDE)]
        return ([cat.WatchlistRow(**r) for r in rows],
                [cat_reversal.WatchlistRow(**r, reversal_of="e-earlier") for r in rows])

    monkeypatch.setattr(opsmain, "_read_cat_watchlist", cat_rows)

    def admit(self, candidates, today, *, in_window=True):
        world.admits.append(([(c.strategy_id, c.symbol) for c in candidates], in_window))
        kept = [c for c in candidates if (c.strategy_id, c.symbol) not in world.suppress]
        return _REAL_ADMIT(self, kept, today, in_window=in_window)

    monkeypatch.setattr(SignalPreScreen, "admit", admit)


def _seed_world_market(store) -> None:
    today = FIXED_NOW.date()
    store.upsert_universe_daily([
        {"d": today, "symbol": _WATCH, "included": True, "exclusion_reasons": None},
        {"d": today, "symbol": _CORE, "included": False, "exclusion_reasons": ["watchlist_cap"]},
        {"d": today, "symbol": _WIDE, "included": False, "exclusion_reasons": ["watchlist_cap"],
         "core_index": False},
    ])
    yesterday = today - timedelta(days=1)
    store.upsert_bars_1d([
        DailyBar(symbol=s, d=yesterday, open=Decimal("100"), high=Decimal("100"), low=Decimal("100"),
                 close=Decimal("100"), volume=1000)
        for s in (_WATCH, _CORE, _WIDE)
    ])


@asynccontextmanager
async def _sweep_world(tmp_path, monkeypatch, db_template, *, state_db=None, **kwargs):
    """A composed engine (default 10:05 on a session day, inside the 10:00-10:30 window) in RECOMMEND
    with the window persisted, today's universe and yesterday's bars in its market store, one ``ins``
    crossing pending for the core name, and every batch leg a recorder (:class:`_World`)."""
    world = _World()
    _patch_world(monkeypatch, world)

    def seed(conn) -> None:
        _seed_ins_pending(conn, FIXED_NOW.date(), _CORE)
        if state_db is not None:
            state_db(conn)

    async with composed_engine(tmp_path, monkeypatch, db_template=db_template, market=_seed_world_market,
                               state_db=seed, **kwargs) as c:
        await c.engine.mode.request_transition(Mode.RECOMMEND, Actor.OWNER)
        c.engine.mode.seed_trade_window_if_absent(TradeWindow(start=time(10, 0), end=time(10, 30)))
        yield c, world


def _watch_feed(monkeypatch, engine: opsmain.Engine) -> list[list[int]]:
    """The ticker as a live feed recording every resubscription, over :data:`_TOKENS`."""
    subscriptions: list[list[int]] = []

    async def update(tokens) -> None:
        subscriptions.append(list(tokens))

    monkeypatch.setattr(engine.ticker, "health", lambda: SimpleNamespace(state="HEALTHY"))
    monkeypatch.setattr(engine.ticker, "update_subscriptions", update)
    monkeypatch.setattr(engine.instruments, "token_for_symbol", _TOKENS.get)
    return subscriptions


async def _job(engine: opsmain.Engine, job_id: str) -> None:
    """Fire one armed scheduler job the way APScheduler would."""
    await engine.scheduler._sched.get_job(job_id).func()


async def _settle(engine: opsmain.Engine) -> None:
    """Wait out the detached freeze-lift tasks a risk-state publication started."""
    if engine.freeze_lift_tasks:
        await asyncio.wait_for(asyncio.gather(*list(engine.freeze_lift_tasks)), timeout=10)


def _resting_level(conn, symbol: str, signal_d: date, expires_d: date) -> None:
    conn.execute(
        "INSERT INTO brk20_resting_levels (symbol, signal_d, entry, stop, target, score, expires_d, "
        "status, created_at, updated_at) VALUES (?, ?, '100', '94', NULL, 0.5, ?, 'resting', ?, ?)",
        (symbol, signal_d.isoformat(), expires_d.isoformat(), signal_d.isoformat(), signal_d.isoformat()),
    )


async def test_the_composition_root_really_arms_the_retest_rearm(
    tmp_path, monkeypatch, migrated_db_template
) -> None:
    """Both halves of the mechanism are wirings inside ``compose`` (the `hi52` promotion's lesson: a
    wiring nothing exercises silently disappears). Half one: every ADMITTED brk20 candidate starts
    resting, recorded on the loop thread, and the feed follows a resting level. Half two: the 60 s
    drain tick re-offers under the window predicate, armed only by an in-window sweep, off the loop,
    ahead of the drain and never at its expense, and publishes what admit accepted."""
    loop_thread = threading.get_ident()
    order: list[tuple] = []
    real_record, real_consume = RestingLevelBook.record, opsmain._consume_ins_pending

    def record(self, cand):
        order.append(("record", cand.symbol, threading.get_ident()))
        return real_record(self, cand)

    def consume(*args, **kwargs):
        order.append(("consume",))
        return real_consume(*args, **kwargs)

    monkeypatch.setattr(RestingLevelBook, "record", record)
    monkeypatch.setattr(opsmain, "_consume_ins_pending", consume)
    offers: list[tuple] = []
    failing = {"on": False}

    def republish(book, prescreen, features, **kwargs):
        offers.append((book, kwargs, threading.get_ident()))
        if failing["on"]:
            raise RuntimeError("duckdb stall")
        return [_batch_cand(brk20.STRATEGY_ID, "RESTER", 0.5)]

    active_args: list[tuple] = []

    def active(*args) -> bool:
        active_args.append(args)
        return True

    drained: list[int] = []

    async def drain(self) -> None:
        drained.append(len(offers))

    monkeypatch.setattr(opsmain, "_retest_republish", republish)
    monkeypatch.setattr(opsmain, "_retest_active", active)
    monkeypatch.setattr(RecommendationPipeline, "drain_forward_queue", drain)
    built: dict[str, list] = {}
    _spy(monkeypatch, "RestingLevelBook", built)
    prior = date(2026, 6, 16)                          # the session before FIXED_NOW's

    def owner_window(settings) -> None:          # off the code default, so a hardcoded 5 cannot pass
        settings.brk20.retest_sessions = 3

    async with _sweep_world(tmp_path, monkeypatch, migrated_db_template, paper=False,
                            now=datetime(2026, 6, 17, 9, 50, tzinfo=IST), configure=owner_window,
                            state_db=lambda conn: _resting_level(conn, "RESTER", prior, date(2026, 6, 23))
                            ) as (c, w):
        e = c.engine
        feed = _watch_feed(monkeypatch, e)
        await _job(e, "forward_drain_tick")
        await e.scan_sweep("scan_now")                 # pre-window: admits nothing, arms nothing
        await _job(e, "forward_drain_tick")
        assert [o[1]["sweep_ready"] for o in offers] == [False, False]

        c.time.set(FIXED_NOW)
        w.suppress = {(brk20.STRATEGY_ID, _WIDE)}      # a raw fire the caps refuse
        await e.scan_sweep("window_open")
        resting = {r["symbol"] for r in e.conn.execute(
            "SELECT symbol FROM brk20_resting_levels WHERE signal_d = ?", (FIXED_NOW.date().isoformat(),))}
        assert resting == {_CORE, _WATCH}             # ADMITTED, never raw
        records = [o for o in order if o[0] == "record"]
        assert records and {o[2] for o in records} == {loop_thread}
        assert order.index(("consume",)) < order.index(records[0])
        assert _TOKENS["RESTER"] in feed[-1]           # the feed follows a level resting since 06-16

        tick = Tick(instrument_token=2, tradingsymbol=_CORE, ltp=Decimal("101.50"), volume_traded=10,
                    exchange_ts=FIXED_NOW - timedelta(seconds=3))
        await e.bus.apublish("tick", tick)
        published_before = len(w.published)
        await _job(e, "forward_drain_tick")
        book, kw, thread = offers[-1]
        assert kw["sweep_ready"] is True and kw["active"] is True and kw["today"] == FIXED_NOW.date()
        now, calendar, mode, kill, lock = active_args[-1]
        assert (now, calendar, mode) == (FIXED_NOW, e.calendar, e.mode)
        assert type(kill).__name__ == "KillSwitch" and isinstance(lock, asyncio.Lock)
        ((_, book_kw, the_book),) = built["RestingLevelBook"]
        assert book_kw["sessions"] == 3                                     # owner-configured
        assert book is the_book and book.resting_symbols(FIXED_NOW.date()) == ["RESTER"]
        assert kw["ltp_fn"](_CORE) == Decimal("101.50") and kw["tick_age_fn"](_CORE) == pytest.approx(3.0)
        lim = LimitsEngine(ProtectedStore(config_dir(), e.conn, e.clock)).load().limits
        assert kw["limits_fn"]() == (float(lim.entry_sanity_band.cnc_pct),
                                     float(lim.stale_data_guard.max_tick_age_s))
        skipped = kw["skip_fn"]([_CORE, "GONE"])
        assert list(skipped) == ["GONE"] and "eligible universe" in skipped["GONE"]
        assert thread != loop_thread                   # off the event loop
        assert w.published[published_before:] == [(brk20.STRATEGY_ID, "RESTER")]
        assert drained[-1] == len(offers)              # the drain ran after the offer

        failing["on"] = True
        await _job(e, "forward_drain_tick")
        assert drained[-1] == len(offers)              # a failing retest never costs the drain

    # The admission and the post-admit mint are the sweep's own, not a parallel path.
    assert "_attach_feature_snapshots(features, prescreen.admit(due, today, in_window=True))" in \
        inspect.getsource(_retest_republish)


def test_the_unverified_limits_fallback_base_is_the_registration_base() -> None:
    """A boot on an unverified limit table builds the ExposureTracker on this constant; it must stay
    the registration base. Since O19 the live base can differ (the owner changes it from the
    dashboard); the fallback is display-only on a FROZEN boot and may lag the live value."""
    assert Decimal(str(PINNED_CAPITAL["capital_base_inr"])) == opsmain._FALLBACK_CAPITAL_BASE_INR


class _Limits:
    """``LimitsEngine.load`` stand-in: a base that can move, or an unverified table."""

    def __init__(self, base: str | None = None, exc: Exception | None = None) -> None:
        self.base, self.exc, self.loads = base, exc, 0

    def load(self) -> SimpleNamespace:
        self.loads += 1
        if self.exc is not None:
            raise self.exc
        return SimpleNamespace(capital_base_inr=Decimal(self.base))


def test_the_real_tracker_base_reads_through_the_limits_engine_on_every_call() -> None:
    """O19 §2: no boot-time copy, so a ``LimitsEngine.reload()`` reaches the real brakes."""
    limits = _Limits("40000")
    base = opsmain._limits_capital_base(limits)
    assert base() == Decimal("40000")
    limits.base = "60000"
    assert base() == Decimal("60000") and limits.loads == 2


def test_an_unverified_limits_table_falls_back_and_says_so_once(caplog) -> None:
    """R9: IntegrityError ONLY falls back (the boot is FROZEN by the self-test), logged once per process."""
    limits = _Limits(exc=IntegrityError("limits.yaml hash mismatch"))
    base = opsmain._limits_capital_base(limits)
    with caplog.at_level(logging.WARNING, logger="engine.ops.main"):
        assert [base(), base(), base()] == [opsmain._FALLBACK_CAPITAL_BASE_INR] * 3
    assert [r.getMessage() for r in caplog.records].count("limits_unverified_capital_fallback") == 1
    # A reseed in-run is read on the next call: the fallback is not latched.
    limits.exc, limits.base = None, "60000"
    assert base() == Decimal("60000")
    # Anything else is a real defect and propagates — never silently priced at the fallback.
    limits.exc = ValueError("bad table")
    with pytest.raises(ValueError):
        base()


def _kw(call: ast.Call, arg: str) -> str:
    return next(ast.unparse(k.value) for k in call.keywords if k.arg == arg)


async def test_the_real_tracker_is_built_on_the_live_base(
    tmp_path, monkeypatch, migrated_db_template, caplog
) -> None:
    """O19 §2: the composed tracker reads the verified table on every call — an owner update plus the
    gate's ONE ``reload()`` moves it (no boot-time copy) — and an unverifiable table at boot warns
    and falls back to the shipped base instead of crashing the boot."""
    built: dict[str, list] = {}
    _spy(monkeypatch, "ExposureTracker", built)
    _spy(monkeypatch, "RiskGate", built)
    owner = OwnerConfirmation(actor=Actor.OWNER, confirmed=True, note="test")
    async with composed_engine(tmp_path / "verified", monkeypatch, db_template=migrated_db_template) as c:
        ((tracker_args, _, tracker),) = built["ExposureTracker"]
        ((gate_args, _, _),) = built["RiskGate"]
        assert tracker_args[:2] == (c.engine.conn, c.engine.clock)
        assert tracker.capital_base == Decimal(str(PINNED_CAPITAL["capital_base_inr"]))
        shipped = (config_dir() / "limits.yaml").read_bytes().decode("utf-8")
        moved, n = re.subn(r"(?m)^capital_base_inr: \d+\b", "capital_base_inr: 20000", shipped)
        assert n == 1
        ProtectedStore(config_dir(), c.engine.conn, c.engine.clock).owner_update("limits.yaml", moved, owner)
        gate_args[0].reload()                                      # the gate's own LimitsEngine
        assert tracker.capital_base == Decimal("20000")

    built.clear()
    with caplog.at_level(logging.WARNING, logger="engine.ops.main"):
        async with composed_engine(tmp_path / "unverified", monkeypatch, db_template=migrated_db_template,
                                   registered=False):
            ((_, _, tracker),) = built["ExposureTracker"]
            assert tracker.capital_base == opsmain._FALLBACK_CAPITAL_BASE_INR
    assert [r.getMessage() for r in caplog.records].count("limits_unverified_at_boot") == 1


async def test_the_prescreen_displacement_margin_is_the_owner_setting(
    tmp_path, monkeypatch, migrated_db_template
) -> None:
    """2026-08-27 cap displacement: how much better a later candidate must score to take a full cap's
    slot from an unevaluated incumbent is an owner knob in settings.yaml (§6.3) — the composed
    pre-screen must be handed THAT value, not a constructor default."""
    def owner_knob(settings) -> None:
        settings.strategy.prescreen.displacement_margin = 0.37

    built: dict[str, list] = {}
    _spy(monkeypatch, "SignalPreScreen", built)
    async with composed_engine(tmp_path, monkeypatch, db_template=migrated_db_template, configure=owner_knob):
        pass
    ((_, kw, _),) = built["SignalPreScreen"]
    assert kw["displacement_margin"] == 0.37


async def test_the_gate_and_the_pipeline_share_one_live_strategy_edges(
    tmp_path, monkeypatch, migrated_db_template
) -> None:
    """O19 §3.3: ONE ``StrategyEdges`` on the hash-verified limits and the gate's own cost model, so
    the gate's C3 edge and the pipeline's registered edge can never disagree after a capital change."""
    built: dict[str, list] = {}
    for name in ("StrategyEdges", "RiskGate", "RecommendationPipeline"):
        _spy(monkeypatch, name, built)
    async with composed_engine(tmp_path, monkeypatch, db_template=migrated_db_template) as c:
        settings = c.engine.settings

    ((edges_args, edges_kw, edges),) = built["StrategyEdges"]
    ((gate_args, gate_kw, gate),) = built["RiskGate"]
    ((pipe_args, pipe_kw, _pipeline),) = built["RecommendationPipeline"]
    limits_engine, cost_model = gate_args[0], gate_args[1]
    assert edges_args == (opsmain._strategy_expected_edge_pct(settings),)
    assert edges_kw["table_fn"] == limits_engine.load               # the hash-verified table, live
    assert edges_kw["cost_model"] is cost_model                     # the gate's own cost model
    assert gate_kw["strategy_expected_edge_pct"] is edges
    assert pipe_kw["strategy_edge_pct"] is edges                    # the gate's object, never a copy
    assert pipe_args[3] is gate


def test_one_limits_reload_reaches_the_real_base_and_the_hi52_edge(tmp_path, conn, clock) -> None:
    """The seam end to end on the real artefacts: the shipped limits.yaml through a ProtectedStore and
    LimitsEngine, the shipped settings map, the gate's kind of cost model. An owner update plus ONE
    ``reload()`` moves the real tracker's base and lowers hi52's edge; ins is untouched."""
    from engine.core.protected_store import ProtectedStore
    from engine.core.types import OwnerConfirmation
    from engine.risk.exposure import ExposureTracker
    from engine.risk.limits import LimitsEngine
    from engine.strategy.cost_model import CostModel
    from engine.strategy.hi52_edge import StrategyEdges

    def at_base(inr: int) -> str:     # base-agnostic: the shipped base itself moves with O19 changes
        text, n = re.subn(r"(?m)^capital_base_inr: \d+\b", f"capital_base_inr: {inr}", shipped)
        assert n == 1
        return text

    owner = OwnerConfirmation(actor=Actor.OWNER, confirmed=True, note="test")
    cfg = tmp_path / "config"
    cfg.mkdir()
    shipped = (config_dir() / "limits.yaml").read_bytes().decode("utf-8")
    (cfg / "limits.yaml").write_bytes(at_base(40000).encode("utf-8"))     # the registration base
    store = ProtectedStore(cfg, conn, clock)
    store.register_initial("limits.yaml", owner)
    engine = LimitsEngine(store)
    tracker = ExposureTracker(conn, clock, opsmain._limits_capital_base(engine))
    edges = StrategyEdges(_strategy_expected_edge_pct(load_settings()), table_fn=engine.load,
                          cost_model=CostModel.from_config(edge_multiple_min=Decimal("2.0")))
    assert tracker.capital_base == Decimal("40000")
    assert (edges[hi52.STRATEGY_ID], edges[ins.STRATEGY_ID]) == (Decimal("1.53"), Decimal("1.58"))

    store.owner_update("limits.yaml", at_base(20000), owner)
    assert tracker.capital_base == Decimal("40000")                    # cached until the reload
    engine.reload()
    assert tracker.capital_base == Decimal("20000")
    assert (edges[hi52.STRATEGY_ID], edges[ins.STRATEGY_ID]) == (Decimal("1.27"), Decimal("1.58"))


def test_paper_risk_reads_the_same_live_base_as_the_paper_runtime() -> None:
    module = ast.parse(inspect.getsource(opsmain))
    compose = next(n for n in module.body if isinstance(n, ast.AsyncFunctionDef) and n.name == "_compose_paper")
    calls = {ast.unparse(n.func): n for n in ast.walk(compose) if isinstance(n, ast.Call)}
    live = "lambda: limits.load().capital_base_inr"
    assert _kw(calls["PaperRisk"], "capital_base") == live
    assert _kw(calls["PaperRuntime"], "capital_base_fn") == live


def test_the_shipped_retest_window_is_the_variant_the_backtest_selected() -> None:
    """5 sessions = `V2_limit_at_H20_N5`. The number IS the registered mechanism — N=1 (what shipped
    before WO-R) and N=3 were both measured and both lost, every other N was never run — so it is an
    owner-only key and the code default must agree with the shipped settings, not shadow it."""
    assert load_settings().brk20.retest_sessions == DEFAULT_RETEST_SESSIONS == 5
    # …and it is NOT in the learnable envelope: the rule's own params live in brk20.DEFAULT_PARAMS.
    assert "retest_sessions" not in brk20.DEFAULT_PARAMS
    assert "retest_sessions" not in brk20.FLOOR_PARAMS


# --------------------------------------------------------------------------- bounded news resolve
@pytest.mark.asyncio
async def test_resolve_news_bounded_completes_times_out_and_frees_the_lock() -> None:
    """2026-08-10 boot wedge: a post-clustering await hung 8+ h holding the news chain. The bound
    covers the chain AND lock acquisition; expiry cancels, alerts, frees the lock for the next
    caller, and returns False — the boot moves on (E5: the chain is never load-bearing)."""
    import asyncio

    from engine.ops.main import resolve_news_bounded

    lock = asyncio.Lock()
    alerts: list[int] = []

    async def on_timeout():
        alerts.append(1)

    async def quick():
        return None

    assert await resolve_news_bounded(lock, quick, timeout_s=5) is True
    assert not lock.locked()

    async def hangs():
        await asyncio.sleep(3600)

    assert await resolve_news_bounded(lock, hangs, timeout_s=0.05, on_timeout=on_timeout) is False
    assert alerts == [1]
    assert not lock.locked()                                   # cancellation released it
    assert await resolve_news_bounded(lock, quick, timeout_s=5) is True   # next caller unblocked

    # A wedged HOLDER must not wedge later callers past their own bound.
    await lock.acquire()
    try:
        assert await resolve_news_bounded(lock, quick, timeout_s=0.05, on_timeout=on_timeout) is False
        assert alerts == [1, 1]
    finally:
        lock.release()


# --------------------------------------------------------------------------- orphan re-sweep cutoff
#: ``job_news_chain``'s ABANDON horizon (2026-08-10): the re-sweep only reaches back this far, so a
#: permanently-unclusterable orphan ages out of the retry set instead of being carried forever.
_RESWEEP_ABANDON_DAYS = 4


async def test_orphan_resweep_abandons_headlines_older_than_four_days(
    tmp_path, monkeypatch, migrated_db_template
) -> None:
    """2026-08-10 filed follow-up: the 4-day abandon cutoff of ``job_news_chain``'s orphan re-sweep.
    A headline one minute PAST the cutoff is abandoned, one exactly on it (inclusive) and everything
    inside it is swept, oldest-first, already-clustered rows never, and at most 500 per run — the cap
    that keeps repeated timeouts from growing the next attempt past its own deadline."""
    now = FIXED_NOW
    cutoff = now - timedelta(days=_RESWEEP_ABANDON_DAYS)

    def row(hid: str, at: datetime, **extra) -> dict:
        return {"headline_id": hid, "title": f"Orphan headline {hid}", "source_domain": "livemint.com",
                "url": f"https://www.livemint.com/market/{hid}.html", "published_at": at, **extra}

    def news(store) -> None:
        store.insert_news([
            row("past-cutoff", cutoff - timedelta(minutes=1)), row("at-cutoff", cutoff),
            row("inside-cutoff", cutoff + timedelta(minutes=1)),
            row("already-clustered", now, cluster_id="c-already"),
            *(row(f"bulk-{i:03d}", cutoff + timedelta(hours=1, minutes=i)) for i in range(499)),
        ])

    clustered: list[list[str]] = []

    async def backfill(self) -> list:
        return []

    async def cluster(self, headlines) -> list:
        clustered.append([h.headline_id for h in headlines])
        return []

    async def no_scoring(self, **_kw) -> None:
        return None

    monkeypatch.setattr(opsmain.NewsIngest, "backfill", backfill)
    monkeypatch.setattr(opsmain.HeadlineClusterer, "run", cluster)
    monkeypatch.setattr(opsmain.NewsScoringJob, "run_batch", no_scoring)
    async with composed_engine(tmp_path, monkeypatch, db_template=migrated_db_template, market=news) as c:
        await _spec(c.engine, opsmain.JOB_NEWS_CHAIN).run()

    (swept,) = clustered
    assert len(swept) == 500                                    # 501 eligible orphans, capped
    assert swept[:2] == ["at-cutoff", "inside-cutoff"]          # oldest first, boundary inclusive
    assert swept[-1] == "bulk-497" and "bulk-498" not in swept
    assert "past-cutoff" not in swept and "already-clustered" not in swept


# --------------------------------------------------------------------------- boot-phase safety ticks
@pytest.mark.asyncio
async def test_boot_phase_ticks_run_survive_errors_and_cancel_cleanly() -> None:
    """2026-08-07: warm-up lift + health visibility must not wait out a news-backlog catch-up. The
    boot tick loop fires both callables each interval, survives a raising tick (fail-open on
    observation), and dies instantly on cancel (the scheduler taking over)."""
    import asyncio

    from engine.ops.main import boot_phase_ticks

    refreshes: list[int] = []
    healths: list[int] = []

    async def refresh():
        refreshes.append(1)
        if len(refreshes) == 2:
            raise RuntimeError("one bad tick")

    async def health_check():
        healths.append(1)

    task = asyncio.create_task(boot_phase_ticks(refresh, health_check, interval_s=0.01))
    while len(refreshes) < 4:                                   # the raising tick #2 didn't end the loop
        await asyncio.sleep(0.005)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    assert len(refreshes) >= 4
    assert len(healths) >= 3                                    # tick #2's health skipped by the raise
    assert len(healths) < len(refreshes)                        # the raise short-circuited that tick only

    # Sleep-first pin: a fast boot cancels before the first interval elapses ⇒ ZERO fires.
    fast_refreshes: list[int] = []

    async def fast_refresh():
        fast_refreshes.append(1)

    fast = asyncio.create_task(boot_phase_ticks(fast_refresh, health_check, interval_s=60.0))
    await asyncio.sleep(0.01)
    fast.cancel()
    try:
        await fast
    except asyncio.CancelledError:
        pass
    assert fast_refreshes == []                                 # a normal boot never sees a tick


# --------------------------------------------------------------------------- warm-up gap self-repair
def _repair_setup(now, *, fetched=1, bars_written=120, failed=()):
    """Stub clock (mutable now), the REAL calendar, a not-ready orb-gap status, and a recorder
    whose repair returns a BackfillReport-shaped result. Budget charges on BROKER SPEND, counted in
    KITE REQUESTS (2026-09-18): one per ``fetched`` span (a completed per-symbol historical call)
    plus one per non-``unknown_instrument_token`` failure span."""
    from types import SimpleNamespace

    holder = {"now": now}
    clock = SimpleNamespace(now=lambda: holder["now"])
    status = SimpleNamespace(ready=False, blockers=["orb:ABB bars 146/147", "orb:TCS bars 146/147"])
    calls: list[tuple] = []

    async def repair(frm, to):
        calls.append((frm, to))
        return SimpleNamespace(
            fetched=[SimpleNamespace(symbol=f"F{i}") for i in range(fetched)],
            bars_written=bars_written,
            failed=[SimpleNamespace(error=e) for e in failed],
        )

    return holder, clock, status, calls, repair


@pytest.mark.asyncio
async def test_warmup_gap_repair_fires_trimmed_and_budget_charges_on_activity(calendar) -> None:
    """2026-08-06 seam hole + review round: the 60 s refresh re-triggers the gap backfill itself —
    in-session only, ``to`` trimmed 2 min back (the live builder owns the tail minutes), one attempt
    per cooldown window, and the per-day budget charges only on attempts with BROKER SPEND — counted
    in Kite REQUESTS since 2026-09-18 (one per completed per-symbol span), so a one-symbol repair
    costs one request while a full-watchlist sweep costs one per symbol."""
    from datetime import datetime as _dt
    from datetime import timedelta as _td

    from engine.core.clock import IST
    from engine.ops.main import _GAP_REPAIR_MAX_REQUESTS_PER_DAY, maybe_repair_warmup_gaps

    assert _GAP_REPAIR_MAX_REQUESTS_PER_DAY == 900       # 3 sweeps x a 300-symbol watchlist

    now = _dt(2026, 6, 17, 10, 5, 33, tzinfo=IST)             # Wed, in-session, mid-minute
    holder, clock, status, calls, repair = _repair_setup(now)   # one-symbol span ⇒ 1 request/attempt
    state: dict = {}

    assert await maybe_repair_warmup_gaps(status, state, clock=clock, calendar=calendar,
                                          repair=repair, max_requests_per_day=3)
    assert len(calls) == 1
    frm, to = calls[0]
    assert frm.hour == 9 and frm.minute == 15
    assert to == now.replace(second=0, microsecond=0) - _td(minutes=2)   # builder-owned tail excluded
    assert state["requests"] == 1 and state["attempts"] == 1   # one completed span ⇒ one request

    # Within the cooldown: no second attempt (paced even when the first was productive).
    holder["now"] = now + _td(minutes=2)
    assert not await maybe_repair_warmup_gaps(status, state, clock=clock, calendar=calendar,
                                              repair=repair, max_requests_per_day=3)
    assert len(calls) == 1

    # Past the cooldown: retries, until the day's REQUEST budget is spent.
    holder["now"] = now + _td(minutes=6)
    assert await maybe_repair_warmup_gaps(status, state, clock=clock, calendar=calendar,
                                          repair=repair, max_requests_per_day=3)
    holder["now"] = now + _td(minutes=12)
    assert await maybe_repair_warmup_gaps(status, state, clock=clock, calendar=calendar,
                                          repair=repair, max_requests_per_day=3)
    holder["now"] = now + _td(minutes=18)
    assert not await maybe_repair_warmup_gaps(status, state, clock=clock, calendar=calendar,
                                              repair=repair, max_requests_per_day=3)
    # Capped: retrying harder can't close an unfetchable hole.
    assert len(calls) == 3 and state["requests"] == 3 and state["attempts"] == 3

    # A NEW session day resets the budget.
    holder["now"] = _dt(2026, 6, 18, 9, 30, tzinfo=IST)
    assert await maybe_repair_warmup_gaps(status, state, clock=clock, calendar=calendar,
                                          repair=repair, max_requests_per_day=3)
    assert len(calls) == 4 and state["requests"] == 1 and state["attempts"] == 1

    # 2026-09-16, the reason the unit changed: a market-wide sweep charges PER SYMBOL, so three of
    # them spend the real 900/day budget — and a single thin symbol's repair after them costs 1,
    # where the old 3-ATTEMPT cap had already refused it (exhausted at 13:37:01, one second after
    # the one-symbol hole appeared).
    sweep_state: dict = {}
    for i in range(3):
        _, sweep_clock, _, sweep_calls, sweep = _repair_setup(holder["now"] + _td(minutes=6 * i),
                                                              fetched=300)
        assert await maybe_repair_warmup_gaps(status, sweep_state, clock=sweep_clock,
                                              calendar=calendar, repair=sweep)
        assert len(sweep_calls) == 1
    assert sweep_state["requests"] == 900 and sweep_state["attempts"] == 3
    _, thin_clock, _, thin_calls, thin = _repair_setup(holder["now"] + _td(minutes=24))
    assert not await maybe_repair_warmup_gaps(status, sweep_state, clock=thin_clock,
                                              calendar=calendar, repair=thin)
    assert thin_calls == []                                     # 900 spent — exhausted, as designed


@pytest.mark.asyncio
async def test_warmup_gap_repair_budget_charges_on_broker_spend_only(calendar) -> None:
    """Review round 2: the budget predicate is BROKER SPEND, in Kite REQUESTS (2026-09-18). Free:
    pure local scans (transient just-closed-minute deficit ⇒ zero gaps in the trimmed window ⇒ no
    historical call) and ``unknown_instrument_token`` spans (recorded pre-network — the instruments
    map is post-login's repair). Charged one request per span: completed fetches EVEN WITH ZERO BARS
    LANDED (the unfillable-hole sweep must not retry uncapped all session — one request is one
    request) and real broker failures; a raising repair charges one per intraday blocker it could
    have swept, never less than one (spend-safe: we do not know how far it got)."""
    from datetime import datetime as _dt
    from datetime import timedelta as _td

    from engine.core.clock import IST
    from engine.ops.main import maybe_repair_warmup_gaps

    now = _dt(2026, 6, 17, 10, 5, 33, tzinfo=IST)
    holder, clock, status, calls, scan_only = _repair_setup(now, fetched=0, bars_written=0)
    state: dict = {}
    budget = {"max_requests_per_day": 5}                       # small enough that exhaustion is reachable

    # Pure local scans: fire (True) every cooldown window, never charge.
    for i in range(3):
        holder["now"] = now + _td(minutes=6 * i)
        assert await maybe_repair_warmup_gaps(status, state, clock=clock, calendar=calendar,
                                              repair=scan_only, **budget)
    assert len(calls) == 3 and state["requests"] == 0 and state["attempts"] == 3

    # Instruments-map misses only (pre-network): free — the budget survives until the map heals.
    _, _, _, calls_ut, unknown_token = _repair_setup(now, fetched=0, bars_written=0,
                                                     failed=("unknown_instrument_token",))
    holder["now"] = now + _td(minutes=20)
    assert await maybe_repair_warmup_gaps(status, state, clock=clock, calendar=calendar,
                                          repair=unknown_token, **budget)
    assert len(calls_ut) == 1 and state["requests"] == 0

    # A repair that raises: swallowed, returns True, charges one request per intraday blocker (2).
    async def boom(frm, to):
        raise RuntimeError("kite down")

    holder["now"] = now + _td(minutes=26)
    assert await maybe_repair_warmup_gaps(status, state, clock=clock, calendar=calendar,
                                          repair=boom, **budget)
    assert len(status.blockers) == 2 and state["requests"] == 2

    # Real broker failure spans: charged one request each.
    _, _, _, calls_f, broker_fail = _repair_setup(now, fetched=0, bars_written=0, failed=("ReadTimeout",))
    holder["now"] = now + _td(minutes=32)
    assert await maybe_repair_warmup_gaps(status, state, clock=clock, calendar=calendar,
                                          repair=broker_fail, **budget)
    assert len(calls_f) == 1 and state["requests"] == 3

    # The UNFILLABLE hole (fetch completed, zero bars landed): charged — the case the cap exists for.
    _, _, _, calls_u, unfillable = _repair_setup(now, fetched=2, bars_written=0)
    holder["now"] = now + _td(minutes=38)
    assert await maybe_repair_warmup_gaps(status, state, clock=clock, calendar=calendar,
                                          repair=unfillable, **budget)
    assert len(calls_u) == 1 and state["requests"] == 5        # two spans ⇒ two requests

    # The day's requests are now spent: the next window is refused, and nothing reaches the broker.
    _, _, _, calls_x, after = _repair_setup(now, fetched=1)
    holder["now"] = now + _td(minutes=44)
    assert not await maybe_repair_warmup_gaps(status, state, clock=clock, calendar=calendar,
                                              repair=after, **budget)
    assert calls_x == [] and state["requests"] == 5


@pytest.mark.asyncio
async def test_warmup_gap_repair_guards_token_session_shape_all_leave_budget_untouched(calendar) -> None:
    """Review round (the key finding): NO repair — and NO budget consumed — on an invalid token
    (a doomed pre-login attempt must not spend the budget before the login-lag seam hole even
    exists), when warm-up is ready, when blockers are daily-bars-shaped, outside the session, or
    in the first 2 minutes after open (no repairable window yet)."""
    from datetime import datetime as _dt
    from types import SimpleNamespace

    from engine.core.clock import IST
    from engine.ops.main import maybe_repair_warmup_gaps

    now = _dt(2026, 6, 17, 10, 5, 33, tzinfo=IST)
    holder, clock, gappy, calls, repair = _repair_setup(now)
    state: dict = {}

    # Invalid token: skipped, uncharged — the budget survives until login (2026-08-06 shape).
    assert not await maybe_repair_warmup_gaps(
        gappy, state, clock=clock, calendar=calendar, repair=repair, token_valid=lambda: False)
    assert calls == [] and state.get("requests", 0) == 0
    # Token comes back: the same state fires immediately (no cooldown was consumed).
    assert await maybe_repair_warmup_gaps(
        gappy, state, clock=clock, calendar=calendar, repair=repair, token_valid=lambda: True)
    assert len(calls) == 1

    ready = SimpleNamespace(ready=True, blockers=[])
    daily_only = SimpleNamespace(ready=False, blockers=["rsi2:TCS daily bars 195/200"])
    rejected_state: dict = {}
    assert not await maybe_repair_warmup_gaps(ready, rejected_state, clock=clock, calendar=calendar, repair=repair)
    assert not await maybe_repair_warmup_gaps(daily_only, rejected_state, clock=clock, calendar=calendar, repair=repair)
    holder["now"] = _dt(2026, 6, 17, 17, 0, tzinfo=IST)        # after close
    assert not await maybe_repair_warmup_gaps(gappy, rejected_state, clock=clock, calendar=calendar, repair=repair)
    holder["now"] = _dt(2026, 6, 14, 10, 0, tzinfo=IST)        # Sunday — no session
    assert not await maybe_repair_warmup_gaps(gappy, rejected_state, clock=clock, calendar=calendar, repair=repair)
    holder["now"] = _dt(2026, 6, 17, 9, 16, 30, tzinfo=IST)    # 90 s after open — window too young
    assert not await maybe_repair_warmup_gaps(gappy, rejected_state, clock=clock, calendar=calendar, repair=repair)
    # Every rejection left the budget untouched (the day-roll may initialize the dict, never spend).
    assert len(calls) == 1
    assert rejected_state.get("requests", 0) == 0 and rejected_state.get("attempts", 0) == 0
    assert rejected_state.get("last") is None


# --------------------------------------------------------------------------- login API bind confirmation
@pytest.mark.asyncio
async def test_serve_api_confirms_the_bind() -> None:
    """2026-07-21 lockout: _serve_api must CONFIRM the login-callback socket actually bound (a silent
    bind failure inside the fire-and-forget task is what locked the owner out). On a real ephemeral
    port the server flips ``started`` True; _stop_api then tears it down cleanly."""
    import socket
    from types import SimpleNamespace

    from fastapi import FastAPI

    # Reserve a definitely-free port, then release it for uvicorn (avoids a settings-model port=0 fight).
    probe = socket.socket()
    probe.bind(("127.0.0.1", 0))
    port = probe.getsockname()[1]
    probe.close()

    stub = SimpleNamespace(api=SimpleNamespace(host="127.0.0.1", port=port))
    task = await opsmain._serve_api(FastAPI(), stub)
    try:
        assert task is not None
        assert task._mt_server.started is True
    finally:
        await opsmain._stop_api(task)
    assert task.done()


@pytest.mark.asyncio
async def test_serve_api_survives_an_occupied_port() -> None:
    """The 2026-07-21 incident case itself: the port is ALREADY HELD (stale/second engine). uvicorn's
    own bind paths sys.exit(1) — which would escape the serve task and kill the whole engine loop
    before any alert could fire. _serve_api binds the socket itself instead: it must return ``None``
    (no SystemExit, no exception), and _stop_api(None) must no-op so shutdown teardown still runs."""
    import socket
    from types import SimpleNamespace

    from fastapi import FastAPI

    holder = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    if hasattr(socket, "SO_EXCLUSIVEADDRUSE"):  # mirror the engine's own Windows bind posture
        holder.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
    holder.bind(("127.0.0.1", 0))
    holder.listen(1)
    port = holder.getsockname()[1]
    try:
        stub = SimpleNamespace(api=SimpleNamespace(host="127.0.0.1", port=port))
        task = await opsmain._serve_api(FastAPI(), stub)   # must NOT raise SystemExit
        assert task is None
        await opsmain._stop_api(task)                      # None → no-op, never raises
    finally:
        holder.close()


# --------------------------------------------------------------------------- warm-up lift cadence (2026-08-03)
@pytest.mark.asyncio
async def test_warmup_refresh_lifts_freeze_without_a_login_event(conn, clock, calendar, tmp_path):
    """2026-08-03 gap: the warm-up lift hung off the post-login hook only, so a VALID-TOKEN
    mid-session restart (boot 12:22, ORB lookbacks short) froze entries with NOTHING to lift them —
    no login event ever fires on such a boot. The 60s refresh cadence must lift it by itself."""
    from engine.core.enums import Actor, RiskState
    from engine.core.protected_store import ProtectedStore
    from engine.core.types import TradeWindow
    from engine.ops.lifecycle import SessionLifecycle
    from engine.ops.main import refresh_and_lift_warmup
    from engine.ops.selftest import SelfTest
    from engine.ops.warmup import WarmupStatus
    from engine.risk.causes import RiskStateLatch
    from engine.risk.kill import KillSwitch
    from engine.risk.mode import ModeManager
    from tests.unit.test_lifecycle_selftest import OWNER_OK, REQUIRED_AT_STARTUP, FakeSecrets, FakeSettings

    cfg = tmp_path / "config"
    cfg.mkdir()
    (cfg / "limits.yaml").write_text("schema_version: 1\nlimits: {}\n", encoding="utf-8")
    (cfg / "envelope.yaml").write_text("schema_version: 1\nparameters: {}\n", encoding="utf-8")
    pstore = ProtectedStore(cfg, conn, clock)
    pstore.register_initial("limits.yaml", OWNER_OK)
    pstore.register_initial("envelope.yaml", OWNER_OK)

    mode = ModeManager(conn, clock, None, calendar)
    kill = KillSwitch(conn, clock)
    latch = RiskStateLatch(conn, clock, mode)
    mode.seed_trade_window_if_absent(TradeWindow(
        start=FakeSettings._TW.start_ist, end=FakeSettings._TW.end_ist, squareoff_buffer_min=5,
    ))

    class TogglingGate:
        def __init__(self):
            self.ready = False
        async def status(self):
            # A REGIME-class blocker: since 2026-09-13 only the freezing classes hold the risk state
            # and since 2026-09-17 DAILY is not one of them, so "still short => stays frozen" has to
            # be short in a class that still freezes.
            return WarmupStatus(ready=self.ready,
                                blockers=[] if self.ready else ["regime:NIFTY 50 daily bars 0/200"])

    gate = TogglingGate()
    st = SelfTest(conn=conn, clock=clock, settings=FakeSettings(), secrets=FakeSecrets(REQUIRED_AT_STARTUP),
                  protected_store=pstore, kill_switch=kill, mode_manager=mode)
    lifecycle = SessionLifecycle(
        conn=conn, clock=clock, calendar=calendar, settings=FakeSettings(),
        mode_manager=mode, kill_switch=kill, self_test=st, catch_up=None,
        warmup_gate=gate, latch=latch, build_version="test-0",
    )

    # The mid-session cold boot: warm-up short => FROZEN via the cause the lifecycle owns.
    await latch.set_cause("warmup_ready", RiskState.FROZEN, "orb lookback short", Actor.RISK_GATE)
    assert mode.risk_state() == RiskState.FROZEN

    holder: dict = {"status": None}
    await refresh_and_lift_warmup(gate, holder, mode, lifecycle)      # still short => stays frozen
    assert holder["status"].ready is False
    assert mode.risk_state() == RiskState.FROZEN

    gate.ready = True                                                  # coverage completes ~12:40
    await refresh_and_lift_warmup(gate, holder, mode, lifecycle)
    assert holder["status"].ready is True
    assert mode.risk_state() == RiskState.NORMAL                       # lifted with NO login event


@pytest.mark.asyncio
async def test_warmup_refresh_lifts_with_only_the_intraday_class_short(conn, clock, calendar, tmp_path):
    """2026-09-13 per-class scoping, the 60 s cadence half: the lift is owed to the FREEZING classes
    (REGIME ∪ unattributable since 2026-09-17). The market-wide one-bar hole a reconnect leaves
    (09-09 14:47) must not hold the daily-bar legs frozen for the rest of the session, and the
    intraday class's readiness is logged ONCE PER TRANSITION — not on each of the ~375 ticks a
    session-long hole would see."""
    from engine.core.enums import Actor, RiskState
    from engine.core.protected_store import ProtectedStore
    from engine.core.types import TradeWindow
    from engine.ops.lifecycle import SessionLifecycle
    from engine.ops.main import _log_intraday_class_transition, refresh_and_lift_warmup
    from engine.ops.selftest import SelfTest
    from engine.ops.warmup import WarmupStatus
    from engine.risk.causes import RiskStateLatch
    from engine.risk.kill import KillSwitch
    from engine.risk.mode import ModeManager
    from tests.unit.test_lifecycle_selftest import OWNER_OK, REQUIRED_AT_STARTUP, FakeSecrets, FakeSettings

    cfg = tmp_path / "config"
    cfg.mkdir()
    (cfg / "limits.yaml").write_text("schema_version: 1\nlimits: {}\n", encoding="utf-8")
    (cfg / "envelope.yaml").write_text("schema_version: 1\nparameters: {}\n", encoding="utf-8")
    pstore = ProtectedStore(cfg, conn, clock)
    pstore.register_initial("limits.yaml", OWNER_OK)
    pstore.register_initial("envelope.yaml", OWNER_OK)

    mode = ModeManager(conn, clock, None, calendar)
    kill = KillSwitch(conn, clock)
    latch = RiskStateLatch(conn, clock, mode)
    mode.seed_trade_window_if_absent(TradeWindow(
        start=FakeSettings._TW.start_ist, end=FakeSettings._TW.end_ist, squareoff_buffer_min=5,
    ))

    class IntradayHoleGate:
        async def status(self):
            return WarmupStatus(ready=False, blockers=["orb:AAA bars 165/182", "orb:BBB bars 165/182"])

    gate = IntradayHoleGate()
    st = SelfTest(conn=conn, clock=clock, settings=FakeSettings(), secrets=FakeSecrets(REQUIRED_AT_STARTUP),
                  protected_store=pstore, kill_switch=kill, mode_manager=mode)
    lifecycle = SessionLifecycle(
        conn=conn, clock=clock, calendar=calendar, settings=FakeSettings(),
        mode_manager=mode, kill_switch=kill, self_test=st, catch_up=None,
        warmup_gate=gate, latch=latch, build_version="test-0",
    )
    await latch.set_cause("warmup_ready", RiskState.FROZEN, "daily bars short", Actor.RISK_GATE)
    assert mode.risk_state() == RiskState.FROZEN

    alerts: list[tuple[str, str]] = []

    async def alert(severity, message):
        alerts.append((severity, message))

    holder: dict = {"status": None}
    await refresh_and_lift_warmup(gate, holder, mode, lifecycle, alert=alert)
    assert holder["status"].ready is False                 # coverage IS still missing...
    assert mode.risk_state() == RiskState.NORMAL           # ...but not in a class that freezes
    assert holder["intraday_short"] is True
    # The class reaches neither the risk state nor a WARMUP_FROZEN page any more, so a hole that
    # opens MID-SESSION has to reach the owner from here or not at all.
    assert len(alerts) == 1 and alerts[0][0] == "warning"
    assert "INTRADAY coverage short" in alerts[0][1] and "orb:AAA bars 165/182" in alerts[0][1]
    assert "NOT frozen" in alerts[0][1]

    # The memo is per TRANSITION: a second tick on the same standing shortfall says nothing, and a
    # heal flips it back so the recovery is visible too.
    assert _log_intraday_class_transition(holder, holder["status"]) is None
    assert holder["intraday_short"] is True
    severity, message = _log_intraday_class_transition(holder, WarmupStatus(ready=True))
    assert holder["intraday_short"] is False
    assert severity == "info" and "restored" in message
    await refresh_and_lift_warmup(gate, holder, mode, lifecycle, alert=alert)   # short again
    assert [s for s, _m in alerts] == ["warning", "warning"]


@pytest.mark.asyncio
async def test_warmup_refresh_lifts_with_only_the_daily_class_short(conn, clock, calendar, tmp_path):
    """2026-09-17 per-SYMBOL scoping, the 60 s cadence half: DAILY stopped freezing entries, so one
    symbol's missing daily history no longer holds the book frozen (2026-09-15 OLAELEC held the
    global ``warmup_ready`` cause for ~12 hours). The lift happens, and — because no WARMUP_FROZEN
    page carries the state any more — the owner gets the DAILY transition notice instead, naming the
    symbol COUNT so a market-wide daily hole reads as an infrastructure failure at a glance."""
    from engine.core.enums import Actor, RiskState
    from engine.core.protected_store import ProtectedStore
    from engine.core.types import TradeWindow
    from engine.ops.lifecycle import SessionLifecycle
    from engine.ops.main import refresh_and_lift_warmup
    from engine.ops.selftest import SelfTest
    from engine.ops.warmup import WarmupStatus
    from engine.risk.causes import RiskStateLatch
    from engine.risk.kill import KillSwitch
    from engine.risk.mode import ModeManager
    from tests.unit.test_lifecycle_selftest import OWNER_OK, REQUIRED_AT_STARTUP, FakeSecrets, FakeSettings

    cfg = tmp_path / "config"
    cfg.mkdir()
    (cfg / "limits.yaml").write_text("schema_version: 1\nlimits: {}\n", encoding="utf-8")
    (cfg / "envelope.yaml").write_text("schema_version: 1\nparameters: {}\n", encoding="utf-8")
    pstore = ProtectedStore(cfg, conn, clock)
    pstore.register_initial("limits.yaml", OWNER_OK)
    pstore.register_initial("envelope.yaml", OWNER_OK)

    mode = ModeManager(conn, clock, None, calendar)
    kill = KillSwitch(conn, clock)
    latch = RiskStateLatch(conn, clock, mode)
    mode.seed_trade_window_if_absent(TradeWindow(
        start=FakeSettings._TW.start_ist, end=FakeSettings._TW.end_ist, squareoff_buffer_min=5,
    ))

    class DailyHoleGate:
        async def status(self):
            return WarmupStatus(ready=False, blockers=[
                "rsi2/trend/mom:OLAELEC daily bars 193/200",
                "rsi2/trend/mom:M&M daily bars 12/200",
            ])

    gate = DailyHoleGate()
    st = SelfTest(conn=conn, clock=clock, settings=FakeSettings(), secrets=FakeSecrets(REQUIRED_AT_STARTUP),
                  protected_store=pstore, kill_switch=kill, mode_manager=mode)
    lifecycle = SessionLifecycle(
        conn=conn, clock=clock, calendar=calendar, settings=FakeSettings(),
        mode_manager=mode, kill_switch=kill, self_test=st, catch_up=None,
        warmup_gate=gate, latch=latch, build_version="test-0",
    )
    await latch.set_cause("warmup_ready", RiskState.FROZEN, "daily bars short", Actor.RISK_GATE)
    assert mode.risk_state() == RiskState.FROZEN

    alerts: list[tuple[str, str]] = []

    async def alert(severity, message):
        alerts.append((severity, message))

    holder: dict = {"status": None}
    await refresh_and_lift_warmup(gate, holder, mode, lifecycle, alert=alert)
    assert holder["status"].ready is False                 # coverage IS still missing...
    assert mode.risk_state() == RiskState.NORMAL           # ...but not in a class that freezes
    assert holder["daily_short"] is True
    assert holder.get("intraday_short") is False           # the intraday class is clean and silent
    assert len(alerts) == 1 and alerts[0][0] == "warning"
    assert "DAILY coverage short for 2 symbol(s)" in alerts[0][1]
    assert "OLAELEC" in alerts[0][1] and "M&M" in alerts[0][1]
    assert "NOT frozen" in alerts[0][1]

    # The memo is per TRANSITION: a second tick on the same standing shortfall says nothing.
    await refresh_and_lift_warmup(gate, holder, mode, lifecycle, alert=alert)
    assert len(alerts) == 1


def test_the_daily_notice_fires_once_per_transition_with_symbol_counts() -> None:
    """The DAILY half of the per-class notice (2026-09-17). It counts SYMBOLS, not blocker lines:
    the signal the owner needs from a market-wide daily hole is "how much of the book", and the
    first three names make a one-symbol case diagnosable without the log."""
    from datetime import date as _date

    from engine.ops.main import _log_class_transition
    from engine.ops.warmup import CLASS_DAILY, WarmupStatus

    holder: dict = {}
    short = WarmupStatus(ready=False, blockers=[
        "rsi2/trend/mom:OLAELEC daily bars 193/200",
        "rsi2/trend/mom:M&M daily bars 12/200",
    ])
    notice = _log_class_transition(holder, short, CLASS_DAILY)
    assert notice is not None and notice[0] == "warning"
    assert "DAILY coverage short for 2 symbol(s) (OLAELEC, M&M)" in notice[1]
    assert "swing/position candidates for those symbols are refused one by one" in notice[1]
    assert "entries are NOT frozen by warm-up" in notice[1]
    assert holder["daily_short"] is True

    # Second tick, same standing shortfall: nothing new to say.
    assert _log_class_transition(holder, short, CLASS_DAILY) is None

    # The nightly job lands: the recovery IS reported.
    restored = _log_class_transition(holder, WarmupStatus(ready=True), CLASS_DAILY)
    assert restored is not None and restored[0] == "info"
    assert restored[1] == ("warm-up: DAILY coverage restored — swing/position candidates are "
                           "accepted again")
    assert holder["daily_short"] is False

    # A FIRST observation of a ready daily class pages nobody — every clean boot would otherwise
    # announce a recovery from nothing.
    fresh: dict = {}
    assert _log_class_transition(fresh, WarmupStatus(ready=True), CLASS_DAILY) is None
    assert fresh["daily_short"] is False

    # …and while a FREEZING class holds entries frozen, the daily notice is silent too: "entries are
    # NOT frozen" would be the opposite of the live risk state.
    frozen: dict = {}
    both = WarmupStatus(ready=False, blockers=[
        "regime:NIFTY 50 daily bars 0/200", "rsi2/trend/mom:OLAELEC daily bars 193/200",
    ])
    assert _log_class_transition(frozen, both, CLASS_DAILY) is None
    assert "daily_short" not in frozen

    # The memos are per class and per IST day, keyed off the class name.
    day: dict = {}
    assert _log_class_transition(day, short, CLASS_DAILY, today=_date(2026, 9, 15)) is not None
    assert day["daily_short"] is True and day["daily_short_day"] == _date(2026, 9, 15)
    assert _log_class_transition(day, WarmupStatus(ready=True), CLASS_DAILY,
                                 today=_date(2026, 9, 16)) is None
    assert day["daily_short"] is False and day["daily_short_day"] == _date(2026, 9, 16)


@pytest.mark.asyncio
async def test_a_first_observation_of_a_ready_intraday_class_pages_nobody() -> None:
    """The transition memo starts UNSET, so every clean boot's first refresh is a transition. It
    must not read as a recovery: "coverage restored" on a boot that was never short is the noise
    that teaches an owner to ignore the line that matters."""
    from engine.ops.main import _log_intraday_class_transition
    from engine.ops.warmup import WarmupStatus

    holder: dict = {}
    assert _log_intraday_class_transition(holder, WarmupStatus(ready=True)) is None
    assert holder["intraday_short"] is False
    # A first observation of a SHORT class IS a notice — the owner learns it on the tick, not only
    # from a boot report that may be hours old.
    holder = {}
    notice = _log_intraday_class_transition(
        holder, WarmupStatus(ready=False, blockers=["orb:AAA bars 1/50"])
    )
    assert notice is not None and notice[0] == "warning"

    # A duck-typed status with no per-class answer is not a transition at all (nothing to say).
    assert _log_intraday_class_transition({}, SimpleNamespace(ready=False, blockers=[])) is None


def test_the_intraday_notice_is_silent_while_a_freezing_class_holds_entries_frozen() -> None:
    """Re-review 2026-09-13: with a FREEZING class ALSO short, lifecycle step 6 DID freeze and
    WARMUP_FROZEN owns that state — a 60 s notice saying "entries are NOT frozen" would state the
    opposite of the live risk state. Nothing is said and the memo is left alone, so the intraday
    shortfall is reported on the first tick after the freeze lifts, when the wording is true. The
    mirror holds: intraday healing under a standing freeze is no "restored". (Since 2026-09-17 the
    freezing set is REGIME ∪ unattributable — a DAILY line no longer silences this notice, because
    it no longer freezes anything.)"""
    from engine.ops.main import _log_intraday_class_transition
    from engine.ops.warmup import WarmupStatus

    regime_short = "regime:NIFTY 50 daily bars 0/200"
    holder: dict = {}
    mixed = WarmupStatus(ready=False, blockers=[regime_short, "orb:AAA bars 3/5"])
    assert _log_intraday_class_transition(holder, mixed) is None
    assert "intraday_short" not in holder
    # An unattributable blocker holds EVERY class down (fail closed): the same silence.
    unknown = WarmupStatus(ready=False, blockers=["warmup check failed", "orb:AAA bars 3/5"])
    assert _log_intraday_class_transition(holder, unknown) is None
    assert "intraday_short" not in holder
    # The regime class heals while intraday is still short: NOW the notice is true, and it is sent.
    notice = _log_intraday_class_transition(
        holder, WarmupStatus(ready=False, blockers=["orb:AAA bars 3/5"])
    )
    assert notice is not None and notice[0] == "warning"
    assert "NOT frozen by warm-up" in notice[1] and "orb:AAA bars 3/5" in notice[1]
    assert holder["intraday_short"] is True
    # Intraday heals but the regime class is short again: entries are frozen — no "restored".
    frozen_again = WarmupStatus(ready=False, blockers=[regime_short])
    assert _log_intraday_class_transition(holder, frozen_again) is None
    assert holder["intraday_short"] is True


def test_the_intraday_transition_memo_is_scoped_to_the_ist_day() -> None:
    """The intraday class reads trivially READY whenever there is no session (before the open, a
    non-trading day), so a memo that survived midnight would manufacture "coverage restored" out of
    the rollover. Given ``today`` the memo resets on a new day: the first reading of the day is a
    first observation (never a recovery), and a real heal after a real hole still is one."""
    from datetime import date as _date

    from engine.ops.main import _log_intraday_class_transition
    from engine.ops.warmup import WarmupStatus

    holder: dict = {}
    short = WarmupStatus(ready=False, blockers=["orb:AAA bars 3/5"])
    assert _log_intraday_class_transition(holder, short, today=_date(2026, 9, 11)) is not None
    assert holder["intraday_short"] is True and holder["intraday_short_day"] == _date(2026, 9, 11)
    # Midnight: the trivially-ready reading on the new day is a first observation, not a recovery.
    assert _log_intraday_class_transition(holder, WarmupStatus(ready=True), today=_date(2026, 9, 12)) is None
    assert holder["intraday_short"] is False and holder["intraday_short_day"] == _date(2026, 9, 12)
    # Same day, a real hole then a real heal: the recovery IS reported.
    assert _log_intraday_class_transition(holder, short, today=_date(2026, 9, 12)) is not None
    notice = _log_intraday_class_transition(holder, WarmupStatus(ready=True), today=_date(2026, 9, 12))
    assert notice is not None and notice[0] == "info" and "restored" in notice[1]


async def test_the_prescreen_reads_the_same_warmup_snapshot_as_the_gate_context(
    tmp_path, monkeypatch, migrated_db_template
) -> None:
    """CONSTRAINT (2026-09-13): the pre-screen's intraday-warm-up refusal and the gate's
    ``warmup_ready`` verdict must be the SAME fact, or the pre-screen would re-arm candidates the
    gate would have approved (or spend analyst calls the gate then rejects). ONE snapshot function
    is wired to the real gate context, the paper gate context and the pipeline."""
    from engine.ops.warmup import WarmupStatus

    built: dict[str, list] = {}
    _spy(monkeypatch, "GateContextBuilder", built)
    _spy(monkeypatch, "RecommendationPipeline", built)
    async with composed_engine(tmp_path, monkeypatch, db_template=migrated_db_template, paper=True) as c:
        assert c.engine.paper is not None
        fns = [kw["warmup_status_fn"] for _, kw, _ in built["GateContextBuilder"] + built["RecommendationPipeline"]]
        assert [kw.get("scope", "real") for _, kw, _ in built["GateContextBuilder"]] == ["real", "paper"]
        assert len(fns) == 3 and len(set(fns)) == 1
        (snapshot,) = set(fns)
        assert snapshot().ready is False                        # fail-closed until the first refresh
        refreshed = WarmupStatus(ready=True)
        c.engine.warmup_holder["status"] = refreshed
        assert snapshot() is refreshed                          # the holder the boot and the 60 s job fill


# =========================================================================== WO-15 boot ordering
# (i) boot catch-up runs load-bearing data steps only; the news chain + digest + planner fire as
# one-shots AFTER scheduler.start(); (iii) engine_ready never waits on the chain.
# The machinery is pinned in test_catchup_runner.py; here it is the composition root's boot TAIL.


class _FakeScheduler:
    """Records arming order against the same list the jobs append to."""

    def __init__(self, events: list[str]) -> None:
        self._events = events
        self.started = False

    def start(self) -> None:
        self.started = True
        self._events.append("scheduler_armed")

    def is_running(self) -> bool:
        """WO-25c: the boot-contract surface the real Scheduler exposes off APScheduler's own state."""
        return self.started


def _post_arm_registry(events: list[str], *, gate: asyncio.Event | None = None,
                       boom: bool = False) -> JobRegistry:
    """universe_build (load-bearing) + news_chain (deferred, optionally wedged/failing)."""
    async def universe() -> None:
        events.append("universe_build")

    async def chain() -> None:
        events.append("chain_started")
        if gate is not None:
            await gate.wait()
        if boom:
            raise RuntimeError("news chain blew up")
        events.append("chain_finished")

    reg = JobRegistry()
    reg.register(JobSpec(JOB_UNIVERSE, JobClass.RUN_LATEST, time(8, 30), universe, order=10))
    reg.register(JobSpec(opsmain.JOB_NEWS_CHAIN, JobClass.RUN_LATEST, time(8, 25), chain, order=20))
    return reg


def test_every_post_arm_job_is_a_registered_never_safety_critical_job(calendar) -> None:
    """WO-15's deferred set must name real registry ids — a typo would silently defer nothing (and
    silently never fire it, since the post-arm pass selects BY id)."""
    fns = _all_noop_fns()
    fns[opsmain.JOB_CATALYST_DIGEST] = _noop
    fns[opsmain.JOB_PREOPEN_PLANNER] = _noop
    fns[opsmain.JOB_RECO_EXPIRE] = _noop
    fns[opsmain.JOB_TIME_EXIT_CHECK] = _noop
    fns[opsmain.JOB_REC_OUTCOMES] = _noop_dated
    fns[opsmain.JOB_WEEKLY_SUMMARY] = _noop_dated
    fns[opsmain.JOB_NIGHTLY_REVIEW] = _noop_dated
    by_id = {s.job_id: s for s in build_job_registry(load_settings(), fns, calendar=calendar).specs()}
    assert set(POST_ARM_JOB_IDS) <= set(by_id)
    # None of them is safety-critical: the deferred set may never contain an entry-gating job.
    assert all(by_id[j].job_class is not JobClass.SAFETY_CRITICAL for j in POST_ARM_JOB_IDS)


def test_early_hydration_set_is_the_real_pre_open_chain() -> None:
    """§2.6 early hydration (2026-09-09): same typo risk as the deferred set — the hook selects BY id,
    so a name that is not a registry job would silently hydrate nothing. Also pins the deliberate
    exclusions: ``instruments`` (Kite regenerates its dump ~08:00, so a 06:30 refresh could pin a
    stale map), the Sunday-only ``sector_map``, and DATE_KEYED jobs (``hydrate_ahead`` rejects them)."""
    fns = _all_noop_fns()
    fns[opsmain.JOB_CATALYST_DIGEST] = _noop
    fns[opsmain.JOB_PREOPEN_PLANNER] = _noop
    fns[opsmain.JOB_RECO_EXPIRE] = _noop
    fns[opsmain.JOB_NIGHTLY_REVIEW] = _noop_dated
    by_id = {s.job_id: s for s in build_job_registry(load_settings(), fns).specs()}

    assert set(opsmain.EARLY_HYDRATION_JOB_IDS) <= set(by_id)
    assert all(
        by_id[j].job_class is not JobClass.DATE_KEYED for j in opsmain.EARLY_HYDRATION_JOB_IDS
    )
    assert opsmain.JOB_INSTRUMENTS not in opsmain.EARLY_HYDRATION_JOB_IDS
    assert JOB_SECTOR_MAP not in opsmain.EARLY_HYDRATION_JOB_IDS
    assert "token_check" not in by_id                     # not a registry job at all (no watermark)


@pytest.mark.asyncio
async def test_scheduler_is_armed_before_the_deferred_chain_fires(conn, clock, calendar) -> None:
    events: list[str] = []
    catch_up = CatchUpRunner(conn, clock, calendar, _post_arm_registry(events),
                             deferred=POST_ARM_JOB_IDS)

    task = start_scheduler_and_fire_post_arm(_FakeScheduler(events), catch_up, armed=asyncio.Event())

    assert events == ["scheduler_armed"]           # the chain has not even started yet
    await task
    assert events == ["scheduler_armed", "chain_started", "chain_finished"]
    assert catch_up.was_run(opsmain.JOB_NEWS_CHAIN, clock.today()) is True


@pytest.mark.asyncio
async def test_armed_event_is_set_after_the_one_shot_is_dispatched(conn, clock, calendar) -> None:
    """§2.6 early hydration (2026-09-09): the login hook must never run the pre-open chain ahead of
    ``scheduler.start()`` (the WO-15 firing-point rule), so it waits on this event — released AFTER
    the post-arm one-shot is dispatched, not before. Released first, the woken hook wins the
    single-flight pass lock and the one-shot degrades to a ``skipped_in_flight`` no-op (the deferred
    chain would then only run at its own 08:25 clock). Dispatched first, the one-shot holds the lock
    and the hook simply queues behind it and finds the watermark."""
    events: list[str] = []
    gate = asyncio.Event()
    catch_up = CatchUpRunner(conn, clock, calendar, _post_arm_registry(events, gate=gate),
                             deferred=POST_ARM_JOB_IDS)
    armed = asyncio.Event()
    outcomes: dict[str, str] = {}

    async def early_login() -> None:                 # the EarlyHydration hook's shape, in miniature
        await armed.wait()
        outcomes.update(
            await catch_up.hydrate_ahead([opsmain.JOB_NEWS_CHAIN], reason="early_login")
        )

    hook = asyncio.create_task(early_login())
    await asyncio.sleep(0)                           # parked on `armed`, exactly like a 06:30 login

    task = start_scheduler_and_fire_post_arm(_FakeScheduler(events), catch_up, armed=armed)

    assert armed.is_set() is True
    assert events == ["scheduler_armed"]             # released at arming, not after the chain ran
    await asyncio.sleep(0)                           # both wake, in dispatch order
    gate.set()
    await asyncio.wait_for(task, timeout=5)
    await asyncio.wait_for(hook, timeout=5)

    assert events == ["scheduler_armed", "chain_started", "chain_finished"]
    assert outcomes == {opsmain.JOB_NEWS_CHAIN: "already_run"}   # the one-shot got the lock first


@pytest.mark.asyncio
async def test_news_backlog_boot_reaches_engine_ready_in_load_bearing_time(conn, clock, calendar) -> None:
    """WO-15 acceptance: a boot whose news chain never returns (the 2026-08-10 wedge: 8 h inside
    lifecycle.startup, ahead of arming) must still reach engine_ready. The whole boot tail runs under
    a timeout — if the chain were replayed inside the boot pass again, this test would hang."""
    events: list[str] = []
    wedged = asyncio.Event()                       # never set: the chain hangs forever
    catch_up = CatchUpRunner(conn, clock, calendar, _post_arm_registry(events, gate=wedged),
                             deferred=POST_ARM_JOB_IDS)
    scheduler = _FakeScheduler(events)

    async def boot_tail() -> asyncio.Task | None:
        await catch_up.catch_up()                  # what SessionLifecycle.startup awaits (2.6 step 5)
        task = start_scheduler_and_fire_post_arm(scheduler, catch_up, armed=asyncio.Event())
        events.append("engine_ready")
        return task

    task = await asyncio.wait_for(boot_tail(), timeout=5)

    assert events == ["universe_build", "scheduler_armed", "engine_ready"]
    assert scheduler.started is True
    await asyncio.sleep(0)                         # let the one-shot start and block
    assert events[-1] == "chain_started" and task is not None and not task.done()
    assert catch_up.was_run(opsmain.JOB_NEWS_CHAIN, clock.today()) is False

    await cancel_post_arm(task)                    # shutdown never waits the chain out
    assert task.done()


@pytest.mark.asyncio
async def test_a_failing_post_arm_chain_records_a_failed_watermark_and_never_raises(
    conn, clock, calendar
) -> None:
    events: list[str] = []
    catch_up = CatchUpRunner(conn, clock, calendar, _post_arm_registry(events, boom=True),
                             deferred=POST_ARM_JOB_IDS)

    task = start_scheduler_and_fire_post_arm(_FakeScheduler(events), catch_up, armed=asyncio.Event())
    await task                                     # a chain failure never escapes into the boot path

    assert task.exception() is None
    assert catch_up.was_run(opsmain.JOB_NEWS_CHAIN, clock.today()) is False   # failed watermark
    row = conn.execute(
        "SELECT status FROM job_runs WHERE job_id=? AND run_for_date=?",
        (opsmain.JOB_NEWS_CHAIN, clock.today().isoformat()),
    ).fetchone()
    assert row["status"] == "failed"


@pytest.mark.asyncio
async def test_rollback_flag_restores_the_pre_wo15_firing_point(conn, clock, calendar, monkeypatch) -> None:
    """WO-15 risk note: flipping ``DEFER_POST_ARM_JOBS`` off restores the old boot exactly — the
    deferred set empties (so the boot pass runs the chain again) and no one-shot is fired."""
    monkeypatch.setattr(opsmain, "DEFER_POST_ARM_JOBS", False)
    events: list[str] = []
    catch_up = CatchUpRunner(conn, clock, calendar, _post_arm_registry(events), deferred=())

    await catch_up.catch_up()                      # the boot pass, with the flag off
    assert events == ["universe_build", "chain_started", "chain_finished"]

    scheduler = _FakeScheduler(events)
    armed = asyncio.Event()
    assert start_scheduler_and_fire_post_arm(scheduler, catch_up, armed=armed) is None
    assert armed.is_set() is True                  # the login hook is released either way
    assert scheduler.started is True               # arming still happens, unconditionally


# ================================ compaction lane + WO-21 (ii): no in-session tick compaction (2026-08-20)
# An 11:26 IST crash-recovery boot fired the post-arm compaction backlog DURING the session: ~16 GB
# peak, tick processing >1 h behind wall clock, /db/query unresponsive, Telegram timing out. Since
# 2026-09-29 compaction runs in its own runner (CompactionLane), which starts no pass inside that
# window — and whose pass lock no other catch-up waits on.

# Trading day (Wed), IST times around the session; and a weekend inside the same clock window.
_IN_SESSION = datetime(2026, 6, 17, 11, 26, tzinfo=IST)      # the incident's own boot time
_EVENING = datetime(2026, 6, 17, 20, 0, tzinfo=IST)
_WEEKEND_MIDDAY = datetime(2026, 6, 20, 11, 26, tzinfo=IST)  # Saturday


def _clock_at(when: datetime) -> Clock:
    return Clock(time_source=lambda: when)


def _compaction_lane(conn, clock, calendar, events: list[str], *, last_done: date,
                     gate: asyncio.Event | None = None) -> CompactionLane:
    """A lane with a real compaction BACKLOG — the 2026-08-20 shape. ``tick_compact`` is DATE_KEYED
    at 22:30, so without a prior watermark today's fire-time has not passed and there is nothing to
    replay; seeding a success at ``last_done`` makes the days after it genuinely missed. ``gate``
    holds each compaction open until set."""
    async def compact(d: date) -> None:
        events.append(f"compact:{d.isoformat()}")
        if gate is not None:
            await gate.wait()

    reg = JobRegistry()
    reg.register(JobSpec(opsmain.JOB_TICK_COMPACT, JobClass.DATE_KEYED, time(22, 30), compact, order=90))
    runner = CatchUpRunner(conn, clock, calendar, reg)
    runner.record_run(opsmain.JOB_TICK_COMPACT, last_done)
    return CompactionLane(runner, clock, calendar)


@pytest.mark.asyncio
async def test_a_running_compaction_never_blocks_the_main_catch_up(conn, calendar) -> None:
    """2026-09-28/29: in the shared runner a backlog compaction held the pass lock from 21:27 to past
    10:07 the next morning; every 30-min sweep logged ``catch_up_skipped_in_flight`` and the 08:35
    digest the host slept through was never caught up. In its own lane, compaction delays nothing
    but compaction."""
    clock = _clock_at(_EVENING)
    events: list[str] = []
    gate = asyncio.Event()                         # the compaction outlives the whole test body
    lane = _compaction_lane(conn, clock, calendar, events, last_done=date(2026, 6, 14), gate=gate)
    main = CatchUpRunner(conn, clock, calendar, _post_arm_registry(events))

    task = lane.spawn("post_arm")
    await asyncio.sleep(0)
    assert events == ["compact:2026-06-15"]        # parked inside the compaction

    result = await asyncio.wait_for(main.catch_up(scope=CatchUpScope.ALL), timeout=5)
    assert result.skipped_in_flight is False
    assert main.was_run(opsmain.JOB_NEWS_CHAIN, clock.today()) is True
    assert not task.done()

    gate.set()
    await asyncio.wait_for(task, timeout=5)
    assert events[-1] == "compact:2026-06-16"


@pytest.mark.asyncio
async def test_in_session_the_lane_starts_nothing_and_logs_it(conn, calendar, caplog) -> None:
    clock = _clock_at(_IN_SESSION)
    events: list[str] = []
    lane = _compaction_lane(conn, clock, calendar, events, last_done=date(2026, 6, 14))

    with caplog.at_level(logging.INFO, logger="engine.ops.main"):
        assert lane.spawn("sweep") is None

    # The backlog (Mon 15th + Tue 16th) is real — the evening test below runs it — and NONE of it
    # starts inside the session: that pass is what peaked at ~16 GB on 2026-08-20.
    assert events == []
    skips = [r for r in caplog.records if r.getMessage() == "post_arm_skipped_in_session"]
    assert len(skips) == 1
    assert skips[0].job_ids == sorted(opsmain.LANE_JOB_IDS) and skips[0].path == "sweep"
    assert skips[0].now.startswith("2026-06-17T11:26")
    # A veto is not a watermark: nothing is recorded, so the first pass after the window owns it.
    assert conn.execute(
        "SELECT count(*) FROM job_runs WHERE job_id=? AND run_for_date>?",
        (opsmain.JOB_TICK_COMPACT, "2026-06-14"),
    ).fetchone()[0] == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("when", "trading_day", "last_done", "expected_events"),
    [
        (_EVENING, True, date(2026, 6, 14), ["compact:2026-06-15", "compact:2026-06-16"]),
        (_WEEKEND_MIDDAY, False, date(2026, 6, 17), ["compact:2026-06-18", "compact:2026-06-19"]),
    ],
    ids=["evening", "weekend"],
)
async def test_a_lane_pass_in_the_clock_window_runs_the_backlog(
    conn, calendar, caplog, when: datetime, trading_day: bool, last_done: date,
    expected_events: list[str],
) -> None:
    """The gate protects a SESSION, not a wall-clock range: a Saturday 11:26 recovery boot is
    exactly when the fragment backlog should be collapsed."""
    clock = _clock_at(when)
    assert calendar.is_trading_day(clock.today()) is trading_day
    events: list[str] = []
    lane = _compaction_lane(conn, clock, calendar, events, last_done=last_done)

    with caplog.at_level(logging.INFO, logger="engine.ops.main"):
        await lane.spawn("post_arm")

    assert events == expected_events
    assert not [r for r in caplog.records if r.getMessage() == "post_arm_skipped_in_session"]


@pytest.mark.asyncio
async def test_the_lane_runs_one_pass_at_a_time_and_cancels_on_shutdown(conn, calendar) -> None:
    clock = _clock_at(_EVENING)
    events: list[str] = []
    lane = _compaction_lane(conn, clock, calendar, events, last_done=date(2026, 6, 14),
                            gate=asyncio.Event())

    task = lane.spawn("post_arm")
    await asyncio.sleep(0)
    assert lane.spawn("sweep") is None             # the running pass owns the backlog
    await lane.cancel()
    assert task.cancelled()
    assert events == ["compact:2026-06-15"]
    assert lane.spawn("sweep") is not None         # a finished (here: cancelled) pass frees the lane
    await lane.cancel()


@pytest.mark.parametrize(
    ("when", "inside"),
    [
        (datetime(2026, 6, 17, 8, 44, tzinfo=IST), False),     # just before open-side bound
        (datetime(2026, 6, 17, 8, 45, tzinfo=IST), True),      # inclusive lower bound
        (datetime(2026, 6, 17, 15, 45, tzinfo=IST), True),     # inclusive upper bound
        (datetime(2026, 6, 17, 15, 46, tzinfo=IST), False),    # just after close-side bound
        (datetime(2026, 6, 17, 3, 0, tzinfo=IST), False),      # small hours
        (_WEEKEND_MIDDAY, False),                              # the clock window, but no session
    ],
)
def test_in_session_window_bounds(calendar, when: datetime, inside: bool) -> None:
    assert opsmain._in_session_window(_clock_at(when), calendar) is inside


# =========================================================================== WO-14 (c) translation
# preopen_planner/nightly_review return a tri-state; the composition-root wrappers translate it into
# the ok-bearing watermark verdict. A governor block is a CORRECT outcome (success watermark, no
# retry - a blind retry loop spends LLM budget); only a harness failure is retryable.


class _FakeAdvisoryJob:
    """Mirrors PreopenPlannerJob/NightlyReviewJob's contract: check the governor first, and return
    BLOCKED without calling the harness when it says no (the tri-state itself is pinned against the
    real jobs in test_preopen_planner.py / test_nightly_review.py)."""

    def __init__(self, *, allowed: bool = True, harness_ok: bool = True) -> None:
        self.allowed = allowed
        self.harness_ok = harness_ok
        self.governor_calls = 0
        self.harness_calls = 0

    async def run(self, _d: date | None = None) -> AdvisoryOutcome:
        self.governor_calls += 1
        if not self.allowed:
            return AdvisoryOutcome.BLOCKED
        self.harness_calls += 1
        return AdvisoryOutcome.RAN if self.harness_ok else AdvisoryOutcome.FAILED


@pytest.mark.parametrize("wrapper_name", ("job_preopen_planner", "job_nightly_review"))
async def test_advisory_wrapper_translates_the_tristate(
    wrapper_name: str, tmp_path, monkeypatch, migrated_db_template
) -> None:
    """The wrapper - not ``_job_result_ok`` - is where the tri-state becomes a watermark verdict: the
    composed engine's registry entry hands back the job's outcome as an ``AdvisoryRun``, so a
    governor block is ok and only a harness failure sinks the watermark. A bare ``await job.run(...)``
    (discarding the outcome) fails here."""
    job_id, job_class, date_keyed = {
        "job_preopen_planner": (opsmain.JOB_PREOPEN_PLANNER, opsmain.PreopenPlannerJob, False),
        "job_nightly_review": (opsmain.JOB_NIGHTLY_REVIEW, opsmain.NightlyReviewJob, True),
    }[wrapper_name]
    outcome = {"next": AdvisoryOutcome.RAN}

    async def run(self, *_args) -> AdvisoryOutcome:
        return outcome["next"]

    monkeypatch.setattr(job_class, "run", run)
    async with composed_engine(tmp_path, monkeypatch, db_template=migrated_db_template) as c:
        spec = _spec(c.engine, job_id)
        args = (c.engine.clock.today(),) if date_keyed else ()
        for tristate, ok in ((AdvisoryOutcome.RAN, True), (AdvisoryOutcome.BLOCKED, True),
                             (AdvisoryOutcome.FAILED, False)):
            outcome["next"] = tristate
            result = await spec.run(*args)
            assert isinstance(result, AdvisoryRun) and result.outcome is tristate, wrapper_name
            assert opsmain._job_result_ok(result) is ok


@pytest.mark.asyncio
async def test_governor_block_green_stamps_the_watermark_and_is_never_retried(
    conn, clock, calendar
) -> None:
    planner = _FakeAdvisoryJob(allowed=False)

    async def job_preopen_planner() -> AdvisoryRun:
        return AdvisoryRun(await planner.run(clock.today()))    # mirrors engine.ops.main

    spec = JobSpec(JOB_PREOPEN_PLANNER, JobClass.RUN_LATEST, time(8, 50), job_preopen_planner, order=28)
    reg = JobRegistry()
    reg.register(spec)
    catch_up = CatchUpRunner(conn, clock, calendar, reg)

    await _scheduled_runner(spec, catch_up, clock)()

    assert planner.harness_calls == 0                          # blocked before the SDK boundary
    assert catch_up.was_run(JOB_PREOPEN_PLANNER, clock.today()) is True    # SUCCESS watermark
    await catch_up.catch_up(scope=CatchUpScope.ALL)            # the sweep must not re-run it
    assert planner.governor_calls == 1


@pytest.mark.asyncio
async def test_harness_failure_sinks_the_watermark_and_the_sweep_retry_is_governor_gated(
    conn, clock, calendar
) -> None:
    """The retry is bounded by the governor, not by a counter: the re-run re-enters ``can_invoke``,
    so a governor that has since closed the tap ends the retry chain with a success watermark and
    zero spend (WO-14: 'a retried run is still governor-gated at execution')."""
    nightly = _FakeAdvisoryJob(harness_ok=False)

    async def job_nightly_review(d) -> AdvisoryRun:
        return AdvisoryRun(await nightly.run(d))               # mirrors engine.ops.main

    # Fire-time brought forward of the real 21:00 slot so the frozen 10:05 clock is PAST it — the
    # sweep only replays days whose fire-time has come (§2.6 _missed_days).
    spec = JobSpec(JOB_NIGHTLY_REVIEW, JobClass.DATE_KEYED, time(9, 0), job_nightly_review, order=80)
    reg = JobRegistry()
    reg.register(spec)
    catch_up = CatchUpRunner(conn, clock, calendar, reg)
    today = clock.today()

    await _scheduled_runner(spec, catch_up, clock)()
    assert catch_up.was_run(JOB_NIGHTLY_REVIEW, today) is False            # FAILED watermark
    assert nightly.harness_calls == 1

    nightly.allowed = False                                    # the governor closes the tap
    result = await catch_up.catch_up(scope=CatchUpScope.ALL)   # the sweep retries the failed day

    assert nightly.governor_calls == 2 and nightly.harness_calls == 1      # retried, spent nothing
    assert catch_up.was_run(JOB_NIGHTLY_REVIEW, today) is True             # blocked => correct outcome
    assert result.jobs_failed == []


# =========================================================== WO-23: §4.5 retention actually WIRED
# ``MarketStore.apply_retention`` (ticks 30 d, news/clusters/sentiment 1 y, corrections 90 d) was
# designed, implemented and unit-tested — and never CALLED from any scheduled job. It now hangs off
# the nightly tick_compact pass, which already owns the closed tick partitions.


class _FakeRetentionStore:
    """The one MarketStore seam ``apply_tick_retention`` uses."""

    def __init__(self, *, raises: Exception | None = None) -> None:
        self.calls = 0
        self._raises = raises

    async def aapply_retention(self) -> dict[str, int]:
        self.calls += 1
        if self._raises is not None:
            raise self._raises
        return {"tick_partitions": 2, "corrections_log": 5, "news": 0,
                "news_clusters": 0, "sentiment_agg": 0}


@pytest.mark.asyncio
async def test_retention_runs_after_a_successful_compaction(caplog) -> None:
    store = _FakeRetentionStore()
    with caplog.at_level(logging.INFO, logger="engine.ops.main"):
        await opsmain.apply_tick_retention(store, TickCompactionResult(ok=True))

    assert store.calls == 1
    applied = [r for r in caplog.records if r.getMessage() == "tick_retention_applied"]
    assert len(applied) == 1
    assert applied[0].tick_partitions == 2            # the report is logged, not swallowed
    assert applied[0].corrections_log == 5


@pytest.mark.asyncio
async def test_retention_failure_warns_and_never_breaks_the_job(caplog) -> None:
    store = _FakeRetentionStore(raises=OSError("parquet dir busy"))
    with caplog.at_level(logging.WARNING, logger="engine.ops.main"):
        await opsmain.apply_tick_retention(store, TickCompactionResult(ok=True))   # must NOT raise

    failed = [r for r in caplog.records if r.getMessage() == "tick_retention_failed"]
    assert len(failed) == 1
    assert failed[0].error_type == "OSError"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "result",
    [
        TickCompactionResult(ok=False, failures=["2026-08-01/RELIANCE: boom"]),
        TickCompactionResult(skipped_in_flight=True),
        TickCompactionResult(stopped=True),
    ],
    ids=["degraded_pass", "another_run_holds_the_lock", "stopped_at_the_window_or_shutdown"],
)
async def test_retention_is_gated_on_a_clean_pass(result: TickCompactionResult) -> None:
    """A degraded pass leaves un-compacted symbol-days for the retry; a skipped-in-flight pass means
    ANOTHER compaction run is walking those partitions right now; a stopped pass was cut off at the
    session window or by a stop signal. None of them is a moment to rmtree."""
    store = _FakeRetentionStore()
    await opsmain.apply_tick_retention(store, result)
    assert store.calls == 0


async def test_tick_compact_closure_calls_retention_after_the_pass(
    tmp_path, monkeypatch, migrated_db_template
) -> None:
    """WO-23: the composed engine's ``tick_compact`` job runs the retention sweep AFTER the pass and
    hands back the pass's own verdict — the dropped retention call was the defect. Buffered ticks are
    written BEFORE compaction reads the partitions (a session tail left for the next morning's flush
    would land beside a date a post-midnight run already compacted), and the run stops at the WO-21
    window and on a stop signal (2026-09-29: a worker still compacting at shutdown held the process
    past NSSM's grace on every stop during a run)."""
    events: list[tuple] = []
    stops: list = []
    verdict = TickCompactionResult(ok=True)

    def compact(parquet_dir, *, upto, today, stop):
        events.append(("compact", upto, today))
        stops.append(stop)
        return verdict

    async def retention(store, result) -> None:
        events.append(("retention", result))

    monkeypatch.setattr(opsmain, "compact_ticks", compact)
    monkeypatch.setattr(opsmain, "apply_tick_retention", retention)
    async with composed_engine(tmp_path, monkeypatch, db_template=migrated_db_template) as c:
        e = c.engine
        monkeypatch.setattr(e.store, "flush_ticks", lambda *, wait_s: events.append(("flush", wait_s)))
        run_for = e.clock.today() - timedelta(days=1)
        assert await _spec(e, opsmain.JOB_TICK_COMPACT).run(run_for) is verdict
        assert events == [("flush", 15.0), ("compact", run_for, e.clock.today()), ("retention", verdict)]
        (stop,) = stops
        assert stop() is True                       # 10:05 on a trading day: inside the WO-21 window
        c.time.set(datetime.combine(e.clock.today(), time(22, 30), tzinfo=IST))
        assert stop() is False
        c.stop_event.set()
        assert stop() is True                       # …and a stop signal ends it at any hour


async def test_run_gives_tick_compact_its_own_lane(tmp_path, monkeypatch, migrated_db_template) -> None:
    """The 2026-09-29 wiring: the composed main runner never holds compaction and the lane runner holds
    nothing else. (Its start behind the post-arm one-shot is ``_boot``'s order test; its cancel on
    stop is ``test_shutdown_cancels_the_boot_watchdog_cleanly``'s.)"""
    async with composed_engine(tmp_path, monkeypatch, db_template=migrated_db_template) as c:
        main_ids = {s.job_id for s in c.engine.catch_up._registry.specs()}
        lane_ids = {s.job_id for s in c.engine.compaction_lane._runner._registry.specs()}
        every = {s.job_id for s in c.engine.registry.specs()}
    assert lane_ids == opsmain.LANE_JOB_IDS
    assert main_ids == every - opsmain.LANE_JOB_IDS


async def test_market_backup_runs_in_compaction_lane_only(tmp_path, monkeypatch, migrated_db_template) -> None:
    """§10.5 DuckDB export (2026-10-11): a lane job — never in the boot catch-up (the main runner never
    holds it), never on the stop path (the shutdown hook is the SQLite snapshot alone), and its 22:00
    fire starts a lane pass, so it never overlaps the compaction it shares the lane with."""
    assert opsmain.LANE_JOB_IDS == {opsmain.JOB_TICK_COMPACT, opsmain.JOB_MARKET_BACKUP}
    exports: list[object] = []
    snapshots: list[str] = []
    verdict = object()

    async def market_backup(store, settings, clock, calendar, *, retry):
        exports.append(retry)
        return verdict

    async def snapshot(conn, settings, clock) -> None:
        snapshots.append("sqlite")

    monkeypatch.setattr(opsmain, "_market_backup", market_backup)
    monkeypatch.setattr(opsmain, "_snapshot_backup", snapshot)
    async with composed_engine(tmp_path, monkeypatch, db_template=migrated_db_template) as c:
        e = c.engine
        assert opsmain.JOB_MARKET_BACKUP not in {s.job_id for s in e.catch_up._registry.specs()}
        spawned: list[str] = []
        monkeypatch.setattr(e.compaction_lane, "spawn",
                            lambda path, *, quiet=frozenset(): spawned.append(path))
        await _job(e, opsmain.JOB_MARKET_BACKUP)                 # the 22:00 fire
        assert spawned == [f"scheduled:{opsmain.JOB_MARKET_BACKUP}"] and exports == []
        # The job forwards the ok-bearing result, on ONE retry hold shared by every lane pass.
        spec = _spec(e, opsmain.JOB_MARKET_BACKUP)
        assert await spec.run() is verdict and await spec.run() is verdict
        assert len(exports) == 2 and exports[0] is exports[1]
        assert e.compaction_lane._interrupt == e.store.interrupt_export
        await e.lifecycle._backup()                             # the stop path's backup hook
        assert snapshots == ["sqlite"] and len(exports) == 2


class _SpawnLane:
    def __init__(self) -> None:
        self.paths: list[str] = []
        self.quiet: list[frozenset[str]] = []

    def spawn(self, path: str, *, quiet: frozenset[str] = frozenset()):
        self.paths.append(path)
        self.quiet.append(quiet)
        return object()


@pytest.mark.asyncio
async def test_lane_job_fires_start_a_lane_pass_and_record_nothing_themselves(conn, clock, calendar) -> None:
    settings = load_settings()
    ran: list[str] = []

    async def other() -> None:
        ran.append("universe")

    fns = _all_noop_fns() | {JOB_UNIVERSE: other}
    reg = build_job_registry(settings, fns)
    catch_up = CatchUpRunner(conn, clock, calendar, reg)
    sched = Scheduler(clock, calendar)
    lane = _SpawnLane()

    _arm_registry_jobs(sched, reg, catch_up, clock, lane=lane)

    for job_id in sorted(opsmain.LANE_JOB_IDS):
        await sched._sched.get_job(job_id).func()
    await sched._sched.get_job(JOB_UNIVERSE).func()
    assert lane.paths == [f"scheduled:{j}" for j in sorted(opsmain.LANE_JOB_IDS)]
    # Each fire is quiet about its own on-time entry only (the runner's "job_id:run_for" key).
    today = clock.today().isoformat()
    assert lane.quiet == [frozenset({f"{j}:{today}"}) for j in sorted(opsmain.LANE_JOB_IDS)]
    assert ran == ["universe"]                                   # everything else: _scheduled_runner
    recorded = {r[0] for r in conn.execute("SELECT job_id FROM job_runs").fetchall()}
    assert recorded == {JOB_UNIVERSE}                            # the lane pass owns the lane watermarks


@pytest.mark.asyncio
async def test_lane_cancel_interrupts_a_running_export(conn, calendar) -> None:
    calls: list[str] = []
    gate = asyncio.Event()
    clock = _clock_at(_EVENING)
    reg = JobRegistry()

    async def backup() -> None:
        await gate.wait()

    reg.register(JobSpec(opsmain.JOB_MARKET_BACKUP, JobClass.RUN_LATEST, time(19, 0), backup, order=95))
    lane = CompactionLane(CatchUpRunner(conn, clock, calendar, reg), clock, calendar,
                          interrupt=lambda: calls.append("interrupt"))

    await lane.cancel()                                          # idle: nothing to interrupt
    assert calls == []
    task = lane.spawn("post_arm")
    await asyncio.sleep(0)
    await lane.cancel()
    assert calls == ["interrupt"] and task.done()


async def _compact_ok(_d: date) -> None:
    return None


def _armed_lane(
    conn, calendar, now: dict, sent: list, backup, *, compact=_compact_ok, done: date = date(2026, 6, 16),
) -> tuple[Scheduler, CompactionLane]:
    """The real registry armed as run() arms it: lane jobs through a real CompactionLane whose runner
    has a recording notify, the lane work through ``done`` (default Tue) already recorded."""
    clock = Clock(time_source=lambda: now["t"])

    async def notify(msg) -> None:
        sent.append(msg)

    reg = build_job_registry(load_settings(), _all_noop_fns() | {
        opsmain.JOB_MARKET_BACKUP: backup, opsmain.JOB_TICK_COMPACT: compact})
    runner = CatchUpRunner(conn, clock, calendar, reg.select(lambda s: s.job_id in opsmain.LANE_JOB_IDS),
                           notify=notify)
    for job_id in opsmain.LANE_JOB_IDS:
        runner.record_run(job_id, done)
    lane = CompactionLane(runner, clock, calendar)
    main_runner = CatchUpRunner(conn, clock, calendar,
                                reg.select(lambda s: s.job_id not in opsmain.LANE_JOB_IDS))
    sched = Scheduler(clock, calendar)
    _arm_registry_jobs(sched, reg, main_runner, clock, lane=lane)
    return sched, lane


async def _fire_lane_job(sched: Scheduler, lane: CompactionLane, job_id: str) -> None:
    await sched._sched.get_job(job_id).func()
    assert lane._task is not None
    await asyncio.wait_for(lane._task, timeout=5)


class _ExportStore:
    """The two ``MarketStore`` seams ``_market_backup`` uses; ``fail`` makes the export raise."""

    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.attempts: list[str] = []

    async def arun(self, fn, *args):
        return fn(*args)

    def export_snapshot(self, dest):
        self.attempts.append(dest.name)
        if self.fail:
            raise RuntimeError("snapshot export incomplete: no data file for ['bars_1m']")
        dest.mkdir(parents=True)
        (dest / "bars_1m.parquet").write_bytes(b"x")
        return {"tables": 1, "bytes": 1}


def _real_backup(tmp_path: Path, now: dict, calendar, store: _ExportStore,
                 retry: opsmain.MarketBackupRetry | None):
    """The lane's ``market_backup`` body as run() builds it: the real ``_market_backup`` (trading-day
    start gate, retry hold) over a fake export store."""
    clock = Clock(time_source=lambda: now["t"])
    settings = SimpleNamespace(backups_dir=lambda: tmp_path / "backups",
                               duckdb_path=lambda: tmp_path / "market.duckdb")

    async def backup():
        return await opsmain._market_backup(store, settings, clock, calendar, retry=retry)

    return backup


def _catchup_reports(sent: list) -> list:
    return [m for m in sent if m.kind == MessageKind.CATCHUP_REPORT]


@pytest.mark.asyncio
async def test_on_time_lane_fires_send_no_catchup_report(conn, calendar) -> None:
    """The 22:00 export and the 22:30 compaction ran on schedule: neither was missed, so neither is a
    "Missed-job catch-up complete" message (two false reports every trading night otherwise)."""
    now = {"t": datetime(2026, 6, 17, 22, 0, 1, tzinfo=IST)}       # Wed, a trading day
    sent: list = []
    exports: list[datetime] = []

    async def backup() -> None:
        exports.append(now["t"])

    sched, lane = _armed_lane(conn, calendar, now, sent, backup)
    await _fire_lane_job(sched, lane, opsmain.JOB_MARKET_BACKUP)
    now["t"] = datetime(2026, 6, 17, 22, 30, 1, tzinfo=IST)
    await _fire_lane_job(sched, lane, opsmain.JOB_TICK_COMPACT)

    assert len(exports) == 1
    assert lane._runner.was_run(opsmain.JOB_MARKET_BACKUP, date(2026, 6, 17))
    assert lane._runner.was_run(opsmain.JOB_TICK_COMPACT, date(2026, 6, 17))
    assert [m for m in sent if m.kind == MessageKind.CATCHUP_REPORT] == []


@pytest.mark.asyncio
async def test_a_failing_scheduled_export_still_reports_once(conn, calendar) -> None:
    now = {"t": datetime(2026, 6, 17, 22, 0, 1, tzinfo=IST)}
    sent: list = []

    async def backup() -> None:
        raise RuntimeError("snapshot export incomplete")

    sched, lane = _armed_lane(conn, calendar, now, sent, backup)
    await _fire_lane_job(sched, lane, opsmain.JOB_MARKET_BACKUP)
    now["t"] = datetime(2026, 6, 17, 22, 30, 1, tzinfo=IST)
    await _fire_lane_job(sched, lane, opsmain.JOB_TICK_COMPACT)  # retries the export: same failure set

    reports = [m for m in sent if m.kind == MessageKind.CATCHUP_REPORT]
    assert len(reports) == 1
    assert reports[0].data["jobs_failed"] == [f"{opsmain.JOB_MARKET_BACKUP}:2026-06-17"]
    assert lane._runner.was_run(opsmain.JOB_TICK_COMPACT, date(2026, 6, 17))


@pytest.mark.asyncio
async def test_a_sweep_lane_pass_still_reports_what_it_caught_up(conn, calendar, tmp_path) -> None:
    """Only the scheduled fire is quiet: a sweep's lane pass that replays a missed day is news. The
    missed Wed export waits for the first pass after 15:45 (the real start gate), then reports."""
    now = {"t": datetime(2026, 6, 18, 7, 0, tzinfo=IST)}            # Thu morning, 22:00 Wed missed
    sent: list = []
    store = _ExportStore()
    backup = _real_backup(tmp_path, now, calendar, store, opsmain.MarketBackupRetry())

    _sched, lane = _armed_lane(conn, calendar, now, sent, backup)
    await asyncio.wait_for(lane.spawn("sweep"), timeout=5)

    assert store.attempts == []                                     # deferred: no export, no watermark
    assert not lane._runner.was_run(opsmain.JOB_MARKET_BACKUP, date(2026, 6, 17))
    assert [m.data["jobs_caught_up"] for m in _catchup_reports(sent)] == [
        [f"{opsmain.JOB_TICK_COMPACT}:2026-06-17"]]

    now["t"] = datetime(2026, 6, 18, 16, 0, tzinfo=IST)             # the first sweep after 15:45
    await asyncio.wait_for(lane.spawn("sweep"), timeout=5)

    assert len(store.attempts) == 1
    reports = _catchup_reports(sent)
    assert len(reports) == 2
    assert reports[1].data["jobs_caught_up"] == [f"{opsmain.JOB_MARKET_BACKUP}:2026-06-17"]


@pytest.mark.asyncio
async def test_a_scheduled_fire_reports_a_missed_earlier_compaction_day(conn, calendar) -> None:
    """Quiet is per entry: the 22:00 fire's own export is not news, but the Wed compaction the same
    pass replays (the host slept through Wed's lane work) is."""
    now = {"t": datetime(2026, 6, 18, 22, 0, 1, tzinfo=IST)}        # Thu 22:00; Wed's lane work missed
    sent: list = []
    compacted: list[date] = []

    async def backup() -> None:
        return None

    async def compact(d: date) -> None:
        compacted.append(d)

    sched, lane = _armed_lane(conn, calendar, now, sent, backup, compact=compact)
    await _fire_lane_job(sched, lane, opsmain.JOB_MARKET_BACKUP)

    assert compacted == [date(2026, 6, 17)]
    reports = _catchup_reports(sent)
    assert len(reports) == 1
    assert reports[0].data["jobs_caught_up"] == [
        f"{opsmain.JOB_MARKET_BACKUP}:2026-06-18", f"{opsmain.JOB_TICK_COMPACT}:2026-06-17"]


@pytest.mark.asyncio
async def test_the_first_good_export_after_a_reported_failure_reports_the_recovery(
    conn, calendar, tmp_path,
) -> None:
    """Wed 22:00 export fails (reported); the 22:30 fire is inside the retry hold (no repeat report);
    Thu's on-time 22:00 export succeeds — the owner, told it failed, is told it recovered."""
    now = {"t": datetime(2026, 6, 17, 22, 0, 1, tzinfo=IST)}
    sent: list = []
    store = _ExportStore(fail=True)
    backup = _real_backup(tmp_path, now, calendar, store, opsmain.MarketBackupRetry())
    sched, lane = _armed_lane(conn, calendar, now, sent, backup)

    await _fire_lane_job(sched, lane, opsmain.JOB_MARKET_BACKUP)
    now["t"] = datetime(2026, 6, 17, 22, 30, 1, tzinfo=IST)
    await _fire_lane_job(sched, lane, opsmain.JOB_TICK_COMPACT)
    assert len(store.attempts) == 1
    assert [m.data["jobs_failed"] for m in _catchup_reports(sent)] == [
        [f"{opsmain.JOB_MARKET_BACKUP}:2026-06-17"]]

    store.fail = False
    now["t"] = datetime(2026, 6, 18, 22, 0, 1, tzinfo=IST)
    await _fire_lane_job(sched, lane, opsmain.JOB_MARKET_BACKUP)

    assert len(store.attempts) == 2
    reports = _catchup_reports(sent)
    assert len(reports) == 2
    assert reports[1].data["jobs_caught_up"] == [f"{opsmain.JOB_MARKET_BACKUP}:2026-06-18"]
    assert reports[1].data["jobs_failed"] == []


async def _weekend_of_failing_lane_jobs(tmp_path: Path, calendar, *, hold: bool) -> list[list[str]]:
    """The export raising and a stuck ``tick_compact`` day together, Fri 22:00 through Sun 22:00 with
    a lane pass every 30 min; returns the failure list of every CATCHUP_REPORT sent."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    conn = connect(tmp_path / "state.db")
    apply_migrations(conn)
    now = {"t": datetime(2026, 6, 19, 22, 0, 1, tzinfo=IST)}        # Fri 22:00 fire
    sent: list = []
    backup = _real_backup(tmp_path, now, calendar, _ExportStore(fail=True),
                          opsmain.MarketBackupRetry() if hold else None)

    async def compact(_d: date) -> TickCompactionResult:
        return TickCompactionResult(ok=False, failures=["2026-06-19/RELIANCE: fragment merge refused"])

    try:
        sched, lane = _armed_lane(conn, calendar, now, sent, backup, compact=compact,
                                  done=date(2026, 6, 18))
        await _fire_lane_job(sched, lane, opsmain.JOB_MARKET_BACKUP)
        now["t"] = datetime(2026, 6, 19, 22, 30, 1, tzinfo=IST)
        await _fire_lane_job(sched, lane, opsmain.JOB_TICK_COMPACT)
        t = datetime(2026, 6, 19, 22, 45, tzinfo=IST)
        while t < datetime(2026, 6, 21, 22, 0, tzinfo=IST):
            now["t"] = t
            await asyncio.wait_for(lane.spawn("sweep"), timeout=5)
            t += timedelta(minutes=30)
    finally:
        conn.close()
    return [m.data["jobs_failed"] for m in _catchup_reports(sent)]


@pytest.mark.asyncio
async def test_the_export_retry_hold_does_not_flap_the_failure_report(calendar, tmp_path) -> None:
    """A held export drops out of the pass's failures, but its failed key stays in the comparison
    set, so the hold sends no more reports than retrying every pass would (2 for the weekend: the
    export failure, then export + compaction)."""
    held = await _weekend_of_failing_lane_jobs(tmp_path / "held", calendar, hold=True)
    unheld = await _weekend_of_failing_lane_jobs(tmp_path / "unheld", calendar, hold=False)

    mb, tc = f"{opsmain.JOB_MARKET_BACKUP}:2026-06-19", f"{opsmain.JOB_TICK_COMPACT}:2026-06-19"
    assert unheld == [[mb], [mb, tc]]
    assert held == [[mb], [tc]]                       # the export's failure was already reported
    assert len(held) <= len(unheld)


# =========================================================================== WO-25c boot contract
# 2026-08-24, the 12:44:58 mid-session restart: `startup_complete` logged at 12:46:56 and run() then
# logged NOTHING for 11 h. It had parked in the pre-arm warm-up seeding — two bare awaits that WO-15
# left standing in front of `scheduler.start()` — so APScheduler was never started: no forward drain,
# no health pulse, no EOD job, while ticks/bars/features kept flowing and made the engine look alive.
# Layer 1: the seeding can no longer gate arming. Layer 2: a plain asyncio task (never an APScheduler
# job — it has to work when the scheduler is the broken thing) checks the boot contract and pages.


def _boot_state(*, ready: bool, armed: bool, pages: list | None = None) -> dict:
    """The late-bound holder run() hands the watchdog: engine_ready flag, scheduler, alert sink."""
    scheduler = _FakeScheduler([])
    if armed:
        scheduler.start()
    sink = pages if pages is not None else []

    async def alert(severity: str, message: str) -> None:
        sink.append((severity, message))

    return {"engine_ready": ready, "scheduler": scheduler, "alert": alert}


# --------------------------------------------------------------------------- layer 1: bounded seeding
@pytest.mark.asyncio
async def test_boot_seeding_completes_and_reports_success() -> None:
    """The healthy path is unchanged: both refreshes run, in order, and the boot is told so."""
    order: list[str] = []

    async def warmup_refresh() -> None:
        order.append("warmup")

    async def health_check() -> None:
        order.append("health")

    assert await seed_boot_snapshots(warmup_refresh, health_check, timeout_s=5) is True
    assert order == ["warmup", "health"]


@pytest.mark.asyncio
async def test_wedged_boot_seeding_times_out_loudly(caplog) -> None:
    """THE 2026-08-24 SHAPE: warm-up seeding never returns (the DuckDB single-writer lock was being
    taken a few hundred times per tick flush and ~400 sequential coverage probes never got through).
    It must now hit a ceiling, say so at CRITICAL, page the owner, and hand control back."""
    wedged = asyncio.Event()                    # never set
    healths: list[int] = []
    pages: list[tuple[str, str]] = []

    async def warmup_refresh() -> None:
        await wedged.wait()

    async def health_check() -> None:
        healths.append(1)

    async def alert(severity: str, message: str) -> None:
        pages.append((severity, message))

    with caplog.at_level(logging.CRITICAL, logger="engine.ops.main"):
        # The outer wait_for is the assertion: if the deadline were awaited-on-cancel (plain
        # asyncio.wait_for inside), a coroutine parked on a thread offload would wedge it too.
        ok = await asyncio.wait_for(
            seed_boot_snapshots(warmup_refresh, health_check, timeout_s=0.05, alert=alert),
            timeout=5,
        )

    assert ok is False
    assert healths == []                        # the health seed never ran — the 60 s job re-does it
    timeouts = [r for r in caplog.records if r.getMessage() == "boot_seed_timeout"]
    assert len(timeouts) == 1
    assert timeouts[0].levelno == logging.CRITICAL
    assert pages == [pages[0]] and pages[0][0] == "critical"


@pytest.mark.asyncio
async def test_a_raising_boot_seed_never_reaches_the_boot_path(caplog) -> None:
    """A seed that RAISES is not the incident, but it must not propagate into run() either."""
    async def warmup_refresh() -> None:
        raise RuntimeError("duckdb connection invalidated")

    async def health_check() -> None:
        raise AssertionError("must not be reached")

    with caplog.at_level(logging.ERROR, logger="engine.ops.main"):
        assert await seed_boot_snapshots(warmup_refresh, health_check, timeout_s=5) is False
    assert [r for r in caplog.records if r.getMessage() == "boot_seed_failed"]


@pytest.mark.asyncio
async def test_wedged_seeding_boot_still_arms_the_scheduler(conn, clock, calendar) -> None:
    """WO-25c acceptance. Before the fix the wedged seed sat bare in front of
    ``start_scheduler_and_fire_post_arm`` and the scheduler was never armed for the rest of the day.
    The boot tail must now reach arming — and engine_ready — regardless."""
    events: list[str] = []
    wedged = asyncio.Event()                    # never set: the 12:44:58 warm-up refresh
    catch_up = CatchUpRunner(conn, clock, calendar, _post_arm_registry(events),
                             deferred=POST_ARM_JOB_IDS)
    scheduler = _FakeScheduler(events)

    async def warmup_refresh() -> None:
        events.append("seed_started")
        await wedged.wait()

    async def health_check() -> None:
        events.append("health_seeded")

    async def boot_tail() -> asyncio.Task | None:
        await seed_boot_snapshots(warmup_refresh, health_check, timeout_s=0.05)
        task = start_scheduler_and_fire_post_arm(scheduler, catch_up, armed=asyncio.Event())
        events.append("engine_ready")
        return task

    task = await asyncio.wait_for(boot_tail(), timeout=5)

    assert events == ["seed_started", "scheduler_armed", "engine_ready"]
    assert scheduler.is_running() is True       # the whole point: triggers can fire again
    await cancel_post_arm(task)


# --------------------------------------------------------------------------- layer 2: the watchdog
def test_boot_contract_names_exactly_what_is_missing() -> None:
    """Both halves are reported, and a scheduler object that exists but was never started counts as
    missing — that is precisely the 12:44:58 state (Scheduler built, ``start()`` never called)."""
    from engine.ops.main import _boot_contract_missing

    assert _boot_contract_missing({"engine_ready": True, "scheduler": None}) == ["scheduler_running"]
    assert _boot_contract_missing(
        {"engine_ready": False, "scheduler": None}
    ) == ["engine_ready", "scheduler_running"]
    unarmed = _boot_state(ready=False, armed=False)
    assert _boot_contract_missing(unarmed) == ["engine_ready", "scheduler_running"]
    assert _boot_contract_missing(_boot_state(ready=True, armed=True)) == []


@pytest.mark.asyncio
async def test_boot_contract_ok_fires_once_when_the_boot_completed(caplog) -> None:
    state = _boot_state(ready=True, armed=True)
    with caplog.at_level(logging.INFO, logger="engine.ops.main"):
        await asyncio.wait_for(
            boot_contract_watchdog(state, deadline_s=0.01, recheck_s=0.01), timeout=5
        )
    oks = [r for r in caplog.records if r.getMessage() == "boot_contract_ok"]
    assert len(oks) == 1                        # once, then the task retires
    assert oks[0].levelno == logging.INFO
    assert oks[0].elapsed_s >= 0
    assert not [r for r in caplog.records if r.getMessage() == "boot_incomplete"]


@pytest.mark.asyncio
async def test_unarmed_scheduler_pages_once_repeats_then_resolves_when_it_arms_late(caplog) -> None:
    """The alarm that did not exist on 2026-08-24: CRITICAL + ONE owner page, re-logged on the
    re-check cadence while it stands, and closed out with a single boot_contract_ok if it heals."""
    pages: list[tuple[str, str]] = []
    state = _boot_state(ready=True, armed=False, pages=pages)

    def _records(event: str) -> list:
        return [r for r in caplog.records if r.getMessage() == event]

    with caplog.at_level(logging.INFO, logger="engine.ops.main"):
        task = asyncio.create_task(
            boot_contract_watchdog(state, deadline_s=0.01, recheck_s=0.01)
        )
        try:
            while len(_records("boot_incomplete")) < 3:      # re-logs on the cadence
                await asyncio.sleep(0.005)
            incomplete = _records("boot_incomplete")
            assert all(r.levelno == logging.CRITICAL for r in incomplete)
            assert incomplete[0].missing == ["scheduler_running"]
            assert len(pages) == 1                           # ONE page however long it stands
            assert pages[0][0] == "critical"
            assert "scheduler_running" in pages[0][1]
            state["scheduler"].start()                       # a late arm resolves the contract
            await asyncio.wait_for(task, timeout=5)
        finally:
            await cancel_post_arm(task)
    assert len(_records("boot_contract_ok")) == 1
    assert len(pages) == 1                                   # resolving never pages again


@pytest.mark.asyncio
async def test_a_raising_owner_page_never_ends_the_watchdog(caplog) -> None:
    """The notify path is best-effort: a dead Telegram must not silence the CRITICAL cadence."""
    async def bad_alert(severity: str, message: str) -> None:
        raise RuntimeError("telegram outage")

    state = {"engine_ready": False, "scheduler": None, "alert": bad_alert}
    with caplog.at_level(logging.INFO, logger="engine.ops.main"):
        task = asyncio.create_task(
            boot_contract_watchdog(state, deadline_s=0.01, recheck_s=0.01)
        )
        try:
            while len([r for r in caplog.records
                       if r.getMessage() == "boot_incomplete"]) < 3:
                await asyncio.sleep(0.005)
        finally:
            await cancel_post_arm(task)
    assert [r for r in caplog.records if r.getMessage() == "boot_incomplete_alert_failed"]


@pytest.mark.asyncio
async def test_shutdown_cancels_the_boot_watchdog_cleanly() -> None:
    """The teardown retires the watchdog BEFORE scheduler.shutdown() — otherwise a slow stop would page
    a violation for a deliberate stop. Two more orders its comments rely on: paper stops before the
    lifecycle guard's shutdown backup, and the bus drains before the market store closes."""
    events: list[str] = []
    state = _boot_state(ready=False, armed=False)
    watchdog = asyncio.create_task(boot_contract_watchdog(state, deadline_s=60, recheck_s=60))
    post_arm = asyncio.create_task(asyncio.sleep(3600))
    hydration = asyncio.create_task(asyncio.sleep(3600))
    await asyncio.sleep(0.01)

    def sync(name: str):
        return lambda *_a, **_k: events.append(name)

    def coro(name: str, result: object = None):
        async def call(*_a, **_k):
            events.append(name)
            return result
        return call

    def scheduler_shutdown() -> None:
        events.append("scheduler.shutdown" if watchdog.cancelled() else "scheduler.shutdown_under_watchdog")

    engine = SimpleNamespace(
        scheduler=SimpleNamespace(shutdown=scheduler_shutdown),
        compaction_lane=SimpleNamespace(cancel=coro("compaction_lane.cancel")),
        freeze_lift_tasks=set(),
        bar_builder=SimpleNamespace(flush_all=sync("bar_builder.flush_all")),
        ticker=SimpleNamespace(stop=coro("ticker.stop")),
        paper=SimpleNamespace(stop=coro("paper.stop")),
        lifecycle=SimpleNamespace(shutdown=coro("lifecycle.shutdown")),
        telegram=SimpleNamespace(stop=coro("telegram.stop")),
        http=SimpleNamespace(aclose=coro("http.aclose")),
        bus=SimpleNamespace(drain=coro("bus.drain", 0)),
        store=SimpleNamespace(close=sync("store.close")),
        conn=SimpleNamespace(close=sync("conn.close")),
    )
    await opsmain._shutdown(engine, opsmain._BootTasks(None, post_arm, hydration), boot_watchdog=watchdog)

    assert watchdog.cancelled() and post_arm.cancelled() and hydration.cancelled()
    assert events[0] == "scheduler.shutdown"     # the watchdog was already retired when it ran
    assert events.index("paper.stop") < events.index("lifecycle.shutdown")
    assert events.index("bus.drain") < events.index("store.close")
    assert sorted(events) == sorted([
        "scheduler.shutdown", "compaction_lane.cancel", "bar_builder.flush_all", "ticker.stop",
        "paper.stop", "lifecycle.shutdown", "telegram.stop", "http.aclose", "bus.drain",
        "store.close", "conn.close",
    ])


class _RecordingState(dict):
    """The boot-contract holder, recording every flag the boot raises into the shared event list."""

    def __init__(self, events: list[str]) -> None:
        super().__init__(engine_ready=False, scheduler=None, alert=None)
        self._events = events

    def __setitem__(self, key, value) -> None:
        self._events.append(f"boot_state[{key}]={value}")
        super().__setitem__(key, value)


async def test_boot_runs_its_steps_in_the_order_its_comments_state(monkeypatch) -> None:
    """``_boot``'s own comments, as orders: the owner channel comes up before startup recovery (its
    notifications must not be dropped); the login API binds before the token probe and before
    ``lifecycle.startup`` (2026-07-21 lockout); the LIVE probe precedes the instruments hydrate,
    which precedes startup (F2); the boot-phase ticks run alongside startup and are gone before the
    seeding; the seeding follows startup; the scheduler is armed before the compaction lane is
    spawned (the post-arm one-shot fires inside the arming, WO-15); and ``engine_ready`` is raised
    last (WO-25c: the other half of the boot contract)."""
    events: list[str] = []

    def sync(name: str, result: object = None):
        return lambda *_a, **_k: events.append(name) or result

    def coro(name: str, result: object = None):
        async def call(*_a, **_k):
            events.append(name)
            return result
        return call

    async def boot_ticks(*_fns) -> None:
        events.append("boot_phase_ticks.start")
        try:
            await asyncio.sleep(3600)
        except asyncio.CancelledError:
            events.append("boot_phase_ticks.cancelled")
            raise

    report = SimpleNamespace(sticky_mode="RECOMMEND", killed=False, needs_login=False, integrity_ok=True,
                             jobs_caught_up=[], frozen_reasons=[])

    async def startup(*, check_skew: bool):
        await asyncio.sleep(0)                  # the recovery awaits; the ticks run alongside it
        events.append("lifecycle.startup")
        return report
    monkeypatch.setattr(opsmain, "check_calendar_horizon", coro("check_calendar_horizon"))
    monkeypatch.setattr(opsmain, "_serve_api",
                        coro("_serve_api", SimpleNamespace(_mt_server=SimpleNamespace(started=True))))
    monkeypatch.setattr(opsmain, "hydrate_instruments_at_startup", coro("hydrate_instruments", "snapshot"))
    monkeypatch.setattr(opsmain, "boot_phase_ticks", boot_ticks)
    monkeypatch.setattr(opsmain, "seed_boot_snapshots", coro("seed_boot_snapshots"))
    monkeypatch.setattr(opsmain, "start_scheduler_and_fire_post_arm", sync("arm_scheduler_fire_post_arm"))
    engine = SimpleNamespace(
        telegram=SimpleNamespace(start=coro("telegram.start")),
        calendar=object(), clock=object(), app=object(), instruments=object(), store=object(), kite=None,
        settings=SimpleNamespace(clock=SimpleNamespace(calendar_horizon_alert_sessions=5),
                                 api=SimpleNamespace(host="127.0.0.1", port=8400)),
        notify=coro("notify"), alert=coro("alert"),
        session=SimpleNamespace(verify_token=coro("verify_token", "valid"), token_valid=lambda: True,
                                login_url=lambda: "https://kite.example/login"),
        warmup_gate=object(), warmup_holder={}, skew_holder={}, warmup_refresh=coro("warmup_refresh"),
        health=SimpleNamespace(check=coro("health.check")),
        lifecycle=SimpleNamespace(startup=startup),
        scheduler=object(), catch_up=object(), scheduler_armed=asyncio.Event(),
        compaction_lane=SimpleNamespace(spawn=sync("compaction_lane.spawn")),
        early_hydration=SimpleNamespace(on_boot=coro("early_hydration.on_boot")),
        mode=SimpleNamespace(mode=lambda: Mode.RECOMMEND),
    )
    boot_state = _RecordingState(events)
    tasks = await opsmain._boot(engine, boot_state)
    await tasks.early_hydration_task

    def before(a: str, b: str) -> bool:
        return events.index(a) < events.index(b)

    assert before("telegram.start", "lifecycle.startup")
    assert before("_serve_api", "verify_token") and before("_serve_api", "lifecycle.startup")
    assert before("verify_token", "hydrate_instruments") and before("hydrate_instruments", "lifecycle.startup")
    assert before("boot_phase_ticks.start", "lifecycle.startup")
    assert before("lifecycle.startup", "boot_phase_ticks.cancelled")
    assert before("boot_phase_ticks.cancelled", "seed_boot_snapshots")
    assert before("lifecycle.startup", "seed_boot_snapshots")
    assert before("arm_scheduler_fire_post_arm", "compaction_lane.spawn")
    assert [e for e in events if e.startswith("boot_state")] == ["boot_state[engine_ready]=True"]
    assert events[-2:] == ["boot_state[engine_ready]=True", "early_hydration.on_boot"]  # the task runs after
    assert boot_state["engine_ready"] is True and engine.skew_holder == {"ok": True}


@pytest.mark.asyncio
async def test_scheduler_is_running_reads_apscheduler_state(clock, calendar) -> None:
    """The watchdog must not be able to be lied to: is_running() reads APScheduler's own state, so a
    Scheduler that was built but never started answers False — the 12:44:58 condition."""
    s = Scheduler(clock, calendar)
    assert s.is_running() is False              # built, never armed
    s.start()
    assert s.is_running() is True
    s.shutdown()
    # APScheduler 3.11 defers AsyncIOScheduler.shutdown to the loop (@run_in_event_loop), so the flip
    # lands on the NEXT turn, not synchronously. Pinned so the caveat in is_running() stays honest.
    assert s.is_running() is True
    await asyncio.sleep(0.05)
    assert s.is_running() is False


def test_watchlist_eligible_pin_filters_real_row_tuples() -> None:
    """2026-09-02 incident regression: the eligible-pin filter must work on the REAL cat/cat_reversal
    WatchlistRow NamedTuples (attribute access) - the inline .get() version killed the 10:51
    window_open sweep (batch admission + hi52 first sweep aborted for the morning)."""
    from decimal import Decimal

    from engine.ops.main import _watchlist_rows_for_symbols
    from engine.strategy.scanners import cat, cat_reversal

    cat_row = cat.WatchlistRow(
        entry_id="e1", symbol="RELIANCE", grade="originating", direction="long",
        event_age_sessions=0, materiality=0.8, reference_close=Decimal("1300"),
    )
    ext_row = cat_row._replace(entry_id="e2", symbol="JINDALSAW")
    assert _watchlist_rows_for_symbols([cat_row, ext_row], {"RELIANCE"}) == [cat_row]

    rev_fields = {f: None for f in cat_reversal.WatchlistRow._fields}
    rev_fields.update(entry_id="r1", symbol="HINDZINC", grade="originating")
    rev_row = cat_reversal.WatchlistRow(**rev_fields)
    assert _watchlist_rows_for_symbols([rev_row], {"HINDZINC"}) == [rev_row]
    assert _watchlist_rows_for_symbols([rev_row], {"RELIANCE"}) == []


async def test_reconcile_catchup_freeze_branching() -> None:
    """2026-09-02 review: the sweep-path catchup_safety_jobs symmetry, extracted testable. Failures
    latch; a clean pass clears ONLY an active latch (no no-op clear churn); a skipped single-flight
    pass and a killed engine leave the latch alone."""
    from engine.core.enums import RiskState
    from engine.ops.jobs import CatchUpResult
    from engine.ops.main import _reconcile_catchup_freeze

    class FakeLatch:
        def __init__(self, active=()):
            self.active = list(active)
            self.calls: list[tuple] = []

        def active_causes(self):
            return [(c, RiskState.FROZEN, "d") for c in self.active]

        async def set_cause(self, cause, state, detail, who):
            self.calls.append(("set", cause, detail))

        async def clear_cause(self, cause, who):
            self.calls.append(("clear", cause))

    class FakeKill:
        def __init__(self, killed=False):
            self._killed = killed

        def is_killed(self):
            return self._killed

    # Safety-critical failure -> latch.
    latch = FakeLatch()
    await _reconcile_catchup_freeze(
        CatchUpResult(frozen_reasons=["data_freshness:instruments"]), latch, FakeKill()
    )
    assert latch.calls == [("set", "catchup_safety_jobs", "data_freshness:instruments")]

    # Clean pass with the cause ACTIVE -> clear.
    latch = FakeLatch(active=["catchup_safety_jobs"])
    await _reconcile_catchup_freeze(CatchUpResult(), latch, FakeKill())
    assert latch.calls == [("clear", "catchup_safety_jobs")]

    # Clean pass, cause NOT active -> no calls at all (no 48-a-day no-op WARNING churn).
    latch = FakeLatch()
    await _reconcile_catchup_freeze(CatchUpResult(), latch, FakeKill())
    assert latch.calls == []

    # Skipped single-flight pass verified nothing -> untouched, even with the cause active.
    latch = FakeLatch(active=["catchup_safety_jobs"])
    await _reconcile_catchup_freeze(CatchUpResult(skipped_in_flight=True), latch, FakeKill())
    assert latch.calls == []

    # Killed engine -> untouched in both directions.
    latch = FakeLatch(active=["catchup_safety_jobs"])
    await _reconcile_catchup_freeze(
        CatchUpResult(frozen_reasons=["x"]), latch, FakeKill(killed=True)
    )
    assert latch.calls == []


async def test_catchup_sweep_runs_the_main_pass_then_the_compaction_lane() -> None:
    """The sweep's main pass is the whole main registry (compaction is not in it, so nothing is
    vetoed there), the freeze still reconciles, and the compaction lane is offered its pass AFTER
    — the lane itself applies the WO-21 window (2026-09-04 11:39: an in-session sweep replay of
    ``tick_compact`` cost the afternoon in store stalls and late ticks)."""
    from engine.core.enums import RiskState
    from engine.ops.jobs import CatchUpResult
    from engine.ops.main import _catchup_sweep_once

    order: list[str] = []

    class FakeCatchUp:
        def __init__(self):
            self.calls: list[dict] = []

        async def catch_up(self, **kw):
            self.calls.append(kw)
            order.append("main")
            return CatchUpResult()

    class FakeLane:
        def __init__(self):
            self.paths: list[str] = []

        def spawn(self, path):
            self.paths.append(path)
            order.append("lane")

    class FakeLatch:
        def __init__(self):
            self.calls: list[tuple] = []

        def active_causes(self):
            return [("catchup_safety_jobs", RiskState.FROZEN, "d")]

        async def set_cause(self, cause, state, detail, who):
            self.calls.append(("set", cause))

        async def clear_cause(self, cause, who):
            self.calls.append(("clear", cause))

    class FakeKill:
        def is_killed(self):
            return False

    cu, latch, lane = FakeCatchUp(), FakeLatch(), FakeLane()
    await _catchup_sweep_once(cu, latch, FakeKill(), lane)
    assert cu.calls == [{"scope": CatchUpScope.ALL}]
    assert latch.calls == [("clear", "catchup_safety_jobs")]
    assert lane.paths == ["sweep"] and order == ["main", "lane"]


# ------------------------------------------------- freeze-lift re-sweep + batch ticks (2026-09-11)
#: Inside the session AND inside the window below; 2026-06-17 is a real trading day (conftest).
_IN_WINDOW = datetime(2026, 6, 17, 10, 5, tzinfo=IST)
_WINDOW = TradeWindow(start=time(9, 20), end=time(15, 0))


class _FakeMode:
    """The two ``ModeManager`` seams the sweep predicate reads (§3.2.7 window + O2 mode)."""

    def __init__(self, *, window: TradeWindow | None = _WINDOW, mode_: Mode = Mode.RECOMMEND) -> None:
        self._window, self._mode = window, mode_

    def get_trade_window(self) -> TradeWindow | None:
        return self._window

    def mode(self) -> Mode:
        return self._mode


class _FakeRiskMode(_FakeMode):
    """`_FakeMode` plus the risk-state seam the RETEST predicate reads (2026-09-12 review)."""

    def __init__(self, *, risk: RiskState = RiskState.NORMAL, **kw) -> None:
        super().__init__(**kw)
        self._risk = risk

    def risk_state(self) -> RiskState:
        return self._risk


class _FakeKillSwitch:
    def __init__(self, killed: bool = False) -> None:
        self._killed = killed

    def is_killed(self) -> bool:
        return self._killed


def test_the_retest_never_offers_into_a_state_the_pipeline_drops(calendar) -> None:
    """2026-09-12 review of WO-R: the retest's once-a-day offer is spent at OFFER time and it has
    no freeze-lift path to redo a dropped publication, so it must not offer while the pipeline is
    certain to drop (FROZEN, killed) nor while a sweep is running (that sweep may have re-armed
    a fresh crossing of the same symbol whose dedupe slot the stale level would otherwise take)."""
    lock = asyncio.Lock()
    assert _retest_active(_IN_WINDOW, calendar, _FakeRiskMode(), _FakeKillSwitch(), lock) is True
    assert _retest_active(_IN_WINDOW, calendar, _FakeRiskMode(risk=RiskState.FROZEN),
                          _FakeKillSwitch(), lock) is False
    assert _retest_active(_IN_WINDOW, calendar, _FakeRiskMode(), _FakeKillSwitch(killed=True),
                          lock) is False
    assert _retest_active(_IN_WINDOW, calendar, _FakeRiskMode(mode_=Mode.OFF),
                          _FakeKillSwitch(), lock) is False
    outside = _IN_WINDOW.replace(hour=16, minute=0)
    assert _retest_active(outside, calendar, _FakeRiskMode(), _FakeKillSwitch(), lock) is False

    async def while_locked() -> bool:
        async with lock:
            return _retest_active(_IN_WINDOW, calendar, _FakeRiskMode(), _FakeKillSwitch(), lock)
    assert asyncio.run(while_locked()) is False


def _lift(old: RiskState = RiskState.FROZEN, new: RiskState = RiskState.NORMAL) -> RiskStateChanged:
    return RiskStateChanged(old_state=old, new_state=new, actor=Actor.RISK_GATE,
                            reason="warm-up coverage met", at=_IN_WINDOW)


def _sweep_state(*, started_at: datetime | None = None, published_at: datetime | None = None,
                 done_at: datetime | None = None, frozen: bool = False) -> dict:
    """A ``_last_sweep`` record with the three phase marks ``run_scan_sweep`` stamps: start (past the
    session check), publication (where the risk state is read) and completion."""
    return {"started_at": started_at, "published_at": published_at, "done_at": done_at,
            "published": 0, "pending": 0, "frozen": frozen}


def _finished(at: datetime, *, frozen: bool = False) -> dict:
    """A sweep that ran to completion at ``at`` — all three marks on that instant."""
    return _sweep_state(started_at=at, published_at=at, done_at=at, frozen=frozen)


class _Sweeps:
    """``run_scan_sweep`` stand-in that stamps ``_last_sweep`` exactly as the real one does — start
    mark first, then the publication mark carrying the risk state, then completion."""

    def __init__(self, clock: Clock, last_sweep: dict, *, raises: bool = False,
                 frozen: bool = False) -> None:
        self.calls: list[str] = []
        self._clock, self._last_sweep, self._raises, self._frozen = clock, last_sweep, raises, frozen

    async def __call__(self, trigger: str) -> str:
        self.calls.append(trigger)
        self._last_sweep["started_at"] = self._clock.now()
        if self._raises:
            raise RuntimeError("duckdb stall")     # …leaving the start mark standing, as the real one does
        self._last_sweep.update(published_at=self._clock.now(), frozen=self._frozen)
        self._last_sweep.update(done_at=self._clock.now(), published=2, pending=3)
        return "body"


@pytest.mark.parametrize(
    ("when", "mode_kw", "expected"),
    [
        (_IN_WINDOW, {}, True),
        (_IN_WINDOW, {"mode_": Mode.AUTO}, True),
        (_IN_WINDOW, {"mode_": Mode.OFF}, False),                      # OFF never originates
        (_IN_WINDOW, {"window": None}, False),                         # no window seeded yet
        (datetime(2026, 6, 17, 9, 16, tzinfo=IST), {}, False),         # session open, window shut
        (datetime(2026, 6, 17, 15, 10, tzinfo=IST), {}, False),        # window ended
        (datetime(2026, 6, 17, 15, 45, tzinfo=IST), {"window": TradeWindow(start=time(9, 20), end=time(23, 0))}, False),  # session closed
        (_WEEKEND_MIDDAY, {}, False),                                  # no session at all
        # All four bounds are INCLUSIVE, and every one of them is a plausible instant: the window tick
        # and the warm-up refresh ride 60 s timers armed at the same boot, so a lift lands ON the
        # second boundary often enough that an off-by-one `<` would silently refuse the day's re-sweep.
        (datetime(2026, 6, 17, 9, 15, tzinfo=IST),
         {"window": TradeWindow(start=time(9, 15), end=time(15, 0))}, True),      # == session open
        (datetime(2026, 6, 17, 15, 30, tzinfo=IST),
         {"window": TradeWindow(start=time(9, 20), end=time(15, 30))}, True),     # == session close
        (datetime(2026, 6, 17, 9, 20, tzinfo=IST), {}, True),                     # == window start
        (datetime(2026, 6, 17, 15, 0, tzinfo=IST), {}, True),                     # == window end
    ],
)
def test_sweep_window_predicate(calendar, when: datetime, mode_kw: dict, expected: bool) -> None:
    """ONE predicate for the window edge and the freeze-lift subscriber — the 2026-09-11 finding was
    that the edge's own test omitted risk_state; a second, drifting copy is how that repeats."""
    assert _sweep_window_active(when, calendar, _FakeMode(**mode_kw)) is expected


async def test_freeze_lift_sweeps_on_the_frozen_to_normal_edge(calendar) -> None:
    """08-28/08-31/09-04: the window-open sweep landed inside a boot warm-up freeze, the pipeline
    re-armed every batch pair and nothing re-published them — batch rules have no next bar."""
    last_sweep = _sweep_state()
    clock = _clock_at(_IN_WINDOW)
    sweeps = _Sweeps(clock, last_sweep)
    state: dict = {"fired": None}

    await _freeze_lift_sweep(_lift(), clock, calendar, _FakeMode(), sweeps, state, last_sweep,
                             ready=lambda: True)

    assert sweeps.calls == ["freeze_lift"]
    assert state["fired"] == (_IN_WINDOW.date(), 10, 5)


async def test_freeze_lift_does_not_sweep_before_the_boot_completes(
    calendar, caplog, tmp_path, monkeypatch, migrated_db_template
) -> None:
    """The RECOVERY publishes lifts of its own — the self-test's ``clear_stale_daily`` (step 1c) and
    step 5's ``catchup_safety_jobs`` clear — ahead of step 4's data-gap backfill, step 5's daily_bars
    catch-up, step 6's warm-up gate and step 7's ticker resume. A sweep there reads a daily history
    still missing the last session(s) and charges those pairs through ``prescreen.admit``, deduping
    the real window-open sweep out of exactly them. Nothing is lost by waiting: no sweep has run yet,
    so there is nothing to redo."""
    last_sweep = _sweep_state()
    clock = _clock_at(_IN_WINDOW)
    sweeps = _Sweeps(clock, last_sweep)
    state: dict = {"fired": None}
    ready = {"engine": False}               # the boot contract's own flag, seen through the closure

    with caplog.at_level(logging.INFO, logger="engine.ops.main"):
        await _freeze_lift_sweep(_lift(), clock, calendar, _FakeMode(), sweeps, state, last_sweep,
                                 ready=lambda: ready["engine"])

    assert sweeps.calls == []
    assert state["fired"] is None          # the minute is NOT claimed — a later lift may still sweep
    skips = [r for r in caplog.records if r.getMessage() == "freeze_lift_sweep_skipped"]
    assert [r.reason for r in skips] == ["boot_in_progress"]

    # …and the SAME event once the boot has completed does sweep.
    ready["engine"] = True
    await _freeze_lift_sweep(_lift(), clock, calendar, _FakeMode(), sweeps, state, last_sweep,
                             ready=lambda: ready["engine"])
    assert sweeps.calls == ["freeze_lift"]

    # The flag the composed engine's subscriber reads is the boot contract's own (raised by ``_boot``
    # after its last blocking step — ``test_boot_runs_its_steps_in_the_order_its_comments_state``),
    # and the subscriber is armed by ``compose``, ahead of the whole boot.
    caplog.clear()
    async with _sweep_world(tmp_path, monkeypatch, migrated_db_template) as (c, w):
        with caplog.at_level(logging.INFO, logger="engine.ops.main"):
            await c.engine.bus.apublish(TOPIC_RISK_STATE, _lift())
            await _settle(c.engine)
            assert [r.reason for r in caplog.records
                    if r.getMessage() == "freeze_lift_sweep_skipped"] == ["boot_in_progress"]
            assert w.admits == []
            c.boot_state["engine_ready"] = True
            await c.engine.bus.apublish(TOPIC_RISK_STATE, _lift())
            await _settle(c.engine)
        assert [kw["trigger"] for _, kw in w.legs["hi52"]] == ["freeze_lift"]


@pytest.mark.parametrize(
    ("old", "new"),
    [
        (RiskState.NORMAL, RiskState.FROZEN),       # the freeze itself: nothing to re-publish
        (RiskState.FROZEN, RiskState.CLOSE_ONLY),   # still no entries
        (RiskState.FROZEN, RiskState.KILLED),
        (RiskState.NORMAL, RiskState.NORMAL),       # not an edge
    ],
)
async def test_freeze_lift_ignores_every_non_lift_transition(calendar, old, new) -> None:
    last_sweep = _sweep_state()
    clock = _clock_at(_IN_WINDOW)
    sweeps = _Sweeps(clock, last_sweep)

    await _freeze_lift_sweep(_lift(old, new), clock, calendar, _FakeMode(), sweeps,
                             {"fired": None}, last_sweep, ready=lambda: True)

    assert sweeps.calls == []


@pytest.mark.parametrize(
    ("when", "mode_kw"),
    [
        (datetime(2026, 6, 17, 8, 40, tzinfo=IST), {}),      # pre-open lift (the 08-31 shape)
        (datetime(2026, 6, 17, 15, 40, tzinfo=IST), {}),     # after the close
        (_WEEKEND_MIDDAY, {}),
        (_IN_WINDOW, {"mode_": Mode.OFF}),
    ],
)
async def test_freeze_lift_does_not_sweep_outside_the_window(calendar, when, mode_kw) -> None:
    """A sweep out of the window spends unrefundable day slots on candidates the pipeline is
    guaranteed to drop (the 2026-08-18 batch-window finding)."""
    last_sweep = _sweep_state()
    clock = _clock_at(when)
    sweeps = _Sweeps(clock, last_sweep)

    await _freeze_lift_sweep(_lift(), clock, calendar, _FakeMode(**mode_kw), sweeps,
                             {"fired": None}, last_sweep, ready=lambda: True)

    assert sweeps.calls == []


async def test_freeze_lift_debounces_the_minute_and_a_just_finished_sweep(calendar) -> None:
    """A latch resolving several causes publishes one NORMAL transition per cause, and a lift can
    land on the heels of the window-open sweep it interrupted."""
    now = {"t": _IN_WINDOW}
    clock = Clock(time_source=lambda: now["t"])
    last_sweep = _sweep_state()
    sweeps = _Sweeps(clock, last_sweep)
    state: dict = {"fired": None}

    async def lift() -> None:
        await _freeze_lift_sweep(_lift(), clock, calendar, _FakeMode(), sweeps, state, last_sweep,
                                 ready=lambda: True)

    await lift()
    await lift()                                   # same minute -> one sweep
    assert sweeps.calls == ["freeze_lift"]

    now["t"] = _IN_WINDOW + timedelta(seconds=90)  # new minute, but the sweep just finished
    await lift()
    assert sweeps.calls == ["freeze_lift"]

    now["t"] = _IN_WINDOW + timedelta(minutes=3)   # gap cleared -> a second lift may re-sweep
    await lift()
    assert sweeps.calls == ["freeze_lift", "freeze_lift"]


def test_freeze_lift_skip_reasons_are_named() -> None:
    """The skip must be readable in the log — "nothing happened" is what made 09-04 take a day to
    diagnose."""
    assert _freeze_lift_skip_reason(_IN_WINDOW, (_IN_WINDOW.date(), 10, 5), _sweep_state()) == \
        "already_swept_this_minute"
    assert _freeze_lift_skip_reason(
        _IN_WINDOW, None, _sweep_state(started_at=_IN_WINDOW - timedelta(seconds=5)), in_flight=True,
    ) == "sweep_in_flight_pre_publication"    # it will publish under the state this lift established
    # Lock held, no phase mark yet (between `acquire` and the first stamp): still pre-publication.
    assert _freeze_lift_skip_reason(_IN_WINDOW, None, _sweep_state(), in_flight=True) \
        == "sweep_in_flight_pre_publication"
    # A sweep that has ALREADY published — into the freeze this lift just cleared — is the one to
    # redo, running or not: it is why the lift exists.
    assert _freeze_lift_skip_reason(
        _IN_WINDOW, None,
        _sweep_state(started_at=_IN_WINDOW - timedelta(seconds=5),
                     published_at=_IN_WINDOW - timedelta(seconds=1), frozen=True),
        in_flight=True,
    ) is None
    # A sweep that DIED mid-flight leaves its start mark standing with no `done_at`; "running" is the
    # LOCK, never an inference from the marks, so the stale mark cannot latch the lift shut (the
    # 2026-09-01 set-without-clear lesson, 2026-09-12 review).
    assert _freeze_lift_skip_reason(
        _IN_WINDOW, None,
        _sweep_state(started_at=_IN_WINDOW - timedelta(minutes=30)), in_flight=False,
    ) is None
    assert _freeze_lift_skip_reason(_IN_WINDOW, None, _finished(_IN_WINDOW - timedelta(seconds=119))) \
        == "sweep_finished_under_2min_ago"
    assert _freeze_lift_skip_reason(_IN_WINDOW, None, _finished(_IN_WINDOW - timedelta(minutes=2))) \
        is None
    # Yesterday's last sweep and yesterday's fired-minute never suppress today's first lift.
    assert _freeze_lift_skip_reason(_IN_WINDOW, (date(2026, 6, 16), 10, 5), _sweep_state()) is None
    assert _freeze_lift_skip_reason(_IN_WINDOW, None, _finished(_IN_WINDOW - timedelta(days=1))) \
        is None


async def test_a_lift_seconds_after_a_frozen_sweep_still_re_sweeps(calendar) -> None:
    """THE boot shape: the window-edge tick and the warm-up refresh both ride 60 s timers armed at
    the same boot, so the window_open sweep that published into the freeze and the lift that clears
    it land inside the same two minutes. Debouncing against that sweep would re-lose the day."""
    now = {"t": _IN_WINDOW}
    clock = Clock(time_source=lambda: now["t"])
    last_sweep = _sweep_state()
    frozen_sweep = _Sweeps(clock, last_sweep, frozen=True)

    await frozen_sweep("window_open")                      # published into the freeze -> re-armed
    now["t"] = _IN_WINDOW + timedelta(seconds=20)
    sweeps = _Sweeps(clock, last_sweep)
    await _freeze_lift_sweep(_lift(), clock, calendar, _FakeMode(), sweeps, {"fired": None},
                             last_sweep, ready=lambda: True)

    assert sweeps.calls == ["freeze_lift"]
    # …and the same gap after a sweep that published while NORMAL is a plain no-op.
    assert _freeze_lift_skip_reason(
        _IN_WINDOW + timedelta(seconds=20), None, _finished(_IN_WINDOW),
    ) == "sweep_finished_under_2min_ago"


async def test_freeze_lift_survives_a_failing_sweep(calendar, caplog) -> None:
    """A sweep failure must never break the risk-state fan-out (the other subscribers are the owner's
    alert and the dashboard relay)."""
    last_sweep = _sweep_state()
    clock = _clock_at(_IN_WINDOW)
    sweeps = _Sweeps(clock, last_sweep, raises=True)
    state: dict = {"fired": None}

    with caplog.at_level(logging.ERROR, logger="engine.ops.main"):
        await _freeze_lift_sweep(_lift(), clock, calendar, _FakeMode(), sweeps, state, last_sweep,
                                 ready=lambda: True)

    assert sweeps.calls == ["freeze_lift"]
    assert state["fired"] is not None                    # claimed: no same-minute retry storm
    assert "freeze_lift_sweep_failed" in caplog.text


async def test_a_sweep_that_raises_does_not_latch_the_lift_shut(calendar) -> None:
    """2026-09-12 review: the in-flight test is the single-flight LOCK, which `async with` releases
    on every exit path. A sweep that raised stamped `started_at` and never `done_at`; deriving
    "running" from those marks would have read the lift as in-flight for a whole grace period."""
    lock = asyncio.Lock()
    last_sweep = _sweep_state()
    now = {"t": _IN_WINDOW}
    clock = Clock(time_source=lambda: now["t"])

    async def dying_sweep(trigger: str) -> str:
        last_sweep["started_at"] = clock.now()           # phase mark 1, then death: no done_at
        raise RuntimeError("duckdb stall")

    with pytest.raises(RuntimeError):
        await _single_flight_sweep(lock, dying_sweep, "window_open")
    assert not lock.locked()

    now["t"] = _IN_WINDOW + timedelta(minutes=3)         # past the minute + 2-minute debounces
    sweeps = _Sweeps(clock, last_sweep)
    await _freeze_lift_sweep(_lift(), clock, calendar, _FakeMode(), sweeps, {"fired": None},
                             last_sweep, ready=lambda: True, in_flight=lock.locked)
    assert sweeps.calls == ["freeze_lift"]               # the stale start mark did not latch it


def _fresh_cross_frame() -> dict[str, list[float]]:
    """130 completed sessions that fire the LIVE hi52 rule under its own DEFAULT_PARAMS: 128 sessions
    climbing smoothly to 94.00 under a constant 100.00 high (prox 0.94, below the band), then y at
    96.00 on confirming volume — a fresh cross that also clears v2's smooth and no-gap filters."""
    closes = [94.0 - 0.2 * (127 - i) for i in range(128)] + [94.0, 96.0]
    return {
        "high": [100.0] * 130,
        "close": closes,
        "volume": [1000.0] * 129 + [1500.0],
        "open": list(closes),
    }


class _FakeHi52Store:
    """The two ``MarketStore`` reads the hi52 leg makes. One symbol returns a firing history and the
    other an empty frame, so the leg's OWN output — the candidates it hands the sweep's one ranked
    admission — is what the branch test observes."""

    def __init__(self) -> None:
        self.frames: list[tuple[str, date, date]] = []
        self.corp_actions: list[tuple[date, date]] = []

    def get_corp_actions(self, *, ex_from: date, ex_to: date) -> list:
        self.corp_actions.append((ex_from, ex_to))
        return []

    def get_bars_1d_frame(self, symbol: str, start: date, end: date):
        self.frames.append((symbol, start, end))
        return _fresh_cross_frame() if symbol == "RELIANCE" else []


def _hi52_branch(trigger: str) -> tuple:
    """Everything the hi52 leg does, observably: the candidates it returns (symbol, entry, score),
    which histories it read, and which corp-action window it asked for."""
    store = _FakeHi52Store()
    raw = _hi52_daily_leg(
        store, trigger=trigger, eligible=["RELIANCE", "TCS"],
        today=_IN_WINDOW.date(), yesterday=_IN_WINDOW.date() - timedelta(days=1), ex_map={},
    )
    return ([(c.symbol, c.strategy_id, c.raw_levels.entry, c.score) for c in raw],
            store.frames, store.corp_actions)


async def test_freeze_lift_takes_every_window_open_branch(tmp_path, monkeypatch, migrated_db_template) -> None:
    """``freeze_lift`` IS the window_open sweep, re-run because a freeze swallowed the first one, so
    the once-per-session hi52 leg — the ONLY trigger-keyed branch — must fire for it too. Asserted
    on the leg's own candidates (2026-09-11 review: a source pin passes for any new branch spelled a
    different way; 2026-09-12: those candidates ARE the observable now that the promotion folded them
    into the one ranked batch admission and the leg no longer admits anything itself)."""
    window_open = _hi52_branch("window_open")
    assert _hi52_branch("freeze_lift") == window_open     # byte-for-byte the same work
    raw, frames, corp_actions = window_open
    assert raw == [("RELIANCE", "hi52", Decimal("96.00"), 0.96)]
    assert [f[0] for f in frames] == ["RELIANCE", "TCS"]  # over the ELIGIBLE set (2026-09-09)
    assert corp_actions == [(_IN_WINDOW.date() - timedelta(days=400),
                             _IN_WINDOW.date() - timedelta(days=1))]
    # /scan_now stays cheap: no candidates, no ~480×400-session history read.
    assert _hi52_branch("scan_now") == ([], [], [])

    assert _FULL_SWEEP_TRIGGERS == {"window_open", "freeze_lift"}
    assert "scan_now" not in _FULL_SWEEP_TRIGGERS        # the owner's cadence stays cheap
    assert 'await sweep("freeze_lift")' in inspect.getsource(_freeze_lift_sweep)

    # …and in the composed engine, no OTHER branch keys on the trigger: a freeze_lift sweep hands every
    # leg the same input, makes the same ONE ranked admission of all five legs (not a second
    # leftover-capacity one — the §8.6 promotion: a rule that can reach RECOMMEND contests the same
    # slots as brk20/ins/cat) and publishes the same candidates as a window_open sweep.
    seen = {}
    for trigger in ("window_open", "freeze_lift"):
        async with _sweep_world(tmp_path / trigger, monkeypatch, migrated_db_template) as (c, w):
            await c.engine.scan_sweep(trigger)
            assert [kw.pop("trigger") for _, kw in w.legs["hi52"]] == [trigger]
            seen[trigger] = (w.legs, w.admits, w.published)
            if trigger == "freeze_lift":
                # Both fire sites resolve the SAME predicate, and the lift is actually subscribed.
                asked: list[tuple] = []
                monkeypatch.setattr(opsmain, "_sweep_window_active",
                                    lambda *args, asked=asked: asked.append(args) or False)
                await _job(c.engine, "window_sweep_tick")
                c.boot_state["engine_ready"] = True
                await c.engine.bus.apublish(TOPIC_RISK_STATE, _lift())
                await _settle(c.engine)
                assert asked == [(FIXED_NOW, c.engine.calendar, c.engine.mode)] * 2
    assert seen["freeze_lift"] == seen["window_open"]
    ((admitted, in_window),) = seen["window_open"][1]
    assert {sid for sid, _ in admitted} == set(_PUBLICATION_LEG_ORDER) and in_window is True


def _batch_cand(strategy_id: str, symbol: str, score: float) -> SignalCandidate:
    """A batch candidate; only ``strategy_id``/``symbol``/``score`` are read by the order under test."""
    return SignalCandidate(
        signal_id=f"{strategy_id}-{symbol}", strategy_id=strategy_id, symbol=symbol,
        side="BUY", style="swing",
        raw_levels=RawLevels(entry=Decimal("100.00"), stop=Decimal("94.00"), target=None),
        score=score,
    )


async def test_the_batch_publishes_in_leg_order_never_in_cross_strategy_score_order(
    tmp_path, monkeypatch, migrated_db_template
) -> None:
    """ADMISSION and PUBLICATION are two separable consequences of one ranked list, and the §8.6
    promotion deliberately bought `hi52` only the first (2026-09-12 review).

    `prescreen._rank` sorts a batch by RAW score ACROSS strategies, and those scores are not
    comparable: hi52 scores `prox` in [0.95, 1.0] where `ins` scores ~0.5 at a bare ₹1cr crossing
    and `brk20` ~0.5-0.6 — which is why the forward slot ranks by per-strategy QUANTILE instead.
    Publication order is what stamps `fired_at`, and `pipeline._forward_key` breaks ties INSIDE a
    quantile band by `fired_at` ASCENDING — under `MIN_RANK_POPULATION` = 3 that band holds every
    strategy's first two candidates of the day — so first-published wins the scarce analyst forward
    slot (4/day under the DG1-degraded cap). Publishing in LEG order keeps an incomparable score
    SCALE out of that tie-break entirely, for every leg and not just for hi52.
    """
    ranked = [                                   # as `_rank` would return it: score-descending
        _batch_cand(hi52.STRATEGY_ID, "AAA", 0.99),
        _batch_cand(hi52.STRATEGY_ID, "BBB", 0.96),
        _batch_cand(cat.STRATEGY_ID, "EEE", 0.82),
        _batch_cand(ins.STRATEGY_ID, "CCC", 0.55),
        _batch_cand(brk20.STRATEGY_ID, "DDD", 0.52),
    ]
    assert [c.symbol for c in _publication_order(ranked)] == \
        ["DDD", "CCC", "EEE", "AAA", "BBB"]
    # The work order's own case: a 0.99 hi52 and a 0.55 brk20 both admitted publish brk20 FIRST.
    pair = [_batch_cand(hi52.STRATEGY_ID, "AAA", 0.99), _batch_cand(brk20.STRATEGY_ID, "DDD", 0.55)]
    assert [c.symbol for c in _publication_order(pair)] == ["DDD", "AAA"]
    # …by a STABLE sort, so the admit order within each leg survives untouched — and §9.6 replay
    # determinism holds, the key reading `strategy_id` and never a clock.
    assert [c.symbol for c in _publication_order(list(reversed(ranked)))] == \
        ["DDD", "CCC", "EEE", "BBB", "AAA"]
    assert _PUBLICATION_LEG_ORDER == ("brk20", "ins", "cat", "cat_reversal", "hi52")
    # An id outside the declared order publishes last, deterministically, rather than crashing or
    # jumping the queue: a new leg added to the concatenation is a deliberate edit here, not a
    # silent reallocation of `fired_at`.
    unknown = [_batch_cand("newleg", "ZZZ", 0.10), _batch_cand(hi52.STRATEGY_ID, "AAA", 0.99)]
    assert [c.symbol for c in _publication_order(unknown)] == ["AAA", "ZZZ"]
    # Empty and single-leg batches are the ordinary quiet morning, not an edge case to guard.
    assert _publication_order([]) == []
    assert [c.symbol for c in _publication_order(ranked[:1])] == ["AAA"]
    # …and the composed sweep actually publishes its one admission that way: hi52 scores 0.99 against
    # 0.5 for every other leg, and still publishes last.
    async with _sweep_world(tmp_path, monkeypatch, migrated_db_template) as (c, w):
        await c.engine.scan_sweep("window_open")
    legs = [sid for sid, _ in w.published]
    assert [sid for i, sid in enumerate(legs) if i == 0 or legs[i - 1] != sid] == list(_PUBLICATION_LEG_ORDER)


async def test_the_hi52_promotion_is_one_state_in_both_of_its_halves(
    tmp_path, monkeypatch, migrated_db_template
) -> None:
    """The §8.6 promotion is TWO wirings and a demotion is two edits; either half alone leaves a
    half-promoted rule, so both halves are pinned here (2026-09-12 review — nothing asserted the
    promotion itself, and `NO_EDGE_SHADOW_STRATEGIES`' own docstring makes the case for a test).

    Half one: hi52 is OUT of the unconditional-C3-reject set. Half two: an expected edge IS
    registered, because a targetless proposal with no registered edge is the gate's ordinary
    `_NO_TARGET` reject and the rule would never reach RECOMMEND.
    """
    assert hi52.STRATEGY_ID not in NO_EDGE_SHADOW_STRATEGIES
    live = load_settings()
    assert live.hi52.expected_edge_pct is not None
    # …asserted on the MAP the composition root actually hands the gate, not on a source string.
    # hi52 1.53 = registered_edge_pct() under O17's 2.0x gap mult (1.47 at 2.5x) — the derivation is
    # pinned in tests/unit/test_hi52_edge.py; this pins the WIRING of its output.
    assert _strategy_expected_edge_pct(live) == {
        ins.STRATEGY_ID: Decimal("1.58"), hi52.STRATEGY_ID: Decimal("1.53"),
    }
    # …while the shadows it left behind stay shadows.
    assert NO_EDGE_SHADOW_STRATEGIES == {"cat", "cat_reversal"}
    # …through the live StrategyEdges wrapper over this map (O19 §3.3; its identity is
    # test_the_gate_and_the_pipeline_share_one_live_strategy_edges), which only ever lowers hi52 —
    # here as the composed gate actually holds both halves.
    built: dict[str, list] = {}
    _spy(monkeypatch, "RiskGate", built)
    async with composed_engine(tmp_path, monkeypatch, db_template=migrated_db_template):
        pass
    ((_, gate_kw, _),) = built["RiskGate"]
    assert gate_kw["strategy_expected_edge_pct"][hi52.STRATEGY_ID] == Decimal("1.53")
    assert gate_kw["no_edge_shadow_strategies"] == NO_EDGE_SHADOW_STRATEGIES


def test_deleting_the_settings_key_really_un_registers_the_hi52_edge(tmp_path) -> None:
    """Step 2 of the kill criterion — "delete `hi52.expected_edge_pct` from settings.yaml" — has to
    BITE (2026-09-12 review: a numeric model default would have made it a no-op, leaving the gate
    consuming a 1.47% edge from a number no reader of settings.yaml can see).

    So: load the REAL settings.yaml with the `hi52:` block removed, and assert both halves of the
    demotion's second edit — the config value is None, and the map the composition root builds has
    no hi52 entry at all. A targetless proposal with no registered edge is the gate's ordinary
    `_NO_TARGET` reject, so the absence fails closed.
    """
    raw = yaml.safe_load((config_dir() / "settings.yaml").read_text(encoding="utf-8"))
    assert raw.pop("hi52")                               # it IS there today — the promotion is live
    (tmp_path / "settings.yaml").write_text(yaml.safe_dump(raw), encoding="utf-8")

    demoted = load_settings(tmp_path)
    assert demoted.hi52.expected_edge_pct is None
    edges = _strategy_expected_edge_pct(demoted)
    assert hi52.STRATEGY_ID not in edges
    assert edges == {ins.STRATEGY_ID: Decimal(str(demoted.ins.expected_edge_pct))}
    # The model default is the load-bearing half of that: `Settings()` with no YAML at all must not
    # resurrect an edge either (the cat/cat_reversal idiom — absence means absence).
    assert Settings().hi52.expected_edge_pct is None
    assert hi52.STRATEGY_ID not in _strategy_expected_edge_pct(Settings())


async def test_a_second_trigger_is_skipped_while_a_sweep_is_in_flight(caplog) -> None:
    """Single-flight (2026-09-11 review): the window edge and the warm-up refresh ride 60 s timers
    armed by the same scheduler.start(), so a lift and the sweep it interrupts fire in the same tick.
    A sweep is two passes over the eligible universe's daily history, each taking MarketStore._lock —
    two at once is the WO-25c contention profile that wedged an 11-hour boot."""
    lock = asyncio.Lock()
    running, release = asyncio.Event(), asyncio.Event()
    calls: list[str] = []

    async def body(trigger: str) -> str:
        calls.append(trigger)
        running.set()
        await release.wait()
        return f"{trigger} body"

    first = asyncio.create_task(_single_flight_sweep(lock, body, "window_open"))
    await running.wait()
    with caplog.at_level(logging.INFO, logger="engine.ops.main"):
        assert await _single_flight_sweep(lock, body, "scan_now") == _SWEEP_IN_FLIGHT_REPLY
    assert calls == ["window_open"]                       # the body never ran a second time
    skipped = [r for r in caplog.records if r.getMessage() == "scan_sweep_skipped_in_flight"]
    assert [r.trigger for r in skipped] == ["scan_now"]

    release.set()
    assert await first == "window_open body"
    # …and the guard is released, not latched: the next trigger runs normally.
    assert await _single_flight_sweep(lock, body, "window_open") == "window_open body"
    assert calls == ["window_open", "window_open"]


async def test_the_freeze_lift_queues_behind_an_in_flight_sweep_instead_of_being_dropped(
    caplog,
) -> None:
    """The lift is the day's ONLY re-publication of the batch legs, and a sweep that is still running
    can already have published its whole batch INTO the freeze this lift just cleared. Dropping it
    there would re-lose the day; `_freeze_lift_skip_reason` has already skipped the in-flight case
    where nothing is lost (the running sweep has not published yet)."""
    lock = asyncio.Lock()
    running, release = asyncio.Event(), asyncio.Event()
    calls: list[str] = []

    async def body(trigger: str) -> str:
        calls.append(trigger)
        running.set()
        await release.wait()
        return f"{trigger} body"

    first = asyncio.create_task(_single_flight_sweep(lock, body, "window_open"))
    await running.wait()
    with caplog.at_level(logging.INFO, logger="engine.ops.main"):
        queued = asyncio.create_task(_single_flight_sweep(lock, body, "freeze_lift", queue=True))
        await asyncio.sleep(0)                            # a chance to run, which it must not take
    assert calls == ["window_open"]                       # waiting, NOT dropped and NOT concurrent
    # …and the wait is NAMED: a lift that silently sat out a slow sweep is unreadable in the log.
    waits = [r for r in caplog.records if r.getMessage() == "scan_sweep_queued_behind_in_flight"]
    assert [r.trigger for r in waits] == ["freeze_lift"]
    assert not [r for r in caplog.records if r.getMessage() == "scan_sweep_skipped_in_flight"]

    release.set()
    assert await first == "window_open body"
    assert await queued == "freeze_lift body"
    assert calls == ["window_open", "freeze_lift"]        # sequential, never overlapping


class _FakeTelegram:
    """The bot's wiring surface ``compose`` uses; keeps what it was handed."""

    def __init__(self) -> None:
        self.scan_sweep_fn = None
        self.sent: list = []

    def set_paper(self, *_args, **_kwargs) -> None: ...
    def set_reco_book(self, _book) -> None: ...
    def set_why_fn(self, _fn) -> None: ...

    def set_scan_sweep_fn(self, fn) -> None:
        self.scan_sweep_fn = fn

    async def send(self, msg) -> None:
        self.sent.append(msg)


async def test_every_sweep_trigger_goes_through_the_single_flight_entry_point(
    tmp_path, monkeypatch, migrated_db_template, caplog
) -> None:
    """One lock, one entry point: the window edge, the owner's /scan_now and the lift all resolve it,
    and the lift is the only one that waits instead of being skipped. The window edge is a
    once-per-day event, so a skipped edge is GIVEN BACK and the next 60 s tick re-fires it (an owner
    /scan_now in flight is no substitute: it skips the once-per-session hi52 leg). The lift's
    in-flight test is the lock itself, never the phase marks (2026-09-12 review)."""
    telegram = _FakeTelegram()
    monkeypatch.setattr(opsmain, "_build_telegram", lambda *_a, **_k: telegram)
    entered, release = threading.Event(), threading.Event()
    async with _sweep_world(tmp_path, monkeypatch, migrated_db_template) as (c, w):
        e = c.engine
        recording_leg = brk20.sweep_daily

        def held_leg(histories, **kwargs):                    # inside the worker, BEFORE publication
            entered.set()
            assert release.wait(10)
            return recording_leg(histories, **kwargs)

        monkeypatch.setattr(brk20, "sweep_daily", held_leg)
        assert telegram.scan_sweep_fn is e.scan_sweep                        # /scan_now: the entry point
        owner_sweep = asyncio.create_task(telegram.scan_sweep_fn("scan_now"))
        assert await asyncio.to_thread(entered.wait, 10)
        assert await e.scan_sweep("scan_now") == _SWEEP_IN_FLIGHT_REPLY      # a second /scan_now skips
        await _job(e, "window_sweep_tick")                                   # the edge skips…
        c.boot_state["engine_ready"] = True
        with caplog.at_level(logging.INFO, logger="engine.ops.main"):
            await e.bus.apublish(TOPIC_RISK_STATE, _lift())                  # pre-publication: no redo
            await _settle(e)
        assert [r.reason for r in caplog.records if r.getMessage() == "freeze_lift_sweep_skipped"] \
            == ["sweep_in_flight_pre_publication"]
        release.set()
        await owner_sweep
        await _job(e, "window_sweep_tick")                                   # …and is given back
        assert [kw["trigger"] for _, kw in w.legs["hi52"]] == ["scan_now", "window_open"]

        # A lift landing while a sweep holds the lock AFTER publishing (into the freeze it lifts)
        # queues behind it and re-sweeps once it is done.
        c.time.set(FIXED_NOW + timedelta(minutes=3))
        monkeypatch.setattr(brk20, "sweep_daily", recording_leg)
        e.store.upsert_universe_daily([{"d": FIXED_NOW.date(), "symbol": "LATE", "included": True,
                                        "exclusion_reasons": None}])
        e.store.upsert_bars_1d([DailyBar(symbol="LATE", d=FIXED_NOW.date() - timedelta(days=1),
                                         open=Decimal("1"), high=Decimal("1"), low=Decimal("1"),
                                         close=Decimal("1"), volume=1)])
        w.hold = asyncio.Event()
        held = asyncio.create_task(e.scan_sweep("scan_now"))
        while not w.published or w.published[-1] != (brk20.STRATEGY_ID, "LATE"):
            await asyncio.sleep(0.01)
        await e.bus.apublish(TOPIC_RISK_STATE, _lift())
        await asyncio.sleep(0.05)
        assert [kw["trigger"] for _, kw in w.legs["hi52"]][-1] == "scan_now"   # still queued
        w.hold.set()
        await held
        await _settle(e)
        assert [kw["trigger"] for _, kw in w.legs["hi52"]] == ["scan_now", "window_open", "scan_now",
                                                               "freeze_lift"]


async def test_sweep_evidence_bound_legs_read_the_core_set_brk20_the_eligible_set(
    tmp_path, monkeypatch, migrated_db_template
) -> None:
    """O18 (2026-10-08): with the index widened to NIFTY Total Market, hi52 and both cat legs
    originate from the CORE (NIFTY 500) subset — their registered edge / frozen verdict population —
    while brk20 keeps scanning the full eligible set."""
    async with _sweep_world(tmp_path, monkeypatch, migrated_db_template) as (c, w):
        await c.engine.scan_sweep("window_open")
    assert [symbols for symbols, _ in w.legs["brk20"]] == [[_CORE, _WATCH, _WIDE]]   # eligible
    assert [symbols for symbols, _ in w.legs["hi52"]] == [[_CORE, _WATCH]]           # core
    assert [symbols for symbols, _ in w.legs["cat"]] == [[_CORE]]                    # core rows only
    assert [symbols for symbols, _ in w.legs["cat_reversal"]] == [[_CORE]]


def test_ticker_tokens_carry_todays_batch_symbols_until_the_day_rolls() -> None:
    """The §7.1 gate's only price source is the tick cache, and brk20/hi52/ins/cat originate over the
    ELIGIBLE set — 27 of 84 brk20 slots and 1 of 16 hi52 slots reached the gate with ltp=None and
    failed closed (2026-09-11). Exercises the PRODUCTION composition (`ticker_tokens` delegates to it
    verbatim), roll included, rather than re-assembling the same list here."""
    tokens = {"RELIANCE": 1, "TCS": 2, "BPCL": 3, "NIFTY 50": 99, "INDIA VIX": 98}
    state: dict = {"day": None, "symbols": set()}
    day = date(2026, 6, 17)

    def subscription(today: date) -> list[int]:
        return _ticker_tokens(watchlist=["RELIANCE", "TCS"], held=["RELIANCE"], batch_state=state,
                              today=today, token_for_symbol=tokens.get)

    assert subscription(day) == [1, 2, 99, 98]           # nothing admitted yet: watchlist + index/VIX
    _roll_batch_ticks(state, day).update(["BPCL", "RELIANCE", "DELISTED"])   # DELISTED: no token
    # BPCL joins; RELIANCE (watchlist AND held AND batch) is not duplicated; an untokened symbol drops.
    assert subscription(day) == [1, 2, 3, 99, 98]
    # Day rolls: yesterday's admissions are not today's universe. The roll lives INSIDE the function
    # under test, so losing it fails here instead of growing the subscription every session.
    assert subscription(day + timedelta(days=1)) == [1, 2, 99, 98]
    assert state["symbols"] == set()


def _positions(conn, *, real: str | None = None, paper: tuple[str, ...] = ()) -> None:
    """An open REAL (recommended) position and open PAPER positions, in the current paper epoch."""
    at = FIXED_NOW.replace(hour=9, minute=30).isoformat()
    conn.execute("INSERT INTO paper_state (id, epoch_started_at) VALUES (1, ?)",
                 (FIXED_NOW.replace(hour=9, minute=0).isoformat(),))
    rows = [("r-1", real, 0, "recommended")] if real is not None else []
    rows += [(f"p-{i}", symbol, 1, "platform") for i, symbol in enumerate(paper, 1)]
    for pid, symbol, is_paper, origin in rows:
        conn.execute(
            "INSERT INTO positions (position_id, symbol, side, product, qty, avg_entry, state, is_paper, "
            "origin, opened_at) VALUES (?, ?, 'BUY', 'CNC', 10, '100', 'OPEN', ?, ?, ?)",
            (pid, symbol, is_paper, origin, at),
        )


def _quiet_universe_job(monkeypatch) -> None:
    """``job_universe``'s three network-backed steps as no-ops: the build keeps today's seeded rows."""
    async def nothing(self, *_args, **_kwargs) -> None:
        return None

    for cls, method in ((opsmain.MisLeverageIngest, "refresh"), (opsmain.SurveillanceIngest, "current"),
                        (opsmain.UniverseBuilder, "build")):
        monkeypatch.setattr(cls, method, nothing)


async def test_batch_symbols_reach_the_feed_but_never_the_warmup_coverage_set(
    tmp_path, monkeypatch, migrated_db_template
) -> None:
    """CONSTRAINT: the warm-up coverage set stays the WATCHLIST. A batch symbol in it means one missing
    bar on a name nobody trades FREEZES entries — the exact failure this work order exists to stop —
    while the FEED does carry the batch admissions, after watchlist + held, on every sweep and every
    universe build."""
    from engine.ops.warmup import WarmupGate

    built: dict[str, list] = {}
    _spy(monkeypatch, "WarmupGate", built)
    coverage: list[list[str]] = []
    real_set = WarmupGate.set_symbols

    def set_symbols(self, symbols) -> None:
        coverage.append(list(symbols))
        real_set(self, symbols)

    monkeypatch.setattr(WarmupGate, "set_symbols", set_symbols)
    _quiet_universe_job(monkeypatch)
    async with _sweep_world(tmp_path, monkeypatch, migrated_db_template,
                            state_db=lambda conn: _positions(conn, paper=("PAPONLY",))) as (c, w):
        e = c.engine
        ((_, gate_kw, _),) = built["WarmupGate"]
        assert gate_kw["symbols"] == [_WATCH]                 # the boot coverage set: the watchlist
        feed = _watch_feed(monkeypatch, e)
        await e.scan_sweep("window_open")
        watch, held, batch, index = [1], [5], [2, 3], [99, 98]   # WATCH | PAPONLY | CORE1 WIDE1 | idx
        assert feed == [watch + held + batch + index] and coverage == []
        await _spec(e, JOB_UNIVERSE).run()
        assert coverage == [[_WATCH]]
        assert feed[-1] == watch + held + batch + index and len(feed) == 2


async def test_surveillance_ingest_is_wired_to_open_real_positions(
    tmp_path, monkeypatch, migrated_db_template
) -> None:
    """A8: the held-position surveillance alert lives in ``SurveillanceIngest.refresh`` so every
    refresh path (08:20 job, catch-up, early hydration, job_universe's first use) runs it; it only
    fires when the engine passes the real-scoped held-positions reader."""
    built: dict[str, list] = {}
    _spy(monkeypatch, "SurveillanceIngest", built)
    async with composed_engine(tmp_path, monkeypatch, db_template=migrated_db_template,
                               state_db=lambda conn: _positions(conn, real="REALCO", paper=("PAPERCO",))) as c:
        ((_, kw, _),) = built["SurveillanceIngest"]
        assert kw["held_positions"]() == [("r-1", "REALCO", 10)]          # the real one, never paper
        c.engine.conn.execute("UPDATE positions SET qty = 7 WHERE position_id = 'r-1'")
        assert kw["held_positions"]() == [("r-1", "REALCO", 7)]           # read live, not at boot


async def test_universe_wrapper_repairs_newcomers_daily_history_at_any_hour(
    tmp_path, monkeypatch, migrated_db_template
) -> None:
    """2026-09-15: OLAELEC entered the watchlist at 10:09 with a 57-session hole in bars_1d and froze
    the DAILY class for 12 h — the minute-only newcomer fill (mid-session-gated) never touched daily
    history. The daily-history repair must run for every newcomer regardless of time of day (pre-open
    builds included — daily history is historical); the minute fill stays mid-session-only, and
    (2026-09-18) CONFIRMS upstream-empty minutes clamped to the session close, so a post-close build
    never marks after-hours minutes no-trade."""
    fills: list[tuple] = []

    async def daily_gap(self, symbols, sessions):
        fills.append(("daily", list(symbols)))
        return SimpleNamespace(bars_written=0, skipped_covered=0, failed=[])

    async def warmup_gap(self, symbols, frm, to, *, confirm_until):
        fills.append(("minute", list(symbols), frm, to, confirm_until))
        return SimpleNamespace(bars_written=0)

    newcomers = iter(["PREOPEN", "MIDDAY", "AFTERCLOSE"])
    stores: list = []

    async def build(self, d) -> None:
        stores[0].upsert_universe_daily([{"d": d, "symbol": next(newcomers), "included": True,
                                          "exclusion_reasons": None}])

    async def nothing(self, *_args) -> None:
        return None

    monkeypatch.setattr(opsmain.BackfillJob, "daily_gap", daily_gap)
    monkeypatch.setattr(opsmain.BackfillJob, "warmup_gap", warmup_gap)
    monkeypatch.setattr(opsmain.UniverseBuilder, "build", build)
    monkeypatch.setattr(opsmain.MisLeverageIngest, "refresh", nothing)
    monkeypatch.setattr(opsmain.SurveillanceIngest, "current", nothing)
    day = FIXED_NOW.date()
    async with composed_engine(tmp_path, monkeypatch, db_template=migrated_db_template, kite=True,
                               now=datetime.combine(day, time(8, 30), tzinfo=IST)) as c:
        e = c.engine
        stores.append(e.store)
        session = e.calendar.session(day)
        await _spec(e, JOB_UNIVERSE).run()                              # 08:30, before the open
        midday = datetime.combine(day, time(10, 5), tzinfo=IST)
        c.time.set(midday)
        await _spec(e, JOB_UNIVERSE).run()
        after_close = datetime.combine(day, time(16, 0), tzinfo=IST)
        c.time.set(after_close)
        await _spec(e, JOB_UNIVERSE).run()
    assert fills == [
        ("daily", ["PREOPEN"]),
        ("daily", ["MIDDAY"]),
        ("minute", ["MIDDAY"], session.open, midday, midday - timedelta(minutes=2)),
        ("daily", ["AFTERCLOSE"]),
        ("minute", ["AFTERCLOSE"], session.open, after_close, session.close),
    ]


async def test_warmup_refresh_repair_confirms_upstream_empty_minutes(
    tmp_path, monkeypatch, migrated_db_template
) -> None:
    """2026-09-18: the 60 s self-heal is the path that closes a tradeless-minute hole (PTCIL 13:36
    on 09-16). Its repair must pass ``confirm_until=to`` — ``to`` is already ``now − 2 min`` and
    in-session, so a minute Kite has not published yet can never be confirmed — over today's
    watchlist."""
    from engine.ops.warmup import WarmupStatus

    handed: dict = {}
    repaired: list[tuple] = []

    async def refresh(warmup_gate, holder, mode, lifecycle, *, alert=None, clock=None) -> None:
        holder["status"] = WarmupStatus(ready=False, blockers=["intraday:hole 1/1"])

    async def maybe_repair(status, state, *, clock, calendar, repair, token_valid=None, **_kw) -> bool:
        handed.update(status=status, repair=repair, token_valid=token_valid)
        return False

    async def warmup_gap(self, symbols, frm, to, *, confirm_until):
        repaired.append((list(symbols), frm, to, confirm_until))

    monkeypatch.setattr(opsmain, "refresh_and_lift_warmup", refresh)
    monkeypatch.setattr(opsmain, "maybe_repair_warmup_gaps", maybe_repair)
    monkeypatch.setattr(opsmain.BackfillJob, "warmup_gap", warmup_gap)
    async with composed_engine(tmp_path, monkeypatch, db_template=migrated_db_template, kite=True,
                               market=_seed_world_market) as c:
        e = c.engine
        await e.warmup_refresh()
        assert handed["status"] is e.warmup_holder["status"]
        assert handed["token_valid"] == e.session.token_valid
        frm, to = FIXED_NOW - timedelta(minutes=30), FIXED_NOW - timedelta(minutes=2)
        await handed["repair"](frm, to)
    assert repaired == [([_WATCH], frm, to, to)]


# --- the `ins` leg's once-only bound: the one batch leg a re-sweep cannot re-derive -------------
def _seed_ins_pending(conn, today: date, symbol: str = "RELIANCE") -> None:
    conn.execute(
        "INSERT INTO ins_pending (for_session, symbol, crossing_session, trailing_value, "
        "contributing_filings_n, reference_close, consumed, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?, 0, ?)",
        (today.isoformat(), symbol, (today - timedelta(days=1)).isoformat(), "25000000",
         3, "1200.00", today.isoformat()),
    )


def _sweep_ins_once(conn, today: date, *, in_window: bool, published_frozen: bool) -> list:
    """One sweep's `ins` leg, in the production order: READ in the worker, DECIDE at publication."""
    rows = _read_ins_pending(conn, today)
    candidates = ins.sweep_crossings(rows)
    _consume_ins_pending(
        conn, today,
        _ins_rows_to_consume([c.symbol for c in rows], in_window=in_window,
                             published_frozen=published_frozen),
        now=datetime(today.year, today.month, today.day, 10, 5, tzinfo=IST),
    )
    return candidates


def test_a_sweep_that_publishes_into_a_freeze_leaves_the_crossings_pending(conn) -> None:
    """The 09-04 shape: the window_open sweep published its batch INTO a boot warm-up freeze, the
    pipeline re-armed every pair — and for brk20/hi52/cat the freeze-lift re-sweep re-derives them
    from stored bars. `ins` cannot: the EOD job decided the event and the row's consumed flag is
    PERSISTED, so consuming it under a freeze loses the day's crossing for good. insider_net_buy is
    the only cost-clearing edge in the stack."""
    today = date(2026, 6, 17)
    _seed_ins_pending(conn, today)

    # Sweep 1 — published into a standing freeze: the candidate is re-armed, so nothing is consumed.
    assert [c.symbol for c in _sweep_ins_once(conn, today, in_window=True, published_frozen=True)] \
        == ["RELIANCE"]
    assert [c.symbol for c in _read_ins_pending(conn, today)] == ["RELIANCE"]

    # Sweep 2 — the freeze-lift re-sweep: the SAME crossing originates again, now under NORMAL, and
    # THIS publication is the evaluation, so the row is consumed.
    assert [c.symbol for c in _sweep_ins_once(conn, today, in_window=True, published_frozen=False)] \
        == ["RELIANCE"]
    assert _read_ins_pending(conn, today) == []

    # Sweep 3 — a later in-window sweep re-offers nothing: the bound is once-only and restart-safe.
    assert _sweep_ins_once(conn, today, in_window=True, published_frozen=False) == []
    row = conn.execute(
        "SELECT consumed, consumed_at FROM ins_pending WHERE for_session = ? AND symbol = ?",
        (today.isoformat(), "RELIANCE"),
    ).fetchone()
    assert row["consumed"] == 1 and row["consumed_at"] is not None   # audit trail, never deleted


def test_an_out_of_window_sweep_also_leaves_the_crossings_pending(conn) -> None:
    """The 2026-08-18 carve-out this extends: a WINDOW refusal is not an evaluation either, so an
    out-of-window /scan_now must leave the day's crossings for the next in-window sweep."""
    today = date(2026, 6, 17)
    _seed_ins_pending(conn, today)
    _sweep_ins_once(conn, today, in_window=False, published_frozen=False)
    assert [c.symbol for c in _read_ins_pending(conn, today)] == ["RELIANCE"]


@pytest.mark.parametrize(
    ("in_window", "published_frozen", "expected"),
    [
        (True, False, ["AAA", "BBB"]),   # evaluated: consume EVERY row read, admitted or suppressed
        (True, True, []),                # published into a freeze: re-armed, nothing was decided
        (False, False, []),              # window refusal: never evaluated (2026-08-18)
        (False, True, []),
    ],
)
def test_ins_rows_are_consumed_only_when_the_publication_could_be_evaluated(
    in_window: bool, published_frozen: bool, expected: list[str],
) -> None:
    assert _ins_rows_to_consume(["AAA", "BBB"], in_window=in_window,
                                published_frozen=published_frozen) == expected


async def test_the_sweep_decides_the_ins_consume_at_publication_from_the_real_risk_state(
    tmp_path, monkeypatch, migrated_db_template, caplog
) -> None:
    """ONE read of the risk state serves both the freeze-lift debounce and the consume decision — if
    they disagreed, a sweep could be debounced away as "already done" after destroying the rows it
    was meant to re-publish. Published into a freeze: the crossing stays pending AND a lift right
    after re-sweeps; published under NORMAL: the crossing is consumed AND the same lift is a no-op."""
    for state in (RiskState.FROZEN, RiskState.NORMAL):
        async with _sweep_world(tmp_path / state.value, monkeypatch, migrated_db_template) as (c, w):
            e = c.engine
            reads: list[RiskState] = []
            monkeypatch.setattr(e.mode, "risk_state",
                                lambda reads=reads, state=state: reads.append(state) or state)
            await e.scan_sweep("window_open")
            assert reads == [state]                                       # ONE read, at publication
            pending = [r.symbol for r in _read_ins_pending(e.conn, FIXED_NOW.date())]
            assert pending == ([_CORE] if state is RiskState.FROZEN else [])
            c.boot_state["engine_ready"] = True
            c.time.set(FIXED_NOW + timedelta(seconds=30))
            caplog.clear()
            with caplog.at_level(logging.INFO, logger="engine.ops.main"):
                await e.bus.apublish(TOPIC_RISK_STATE, _lift())
                await _settle(e)
            resweeps = [kw["trigger"] for _, kw in w.legs["hi52"]][1:]
            if state is RiskState.FROZEN:
                assert resweeps == ["freeze_lift"]                         # the redo
            else:
                assert resweeps == []
                assert [r.reason for r in caplog.records if r.getMessage() == "freeze_lift_sweep_skipped"] \
                    == ["sweep_finished_under_2min_ago"]


#: Every paper manager the composition root may construct (the runtime builds the PaperBroker itself).
_PAPER_BUILDERS = ("PaperRuntime", "OrderManager", "PositionBook", "ExitManager", "ProtectionManager",
                   "PaperRisk", "paper_order_guard")


async def test_nothing_paper_is_built_unless_the_subsystem_is_enabled(
    tmp_path, monkeypatch, migrated_db_template
) -> None:
    """Plan §1.1/Q4.1/Q4.9: every paper construction lives in ``_compose_paper``, which the composition
    calls once, under the flag, before the pipeline and the scheduler. Disabled, nothing paper is
    constructed and every paper seam the pipeline, the scheduler and the shutdown see is None."""
    events: list[str] = []
    built: dict[str, list] = {}
    for name in _PAPER_BUILDERS:
        _spy(monkeypatch, name, built)
    real = {name: getattr(opsmain, name) for name in
            ("_compose_paper", "RecommendationPipeline", "_arm_registry_jobs")}

    async def compose_paper(*args, **kwargs):
        events.append("_compose_paper")
        return await real["_compose_paper"](*args, **kwargs)

    def recorded(name: str):
        def call(*args, **kwargs):
            events.append(name)
            obj = real[name](*args, **kwargs)
            built.setdefault(name, []).append((args, kwargs, obj))
            return obj
        return call

    monkeypatch.setattr(opsmain, "_compose_paper", compose_paper)
    monkeypatch.setattr(opsmain, "RecommendationPipeline", recorded("RecommendationPipeline"))
    monkeypatch.setattr(opsmain, "_arm_registry_jobs", recorded("_arm_registry_jobs"))
    for enabled in (False, True):
        events.clear()
        built.clear()
        async with composed_engine(tmp_path / str(enabled), monkeypatch, db_template=migrated_db_template,
                                   paper=enabled) as c:
            e = c.engine
            ((_, pipe_kw, _),) = built["RecommendationPipeline"]
            seams = [pipe_kw[k] for k in ("paper_ctx_builder", "paper_submit", "paper_enabled_fn")]
            paper_tick = e.scheduler._sched.get_job("paper_tick")
            if not enabled:
                assert events == ["RecommendationPipeline", "_arm_registry_jobs"]
                assert [name for name in _PAPER_BUILDERS if name in built] == []
                assert seams == [None, None, None] and paper_tick is None and e.paper is None
            else:
                assert events == ["_compose_paper", "RecommendationPipeline", "_arm_registry_jobs"]
                assert set(_PAPER_BUILDERS) <= set(built)
                assert seams == [e.paper.ctx_builder, e.paper.submit, e.paper.entries_open]
                assert paper_tick is not None


def test_every_live_engine_calendar_excludes_special_sessions() -> None:
    """2026-10-11 decision: the live engine treats special sessions (muhurat) as closed days. Every
    NSECalendar the engine package builds (the composition root and the ins_crossings --once CLI)
    passes include_special=False, so for_session, the scheduler guard and the sweep all agree."""
    engine_dir = Path(opsmain.__file__).resolve().parents[1]
    sites: dict[str, list[object]] = {}
    for path in sorted(engine_dir.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8-sig"))   # some modules carry a BOM
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and ast.unparse(node.func) == "NSECalendar":
                flag = next((k.value for k in node.keywords if k.arg == "include_special"), None)
                rel = path.relative_to(engine_dir).as_posix()
                sites.setdefault(rel, []).append(flag.value if isinstance(flag, ast.Constant) else flag)
    assert sites == {"datafeeds/ins_crossings.py": [False], "ops/main.py": [False]}


async def test_the_capital_changer_is_built_once_on_the_live_objects_and_handed_to_the_app(
    tmp_path, monkeypatch, migrated_db_template
) -> None:
    """O19 Task 5b: ONE CapitalChanger, on the real tracker, the gate's StrategyEdges, the paper
    book's mark and the market store's last 1m close (the price a held symbol gets when neither live
    cache has one), reaches the dashboard through ``_create_app``; the paper runtime seeds its marks
    from that same store."""
    built: dict[str, list] = {}
    for name in ("CapitalChanger", "ExposureTracker", "StrategyEdges", "RiskGate", "PaperRuntime"):
        _spy(monkeypatch, name, built)
    bar_at = FIXED_NOW.replace(hour=9, minute=59)

    def last_close(store) -> None:
        store.insert_bars_1m([Bar(symbol="HELD", ts_minute=bar_at, open=Decimal("10"), high=Decimal("10"),
                                  low=Decimal("10"), close=Decimal("123.45"), volume=1, src="self")])

    async with composed_engine(tmp_path, monkeypatch, db_template=migrated_db_template, paper=True,
                               market=last_close) as c:
        e = c.engine
        ((_, kw, changer),) = built["CapitalChanger"]
        ((gate_args, _, _),) = built["RiskGate"]
        ((_, _, tracker),) = built["ExposureTracker"]
        ((_, _, edges),) = built["StrategyEdges"]
        ((_, paper_kw, _),) = built["PaperRuntime"]
        limits_engine = gate_args[0]
        assert (kw["conn"], kw["clock"], kw["calendar"], kw["mode"]) == (e.conn, e.clock, e.calendar, e.mode)
        assert kw["store"] is limits_engine._store and kw["limits"] is limits_engine
        assert kw["real"] is tracker and kw["edges"] is edges and kw["notify"] is e.notify
        assert kw["paper_mark"] == e.paper.view().mark
        expected = (Decimal("123.45"), bar_at)
        assert await kw["last_mark"]("HELD", FIXED_NOW) == expected               # the run's MarketStore
        assert await paper_kw["last_mark_fn"]("HELD", FIXED_NOW) == expected      # …paper prep's too
        assert e.app.state.capital is changer
