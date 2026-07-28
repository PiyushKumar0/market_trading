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
"""

from __future__ import annotations

from datetime import date, time

import pytest

from engine.broker.instruments import InstrumentStore
from engine.core.calendar import NSECalendar
from engine.core.config import config_dir, load_settings
from engine.marketdata.store import MarketStore
from engine.ops import main as opsmain
from engine.ops.jobs import (
    JOB_BHAVCOPY,
    JOB_EARNINGS,
    JOB_INSTRUMENTS,
    JOB_SECTOR_MAP,
    JOB_UNIVERSE,
    CatchUpRunner,
    JobClass,
    JobSpec,
)
from engine.ops.main import (
    PHASE1_JOB_IDS,
    _arm_live_jobs,
    _arm_registry_jobs,
    _scheduled_runner,
    build_job_registry,
    hydrate_instruments_at_startup,
)
from engine.ops.scheduler import Scheduler
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
async def test_degraded_snapshot_without_session_stays_hydrated_with_warning(market_store, clock):
    """Same degraded snapshot pre-login: no session to escalate with — the ladder keeps the hydrated
    (degraded) map and the post-login instruments step heals it on login."""
    degraded = InstrumentStore(clock)
    await degraded.refresh(FakeKite([RELIANCE_ROW]))
    market_store.upsert_instruments_daily(degraded.snapshot_rows(clock.today()))

    instruments = InstrumentStore(clock)
    branch = await hydrate_instruments_at_startup(
        instruments, market_store, kite=None, session_valid=False, clock=clock,
    )
    assert branch == "hydrated"
    assert instruments.index_count == 0
    assert instruments.token_for_symbol("NIFTY 50") is None    # degraded until login refresh


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
    assert len(reg) == len(PHASE1_JOB_IDS) == 17   # +4 §2.8 filings jobs (incl. stage-3 fresh insider)


def test_registry_phase2_jobs_register_when_their_fns_exist() -> None:
    """§8.3 wiring: digest 08:35 → planner 08:50 (run-latest, after the news chain in catch-up
    order), reco-expiry 15:45 run-latest, nightly review 21:00 date-keyed (§2.6 per missed day)."""
    fns = _all_noop_fns()
    fns[opsmain.JOB_CATALYST_DIGEST] = _noop
    fns[opsmain.JOB_PREOPEN_PLANNER] = _noop
    fns[opsmain.JOB_RECO_EXPIRE] = _noop
    fns[opsmain.JOB_NIGHTLY_REVIEW] = _noop_dated
    by_id = {s.job_id: s for s in build_job_registry(load_settings(), fns).specs()}
    assert set(by_id) == set(PHASE1_JOB_IDS) | set(opsmain.PHASE2_JOB_IDS)

    expected = {
        opsmain.JOB_CATALYST_DIGEST: (JobClass.RUN_LATEST, time(8, 35), 25),
        opsmain.JOB_PREOPEN_PLANNER: (JobClass.RUN_LATEST, time(8, 50), 28),
        opsmain.JOB_RECO_EXPIRE:     (JobClass.RUN_LATEST, time(15, 45), 60),
        opsmain.JOB_NIGHTLY_REVIEW:  (JobClass.DATE_KEYED, time(21, 0), 80),
    }
    for jid, (cls, at, order) in expected.items():
        assert (by_id[jid].job_class, by_id[jid].at, by_id[jid].order) == (cls, at, order), jid
    # Catch-up dependency order (§2.7 steps 4-6): news chain before digest before planner.
    news = by_id[opsmain.JOB_NEWS_CHAIN].order
    assert news < by_id[opsmain.JOB_CATALYST_DIGEST].order < by_id[opsmain.JOB_PREOPEN_PLANNER].order


def test_registry_classes_and_fire_times_match_the_schedule() -> None:
    """§10.1: instruments 08:15, surveillance 08:20, universe 08:30, reconcile 15:50, bhavcopy 18:00,
    corp-actions 18:15, earnings 18:30, deals 18:45 — in their §2.6 step-5 classes."""
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
        opsmain.JOB_FEATURES:     (JobClass.DATE_KEYED,      time(18, 50)),
        opsmain.JOB_FILINGS_PIT:       (JobClass.DATE_KEYED, time(18, 35)),
        opsmain.JOB_FILINGS_PIT_FRESH: (JobClass.DATE_KEYED, time(19, 0)),
        opsmain.JOB_FILINGS_RESULTS:   (JobClass.DATE_KEYED, time(18, 45)),
        opsmain.JOB_FILINGS_SHP:     (JobClass.RUN_LATEST,   time(18, 50)),
    }
    for jid, (cls, at) in expected.items():
        assert by_id[jid].job_class == cls, jid
        assert by_id[jid].at == at, jid


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
    assert {"bar_advance", "health_check", "feed_stats",
            "news_poll_et", "news_poll_mc", "news_poll_gdelt"} <= armed


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
