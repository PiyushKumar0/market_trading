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

import inspect
from datetime import date, time, timedelta

import httpx
import pytest

from engine.broker.instruments import InstrumentStore
from engine.core.calendar import NSECalendar
from engine.core.config import config_dir, load_settings
from engine.datafeeds.bhavcopy import BhavcopyJob
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


# --------------------------------------------------------------------------- composition-root closure seam


@pytest.mark.asyncio
async def test_bhavcopy_composition_root_closure_forwards_degraded_result(
    market_store, clock, calendar, conn, monkeypatch
) -> None:
    """Composition-root regression (2026-08-13): ``engine.ops.main``'s ``job_bhavcopy`` closure
    (main.py:832-834) MUST forward ``BhavcopyJob.run()``'s return value to ``spec.run`` — the
    2026-08-12 live bug was exactly this closure discarding it (``await bhavcopy.run(d)`` with no
    ``return``), which meant the ``_job_result_ok`` watermark fix never saw the degraded result
    because ``spec.run`` was always ``None`` regardless of what the underlying job returned.

    Unlike the machinery tests in ``test_catchup_runner.py``/this file's ``_scheduled_runner`` tests
    (which hand-construct ``JobSpec.run`` to already return a meaningful object), this test goes
    through the REAL pieces: a real ``BhavcopyJob`` degraded by a failing HTTP client, a closure that
    mirrors ``engine.ops.main:job_bhavcopy`` verbatim, registered via the real ``build_job_registry``,
    driven through the real ``_scheduled_runner`` — pinning the exact seam that let the closure
    silently swallow the result."""
    async def _instant(_delay: float) -> None:
        return None

    monkeypatch.setattr("engine.core.nse_http._sleep", _instant)  # no retry backoff wait

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("nse unreachable", request=request)

    bhavcopy = BhavcopyJob(market_store, clock, httpx.AsyncClient(transport=httpx.MockTransport(handler)))

    async def job_bhavcopy(d):
        # Mirrors engine.ops.main:job_bhavcopy (main.py:832-834) verbatim — MUST stay in sync.
        return await bhavcopy.run(d)

    fns = _all_noop_fns()
    fns[opsmain.JOB_BHAVCOPY] = job_bhavcopy
    registry = build_job_registry(load_settings(), fns)
    spec = next(s for s in registry.specs() if s.job_id == opsmain.JOB_BHAVCOPY)

    catch_up = CatchUpRunner(conn, clock, calendar)
    await _scheduled_runner(spec, catch_up, clock)()

    today = clock.today()
    row = conn.execute(
        "SELECT status FROM job_runs WHERE job_id=? AND run_for_date=?",
        (opsmain.JOB_BHAVCOPY, today.isoformat()),
    ).fetchone()
    assert row["status"] == "failed"                                    # not the pre-fix "success"
    assert catch_up.was_run(opsmain.JOB_BHAVCOPY, today) is False


#: The 9 composition-root closures forwarded (2026-08-13) so an ok-bearing job result reaches
#: JobSpec.run instead of being discarded to None (the bhavcopy seam above, closed for 8 more jobs).
_FORWARDING_WRAPPERS: tuple[str, ...] = (
    "job_bhavcopy", "job_earnings", "job_corp_actions", "job_sector_map", "job_filings_shp",
    "job_deals", "job_filings_pit", "job_filings_pit_fresh", "job_filings_results",
)


def _wrapper_body(src: str, wrapper_name: str) -> str:
    """Isolate one composition-root closure's own source (blank-line + 4-space-indent separated
    ``async def job_...`` closures inside ``engine.ops.main:run``) — same ``inspect.getsource``
    technique this file already uses (see the news-chain re-sweep pin above)."""
    start = src.index(f"async def {wrapper_name}(")
    next_def = src.find("\n\n    async def ", start)
    end = next_def if next_def != -1 else src.index("\n\n    registry = build_job_registry", start)
    return src[start:end]


@pytest.mark.parametrize("wrapper_name", _FORWARDING_WRAPPERS)
def test_ok_bearing_wrapper_forwards_return_value(wrapper_name: str) -> None:
    """Composition-root regression (2026-08-13), swept over all 9 ok-bearing closures: each MUST
    ``return await <job>.run(...)``, not a bare ``await`` that discards the result and always returns
    None to ``JobSpec.run`` — the exact seam ``test_bhavcopy_composition_root_closure_forwards_degraded_result``
    pins end-to-end for bhavcopy alone. A lightweight source-level sweep (rather than constructing all
    9 real job objects) for the remaining 8: fails loudly if a future edit reintroduces a bare
    ``await job.run(...)`` on any of them."""
    src = inspect.getsource(opsmain.run)
    body = _wrapper_body(src, wrapper_name)
    assert "return await" in body, f"{wrapper_name} does not forward its job's return value"


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
    # One news_poll_<name> job per configured RSS feed, plus the GDELT job.
    assert {"bar_advance", "health_check", "feed_stats", "news_poll_gdelt"} <= armed
    assert {f"news_poll_{name}" for name in settings.news.feeds.rss} <= armed


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


@pytest.mark.asyncio
async def test_orphan_resweep_abandons_headlines_older_than_four_days(market_store, clock) -> None:
    """2026-08-10 filed follow-up: pin the 4-day abandon cutoff. Behaviour first — the exact query
    ``job_news_chain`` issues, with a headline one minute PAST the cutoff excluded and one minute
    inside it swept (boundary itself inclusive, ``published_at >= published_after``), oldest-first,
    already-clustered rows never re-swept. The constant lives inside ``run()``'s closure and cannot
    be imported, so it is pinned over the source the way ``test_stop_path`` pins wiring order —
    changing ``days=4`` (or dropping the 500 cap that keeps re-sweeps bounded) fails here."""
    cutoff = clock.now() - timedelta(days=_RESWEEP_ABANDON_DAYS)
    market_store.insert_news([
        {"headline_id": "past-cutoff", "title": "Stale orphan a minute past the abandon horizon",
         "source_domain": "economictimes.indiatimes.com",
         "url": "https://economictimes.indiatimes.com/markets/past-cutoff.cms",
         "published_at": cutoff - timedelta(minutes=1)},
        {"headline_id": "at-cutoff", "title": "Orphan exactly on the abandon horizon",
         "source_domain": "moneycontrol.com",
         "url": "https://www.moneycontrol.com/news/at-cutoff.html",
         "published_at": cutoff},
        {"headline_id": "inside-cutoff", "title": "Orphan a minute inside the abandon horizon",
         "source_domain": "livemint.com",
         "url": "https://www.livemint.com/market/inside-cutoff.html",
         "published_at": cutoff + timedelta(minutes=1)},
        {"headline_id": "already-clustered", "title": "Fresh headline that already has a cluster",
         "source_domain": "business-standard.com",
         "url": "https://www.business-standard.com/markets/already-clustered.html",
         "published_at": clock.now(), "cluster_id": "c-already"},
    ])

    swept = await market_store.arun(
        market_store.get_news,
        published_after=clock.now() - timedelta(days=4), unclustered_only=True,
    )
    assert [r["headline_id"] for r in swept] == ["at-cutoff", "inside-cutoff"]

    src = inspect.getsource(opsmain.run)
    assert "published_after=clock.now() - timedelta(days=4), unclustered_only=True" in src
    assert "][:500]" in src          # the oldest-first cap: repeated timeouts stay bounded


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
    whose repair returns a BackfillReport-shaped result. Budget charges on BROKER SPEND:
    ``fetched`` spans (completed historical calls) or non-``unknown_instrument_token`` failures."""
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
    in-session only, ``to`` trimmed 2 min back (the live builder owns the tail minutes), one
    attempt per cooldown window, and the 3/day budget charges only on PRODUCTIVE attempts."""
    from datetime import datetime as _dt
    from datetime import timedelta as _td

    from engine.core.clock import IST
    from engine.ops.main import maybe_repair_warmup_gaps

    now = _dt(2026, 6, 17, 10, 5, 33, tzinfo=IST)             # Wed, in-session, mid-minute
    holder, clock, status, calls, repair = _repair_setup(now)
    state: dict = {}

    assert await maybe_repair_warmup_gaps(status, state, clock=clock, calendar=calendar, repair=repair)
    assert len(calls) == 1
    frm, to = calls[0]
    assert frm.hour == 9 and frm.minute == 15
    assert to == now.replace(second=0, microsecond=0) - _td(minutes=2)   # builder-owned tail excluded
    assert state["count"] == 1                                 # productive (bars_written>0) ⇒ charged

    # Within the cooldown: no second attempt (paced even when the first was productive).
    holder["now"] = now + _td(minutes=2)
    assert not await maybe_repair_warmup_gaps(status, state, clock=clock, calendar=calendar, repair=repair)
    assert len(calls) == 1

    # Past the cooldown: retries, up to the per-day budget of 3 PRODUCTIVE attempts.
    holder["now"] = now + _td(minutes=6)
    assert await maybe_repair_warmup_gaps(status, state, clock=clock, calendar=calendar, repair=repair)
    holder["now"] = now + _td(minutes=12)
    assert await maybe_repair_warmup_gaps(status, state, clock=clock, calendar=calendar, repair=repair)
    holder["now"] = now + _td(minutes=18)
    assert not await maybe_repair_warmup_gaps(status, state, clock=clock, calendar=calendar, repair=repair)
    assert len(calls) == 3 and state["count"] == 3             # capped: retrying harder can't close an unfetchable hole

    # A NEW session day resets the budget.
    holder["now"] = _dt(2026, 6, 18, 9, 30, tzinfo=IST)
    assert await maybe_repair_warmup_gaps(status, state, clock=clock, calendar=calendar, repair=repair)
    assert len(calls) == 4 and state["count"] == 1


@pytest.mark.asyncio
async def test_warmup_gap_repair_budget_charges_on_broker_spend_only(calendar) -> None:
    """Review round 2: the budget predicate is BROKER SPEND. Free: pure local scans (transient
    just-closed-minute deficit ⇒ zero gaps in the trimmed window ⇒ no historical call) and
    ``unknown_instrument_token`` spans (recorded pre-network — the instruments map is post-login's
    repair). Charged: completed fetches EVEN WITH ZERO BARS LANDED (the unfillable-hole sweep must
    not retry uncapped all session), real broker failures, and raising repairs (spend-safe)."""
    from datetime import datetime as _dt
    from datetime import timedelta as _td

    from engine.core.clock import IST
    from engine.ops.main import maybe_repair_warmup_gaps

    now = _dt(2026, 6, 17, 10, 5, 33, tzinfo=IST)
    holder, clock, status, calls, scan_only = _repair_setup(now, fetched=0, bars_written=0)
    state: dict = {}

    # Pure local scans: fire (True) every cooldown window, never charge.
    for i in range(3):
        holder["now"] = now + _td(minutes=6 * i)
        assert await maybe_repair_warmup_gaps(status, state, clock=clock, calendar=calendar, repair=scan_only)
    assert len(calls) == 3 and state["count"] == 0

    # Instruments-map misses only (pre-network): free — the budget survives until the map heals.
    _, _, _, calls_ut, unknown_token = _repair_setup(now, fetched=0, bars_written=0,
                                                     failed=("unknown_instrument_token",))
    holder["now"] = now + _td(minutes=20)
    assert await maybe_repair_warmup_gaps(status, state, clock=clock, calendar=calendar, repair=unknown_token)
    assert len(calls_ut) == 1 and state["count"] == 0

    # A repair that raises: swallowed, returns True, charges.
    async def boom(frm, to):
        raise RuntimeError("kite down")

    holder["now"] = now + _td(minutes=26)
    assert await maybe_repair_warmup_gaps(status, state, clock=clock, calendar=calendar, repair=boom)
    assert state["count"] == 1

    # Real broker failure spans: charged.
    _, _, _, calls_f, broker_fail = _repair_setup(now, fetched=0, bars_written=0, failed=("ReadTimeout",))
    holder["now"] = now + _td(minutes=32)
    assert await maybe_repair_warmup_gaps(status, state, clock=clock, calendar=calendar, repair=broker_fail)
    assert len(calls_f) == 1 and state["count"] == 2

    # The UNFILLABLE hole (fetch completed, zero bars landed): charged — the case the cap exists for.
    _, _, _, calls_u, unfillable = _repair_setup(now, fetched=2, bars_written=0)
    holder["now"] = now + _td(minutes=38)
    assert await maybe_repair_warmup_gaps(status, state, clock=clock, calendar=calendar, repair=unfillable)
    assert len(calls_u) == 1 and state["count"] == 3


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
    assert calls == [] and state.get("count", 0) == 0
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
    assert rejected_state.get("count", 0) == 0 and rejected_state.get("last") is None


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
    from engine.core.enums import RiskState
    from engine.core.protected_store import ProtectedStore
    from engine.core.types import OwnerConfirmation
    from engine.ops.lifecycle import SessionLifecycle
    from engine.ops.main import refresh_and_lift_warmup
    from engine.ops.selftest import SelfTest
    from engine.ops.warmup import WarmupStatus
    from engine.risk.causes import RiskStateLatch
    from engine.core.enums import Actor
    from engine.core.types import TradeWindow
    from engine.risk.kill import KillSwitch
    from engine.risk.mode import ModeManager
    from tests.unit.test_lifecycle_selftest import OWNER_OK, FakeSecrets, FakeSettings, REQUIRED_AT_STARTUP

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
            return WarmupStatus(ready=self.ready, blockers=[] if self.ready else ["orb:AAA bars 165/182"])

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
