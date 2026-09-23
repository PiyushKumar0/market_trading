"""Plan §9.4 chaos case 13 — Platform starts on an exchange holiday (R6).

Must hold (§9.4 row 13, verbatim): "calendar guard: no session, no LLM calls (governor), jobs skip,
heartbeat-only health".

Scenario: the engine boots at 09:40 IST on Fri 2026-06-26 — "Muharram", a full-day holiday in
``config/calendar/2026.yaml`` (the calendar is loaded STRICT, as ``engine.ops.main`` does in prod).
The real §2.6 ``SessionLifecycle.startup`` runs with the real ``SelfTest`` (incl. the D11 SDK smoke
seam), the real ``build_job_registry`` inventory behind a real ``CatchUpRunner``, the real
``HeartbeatWriter`` thread and the real ``HealthMonitor``. Only the external edges are faked: the SDK
smoke call and every job body are recorders, and anything that would reach an LLM / the store is a
tripwire that fails the test on first touch.

Clauses covered (one test each):
* "no session"               — ``test_holiday_boot_has_no_session_and_opens_no_entries``
* "no LLM calls (governor)"  — ``test_holiday_makes_no_llm_calls``
* "jobs skip"                — ``test_holiday_jobs_skip_and_are_never_replayed``
* "heartbeat-only health"    — ``test_holiday_health_is_heartbeat_only``
Every guard assertion is paired with a trading-day CONTROL through the same object, so a guard that
stopped guarding (or a test that stopped exercising it) fails loudly.

Phase-3-gated: none — every clause is observable in Phase 2 (RECOMMEND).

Resolved ambiguity: "(governor)" — the §5.6 ``BudgetGovernor.can_invoke`` is deliberately NOT
calendar-aware (its module docstring: "No burn on holidays is emergent — no calls fire"); the holiday
guard lives at every LLM dispatch site (calendar-guarded scheduler, catch-up fire-days, the
pipeline's trade-window seam, the news scorer's market-hours leg, the D11 smoke skip). The test
therefore asserts each dispatch site refuses AND that the governor's spend ledger — which every SDK
call is priced into — stays empty. The §5.4 19:00–22:00 news sweep deliberately scores on holidays
too (news_scoring.py "Spec ambiguities resolved"); it is outside a 09:40 boot and not asserted here.
"""

from __future__ import annotations

import asyncio
from datetime import date, datetime, timedelta

import pytest

from engine.broker.ticker_supervisor import FeedHealth, TickerSupervisor
from engine.core.calendar import NSECalendar
from engine.core.clock import IST, Clock
from engine.core.config import config_dir, load_settings, load_yaml
from engine.core.enums import Actor, Mode, RiskState
from engine.core.protected_store import ProtectedStore
from engine.core.secrets import REQUIRED_AT_STARTUP
from engine.intelligence.governor import BudgetGovernor
from engine.intelligence.harness import load_agent_defs
from engine.ops import main as opsmain
from engine.ops.health import HealthMonitor
from engine.ops.heartbeat import HeartbeatWriter
from engine.ops.jobs import CatchUpRunner, CatchUpScope, JobClass
from engine.ops.lifecycle import SessionLifecycle
from engine.ops.news_scoring import SKIP_OUTSIDE_WINDOWS, NewsScoringJob
from engine.ops.pipeline import RecommendationPipeline
from engine.ops.scheduler import Scheduler
from engine.ops.selftest import SelfTest
from engine.risk.causes import CAUSE_OWNER_PAUSE, RiskStateLatch
from engine.risk.kill import KillSwitch
from engine.risk.mode import ModeManager
from tests.unit.test_lifecycle_selftest import OWNER_OK, FakeSecrets

#: A real full-day holiday read from config/calendar/2026.yaml (a Friday, so the weekend rule cannot
#: be what makes it a non-trading day).
HOLIDAY = date(2026, 6, 26)
HOLIDAY_NAME = "Muharram"
BOOT_AT = datetime(2026, 6, 26, 9, 40, tzinfo=IST)          # mid-"session" on any normal day
PRIOR_TRADING_DAY = date(2026, 6, 25)                        # Thu — every job watermarked up to here
NEXT_TRADING_DAY = date(2026, 6, 29)                         # Mon (27/28 = weekend)
TRADING_CONTROL_AT = datetime(2026, 6, 17, 9, 40, tzinfo=IST)  # the root conftest's trading Wednesday


class _Now:
    """Movable IST time source shared by every component's Clock."""

    def __init__(self, at: datetime) -> None:
        self.at = at

    def __call__(self) -> datetime:
        return self.at


class _Tripped(AssertionError):
    """A component went past its calendar guard and touched something it must not on a holiday."""


class _Tripwire:
    """Stands in for an LLM-reaching collaborator (harness / governor / store / assembler): the first
    attribute access records the hit and raises, so reaching it at all is the failure."""

    def __init__(self, name: str, hits: list[str]) -> None:
        self._name = name
        self._hits = hits

    def __getattr__(self, attr: str):
        self._hits.append(f"{self._name}.{attr}")
        raise _Tripped(f"{self._name}.{attr} touched on a non-trading day")


class _StaleTicker:
    """Duck-typed ``TickerSupervisor.health()`` (the HealthMonitor unit tests' shape): the WORST feed
    state — STALE — so any session-scoped alarm that leaks onto the holiday shows up."""

    def health(self) -> FeedHealth:
        return FeedHealth(state="STALE", last_tick_age_s=99_999.0)


class _KeepAwake:
    def __init__(self) -> None:
        self.calls: list[bool] = []

    def update(self, session_open: bool) -> None:
        self.calls.append(session_open)


@pytest.fixture
def holiday_engine(conn, db_path, tmp_path, monkeypatch):
    """The §2.6 boot graph as engine.ops.main composes it, on the holiday clock."""
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.setenv("MT_DATA_DIR", str(tmp_path / "data"))   # keep every settings path in tmp
    (tmp_path / "data").mkdir()
    settings = load_settings()

    now = _Now(BOOT_AT)
    clock = Clock(time_source=now)
    calendar = NSECalendar(config_dir() / "calendar", clock, strict=True, sqlite_conn=conn)
    mode = ModeManager(conn, clock, None, calendar)
    kill = KillSwitch(conn, clock)
    latch = RiskStateLatch(conn, clock, mode)

    cfg = tmp_path / "config"
    cfg.mkdir()
    (cfg / "limits.yaml").write_text("schema_version: 1\nlimits: {}\n", encoding="utf-8")
    (cfg / "envelope.yaml").write_text("schema_version: 1\nparameters: {}\n", encoding="utf-8")
    protected = ProtectedStore(cfg, conn, clock)
    protected.register_initial("limits.yaml", OWNER_OK)
    protected.register_initial("envelope.yaml", OWNER_OK)

    sdk_smoke_calls: list[str] = []

    async def sdk_smoke() -> str:          # D11: the ONE cheap SDK call the boot makes (Claude SDK edge)
        sdk_smoke_calls.append(clock.now().isoformat())
        return "ok"

    job_calls: list[tuple[str, date | None]] = []

    def _recorder(job_id: str):
        async def run(*args):
            job_calls.append((job_id, args[0] if args else None))
        return run

    fns = {jid: _recorder(jid) for jid in (*opsmain.PHASE1_JOB_IDS, *opsmain.PHASE2_JOB_IDS)}
    registry = opsmain.build_job_registry(settings, fns)
    catch_up = CatchUpRunner(conn, clock, calendar, registry, deferred=opsmain.POST_ARM_JOB_IDS)
    for spec in registry.specs():           # an engine that ran normally through Thursday
        catch_up.record_run(spec.job_id, PRIOR_TRADING_DAY)

    self_test = SelfTest(
        conn=conn, clock=clock, settings=settings, secrets=FakeSecrets(REQUIRED_AT_STARTUP),
        protected_store=protected, kill_switch=kill, mode_manager=mode, catch_up=catch_up,
        latch=latch, sdk_smoke=sdk_smoke, calendar=calendar,
    )
    heartbeat = HeartbeatWriter(db_path, clock, interval_s=3600)
    notified: list = []

    async def notify(msg) -> None:
        notified.append(msg)

    lifecycle = SessionLifecycle(
        conn=conn, clock=clock, calendar=calendar, settings=settings, mode_manager=mode,
        kill_switch=kill, self_test=self_test, catch_up=catch_up, notify=notify,
        build_version="chaos-13", heartbeat=heartbeat, latch=latch,
    )
    rig = type("HolidayRig", (), {})()
    rig.__dict__.update(
        now=now, clock=clock, calendar=calendar, mode=mode, kill=kill, latch=latch,
        settings=settings, registry=registry, catch_up=catch_up, lifecycle=lifecycle,
        self_test=self_test, heartbeat=heartbeat, notified=notified,
        sdk_smoke_calls=sdk_smoke_calls, job_calls=job_calls, conn=conn,
    )
    yield rig
    heartbeat.stop()


async def _boot(rig):
    await rig.mode.request_transition(Mode.RECOMMEND, Actor.OWNER)   # the Phase-2 sticky mode
    return await rig.lifecycle.startup(check_skew=False)             # NTP is not this case's subject


# ------------------------------------------------------------------------------------ no session
async def test_holiday_boot_has_no_session_and_opens_no_entries(holiday_engine):
    rig = holiday_engine
    holidays = {h["date"]: h["name"] for h in load_yaml(config_dir() / "calendar" / "2026.yaml")["holidays"]}
    assert holidays[HOLIDAY.isoformat()] == HOLIDAY_NAME and HOLIDAY.weekday() == 4   # a real weekday holiday

    report = await _boot(rig)

    # The boot itself completes cleanly — a holiday start is NOT a fault (runbook §10.4 13/14: "no action").
    assert report.frozen_reasons == [] and report.killed is False
    assert any(str(m.kind) == "startup_report" for m in rig.notified)
    # Calendar guard: no session, no trade window, so nothing can be in-window.
    assert rig.calendar.is_trading_day(HOLIDAY) is False
    assert rig.calendar.session(HOLIDAY) is None
    with pytest.raises(ValueError, match="not a trading day"):
        rig.calendar.trade_window(HOLIDAY)
    # The order-surface / entry predicate the composition root builds: not a trading day ⇒ never
    # in-window (main.order_guard's ValueError branch) ⇒ no entry and no opening order in any mode.
    assert rig.mode.risk_state() == RiskState.NORMAL
    assert rig.mode.entries_allowed(in_window=False) is False
    assert rig.mode.opening_orders_allowed(in_window=False) is False
    # CONTROL: the same calendar hands a real trading day a session + window.
    assert rig.calendar.session(NEXT_TRADING_DAY) is not None
    rig.calendar.trade_window(NEXT_TRADING_DAY)


# --------------------------------------------------------------------------------- no LLM calls
async def test_holiday_makes_no_llm_calls(holiday_engine):
    rig = holiday_engine
    hits: list[str] = []
    await _boot(rig)

    async def smoke_check():
        report = await rig.self_test.run(check_skew=False, include_freshness=False)
        return next(c for c in report.checks if c.name == "sdk_smoke")

    # (1) D11 boot smoke — the one SDK call every startup may make — is skipped on a holiday.
    assert rig.sdk_smoke_calls == []
    smoke = await smoke_check()
    assert smoke.status.value == "SKIP" and "non-trading day" in smoke.detail
    assert rig.sdk_smoke_calls == []

    # (2) The LLM-dispatching registry jobs (pre-open planner, nightly reviewer, news chain → digest)
    #     never fire: not from the boot catch-up, not from the armed scheduler, not from the sweep.
    llm_jobs = {opsmain.JOB_PREOPEN_PLANNER, opsmain.JOB_NIGHTLY_REVIEW, opsmain.JOB_NEWS_CHAIN,
                opsmain.JOB_CATALYST_DIGEST}
    scheduler = Scheduler(rig.clock, rig.calendar)
    opsmain._arm_registry_jobs(scheduler, rig.registry, rig.catch_up, rig.clock)
    for job_id in sorted(llm_jobs):
        await scheduler._sched.get_job(job_id).func()           # the calendar-guarded fire itself
    await rig.catch_up.catch_up(scope=CatchUpScope.DEFERRED)    # the post-arm one-shot
    rig.now.at = datetime(2026, 6, 26, 23, 30, tzinfo=IST)       # every fire-time of the day has passed
    await rig.catch_up.catch_up(scope=CatchUpScope.ALL)         # the 30-min sweep
    assert [c for c in rig.job_calls if c[0] in llm_jobs] == []
    rig.now.at = BOOT_AT

    # (3) Intraday analyst: the pipeline's heartbeat trigger reads the calendar's trade window
    #     (pipeline._window) and returns before the governor, the assembler or the harness.
    pipeline = RecommendationPipeline.__new__(RecommendationPipeline)
    pipeline._clock, pipeline._calendar = rig.clock, rig.calendar
    pipeline._governor = _Tripwire("governor", hits)
    pipeline._assembler = _Tripwire("assembler", hits)
    pipeline._harness = _Tripwire("harness", hits)
    await pipeline.heartbeat()
    assert hits == []

    # (4) News analyst: the §5.4 market-hours scoring leg requires a trading day (R6).
    scorer = NewsScoringJob(
        _Tripwire("store", hits), _Tripwire("resolver", hits), _Tripwire("assembler", hits),
        _Tripwire("harness", hits), load_agent_defs(load_yaml(config_dir() / "agents.yaml")),
        _Tripwire("governor", hits), rig.clock, rig.calendar,
    )
    result = await scorer.run_batch()
    assert result.skipped_reason == SKIP_OUTSIDE_WINDOWS and result.scored == 0
    assert hits == []

    # (governor) Every SDK call is priced into the governor's ledger — the holiday left it empty.
    governor = BudgetGovernor.from_config(rig.conn, rig.clock, rig.calendar)
    assert governor.window_spend() == 0
    assert rig.conn.execute("SELECT COUNT(*) FROM budget_ledger").fetchone()[0] == 0

    # CONTROLS on a trading day (same objects, clock moved): each guard really is the thing that
    # stopped the call — past it, the tripwire is hit.
    rig.now.at = TRADING_CONTROL_AT.replace(hour=10, minute=5)   # inside the seeded 10:00–10:30 window
    with pytest.raises(_Tripped):
        await pipeline.heartbeat()
    rig.now.at = TRADING_CONTROL_AT
    with pytest.raises(_Tripped):
        await scorer.run_batch()
    smoke = await smoke_check()
    assert smoke.status.value == "PASS" and len(rig.sdk_smoke_calls) == 1


# ------------------------------------------------------------------------------------ jobs skip
async def test_holiday_jobs_skip_and_are_never_replayed(holiday_engine):
    rig = holiday_engine
    report = await _boot(rig)
    assert report.jobs_caught_up == [] and report.jobs_failed == []

    # The live scheduler: every calendar-guarded trading-day fire is a logged no-op on the holiday.
    scheduler = Scheduler(rig.clock, rig.calendar)
    opsmain._arm_registry_jobs(scheduler, rig.registry, rig.catch_up, rig.clock)
    guarded = [s.job_id for s in rig.registry.specs() if s.job_id != opsmain.JOB_SECTOR_MAP]
    for job_id in guarded:
        await scheduler._sched.get_job(job_id).func()
    # Safety-critical jobs are not "stale" on a holiday — so no data_freshness FROZEN either.
    assert rig.catch_up.stale_safety_jobs() == []
    # The post-arm one-shot + a late-evening sweep, after every fire-time of the day has passed.
    await rig.catch_up.catch_up(scope=CatchUpScope.DEFERRED)
    rig.now.at = datetime(2026, 6, 26, 23, 30, tzinfo=IST)
    await rig.catch_up.catch_up(scope=CatchUpScope.ALL)

    assert rig.job_calls == []
    assert rig.conn.execute(
        "SELECT COUNT(*) FROM job_runs WHERE run_for_date = ?", (HOLIDAY.isoformat(),)
    ).fetchone()[0] == 0
    assert rig.mode.risk_state() == RiskState.NORMAL          # nothing latched a freeze

    # Next trading day, after the EOD block: the sweep runs each date-keyed job for Mon ONLY — the
    # holiday is never replayed as a "missed" day (§2.6 step 5: fire-days are NSE trading days).
    rig.now.at = datetime(2026, 6, 29, 23, 30, tzinfo=IST)
    await rig.catch_up.catch_up(scope=CatchUpScope.ALL)
    date_keyed = {s.job_id for s in rig.registry.specs(JobClass.DATE_KEYED)}
    ran_for = {(jid, d) for jid, d in rig.job_calls if jid in date_keyed}
    assert ran_for == {(jid, NEXT_TRADING_DAY) for jid in date_keyed}
    assert all(d != HOLIDAY for _jid, d in rig.job_calls)


# ------------------------------------------------------------------------- heartbeat-only health
async def test_holiday_health_is_heartbeat_only(holiday_engine):
    rig = holiday_engine
    await _boot(rig)

    # The §2.2 liveness heartbeat (dedicated thread, own sqlite connection) IS running on the holiday.
    for _ in range(200):
        if rig.heartbeat.writes >= 1:
            break
        await asyncio.sleep(0.01)
    assert rig.heartbeat.running and rig.heartbeat.writes >= 1
    row = rig.conn.execute("SELECT state, last_alive_at FROM engine_lifecycle WHERE id=1").fetchone()
    assert row["state"] == "RUNNING" and row["last_alive_at"] == BOOT_AT.isoformat()

    # ...and nothing session-scoped raises: a STALE feed, a FROZEN book held for 3 h in an armed
    # mode and a funnel that never forwarded — every one an incident on a trading day — are silent.
    await rig.latch.set_cause(CAUSE_OWNER_PAUSE, RiskState.FROZEN, "chaos-13 worst case", Actor.OWNER)
    alerts: list[tuple[str, str]] = []

    async def alert(severity: str, message: str) -> None:
        alerts.append((severity, message))

    keep_awake = _KeepAwake()

    def monitor(clock: Clock, cal: NSECalendar) -> HealthMonitor:
        return HealthMonitor(
            clock, rig.settings, ticker_supervisor=_StaleTicker(), alert=alert, calendar=cal,
            keep_awake=keep_awake, mode_manager=rig.mode, latch=rig.latch,
            funnel_probe=lambda: (40, 0, 48),
        )

    mon = monitor(rig.clock, rig.calendar)
    problems: list[list[str]] = []
    for minutes in (0, 60, 120, 180):
        rig.now.at = BOOT_AT + timedelta(minutes=minutes)
        problems.append((await mon.check(check_skew=False)).problems)
    session_scoped = {"feed_stale", "store_stalled", "entries_frozen_in_session", "funnel_zero_in_session"}
    assert all(not (set(p) & session_scoped) for p in problems), problems
    assert [a for a in alerts if any(s in a[1] for s in session_scoped)] == []
    assert keep_awake.calls and not any(keep_awake.calls)       # OS keep-awake released all day

    # The tick-silence DEGRADED guard is out-of-session too: a HEALTHY-but-tickless feed stays quiet.
    degraded_notes: list = []

    async def feed_notify(msg) -> None:
        degraded_notes.append(msg)

    sup = TickerSupervisor(rig.settings, rig.clock, bus=None, calendar=rig.calendar, notify=feed_notify)
    sup._state, sup._healthy_since = "HEALTHY", rig.clock.now() - timedelta(hours=1)
    await sup._check_tick_silence(float(rig.settings.ticker.tick_silence_degrade_s))
    assert sup.health().state == "HEALTHY" and degraded_notes == []

    # CONTROL on a trading day: the very same checks DO fire, so the silence above is the calendar's.
    trading = _Now(TRADING_CONTROL_AT)
    tclock = Clock(time_source=trading)
    tcal = NSECalendar(config_dir() / "calendar", tclock, strict=True)
    tmon = monitor(tclock, tcal)
    first = await tmon.check(check_skew=False)
    trading.at = TRADING_CONTROL_AT + timedelta(minutes=45)
    later = await tmon.check(check_skew=False)
    assert "feed_stale" in first.problems and "entries_frozen_in_session" in later.problems
    tsup = TickerSupervisor(rig.settings, tclock, bus=None, calendar=tcal, notify=feed_notify)
    tsup._state, tsup._healthy_since = "HEALTHY", tclock.now() - timedelta(hours=1)
    await tsup._check_tick_silence(float(rig.settings.ticker.tick_silence_degrade_s))
    assert tsup.health().state == "DEGRADED" and len(degraded_notes) == 1
