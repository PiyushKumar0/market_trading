"""Chaos case 13 — Platform starts on an exchange holiday.

Failure injected: the engine boots at 09:40 IST on an exchange holiday (Fri 2026-06-26, Muharram; the
calendar is STRICT as in prod). Must hold: no session, no LLM calls, jobs skip and are never replayed,
health is heartbeat-only. Every guard assertion is paired with a trading-day CONTROL through the same
object, so a guard that stopped guarding fails loudly.

Real ``SessionLifecycle.startup`` + ``SelfTest``, ``build_job_registry`` behind a ``CatchUpRunner``,
``HeartbeatWriter`` and ``HealthMonitor``; the SDK smoke call and job bodies are recorders, and anything
that would reach an LLM / the store is a tripwire.

``BudgetGovernor.can_invoke`` is deliberately NOT calendar-aware: the holiday guard lives at every LLM
dispatch site (scheduler, catch-up fire-days, the pipeline's trade-window seam, the news scorer's
market-hours leg, the SDK smoke skip), so the test asserts each refuses and that the governor's spend
ledger stays empty. The 19:00–22:00 news sweep deliberately scores on holidays; it is outside a 09:40 boot.
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

#: A full-day holiday from config/calendar/2026.yaml (a Friday, so the weekend rule is not the cause).
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
    """Duck-typed ``TickerSupervisor.health()`` in the worst state (STALE), so any session-scoped alarm leaking onto the holiday shows up."""

    def health(self) -> FeedHealth:
        return FeedHealth(state="STALE", last_tick_age_s=99_999.0)


class _KeepAwake:
    def __init__(self) -> None:
        self.calls: list[bool] = []

    def update(self, session_open: bool) -> None:
        self.calls.append(session_open)


@pytest.fixture
def holiday_engine(conn, db_path, tmp_path, monkeypatch):
    """The boot graph as engine.ops.main composes it, on the holiday clock."""
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

    async def sdk_smoke() -> str:          # the ONE cheap SDK call the boot makes (Claude SDK edge)
        sdk_smoke_calls.append(clock.now().isoformat())
        return "ok"

    job_calls: list[tuple[str, date | None]] = []

    def _recorder(job_id: str):
        async def run(*args):
            job_calls.append((job_id, args[0] if args else None))
        return run

    fns = {jid: _recorder(jid) for jid in (*opsmain.CORE_JOB_IDS, *opsmain.DECISION_JOB_IDS)}
    registry = opsmain.build_job_registry(settings, fns, calendar=calendar)
    catch_up = CatchUpRunner(conn, clock, calendar,
                             registry.select(lambda s: s.job_id not in opsmain.LANE_JOB_IDS),
                             deferred=opsmain.POST_ARM_JOB_IDS)
    compaction_lane = opsmain.CompactionLane(
        CatchUpRunner(conn, clock, calendar, registry.select(lambda s: s.job_id in opsmain.LANE_JOB_IDS)),
        clock, calendar,
    )
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
        settings=settings, registry=registry, catch_up=catch_up, compaction_lane=compaction_lane,
        lifecycle=lifecycle,
        self_test=self_test, heartbeat=heartbeat, notified=notified,
        sdk_smoke_calls=sdk_smoke_calls, job_calls=job_calls, conn=conn,
    )
    yield rig
    heartbeat.stop()


async def _boot(rig):
    await rig.mode.request_transition(Mode.RECOMMEND, Actor.OWNER)   # the RECOMMEND sticky mode
    return await rig.lifecycle.startup(check_skew=False)             # NTP is not this case's subject


# ------------------------------------------------------------------------------------ no session
async def test_holiday_boot_has_no_session_and_opens_no_entries(holiday_engine):
    rig = holiday_engine
    holidays = {h["date"]: h["name"] for h in load_yaml(config_dir() / "calendar" / "2026.yaml")["holidays"]}
    assert holidays[HOLIDAY.isoformat()] == HOLIDAY_NAME and HOLIDAY.weekday() == 4   # a real weekday holiday

    report = await _boot(rig)

    # A holiday start is NOT a fault.
    assert report.frozen_reasons == [] and report.killed is False
    assert any(str(m.kind) == "startup_report" for m in rig.notified)
    assert rig.calendar.is_trading_day(HOLIDAY) is False
    assert rig.calendar.session(HOLIDAY) is None
    with pytest.raises(ValueError, match="not a trading day"):
        rig.calendar.trade_window(HOLIDAY)
    # Not a trading day ⇒ never in-window (main.order_guard's ValueError branch) ⇒ no entry or opening order.
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

    # (1) The SDK boot smoke is skipped.
    assert rig.sdk_smoke_calls == []
    smoke = await smoke_check()
    assert smoke.status.value == "SKIP" and "non-trading day" in smoke.detail
    assert rig.sdk_smoke_calls == []

    # (2) The LLM-dispatching registry jobs never fire: not from the catch-up, the scheduler or the sweep.
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

    # (3) Intraday analyst: entry triggers gate on the pipeline's trade-window seam, which has no window.
    pipeline = RecommendationPipeline.__new__(RecommendationPipeline)
    pipeline._calendar = rig.calendar
    assert pipeline._window(HOLIDAY) is None

    # (4) News analyst: the market-hours scoring leg requires a trading day.
    scorer = NewsScoringJob(
        _Tripwire("store", hits), _Tripwire("resolver", hits), _Tripwire("assembler", hits),
        _Tripwire("harness", hits), load_agent_defs(load_yaml(config_dir() / "agents.yaml")),
        _Tripwire("governor", hits), rig.clock, rig.calendar,
    )
    result = await scorer.run_batch()
    assert result.skipped_reason == SKIP_OUTSIDE_WINDOWS and result.scored == 0
    assert hits == []

    # (governor) Every SDK call is priced into the ledger; the holiday left it empty.
    governor = BudgetGovernor.from_config(rig.conn, rig.clock, rig.calendar)
    assert governor.window_spend() == 0
    assert rig.conn.execute("SELECT COUNT(*) FROM budget_ledger").fetchone()[0] == 0

    # CONTROLS on a trading day (same objects, clock moved): past each guard the tripwire is hit.
    assert pipeline._window(TRADING_CONTROL_AT.date()) is not None
    rig.now.at = TRADING_CONTROL_AT
    with pytest.raises(_Tripped):
        await scorer.run_batch()
    smoke = await smoke_check()
    assert smoke.status.value == "PASS" and len(rig.sdk_smoke_calls) == 1


# ------------------------------------------------------------------------------------ jobs skip
async def _lane_pass(rig) -> None:
    task = rig.compaction_lane.spawn("sweep")
    assert task is not None                 # a holiday and 23:30 are outside the tick_compact no-run window
    await task


async def test_holiday_jobs_skip_and_are_never_replayed(holiday_engine):
    rig = holiday_engine
    report = await _boot(rig)
    assert report.jobs_caught_up == [] and report.jobs_failed == []

    # Every calendar-guarded trading-day fire is a logged no-op.
    scheduler = Scheduler(rig.clock, rig.calendar)
    opsmain._arm_registry_jobs(scheduler, rig.registry, rig.catch_up, rig.clock)
    guarded = [s.job_id for s in rig.registry.specs() if s.job_id != opsmain.JOB_SECTOR_MAP]
    for job_id in guarded:
        await scheduler._sched.get_job(job_id).func()
    # Safety-critical jobs are not "stale" on a holiday, so no data_freshness FROZEN either.
    assert rig.catch_up.stale_safety_jobs() == []
    # Post-arm one-shot + a late-evening sweep, each followed by the compaction lane pass.
    await rig.catch_up.catch_up(scope=CatchUpScope.DEFERRED)
    await _lane_pass(rig)
    rig.now.at = datetime(2026, 6, 26, 23, 30, tzinfo=IST)
    await rig.catch_up.catch_up(scope=CatchUpScope.ALL)
    await _lane_pass(rig)

    assert rig.job_calls == []
    assert rig.conn.execute(
        "SELECT COUNT(*) FROM job_runs WHERE run_for_date = ?", (HOLIDAY.isoformat(),)
    ).fetchone()[0] == 0
    assert rig.mode.risk_state() == RiskState.NORMAL          # nothing latched a freeze

    # Next trading day: date-keyed jobs run for Monday ONLY; the holiday is never replayed as missed.
    rig.now.at = datetime(2026, 6, 29, 23, 30, tzinfo=IST)
    await rig.catch_up.catch_up(scope=CatchUpScope.ALL)
    await _lane_pass(rig)
    date_keyed = {s.job_id for s in rig.registry.specs(JobClass.DATE_KEYED) if s.fire_day is None}
    ran_for = {(jid, d) for jid, d in rig.job_calls if jid in date_keyed}
    assert ran_for == {(jid, NEXT_TRADING_DAY) for jid in date_keyed}
    assert all(d != HOLIDAY for _jid, d in rig.job_calls)


# ------------------------------------------------------------------------- heartbeat-only health
async def test_holiday_health_is_heartbeat_only(holiday_engine):
    rig = holiday_engine
    await _boot(rig)

    # The liveness heartbeat thread runs on the holiday.
    for _ in range(200):
        if rig.heartbeat.writes >= 1:
            break
        await asyncio.sleep(0.01)
    assert rig.heartbeat.running and rig.heartbeat.writes >= 1
    row = rig.conn.execute("SELECT state, last_alive_at FROM engine_lifecycle WHERE id=1").fetchone()
    assert row["state"] == "RUNNING" and row["last_alive_at"] == BOOT_AT.isoformat()

    # Nothing session-scoped raises: a STALE feed, a FROZEN book held 3 h and a dead funnel are silent.
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

    # The tick-silence DEGRADED guard is also out-of-session.
    degraded_notes: list = []

    async def feed_notify(msg) -> None:
        degraded_notes.append(msg)

    sup = TickerSupervisor(rig.settings, rig.clock, bus=None, calendar=rig.calendar, notify=feed_notify)
    sup._state, sup._healthy_since = "HEALTHY", rig.clock.now() - timedelta(hours=1)
    await sup._check_tick_silence(float(rig.settings.ticker.tick_silence_degrade_s))
    assert sup.health().state == "HEALTHY" and degraded_notes == []

    # CONTROL on a trading day: the same checks fire.
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
