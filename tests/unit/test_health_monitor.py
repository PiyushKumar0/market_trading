"""HealthMonitor feed-staleness alerting (§3.2.12, R2) — session-aware since 2026-07-31.

The midnight-rollover incident: a STALE feed out of session is DEFINITIONAL (no ticks exist at
night), but ``check()`` flagged it unconditionally and alerted every minute from 00:32 once the
date rolled. ``feed_stale`` is a problem ONLY while the session is open (feed lost while running).
"""

from __future__ import annotations

from datetime import datetime, timedelta
from types import SimpleNamespace

from engine.core.calendar import NSECalendar
from engine.core.clock import IST, Clock
from engine.core.config import config_dir, load_settings
from engine.ops.health import HEALTH_REPEAT_MIN, HealthMonitor
from engine.ops.process_memory import ProcessMemory


class FakeTicker:
    def __init__(self, state: str) -> None:
        self._state = state

    def health(self):
        return SimpleNamespace(state=self._state, last_tick_age_s=12345.0)


def _monitor(state: str, at: datetime, *, calendar: bool = True):
    clock = Clock(time_source=lambda: at)
    alerts: list[tuple[str, str]] = []

    async def alert(severity: str, message: str) -> None:
        alerts.append((severity, message))

    cal = NSECalendar(config_dir() / "calendar", clock, strict=False) if calendar else None
    return HealthMonitor(clock, load_settings(), ticker_supervisor=FakeTicker(state),
                         alert=alert, calendar=cal), alerts


async def test_stale_in_session_is_an_incident():
    mon, alerts = _monitor("STALE", datetime(2026, 6, 17, 11, 0, tzinfo=IST))   # trading Wednesday
    report = await mon.check(check_skew=False)
    assert "feed_stale" in report.problems
    assert alerts and "feed_stale" in alerts[0][1]


async def test_stale_out_of_session_is_definitional_not_an_incident():
    # 00:32 the night after a trading day — the 2026-07-31 midnight-rollover alert spam.
    mon, alerts = _monitor("STALE", datetime(2026, 6, 17, 0, 32, tzinfo=IST))
    report = await mon.check(check_skew=False)
    assert "feed_stale" not in report.problems
    assert alerts == []
    # Post-close the same trading day: equally quiet.
    mon2, alerts2 = _monitor("STALE", datetime(2026, 6, 17, 18, 5, tzinfo=IST))
    report2 = await mon2.check(check_skew=False)
    assert "feed_stale" not in report2.problems and alerts2 == []


async def test_stale_without_calendar_stays_quiet():
    mon, alerts = _monitor("STALE", datetime(2026, 6, 17, 11, 0, tzinfo=IST), calendar=False)
    report = await mon.check(check_skew=False)
    assert "feed_stale" not in report.problems and alerts == []


async def test_healthy_and_warming_raise_nothing_in_session():
    for state in ("HEALTHY", "WARMING", "STOPPED"):
        mon, alerts = _monitor(state, datetime(2026, 6, 17, 11, 0, tzinfo=IST))
        report = await mon.check(check_skew=False)
        assert "feed_stale" not in report.problems, state
        assert alerts == [], state


# ------------------------------------------------- problem-set alert episodes (WO-25b, 2026-08-24)
# The pulse is 60 s and used to alert on EVERY pulse a problem existed: 57 identical
# ``health problems: ['feed_stale']`` messages in one morning, the bulk of the 203-deep notification
# backlog that then starved a fresh owner ack. An episode is the problem SET: a change alerts at once,
# an unchanged set repeats at most every HEALTH_REPEAT_MIN minutes, and recovery is announced once.
class _Ticker:
    """A movable time source — ``ticker.at = ...`` advances every Clock built on it."""

    def __init__(self, at: datetime) -> None:
        self.at = at

    def __call__(self) -> datetime:
        return self.at


def _episodic(state: str = "STALE"):
    """Monitor + captured alerts + the ticker that drives its clock, in session on a trading day."""
    ticker = _Ticker(datetime(2026, 6, 17, 11, 0, tzinfo=IST))
    clock = Clock(time_source=ticker)
    alerts: list[tuple[str, str]] = []

    async def alert(severity: str, message: str) -> None:
        alerts.append((severity, message))

    mon = HealthMonitor(clock, load_settings(), ticker_supervisor=FakeTicker(state), alert=alert,
                        calendar=NSECalendar(config_dir() / "calendar", clock, strict=False))
    return mon, alerts, ticker


async def test_a_persistent_problem_alerts_once_not_once_per_pulse():
    """The 57-messages-in-a-morning bug: ten consecutive pulses, one unchanged problem, ONE alert."""
    mon, alerts, ticker = _episodic()

    for i in range(10):
        ticker.at = ticker.at + timedelta(minutes=1)
        report = await mon.check(check_skew=False)
        assert report.problems == ["feed_stale"], i          # the CHECK is unchanged; only the cadence

    assert len(alerts) == 1
    assert alerts[0] == ("warning", "health problems: ['feed_stale']")


async def test_an_unchanged_problem_repeats_only_after_the_quiet_window():
    """A real outage should keep nagging — just not every minute. One reminder per
    HEALTH_REPEAT_MIN, and the window is measured from the last alert, not from the first."""
    mon, alerts, ticker = _episodic()

    await mon.check(check_skew=False)                        # first pulse: alerts
    ticker.at = ticker.at + timedelta(minutes=HEALTH_REPEAT_MIN - 1)
    await mon.check(check_skew=False)
    assert len(alerts) == 1                                  # still inside the window

    ticker.at = ticker.at + timedelta(minutes=2)
    await mon.check(check_skew=False)
    assert len(alerts) == 2                                  # window elapsed: one reminder
    assert alerts[1] == ("warning", "health problems: ['feed_stale']")


async def test_recovery_is_announced_once_and_then_silence():
    """The owner learns the incident ENDED instead of inferring it from the alerts stopping — and
    learns it exactly once, however many clean pulses follow."""
    mon, alerts, ticker = _episodic()

    await mon.check(check_skew=False)
    mon._ticker = FakeTicker("HEALTHY")
    for _ in range(3):
        ticker.at = ticker.at + timedelta(minutes=1)
        await mon.check(check_skew=False)

    assert len(alerts) == 2
    assert alerts[1][0] == "info" and "all clear" in alerts[1][1] and "feed_stale" in alerts[1][1]


async def test_a_problem_returning_after_recovery_is_a_new_episode():
    """Post-recovery the throttle must be closed: the same problem coming back is NEWS, and waiting
    out a window opened by the previous incident would hide it for half an hour."""
    mon, alerts, ticker = _episodic()

    await mon.check(check_skew=False)                         # incident 1
    mon._ticker = FakeTicker("HEALTHY")
    ticker.at = ticker.at + timedelta(minutes=1)
    await mon.check(check_skew=False)                         # recovery announced
    mon._ticker = FakeTicker("STALE")
    ticker.at = ticker.at + timedelta(minutes=1)              # well inside the 30-minute window
    await mon.check(check_skew=False)                         # incident 2

    assert [a[0] for a in alerts] == ["warning", "info", "warning"]


async def test_a_changed_problem_set_alerts_immediately():
    """A change is news whatever the quiet window says — a problem appearing, and one clearing while
    others remain, both alert at once. Driven through the alert seam directly so the set can be
    varied without inventing five different machine faults."""
    mon, alerts, ticker = _episodic()

    await mon._alert_problems(["feed_stale"])
    await mon._alert_problems(["feed_stale"])                          # unchanged: suppressed
    ticker.at = ticker.at + timedelta(minutes=1)
    await mon._alert_problems(["feed_stale", "low_disk"])              # a new problem: fires
    ticker.at = ticker.at + timedelta(minutes=1)
    await mon._alert_problems(["low_disk", "feed_stale"])              # same SET, reordered: quiet
    ticker.at = ticker.at + timedelta(minutes=1)
    await mon._alert_problems(["low_disk"])                            # one cleared: fires

    assert [a[1] for a in alerts] == [
        "health problems: ['feed_stale']",
        "health problems: ['feed_stale', 'low_disk']",
        "health problems: ['low_disk']",
    ]


async def test_an_unwired_alert_callback_is_still_a_no_op():
    """No alert seam ⇒ nothing to throttle and nothing to raise: the check still returns its verdict."""
    clock = Clock(time_source=lambda: datetime(2026, 6, 17, 11, 0, tzinfo=IST))
    mon = HealthMonitor(clock, load_settings(), ticker_supervisor=FakeTicker("STALE"),
                        calendar=NSECalendar(config_dir() / "calendar", clock, strict=False))

    report = await mon.check(check_skew=False)                # must not raise

    assert report.problems == ["feed_stale"]


# ------------------------------------------------------- process-memory telemetry (2026-08-17 crisis)
class _FakeMemoryReader:
    """Records call count and returns a fixed :class:`ProcessMemory`."""

    def __init__(self, mem: ProcessMemory | None) -> None:
        self._mem = mem
        self.calls = 0

    def read(self) -> ProcessMemory | None:
        self.calls += 1
        return self._mem


class _RaisingMemoryReader:
    """Simulates an injected reader that raises directly (not caught by ProcessMemoryReader itself) —
    exercises HealthMonitor's own defensive wrapper, not just the reader's internal try/except."""

    def read(self) -> ProcessMemory:
        raise RuntimeError("boom")


async def test_process_memory_logged_every_fifth_pulse(caplog):
    """The health pulse is 60s (settings.lifecycle.watchdog_poll_s); process_memory is sampled every
    5th pulse (~5 min), not every pulse — see HealthMonitor.__init__ for the cadence rationale."""
    mem = ProcessMemory(private_bytes=111_222_333, working_set_bytes=44_555_666, peak_working_set_bytes=77_888_999)
    reader = _FakeMemoryReader(mem)
    clock = Clock(time_source=lambda: datetime(2026, 6, 17, 11, 0, tzinfo=IST))
    mon = HealthMonitor(clock, load_settings(), memory_reader=reader)

    caplog.set_level("INFO", logger="engine.ops.health")
    for _ in range(4):
        await mon.check(check_skew=False)
    assert reader.calls == 0
    assert "process_memory" not in caplog.text

    await mon.check(check_skew=False)  # 5th pulse: fires
    assert reader.calls == 1
    assert "process_memory" in caplog.text


async def test_process_memory_log_line_carries_the_three_counter_fields(caplog):
    """The emitted line names exactly the fields the crisis needed: private_bytes (PagefileUsage),
    working_set_bytes, peak_working_set_bytes."""
    mem = ProcessMemory(private_bytes=1, working_set_bytes=2, peak_working_set_bytes=3)
    reader = _FakeMemoryReader(mem)
    clock = Clock(time_source=lambda: datetime(2026, 6, 17, 11, 0, tzinfo=IST))
    mon = HealthMonitor(clock, load_settings(), memory_reader=reader, memory_log_every=1)

    caplog.set_level("INFO", logger="engine.ops.health")
    await mon.check(check_skew=False)

    record = next(r for r in caplog.records if r.getMessage() == "process_memory")
    assert record.private_bytes == 1
    assert record.working_set_bytes == 2
    assert record.peak_working_set_bytes == 3


async def test_process_memory_reader_returning_none_logs_nothing(caplog):
    """A read failure inside the reader (already DEBUG-logged there) yields no INFO line at all."""
    reader = _FakeMemoryReader(None)
    clock = Clock(time_source=lambda: datetime(2026, 6, 17, 11, 0, tzinfo=IST))
    mon = HealthMonitor(clock, load_settings(), memory_reader=reader, memory_log_every=1)

    caplog.set_level("INFO", logger="engine.ops.health")
    await mon.check(check_skew=False)

    assert "process_memory" not in caplog.text


async def test_process_memory_reader_failure_does_not_block_health_check():
    """A reader failure must never raise and must never affect the health verdict (telemetry, not a
    check) — compared against a baseline run with no memory reader at all."""
    clock = Clock(time_source=lambda: datetime(2026, 6, 17, 11, 0, tzinfo=IST))
    baseline = HealthMonitor(clock, load_settings(), memory_log_every=1)
    baseline_report = await baseline.check(check_skew=False)

    mon = HealthMonitor(clock, load_settings(), memory_reader=_RaisingMemoryReader(), memory_log_every=1)
    report = await mon.check(check_skew=False)  # must not raise

    assert report.problems == baseline_report.problems
    assert report.healthy == baseline_report.healthy


# ----------------------------------------------- origination liveness (2026-09-01 latch incident)
# The catchup_safety_jobs latch froze entries for TWO whole sessions (08-31, 09-01) with zero pages:
# the engine hummed, exits flowed, and nothing said "you are not originating". Two problems close
# that class: entries_frozen_in_session (armed mode + risk != NORMAL past a threshold) and
# funnel_zero_in_session (slots published, zero forwarded, risk NORMAL past a threshold).
class _FakeMode:
    def __init__(self, mode: str = "RECOMMEND", risk: str = "FROZEN") -> None:
        from engine.core.enums import Mode, RiskState
        self._mode, self._risk = Mode(mode), RiskState(risk)

    def mode(self):
        return self._mode

    def risk_state(self):
        return self._risk


class _FakeLatch:
    def active_causes(self):
        return [("catchup_safety_jobs", "FROZEN", "data_freshness:instruments")]


def _origination_monitor(*, mode: str = "RECOMMEND", risk: str = "FROZEN", funnel=None,
                         at: datetime | None = None, calendar: bool = True):
    ticker = _Ticker(at or datetime(2026, 6, 17, 10, 0, tzinfo=IST))    # trading Wednesday, in-session
    clock = Clock(time_source=ticker)
    alerts: list[tuple[str, str]] = []

    async def alert(severity: str, message: str) -> None:
        alerts.append((severity, message))

    cal = NSECalendar(config_dir() / "calendar", clock, strict=False) if calendar else None
    mon = HealthMonitor(clock, load_settings(), ticker_supervisor=FakeTicker("HEALTHY"), alert=alert,
                        calendar=cal, mode_manager=_FakeMode(mode, risk), latch=_FakeLatch(),
                        funnel_probe=funnel)
    return mon, alerts, ticker


async def test_frozen_in_session_pages_after_threshold_not_before():
    """The 08-31 replay: RECOMMEND + FROZEN from the open — quiet through the warm-up-sized grace
    (a morning boot legitimately holds ~20 min of warm-up freeze), then a page."""
    mon, alerts, ticker = _origination_monitor()

    # The episode clock starts at the FIRST OBSERVING pulse (a sampling watchdog cannot know the
    # true onset), so 30 elapsed minutes exist on the 31st minute-cadence pulse.
    for _ in range(30):                                       # observed minutes 0..29 — inside the grace
        ticker.at = ticker.at + timedelta(minutes=1)
        report = await mon.check(check_skew=False)
        assert "entries_frozen_in_session" not in report.problems

    ticker.at = ticker.at + timedelta(minutes=1)              # 30 observed minutes: past the threshold
    report = await mon.check(check_skew=False)
    assert "entries_frozen_in_session" in report.problems
    assert any("entries_frozen_in_session" in m for _s, m in alerts)
    # The page itself is diagnosable without a shell (2026-09-01 review): cause + state ride the text.
    assert any("catchup_safety_jobs" in m and "FROZEN" in m for _s, m in alerts)


async def test_frozen_timer_resets_on_normal_and_counts_only_in_session():
    """A freeze that clears resets the clock (no page for accumulated non-contiguous minutes), and
    out-of-session or unarmed-mode observations never start the clock at all."""
    mon, alerts, ticker = _origination_monitor()
    for _ in range(25):                                       # 25 frozen minutes — inside the grace
        ticker.at = ticker.at + timedelta(minutes=1)
        await mon.check(check_skew=False)
    mon._mode_manager = _FakeMode("RECOMMEND", "NORMAL")      # freeze lifts
    ticker.at = ticker.at + timedelta(minutes=1)
    await mon.check(check_skew=False)
    mon._mode_manager = _FakeMode("RECOMMEND", "FROZEN")      # re-freezes: a NEW clock
    for _ in range(10):                                       # only 10 contiguous minutes
        ticker.at = ticker.at + timedelta(minutes=1)
        report = await mon.check(check_skew=False)
    assert "entries_frozen_in_session" not in report.problems
    assert alerts == []

    # Out-of-session: FROZEN at 07:00 for hours must never page (lag-watchdog lesson, 2026-08-26).
    mon2, alerts2, ticker2 = _origination_monitor(at=datetime(2026, 6, 17, 7, 0, tzinfo=IST))
    for _ in range(90):
        ticker2.at = ticker2.at + timedelta(minutes=1)        # 07:01..08:30, all pre-open
        report2 = await mon2.check(check_skew=False)
        assert "entries_frozen_in_session" not in report2.problems
    assert alerts2 == []

    # Mode OFF in-session: not armed, never counts.
    mon3, alerts3, ticker3 = _origination_monitor(mode="OFF")
    for _ in range(60):
        ticker3.at = ticker3.at + timedelta(minutes=1)
        report3 = await mon3.check(check_skew=False)
        assert "entries_frozen_in_session" not in report3.problems
    assert alerts3 == []


async def test_funnel_zero_pages_only_when_risk_is_normal():
    """The 08-31 shape (slots published, zero forwarded all day) pages after the threshold while
    NORMAL; while FROZEN it stays quiet (the frozen problem owns that page — one incident, one
    problem). The first eligible pulse only baselines, so the clock starts one pulse later."""
    mon, alerts, ticker = _origination_monitor(risk="NORMAL", funnel=lambda: (5, 0, 6))
    for _ in range(121):                                      # baseline pulse + 120 observed minutes
        ticker.at = ticker.at + timedelta(minutes=1)
        report = await mon.check(check_skew=False)
        assert "funnel_zero_in_session" not in report.problems
    ticker.at = ticker.at + timedelta(minutes=1)              # threshold crossed
    report = await mon.check(check_skew=False)
    assert "funnel_zero_in_session" in report.problems
    assert any("funnel_zero_in_session" in m and "forwarded stuck at 0" in m for _s, m in alerts)

    # Same funnel shape while FROZEN: the funnel problem must never fire.
    mon2, _alerts2, ticker2 = _origination_monitor(risk="FROZEN", funnel=lambda: (5, 0, 6))
    for _ in range(130):
        ticker2.at = ticker2.at + timedelta(minutes=1)
        report2 = await mon2.check(check_skew=False)
        assert "funnel_zero_in_session" not in report2.problems


async def test_funnel_zero_resets_once_anything_forwards():
    """A forward is PROGRESS: it re-baselines and resets the clock, and a static published count
    thereafter is not a stall (nothing new arrived for the analyst)."""
    calls = {"n": 0}

    def funnel():
        calls["n"] += 1
        return (5, 0, 6) if calls["n"] < 100 else (5, 1, 6)   # a forward lands on pulse 100

    mon, alerts, ticker = _origination_monitor(risk="NORMAL", funnel=funnel)
    for _ in range(140):
        ticker.at = ticker.at + timedelta(minutes=1)
        report = await mon.check(check_skew=False)
        assert "funnel_zero_in_session" not in report.problems
    assert alerts == []


async def test_funnel_stall_after_first_forward_still_pages():
    """2026-09-01 review (blocking): `forwarded` is day-cumulative and never returns to zero, so a
    forward path that wedges AFTER the day's first forward must still page — new slots keep
    publishing past the last-progress baseline while the forward count stays put."""
    calls = {"n": 0}

    def funnel():
        calls["n"] += 1
        return (3, 1, 6) if calls["n"] == 1 else (10, 1, 6)   # baseline (3,1), then the wedge

    mon, alerts, ticker = _origination_monitor(risk="NORMAL", funnel=funnel)
    for _ in range(121):                                      # baseline pulse + 120 observed minutes
        ticker.at = ticker.at + timedelta(minutes=1)
        report = await mon.check(check_skew=False)
        assert "funnel_zero_in_session" not in report.problems
    ticker.at = ticker.at + timedelta(minutes=1)
    report = await mon.check(check_skew=False)
    assert "funnel_zero_in_session" in report.problems
    assert any("forwarded stuck at 1" in m for _s, m in alerts)


async def test_funnel_quiet_when_forward_cap_is_spent():
    """A spent §5.6 daily forward cap is quiet BY DESIGN: slots keep publishing on a busy afternoon
    but the governor is deliberately done forwarding — paging here would train the owner to ignore
    the alarm (WO-25b lesson)."""
    calls = {"n": 0}

    def funnel():
        calls["n"] += 1
        return (50 + calls["n"], 6, 6)                        # published grows, cap 6 fully spent

    mon, alerts, ticker = _origination_monitor(risk="NORMAL", funnel=funnel)
    for _ in range(130):
        ticker.at = ticker.at + timedelta(minutes=1)
        report = await mon.check(check_skew=False)
        assert "funnel_zero_in_session" not in report.problems
    assert alerts == []


async def test_funnel_unreadable_cap_narrows_to_zero_forwarded_shape():
    """cap=None (governor unreadable) must not invent pages: with any forward on the book the alarm
    stays quiet, while the unambiguous zero-forwarded-all-day shape still pages."""
    calls = {"n": 0}

    def growing_with_forward():
        calls["n"] += 1
        return (10 + calls["n"], 1, None)

    mon, alerts, ticker = _origination_monitor(risk="NORMAL", funnel=growing_with_forward)
    for _ in range(130):
        ticker.at = ticker.at + timedelta(minutes=1)
        report = await mon.check(check_skew=False)
        assert "funnel_zero_in_session" not in report.problems
    assert alerts == []

    mon2, alerts2, ticker2 = _origination_monitor(risk="NORMAL", funnel=lambda: (10, 0, None))
    for _ in range(121):
        ticker2.at = ticker2.at + timedelta(minutes=1)
        await mon2.check(check_skew=False)
    ticker2.at = ticker2.at + timedelta(minutes=1)
    report2 = await mon2.check(check_skew=False)
    assert "funnel_zero_in_session" in report2.problems


async def test_origination_watch_failures_never_break_the_pulse():
    """A raising funnel probe (or a monitor with no mode manager wired) must never raise into the
    pulse and never invent a problem."""
    def bad_funnel():
        raise RuntimeError("boom")

    mon, alerts, ticker = _origination_monitor(risk="NORMAL", funnel=bad_funnel)
    for _ in range(3):
        ticker.at = ticker.at + timedelta(minutes=1)
        report = await mon.check(check_skew=False)            # must not raise
    assert "funnel_zero_in_session" not in report.problems and alerts == []

    # No mode manager wired (older construction): both checks are inert, nothing raises.
    clock = Clock(time_source=lambda: datetime(2026, 6, 17, 11, 0, tzinfo=IST))
    plain = HealthMonitor(clock, load_settings(),
                          calendar=NSECalendar(config_dir() / "calendar", clock, strict=False))
    report = await plain.check(check_skew=False)
    assert "entries_frozen_in_session" not in report.problems
    assert "funnel_zero_in_session" not in report.problems


async def test_funnel_quiet_when_forward_cap_is_deliberately_zero():
    """2026-09-02 review: a KNOWN cap of 0 (deliberate full pause on analyst spend) is spent from
    minute one and must stay quiet - only cap=None (unreadable) falls back to the zero-forwarded
    shape."""
    from datetime import timedelta

    mon, alerts, ticker = _origination_monitor(risk="NORMAL", funnel=lambda: (5, 0, 0))
    for _ in range(130):
        ticker.at = ticker.at + timedelta(minutes=1)
        report = await mon.check(check_skew=False)
        assert "funnel_zero_in_session" not in report.problems
    assert alerts == []
