"""Owner-alert cadence: one alert per EPISODE, not one per occurrence (WO-25b, 2026-08-24).

Measured that morning: 139-223 Telegram sends failed per day for three days, and the outbox that was
supposed to protect them reached 203 rows — of which almost none were things the owner needed twice.
Two sources built it:

* 57 identical ``health problems: ['feed_stale']`` alerts in one morning (one per 60 s health pulse);
* one "Intraday analyst unavailable" alert per failed agent call, for as long as the SDK stayed down.

The health half is pinned in ``test_health_monitor``; this module covers the generic episode clock
(:mod:`engine.notify.episodes`) and its two agent-failure users — the harness's own
``agent X failed`` alert and the pipeline's ``Intraday analyst unavailable``.

The invariant under every test below: **suppression only ever applies to a REPEAT**. The first
occurrence alerts, a change alerts, a failure after a success alerts. What is dropped is the second
identical message inside the window — and it is still logged, so the record is complete either way.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

import pytest
import yaml

from engine.core.calendar import NSECalendar
from engine.core.clock import Clock
from engine.intelligence.governor import TokenUsage
from engine.intelligence.harness import AgentDef, AgentHarness, load_agent_defs
from engine.notify.episodes import REPEAT_AFTER, AlertEpisodes
from engine.ops.pipeline import INTRADAY_AGENT_ID, RecommendationBook
from engine.risk.limits import LimitTable
from engine.strategy.cost_model import CostModel
from tests.unit.test_agent_harness import (
    ENTER_JSON,
    TEST_AGENTS,
    FakeAssistantMessage,
    FakeContext,
    FakeOptions,
    FakeQuery,
    FakeResultMessage,
    enter_validator,
)
from tests.unit.test_reco_pipeline import (
    CALENDAR_DIR,
    LIMITS_YAML,
    NOW,
    FakeHarness,
    StubGate,
    StubLimits,
    Ticker,
    make_pipeline,
    passing_ctx,
    verdict_of,
)

AGENT = "intraday_analyst"


# =========================================================================== the episode clock
def _at(minutes: int) -> datetime:
    return NOW + timedelta(minutes=minutes)


def test_the_first_occurrence_always_alerts():
    """An alerting layer that can swallow the OPENING report of an incident is worse than none."""
    episodes = AlertEpisodes()
    assert episodes.should_alert((AGENT, "sdk_error"), _at(0)) is True


def test_a_repeat_inside_the_window_is_suppressed_and_one_after_it_is_not():
    """The window is measured from the last ALERT, so a condition that never changes produces a
    steady drip (~2/hour) instead of one message per occurrence."""
    episodes = AlertEpisodes()
    assert episodes.should_alert((AGENT, "sdk_error"), _at(0)) is True
    assert episodes.should_alert((AGENT, "sdk_error"), _at(1)) is False
    assert episodes.should_alert((AGENT, "sdk_error"), _at(29)) is False
    assert episodes.should_alert((AGENT, "sdk_error"), _at(31)) is True
    assert episodes.should_alert((AGENT, "sdk_error"), _at(32)) is False   # the NEW window is open


def test_a_different_key_is_a_different_episode():
    """Two things broken at once is two things to know — the throttle is per key, never global."""
    episodes = AlertEpisodes()
    assert episodes.should_alert((AGENT, "sdk_error"), _at(0)) is True
    assert episodes.should_alert((AGENT, "timeout"), _at(0)) is True
    assert episodes.should_alert(("nightly_reviewer", "sdk_error"), _at(0)) is True


def test_reset_closes_the_episode_so_the_next_occurrence_alerts():
    """Recovery closes the incident. Without this a failure 5 minutes after a good call would be
    silently eaten by a window the PREVIOUS outage opened."""
    episodes = AlertEpisodes()
    episodes.should_alert((AGENT, "sdk_error"), _at(0))
    episodes.reset(AGENT)
    assert episodes.should_alert((AGENT, "sdk_error"), _at(1)) is True


def test_reset_is_prefix_scoped_and_leaves_other_subjects_alone():
    """One agent recovering says nothing about another's outage, so only its own keys are retired."""
    episodes = AlertEpisodes()
    episodes.should_alert((AGENT, "sdk_error"), _at(0))
    episodes.should_alert((AGENT, "timeout"), _at(0))
    episodes.should_alert(("nightly_reviewer", "sdk_error"), _at(0))

    episodes.reset(AGENT)

    assert episodes.open_episodes() == 1
    assert episodes.should_alert(("nightly_reviewer", "sdk_error"), _at(1)) is False
    assert episodes.should_alert((AGENT, "timeout"), _at(1)) is True


def test_a_bare_reset_closes_everything():
    episodes = AlertEpisodes()
    episodes.should_alert((AGENT, "sdk_error"), _at(0))
    episodes.should_alert(("nightly_reviewer", "timeout"), _at(0))

    episodes.reset()

    assert episodes.open_episodes() == 0


def test_the_window_is_thirty_minutes():
    """Pinned: often enough that a real outage keeps nagging, rare enough that a whole session of one
    broken thing is ~13 messages rather than the ~390 a 60 s pulse produces."""
    assert REPEAT_AFTER == timedelta(minutes=30)


# =========================================================================== harness alerts (D7)
class _Governor:
    """The three governor calls ``_run`` makes; the ledger itself is tested elsewhere."""

    def __init__(self) -> None:
        self.recorded: list[str] = []

    def can_invoke(self, agent_id: str, call_class: str = "schedule") -> Any:
        return SimpleNamespace(allowed=True, reason=None)

    def price(self, model: str, usage: TokenUsage) -> Decimal:
        return Decimal("0.01")

    async def record(self, agent_id: str, model: str, usage: TokenUsage) -> None:
        self.recorded.append(agent_id)


def _agent_def() -> AgentDef:
    return load_agent_defs(TEST_AGENTS)[AGENT]


def _harness(conn, clock, alerts: list[tuple[str, str]], query_fn=None) -> AgentHarness:
    async def _alert(severity: str, message: str) -> None:
        alerts.append((severity, message))

    return AgentHarness(
        load_agent_defs(TEST_AGENTS), _Governor(), clock, conn,
        query_fn=query_fn, alert=_alert, options_cls=FakeOptions,
    )


@pytest.fixture
def hticker() -> Ticker:
    return Ticker(NOW)


@pytest.fixture
def hclock(hticker: Ticker) -> Clock:
    return Clock(time_source=hticker)


@pytest.mark.asyncio
async def test_the_first_agent_failure_alerts_and_the_repeat_does_not(conn, hclock, hticker):
    """A dead SDK used to raise one owner alert per call — for hours. The failure handling is
    untouched (both calls still return Failed); only the second alert is dropped."""
    alerts: list[tuple[str, str]] = []
    harness = _harness(conn, hclock, alerts)
    agent = _agent_def()

    first = await harness._fail(agent, "sdk_error", "connection reset", call_id="c1")
    hticker.at = NOW + timedelta(minutes=5)
    second = await harness._fail(agent, "sdk_error", "connection reset", call_id="c2")

    assert first.ok is False and second.ok is False        # D7 outcome unchanged for BOTH
    assert len(alerts) == 1
    assert alerts[0][0] == "error" and "sdk_error" in alerts[0][1] and "c1" in alerts[0][1]


@pytest.mark.asyncio
async def test_a_repeat_is_logged_even_when_it_is_not_alerted(conn, hclock, hticker, caplog):
    """The log stays the complete record: the caller's ``agent_call_failed`` WARNING is untouched and
    the suppressed alert leaves its own breadcrumb, so a silent phone is never an unexplained gap."""
    alerts: list[tuple[str, str]] = []
    harness = _harness(conn, hclock, alerts)
    agent = _agent_def()

    await harness._fail(agent, "timeout", "no response", call_id="c1")
    with caplog.at_level("INFO", logger="engine.intelligence.harness"):
        await harness._fail(agent, "timeout", "no response", call_id="c2")

    throttled = [r for r in caplog.records if r.getMessage() == "agent_alert_throttled"]
    assert len(throttled) == 1
    assert throttled[0].agent == AGENT and throttled[0].reason == "timeout"


@pytest.mark.asyncio
async def test_a_different_failure_reason_alerts_immediately(conn, hclock, hticker):
    """Throttling by ``(agent, reason)``: an outage turning into a credit exhaustion is new news."""
    alerts: list[tuple[str, str]] = []
    harness = _harness(conn, hclock, alerts)
    agent = _agent_def()

    await harness._fail(agent, "sdk_error", "connection reset", call_id="c1")
    await harness._fail(agent, "timeout", "no response", call_id="c2")

    assert len(alerts) == 2


@pytest.mark.asyncio
async def test_the_window_expires_and_the_outage_nags_again(conn, hclock, hticker):
    """Still broken half an hour later is worth saying again — the throttle slows the alerts, it must
    never silence a live incident forever."""
    alerts: list[tuple[str, str]] = []
    harness = _harness(conn, hclock, alerts)
    agent = _agent_def()

    await harness._fail(agent, "sdk_error", "connection reset", call_id="c1")
    hticker.at = NOW + REPEAT_AFTER + timedelta(minutes=1)
    await harness._fail(agent, "sdk_error", "connection reset", call_id="c2")

    assert len(alerts) == 2


@pytest.mark.asyncio
async def test_a_successful_call_closes_the_episode_and_the_next_failure_alerts(
    conn, hclock, hticker
):
    """The wiring, through a REAL run: fail, succeed, fail again — all inside one window, and the
    owner hears about both failures because the good call in between ended the first incident."""
    alerts: list[tuple[str, str]] = []
    fake = FakeQuery([FakeAssistantMessage(ENTER_JSON, None), FakeResultMessage(None, 0.0)])
    harness = _harness(conn, hclock, alerts, query_fn=fake)
    agent = _agent_def()

    await harness._fail(agent, "sdk_error", "connection reset", call_id="c1")
    result = await harness.run_single_shot(agent, FakeContext(), enter_validator(hclock))
    assert result.ok, "the fake stream must produce a clean run for this test to mean anything"
    hticker.at = NOW + timedelta(minutes=2)                 # well inside the 30-minute window
    await harness._fail(agent, "sdk_error", "connection reset", call_id="c3")

    assert len(alerts) == 2
    assert "c1" in alerts[0][1] and "c3" in alerts[1][1]


# =========================================================================== pipeline alerts (D7)
@pytest.fixture
def pticker() -> Ticker:
    return Ticker(NOW)


@pytest.fixture
def pclock(pticker: Ticker) -> Clock:
    return Clock(time_source=pticker)


@pytest.fixture
def pcalendar(pclock: Clock, conn) -> NSECalendar:
    return NSECalendar(CALENDAR_DIR, pclock, sqlite_conn=conn)


@pytest.fixture(scope="module")
def limit_table() -> LimitTable:
    return LimitTable.model_validate(yaml.safe_load(LIMITS_YAML.read_text(encoding="utf-8")))


@pytest.fixture(scope="module")
def cost_model() -> CostModel:
    return CostModel.from_config()


@pytest.fixture
def pipeline(conn, pclock, pcalendar, limit_table, cost_model):
    pipe, parts = make_pipeline(
        conn=conn, clock=pclock, calendar=pcalendar,
        book=RecommendationBook(conn, pclock, cost_model),
        harness=FakeHarness(), gate=StubGate(verdict_of("approve", cost_model)), ctx=passing_ctx(),
        limits=StubLimits(limit_table),
    )
    return pipe, parts


def _failed(reason: str = "sdk_error") -> SimpleNamespace:
    return SimpleNamespace(ok=False, reason=reason, detail="the analyst call blew up", call_id="c1")


def _agent_alerts(parts) -> list:
    return [m for m in parts["notify"].messages if m.data.get("rule_id") == "agent_failed"]


@pytest.mark.asyncio
async def test_the_analyst_unavailable_alert_fires_once_per_episode(pipeline, pticker):
    """The heartbeat runs all session; with the SDK down that was one "Intraday analyst unavailable"
    per trigger, per failure — a large share of the 203-deep backlog. One per episode now."""
    pipe, parts = pipeline

    for i in range(6):
        pticker.at = NOW + timedelta(minutes=i)
        await pipe._alert_agent_failed("heartbeat", _failed())

    assert len(_agent_alerts(parts)) == 1
    assert _agent_alerts(parts)[0].title == "Intraday analyst unavailable"


@pytest.mark.asyncio
async def test_the_same_outage_on_another_trigger_is_still_one_episode(pipeline, pticker):
    """The key is ``(agent, reason)`` and deliberately excludes the trigger: a heartbeat and a
    candidate failing on the same reason are ONE outage, and the alert body says so either way."""
    pipe, parts = pipeline

    await pipe._alert_agent_failed("heartbeat", _failed())
    await pipe._alert_agent_failed("signal_candidate", _failed())
    await pipe._alert_agent_failed("position_event", _failed())

    assert len(_agent_alerts(parts)) == 1


@pytest.mark.asyncio
async def test_a_different_reason_and_a_lapsed_window_both_alert(pipeline, pticker):
    """Two escapes from the quiet window, both required: a NEW failure mode, and time passing."""
    pipe, parts = pipeline

    await pipe._alert_agent_failed("heartbeat", _failed("sdk_error"))
    await pipe._alert_agent_failed("heartbeat", _failed("credit_exhausted"))
    pticker.at = NOW + REPEAT_AFTER + timedelta(minutes=1)
    await pipe._alert_agent_failed("heartbeat", _failed("sdk_error"))

    assert len(_agent_alerts(parts)) == 3


@pytest.mark.asyncio
async def test_a_working_analyst_closes_the_episode(pipeline, pticker):
    """``_note_agent_ok`` is called on every successful analyst result: the outage is over, so the
    next failure is a new incident and reaches the owner immediately."""
    pipe, parts = pipeline

    await pipe._alert_agent_failed("heartbeat", _failed())
    pipe._note_agent_ok()
    pticker.at = NOW + timedelta(minutes=1)
    await pipe._alert_agent_failed("heartbeat", _failed())

    assert len(_agent_alerts(parts)) == 2


@pytest.mark.asyncio
async def test_the_failure_is_logged_every_time_it_happens(pipeline, pticker, caplog):
    """D7 is untouched: every occurrence still logs ``intraday_agent_failed`` and still resolves to
    no-proposal. Only the owner-facing cadence changed."""
    pipe, _parts = pipeline

    with caplog.at_level("INFO", logger="engine.ops.pipeline"):
        await pipe._alert_agent_failed("heartbeat", _failed())
        await pipe._alert_agent_failed("heartbeat", _failed())

    assert len([r for r in caplog.records if r.getMessage() == "intraday_agent_failed"]) == 2
    assert len([r for r in caplog.records
                if r.getMessage() == "intraday_agent_alert_throttled"]) == 1


def test_the_pipeline_throttles_the_agent_it_actually_calls():
    """Guards a rename: the episode key uses the pipeline's own analyst id, so a changed constant
    cannot leave the throttle keyed on a name nothing ever reports."""
    assert INTRADAY_AGENT_ID == "intraday_analyst"
