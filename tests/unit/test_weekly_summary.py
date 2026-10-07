"""Plan Q2.6: the weekly owner summary job."""

from __future__ import annotations

import json
from datetime import date, datetime

import pytest

from engine.core.calendar import NSECalendar
from engine.core.clock import IST, Clock
from engine.core.config import config_dir, load_settings
from engine.ops import main as opsmain
from engine.ops.jobs import JOB_REC_OUTCOMES, JOB_WEEKLY_SUMMARY, CatchUpRunner, CatchUpScope
from engine.ops.weekly_summary import WeeklySummaryJob
from tests.unit.test_ops_main_wiring import _all_noop_fns
from tests.unit.test_scorecard import seed_paper

FRI = date(2026, 10, 9)


class _Now:
    at = datetime(2026, 10, 9, 19, 0, tzinfo=IST)

    def __call__(self) -> datetime:
        return self.at


@pytest.fixture
def now() -> _Now:
    return _Now()


@pytest.fixture
def clk(now) -> Clock:
    return Clock(time_source=now)


@pytest.fixture
def cal(clk) -> NSECalendar:
    return NSECalendar(config_dir() / "calendar", clk, strict=False)


def _job(conn, sent, *, was_run=lambda job_id, d: True) -> WeeklySummaryJob:
    async def notify(msg) -> None:
        sent.append(msg)

    return WeeklySummaryJob(conn, notify, was_run)


# rec_id, delivered, kind, human_action, skip_reason, strategy, outcome (status, net_pct, excess_pct), notional
_RECS = [
    ("a", "2026-10-05T10:00:00+05:30", "entry", "taken", None, "hi52", ("closed", 2.0, 1.0), 1000),
    ("b", "2026-10-06T10:00:00+05:30", "entry", "dismissed", "price", "hi52", ("closed", -1.0, -2.0), 2000),
    ("c", "2026-10-07T10:00:00+05:30", "entry", "dismissed", "trust", "ins", ("open", None, None), 1000),
    ("d", "2026-10-08T10:00:00+05:30", "entry", "expired", None, "ins", None, 1000),
    ("e", "2026-10-09T10:00:00+05:30", "entry", "closed", None, "ins", None, 1000),
    ("f", "2026-10-09T10:00:00+05:30", "exit", "taken", None, None, None, 1000),
    ("g", "2026-10-02T10:00:00+05:30", "entry", "dismissed", "price", "ins", ("closed", 4.0, 3.0), 1500),
    ("h", "2026-10-02T10:00:00+05:30", "entry", "taken", None, "ins", ("closed", 6.0, 5.0), 500),
]


def _seed(conn) -> None:
    for rec_id, at, kind, action, skip, sid, outcome, notional in _RECS:
        conn.execute(
            "INSERT INTO recommendations (rec_id, payload, delivered_at, human_action, skip_reason) "
            "VALUES (?, ?, ?, ?, ?)",
            (rec_id, json.dumps({"kind": kind, "notional": str(notional)}), at, action, skip),
        )
        if outcome:
            conn.execute(
                "INSERT INTO rec_outcomes (rec_id, strategy_id, status, net_pct, excess_pct, updated_at) "
                "VALUES (?, ?, ?, ?, ?, 'x')", (rec_id, sid, *outcome),
            )


async def test_content_is_three_hindsight_lines(conn) -> None:
    _seed(conn)
    sent: list = []
    assert (await _job(conn, sent).run(FRI)).unfinished is False
    (msg,) = sent
    # Week of Mon 10-05: a-e delivered; the exit rec and last week's recs are not counted.
    assert msg.body.splitlines() == [
        "Delivered 5, taken 2, skipped 2 (price 1, trust 1), expired without a reason 1.",
        "Skipped recs closed to date: 2, hindsight +40 rupees at each rec's own notional, not platform P&L.",
        "Closed recs to date, hindsight, mean per rec: hi52 n=2 net +0.50% excess -0.50%; "
        "ins n=2 net +5.00% excess +4.00%",
    ]
    assert msg.dedupe_key == "weekly_summary:2026-10-09"


async def test_paper_line_only_with_paper_activity(conn) -> None:
    sent: list = []
    await _job(conn, sent).run(FRI)
    assert "Paper" not in sent[0].body and "paper" not in sent[0].data

    seed_paper(conn)
    await _job(conn, sent).run(FRI)
    assert sent[1].body.splitlines()[3] == (
        "Paper, all epochs, simulated fills: 4 closed, hit 50%, net +550 rupees, 3 open."
    )
    assert sent[1].data["paper"] == {"closed": 4, "net": 550.25, "open": 3}
    assert sent[1].body.splitlines()[:3] == sent[0].body.splitlines()

    conn.execute("DELETE FROM learning_ledger")
    await _job(conn, sent).run(FRI)
    assert sent[2].body.splitlines()[3] == "Paper, all epochs, simulated fills: 0 closed, 3 open."


async def test_a_failing_paper_query_drops_the_paper_line_not_the_summary(conn, monkeypatch) -> None:
    _seed(conn)
    monkeypatch.setattr("engine.ops.scorecard._PAPER_OPEN_SQL", "SELECT broken")
    sent: list = []
    await _job(conn, sent).run(FRI)
    (msg,) = sent
    assert len(msg.body.splitlines()) == 3 and "paper" not in msg.data
    assert "hi52 n=2" in msg.body


async def test_no_recs_still_sends_with_zeros(conn) -> None:
    sent: list = []
    await _job(conn, sent).run(FRI)
    assert sent[0].body.splitlines() == [
        "Delivered 0, taken 0, skipped 0, expired without a reason 0.",
        "No skipped rec has closed yet.",
        "Closed recs to date, hindsight, mean per rec: none yet.",
    ]


@pytest.mark.parametrize(("status", "sends"), [(None, False), ("failed", False), ("success", True), ("skipped", True)])
async def test_unfinished_only_while_rec_outcomes_is_unrecorded_or_failed(conn, cal, clk, status, sends) -> None:
    runner = CatchUpRunner(conn, clk, cal)
    if status:
        runner.record_run(JOB_REC_OUTCOMES, FRI, status=status)
    sent: list = []
    result = await _job(conn, sent, was_run=runner.was_run).run(FRI)
    assert (result.unfinished, len(sent)) == (not sends, int(sends))


def _fire_day(cal):
    registry = opsmain.build_job_registry(
        load_settings(), {**_all_noop_fns(), JOB_WEEKLY_SUMMARY: _all_noop_fns()[opsmain.JOB_UNIVERSE]},
        calendar=cal,
    )
    return next(s for s in registry.specs() if s.job_id == JOB_WEEKLY_SUMMARY).fire_day


@pytest.mark.parametrize(
    ("d", "fires"),
    [
        (date(2026, 10, 9), True),    # Friday
        (date(2026, 10, 7), False),   # Wednesday
        (date(2026, 10, 1), True),    # Thursday before the Friday 10-02 holiday
        (date(2026, 10, 2), False),   # that holiday
        (date(2026, 10, 10), False),  # Saturday
        (date(2026, 11, 6), True),    # Friday before the muhurat Sunday
        (date(2026, 11, 8), False),   # muhurat Sunday
    ],
)
def test_fires_on_the_weeks_last_regular_session_only(cal, d, fires) -> None:
    assert _fire_day(cal)(d) is fires


async def test_a_missed_friday_is_replayed_on_monday(conn, cal, clk, now) -> None:
    sent: list = []
    registry = opsmain.build_job_registry(
        load_settings(), {**_all_noop_fns(), JOB_WEEKLY_SUMMARY: _job(conn, sent).run}, calendar=cal,
    ).select(lambda s: s.job_id == JOB_WEEKLY_SUMMARY)
    now.at = datetime(2026, 10, 12, 10, 0, tzinfo=IST)
    runner = CatchUpRunner(conn, clk, cal, registry, deferred=opsmain.POST_ARM_JOB_IDS)
    runner.record_run(JOB_WEEKLY_SUMMARY, date(2026, 10, 1))
    await runner.catch_up(scope=CatchUpScope.DEFERRED)
    assert [m.dedupe_key for m in sent] == ["weekly_summary:2026-10-09"]
