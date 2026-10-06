"""Plan Q1.6: the D2 time exit (``RecommendationPipeline.time_exit_check``) and its job wiring."""

from __future__ import annotations

import json
import logging
from datetime import date, datetime
from decimal import Decimal

import pytest
import yaml
from ulid import ULID

from engine.core.calendar import NSECalendar
from engine.core.clock import IST, Clock
from engine.core.config import load_settings
from engine.ops import main as opsmain
from engine.ops.holds import build_hold_fn
from engine.ops.jobs import CatchUpRunner, CatchUpScope
from engine.ops.pipeline import RecommendationBook
from engine.risk.limits import LimitTable
from engine.strategy.cost_model import CostModel
from tests.unit.test_ops_main_wiring import _all_noop_fns
from tests.unit.test_reco_pipeline import (
    CALENDAR_DIR,
    LIMITS_YAML,
    NOW,
    SYMBOL,
    FakeHarness,
    StubGate,
    StubLimits,
    Ticker,
    log_events,
    make_pipeline,
    make_rec,
    passing_ctx,
    verdict_of,
)

_LEGACY = object()


class _ApproveGate(StubGate):
    def evaluate(self, action, ctx):
        return super().evaluate(action, ctx).model_copy(update={"verdict_id": str(ULID())})


@pytest.fixture
def ticker() -> Ticker:
    return Ticker(NOW)


@pytest.fixture
def pclock(ticker: Ticker) -> Clock:
    return Clock(time_source=ticker)


@pytest.fixture
def calendar(pclock: Clock, conn) -> NSECalendar:
    return NSECalendar(CALENDAR_DIR, pclock, sqlite_conn=conn)


@pytest.fixture(scope="module")
def limit_table() -> LimitTable:
    return LimitTable.model_validate(yaml.safe_load(LIMITS_YAML.read_text(encoding="utf-8")))


@pytest.fixture(scope="module")
def cost_model() -> CostModel:
    return CostModel.from_config()


@pytest.fixture
def book(conn, pclock: Clock, cost_model: CostModel) -> RecommendationBook:
    return RecommendationBook(conn, pclock, cost_model)


@pytest.fixture
def pipeline(conn, pclock, calendar, book, limit_table, cost_model):
    return make_pipeline(
        conn=conn, clock=pclock, calendar=calendar, book=book, harness=FakeHarness(),
        gate=_ApproveGate(verdict_of("approve", cost_model)), ctx=passing_ctx(),
        limits=StubLimits(limit_table), hold_fn=build_hold_fn(load_settings(), StubLimits(limit_table)),
    )[0]


def _at(d: date, hh: int = 10, mm: int = 5) -> datetime:
    return datetime(d.year, d.month, d.day, hh, mm, tzinfo=IST)


async def _taken(conn, ticker, book, cost_model, *, delivered: datetime, taken: datetime | None = None,
                 strategy: str = "hi52", style: str = "swing", exit_session=_LEGACY) -> None:
    """An entry rec delivered at ``delivered`` and ``/taken`` at ``taken``; ``_LEGACY`` stores a
    pre-Q1.1 payload with no exit fields."""
    ticker.at = delivered
    stamp = {} if exit_session is _LEGACY else {"exit_session": exit_session}
    rec = make_rec(cost_model, created_at=delivered, style=style, product="CNC", targets=[],
                   strategy_id=strategy, **stamp)
    book.deliver(rec, ledger_fields={"strategy_id": strategy})
    if exit_session is _LEGACY:
        payload = json.loads(conn.execute(
            "SELECT payload FROM recommendations WHERE rec_id=?", (rec.rec_id,)).fetchone()[0])
        for key in ("exit_session", "hold_sessions", "strategy_id"):
            payload.pop(key, None)
        conn.execute("UPDATE recommendations SET payload=? WHERE rec_id=?",
                     (json.dumps(payload), rec.rec_id))
    ticker.at = taken or delivered
    await book.take(rec.rec_id, 10, Decimal("100"))


def _checklists(conn) -> list[str]:
    rows = conn.execute("SELECT payload FROM recommendations ORDER BY delivered_at").fetchall()
    return [line for r in rows if json.loads(r[0])["kind"] == "exit"
            for line in json.loads(r[0])["manual_checklist"]]


@pytest.mark.parametrize(
    ("delivered", "taken", "strategy", "style", "stamped", "due"),
    [
        # Muhurat 11-08 and the 11-10 / 11-24 holidays are not counted.
        (_at(date(2026, 10, 27)), None, "hi52", "swing", _LEGACY, date(2026, 11, 25)),
        # Taken at 23:26 the next day: still counted from delivery (from opened_at: 07-17).
        (_at(date(2026, 6, 17)), _at(date(2026, 6, 18), 23, 26), "ins", "swing", _LEGACY,
         date(2026, 7, 15)),
        (_at(date(2026, 6, 17)), None, "hi52", "swing", date(2026, 7, 20), date(2026, 7, 20)),
        (_at(date(2026, 3, 2)), None, "trend", "position", _LEGACY, date(2026, 8, 26)),
    ],
    ids=["holidays_and_muhurat", "late_taken", "payload_exit_session", "position_style"],
)
async def test_the_exit_fires_on_the_exit_session_not_before(
    conn, ticker, calendar, book, cost_model, pipeline, delivered, taken, strategy, style, stamped, due
):
    await _taken(conn, ticker, book, cost_model, delivered=delivered, taken=taken,
                 strategy=strategy, style=style, exit_session=stamped)
    eve = calendar.previous_trading_day(due)
    ticker.at = _at(eve)
    assert await pipeline.time_exit_check(eve) == 0
    ticker.at = _at(due)
    assert await pipeline.time_exit_check(due) == 1
    assert _checklists(conn) == [f"{SYMBOL} x10 — sell before the close on {due.isoformat()}"]


async def test_a_pending_exit_session_is_recomputed_each_run_and_never_fires(
    conn, ticker, book, cost_model, pipeline, caplog
):
    await _taken(conn, ticker, book, cost_model, delivered=_at(date(2026, 12, 10)), exit_session=None)
    end = date(2026, 12, 31)
    ticker.at = _at(end)
    with caplog.at_level(logging.INFO, logger="engine.ops.holds"):
        assert await pipeline.time_exit_check(end) == 0
        assert await pipeline.time_exit_check(end) == 0
    assert len(log_events(caplog, "exit_session_pending")) == 2


async def test_the_exit_is_reissued_each_run_with_dated_wording(
    conn, ticker, book, cost_model, pipeline
):
    """Catch-up after a 14:00 and a 16:00 boot on the exit session, then the next session's pre-open
    catch-up and its 09:30 fire."""
    due, nxt = date(2026, 6, 17), date(2026, 6, 18)
    await _taken(conn, ticker, book, cost_model, delivered=_at(date(2026, 5, 19)), exit_session=due)
    for at in (_at(due, 14, 0), _at(due, 16, 0), _at(nxt, 8, 5), _at(nxt, 9, 30)):
        ticker.at = at
        assert await pipeline.time_exit_check(at.date()) == 1
    head = f"{SYMBOL} x10 — "
    assert _checklists(conn) == [
        head + "sell before the close on 2026-06-17",
        head + "overdue since 2026-06-17: sell at the next open",
        head + "overdue since 2026-06-17: sell at the next open",
        head + "overdue since 2026-06-17: sell now",
    ]


# --------------------------------------------------------------------------- job wiring
def _registry(calendar: NSECalendar, ran: list[str]):
    def recorder(job_id: str):
        async def run() -> None:
            ran.append(job_id)
        return run

    fns = {**_all_noop_fns(), **{j: recorder(j) for j in (opsmain.JOB_TIME_EXIT_CHECK, opsmain.JOB_SECTOR_MAP)}}
    return opsmain.build_job_registry(load_settings(), fns, calendar=calendar)


def test_the_time_exit_job_needs_the_calendar() -> None:
    fns = {**_all_noop_fns(), opsmain.JOB_TIME_EXIT_CHECK: _all_noop_fns()[opsmain.JOB_UNIVERSE]}
    with pytest.raises(ValueError, match="calendar"):
        opsmain.build_job_registry(load_settings(), fns)


@pytest.mark.parametrize(
    ("job_id", "at", "runs"),
    [
        (opsmain.JOB_TIME_EXIT_CHECK, _at(date(2026, 6, 17), 9, 30), True),
        (opsmain.JOB_TIME_EXIT_CHECK, _at(date(2026, 11, 8), 9, 30), False),   # muhurat Sunday
        (opsmain.JOB_TIME_EXIT_CHECK, _at(date(2026, 6, 26), 9, 30), False),   # Muharram
        (opsmain.JOB_SECTOR_MAP, _at(date(2026, 11, 8), 8, 30), True),
    ],
    ids=["regular_session", "muhurat_sunday", "holiday", "sector_map_sunday"],
)
async def test_the_scheduled_runner_honours_fire_day(conn, ticker, pclock, calendar, job_id, at, runs):
    ran: list[str] = []
    spec = next(s for s in _registry(calendar, ran).specs() if s.job_id == job_id)
    ticker.at = at
    catch_up = CatchUpRunner(conn, pclock, calendar)
    await opsmain._scheduled_runner(spec, catch_up, pclock)()
    assert ran == ([job_id] if runs else [])
    assert catch_up.was_run(job_id, at.date()) is runs


@pytest.mark.parametrize(
    ("boot", "last_ok", "runs"),
    [
        (_at(date(2026, 6, 17), 14, 0), date(2026, 6, 16), True),
        (_at(date(2026, 6, 17), 16, 0), date(2026, 6, 16), True),
        (_at(date(2026, 11, 8), 14, 0), date(2026, 11, 6), False),             # muhurat Sunday
    ],
    ids=["boot_1400", "boot_1600", "muhurat_sunday"],
)
async def test_the_catch_up_replays_a_missed_time_exit(
    conn, ticker, pclock, calendar, boot, last_ok, runs
):
    ran: list[str] = []
    registry = _registry(calendar, ran).select(lambda s: s.job_id == opsmain.JOB_TIME_EXIT_CHECK)
    ticker.at = boot
    catch_up = CatchUpRunner(conn, pclock, calendar, registry, deferred=opsmain.POST_ARM_JOB_IDS)
    catch_up.record_run(opsmain.JOB_TIME_EXIT_CHECK, last_ok)
    assert (await catch_up.catch_up(scope=CatchUpScope.LOAD_BEARING)).jobs_caught_up == []
    await catch_up.catch_up(scope=CatchUpScope.DEFERRED)
    assert ran == ([opsmain.JOB_TIME_EXIT_CHECK] if runs else [])
