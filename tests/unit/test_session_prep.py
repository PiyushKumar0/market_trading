"""Paper session prep (plan Q4.7): catch-up, the corporate-action check, pending exits, the deadline."""

from __future__ import annotations

import asyncio
import contextlib
import datetime as dt
from collections.abc import AsyncIterator, Callable
from decimal import Decimal
from types import SimpleNamespace

import pytest

from engine.core.calendar import NSECalendar
from engine.core.clock import Clock
from engine.core.config import PaperSettings, config_dir
from engine.core.contracts import PAPER_ORDER_UPDATE_TOPIC
from engine.core.types import Bar
from engine.oms import prep as prep_mod
from engine.oms.exits import ExitManager
from engine.oms.manager import OrderManager
from engine.oms.positions import PositionBook
from engine.oms.prep import PrepIncomplete, SessionPrep, Walk
from engine.oms.protection import ProtectionManager
from engine.oms.reconcile import Reconciler
from engine.oms.state import CloseReason
from engine.ops.paper_runtime import backfill_bars_fn, exit_sim_replay
from engine.paper.broker import PaperBroker
from engine.paper.fill_model import FillModelConfig
from tests.unit.test_exit_manager import weekdays
from tests.unit.test_protection_manager import (
    NEXT,
    SYM,
    TICK,
    Now,
    at,
    orders,
    position,
    round_tick,
    seed,
    session,
    tick,
)

R = CloseReason
PREV = dt.date(2026, 6, 16)


def bar(minute: dt.datetime, *, open_: str = "100", high: str = "100.50", low: str = "99.50", close: str = "100",
        symbol: str = SYM) -> Bar:
    return Bar(symbol=symbol, ts_minute=minute, open=Decimal(open_), high=Decimal(high), low=Decimal(low),
               close=Decimal(close), volume=100)


def flat(start: dt.datetime, minutes: int) -> list[Bar]:
    return [bar(start + dt.timedelta(minutes=m)) for m in range(minutes)]


class Stack:
    """OrderManager -> ProtectionManager (PositionBook first) -> ExitManager, with SessionPrep and the
    Reconciler, over one PaperBroker; ``bars``/``corp``/``marks``/``pre_ex`` feed the injected fns."""

    def __init__(self, conn, bus, *, broker: PaperBroker | None = None, now: Now | None = None) -> None:
        self.conn, self.bus = conn, bus
        self.now = now or Now()
        clock = Clock(time_source=self.now)
        self.broker = broker or PaperBroker(clock, bus.publish, FillModelConfig(), lambda _: TICK, 7,
                                            rejection_rate=0.0, topic=PAPER_ORDER_UPDATE_TOPIC)
        self.bars: dict[str, list[Bar] | None] = {}
        self.fetches: list[tuple[list[str], dt.datetime, dt.datetime]] = []
        self.corp: list[dict] = []
        self.marks: dict[str, Decimal] = {}
        self.pre_ex: dict[str, Decimal] = {}
        self.alerts: list[tuple[str, str]] = []
        self.mgr = OrderManager(
            conn, clock, self.broker, order_guard=lambda _: None,
            sell_check=lambda pid, qty: self.book.check_sell(pid, qty),
            on_fill=lambda *a: self.pm.on_fill(*a), on_transition=lambda *a: self.pm.on_transition(*a),
        )
        calendar = {"session_of": lambda ts: ts.date(), "add_sessions": weekdays}
        self.book = PositionBook(conn, clock, self.broker, orders=self.mgr, hold_fn=lambda *_: 5,
                                 round_trip_fn=lambda notional, _p: notional * Decimal("0.003"), **calendar)
        self.em = ExitManager(conn, clock, self.broker, orders=self.mgr, book=self.book, session_fn=session,
                              hold_fn=lambda *_: 5, corp_actions_fn=self.corp_actions, pre_ex_mark_fn=self.pre_ex_mark,
                              settings=PaperSettings(), **calendar)
        self.pm = ProtectionManager(
            conn, clock, self.broker, orders=self.mgr, book=self.book, exit_fn=self.em.exit_position,
            notify=self.notify, session_fn=session, ltp_fn=self.marks.get, round_tick_fn=round_tick,
            gtt_limit_offset_pct=Decimal("1.0"), settings=PaperSettings(),
        )
        self.prep = SessionPrep(conn, book=self.book, exit_fn=self.em.exit_position, bars_fn=self.bars_fn,
                                replay_fn=exit_sim_replay, corp_actions_fn=self.corp_actions,
                                pre_ex_mark_fn=self.pre_ex_mark, mark_fn=self.marks.get)
        self.reconciler = Reconciler(conn, clock, self.broker, orders=self.mgr, protection=self.pm)
        self.mgr.start(bus)

    async def bars_fn(self, symbols, frm: dt.datetime, to: dt.datetime) -> dict[str, list[Bar] | None]:
        self.fetches.append((list(symbols), frm, to))
        return {s: self.bars[s] for s in symbols if s in self.bars}

    async def corp_actions(self, ex_from: dt.date, ex_to: dt.date | None) -> list[dict]:
        return [r for r in self.corp if ex_from <= r["ex_date"] and (ex_to is None or r["ex_date"] <= ex_to)]

    async def pre_ex_mark(self, symbol: str, _ex_date: dt.date) -> Decimal | None:
        return self.pre_ex.get(symbol)

    async def notify(self, key: str, message: str) -> None:
        self.alerts.append((key, message))

    async def settle(self) -> None:
        for _ in range(6):
            await self.bus.drain(1.0)
            await self.mgr.join()

    async def px(self, ts: dt.datetime, ltp: str) -> None:
        self.now.value = ts
        self.marks[SYM] = Decimal(ltp)
        self.broker.on_tick(tick(ts, ltp))
        await self.settle()

    async def enter(self, *, n: int = 1, ts: dt.datetime | None = None) -> str:
        ts = ts or at(10, 0, 1)
        self.now.value = ts - dt.timedelta(seconds=1)
        order = await self.mgr.submit(*seed(self.conn, n=n))
        await self.px(ts, "100.00")
        return order.position_id


@contextlib.asynccontextmanager
async def stacks(conn, bus) -> AsyncIterator[Callable[..., Stack]]:
    """``build(**kwargs)`` makes a Stack; every Stack's OrderManager is stopped on exit. Modules that
    share the Stack wrap this in a local fixture: an imported fixture trips ruff F811."""
    built: list[Stack] = []

    def build(**kwargs) -> Stack:
        built.append(Stack(conn, bus, **kwargs))
        return built[-1]

    try:
        yield build
    finally:
        for s in built:
            await s.mgr.stop()


@pytest.fixture
async def make_stack(conn, bus):
    async with stacks(conn, bus) as build:
        yield build


@pytest.fixture
async def stack(make_stack) -> Stack:
    return make_stack()


def corp(symbol: str, ex: dt.date, kind: str = "bonus") -> dict:
    return {"symbol": symbol, "ex_date": ex, "kind": kind}


def closed(conn, pid: str) -> tuple:
    pos = position(conn, pid)
    return pos["state"], pos["close_reason"], pos["close_basis"]


def gross(conn, pid: str, price: str) -> Decimal:
    return (10 * (Decimal(price) - Decimal(position(conn, pid)["avg_entry"]))).quantize(Decimal("0.01"))


# ------------------------------------------------------------------------------------- catch-up
@pytest.mark.parametrize("pending", [False, True], ids=["open", "pending-exit"])
async def test_a_dip_through_the_stop_during_the_gap_that_recovers_closes_by_bookkeeping(conn, stack, pending) -> None:
    pid = await stack.enter()
    if pending:
        stack.book.mark_pending_exit(pid, R.TIME_STOP, "time")
    stack.bars[SYM] = flat(at(10, 0), 30) + [bar(at(10, 30), open_="96", low="94")] + flat(at(10, 31), 28)

    await stack.prep.run(at(10, 0, 1), at(11, 0))

    pos = position(conn, pid)
    assert closed(conn, pid) == ("CLOSED", "stop", "downtime_replay")
    assert (Decimal(pos["realized_pnl"]), pos["closed_at"]) == (gross(conn, pid, "95"), at(10, 30).isoformat())
    assert stack.fetches == [([SYM], at(10, 0), at(10, 58))]
    assert await stack.broker.gtts() == [] and orders(conn, "exit") == []
    assert (stack.prep.counters.replay_exits, stack.prep.counters.voids) == (1, 0)


async def test_an_ex_date_in_the_window_voids_at_the_pre_ex_mark_without_a_walk_and_no_gtt_fires(conn, stack) -> None:
    pid = await stack.enter()
    stack.corp.append(corp(SYM, NEXT))
    stack.pre_ex[SYM], stack.marks[SYM] = Decimal("100.50"), Decimal("101")

    await stack.prep.run(at(15, 29), at(9, 30, d=NEXT))

    assert closed(conn, pid) == ("CLOSED", "void", "corp_action")
    assert Decimal(position(conn, pid)["realized_pnl"]) == gross(conn, pid, "100.50")
    assert stack.fetches == [] and await stack.broker.gtts() == []
    await stack.px(at(9, 31, d=NEXT), "50.00")
    assert orders(conn, "gtt_leg") == [] and orders(conn, "exit") == []


@pytest.mark.parametrize("fetched", [True, False], ids=["tradeless-minute", "failed-fetch"])
async def test_a_tradeless_minute_is_not_a_gap_and_only_a_failed_fetch_voids(conn, stack, fetched) -> None:
    pid = await stack.enter()
    stack.marks[SYM] = Decimal("98")
    # 10:20 traded nothing; the 10:58 dip lies past the walk's end (prep start - 2 min).
    stack.bars[SYM] = flat(at(10, 0), 20) + flat(at(10, 21), 37) + [bar(at(10, 58), low="90")] if fetched else None

    await stack.prep.run(at(10, 0, 1), at(11, 0))

    if fetched:
        assert closed(conn, pid) == ("OPEN", None, None) and len(await stack.broker.gtts()) == 1
        return
    assert closed(conn, pid) == ("CLOSED", "void", "downtime_uncovered")
    assert Decimal(position(conn, pid)["realized_pnl"]) == gross(conn, pid, "98")


async def test_an_ex_date_today_voids_a_position_held_across_it_but_not_one_opened_today(conn, stack) -> None:
    held = await stack.enter()
    today = await stack.enter(n=2, ts=at(10, 0, 3, d=NEXT))
    stack.corp.append(corp(SYM, NEXT))
    stack.pre_ex[SYM] = Decimal("100.50")
    stack.bars[SYM] = flat(at(10, 0, d=NEXT), 30)

    await stack.prep.run(at(10, 0, 3, d=NEXT), at(10, 30, d=NEXT))

    assert closed(conn, held) == ("CLOSED", "void", "corp_action")
    assert closed(conn, today)[0] == "OPEN"


async def test_a_survivor_still_pending_exit_re_enters_the_exit_routine_late(conn, stack) -> None:
    pid = await stack.enter()
    stack.book.mark_pending_exit(pid, R.TIME_STOP, "time")
    stack.bars[SYM] = flat(at(10, 0), 58)
    stack.now.value = at(11, 0)

    await stack.prep.run(at(10, 0, 1), at(11, 0))

    [exit_order] = orders(conn, "exit")
    assert closed(conn, pid) == ("PENDING_EXIT", "time_stop", "restart_late") and exit_order["qty"] == 10
    assert await stack.broker.gtts() == []


@pytest.mark.parametrize("failure", ["raises", "times-out"])
async def test_a_failed_or_timed_out_step_voids_what_it_did_not_finish(conn, stack, monkeypatch, failure) -> None:
    pid = await stack.enter()
    stack.marks[SYM] = Decimal("99")
    monkeypatch.setattr(prep_mod, "PREP_DEADLINE_S", 0.05)

    async def broken(*_args) -> dict:
        if failure == "raises":
            raise RuntimeError("kite down")
        await asyncio.Event().wait()
        return {}

    stack.prep._bars_fn = broken
    with pytest.raises(PrepIncomplete):
        await stack.prep.run(at(10, 0, 1), at(11, 0))

    assert closed(conn, pid) == ("CLOSED", "void", "prep_incomplete")
    assert Decimal(position(conn, pid)["realized_pnl"]) == gross(conn, pid, "99")
    assert (stack.prep.counters.failed_steps, stack.prep.counters.voids) == (1, 1)


# ------------------------------------------------------------------------------------- the ops-side fns
class FakeBackfill:
    def __init__(self, failed: set[str]) -> None:
        self.failed = failed
        self.calls: list[tuple[list[str], dt.datetime, dt.datetime]] = []

    async def warmup_gap(self, symbols, frm: dt.datetime, to: dt.datetime) -> SimpleNamespace:
        self.calls.append((list(symbols), frm, to))
        return SimpleNamespace(failed=[SimpleNamespace(symbol=s) for s in symbols if s in self.failed])


class FakeStore:
    def __init__(self, bars: list[Bar]) -> None:
        self.bars = bars

    async def aget_bars_1m(self, symbol: str, start: dt.datetime, end: dt.datetime) -> list[Bar]:
        return [b for b in self.bars if b.symbol == symbol and start <= b.ts_minute < end]


async def test_bars_fn_backfills_any_symbol_once_per_session_segment_and_none_marks_a_failed_fetch(clock) -> None:
    calendar = NSECalendar(config_dir() / "calendar", clock, strict=False)
    stored = [bar(at(15, 29)), bar(at(9, 15, d=NEXT), symbol="OUTSIDE")]
    backfill = FakeBackfill(failed={"FAILS"})
    fetch = backfill_bars_fn(backfill, FakeStore(stored), calendar)

    got = await fetch(["OUTSIDE", "FAILS", SYM], at(15, 0, d=PREV), at(9, 30, d=NEXT))

    symbols = ["OUTSIDE", "FAILS", SYM]
    assert backfill.calls == [(symbols, at(15, 0, d=PREV), at(15, 30, d=PREV)), (symbols, at(9, 15), at(15, 30)),
                              (symbols, at(9, 15, d=NEXT), at(9, 30, d=NEXT))]
    assert got == {"OUTSIDE": [stored[1]], "FAILS": None, SYM: [stored[0]]}


async def test_bars_fn_never_asks_for_the_forming_minute_of_a_session_first_tick(clock) -> None:
    backfill = FakeBackfill(failed=set())
    fetch = backfill_bars_fn(backfill, FakeStore([]), NSECalendar(config_dir() / "calendar", clock, strict=False))
    await fetch([SYM], at(15, 29), at(9, 15, 2, d=NEXT) - prep_mod.WALK_LAG)
    assert backfill.calls == [([SYM], at(15, 29), at(15, 30))]


@pytest.mark.parametrize("bars, expected", [
    ([bar(at(10, 5), open_="96", low="94")], (Decimal("95"), R.STOP, at(10, 5))),
    ([bar(at(10, 5), open_="92", low="91")], (Decimal("92"), R.STOP, at(10, 5))),
    ([bar(at(10, 5)), bar(at(10, 6), high="111")], (Decimal("110"), R.TARGET, at(10, 6))),
    ([bar(at(10, 5)), bar(at(15, 28, d=NEXT), close="101"), bar(at(15, 29, d=NEXT), close="102")],
     (Decimal("102"), R.TIME_STOP, at(15, 30, d=NEXT))),
    ([bar(at(10, 4), low="90"), bar(at(10, 5))], None),
    ([bar(at(10, 5))], None),
], ids=["stop", "gap-through", "target", "time", "before-observed", "open"])
def test_exit_sim_replay_walks_from_the_observed_minute_and_stamps_the_exit_bar(bars, expected) -> None:
    walk = Walk(avg_entry=Decimal("100"), observed=at(10, 5, 30), stop=Decimal("95"), target=Decimal("110"),
                exit_session=NEXT, end=at(9, 20, d=NEXT + dt.timedelta(days=1)))
    replayed = exit_sim_replay(bars, walk)
    assert (None if replayed is None else (replayed.price, replayed.reason, replayed.at)) == expected
