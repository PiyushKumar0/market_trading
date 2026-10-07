"""Paper ExitManager (plan Q4.6): the exit routine, the D2 time exit, corporate actions, flatten, scope."""

from __future__ import annotations

import datetime as dt
import logging
from decimal import Decimal

import pytest

from engine.core.clock import Clock
from engine.core.config import PaperSettings
from engine.core.contracts import PAPER_ORDER_UPDATE_TOPIC
from engine.core.types import Bar
from engine.marketdata.store import MarketStore
from engine.oms.exits import ExitManager
from engine.oms.manager import OrderManager
from engine.oms.positions import PositionBook
from engine.oms.protection import ProtectionManager
from engine.oms.state import CloseReason
from engine.ops.paper_runtime import store_corp_actions_fn, store_pre_ex_mark_fn
from engine.paper.broker import PaperBroker, PaperOrderError
from engine.paper.fill_model import FillModelConfig
from tests.unit.test_protection_manager import (
    DAY,
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
    volume,
)

R = CloseReason
PREV = DAY - dt.timedelta(days=1)
FRI, MON = dt.date(2026, 6, 19), dt.date(2026, 6, 22)
YEAR_END = dt.date(2026, 12, 31)


def weekdays(d: dt.date, n: int, horizon: dt.date = YEAR_END) -> dt.date:
    """``add_sessions`` over a weekday-only calendar that ends at ``horizon``."""
    while d.weekday() >= 5:
        d += dt.timedelta(days=1)
    for _ in range(n):
        d += dt.timedelta(days=1)
        while d.weekday() >= 5:
            d += dt.timedelta(days=1)
    if d > horizon:
        raise ValueError(f"{d} is beyond the loaded calendars")
    return d


class Rig:
    """OrderManager -> ProtectionManager (PositionBook first) -> ExitManager over one PaperBroker."""

    def __init__(self, conn, bus, *, market: MarketStore | None = None) -> None:
        self.conn = conn
        self.now = Now()
        clock = Clock(time_source=self.now)
        self.hold: int | None = 1
        self.horizon = YEAR_END
        self.corp: list[dict] = []
        self.pre_ex: dict[str, Decimal] = {}
        self.ltp: dict[str, Decimal] = {}
        self.alerts: list[tuple[str, str]] = []
        self.broker = PaperBroker(clock, bus.publish, FillModelConfig(), lambda _: TICK, 7, rejection_rate=0.0,
                                  topic=PAPER_ORDER_UPDATE_TOPIC)
        self.mgr = OrderManager(
            conn, clock, self.broker, order_guard=lambda _: None,
            sell_check=lambda pid, qty: self.book.check_sell(pid, qty),
            on_fill=lambda *a: self.pm.on_fill(*a), on_transition=lambda *a: self.pm.on_transition(*a),
        )
        calendar = {"session_of": lambda ts: ts.date(), "add_sessions": self.add_sessions}
        self.book = PositionBook(conn, clock, self.broker, orders=self.mgr, hold_fn=self.hold_fn,
                                 round_trip_fn=lambda notional, _p: notional * Decimal("0.003"), **calendar)
        self.em = ExitManager(
            conn, clock, self.broker, orders=self.mgr, book=self.book, session_fn=session, hold_fn=self.hold_fn,
            corp_actions_fn=store_corp_actions_fn(market) if market else self.corp_actions,
            pre_ex_mark_fn=store_pre_ex_mark_fn(market) if market else self.pre_ex_mark,
            settings=PaperSettings(), **calendar,
        )
        self.pm = ProtectionManager(
            conn, clock, self.broker, orders=self.mgr, book=self.book, exit_fn=self.em.exit_position,
            notify=self.notify, session_fn=session, ltp_fn=self.ltp.get, round_tick_fn=round_tick,
            gtt_limit_offset_pct=Decimal("1.0"), settings=PaperSettings(),
        )
        self.mgr.start(bus)
        self.bus = bus

    def add_sessions(self, d: dt.date, n: int) -> dt.date:
        return weekdays(d, n, self.horizon)

    def hold_fn(self, _strategy: str, _style: str) -> int | None:
        return self.hold

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

    async def px(self, ts: dt.datetime, ltp: str, cum: int = 0) -> None:
        self.now.value = ts
        self.ltp[SYM] = Decimal(ltp)
        self.broker.on_tick(tick(ts, ltp, cum))
        await self.settle()

    async def run(self, ts: dt.datetime) -> None:
        """One ``paper_tick``'s exit steps at ``ts``."""
        self.now.value = ts
        await self.pm.tick()
        await self.em.tick()
        await self.settle()

    async def enter(self, *, n: int = 1, ts: dt.datetime | None = None) -> str:
        ts = ts or at(10, 0, 1)
        self.now.value = ts - dt.timedelta(seconds=1)
        order = await self.mgr.submit(*seed(self.conn, n=n))
        await self.px(ts, "100.00")
        return order.position_id

    async def gtts(self) -> list[dict]:
        return await self.broker.gtts()


@pytest.fixture
async def make_rig(conn, bus):
    rigs: list[Rig] = []

    def build(**kwargs) -> Rig:
        rigs.append(Rig(conn, bus, **kwargs))
        return rigs[-1]

    yield build
    for r in rigs:
        await r.mgr.stop()


@pytest.fixture
async def rig(make_rig) -> Rig:
    return make_rig()


def corp(symbol: str, ex: dt.date, kind: str = "bonus") -> dict:
    return {"symbol": symbol, "ex_date": ex, "kind": kind, "ratio": None, "amount": None, "source": "test",
            "recorded_at": at(9, 0, d=PREV)}


@pytest.fixture(scope="module")
def market(tmp_path_factory):
    root = tmp_path_factory.mktemp("market")
    store = MarketStore(root / "market.duckdb", root / "parquet", Clock(time_source=lambda: at(10, 0))).open()
    store.upsert_corp_actions([
        corp("ONENTRY", DAY), corp("NEXTDAY", NEXT, "split"), corp("ATEXIT", FRI), corp("AFTEREXIT", MON),
        corp("DIVIDEND", NEXT, "dividend"), corp("FARAWAY", dt.date(2026, 9, 1)),
    ])
    px = Decimal("101.25")
    store.insert_bars_1m([Bar(symbol=SYM, ts_minute=ts, open=px, high=px, low=px, close=c, volume=100)
                          for ts, c in ((at(15, 28), Decimal("101")), (at(15, 29), px),
                                        (at(9, 15, d=NEXT), Decimal("50")))])
    yield store
    store.close()


def real_rows(conn) -> list[tuple]:
    return [tuple(r) for q in ("positions", "gtts", "orders")
            for r in conn.execute(f"SELECT * FROM {q} WHERE COALESCE(is_paper, 0) = 0")]


def ledger(conn, pid: str):
    return conn.execute("SELECT * FROM learning_ledger WHERE position_id = ?", (pid,)).fetchone()


# ------------------------------------------------------------------------------------- construction
def test_ctor_takes_only_a_paper_broker(conn, clock) -> None:
    with pytest.raises(TypeError, match="PaperBroker only"):
        ExitManager(conn, clock, object(), orders=None, book=None, session_fn=session, session_of=None,
                    add_sessions=weekdays, hold_fn=None, corp_actions_fn=None, pre_ex_mark_fn=None,
                    settings=PaperSettings())


# ------------------------------------------------------------------------------------- time exits
@pytest.mark.parametrize("hold, exit_day, before", [
    (1, DAY, [at(15, 19, 59)]),
    (2, NEXT, [at(15, 19, 59), at(15, 25), at(9, 15, d=NEXT), at(15, 19, 59, d=NEXT)]),
])
async def test_the_time_exit_sells_the_open_qty_at_close_less_10_of_the_exit_session(
    conn, rig, hold, exit_day, before
) -> None:
    rig.hold = hold
    pid = await rig.enter()
    assert position(conn, pid)["exit_session"] == exit_day.isoformat()

    for ts in before:
        await rig.run(ts)
    assert orders(conn, "exit") == []
    await rig.run(at(15, 20, d=exit_day))

    [exit_order] = orders(conn, "exit")
    pos = position(conn, pid)
    assert (exit_order["qty"], exit_order["price"]) == (10, None)
    assert (pos["state"], pos["close_reason"], pos["close_basis"]) == ("PENDING_EXIT", "time_stop", "time")
    assert await rig.gtts() == [] and conn.execute("SELECT state FROM gtts").fetchone()["state"] == "deleted"
    await rig.px(at(15, 20, 2, d=exit_day), "100.00")
    assert (position(conn, pid)["state"], position(conn, pid)["close_reason"]) == ("CLOSED", "time_stop")


@pytest.mark.parametrize("case", ["leg-working", "leg-adopted", "leg-filled-unapplied"])
async def test_a_time_exit_racing_a_triggered_gtt_sends_no_second_exit(conn, rig, case) -> None:
    pid = await rig.enter()
    for ts in [at(15, 20)] + ([at(15, 20, 2)] if case == "leg-filled-unapplied" else []):
        rig.now.value = ts
        rig.broker.on_tick(tick(ts, "94.50"))
    if case == "leg-adopted":
        await rig.settle()
        await rig.em.exit_position(pid, R.TIME_STOP, "time")
    else:
        await rig.em.tick()

    pos = position(conn, pid)
    assert orders(conn, "exit") == []
    assert pos["state"] == ("OPEN" if case == "leg-filled-unapplied" else "PENDING_EXIT")
    await rig.settle()
    await rig.px(at(15, 21), "94.50")
    await rig.run(at(15, 21, 30))
    pos = position(conn, pid)
    assert (pos["state"], pos["close_reason"], orders(conn, "exit")) == ("CLOSED", "stop", [])


async def test_no_exit_is_sent_while_the_gtt_deletion_is_unverified(conn, rig, monkeypatch) -> None:
    pid = await rig.enter()
    rig.now.value = at(11, 0)

    async def refuse(gtt_id: int) -> None:
        raise PaperOrderError(f"gtt {gtt_id} busy")

    monkeypatch.setattr(rig.broker, "delete_gtt", refuse)
    with pytest.raises(RuntimeError, match="still armed"):
        await rig.em.exit_position(pid, R.TIME_STOP, "time")
    [g] = await rig.gtts()
    assert (g["status"], orders(conn, "exit"), position(conn, pid)["state"]) == ("active", [], "PENDING_EXIT")


async def test_a_time_exit_while_a_partial_entry_rests_cancels_it_and_sells_the_filled_qty(conn, rig) -> None:
    await rig.mgr.submit(*seed(conn, qty=20, limit="96"))
    volume(rig, 10)
    await rig.px(at(10, 1), "95.90", cum=5000)
    [entry] = orders(conn, "entry")
    pid = entry["position_id"]
    assert (entry["state"], entry["filled_qty"]) == ("PARTIALLY_FILLED", 10)

    await rig.run(at(15, 20))
    [entry] = orders(conn, "entry")
    [exit_order] = orders(conn, "exit")
    assert (entry["state"], entry["filled_qty"], exit_order["qty"]) == ("CANCELLED", 10, 10)
    assert rig.mgr.cancel_intended(entry["order_id"])
    await rig.px(at(15, 21), "96.50", cum=5000)
    pos = position(conn, pid)
    assert (pos["state"], pos["qty"], pos["close_reason"]) == ("CLOSED", 10, "time_stop")


async def test_an_exit_handed_to_a_working_leg_cancels_the_resting_entry(conn, rig) -> None:
    await rig.mgr.submit(*seed(conn, qty=20, limit="96"))
    volume(rig, 10)
    await rig.px(at(10, 1), "95.90", cum=5000)
    [entry] = orders(conn, "entry")
    rig.now.value = at(10, 1, 30)
    rig.broker.on_tick(tick(at(10, 1, 30), "94.00", cum=5000))

    await rig.em.exit_position(entry["position_id"], R.TIME_STOP, "time")
    assert position(conn, entry["position_id"])["state"] == "PENDING_EXIT"
    assert rig.mgr.cancel_intended(entry["order_id"]) and orders(conn, "exit") == []


async def test_a_missed_time_exit_goes_at_the_first_in_session_moment_noted_overdue(conn, rig) -> None:
    pid = await rig.enter()
    for ts in (at(15, 31), at(9, 14, 59, NEXT)):
        await rig.run(ts)
    assert orders(conn, "exit") == [] and rig.em.counters.overdue == 0

    for ts in (at(9, 15, 0, NEXT), at(9, 15, 30, NEXT)):
        await rig.run(ts)
    [exit_order] = orders(conn, "exit")
    pos = position(conn, pid)
    assert (pos["state"], pos["close_reason"], pos["close_basis"]) == ("PENDING_EXIT", "time_stop", "time_overdue")
    assert rig.em.counters.overdue == 1


async def test_a_calendar_raise_is_logged_and_counted_and_the_pending_exit_session_recomputed(conn, rig) -> None:
    rig.hold, rig.horizon = 20, DAY + dt.timedelta(days=5)
    pid = await rig.enter()
    assert position(conn, pid)["exit_session"] is None

    for s in (0, 30):
        await rig.run(at(10, 5, s))
    assert (rig.em.counters.calendar_raises, position(conn, pid)["exit_session"]) == (1, None)
    rig.horizon = YEAR_END
    await rig.run(at(10, 6))
    assert position(conn, pid)["exit_session"] == weekdays(DAY, 19).isoformat()
    assert rig.em.counters.calendar_raises == 1 and orders(conn, "exit") == [] and rig.alerts == []


# ------------------------------------------------------------------------------------- corporate actions
@pytest.mark.parametrize("symbol, pending, refused", [
    ("ONENTRY", False, False),
    ("NEXTDAY", False, True),
    ("ATEXIT", False, True),
    ("AFTEREXIT", False, False),
    ("DIVIDEND", False, False),
    ("FARAWAY", False, False),
    ("FARAWAY", True, True),
    ("ONENTRY", True, False),
])
async def test_entry_refusal_reads_real_corp_actions_rows_bounded_below_by_the_entry_session(
    make_rig, market, symbol, pending, refused
) -> None:
    rig = make_rig(market=market)
    rig.hold = 3
    if pending:
        rig.horizon = NEXT
    reason = await rig.em.entry_refusal(symbol, "hi52", "swing", at(10, 0))
    assert (reason is not None) is refused, reason
    assert rig.em.counters.calendar_raises == int(pending)


async def test_the_pre_ex_mark_is_the_last_stored_minute_before_the_ex_date(market) -> None:
    mark = store_pre_ex_mark_fn(market)
    assert (await mark(SYM, NEXT), await mark(SYM, DAY), await mark("NEXTDAY", NEXT)) == (Decimal("101.25"), None, None)


@pytest.mark.parametrize("entered", [DAY, NEXT], ids=["held-across", "entered-on-the-ex-date"])
async def test_a_late_ex_date_today_voids_a_position_held_across_it_at_the_pre_ex_mark(conn, rig, entered) -> None:
    rig.hold = 5
    pid = await rig.enter(ts=at(9, 30, d=entered))
    rig.pre_ex[SYM] = Decimal("101")
    await rig.run(at(10, 0, d=NEXT))
    rig.corp.append(corp(SYM, NEXT))
    await rig.run(at(10, 14, 59, d=NEXT))
    assert position(conn, pid)["state"] == "OPEN"

    await rig.run(at(10, 15, d=NEXT))
    pos = position(conn, pid)
    if entered == NEXT:
        assert pos["state"] == "OPEN" and rig.em.counters.voids == 0
        return
    gross = (10 * (Decimal(101) - Decimal(pos["avg_entry"]))).quantize(Decimal("0.01"))
    assert (pos["state"], pos["close_reason"], pos["close_basis"], pos["costs"]) == (
        "CLOSED", "void", "corp_action_late", "0.00")
    assert Decimal(pos["realized_pnl"]) == gross and ledger(conn, pid)["outcome_label"] == "void"
    assert await rig.gtts() == [] and orders(conn, "exit") == []
    assert (rig.em.counters.voids, rig.em.counters.late_corp_actions) == (1, 1)


@pytest.mark.parametrize("ex, voided", [(DAY, False), (NEXT, True)], ids=["on-entry", "inside-hold"])
async def test_an_ex_date_found_sessions_later_voids_a_position_held_across_it(conn, rig, caplog, ex, voided) -> None:
    rig.hold = 5
    pid = await rig.enter()
    conn.execute("INSERT INTO paper_halts (cause, rung, set_at) VALUES ('daily_loss', 'REDUCED', ?)",
                 (at(10, 0, d=NEXT).isoformat(),))
    rig.corp.append(corp(SYM, ex))
    with caplog.at_level(logging.WARNING, logger="engine.oms.exits"):
        await rig.run(at(10, 0, d=FRI))

    assert position(conn, pid)["state"] == ("CLOSED" if voided else "OPEN")
    assert [(r.ex_date, r.halts) for r in caplog.records if r.getMessage() == "paper_corp_action_late"] == (
        [(NEXT.isoformat(), ["daily_loss"])] if voided else [])


async def test_a_close_made_on_a_late_found_ex_date_is_relabelled_void_at_the_pre_ex_mark(conn, rig) -> None:
    rig.hold = 5
    pid = await rig.enter()
    await rig.px(at(9, 20, d=NEXT), "94.50")
    await rig.px(at(9, 20, 2, d=NEXT), "94.50")
    pos = position(conn, pid)
    assert (pos["state"], pos["close_reason"]) == ("CLOSED", "stop") and Decimal(pos["costs"]) > 0

    rig.pre_ex[SYM] = Decimal("100.50")
    rig.corp.append(corp(SYM, NEXT))
    await rig.run(at(10, 0, d=NEXT))
    await rig.run(at(10, 15, d=NEXT))
    pos, row = position(conn, pid), ledger(conn, pid)
    gross = (10 * (Decimal("100.50") - Decimal(pos["avg_entry"]))).quantize(Decimal("0.01"))
    assert (pos["close_reason"], pos["close_basis"], pos["costs"], Decimal(pos["realized_pnl"])) == (
        "void", "corp_action_relabel", "0.00", gross)
    assert (row["close_reason"], row["outcome_label"], Decimal(row["exit_px"]), row["costs"]) == (
        "void", "void", Decimal("100.50"), "0.00")
    assert Decimal(row["gross_pnl"]) == Decimal(row["net_pnl"]) == gross
    assert (rig.em.counters.voids, rig.em.counters.late_corp_actions) == (1, 1)


# ------------------------------------------------------------------------------------- flatten and step 6
async def test_flatten_exits_every_held_position_once_and_leaves_a_working_exit_alone(conn, rig) -> None:
    p1 = await rig.enter(n=1)
    p2 = await rig.enter(n=2, ts=at(10, 0, 3))
    rig.now.value = at(11, 0)
    await rig.em.exit_position(p1, R.TIME_STOP, "time")

    assert [await rig.em.flatten_all(), await rig.em.flatten_all()] == [2, 2]
    assert sorted(o["position_id"] for o in orders(conn, "exit")) == sorted([p1, p2])
    await rig.px(at(11, 0, 2), "100.00")
    assert [(position(conn, p)["state"], position(conn, p)["close_reason"], position(conn, p)["close_basis"])
            for p in (p1, p2)] == [("CLOSED", "time_stop", "time"), ("CLOSED", "risk_flatten", "equity_floor")]


async def test_a_flatten_outside_the_session_goes_at_the_first_in_session_tick(conn, rig) -> None:
    rig.hold = 5
    pid = await rig.enter()
    rig.now.value = at(16, 0)
    await rig.em.flatten_all()
    await rig.run(at(9, 14, 59, d=NEXT))
    assert orders(conn, "exit") == [] and position(conn, pid)["state"] == "OPEN" and len(await rig.gtts()) == 1

    await rig.run(at(9, 15, d=NEXT))
    pos = position(conn, pid)
    assert len(orders(conn, "exit")) == 1 and (pos["close_reason"], pos["close_basis"]) == ("risk_flatten", "equity_floor")


async def test_an_active_gtt_outliving_its_position_is_deleted(conn, rig) -> None:
    pid = await rig.enter()
    conn.execute("UPDATE positions SET state = 'CLOSED' WHERE position_id = ?", (pid,))
    await rig.run(at(16, 0))
    assert await rig.gtts() == [] and conn.execute("SELECT state FROM gtts").fetchone()["state"] == "deleted"


# ------------------------------------------------------------------------------------- scope
async def test_every_selection_is_paper_scope_and_a_same_symbol_real_position_is_untouched(conn, rig) -> None:
    conn.execute("INSERT INTO positions (position_id, symbol, side, product, qty, avg_entry, stop, state, is_paper, "
                 "origin, opened_at, exit_session) VALUES ('REAL', ?, 'BUY', 'CNC', 10, '100', '95', 'OPEN', 0, "
                 "'recommended', ?, ?)", (SYM, at(9, 30, d=PREV).isoformat(), PREV.isoformat()))
    conn.execute("INSERT INTO positions (position_id, symbol, side, product, qty, avg_entry, stop, state, is_paper, "
                 "origin, opened_at, closed_at, close_reason) VALUES ('REAL_CLOSED', ?, 'BUY', 'CNC', 10, '100', '95', "
                 "'CLOSED', 0, 'platform', ?, ?, 'stop')", (SYM, at(9, 30, d=PREV).isoformat(), at(9, 30, d=NEXT).isoformat()))
    conn.execute("INSERT INTO gtts (gtt_id, position_id, state, trigger_low) VALUES (1, 'REAL', 'active', '95')")
    conn.execute("INSERT INTO orders (order_id, broker_order_id, position_id, role, is_paper, state, product, side, "
                 "qty) VALUES ('RO', '250000000000001', 'REAL', 'exit', 0, 'ACKED', 'CNC', 'SELL', 10)")
    before = real_rows(conn)

    timed = await rig.enter(n=1)
    rig.hold = 5
    stopped = await rig.enter(n=2, ts=at(10, 0, 3))
    await rig.run(at(15, 20))
    await rig.px(at(15, 20, 2), "100.00")
    for s in (0, 2):
        await rig.px(at(9, 20, s, d=NEXT), "94.50")
    rig.corp.append(corp(SYM, NEXT))
    await rig.run(at(10, 0, d=NEXT))

    assert await rig.em.flatten_all() == 0
    assert [(position(conn, p)["close_reason"], position(conn, p)["close_basis"]) for p in (timed, stopped)] == [
        ("time_stop", "time"), ("void", "corp_action_relabel")]
    paper_ids = {r["broker_order_id"] for r in conn.execute("SELECT broker_order_id FROM orders WHERE is_paper = 1")}
    assert {o["order_id"] for o in await rig.broker.orders()} <= paper_ids
    assert real_rows(conn) == before
