"""Paper PositionBook (plan Q4.4): open and accrete, exit fills and closes, oversell, bookkeeping and void."""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from decimal import Decimal

import pytest

from engine.core.clock import IST, Clock
from engine.core.contracts import PAPER_ORDER_UPDATE_TOPIC, EnterAction, GateVerdict
from engine.core.enums import Mode, RiskState
from engine.core.types import Bar, Tick
from engine.oms.manager import OrderManager, SubmitRefused
from engine.oms.positions import CloseDeferred, Oversell, PositionBook, PositionNotHeld
from engine.oms.state import CloseReason, OrderRole, OrderState, PlatformOrder
from engine.oms.store import OrderStore
from engine.paper.broker import PaperBroker
from engine.paper.fill_model import FillModelConfig
from engine.risk.exposure import ExposureTracker

SYM = "TCS"
DAY = dt.date(2026, 6, 17)
COST_RATE = Decimal("0.003")            # fees + spread: bar-priced closes
FEE_RATE = Decimal("0.002")             # fees only: a broker fill's price already carries the spread


def at(h: int, m: int, s: int = 0) -> dt.datetime:
    return dt.datetime.combine(DAY, dt.time(h, m, s), tzinfo=IST)


def tick(ts: dt.datetime, ltp: str, cum: int = 0) -> Tick:
    px = Decimal(ltp)
    return Tick(instrument_token=1, tradingsymbol=SYM, ltp=px, volume_traded=cum, exchange_ts=ts,
                bid=px - Decimal("0.05"), ask=px + Decimal("0.05"))


class _Now:
    def __init__(self) -> None:
        self.value = at(10, 0)

    def __call__(self) -> dt.datetime:
        return self.value


@dataclass
class _Rig:
    broker: PaperBroker
    mgr: OrderManager
    book: PositionBook
    now: _Now
    hold: int | None = 20
    calendar_ends: bool = False
    costs: list[tuple[Decimal, str]] = field(default_factory=list)
    fills: list[tuple[OrderRole, int, Decimal]] = field(default_factory=list)


@pytest.fixture
async def rig(conn, bus):
    now = _Now()
    clock = Clock(time_source=now)
    broker = PaperBroker(clock, bus.publish, FillModelConfig(), lambda _: Decimal("0.05"), 7,
                         rejection_rate=0.0, topic=PAPER_ORDER_UPDATE_TOPIC)
    holder: dict[str, _Rig] = {}

    def add_sessions(d: dt.date, n: int) -> dt.date:
        if holder["rig"].calendar_ends:
            raise ValueError("beyond the loaded calendars")
        return d + dt.timedelta(days=n)

    def round_trip(notional: Decimal, product: str) -> Decimal:
        holder["rig"].costs.append((notional, product))
        return notional * COST_RATE

    def fees(notional: Decimal, product: str) -> Decimal:
        holder["rig"].costs.append((notional, product))
        return notional * FEE_RATE

    def on_fill(order, qty, price, when) -> None:
        holder["rig"].fills.append((order.role, qty, price))
        book.on_fill(order, qty, price, when)

    mgr = OrderManager(conn, clock, broker, order_guard=lambda _: None,
                       sell_check=lambda pid, qty: book.check_sell(pid, qty), on_fill=on_fill)
    book = PositionBook(conn, clock, broker, orders=mgr, hold_fn=lambda _s, _style: holder["rig"].hold,
                        session_of=lambda ts: ts.date(), add_sessions=add_sessions, round_trip_fn=round_trip,
                        fees_fn=fees)
    mgr.start(bus)
    holder["rig"] = _Rig(broker, mgr, book, now)
    yield holder["rig"]
    await mgr.stop()


async def settle(bus, rig: _Rig) -> None:
    for _ in range(3):
        await bus.drain(1.0)
        await rig.mgr.join()


def seed(conn, *, n: int = 1, qty: int = 10) -> tuple[EnterAction, GateVerdict]:
    proposal = EnterAction(
        action="enter", proposal_id=f"P{n}", agent_id="swing", thesis="paper position book test thesis",
        confidence=0.6, tradingsymbol=SYM, exchange="NSE", side="BUY", style="swing", entry_type="MARKET",
        stop_price=Decimal("95"), target_price=Decimal("110"), quantity=qty, signal_id="s", strategy_id="hi52",
        features_snapshot_id="f",
    )
    verdict = GateVerdict(
        verdict_id=f"V{n}", proposal_id=proposal.proposal_id, verdict="approve", original_qty=qty,
        approved_qty=qty, checks=[], mode=Mode.RECOMMEND, risk_state=RiskState.NORMAL, degrade_tier="DG0",
        evaluated_at=at(10, 0),
    )
    conn.execute("INSERT INTO proposals (proposal_id, agent_id, action, payload, inputs_digest, created_at) "
                 "VALUES (?, 'swing', 'enter', ?, 'd', ?)",
                 (proposal.proposal_id, proposal.model_dump_json(), at(10, 0).isoformat()))
    conn.execute("INSERT INTO verdicts (verdict_id, proposal_id, verdict, payload, evaluated_at, is_paper) "
                 "VALUES (?, ?, 'approve', ?, ?, 1)",
                 (verdict.verdict_id, proposal.proposal_id, verdict.model_dump_json(), at(10, 0).isoformat()))
    return proposal, verdict


def fill_entry(conn, rig: _Rig, fills=((10, "100"),), *, n: int = 1) -> str:
    """A filled paper entry order, its fills applied straight to the book."""
    total = sum(q for q, _ in fills)
    seed(conn, n=n, qty=total)
    order = PlatformOrder(order_id=f"O{n}", position_id=f"POS{n}", proposal_id=f"P{n}", verdict_id=f"V{n}",
                          role=OrderRole.ENTRY, is_paper=True, state=OrderState.FILLED, product="CNC", side="BUY",
                          qty=total, filled_qty=total)
    OrderStore(conn).insert(order)
    for qty, px in fills:
        rig.book.on_fill(order, qty, Decimal(px), at(10, 1))
    return order.position_id


def sell(rig: _Rig, position_id: str, qty: int, px: str, *, tag: str | None = None, when=None) -> None:
    order = PlatformOrder(order_id="X", position_id=position_id, role=OrderRole.GTT_LEG if tag else OrderRole.EXIT,
                          is_paper=True, state=OrderState.FILLED, product="CNC", side="SELL", qty=qty,
                          filled_qty=qty, raw_broker_payload={"tag": tag} if tag else None)
    rig.book.on_fill(order, qty, Decimal(px), when or at(11, 0))


def position(conn, position_id: str = "POS1"):
    return conn.execute("SELECT * FROM positions WHERE position_id = ?", (position_id,)).fetchone()


def ledger(conn) -> list:
    return conn.execute("SELECT * FROM learning_ledger ORDER BY closed_at").fetchall()


async def place_gtt(conn, rig: _Rig, position_id: str) -> int:
    gtt_id = await rig.broker.place_gtt({
        "tradingsymbol": SYM, "last_price": Decimal(100), "trigger_values": [Decimal(95)],
        "orders": [{"transaction_type": "SELL", "quantity": 10, "product": "CNC", "price": Decimal(94)}],
    })
    for paper, gid in ((1, gtt_id), (0, 1)):
        conn.execute("INSERT INTO gtts (gtt_id, position_id, state, trigger_low, is_paper, symbol, side, product, qty) "
                     "VALUES (?, ?, 'active', '95', ?, ?, 'SELL', 'CNC', 10)", (gid, position_id, paper, SYM))
    return gtt_id


# ------------------------------------------------------------------------------------- construction
def test_ctor_takes_only_a_paper_broker(conn, clock) -> None:
    with pytest.raises(TypeError, match="PaperBroker only"):
        PositionBook(conn, clock, object(), orders=None, hold_fn=lambda *_: 20, session_of=lambda ts: ts.date(),
                     add_sessions=lambda d, n: d, round_trip_fn=lambda *_: Decimal(0))


# ------------------------------------------------------------------------------------- fills
async def test_partial_entry_fills_open_then_accrete_and_exit_fills_close_through_the_order_manager(
    conn, bus, rig
) -> None:
    proposal, verdict = seed(conn, qty=60)
    entry = await rig.mgr.submit(proposal, verdict)
    rig.broker.on_bar(Bar(symbol=SYM, ts_minute=at(10, 0), open=Decimal(100), high=Decimal(100),
                          low=Decimal(100), close=Decimal(100), volume=300))
    rig.broker.on_tick(tick(at(10, 1), "100.00", cum=5000))
    await settle(bus, rig)
    first = dict(position(conn, entry.position_id))
    rig.broker.on_tick(tick(at(10, 2), "100.00", cum=5000))
    await settle(bus, rig)

    (_, q1, p1), (_, q2, p2) = rig.fills
    avg = ((p1 * q1 + p2 * q2) / 60).quantize(Decimal("0.0001"))
    assert (first["qty"], Decimal(first["avg_entry"])) == (30, p1)
    row = position(conn, entry.position_id)
    assert (row["symbol"], row["is_paper"], row["origin"], row["state"], row["protection_state"], row["strategy_id"],
            row["style"], row["product"], row["stop"], row["target"]) == (
        SYM, 1, "platform", "OPEN", "PROTECTION_PENDING", "hi52", "swing", "CNC", "95", "110")
    assert (row["qty"], Decimal(row["avg_entry"]), row["opened_at"], row["exit_session"]) == (
        60, avg, at(10, 1).isoformat(), "2026-07-06")

    rig.book.mark_pending_exit(entry.position_id, CloseReason.TIME_STOP)
    rig.now.value = at(10, 3)
    await rig.mgr.submit_exit(entry.position_id, 60)
    for minute in range(4, 9):
        rig.broker.on_tick(tick(at(10, minute), "101.00", cum=5000))
        await settle(bus, rig)

    sells = [(q, p) for role, q, p in rig.fills if role is OrderRole.EXIT]
    assert len(sells) > 1 and sum(q for q, _ in sells) == 60
    gross = sum((q * (p - avg) for q, p in sells), Decimal(0)).quantize(Decimal("0.01"))
    costs = (avg * 60 * FEE_RATE).quantize(Decimal("0.01"))
    row = position(conn, entry.position_id)
    assert (row["state"], row["qty"], row["close_reason"], row["close_basis"]) == ("CLOSED", 60, "time_stop", "fill")
    assert (Decimal(row["realized_pnl"]), Decimal(row["costs"])) == (gross, costs)
    [entry_ledger] = ledger(conn)
    assert (entry_ledger["is_paper"], entry_ledger["rec_id"], entry_ledger["proposal_id"], entry_ledger["verdict_id"],
            entry_ledger["strategy_id"], entry_ledger["qty"], Decimal(entry_ledger["net_pnl"])) == (
        1, None, "P1", "V1", "hi52", 60, gross - costs)


@pytest.mark.parametrize("hold, calendar_ends, expected", [
    (20, False, "2026-07-06"), (1, False, "2026-06-17"), (None, False, None), (20, True, None),
], ids=["n20", "n1", "no-hold", "beyond-calendar"])
def test_exit_session_is_session_n_of_the_hold_else_pending(conn, rig, hold, calendar_ends, expected) -> None:
    rig.hold, rig.calendar_ends = hold, calendar_ends
    assert position(conn, fill_entry(conn, rig))["exit_session"] == expected


@pytest.mark.parametrize("exit_px, tag, reason, outcome", [
    ("110", "gtt:900001:1", "target", "win"),
    ("95", "gtt:900001:0", "stop", "loss"),
    ("100.20", None, "external_unknown", "scratch"),
], ids=["target-leg", "stop-leg", "unnamed-exit"])
def test_close_books_gross_costs_at_the_entry_notional_and_a_paper_ledger_row(
    conn, rig, exit_px, tag, reason, outcome
) -> None:
    pid = fill_entry(conn, rig, fills=((4, "99"), (6, "100.6667")))
    sell(rig, pid, 10, exit_px, tag=tag)

    avg = Decimal("100.0000")
    gross = ((Decimal(exit_px) - avg) * 10).quantize(Decimal("0.01"))
    assert rig.costs == [(avg * 10, "CNC")]
    row = position(conn, pid)
    assert (row["state"], row["qty"], Decimal(row["avg_entry"]), row["close_reason"], row["close_basis"]) == (
        "CLOSED", 10, avg, reason, "fill")
    assert (Decimal(row["realized_pnl"]), row["costs"], row["closed_at"]) == (gross, "2.00", at(11, 0).isoformat())
    [entry] = ledger(conn)
    expected = {
        "position_id": pid, "rec_id": None, "is_paper": 1, "strategy_id": "hi52",
        "features_snapshot_id": "f", "agent_id": "swing", "proposal_id": "P1", "verdict_id": "V1",
        "entry_px": "100.0000", "exit_px": str(Decimal(exit_px).quantize(Decimal("0.0001"))), "qty": 10,
        "costs": "2.00", "gross_pnl": str(gross), "net_pnl": str(gross - Decimal("2.00")), "holding_minutes": 59,
        "close_reason": reason, "outcome_label": outcome, "created_at": at(10, 1).isoformat(),
        "closed_at": at(11, 0).isoformat(),
    }
    assert {k: entry[k] for k in expected} == expected


def test_a_partial_exit_reduces_the_open_qty_and_accrues_gross(conn, rig) -> None:
    pid = fill_entry(conn, rig)
    sell(rig, pid, 4, "105")
    row = position(conn, pid)
    assert (row["state"], row["qty"], Decimal(row["realized_pnl"]), row["costs"]) == ("OPEN", 6, 20, None)
    assert ledger(conn) == []


# ------------------------------------------------------------------------------------- oversell
@pytest.mark.parametrize("qty, error", [(7, None), (8, Oversell), (0, SubmitRefused)])
async def test_an_exit_above_open_minus_working_sells_is_refused_by_the_order_manager(conn, rig, qty, error) -> None:
    pid = fill_entry(conn, rig)
    for oid, state, paper, filled in (("W", "PARTIALLY_FILLED", 1, 1), ("C", "CANCELLED", 1, 0), ("R", "ACKED", 0, 0)):
        conn.execute("INSERT INTO orders (order_id, position_id, role, is_paper, state, product, side, qty, filled_qty) "
                     "VALUES (?, ?, 'exit', ?, ?, 'CNC', 'SELL', 4, ?)", (oid, pid, paper, state, filled))
    if error is None:
        await rig.mgr.submit_exit(pid, qty)
        assert len(await rig.broker.orders()) == 1
    else:
        with pytest.raises(error):
            await rig.mgr.submit_exit(pid, qty)
        assert await rig.broker.orders() == [] and conn.execute("SELECT count(*) FROM orders").fetchone()[0] == 4


# ------------------------------------------------------------------------------------- bookkeeping
@pytest.mark.parametrize("gtt_fired", [False, True], ids=["active-gtt-and-working-exit", "triggered-gtt-working-leg"])
async def test_a_bookkeeping_close_cancels_working_orders_and_deletes_the_active_gtt_first(
    conn, bus, rig, gtt_fired
) -> None:
    pid = fill_entry(conn, rig)
    gtt_id = await place_gtt(conn, rig, pid)
    if gtt_fired:
        rig.broker.on_tick(tick(at(10, 2), "94.50"))
    else:
        await rig.mgr.submit_exit(pid, 5, price=Decimal(120))
    await settle(bus, rig)
    real_gtt = dict(conn.execute("SELECT * FROM gtts WHERE is_paper = 0").fetchone())

    await rig.book.close_bookkeeping(pid, Decimal(97), at(11, 0), CloseReason.STOP, "downtime_replay")
    await settle(bus, rig)

    assert all(o["status"] == "CANCELLED" for o in await rig.broker.orders())
    assert [g["status"] for g in await rig.broker.gtts()] == (["triggered"] if gtt_fired else [])
    working = conn.execute("SELECT order_id, role, state FROM orders WHERE role != 'entry'").fetchall()
    assert [(r["role"], r["state"]) for r in working] == [("gtt_leg" if gtt_fired else "exit", "CANCELLED")]
    assert (OrderState.CANCEL_PENDING, OrderState.CANCELLED) in [
        (e.from_state, e.to_state) for e in OrderStore(conn).events(working[0]["order_id"])]
    paper_gtt = conn.execute("SELECT state FROM gtts WHERE gtt_id = ?", (gtt_id,)).fetchone()["state"]
    assert paper_gtt == ("active" if gtt_fired else "deleted")
    assert dict(conn.execute("SELECT * FROM gtts WHERE is_paper = 0").fetchone()) == real_gtt
    row = position(conn, pid)
    assert (row["state"], row["close_reason"], row["close_basis"], row["realized_pnl"]) == (
        "CLOSED", "stop", "downtime_replay", "-30.00")
    assert row["costs"] == "3.00"                       # priced off a bar: fees plus the spread


@pytest.mark.parametrize("neutered", ["delete_gtt", "cancel_order"])
async def test_a_bookkeeping_close_books_nothing_while_the_broker_side_survives(
    conn, bus, rig, monkeypatch, neutered
) -> None:
    pid = fill_entry(conn, rig)
    await place_gtt(conn, rig, pid)
    await rig.mgr.submit_exit(pid, 5, price=Decimal(120))
    await settle(bus, rig)

    async def no_op(*_args, **_kwargs) -> None:
        return None

    monkeypatch.setattr(rig.broker, neutered, no_op)
    with pytest.raises(CloseDeferred):
        await rig.book.close_bookkeeping(pid, Decimal(97), at(11, 0), CloseReason.STOP, "downtime_replay")
    assert position(conn, pid)["state"] == "OPEN" and ledger(conn) == []


async def test_a_fill_the_oms_has_not_applied_defers_the_close_until_it_lands(conn, bus, rig) -> None:
    pid = fill_entry(conn, rig)
    await rig.mgr.submit_exit(pid, 4, price=Decimal(100))
    await settle(bus, rig)
    rig.now.value = at(10, 5)
    rig.broker.on_tick(tick(at(10, 5), "101.00"))

    with pytest.raises(CloseDeferred):
        await rig.book.close_bookkeeping(pid, Decimal(102), at(10, 6), CloseReason.STOP, "downtime_replay")
    assert position(conn, pid)["qty"] == 10
    await settle(bus, rig)
    [(_, sold, px)] = [f for f in rig.fills if f[0] is OrderRole.EXIT]
    await rig.book.close_bookkeeping(pid, Decimal(102), at(10, 6), CloseReason.STOP, "downtime_replay")

    row = position(conn, pid)
    assert (sold, row["state"], row["qty"]) == (4, "CLOSED", 10)
    assert Decimal(row["realized_pnl"]) == (4 * (px - 100) + 6 * 2).quantize(Decimal("0.01"))


# ------------------------------------------------------------------------------------- void
@pytest.mark.parametrize("mark", [Decimal(90), None], ids=["mark", "no-mark"])
async def test_a_void_keeps_paper_equity_continuous_and_is_skipped_by_the_loss_streak(conn, rig, mark) -> None:
    sell(rig, fill_entry(conn, rig, n=2), 10, "95", when=at(10, 5))
    pid = fill_entry(conn, rig)
    tracker = ExposureTracker(conn, Clock(time_source=lambda: at(10, 10)), Decimal(40000),
                              mark_price=lambda _symbol: mark, scope="paper")
    before = tracker.equity()

    await rig.book.void(pid, mark, at(10, 10), "corp_action")

    assert tracker.equity() == before
    assert tracker.consecutive_losses(DAY) == 1
    row = position(conn, pid)
    void = ledger(conn)[-1]
    assert (row["state"], row["close_reason"], row["close_basis"], row["costs"]) == ("CLOSED", "void", "corp_action", "0.00")
    assert (void["outcome_label"], void["close_reason"], void["costs"], void["is_paper"], void["rec_id"]) == (
        "void", "void", "0.00", 1, None)


# ------------------------------------------------------------------------------------- isolation
REAL = "REAL"


@pytest.mark.parametrize("call, error", [
    (lambda book: book.on_fill(PlatformOrder(order_id="E", position_id=REAL, proposal_id="P1", role=OrderRole.ENTRY,
                                             is_paper=True, state=OrderState.FILLED, product="CNC", side="BUY"),
                               5, Decimal(100), at(11, 0)), Exception),
    (lambda book: book.on_fill(PlatformOrder(order_id="E", position_id=REAL, role=OrderRole.EXIT, is_paper=True,
                                             state=OrderState.FILLED, product="CNC", side="SELL"),
                               5, Decimal(100), at(11, 0)), None),
    (lambda book: book.on_fill(PlatformOrder(order_id="E", position_id=REAL, role=OrderRole.ENTRY, is_paper=False,
                                             state=OrderState.FILLED, product="CNC", side="BUY"),
                               5, Decimal(100), at(11, 0)), None),
    (lambda book: book.check_sell(REAL, 1), PositionNotHeld),
    (lambda book: book.mark_pending_exit(REAL, CloseReason.TIME_STOP), PositionNotHeld),
    (lambda book: book.close_bookkeeping(REAL, Decimal(100), at(11, 0), CloseReason.STOP, "x"), PositionNotHeld),
    (lambda book: book.void(REAL, Decimal(100), at(11, 0), "x"), PositionNotHeld),
], ids=["entry-fill", "exit-fill", "real-order", "check-sell", "pending-exit", "bookkeeping", "void"])
async def test_no_writer_touches_a_real_row(conn, rig, call, error) -> None:
    seed(conn)
    conn.execute("INSERT INTO positions (position_id, symbol, side, product, qty, avg_entry, state, is_paper, origin, "
                 "opened_at) VALUES (?, ?, 'BUY', 'CNC', 10, '100', 'OPEN', 0, 'recommended', ?)",
                 (REAL, SYM, at(9, 30).isoformat()))
    before = dict(position(conn, REAL))
    if error is None:
        call(rig.book)
    else:
        with pytest.raises(error):
            result = call(rig.book)
            if result is not None:
                await result
    assert dict(position(conn, REAL)) == before and ledger(conn) == []
