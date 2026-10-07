"""Paper ProtectionManager (plan Q4.5): GTT placement, the trigger and its leg, failures, exit retries, R3."""

from __future__ import annotations

import datetime as dt
import random
from decimal import ROUND_FLOOR, Decimal

import pytest

from engine.core.clock import IST, Clock
from engine.core.config import PaperSettings
from engine.core.contracts import PAPER_ORDER_UPDATE_TOPIC, EnterAction, GateVerdict
from engine.core.enums import Mode, RiskState
from engine.core.types import Bar, Session, Tick
from engine.oms.manager import OrderManager
from engine.oms.positions import PositionBook, leg_of
from engine.oms.protection import ProtectionManager
from engine.oms.state import TERMINAL_ORDER_STATES, CloseReason
from engine.oms.store import OrderStore
from engine.paper.broker import TERMINAL_STATUSES, PaperBroker, PaperOrderError
from engine.paper.fill_model import FillModelConfig

SYM = "TCS"
DAY = dt.date(2026, 6, 17)
NEXT = dt.date(2026, 6, 18)
TICK = Decimal("0.05")
R = CloseReason


def at(h: int, m: int, s: int = 0, d: dt.date = DAY) -> dt.datetime:
    return dt.datetime.combine(d, dt.time(h, m, s), tzinfo=IST)


def tick(ts: dt.datetime, ltp: str, cum: int = 0) -> Tick:
    px = Decimal(ltp)
    return Tick(instrument_token=1, tradingsymbol=SYM, ltp=px, volume_traded=cum, exchange_ts=ts,
                bid=px - TICK, ask=px + TICK)


def session(d: dt.date) -> Session | None:
    if d.weekday() >= 5:
        return None
    return Session(date_ist=d.isoformat(), pre_open_start=at(9, 0, d=d), pre_open_end=at(9, 15, d=d),
                   open=at(9, 15, d=d), close=at(15, 30, d=d))


def round_tick(_symbol: str, price: Decimal) -> Decimal:
    return (price / TICK).to_integral_value(ROUND_FLOOR) * TICK


class Now:
    def __init__(self) -> None:
        self.value = at(10, 0)

    def __call__(self) -> dt.datetime:
        return self.value


class Rig:
    """OrderManager -> ProtectionManager (PositionBook first) over one PaperBroker; ``exit_routine``
    stands in for the Q4.6 routine."""

    def __init__(self, conn, bus, *, broker: PaperBroker | None = None, now: Now | None = None,
                 transitions: bool = True) -> None:
        self.conn, self.bus = conn, bus
        self.now = now or Now()
        clock = Clock(time_source=self.now)
        self.broker = broker or PaperBroker(clock, bus.publish, FillModelConfig(), lambda _: TICK, 7,
                                            rejection_rate=0.0, topic=PAPER_ORDER_UPDATE_TOPIC)
        self.ltp: dict[str, Decimal] = {}
        self.exits: list[tuple[CloseReason, str]] = []
        self.alerts: list[tuple[str, str]] = []
        self.mgr = OrderManager(
            conn, clock, self.broker, order_guard=lambda _: None,
            sell_check=lambda pid, qty: self.book.check_sell(pid, qty),
            on_fill=lambda *a: self.pm.on_fill(*a),
            on_transition=(lambda *a: self.pm.on_transition(*a)) if transitions else None,
        )
        self.book = PositionBook(conn, clock, self.broker, orders=self.mgr, hold_fn=lambda *_: 20,
                                 session_of=lambda ts: ts.date(), add_sessions=lambda d, n: d + dt.timedelta(n),
                                 round_trip_fn=lambda notional, _p: notional * Decimal("0.003"))
        self.pm = ProtectionManager(
            conn, clock, self.broker, orders=self.mgr, book=self.book, exit_fn=self.exit_routine,
            notify=self.notify, session_fn=session, ltp_fn=self.ltp.get, round_tick_fn=round_tick,
            gtt_limit_offset_pct=Decimal("1.0"), settings=PaperSettings(),
        )
        self.mgr.start(bus)

    async def notify(self, key: str, message: str) -> None:
        self.alerts.append((key, message))

    async def exit_routine(self, pid: str, reason: CloseReason, basis: str) -> None:
        self.exits.append((reason, basis))
        if working(self.conn, pid, "SELL"):
            return
        ours = {r["gtt_id"] for r in self.conn.execute("SELECT gtt_id FROM gtts WHERE position_id = ?", (pid,))}
        for g in await self.broker.gtts():
            if g["id"] in ours and g["status"] == "active":
                await self.broker.delete_gtt(g["id"])
                self.conn.execute("UPDATE gtts SET state = 'deleted' WHERE gtt_id = ?", (g["id"],))
        for order in working(self.conn, pid, "BUY"):
            await self.mgr.cancel(order["broker_order_id"], reason="exit")
        self.book.mark_pending_exit(pid, reason, basis)
        await self.mgr.submit_exit(pid, position(self.conn, pid)["qty"])

    async def settle(self) -> None:
        for _ in range(6):
            await self.bus.drain(1.0)
            await self.mgr.join()

    async def px(self, ts: dt.datetime, ltp: str, cum: int = 0) -> None:
        self.now.value = ts
        self.ltp[SYM] = Decimal(ltp)
        self.broker.on_tick(tick(ts, ltp, cum))
        await self.settle()

    async def enter(self, *, qty: int = 10, target: str | None = "110", limit: str | None = None, n: int = 1,
                    ltp: str = "100", ts: dt.datetime | None = None) -> str:
        order = await self.mgr.submit(*seed(self.conn, n=n, qty=qty, target=target, limit=limit))
        await self.px(ts or at(10, 0, 1), ltp)
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


def seed(conn, *, n: int = 1, qty: int = 10, target: str | None = "110",
         limit: str | None = None) -> tuple[EnterAction, GateVerdict]:
    proposal = EnterAction(
        action="enter", proposal_id=f"P{n}", agent_id="swing", thesis="paper protection manager test",
        confidence=0.6, tradingsymbol=SYM, exchange="NSE", side="BUY", style="swing",
        entry_type="MARKET" if limit is None else "LIMIT", entry_price=None if limit is None else Decimal(limit),
        stop_price=Decimal("95"), target_price=None if target is None else Decimal(target), quantity=qty,
        signal_id="s", strategy_id="hi52", features_snapshot_id="f",
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


def position(conn, pid: str):
    return conn.execute("SELECT * FROM positions WHERE position_id = ?", (pid,)).fetchone()


def working(conn, pid: str, side: str) -> list:
    states = ", ".join(f"'{s.value}'" for s in TERMINAL_ORDER_STATES)
    return conn.execute(f"SELECT * FROM orders WHERE position_id = ? AND side = ? AND state NOT IN ({states})",
                        (pid, side)).fetchall()


def orders(conn, role: str) -> list:
    return conn.execute("SELECT * FROM orders WHERE role = ? ORDER BY created_at", (role,)).fetchall()


def volume(rig: Rig, per_minute_cap: int) -> None:
    """A 10 % participation budget of ``per_minute_cap`` shares per minute."""
    px = Decimal(100)
    rig.broker.on_bar(Bar(symbol=SYM, ts_minute=at(10, 0), open=px, high=px, low=px, close=px,
                          volume=per_minute_cap * 10))


async def fire_leg_below_its_limit(rig: Rig, fire_at: dt.datetime) -> str:
    pid = await rig.enter()
    await rig.px(fire_at, "90.00")
    [leg] = orders(rig.conn, "gtt_leg")
    assert (leg["state"], leg["price"], position(rig.conn, pid)["state"]) == ("ACKED", "94.05", "PENDING_EXIT")
    return pid


# ------------------------------------------------------------------------------------- construction
def test_ctor_takes_only_a_paper_broker(conn, clock) -> None:
    with pytest.raises(TypeError, match="PaperBroker only"):
        ProtectionManager(conn, clock, object(), orders=None, book=None, exit_fn=None, notify=None, session_fn=session,
                          ltp_fn=dict().get, round_tick_fn=round_tick, gtt_limit_offset_pct=Decimal(1),
                          settings=PaperSettings())


# ------------------------------------------------------------------------------------- placement
@pytest.mark.parametrize("target", ["110", None], ids=["oco", "single"])
async def test_an_entry_fill_places_one_gtt_at_the_stop_with_last_price_the_entry(conn, rig, target) -> None:
    pid = await rig.enter(target=target)

    avg = Decimal(position(conn, pid)["avg_entry"])
    [g] = await rig.gtts()
    legs = [Decimal("95")] + ([Decimal(target)] if target else [])
    prices = [Decimal("94.05")] + ([Decimal(target)] if target else [])
    assert (g["status"], g["condition"]["trigger_values"], g["condition"]["last_price"]) == ("active", legs, avg)
    assert [(o["transaction_type"], o["quantity"], o["product"], o["price"]) for o in g["orders"]] == [
        ("SELL", 10, "CNC", p) for p in prices]
    row = conn.execute("SELECT * FROM gtts").fetchone()
    assert {k: row[k] for k in ("gtt_id", "position_id", "state", "trigger_low", "trigger_high", "is_paper", "symbol",
                                 "side", "product", "qty", "stop_limit", "target_limit", "created_at")} == {
        "gtt_id": g["id"], "position_id": pid, "state": "active", "trigger_low": "95", "trigger_high": target,
        "is_paper": 1, "symbol": SYM, "side": "SELL", "product": "CNC", "qty": 10, "stop_limit": "94.05",
        "target_limit": target, "created_at": at(10, 0, 1).isoformat()}
    assert Decimal(row["last_price"]) == avg
    assert position(conn, pid)["protection_state"] == "PROTECTED"


async def test_partial_entry_fills_keep_one_gtt_sized_to_the_open_qty(conn, rig) -> None:
    await rig.mgr.submit(*seed(conn, qty=60))
    volume(rig, 30)
    await rig.px(at(10, 1), "100.00", cum=5000)
    await rig.px(at(10, 2), "100.00", cum=5000)

    [g] = await rig.gtts()
    [row] = conn.execute("SELECT * FROM gtts").fetchall()
    pos = position(conn, orders(conn, "entry")[0]["position_id"])
    assert (pos["qty"], g["orders"][0]["quantity"], row["qty"], g["status"]) == (60, 60, 60, "active")
    assert Decimal(row["last_price"]) == g["condition"]["last_price"] == Decimal(pos["avg_entry"])


# ------------------------------------------------------------------------------------- trigger and leg
async def test_a_triggered_gtt_whose_leg_works_is_exit_working_and_never_replaced_or_doubled(conn, rig) -> None:
    pid = await rig.enter()
    await rig.px(at(10, 5), "94.50")
    [g] = await rig.gtts()

    with pytest.raises(PaperOrderError, match="triggered"):
        await rig.broker.delete_gtt(g["id"])
    await rig.pm.tick()
    await rig.pm.verify_all()
    pos = position(conn, pid)
    assert (pos["state"], pos["close_reason"], g["status"]) == ("PENDING_EXIT", "stop", "triggered")
    assert conn.execute("SELECT state FROM gtts").fetchone()["state"] == "triggered"
    assert len(await rig.gtts()) == 1 and len(await rig.broker.orders()) == 2 and rig.exits == []

    await rig.px(at(10, 6), "94.50")
    pos = position(conn, pid)
    assert (pos["state"], pos["close_reason"], pos["close_basis"]) == ("CLOSED", "stop", "fill")
    assert orders(conn, "exit") == [] and rig.exits == []


@pytest.mark.parametrize("fire_at, tick_at, cancelled", [
    (at(10, 5), at(10, 9, 59), False),
    (at(10, 5), at(10, 10), True),
    (at(15, 18), at(15, 20), True),
], ids=["inside-timeout", "timeout", "close-less-10"])
async def test_a_leg_resting_below_its_limit_is_cancelled_after_the_timeout_or_at_close_less_10(
    conn, rig, fire_at, tick_at, cancelled
) -> None:
    pid = await fire_leg_below_its_limit(rig, fire_at)
    rig.now.value = tick_at
    await rig.pm.tick()
    await rig.settle()

    [leg] = orders(conn, "gtt_leg")
    assert leg["state"] == ("CANCELLED" if cancelled else "ACKED")
    assert rig.mgr.cancel_intended(leg["order_id"]) is cancelled
    assert rig.exits == ([(R.GTT_FAILURE_EXIT, "leg_cancelled")] if cancelled else [])
    if cancelled:
        await rig.px(tick_at + dt.timedelta(seconds=2), "90.00")
        pos = position(conn, pid)
        assert (pos["state"], pos["close_reason"], pos["close_basis"]) == ("CLOSED", "gtt_failure_exit", "leg_cancelled")


async def test_a_leg_lapsed_at_the_session_roll_goes_to_the_exit_routine(conn, rig) -> None:
    pid = await fire_leg_below_its_limit(rig, at(15, 25))
    await rig.px(at(9, 15, 1, NEXT), "90.00")

    [leg] = orders(conn, "gtt_leg")
    assert (leg["state"], rig.exits) == ("CANCELLED", [(R.GTT_FAILURE_EXIT, "leg_cancelled")])
    await rig.px(at(9, 15, 3, NEXT), "90.00")
    assert position(conn, pid)["state"] == "CLOSED"


@pytest.mark.parametrize("leg_filled", [False, True], ids=["leg-working", "leg-filled-unapplied"])
async def test_a_reconcile_during_a_leg_fire_neither_replaces_nor_exits(conn, rig, leg_filled) -> None:
    pid = await rig.enter()
    for ts in [at(10, 5)] + ([at(10, 5, 2)] if leg_filled else []):
        rig.now.value = ts
        rig.broker.on_tick(tick(ts, "94.50"))
    await rig.pm.verify_all()

    assert orders(conn, "gtt_leg") == [] and orders(conn, "exit") == [] and rig.exits == []
    assert position(conn, pid)["state"] == ("OPEN" if leg_filled else "PENDING_EXIT")
    assert len(await rig.gtts()) == 1 and len(await rig.broker.orders()) == 2
    await rig.settle()
    await rig.px(at(10, 6), "94.50")
    await rig.pm.tick()
    assert position(conn, pid)["state"] == "CLOSED"
    assert (len(orders(conn, "gtt_leg")), orders(conn, "exit"), rig.exits) == (1, [], [])


async def test_an_entry_still_resting_when_the_gtt_triggers_is_cancelled(conn, rig) -> None:
    await rig.mgr.submit(*seed(conn, qty=20, limit="96"))
    volume(rig, 10)
    await rig.px(at(10, 1), "95.90", cum=5000)
    await rig.px(at(10, 1, 30), "94.00", cum=5000)

    [entry] = orders(conn, "entry")
    assert (entry["state"], entry["filled_qty"]) == ("CANCELLED", 10)
    assert rig.mgr.cancel_intended(entry["order_id"])
    await rig.px(at(10, 2), "94.50", cum=5000)
    pos = position(conn, entry["position_id"])
    assert (pos["state"], pos["qty"], pos["close_reason"], rig.exits) == ("CLOSED", 10, "stop", [])


async def test_a_residual_after_a_leg_fill_reenters_the_exit_routine(conn, rig) -> None:
    await rig.mgr.submit(*seed(conn, qty=20, limit="100"))
    volume(rig, 10)
    await rig.px(at(10, 1), "99.90", cum=5000)
    await rig.px(at(10, 2), "94.00", cum=5000)
    [entry] = orders(conn, "entry")
    pid = entry["position_id"]
    assert (entry["state"], position(conn, pid)["qty"]) == ("FILLED", 20)

    await rig.px(at(10, 3), "94.50", cum=5000)
    assert position(conn, pid)["qty"] == 10 and rig.exits == [(R.STOP, "leg_residual")]
    await rig.px(at(10, 4), "94.50", cum=5000)
    pos = position(conn, pid)
    assert (pos["state"], pos["qty"], pos["close_reason"], pos["close_basis"]) == ("CLOSED", 20, "stop", "leg_residual")


async def test_a_spent_gtt_with_open_qty_reaches_a_market_sell(conn, make_rig, monkeypatch) -> None:
    rig = make_rig(transitions=False)
    pid = await rig.enter()
    match = rig.broker._match

    def reject_sells(order, *args):
        if order.transaction_type == "SELL":
            raise RuntimeError("leg cannot book")
        return match(order, *args)

    monkeypatch.setattr(rig.broker, "_match", reject_sells)
    await rig.px(at(10, 5), "94.50")
    await rig.px(at(10, 5, 2), "94.50")
    monkeypatch.setattr(rig.broker, "_match", match)
    assert orders(conn, "gtt_leg")[0]["state"] == "REJECTED" and position(conn, pid)["state"] == "OPEN"

    await rig.pm.tick()
    [exit_order] = orders(conn, "exit")
    assert (exit_order["price"], exit_order["qty"], rig.exits) == (None, 10, [(R.GTT_FAILURE_EXIT, "gtt_spent")])
    await rig.px(at(10, 6), "94.50")
    assert position(conn, pid)["close_reason"] == "gtt_failure_exit"


# ------------------------------------------------------------------------------------- gap-through and failures
@pytest.mark.parametrize("case", ["first-arm", "re-arm", "re-arm-after-close"])
async def test_a_rearm_at_or_below_the_stop_is_the_exit_routine(conn, rig, case) -> None:
    if case == "first-arm":
        rig.ltp[SYM] = Decimal("94.90")
        await rig.mgr.submit(*seed(conn))
        rig.now.value = at(10, 0, 1)
        rig.broker.on_tick(tick(at(10, 0, 1), "100.00"))
        await rig.settle()
        pid = orders(conn, "entry")[0]["position_id"]
    else:
        pid = await rig.enter()
        [g] = await rig.gtts()
        await rig.broker.delete_gtt(g["id"])
        rig.ltp[SYM] = Decimal("95")
        rig.now.value = at(15, 35) if case == "re-arm-after-close" else at(10, 5)
        await rig.pm.verify_all()
        assert conn.execute("SELECT state FROM gtts").fetchone()["state"] == "missing"
    if case == "re-arm-after-close":
        assert rig.exits == []
        rig.now.value = at(9, 15, 0, NEXT)
        await rig.pm.tick()
    await rig.settle()

    assert rig.exits == [(R.STOP, "stop_gap_through")] and await rig.gtts() == []
    [exit_order] = orders(conn, "exit")
    pos = position(conn, pid)
    assert (exit_order["qty"], exit_order["price"], pos["state"], pos["close_basis"]) == (
        10, None, "PENDING_EXIT", "stop_gap_through")


@pytest.mark.parametrize("cause", ["gtt-lost-four-times", "broker-failed-gtt"])
async def test_protection_failed_runs_the_exit_routine_and_alerts(conn, rig, monkeypatch, cause) -> None:
    pid = await rig.enter()
    if cause == "broker-failed-gtt":
        def broken(*_args) -> None:
            raise RuntimeError("leg cannot be registered")

        monkeypatch.setattr(rig.broker, "_fire_gtt_leg", broken)
        await rig.px(at(10, 5), "94.50")
        await rig.pm.tick()
        assert conn.execute("SELECT state FROM gtts").fetchone()["state"] == "failed"
    else:
        rig.now.value = at(10, 5)
        for lost in range(4):
            [g] = await rig.gtts()
            await rig.broker.delete_gtt(g["id"])
            await rig.pm.verify_all()
            assert rig.alerts == [] or lost == 3
        assert [r["state"] for r in conn.execute("SELECT state FROM gtts ORDER BY gtt_id")] == ["missing"] * 4

    pos = position(conn, pid)
    assert (pos["protection_state"], pos["state"], [k for k, _ in rig.alerts]) == (
        "PROTECTION_FAILED", "PENDING_EXIT", [f"{pid}:1"])
    assert rig.exits == [(R.GTT_FAILURE_EXIT, "protection_failed")] and len(orders(conn, "exit")) == 1


async def test_a_rejected_exit_is_retried_three_times_then_fails_and_retries_next_session(
    conn, rig, monkeypatch
) -> None:
    pid = await rig.enter()
    monkeypatch.setattr(rig.broker, "_rejection_rate", 1.0)
    rig.now.value = at(15, 20)
    await rig.exit_routine(pid, R.TIME_STOP, "fill")
    await rig.settle()

    rejected = [o["state"] for o in orders(conn, "exit")]
    pos = position(conn, pid)
    assert rejected == ["REJECTED"] * 4 and rig.exits == [(R.TIME_STOP, "fill")] * 4
    assert (pos["state"], pos["protection_state"], [k for k, _ in rig.alerts]) == (
        "PENDING_EXIT", "PROTECTION_FAILED", [f"{pid}:1"])
    rig.now.value = at(15, 25)
    await rig.pm.tick()
    assert len(rig.exits) == 4

    monkeypatch.setattr(rig.broker, "_rejection_rate", 0.0)
    rig.now.value = at(9, 15, 0, NEXT)
    await rig.pm.tick()
    await rig.px(at(9, 15, 2, NEXT), "100.00")
    pos = position(conn, pid)
    assert len(rig.exits) == 5 and (pos["state"], pos["close_reason"]) == ("CLOSED", "time_stop")


async def test_an_exit_the_platform_cancelled_is_not_retried_on_its_postback_but_a_stalled_exit_is(conn, rig) -> None:
    pid = await rig.enter()
    rig.book.mark_pending_exit(pid, R.TIME_STOP)
    resting = await rig.mgr.submit_exit(pid, 10, price=Decimal(120))
    await rig.settle()
    await rig.mgr.cancel(resting.broker_order_id, reason="test")
    await rig.settle()
    assert (orders(conn, "exit")[0]["state"], rig.exits) == ("CANCELLED", [])

    await rig.pm.tick()
    assert rig.exits == [(R.TIME_STOP, "fill")] and len(working(conn, pid, "SELL")) == 1


# ------------------------------------------------------------------------------------- isolation
async def test_a_same_symbol_real_position_and_its_rows_stay_byte_identical(conn, rig) -> None:
    conn.execute("INSERT INTO positions (position_id, symbol, side, product, qty, avg_entry, stop, state, "
                 "protection_state, is_paper, origin, opened_at) VALUES ('REAL', ?, 'BUY', 'CNC', 10, '100', '95', "
                 "'OPEN', 'PROTECTION_PENDING', 0, 'recommended', ?)", (SYM, at(9, 30).isoformat()))
    conn.execute("INSERT INTO gtts (gtt_id, position_id, state, trigger_low) VALUES (1, 'REAL', 'active', '95')")
    conn.execute("INSERT INTO orders (order_id, broker_order_id, position_id, role, is_paper, state, product, side, "
                 "qty) VALUES ('RO', '250000000000001', 'REAL', 'exit', 0, 'ACKED', 'CNC', 'SELL', 10)")

    def real() -> list:
        return [tuple(r) for q in ("SELECT * FROM positions WHERE is_paper = 0", "SELECT * FROM gtts WHERE is_paper = 0",
                                   "SELECT * FROM orders WHERE is_paper = 0") for r in conn.execute(q)]

    before = real()
    pid = await fire_leg_below_its_limit(rig, at(10, 5))
    rig.now.value = at(10, 10)
    await rig.pm.tick()
    await rig.settle()
    await rig.px(at(10, 10, 2), "90.00")
    await rig.pm.tick()

    assert position(conn, pid)["state"] == "CLOSED" and real() == before


# ------------------------------------------------------------------------------------- R3 property
N_TICKS = 3


async def held_and_protected(rig: Rig) -> dict[str, bool]:
    """Per held paper position: an ACTIVE GTT for the open qty, or PENDING_EXIT with a working sell, or
    PROTECTION_FAILED that has alerted."""
    live = {g["id"]: g for g in await rig.gtts()}
    book = await rig.broker.orders()
    out = {}
    for pos in rig.conn.execute("SELECT * FROM positions WHERE is_paper = 1 AND state IN ('OPEN', 'PENDING_EXIT')"):
        pid = pos["position_id"]
        gtt_ids = {r["gtt_id"] for r in rig.conn.execute("SELECT gtt_id FROM gtts WHERE position_id = ?", (pid,))}
        active = [g for i, g in live.items() if i in gtt_ids and g["status"] == "active"]
        leg_working = any(o["status"] not in TERMINAL_STATUSES and (leg_of(o["tag"]) or (None,))[0] in gtt_ids
                          for o in book)
        out[pid] = bool(
            (pos["state"] == "OPEN" and active and active[0]["orders"][0]["quantity"] == pos["qty"])
            or (pos["state"] == "PENDING_EXIT" and (working(rig.conn, pid, "SELL") or leg_working))
            or (pos["protection_state"] == "PROTECTION_FAILED" and any(k.startswith(pid) for k, _ in rig.alerts))
        )
    return out


@pytest.mark.parametrize("seed_", range(12))
async def test_r3_every_open_position_is_protected_or_exiting_within_n_ticks(conn, bus, make_rig, seed_) -> None:
    rng = random.Random(seed_)
    now = Now()
    rig = make_rig(now=now, broker=PaperBroker(Clock(time_source=now), bus.publish, FillModelConfig(), lambda _: TICK,
                                               seed_, rejection_rate=0.25, topic=PAPER_ORDER_UPDATE_TOPIC))
    volume(rig, rng.choice([5, 20, 1000]))
    for n in range(1, 4):
        await rig.mgr.submit(*seed(conn, n=n, qty=rng.randint(10, 40), target=rng.choice(["106", None]),
                                   limit=rng.choice([None, "100"])))
    price, ts, unprotected = Decimal(100), at(10, 0, 1), {}
    for _ in range(80):
        ts += dt.timedelta(seconds=20)
        price = max(Decimal(80), price + Decimal(rng.choice(["-0.6", "-0.3", "0", "0.3", "0.6", "-3", "2"])))
        await rig.px(ts, str(price), cum=5000)
        if rng.random() < 0.05:
            for g in await rig.gtts():
                if g["status"] == "active":
                    await rig.broker.delete_gtt(g["id"])
        await rig.pm.tick()
        await rig.settle()

        for pid, ok in (await held_and_protected(rig)).items():
            unprotected[pid] = 0 if ok else unprotected.get(pid, 0) + 1
            assert unprotected[pid] <= N_TICKS, f"{pid} unprotected for {unprotected[pid]} ticks at {ts}"
        for pos in (await rig.broker.positions())["net"]:
            assert pos["quantity"] >= 0, f"paper CNC went short: {pos}"
        for pid in {r["position_id"] for r in conn.execute("SELECT position_id FROM positions")}:
            assert len(working(conn, pid, "SELL")) <= 1, f"{pid} has two working exits"
    sold = conn.execute("SELECT COALESCE(SUM(filled_qty), 0) FROM orders WHERE side = 'SELL'").fetchone()[0]
    bought = conn.execute("SELECT COALESCE(SUM(filled_qty), 0) FROM orders WHERE side = 'BUY'").fetchone()[0]
    assert sold <= bought
    assert all(OrderStore(conn).get(o["order_id"]).is_paper for o in conn.execute("SELECT order_id FROM orders"))
