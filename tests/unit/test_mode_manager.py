"""Mode + risk-state stickiness, transition guards, and the trade-window setter (R5/R10, §3.5.3)."""

from __future__ import annotations

from datetime import time

import pytest

from engine.core.db import connect
from engine.core.enums import Actor, Mode, RiskState, Routing
from engine.core.types import OwnerConfirmation, TradeWindow
from engine.risk.events import TOPIC_TRADE_WINDOW, TradeWindowChanged
from engine.risk.mode import ModeManager


def test_defaults_are_safe(conn, clock):
    mm = ModeManager(conn, clock)
    assert mm.mode() == Mode.OFF
    assert mm.risk_state() == RiskState.NORMAL
    assert mm.routing() is None


@pytest.mark.asyncio
async def test_owner_can_go_recommend_single_step(conn, clock):
    mm = ModeManager(conn, clock)
    assert await mm.request_transition(Mode.RECOMMEND, Actor.OWNER) is True
    assert mm.mode() == Mode.RECOMMEND
    assert mm.routing() is None


@pytest.mark.asyncio
async def test_auto_requires_two_step_confirmation(conn, clock):
    mm = ModeManager(conn, clock)
    with pytest.raises(PermissionError):
        await mm.request_transition(Mode.AUTO, Actor.OWNER)  # no confirmation
    with pytest.raises(PermissionError):
        await mm.request_transition(Mode.AUTO, Actor.OWNER, OwnerConfirmation(actor=Actor.OWNER, confirmed=False))
    assert mm.mode() == Mode.OFF

    ok = await mm.request_transition(
        Mode.AUTO, Actor.OWNER, OwnerConfirmation(actor=Actor.OWNER, confirmed=True)
    )
    assert ok and mm.mode() == Mode.AUTO
    assert mm.routing() == Routing.PAPER  # default safe routing (live gated elsewhere, §8.5)


@pytest.mark.asyncio
async def test_non_owner_cannot_transition(conn, clock):
    mm = ModeManager(conn, clock)
    with pytest.raises(PermissionError):
        await mm.request_transition(Mode.RECOMMEND, Actor.LEARNER)


@pytest.mark.asyncio
async def test_force_downgrade_only_lowers(conn, clock):
    mm = ModeManager(conn, clock)
    await mm.request_transition(Mode.AUTO, Actor.OWNER, OwnerConfirmation(actor=Actor.OWNER, confirmed=True))
    await mm.force_downgrade(Mode.RECOMMEND, "daily_loss_hard")
    assert mm.mode() == Mode.RECOMMEND
    # A "downgrade" to a higher mode is a no-op (re-arm is owner-only, R3).
    await mm.force_downgrade(Mode.AUTO, "should_not_upgrade")
    assert mm.mode() == Mode.RECOMMEND


@pytest.mark.asyncio
async def test_opening_orders_predicate(conn, clock):
    mm = ModeManager(conn, clock)
    # OFF: never.
    assert mm.opening_orders_allowed(in_window=True) is False
    await mm.request_transition(Mode.AUTO, Actor.OWNER, OwnerConfirmation(actor=Actor.OWNER, confirmed=True))
    assert mm.opening_orders_allowed(in_window=True) is True
    assert mm.opening_orders_allowed(in_window=False) is False  # outside window
    await mm.set_risk_state(RiskState.FROZEN, "stale_feed", Actor.RISK_GATE)
    assert mm.opening_orders_allowed(in_window=True) is False    # FROZEN blocks opening


@pytest.mark.asyncio
async def test_risk_state_sticky_across_restart(conn, clock, db_path):
    mm = ModeManager(conn, clock)
    await mm.set_risk_state(RiskState.CLOSE_ONLY, "equity_floor_rung", Actor.RISK_GATE)
    conn2 = connect(db_path)
    try:
        assert ModeManager(conn2, clock).risk_state() == RiskState.CLOSE_ONLY
    finally:
        conn2.close()


@pytest.mark.asyncio
async def test_set_trade_window_validates_and_persists(conn, clock):
    mm = ModeManager(conn, clock)
    # invalid: empty MIS sub-window after the buffer (end-buffer <= start)
    assert await mm.set_trade_window(time(10, 0), time(10, 3), Actor.OWNER, squareoff_buffer_min=5) is False
    assert mm.get_trade_window() is None  # value unchanged

    # invalid: start >= end
    assert await mm.set_trade_window(time(11, 0), time(10, 0), Actor.OWNER) is False

    # valid
    assert await mm.set_trade_window(time(10, 0), time(10, 45), Actor.OWNER, squareoff_buffer_min=5) is True
    tw = mm.get_trade_window()
    assert tw == TradeWindow(start=time(10, 0), end=time(10, 45), squareoff_buffer_min=5)
    # audited
    rows = conn.execute("SELECT name FROM config_audit WHERE name='trade_window_state'").fetchall()
    assert len(rows) == 1


@pytest.mark.asyncio
async def test_set_trade_window_owner_only(conn, clock):
    mm = ModeManager(conn, clock)
    with pytest.raises(PermissionError):
        await mm.set_trade_window(time(10, 0), time(10, 45), Actor.LEARNER)


def test_seed_trade_window_if_absent(conn, clock):
    mm = ModeManager(conn, clock)
    seed = TradeWindow(start=time(10, 0), end=time(10, 30), squareoff_buffer_min=5)
    assert mm.seed_trade_window_if_absent(seed) is True
    assert mm.get_trade_window() == seed
    # Idempotent: a second seed does not overwrite.
    assert mm.seed_trade_window_if_absent(TradeWindow(start=time(9, 30), end=time(15, 0))) is False
    assert mm.get_trade_window() == seed


@pytest.mark.asyncio
async def test_set_trade_window_survives_restart(conn, clock, db_path):
    mm = ModeManager(conn, clock)
    tw = TradeWindow(start=time(9, 15), end=time(15, 0), squareoff_buffer_min=10)
    assert await mm.set_trade_window(
        tw.start, tw.end, Actor.OWNER, squareoff_buffer_min=tw.squareoff_buffer_min
    ) is True

    conn2 = connect(db_path)
    try:
        assert ModeManager(conn2, clock).get_trade_window() == tw
    finally:
        conn2.close()


@pytest.mark.asyncio
async def test_set_trade_window_emits_trade_window_changed(conn, clock, bus):
    captured: list[TradeWindowChanged] = []

    async def _capture(event: TradeWindowChanged) -> None:
        captured.append(event)

    bus.subscribe(TOPIC_TRADE_WINDOW, _capture)
    mm = ModeManager(conn, clock, bus=bus)

    assert await mm.set_trade_window(time(10, 0), time(10, 45), Actor.OWNER, squareoff_buffer_min=5) is True
    assert len(captured) == 1
    event = captured[0]
    assert isinstance(event, TradeWindowChanged)
    assert event.start_ist == "10:00"
    assert event.end_ist == "10:45"
    assert event.squareoff_buffer_min == 5
    assert event.actor == Actor.OWNER

    # invalid: start >= end — rejected before persist/publish, no additional event.
    assert await mm.set_trade_window(time(11, 0), time(10, 0), Actor.OWNER) is False
    assert len(captured) == 1


@pytest.mark.asyncio
async def test_set_trade_window_shrink_fires_squareoff(conn, clock):
    mm = ModeManager(conn, clock)

    # Case A: new end <= now (clock.now() is frozen at 10:05 IST) -> squareoff fires exactly once.
    calls_a: list[None] = []

    async def cb_a() -> None:
        calls_a.append(None)

    assert await mm.set_trade_window(
        time(9, 0), time(10, 0), Actor.OWNER, squareoff_buffer_min=5, on_shrink_squareoff=cb_a
    ) is True
    assert len(calls_a) == 1

    # Case B: new end > now -> no squareoff.
    calls_b: list[None] = []

    async def cb_b() -> None:
        calls_b.append(None)

    assert await mm.set_trade_window(
        time(10, 0), time(10, 45), Actor.OWNER, squareoff_buffer_min=5, on_shrink_squareoff=cb_b
    ) is True
    assert len(calls_b) == 0

    # Case C: invalid window (start >= end) -> validation rejects before the shrink check runs.
    calls_c: list[None] = []

    async def cb_c() -> None:
        calls_c.append(None)

    assert await mm.set_trade_window(
        time(11, 0), time(10, 0), Actor.OWNER, on_shrink_squareoff=cb_c
    ) is False
    assert len(calls_c) == 0
