"""Kill-switch stickiness + control (R10, §7.2). The G0 gate item: set kill → "restart" → blocked."""

from __future__ import annotations

import pytest

from engine.core.db import connect
from engine.core.enums import Actor
from engine.core.types import OwnerConfirmation
from engine.risk.kill import KillSwitch, KillSwitchEngaged


@pytest.mark.asyncio
async def test_trigger_persists_killed_and_blocks_orders(conn, clock):
    ks = KillSwitch(conn, clock)
    assert ks.is_killed() is False
    ks.assert_orders_allowed()  # no raise when not killed

    await ks.trigger("manual /kill", actor=Actor.OWNER, flatten=False)
    assert ks.is_killed() is True
    assert ks.reason() == "manual /kill"
    with pytest.raises(KillSwitchEngaged):
        ks.assert_orders_allowed()


@pytest.mark.asyncio
async def test_kill_state_survives_restart(conn, clock, db_path):
    ks = KillSwitch(conn, clock)
    await ks.trigger("cumulative_floor breach", actor=Actor.RISK_GATE, flatten=False)

    # Simulate a process restart: a brand-new connection to the same db sees KILLED (sticky, R10).
    conn2 = connect(db_path)
    try:
        ks2 = KillSwitch(conn2, clock)
        assert ks2.is_killed() is True
        with pytest.raises(KillSwitchEngaged):
            ks2.assert_orders_allowed()
    finally:
        conn2.close()


@pytest.mark.asyncio
async def test_reset_requires_owner_two_step(conn, clock):
    ks = KillSwitch(conn, clock)
    await ks.trigger("test", flatten=False)

    # Unconfirmed reset is rejected.
    with pytest.raises(PermissionError):
        await ks.owner_reset(OwnerConfirmation(actor=Actor.OWNER, confirmed=False))
    # Non-owner confirmed is rejected.
    with pytest.raises(PermissionError):
        await ks.owner_reset(OwnerConfirmation(actor=Actor.RISK_GATE, confirmed=True))
    assert ks.is_killed() is True

    await ks.owner_reset(OwnerConfirmation(actor=Actor.OWNER, confirmed=True, note="two-step via Telegram"))
    assert ks.is_killed() is False
    ks.assert_orders_allowed()


@pytest.mark.asyncio
async def test_flatten_callback_runs_after_persist(conn, clock):
    order: list[str] = []

    async def flatten(reason: str) -> None:
        # By the time flatten runs, KILLED must already be persisted (persist-before-act, R10).
        assert ks.is_killed() is True
        order.append("flatten")

    ks = KillSwitch(conn, clock, flatten_callback=flatten)
    await ks.trigger("daily_loss_hard", actor=Actor.RISK_GATE, flatten=True)
    assert order == ["flatten"]
