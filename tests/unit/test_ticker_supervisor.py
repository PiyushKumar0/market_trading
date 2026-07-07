"""TickerSupervisor respawn lifecycle (§2.2/§3.2.2, A4/R2): a respawn must cancel the old read loop
before installing a fresh child, or the parked read task leaks (it runs forever)."""

from __future__ import annotations

import asyncio

import pytest

from engine.broker.ticker_supervisor import TickerSupervisor


class _FakeTickerCfg:
    tcp_host = "127.0.0.1"
    tcp_port = 8401
    heartbeat_silence_kill_s = 10
    max_instruments_per_conn = 3000


class _FakeSettings:
    ticker = _FakeTickerCfg()


@pytest.mark.asyncio
async def test_respawn_cancels_old_read_task(clock, monkeypatch):
    sup = TickerSupervisor(_FakeSettings(), clock, bus=None)
    # Stand in for a live child + its parked read loop (as _spawn_child would have created).
    old_read = asyncio.create_task(asyncio.Event().wait())
    sup._read_task = old_read
    sup._access_token = "tok"

    spawned = {"n": 0}

    async def fake_spawn():
        spawned["n"] += 1

    async def fake_terminate():
        sup._proc = None

    monkeypatch.setattr(sup, "_spawn_child", fake_spawn)
    monkeypatch.setattr(sup, "_terminate_child", fake_terminate)

    await sup._respawn(reason="heartbeat_silence")

    assert old_read.cancelled()          # the old read loop is cancelled, not leaked
    assert sup._read_task is None
    assert spawned["n"] == 1             # a fresh child was spawned


@pytest.mark.asyncio
async def test_respawn_without_token_stops(clock, monkeypatch):
    sup = TickerSupervisor(_FakeSettings(), clock, bus=None)
    old_read = asyncio.create_task(asyncio.Event().wait())
    sup._read_task = old_read
    sup._access_token = None            # cannot respawn without a token

    async def fake_spawn():
        raise AssertionError("must not spawn without an access token")

    async def fake_terminate():
        sup._proc = None

    monkeypatch.setattr(sup, "_spawn_child", fake_spawn)
    monkeypatch.setattr(sup, "_terminate_child", fake_terminate)

    await sup._respawn(reason="child_exited")
    assert old_read.cancelled()
    assert sup.health().state == "STOPPED"
