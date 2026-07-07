"""Order-rate split (R3 / §7.1 ``order_rate``): entry hard-capped, risk-reducing never starved."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta

import pytest

from engine.broker.rate_limiter import EntryBudgetExhausted, RateLimiter
from engine.core.clock import IST, Clock


class _FakeTime:
    """A controllable time source so refill/pacing are testable without real-time sleeps."""

    def __init__(self, start: datetime) -> None:
        self.t = start

    def __call__(self) -> datetime:
        return self.t

    def advance(self, secs: float) -> None:
        self.t = self.t + timedelta(seconds=secs)


@pytest.mark.asyncio
async def test_entry_budget_hard_capped(monkeypatch):
    # Large burst => no pacing needed; small entry cap => budget logic isolated from timing.
    clock = Clock(time_source=lambda: datetime(2026, 6, 17, 10, 0, tzinfo=IST))
    rl = RateLimiter(clock, sustained_per_s=1.0, burst=100, entry_calls_per_day=3)

    for _ in range(3):
        await rl.acquire("orders", intent="entry")
    assert rl.orders_today() == (3, 0)

    # 4th entry call is rejected — the entry budget is exhausted (B3 / §7.1).
    with pytest.raises(EntryBudgetExhausted):
        await rl.acquire("orders", intent="entry")


@pytest.mark.asyncio
async def test_risk_reducing_never_rejected_even_when_entry_exhausted():
    clock = Clock(time_source=lambda: datetime(2026, 6, 17, 10, 0, tzinfo=IST))
    rl = RateLimiter(clock, sustained_per_s=1.0, burst=100, entry_calls_per_day=2)

    await rl.acquire("orders", intent="entry")
    await rl.acquire("orders", intent="entry")
    with pytest.raises(EntryBudgetExhausted):
        await rl.acquire("orders", intent="entry")

    # The flatten lane keeps working with the entry budget fully exhausted (R3 — never starved).
    for _ in range(10):
        await rl.acquire("orders", intent="risk_reducing")
    assert rl.orders_today() == (2, 10)


@pytest.mark.asyncio
async def test_reset_day_zeroes_counters():
    clock = Clock(time_source=lambda: datetime(2026, 6, 17, 10, 0, tzinfo=IST))
    rl = RateLimiter(clock, sustained_per_s=1.0, burst=100, entry_calls_per_day=70)
    await rl.acquire("orders", intent="entry")
    await rl.acquire("orders", intent="risk_reducing")
    assert rl.orders_today() == (1, 1)
    rl.reset_day()
    assert rl.orders_today() == (0, 0)


@pytest.mark.asyncio
async def test_pacing_waits_for_refill(monkeypatch):
    # An advancing clock + a fake sleep that advances it => verify pacing without real-time waits.
    ft = _FakeTime(datetime(2026, 6, 17, 10, 0, tzinfo=IST))
    clock = Clock(time_source=ft)

    async def fake_sleep(secs: float) -> None:
        ft.advance(secs)

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    rl = RateLimiter(clock, sustained_per_s=1.0, burst=2, entry_calls_per_day=100)
    # Burst of 2 is instant; the 3rd must pace ~1 s (the fake sleep advances the clock so refill happens).
    await rl.acquire("orders", intent="entry")
    await rl.acquire("orders", intent="entry")
    t_before = ft.t
    await rl.acquire("orders", intent="entry")   # forces a refill wait
    assert ft.t >= t_before + timedelta(seconds=0.9)   # the limiter paced ~1 s of (simulated) time
    assert rl.orders_today() == (3, 0)
