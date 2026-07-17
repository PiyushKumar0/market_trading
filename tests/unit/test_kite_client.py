"""KiteClient market-data passthrough coverage: instruments() (§3.2.2, A10)."""

from __future__ import annotations

from datetime import datetime

import pytest

from engine.broker.kite_client import KiteClient
from engine.broker.rate_limiter import RateLimiter
from engine.core.clock import IST, Clock


class _FakeKC:
    """Records the ``exchange`` kwarg it was called with and returns a canned dump."""

    def __init__(self, rows: list) -> None:
        self._rows = rows
        self.last_exchange: str | None = "__unset__"

    def instruments(self, exchange: str | None = None) -> list:
        self.last_exchange = exchange
        return self._rows


@pytest.fixture
def clock() -> Clock:
    return Clock(time_source=lambda: datetime(2026, 6, 17, 10, 0, tzinfo=IST))


@pytest.fixture
def rate_limiter(clock: Clock) -> RateLimiter:
    return RateLimiter(clock, sustained_per_s=1.0, burst=100, entry_calls_per_day=70)


@pytest.mark.asyncio
async def test_instruments_returns_dump(clock: Clock, rate_limiter: RateLimiter) -> None:
    rows = [{"instrument_token": 1, "tradingsymbol": "FOO"}]
    fake_kc = _FakeKC(rows)
    client = KiteClient(fake_kc, rate_limiter, clock)

    result = await client.instruments()

    assert result == rows


@pytest.mark.asyncio
async def test_instruments_forwards_exchange(clock: Clock, rate_limiter: RateLimiter) -> None:
    fake_kc = _FakeKC([])
    client = KiteClient(fake_kc, rate_limiter, clock)

    await client.instruments(exchange="NSE")

    assert fake_kc.last_exchange == "NSE"
