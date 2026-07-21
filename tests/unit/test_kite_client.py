"""KiteClient market-data passthrough coverage: instruments() (§3.2.2, A10) + the TokenException
circuit breaker (2026-07-21 fix): a rejected-token broker call fires ``on_token_rejected`` exactly
once and ALWAYS re-raises (R5/R8 — never swallow); a non-token error leaves the hook untouched; a
hook that itself raises never masks the original TokenException."""

from __future__ import annotations

from datetime import datetime

import pytest
from kiteconnect.exceptions import TokenException

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


# --------------------------------------------------------------------------- TokenException breaker
class _BoomKC:
    """pykiteconnect stand-in whose historical/order calls raise a configured exception."""

    def __init__(self, exc: Exception) -> None:
        self._exc = exc

    def historical_data(self, instrument_token, from_date, to_date, interval):
        raise self._exc

    def place_order(self, variety, **params):
        raise self._exc


_FRM = datetime(2026, 6, 17, 9, 15, tzinfo=IST)
_TO = datetime(2026, 6, 17, 15, 30, tzinfo=IST)


@pytest.mark.asyncio
async def test_token_exception_fires_hook_once_and_reraises(clock, rate_limiter) -> None:
    """A TokenException on the historical (_call) path AND the order (_order_call) path each fire the
    breaker exactly once, and the exception always propagates."""
    fired: list[int] = []

    async def hook() -> None:
        fired.append(1)

    client = KiteClient(_BoomKC(TokenException("bad token")), rate_limiter, clock, on_token_rejected=hook)

    with pytest.raises(TokenException):
        await client.historical(408065, _FRM, _TO, "minute")
    assert fired == [1]                       # awaited exactly once on the _call path

    with pytest.raises(TokenException):
        await client.place_order({"tradingsymbol": "RELIANCE", "quantity": 1})
    assert fired == [1, 1]                     # and once on the _order_call path


@pytest.mark.asyncio
async def test_non_token_exception_does_not_fire_hook(clock, rate_limiter) -> None:
    fired: list[int] = []

    async def hook() -> None:
        fired.append(1)

    client = KiteClient(_BoomKC(RuntimeError("network blip")), rate_limiter, clock, on_token_rejected=hook)

    with pytest.raises(RuntimeError):
        await client.historical(408065, _FRM, _TO, "minute")
    assert fired == []                         # hook untouched for a non-token error


@pytest.mark.asyncio
async def test_hook_failure_never_masks_the_original_token_exception(clock, rate_limiter) -> None:
    async def hook() -> None:
        raise ValueError("hook itself blew up")

    client = KiteClient(_BoomKC(TokenException("bad token")), rate_limiter, clock, on_token_rejected=hook)

    with pytest.raises(TokenException):        # the ValueError is swallowed+logged, TokenException wins
        await client.historical(408065, _FRM, _TO, "minute")
