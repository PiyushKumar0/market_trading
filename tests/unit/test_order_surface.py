"""Order-surface guard (A7, §3.5.3 predicate (a) / B7): position-opening broker calls must be
structurally impossible outside AUTO ∧ NORMAL ∧ in-window. ``KiteClient`` never imports
``engine.risk`` — the guard is a plain callback the wiring layer injects; here we use the composition
root's own ``make_order_guard`` and prove every gated path routes through it before the rate limiter
is even touched. D8: a client built without ``orders_enabled`` refuses every order/GTT call outright."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime

import pytest

from engine.broker.kite_client import KiteClient, OrderSurfaceViolation
from engine.broker.rate_limiter import RateLimiter
from engine.core.calendar import NSECalendar
from engine.core.clock import IST, Clock
from engine.core.config import config_dir
from engine.core.enums import Actor, Mode, RiskState
from engine.core.types import OwnerConfirmation
from engine.ops.main import make_order_guard
from engine.risk.kill import KillSwitch, KillSwitchEngaged
from engine.risk.mode import ModeManager

#: The guard's clock outside the default 10:00–10:30 trade window (conftest's clock is 10:05, inside).
_OUTSIDE = Clock(time_source=lambda: datetime(2026, 6, 17, 11, 0, tzinfo=IST))


class _FakeKC:
    """Records every order/GTT call it receives; never talks to a real broker."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []

    def place_order(self, variety: str, **params) -> str:
        self.calls.append(("place_order", {"variety": variety, **params}))
        return "ORDER1"

    def modify_order(self, variety: str, order_id: str, **params) -> str:
        self.calls.append(("modify_order", {"variety": variety, "order_id": order_id, **params}))
        return order_id

    def cancel_order(self, variety: str, order_id: str) -> str:
        self.calls.append(("cancel_order", {"variety": variety, "order_id": order_id}))
        return order_id

    def place_gtt(self, **params) -> int:
        self.calls.append(("place_gtt", params))
        return 1

    def modify_gtt(self, trigger_id: int, **params) -> int:
        self.calls.append(("modify_gtt", {"trigger_id": trigger_id, **params}))
        return trigger_id

    def delete_gtt(self, trigger_id: int) -> None:
        self.calls.append(("delete_gtt", {"trigger_id": trigger_id}))


def _guard(conn, mm: ModeManager, kill: KillSwitch, clock: Clock) -> Callable[[str], None]:
    return make_order_guard(mm, kill, NSECalendar(config_dir() / "calendar", clock, sqlite_conn=conn), clock)


def _client(fake_kc: _FakeKC, rate_limiter: RateLimiter, clock: Clock, guard=None) -> KiteClient:
    return KiteClient(fake_kc, rate_limiter, clock, order_guard=guard, orders_enabled=True)


async def _auto(mm: ModeManager) -> None:
    await mm.request_transition(Mode.AUTO, Actor.OWNER, OwnerConfirmation(actor=Actor.OWNER, confirmed=True))


@pytest.fixture
def mm(conn, clock) -> ModeManager:
    return ModeManager(conn, clock, paper_only=False)  # default mode is OFF


@pytest.fixture
def kill(conn, clock) -> KillSwitch:
    return KillSwitch(conn, clock)


@pytest.fixture
def rate_limiter(clock) -> RateLimiter:
    # burst=100: the conftest clock is FROZEN, so the token bucket never refills — with the
    # production burst of 2, any test making a 3rd order call sleeps forever waiting for tokens
    # (the 2026-07-27 full-suite wedge). Same idiom as test_rate_limiter.py; pacing itself is
    # exercised there with a ticking clock, not here.
    return RateLimiter(clock, burst=100)


_ENTRY_REQ = {"tradingsymbol": "RELIANCE", "exchange": "NSE", "quantity": 1}

_ORDER_CALLS = {
    "place_order": lambda c: c.place_order(_ENTRY_REQ, intent="risk_reducing"),
    "modify_order": lambda c: c.modify_order("OID1", {"quantity": 2}),
    "cancel_order": lambda c: c.cancel_order("OID1"),
    "place_gtt": lambda c: c.place_gtt({"trigger_values": [100]}),
    "modify_gtt": lambda c: c.modify_gtt(1, {"trigger_values": [105]}),
    "delete_gtt": lambda c: c.delete_gtt(1),
}


# --------------------------------------------------------------------------- (0) D8: orders disabled
@pytest.mark.asyncio
@pytest.mark.parametrize("op", sorted(_ORDER_CALLS))
async def test_orders_disabled_refuses_before_guard_and_limiter(clock, rate_limiter, op) -> None:
    guard_calls: list[str] = []
    fake_kc = _FakeKC()
    client = KiteClient(fake_kc, rate_limiter, clock, order_guard=guard_calls.append)

    with pytest.raises(OrderSurfaceViolation, match="orders_enabled"):
        await _ORDER_CALLS[op](client)
    assert (fake_kc.calls, guard_calls, rate_limiter.orders_today()) == ([], [], (0, 0))


# --------------------------------------------------------------------------- (1) entry blocked
@pytest.mark.asyncio
async def test_entry_blocked_in_recommend_mode(conn, clock, rate_limiter, mm, kill) -> None:
    """B7: zero opening calls in Phase 2 — RECOMMEND is not AUTO, so entry is blocked."""
    await mm.request_transition(Mode.RECOMMEND, Actor.OWNER)
    fake_kc = _FakeKC()
    client = _client(fake_kc, rate_limiter, clock, _guard(conn, mm, kill, clock))

    with pytest.raises(OrderSurfaceViolation):
        await client.place_order(_ENTRY_REQ, intent="entry")
    assert fake_kc.calls == []  # the broker was never touched


@pytest.mark.asyncio
async def test_entry_blocked_in_off_mode(conn, clock, rate_limiter, mm, kill) -> None:
    fake_kc = _FakeKC()
    client = _client(fake_kc, rate_limiter, clock, _guard(conn, mm, kill, clock))

    with pytest.raises(OrderSurfaceViolation):
        await client.place_order(_ENTRY_REQ, intent="entry")
    assert fake_kc.calls == []


@pytest.mark.asyncio
async def test_entry_blocked_in_auto_frozen(conn, clock, rate_limiter, mm, kill) -> None:
    await _auto(mm)
    await mm.set_risk_state(RiskState.FROZEN, "stale_feed", Actor.RISK_GATE)
    fake_kc = _FakeKC()
    client = _client(fake_kc, rate_limiter, clock, _guard(conn, mm, kill, clock))

    with pytest.raises(OrderSurfaceViolation):
        await client.place_order(_ENTRY_REQ, intent="entry")
    assert fake_kc.calls == []


@pytest.mark.asyncio
async def test_entry_blocked_in_auto_normal_outside_window(conn, clock, rate_limiter, mm, kill) -> None:
    await _auto(mm)
    fake_kc = _FakeKC()
    client = _client(fake_kc, rate_limiter, clock, _guard(conn, mm, kill, _OUTSIDE))

    with pytest.raises(OrderSurfaceViolation, match="in_window=False"):
        await client.place_order(_ENTRY_REQ, intent="entry")
    assert fake_kc.calls == []


# --------------------------------------------------------------------------- (2) entry allowed
@pytest.mark.asyncio
async def test_entry_succeeds_in_auto_normal_in_window(conn, clock, rate_limiter, mm, kill) -> None:
    await _auto(mm)
    fake_kc = _FakeKC()
    client = _client(fake_kc, rate_limiter, clock, _guard(conn, mm, kill, clock))

    order_id = await client.place_order(_ENTRY_REQ, intent="entry")

    assert order_id == "ORDER1"
    assert fake_kc.calls == [("place_order", {"variety": "regular", **_ENTRY_REQ})]


# --------------------------------------------------------------------------- (3) risk_reducing bypasses mode
@pytest.mark.asyncio
@pytest.mark.parametrize("start_mode", [None, Mode.RECOMMEND], ids=["off", "recommend"])
async def test_risk_reducing_bypasses_mode_gate(conn, clock, rate_limiter, mm, kill, start_mode) -> None:
    """R3 mode invariant: risk-reducing place/modify/cancel succeed regardless of mode — OFF and
    RECOMMEND included — even outside the window and with no mode transition to AUTO."""
    if start_mode is not None:
        await mm.request_transition(start_mode, Actor.OWNER)
    fake_kc = _FakeKC()
    client = _client(fake_kc, rate_limiter, clock, _guard(conn, mm, kill, _OUTSIDE))

    await client.place_order({"tradingsymbol": "RELIANCE", "quantity": 1}, intent="risk_reducing")
    await client.modify_order("OID1", {"quantity": 2}, intent="risk_reducing")
    await client.cancel_order("OID1", intent="risk_reducing")

    assert [op for op, _ in fake_kc.calls] == ["place_order", "modify_order", "cancel_order"]


# --------------------------------------------------------------------------- (4) killed blocks entry
@pytest.mark.asyncio
async def test_killed_blocks_entry_even_in_auto_normal_in_window(conn, clock, rate_limiter, mm, kill) -> None:
    await _auto(mm)
    await kill.trigger("test_kill", actor=Actor.OWNER, flatten=False)
    fake_kc = _FakeKC()
    client = _client(fake_kc, rate_limiter, clock, _guard(conn, mm, kill, clock))

    with pytest.raises(KillSwitchEngaged):
        await client.place_order(_ENTRY_REQ, intent="entry")
    assert fake_kc.calls == []


# --------------------------------------------------------------------------- (5) no guard => back-compat
@pytest.mark.asyncio
async def test_no_guard_wired_entry_passes(clock, rate_limiter) -> None:
    """Unwired guard (None, the default) => no gating beyond ``orders_enabled``."""
    fake_kc = _FakeKC()
    client = _client(fake_kc, rate_limiter, clock)

    order_id = await client.place_order(_ENTRY_REQ, intent="entry")

    assert order_id == "ORDER1"
    assert fake_kc.calls == [("place_order", {"variety": "regular", **_ENTRY_REQ})]


# --------------------------------------------------------------------------- GTT paths: no special-casing
@pytest.mark.asyncio
async def test_gtt_calls_route_through_the_guard_with_their_own_intent(conn, clock, rate_limiter, mm, kill) -> None:
    """place_gtt/modify_gtt/delete_gtt carry ``intent='risk_reducing'`` already — KiteClient does not
    special-case GTT-as-opening; the guard sees the same intent string it always would and decides.
    Proven here in OFF mode (would block ``intent='entry'``) where the calls still succeed because the
    guard receives ``'risk_reducing'``, and the guard is provably invoked (recorded) for each of them."""
    seen_intents: list[str] = []
    base_guard = _guard(conn, mm, kill, _OUTSIDE)

    def spying_guard(intent: str) -> None:
        seen_intents.append(intent)
        base_guard(intent)

    fake_kc = _FakeKC()
    client = _client(fake_kc, rate_limiter, clock, spying_guard)

    await client.place_gtt({"trigger_values": [100]})
    await client.modify_gtt(1, {"trigger_values": [105]})
    await client.delete_gtt(1)

    assert seen_intents == ["risk_reducing", "risk_reducing", "risk_reducing"]
    assert [op for op, _ in fake_kc.calls] == ["place_gtt", "modify_gtt", "delete_gtt"]


@pytest.mark.asyncio
async def test_guard_runs_before_rate_limiter_is_acquired(conn, clock, rate_limiter, mm, kill) -> None:
    """The guard fires BEFORE the limiter acquire — a blocked call must not consume/pace the budget."""
    fake_kc = _FakeKC()
    client = _client(fake_kc, rate_limiter, clock, _guard(conn, mm, kill, clock))  # OFF: entry blocked

    before = rate_limiter.orders_today()
    with pytest.raises(OrderSurfaceViolation):
        await client.place_order(_ENTRY_REQ, intent="entry")
    after = rate_limiter.orders_today()

    assert before == after == (0, 0)
