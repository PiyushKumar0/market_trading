"""Order-surface guard (A7, §3.5.3 predicate (a) / B7): position-opening broker calls must be
structurally impossible outside AUTO ∧ NORMAL ∧ in-window. ``KiteClient`` never imports
``engine.risk`` — the guard is a plain callback the wiring layer injects; here we build it the way
ops will (mode-manager + kill-switch check) and prove every gated path routes through it before the
rate limiter is even touched."""

from __future__ import annotations

from collections.abc import Callable

import pytest

from engine.broker.kite_client import KiteClient, OrderSurfaceViolation
from engine.broker.rate_limiter import RateLimiter
from engine.core.enums import Actor, Mode, RiskState
from engine.core.types import OwnerConfirmation
from engine.risk.kill import KillSwitch, KillSwitchEngaged
from engine.risk.mode import ModeManager


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


def _make_guard(mode_manager: ModeManager, kill: KillSwitch, in_window: bool) -> Callable[[str], None]:
    """The order-surface guard the way ops will build it (§3.5.3 predicate (a) / B7): risk-reducing
    intent bypasses both checks entirely (R3 — never gated, not even by the kill switch, which has its
    own risk-reducing flatten exemption); any other intent must clear the kill switch AND
    ``opening_orders_allowed``."""

    def guard(intent: str) -> None:
        if intent != "risk_reducing":
            kill.assert_orders_allowed()
            if not mode_manager.opening_orders_allowed(in_window):
                raise OrderSurfaceViolation(
                    f"order surface blocked: intent={intent!r} mode={mode_manager.mode().value} "
                    f"risk_state={mode_manager.risk_state().value} in_window={in_window}"
                )

    return guard


@pytest.fixture
def rate_limiter(clock) -> RateLimiter:
    # burst=100: the conftest clock is FROZEN, so the token bucket never refills — with the
    # production burst of 2, any test making a 3rd order call sleeps forever waiting for tokens
    # (the 2026-07-27 full-suite wedge). Same idiom as test_rate_limiter.py; pacing itself is
    # exercised there with a ticking clock, not here.
    return RateLimiter(clock, burst=100)


_ENTRY_REQ = {"tradingsymbol": "RELIANCE", "exchange": "NSE", "quantity": 1}


# --------------------------------------------------------------------------- (1) entry blocked
@pytest.mark.asyncio
async def test_entry_blocked_in_recommend_mode(conn, clock, rate_limiter) -> None:
    """B7: zero opening calls in Phase 2 — RECOMMEND is not AUTO, so entry is blocked."""
    mm = ModeManager(conn, clock)
    kill = KillSwitch(conn, clock)
    await mm.request_transition(Mode.RECOMMEND, Actor.OWNER)
    fake_kc = _FakeKC()
    client = KiteClient(fake_kc, rate_limiter, clock, order_guard=_make_guard(mm, kill, in_window=True))

    with pytest.raises(OrderSurfaceViolation):
        await client.place_order(_ENTRY_REQ, intent="entry")
    assert fake_kc.calls == []  # the broker was never touched


@pytest.mark.asyncio
async def test_entry_blocked_in_off_mode(conn, clock, rate_limiter) -> None:
    mm = ModeManager(conn, clock)  # default mode is OFF
    kill = KillSwitch(conn, clock)
    fake_kc = _FakeKC()
    client = KiteClient(fake_kc, rate_limiter, clock, order_guard=_make_guard(mm, kill, in_window=True))

    with pytest.raises(OrderSurfaceViolation):
        await client.place_order(_ENTRY_REQ, intent="entry")
    assert fake_kc.calls == []


@pytest.mark.asyncio
async def test_entry_blocked_in_auto_frozen(conn, clock, rate_limiter) -> None:
    mm = ModeManager(conn, clock)
    kill = KillSwitch(conn, clock)
    await mm.request_transition(Mode.AUTO, Actor.OWNER, OwnerConfirmation(actor=Actor.OWNER, confirmed=True))
    await mm.set_risk_state(RiskState.FROZEN, "stale_feed", Actor.RISK_GATE)
    fake_kc = _FakeKC()
    client = KiteClient(fake_kc, rate_limiter, clock, order_guard=_make_guard(mm, kill, in_window=True))

    with pytest.raises(OrderSurfaceViolation):
        await client.place_order(_ENTRY_REQ, intent="entry")
    assert fake_kc.calls == []


@pytest.mark.asyncio
async def test_entry_blocked_in_auto_normal_outside_window(conn, clock, rate_limiter) -> None:
    mm = ModeManager(conn, clock)
    kill = KillSwitch(conn, clock)
    await mm.request_transition(Mode.AUTO, Actor.OWNER, OwnerConfirmation(actor=Actor.OWNER, confirmed=True))
    fake_kc = _FakeKC()
    client = KiteClient(fake_kc, rate_limiter, clock, order_guard=_make_guard(mm, kill, in_window=False))

    with pytest.raises(OrderSurfaceViolation):
        await client.place_order(_ENTRY_REQ, intent="entry")
    assert fake_kc.calls == []


# --------------------------------------------------------------------------- (2) entry allowed
@pytest.mark.asyncio
async def test_entry_succeeds_in_auto_normal_in_window(conn, clock, rate_limiter) -> None:
    mm = ModeManager(conn, clock)
    kill = KillSwitch(conn, clock)
    await mm.request_transition(Mode.AUTO, Actor.OWNER, OwnerConfirmation(actor=Actor.OWNER, confirmed=True))
    fake_kc = _FakeKC()
    client = KiteClient(fake_kc, rate_limiter, clock, order_guard=_make_guard(mm, kill, in_window=True))

    order_id = await client.place_order(_ENTRY_REQ, intent="entry")

    assert order_id == "ORDER1"
    assert fake_kc.calls == [("place_order", {"variety": "regular", **_ENTRY_REQ})]


# --------------------------------------------------------------------------- (3) risk_reducing bypasses mode
@pytest.mark.asyncio
@pytest.mark.parametrize("start_mode", [None, Mode.RECOMMEND], ids=["off", "recommend"])
async def test_risk_reducing_bypasses_mode_gate(conn, clock, rate_limiter, start_mode) -> None:
    """R3 mode invariant: risk-reducing place/modify/cancel succeed regardless of mode — OFF and
    RECOMMEND included — even with ``in_window=False`` and no mode transition to AUTO."""
    mm = ModeManager(conn, clock)
    kill = KillSwitch(conn, clock)
    if start_mode is not None:
        await mm.request_transition(start_mode, Actor.OWNER)
    fake_kc = _FakeKC()
    client = KiteClient(fake_kc, rate_limiter, clock, order_guard=_make_guard(mm, kill, in_window=False))

    await client.place_order({"tradingsymbol": "RELIANCE", "quantity": 1}, intent="risk_reducing")
    await client.modify_order("OID1", {"quantity": 2}, intent="risk_reducing")
    await client.cancel_order("OID1", intent="risk_reducing")

    assert [op for op, _ in fake_kc.calls] == ["place_order", "modify_order", "cancel_order"]


# --------------------------------------------------------------------------- (4) killed blocks entry
@pytest.mark.asyncio
async def test_killed_blocks_entry_even_in_auto_normal_in_window(conn, clock, rate_limiter) -> None:
    mm = ModeManager(conn, clock)
    kill = KillSwitch(conn, clock)
    await mm.request_transition(Mode.AUTO, Actor.OWNER, OwnerConfirmation(actor=Actor.OWNER, confirmed=True))
    await kill.trigger("test_kill", actor=Actor.OWNER, flatten=False)
    fake_kc = _FakeKC()
    client = KiteClient(fake_kc, rate_limiter, clock, order_guard=_make_guard(mm, kill, in_window=True))

    with pytest.raises(KillSwitchEngaged):
        await client.place_order(_ENTRY_REQ, intent="entry")
    assert fake_kc.calls == []


# --------------------------------------------------------------------------- (5) no guard => back-compat
@pytest.mark.asyncio
async def test_no_guard_wired_entry_passes(conn, clock, rate_limiter) -> None:
    """Unwired guard (None, the default) => current behaviour unchanged (unit tests / scripts)."""
    fake_kc = _FakeKC()
    client = KiteClient(fake_kc, rate_limiter, clock)  # order_guard omitted

    order_id = await client.place_order(_ENTRY_REQ, intent="entry")

    assert order_id == "ORDER1"
    assert fake_kc.calls == [("place_order", {"variety": "regular", **_ENTRY_REQ})]


# --------------------------------------------------------------------------- GTT paths: no special-casing
@pytest.mark.asyncio
async def test_gtt_calls_route_through_the_guard_with_their_own_intent(conn, clock, rate_limiter) -> None:
    """place_gtt/modify_gtt/delete_gtt carry ``intent='risk_reducing'`` already — KiteClient does not
    special-case GTT-as-opening; the guard sees the same intent string it always would and decides.
    Proven here in OFF mode (would block ``intent='entry'``) where the calls still succeed because the
    guard receives ``'risk_reducing'``, and the guard is provably invoked (recorded) for each of them."""
    mm = ModeManager(conn, clock)  # OFF — would block an 'entry' intent
    kill = KillSwitch(conn, clock)
    seen_intents: list[str] = []
    base_guard = _make_guard(mm, kill, in_window=False)

    def spying_guard(intent: str) -> None:
        seen_intents.append(intent)
        base_guard(intent)

    fake_kc = _FakeKC()
    client = KiteClient(fake_kc, rate_limiter, clock, order_guard=spying_guard)

    await client.place_gtt({"trigger_values": [100]})
    await client.modify_gtt(1, {"trigger_values": [105]})
    await client.delete_gtt(1)

    assert seen_intents == ["risk_reducing", "risk_reducing", "risk_reducing"]
    assert [op for op, _ in fake_kc.calls] == ["place_gtt", "modify_gtt", "delete_gtt"]


@pytest.mark.asyncio
async def test_guard_runs_before_rate_limiter_is_acquired(conn, clock, rate_limiter) -> None:
    """The guard fires BEFORE the limiter acquire — a blocked call must not consume/pace the budget."""
    mm = ModeManager(conn, clock)  # OFF: entry is always blocked
    kill = KillSwitch(conn, clock)
    fake_kc = _FakeKC()
    client = KiteClient(fake_kc, rate_limiter, clock, order_guard=_make_guard(mm, kill, in_window=True))

    before = rate_limiter.orders_today()
    with pytest.raises(OrderSurfaceViolation):
        await client.place_order(_ENTRY_REQ, intent="entry")
    after = rate_limiter.orders_today()

    assert before == after == (0, 0)
