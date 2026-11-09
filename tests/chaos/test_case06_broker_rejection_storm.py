"""Chaos case 6 — broker rejection storm (incl. 429s).

Failure injected: a storm of broker rejections / HTTP 429s. Must hold: no order spam (every attempt
paced inside the rate budget); ≥3 rejects/60 s ⇒ FROZEN + alert; with the 70/day entry budget exhausted,
kill-flatten / square-off / exit still places from the uncapped-but-paced risk-reducing lane.

Composition as ``engine.ops.main`` wires it: real ``RateLimiter`` (1 order call/s, burst 2, 70 entry
calls/day), ``KiteClient`` (``orders_enabled=True``, which production never passes), ``ModeManager`` /
``KillSwitch`` / ``RiskStateLatch`` with the kill→latch bridge, and ``make_order_guard``. The only fake is
the pykiteconnect ``KiteConnect`` answering from a script. ``asyncio.sleep`` advances a controllable
clock, so pacing is asserted on simulated time.

Skipped: the real-path storm freeze (``CAUSE_REJECTION_STORM`` on the real latch) and a retry/backoff
policy wait for the live AUTO OMS; ``KiteClient`` re-raises every broker error and nothing retries.
"""

from __future__ import annotations

import asyncio
import random
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest
from kiteconnect.exceptions import NetworkException

from engine.broker.kite_client import KiteClient
from engine.broker.rate_limiter import EntryBudgetExhausted, RateLimiter
from engine.core.calendar import NSECalendar
from engine.core.clock import IST, Clock
from engine.core.config import config_dir
from engine.core.contracts import PAPER_ORDER_UPDATE_TOPIC, EnterAction, GateVerdict
from engine.core.enums import Actor, Mode, RiskState
from engine.core.eventbus import EventBus
from engine.core.types import OwnerConfirmation
from engine.oms.manager import OrderManager, paper_order_guard
from engine.oms.state import OrderState
from engine.oms.store import OrderStore
from engine.ops.main import make_order_guard
from engine.ops.paper_risk import STORM_REJECTS, STORM_WINDOW, PaperRisk, active_halts, paper_effective_state
from engine.paper.broker import PaperBroker
from engine.paper.fill_model import FillModelConfig
from engine.risk.causes import RiskStateLatch
from engine.risk.events import TOPIC_KILL_STATE
from engine.risk.kill import KillSwitch, KillSwitchEngaged
from engine.risk.mode import ModeManager
from tests.chaos.conftest import LIVE_ORDER_PATH_GATED

#: Inside the seeded 10:00–10:30 trade window (NSECalendar default seed).
START = datetime(2026, 6, 17, 10, 5, tzinfo=IST)
OWNER_OK = OwnerConfirmation(actor=Actor.OWNER, confirmed=True, note="chaos case 6")
ENTRY_REQ = {"tradingsymbol": "RELIANCE", "exchange": "NSE", "transaction_type": "BUY", "quantity": 1,
             "order_type": "LIMIT", "product": "MIS", "price": 1420.5}
EXIT_REQ = {"tradingsymbol": "RELIANCE", "exchange": "NSE", "transaction_type": "SELL", "quantity": 1,
            "order_type": "MARKET", "product": "MIS", "market_protection": -1}
#: The shipped ``order_rate`` numbers the default ``RateLimiter(clock)`` carries.
ENTRY_BUDGET = 70
BURST = 2
ONE_S = timedelta(seconds=1)


def _http_429() -> NetworkException:
    """What pykiteconnect raises for a Kite HTTP 429."""
    return NetworkException("Too many requests", code=429)


# --------------------------------------------------------------------------- the broker boundary
class _Ticker:
    """Controllable time source; the patched ``asyncio.sleep`` advances it (pacing on sim time)."""

    def __init__(self, start: datetime) -> None:
        self.at = start

    def __call__(self) -> datetime:
        return self.at

    def advance(self, secs: float) -> None:
        self.at = self.at + timedelta(seconds=secs)


class _ScriptedKiteConnect:
    """Stand-in for pykiteconnect ``KiteConnect``.

    Each call pops the next scripted outcome (an exception to raise, or the id to return; an empty
    script answers with a fresh id) and records ``(op, sim_time, params)``. Calls arrive strictly one at
    a time, so reading the ticker from the executor thread is race-free.
    """

    def __init__(self, ticker: _Ticker, script: list[Any] | None = None) -> None:
        self._ticker = ticker
        self.script = list(script or [])
        self.calls: list[tuple[str, datetime, dict[str, Any]]] = []

    def _answer(self, op: str, params: dict[str, Any]) -> Any:
        self.calls.append((op, self._ticker.at, params))
        outcome = self.script.pop(0) if self.script else f"OID{len(self.calls)}"
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    def place_order(self, variety: str, **params: Any) -> str:
        return self._answer("place_order", {"variety": variety, **params})

    def cancel_order(self, variety: str, order_id: str) -> str:
        return self._answer("cancel_order", {"variety": variety, "order_id": order_id})

    def place_gtt(self, **params: Any) -> int:
        self._answer("place_gtt", params)
        return 1


# --------------------------------------------------------------------------- composition
class _World:
    def __init__(self, conn, ticker: _Ticker, script: list[Any] | None = None) -> None:
        self.ticker = ticker
        self.clock = Clock(time_source=ticker)
        self.bus = EventBus()
        self.calendar = NSECalendar(config_dir() / "calendar", self.clock, sqlite_conn=conn)
        self.mode = ModeManager(conn, self.clock, self.bus, self.calendar, paper_only=False)
        self.latch = RiskStateLatch(conn, self.clock, self.mode)
        self.kill = KillSwitch(conn, self.clock, self.bus)
        # The kill ⇄ cause-latch bridge (as main.py), so risk_state reads KILLED after a kill.

        async def _kill_to_latch(evt: Any) -> None:
            if evt.killed:
                await self.latch.set_cause("kill", RiskState.KILLED, evt.reason or "kill", evt.actor)
            else:
                await self.latch.clear_cause("kill", evt.actor)

        self.bus.subscribe(TOPIC_KILL_STATE, _kill_to_latch)
        self.limiter = RateLimiter(self.clock)          # shipped risk-limit defaults
        self.kc = _ScriptedKiteConnect(ticker, script)
        self.client = KiteClient(
            self.kc, self.limiter, self.clock,
            order_guard=make_order_guard(self.mode, self.kill, self.calendar, self.clock),
            orders_enabled=True,
        )

    async def go_auto(self) -> None:
        await self.mode.request_transition(Mode.AUTO, Actor.OWNER, OWNER_OK)
        assert self.mode.opening_orders_allowed(True)


@pytest.fixture
def ticker(monkeypatch) -> _Ticker:
    t = _Ticker(START)

    async def _sim_sleep(secs: float) -> None:
        t.advance(secs)

    monkeypatch.setattr(asyncio, "sleep", _sim_sleep)
    return t


def _broker_times(kc: _ScriptedKiteConnect, *ops: str) -> list[datetime]:
    return [at for op, at, _ in kc.calls if not ops or op in ops]


def _assert_paced(times: list[datetime], *, already_drained: bool = False) -> None:
    """After the burst, consecutive broker hits are ≥ 1 s apart and no rolling 1-s window holds more than the burst."""
    slack = timedelta(milliseconds=1)             # float refill arithmetic, never a real gap
    first_paced = 1 if already_drained else BURST
    for i in range(first_paced, len(times)):
        assert times[i] - times[i - 1] >= ONE_S - slack, (i, times[i - 1], times[i])
    for i, t in enumerate(times):
        in_window = sum(1 for u in times[i:] if u - t < ONE_S - slack)
        assert in_window <= BURST, (t, in_window)


# --------------------------------------------------------------------------- clauses
async def test_429_storm_every_attempt_is_paced_and_never_amplified(conn, ticker) -> None:
    """A caller retrying through a wall of 429s produces exactly one broker hit per attempt, each paced
    by the shared 1/s order bucket and charged to the entry budget: the storm never becomes spam."""
    attempts = 12
    world = _World(conn, ticker, script=[_http_429() for _ in range(attempts)])
    await world.go_auto()

    for _ in range(attempts):
        with pytest.raises(NetworkException) as exc_info:
            await world.client.place_order(ENTRY_REQ, intent="entry")
        assert exc_info.value.code == 429           # re-raised verbatim, never swallowed

    times = _broker_times(world.kc)
    assert len(world.kc.calls) == attempts          # one broker hit per attempt — no hidden retry
    assert world.limiter.orders_today() == (attempts, 0)   # every attempt is inside the rate budget
    _assert_paced(times)
    # 12 attempts through a burst-2 / 1-per-second bucket span at least 10 s of (simulated) time.
    assert times[-1] - times[0] >= timedelta(seconds=attempts - BURST) - timedelta(milliseconds=1)


async def test_entry_budget_exhausted_flatten_lane_never_starved(conn, ticker) -> None:
    """With the 70/day entry budget spent and the kill thrown, every risk-reducing call (exits, cancels,
    GTT; a 429 retry included) still places, paced behind the same 1/s bucket, never budget-rejected
    or blocked by the kill switch."""
    world = _World(conn, ticker)
    await world.go_auto()

    for _ in range(ENTRY_BUDGET):
        await world.client.place_order(ENTRY_REQ, intent="entry")
    assert world.limiter.orders_today() == (ENTRY_BUDGET, 0)
    entry_hits = len(world.kc.calls)
    assert entry_hits == ENTRY_BUDGET
    _assert_paced(_broker_times(world.kc))

    # The 71st entry is refused by the budget, before the broker.
    with pytest.raises(EntryBudgetExhausted):
        await world.client.place_order(ENTRY_REQ, intent="entry")
    assert len(world.kc.calls) == entry_hits

    await world.kill.trigger("chaos case 6: rejection storm", actor=Actor.OWNER, flatten=False)
    assert world.mode.risk_state() == RiskState.KILLED
    with pytest.raises(KillSwitchEngaged):
        await world.client.place_order(ENTRY_REQ, intent="entry")
    assert len(world.kc.calls) == entry_hits

    # Flatten lane: the first square-off meets a 429 and the caller retries it.
    world.kc.script = [_http_429()]
    flatten_started = world.ticker.at
    with pytest.raises(NetworkException):
        await world.client.place_order(EXIT_REQ, intent="risk_reducing")
    for _ in range(4):                                            # retry + three more exits
        await world.client.place_order(EXIT_REQ, intent="risk_reducing")
    for oid in ("OID1", "OID2", "OID3"):                          # cancel resting entry orders
        await world.client.cancel_order(oid)                      # default intent = risk_reducing
    await world.client.place_gtt({"trigger_type": "single", "tradingsymbol": "RELIANCE"})

    lane = [(op, at) for op, at, _ in world.kc.calls[entry_hits:]]
    assert [op for op, _ in lane] == ["place_order"] * 5 + ["cancel_order"] * 3 + ["place_gtt"]
    assert world.limiter.orders_today() == (ENTRY_BUDGET, 9)      # uncapped lane, still counted
    # Same bucket the entries drained: no burst left, so ≥ 1 s between every hit, the 429 retry included.
    lane_times = [at for _, at in lane]
    assert lane_times[0] >= flatten_started
    _assert_paced(lane_times, already_drained=True)
    assert lane_times[1] - lane_times[0] >= ONE_S - timedelta(milliseconds=1)


# --------------------------------------------------------------------------- the paper path
#: High enough that a short search finds a seed rejecting exactly the first ``STORM_REJECTS`` placements.
_RATE = 0.9


def _seed_rejecting_first(n: int) -> int:
    """A PaperBroker seed whose injected-rejection draws reject placements 1..n and accept n+1 (the broker
    draws once per placement that passes its guard)."""
    for seed in range(10_000):
        rng = random.Random(seed)
        draws = [rng.random() for _ in range(n + 1)]
        if all(d < _RATE for d in draws[:n]) and draws[n] >= _RATE:
            return seed
    raise AssertionError("no seed found")


def _seed_verdict(conn, n: int) -> tuple[EnterAction, GateVerdict]:
    proposal = EnterAction(
        action="enter", proposal_id=f"P{n}", thesis="chaos case 6 paper storm thesis", confidence=0.6,
        tradingsymbol="TCS", exchange="NSE", side="BUY", style="swing", entry_type="MARKET",
        stop_price=Decimal("95"), quantity=10, signal_id="s", strategy_id="hi52", features_snapshot_id="f",
    )
    verdict = GateVerdict(
        verdict_id=f"V{n}", proposal_id=proposal.proposal_id, verdict="approve", original_qty=10,
        approved_qty=10, checks=[], mode=Mode.RECOMMEND, risk_state=RiskState.NORMAL,
        degrade_tier="DG0", evaluated_at=START,
    )
    conn.execute("INSERT INTO proposals (proposal_id, agent_id, action, payload, inputs_digest, created_at) "
                 "VALUES (?, 'a', 'enter', ?, 'd', ?)", (proposal.proposal_id, proposal.model_dump_json(),
                                                       START.isoformat()))
    conn.execute("INSERT INTO verdicts (verdict_id, proposal_id, verdict, payload, evaluated_at, is_paper) "
                 "VALUES (?, ?, 'approve', ?, ?, 1)",
                 (verdict.verdict_id, proposal.proposal_id, verdict.model_dump_json(), START.isoformat()))
    return proposal, verdict


async def _settle(bus: EventBus, orders: OrderManager) -> None:
    for _ in range(3):
        await bus.drain(1.0)
        await orders.join()


def _never(*_args: Any) -> Any:
    raise AssertionError("the storm path never reads limits or flattens the book")


async def test_paper_three_broker_rejects_in_60s_freeze_paper_entries_and_exits_still_place(
    conn, bus, caplog
) -> None:
    """Three broker rejects inside 60 s ⇒ the daily ``rejection_storm`` paper halt (FROZEN) + its WARNING; the
    next entry never reaches the broker, an exit still places, and the real risk latch never moves."""
    ticker = _Ticker(START)
    clock = Clock(time_source=ticker)
    mode = ModeManager(conn, clock, paper_only=False)
    RiskStateLatch(conn, clock, mode)
    guard = paper_order_guard(lambda: True, paper_effective_state(conn, mode.risk_state))
    broker = PaperBroker(clock, bus.publish, FillModelConfig(), lambda _s: Decimal("0.05"),
                         _seed_rejecting_first(STORM_REJECTS), rejection_rate=_RATE, order_guard=guard,
                         topic=PAPER_ORDER_UPDATE_TOPIC)
    risk = PaperRisk(conn, clock, NSECalendar(config_dir() / "calendar", clock, strict=False).session, broker,
                     capital_base=Decimal("40000"), mark_price=lambda _s: None, limits_fn=_never, flatten=_never)
    orders = OrderManager(conn, clock, broker, order_guard=guard, sell_check=lambda *_: None,
                          on_fill=lambda *_: None, on_reject=lambda _order, at: risk.note_reject(at))
    store = OrderStore(conn)
    orders.start(bus)
    try:
        with caplog.at_level("WARNING", logger="engine.ops.paper_risk"):
            step = STORM_WINDOW / STORM_REJECTS                       # 20 s apart: all inside 60 s
            for n in range(STORM_REJECTS):
                assert active_halts(conn) == {}
                ticker.at = START + n * step
                entry = await orders.submit(*_seed_verdict(conn, n))
                await _settle(bus, orders)
                assert store.get(entry.order_id).state is OrderState.REJECTED
                assert "injected rejection" in store.get(entry.order_id).reject_reason
        assert active_halts(conn) == {"rejection_storm": RiskState.FROZEN}
        assert paper_effective_state(conn, mode.risk_state)() is RiskState.FROZEN
        [alert] = [r for r in caplog.records if r.getMessage() == "paper_halt_set"]
        assert alert.cause == "rejection_storm" and alert.reason.startswith("3 paper broker rejects in 40 s")
        assert mode.risk_state() is RiskState.NORMAL
        assert conn.execute("SELECT COUNT(*) FROM risk_state_causes").fetchone()[0] == 0

        # The paper guard refuses the next entry before the broker: no fourth placement.
        ticker.at = START + timedelta(seconds=55)
        blocked = await orders.submit(*_seed_verdict(conn, STORM_REJECTS))
        assert blocked.state is OrderState.REJECTED and "FROZEN" in (blocked.reject_reason or "")
        assert [(e.from_state, e.to_state) for e in store.events(blocked.order_id)] == [
            (OrderState.DRAFT, OrderState.REJECTED)]
        assert len(await broker.orders()) == STORM_REJECTS

        # A risk-reducing exit still passes the guard and rests at the broker.
        conn.execute(
            "INSERT INTO positions (position_id, symbol, side, product, qty, avg_entry, stop, state, "
            "protection_state, is_paper, origin, opened_at) VALUES ('POS1', 'TCS', 'BUY', 'CNC', 10, '100', '95', "
            "'OPEN', 'PROTECTED', 1, 'platform', ?)", ((START - timedelta(days=1)).isoformat(),))
        exit_order = await orders.submit_exit("POS1", 10, price=Decimal("120"))
        await _settle(bus, orders)
        assert store.get(exit_order.order_id).state is OrderState.ACKED
        assert [o["transaction_type"] for o in await broker.orders()] == ["BUY"] * STORM_REJECTS + ["SELL"]
        assert active_halts(conn) == {"rejection_storm": RiskState.FROZEN}    # one row, not re-latched
    finally:
        await orders.stop()


@pytest.mark.skip(reason=(
    "Real path, lands with the live AUTO OMS: real orders are structurally impossible today "
    "(KiteClient refuses every order without orders_enabled=True, which production never passes), "
    "so CAUSE_REJECTION_STORM (engine.risk.causes) on the real RiskStateLatch, its owner /resume_entries "
    "clear, and the >=3-storms-per-session kill escalation have no order path to observe. The PAPER path "
    "is built and asserted by "
    "test_paper_three_broker_rejects_in_60s_freeze_paper_entries_and_exits_still_place."
))
async def test_three_rejects_in_60s_freeze_entries_and_alert() -> None:
    """Real path: ≥3 broker rejects inside 60 s latch CAUSE_REJECTION_STORM (FROZEN) + owner alert;
    ≥3 storms in a session ⇒ kill."""


@pytest.mark.skip(reason=(
    "UNBUILT: retry/backoff policy — KiteClient re-raises every broker error including HTTP 429 "
    "(KiteClient._order_call's except/re-raise) and no caller retries or backs off; the OMS order "
    "executor that would own the policy is the unbuilt live AUTO routing. The pacing half of the clause (every attempt inside the rate budget, no "
    "amplification) IS asserted by test_429_storm_every_attempt_is_paced_and_never_amplified. "
    + LIVE_ORDER_PATH_GATED
))
async def test_429_retry_backoff_policy() -> None:
    """On a 429 the order path retries with backoff — each retry drawing from the rate budget."""
