"""§9.4 chaos case 6 — broker rejection storm (incl. 429s).

Plan row (IMPLEMENTATION_PLAN.md §9.4, case 6), "Must hold", verbatim:

    retry/backoff inside rate budget (A2); ≥3 rejects/60 s ⇒ FROZEN + alert (§3.5.1); no order spam
    (B3); with the 70/day entry budget exhausted, a triggered kill-flatten/square-off/exit still places
    from the uncapped-but-paced risk-reducing lane — never starved (R3, §7.1)

Composition (as ``engine.ops.main`` wires it, main.py:601-626): the REAL ``RateLimiter(clock)`` with
its shipped defaults (1 order call/s, burst 2, 70 entry calls/day), the REAL ``KiteClient`` facade,
the REAL ``ModeManager`` / ``KillSwitch`` / ``RiskStateLatch`` (incl. the main.py:524-530 kill→latch
bridge) and the composition root's own ``make_order_guard``. The client is built with
``orders_enabled=True``, which production never passes (D8). The ONLY fake is the pykiteconnect ``KiteConnect``
object — the broker HTTP boundary — which answers from a script (429s, rejections, order ids).
Time is a ticking controllable clock; ``asyncio.sleep`` advances it (the ``test_rate_limiter``
idiom), so pacing is asserted on simulated time without real waits.

Clauses asserted here:

* **no order spam (B3) / "inside rate budget" (A2)** — a caller hammering through a 429 storm
  reaches the broker exactly once per attempt (KiteClient never retries or amplifies internally),
  every attempt is admitted by the order bucket (≤ burst 2, then ≥ 1 s apart) and counted against
  the entry budget: ``test_429_storm_every_attempt_is_paced_and_never_amplified``.
* **entry budget exhausted ⇒ the risk-reducing lane is never starved (R3, §7.1)** — 70 entries
  spend the budget, the 71st is budget-rejected before the broker is touched, the kill switch is
  thrown, and exit / square-off / cancel / GTT calls still reach the broker, paced behind the SAME
  1/s bucket, including a retry after a 429: ``test_entry_budget_exhausted_flatten_lane_never_starved``.
* **≥3 rejects/60 s ⇒ FROZEN + alert (§3.5.1), paper path** — the real ``PaperBroker`` rejects three
  entries inside 60 s, the real paper ``OrderManager`` reports each broker rejection to the real
  ``PaperRisk``, which latches the daily ``rejection_storm`` paper halt (FROZEN; the alert is the
  ``paper_halt_set`` WARNING, paper plan D4); the next entry is refused by ``paper_order_guard``
  before the broker while an exit still places, and the real risk latch is untouched:
  ``test_paper_three_broker_rejects_in_60s_freeze_paper_entries_and_exits_still_place``.

Clauses skipped (with the reason printed by ``-rs``):

* **≥3 rejects/60 s ⇒ FROZEN + alert (§3.5.1), real path** — lands with the Phase-4 live OMS:
  ``CAUSE_REJECTION_STORM`` on the real ``RiskStateLatch``, its owner ``/resume_entries`` clear, and the
  ≥3-storms-per-session kill (see ``test_three_rejects_in_60s_freeze_entries_and_alert``).
* **retry/backoff policy** — UNBUILT: ``KiteClient`` re-raises every broker error and nothing above
  it retries; the pacing half of the clause is asserted above.

Phase-2 reality: RECOMMEND mode places zero broker orders (B7), so the KiteClient scenarios run the
order surface in AUTO(paper routing) exactly as the Phase-3 OMS will drive it; nothing here depends on
an unbuilt component except the two skipped clauses.
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
from tests.chaos.conftest import PHASE3_GATED

#: Wed 2026-06-17 10:05 IST — inside the seeded 10:00–10:30 trade window (NSECalendar default seed).
START = datetime(2026, 6, 17, 10, 5, tzinfo=IST)
OWNER_OK = OwnerConfirmation(actor=Actor.OWNER, confirmed=True, note="chaos case 6")
ENTRY_REQ = {"tradingsymbol": "RELIANCE", "exchange": "NSE", "transaction_type": "BUY", "quantity": 1,
             "order_type": "LIMIT", "product": "MIS", "price": 1420.5}
EXIT_REQ = {"tradingsymbol": "RELIANCE", "exchange": "NSE", "transaction_type": "SELL", "quantity": 1,
            "order_type": "MARKET", "product": "MIS", "market_protection": -1}
#: The shipped §7.1 ``order_rate`` numbers the default ``RateLimiter(clock)`` carries (main.py:625).
ENTRY_BUDGET = 70
BURST = 2
ONE_S = timedelta(seconds=1)


def _http_429() -> NetworkException:
    """What pykiteconnect raises for a Kite HTTP 429 (connect.py maps ``error_type`` + status code)."""
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
    """Stand-in for pykiteconnect ``KiteConnect`` — the only faked boundary.

    Each call pops the next scripted outcome (an exception to raise, or the id to return; an empty
    script answers with a fresh id) and records ``(op, sim_time, params)`` at the moment the broker
    was actually hit. Calls arrive strictly one at a time (sequential awaits), so reading the ticker
    from the executor thread is race-free.
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
        # main.py:524-530 — the kill ⇄ cause-latch bridge, so risk_state reads KILLED after a kill.

        async def _kill_to_latch(evt: Any) -> None:
            if evt.killed:
                await self.latch.set_cause("kill", RiskState.KILLED, evt.reason or "kill", evt.actor)
            else:
                await self.latch.clear_cause("kill", evt.actor)

        self.bus.subscribe(TOPIC_KILL_STATE, _kill_to_latch)
        self.limiter = RateLimiter(self.clock)          # main.py:625 — shipped §7.1 defaults
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
    """Order-bucket pacing (A2/B3): after the burst, consecutive broker hits are ≥ 1 s apart, and
    no rolling 1-s window ever holds more than the burst — far under the B3 10-ops/s ceiling."""
    slack = timedelta(milliseconds=1)             # float refill arithmetic, never a real gap
    first_paced = 1 if already_drained else BURST
    for i in range(first_paced, len(times)):
        assert times[i] - times[i - 1] >= ONE_S - slack, (i, times[i - 1], times[i])
    for i, t in enumerate(times):
        in_window = sum(1 for u in times[i:] if u - t < ONE_S - slack)
        assert in_window <= BURST, (t, in_window)


# --------------------------------------------------------------------------- clauses
async def test_429_storm_every_attempt_is_paced_and_never_amplified(conn, ticker) -> None:
    """A2/B3: a caller that keeps retrying through a solid wall of HTTP 429s produces exactly one
    broker hit per attempt, each one admitted by the shared 1/s order bucket and charged to the entry
    budget — the storm can never turn into spam, whatever the caller's retry loop does."""
    attempts = 12
    world = _World(conn, ticker, script=[_http_429() for _ in range(attempts)])
    await world.go_auto()

    for _ in range(attempts):
        with pytest.raises(NetworkException) as exc_info:
            await world.client.place_order(ENTRY_REQ, intent="entry")
        assert exc_info.value.code == 429           # re-raised verbatim, never swallowed (R5/R8)

    times = _broker_times(world.kc)
    assert len(world.kc.calls) == attempts          # one broker hit per attempt — no hidden retry
    assert world.limiter.orders_today() == (attempts, 0)   # every attempt is inside the rate budget
    _assert_paced(times)
    # 12 attempts through a burst-2 / 1-per-second bucket span at least 10 s of (simulated) time.
    assert times[-1] - times[0] >= timedelta(seconds=attempts - BURST) - timedelta(milliseconds=1)


async def test_entry_budget_exhausted_flatten_lane_never_starved(conn, ticker) -> None:
    """R3/§7.1: a churny day spends the 70/day entry budget; the owner then kills. Every
    risk-reducing broker call — the square-off/exit orders, the cancels of resting entries and the
    GTT protection — still places (a 429 on the way is retried and admitted), paced behind the same
    1/s bucket, never budget-rejected, never blocked by the kill switch that blocks entries."""
    world = _World(conn, ticker)
    await world.go_auto()

    for _ in range(ENTRY_BUDGET):
        await world.client.place_order(ENTRY_REQ, intent="entry")
    assert world.limiter.orders_today() == (ENTRY_BUDGET, 0)
    entry_hits = len(world.kc.calls)
    assert entry_hits == ENTRY_BUDGET
    _assert_paced(_broker_times(world.kc))

    # The 71st entry is refused BY THE BUDGET, before the broker is touched (B3).
    with pytest.raises(EntryBudgetExhausted):
        await world.client.place_order(ENTRY_REQ, intent="entry")
    assert len(world.kc.calls) == entry_hits

    # The owner kills; the kill latches KILLED and every opening call is now refused at the guard.
    await world.kill.trigger("chaos case 6: rejection storm", actor=Actor.OWNER, flatten=False)
    assert world.mode.risk_state() == RiskState.KILLED
    with pytest.raises(KillSwitchEngaged):
        await world.client.place_order(ENTRY_REQ, intent="entry")
    assert len(world.kc.calls) == entry_hits

    # The flatten lane. The first square-off order meets a 429 and the caller retries it.
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
    # Paced behind the SAME bucket the entries drained: no burst left, so ≥ 1 s between every hit,
    # including the 429-then-retry pair (the retry stays inside the rate budget, A2).
    lane_times = [at for _, at in lane]
    assert lane_times[0] >= flatten_started
    _assert_paced(lane_times, already_drained=True)
    assert lane_times[1] - lane_times[0] >= ONE_S - timedelta(milliseconds=1)


# --------------------------------------------------------------------------- the paper path (§3.5.1)
#: High enough that a short search finds a seed rejecting exactly the first ``STORM_REJECTS`` placements.
_RATE = 0.9


def _seed_rejecting_first(n: int) -> int:
    """A PaperBroker seed whose injected-rejection draws reject placements 1..n and accept n+1. The
    broker draws once per placement that passes its guard (``PaperBroker.place_order``), so the
    sequence is a pure function of (seed, placement count)."""
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
    """§3.5.1 on the paper OMS: three broker rejects inside 60 s ⇒ the daily ``rejection_storm`` paper
    halt (FROZEN) + its WARNING; the next entry never reaches the broker, an exit still places, and
    the real risk latch never moves (paper never writes it, §1.5)."""
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

        # The next entry is refused by the paper guard before the broker: no fourth placement.
        ticker.at = START + timedelta(seconds=55)
        blocked = await orders.submit(*_seed_verdict(conn, STORM_REJECTS))
        assert blocked.state is OrderState.REJECTED and "FROZEN" in (blocked.reject_reason or "")
        assert [(e.from_state, e.to_state) for e in store.events(blocked.order_id)] == [
            (OrderState.DRAFT, OrderState.REJECTED)]
        assert len(await broker.orders()) == STORM_REJECTS

        # A risk-reducing exit still passes the guard, reaches the broker and rests there.
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
    "Real path, lands with the Phase-4 live OMS: real orders are structurally impossible today "
    "(KiteClient refuses every order without orders_enabled=True, which production never passes; D8), "
    "so CAUSE_REJECTION_STORM (engine.risk.causes) on the real RiskStateLatch, its owner /resume_entries "
    "clear, and the >=3-storms-per-session kill escalation have no order path to observe. The PAPER path "
    "is built and asserted by "
    "test_paper_three_broker_rejects_in_60s_freeze_paper_entries_and_exits_still_place."
))
async def test_three_rejects_in_60s_freeze_entries_and_alert() -> None:
    """§3.5.1 real path: ≥3 broker rejects inside 60 s latch CAUSE_REJECTION_STORM (FROZEN) + owner alert;
    ≥3 storms in a session ⇒ kill."""


@pytest.mark.skip(reason=(
    "UNBUILT: retry/backoff policy — KiteClient re-raises every broker error including HTTP 429 "
    "(KiteClient._order_call's except/re-raise) and no caller retries or backs off; the OMS order "
    "executor that would own the policy is WO-P3-5. The pacing half of the clause (every attempt inside the rate budget, no "
    "amplification) IS asserted by test_429_storm_every_attempt_is_paced_and_never_amplified. "
    + PHASE3_GATED
))
async def test_429_retry_backoff_policy() -> None:
    """A2: on a 429 the order path retries with backoff — each retry drawing from the rate budget."""
