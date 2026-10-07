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

Clauses skipped (with the reason printed by ``-rs``):

* **≥3 rejects/60 s ⇒ FROZEN + alert (§3.5.1)** — UNBUILT: the cause constant and its owner clear
  path exist, nothing detects the storm (see ``test_three_rejects_in_60s_freeze_entries_and_alert``).
* **retry/backoff policy** — UNBUILT: ``KiteClient`` re-raises every broker error and nothing above
  it retries; the pacing half of the clause is asserted above.

Phase-2 reality: RECOMMEND mode places zero broker orders (B7), so this scenario runs the order
surface in AUTO(paper routing) exactly as the Phase-3 OMS will drive it; nothing here depends on an
unbuilt Phase-3 component except the two skipped clauses.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from typing import Any

import pytest
from kiteconnect.exceptions import NetworkException

from engine.broker.kite_client import KiteClient
from engine.broker.rate_limiter import EntryBudgetExhausted, RateLimiter
from engine.core.calendar import NSECalendar
from engine.core.clock import IST, Clock
from engine.core.config import config_dir
from engine.core.enums import Actor, Mode, RiskState
from engine.core.eventbus import EventBus
from engine.core.types import OwnerConfirmation
from engine.ops.main import make_order_guard
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


@pytest.mark.skip(reason=(
    "UNBUILT: no '>=3 broker rejects/60 s => FROZEN + alert' detector exists. "
    "engine.risk.causes.CAUSE_REJECTION_STORM (causes.py:55) is declared and /resume_entries clears it "
    "(notify/telegram.py:1150), but no code path counts broker rejects or calls "
    "RiskStateLatch.set_cause(CAUSE_REJECTION_STORM, ...) — grep of src/ finds only those two sites; "
    "KiteClient re-raises rejects (kite_client.py:172-183) and the OMS executor/PaperBroker routing that "
    "would observe REJECTED postbacks is not wired. " + PHASE3_GATED
))
async def test_three_rejects_in_60s_freeze_entries_and_alert() -> None:
    """§3.5.1: ≥3 broker rejects inside 60 s latch CAUSE_REJECTION_STORM (FROZEN) + owner alert."""


@pytest.mark.skip(reason=(
    "UNBUILT: retry/backoff policy — KiteClient re-raises every broker error including HTTP 429 "
    "(kite_client.py:172-183) and no caller retries or backs off; the OMS order executor that would own "
    "the policy is WO-P3-5. The pacing half of the clause (every attempt inside the rate budget, no "
    "amplification) IS asserted by test_429_storm_every_attempt_is_paced_and_never_amplified. "
    + PHASE3_GATED
))
async def test_429_retry_backoff_policy() -> None:
    """A2: on a 429 the order path retries with backoff — each retry drawing from the rate budget."""
