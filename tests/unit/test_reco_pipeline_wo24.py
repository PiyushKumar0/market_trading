"""WO-24 (2026-08-21): the RECOMMEND funnel can no longer freeze silently.

THE INCIDENT. At 09:56:40 the platform's first-ever proposal reached
``RecommendationPipeline._gate_and_persist``. The proposal row was written, and the very next
statement -- ``await self._ctx_builder.build(...)`` -- never returned. At that same instant the
market store stopped answering everything else too (feature snapshots, the ``warmup_refresh``
interval job), and stayed that way for 14 minutes until a restart. The drain tick was parked inside
that await the whole time, so: no verdict, no recommendation, no alert, and no later candidate ever
evaluated. Nothing in the platform said a word.

Two halves are pinned here, deliberately as separate mechanisms:

* **Fix A (deadline)** -- the context build is the only store-touching await on that path, so it now
  carries one. Blowing it re-arms the candidate's day slot, alerts the owner and RETURNS. It must
  NOT reach the WO-20d guard: re-queueing a candidate whose store is dead just spends a second
  analyst call to learn the same thing.
* **Fix C (sweep)** -- the deadline cannot cover a crash between the two writes, so the orphan a
  verdict-less proposal represents is swept for directly. Fix A is prevention; Fix C is the measure
  that would have made the incident visible on the day it happened.

The seams are the ones ``test_reco_pipeline`` already uses; only the context builder is new, because
"a builder that never answers" is the entire subject.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import timedelta

import pytest
import yaml
from ulid import ULID

from engine.core.calendar import NSECalendar
from engine.core.clock import Clock
from engine.ops import pipeline as pipeline_mod
from engine.ops.pipeline import FORWARD_PACING_MIN, GateContextTimeout, RecommendationBook
from engine.risk.limits import LimitTable
from engine.strategy.cost_model import CostModel
from tests.unit.test_reco_pipeline import (
    CALENDAR_DIR,
    ENTER_JSON,
    LIMITS_YAML,
    NOW,
    SYMBOL,
    TODAY,
    FakeHarness,
    StubGate,
    StubLimits,
    Ticker,
    candidate,
    log_events,
    make_pipeline,
    passing_ctx,
    verdict_of,
)

#: Small enough that a hung build is resolved in milliseconds, large enough that a healthy build on
#: a loaded CI box is never mistaken for the incident.
FAST_DEADLINE_S = 0.05


# =========================================================================== fixtures (local copies)
@pytest.fixture
def ticker() -> Ticker:
    return Ticker(NOW)


@pytest.fixture
def pclock(ticker: Ticker) -> Clock:
    return Clock(time_source=ticker)


@pytest.fixture
def calendar(pclock: Clock, conn) -> NSECalendar:
    return NSECalendar(CALENDAR_DIR, pclock, sqlite_conn=conn)


@pytest.fixture(scope="module")
def limit_table() -> LimitTable:
    return LimitTable.model_validate(yaml.safe_load(LIMITS_YAML.read_text(encoding="utf-8")))


@pytest.fixture(scope="module")
def cost_model() -> CostModel:
    return CostModel.from_config()


@pytest.fixture
def book(conn, pclock: Clock, cost_model: CostModel) -> RecommendationBook:
    return RecommendationBook(conn, pclock, cost_model)


# =========================================================================== doubles
class HangingCtxBuilder:
    """A ``GateContextBuilder`` that accepts the call and never answers -- the 09:56:40 store.

    Records what it was asked for and how many times ``asyncio.wait_for`` cancelled it, so the test
    can prove the DEADLINE fired rather than the coroutine merely being abandoned.
    """

    def __init__(self) -> None:
        self.calls: list[tuple[str, str, str, object]] = []
        self.cancelled = 0

    async def build(self, symbol: str, side: str, style: str, d) -> object:
        self.calls.append((symbol, side, style, d))
        try:
            await asyncio.Event().wait()          # never set: the store is gone
        except asyncio.CancelledError:
            self.cancelled += 1
            raise
        raise AssertionError("unreachable -- the hanging builder must never return")


class StubAction:
    """The minimum ``_persist_proposal`` touches -- enough to exercise the raise contract alone."""

    def __init__(self, proposal_id: str) -> None:
        self.proposal_id = proposal_id
        self.agent_id = "intraday_analyst"
        self.action = "enter"
        self.inputs_digest = "digest"

    def model_dump(self, mode: str = "json") -> dict[str, str]:
        return {"action": self.action}


# =========================================================================== helpers
def build_pipeline(
    conn, pclock, calendar, book, limit_table, cost_model, *,
    ctx_builder=None, harness=None, rearm=None,
):
    """A ranked+paced pipeline whose context builder can be swapped for the hanging one.

    ``make_pipeline`` hard-wires ``FakeCtxBuilder``; the attribute is replaced afterwards rather
    than widening that shared helper for one suite.
    """
    pipeline, parts = make_pipeline(
        conn=conn, clock=pclock, calendar=calendar, book=book,
        harness=harness or FakeHarness(),
        gate=StubGate(verdict_of("approve", cost_model)), ctx=passing_ctx(),
        limits=StubLimits(limit_table), rearm=rearm,
    )
    if ctx_builder is not None:
        pipeline._ctx_builder = ctx_builder
        parts["ctx_builder"] = ctx_builder
    return pipeline, parts


def enter_json(*, signal_id: str, symbol: str = SYMBOL) -> dict:
    """``ENTER_JSON`` re-identified for one candidate (the R1 coherence guard compares them)."""
    return {**ENTER_JSON, "signal_id": signal_id, "tradingsymbol": symbol}


def write_proposal(conn, *, created_at, proposal_id: str | None = None, action: str = "enter") -> str:
    """Persist a bare proposal row exactly as ``_persist_proposal`` would, at a chosen time."""
    pid = proposal_id or str(ULID())
    conn.execute(
        "INSERT INTO proposals (proposal_id, agent_id, action, payload, inputs_digest, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (pid, "intraday_analyst", action, "{}", "digest", created_at.isoformat()),
    )
    return pid


def write_verdict(conn, proposal_id: str, *, evaluated_at) -> None:
    conn.execute(
        "INSERT INTO verdicts (verdict_id, proposal_id, verdict, payload, evaluated_at) "
        "VALUES (?, ?, ?, ?, ?)",
        (str(ULID()), proposal_id, "approve", "{}", evaluated_at.isoformat()),
    )


def orphan_alerts(parts) -> list:
    return [m for m in parts["notify"].messages if m.data.get("rule_id") == "proposal_orphaned"]


def counted_rows(conn, table: str) -> int:
    return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


# ======================================================== Fix A: the gate-context build has a deadline
def test_the_deadline_constants_are_pinned_and_ordered():
    """90 s is far above a healthy build (a handful of point reads) and below the 3-minute drain
    cadence, so it can only fire on a real stall. And the orphan TTL must sit ABOVE the deadline:
    if it did not, the sweep would announce a proposal the timeout path is still in the middle of
    alerting about, and every WO-24a incident would arrive as two messages."""
    assert pipeline_mod._GATE_CONTEXT_DEADLINE_S == 90.0
    assert pipeline_mod._ORPHAN_TTL_MIN == 10
    assert pipeline_mod._ORPHAN_SWEEP_INTERVAL_MIN == 5
    assert pipeline_mod._ORPHAN_TTL_MIN * 60 > pipeline_mod._GATE_CONTEXT_DEADLINE_S


async def test_a_hung_context_build_raises_carrying_the_orphan_it_left_behind(
    conn, pclock, calendar, book, limit_table, cost_model, monkeypatch
):
    """The raise contract on its own: the proposal is ALREADY persisted, so the exception carries
    its id. No verdict is invented for it -- an invented ``reject`` would read forever after as a
    decision the gate made (3.4), and the sweep below is what closes the loop instead."""
    monkeypatch.setattr(pipeline_mod, "_GATE_CONTEXT_DEADLINE_S", FAST_DEADLINE_S)
    hanging = HangingCtxBuilder()
    pipeline, _ = build_pipeline(
        conn, pclock, calendar, book, limit_table, cost_model, ctx_builder=hanging)
    action = StubAction(str(ULID()))

    with pytest.raises(GateContextTimeout) as excinfo:
        await pipeline._gate_and_persist(action, SYMBOL, "BUY", "intraday", TODAY)

    assert excinfo.value.symbol == SYMBOL
    assert excinfo.value.proposal_id == action.proposal_id
    assert action.proposal_id in str(excinfo.value)
    assert hanging.calls == [(SYMBOL, "BUY", "intraday", TODAY)]
    assert hanging.cancelled == 1                     # the DEADLINE fired, not a bare abandonment
    assert counted_rows(conn, "proposals") == 1       # the orphan is left on purpose
    assert counted_rows(conn, "verdicts") == 0


async def test_a_frozen_store_costs_one_candidate_and_the_drain_keeps_going(
    conn, ticker, pclock, calendar, book, limit_table, cost_model, caplog, monkeypatch
):
    """The whole of Fix A on the live path, against a store that stays dead.

    The 2026-08-21 shape: the deadline fires, the candidate's day slot goes back to the prescreen,
    the owner gets ONE loud message naming the symbol, no verdict and no recommendation are written
    -- and, load-bearing, the candidate is NOT re-queued. The WO-20d guard would have put it at the
    front of the queue for a retry that hits the same dead store; a second candidate arriving after
    the freeze is evaluated on the very next tick instead, which is exactly what did not happen for
    14 minutes on the day.
    """
    monkeypatch.setattr(pipeline_mod, "_GATE_CONTEXT_DEADLINE_S", FAST_DEADLINE_S)
    rearmed: list[tuple[str, str]] = []
    hanging = HangingCtxBuilder()
    harness = FakeHarness(
        enter_json(signal_id="FROZEN1"), enter_json(signal_id="FROZEN2", symbol="TCS"))
    pipeline, parts = build_pipeline(
        conn, pclock, calendar, book, limit_table, cost_model,
        ctx_builder=hanging, harness=harness,
        rearm=lambda sym, sid: rearmed.append((sym, sid)) or True,
    )

    first = candidate(signal_id="FROZEN1", score=0.9)
    await pipeline.on_signal_candidate(first)
    with caplog.at_level(logging.WARNING, logger="engine.ops.pipeline"):
        assert await pipeline.drain_forward_queue() is True        # the tick neither hangs nor dies

        # -- the deadline fired on the BUILD, and the proposal is an orphan by design ------------
        assert hanging.cancelled == 1
        proposals = conn.execute("SELECT proposal_id FROM proposals").fetchall()
        assert len(proposals) == 1
        proposal_id = str(proposals[0]["proposal_id"])
        assert counted_rows(conn, "verdicts") == 0
        assert counted_rows(conn, "recommendations") == 0
        assert counted_rows(conn, "learning_ledger") == 0

        # -- the day slot went back, exactly like an analyst infrastructure failure --------------
        assert rearmed == [(first.symbol, first.strategy_id)]
        evaluated = conn.execute(
            "SELECT evaluated FROM prescreen_day_slots WHERE symbol=? AND strategy_id=?",
            (first.symbol, first.strategy_id),
        ).fetchone()["evaluated"]
        assert evaluated == 0

        # -- one loud message, naming the symbol -------------------------------------------------
        assert len(parts["notify"].messages) == 1
        alert = parts["notify"].messages[0]
        assert alert.severity == "critical"
        assert SYMBOL in alert.title
        assert alert.data["proposal_id"] == proposal_id
        assert alert.data["signal_id"] == "FROZEN1"

        # -- the ERROR line a human greps for ----------------------------------------------------
        timeouts = log_events(caplog, "gate_context_timeout")
        assert len(timeouts) == 1
        assert timeouts[0].levelname == "ERROR"
        assert (timeouts[0].signal_id, timeouts[0].symbol, timeouts[0].proposal_id) == (
            "FROZEN1", SYMBOL, proposal_id)

        # -- NOT re-queued: GateContextTimeout never reaches _evaluate_forward_guarded -----------
        assert pipeline._pending_forwards == []
        assert pipeline._requeued_forwards == set()
        assert log_events(caplog, "forward_evaluation_requeued") == []
        assert log_events(caplog, "forward_evaluation_lost") == []

        # -- the drain survives to the NEXT candidate --------------------------------------------
        await pipeline.on_signal_candidate(candidate(symbol="TCS", signal_id="FROZEN2", score=0.5))
        ticker.at = NOW + timedelta(minutes=FORWARD_PACING_MIN)
        assert await pipeline.drain_forward_queue() is True

    assert len(harness.calls) == 2                    # both candidates really reached the analyst
    assert hanging.cancelled == 2
    assert counted_rows(conn, "proposals") == 2
    assert counted_rows(conn, "verdicts") == 0
    assert len(rearmed) == 2
    assert len(log_events(caplog, "gate_context_timeout")) == 2
    assert pipeline._pending_forwards == []


# ============================================================ Fix C: the orphaned-proposal sweep
async def test_an_orphan_past_the_ttl_is_announced_once_and_not_again(
    conn, pclock, calendar, book, limit_table, cost_model, caplog
):
    """A verdict-less proposal older than the TTL is an ERROR line plus one owner message -- and the
    NEXT sweep says nothing, because a watchdog that repeats every five minutes is a mute button
    waiting to be pressed."""
    pipeline, parts = build_pipeline(conn, pclock, calendar, book, limit_table, cost_model)
    stranded = write_proposal(
        conn, created_at=NOW - timedelta(minutes=pipeline_mod._ORPHAN_TTL_MIN + 5))

    with caplog.at_level(logging.WARNING, logger="engine.ops.pipeline"):
        assert await pipeline.sweep_orphaned_proposals() == 1
        assert await pipeline.sweep_orphaned_proposals() == 0

    events = log_events(caplog, "proposal_orphaned")
    assert len(events) == 1
    assert events[0].levelname == "ERROR"
    assert events[0].proposal_id == stranded
    assert events[0].action == "enter"
    assert events[0].age_min == pytest.approx(15.0)

    alerts = orphan_alerts(parts)
    assert len(alerts) == 1
    assert alerts[0].severity == "warning"
    assert stranded in alerts[0].body
    assert alerts[0].data["proposal_id"] == stranded
    # No verdict was manufactured to tidy the row away.
    assert counted_rows(conn, "verdicts") == 0


async def test_a_proposal_younger_than_the_ttl_is_left_alone(
    conn, pclock, calendar, book, limit_table, cost_model, caplog
):
    """A proposal mid-flight is not an orphan. The gate is allowed to take a moment."""
    pipeline, parts = build_pipeline(conn, pclock, calendar, book, limit_table, cost_model)
    write_proposal(conn, created_at=NOW - timedelta(minutes=pipeline_mod._ORPHAN_TTL_MIN - 1))

    with caplog.at_level(logging.WARNING, logger="engine.ops.pipeline"):
        assert await pipeline.sweep_orphaned_proposals() == 0

    assert log_events(caplog, "proposal_orphaned") == []
    assert orphan_alerts(parts) == []


async def test_a_proposal_that_reached_a_verdict_is_never_an_orphan(
    conn, pclock, calendar, book, limit_table, cost_model, caplog
):
    """The join is the whole test: an old proposal WITH a verdict is a completed decision, however
    long ago it was made. Only the one still missing its verdict is announced."""
    pipeline, parts = build_pipeline(conn, pclock, calendar, book, limit_table, cost_model)
    long_ago = NOW - timedelta(hours=3)
    judged = write_proposal(conn, created_at=long_ago, action="exit")
    write_verdict(conn, judged, evaluated_at=long_ago)
    stranded = write_proposal(conn, created_at=long_ago)

    with caplog.at_level(logging.WARNING, logger="engine.ops.pipeline"):
        assert await pipeline.sweep_orphaned_proposals() == 1

    events = log_events(caplog, "proposal_orphaned")
    assert [e.proposal_id for e in events] == [stranded]
    assert len(orphan_alerts(parts)) == 1


async def test_the_drain_tick_sweeps_on_a_five_minute_throttle(
    conn, ticker, pclock, calendar, book, limit_table, cost_model
):
    """Wiring plus cadence. The 60 s forward-drain tick is what invokes the sweep -- there is no new
    scheduler job -- and the internal throttle decides which of those pulses actually reads the DB.
    A minute later is too soon; ``_ORPHAN_SWEEP_INTERVAL_MIN`` later is due."""
    pipeline, _ = build_pipeline(conn, pclock, calendar, book, limit_table, cost_model)
    swept: list[object] = []
    real_sweep = pipeline.sweep_orphaned_proposals

    async def counting_sweep() -> int:
        swept.append(pclock.now())
        return await real_sweep()

    pipeline.sweep_orphaned_proposals = counting_sweep

    await pipeline.drain_forward_queue()                    # first tick: no anchor yet, so it runs
    assert len(swept) == 1

    ticker.at = NOW + timedelta(minutes=1)
    await pipeline.drain_forward_queue()                    # throttled
    assert len(swept) == 1

    ticker.at = NOW + timedelta(minutes=pipeline_mod._ORPHAN_SWEEP_INTERVAL_MIN)
    await pipeline.drain_forward_queue()                    # due again
    assert len(swept) == 2


async def test_the_announced_memo_rolls_with_the_day_like_its_siblings(
    conn, pclock, calendar, book, limit_table, cost_model
):
    """The memo is per-process and per-day, exactly like ``_requeued_forwards``: it can never grow
    without bound, and an orphan still unresolved tomorrow re-announces itself once tomorrow."""
    pipeline, _ = build_pipeline(conn, pclock, calendar, book, limit_table, cost_model)
    stranded = write_proposal(conn, created_at=NOW - timedelta(hours=1))

    assert await pipeline.sweep_orphaned_proposals() == 1
    assert pipeline._orphans_alerted == {stranded}
    assert pipeline._orphans_alerted_day == TODAY

    pipeline._roll_orphan_day(TODAY + timedelta(days=1))
    assert pipeline._orphans_alerted == set()
    assert await pipeline.sweep_orphaned_proposals() == 1


async def test_a_broken_sweep_read_never_costs_the_drain_its_tick(
    conn, pclock, calendar, book, limit_table, cost_model, caplog
):
    """A watchdog that can kill the tick it rides on is not a watchdog. A failing read warns and
    returns 0; the drain that invoked it carries on."""
    pipeline, _ = build_pipeline(conn, pclock, calendar, book, limit_table, cost_model)
    conn.execute("DROP TABLE verdicts")

    with caplog.at_level(logging.WARNING, logger="engine.ops.pipeline"):
        assert await pipeline.sweep_orphaned_proposals() == 0
        assert await pipeline.drain_forward_queue() is False

    assert len(log_events(caplog, "orphan_sweep_failed")) >= 1
