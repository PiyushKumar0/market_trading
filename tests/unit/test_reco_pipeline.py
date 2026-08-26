"""RECOMMEND pipeline (Â§3.6 / Â§5.2 / Â§7.1 ``max_holding``) â€” book + trigger handlers.

The seams are faked exactly where a fake is the point (the Claude harness, the context assembler, the
governor) and REAL everywhere the behaviour under test depends on real policy:

* the end-to-end approve path runs the **real** :class:`~engine.risk.gate.RiskGate` over the shipped
  ``config/limits.yaml`` and the **real** :class:`~engine.strategy.cost_model.CostModel` from
  ``config/costs.yaml`` â€” a recommendation that the shipped limit table would not approve must not
  pass here either;
* a stub gate is used only to reach the verdict branches (shrink / owner_approval_required) that a
  passing baseline cannot produce;
* the ledger matrix runs against the **real** migrated SQLite schema and asserts the P&L to the
  paisa, including that :class:`~engine.risk.exposure.ExposureTracker` reads the closed position back
  as the same net figure (positions.realized_pnl is GROSS, costs are a separate column).
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
import yaml
from ulid import ULID

from engine.core.calendar import NSECalendar
from engine.core.clock import IST, Clock
from engine.core.contracts import CheckResult, CostBreakdown, EnterAction, GateVerdict, Recommendation
from engine.core.enums import Mode, RiskState
from engine.core.types import Bar, TradeWindow
from engine.intelligence.context import AssembledContext
from engine.intelligence.harness import AgentDef, AgentResult
from engine.notify.catalog import MessageKind
from engine.ops.nightly_review import read_funnel_raw_counts
from engine.ops.pipeline import (
    ATR_PERIOD,
    FORWARD_PACING_MIN,
    POSITION_EVENT_DEBOUNCE_MIN,
    TTL_INTRADAY_MIN,
    RecommendationBook,
    RecommendationPipeline,
)
from engine.risk.exposure import ExposureTracker
from engine.risk.gate import GateContext, RiskGate
from engine.risk.limits import LimitTable
from engine.strategy.cost_model import CostModel
from engine.strategy.indicators import wilder_atr
from engine.strategy.prescreen import SignalPreScreen
from engine.strategy.types import RawLevels, SignalCandidate

REPO = Path(__file__).resolve().parents[2]
LIMITS_YAML = REPO / "config" / "limits.yaml"
CALENDAR_DIR = REPO / "config" / "calendar"

#: Wed 2026-06-17 10:05 IST â€” a real trading day inside the seeded 10:00â€“10:30 trade window.
NOW = datetime(2026, 6, 17, 10, 5, tzinfo=IST)
TODAY = NOW.date()
SYMBOL = "RELIANCE"
CAPITAL_BASE = Decimal("20000")


# =========================================================================== doubles
class Ticker:
    """A movable time source â€” ``ticker.at = ...`` advances every Clock built on it."""

    def __init__(self, at: datetime) -> None:
        self.at = at

    def __call__(self) -> datetime:
        return self.at


class StubLimits:
    """Duck-typed ``LimitsEngine``: the gate and the pipeline only ever call ``load()`` (R4)."""

    def __init__(self, table: LimitTable) -> None:
        self._table = table

    def load(self) -> LimitTable:
        return self._table


class FakeHarness:
    """Canned single-shot results. ``dict`` â‡’ Ok(validate(json)); ``AgentResult`` â‡’ returned as-is."""

    def __init__(self, *results: Any) -> None:
        self.queued = list(results)
        self.calls: list[Any] = []

    async def run_single_shot(self, agent_def, context, validate, *, json_schema=None, call_class=None):
        self.calls.append(context)
        assert self.queued, "fake harness called more times than it has canned results"
        item = self.queued.pop(0)
        if isinstance(item, AgentResult):
            return item
        return AgentResult.Ok(call_id=str(ULID()), payload=validate(json.dumps(item)))


class FakeAssembler:
    """Records the kwargs each trigger assembled with; returns a real :class:`AssembledContext`."""

    def __init__(self) -> None:
        self.signal_kwargs: list[dict[str, Any]] = []
        self.position_events: list[tuple[dict[str, Any], str, str]] = []
        self.contexts: list[AssembledContext] = []
        self.heartbeats = 0
        self.regime_notes: list[str] = []

    def set_regime_note(self, note: str) -> None:
        self.regime_notes.append(note)

    def for_signal(self, candidate, **kwargs):
        self.signal_kwargs.append(kwargs)
        return self._record(self._ctx("signal_candidate", candidate.signal_id))

    def for_position_event(self, position_row, event_kind, detail, *, ltp_line):
        self.position_events.append((dict(position_row), event_kind, detail))
        return self._record(self._ctx("position_event", str(position_row.get("position_id"))))

    def for_heartbeat(self, *, regime_lines, open_positions_summary):
        self.heartbeats += 1
        return self._record(self._ctx("heartbeat", "hb"))

    def _record(self, ctx: AssembledContext) -> AssembledContext:
        self.contexts.append(ctx)
        return ctx

    @staticmethod
    def _ctx(call_class: str, tag: str) -> AssembledContext:
        return AssembledContext.build(
            system_prompt="system", stable_block=f"stable {tag}",
            volatile_block=f"volatile {tag}", call_class=call_class,
        )


class FakeCtxBuilder:
    def __init__(self, ctx: GateContext) -> None:
        self.ctx = ctx
        self.calls: list[tuple[str, str, str, date]] = []

    async def build(self, symbol: str, side: str, style: str, d: date) -> GateContext:
        self.calls.append((symbol, side, style, d))
        return self.ctx


class Decision:
    def __init__(self, allowed: bool, reason: str | None = None) -> None:
        self.allowed = allowed
        self.reason = reason


class FakeGovernor:
    def __init__(self, allowed: bool = True) -> None:
        self.allowed = allowed
        self.calls: list[tuple[str, str]] = []

    def can_invoke(self, agent_id: str, call_class: str = "schedule") -> Decision:
        self.calls.append((agent_id, call_class))
        return Decision(self.allowed, None if self.allowed else "DG4_zero_sdk_calls")


class FakeMode:
    def __init__(self, mode: Mode = Mode.RECOMMEND, risk_state: RiskState = RiskState.NORMAL) -> None:
        self._mode, self._risk = mode, risk_state

    def mode(self) -> Mode:
        return self._mode

    def risk_state(self) -> RiskState:
        return self._risk


class FakeKill:
    def __init__(self, killed: bool = False) -> None:
        self.killed = killed

    def is_killed(self) -> bool:
        return self.killed


class FakeStore:
    def __init__(self, bars: list[Bar] | None = None, sectors: list[dict[str, str]] | None = None) -> None:
        self.bars = bars or []
        self.sectors = sectors or [{"symbol": SYMBOL, "sector": "ENERGY"}]

    def get_bars_1m(self, symbol, start, end) -> list[Bar]:
        return [b for b in self.bars if b.symbol == symbol]

    def get_sector_map(self, as_of=None) -> list[dict[str, str]]:
        return self.sectors


class Notifier:
    def __init__(self) -> None:
        self.messages: list[Any] = []

    async def __call__(self, message) -> None:
        self.messages.append(message)


class StubGate:
    """Returns a canned verdict â€” the only way to reach shrink / owner_approval_required."""

    def __init__(self, verdict: GateVerdict) -> None:
        self.verdict = verdict

    def evaluate(self, action, ctx) -> GateVerdict:
        return self.verdict.model_copy(update={"proposal_id": action.proposal_id})


class ZeroCostModel:
    """Duck-typed cost model with a zero round trip â€” the only way to land an exact ``scratch``."""

    def round_trip(self, notional: Decimal, product: str, n_scrips_sell_day: int = 1) -> CostBreakdown:
        return CostBreakdown(
            notional=notional, total_cost=Decimal("0"), breakeven_pct=Decimal("0"),
            expected_edge_pct=Decimal("0"), edge_multiple=Decimal("0"), components={},
        )


# =========================================================================== fixtures
@pytest.fixture(scope="module")
def limit_table() -> LimitTable:
    return LimitTable.model_validate(yaml.safe_load(LIMITS_YAML.read_text(encoding="utf-8")))


@pytest.fixture(scope="module")
def cost_model() -> CostModel:
    return CostModel.from_config()          # the REAL config/costs.yaml (C1/C3)


@pytest.fixture
def ticker() -> Ticker:
    return Ticker(NOW)


@pytest.fixture
def pclock(ticker: Ticker) -> Clock:
    return Clock(time_source=ticker)


@pytest.fixture
def calendar(pclock: Clock, conn) -> NSECalendar:
    return NSECalendar(CALENDAR_DIR, pclock, sqlite_conn=conn)


@pytest.fixture
def book(conn, pclock: Clock, cost_model: CostModel) -> RecommendationBook:
    return RecommendationBook(conn, pclock, cost_model)


# --------------------------------------------------------------------------- gate context baseline
def passing_ctx(**overrides: Any) -> GateContext:
    """A context in which every Â§7.1 enter rule passes (mirrors the gate suite's baseline)."""
    base: dict[str, Any] = {
        "now": NOW,
        "mode": Mode.RECOMMEND,
        "risk_state": RiskState.NORMAL,
        "trade_window": TradeWindow(start=time(9, 30), end=time(15, 0), squareoff_buffer_min=5),
        "session_open": time(9, 15),
        "session_close": time(15, 30),
        "equity": CAPITAL_BASE,
        "day_mtm_pct": Decimal("0"),
        "sector_of": {SYMBOL: "ENERGY"},
        "ltp": Decimal("100"),
        "tick_age_s": 1.0,
        "index_tick_age_s": 1.0,
        "in_universe": True,
        "mis_candidate": True,
        "is_fno": True,
        "warmup_ready": True,
        "regime_ready": True,
        "clock_skew_ok": True,
    }
    return GateContext(**{**base, **overrides})


def candidate(**overrides: Any) -> SignalCandidate:
    # signal_id matches ENTER_JSON: the pipeline's structural-coherence guard (R1) drops an analyst
    # payload whose identity fields differ from the candidate's â€” deliberate, tested below.
    base: dict[str, Any] = {
        "signal_id": "01SIGNAL",
        "strategy_id": "orb",
        "symbol": SYMBOL,
        "side": "BUY",
        "style": "intraday",
        "raw_levels": RawLevels(entry=Decimal("100"), stop=Decimal("99"), target=Decimal("103")),
        "score": 0.8,
        "features_snapshot_id": "01SNAP",
        "catalyst_ref": "01CATREF",
    }
    return SignalCandidate(**{**base, **overrides})


ENTER_JSON: dict[str, Any] = {
    "action": "enter",
    "thesis": "Opening-range breakout with volume confirmation and a tight invalidation level.",
    "confidence": 0.7,
    "tradingsymbol": SYMBOL,
    "exchange": "NSE",
    "side": "BUY",
    "style": "intraday",
    "entry_type": "LIMIT",
    "entry_price": "100",
    "stop_price": "99",
    "target_price": "103",
    "quantity": 10,
    "signal_id": "01SIGNAL",
    "strategy_id": "orb",
    "features_snapshot_id": "01SNAP",
}

NO_ACTION_JSON: dict[str, Any] = {
    "action": "no_action",
    "reason": "chop â€” the range has not resolved",
    "regime_note": "NIFTY balancing inside the opening range; breakouts failing.",
}


def agent_defs() -> dict[str, AgentDef]:
    return {
        "intraday_analyst": AgentDef(
            agent_id="intraday_analyst", model="sonnet-4.6", shape="single_shot",
            tools_enabled=False, allowed_tools=[], max_output_tokens=1200, timeout_s=45.0,
        )
    }


async def publish_candidate(pipeline: RecommendationPipeline, cand: SignalCandidate) -> None:
    """Publish a candidate AND let the Â§5.2(a) drain spend a slot on the queue.

    Since 2026-08-14 an arriving candidate only ENQUEUES â€” the analyst call is made by the paced
    drain tick, so a test that wants a candidate evaluated has to run the drain too.
    ``_drain_one_forward`` is that tick with its cadence guard removed: the cadence itself is pinned
    by the pacing block at the bottom of this file (through the PUBLIC ``drain_forward_queue``), and
    every other test here is about what happens to a candidate AFTER it is dispatched rather than
    about when. A drain on an empty queue is a no-op, so the "nothing was forwarded" tests keep
    asserting exactly what they did before.
    """
    await pipeline.on_signal_candidate(cand)
    await pipeline._drain_one_forward()


def make_pipeline(
    *, conn, clock, calendar, book, harness, gate, ctx, limits, store=None, governor=None,
    mode=None, kill=None, notify=None, assembler=None, rearm=None, funnel_raw=None,
    admission_mode="ranked", forward_drain_mode="paced",
) -> tuple[RecommendationPipeline, dict[str, Any]]:
    parts = {
        "assembler": assembler or FakeAssembler(),
        "harness": harness,
        "gate": gate,
        "ctx_builder": FakeCtxBuilder(ctx),
        "governor": governor or FakeGovernor(),
        "mode": mode or FakeMode(),
        "kill": kill or FakeKill(),
        "notify": notify or Notifier(),
        "store": store or FakeStore(),
        "exposure": ExposureTracker(conn, clock, CAPITAL_BASE),
    }
    pipeline = RecommendationPipeline(
        parts["assembler"], harness, agent_defs(), gate, parts["ctx_builder"], book,
        parts["mode"], parts["kill"], parts["governor"], parts["exposure"], limits,
        parts["notify"], clock, calendar, conn, parts["store"], rearm=rearm,
        funnel_raw=funnel_raw, admission_mode=admission_mode,
        forward_drain_mode=forward_drain_mode,
    )
    return pipeline, parts


def real_gate(limit_table: LimitTable, cost_model: CostModel, clock: Clock) -> RiskGate:
    return RiskGate(StubLimits(limit_table), cost_model, clock)


def verdict_of(kind: str, cost_model: CostModel, **overrides: Any) -> GateVerdict:
    base: dict[str, Any] = {
        "verdict_id": str(ULID()),
        "proposal_id": "01PROPOSAL",
        "verdict": kind,
        "original_qty": 10,
        "approved_qty": 10,
        "checks": [CheckResult(rule_id="per_trade_risk", passed=True, value="ok", limit="ok",
                               headroom="clear")],
        "cost": cost_model.round_trip(Decimal("1000"), "MIS"),
        "reasons": [],
        "mode": Mode.RECOMMEND,
        "risk_state": RiskState.NORMAL,
        "degrade_tier": "DG0",
        "evaluated_at": NOW,
    }
    return GateVerdict(**{**base, **overrides})


def make_rec(cost_model: CostModel, **overrides: Any) -> Recommendation:
    base: dict[str, Any] = {
        "rec_id": str(ULID()),
        "created_at": NOW,
        "valid_until": NOW + timedelta(minutes=TTL_INTRADAY_MIN),
        "kind": "entry",
        "instrument": SYMBOL,
        "side": "BUY",
        "style": "intraday",
        "product": "MIS",
        "entry_zone": (Decimal("100"), Decimal("100")),
        "stop": Decimal("99"),
        "targets": [Decimal("103")],
        "qty": 10,
        "notional": Decimal("1000"),
        "thesis": "Opening-range breakout with volume confirmation and a tight invalidation.",
        "confidence": 0.7,
        "gate": verdict_of("approve", cost_model),
        "cost": cost_model.round_trip(Decimal("1000"), "MIS"),
        "manual_checklist": ["enter BUY RELIANCE (MIS) in 100-100 (limit)"],
    }
    return Recommendation(**{**base, **overrides})


LEDGER_FIELDS: dict[str, Any] = {
    "strategy_id": "orb",
    "features_snapshot_id": "01SNAP",
    "thesis": "t" * 30,
    "confidence": 0.7,
    "agent_id": "intraday_analyst",
    "baseline_signal": 1,
    "llm_filter_decision": "confirmed",
    "catalyst_ref": "01CATREF",
}


# =========================================================================== trigger (a) â€” entries
async def test_happy_path_writes_the_full_provenance_chain(
    conn, pclock, calendar, book, limit_table, cost_model
):
    """proposals â†’ verdicts â†’ recommendations â†’ learning_ledger, plus a rendered owner message."""
    harness = FakeHarness(dict(ENTER_JSON))
    pipeline, parts = make_pipeline(
        conn=conn, clock=pclock, calendar=calendar, book=book, harness=harness,
        gate=real_gate(limit_table, cost_model, pclock), ctx=passing_ctx(),
        limits=StubLimits(limit_table),
    )
    await publish_candidate(pipeline, candidate())

    proposal = conn.execute("SELECT * FROM proposals").fetchone()
    verdict = conn.execute("SELECT * FROM verdicts").fetchone()
    rec_row = conn.execute("SELECT * FROM recommendations").fetchone()
    ledger = conn.execute("SELECT * FROM learning_ledger").fetchone()

    assert proposal["action"] == "enter" and proposal["agent_id"] == "intraday_analyst"
    # inputs_digest is PLATFORM-stamped from the assembled context â€” the replay key (R8).
    assert proposal["inputs_digest"] == parts["assembler"].contexts[0].inputs_digest
    assert verdict["verdict"] == "approve" and verdict["proposal_id"] == proposal["proposal_id"]

    payload = json.loads(rec_row["payload"])
    assert payload["kind"] == "entry" and payload["instrument"] == SYMBOL and payload["qty"] == 10
    assert rec_row["human_action"] is None and rec_row["delivered_at"]
    # valid_until is PLATFORM-stamped from Clock, never model-emitted (Â§3.2).
    assert payload["valid_until"] == (NOW + timedelta(minutes=TTL_INTRADAY_MIN)).isoformat()

    assert ledger["rec_id"] == rec_row["rec_id"] and ledger["is_paper"] == 0
    assert ledger["baseline_signal"] == 1 and ledger["llm_filter_decision"] == "confirmed"
    assert ledger["proposal_id"] == proposal["proposal_id"]
    assert ledger["verdict_id"] == verdict["verdict_id"]
    assert ledger["outcome_label"] is None and ledger["net_pnl"] is None   # open until close/expiry

    message = parts["notify"].messages[0]
    assert message.kind == MessageKind.RECOMMENDATION
    assert message.data["rec_id"] == rec_row["rec_id"]
    rendered = message.render()
    assert "checklist" in rendered and "SL-M at 99" in rendered and "square off by" in rendered


@pytest.mark.parametrize(
    ("label", "kwargs"),
    [
        ("mode OFF", {"mode": FakeMode(mode=Mode.OFF)}),
        ("risk FROZEN", {"mode": FakeMode(risk_state=RiskState.FROZEN)}),
        ("killed", {"kill": FakeKill(killed=True)}),
        ("governor blocked", {"governor": FakeGovernor(allowed=False)}),
    ],
)
async def test_gatekeepers_fail_to_no_proposal(
    conn, pclock, calendar, book, limit_table, cost_model, label, kwargs
):
    harness = FakeHarness()                 # any call at all would raise "more times than results"
    pipeline, _ = make_pipeline(
        conn=conn, clock=pclock, calendar=calendar, book=book, harness=harness,
        gate=real_gate(limit_table, cost_model, pclock), ctx=passing_ctx(),
        limits=StubLimits(limit_table), **kwargs,
    )
    await publish_candidate(pipeline, candidate())
    assert harness.calls == [], label
    assert conn.execute("SELECT COUNT(*) FROM proposals").fetchone()[0] == 0


async def test_out_of_window_never_calls_the_analyst(
    conn, ticker, pclock, calendar, book, limit_table, cost_model
):
    """Entry-seeking calls fire ONLY inside the owner-set window (Â§7.1 trade_window / Â§1.4 item 11)."""
    ticker.at = datetime(2026, 6, 17, 11, 0, tzinfo=IST)      # seeded window is 10:00â€“10:30
    harness = FakeHarness()
    pipeline, _ = make_pipeline(
        conn=conn, clock=pclock, calendar=calendar, book=book, harness=harness,
        gate=real_gate(limit_table, cost_model, pclock), ctx=passing_ctx(),
        limits=StubLimits(limit_table),
    )
    await publish_candidate(pipeline, candidate())
    assert harness.calls == []
    assert conn.execute("SELECT COUNT(*) FROM recommendations").fetchone()[0] == 0


async def test_agent_failure_alerts_and_writes_no_proposal(
    conn, pclock, calendar, book, limit_table, cost_model
):
    """D7: schema-invalid/timeout/SDK death â‡’ no proposal + owner alert, never a salvaged action."""
    harness = FakeHarness(AgentResult.Failed("timeout", "45s elapsed", call_id="01CALL"))
    pipeline, parts = make_pipeline(
        conn=conn, clock=pclock, calendar=calendar, book=book, harness=harness,
        gate=real_gate(limit_table, cost_model, pclock), ctx=passing_ctx(),
        limits=StubLimits(limit_table),
    )
    await publish_candidate(pipeline, candidate())

    assert conn.execute("SELECT COUNT(*) FROM proposals").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM recommendations").fetchone()[0] == 0
    assert len(parts["notify"].messages) == 1
    assert parts["notify"].messages[0].data["value"] == "timeout"


async def test_agent_infra_failure_rearms_the_prescreen_slot(
    conn, pclock, calendar, book, limit_table, cost_model
):
    """Owner-directed 2026-07-29: an analyst INFRASTRUCTURE failure hands the (symbol, strategy)
    day slot back so a still-true condition can re-publish; a governor block (budget policy,
    the call never went out) must NOT re-arm."""
    rearmed: list[tuple[str, str]] = []

    harness = FakeHarness(AgentResult.Failed("timeout", "45s elapsed", call_id="01CALL"))
    pipeline, _ = make_pipeline(
        conn=conn, clock=pclock, calendar=calendar, book=book, harness=harness,
        gate=real_gate(limit_table, cost_model, pclock), ctx=passing_ctx(),
        limits=StubLimits(limit_table),
        rearm=lambda sym, sid: rearmed.append((sym, sid)) or True,
    )
    cand = candidate()
    await publish_candidate(pipeline, cand)
    assert rearmed == [(cand.symbol, cand.strategy_id)]

    rearmed.clear()
    pipeline2, _ = make_pipeline(
        conn=conn, clock=pclock, calendar=calendar, book=book, harness=FakeHarness(),
        gate=real_gate(limit_table, cost_model, pclock), ctx=passing_ctx(),
        limits=StubLimits(limit_table), governor=FakeGovernor(allowed=False),
        rearm=lambda sym, sid: rearmed.append((sym, sid)) or True,
    )
    await publish_candidate(pipeline2, candidate())
    assert rearmed == []                                   # governor block: no re-arm


async def test_never_evaluated_drops_rearm_the_slot(
    conn, ticker, pclock, calendar, book, limit_table, cost_model
):
    """2026-07-29 owner decision: out-of-window / mode / freeze drops re-arm the day slot (the
    candidate was never evaluated) â€” so a still-true condition is waiting when the window opens."""
    rearmed: list[tuple[str, str]] = []

    ticker.at = datetime(2026, 6, 17, 11, 0, tzinfo=IST)      # outside the seeded 10:00â€“10:30 window
    pipeline, _ = make_pipeline(
        conn=conn, clock=pclock, calendar=calendar, book=book, harness=FakeHarness(),
        gate=real_gate(limit_table, cost_model, pclock), ctx=passing_ctx(),
        limits=StubLimits(limit_table),
        rearm=lambda sym, sid: rearmed.append((sym, sid)) or True,
    )
    cand = candidate()
    await publish_candidate(pipeline, cand)
    assert rearmed == [(cand.symbol, cand.strategy_id)]       # out-of-window â†’ slot back

    rearmed.clear()
    ticker.at = datetime(2026, 6, 17, 10, 15, tzinfo=IST)     # back inside the window
    pipeline_off, _ = make_pipeline(
        conn=conn, clock=pclock, calendar=calendar, book=book, harness=FakeHarness(),
        gate=real_gate(limit_table, cost_model, pclock), ctx=passing_ctx(),
        limits=StubLimits(limit_table), mode=FakeMode(mode=Mode.OFF),
        rearm=lambda sym, sid: rearmed.append((sym, sid)) or True,
    )
    await publish_candidate(pipeline_off, candidate())
    assert len(rearmed) == 1                                  # mode OFF â†’ slot back


async def test_day_slot_journal_and_rehydration_round_trip(
    conn, ticker, pclock, calendar, book, limit_table, cost_model
):
    """2026-08-04 (owner-directed): the prescreen's dedupe/caps day state was process memory, so a
    restart reset the 20/day bound (~54 publications observed across two restarts). Publications now
    journal to ``prescreen_day_slots`` â€” evaluated=1 on receipt (the conservative default for every
    handler path), flipped to 0 by the never-evaluated rearm paths â€” and ``_hydrate_prescreen``
    rebuilds a fresh prescreen from those rows exactly per the 2026-07-29 rearm semantics."""
    from datetime import date as _date

    from engine.ops.main import _hydrate_prescreen
    from engine.strategy.prescreen import SignalPreScreen
    from engine.strategy.types import RawLevels as _RL
    from engine.strategy.types import ScanContext as _SC
    from engine.strategy.types import SignalCandidate as _Cand

    # (a) An EVALUATED candidate (analyst ran, said no_action): journal row stays evaluated=1.
    ticker.at = datetime(2026, 6, 17, 10, 15, tzinfo=IST)
    pipeline, _ = make_pipeline(
        conn=conn, clock=pclock, calendar=calendar, book=book,
        harness=FakeHarness(dict(NO_ACTION_JSON)),
        gate=real_gate(limit_table, cost_model, pclock), ctx=passing_ctx(),
        limits=StubLimits(limit_table),
    )
    await publish_candidate(pipeline, candidate())
    rows = conn.execute(
        "SELECT strategy_id, evaluated FROM prescreen_day_slots WHERE d='2026-06-17'"
    ).fetchall()
    assert {(r["strategy_id"], r["evaluated"]) for r in rows} == {("orb", 1)}

    # (b) A NEVER-EVALUATED drop (out of window): journalled on receipt, flipped to evaluated=0.
    ticker.at = datetime(2026, 6, 17, 11, 0, tzinfo=IST)
    pipeline2, _ = make_pipeline(
        conn=conn, clock=pclock, calendar=calendar, book=book, harness=FakeHarness(),
        gate=real_gate(limit_table, cost_model, pclock), ctx=passing_ctx(),
        limits=StubLimits(limit_table), rearm=lambda sym, sid: True,
    )
    await publish_candidate(pipeline2, candidate(strategy_id="rsi2", signal_id="01SIGNAL2"))
    by_sid = {
        r["strategy_id"]: r["evaluated"] for r in conn.execute(
            "SELECT strategy_id, evaluated FROM prescreen_day_slots WHERE d='2026-06-17'"
        ).fetchall()
    }
    assert by_sid == {"orb": 1, "rsi2": 0}

    # (c) "Restart": a FRESH prescreen hydrated from the journal â€” the evaluated pair is deduped,
    # the in-flight-lost pair re-publishes within its already-paid cap slot.
    def _ext(strategy_id: str) -> _Cand:
        return _Cand(
            signal_id=f"sig-{strategy_id}", strategy_id=strategy_id, symbol=SYMBOL, side="BUY",
            style="intraday", raw_levels=_RL(entry=Decimal("100"), stop=Decimal("99")), score=0.5,
        )

    ps = SignalPreScreen([], lambda bar: _SC(), None, max_candidates_per_day=2)
    _hydrate_prescreen(conn, ps, _date(2026, 6, 17))
    day = _date(2026, 6, 17)
    assert ps.admit([_ext("orb")], day) == []                       # evaluated â†’ still deduped
    assert ps.admit([_ext("trend")], day) == []                     # cap full (2 charged pairs)
    assert [c.strategy_id for c in ps.admit([_ext("rsi2")], day)] == ["rsi2"]   # paid quota re-publish


async def test_swing_max_qty_charges_the_overnight_gap_mult_like_the_gate(
    conn, pclock, calendar, book, limit_table, cost_model
):
    """2026-08-26: the gate's first three LIVE verdicts all rejected on ``per_trade_risk`` because
    this quote omitted the overnight gap multiplier — every swing proposal arrived sized ~2.5× over
    the real budget and prompt rule 7 had bound the analyst to the bad number. The quote must
    MIRROR the gate's unit-risk formula: gap-charged for swing/position, raw distance intraday."""
    pipeline, _ = make_pipeline(
        conn=conn, clock=pclock, calendar=calendar, book=book, harness=FakeHarness(),
        gate=real_gate(limit_table, cost_model, pclock), ctx=passing_ctx(),
        limits=StubLimits(limit_table),
    )
    ptr = limit_table.limits.per_trade_risk
    entry, stop = Decimal("100.00"), Decimal("98.00")
    swing = candidate(style="swing", raw_levels=RawLevels(entry=entry, stop=stop, target=None))
    intraday = candidate(
        style="intraday", raw_levels=RawLevels(entry=entry, stop=stop, target=None)
    )
    equity = pipeline._exposure.equity()
    gap = Decimal(str(ptr.overnight_gap_mult))
    expected_swing = int(
        (Decimal(str(ptr.swing_position_pct)) / 100 * equity) / ((entry - stop) * gap)
    )
    expected_intraday = int(
        (Decimal(str(ptr.intraday_pct)) / 100 * equity) / (entry - stop)
    )
    assert pipeline._max_qty_by_risk(swing) == expected_swing
    assert pipeline._max_qty_by_risk(intraday) == expected_intraday
    assert gap > 1 and expected_swing < expected_intraday


async def test_stopless_candidate_never_reaches_the_analyst(
    conn, pclock, calendar, book, limit_table, cost_model
):
    """2026-07-29 owner decision: no stop level â‡’ max_qty_by_risk is 0 â‡’ a guaranteed no_action â€”
    the analyst call is never spent (today: `mom` until rebalance state lands). The slot stays
    consumed (no stop will appear today) and the forward cap is not charged."""
    rearmed: list[tuple[str, str]] = []
    harness = FakeHarness()                 # any call would raise "more times than results"
    pipeline, _ = make_pipeline(
        conn=conn, clock=pclock, calendar=calendar, book=book, harness=harness,
        gate=real_gate(limit_table, cost_model, pclock), ctx=passing_ctx(),
        limits=StubLimits(limit_table),
        rearm=lambda sym, sid: rearmed.append((sym, sid)) or True,
    )
    stopless = candidate(raw_levels=RawLevels(entry=Decimal("100.00")))
    await publish_candidate(pipeline, stopless)
    assert harness.calls == []                                # no analyst spend
    assert rearmed == []                                      # slot deliberately NOT re-armed
    assert pipeline._forwarded_count == 0                     # forward cap untouched

    row = conn.execute(
        "SELECT unsizeable FROM prescreen_day_slots WHERE d=? AND symbol=? AND strategy_id=?",
        (TODAY.isoformat(), stopless.symbol, stopless.strategy_id),
    ).fetchone()
    assert row["unsizeable"] == 1                             # distinguishable from real starvation


async def test_stopless_candidate_never_appears_in_best_unforwarded(
    conn, pclock, calendar, book, limit_table, cost_model
):
    """2026-08-14 nightly-review confusion: a `mom` candidate scored 1.0 with no stop was journalled
    like any other refused candidate, so the funnel's ``best_unforwarded_score`` (the direct
    starvation reading, WO-9) reported 1.0 sitting unforwarded â€” indistinguishable from a good
    candidate that never got an analyst slot. A structurally-unsizeable candidate was never eligible
    for a slot in the first place and must never surface there."""
    from engine.ops.nightly_review import build_funnel_summary

    pipeline, _ = make_pipeline(
        conn=conn, clock=pclock, calendar=calendar, book=book, harness=FakeHarness(),
        gate=real_gate(limit_table, cost_model, pclock), ctx=passing_ctx(),
        limits=StubLimits(limit_table), rearm=lambda sym, sid: True,
    )
    stopless = candidate(
        strategy_id="mom", raw_levels=RawLevels(entry=Decimal("100.00")), score=1.0,
    )
    await publish_candidate(pipeline, stopless)

    summary = build_funnel_summary(conn, TODAY)
    assert summary.best_unforwarded_score is None
    mom = {s.strategy_id: s for s in summary.by_strategy}["mom"]
    assert mom.best_unforwarded_score is None
    assert mom.unsizeable == 1


async def test_qty_zero_candidate_never_reaches_the_analyst(
    conn, pclock, calendar, book, limit_table, cost_model
):
    """2026-08-14 extension: a candidate WITH a real stop can still be structurally unsizeable when
    the §7.1 per-trade-risk budget cannot afford even one share at that stop's absolute distance â€”
    OFSS-shaped, live 2026-08-14: entry 11366.0 / stop 10911.35 (â‚¹454.65/share) against a â‚¹400 (2%
    swing) budget on the â‚¹20,000 test account. Same funnel category as the stopless case: journalled
    unsizeable, never enqueued, never forwarded, no analyst spend â€” and never surfaces as the day's
    best unforwarded score even though nothing else was published."""
    from engine.ops.nightly_review import build_funnel_summary

    rearmed: list[tuple[str, str]] = []
    harness = FakeHarness()                 # any call would raise "more times than results"
    pipeline, _ = make_pipeline(
        conn=conn, clock=pclock, calendar=calendar, book=book, harness=harness,
        gate=real_gate(limit_table, cost_model, pclock), ctx=passing_ctx(),
        limits=StubLimits(limit_table),
        rearm=lambda sym, sid: rearmed.append((sym, sid)) or True,
    )
    ofss = candidate(
        symbol="OFSS", strategy_id="rsi2", style="swing",
        raw_levels=RawLevels(entry=Decimal("11366.0"), stop=Decimal("10911.35")),
        score=0.48,
    )
    await publish_candidate(pipeline, ofss)

    assert harness.calls == []                                # no analyst spend
    assert rearmed == []                                      # slot deliberately NOT re-armed
    assert pipeline._forwarded_count == 0                     # forward cap untouched
    assert pipeline._pending_forwards == []                   # never enqueued

    row = conn.execute(
        "SELECT unsizeable FROM prescreen_day_slots WHERE d=? AND symbol=? AND strategy_id=?",
        (TODAY.isoformat(), "OFSS", "rsi2"),
    ).fetchone()
    assert row["unsizeable"] == 1

    summary = build_funnel_summary(conn, TODAY)
    assert summary.unsizeable == 1
    assert summary.best_unforwarded_score is None
    rsi2 = {s.strategy_id: s for s in summary.by_strategy}["rsi2"]
    assert rsi2.unsizeable == 1
    assert rsi2.best_unforwarded_score is None


async def test_sizeable_candidate_is_unaffected_by_the_qty_zero_check(
    conn, pclock, calendar, book, limit_table, cost_model
):
    """The qty-zero check must not become a second, over-eager stopless gate: a candidate whose stop
    distance the risk budget CAN afford still enqueues and reaches the analyst exactly as before."""
    harness = FakeHarness(dict(NO_ACTION_JSON))
    pipeline, _ = make_pipeline(
        conn=conn, clock=pclock, calendar=calendar, book=book, harness=harness,
        gate=real_gate(limit_table, cost_model, pclock), ctx=passing_ctx(),
        limits=StubLimits(limit_table),
    )
    await publish_candidate(pipeline, candidate())   # default: intraday, entry 100 / stop 99

    assert len(harness.calls) == 1                            # analyst WAS called
    row = conn.execute(
        "SELECT unsizeable FROM prescreen_day_slots WHERE d=? AND symbol=? AND strategy_id=?",
        (TODAY.isoformat(), SYMBOL, "orb"),
    ).fetchone()
    assert row["unsizeable"] == 0


async def test_no_action_records_the_regime_note_and_no_proposal(
    conn, pclock, calendar, book, limit_table, cost_model
):
    harness = FakeHarness(dict(NO_ACTION_JSON))
    pipeline, parts = make_pipeline(
        conn=conn, clock=pclock, calendar=calendar, book=book, harness=harness,
        gate=real_gate(limit_table, cost_model, pclock), ctx=passing_ctx(),
        limits=StubLimits(limit_table),
    )
    await publish_candidate(pipeline, candidate())

    assert conn.execute("SELECT COUNT(*) FROM proposals").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM recommendations").fetchone()[0] == 0
    assert parts["assembler"].regime_notes == [NO_ACTION_JSON["regime_note"]]
    assert parts["notify"].messages == []


async def test_shrink_verdict_resizes_the_recommendation(
    conn, pclock, calendar, book, limit_table, cost_model
):
    """R1: the gate may only shrink â€” the delivered size and notional are the APPROVED ones."""
    harness = FakeHarness(dict(ENTER_JSON))
    gate = StubGate(verdict_of("shrink", cost_model, original_qty=10, approved_qty=4,
                               reasons=["shrink: qty 10 -> 4 (bound by per_trade_risk)"]))
    pipeline, _ = make_pipeline(
        conn=conn, clock=pclock, calendar=calendar, book=book, harness=harness, gate=gate,
        ctx=passing_ctx(), limits=StubLimits(limit_table),
    )
    await publish_candidate(pipeline, candidate())

    payload = json.loads(conn.execute("SELECT payload FROM recommendations").fetchone()[0])
    assert payload["qty"] == 4
    assert Decimal(payload["notional"]) == Decimal("400")       # 4 x the 100 zone low, recomputed
    assert payload["gate"]["verdict"] == "shrink"


async def test_owner_approval_required_opens_a_pending_row(
    conn, pclock, calendar, book, limit_table, cost_model
):
    harness = FakeHarness(dict(ENTER_JSON))
    gate = StubGate(verdict_of("owner_approval_required", cost_model,
                               reasons=["routed to the owner"]))
    pipeline, parts = make_pipeline(
        conn=conn, clock=pclock, calendar=calendar, book=book, harness=harness, gate=gate,
        ctx=passing_ctx(), limits=StubLimits(limit_table),
    )
    await publish_candidate(pipeline, candidate())

    row = conn.execute("SELECT * FROM owner_approvals").fetchone()
    assert row["status"] == "pending" and row["kind"] == "entry"
    body = json.loads(row["payload"])
    assert body["action"] == "enter" and body["tradingsymbol"] == SYMBOL
    assert conn.execute("SELECT COUNT(*) FROM recommendations").fetchone()[0] == 0
    assert parts["notify"].messages[0].data["approval_id"] == row["approval_id"]


async def test_market_entry_zone_spans_the_sanity_band(
    conn, pclock, calendar, book, limit_table, cost_model
):
    """MARKET has no price yet â‡’ the zone runs to the Â§7.1 entry_sanity_band edge (+1% MIS, +2% CNC)."""
    pipeline, _ = make_pipeline(
        conn=conn, clock=pclock, calendar=calendar, book=book, harness=FakeHarness(),
        gate=real_gate(limit_table, cost_model, pclock), ctx=passing_ctx(),
        limits=StubLimits(limit_table),
    )
    common = {**ENTER_JSON, "entry_type": "MARKET", "entry_price": None,
              "stop_price": "247.50", "target_price": "256",
              "proposal_id": "01P", "agent_id": "a", "valid_until": NOW + timedelta(minutes=20),
              "inputs_digest": "d"}
    mis = pipeline.build_recommendation(
        EnterAction(**common), verdict_of("approve", cost_model), Decimal("250.00")
    )
    cnc = pipeline.build_recommendation(
        EnterAction(**{**common, "style": "swing"}), verdict_of("approve", cost_model),
        Decimal("250.00"),
    )
    assert mis.entry_zone == (Decimal("250.00"), Decimal("252.50")) and mis.product == "MIS"
    assert cnc.entry_zone == (Decimal("250.00"), Decimal("255.00")) and cnc.product == "CNC"
    assert mis.notional == Decimal("2500.00")                   # qty 10 x the zone LOW
    assert "GTT OCO stop 247.50 / target 256" in " ".join(cnc.manual_checklist)   # CNC, not SL-M
    assert "SL-M at 247.50" in " ".join(mis.manual_checklist)     # MIS wording
    assert not any("square off by" in item for item in cnc.manual_checklist)   # MIS-only line
    assert "set alert at 248.75" in mis.manual_checklist          # stop + 0.5 x (entry - stop)


# =========================================================================== the ledger matrix (Â§3.6)
async def test_take_close_veto_and_the_worked_pnl(conn, ticker, pclock, book, cost_model):
    """The full outcome-capture matrix, to the paisa, with the Â§6.5 label it produces."""
    rec = make_rec(cost_model)
    book.deliver(rec, ledger_fields=dict(LEDGER_FIELDS))

    # -- /taken -------------------------------------------------------------------------
    summary = await book.take(rec.rec_id, 10, Decimal("100.00"))
    position = conn.execute("SELECT * FROM positions").fetchone()
    assert position["position_id"] in summary and "B7" in summary   # owner sees the id + who protects
    assert position["origin"] == "recommended" and position["state"] == "OPEN"
    assert position["protection_state"] is None and position["is_paper"] == 0
    assert position["symbol"] == SYMBOL and position["product"] == "MIS" and position["qty"] == 10
    assert position["avg_entry"] == "100.00" and position["stop"] == "99" and position["target"] == "103"
    rec_row = conn.execute("SELECT * FROM recommendations").fetchone()
    assert rec_row["human_action"] == "taken" and rec_row["human_fill_price"] == "100.00"
    ledger = conn.execute("SELECT * FROM learning_ledger").fetchone()
    assert ledger["entry_px"] == "100.00" and ledger["qty"] == 10
    assert ledger["position_id"] == position["position_id"]

    with pytest.raises(ValueError, match="already 'taken'"):
        await book.take(rec.rec_id, 10, Decimal("100.00"))
    with pytest.raises(ValueError, match="unknown recommendation"):
        await book.take("no-such-rec", 1, Decimal("1"))

    # -- /closed: the worked example ------------------------------------------------------
    ticker.at = NOW + timedelta(minutes=45)
    await book.close(rec.rec_id, Decimal("103.50"))

    expected_gross = (Decimal("103.50") - Decimal("100.00")) * 10        # â‚¹35.00
    expected_costs = cost_model.round_trip(Decimal("1035.00"), "MIS").total_cost
    expected_net = expected_gross - expected_costs
    assert expected_gross == Decimal("35.00")
    assert expected_net > 0                                              # a win at the shipped rates

    ledger = conn.execute("SELECT * FROM learning_ledger").fetchone()
    assert Decimal(ledger["gross_pnl"]) == expected_gross
    assert Decimal(ledger["costs"]) == expected_costs
    assert Decimal(ledger["net_pnl"]) == expected_net
    assert ledger["exit_px"] == "103.50" and ledger["holding_minutes"] == 45
    assert ledger["outcome_label"] == "win" and ledger["closed_at"]

    closed = conn.execute("SELECT * FROM positions").fetchone()
    assert closed["state"] == "CLOSED" and closed["close_reason"] == "manual_owner"
    # positions.realized_pnl is GROSS + a separate costs column: ExposureTracker computes equity as
    # Î£(realized_pnl âˆ’ costs), so writing net here would charge the costs twice (Â§7.1).
    assert Decimal(closed["realized_pnl"]) == expected_gross
    assert Decimal(closed["costs"]) == expected_costs
    assert ExposureTracker(conn, pclock, CAPITAL_BASE).realized_net() == expected_net

    outcome = json.loads(conn.execute("SELECT outcome FROM recommendations").fetchone()[0])
    assert outcome == {"exit_price": "103.50", "gross": "35.00", "net": str(expected_net)}

    with pytest.raises(ValueError, match="no OPEN position"):
        await book.close(rec.rec_id, Decimal("103.50"))


async def test_close_labels_a_loss_and_a_scratch(conn, pclock, book, cost_model):
    loser = make_rec(cost_model)
    book.deliver(loser, ledger_fields=dict(LEDGER_FIELDS))
    await book.take(loser.rec_id, 10, Decimal("100.00"))
    await book.close(loser.rec_id, Decimal("96.00"))
    row = conn.execute(
        "SELECT * FROM learning_ledger WHERE rec_id=?", (loser.rec_id,)
    ).fetchone()
    assert Decimal(row["gross_pnl"]) == Decimal("-40.00")
    assert Decimal(row["net_pnl"]) == Decimal("-40.00") - Decimal(row["costs"])
    assert row["outcome_label"] == "loss"

    # A zero-cost book is the only way to land net == 0 exactly at the shipped tick size.
    free = RecommendationBook(conn, pclock, ZeroCostModel())
    flat = make_rec(cost_model)
    free.deliver(flat, ledger_fields=dict(LEDGER_FIELDS))
    await free.take(flat.rec_id, 10, Decimal("100.00"))
    await free.close(flat.rec_id, Decimal("100.00"))
    row = conn.execute("SELECT * FROM learning_ledger WHERE rec_id=?", (flat.rec_id,)).fetchone()
    assert Decimal(row["net_pnl"]) == Decimal("0.00") and row["outcome_label"] == "scratch"


async def test_veto_records_a_no_action_outcome(conn, book, cost_model):
    rec = make_rec(cost_model)
    book.deliver(rec, ledger_fields=dict(LEDGER_FIELDS))
    await book.veto(rec.rec_id)

    assert conn.execute("SELECT human_action FROM recommendations").fetchone()[0] == "dismissed"
    ledger = conn.execute("SELECT * FROM learning_ledger").fetchone()
    assert ledger["outcome_label"] == "no_action" and ledger["closed_at"]
    with pytest.raises(ValueError, match="already 'dismissed'"):
        await book.veto(rec.rec_id)


async def test_expire_stale_labels_non_fills_and_is_idempotent(conn, book, cost_model):
    """Â§3.6: a non-fill is itself training signal â€” attribution must not be biased to taken trades."""
    stale = make_rec(cost_model, valid_until=NOW - timedelta(minutes=1))
    live = make_rec(cost_model, valid_until=NOW + timedelta(hours=2))
    taken = make_rec(cost_model, valid_until=NOW - timedelta(minutes=1))
    for rec in (stale, live, taken):
        book.deliver(rec, ledger_fields=dict(LEDGER_FIELDS))
    await book.take(taken.rec_id, 10, Decimal("100.00"))

    assert book.expire_stale(NOW) == 1
    assert book.expire_stale(NOW) == 0                       # idempotent â€” human_action IS NULL filter

    actions = dict(conn.execute("SELECT rec_id, human_action FROM recommendations").fetchall())
    assert actions[stale.rec_id] == "expired"
    assert actions[live.rec_id] is None
    assert actions[taken.rec_id] == "taken"
    labels = dict(conn.execute("SELECT rec_id, outcome_label FROM learning_ledger").fetchall())
    assert labels[stale.rec_id] == "no_action" and labels[live.rec_id] is None
    assert labels[taken.rec_id] is None


# =========================================================================== trigger (b) + max_holding
def _open_position(conn, clock, *, style="intraday", opened_at=None, stop="99", side="BUY") -> str:
    position_id = str(ULID())
    conn.execute(
        "INSERT INTO positions (position_id, symbol, side, style, product, qty, avg_entry, stop, "
        "state, origin, opened_at) VALUES (?, ?, ?, ?, ?, 10, '100', ?, 'OPEN', 'recommended', ?)",
        (position_id, SYMBOL, side, style, "MIS" if style == "intraday" else "CNC", stop,
         (opened_at or clock.now()).isoformat()),
    )
    return position_id


def _flat_bars(n: int = 20) -> list[Bar]:
    """n identical 1m bars with a 1.00 range â‡’ ATR(14,1m) == 1.00 exactly."""
    return [
        Bar(symbol=SYMBOL, ts_minute=NOW - timedelta(minutes=n - i), open=Decimal("100"),
            high=Decimal("100.50"), low=Decimal("99.50"), close=Decimal("100"), volume=1000)
        for i in range(n)
    ]


async def test_stop_proximity_fires_once_then_debounces(
    conn, ticker, pclock, calendar, book, limit_table, cost_model
):
    """Â§5.2 (b): risk-reducing, never window-gated â€” and at most once per position per hour."""
    position_id = _open_position(conn, pclock)
    exit_json = {
        "action": "exit", "position_id": position_id, "exit_type": "MARKET",
        "reason": "risk_event", "confidence": 0.9,
        "thesis": "Price is inside half an ATR of the stop; the breakout thesis is failing.",
    }
    harness = FakeHarness(dict(exit_json), dict(exit_json))
    ticker.at = datetime(2026, 6, 17, 14, 0, tzinfo=IST)     # OUTSIDE the 10:00-10:30 entry window
    pipeline, parts = make_pipeline(
        conn=conn, clock=pclock, calendar=calendar, book=book, harness=harness,
        gate=real_gate(limit_table, cost_model, pclock),
        ctx=passing_ctx(positions_known=frozenset({position_id})),
        limits=StubLimits(limit_table), store=FakeStore(bars=_flat_bars()),
    )
    # 0.5 x ATR(1.00) = 0.50; stop 99 â‡’ anything at or below 99.50 is "near".
    near = Bar(symbol=SYMBOL, ts_minute=NOW, open=Decimal("99.4"), high=Decimal("99.5"),
               low=Decimal("99.3"), close=Decimal("99.40"), volume=100)

    await pipeline.on_bar(near)
    assert len(harness.calls) == 1
    rec_payload = json.loads(conn.execute("SELECT payload FROM recommendations").fetchone()[0])
    assert rec_payload["kind"] == "exit" and rec_payload["side"] == "SELL"       # closing a long
    assert rec_payload["manual_checklist"] == [f"exit at market: close {SYMBOL} x10 now"]
    assert parts["notify"].messages[-1].kind == MessageKind.RECOMMENDATION

    await pipeline.on_bar(near)                              # same position, same hour â‡’ debounced
    assert len(harness.calls) == 1
    assert conn.execute("SELECT COUNT(*) FROM recommendations").fetchone()[0] == 1

    ticker.at = ticker.at + timedelta(minutes=POSITION_EVENT_DEBOUNCE_MIN + 1)
    await pipeline.on_bar(near)
    assert len(harness.calls) == 2


async def test_far_from_stop_never_calls_the_analyst(
    conn, pclock, calendar, book, limit_table, cost_model
):
    _open_position(conn, pclock)
    harness = FakeHarness()
    pipeline, _ = make_pipeline(
        conn=conn, clock=pclock, calendar=calendar, book=book, harness=harness,
        gate=real_gate(limit_table, cost_model, pclock), ctx=passing_ctx(),
        limits=StubLimits(limit_table), store=FakeStore(bars=_flat_bars()),
    )
    far = Bar(symbol=SYMBOL, ts_minute=NOW, open=Decimal("104"), high=Decimal("104.5"),
              low=Decimal("103.5"), close=Decimal("104"), volume=100)
    await pipeline.on_bar(far)
    assert harness.calls == []


async def test_aged_position_exit_is_deterministic(
    conn, pclock, calendar, book, limit_table, cost_model
):
    """Â§7.1 max_holding: swing > 20 sessions â‡’ an exit recommendation built WITHOUT the LLM (R1)."""
    opened = datetime(2026, 3, 2, 10, 0, tzinfo=IST)          # far more than 20 trading sessions back
    position_id = _open_position(conn, pclock, style="swing", opened_at=opened, stop="95")
    _open_position(conn, pclock, style="swing")               # opened today â‡’ not aged
    harness = FakeHarness()                                   # ANY call raises: exits never use Tier 1
    pipeline, parts = make_pipeline(
        conn=conn, clock=pclock, calendar=calendar, book=book, harness=harness,
        gate=real_gate(limit_table, cost_model, pclock),
        ctx=passing_ctx(positions_known=frozenset({position_id})),
        limits=StubLimits(limit_table),
    )
    issued = await pipeline.check_aged_positions(TODAY)

    assert issued == 1 and harness.calls == []
    payload = json.loads(conn.execute("SELECT payload FROM recommendations").fetchone()[0])
    assert payload["kind"] == "exit" and payload["qty"] == 10
    assert payload["manual_checklist"] == [f"exit at market: close {SYMBOL} x10 now"]
    proposal = json.loads(conn.execute("SELECT payload FROM proposals").fetchone()[0])
    assert proposal["reason"] == "time_stop" and proposal["agent_id"] == "platform"
    assert parts["notify"].messages[-1].kind == MessageKind.RECOMMENDATION


# =========================================================================== trigger (c) â€” heartbeat
async def test_heartbeat_only_updates_the_regime_note(
    conn, pclock, calendar, book, limit_table, cost_model
):
    harness = FakeHarness(dict(NO_ACTION_JSON))
    pipeline, parts = make_pipeline(
        conn=conn, clock=pclock, calendar=calendar, book=book, harness=harness,
        gate=real_gate(limit_table, cost_model, pclock), ctx=passing_ctx(),
        limits=StubLimits(limit_table),
    )
    await pipeline.heartbeat()

    assert parts["assembler"].heartbeats == 1
    assert parts["assembler"].regime_notes == [NO_ACTION_JSON["regime_note"]]
    assert conn.execute("SELECT COUNT(*) FROM proposals").fetchone()[0] == 0
    assert parts["governor"].calls == [("intraday_analyst", "heartbeat")]


async def test_heartbeat_is_window_gated(conn, ticker, pclock, calendar, book, limit_table, cost_model):
    ticker.at = datetime(2026, 6, 17, 14, 0, tzinfo=IST)
    harness = FakeHarness()
    pipeline, parts = make_pipeline(
        conn=conn, clock=pclock, calendar=calendar, book=book, harness=harness,
        gate=real_gate(limit_table, cost_model, pclock), ctx=passing_ctx(),
        limits=StubLimits(limit_table),
    )
    await pipeline.heartbeat()
    assert harness.calls == [] and parts["assembler"].heartbeats == 0


# --------------------------------------------------------------------------- structural coherence (R1)
async def test_identity_mismatch_drops_payload_before_the_gate(
    conn, pclock, calendar, book, limit_table, cost_model
):
    """An analyst payload naming a different symbol than the candidate is a hallucination: dropped
    like schema-invalid output (D7) â€” no proposal row, no verdict, nothing delivered."""
    hijacked = dict(ENTER_JSON, tradingsymbol="SUZLON")
    harness = FakeHarness(hijacked)
    gate = StubGate(verdict_of("approve", cost_model))
    pipeline, parts = make_pipeline(
        conn=conn, clock=pclock, calendar=calendar, book=book, harness=harness, gate=gate,
        ctx=passing_ctx(), limits=StubLimits(limit_table),
    )
    await publish_candidate(pipeline, candidate())
    assert conn.execute("SELECT COUNT(*) FROM proposals").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM recommendations").fetchone()[0] == 0
    assert any("identity mismatch" in str(getattr(m, "title", "")) for m in parts["notify"].messages)


async def test_forward_cap_stops_analyst_calls_for_the_day(
    conn, pclock, calendar, book, limit_table, cost_model
):
    """S5.2(a)/S5.6: at most governor.prescreen_forward_cap() candidates reach the analyst per day."""

    class CappedGovernor(FakeGovernor):
        def prescreen_forward_cap(self) -> int:
            return 2

    harness = FakeHarness(dict(NO_ACTION_JSON), dict(NO_ACTION_JSON))
    gate = StubGate(verdict_of("approve", cost_model))
    pipeline, _ = make_pipeline(
        conn=conn, clock=pclock, calendar=calendar, book=book, harness=harness, gate=gate,
        ctx=passing_ctx(), limits=StubLimits(limit_table), governor=CappedGovernor(),
    )
    for _i in range(4):
        await publish_candidate(pipeline, candidate())
    assert len(harness.calls) == 2


# ======================================================= WO-1: forward queue + journalled counter
class TunableGovernor(FakeGovernor):
    """A governor whose Â§5.2(a) forward cap the test can move â€” the live cap really does move
    (agents.yaml ``prescreen_cap_per_day`` is 12 at DG0 and 4 at DG1+, Â§5.6)."""

    def __init__(self, cap: int) -> None:
        super().__init__()
        self.cap = cap

    def prescreen_forward_cap(self) -> int:
        return self.cap


def forward_journal(conn) -> dict[tuple[str, str], int]:
    return {
        (r["symbol"], r["strategy_id"]): int(r["forwarded"])
        for r in conn.execute(
            "SELECT symbol, strategy_id, forwarded FROM prescreen_day_slots"
        ).fetchall()
    }


async def test_forward_queue_selects_by_per_strategy_quantile_not_raw_score(
    conn, pclock, calendar, book, limit_table, cost_model
):
    """WO-1 (ii): at an analyst slot the queue picks the highest per-strategy score QUANTILE, not
    the highest raw score - scores are comparable within a strategy and meaningless across them.
    Here rsi2's only candidate (0.30) is the top of rsi2's day and orb's 0.95 is the top of orb's;
    the tie inside the top quantile band falls back to fired_at, so the earlier one goes first."""
    gov = TunableGovernor(0)               # no slots yet: everything queues, nothing is lost
    harness = FakeHarness(dict(NO_ACTION_JSON), dict(NO_ACTION_JSON))
    pipeline, parts = make_pipeline(
        conn=conn, clock=pclock, calendar=calendar, book=book, harness=harness,
        gate=StubGate(verdict_of("approve", cost_model)), ctx=passing_ctx(),
        limits=StubLimits(limit_table), governor=gov,
    )
    await publish_candidate(pipeline,
        candidate(symbol="INFY", strategy_id="rsi2", signal_id="R1", score=0.30))
    await publish_candidate(pipeline,
        candidate(symbol="TCS", strategy_id="orb", signal_id="O1", score=0.95))
    await publish_candidate(pipeline,
        candidate(symbol="WIPRO", strategy_id="orb", signal_id="O2", score=0.50))
    assert harness.calls == []                                  # cap 0 - nothing forwarded
    assert set(forward_journal(conn).values()) == {0}

    gov.cap = 1                                                 # one slot opens (DG1 -> DG0)
    await publish_candidate(pipeline,
        candidate(symbol="ITC", strategy_id="orb", signal_id="O3", score=0.01))
    assert parts["assembler"].contexts[-1].stable_block == "stable R1"    # rsi2's top, not orb's
    assert forward_journal(conn)[("INFY", "rsi2")] == 1

    gov.cap = 2                                                 # a second slot
    await publish_candidate(pipeline,
        candidate(symbol="SBIN", strategy_id="orb", signal_id="O4", score=0.02))
    assert parts["assembler"].contexts[-1].stable_block == "stable O1"    # now orb's own best
    assert forward_journal(conn)[("TCS", "orb")] == 1
    assert forward_journal(conn)[("WIPRO", "orb")] == 0         # still queued, still unforwarded


async def test_forward_count_survives_a_mid_day_restart(
    conn, pclock, calendar, book, limit_table, cost_model
):
    """WO-1 (iv): the forward counter is journalled, so a restart RESUMES the day's analyst quota.
    Before this it was process memory - the same defect the 2026-08-04 journal fixed for the
    publication caps, still open on the more expensive of the two bounds."""
    gov = TunableGovernor(3)
    harness = FakeHarness(dict(NO_ACTION_JSON), dict(NO_ACTION_JSON))
    pipeline, _ = make_pipeline(
        conn=conn, clock=pclock, calendar=calendar, book=book, harness=harness,
        gate=StubGate(verdict_of("approve", cost_model)), ctx=passing_ctx(),
        limits=StubLimits(limit_table), governor=gov,
    )
    await publish_candidate(pipeline, candidate(symbol="TCS", strategy_id="orb", score=0.9))
    await publish_candidate(pipeline, candidate(symbol="INFY", strategy_id="orb", score=0.8))
    assert len(harness.calls) == 2
    assert sum(forward_journal(conn).values()) == 2

    # --- the restart: a brand-new pipeline object on the same state DB ---
    harness2 = FakeHarness(dict(NO_ACTION_JSON))
    restarted, _ = make_pipeline(
        conn=conn, clock=pclock, calendar=calendar, book=book, harness=harness2,
        gate=StubGate(verdict_of("approve", cost_model)), ctx=passing_ctx(),
        limits=StubLimits(limit_table), governor=gov,
    )
    await publish_candidate(restarted, candidate(symbol="SBIN", strategy_id="orb", score=0.7))
    await publish_candidate(restarted, candidate(symbol="ITC", strategy_id="orb", score=0.6))
    assert restarted._forwarded_count == 3                      # 2 hydrated + 1, not 1
    assert len(harness2.calls) == 1                             # the 3-call day cap held across it
    assert sum(forward_journal(conn).values()) == 3


async def test_heartbeat_never_consumes_a_forward_slot(
    conn, pclock, calendar, book, limit_table, cost_model
):
    """5.2(c) is regime context only and is metered by the governor, never by the 5.2(a) forward
    cap - a chatty heartbeat must not eat the day's candidate evaluations."""
    gov = TunableGovernor(1)
    harness = FakeHarness(dict(NO_ACTION_JSON), dict(NO_ACTION_JSON), dict(NO_ACTION_JSON))
    pipeline, _ = make_pipeline(
        conn=conn, clock=pclock, calendar=calendar, book=book, harness=harness,
        gate=StubGate(verdict_of("approve", cost_model)), ctx=passing_ctx(),
        limits=StubLimits(limit_table), governor=gov,
    )
    await pipeline.heartbeat()
    await pipeline.heartbeat()
    assert len(harness.calls) == 2
    assert pipeline._forwarded_count == 0
    assert forward_journal(conn) == {}                          # no day slot touched either
    await publish_candidate(pipeline, candidate(symbol="TCS", strategy_id="orb", score=0.9))
    assert len(harness.calls) == 3                              # the one slot was still there
    assert pipeline._forwarded_count == 1


async def test_forward_mode_arrival_is_the_rollback_to_fifo(
    conn, pclock, calendar, book, limit_table, cost_model
):
    """WO-1 risk note: admission_mode='arrival' restores pre-WO-1 first-come-first-served
    forwarding, so a rollback is a settings edit rather than a revert."""
    gov = TunableGovernor(0)
    harness = FakeHarness(dict(NO_ACTION_JSON))
    pipeline, parts = make_pipeline(
        conn=conn, clock=pclock, calendar=calendar, book=book, harness=harness,
        gate=StubGate(verdict_of("approve", cost_model)), ctx=passing_ctx(),
        limits=StubLimits(limit_table), governor=gov, admission_mode="arrival",
    )
    await publish_candidate(pipeline,
        candidate(symbol="ITC", strategy_id="orb", signal_id="LOW", score=0.01))
    await publish_candidate(pipeline,
        candidate(symbol="TCS", strategy_id="orb", signal_id="TOP", score=0.99))
    gov.cap = 1
    await publish_candidate(pipeline,
        candidate(symbol="SBIN", strategy_id="orb", signal_id="MID", score=0.50))
    assert parts["assembler"].contexts[-1].stable_block == "stable LOW"   # earliest, not best


async def test_queued_candidate_expires_instead_of_going_stale(
    conn, pclock, calendar, book, limit_table, ticker, cost_model
):
    """A queued candidate is a REFUSED one we kept a pointer to; it must never be forwarded past
    its own 5.2 TTL horizon - a 90-minute-old breakout level is not the setup the scanner saw.
    Expiry does not re-arm the day slot (the forward cap deliberately never re-arms, 2026-07-29)."""
    gov = TunableGovernor(0)
    harness = FakeHarness(dict(NO_ACTION_JSON))
    pipeline, parts = make_pipeline(
        conn=conn, clock=pclock, calendar=calendar, book=book, harness=harness,
        gate=StubGate(verdict_of("approve", cost_model)), ctx=passing_ctx(),
        limits=StubLimits(limit_table), governor=gov,
    )
    await publish_candidate(pipeline,
        candidate(symbol="TCS", strategy_id="orb", signal_id="STALE", score=0.99))
    ticker.at = NOW + timedelta(minutes=TTL_INTRADAY_MIN + 1)
    gov.cap = 1
    await publish_candidate(pipeline,
        candidate(symbol="ITC", strategy_id="orb", signal_id="FRESH", score=0.10))
    assert parts["assembler"].contexts[-1].stable_block == "stable FRESH"
    assert forward_journal(conn)[("TCS", "orb")] == 0


# ============================================== 2026-08-14: the PACED drain (the WO-1 ranking's teeth)
def paced_pipeline(conn, pclock, calendar, book, limit_table, cost_model, *, cap, results=6):
    """A ranked+paced pipeline whose forward cap the test can move, with canned analyst declines."""
    gov = TunableGovernor(cap)
    harness = FakeHarness(*[dict(NO_ACTION_JSON) for _ in range(results)])
    pipeline, parts = make_pipeline(
        conn=conn, clock=pclock, calendar=calendar, book=book, harness=harness,
        gate=StubGate(verdict_of("approve", cost_model)), ctx=passing_ctx(),
        limits=StubLimits(limit_table), governor=gov,
    )
    return pipeline, parts, harness, gov


async def test_an_arriving_candidate_never_drains_its_own_slot(
    conn, pclock, calendar, book, limit_table, cost_model
):
    """THE 2026-08-14 REGRESSION, in the shape it actually happened.

    Under the inline drain the queue held exactly one entry at every slot â€” the candidate that had
    just arrived â€” so "forward the best pending" chose between a set of one and the day's 12 slots
    went to rsi2 [0.14 .. 0.66] inside the first five minutes while 0.83 arrived later and never got
    one. Paced: the 0.14 only ENQUEUES, and at the next tick the 0.83 that landed inside the same
    interval takes the slot.
    """
    pipeline, parts, harness, _ = paced_pipeline(
        conn, pclock, calendar, book, limit_table, cost_model, cap=12)

    await pipeline.on_signal_candidate(
        candidate(symbol="TATAMOTORS", strategy_id="rsi2", signal_id="EARLY_LOW", score=0.14))
    assert harness.calls == []                                  # NOT evaluated inline
    assert [p.candidate.signal_id for p in pipeline._pending_forwards] == ["EARLY_LOW"]
    assert pipeline._forwarded_count == 0

    await pipeline.on_signal_candidate(
        candidate(symbol="HDFCBANK", strategy_id="rsi2", signal_id="LATE_HIGH", score=0.83))
    assert harness.calls == []                                  # still nothing spent

    assert await pipeline.drain_forward_queue() is True
    assert parts["assembler"].contexts[-1].stable_block == "stable LATE_HIGH"
    assert forward_journal(conn)[("HDFCBANK", "rsi2")] == 1
    assert forward_journal(conn)[("TATAMOTORS", "rsi2")] == 0   # queued, still waiting its turn


async def test_the_drain_spends_one_slot_per_pacing_interval(
    conn, pclock, calendar, book, limit_table, ticker, cost_model
):
    """One candidate per tick, never a burst: draining the whole remaining budget the moment it is
    available is the inline drain again, just batched â€” the day's later candidates would still meet
    an exhausted cap. And a tick INSIDE the interval is a no-op, which is what makes the interval an
    accumulation window rather than a 60 s pulse."""
    pipeline, _, harness, _ = paced_pipeline(
        conn, pclock, calendar, book, limit_table, cost_model, cap=12)
    for i, symbol in enumerate(("TCS", "INFY", "WIPRO")):
        await pipeline.on_signal_candidate(
            candidate(symbol=symbol, strategy_id="orb", signal_id=f"Q{i}", score=0.5 + i / 10))

    assert await pipeline.drain_forward_queue() is True
    assert len(harness.calls) == 1                              # ONE, with three pending

    ticker.at = NOW + timedelta(minutes=FORWARD_PACING_MIN - 1)
    assert await pipeline.drain_forward_queue() is False        # inside the interval: no-op
    assert len(harness.calls) == 1

    ticker.at = NOW + timedelta(minutes=FORWARD_PACING_MIN)
    assert await pipeline.drain_forward_queue() is True
    assert len(harness.calls) == 2
    ticker.at = NOW + timedelta(minutes=2 * FORWARD_PACING_MIN)
    assert await pipeline.drain_forward_queue() is True
    assert len(harness.calls) == 3
    assert pipeline._pending_forwards == []


async def test_the_drain_stops_at_the_forward_cap_and_keeps_the_rest_queued(
    conn, pclock, calendar, book, limit_table, ticker, cost_model
):
    """Â§5.6: the cap bounds analyst SPEND, and a candidate refused at a full cap stays available for
    a slot that opens later (a degrade-tier recovery raises the cap mid-day) â€” pacing must not turn
    that into a drop."""
    pipeline, parts, harness, gov = paced_pipeline(
        conn, pclock, calendar, book, limit_table, cost_model, cap=1)
    await pipeline.on_signal_candidate(
        candidate(symbol="TCS", strategy_id="orb", signal_id="FIRST", score=0.9))
    await pipeline.on_signal_candidate(
        candidate(symbol="INFY", strategy_id="orb", signal_id="SECOND", score=0.8))

    assert await pipeline.drain_forward_queue() is True
    assert len(harness.calls) == 1
    ticker.at = NOW + timedelta(minutes=FORWARD_PACING_MIN)
    assert await pipeline.drain_forward_queue() is False        # cap 1 is spent
    assert len(harness.calls) == 1
    assert [p.candidate.signal_id for p in pipeline._pending_forwards] == ["SECOND"]

    gov.cap = 2                                                 # DG1 -> DG0 mid-day
    ticker.at = NOW + timedelta(minutes=2 * FORWARD_PACING_MIN)
    assert await pipeline.drain_forward_queue() is True
    assert parts["assembler"].contexts[-1].stable_block == "stable SECOND"
    assert forward_journal(conn)[("INFY", "orb")] == 1


async def test_the_drain_skips_a_candidate_past_its_own_ttl(
    conn, pclock, calendar, book, limit_table, ticker, cost_model
):
    """Pacing delays a candidate, so TTL expiry is the thing it must not break: a level the scanner
    saw 20 minutes ago is not the setup any more, and expiry never re-arms the day slot."""
    pipeline, _, harness, _ = paced_pipeline(
        conn, pclock, calendar, book, limit_table, cost_model, cap=12)
    await pipeline.on_signal_candidate(
        candidate(symbol="TCS", strategy_id="orb", signal_id="STALE", score=0.99))

    ticker.at = NOW + timedelta(minutes=TTL_INTRADAY_MIN + 1)   # 10:26, still inside the window
    assert await pipeline.drain_forward_queue() is False
    assert harness.calls == []
    assert pipeline._pending_forwards == []                     # expired out of the queue
    assert pipeline._forwarded_count == 0                       # expiry costs no analyst slot
    assert forward_journal(conn)[("TCS", "orb")] == 0


async def test_the_drain_re_checks_the_window_it_was_queued_under(
    conn, pclock, calendar, book, limit_table, ticker, cost_model
):
    """A queued candidate is dispatched LATER than it arrived, so every entry-path gate has to be
    re-asked at the drain: the window can have closed since (Â§1.4 item 11 / Â§7.1 trade_window). A
    closed gate leaves the queue untouched â€” it never re-arms, exactly like the cap (2026-07-29)."""
    pipeline, _, harness, _ = paced_pipeline(
        conn, pclock, calendar, book, limit_table, cost_model, cap=12)
    await pipeline.on_signal_candidate(
        candidate(symbol="TCS", strategy_id="orb", signal_id="INWINDOW", score=0.9))

    ticker.at = datetime(2026, 6, 17, 10, 31, tzinfo=IST)       # seeded window is 10:00-10:30
    assert await pipeline.drain_forward_queue() is False
    assert harness.calls == []
    assert [p.candidate.signal_id for p in pipeline._pending_forwards] == ["INWINDOW"]
    assert forward_journal(conn)[("TCS", "orb")] == 0


async def test_immediate_mode_is_the_rollback_to_the_inline_drain(
    conn, pclock, calendar, book, limit_table, cost_model
):
    """forward_drain_mode='immediate' restores the pre-2026-08-14 behaviour byte for byte: the
    arriving candidate drains its own slot inline and the paced tick does nothing at all, so the
    rollback is a settings edit rather than a revert (and cannot double-dispatch)."""
    gov = TunableGovernor(2)
    harness = FakeHarness(dict(NO_ACTION_JSON), dict(NO_ACTION_JSON))
    pipeline, parts = make_pipeline(
        conn=conn, clock=pclock, calendar=calendar, book=book, harness=harness,
        gate=StubGate(verdict_of("approve", cost_model)), ctx=passing_ctx(),
        limits=StubLimits(limit_table), governor=gov, forward_drain_mode="immediate",
    )
    await pipeline.on_signal_candidate(
        candidate(symbol="TCS", strategy_id="orb", signal_id="INLINE", score=0.9))
    assert len(harness.calls) == 1                              # evaluated by its OWN arrival
    assert parts["assembler"].contexts[-1].stable_block == "stable INLINE"
    assert pipeline._pending_forwards == []
    assert forward_journal(conn)[("TCS", "orb")] == 1

    assert await pipeline.drain_forward_queue() is False        # the tick is inert in this mode
    assert len(harness.calls) == 1


async def test_paced_drain_survives_a_mid_day_restart(
    conn, pclock, calendar, book, limit_table, ticker, cost_model
):
    """The two halves of the day's forward state under pacing: the journalled COUNTER resumes across
    a restart (WO-1 (iv)) while the in-memory pending QUEUE does not â€” a restart drops the pointers,
    it does not refund the quota. Both are load-bearing: refilling the counter would double the
    day's analyst spend, and resurrecting the queue would forward levels from before the outage."""
    pipeline, _, harness, gov = paced_pipeline(
        conn, pclock, calendar, book, limit_table, cost_model, cap=2)
    await pipeline.on_signal_candidate(
        candidate(symbol="TCS", strategy_id="orb", signal_id="BEFORE_1", score=0.9))
    await pipeline.on_signal_candidate(
        candidate(symbol="INFY", strategy_id="orb", signal_id="BEFORE_2", score=0.8))
    assert await pipeline.drain_forward_queue() is True
    assert len(harness.calls) == 1
    assert len(pipeline._pending_forwards) == 1                 # BEFORE_2 still waiting

    # --- the restart: a brand-new pipeline object on the same state DB ---
    harness2 = FakeHarness(dict(NO_ACTION_JSON))
    restarted, parts2 = make_pipeline(
        conn=conn, clock=pclock, calendar=calendar, book=book, harness=harness2,
        gate=StubGate(verdict_of("approve", cost_model)), ctx=passing_ctx(),
        limits=StubLimits(limit_table), governor=gov,
    )
    assert await restarted.drain_forward_queue() is False       # nothing pending: the queue is gone
    ticker.at = NOW + timedelta(minutes=FORWARD_PACING_MIN)
    await restarted.on_signal_candidate(
        candidate(symbol="SBIN", strategy_id="orb", signal_id="AFTER_1", score=0.7))
    await restarted.on_signal_candidate(
        candidate(symbol="ITC", strategy_id="orb", signal_id="AFTER_2", score=0.6))
    assert await restarted.drain_forward_queue() is True
    assert restarted._forwarded_count == 2                      # 1 hydrated + 1, not 1
    assert len(harness2.calls) == 1

    ticker.at = NOW + timedelta(minutes=2 * FORWARD_PACING_MIN)
    assert await restarted.drain_forward_queue() is False        # the 2-call day cap held across it
    assert sum(forward_journal(conn).values()) == 2


# ================================= WO-20d (2026-08-20): the drain never silently loses a candidate
class FlakyHarness(FakeHarness):
    """A harness that blows up mid-call, exactly where the 2026-08-18 KALYANKJIL incident did.

    An ``asyncio.CancelledError`` raised inside the analyst-call transport propagated out of
    ``_evaluate_forward``, through the drain, and killed the APScheduler tick. The candidate had
    already been popped from the queue and charged a forward in the day-slot journal, and the call
    never reached the point where an ``agent_calls`` row is written -- so the day's record said
    ``forwarded=1`` and every other measure said nothing at all. Invisible, not merely failed.

    Raises ``error`` on the first ``failures`` calls, then behaves like :class:`FakeHarness`. A
    failing attempt is counted in ``attempts`` but never lands in ``calls``, so "was this candidate
    actually EVALUATED" stays a clean assertion.
    """

    def __init__(self, error: type[BaseException], failures: int, *results: Any) -> None:
        super().__init__(*results)
        self.error = error
        self.failures = failures
        self.attempts = 0

    async def run_single_shot(self, agent_def, context, validate, *, json_schema=None, call_class=None):
        self.attempts += 1
        if self.attempts <= self.failures:
            raise self.error("analyst transport died mid-call")
        return await super().run_single_shot(
            agent_def, context, validate, json_schema=json_schema, call_class=call_class
        )


def flaky_pipeline(
    conn, pclock, calendar, book, limit_table, cost_model, *, error, failures, cap=12, results=2
):
    """A ranked+paced pipeline whose analyst call fails ``failures`` times before it works."""
    gov = TunableGovernor(cap)
    harness = FlakyHarness(error, failures, *[dict(NO_ACTION_JSON) for _ in range(results)])
    pipeline, parts = make_pipeline(
        conn=conn, clock=pclock, calendar=calendar, book=book, harness=harness,
        gate=StubGate(verdict_of("approve", cost_model)), ctx=passing_ctx(),
        limits=StubLimits(limit_table), governor=gov,
    )
    return pipeline, parts, harness


def log_events(caplog, event: str) -> list[Any]:
    return [r for r in caplog.records if r.getMessage() == event]


async def test_a_blown_up_evaluation_is_requeued_at_the_FRONT_and_retried_next_tick(
    conn, pclock, calendar, book, limit_table, ticker, cost_model, caplog
):
    """First failure: the tick survives, the candidate goes back at the FRONT, the next tick retries.

    "Front" is the load-bearing word and this fixture proves it rather than assuming it. BOOM is
    re-queued first; a HIGHER-scoring rival then arrives and would win the ranked selection outright
    (its per-strategy quantile band is 4 against BOOM's 2). The re-queued candidate still goes first,
    because it already won a slot once and is owed the answer that slot bought.
    """
    pipeline, parts, harness = flaky_pipeline(
        conn, pclock, calendar, book, limit_table, cost_model, error=RuntimeError, failures=1)
    await pipeline.on_signal_candidate(
        candidate(symbol="TCS", strategy_id="orb", signal_id="BOOM", score=0.9))

    with caplog.at_level(logging.WARNING, logger="engine.ops.pipeline"):
        assert await pipeline.drain_forward_queue() is True      # the tick does NOT die
    assert harness.attempts == 1
    assert harness.calls == []                                   # never evaluated: it blew up
    assert [p.candidate.signal_id for p in pipeline._pending_forwards] == ["BOOM"]
    assert pipeline._pending_forwards[0].front is True

    requeued = log_events(caplog, "forward_evaluation_requeued")
    assert len(requeued) == 1
    assert (requeued[0].signal_id, requeued[0].symbol, requeued[0].strategy_id) == (
        "BOOM", "TCS", "orb")
    assert requeued[0].error_class == "RuntimeError"

    # A strictly better candidate lands in the meantime and still does not jump the retry.
    await pipeline.on_signal_candidate(
        candidate(symbol="INFY", strategy_id="orb", signal_id="RIVAL", score=0.99))
    ticker.at = NOW + timedelta(minutes=FORWARD_PACING_MIN)
    assert await pipeline.drain_forward_queue() is True

    assert len(harness.calls) == 1
    assert parts["assembler"].contexts[-1].stable_block == "stable BOOM"
    assert [p.candidate.signal_id for p in pipeline._pending_forwards] == ["RIVAL"]
    # Journalling semantics are UNCHANGED: forwarded counts ATTEMPTS against the Â§5.2(a) cap
    # (2026-07-29), so the retry is a second real charge -- not a refund, not a silent freebie.
    assert forward_journal(conn)[("TCS", "orb")] == 2
    assert pipeline._forwarded_count == 2


async def test_a_second_failure_is_lost_loudly_and_never_re_queued_again(
    conn, pclock, calendar, book, limit_table, ticker, cost_model, caplog
):
    """One retry, not a loop. A candidate failing twice is failing for a reason a third attempt will
    not fix, and a self-refilling queue would spend the whole day's analyst cap on one broken
    symbol. The drain survives both times; the loss is an ERROR line naming the candidate."""
    pipeline, _, harness = flaky_pipeline(
        conn, pclock, calendar, book, limit_table, cost_model, error=RuntimeError, failures=2)
    await pipeline.on_signal_candidate(
        candidate(symbol="TCS", strategy_id="orb", signal_id="BOOM", score=0.9))

    with caplog.at_level(logging.WARNING, logger="engine.ops.pipeline"):
        assert await pipeline.drain_forward_queue() is True
        ticker.at = NOW + timedelta(minutes=FORWARD_PACING_MIN)
        assert await pipeline.drain_forward_queue() is True       # survives the SECOND blow-up too

    assert harness.attempts == 2
    assert harness.calls == []                                    # never evaluated
    assert pipeline._pending_forwards == []                       # not re-queued a second time
    lost = log_events(caplog, "forward_evaluation_lost")
    assert len(lost) == 1
    assert (lost[0].signal_id, lost[0].symbol, lost[0].strategy_id) == ("BOOM", "TCS", "orb")
    assert lost[0].error_class == "RuntimeError"
    assert lost[0].levelname == "ERROR"                           # a lost candidate is not a warning
    assert len(log_events(caplog, "forward_evaluation_requeued")) == 1

    ticker.at = NOW + timedelta(minutes=2 * FORWARD_PACING_MIN)
    assert await pipeline.drain_forward_queue() is False           # the queue really is empty


async def test_cancellation_requeues_the_candidate_and_still_propagates(
    conn, pclock, calendar, book, limit_table, ticker, cost_model, caplog
):
    """THE INCIDENT ITSELF, in both halves.

    A ``CancelledError`` is how the engine is asked to STOP: swallowing it to protect the tick would
    make shutdown hang, so it is re-raised in every branch. But the candidate is still saved first --
    which is precisely what did not happen on 2026-08-18, when the forwarded candidate left no
    ``agent_calls`` row and simply ceased to exist.
    """
    pipeline, _, harness = flaky_pipeline(
        conn, pclock, calendar, book, limit_table, cost_model,
        error=asyncio.CancelledError, failures=2)
    await pipeline.on_signal_candidate(
        candidate(symbol="TCS", strategy_id="orb", signal_id="BOOM", score=0.9))

    with caplog.at_level(logging.WARNING, logger="engine.ops.pipeline"):
        with pytest.raises(asyncio.CancelledError):
            await pipeline.drain_forward_queue()

        assert [p.candidate.signal_id for p in pipeline._pending_forwards] == ["BOOM"]
        assert pipeline._pending_forwards[0].front is True
        requeued = log_events(caplog, "forward_evaluation_requeued")
        assert len(requeued) == 1 and requeued[0].error_class == "CancelledError"

        # Second cancellation: no re-queue, and the cancellation STILL propagates.
        ticker.at = NOW + timedelta(minutes=FORWARD_PACING_MIN)
        with pytest.raises(asyncio.CancelledError):
            await pipeline.drain_forward_queue()

    assert pipeline._pending_forwards == []
    assert harness.calls == []
    lost = log_events(caplog, "forward_evaluation_lost")
    assert len(lost) == 1 and lost[0].error_class == "CancelledError"


async def test_the_retry_budget_is_per_day_like_the_rest_of_the_forward_state(
    conn, pclock, calendar, book, limit_table, ticker, cost_model, caplog
):
    """The re-queued set rolls with ``_roll_forward_day``: a candidate that burned its one retry
    yesterday is not still carrying that debt today, and the set can never outlive the queue it
    points into."""
    pipeline, _, _ = flaky_pipeline(
        conn, pclock, calendar, book, limit_table, cost_model, error=RuntimeError, failures=1)
    await pipeline.on_signal_candidate(
        candidate(symbol="TCS", strategy_id="orb", signal_id="BOOM", score=0.9))
    with caplog.at_level(logging.WARNING, logger="engine.ops.pipeline"):
        assert await pipeline.drain_forward_queue() is True
    assert pipeline._requeued_forwards == {"BOOM"}

    pipeline._roll_forward_day(TODAY + timedelta(days=1))
    assert pipeline._requeued_forwards == set()
    assert pipeline._pending_forwards == []


# ======================================================= WO-8: hot-path read hygiene (Â§3.2 inv. 7)
class CountingStore(FakeStore):
    """A store double that HONORS the ``[start, end)`` bar window and COUNTS every read.

    ``FakeStore`` returns every bar it holds regardless of the window, which is fine for the
    behavioural tests above but useless here: WO-8 is entirely about *how many* store reads the
    trigger path performs and about the exact bar set a seed read returns.
    """

    def __init__(self, bars: list[Bar] | None = None, sectors: list[dict[str, str]] | None = None):
        super().__init__(bars, sectors)
        self.bar_reads = 0
        self.sector_reads = 0

    def get_bars_1m(self, symbol, start, end) -> list[Bar]:
        self.bar_reads += 1
        return [b for b in self.bars if b.symbol == symbol and start <= b.ts_minute < end]

    def get_sector_map(self, as_of=None) -> list[dict[str, str]]:
        self.sector_reads += 1
        return self.sectors


def _walk_bars(n: int, *, first_ts: datetime, symbol: str = SYMBOL, seed: int = 7) -> list[Bar]:
    """``n`` contiguous 1m bars on a deterministic random walk.

    NOT flat bars: a constant range makes every ATR recursion return the same number, so an
    equivalence test over flat bars passes even against a broken recursion. Every bar here has a
    different true range, and gaps/closes wander, so the Wilder state genuinely has to be carried.
    """
    rng = random.Random(seed)
    bars: list[Bar] = []
    px = Decimal("100")
    for i in range(n):
        close = px + Decimal(str(round(rng.uniform(-0.8, 0.8), 2)))
        bars.append(Bar(
            symbol=symbol, ts_minute=first_ts + timedelta(minutes=i), open=px,
            high=max(px, close) + Decimal(str(round(rng.uniform(0.0, 0.5), 2))),
            low=min(px, close) - Decimal(str(round(rng.uniform(0.0, 0.5), 2))),
            close=close, volume=1000 + i,
        ))
        px = close
    return bars


def _store_atr(bars: list[Bar]) -> Decimal:
    """ATR(14,1m) THE STORE-READ WAY: the shared Â§6.1 primitive over a bar sequence, converted
    exactly as ``_atr_1m`` converts it. This is the reference the incremental path must equal."""
    series = wilder_atr(
        [b.high for b in bars], [b.low for b in bars], [b.close for b in bars], ATR_PERIOD
    )
    return Decimal(str(float(series.iloc[-1])))


async def test_incremental_atr_equals_the_store_read_at_every_bar(
    conn, ticker, pclock, calendar, book, limit_table, cost_model
):
    """WO-8 (i) acceptance: EXACT equivalence across all three phases of the ATR seam.

    * **seed** â€” the first bar for a symbol reads the store once and lands on the store value;
    * **steady state** â€” every subsequent contiguous bar is served by the Wilder recursion alone,
      and equals the store-derived ATR over the same anchored window to the last bit;
    * **gap** â€” a bar whose minute is not contiguous with the last seen minute RESEEDS from the
      store rather than carrying a stale ATR forward, and the reseeded value differs from what
      carrying forward would have produced (otherwise the reseed would be untested decoration).
    """
    history = _walk_bars(30, first_ts=NOW - timedelta(minutes=30))       # 09:35..10:04
    store = CountingStore(bars=list(history))
    pipeline, _ = make_pipeline(
        conn=conn, clock=pclock, calendar=calendar, book=book, harness=FakeHarness(),
        gate=real_gate(limit_table, cost_model, pclock), ctx=passing_ctx(),
        limits=StubLimits(limit_table), store=store,
    )
    live = _walk_bars(12, first_ts=NOW, seed=11)                          # 10:05..10:16

    # --- seed + steady state: ten contiguous bars, ONE store read -------------------------------
    anchored = list(history)
    for bar in live[:10]:
        ticker.at = bar.ts_minute + timedelta(seconds=1)
        anchored.append(bar)
        assert pipeline._atr_1m(bar) == _store_atr(anchored)              # exact, not approximate
        store.bars.append(bar)                       # the bar writer persists it after the bus
    assert store.bar_reads == 1                      # ten bars, one read: the WO-8 claim

    # --- the gap: live[10] reaches the store but never reaches this pipeline --------------------
    store.bars.append(live[10])
    carried = pipeline._atr_1m(live[9])              # replay of a seen minute: no read, no change
    assert store.bar_reads == 1

    gap_bar = live[11]
    ticker.at = gap_bar.ts_minute + timedelta(seconds=1)
    reseeded = pipeline._atr_1m(gap_bar)
    assert store.bar_reads == 2                                          # the reseed happened
    assert reseeded == _store_atr([*store.bars, gap_bar])                 # ... and it is the store's
    stale = (float(carried) * (ATR_PERIOD - 1) + float(
        max(gap_bar.high - gap_bar.low, abs(gap_bar.high - live[9].close),
            abs(gap_bar.low - live[9].close))
    )) / ATR_PERIOD
    assert reseeded != Decimal(str(stale))           # carrying forward would have been WRONG

    # --- a duplicate bar is already folded in: no read, no double count -------------------------
    assert pipeline._atr_1m(gap_bar) == reseeded
    assert store.bar_reads == 2


async def test_seed_does_not_double_count_a_bar_the_store_already_holds(
    conn, ticker, pclock, calendar, book, limit_table, cost_model
):
    """The PRODUCTION ordering: ``BarBuilder._write_and_publish`` persists the batch BEFORE it
    publishes on ``bar.1m``, so the very first bar a symbol is seeded on is already in the store.
    Its true range must be folded in exactly once - the seed already contains it, so the recursion
    must NOT advance on top of the seed."""
    history = _walk_bars(21, first_ts=NOW - timedelta(minutes=20))    # ..NOW: includes the live bar
    store = CountingStore(bars=list(history))
    pipeline, _ = make_pipeline(
        conn=conn, clock=pclock, calendar=calendar, book=book, harness=FakeHarness(),
        gate=real_gate(limit_table, cost_model, pclock), ctx=passing_ctx(),
        limits=StubLimits(limit_table), store=store,
    )
    assert pipeline._atr_1m(history[-1]) == _store_atr(history)       # once, not twice
    assert pipeline._atr_1m(history[-1]) == _store_atr(history)       # replay: still once
    assert store.bar_reads == 1


async def test_on_bar_stops_reading_the_store_per_bar(
    conn, ticker, pclock, calendar, book, limit_table, cost_model
):
    """The F9 defect through the PUBLIC path: ``on_bar`` used to issue one ``get_bars_1m`` per bar
    for every symbol carrying an open recommended position. Twelve bars must now cost one read."""
    _open_position(conn, pclock, stop="90")                              # far from stop: no analyst
    history = _walk_bars(20, first_ts=NOW - timedelta(minutes=20))
    store = CountingStore(bars=list(history))
    pipeline, _ = make_pipeline(
        conn=conn, clock=pclock, calendar=calendar, book=book, harness=FakeHarness(),
        gate=real_gate(limit_table, cost_model, pclock), ctx=passing_ctx(),
        limits=StubLimits(limit_table), store=store,
    )
    for bar in _walk_bars(12, first_ts=NOW, seed=3):
        ticker.at = bar.ts_minute + timedelta(seconds=1)
        await pipeline.on_bar(bar)
        store.bars.append(bar)
    assert store.bar_reads == 1
    stats = pipeline.hot_path_stats()
    assert stats["atr_store_reads"] == 1 and stats["atr_incremental"] == 11


async def test_sector_map_is_read_once_per_trading_date(
    conn, ticker, pclock, calendar, book, limit_table, cost_model
):
    """WO-8 (ii) acceptance: the sector map is a WEEKLY snapshot read per candidate. Four
    candidates in one session == one read; the next trading date invalidates the cache == two."""
    harness = FakeHarness(*[dict(NO_ACTION_JSON) for _ in range(5)])
    store = CountingStore()
    pipeline, _ = make_pipeline(
        conn=conn, clock=pclock, calendar=calendar, book=book, harness=harness,
        gate=StubGate(verdict_of("approve", cost_model)), ctx=passing_ctx(),
        limits=StubLimits(limit_table), store=store,
    )
    for i, symbol in enumerate(("TCS", "INFY", "WIPRO", "SBIN")):
        await publish_candidate(pipeline, candidate(symbol=symbol, signal_id=f"S{i}"))
    assert len(harness.calls) == 4
    assert store.sector_reads == 1

    ticker.at = NOW + timedelta(days=1)                  # Thu 2026-06-18, a trading day
    await publish_candidate(pipeline, candidate(symbol="ITC", signal_id="S9"))
    assert len(harness.calls) == 5
    assert store.sector_reads == 2                       # the date change invalidated it


async def test_hot_path_stats_count_reads_avoided_against_reads_performed(
    conn, ticker, pclock, calendar, book, limit_table, cost_model
):
    """WO-8 (iii): the one-line session log is what makes the before/after quantifiable, so the
    counters behind it are asserted rather than trusted."""
    _open_position(conn, pclock, stop="90")
    store = CountingStore(bars=_walk_bars(20, first_ts=NOW - timedelta(minutes=20)))
    harness = FakeHarness(dict(NO_ACTION_JSON))
    pipeline, _ = make_pipeline(
        conn=conn, clock=pclock, calendar=calendar, book=book, harness=harness,
        gate=StubGate(verdict_of("approve", cost_model)), ctx=passing_ctx(),
        limits=StubLimits(limit_table), store=store,
    )
    await publish_candidate(pipeline, candidate(symbol="TCS", signal_id="SX"))
    for bar in _walk_bars(4, first_ts=NOW, seed=5):
        ticker.at = bar.ts_minute + timedelta(seconds=1)
        await pipeline.on_bar(bar)
        store.bars.append(bar)

    stats = pipeline.hot_path_stats()
    assert stats["d"] == TODAY.isoformat()
    assert (stats["atr_store_reads"], stats["atr_incremental"], stats["atr_gap_reseeds"]) == (1, 3, 0)
    assert (stats["sector_store_reads"], stats["sector_cache_hits"]) == (1, 0)
    assert stats["store_reads_avoided"] == 3 and stats["store_reads_performed"] == 2
    assert stats["atr_store_read_ms"] >= 0.0 and stats["atr_incremental_ms"] >= 0.0


# ================================ 2026-08-21: the WO-9 RAW counters survive a restart (funnel_raw_counts)
#
# THE BUG. `raw` — what the scanners PRODUCED, before dedupe and the caps — was the one funnel number
# with no DB home, so every engine restart zeroed it and the 22:35 `funnel_utilization` line reported
# `raw=None` ("unmeasured") for the whole day. That happened on 4 of the last 6 trade days. The 60 s
# drain tick now flushes the counters; the pre-screen loads them back on its first day roll.

def funnel_rows(conn, d: date = TODAY) -> dict[str, int]:
    rows = conn.execute(
        "SELECT strategy_id, fires FROM funnel_raw_counts WHERE d = ? ORDER BY strategy_id",
        (d.isoformat(),),
    ).fetchall()
    return {r["strategy_id"]: r["fires"] for r in rows}


def restartable_prescreen(conn) -> SignalPreScreen:
    """A pre-screen wired to the DB exactly as the composition root wires it — no scanners needed,
    since `admit` (the brk20/cat batch leg) runs the same raw-counting spine as the bar path."""
    return SignalPreScreen([], lambda bar: None,
                           raw_counts_loader=lambda d: read_funnel_raw_counts(conn, d))


async def test_the_drain_tick_flushes_the_raw_funnel_counters(
    conn, pclock, calendar, book, limit_table, cost_model
):
    """One flush per tick, ABSOLUTE values, and only when the counts CHANGED — the counters move at
    most once per bar, so an unchanged afternoon must cost a dict comparison and no write at all."""
    counts: dict[str, int] = {"orb": 3}
    pipeline, _ = make_pipeline(
        conn=conn, clock=pclock, calendar=calendar, book=book, harness=FakeHarness(),
        gate=StubGate(verdict_of("approve", cost_model)), ctx=passing_ctx(),
        limits=StubLimits(limit_table), funnel_raw=lambda d: dict(counts),
    )
    await pipeline.drain_forward_queue()
    assert funnel_rows(conn) == {"orb": 3}

    counts.update({"orb": 9, "rsi2": 2})                 # a morning's worth of further fires
    await pipeline.drain_forward_queue()
    assert funnel_rows(conn) == {"orb": 9, "rsi2": 2}    # absolute, not 3+9

    # Unchanged counters => the tick writes NOTHING. Proven by moving the row out from under it:
    # a write would put the flushed value back.
    conn.execute("UPDATE funnel_raw_counts SET fires = 999 WHERE strategy_id = 'orb'")
    await pipeline.drain_forward_queue()
    assert funnel_rows(conn) == {"orb": 999, "rsi2": 2}


async def test_a_restart_continues_the_raw_count_instead_of_resetting_it(
    conn, pclock, calendar, book, limit_table, cost_model
):
    """THE REGRESSION, in the shape it actually happened: new pre-screen + new pipeline over the same
    DB (a mid-session restart) must CONTINUE the day's count, not start it over."""
    day = pclock.today()
    ps1 = restartable_prescreen(conn)
    p1, _ = make_pipeline(
        conn=conn, clock=pclock, calendar=calendar, book=book, harness=FakeHarness(),
        gate=StubGate(verdict_of("approve", cost_model)), ctx=passing_ctx(),
        limits=StubLimits(limit_table), funnel_raw=ps1.raw_counts,
    )
    ps1.admit([candidate(symbol="TCS", signal_id="A", score=0.9, catalyst_ref=None),
               candidate(symbol="INFY", signal_id="B", score=0.8, catalyst_ref=None)], day)
    await p1.drain_forward_queue()
    assert ps1.raw_counts(day) == {"orb": 2}
    assert funnel_rows(conn) == {"orb": 2}

    ps2 = restartable_prescreen(conn)                    # --- restart: nothing in memory survives ---
    p2, _ = make_pipeline(
        conn=conn, clock=pclock, calendar=calendar, book=book, harness=FakeHarness(),
        gate=StubGate(verdict_of("approve", cost_model)), ctx=passing_ctx(),
        limits=StubLimits(limit_table), funnel_raw=ps2.raw_counts,
    )
    ps2.admit([candidate(symbol="SBIN", signal_id="C", score=0.7, catalyst_ref=None)], day)
    assert ps2.raw_counts(day) == {"orb": 3}             # 2 hydrated + 1 new, NOT 1
    await p2.drain_forward_queue()
    assert funnel_rows(conn) == {"orb": 3}


async def test_a_failed_raw_funnel_flush_warns_and_never_reaches_the_drain(
    conn, pclock, calendar, book, limit_table, cost_model, caplog
):
    """D7: telemetry must not be able to break trading. A dead connection costs the raw row for this
    tick and nothing else — and leaves the flushed-state marker untouched, so the next healthy tick
    still writes rather than believing it already had."""
    pipeline, _ = make_pipeline(
        conn=conn, clock=pclock, calendar=calendar, book=book, harness=FakeHarness(),
        gate=StubGate(verdict_of("approve", cost_model)), ctx=passing_ctx(),
        limits=StubLimits(limit_table), funnel_raw=lambda d: {"orb": 3},
    )
    conn.close()
    with caplog.at_level(logging.WARNING, logger="engine.ops.pipeline"):
        assert await pipeline.drain_forward_queue() is False      # no raise, no tick lost

    assert [r for r in caplog.records if r.getMessage() == "funnel_raw_flush_failed"]
    assert pipeline._funnel_raw_flushed == {} and pipeline._funnel_raw_day is None
