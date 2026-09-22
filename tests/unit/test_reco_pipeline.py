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
from types import SimpleNamespace
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
from engine.ops import pipeline as pipeline_module
from engine.ops.nightly_review import read_funnel_raw_counts
from engine.ops.pipeline import (
    ATR_PERIOD,
    FORWARD_PACING_MIN,
    POSITION_EVENT_DEBOUNCE_MIN,
    QUANTILE_BANDS,
    TTL_INTRADAY_MIN,
    RecommendationBook,
    RecommendationPipeline,
)
from engine.risk.exposure import ExposureTracker
from engine.risk.gate import GateContext, RiskGate, _exiting_symbols
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
    claim_slot=None, take_displaced=None, decline=None, ltp_fn=None, warmup_status_fn=None,
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
        funnel_raw=funnel_raw, claim_slot=claim_slot, take_displaced=take_displaced,
        decline=decline, ltp_fn=ltp_fn, warmup_status_fn=warmup_status_fn,
        admission_mode=admission_mode, forward_drain_mode=forward_drain_mode,
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


async def test_missing_limit_entry_price_defaults_from_the_candidates_level(
    conn, pclock, calendar, book, limit_table, cost_model, caplog
):
    """2026-09-21 evidence: 7 of 13 sonnet-5 LIMIT proposals since 09-16 arrived with entry_price
    null, and the gate rejected every one on a band check that could not see a price even though the
    scanner's own (pre-screened) level was sitting right there in ``candidate.raw_levels.entry``. The
    pipeline now fills the gap instead of dropping the proposal. Sibling of
    ``test_happy_path_writes_the_full_provenance_chain`` with ``entry_price`` stripped from the
    analyst payload.
    """
    harness = FakeHarness(dict(ENTER_JSON, entry_price=None))
    pipeline, parts = make_pipeline(
        conn=conn, clock=pclock, calendar=calendar, book=book, harness=harness,
        gate=real_gate(limit_table, cost_model, pclock), ctx=passing_ctx(),
        limits=StubLimits(limit_table),
    )
    with caplog.at_level(logging.INFO, logger="engine.ops.pipeline"):
        await publish_candidate(pipeline, candidate())

    proposal = conn.execute("SELECT * FROM proposals").fetchone()
    verdict = conn.execute("SELECT * FROM verdicts").fetchone()
    assert proposal is not None, "the proposal must still be persisted, not dropped"

    payload = json.loads(proposal["payload"])
    # Same string form a STATED price gets: DecimalStr serialises via str(Decimal(...)), and the
    # happy-path sibling's stated "100" normalises to the identical string (compare the two forms).
    assert payload["entry_price"] == str(Decimal(ENTER_JSON["entry_price"])) == "100"

    verdict_payload = json.loads(verdict["payload"])
    band = next(c for c in verdict_payload["checks"] if c["rule_id"] == "entry_sanity_band")
    assert band["passed"] is True, band   # not failed on entry_sanity_band once the price is filled

    assert len(log_events(caplog, "enter_limit_price_defaulted")) == 1


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


async def test_intraday_warmup_shortfall_rearms_without_spending_an_analyst_call(
    conn, ticker, pclock, calendar, book, limit_table, cost_model
):
    """2026-09-13 per-class warm-up (§2.6 step-6 addendum). The risk state no longer freezes on an
    intraday-only coverage hole, so the refusal is per candidate — and it happens BEFORE the analyst
    call, because the gate would reject the proposal on ``warmup_ready`` after the call was spent.
    The day slot goes back (never evaluated; a minute hole heals mid-session), and the daily-class
    candidate riding the SAME snapshot is forwarded: the whole point of the change.

    The hole is attributed to the candidate's OWN symbol, because since 2026-09-17 that is what the
    screen asks (see the sibling test for another symbol's hole)."""
    from engine.ops.warmup import WarmupStatus

    intraday_hole = WarmupStatus(ready=False, blockers=[f"orb:{SYMBOL} bars 113/114"])
    rearmed: list[tuple[str, str]] = []

    harness = FakeHarness()          # asserts if called: the analyst must not be reached at all
    pipeline, _ = make_pipeline(
        conn=conn, clock=pclock, calendar=calendar, book=book, harness=harness,
        gate=real_gate(limit_table, cost_model, pclock), ctx=passing_ctx(),
        limits=StubLimits(limit_table), warmup_status_fn=lambda: intraday_hole,
        rearm=lambda sym, sid: rearmed.append((sym, sid)) or True,
    )
    cand = candidate()
    await publish_candidate(pipeline, cand)
    assert harness.calls == []
    assert rearmed == [(cand.symbol, cand.strategy_id)]

    # Same snapshot, same tick: the swing leg reads completed daily bars and is NOT this screen's
    # business, so it reaches the analyst.
    rearmed.clear()
    swing_harness = FakeHarness(dict(NO_ACTION_JSON))
    swing_pipeline, _ = make_pipeline(
        conn=conn, clock=pclock, calendar=calendar, book=book, harness=swing_harness,
        gate=real_gate(limit_table, cost_model, pclock), ctx=passing_ctx(),
        limits=StubLimits(limit_table), warmup_status_fn=lambda: intraday_hole,
        rearm=lambda sym, sid: rearmed.append((sym, sid)) or True,
    )
    await publish_candidate(swing_pipeline, candidate(style="swing", strategy_id="brk20"))
    assert len(swing_harness.calls) == 1
    assert rearmed == []


async def test_another_symbols_intraday_hole_does_not_rearm_this_candidate(
    conn, ticker, pclock, calendar, book, limit_table, cost_model
):
    """2026-09-17 per-SYMBOL readiness. ``ready_for`` was CLASS-WIDE, so ONE symbol's hole refused
    every intraday candidate in the book: on 2026-09-16 PTCIL printed no trade at 13:36 (Kite
    publishes no candle for a tradeless minute — the bar is unfillable, not late) and
    ``orb:PTCIL bars 374/375`` refused everything from 13:37 to the close. The screen now asks about
    the candidate's own symbol, so this candidate reaches the analyst."""
    from engine.ops.warmup import WarmupStatus

    other_hole = WarmupStatus(ready=False, blockers=["orb:PTCIL bars 374/375"])
    rearmed: list[tuple[str, str]] = []

    harness = FakeHarness(dict(NO_ACTION_JSON))
    pipeline, _ = make_pipeline(
        conn=conn, clock=pclock, calendar=calendar, book=book, harness=harness,
        gate=real_gate(limit_table, cost_model, pclock), ctx=passing_ctx(),
        limits=StubLimits(limit_table), warmup_status_fn=lambda: other_hole,
        rearm=lambda sym, sid: rearmed.append((sym, sid)) or True,
    )
    await publish_candidate(pipeline, candidate())
    assert len(harness.calls) == 1
    assert rearmed == []

    # …and a line in the same class that attributes to NO symbol still refuses everyone (R6).
    unattributable = WarmupStatus(ready=False, blockers=["orb:?? garbage"])
    rearmed.clear()
    closed = FakeHarness()          # asserts if called
    pipeline_closed, _ = make_pipeline(
        conn=conn, clock=pclock, calendar=calendar, book=book, harness=closed,
        gate=real_gate(limit_table, cost_model, pclock), ctx=passing_ctx(),
        limits=StubLimits(limit_table), warmup_status_fn=lambda: unattributable,
        rearm=lambda sym, sid: rearmed.append((sym, sid)) or True,
    )
    cand = candidate()
    await publish_candidate(pipeline_closed, cand)
    assert closed.calls == []
    assert rearmed == [(cand.symbol, cand.strategy_id)]


async def test_intraday_warmup_screen_is_silent_when_it_cannot_answer(
    conn, ticker, pclock, calendar, book, limit_table, cost_model
):
    """The screen is a cost optimisation, never the enforcement (the GATE fails closed on the same
    snapshot), so every case it cannot answer fails OPEN — one wasted analyst call at worst, never a
    lost candidate. Unwired seam, a status with no per-class answer, a raising seam, and a covered
    intraday class all let the candidate through."""
    from engine.ops.warmup import WarmupStatus

    def boom():
        raise RuntimeError("snapshot holder went away")

    for status_fn in (None,
                      lambda: SimpleNamespace(ready=False, blockers=["orb:RELIANCE bars 1/50"]),
                      boom,
                      lambda: WarmupStatus(ready=True)):
        harness = FakeHarness(dict(NO_ACTION_JSON))
        pipeline, _ = make_pipeline(
            conn=conn, clock=pclock, calendar=calendar, book=book, harness=harness,
            gate=real_gate(limit_table, cost_model, pclock), ctx=passing_ctx(),
            limits=StubLimits(limit_table), warmup_status_fn=status_fn,
        )
        await publish_candidate(pipeline, candidate())
        assert len(harness.calls) == 1, status_fn


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


def _ttl_pipeline(conn, pclock, calendar, book, limit_table, cost_model):
    """A pipeline built for _ttl arithmetic only — no harness call is made by any of these."""
    pipeline, _ = make_pipeline(
        conn=conn, clock=pclock, calendar=calendar, book=book, harness=FakeHarness(),
        gate=real_gate(limit_table, cost_model, pclock), ctx=passing_ctx(),
        limits=StubLimits(limit_table),
    )
    return pipeline


def _horizon_calendar(tmp_path: Path, pclock: Clock, conn) -> NSECalendar:
    """A calendar whose verified horizon the TEST owns: 2026 and nothing else.

    The fallback branch is reached by running off the END of the loaded calendars, and pointing that
    probe at the repo's ``config/calendar`` would make these tests fail the routine December morning
    someone adds ``2027.yaml``. Copying one year into ``tmp_path`` keeps the trigger condition
    permanent and local."""
    (tmp_path / "2026.yaml").write_text(
        (CALENDAR_DIR / "2026.yaml").read_text(encoding="utf-8"), encoding="utf-8"
    )
    return NSECalendar(tmp_path, pclock, sqlite_conn=conn)


async def test_ttl_intraday_is_now_plus_ttl_minutes(
    conn, pclock, ticker, calendar, book, limit_table, cost_model
):
    """Intraday is the only style _ttl treats specially: TTL_INTRADAY_MIN minutes from now, even five
    minutes before a Friday close where the swing/position branch would otherwise land on today's own
    15:30."""
    pipeline = _ttl_pipeline(conn, pclock, calendar, book, limit_table, cost_model)
    ticker.at = datetime(2026, 6, 19, 15, 25, tzinfo=IST)             # 5 minutes before Friday close
    assert pipeline._ttl("intraday") == ticker.at + timedelta(minutes=TTL_INTRADAY_MIN)


async def test_ttl_swing_and_position_are_todays_close(
    conn, pclock, ticker, calendar, book, limit_table, cost_model
):
    """_ttl is keyed on style alone: an entry, an exit, a stop adjust and the §5.2(a) forward-queue
    horizon all keep TODAY's close. WO-D2's one-exit-per-session cadence depends on it
    (``_delivered_exit_today`` screens today's deliveries only), and the forward queue is a
    within-day structure."""
    pipeline = _ttl_pipeline(conn, pclock, calendar, book, limit_table, cost_model)
    ticker.at = datetime(2026, 6, 19, 10, 5, tzinfo=IST)              # Friday, a trading day
    assert pipeline._ttl("swing") == datetime(2026, 6, 19, 15, 30, tzinfo=IST)
    assert pipeline._ttl("position") == datetime(2026, 6, 19, 15, 30, tzinfo=IST)


async def test_ttl_entry_expires_the_same_session_per_2026_09_23_owner_directive(
    conn, pclock, ticker, calendar, book, limit_table, cost_model
):
    """2026-09-23 owner directive: "trade recommendations should close automatically and free the
    gate count at the end of the day if no trade decision were made." This withdraws the 2026-09-13
    (WO-V) next-session entry TTL -- on 2026-09-21/22 four untaken pending entry recs held the CNC
    ``max_open_positions`` cap of 4 across two sessions, killing 12 of 19 gate verdicts. An entry's
    TTL is TODAY's session close, the same as every other swing/position stamp."""
    pipeline = _ttl_pipeline(conn, pclock, calendar, book, limit_table, cost_model)
    ticker.at = datetime(2026, 6, 19, 10, 20, tzinfo=IST)             # Friday, a trading day
    assert pipeline._ttl("swing") == datetime(2026, 6, 19, 15, 30, tzinfo=IST)
    assert pipeline._ttl("position") == datetime(2026, 6, 19, 15, 30, tzinfo=IST)


async def test_a_swing_exit_recommendation_is_still_stamped_to_todays_close(
    conn, ticker, pclock, calendar, book, limit_table, cost_model
):
    """The same scope line end to end, on the delivered payload rather than the seam. Two live exit
    recs naming two different stops for one position is what a shared two-session TTL would put in
    front of the owner, because the WO-D2 repeat screen is bounded by today's ``delivered_at``."""
    position_id = _open_position(conn, pclock, style="swing")
    ticker.at = datetime(2026, 6, 19, 11, 0, tzinfo=IST)              # Friday, a trading day
    pipeline, _ = _exit_pipeline(
        conn, pclock, calendar, book, limit_table, cost_model,
        FakeHarness(_exit_json(position_id)), position_id,
    )

    await pipeline.on_bar(_near_bar())

    payload = json.loads(conn.execute("SELECT payload FROM recommendations").fetchone()["payload"])
    assert payload["kind"] == "exit"
    assert payload["valid_until"] == datetime(2026, 6, 19, 15, 30, tzinfo=IST).isoformat()


async def test_ttl_never_mints_an_already_dead_stamp(
    conn, pclock, ticker, book, limit_table, cost_model, tmp_path
):
    """``on_bar`` is never window-gated, so a stop-proximity event in the 15:30-15:45 settlement
    buffer can reach _ttl after today's close has already passed. A ``valid_until`` at or before now
    is rejected by the gate's own envelope check (``_rule_proposal_stale``, "expired/unstamped
    proposal - fail closed"), so a dead stamp would silently swallow a risk-reducing exit the owner
    should have seen; _ttl falls back to the intraday TTL instead."""
    calendar = _horizon_calendar(tmp_path, pclock, conn)
    pipeline = _ttl_pipeline(conn, pclock, calendar, book, limit_table, cost_model)
    ticker.at = datetime(2026, 12, 31, 15, 40, tzinfo=IST)            # past the 15:30 close
    floor = ticker.at + timedelta(minutes=TTL_INTRADAY_MIN)
    assert pipeline._ttl("swing") == floor


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
    declined: list[tuple[str, str]] = []
    pipeline, parts = make_pipeline(
        conn=conn, clock=pclock, calendar=calendar, book=book, harness=harness,
        gate=real_gate(limit_table, cost_model, pclock), ctx=passing_ctx(),
        limits=StubLimits(limit_table),
        decline=lambda sym, sid: declined.append((sym, sid)) or True,
    )
    cand = candidate()
    await publish_candidate(pipeline, cand)

    assert conn.execute("SELECT COUNT(*) FROM proposals").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM recommendations").fetchone()[0] == 0
    assert parts["assembler"].regime_notes == [NO_ACTION_JSON["regime_note"]]
    assert parts["notify"].messages == []
    # 2026-09-04: a no_action verdict hands the slot back to displacement (the analyst RAN, so the
    # day slot itself stays spent — this is not the 2026-07-29 infrastructure re-arm).
    assert declined == [(cand.symbol, cand.strategy_id)]
    row = conn.execute(
        "SELECT evaluated FROM prescreen_day_slots WHERE symbol=? AND strategy_id=?",
        (cand.symbol, cand.strategy_id),
    ).fetchone()
    assert row["evaluated"] == 1


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


async def test_owner_approval_on_a_position_event_names_the_symbol(
    conn, ticker, pclock, calendar, book, limit_table, cost_model
):
    """An exit/modify action carries only a position_id, so the prompt used to be a bare ULID — an
    owner cannot approve what they cannot identify. The caller's symbol is persisted and printed."""
    position_id = _open_position(conn, pclock)
    exit_json = {
        "action": "exit", "position_id": position_id, "exit_type": "MARKET",
        "reason": "risk_event", "confidence": 0.9,
        "thesis": "Price is inside half an ATR of the stop; the breakout thesis is failing.",
    }
    harness = FakeHarness(dict(exit_json))
    ticker.at = datetime(2026, 6, 17, 14, 0, tzinfo=IST)
    pipeline, parts = make_pipeline(
        conn=conn, clock=pclock, calendar=calendar, book=book, harness=harness,
        gate=StubGate(verdict_of("owner_approval_required", cost_model,
                                 reasons=["exit sizing is above the auto-approval band"])),
        ctx=passing_ctx(positions_known=frozenset({position_id})),
        limits=StubLimits(limit_table), store=FakeStore(bars=_flat_bars()),
    )
    near = Bar(symbol=SYMBOL, ts_minute=NOW, open=Decimal("99.4"), high=Decimal("99.5"),
               low=Decimal("99.3"), close=Decimal("99.40"), volume=100)

    await pipeline.on_bar(near)

    row = conn.execute("SELECT * FROM owner_approvals").fetchone()
    assert row["status"] == "pending" and row["kind"] == "exit"
    payload = json.loads(row["payload"])
    assert payload["symbol"] == SYMBOL                       # the whole point
    assert payload["position_id"] == position_id             # provenance is kept, not replaced
    message = parts["notify"].messages[-1]
    assert SYMBOL in message.title
    assert SYMBOL in message.body and position_id in message.body
    assert conn.execute("SELECT COUNT(*) FROM recommendations").fetchone()[0] == 0


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


async def test_an_expired_recommendation_still_accepts_the_owners_taken(
    conn, ticker, pclock, book, cost_model
):
    """2026-08-26, the FIRST live /taken: the owner executed intraday and recorded in the evening —
    the 15:45 sweep had already labelled the row 'expired' and the book refused the platform's first
    real fill. Expiry marks 'no longer actionable', never 'never happened': expired → taken is
    legal; dismissed stays refused."""
    rec = make_rec(cost_model)
    book.deliver(rec, ledger_fields=dict(LEDGER_FIELDS))
    conn.execute(
        "UPDATE recommendations SET human_action='expired' WHERE rec_id=?", (rec.rec_id,)
    )

    summary = await book.take(rec.rec_id, 3, Decimal("101.50"))

    assert "recorded" in summary
    rec_row = conn.execute("SELECT * FROM recommendations").fetchone()
    assert rec_row["human_action"] == "taken" and rec_row["human_fill_price"] == "101.50"
    assert conn.execute("SELECT COUNT(1) FROM positions").fetchone()[0] == 1

    conn.execute(
        "UPDATE recommendations SET human_action='dismissed' WHERE rec_id=?", (rec.rec_id,)
    )
    with pytest.raises(ValueError, match="already 'dismissed'"):
        await book.take(rec.rec_id, 3, Decimal("101.50"))


async def test_a_gapped_fill_is_recorded_and_the_stop_risk_drift_is_called_out(
    conn, ticker, pclock, book, cost_model, caplog
):
    """The overnight-gap drift check, made visible where it lands — and judged on the gate's OWN
    basis (re-review 2026-09-13). ``take`` never gates on ``valid_until`` (a swing rec can be
    confirmed the morning after its TTL expired at yesterday's close), and nothing re-gates at
    capture: the position row is written with the RECOMMENDATION's stop. But the gate
    sized that swing on overnight_gap_mult x the stop distance (_rule_per_trade_risk), so a fill
    anywhere inside that allowance carries stop risk the per_trade_risk verdict ALREADY approved and
    is silent; the call-out fires only once the approved budget is genuinely exceeded. The fill is
    recorded regardless — the owner is the authority on what they executed and an untracked live
    position is the worse failure — and the drift is said in the reply and at WARNING in the log.
    A fill anywhere INSIDE the delivered zone is never called out; a fill on the WRONG SIDE of the
    stop is called out unconditionally. Zone 1000-1020, stop 940: planned 80/share; gap 2.5 ⇒ 200."""
    gated = RecommendationBook(conn, pclock, cost_model, overnight_gap_mult_fn=lambda: Decimal("2.5"))

    def swing_rec():
        return make_rec(
            cost_model, style="swing", product="CNC", entry_zone=(Decimal("1000"), Decimal("1020")),
            stop=Decimal("940"), targets=[Decimal("1100")], qty=13, notional=Decimal("13000"),
        )

    # 1080: 140/share = 1.75x planned — INSIDE the 2.5x overnight-gap allowance the verdict sized on.
    inside_gap = swing_rec()
    gated.deliver(inside_gap, ledger_fields=dict(LEDGER_FIELDS))
    with caplog.at_level(logging.WARNING, logger="engine.ops.pipeline"):
        assert "CHECK" not in await gated.take(inside_gap.rec_id, 13, Decimal("1080"))
    assert log_events(caplog, "recommendation_taken_off_zone") == []

    # 1150: 210/share > the 200 approved — 13 x 210 = ₹2,730 of stop risk against ₹2,600 approved.
    gapped = swing_rec()
    gated.deliver(gapped, ledger_fields=dict(LEDGER_FIELDS))
    with caplog.at_level(logging.WARNING, logger="engine.ops.pipeline"):
        summary = await gated.take(gapped.rec_id, 13, Decimal("1150"))
    assert "recorded" in summary and "CHECK THE SIZE" in summary
    assert "2730.00" in summary and "2600.00" in summary and "2.5x" in summary
    assert len(log_events(caplog, "recommendation_taken_off_zone")) == 1
    position = conn.execute("SELECT * FROM positions WHERE avg_entry='1150'").fetchone()
    assert position["stop"] == "940"   # copied verbatim

    # Wrong side: a BUY filled at or below its stop — the gap went THROUGH the stop.
    through = swing_rec()
    gated.deliver(through, ledger_fields=dict(LEDGER_FIELDS))
    summary = await gated.take(through.rec_id, 13, Decimal("930"))
    assert "recorded" in summary and "WRONG SIDE" in summary and "CHECK THE SIZE" not in summary

    # Inside the delivered zone: never called out.
    inside = swing_rec()
    gated.deliver(inside, ledger_fields=dict(LEDGER_FIELDS))
    assert "CHECK" not in await gated.take(inside.rec_id, 13, Decimal("1020"))

    # A book wired WITHOUT the limits seam keeps the 1.25x fallback: 1080 (1.75x) IS called out there,
    # against the plain zone-edge distance, with no gap multiple quoted.
    fallback = swing_rec()
    book.deliver(fallback, ledger_fields=dict(LEDGER_FIELDS))
    summary = await book.take(fallback.rec_id, 13, Decimal("1080"))
    assert "CHECK THE SIZE" in summary and "1820.00" in summary and "2.5x" not in summary


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
def _open_position(
    conn, clock, *, style="intraday", opened_at=None, stop="99", side="BUY", symbol=SYMBOL
) -> str:
    position_id = str(ULID())
    conn.execute(
        "INSERT INTO positions (position_id, symbol, side, style, product, qty, avg_entry, stop, "
        "state, origin, opened_at) VALUES (?, ?, ?, ?, ?, 10, '100', ?, 'OPEN', 'recommended', ?)",
        (position_id, symbol, side, style, "MIS" if style == "intraday" else "CNC", stop,
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

    # Past the debounce the hour-cadence no longer blocks the event. Since WO-D2 the SECOND gate on
    # this path is the repeat-exit screen, so the position also has to have changed for the analyst
    # to be worth calling — the owner tightened the stop on a long (99 -> 99.5) here. The unchanged
    # case (a second identical exit recommendation, suppressed) is
    # ``test_repeat_exit_is_skipped_until_the_stop_changes`` below.
    conn.execute("UPDATE positions SET stop='99.5' WHERE position_id=?", (position_id,))
    conn.commit()
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


# =========================================================================== WO-D2: exit hygiene
# 2026-09-11 forensics: two positions the owner sold outside the ledger on 08-26 stayed OPEN until
# 09-11 and produced 68 exit recommendations (6-10 a day), 182 position-event analyst calls and 11
# day plans that reasoned about their "overnight risk". The month's one genuine ins entry
# (JINDALSTEL, 09-08) arrived as notification 2 of 8 that day; the other seven were those repeats.
#
# _run_position_event is the single convergence point of every position-event ENTRY POINT -- today
# only on_bar (trigger b) reaches it -- so both screens are pinned through the public on_bar path.
EXIT_JSON: dict[str, Any] = {
    "action": "exit", "exit_type": "MARKET", "reason": "risk_event", "confidence": 0.9,
    "thesis": "Price is inside half an ATR of the stop; the breakout thesis is failing.",
}


def _exit_json(position_id: str) -> dict[str, Any]:
    return {**EXIT_JSON, "position_id": position_id}


def _observe_holding(conn, position_id: str, d: date, *, tracked: int = 10, held: int = 0) -> None:
    """One §3.6 ``holdings_observations`` row - what the hourly reconcile writes (migration 0013)."""
    conn.execute(
        "INSERT INTO holdings_observations (position_id, d, tracked_qty, held_qty, observed_at) "
        "VALUES (?, ?, ?, ?, ?)",
        (position_id, d.isoformat(), tracked, held, f"{d.isoformat()}T10:05:00+05:30"),
    )
    conn.commit()


def _near_bar() -> Bar:
    """0.5 x ATR(1.00) = 0.50; stop 99 => anything at or below 99.50 is "near"."""
    return Bar(symbol=SYMBOL, ts_minute=NOW, open=Decimal("99.4"), high=Decimal("99.5"),
               low=Decimal("99.3"), close=Decimal("99.40"), volume=100)


def _exit_pipeline(conn, pclock, calendar, book, limit_table, cost_model, harness, position_id):
    return make_pipeline(
        conn=conn, clock=pclock, calendar=calendar, book=book, harness=harness,
        gate=real_gate(limit_table, cost_model, pclock),
        ctx=passing_ctx(positions_known=frozenset({position_id})),
        limits=StubLimits(limit_table), store=FakeStore(bars=_flat_bars()),
    )


async def test_position_sold_outside_the_ledger_never_reaches_the_analyst(
    conn, ticker, pclock, calendar, book, limit_table, cost_model, caplog
):
    """Screen (a): two consecutive broker observations show the position unheld => no analyst call,
    no exit recommendation, and NO governor decision either (the screen runs before admission).

    The platform cannot recommend an exit from a position that does not exist. The owner is still
    told - once a day by the §3.6 reconcile alert, and every morning by the day plan's "sold outside
    the ledger" block - and ``/closed`` is what ends it."""
    position_id = _open_position(conn, pclock)
    _observe_holding(conn, position_id, TODAY - timedelta(days=1))
    _observe_holding(conn, position_id, TODAY)
    harness = FakeHarness()                      # ANY call raises: the screen must come first
    ticker.at = datetime(2026, 6, 17, 14, 0, tzinfo=IST)
    pipeline, parts = _exit_pipeline(
        conn, pclock, calendar, book, limit_table, cost_model, harness, position_id
    )

    with caplog.at_level(logging.WARNING, logger="engine.ops.pipeline"):
        await pipeline.on_bar(_near_bar())
        ticker.at = ticker.at + timedelta(minutes=POSITION_EVENT_DEBOUNCE_MIN + 1)
        await pipeline.on_bar(_near_bar())

    assert harness.calls == []
    assert parts["governor"].calls == []
    assert conn.execute("SELECT COUNT(*) FROM recommendations").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM proposals").fetchone()[0] == 0
    assert parts["notify"].messages == []
    # Logged ONCE per position per day: twelve hourly ticks must leave one line, not twelve.
    skipped = log_events(caplog, "position_event_skipped_sold_outside_ledger")
    assert len(skipped) == 1
    assert skipped[0].position_id == position_id and skipped[0].symbol == SYMBOL


async def test_one_short_observation_day_does_not_silence_the_exit_path(
    conn, ticker, pclock, calendar, book, limit_table, cost_model
):
    """The asymmetry that makes screen (a) safe: ONE short reading (a settlement edge, a truncated
    holdings payload) is not evidence of a sale, so the exit recommendation still fires."""
    position_id = _open_position(conn, pclock)
    _observe_holding(conn, position_id, TODAY)
    harness = FakeHarness(_exit_json(position_id))
    ticker.at = datetime(2026, 6, 17, 14, 0, tzinfo=IST)
    pipeline, _ = _exit_pipeline(
        conn, pclock, calendar, book, limit_table, cost_model, harness, position_id
    )

    await pipeline.on_bar(_near_bar())

    assert len(harness.calls) == 1
    assert conn.execute("SELECT COUNT(*) FROM recommendations").fetchone()[0] == 1


async def test_a_position_back_in_holdings_is_managed_again_the_same_day(
    conn, ticker, pclock, calendar, book, limit_table, cost_model
):
    """The reconcile REWRITES today's observation hourly (last write of the day wins), so the screen
    is re-decided on every event: the 13:00 pulse that finds the position held must un-silence it
    immediately, which a per-day cached missing set could not do."""
    position_id = _open_position(conn, pclock)
    _observe_holding(conn, position_id, TODAY - timedelta(days=1))
    _observe_holding(conn, position_id, TODAY)
    harness = FakeHarness(_exit_json(position_id))
    ticker.at = datetime(2026, 6, 17, 14, 0, tzinfo=IST)
    pipeline, _ = _exit_pipeline(
        conn, pclock, calendar, book, limit_table, cost_model, harness, position_id
    )

    await pipeline.on_bar(_near_bar())
    assert harness.calls == []

    conn.execute("UPDATE holdings_observations SET held_qty=10 WHERE position_id=? AND d=?",
                 (position_id, TODAY.isoformat()))
    conn.commit()
    ticker.at = ticker.at + timedelta(minutes=POSITION_EVENT_DEBOUNCE_MIN + 1)
    await pipeline.on_bar(_near_bar())

    assert len(harness.calls) == 1
    assert conn.execute("SELECT COUNT(*) FROM recommendations").fetchone()[0] == 1


async def test_a_partial_holding_is_short_but_still_gets_its_exit_recommendation(
    conn, ticker, pclock, calendar, book, limit_table, cost_model, caplog
):
    """Screen (a) means "the broker holds NONE of it", not "the broker is short".

    The §3.6 journal's wide predicate is ``held < tracked``, which also catches a partial exit the
    owner never reported. Those 3 remaining shares are real exposure with a real stop, so withholding
    every exit recommendation from them would turn a diagnostic into a hole in the only protective
    output RECOMMEND mode has. The owner still hears about the mismatch - the §3.6 alert and the day
    plan's block both use the wide reading - but the exit path keeps running."""
    position_id = _open_position(conn, pclock, style="swing")
    _observe_holding(conn, position_id, TODAY - timedelta(days=1), tracked=10, held=3)
    _observe_holding(conn, position_id, TODAY, tracked=10, held=3)
    harness = FakeHarness(_exit_json(position_id))
    ticker.at = datetime(2026, 6, 17, 11, 0, tzinfo=IST)
    pipeline, _ = _exit_pipeline(
        conn, pclock, calendar, book, limit_table, cost_model, harness, position_id
    )

    with caplog.at_level(logging.WARNING, logger="engine.ops.pipeline"):
        await pipeline.on_bar(_near_bar())

    assert len(harness.calls) == 1
    assert conn.execute("SELECT COUNT(*) FROM recommendations").fetchone()[0] == 1
    assert log_events(caplog, "position_event_skipped_sold_outside_ledger") == []


async def test_a_position_that_only_reaches_zero_today_is_not_silenced_yet(
    conn, ticker, pclock, calendar, book, limit_table, cost_model
):
    """The two-day rule applies to the ZERO run in its own right. Held 3 yesterday and 0 today means
    the position only became "gone" on one observation - the same single reading the whole design
    refuses to act on, because a truncated holdings payload looks exactly like this."""
    position_id = _open_position(conn, pclock, style="swing")
    _observe_holding(conn, position_id, TODAY - timedelta(days=1), tracked=10, held=3)
    _observe_holding(conn, position_id, TODAY, tracked=10, held=0)
    harness = FakeHarness(_exit_json(position_id))
    ticker.at = datetime(2026, 6, 17, 11, 0, tzinfo=IST)
    pipeline, _ = _exit_pipeline(
        conn, pclock, calendar, book, limit_table, cost_model, harness, position_id
    )

    await pipeline.on_bar(_near_bar())

    assert len(harness.calls) == 1


async def test_repeat_exit_is_skipped_until_the_stop_changes(
    conn, ticker, pclock, calendar, book, limit_table, cost_model, caplog
):
    """Screen (b): today's exit recommendation already says everything this event would say, so the
    second one is not sent. A MOVED STOP is new information and passes - the screen compares the
    delivered payload against the position row, not a bare "seen today" flag.

    A SWING/CNC position at in-session times, which is the shape the 2026-09-11 incident actually
    had (HDFCAMC and HINDZINC were CNC): a swing exit is stamped valid to the session close, so the
    day's first recommendation is still a live instruction when the next breach arrives. The intraday
    case, where the 20-minute TTL kills the instruction before the 60-minute debounce lets the next
    event through, is ``test_an_expired_exit_no_longer_suppresses_the_next_event``."""
    position_id = _open_position(conn, pclock, style="swing")
    harness = FakeHarness(_exit_json(position_id), _exit_json(position_id))
    ticker.at = datetime(2026, 6, 17, 11, 0, tzinfo=IST)
    pipeline, parts = _exit_pipeline(
        conn, pclock, calendar, book, limit_table, cost_model, harness, position_id
    )

    with caplog.at_level(logging.INFO, logger="engine.ops.pipeline"):
        await pipeline.on_bar(_near_bar())
        assert len(harness.calls) == 1

        ticker.at = ticker.at + timedelta(minutes=POSITION_EVENT_DEBOUNCE_MIN + 1)
        await pipeline.on_bar(_near_bar())                 # same stop, same qty => nothing new
        assert len(harness.calls) == 1
        assert conn.execute("SELECT COUNT(*) FROM recommendations").fetchone()[0] == 1
        repeat = log_events(caplog, "position_event_skipped_repeat_exit")
        assert len(repeat) == 1 and repeat[0].position_id == position_id

        # The owner tightened the stop on a long (99 -> 99.5): a new fact, a new recommendation.
        conn.execute("UPDATE positions SET stop='99.5' WHERE position_id=?", (position_id,))
        conn.commit()
        ticker.at = ticker.at + timedelta(minutes=POSITION_EVENT_DEBOUNCE_MIN + 1)
        await pipeline.on_bar(_near_bar())

    assert len(harness.calls) == 2
    assert conn.execute("SELECT COUNT(*) FROM recommendations").fetchone()[0] == 2
    assert parts["notify"].messages[-1].kind == MessageKind.RECOMMENDATION


async def test_a_changed_quantity_also_passes_the_repeat_screen(
    conn, ticker, pclock, calendar, book, limit_table, cost_model
):
    """A partial exit the owner reported changes what "close the position" means, so the day's
    second exit recommendation is a different instruction and must be delivered. Swing/CNC and both
    events inside the session, so the qty comparison is the ONLY thing that can let the second
    recommendation through."""
    position_id = _open_position(conn, pclock, style="swing")
    harness = FakeHarness(_exit_json(position_id), _exit_json(position_id))
    ticker.at = datetime(2026, 6, 17, 11, 0, tzinfo=IST)
    pipeline, _ = _exit_pipeline(
        conn, pclock, calendar, book, limit_table, cost_model, harness, position_id
    )

    await pipeline.on_bar(_near_bar())
    conn.execute("UPDATE positions SET qty=4 WHERE position_id=?", (position_id,))
    conn.commit()
    ticker.at = ticker.at + timedelta(minutes=POSITION_EVENT_DEBOUNCE_MIN + 1)
    await pipeline.on_bar(_near_bar())

    assert len(harness.calls) == 2
    payloads = [
        json.loads(r[0]) for r in
        conn.execute("SELECT payload FROM recommendations ORDER BY delivered_at").fetchall()
    ]
    assert [p["qty"] for p in payloads] == [10, 4]


async def test_an_expired_exit_no_longer_suppresses_the_next_event(
    conn, ticker, pclock, calendar, book, limit_table, cost_model
):
    """Screen (b) suppresses a repeat only while the earlier exit is STILL AN INSTRUCTION.

    An INTRADAY (MIS) exit is stamped ``now + TTL_INTRADAY_MIN`` (20 minutes) while the position-event
    debounce is 60, so the day's first exit recommendation is always dead by the time the next breach
    is allowed through. Suppressing on "delivered today" alone would leave an owner whose MIS stop
    stayed breached all session with exactly one exit instruction, one that left ``/pending`` twenty
    minutes after it arrived - the platform's only protective output in RECOMMEND mode, silently
    withheld for five more hours. A swing exit is valid to the session close and is unaffected."""
    position_id = _open_position(conn, pclock, style="intraday")
    harness = FakeHarness(_exit_json(position_id), _exit_json(position_id))
    ticker.at = datetime(2026, 6, 17, 11, 0, tzinfo=IST)
    pipeline, _ = _exit_pipeline(
        conn, pclock, calendar, book, limit_table, cost_model, harness, position_id
    )

    await pipeline.on_bar(_near_bar())
    first = json.loads(conn.execute("SELECT payload FROM recommendations").fetchone()[0])
    assert first["valid_until"] == (ticker.at + timedelta(minutes=TTL_INTRADAY_MIN)).isoformat()

    # 61 minutes on: the debounce releases, and the 11:20 instruction is 41 minutes dead.
    ticker.at = ticker.at + timedelta(minutes=POSITION_EVENT_DEBOUNCE_MIN + 1)
    await pipeline.on_bar(_near_bar())

    assert len(harness.calls) == 2
    assert conn.execute("SELECT COUNT(*) FROM recommendations").fetchone()[0] == 2


async def test_an_exit_the_owner_actioned_still_suppresses_the_repeat(
    conn, ticker, pclock, calendar, book, limit_table, cost_model
):
    """The other half of "still an instruction": ``taken``/``dismissed``/``closed`` mean the owner
    ENGAGED with the message. Re-sending it is exactly the noise WO-D2 removes, so an actioned
    recommendation suppresses the repeat even once its TTL has passed."""
    position_id = _open_position(conn, pclock, style="intraday")
    harness = FakeHarness(_exit_json(position_id), _exit_json(position_id))
    ticker.at = datetime(2026, 6, 17, 11, 0, tzinfo=IST)
    pipeline, _ = _exit_pipeline(
        conn, pclock, calendar, book, limit_table, cost_model, harness, position_id
    )

    await pipeline.on_bar(_near_bar())
    conn.execute("UPDATE recommendations SET human_action='dismissed'")
    conn.commit()

    ticker.at = ticker.at + timedelta(minutes=POSITION_EVENT_DEBOUNCE_MIN + 1)
    await pipeline.on_bar(_near_bar())

    assert len(harness.calls) == 1
    assert conn.execute("SELECT COUNT(*) FROM recommendations").fetchone()[0] == 1


async def test_exactly_one_exit_per_session_while_the_stop_stays_breached(
    conn, ticker, pclock, calendar, book, limit_table, cost_model
):
    """THE constraint on screen (b). It is scoped to the DAY, so a position sitting through its stop
    still produces one exit recommendation every session - which is what keeps it inside
    ``gate._exiting_symbols``'s three-calendar-day window (gate.py:1166-1205) and therefore off the
    §7.1 position and sector caps. A screen scoped to the position's lifetime would let the O16
    relaxation lapse and re-block every new swing entry for capacity, which is the 2026-09-07 bug
    this must not re-introduce.

    Swing/CNC, the shape the two 08-26 positions actually had: its exit is valid to the session
    close, so "one per session" is the day-scoped screen doing the work and not a lapsed TTL. The
    ``_exiting_symbols`` assertion is read at the TOP of sessions 2 and 3 - BEFORE that day's own
    recommendation exists - because that is the only moment where a lapse could show: read straight
    after a rec is created it would pass on a zero-age rec no matter how short the window was."""
    position_id = _open_position(conn, pclock, style="swing")
    harness = FakeHarness(*(_exit_json(position_id) for _ in range(3)))
    pipeline, _ = _exit_pipeline(
        conn, pclock, calendar, book, limit_table, cost_model, harness, position_id
    )
    sessions = [date(2026, 6, 17), date(2026, 6, 18), date(2026, 6, 19)]   # Wed / Thu / Fri

    def _exiting() -> frozenset[str]:
        rows = conn.execute("SELECT payload, human_action FROM recommendations").fetchall()
        return _exiting_symbols(rows, frozenset({SYMBOL}), pclock.now())

    for n, session in enumerate(sessions, start=1):
        ticker.at = datetime(session.year, session.month, session.day, 11, 0, tzinfo=IST)
        if n > 1:
            # Yesterday's recommendation, one calendar day old and alone, still carries the O16
            # relaxation into this morning. This is the assertion a lifetime-scoped screen fails.
            assert _exiting() == frozenset({SYMBOL}), f"session {session}: O16 window lapsed"

        await pipeline.on_bar(_near_bar())
        assert len(harness.calls) == n, f"session {session}: expected one analyst call"

        ticker.at = ticker.at + timedelta(minutes=POSITION_EVENT_DEBOUNCE_MIN + 1)
        await pipeline.on_bar(_near_bar())                 # the day's second breach: suppressed
        assert len(harness.calls) == n
        assert conn.execute("SELECT COUNT(*) FROM recommendations").fetchone()[0] == n
        assert _exiting() == frozenset({SYMBOL})

    assert conn.execute("SELECT COUNT(*) FROM recommendations").fetchone()[0] == 3


async def test_an_unreadable_repeat_exit_query_still_lets_the_event_through(
    conn, ticker, pclock, calendar, book, limit_table, cost_model, caplog
):
    """D7 fail-to-zero on screen (b)'s read. The query exists only to WITHHOLD the one protective
    output RECOMMEND mode has, so a database that cannot answer it must not get to decide: the event
    runs. Raising instead would be worse than wrong — ``on_bar`` awaits this inside its per-position
    loop, so one unreadable row would abandon every OTHER position on the same bar."""
    position_id = _open_position(conn, pclock, style="swing")
    harness = FakeHarness(dict(NO_ACTION_JSON))
    ticker.at = datetime(2026, 6, 17, 11, 0, tzinfo=IST)
    pipeline, _ = _exit_pipeline(
        conn, pclock, calendar, book, limit_table, cost_model, harness, position_id
    )
    conn.execute("DROP TABLE learning_ledger")             # the join in _delivered_exit_today

    with caplog.at_level(logging.WARNING, logger="engine.ops.pipeline"):
        await pipeline.on_bar(_near_bar())

    assert len(harness.calls) == 1
    assert len(log_events(caplog, "delivered_exit_read_failed")) == 1


async def test_a_sold_outside_the_ledger_position_ages_out_of_the_O16_window(
    conn, ticker, pclock, calendar, book, limit_table, cost_model
):
    """THE KNOWN COST of screen (a), pinned here rather than discovered live (WO-D2 open question 1).

    Screen (b) leaves one exit recommendation per session standing, which is exactly what keeps a
    breached position inside ``gate._exiting_symbols``'s three-calendar-day window. Screen (a)
    delivers NO exit recommendation at all, so three calendar days after the last one the position
    stops reading as "exiting" and starts counting against the §7.1 ``max_open_positions`` /
    ``per_sector_exposure`` caps again - capacity the owner no longer really has, until the ``/closed``
    reply lands or the §7.1 ``max_holding`` sweep starts issuing its daily deterministic exit (which
    re-enters the window: ``test_the_max_holding_sweep_is_not_gated_by_the_position_event_screens``).

    It is a CAPACITY cost, never a risk one - the caps get tighter, not looser - and the alternative
    inside WO-D2's file scope was worse: teaching the gate this predicate would relax a §7.1 limit on
    the strength of a broker heuristic, and ``risk/gate.py`` is outside the work order. The skip logs
    the consequence at WARNING (``position_event_skipped_sold_outside_ledger``), and this test is the
    other half of that record: a change in either direction has to come past it."""
    position_id = _open_position(conn, pclock, style="swing")
    harness = FakeHarness(_exit_json(position_id))         # exactly ONE canned verdict: a second
    ticker.at = datetime(2026, 6, 17, 11, 0, tzinfo=IST)   # analyst call would raise
    pipeline, _ = _exit_pipeline(
        conn, pclock, calendar, book, limit_table, cost_model, harness, position_id
    )

    def _exiting() -> frozenset[str]:
        rows = conn.execute("SELECT payload, human_action FROM recommendations").fetchall()
        return _exiting_symbols(rows, frozenset({SYMBOL}), pclock.now())

    await pipeline.on_bar(_near_bar())                     # Wed: the last exit rec there will be
    assert len(harness.calls) == 1 and _exiting() == frozenset({SYMBOL})

    # The owner sells outside the ledger; Thu and Fri both journal the broker holding NOTHING.
    _observe_holding(conn, position_id, date(2026, 6, 18))
    _observe_holding(conn, position_id, date(2026, 6, 19))
    ticker.at = datetime(2026, 6, 19, 11, 0, tzinfo=IST)
    await pipeline.on_bar(_near_bar())
    assert len(harness.calls) == 1                         # screen (a): no call, no recommendation
    assert _exiting() == frozenset({SYMBOL})               # Wednesday's rec still carries the window

    # Monday, five calendar days after the only exit recommendation this position will ever get.
    ticker.at = datetime(2026, 6, 22, 11, 0, tzinfo=IST)
    await pipeline.on_bar(_near_bar())

    assert len(harness.calls) == 1
    assert conn.execute("SELECT COUNT(*) FROM recommendations").fetchone()[0] == 1
    assert _exiting() == frozenset()      # ⇒ it occupies a §7.1 position/sector slot again


async def test_the_max_holding_sweep_is_not_gated_by_the_position_event_screens(
    conn, pclock, calendar, book, limit_table, cost_model
):
    """§7.1 ``max_holding`` is a DETERMINISTIC platform decision, not a position event: it spends no
    analyst call and it is the backstop that keeps a stale position from living forever. The WO-D2
    screens sit on the analyst path only and must not reach it."""
    opened = datetime(2026, 3, 2, 10, 0, tzinfo=IST)
    position_id = _open_position(conn, pclock, style="swing", opened_at=opened, stop="95")
    _observe_holding(conn, position_id, TODAY - timedelta(days=1))
    _observe_holding(conn, position_id, TODAY)
    pipeline, _ = _exit_pipeline(
        conn, pclock, calendar, book, limit_table, cost_model, FakeHarness(), position_id
    )

    assert await pipeline.check_aged_positions(TODAY) == 1


# ------------------------------------ the open-positions line the analyst reasons over (WO-D2 residue)
def _summary_pipeline(conn, pclock, calendar, book, limit_table, cost_model):
    """A pipeline built only to render context lines — ``FakeHarness()`` raises on any analyst call."""
    return make_pipeline(
        conn=conn, clock=pclock, calendar=calendar, book=book, harness=FakeHarness(),
        gate=real_gate(limit_table, cost_model, pclock), ctx=passing_ctx(),
        limits=StubLimits(limit_table), store=FakeStore(bars=_flat_bars()),
    )[0]


def test_a_position_sold_outside_the_ledger_leaves_the_open_positions_line(
    conn, pclock, calendar, book, limit_table, cost_model
):
    """The §5.2 analyst and the heartbeat see the book the OWNER holds, not the platform's fiction.

    The position-event screen already refuses to spend a call on a position the §3.6 journal reads
    empty on two sessions, but this line still counted it as open — which is how two 08-26 positions
    stayed in the intraday context for eleven sessions. It moves to a trailing line naming the
    evidence and the reply that settles it, and comes out of the counts above.

    The §7.1 headroom line is asserted UNCHANGED in the same breath: those positions still occupy
    ``max_open_positions`` until ``/closed`` lands, and the gate was deliberately not taught this
    predicate. The two lines disagreeing is the ambiguity, stated."""
    _open_position(conn, pclock, style="intraday")                        # MIS, still held
    gone = _open_position(conn, pclock, style="swing", symbol="HDFCAMC")  # CNC, broker holds none
    _observe_holding(conn, gone, TODAY - timedelta(days=1))
    _observe_holding(conn, gone, TODAY)
    pipeline = _summary_pipeline(conn, pclock, calendar, book, limit_table, cost_model)

    lines = pipeline._positions_summary().splitlines()

    assert lines[0] == "1 open (MIS 1, CNC 0)"        # the CNC one is out of the counts...
    assert len(lines) == 2 and "HDFCAMC" not in lines[0]
    assert lines[1] == (                              # ...and named underneath instead
        "sold outside the ledger (broker holds 0 on 2 sessions, awaiting /closed): HDFCAMC"
    )
    # The gate's own headroom still counts BOTH: this line was not allowed to relax a §7.1 limit.
    assert pipeline._headroom_lines(TODAY)[0].startswith("open positions 2/")


def test_a_held_position_renders_exactly_as_it_did_before(
    conn, pclock, calendar, book, limit_table, cost_model
):
    """No journal rows at all (a fresh position, an engine that was down, a broker erring all week)
    ⇒ the pre-WO-D2 rendering, with no trailing line to explain away."""
    _open_position(conn, pclock, style="intraday")
    _open_position(conn, pclock, style="swing", symbol="HDFCAMC")
    pipeline = _summary_pipeline(conn, pclock, calendar, book, limit_table, cost_model)

    assert pipeline._positions_summary() == "2 open (MIS 1, CNC 1)"


def test_a_partial_holding_stays_in_the_open_positions_line(
    conn, pclock, calendar, book, limit_table, cost_model
):
    """``require_zero=True``, the same reading the exit screen uses: held 3 of a tracked 10 is an
    unreported PARTIAL exit, and those 3 shares are real exposure the analyst must still reason
    about. Only "the broker held nothing on every day of the run" leaves the book."""
    partial = _open_position(conn, pclock, style="swing", symbol="HDFCAMC")
    _observe_holding(conn, partial, TODAY - timedelta(days=1), tracked=10, held=3)
    _observe_holding(conn, partial, TODAY, tracked=10, held=3)
    pipeline = _summary_pipeline(conn, pclock, calendar, book, limit_table, cost_model)

    assert pipeline._positions_summary() == "1 open (MIS 0, CNC 1)"


def test_the_whole_book_sold_outside_the_ledger_reads_none_open(
    conn, pclock, calendar, book, limit_table, cost_model
):
    """The 08-26 shape itself: the counts must say "none open" rather than go negative or keep a
    phantom, and the trailing line carries every name. ``sessions`` is the SHORTEST zero-run among
    them (3 and 2 here), so the line never claims evidence one of its names does not have."""
    old = _open_position(conn, pclock, style="swing", symbol="HDFCAMC")
    new = _open_position(conn, pclock, style="swing", symbol="HINDZINC")
    for day in (TODAY - timedelta(days=2), TODAY - timedelta(days=1), TODAY):
        _observe_holding(conn, old, day)
    _observe_holding(conn, new, TODAY - timedelta(days=1))
    _observe_holding(conn, new, TODAY)
    pipeline = _summary_pipeline(conn, pclock, calendar, book, limit_table, cost_model)

    assert pipeline._positions_summary() == (
        "none open\nsold outside the ledger (broker holds 0 on 2 sessions, awaiting /closed): "
        "HDFCAMC, HINDZINC"
    )


def test_an_unreadable_holdings_journal_leaves_the_plain_counts(
    conn, pclock, calendar, book, limit_table, cost_model, caplog
):
    """D7 fail-to-zero on a context line: a database that cannot answer the journal question degrades
    to the pre-WO-D2 rendering — it never raises into the analyst path and never edits the book."""
    gone = _open_position(conn, pclock, style="swing", symbol="HDFCAMC")
    _observe_holding(conn, gone, TODAY - timedelta(days=1))
    _observe_holding(conn, gone, TODAY)
    pipeline = _summary_pipeline(conn, pclock, calendar, book, limit_table, cost_model)
    assert pipeline._positions_summary().splitlines()[0] == "none open"   # the feature is live...

    conn.execute("DROP TABLE holdings_observations")

    with caplog.at_level(logging.WARNING, logger="engine.ops.pipeline"):
        assert pipeline._positions_summary() == "1 open (MIS 0, CNC 1)"

    assert len(log_events(caplog, "sold_outside_ledger_read_failed")) == 1


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
    Here rsi2's 0.30 is the top of rsi2's measured day and orb's 0.95 is the top of orb's; the tie
    inside the top quantile band falls back to fired_at and then to arrival order, so the earlier
    one goes first even though its raw score is a third of its rival's.

    Both strategies need a MEASURED day for that comparison to mean anything: since D1 (d) a
    population below :data:`MIN_RANK_POPULATION` lands in the middle band instead of the top, so the
    singleton rsi2 this test used to rely on no longer wins by arithmetic (see the 09-10 inversion
    test below)."""
    gov = TunableGovernor(0)               # no slots yet: everything queues, nothing is lost
    harness = FakeHarness(dict(NO_ACTION_JSON), dict(NO_ACTION_JSON))
    pipeline, parts = make_pipeline(
        conn=conn, clock=pclock, calendar=calendar, book=book, harness=harness,
        gate=StubGate(verdict_of("approve", cost_model)), ctx=passing_ctx(),
        limits=StubLimits(limit_table), governor=gov,
    )
    await publish_candidate(pipeline,
        candidate(symbol="HDFCBANK", strategy_id="rsi2", signal_id="R0", score=0.10))
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
def paced_pipeline(conn, pclock, calendar, book, limit_table, cost_model, *, cap, results=6,
                   rearm=None, prescreen=None, warmup_status_fn=None):
    """A ranked+paced pipeline whose forward cap the test can move, with canned analyst declines.

    ``prescreen`` wires the whole §3.2.5 admission seam at once (2026-08-27) — the ``rearm`` callback
    plus the two displacement halves — so an integration test drives a REAL ``SignalPreScreen``
    rather than a mock of the class whose bookkeeping is the thing under test.
    """
    gov = TunableGovernor(cap)
    harness = FakeHarness(*[dict(NO_ACTION_JSON) for _ in range(results)])
    pipeline, parts = make_pipeline(
        conn=conn, clock=pclock, calendar=calendar, book=book, harness=harness,
        gate=StubGate(verdict_of("approve", cost_model)), ctx=passing_ctx(),
        limits=StubLimits(limit_table), governor=gov,
        rearm=rearm if prescreen is None else prescreen.rearm,
        claim_slot=None if prescreen is None else prescreen.claim_slot,
        take_displaced=None if prescreen is None else prescreen.take_displaced,
        warmup_status_fn=warmup_status_fn,
    )
    return pipeline, parts, harness, gov


def slot_evaluated(conn) -> dict[tuple[str, str], int]:
    """The day-slot journal's ``evaluated`` flag per pair â€” 1 = the slot is spent, 0 = re-armed."""
    return {
        (r["symbol"], r["strategy_id"]): int(r["evaluated"])
        for r in conn.execute(
            "SELECT symbol, strategy_id, evaluated FROM prescreen_day_slots"
        ).fetchall()
    }


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


async def test_an_intraday_hole_that_opens_after_a_candidate_is_queued_costs_no_analyst_call(
    conn, pclock, calendar, book, limit_table, ticker, cost_model
):
    """2026-09-13 per-class warm-up, at the OTHER commit point. The arrival screen cannot see this:
    under the default paced drain the candidate is queued while coverage is complete and dispatched
    minutes later — and a reconnect can open a market-wide one-bar hole in between (09-09 14:47).
    Since that no longer freezes the risk state, the drain would pass mode/risk/kill/window/governor,
    claim the §3.2.5 admission slot and spend a §5.2(a) forward slot on a call the gate then rejects
    on ``warmup_ready`` — a charge that bought a verdict, so D1 (c) refunds nothing.

    The screen therefore runs at :meth:`_take_forward_slot`, in the D1 (e) band screen's idiom:
    DEFER, never drop. ``orb`` publishes once a day, so re-arming it out of the queue would silence
    the level for the session; coverage heals mid-session and the queued candidate is still there."""
    from engine.ops.warmup import WarmupStatus

    snapshot = {"status": WarmupStatus(ready=True)}
    pipeline, _, harness, _ = paced_pipeline(
        conn, pclock, calendar, book, limit_table, cost_model, cap=12,
        warmup_status_fn=lambda: snapshot["status"])

    await pipeline.on_signal_candidate(
        candidate(symbol="TCS", strategy_id="orb", signal_id="QUEUED", score=0.9))
    assert [p.candidate.signal_id for p in pipeline._pending_forwards] == ["QUEUED"]

    snapshot["status"] = WarmupStatus(ready=False, blockers=["orb:TCS bars 113/114"])
    assert await pipeline.drain_forward_queue() is False
    assert harness.calls == []                                   # no analyst call was spent
    assert pipeline._forwarded_count == 0                        # and no §5.2(a) slot charged
    assert [p.candidate.signal_id for p in pipeline._pending_forwards] == ["QUEUED"]
    assert forward_journal(conn)[("TCS", "orb")] == 0

    snapshot["status"] = WarmupStatus(ready=True)                # the 60 s gap repair lands
    ticker.at = NOW + timedelta(minutes=FORWARD_PACING_MIN)
    assert await pipeline.drain_forward_queue() is True
    assert len(harness.calls) == 1
    assert pipeline._pending_forwards == []


async def test_a_daily_class_candidate_drains_through_an_intraday_hole(
    conn, pclock, calendar, book, limit_table, ticker, cost_model
):
    """The whole point of the change, at the drain: the same snapshot that defers the intraday leg
    lets the swing leg through — it reads completed daily bars and no 1-minute bar at all."""
    from engine.ops.warmup import WarmupStatus

    snapshot = {"status": WarmupStatus(ready=True)}
    pipeline, _, harness, _ = paced_pipeline(
        conn, pclock, calendar, book, limit_table, cost_model, cap=12,
        warmup_status_fn=lambda: snapshot["status"])

    await pipeline.on_signal_candidate(
        candidate(symbol="TCS", strategy_id="orb", signal_id="INTRA", score=0.9))
    await pipeline.on_signal_candidate(
        candidate(symbol="INFY", strategy_id="brk20", style="swing", signal_id="SWING", score=0.5))
    snapshot["status"] = WarmupStatus(ready=False, blockers=["orb:TCS bars 113/114"])

    # INTRA has the higher score and is picked first; it DEFERS, and the loop takes the next-best in
    # the SAME tick rather than stalling the whole pacing interval on a guaranteed reject.
    assert await pipeline.drain_forward_queue() is True
    assert len(harness.calls) == 1
    assert forward_journal(conn)[("INFY", "brk20")] == 1
    assert forward_journal(conn)[("TCS", "orb")] == 0
    assert [p.candidate.signal_id for p in pipeline._pending_forwards] == ["INTRA"]


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
    saw 20 minutes ago is not the setup any more. Expiry costs no §5.2(a) analyst slot â€” and since
    2026-08-27 it hands the §3.2.5 admission slot back, because nothing evaluated it."""
    rearmed: list[tuple[str, str]] = []
    pipeline, _, harness, _ = paced_pipeline(
        conn, pclock, calendar, book, limit_table, cost_model, cap=12,
        rearm=lambda sym, sid: rearmed.append((sym, sid)) or True)
    await pipeline.on_signal_candidate(
        candidate(symbol="TCS", strategy_id="orb", signal_id="STALE", score=0.99))

    ticker.at = NOW + timedelta(minutes=TTL_INTRADAY_MIN + 1)   # 10:26, still inside the window
    assert await pipeline.drain_forward_queue() is False
    assert harness.calls == []
    assert pipeline._pending_forwards == []                     # expired out of the queue
    assert pipeline._forwarded_count == 0                       # expiry costs no analyst slot
    assert forward_journal(conn)[("TCS", "orb")] == 0
    assert rearmed == [("TCS", "orb")]                          # never evaluated â‡’ slot back
    assert slot_evaluated(conn)[("TCS", "orb")] == 0


async def test_a_candidate_that_ages_out_unseen_gets_its_day_slot_back(
    conn, pclock, calendar, book, limit_table, ticker, cost_model
):
    """THE 2026-08-27 BURN, in the shape it actually happened, against a REAL pre-screen.

    SHRIRAMFIN and POLICYBZR (both ``orb``) were admitted at 09:46:0x, queued behind the paced
    drain, and aged out at their own 20-minute TTL with ZERO rows between them anywhere in
    ``agent_calls`` â€” no analyst call, no gate verdict, nobody judged them. Expiry nevertheless left
    both (symbol, strategy) pairs deduped for the rest of the session, permanently spending 2 of
    ``orb``'s daily admission slots on candidates nothing had looked at. A never-evaluated drop is
    exactly what :meth:`SignalPreScreen.rearm` is for (2026-07-29), so the pairs come back the SAME
    day â€” inside their already-paid quota, since ``_charged`` is never refunded.
    """
    prescreen = SignalPreScreen([], lambda bar: None, max_per_strategy_day={"orb": 2})
    pipeline, _, harness, _ = paced_pipeline(
        conn, pclock, calendar, book, limit_table, cost_model, cap=12, rearm=prescreen.rearm)
    cands = [candidate(symbol=sym, strategy_id="orb", signal_id=sym, score=0.9, catalyst_ref=None)
             for sym in ("SHRIRAMFIN", "POLICYBZR")]

    assert [c.symbol for c in prescreen.admit(cands, TODAY)] == ["SHRIRAMFIN", "POLICYBZR"]
    assert prescreen.admit(cands, TODAY) == []                  # both day slots are now spent
    for cand in cands:
        await pipeline.on_signal_candidate(cand)
    assert harness.calls == []                                  # queued only â€” never evaluated
    assert slot_evaluated(conn) == {("SHRIRAMFIN", "orb"): 1, ("POLICYBZR", "orb"): 1}

    ticker.at = NOW + timedelta(minutes=TTL_INTRADAY_MIN + 1)   # 10:26, still inside the window
    assert await pipeline.drain_forward_queue() is False
    assert harness.calls == []                                  # aged out with no analyst call
    assert pipeline._pending_forwards == []
    assert forward_journal(conn) == {("SHRIRAMFIN", "orb"): 0, ("POLICYBZR", "orb"): 0}
    assert slot_evaluated(conn) == {("SHRIRAMFIN", "orb"): 0, ("POLICYBZR", "orb"): 0}

    # THE FIX: both pairs are admissible again the same day, and the ``orb`` cap of 2 does not
    # bite a second time â€” a re-armed pair re-publishes inside the quota it already paid for.
    assert [c.symbol for c in prescreen.admit(cands, TODAY)] == ["SHRIRAMFIN", "POLICYBZR"]


async def test_a_real_evaluation_never_gets_its_day_slot_back(
    conn, pclock, calendar, book, limit_table, ticker, cost_model
):
    """The other half of the 2026-08-27 rule: only NEVER-EVALUATED is refundable.

    An analyst that RAN and answered ``no_action``, and a candidate the gate actually judged, are
    both real evaluations â€” the day slot stays spent, exactly as it did before (2026-07-29). Nor can
    either be refunded retroactively: :meth:`_take_forward_slot` pops an entry off the queue before
    anything downstream sees it, so no later expiry sweep can ever reach a dispatched candidate.
    """
    rearmed: list[tuple[str, str]] = []
    harness = FakeHarness(dict(NO_ACTION_JSON))
    pipeline, _ = make_pipeline(
        conn=conn, clock=pclock, calendar=calendar, book=book, harness=harness,
        gate=StubGate(verdict_of("approve", cost_model)), ctx=passing_ctx(),
        limits=StubLimits(limit_table), governor=TunableGovernor(12),
        rearm=lambda sym, sid: rearmed.append((sym, sid)) or True,
    )
    await publish_candidate(
        pipeline, candidate(symbol="TCS", strategy_id="orb", signal_id="01SIGNAL", score=0.9))
    assert len(harness.calls) == 1                              # the analyst RAN and declined
    assert pipeline._pending_forwards == []                     # off the queue before the call
    assert rearmed == []                                        # a real evaluation keeps the slot
    assert slot_evaluated(conn)[("TCS", "orb")] == 1

    ticker.at = NOW + timedelta(minutes=TTL_INTRADAY_MIN + 1)   # past its TTL, but long gone
    assert await pipeline.drain_forward_queue() is False
    assert rearmed == []                                        # no retroactive refund
    assert slot_evaluated(conn)[("TCS", "orb")] == 1

    ticker.at = NOW
    judged: list[tuple[str, str]] = []
    gated, _ = make_pipeline(
        conn=conn, clock=pclock, calendar=calendar, book=book, harness=FakeHarness(dict(ENTER_JSON)),
        gate=StubGate(verdict_of("reject", cost_model)), ctx=passing_ctx(),
        limits=StubLimits(limit_table), governor=TunableGovernor(12),
        rearm=lambda sym, sid: judged.append((sym, sid)) or True,
    )
    await publish_candidate(gated, candidate(score=0.9))        # RELIANCE/orb, matches ENTER_JSON
    assert judged == []                                         # a gate verdict IS an evaluation
    assert slot_evaluated(conn)[(SYMBOL, "orb")] == 1


async def test_the_drain_re_checks_the_window_it_was_queued_under(
    conn, pclock, calendar, book, limit_table, ticker, cost_model
):
    """A queued candidate is dispatched LATER than it arrived, so every entry-path gate has to be
    re-asked at the drain: the window can have closed since (1.4 item 11 / 7.1 trade_window). No
    analyst call is made and the forward cap is untouched.

    Since D1 (b) the CLOSED-window branch also flushes: unlike mode/FROZEN/kill, no later tick can
    ever drain what is queued, so the entry leaves with its 3.2.5 admission slot handed back rather
    than sitting in a dead queue for the rest of the process (see the window-close test below)."""
    rearmed: list[tuple[str, str]] = []
    pipeline, _, harness, _ = paced_pipeline(
        conn, pclock, calendar, book, limit_table, cost_model, cap=12,
        rearm=lambda sym, sid: rearmed.append((sym, sid)) or True)
    await pipeline.on_signal_candidate(
        candidate(symbol="TCS", strategy_id="orb", signal_id="INWINDOW", score=0.9))

    ticker.at = datetime(2026, 6, 17, 10, 31, tzinfo=IST)       # seeded window is 10:00-10:30
    assert await pipeline.drain_forward_queue() is False
    assert harness.calls == []
    assert pipeline._forwarded_count == 0
    assert forward_journal(conn)[("TCS", "orb")] == 0
    assert pipeline._pending_forwards == []                     # flushed, not stranded
    assert rearmed == [("TCS", "orb")]


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
    (orb's day is [0.9, 0.9, 0.99] by then - see the D1 (d) tests for why the first candidate of the
    day appears twice - which puts RIVAL in band 4 against BOOM's 3, above ``MIN_RANK_POPULATION``
    so the floor does not flatten the two together). The re-queued candidate still goes first,
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
    # D1 (c) changed the arithmetic here, not the principle: the re-queue REFUNDS the charge the
    # blown-up attempt made, because the front entry it enqueues is charged again when it is
    # re-drained. One candidate, one net charge -- previously two, which at the DG1+ cap of 4 spent
    # half the day on one symbol that was never evaluated.
    assert forward_journal(conn)[("TCS", "orb")] == 1
    assert pipeline._forwarded_count == 1


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


# ====================== 2026-08-27: the per-strategy cap stops being "first 90 seconds wins the day"
def orb_candidate(symbol: str, score: float, **kw) -> SignalCandidate:
    """An ``orb`` candidate with NO catalyst_ref - the default one would make it undisplaceable."""
    return candidate(symbol=symbol, signal_id=f"01{symbol}", score=score, catalyst_ref=None, **kw)


async def test_a_better_afternoon_candidate_takes_a_locked_out_admission_slot(
    conn, pclock, calendar, book, limit_table, ticker, cost_model
):
    """THE 2026-08-27 LOCKOUT, end to end against a REAL pre-screen and a REAL forward queue.

    All 7 of ``orb``'s daily admission slots were charged between 09:46:06 and 09:47:05 - 90 seconds
    of the trade window. TATACONSUM (0.80, 12:28) and KALYANKJIL (**1.0**, 12:49) were then refused
    outright with ``prescreen_cap_suppressed cap="strategy_day"``, three hours later, with zero
    chance regardless of quality and no trace anywhere but the raw log line. A 12:49 ``orb`` fire is
    by design: the scanner's entry window runs to 14:30 and only its RANGE is the first 30 minutes.

    Now the slot moves off the worst incumbent NOBODY HAS LOOKED AT, and both halves of that land:
    the pre-screen re-assigns the admission slot, and the pipeline drops the evicted candidate from
    the forward queue and reverts its journal row - so the freed slot cannot be spent twice.
    """
    prescreen = SignalPreScreen([], lambda bar: None, max_per_strategy_day={"orb": 2})
    pipeline, _, harness, _ = paced_pipeline(
        conn, pclock, calendar, book, limit_table, cost_model, cap=12, prescreen=prescreen)

    morning = [orb_candidate("SHRIRAMFIN", 0.50), orb_candidate("POLICYBZR", 0.62)]
    assert len(prescreen.admit(morning, TODAY)) == 2            # 09:46 - the burst takes both slots
    for cand in morning:
        await pipeline.on_signal_candidate(cand)
    assert harness.calls == []                                  # queued, unevaluated, paced
    assert slot_evaluated(conn) == {("SHRIRAMFIN", "orb"): 1, ("POLICYBZR", "orb"): 1}

    # 12:49. Before this fix `prescreen.admit` returned [] here, and KALYANKJIL ended the day with
    # zero rows in prescreen_day_slots and zero in agent_calls - the log line was its only trace.
    kalyan = orb_candidate("KALYANKJIL", 1.0)
    assert [c.symbol for c in prescreen.admit([kalyan], TODAY)] == ["KALYANKJIL"]
    await pipeline.on_signal_candidate(kalyan)

    # SHRIRAMFIN (0.50, the WORST unevaluated incumbent - not merely the oldest) gave up its slot,
    # and the pipeline acted on that: off the queue, and journalled as never-evaluated.
    queued = {(p.candidate.symbol, p.candidate.strategy_id) for p in pipeline._pending_forwards}
    assert queued == {("POLICYBZR", "orb"), ("KALYANKJIL", "orb")}
    assert slot_evaluated(conn)[("SHRIRAMFIN", "orb")] == 0
    assert slot_evaluated(conn)[("KALYANKJIL", "orb")] == 1
    assert prescreen._count_by_strategy["orb"] == 2             # the SWAP kept the cap exact

    # The analyst budget follows the re-assignment: KALYANKJIL is evaluated, SHRIRAMFIN never is.
    await pipeline._drain_one_forward()
    await pipeline._drain_one_forward()
    assert forward_journal(conn) == {
        ("SHRIRAMFIN", "orb"): 0, ("POLICYBZR", "orb"): 1, ("KALYANKJIL", "orb"): 1,
    }
    assert len(harness.calls) == 2                              # exactly the cap, never cap + 1


async def test_an_evaluated_candidate_survives_a_better_arrival(
    conn, pclock, calendar, book, limit_table, ticker, cost_model
):
    """INVARIANT #1 END TO END - now load-bearing across three fixes (2026-07-29 re-arm, the
    2026-08-27 TTL refund, and displacement), so it gets an integration test of its own.

    Once the analyst has actually been spent on a candidate its slot is permanent. A later arrival
    scoring a PERFECT 1.0 - the strongest displacement claim the score range can express - is
    refused flat, because the cap is now protecting a budget that has genuinely been consumed
    rather than protecting an arrival order.
    """
    prescreen = SignalPreScreen([], lambda bar: None, max_per_strategy_day={"orb": 1})
    pipeline, _, harness, _ = paced_pipeline(
        conn, pclock, calendar, book, limit_table, cost_model, cap=12, prescreen=prescreen)

    incumbent = orb_candidate("SHRIRAMFIN", 0.20)               # a WEAK incumbent, deliberately
    assert len(prescreen.admit([incumbent], TODAY)) == 1
    await publish_candidate(pipeline, incumbent)                # publish AND drain: the analyst runs
    assert len(harness.calls) == 1
    assert pipeline._pending_forwards == []                     # off the queue before the call
    assert forward_journal(conn)[("SHRIRAMFIN", "orb")] == 1
    assert slot_evaluated(conn)[("SHRIRAMFIN", "orb")] == 1

    # 0.20 against 1.00 is the widest margin the clamped score range allows. It still loses.
    assert prescreen.admit([orb_candidate("KALYANKJIL", 1.0)], TODAY) == []
    assert prescreen.take_displaced() == []
    assert prescreen._charged == {("SHRIRAMFIN", "orb")}
    assert slot_evaluated(conn)[("SHRIRAMFIN", "orb")] == 1     # untouched, still spent
    assert len(harness.calls) == 1                              # and no second analyst call


async def test_a_displaced_candidate_is_refused_at_the_analyst_slot(
    conn, pclock, calendar, book, limit_table, ticker, cost_model
):
    """The belt-and-braces half: ``claim_slot`` catches a displacement the queue sweep has not seen.

    ``_apply_displacements`` is the tidy path and runs on the event loop, but the pre-screen decides
    displacement on a SCAN WORKER THREAD, so a drain tick can pop an entry in between. Here the
    displacing candidate is admitted and deliberately NOT published, which leaves the pipeline
    holding a queue pointer to a pair that no longer owns an admission slot. Forwarding it anyway
    would spend the re-assigned slot twice and breach the cap by a real analyst call.
    """
    prescreen = SignalPreScreen([], lambda bar: None, max_per_strategy_day={"orb": 1})
    pipeline, _, harness, _ = paced_pipeline(
        conn, pclock, calendar, book, limit_table, cost_model, cap=12, prescreen=prescreen)

    incumbent = orb_candidate("SHRIRAMFIN", 0.50)
    assert len(prescreen.admit([incumbent], TODAY)) == 1
    await pipeline.on_signal_candidate(incumbent)               # queued, never evaluated

    kalyan = orb_candidate("KALYANKJIL", 1.0)
    assert len(prescreen.admit([kalyan], TODAY)) == 1           # displaces SHRIRAMFIN...
    assert len(pipeline._pending_forwards) == 1                 # ...but the queue has not heard yet

    await pipeline._drain_one_forward()
    assert harness.calls == []                                  # refused at the claim: no call made
    assert pipeline._pending_forwards == []                     # and dropped rather than left to rot
    assert pipeline._forwarded_count == 0                       # a refusal costs no Â§5.2(a) slot
    assert forward_journal(conn)[("SHRIRAMFIN", "orb")] == 0
    assert slot_evaluated(conn)[("SHRIRAMFIN", "orb")] == 0     # journalled as never-evaluated

    # The slot's new owner still gets what it won - the refusal above is a skip, not a stall.
    await publish_candidate(pipeline, kalyan)
    assert forward_journal(conn)[("KALYANKJIL", "orb")] == 1
    assert len(harness.calls) == 1


async def test_a_queue_overflow_hands_back_the_admission_slot_it_drops(
    conn, pclock, calendar, book, limit_table, cost_model, monkeypatch
):
    """The ``MAX_PENDING_FORWARDS`` eviction is the same never-evaluated fact a TTL expiry is.

    Both leave the queue without ever reaching ``_take_forward_slot``, so no analyst call, no
    ``agent_calls`` row and no gate verdict exists for the dropped candidate - which is exactly the
    case the 2026-08-27 TTL refund was written for. The overflow path was left out of that change
    and went on burning the (symbol, strategy) admission slot permanently.
    """
    monkeypatch.setattr(pipeline_module, "MAX_PENDING_FORWARDS", 2)
    prescreen = SignalPreScreen([], lambda bar: None, max_per_strategy_day={"orb": 5})
    pipeline, _, harness, _ = paced_pipeline(
        conn, pclock, calendar, book, limit_table, cost_model, cap=12, prescreen=prescreen)

    cands = [orb_candidate("AAA", 0.90), orb_candidate("BBB", 0.50), orb_candidate("CCC", 0.80)]
    assert len(prescreen.admit(cands, TODAY)) == 3
    for cand in cands:
        await pipeline.on_signal_candidate(cand)          # paced: enqueue only, no drain
    assert harness.calls == []
    assert [p.candidate.symbol for p in pipeline._pending_forwards] == ["AAA", "CCC"]

    # BBB was the worst pending candidate and is the one the overflow drops - so its slot goes back,
    # in memory and in the journal, and it may compete again later.
    assert slot_evaluated(conn)[("BBB", "orb")] == 0
    assert ("BBB", "orb") not in prescreen._seen
    assert slot_evaluated(conn)[("AAA", "orb")] == 1       # the retained entries are untouched
    assert slot_evaluated(conn)[("CCC", "orb")] == 1


async def test_a_queue_overflow_hands_back_a_front_entry_too(
    conn, pclock, calendar, book, limit_table, cost_model, monkeypatch, caplog
):
    """The third exit that used to EXCLUDE a WO-20d ``front`` entry, made to agree with D1 (c).

    The exclusion's premise was "a forward was already charged for this entry, so its slot stays
    spent". The re-queue refunds that charge now, so an evicted front entry holds no charge, has no
    ``agent_calls`` row and no verdict - the exact ``evaluated=1, forwarded=0`` orphan signature
    D1 (a) exists to eliminate. ``_forward_key`` still makes a front entry the LAST thing a full
    queue drops (it is the only entry known to be mid-retry); this is about what happens when it is
    dropped anyway."""
    monkeypatch.setattr(pipeline_module, "MAX_PENDING_FORWARDS", 1)
    rearmed: list[tuple[str, str]] = []
    pipeline, _ = make_pipeline(
        conn=conn, clock=pclock, calendar=calendar, book=book, harness=FakeHarness(),
        gate=StubGate(verdict_of("approve", cost_model)), ctx=passing_ctx(),
        limits=StubLimits(limit_table), governor=TunableGovernor(12),
        rearm=lambda sym, sid: rearmed.append((sym, sid)) or True,
    )
    pipeline._enqueue_forward(orb_candidate("AAA", 0.90), front=True)
    with caplog.at_level(logging.WARNING, logger="engine.ops.pipeline"):
        pipeline._enqueue_forward(orb_candidate("BBB", 0.50), front=True)

    over = log_events(caplog, "forward_queue_overflow")
    assert len(over) == 1 and over[0].front is True
    assert [p.candidate.symbol for p in pipeline._pending_forwards] == ["AAA"]
    assert rearmed == [("BBB", "orb")]


async def test_a_front_entry_kept_in_the_queue_is_not_re_armed(
    conn, pclock, calendar, book, limit_table, cost_model
):
    """``_apply_displacements`` walks the queue and the displaced pairs separately, and the two must
    agree. The prune deliberately RETAINS a WO-20d ``front`` entry (a forward was already charged for
    it, so the pre-screen could not have chosen it as a victim), but the re-arm walk flipped every
    pair in the notice list - handing back a slot the queue still holds a live pointer to. Latent
    under the live wiring; cheap to make consistent."""
    rearmed: list[tuple[str, str]] = []
    notices = [("SHRIRAMFIN", "orb"), ("POLICYBZR", "orb")]
    pipeline, _ = make_pipeline(
        conn=conn, clock=pclock, calendar=calendar, book=book,
        harness=FakeHarness(dict(NO_ACTION_JSON)),
        gate=StubGate(verdict_of("approve", cost_model)), ctx=passing_ctx(),
        limits=StubLimits(limit_table),
        rearm=lambda sym, sid: bool(rearmed.append((sym, sid))),
        take_displaced=lambda: notices,
    )
    pipeline._enqueue_forward(orb_candidate("SHRIRAMFIN", 0.50), front=True)
    pipeline._enqueue_forward(orb_candidate("POLICYBZR", 0.50))

    pipeline._apply_displacements()
    # The front entry is kept, so it keeps its slot too; the ordinary one leaves and gives its back.
    assert [p.candidate.symbol for p in pipeline._pending_forwards] == ["SHRIRAMFIN"]
    assert rearmed == [("POLICYBZR", "orb")]


async def test_expired_then_taken_then_closed_records_the_real_outcome(
    conn, ticker, pclock, book, cost_model
):
    """2026-09-02 review (CONFIRMED): expire_stale stamps outcome_label='no_action', and close()'s
    completion UPDATE matches `outcome_label IS NULL` only - so before the fix an expired->taken->
    closed trade kept the no_action label forever and its real P&L never reached the ledger.
    take() must un-expire the ledger row (outcome_label/closed_at back to NULL)."""
    from datetime import timedelta as _td

    rec = make_rec(cost_model)
    book.deliver(rec, ledger_fields=dict(LEDGER_FIELDS))
    assert book.expire_stale(NOW + _td(days=1)) == 1
    row = conn.execute("SELECT outcome_label FROM learning_ledger WHERE rec_id=?", (rec.rec_id,)).fetchone()
    assert row["outcome_label"] == "no_action"                # the expiry stamp the fix must undo

    await book.take(rec.rec_id, 10, Decimal("100.00"))
    row = conn.execute(
        "SELECT outcome_label, closed_at FROM learning_ledger WHERE rec_id=?", (rec.rec_id,)
    ).fetchone()
    assert row["outcome_label"] is None and row["closed_at"] is None   # un-expired

    await book.close(rec.rec_id, Decimal("103.50"))
    row = conn.execute(
        "SELECT outcome_label, net_pnl FROM learning_ledger WHERE rec_id=?", (rec.rec_id,)
    ).fetchone()
    assert row["outcome_label"] == "win"                      # the real outcome, not no_action
    assert row["net_pnl"] is not None


# ============================== D1 (2026-09-12): a slot is spent only by something that was looked at
def insert_slot(conn, symbol: str, strategy_id: str, *, evaluated: int, forwarded: int,
                unsizeable: int = 0, d=TODAY, score: float = 0.5) -> None:
    """Write a day-slot journal row straight into the DB - the state a RESTART actually inherits."""
    conn.execute(
        "INSERT INTO prescreen_day_slots "
        "(d, symbol, strategy_id, published_at, evaluated, score, forwarded, unsizeable) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (d.isoformat(), symbol, strategy_id, NOW.isoformat(), evaluated, score, forwarded,
         unsizeable),
    )


async def test_a_restart_re_arms_only_the_forward_queue_orphans(
    conn, pclock, calendar, book, limit_table, cost_model, caplog
):
    """D1 (a), in the shape it actually happened (verified 2026-09-11).

    ``_roll_forward_day`` clears the pending queue and rehydrates only the COUNTER, and the owner
    restarts the engine 2-3 times per session - so every restart dropped the queue while the journal
    rows kept ``evaluated=1, forwarded=0``. The pairs stayed deduped for the rest of the day and
    nothing ever looked at them: ~70 admitted candidates burned since 08-17, 210 of 520 slots
    all-time never forwarded. The boot sweep hands exactly those rows back, and nothing else.
    """
    insert_slot(conn, "TCS", "orb", evaluated=1, forwarded=0)            # THE ORPHAN
    insert_slot(conn, "INFY", "orb", evaluated=1, forwarded=1)           # really evaluated
    insert_slot(conn, "WIPRO", "mom", evaluated=1, forwarded=0, unsizeable=1)   # nothing to wait for
    insert_slot(conn, "SBIN", "rsi2", evaluated=0, forwarded=0)          # already re-armed

    rearmed: list[tuple[str, str]] = []
    pipeline, _, harness, _ = paced_pipeline(
        conn, pclock, calendar, book, limit_table, cost_model, cap=12,
        rearm=lambda sym, sid: rearmed.append((sym, sid)) or True)

    with caplog.at_level(logging.INFO, logger="engine.ops.pipeline"):
        # The arriving candidate is journalled BEFORE the day roll, so its own row looks exactly
        # like an orphan - the in_flight exclusion is what keeps the sweep off the candidate it is
        # admitting.
        await pipeline.on_signal_candidate(
            candidate(symbol="ITC", strategy_id="orb", signal_id="ARRIVAL", score=0.9))

    assert rearmed == [("TCS", "orb")]
    slots = slot_evaluated(conn)
    assert slots[("TCS", "orb")] == 0                        # handed back
    assert slots[("INFY", "orb")] == 1                       # forwarded: really evaluated
    assert slots[("WIPRO", "mom")] == 1                      # unsizeable: nothing waits for it
    assert slots[("SBIN", "rsi2")] == 0                      # untouched, already 0
    assert slots[("ITC", "orb")] == 1                        # the in-flight candidate keeps its slot
    assert [p.candidate.signal_id for p in pipeline._pending_forwards] == ["ARRIVAL"]
    assert pipeline._forwarded_count == 1                    # hydrated from INFY, not reset
    assert harness.calls == []

    events = log_events(caplog, "forward_queue_orphans_rearmed")
    assert len(events) == 1
    assert events[0].count == 1 and events[0].pairs == ["TCS/orb"]


async def test_the_orphan_sweep_runs_once_per_day_not_once_per_candidate(
    conn, pclock, calendar, book, limit_table, cost_model, caplog
):
    """The sweep rides the day ROLL, so a second candidate in the same session must not re-arm the
    pair the first one just spent - that would undo every admission the day makes."""
    insert_slot(conn, "TCS", "orb", evaluated=1, forwarded=0)
    rearmed: list[tuple[str, str]] = []
    pipeline, _, _, _ = paced_pipeline(
        conn, pclock, calendar, book, limit_table, cost_model, cap=12,
        rearm=lambda sym, sid: rearmed.append((sym, sid)) or True)

    with caplog.at_level(logging.INFO, logger="engine.ops.pipeline"):
        await pipeline.on_signal_candidate(
            candidate(symbol="ITC", strategy_id="orb", signal_id="FIRST", score=0.9))
        await pipeline.on_signal_candidate(
            candidate(symbol="SBIN", strategy_id="orb", signal_id="SECOND", score=0.8))

    assert rearmed == [("TCS", "orb")]                       # once, on the roll - not per candidate
    assert len(log_events(caplog, "forward_queue_orphans_rearmed")) == 1
    assert slot_evaluated(conn)[("ITC", "orb")] == 1
    assert slot_evaluated(conn)[("SBIN", "orb")] == 1


async def test_the_orphan_sweep_reaches_a_day_where_no_candidate_ever_arrives(
    conn, pclock, calendar, book, limit_table, ticker, cost_model, caplog
):
    """The sweep rides the day roll, and the roll has to reach a RESTART, not just a publication.

    The orphaned pairs are exactly the ones the pre-screen's boot rehydration keeps deduped, so they
    cannot publish themselves - and on a thin day nothing else does either. Hung off
    ``on_signal_candidate`` alone the sweep would then never run and the slots would stay burned, the
    very drought D1 (a) is aimed at. ``drain_forward_queue`` is the 60 s tick that always fires."""
    insert_slot(conn, "BHEL", "hi52", evaluated=1, forwarded=0, score=0.9705)
    insert_slot(conn, "INFY", "brk20", evaluated=1, forwarded=1)
    rearmed: list[tuple[str, str]] = []
    pipeline, _, harness, _ = paced_pipeline(
        conn, pclock, calendar, book, limit_table, cost_model, cap=12,
        rearm=lambda sym, sid: rearmed.append((sym, sid)) or True)

    with caplog.at_level(logging.INFO, logger="engine.ops.pipeline"):
        assert await pipeline.drain_forward_queue() is False  # empty queue: no candidate, ever
    assert rearmed == [("BHEL", "hi52")]
    assert slot_evaluated(conn)[("BHEL", "hi52")] == 0
    assert slot_evaluated(conn)[("INFY", "brk20")] == 1       # forwarded: really evaluated
    assert harness.calls == []
    assert len(log_events(caplog, "forward_queue_orphans_rearmed")) == 1

    ticker.at = NOW + timedelta(minutes=FORWARD_PACING_MIN)
    assert await pipeline.drain_forward_queue() is False      # still once per DAY, not per tick
    assert rearmed == [("BHEL", "hi52")]


async def test_the_window_closing_flushes_the_queue_once_and_hands_the_slots_back(
    conn, pclock, calendar, book, limit_table, ticker, cost_model, caplog
):
    """D1 (b): six candidates were stranded at 12:26 on 2026-09-10, the best of them the hi52 BHEL
    at 0.9705.

    ``_drain_one_forward`` returned on a closed window without touching the queue, and
    ``_expire_forwards`` only ever runs inside ``_take_forward_slot`` - which that early return never
    reaches. So a candidate queued near the close neither forwarded nor expired: it sat in a dead
    queue holding a day slot nothing would ever look at. A ``front`` entry (a WO-20d retry) goes with
    the rest: since D1 (c) the re-queue REFUNDS its forward charge, so leaving it behind would strand
    the one entry whose journal row is already the ``evaluated=1, forwarded=0`` orphan signature.
    """
    rearmed: list[tuple[str, str]] = []
    pipeline, _, harness, _ = paced_pipeline(
        conn, pclock, calendar, book, limit_table, cost_model, cap=12,
        rearm=lambda sym, sid: rearmed.append((sym, sid)) or True)
    await pipeline.on_signal_candidate(
        candidate(symbol="TCS", strategy_id="orb", signal_id="SMALL", score=0.30))
    await pipeline.on_signal_candidate(
        candidate(symbol="BHEL", strategy_id="hi52", signal_id="BEST", score=0.9705))
    pipeline._enqueue_forward(
        candidate(symbol="INFY", strategy_id="orb", signal_id="RETRY", score=0.5), front=True)

    ticker.at = datetime(2026, 6, 17, 12, 26, tzinfo=IST)   # the seeded window closed at 10:30
    with caplog.at_level(logging.WARNING, logger="engine.ops.pipeline"):
        assert await pipeline.drain_forward_queue() is False
    assert harness.calls == []
    assert pipeline._forwarded_count == 0                    # a flush costs no analyst slot
    assert sorted(rearmed) == [("BHEL", "hi52"), ("INFY", "orb"), ("TCS", "orb")]
    assert pipeline._pending_forwards == []

    closed = log_events(caplog, "forward_queue_window_closed")
    assert len(closed) == 1
    assert closed[0].count == 3 and closed[0].best_score == pytest.approx(0.9705)
    assert sorted(closed[0].pairs) == ["BHEL/hi52", "INFY/orb", "TCS/orb"]
    assert closed[0].front == 1
    assert closed[0].closed_at == datetime(2026, 6, 17, 10, 30, tzinfo=IST).isoformat()

    # ONCE per close: an entry that reaches the queue again under the SAME closed window is not
    # re-announced and not re-flushed - that is what the latch is for.
    caplog.clear()
    pipeline._enqueue_forward(
        candidate(symbol="LT", strategy_id="orb", signal_id="LATE", score=0.20))
    ticker.at = datetime(2026, 6, 17, 12, 29, tzinfo=IST)
    with caplog.at_level(logging.WARNING, logger="engine.ops.pipeline"):
        assert await pipeline.drain_forward_queue() is False
    assert log_events(caplog, "forward_queue_window_closed") == []
    assert [p.candidate.signal_id for p in pipeline._pending_forwards] == ["LATE"]
    assert len(rearmed) == 3

    # ...but a window the owner RE-OPENS is a NEW close, not the latched one. ``trade_window`` re-reads
    # the sticky trade_window_state row on every call (3.2.7), and on 2026-08-18 the owner really did
    # move a window mid-session. Keyed on the day, the flush would stay disabled for the rest of that
    # day and the second window's queue would be stranded exactly as before D1 (b) - silently, since
    # the one log line had already been emitted.
    conn.execute(
        "INSERT INTO trade_window_state (id, start_ist, end_ist, squareoff_buffer_min) "
        "VALUES (1, '12:30', '14:00', 0)")
    ticker.at = datetime(2026, 6, 17, 13, 0, tzinfo=IST)     # inside the re-opened window
    await pipeline.on_signal_candidate(
        candidate(symbol="SBIN", strategy_id="orb", signal_id="SECOND_WINDOW", score=0.40))

    ticker.at = datetime(2026, 6, 17, 14, 1, tzinfo=IST)     # and past ITS close
    with caplog.at_level(logging.WARNING, logger="engine.ops.pipeline"):
        assert await pipeline.drain_forward_queue() is False
    reclosed = log_events(caplog, "forward_queue_window_closed")
    assert len(reclosed) == 1
    assert sorted(reclosed[0].pairs) == ["LT/orb", "SBIN/orb"]
    assert reclosed[0].closed_at == datetime(2026, 6, 17, 14, 0, tzinfo=IST).isoformat()
    assert pipeline._pending_forwards == []
    assert ("SBIN", "orb") in rearmed and ("LT", "orb") in rearmed


async def test_a_window_that_has_not_opened_yet_leaves_the_queue_alone(
    conn, pclock, calendar, book, limit_table, ticker, cost_model
):
    """The flush is for a window BEHIND us. Before the open the queue is still live and a later tick
    will drain it - dropping it there would burn the slot the open is about to use."""
    rearmed: list[tuple[str, str]] = []
    pipeline, _, _, _ = paced_pipeline(
        conn, pclock, calendar, book, limit_table, cost_model, cap=12,
        rearm=lambda sym, sid: rearmed.append((sym, sid)) or True)
    pipeline._roll_forward_day(TODAY)                      # the day roll CLEARS the queue; seed after
    pipeline._enqueue_forward(
        candidate(symbol="TCS", strategy_id="orb", signal_id="EARLY", score=0.9))

    ticker.at = datetime(2026, 6, 17, 9, 45, tzinfo=IST)    # before the seeded 10:00 open
    assert await pipeline.drain_forward_queue() is False
    assert [p.candidate.signal_id for p in pipeline._pending_forwards] == ["EARLY"]
    assert rearmed == []


async def test_an_analyst_infrastructure_failure_refunds_the_forward_charge(
    conn, pclock, calendar, book, limit_table, cost_model, caplog
):
    """D1 (c): ``_take_forward_slot`` charges the cap and journals the forward BEFORE the call, so a
    timeout or an SDK death spent a slot on a call that produced no verdict, no ``agent_calls`` row
    and no proposal - 110 charges burned that way, and at the DG1+ cap of 4 one timeout is a quarter
    of the day. The admission slot was already refunded on this path (2026-07-29); the analyst quota
    now follows it, one unit, for the same reason: nothing evaluated it."""
    rearmed: list[tuple[str, str]] = []
    harness = FakeHarness(AgentResult.Failed("timeout", "45s elapsed", call_id="01CALL"))
    pipeline, _ = make_pipeline(
        conn=conn, clock=pclock, calendar=calendar, book=book, harness=harness,
        gate=StubGate(verdict_of("approve", cost_model)), ctx=passing_ctx(),
        limits=StubLimits(limit_table), governor=TunableGovernor(12),
        rearm=lambda sym, sid: rearmed.append((sym, sid)) or True,
    )
    with caplog.at_level(logging.INFO, logger="engine.ops.pipeline"):
        await publish_candidate(
            pipeline, candidate(symbol="TCS", strategy_id="orb", signal_id="DEAD", score=0.9))

    assert rearmed == [("TCS", "orb")]                       # the 2026-07-29 admission re-arm
    assert pipeline._forwarded_count == 0                    # and now the 5.2(a) charge too
    assert forward_journal(conn)[("TCS", "orb")] == 0
    refunds = log_events(caplog, "forward_cap_refunded")
    assert len(refunds) == 1 and refunds[0].reason == "timeout"


async def test_a_governor_block_is_policy_and_refunds_nothing(
    conn, pclock, calendar, book, limit_table, cost_model, caplog
):
    """The other half of D1 (c). A governor block is a deliberate budget decision, not an outage:
    it re-arms nothing (2026-07-29) and refunds nothing, or a blocked window would hand the day's
    whole quota back and hammer the admission gate the moment the block lifts."""
    rearmed: list[tuple[str, str]] = []
    harness = FakeHarness(AgentResult.Failed("governor_blocked", "DG4", call_id="01CALL"))
    pipeline, _ = make_pipeline(
        conn=conn, clock=pclock, calendar=calendar, book=book, harness=harness,
        gate=StubGate(verdict_of("approve", cost_model)), ctx=passing_ctx(),
        limits=StubLimits(limit_table), governor=TunableGovernor(12),
        rearm=lambda sym, sid: rearmed.append((sym, sid)) or True,
    )
    with caplog.at_level(logging.INFO, logger="engine.ops.pipeline"):
        await publish_candidate(
            pipeline, candidate(symbol="TCS", strategy_id="orb", signal_id="BLOCKED", score=0.9))

    assert rearmed == []
    assert pipeline._forwarded_count == 1                    # the charge stands
    assert forward_journal(conn)[("TCS", "orb")] == 1
    assert log_events(caplog, "forward_cap_refunded") == []


async def test_a_refund_can_never_drive_either_counter_negative(
    conn, pclock, calendar, book, limit_table, cost_model
):
    """A restart loses the in-memory count but not the journal row, so a refund can legitimately
    meet a 0 on one side and a stale value on the other. Both are floored: a negative forward count
    would hand the day free analyst calls.

    Two DIFFERENT signal_ids for the same pair, which is the live shape - a re-armed pair
    re-publishes with a freshly minted ULID - and the shape that still drives the floor twice now
    that a repeat refund of the SAME charge is memoed away (see the idempotency test below)."""
    pipeline, _ = make_pipeline(
        conn=conn, clock=pclock, calendar=calendar, book=book, harness=FakeHarness(),
        gate=StubGate(verdict_of("approve", cost_model)), ctx=passing_ctx(),
        limits=StubLimits(limit_table), governor=TunableGovernor(12),
    )
    insert_slot(conn, "TCS", "orb", evaluated=1, forwarded=0)

    pipeline._refund_forward(
        candidate(symbol="TCS", strategy_id="orb", signal_id="NEVER_CHARGED", score=0.9),
        TODAY, reason="test")
    pipeline._refund_forward(
        candidate(symbol="TCS", strategy_id="orb", signal_id="NEVER_CHARGED_EITHER", score=0.9),
        TODAY, reason="test")
    assert pipeline._forwarded_count == 0
    assert forward_journal(conn)[("TCS", "orb")] == 0


async def test_a_refunded_front_entry_hands_its_admission_slot_back_when_it_ages_out(
    conn, pclock, calendar, book, limit_table, ticker, cost_model
):
    """The D1 (c) refund and the WO-20d ``front`` exclusions had to be made to agree.

    Every front exclusion (TTL expiry, queue overflow, the window-close flush) rested on "a forward
    was already charged for this entry, so its slot stays spent". The refund makes that false: a
    re-queued entry carries no charge. Live shape - an ``orb`` candidate is drained, the transport
    blows up, the guard refunds and re-queues it at the FRONT, and then the warm-up freeze re-arms
    (the 2026-08-28 freeze ran 57 minutes) so no tick reaches ``_take_forward_slot`` before the
    entry's 20-minute TTL. It then aged out with ``evaluated=1, forwarded=0``, zero ``agent_calls``
    rows and no verdict: the exact orphan signature D1 (a) exists to eliminate, and one the
    same-process day can never sweep, because the sweep runs only on a day CHANGE."""
    rearmed: list[tuple[str, str]] = []
    harness = FlakyHarness(RuntimeError, 1, dict(NO_ACTION_JSON))
    pipeline, _ = make_pipeline(
        conn=conn, clock=pclock, calendar=calendar, book=book, harness=harness,
        gate=StubGate(verdict_of("approve", cost_model)), ctx=passing_ctx(),
        limits=StubLimits(limit_table), governor=TunableGovernor(12),
        rearm=lambda sym, sid: rearmed.append((sym, sid)) or True,
    )
    await pipeline.on_signal_candidate(
        candidate(symbol="TCS", strategy_id="orb", signal_id="BOOM", score=0.9))
    assert await pipeline.drain_forward_queue() is True
    assert pipeline._pending_forwards[0].front is True
    assert forward_journal(conn)[("TCS", "orb")] == 0        # refunded: it bought no verdict
    assert slot_evaluated(conn)[("TCS", "orb")] == 1         # ...and still holds the admission slot
    assert rearmed == []

    ticker.at = NOW + timedelta(minutes=TTL_INTRADAY_MIN + 1)   # 10:26, still inside the window
    assert await pipeline.drain_forward_queue() is False
    assert harness.attempts == 1                             # the retry never got a tick
    assert pipeline._pending_forwards == []
    assert rearmed == [("TCS", "orb")]                       # never evaluated by anything => back
    assert slot_evaluated(conn)[("TCS", "orb")] == 0
    assert forward_journal(conn)[("TCS", "orb")] == 0


async def test_a_raising_owner_alert_never_refunds_the_same_charge_twice(
    conn, pclock, calendar, book, limit_table, cost_model, caplog
):
    """The refund is ONE unit per charge, and the alert is the seam that could double it.

    ``_evaluate_forward`` runs inside ``_evaluate_forward_guarded``'s try, so an owner-notify seam
    that raises after the refund lands in ``_handle_forward_failure``, which refunds AGAIN for the
    front re-queue it makes - two units undone for one charge, and the re-queued entry is then
    charged a third time when it drains. Ordering the alert BEFORE the refund is what makes the
    arithmetic exact."""
    async def exploding_alert(trigger, result):
        raise RuntimeError("notify seam died")

    harness = FakeHarness(AgentResult.Failed("timeout", "45s elapsed", call_id="01CALL"))
    pipeline, _ = make_pipeline(
        conn=conn, clock=pclock, calendar=calendar, book=book, harness=harness,
        gate=StubGate(verdict_of("approve", cost_model)), ctx=passing_ctx(),
        limits=StubLimits(limit_table), governor=TunableGovernor(12),
    )
    pipeline._alert_agent_failed = exploding_alert
    with caplog.at_level(logging.INFO, logger="engine.ops.pipeline"):
        await publish_candidate(
            pipeline, candidate(symbol="TCS", strategy_id="orb", signal_id="DEAD", score=0.9))

    refunds = log_events(caplog, "forward_cap_refunded")
    assert len(refunds) == 1 and refunds[0].reason == "requeued_RuntimeError"
    assert pipeline._forwarded_count == 0
    assert forward_journal(conn)[("TCS", "orb")] == 0
    assert [p.candidate.signal_id for p in pipeline._pending_forwards] == ["DEAD"]


async def test_one_charge_is_refunded_once_and_a_new_charge_is_refundable_again(
    conn, pclock, calendar, book, limit_table, cost_model, caplog
):
    """D1 (d): the refund is memoed per CHARGE, not merely ordered around the one seam that could
    double it.

    Ordering the alert first fixes the known double; the memo fixes the CLASS. Anything that raises
    between the refund and ``_evaluate_forward``'s return lands in ``_handle_forward_failure``,
    which refunds again for the re-queue it makes - and two units undone for one charge hands the
    day an analyst call the 5.2(a) cap never granted, the one direction of D1 (c) that is not
    conservative (over-charging is safe, under-charging is a budget breach).

    The memo is per charge and not per candidate-day: ``_take_forward_slot`` clears it when it
    charges the same candidate again, because the WO-20d front retry really does make a SECOND
    charge, and that one is really refundable. Anything else would silently re-open the burn D1 (c)
    exists to close."""
    pipeline, _ = make_pipeline(
        conn=conn, clock=pclock, calendar=calendar, book=book, harness=FakeHarness(),
        gate=StubGate(verdict_of("approve", cost_model)), ctx=passing_ctx(),
        limits=StubLimits(limit_table), governor=TunableGovernor(12),
    )
    cand = candidate(symbol="TCS", strategy_id="orb", signal_id="CHARGED", score=0.9)
    pipeline._roll_forward_day(TODAY)                        # the roll clears the queue; seed after
    pipeline._enqueue_forward(cand)
    assert pipeline._take_forward_slot(12) is cand           # the charge, as the drain makes it
    assert pipeline._forwarded_count == 1
    assert forward_journal(conn)[("TCS", "orb")] == 1

    with caplog.at_level(logging.INFO, logger="engine.ops.pipeline"):
        pipeline._refund_forward(cand, TODAY, reason="timeout")
        pipeline._refund_forward(cand, TODAY, reason="requeued_RuntimeError")
    assert pipeline._forwarded_count == 0                    # ONE unit back, not two
    assert forward_journal(conn)[("TCS", "orb")] == 0
    assert len(log_events(caplog, "forward_cap_refunded")) == 1
    skipped = log_events(caplog, "forward_cap_refund_skipped")
    assert len(skipped) == 1 and skipped[0].reason == "requeued_RuntimeError"

    # The WO-20d retry drains the same candidate again: a new charge, refundable on its own terms.
    caplog.clear()
    pipeline._enqueue_forward(cand, front=True)
    assert pipeline._take_forward_slot(12) is cand
    assert pipeline._forwarded_count == 1
    assert forward_journal(conn)[("TCS", "orb")] == 1
    with caplog.at_level(logging.INFO, logger="engine.ops.pipeline"):
        pipeline._refund_forward(cand, TODAY, reason="timeout")
    assert pipeline._forwarded_count == 0
    assert forward_journal(conn)[("TCS", "orb")] == 0
    assert len(log_events(caplog, "forward_cap_refunded")) == 1

    # ...and the memo is per DAY, like every other piece of forward state.
    pipeline._roll_forward_day(TODAY + timedelta(days=1))
    assert pipeline._refunded_forwards == set()


def test_a_strategy_with_an_unmeasured_day_lands_in_the_middle_band(
    conn, pclock, calendar, book, limit_table, cost_model
):
    """D1 (d): an UNMEASURED population is not a TOP one.

    The SINGLETON is the live shape, not the empty set: ``on_signal_candidate`` appends the arriving
    score to ``_day_scores`` one line before it enqueues, so a strategy's first candidate of the day
    is ranked against a population of exactly itself - quantile 1/1 = 1.0, the top band, by
    arithmetic rather than by standing. ``MIN_RANK_POPULATION`` is the floor that removes it, and it
    covers the empty case a journal-hydrated day can present as well.

    Two observations are not a measured day either, which is why the floor is 3 and not 2: a
    two-element CDF can only return 0.5 or 1.0, so the second candidate of a strategy's day would
    still take the TOP band by out-scoring exactly one rival."""
    pipeline, _ = make_pipeline(
        conn=conn, clock=pclock, calendar=calendar, book=book, harness=FakeHarness(),
        gate=StubGate(verdict_of("approve", cost_model)), ctx=passing_ctx(),
        limits=StubLimits(limit_table), governor=TunableGovernor(12),
    )
    assert pipeline._day_scores == {}
    assert pipeline._quantile_band(candidate(strategy_id="orb", score=0.99)) == QUANTILE_BANDS // 2

    pipeline._day_scores["orb"] = [0.99]                      # the singleton: itself, and nothing else
    assert pipeline._quantile_band(candidate(strategy_id="orb", score=0.99)) == QUANTILE_BANDS // 2

    pipeline._day_scores["orb"] = [0.10, 0.99]                # one rival beaten is not a distribution
    assert pipeline._quantile_band(candidate(strategy_id="orb", score=0.99)) == QUANTILE_BANDS // 2
    assert pipeline._quantile_band(candidate(strategy_id="orb", score=0.10)) == QUANTILE_BANDS // 2

    # A MEASURED population still ranks exactly as before - only the unrankable case moved.
    pipeline._day_scores["hi52"] = [0.1, 0.2, 0.3, 0.9705]
    assert pipeline._quantile_band(candidate(strategy_id="hi52", score=0.9705)) == QUANTILE_BANDS - 1
    assert pipeline._quantile_band(candidate(strategy_id="hi52", score=0.05)) == 0
    pipeline._day_scores["cat"] = [0.1, 0.2, 0.3]             # exactly at the floor: ranked, not flat
    assert pipeline._quantile_band(candidate(strategy_id="cat", score=0.3)) == QUANTILE_BANDS - 1
    assert pipeline._quantile_band(candidate(strategy_id="cat", score=0.1)) == 1


async def test_a_lone_late_candidate_no_longer_outranks_a_measured_day(
    conn, pclock, calendar, book, limit_table, cost_model
):
    """THE 2026-09-10 INVERSION, driven through the real admission and selection path.

    A lone late ``orb`` candidate took band 4 on the strength of its own score being the only one in
    its population, tied with the genuinely top-of-its-day ``hi52`` BHEL at 0.9705, and won the tie
    on arrival order. The analyst spent the slot on the 0.12. Asserting through
    ``_quantile_band`` alone cannot tell the fixed code from the broken code here - only which
    candidate the harness actually SEES can."""
    pipeline, parts, harness, _ = paced_pipeline(
        conn, pclock, calendar, book, limit_table, cost_model, cap=1)

    # The lone orb arrives FIRST, so under the old rule it also won the band-4 tie-break.
    await pipeline.on_signal_candidate(
        candidate(symbol="TATASTEEL", strategy_id="orb", signal_id="LONE_ORB", score=0.12))
    for symbol, signal_id, score in (("SBIN", "HI52_LOW", 0.62), ("LT", "HI52_MID", 0.71),
                                     ("BHEL", "HI52_BEST", 0.9705)):
        await pipeline.on_signal_candidate(
            candidate(symbol=symbol, strategy_id="hi52", signal_id=signal_id, score=score))

    # The orb "population of one" is really TWO here, and the difference is why the floor is 3:
    # ``_journal_slot`` writes the arriving row BEFORE ``_roll_forward_day`` hydrates the day's
    # scores from the journal, so the day's very first candidate is counted once by the hydration
    # and once by the append below it. A floor of 2 would have read that as a measured day and
    # handed LONE_ORB the top band all over again.
    assert pipeline._day_scores["orb"] == [0.12, 0.12]
    assert len(pipeline._day_scores["hi52"]) == 3                             # a really measured day
    assert pipeline._quantile_band(
        candidate(strategy_id="orb", score=0.12)) == QUANTILE_BANDS // 2      # was QUANTILE_BANDS - 1
    assert pipeline._quantile_band(
        candidate(strategy_id="hi52", score=0.9705)) == QUANTILE_BANDS - 1
    assert await pipeline._drain_one_forward() is True
    assert parts["assembler"].contexts[-1].stable_block == "stable HI52_BEST"
    assert len(harness.calls) == 1
    assert forward_journal(conn)[("BHEL", "hi52")] == 1
    assert forward_journal(conn)[("TATASTEEL", "orb")] == 0
    assert "LONE_ORB" in [p.candidate.signal_id for p in pipeline._pending_forwards]


# ------------------------------------------------------ D1 (e): the brk20 LIMIT-at-level band screen
def brk20_candidate(entry: str, stop: str, **overrides: Any) -> SignalCandidate:
    """A ``brk20`` swing candidate: a LIMIT pinned to the 20-day breakout LEVEL, product CNC."""
    base: dict[str, Any] = {
        "symbol": "TCS", "strategy_id": "brk20", "style": "swing", "signal_id": "BRK",
        "raw_levels": RawLevels(entry=Decimal(entry), stop=Decimal(stop), target=None),
        "score": 0.9,
    }
    return candidate(**{**base, **overrides})


def band_pipeline(conn, pclock, calendar, book, limit_table, cost_model, ltp):
    """A paced pipeline with the D1 (e) LTP seam wired; ``probe`` records every symbol it asked for.

    ``ltp`` may be a mutable one-element list, so a test can walk the price between drains."""
    probe: list[str] = []
    rearmed: list[tuple[str, str]] = []

    def ltp_fn(symbol: str):
        probe.append(symbol)
        return ltp[0] if isinstance(ltp, list) else ltp

    harness = FakeHarness(dict(NO_ACTION_JSON), dict(NO_ACTION_JSON))
    pipeline, _ = make_pipeline(
        conn=conn, clock=pclock, calendar=calendar, book=book, harness=harness,
        gate=StubGate(verdict_of("approve", cost_model)), ctx=passing_ctx(),
        limits=StubLimits(limit_table), governor=TunableGovernor(12),
        rearm=lambda sym, sid: rearmed.append((sym, sid)) or True, ltp_fn=ltp_fn,
    )
    return pipeline, harness, probe, rearmed


async def test_a_brk20_level_outside_the_entry_band_never_spends_an_analyst_slot(
    conn, pclock, calendar, book, limit_table, cost_model, caplog
):
    """D1 (e): 6 of 15 brk20 proposals died on the gate's ``entry_sanity_band``, each after spending
    an analyst slot. The level is pinned to the 20-day breakout price, so a candidate that waits in
    the paced queue while the price walks away from it is a GUARANTEED reject - deterministic, and
    knowable before the call.

    A DEFERRAL, not a drop. The deviation is a live reading and the band screen is a proxy for the
    gate's rule (the gate bands the analyst's proposal, this bands the scanner's level), so a
    momentary excursion must not end the candidate's day - and it would: brk20 originates only from
    ``run_scan_sweep``, so a pair re-armed out of the queue has nothing to re-publish it. The entry
    stays queued at its own TTL, un-charged and unclaimed, and is re-tested at the next drain."""
    # CNC band is 2.0%; |100 - 97| / 97 = 3.09%.
    ltp = [Decimal("97")]
    pipeline, harness, probe, rearmed = band_pipeline(
        conn, pclock, calendar, book, limit_table, cost_model, ltp)
    with caplog.at_level(logging.INFO, logger="engine.ops.pipeline"):
        await publish_candidate(pipeline, brk20_candidate("100", "98"))

    assert harness.calls == []
    assert pipeline._forwarded_count == 0                    # the slot was never charged
    assert forward_journal(conn)[("TCS", "brk20")] == 0
    assert [p.candidate.signal_id for p in pipeline._pending_forwards] == ["BRK"]
    assert rearmed == []                                     # still queued, so its slot is still its
    assert slot_evaluated(conn)[("TCS", "brk20")] == 1
    assert probe == ["TCS"]
    skipped = log_events(caplog, "forward_skipped_outside_band")
    assert len(skipped) == 1
    assert (skipped[0].symbol, skipped[0].entry, skipped[0].ltp) == ("TCS", "100", "97")
    assert skipped[0].band == "2.0"

    # The price comes back inside the band and the very next drain forwards it - which is the whole
    # reason the entry was kept rather than handed back.
    ltp[0] = Decimal("99")                                   # |100 - 99| / 99 = 1.01% <= 2.0%
    assert await pipeline._drain_one_forward() is True
    assert len(harness.calls) == 1
    assert pipeline._forwarded_count == 1
    assert forward_journal(conn)[("TCS", "brk20")] == 1
    assert pipeline._pending_forwards == []


async def test_a_brk20_level_exactly_on_the_band_edge_is_forwarded(
    conn, pclock, calendar, book, limit_table, cost_model
):
    """The gate's rule is ``dev <= band``, so the edge is a PASS - the screen mirrors it exactly
    rather than approximating it, or it would drop candidates the gate would have approved."""
    # |102 - 100| / 100 = 2.00% == the CNC band.
    pipeline, harness, probe, rearmed = band_pipeline(
        conn, pclock, calendar, book, limit_table, cost_model, Decimal("100"))
    await publish_candidate(pipeline, brk20_candidate("102", "100"))

    assert len(harness.calls) == 1                           # forwarded, not skipped
    assert pipeline._forwarded_count == 1
    assert forward_journal(conn)[("TCS", "brk20")] == 1
    assert rearmed == []
    assert probe == ["TCS"]


async def test_an_unknown_ltp_forwards_the_brk20_candidate_unchanged(
    conn, pclock, calendar, book, limit_table, cost_model
):
    """Fails OPEN in every unknown (D7). No tick for the symbol means "we cannot say this is a
    guaranteed reject", and the direction for that is to forward it and let the gate decide."""
    pipeline, harness, probe, rearmed = band_pipeline(
        conn, pclock, calendar, book, limit_table, cost_model, None)
    await publish_candidate(pipeline, brk20_candidate("100", "98"))

    assert len(harness.calls) == 1
    assert pipeline._forwarded_count == 1
    assert forward_journal(conn)[("TCS", "brk20")] == 1
    assert rearmed == []
    assert probe == ["TCS"]


async def test_an_unwired_ltp_seam_forwards_the_brk20_candidate_unchanged(
    conn, pclock, calendar, book, limit_table, cost_model
):
    """``ltp_fn`` unwired is the pre-2026-09-12 behaviour byte for byte - the screen is opt-in."""
    harness = FakeHarness(dict(NO_ACTION_JSON))
    pipeline, _ = make_pipeline(
        conn=conn, clock=pclock, calendar=calendar, book=book, harness=harness,
        gate=StubGate(verdict_of("approve", cost_model)), ctx=passing_ctx(),
        limits=StubLimits(limit_table), governor=TunableGovernor(12),
    )
    await publish_candidate(pipeline, brk20_candidate("100", "98"))
    assert len(harness.calls) == 1
    assert forward_journal(conn)[("TCS", "brk20")] == 1


async def test_a_price_relative_strategy_is_never_band_screened(
    conn, pclock, calendar, book, limit_table, cost_model
):
    """Scoped to the LIMIT-at-level legs only. An ``orb`` entry tracks the live price, so the same
    screen there would drop candidates on a stale tick rather than on a structural mismatch - this
    one deviates 3.09% against the 1.0% MIS band and is still forwarded, and the LTP seam is never
    even asked."""
    pipeline, harness, probe, rearmed = band_pipeline(
        conn, pclock, calendar, book, limit_table, cost_model, Decimal("97"))
    await publish_candidate(
        pipeline, candidate(symbol="TCS", strategy_id="orb", signal_id="ORB", score=0.9))

    assert len(harness.calls) == 1
    assert pipeline._forwarded_count == 1
    assert forward_journal(conn)[("TCS", "orb")] == 1
    assert rearmed == []
    assert probe == []                                       # not in BAND_SKIP_STRATEGIES


async def test_a_raising_ltp_seam_forwards_the_brk20_candidate_and_warns(
    conn, pclock, calendar, book, limit_table, cost_model, caplog
):
    """A broken seam is the same undecidable case as a missing tick: it warns and forwards. A screen
    that could withhold evaluations whenever its own inputs broke would be a silent kill switch on
    the whole brk20 leg."""
    def ltp_fn(symbol: str):
        raise RuntimeError("tick cache exploded")

    harness = FakeHarness(dict(NO_ACTION_JSON))
    pipeline, _ = make_pipeline(
        conn=conn, clock=pclock, calendar=calendar, book=book, harness=harness,
        gate=StubGate(verdict_of("approve", cost_model)), ctx=passing_ctx(),
        limits=StubLimits(limit_table), governor=TunableGovernor(12), ltp_fn=ltp_fn,
    )
    with caplog.at_level(logging.WARNING, logger="engine.ops.pipeline"):
        await publish_candidate(pipeline, brk20_candidate("100", "98"))

    assert len(harness.calls) == 1
    assert forward_journal(conn)[("TCS", "brk20")] == 1
    failed = log_events(caplog, "forward_band_screen_failed")
    assert len(failed) == 1 and failed[0].symbol == "TCS"
