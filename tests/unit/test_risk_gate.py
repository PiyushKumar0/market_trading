"""RiskGate + GateContext (§3.2.7 / §7.1 / §3.4) — the Tier-2 deterministic gate.

Table-driven by design (§9.1): every ``enter`` rule_id gets a passing case, a failing case and — for
every rule with a numeric/time threshold — an exact-boundary case, all built by flipping ONE field of
a fully-passing baseline context. Two meta-tests make the table self-policing:

* :func:`test_every_documented_rule_has_a_case` fails if a rule_id lacks the required case kinds, so
  adding a limit without tests fails loudly.
* :func:`test_limits_yaml_rule_ids_are_partitioned` fails if a NEW block appears in ``limits.yaml``
  that is neither gate-checked nor consciously listed in ``NOT_GATE_ENFORCED_LIMITS``.

All limit values come from the REAL ``config/limits.yaml`` and the REAL ``config/costs.yaml`` — the
tests assert behaviour against the shipped table, never against numbers duplicated here.
"""

from __future__ import annotations

import re
from datetime import datetime, time, timedelta
from decimal import Decimal
from itertools import product
from pathlib import Path
from typing import Any, NamedTuple

import pytest
import yaml
from pydantic import ValidationError

import engine.risk.gate as gate_module
from engine.core.clock import IST, Clock
from engine.core.contracts import (
    CancelAction,
    EnterAction,
    ExitAction,
    GateVerdict,
    ModifyStopAction,
    ModifyTargetAction,
)
from engine.core.enums import Mode, RiskState
from engine.core.types import TradeWindow
from engine.risk.gate import (
    DOCUMENTED_ENTER_RULES,
    NOT_GATE_ENFORCED_LIMITS,
    SHRINKABLE_RULES,
    GateContext,
    RiskGate,
)
from engine.risk.limits import LimitsBlock, LimitTable
from engine.strategy.cost_model import CostModel, load_cost_rates

REPO = Path(__file__).resolve().parents[2]
LIMITS_YAML = REPO / "config" / "limits.yaml"
COSTS_YAML = REPO / "config" / "costs.yaml"

#: Wed 2026-06-17 10:05 IST — a real trading day, inside every baseline window.
NOW = datetime(2026, 6, 17, 10, 5, tzinfo=IST)
SYMBOL = "RELIANCE"


# --------------------------------------------------------------------------- fixtures
class _StubLimits:
    """Duck-typed ``LimitsEngine``: the gate only ever calls ``load()`` (R4 — no write path)."""

    def __init__(self, table: LimitTable) -> None:
        self._table = table

    def load(self) -> LimitTable:
        return self._table


@pytest.fixture(scope="module")
def limit_table() -> LimitTable:
    return LimitTable.model_validate(yaml.safe_load(LIMITS_YAML.read_text(encoding="utf-8")))


@pytest.fixture(scope="module")
def cost_model() -> CostModel:
    return CostModel(load_cost_rates(COSTS_YAML), edge_multiple_min=Decimal("2.0"))


@pytest.fixture
def gate_clock() -> Clock:
    return Clock(time_source=lambda: NOW)


#: §6.1 `ins` (2026-08-17): the per-strategy pre-registered expected edge the composition root wires
#: from ``settings.yaml`` (``ins.expected_edge_pct`` = the validated WO-16 T+20 NET drift). Consumed by
#: ``min_viable_size`` ONLY for a proposal with no target; every other strategy is unaffected.
INS_EDGE_PCT = Decimal("1.58")


@pytest.fixture
def gate(limit_table: LimitTable, cost_model: CostModel, gate_clock: Clock) -> RiskGate:
    return RiskGate(
        _StubLimits(limit_table), cost_model, gate_clock,
        strategy_expected_edge_pct={"ins": INS_EDGE_PCT},
    )


@pytest.fixture
def gate_no_registered_edges(
    limit_table: LimitTable, cost_model: CostModel, gate_clock: Clock
) -> RiskGate:
    """The pre-2026-08-17 gate: no strategy registers a measured edge, so a targetless entry is a hard
    C3 reject for EVERY strategy. Pins that the new path is opt-in, not a weakening of the default."""
    return RiskGate(_StubLimits(limit_table), cost_model, gate_clock)


# --------------------------------------------------------------------------- baseline builders
#: A context in which EVERY enter rule passes. Each case below flips exactly one field.
BASE_CTX: dict[str, Any] = {
    "now": NOW,
    "mode": Mode.RECOMMEND,
    "risk_state": RiskState.NORMAL,
    "killed": False,
    "degrade_tier": "DG0",
    "trade_window": TradeWindow(start=time(9, 30), end=time(15, 0), squareoff_buffer_min=5),
    "session_open": time(9, 15),
    "session_close": time(15, 30),
    "equity": Decimal("20000"),
    "day_mtm_pct": Decimal("0"),
    "consecutive_losses": 0,
    "entry_recs_today": 0,
    "open_total": 0,
    "open_mis": 0,
    "open_cnc": 0,
    "open_symbols": frozenset(),
    "pending_rec_symbols": frozenset(),
    "per_symbol_cnc_notional": {},
    "sector_of": {SYMBOL: "ENERGY"},
    "open_sector_counts": {},
    "max_corr_with_open": None,
    "deployed_capital": Decimal("0"),
    "ltp": Decimal("100"),
    "tick_age_s": 1.0,
    "index_tick_age_s": 1.0,
    "in_universe": True,
    "mis_candidate": True,
    "surveillance_flag": None,
    "is_fno": True,
    "results_day_today": False,
    "expiry_day": False,
    "is_nifty50": False,
    "warmup_ready": True,
    "regime_ready": True,
    "clock_skew_ok": True,
    "available_margin": None,
    "positions_known": frozenset(),
    "protective_order_ids": frozenset(),
    "known_order_ids": frozenset(),
    "position_side": {},
    "position_stop": {},
    "position_target": {},
}

BASE_ACTION: dict[str, Any] = {
    "action": "enter",
    "proposal_id": "01PROPOSAL",
    "agent_id": "analyst",
    "thesis": "Opening-range breakout with volume confirmation and a tight invalidation level.",
    "confidence": 0.70,
    # Deliberately far ahead so `proposal_stale` never confounds a case that moves ctx.now.
    "valid_until": datetime(2026, 6, 17, 23, 59, tzinfo=IST),
    "inputs_digest": "deadbeef",
    "tradingsymbol": SYMBOL,
    "exchange": "NSE",
    "side": "BUY",
    "style": "intraday",
    "entry_type": "LIMIT",
    "entry_price": Decimal("100"),
    "stop_price": Decimal("99"),
    "target_price": Decimal("103"),
    "quantity": 10,
    "signal_id": "01SIGNAL",
    "strategy_id": "orb",
    "features_snapshot_id": "01SNAP",
}


def make_ctx(**overrides: Any) -> GateContext:
    return GateContext(**{**BASE_CTX, **overrides})


def make_action(**overrides: Any) -> EnterAction:
    return EnterAction(**{**BASE_ACTION, **overrides})


def check_of(verdict: GateVerdict, rule_id: str):
    matches = [c for c in verdict.checks if c.rule_id == rule_id]
    assert len(matches) == 1, f"expected exactly one {rule_id} check, got {len(matches)}"
    return matches[0]


#: The shipped cost surface at module scope, so the CASES table can DERIVE its ``min_viable_size``
#: boundary instead of hardcoding one. Same discipline as the rest of the file (assert against the
#: shipped table, never against numbers duplicated here) — and load-bearing now that costs.yaml is a
#: moving surface: the 2026-08-13 ``spread_pct`` addition shifted the MIS breakeven 0.106% -> 0.126%
#: and silently stale-dated the literal that used to sit here.
_COSTS = CostModel(load_cost_rates(COSTS_YAML), edge_multiple_min=Decimal("2.0"))


def target_at_edge_multiple(entry: Decimal, qty: int, product: str, multiple: Decimal) -> Decimal:
    """A LONG target whose expected edge is EXACTLY ``multiple`` x the shipped breakeven at
    ``qty x entry`` notional — an exact boundary that tracks config/costs.yaml instead of drifting
    from it."""
    breakeven = _COSTS.round_trip(Decimal(qty) * entry, product).breakeven_pct
    return entry * (Decimal(1) + multiple * breakeven / Decimal(100))


#: The exact-2x-breakeven target for the BASE_ACTION shape (entry 100, qty 10, MIS).
BOUNDARY_TARGET = target_at_edge_multiple(Decimal("100"), 10, "MIS", Decimal("2"))

#: …and for a proposal SHRUNK to 3 units. Separate because ``min_viable_size`` is re-run at the
#: shrunk notional, where paise-rounding of the fee components moves the breakeven (₹300 ⇒ 0.130%
#: vs ₹1,000 ⇒ 0.126%) — a target calibrated at the opening ask would not sit on the boundary there.
SHRUNK_BOUNDARY_TARGET = target_at_edge_multiple(Decimal("100"), 3, "MIS", Decimal("2"))

#: A real §6.1 `ins` proposal (2026-08-17): CNC swing, long-only, entry on the pre-open reference,
#: stop a flat 6% below it, and NO target — the exit is the §7.1 20-td time cap. qty 66 is what the
#: shipped table actually allows: 2% swing risk on ₹20,000 = ₹400 budget / a ₹6 stop distance = 66
#: units (₹6,600 notional, inside the ₹8,000/symbol CNC cap) — the plan's "≈ ₹6.7k notional" sizing.
INS_ACTION: dict[str, Any] = {
    "style": "swing",
    "entry_type": "LIMIT",
    "entry_price": Decimal("100"),
    "stop_price": Decimal("94"),
    "target_price": None,
    "quantity": 66,
    "strategy_id": "ins",
}


# --------------------------------------------------------------------------- the rule table
class Case(NamedTuple):
    rule_id: str
    label: str
    kind: str                    # "pass" | "fail" | "boundary" | "na"
    expect_pass: bool
    act: dict[str, Any] = {}
    ctx: dict[str, Any] = {}


CNC = {"style": "swing"}

CASES: tuple[Case, ...] = (
    # ---- mode / kill / freshness -------------------------------------------------------
    Case("mode_risk_state", "auto+normal", "pass", True, ctx={"mode": Mode.AUTO}),
    Case("mode_risk_state", "recommend+normal", "pass", True, ctx={"mode": Mode.RECOMMEND}),
    Case("mode_risk_state", "mode OFF", "fail", False, ctx={"mode": Mode.OFF}),
    Case("mode_risk_state", "risk FROZEN", "fail", False, ctx={"risk_state": RiskState.FROZEN}),
    Case("kill_state", "not killed", "pass", True),
    Case("kill_state", "killed", "fail", False, ctx={"killed": True}),
    Case("proposal_stale", "fresh", "pass", True,
         act={"valid_until": NOW + timedelta(minutes=10)}),
    Case("proposal_stale", "expired", "fail", False,
         act={"valid_until": NOW - timedelta(seconds=1)}),
    Case("proposal_stale", "valid_until == now", "boundary", False, act={"valid_until": NOW}),
    # 2026-07-28 review F4: direction from action.side, never inferred from the stop.
    Case("levels_coherent", "BUY stop below entry, target above", "pass", True),
    Case("levels_coherent", "BUY stop ABOVE entry (inverted short shape)", "fail", False,
         act={"stop_price": Decimal("105"), "target_price": Decimal("95")}),
    Case("levels_coherent", "BUY target below entry", "fail", False,
         act={"target_price": Decimal("99.50")}),
    Case("analyst_confidence_min", "0.70", "pass", True, act={"confidence": 0.70}),
    Case("analyst_confidence_min", "0.50", "fail", False, act={"confidence": 0.50}),
    Case("analyst_confidence_min", "exactly the floor", "boundary", True,
         act={"confidence": 0.55}),
    # ---- windows -----------------------------------------------------------------------
    Case("trade_window", "inside", "pass", True),
    Case("trade_window", "no window set", "fail", False, ctx={"trade_window": None}),
    Case("trade_window", "before start", "fail", False,
         ctx={"now": NOW.replace(hour=9, minute=0)}),
    Case("trade_window", "exactly at start", "boundary", True,
         ctx={"now": NOW.replace(hour=9, minute=30)}),
    Case("trade_window", "exactly at the MIS cutoff", "boundary", True,
         ctx={"now": NOW.replace(hour=14, minute=55)}),
    Case("trade_window", "one minute past the MIS cutoff", "fail", False,
         ctx={"now": NOW.replace(hour=14, minute=56)}),
    Case("no_trade_windows", "mid-window MIS", "pass", True),
    Case("no_trade_windows", "results day T", "fail", False, ctx={"results_day_today": True}),
    Case("no_trade_windows", "exactly at the MIS end", "boundary", True,
         ctx={"now": NOW.replace(hour=14, minute=30)}),
    Case("no_trade_windows", "one minute past the MIS end", "fail", False,
         ctx={"now": NOW.replace(hour=14, minute=31)}),
    Case("no_trade_windows", "expiry NIFTY50 MIS after 14:00", "fail", False,
         ctx={"now": NOW.replace(hour=14, minute=15), "expiry_day": True, "is_nifty50": True}),
    Case("no_trade_windows", "expiry NIFTY50 MIS exactly at 14:00", "boundary", True,
         ctx={"now": NOW.replace(hour=14, minute=0), "expiry_day": True, "is_nifty50": True}),
    Case("no_trade_windows", "CNC window", "pass", True, act=CNC),
    Case("min_residual_window", "plenty of runway", "pass", True),
    Case("min_residual_window", "5 min to the cutoff", "fail", False,
         ctx={"now": NOW.replace(hour=14, minute=50)}),
    Case("min_residual_window", "exactly min_hold_min to the cutoff", "boundary", True,
         ctx={"now": NOW.replace(hour=14, minute=45)}),
    Case("min_residual_window", "n/a for CNC", "pass", True, act=CNC),
    # ---- instrument --------------------------------------------------------------------
    Case("instrument_eligible", "in universe, MIS candidate, F&O", "pass", True),
    Case("instrument_eligible", "out of universe", "fail", False, ctx={"in_universe": False}),
    Case("instrument_eligible", "not an MIS candidate", "fail", False,
         ctx={"mis_candidate": False}),
    Case("instrument_eligible", "not F&O listed (C7)", "fail", False, ctx={"is_fno": False}),
    Case("instrument_eligible", "CNC needs neither MIS flag", "pass", True, act=CNC,
         ctx={"mis_candidate": False, "is_fno": False}),
    Case("surveillance", "unflagged", "pass", True),
    Case("surveillance", "ASM flagged", "fail", False, ctx={"surveillance_flag": "ASM"}),
    Case("surveillance", "empty string is unflagged", "pass", True,
         ctx={"surveillance_flag": "  "}),
    # ---- capital / risk / leverage -----------------------------------------------------
    # O16 2026-09-07: base 40000 / caps 6-2-4 — max_deployed_capital_inr 20000 -> 40000, so every
    # deployed_capital literal below is shifted by the same +20000 to keep sitting on the real cap.
    Case("capital_cap", "nothing deployed", "pass", True),
    Case("capital_cap", "cap exhausted", "fail", False,
         ctx={"deployed_capital": Decimal("39999")}),
    Case("capital_cap", "CNC landing exactly on the cap", "boundary", True, act=CNC,
         ctx={"deployed_capital": Decimal("39000")}),
    Case("capital_cap", "CNC one rupee over the cap", "fail", False, act=CNC,
         ctx={"deployed_capital": Decimal("39001")}),
    # WO-4 notional basis: the SAME deployment that lands exactly on the cap at the stated entry
    # breaches it once the live price the order would actually fill at is used.
    Case("capital_cap", "LTP past the limit tips the exact-cap case over", "fail", False, act=CNC,
         ctx={"deployed_capital": Decimal("39000"), "ltp": Decimal("100.50")}),
    Case("per_trade_risk", "10 x Rs1 stop distance", "pass", True),
    Case("per_trade_risk", "300 units breaches the 1% budget", "fail", False,
         act={"quantity": 300}),
    Case("per_trade_risk", "exactly 1% of equity", "boundary", True, act={"quantity": 200}),
    Case("per_trade_risk", "MARKET with no LTP is unpriceable", "fail", False,
         act={"entry_type": "MARKET", "entry_price": None}, ctx={"ltp": None}),
    Case("per_trade_risk", "swing gap-multiplied budget", "pass", True,
         act={**CNC, "quantity": 160}),
    # ---- WO-4 sizing reference: risk measured from max(entry, LTP) on a long -------------
    Case("per_trade_risk", "LTP past the limit, still inside the budget", "pass", True,
         ctx={"ltp": Decimal("100.50")}),
    Case("per_trade_risk", "LTP past the limit breaches a budget the stale entry cleared",
         "fail", False, act={"quantity": 200}, ctx={"ltp": Decimal("100.50")}),
    Case("per_trade_risk", "LTP exactly at the limit price (max() tie)", "boundary", True,
         act={"quantity": 200}, ctx={"ltp": Decimal("100")}),
    Case("per_trade_risk", "LTP one paisa past the limit", "fail", False,
         act={"quantity": 200}, ctx={"ltp": Decimal("100.01")}),
    Case("per_trade_risk", "LTP below the limit never RELAXES a long", "pass", True,
         act={"quantity": 200}, ctx={"ltp": Decimal("99.50")}),
    # ---- day-scoped counters -----------------------------------------------------------
    Case("daily_loss_soft", "flat day", "pass", True),
    Case("daily_loss_soft", "-5.5%", "fail", False, ctx={"day_mtm_pct": Decimal("-5.5")}),
    Case("daily_loss_soft", "exactly -5% freezes", "boundary", False,
         ctx={"day_mtm_pct": Decimal("-5.0")}),
    Case("daily_loss_soft", "just above -5%", "pass", True,
         ctx={"day_mtm_pct": Decimal("-4.999")}),
    Case("consecutive_losses", "none", "pass", True),
    Case("consecutive_losses", "three in the session", "fail", False,
         ctx={"consecutive_losses": 3}),
    Case("consecutive_losses", "two is still allowed", "boundary", True,
         ctx={"consecutive_losses": 2}),
    Case("max_new_trades_day", "none issued", "pass", True),
    Case("max_new_trades_day", "cap reached", "fail", False, ctx={"entry_recs_today": 5}),
    Case("max_new_trades_day", "one slot left", "boundary", True, ctx={"entry_recs_today": 4}),
    # ---- open exposure -----------------------------------------------------------------
    # O16 2026-09-07: base 40000 / caps 6-2-4 — max_open_positions.total 3 -> 6 (max_mis unchanged
    # at 2), so both the "reached" and "last slot" counts move to sit on the new total cap.
    Case("max_open_positions", "book empty", "pass", True),
    Case("max_open_positions", "total cap reached", "fail", False, ctx={"open_total": 6}),
    Case("max_open_positions", "last total slot", "boundary", True,
         ctx={"open_total": 5, "open_mis": 1}),
    Case("max_open_positions", "MIS leg full", "fail", False,
         ctx={"open_total": 2, "open_mis": 2}),
    Case("max_open_positions", "pending recs occupy slots", "fail", False,
         ctx={"open_total": 1, "pending_rec_symbols": frozenset({"ACME", "ZED"})}),
    Case("per_stock_exposure", "not held", "pass", True),
    Case("per_stock_exposure", "already open in the symbol", "fail", False,
         ctx={"open_symbols": frozenset({SYMBOL})}),
    Case("per_stock_exposure", "pending rec in the symbol", "fail", False,
         ctx={"pending_rec_symbols": frozenset({SYMBOL})}),
    Case("per_stock_exposure", "CNC notional exactly on the cap", "boundary", True, act=CNC,
         ctx={"per_symbol_cnc_notional": {SYMBOL: Decimal("7000")}}),
    Case("per_stock_exposure", "CNC notional over the cap", "fail", False, act=CNC,
         ctx={"per_symbol_cnc_notional": {SYMBOL: Decimal("7500")}}),
    Case("per_sector_exposure", "sector empty", "pass", True),
    Case("per_sector_exposure", "sector cap reached", "fail", False,
         ctx={"open_sector_counts": {"ENERGY": 2}}),
    Case("per_sector_exposure", "one sector slot left", "boundary", True,
         ctx={"open_sector_counts": {"ENERGY": 1}}),
    Case("per_sector_exposure", "UNCLASSIFIED cap is 1", "fail", False,
         ctx={"sector_of": {}, "open_sector_counts": {"UNCLASSIFIED": 1}}),
    Case("co_movement_cap", "no open positions", "pass", True),
    Case("co_movement_cap", "mild correlation", "pass", True,
         ctx={"max_corr_with_open": Decimal("0.5")}),
    Case("co_movement_cap", "exactly corr_max", "boundary", True,
         ctx={"max_corr_with_open": Decimal("0.7")}),
    Case("co_movement_cap", "above corr_max", "fail", False,
         ctx={"max_corr_with_open": Decimal("0.71")}),
    Case("max_leverage", "well inside the cap", "pass", True),
    Case("max_leverage", "3x cap breached", "fail", False,
         act={"stop_price": Decimal("99.9")}, ctx={"equity": Decimal("300")}),
    Case("max_leverage", "exactly 3x equity", "boundary", True,
         act={"stop_price": Decimal("99.9"), "quantity": 9}, ctx={"equity": Decimal("300")}),
    Case("max_leverage", "CNC is 1x cash", "fail", False,
         act={**CNC, "stop_price": Decimal("99.9"), "quantity": 4},
         ctx={"equity": Decimal("300")}),
    # ---- data health -------------------------------------------------------------------
    Case("stale_data_guard", "fresh ticks", "pass", True),
    Case("stale_data_guard", "no symbol feed", "fail", False, ctx={"tick_age_s": None}),
    Case("stale_data_guard", "no index feed", "fail", False, ctx={"index_tick_age_s": None}),
    Case("stale_data_guard", "symbol tick 6s old", "fail", False, ctx={"tick_age_s": 6.0}),
    Case("stale_data_guard", "exactly max tick age", "boundary", True,
         ctx={"tick_age_s": 5.0, "index_tick_age_s": 5.0}),
    Case("warmup_ready", "warm", "pass", True),
    Case("warmup_ready", "cold", "fail", False, ctx={"warmup_ready": False}),
    Case("regime_data_ready", "regime history present", "pass", True),
    Case("regime_data_ready", "regime history missing", "fail", False, ctx={"regime_ready": False}),
    Case("clock_skew", "in sync", "pass", True),
    Case("clock_skew", "skewed/unverifiable", "fail", False, ctx={"clock_skew_ok": False}),
    # ---- pricing sanity ----------------------------------------------------------------
    Case("entry_sanity_band", "limit at LTP", "pass", True),
    Case("entry_sanity_band", "MIS limit 2% off LTP", "fail", False,
         act={"entry_price": Decimal("102")}),
    Case("entry_sanity_band", "MIS limit exactly 1% off LTP", "boundary", True,
         act={"entry_price": Decimal("101")}),
    Case("entry_sanity_band", "CNC limit exactly 2% off LTP", "boundary", True,
         act={**CNC, "entry_price": Decimal("102")}),
    Case("entry_sanity_band", "MARKET has no band to check", "pass", True,
         act={"entry_type": "MARKET", "entry_price": None}),
    Case("entry_sanity_band", "LIMIT with no LTP", "fail", False, ctx={"ltp": None}),
    Case("circuit_proximity", "band feed lands Phase 3", "na", True),
    # ---- order surface / margin --------------------------------------------------------
    Case("margin_buffer", "no API order in RECOMMEND", "pass", True),
    Case("margin_buffer", "margin short of 1.1x", "fail", False, act=CNC,
         ctx={"available_margin": Decimal("1099")}),
    Case("margin_buffer", "exactly 1.1x the requirement", "boundary", True, act=CNC,
         ctx={"available_margin": Decimal("1100")}),
    Case("order_rate", "no API orders in this phase", "na", True),
    Case("order_modifications", "no API orders in this phase", "na", True),
    # ---- edge vs cost ------------------------------------------------------------------
    Case("min_viable_size", "3% edge on a 0.106% breakeven", "pass", True),
    Case("min_viable_size", "no target at all (C3)", "fail", False, act={"target_price": None}),
    Case("min_viable_size", "0.1% edge is sub-viable", "fail", False,
         act={"target_price": Decimal("100.10")}),
    Case("min_viable_size", "edge exactly 2x breakeven", "boundary", True,
         act={"target_price": BOUNDARY_TARGET}),
    # WO-4: the edge is priced at the obtainable price. A trade that clears exactly 2x from the
    # stated entry does not clear it from the price the market has actually moved to.
    Case("min_viable_size", "edge measured from the live reference, not the stated entry",
         "fail", False, act={"target_price": BOUNDARY_TARGET}, ctx={"ltp": Decimal("100.50")}),
    # §6.1 `ins` (2026-08-17): targetless BY DESIGN (the exit is the §7.1 20-td time cap), so the C3
    # edge comes from the strategy's pre-registered measured drift instead of from a fabricated level.
    # The registered edge IS consumed (see the value string) — and at the SHIPPED 6% stop it still
    # falls short of the 2x floor. That is the OPEN BLOCKER pinned in
    # ``test_ins_at_the_shipped_6pct_stop_is_structurally_rejected`` below; this case asserts the
    # live arithmetic, not the outcome anyone wanted.
    Case("min_viable_size", "ins: registered edge consumed but 6% stop sizes below the cost floor",
         "fail", False, act=INS_ACTION),
    # A 4% stop DOES clear — the same shape, sized larger, so the mechanism itself is sound.
    Case("min_viable_size", "ins: registered edge clears costs at a 4% stop", "pass", True,
         act={**INS_ACTION, "stop_price": Decimal("96"), "quantity": 40}),
    # ...and the exception is strategy-scoped: the SAME targetless shape under any other strategy_id
    # is the unchanged hard C3 reject, on the "no target at all" ground rather than on the numbers.
    Case("min_viable_size", "targetless under an unregistered strategy still fails", "fail", False,
         act={**INS_ACTION, "strategy_id": "brk20"}),
)

#: Rules whose threshold is a plain boolean flag — there is no interior value to sit exactly on, so
#: the pass/fail pair IS the boundary. Documented rather than faked with a bogus "boundary" case.
BOOLEAN_ONLY_RULES: frozenset[str] = frozenset({
    "mode_risk_state", "kill_state", "instrument_eligible", "surveillance",
    "warmup_ready", "regime_data_ready", "clock_skew", "levels_coherent",
})

#: Rules that CANNOT fail in Phase 2 because their data source lands later. They must never claim a
#: verified pass — :func:`test_phase2_na_rules_are_marked_na` asserts the honest n/a marker.
PHASE2_NA_RULES: frozenset[str] = frozenset({
    "circuit_proximity", "order_rate", "order_modifications",
})


# --------------------------------------------------------------------------- baseline sanity
def test_baseline_context_approves(gate: RiskGate) -> None:
    verdict = gate.evaluate(make_action(), make_ctx())
    failed = [c.rule_id for c in verdict.checks if not c.passed]
    assert failed == [], f"baseline must pass every rule; failing: {failed}"
    assert verdict.verdict == "approve"
    assert verdict.original_qty == 10
    assert verdict.approved_qty == 10
    assert verdict.reasons == []
    assert verdict.cost is not None and verdict.cost.edge_multiple >= Decimal("2.0")
    assert verdict.mode is Mode.RECOMMEND
    assert verdict.risk_state is RiskState.NORMAL
    assert verdict.degrade_tier == "DG0"
    assert verdict.evaluated_at == NOW
    assert verdict.verdict_id and verdict.proposal_id == "01PROPOSAL"


def test_checks_are_emitted_in_documented_order(gate: RiskGate) -> None:
    verdict = gate.evaluate(make_action(), make_ctx())
    assert [c.rule_id for c in verdict.checks] == list(DOCUMENTED_ENTER_RULES)


# --------------------------------------------------------------------------- per-rule table
@pytest.mark.parametrize("case", CASES, ids=lambda c: f"{c.rule_id}::{c.label}")
def test_rule_case(gate: RiskGate, case: Case) -> None:
    verdict = gate.evaluate(make_action(**case.act), make_ctx(**case.ctx))
    check = check_of(verdict, case.rule_id)
    assert check.passed is case.expect_pass, (
        f"{case.rule_id} [{case.label}] expected passed={case.expect_pass}; "
        f"value={check.value!r} limit={check.limit!r}"
    )
    if not case.expect_pass:
        assert verdict.verdict != "approve"
        assert any(case.rule_id in reason for reason in verdict.reasons)


def test_every_documented_rule_has_a_case() -> None:
    """§9.1: adding a limit without tests must fail. Every rule needs a pass AND a fail case; every
    threshold-bearing rule additionally needs an exact-boundary case."""
    kinds: dict[str, set[str]] = {}
    for case in CASES:
        kinds.setdefault(case.rule_id, set()).add(case.kind)
    missing: list[str] = []
    for rule_id in DOCUMENTED_ENTER_RULES:
        have = kinds.get(rule_id, set())
        if rule_id in PHASE2_NA_RULES:
            required = {"na"}
        elif rule_id in BOOLEAN_ONLY_RULES:
            required = {"pass", "fail"}
        else:
            required = {"pass", "fail", "boundary"}
        if not required <= have:
            missing.append(f"{rule_id}: missing {sorted(required - have)}")
    assert not missing, "rule_ids without full case coverage: " + "; ".join(missing)
    assert set(kinds) <= set(DOCUMENTED_ENTER_RULES), "case table references an unknown rule_id"


def test_phase2_na_rules_are_marked_na(gate: RiskGate) -> None:
    """A rule whose data source is not wired yet must SAY so — never a silent pass claim."""
    verdict = gate.evaluate(make_action(), make_ctx())
    for rule_id in PHASE2_NA_RULES:
        check = check_of(verdict, rule_id)
        assert check.passed is True
        assert "n/a" in check.value, f"{rule_id} must be marked n/a, got {check.value!r}"
    band = check_of(verdict, "circuit_proximity")
    assert "Phase 3" in band.value and "NOT a verified pass" in band.headroom


# --------------------------------------------------------------------------- registry completeness
EXPECTED_ENTER_RULES: tuple[str, ...] = (
    "mode_risk_state", "kill_state", "proposal_stale", "levels_coherent",
    "analyst_confidence_min", "trade_window",
    "no_trade_windows", "min_residual_window", "instrument_eligible", "surveillance", "capital_cap",
    "per_trade_risk", "daily_loss_soft", "consecutive_losses", "max_new_trades_day",
    "max_open_positions", "per_stock_exposure", "per_sector_exposure", "co_movement_cap",
    "max_leverage", "stale_data_guard", "warmup_ready", "regime_data_ready", "clock_skew",
    "entry_sanity_band", "circuit_proximity", "margin_buffer", "order_rate", "order_modifications",
    "min_viable_size",
)


def test_documented_rule_registry_is_frozen(gate: RiskGate) -> None:
    """The rule_ids the gate EMITS must equal the documented registry exactly — a new check without
    a registry entry (or vice versa) fails loudly."""
    assert DOCUMENTED_ENTER_RULES == EXPECTED_ENTER_RULES
    verdict = gate.evaluate(make_action(), make_ctx())
    assert {c.rule_id for c in verdict.checks} == set(EXPECTED_ENTER_RULES)


def test_limits_yaml_rule_ids_are_partitioned() -> None:
    """Every ``limits:`` block is either gate-checked or consciously listed as enforced elsewhere.
    A NEW block in limits.yaml therefore fails here until someone decides which it is."""
    block_ids = set(LimitsBlock.model_fields)
    gate_ids = set(DOCUMENTED_ENTER_RULES)
    assert block_ids - gate_ids == set(NOT_GATE_ENFORCED_LIMITS)
    assert not (set(NOT_GATE_ENFORCED_LIMITS) & gate_ids)
    # §7.1 pins the origination-guard block as enforced UPSTREAM at digest/pre-screen, never here.
    assert "catalyst_guard" not in gate_ids
    assert "catalyst_guard" in NOT_GATE_ENFORCED_LIMITS
    # And the yaml itself carries exactly the blocks the model types (no drift).
    raw = yaml.safe_load(LIMITS_YAML.read_text(encoding="utf-8"))
    assert set(raw["limits"]) == block_ids


def test_shrinkable_registry_covers_yaml_on_breach() -> None:
    """Every rule limits.yaml marks ``shrink*`` must be shrinkable in the gate.

    ``capital_cap`` is the one deliberate ADDITION: its yaml marker is ``reject_entry``, but sizing
    down to the deployed-capital headroom is strictly safer than rejecting outright, and the shrink
    loop re-runs ``min_viable_size`` at the reduced size (so a shrink that stops clearing costs still
    ends in reject, R1/C3). Asserted explicitly so the deviation cannot drift silently.
    """
    raw = yaml.safe_load(LIMITS_YAML.read_text(encoding="utf-8"))["limits"]
    yaml_shrinkable = {
        rule for rule, block in raw.items() if "shrink" in str(block.get("on_breach", ""))
    }
    assert yaml_shrinkable <= set(SHRINKABLE_RULES)
    assert set(SHRINKABLE_RULES) - yaml_shrinkable == {"capital_cap"}
    assert raw["capital_cap"]["on_breach"] == "reject_entry"


# --------------------------------------------------------------------------- shrink loop (R1/C3)
def test_shrink_bound_by_capital_cap(gate: RiskGate) -> None:
    # MIS charged at FULL notional on BOTH sides (2026-07-28 review: the old notional/3x new-leg
    # basis mixed units against the tracker's notional-based deployed figure).
    # O16 2026-09-07: base 40000 / caps 6-2-4 — deployed_capital shifted +20000 to keep Rs300 headroom.
    verdict = gate.evaluate(make_action(), make_ctx(deployed_capital=Decimal("39700")))
    assert verdict.verdict == "shrink"
    assert verdict.original_qty == 10
    assert verdict.approved_qty == 3          # Rs300 headroom / Rs100 full notional = 3
    assert check_of(verdict, "capital_cap").passed is False
    # min_viable_size is re-run at the SHRUNK size and still clears 2x breakeven.
    assert check_of(verdict, "min_viable_size").passed is True
    assert verdict.cost is not None and verdict.cost.notional == Decimal("300.00")
    assert any("shrink: qty 10 -> 3" in r for r in verdict.reasons)


def test_shrink_bound_by_per_trade_risk(gate: RiskGate) -> None:
    verdict = gate.evaluate(make_action(stop_price=Decimal("70")), make_ctx())
    assert verdict.verdict == "shrink"
    assert verdict.approved_qty == 6          # 1% x Rs20,000 = Rs200 budget / Rs30 stop distance
    assert check_of(verdict, "per_trade_risk").passed is False
    assert check_of(verdict, "min_viable_size").passed is True


def test_shrink_bound_by_per_stock_exposure_cnc_notional(gate: RiskGate) -> None:
    verdict = gate.evaluate(
        make_action(style="swing", target_price=Decimal("110")),
        make_ctx(per_symbol_cnc_notional={SYMBOL: Decimal("7500")}),
    )
    assert verdict.verdict == "shrink"
    assert verdict.approved_qty == 5          # Rs500 of the Rs8,000/symbol CNC cap left
    assert check_of(verdict, "per_stock_exposure").passed is False
    assert check_of(verdict, "min_viable_size").passed is True


def test_shrink_bound_by_max_leverage(gate: RiskGate) -> None:
    verdict = gate.evaluate(
        make_action(stop_price=Decimal("99.9")), make_ctx(equity=Decimal("300"))
    )
    assert verdict.verdict == "shrink"
    assert verdict.approved_qty == 9          # 3x Rs300 equity = Rs900 exposure / Rs100
    assert check_of(verdict, "max_leverage").passed is False
    assert check_of(verdict, "min_viable_size").passed is True


def test_shrink_to_sub_viable_size_rejects_mis(gate: RiskGate) -> None:
    """R1/C3: shrink-then-recheck ends in REJECT when the shrunk size no longer clears costs."""
    verdict = gate.evaluate(
        make_action(target_price=Decimal("100.10")),
        make_ctx(deployed_capital=Decimal("19900")),
    )
    assert verdict.verdict == "reject"
    assert verdict.approved_qty == 0
    assert check_of(verdict, "min_viable_size").passed is False
    assert check_of(verdict, "min_viable_size").value.startswith("edge ")


def test_shrink_to_sub_viable_size_rejects_cnc(gate: RiskGate) -> None:
    """The DP charge makes a small delivery clip structurally sub-viable (C4)."""
    verdict = gate.evaluate(
        make_action(style="swing"),
        make_ctx(per_symbol_cnc_notional={SYMBOL: Decimal("7500")}),
    )
    assert verdict.verdict == "reject"
    assert verdict.approved_qty == 0
    assert check_of(verdict, "min_viable_size").passed is False


# --------------------------------------------------------------------------- §6.1 `ins` (2026-08-17)
def test_ins_at_the_shipped_6pct_stop_is_structurally_rejected(gate: RiskGate) -> None:
    """**OPEN BLOCKER, pinned deliberately (found 2026-08-17 building this leg).**

    The §6.1 `ins` addendum sizes the leg as "2% swing risk / 6% stop ≈ ₹6.7k notional ⇒ CNC
    round-trip ≈ 0.53% ⇒ edge multiple ≈ 3x". That arithmetic omits §7.1 ``per_trade_risk``'s
    ``overnight_gap_mult``: for a swing/overnight position the gate charges **2.5 x the stop
    distance** per unit (``gate.py`` ``_rule_per_trade_risk``), not the stop distance itself. So:

        unit risk   = 2.5 x ₹6.00        = ₹15.00      (not ₹6.00)
        budget      = 2% x ₹20,000       = ₹400
        approved    = floor(400 / 15)    = 26 units    (not 66)
        notional    = 26 x ₹100          = ₹2,600      (not ₹6,600)
        breakeven   = CNC round trip @ ₹2,600          = 0.832692%   (not ~0.53%)
        edge mult   = 1.58 / 0.832692    = 1.8975x  <  2.0x  ⇒ REJECT

    At the ₹20,000 capital base every `ins` candidate therefore hard-rejects at C3 with the shipped
    6% stop. This test asserts what the code ACTUALLY does; it is not an endorsement. Resolving it is
    an owner decision (see the sibling test for which stops do clear) — the numbers are pinned here so
    the blocker cannot be forgotten or silently "fixed" by moving a risk limit."""
    verdict = gate.evaluate(make_action(**INS_ACTION), make_ctx())
    assert verdict.verdict == "reject"
    assert verdict.approved_qty == 0

    # The per_trade_risk cap is what shrank it, and it says why in its own words.
    ptr = check_of(verdict, "per_trade_risk")
    assert "2.5x stop distance" in ptr.value

    cost = verdict.cost
    assert cost is not None
    assert cost.notional == Decimal("2600.00")             # 26 units, not the plan's 66
    assert cost.expected_edge_pct == INS_EDGE_PCT          # the registered edge WAS consumed
    assert cost.breakeven_pct == Decimal("0.832692")
    assert cost.edge_multiple == Decimal("1.8975")
    assert cost.edge_multiple < Decimal("2.0")
    assert "pre-registered measured edge" in check_of(verdict, "min_viable_size").value


def test_ins_clears_the_cost_floor_at_a_4pct_stop(gate: RiskGate) -> None:
    """The mechanism is sound — only the shipped 6% default is out of reach. Sweeping the `ins`
    envelope [4-8] against the SHIPPED cost surface, with the 2.5x overnight multiplier applied:

        4% stop -> 40 units, ₹4,000 notional, 0.626250% breakeven -> 2.5230x  PASS
        5% stop -> 32 units, ₹3,200 notional, 0.722188% breakeven -> 2.1878x  PASS
        6% stop -> 26 units, ₹2,600 notional, 0.832692% breakeven -> 1.8975x  REJECT (shipped)
        7% stop -> 22 units, ₹2,200 notional, 0.940000% breakeven -> 1.6809x  REJECT
        8% stop -> 20 units, ₹2,000 notional, 1.009000% breakeven -> 1.5659x  REJECT

    A WIDER stop makes it worse, not better: it shrinks the position, and the CNC DP flat charge is
    a bigger fraction of a smaller notional. The whole viable band is the bottom of the envelope."""
    verdict = gate.evaluate(
        make_action(**{**INS_ACTION, "stop_price": Decimal("96"), "quantity": 40}), make_ctx()
    )
    assert verdict.verdict == "approve", verdict.reasons
    assert verdict.approved_qty == 40                      # 400 budget / (2.5 x 4.00) = 40
    assert verdict.cost is not None
    assert verdict.cost.notional == Decimal("4000.00")
    assert verdict.cost.expected_edge_pct == INS_EDGE_PCT
    assert verdict.cost.edge_multiple == Decimal("2.5230")


def test_ins_targetless_shape_is_levels_coherent(gate: RiskGate) -> None:
    """``levels_coherent`` must accept a missing target (BUY: stop < entry, target unconstrained) —
    otherwise the targetless design could never reach the edge check at all."""
    check = check_of(gate.evaluate(make_action(**INS_ACTION), make_ctx()), "levels_coherent")
    assert check.passed is True


def test_ins_stop_still_binds_per_trade_risk(gate: RiskGate) -> None:
    """The 6% stop is a REAL risk distance, not a formality: it caps the position at the overnight
    risk budget exactly as for any other swing. (The cap here is 26 units — see the blocker test —
    and the proposal is then rejected at C3, so the verdict is reject rather than shrink.)"""
    verdict = gate.evaluate(make_action(**{**INS_ACTION, "quantity": 200}), make_ctx())
    ptr = check_of(verdict, "per_trade_risk")
    assert ptr.passed is False
    assert "max qty 26" in ptr.headroom
    # ...and at a stop the cost floor accepts, the same over-ask SHRINKS cleanly instead of rejecting.
    ok = gate.evaluate(
        make_action(**{**INS_ACTION, "stop_price": Decimal("96"), "quantity": 200}), make_ctx()
    )
    assert ok.verdict == "shrink"
    assert ok.approved_qty == 40


def test_ins_measured_edge_is_opt_in_not_a_weakening_of_c3(
    gate_no_registered_edges: RiskGate,
) -> None:
    """Without the registration the pre-2026-08-17 behaviour is byte-for-byte intact: a targetless
    entry — `ins` or otherwise — is a hard C3 reject. The new path cannot be reached by accident."""
    verdict = gate_no_registered_edges.evaluate(make_action(**INS_ACTION), make_ctx())
    check = check_of(verdict, "min_viable_size")
    assert check.passed is False
    assert check.value == gate_module._NO_TARGET
    assert verdict.verdict == "reject"


def test_ins_registered_edge_is_never_used_when_a_target_exists(gate: RiskGate) -> None:
    """The override is scoped to the targetless case ONLY. If an `ins` proposal ever DID carry a
    target, the edge must still be derived from that target — a registered number may not silently
    replace a stated one."""
    verdict = gate.evaluate(
        make_action(**{**INS_ACTION, "target_price": Decimal("110")}), make_ctx()
    )
    edge = verdict.cost.expected_edge_pct
    assert edge == Decimal("10")                          # (110 - 100)/100, not the registered 1.58
    assert "pre-registered measured edge" not in check_of(verdict, "min_viable_size").value


def test_ins_measured_edge_still_faces_the_cost_floor(
    limit_table: LimitTable, gate_clock: Clock
) -> None:
    """The registered edge is an INPUT to C3, never an exemption from it: a strategy whose measured
    edge does not clear 2x the breakeven at the approved size is rejected exactly like any other."""
    thin = RiskGate(
        _StubLimits(limit_table),
        CostModel(load_cost_rates(COSTS_YAML), edge_multiple_min=Decimal("2.0")),
        gate_clock,
        strategy_expected_edge_pct={"ins": Decimal("0.10")},   # a tenth of a percent — sub-cost
    )
    verdict = thin.evaluate(make_action(**INS_ACTION), make_ctx())
    assert check_of(verdict, "min_viable_size").passed is False
    assert verdict.verdict == "reject"


def test_ins_edge_matches_the_shipped_settings_value() -> None:
    """The 1.58 asserted above is the validated WO-16 T+20 NET drift the settings file actually
    ships — the tests must not drift from the config the engine boots with."""
    from engine.core.config import load_settings

    assert Decimal(str(load_settings().ins.expected_edge_pct)) == INS_EDGE_PCT


def test_zero_headroom_rejects_without_shrinking(gate: RiskGate) -> None:
    # O16 2026-09-07: base 40000 / caps 6-2-4 — deployed_capital at the new cap (was 20000).
    verdict = gate.evaluate(make_action(), make_ctx(deployed_capital=Decimal("40000")))
    assert verdict.verdict == "reject"
    assert verdict.approved_qty == 0
    assert any("nothing approvable" in r for r in verdict.reasons)


def test_hard_failure_beats_shrink(gate: RiskGate) -> None:
    """Verdict precedence: a non-shrinkable breach forces reject even when a shrink would fit."""
    verdict = gate.evaluate(
        make_action(), make_ctx(deployed_capital=Decimal("19900"), killed=True)
    )
    assert verdict.verdict == "reject"
    assert check_of(verdict, "kill_state").passed is False


def test_unpriceable_market_order_rejects(gate: RiskGate) -> None:
    verdict = gate.evaluate(
        make_action(entry_type="MARKET", entry_price=None), make_ctx(ltp=None)
    )
    assert verdict.verdict == "reject"
    assert verdict.approved_qty == 0
    for rule_id in ("capital_cap", "per_trade_risk", "max_leverage", "min_viable_size"):
        assert check_of(verdict, rule_id).passed is False


# --------------------------------------------------------------------------- monotone properties
def test_gate_never_enlarges_over_a_grid(gate: RiskGate) -> None:
    """R1 monotone property: approved_qty <= original_qty for every input combination."""
    quantities = (1, 3, 10, 47, 250, 1000)
    equities = (Decimal("300"), Decimal("5000"), Decimal("20000"), Decimal("41000"))
    deployed = (Decimal("0"), Decimal("9000"), Decimal("19900"), Decimal("20000"))
    stops = (Decimal("99.9"), Decimal("99"), Decimal("80"))
    styles = ("intraday", "swing")
    seen_shrink = seen_approve = seen_reject = False
    for qty, equity, dep, stop, style in product(quantities, equities, deployed, stops, styles):
        verdict = gate.evaluate(
            make_action(quantity=qty, stop_price=stop, style=style),
            make_ctx(equity=equity, deployed_capital=dep),
        )
        assert verdict.original_qty == qty
        assert verdict.approved_qty is not None
        assert 0 <= verdict.approved_qty <= qty, (
            f"gate enlarged: {verdict.approved_qty} > {qty} "
            f"(equity={equity} deployed={dep} stop={stop} style={style})"
        )
        if verdict.verdict == "shrink":
            seen_shrink = True
            assert verdict.approved_qty < qty
        elif verdict.verdict == "approve":
            seen_approve = True
            assert verdict.approved_qty == qty
        elif verdict.verdict == "reject":
            seen_reject = True
            assert verdict.approved_qty == 0
    assert seen_shrink and seen_approve and seen_reject, "grid must exercise all three verdicts"


# ------------------------------------------------------- WO-4 sizing reference (§7.1 / F5)
#: A coherent SHORT built off the same baseline: stop ABOVE entry, target BELOW (levels_coherent).
SHORT: dict[str, Any] = {
    "side": "SELL", "stop_price": Decimal("101"), "target_price": Decimal("97"),
}


def sizing_refs_of(verdict: GateVerdict) -> tuple[Decimal, Decimal]:
    """The two WO-4 bases the gate ACTUALLY sized on, read back off the audit trail.

    Deliberately parsed from the shipped CheckResults rather than recomputed here: the reference is
    only auditable (and only renderable into the §3.6 payload) if the verdict states it, so these
    strings are part of the contract, not incidental prose.
    """
    risk = re.search(r"stop distance from ([\d.]+)", check_of(verdict, "per_trade_risk").value)
    notional = re.search(r"@ ([\d.]+)", check_of(verdict, "capital_cap").value)
    assert risk and notional, "the gate must state the price each sizing rule sized off"
    return Decimal(risk.group(1)), Decimal(notional.group(1))


class RefCase(NamedTuple):
    label: str
    kind: str                     # "pass" | "fail" | "boundary" | "shrink"
    act: dict[str, Any]
    ltp: Decimal | None
    risk_ref: str                 # expected risk basis  (per_trade_risk / min_viable_size)
    notional_ref: str             # expected notional basis (capital_cap / leverage / margin)


REF_CASES: tuple[RefCase, ...] = (
    # -- long: risk and notional both take the HIGHER price -------------------------------
    RefCase("BUY, LTP ran past the limit", "pass", {}, Decimal("100.50"), "100.50", "100.50"),
    RefCase("BUY, LTP below the limit — the stated entry is already the worse price",
            "pass", {}, Decimal("99.50"), "100.00", "100.00"),
    RefCase("BUY, LTP exactly at the limit (max() tie)", "boundary", {}, Decimal("100"),
            "100.00", "100.00"),
    # -- short: risk takes the LOWER price, notional still takes the higher ----------------
    RefCase("SELL, LTP fell below the limit", "pass", SHORT, Decimal("99.50"), "99.50", "100.00"),
    RefCase("SELL, LTP rose above the limit", "pass", SHORT, Decimal("100.50"), "100.00", "100.50"),
    RefCase("SELL, LTP exactly at the limit (min() tie)", "boundary", SHORT, Decimal("100"),
            "100.00", "100.00"),
    # -- unchanged paths -------------------------------------------------------------------
    RefCase("MARKET already sizes off the live LTP", "pass",
            {"entry_type": "MARKET", "entry_price": None}, Decimal("100.50"), "100.50", "100.50"),
    RefCase("LIMIT with no LTP falls back to the stated entry", "fail", {}, None,
            "100.00", "100.00"),
)


@pytest.mark.parametrize("case", REF_CASES, ids=lambda c: c.label)
def test_sizing_reference_selection(gate: RiskGate, case: RefCase) -> None:
    """WO-4: ONE selection point, stated as ``max(entry, LTP)`` long / ``min`` short for the risk
    basis and ``max`` on both sides for the notional basis."""
    verdict = gate.evaluate(make_action(**case.act), make_ctx(ltp=case.ltp))
    risk, notional = sizing_refs_of(verdict)
    assert (str(risk), str(notional)) == (case.risk_ref, case.notional_ref)


def test_entry_sanity_band_still_judges_the_STATED_entry(gate: RiskGate) -> None:
    """WO-4 explicitly leaves the hard-reject alone: it compares ``action.entry_price`` with the
    live LTP. Routing it through the sizing reference would make it compare the LTP with itself and
    the band could never fail — the exact rule that catches a nonsense limit price."""
    action = make_action(entry_price=Decimal("102"))          # 2% off a 100 LTP; MIS band is 1%
    verdict = gate.evaluate(action, make_ctx())
    band = check_of(verdict, "entry_sanity_band")
    assert band.passed is False
    assert "102" in band.value and "100" in band.value
    assert verdict.verdict == "reject"
    # ...and the sizing reference did move, so the band is failing on its own terms, not by accident.
    assert sizing_refs_of(verdict) == (Decimal("102.00"), Decimal("102.00"))


def test_sizing_reference_shrink_then_recheck_rejects(gate: RiskGate) -> None:
    """R1/C3 shrink-then-recheck through the WO-4 reference: the live price shrinks the capital-cap
    headroom, and ``min_viable_size`` — re-run at the SHRUNK size and the SAME live price — no
    longer clears costs, so the shrink ends in reject rather than a thinner recommendation."""
    # O16 2026-09-07: base 40000 / caps 6-2-4 — deployed_capital shifted +20000 to keep Rs300 headroom.
    action = make_action(target_price=SHRUNK_BOUNDARY_TARGET)  # exactly 2x breakeven at qty 3
    stale = gate.evaluate(action, make_ctx(deployed_capital=Decimal("39700")))
    assert stale.verdict == "shrink" and stale.approved_qty == 3     # Rs300 headroom / Rs100
    assert check_of(stale, "min_viable_size").passed is True

    moved = gate.evaluate(action, make_ctx(deployed_capital=Decimal("39700"),
                                           ltp=Decimal("100.50")))
    assert "max qty 2" in check_of(moved, "capital_cap").headroom   # Rs300 / Rs100.50 = 2, not 3
    assert check_of(moved, "min_viable_size").passed is False
    assert moved.verdict == "reject" and moved.approved_qty == 0
    assert any("min_viable_size" in reason for reason in moved.reasons)


#: LTP moves that stay INSIDE both entry_sanity_band widths (MIS 1% / CNC 2%) around entry 100, so
#: the grid isolates the sizing reference instead of measuring the band's hard reject.
IN_BAND_LTPS = (Decimal("99.20"), Decimal("99.60"), Decimal("100"), Decimal("100.40"),
                Decimal("100.80"))


def test_sizing_reference_can_only_shrink_long_and_short(gate: RiskGate) -> None:
    """THE monotone-safety property (WO-4 risk note): switching from the stale stated-entry basis to
    the live reference can only DECREASE the approved size — never increase it — in BOTH directions.

    The stale basis is reproduced exactly by evaluating with ``LTP == entry_price``, where the
    selection is provably a no-op (``max(x, x) == min(x, x) == x``); every other point is compared
    against it.
    """
    strict_decreases = 0
    for side, qty, dep, style, ltp in product(
        ("BUY", "SELL"), (1, 10, 133, 200), (Decimal("0"), Decimal("19000")),
        ("intraday", "swing"), IN_BAND_LTPS,
    ):
        act = {"quantity": qty, "style": style, **({} if side == "BUY" else SHORT)}
        action = make_action(**act)
        stale = gate.evaluate(action, make_ctx(deployed_capital=dep, ltp=Decimal("100")))
        moved = gate.evaluate(action, make_ctx(deployed_capital=dep, ltp=ltp))
        assert stale.approved_qty is not None and moved.approved_qty is not None
        assert moved.approved_qty <= stale.approved_qty, (
            f"sizing reference ENLARGED the size: {stale.approved_qty} -> {moved.approved_qty} "
            f"(side={side} qty={qty} deployed={dep} style={style} ltp={ltp})"
        )
        assert moved.approved_qty <= moved.original_qty     # the R1 invariant, restated per point
        if moved.approved_qty < stale.approved_qty:
            strict_decreases += 1
    # Non-vacuous: the property is not holding merely because nothing ever changes.
    assert strict_decreases > 0, "grid never exercised an adverse reference"


@pytest.mark.parametrize("side", ["BUY", "SELL"])
def test_adverse_reference_widens_the_risk_distance(gate: RiskGate, side: str) -> None:
    """Why the shrink direction is guaranteed: with coherent levels the adverse fill always sits on
    the far side of the stated entry FROM the stop, so |ref - stop| can only grow."""
    act = {} if side == "BUY" else SHORT
    stop = Decimal("99") if side == "BUY" else Decimal("101")
    adverse = Decimal("100.80") if side == "BUY" else Decimal("99.20")
    stale_risk, _ = sizing_refs_of(gate.evaluate(make_action(**act), make_ctx(ltp=Decimal("100"))))
    moved_risk, _ = sizing_refs_of(gate.evaluate(make_action(**act), make_ctx(ltp=adverse)))
    assert abs(moved_risk - stop) > abs(stale_risk - stop)


def test_evaluate_is_pure(gate: RiskGate) -> None:
    """Same (action, ctx) ⇒ same verdict apart from the minted verdict_id (replay determinism)."""
    action, ctx = make_action(), make_ctx()
    first, second = gate.evaluate(action, ctx), gate.evaluate(action, ctx)
    assert first.model_dump(exclude={"verdict_id"}) == second.model_dump(exclude={"verdict_id"})
    assert first.verdict_id != second.verdict_id


# --------------------------------------------------------------------------- exit (§3.2.7)
HOSTILE = {
    "mode": Mode.OFF,
    "risk_state": RiskState.CLOSE_ONLY,
    "killed": True,
    "day_mtm_pct": Decimal("-20"),
    "consecutive_losses": 9,
    "entry_recs_today": 99,
    "open_total": 9,
    "warmup_ready": False,
    "regime_ready": False,
    "clock_skew_ok": False,
    "tick_age_s": None,
    "index_tick_age_s": None,
    "trade_window": None,
    "deployed_capital": Decimal("20000"),
}


def _exit(**kw: Any) -> ExitAction:
    return ExitAction(
        action="exit", proposal_id="01EXIT", thesis="Thesis invalidated on the 5m close." * 2,
        confidence=0.9, valid_until=NOW + timedelta(minutes=5), position_id="POS1",
        exit_type="MARKET", reason="thesis_invalidated", **kw,
    )


def test_exit_is_never_limit_rejected(gate: RiskGate) -> None:
    """R3: exits are risk-reducing — no §7.1 limit, mode, kill or window may block one."""
    verdict = gate.evaluate(_exit(), make_ctx(positions_known=frozenset({"POS1"}), **HOSTILE))
    assert verdict.verdict == "approve"
    assert [c.rule_id for c in verdict.checks] == ["position_known"]
    assert verdict.original_qty is None and verdict.approved_qty is None


def test_exit_on_unknown_position_rejects(gate: RiskGate) -> None:
    verdict = gate.evaluate(_exit(), make_ctx(positions_known=frozenset({"OTHER"})))
    assert verdict.verdict == "reject"
    assert any("unknown position" in r for r in verdict.reasons)


# --------------------------------------------------------------------------- modify-stop
def _modify_stop(new_stop: str) -> ModifyStopAction:
    return ModifyStopAction(
        action="modify-stop", proposal_id="01MSTOP",
        thesis="Trail the stop up behind the 20-period moving average.",
        confidence=0.8, valid_until=NOW + timedelta(minutes=5),
        position_id="POS1", new_stop=Decimal(new_stop),
    )


LONG_POS = {
    "positions_known": frozenset({"POS1"}),
    "position_side": {"POS1": "BUY"},
    "position_stop": {"POS1": Decimal("95")},
    "position_target": {"POS1": Decimal("110")},
}
SHORT_POS = {**LONG_POS, "position_side": {"POS1": "SELL"},
             "position_stop": {"POS1": Decimal("105")},
             "position_target": {"POS1": Decimal("90")}}


@pytest.mark.parametrize(
    ("new_stop", "pos", "expected"),
    [
        ("97", LONG_POS, "approve"),                    # long: raise the stop = tighten
        ("95", LONG_POS, "approve"),                    # unchanged counts as tighten (boundary)
        ("90", LONG_POS, "owner_approval_required"),    # long: lower the stop = widen
        ("103", SHORT_POS, "approve"),                  # short: lower the stop = tighten
        ("108", SHORT_POS, "owner_approval_required"),  # short: raise the stop = widen
    ],
)
def test_modify_stop_direction(gate: RiskGate, new_stop: str, pos: dict, expected: str) -> None:
    verdict = gate.evaluate(_modify_stop(new_stop), make_ctx(**pos))
    assert verdict.verdict == expected


def test_modify_stop_unknown_position_rejects(gate: RiskGate) -> None:
    verdict = gate.evaluate(_modify_stop("97"), make_ctx())
    assert verdict.verdict == "reject"


def test_modify_stop_without_a_known_current_stop_routes_to_owner(gate: RiskGate) -> None:
    """Fail-closed: a change whose direction cannot be verified is never auto-approved."""
    verdict = gate.evaluate(
        _modify_stop("97"),
        make_ctx(positions_known=frozenset({"POS1"}), position_side={"POS1": "BUY"}),
    )
    assert verdict.verdict == "owner_approval_required"


# --------------------------------------------------------------------------- modify-target
def _modify_target(new_target: str | None) -> ModifyTargetAction:
    return ModifyTargetAction(
        action="modify-target", proposal_id="01MTGT",
        thesis="Momentum is fading into resistance; pull the target in.",
        confidence=0.8, valid_until=NOW + timedelta(minutes=5),
        position_id="POS1", new_target=None if new_target is None else Decimal(new_target),
    )


@pytest.mark.parametrize(
    ("new_target", "pos", "expected"),
    [
        ("105", LONG_POS, "approve"),                    # long: pull the target in = tighten
        (None, LONG_POS, "approve"),                     # removing a target is risk-reducing
        ("120", LONG_POS, "owner_approval_required"),    # long: push the target out = extend
        ("95", SHORT_POS, "approve"),                    # short: pull in = raise the target
        ("80", SHORT_POS, "owner_approval_required"),    # short: push out = lower the target
    ],
)
def test_modify_target_direction(gate: RiskGate, new_target, pos: dict, expected: str) -> None:
    verdict = gate.evaluate(_modify_target(new_target), make_ctx(**pos))
    assert verdict.verdict == expected


def test_modify_target_unknown_position_rejects(gate: RiskGate) -> None:
    assert gate.evaluate(_modify_target("105"), make_ctx()).verdict == "reject"


# --------------------------------------------------------------------------- cancel
def _cancel(order_id: str) -> CancelAction:
    return CancelAction(
        action="cancel", proposal_id="01CANCEL",
        thesis="The pending entry no longer has a valid setup behind it.",
        confidence=0.8, valid_until=NOW + timedelta(minutes=5), order_id=order_id,
    )


def test_cancel_of_a_protective_order_is_rejected(gate: RiskGate) -> None:
    """R1/R3: protective orders are NEVER cancellable by proposal — stops move via modify-stop."""
    ctx = make_ctx(
        protective_order_ids=frozenset({"O-SL"}), known_order_ids=frozenset({"O-SL", "O-ENTRY"})
    )
    verdict = gate.evaluate(_cancel("O-SL"), ctx)
    assert verdict.verdict == "reject"
    assert check_of(verdict, "order_not_protective").passed is False
    assert any("not cancellable" in r for r in verdict.reasons)


def test_cancel_of_a_pending_entry_is_approved(gate: RiskGate) -> None:
    ctx = make_ctx(
        protective_order_ids=frozenset({"O-SL"}), known_order_ids=frozenset({"O-SL", "O-ENTRY"})
    )
    assert gate.evaluate(_cancel("O-ENTRY"), ctx).verdict == "approve"


def test_cancel_of_an_unknown_order_is_rejected(gate: RiskGate) -> None:
    verdict = gate.evaluate(_cancel("O-GHOST"), make_ctx(known_order_ids=frozenset({"O-ENTRY"})))
    assert verdict.verdict == "reject"
    assert any("unknown order" in r for r in verdict.reasons)


# --------------------------------------------------------------------------- contracts
def test_gate_verdict_json_round_trips(gate: RiskGate) -> None:
    verdict = gate.evaluate(make_action(), make_ctx())
    restored = GateVerdict.model_validate_json(verdict.model_dump_json())
    assert restored.model_dump_json() == verdict.model_dump_json()
    assert restored.cost is not None
    assert restored.cost.breakeven_pct == verdict.cost.breakeven_pct
    assert len(restored.checks) == len(DOCUMENTED_ENTER_RULES)
    assert restored.evaluated_at == NOW


def test_shrink_verdict_json_round_trips(gate: RiskGate) -> None:
    # O16 2026-09-07: base 40000 / caps 6-2-4 — deployed_capital shifted +20000 to keep Rs300 headroom.
    verdict = gate.evaluate(make_action(), make_ctx(deployed_capital=Decimal("39700")))
    restored = GateVerdict.model_validate_json(verdict.model_dump_json())
    assert restored.verdict == "shrink"
    assert restored.original_qty == 10 and restored.approved_qty == 3


def test_gate_context_is_frozen() -> None:
    ctx = make_ctx()
    with pytest.raises(ValidationError):
        ctx.equity = Decimal("1")           # type: ignore[misc]


# --------------------------------------------------------------------------- §9.1 invariant
FORBIDDEN_SOURCES = ("news", "news_clusters", "sentiment_agg", "catalyst_watchlist")


def test_gate_source_reads_no_origination_tables() -> None:
    """§9.1 / §2.4 item 4: GateContext is assembled from deterministic broker/exchange sources ONLY.

    The §7.1 origination-guard block is enforced UPSTREAM at digest/pre-screen (§3.2.4/§3.2.5) — a
    reference to any of the four media/origination tables inside the gate or its context builder is a
    structural violation, so the module source is scanned for them directly.
    """
    source = Path(gate_module.__file__).read_text(encoding="utf-8")
    # Non-vacuous: this really is the module that carries the gate AND its builder.
    assert "class RiskGate" in source
    assert "class GateContextBuilder" in source
    offenders = [
        token for token in FORBIDDEN_SOURCES if re.search(rf"\b{re.escape(token)}\b", source)
    ]
    assert not offenders, (
        f"engine/risk/gate.py must not reference the origination tables {offenders} "
        "(§9.1 deterministic-context invariant)"
    )


def test_exiting_positions_do_not_occupy_position_or_sector_slots() -> None:
    """O16 (owner-directed 2026-09-07): a position the platform has already told the owner to SELL
    (an exit recommendation delivered within the last 3 days) is EXITING — it must not hold a
    position-count or sector slot against a new BUY. Two through-the-stop CNC positions with 39
    expired exit recommendations had the book at 2/2 for eleven sessions. Still one-per-symbol
    (a BUY on an exiting name stays blocked) and still deployed cash; only the counts change."""
    import json as _json
    from datetime import datetime as _dt

    now = _dt(2026, 9, 7, 12, 0, tzinfo=IST)
    recs = [
        {"payload": _json.dumps({"kind": "exit", "instrument": "HDFCAMC", "created_at": "2026-09-07T10:21:15+05:30"}), "human_action": "expired"},
        {"payload": _json.dumps({"kind": "exit", "instrument": "HINDZINC", "created_at": "2026-09-05T14:43:00+05:30"}), "human_action": None},
        {"payload": _json.dumps({"kind": "exit", "instrument": "OLDONE", "created_at": "2026-08-20T10:00:00+05:30"}), "human_action": "expired"},
        {"payload": _json.dumps({"kind": "entry", "instrument": "RELIANCE", "created_at": "2026-09-07T10:30:00+05:30"}), "human_action": None},
    ]
    open_symbols = frozenset({"HDFCAMC", "HINDZINC", "OLDONE", "RELIANCE"})
    exiting = gate_module._exiting_symbols(recs, open_symbols, now)
    assert exiting == frozenset({"HDFCAMC", "HINDZINC"})      # recent exits only; the entry is not an exit

    positions = [
        {"symbol": "HDFCAMC", "product": "CNC"}, {"symbol": "HINDZINC", "product": "CNC"},
        {"symbol": "OLDONE", "product": "CNC"}, {"symbol": "RELIANCE", "product": "MIS"},
    ]
    sector_of = {"HDFCAMC": "FINANCIAL_SERVICES", "HINDZINC": "METAL", "OLDONE": "METAL", "RELIANCE": "ENERGY"}
    total, mis, cnc, sectors = gate_module._active_counts(
        positions, exiting, sector_of, total=4, mis=1, cnc=3,
        sector_counts={"FINANCIAL_SERVICES": 1, "METAL": 2, "ENERGY": 1},
    )
    assert (total, mis, cnc) == (2, 1, 1)
    assert sectors == {"FINANCIAL_SERVICES": 0, "METAL": 1, "ENERGY": 1}



def test_max_open_positions_ledger_names_the_exiting_exclusion(gate: RiskGate) -> None:
    """The rule reads the already-reduced counts; its ledger line must say how many exiting
    positions were left out, so a verdict is auditable against the raw book (O16)."""
    verdict = gate.evaluate(
        make_action(),
        make_ctx(open_total=2, open_mis=1, exiting_symbols=frozenset({"HDFCAMC", "HINDZINC"})),
    )
    check = check_of(verdict, "max_open_positions")
    assert check.passed
    assert "2 exiting excluded" in check.value


def test_gate_universe_membership_is_the_eligible_set_not_the_focus_watchlist() -> None:
    """O15 (2026-09-04): with the eligible set at ~350–450 NIFTY 500 names and the tick watchlist
    capped at 200, ``universe_daily`` rows excluded for ``watchlist_cap`` ALONE are eligible and must
    clear the gate's ``in_universe`` check — reading ``included_only`` (the pre-O15 shortcut, exact
    only while the cap did not bind) would have rejected every swing candidate from the widened
    tail as out-of-universe. Extended-leg rows (``not_in_index``, legacy ``not_nifty200``) and rows
    with any real exclusion stay OUT: the RECOMMEND boundary is the eligible set by definition."""
    rows = [
        {"symbol": "RELIANCE", "included": True, "exclusion_reasons": None, "mis_candidate": True},
        {"symbol": "NIACL", "included": False, "exclusion_reasons": ["watchlist_cap"], "mis_candidate": False},
        {"symbol": "JINDWORLD", "included": False, "exclusion_reasons": ["not_in_index"], "mis_candidate": True},
        {"symbol": "OLDEXT", "included": False, "exclusion_reasons": ["not_nifty200"], "mis_candidate": True},
        {"symbol": "LOWVAL", "included": False, "exclusion_reasons": ["watchlist_cap", "low_value"], "mis_candidate": True},
    ]
    pick = gate_module._eligible_universe_row
    assert pick(rows, "RELIANCE")["symbol"] == "RELIANCE"
    assert pick(rows, "NIACL")["symbol"] == "NIACL"            # capped, still eligible
    assert pick(rows, "JINDWORLD") is None                      # extended leg: advisory only
    assert pick(rows, "OLDEXT") is None                         # legacy marker, same verdict
    assert pick(rows, "LOWVAL") is None                         # a real exclusion alongside the cap
    assert pick(rows, "ABSENT") is None
