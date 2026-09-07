"""Tier-2 deterministic risk gate (§3.2.7 / §7.1 / §3.4) — the platform's core safety artifact.

Two objects live here, deliberately split so the *decision* is pure and the *assembly* is the only
part that touches I/O:

- :class:`GateContext` — a FROZEN bag of PLAIN VALUES (numbers, flags, sets, maps). Every input any
  §7.1 rule needs, already resolved. Because it holds no handles, :meth:`RiskGate.evaluate` is a pure
  function of ``(action, ctx)`` and the whole limit table is table-testable (§9.1).
- :class:`GateContextBuilder` — assembles that bag from DETERMINISTIC broker/exchange sources ONLY
  (R1): ``InstrumentStore`` (tick/lot, F&O flag), the ``universe_daily`` snapshot, the surveillance
  columns, ``sector_map``, ``earnings_calendar``, ``ExposureTracker``, ``ModeManager`` (incl. the
  owner-set trade window), ``KillSwitch``, ``NSECalendar``, live LTP/feed-age callbacks and the
  margins API. §9.1 pins that the media/origination tables (headline store, cluster store, aggregated
  sentiment, the origination watchlist) are NOT readable from here — the §7.1 ``catalyst_guard`` block
  is stored in the protected table for governance but ENFORCED UPSTREAM in the deterministic
  digest/pre-screen code (§3.2.4/§3.2.5), never inside this gate. A unit test greps this module's
  source for those four table names precisely so the invariant cannot rot.

``engine.risk`` imports NOTHING from ``engine.intelligence`` (R1, AST-enforced by
``tests/unit/test_import_graph.py``): the action union and :class:`GateVerdict` come from
``engine.core.contracts``. Everything the builder only type-hints is imported under
``TYPE_CHECKING`` so importing the gate costs nothing beyond ``engine.core`` + the limit table.

Verdict precedence (§3.2.7 monotone table): ``reject`` > ``owner_approval_required`` > ``shrink`` >
``approve``. The gate may only ever SHRINK an entry — :meth:`RiskGate.evaluate` asserts
``approved_qty <= original_qty`` as a code invariant, and §9.1 asserts it as a property.

SIZING REFERENCE (WO-4, 2026-08-13): a LIMIT proposal's ``entry_price`` is a STATED price, and by
the time the gate runs the market may have moved past it — sizing off it computes risk and notional
from a price better than obtainable. :meth:`RiskGate._sizing_reference` is the ONE point where the
price the §7.1 sizing rules consume is chosen; see its docstring for the rule and for why the
selection can only ever TIGHTEN a cap. The ``entry_sanity_band`` hard-reject is deliberately NOT
routed through it — it band-checks the STATED entry against the live LTP, which is the whole point
of that rule.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Callable, Iterable, Mapping, Sequence
from datetime import date, datetime, time, timedelta
from decimal import ROUND_FLOOR, Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any, Literal, NamedTuple

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field
from ulid import ULID

from engine.core.contracts import (
    CancelAction,
    CheckResult,
    CostBreakdown,
    EnterAction,
    ExitAction,
    GateVerdict,
    ModifyStopAction,
    ModifyTargetAction,
)
from engine.core.enums import Mode, RiskState
from engine.core.log import get_logger
from engine.core.recommendations import parse_valid_until
from engine.core.types import TradeWindow
from engine.risk.limits import LimitTable

if TYPE_CHECKING:  # heavy/optional deps — type-hints only, so importing the gate stays cheap
    from engine.broker.instruments import InstrumentStore
    from engine.core.calendar import NSECalendar
    from engine.core.clock import Clock
    from engine.core.contracts import ActionProposal
    from engine.marketdata.store import MarketStore
    from engine.ops.warmup import WarmupStatus
    from engine.risk.exposure import ExposureTracker
    from engine.risk.kill import KillSwitch
    from engine.risk.limits import LimitsEngine
    from engine.risk.mode import ModeManager
    from engine.strategy.cost_model import CostModel

_log = get_logger("engine.risk.gate")

Verdict = Literal["approve", "shrink", "reject", "owner_approval_required"]

#: EXACTLY the ``rule_id``s :meth:`RiskGate.evaluate` emits for an ``enter`` proposal, in audit order
#: (§3.4: every rule evaluated, pass or fail). §9.1 asserts set-equality against this tuple so adding
#: a limit without a gate check — or a check without a documented id — fails loudly.
DOCUMENTED_ENTER_RULES: tuple[str, ...] = (
    "mode_risk_state",
    "kill_state",
    "proposal_stale",
    "levels_coherent",
    "analyst_confidence_min",
    "trade_window",
    "no_trade_windows",
    "min_residual_window",
    "instrument_eligible",
    "surveillance",
    "capital_cap",
    "per_trade_risk",
    "daily_loss_soft",
    "consecutive_losses",
    "max_new_trades_day",
    "max_open_positions",
    "per_stock_exposure",
    "per_sector_exposure",
    "co_movement_cap",
    "max_leverage",
    "stale_data_guard",
    "warmup_ready",
    "regime_data_ready",
    "clock_skew",
    "entry_sanity_band",
    "circuit_proximity",
    "margin_buffer",
    "order_rate",
    "order_modifications",
    "min_viable_size",
)

#: §7.1 ``rule_id``s that are deliberately NOT evaluated inside the gate, with their enforcement
#: locus. The §9.1 completeness test partitions ``LimitsBlock``'s fields into this set and the §7.1
#: ids the gate emits — so a NEW block in ``limits.yaml`` fails until it is either gate-checked or
#: consciously listed here.
NOT_GATE_ENFORCED_LIMITS: Mapping[str, str] = {
    "daily_loss_hard": "ExposureTracker floor ladder + ModeManager.force_downgrade (§7.1/§3.5.3)",
    "weekly_drawdown": "ExposureTracker.evaluate_floors (§7.1 halt ladder)",
    "equity_floor_rung": "ExposureTracker.evaluate_floors (§7.1 halt ladder)",
    "cumulative_floor": "ExposureTracker.evaluate_floors -> KillSwitch (§7.2)",
    "catalyst_guard": "digest/pre-screen (§3.2.4/§3.2.5) — NEVER in the gate (§2.4 item 4/§9.1)",
    "max_holding": "OMS square-off scheduler + startup age catch-up (§3.2.8/§2.6)",
}

#: Rules whose breach may be cured by SHRINKING the quantity rather than rejecting (§7.1
#: ``on_breach``: ``shrink_else_reject`` / ``reject_or_shrink`` / ``shrink_to_cap_else_reject``).
#: ``per_stock_exposure`` is shrinkable only on its CNC-notional leg — the "1 position per symbol"
#: leg is a hard reject and is classified as such at evaluation time.
#: ``capital_cap`` is the one DELIBERATE addition to the yaml markers (its ``on_breach`` reads
#: ``reject_entry``): sizing down to the deployed-capital headroom is strictly safer than rejecting,
#: and the shrink loop re-runs ``min_viable_size`` at the reduced size, so a shrink that stops
#: clearing costs still ends in reject (R1/C3).
SHRINKABLE_RULES: frozenset[str] = frozenset(
    {"capital_cap", "per_trade_risk", "per_stock_exposure", "max_leverage"}
)

#: Sentinel for the "no target" reject (C3): the analyst MUST emit a target or the edge is
#: unverifiable and the trade cannot be shown to clear costs — UNLESS the strategy carries a
#: pre-registered measured edge (see :data:`_NO_TARGET_MEASURED_EDGE` and ``RiskGate``'s
#: ``strategy_expected_edge_pct``).
_NO_TARGET = "no target_price — expected edge unverifiable (C3)"

#: The narrow, deliberate exception to :data:`_NO_TARGET`, added 2026-08-17 for the §6.1 ``ins`` leg.
#:
#: The C3 check needs ONE number: the expected favourable move, as a percentage. For every strategy
#: shipped before ``ins`` that number is derived from the proposal's own target — a target is the
#: strategy's own statement of where it expects price to go, so deriving the edge from it is honest.
#: ``ins`` has no target BY DESIGN: its exit is TIME (the §7.1 20-td swing cap = the validated T+20
#: horizon), and the drift it captures was measured, not predicted. Inventing a target purely to feed
#: this check would push a fabricated price into the gate's arithmetic and into the owner's payload —
#: strictly worse than using the measured number directly.
#:
#: So a strategy MAY register a pre-measured expected edge, and it is used ONLY when the proposal has
#: no target. Guard-rails that keep this from becoming a bypass:
#:   * It is a per-strategy OWNER setting (``settings.yaml`` ``ins.expected_edge_pct``), never
#:     learner-movable and never model-supplied — Tier 1 cannot hand itself an edge.
#:   * It does not weaken the check: ``edge_multiple = edge / breakeven >= edge_multiple_min`` is
#:     evaluated exactly as before, at the post-shrink size, against the same shipped cost surface.
#:   * A strategy with NO registered edge and no target still hard-fails, unchanged.
_NO_TARGET_MEASURED_EDGE = "no target_price — using the strategy's pre-registered measured edge (C3)"

#: Sentinel for the SHADOW reject (C3): the strategy is registered in ``no_edge_shadow_strategies``,
#: so C3 refuses it unconditionally — whatever ``target_price`` the proposal carries.
#:
#: WHY THIS EXISTS (2026-08-27, shipped with §2.7 ``cat_reversal``). A shadow strategy accumulates a
#: validation population and must reach RECOMMEND exactly never until its §8.6 owner gate. The
#: mechanism relied on until now was INDIRECT: ship no ``expected_edge_pct``, and let the
#: :data:`_NO_TARGET` branch below reject every candidate. That reasoning has a hole — the branch
#: fires only when ``target_price is None``, but the ANALYST emits the ``EnterAction`` and the wire
#: schema (``intelligence.schemas``) lets it supply a target; nothing between the candidate and this
#: gate reconciles that field against the candidate's ``raw_levels.target``. An analyst that
#: volunteers a target hands C3 a real edge basis and the shadow's defining property evaporates on a
#: model whim.
#:
#: So the property is declared here instead of inferred: a registered id is rejected BEFORE the target
#: is even read. It is Tier-2 owned, fails closed by construction, can only ever REJECT (it adds no
#: path to approval), and no prompt, model output or upstream layer can bypass it. Promoting a
#: strategy out of the shadow means removing it from this set at the §8.6 gate — a deliberate,
#: reviewable edit, which is exactly the ceremony the promotion deserves.
_SHADOW_NO_EDGE = "shadow strategy — no validated edge exists; C3 cannot be satisfied at any target"

_HUNDRED = Decimal(100)
_UNCLASSIFIED = "UNCLASSIFIED"

#: ``orders.role`` values that are PROTECTIVE (R1/R3): never cancellable by a Tier-1 proposal.
PROTECTIVE_ORDER_ROLES: frozenset[str] = frozenset({"protective_sl", "target", "gtt_leg"})


# --------------------------------------------------------------------------- small helpers
def _dec(value: Any) -> Decimal:
    """Any scalar -> exact Decimal. Floats go through ``str()`` so a yaml/limit float like ``0.7``
    never contributes a binary-float artifact to a comparison."""
    if isinstance(value, Decimal):
        return value
    if value is None:
        return Decimal(0)
    return Decimal(str(value))


def _mins(t: time) -> Decimal:
    """Minutes-since-midnight of a wall-clock time (window arithmetic without a date)."""
    return Decimal(t.hour * 60 + t.minute) + Decimal(t.second) / 60


def _floor_div(numerator: Decimal, denominator: Decimal) -> int:
    """``floor(numerator / denominator)`` clamped at 0 — the implied max qty of a linear constraint.
    A non-positive denominator (unpriceable unit) yields 0: fail-closed, never "unbounded"."""
    if denominator <= 0 or numerator <= 0:
        return 0
    return int((numerator / denominator).to_integral_value(rounding=ROUND_FLOOR))


def _q(value: Decimal, places: str = "0.01") -> str:
    """Money/percent rendered for a human-readable CheckResult field."""
    try:
        return str(value.quantize(Decimal(places)))
    except (InvalidOperation, ValueError):
        return str(value)


class SizingReference(NamedTuple):
    """The prices the §7.1 sizing rules size off, chosen at ONE point (:meth:`RiskGate._sizing_reference`).

    Two fields rather than one because the two families of priced rules bind in OPPOSITE price
    directions, and a single scalar cannot be conservative for both on a SHORT:

    ``risk``
        Basis for the stop-distance and edge rules (``per_trade_risk``, ``min_viable_size``). A cap
        derived from it shrinks as the basis moves AWAY from the stop.
    ``notional``
        Basis for the capital/exposure/leverage/margin rules (``capital_cap``,
        ``per_stock_exposure``, ``max_leverage``, ``margin_buffer``). A cap derived from it shrinks
        as the basis moves UP.
    """

    risk: Decimal
    notional: Decimal


# --------------------------------------------------------------------------- GateContext
class GateContext(BaseModel):
    """Every input the §7.1 rules need, resolved to PLAIN VALUES (§3.2.7).

    Frozen and handle-free on purpose: :meth:`RiskGate.evaluate` must be a pure function so the same
    ``(action, ctx)`` always produces the same :class:`GateVerdict` (replay determinism, §9.1) and so
    each rule can be exercised by flipping ONE field of a fully-passing baseline.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    # -- clock / control plane -------------------------------------------------------------
    now: AwareDatetime                      # tz-aware IST, from Clock (§3.2) — never datetime.now()
    mode: Mode
    risk_state: RiskState
    killed: bool = False
    degrade_tier: str = "DG0"

    # -- session + owner-set window (§7.1 trade_window) ------------------------------------
    trade_window: TradeWindow | None = None
    session_open: time | None = None
    session_close: time | None = None

    # -- equity + day-scoped counters (ExposureTracker) ------------------------------------
    equity: Decimal
    day_mtm_pct: Decimal                    # percent of the capital base (−5.0 == −5%, §7.1)
    consecutive_losses: int = 0
    entry_recs_today: int = 0               # Phase-2 max_new_trades_day basis (see the rule)

    # -- open exposure ---------------------------------------------------------------------
    open_total: int = 0
    open_mis: int = 0
    open_cnc: int = 0
    #: Symbols of OPEN positions the platform has already told the owner to SELL (an exit
    #: recommendation delivered within :data:`_EXITING_LOOKBACK_DAYS`) — EXCLUDED from
    #: ``open_total``/``open_mis``/``open_cnc`` and ``open_sector_counts`` by the builder (O16,
    #: owner-directed 2026-09-07). Still members of ``open_symbols`` (one position per symbol).
    exiting_symbols: frozenset[str] = frozenset()
    open_symbols: frozenset[str] = frozenset()
    pending_rec_symbols: frozenset[str] = frozenset()   # unexpired, unconfirmed entry recs
    per_symbol_cnc_notional: Mapping[str, Decimal] = Field(default_factory=dict)
    sector_of: Mapping[str, str] = Field(default_factory=dict)
    open_sector_counts: Mapping[str, int] = Field(default_factory=dict)
    #: Max 20d daily-return Pearson correlation of the candidate vs each OPEN position. ``None`` =
    #: no open positions OR insufficient history — the rule then PASSES with an explicit note:
    #: absence of data must never silently reject (R1 determinism over guesswork).
    max_corr_with_open: Decimal | None = None
    deployed_capital: Decimal = Decimal(0)

    # -- live market / feed health ---------------------------------------------------------
    ltp: Decimal | None = None
    tick_age_s: float | None = None
    index_tick_age_s: float | None = None

    # -- instrument eligibility ------------------------------------------------------------
    in_universe: bool = False
    mis_candidate: bool = False
    surveillance_flag: str | None = None
    is_fno: bool = False
    results_day_today: bool = False
    expiry_day: bool = False
    is_nifty50: bool = False

    # -- readiness gates (fail-closed defaults) --------------------------------------------
    warmup_ready: bool = False
    regime_ready: bool = False
    clock_skew_ok: bool = False

    # -- broker margins --------------------------------------------------------------------
    available_margin: Decimal | None = None     # None ⇒ RECOMMEND: no API order, rule is n/a

    # -- references for exit / modify / cancel ---------------------------------------------
    positions_known: frozenset[str] = frozenset()
    protective_order_ids: frozenset[str] = frozenset()
    known_order_ids: frozenset[str] = frozenset()
    position_side: Mapping[str, str] = Field(default_factory=dict)      # position_id -> BUY/SELL
    position_stop: Mapping[str, Decimal] = Field(default_factory=dict)
    position_target: Mapping[str, Decimal | None] = Field(default_factory=dict)


# --------------------------------------------------------------------------- evaluation scratchpad
class _Ledger:
    """Accumulates :class:`CheckResult`s plus the reject/shrink classification of each failure."""

    def __init__(self) -> None:
        self.checks: dict[str, CheckResult] = {}
        self.hard_reasons: list[str] = []       # force ``reject``
        self.shrink_reasons: list[str] = []     # bound the qty only
        self.caps: dict[str, int] = {}          # rule_id -> implied max qty

    def add(
        self,
        rule_id: str,
        passed: bool,
        value: str,
        limit: str,
        headroom: str,
        *,
        shrinkable: bool = False,
    ) -> None:
        self.checks[rule_id] = CheckResult(
            rule_id=rule_id, passed=passed, value=value, limit=limit, headroom=headroom
        )
        if not passed:
            summary = f"{rule_id}: {value} (limit {limit})"
            (self.shrink_reasons if shrinkable else self.hard_reasons).append(summary)

    def cap(self, rule_id: str, qty_max: int) -> None:
        self.caps[rule_id] = qty_max


# --------------------------------------------------------------------------- the gate
class RiskGate:
    """The Tier-2 deterministic gate (§3.2.7). :meth:`evaluate` is PURE — no I/O, no clock reads
    beyond the verdict stamp, no network. Every limit value comes from :class:`LimitTable`; nothing
    here is hardcoded (R4)."""

    def __init__(
        self,
        limits_engine: LimitsEngine,
        cost_model: CostModel,
        clock: Clock,
        *,
        strategy_expected_edge_pct: Mapping[str, Decimal] | None = None,
        no_edge_shadow_strategies: Iterable[str] | None = None,
    ) -> None:
        """``strategy_expected_edge_pct`` maps ``strategy_id`` -> a pre-registered, owner-set expected
        edge in percent, consumed by ``min_viable_size`` ONLY for a proposal with no ``target_price``
        (see :data:`_NO_TARGET_MEASURED_EDGE`). Unset ⇒ the pre-2026-08-17 behaviour exactly: a
        targetless entry is a hard C3 reject.

        ``no_edge_shadow_strategies`` lists ``strategy_id``s in SHADOW mode: C3 rejects them
        unconditionally, whatever target the proposal carries (see :data:`_SHADOW_NO_EDGE`). Unset ⇒
        behaviour is byte-identical to before this parameter existed. An id in BOTH mappings is a
        deployment defect and the shadow wins — refusing is the safe reading of a contradiction."""
        self._limits = limits_engine
        self._costs = cost_model
        self._clock = clock
        self._strategy_edge = dict(strategy_expected_edge_pct or {})
        self._shadow_no_edge = frozenset(no_edge_shadow_strategies or ())

    # ------------------------------------------------------------------ dispatch
    def evaluate(self, action: ActionProposal, ctx: GateContext) -> GateVerdict:
        """Evaluate ``action`` against ``ctx`` and return the full-audit :class:`GateVerdict` (§3.4)."""
        if isinstance(action, EnterAction):
            return self._evaluate_enter(action, ctx)
        if isinstance(action, ExitAction):
            return self._evaluate_exit(action, ctx)
        if isinstance(action, ModifyStopAction):
            return self._evaluate_modify_stop(action, ctx)
        if isinstance(action, ModifyTargetAction):
            return self._evaluate_modify_target(action, ctx)
        if isinstance(action, CancelAction):
            return self._evaluate_cancel(action, ctx)
        raise TypeError(f"unsupported action type {type(action).__name__}")   # pragma: no cover

    # ------------------------------------------------------------------ enter (§7.1)
    def _evaluate_enter(self, action: EnterAction, ctx: GateContext) -> GateVerdict:
        table = self._limits.load()
        lim = table.limits
        intraday = action.style == "intraday"
        product: Literal["MIS", "CNC"] = "MIS" if intraday else "CNC"
        now_t = ctx.now.time()
        qty = int(action.quantity)
        led = _Ledger()

        # STATED entry reference: the LIMIT price when given, else the live LTP. A MARKET proposal
        # with no LTP is UNPRICEABLE — every price-derived rule then fails closed rather than
        # guessing. This is the price the PROPOSAL asserts; `levels_coherent` judges that shape and
        # `entry_sanity_band` band-checks it against the live LTP.
        entry_ref: Decimal | None = action.entry_price if action.entry_type == "LIMIT" else ctx.ltp
        if entry_ref is not None and entry_ref <= 0:
            entry_ref = None
        # ...and the ONE point where the price the SIZING rules consume is chosen (WO-4).
        sizing = self._sizing_reference(action, ctx, entry_ref)
        risk_ref = sizing.risk if sizing is not None else None
        notional_ref = sizing.notional if sizing is not None else None

        self._rule_mode_risk_state(led, ctx)
        self._rule_kill_state(led, ctx)
        self._rule_proposal_stale(led, action, ctx)
        self._rule_levels_coherent(led, action, entry_ref)
        self._rule_analyst_confidence(led, action, table)
        self._rule_trade_window(led, ctx, now_t, intraday)
        self._rule_no_trade_windows(led, ctx, lim, now_t, intraday)
        self._rule_min_residual_window(led, ctx, lim, now_t, intraday)
        self._rule_instrument_eligible(led, ctx, intraday)
        self._rule_surveillance(led, ctx)
        self._rule_capital_cap(led, ctx, lim, notional_ref, qty, product)
        self._rule_per_trade_risk(led, action, ctx, lim, risk_ref, qty, intraday)
        self._rule_daily_loss_soft(led, ctx, lim)
        self._rule_consecutive_losses(led, ctx, lim)
        self._rule_max_new_trades(led, ctx, lim)
        self._rule_max_open_positions(led, ctx, lim, intraday)
        self._rule_per_stock_exposure(led, action, ctx, lim, notional_ref, qty, product)
        self._rule_per_sector_exposure(led, action, ctx, lim)
        self._rule_co_movement(led, ctx, lim)
        self._rule_max_leverage(led, ctx, lim, notional_ref, qty, intraday)
        self._rule_stale_data(led, ctx, lim)
        self._rule_readiness(led, ctx)
        self._rule_entry_sanity_band(led, action, ctx, lim, intraday)
        self._rule_circuit_proximity(led, lim)
        self._rule_order_surface(led)

        # ---- SHRINK LOOP (R1): the gate may only ever reduce the quantity ----------------
        qty_max = min(led.caps.values()) if led.caps else qty
        approved = min(qty, qty_max)
        if approved < 0:
            approved = 0

        # margin + min-viable-size are evaluated on the FINAL size: they are statements about the
        # order that would actually be placed, not about the proposal's opening ask.
        self._rule_margin_buffer(led, ctx, lim, notional_ref, approved, product)
        cost = self._rule_min_viable_size(led, action, ctx, risk_ref, approved, product)

        checks = [led.checks[rule_id] for rule_id in DOCUMENTED_ENTER_RULES]
        assert len(checks) == len(led.checks), (          # noqa: S101 - developer invariant (§9.1)
            f"gate emitted undocumented rule_ids: {sorted(set(led.checks) - set(DOCUMENTED_ENTER_RULES))}"
        )

        reasons = list(led.hard_reasons) + list(led.shrink_reasons)
        if led.hard_reasons or approved <= 0:
            verdict: Verdict = "reject"
            if approved <= 0 and not led.hard_reasons:
                reasons.append(f"shrink floor: implied max qty {qty_max} — nothing approvable")
            approved = 0
        elif approved < qty:
            verdict = "shrink"
            binding = [r for r, c in led.caps.items() if c == qty_max]
            reasons.append(f"shrink: qty {qty} -> {approved} (bound by {', '.join(sorted(binding))})")
        else:
            verdict = "approve"

        # The gate NEVER enlarges (R1). Code invariant; §9.1 also asserts it as a property.
        assert approved <= qty, "RiskGate must never enlarge a proposal"    # noqa: S101

        return self._verdict(
            action, ctx, verdict, checks, reasons, original_qty=qty, approved_qty=approved, cost=cost
        )

    # -- sizing-reference selection (WO-4) ----------------------------------------------------
    @staticmethod
    def _sizing_reference(
        action: EnterAction, ctx: GateContext, entry_ref: Decimal | None
    ) -> SizingReference | None:
        """THE single point at which the price the §7.1 sizing rules size off is chosen (WO-4).

        A LIMIT proposal states a price; the market does not owe it to us. When the live LTP has run
        PAST the stated entry, sizing off ``entry_price`` computes both the risk-at-stop and the
        deployed notional from a price better than obtainable — a size the owner could not actually
        put on at the risk the gate thinks it approved. So::

            risk basis     = max(entry_price, LTP) when BUY, min(entry_price, LTP) when SELL
            notional basis = max(entry_price, LTP) on BOTH sides

        Both are the ADVERSE choice for the rule family that consumes them, which is why the
        selection is MONOTONE-SAFE — it can shrink an approved size, never enlarge one:

        * ``risk``: with coherent levels (``levels_coherent``: BUY ``stop < entry``, SELL
          ``stop > entry``) the adverse fill always lies on the far side of the stated entry FROM
          the stop, so ``|risk − stop| >= |entry_price − stop|``: ``per_trade_risk``'s implied cap
          can only fall and ``min_viable_size``'s edge can only narrow. With INcoherent levels the
          inequality need not hold — but ``levels_coherent`` is a hard reject, so ``approved_qty``
          is 0 there regardless and the composite property survives.
        * ``notional``: ``max`` on both sides, i.e. ``>= entry_price`` unconditionally, so every
          per-unit divisor in ``capital_cap`` / ``per_stock_exposure`` / ``max_leverage`` /
          ``margin_buffer`` can only rise and their caps can only fall. Taking ``min`` on the SHORT
          side here would be arithmetically defensible (a fill at 90 really does deploy 90/unit) but
          would LOOSEN those caps, and a gate change that can enlarge a size is not one this file
          accepts — the conservative choice is the only one compatible with R1.

        Unchanged paths: MARKET proposals (``entry_ref`` is already the live LTP, so both bases are
        it), a LIMIT with no usable LTP (nothing better than the stated price is known — and
        ``entry_sanity_band`` fails that proposal closed anyway), and the unpriceable case
        (``None`` in, ``None`` out, every priced rule fails closed as before).
        """
        if entry_ref is None:
            return None
        ltp = ctx.ltp if (ctx.ltp is not None and ctx.ltp > 0) else None
        if action.entry_type != "LIMIT" or ltp is None:
            return SizingReference(risk=entry_ref, notional=entry_ref)
        adverse = max(entry_ref, ltp) if action.side == "BUY" else min(entry_ref, ltp)
        return SizingReference(risk=adverse, notional=max(entry_ref, ltp))

    # -- individual enter rules ---------------------------------------------------------------
    def _rule_mode_risk_state(self, led: _Ledger, ctx: GateContext) -> None:
        ok = ctx.mode in (Mode.AUTO, Mode.RECOMMEND) and ctx.risk_state == RiskState.NORMAL
        led.add(
            "mode_risk_state", ok,
            f"mode={ctx.mode.value} risk_state={ctx.risk_state.value}",
            "mode in (AUTO, RECOMMEND) and risk_state == NORMAL",
            "entries permitted" if ok else "entries blocked by the mode/risk-state machine (§3.5.3)",
        )

    def _rule_kill_state(self, led: _Ledger, ctx: GateContext) -> None:
        led.add(
            "kill_state", not ctx.killed,
            "KILLED" if ctx.killed else "not killed",
            "kill switch not engaged (R10)",
            "owner two-step reset required" if ctx.killed else "clear",
        )

    def _rule_proposal_stale(self, led: _Ledger, action: EnterAction, ctx: GateContext) -> None:
        vu = action.valid_until
        ok = vu is not None and vu > ctx.now
        led.add(
            "proposal_stale", ok,
            f"valid_until={vu.isoformat() if vu else 'unstamped'} now={ctx.now.isoformat()}",
            "valid_until > now",
            "fresh" if ok else "expired/unstamped proposal — fail closed",
        )

    def _rule_levels_coherent(
        self, led: _Ledger, action: EnterAction, entry_ref: Decimal | None
    ) -> None:
        """Stop/target must agree with ``action.side`` (2026-07-28 review: the edge/risk math infers
        direction from ``stop < entry``, so a BUY with the stop ABOVE entry was silently scored as a
        healthy short — and its 'protective' SL-M would sit above the market). BUY: stop < entry <
        target; SELL mirrored. With no usable entry reference, orientation of stop vs target alone."""
        long_side = action.side == "BUY"
        stop, target = action.stop_price, action.target_price
        problems: list[str] = []
        if entry_ref is not None:
            if (stop >= entry_ref) if long_side else (stop <= entry_ref):
                problems.append(f"stop {stop} on the wrong side of entry {_q(entry_ref)}")
            if target is not None and ((target <= entry_ref) if long_side else (target >= entry_ref)):
                problems.append(f"target {target} on the wrong side of entry {_q(entry_ref)}")
        elif target is not None and ((stop >= target) if long_side else (stop <= target)):
            problems.append(f"stop {stop} vs target {target} inverted for {action.side}")
        ok = not problems
        led.add(
            "levels_coherent", ok,
            "; ".join(problems) if problems else f"{action.side}: stop {stop}, target {target}",
            "BUY: stop < entry < target; SELL mirrored (direction from side, never inferred)",
            "coherent" if ok else "inverted levels — the stop cannot protect this side",
        )

    def _rule_analyst_confidence(self, led: _Ledger, action: EnterAction, table: LimitTable) -> None:
        floor = table.analyst_confidence_min
        ok = action.confidence >= floor
        led.add(
            "analyst_confidence_min", ok,
            f"confidence={action.confidence}",
            f">= {floor} (owner-only, §6.3)",
            f"{action.confidence - floor:+.3f}",
        )

    def _rule_trade_window(
        self, led: _Ledger, ctx: GateContext, now_t: time, intraday: bool
    ) -> None:
        w = ctx.trade_window
        if w is None:
            # Startup validation should have frozen a window (§3.2.12); with none set we fail closed.
            led.add("trade_window", False, "no owner-set trade window", "start <= now <= end",
                    "fail closed — startup validation should have seeded the window")
            return
        ok = w.start <= now_t <= w.end
        cutoff = w.mis_entry_cutoff()
        if intraday:
            ok = ok and now_t <= cutoff
        limit = f"{w.start}-{w.end}" + (f" (MIS entries cut off {cutoff})" if intraday else "")
        led.add(
            "trade_window", ok, f"now={now_t}", limit,
            "inside the owner-set window" if ok else "outside the owner-set window (R3: exits unaffected)",
        )

    def _rule_no_trade_windows(
        self, led: _Ledger, ctx: GateContext, lim: Any, now_t: time, intraday: bool
    ) -> None:
        nt = lim.no_trade_windows
        start, end = (
            (nt.mis_entry_start, nt.mis_entry_end) if intraday
            else (nt.cnc_entry_start, nt.cnc_entry_end)
        )
        ok = start <= now_t <= end
        notes: list[str] = [f"now={now_t}"]
        if nt.no_entry_on_results_day and ctx.results_day_today:
            ok = False
            notes.append("results day T (T+1 remains legal, O13)")
        expiry_cut = nt.nifty50_no_new_mis_after_on_expiry
        if ctx.expiry_day and ctx.is_nifty50 and intraday and now_t > expiry_cut:
            ok = False
            notes.append(f"expiry-day NIFTY50 MIS after {expiry_cut}")
        led.add(
            "no_trade_windows", ok, "; ".join(notes),
            f"{'MIS' if intraday else 'CNC'} {start}-{end}; no results-day entry; "
            f"expiry NIFTY50 MIS cutoff {expiry_cut}",
            "inside" if ok else "blocked by a static no-trade window",
        )

    def _rule_min_residual_window(
        self, led: _Ledger, ctx: GateContext, lim: Any, now_t: time, intraday: bool
    ) -> None:
        need = lim.min_residual_window.min_hold_min
        if not intraday:
            led.add("min_residual_window", True, "n/a (CNC — held across windows)",
                    f">= {need} min for MIS", "not applicable")
            return
        if ctx.trade_window is None:
            led.add("min_residual_window", False, "no trade window", f">= {need} min",
                    "fail closed")
            return
        cutoff = ctx.trade_window.mis_entry_cutoff()
        remaining = _mins(cutoff) - _mins(now_t)
        ok = remaining >= Decimal(need)
        led.add(
            "min_residual_window", ok,
            f"{_q(remaining)} min to MIS cutoff {cutoff}",
            f">= {need} min (C3: a full round trip for a near-zero hold)",
            f"{_q(remaining - Decimal(need))} min",
        )

    def _rule_instrument_eligible(self, led: _Ledger, ctx: GateContext, intraday: bool) -> None:
        ok = ctx.in_universe
        parts = [f"in_universe={ctx.in_universe}"]
        if intraday:
            parts.append(f"mis_candidate={ctx.mis_candidate} is_fno={ctx.is_fno}")
            ok = ok and ctx.mis_candidate and ctx.is_fno
        led.add(
            "instrument_eligible", ok, " ".join(parts),
            "in today's universe; MIS also requires mis_candidate + F&O list (C7 dynamic band)",
            "eligible" if ok else "ineligible instrument (A8/C7)",
        )

    def _rule_surveillance(self, led: _Ledger, ctx: GateContext) -> None:
        flag = (ctx.surveillance_flag or "").strip()
        ok = not flag
        led.add(
            "surveillance", ok, flag or "none",
            "no GSM/ASM/T2T/ESM flag (A8)",
            "clear" if ok else f"flagged: {flag}",
        )

    def _rule_capital_cap(
        self, led: _Ledger, ctx: GateContext, lim: Any, notional_ref: Decimal | None,
        qty: int, product: str,
    ) -> None:
        cap = _dec(lim.capital_cap.max_deployed_capital_inr)
        headroom = cap - ctx.deployed_capital
        if notional_ref is None:
            led.cap("capital_cap", 0)
            led.add("capital_cap", False, "unpriceable (MARKET with no LTP)",
                    f"deployed <= {_q(cap)}", "fail closed", shrinkable=True)
            return
        # Phase 2 charges MIS at FULL notional on BOTH sides of the inequality — matching
        # ExposureTracker.deployed_capital(), which has no per-stock leverage until Phase 3
        # (2026-07-28 review: charging the NEW leg at notional/3 against open legs at 1x notional
        # made one MIS fill consume the whole cap). Conservative: never under-charges deployment.
        per_unit = notional_ref                      # WO-4 notional basis (see _sizing_reference)
        qty_max = _floor_div(headroom, per_unit)
        led.cap("capital_cap", qty_max)
        new_deployed = Decimal(qty) * per_unit
        ok = ctx.deployed_capital + new_deployed <= cap
        basis = (
            f"@ {_q(per_unit)} " + ("notional (CNC cash)" if product == "CNC" else (
                "FULL notional — per-stock MIS margin accounting lands with the Phase-3 OMS"
            ))
        )
        led.add(
            "capital_cap", ok,
            f"deployed {_q(ctx.deployed_capital)} + new {_q(new_deployed)} [{basis}]",
            f"<= {_q(cap)} (O1)",
            f"{_q(headroom)} INR ⇒ max qty {qty_max}",
            shrinkable=True,
        )

    def _rule_per_trade_risk(
        self, led: _Ledger, action: EnterAction, ctx: GateContext, lim: Any,
        risk_ref: Decimal | None, qty: int, intraday: bool,
    ) -> None:
        ptr = lim.per_trade_risk
        pct = _dec(ptr.intraday_pct if intraday else ptr.swing_position_pct)
        budget = pct / _HUNDRED * ctx.equity
        if risk_ref is None:
            led.cap("per_trade_risk", 0)
            led.add("per_trade_risk", False, "unpriceable (MARKET with no LTP)",
                    f"qty x unit risk <= {_q(budget)}", "fail closed — unpriceable risk",
                    shrinkable=True)
            return
        # WO-4: distance measured from the ADVERSE fill reference, not from the stated entry.
        unit_risk = abs(risk_ref - action.stop_price)
        if intraday:
            unit, basis = unit_risk, f"stop distance from {_q(risk_ref)}"
        else:
            gap = _dec(ptr.overnight_gap_mult)
            unit = gap * unit_risk
            basis = (f"{gap}x stop distance from {_q(risk_ref)} "
                     "(daily-band leg unavailable in Phase 2 — gap mult only)")
        qty_max = _floor_div(budget, unit)
        led.cap("per_trade_risk", qty_max)
        at_risk = Decimal(qty) * unit
        ok = unit > 0 and at_risk <= budget
        led.add(
            "per_trade_risk", ok,
            f"{qty} x {_q(unit)} = {_q(at_risk)} [{basis}]",
            f"<= {pct}% of equity {_q(ctx.equity)} = {_q(budget)}",
            f"{_q(budget - at_risk)} INR ⇒ max qty {qty_max}",
            shrinkable=True,
        )

    def _rule_daily_loss_soft(self, led: _Ledger, ctx: GateContext, lim: Any) -> None:
        floor = _dec(lim.daily_loss_soft.day_mtm_pct)
        ok = ctx.day_mtm_pct > floor
        led.add(
            "daily_loss_soft", ok, f"day MTM {_q(ctx.day_mtm_pct, '0.001')}%",
            f"> {floor}% (at-or-below ⇒ FREEZE for the rest of the day)",
            f"{_q(ctx.day_mtm_pct - floor, '0.001')} pp",
        )

    def _rule_consecutive_losses(self, led: _Ledger, ctx: GateContext, lim: Any) -> None:
        cap = lim.consecutive_losses.max_per_session
        ok = ctx.consecutive_losses < cap
        led.add(
            "consecutive_losses", ok, str(ctx.consecutive_losses),
            f"< {cap} in a session", f"{cap - ctx.consecutive_losses} left",
        )

    def _rule_max_new_trades(self, led: _Ledger, ctx: GateContext, lim: Any) -> None:
        cap = lim.max_new_trades_day.count
        ok = ctx.entry_recs_today < cap
        led.add(
            "max_new_trades_day", ok, str(ctx.entry_recs_today),
            # Phase-2 semantics: in RECOMMEND no position is opened, so the churn cap counts ENTRY
            # RECOMMENDATIONS issued today (§3.6) rather than positions opened.
            f"< {cap} entry recommendations/day (C3 churn protection)",
            f"{cap - ctx.entry_recs_today} left",
        )

    def _rule_max_open_positions(
        self, led: _Ledger, ctx: GateContext, lim: Any, intraday: bool
    ) -> None:
        mop = lim.max_open_positions
        # CONSERVATIVE: unexpired, unconfirmed entry recommendations already occupy slots — the owner
        # may act on any of them. They have no product on the context, so they are charged against
        # BOTH the total and the style-specific leg.
        pending = len(ctx.pending_rec_symbols)
        total_after = ctx.open_total + pending + 1
        leg_open = ctx.open_mis if intraday else ctx.open_cnc
        leg_cap = mop.max_mis if intraday else mop.max_cnc
        leg_after = leg_open + pending + 1
        ok = total_after <= mop.total and leg_after <= leg_cap
        exiting = len(ctx.exiting_symbols)
        led.add(
            "max_open_positions", ok,
            f"total {ctx.open_total}+{pending} pending+1 = {total_after}; "
            f"{'MIS' if intraday else 'CNC'} {leg_open}+{pending}+1 = {leg_after}"
            + (f"; {exiting} exiting excluded (O16)" if exiting else ""),
            f"total <= {mop.total}; MIS <= {mop.max_mis}; CNC <= {mop.max_cnc}",
            f"{mop.total - total_after} total / {leg_cap - leg_after} leg",
        )

    def _rule_per_stock_exposure(
        self, led: _Ledger, action: EnterAction, ctx: GateContext, lim: Any,
        notional_ref: Decimal | None, qty: int, product: str,
    ) -> None:
        pse = lim.per_stock_exposure
        sym = action.tradingsymbol
        held = sym in ctx.open_symbols or sym in ctx.pending_rec_symbols
        cap = _dec(pse.cnc_notional_inr)
        existing = _dec(ctx.per_symbol_cnc_notional.get(sym, Decimal(0)))
        notes = [f"held={held}"]
        ok_notional = True
        if product == "CNC":
            if notional_ref is None:
                led.cap("per_stock_exposure", 0)
                ok_notional = False
                notes.append("unpriceable CNC notional")
            else:
                qty_max = _floor_div(cap - existing, notional_ref)
                led.cap("per_stock_exposure", qty_max)
                new_notional = Decimal(qty) * notional_ref
                ok_notional = existing + new_notional <= cap
                notes.append(
                    f"CNC notional {_q(existing)} + {_q(new_notional)} @ {_q(notional_ref)}"
                )
        ok = (not held) and ok_notional
        if held:
            # The one-position-per-symbol leg can NOT be cured by shrinking — hard reject.
            led.hard_reasons.append(f"per_stock_exposure: {sym} already held or pending")
        led.checks["per_stock_exposure"] = CheckResult(
            rule_id="per_stock_exposure", passed=ok, value="; ".join(notes),
            limit=f"{pse.max_positions_per_symbol} position/symbol; CNC notional <= {_q(cap)} (C4)",
            headroom="clear" if ok else "per-symbol exposure breached",
        )
        if not ok and not held:
            led.shrink_reasons.append(
                f"per_stock_exposure: {'; '.join(notes)} (limit CNC notional <= {_q(cap)})"
            )

    def _rule_per_sector_exposure(
        self, led: _Ledger, action: EnterAction, ctx: GateContext, lim: Any
    ) -> None:
        pse = lim.per_sector_exposure
        sector = ctx.sector_of.get(action.tradingsymbol) or _UNCLASSIFIED
        cap = pse.unclassified_cap if sector == _UNCLASSIFIED else pse.max_positions_per_sector
        count = int(ctx.open_sector_counts.get(sector, 0))
        ok = count < cap
        led.add(
            "per_sector_exposure", ok, f"{sector}: {count} open",
            f"< {cap} ({_UNCLASSIFIED} cap {pse.unclassified_cap}; sector_map, §4.4)",
            f"{cap - count} slot(s)",
        )

    def _rule_co_movement(self, led: _Ledger, ctx: GateContext, lim: Any) -> None:
        corr_max = _dec(lim.co_movement_cap.corr_max)
        corr = ctx.max_corr_with_open
        ok = corr is None or corr <= corr_max
        value = (
            "n/a (no open positions or insufficient 20d history — absence of data never rejects)"
            if corr is None else f"max 20d return corr {_q(corr, '0.0001')}"
        )
        led.add(
            "co_movement_cap", ok, value, f"<= {corr_max} (R1 concentration guard)",
            "clear" if ok else "correlated with an open position",
        )

    def _rule_max_leverage(
        self, led: _Ledger, ctx: GateContext, lim: Any, notional_ref: Decimal | None,
        qty: int, intraday: bool,
    ) -> None:
        capx = _dec(lim.max_leverage.platform_cap_x) if intraday else Decimal(1)
        max_exposure = capx * ctx.equity
        if notional_ref is None:
            led.cap("max_leverage", 0)
            led.add("max_leverage", False, "unpriceable (MARKET with no LTP)",
                    f"exposure <= {capx}x equity", "fail closed", shrinkable=True)
            return
        qty_max = _floor_div(max_exposure, notional_ref)
        led.cap("max_leverage", qty_max)
        exposure = Decimal(qty) * notional_ref
        ok = exposure <= max_exposure
        led.add(
            "max_leverage", ok, f"exposure {_q(exposure)}",
            f"<= {capx}x equity {_q(ctx.equity)} = {_q(max_exposure)}"
            + ("" if intraday else " (CNC is 1x cash — no delivery leverage)"),
            f"{_q(max_exposure - exposure)} INR ⇒ max qty {qty_max}",
            shrinkable=True,
        )

    def _rule_stale_data(self, led: _Ledger, ctx: GateContext, lim: Any) -> None:
        mx = float(lim.stale_data_guard.max_tick_age_s)
        sym_age, idx_age = ctx.tick_age_s, ctx.index_tick_age_s
        ok = (
            sym_age is not None and sym_age <= mx
            and idx_age is not None and idx_age <= mx
        )
        led.add(
            "stale_data_guard", ok,
            f"symbol {sym_age if sym_age is not None else 'no feed'}s / "
            f"index {idx_age if idx_age is not None else 'no feed'}s",
            f"both <= {mx}s (entry-time, per symbol + index)",
            "fresh" if ok else "no feed / stale feed ⇒ no entry (never blocks exits, R3)",
        )

    def _rule_readiness(self, led: _Ledger, ctx: GateContext) -> None:
        led.add("warmup_ready", ctx.warmup_ready, str(ctx.warmup_ready),
                "contiguous bars cover every feature lookback (§2.6)",
                "warm" if ctx.warmup_ready else "FROZEN for entries until coverage")
        led.add("regime_data_ready", ctx.regime_ready, str(ctx.regime_ready),
                "NIFTY 50 + India VIX history present for the regime lookbacks",
                "ready" if ctx.regime_ready else "regime-dependent entries FROZEN")
        led.add("clock_skew", ctx.clock_skew_ok, str(ctx.clock_skew_ok),
                "NTP skew within the limit (R6); unverifiable ⇒ not ok",
                "in sync" if ctx.clock_skew_ok else "no entries until resync")

    def _rule_entry_sanity_band(
        self, led: _Ledger, action: EnterAction, ctx: GateContext, lim: Any, intraday: bool
    ) -> None:
        band = _dec(lim.entry_sanity_band.mis_pct if intraday else lim.entry_sanity_band.cnc_pct)
        if action.entry_type == "MARKET":
            led.add("entry_sanity_band", True, "MARKET (no limit price to band-check)",
                    f"LIMIT within +/-{band}% of LTP", "not applicable")
            return
        if ctx.ltp is None or ctx.ltp <= 0 or action.entry_price is None:
            led.add("entry_sanity_band", False, "LIMIT with no usable LTP",
                    f"within +/-{band}% of LTP", "fail closed")
            return
        dev = abs(action.entry_price - ctx.ltp) / ctx.ltp * _HUNDRED
        ok = dev <= band
        led.add(
            "entry_sanity_band", ok,
            f"|{action.entry_price} - {ctx.ltp}| = {_q(dev, '0.0001')}%",
            f"<= {band}% ({'MIS' if intraday else 'CNC'})",
            f"{_q(band - dev, '0.0001')} pp",
        )

    def _rule_circuit_proximity(self, led: _Ledger, lim: Any) -> None:
        cp = lim.circuit_proximity
        # The MIS-must-be-F&O leg (C7) is enforced by `instrument_eligible`. The band-proximity leg
        # needs the exchange band feed, which lands in Phase 3 — reported as an explicit n/a so the
        # audit never reads as a verified pass.
        led.add(
            "circuit_proximity", True, "n/a (band data Phase 3)",
            f"LTP outside {cp.band_proximity_pct}% of the band edge; "
            f"MIS requires F&O (checked by instrument_eligible)",
            "band feed unavailable — NOT a verified pass",
        )

    def _rule_order_surface(self, led: _Ledger) -> None:
        na = "n/a (no API orders in RECOMMEND — OMS Phase 3)"
        led.add("order_rate", True, na, "1 order-API call/s sustained, burst 2; 70 entry calls/day",
                "not applicable in this phase")
        led.add("order_modifications", True, na, "<= 20 modifications per order (A2)",
                "not applicable in this phase")

    def _rule_margin_buffer(
        self, led: _Ledger, ctx: GateContext, lim: Any, notional_ref: Decimal | None,
        approved: int, product: str,
    ) -> None:
        ratio = _dec(lim.margin_buffer.min_ratio)
        if ctx.available_margin is None:
            led.add("margin_buffer", True, "n/a (RECOMMEND: no API order)",
                    f"available >= {ratio} x requirement (C6)", "not applicable in this phase")
            return
        if notional_ref is None or approved <= 0:
            led.add("margin_buffer", False, "unpriceable / nothing approvable",
                    f"available >= {ratio} x requirement", "fail closed")
            return
        # MIS margin ~ notional/leverage IS the right basis HERE (the broker blocks margin, not
        # notional) — unlike capital_cap this compares against the broker-reported available margin,
        # not against the tracker's notional-based deployed figure, so the units already agree.
        lev = _dec(lim.max_leverage.platform_cap_x)
        per_unit = (
            notional_ref if product == "CNC"
            else (notional_ref / lev if lev > 0 else notional_ref)
        )
        required = Decimal(approved) * per_unit
        need = ratio * required
        ok = ctx.available_margin >= need
        led.add(
            "margin_buffer", ok,
            f"available {_q(ctx.available_margin)} vs required {_q(required)} @ qty {approved}",
            f">= {ratio} x requirement = {_q(need)} (C6)",
            f"{_q(ctx.available_margin - need)} INR",
        )

    def _rule_min_viable_size(
        self, led: _Ledger, action: EnterAction, ctx: GateContext, risk_ref: Decimal | None,
        approved: int, product: Literal["MIS", "CNC"],
    ) -> CostBreakdown | None:
        """Post-shrink edge check (§7.1 ``min_viable_size``, C2/C3). Returns the CostBreakdown to
        attach to the verdict when it could be computed at all.

        Priced on the WO-4 risk basis — the edge that survives at the price the order can actually
        be filled at, not the one the stated entry advertises."""
        need = self._costs.edge_multiple_min
        limit = f"expected edge >= {need} x breakeven (C2/C3)"
        # SHADOW strategies are refused FIRST and unconditionally — before target_price is read at
        # all, so an analyst-supplied target cannot buy one an edge basis (:data:`_SHADOW_NO_EDGE`).
        # This is the property that keeps a shadow's signals a measurement and never a recommendation.
        if action.strategy_id in self._shadow_no_edge:
            led.add("min_viable_size", False, _SHADOW_NO_EDGE, limit,
                    "shadow mode: origination is journalled for validation, never recommended")
            return None
        # A targetless proposal is a hard reject UNLESS its strategy registered a measured edge
        # (§6.1 `ins`, 2026-08-17 — see _NO_TARGET_MEASURED_EDGE for why and for the guard-rails).
        measured_edge = self._strategy_edge.get(action.strategy_id)
        if action.target_price is None and (measured_edge is None or measured_edge <= 0):
            led.add("min_viable_size", False, _NO_TARGET, limit,
                    "an entry without a target can never be shown to clear costs")
            return None
        if risk_ref is None or approved <= 0:
            led.add("min_viable_size", False, "unpriceable / nothing approvable", limit,
                    "fail closed")
            return None
        try:
            if action.target_price is None:
                edge_pct = Decimal(measured_edge)
            else:
                edge_pct = self._costs.expected_edge_pct(
                    risk_ref, action.stop_price, action.target_price
                )
            breakdown = self._costs.round_trip(Decimal(approved) * risk_ref, product)
        except (ValueError, TypeError) as exc:
            led.add("min_viable_size", False, f"incoherent levels: {exc}", limit, "fail closed")
            return None
        cost = self._costs.with_edge(breakdown, edge_pct)
        ok = cost.edge_multiple >= need
        source = "" if action.target_price is not None else f" [{_NO_TARGET_MEASURED_EDGE}]"
        led.add(
            "min_viable_size", ok,
            f"edge {_q(cost.expected_edge_pct, '0.0001')}% vs breakeven "
            f"{_q(cost.breakeven_pct, '0.000001')}% @ qty {approved} = {cost.edge_multiple}x{source}",
            limit,
            f"{_q(cost.edge_multiple - need, '0.0001')}x",
        )
        return cost

    # ------------------------------------------------------------------ exit (§3.2.7)
    def _evaluate_exit(self, action: ExitAction, ctx: GateContext) -> GateVerdict:
        known = action.position_id in ctx.positions_known
        checks = [CheckResult(
            rule_id="position_known", passed=known,
            value=f"position_id={action.position_id}",
            limit="must reference a known open position",
            headroom="exits are risk-reducing — NEVER rejected for a §7.1 limit (R3)",
        )]
        verdict: Verdict = "approve" if known else "reject"
        reasons = [] if known else [f"unknown position {action.position_id}"]
        return self._verdict(action, ctx, verdict, checks, reasons)

    # ------------------------------------------------------------------ modify-stop
    def _evaluate_modify_stop(self, action: ModifyStopAction, ctx: GateContext) -> GateVerdict:
        known = action.position_id in ctx.positions_known
        checks = [self._position_known_check(action.position_id, known)]
        if not known:
            return self._verdict(action, ctx, "reject", checks,
                                 [f"unknown position {action.position_id}"])
        current = ctx.position_stop.get(action.position_id)
        side = (ctx.position_side.get(action.position_id) or "BUY").upper()
        long_side = side in ("BUY", "LONG")
        if current is None:
            # Cannot verify the direction of the change ⇒ route to the owner, never auto-approve.
            tightens: bool | None = None
        else:
            tightens = action.new_stop >= current if long_side else action.new_stop <= current
        verdict: Verdict = "approve" if tightens else "owner_approval_required"
        checks.append(CheckResult(
            rule_id="stop_direction", passed=bool(tightens),
            value=f"{side}: {current if current is not None else 'unknown'} -> {action.new_stop}",
            limit="tighten (toward entry) is auto-approved; widen needs the owner (R1)",
            headroom="tightening" if tightens else "widening / unverifiable ⇒ owner approval",
        ))
        reasons = [] if tightens else ["stop widens or current stop unknown — owner approval required"]
        return self._verdict(action, ctx, verdict, checks, reasons)

    # ------------------------------------------------------------------ modify-target
    def _evaluate_modify_target(self, action: ModifyTargetAction, ctx: GateContext) -> GateVerdict:
        known = action.position_id in ctx.positions_known
        checks = [self._position_known_check(action.position_id, known)]
        if not known:
            return self._verdict(action, ctx, "reject", checks,
                                 [f"unknown position {action.position_id}"])
        current = ctx.position_target.get(action.position_id)
        side = (ctx.position_side.get(action.position_id) or "BUY").upper()
        long_side = side in ("BUY", "LONG")
        if action.new_target is None or current is None:
            # Removing a target, or setting one where none existed, does not extend the trade.
            tightens = True
        else:
            tightens = action.new_target <= current if long_side else action.new_target >= current
        verdict: Verdict = "approve" if tightens else "owner_approval_required"
        checks.append(CheckResult(
            rule_id="target_direction", passed=tightens,
            value=f"{side}: {current if current is not None else 'none'} -> "
                  f"{action.new_target if action.new_target is not None else 'removed'}",
            limit="tighten/remove is auto-approved; extend needs the owner (R1)",
            headroom="tightening/removal" if tightens else "extension ⇒ owner approval",
        ))
        reasons = [] if tightens else ["target extends — owner approval required"]
        return self._verdict(action, ctx, verdict, checks, reasons)

    # ------------------------------------------------------------------ cancel
    def _evaluate_cancel(self, action: CancelAction, ctx: GateContext) -> GateVerdict:
        protective = action.order_id in ctx.protective_order_ids
        known = action.order_id in ctx.known_order_ids
        checks = [
            CheckResult(
                rule_id="order_not_protective", passed=not protective,
                value=f"order_id={action.order_id} protective={protective}",
                limit="protective orders are NEVER cancellable by proposal (R1/R3)",
                headroom="stops move via modify-stop; only Tier 3 may cancel-and-replace them",
            ),
            CheckResult(
                rule_id="order_known", passed=known,
                value=f"known={known}", limit="must reference a known order",
                headroom="clear" if known else "unknown order",
            ),
        ]
        if protective:
            return self._verdict(action, ctx, "reject", checks,
                                 [f"protective order {action.order_id} is not cancellable"])
        if not known:
            return self._verdict(action, ctx, "reject", checks,
                                 [f"unknown order {action.order_id}"])
        return self._verdict(action, ctx, "approve", checks, [])

    # ------------------------------------------------------------------ assembly
    @staticmethod
    def _position_known_check(position_id: str, known: bool) -> CheckResult:
        return CheckResult(
            rule_id="position_known", passed=known, value=f"position_id={position_id}",
            limit="must reference a known open position",
            headroom="clear" if known else "unknown position",
        )

    def _verdict(
        self,
        action: Any,
        ctx: GateContext,
        verdict: Verdict,
        checks: list[CheckResult],
        reasons: list[str],
        *,
        original_qty: int | None = None,
        approved_qty: int | None = None,
        cost: CostBreakdown | None = None,
    ) -> GateVerdict:
        out = GateVerdict(
            verdict_id=str(ULID()),
            proposal_id=action.proposal_id,
            verdict=verdict,
            original_qty=original_qty,
            approved_qty=approved_qty,
            checks=checks,
            cost=cost,
            reasons=reasons,
            mode=ctx.mode,
            risk_state=ctx.risk_state,
            degrade_tier=ctx.degrade_tier,
            evaluated_at=self._clock.now(),
        )
        _log.info(
            "gate_verdict", verdict=verdict, action=action.action, proposal_id=action.proposal_id,
            verdict_id=out.verdict_id, failed=[c.rule_id for c in checks if not c.passed],
        )
        return out


# --------------------------------------------------------------------------- context assembly
#: O16 (owner-directed 2026-09-07): an OPEN position with an exit recommendation delivered within
#: this many calendar days is EXITING. Calendar days rather than sessions so a Friday exit still
#: counts on Monday; the hourly position review re-issues an exit while a stop stays breached, so
#: a genuinely exiting position never ages out of the window while it is still open.
_EXITING_LOOKBACK_DAYS = 3


def _exiting_symbols(
    rec_rows: Sequence[Mapping[str, Any]], open_symbols: frozenset[str], now: datetime
) -> frozenset[str]:
    """Open symbols the platform has already told the owner to SELL (O16).

    "Keep sell recommendations separate from the buy limits": until 2026-09-07 a position the
    platform had recommended exiting kept occupying a position-count and a sector slot until the
    owner actually sold — two through-the-stop CNC positions with 39 expired exit recommendations
    held the book at 2/2 for eleven sessions and every swing entry was refused for capacity. A
    delivered exit recommendation is the platform's own record that the position is no longer
    wanted; from then on it holds no slot against a new BUY. It still blocks a new entry on its own
    symbol and still counts as deployed cash — only the counts are relaxed.
    """
    out: set[str] = set()
    for row in rec_rows:
        try:
            data = json.loads(row["payload"] or "{}")
        except (TypeError, ValueError, json.JSONDecodeError, KeyError):
            continue
        if not isinstance(data, dict) or data.get("kind") != "exit":
            continue
        symbol = str(data.get("instrument") or "")
        if symbol not in open_symbols:
            continue
        try:
            created = datetime.fromisoformat(str(data.get("created_at") or ""))
        except ValueError:
            continue
        if created.tzinfo is None:
            continue                               # naive stamps are a bug upstream; never trust
        if now - created <= timedelta(days=_EXITING_LOOKBACK_DAYS):
            out.add(symbol)
    return frozenset(out)


def _active_counts(
    positions: Sequence[Mapping[str, Any]],
    exiting: frozenset[str],
    sector_of: Mapping[str, str],
    *,
    total: int,
    mis: int,
    cnc: int,
    sector_counts: Mapping[str, int],
) -> tuple[int, int, int, dict[str, int]]:
    """Position and sector counts with the EXITING positions taken out (O16), floored at zero."""
    sectors = dict(sector_counts)
    for row in positions:
        symbol = str(row["symbol"])
        if symbol not in exiting:
            continue
        total -= 1
        if str(row["product"] or "").upper() == "MIS":
            mis -= 1
        else:
            cnc -= 1
        sector = sector_of.get(symbol, _UNCLASSIFIED)
        sectors[sector] = max(0, sectors.get(sector, 0) - 1)
    return max(0, total), max(0, mis), max(0, cnc), sectors


#: The §3.2.4 focus-cap marker, duplicated from ``engine.universe.builder.EXCL_CAP`` for the same
#: layering reason ``engine.marketdata.store`` duplicates it: the gate must not import the universe
#: builder. A row excluded for the cap ALONE is eligible; any other reason, or the extended-leg
#: markers, is not.
_EXCL_CAP_ONLY = ["watchlist_cap"]


def _eligible_universe_row(rows: Sequence[Mapping[str, Any]], symbol: str) -> dict[str, Any] | None:
    """``symbol``'s ``universe_daily`` row iff it is in the ELIGIBLE set: ``included`` (the tick
    watchlist) OR excluded for ``watchlist_cap`` alone (O15, 2026-09-04).

    Until O15 the gate read ``included_only`` rows, which equalled the eligible set only because the
    200 cap did not bind on a 200-name index. With NIFTY 500 eligible at ~350–450 names, that shortcut
    would have rejected every swing candidate from the capped tail as out-of-universe and made the
    widening a no-op. The cap governs ticks; the RECOMMEND boundary is the eligible set by definition
    (§3.2.4). Extended-leg rows (``not_in_index`` / legacy ``not_nifty200``) and rows carrying any
    real exclusion stay out — the gate approves nothing the rule did not pass.
    """
    for row in rows:
        if row.get("symbol") != symbol:
            continue
        if row.get("included") or list(row.get("exclusion_reasons") or []) == _EXCL_CAP_ONLY:
            return dict(row)
        return None
    return None


class GateContextBuilder:
    """Assembles a :class:`GateContext` from DETERMINISTIC broker/exchange sources only (R1).

    The forbidden surface is as load-bearing as the allowed one: this class reads instruments,
    universe, surveillance, sector map, earnings calendar, positions, orders, recommendations,
    equity/exposure, the mode+kill state, the calendar/session and the live feed — and NOTHING from
    the media/origination layer (§2.4 item 4, §9.1). The §7.1 origination guards are enforced
    upstream in the digest/pre-screen path.

    Parameters
    ----------
    ltp_fn / tick_age_fn / margins_fn:
        Live-feed and margins seams, injected so the builder stays testable and so an unwired seam
        fails CLOSED (no LTP ⇒ unpriceable ⇒ reject; no tick age ⇒ ``stale_data_guard`` fails).
    warmup_status_fn:
        Returns a :class:`~engine.ops.warmup.WarmupStatus`; duck-typed to avoid importing the
        composition root from Tier 2. Unwired ⇒ not ready (fail closed).
    clock_skew_ok_fn:
        Unwired ⇒ ``False`` — an unverifiable clock is never treated as "skew is fine" (R6).
    conn:
        The single sqlite connection (positions/orders/recommendations). Defaults to the one the
        :class:`~engine.risk.exposure.ExposureTracker` already holds so the composition root need
        not thread it twice.
    """

    def __init__(
        self,
        limits_engine: LimitsEngine,
        exposure: ExposureTracker,
        instruments: InstrumentStore,
        store: MarketStore,
        calendar: NSECalendar,
        clock: Clock,
        mode_manager: ModeManager,
        kill_switch: KillSwitch,
        *,
        ltp_fn: Callable[[str], Decimal | None] | None = None,
        tick_age_fn: Callable[[str], float | None] | None = None,
        margins_fn: Callable[[], Decimal | None] | None = None,
        warmup_status_fn: Callable[[], WarmupStatus] | None = None,
        clock_skew_ok_fn: Callable[[], bool] | None = None,
        degrade_tier_fn: Callable[[], str] | None = None,
        conn: sqlite3.Connection | None = None,
        nifty50_fn: Callable[[str], bool] | None = None,
        expiry_day_fn: Callable[[date], bool] | None = None,
        index_symbol: str = "NIFTY 50",
        corr_lookback_sessions: int = 20,
    ) -> None:
        self._limits = limits_engine
        self._exposure = exposure
        self._instruments = instruments
        self._store = store
        self._calendar = calendar
        self._clock = clock
        self._mode = mode_manager
        self._kill = kill_switch
        self._ltp_fn = ltp_fn
        self._tick_age_fn = tick_age_fn
        self._margins_fn = margins_fn
        self._warmup_status_fn = warmup_status_fn
        self._clock_skew_ok_fn = clock_skew_ok_fn
        self._degrade_tier_fn = degrade_tier_fn
        self._conn = conn if conn is not None else getattr(exposure, "_conn", None)
        self._nifty50_fn = nifty50_fn
        self._expiry_day_fn = expiry_day_fn
        self._index_symbol = index_symbol
        self._corr_n = int(corr_lookback_sessions)

    # ------------------------------------------------------------------ public surface
    async def build(self, symbol: str, side: str, style: str, d: date) -> GateContext:
        """Resolve every gate input for a candidate ``(symbol, side, style)`` on session ``d``.

        ``side``/``style`` are part of the pinned seam but do NOT change the assembly: the context is
        a bag of facts and the per-style branches (MIS vs CNC caps, windows, leverage) live in the
        RULES, so one context can be evaluated against any proposal for that symbol. They are kept in
        the signature so a future style-scoped source (per-stock MIS leverage, Phase 3) has a home
        without a call-site change.
        """
        del side, style          # documented above: assembly is style-agnostic in Phase 2
        table = self._limits.load()
        now = self._clock.now()

        session = self._calendar.session(d)
        positions = self._open_positions()
        open_symbols = frozenset(str(r["symbol"]) for r in positions)
        counts = self._exposure.open_position_counts()
        sector_of = self._sector_map(d)

        base = _dec(table.capital_base_inr)
        day_mtm = self._exposure.day_mtm(d)
        day_mtm_pct = (day_mtm / base * _HUNDRED) if base > 0 else Decimal(0)

        universe_row = await self._universe_row(symbol, d)
        pending = self._pending_entry_rec_symbols(now)
        orders = self._orders()

        # Un-actioned recommendations occupy concentration room too (2026-07-28 review: in RECOMMEND
        # no position exists until /taken, so sector/correlation caps never bound while the sibling
        # position-count rules already charged pending recs). Sector: each pending symbol counts in
        # its sector. Correlation: the candidate is compared against pending symbols as well.
        sector_counts: dict[str, int] = dict(self._exposure.per_sector_open(sector_of))
        for pending_symbol in pending:
            if pending_symbol == symbol:
                continue
            sector = sector_of.get(pending_symbol, _UNCLASSIFIED)
            sector_counts[sector] = sector_counts.get(sector, 0) + 1

        # O16 (2026-09-07): positions already recommended for EXIT hold no position-count or
        # sector slot against a new BUY — see _exiting_symbols. open_symbols keeps them (one
        # position per symbol) and deployed_capital keeps them (real cash).
        exiting = _exiting_symbols(self._recommendations(), open_symbols, now)
        open_total, open_mis, open_cnc, sector_counts = _active_counts(
            positions, exiting, sector_of,
            total=counts.total, mis=counts.mis, cnc=counts.cnc, sector_counts=sector_counts,
        )

        return GateContext(
            now=now,
            mode=self._mode.mode(),
            risk_state=self._mode.risk_state(),
            killed=self._kill.is_killed(),
            degrade_tier=self._degrade_tier_fn() if self._degrade_tier_fn else "DG0",
            trade_window=self._mode.get_trade_window(),
            session_open=session.open.time() if session else None,
            session_close=session.close.time() if session else None,
            equity=self._exposure.equity(),
            day_mtm_pct=day_mtm_pct,
            consecutive_losses=self._exposure.consecutive_losses(d),
            entry_recs_today=self._entry_recs_today(d),
            open_total=open_total,
            open_mis=open_mis,
            open_cnc=open_cnc,
            open_symbols=open_symbols,
            exiting_symbols=exiting,
            pending_rec_symbols=pending,
            per_symbol_cnc_notional={symbol: self._exposure.cnc_notional(symbol)},
            sector_of=sector_of,
            open_sector_counts=sector_counts,
            max_corr_with_open=await self._max_corr(symbol, open_symbols | pending, d),
            deployed_capital=self._exposure.deployed_capital(),
            ltp=self._ltp_fn(symbol) if self._ltp_fn else None,
            tick_age_s=self._tick_age_fn(symbol) if self._tick_age_fn else None,
            index_tick_age_s=self._tick_age_fn(self._index_symbol) if self._tick_age_fn else None,
            in_universe=universe_row is not None,
            mis_candidate=bool(universe_row and universe_row.get("mis_candidate")),
            surveillance_flag=await self._surveillance_flag(symbol, d),
            is_fno=self._instruments.is_fno(symbol),
            results_day_today=await self._results_day(symbol, d),
            expiry_day=self._expiry_day_fn(d) if self._expiry_day_fn else False,
            is_nifty50=self._nifty50_fn(symbol) if self._nifty50_fn else False,
            warmup_ready=self._warmup_ready(),
            regime_ready=self._regime_ready(),
            clock_skew_ok=self._clock_skew_ok_fn() if self._clock_skew_ok_fn else False,
            available_margin=self._margins_fn() if self._margins_fn else None,
            positions_known=frozenset(str(r["position_id"]) for r in positions),
            protective_order_ids=frozenset(
                str(r["order_id"]) for r in orders
                if (r["role"] or "") in PROTECTIVE_ORDER_ROLES
            ),
            known_order_ids=frozenset(str(r["order_id"]) for r in orders),
            position_side={str(r["position_id"]): str(r["side"] or "BUY") for r in positions},
            position_stop={
                str(r["position_id"]): _dec(r["stop"]) for r in positions if r["stop"]
            },
            position_target={
                str(r["position_id"]): (_dec(r["target"]) if r["target"] else None)
                for r in positions
            },
        )

    # ------------------------------------------------------------------ sqlite reads
    def _rows(self, sql: str, params: Sequence[Any] = ()) -> list[sqlite3.Row]:
        if self._conn is None:
            return []
        return list(self._conn.execute(sql, tuple(params)).fetchall())

    def _open_positions(self) -> list[sqlite3.Row]:
        """Open PLATFORM positions (O5 excludes the owner's own ``external`` trades)."""
        return self._rows(
            "SELECT position_id, symbol, side, product, qty, avg_entry, stop, target "
            "FROM positions WHERE state='OPEN' AND origin IN ('platform','recommended')"
        )

    def _orders(self) -> list[sqlite3.Row]:
        return self._rows("SELECT order_id, role FROM orders")

    def _recommendations(self) -> list[sqlite3.Row]:
        return self._rows("SELECT payload, human_action FROM recommendations")

    @staticmethod
    def _payload(row: sqlite3.Row) -> dict[str, Any]:
        try:
            data = json.loads(row["payload"] or "{}")
        except (TypeError, ValueError, json.JSONDecodeError):
            return {}
        return data if isinstance(data, dict) else {}

    def _entry_recs_today(self, d: date) -> int:
        """Entry recommendations ISSUED today (§3.6) — the Phase-2 ``max_new_trades_day`` basis: in
        RECOMMEND no position is opened, so counting positions would never bind."""
        n = 0
        for row in self._recommendations():
            data = self._payload(row)
            created = str(data.get("created_at") or "")
            if data.get("kind") == "entry" and created[:10] == d.isoformat():
                n += 1
        return n

    def _pending_entry_rec_symbols(self, now: datetime) -> frozenset[str]:
        """Unexpired, UNCONFIRMED entry recommendations — they still occupy position slots.

        Shares ``core.recommendations.parse_valid_until`` with the expiry predicate rather than
        negating it: an absent/naive/unparseable ``valid_until`` must stay excluded here (not flip to
        "pending" the way negating ``recommendation_expired`` would), so this reads the parsed instant
        directly and applies its own ``> now`` (still-in-the-future) comparison.
        """
        out: set[str] = set()
        for row in self._recommendations():
            if row["human_action"]:
                continue                       # taken / expired / dismissed / closed ⇒ not pending
            data = self._payload(row)
            if data.get("kind") != "entry":
                continue
            valid_until = parse_valid_until(data.get("valid_until"))
            if valid_until is not None and valid_until > now and data.get("instrument"):
                out.add(str(data["instrument"]))
        return frozenset(out)

    # ------------------------------------------------------------------ store reads (executor)
    async def _universe_row(self, symbol: str, d: date) -> dict[str, Any] | None:
        """``symbol``'s ``universe_daily`` row for ``d`` iff the symbol is ELIGIBLE — see
        :func:`_eligible_universe_row` (O15, 2026-09-04)."""
        rows = await self._store.arun(self._store.get_universe_daily, d)
        return _eligible_universe_row(rows, symbol)

    async def _surveillance_flag(self, symbol: str, d: date) -> str | None:
        rows = await self._store.arun(self._store.get_instruments_daily, d)
        for row in rows:
            if row.get("tradingsymbol") == symbol:
                flag = row.get("surveillance")
                return str(flag) if flag else None
        return None

    async def _results_day(self, symbol: str, d: date) -> bool:
        rows = await self._store.arun(
            self._store.get_earnings_calendar, d, d, symbol=symbol
        )
        return bool(rows)

    def _sector_map(self, d: date) -> dict[str, str]:
        rows = self._store.get_sector_map(as_of=d)
        return {str(r["symbol"]): str(r["sector"]) for r in rows if r.get("sector")}

    # ------------------------------------------------------------------ readiness seams
    def _warmup_ready(self) -> bool:
        if self._warmup_status_fn is None:
            return False
        return bool(getattr(self._warmup_status_fn(), "ready", False))

    def _regime_ready(self) -> bool:
        """Regime readiness is the ``regime:`` slice of the warm-up blockers (§7.1
        ``regime_data_ready`` is a separate rule from ``warmup_ready``)."""
        if self._warmup_status_fn is None:
            return False
        blockers = getattr(self._warmup_status_fn(), "blockers", []) or []
        return not any(str(b).startswith("regime:") for b in blockers)

    # ------------------------------------------------------------------ co-movement (§7.1)
    async def _max_corr(
        self, symbol: str, open_symbols: frozenset[str], d: date
    ) -> Decimal | None:
        """Max 20d daily-return Pearson correlation of ``symbol`` vs each open position.

        ``None`` when there are no open positions, or when no pair has the full return history —
        missing history must NOT masquerade as a correlation, and it must never silently reject
        (the rule passes with an explicit note in that case).
        """
        others = sorted(s for s in open_symbols if s != symbol)
        if not others:
            return None
        import pandas as pd  # noqa: PLC0415 - lazy: keeps `import engine.risk.gate` cheap

        # Calendar-day span comfortably containing corr_n+1 trading sessions.
        start = d - timedelta(days=self._corr_n * 3 + 20)

        async def returns(sym: str) -> Any:
            frame = await self._store.arun(self._store.get_bars_1d_frame, sym, start, d)
            if frame is None or frame.empty or "close" not in frame:
                return None
            series = frame["close"].astype(float).pct_change().dropna()
            return series.iloc[-self._corr_n:] if len(series) >= self._corr_n else None

        base = await returns(symbol)
        if base is None:
            return None
        best: float | None = None
        for other in others:
            series = await returns(other)
            if series is None:
                continue
            joined = pd.concat([base, series], axis=1, join="inner").dropna()
            if len(joined) < self._corr_n:
                continue
            value = joined.iloc[:, 0].corr(joined.iloc[:, 1])
            if value is None or pd.isna(value):
                continue
            best = float(value) if best is None else max(best, float(value))
        return None if best is None else Decimal(str(best))
