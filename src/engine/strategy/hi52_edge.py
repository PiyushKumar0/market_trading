"""``hi52``'s registered edge: the derivation, and the live map that caps it at the current limits.

``settings.yaml hi52.expected_edge_pct`` is the OUTPUT of this arithmetic at the registration
inputs (``capital_base_inr`` 40,000, the ``equity_floor_rung``, §7.1 ``per_trade_risk``). The cost
floor it must clear RISES as the book shrinks, so a capital change (O19) can leave that constant
stale-high. :class:`StrategyEdges` is the runtime guard: it hands the gate ``min(registered,
derived from the CURRENT limits table)`` for ``hi52`` and leaves every other strategy untouched. It
only ever LOWERS an edge: a larger base derives a bigger number, but an edge registered at 1.53 was
measured at 1.53 and is not raised by arithmetic.

``scripts/hi52_forward_verdict.py`` delegates here, so there is one derivation.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping
from decimal import ROUND_FLOOR, Decimal
from typing import TYPE_CHECKING

from engine.core.log import get_logger
from engine.strategy.cost_model import CostModel
from engine.strategy.scanners import hi52

if TYPE_CHECKING:  # engine.risk modules may import engine.strategy; a runtime import would cycle
    from engine.risk.limits import LimitTable

_log = get_logger("engine.strategy.hi52_edge")

#: The v2 study's measured T+20 median GROSS drift — ``data/reports/backtest_hi52_v2_2026-09-09.json``
#: (``geometry/discrete_fresh_cross/horizons/20/median_gross_pct``, n=1,794). GROSS on purpose: the
#: study charged its own ₹20,000-reference round trip, a size this book never trades, so the net it
#: reports is not this book's net. The cost floor is what makes it one.
MEASURED_T20_MEDIAN_GROSS_PCT = Decimal("2.0336")

#: The rule's own disaster stop — read from the scanner, never re-typed: it is half the derivation
#: (a wider stop shrinks the notional and RAISES the cost floor), so a change there must move this.
STOP_PCT = Decimal(str(hi52.DEFAULT_PARAMS["stop_pct"]))

PRODUCT = "CNC"          # swing/delivery; NSE cash cannot be held overnight under MIS

_HUNDRED = Decimal("100")


def sizing_notional(
    equity: Decimal, *, position_pct: Decimal, gap_mult: Decimal, stop_pct: Decimal
) -> Decimal:
    """The §7.1 ``per_trade_risk`` cap on a swing position's NOTIONAL, in rupees.

    ``qty_max = floor(position_pct% × equity / (gap_mult × stop_pct% × price))`` (``gate.py``
    ``_rule_per_trade_risk``), so ``qty_max × price <= position_pct% × equity / (gap_mult ×
    stop_pct%)`` — the price cancels and the cap is a pure function of these three numbers. The
    other sizing rules do not bind here: ``capital_cap`` allows ₹40,000 deployed against this ~₹6.5k.

    Returns arithmetic on the ``equity`` passed in only (no module constant); a non-positive or non-finite input is a
    refusal (``ValueError``) rather than a silently tiny notional, because a tiny notional means a
    huge cost floor and a spurious DEMOTE.
    """
    if equity <= 0 or position_pct <= 0 or gap_mult <= 0 or stop_pct <= 0:
        raise ValueError(
            f"sizing inputs must be positive: equity={equity} position_pct={position_pct} "
            f"gap_mult={gap_mult} stop_pct={stop_pct}"
        )
    return (equity * position_pct / _HUNDRED) / (gap_mult * stop_pct / _HUNDRED)


def cost_floor_pct(notional: Decimal, cost_model: CostModel | None = None) -> Decimal:
    """One full CNC round trip at ``notional``, in percent — the repo's own ``CostModel``.

    ``breakeven_pct``, never the fees-only view: the spread is the largest single component at this
    size and a floor that omits it is not a floor.
    """
    model = cost_model if cost_model is not None else CostModel.from_config()
    return Decimal(str(model.breakeven_pct(notional, PRODUCT)))


def edge_at(
    base: Decimal,
    rung_pct: Decimal,
    position_pct: Decimal,
    gap_mult: Decimal,
    *,
    stop_pct: Decimal = STOP_PCT,
    cost_model: CostModel | None = None,
) -> Decimal:
    """``measured T+20 median GROSS − one CNC round trip at the §7.1-sized notional``, to 0.01.

    Derived at the ``equity_floor_rung`` (``base × (1 + rung_pct/100)``), the smallest equity the
    risk table lets a new position be opened at — the WORST case, since the cost floor rises as the
    book shrinks. ROUND_FLOOR, never nearest: rounding an edge UP is the one direction that can buy
    a candidate a C3 pass it did not earn (and a negative result rounds away from zero too).
    """
    equity = base * (Decimal("1") + rung_pct / _HUNDRED)
    if base <= 0 or equity <= 0 or equity >= base:
        raise ValueError(f"implausible equity floor rung: base={base} pct={rung_pct}")
    notional = sizing_notional(
        equity, position_pct=position_pct, gap_mult=gap_mult, stop_pct=stop_pct
    )
    edge = MEASURED_T20_MEDIAN_GROSS_PCT - cost_floor_pct(notional, cost_model)
    return edge.quantize(Decimal("0.01"), rounding=ROUND_FLOOR)


def derived_edge_pct(table: LimitTable, cost_model: CostModel | None = None) -> Decimal:
    """:func:`edge_at` on a parsed limits table (``LimitsEngine.load()``)."""
    lim = table.limits
    return edge_at(
        table.capital_base_inr,
        Decimal(str(lim.equity_floor_rung.equity_pct_of_base)),
        Decimal(str(lim.per_trade_risk.swing_position_pct)),
        Decimal(str(lim.per_trade_risk.overnight_gap_mult)),
        cost_model=cost_model,
    )


class StrategyEdges(Mapping[str, Decimal]):
    """The gate's ``strategy_id -> registered edge`` map, with ``hi52`` capped at its derivation.

    Read on every candidate, so the derivation is cached per limits-table OBJECT (``is``): the
    engine hands out a fresh table when the limits change, and only then is it recomputed. A failed
    derivation (unverified limits, a bad rung) keeps the registered edge — the guard may only ever
    lower an edge, never be the reason one is refused — and is logged once per failure streak.
    """

    def __init__(
        self,
        registered: Mapping[str, Decimal],
        *,
        table_fn: Callable[[], LimitTable] | None = None,
        cost_model: CostModel | None = None,
    ) -> None:
        self._registered = registered
        self._table_fn = table_fn
        self._cost_model = cost_model
        self._cached: tuple[object, Decimal] | None = None
        self._failure_logged = False

    def __getitem__(self, sid: str) -> Decimal:
        registered = self._registered[sid]
        if sid != hi52.STRATEGY_ID or self._table_fn is None:
            return registered
        try:
            table = self._table_fn()
            if self._cached is not None and self._cached[0] is table:
                derived = self._cached[1]
            else:
                derived = derived_edge_pct(table, self._cost_model)
                self._cached = (table, derived)
            self._failure_logged = False
        except Exception as exc:  # noqa: BLE001 - the guard must never take the gate down
            if not self._failure_logged:
                self._failure_logged = True
                _log.warning("hi52_edge_derivation_failed", error=str(exc))
            return registered
        return min(registered, derived)

    def edge_for_table(self, sid: str, table: LimitTable) -> Decimal:
        """The edge the gate would use for ``sid`` if ``table`` were live (O19 capital preview).

        Same rule as a live read: registered for every strategy but ``hi52``, ``min(registered,
        derived at table)`` for it, and the registered edge when the derivation fails. Leaves the live
        read's per-table cache alone, so previewing a proposal never costs the gate a re-derivation.
        """
        registered = self._registered[sid]
        if sid != hi52.STRATEGY_ID:
            return registered
        try:
            derived = derived_edge_pct(table, self._cost_model)
        except Exception as exc:  # noqa: BLE001 - mirrors the live read: never raise, never raise an edge
            _log.warning("hi52_edge_derivation_failed", error=str(exc), at="edge_for_table")
            return registered
        return min(registered, derived)

    def __iter__(self) -> Iterator[str]:
        return iter(self._registered)

    def __len__(self) -> int:
        return len(self._registered)
