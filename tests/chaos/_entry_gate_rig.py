"""Shared plumbing for chaos cases 3 / 14 / 18: the §7.1 ENTRY gate exactly as ``engine.ops.main``
composes it (``main.py`` ``ctx_builder = GateContextBuilder(...)`` + ``gate = RiskGate(...)``) — the
real :class:`GateContextBuilder` over a temp :class:`MarketStore` and the migrated temp SQLite, the
real :class:`RiskGate` over the SHIPPED ``config/limits.yaml`` / ``config/costs.yaml`` (read-only).

The seams main.py injects as closures (``ltp_fn`` / ``tick_age_fn`` / ``warmup_status_fn`` /
``clock_skew_ok_fn``) are passed in by each case, which is precisely what each case perturbs. A case
asserts on named §7.1 checks of the verdict (``stale_data_guard``, ``clock_skew``, ``warmup_ready``,
``regime_data_ready``, ``mode_risk_state``) — the other rules depend on universe/instrument data the
chaos stores deliberately do not carry, so a whole-verdict ``approve`` is never the assertion.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import yaml

from engine.broker.instruments import InstrumentStore
from engine.core.contracts import CheckResult, EnterAction
from engine.risk.exposure import ExposureTracker
from engine.risk.gate import GateContextBuilder, RiskGate
from engine.risk.limits import LimitTable
from engine.strategy.cost_model import CostModel, load_cost_rates

REPO = Path(__file__).resolve().parents[2]
LIMITS_YAML = REPO / "config" / "limits.yaml"
COSTS_YAML = REPO / "config" / "costs.yaml"
INDEX_SYMBOL = "NIFTY 50"


class ShippedLimits:
    """Duck-typed ``LimitsEngine`` over the shipped limits.yaml (the gate only calls ``load()``)."""

    def __init__(self) -> None:
        self._table = LimitTable.model_validate(yaml.safe_load(LIMITS_YAML.read_text(encoding="utf-8")))

    def load(self) -> LimitTable:
        return self._table


def build_entry_gate(
    *, conn, clock, calendar, mode, kill, store,
    ltp_fn: Callable[[str], Decimal | None] | None = None,
    tick_age_fn: Callable[[str], float | None] | None = None,
    warmup_status_fn: Callable[[], Any] | None = None,
    clock_skew_ok_fn: Callable[[], bool] | None = None,
) -> tuple[GateContextBuilder, RiskGate]:
    limits = ShippedLimits()
    exposure = ExposureTracker(conn, clock, Decimal(str(limits.load().capital_base_inr)), mark_price=ltp_fn)
    builder = GateContextBuilder(
        limits, exposure, InstrumentStore(clock), store, calendar, clock, mode, kill,
        ltp_fn=ltp_fn, tick_age_fn=tick_age_fn, warmup_status_fn=warmup_status_fn,
        clock_skew_ok_fn=clock_skew_ok_fn, degrade_tier_fn=lambda: "DG0",
        conn=conn, index_symbol=INDEX_SYMBOL,
    )
    gate = RiskGate(limits, CostModel(load_cost_rates(COSTS_YAML), edge_multiple_min=Decimal("2.0")), clock)
    return builder, gate


def enter_action(symbol: str, now: datetime, *, style: str = "intraday") -> EnterAction:
    """A well-formed LIMIT BUY entry proposal (the gate-suite baseline shape) for ``symbol``."""
    return EnterAction(
        action="enter", proposal_id="01CHAOSPROPOSAL", agent_id="analyst",
        thesis="Chaos-suite probe proposal: a well-formed entry used only to read the gate's checks.",
        confidence=0.70, valid_until=now + timedelta(minutes=15), inputs_digest="chaos",
        tradingsymbol=symbol, exchange="NSE", side="BUY", style=style, entry_type="LIMIT",
        entry_price=Decimal("100"), stop_price=Decimal("99"), target_price=Decimal("103"),
        quantity=1, signal_id="01CHAOSSIGNAL", strategy_id="orb" if style == "intraday" else "rsi2",
        features_snapshot_id="01CHAOSSNAP",
    )


async def entry_checks(
    builder: GateContextBuilder, gate: RiskGate, clock, symbol: str, *, style: str = "intraday"
) -> tuple[str, dict[str, CheckResult]]:
    """Build the REAL context for ``symbol`` and evaluate one entry: ``(verdict, {rule_id: check})``."""
    now = clock.now()
    ctx = await builder.build(symbol, "BUY", style, now.date())
    verdict = gate.evaluate(enter_action(symbol, now, style=style), ctx)
    return verdict.verdict, {c.rule_id: c for c in verdict.checks}
