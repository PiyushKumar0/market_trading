"""Typed, hash-verified access to ``config/limits.yaml`` (§7.1, §3.2.7). Tier-2-owned.

``LimitsEngine`` wraps a :class:`~engine.core.protected_store.ProtectedStore` and parses the raw dict
:meth:`~engine.core.protected_store.ProtectedStore.load_verified` returns into a frozen, fully-typed
:class:`LimitTable`. It does NOT decide the integrity-failure consequence: a hash mismatch raises
:class:`~engine.core.protected_store.IntegrityError`, which propagates untouched to the caller — the
gate maps that to FROZEN/kill per §2.4, not this module's job (mirrors ``ProtectedStore`` itself).

Every block in ``limits:`` is typed with ``extra="forbid"`` so a yaml typo or stray key fails loudly at
load time rather than silently vanishing — EXCEPT each block's ``on_breach`` marker, which stays a plain
``str`` (not a closed ``Literal``) so its wording can change without touching this file. Percentages are
kept exactly as given in the yaml (e.g. ``-5.0`` means -5%, not a fraction); money fields are ``Decimal``.
"""

from __future__ import annotations

from datetime import time
from decimal import Decimal

from pydantic import BaseModel, ConfigDict

from engine.core.protected_store import ProtectedStore

LIMITS_FILE = "limits.yaml"


# --------------------------------------------------------------------------- per-rule_id blocks (§7.1)
class _Block(BaseModel):
    """Base for every ``limits.<rule_id>`` block: frozen, unknown keys fail loud (R4)."""

    model_config = ConfigDict(frozen=True, extra="forbid")


class CapitalCap(_Block):
    max_deployed_capital_inr: Decimal
    on_breach: str


class PerTradeRisk(_Block):
    intraday_pct: float
    swing_position_pct: float
    overnight_gap_mult: float
    on_breach: str


class DailyLossSoft(_Block):
    day_mtm_pct: float
    on_breach: str


class DailyLossHard(_Block):
    day_mtm_pct: float
    on_breach: str


class WeeklyDrawdown(_Block):
    rolling_sessions: int
    drawdown_pct: float
    on_breach: str


class EquityFloorRung(_Block):
    equity_pct_of_base: float
    on_breach: str


class CumulativeFloor(_Block):
    equity_pct_of_base: float
    on_breach: str


class ConsecutiveLosses(_Block):
    max_per_session: int
    on_breach: str


class MaxNewTradesDay(_Block):
    count: int
    on_breach: str


class MaxOpenPositions(_Block):
    total: int
    max_mis: int
    max_cnc: int
    on_breach: str


class PerStockExposure(_Block):
    max_positions_per_symbol: int
    cnc_notional_inr: Decimal
    on_breach: str


class PerSectorExposure(_Block):
    max_positions_per_sector: int
    unclassified_cap: int
    on_breach: str


class CoMovementCap(_Block):
    corr_max: float
    on_breach: str


class MaxLeverage(_Block):
    platform_cap_x: float
    phase4_start_x: float
    on_breach: str


class NoTradeWindows(_Block):
    mis_entry_start: time
    mis_entry_end: time
    cnc_entry_start: time
    cnc_entry_end: time
    no_entry_on_results_day: bool
    nifty50_no_new_mis_after_on_expiry: time
    on_breach: str


class CatalystGuard(_Block):
    """News-origination anti-manipulation surface (O11, §2.7 step 5). Owner-only, never learnable."""

    min_source_domains: int
    sentiment_min_long: float
    max_catalyst_entries_day: int
    digest_stale_max_h: int
    originating_event_types: list[str]
    on_breach: str


class TradeWindowMarker(_Block):
    """Static guardrails only — the live owner-set window lives in SQLite ``trade_window_state``
    (:class:`engine.risk.mode.ModeManager`), not here (§3.2.7)."""

    must_be_within_session: bool
    mis_sub_window_must_be_nonempty_after_buffer: bool
    on_breach: str


class StaleDataGuard(_Block):
    max_tick_age_s: int
    feed_heartbeat_silence_s: int
    on_breach: str


class WarmupReady(_Block):
    on_breach: str


class RegimeDataReady(_Block):
    on_breach: str


class ClockSkew(_Block):
    max_skew_s: int
    on_breach: str


class EntrySanityBand(_Block):
    mis_pct: float
    cnc_pct: float
    on_breach: str


class CircuitProximity(_Block):
    band_proximity_pct: float
    mis_requires_fno: bool
    on_breach: str


class MaxHolding(_Block):
    swing_trading_days: int
    position_trading_days: int
    on_breach: str


class MinResidualWindow(_Block):
    min_hold_min: int
    on_breach: str


class OrderRate(_Block):
    sustained_per_s: int
    burst: int
    entry_calls_per_day: int
    broker_hard_ceiling_per_day: int
    on_breach: str


class OrderModifications(_Block):
    self_cap: int
    on_breach: str


class MarginBuffer(_Block):
    min_ratio: float
    on_breach: str


class MinViableSize(_Block):
    edge_multiple_min_default: float
    on_breach: str


# --------------------------------------------------------------------------- the ``limits:`` map
class LimitsBlock(BaseModel):
    """Every ``rule_id`` under ``limits:`` (§7.1 table). Unknown top-level rule_ids fail loud."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    capital_cap: CapitalCap
    per_trade_risk: PerTradeRisk
    daily_loss_soft: DailyLossSoft
    daily_loss_hard: DailyLossHard
    weekly_drawdown: WeeklyDrawdown
    equity_floor_rung: EquityFloorRung
    cumulative_floor: CumulativeFloor
    consecutive_losses: ConsecutiveLosses
    max_new_trades_day: MaxNewTradesDay
    max_open_positions: MaxOpenPositions
    per_stock_exposure: PerStockExposure
    per_sector_exposure: PerSectorExposure
    co_movement_cap: CoMovementCap
    max_leverage: MaxLeverage
    no_trade_windows: NoTradeWindows
    catalyst_guard: CatalystGuard
    trade_window: TradeWindowMarker
    stale_data_guard: StaleDataGuard
    warmup_ready: WarmupReady
    regime_data_ready: RegimeDataReady
    clock_skew: ClockSkew
    entry_sanity_band: EntrySanityBand
    circuit_proximity: CircuitProximity
    max_holding: MaxHolding
    min_residual_window: MinResidualWindow
    order_rate: OrderRate
    order_modifications: OrderModifications
    margin_buffer: MarginBuffer
    min_viable_size: MinViableSize


class LimitTable(BaseModel):
    """The full parsed ``config/limits.yaml`` (§7.1). ``capital_base_inr`` and
    ``analyst_confidence_min`` are top-level, OUTSIDE the ``limits:`` map (§6.3)."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    schema_version: int
    capital_base_inr: Decimal
    limits: LimitsBlock
    analyst_confidence_min: float


# --------------------------------------------------------------------------- engine
class LimitsEngine:
    """Typed, cached, hash-verified reader of ``limits.yaml`` (§3.2.7).

    Read-only: there is no write path here (R4) — changes go through
    ``ProtectedStore.owner_update`` and are picked up on the next :meth:`reload`.
    """

    def __init__(self, store: ProtectedStore) -> None:
        self._store = store
        self._table: LimitTable | None = None

    def load(self) -> LimitTable:
        """Parsed, validated limit table. Cached after the first successful load."""
        if self._table is None:
            self._table = self._parse()
        return self._table

    def reload(self) -> LimitTable:
        """Force a re-verify + re-parse, discarding the cache. ``IntegrityError`` propagates
        uncaught (R4) — the caller (self-test / gate) decides the consequence (§2.4)."""
        self._table = self._parse()
        return self._table

    def table(self) -> LimitTable:
        """The full typed limit table (convenience alias for :meth:`load`)."""
        return self.load()

    def catalyst_guard(self) -> CatalystGuard:
        """The ``catalyst_guard`` block (used by ``CatalystDigestJob`` / pre-screen, §2.7 step 5)."""
        return self.load().limits.catalyst_guard

    def _parse(self) -> LimitTable:
        raw = self._store.load_verified(LIMITS_FILE)
        return LimitTable.model_validate(raw)
