"""Strategy-layer value types — the ``SignalCandidate`` published on ``signal.candidate``.

``SignalCandidate`` carries EXACTLY the pinned fields::

    {signal_id, strategy_id, symbol, side, style, raw_levels, score, features_snapshot_id,
     catalyst_ref: str | None}

``catalyst_ref`` is the ``catalyst_watchlist.entry_id`` audit link; it is ``None`` for every price
baseline and set only by the cat/cat_reversal batch rules (``_shadow_catalyst.build_candidate``).
Prices are ``Decimal`` (money convention; pydantic serializes them as strings in JSON mode).

``ScanContext`` is the internal pre-screen → scanner seam (not a pinned surface): the
``SignalPreScreen`` context provider assembles everything a scanner may need for one bar so the
scanners themselves stay pure functions of (bar, context, params) — deterministic and replayable
(same inputs ⇒ same candidates, modulo the platform-minted ``signal_id`` ULID).
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import ROUND_HALF_UP, Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from engine.core.types import Bar
from engine.marketdata.store import DailyBar

#: Order side / strategy style vocabularies — kept literal-identical to
#: ``engine.intelligence.schemas.EnterAction`` so a candidate maps 1:1 onto a proposal.
Side = Literal["BUY", "SELL"]
Style = Literal["intraday", "swing", "position"]

#: NSE cash-equity minimum tick. Scanner levels are quantized to this before publication.
NSE_TICK = Decimal("0.05")


def round_to_tick(value: Decimal | float, tick: Decimal = NSE_TICK) -> Decimal:
    """Quantize a price to the nearest ``tick`` (half-up), returned as a 2-dp ``Decimal``.

    Floats are converted via ``str()`` first (never inherit binary-float artifacts into a price —
    the decimal convention). Used by every scanner to turn float indicator arithmetic
    (ATR multiples, percentages) back into exact ``DECIMAL(12,2)``-compatible levels.
    """
    v = value if isinstance(value, Decimal) else Decimal(str(value))
    steps = (v / tick).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    return (steps * tick).quantize(Decimal("0.01"))


class RawLevels(BaseModel):
    """Raw (pre-gate, pre-sizing) price levels of a candidate (``raw_levels``).

    ``entry`` is always present (the trigger bar close). ``stop``/``target`` are ``None`` where the
    rule sketch defines no fixed level (rsi2/trend exit on rule or trail; mom is
    rebalance-driven) — the level is informational for Tier-1/gate, never a synthetic invention.
    """

    model_config = ConfigDict(frozen=True)

    entry: Decimal
    stop: Decimal | None = None
    target: Decimal | None = None


class SignalCandidate(BaseModel):
    """A pre-screened baseline signal (PINNED field set) published on ``signal.candidate``."""

    model_config = ConfigDict(frozen=True)

    signal_id: str                            # platform-minted ULID
    strategy_id: str                          # id: orb | rsi2 | trend | mom | brk20 | ins | hi52 | cat | cat_reversal
    symbol: str
    side: Side
    style: Style
    raw_levels: RawLevels
    score: float = Field(ge=0.0, le=1.0)      # informational strength, per-scanner semantics
    features_snapshot_id: str | None = None   # feature_snapshots key (None until wired)
    catalyst_ref: str | None = None           # catalyst_watchlist.entry_id; price baselines: None


class PendingSetup(BaseModel):
    """A strategy condition that is NOT currently true, with the deterministic price at which it
    would arm.

    Purely informational — it never enters the pipeline and no LLM sees it at origination. The owner
    uses it to decide whether extending the trade window is worth it. ``trigger_price`` is None where
    the condition is not price-invertible (cross-sectional / rule-based) and ``condition`` then
    carries the plain-text rule alone. ``arms_when`` is the SCANNER's own statement of the crossing
    direction (inferring it from a price comparison mislabels when trigger == last).
    ``stop_price`` / ``target_price`` / ``exit_rule`` carry the full would-be plan, never a naked
    level.
    """

    model_config = ConfigDict(frozen=True)

    strategy_id: str
    symbol: str
    side: Side
    style: Style
    trigger_price: Decimal | None = None       # tick-rounded arm level; None = not price-invertible
    arms_when: Literal["above", "below"] = "above"   # crossing direction, declared by the scanner
    last_price: Decimal | None = None          # the scanned bar's close, for at-a-glance distance
    stop_price: Decimal | None = None          # the stop the strategy would use at the trigger
    target_price: Decimal | None = None        # fixed target where the rule defines one
    exit_rule: str = ""                        # words, where exit is rule-based instead of a target
    condition: str = ""                        # human-readable arming rule (volume gate, window, …)


class ScanContext(BaseModel):
    """Everything a scanner may read for one bar, assembled by the pre-screen's context provider.

    Scanners must FAIL TO ZERO on missing context (return no candidates), never raise or guess —
    thin data is a warm-up condition (``warmup_ready``), not an error. All datetimes tz-aware
    IST.
    """

    model_config = ConfigDict(frozen=True)

    intraday_bars: list[Bar] = Field(default_factory=list)
    """Today's session 1m bars ascending, INCLUDING the bar being scanned (it is the last element)."""

    daily_bars: list[DailyBar] = Field(default_factory=list)
    """Completed daily bars for the symbol, ascending (normally through the previous session)."""

    index_daily_closes: list[Decimal] = Field(default_factory=list)
    """Reference-index (NIFTY 50) daily closes ascending — the rsi2 regime filter input."""

    flagged: bool = False
    """Symbol appears in ``flagged_instrument_days`` for the PRIOR trading session (bulk/block
    deal) — volume-breakout scanners suppress. Prior session, not today: NSE publishes deals ~EOD and
    the deals job writes them at 20:30, so a day's own flags are unknowable while it trades."""

    trade_window: tuple[datetime, datetime] | None = None
    """Today's owner trade window [start, end], already session-clamped (``NSECalendar.trade_window``)."""

    session_open: datetime | None = None
    """Today's continuous-session open (09:15 regular; shortened-session aware). ORB range anchor."""

    momentum_by_symbol: dict[str, float] = Field(default_factory=dict)
    """Cross-sectional N-week momentum per universe symbol (provider computes via
    ``indicators.momentum`` on the same bars_1d data) — the ``mom`` ranking input."""

    upcoming_ex_dates: list[date] = Field(default_factory=list)
    """Known upcoming corp-action ex-dates for THIS symbol — the ``mom`` skip input."""

    mom_sessions_since_rebalance: int | None = None
    """TRADING SESSIONS elapsed since the last ``mom`` rebalance (None ⇒ never ⇒ due now).
    Counted by the provider via ``NSECalendar`` so the scanner stays calendar-free;
    ``mom.rebalance_days`` is denominated in trading days (its 20 upper bound = the swing cap)."""

    features_snapshot_id: str | None = None
    """Intraday feature snapshot id minted by FeatureEngine for this bar; carried onto
    every candidate for the audit chain."""
