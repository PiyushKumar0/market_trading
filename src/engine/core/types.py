"""Small shared value types used across ``core`` and consumed by other packages.

Kept deliberately minimal — only types that genuinely cross module boundaries live here. All
datetimes are tz-aware IST (produced via ``core.Clock`` / ``core.NSECalendar``, never a bare
``datetime.now()`` — §3.2 convention). Money is ``Decimal``.
"""

from __future__ import annotations

from datetime import datetime, time
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from engine.core.enums import Actor


class OwnerConfirmation(BaseModel):
    """Proof token for an owner-confirmed destructive/protected action (R10).

    The actual two-step verification (one-time phrase from the owner's chat ID, or dashboard bearer
    token) happens in the ``notify``/``api`` layer; this model is the validated proof passed down to
    ``ProtectedStore.owner_update`` / ``ModeManager`` / ``KillSwitch``. ``confirmed`` is only ever set
    True by that authenticated flow — never by Tier 1 or the learner (R4).
    """

    model_config = ConfigDict(frozen=True)

    actor: Actor
    confirmed: bool = False
    phrase: str | None = None        # the one-time confirmation phrase, when applicable
    note: str | None = None          # free-text audit note (e.g. "two-step via Telegram")


class Session(BaseModel):
    """A single trading day's session times (R6). Returned by ``NSECalendar.session``.

    Handles muhurat (Diwali evening) and shortened/special sessions. All times tz-aware IST.
    """

    model_config = ConfigDict(frozen=True)

    date_ist: str                    # ISO date "YYYY-MM-DD"
    pre_open_start: datetime
    pre_open_end: datetime
    open: datetime                   # continuous session open (09:15 regular)
    close: datetime                  # continuous session close (15:30 regular)
    post_close_start: datetime | None = None
    post_close_end: datetime | None = None
    is_muhurat: bool = False
    is_shortened: bool = False


class CorpAction(BaseModel):
    """A corporate action with an ex-date (A12). Drives GTT ex-date adjustment + ledger attribution.

    Phase 0 ships the type; the corp-actions feed (datafeeds) populates it in Phase 1.
    """

    model_config = ConfigDict(frozen=True)

    symbol: str
    ex_date: str                     # ISO date
    kind: str                        # "dividend" | "split" | "bonus" | ...
    ratio: str | None = None         # e.g. "1:2" for split/bonus
    amount: Decimal | None = None    # dividend per share


#: Provenance of a 1-minute bar row (§4.3 ``bars_1m.src``): ``self`` = built live from ticks (A13);
#: ``kite_official`` = official historical candle (canonical where it exists, §4.4 job 2);
#: ``gap_backfilled`` = an offline-span fill (§2.6) — excluded from the reconcile drift denominator.
BarSrc = Literal["self", "kite_official", "gap_backfilled"]


class Tick(BaseModel):
    """One KiteTicker FULL-mode tick frame, as forwarded by the mt-ticker child (§3.2.2/§3.2.3).

    Field names track the KiteTicker FULL-mode payload (``ticker/main.py`` wire contract) so the
    frame→model mapping is mechanical. ``volume_traded`` is the **cumulative day volume**, NOT a
    per-tick delta — bar volume is computed as the delta of this field (A13, load-bearing).
    ``exchange_ts`` is the exchange timestamp, tz-aware IST always (§3.2 convention). The optional
    fields carry the FULL-mode extras persisted to the tick Parquet dataset (depth top-of-book feeds
    the §6.2 spread estimate; ``avg_price`` is the exchange day-VWAP).
    """

    model_config = ConfigDict(frozen=True)

    instrument_token: int
    tradingsymbol: str
    ltp: Decimal                          # last traded price (KiteTicker last_price)
    volume_traded: int = Field(ge=0)      # CUMULATIVE day volume (A13)
    exchange_ts: datetime                 # tz-aware IST (validated below)
    ohlc_open: Decimal | None = None      # FULL-mode day ohlc snapshot
    ohlc_high: Decimal | None = None
    ohlc_low: Decimal | None = None
    ohlc_close: Decimal | None = None     # previous close during the session
    avg_price: Decimal | None = None      # average traded price (day VWAP)
    bid: Decimal | None = None            # depth top-of-book best bid
    ask: Decimal | None = None            # depth top-of-book best ask

    @field_validator("exchange_ts")
    @classmethod
    def _tz_aware(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("exchange_ts must be tz-aware IST (naive datetimes are a bug, §3.2)")
        return v


class Bar(BaseModel):
    """A finalized 1-minute OHLCV bar (§3.2.3/§4.3 ``bars_1m``), published on ``bar.1m``.

    ``volume`` is the per-bar volume derived from cumulative-volume deltas (A13). ``src`` records
    provenance (:data:`BarSrc`). ``auction_open`` is the pre-open-auction-derived open price, present
    ONLY on the 09:15 row (A14 — pre-open ticks are excluded from bars; the auction open is captured
    separately). ``ts_minute`` is the bar's minute START, tz-aware IST, minute-aligned.
    """

    model_config = ConfigDict(frozen=True)

    symbol: str
    ts_minute: datetime                   # bar minute start, tz-aware IST
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int = Field(ge=0)             # Δ(cumulative day volume) over the minute (A13)
    src: BarSrc = "self"
    auction_open: Decimal | None = None   # only on the 09:15 row (A14)

    @field_validator("ts_minute")
    @classmethod
    def _tz_aware(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("ts_minute must be tz-aware IST (naive datetimes are a bug, §3.2)")
        return v


class TradeWindow(BaseModel):
    """The owner-set daily trade window (§7.1 ``trade_window``).

    A STICKY control-plane value (SQLite ``trade_window_state``), seeded from settings.yaml and
    owner-adjustable at runtime (§3.2.7). Gates entry decisions + entry recommendations only — NEVER
    risk-reducing actions (R3).
    """

    model_config = ConfigDict(frozen=True)

    start: time
    end: time
    squareoff_buffer_min: int = Field(ge=0, default=5)

    def mis_entry_cutoff(self) -> time:
        """MIS entries cut off at ``end − squareoff_buffer`` (§3.2.8)."""
        from datetime import date as _date
        from datetime import timedelta

        anchor = datetime.combine(_date(2000, 1, 1), self.end)
        cutoff = (anchor - timedelta(minutes=self.squareoff_buffer_min)).time()
        return cutoff
