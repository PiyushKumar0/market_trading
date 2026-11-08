"""Small shared value types used across ``core`` and consumed by other packages.

Kept deliberately minimal — only types that genuinely cross module boundaries live here. All
datetimes are tz-aware IST (produced via ``core.Clock`` / ``core.NSECalendar``, never a bare
``datetime.now()``). Money is ``Decimal``.
"""

from __future__ import annotations

from datetime import datetime, time
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from engine.core.enums import Actor


class OwnerConfirmation(BaseModel):
    """Proof token for an owner-confirmed destructive/protected action.

    The actual two-step verification (one-time phrase from the owner's chat ID, or dashboard bearer
    token) happens in the ``notify``/``api`` layer; this model is the validated proof passed down to
    ``ProtectedStore.owner_update`` / ``ModeManager`` / ``KillSwitch``. ``confirmed`` is only ever set
    True by that authenticated flow — never by Tier 1 or the learner.
    """

    model_config = ConfigDict(frozen=True)

    actor: Actor
    confirmed: bool = False
    phrase: str | None = None        # the one-time confirmation phrase, when applicable
    note: str | None = None          # free-text audit note (e.g. "two-step via Telegram")


class Session(BaseModel):
    """A single trading day's session times. Returned by ``NSECalendar.session``.

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
    """A corporate action with an ex-date. Drives GTT ex-date adjustment + ledger attribution.

    Return type of ``NSECalendar.ex_dates`` (the planned GTT ex-date scan). The corp-actions
    feed stores raw rows in the ``corp_actions`` table, not this type.
    """

    model_config = ConfigDict(frozen=True)

    symbol: str
    ex_date: str                     # ISO date
    kind: str                        # "dividend" | "split" | "bonus" | ...
    ratio: str | None = None         # e.g. "1:2" for split/bonus
    amount: Decimal | None = None    # dividend per share


#: Provenance of a 1-minute bar row (``bars_1m.src``): ``self`` = built live from ticks;
#: ``kite_official`` = official historical candle (canonical where it exists);
#: ``gap_backfilled`` = an offline-span fill — excluded from the reconcile drift denominator.
BarSrc = Literal["self", "kite_official", "gap_backfilled"]


class Tick(BaseModel):
    """One KiteTicker FULL-mode tick frame, as forwarded by the mt-ticker child.

    Field names track the KiteTicker FULL-mode payload (``ticker/main.py`` wire contract) so the
    frame→model mapping is mechanical. ``volume_traded`` is the **cumulative day volume**, NOT a
    per-tick delta — bar volume is computed as the delta of this field (load-bearing).
    ``exchange_ts`` is the exchange timestamp, tz-aware IST always. The optional
    fields carry the FULL-mode extras persisted to the tick Parquet dataset (depth top-of-book feeds
    the spread estimate; ``avg_price`` is the exchange day-VWAP).
    """

    model_config = ConfigDict(frozen=True)

    instrument_token: int
    tradingsymbol: str
    ltp: Decimal                          # last traded price (KiteTicker last_price)
    volume_traded: int = Field(ge=0)      # CUMULATIVE day volume
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
    """A finalized 1-minute OHLCV bar (``bars_1m``), published on ``bar.1m``.

    ``volume`` is the per-bar volume derived from cumulative-volume deltas. ``src`` records
    provenance (:data:`BarSrc`). ``auction_open`` is the pre-open-auction-derived open price, present
    ONLY on the 09:15 row (pre-open ticks are excluded from bars; the auction open is captured
    separately). ``ts_minute`` is the bar's minute START, tz-aware IST, minute-aligned.
    """

    model_config = ConfigDict(frozen=True)

    symbol: str
    ts_minute: datetime                   # bar minute start, tz-aware IST
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int = Field(ge=0)             # Δ(cumulative day volume) over the minute
    src: BarSrc = "self"
    auction_open: Decimal | None = None   # only on the 09:15 row

    @field_validator("ts_minute")
    @classmethod
    def _tz_aware(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("ts_minute must be tz-aware IST (naive datetimes are a bug, §3.2)")
        return v


class TradeWindow(BaseModel):
    """The owner-set daily trade window (``trade_window``).

    A STICKY control-plane value (SQLite ``trade_window_state``), seeded from settings.yaml and
    owner-adjustable at runtime. Gates entry decisions + entry recommendations only — NEVER
    risk-reducing actions.
    """

    model_config = ConfigDict(frozen=True)

    start: time
    end: time
    squareoff_buffer_min: int = Field(ge=0, default=5)

    def mis_entry_cutoff(self) -> time:
        """MIS entries cut off at ``end − squareoff_buffer``."""
        from datetime import date as _date
        from datetime import timedelta

        anchor = datetime.combine(_date(2000, 1, 1), self.end)
        cutoff = (anchor - timedelta(minutes=self.squareoff_buffer_min)).time()
        return cutoff
