"""Small shared value types used across ``core`` and consumed by other packages.

Kept deliberately minimal — only types that genuinely cross module boundaries live here. All
datetimes are tz-aware IST (produced via ``core.Clock`` / ``core.NSECalendar``, never a bare
``datetime.now()`` — §3.2 convention). Money is ``Decimal``.
"""

from __future__ import annotations

from datetime import datetime, time
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field

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
