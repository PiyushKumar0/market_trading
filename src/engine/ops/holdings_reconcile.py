"""Holdings reconcile for tracked CNC positions (§3.6, owner-directed 2026-09-07).

The §3.2.8 fill-side reconciler (``REC_FILL_SUSPECTED``) is still a Phase-3 item, so until now the
platform could not tell whether a tracked position still EXISTED in the owner's account. HDFCAMC and
HINDZINC carried 39 expired exit recommendations across eleven sessions with no way to distinguish
"the owner is ignoring the exit" from "the owner sold and never sent ``/closed``" — and a position
the platform wrongly believes is open keeps consuming the O16 per-name cap.

This job closes that hole with the cheapest possible question: does the broker still hold it? One
``holdings()`` call, compared against every OPEN CNC position with ``origin in
('platform','recommended')`` that is old enough to have settled, and one owner alert naming the exact
``/closed <entry_rec_id> <price>`` reply.

**Alert-only, never load-bearing (§3.6).** The platform never auto-closes a human-owned position (the
close price is the owner's fact, §6.5 outcome labelling), never touches risk state, the gate or the
prescreen, and a broker/token failure degrades to a WARNING log plus ``error`` on the result — the
check is diagnostic, not a gate input. Every design choice here is biased against the FALSE positive
"you sold this", because an owner who stops believing these alerts is worse off than one who never
got them:

- **T+1 age gate.** A buy is not in ``holdings`` until it settles, so a position is only compared
  once at least ``min_age_sessions`` trading sessions have COMPLETED since it was opened (counted
  strictly between ``opened_at`` and today — the day it was opened and today itself are excluded).
- **``quantity + t1_quantity``.** The unsettled leg IS held; counting only ``quantity`` would page
  the owner about every position bought the previous session.
- **Summed across holdings rows.** Kite returns one row per (symbol, exchange); a name held on both
  NSE and BSE would otherwise read short and alert falsely.
- **Short counts, not just absent.** Held 3 of a tracked 7 is a partial exit that was never
  reported — the ledger's qty is already fiction.

Alerting is once per position per TRADING DAY (in-memory ``{position_id: date}``); the flag is logged
on every run it holds, so the log stays the evidence trail while the phone buzzes once. A restart
re-alerts once — accepted, because the alternative (persisting the dedupe) buys nothing the owner's
own ``/closed`` would not settle.

Log events: ``position_not_in_holdings`` (WARNING, every flagged run), ``holdings_reconcile_done``
(INFO, every run), ``holdings_reconcile_failed`` (WARNING, broker error).
"""

from __future__ import annotations

import sqlite3
from collections.abc import Awaitable, Callable, Mapping
from datetime import date, datetime, time, timedelta
from typing import Any

from pydantic import BaseModel, Field

from engine.core.calendar import NSECalendar
from engine.core.clock import Clock
from engine.core.log import get_logger
from engine.notify import catalog
from engine.notify.catalog import CatalogMessage

_log = get_logger("engine.ops.holdings_reconcile")

#: Typed owner-notification sink (``engine.ops.main.notify``).
NotifyFn = Callable[[CatalogMessage], Awaitable[None]]

#: In-session window for the hourly tick (IST wall-clock, inclusive). Before 09:20 the previous day's
#: settlement may still be landing; after the close the day is the EOD path's business.
RECONCILE_WINDOW: tuple[time, time] = (time(9, 20), time(15, 30))

#: Upper bound on the age-gate day scan, so a position with a corrupt ``opened_at`` far in the past
#: cannot turn the counter into an unbounded loop. Any position older than this is trivially eligible.
_MAX_AGE_SCAN_DAYS = 400

#: The tracked-position scope (§3.6): the platform's own and the owner's confirmed recommendations.
#: ``external`` positions are the owner's alone (R5), MIS never reaches holdings at all, and a PAPER
#: position (§3.2.9, Phase 3) exists only in the simulator — it can never be in real holdings, so
#: including it would page the owner about every paper trade the day paper trading goes live.
_TRACKED_SQL = (
    "SELECT position_id, symbol, qty, opened_at FROM positions "
    "WHERE state='OPEN' AND product='CNC' AND origin IN ('platform','recommended') "
    "AND COALESCE(is_paper, 0) = 0"
)


class HoldingsReconcileResult(BaseModel):
    """One :meth:`HoldingsReconcileJob.run` outcome (evidence, R8).

    ``flagged`` carries POSITION IDs, not symbols: the position id is the identity the alert, the log
    line and the ledger lookup all key on, and two OPEN positions can share a symbol.
    ``error`` set means the broker call failed — ``checked``/``flagged`` are then empty by
    construction, never "nothing is held".
    """

    checked: int = 0
    flagged: list[str] = Field(default_factory=list)
    skipped_young: int = 0
    error: str | None = None


def in_reconcile_window(now: datetime, calendar: NSECalendar) -> bool:
    """True on a trading day inside :data:`RECONCILE_WINDOW` — the hourly tick's whole gate.

    Lives here rather than in the composition root so the condition the scheduler applies is the one
    the tests pin.
    """
    return calendar.is_trading_day(now.date()) and RECONCILE_WINDOW[0] <= now.time() <= RECONCILE_WINDOW[1]


class HoldingsReconcileJob:
    """Compare tracked OPEN CNC positions against the broker's holdings (§3.6). See module docstring.

    Parameters
    ----------
    conn:
        SQLite state connection. READ-ONLY here by design: this job never writes a position, a ledger
        row or a risk state — the owner's ``/closed`` is the only thing allowed to close a human-owned
        position (§6.5).
    kite:
        The :class:`~engine.broker.kite_client.KiteClient` facade; only ``holdings()`` is called. It
        routes through ``KiteClient._call``, so a ``TokenException`` also fires the existing R6
        circuit breaker — this job additionally swallows it (see :meth:`run`).
    clock, calendar:
        The single "now" and the trading-day facts behind the T+1 age gate and the per-day dedupe.
    notify:
        Typed owner sink. A send failure is swallowed: a diagnostic must never raise onto the
        scheduler thread.
    min_age_sessions:
        Completed trading sessions a position must have before it is compared (T+1 settlement).
    """

    def __init__(
        self,
        conn: sqlite3.Connection,
        kite: Any,
        clock: Clock,
        calendar: NSECalendar,
        notify: NotifyFn | None,
        *,
        min_age_sessions: int = 2,
    ) -> None:
        self._conn = conn
        self._kite = kite
        self._clock = clock
        self._calendar = calendar
        self._notify = notify
        self._min_age_sessions = min_age_sessions
        #: position_id → the trading date its alert was last sent on (per-day dedupe, in-memory).
        self._alerted_on: dict[str, date] = {}

    # ------------------------------------------------------------------ run
    async def run(self) -> HoldingsReconcileResult:
        """One reconcile pass. NEVER raises — the check is diagnostic (see the module docstring)."""
        try:
            rows = await self._kite.holdings()
        except Exception as exc:  # noqa: BLE001 - a broker failure must not claim everything was sold
            _log.warning(
                "holdings_reconcile_failed",
                error_type=type(exc).__name__,
                error=str(exc)[:200],
                effect="no comparison this run — tracked positions are unverified, not missing",
            )
            return HoldingsReconcileResult(error=f"{type(exc).__name__}: {exc}")

        held = _held_quantities(rows)
        today = self._clock.today()
        checked = 0
        skipped_young = 0
        flagged: list[str] = []
        for position in self._conn.execute(_TRACKED_SQL).fetchall():
            if self._is_young(position["opened_at"], today):
                skipped_young += 1
                continue
            checked += 1
            symbol = str(position["symbol"] or "")
            tracked = int(position["qty"] or 0)
            held_qty = held.get(symbol, 0)
            if held_qty >= tracked:
                continue
            position_id = str(position["position_id"])
            flagged.append(position_id)
            entry_rec_id = self._entry_rec_id(position_id)
            _log.warning(
                "position_not_in_holdings",
                symbol=symbol,
                position_id=position_id,
                tracked=tracked,
                held=held_qty,
                entry_rec_id=entry_rec_id,
            )
            await self._maybe_alert(
                position_id=position_id, symbol=symbol, tracked=tracked, held=held_qty,
                entry_rec_id=entry_rec_id, today=today,
            )

        _log.info(
            "holdings_reconcile_done",
            checked=checked, flagged=len(flagged), skipped_young=skipped_young,
        )
        return HoldingsReconcileResult(
            checked=checked, flagged=flagged, skipped_young=skipped_young
        )

    # ------------------------------------------------------------------ T+1 age gate
    def _is_young(self, opened_at: Any, today: date) -> bool:
        """True when fewer than ``min_age_sessions`` trading sessions COMPLETED since ``opened_at``.

        Sessions are counted strictly between the open date and today, so neither the opening day nor
        the (incomplete) current one counts. A missing/unparseable ``opened_at`` is treated as OLD:
        such a row is a stale artefact, which is exactly what this check hunts — and skipping it
        would restore the silence.
        """
        opened = _as_date(opened_at)
        if opened is None:
            return False
        if self._min_age_sessions <= 0:
            return False
        sessions = 0
        probe = opened + timedelta(days=1)
        for _ in range(_MAX_AGE_SCAN_DAYS):
            if probe >= today:
                break
            if self._calendar.is_trading_day(probe):
                sessions += 1
                if sessions >= self._min_age_sessions:
                    return False        # old enough — stop counting
            probe += timedelta(days=1)
        return sessions < self._min_age_sessions

    # ------------------------------------------------------------------ the /closed id
    def _entry_rec_id(self, position_id: str) -> str | None:
        """The ENTRY recommendation id for ``position_id``, or ``None`` when the ledger has no row.

        ``entry_px IS NOT NULL`` identifies the entry row (the same discriminator
        ``ops.pipeline.close`` uses); a position whose only ledger rows are exit/adjust recs falls
        back to any of them, since ``/closed`` accepts an exit rec's id too.
        """
        row = self._conn.execute(
            "SELECT rec_id FROM learning_ledger WHERE position_id=? AND entry_px IS NOT NULL "
            "ORDER BY created_at LIMIT 1",
            (position_id,),
        ).fetchone()
        if row is None:
            row = self._conn.execute(
                "SELECT rec_id FROM learning_ledger WHERE position_id=? ORDER BY created_at LIMIT 1",
                (position_id,),
            ).fetchone()
        if row is None or row["rec_id"] is None:
            return None
        return str(row["rec_id"])

    # ------------------------------------------------------------------ one alert per day
    async def _maybe_alert(
        self, *, position_id: str, symbol: str, tracked: int, held: int,
        entry_rec_id: str | None, today: date,
    ) -> None:
        """Send the owner alert unless this position was already alerted on ``today``."""
        if self._notify is None or self._alerted_on.get(position_id) == today:
            return
        self._alerted_on[position_id] = today
        msg = catalog.position_not_in_holdings(
            symbol=symbol, position_id=position_id, tracked_qty=tracked, held_qty=held,
            entry_rec_id=entry_rec_id,
        )
        try:
            await self._notify(msg)
        except Exception:  # noqa: BLE001 - a diagnostic must never be load-bearing, alerting included
            _log.exception("holdings_reconcile_notify_failed", position_id=position_id)


# --------------------------------------------------------------------------- helpers
def _held_quantities(rows: Any) -> dict[str, int]:
    """``{tradingsymbol: quantity + t1_quantity}`` from a Kite ``holdings()`` payload.

    Summed across rows: Kite returns one row per (symbol, exchange), and a name held on both NSE and
    BSE must not read short. Missing/garbage quantity keys read 0 — a malformed row costs its own
    quantity, never the run.
    """
    held: dict[str, int] = {}
    for row in rows or []:
        symbol = _field(row, "tradingsymbol")
        if not symbol:
            continue
        key = str(symbol)
        held[key] = (
            held.get(key, 0)
            + _as_int(_field(row, "quantity"))
            + _as_int(_field(row, "t1_quantity"))
        )
    return held


def _field(row: Any, key: str) -> Any:
    """One holdings field, whether the SDK handed back a mapping or an attribute object."""
    if isinstance(row, Mapping):
        return row.get(key)
    return getattr(row, key, None)


def _as_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _as_date(value: Any) -> date | None:
    """``opened_at`` (ISO-8601 string, or already a date/datetime) as a date; ``None`` if unusable."""
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value)).date()
    except ValueError:
        return None
