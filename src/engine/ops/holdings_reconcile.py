"""Holdings reconcile for tracked CNC positions.

Does the broker still hold each tracked position? One ``holdings()`` call, compared against every
OPEN CNC position with ``origin in ('platform','recommended')`` that is old enough to have settled,
and one owner alert naming the exact ``/closed <entry_rec_id> <price>`` reply. A position the
platform wrongly believes is open keeps consuming the per-name cap.

**Alert-only, never load-bearing.** The platform never auto-closes a human-owned position (the
close price is the owner's fact, outcome labelling), never touches risk state or the prescreen,
and a broker/token failure degrades to a WARNING log plus ``error`` on the result — the check itself
is diagnostic; its only gate input is the two-day journal below, which releases risk-limit count
capacity and never blocks anything. Every design choice is biased against the FALSE positive
"you sold this", because an owner who stops believing these alerts is worse off than one who never
got them:

- **T+1 age gate.** A buy is not in ``holdings`` until it settles, so a position is only compared
  once at least ``min_age_sessions`` trading sessions have COMPLETED since it was opened (counted
  strictly between ``opened_at`` and today — the day it was opened and today itself are excluded).
- **``quantity + t1_quantity + collateral_quantity``.** The unsettled leg IS held. So is the pledged
  leg: Kite moves shares pledged for margin OUT of ``quantity`` into ``collateral_quantity``, so a
  fully pledged position would otherwise read held 0 and silence its own exit path.
- **Summed across holdings rows.** Kite returns one row per (symbol, exchange); a name held on both
  NSE and BSE would otherwise read short and alert falsely.
- **Short counts, not just absent.** Held 3 of a tracked 7 is a partial exit that was never
  reported — the ledger's qty is already fiction.

Alerting is once per position per TRADING DAY (in-memory ``{position_id: date}``); the flag is logged
on every run it holds. A restart re-alerts once — accepted, persisting the dedupe buys nothing the
owner's own ``/closed`` would not settle.

**The observation journal.** Every CHECKED position also writes one ``holdings_observations`` row per
trading day (upsert, last write of the day wins — migration 0013).
:func:`positions_missing_from_holdings` reads it as "the two most recent observation days both read
SHORT". Every caller uses that set to STOP DOING THINGS — the position-event path and the
analyst positions summary stop treating the position as held, the pre-open planner moves it out
of the overnight-risk line into its own "sold outside the ledger" block, protection reminders stop
nagging, and the gate drops it from the risk-limit position/sector counts (``gate._gone_symbols``, a
deliberate capacity release). None of them closes anything: only the owner's ``/closed`` ends a
human-owned position. The authority granted is silence plus that count release, never a
position-state change — which is why the missing rule needs TWO days: one bad observation (a
settlement edge, a symbol rename, a partial broker payload) must never silence a real position.

Two guards on that silence:

- **Only TRADING days are journalled.** The hourly scheduler gates ``run`` by window, but the
  post-login ladder calls it unconditionally; without this a Saturday deploy could write the second
  "day" of a streak on one session's evidence. On a non-trading ``today`` the observation is skipped
  (the ALERT still fires), so ``MissingHolding.sessions`` counts sessions.
- **"Short" and "gone" are different questions.** ``held < tracked`` is the right trigger for an
  ALERT and for the day plan's block. It is NOT enough to stop recommending an exit: residual
  exposure still needs a stop. Callers that convert the journal into SILENCE on a risk-reducing path
  pass ``require_zero=True`` (:data:`MissingHolding.zero_sessions`): every day of the run must read
  the broker holding NOTHING.

Log events: ``position_not_in_holdings`` (WARNING, every flagged run), ``holdings_reconcile_done``
(INFO, every run), ``holdings_reconcile_failed`` (WARNING, broker error),
``holdings_observations_write_failed`` (WARNING, journal write failed — never fatal),
``holdings_reconcile_all_short`` (WARNING, ≥2 tracked positions checked and NOT ONE of them held —
the signature of a broken holdings feed rather than of an owner who sold).
"""

from __future__ import annotations

import sqlite3
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Any

from pydantic import BaseModel, Field

from engine.core.calendar import NSECalendar
from engine.core.clock import Clock
from engine.core.db import transaction
from engine.core.log import get_logger
from engine.core.scope import scope_sql
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

#: The tracked-position scope: the platform's own and the owner's confirmed recommendations.
#: ``external`` positions are the owner's alone, MIS never reaches holdings, and a PAPER position
#: can never be in real holdings (it would page the owner about every paper trade).
_TRACKED_SQL = (
    "SELECT position_id, symbol, qty, opened_at FROM positions "
    f"WHERE state='OPEN' AND product='CNC' AND {scope_sql('real', has_origin=True)}"
)

#: One row per (position, trading day); re-running inside the same day REPLACES the day's reading,
#: because the answer can legitimately change between the 09:20 and the 15:20 pulse (migration 0013).
_OBSERVATION_UPSERT = (
    "INSERT INTO holdings_observations (position_id, d, tracked_qty, held_qty, observed_at) "
    "VALUES (?, ?, ?, ?, ?) "
    "ON CONFLICT(position_id, d) DO UPDATE SET "
    "tracked_qty=excluded.tracked_qty, held_qty=excluded.held_qty, observed_at=excluded.observed_at"
)

#: Consecutive most-recent observation days that must read SHORT before a position is treated as sold
#: outside the ledger, and how far back an observation may be and still count. Defaults for
#: :func:`positions_missing_from_holdings`; both are parameters so a caller can be stricter.
MISSING_SESSIONS = 2
MISSING_LOOKBACK_DAYS = 7

#: Tracked positions that must be checked before "every one reads short" gets its own log line: two
#: or more with none held is the shape of a broken holdings FEED, not of an owner who sold.
_ALL_SHORT_MIN_POSITIONS = 2


class HoldingsReconcileResult(BaseModel):
    """One :meth:`HoldingsReconcileJob.run` outcome (evidence).

    ``flagged`` carries POSITION IDs, not symbols: two OPEN positions can share a symbol.
    ``error`` set means the broker call failed — ``checked``/``flagged`` are then empty by
    construction, never "nothing is held".
    """

    checked: int = 0
    flagged: list[str] = Field(default_factory=list)
    skipped_young: int = 0
    #: ``holdings_observations`` rows written — equal to ``checked`` unless the journal write failed
    #: (logged and swallowed).
    observed: int = 0
    error: str | None = None


def in_reconcile_window(now: datetime, calendar: NSECalendar) -> bool:
    """True on a trading day inside :data:`RECONCILE_WINDOW` — the hourly tick's whole gate.

    Lives here rather than in the composition root so the condition the scheduler applies is the one
    the tests pin.
    """
    return calendar.is_trading_day(now.date()) and RECONCILE_WINDOW[0] <= now.time() <= RECONCILE_WINDOW[1]


class HoldingsReconcileJob:
    """Compare tracked OPEN CNC positions against the broker's holdings. See module docstring.

    Parameters
    ----------
    conn:
        SQLite state connection. The ONLY table this job writes is ``holdings_observations`` (its own
        evidence journal): it never writes a position, a ledger row, a recommendation or a risk
        state — the owner's ``/closed`` is the only thing allowed to close a human-owned position.
    kite:
        The :class:`~engine.broker.kite_client.KiteClient` facade; only ``holdings()`` is called. It
        routes through ``KiteClient._call``, so a ``TokenException`` also fires the existing token
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
        # Only the hourly tick applies ``in_reconcile_window``; the post-login ladder
        # (``post_login._step_holdings``) does not. A non-trading day must not contribute an
        # observation DAY (see the module docstring); comparison, log and alert still happen.
        journalling = self._calendar.is_trading_day(today)
        observed_at = self._clock.now().isoformat()
        checked = 0
        skipped_young = 0
        flagged: list[str] = []
        #: (position_id, d, tracked, held, observed_at) for every CHECKED position — written in one
        #: transaction after the loop so no DB write straddles the ``await`` on the notify seam.
        observations: list[tuple[str, str, int, int, str]] = []
        for position in self._conn.execute(_TRACKED_SQL).fetchall():
            if self._is_young(position["opened_at"], today):
                # Not OBSERVED either: journalling "held 0" for a T+1 leg would manufacture the
                # two-day streak that :func:`positions_missing_from_holdings` reads as a sale.
                skipped_young += 1
                continue
            checked += 1
            symbol = str(position["symbol"] or "")
            tracked = int(position["qty"] or 0)
            held_qty = held.get(symbol, 0)
            if journalling:
                observations.append(
                    (str(position["position_id"]), today.isoformat(), tracked, held_qty, observed_at)
                )
            if held_qty >= tracked:
                continue
            position_id = str(position["position_id"])
            flagged.append(position_id)
            # ``rec_id``, not ``entry_rec_id``: a local of that name would shadow the module-level function.
            rec_id = entry_rec_id(self._conn, position_id)
            _log.warning(
                "position_not_in_holdings",
                symbol=symbol,
                position_id=position_id,
                tracked=tracked,
                held=held_qty,
                entry_rec_id=rec_id,
            )
            await self._maybe_alert(
                position_id=position_id, symbol=symbol, tracked=tracked, held=held_qty,
                entry_rec_id=rec_id, today=today,
            )

        if checked >= _ALL_SHORT_MIN_POSITIONS and len(flagged) == checked:
            # The systemic signature a per-position alert cannot show: a SUCCESSFUL holdings call that
            # returned nothing useful (API shape change, truncated page, token on the wrong account)
            # looks, position by position, like "the owner sold everything" — and two such runs
            # silence the position-event exit path on the whole book. The journal and per-position
            # alerts are unchanged; this line says "look at the FEED, not at the positions".
            _log.warning(
                "holdings_reconcile_all_short",
                checked=checked, holdings_rows=len(held),
                effect="every tracked position reads short in one run — verify the holdings feed "
                       "before trusting the sold-outside-the-ledger screens",
            )
        observed = self._journal(observations)
        _log.info(
            "holdings_reconcile_done",
            checked=checked, flagged=len(flagged), skipped_young=skipped_young, observed=observed,
            trading_day=journalling,
        )
        return HoldingsReconcileResult(
            checked=checked, flagged=flagged, skipped_young=skipped_young, observed=observed
        )

    # ------------------------------------------------------------------ the observation journal
    def _journal(self, observations: list[tuple[str, str, int, int, str]]) -> int:
        """Upsert this run's observations. Returns how many rows were written.

        Swallows its own failure: a failed write costs a day of evidence, never the alert or the
        scheduler thread, and leaves that day without an observation — "not missing", the safe side.
        """
        if not observations:
            return 0
        try:
            with transaction(self._conn):
                self._conn.executemany(_OBSERVATION_UPSERT, observations)
        except sqlite3.Error as exc:
            _log.warning(
                "holdings_observations_write_failed",
                rows=len(observations), error_type=type(exc).__name__, error=str(exc)[:200],
            )
            return 0
        return len(observations)

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


# --------------------------------------------------------------------------- the /closed id
def entry_rec_id(conn: sqlite3.Connection, position_id: str) -> str | None:
    """The ENTRY recommendation id for ``position_id``, or ``None`` when the ledger has no row.

    ``entry_px IS NOT NULL`` identifies the entry row (the same discriminator ``ops.pipeline.close``
    uses); a position whose only ledger rows are exit/adjust recs falls back to any of them, since
    ``/closed`` accepts an exit rec's id too.

    Module-level so the owner alert and the pre-open day plan name the same id.
    """
    row = conn.execute(
        "SELECT rec_id FROM learning_ledger WHERE position_id=? AND entry_px IS NOT NULL "
        "ORDER BY created_at LIMIT 1",
        (position_id,),
    ).fetchone()
    if row is None:
        row = conn.execute(
            "SELECT rec_id FROM learning_ledger WHERE position_id=? ORDER BY created_at LIMIT 1",
            (position_id,),
        ).fetchone()
    if row is None or row["rec_id"] is None:
        return None
    return str(row["rec_id"])


# --------------------------------------------------------------------------- "sold outside the ledger"
@dataclass(frozen=True)
class MissingHolding:
    """One position's standing in the observation journal.

    ``sessions`` is the length of the UNBROKEN run of SHORT (``held < tracked``) observation days
    ending at the most recent one inside the lookback — the moment a day reads fully held the run is
    over, so a position the owner still holds can never accumulate a streak.
    ``tracked_qty``/``held_qty`` come from that most recent observation (a partial exit reads short
    with a non-zero held quantity).

    ``zero_sessions`` is the stricter sub-run: consecutive most-recent days on which the broker held
    NOTHING. Always ``<= sessions``; they differ exactly when residual exposure remains (an unreported
    partial exit). "Short" is enough to TELL the owner; only "gone" is enough to stop managing the
    position's risk (``require_zero``). Pledged shares count as held, so a pledge is neither.
    """

    position_id: str
    sessions: int
    zero_sessions: int
    d: str
    tracked_qty: int
    held_qty: int


def missing_holdings_observations(
    conn: sqlite3.Connection, today: date, *, lookback_days: int = MISSING_LOOKBACK_DAYS
) -> dict[str, MissingHolding]:
    """``position_id -> MissingHolding`` for every position whose LATEST observation reads short.

    Pure SQL + Python: no broker call, no clock — ``today`` is passed in. Observations stamped after
    ``today`` (clock-skewed or replayed) are ignored; the window is ``[today - lookback_days, today]``
    inclusive, and ISO-8601 dates compare lexicographically.

    A position with NO observation in the window is absent from the result — never "missing". This
    asymmetry is load-bearing: an engine that was down, a broker that erred all week, or a fresh
    position produces silence, and silence means the platform keeps managing the position normally.
    """
    if lookback_days < 0:
        lookback_days = 0
    floor = (today - timedelta(days=lookback_days)).isoformat()
    rows = conn.execute(
        "SELECT position_id, d, tracked_qty, held_qty FROM holdings_observations "
        "WHERE d >= ? AND d <= ? ORDER BY position_id, d DESC",
        (floor, today.isoformat()),
    ).fetchall()
    by_position: dict[str, list[Any]] = {}
    for row in rows:
        by_position.setdefault(str(row["position_id"]), []).append(row)
    out: dict[str, MissingHolding] = {}
    for position_id, observations in by_position.items():
        streak = 0
        zero_streak = 0
        zero_run_live = True
        for row in observations:                      # already newest-first
            held_qty = int(row["held_qty"] or 0)
            if held_qty >= int(row["tracked_qty"] or 0):
                break
            streak += 1
            # The all-zero run is a PREFIX of the short run: the first day the broker held anything
            # ends it while the short run continues.
            if zero_run_live and held_qty == 0:
                zero_streak += 1
            else:
                zero_run_live = False
        if streak == 0:
            continue
        latest = observations[0]
        out[position_id] = MissingHolding(
            position_id=position_id,
            sessions=streak,
            zero_sessions=zero_streak,
            d=str(latest["d"]),
            tracked_qty=int(latest["tracked_qty"] or 0),
            held_qty=int(latest["held_qty"] or 0),
        )
    return out


def positions_missing_from_holdings(
    conn: sqlite3.Connection,
    today: date,
    *,
    sessions: int = MISSING_SESSIONS,
    lookback_days: int = MISSING_LOOKBACK_DAYS,
    require_zero: bool = False,
) -> dict[str, MissingHolding]:
    """``position_id -> MissingHolding`` for every position whose ``sessions`` most recent observation
    days inside the lookback ALL read short.

    The platform's "the owner sold this outside the ledger" predicate — a filtered VIEW of
    :func:`missing_holdings_observations`, returning the same :class:`MissingHolding` values. Two
    days rather than one because a single short reading has innocent explanations (a settlement
    edge, a symbol rename, a truncated payload) and every consequence is going quiet about a
    position — silence about a really-open position is the expensive error.

    ``require_zero=True`` narrows "short" to "the broker holds NOTHING" on every day of the run
    (:attr:`MissingHolding.zero_sessions`). A caller whose consequence is INFORMATION (the day-plan
    block; the owner alert applies its own held-vs-tracked check) wants the wide reading. A caller
    whose consequence is SILENCE ON A RISK-REDUCING PATH (the position-event screen) must use the
    narrow one: a position with residual exposure still needs its stop watched.

    ``sessions < 1`` returns the empty dict rather than "everything": a mis-configured 0 must not
    silence the platform about every position it tracks.
    """
    if sessions < 1:
        return {}
    return {
        position_id: missing
        for position_id, missing in missing_holdings_observations(
            conn, today, lookback_days=lookback_days
        ).items()
        if (missing.zero_sessions if require_zero else missing.sessions) >= sessions
    }


# --------------------------------------------------------------------------- helpers
def _held_quantities(rows: Any) -> dict[str, int]:
    """``{tradingsymbol: quantity + t1_quantity + collateral_quantity}`` from a Kite ``holdings()``
    payload.

    Summed across rows: Kite returns one row per (symbol, exchange), and a name held on both NSE and
    BSE must not read short. ``collateral_quantity`` is the PLEDGED leg: Kite moves pledged shares
    out of ``quantity`` into that bucket (see the module docstring). ``authorised_quantity`` is deliberately NOT added — it is the CDSL-authorised subset of
    ``quantity`` (a flag on shares already counted), not a separate holding. Missing/garbage
    quantity keys read 0 — a malformed row costs its own quantity, never the run.
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
            + _as_int(_field(row, "collateral_quantity"))
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
