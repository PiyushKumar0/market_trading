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
- **``quantity + t1_quantity + collateral_quantity``.** The unsettled leg IS held; counting only
  ``quantity`` would page the owner about every position bought the previous session. The pledged
  leg IS held too (2026-09-12 review): Kite moves shares pledged for margin OUT of ``quantity`` and
  into ``collateral_quantity``, so a fully pledged position would otherwise read held 0 on every
  run and, two sessions later, silence its own exit path.
- **Summed across holdings rows.** Kite returns one row per (symbol, exchange); a name held on both
  NSE and BSE would otherwise read short and alert falsely.
- **Short counts, not just absent.** Held 3 of a tracked 7 is a partial exit that was never
  reported — the ledger's qty is already fiction.

Alerting is once per position per TRADING DAY (in-memory ``{position_id: date}``); the flag is logged
on every run it holds, so the log stays the evidence trail while the phone buzzes once. A restart
re-alerts once — accepted, because the alternative (persisting the dedupe) buys nothing the owner's
own ``/closed`` would not settle.

**The observation journal (WO-D2, 2026-09-12).** Every CHECKED position also writes one
``holdings_observations`` row per trading day (upsert, last write of the day wins — see migration
0013). That turns a per-run in-memory answer into a durable series, and
:func:`positions_missing_from_holdings` reads it as "the two most recent observation days both read
SHORT". Two consumers use that set to STOP DOING THINGS — the §5.2(b) position-event path stops
spending analyst calls on the position, and the §5.3 pre-open planner moves it out of the
overnight-risk line into its own "sold outside the ledger" block. Neither closes anything: §3.6 stays
alert-only for CLOSING, and only the owner's ``/closed`` ends a human-owned position (§6.5). The
authority this table grants is silence, never a state change — which is also why the missing rule
needs TWO days: one bad observation (a settlement edge, a symbol rename, a partial broker payload)
must never be enough to silence the platform about a real position.

Two guards on that silence, both added by the WO-D2 fix pass (2026-09-12):

- **Only TRADING days are journalled.** ``run`` is window-gated when the hourly scheduler calls it,
  but the §2.6 post-login ladder calls it unconditionally — so a Saturday deploy used to be able to
  write the second "day" of a two-day streak on a single trading session's worth of evidence. The
  observation is now skipped (the ALERT still fires) when ``today`` is not a trading day, so every
  row in the table is a session and ``MissingHolding.sessions`` counts what its name says.
- **"Short" and "gone" are different questions.** ``held < tracked`` is the right trigger for an
  ALERT and for the day plan's block — a partial exit the owner never reported means the ledger's
  qty is already fiction. It is NOT enough to stop recommending an exit: residual exposure still
  needs a stop. Callers that convert this journal into SILENCE on a risk-reducing path therefore
  pass ``require_zero=True`` (:data:`MissingHolding.zero_sessions`), which demands that every day of
  the qualifying run read the broker holding NOTHING. (A pledged holding used to be the other
  "reads short forever" case; it is closed at the source now that ``collateral_quantity`` counts as
  held, so ``require_zero`` guards the partial exit and nothing else.)

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

#: Tracked positions that must be checked before "every one of them reads short" is worth its own
#: log line. One position reading short IS the ordinary flagged case; two or more, with none held,
#: is the shape of a broken holdings FEED rather than of an owner who sold.
_ALL_SHORT_MIN_POSITIONS = 2


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
    #: ``holdings_observations`` rows written by this run (WO-D2) — equal to ``checked`` unless the
    #: journal write itself failed, which is logged and swallowed (the reconcile is diagnostic).
    observed: int = 0
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
        SQLite state connection. The ONLY table this job writes is ``holdings_observations`` (its own
        evidence journal, WO-D2): it never writes a position, a ledger row, a recommendation or a risk
        state — the owner's ``/closed`` is the only thing allowed to close a human-owned position
        (§6.5).
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
        # The §2.6 post-login ladder runs this job unconditionally (``post_login._step_holdings``);
        # only the hourly scheduler tick applies ``in_reconcile_window``. A weekend/holiday boot must
        # therefore not contribute an observation DAY: two rows, one of them a Saturday deploy, would
        # satisfy the two-session missing rule on a single session's evidence. The comparison, the
        # log line and the owner alert all still happen — only the durable row is withheld.
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
                # A young position is not OBSERVED either: the comparison was never made, and
                # journalling "held 0" for a T+1 leg would manufacture exactly the two-day streak
                # that :func:`positions_missing_from_holdings` treats as proof of a sale.
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
            # Named ``rec_id`` rather than ``entry_rec_id``: since WO-D2 the latter is a MODULE-LEVEL
            # function, and a local of that name would shadow it for the whole of this method.
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
            # The systemic signature a per-position alert cannot show: a SUCCESSFUL holdings call
            # that returned nothing useful (an API shape change, a truncated page, a fresh token on
            # the wrong account) is indistinguishable, position by position, from "the owner sold
            # everything" — and since WO-D2 two such runs silence the §5.2(b) exit path on the whole
            # tracked book at once. Not a behaviour change: the journal is still written and the
            # owner is still alerted per position (loudly — one message each, plus a day-plan block
            # naming every one of them). This is the one line that says "look at the FEED, not at
            # the positions". The threshold is 2 because one tracked position reading short is just
            # the ordinary flagged case.
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
        """Upsert this run's observations (WO-D2). Returns how many rows were written.

        Swallows its own failure: the journal exists to let OTHER code stay quiet, so a write that
        fails costs a day of evidence, never the reconcile's alert (which has already been sent by
        the time this runs) and never the scheduler thread. A failed write simply leaves that day
        without an observation — which reads as "not missing", the safe side of this rule.
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

    Module-level since WO-D2: the owner alert and the pre-open day plan both name the id the owner
    must type, and two lookups that could disagree about WHICH id that is would be worse than none.
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
    """One position's standing in the observation journal (WO-D2).

    ``sessions`` is the length of the UNBROKEN run of SHORT (``held < tracked``) observation days
    ending at the most recent one inside the lookback — the moment a day reads fully held the run is
    over, so a position the owner still holds can never accumulate a streak.
    ``tracked_qty``/``held_qty`` come from that most recent observation, so the day plan can state
    what the broker actually showed rather than assuming zero (a partial exit reads short with a
    non-zero held quantity).

    ``zero_sessions`` is the stricter sub-run: consecutive most-recent days on which the broker held
    NOTHING. It is always ``<= sessions``, and the two differ exactly when the position still has
    residual exposure — a partial exit the owner never reported. "Short" is enough to TELL the
    owner something; only "gone" is enough to stop managing the position's risk, which is why
    :func:`positions_missing_from_holdings` takes ``require_zero``. (Shares pledged for margin are
    counted as held — Kite reports them under ``collateral_quantity`` — so a pledge is neither
    short nor gone here.)
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

    Pure SQL + Python: no broker call, no clock — ``today`` is passed in, so a caller inside the
    session and the 08:50 planner ask the same question of the same window. Observations stamped
    after ``today`` (a clock-skewed or replayed write) are ignored rather than trusted; the window is
    ``[today - lookback_days, today]`` inclusive, and ISO-8601 dates compare lexicographically.

    A position with NO observation in the window is absent from the result — never "missing". That is
    the load-bearing asymmetry of this whole feature: an engine that was down, a broker that erred
    all week, or a fresh position produces silence here, and silence means the platform keeps
    managing the position normally.
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
            # ends it, even though the short run continues. Counted in the same pass so the two can
            # never disagree about which days they looked at.
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

    The platform's "the owner sold this outside the ledger" predicate (WO-D2) — a filtered VIEW of
    :func:`missing_holdings_observations`, returning the same :class:`MissingHolding` values so a
    caller that needs the streak/quantities behind the filter (the §5.2 positions summary, the §5.3
    day-plan block) never has to read the journal a second time to get them. Two days rather than one because
    a single short reading has innocent explanations (a settlement edge, a symbol rename, a truncated
    holdings payload) and the consequences here are all forms of going quiet about a position —
    silence about a position that really is open is the expensive error.

    ``require_zero=True`` narrows "short" to "the broker holds NOTHING" on every day of the run
    (:attr:`MissingHolding.zero_sessions`). A caller whose consequence is INFORMATION — the §5.3
    day-plan block (the §3.6 alert applies its own held-vs-tracked check and never calls this) —
    wants the wide reading: held 3 of a tracked 7 is a real
    unreported exit and the owner should hear about it. The caller whose consequence is SILENCE ON A
    RISK-REDUCING PATH — the §5.2(b) position-event screen — must use the narrow one, because a
    position with residual exposure (a partial exit) still needs its stop watched. Withholding an
    exit recommendation from a position that exists is the failure mode this flag exists to prevent.
    (A holding pledged for margin is counted as held via ``collateral_quantity`` and never enters
    this dict at all.)

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
    BSE must not read short. ``collateral_quantity`` is the PLEDGED leg (2026-09-12 review): Kite
    moves shares pledged for margin out of ``quantity`` into that bucket, so without it a fully
    pledged position reads held 0 on every run and two sessions later silences its own exit path.
    ``authorised_quantity`` is deliberately NOT added — it is the CDSL-authorised subset of
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
