"""``brk20`` RETEST re-arm — the broken level RESTS for N trading sessions.

The pre-registered ``brk20`` entry-mechanism backtest (``scripts/backtest_brk20.py``) selected
``limit_5_sessions``: a limit resting at the candidate's own ``raw_levels.entry``
(``round_to_tick(H20)``) for up to FIVE sessions, filled on the first session whose low reaches it.
``brk20`` itself publishes the level exactly ONCE (the session after the crossing, under the
pre-screen's same-day ``(symbol, strategy)`` dedupe), and a wide breakout margin — the highest
score — is what puts that level far below the live price and kills it on ``entry_sanity_band``.

So every ADMITTED ``brk20`` candidate's level is journalled here on its publication session
``signal_d`` (y+1, the session after the crossing y) and stays DUE for the next N−1 trading
sessions, i.e. it is offered on y+1..y+N. On each re-offer session, while the market is open and
inside the owner trade window, the level is re-offered for judgement on the FIRST minute the live
price is back inside the ``entry_sanity_band`` (same band, same hash-verified limits table).

What it does NOT do:

* **No new entry price.** The re-publication carries the ORIGINAL ``raw_levels`` and score verbatim.
* **No widened band.** The band test is a NARROWING of the gate's own ``entry_sanity_band``; nothing
  here can make a candidate the gate would reject acceptable.
* **No new admission capacity.** Every re-publication goes through the SAME ``prescreen.admit``;
  the day cap, per-strategy cap and same-day dedupe bind identically.
* **No expectancy claim.** ``brk20`` remains exploratory; the backtest chose a mechanism, promoted
  no rule, and ran on the survivorship-tainted eligible-universe proxy the ``hi52`` study carries.
* **No wider population, no stale trigger.** A re-offer is refused for any symbol the caller's
  ``skip_fn`` names (see :meth:`due`) and for any level whose live price is older than the
  ``stale_data_guard.max_tick_age_s`` the gate will judge it by. Both fail to zero.

FRESH PRICE. ``ltp_fn`` is the engine's tick cache, which has no upper bound on age and
``mark_price`` will not say so. Here a stale LTP would SPEND a prescreen day slot (elsewhere it only
defers or fails closed at the gate), so :meth:`due` takes the age seam (``main.tick_age_s``) beside
the price and applies the gate's own ``max_tick_age_s``.

ONE TRADE PER LEVEL. The measured mechanism books one trade per signal. A level that already
produced a position or an unactioned entry recommendation is not re-offered
(``gate._rule_per_stock_exposure`` would reject it as ``already held or pending``, a HARD reason,
after the slot and an analyst call were spent). The caller owns that verdict; this module owns only
the refusal.

BAND SCREEN (``pipeline.BAND_SKIP_STRATEGIES``). That screen defers a queued candidate WITHIN a day,
but only while it is in today's forward queue, which empties at its TTL. This book re-arms ACROSS
days from durable state. The two are complements; both read the same band.

DURABLE. State lives in SQLite (migration 0014), not process memory: the engine reboots inside the
session, and a multi-session mechanism that loses state on restart degenerates to the single-session one.

FAIL TO ZERO. Nothing here raises. A failed journal read, an unparseable row, a throwing ``ltp_fn``
or ``skip_fn``, a calendar past its verified horizon — each costs at most its own row (or the whole
tick) and never the drain tick it is called from; the worst outcome is a level not re-offered today.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Callable, Mapping
from datetime import date
from decimal import ROUND_HALF_UP, Decimal
from typing import TYPE_CHECKING

from ulid import ULID

from engine.core.log import get_logger
from engine.strategy.scanners import brk20
from engine.strategy.types import RawLevels, SignalCandidate

if TYPE_CHECKING:  # pragma: no cover - typing only; the runtime seam is duck-typed
    from engine.core.calendar import NSECalendar
    from engine.core.clock import Clock

_log = get_logger("engine.strategy.retest")

#: Default retest window, in TRADING sessions counting ``signal_d`` (y+1..y+N): the registered
#: 5-session-limit. Owner-only and deliberately NOT learnable — the number IS the registered
#: mechanism; other N were never measured.
DEFAULT_RETEST_SESSIONS = 5

STATUS_RESTING = "resting"
STATUS_EXPIRED = "expired"

_HUNDRED = Decimal("100")
#: Log-only rounding for the deviation percentage: the COMPARISON is exact Decimal.
_PAISA = Decimal("0.01")


class RestingLevelBook:
    """The durable set of ``brk20`` levels still resting, and the daily re-offer that reads it.

    Four operations, each total (never raises):

    * :meth:`record` — journal one ADMITTED ``brk20`` candidate's level on its publication session.
      Idempotent per ``(symbol, signal_d)``.
    * :meth:`expire` — mark rows past their window ``expired``. Housekeeping only; :meth:`due`
      does not depend on it (its own predicate is ``expires_d >= today``).
    * :meth:`due` — the levels whose price is back inside the band today, as ready-to-admit
      :class:`SignalCandidate` objects.
    * :meth:`resting_symbols` — the symbols the FEED must carry today for :meth:`due` to band them.

    ``sessions`` is the window in TRADING sessions; ``calendar`` resolves it (``+N days`` would
    shorten it across weekends and holidays).
    """

    def __init__(
        self,
        conn: sqlite3.Connection,
        calendar: NSECalendar,
        clock: Clock,
        *,
        sessions: int = DEFAULT_RETEST_SESSIONS,
    ) -> None:
        self._conn = conn
        self._calendar = calendar
        self._clock = clock
        # Clamp to >=1: a mis-set knob degrades to N=1 (offered on signal_d only), so every row's
        # expires_d is on or after its signal_d.
        self._sessions = max(1, int(sessions))
        # Per-DAY re-offer bound, keyed by SYMBOL, in process memory on purpose (see :meth:`due`).
        self._offered_day: date | None = None
        self._offered: set[str] = set()
        # Per-DAY log bound for refusal lines (re-evaluated every 60 s pulse); rolled with `_offered`.
        self._logged: set[tuple[str, str]] = set()

    # ------------------------------------------------------------------ write path
    def record(self, candidate: SignalCandidate) -> bool:
        """Journal ``candidate``'s level as resting from TODAY. Returns True when a row was written.

        Called for every ``brk20`` candidate the sweep ADMITTED, not raw: resting raw fires would
        re-offer levels the caps refused the first time.

        Idempotent per ``(symbol, signal_d)`` via ``ON CONFLICT DO NOTHING``; the FIRST row's
        ``created_at`` and expiry stand.

        A non-``brk20`` candidate is refused and logged: a row from another leg would be
        re-published under ``brk20``'s strategy_id downstream.
        """
        if candidate.strategy_id != brk20.STRATEGY_ID:
            _log.warning("brk20_retest_record_rejected", symbol=candidate.symbol,
                         strategy_id=candidate.strategy_id,
                         reason="the resting book is brk20-only (the level/retest semantics are its)")
            return False
        try:
            signal_d = self._clock.today()
            expires_d = self._expiry(signal_d)
            now = self._clock.now().isoformat()
            levels = candidate.raw_levels
            # Bare execute on the autocommit connection (``isolation_level=None``), like the other
            # journal writers: an explicit BEGIN could land inside one the OMS already opened.
            cur = self._conn.execute(
                "INSERT INTO brk20_resting_levels "
                "(symbol, signal_d, entry, stop, target, score, expires_d, status, "
                " created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT (symbol, signal_d) DO NOTHING",
                (
                    candidate.symbol,
                    signal_d.isoformat(),
                    str(levels.entry),
                    None if levels.stop is None else str(levels.stop),
                    None if levels.target is None else str(levels.target),
                    float(candidate.score),
                    expires_d.isoformat(),
                    STATUS_RESTING,
                    now,
                    now,
                ),
            )
        except Exception as exc:  # noqa: BLE001 - journalling never kills the sweep that calls it
            _log.warning("brk20_retest_record_failed", symbol=candidate.symbol, error=str(exc))
            return False
        # `rowcount` is -1 when undeterminable and `bool(-1)` is True: compare, don't truth-test.
        written = (cur.rowcount or 0) > 0
        if written:
            _log.info("brk20_retest_recorded", symbol=candidate.symbol,
                      signal_d=signal_d.isoformat(), expires_d=expires_d.isoformat(),
                      entry=str(levels.entry), sessions=self._sessions)
        return written

    def expire(self, today: date) -> int:
        """Mark every resting row whose window CLOSED before ``today`` as expired. Returns the count.

        Strictly ``expires_d < today``: the expiry session is the LAST on which the level may be
        re-offered (``due`` is inclusive there), so expiring it on its own day ships an N-1 window.

        Housekeeping, never a gate: :meth:`due` re-tests ``expires_d >= today`` itself.
        """
        try:
            cur = self._conn.execute(
                "UPDATE brk20_resting_levels SET status = ?, updated_at = ? "
                "WHERE status = ? AND expires_d < ?",
                (STATUS_EXPIRED, self._clock.now().isoformat(), STATUS_RESTING, today.isoformat()),
            )
        except Exception as exc:  # noqa: BLE001 - bookkeeping; it never kills the drain tick
            _log.warning("brk20_retest_expire_failed", d=today.isoformat(), error=str(exc))
            return 0
        n = max(0, int(cur.rowcount or 0))
        if n:
            _log.info("brk20_retest_expired", d=today.isoformat(), rows=n)
        return n

    # ------------------------------------------------------------------ read path
    def resting_symbols(self, today: date) -> list[str]:
        """Symbols with a level that may be re-offered TODAY — the feed's subscription list.

        The mechanism depends on this. :meth:`due` bands the level against a LIVE price from the tick
        cache, but ``brk20`` originates over the whole ELIGIBLE set while the feed carries only the
        included watchlist, and the sweep's ``_batch_ticks`` subscription ROLLS AT MIDNIGHT. Without
        this list a level on a sub-cap symbol reads ``ltp is None`` on retest days 2..5 and is never
        offered. The sweep adds it to its per-day subscription set.

        Same window predicate as :meth:`due` (``signal_d < today <= expires_d``), minus the price
        (today's own admissions are already in the sweep's batch). Deterministic order; ``[]`` on
        any failure — an unsubscribed symbol costs a re-offer, never the sweep.
        """
        try:
            rows = self._conn.execute(
                "SELECT DISTINCT symbol FROM brk20_resting_levels "
                "WHERE status = ? AND signal_d < ? AND expires_d >= ? ORDER BY symbol ASC",
                (STATUS_RESTING, today.isoformat(), today.isoformat()),
            ).fetchall()
        except Exception as exc:  # noqa: BLE001 - a feed hint is never worth a sweep
            _log.warning("brk20_retest_symbols_failed", d=today.isoformat(), error=str(exc))
            return []
        return [str(r["symbol"]) for r in rows]

    def due(
        self,
        ltp_fn: Callable[[str], Decimal | float | None],
        band_pct: float | Decimal,
        today: date,
        *,
        tick_age_fn: Callable[[str], float | None] | None = None,
        max_tick_age_s: float | Decimal | None = None,
        skip_fn: Callable[[list[str]], Mapping[str, str]] | None = None,
        offers: dict[str, dict[str, str]] | None = None,
    ) -> list[SignalCandidate]:
        """The resting levels whose live price is back inside the band today, as fresh candidates.

        Window: ``signal_d < today <= expires_d``. Strictly after ``signal_d`` (published that day;
        the pre-screen's same-day dedupe owns it), inclusive at the far end: a 5-session window is
        ``signal_d`` plus four re-offer sessions (y+2..y+5).

        Band: ``abs(entry - ltp) / ltp * 100 <= band_pct``, the ``entry_sanity_band`` formula and
        its ``<=`` edge verbatim. ``ltp_fn`` returning None or a non-positive price publishes
        NOTHING: this mechanism SPENDS a scarce analyst slot, so an unpriceable symbol stays silent.

        Freshness: with ``tick_age_fn`` and ``max_tick_age_s`` supplied (the engine passes
        ``main.tick_age_s`` and ``stale_data_guard.max_tick_age_s``), a level whose price has no age
        or is OLDER than the bound is not offered. Omitting either argument disables the test, which
        is why the wiring pins both.

        Not-offerable screen: ``skip_fn`` is handed today's due symbols ONCE (only when a row is in
        the window) and returns ``{symbol: reason}`` for those that must not be re-offered: held or
        PENDING-recommendation symbols (the gate would reject them as HARD ``already held or
        pending``), symbols with an ex-date inside brk20's ex-date-skip horizon (the stored level is
        a pre-ex price), and symbols that left the eligible universe. A ``skip_fn`` that RAISES
        offers nothing this tick.

        ``offers``, when supplied, is filled in place with ``{signal_id: {symbol, entry, ltp, dev,
        signal_d}}`` for every candidate returned, so the CALLER can log
        ``brk20_retest_republished`` for those ``prescreen.admit`` actually accepted.

        The candidate carries the ORIGINAL ``raw_levels`` and score, a FRESH ``signal_id`` (a new
        publication is a new signal) and NO ``features_snapshot_id``: the caller mints it AFTER
        ``prescreen.admit`` (``ops.main._attach_feature_snapshots``) so a suppressed candidate
        never spends a snapshot write.

        The row STAYS ``resting``: a re-publication that expires unactioned may fire again on the
        next session in the window. The OFFER is bounded to once per day so the minute cadence
        cannot inflate ``prescreen.admit``'s raw counters (375 ticks a day). The bound is spent on
        an OFFER, never on a look: a level out of band at 09:20 and back inside at 14:00 is the case
        this exists for.

        The bound is keyed by SYMBOL, not row: a symbol can cross twice in one window and carry two
        resting rows, and the pre-screen's ``(symbol, strategy)`` dedupe could only suppress the
        second. So one symbol yields at most ONE candidate per DAY — the strongest (``score`` desc,
        ``symbol`` asc, as the ``brk20`` sweep publishes); the other row stays for tomorrow.

        The bound is process memory, not a column, deliberately: it is a telemetry bound, and losing
        it in a restart costs one deduped extra offer, as a restart already costs the ordinary sweep.
        """
        try:
            self._roll_offered(today)
            band = Decimal(str(band_pct))
            # Coerced inside the guard like `band`: an unparseable limits value must cost this tick.
            max_age = None if max_tick_age_s is None else float(max_tick_age_s)
            rows = self._conn.execute(
                "SELECT symbol, signal_d, entry, stop, target, score "
                "FROM brk20_resting_levels "
                "WHERE status = ? AND signal_d < ? AND expires_d >= ? "
                "ORDER BY score DESC, symbol ASC",
                (STATUS_RESTING, today.isoformat(), today.isoformat()),
            ).fetchall()
        except Exception as exc:  # noqa: BLE001 - an unreadable journal re-offers nothing
            _log.warning("brk20_retest_due_failed", d=today.isoformat(), error=str(exc))
            return []
        if not rows:
            return []

        # One call per tick, only when a row is in the window: the screen reads the positions and
        # recommendations tables and the ordinary minute must stay one cheap SQL read.
        skip: Mapping[str, str] = {}
        if skip_fn is not None:
            try:
                skip = skip_fn(sorted({str(r["symbol"]) for r in rows}))
            except Exception as exc:  # noqa: BLE001 - an unevaluable narrowing offers nothing
                _log.warning("brk20_retest_skip_unreadable", d=today.isoformat(), error=str(exc))
                return []

        out: list[SignalCandidate] = []
        for row in rows:
            symbol = str(row["symbol"])
            # One offer per symbol per day; also skips a second resting row of the symbol in this
            # loop (rows arrive strongest-first), so no intra-call seen-set is needed.
            if symbol in self._offered:
                continue
            reason = skip.get(symbol)
            if reason is not None:
                # No offer bound is spent: the reason can lapse inside the day.
                self._log_once(symbol, "brk20_retest_not_offerable", d=today.isoformat(),
                               symbol=symbol, reason=reason)
                continue
            try:
                cand = self._candidate_if_in_band(
                    row, symbol, ltp_fn, band, tick_age_fn, max_age, offers,
                )
            except Exception as exc:  # noqa: BLE001 - one bad row costs itself and nothing else
                _log.warning("brk20_retest_row_failed", symbol=symbol, error=str(exc))
                continue
            if cand is None:
                continue
            self._offered.add(symbol)
            out.append(cand)
        return out

    # ------------------------------------------------------------------ internals
    def _candidate_if_in_band(
        self,
        row: sqlite3.Row,
        symbol: str,
        ltp_fn: Callable[[str], Decimal | float | None],
        band: Decimal,
        tick_age_fn: Callable[[str], float | None] | None = None,
        max_age_s: float | None = None,
        offers: dict[str, dict[str, str]] | None = None,
    ) -> SignalCandidate | None:
        """One row -> a candidate, or None when it is not in the band / not priceable this minute."""
        raw_ltp = ltp_fn(symbol)
        if raw_ltp is None:
            return None
        ltp = Decimal(str(raw_ltp))
        if ltp <= 0:
            return None
        entry = Decimal(str(row["entry"]))
        dev = abs(entry - ltp) / ltp * _HUNDRED
        if dev > band:
            return None
        # Freshness, after the band so the log fires only for a level otherwise offered: the gate
        # rejects anything past `max_tick_age_s`, so an older price wastes a slot on a certain
        # reject. No age at all is the same refusal as a stale one.
        if tick_age_fn is not None and max_age_s is not None:
            age = tick_age_fn(symbol)
            if age is None or age > max_age_s:
                self._log_once(symbol, "brk20_retest_tick_stale", symbol=symbol,
                               age_s=("none" if age is None else f"{age:.1f}"),
                               max_tick_age_s=max_age_s, entry=str(entry), ltp=str(ltp))
                return None
        stop = None if row["stop"] is None else Decimal(str(row["stop"]))
        target = None if row["target"] is None else Decimal(str(row["target"]))
        cand = SignalCandidate(
            signal_id=str(ULID()),
            strategy_id=brk20.STRATEGY_ID,
            symbol=symbol,
            side="BUY",                       # brk20 is long-only (NSE cash cannot be shorted swing)
            style="swing",
            raw_levels=RawLevels(entry=entry, stop=stop, target=target),
            score=float(row["score"]),
        )
        # The level rides as ``entry``: ``level`` is a RESERVED kwarg on ``core.log.BoundLogger._log``
        # (it collides with the log level), and the sibling ``pipeline.forward_skipped_outside_band``
        # line already calls it ``entry``.
        #
        # `_offered`, not `_republished`: nothing is published until `prescreen.admit` accepts it;
        # the caller emits `brk20_retest_republished` off `offers` for the survivors, so each name
        # counts one funnel stage.
        detail = {
            "symbol": symbol, "entry": str(entry), "ltp": str(ltp),
            "dev": str(dev.quantize(_PAISA, rounding=ROUND_HALF_UP)),
            "signal_d": str(row["signal_d"]),
        }
        _log.info("brk20_retest_offered", band=str(band), signal_id=cand.signal_id, **detail)
        if offers is not None:
            offers[cand.signal_id] = detail
        return cand

    def _expiry(self, signal_d: date) -> date:
        """The (``sessions`` − 1)-th TRADING session after ``signal_d`` (offered on y+1..y+N).

        Raises only what the calendar raises past its verified horizon; every caller is inside a
        ``try`` that turns that into "no row written".
        """
        d = signal_d
        for _ in range(self._sessions - 1):
            d = self._calendar.next_trading_day(d)
        return d

    def _roll_offered(self, today: date) -> None:
        """Reset the per-day offer bound on a date change (the midnight roll, and a replay's jump)."""
        if self._offered_day != today:
            self._offered_day = today
            self._offered.clear()
            self._logged.clear()

    def _log_once(self, _symbol: str, _event: str, **fields: object) -> None:
        """Emit ``_event`` for ``_symbol`` at most once per DAY (refusals recur every 60 s pulse).

        Both positionals are underscored so a structured field may be called ``symbol`` or ``event``."""
        key = (_symbol, _event)
        if key in self._logged:
            return
        self._logged.add(key)
        _log.info(_event, **fields)


__all__ = ["DEFAULT_RETEST_SESSIONS", "STATUS_EXPIRED", "STATUS_RESTING", "RestingLevelBook"]
