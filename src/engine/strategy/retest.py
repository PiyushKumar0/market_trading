"""``brk20`` RETEST re-arm — the broken level RESTS for N trading sessions (WO-R, 2026-09-12).

WHAT THIS IMPLEMENTS, AND WHAT SELECTED IT. The 2026-09-12 pre-registered ``brk20`` entry-mechanism
backtest (plan §8.6, ``scripts/backtest_brk20.py``) compared three entry variants on one signal
population and its DECISION RULE — fixed before the run — returned ``V2_limit_at_H20_N5``: a limit
resting at the candidate's own ``raw_levels.entry`` (``round_to_tick(H20)``) for up to FIVE sessions,
filled on the first session whose low reaches the level. That variant was the only one
geometry-viable at T+10 (median net +0.0513% against a 0.3192% round trip) and CPCV-promotable there
(fold pass 73.3%) and at T+20 (+0.9168%, 80.0%).

The rule as SHIPPED does not implement it. ``brk20`` publishes the level exactly ONCE, on the
crossing session, under the pre-screen's same-day ``(symbol, strategy)`` dedupe — the N=1 variant,
which was not among the registered variants, has no measured return, and whose fill rate reads 31.4%
off the registered delay histogram against V2-5's 59.1%. The live cost of that gap is measured: of 15
``brk20`` proposals on 2026-09-11, SIX died on the §7.1 ``entry_sanity_band`` with the level already
>2% below LTP at sweep time, the band being the SOLE reject cause for three of them — and those three
are the day's top three ``brk20`` scores, because the score is monotone in breakout margin and a wide
margin is precisely what puts the rested level far below the live price.

So: every ADMITTED ``brk20`` candidate's level is journalled here on its crossing session and stays
DUE for the next N trading sessions. On each of those sessions, while the market is open and inside
the owner trade window, the level is re-offered for judgement on the FIRST minute the live price is
back inside the §7.1 ``entry_sanity_band`` — the same band, read from the same hash-verified limits
table, that rejects it when it is not.

WHAT IT DOES NOT DO, stated because the temptation runs the other way:

* **No new entry price.** The re-publication carries the ORIGINAL ``raw_levels`` verbatim — same
  entry, same stop, same target, same score. A retest is the same trade offered again, never a
  re-anchored one.
* **No widened band.** The band condition here is the gate's own ``entry_sanity_band`` (CNC), and it
  is a NARROWING: a level outside the band is not offered at all. Nothing in this module can make a
  candidate the gate would reject acceptable.
* **No new admission capacity.** Every re-publication goes through the SAME ``prescreen.admit`` —
  the §3.2.5 day cap, the per-strategy cap and the same-day dedupe all bind identically. A retest
  competes for the same scarce analyst slot as a fresh candidate and wins nothing by being one.
* **No expectancy claim.** ``brk20`` remains exploratory (§6.1). The backtest chose a MECHANISM
  between two mechanisms; it promoted no rule, and the population it chose on is the same
  survivorship-tainted eligible-universe proxy the ``hi52`` study carries.
* **No wider population and no stale trigger.** A re-offer is refused for any symbol the caller's
  ``skip_fn`` names (see :meth:`due`) and for any level whose live price is older than the §7.1
  ``stale_data_guard.max_tick_age_s`` the gate will judge it by. Both are NARROWINGS on top of the
  band, and both fail to zero.

THE TRIGGER IS A FRESH PRICE, NOT MERELY A PRICE. ``ltp_fn`` is the engine's tick cache, which has
no upper bound on age — a symbol that last printed at 11:02 still reads 11:02's price at 14:00 and
``mark_price`` will not say so. Everywhere else in the platform an unaged LTP only DEFERS (the D1 (e)
band screen) or fails closed at the gate; here it would be the thing that SPENDS a §3.2.5 day slot,
so :meth:`due` takes the age seam (``main.tick_age_s``) beside the price and applies the gate's own
``max_tick_age_s``. The population this matters most for is the one :meth:`resting_symbols` exists to
reach: sub-cap eligible names that can go minutes between prints.

ONE TRADE PER LEVEL. The mechanism the backtest measured books ONE trade per signal ("filled on the
FIRST session in y+1…y+N whose low reaches the level"). A level that has already produced a position
or an unactioned entry recommendation is therefore not re-offered: ``gate._rule_per_stock_exposure``
would reject it as ``already held or pending`` — a HARD reason no shrink can cure — after it had
already spent a day slot and one of the day's analyst calls. The caller owns that verdict (it owns
the positions and recommendations tables); this module owns only the refusal.

INTERACTION WITH THE D1 (e) BAND SCREEN (``pipeline.BAND_SKIP_STRATEGIES``). That screen defers a
queued ``brk20`` candidate WITHIN a day while its level sits outside the band, and the two mechanisms
are complements, not duplicates: the screen can only defer a candidate that is still in today's
forward queue, and the queue empties at its TTL, so a level that walks away in the morning and comes
back after the candidate expired has nothing left to re-publish it. This book re-arms ACROSS days,
from durable state, and the band condition here is what makes the re-offer worth an analyst slot at
all. Both read the same band; neither widens it.

DURABILITY IS THE POINT. The state lives in SQLite (migration 0014), not in process memory, for the
reason the 2026-09-11 forensics recorded against the in-memory forward queue: the engine reboots
inside the session (a warm-up freeze, a token refresh, a deploy), and a multi-session mechanism whose
state does not survive a restart silently degenerates into the single-session one it was built to
replace.

FAIL TO ZERO (§3.2.5 / D7). Nothing here raises. A journal read that fails, a row that will not
parse, an ``ltp_fn`` that throws, a ``skip_fn`` that cannot be evaluated, a calendar past its
verified horizon — each costs at most its own row (the screen, the whole tick) and never the drain
tick it is called from. The worst outcome of a failure in this module is that
a level is not re-offered today, which is exactly the pre-WO-R behaviour.
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

#: Default retest window, in TRADING sessions after the crossing session. 5 = the backtest's V2-5,
#: the variant its pre-registered decision rule selected. Owner-only and deliberately NOT learnable:
#: the number IS the registered mechanism, so a learner allowed to move it would be selecting an
#: entry mechanism the study never measured (N=1 and N=3 were both measured and both lost; every
#: other N was never run at all).
DEFAULT_RETEST_SESSIONS = 5

STATUS_RESTING = "resting"
STATUS_EXPIRED = "expired"

_HUNDRED = Decimal("100")
#: Log-only rounding for the deviation percentage (the pipeline band screen's own ``_PAISA``): the
#: COMPARISON is exact Decimal, and only the rendered number is quantized.
_PAISA = Decimal("0.01")


class RestingLevelBook:
    """The durable set of ``brk20`` levels still resting, and the daily re-offer that reads it.

    Four operations, each total (never raises):

    * :meth:`record` — journal one ADMITTED ``brk20`` candidate's level on its crossing session.
      Idempotent per ``(symbol, signal_d)``.
    * :meth:`expire` — mark rows past their window ``expired``. Housekeeping only; :meth:`due`
      does not depend on it having run (its own predicate is ``expires_d >= today``).
    * :meth:`due` — the levels whose price has come back inside the band today, as ready-to-admit
      :class:`SignalCandidate` objects.
    * :meth:`resting_symbols` — which symbols the FEED must carry today for :meth:`due` to be able
      to band them at all. Not an accessory: without it the mechanism is dead on sub-cap symbols.

    ``sessions`` is the retest window in TRADING sessions; ``calendar`` resolves it (a calendar-blind
    ``+N days`` would shorten the window across every weekend and holiday).
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
        # A window of 0 sessions is not a configuration, it is a disabled mechanism expressed as one:
        # clamp to >=1 so the invariant "expires_d is a session strictly after signal_d" holds for
        # every row ever written, and a mis-set knob degrades to the shortest real retest rather than
        # to rows that are born expired.
        self._sessions = max(1, int(sessions))
        # Per-DAY re-offer bound, keyed by SYMBOL and held in process memory on purpose
        # (see :meth:`due` for both choices).
        self._offered_day: date | None = None
        self._offered: set[str] = set()
        # Per-DAY log bound for the REFUSAL lines (skip reason, stale tick). A refusal is evaluated
        # on every 60 s pulse, so an unbounded line would be ~375 copies a day per symbol; rolled
        # with `_offered` so a day change re-states each reason exactly once.
        self._logged: set[tuple[str, str]] = set()

    # ------------------------------------------------------------------ write path
    def record(self, candidate: SignalCandidate) -> bool:
        """Journal ``candidate``'s level as resting from TODAY. Returns True when a row was written.

        Called for every ``brk20`` candidate the sweep ADMITTED — admitted, not raw: the pre-screen
        has already decided which levels the platform is willing to spend judgement on, and a rule
        that rested every raw fire would re-offer levels the caps refused the first time.

        Idempotent per ``(symbol, signal_d)`` via ``ON CONFLICT DO NOTHING``: a second sweep the same
        day (an owner ``/scan_now``, a freeze-lift re-sweep) re-admits nothing anyway, and if it ever
        did, the FIRST row's ``created_at`` and expiry are the ones the crossing session earned.

        A non-``brk20`` candidate is refused and logged rather than silently stored: this book's
        whole semantics — a level, a retest, a 5-session window — are ``brk20``'s, and a row from
        another leg would be re-published under ``brk20``'s strategy_id downstream.
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
            # Bare execute on the autocommit connection (``isolation_level=None``), matching the
            # other journal writers (``main._consume_ins_pending``, ``pipeline._journal_slot``):
            # an explicit BEGIN could land inside one the OMS already opened on this connection.
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
        # `Cursor.rowcount` is -1 when sqlite3 cannot determine the affected-row count, and
        # `bool(-1)` is True — the ON CONFLICT path would then report "written" for a row it did
        # not write, inverting the idempotence signal on any driver that reports the sentinel.
        written = (cur.rowcount or 0) > 0
        if written:
            _log.info("brk20_retest_recorded", symbol=candidate.symbol,
                      signal_d=signal_d.isoformat(), expires_d=expires_d.isoformat(),
                      entry=str(levels.entry), sessions=self._sessions)
        return written

    def expire(self, today: date) -> int:
        """Mark every resting row whose window CLOSED before ``today`` as expired. Returns the count.

        Strictly ``expires_d < today``: the expiry session is the LAST one on which the level may
        still be re-offered (``due`` is inclusive at that end), so expiring it on its own day would
        silently ship an N-1 window.

        Housekeeping, never a gate: :meth:`due` re-tests ``expires_d >= today`` itself, so a day this
        never ran (the engine was down, the window never opened) re-offers nothing it should not.
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

        THE MECHANISM DEPENDS ON THIS. :meth:`due` bands the level against a LIVE price, and the
        engine's only price source is the tick cache: ``brk20`` originates over the whole ELIGIBLE
        set (~480 symbols) while the feed carries the included watchlist (~300), and the sweep's
        ``_batch_ticks`` subscription ROLLS AT MIDNIGHT (yesterday's admissions are not today's
        universe). So on retest day 2..5 a level on a sub-cap symbol would read ``ltp is None`` and
        never be offered again — silently dead for exactly the population ``brk20`` exists to catch,
        which is the same hole the 2026-09-11 forensics found at the gate (27 of 84 ``brk20`` slots
        reached it with no tick). The sweep feeds this list into the same per-day subscription set it
        builds from today's admissions, so the feed follows a resting level for as long as it rests.

        Same window predicate as :meth:`due` (``signal_d < today <= expires_d``), minus the price:
        today's OWN admissions carry ``signal_d == today`` and are already in the sweep's batch.
        Deterministic order, and ``[]`` on any failure — an unsubscribed symbol costs a re-offer,
        never the sweep.
        """
        try:
            rows = self._conn.execute(
                "SELECT DISTINCT symbol FROM brk20_resting_levels "
                "WHERE status = ? AND signal_d < ? AND expires_d >= ? ORDER BY symbol ASC",
                (STATUS_RESTING, today.isoformat(), today.isoformat()),
            ).fetchall()
        except Exception as exc:  # noqa: BLE001 - a feed hint is never worth a sweep (D7)
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

        Window: ``signal_d < today <= expires_d``. STRICTLY after the crossing session at the near
        end — the level was already published that day and the pre-screen's same-day dedupe owns
        that — and INCLUSIVE at the far end, so a 5-session window really offers five retest
        sessions.

        Band: ``abs(entry - ltp) / ltp * 100 <= band_pct``, the §7.1 ``entry_sanity_band`` formula
        and its ``<=`` edge verbatim (equality passes, as in the gate). ``ltp_fn`` returning None —
        no tick for the symbol — or a non-positive price publishes NOTHING: an unpriceable symbol is
        the one case where the band cannot be evaluated, and the D7 direction for a mechanism that
        SPENDS a scarce analyst slot is to stay silent, not to guess.

        Freshness: with ``tick_age_fn`` and ``max_tick_age_s`` supplied (the engine passes
        ``main.tick_age_s`` and the §7.1 ``stale_data_guard.max_tick_age_s``), a level whose live
        price has no age or is OLDER than that bound is not offered. The tick cache is unbounded in
        age and ``ltp_fn`` cannot say so, so without this the trigger can be a price from hours ago
        — while the gate that judges the resulting proposal rejects anything past the same bound.
        Omitting either argument disables the test, which is why the wiring pins both.

        Not-offerable screen: ``skip_fn`` is handed today's due symbols ONCE (only when at least one
        row is in the window) and returns ``{symbol: reason}`` for the ones that must not be
        re-offered at all — the engine names symbols already HELD or carrying a PENDING entry
        recommendation (``gate._rule_per_stock_exposure`` rejects those as ``already held or
        pending``, a HARD reason, only after the slot and the analyst call are spent), symbols with
        an ex-date inside brk20's own A12 horizon (the stored level is a pre-ex price the tape no
        longer quotes), and symbols that have left the eligible universe since the crossing. A
        ``skip_fn`` that RAISES offers nothing this tick: the screen is a narrowing, and a narrowing
        that cannot be evaluated fails to zero like the band.

        ``offers``, when supplied, is filled in place with ``{signal_id: {symbol, entry, ltp, dev,
        signal_d}}`` for every candidate returned (the ``brk20.sweep_daily(veto_counts=...)``
        convention). It is what lets the CALLER log ``brk20_retest_republished`` against the
        candidates ``prescreen.admit`` actually accepted, rather than against every one offered.

        The candidate carries the ORIGINAL ``raw_levels`` and the ORIGINAL score, a FRESH
        ``signal_id`` (a new publication is a new signal — the journal, the forward queue and the
        recommendation all key off it) and NO ``features_snapshot_id``: the caller mints that AFTER
        ``prescreen.admit``, exactly as the sweep does via ``ops.main._attach_feature_snapshots``, so
        a candidate the caps suppress never spends a snapshot write.

        The row STAYS ``resting``. A re-publication that expires unactioned — the analyst declined
        it, the day cap suppressed it, a standing freeze re-armed it — may fire again on the next
        session inside the window; the level has not stopped being a level. What bounds it is
        per-day: the pre-screen's same-day ``(symbol, strategy)`` dedupe bounds PUBLICATION to once a
        day, and this book bounds the OFFER to once a day so the minute cadence cannot inflate the
        WO-9 raw counters — ``prescreen.admit`` counts every candidate handed to it as raw, and 375
        ticks a day against one resting row would make the ``brk20`` funnel unreadable. The bound is
        spent on an OFFER, never on a look: a level out of band at 09:20 and back inside it at 14:00
        is the case this mechanism exists for.

        The bound is keyed by SYMBOL, not by row, and that is load-bearing: a symbol can cross twice
        inside one window (cross, fall back, cross again) and so carry two resting rows. The
        pre-screen's dedupe is ``(symbol, strategy)``, so a second same-symbol candidate — in the
        same batch or an hour later — can only ever be suppressed, at the cost of a raw count and a
        suppression line. So one symbol yields at most ONE candidate per DAY, the strongest breakout
        margin (the ``score`` desc, ``symbol`` asc order the ``brk20`` sweep itself publishes in,
        §9.6 deterministic); the other row is left un-offered, still there tomorrow.

        That bound is process memory rather than a column, deliberately. It is a TELEMETRY bound, not
        a correctness one: losing it in a restart costs one extra offer that day (deduped by the
        pre-screen, +1 on a raw counter) — which is exactly what a restart already costs the ordinary
        sweep, whose re-run re-admits and re-counts the same batch. A column would make it durable
        state that has to be reconciled against a day roll for no behavioural gain.
        """
        try:
            self._roll_offered(today)
            band = Decimal(str(band_pct))
            # Coerced HERE, inside the guard, for the same reason `band` is: both arrive from the
            # limits table and a value that will not parse must cost this tick, never raise out of a
            # module whose whole contract is that it does not.
            max_age = None if max_tick_age_s is None else float(max_tick_age_s)
            rows = self._conn.execute(
                "SELECT symbol, signal_d, entry, stop, target, score "
                "FROM brk20_resting_levels "
                "WHERE status = ? AND signal_d < ? AND expires_d >= ? "
                "ORDER BY score DESC, symbol ASC",
                (STATUS_RESTING, today.isoformat(), today.isoformat()),
            ).fetchall()
        except Exception as exc:  # noqa: BLE001 - an unreadable journal re-offers nothing (D7)
            _log.warning("brk20_retest_due_failed", d=today.isoformat(), error=str(exc))
            return []
        if not rows:
            return []

        # ONE call per tick, and only once a row is actually in the window: the screen reads the
        # positions/recommendations tables, and the ordinary minute (nothing resting, or nothing in
        # the window) must stay one cheap SQL read.
        skip: Mapping[str, str] = {}
        if skip_fn is not None:
            try:
                skip = skip_fn(sorted({str(r["symbol"]) for r in rows}))
            except Exception as exc:  # noqa: BLE001 - an unevaluable narrowing offers nothing (D7)
                _log.warning("brk20_retest_skip_unreadable", d=today.isoformat(), error=str(exc))
                return []

        out: list[SignalCandidate] = []
        for row in rows:
            symbol = str(row["symbol"])
            # ONE offer per symbol per day — the same check that skips an already-offered symbol on a
            # later tick also skips a SECOND resting row of that symbol inside this very loop, which
            # is why no separate intra-call seen-set exists (rows arrive strongest-first).
            if symbol in self._offered:
                continue
            reason = skip.get(symbol)
            if reason is not None:
                # No offer bound is spent: the reason can lapse inside the day (a pending
                # recommendation expires, a position is closed) and the level is still a level.
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
        # FRESHNESS, after the band so the log only fires on a level that would otherwise have been
        # offered. `ltp_fn` is the tick cache and carries no age; the §7.1 gate that will judge the
        # resulting proposal rejects anything past `max_tick_age_s`, so originating on an older
        # price spends a day slot, the symbol's one offer and an analyst call on a certain reject.
        # No age at all is the same refusal as a stale one — fail to zero, never to a guess.
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
        # The level rides as ``entry``, not ``level``: ``level`` is a RESERVED kwarg on
        # ``core.log.BoundLogger._log`` (it would collide with the log level and raise), and
        # ``entry`` is what the sibling band line ``pipeline.forward_skipped_outside_band`` already
        # calls the same number — one name for one thing across both band decisions.
        #
        # `_offered`, NOT `_republished`: nothing is published until `prescreen.admit` has seen it,
        # and the day cap / the brk20 sub-cap / the same-day dedupe can each refuse it. The caller
        # emits `brk20_retest_republished` off `offers` for the ones that survive admission, so a
        # grep of either name counts exactly one stage of the funnel (WO-9).
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
        """The ``sessions``-th TRADING session after ``signal_d`` (weekends/holidays are not days).

        Raises only what the calendar raises past its verified horizon; every caller is inside a
        ``try`` that turns that into "no row written" rather than a dead sweep.
        """
        d = signal_d
        for _ in range(self._sessions):
            d = self._calendar.next_trading_day(d)
        return d

    def _roll_offered(self, today: date) -> None:
        """Reset the per-day offer bound on a date change (the midnight roll, and a replay's jump)."""
        if self._offered_day != today:
            self._offered_day = today
            self._offered.clear()
            self._logged.clear()

    def _log_once(self, _symbol: str, _event: str, **fields: object) -> None:
        """Emit ``_event`` for ``_symbol`` at most once this DAY — a refusal is re-evaluated on every
        60 s pulse and an unbounded line would be ~375 copies a day (the `prescreen_out_of_window`
        discipline). The bound is cleared by :meth:`_roll_offered`, so each day re-states it once.

        Both positionals are underscored so a structured field may legitimately be called ``symbol``
        or ``event`` without colliding with them."""
        key = (_symbol, _event)
        if key in self._logged:
            return
        self._logged.add(key)
        _log.info(_event, **fields)


__all__ = ["DEFAULT_RETEST_SESSIONS", "STATUS_EXPIRED", "STATUS_RESTING", "RestingLevelBook"]
