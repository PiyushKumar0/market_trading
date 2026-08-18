"""Live ``context_provider`` for ``SignalPreScreen`` (§3.2.5) — the MarketStore/calendar/FeatureEngine
seam that lets the Phase-1 scanners run off the live ``bar.1m`` stream.

``SignalPreScreen`` calls ``context_provider(bar) -> ScanContext`` once per bar and hands the result
to every enabled scanner (scanners are pure — all I/O lives here, see ``scanners.base``). Phase 1
only ever built contexts offline (backtest frames); this provider is the live one.

Threading (§2.2 / §3.2 convention 4)
-----------------------------------
``SignalPreScreen.handle_bar`` offloads ``_scan`` — and therefore every call into this provider — to
a worker thread via ``asyncio.to_thread``, so **synchronous DuckDB reads here are correct and the
event loop is never blocked**. No asyncio in this module; the ``MarketStore.a*`` wrappers are for
loop-side callers. The pre-screen holds its own lock across ``_scan``, so provider calls are already
serialized: this class is deliberately NOT internally synchronized (a second lock would be dead
weight). Wiring it anywhere that can call it concurrently is a caller error.

Read budget (the §3.2 hot-path invariant)
-----------------------------------------
The event path must not do per-bar N+1 DuckDB reads. Everything that is constant for a day is built
ONCE into a day-scoped cache, rebuilt only when the bar's date changes:

* ``daily_bars`` per symbol (completed sessions through the PRIOR day) — one ``get_bars_1d`` per
  symbol per day; a symbol first seen mid-day is loaded lazily, once, then cached.
* index (``NIFTY 50``) daily closes — one read per day.
* the PRIOR session's ``flagged_instrument_days`` symbol set — one read per day (the deals job
  writes a day's rows at 20:30 of that day, so a day's own flags are unknowable intraday; reading
  ``d`` here was structurally empty — fixed 2026-08-18).
* upcoming corp-action ex-dates for ALL symbols — one ``get_corp_actions`` range read per day,
  bucketed by symbol (never one read per symbol).
* ``momentum_by_symbol`` — computed from the SAME cached daily closes (``indicators.momentum``), so a
  symbol joining the cache costs no extra read.
* ``trade_window`` / ``session_open`` — pure ``NSECalendar`` lookups, cached with the day.

Per bar the provider therefore does at most: zero store reads (steady state), plus the intraday seed
on a symbol's first bar of the day, plus whatever ``FeatureEngine.intraday_snapshot`` costs.

Intraday bars
-------------
``ScanContext.intraday_bars`` must be today's session bars ascending WITH the scanned bar last. The
provider keeps an in-memory per-symbol list and appends each incoming bar. On a symbol's FIRST bar of
the day it seeds that list from ``store.get_bars_1m(symbol, session_open, bar.ts_minute)`` — a
mid-session start (restart + §2.6 gap backfill) then still yields a complete list rather than an
orb range that begins wherever the process happened to come up. Redelivery is tolerated: a bar with
the same ``ts_minute`` REPLACES the last element (a late-tick amendment rewrites a bar, §4.4 job 1),
and a bar older than the last one is IGNORED (a stale/out-of-order delivery must not corrupt the
ascending series). An ignored bar leaves ``intraday_bars[-1] != bar``, which every intraday scanner
already treats as fail-to-zero.

``mom`` rebalance-day state (WO-13, F11, resolved 2026-08-13)
---------------------------------------------------------------
``mom_sessions_since_rebalance`` used to be hard-coded ``None`` ("never rebalanced ⇒ due now" on
EVERY day), a live/backtest cadence mismatch against the sweep's fixed ``rebalance_days``-session
turnover. The provider now persists the last-rebalanced trading day in ``mom_rebalance_state``
(migration 0006, one row, singleton pattern) and reports the real trading-session count — see
:meth:`LiveScanContextProvider._resolve_mom_rebalance`. ``None`` is still the correct value for the
genuine "never rebalanced" bootstrap case (fresh install / no ``conn`` wired); the scanner already
treats ``None`` and "due" identically (``engine.strategy.scanners.momentum``).

Trade-window staleness (fixed 2026-08-18)
-----------------------------------------
``trade_window`` is day-cached like everything else above, but unlike the rest of the cache it is a
CONTROL-PLANE value the owner can change mid-session (``/trade_window``, ``POST /config/trade_window``
— §3.2.7). The cache is per PROCESS per date, so before this fix an owner change was invisible to
every scanner until the next restart or day rollover. Observed live 2026-08-18: the cache was built
09:52:06 holding 10:00–10:30, the owner moved the window to 10:10–15:30 at 09:57:52, and ``orb``
(which intersects ``ctx.trade_window`` at :meth:`~engine.strategy.scanners.orb.OrbScanner.scan`)
went on firing from 10:00:05 against the window that no longer existed — exhausting its entire
6-slot day sub-cap nine minutes before the real window opened, and refusing a score-1.0 KOTAKBANK
to do it.

:meth:`LiveScanContextProvider.invalidate_trade_window` is the fix, wired in ``engine.ops.main`` to
the ``trade_window.changed`` bus event that :class:`~engine.risk.mode.ModeManager` has always
published (the event existed; only telegram and the API relay listened). It refreshes ONE field
rather than dropping the cache — a full rebuild would re-run the index/flagged/ex-date reads and the
~100-symbol momentum preload on the event-loop's worker, which is precisely the per-bar read budget
this cache exists to protect. This is the one piece of cross-thread state in the class (the flag is
set on the event loop, consumed on the pre-screen worker thread), hence the :class:`threading.Event`
where nothing else here is synchronized.

Known v1 gaps (deliberate, documented — not silent)
---------------------------------------------------
* ``momentum_by_symbol`` spans the symbols CACHED SO FAR today, not the whole universe — the
  cross-section fills in as symbols tick. Combined with the gap above, the first bars of a day can
  rank a symbol against a partial cross-section. Pass ``momentum_universe`` (typically today's
  included ``universe_daily`` symbols) to preload the full cross-section on the day's first call and
  remove that skew; the cost is N daily reads once per day, in the worker thread.
"""

from __future__ import annotations

import math
import sqlite3
import threading
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from decimal import Decimal

from engine.core.calendar import NSECalendar
from engine.core.clock import Clock
from engine.core.log import get_logger
from engine.core.types import Bar
from engine.features.engine import FeatureEngine
from engine.marketdata.store import DailyBar, MarketStore
from engine.strategy.indicators import momentum
from engine.strategy.types import ScanContext

_log = get_logger("engine.ops.scan_context")

#: Calendar-day look-ahead for ``upcoming_ex_dates`` (A12). NOT ~10 sessions: the ``mom`` scanner
#: applies its OWN skip horizon of ``ceil(rebalance_days x 7/5)`` calendar days, which reaches 28 at
#: the §6.3 ``rebalance_days`` upper bound of 20. Supplying fewer ex-dates than the scanner filters on
#: would silently defeat the A12 skip; supplying more is free (the scanner discards the surplus).
DEFAULT_EX_HORIZON_DAYS = 28

#: §6.1 ``mom.rebalance_days`` default (envelope.yaml: min 10 / max 20 / default 15; matches
#: ``MomentumScanner.DEFAULT_PARAMS``) — the cadence this provider uses to decide when a rebalance-
#: due day advances the persisted marker (WO-13, F11). MUST match the live ``MomentumScanner``'s
#: configured ``rebalance_days``: today both default to 15 and ``engine.ops.main`` wires no envelope
#: override into ``build_enabled_scanners`` yet, so they cannot drift in production; a future
#: per-strategy envelope override would need to pass the same value here.
DEFAULT_MOM_REBALANCE_DAYS = 15


def _render_window(w: tuple[datetime, datetime] | None) -> str:
    """``HH:MM-HH:MM`` (or ``none``) for the window-change log line — never a raw datetime pair."""
    return "none" if w is None else f"{w[0]:%H:%M}-{w[1]:%H:%M}"


@dataclass
class _DayCache:
    """Everything constant for one trading day, built once per day inside ``__call__``."""

    d: date
    daily_start: date
    daily_end: date
    session_open: datetime | None
    trade_window: tuple[datetime, datetime] | None
    flagged: frozenset[str]
    index_closes: list[Decimal]
    ex_dates: dict[str, list[date]]
    daily_bars: dict[str, list[DailyBar]] = field(default_factory=dict)
    momentum: dict[str, float] = field(default_factory=dict)
    intraday: dict[str, list[Bar]] = field(default_factory=dict)
    mom_sessions_since_rebalance: int | None = None


class LiveScanContextProvider:
    """Assemble a :class:`ScanContext` per live bar (§3.2.5 ``context_provider``).

    Parameters
    ----------
    store / clock / calendar:
        The single-writer :class:`MarketStore`, the platform :class:`Clock` (the ONLY time source,
        §3.2) and the :class:`NSECalendar` (session open + owner trade window, R6).
    features:
        The :class:`FeatureEngine` whose ``intraday_snapshot`` mints the ``features_snapshot_id``
        carried onto every candidate (§3.2.5 audit chain).
    index_symbol:
        ``bars_1d`` symbol the backfill job persists NIFTY 50 under — the rsi2 regime input (§6.1).
        Must match ``FeatureEngine``/``WarmupGate`` (§7.1 ``regime_data_ready``).
    momentum_weeks:
        Momentum lookback in weeks (§6.1 ``mom``: 4 weeks = 20 sessions).
    daily_lookback_days:
        Calendar-day span fetched from ``bars_1d`` (400 covers the 200-session DMA plus holidays —
        same convention as ``FeatureEngine``).
    ex_horizon_days:
        Corp-action look-ahead for ``upcoming_ex_dates`` (see :data:`DEFAULT_EX_HORIZON_DAYS`).
    momentum_universe:
        Optional symbols to preload into the day cache on the day's FIRST call, so
        ``momentum_by_symbol`` is the complete cross-section from the first bar instead of filling in
        as symbols tick. Accepts a static sequence OR a zero-arg callable resolved at each day-cache
        build (the universe is day-scoped — composition passes the ``universe_daily`` watchlist
        closure so a fresh 08:30 build is picked up without reconstructing the provider).
        ``None`` (default) = purely lazy.
    conn:
        Optional state-DB connection for the WO-13 ``mom`` rebalance-day marker
        (``mom_rebalance_state``, migration 0006). ``None`` (default) degrades to the pre-WO-13
        behaviour — ``mom_sessions_since_rebalance`` is always ``None`` ("due now") and nothing is
        persisted — so a caller that has not wired a connection yet is unaffected, never broken.
    mom_rebalance_days:
        The cadence (trading sessions) this provider uses to decide a rebalance is due and advance
        the persisted marker (see :data:`DEFAULT_MOM_REBALANCE_DAYS`). MUST match the live
        ``MomentumScanner``'s configured ``rebalance_days``.
    """

    def __init__(
        self,
        store: MarketStore,
        clock: Clock,
        calendar: NSECalendar,
        features: FeatureEngine,
        *,
        index_symbol: str = "NIFTY 50",
        momentum_weeks: int = 4,
        daily_lookback_days: int = 400,
        ex_horizon_days: int = DEFAULT_EX_HORIZON_DAYS,
        momentum_universe: Sequence[str] | Callable[[], Sequence[str]] | None = None,
        conn: sqlite3.Connection | None = None,
        mom_rebalance_days: int = DEFAULT_MOM_REBALANCE_DAYS,
    ) -> None:
        if momentum_weeks < 1:
            raise ValueError("momentum_weeks must be >= 1")
        if daily_lookback_days < 1:
            raise ValueError("daily_lookback_days must be >= 1")
        if ex_horizon_days < 0:
            raise ValueError("ex_horizon_days must be >= 0")
        if mom_rebalance_days < 1:
            raise ValueError("mom_rebalance_days must be >= 1")
        self._store = store
        self._clock = clock
        self._calendar = calendar
        self._features = features
        self._index_symbol = index_symbol
        self._momentum_weeks = int(momentum_weeks)
        self._lookback_days = int(daily_lookback_days)
        self._ex_horizon_days = int(ex_horizon_days)
        self._conn = conn
        self._mom_rebalance_days = int(mom_rebalance_days)
        if callable(momentum_universe):
            self._momentum_universe_fn: Callable[[], Sequence[str]] | None = momentum_universe
        else:
            static = list(momentum_universe) if momentum_universe else []
            self._momentum_universe_fn = (lambda: static) if static else None
        # Built lazily on the first __call__ — NO store read happens at construction (a provider is
        # wired at composition time, long before the worker thread exists).
        self._cache: _DayCache | None = None
        #: Set from the event loop by :meth:`invalidate_trade_window`, consumed on the pre-screen
        #: worker thread by :meth:`_day` (module docstring "Trade-window staleness"). The only
        #: cross-thread state in this class.
        self._window_dirty = threading.Event()

    # ------------------------------------------------------------------ ContextProvider surface
    def __call__(self, bar: Bar) -> ScanContext:
        """Build the :class:`ScanContext` for ``bar`` (called from the pre-screen worker thread)."""
        day = self._day(bar.ts_minute.date())
        symbol = bar.symbol
        daily = day.daily_bars.get(symbol)
        if daily is None:
            daily = self._load_daily(day, symbol)
        return ScanContext(
            intraday_bars=self._intraday(day, bar),
            daily_bars=daily,
            index_daily_closes=day.index_closes,
            flagged=symbol in day.flagged,
            trade_window=day.trade_window,
            session_open=day.session_open,
            momentum_by_symbol=day.momentum,
            upcoming_ex_dates=day.ex_dates.get(symbol, []),
            # WO-13: real trading-session count (or None = never rebalanced ⇒ due now), day-cached —
            # see _resolve_mom_rebalance / module docstring "mom rebalance-day state".
            mom_sessions_since_rebalance=day.mom_sessions_since_rebalance,
            features_snapshot_id=self._snapshot_id(symbol),
        )

    # ------------------------------------------------------------------ owner window changes (§3.2.7)
    def invalidate_trade_window(self) -> None:
        """Mark the cached ``trade_window`` stale; the next :meth:`_day` re-reads it (2026-08-18).

        Wired to the ``trade_window.changed`` bus topic in ``engine.ops.main``. Deliberately does NOT
        touch the cache here: this runs on the event loop while the pre-screen worker thread may be
        mid-``__call__``, and every other field is built under the "provider calls are serialized by
        the pre-screen's lock" assumption this module documents. Setting a flag is the only mutation
        that is safe from either thread; the refresh itself happens on the worker, in :meth:`_day`.

        Idempotent, and safe before the first ``__call__`` (no cache yet ⇒ the first build reads the
        new value anyway).
        """
        self._window_dirty.set()
        _log.info("scan_context_window_invalidated")

    # ------------------------------------------------------------------ day-scoped cache
    def _day(self, d: date) -> _DayCache:
        """The cache for ``d``, rebuilt whenever the bar date moves (day rollover / replay jump).

        Also services a pending :meth:`invalidate_trade_window` — one field, not a rebuild (module
        docstring "Trade-window staleness").
        """
        cache = self._cache
        if cache is not None and cache.d == d:
            if self._window_dirty.is_set():
                self._refresh_window(cache)
            return cache
        cache = self._build_day(d)
        self._cache = cache
        return cache

    def _refresh_window(self, cache: _DayCache) -> None:
        """Re-read the owner trade window into an EXISTING day cache (worker thread)."""
        # Cleared BEFORE the read: an owner change landing during the read leaves the flag set, so
        # the next bar re-reads. Clearing after would swallow it.
        self._window_dirty.clear()
        try:
            window: tuple[datetime, datetime] | None = self._calendar.trade_window(cache.d)
        except ValueError:
            window = None           # not a trading day — same meaning as in _build_day (R6)
        except Exception as exc:  # noqa: BLE001 - an UNEXPECTED read failure must not eat the event
            # (2026-08-18 review): the flag is already cleared, so swallowing this would drop the
            # owner's change until the NEXT change or restart. Re-arm the flag (the next bar
            # retries) and keep the last-known window — one bad SQLite read degrades to a
            # one-bar-late refresh, never to a silently stale session.
            self._window_dirty.set()
            _log.warning("scan_context_window_refresh_failed", d=cache.d.isoformat(),
                         error=f"{type(exc).__name__}: {exc}")
            return
        if window == cache.trade_window:
            return
        _log.info(
            "scan_context_window_refreshed", d=cache.d.isoformat(),
            old=_render_window(cache.trade_window), new=_render_window(window),
        )
        cache.trade_window = window

    def _build_day(self, d: date) -> _DayCache:
        session = self._calendar.session(d)
        # Cleared BEFORE the read, same race reasoning as _refresh_window.
        self._window_dirty.clear()
        try:
            window: tuple[datetime, datetime] | None = self._calendar.trade_window(d)
        except ValueError:
            window = None          # not a trading day — no window (R6); intraday scanners fail to zero
        # Daily history is COMPLETED sessions only: today's bars_1d row does not exist intraday, and
        # anchoring on it would leak a partially-formed day into the daily lookbacks.
        daily_end = d - timedelta(days=1)
        daily_start = d - timedelta(days=self._lookback_days)
        index_closes = [b.close for b in self._store.get_bars_1d(self._index_symbol, daily_start, daily_end)]
        # PRIOR session's bulk/block-deal flags, not day ``d``'s (2026-08-18 fix): the deals job
        # writes ``d``'s rows at 20:30 of ``d`` itself, so an intraday read of ``d`` was structurally
        # empty and the §6.1 orb suppression had never fired live. "A deal on the last completed
        # session" is the only knowable — and therefore the intended — live meaning.
        try:
            flagged_day: date | None = self._calendar.previous_trading_day(d)
        except ValueError:       # no prior session resolvable (calendar hole) — no flags, no error
            flagged_day = None
        flagged = (
            frozenset(r["symbol"] for r in self._store.get_flagged_instrument_days(flagged_day))
            if flagged_day is not None else frozenset()
        )
        ex_dates: dict[str, list[date]] = {}
        for row in self._store.get_corp_actions(
            ex_from=d, ex_to=d + timedelta(days=self._ex_horizon_days)
        ):
            ex_dates.setdefault(row["symbol"], []).append(row["ex_date"])
        # WO-13: only a real trading day is a candidate rebalance day — a weekend/holiday date (no
        # session) must never advance the persisted marker.
        mom_since = self._resolve_mom_rebalance(d) if session is not None else None
        cache = _DayCache(
            d=d,
            daily_start=daily_start,
            daily_end=daily_end,
            session_open=session.open if session is not None else None,
            trade_window=window,
            flagged=flagged,
            index_closes=index_closes,
            ex_dates=ex_dates,
            mom_sessions_since_rebalance=mom_since,
        )
        preload: Sequence[str] = ()
        if self._momentum_universe_fn is not None:
            try:
                preload = self._momentum_universe_fn()
            except Exception:  # noqa: BLE001 - a failed universe read degrades to lazy fill, never raises
                _log.exception("scan_context_universe_preload_failed")
        for symbol in preload:
            self._load_daily(cache, symbol)
        _log.info(
            "scan_context_day_built",
            d=d.isoformat(), index_closes=len(index_closes), flagged=len(flagged),
            flagged_day=None if flagged_day is None else flagged_day.isoformat(),
            ex_date_symbols=len(ex_dates), preloaded=len(preload),
            trading_day=window is not None,
        )
        return cache

    def _load_daily(self, day: _DayCache, symbol: str) -> list[DailyBar]:
        """One ``get_bars_1d`` for ``symbol`` per day; also fixes its momentum from those closes."""
        bars = self._store.get_bars_1d(symbol, day.daily_start, day.daily_end)
        day.daily_bars[symbol] = bars
        day.momentum[symbol] = self._momentum(bars)
        return bars

    def _momentum(self, bars: list[DailyBar]) -> float:
        """Latest N-week momentum over ``bars``' closes; NaN when the lookback is short (§6.1 ``mom``
        treats NaN as unrankable, so a thin symbol simply cannot make ``top_n``)."""
        if not bars:
            return math.nan
        return float(momentum([b.close for b in bars], weeks=self._momentum_weeks).iloc[-1])

    # ------------------------------------------------------------------ §6.1 mom rebalance state (WO-13)
    def _resolve_mom_rebalance(self, d: date) -> int | None:
        """Trading sessions since the last ``mom`` rebalance, or ``None`` if never rebalanced — both
        read as "due now" by :class:`~engine.strategy.scanners.momentum.MomentumScanner` (module
        docstring). Persists the day's cadence decision to ``mom_rebalance_state`` (migration 0006):
        a DUE day (``since >= mom_rebalance_days``), or the never-rebalanced bootstrap, stamps ``d``
        as the new reference point — matching the sweep's fixed-cadence indexing
        (``valid_positions[::rebalance_days]``, ``engine.learning.sweep``), where a rebalance is a
        calendar/session event and not conditioned on whether a candidate actually cleared ``top_n``
        that day. No ``conn`` wired ⇒ degrades to the pre-WO-13 ``None`` (always due), never raises:
        persistence is additive, not a hard dependency of the scan path.
        """
        if self._conn is None:
            return None
        last_d = self._read_last_rebalance_d()
        if last_d is None:
            self._write_last_rebalance_d(d)
            return None                              # never rebalanced ⇒ due now (pre-existing semantics)
        since = self._sessions_between(last_d, d)
        if since >= self._mom_rebalance_days:
            self._write_last_rebalance_d(d)           # today becomes the new reference point
        return since

    def _read_last_rebalance_d(self) -> date | None:
        try:
            row = self._conn.execute(
                "SELECT last_rebalance_d FROM mom_rebalance_state WHERE id=1"
            ).fetchone()
        except Exception as exc:  # noqa: BLE001 - a journal read must never block the scan path
            _log.warning("mom_rebalance_read_failed", error=str(exc))
            return None
        raw = row["last_rebalance_d"] if row is not None else None
        if not raw:
            return None
        try:
            return date.fromisoformat(str(raw))
        except ValueError:
            _log.warning("mom_rebalance_unparseable", raw=str(raw))
            return None

    def _write_last_rebalance_d(self, d: date) -> None:
        try:
            self._conn.execute(
                "UPDATE mom_rebalance_state SET last_rebalance_d=? WHERE id=1", (d.isoformat(),)
            )
        except Exception as exc:  # noqa: BLE001 - journaling is resilience bookkeeping, never a gate
            _log.warning("mom_rebalance_write_failed", d=d.isoformat(), error=str(exc))

    def _sessions_between(self, start: date, end: date) -> int:
        """Trading sessions in (``start``, ``end``], via ``NSECalendar.is_trading_day`` (never a
        second calendar) — the WO-13 cadence counter. ``end <= start`` (a stale/backdated marker)
        returns 0 rather than a negative count."""
        if end <= start:
            return 0
        count = 0
        probe = start + timedelta(days=1)
        while probe <= end:
            if self._calendar.is_trading_day(probe):
                count += 1
            probe += timedelta(days=1)
        return count

    # ------------------------------------------------------------------ intraday series
    def _intraday(self, day: _DayCache, bar: Bar) -> list[Bar]:
        """Today's ascending 1m series for ``bar.symbol`` with ``bar`` last (see module docstring)."""
        bars = day.intraday.get(bar.symbol)
        if bars is None:
            bars = []
            if day.session_open is not None and bar.ts_minute > day.session_open:
                # Mid-session start: everything already persisted for today (live + §2.6 gap
                # backfill), exclusive of the incoming minute (get_bars_1m end is exclusive).
                bars = list(self._store.get_bars_1m(bar.symbol, day.session_open, bar.ts_minute))
            day.intraday[bar.symbol] = bars
        if not bars or bar.ts_minute > bars[-1].ts_minute:
            bars.append(bar)
        elif bar.ts_minute == bars[-1].ts_minute:
            bars[-1] = bar                     # redelivery / late-tick amendment: freshest bar wins
        else:
            _log.warning(
                "scan_context_out_of_order_bar",
                symbol=bar.symbol, ts_minute=bar.ts_minute.isoformat(),
                last_ts_minute=bars[-1].ts_minute.isoformat(),
            )
        return bars

    # ------------------------------------------------------------------ feature snapshot
    def _snapshot_id(self, symbol: str) -> str | None:
        """Mint this bar's ``features_snapshot_id`` (§3.2.5). A minting failure degrades to ``None``
        (the candidate loses its feature link) and NEVER raises: a FeatureEngine problem must not take
        the whole scan path down mid-session."""
        try:
            return self._features.intraday_snapshot(symbol).features_snapshot_id
        except Exception as exc:  # noqa: BLE001 - fail to None, never propagate into the scan path
            _log.warning("scan_context_snapshot_failed", symbol=symbol, error=str(exc))
            return None
