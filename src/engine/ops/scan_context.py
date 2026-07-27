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
* today's ``flagged_instrument_days`` symbol set — one read per day.
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

Known v1 gaps (deliberate, documented — not silent)
---------------------------------------------------
* ``mom_sessions_since_rebalance`` is always ``None`` ("never rebalanced ⇒ due now"). The real count
  is ledger-driven and lands with live ``mom`` state tracking; until then the ``mom`` scanner treats
  every day as a rebalance day (its own dedupe still limits it to one candidate per symbol per day).
* ``momentum_by_symbol`` spans the symbols CACHED SO FAR today, not the whole universe — the
  cross-section fills in as symbols tick. Combined with the gap above, the first bars of a day can
  rank a symbol against a partial cross-section. Pass ``momentum_universe`` (typically today's
  included ``universe_daily`` symbols) to preload the full cross-section on the day's first call and
  remove that skew; the cost is N daily reads once per day, in the worker thread.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
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
        as symbols tick. ``None`` (default) = purely lazy.
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
        momentum_universe: Sequence[str] | None = None,
    ) -> None:
        if momentum_weeks < 1:
            raise ValueError("momentum_weeks must be >= 1")
        if daily_lookback_days < 1:
            raise ValueError("daily_lookback_days must be >= 1")
        if ex_horizon_days < 0:
            raise ValueError("ex_horizon_days must be >= 0")
        self._store = store
        self._clock = clock
        self._calendar = calendar
        self._features = features
        self._index_symbol = index_symbol
        self._momentum_weeks = int(momentum_weeks)
        self._lookback_days = int(daily_lookback_days)
        self._ex_horizon_days = int(ex_horizon_days)
        self._momentum_universe = list(momentum_universe) if momentum_universe else []
        # Built lazily on the first __call__ — NO store read happens at construction (a provider is
        # wired at composition time, long before the worker thread exists).
        self._cache: _DayCache | None = None

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
            # v1: no live `mom` state yet ⇒ "never rebalanced ⇒ due now" (module docstring).
            mom_sessions_since_rebalance=None,
            features_snapshot_id=self._snapshot_id(symbol),
        )

    # ------------------------------------------------------------------ day-scoped cache
    def _day(self, d: date) -> _DayCache:
        """The cache for ``d``, rebuilt whenever the bar date moves (day rollover / replay jump)."""
        cache = self._cache
        if cache is not None and cache.d == d:
            return cache
        cache = self._build_day(d)
        self._cache = cache
        return cache

    def _build_day(self, d: date) -> _DayCache:
        session = self._calendar.session(d)
        try:
            window: tuple[datetime, datetime] | None = self._calendar.trade_window(d)
        except ValueError:
            window = None          # not a trading day — no window (R6); intraday scanners fail to zero
        # Daily history is COMPLETED sessions only: today's bars_1d row does not exist intraday, and
        # anchoring on it would leak a partially-formed day into the daily lookbacks.
        daily_end = d - timedelta(days=1)
        daily_start = d - timedelta(days=self._lookback_days)
        index_closes = [b.close for b in self._store.get_bars_1d(self._index_symbol, daily_start, daily_end)]
        flagged = frozenset(r["symbol"] for r in self._store.get_flagged_instrument_days(d))
        ex_dates: dict[str, list[date]] = {}
        for row in self._store.get_corp_actions(
            ex_from=d, ex_to=d + timedelta(days=self._ex_horizon_days)
        ):
            ex_dates.setdefault(row["symbol"], []).append(row["ex_date"])
        cache = _DayCache(
            d=d,
            daily_start=daily_start,
            daily_end=daily_end,
            session_open=session.open if session is not None else None,
            trade_window=window,
            flagged=flagged,
            index_closes=index_closes,
            ex_dates=ex_dates,
        )
        for symbol in self._momentum_universe:
            self._load_daily(cache, symbol)
        _log.info(
            "scan_context_day_built",
            d=d.isoformat(), index_closes=len(index_closes), flagged=len(flagged),
            ex_date_symbols=len(ex_dates), preloaded=len(self._momentum_universe),
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
