"""§3.2.5 ``FeatureEngine`` — deterministic, versioned feature computation (§6.2 feature set v2).

Two pinned surfaces (§3.2.5)::

    daily_snapshot(d: date) -> None          # writes features_daily rows to DuckDB (nightly job, §4.4)
    intraday_snapshot(symbol) -> FeatureVector   # from live bars; persisted to feature_snapshots

Determinism contract (§9.6): both paths are pure functions of (store contents, d/now) — no wall
clock, no network, no randomness (the only minted value is the snapshot ULID, which is identity, not
data). The same store contents always produce byte-identical ``features_daily`` rows (canonical
``features_json``); the §9.1 feature tests assert exactly that.

Feature set v2 (§6.2, ``feature_set_version = 2`` stamped on every row):

* **Price/volatility (per symbol, from ``bars_1d``):** returns 1/5/20d, ATR(14) daily, realized vol
  20d (annualized stdev of log returns), gap stats (today's open gap + 20d mean |gap|), distance to
  20/50/200-DMA, day-range position.
* **Volume/liquidity:** 20d median traded value (close x volume).
* **Market context:** NIFTY 50 returns + trend state, India VIX level/delta (E5), universe
  advance-decline, expiry-day flag, results-day flag (``earnings_calendar``), ex-date proximity
  (A12), surveillance status (A8), ``flagged_instrument_day`` (block deals). Sector "index" returns
  are the equal-weight mean return of the universe's same-sector constituents (``sector_map``) —
  a deterministic proxy; Phase 1 has no sectoral-index bar feed.
* **Sentiment / catalyst (v2, §6.2):** decay-weighted ``sentiment_{symbol,sector,theme,market}``
  from the LATEST ``sentiment_agg`` rows (the digest's own decay-weighted sum, §2.7 step 5(i)) plus
  per-symbol catalyst fields from TODAY's ``catalyst_watchlist`` (§2.7 step 5(ii)) —
  ``on_watchlist``, ``watchlist_grade``, ``catalyst_event_type``, ``catalyst_event_age_h``,
  ``materiality``, ``catalyst_source_domain_count`` — and ``sentiment_available``. Each half falls
  back independently to its slice of the PINNED absent-news defaults
  (:data:`ABSENT_NEWS_DEFAULTS`) when its source is empty: sentiment* = 0 / ``sentiment_available``
  = False when no digest has ever run; ``on_watchlist`` = False / ``materiality`` = 0 (grade/event
  fields = None) when today's watchlist has no row for the symbol. An in-distribution "no news"
  vector, never NaN/missing (§6.2, chaos case 20). Only ``sentiment_agg`` + ``catalyst_watchlist``
  are read here — never ``news``/``news_clusters`` directly (§2.4 single-seam discipline).
* **Microstructure (intraday only):** opening-range stats, VWAP distance, relative volume (both the
  legacy full-day-denominator ``rel_volume`` and the time-of-day-normalized ``rel_volume_tod``),
  ATR(14) 1m, last-30-bar summary.

Schema stability: every row carries the FULL key set (:data:`DAILY_FEATURE_KEYS` /
:data:`INTRADAY_FEATURE_KEYS`); unavailable values are ``None`` (indicator warm-up), never absent —
River and the LLM context see a fixed vocabulary.

Indicator math is reused verbatim from ``engine.strategy.indicators`` (same §3.2.5 module family) so
live features and the vectorbt backtest sweep cannot diverge. Statistics are floats (never money);
the expiry-day rule (monthly F&O expiry = last ``expiry_weekday`` of the month, holiday-rolled to
the previous trading day) defaults to Tuesday per the 2025 NSE expiry-day move [VERIFY Phase-1].
"""

from __future__ import annotations

import math
import statistics
from calendar import monthrange
from collections.abc import Mapping
from datetime import date, datetime, time, timedelta
from typing import Any

from engine.core.calendar import NSECalendar
from engine.core.clock import Clock
from engine.core.log import get_logger
from engine.features.snapshots import (
    FEATURE_SET_VERSION,
    FeatureVector,
    features_json,
    new_feature_vector,
    persist_snapshot,
)
from engine.marketdata.store import DailyBar, MarketStore
from engine.strategy.indicators import vwap, wilder_atr

_log = get_logger("engine.features.engine")

#: §6.2 PINNED absent-news defaults (a news outage falls back to this SAME vector, so non-`cat`
#: candidate ranking cannot shift — chaos case 20). Values are in-distribution scalars, never
#: None/NaN. Changing any of these is a plan change, not a tweak. (The catalyst DESCRIPTIVE fields —
#: watchlist_grade / catalyst_event_type / catalyst_event_age_h / catalyst_source_domain_count —
#: are not pinned scalars: they default to None like any other warm-up/unavailable feature.)
ABSENT_NEWS_DEFAULTS: dict[str, float | bool] = {
    "sentiment_symbol": 0.0,
    "sentiment_sector": 0.0,
    "sentiment_theme": 0.0,
    "sentiment_market": 0.0,
    "on_watchlist": False,
    "materiality": 0.0,
    "sentiment_available": False,
}

#: Catalyst-watchlist DESCRIPTIVE fields (§6.2 v2) — present only when the symbol has a today's
#: ``catalyst_watchlist`` row; None otherwise (not part of the pinned never-None scalar set above).
_CATALYST_DESCRIPTIVE_KEYS: tuple[str, ...] = (
    "watchlist_grade", "catalyst_event_type", "catalyst_event_age_h", "catalyst_source_domain_count",
)

#: The complete, ordered v2 daily feature vocabulary — every features_daily row carries exactly
#: these keys (None = warm-up/unavailable; ABSENT_NEWS_DEFAULTS keys always in-distribution scalars).
DAILY_FEATURE_KEYS: tuple[str, ...] = (
    # price / volatility (per symbol, bars_1d)
    "ret_1d", "ret_5d", "ret_20d",
    "atr14_1d", "realized_vol_20d",
    "gap_open_pct", "gap_abs_mean_20d",
    "dist_sma20", "dist_sma50", "dist_sma200",
    "day_range_pos",
    # volume / liquidity
    "median_traded_value_20d",
    # market context (same values on every symbol's row for the day)
    "nifty_ret_1d", "nifty_ret_5d", "nifty_ret_20d", "nifty_trend_state",
    "vix_level", "vix_delta_1d",
    "advance_decline", "expiry_day",
    # per-symbol context
    "sector", "sector_ret_1d", "sector_ret_5d",
    "results_day", "days_to_ex_date", "ex_date_within_5d",
    "surveillance", "surveillance_flagged", "flagged_instrument_day",
    # sentiment / catalyst (§6.2 v2) — pinned absent-news defaults + catalyst descriptive fields
    *ABSENT_NEWS_DEFAULTS,
    *_CATALYST_DESCRIPTIVE_KEYS,
)

#: The complete intraday (microstructure) vocabulary (§6.2 v2) — FeatureVector.features keys.
INTRADAY_FEATURE_KEYS: tuple[str, ...] = (
    "bar_count", "minutes_elapsed",
    "or_complete", "or_high", "or_low", "or_range_pct",
    "last_price", "cum_volume",
    "vwap", "vwap_dist",
    "atr14_1m", "rel_volume", "rel_volume_tod",
    "last30_ret", "last30_range_pct", "last30_up_frac", "last30_volume",
    # sentiment / catalyst (§6.2 v2) — same block + fallback rules as DAILY_FEATURE_KEYS
    *ABSENT_NEWS_DEFAULTS,
    *_CATALYST_DESCRIPTIVE_KEYS,
)

_ANNUALIZATION = math.sqrt(252.0)   # NSE ~252 trading sessions/year (realized-vol convention)

#: How many of the 20 completed sessions must have a usable cumulative volume at the same elapsed
#: minute before ``rel_volume_tod`` is computed. A "median pace" built from three surviving sessions
#: is noise wearing a baseline's clothes — below the floor the feature is None, never a guess (§6.2
#: warm-up rule). Missing 1m history is ordinary: only the intraday tick watchlist carries minute
#: bars, so a symbol swept daily can have 20 daily bars and no minute history at all.
_REL_VOLUME_TOD_MIN_SESSIONS = 10


# --------------------------------------------------------------------------- pure stat helpers
def _ret(closes: list[float], k: int) -> float | None:
    """k-session fractional return: closes[-1]/closes[-1-k] - 1 (None during warm-up)."""
    if len(closes) <= k or closes[-1 - k] <= 0.0:
        return None
    return closes[-1] / closes[-1 - k] - 1.0


def _dist_sma(closes: list[float], n: int) -> float | None:
    """Fractional distance of the last close to its n-session SMA (None during warm-up)."""
    if len(closes) < n:
        return None
    m = sum(closes[-n:]) / n
    return closes[-1] / m - 1.0 if m > 0.0 else None


def _trend_state(closes: list[float]) -> int | None:
    """+1 above a rising structure (close > SMA50 > SMA200), -1 below a falling one, else 0.

    None until 200 sessions exist — the deepest v1 lookback (§6.2), matching the warm-up gate.
    """
    if len(closes) < 200:
        return None
    sma50 = sum(closes[-50:]) / 50.0
    sma200 = sum(closes[-200:]) / 200.0
    last = closes[-1]
    if last > sma50 > sma200:
        return 1
    if last < sma50 < sma200:
        return -1
    return 0


def _realized_vol_20d(closes: list[float]) -> float | None:
    """Annualized sample stdev of the last 20 daily log returns (needs 21 closes)."""
    if len(closes) < 21:
        return None
    rets = [math.log(closes[i] / closes[i - 1]) for i in range(len(closes) - 20, len(closes))]
    return statistics.stdev(rets) * _ANNUALIZATION


def _gap_stats(opens: list[float], closes: list[float]) -> tuple[float | None, float | None]:
    """(today's open gap vs prior close, mean |gap| over the last 20 sessions)."""
    gap_open = None
    if len(closes) >= 2 and closes[-2] > 0.0:
        gap_open = opens[-1] / closes[-2] - 1.0
    gap_abs_mean = None
    if len(closes) >= 21:
        gaps = [
            abs(opens[i] / closes[i - 1] - 1.0)
            for i in range(len(closes) - 20, len(closes))
            if closes[i - 1] > 0.0
        ]
        gap_abs_mean = sum(gaps) / len(gaps) if gaps else None
    return gap_open, gap_abs_mean


class FeatureEngine:
    """Deterministic feature computation over ``MarketStore`` (§3.2.5/§6.2).

    Parameters
    ----------
    store / clock / calendar:
        The single-writer :class:`MarketStore`, the platform :class:`Clock` (the ONLY time source,
        §3.2), and the :class:`NSECalendar` (session open for intraday anchoring; expiry-day flag).
    index_symbol / vix_symbol:
        Canonical ``bars_1d`` symbols the backfill job persists NIFTY 50 / India VIX under
        (must match ``ops.warmup.WarmupGate`` — §7.1 ``regime_data_ready``).
    opening_range_minutes:
        Opening-range window for the intraday OR stats (default 30 = 09:15-09:45, the §6.1 anchor).
    expiry_weekday:
        Monday=0 weekday of the monthly F&O expiry; default 1 (Tuesday) per the 2025 NSE
        expiry-day migration [VERIFY Phase-1 — flips back to 3/Thursday if NSE reverts].
    daily_lookback_days:
        Calendar-day span fetched from ``bars_1d`` (400 covers the 200-session DMA plus holidays).
    ex_date_horizon_days:
        Corp-action look-ahead for the ex-date proximity feature (A12).
    """

    def __init__(
        self,
        store: MarketStore,
        clock: Clock,
        calendar: NSECalendar,
        *,
        index_symbol: str = "NIFTY 50",
        vix_symbol: str = "INDIA VIX",
        opening_range_minutes: int = 30,
        expiry_weekday: int = 1,
        daily_lookback_days: int = 400,
        ex_date_horizon_days: int = 10,
    ) -> None:
        if not 0 <= expiry_weekday <= 6:
            raise ValueError(f"expiry_weekday must be 0..6 (Mon..Sun), got {expiry_weekday}")
        if opening_range_minutes < 1:
            raise ValueError("opening_range_minutes must be >= 1")
        self._store = store
        self._clock = clock
        self._calendar = calendar
        self._index_symbol = index_symbol
        self._vix_symbol = vix_symbol
        self._or_minutes = int(opening_range_minutes)
        self._expiry_weekday = int(expiry_weekday)
        self._lookback_days = int(daily_lookback_days)
        self._ex_horizon_days = int(ex_date_horizon_days)

    # ------------------------------------------------------------------ pinned surface (§3.2.5)
    def daily_snapshot(self, d: date) -> None:
        """Compute + upsert the §6.2 v2 ``features_daily`` rows for day ``d``'s universe.

        Nightly-job entry point (§4.4): one row per INCLUDED ``universe_daily`` symbol, stamped
        ``feature_set_version = 2``, features serialized with the canonical ``features_json`` so a
        re-run writes byte-identical rows (idempotent upsert on (d, symbol, version)).
        """
        universe = [r["symbol"] for r in self._store.get_universe_daily(d, included_only=True)]
        if not universe:
            _log.warning("features_daily_skipped_empty_universe", d=d.isoformat())
            return
        start = d - timedelta(days=self._lookback_days)

        bars_by_symbol = {sym: self._store.get_bars_1d(sym, start, d) for sym in universe}
        ret1: dict[str, float | None] = {}
        ret5: dict[str, float | None] = {}
        for sym, bars in bars_by_symbol.items():
            closes = [float(b.close) for b in bars] if bars and bars[-1].d == d else []
            ret1[sym] = _ret(closes, 1)
            ret5[sym] = _ret(closes, 5)

        market = self._market_context(d, start, ret1)
        sector_of = {r["symbol"]: r["sector"] for r in self._store.get_sector_map(as_of=d)}
        sector_ret1 = _sector_means(sector_of, ret1)
        sector_ret5 = _sector_means(sector_of, ret5)
        results_today = {r["symbol"] for r in self._store.get_earnings_calendar(d, d)}
        days_to_ex = self._days_to_ex_date(d, universe)
        surveillance = {
            r["tradingsymbol"]: r.get("surveillance") for r in self._store.get_instruments_daily(d)
        }
        flagged_today = {r["symbol"] for r in self._store.get_flagged_instrument_days(d)}
        sentiment_by_key, theme_symbols, watchlist_by_symbol, sentiment_available = (
            self._sentiment_catalyst_context(d)
        )

        rows: list[dict[str, Any]] = []
        for sym in universe:
            feats: dict[str, Any] = dict.fromkeys(DAILY_FEATURE_KEYS)
            feats.update(self._price_features(bars_by_symbol[sym], d))
            feats.update(market)
            sector = sector_of.get(sym)
            surv = surveillance.get(sym) or None
            dte = days_to_ex.get(sym)
            feats.update({
                "sector": sector,
                "sector_ret_1d": sector_ret1.get(sector) if sector else None,
                "sector_ret_5d": sector_ret5.get(sector) if sector else None,
                "results_day": sym in results_today,
                "days_to_ex_date": dte,
                "ex_date_within_5d": dte is not None and dte <= 5,
                "surveillance": surv,
                "surveillance_flagged": surv is not None,
                "flagged_instrument_day": sym in flagged_today,
            })
            feats.update(_sentiment_catalyst_features(         # §6.2 v2 — LAST, nothing may override
                sym, sector, sentiment_by_key, theme_symbols, watchlist_by_symbol, sentiment_available,
            ))
            rows.append({
                "d": d,
                "symbol": sym,
                "feature_set_version": FEATURE_SET_VERSION,
                "features": features_json(feats),
            })
        self._store.upsert_features_daily(rows)
        _log.info(
            "features_daily_written",
            d=d.isoformat(), rows=len(rows), feature_set_version=FEATURE_SET_VERSION,
        )

    def intraday_snapshot(self, symbol: str) -> FeatureVector:
        """Microstructure features from today's live 1m bars (§6.2), persisted to
        ``feature_snapshots`` keyed by a freshly minted ``features_snapshot_id`` (ULID) — the id
        proposals and ledger rows reference (§4.3). Missing data ⇒ ``None`` values (warm-up),
        never an error; the key set is always :data:`INTRADAY_FEATURE_KEYS`.
        """
        now = self._clock.now()
        d = self._clock.today()
        session = self._calendar.session(d)
        session_open = session.open if session is not None else self._clock.combine(d, time(9, 15))
        bars = self._store.get_bars_1m(symbol, session_open, now)
        feats = self._intraday_features(symbol, d, session_open, now, bars)
        sentiment_by_key, theme_symbols, watchlist_by_symbol, sentiment_available = (
            self._sentiment_catalyst_context(d)
        )
        sector = {r["symbol"]: r["sector"] for r in self._store.get_sector_map(as_of=d)}.get(symbol)
        feats.update(_sentiment_catalyst_features(             # §6.2 v2
            symbol, sector, sentiment_by_key, theme_symbols, watchlist_by_symbol, sentiment_available,
        ))
        vector = new_feature_vector(symbol, now, feats)
        persist_snapshot(self._store, vector)
        _log.info(
            "feature_snapshot_written",
            symbol=symbol, snapshot_id=vector.features_snapshot_id, bars=len(bars),
        )
        return vector

    # ------------------------------------------------------------------ daily internals
    def _price_features(self, bars: list[DailyBar], d: date) -> dict[str, Any]:
        """Per-symbol §6.2 price/volatility/liquidity block. All-None unless the day-``d`` bar
        exists (an EOD snapshot anchored anywhere else would silently describe the wrong day)."""
        out: dict[str, Any] = dict.fromkeys((
            "ret_1d", "ret_5d", "ret_20d", "atr14_1d", "realized_vol_20d",
            "gap_open_pct", "gap_abs_mean_20d", "dist_sma20", "dist_sma50", "dist_sma200",
            "day_range_pos", "median_traded_value_20d",
        ))
        if not bars or bars[-1].d != d:
            return out
        opens = [float(b.open) for b in bars]
        highs = [float(b.high) for b in bars]
        lows = [float(b.low) for b in bars]
        closes = [float(b.close) for b in bars]
        volumes = [b.volume for b in bars]

        out["ret_1d"] = _ret(closes, 1)
        out["ret_5d"] = _ret(closes, 5)
        out["ret_20d"] = _ret(closes, 20)
        if len(bars) >= 14:
            out["atr14_1d"] = float(wilder_atr(highs, lows, closes, 14).iloc[-1])
        out["realized_vol_20d"] = _realized_vol_20d(closes)
        out["gap_open_pct"], out["gap_abs_mean_20d"] = _gap_stats(opens, closes)
        out["dist_sma20"] = _dist_sma(closes, 20)
        out["dist_sma50"] = _dist_sma(closes, 50)
        out["dist_sma200"] = _dist_sma(closes, 200)
        rng = highs[-1] - lows[-1]
        out["day_range_pos"] = (closes[-1] - lows[-1]) / rng if rng > 0.0 else 0.5
        if len(bars) >= 20:
            out["median_traded_value_20d"] = statistics.median(
                c * v for c, v in zip(closes[-20:], volumes[-20:], strict=True)
            )
        return out

    def _market_context(
        self, d: date, start: date, ret1: dict[str, float | None]
    ) -> dict[str, Any]:
        """Day-level market-context block (identical on every symbol's row)."""
        idx_closes = [float(b.close) for b in self._store.get_bars_1d(self._index_symbol, start, d)]
        vix_closes = [float(b.close) for b in self._store.get_bars_1d(self._vix_symbol, start, d)]
        directional = [r for r in ret1.values() if r is not None]
        advance_decline = None
        if directional:
            adv = sum(1 for r in directional if r > 0.0)
            dec = sum(1 for r in directional if r < 0.0)
            advance_decline = (adv - dec) / len(directional)
        return {
            "nifty_ret_1d": _ret(idx_closes, 1),
            "nifty_ret_5d": _ret(idx_closes, 5),
            "nifty_ret_20d": _ret(idx_closes, 20),
            "nifty_trend_state": _trend_state(idx_closes),
            "vix_level": vix_closes[-1] if vix_closes else None,
            "vix_delta_1d": vix_closes[-1] - vix_closes[-2] if len(vix_closes) >= 2 else None,
            "advance_decline": advance_decline,
            "expiry_day": self._is_expiry_day(d),
        }

    def _days_to_ex_date(self, d: date, universe: list[str]) -> dict[str, int]:
        """symbol -> calendar days until its NEAREST upcoming ex-date within the horizon (A12)."""
        rows = self._store.get_corp_actions(
            ex_from=d, ex_to=d + timedelta(days=self._ex_horizon_days)
        )
        out: dict[str, int] = {}
        in_universe = set(universe)
        for row in rows:
            sym = row["symbol"]
            if sym not in in_universe:
                continue
            days = (row["ex_date"] - d).days
            if sym not in out or days < out[sym]:
                out[sym] = days
        return out

    def _is_expiry_day(self, d: date) -> bool:
        """Monthly F&O expiry flag: last ``expiry_weekday`` of ``d``'s month, rolled BACK to the
        previous trading day when it is a holiday (NSE convention). Fails closed (False) when the
        calendar has no data for the month (R6: no calendar, no assumptions)."""
        probe = date(d.year, d.month, monthrange(d.year, d.month)[1])
        while probe.weekday() != self._expiry_weekday:
            probe -= timedelta(days=1)
        while probe.month == d.month and not self._calendar.is_trading_day(probe):
            probe -= timedelta(days=1)
        return probe.month == d.month and d == probe

    # ------------------------------------------------------------------ shared: sentiment/catalyst (§6.2 v2)
    def _sentiment_catalyst_context(
        self, d: date
    ) -> tuple[dict[tuple[str, str], float], dict[str, set[str]], dict[str, dict[str, Any]], bool]:
        """Day-level sentiment/catalyst context, shared by ``daily_snapshot`` and
        ``intraday_snapshot`` so both compute from the SAME digest snapshot within one call:

        * ``sentiment_by_key`` — the LATEST ``sentiment_agg`` rows (whichever digest run stamped
          them last; the digest runs once daily, §2.7 step 5(i)), keyed by ``(scope, scope_key)``.
        * ``theme_symbols`` — ``theme_map`` as ``theme -> {symbols}`` for the best-theme lookup.
        * ``watchlist_by_symbol`` — TODAY's ``catalyst_watchlist`` rows keyed by symbol (§2.7 step
          5(ii); at most one row per symbol per day — best cluster wins upstream).
        * ``sentiment_available`` — True only when a digest has EVER run (chaos case 20 gate); the
          per-symbol/-sector/-theme/-market sentiment values fall back to 0.0 otherwise, never None.

        Only ``sentiment_agg`` / ``theme_map`` / ``catalyst_watchlist`` are read — never
        ``news``/``news_clusters`` directly (§2.4 single-seam discipline).
        """
        latest_as_of = self._store.latest_sentiment_as_of()
        sentiment_by_key = {
            (r["scope"], r["scope_key"]): float(r["value"])
            for r in (self._store.get_sentiment_agg(latest_as_of) if latest_as_of is not None else [])
        }
        theme_symbols = {r["theme"]: set(r["symbols"] or []) for r in self._store.get_theme_map()}
        watchlist_by_symbol = {r["symbol"]: r for r in self._store.get_catalyst_watchlist(d)}
        return sentiment_by_key, theme_symbols, watchlist_by_symbol, latest_as_of is not None

    # ------------------------------------------------------------------ intraday internals
    def _intraday_features(
        self, symbol: str, d: date, session_open: datetime, now: datetime, bars: list
    ) -> dict[str, Any]:
        feats: dict[str, Any] = dict.fromkeys(INTRADAY_FEATURE_KEYS)
        feats["bar_count"] = len(bars)
        feats["minutes_elapsed"] = max(int((now - session_open).total_seconds() // 60), 0)
        or_end = session_open + timedelta(minutes=self._or_minutes)
        feats["or_complete"] = now >= or_end

        or_bars = [b for b in bars if b.ts_minute < or_end]
        if or_bars:
            or_high = max(float(b.high) for b in or_bars)
            or_low = min(float(b.low) for b in or_bars)
            feats["or_high"] = or_high
            feats["or_low"] = or_low
            feats["or_range_pct"] = (or_high - or_low) / or_low if or_low > 0.0 else None

        cum_volume = 0
        if bars:
            highs = [float(b.high) for b in bars]
            lows = [float(b.low) for b in bars]
            closes = [float(b.close) for b in bars]
            volumes = [b.volume for b in bars]
            cum_volume = sum(volumes)
            feats["last_price"] = closes[-1]
            feats["cum_volume"] = cum_volume
            v = float(vwap(highs, lows, closes, volumes).iloc[-1])
            feats["vwap"] = v                                       # NaN -> None via clean_features
            if math.isfinite(v) and v > 0.0:
                feats["vwap_dist"] = closes[-1] / v - 1.0
            if len(bars) >= 14:
                feats["atr14_1m"] = float(wilder_atr(highs, lows, closes, 14).iloc[-1])
            w_closes = closes[-30:]
            w_opens = [float(b.open) for b in bars[-30:]]
            w_low = min(lows[-30:])
            feats["last30_ret"] = w_closes[-1] / w_closes[0] - 1.0 if w_closes[0] > 0.0 else None
            feats["last30_range_pct"] = (max(highs[-30:]) - w_low) / w_low if w_low > 0.0 else None
            feats["last30_up_frac"] = (
                sum(1 for o, c in zip(w_opens, w_closes, strict=True) if c > o) / len(w_closes)
            )
            feats["last30_volume"] = sum(volumes[-30:])

        # Relative volume: today's cumulative volume vs the 20d median DAILY volume (strictly
        # before today — day d has no completed daily bar intraday). The 20 sessions this read
        # yields are ALSO the sessions rel_volume_tod normalizes against — one daily read, not two.
        hist = self._store.get_bars_1d(symbol, d - timedelta(days=90), d - timedelta(days=1))[-20:]
        if bars and len(hist) == 20:
            med = statistics.median(b.volume for b in hist)
            feats["rel_volume"] = cum_volume / med if med > 0 else None
            feats["rel_volume_tod"] = self._rel_volume_tod(
                symbol, [b.d for b in hist], session_open, now, cum_volume,
            )
        return feats

    def _rel_volume_tod(
        self,
        symbol: str,
        hist_days: list[date],
        session_open: datetime,
        now: datetime,
        cum_volume: int,
    ) -> float | None:
        """Time-of-day-normalized relative volume: today's cumulative volume ÷ the MEDIAN cumulative
        volume of the same 20 completed sessions through the SAME elapsed minutes after their open.

        1.0 = a typical participation pace for this point of the session — the number a reader
        actually means by "relative volume". ``rel_volume`` is kept unchanged beside it for
        continuity, but its denominator is the median FULL-DAY volume, so it is structurally
        ~0.02-0.3 for most of the session and reads as "nobody is trading this" on an ordinary tape
        (the misreading WO-20's legend documents and this feature retires).

        Returns None — never a guess — when the session has not opened yet or fewer than
        :data:`_REL_VOLUME_TOD_MIN_SESSIONS` of the 20 sessions have a usable (>0) cumulative volume
        at the cutoff. Each session's cutoff is its own 09:15 IST + elapsed and is EXCLUSIVE, so the
        bar stamped exactly at the cutoff minute is out of both today's sum and every historical one.

        Cost: ONE ranged ``get_bars_1m`` call spanning the whole window (grouped by date here), on
        the same gate that already guards ``rel_volume`` — a symbol with no bars today never pays it.
        """
        elapsed = now - session_open
        if elapsed <= timedelta(0):
            return None
        cutoffs = {day: self._clock.combine(day, time(9, 15)) + elapsed for day in hist_days}
        hist_bars = self._store.get_bars_1m(
            symbol, self._clock.combine(hist_days[0], time(9, 15)), cutoffs[hist_days[-1]],
        )
        cum_by_day: dict[date, int] = dict.fromkeys(hist_days, 0)
        bars_by_day: set[date] = set()
        for b in hist_bars:                      # the range spans whole sessions; clip each to ITS cutoff
            day = b.ts_minute.date()
            cutoff = cutoffs.get(day)
            if cutoff is None or b.ts_minute >= cutoff:
                continue
            cum_by_day[day] += b.volume
            bars_by_day.add(day)
        valid = [cum_by_day[day] for day in hist_days if day in bars_by_day and cum_by_day[day] > 0]
        if len(valid) < _REL_VOLUME_TOD_MIN_SESSIONS:
            _log.debug(
                "rel_volume_tod_insufficient_history",
                symbol=symbol, valid_sessions=len(valid), required=_REL_VOLUME_TOD_MIN_SESSIONS,
            )
            return None
        return cum_volume / statistics.median(valid)


def _sector_means(
    sector_of: dict[str, str], rets: dict[str, float | None]
) -> dict[str, float]:
    """Equal-weight mean return per sector over the universe symbols with data (§6.2 proxy)."""
    by_sector: dict[str, list[float]] = {}
    for sym, r in rets.items():
        sector = sector_of.get(sym)
        if sector and sector != "UNCLASSIFIED" and r is not None:
            by_sector.setdefault(sector, []).append(r)
    return {sector: sum(v) / len(v) for sector, v in by_sector.items()}


def _sentiment_catalyst_features(
    symbol: str,
    sector: str | None,
    sentiment_by_key: Mapping[tuple[str, str], float],
    theme_symbols: Mapping[str, set[str]],
    watchlist_by_symbol: Mapping[str, Mapping[str, Any]],
    sentiment_available: bool,
) -> dict[str, Any]:
    """§6.2 v2 sentiment/catalyst block for ONE symbol, from the shared day-level context built by
    :meth:`FeatureEngine._sentiment_catalyst_context`. Falls back field-by-field to the PINNED
    :data:`ABSENT_NEWS_DEFAULTS` — the two halves are independent (a symbol with no watchlist row
    still gets its real sentiment_* values on a day the digest ran, and vice versa is impossible but
    not assumed): sentiment_* stay 0.0 / ``sentiment_available`` stays False unless a digest has
    EVER run; ``on_watchlist``/``materiality`` stay False/0.0 (descriptive fields stay None) unless
    TODAY's ``catalyst_watchlist`` has a row for ``symbol`` (chaos case 20, never NaN/None for the
    pinned numeric fields).
    """
    out: dict[str, Any] = dict(ABSENT_NEWS_DEFAULTS)
    out["watchlist_grade"] = None
    out["catalyst_event_type"] = None
    out["catalyst_event_age_h"] = None
    out["catalyst_source_domain_count"] = None
    out["sentiment_available"] = sentiment_available
    if sentiment_available:
        out["sentiment_symbol"] = sentiment_by_key.get(("symbol", symbol), 0.0)
        if sector:
            out["sentiment_sector"] = sentiment_by_key.get(("sector", sector), 0.0)
        best_theme_value: float | None = None
        for theme, symbols in theme_symbols.items():
            if symbol not in symbols:
                continue
            value = sentiment_by_key.get(("theme", theme))
            if value is not None and (best_theme_value is None or abs(value) > abs(best_theme_value)):
                best_theme_value = value
        out["sentiment_theme"] = best_theme_value if best_theme_value is not None else 0.0
        out["sentiment_market"] = sentiment_by_key.get(("market", "market"), 0.0)
    watch = watchlist_by_symbol.get(symbol)
    if watch is not None:
        out["on_watchlist"] = True
        out["watchlist_grade"] = watch["grade"]
        out["catalyst_event_type"] = watch.get("event_type")
        out["catalyst_event_age_h"] = watch.get("event_age_h")
        materiality = watch.get("materiality")
        out["materiality"] = float(materiality) if materiality is not None else 0.0
        out["catalyst_source_domain_count"] = watch.get("source_domain_count")
    return out
