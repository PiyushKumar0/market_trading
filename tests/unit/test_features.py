"""FeatureEngine (§3.2.5/§6.2 v1): deterministic daily + intraday features on synthetic bars —
same input => byte-identical rows (§9.6), the PINNED absent-news defaults, the v1 version stamp on
every row/snapshot, market context wiring, and the canonical snapshot serialization contract."""

from __future__ import annotations

import json
import math
import statistics
import threading
from datetime import date, time, timedelta
from decimal import Decimal

import numpy as np
import pytest

from engine.core.calendar import NSECalendar
from engine.core.config import config_dir
from engine.core.types import Bar
from engine.features.engine import (
    _SENTIMENT_CACHE_TTL_S,  # noqa: PLC2701 - the TTL under test; a literal 60 would drift silently
    ABSENT_NEWS_DEFAULTS,
    DAILY_FEATURE_KEYS,
    INTRADAY_FEATURE_KEYS,
    FeatureEngine,
    _prox_high,
    _trend_state,
)
from engine.features.snapshots import (
    FEATURE_SET_VERSION,
    clean_features,
    features_json,
    load_snapshot,
)
from engine.marketdata.store import DailyBar, MarketStore
from tests.conftest import FIXED_NOW

D = FIXED_NOW.date()                      # 2026-06-17, a real trading day (conftest)


@pytest.fixture
def store(tmp_path, clock):
    s = MarketStore(tmp_path / "market.duckdb", tmp_path / "parquet", clock)
    s.open()
    yield s
    s.close()


@pytest.fixture
def calendar(clock) -> NSECalendar:
    return NSECalendar(config_dir() / "calendar", clock)


@pytest.fixture
def engine(store, clock, calendar) -> FeatureEngine:
    return FeatureEngine(store, clock, calendar)


# --------------------------------------------------------------------------- synthetic data
def _weekdays_back(end: date, n: int) -> list[date]:
    """The last ``n`` weekdays ending at ``end`` inclusive, ascending (bars_1d needs no calendar)."""
    days: list[date] = []
    probe = end
    while len(days) < n:
        if probe.weekday() < 5:
            days.append(probe)
        probe -= timedelta(days=1)
    return list(reversed(days))


def _seed_daily(store, symbol: str, days: list[date], base: float, drift: float, wiggle: float):
    """Deterministic OHLCV path: close = base + drift*i + wiggle*(i%5); open = prev close + 0.10."""
    bars = []
    prev: Decimal | None = None
    for i, dd in enumerate(days):
        close = Decimal(f"{base + drift * i + wiggle * (i % 5):.2f}")
        open_ = (prev if prev is not None else close) + Decimal("0.10")
        bars.append(DailyBar(
            symbol=symbol, d=dd, open=open_,
            high=max(open_, close) + Decimal("0.50"), low=min(open_, close) - Decimal("0.50"),
            close=close, volume=10_000 + (i % 7) * 100,
        ))
        prev = close
    store.upsert_bars_1d(bars)
    return bars


@pytest.fixture
def seeded(store, clock):
    """Two-symbol universe (AAA advancing, BBB declining), NIFTY 50 + INDIA VIX history, sector
    map, per-symbol context rows (earnings/corp action/surveillance/flagged), one excluded symbol."""
    days = _weekdays_back(D, 210)
    data = {
        "AAA": _seed_daily(store, "AAA", days, 100.0, 0.05, 0.2),
        "BBB": _seed_daily(store, "BBB", days, 200.0, -0.05, -0.2),
        "NIFTY 50": _seed_daily(store, "NIFTY 50", days, 20000.0, 2.0, 5.0),
        "INDIA VIX": _seed_daily(store, "INDIA VIX", days[-30:], 14.0, 0.05, 0.0),
    }
    store.upsert_universe_daily([
        {"d": D, "symbol": "AAA", "included": True},
        {"d": D, "symbol": "BBB", "included": True},
        {"d": D, "symbol": "ZZZ", "included": False, "exclusion_reasons": ["surveillance"]},
    ])
    store.upsert_sector_map(D, [{"symbol": "AAA", "sector": "IT"}, {"symbol": "BBB", "sector": "IT"}])
    store.upsert_instruments_daily([
        {"d": D, "instrument_token": 1, "tradingsymbol": "AAA"},
        {"d": D, "instrument_token": 2, "tradingsymbol": "BBB", "surveillance": "GSM Stage 1"},
    ])
    store.upsert_earnings_calendar([{"symbol": "AAA", "event_date": D}])
    store.upsert_corp_actions([{"symbol": "BBB", "ex_date": D + timedelta(days=3), "kind": "dividend"}])
    store.upsert_flagged_instrument_days([{"symbol": "AAA", "d": D, "reason": "block_deal"}])
    return data


def _rows_by_symbol(store) -> dict[str, dict]:
    return {r["symbol"]: json.loads(r["features"]) for r in store.get_features_daily(D)}


def _seed_sentiment_layer(store, clock):
    """§2.7 digest outputs (sentiment_agg + theme_map + catalyst_watchlist) for the ``seeded``
    universe: AAA (sector IT) gets a symbol row AND is on today's watchlist (`originating`, graded);
    BBB (also sector IT) gets no symbol row and no watchlist row — only the sector/market fan-out and
    one matching theme, so the two symbols' §6.2 v2 blocks diverge on every axis."""
    as_of = clock.now() - timedelta(hours=2)
    store.upsert_sentiment_agg([
        {"scope": "symbol", "scope_key": "AAA", "as_of": as_of, "value": 0.42},
        {"scope": "sector", "scope_key": "IT", "as_of": as_of, "value": 0.15},
        {"scope": "theme", "scope_key": "THEME1", "as_of": as_of, "value": 0.05},
        {"scope": "theme", "scope_key": "THEME2", "as_of": as_of, "value": -0.30},
        {"scope": "market", "scope_key": "market", "as_of": as_of, "value": 0.10},
    ])
    store.upsert_theme_map([
        {"theme": "THEME1", "keywords": ["kw1"], "symbols": ["AAA", "BBB"]},   # both symbols
        {"theme": "THEME2", "keywords": ["kw2"], "symbols": ["AAA"]},          # AAA only, |value| wins
    ])
    store.replace_catalyst_watchlist(D, [{
        "symbol": "AAA", "grade": "originating", "direction": "long", "event_type": "results",
        "materiality": 0.75, "source_domain_count": 3, "event_age_h": 5.5,
    }])
    return as_of


# --------------------------------------------------------------------------- daily snapshot
def test_daily_snapshot_versioned_universe_scoped_full_key_set(engine, store, seeded):
    engine.daily_snapshot(D)
    rows = store.get_features_daily(D)
    assert {r["symbol"] for r in rows} == {"AAA", "BBB"}          # excluded ZZZ gets no row
    for r in rows:
        assert r["feature_set_version"] == FEATURE_SET_VERSION == 2
        feats = json.loads(r["features"])
        assert set(feats) == set(DAILY_FEATURE_KEYS)              # stable vocabulary, always


def test_daily_price_features_match_hand_math(engine, store, seeded):
    engine.daily_snapshot(D)
    feats = _rows_by_symbol(store)["AAA"]
    bars = seeded["AAA"]
    closes = [float(b.close) for b in bars]
    assert feats["ret_1d"] == pytest.approx(closes[-1] / closes[-2] - 1)
    assert feats["ret_5d"] == pytest.approx(closes[-1] / closes[-6] - 1)
    assert feats["ret_20d"] == pytest.approx(closes[-1] / closes[-21] - 1)
    assert feats["gap_open_pct"] == pytest.approx(float(bars[-1].open) / closes[-2] - 1)
    assert feats["dist_sma20"] == pytest.approx(closes[-1] / statistics.mean(closes[-20:]) - 1)
    assert feats["dist_sma200"] == pytest.approx(closes[-1] / statistics.mean(closes[-200:]) - 1)
    last = bars[-1]
    expected_drp = (float(last.close) - float(last.low)) / (float(last.high) - float(last.low))
    assert feats["day_range_pos"] == pytest.approx(expected_drp)
    assert feats["median_traded_value_20d"] == pytest.approx(statistics.median(
        float(b.close) * b.volume for b in bars[-20:]
    ))
    expected_vol = statistics.stdev(
        math.log(closes[i] / closes[i - 1]) for i in range(len(closes) - 20, len(closes))
    ) * math.sqrt(252.0)
    assert feats["realized_vol_20d"] == pytest.approx(expected_vol)
    assert feats["atr14_1d"] is not None and feats["atr14_1d"] > 0
    assert feats["gap_abs_mean_20d"] is not None and feats["gap_abs_mean_20d"] > 0


def test_daily_market_context(engine, store, seeded):
    engine.daily_snapshot(D)
    by_symbol = _rows_by_symbol(store)
    idx = [float(b.close) for b in seeded["NIFTY 50"]]
    vix = [float(b.close) for b in seeded["INDIA VIX"]]
    for feats in by_symbol.values():                              # identical on every row
        assert feats["nifty_ret_1d"] == pytest.approx(idx[-1] / idx[-2] - 1)
        assert feats["nifty_ret_20d"] == pytest.approx(idx[-1] / idx[-21] - 1)
        assert feats["nifty_trend_state"] == 1                    # rising drift: close > SMA50 > SMA200
        assert feats["vix_level"] == pytest.approx(vix[-1])
        assert feats["vix_delta_1d"] == pytest.approx(vix[-1] - vix[-2])
        assert feats["advance_decline"] == 0.0                    # AAA advanced, BBB declined
        assert feats["expiry_day"] is False                       # mid-June is never month-end expiry
    # Sector "index" return = equal-weight constituent mean (deterministic §6.2 proxy).
    a, b = by_symbol["AAA"], by_symbol["BBB"]
    assert a["sector"] == b["sector"] == "IT"
    assert a["sector_ret_1d"] == pytest.approx((a["ret_1d"] + b["ret_1d"]) / 2)
    assert a["sector_ret_1d"] == b["sector_ret_1d"]


def test_daily_advance_decline_reflects_asymmetric_breadth(engine, store):
    """§6.2 advance-decline must carry the correct SIGN and MAGNITUDE on an asymmetric day. The
    symmetric AAA/BBB fixture yields 0.0 — a value a sign-inverted ``(dec − adv)/N`` or a hardcoded
    ``0.0`` stub both reproduce. Three advancers + one decliner pins it to +0.5, so an inverted sign
    (−0.5) or a constant zero fails here."""
    days = _weekdays_back(D, 5)
    advancers = ("UP1", "UP2", "UP3")
    decliners = ("DN1",)
    for i, sym in enumerate(advancers):
        _seed_daily(store, sym, days, 100.0 + 10.0 * i, 0.05, 0.0)   # monotone rising ⇒ ret_1d > 0
    for sym in decliners:
        _seed_daily(store, sym, days, 100.0, -0.05, 0.0)             # monotone falling ⇒ ret_1d < 0
    store.upsert_universe_daily(
        [{"d": D, "symbol": s, "included": True} for s in (*advancers, *decliners)]
    )
    engine.daily_snapshot(D)

    by_symbol = _rows_by_symbol(store)
    adv, dec = len(advancers), len(decliners)
    expected = (adv - dec) / (adv + dec)
    assert expected == pytest.approx(0.5)                            # guard the fixture asymmetry
    for feats in by_symbol.values():                                 # identical on every symbol's row
        assert feats["ret_1d"] is not None                          # each symbol is directional
        assert feats["advance_decline"] == pytest.approx(expected)  # +0.5: correct sign AND magnitude
        assert feats["advance_decline"] > 0.0                       # kills the sign-inverted variant


def test_trend_state_branches_up_down_flat_and_warmup():
    """§6.2 ``nifty_trend_state`` — the market-context fixture only ever exercises the rising (+1)
    branch, so a constant-1 stub or a broken bear/flat branch would slip through. Assert all three
    branches directly plus the 200-session warm-up: +1 close > SMA50 > SMA200, −1 the mirror, 0
    otherwise, None until 200 closes exist."""
    assert _trend_state([100.0 + i for i in range(200)]) == 1        # monotone rising structure
    assert _trend_state([300.0 - i for i in range(200)]) == -1       # monotone falling structure
    assert _trend_state([100.0] * 200) == 0                          # flat: neither strict ordering
    assert _trend_state([100.0] * 199) is None                      # warm-up: needs 200 sessions


def test_prox_high_hand_computed_window_shorter_and_longer_than_data():
    """2026-09-01 origination review: 52wk/20d-high proximity. Unlike ``_dist_sma`` this degrades
    gracefully to whatever window is available rather than requiring a FULL ``n``-session lookback."""
    highs = [100.0, 150.0, 90.0, 130.0, 95.0]
    closes = [95.0, 140.0, 85.0, 125.0, 90.0]
    # n=3 (shorter than the 5-bar history): last 3 highs = [90, 130, 95] -> peak 130.
    assert _prox_high(closes, highs, 3) == pytest.approx(90.0 / 130.0)
    # n=10 (longer than the available history): degrades to the full 5-bar history -> peak 150.
    assert _prox_high(closes, highs, 10) == pytest.approx(90.0 / 150.0)


def test_prox_high_no_history_is_none():
    assert _prox_high([], [], 20) is None


def test_daily_per_symbol_context_flags(engine, store, seeded):
    engine.daily_snapshot(D)
    by_symbol = _rows_by_symbol(store)
    a, b = by_symbol["AAA"], by_symbol["BBB"]
    assert a["results_day"] is True and b["results_day"] is False
    assert a["flagged_instrument_day"] is True and b["flagged_instrument_day"] is False
    assert a["surveillance"] is None and a["surveillance_flagged"] is False
    assert b["surveillance"] == "GSM Stage 1" and b["surveillance_flagged"] is True
    assert b["days_to_ex_date"] == 3 and b["ex_date_within_5d"] is True
    assert a["days_to_ex_date"] is None and a["ex_date_within_5d"] is False


def test_absent_news_defaults_are_pinned_and_in_distribution(engine, store, seeded):
    """§6.2 pinned: sentiment* = 0, on_watchlist = false, materiality = 0, sentiment_available =
    false — present on EVERY row when no digest has ever run, never None/NaN (chaos case 20). The
    catalyst DESCRIPTIVE fields (not pinned scalars) fall back to None like any warm-up feature."""
    engine.daily_snapshot(D)
    for feats in _rows_by_symbol(store).values():
        for key, pinned in ABSENT_NEWS_DEFAULTS.items():
            assert feats[key] == pinned and feats[key] is not None
        assert feats["sentiment_available"] is False
        assert feats["on_watchlist"] is False
        assert feats["materiality"] == 0
        for scope in ("symbol", "sector", "theme", "market"):
            assert feats[f"sentiment_{scope}"] == 0
        assert feats["watchlist_grade"] is None
        assert feats["catalyst_event_type"] is None
        assert feats["catalyst_event_age_h"] is None
        assert feats["catalyst_source_domain_count"] is None


def test_daily_snapshot_deterministic_and_idempotent(engine, store, seeded):
    """Same store contents => byte-identical features_daily rows; the re-run upserts, not dupes."""
    engine.daily_snapshot(D)
    first = {r["symbol"]: r["features"] for r in store.get_features_daily(D)}
    engine.daily_snapshot(D)
    second = {r["symbol"]: r["features"] for r in store.get_features_daily(D)}
    assert first == second                                        # byte-identical JSON (§9.6)
    assert len(store.get_features_daily(D)) == 2


def test_daily_snapshot_empty_universe_writes_nothing(engine, store):
    engine.daily_snapshot(D)
    assert store.get_features_daily(D) == []


def test_daily_symbol_without_day_d_bar_gets_none_price_features(engine, store, seeded):
    """A universe symbol missing its day-d daily bar still gets a row (stable schema) with None
    price features and the pinned sentiment defaults — warm-up, not an error."""
    store.upsert_universe_daily([{"d": D, "symbol": "CCC", "included": True}])
    engine.daily_snapshot(D)
    feats = _rows_by_symbol(store)["CCC"]
    assert feats["ret_1d"] is None and feats["atr14_1d"] is None and feats["day_range_pos"] is None
    assert feats["sentiment_available"] is False and feats["sentiment_symbol"] == 0
    assert feats["nifty_ret_1d"] is not None                      # market context still present
    assert feats["prox_52wk_high"] is None and feats["prox_20d_high"] is None


# ------------------------------------------- 2026-09-01 origination review: 52wk/20d-high proximity
def test_prox_high_keys_registered_in_daily_feature_vocabulary():
    assert "prox_52wk_high" in DAILY_FEATURE_KEYS
    assert "prox_20d_high" in DAILY_FEATURE_KEYS


def test_daily_prox_high_features_respect_window_caps(engine, store):
    """Exact-value check on a constructed 260-session history: an ancient spike outside BOTH
    windows must never leak in, and the 252-session and 20-session windows must resolve against
    DIFFERENT peaks so the test cannot pass on a single shared (unwindowed) max."""
    days = _weekdays_back(D, 260)                                  # index 0 oldest .. 259 == D
    highs = [100.0] * 260
    highs[5] = 999.0     # ancient spike: outside the 252-session window (260-252=8) => excluded
    highs[50] = 150.0    # the 252-session-window peak: inside [8,259], outside the 20-session window
    highs[250] = 130.0   # the 20-session-window peak: inside [240,259] AND inside [8,259]
    bars = []
    for i, dd in enumerate(days):
        close = Decimal("90.00") if dd == D else Decimal("95.00")
        bars.append(DailyBar(
            symbol="PROXTEST", d=dd, open=Decimal("95.00"),
            high=Decimal(f"{highs[i]:.2f}"), low=Decimal("80.00"), close=close, volume=10_000,
        ))
    store.upsert_bars_1d(bars)
    store.upsert_universe_daily([{"d": D, "symbol": "PROXTEST", "included": True}])

    engine.daily_snapshot(D)
    feats = _rows_by_symbol(store)["PROXTEST"]
    assert feats["prox_52wk_high"] == pytest.approx(90.0 / 150.0)   # peak = idx50 (999 excluded)
    assert feats["prox_20d_high"] == pytest.approx(90.0 / 130.0)    # peak = idx250 (idx50 outside 20d)


def test_daily_prox_high_features_use_partial_window_when_history_is_short(engine, store):
    """Unlike ``dist_sma20`` (None below a full 20-session window), prox_*_high degrades
    gracefully: 5 sessions is far short of even the 20-session window, but the feature still
    resolves against whatever history exists rather than returning None."""
    days = _weekdays_back(D, 5)
    highs = [100.0, 100.0, 120.0, 100.0, 110.0]
    bars = []
    for i, dd in enumerate(days):
        close = Decimal("90.00") if dd == D else Decimal("95.00")
        bars.append(DailyBar(
            symbol="THIN", d=dd, open=Decimal("95.00"),
            high=Decimal(f"{highs[i]:.2f}"), low=Decimal("80.00"), close=close, volume=10_000,
        ))
    store.upsert_bars_1d(bars)
    store.upsert_universe_daily([{"d": D, "symbol": "THIN", "included": True}])

    engine.daily_snapshot(D)
    feats = _rows_by_symbol(store)["THIN"]
    assert feats["dist_sma20"] is None                                  # too little history for SMA20
    assert feats["prox_52wk_high"] == pytest.approx(90.0 / 120.0)       # degrades to all 5 sessions
    assert feats["prox_20d_high"] == pytest.approx(90.0 / 120.0)        # same 5-session window


def test_daily_sentiment_catalyst_populated_from_store(engine, store, clock, seeded):
    """§6.2 v2: real decay-weighted sentiment_agg values + today's catalyst_watchlist row land on
    the row — the pinned absent-news defaults are a per-field FALLBACK, not the only path. AAA and
    BBB (same IT sector) diverge on every sentiment/catalyst axis, proving the lookups are keyed
    correctly (not just echoing the pinned defaults)."""
    _seed_sentiment_layer(store, clock)
    engine.daily_snapshot(D)
    by_symbol = _rows_by_symbol(store)
    a, b = by_symbol["AAA"], by_symbol["BBB"]

    assert a["sentiment_available"] is True and b["sentiment_available"] is True
    assert a["sentiment_symbol"] == pytest.approx(0.42)           # ("symbol","AAA") row
    assert a["sentiment_sector"] == pytest.approx(0.15)           # ("sector","IT") row
    assert a["sentiment_theme"] == pytest.approx(-0.30)           # THEME2 wins: |-0.30| > |0.05|
    assert a["sentiment_market"] == pytest.approx(0.10)
    assert a["on_watchlist"] is True
    assert a["watchlist_grade"] == "originating"
    assert a["catalyst_event_type"] == "results"
    assert a["materiality"] == pytest.approx(0.75)                # watchlist-sourced, not sentiment_agg
    assert a["catalyst_source_domain_count"] == 3
    assert a["catalyst_event_age_h"] == pytest.approx(5.5)

    # BBB: no ("symbol","BBB") row, no watchlist row; shares the IT sector + market rows; only
    # THEME1 contains BBB (THEME2 doesn't) so its theme value can't leak in.
    assert b["sentiment_symbol"] == 0.0
    assert b["sentiment_sector"] == pytest.approx(0.15)
    assert b["sentiment_theme"] == pytest.approx(0.05)
    assert b["sentiment_market"] == pytest.approx(0.10)
    assert b["on_watchlist"] is False and b["watchlist_grade"] is None
    assert b["materiality"] == 0.0
    assert b["catalyst_event_type"] is None
    assert b["catalyst_event_age_h"] is None
    assert b["catalyst_source_domain_count"] is None


# --------------------------------------------------------------------------- expiry-day flag
def test_expiry_flag_exactly_one_day_in_month_end_week(engine):
    """Monthly expiry: exactly one flagged day in June 2026, in the final week (holiday-rolled)."""
    flagged = [date(2026, 6, x) for x in range(1, 31) if engine._is_expiry_day(date(2026, 6, x))]
    assert len(flagged) == 1
    assert date(2026, 6, 24) <= flagged[0] <= date(2026, 6, 30)
    assert engine._is_expiry_day(D) is False


def test_expiry_flag_fails_closed_without_calendar(store, clock, tmp_path):
    """R6: no calendar data => never claims an expiry day (fail closed, no assumptions)."""
    bare = FeatureEngine(store, clock, NSECalendar(tmp_path / "no_calendar", clock))
    assert not any(bare._is_expiry_day(date(2026, 6, x)) for x in range(1, 31))


# --------------------------------------------------------------------------- intraday snapshot
def _seed_intraday(store, clock, symbol: str, n: int = 50) -> list[Bar]:
    open_915 = clock.combine(D, time(9, 15))
    bars = []
    for i in range(n):
        close = Decimal(f"{100 + 0.01 * i:.2f}")
        bars.append(Bar(
            symbol=symbol, ts_minute=open_915 + timedelta(minutes=i),
            open=close - Decimal("0.01"), high=close + Decimal("0.05"),
            low=close - Decimal("0.05"), close=close, volume=1000,
        ))
    store.insert_bars_1m(bars)
    return bars


def test_intraday_snapshot_math_version_and_persistence(engine, store, clock, seeded):
    bars = _seed_intraday(store, clock, "AAA")                    # 09:15..10:04, now = 10:05
    vec = engine.intraday_snapshot("AAA")
    assert vec.feature_set_version == FEATURE_SET_VERSION == 2
    assert vec.symbol == "AAA" and vec.ts == FIXED_NOW
    f = vec.features
    assert set(f) == set(INTRADAY_FEATURE_KEYS)
    assert f["bar_count"] == 50 and f["minutes_elapsed"] == 50
    # Opening range = first 30 minutes (09:15-09:45 window, §6.1 anchor), complete by 10:05.
    assert f["or_complete"] is True
    assert f["or_high"] == pytest.approx(float(bars[29].high))
    assert f["or_low"] == pytest.approx(float(bars[0].low))
    assert f["or_range_pct"] == pytest.approx((f["or_high"] - f["or_low"]) / f["or_low"])
    assert f["last_price"] == pytest.approx(float(bars[-1].close))
    assert f["cum_volume"] == 50_000
    # Equal volumes + symmetric H/L around close => VWAP = mean(closes); price above it (uptrend).
    closes = [float(b.close) for b in bars]
    assert f["vwap"] == pytest.approx(statistics.mean(closes))
    assert f["vwap_dist"] == pytest.approx(closes[-1] / statistics.mean(closes) - 1)
    assert f["vwap_dist"] > 0
    assert f["atr14_1m"] == pytest.approx(0.10, rel=1e-3)         # constant 0.10 true range
    assert f["last30_ret"] == pytest.approx(closes[-1] / closes[-30] - 1)
    assert f["last30_up_frac"] == 1.0                             # every bar closed above its open
    assert f["last30_volume"] == 30_000
    # Relative volume vs the 20d median DAILY volume strictly before today.
    daily_vols = [b.volume for b in seeded["AAA"] if b.d < D][-20:]
    assert f["rel_volume"] == pytest.approx(50_000 / statistics.median(daily_vols))
    # Persisted under its ULID key (§4.3): round-trips exactly and is referenced later.
    loaded = load_snapshot(store, vec.features_snapshot_id)
    assert loaded is not None
    assert loaded.features == f and loaded.feature_set_version == 2 and loaded.symbol == "AAA"


def test_intraday_snapshot_deterministic_features_fresh_ids(engine, store, clock, seeded):
    _seed_intraday(store, clock, "AAA")
    v1, v2 = engine.intraday_snapshot("AAA"), engine.intraday_snapshot("AAA")
    assert v1.features == v2.features                             # same inputs => same features (§9.6)
    assert v1.features_snapshot_id != v2.features_snapshot_id     # identity is minted per snapshot


def test_intraday_snapshot_no_bars_is_warmup_not_error(engine, store):
    vec = engine.intraday_snapshot("NOBARS")
    f = vec.features
    assert set(f) == set(INTRADAY_FEATURE_KEYS)                   # full key set even when thin
    assert f["bar_count"] == 0 and f["or_high"] is None and f["rel_volume"] is None
    assert f["rel_volume_tod"] is None                            # no numerator => no pace ratio
    assert f["vwap"] is None and f["atr14_1m"] is None
    assert f["or_complete"] is True                               # 10:05 is past the 09:45 OR end
    assert load_snapshot(store, vec.features_snapshot_id) is not None


# ------------------------------------------- WO-21: time-of-day-normalized relative volume (§6.2)
#: Stamped on the 10:05 bar of every seeded historical session. 10:05 is exactly the cutoff and the
#: cutoff is EXCLUSIVE, so this volume must never reach a denominator — if it does, every ratio
#: collapses by ~6 orders of magnitude and the assertions below say so loudly.
_CUTOFF_SENTINEL_VOLUME = 10 ** 9


def _hist_sessions(daily_bars: list[DailyBar]) -> list[date]:
    """The same 20 completed sessions ``rel_volume``'s denominator is built from (strictly < D)."""
    return [b.d for b in daily_bars if b.d < D][-20:]


def _seed_hist_intraday(store, clock, symbol: str, vol_by_day: dict[date, int]) -> None:
    """51 flat 1m bars from 09:15 on each given session: 50 carrying that day's per-bar volume
    (09:15..10:04 — the window "now" = 10:05 closes) plus the sentinel bar stamped exactly 10:05.
    So each session's cumulative volume through the cutoff is exactly ``50 * vol``."""
    bars = []
    for dd, vol in vol_by_day.items():
        open_915 = clock.combine(dd, time(9, 15))
        price = Decimal("100.00")
        bars.extend(
            Bar(
                symbol=symbol, ts_minute=open_915 + timedelta(minutes=i),
                open=price, high=price, low=price, close=price,
                volume=vol if i < 50 else _CUTOFF_SENTINEL_VOLUME,
            )
            for i in range(51)
        )
    store.insert_bars_1m(bars)


def test_rel_volume_tod_is_the_median_pace_at_the_same_elapsed_minute(engine, store, clock, seeded):
    """WO-21: today's cumulative volume ÷ the MEDIAN cumulative volume of the same 20 sessions
    through the SAME 50 elapsed minutes. 1.0 = a typical pace for this time of day — the number a
    reader means by "relative volume", which ``rel_volume``'s full-day denominator never was."""
    _seed_intraday(store, clock, "AAA")                           # today: 50 bars x 1000 = 50_000
    days = _hist_sessions(seeded["AAA"])
    per_bar = {dd: 100 * (i + 1) for i, dd in enumerate(days)}     # 100..2000 per bar, per session
    _seed_hist_intraday(store, clock, "AAA", per_bar)

    f = engine.intraday_snapshot("AAA").features
    cums = [50 * v for v in per_bar.values()]                     # 5_000..100_000; sentinel excluded
    assert len(cums) == 20 and statistics.median(cums) == 52_500
    assert f["rel_volume_tod"] == pytest.approx(50_000 / 52_500)
    assert f["rel_volume_tod"] == pytest.approx(50_000 / statistics.median(cums))
    # The legacy key is untouched beside it: same numerator, 20d median FULL-DAY denominator.
    daily_vols = [b.volume for b in seeded["AAA"] if b.d < D][-20:]
    assert f["rel_volume"] == pytest.approx(50_000 / statistics.median(daily_vols))
    assert f["rel_volume"] != pytest.approx(f["rel_volume_tod"])   # different denominators, by design


def test_rel_volume_tod_needs_ten_valid_sessions(engine, store, clock, calendar, seeded):
    """Fewer than 10 usable sessions ⇒ None, never a guess: a "median pace" over a handful of
    sessions is noise. Only the intraday tick watchlist carries 1m history, so a thin symbol is
    ordinary — and ``rel_volume``, which needs only daily bars, must keep computing."""
    _seed_intraday(store, clock, "AAA")
    days = _hist_sessions(seeded["AAA"])
    _seed_hist_intraday(store, clock, "AAA", {dd: 500 for dd in days[-9:]})

    f9 = engine.intraday_snapshot("AAA").features
    assert f9["rel_volume_tod"] is None
    assert f9["rel_volume"] is not None                           # independent denominators

    _seed_hist_intraday(store, clock, "AAA", {days[-10]: 500})     # the tenth session flips it on
    # WO-27: the 20-session baseline is day-cached, so an engine that already built it does not see
    # a mid-session reseed — which is the point (those sessions are completed and cannot change in
    # production). A fresh engine is the honest stand-in for "the tenth session existed all along".
    f10 = FeatureEngine(store, clock, calendar).intraday_snapshot("AAA").features
    assert f10["rel_volume_tod"] == pytest.approx(50_000 / (50 * 500))
    assert f10["rel_volume"] == pytest.approx(f9["rel_volume"])    # nothing else moved


def test_rel_volume_tod_excludes_zero_volume_sessions(engine, store, clock, seeded):
    """A session with bars but no traded volume (halt, suspension, a gap-filled shell) is not a
    zero-pace day to average in — it is absent evidence and leaves the median entirely."""
    _seed_intraday(store, clock, "AAA")
    a_days = _hist_sessions(seeded["AAA"])
    _seed_hist_intraday(store, clock, "AAA", {
        dd: (0 if i < 6 else 200) for i, dd in enumerate(a_days)   # 6 dead, 14 alive
    })
    assert engine.intraday_snapshot("AAA").features["rel_volume_tod"] == pytest.approx(
        50_000 / (50 * 200)                                       # median of the 14 alive sessions
    )

    _seed_intraday(store, clock, "BBB")                           # 11 dead, 9 alive => under the floor
    b_days = _hist_sessions(seeded["BBB"])
    _seed_hist_intraday(store, clock, "BBB", {
        dd: (0 if i < 11 else 300) for i, dd in enumerate(b_days)
    })
    assert engine.intraday_snapshot("BBB").features["rel_volume_tod"] is None


def test_rel_volume_tod_skips_the_ranged_read_without_todays_bars(
    engine, store, clock, seeded, monkeypatch
):
    """No tape today ⇒ no numerator ⇒ None, and the ranged 1m history read never happens: the new
    feature rides the gate that already guards ``rel_volume`` (only today's own bars are read)."""
    _seed_hist_intraday(store, clock, "AAA", {dd: 400 for dd in _hist_sessions(seeded["AAA"])})
    reads: list[tuple] = []
    original = store.get_bars_1m
    monkeypatch.setattr(
        store, "get_bars_1m", lambda *a, **kw: (reads.append(a), original(*a, **kw))[1]
    )

    f = engine.intraday_snapshot("AAA").features                  # today's bars never seeded
    assert f["bar_count"] == 0
    assert f["rel_volume_tod"] is None and f["rel_volume"] is None
    assert len(reads) == 1                                        # today's bars only, no history sweep


def test_intraday_sentiment_catalyst_populated_from_store(engine, store, clock, seeded):
    """§6.2 v2: the same sentiment_agg + catalyst_watchlist block lands on the intraday
    microstructure snapshot as on the daily row (shared context helper)."""
    _seed_intraday(store, clock, "AAA")
    _seed_sentiment_layer(store, clock)
    vec = engine.intraday_snapshot("AAA")
    f = vec.features
    assert f["sentiment_available"] is True
    assert f["sentiment_symbol"] == pytest.approx(0.42)
    assert f["sentiment_sector"] == pytest.approx(0.15)
    assert f["sentiment_theme"] == pytest.approx(-0.30)
    assert f["sentiment_market"] == pytest.approx(0.10)
    assert f["on_watchlist"] is True
    assert f["watchlist_grade"] == "originating"
    assert f["catalyst_event_type"] == "results"
    assert f["materiality"] == pytest.approx(0.75)
    assert f["catalyst_source_domain_count"] == 3
    assert f["catalyst_event_age_h"] == pytest.approx(5.5)


# ---------------------------------------- WO-27: the scan-path read budget (2026-08-26 open stall)
class _StatementSpy:
    """Census of every DuckDB statement the store issues while the spy is installed.

    Wraps ``MarketStore._execute_locked`` — the single funnel ``_execute``, ``_fetchall``,
    ``_fetch_dicts`` and the simple writes all pass through — rather than individual read methods, so
    a read introduced by some future helper cannot slip past these assertions the way a per-method spy
    would let it. It was ``_execute`` until 2026-09-09, when the §2.6 slow-statement telemetry moved
    the read helpers onto the silent ``_execute_locked`` (one ``_lock`` hold must log exactly one
    note, and nesting the logging ``_execute`` inside a hold logged two).

    Census effect of that move: the READ census is unchanged — every read that reached ``_execute``
    reaches ``_execute_locked``, one level down the same funnel. The WRITE census is WIDER: the
    telemetry also routed ``_bulk_write``'s ``INSERT … SELECT`` and ``_flush_locked``'s per-partition
    ``COPY`` through ``_execute_locked``, and both used to call ``con.execute`` directly and so were
    invisible to the old ``_execute`` spy. The WO-27 assertions below are read-budget assertions, so
    they are unaffected; a future write-count assertion must expect bulk writes and tick flushes too.
    """

    def __init__(self, store, monkeypatch) -> None:      # noqa: ANN001 - test helper
        self.sql: list[str] = []
        original = store._execute_locked

        def spy(con, sql, *args, **kwargs):              # noqa: ANN001, ANN202 - passthrough recorder
            self.sql.append(" ".join(str(sql).split()))
            return original(con, sql, *args, **kwargs)

        monkeypatch.setattr(store, "_execute_locked", spy)

    @property
    def reads(self) -> list[str]:
        return [s for s in self.sql if s.upper().startswith("SELECT")]

    @property
    def writes(self) -> list[str]:
        return [s for s in self.sql if not s.upper().startswith("SELECT")]

    def refreshes(self) -> int:
        """Day-context refreshes: the watchlist read happens exactly once per combined refresh."""
        return sum(1 for s in self.reads if "catalyst_watchlist" in s)

    def clear(self) -> None:
        self.sql.clear()


class _FakeMonotonic:
    """Controllable stand-in for ``time.monotonic`` — the TTL's only clock.

    Starts far ahead of any real monotonic reading so the first call under it always sees the
    warm-up's stamp as expired and re-anchors the cache on this clock.
    """

    def __init__(self, start: float = 1e9) -> None:
        self.t = start

    def __call__(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


@pytest.fixture
def hot_path(engine, store, clock, seeded) -> list[Bar]:
    """A warmed-up engine plus today's 1m series, in the shape the SCAN path calls it: the provider
    hands over the series it already holds (``bars=``), so nothing on the path needs the store."""
    _seed_sentiment_layer(store, clock)
    _seed_hist_intraday(store, clock, "AAA", {dd: 400 for dd in _hist_sessions(seeded["AAA"])})
    bars = _seed_intraday(store, clock, "AAA")
    engine.intraday_snapshot("AAA", bars=bars)      # warm-up: day context + per-symbol baselines
    return bars


def test_steady_state_scan_snapshot_issues_zero_reads(engine, store, monkeypatch, hot_path):
    """The WO-27 result contract. ``intraday_snapshot`` runs inside ``SignalPreScreen._scan``'s lock,
    so every store read there is held across the pre-screen lock AND the store's connection lock — at
    the 2026-08-26 09:36 open ~20 executor threads were queued behind one ``get_catalyst_watchlist``
    from this path. After warm-up a snapshot must read NOTHING; the only statement left is the §4.3
    audit-chain INSERT that IS the snapshot."""
    spy = _StatementSpy(store, monkeypatch)

    vectors = [engine.intraday_snapshot("AAA", bars=hot_path) for _ in range(5)]

    assert spy.reads == []                                          # zero DuckDB reads per snapshot
    assert len(spy.writes) == 5
    assert all(w.upper().startswith("INSERT INTO FEATURE_SNAPSHOTS") for w in spy.writes)
    assert len({v.features_snapshot_id for v in vectors}) == 5       # identity still minted per call
    assert all(v.features == vectors[0].features for v in vectors)   # memory serves the same answers
    assert vectors[0].features["on_watchlist"] is True               # ...and they are the REAL values,
    assert vectors[0].features["sentiment_symbol"] == pytest.approx(0.42)
    assert vectors[0].features["rel_volume_tod"] is not None         # not absent-news/warm-up defaults


def test_batch_path_snapshot_still_reads_only_todays_tape(engine, store, monkeypatch, hot_path):
    """A caller with no series of its own (the brk20/ins/cat sweep in ``engine.ops.main``) keeps the
    one read it genuinely needs — today's tape is the snapshot's payload and can never be cached —
    and pays for nothing else. That path is per-candidate, not per-bar."""
    spy = _StatementSpy(store, monkeypatch)

    engine.intraday_snapshot("AAA")

    assert len(spy.reads) == 1
    assert "FROM bars_1m" in spy.reads[0]


def test_caller_supplied_bars_reproduce_the_store_read_exactly(engine, store, hot_path):
    """The ``bars=`` shortcut must be an optimization, never a semantic change: the provider's
    in-memory series and the store read it replaces describe the same session (``BarBuilder``
    persists before publishing ``bar.1m``), so the features are identical."""
    passed = engine.intraday_snapshot("AAA", bars=hot_path).features
    read = engine.intraday_snapshot("AAA").features
    assert passed == read


def test_day_context_refreshes_at_most_once_per_ttl(engine, store, monkeypatch, hot_path):
    """Bounded refresh: snapshot VOLUME buys no reads at all, and one elapsed TTL buys exactly one
    combined read — not one per snapshot, and not one per store table."""
    mono = _FakeMonotonic()
    monkeypatch.setattr("engine.features.engine.monotonic", mono)
    engine.intraday_snapshot("AAA", bars=hot_path)      # re-anchor the cache on the fake clock
    spy = _StatementSpy(store, monkeypatch)

    for _ in range(10):
        engine.intraday_snapshot("AAA", bars=hot_path)
    assert spy.reads == []                              # ten snapshots inside one TTL: no reads

    mono.advance(_SENTIMENT_CACHE_TTL_S + 1)
    spy.clear()
    for _ in range(10):
        engine.intraday_snapshot("AAA", bars=hot_path)

    assert spy.refreshes() == 1                         # exactly ONE refresh, then memory again
    # ...and that refresh is ONE combined read: as_of + sentiment_agg + theme_map +
    # catalyst_watchlist + sector_map (two statements — latest snapshot date, then its rows).
    assert len(spy.reads) == 6


def test_digest_rerun_reaches_the_feature_block_within_one_ttl(
    engine, store, clock, monkeypatch, hot_path
):
    """The as-of stamp is refreshed BY the TTL'd read, never probed per bar — so a mid-session digest
    re-run is invisible for at most one TTL and then lands whole. 60 s of staleness is immaterial
    here: the digest updates a few times a day, and the precision consumer of catalyst context is the
    analyst path, which reads the store directly."""
    mono = _FakeMonotonic()
    monkeypatch.setattr("engine.features.engine.monotonic", mono)
    before = engine.intraday_snapshot("AAA", bars=hot_path).features
    assert before["on_watchlist"] is True and before["sentiment_symbol"] == pytest.approx(0.42)

    store.upsert_sentiment_agg([{                        # a fresh digest run, newer as_of
        "scope": "symbol", "scope_key": "AAA",
        "as_of": clock.now() - timedelta(minutes=5), "value": -0.90,
    }])
    store.replace_catalyst_watchlist(D, [])              # ...which drops AAA off the watchlist

    assert engine.intraday_snapshot("AAA", bars=hot_path).features == before   # inside the TTL

    mono.advance(_SENTIMENT_CACHE_TTL_S + 1)
    after = engine.intraday_snapshot("AAA", bars=hot_path).features
    assert after["sentiment_symbol"] == pytest.approx(-0.90)     # the new run's rows, whole
    assert after["on_watchlist"] is False and after["watchlist_grade"] is None


def test_day_context_refreshes_on_day_change(engine, store, monkeypatch, hot_path):
    """A day-keyed cache may never outlive its day: ``catalyst_watchlist`` is day-keyed and so is the
    ``sector_map`` as-of, so a session rollover can never be served yesterday's watchlist."""
    spy = _StatementSpy(store, monkeypatch)

    engine._day_context(D)
    assert spy.refreshes() == 0                          # warm cache, same day, TTL unexpired

    engine._day_context(D - timedelta(days=1))
    assert spy.refreshes() == 1                          # a different day is never served from memory

    engine._day_context(D)
    assert spy.refreshes() == 2                          # single slot: rolling back re-reads, never lies


def test_concurrent_snapshots_collapse_into_one_day_context_read(
    store, clock, calendar, monkeypatch, seeded
):
    """Thread-safety (the scan path runs on the shared executor's threads, and the batch sweep calls
    the same engine off that path): two threads arriving across a cold cache must produce ONE store
    read, not two, and neither may see an exception."""
    _seed_sentiment_layer(store, clock)
    bars = _seed_intraday(store, clock, "AAA")
    engine = FeatureEngine(store, clock, calendar)       # cold caches: both threads race everything

    reading = threading.Event()
    reads: list[date] = []
    original = store.get_catalyst_watchlist

    def slow(d, **kwargs):                               # noqa: ANN001, ANN202 - blocking passthrough
        reads.append(d)
        reading.set()
        threading.Event().wait(0.3)                      # hold the refresh long enough to queue a peer
        return original(d, **kwargs)

    monkeypatch.setattr(store, "get_catalyst_watchlist", slow)

    results, errors = [], []

    def run() -> None:
        try:
            results.append(engine.intraday_snapshot("AAA", bars=bars))
        except Exception as exc:                         # noqa: BLE001 - "no exception" IS the assertion
            errors.append(exc)

    first, second = threading.Thread(target=run), threading.Thread(target=run)
    first.start()
    assert reading.wait(5)                               # the second thread joins mid-refresh
    second.start()
    first.join(10)
    second.join(10)

    assert errors == []
    assert len(results) == 2 and len({v.features_snapshot_id for v in results}) == 2
    assert reads == [D]                                  # ONE combined refresh served both threads
    assert all(v.features["on_watchlist"] is True for v in results)


# --------------------------------------------------------------------------- serialization contract
def test_clean_features_scalars_only_never_nan():
    cleaned = clean_features({
        "nan": float("nan"), "inf": float("inf"), "np": np.float64(1.5),
        "dec": Decimal("2338.55"), "b": True, "s": "GSM", "n": None, "i": 7,
    })
    assert cleaned["nan"] is None and cleaned["inf"] is None      # warm-up NaN/inf -> None (§6.2)
    assert cleaned["np"] == 1.5 and isinstance(cleaned["np"], float)
    assert cleaned["dec"] == "2338.55"                            # Decimals as strings (§4.3)
    assert cleaned["b"] is True and cleaned["s"] == "GSM" and cleaned["n"] is None and cleaned["i"] == 7
    with pytest.raises(TypeError):
        clean_features({"bad": [1, 2]})


def test_features_json_is_canonical():
    a = features_json({"b": 1.0, "a": float("nan")})
    b = features_json({"a": float("nan"), "b": 1.0})
    assert a == b == '{"a":null,"b":1.0}'                         # sorted, compact, NaN-free bytes
