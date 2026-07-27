"""FeatureEngine (§3.2.5/§6.2 v1): deterministic daily + intraday features on synthetic bars —
same input => byte-identical rows (§9.6), the PINNED absent-news defaults, the v1 version stamp on
every row/snapshot, market context wiring, and the canonical snapshot serialization contract."""

from __future__ import annotations

import json
import math
import statistics
from datetime import date, time, timedelta
from decimal import Decimal

import numpy as np
import pytest

from engine.core.calendar import NSECalendar
from engine.core.config import config_dir
from engine.core.types import Bar
from engine.features.engine import (
    ABSENT_NEWS_DEFAULTS,
    DAILY_FEATURE_KEYS,
    INTRADAY_FEATURE_KEYS,
    FeatureEngine,
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


# --------------------------------------------------------------------------- daily snapshot
def test_daily_snapshot_versioned_universe_scoped_full_key_set(engine, store, seeded):
    engine.daily_snapshot(D)
    rows = store.get_features_daily(D)
    assert {r["symbol"] for r in rows} == {"AAA", "BBB"}          # excluded ZZZ gets no row
    for r in rows:
        assert r["feature_set_version"] == FEATURE_SET_VERSION == 1
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
    false — present on EVERY v1 row, never None/NaN (chaos case 20)."""
    engine.daily_snapshot(D)
    for feats in _rows_by_symbol(store).values():
        for key, pinned in ABSENT_NEWS_DEFAULTS.items():
            assert feats[key] == pinned and feats[key] is not None
        assert feats["sentiment_available"] is False
        assert feats["on_watchlist"] is False
        assert feats["materiality"] == 0
        for scope in ("symbol", "sector", "theme", "market"):
            assert feats[f"sentiment_{scope}"] == 0


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
    assert vec.feature_set_version == FEATURE_SET_VERSION == 1
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
    assert loaded.features == f and loaded.feature_set_version == 1 and loaded.symbol == "AAA"


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
    assert f["vwap"] is None and f["atr14_1m"] is None
    assert f["or_complete"] is True                               # 10:05 is past the 09:45 OR end
    assert load_snapshot(store, vec.features_snapshot_id) is not None


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
