"""Hand-computed expected signals for the four §6.1 price-baseline scanners.

Every expected level below is derived BY HAND from the pinned rule sketches (§6.1), the §6.3
envelope defaults, and the smoothing conventions pinned in ``engine.strategy.indicators`` (Wilder
SMA-seeded recursions; EMA seeded at the first value). Synthetic bar sequences are built so the
intermediate indicator values are exact (uniform true ranges ⇒ ATR is a closed-form number).
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from decimal import Decimal

import pytest

from engine.core.clock import IST
from engine.core.types import Bar
from engine.marketdata.store import DailyBar
from engine.strategy.scanners import MomentumScanner, OrbScanner, Rsi2Scanner, TrendScanner
from engine.strategy.types import ScanContext, SignalCandidate

DAY = date(2026, 6, 17)  # a real 2026 trading day (see tests/conftest.py)


def _dt(hh: int, mm: int) -> datetime:
    return datetime(2026, 6, 17, hh, mm, tzinfo=IST)


def _window(start: tuple[int, int], end: tuple[int, int]) -> tuple[datetime, datetime]:
    return (
        datetime.combine(DAY, time(*start), tzinfo=IST),
        datetime.combine(DAY, time(*end), tzinfo=IST),
    )


# ============================================================================ orb (§6.1 row 1)
#
# Session built for orb_minutes=15 (inside the §6.3 range 15–45, small enough to hand-verify):
#   * range bars 09:15–09:29 — uniform O=100 H=100.50 L=99.50 C=100 V=1000; the 09:15 seed bar
#     carries auction_open=102 (A14) ⇒ effective range = [99.50, 102.00]. §6.1 v2 (2026-07-12)
#     anchors the stop/target at the OPPOSITE range edge, so only these two edges — not any
#     TR/ATR path — drive the levels; the bar shape need only pin the range and the volume median.
#   * filler bars 09:30..trigger−1 — same uniform shape.
#   * trigger bar at ``trigger`` — the scanned bar.
# Volume median over the 20 bars before the trigger = 1000 exactly.


def _uniform_bar(ts: datetime, *, volume: int = 1000, auction_open: Decimal | None = None) -> Bar:
    return Bar(
        symbol="TCS", ts_minute=ts, open=Decimal("100"), high=Decimal("100.50"),
        low=Decimal("99.50"), close=Decimal("100"), volume=volume, auction_open=auction_open,
    )


def _orb_session(
    trigger: Bar,
    *,
    auction_open: Decimal | None = Decimal("102"),
    prior_volume: int = 1000,
) -> list[Bar]:
    open_dt = _dt(9, 15)
    bars: list[Bar] = []
    ts = open_dt
    while ts < trigger.ts_minute:
        bars.append(_uniform_bar(ts, volume=prior_volume, auction_open=auction_open if ts == open_dt else None))
        ts += timedelta(minutes=1)
    bars.append(trigger)
    return bars


def _orb_ctx(
    bars: list[Bar],
    *,
    window: tuple[datetime, datetime] | None = None,
    flagged: bool = False,
    session_open: datetime | None = None,
    features_snapshot_id: str | None = None,
) -> ScanContext:
    return ScanContext(
        intraday_bars=bars,
        session_open=_dt(9, 15) if session_open is None else session_open,
        trade_window=_window((9, 30), (14, 30)) if window is None else window,
        flagged=flagged,
        features_snapshot_id=features_snapshot_id,
    )


def _orb() -> OrbScanner:
    return OrbScanner({"orb_minutes": 15})


def _buy_trigger(ts: datetime, *, close: str = "102.05", high: str = "102.10",
                 low: str = "101.10", volume: int = 1500) -> Bar:
    return Bar(symbol="TCS", ts_minute=ts, open=Decimal("101.50"), high=Decimal(high),
               low=Decimal(low), close=Decimal(close), volume=volume)


def test_signal_candidate_pinned_field_set():
    # §3.2.5 PINNED — a drift here is a plan change, not a refactor.
    assert set(SignalCandidate.model_fields) == {
        "signal_id", "strategy_id", "symbol", "side", "style", "raw_levels", "score",
        "features_snapshot_id", "catalyst_ref",
    }


def test_orb_buy_breakout_hand_computed_levels():
    # Trigger 09:35, close 102.05 > 102.00 (auction-seeded range high). vol 1500 = 1.5 × median(1000).
    # §6.1 v2 (2026-07-12): risk anchored at the OPPOSITE range edge — the range low 99.50:
    #   risk   = stop_range_frac(1.0) × (102.05 − 99.50) = 2.55
    #   stop   = 102.05 − 2.55 = 99.50            → round_to_tick 99.50
    #   target = 102.05 + 1.5 × 2.55 = 105.875    → round_to_tick 105.90
    #            (105.875 / 0.05 = 2117.5 steps, ROUND_HALF_UP → 2118 → 2118 × 0.05 = 105.90)
    # score = 1.5 / (2 × 1.5) = 0.5.
    trigger = _buy_trigger(_dt(9, 35))
    ctx = _orb_ctx(_orb_session(trigger), features_snapshot_id="fs-001")
    out = _orb().scan(trigger, ctx)
    assert len(out) == 1
    c = out[0]
    assert c.strategy_id == "orb"
    assert c.symbol == "TCS"
    assert c.side == "BUY"
    assert c.style == "intraday"
    assert c.raw_levels.entry == Decimal("102.05")
    assert c.raw_levels.stop == Decimal("99.50")
    assert c.raw_levels.target == Decimal("105.90")
    assert c.score == pytest.approx(0.5)
    assert c.features_snapshot_id == "fs-001"
    assert c.catalyst_ref is None            # price baseline — never a catalyst link (§3.2.5)
    assert len(c.signal_id) == 26            # ULID (§3.2 convention 6)


def test_orb_sell_breakout_hand_computed_levels():
    # close 99.40 < 99.50 range low. §6.1 v2: risk anchored at the OPPOSITE (upper) range edge 102.00:
    #   risk   = stop_range_frac(1.0) × (102.00 − 99.40) = 2.60
    #   stop   = 99.40 + 2.60 = 102.00            → round_to_tick 102.00
    #   target = 99.40 − 1.5 × 2.60 = 95.50       → round_to_tick 95.50
    trigger = Bar(symbol="TCS", ts_minute=_dt(9, 35), open=Decimal("100"), high=Decimal("100.40"),
                  low=Decimal("99.40"), close=Decimal("99.40"), volume=1500)
    out = _orb().scan(trigger, _orb_ctx(_orb_session(trigger)))
    assert len(out) == 1
    c = out[0]
    assert c.side == "SELL"
    assert c.raw_levels.entry == Decimal("99.40")
    assert c.raw_levels.stop == Decimal("102.00")
    assert c.raw_levels.target == Decimal("95.50")


def test_orb_stop_range_frac_scales_risk():
    # stop_range_frac=0.5 halves the range-anchored risk on the same BUY breakout (close 102.05,
    # range low 99.50): risk = 0.5 × (102.05 − 99.50) = 1.275.
    #   stop   = 102.05 − 1.275 = 100.775         → round_to_tick 100.80
    #            (100.775 / 0.05 = 2015.5 steps, ROUND_HALF_UP → 2016 → 100.80)
    #   target = 102.05 + 1.5 × 1.275 = 103.9625  → round_to_tick 103.95
    #            (103.9625 / 0.05 = 2079.25 steps, ROUND_HALF_UP → 2079 → 103.95)
    trigger = _buy_trigger(_dt(9, 35))
    scanner = OrbScanner({"orb_minutes": 15, "stop_range_frac": 0.5})
    out = scanner.scan(trigger, _orb_ctx(_orb_session(trigger)))
    assert len(out) == 1
    c = out[0]
    assert c.side == "BUY"
    assert c.raw_levels.entry == Decimal("102.05")
    assert c.raw_levels.stop == Decimal("100.80")
    assert c.raw_levels.target == Decimal("103.95")


def test_orb_close_exactly_at_range_edge_does_not_trigger():
    # "beyond range" is STRICT: close == range_high (102.00) is not a breakout.
    trigger = _buy_trigger(_dt(9, 35), close="102.00")
    assert _orb().scan(trigger, _orb_ctx(_orb_session(trigger))) == []


def test_orb_auction_open_seeds_the_range():
    # close 101.55 clears the tick-derived high (100.50) but NOT the auction-seeded 102.00 (A14).
    trigger = _buy_trigger(_dt(9, 35), close="101.55", high="101.60", low="100.60")
    assert _orb().scan(trigger, _orb_ctx(_orb_session(trigger))) == []
    # Without the auction print the same close IS a breakout — proves the seed changed the range.
    bars = _orb_session(trigger, auction_open=None)
    out = _orb().scan(trigger, _orb_ctx(bars))
    assert len(out) == 1 and out[0].side == "BUY"


def test_orb_volume_filter_boundary():
    # Median of the 20 prior bars = 1000; vol_mult = 1.5 ⇒ threshold 1500 (≥, so exactly-at passes).
    at = _buy_trigger(_dt(9, 35), volume=1500)
    assert len(_orb().scan(at, _orb_ctx(_orb_session(at)))) == 1
    below = _buy_trigger(_dt(9, 35), volume=1499)
    assert _orb().scan(below, _orb_ctx(_orb_session(below))) == []


def test_orb_zero_volume_baseline_fails_to_zero():
    trigger = _buy_trigger(_dt(9, 35))
    bars = _orb_session(trigger, prior_volume=0)  # median 0 — no trustworthy baseline
    assert _orb().scan(trigger, _orb_ctx(bars)) == []


def test_orb_flagged_day_suppressed():
    # Bulk/block-deal day (§4.4 job 9): the volume breakout is untrustworthy — suppressed (§6.1).
    trigger = _buy_trigger(_dt(9, 35))
    assert _orb().scan(trigger, _orb_ctx(_orb_session(trigger), flagged=True)) == []


def test_orb_window_intersection_lower_edge():
    # Entry instant = trigger bar CLOSE (09:36). Owner window starting exactly 09:36 admits it
    # (inclusive); 09:37 rejects it — the 09:30 base start is intersected with the owner window.
    trigger = _buy_trigger(_dt(9, 35))
    bars = _orb_session(trigger)
    assert len(_orb().scan(trigger, _orb_ctx(bars, window=_window((9, 36), (14, 30))))) == 1
    assert _orb().scan(trigger, _orb_ctx(bars, window=_window((9, 37), (14, 30)))) == []


def test_orb_window_intersection_upper_edge_base_1430_binds():
    # Owner window extends to 15:00 but the §6.1 base window ends 14:30 — most restrictive wins.
    # Trigger bar 14:29 closes AT 14:30 (inclusive edge) ⇒ admitted; 14:30 closes 14:31 ⇒ rejected.
    at_edge = _buy_trigger(_dt(14, 29))
    bars = _orb_session(at_edge)
    assert len(_orb().scan(at_edge, _orb_ctx(bars, window=_window((9, 30), (15, 0))))) == 1
    past = _buy_trigger(_dt(14, 30))
    assert _orb().scan(past, _orb_ctx(_orb_session(past), window=_window((9, 30), (15, 0)))) == []


def test_orb_no_signal_while_range_is_forming():
    # 09:29 is still inside the 15-minute opening range — no breakout can exist yet.
    trigger = _buy_trigger(_dt(9, 29))
    assert _orb().scan(trigger, _orb_ctx(_orb_session(trigger))) == []


def test_orb_missing_context_fails_to_zero():
    trigger = _buy_trigger(_dt(9, 35))
    bars = _orb_session(trigger)
    assert _orb().scan(trigger, ScanContext(intraday_bars=bars, trade_window=_window((9, 30), (14, 30)))) == []
    assert _orb().scan(trigger, ScanContext(intraday_bars=bars, session_open=_dt(9, 15))) == []
    assert _orb().scan(trigger, _orb_ctx([])) == []


# ============================================================================ rsi2 (§6.1 row 2)
#
# Stock series: 198 closes rising +0.50 (100 → 198.50), a −2.00 daily close (196.50), then the
# provisional bar close 194.50 (another −2.00). Wilder RSI(2) by hand: the constant-gain block pins
# (avg_gain, avg_loss) = (0.5, 0); the two −2 deltas give ag=0.25, al=1.0 then ag=0.125, al=1.5
# ⇒ RSI = 100×0.125/1.625 = 7.6923… < 10. 200-DMA ≈ 149.71 — the 194.50 close is far above it.


def _dailies(closes: list[float], symbol: str = "TCS") -> list[DailyBar]:
    d0 = date(2025, 9, 1)
    return [
        DailyBar(symbol=symbol, d=d0 + timedelta(days=i), open=Decimal(str(c)), high=Decimal(str(c)),
                 low=Decimal(str(c)), close=Decimal(str(c)), volume=1000)
        for i, c in enumerate(closes)
    ]


def _swing_bar(close: str, symbol: str = "TCS") -> Bar:
    c = Decimal(close)
    return Bar(symbol=symbol, ts_minute=_dt(10, 0), open=c, high=c, low=c, close=c, volume=1000)


UPTREND_INDEX = [Decimal(str(100 + 0.5 * i)) for i in range(80)]          # close > 50-DMA, rising
FLAT_INDEX = [Decimal("150")] * 80                                        # close == 50-DMA — no uptrend
# Close (160) > its 50-DMA (150.2) but the 50-DMA is FALLING vs 20 sessions ago (202.2) — the
# "rising" leg of the task-pinned uptrend definition must reject this.
FALLING_DMA_INDEX = [Decimal(str(300 - i)) for i in range(30)] + [Decimal("150")] * 49 + [Decimal("160")]

RSI2_RISING = [100 + 0.5 * i for i in range(198)]                         # 100 … 198.5


def test_rsi2_hand_computed_signal():
    ctx = ScanContext(daily_bars=_dailies([*RSI2_RISING, 196.5]), index_daily_closes=UPTREND_INDEX)
    out = Rsi2Scanner().scan(_swing_bar("194.50"), ctx)
    assert len(out) == 1
    c = out[0]
    assert c.strategy_id == "rsi2"
    assert c.side == "BUY"                       # long-only mean reversion (C5)
    assert c.style == "swing"
    assert c.raw_levels.entry == Decimal("194.50")
    # stop = 194.50 × (1 − 4%) = 186.72 → tick 186.70 (3734.4 steps rounds half-up to 3734).
    assert c.raw_levels.stop == Decimal("186.70")
    assert c.raw_levels.target is None           # exit is RSI>rsi_exit / max_hold_days — informational
    assert c.score == pytest.approx((10 - 7.6923076923076925) / 10)
    assert c.catalyst_ref is None


def test_rsi2_boundary_rsi_exactly_at_entry_threshold_rejected():
    # Two −1.5 deltas ⇒ ag=0.125, al=1.125 ⇒ RSI = 12.5/1.25 = 10.0 exactly — NOT < rsi_entry (10).
    ctx = ScanContext(daily_bars=_dailies([*RSI2_RISING, 197.0]), index_daily_closes=UPTREND_INDEX)
    assert Rsi2Scanner().scan(_swing_bar("195.50"), ctx) == []


def test_rsi2_below_200dma_rejected():
    # Deeply oversold (RSI ≈ 0) but UNDER the 200-DMA — the regime filter rejects before RSI.
    ctx = ScanContext(daily_bars=_dailies([100.0] * 199), index_daily_closes=UPTREND_INDEX)
    assert Rsi2Scanner().scan(_swing_bar("99.00"), ctx) == []


def test_rsi2_index_not_uptrending_rejected():
    good_stock = _dailies([*RSI2_RISING, 196.5])
    bar = _swing_bar("194.50")
    assert Rsi2Scanner().scan(bar, ScanContext(daily_bars=good_stock, index_daily_closes=FLAT_INDEX)) == []
    assert Rsi2Scanner().scan(bar, ScanContext(daily_bars=good_stock, index_daily_closes=FALLING_DMA_INDEX)) == []


def test_rsi2_insufficient_history_fails_to_zero():
    bar = _swing_bar("194.50")
    # 150 stock dailies: no 200-DMA yet (warm-up, §7.1 warmup_ready).
    ctx = ScanContext(daily_bars=_dailies([100.0 + 0.5 * i for i in range(150)]),
                      index_daily_closes=UPTREND_INDEX)
    assert Rsi2Scanner().scan(bar, ctx) == []
    # 69 index closes: the 50-DMA + 20-session rising check needs 70.
    ctx = ScanContext(daily_bars=_dailies([*RSI2_RISING, 196.5]), index_daily_closes=UPTREND_INDEX[:69])
    assert Rsi2Scanner().scan(bar, ctx) == []


# ============================================================================ trend (§6.1 row 3)
#
# 60 dailies with CONSTANT close 100 pin EMA20 == EMA50 == 100 (seeded-at-first-value EMAs of a
# constant series), so an up provisional close makes the golden cross fire TODAY by construction.
# Highs/lows creep +0.01/day (+DM=0.01, −DM=0, TR=1.0 uniform) ⇒ +DI=1, −DI=0 ⇒ DX=100 ⇒ ADX=100
# exactly, and ATR(14,1d)=1.0 exactly — every level is closed-form.


def _trend_dailies(n: int = 60, *, directional: bool = True) -> list[DailyBar]:
    d0 = date(2026, 3, 1)
    out = []
    for i in range(n):
        if directional:
            hi = Decimal("100") + Decimal("0.01") * i
            lo = Decimal("99") + Decimal("0.01") * i
        else:
            hi, lo = Decimal("101"), Decimal("99")   # static H/L: ±DM = 0 ⇒ ADX = 0
        out.append(DailyBar(symbol="TCS", d=d0 + timedelta(days=i), open=Decimal("100"),
                            high=hi, low=lo, close=Decimal("100"), volume=1000))
    return out


def test_trend_golden_cross_hand_computed():
    out = TrendScanner().scan(_swing_bar("101"), ScanContext(daily_bars=_trend_dailies()))
    assert len(out) == 1
    c = out[0]
    assert c.strategy_id == "trend"
    assert c.side == "BUY"                        # golden cross only — no CNC shorts (C5)
    assert c.style == "position"
    assert c.raw_levels.entry == Decimal("101")
    assert c.raw_levels.stop == Decimal("98.50")  # 101 − 2.5 × ATR(1.0) — the initial trail level
    assert c.raw_levels.target is None            # trail-managed (§6.1)
    assert c.score == pytest.approx(1.0)          # ADX 100 ≥ the 50 score ceiling


def test_trend_adx_filter_blocks_cross_without_trend_strength():
    # Same closes (cross fires) but non-directional H/L ⇒ ADX = 0 — rejected by ADX > adx_min.
    ctx = ScanContext(daily_bars=_trend_dailies(directional=False))
    assert TrendScanner().scan(_swing_bar("101"), ctx) == []


def test_trend_adx_boundary_is_strict():
    # The synthetic ADX is exactly 100.0: adx_min=100 must reject (strict >), 99.5 must admit.
    ctx = ScanContext(daily_bars=_trend_dailies())
    assert TrendScanner({"adx_min": 100.0}).scan(_swing_bar("101"), ctx) == []
    out = TrendScanner({"adx_min": 99.5}).scan(_swing_bar("101"), ctx)
    assert len(out) == 1
    assert out[0].score == pytest.approx(0.5)     # (100 − 99.5) / max(50 − 99.5, 1.0)


def test_trend_no_fresh_cross_rejected():
    # Steadily rising closes: EMA20 already above EMA50 yesterday — no NEW cross to trade.
    d0 = date(2026, 3, 1)
    dailies = [
        DailyBar(symbol="TCS", d=d0 + timedelta(days=i), open=Decimal(str(100 + 0.5 * i)),
                 high=Decimal(str(101 + 0.5 * i)), low=Decimal(str(99 + 0.5 * i)),
                 close=Decimal(str(100 + 0.5 * i)), volume=1000)
        for i in range(60)
    ]
    assert TrendScanner().scan(_swing_bar("135"), ScanContext(daily_bars=dailies)) == []


def test_trend_death_cross_not_emitted():
    # Down provisional close ⇒ EMA20 < EMA50 — the bearish cross is a CNC short: never emitted.
    assert TrendScanner().scan(_swing_bar("99"), ScanContext(daily_bars=_trend_dailies())) == []


def test_trend_insufficient_dailies_fails_to_zero():
    assert TrendScanner().scan(_swing_bar("101"), ScanContext(daily_bars=_trend_dailies(59))) == []


# ============================================================================ mom (§6.1 row 4)

MOMENTUM = {"AAA": 0.30, "BBB": 0.20, "CCC": 0.10, "DDD": float("nan")}


def _mom_ctx(*, since: int | None = None, ex_dates: list[date] | None = None) -> ScanContext:
    return ScanContext(momentum_by_symbol=MOMENTUM, mom_sessions_since_rebalance=since,
                       upcoming_ex_dates=ex_dates or [])


def test_mom_rank_within_top_n():
    # top_n=2 (§6.3 default): AAA rank 1 (score 1.0), BBB rank 2 (score 0.5), CCC rank 3 — out.
    scanner = MomentumScanner()
    out = scanner.scan(_swing_bar("100", symbol="AAA"), _mom_ctx())
    assert len(out) == 1
    c = out[0]
    assert (c.strategy_id, c.side, c.style) == ("mom", "BUY", "swing")
    assert c.raw_levels.entry == Decimal("100")
    assert c.raw_levels.stop is None and c.raw_levels.target is None   # rebalance-driven book
    assert c.score == pytest.approx(1.0)
    out_b = scanner.scan(_swing_bar("100", symbol="BBB"), _mom_ctx())
    assert len(out_b) == 1 and out_b[0].score == pytest.approx(0.5)
    assert scanner.scan(_swing_bar("100", symbol="CCC"), _mom_ctx()) == []


def test_mom_nan_or_unknown_momentum_unrankable():
    scanner = MomentumScanner()
    assert scanner.scan(_swing_bar("100", symbol="DDD"), _mom_ctx()) == []   # NaN — no rank
    assert scanner.scan(_swing_bar("100", symbol="ZZZ"), _mom_ctx()) == []   # not in the universe map


def test_mom_deterministic_alphabetical_tie_break():
    ctx = ScanContext(momentum_by_symbol={"BBB": 0.2, "AAA": 0.2})
    scanner = MomentumScanner({"top_n": 1})
    assert len(scanner.scan(_swing_bar("100", symbol="AAA"), ctx)) == 1     # tie → alphabetical
    assert scanner.scan(_swing_bar("100", symbol="BBB"), ctx) == []


def test_mom_rebalance_gating_in_trading_sessions():
    scanner = MomentumScanner()   # rebalance_days = 15
    bar = _swing_bar("100", symbol="AAA")
    assert scanner.scan(bar, _mom_ctx(since=14)) == []                      # not due yet
    assert len(scanner.scan(bar, _mom_ctx(since=15))) == 1                  # due exactly at horizon
    assert len(scanner.scan(bar, _mom_ctx(since=None))) == 1                # never rebalanced ⇒ due


def test_mom_ex_date_skip_horizon():
    # A12: horizon = ceil(15 × 7/5) = 21 calendar days from the bar's day, inclusive.
    scanner = MomentumScanner()
    bar = _swing_bar("100", symbol="AAA")                                   # bar day = 2026-06-17
    assert scanner.scan(bar, _mom_ctx(ex_dates=[date(2026, 7, 8)])) == []   # day+21 — inside, skip
    assert len(scanner.scan(bar, _mom_ctx(ex_dates=[date(2026, 7, 9)]))) == 1   # day+22 — outside
    assert len(scanner.scan(bar, _mom_ctx(ex_dates=[date(2026, 6, 16)]))) == 1  # past ex-date ignored
