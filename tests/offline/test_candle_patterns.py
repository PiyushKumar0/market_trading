"""Candle rules of the swing timing study: one positive and at least one single-fault negative per rule.

Reference bars are quiet and identical in shape (body 1, upper and lower shadow 1, range 3), so
AB = 1, AR = 3, AS = 1, AR5 = 3: long means body > 1, small < 1, doji <= 0.3, no shadow < 0.3,
equal <= 0.15, near <= 0.6. Their level drifts to give an up, down or flat trend.
"""

from __future__ import annotations

import numpy as np
import pytest
from scripts.candle_patterns import BEARISH, BULLISH, FIVE_ENTRY, FIVE_EXIT, detect

REF_BARS = 10
SLOPE = {"up": 1.0, "down": -1.0, "flat": 0.0}


def bar(o, c, u=0.5, d=0.5):
    """(O, H, L, C) from the body ends and the upper and lower shadow lengths."""
    return (o, max(o, c) + u, min(o, c) - d, c)


def W(o, b, u=0.5, d=0.5):
    return bar(o, o + b, u, d)


def K(o, b, u=0.5, d=0.5):
    return bar(o, o - b, u, d)


def prefix(trend, n=REF_BARS):
    """n reference bars of fixed shape whose level moves with the trend (close[f-1] vs close[f-6])."""
    s = SLOPE[trend]
    out = []
    for i in range(REF_BARS):
        base = 100.0 + s * i
        out.append((base, base + 2, base - 1, base + 1))
    return out[REF_BARS - n :]


def flags(name, candles, trend, n_before=REF_BARS):
    bars = prefix(trend, n_before) + list(candles)
    o, h, lo, c = (np.array([b[i] for b in bars], dtype=float) for i in range(4))
    return detect(o, h, lo, c, [name])[name]


def completes_only_at_end(name, candles, trend):
    f = flags(name, candles, trend)
    assert f[-1], f"{name} did not complete on the last bar"
    assert f.sum() == 1


# trend: the trend the rule needs ("flat" when the rule has none). Each negative is
# (what it breaks, candles) or (what it breaks, candles, trend).
SPEC = {
    "bullish_engulfing": dict(
        trend="flat",
        pos=[K(100, 2), W(97, 5)],
        neg=[
            ("candle 1 white, not black", [W(98, 2), W(97, 5)]),
            ("2 does not engulf: its bottom is above 1's", [K(100, 2), W(98.5, 5)]),
        ],
    ),
    "hammer": dict(
        trend="down",
        pos=[W(100, 0.5, 0.1, 2)],
        neg=[
            ("lower shadow 0.4 not > body 0.5", [W(100, 0.5, 0.1, 0.4)]),
            ("body 1.5 is not small", [W(100, 1.5, 0.1, 2)]),
            ("no trend", [W(100, 0.5, 0.1, 2)], "flat"),
            ("uptrend, not downtrend", [W(100, 0.5, 0.1, 2)], "up"),
        ],
    ),
    "morning_star": dict(
        trend="down",
        pos=[K(110, 4), K(105.5, 0.5), W(106, 4)],
        neg=[
            ("candle 1 not long", [K(110, 0.8), K(105.5, 0.5), W(106, 4)]),
            ("candle 2 not small", [K(110, 4), K(105.5, 1.5), W(106, 4)]),
            ("candle 3 not long", [K(110, 4), K(105.5, 0.5), W(106, 0.8)]),
            ("no trend", [K(110, 4), K(105.5, 0.5), W(106, 4)], "flat"),
        ],
    ),
    "piercing_line": dict(
        trend="flat",
        pos=[K(110, 4), W(105, 4)],
        neg=[
            ("O2 = C1, not below", [K(110, 4), W(106, 4)]),
            ("C2 = mid1, not above", [K(110, 4), W(105, 3)]),
            ("candle 1 not long", [K(110, 0.8), W(108, 2)]),
        ],
    ),
    "bullish_harami": dict(
        trend="flat",
        pos=[K(110, 4), W(107, 0.5)],
        neg=[
            ("candle 2 not small", [K(110, 4), W(106.5, 2.0)]),
            ("candle 2 pokes below 1's body", [K(110, 4), W(105.8, 0.5)]),
            ("candle 2 black", [K(110, 4), K(108, 0.5)]),
            ("candle 1 not long", [K(110, 0.8), W(109.4, 0.5)]),
        ],
    ),
    "three_white_soldiers": dict(
        trend="flat",
        pos=[W(100, 4), W(102, 4), W(104, 4)],
        neg=[
            ("candle 2 upper shadow 1.5 not short", [W(100, 4), W(102, 4, 1.5), W(104, 4)]),
            ("O3 = top of 2, not strictly inside", [W(100, 4), W(102, 4), W(106, 4)]),
            ("O2 = bottom of 1, not strictly inside", [W(100, 4), W(100, 4), W(104, 4)]),
            ("C3 = C2, not above", [W(100, 4), W(102, 4), W(102.5, 3.5)]),
        ],
    ),
    "inverted_hammer": dict(
        trend="down",
        pos=[W(100, 0.5, 2, 0.1)],
        neg=[
            ("lower shadow 0.5 is not 'no shadow'", [W(100, 0.5, 2, 0.5)]),
            ("upper shadow 0.4 not > body 0.5", [W(100, 0.5, 0.4, 0.1)]),
            ("body 1.5 is not small", [W(100, 1.5, 2, 0.1)]),
            ("no trend", [W(100, 0.5, 2, 0.1)], "flat"),
        ],
    ),
    "dragonfly_doji": dict(
        trend="down",
        pos=[W(100, 0.1, 0.1, 2)],
        neg=[
            ("body 0.5 is not a doji", [W(100, 0.5, 0.1, 2)]),
            ("lower shadow 0.05 not > body 0.1", [W(100, 0.1, 0.1, 0.05)]),
            ("no trend", [W(100, 0.1, 0.1, 2)], "flat"),
        ],
    ),
    "bullish_abandoned_baby": dict(
        trend="down",
        pos=[K(110, 4), W(104.7, 0.1, 0.1, 0.1), W(106, 4)],
        neg=[
            ("no shadow gap down: H2 >= L1", [K(110, 4), W(105.5, 0.1, 0.1, 0.1), W(106.5, 4)]),
            ("no shadow gap up: L3 <= H2", [K(110, 4), W(104.7, 0.1, 0.1, 0.1), W(105, 4)]),
            ("candle 2 body 0.5 is not a doji", [K(110, 4), W(104.5, 0.5, 0.1, 0.1), W(106, 4)]),
            ("no trend", [K(110, 4), W(104.7, 0.1, 0.1, 0.1), W(106, 4)], "flat"),
        ],
    ),
    "three_inside_up": dict(
        trend="flat",
        pos=[K(110, 4), W(106.5, 2), W(108.5, 2.5)],
        neg=[
            ("C2 = mid1, not above", [K(110, 4), W(106.5, 1.5), W(108, 3)]),
            ("2 is not inside 1: bottom below", [K(110, 4), W(105, 4), W(108.5, 2.5)]),
            ("C3 = O1, not above", [K(110, 4), W(106.5, 2), W(108.5, 1.5)]),
            ("candle 1 not long", [K(110, 0.8), W(109.3, 0.5), W(109.8, 1)]),
        ],
    ),
    "three_outside_up": dict(
        trend="flat",
        pos=[K(100, 2), W(97, 5), W(102, 2)],
        neg=[
            ("2 does not engulf", [K(100, 2), W(98.5, 5), W(103.5, 2)]),
            ("C3 = C2, not above", [K(100, 2), W(97, 5), W(101, 1)]),
            ("candle 1 white", [W(98, 2), W(97, 5), W(102, 2)]),
            ("candle 3 black", [K(100, 2), W(97, 5), K(105, 2)]),
        ],
    ),
    "bullish_kicker": dict(
        trend="flat",
        pos=[K(100, 4), W(97, 5)],
        neg=[
            ("O2 = C1, not above", [K(100, 4), W(96, 5)]),
            ("B2 3 not > B1 4", [K(100, 4), W(97, 3)]),
            ("candle 1 not long", [K(100, 0.8), W(99.5, 1)]),
            ("candle 2 black", [K(100, 4), K(97, 5)]),
        ],
    ),
    "tweezer_bottom": dict(
        trend="flat",
        pos=[K(100, 2), W(98, 2.2)],
        neg=[
            ("bodies not near: 2 vs 3.5", [K(100, 2), W(98, 3.5)]),
            ("lows not equal: 0.5 apart", [K(100, 2), W(98, 2.2, 0.5, 1.0)]),
            ("candle 1 white", [W(100, 2), W(100, 2)]),
        ],
    ),
    "rising_three_methods": dict(
        trend="flat",
        pos=[W(100, 5), K(104, 0.5), K(104, 0.5), K(104, 0.5), W(104, 2.5)],
        neg=[
            ("candle 3 high above H1", [W(100, 5), K(104, 0.5), K(105, 0.5, 1.0), K(104, 0.5), W(104, 2.5)]),
            ("candle 4 low below L1", [W(100, 5), K(104, 0.5), K(104, 0.5), K(100, 0.5, 0.5, 1.0), W(104, 2.5)]),
            ("C5 = H1, not above", [W(100, 5), K(104, 0.5), K(104, 0.5), K(104, 0.5), W(104, 1.5)]),
            ("candle 2 not small", [W(100, 5), K(104, 1.5), K(104, 0.5), K(104, 0.5), W(104, 2.5)]),
            ("candle 3 white, not black", [W(100, 5), K(104, 0.5), W(103, 0.5), K(104, 0.5), W(104, 2.5)]),
        ],
    ),
    "concealing_baby_swallow": dict(
        trend="flat",
        pos=[K(110, 4), K(105, 4), K(100.5, 0.5), K(102, 4)],
        neg=[
            ("C4 = 99.6 not below L3 99.5", [K(110, 4), K(105, 4), K(100.5, 0.5), K(102, 2.4)]),
            ("O4 = H3, not above", [K(110, 4), K(105, 4), K(100.5, 0.5), K(101, 3.5)]),
            ("no body gap down on 3", [K(110, 4), K(105, 4), K(101.5, 0.5), K(103, 5)]),
            ("candle 2 not long", [K(110, 4), K(105, 0.8), K(100.5, 0.5), K(102, 4)]),
        ],
    ),
    "bullish_separating_lines": dict(
        trend="flat",
        pos=[K(100, 2), W(100.1, 3)],
        neg=[
            ("O2 1 away from O1, not equal", [K(100, 2), W(101, 3)]),
            ("candle 2 black", [K(100, 2), K(100, 3)]),
            ("candle 1 white", [W(100, 2), W(100, 3)]),
        ],
    ),
    "bullish_belt_hold": dict(
        trend="down",
        pos=[W(100, 4, 0.5, 0.1)],
        neg=[
            ("lower shadow 0.5 is not 'none'", [W(100, 4, 0.5, 0.5)]),
            ("body 0.8 is not long", [W(100, 0.8, 0.5, 0.1)]),
            ("black, not white", [K(100, 4, 0.5, 0.1)]),
            ("no trend", [W(100, 4, 0.5, 0.1)], "flat"),
        ],
    ),
    "three_line_strike_groww": dict(
        trend="flat",
        pos=[W(100, 2), W(102, 2), W(104, 2), K(107, 8)],
        neg=[
            ("C4 = O1, not below", [W(100, 2), W(102, 2), W(104, 2), K(107, 7)]),
            ("O4 = C3, not above", [W(100, 2), W(102, 2), W(104, 2), K(106, 7)]),
            ("C2 = C1, not above", [W(100, 2), W(100.5, 1.5), W(102, 2), K(105, 6)]),
            ("candle 4 white", [W(100, 2), W(102, 2), W(104, 2), W(107, 4)]),
        ],
    ),
    "ladder_bottom": dict(
        trend="flat",
        pos=[K(110, 4), K(108, 4), K(106, 4), W(102, 0.5), W(103, 4)],
        neg=[
            ("C3 = C2, not below", [K(110, 4), K(108, 4), K(107, 3), W(102, 0.5), W(103, 4)]),
            ("candle 4 not small", [K(110, 4), K(108, 4), K(106, 4), W(102, 1.5), W(103, 4)]),
            ("candle 5 not long", [K(110, 4), K(108, 4), K(106, 4), W(102, 0.5), W(103, 0.8)]),
        ],
    ),
    "meeting_lines": dict(
        trend="flat",
        pos=[K(110, 4), W(102, 4.1)],
        neg=[
            ("C2 1 above C1, not equal", [K(110, 4), W(102, 5)]),
            ("candle 2 not long (O2 < C1 and C2 ~ C1 hold)", [K(110, 4), W(105.5, 0.6)]),
            ("candle 1 not long", [K(110, 0.8), W(105, 4.3)]),
        ],
    ),
    "bullish_three_line_strike": dict(
        trend="down",
        pos=[K(110, 2), K(108, 2), K(106, 2), W(103, 8)],
        neg=[
            ("C4 = H1, not above", [K(110, 2), K(108, 2), K(106, 2), W(103, 7.5)]),
            ("O4 = C3, not below", [K(110, 2), K(108, 2), K(106, 2), W(104, 7)]),
            ("C2 = C1, not below", [K(110, 2), K(110, 2), K(106, 2), W(103, 8)]),
            ("no trend", [K(110, 2), K(108, 2), K(106, 2), W(103, 8)], "flat"),
            ("uptrend, not downtrend", [K(110, 2), K(108, 2), K(106, 2), W(103, 8)], "up"),
        ],
    ),
    # bearish
    "bearish_engulfing": dict(
        trend="flat",
        pos=[W(100, 2), K(103, 5)],
        neg=[
            ("candle 1 black", [K(102, 2), K(103, 5)]),
            ("2 does not engulf: its top is below 1's", [W(100, 2), K(101.5, 5)]),
            ("candle 2 white", [W(100, 2), W(98, 5)]),
        ],
    ),
    "bearish_belt_hold": dict(
        trend="up",
        pos=[K(100, 4, 0.1, 0.5)],
        neg=[
            ("upper shadow 0.5 is not 'none'", [K(100, 4, 0.5, 0.5)]),
            ("body 0.8 is not long", [K(100, 0.8, 0.1, 0.5)]),
            ("white, not black", [W(100, 4, 0.1, 0.5)]),
            ("no trend", [K(100, 4, 0.1, 0.5)], "flat"),
        ],
    ),
    "three_black_crows": dict(
        trend="up",
        pos=[K(110, 4, 0.5, 0.1), K(108, 4, 0.5, 0.1), K(106, 4, 0.5, 0.1)],
        neg=[
            ("candle 2 lower shadow 0.5 is not 'none'", [K(110, 4, 0.5, 0.1), K(108, 4, 0.5, 0.5), K(106, 4, 0.5, 0.1)]),
            ("L2 = L1, not below", [K(110, 4, 0.5, 0.1), K(110, 4, 0.5, 0.1), K(108, 4, 0.5, 0.1)]),
            ("candle 3 not long", [K(110, 4, 0.5, 0.1), K(108, 4, 0.5, 0.1), K(104.5, 0.8, 0.5, 0.1)]),
            ("no trend", [K(110, 4, 0.5, 0.1), K(108, 4, 0.5, 0.1), K(106, 4, 0.5, 0.1)], "flat"),
        ],
    ),
    "hanging_man": dict(
        trend="up",
        pos=[W(100, 0.5, 0.1, 2)],
        neg=[
            ("lower shadow 0.4 not > body 0.5", [W(100, 0.5, 0.1, 0.4)]),
            ("body 1.5 is not small", [W(100, 1.5, 0.1, 2)]),
            ("no trend", [W(100, 0.5, 0.1, 2)], "flat"),
            ("downtrend, not uptrend", [W(100, 0.5, 0.1, 2)], "down"),
        ],
    ),
    "upside_gap_two_crows": dict(
        trend="flat",
        pos=[W(100, 4), K(105.5, 0.5), K(105, 0.5)],
        neg=[
            ("no body gap up: bot2 = top1", [W(100, 4), K(104.5, 0.5), K(104, 0.5)]),
            ("C3 > C2, not below", [W(100, 4), K(105.5, 0.5), K(106, 0.5)]),
            ("candle 2 not small", [W(100, 4), K(106.5, 1.5), K(105, 0.5)]),
            ("candle 2 white", [W(100, 4), W(105, 0.5), K(105, 0.5)]),
        ],
    ),
    "evening_star": dict(
        trend="up",
        pos=[W(100, 4), W(105, 0.5), K(105, 4)],
        neg=[
            ("no body gap up: bot2 = top1", [W(100, 4), W(104, 0.5), K(105, 4)]),
            ("C3 = mid1, not below", [W(100, 4), W(105, 0.5), K(105, 3)]),
            ("candle 2 not small", [W(100, 4), W(105, 1.5), K(105, 4)]),
            ("no trend", [W(100, 4), W(105, 0.5), K(105, 4)], "flat"),
        ],
    ),
    "shooting_star": dict(
        trend="up",
        pos=[W(100, 0.5, 2, 0.1)],
        neg=[
            ("lower shadow 0.5 is not 'none'", [W(100, 0.5, 2, 0.5)]),
            ("upper shadow 0.4 not > body 0.5", [W(100, 0.5, 0.4, 0.1)]),
            ("body 1.5 is not small", [W(100, 1.5, 2, 0.1)]),
            ("no trend", [W(100, 0.5, 2, 0.1)], "flat"),
        ],
    ),
    "bearish_harami": dict(
        trend="flat",
        pos=[W(100, 4), K(103, 0.5)],
        neg=[
            ("candle 2 white", [W(100, 4), W(101, 0.5)]),
            ("candle 2 pokes above 1's body", [W(100, 4), K(104.5, 0.5)]),
            ("candle 2 not small", [W(100, 4), K(103.5, 1.5)]),
            ("candle 1 not long", [W(100, 0.8), K(100.6, 0.5)]),
        ],
    ),
    "bearish_doji_star": dict(
        trend="flat",
        pos=[W(100, 4), W(105, 0.1)],
        neg=[
            ("candle 2 body 0.5 is not a doji", [W(100, 4), W(105, 0.5)]),
            ("candle 1 not long", [W(100, 0.8), W(105, 0.1)]),
            ("candle 1 black", [K(104, 4), W(105, 0.1)]),
        ],
    ),
    "bearish_abandoned_baby": dict(
        trend="flat",
        pos=[W(100, 4), W(105.5, 0.1, 0.1, 0.1), K(104.5, 4)],
        neg=[
            ("no shadow gap up: L2 < H1", [W(100, 4), W(104.2, 0.1, 0.1, 0.1), K(103.5, 4)]),
            ("no shadow gap down: H3 > L2", [W(100, 4), W(105.5, 0.1, 0.1, 0.1), K(105.2, 4)]),
            ("candle 2 body 0.5 is not a doji", [W(100, 4), W(105.5, 0.5, 0.1, 0.1), K(104.5, 4)]),
        ],
    ),
    "tweezer_top": dict(
        trend="up",
        pos=[W(100, 4), K(104, 3)],
        neg=[
            ("highs not equal: 1.0 apart", [W(100, 4), K(104, 3, 1.5)]),
            ("candle 1 black", [K(104, 4), K(104, 3)]),
            ("candle 2 white", [W(100, 4), W(101, 3)]),
            ("no trend", [W(100, 4), K(104, 3)], "flat"),
        ],
    ),
    "bearish_kicker": dict(
        trend="flat",
        pos=[W(100, 4), K(99, 4)],
        neg=[
            ("O2 = O1, not below", [W(100, 4), K(100, 4)]),
            ("candle 2 not long", [W(100, 4), K(99, 0.8)]),
            ("candle 1 not long", [W(100, 0.8), K(99, 2)]),
            ("candle 2 white", [W(100, 4), W(99, 4)]),
        ],
    ),
    "three_inside_down": dict(
        trend="flat",
        pos=[W(100, 4), K(103, 2), K(101.5, 1.5)],
        neg=[
            ("candle 2 white", [W(100, 4), W(101, 2), K(102, 3)]),
            ("2 not inside 1: top above", [W(100, 4), K(105, 4), K(101.5, 1.5)]),
            ("C3 > C2, not below", [W(100, 4), K(103, 2), K(102.5, 1)]),
            ("candle 3 white", [W(100, 4), K(103, 2), W(100, 0.5)]),
        ],
    ),
    "three_outside_down": dict(
        trend="flat",
        pos=[W(100, 2), K(103, 5), K(98, 2)],
        neg=[
            ("2 does not engulf", [W(100, 2), K(101.5, 5), K(96.5, 2)]),
            ("C3 = C2, not below", [W(100, 2), K(103, 5), K(99, 1)]),
            ("candle 3 white", [W(100, 2), K(103, 5), W(96, 1)]),
        ],
    ),
    "falling_three_methods": dict(
        trend="flat",
        pos=[K(110, 5), W(106, 0.5), W(106, 0.5), W(106, 0.5), K(107, 6)],
        neg=[
            ("candle 2 not small", [K(110, 5), W(106, 1.5), W(106, 0.5), W(106, 0.5), K(107, 6)]),
            ("candle 3 black, not white", [K(110, 5), W(106, 0.5), K(106, 0.5), W(106, 0.5), K(107, 6)]),
            ("C5 = C1, not below", [K(110, 5), W(106, 0.5), W(106, 0.5), W(106, 0.5), K(107, 2)]),
            ("candle 5 not long", [K(110, 5), W(106, 0.5), W(106, 0.5), W(106, 0.5), K(105.7, 0.8)]),
            ("candle 1 white", [W(110, 5), W(106, 0.5), W(106, 0.5), W(106, 0.5), K(107, 6)]),
        ],
    ),
    "dark_cloud_cover": dict(
        trend="flat",
        pos=[W(100, 4), K(105, 4)],
        neg=[
            ("O2 = H1, not above", [W(100, 4), K(104.5, 4)]),
            ("C2 = mid1, not below", [W(100, 4), K(105, 3)]),
            ("candle 1 not long", [W(100, 0.8), K(102, 2)]),
        ],
    ),
    "two_black_gapping": dict(
        trend="up",
        pos=[W(100, 2), K(99, 1, 0.2, 0.2), K(97.5, 1, 0.2, 0.2)],
        extra_pos=[[bar(100, 100.05), K(99, 1, 0.2, 0.2), K(97.5, 1, 0.2, 0.2)]],
        neg=[
            ("no shadow gap down: H2 >= L1", [W(100, 2), K(99.8, 1, 0.2, 0.2), K(97.5, 1, 0.2, 0.2)]),
            ("L3 > L2, not below", [W(100, 2), K(99, 1, 0.2, 0.2), K(98.5, 0.5, 0.1, 0.1)]),
            ("candle 2 white", [W(100, 2), W(98, 1, 0.2, 0.2), K(97.5, 1, 0.2, 0.2)]),
            ("no trend", [W(100, 2), K(99, 1, 0.2, 0.2), K(97.5, 1, 0.2, 0.2)], "flat"),
        ],
    ),
}


def test_spec_covers_every_rule():
    assert set(SPEC) == set(BULLISH) | set(BEARISH)


@pytest.mark.parametrize("name", sorted(SPEC))
def test_positive_completes_on_last_bar_only(name):
    spec = SPEC[name]
    for candles in [spec["pos"], *spec.get("extra_pos", [])]:
        completes_only_at_end(name, candles, spec["trend"])


@pytest.mark.parametrize("name", sorted(SPEC))
def test_negatives_each_break_one_condition(name):
    spec = SPEC[name]
    assert spec["neg"], name
    for item in spec["neg"]:
        why, candles = item[0], item[1]
        trend = item[2] if len(item) == 3 else spec["trend"]
        f = flags(name, candles, trend)
        assert not f[-1], f"{name} fired although: {why}"


@pytest.mark.parametrize("name", sorted(SPEC))
def test_trend_rules_have_a_trend_only_negative(name):
    spec = SPEC[name]
    if spec["trend"] == "flat":
        return
    assert any(len(item) == 3 for item in spec["neg"]), name


@pytest.mark.parametrize("name", sorted(SPEC))
def test_nine_reference_bars_is_not_enough(name):
    spec = SPEC[name]
    assert flags(name, spec["pos"], spec["trend"])[-1]
    f = flags(name, spec["pos"], spec["trend"], n_before=REF_BARS - 1)
    assert not f.any()


def test_trend_equal_closes_is_neither():
    # close[f-1] == close[f-6]: a flat window gives no uptrend and no downtrend
    bars = [(100.0, 102.0, 99.0, 101.0)] * REF_BARS + [W(100, 0.5, 0.1, 2)]
    o, h, lo, c = (np.array([b[i] for b in bars]) for i in range(4))
    out = detect(o, h, lo, c, ["hammer", "hanging_man"])
    assert not out["hammer"][-1] and not out["hanging_man"][-1]


def test_no_lookahead_prefix_equals_truncated_full():
    rng = np.random.default_rng(7)
    n = 300
    c = 100 + np.cumsum(rng.normal(0, 1.5, n))
    o = c + rng.normal(0, 1.2, n)
    h = np.maximum(o, c) + np.abs(rng.normal(0, 0.8, n))
    lo = np.minimum(o, c) - np.abs(rng.normal(0, 0.8, n))
    names = [*BULLISH, *BEARISH]
    full = detect(o, h, lo, c, names)
    assert sum(int(v.sum()) for v in full.values()) > 0
    for k in (12, 40, 99, 150, 217, 299):
        part = detect(o[: k + 1], h[: k + 1], lo[: k + 1], c[: k + 1], names)
        for name in names:
            assert part[name].shape == (k + 1,)
            assert np.array_equal(part[name], full[name][: k + 1]), (name, k)


def test_names_and_the_five():
    assert len(BULLISH) == 21 and len(set(BULLISH)) == 21
    assert len(BEARISH) == 17 and len(set(BEARISH)) == 17
    assert not set(BULLISH) & set(BEARISH)
    assert BULLISH[0] == "bullish_engulfing" and BULLISH[-1] == "bullish_three_line_strike"
    assert BEARISH[0] == "bearish_engulfing" and BEARISH[-1] == "two_black_gapping"
    assert set(FIVE_ENTRY) <= set(BULLISH)
    assert set(FIVE_EXIT) <= set(BEARISH)


def test_unknown_name_raises_keyerror():
    with pytest.raises(KeyError):
        detect([1.0], [1.0], [1.0], [1.0], ["no_such_pattern"])


def test_accepts_lists_and_short_series():
    out = detect([], [], [], [], ["hammer"])
    assert out["hammer"].shape == (0,)
    out = detect([1.0] * 3, [2.0] * 3, [0.5] * 3, [1.5] * 3, list(BULLISH))
    assert all(v.shape == (3,) and not v.any() for v in out.values())
