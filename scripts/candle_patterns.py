"""Candlestick pattern rules for the swing timing study.

Implements the study's registered candle rules (the "Swing timing study" entry in
IMPLEMENTATION_PLAN.md): 21 bullish and 17 bearish patterns on one stock's own daily bars.
The vague words (long, small, doji, short shadow, equal, near) use TA-Lib's default candle
settings, as registered. A flag at bar d reads only bars <= d.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from types import SimpleNamespace

import numpy as np
from numpy.lib.stride_tricks import sliding_window_view

BULLISH: tuple[str, ...] = (
    "bullish_engulfing",
    "hammer",
    "morning_star",
    "piercing_line",
    "bullish_harami",
    "three_white_soldiers",
    "inverted_hammer",
    "dragonfly_doji",
    "bullish_abandoned_baby",
    "three_inside_up",
    "three_outside_up",
    "bullish_kicker",
    "tweezer_bottom",
    "rising_three_methods",
    "concealing_baby_swallow",
    "bullish_separating_lines",
    "bullish_belt_hold",
    "three_line_strike_groww",
    "ladder_bottom",
    "meeting_lines",
    "bullish_three_line_strike",
)
BEARISH: tuple[str, ...] = (
    "bearish_engulfing",
    "bearish_belt_hold",
    "three_black_crows",
    "hanging_man",
    "upside_gap_two_crows",
    "evening_star",
    "shooting_star",
    "bearish_harami",
    "bearish_doji_star",
    "bearish_abandoned_baby",
    "tweezer_top",
    "bearish_kicker",
    "three_inside_down",
    "three_outside_down",
    "falling_three_methods",
    "dark_cloud_cover",
    "two_black_gapping",
)
FIVE_ENTRY: tuple[str, ...] = ("bullish_abandoned_baby", "bullish_three_line_strike")
FIVE_EXIT: tuple[str, ...] = ("three_black_crows", "evening_star", "two_black_gapping")

_REF_BARS = 10  # own bars before candle 1 that feed AB, AR, AS
_AR5_BARS = 5
_TREND_LAG = 5  # C[f-1] against C[f-6]


def _shift(x: np.ndarray, s: int, fill: float | bool = np.nan) -> np.ndarray:
    """y[d] = x[d - s]; the first s entries are `fill`."""
    out = np.full(x.shape, fill, dtype=x.dtype)
    if s < x.size:
        out[s:] = x[: x.size - s]
    return out


def _trailing_mean(x: np.ndarray, w: int) -> np.ndarray:
    """out[f] = mean of x[f-w .. f-1]; NaN for f < w."""
    out = np.full(x.shape, np.nan)
    if x.size > w:
        out[w:] = sliding_window_view(x, w).mean(axis=1)[: x.size - w]
    return out


def _context(o, h, lo, c):
    """Per-bar arrays and the reference arrays indexed by candle-1 bar f."""
    body = np.abs(c - o)
    top = np.maximum(o, c)
    bot = np.minimum(o, c)
    upper = h - top
    lower = bot - lo
    base = SimpleNamespace(o=o, h=h, l=lo, c=c, b=body, top=top, bot=bot, u=upper, d=lower)
    n = c.size
    trend_up = np.zeros(n, dtype=bool)
    trend_dn = np.zeros(n, dtype=bool)
    lag = _TREND_LAG + 1
    if n > lag:
        # index f: close of the bar before candle 1 (f-1) against the close five bars before that
        trend_up[lag:] = c[lag - 1 : n - 1] > c[: n - lag]
        trend_dn[lag:] = c[lag - 1 : n - 1] < c[: n - lag]
    refs = SimpleNamespace(
        ab=_trailing_mean(body, _REF_BARS),
        ar=_trailing_mean(h - lo, _REF_BARS),
        sh=_trailing_mean((upper + lower) / 2.0, _REF_BARS),
        ar5=_trailing_mean(h - lo, _AR5_BARS),
        up=trend_up,
        dn=trend_dn,
    )
    return base, refs


def _view(base, refs, m: int):
    """Candles 1..m aligned to the completion bar d, and the references of candle 1's bar."""
    candles: list = [None]
    for k in range(1, m + 1):
        s = m - k
        candles.append(
            SimpleNamespace(**{name: _shift(arr, s) for name, arr in vars(base).items()})
        )
    s = m - 1
    r = SimpleNamespace(
        ab=_shift(refs.ab, s),
        ar=_shift(refs.ar, s),
        sh=_shift(refs.sh, s),
        ar5=_shift(refs.ar5, s),
        up=_shift(refs.up, s, False),
        dn=_shift(refs.dn, s, False),
    )
    return candles, r


# --- words and relations (NaN compares False, so short histories never flag) ---


def _long(k, r):
    return k.b > r.ab


def _small(k, r):
    return k.b < r.ab


def _doji(k, r):
    return k.b <= 0.1 * r.ar


def _white(k):
    return k.c > k.o


def _black(k):
    return k.c < k.o


def _mid(k):
    return (k.o + k.c) / 2.0


def _long_u(k):
    return k.u > k.b


def _long_d(k):
    return k.d > k.b


def _short_shadows(k, r):
    return (k.u < r.sh) & (k.d < r.sh)


def _no_u(k, r):
    return k.u < 0.1 * r.ar


def _no_d(k, r):
    return k.d < 0.1 * r.ar


def _equal(x, y, r):
    return np.abs(x - y) <= 0.05 * r.ar5


def _near(x, y, r):
    return np.abs(x - y) <= 0.2 * r.ar5


def _engulfs(k, j):
    return (k.bot <= j.bot) & (k.top >= j.top) & (k.b > j.b)


def _inside(k, j):
    return (k.bot >= j.bot) & (k.top <= j.top) & (k.b < j.b)


def _body_gap_up(k, j):
    return k.bot > j.top


def _body_gap_dn(k, j):
    return k.top < j.bot


def _shadow_gap_up(k, j):
    return k.l > j.h


def _shadow_gap_dn(k, j):
    return k.h < j.l


# --- bullish rules: each takes (candles, refs) with candles 1-based ---


def _bullish_engulfing(k, r):
    return _black(k[1]) & _white(k[2]) & _engulfs(k[2], k[1])


def _hammer(k, r):
    return _small(k[1], r) & _long_d(k[1]) & r.dn


def _morning_star(k, r):
    return (
        _long(k[1], r) & _black(k[1]) & _small(k[2], r) & _long(k[3], r) & _white(k[3]) & r.dn
    )


def _piercing_line(k, r):
    return (
        _long(k[1], r)
        & _black(k[1])
        & _white(k[2])
        & (k[2].o < k[1].c)
        & (k[2].c > _mid(k[1]))
    )


def _bullish_harami(k, r):
    return (
        _long(k[1], r) & _black(k[1]) & _small(k[2], r) & _white(k[2]) & _inside(k[2], k[1])
    )


def _three_white_soldiers(k, r):
    ok = np.ones(k[1].c.shape, dtype=bool)
    for i in (1, 2, 3):
        ok &= _long(k[i], r) & _white(k[i]) & _short_shadows(k[i], r)
    for i in (2, 3):
        ok &= (k[i - 1].bot < k[i].o) & (k[i].o < k[i - 1].top)
    return ok & (k[1].c < k[2].c) & (k[2].c < k[3].c)


def _inverted_hammer(k, r):
    return _small(k[1], r) & _long_u(k[1]) & _no_d(k[1], r) & r.dn


def _dragonfly_doji(k, r):
    return _doji(k[1], r) & _long_d(k[1]) & r.dn


def _bullish_abandoned_baby(k, r):
    return (
        _long(k[1], r)
        & _black(k[1])
        & _doji(k[2], r)
        & _shadow_gap_dn(k[2], k[1])
        & _long(k[3], r)
        & _white(k[3])
        & _shadow_gap_up(k[3], k[2])
        & r.dn
    )


def _three_inside_up(k, r):
    return (
        _long(k[1], r)
        & _black(k[1])
        & _white(k[2])
        & _inside(k[2], k[1])
        & (k[2].c > _mid(k[1]))
        & _white(k[3])
        & (k[3].c > k[1].o)
    )


def _three_outside_up(k, r):
    return (
        _black(k[1])
        & _white(k[2])
        & _engulfs(k[2], k[1])
        & _white(k[3])
        & (k[3].c > k[2].c)
    )


def _bullish_kicker(k, r):
    return (
        _long(k[1], r)
        & _black(k[1])
        & _white(k[2])
        & (k[2].o > k[1].c)
        & (k[2].b > k[1].b)
    )


def _tweezer_bottom(k, r):
    return (
        _black(k[1])
        & _white(k[2])
        & _near(k[1].b, k[2].b, r)
        & _equal(k[1].l, k[2].l, r)
    )


def _rising_three_methods(k, r):
    ok = _long(k[1], r) & _white(k[1]) & _long(k[5], r) & _white(k[5]) & (k[5].c > k[1].h)
    for i in (2, 3, 4):
        ok &= _small(k[i], r) & _black(k[i]) & (k[i].h < k[1].h) & (k[i].l > k[1].l)
    return ok


def _concealing_baby_swallow(k, r):
    return (
        _long(k[1], r)
        & _black(k[1])
        & _long(k[2], r)
        & _black(k[2])
        & _small(k[3], r)
        & _body_gap_dn(k[3], k[2])
        & _long(k[4], r)
        & _black(k[4])
        & (k[4].o > k[3].h)
        & (k[4].c < k[3].l)
    )


def _bullish_separating_lines(k, r):
    return _black(k[1]) & _white(k[2]) & _equal(k[2].o, k[1].o, r)


def _bullish_belt_hold(k, r):
    return _long(k[1], r) & _white(k[1]) & _no_d(k[1], r) & r.dn


def _three_line_strike_groww(k, r):
    return (
        _white(k[1])
        & _white(k[2])
        & _white(k[3])
        & (k[1].c < k[2].c)
        & (k[2].c < k[3].c)
        & _long(k[4], r)
        & _black(k[4])
        & (k[4].o > k[3].c)
        & (k[4].c < k[1].o)
    )


def _ladder_bottom(k, r):
    ok = _small(k[4], r) & _long(k[5], r) & _white(k[5])
    for i in (1, 2, 3):
        ok &= _long(k[i], r) & _black(k[i])
    return ok & (k[1].c > k[2].c) & (k[2].c > k[3].c)


def _meeting_lines(k, r):
    return (
        _long(k[1], r)
        & _black(k[1])
        & _long(k[2], r)
        & _white(k[2])
        & (k[2].o < k[1].c)
        & _equal(k[2].c, k[1].c, r)
    )


def _bullish_three_line_strike(k, r):
    return (
        _black(k[1])
        & _black(k[2])
        & _black(k[3])
        & (k[1].c > k[2].c)
        & (k[2].c > k[3].c)
        & _white(k[4])
        & (k[4].o < k[3].c)
        & (k[4].c > k[1].h)
        & r.dn
    )


# --- bearish rules ---


def _bearish_engulfing(k, r):
    return _white(k[1]) & _black(k[2]) & _engulfs(k[2], k[1])


def _bearish_belt_hold(k, r):
    return _long(k[1], r) & _black(k[1]) & _no_u(k[1], r) & r.up


def _three_black_crows(k, r):
    ok = r.up & (k[1].l > k[2].l) & (k[2].l > k[3].l)
    for i in (1, 2, 3):
        ok &= _long(k[i], r) & _black(k[i]) & _no_d(k[i], r)
    return ok


def _hanging_man(k, r):
    return _small(k[1], r) & _long_d(k[1]) & r.up


def _upside_gap_two_crows(k, r):
    return (
        _long(k[1], r)
        & _white(k[1])
        & _small(k[2], r)
        & _black(k[2])
        & _body_gap_up(k[2], k[1])
        & _small(k[3], r)
        & _black(k[3])
        & (k[3].c < k[2].c)
    )


def _evening_star(k, r):
    return (
        _long(k[1], r)
        & _white(k[1])
        & _small(k[2], r)
        & _body_gap_up(k[2], k[1])
        & _long(k[3], r)
        & _black(k[3])
        & (k[3].c < _mid(k[1]))
        & r.up
    )


def _shooting_star(k, r):
    return _small(k[1], r) & _long_u(k[1]) & _no_d(k[1], r) & r.up


def _bearish_harami(k, r):
    return (
        _long(k[1], r) & _white(k[1]) & _small(k[2], r) & _black(k[2]) & _inside(k[2], k[1])
    )


def _bearish_doji_star(k, r):
    return _long(k[1], r) & _white(k[1]) & _doji(k[2], r)


def _bearish_abandoned_baby(k, r):
    return (
        _long(k[1], r)
        & _white(k[1])
        & _doji(k[2], r)
        & _shadow_gap_up(k[2], k[1])
        & _long(k[3], r)
        & _black(k[3])
        & _shadow_gap_dn(k[3], k[2])
    )


def _tweezer_top(k, r):
    return _white(k[1]) & _black(k[2]) & _equal(k[1].h, k[2].h, r) & r.up


def _bearish_kicker(k, r):
    return (
        _long(k[1], r)
        & _white(k[1])
        & _long(k[2], r)
        & _black(k[2])
        & (k[2].o < k[1].o)
    )


def _three_inside_down(k, r):
    return (
        _white(k[1])
        & _black(k[2])
        & _inside(k[2], k[1])
        & _black(k[3])
        & (k[3].c < k[2].c)
    )


def _three_outside_down(k, r):
    return (
        _white(k[1])
        & _black(k[2])
        & _engulfs(k[2], k[1])
        & _black(k[3])
        & (k[3].c < k[2].c)
    )


def _falling_three_methods(k, r):
    ok = _long(k[1], r) & _black(k[1]) & _long(k[5], r) & _black(k[5]) & (k[5].c < k[1].c)
    for i in (2, 3, 4):
        ok &= _small(k[i], r) & _white(k[i])
    return ok


def _dark_cloud_cover(k, r):
    return (
        _long(k[1], r)
        & _white(k[1])
        & _black(k[2])
        & (k[2].o > k[1].h)
        & (k[2].c < _mid(k[1]))
    )


def _two_black_gapping(k, r):
    # candle 1 is unconstrained: any colour, even a doji
    return (
        _black(k[2])
        & _shadow_gap_dn(k[2], k[1])
        & _black(k[3])
        & (k[3].l < k[2].l)
        & r.up
    )


_RULES: dict[str, tuple[int, Callable]] = {
    "bullish_engulfing": (2, _bullish_engulfing),
    "hammer": (1, _hammer),
    "morning_star": (3, _morning_star),
    "piercing_line": (2, _piercing_line),
    "bullish_harami": (2, _bullish_harami),
    "three_white_soldiers": (3, _three_white_soldiers),
    "inverted_hammer": (1, _inverted_hammer),
    "dragonfly_doji": (1, _dragonfly_doji),
    "bullish_abandoned_baby": (3, _bullish_abandoned_baby),
    "three_inside_up": (3, _three_inside_up),
    "three_outside_up": (3, _three_outside_up),
    "bullish_kicker": (2, _bullish_kicker),
    "tweezer_bottom": (2, _tweezer_bottom),
    "rising_three_methods": (5, _rising_three_methods),
    "concealing_baby_swallow": (4, _concealing_baby_swallow),
    "bullish_separating_lines": (2, _bullish_separating_lines),
    "bullish_belt_hold": (1, _bullish_belt_hold),
    "three_line_strike_groww": (4, _three_line_strike_groww),
    "ladder_bottom": (5, _ladder_bottom),
    "meeting_lines": (2, _meeting_lines),
    "bullish_three_line_strike": (4, _bullish_three_line_strike),
    "bearish_engulfing": (2, _bearish_engulfing),
    "bearish_belt_hold": (1, _bearish_belt_hold),
    "three_black_crows": (3, _three_black_crows),
    "hanging_man": (1, _hanging_man),
    "upside_gap_two_crows": (3, _upside_gap_two_crows),
    "evening_star": (3, _evening_star),
    "shooting_star": (1, _shooting_star),
    "bearish_harami": (2, _bearish_harami),
    "bearish_doji_star": (2, _bearish_doji_star),
    "bearish_abandoned_baby": (3, _bearish_abandoned_baby),
    "tweezer_top": (2, _tweezer_top),
    "bearish_kicker": (2, _bearish_kicker),
    "three_inside_down": (3, _three_inside_down),
    "three_outside_down": (3, _three_outside_down),
    "falling_three_methods": (5, _falling_three_methods),
    "dark_cloud_cover": (2, _dark_cloud_cover),
    "two_black_gapping": (3, _two_black_gapping),
}


def detect(o, h, l, c, names: Sequence[str]) -> dict[str, np.ndarray]:  # noqa: E741
    """One stock's own daily bars, ascending, as equal-length float arrays (numpy or list).

    Returns, per requested name, a bool array aligned to the bars: True at index d when that
    pattern completes on bar d (its last candle is bar d). Unknown name -> KeyError.
    """
    for name in names:
        if name not in _RULES:
            raise KeyError(name)
    o, h, lo, c = (np.asarray(x, dtype=float) for x in (o, h, l, c))
    n = c.size
    base, refs = _context(o, h, lo, c)
    views: dict[int, tuple] = {}
    out: dict[str, np.ndarray] = {}
    for name in names:
        m, rule = _RULES[name]
        if m not in views:
            views[m] = _view(base, refs, m)
        candles, r = views[m]
        flags = np.asarray(rule(candles, r), dtype=bool)
        # candle 1 is bar d-m+1 and needs _REF_BARS own bars before it
        first = _REF_BARS + m - 1
        flags[:first] = False
        out[name] = flags if flags.size == n else np.zeros(n, dtype=bool)
    return out
