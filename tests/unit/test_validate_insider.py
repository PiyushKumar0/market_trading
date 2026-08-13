"""``scripts/validate_insider.py`` PURE daily-return-series builder (§6.4 / §2.8.4 stage 3). Synthetic
events + bars -> a hand-computed cost-adjusted daily portfolio series: PIT entry after the
event-session close, T+1..T+H returns, per-side CNC legs on the entry + exit sessions,
cross-sectional MEAN across open positions, 0 on flat days. WO-16 adds the fill convention —
``next_open`` (default, buy at open_(T+1)) vs the superseded ``close_t`` — pinned here against a
series where the two differ. The loose script is loaded by path (the repo pattern used by
``test_event_study_filings.py``); no store, no network.
"""

from __future__ import annotations

import importlib.util
import sys
from datetime import date, timedelta
from pathlib import Path

import pytest

_VI_PATH = Path(__file__).resolve().parents[2] / "scripts" / "validate_insider.py"
_spec = importlib.util.spec_from_file_location("mt_validate_insider", _VI_PATH)
vi = importlib.util.module_from_spec(_spec)
sys.modules["mt_validate_insider"] = vi
_spec.loader.exec_module(vi)

DAYS = [date(2026, 1, 5) + timedelta(days=i) for i in range(6)]   # d0..d5


def _bars(closes: list[float], opens: list[float] | None = None) -> list[tuple]:
    """``(date, open, close)`` bars; ``opens`` defaults to the prior close (a gapless series)."""
    opens = opens if opens is not None else [closes[0]] + closes[:-1]
    return [(DAYS[i], float(opens[i]), float(closes[i])) for i in range(len(closes))]


def test_build_daily_return_series_cross_sectional_mean_with_legs():
    # AAA enters at d1 (close 110); BBB enters at d2 (close 220). hold=2, per-side fee 0.4%.
    # entry_fill=close_t pins the pre-WO-16 arithmetic (every held session close-to-close).
    bars = {
        "AAA": _bars([100.0, 110.0, 121.0, 121.0, 121.0, 121.0]),
        "BBB": _bars([200.0, 200.0, 220.0, 231.0, 231.0, 231.0]),
    }
    events = [
        {"symbol": "AAA", "event_session": DAYS[1]},
        {"symbol": "BBB", "event_session": DAYS[2]},
    ]
    series = vi.build_daily_return_series(
        events, bars, DAYS[0], DAYS[5], per_side_fee_pct=0.4, hold_sessions=2,
        entry_fill=vi.ENTRY_FILL_CLOSE_T,
    )
    # AAA: d2 = 121/110-1 - 0.004 (entry leg); d3 = 121/121-1 - 0.004 (exit leg).
    # BBB: d3 = 231/220-1 - 0.004 (entry leg); d4 = 231/231-1 - 0.004 (exit leg).
    # d3 is cross-sectional: mean(AAA -0.004, BBB +0.046) = 0.021.
    assert series[DAYS[0]] == 0.0 and series[DAYS[1]] == 0.0 and series[DAYS[5]] == 0.0
    assert series[DAYS[2]] == pytest.approx(0.096)
    assert series[DAYS[3]] == pytest.approx(0.021)
    assert series[DAYS[4]] == pytest.approx(-0.004)
    # spans the full trading calendar in [start, end]
    assert set(series) == set(DAYS)


def test_single_held_session_charges_both_legs():
    # Entry at d4, hold=2 but only d5 remains before --to -> one slot carries BOTH legs (one round trip).
    bars = {"AAA": _bars([100.0 + i for i in range(6)])}        # d5 close 105, d4 close 104
    events = [{"symbol": "AAA", "event_session": DAYS[4]}]
    series = vi.build_daily_return_series(
        events, bars, DAYS[0], DAYS[5], per_side_fee_pct=0.5, hold_sessions=2,
        entry_fill=vi.ENTRY_FILL_CLOSE_T,
    )
    expected = (105.0 / 104.0 - 1.0) - 2 * 0.005                # entry+exit legs on the same session
    assert series[DAYS[5]] == pytest.approx(expected)
    assert series[DAYS[4]] == 0.0
    # Same event under the WO-16 default: the single held session is bought at ITS OWN open (104),
    # so the d4->d5 overnight gap is not booked; both legs still land on that one session.
    series_next = vi.build_daily_return_series(
        events, bars, DAYS[0], DAYS[5], per_side_fee_pct=0.5, hold_sessions=2
    )
    assert series_next[DAYS[5]] == pytest.approx((105.0 / 104.0 - 1.0) - 2 * 0.005)


def test_event_with_no_forward_bar_contributes_nothing():
    # Entry at the LAST bar -> no T+1 -> the event drops out; the series is all zeros.
    bars = {"AAA": _bars([100.0 + i for i in range(6)])}
    events = [{"symbol": "AAA", "event_session": DAYS[5]}]
    series = vi.build_daily_return_series(
        events, bars, DAYS[0], DAYS[5], per_side_fee_pct=0.4, hold_sessions=2
    )
    assert set(series.values()) == {0.0}


# ====================================================== WO-16 entry fill: next_open vs close_t
def test_next_open_is_the_default_and_excludes_the_entry_overnight_gap():
    """The whole move sits in the T->T+1 GAP: close_t books it, next_open (the default) cannot."""
    # Flat intraday sessions; a +10% gap between d1 (close 100) and d2 (open 110).
    closes = [100.0, 100.0, 110.0, 110.0, 110.0, 110.0]
    opens = [100.0, 100.0, 110.0, 110.0, 110.0, 110.0]
    bars = {"AAA": _bars(closes, opens)}
    events = [{"symbol": "AAA", "event_session": DAYS[1]}]

    default = vi.build_daily_return_series(
        events, bars, DAYS[0], DAYS[5], per_side_fee_pct=0.0, hold_sessions=1
    )
    next_open = vi.build_daily_return_series(
        events, bars, DAYS[0], DAYS[5], per_side_fee_pct=0.0, hold_sessions=1,
        entry_fill=vi.ENTRY_FILL_NEXT_OPEN,
    )
    close_t = vi.build_daily_return_series(
        events, bars, DAYS[0], DAYS[5], per_side_fee_pct=0.0, hold_sessions=1,
        entry_fill=vi.ENTRY_FILL_CLOSE_T,
    )
    assert vi.DEFAULT_ENTRY_FILL == vi.ENTRY_FILL_NEXT_OPEN
    assert default == next_open                                 # the DEFAULT is the corrected one
    assert next_open[DAYS[2]] == pytest.approx(0.0)             # bought at the 110 open, closed 110
    assert close_t[DAYS[2]] == pytest.approx(0.10)              # bought at the 100 close: +10%


def test_next_open_only_rebases_the_first_held_session():
    # d1 event; d2 opens 90 / closes 100, d3 closes 110. Only d2 is rebased onto its own open.
    bars = {"AAA": _bars([100.0, 100.0, 100.0, 110.0, 110.0, 110.0],
                         [100.0, 100.0, 90.0, 100.0, 110.0, 110.0])}
    events = [{"symbol": "AAA", "event_session": DAYS[1]}]
    series = vi.build_daily_return_series(
        events, bars, DAYS[0], DAYS[5], per_side_fee_pct=0.5, hold_sessions=2
    )
    assert series[DAYS[2]] == pytest.approx(100.0 / 90.0 - 1.0 - 0.005)    # open->close + entry leg
    assert series[DAYS[3]] == pytest.approx(110.0 / 100.0 - 1.0 - 0.005)   # close->close + exit leg


def test_invalid_entry_fill_rejected():
    bars = {"AAA": _bars([100.0 + i for i in range(6)])}
    with pytest.raises(ValueError, match="entry_fill"):
        vi.build_daily_return_series(
            [], bars, DAYS[0], DAYS[5], per_side_fee_pct=0.4, entry_fill="same_bar_please"
        )
