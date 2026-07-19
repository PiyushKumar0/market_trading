"""``scripts/validate_insider.py`` PURE daily-return-series builder (§6.4 / §2.8.4 stage 3). Synthetic
events + bars -> a hand-computed cost-adjusted daily portfolio series: PIT entry at the event-session
close, T+1..T+H close-to-close returns, per-side CNC legs on the entry + exit sessions, cross-sectional
MEAN across open positions, 0 on flat days. The loose script is loaded by path (the repo pattern used
by ``test_event_study_filings.py``); no store, no network.
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


def test_build_daily_return_series_cross_sectional_mean_with_legs():
    # AAA enters at d1 (close 110); BBB enters at d2 (close 220). hold=2, per-side fee 0.4%.
    bars = {
        "AAA": [(DAYS[0], 100.0), (DAYS[1], 110.0), (DAYS[2], 121.0),
                (DAYS[3], 121.0), (DAYS[4], 121.0), (DAYS[5], 121.0)],
        "BBB": [(DAYS[0], 200.0), (DAYS[1], 200.0), (DAYS[2], 220.0),
                (DAYS[3], 231.0), (DAYS[4], 231.0), (DAYS[5], 231.0)],
    }
    events = [
        {"symbol": "AAA", "event_session": DAYS[1]},
        {"symbol": "BBB", "event_session": DAYS[2]},
    ]
    series = vi.build_daily_return_series(
        events, bars, DAYS[0], DAYS[5], per_side_fee_pct=0.4, hold_sessions=2
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
    bars = {"AAA": [(DAYS[i], 100.0 + i) for i in range(6)]}   # d5 close 105, d4 close 104
    events = [{"symbol": "AAA", "event_session": DAYS[4]}]
    series = vi.build_daily_return_series(
        events, bars, DAYS[0], DAYS[5], per_side_fee_pct=0.5, hold_sessions=2
    )
    expected = (105.0 / 104.0 - 1.0) - 2 * 0.005                # entry+exit legs on the same session
    assert series[DAYS[5]] == pytest.approx(expected)
    assert series[DAYS[4]] == 0.0


def test_event_with_no_forward_bar_contributes_nothing():
    # Entry at the LAST bar -> no T+1 -> the event drops out; the series is all zeros.
    bars = {"AAA": [(DAYS[i], 100.0 + i) for i in range(6)]}
    events = [{"symbol": "AAA", "event_session": DAYS[5]}]
    series = vi.build_daily_return_series(
        events, bars, DAYS[0], DAYS[5], per_side_fee_pct=0.4, hold_sessions=2
    )
    assert set(series.values()) == {0.0}
