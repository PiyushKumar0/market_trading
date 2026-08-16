"""``ins`` scanner (§6.1 addendum, 2026-08-17) — crossing row -> SignalCandidate translation.

PINNED worked example: a ₹1cr-threshold crossing worth ₹3cr on a symbol whose crossing-session close
was 100.00, at the shipped 6% stop. All arithmetic is hand-computed and params are passed explicitly,
so an owner settings change can never silently break these tests.
"""

from __future__ import annotations

import math
from datetime import date
from decimal import Decimal

from engine.strategy.scanners import ins
from engine.strategy.scanners.ins import Crossing, scan_crossing, sweep_crossings

SESSION = date(2026, 8, 17)
P = {"stop_pct": 6.0, "hold_sessions": 20, "threshold_inr": 10_000_000.0}


def _crossing(
    symbol: str = "AAA",
    *,
    close: str = "100.00",
    value: int = 30_000_000,
    filings_n: int = 2,
) -> Crossing:
    return Crossing(
        symbol=symbol,
        crossing_session=SESSION,
        trailing_value=Decimal(value),
        contributing_filings_n=filings_n,
        reference_close=Decimal(close),
    )


# =========================================================================== pinned level math
def test_pinned_level_math_entry_stop_and_no_target():
    """entry = the pre-open reference (the crossing session's close); stop = entry x (1 - 6/100);
    target = None — the validated design has no price target and one is never invented."""
    cand = scan_crossing(_crossing(), params=P)
    assert cand is not None
    assert cand.strategy_id == "ins"
    assert cand.side == "BUY" and cand.style == "swing"
    assert cand.raw_levels.entry == Decimal("100.00")
    assert cand.raw_levels.stop == Decimal("94.00")        # 100 x 0.94
    assert cand.raw_levels.target is None


def test_target_is_none_for_every_crossing_magnitude():
    """The absent target is STRUCTURAL, not an artefact of one fixture: `ins` exits on TIME (the §7.1
    20-td swing cap = the validated T+20 horizon), so no magnitude may conjure a price level."""
    for value in (10_000_000, 25_000_000, 100_000_000, 900_000_000):
        cand = scan_crossing(_crossing(value=value), params=P)
        assert cand is not None, value
        assert cand.raw_levels.target is None, value


def test_stop_sits_the_configured_percent_below_entry_across_prices():
    """The stop is a fixed FRACTION of entry, so the relationship must hold at every price scale —
    a percentage stop that only works at 100 is a hardcoded number in disguise."""
    for close, expected_stop in (
        ("100.00", "94.00"),
        ("250.00", "235.00"),
        ("1000.00", "940.00"),
        ("3333.35", "3133.35"),   # 3133.349 -> tick-rounded half-up
    ):
        cand = scan_crossing(_crossing(close=close), params=P)
        assert cand is not None, close
        assert cand.raw_levels.stop == Decimal(expected_stop), close
        assert cand.raw_levels.stop < cand.raw_levels.entry, close


def test_alternate_stop_pct_moves_only_the_stop():
    """`ins.stop_pct` is an owner knob (a FUTURE §6.3 envelope row, range [4-8]): the whole envelope
    must produce coherent levels, not just the 6.0 default."""
    for stop_pct, expected in (("4.0", "96.00"), ("6.0", "94.00"), ("8.0", "92.00")):
        cand = scan_crossing(_crossing(), params={**P, "stop_pct": float(stop_pct)})
        assert cand is not None, stop_pct
        assert cand.raw_levels.entry == Decimal("100.00"), stop_pct
        assert cand.raw_levels.stop == Decimal(expected), stop_pct


# =========================================================================== tick-rounding degeneracy
def test_tick_rounding_degeneracy_emits_nothing():
    """A stop distance that vanishes under NSE's ₹0.05 tick leaves ``stop >= entry`` — the §7.1
    ``levels_coherent`` shape is then unsatisfiable and the rule must emit NOTHING rather than ship
    an incoherent plan (the brk20 convention: coherence is tested AFTER rounding, not before).

    Hand-computed at the shipped 6%: 0.40 x 0.94 = 0.376, which is 7.52 ticks and rounds HALF-UP to
    8 ticks = 0.40 — straight back onto the entry, leaving no risk distance at all."""
    assert scan_crossing(_crossing(close="0.40"), params=P) is None
    # Everything below it collapses the same way (the stop distance is sub-half-tick throughout).
    for close in ("0.05", "0.15", "0.25", "0.35"):
        assert scan_crossing(_crossing(close=close), params=P) is None, close


def test_tick_rounding_boundary_the_smallest_price_that_still_emits():
    """The EXACT boundary of the degeneracy above, hand-computed at the shipped 6% stop:

    * entry 0.40 -> 0.376 = 7.52 ticks -> half-up 8 ticks = 0.40 == entry  => refuses (above).
    * entry 0.45 -> 0.423 = 8.46 ticks -> half-up 8 ticks = 0.40 <  entry  => emits, by one tick.

    0.45 is therefore the smallest reference price at which `ins` can produce a candidate at all."""
    emits = scan_crossing(_crossing(close="0.45"), params=P)
    assert emits is not None
    assert emits.raw_levels.entry == Decimal("0.45")
    assert emits.raw_levels.stop == Decimal("0.40")      # exactly one tick of risk — the minimum
    assert scan_crossing(_crossing(close="0.40"), params=P) is None


def test_non_positive_or_absurd_inputs_fail_to_none_never_raise():
    """§3.2.5 fail-to-zero posture: bad data costs the candidate, never the sweep."""
    assert scan_crossing(_crossing(close="0"), params=P) is None
    assert scan_crossing(_crossing(close="-100.00"), params=P) is None
    assert scan_crossing(_crossing(), params={**P, "stop_pct": 0.0}) is None
    assert scan_crossing(_crossing(), params={**P, "stop_pct": 100.0}) is None


# =========================================================================== score
def test_score_is_orders_of_magnitude_above_the_threshold():
    """score = min(1, max(0, 0.5 + 0.5 x log10(value / threshold))): a BARE crossing scores 0.5 and a
    10x cluster saturates at 1.0, so the strength signal is the crossing's magnitude."""
    bare = scan_crossing(_crossing(value=10_000_000), params=P)
    ten_x = scan_crossing(_crossing(value=100_000_000), params=P)
    hundred_x = scan_crossing(_crossing(value=1_000_000_000), params=P)
    assert bare is not None and ten_x is not None and hundred_x is not None
    assert abs(bare.score - 0.5) < 1e-9
    assert abs(ten_x.score - 1.0) < 1e-9
    assert hundred_x.score == 1.0                       # clamped, never above 1.0 (the contract bound)

    three_x = scan_crossing(_crossing(value=30_000_000), params=P)
    assert three_x is not None
    assert abs(three_x.score - (0.5 + 0.5 * math.log10(3.0))) < 1e-9


def test_score_is_monotone_in_crossing_value():
    values = [10_000_000, 15_000_000, 30_000_000, 60_000_000, 100_000_000]
    scores = [scan_crossing(_crossing(value=v), params=P).score for v in values]
    assert scores == sorted(scores)
    assert all(0.0 <= s <= 1.0 for s in scores)


# =========================================================================== sweep
def test_sweep_orders_by_score_desc_then_symbol_and_drops_degenerates():
    """§9.6 determinism, matching ``brk20.sweep_daily``'s convention exactly."""
    rows = [
        _crossing("CCC", value=10_000_000),      # score 0.5
        _crossing("AAA", value=100_000_000),     # score 1.0
        _crossing("BBB", value=100_000_000),     # score 1.0 — ties break on symbol asc
        _crossing("DDD", close="0.40"),          # tick-degenerate -> dropped
    ]
    out = sweep_crossings(rows, params=P)
    assert [c.symbol for c in out] == ["AAA", "BBB", "CCC"]
    assert out[0].score >= out[1].score >= out[2].score


def test_sweep_of_nothing_is_empty_not_an_error():
    assert sweep_crossings([], params=P) == []


def test_defaults_match_the_shipped_settings_block():
    """``DEFAULT_PARAMS`` documents the same numbers ``config/settings.yaml``'s ``ins:`` block ships;
    a drift between the two would make the module docstring's pinned math a lie."""
    assert ins.DEFAULT_PARAMS["stop_pct"] == 6.0
    assert ins.DEFAULT_PARAMS["hold_sessions"] == 20
    assert ins.DEFAULT_PARAMS["threshold_inr"] == 10_000_000.0
