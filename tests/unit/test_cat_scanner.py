"""``cat`` v2 SHADOW scanner (§2.7 amendment, 2026-08-18) — watchlist row -> SignalCandidate.

PINNED worked example: an ``originating``/``long`` watchlist row of weighted materiality 0.72, on a
symbol whose PRIOR-session bhavcopy close was 100.00, at the shipped 5% stop. All arithmetic is
hand-computed and params are passed explicitly, so an owner settings change can never silently break
these tests.

The eligibility filter (grade ∧ direction ∧ event age <= 1) is tested HERE rather than against a
database because for ``cat`` the filter IS the rule's event — single-shot semantics included — and
``sweep_watchlist`` owns it (``ins`` can afford to filter in SQL: its ``consumed`` flag is
bookkeeping, not the rule). The last section pins the OTHER half of the caps story: the
``catalyst_guard.max_catalyst_entries_day`` bound the pre-screen enforces on any candidate carrying a
``catalyst_ref``.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from engine.strategy.prescreen import SignalPreScreen
from engine.strategy.scanners import cat
from engine.strategy.scanners.cat import WatchlistRow, is_eligible, scan_entry, sweep_watchlist
from engine.strategy.types import RawLevels, ScanContext, SignalCandidate

DAY = date(2026, 8, 18)
P = {"stop_pct": 5.0, "hold_sessions": 20}


def _row(
    symbol: str = "AAA",
    *,
    close: str | None = "100.00",
    materiality: float | None = 0.72,
    grade: str = "originating",
    direction: str | None = "long",
    age: int | None = 1,
    entry_id: str | None = None,
) -> WatchlistRow:
    return WatchlistRow(
        entry_id=entry_id or f"entry-{symbol}",
        symbol=symbol,
        grade=grade,
        direction=direction,
        event_age_sessions=age,
        materiality=materiality,
        reference_close=None if close is None else Decimal(close),
    )


# =========================================================================== pinned level math
def test_pinned_level_math_entry_stop_and_no_target():
    """entry = the pre-open reference (the PRIOR session's bhavcopy close); stop = entry x (1 - 5/100);
    target = None — the v1 ATR/rr_target band died with the retired confirmation design (WO-18) and
    is never re-invented."""
    cand = scan_entry(_row(), params=P)
    assert cand is not None
    assert cand.strategy_id == "cat"
    assert cand.side == "BUY" and cand.style == "swing"
    assert cand.raw_levels.entry == Decimal("100.00")
    assert cand.raw_levels.stop == Decimal("95.00")        # 100 x 0.95
    assert cand.raw_levels.target is None


def test_catalyst_ref_carries_the_watchlist_entry_id():
    """The §6.5 audit link: candidate -> graded row -> (via cluster_refs) the headlines behind it.
    It is ALSO the field the pre-screen's catalyst cap keys on, so losing it would silently unbind
    the §2.7 guard."""
    cand = scan_entry(_row("RELIANCE", entry_id="01J-ENTRY-ULID"), params=P)
    assert cand is not None
    assert cand.catalyst_ref == "01J-ENTRY-ULID"
    assert cand.symbol == "RELIANCE"


def test_target_is_none_at_every_materiality():
    """The absent target is STRUCTURAL, not an artefact of one fixture: `cat` exits on TIME (the §7.1
    20-td swing cap), so no story's strength may conjure a price level."""
    for materiality in (0.20, 0.55, 0.70, 0.95, 1.0):
        cand = scan_entry(_row(materiality=materiality), params=P)
        assert cand is not None, materiality
        assert cand.raw_levels.target is None, materiality


def test_stop_sits_the_configured_percent_below_entry_across_prices():
    """The stop is a fixed FRACTION of entry, so the relationship must hold at every price scale —
    a percentage stop that only works at 100 is a hardcoded number in disguise."""
    for close, expected_stop in (
        ("100.00", "95.00"),
        ("250.00", "237.50"),
        ("1000.00", "950.00"),
        ("3333.35", "3166.70"),   # 3166.6825 -> 63333.65 ticks -> half-up 63334 -> 3166.70
    ):
        cand = scan_entry(_row(close=close), params=P)
        assert cand is not None, close
        assert cand.raw_levels.stop == Decimal(expected_stop), close
        assert cand.raw_levels.stop < cand.raw_levels.entry, close


def test_entry_is_tick_rounded_not_the_raw_close():
    """A bhavcopy close is not guaranteed to sit on NSE's ₹0.05 tick; the ENTRY is quantized before
    it can reach the §7.1 levels_coherent / sizing arithmetic (A10)."""
    cand = scan_entry(_row(close="1234.567"), params=P)
    assert cand is not None
    assert cand.raw_levels.entry == Decimal("1234.55")     # 24691.34 ticks -> half-up 24691
    assert cand.raw_levels.stop == Decimal("1172.80")      # 1172.8225 -> 23456.45 -> half-up 23456


def test_alternate_stop_pct_moves_only_the_stop():
    """`cat.stop_pct` is an owner knob (frozen for the shadow window, not an envelope row): the
    plausible range must produce coherent levels, not just the shipped 5.0."""
    for stop_pct, expected in (("4.0", "96.00"), ("5.0", "95.00"), ("6.0", "94.00")):
        cand = scan_entry(_row(), params={**P, "stop_pct": float(stop_pct)})
        assert cand is not None, stop_pct
        assert cand.raw_levels.entry == Decimal("100.00"), stop_pct
        assert cand.raw_levels.stop == Decimal(expected), stop_pct


# =========================================================================== fail-to-zero (§3.2.5)
def test_tick_rounding_degeneracy_emits_nothing():
    """A stop distance that vanishes under NSE's ₹0.05 tick leaves ``stop >= entry`` — the §7.1
    ``levels_coherent`` shape is then unsatisfiable and the rule must emit NOTHING rather than ship
    an incoherent plan (the brk20/ins convention: coherence is tested AFTER rounding).

    Hand-computed at the shipped 5%: 0.50 x 0.95 = 0.4750, which is 9.5 ticks and rounds HALF-UP to
    10 ticks = 0.50 — straight back onto the entry, leaving no risk distance at all."""
    assert scan_entry(_row(close="0.50"), params=P) is None
    # Everything below it collapses the same way (the stop distance is sub-half-tick throughout).
    for close in ("0.05", "0.15", "0.25", "0.35", "0.45"):
        assert scan_entry(_row(close=close), params=P) is None, close


def test_tick_rounding_boundary_the_smallest_price_that_still_emits():
    """The EXACT boundary of the degeneracy above, hand-computed at the shipped 5% stop:

    * entry 0.50 -> 0.4750 = 9.50 ticks  -> half-up 10 ticks = 0.50 == entry => refuses (above).
    * entry 0.55 -> 0.5225 = 10.45 ticks -> half-up 10 ticks = 0.50 <  entry => emits, by one tick.

    0.55 is therefore the smallest reference price at which `cat` can produce a candidate at all."""
    emits = scan_entry(_row(close="0.55"), params=P)
    assert emits is not None
    assert emits.raw_levels.entry == Decimal("0.55")
    assert emits.raw_levels.stop == Decimal("0.50")        # exactly one tick of risk — the minimum
    assert scan_entry(_row(close="0.50"), params=P) is None


def test_missing_or_non_positive_reference_close_fails_to_none_never_raises():
    """A symbol with no prior daily bar in the lookback arrives with ``reference_close=None``; a
    zero/negative close is corrupt data. Both cost this candidate and nothing else (§3.2.5)."""
    assert scan_entry(_row(close=None), params=P) is None
    assert scan_entry(_row(close="0"), params=P) is None
    assert scan_entry(_row(close="-100.00"), params=P) is None
    assert scan_entry(_row(), params={**P, "stop_pct": 0.0}) is None
    assert scan_entry(_row(), params={**P, "stop_pct": 100.0}) is None


# =========================================================================== score
def test_score_is_the_rows_weighted_materiality():
    """§2.7: the score IS the digest's weighted materiality — the same number the grade decision
    used (fan-out weighting already applied upstream). Nothing is recomputed here."""
    for materiality in (0.55, 0.70, 0.72, 0.9):
        cand = scan_entry(_row(materiality=materiality), params=P)
        assert cand is not None, materiality
        assert abs(cand.score - materiality) < 1e-9, materiality


def test_score_is_clamped_into_the_contract_bounds():
    """SignalCandidate.score is contractually [0, 1]; an out-of-range materiality must be clamped,
    never allowed to raise a pydantic ValidationError inside the sweep."""
    over = scan_entry(_row(materiality=1.5), params=P)
    under = scan_entry(_row(materiality=-0.2), params=P)
    assert over is not None and over.score == 1.0
    assert under is not None and under.score == 0.0


def test_missing_materiality_still_originates_but_ranks_at_the_floor():
    """The GRADE is the origination decision and the digest already made it, so a null materiality
    may not silently drop a graded event — but an unmeasured magnitude must never flatter the §5.2(a)
    funnel either, so it ranks at 0.0."""
    cand = scan_entry(_row(materiality=None), params=P)
    assert cand is not None
    assert cand.score == 0.0
    assert cand.catalyst_ref == "entry-AAA"


# =========================================================================== eligibility (the EVENT)
def test_is_eligible_pins_grade_direction_and_age():
    assert is_eligible(_row()) is True
    assert is_eligible(_row(age=0)) is True                # a same-morning cluster is age 0
    assert is_eligible(_row(age=2)) is False              # age-2 re-grade: the single-shot bound
    assert is_eligible(_row(age=None)) is False
    assert is_eligible(_row(grade="context")) is False
    assert is_eligible(_row(direction="short")) is False   # §1.4.9 short gate is shut
    assert is_eligible(_row(direction=None)) is False


def test_sweep_owns_the_filter_so_a_widened_fetch_cannot_widen_the_rule():
    """The caller narrows its query for cheapness; the sweep is the AUTHORITY. Handing it the whole
    day's watchlist must still yield exactly the eligible rows."""
    rows = [
        _row("AAA"),                                   # eligible
        _row("BBB", grade="context"),                  # never originates (§2.7 step 5)
        _row("CCC", direction="short"),                # long-only
        _row("DDD", age=2),                            # age-2 re-grade of a story already fired
        _row("EEE", age=None),
    ]
    assert [c.symbol for c in sweep_watchlist(rows, params=P)] == ["AAA"]


def test_age_two_regrade_of_the_same_story_never_re_originates():
    """WO-18 single-shot semantics: the same (symbol, story) reappears on subsequent watchlists as
    context/advisory, and the age bound — not a consumed flag — is what stops a second entry."""
    day1 = sweep_watchlist([_row("HAL", age=1, entry_id="e1")], params=P)
    day2 = sweep_watchlist([_row("HAL", age=2, entry_id="e2")], params=P)
    assert [c.catalyst_ref for c in day1] == ["e1"]
    assert day2 == []


# =========================================================================== sweep
def test_sweep_orders_by_score_desc_then_symbol_and_drops_degenerates():
    """§9.6 determinism, matching ``ins.sweep_crossings``/``brk20.sweep_daily`` exactly."""
    rows = [
        _row("CCC", materiality=0.60),
        _row("AAA", materiality=0.95),
        _row("BBB", materiality=0.95),                  # ties break on symbol asc
        _row("DDD", materiality=0.99, close="0.50"),    # tick-degenerate -> dropped
        _row("EEE", materiality=0.99, close=None),      # no prior bar -> dropped
    ]
    out = sweep_watchlist(rows, params=P)
    assert [c.symbol for c in out] == ["AAA", "BBB", "CCC"]
    assert out[0].score >= out[1].score >= out[2].score


def test_sweep_of_nothing_is_empty_not_an_error():
    """The §2.7 fail-safe ladder's empty-watchlist case: no digest rows today ⇒ originate nothing."""
    assert sweep_watchlist([], params=P) == []


def test_defaults_match_the_shipped_settings_block():
    """``DEFAULT_PARAMS`` documents the same numbers ``config/settings.yaml``'s ``cat:`` block ships;
    a drift between the two would make the module docstring's pinned math a lie."""
    assert cat.DEFAULT_PARAMS["stop_pct"] == 5.0
    assert cat.DEFAULT_PARAMS["hold_sessions"] == 20
    assert cat.MAX_EVENT_AGE_SESSIONS == 1


def test_cat_is_not_a_registered_scanner():
    """`cat` v2 is a BATCH rule (like brk20/ins), not a per-bar :class:`Scanner`: the retired v1
    confirmation design was the only reason it would have registered."""
    from engine.strategy.scanners import SCANNER_REGISTRY
    assert "cat" not in SCANNER_REGISTRY


# ==================================================== catalyst_guard cap (§3.2.5 pre-screen, §7.1)
def _prescreen(**kw) -> SignalPreScreen:
    kw.setdefault("max_candidates_per_day", 20)
    return SignalPreScreen([], lambda bar: ScanContext(), None, **kw)


def _cat_cand(symbol: str, *, score: float = 0.8) -> SignalCandidate:
    return SignalCandidate(
        signal_id=f"sig-{symbol}", strategy_id="cat", symbol=symbol, side="BUY", style="swing",
        raw_levels=RawLevels(entry=Decimal("100.00"), stop=Decimal("95.00")), score=score,
        catalyst_ref=f"entry-{symbol}",
    )


def test_catalyst_guard_caps_entries_below_the_per_strategy_cap():
    """§7.1 ``catalyst_guard.max_catalyst_entries_day`` (2) binds INDEPENDENTLY of
    ``max_per_strategy_day``: with the per-strategy cap deliberately loosened to 5, the third `cat`
    candidate of the day is still refused. This is the news carve-out's own anti-manipulation bound
    and it is enforced at the pre-screen, never in the gate (§2.4 item 4)."""
    ps = _prescreen(max_per_strategy_day={"cat": 5}, catalyst_cap_fn=lambda: 2)
    admitted = ps.admit([_cat_cand("AAA"), _cat_cand("BBB"), _cat_cand("CCC")], DAY)
    assert [c.symbol for c in admitted] == ["AAA", "BBB"]
    assert ps.admit([_cat_cand("DDD")], DAY) == []          # and it stays bound for the rest of the day


def test_catalyst_cap_counts_across_admission_batches_and_resets_next_day():
    ps = _prescreen(max_per_strategy_day={"cat": 5}, catalyst_cap_fn=lambda: 2)
    assert [c.symbol for c in ps.admit([_cat_cand("AAA")], DAY)] == ["AAA"]
    assert [c.symbol for c in ps.admit([_cat_cand("BBB")], DAY)] == ["BBB"]
    assert ps.admit([_cat_cand("CCC")], DAY) == []
    assert [c.symbol for c in ps.admit([_cat_cand("CCC")], date(2026, 8, 19))] == ["CCC"]


def test_catalyst_cap_binds_the_field_not_the_strategy_id():
    """The guard bounds NEWS-originated entries: a candidate carrying a ``catalyst_ref`` is capped
    whatever its strategy id, and a `cat`-ish candidate without one is not this guard's business."""
    ps = _prescreen(catalyst_cap_fn=lambda: 1)
    tagged = _cat_cand("AAA").model_copy(update={"strategy_id": "somethingelse"})
    assert [c.symbol for c in ps.admit([tagged], DAY)] == ["AAA"]
    assert ps.admit([_cat_cand("BBB")], DAY) == []          # cap already spent by the tagged one
    untagged = _cat_cand("CCC").model_copy(update={"catalyst_ref": None})
    assert [c.symbol for c in ps.admit([untagged], DAY)] == ["CCC"]


def test_unreadable_or_unwired_guard_refuses_catalyst_candidates_only():
    """D7: an anti-manipulation surface that cannot be established must not degrade into "no cap".
    Price baselines are untouched — a news-layer failure never blocks another strategy (E5)."""
    def _boom() -> int:
        raise RuntimeError("limits.yaml hash mismatch")

    for ps in (_prescreen(), _prescreen(catalyst_cap_fn=_boom), _prescreen(catalyst_cap_fn=lambda: -1)):
        assert ps.admit([_cat_cand("AAA")], DAY) == []
        plain = _cat_cand("BBB").model_copy(update={"strategy_id": "brk20", "catalyst_ref": None})
        assert [c.symbol for c in ps.admit([plain], DAY)] == ["BBB"]


def test_catalyst_cap_keeps_the_best_of_a_batch():
    """WO-1 ranked admission still applies underneath: a binding catalyst cap keeps the batch's
    highest-scored stories, not whichever the sweep emitted first."""
    ps = _prescreen(max_per_strategy_day={"cat": 5}, catalyst_cap_fn=lambda: 2)
    out = ps.admit(
        [_cat_cand("LOW", score=0.10), _cat_cand("TOP", score=0.95), _cat_cand("MID", score=0.60)],
        DAY,
    )
    assert [c.symbol for c in out] == ["TOP", "MID"]
