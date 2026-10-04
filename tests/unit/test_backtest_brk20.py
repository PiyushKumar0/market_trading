"""scripts/backtest_brk20.py — the pre-registered brk20 entry-mechanism harness, on synthetic bars.

Six engineered symbols in a temp :class:`MarketStore`, each 47 sessions long so the ONLY measurable
signal in every one of them is at index 21 (``lookback_days`` 20 means index 21 is the first day
``scan_daily`` can fire on — it needs ``y``, its 20-session window and one more session for the
fresh-cross test — and the shared admission rule reserves ``MAX_FILL_WINDOW + 20`` = 25 forward
sessions, so index 21 is also the LAST index that can book a trade). Every symbol trades flat at
100.00 with every high pinned at 100.00 up to its trigger day, so ``H20`` is exactly 100.00 and the
rested limit is exactly 100.00; every forward high is 130.00, which makes a second breakout
arithmetically impossible and keeps the signal counts exact.

* ``WINNER``     — trigger close 104.00, then opens 103.00 and dips to 99.00: V1 fills at 103.00,
  V2 at the 100.00 level, and the two hand-computed return sets differ by the mechanism alone.
* ``GAPPER``     — trigger close 102.00, then GAPS THROUGH the level (open 95.00): V2 must fill at
  the OPEN, not at the level, and the two mechanisms then get the same price.
* ``LATEFILL``   — trigger close 106.00, then holds above the level for three sessions and only
  touches it on y+4: unfilled under V2-3, filled under V2-5, and its V1 and V2-5 exits land on
  DIFFERENT sessions — the fill-anchored horizon's whole point.
* ``NOFILL``     — trigger close 108.00, then never trades at or below the level: V1 fills, both V2
  variants book nothing, and the fill rate's denominator stays the shared admitted population.
* ``OUTSIDER``   — WINNER's bars, excluded ``not_in_index``: must never appear.
* ``CAPPEDPLUS`` — WINNER's bars, excluded ``['watchlist_cap', 'surveillance_asm']``: the STRICT
  single-reason eligibility equality must reject it.

Every price is exact in 2 dp (the ``bars_1d`` DECIMAL(12,2) column) so the arithmetic is
hand-checkable; the cost deduction is asserted as "gross minus exactly one CNC round trip" against
the repo's own CostModel rather than a frozen constant, so a costs.yaml re-scrape moves the expected
value with the model instead of breaking the test for the wrong reason.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

_SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "backtest_brk20.py"
_spec = importlib.util.spec_from_file_location("mt_backtest_brk20", _SCRIPT)
bt = importlib.util.module_from_spec(_spec)
sys.modules["mt_backtest_brk20"] = bt
_spec.loader.exec_module(bt)

from engine.learning.validate import fold_pass_min  # noqa: E402
from engine.marketdata.store import DailyBar, MarketStore  # noqa: E402
from engine.strategy.cost_model import CostModel  # noqa: E402
from engine.strategy.scanners import brk20  # noqa: E402

N_SESSIONS = 47
SIGNAL_IDX = 21                     # lookback_days (20) + 1: the first day scan_daily can fire on
ENTRY_IDX = SIGNAL_IDX + 1
LEVEL = 100.00                      # H20, and therefore the rested limit, for every fixture symbol
START = date(2024, 1, 1)


# --------------------------------------------------------------------------- synthetic bar seeding
def _sessions(n: int, start: date = START) -> list[date]:
    out: list[date] = []
    d = start
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d)
        d += timedelta(days=1)
    return out


SESSIONS = _sessions(N_SESSIONS)
UNIVERSE_DAY = SESSIONS[-1]


def _bar(sym: str, idx: int, *, o: str, h: str, lo: str, c: str, v: int = 1000) -> DailyBar:
    return DailyBar(
        symbol=sym, d=SESSIONS[idx], open=Decimal(o), high=Decimal(h), low=Decimal(lo),
        close=Decimal(c), volume=v, src="bhavcopy",
    )


def _flat(sym: str, idx: int, price: str, *, high: str, v: int = 1000) -> DailyBar:
    return _bar(sym, idx, o=price, h=high, lo=price, c=price, v=v)


def _prefix(sym: str) -> list[DailyBar]:
    """Sessions 0..20: dead flat at 100.00 with every high at 100.00, so H20 == 100.00 exactly and
    every overnight gap is 0.00 — which makes the WO-19 stop-geometry floor 2.0 x median(0) = 0 and
    therefore never the reason a fixture signal does or does not fire."""
    return [_flat(sym, i, "100.00", high="100.00") for i in range(SIGNAL_IDX)]


def _trigger(sym: str, close: str) -> DailyBar:
    """The breakout session: close above the 100.00 band on 2x the window's mean volume (the rule
    needs 1.2x). The high equals the close so the next session's own window sees no phantom high."""
    return _bar(sym, SIGNAL_IDX, o="100.00", h=close, lo="100.00", c=close, v=2000)


def winner_bars(sym: str = "WINNER") -> list[DailyBar]:
    """Trigger 104.00; entry session opens 103.00 and dips to 99.00 (V2 fills the level, V1 the
    open); closes 105 / 110 / 120 at T+5 / T+10 / T+20 counted FROM THE FILL at index 22."""
    bars = _prefix(sym) + [_trigger(sym, "104.00")]
    bars.append(_bar(sym, ENTRY_IDX, o="103.00", h="130.00", lo="99.00", c="100.00"))
    for i in range(ENTRY_IDX + 1, N_SESSIONS):
        if i < 27:
            bars.append(_flat(sym, i, "100.00", high="130.00"))
        elif i < 32:
            bars.append(_flat(sym, i, "105.00", high="130.00"))    # close(fill + 5)  = 105.00
        elif i < 42:
            bars.append(_flat(sym, i, "110.00", high="130.00"))    # close(fill + 10) = 110.00
        else:
            bars.append(_flat(sym, i, "120.00", high="130.00"))    # close(fill + 20) = 120.00
    return bars


def gapper_bars() -> list[DailyBar]:
    """Trigger 102.00, then the entry session GAPS THROUGH the level: open 95.00, low 94.00."""
    sym = "GAPPER"
    bars = _prefix(sym) + [_trigger(sym, "102.00")]
    bars.append(_bar(sym, ENTRY_IDX, o="95.00", h="130.00", lo="94.00", c="96.00"))
    bars += [_flat(sym, i, "96.00", high="130.00") for i in range(ENTRY_IDX + 1, N_SESSIONS)]
    return bars


def latefill_bars() -> list[DailyBar]:
    """Trigger 106.00; lows hold at 101.00 for y+1..y+3 and only reach 99.00 on y+4 (index 25).

    So V2-3's window (y+1..y+3) closes unfilled and V2-5 fills at index 25 — and the T+20 exits then
    land on DIFFERENT sessions: index 42 for V1's index-22 fill (close 102.00) and index 45 for
    V2-5's (close 115.00). A signal-anchored horizon would give both the same exit and hide the
    difference the fill anchor exists to measure.
    """
    sym = "LATEFILL"
    bars = _prefix(sym) + [_trigger(sym, "106.00")]
    bars += [_bar(sym, i, o="105.00", h="130.00", lo="101.00", c="105.00") for i in range(22, 25)]
    bars.append(_bar(sym, 25, o="102.00", h="130.00", lo="99.00", c="102.00"))
    for i in range(26, N_SESSIONS):
        bars.append(_flat(sym, i, "115.00" if i == 45 else "102.00", high="130.00"))
    return bars


def nofill_bars() -> list[DailyBar]:
    """Trigger 108.00, then the tape never returns to the level — the rested limit expires unfilled
    under both V2 windows while V1 is filled at the next open (107.00)."""
    sym = "NOFILL"
    bars = _prefix(sym) + [_trigger(sym, "108.00")]
    bars += [_flat(sym, i, "107.00", high="130.00") for i in range(ENTRY_IDX, N_SESSIONS)]
    return bars


#: The fixture's structural ex-date: 20 calendar days before the DECISION day, i.e. inside the
#: 35-day unadjusted-history window but far outside the 10-day UPCOMING-ex skip, so the two vetoes
#: cannot be confused for one another.
ACTION_EX = SESSIONS[ENTRY_IDX] - timedelta(days=20)

#: ``universe_daily`` exclusion vectors. NOFILL proves a cap-only row is ELIGIBLE; CAPPEDPLUS proves
#: the eligibility equality is STRICT (cap plus anything else is out); OUTSIDER is the ordinary
#: out-of-index name.
UNIVERSE_ROWS: dict[str, list[str]] = {
    "WINNER": [], "GAPPER": [], "LATEFILL": [],
    "NOFILL": ["watchlist_cap"],
    "OUTSIDER": ["not_in_index"],
    "CAPPEDPLUS": ["watchlist_cap", "surveillance_asm"],
}
ELIGIBLE = {"WINNER", "GAPPER", "LATEFILL", "NOFILL"}


def all_bars() -> list[DailyBar]:
    return (
        winner_bars() + gapper_bars() + latefill_bars() + nofill_bars()
        + winner_bars("OUTSIDER") + winner_bars("CAPPEDPLUS")
    )


def _corp_action(sym: str, ex: date, kind: str, clock) -> dict:
    """One ``corp_actions`` row shaped as ``CorpActionsJob.run`` stamps them."""
    return {
        "symbol": sym, "ex_date": ex, "kind": kind, "ratio": None, "amount": None,
        "source": "test", "recorded_at": clock.now(),
    }


def _universe_rows() -> list[dict]:
    return [
        {"d": UNIVERSE_DAY, "symbol": sym, "included": not reasons,
         "exclusion_reasons": reasons or None, "median_traded_value": Decimal("50000000.00")}
        for sym, reasons in UNIVERSE_ROWS.items()
    ]


def _seed(path: Path, parquet: Path, clock, corp_actions: list[dict], *, universe: bool = True) -> Path:
    """A temp market.duckdb seeded with the six symbols, CLOSED so the study can attach read-only."""
    store = MarketStore(path, parquet, clock).open()
    try:
        store.upsert_bars_1d(all_bars())
        if universe:
            store.upsert_universe_daily(_universe_rows())
        if corp_actions:
            store.upsert_corp_actions(corp_actions)
    finally:
        store.close()
    return path


@pytest.fixture
def db_file(tmp_path, clock) -> Path:
    return _seed(tmp_path / "market.duckdb", tmp_path / "parquet", clock, [])


@pytest.fixture
def db_no_universe(tmp_path, clock) -> Path:
    return _seed(tmp_path / "bare.duckdb", tmp_path / "parquet_bare", clock, [], universe=False)


@pytest.fixture
def db_with_action(tmp_path, clock):
    """``kind -> `` a temp DB whose WINNER carries that corp action on :data:`ACTION_EX`."""
    def _make(kind: str) -> Path:
        return _seed(
            tmp_path / f"market_{kind}.duckdb", tmp_path / f"parquet_{kind}", clock,
            [_corp_action("WINNER", ACTION_EX, kind, clock)],
        )
    return _make


def _run(db: Path):
    conn = bt.open_readonly(db)
    try:
        return bt.run_study(
            conn,
            start=SESSIONS[0],
            end=SESSIONS[-1],
            cost_model=CostModel.from_config(),
            verify_window=10,          # re-scan every fixture symbol with the full row prefix
        )
    finally:
        conn.close()


@pytest.fixture
def study(db_file):
    return _run(db_file)


def _by_symbol(trades, variant) -> dict:
    return {t.symbol: t for t in trades[variant]}


def _cost_pct() -> float:
    return float(CostModel.from_config().breakeven_pct(bt.REFERENCE_NOTIONAL, "CNC"))


# ============================================================ 0. pre-registration discipline
def test_three_pre_registered_variants_no_sweep():
    """N=3 and the rule params ARE the live scanner's frozen defaults — no sweep, no drift."""
    assert bt.TRIAL_COUNT_N == 3
    assert bt.PRE_REGISTERED_PARAMS == dict(brk20.DEFAULT_PARAMS)
    assert bt.PRE_REGISTERED_FLOOR_PARAMS == dict(brk20.FLOOR_PARAMS)
    assert bt.PRE_REGISTERED_PARAMS["lookback_days"] == 20
    assert bt.PRE_REGISTERED_PARAMS["vol_mult"] == 1.2
    assert bt.VARIANTS == (bt.VARIANT_V1, bt.VARIANT_V2_N3, bt.VARIANT_V2_N5)
    assert bt.FILL_WINDOW == {bt.VARIANT_V1: None, bt.VARIANT_V2_N3: 3, bt.VARIANT_V2_N5: 5}
    assert bt.MAX_FILL_WINDOW == 5
    assert fold_pass_min(bt.TRIAL_COUNT_N) == 0.60
    # the CLI exposes no knob that could turn three trials into many
    flags = {a.option_strings[0] for a in bt.build_parser()._actions if a.option_strings}
    assert not (flags & {"--vol-mult", "--lookback", "--fill-window", "--horizon", "--grid-density"})


def test_unadjusted_window_is_the_live_brk20_sweep_window():
    """35 calendar days, not hi52's 400: brk20's lookback is 20 sessions, and reading hi52's window
    here would veto symbols the live brk20 sweep happily scans."""
    assert bt.UNADJUSTED_LOOKBACK_DAYS == 35
    today = date(2025, 6, 30)
    span = bt.UNADJUSTED_LOOKBACK_DAYS
    assert bt._unadjusted_at([], today) is False
    assert bt._unadjusted_at([today], today) is False                       # today = the A12 skip's case
    assert bt._unadjusted_at([today - timedelta(days=1)], today) is True
    assert bt._unadjusted_at([today - timedelta(days=span)], today) is True
    assert bt._unadjusted_at([today - timedelta(days=span + 1)], today) is False
    assert bt._unadjusted_at([today + timedelta(days=1)], today) is False


def test_cost_path_is_cnc_delivery_and_spread_inclusive():
    cm = CostModel.from_config()
    full = float(cm.breakeven_pct(bt.REFERENCE_NOTIONAL, "CNC"))
    fees = float(cm.fee_breakeven_pct(bt.REFERENCE_NOTIONAL, "CNC"))
    mis = float(cm.breakeven_pct(bt.REFERENCE_NOTIONAL, "MIS"))
    assert bt.PRODUCT == "CNC"
    assert full > fees                     # the measured bid-ask spread is included (WO-2)
    assert full > mis                      # delivery, the dearer surface — not the intraday one
    assert 0.15 < full < 0.60              # sanity band around the documented ~0.32% CNC round trip


# ============================================================ 1. the signal set IS scan_daily's
def test_windowed_scan_matches_the_full_prefix(db_file):
    """The bounded scan window must offer scan_daily a BIT-IDENTICAL read to the full row prefix."""
    conn = bt.open_readonly(db_file)
    try:
        series = bt.load_series(conn, SESSIONS[0], SESSIONS[-1], symbols=sorted(ELIGIBLE))
    finally:
        conn.close()
    assert set(series) == ELIGIBLE
    for sym, s in series.items():
        fast = bt.signals_for(s, bt.PRE_REGISTERED_PARAMS, [])
        slow = bt.signals_for(s, bt.PRE_REGISTERED_PARAMS, [], full_prefix=True)
        assert [x.idx for x in fast] == [x.idx for x in slow] == [SIGNAL_IDX], sym
        assert [x.level for x in fast] == [x.level for x in slow], sym
        assert [x.score for x in fast] == [x.score for x in slow], sym
        # and the unadjusted veto must land on BOTH paths, or --verify-window would fire on it
        counts: dict[str, int] = {}
        assert bt.signals_for(s, bt.PRE_REGISTERED_PARAMS, [], [ACTION_EX], veto_counts=counts) == []
        assert bt.signals_for(s, bt.PRE_REGISTERED_PARAMS, [], [ACTION_EX], full_prefix=True) == []
        assert counts == {brk20.VETO_UNADJUSTED_HISTORY: 1}, sym


def test_signal_level_is_the_rules_own_tick_rounded_entry(study):
    """The rested level is the candidate's ``raw_levels.entry``, and the margin denominator read
    back for the tercile split is the same physical number (``signals_for`` tripwires otherwise)."""
    doc, trades = study
    assert doc["meta"]["n_signals_after_vetoes"] == len(ELIGIBLE)
    assert doc["meta"]["n_signals_admitted"] == len(ELIGIBLE)
    t = _by_symbol(trades, bt.VARIANT_V2_N5)["WINNER"]
    assert t.margin == pytest.approx(0.04, abs=1e-9)        # close(y) 104.00 / H20 100.00 - 1
    assert t.fill_px == pytest.approx(LEVEL)                # the level is what the rule shipped
    # the margins the tercile split reads are the rule's own strength signal, one per fixture
    margins = {s: _by_symbol(trades, bt.VARIANT_V1)[s].margin for s in ELIGIBLE}
    assert margins == pytest.approx(
        {"GAPPER": 0.02, "WINNER": 0.04, "LATEFILL": 0.06, "NOFILL": 0.08}, abs=1e-9
    )


# ============================================================ 2. hand-computed fills, net of costs
def test_v1_and_v2_book_the_same_signal_at_different_prices(study):
    """WINNER: V1 pays the next OPEN (103.00), V2 rests and gets the level (100.00). Same signal,
    same exit sessions, and the only difference in the return is the mechanism."""
    _doc, trades = study
    cost = _cost_pct()
    v1 = _by_symbol(trades, bt.VARIANT_V1)["WINNER"]
    v2 = _by_symbol(trades, bt.VARIANT_V2_N5)["WINNER"]

    assert v1.signal_date == v2.signal_date == SESSIONS[SIGNAL_IDX]
    assert v1.fill_date == v2.fill_date == SESSIONS[ENTRY_IDX]
    assert v1.fill_px == pytest.approx(103.00)          # the next session's OPEN
    assert v1.fill_px != pytest.approx(104.00)          # NOT the signal close — no same-bar fill
    assert v2.fill_px == pytest.approx(LEVEL)           # the rested limit, not the 103.00 open
    assert v1.fill_delay_sessions == v2.fill_delay_sessions == 1

    for horizon, exit_px in ((5, 105.0), (10, 110.0), (20, 120.0)):
        assert v2.gross[horizon] == pytest.approx((exit_px / 100.0 - 1.0) * 100.0, abs=1e-9)
        assert v1.gross[horizon] == pytest.approx((exit_px / 103.0 - 1.0) * 100.0, abs=1e-9)
        assert v2.net[horizon] == pytest.approx(v2.gross[horizon] - cost, abs=1e-9)
        assert v1.net[horizon] == pytest.approx(v1.gross[horizon] - cost, abs=1e-9)
    # exactly ONE round trip is deducted, at every horizon — never one per session held
    assert v1.gross[20] - v1.net[20] == pytest.approx(v1.gross[5] - v1.net[5], abs=1e-12)


def test_a_gap_through_fills_at_the_open_not_at_the_level(study):
    """GAPPER opens at 95.00, below the 100.00 limit: ``min(open, level)`` must book 95.00 — a
    marketable-on-open limit gets the opening print, and booking 100.00 would invent a fill the
    tape never offered."""
    _doc, trades = study
    v2 = _by_symbol(trades, bt.VARIANT_V2_N3)["GAPPER"]
    v1 = _by_symbol(trades, bt.VARIANT_V1)["GAPPER"]
    assert v2.fill_px == pytest.approx(95.00)
    assert v2.fill_px != pytest.approx(LEVEL)
    assert v1.fill_px == pytest.approx(95.00)     # a gap-through gives both mechanisms one price
    assert v1.gross == v2.gross


def test_the_limit_window_boundary_is_n_sessions_exactly(study):
    """LATEFILL is touched on y+4: V2-3's window (y+1..y+3) expires unfilled, V2-5's fills."""
    _doc, trades = study
    assert "LATEFILL" not in _by_symbol(trades, bt.VARIANT_V2_N3)
    late = _by_symbol(trades, bt.VARIANT_V2_N5)["LATEFILL"]
    assert late.fill_delay_sessions == 4
    assert late.fill_date == SESSIONS[25]
    assert late.fill_px == pytest.approx(LEVEL)   # min(open 102.00, level 100.00)


def test_horizons_are_anchored_on_the_fill_not_the_signal(study):
    """LATEFILL's V1 and V2-5 legs fill 3 sessions apart, so their T+20 exits are 3 sessions apart
    too: index 42 (close 102.00) and index 45 (close 115.00). A signal anchor would give both the
    same exit and silently charge V2 for the days it spent waiting."""
    _doc, trades = study
    v1 = _by_symbol(trades, bt.VARIANT_V1)["LATEFILL"]
    v5 = _by_symbol(trades, bt.VARIANT_V2_N5)["LATEFILL"]
    assert (v1.fill_idx, v5.fill_idx) == (22, 25)
    assert v1.gross[20] == pytest.approx((102.0 / 105.0 - 1.0) * 100.0, abs=1e-9)
    assert v5.gross[20] == pytest.approx((115.0 / 100.0 - 1.0) * 100.0, abs=1e-9)


def test_an_untouched_level_books_no_trade_and_the_fill_rate_says_so(study):
    """NOFILL's limit is never reached: both V2 variants book nothing, V1 books the open, and the
    fill rate's denominator stays the SHARED admitted population — not each variant's own."""
    doc, trades = study
    assert "NOFILL" in _by_symbol(trades, bt.VARIANT_V1)
    for variant in (bt.VARIANT_V2_N3, bt.VARIANT_V2_N5):
        assert "NOFILL" not in _by_symbol(trades, variant)
    blocks = doc["variants"]
    assert blocks[bt.VARIANT_V1]["fill_rate"] == 1.0
    assert blocks[bt.VARIANT_V1]["n_filled"] == 4 and blocks[bt.VARIANT_V1]["n_unfilled"] == 0
    assert blocks[bt.VARIANT_V2_N3]["n_filled"] == 2 and blocks[bt.VARIANT_V2_N3]["n_unfilled"] == 2
    assert blocks[bt.VARIANT_V2_N5]["n_filled"] == 3 and blocks[bt.VARIANT_V2_N5]["n_unfilled"] == 1
    for variant in bt.VARIANTS:
        b = blocks[variant]
        # ONE shared SIGNAL population; the TRADE sets above are deliberately NOT shared
        assert b["n_signals_admitted"] == 4
        assert b["n_filled"] + b["n_unfilled"] == 4
        assert b["fill_rate"] == pytest.approx(b["n_filled"] / 4)


# ============================================================ 3. the population predicate
def test_population_is_the_eligible_universe_applied_backwards(study, db_file):
    """``included`` OR cap-only; an out-of-index name and a cap-PLUS-something name are both out."""
    conn = bt.open_readonly(db_file)
    try:
        eligible, source, as_of = bt.load_eligible_universe(conn)
    finally:
        conn.close()
    assert eligible == ELIGIBLE
    assert as_of == UNIVERSE_DAY
    assert str(UNIVERSE_DAY) in source

    doc, trades = study
    booked = {t.symbol for v in bt.VARIANTS for t in trades[v]}
    assert booked <= ELIGIBLE
    assert "OUTSIDER" not in booked and "CAPPEDPLUS" not in booked
    assert doc["meta"]["n_eligible_symbols"] == 4
    assert doc["meta"]["population_is_survivorship_tainted_proxy"] is True
    assert any("SURVIVORSHIP-TAINTED PROXY" in n for n in doc["notes"])


# ============================================================ 4. the margin-tercile split
def test_margin_tercile_split_partitions_the_population(study):
    """The cut is taken ONCE over the admitted signals, so a signal sits in the same cell under
    every variant and the cells compare mechanisms rather than populations."""
    doc, trades = study
    cuts = doc["margin_tercile_cuts"]
    assert cuts is not None and cuts["cut_1"] < cuts["cut_2"]

    by_symbol = _by_symbol(trades, bt.VARIANT_V1)
    assert by_symbol["GAPPER"].tercile == bt.CELL_MARGIN_LOW       # margin 0.02, the narrowest
    assert by_symbol["NOFILL"].tercile == bt.CELL_MARGIN_HIGH      # margin 0.08, the widest
    assert {by_symbol[s].tercile for s in ("WINNER", "LATEFILL")} == {bt.CELL_MARGIN_MID}
    # every variant sees the SAME tercile for the same symbol
    for variant in (bt.VARIANT_V2_N3, bt.VARIANT_V2_N5):
        for sym, t in _by_symbol(trades, variant).items():
            assert t.tercile == by_symbol[sym].tercile

    for variant in bt.VARIANTS:
        cells = doc["variants"][variant]["cells"]
        total = sum(cells[name]["n_trades"] for name in bt.MARGIN_CELLS)
        assert total == cells[bt.CELL_ALL]["n_trades"]             # a partition, never a double count
    # the widest-margin signal is exactly the one the rested limit never catches, which is the
    # mechanism cost the split exists to surface
    assert doc["variants"][bt.VARIANT_V2_N5]["cells"][bt.CELL_MARGIN_HIGH]["n_trades"] == 0
    assert doc["variants"][bt.VARIANT_V1]["cells"][bt.CELL_MARGIN_HIGH]["n_trades"] == 1
    assert any("DESCRIPTIVE, NOT TRADEABLE" in n for n in doc["notes"])


def test_tercile_of_is_a_total_partition_on_the_cuts():
    cuts = (0.03, 0.07)
    assert bt.tercile_of(0.03, cuts) == bt.CELL_MARGIN_LOW          # the cut itself is LOW
    assert bt.tercile_of(0.0300001, cuts) == bt.CELL_MARGIN_MID
    assert bt.tercile_of(0.07, cuts) == bt.CELL_MARGIN_MID          # and cut_2 is MIDDLE
    assert bt.tercile_of(0.0700001, cuts) == bt.CELL_MARGIN_HIGH
    assert bt.tercile_of(0.05, None) is None
    assert bt.tercile_of(float("nan"), cuts) is None
    assert bt.margin_tercile_cuts([]) is None


# ============================================================ 4B. the matched-cohort decomposition
def test_the_signal_population_is_shared_but_the_trade_sets_are_not(study):
    """The claim the 2026-09-12 audit struck must not come back: all three variants are offered the
    SAME admitted signals, and exactly two of them book a different set of TRADES."""
    doc, trades = study
    admitted = {(t.symbol, t.signal_date) for t in trades[bt.VARIANT_V1]}
    n3 = {(t.symbol, t.signal_date) for t in trades[bt.VARIANT_V2_N3]}
    n5 = {(t.symbol, t.signal_date) for t in trades[bt.VARIANT_V2_N5]}
    assert n3 < admitted and n5 < admitted            # strict subsets: the limit forfeits signals
    assert n3 < n5                                    # and the windows nest
    assert len(admitted) == 4 and len(n3) == 2 and len(n5) == 3

    text = bt.render_text(doc)
    assert "one shared population" not in text.lower()
    assert "ONE SHARED SIGNAL POPULATION" in text
    assert "trade sets      : NOT SHARED" in text
    assert any("SHARED SIGNAL POPULATION, DIFFERENT TRADE SETS" in n for n in doc["notes"])
    md = bt.render_markdown(doc)
    assert "share a SIGNAL population but NOT a trade set" in md


def test_matched_cohorts_split_v1_by_whether_the_v2_limit_filled(study):
    """V1 re-quoted on the cohort V2-5 FILLED and on the cohort it never filled, hand-checked.

    V2-5 fills WINNER (level 100.00), GAPPER (gap-through 95.00) and LATEFILL (level 100.00 on y+4);
    NOFILL's level is never touched. V1 books all four at the next open. At T+20 the V1 legs are
    WINNER 120/103 = +16.5049%, GAPPER 96/95 = +1.0526%, LATEFILL 102/105 = -2.8571%, NOFILL 0.0%.
    """
    doc, _trades = study
    blk = doc["matched_cohorts"]["variants"][bt.VARIANT_V2_N5]
    assert doc["matched_cohorts"]["descriptive_post_hoc_not_a_registered_trial"] is True
    assert blk["n_v2_filled"] == 3 and blk["n_v2_unfilled"] == 1
    assert blk["every_v2_fill_has_a_v1_leg"] is True

    filled = blk["cells"][bt.cohort_label(bt.VARIANT_V2_N5, bt.COHORT_FILLED)]
    unfilled = blk["cells"][bt.cohort_label(bt.VARIANT_V2_N5, bt.COHORT_UNFILLED)]
    v2 = blk["cells"][f"{bt.VARIANT_V2_N5}_{bt.COHORT_FILLED}"]
    # the two cohorts PARTITION V1's trades: nothing double-counted, nothing dropped
    assert filled["n_trades"] == 3 and unfilled["n_trades"] == 1 and v2["n_trades"] == 3
    assert filled["n_trades"] + unfilled["n_trades"] == doc["variants"][bt.VARIANT_V1]["n_filled"]

    gapper20 = (96.0 / 95.0 - 1.0) * 100.0                       # the median of the filled cohort
    assert filled["horizons"]["20"]["median_gross"] == pytest.approx(gapper20, abs=1e-4)
    assert unfilled["horizons"]["20"]["median_gross"] == pytest.approx(0.0, abs=1e-9)   # NOFILL
    assert v2["horizons"]["20"]["median_gross"] == pytest.approx(15.0, abs=1e-4)        # LATEFILL
    cost = _cost_pct()
    assert filled["horizons"]["20"]["median_net"] == pytest.approx(gapper20 - cost, abs=1e-4)

    e = blk["effects"]["20"]
    # PRICE: V2 minus V1 on the SAME events. SELECTION: V1 filled minus V1 unfilled.
    assert e["price_effect_median_net_pp"] == pytest.approx(15.0 - gapper20, abs=1e-3)
    assert e["selection_effect_median_net_pp"] == pytest.approx(gapper20, abs=1e-3)
    assert e["pooled_gap_median_net_pp"] is not None
    assert "BOTH" in e["carried_by"]

    # V2-3 is a different, smaller cohort: LATEFILL is only touched on y+4, outside its window
    blk3 = doc["matched_cohorts"]["variants"][bt.VARIANT_V2_N3]
    assert blk3["n_v2_filled"] == 2 and blk3["n_v2_unfilled"] == 2
    f3 = blk3["cells"][bt.cohort_label(bt.VARIANT_V2_N3, bt.COHORT_FILLED)]["horizons"]["20"]
    u3 = blk3["cells"][bt.cohort_label(bt.VARIANT_V2_N3, bt.COHORT_UNFILLED)]["horizons"]["20"]
    assert f3["n"] == 2 and u3["n"] == 2
    # V1 on the UNFILLED cohort is LATEFILL (-2.8571%) and NOFILL (0.0%) -> median -1.4286%
    assert u3["median_gross"] == pytest.approx(-(100.0 * 3.0 / 105.0) / 2.0, abs=1e-4)


def test_carried_by_names_the_effect_and_never_guesses():
    """The four sign combinations, stated plainly — and an n=0 cell is 'unknown', never a verdict."""
    assert "PRICE" in bt._carried_by(2.0, -1.0)
    assert "runs AGAINST" in bt._carried_by(2.0, -1.0)
    assert "SELECTION" in bt._carried_by(-1.0, 2.0)
    assert bt._carried_by(2.0, 1.0).startswith("BOTH")
    assert bt._carried_by(-2.0, -1.0).startswith("NEITHER")
    assert bt._carried_by(None, 1.0).startswith("unknown")
    assert bt._carried_by(1.0, None).startswith("unknown")


def test_every_cell_carries_its_own_fill_rate_and_the_tercile_claim_is_matched(study):
    """The 'in every margin tercile' claim is only readable off MATCHED cells with n per cell.

    On the fixture the widest tercile is NOFILL alone — the one signal the rested limit never
    catches — so V2 has no trade there at all and the claim is NOT TESTABLE, never 'true'.
    """
    doc, _trades = study
    cells = doc["variants"][bt.VARIANT_V2_N5]["cells"]
    assert cells[bt.CELL_ALL]["n_signals_in_cell"] == 4
    assert cells[bt.CELL_ALL]["fill_rate_in_cell"] == pytest.approx(0.75)
    assert cells[bt.CELL_MARGIN_HIGH]["n_signals_in_cell"] == 1          # NOFILL
    assert cells[bt.CELL_MARGIN_HIGH]["n_trades"] == 0
    assert cells[bt.CELL_MARGIN_HIGH]["fill_rate_in_cell"] == 0.0
    assert cells[bt.CELL_MARGIN_HIGH]["n_unfilled_in_cell"] == 1
    assert cells[bt.CELL_MARGIN_LOW]["fill_rate_in_cell"] == 1.0        # GAPPER, gapped through
    # V1 fills everything, so its per-cell fill rate is 1.0 in every cell
    for name in (bt.CELL_ALL, *bt.MARGIN_CELLS):
        assert doc["variants"][bt.VARIANT_V1]["cells"][name]["fill_rate_in_cell"] == 1.0

    ter = doc["matched_cohorts"]["variants"][bt.VARIANT_V2_N5]["by_margin_tercile"]
    assert ter[bt.CELL_MARGIN_HIGH]["n_signals"] == 1
    assert ter[bt.CELL_MARGIN_HIGH]["n_v2_filled"] == 0
    assert ter[bt.CELL_MARGIN_HIGH]["n_v2_unfilled"] == 1
    assert ter[bt.CELL_MARGIN_HIGH]["fill_rate"] == 0.0
    assert ter[bt.CELL_MARGIN_HIGH]["price_effect_median_net_pp"]["20"] is None
    assert ter[bt.CELL_MARGIN_LOW]["fill_rate"] == 1.0
    holds = doc["matched_cohorts"]["variants"][bt.VARIANT_V2_N5][
        "v2_beats_matched_v1_in_every_margin_tercile"]
    assert holds == {"5": None, "10": None, "20": None}                 # NOT TESTABLE, not True
    text = bt.render_text(doc)
    assert "NOT TESTABLE" in text
    assert "STEP 3B - MATCHED-COHORT DECOMPOSITION" in text


# ============================================================ 5. geometry, CPCV, the decision rule
def test_geometry_is_the_median_gross_of_the_variants_own_fills(study):
    """The geometry line is per VARIANT, over that variant's own fills — the unfilled signals are
    absent from it, not zero-filled, or a mechanism that never fills would look costless."""
    doc, _trades = study
    cost = _cost_pct()
    v2 = doc["geometry"][bt.VARIANT_V2_N5]["horizons"]["20"]
    assert doc["geometry"][bt.VARIANT_V2_N5]["cost_floor_pct"] == pytest.approx(cost, abs=1e-6)
    # V2-5 books WINNER (+20%), GAPPER (96/95 - 1) and LATEFILL (+15%) -> median +15%.
    assert v2["n"] == 3
    assert v2["median_gross_pct"] == pytest.approx(15.0, abs=1e-4)
    assert v2["verdict"] == "viable"
    # V1 books all four: -2.857% (LATEFILL), 0.000% (NOFILL), +1.053% (GAPPER), +16.505% (WINNER).
    v1 = doc["geometry"][bt.VARIANT_V1]["horizons"]["20"]
    assert v1["n"] == 4
    assert v1["median_gross_pct"] == pytest.approx(50.0 / 95.0, abs=1e-4)
    assert v1["margin_pct"] == pytest.approx(50.0 / 95.0 - cost, abs=1e-4)


def test_geometry_verdict_is_strict_against_the_cost_floor():
    """`viable` iff the MEDIAN gross clears one full round trip — equality is DEAD, because a trade
    that exactly pays its costs earns nothing. Probed directly so the boundary is exercised."""
    def _t(gross: float) -> bt.Trade:
        return bt.Trade(
            symbol="X", variant=bt.VARIANT_V1, signal_date=SESSIONS[0], fill_date=SESSIONS[1],
            fill_idx=1, fill_px=100.0, fill_delay_sessions=1, margin=0.01, tercile=None,
            gross={20: gross}, net={20: gross - 0.3192},
        )
    at_floor = bt.geometry([_t(0.3192)], 0.3192, horizons=(20,))["horizons"]["20"]
    assert at_floor["verdict"] == "dead" and at_floor["margin_pct"] == pytest.approx(0.0)
    below = bt.geometry([_t(0.3191)], 0.3192, horizons=(20,))["horizons"]["20"]
    assert below["verdict"] == "dead"
    above = bt.geometry([_t(0.3193)], 0.3192, horizons=(20,))["horizons"]["20"]
    assert above["verdict"] == "viable"
    empty = bt.geometry([], 0.3192, horizons=(20,))["horizons"]["20"]
    assert empty["n"] == 0 and empty["verdict"] == "unknown"   # honest n=0, never a silent zero


def test_cpcv_degrades_honestly_on_a_tiny_population(study):
    """Four signals on one day cannot be cross-validated: no folds, not promotable, and it SAYS so."""
    doc, _trades = study
    for variant in bt.VARIANTS:
        cpcv = doc["variants"][variant]["cpcv"]["20"]
        assert cpcv["n_splits"] == 0
        assert cpcv["trial_count_n"] == 3
        assert cpcv["fold_pass_min"] == fold_pass_min(3)
        assert cpcv["promotable"] is False
        assert any("CPCV produced no folds" in r for r in cpcv["reasons"])
        assert cpcv["margin_floor_pct_per_day"] == pytest.approx(_cost_pct() / 20.0, abs=1e-9)
    # the CPCV series is keyed on the FILL day: V1 fills everything on one day, V2-5 spans two
    assert doc["variants"][bt.VARIANT_V1]["cpcv"]["20"]["n_obs_days"] == 1
    assert doc["variants"][bt.VARIANT_V2_N5]["cpcv"]["20"]["n_obs_days"] == 2


def test_decision_rule_is_neither_when_nothing_is_promotable(study):
    doc, _trades = study
    d = doc["decision"]
    assert d["rule"] == bt.DECISION_RULE
    assert d["horizon_sessions"] == 10
    assert d["promotable_at_horizon"] == []
    assert d["outcome"] == bt.DECISION_NEITHER
    assert d["shipped_mechanism_unchanged"] is True


def _decision_doc(entries: dict[str, tuple]) -> dict:
    """A minimal document shaped exactly as :func:`decision_outcome` reads it.

    Each entry is ``(cpcv_promotable, median_net)`` or ``(cpcv_promotable, median_net,
    reported_promotable)``; the third element defaults to the first, which is what a run with no
    geometry disagreement produces.
    """
    def _row(entry: tuple) -> dict:
        promotable, med, *rest = entry
        reported = rest[0] if rest else promotable
        return {
            "cells": {bt.CELL_ALL: {"horizons": {str(bt.DECISION_HORIZON): {"median_net": med}}}},
            "cpcv": {str(bt.DECISION_HORIZON): {
                "promotable": promotable, "reported_promotable": reported,
            }},
        }
    return {"variants": {name: _row(entry) for name, entry in entries.items()}}


def test_decision_rule_picks_the_higher_median_among_promotable_variants_only():
    """A higher median that is NOT CPCV-promotable never wins — the proviso is the whole rule."""
    doc = _decision_doc({
        bt.VARIANT_V1: (False, 9.0),       # best median, not promotable -> ineligible
        bt.VARIANT_V2_N3: (True, 1.0),
        bt.VARIANT_V2_N5: (True, 2.0),
    })
    d = bt.decision_outcome(doc)
    assert d["promotable_at_horizon"] == [bt.VARIANT_V2_N5, bt.VARIANT_V2_N3]
    assert d["outcome"] == d["winner"] == bt.VARIANT_V2_N5
    assert d["shipped_mechanism_unchanged"] is True

    won_by_v1 = bt.decision_outcome(_decision_doc({
        bt.VARIANT_V1: (True, 3.0), bt.VARIANT_V2_N3: (True, 1.0), bt.VARIANT_V2_N5: (False, 9.0),
    }))
    assert won_by_v1["winner"] == bt.VARIANT_V1
    assert won_by_v1["shipped_mechanism_unchanged"] is False

    tie = bt.decision_outcome(_decision_doc({
        bt.VARIANT_V1: (True, 2.0), bt.VARIANT_V2_N3: (True, 2.0), bt.VARIANT_V2_N5: (False, None),
    }))
    assert tie["outcome"] == bt.DECISION_TIE and tie["winner"] is None

    # a promotable variant with no median (n=0 at the horizon) is not a winner by default
    none_med = bt.decision_outcome(_decision_doc({
        bt.VARIANT_V1: (True, None), bt.VARIANT_V2_N3: (False, None), bt.VARIANT_V2_N5: (False, None),
    }))
    assert none_med["outcome"] == bt.DECISION_NEITHER


def test_reported_promotable_requires_cpcv_AND_viable_geometry():
    """The 2026-09-12 reporting amendment: CPCV alone never earns a PROMOTABLE label.

    The gates are built on different statistics — the CPCV series is a per-fill-day MEAN, the
    geometry verdict a MEDIAN — so a tail-driven cell can pass CPCV while its typical trade pays the
    round trip for nothing (V1 at T+10 on the real run). Reported promotability is the AND.
    """
    dead = bt.apply_reporting_rule(
        {"promotable": True, "reasons": []}, {"verdict": "dead"}, -0.3192
    )
    assert dead["reported_promotable"] is False
    assert dead["gates_disagree"] is True
    assert dead["geometry_verdict"] == "dead" and dead["median_net_pct"] == -0.3192
    assert any("TIGHTENED REPORTING RULE" in r for r in dead["reported_promotable_reasons"])
    assert "MEAN" in dead["cpcv_series_statistic"] and "MEDIAN" in dead["geometry_statistic"]

    ok = bt.apply_reporting_rule({"promotable": True, "reasons": []}, {"verdict": "viable"}, 0.0513)
    assert ok["reported_promotable"] is True and ok["gates_disagree"] is False

    # a median net of EXACTLY zero is not viable: that trade pays the full round trip for nothing
    zero = bt.apply_reporting_rule({"promotable": True, "reasons": []}, {"verdict": "dead"}, 0.0)
    assert zero["reported_promotable"] is False
    # an honest n=0 cell has no median and is never reported promotable
    none_med = bt.apply_reporting_rule(
        {"promotable": True, "reasons": []}, {"verdict": "unknown"}, None
    )
    assert none_med["reported_promotable"] is False
    # and the amendment can only WITHHOLD: a CPCV refusal is never rescued by good geometry
    refused = bt.apply_reporting_rule({"promotable": False, "reasons": []}, {"verdict": "viable"}, 9.0)
    assert refused["reported_promotable"] is False and refused["gates_disagree"] is True


def test_the_tightened_gate_can_only_withhold_never_promote():
    """Same decision rule, tighter proviso: it can change the winner by REMOVING a candidate, and
    can never add one the registered CPCV gate refused."""
    doc = _decision_doc({
        bt.VARIANT_V1: (True, 3.0, False),       # CPCV passes, geometry dead -> withheld
        bt.VARIANT_V2_N3: (True, 1.0, True),
        bt.VARIANT_V2_N5: (False, 9.0, False),   # CPCV refuses; the amendment cannot rescue it
    })
    registered = bt.decision_outcome(doc)
    tightened = bt.decision_outcome(doc, gate_key="reported_promotable")
    assert registered["promotability_gate"] == "promotable"
    assert registered["winner"] == bt.VARIANT_V1                 # the registered rule, verbatim
    assert registered["shipped_mechanism_unchanged"] is False
    assert tightened["promotability_gate"] == "reported_promotable"
    assert tightened["winner"] == bt.VARIANT_V2_N3               # V1's cell is not viable
    assert tightened["shipped_mechanism_unchanged"] is True
    assert set(tightened["promotable_at_horizon"]) <= set(registered["promotable_at_horizon"])


def test_both_decision_outcomes_are_reported_side_by_side(study):
    """Every run states the registered outcome AND the tightened one, and says whether they differ —
    a tightening that silently replaced the registered outcome would be a post-hoc rule change."""
    doc, _trades = study
    t = doc["decision_under_tightened_reporting_rule"]
    assert doc["decision"]["rule"] == t["rule"] == bt.DECISION_RULE
    assert t["promotability_gate"] == "reported_promotable"
    assert t["amendment"] == bt.REPORTING_RULE_AMENDMENT
    assert isinstance(t["changes_the_registered_outcome"], bool)
    # nothing is promotable on this fixture, so both gates say 'neither' and nothing changes
    assert doc["decision"]["outcome"] == t["outcome"] == bt.DECISION_NEITHER
    assert t["changes_the_registered_outcome"] is False
    text = bt.render_text(doc)
    assert "DECISION-RULE OUTCOME UNDER THE TIGHTENED GATE:" in text
    assert "REPORTED PROMOTABLE" in text
    assert any("REPORTING RULE (2026-09-12 amendment" in n for n in doc["notes"])


def test_purged_kfold_fallback_honours_purge_and_embargo():
    """The self-implemented fallback keeps train observations clear of every test block."""
    splits = bt._purged_kfold_splits(120, n_folds=6, purge=5, embargo=5)
    assert len(splits) == 6
    for train, test in splits:
        lo, hi = int(test.min()), int(test.max())
        assert not set(train) & set(test)
        assert all(j < lo - 5 or j > hi + 5 for j in train)


# ============================================================ 6. the unadjusted-history veto
@pytest.mark.parametrize(
    ("kind", "books_trade", "vetoes"),
    [("bonus", False, 1), ("dividend", True, 0)],
)
def test_a_structural_ex_date_vetoes_the_signal(db_with_action, kind, books_trade, vetoes):
    """A bonus 20 days before WINNER's cross holds its 20-session window in two units; a dividend
    rescales nothing and must not suppress anything."""
    doc, trades = _run(db_with_action(kind))
    booked = {t.symbol for t in trades[bt.VARIANT_V1]}
    assert ("WINNER" in booked) is books_trade
    assert booked >= {"GAPPER", "LATEFILL", "NOFILL"}        # the veto is per symbol, never global
    assert doc["vetoes"][brk20.VETO_UNADJUSTED_HISTORY] == vetoes
    assert doc["meta"]["n_signals_after_vetoes"] == (4 if books_trade else 3)


def test_corp_actions_coverage_is_reported_with_the_veto_counts(db_file, db_with_action):
    """The veto reaches exactly as far as corp_actions does, so the report states the coverage."""
    doc_empty, _ = _run(db_file)
    assert doc_empty["corp_actions_coverage"] == {
        "rows": 0, "ex_date_min": None, "ex_date_max": None,
        "structural_rows": 0, "structural_ex_date_min": None, "structural_ex_date_max": None,
    }
    assert doc_empty["vetoes"][brk20.VETO_UNADJUSTED_HISTORY] == 0

    doc, _trades = _run(db_with_action("bonus"))
    assert doc["corp_actions_coverage"]["structural_rows"] == 1
    assert doc["corp_actions_coverage"]["structural_ex_date_min"] == str(ACTION_EX)
    # A dividend-only table is "backfilled" but gives the veto NO reach: the structural span is empty.
    doc_div, _ = _run(db_with_action("dividend"))
    assert doc_div["corp_actions_coverage"]["rows"] == 1
    assert doc_div["corp_actions_coverage"]["structural_rows"] == 0
    assert any("UNADJUSTED-HISTORY VETO" in n for n in doc["notes"])
    text = bt.render_text(doc)
    assert "unadjusted_history 1" in text
    assert "structural corp_actions rows=1" in text


# ============================================================ 7. the CLI: reports, refusals
def test_main_writes_json_and_md_and_orders_the_report(db_file, tmp_path, capsys):
    out = tmp_path / "results" / "brk20.json"
    rc = bt.main([
        "--db", str(db_file), "--out", str(out),
        "--start", str(SESSIONS[0]), "--end", str(SESSIONS[-1]), "--verify-window", "10",
    ])
    assert rc == 0
    assert out.exists() and out.with_suffix(".md").exists()
    doc = json.loads(out.read_text(encoding="utf-8"))
    assert doc["meta"]["parameter_sweep_run"] is False
    assert doc["meta"]["trial_count_n"] == 3
    assert doc["meta"]["product"] == "CNC"
    assert doc["meta"]["params_match_live_defaults"] is True
    assert doc["meta"]["floor_params_match_live_defaults"] is True
    assert "FILL" in doc["meta"]["horizon_anchor"]

    assert "matched_cohorts" in doc
    assert set(doc["matched_cohorts"]["variants"]) == {bt.VARIANT_V2_N3, bt.VARIANT_V2_N5}
    assert doc["decision_under_tightened_reporting_rule"]["outcome"] == bt.DECISION_NEITHER
    assert doc["variants"][bt.VARIANT_V1]["cpcv"]["10"]["reported_promotable"] is False

    text = capsys.readouterr().out
    assert text.isascii()                                   # Windows console is cp1252
    assert (text.index("STEP 1 - COST GEOMETRY")
            < text.index("STEP 2 - FILL RATE")
            < text.index("STEP 3 - RETURNS BY VARIANT")
            < text.index("STEP 3B - MATCHED-COHORT DECOMPOSITION")
            < text.index("STEP 4 - CPCV")
            < text.index("STEP 5 - THE PRE-REGISTERED DECISION RULE"))
    assert "DECISION-RULE OUTCOME: neither" in text
    assert str(out) in text and str(out.with_suffix(".md")) in text

    md = out.with_suffix(".md").read_text(encoding="utf-8")
    assert bt.DECISION_RULE in md
    assert "Outcome under the registered rule: `neither`" in md
    assert "Outcome under the 2026-09-12 tightened reporting gate: `neither`" in md
    assert "## Matched-cohort decomposition" in md
    assert f"`{bt.cohort_label(bt.VARIANT_V2_N5, bt.COHORT_UNFILLED)}`" in md
    for variant in bt.VARIANTS:
        assert f"`{variant}`" in md


def test_refuses_a_missing_database_file(tmp_path, capsys):
    rc = bt.main(["--db", str(tmp_path / "nope.duckdb"), "--out", str(tmp_path / "x.json")])
    assert rc == 2
    err = capsys.readouterr().err
    assert "REFUSING TO RUN" in err
    assert "no such database file" in err
    assert not (tmp_path / "x.json").exists()               # nothing written on a refusal


def test_refuses_a_locked_database_file(db_file, tmp_path, clock, capsys):
    """A running engine holds market.duckdb read-write; DuckDB then refuses even a read-only attach."""
    holder = MarketStore(db_file, tmp_path / "parquet2", clock).open()
    try:
        rc = bt.main(["--db", str(db_file), "--out", str(tmp_path / "y.json")])
    finally:
        holder.close()
    assert rc == 2
    err = capsys.readouterr().err
    assert "REFUSING TO RUN" in err
    assert "cannot open read-only" in err
    assert "mt-engine" in err
    assert not (tmp_path / "y.json").exists()


def test_refuses_an_empty_universe_rather_than_measuring_it(db_no_universe, tmp_path, capsys):
    """The eligible set IS the population here — an empty one is a refusal, not a negative result."""
    rc = bt.main(["--db", str(db_no_universe), "--out", str(tmp_path / "z.json")])
    assert rc == 2
    assert "no eligible universe" in capsys.readouterr().err
    assert not (tmp_path / "z.json").exists()
