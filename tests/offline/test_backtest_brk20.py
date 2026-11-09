"""scripts/backtest_brk20.py — the pre-registered brk20 entry-mechanism harness, on synthetic bars.

Six engineered symbols in a temp :class:`MarketStore`, each 47 sessions long so the only measurable
signal is at index 21 (the first day ``scan_daily`` can fire on with ``lookback_days`` 20, and the last
that can book a trade given the ``MAX_FILL_WINDOW + 20`` = 25 forward sessions the admission rule
reserves). Every symbol trades flat at 100.00 with highs pinned at 100.00 up to its trigger day, so
``H20`` and the rested limit are exactly 100.00; every forward high is 130.00, which rules out a
second breakout.

* ``WINNER``     — trigger close 104.00, then opens 103.00 and dips to 99.00: next-open fills at 103.00,
  a limit at the 100.00 level; the return sets differ by the mechanism alone.
* ``GAPPER``     — trigger close 102.00, then gaps through the level (open 95.00): the limit fills at the
  open, so both mechanisms get the same price.
* ``LATEFILL``   — trigger close 106.00, holds above the level for three sessions, touches it on y+4:
  unfilled under 3-session-limit, filled under 5-session-limit, and the next-open and 5-session-limit
  exits land on different sessions (the fill-anchored horizon).
* ``NOFILL``     — trigger close 108.00, never trades at or below the level: next-open fills, both limit
  variants book nothing, and the fill rate's denominator stays the shared admitted population.
* ``OUTSIDER``   — WINNER's bars, excluded ``not_in_index``: must never appear.
* ``CAPPEDPLUS`` — WINNER's bars, excluded ``['watchlist_cap', 'surveillance_asm']``: the strict
  single-reason eligibility equality must reject it.

Every price is exact in 2 dp (the ``bars_1d`` DECIMAL(12,2) column). Costs are asserted as gross minus
one CNC round trip from the repo's CostModel, not a frozen constant.
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
    """Sessions 0..20: flat at 100.00 with highs at 100.00, so H20 == 100.00 and every overnight gap is 0.00
    (the stop-geometry floor is then 0 and never decides whether a fixture signal fires)."""
    return [_flat(sym, i, "100.00", high="100.00") for i in range(SIGNAL_IDX)]


def _trigger(sym: str, close: str) -> DailyBar:
    """The breakout session: close above the band on 2x the window's mean volume (the rule needs 1.2x); high == close."""
    return _bar(sym, SIGNAL_IDX, o="100.00", h=close, lo="100.00", c=close, v=2000)


def winner_bars(sym: str = "WINNER") -> list[DailyBar]:
    """Trigger 104.00; entry session opens 103.00 and dips to 99.00 (a limit fills the level, next-open the
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

    3-session-limit closes unfilled; 5-session-limit fills at index 25, so the T+20 exits differ:
    index 42 for next-open's index-22 fill (close 102.00), index 45 for 5-session-limit's (close 115.00).
    """
    sym = "LATEFILL"
    bars = _prefix(sym) + [_trigger(sym, "106.00")]
    bars += [_bar(sym, i, o="105.00", h="130.00", lo="101.00", c="105.00") for i in range(22, 25)]
    bars.append(_bar(sym, 25, o="102.00", h="130.00", lo="99.00", c="102.00"))
    for i in range(26, N_SESSIONS):
        bars.append(_flat(sym, i, "115.00" if i == 45 else "102.00", high="130.00"))
    return bars


def nofill_bars() -> list[DailyBar]:
    """Trigger 108.00, then the tape never returns to the level: both limits expire unfilled, next-open fills at 107.00."""
    sym = "NOFILL"
    bars = _prefix(sym) + [_trigger(sym, "108.00")]
    bars += [_flat(sym, i, "107.00", high="130.00") for i in range(ENTRY_IDX, N_SESSIONS)]
    return bars


#: Structural ex-date 20 days before the decision day: inside the 35-day unadjusted-history window, outside the 10-day upcoming-ex skip.
ACTION_EX = SESSIONS[ENTRY_IDX] - timedelta(days=20)

#: ``universe_daily`` exclusion vectors. NOFILL: a cap-only row is eligible; CAPPEDPLUS: eligibility is
#: strict (cap plus anything else is out); OUTSIDER: the ordinary out-of-index name.
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
    """N=3 and the rule params are the live scanner's frozen defaults — no sweep, no drift."""
    assert bt.TRIAL_COUNT_N == 3
    assert bt.PRE_REGISTERED_PARAMS == dict(brk20.DEFAULT_PARAMS)
    assert bt.PRE_REGISTERED_FLOOR_PARAMS == dict(brk20.FLOOR_PARAMS)
    assert bt.PRE_REGISTERED_PARAMS["lookback_days"] == 20
    assert bt.PRE_REGISTERED_PARAMS["vol_mult"] == 1.2
    assert bt.VARIANTS == (bt.VARIANT_NEXT_OPEN, bt.VARIANT_LIMIT_3, bt.VARIANT_LIMIT_5)
    assert bt.FILL_WINDOW == {bt.VARIANT_NEXT_OPEN: None, bt.VARIANT_LIMIT_3: 3, bt.VARIANT_LIMIT_5: 5}
    assert bt.MAX_FILL_WINDOW == 5
    assert fold_pass_min(bt.TRIAL_COUNT_N) == 0.60
    # the CLI exposes no knob that could turn three trials into many
    flags = {a.option_strings[0] for a in bt.build_parser()._actions if a.option_strings}
    assert not (flags & {"--vol-mult", "--lookback", "--fill-window", "--horizon", "--grid-density"})


def test_unadjusted_window_is_the_live_brk20_sweep_window():
    """35 calendar days, not hi52's 400: hi52's window would veto symbols the live brk20 sweep scans."""
    assert bt.UNADJUSTED_LOOKBACK_DAYS == 35
    today = date(2025, 6, 30)
    span = bt.UNADJUSTED_LOOKBACK_DAYS
    assert bt._unadjusted_at([], today) is False
    assert bt._unadjusted_at([today], today) is False                       # today = the skip's case
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
    assert full > fees                     # the measured bid-ask spread is included
    assert full > mis                      # delivery, the dearer surface — not the intraday one
    assert 0.15 < full < 0.60              # sanity band around the documented ~0.32% CNC round trip


# ============================================================ 1. the signal set is scan_daily's
def test_windowed_scan_matches_the_full_prefix(db_file):
    """The bounded scan window offers scan_daily a bit-identical read to the full row prefix."""
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
    """The rested level is the candidate's ``raw_levels.entry``, the same number the tercile split's margin reads."""
    doc, trades = study
    assert doc["meta"]["n_signals_after_vetoes"] == len(ELIGIBLE)
    assert doc["meta"]["n_signals_admitted"] == len(ELIGIBLE)
    t = _by_symbol(trades, bt.VARIANT_LIMIT_5)["WINNER"]
    assert t.margin == pytest.approx(0.04, abs=1e-9)        # close(y) 104.00 / H20 100.00 - 1
    assert t.fill_px == pytest.approx(LEVEL)                # the level is what the rule shipped
    margins = {s: _by_symbol(trades, bt.VARIANT_NEXT_OPEN)[s].margin for s in ELIGIBLE}
    assert margins == pytest.approx(
        {"GAPPER": 0.02, "WINNER": 0.04, "LATEFILL": 0.06, "NOFILL": 0.08}, abs=1e-9
    )


# ============================================================ 2. hand-computed fills, net of costs
def test_next_open_and_limit_book_the_same_signal_at_different_prices(study):
    """WINNER: next-open pays the next open (103.00), the limit gets the level (100.00); same signal and exits."""
    _doc, trades = study
    cost = _cost_pct()
    next_open = _by_symbol(trades, bt.VARIANT_NEXT_OPEN)["WINNER"]
    limit = _by_symbol(trades, bt.VARIANT_LIMIT_5)["WINNER"]

    assert next_open.signal_date == limit.signal_date == SESSIONS[SIGNAL_IDX]
    assert next_open.fill_date == limit.fill_date == SESSIONS[ENTRY_IDX]
    assert next_open.fill_px == pytest.approx(103.00)          # the next session's OPEN
    assert next_open.fill_px != pytest.approx(104.00)          # NOT the signal close — no same-bar fill
    assert limit.fill_px == pytest.approx(LEVEL)           # the rested limit, not the 103.00 open
    assert next_open.fill_delay_sessions == limit.fill_delay_sessions == 1

    for horizon, exit_px in ((5, 105.0), (10, 110.0), (20, 120.0)):
        assert limit.gross[horizon] == pytest.approx((exit_px / 100.0 - 1.0) * 100.0, abs=1e-9)
        assert next_open.gross[horizon] == pytest.approx((exit_px / 103.0 - 1.0) * 100.0, abs=1e-9)
        assert limit.net[horizon] == pytest.approx(limit.gross[horizon] - cost, abs=1e-9)
        assert next_open.net[horizon] == pytest.approx(next_open.gross[horizon] - cost, abs=1e-9)
    # exactly ONE round trip is deducted, at every horizon — never one per session held
    assert next_open.gross[20] - next_open.net[20] == pytest.approx(next_open.gross[5] - next_open.net[5], abs=1e-12)


def test_a_gap_through_fills_at_the_open_not_at_the_level(study):
    """GAPPER opens at 95.00, below the 100.00 limit: ``min(open, level)`` books 95.00; 100.00 would invent a fill the tape never offered."""
    _doc, trades = study
    limit = _by_symbol(trades, bt.VARIANT_LIMIT_3)["GAPPER"]
    next_open = _by_symbol(trades, bt.VARIANT_NEXT_OPEN)["GAPPER"]
    assert limit.fill_px == pytest.approx(95.00)
    assert limit.fill_px != pytest.approx(LEVEL)
    assert next_open.fill_px == pytest.approx(95.00)     # a gap-through gives both mechanisms one price
    assert next_open.gross == limit.gross


def test_the_limit_window_boundary_is_n_sessions_exactly(study):
    """LATEFILL is touched on y+4: 3-session-limit's window (y+1..y+3) expires unfilled, 5-session-limit's fills."""
    _doc, trades = study
    assert "LATEFILL" not in _by_symbol(trades, bt.VARIANT_LIMIT_3)
    late = _by_symbol(trades, bt.VARIANT_LIMIT_5)["LATEFILL"]
    assert late.fill_delay_sessions == 4
    assert late.fill_date == SESSIONS[25]
    assert late.fill_px == pytest.approx(LEVEL)   # min(open 102.00, level 100.00)


def test_horizons_are_anchored_on_the_fill_not_the_signal(study):
    """LATEFILL's legs fill 3 sessions apart, so their T+20 exits are too: index 42 (close 102.00) and 45 (close 115.00)."""
    _doc, trades = study
    next_open = _by_symbol(trades, bt.VARIANT_NEXT_OPEN)["LATEFILL"]
    limit5 = _by_symbol(trades, bt.VARIANT_LIMIT_5)["LATEFILL"]
    assert (next_open.fill_idx, limit5.fill_idx) == (22, 25)
    assert next_open.gross[20] == pytest.approx((102.0 / 105.0 - 1.0) * 100.0, abs=1e-9)
    assert limit5.gross[20] == pytest.approx((115.0 / 100.0 - 1.0) * 100.0, abs=1e-9)


def test_an_untouched_level_books_no_trade_and_the_fill_rate_says_so(study):
    """NOFILL's limit is never reached: both limit variants book nothing; the fill-rate denominator is the shared admitted population."""
    doc, trades = study
    assert "NOFILL" in _by_symbol(trades, bt.VARIANT_NEXT_OPEN)
    for variant in (bt.VARIANT_LIMIT_3, bt.VARIANT_LIMIT_5):
        assert "NOFILL" not in _by_symbol(trades, variant)
    blocks = doc["variants"]
    assert blocks[bt.VARIANT_NEXT_OPEN]["fill_rate"] == 1.0
    assert blocks[bt.VARIANT_NEXT_OPEN]["n_filled"] == 4 and blocks[bt.VARIANT_NEXT_OPEN]["n_unfilled"] == 0
    assert blocks[bt.VARIANT_LIMIT_3]["n_filled"] == 2 and blocks[bt.VARIANT_LIMIT_3]["n_unfilled"] == 2
    assert blocks[bt.VARIANT_LIMIT_5]["n_filled"] == 3 and blocks[bt.VARIANT_LIMIT_5]["n_unfilled"] == 1
    for variant in bt.VARIANTS:
        b = blocks[variant]
        # The signal population is shared; the trade sets are not.
        assert b["n_signals_admitted"] == 4
        assert b["n_filled"] + b["n_unfilled"] == 4
        assert b["fill_rate"] == pytest.approx(b["n_filled"] / 4)


# ============================================================ 3. the population predicate
def test_population_is_the_eligible_universe_applied_backwards(study, db_file):
    """Eligible = ``included`` or cap-only; an out-of-index name and a cap-plus-something name are out."""
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
    """The cut is taken once over the admitted signals, so a signal sits in the same cell under every variant."""
    doc, trades = study
    cuts = doc["margin_tercile_cuts"]
    assert cuts is not None and cuts["cut_1"] < cuts["cut_2"]

    by_symbol = _by_symbol(trades, bt.VARIANT_NEXT_OPEN)
    assert by_symbol["GAPPER"].tercile == bt.CELL_MARGIN_LOW       # margin 0.02, the narrowest
    assert by_symbol["NOFILL"].tercile == bt.CELL_MARGIN_HIGH      # margin 0.08, the widest
    assert {by_symbol[s].tercile for s in ("WINNER", "LATEFILL")} == {bt.CELL_MARGIN_MID}
    for variant in (bt.VARIANT_LIMIT_3, bt.VARIANT_LIMIT_5):
        for sym, t in _by_symbol(trades, variant).items():
            assert t.tercile == by_symbol[sym].tercile

    for variant in bt.VARIANTS:
        cells = doc["variants"][variant]["cells"]
        total = sum(cells[name]["n_trades"] for name in bt.MARGIN_CELLS)
        assert total == cells[bt.CELL_ALL]["n_trades"]             # a partition, never a double count
    # The widest-margin signal is the one the rested limit never catches.
    assert doc["variants"][bt.VARIANT_LIMIT_5]["cells"][bt.CELL_MARGIN_HIGH]["n_trades"] == 0
    assert doc["variants"][bt.VARIANT_NEXT_OPEN]["cells"][bt.CELL_MARGIN_HIGH]["n_trades"] == 1
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
    """All three variants are offered the same admitted signals, but the limit variants book a different (nested) set of trades."""
    doc, trades = study
    admitted = {(t.symbol, t.signal_date) for t in trades[bt.VARIANT_NEXT_OPEN]}
    n3 = {(t.symbol, t.signal_date) for t in trades[bt.VARIANT_LIMIT_3]}
    n5 = {(t.symbol, t.signal_date) for t in trades[bt.VARIANT_LIMIT_5]}
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


def test_matched_cohorts_split_next_open_by_whether_the_limit_filled(study):
    """Next-open re-quoted on the cohort 5-session-limit filled and on the cohort it never filled.

    5-session-limit fills WINNER (100.00), GAPPER (gap-through 95.00) and LATEFILL (100.00 on y+4); NOFILL
    is never touched. At T+20 the next-open legs are WINNER 120/103 = +16.5049%, GAPPER 96/95 = +1.0526%,
    LATEFILL 102/105 = -2.8571%, NOFILL 0.0%.
    """
    doc, _trades = study
    blk = doc["matched_cohorts"]["variants"][bt.VARIANT_LIMIT_5]
    assert doc["matched_cohorts"]["descriptive_post_hoc_not_a_registered_trial"] is True
    assert blk["n_limit_filled"] == 3 and blk["n_limit_unfilled"] == 1
    assert blk["every_limit_fill_has_a_next_open_leg"] is True

    filled = blk["cells"][bt.cohort_label(bt.VARIANT_LIMIT_5, bt.COHORT_FILLED)]
    unfilled = blk["cells"][bt.cohort_label(bt.VARIANT_LIMIT_5, bt.COHORT_UNFILLED)]
    limit = blk["cells"][f"{bt.VARIANT_LIMIT_5}_{bt.COHORT_FILLED}"]
    # The two cohorts partition next-open's trades.
    assert filled["n_trades"] == 3 and unfilled["n_trades"] == 1 and limit["n_trades"] == 3
    assert filled["n_trades"] + unfilled["n_trades"] == doc["variants"][bt.VARIANT_NEXT_OPEN]["n_filled"]

    gapper20 = (96.0 / 95.0 - 1.0) * 100.0                       # the median of the filled cohort
    assert filled["horizons"]["20"]["median_gross"] == pytest.approx(gapper20, abs=1e-4)
    assert unfilled["horizons"]["20"]["median_gross"] == pytest.approx(0.0, abs=1e-9)   # NOFILL
    assert limit["horizons"]["20"]["median_gross"] == pytest.approx(15.0, abs=1e-4)        # LATEFILL
    cost = _cost_pct()
    assert filled["horizons"]["20"]["median_net"] == pytest.approx(gapper20 - cost, abs=1e-4)

    e = blk["effects"]["20"]
    # PRICE: limit minus next-open on the same events. SELECTION: next-open filled minus unfilled.
    assert e["price_effect_median_net_pp"] == pytest.approx(15.0 - gapper20, abs=1e-3)
    assert e["selection_effect_median_net_pp"] == pytest.approx(gapper20, abs=1e-3)
    assert e["pooled_gap_median_net_pp"] is not None
    assert "BOTH" in e["carried_by"]

    # 3-session-limit is a different, smaller cohort: LATEFILL is only touched on y+4, outside its window
    blk3 = doc["matched_cohorts"]["variants"][bt.VARIANT_LIMIT_3]
    assert blk3["n_limit_filled"] == 2 and blk3["n_limit_unfilled"] == 2
    f3 = blk3["cells"][bt.cohort_label(bt.VARIANT_LIMIT_3, bt.COHORT_FILLED)]["horizons"]["20"]
    u3 = blk3["cells"][bt.cohort_label(bt.VARIANT_LIMIT_3, bt.COHORT_UNFILLED)]["horizons"]["20"]
    assert f3["n"] == 2 and u3["n"] == 2
    # next-open on the UNFILLED cohort is LATEFILL (-2.8571%) and NOFILL (0.0%) -> median -1.4286%
    assert u3["median_gross"] == pytest.approx(-(100.0 * 3.0 / 105.0) / 2.0, abs=1e-4)


def test_carried_by_names_the_effect_and_never_guesses():
    """The four sign combinations; an n=0 cell is 'unknown', never a verdict."""
    assert "PRICE" in bt._carried_by(2.0, -1.0)
    assert "runs AGAINST" in bt._carried_by(2.0, -1.0)
    assert "SELECTION" in bt._carried_by(-1.0, 2.0)
    assert bt._carried_by(2.0, 1.0).startswith("BOTH")
    assert bt._carried_by(-2.0, -1.0).startswith("NEITHER")
    assert bt._carried_by(None, 1.0).startswith("unknown")
    assert bt._carried_by(1.0, None).startswith("unknown")


def test_every_cell_carries_its_own_fill_rate_and_the_tercile_claim_is_matched(study):
    """The 'in every margin tercile' claim reads off matched cells with n per cell.

    The widest tercile is NOFILL alone, which the limit never catches, so the claim is NOT TESTABLE, never 'true'.
    """
    doc, _trades = study
    cells = doc["variants"][bt.VARIANT_LIMIT_5]["cells"]
    assert cells[bt.CELL_ALL]["n_signals_in_cell"] == 4
    assert cells[bt.CELL_ALL]["fill_rate_in_cell"] == pytest.approx(0.75)
    assert cells[bt.CELL_MARGIN_HIGH]["n_signals_in_cell"] == 1          # NOFILL
    assert cells[bt.CELL_MARGIN_HIGH]["n_trades"] == 0
    assert cells[bt.CELL_MARGIN_HIGH]["fill_rate_in_cell"] == 0.0
    assert cells[bt.CELL_MARGIN_HIGH]["n_unfilled_in_cell"] == 1
    assert cells[bt.CELL_MARGIN_LOW]["fill_rate_in_cell"] == 1.0        # GAPPER, gapped through
    # next-open fills everything, so its per-cell fill rate is 1.0 in every cell
    for name in (bt.CELL_ALL, *bt.MARGIN_CELLS):
        assert doc["variants"][bt.VARIANT_NEXT_OPEN]["cells"][name]["fill_rate_in_cell"] == 1.0

    ter = doc["matched_cohorts"]["variants"][bt.VARIANT_LIMIT_5]["by_margin_tercile"]
    assert ter[bt.CELL_MARGIN_HIGH]["n_signals"] == 1
    assert ter[bt.CELL_MARGIN_HIGH]["n_limit_filled"] == 0
    assert ter[bt.CELL_MARGIN_HIGH]["n_limit_unfilled"] == 1
    assert ter[bt.CELL_MARGIN_HIGH]["fill_rate"] == 0.0
    assert ter[bt.CELL_MARGIN_HIGH]["price_effect_median_net_pp"]["20"] is None
    assert ter[bt.CELL_MARGIN_LOW]["fill_rate"] == 1.0
    holds = doc["matched_cohorts"]["variants"][bt.VARIANT_LIMIT_5][
        "limit_beats_matched_next_open_in_every_margin_tercile"]
    assert holds == {"5": None, "10": None, "20": None}                 # NOT TESTABLE, not True
    text = bt.render_text(doc)
    assert "NOT TESTABLE" in text
    assert "STEP 3B - MATCHED-COHORT DECOMPOSITION" in text


# ============================================================ 5. geometry, CPCV, the decision rule
def test_geometry_is_the_median_gross_of_the_variants_own_fills(study):
    """Geometry is per variant over its own fills; unfilled signals are absent, not zero-filled."""
    doc, _trades = study
    cost = _cost_pct()
    limit = doc["geometry"][bt.VARIANT_LIMIT_5]["horizons"]["20"]
    assert doc["geometry"][bt.VARIANT_LIMIT_5]["cost_floor_pct"] == pytest.approx(cost, abs=1e-6)
    # 5-session-limit books WINNER (+20%), GAPPER (96/95 - 1) and LATEFILL (+15%) -> median +15%.
    assert limit["n"] == 3
    assert limit["median_gross_pct"] == pytest.approx(15.0, abs=1e-4)
    assert limit["verdict"] == "viable"
    # next-open books all four: -2.857% (LATEFILL), 0.000% (NOFILL), +1.053% (GAPPER), +16.505% (WINNER).
    next_open = doc["geometry"][bt.VARIANT_NEXT_OPEN]["horizons"]["20"]
    assert next_open["n"] == 4
    assert next_open["median_gross_pct"] == pytest.approx(50.0 / 95.0, abs=1e-4)
    assert next_open["margin_pct"] == pytest.approx(50.0 / 95.0 - cost, abs=1e-4)


def test_geometry_verdict_is_strict_against_the_cost_floor():
    """`viable` iff the median gross clears one full round trip; equality is dead (it earns nothing)."""
    def _t(gross: float) -> bt.Trade:
        return bt.Trade(
            symbol="X", variant=bt.VARIANT_NEXT_OPEN, signal_date=SESSIONS[0], fill_date=SESSIONS[1],
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
    """Four signals on one day cannot be cross-validated: no folds, not promotable, and the report says so."""
    doc, _trades = study
    for variant in bt.VARIANTS:
        cpcv = doc["variants"][variant]["cpcv"]["20"]
        assert cpcv["n_splits"] == 0
        assert cpcv["trial_count_n"] == 3
        assert cpcv["fold_pass_min"] == fold_pass_min(3)
        assert cpcv["promotable"] is False
        assert any("CPCV produced no folds" in r for r in cpcv["reasons"])
        assert cpcv["margin_floor_pct_per_day"] == pytest.approx(_cost_pct() / 20.0, abs=1e-9)
    # The CPCV series is keyed on the fill day: next-open fills on one day, 5-session-limit on two.
    assert doc["variants"][bt.VARIANT_NEXT_OPEN]["cpcv"]["20"]["n_obs_days"] == 1
    assert doc["variants"][bt.VARIANT_LIMIT_5]["cpcv"]["20"]["n_obs_days"] == 2


def test_decision_rule_is_neither_when_nothing_is_promotable(study):
    doc, _trades = study
    d = doc["decision"]
    assert d["rule"] == bt.DECISION_RULE
    assert d["horizon_sessions"] == 10
    assert d["promotable_at_horizon"] == []
    assert d["outcome"] == bt.DECISION_NEITHER
    assert d["shipped_mechanism_unchanged"] is True


def _decision_doc(entries: dict[str, tuple]) -> dict:
    """A minimal document shaped as :func:`decision_outcome` reads it.

    Entries are ``(cpcv_promotable, median_net[, reported_promotable])``; the third defaults to the first.
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
    """A higher median that is not CPCV-promotable never wins."""
    doc = _decision_doc({
        bt.VARIANT_NEXT_OPEN: (False, 9.0),       # best median, not promotable -> ineligible
        bt.VARIANT_LIMIT_3: (True, 1.0),
        bt.VARIANT_LIMIT_5: (True, 2.0),
    })
    d = bt.decision_outcome(doc)
    assert d["promotable_at_horizon"] == [bt.VARIANT_LIMIT_5, bt.VARIANT_LIMIT_3]
    assert d["outcome"] == d["winner"] == bt.VARIANT_LIMIT_5
    assert d["shipped_mechanism_unchanged"] is True

    won_by_next_open = bt.decision_outcome(_decision_doc({
        bt.VARIANT_NEXT_OPEN: (True, 3.0), bt.VARIANT_LIMIT_3: (True, 1.0), bt.VARIANT_LIMIT_5: (False, 9.0),
    }))
    assert won_by_next_open["winner"] == bt.VARIANT_NEXT_OPEN
    assert won_by_next_open["shipped_mechanism_unchanged"] is False

    tie = bt.decision_outcome(_decision_doc({
        bt.VARIANT_NEXT_OPEN: (True, 2.0), bt.VARIANT_LIMIT_3: (True, 2.0), bt.VARIANT_LIMIT_5: (False, None),
    }))
    assert tie["outcome"] == bt.DECISION_TIE and tie["winner"] is None

    # a promotable variant with no median (n=0 at the horizon) is not a winner by default
    none_med = bt.decision_outcome(_decision_doc({
        bt.VARIANT_NEXT_OPEN: (True, None), bt.VARIANT_LIMIT_3: (False, None), bt.VARIANT_LIMIT_5: (False, None),
    }))
    assert none_med["outcome"] == bt.DECISION_NEITHER


def test_reported_promotable_requires_cpcv_AND_viable_geometry():
    """Reported promotability is CPCV AND viable geometry: the CPCV series is a per-fill-day mean and the
    geometry verdict a median, so a tail-driven cell can pass CPCV while its typical trade pays the round trip for nothing.
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

    # A median net of exactly zero is not viable.
    zero = bt.apply_reporting_rule({"promotable": True, "reasons": []}, {"verdict": "dead"}, 0.0)
    assert zero["reported_promotable"] is False
    # An n=0 cell has no median and is never reported promotable.
    none_med = bt.apply_reporting_rule(
        {"promotable": True, "reasons": []}, {"verdict": "unknown"}, None
    )
    assert none_med["reported_promotable"] is False
    # The rule can only withhold: a CPCV refusal is never rescued by good geometry.
    refused = bt.apply_reporting_rule({"promotable": False, "reasons": []}, {"verdict": "viable"}, 9.0)
    assert refused["reported_promotable"] is False and refused["gates_disagree"] is True


def test_the_tightened_gate_can_only_withhold_never_promote():
    """The tightened gate can change the winner only by removing a candidate, never by adding one CPCV refused."""
    doc = _decision_doc({
        bt.VARIANT_NEXT_OPEN: (True, 3.0, False),       # CPCV passes, geometry dead -> withheld
        bt.VARIANT_LIMIT_3: (True, 1.0, True),
        bt.VARIANT_LIMIT_5: (False, 9.0, False),   # CPCV refuses; the amendment cannot rescue it
    })
    registered = bt.decision_outcome(doc)
    tightened = bt.decision_outcome(doc, gate_key="reported_promotable")
    assert registered["promotability_gate"] == "promotable"
    assert registered["winner"] == bt.VARIANT_NEXT_OPEN                 # the registered rule, verbatim
    assert registered["shipped_mechanism_unchanged"] is False
    assert tightened["promotability_gate"] == "reported_promotable"
    assert tightened["winner"] == bt.VARIANT_LIMIT_3               # next-open's cell is not viable
    assert tightened["shipped_mechanism_unchanged"] is True
    assert set(tightened["promotable_at_horizon"]) <= set(registered["promotable_at_horizon"])


def test_both_decision_outcomes_are_reported_side_by_side(study):
    """Every run states the registered outcome and the tightened one, and whether they differ."""
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


# ============================================================ 6. the unadjusted-history veto
@pytest.mark.parametrize(
    ("kind", "books_trade", "vetoes"),
    [("bonus", False, 1), ("dividend", True, 0)],
)
def test_a_structural_ex_date_vetoes_the_signal(db_with_action, kind, books_trade, vetoes):
    """A bonus 20 days before WINNER's cross splits its 20-session window into two units; a dividend rescales nothing."""
    doc, trades = _run(db_with_action(kind))
    booked = {t.symbol for t in trades[bt.VARIANT_NEXT_OPEN]}
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
    assert set(doc["matched_cohorts"]["variants"]) == {bt.VARIANT_LIMIT_3, bt.VARIANT_LIMIT_5}
    assert doc["decision_under_tightened_reporting_rule"]["outcome"] == bt.DECISION_NEITHER
    assert doc["variants"][bt.VARIANT_NEXT_OPEN]["cpcv"]["10"]["reported_promotable"] is False

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
    assert f"`{bt.cohort_label(bt.VARIANT_LIMIT_5, bt.COHORT_UNFILLED)}`" in md
    for variant in bt.VARIANTS:
        assert f"`{variant}`" in md


def test_refuses_an_empty_universe_rather_than_measuring_it(db_no_universe, tmp_path, capsys):
    """The eligible set is the population, so an empty one is a refusal, not a negative result."""
    rc = bt.main(["--db", str(db_no_universe), "--out", str(tmp_path / "z.json")])
    assert rc == 2
    assert "no eligible universe" in capsys.readouterr().err
    assert not (tmp_path / "z.json").exists()
