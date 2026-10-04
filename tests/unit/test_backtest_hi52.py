"""scripts/backtest_hi52.py — the pre-registered hi52 backtest harness, on synthetic bars.

Three engineered symbols in a temp :class:`MarketStore`, each 146 sessions long so the ONLY
measurable signal in every one of them is at index 125 (``min_sessions`` = 126 means index 125 is
the first eligible day, and ``measure`` drops any signal without a full T+20 of forward bars, so
indices 126+ can never book a trade):

* ``WINNER``  — a clean fresh cross whose forward path is hand-chosen so the gross returns are
  exactly +5% / +10% / +20% at T+5 / T+10 / T+20 off a 100.00 entry.
* ``GAPPER``  — the same shape but the trigger day moves +6.67%, above the 5% news-gap threshold.
* ``SPIKER``  — the lookahead tripwire: the trigger day EXPLODES +10% into its close and the very
  next session opens back at the pre-spike price and stays there. Entry at the signal CLOSE would
  book -9.09%; entry at the next OPEN books 0.00%. The test asserts the latter.

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

_SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "backtest_hi52.py"
_spec = importlib.util.spec_from_file_location("mt_backtest_hi52", _SCRIPT)
bt = importlib.util.module_from_spec(_spec)
sys.modules["mt_backtest_hi52"] = bt
_spec.loader.exec_module(bt)

from engine.learning.validate import fold_pass_min  # noqa: E402
from engine.marketdata.store import DailyBar, MarketStore  # noqa: E402
from engine.strategy.cost_model import CostModel  # noqa: E402
from engine.strategy.scanners import hi52  # noqa: E402

N_SESSIONS = 146
SIGNAL_IDX = 125                    # min_sessions (126) - 1: the first day scan_daily can fire on
ENTRY_IDX = SIGNAL_IDX + 1
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


def _bar(sym: str, d: date, *, o: str, h: str, lo: str, c: str, v: int) -> DailyBar:
    return DailyBar(
        symbol=sym, d=d, open=Decimal(o), high=Decimal(h), low=Decimal(lo), close=Decimal(c),
        volume=v, src="bhavcopy",
    )


def _flat(sym: str, idx: int, price: str, *, high: str, v: int = 1000) -> DailyBar:
    return _bar(sym, SESSIONS[idx], o=price, h=high, lo=price, c=price, v=v)


def winner_bars() -> list[DailyBar]:
    """prox 0.94 -> 0.96 fresh cross; entry open 100.00; closes 105 / 110 / 120 at T+5 / 10 / 20."""
    sym = "WINNER"
    bars = [_flat(sym, i, "94.00", high="100.00") for i in range(SIGNAL_IDX)]
    # trigger day: close 96.00 (prox 0.96), volume 1.5x the 20-session mean, +2.13% (NOT a gap day)
    bars.append(_bar(sym, SESSIONS[SIGNAL_IDX], o="95.00", h="100.00", lo="94.00", c="96.00", v=1500))
    for i in range(ENTRY_IDX, N_SESSIONS):
        if i < SIGNAL_IDX + 5:
            bars.append(_flat(sym, i, "100.00", high="100.00"))       # entry session opens at 100.00
        elif i < SIGNAL_IDX + 10:
            bars.append(_flat(sym, i, "105.00", high="105.00"))       # T+5 close = 105.00
        elif i < SIGNAL_IDX + 20:
            bars.append(_flat(sym, i, "110.00", high="110.00"))       # T+10 close = 110.00
        else:
            bars.append(_flat(sym, i, "120.00", high="120.00"))       # T+20 close = 120.00
    return bars


def gapper_bars() -> list[DailyBar]:
    """Identical shape, but the trigger day moves 90.00 -> 96.00 = +6.67% (> the 5% gap threshold)."""
    sym = "GAPPER"
    bars = [_flat(sym, i, "90.00", high="100.00") for i in range(SIGNAL_IDX)]
    bars.append(_bar(sym, SESSIONS[SIGNAL_IDX], o="91.00", h="100.00", lo="90.00", c="96.00", v=1500))
    bars += [_flat(sym, i, "96.00", high="100.00") for i in range(ENTRY_IDX, N_SESSIONS)]
    return bars


def spiker_bars() -> list[DailyBar]:
    """Lookahead tripwire: +10% into the trigger close, then straight back to 90.00 from the open."""
    sym = "SPIKER"
    bars = [_flat(sym, i, "90.00", high="100.00") for i in range(SIGNAL_IDX)]
    bars.append(_bar(sym, SESSIONS[SIGNAL_IDX], o="90.00", h="100.00", lo="90.00", c="99.00", v=1500))
    bars += [_flat(sym, i, "90.00", high="90.00") for i in range(ENTRY_IDX, N_SESSIONS)]
    return bars


#: WINNER's ex-date: 30 calendar days before its decision day, i.e. inside the veto's trailing
#: window but far outside the 10-day UPCOMING-ex skip, so the two vetoes cannot be confused.
ACTION_EX = SESSIONS[ENTRY_IDX] - timedelta(days=30)


def _corp_action(sym: str, ex: date, kind: str, clock) -> dict:
    """One ``corp_actions`` row shaped as ``CorpActionsJob.run`` stamps them."""
    return {
        "symbol": sym, "ex_date": ex, "kind": kind, "ratio": None, "amount": None,
        "source": "test", "recorded_at": clock.now(),
    }


def _seed(path: Path, parquet: Path, clock, corp_actions: list[dict]) -> Path:
    """A temp market.duckdb seeded with the three symbols, CLOSED so the study can attach read-only."""
    store = MarketStore(path, parquet, clock).open()
    try:
        store.upsert_bars_1d(winner_bars() + gapper_bars() + spiker_bars())
        if corp_actions:
            store.upsert_corp_actions(corp_actions)
    finally:
        store.close()
    return path


@pytest.fixture
def db_file(tmp_path, clock) -> Path:
    return _seed(tmp_path / "market.duckdb", tmp_path / "parquet", clock, [])


@pytest.fixture
def db_with_action(tmp_path, clock):
    """``kind -> `` a temp DB whose WINNER carries that corp action on :data:`ACTION_EX`."""
    def _make(kind: str) -> Path:
        return _seed(
            tmp_path / f"market_{kind}.duckdb", tmp_path / f"parquet_{kind}", clock,
            [_corp_action("WINNER", ACTION_EX, kind, clock)],
        )
    return _make


@pytest.fixture
def index_csv(tmp_path) -> Path:
    """CURRENT-membership proxy: WINNER is 'in the index', GAPPER/SPIKER are extended names."""
    p = tmp_path / "nifty200.csv"
    p.write_text(
        "Company Name,Industry,Symbol,Series,ISIN Code\nWinner Ltd,Misc,WINNER,EQ,INE000000001\n",
        encoding="utf-8",
    )
    return p


def _run(db: Path, index_csv: Path):
    conn = bt.open_readonly(db)
    try:
        return bt.run_study(
            conn,
            start=SESSIONS[0],
            end=SESSIONS[-1],
            cost_model=CostModel.from_config(),
            nifty200_csv=index_csv,
            db_path=db,
            verify_prefilter=10,          # brute-force every eligible day for all three symbols
        )
    finally:
        conn.close()


@pytest.fixture
def study(db_file, index_csv):
    return _run(db_file, index_csv)


def _by_symbol(trades) -> dict:
    return {t.symbol: t for t in trades[bt.CONSTRUCT_DISCRETE]}


def _cost_pct() -> float:
    return float(CostModel.from_config().breakeven_pct(bt.REFERENCE_NOTIONAL, "CNC"))


# ============================================================ 0. pre-registration discipline
def test_one_pre_registered_param_set_no_sweep():
    """N=1 and the params ARE the live rule's frozen v1 defaults — no sweep, no drift.

    ``V1_PARAMS``, not ``DEFAULT_PARAMS``, since the 2026-09-12 promotion made the three v2 filter
    thresholds GATING in the live rule: the v1 registration must keep scanning the rule as
    registered, and ``V1_PARAMS`` is exactly the live defaults with those three neutralized. Every
    v1 parameter still tracks the live rule, which is the drift this assertion exists to catch."""
    assert bt.TRIAL_COUNT_N == 1
    assert bt.PRE_REGISTERED_PARAMS == dict(hi52.V1_PARAMS)
    assert {k: v for k, v in bt.PRE_REGISTERED_PARAMS.items() if k not in hi52.V2_FILTER_PARAMS} == \
        {k: v for k, v in hi52.DEFAULT_PARAMS.items() if k not in hi52.V2_FILTER_PARAMS}
    # …and the v2 constants registered here are the ones the LIVE rule now gates on.
    assert bt.V2_SMOOTH_UP_FRAC_MIN == hi52.DEFAULT_PARAMS["smooth_up_day_frac_min"]
    assert bt.V2_SMOOTH_MAX_DAY_MOVE_MAX == hi52.DEFAULT_PARAMS["smooth_max_day_move"]
    assert bt.V2_GAP_MAX == hi52.DEFAULT_PARAMS["gap_day_max"]
    assert bt.PRE_REGISTERED_PARAMS["proximity_min"] == 0.95
    assert bt.PRE_REGISTERED_PARAMS["lookback_sessions"] == 252
    assert bt.PRE_REGISTERED_PARAMS["vol_mult"] == 1.0
    # the CLI exposes no knob that could turn one trial into many
    flags = {a.option_strings[0] for a in bt.build_parser()._actions if a.option_strings}
    assert not (flags & {"--proximity-min", "--lookback", "--vol-mult", "--grid-density"})


def test_cost_path_is_cnc_delivery_and_spread_inclusive():
    cm = CostModel.from_config()
    full = float(cm.breakeven_pct(bt.REFERENCE_NOTIONAL, "CNC"))
    fees = float(cm.fee_breakeven_pct(bt.REFERENCE_NOTIONAL, "CNC"))
    mis = float(cm.breakeven_pct(bt.REFERENCE_NOTIONAL, "MIS"))
    assert bt.PRODUCT == "CNC"
    assert full > fees                     # the measured bid-ask spread is included (WO-2)
    assert full > mis                      # delivery, the dearer surface — not the intraday one
    assert 0.15 < full < 0.60              # sanity band around the documented ~0.32% CNC round trip


# ============================================================ 1. hand-computed winner, net of costs
def test_winner_net_returns_match_hand_computed_values(study):
    doc, trades = study
    t = _by_symbol(trades)["WINNER"]
    cost = _cost_pct()

    assert t.signal_date == SESSIONS[SIGNAL_IDX]
    assert t.entry_date == SESSIONS[ENTRY_IDX]
    assert t.entry_px == pytest.approx(100.00)         # NEXT session's open, not the 96.00 close
    assert t.prox == pytest.approx(0.96)
    assert not t.gap_day
    assert t.in_index_proxy

    for horizon, gross in ((5, 5.0), (10, 10.0), (20, 20.0)):
        assert t.gross[horizon] == pytest.approx(gross, abs=1e-9)
        assert t.net[horizon] == pytest.approx(gross - cost, abs=1e-9)
    # exactly ONE round trip is deducted, at every horizon — never one per session held
    assert t.gross[20] - t.net[20] == pytest.approx(t.gross[5] - t.net[5], abs=1e-12)

    stats = doc["constructs"][bt.CONSTRUCT_DISCRETE]["cells"][bt.CELL_INDEX]["horizons"]["20"]
    assert stats["n"] == 1
    assert stats["mean_net"] == pytest.approx(round(20.0 - cost, 4), abs=5e-4)


# ============================================================ 2. the gap-day A/B split
def test_gap_day_signal_lands_in_the_excluded_by_gap_cell(study):
    doc, trades = study
    cells = bt.split_cells(trades[bt.CONSTRUCT_DISCRETE])
    gap_only = {t.symbol for t in cells[bt.CELL_GAP_ONLY]}
    no_gap = {t.symbol for t in cells[bt.CELL_NO_GAP]}

    assert "GAPPER" in gap_only                        # +6.67% trigger day > the 5% threshold
    assert "GAPPER" not in no_gap                      # and therefore excluded from cell B
    assert "WINNER" in no_gap and "WINNER" not in gap_only   # +2.13% trigger day survives the filter
    assert gap_only | no_gap == {t.symbol for t in cells[bt.CELL_ALL]}
    assert not (gap_only & no_gap)                     # A/B is a partition, never a double count

    block = doc["constructs"][bt.CONSTRUCT_DISCRETE]["cells"]
    assert block[bt.CELL_ALL]["n_trades"] == 3
    assert block[bt.CELL_GAP_ONLY]["n_trades"] == len(gap_only)
    assert block[bt.CELL_NO_GAP]["n_trades"] == len(no_gap)
    assert block[bt.CELL_GAP_ONLY]["n_trades"] + block[bt.CELL_NO_GAP]["n_trades"] == 3


# ============================================================ 3. lookahead tripwire
def test_entry_is_the_next_open_not_the_signal_close(study):
    """SPIKER closes the trigger day at 99.00 and reopens at 90.00, never recovering.

    Entering at the signal close would book close(T+k)/99.00 - 1 = -9.09% at every horizon; entering
    at the next open books exactly 0.00%. Anything other than 0.00% here means a same-bar fill has
    crept back in.
    """
    _doc, trades = study
    t = _by_symbol(trades)["SPIKER"]
    cost = _cost_pct()

    assert t.entry_px == pytest.approx(90.00)
    close_entry_return = (90.0 / 99.0 - 1.0) * 100.0             # what a same-bar fill would book
    for horizon in bt.HORIZONS:
        assert t.gross[horizon] == pytest.approx(0.0, abs=1e-9)
        assert t.gross[horizon] != pytest.approx(close_entry_return, abs=1e-3)
        assert t.net[horizon] == pytest.approx(-cost, abs=1e-9)  # pays the round trip, earns nothing


# ============================================================ 4. splits, geometry, CPCV plumbing
def test_prefilter_agrees_with_an_exhaustive_scan_daily(db_file):
    """The vectorized narrowing offers scan_daily exactly the days a brute-force scan would."""
    conn = bt.open_readonly(db_file)
    try:
        series = bt.load_series(conn, SESSIONS[0], SESSIONS[-1])
    finally:
        conn.close()
    assert set(series) == {"WINNER", "GAPPER", "SPIKER"}
    for sym, s in series.items():
        fast = bt.discrete_signals(s, bt.PRE_REGISTERED_PARAMS, [])
        slow = bt.discrete_signals(s, bt.PRE_REGISTERED_PARAMS, [], exhaustive=True)
        assert [i for i, _ in fast] == [i for i, _ in slow] == [SIGNAL_IDX], sym
        # the unadjusted-history veto must land on BOTH paths, or --verify-prefilter would fire on it
        counts: dict[str, int] = {}
        vetoed_fast = bt.discrete_signals(
            s, bt.PRE_REGISTERED_PARAMS, [], [ACTION_EX], veto_counts=counts
        )
        vetoed_slow = bt.discrete_signals(
            s, bt.PRE_REGISTERED_PARAMS, [], [ACTION_EX], exhaustive=True
        )
        assert vetoed_fast == vetoed_slow == [], sym
        assert counts == {hi52.VETO_UNADJUSTED_HISTORY: 1}, sym


def test_index_split_is_labelled_a_survivorship_tainted_proxy(study):
    doc, trades = study
    cells = bt.split_cells(trades[bt.CONSTRUCT_DISCRETE])
    assert {t.symbol for t in cells[bt.CELL_INDEX]} == {"WINNER"}
    assert {t.symbol for t in cells[bt.CELL_EXTENDED]} == {"GAPPER", "SPIKER"}
    assert doc["meta"]["index_split_is_survivorship_tainted_proxy"] is True
    assert any("SURVIVORSHIP-TAINTED PROXY" in n for n in doc["notes"])


def test_smooth_jumpy_cut_is_the_documented_two_sided_and(study):
    doc, trades = study
    cells = bt.split_cells(trades[bt.CONSTRUCT_DISCRETE])
    cut = doc["constructs"][bt.CONSTRUCT_DISCRETE]["smoothness_cut"]
    # every trade climbed on exactly one of the last 20 pairs -> the up-day median is 0.05; the
    # max-day-move medians are 0.0213 / 0.0667 / 0.1000, so the median is GAPPER's 0.0667.
    assert cut["up_day_frac_median"] == pytest.approx(0.05)
    assert cut["max_day_move_median"] == pytest.approx(0.0667, abs=1e-4)
    smooth = {t.symbol for t in cells[bt.CELL_SMOOTH]}
    jumpy = {t.symbol for t in cells[bt.CELL_JUMPY]}
    assert "SPIKER" in jumpy                       # max_day_move 0.10 > the 0.0667 median
    assert "WINNER" in smooth                      # 0.0213 <= median and up_day_frac == median
    assert smooth | jumpy == {"WINNER", "GAPPER", "SPIKER"} and not (smooth & jumpy)


def test_geometry_verdict_compares_median_gross_with_the_cost_floor(study):
    doc, _trades = study
    geo = doc["geometry"][bt.CONSTRUCT_DISCRETE]
    cost = _cost_pct()
    assert geo["cost_floor_pct"] == pytest.approx(cost, abs=1e-6)
    # medians across {WINNER, GAPPER, SPIKER} = {20.0, 0.0, 0.0} -> 0.0 at every horizon: DEAD.
    for k in ("5", "10", "20"):
        assert geo["horizons"][k]["n"] == 3
        assert geo["horizons"][k]["median_gross_pct"] == pytest.approx(0.0, abs=1e-9)
        assert geo["horizons"][k]["verdict"] == "dead"


def test_cpcv_degrades_honestly_on_a_tiny_population(study):
    """One signal day cannot be cross-validated: no folds, not promotable, and it SAYS so."""
    doc, _trades = study
    cpcv = doc["constructs"][bt.CONSTRUCT_DISCRETE]["cpcv"]["20"]
    assert cpcv["n_obs_days"] == 1
    assert cpcv["n_splits"] == 0
    assert cpcv["trial_count_n"] == 1
    assert cpcv["fold_pass_min"] == 0.60                 # fold_pass_min(N=1)
    assert cpcv["promotable"] is False
    assert any("CPCV produced no folds" in r for r in cpcv["reasons"])
    assert cpcv["margin_floor_pct_per_day"] == pytest.approx(_cost_pct() / 20.0, abs=1e-9)


def test_purged_kfold_fallback_honours_purge_and_embargo():
    """The self-implemented fallback keeps train observations clear of every test block."""
    splits = bt._purged_kfold_splits(120, n_folds=6, purge=5, embargo=5)
    assert len(splits) == 6
    for train, test in splits:
        lo, hi = int(test.min()), int(test.max())
        assert not set(train) & set(test)
        assert all(j < lo - 5 or j > hi + 5 for j in train)


# ============================================================ 5. the CLI: report, JSON, refusal
def test_main_writes_json_and_prints_geometry_before_signal_quality(db_file, index_csv, tmp_path, capsys):
    out = tmp_path / "results" / "hi52.json"
    rc = bt.main([
        "--db", str(db_file), "--out", str(out),
        "--start", str(SESSIONS[0]), "--end", str(SESSIONS[-1]),
        "--nifty200-csv", str(index_csv),
    ])
    assert rc == 0
    assert out.exists()
    doc = json.loads(out.read_text(encoding="utf-8"))
    assert doc["meta"]["parameter_sweep_run"] is False
    assert doc["meta"]["trial_count_n"] == 1
    assert doc["meta"]["product"] == "CNC"
    assert "next session's OPEN" in doc["meta"]["entry_convention"]

    text = capsys.readouterr().out
    assert text.isascii()                                   # Windows console is cp1252
    assert text.index("STEP 1 - COST GEOMETRY") < text.index("STEP 2 - SIGNAL QUALITY")
    assert "GEOMETRY: dead at T+20" in text
    assert str(out) in text


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


# ============================================================ 6. the unadjusted-history veto
def test_unadjusted_at_window_boundaries():
    """The trailing window is [today - 400, today - 1] INCLUSIVE; today itself belongs to the
    upcoming-ex skip (every bar in the window is still pre-ex, one unit)."""
    today = date(2025, 6, 30)
    span = bt.UNADJUSTED_LOOKBACK_DAYS
    assert bt._unadjusted_at([], today) is False
    assert bt._unadjusted_at([today], today) is False
    assert bt._unadjusted_at([today - timedelta(days=1)], today) is True
    assert bt._unadjusted_at([today - timedelta(days=span)], today) is True
    assert bt._unadjusted_at([today - timedelta(days=span + 1)], today) is False
    assert bt._unadjusted_at([today + timedelta(days=1)], today) is False
    # The rank construct's window ends ON the rebalance day, so it calls with d + 1: an ex-date on
    # the rebalance day itself is then in range (owned by this veto, not by the upcoming-ex skip).
    assert bt._unadjusted_at([today], today + timedelta(days=1)) is True


@pytest.mark.parametrize(
    ("kind", "books_trade", "vetoes"),
    [("bonus", False, 1), ("dividend", True, 0)],
)
def test_a_structural_ex_date_vetoes_the_discrete_signal(
    db_with_action, index_csv, kind, books_trade, vetoes
):
    """A bonus 30 days before WINNER's cross holds its window in two units; a dividend does not."""
    doc, trades = _run(db_with_action(kind), index_csv)
    booked = {t.symbol for t in trades[bt.CONSTRUCT_DISCRETE]}
    assert ("WINNER" in booked) is books_trade
    assert booked >= {"GAPPER", "SPIKER"}                    # the veto is per symbol, never global
    assert doc["unadjusted_vetoes"]["discrete"] == vetoes
    assert doc["meta"]["n_discrete_signals_fired"] == (3 if books_trade else 2)


def test_rank_construct_skips_a_symbol_with_unadjusted_history(db_with_action, index_csv):
    """Every rebalance whose index clears min_sessions is a skip for WINNER — and none for a
    dividend, whose ex-date rescales nothing."""
    eligible = [i for i in bt.month_end_indices(SESSIONS) if i >= SIGNAL_IDX]
    assert eligible                                          # the fixture must reach a rebalance
    doc, _trades = _run(db_with_action("bonus"), index_csv)
    assert doc["unadjusted_vetoes"]["rank"] == len(eligible)
    doc_div, _ = _run(db_with_action("dividend"), index_csv)
    assert doc_div["unadjusted_vetoes"]["rank"] == 0


def test_corp_actions_coverage_is_reported_with_the_veto_counts(db_file, db_with_action, index_csv):
    """The veto reaches exactly as far as corp_actions does, so the report states the coverage."""
    doc_empty, _ = _run(db_file, index_csv)
    assert doc_empty["corp_actions_coverage"] == {
        "rows": 0, "ex_date_min": None, "ex_date_max": None,
        "structural_rows": 0, "structural_ex_date_min": None, "structural_ex_date_max": None,
    }
    assert doc_empty["unadjusted_vetoes"] == {"discrete": 0, "rank": 0}

    doc, _trades = _run(db_with_action("bonus"), index_csv)
    assert doc["corp_actions_coverage"] == {
        "rows": 1, "ex_date_min": str(ACTION_EX), "ex_date_max": str(ACTION_EX),
        "structural_rows": 1, "structural_ex_date_min": str(ACTION_EX),
        "structural_ex_date_max": str(ACTION_EX),
    }
    # A dividend-only table is "backfilled" but gives the veto NO reach: the structural span is empty.
    doc_div, _ = _run(db_with_action("dividend"), index_csv)
    assert doc_div["corp_actions_coverage"]["rows"] == 1
    assert doc_div["corp_actions_coverage"]["structural_rows"] == 0
    assert any("UNADJUSTED-HISTORY VETO" in n for n in doc["notes"])
    text = bt.render_text(doc)
    assert "unadjusted-history vetoes: discrete 1 signals" in text
    assert "structural corp_actions rows=1" in text
    assert str(ACTION_EX) in text


# ============================================================ 7. the v2 PRE-REGISTRATION (2026-09-09)
# Four MORE engineered symbols, in their OWN database so the v1 fixtures above keep their exact
# populations (n=3 everywhere). Each isolates ONE v2 signal-time filter, so the veto attribution the
# assertions read is order-independent:
#   SMOOTHIE - index member, smooth approach, no gap day        -> v1 and v2 both admit
#   OUTSIDER - identical bars, NOT an index member              -> v2 not_index veto
#   JUMPY    - index member, no gap day, one +17.5% mid-window jump -> v2 smooth veto
#   GAPPY    - index member, smooth approach, +6.15% trigger day    -> v2 gap veto
V2_FIRST_APPROACH_IDX = SIGNAL_IDX - 20        # 105: the first of the 21 rows diagnostics_for reads

#: closes at indices 105..125. +0.75/session: up_day_frac 1.00, max_day_move 0.0094, gap 0.0080.
SMOOTH_APPROACH = [f"{80.00 + 0.75 * k:.2f}" for k in range(21)]
#: +0.50/session then a +6.15% trigger day: smooth (max_day_move 0.0615 <= 0.07) but a GAP day.
GAP_APPROACH = [f"{80.00 + 0.50 * k:.2f}" for k in range(20)] + ["95.00"]
#: one +17.5% jump mid-window: up_day_frac 0.10, max_day_move 0.175 - jumpy, but NOT a gap day.
JUMP_APPROACH = ["80.00"] * 14 + ["94.00"] * 6 + ["95.00"]


def _v2_symbol_bars(sym: str, approach: list[str]) -> list[DailyBar]:
    """One v2 fixture symbol: a flat 80.00 prefix, ``approach`` as the closes at indices 105..125
    (the 21 rows ``diagnostics_for``'s 20 pairs read), then a flat forward path.

    Every high is pinned at 100.00, so the 252-session rolling high is exactly 100.00, prox =
    close / 100.00, and the fresh cross lands on the 95.00 trigger close (prox 0.9500 from below).
    Trigger volume 1500 vs the 1000 mean of the 20 sessions before it clears ``vol_mult`` 1.0.
    """
    assert len(approach) == 21
    first = V2_FIRST_APPROACH_IDX
    bars = [_flat(sym, i, "80.00", high="100.00") for i in range(first)]
    bars += [_flat(sym, first + k, px, high="100.00") for k, px in enumerate(approach[:-1])]
    bars.append(_bar(sym, SESSIONS[SIGNAL_IDX], o=approach[-2], h="100.00", lo=approach[-2],
                     c=approach[-1], v=1500))
    bars += [_flat(sym, i, approach[-1], high="100.00") for i in range(ENTRY_IDX, N_SESSIONS)]
    return bars


def _seed_bars(path: Path, parquet: Path, clock, bars: list[DailyBar]) -> Path:
    """A temp market.duckdb seeded with arbitrary bars, CLOSED so the study can attach read-only."""
    store = MarketStore(path, parquet, clock).open()
    try:
        store.upsert_bars_1d(bars)
    finally:
        store.close()
    return path


@pytest.fixture
def v2_db(tmp_path, clock) -> Path:
    return _seed_bars(
        tmp_path / "market_v2.duckdb", tmp_path / "parquet_v2", clock,
        _v2_symbol_bars("SMOOTHIE", SMOOTH_APPROACH)
        + _v2_symbol_bars("OUTSIDER", SMOOTH_APPROACH)
        + _v2_symbol_bars("JUMPY", JUMP_APPROACH)
        + _v2_symbol_bars("GAPPY", GAP_APPROACH),
    )


@pytest.fixture
def v2_index_csv(tmp_path) -> Path:
    """CURRENT-membership proxy for the v2 fixtures: OUTSIDER is the only extended name."""
    p = tmp_path / "index_v2.csv"
    p.write_text(
        "Company Name,Industry,Symbol,Series,ISIN Code\n"
        "Smoothie Ltd,Misc,SMOOTHIE,EQ,INE000000002\n"
        "Jumpy Ltd,Misc,JUMPY,EQ,INE000000003\n"
        "Gappy Ltd,Misc,GAPPY,EQ,INE000000004\n",
        encoding="utf-8",
    )
    return p


def _run_reg(db: Path, index_csv: Path, registration: str):
    conn = bt.open_readonly(db)
    try:
        return bt.run_study(
            conn,
            start=SESSIONS[0],
            end=SESSIONS[-1],
            cost_model=CostModel.from_config(),
            nifty200_csv=index_csv,
            db_path=db,
            registration=registration,
        )
    finally:
        conn.close()


def _discrete_symbols(trades) -> set:
    return {t.symbol for t in trades[bt.CONSTRUCT_DISCRETE]}


def test_v2_rejects_a_jumpy_approach_that_v1_accepts(v2_db, v2_index_csv):
    """JUMPY clears every v1 test (fresh cross, volume, no ex-date) and dies on the smooth filter."""
    _doc1, tr1 = _run_reg(v2_db, v2_index_csv, "v1")
    doc2, tr2 = _run_reg(v2_db, v2_index_csv, "v2")
    assert _discrete_symbols(tr1) == {"SMOOTHIE", "OUTSIDER", "JUMPY", "GAPPY"}
    assert "JUMPY" not in _discrete_symbols(tr2)
    assert doc2["v2_vetoes"]["smooth"] == 1
    # the veto is the DIAGNOSTIC field, not a re-derivation: JUMPY's own measured max_day_move is
    # the number the filter refused (0.175 > 0.07), and its up_day_frac is under 0.55 as well.
    jumpy = next(t for t in tr1[bt.CONSTRUCT_DISCRETE] if t.symbol == "JUMPY")
    assert jumpy.max_day_move > bt.V2_SMOOTH_MAX_DAY_MOVE_MAX
    assert jumpy.up_day_frac < bt.V2_SMOOTH_UP_FRAC_MIN
    assert not jumpy.gap_day                       # NOT rejected for the gap reason


def test_v2_rejects_a_trigger_day_move_above_the_gap_threshold(v2_db, v2_index_csv):
    """GAPPY is smooth (max_day_move 0.0615 <= 0.07) and an index member; only its +6.15% trigger
    day kills it, and that is the SAME move the descriptive gap A/B split flags."""
    _doc1, tr1 = _run_reg(v2_db, v2_index_csv, "v1")
    doc2, tr2 = _run_reg(v2_db, v2_index_csv, "v2")
    gappy = next(t for t in tr1[bt.CONSTRUCT_DISCRETE] if t.symbol == "GAPPY")
    assert gappy.gap_day                                        # v1 flags it descriptively
    assert gappy.up_day_frac >= bt.V2_SMOOTH_UP_FRAC_MIN
    assert gappy.max_day_move <= bt.V2_SMOOTH_MAX_DAY_MOVE_MAX  # so the smooth filter is NOT why
    assert "GAPPY" not in _discrete_symbols(tr2)
    assert doc2["v2_vetoes"]["gap"] == 1


def test_v2_population_is_index_members_only(v2_db, v2_index_csv):
    """OUTSIDER has SMOOTHIE's exact bars and is absent from v2 for one reason: it is not indexed."""
    _doc1, tr1 = _run_reg(v2_db, v2_index_csv, "v1")
    doc2, tr2 = _run_reg(v2_db, v2_index_csv, "v2")
    assert "OUTSIDER" in _discrete_symbols(tr1)
    assert _discrete_symbols(tr2) == {"SMOOTHIE"}                # the only survivor of all three
    assert doc2["v2_vetoes"] == {"smooth": 1, "gap": 1, "not_index": 1, "total": 3}
    assert doc2["meta"]["n_discrete_signals_fired"] == 4         # the RAW fresh-cross count, both regs
    text = bt.render_text(doc2)
    assert "v2 vetoes" in text and "not_index 1" in text
    assert "pre-registered before this run" in text


def test_trial_count_for_and_the_deflation_it_feeds(v2_db, v2_index_csv):
    """N=2 (v1+v2) is cited in the meta AND in every CPCV block a v2 run prints."""
    assert bt.trial_count_for("v1") == 1
    assert bt.trial_count_for("v2") == 2
    doc2, _tr2 = _run_reg(v2_db, v2_index_csv, "v2")
    assert doc2["meta"]["registration"] == "v2"
    assert doc2["meta"]["trial_count_n"] == 2
    assert doc2["meta"]["fold_pass_min"] == fold_pass_min(2)
    cpcv = doc2["constructs"][bt.CONSTRUCT_DISCRETE]["cpcv"]["20"]
    assert cpcv["trial_count_n"] == 2
    assert cpcv["fold_pass_min"] == fold_pass_min(2)
    text = bt.render_text(doc2)
    assert "registration    : v2" in text


def test_v1_default_never_consults_the_v2_constants(v2_db, v2_index_csv, monkeypatch):
    """Absurd v2 thresholds must not move a v1 run by one trade: v1 is a separate registration."""
    doc, trades = _run_reg(v2_db, v2_index_csv, "v1")
    baseline = sorted((t.symbol, t.signal_date, t.entry_px) for t in trades[bt.CONSTRUCT_DISCRETE])
    assert doc["meta"]["registration"] == "v1"
    assert doc["meta"]["trial_count_n"] == 1
    assert "v2_vetoes" not in doc                     # no v2 block on a v1 run

    monkeypatch.setattr(bt, "V2_SMOOTH_UP_FRAC_MIN", 99.0)
    monkeypatch.setattr(bt, "V2_SMOOTH_MAX_DAY_MOVE_MAX", -1.0)
    monkeypatch.setattr(bt, "V2_GAP_MAX", -1.0)
    doc_after, trades_after = _run_reg(v2_db, v2_index_csv, "v1")
    after = sorted((t.symbol, t.signal_date, t.entry_px) for t in trades_after[bt.CONSTRUCT_DISCRETE])
    assert after == baseline
    assert "v2_vetoes" not in doc_after
    assert doc_after["constructs"] == doc["constructs"]

    # and the same absurd constants DO bite a v2 run - proving the test patched the live names
    doc_v2, trades_v2 = _run_reg(v2_db, v2_index_csv, "v2")
    assert trades_v2[bt.CONSTRUCT_DISCRETE] == []
    assert doc_v2["v2_vetoes"]["smooth"] == 4


def test_cli_registration_flag_defaults_to_v1_and_accepts_v2(v2_db, v2_index_csv, tmp_path):
    out = tmp_path / "results" / "hi52_v2.json"
    rc = bt.main([
        "--db", str(v2_db), "--out", str(out),
        "--start", str(SESSIONS[0]), "--end", str(SESSIONS[-1]),
        "--nifty200-csv", str(v2_index_csv), "--registration", "v2",
    ])
    assert rc == 0
    doc = json.loads(out.read_text(encoding="utf-8"))
    assert doc["meta"]["registration"] == "v2"
    assert doc["meta"]["trial_count_n"] == 2
    assert doc["v2_vetoes"]["total"] == 3
    assert bt.build_parser().parse_args([]).registration == "v1"


# ==================================================== 8. REPORT HONESTY under v2 (punch list 2026-09-09)
# A v2 report that still prints v1's "N=1" boilerplate, or lets a reader compare a v2 split cell with
# the v1 cell of the same name, is a DISHONEST report even when every number in it is arithmetically
# right. These lock the three places that honesty lives.

#: The v1 sweep note, verbatim as it has read since the original pre-registration. Asserted as an
#: EXACT string: making the note registration-aware must not move one byte of the v1 rendering.
V1_SWEEP_NOTE = (
    "NO PARAMETER SWEEP was run: one pre-registered parameter set, trial count N=1 "
    "(fold_pass_min = 60%)."
)


def test_v1_sweep_note_is_byte_identical_to_the_original(v2_db, v2_index_csv):
    doc, _tr = _run_reg(v2_db, v2_index_csv, "v1")
    assert V1_SWEEP_NOTE in doc["notes"]


def test_v2_notes_never_cite_the_v1_trial_count(v2_db, v2_index_csv):
    """The sweep note is registration-aware: a v2 report cites N=2 and says N=1 nowhere at all."""
    doc, _tr = _run_reg(v2_db, v2_index_csv, "v2")
    notes = doc["notes"]
    assert not any("N=1" in n for n in notes), [n for n in notes if "N=1" in n]
    assert any("N=2" in n for n in notes)
    sweep = [n for n in notes if n.startswith("NO PARAMETER SWEEP")]
    assert sweep == [
        "NO PARAMETER SWEEP was run: one pre-registered parameter set, trial count N=2 "
        f"(fold_pass_min = {fold_pass_min(2):.0%})."
    ]


def test_v2_declares_which_split_cells_are_empty_by_construction(v2_db, v2_index_csv):
    """v2's own filters define two cells away and pre-filter the third; the report must say so."""
    doc2, _tr2 = _run_reg(v2_db, v2_index_csv, "v2")
    matching = [n for n in doc2["notes"] if "EMPTY BY CONSTRUCTION UNDER v2" in n]
    assert len(matching) == 1
    note = matching[0]
    assert bt.CELL_EXTENDED in note and bt.CELL_GAP_ONLY in note
    assert "already smooth-filtered" in note.lower()
    assert "comparable with the v1 cell of the same name" in note
    # residual #1 (2026-09-09): the claim is scoped to the discrete construct by NAME - the rank
    # blocks below use the same cell names but are declared v1, unchanged, in the same note.
    assert bt.CONSTRUCT_DISCRETE in note
    assert "v1 cells, UNCHANGED" in note
    assert bt.CONSTRUCT_RANK_TOP in note and bt.CONSTRUCT_RANK_BOTTOM in note
    # and it is v2-only: a v1 report carries no such claim
    doc1, _tr1 = _run_reg(v2_db, v2_index_csv, "v1")
    assert not any("EMPTY BY CONSTRUCTION UNDER v2" in n for n in doc1["notes"])
    # the cells really are empty under v2 (the note is a description, not a disclaimer)
    cells = doc2["constructs"][bt.CONSTRUCT_DISCRETE]["cells"]
    assert cells[bt.CELL_EXTENDED]["n_trades"] == 0
    assert cells[bt.CELL_GAP_ONLY]["n_trades"] == 0
    # the rank blocks' SAME-NAMED cells are genuinely v1: v2_admits never touches rank signals, so
    # neither rank construct's population is empty the way the discrete one is.
    for rank_construct in (bt.CONSTRUCT_RANK_TOP, bt.CONSTRUCT_RANK_BOTTOM):
        rank_cells = doc2["constructs"][rank_construct]["cells"]
        doc1_rank_cells = doc1["constructs"][rank_construct]["cells"]
        assert rank_cells == doc1_rank_cells


def test_v2_veto_line_documents_the_gap_count_containment(v2_db, v2_index_csv):
    """`gap` cannot count a trigger move above the smooth cap - that books as `smooth` first."""
    doc2, _tr2 = _run_reg(v2_db, v2_index_csv, "v2")
    text = bt.render_text(doc2)
    assert "max_day_move includes the trigger day" in text
    assert f"({bt.V2_GAP_MAX}, {bt.V2_SMOOTH_MAX_DAY_MOVE_MAX}]" in text
    assert "admission decision is unaffected" in text


# ==================================================== 9. THRESHOLD COUPLING + ROUNDING (residuals #2/#3, 2026-09-09)
# Punch-list residuals #2 and #3: V2_GAP_MAX must not drift from GAP_DAY_PCT, and signal_diag's
# gap_move must read the trigger day's own move exactly as hi52.diagnostics_for's max_day_move does
# (both rounded to the same 4 dp), or the "gap tally counts only trigger moves in
# (V2_GAP_MAX, V2_SMOOTH_MAX_DAY_MOVE_MAX]" claim quietly stops being true.

def test_v2_gap_max_is_coupled_to_the_v1_gap_day_threshold():
    """V2_GAP_MAX is DERIVED from GAP_DAY_PCT (not a second "0.05" literal), so the two thresholds
    cannot silently drift apart - the "gap_days_only is empty by construction under v2" report note
    depends on this exact equality holding.
    """
    assert bt.V2_GAP_MAX == bt.GAP_DAY_PCT / 100.0
    assert abs(bt.V2_GAP_MAX * 100.0 - bt.GAP_DAY_PCT) < 1e-12


def _diag_series(sym: str, trigger_move: float) -> bt.Series:
    """A 22-row synthetic series that bypasses the DB/bars machinery entirely - this probes
    ``signal_diag``/``v2_admits``/``measure`` directly, not the fresh-cross scan.

    Indices 0-20 are the 21-row window ``diagnostics_for`` reads (20 pairs), engineered so exactly
    11 of the 20 are "up" (10 tiny +1.0 nudges on a ~100,000 base, plus the trigger pair itself) -
    ``up_day_frac`` lands at EXACTLY 0.55, the v2 smooth threshold, so only max_day_move/gap_move
    decide what this test probes. Index 20 is the trigger day, whose OWN move is ``trigger_move``
    (raw, unrounded) and by construction the window's biggest mover by a wide margin. Index 21 is a
    filler row so ``measure`` has an entry (open) and a T+1 exit (close); its price is irrelevant to
    every assertion that reads it.
    """
    closes = [100_000.0]
    for j in range(19):
        closes.append(closes[-1] + (1.0 if j % 2 == 0 else 0.0))
    closes.append(closes[-1] * (1.0 + trigger_move))   # index 20: the trigger day
    closes.append(closes[-1])                          # index 21: entry/exit filler row only
    dates = SESSIONS[: len(closes)]
    rows = [bt.DailyRow(high=c, close=c, volume=1000.0, open=c) for c in closes]
    return bt.Series(
        sym, dates, list(closes), list(closes), list(closes), list(closes),
        [1000.0] * len(closes), rows,
    )


def test_gap_move_rounds_to_the_same_4dp_as_max_day_move_at_the_smooth_boundary():
    """A trigger move of 0.070049 rounds to 0.0700 - the exact V2_SMOOTH_MAX_DAY_MOVE_MAX boundary.

    Before the fix, ``hi52.diagnostics_for`` handed back ``max_day_move`` ALREADY rounded to 0.0700
    (src/engine/strategy/scanners/hi52.py:198) while ``signal_diag``'s own ``gap_move`` stayed raw at
    0.070049 - the SAME physical number (the trigger day is the window's biggest mover) disagreeing
    by a rounding artefact. ``signal_diag`` now rounds ``gap_move`` the same way, so the two read
    bit-identically and every filter that reads either one books the signal the same way.
    """
    series = _diag_series("ROUNDBOUNDARY", 0.070049)
    i = 20                                     # the trigger day; index 21 is the entry/exit filler
    params = bt.PRE_REGISTERED_PARAMS

    d = bt.signal_diag(series, i, params)
    assert d.max_day_move == pytest.approx(0.0700, abs=1e-12)
    assert d.gap_move == pytest.approx(0.0700, abs=1e-12)
    assert d.gap_move == d.max_day_move             # bit-identical, not merely close
    assert d.up_day_frac == pytest.approx(0.55)      # exactly at the OTHER v2 boundary too

    veto_counts: dict[str, int] = {}
    admitted = bt.v2_admits(series, i, params, index_members=set(), veto_counts=veto_counts)
    assert admitted is False
    # PASSED the smooth check (0.0700 <= 0.07) first: if gap_move had stayed raw at 0.070049 while
    # max_day_move read rounded, this signal would still land here (0.070049 > 0.05 either way), but
    # the two diagnostics would disagree on what "the trigger day's move" even is.
    assert veto_counts == {bt.V2_VETO_GAP: 1}

    t = bt.measure(
        series, i, construct=bt.CONSTRUCT_DISCRETE, prox=0.99, score=0.99, cost_pct=0.0,
        index_members=set(), params=params, horizons=(1,),
    )
    assert t is not None
    # v1's own GAP_DAY_PCT comparison (scripts/backtest_hi52.py:678) agrees: both filters book this
    # signal as a gap, off the SAME rounded number.
    assert t.gap_day is True
    assert t.max_day_move == pytest.approx(0.0700)


def test_v1_gap_day_cell_unaffected_by_the_gap_move_rounding_for_existing_fixtures(study):
    """None of WINNER/GAPPER/SPIKER sits near the 5% GAP_DAY_PCT boundary at 4dp resolution, so the
    v1 gap_day classification (scripts/backtest_hi52.py:678) is exactly what it was before
    signal_diag started rounding gap_move.
    """
    _doc, trades = study
    by_symbol = _by_symbol(trades)
    assert by_symbol["WINNER"].gap_day is False    # +2.13% raw and rounded - nowhere near 5%
    assert by_symbol["GAPPER"].gap_day is True     # +6.67% raw and rounded - nowhere near 5%
    assert by_symbol["SPIKER"].gap_day is True     # +10.00% exactly - no rounding ambiguity at all
