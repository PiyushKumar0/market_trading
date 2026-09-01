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


@pytest.fixture
def db_file(tmp_path, clock) -> Path:
    """A temp market.duckdb seeded with the three symbols, CLOSED so the study can attach read-only."""
    path = tmp_path / "market.duckdb"
    store = MarketStore(path, tmp_path / "parquet", clock).open()
    try:
        store.upsert_bars_1d(winner_bars() + gapper_bars() + spiker_bars())
    finally:
        store.close()
    return path


@pytest.fixture
def index_csv(tmp_path) -> Path:
    """CURRENT-membership proxy: WINNER is 'in the index', GAPPER/SPIKER are extended names."""
    p = tmp_path / "nifty200.csv"
    p.write_text(
        "Company Name,Industry,Symbol,Series,ISIN Code\nWinner Ltd,Misc,WINNER,EQ,INE000000001\n",
        encoding="utf-8",
    )
    return p


@pytest.fixture
def study(db_file, index_csv):
    conn = bt.open_readonly(db_file)
    try:
        doc, trades = bt.run_study(
            conn,
            start=SESSIONS[0],
            end=SESSIONS[-1],
            cost_model=CostModel.from_config(),
            nifty200_csv=index_csv,
            db_path=db_file,
            verify_prefilter=10,          # brute-force every eligible day for all three symbols
        )
    finally:
        conn.close()
    return doc, trades


def _by_symbol(trades) -> dict:
    return {t.symbol: t for t in trades[bt.CONSTRUCT_DISCRETE]}


def _cost_pct() -> float:
    return float(CostModel.from_config().breakeven_pct(bt.REFERENCE_NOTIONAL, "CNC"))


# ============================================================ 0. pre-registration discipline
def test_one_pre_registered_param_set_no_sweep():
    """N=1 and the params ARE the live shadow rule's frozen defaults — no sweep, no drift."""
    assert bt.TRIAL_COUNT_N == 1
    assert bt.PRE_REGISTERED_PARAMS == dict(hi52.DEFAULT_PARAMS)
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
