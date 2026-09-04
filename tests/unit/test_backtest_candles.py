"""scripts/backtest_candles.py - the pre-registered candle battery, on synthetic 1m bars.

Every fixture is hand-computed so each assertion has an arithmetic answer, not a regression blob.

The synthetic tape is a temp :class:`MarketStore` holding ONE session (``medvol20`` is intra-session,
so unlike ``tdc`` this study needs no prior-session history) of 09:15..15:29 one-minute bars for a
cast engineered so that EXACTLY ONE rule fires for each ``*_POS`` symbol, at EXACTLY ONE bucket, and
its ``*_NEG`` twin fails that rule by ONE clause and nothing else:

* ``R1_POS`` / ``R1_NEG`` - momentum burst; the twin's volume is 1.9x medvol20, under the 2.0x bar.
* ``R2_POS`` / ``R2_NEG`` - three soldiers; the twin's third-bar volume is 1.4x, under 1.5x.
* ``R3_POS`` / ``R3_NEG`` - engulfing pullback; the twin's volume is 1.1x, under 1.2x.
* ``R4_POS`` / ``R4_NEG`` - VWAP hold; the twin's low never reaches VWAP x 1.0015.
* ``R5_POS`` / ``R5_NEG`` - VWAP reclaim; the twin's volume is 1.4x, under 1.5x.
* ``TWICE``    - an R1 setup at bucket 10 AND another at bucket 20: one trade must come out, not two.
* ``SHORTDAY`` - its tape starts at 09:45, so at bucket 10 it has 4 prior valid bars, under the
  8-bar ``medvol20`` floor: no trade, and the floor is what stops it.
* ``GAPPY``    - bucket 3 holds only two 1m bars, so that bucket is not a bar at all.
* ``FILL000..FILL159`` - flat filler names that trigger nothing, present so the session clears the
  ``session_min_symbols`` floor (a session must carry MORE than 150 symbols) without any override of
  the pre-registered PARAMS.

Post-signal paths are engineered so the battery produces one of every exit: ``R1_POS`` takes E1's
TARGET and E2's TRAIL, ``R3_POS`` takes E1's STOP and E2's TRAIL, and ``R2_POS`` reaches the 15:15
squareoff on both - with a 200.00 print after 15:15 that must be unreachable.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path

import pandas as pd
import pytest

_ROOT = Path(__file__).resolve().parents[2]


def _load(name: str, rel: str):
    spec = importlib.util.spec_from_file_location(name, _ROOT / rel)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


bt = _load("mt_backtest_candles", "scripts/backtest_candles.py")
tdc = _load("mt_backtest_tdc_ref", "scripts/backtest_tdc.py")

from engine.core.clock import IST  # noqa: E402
from engine.core.types import Bar  # noqa: E402
from engine.marketdata.store import MarketStore  # noqa: E402
from engine.strategy.cost_model import CostModel  # noqa: E402

D = date(2026, 3, 2)                      # a Monday; one session is the whole fixture
N_BUCKETS = 75                            # 09:15..15:29 = 375 one-minute bars = 75 five-minute bars
FLAT100 = (100.0, 100.0, 100.0, 100.0, 1000)
N_FILLERS = 160


# --------------------------------------------------------------------------- tape construction
def _bar(sym: str, mfo: int, o: float, h: float, low: float, c: float, vol: int) -> Bar:
    total = 9 * 60 + 15 + mfo
    return Bar(
        symbol=sym,
        ts_minute=datetime.combine(D, time(total // 60, total % 60), tzinfo=IST),
        open=Decimal(str(round(o, 2))), high=Decimal(str(round(h, 2))),
        low=Decimal(str(round(low, 2))), close=Decimal(str(round(c, 2))),
        volume=int(vol), src="kite_official",
    )


def _bucket(sym: str, bk: int, spec, minutes=(0, 1, 2, 3, 4)) -> list[Bar]:
    """Five 1m bars whose 5m aggregate is EXACTLY ``spec`` = (open, high, low, close, volume).

    The high is printed in minute 1, the low in minute 2 and the close in minute 4, so the aggregate
    is (first open, max high, min low, last close, sum volume) by construction.
    """
    o, h, low, c, vol = spec
    per = int(vol) // 5
    shape = [
        (o, o, o, o),
        (o, h, o, o),
        (o, o, low, o),
        (o, o, o, o),
        (o, max(o, c), min(o, c), c),
    ]
    return [_bar(sym, bk * 5 + i, *shape[i], per) for i in minutes]


def _session(sym: str, segments, *, gappy_bk: int | None = None) -> list[Bar]:
    specs: dict[int, tuple] = {}
    for lo, hi, spec in segments:
        for bk in range(lo, hi + 1):
            specs[bk] = spec
    bars: list[Bar] = []
    for bk in range(N_BUCKETS):
        spec = specs.get(bk)
        if spec is None:
            continue
        bars += _bucket(sym, bk, spec, minutes=(0, 1) if bk == gappy_bk else (0, 1, 2, 3, 4))
    return bars


CAST: dict[str, list] = {
    # R1: close 102 tops the prior 6 highs (100), volume 2.0x medvol20 (1000), close at the very top
    # of the bar's range. Post-signal: 107 print (E1 target 106), then a 5m close under the prior
    # bar's low (E2 trail), filled at the following 100.50 open.
    "R1_POS": [(0, 9, FLAT100), (10, 10, (100.0, 102.0, 100.0, 102.0, 2000)),
               (11, 11, (102.0, 102.0, 102.0, 102.0, 1000)),
               (12, 12, (102.0, 107.0, 102.0, 103.0, 1000)),
               (13, 13, (103.0, 103.0, 101.0, 101.0, 1000)),
               (14, 74, (100.5, 100.5, 100.5, 100.5, 1000))],
    "R1_NEG": [(0, 9, FLAT100), (10, 10, (100.0, 102.0, 100.0, 102.0, 1900)),
               (11, 74, (102.0, 102.0, 102.0, 102.0, 1000))],
    # R2: three bullish bars, each closing above the prior bar's high; third bar 1.6x medvol20.
    # Post-signal is dead flat, so BOTH exits reach the 15:15 squareoff - and the 200.00 tape after
    # 15:15 must never be reachable.
    "R2_POS": [(0, 7, FLAT100), (8, 8, (100.1, 101.0, 100.1, 101.0, 1000)),
               (9, 9, (101.0, 102.0, 101.0, 102.0, 1000)),
               (10, 10, (102.0, 103.0, 102.0, 103.0, 1600)),
               (11, 72, (103.0, 103.0, 103.0, 103.0, 1000)),
               (73, 74, (200.0, 200.0, 200.0, 200.0, 1000))],
    "R2_NEG": [(0, 7, FLAT100), (8, 8, (100.1, 101.0, 100.1, 101.0, 1000)),
               (9, 9, (101.0, 102.0, 101.0, 102.0, 1000)),
               (10, 10, (102.0, 103.0, 102.0, 103.0, 1400)),
               (11, 74, (103.0, 103.0, 103.0, 103.0, 1000))],
    # R3: session high 106 set at bucket 6 (>= 3 bars before the signal), three lower closes that
    # stay above VWAP, then a bullish body engulf on 1.3x volume. Post-signal digs to 102.50, which
    # is the E1 stop (the signal bar's low, 103.00); E2 trails out one bar later at 102.00.
    "R3_POS": [(0, 5, FLAT100), (6, 6, (100.0, 106.0, 100.0, 105.5, 1000)),
               (7, 7, (105.5, 105.5, 105.0, 105.0, 1000)),
               (8, 8, (105.0, 105.0, 104.0, 104.0, 1000)),
               (9, 9, (104.0, 104.0, 103.0, 103.0, 1000)),
               (10, 10, (103.0, 105.0, 103.0, 104.5, 1300)),
               (11, 11, (104.5, 104.5, 104.5, 104.5, 1000)),
               (12, 12, (104.5, 104.5, 102.5, 103.0, 1000)),
               (13, 74, (102.0, 102.0, 102.0, 102.0, 1000))],
    "R3_NEG": [(0, 5, FLAT100), (6, 6, (100.0, 106.0, 100.0, 105.5, 1000)),
               (7, 7, (105.5, 105.5, 105.0, 105.0, 1000)),
               (8, 8, (105.0, 105.0, 104.0, 104.0, 1000)),
               (9, 9, (104.0, 104.0, 103.0, 103.0, 1000)),
               (10, 10, (103.0, 105.0, 103.0, 104.5, 1100)),
               (11, 74, (104.5, 104.5, 104.5, 104.5, 1000))],
    # R4: six bars holding above a rising VWAP, then a dip to 100.60 (VWAP(bk10) = 100.6376, and
    # 100.60 <= 100.6376 * 1.0015 = 100.7886) closing back up at 101.20.
    "R4_POS": [(0, 3, FLAT100), (4, 9, (101.0, 101.0, 101.0, 101.0, 1000)),
               (10, 10, (101.0, 101.2, 100.6, 101.2, 1000)),
               (11, 74, (101.2, 101.2, 101.2, 101.2, 1000))],
    "R4_NEG": [(0, 3, FLAT100), (4, 9, (101.0, 101.0, 101.0, 101.0, 1000)),
               (10, 10, (101.0, 101.2, 101.0, 101.2, 1000)),
               (11, 74, (101.2, 101.2, 101.2, 101.2, 1000))],
    # R5: six bars closing under VWAP, then a reclaim to 100.50 on 1.6x volume.
    "R5_POS": [(0, 3, FLAT100), (4, 9, (99.0, 99.0, 99.0, 99.0, 1000)),
               (10, 10, (99.0, 100.5, 99.0, 100.5, 1600)),
               (11, 74, (100.5, 100.5, 100.5, 100.5, 1000))],
    "R5_NEG": [(0, 3, FLAT100), (4, 9, (99.0, 99.0, 99.0, 99.0, 1000)),
               (10, 10, (99.0, 100.5, 99.0, 100.5, 1400)),
               (11, 74, (100.5, 100.5, 100.5, 100.5, 1000))],
    # two R1 setups in one session: the SECOND must be discarded by "one trade per symbol per day".
    "TWICE": [(0, 9, FLAT100), (10, 10, (100.0, 102.0, 100.0, 102.0, 2000)),
              (11, 19, (102.0, 102.0, 102.0, 102.0, 1000)),
              (20, 20, (102.0, 104.0, 102.0, 104.0, 3000)),
              (21, 74, (104.0, 104.0, 104.0, 104.0, 1000))],
    # an R1-shaped bar at bucket 10 with only 4 prior valid bars: under the medvol20 floor.
    "SHORTDAY": [(6, 9, FLAT100), (10, 10, (100.0, 102.0, 100.0, 102.0, 2000)),
                 (11, 20, (102.0, 102.0, 102.0, 102.0, 1000))],
    "GAPPY": [(0, 74, FLAT100)],
}

EXPECTED_TRIGGERS = {
    ("R1_POS", "R1"), ("R2_POS", "R2"), ("R3_POS", "R3"),
    ("R4_POS", "R4"), ("R5_POS", "R5"), ("TWICE", "R1"),
}
SIGNAL_BK = 10                    # every engineered trigger is the [10:05,10:10) bucket
SIGNAL_MFO = 54                   # its last 1m bar, 10:09
ENTRY_MFO = 55                    # the fill bar, 10:10
SQUAREOFF_MFO = 360               # 15:15


def _tape() -> list[Bar]:
    bars: list[Bar] = []
    for sym, segs in CAST.items():
        bars += _session(sym, segs, gappy_bk=3 if sym == "GAPPY" else None)
    for i in range(N_FILLERS):
        bars += _session(f"FILL{i:03d}", [(0, 74, FLAT100)])
    return bars


@pytest.fixture(scope="module")
def module_clock():
    from engine.core.clock import Clock
    return Clock()


@pytest.fixture(scope="module")
def db_file(tmp_path_factory, module_clock) -> Path:
    """A temp market.duckdb holding the synthetic tape, CLOSED so the study can attach read-only."""
    root = tmp_path_factory.mktemp("candles")
    path = root / "market.duckdb"
    store = MarketStore(path, root / "parquet", module_clock).open()
    try:
        tape = _tape()
        for i in range(0, len(tape), 20000):
            store.insert_bars_1m(tape[i: i + 20000])
    finally:
        store.close()
    return path


@pytest.fixture(scope="module")
def study(db_file):
    conn = bt.open_readonly(db_file)
    try:
        return bt.run_study(conn, start=D, end=D, cost_model=CostModel.from_config())
    finally:
        conn.close()


@pytest.fixture(scope="module")
def signals(db_file):
    """The raw first-trigger frame, with the eligibility view wired exactly as run_study wires it."""
    conn = bt.open_readonly(db_file)
    try:
        sd = bt.load_symbol_days(conn, D, D, params=bt.PARAMS)
        elig, _fb, _dropped = bt.eligible_universe(sd, bt.load_universe_daily(conn), params=bt.PARAMS)
        conn.register("cnd_elig", pd.DataFrame(
            [{"symbol": s, "d": d} for d in sorted(elig) for s in elig[d]], columns=["symbol", "d"]
        ))
        try:
            return bt.load_signals(conn, D, D, params=bt.PARAMS)
        finally:
            conn.unregister("cnd_elig")
    finally:
        conn.close()


def _cell(study, rule: str, style: str) -> dict[str, bt.Trade]:
    _doc, trades = study
    return {t.symbol: t for t in trades[f"{rule}|{style}"]}


def _cost(entry_px: float) -> float:
    return bt.trade_cost_pct(CostModel.from_config(), bt.PARAMS, entry_px)


# ============================================================ 0. pre-registration discipline
def test_one_params_dict_five_rules_two_exits_and_no_signal_knob_on_the_cli():
    """PARAMS/RULES/EXITS are the pre-registered set; the CLI cannot turn them into a grid."""
    assert set(bt.RULES) == {"R1", "R2", "R3", "R4", "R5"}
    assert [v["name"] for v in bt.RULES.values()] == [
        "momentum_burst", "three_soldiers", "engulf_pullback", "vwap_hold_buy", "vwap_reclaim"
    ]
    assert set(bt.EXITS) == {"E1", "E2"}
    p = bt.PARAMS
    assert (p["bucket_minutes"], p["min_1m_bars_per_bucket"]) == (5, 3)
    assert (p["signal_window_start"], p["signal_window_end"]) == ("09:45", "14:00")
    assert (p["medvol_lookback_bars"], p["medvol_min_bars"]) == (20, 8)
    assert (p["r1_vol_mult"], p["r1_breakout_lookback_bars"], p["r1_close_in_range_frac"]) == (2.0, 6, 0.75)
    assert p["r2_vol_mult"] == 1.5
    assert (p["r3_vol_mult"], p["r3_session_high_min_bars_ago"], p["r3_pullback_bars"]) == (1.2, 3, 3)
    assert (p["r4_prior_bars_above_vwap"], p["r4_vwap_touch_mult"]) == (6, 1.0015)
    assert (p["r5_prior_bars_below_vwap"], p["r5_vol_mult"]) == (6, 1.5)
    assert p["max_trades_per_rule_per_day"] == 5
    assert p["e1_target_r_multiple"] == 2.0
    assert p["squareoff_time"] == "15:15"
    assert p["product"] == "MIS"
    assert (p["promote_min_n"], p["promote_min_t"], p["promote_min_cpcv_positive_share"]) == (200, 2.0, 0.60)
    flags = {a.option_strings[0] for a in bt.build_parser()._actions if a.option_strings}
    assert not (flags & {"--rule", "--rules", "--vol-mult", "--stop", "--target", "--exit",
                         "--signal-window", "--top-n", "--grid"})


def test_report_records_that_no_sweep_and_no_selection_happened(study):
    doc, _trades = study
    assert doc["meta"]["parameter_sweep_run"] is False
    assert doc["meta"]["cell_selection_performed"] is False
    assert set(doc["results"]) == {f"{r}|{e}" for r in bt.RULES for e in bt.EXITS}
    assert len(doc["results"]) == 10


# ============================================================ 1. the 5m grid and VWAP
def test_minute_offsets_and_the_signal_window_are_0915_anchored():
    assert bt.to_mfo("09:15") == 0 and bt.from_mfo(0) == "09:15"
    assert bt.to_mfo("09:45") == 30 and bt.to_mfo("14:00") == 285
    assert bt.from_mfo(374) == "15:29" and bt.to_mfo("15:15") == 360
    assert bt.signal_bucket_bounds(bt.PARAMS) == (30, 285)
    # the [14:00,14:05) bucket could only close at 14:00 by holding ONE bar, under the 3-bar floor,
    # so 13:59 is the last minute any signal bar can contain
    assert bt.signal_relation_mfo_hi(bt.PARAMS) == 284
    assert bt.from_mfo(284) == "13:59"


def test_five_minute_bars_aggregate_1m_into_0915_anchored_buckets(db_file):
    """Bucket 10 of R1_POS is [10:05,10:10): open 100, high 102, low 100, close 102, volume 2000."""
    conn = bt.open_readonly(db_file)
    try:
        f = bt.load_five_min(conn, D, D)
    finally:
        conn.close()
    row = f[(f["symbol"] == "R1_POS") & (f["bk"] == SIGNAL_BK)].iloc[0]
    assert int(row.n1m) == 5
    assert int(row.last_mfo) == SIGNAL_MFO
    assert bt.from_mfo(int(row.bk) * 5) == "10:05" and bt.from_mfo(int(row.last_mfo)) == "10:09"
    assert (float(row.o5), float(row.h5), float(row.l5), float(row.c5)) == (100.0, 102.0, 100.0, 102.0)
    assert float(row.v5) == pytest.approx(2000.0)

    # VWAP(bucket 10) by hand: ten flat buckets of 1000 at typical price 100, then bucket 10's five
    # bars (400 each) with typical prices 100, 100.666667, 100, 100, 101.333333.
    expected = (10 * 1000 * 100.0 + 400 * (100.0 + 302.0 / 3.0 + 100.0 + 100.0 + 304.0 / 3.0)) / 12000.0
    assert float(row.vwap5) == pytest.approx(expected, abs=1e-9)
    assert expected == pytest.approx(100.0666667, abs=1e-6)


def test_a_bucket_with_fewer_than_three_one_minute_bars_is_not_a_bar(db_file):
    """GAPPY's bucket 3 holds two 1m bars, so it is dropped from the relation entirely."""
    conn = bt.open_readonly(db_file)
    try:
        f = bt.load_five_min(conn, D, D)
    finally:
        conn.close()
    g = f[f["symbol"] == "GAPPY"]
    assert set(g["bk"]) == set(range(N_BUCKETS)) - {3}
    assert not g[g["bk"] == 3].shape[0]
    assert g[g["bk"] == 2].shape[0] == 1 and g[g["bk"] == 4].shape[0] == 1


# ============================================================ 2. every rule, positive and negative
@pytest.mark.parametrize("rule,pos,neg", [
    ("R1", "R1_POS", "R1_NEG"),
    ("R2", "R2_POS", "R2_NEG"),
    ("R3", "R3_POS", "R3_NEG"),
    ("R4", "R4_POS", "R4_NEG"),
    ("R5", "R5_POS", "R5_NEG"),
])
def test_each_rule_fires_on_its_setup_and_not_on_the_one_clause_twin(signals, rule, pos, neg):
    """Each twin differs from its setup by exactly one clause, and that clause is disqualifying."""
    fired = signals[signals["rule"] == rule]
    hit = fired[fired["symbol"] == pos]
    assert len(hit) == 1, f"{rule} did not fire once on {pos}"
    assert int(hit.iloc[0].bk) == SIGNAL_BK
    assert int(hit.iloc[0].last_mfo) == SIGNAL_MFO
    assert neg not in set(fired["symbol"]), f"{rule} fired on the negative twin {neg}"


def test_exactly_the_engineered_rules_fire_at_the_signal_bucket(signals):
    """The whole cast in one assertion: nothing fires by accident at bucket 10.

    Without this, every "the twin did not fire" test above would also pass on a dead rule set.
    """
    at_bk = signals[signals["bk"] == SIGNAL_BK]
    assert set(zip(at_bk["symbol"], at_bk["rule"], strict=True)) == EXPECTED_TRIGGERS


def test_the_medvol20_floor_skips_a_bar_without_eight_prior_valid_bars(signals, study):
    """SHORTDAY's tape starts at 09:45, so its bucket-10 burst has 4 prior bars, not 8."""
    assert "SHORTDAY" not in set(signals["symbol"])
    for style in bt.EXITS:
        assert "SHORTDAY" not in _cell(study, "R1", style)


def test_only_the_first_trigger_of_a_rule_is_taken_per_symbol_day(signals, study):
    """TWICE has an R1 setup at bucket 10 AND at bucket 20; exactly one trade must come out."""
    twice = signals[(signals["symbol"] == "TWICE") & (signals["rule"] == "R1")]
    assert len(twice) == 1 and int(twice.iloc[0].bk) == SIGNAL_BK
    _doc, trades = study
    assert sum(1 for t in trades["R1|E1"] if t.symbol == "TWICE") == 1


def test_the_daily_cap_keeps_the_top_five_by_volume_ratio_then_symbol():
    """The cap is per RULE per DAY, ranked by volume ratio DESC then symbol ASC."""
    rows = [
        {"symbol": s, "d": D, "rule": "R1", "vol_ratio": v, "bk": 10}
        for s, v in [("A", 1.0), ("B", 9.0), ("C", 5.0), ("D", 5.0), ("E", 4.0), ("F", 3.0), ("G", 2.0)]
    ]
    rows += [{"symbol": "Z", "d": D, "rule": "R2", "vol_ratio": 0.1, "bk": 10}]
    kept = bt.rank_and_cap(pd.DataFrame(rows), params=bt.PARAMS)
    r1 = kept[kept["rule"] == "R1"]
    assert list(r1["symbol"]) == ["B", "C", "D", "E", "F"]          # ties (C, D) broken by symbol
    assert list(kept[kept["rule"] == "R2"]["symbol"]) == ["Z"]      # the cap is per rule, not per day
    assert len(kept) == 6


# ============================================================ 3. entry
def test_entry_is_the_open_of_the_1m_bar_after_the_signal_bars_last_minute(study):
    """R1_POS's signal bar closes at 10:09 (102.00) and the 10:10 bar OPENS at 102.00 - the fill."""
    for style in bt.EXITS:
        t = _cell(study, "R1", style)["R1_POS"]
        assert t.signal_mfo == SIGNAL_MFO and t.entry_mfo == ENTRY_MFO
        assert bt.from_mfo(t.entry_mfo) == "10:10"
        assert t.entry_px == pytest.approx(102.0)


def test_a_missing_entry_bar_is_a_skipped_trade_never_a_fill_at_another_price():
    bars = ([56, 57], [100.0, 100.0], [100.0, 100.0], [100.0, 100.0], [100.0, 100.0])
    assert bt.simulate_e1(bars, entry_mfo=55, stop_px=99.0, target_r_multiple=2.0,
                          squareoff_mfo=SQUAREOFF_MFO) is None
    assert bt.simulate_e2(bars, [], entry_mfo=55, signal_bk=10,
                          squareoff_mfo=SQUAREOFF_MFO) is None


# ============================================================ 4. E1 - stop, target, squareoff
def test_e1_fills_the_target_at_the_target_price(study):
    """Entry 102.00, stop 100.00 (the signal bar's low), target 102 + 2 x 2 = 106.00; the 10:16 bar
    prints 107.00, so the fill is 106.00 - not the 107.00 that reached it."""
    t = _cell(study, "R1", "E1")["R1_POS"]
    assert (t.stop_px, t.target_px) == (pytest.approx(100.0), pytest.approx(106.0))
    assert t.exit_reason == "target"
    assert t.exit_px == pytest.approx(106.0)
    assert bt.from_mfo(t.exit_mfo) == "10:16"
    assert t.gross_pct == pytest.approx((106.0 / 102.0 - 1.0) * 100.0, abs=1e-9)
    assert t.net_pct == pytest.approx(t.gross_pct - _cost(102.0), abs=1e-12)


def test_e1_fills_the_stop_at_the_stop_price(study):
    """R3_POS: entry 104.50, stop 103.00 (the signal bar's low); the 10:17 bar dips to 102.50."""
    t = _cell(study, "R3", "E1")["R3_POS"]
    assert t.entry_px == pytest.approx(104.5)
    assert t.stop_px == pytest.approx(103.0)
    assert t.target_px == pytest.approx(107.5)
    assert t.exit_reason == "stop"
    assert t.exit_px == pytest.approx(103.0)
    assert bt.from_mfo(t.exit_mfo) == "10:17"
    assert t.stop_gap_through is False                     # that bar opened at 104.50, above the stop
    assert t.gross_pct == pytest.approx((103.0 / 104.5 - 1.0) * 100.0, abs=1e-9)


def test_e1_stop_wins_a_tie_inside_one_minute():
    """A minute that touches BOTH levels books the STOP - a 1m bar cannot say which came first."""
    bars = ([55, 56], [100.0, 100.0], [100.0, 120.0], [100.0, 90.0], [100.0, 110.0])
    got = bt.simulate_e1(bars, entry_mfo=55, stop_px=99.0, target_r_multiple=2.0,
                         squareoff_mfo=SQUAREOFF_MFO)
    assert (got.reason, got.exit_px) == ("stop", 99.0)
    assert got.target_px == pytest.approx(102.0)


def test_e1_never_checks_the_entry_bar_itself():
    """The entry bar's own low is below the stop; the exit must still come from a LATER bar."""
    bars = ([55, 56], [100.0, 100.0], [100.0, 100.0], [1.0, 100.0], [100.0, 100.0])
    got = bt.simulate_e1(bars, entry_mfo=55, stop_px=99.0, target_r_multiple=2.0,
                         squareoff_mfo=SQUAREOFF_MFO)
    assert got.reason == "squareoff_1515" and got.exit_px == pytest.approx(100.0)


def test_e1_refuses_a_trade_whose_stop_is_at_or_above_the_entry():
    """Filling such a trade "at the stop" would print a fictitious gain, so it is dropped."""
    bars = ([55, 56], [100.0, 100.0], [100.0, 100.0], [100.0, 90.0], [100.0, 95.0])
    assert bt.simulate_e1(bars, entry_mfo=55, stop_px=100.0, target_r_multiple=2.0,
                          squareoff_mfo=SQUAREOFF_MFO) is None
    assert bt.simulate_e1(bars, entry_mfo=55, stop_px=101.0, target_r_multiple=2.0,
                          squareoff_mfo=SQUAREOFF_MFO) is None
    ok = bt.simulate_e1(bars, entry_mfo=55, stop_px=99.9, target_r_multiple=2.0,
                        squareoff_mfo=SQUAREOFF_MFO)
    assert ok is not None and ok.reason == "stop"


def test_e1_records_a_gap_through_stop_fill_as_optimistic():
    """The brief says "low <= stop -> fill at stop"; when the minute OPENS below the stop that fill
    is optimistic, and the trade carries the flag that says so."""
    bars = ([55, 56], [100.0, 90.0], [100.0, 90.0], [100.0, 90.0], [100.0, 90.0])
    got = bt.simulate_e1(bars, entry_mfo=55, stop_px=99.0, target_r_multiple=2.0,
                         squareoff_mfo=SQUAREOFF_MFO)
    assert got.reason == "stop" and got.exit_px == pytest.approx(99.0)
    assert got.stop_gap_through is True


def test_e1_stop_for_r5_is_the_signal_bars_vwap_not_its_low(study):
    """The brief's parenthesis: R5's stop is VWAP at the signal bar (99.3862), not the bar's low."""
    t = _cell(study, "R5", "E1")["R5_POS"]
    assert t.stop_px == pytest.approx(99.3862, abs=1e-3)
    assert t.stop_px != pytest.approx(99.0, abs=1e-2)               # the signal bar's low
    assert t.target_px == pytest.approx(t.entry_px + 2.0 * (t.entry_px - t.stop_px), abs=1e-9)


# ============================================================ 5. E2 - the trail
def test_e2_exits_at_the_next_1m_open_after_the_trail_bar_not_at_its_close(study):
    """R1_POS: the [10:20,10:25) bar closes at 101.00, under the prior bar's 102.00 low; the fill is
    the 10:25 OPEN of 100.50. A close-fill would book 101.00 - half a rupee better."""
    t = _cell(study, "R1", "E2")["R1_POS"]
    assert t.exit_reason == "trail_next_open"
    assert bt.from_mfo(t.exit_mfo) == "10:25"
    assert t.exit_px == pytest.approx(100.5)
    assert t.exit_px != pytest.approx(101.0, abs=1e-3)              # never the triggering close
    assert t.gross_pct == pytest.approx((100.5 / 102.0 - 1.0) * 100.0, abs=1e-9)


def test_e2_trails_on_the_first_qualifying_bar(study):
    """R3_POS: the [10:15,10:20) bar closes 103.00 under the prior bar's 104.50 low -> 10:20 open."""
    t = _cell(study, "R3", "E2")["R3_POS"]
    assert t.exit_reason == "trail_next_open"
    assert bt.from_mfo(t.exit_mfo) == "10:20"
    assert t.exit_px == pytest.approx(102.0)


def test_e2_ignores_5m_bars_at_or_before_the_signal_bar():
    """The signal bar itself can close below its own predecessor's low; that is not a trail exit."""
    bars = ([55, 56, 57], [100.0, 100.0, 100.0], [100.0] * 3, [100.0] * 3, [100.0] * 3)
    trail = [(10, 54, 1.0, 999.0), (11, 59, 100.0, 100.0)]          # bucket 10 IS the signal bar
    got = bt.simulate_e2(bars, trail, entry_mfo=55, signal_bk=10, squareoff_mfo=57)
    assert got.reason == "squareoff_1515"


# ============================================================ 6. the 15:15 squareoff
def test_the_1515_squareoff_uses_the_last_close_at_or_before_1515_and_ignores_the_later_tape(study):
    """R2_POS is flat at 103.00 all afternoon and prints 200.00 after 15:15. Both exits must book
    103.00 at 15:15: the 200.00 tape is unreachable, and so is E1's 105.00 target."""
    for style in bt.EXITS:
        t = _cell(study, "R2", style)["R2_POS"]
        assert t.exit_reason == "squareoff_1515"
        assert t.exit_mfo == SQUAREOFF_MFO and bt.from_mfo(t.exit_mfo) == "15:15"
        assert t.exit_px == pytest.approx(103.0)
        assert t.gross_pct == pytest.approx(0.0, abs=1e-9)
        assert t.net_pct == pytest.approx(-_cost(103.0), abs=1e-12)
    assert _cell(study, "R2", "E1")["R2_POS"].target_px == pytest.approx(105.0)


def test_the_squareoff_bar_is_the_last_bar_at_or_before_1515_when_1515_itself_is_missing():
    bars = ([55, 358, 361], [100.0] * 3, [100.0] * 3, [100.0] * 3, [100.0, 110.0, 200.0])
    got = bt.simulate_e1(bars, entry_mfo=55, stop_px=99.0, target_r_multiple=2.0,
                         squareoff_mfo=SQUAREOFF_MFO)
    assert (got.reason, got.exit_px, got.exit_mfo) == ("squareoff_1515", 110.0, 358)


# ============================================================ 7. cost application
def test_cost_is_one_mis_round_trip_plus_one_tick_of_slippage_each_side():
    cm = CostModel.from_config()
    entry = 102.0
    rt = float(cm.breakeven_pct(Decimal(bt.PARAMS["reference_notional_inr"]), "MIS"))
    tick = float(Decimal(bt.PARAMS["tick_size_inr"]))
    assert bt.trade_cost_pct(cm, bt.PARAMS, entry) == pytest.approx(rt + 2.0 * tick / entry * 100.0,
                                                                   abs=1e-12)
    assert bt.PARAMS["product"] == "MIS"
    assert rt < float(cm.breakeven_pct(Decimal(bt.PARAMS["reference_notional_inr"]), "CNC"))
    # slippage is price-relative: a cheap stock pays a bigger percentage for the same tick
    assert bt.trade_cost_pct(cm, bt.PARAMS, 50.0) > bt.trade_cost_pct(cm, bt.PARAMS, 5000.0)


def test_cost_is_numerically_identical_to_the_tdc_harnesss_cost():
    """The sibling study's cost function, reused rather than re-derived: the two must agree."""
    cm = CostModel.from_config()
    for px in (37.5, 102.0, 1234.56, 4000.0):
        assert bt.trade_cost_pct(cm, bt.PARAMS, px) == pytest.approx(
            tdc.trade_cost_pct(cm, tdc.PARAMS, px), abs=1e-12
        )


def test_net_is_gross_minus_exactly_one_round_trip_per_trade(study):
    _doc, trades = study
    for cell in trades.values():
        for t in cell:
            assert t.net_pct == pytest.approx(t.gross_pct - t.cost_pct, abs=1e-12)
            assert t.cost_pct == pytest.approx(_cost(t.entry_px), abs=1e-12)


# ============================================================ 8. splits and the boolean
def _mk(**kw) -> bt.Trade:
    base = dict(
        rule="R1", exit_style="E1", symbol="X", d=D, signal_bk=10, signal_mfo=54, entry_mfo=55,
        entry_px=100.0, exit_px=100.0, exit_mfo=360, exit_reason="squareoff_1515", gross_pct=0.0,
        cost_pct=0.0, net_pct=0.0, vol_ratio=2.0, stop_px=99.0, target_px=102.0,
        stop_gap_through=False, index_ret_pct=0.5, index_is_real=True, breadth=0.2,
        catalyst_at_signal=False, catalyst_split_covered=False,
    )
    base.update(kw)
    return bt.Trade(**base)


def test_breadth_split_partitions_on_the_1100_share_above_the_opening_range_high():
    cells = bt.split_cells([_mk(breadth=0.49), _mk(breadth=0.50), _mk(breadth=0.80)], bt.PARAMS)
    assert [t.breadth for t in cells[bt.SPLIT_BREADTH_HIGH]] == [0.50, 0.80]   # >= is inclusive
    assert [t.breadth for t in cells[bt.SPLIT_BREADTH_LOW]] == [0.49]


def test_index_regime_split_partitions_on_the_index_return_at_the_signal_minute():
    cells = bt.split_cells([_mk(index_ret_pct=-0.1), _mk(index_ret_pct=0.0), _mk(index_ret_pct=0.3)],
                           bt.PARAMS)
    assert len(cells[bt.SPLIT_INDEX_UP]) == 2                         # 0.0 counts as "up" (>= 0)
    assert len(cells[bt.SPLIT_INDEX_DOWN]) == 1


def test_catalyst_cells_only_hold_trades_inside_the_feeds_coverage():
    trades = [
        _mk(catalyst_split_covered=False, catalyst_at_signal=False),
        _mk(catalyst_split_covered=True, catalyst_at_signal=True),
        _mk(catalyst_split_covered=True, catalyst_at_signal=False),
    ]
    cells = bt.split_cells(trades, bt.PARAMS)
    assert len(cells[bt.SPLIT_CATALYST_TRUE]) == 1
    assert len(cells[bt.SPLIT_CATALYST_FALSE]) == 1                   # the uncovered trade is in neither
    assert len(cells[bt.SPLIT_ALL]) == 3
    assert len(cells[bt.SPLIT_REAL_INDEX]) == 3


def test_promotable_is_the_briefs_four_part_boolean_and_nothing_else():
    p = {**bt.PARAMS, "promote_min_n": 3}
    losers = bt.metrics([_mk(net_pct=-1.0 + (i % 3) * 0.1) for i in range(20)], p)
    assert losers["mean_net_pct"] < 0 and losers["promotable"] is False
    thin = bt.metrics([_mk(net_pct=1.0), _mk(net_pct=1.1)], {**p, "promote_min_n": 200})
    assert thin["n"] == 2 and thin["promotable"] is False             # the n floor alone is decisive


def test_metrics_report_gross_and_cost_alongside_net(study):
    doc, _trades = study
    s = doc["results"]["R1|E1"]["splits"][bt.SPLIT_ALL]
    for key in ("n", "mean_net_pct", "median_net_pct", "win_rate", "t_stat",
                "mean_gross_pct", "mean_cost_pct", "promotable"):
        assert key in s
    assert "positive_share" in s["cpcv"]
    assert s["mean_net_pct"] == pytest.approx(s["mean_gross_pct"] - s["mean_cost_pct"], abs=1e-3)


def test_cpcv_degrades_honestly_on_a_one_session_population(study):
    doc, _trades = study
    cv = doc["results"]["R1|E1"]["splits"][bt.SPLIT_ALL]["cpcv"]
    assert cv["n_obs_sessions"] == 1 and cv["n_splits"] == 0 and cv["positive_share"] is None
    assert doc["results"]["R1|E1"]["splits"][bt.SPLIT_ALL]["promotable"] is False


def test_purged_kfold_fallback_honours_purge_and_embargo():
    splits = bt._purged_kfold_splits(120, n_folds=6, purge=5, embargo=5)
    assert len(splits) == 6
    for train, test in splits:
        lo, hi = int(test.min()), int(test.max())
        assert not set(train) & set(test)
        assert all(j < lo - 5 or j > hi + 5 for j in train)


# ============================================================ 9. the CLI: report, JSON, refusal
def test_main_writes_json_and_prints_an_ascii_table(db_file, tmp_path, capsys):
    out = tmp_path / "results" / "candles.json"
    rc = bt.main(["--db", str(db_file), "--out", str(out), "--start", str(D), "--end", str(D)])
    assert rc == 0
    doc = json.loads(out.read_text(encoding="utf-8"))
    assert doc["meta"]["product"] == "MIS"
    assert "OPEN of the 1m bar at signal.last_minute + 1" in doc["meta"]["entry_convention"]
    assert "next 1m OPEN" in doc["meta"]["exit_conventions"]["E2"]
    assert len(doc["results"]) == 10
    assert doc["results"]["R1|E1"]["example_trades"]                  # the audit sample is on record

    text = capsys.readouterr().out
    assert text.isascii()                                             # the Windows console is cp1252
    assert text.index("STEP 1 - SAMPLE SHAPE") < text.index("STEP 2 - RESULTS")
    assert "promotable=True in NO rule x exit x split cell." in text
    assert str(out) in text


def test_refuses_a_missing_database_file(tmp_path, capsys):
    rc = bt.main(["--db", str(tmp_path / "nope.duckdb"), "--out", str(tmp_path / "x.json")])
    assert rc == 2
    err = capsys.readouterr().err
    assert "REFUSING TO RUN" in err and "no such database file" in err
    assert not (tmp_path / "x.json").exists()


def test_refuses_a_locked_database_file(db_file, tmp_path, module_clock, capsys):
    """A running engine holds market.duckdb read-write; DuckDB then refuses a read-only attach."""
    holder = MarketStore(db_file, tmp_path / "parquet2", module_clock).open()
    try:
        rc = bt.main(["--db", str(db_file), "--out", str(tmp_path / "y.json")])
    finally:
        holder.close()
    assert rc == 2
    err = capsys.readouterr().err
    assert "REFUSING TO RUN" in err and "cannot open read-only" in err and "mt-engine" in err
    assert not (tmp_path / "y.json").exists()
