"""``scripts/swing_timing_study.py``: admission, candle entry and exit, add-ons, controls, bootstraps and labels.

Every series and every candle flag is synthetic; nothing reads a research snapshot or the real candle rules.
"""

from __future__ import annotations

import importlib.util
import sys
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest

_SCRIPTS = Path(__file__).resolve().parents[2] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))
_spec = importlib.util.spec_from_file_location("mt_swing_timing_study", _SCRIPTS / "swing_timing_study.py")
sts = importlib.util.module_from_spec(_spec)
sys.modules["mt_swing_timing_study"] = sts
_spec.loader.exec_module(sts)

import pool_widening_common as pw  # noqa: E402
from engine.core.clock import IST  # noqa: E402

rc, pws, bb = sts.rc, sts.pws, sts.bb
FEES = Decimal("0.30")


def _weekdays(start: date, n: int) -> list[date]:
    out, d = [], start
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d)
        d += timedelta(days=1)
    return out


DATES = _weekdays(date(2024, 1, 1), 70)


def _series(sym: str, *, price: float = 100.0, vol: float = 1_000_000.0, n: int = 70,
            dates: list[date] | None = None) -> sts.Series:
    o, h, lo, c = (np.full(n, price) for _ in range(4))
    return sts.Series(sym, (dates or DATES)[:n], o, h * 1.01, lo * 0.99, c, np.full(n, vol), [])


def _gappy(sym: str, missing: set[int], **kw) -> sts.Series:
    keep = [d for i, d in enumerate(DATES) if i not in missing]
    return _series(sym, n=len(keep), dates=keep, **kw)


class FakePat:
    """``Patterns.hits`` over a fixed ``{(symbol, own bar): rules}`` table."""

    def __init__(self, flags: dict[tuple[str, int], tuple[str, ...]] | None = None) -> None:
        self.flags = flags or {}

    def hits(self, sym, j, names):
        on = self.flags.get((sym, j), ())
        return tuple(n for n in names if n in on)

    def masked(self, sym, j, names):
        return 0


SETS = {"bullish": ("b1", "b2", "b9"), "bearish": ("x1", "x2", "x3"), "five_entry": ("b9",), "five_exit": ("x3",)}


def _adm(s: sts.Series, grid, i: int, leg: str = "ins", sig=None) -> sts.Admitted:
    code, rank, liq = grid.band_at(s.symbol, s.dates[i])
    return sts.Admitted(leg, s.symbol, s, i, rc._atr(s), (pw.BANDS[code], rank, liq), sig)


def _sig(s: sts.Series, idx: int, level: float):
    return bb.Signal(s.symbol, idx, s.dates[idx], level, level, 0.01, 0.01)


def test_registered_constants():
    assert sts.FAMILY == "edge/swing_timing" and sts.FAMILY_N == 7 and len(sts.TRIALS) == 7
    assert sts.TRIALS == ("brk20_addon", "brk20_candle_entry", "ins_candle_entry", "brk20_candle_exit",
                          "ins_candle_exit", "five_entry", "five_exit")
    assert sts.FORWARD_SESSIONS == {"brk20": 25, "ins": 24} and sts.ENTRY_SCAN_SESSIONS == 5
    assert sts.POPULATION == {"brk20": (pw.C0, pw.W1), "ins": (pw.C0,)}
    assert sts.BOOTSTRAP_SEED == sts.CONTROL_SEED == 20261111 and sts.MATCHED_SEED == (20261111, 1)
    assert sts.CANDLE_REF_BARS == 10 and sts.FIRST_POSITIONS_LABEL == "all kept brk20 trades (first positions)"
    assert sts.BOOTSTRAP_REPS == 2000 and sts.CONTROL_DRAWS == 1000 and sts.CONTROL_PCT == 95.0
    assert sts.MIN_N == 100 and sts.HELD_OUT_FROM == date(2026, 1, 1)
    assert sts.EXIT_SESSIONS == {"brk20": 20, "ins": 19}
    assert sts.TIMING_TRIALS["five_entry"][1] == sts.TIMING_TRIALS["five_exit"][1] == ("brk20", "ins")


def test_candle_sets_refuse_the_wrong_shape():
    bull = tuple(f"b{i}" for i in range(21))
    bear = tuple(f"x{i}" for i in range(17))
    ok = SimpleNamespace(BULLISH=bull, BEARISH=bear, FIVE_ENTRY=bull[:2], FIVE_EXIT=bear[:3])
    assert sts._candle_sets(ok)["five_exit"] == bear[:3]
    with pytest.raises(rc.Refusal, match="registered"):
        sts._candle_sets(SimpleNamespace(BULLISH=bull[:20], BEARISH=bear, FIVE_ENTRY=bull[:2], FIVE_EXIT=bear[:3]))
    with pytest.raises(rc.Refusal, match="inside"):
        sts._candle_sets(SimpleNamespace(BULLISH=bull, BEARISH=bear, FIVE_ENTRY=(bull[0], "zz"), FIVE_EXIT=bear[:3]))


def test_patterns_pack_flags_from_keep_from():
    s = _series("A")
    seen = []

    def detect(o, h, lo, c, names):
        seen.append(names)
        f = {n: np.zeros(len(o), dtype=bool) for n in names}
        f["p1"][[5, 40]] = True
        f["p2"][40] = True
        return f

    pat = sts.Patterns({"A": s}, detect, ("p1", "p2"), keep_from=DATES[10])
    assert pat.hits("A", 40, ("p2", "p1")) == ("p2", "p1")
    assert pat.hits("A", 40, ("p1",)) == ("p1",) and pat.hits("A", 41, ("p1", "p2")) == ()
    with pytest.raises(IndexError):
        pat.hits("A", 5, ("p1",))                                   # before keep_from: never read
    pat.hits("A", 12, ("p1",))
    assert len(seen) == 1                                           # detected once per stock
    bad = sts.Patterns({"A": s}, lambda o, h, lo, c, names: {n: np.zeros(3, bool) for n in names}, ("p1",))
    with pytest.raises(ValueError, match="flags"):
        bad.hits("A", 60, ("p1",))


def _split_series(sym: str, ex_idx: int) -> sts.Series:
    s = _series(sym)
    for arr in (s.open, s.high, s.low, s.close):
        arr[ex_idx:] /= 2.0                                         # a 1:2 split, unadjusted
    return s


def test_ex_date_window_spans_the_reference_bars_through_completion():
    dates = DATES[:40]
    w1 = sts.ex_date_window(dates, [DATES[20]], 1)                  # reads bars d - 10 .. d
    assert np.flatnonzero(w1).tolist() == list(range(20, 30))       # d - 10 < 20 <= d
    w3 = sts.ex_date_window(dates, [DATES[20]], 3)                  # reads bars d - 12 .. d
    assert np.flatnonzero(w3).tolist() == list(range(20, 32))
    early = sts.ex_date_window(dates, [DATES[5]], 1)               # first bar would be before bar 0
    assert not early[:10].any() and early[10:15].all() and not early[15:].any()
    assert not sts.ex_date_window(dates, [], 2).any()
    assert not sts.ex_date_window(dates, [DATES[45]], 2).any()      # after the series


def test_patterns_clear_flags_across_a_structural_ex_date():
    s = _split_series("A", 20)
    on = {"p1": [20, 25, 30], "p3": [31, 33]}

    def detect(o, h, lo, c, names):
        f = {n: np.zeros(len(o), dtype=bool) for n in names}
        for n, idx in on.items():
            f[n][idx] = True
        return f

    pat = sts.Patterns({"A": s}, detect, ("p1", "p3"), lengths={"p1": 1, "p3": 3}, structural={"A": [DATES[20]]},
                       keep_from=DATES[0])
    assert [pat.hits("A", j, ("p1", "p3")) for j in (20, 25, 30, 31, 33)] == [(), (), ("p1",), (), ("p3",)]
    assert [pat.masked("A", j, ("p1", "p3")) for j in (20, 25, 30, 31, 33)] == [1, 1, 0, 1, 0]
    assert pat.masked("A", 31, ("p1",)) == 0
    plain = sts.Patterns({"A": s}, detect, ("p1", "p3"), keep_from=DATES[0])        # no structural table
    assert plain.hits("A", 20, ("p1",)) == ("p1",) and plain.masked("A", 20, ("p1",)) == 0
    with pytest.raises(ValueError, match="candle count"):
        sts.Patterns({"A": s}, detect, ("p1", "p3"), lengths={"p1": 1}, structural={"A": [DATES[20]]})


def test_scans_count_masked_flags(monkeypatch):
    s = _split_series("A", 18)
    g = _world(monkeypatch, s)

    def detect(o, h, lo, c, names):
        f = {n: np.zeros(len(o), dtype=bool) for n in names}
        f["p1"][[26, 27, 28, 40]] = True
        return f

    pat = sts.Patterns({"A": s}, detect, ("p1",), lengths={"p1": 1}, structural={"A": [DATES[18]]},
                       keep_from=DATES[0])
    counts: dict[str, int] = {}
    e = sts.candle_entry(_adm(s, g, 25), g, pat, ("p1",), counts)
    assert e.day == 28 and e.event.fill_idx == 29 and counts == {"masked_ex_date": 2}   # 26 and 27 read bar 18
    c = sts.live_candidate(_adm(s, g, 25), g, {})
    counts = {}
    assert sts.candle_exit_day(c, pat, ("p1",), counts) == (28, ("p1",)) and counts == {"masked_ex_date": 2}


# ----------------------------------------------------------------------------- admission
@pytest.mark.parametrize("leg, span", [("brk20", 25), ("ins", 24)])
def test_admission_window_runs_through_y_plus_5_plus_the_hold(monkeypatch, leg, span):
    monkeypatch.setattr(pw, "CORE_RANK", 5)
    s = _series("A")
    g = pws.Grid.build({"A": s}, {})
    atr = rc._atr(s)
    counts: dict[str, int] = {}
    assert sts._gate(leg, "A", s, 70 - span - 1, g, atr, counts) is not None    # y + span is the last session
    assert sts._gate(leg, "A", s, 70 - span, g, atr, counts) is None
    assert counts == {"short_forward_window": 1}
    # the latest candle fill (y + 5) with its exit at the leg's hold is the last session the window keeps
    assert 5 + sts.EXIT_SESSIONS[leg] == span


def test_admission_window_counts_market_sessions():
    gap = _gappy("G", {50, 51})                                     # 68 own bars: own-bar counting would drop 45
    s = _series("F")
    g = pws.Grid.build({"F": s, "G": gap}, {})
    counts: dict[str, int] = {}
    assert sts._gate("ins", "G", gap, 45, g, rc._atr(gap), counts) is not None   # grid 45 + 24 = 69 < 70
    assert sts._gate("ins", "G", gap, 46, g, rc._atr(gap), counts) is None
    assert counts == {"short_forward_window": 1}


def test_populations(monkeypatch):
    monkeypatch.setattr(pw, "CORE_RANK", 1)
    big = _series("BIG", vol=2_000_000.0)                           # Rs 20 cr: C0
    mid = _series("MID", vol=600_000.0)                             # Rs 6 cr past the core: W1
    small = _series("SMALL", vol=300_000.0)                         # Rs 3 cr: W2
    g = pws.Grid.build({"BIG": big, "MID": mid, "SMALL": small}, {})
    got = {}
    for leg in sts.LEGS:
        counts: dict[str, int] = {}
        got[leg] = [s.symbol for s in (big, mid, small)
                    if sts._gate(leg, s.symbol, s, 25, g, rc._atr(s), counts) is not None]
        got[leg + "_counts"] = counts
    assert got["brk20"] == ["BIG", "MID"] and got["brk20_counts"] == {"out_of_population": 1}
    assert got["ins"] == ["BIG"] and got["ins_counts"] == {"out_of_population": 2}


class _Store:
    def get_insider_trades(self, symbol):
        return []


def test_ins_admitted_uses_the_shared_window_and_defers_the_fill(monkeypatch):
    monkeypatch.setattr(pw, "CORE_RANK", 5)
    monkeypatch.setattr(sts.rc, "store_reader", lambda conn: _Store())
    monkeypatch.setattr(sts.es, "insider_buy_events", lambda sessions, filings, threshold: [25, 46])
    full = _series("F")
    gap = _gappy("G", {26})                                         # no bar on y + 1: the live rule's drop
    g = pws.Grid.build({"F": full, "G": gap}, {})
    adm, counts = sts.ins_admitted(None, {"F": full, "G": gap}, g, {})
    assert [(a.symbol, a.idx) for a in adm] == [("F", 25), ("G", 25)]
    assert counts == {"signals_in_window": 4, "short_forward_window": 2}
    live_counts: dict[str, int] = {}
    cands = [sts.live_candidate(a, g, live_counts) for a in adm]
    assert cands[0].event.fill_idx == 26 and cands[0].exit_date == DATES[26 + 19]
    assert cands[1] is None and live_counts == {"no_fill_session": 1}


def test_brk20_admitted_and_live_limit(monkeypatch):
    monkeypatch.setattr(pw, "CORE_RANK", 5)
    s = _series("A")
    monkeypatch.setattr(sts.bb, "signals_for", lambda s, *a, **k: [_sig(s, 25, 99.5), _sig(s, 46, 99.5)])
    monkeypatch.setattr(sts.rf, "closes_through", lambda index, d: d)
    monkeypatch.setattr(sts.rf, "regime_on", lambda d: True if d == DATES[25] else None)
    g = pws.Grid.build({"A": s}, {})
    adm, counts = sts.brk20_admitted({"A": s}, g, {}, {}, None)
    assert [a.idx for a in adm] == [25] and counts == {"signals_in_window": 2, "regime_undefined": 1}
    c = sts.live_candidate(adm[0], g, {})
    assert c.event.fill_idx == 26 and float(c.event.fill_px) == 99.5 and c.exit_date == DATES[46]


# ----------------------------------------------------------------------------- candle entry
def _world(monkeypatch, *series):
    monkeypatch.setattr(pw, "CORE_RANK", 5)
    return pws.Grid.build({s.symbol: s for s in series}, {})


def test_candle_entry_takes_the_first_completing_day(monkeypatch):
    s = _series("A")
    g = _world(monkeypatch, s)
    a = _adm(s, g, 25)
    pat = FakePat({("A", 27): ("b1", "b2"), ("A", 28): ("b1",)})
    counts: dict[str, int] = {}
    e = sts.candle_entry(a, g, pat, SETS["bullish"], counts)
    assert e.event.fill_idx == 28 and e.event.fill_px == rc.px(s.open[28]) and e.patterns == ("b1", "b2")
    assert e.exit_idx == 28 + 19 and e.event.signal_idx == 25 and counts == {}
    e_brk = sts.candle_entry(_adm(s, g, 25, "brk20"), g, pat, SETS["bullish"], {})
    assert e_brk.exit_idx == 28 + 20                                # the leg's hold, from the fill
    on_y = sts.candle_entry(a, g, FakePat({("A", 25): ("b9",)}), SETS["bullish"], {})
    assert on_y.event.fill_idx == 26                                # a pattern on y buys open(y+1)
    assert sts.candle_entry(a, g, pat, SETS["five_entry"], counts) is None   # other rules do not count
    assert counts == {"no_pattern": 1}


def test_candle_entry_window_is_y_to_y_plus_4(monkeypatch):
    s = _series("A")
    g = _world(monkeypatch, s)
    a = _adm(s, g, 25)
    counts: dict[str, int] = {}
    assert sts.candle_entry(a, g, FakePat({("A", 29): ("b1",)}), SETS["bullish"], counts).event.fill_idx == 30
    assert sts.candle_entry(a, g, FakePat({("A", 30): ("b1",)}), SETS["bullish"], counts) is None
    assert sts.candle_entry(a, g, FakePat({("A", 24): ("b1",)}), SETS["bullish"], counts) is None
    assert counts == {"no_pattern": 2}


def test_candle_entry_skips_a_day_without_a_next_session_bar(monkeypatch):
    gap = _gappy("G", {28})                                         # out of EQ on y + 3
    g = _world(monkeypatch, _series("F"), gap)
    a = _adm(gap, g, 25)
    # own 27 = DATES[27]: its next market session DATES[28] has no bar; own 28 = DATES[29] (y + 4)
    counts: dict[str, int] = {}
    e = sts.candle_entry(a, g, FakePat({("G", 27): ("b1",), ("G", 28): ("b2",)}), SETS["bullish"], counts)
    assert e.event.fill_date == DATES[30] and e.patterns == ("b2",)
    assert sts.candle_entry(a, g, FakePat({("G", 27): ("b1",)}), SETS["bullish"], counts) is None
    assert counts == {"no_fill_session": 1}
    assert [g.own_pos["G"][d] for d in sts.entry_days(a, g)] == [25, 26, 29]


def test_candle_entry_applies_the_lock_check(monkeypatch):
    s = _series("A")
    s.high[28] = s.low[28] = 100.0
    g = _world(monkeypatch, s)
    counts: dict[str, int] = {}
    assert sts.candle_entry(_adm(s, g, 25), g, FakePat({("A", 27): ("b1",), ("A", 28): ("b1",)}),
                            SETS["bullish"], counts) is None        # the first day decides; no fallback
    assert counts == {"locked_fill_session": 1}


# ----------------------------------------------------------------------------- candle exit + open pricing
def _live(s, g, sig=25, structural=None) -> sts.STrade:
    c = sts.live_candidate(_adm(s, g, sig), g, {})
    return sts.walk_close(c, g, structural or {}, FEES, {})


def test_candle_exit_scans_fill_bar_through_the_bar_before_the_exit(monkeypatch):
    s = _series("A")
    g = _world(monkeypatch, s)
    c = _live(s, g).cand                                           # fill 26, exit 45
    assert sts.candle_exit_day(c, FakePat({("A", 26): ("x1",)}), SETS["bearish"]) == (26, ("x1",))
    assert sts.candle_exit_day(c, FakePat({("A", 44): ("x2", "x3")}), SETS["bearish"]) == (44, ("x2", "x3"))
    assert sts.candle_exit_day(c, FakePat({("A", 45): ("x1",), ("A", 25): ("x1",)}), SETS["bearish"]) is None
    assert sts.candle_exit_day(c, FakePat({("A", 30): ("x1",)}), SETS["five_exit"]) is None


def test_candle_exit_sells_at_the_next_open_and_keeps_the_live_exit_otherwise(monkeypatch):
    s = _series("A")
    s.open[31] = 104.0
    g = _world(monkeypatch, s)
    t = _live(s, g)
    obench = sts.OpenBenchmark(g)
    counts: dict[str, int] = {}
    assert sts.candle_exit(t, FakePat(), SETS["bearish"], g, {}, FEES, obench, counts) is t
    v = sts.candle_exit(t, FakePat({("A", 30): ("x1",)}), SETS["bearish"], g, {}, FEES, obench, counts)
    assert v.exit_kind == "open" and v.exit_date == DATES[31] and v.exit_late == 0 and v.exit_day == 5
    assert v.gross == pytest.approx(4.0) and v.patterns == ("x1",) and v.cand is t.cand
    assert v.benchmark == pytest.approx(4.0) and v.excess == pytest.approx(0.0)   # own stock is the band
    assert counts == {"early_exit": 1}


def test_candle_exit_late_when_the_next_bar_skips_sessions(monkeypatch):
    gap = _gappy("G", {31, 32})                                     # own 31 = DATES[33]
    g = _world(monkeypatch, _series("F"), gap)
    t = _live(gap, g)
    counts: dict[str, int] = {}
    v = sts.candle_exit(t, FakePat({("G", 30): ("x1",)}), SETS["bearish"], g, {}, FEES, sts.OpenBenchmark(g), counts)
    assert v.exit_date == DATES[33] and v.exit_late == 2 and v.exit_day == 33 - 26
    assert counts == {"early_exit": 1, "late_open_exit": 1}


def test_candle_exit_voids_on_a_structural_ex_date(monkeypatch):
    s = _series("A")
    g = _world(monkeypatch, s)
    t = _live(s, g)
    obench = sts.OpenBenchmark(g)
    pat = FakePat({("A", 30): ("x1",)})
    counts: dict[str, int] = {}
    assert sts.candle_exit(t, pat, SETS["bearish"], g, {"A": [DATES[31]]}, FEES, obench, counts) is None
    assert counts == {"early_exit": 1, "void_ca": 1}
    assert sts.candle_exit(t, pat, SETS["bearish"], g, {"A": [DATES[26], DATES[32]]}, FEES, obench, {}) is not None


def test_open_exit_prices_like_exit_sim(monkeypatch):
    s = _series("A")
    s.open[26] = 99.97
    s.open[40] = s.close[40] = 101.13
    g = _world(monkeypatch, s)
    c = sts.live_candidate(_adm(s, g, 25), g, {})
    v = sts.open_exit(c, 39, g, {}, FEES, sts.OpenBenchmark(g))
    # (101.13 - 99.97) / 99.97 x 100 = 1.16034810443..., quantized to 6 dp half-up
    assert v.gross == 1.160348 and v.net == pytest.approx(1.160348 - 0.34) and v.cost == pytest.approx(0.34)
    assert Decimal(str(v.net)) == Decimal("1.160348") - FEES - pws.BAND_SPREAD_PCT[pw.C0]
    # the same bar as a close exit through rc.walk gives the same gross and net
    same = sts.walk_close(pws.Candidate("ins", c.event, c.band, c.rank, c.liquidity, exit_idx=40), g, {}, FEES, {})
    assert (same.gross, same.net) == (v.gross, v.net)
    with pytest.raises(ValueError, match="outside the hold"):
        sts.open_exit(c, c.exit_idx, g, {}, FEES, sts.OpenBenchmark(g))


def test_open_benchmark_runs_open_to_open_in_the_band_of_y(monkeypatch):
    monkeypatch.setattr(pw, "CORE_RANK", 3)
    a, b, c = _series("A"), _series("B"), _series("C")
    d = _series("D", vol=300_000.0)                                 # 4th: W2
    a.open[45], b.open[45], c.open[45], d.open[45] = 110.0, 90.0, 150.0, 200.0
    a.close[45] = 500.0                                             # a close benchmark would read this
    g = pws.Grid.build({"A": a, "B": b, "C": c, "D": d}, {"C": [DATES[30]]})
    ob = sts.OpenBenchmark(g)
    mean, n = ob(pws.BAND_CODE[pw.C0], DATES[25], DATES[26], DATES[45])
    assert n == 2 and mean == pytest.approx(0.0)                    # A +10, B -10; C has an ex-date; D is W2
    assert ob(pws.BAND_CODE[pw.C0], DATES[25], DATES[26], DATES[45]) == (mean, n)   # cached
    assert ob(pws.BAND_CODE[pw.C0], DATES[25], DATES[26], DATES[29]) == (pytest.approx(0.0), 3)
    e = _gappy("E", {45})                                           # no bar on the exit date
    g2 = pws.Grid.build({"A": a, "B": b, "E": e}, {})
    assert sts.OpenBenchmark(g2)(pws.BAND_CODE[pw.C0], DATES[25], DATES[26], DATES[45])[1] == 2


# ----------------------------------------------------------------------------- add-on
def _cand(s, sig: int, fill: int, exit_idx: int) -> pws.Candidate:
    return pws.Candidate("brk20", rc.Event(s.symbol, s, sig, fill, rc.px(100.0), 1.0), pw.C0, 1, 1e8,
                         exit_idx=exit_idx)


def test_addons_held_at_the_signal_first_by_fill():
    a, b = _series("A"), _series("B")
    k1 = _cand(a, 5, 8, 28)                                         # its limit rests 3 sessions, fills on bar 8
    resting = _cand(a, 6, 9, 29)                                    # signalled while k1's order still rested
    slow = _cand(a, 10, 15, 35)                                     # qualifies, fills on bar 15
    fast = _cand(a, 12, 13, 33)                                     # signalled later, fills first: the add-on
    late = _cand(a, 20, 28, 48)                                     # fills on k1's exit session
    k2 = _cand(a, 28, 29, 49)                                       # signal on k1's exit date: kept
    tie_first = _cand(a, 30, 32, 52)                                # ties on fill: the earlier signal wins
    tie_second = _cand(a, 31, 32, 52)
    other = _cand(b, 10, 11, 31)
    cands = [tie_second, other, late, k2, fast, slow, resting, tie_first, k1]
    counts: dict[str, int] = {}
    kept = pws.one_position_per_symbol(cands, counts)
    assert kept == [k1, k2, other] and counts == {"held_overlap": 6}
    got = sts.addons(cands, kept, counts)
    assert got == [fast, tie_first]
    assert counts == {"held_overlap": 6, "signal_before_first_fill": 1, "filled_after_exit": 1, "extra_repeat": 2}


def test_addon_signalled_on_the_first_fill_session_qualifies():
    a = _series("A")
    k = _cand(a, 5, 8, 28)
    on_fill = _cand(a, 8, 9, 29)
    before = _cand(a, 7, 8, 28)                                     # fills on k's fill bar, signalled before it
    for c, want, key in ((on_fill, [on_fill], None), (before, [], "signal_before_first_fill")):
        counts: dict[str, int] = {}
        kept = pws.one_position_per_symbol([k, c], counts)
        assert sts.addons([k, c], kept, counts) == want
        assert counts.get(key, 0) == (1 if key else 0)


# ----------------------------------------------------------------------------- controls
def test_draw_means_is_deterministic_and_moves_only_the_redrawn():
    opts = [np.array([1.0, 2.0]), np.array([10.0, 20.0])]
    m1 = sts.draw_means([], opts)
    assert m1.shape == (sts.CONTROL_DRAWS,) and np.array_equal(m1, sts.draw_means([], opts))
    assert not np.array_equal(m1, sts.draw_means([], opts, seed=1))
    assert set(np.round(m1, 9)) <= {5.5, 6.0, 10.5, 11.0} and len(set(np.round(m1, 9))) == 4
    fixed_only = sts.draw_means([3.0, None, 5.0], [np.array([7.0])])
    assert np.allclose(fixed_only, 5.0)                             # (3 + 5 + 7) / 3; None is out
    assert np.allclose(sts.draw_means([2.0], [np.array([np.nan])]), 2.0)     # a void pick drops from the draw
    assert np.isnan(sts.draw_means([], [np.array([np.nan])])).all()
    assert np.allclose(sts.draw_means([1.0, 3.0], []), 2.0)
    with pytest.raises(ValueError):
        sts.draw_means([], [np.array([])])


def _entry_world(monkeypatch, pattern_day: int = 25):
    a_, b_ = _series("A"), _series("B")
    a_.close[49] = 120.0                                            # only the day-29 entry (fill 30) exits here
    g = _world(monkeypatch, a_, b_)
    adm = _adm(a_, g, 25)
    t = sts.walk_close(sts.candle_entry(adm, g, FakePat({("A", pattern_day): ("b1",)}), SETS["bullish"], {}),
                       g, {}, FEES, {})
    return a_, g, adm, t


def test_entry_table_keeps_failing_and_void_days_eligible(monkeypatch):
    s, g, a, t = _entry_world(monkeypatch)
    tab = sts.entry_table(t, a, g, {}, FEES)
    assert tab.offsets.tolist() == [0, 1, 2, 3, 4] and tab.own == 0 and t.trigger == 25
    # fills 26..30 exit 45..49; B flat in the band: excess = gross / 2
    assert tab.vals == pytest.approx([0.0, 0.0, 0.0, 0.0, 10.0])
    voided = sts.entry_table(t, a, g, {"A": [DATES[48]]}, FEES)     # inside the holds of the last two fills
    assert voided.offsets.tolist() == [0, 1, 2, 3, 4]
    assert np.isnan(voided.vals).tolist() == [False, False, False, True, True]
    s.high[28] = s.low[28] = 100.0                                  # the day-27 entry is locked: void, still eligible
    locked = sts.entry_table(t, a, g, {}, FEES)
    assert locked.offsets.tolist() == [0, 1, 2, 3, 4] and np.isnan(locked.vals).tolist() == [False, False, True,
                                                                                             False, False]


def test_entry_table_offsets_skip_days_without_a_next_session_bar(monkeypatch):
    gap = _gappy("G", {28})
    g = _world(monkeypatch, _series("F"), gap)
    a = _adm(gap, g, 25)
    t = sts.walk_close(sts.candle_entry(a, g, FakePat({("G", 28): ("b1",)}), SETS["bullish"], {}), g, {}, FEES, {})
    tab = sts.entry_table(t, a, g, {}, FEES)
    assert tab.offsets.tolist() == [0, 1, 4] and tab.own == 4       # own bar 28 is market session y + 4


def test_uniform_entry_control_p95_is_pinned(monkeypatch):
    _, g, a, t = _entry_world(monkeypatch)
    ctrl = sts.entry_control([t], {("ins", "A", 25): a}, g, {}, FEES, t.excess)
    u, m = ctrl["uniform"], ctrl["matched"]
    # each draw picks one of [0, 0, 0, 0, 10] uniformly: 10 in about a fifth of draws, so the 95th percentile is 10
    assert u["p95"] == pytest.approx(10.0) and u["n_redrawn"] == 1 and u["seed"] == 20261111
    assert u["variant_share_of_draws_below"] == 0.0 and u["void_per_draw"] == {"mean": 0.0, "min": 0, "max": 0}
    # one trade: the permutation always returns its own offset
    assert m["p95"] == pytest.approx(0.0) and m["seed"] == [20261111, 1]


def test_matched_draws_permute_the_offsets():
    flat = [sts.Table(np.arange(5), np.arange(5, dtype=float), own) for own in (0, 1, 1, 4)]
    means, void = sts.matched_draws([], flat)
    assert np.allclose(means, 1.5) and not void.any()               # every draw uses the multiset {0, 1, 1, 4}
    two = [sts.Table(np.arange(5), np.arange(5, dtype=float), 0), sts.Table(np.arange(5), 100.0 * np.arange(5), 4)]
    m2, _ = sts.matched_draws([], two)
    assert set(np.round(m2, 9)) == {200.0, 2.0}                     # kept (0 + 400) / 2 or swapped (4 + 0) / 2
    assert np.array_equal(m2, sts.matched_draws([], two)[0])        # seeded
    assert not np.array_equal(m2, sts.matched_draws([], two, seed=(1, 2))[0])


def test_matched_draws_void_an_ineligible_offset_and_keep_untouched_trades():
    tabs = [sts.Table(np.array([0]), np.array([1.0]), 0), sts.Table(np.array([3]), np.array([5.0]), 3)]
    means, void = sts.matched_draws([7.0], tabs)
    swapped = void == 2                                             # each gets an offset it has no bar for
    assert set(void.tolist()) == {0, 2} and swapped.any() and (~swapped).any()
    assert np.allclose(means[~swapped], (7.0 + 1.0 + 5.0) / 3) and np.allclose(means[swapped], 7.0)
    void_val = [sts.Table(np.array([0, 1]), np.array([np.nan, 2.0]), 0)]
    assert np.isnan(sts.matched_draws([], void_val)[0]).all()      # eligible but void: void in the draw
    assert np.allclose(sts.matched_draws([4.0], [])[0], 4.0)


def test_exit_table_and_control_leave_untouched_trades(monkeypatch):
    s, b = _series("A"), _series("B")
    s.open[27:45] = np.linspace(101.0, 118.0, 18)
    g = _world(monkeypatch, s, b)
    t = _live(s, g)
    v = sts.candle_exit(t, FakePat({("A", 30): ("x1",)}), SETS["bearish"], g, {}, FEES, sts.OpenBenchmark(g), {})
    tab = sts.exit_table(v, g, {}, FEES, sts.OpenBenchmark(g))
    assert tab.offsets.tolist() == list(range(19)) and tab.own == 4 and v.trigger == 30
    assert tab.vals[4] == pytest.approx(v.excess)
    untouched = dataclass_replace(t, excess=7.0)
    ctrl = sts.exit_control([v, untouched], g, {}, FEES, sts.OpenBenchmark(g), 1.0)
    assert ctrl["uniform"]["n_redrawn"] == ctrl["matched"]["n_redrawn"] == 1
    assert ctrl["uniform"]["p95"] == pytest.approx(float(np.percentile(sts.draw_means([7.0], [tab.vals]), 95)))
    assert ctrl["matched"]["p95"] == pytest.approx((7.0 + v.excess) / 2)   # one moved trade keeps its own offset


def test_exit_table_offsets_count_market_sessions(monkeypatch):
    gap = _gappy("G", {31, 32})
    g = _world(monkeypatch, _series("F"), gap)
    t = _live(gap, g)
    v = sts.candle_exit(t, FakePat({("G", 31): ("x1",)}), SETS["bearish"], g, {}, FEES, sts.OpenBenchmark(g), {})
    tab = sts.exit_table(v, g, {}, FEES, sts.OpenBenchmark(g))
    assert tab.own == 33 - 26 and 5 not in tab.offsets.tolist() and 6 not in tab.offsets.tolist()


def dataclass_replace(t, **kw):
    import dataclasses
    return dataclasses.replace(t, **kw)


# ----------------------------------------------------------------------------- bootstraps
def _st(excess: float | None, d: date, sym: str = "A") -> sts.STrade:
    return sts.STrade("brk20", pw.C0, sym, d, d, d, 1.0, 0.5, 0.5, None if excess is None else 0.0, 1, excess, 0,
                      False, "close", 20)


def test_joint_bootstrap_sign():
    days = DATES[:40]
    better = [_st(1.0 + 0.1 * (k % 3), d) for k, d in enumerate(days)]
    worse = [_st(-1.0 + 0.1 * (k % 5), d) for k, d in enumerate(days[5:])]
    up = sts.joint_bootstrap(better, worse)
    assert up["diff"] > 0 and up["ci_lo"] > 0 and up["n_dates"] == 40
    down = sts.joint_bootstrap(worse, better)
    assert down["diff"] < 0 and down["ci_hi"] < 0
    assert sts.joint_bootstrap(better, worse) == up                 # seeded
    assert sts.joint_bootstrap([], worse)["ci_lo"] is None
    assert sts.joint_bootstrap([_st(None, days[0])], worse)["diff"] is None


def test_paired_bootstrap_on_the_same_trades():
    live = [_st(0.0 + 0.1 * (k % 4), d) for k, d in enumerate(DATES[:30])]
    var = [dataclass_replace(t, excess=t.excess + 0.5 + 0.05 * (k % 3)) for k, t in enumerate(live)]
    p = sts.paired_bootstrap(list(zip(live, var, strict=True)))
    assert p["diff"] > 0.5 and p["ci_lo"] > 0 and p["n_pairs"] == 30
    same = sts.paired_bootstrap([(t, t) for t in live])
    assert same["diff"] == 0.0 and same["degenerate"]
    assert sts.paired_bootstrap([(live[0], None)])["n_pairs"] == 0


# ----------------------------------------------------------------------------- verdict
PASSING = {
    "n": 150, "median_net": 0.5, "mean_net": 0.8, "t_net": 2.5,
    "mean_excess": 0.5, "excess_ci_lo": 0.1, "excess_ci_hi": 0.9, "excess_degenerate": False,
    "cpcv_promotable": True, "top1_dropped_mean_net": 0.4, "held_out_mean_excess": 0.2,
}
CI_UP = {"diff": 0.3, "ci_lo": 0.05, "ci_hi": 0.6}
CTRL = {"uniform": {"p95": 0.3}, "matched": {"p95": 0.4}}


def _timing(m, live=0.2, ci=CI_UP, ctrl=CTRL):
    return sts.timing_label(m, sts.absolute(m), sts.relative(m.get("mean_excess"), live, ci, ctrl))


@pytest.mark.parametrize("change, failed", [
    ({"median_net": 0.0}, 2),
    ({"t_net": 2.0}, 3),
    ({"mean_net": 0.0}, 3),
    ({"excess_ci_lo": 0.0}, 4),
    ({"excess_degenerate": True}, 4),
    ({"cpcv_promotable": False}, 5),
    ({"top1_dropped_mean_net": None}, 6),
    ({"held_out_mean_excess": 0.0}, 7),
    ({"t_net": float("nan")}, 3),
])
def test_each_absolute_failure(change, failed):
    m = {**PASSING, **change}
    assert [r["id"] for r in sts.absolute(m) if not r["pass"]] == [failed]
    assert sts.addon_label(m, sts.absolute(m)) == sts.NOT_SHOWN
    assert _timing(m) == sts.BETTER_NOT_ENOUGH


def test_addon_labels():
    rows = sts.absolute(PASSING)
    assert sts.addon_label(PASSING, rows) == sts.ADOPT
    assert sts.addon_label({**PASSING, "n": 100}, sts.absolute({**PASSING, "n": 100})) == sts.ADOPT
    m99 = {**PASSING, "n": 99}
    assert sts.addon_label(m99, sts.absolute(m99)) == sts.INSUFFICIENT
    m0 = {**PASSING, "mean_excess": 0.0}
    assert sts.addon_label(m0, sts.absolute(m0)) == sts.REFUTED
    mneg = {**PASSING, "n": 99, "mean_excess": -1.0}
    assert sts.addon_label(mneg, sts.absolute(mneg)) == sts.INSUFFICIENT
    assert sts.addon_label({"n": 0}, sts.absolute({"n": 0})) == sts.INSUFFICIENT


def test_timing_labels():
    assert _timing(PASSING) == sts.ADOPT
    assert _timing({**PASSING, "n": 100}) == sts.ADOPT
    assert _timing({**PASSING, "n": 99}) == sts.INSUFFICIENT
    assert _timing(PASSING, live=0.5) == sts.REFUTED                         # equal to the live rule's
    assert _timing(PASSING, live=0.6) == sts.REFUTED
    assert _timing({**PASSING, "mean_excess": -0.1, "excess_ci_lo": -0.2}, live=-0.3) == sts.NOT_SHOWN
    assert _timing(PASSING, ci={**CI_UP, "ci_lo": 0.0}) == sts.NOT_SHOWN      # CI touches 0
    assert _timing(PASSING, ctrl={**CTRL, "uniform": {"p95": 0.5}}) == sts.NOT_SHOWN   # equal to the uniform p95
    assert _timing(PASSING, ctrl={**CTRL, "matched": {"p95": 0.5}}) == sts.NOT_SHOWN   # equal to the matched p95
    assert _timing(PASSING, ctrl={**CTRL, "matched": {"p95": 0.6}}) == sts.NOT_SHOWN   # beats uniform only
    assert _timing(PASSING, ctrl={**CTRL, "uniform": {"p95": 0.6}}) == sts.NOT_SHOWN   # beats matched only
    assert _timing(PASSING, ctrl={**CTRL, "matched": {"p95": None}}) == sts.NOT_SHOWN  # a missing control fails
    assert _timing(PASSING, ctrl={"uniform": {"p95": 0.3}}) == sts.NOT_SHOWN
    assert _timing({**PASSING, "cpcv_promotable": False}, ctrl={**CTRL, "matched": {"p95": 0.6}}) == sts.NOT_SHOWN
    assert _timing(PASSING, live=None) == sts.NOT_SHOWN
    assert _timing({**PASSING, "held_out_mean_excess": None}, ci={**CI_UP, "degenerate": True}) == sts.NOT_SHOWN


def test_relative_flags():
    r = sts.relative(0.5, 0.2, CI_UP, CTRL)
    assert r["pass"] and r["beats_live"] and r["ci_above_0"] and r["beats_uniform"] and r["beats_matched"]
    assert (r["uniform_p95"], r["matched_p95"]) == (0.3, 0.4)
    assert not sts.relative(0.5, 0.5, CI_UP, CTRL)["beats_live"]
    assert not sts.relative(None, 0.2, CI_UP, CTRL)["pass"]
    assert not sts.relative(0.5, 0.2, {"ci_lo": None}, CTRL)["ci_above_0"]


def test_ledger_mapping():
    assert sts.LEDGER == {"ADOPT": "supported", "BETTER, NOT ENOUGH": "null", "NOT SHOWN": "null",
                          "REFUTED": "refuted", "INSUFFICIENT": "insufficient"}


def test_measure_cell_on_no_trades_is_insufficient():
    m = sts.measure_cell([])
    assert m["n"] == 0 and m["mean_net"] is None and m["cost_pct"] is None and m["cpcv_promotable"] is False
    assert sts.addon_label(m, sts.absolute(m)) == sts.INSUFFICIENT


def test_measure_cell_cost_is_the_mean_of_the_trades():
    ts = [dataclass_replace(_st(0.1, DATES[k]), cost=c) for k, c in enumerate((0.34, 0.37, 0.37))]
    assert sts.measure_cell(ts)["cost_pct"] == pytest.approx((0.34 + 0.37 + 0.37) / 3)


def test_measure_cell_runs_cpcv_at_n_7_with_the_mean_cost(monkeypatch):
    seen = []

    def fake_cpcv(trades, n_trials, cost):
        seen.append((len(trades), n_trials, cost, [t.net for t in trades]))
        return {"promotable": True, "fold_pass_fraction": 1.0, "cv_method": "fake", "reasons": []}

    monkeypatch.setattr(sts.rc, "cpcv", fake_cpcv)
    ts = [dataclass_replace(_st(0.1, DATES[k]), cost=c, net=0.2 * k) for k, c in enumerate((0.34, 0.37, 0.41))]
    m = sts.measure_cell(ts)
    ((n, n_trials, cost, nets),) = seen
    assert n == 3 and n_trials == 7 and cost == pytest.approx((0.34 + 0.37 + 0.41) / 3) and nets == [0.0, 0.2, 0.4]
    assert m["cpcv_promotable"] is True and m["cpcv_method"] == "fake"


def _naive_cluster(dates_a, vals_a, dates_b, vals_b, union, reps, seed):
    """Per rep: draw union dates with replacement (the implementation's integer stream); each cell's mean over its
    trades on the drawn dates, a date drawn twice counting twice. A rep leaving a cell empty is NaN (dropped)."""
    draws = np.random.default_rng(seed).integers(0, len(union), size=(reps, len(union)))
    out = []
    for row in draws:
        drawn = [union[k] for k in row]
        a = [v for d in drawn for da, v in zip(dates_a, vals_a, strict=True) if da == d]
        b = [v for d in drawn for db, v in zip(dates_b, vals_b, strict=True) if db == d] if dates_b else None
        if not a or b == []:
            out.append(np.nan)
            continue
        out.append(sum(a) / len(a) - (sum(b) / len(b) if b is not None else 0.0))
    out = np.array(out)
    return out[np.isfinite(out)]


def test_joint_bootstrap_equals_a_naive_date_resample():
    d = DATES
    var = [_st(1.0, d[0]), _st(-0.5, d[0]), _st(2.0, d[1]), _st(0.25, d[3])]
    live = [_st(0.5, d[1]), _st(-1.0, d[2]), _st(0.0, d[2]), _st(1.5, d[3]), _st(None, d[4])]
    got = sts.joint_bootstrap(var, live, reps=300, seed=7)
    union = [d[0], d[1], d[2], d[3]]                                # the unbenchmarked trade's date is out
    naive = _naive_cluster([t.signal_date for t in var], [t.excess for t in var],
                           [t.signal_date for t in live[:4]], [t.excess for t in live[:4]], union, 300, 7)
    lo, hi = np.percentile(naive, [2.5, 97.5])
    assert got["n_dates"] == 4 and got["diff"] == pytest.approx(0.6875 - 0.25)
    assert got["ci_lo"] == pytest.approx(lo, abs=1e-12) and got["ci_hi"] == pytest.approx(hi, abs=1e-12)


def test_paired_bootstrap_equals_a_naive_date_resample():
    d = DATES
    live = [_st(0.0, d[0]), _st(1.0, d[0]), _st(0.5, d[2]), _st(-0.5, d[5])]
    var = [dataclass_replace(lv, excess=lv.excess + inc) for lv, inc in zip(live, (0.3, -0.1, 0.8, 0.2), strict=True)]
    got = sts.paired_bootstrap(list(zip(live, var, strict=True)))
    union = [d[0], d[2], d[5]]
    naive = _naive_cluster([t.signal_date for t in live], [0.3, -0.1, 0.8, 0.2], [], [], union,
                           sts.BOOTSTRAP_REPS, sts.BOOTSTRAP_SEED)
    lo, hi = np.percentile(naive, [2.5, 97.5])
    assert got["diff"] == pytest.approx(0.3) and got["n_dates"] == 3
    assert got["ci_lo"] == pytest.approx(lo, abs=1e-12) and got["ci_hi"] == pytest.approx(hi, abs=1e-12)


# ----------------------------------------------------------------------------- the whole evaluation
def _synthetic(monkeypatch):
    monkeypatch.setattr(pw, "CORE_RANK", 5)
    a, i_, b = _series("A"), _series("I"), _series("B")
    a.close[46] = 105.0
    g = pws.Grid.build({"A": a, "I": i_, "B": b}, {})
    adm = {"brk20": ([_adm(a, g, 25, "brk20", _sig(a, 25, 99.5)), _adm(a, g, 30, "brk20", _sig(a, 30, 99.5))],
                     {"signals_in_window": 2}),
           "ins": ([_adm(i_, g, 25, "ins")], {"signals_in_window": 1})}
    pat = FakePat({("A", 27): ("b1",), ("I", 25): ("b9",), ("A", 35): ("x3",), ("I", 30): ("x1",)})
    return g, adm, pat


def test_counts_only_returns_no_return_bearing_key(monkeypatch):
    g, adm, pat = _synthetic(monkeypatch)
    doc = sts.evaluate(adm, g, {}, FEES, pat, SETS, counts_only=True)
    forbidden = ("gross", "net", "excess", "bench", "mean", "median", "ci", "p95", "measure", "control", "cost")

    def walk(x, path=""):
        if isinstance(x, dict):
            for k, v in x.items():
                assert not any(f in str(k) for f in forbidden), f"{path}/{k}"
                walk(v, f"{path}/{k}")
        elif isinstance(x, list | tuple):
            for v in x:
                walk(v, path)
        else:
            assert isinstance(x, int) and not isinstance(x, bool), f"{path}: {x!r}"

    walk(doc)
    t = doc["trials"]
    assert t["brk20_addon"] == {"variant_n": 1, "first_positions_n": 1, "signal_before_first_fill": 0,
                                "filled_after_exit": 0, "extra_repeat": 0}
    assert t["brk20_candle_entry"] == {"variant_n": 1, "live_n": 1, "no_pattern": 1, "no_fill_session": 0,
                                       "held_overlap": 0, "masked_ex_date": 0}
    assert t["five_entry"]["variant_n"] == 1 and t["five_entry"]["no_pattern"] == 2
    assert t["brk20_candle_exit"] == {"variant_n": 1, "live_n": 1, "early_exit": 1, "late_open_exit": 0,
                                      "masked_ex_date": 0}
    assert t["five_exit"]["early_exit"] == 1 and t["ins_candle_exit"]["early_exit"] == 1
    assert doc["rules"]["exit_bearish/ins"] == {"early_exit": 1}


def test_evaluate_full_run_shape(monkeypatch):
    g, adm, pat = _synthetic(monkeypatch)
    doc = sts.evaluate(adm, g, {}, FEES, pat, SETS, counts_only=False)
    assert list(doc["trials"]) == list(sts.TRIALS)
    for name, t in doc["trials"].items():
        assert t["verdict"] == sts.INSUFFICIENT and t["ledger"] == "insufficient", name
    assert doc["trials"]["brk20_addon"]["measures"]["n"] == 1
    five = doc["trials"]["five_exit"]
    assert five["measures"]["n"] == 2 and five["diagnostics"]["early_exits"] == 1
    assert list(five["diagnostics"]["per_pattern"]) == ["x3"] and five["diagnostics"]["per_pattern"]["x3"]["n"] == 1
    # brk20's x3 on bar 35 sells at open(36) = 100 against the 99.5 limit fill; the band (all flat) is 0
    assert five["diagnostics"]["per_pattern"]["x3"]["mean_excess"] == pytest.approx(0.502513)
    assert five["controls"]["uniform"]["n_redrawn"] == five["controls"]["matched"]["n_redrawn"] == 1
    assert five["relative"]["bootstrap"]["n_pairs"] == 2 and "matched_p95" in five["relative"]
    entry = doc["trials"]["brk20_candle_entry"]
    assert entry["measures"]["n"] == 1 and entry["diagnostics"]["per_pattern"]["b1"]["n"] == 1
    assert entry["controls"]["matched"]["n_redrawn"] == 1
    addon = doc["trials"]["brk20_addon"]["diagnostics"]
    assert addon["comparator"] == sts.FIRST_POSITIONS_LABEL and addon["first_positions_all_kept_brk20"]["n"] == 1
    assert doc["live_cells"]["brk20"]["trial"] is False and len(doc["live_cells"]["ins"]["rows"]) == 7
    assert doc["rules"]["exit_bearish/ins"] == {"early_exit": 1}
    sts.render_text({"meta": {"registration": "r", "family": sts.FAMILY, "family_n": 7, "fold_pass_min": 0.6,
                              "db": "x", "window": {"bars_from": "a", "bars_to": "b", "signals_from": "c"},
                              "n_stocks": 3, "fees_pct": 0.3, "band_spread_pct": {}, "counts_only": False},
                     **doc, "notes": sts.NOTES})


# ----------------------------------------------------------------------------- refusals
def test_main_refuses_in_market_hours(tmp_path, capsys):
    rc_ = sts.main(["--db", str(tmp_path / "x.duckdb")], now=datetime(2026, 11, 11, 10, 0, tzinfo=IST))
    assert rc_ == 2
    assert "market hours" in capsys.readouterr().err


def test_run_refuses_without_recorded_spreads(monkeypatch):
    monkeypatch.setattr(pws, "BAND_SPREAD_PCT", {})
    with pytest.raises(rc.Refusal, match="band spreads"):
        sts.run_study(None, db_path=Path("x.duckdb"), etf=None)
