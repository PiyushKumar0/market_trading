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
    assert sts.FORWARD_SESSIONS == 25 and sts.ENTRY_SCAN_SESSIONS == 5
    assert sts.POPULATION == {"brk20": (pw.C0, pw.W1), "ins": (pw.C0,)}
    assert sts.BOOTSTRAP_SEED == sts.CONTROL_SEED == 20261111
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


# ----------------------------------------------------------------------------- admission
def test_admission_window_is_shared_by_both_legs(monkeypatch):
    monkeypatch.setattr(pw, "CORE_RANK", 5)
    s = _series("A")
    g = pws.Grid.build({"A": s}, {})
    atr = rc._atr(s)
    for leg in sts.LEGS:
        counts: dict[str, int] = {}
        assert sts._gate(leg, "A", s, 70 - 26, g, atr, counts) is not None    # y + 25 is the last session
        assert sts._gate(leg, "A", s, 70 - 25, g, atr, counts) is None
        assert counts == {"short_forward_window": 1}
    assert pws._gate("ins", "A", s, 70 - 25, g, atr, {}) is not None          # the pool study's ins span was 20


def test_admission_window_counts_market_sessions():
    gap = _gappy("G", {50, 51})                                     # 68 own bars: own-bar counting would drop 44
    s = _series("F")
    g = pws.Grid.build({"F": s, "G": gap}, {})
    counts: dict[str, int] = {}
    assert sts._gate("ins", "G", gap, 44, g, rc._atr(gap), counts) is not None   # grid 44 + 25 = 69 < 70
    assert sts._gate("ins", "G", gap, 45, g, rc._atr(gap), counts) is None
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
    monkeypatch.setattr(sts.es, "insider_buy_events", lambda sessions, filings, threshold: [25, 45])
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


def test_addons_first_only_filled_after_exit_and_extra_repeat():
    a, b = _series("A"), _series("B")
    k1 = _cand(a, 5, 6, 26)
    first = _cand(a, 10, 11, 31)                                    # fills before k1's exit: the add-on
    extra = _cand(a, 12, 13, 33)                                    # a second one on k1: not taken
    late = _cand(a, 20, 26, 46)                                     # fills on k1's exit session
    k2 = _cand(a, 26, 27, 47)                                       # signal on k1's exit date: kept
    after_k2 = _cand(a, 28, 30, 50)                                 # k2's own add-on
    other = _cand(b, 10, 11, 31)
    cands = [after_k2, other, late, k2, extra, first, k1]
    counts: dict[str, int] = {}
    kept = pws.one_position_per_symbol(cands, counts)
    assert kept == [k1, k2, other] and counts == {"held_overlap": 4}
    got = sts.addons(cands, kept, counts)
    assert got == [first, after_k2]
    assert counts == {"held_overlap": 4, "filled_after_exit": 1, "extra_repeat": 1}


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


def test_entry_options_cover_the_eligible_days(monkeypatch):
    s = _series("A")
    s.close[46] = 110.0                                             # the day-27 fill exits here (fill + 19)
    g = _world(monkeypatch, s)
    a = _adm(s, g, 25)
    t = sts.walk_close(sts.candle_entry(a, g, FakePat({("A", 25): ("b1",)}), SETS["bullish"], {}), g, {}, FEES, {})
    opts = sts.entry_options(t, a, g, {}, FEES)
    assert len(opts) == 5 and np.allclose(opts, 0.0)                # own stock alone in its band: excess 0
    voided = sts.entry_options(t, a, g, {"A": [DATES[48]]}, FEES)   # fills 26..30 exit 45..49: last two hold it
    assert np.isnan(voided).tolist() == [False, False, False, True, True]
    s.high[28] = s.low[28] = 100.0                                  # a locked fill day is not eligible
    assert len(sts.entry_options(t, a, g, {}, FEES)) == 4


def test_exit_options_and_control_leave_untouched_trades(monkeypatch):
    s, b = _series("A"), _series("B")
    s.open[27:45] = np.linspace(101.0, 118.0, 18)
    g = _world(monkeypatch, s, b)
    t = _live(s, g)
    opts = sts.exit_options(t, g, {}, FEES, sts.OpenBenchmark(g))
    assert len(opts) == t.cand.exit_idx - t.cand.event.fill_idx == 19
    v = sts.candle_exit(t, FakePat({("A", 30): ("x1",)}), SETS["bearish"], g, {}, FEES, sts.OpenBenchmark(g), {})
    untouched = dataclass_replace(t, excess=7.0)
    ctrl = sts.exit_control([v, untouched], g, {}, FEES, sts.OpenBenchmark(g), 1.0)
    assert ctrl["n_redrawn"] == 1 and ctrl["draws"] == sts.CONTROL_DRAWS
    means = sts.draw_means([7.0], [opts])
    assert ctrl["p95"] == pytest.approx(float(np.percentile(means, 95)))


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
CTRL = {"p95": 0.3}


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
    assert _timing(PASSING, ctrl={"p95": 0.5}) == sts.NOT_SHOWN               # equal to the control's p95
    assert _timing(PASSING, ctrl={"p95": None}) == sts.NOT_SHOWN              # a missing control fails
    assert _timing(PASSING, live=None) == sts.NOT_SHOWN
    assert _timing({**PASSING, "held_out_mean_excess": None}, ci={**CI_UP, "degenerate": True}) == sts.NOT_SHOWN


def test_relative_flags():
    r = sts.relative(0.5, 0.2, CI_UP, CTRL)
    assert r["pass"] and r["beats_live"] and r["ci_above_0"] and r["beats_control"]
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
    assert t["brk20_addon"] == {"variant_n": 1, "first_position_n": 1, "filled_after_exit": 0, "extra_repeat": 0}
    assert t["brk20_candle_entry"] == {"variant_n": 1, "live_n": 1, "no_pattern": 1, "no_fill_session": 0,
                                       "held_overlap": 0}
    assert t["five_entry"]["variant_n"] == 1 and t["five_entry"]["no_pattern"] == 2
    assert t["brk20_candle_exit"] == {"variant_n": 1, "live_n": 1, "early_exits": 1, "late_open_exits": 0}
    assert t["five_exit"]["early_exits"] == 1 and t["ins_candle_exit"]["early_exits"] == 1


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
    assert five["control"]["n_redrawn"] == 1 and five["relative"]["bootstrap"]["n_pairs"] == 2
    entry = doc["trials"]["brk20_candle_entry"]
    assert entry["measures"]["n"] == 1 and entry["diagnostics"]["per_pattern"]["b1"]["n"] == 1
    assert entry["control"]["n_redrawn"] == 1
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
