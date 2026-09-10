"""``scripts/calibrate_fill_model.py`` - the R9 fill-model calibration (WO-P3-4, 2026-09-10).

Every fixture is engineered so each assertion has an arithmetic answer rather than a regression
blob. The corpus is written as REAL Parquet partitions in the production layout
(``ticks/date=<d>/symbol=<S>/*.parquet``, the 12 ``_TICK_COLUMNS``) via DuckDB ``COPY``, so the test
exercises the same globbing/scan path the script uses over the live archive - not an injected frame.

The cast on the one synthetic session (plan 3.2.9, method fixed by the work order):

* ``CONSTSPREAD`` - constant ltp, constant L1 (999.50 / 1000.50). Its half-spread pct is a single
  value, so the per-symbol median is exact to the paisa. `median_half_spread_pct` is a PERCENT of
  mid (2026-09-10 round-3 fix: the consumer ``engine.paper.fill_model`` divides the field by 100,
  so the producer must emit a percent - the name carries the unit), i.e. 0.50 / 1000.00 * 100 =
  0.05. Constant ltp also means sigma_1m == 0, so it contributes ZERO normalised slippage samples -
  the divide-by-sigma guard is what this symbol pins.
* ``KTWO`` (open bucket) and ``MIDFULL`` (mid bucket) - the k ~ 2 streams. Construction: ticks every
  750 ms (so the first tick at or after t + 700 ms is exactly the NEXT tick), ltp alternating
  ``B_m`` / ``B_m + delta`` and L1 quoted symmetrically around each tick's own ltp. Then for EVERY
  sampled tick the decision mid is its ltp and the 700 ms-later fill differs by exactly ``delta``,
  so the unexplained excess is ``delta - half`` on every sample. The minute-close series is
  ``B_m + delta`` with ``B_m`` alternating by ``eps``, so sigma_1m is ~ ``eps / B``. With
  delta = 2.5, half = 0.5, eps = 1.0 at B ~ 1000 the normalised sample is (2.5 - 0.5) / 1.0 = 2.0.
* ``THINCLOSE`` - 200 ticks in the close bucket: under the 500-sample floor, so that bucket must
  fall back to the consumer's own default (close = 1.0, ``basis: thin``) rather than fit a k off
  200 points.

* ``NOL1`` - 900 mid-bucket ticks with bid == ask == 0 (the WARMING/backfilled shape, plan 3.2.9
  "half-spread fallback"). Excluded from the spread stat (no ``symbols`` entry at all), still
  counted in the reported tick/sample counts and still producing slippage samples off the
  deterministic fallback half-spread.

The 2026-09-10 round-3 k-floor policy (manager decision, plan 3.2.9 / 8.4) is pinned by four tests
below: a calibration may only TIGHTEN the model, so the shipped k is
``max(fitted p75, the consumer default for that bucket)`` - open 1.0 / mid 0.5 / close 1.0 - and a
degenerate (p50 <= 0) or thin (n < 500) bucket ships the default outright. ``basis`` names which
rule applied, and p50/p75/p90/n ride along in the YAML as the evidence for the Phase-4 (G4)
live-vs-paper recalibration.
"""

from __future__ import annotations

import importlib.util
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import duckdb
import numpy as np
import pandas as pd
import pytest
import yaml
from pydantic import ValidationError

_SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "calibrate_fill_model.py"
_spec = importlib.util.spec_from_file_location("mt_calibrate_fill_model", _SCRIPT)
cfm = importlib.util.module_from_spec(_spec)
sys.modules["mt_calibrate_fill_model"] = cfm
_spec.loader.exec_module(cfm)

IST = ZoneInfo("Asia/Kolkata")

DAY = date(2026, 6, 1)
SPACING_MS = 750          # > the 700 ms latency, so "first tick at/after t+700ms" == the next tick
BASE = 1000.0
EPS = 1.0                 # minute-to-minute move of the base level -> sigma_1m ~ 0.1%
DELTA = 2.5               # every 700 ms-later fill sits this far from the decision mid
HALF = 0.5                # quoted half-spread, in price units

# The 12 production tick columns (engine.marketdata.store._TICK_COLUMNS), in order.
_TICK_COLUMNS = (
    "instrument_token", "tradingsymbol", "ltp", "volume_traded", "exchange_ts",
    "ohlc_open", "ohlc_high", "ohlc_low", "ohlc_close", "avg_price", "bid", "ask",
)


def _stream(symbol: str, token: int, start: datetime, n: int, *, l1: bool = True,
            delta: float = DELTA) -> pd.DataFrame:
    """The engineered tape described in the module docstring, as a production-shaped tick frame.

    `delta` is the gap between a decision tick's mid and the fill 700 ms later; `delta == 0` makes
    every residual exactly 0 while keeping sigma_1m > 0 - the degenerate-estimator case.
    """
    ts = [start + timedelta(milliseconds=SPACING_MS * i) for i in range(n)]
    # Minute index relative to the stream start; B_m alternates by EPS so sigma_1m is well-defined.
    minute = [(t.replace(second=0, microsecond=0) - start.replace(second=0, microsecond=0)).seconds // 60
              for t in ts]
    base = [BASE + (EPS if (m % 2) else 0.0) for m in minute]
    ltp = [b + (delta if (i % 2) else 0.0) for i, b in enumerate(base)]
    bid = [round(p - HALF, 2) if l1 else 0.0 for p in ltp]
    ask = [round(p + HALF, 2) if l1 else 0.0 for p in ltp]
    return _frame(symbol, token, ts, ltp, bid, ask)


def _flat_stream(symbol: str, token: int, start: datetime, n: int) -> pd.DataFrame:
    """Constant price, constant L1 - an exact half-spread median and a zero sigma_1m."""
    ts = [start + timedelta(milliseconds=SPACING_MS * i) for i in range(n)]
    ltp = [BASE] * n
    return _frame(symbol, token, ts, ltp, [BASE - HALF] * n, [BASE + HALF] * n)


def _frame(symbol, token, ts, ltp, bid, ask) -> pd.DataFrame:
    return pd.DataFrame({
        "instrument_token": np.int64(token),
        "tradingsymbol": symbol,
        "ltp": np.array(ltp, dtype="float64"),
        "volume_traded": np.arange(len(ts), dtype="int64") * 100,
        "exchange_ts": pd.DatetimeIndex(ts).tz_convert("Asia/Calcutta"),
        "ohlc_open": np.float64(BASE),
        "ohlc_high": np.float64(BASE + 10),
        "ohlc_low": np.float64(BASE - 10),
        "ohlc_close": np.float64(BASE),
        "avg_price": np.array(ltp, dtype="float64"),
        "bid": np.array(bid, dtype="float64"),
        "ask": np.array(ask, dtype="float64"),
    })


def _write_partition(root: Path, day: date, frame: pd.DataFrame, *,
                     filename: str = "part.parquet") -> None:
    """Write one symbol-day partition with DuckDB COPY, in the production directory layout.

    `filename` names the fragment: the archive's compaction job leaves exactly one
    ``compact-ticks.parquet`` per symbol-day, while an un-compacted day holds many arbitrarily
    named fragments - the distinction the coverage/`compacted_only` tests below turn on.
    """
    symbol = str(frame["tradingsymbol"].iloc[0])
    part = root / "ticks" / f"date={day.isoformat()}" / f"symbol={symbol}"
    part.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    try:
        con.execute("SET TimeZone='Asia/Kolkata'")
        con.register("frame", frame)
        cols = ", ".join(_TICK_COLUMNS)
        target = (part / filename).as_posix()
        con.execute(f"COPY (SELECT {cols} FROM frame) TO '{target}' (FORMAT PARQUET)")
    finally:
        con.close()


def _at(h: int, m: int, s: int = 0, day: date = DAY) -> datetime:
    return datetime(day.year, day.month, day.day, h, m, s, tzinfo=IST)


@pytest.fixture(scope="module")
def corpus(tmp_path_factory) -> Path:
    root = tmp_path_factory.mktemp("parquet")
    _write_partition(root, DAY, _flat_stream("CONSTSPREAD", 101, _at(9, 20), 800))
    _write_partition(root, DAY, _stream("KTWO", 102, _at(9, 15), 3200))          # 09:15-09:55, open
    _write_partition(root, DAY, _stream("MIDFULL", 103, _at(10, 0), 1000))       # 10:00-10:12, mid
    _write_partition(root, DAY, _stream("THINCLOSE", 104, _at(14, 35), 200))     # 14:35-14:37, close
    _write_partition(root, DAY, _stream("NOL1", 105, _at(11, 0), 900, l1=False))  # 11:00-11:11, mid
    return root


@pytest.fixture(scope="module")
def result(corpus):
    return cfm.calibrate(corpus, max_samples=1_000_000, days=None)


def _expected_k() -> float:
    """The normalised sample the KTWO/MIDFULL construction must reproduce, computed from the
    designed minute-close series rather than asserted as a magic number."""
    closes = np.array([BASE + (EPS if (m % 2) else 0.0) + DELTA for m in range(40)])
    sigma_ret = float(np.std(closes[1:] / closes[:-1] - 1.0, ddof=1))
    return (DELTA - HALF) / (sigma_ret * BASE)


# --- (a) constant spread -> that half-spread median -------------------------------------------

def test_constant_spread_yields_exact_half_spread_median(result):
    sym = result.model.symbols["CONSTSPREAD"]
    # PERCENT of mid, not a fraction: the consumer divides by 100 (fill_model.py half_spread()).
    assert sym.median_half_spread_pct == pytest.approx(HALF / BASE * 100.0, abs=1e-9)
    assert sym.n_ticks == 800


# --- (b) fill at mid + 2 sigma -> a fit ABOVE the default, so the fit ships ----------------------

def test_two_sigma_fill_yields_k_about_two(result):
    expected = _expected_k()
    assert expected == pytest.approx(2.0, rel=0.02)   # the construction really does target 2 sigma
    for name in ("open", "mid"):
        bucket = result.model.buckets[name]
        # ~2.0 clears both consumer defaults (open 1.0, mid 0.5), so the fit is what ships.
        assert bucket.basis == "fitted", name
        assert bucket.n >= 500, name
        assert bucket.k == pytest.approx(expected, rel=0.05), name
        assert result.report["buckets"][name]["p50"] == pytest.approx(expected, rel=0.05), name
        assert bucket.k > cfm.CONSUMER_DEFAULT_K[name], name


# --- (c) thin / degenerate / floored buckets -> the consumer's own default -----------------------

def test_thin_bucket_falls_back_to_conservative_default(result):
    bucket = result.model.buckets["close"]
    assert bucket.n < 500
    assert bucket.n > 0                      # the thin samples are still counted, just not fitted
    assert bucket.k == cfm.CONSUMER_DEFAULT_K["close"] == 1.0
    assert bucket.basis == "thin"


def test_degenerate_fit_takes_the_conservative_default_not_zero(tmp_path):
    """A FAT bucket whose fitted quantiles are 0 must not ship k = 0.

    This is the 2026-09-10 finding from the real archive: the residual is floored at 0 and over
    700 ms a liquid name usually has not left its own quote, so the p50/p75 of a large sample is
    exactly 0. Shipping that deletes the sigma term. It takes the same conservative default as a
    thin bucket, and the report must name WHY - so the owner can re-pin the estimator rather than
    discover a silently spread-only fill model in the paper ledger.
    """
    root = tmp_path / "parquet"
    _write_partition(root, DAY, _stream("FLATFILL", 201, _at(9, 15), 900, delta=0.0))
    out = cfm.calibrate(root, max_samples=1_000_000, days=None)

    bucket = out.model.buckets["open"]
    cell = out.report["buckets"]["open"]
    assert cell["n"] >= 500                       # NOT thin - a real, large sample
    assert cell["p50"] == 0.0 and cell["p75"] == 0.0    # the measurement is reported verbatim
    assert bucket.k == cfm.CONSUMER_DEFAULT_K["open"] == 1.0
    assert bucket.basis == "degenerate"
    assert any("ESTIMATOR, NOT DATA" in c for c in out.report["caveats"])


# The floored case: delta 0.7 with a 0.5 half-spread leaves a residual of 0.2 price units, and the
# construction normalises by sigma_1m ~ EPS = 1.0 price unit, so every sample is ~0.2 - a real,
# non-degenerate fit (p50 > 0) that is still well under the mid bucket's 0.5 default.
FLOOR_DELTA = 0.7


def test_a_small_but_nonzero_fit_is_floored_to_the_consumer_default(tmp_path):
    """A calibration may only TIGHTEN the model (manager decision, 2026-09-10 round 3).

    A fitted p75 BELOW the consumer's default for that bucket would have the paper broker charge
    LESS slippage than it charges with no calibration at all - and the evidence for that discount is
    a zero-inflated residual over a window-biased, survivorship-tainted corpus. So the shipped k is
    max(fit, default) and the bucket is stamped `floored_to_default`, with the fitted quantiles kept
    verbatim beside it for the Phase-4 (G4) live-vs-paper recalibration.
    """
    root = tmp_path / "parquet"
    _write_partition(root, DAY, _stream("SLOWDRIFT", 202, _at(10, 0), 900, delta=FLOOR_DELTA))
    out = cfm.calibrate(root, max_samples=1_000_000, days=None)

    bucket = out.model.buckets["mid"]
    cell = out.report["buckets"]["mid"]
    assert cell["n"] >= 500                                  # not thin
    assert 0.0 < cell["p50"] < cfm.CONSUMER_DEFAULT_K["mid"]  # not degenerate, but under the default
    assert 0.0 < cell["p75"] < cfm.CONSUMER_DEFAULT_K["mid"]
    assert bucket.k == cfm.CONSUMER_DEFAULT_K["mid"] == 0.5   # the DEFAULT ships, not the fit
    assert bucket.basis == "floored_to_default"
    # The fit is not discarded - it is the G4 recalibration evidence, carried in the YAML itself.
    assert bucket.p50 == cell["p50"] and bucket.p75 == cell["p75"] and bucket.p90 == cell["p90"]
    assert any("only tighten" in c.lower() for c in out.report["caveats"])


def test_basis_is_classified_on_the_unrounded_quantiles(tmp_path):
    """`basis` must be decided on the RAW quantiles; rounding is a publication step only.

    2026-09-10 round-4 defect: the quantiles were rounded to 4 dp BEFORE the classifier read them,
    so a small-but-real fit - a true p50 of 3e-5 - was rounded to 0.0 and then stamped
    `degenerate`, i.e. "the estimator collapsed onto the zero-inflated mass, the owner should
    re-pin it". It had not collapsed; it had been rounded. The shipped k is the same either way
    (both branches take the consumer default), but the label is the finding the report escalates to
    the owner, so a rounding artefact must not manufacture one. `floored_to_default` is the honest
    label: a real fit that came out below the default.

    `_finalise` is driven directly here because a Parquet corpus cannot be engineered to land a
    p50 on an exact 3e-5.
    """
    acc = cfm._Accumulator()
    acc.bucket_z["mid"] = [np.full(600, 3e-5, dtype="float64")]
    acc.sampled = 600

    out = cfm._finalise(acc)

    cell = out.report["buckets"]["mid"]
    assert cell["n"] == 600                                   # not thin
    assert cell["p50"] == 0.0 and cell["p75"] == 0.0          # published rounding shows zeros ...
    assert cell["basis"] == "floored_to_default"              # ... the classifier saw 3e-5
    assert out.model.buckets["mid"].k == cfm.CONSUMER_DEFAULT_K["mid"] == 0.5
    assert not any("ESTIMATOR, NOT DATA" in c for c in out.report["caveats"])


def test_bucket_windows_match_the_pinned_contract(result):
    windows = {n: (b.start, b.end) for n, b in result.model.buckets.items()}
    assert windows == {
        "open": ("09:15", "10:00"),
        "mid": ("10:00", "14:30"),
        "close": ("14:30", "15:30"),
    }


# --- (d) no-L1 ticks: out of the spread stat, in the sample counts -------------------------------

def test_ticks_without_l1_are_excluded_from_spread_but_counted(result):
    assert "NOL1" not in result.model.symbols          # no L1 tick -> no median to publish
    assert set(result.model.symbols) == {"CONSTSPREAD", "KTWO", "MIDFULL", "THINCLOSE"}

    report = result.report
    assert report["session_ticks"] == 800 + 3200 + 1000 + 200 + 900
    assert report["l1_ticks"] == 800 + 3200 + 1000 + 200
    assert report["no_l1_share"] == pytest.approx(900 / report["session_ticks"], rel=1e-6)

    # NOL1's 900 mid-bucket ticks still produce slippage samples (off the fallback half-spread),
    # so the mid bucket carries materially more than MIDFULL's ~1000 on its own.
    assert report["buckets"]["mid"]["n"] > 1800
    assert result.model.half_spread_fallback_ticks == 2


def test_zero_sigma_symbol_is_dropped_from_samples_not_from_counts(result):
    # CONSTSPREAD is flat: sigma_1m == 0, so every one of its ticks is an unusable sample.
    assert result.report["samples_dropped"]["no_sigma"] >= 799
    assert result.model.source.ticks == result.report["session_ticks"]
    assert result.model.source.symbols == 5
    assert result.model.source.days == [DAY.isoformat()]


# --- (e) YAML round-trip through the in-script strict model --------------------------------------

def test_yaml_round_trips_through_the_strict_model(result, tmp_path):
    out = tmp_path / "fill_model.yaml"
    cfm.write_yaml(result.model, out)
    text = out.read_text(encoding="utf-8")
    loaded = yaml.safe_load(text)

    # The header carries the unit of median_half_spread_pct and the k-floor policy, next to the
    # numbers - a reader of the config alone must not have to find the report to learn either.
    assert "percent" in text.lower() and "G4" in text and "only tighten" in text.lower()
    assert loaded["version"] == 1
    assert loaded["latency_ms"] == 700
    assert set(loaded) == {
        "version", "generated_at", "source", "latency_ms", "buckets",
        "half_spread_fallback_ticks", "symbols",
    }
    assert set(loaded["buckets"]) == {"open", "mid", "close"}
    # The fitted quantiles ride ALONG with k: the YAML is the G4 recalibration evidence, so a
    # reader can see what the fit said and why the shipped k differs from it.
    assert set(loaded["buckets"]["open"]) == {
        "start", "end", "k", "basis", "n", "p50", "p75", "p90",
    }

    back = cfm.FillModel.model_validate(loaded)
    assert back.model_dump(mode="json") == result.model.model_dump(mode="json")
    assert back.generated_at.tzinfo is not None
    assert back.generated_at.utcoffset() == timedelta(hours=5, minutes=30)


def test_strict_model_rejects_unknown_keys(result):
    payload = result.model.model_dump(mode="json")
    payload["buckets"]["open"]["kk"] = 1.0
    with pytest.raises(ValidationError):
        cfm.FillModel.model_validate(payload)


def test_report_carries_the_honest_caveats(result):
    report = result.report
    for name in ("open", "mid", "close"):
        cell = report["buckets"][name]
        assert set(cell) >= {"n", "p50", "p75", "p90", "k", "basis"}
    assert set(report["half_spread_pct"]) >= {"p50", "p90", "n_symbols"}
    assert report["latency_ms"] == 700
    text = " ".join(report["caveats"]).lower()
    assert "window" in text and "survivorship" in text and "700" in text
    assert report["hour_histogram"]                       # which hours dominate the corpus


# --- (f) bounded runs (2026-09-10): the archive is too big to scan whole beside a live session ----
#
# WHY these exist: the real corpus is 27 sessions x ~300 symbols, and an UN-compacted session holds
# thousands of tiny fragments per symbol (a single symbol-day of 2026-09-09 measured 17-33 s to
# read). A calibration that can only run over the whole corpus cannot be run at all on a trading
# day, so the work order pins three bounding controls - `--days`, `--symbols-per-day` and a
# compaction filter - and requires the report to state honestly what was actually covered.


@pytest.mark.parametrize(("m", "n"), [(5, 2), (5, 4), (7, 5), (9, 6), (10, 3), (11, 4), (200, 60)])
def test_select_symbols_lands_exactly_n(m: int, n: int):
    """`--symbols-per-day N` must deliver exactly N symbols, spanning the whole sorted list.

    2026-09-10 round-3 defect: the old `symbols[::ceil(M/N)][:N]` UNDER-delivers whenever
    ceil(M/N) overshoots - M=5,N=4 gave 3 symbols; M=9,N=6 gave 5. The bound then silently reads
    less tape than the operator asked for, and the coverage table reports the shortfall as if it
    were the request. An index-rounding stride (`round(i*(M-1)/(N-1))`) lands exactly N, keeps both
    endpoints, and stays deterministic.
    """
    names = [f"SYM{i:03d}" for i in range(m)]
    picked = cfm.select_symbols(names, n)

    assert len(picked) == n
    assert len(set(picked)) == n                     # deduped
    assert picked == sorted(picked)                  # still in sorted-universe order
    assert picked[0] == names[0] and picked[-1] == names[-1]   # spans the alphabet, not a head slice
    assert cfm.select_symbols(names, n) == picked    # deterministic


def test_symbols_per_day_takes_a_deterministic_spanning_slice(corpus):
    """`--symbols-per-day` must be a stride over the SORTED symbol list, not a head slice.

    A head slice ("the first N symbols") would calibrate the alphabet, not the tape. With M = 5 and
    N = 2 the index-rounding stride takes index 0 and index 4 - CONSTSPREAD and THINCLOSE, the two
    ends of the sorted universe. Deterministic, so two runs of the same bounded command agree.
    """
    out = cfm.calibrate(corpus, max_samples=1_000_000, symbols_per_day=2)

    day = out.report["coverage"][0]
    assert day["symbols_available"] == 5
    assert day["symbols_used"] == 2
    assert out.model.source.symbols == 2
    # A head slice would have produced CONSTSPREAD + KTWO instead.
    assert set(out.model.symbols) == {"CONSTSPREAD", "THINCLOSE"}
    assert out.report["symbols_per_day"] == 2

    again = cfm.calibrate(corpus, max_samples=1_000_000, symbols_per_day=2)
    assert set(again.model.symbols) == set(out.model.symbols)
    assert again.model.source.ticks == out.model.source.ticks


def test_every_symbol_is_represented_when_the_sample_budget_forces_a_stride(corpus):
    """A thin symbol must still contribute samples once `--max-samples` forces a large stride.

    The stride is a per-symbol every-Nth. Anchoring it at row N (`rn % stride = 0`) silently drops
    EVERY symbol with fewer than N ticks - on the real corpus, that is the illiquid tail, exactly
    the names whose slippage matters most. Anchoring at row 1 (`(rn - 1) % stride = 0`) keeps every
    symbol in the sample. THINCLOSE is the probe: it owns the close bucket outright, so a zero there
    is proof its 200 ticks were dropped whole.
    """
    out = cfm.calibrate(corpus, max_samples=5)
    assert out.report["buckets"]["close"]["n"] >= 1
    assert out.report["sampled_ticks"] >= 5


def _two_day_corpus(root: Path) -> tuple[date, date]:
    """Day 1 compacted (one `compact-ticks.parquet`); day 2 fragmented (two loose parts)."""
    compacted, fragmented = date(2026, 6, 1), date(2026, 6, 2)
    _write_partition(root, compacted, _stream("KTWO", 102, _at(9, 15, day=compacted), 900),
                     filename="compact-ticks.parquet")
    head = _stream("KTWO", 102, _at(9, 15, day=fragmented), 900)
    _write_partition(root, fragmented, head.iloc[:450], filename="ticks-0001.parquet")
    _write_partition(root, fragmented, head.iloc[450:], filename="ticks-0002.parquet")
    return compacted, fragmented


def test_coverage_block_reports_compaction_per_day(tmp_path):
    root = tmp_path / "parquet"
    compacted, fragmented = _two_day_corpus(root)

    out = cfm.calibrate(root, max_samples=1_000_000)

    coverage = {row["day"]: row for row in out.report["coverage"]}
    assert set(coverage) == {compacted.isoformat(), fragmented.isoformat()}
    assert coverage[compacted.isoformat()]["compacted"] is True
    assert coverage[fragmented.isoformat()]["compacted"] is False
    assert coverage[compacted.isoformat()]["ticks"] == 900
    assert coverage[fragmented.isoformat()]["ticks"] == 900
    assert coverage[compacted.isoformat()]["elapsed_s"] >= 0.0


def test_compacted_only_skips_the_fragmented_day(tmp_path):
    """`--compacted-only` is the I/O-modesty switch: it must SKIP, and say so, not read anyway."""
    root = tmp_path / "parquet"
    compacted, fragmented = _two_day_corpus(root)

    out = cfm.calibrate(root, max_samples=1_000_000, compacted_only=True)

    assert out.model.source.days == [compacted.isoformat()]
    assert out.model.source.ticks == 900
    assert {"day": fragmented.isoformat(), "reason": "not_compacted"} in out.report["days_skipped"]
    assert out.report["compacted_only"] is True


def _three_day_corpus(root: Path) -> list[date]:
    """Three compacted single-symbol sessions, each with a distinct tick count so the report says
    unambiguously WHICH sessions were read."""
    days = [date(2026, 6, 1), date(2026, 6, 2), date(2026, 6, 3)]
    for index, day in enumerate(days):
        _write_partition(root, day, _stream("KTWO", 102, _at(9, 15, day=day), 600 + index * 100),
                         filename="compact-ticks.parquet")
    return days


def test_days_keeps_only_the_most_recent_sessions(tmp_path):
    """`--days N` is the "most recent N sessions" bound, not the first N."""
    root = tmp_path / "parquet"
    d1, d2, d3 = _three_day_corpus(root)

    out = cfm.calibrate(root, max_samples=1_000_000, days=2)

    assert out.model.source.days == [d2.isoformat(), d3.isoformat()]
    assert d1.isoformat() not in out.model.source.days
    assert out.model.source.ticks == 700 + 800          # d2 + d3, never d1's 600


def test_exclude_is_applied_before_the_days_cut(tmp_path):
    """`exclude` drops named sessions BEFORE `--days` counts, so excluding today still yields N.

    The caller excludes an in-progress session (a half-recorded day is a pure open-bucket sample).
    If the exclusion were applied AFTER the most-recent-N cut, asking for 2 days while excluding the
    latest would silently return 1 - the bound would quietly under-deliver exactly when it matters.
    """
    root = tmp_path / "parquet"
    d1, d2, d3 = _three_day_corpus(root)

    out = cfm.calibrate(root, max_samples=1_000_000, days=2, exclude={d3})

    assert out.model.source.days == [d1.isoformat(), d2.isoformat()]
    assert out.model.source.ticks == 600 + 700


def test_cli_wires_days_and_exclude_day(tmp_path):
    root = tmp_path / "parquet"
    d1, d2, d3 = _three_day_corpus(root)
    out_yaml = tmp_path / "fill_model.yaml"
    stem = tmp_path / "reports" / "calib"

    rc = cfm.main([
        "--parquet-root", str(root), "--out", str(out_yaml), "--report", str(stem),
        "--days", "2", "--exclude-day", d3.isoformat(),
    ])

    assert rc == 0
    loaded = yaml.safe_load(out_yaml.read_text(encoding="utf-8"))
    assert loaded["source"]["days"] == [d1.isoformat(), d2.isoformat()]


def _fragmented_corpus(root: Path) -> list[date]:
    """Two sessions, BOTH still held as loose fragments - nothing `--compacted-only` may read."""
    days = [date(2026, 6, 1), date(2026, 6, 2)]
    for day in days:
        frame = _stream("KTWO", 102, _at(9, 15, day=day), 900)
        _write_partition(root, day, frame.iloc[:450], filename="ticks-0001.parquet")
        _write_partition(root, day, frame.iloc[450:], filename="ticks-0002.parquet")
    return days


def test_a_run_that_reads_no_session_writes_nothing_and_exits_nonzero(tmp_path, capsys):
    """Zero sessions read must NOT overwrite the shipped model with an all-defaults one.

    2026-09-10 round-3 defect: with every session skipped the run still exited 0 and wrote a YAML
    whose buckets were the untouched defaults and whose `symbols` table was EMPTY - i.e. it silently
    deleted the per-symbol half-spread fallbacks that the previous good calibration had established,
    and reported success. A run with no evidence writes nothing and says why.
    """
    root = tmp_path / "parquet"
    days = _fragmented_corpus(root)
    out_yaml = tmp_path / "fill_model.yaml"
    stem = tmp_path / "reports" / "calib"

    rc = cfm.main([
        "--parquet-root", str(root), "--out", str(out_yaml), "--report", str(stem),
        "--compacted-only",
    ])

    assert rc != 0
    assert not out_yaml.exists()
    assert not stem.with_suffix(".md").exists() and not stem.with_suffix(".json").exists()
    err = capsys.readouterr().err
    for day in days:                       # the reasons are printed, not swallowed
        assert day.isoformat() in err
    assert "not_compacted" in err


def test_a_truncated_fragment_lands_in_days_skipped_not_an_abort(tmp_path, monkeypatch):
    """A fragment truncated by concurrent compaction must skip its DAY, never abort the run.

    The live engine compacts this archive underneath the scan, so a fragment can be half-written
    when DuckDB opens it. That surfaces as an InvalidInputException ("No magic bytes found"), not
    the IOException the retry originally caught - so one truncated file aborted the whole
    calibration and the good sessions were lost with it. The retry now covers `duckdb.Error`.
    """
    monkeypatch.setattr(cfm, "_RETRY_SLEEP_S", 0.0)     # don't pay the real backoff in a unit test
    root = tmp_path / "parquet"
    good, broken = date(2026, 6, 1), date(2026, 6, 2)
    _write_partition(root, good, _stream("KTWO", 102, _at(9, 15, day=good), 900),
                     filename="compact-ticks.parquet")
    _write_partition(root, broken, _stream("KTWO", 102, _at(9, 15, day=broken), 900),
                     filename="ticks-0001.parquet")
    victim = root / "ticks" / f"date={broken.isoformat()}" / "symbol=KTWO" / "ticks-0001.parquet"
    victim.write_bytes(victim.read_bytes()[: len(victim.read_bytes()) // 2])   # torn mid-write

    out = cfm.calibrate(root, max_samples=1_000_000)

    assert out.model.source.days == [good.isoformat()]          # the good day still calibrated
    assert out.model.source.ticks == 900
    skipped = {row["day"]: row["reason"] for row in out.report["days_skipped"]}
    assert broken.isoformat() in skipped
    assert skipped[broken.isoformat()].startswith("duckdb_error:")


def test_days_skipped_caveat_names_each_session_in_prose_not_a_python_repr(tmp_path):
    """The caveat block is read by the owner, so the skip list must be prose, not `repr(list)`.

    2026-09-10 round-4 fix: it interpolated the raw list of dicts, so the caveat ended
    ``sessions skipped: [{'day': '2026-06-02', 'reason': 'not_compacted'}]`` - a Python literal
    inside an English sentence in a report whose whole job is to be read before the numbers are
    trusted. The format is now ``day (reason); day (reason)``.
    """
    root = tmp_path / "parquet"
    compacted, fragmented = _two_day_corpus(root)

    out = cfm.calibrate(root, max_samples=1_000_000, compacted_only=True)

    caveat = next(c for c in out.report["caveats"] if "sessions skipped:" in c)
    assert f"{fragmented.isoformat()} (not_compacted)" in caveat
    assert "[{" not in caveat and "'reason'" not in caveat


# --- (h) round 4: a loud warning when a run is too thin to be trusted as a replacement ------------
#
# WHY these are warnings and not a hard stop: a deliberately bounded run (--days / --compacted-only)
# is a legitimate, useful calibration, and the operator asked for it. What is NOT acceptable is that
# a run which read a small slice of the corpus, or which is about to publish a symbols table smaller
# than the one it overwrites, does so silently - the previous file's per-symbol half-spreads are the
# non-L1 fallbacks the paper broker charges, and losing them is a silent loosening of the model.


def test_a_run_that_read_under_half_the_candidate_sessions_warns_loudly(tmp_path, capsys):
    root = tmp_path / "parquet"
    days = _three_day_corpus(root)
    # Re-lay two of the three sessions as loose fragments so --compacted-only reads exactly one.
    for day in days[1:]:
        part = root / "ticks" / f"date={day.isoformat()}" / "symbol=KTWO"
        (part / "compact-ticks.parquet").unlink()
        frame = _stream("KTWO", 102, _at(9, 15, day=day), 900)
        _write_partition(root, day, frame.iloc[:450], filename="ticks-0001.parquet")
        _write_partition(root, day, frame.iloc[450:], filename="ticks-0002.parquet")

    rc = cfm.main([
        "--parquet-root", str(root), "--out", str(tmp_path / "fill_model.yaml"),
        "--report", str(tmp_path / "reports" / "calib"), "--compacted-only",
    ])

    assert rc == 0                       # a bounded run is legitimate - it warns, it does not fail
    err = capsys.readouterr().err
    assert "WARNING" in err
    assert "1 of 3 candidate session" in err


def test_a_shrinking_symbols_table_warns_before_it_replaces_the_file(tmp_path, capsys):
    """Publishing FEWER per-symbol half-spreads than the file being replaced is the silent-loosening
    shape: every dropped symbol falls back to `half_spread_fallback_ticks x tick_size` from the next
    paper session on, with nothing in the run's own output saying so."""
    out_yaml = tmp_path / "fill_model.yaml"
    out_yaml.write_text(
        "version: 1\nsymbols:\n"
        + "".join(f"  SYM{i}: {{median_half_spread_pct: 0.02, n_ticks: 10}}\n" for i in range(4)),
        encoding="utf-8",
    )
    root = tmp_path / "parquet"
    _three_day_corpus(root)                                  # one symbol, KTWO

    rc = cfm.main([
        "--parquet-root", str(root), "--out", str(out_yaml),
        "--report", str(tmp_path / "reports" / "calib"),
    ])

    assert rc == 0
    err = capsys.readouterr().err
    assert "WARNING" in err
    assert "1 symbol" in err and "4 symbol" in err
    assert yaml.safe_load(out_yaml.read_text(encoding="utf-8"))["source"]["symbols"] == 1


def _fixed_book(symbol: str, token: int, start: datetime, n: int, *,
                price: float, half: float) -> pd.DataFrame:
    """A constant price with a constant, symmetric L1 book - an exact half-spread median at an
    exact price level, which is what the fallback-comparison count is read off."""
    ts = [start + timedelta(milliseconds=SPACING_MS * i) for i in range(n)]
    return _frame(symbol, token, ts, [price] * n, [price - half] * n, [price + half] * n)


def test_report_counts_shipped_half_spreads_against_the_tick_fallback(tmp_path):
    """The report must state, from the data, how many shipped symbols resolve BELOW the 2-tick
    fallback - at a common reference price AND at each symbol's own price.

    WHY it is in the report at all (round 4): the fallback multiple is the model's ignorance
    default, and the question "is it a reasonable default?" is answered by the count of measured
    symbols that come in under it. On the shipped corpus that is a large minority at the reference
    price and a clear majority at real prices - which is the evidence behind the consumer's floor
    being HALF A TICK rather than the fallback. Two symbols pin both columns: NARROW is under the
    fallback either way, while CHEAP is over it at the Rs 1,000 reference and under it at its own
    Rs 100 price - so a single-column count cannot produce these numbers by accident.
    """
    root = tmp_path / "parquet"
    _write_partition(root, DAY, _fixed_book("CHEAP", 401, _at(10, 0), 300, price=100.0, half=0.05))
    _write_partition(root, DAY, _fixed_book("NARROW", 402, _at(10, 0), 300, price=1000.0, half=0.01))

    out = cfm.calibrate(root, max_samples=1_000_000)

    assert out.model.symbols["CHEAP"].median_half_spread_pct == pytest.approx(0.05, abs=1e-9)
    assert out.model.symbols["NARROW"].median_half_spread_pct == pytest.approx(0.001, abs=1e-9)

    cell = out.report["half_spread_vs_fallback"]
    assert cell["fallback_price"] == pytest.approx(0.10)        # 2 ticks x Rs 0.05
    assert cell["reference_ltp"] == 1000.0
    assert cell["n_symbols"] == 2
    assert cell["below_at_reference_ltp"] == 1                  # NARROW only
    assert cell["below_at_median_price"] == 2                   # NARROW and CHEAP
    assert cell["median_price"]["CHEAP"] == pytest.approx(100.0)

    text = cfm.render_report_md(out)
    assert "Half-spread vs fallback" in text
    assert "1 of 2" in text and "2 of 2" in text


def test_report_md_renders_the_coverage_table(tmp_path):
    root = tmp_path / "parquet"
    compacted, fragmented = _two_day_corpus(root)
    text = cfm.render_report_md(cfm.calibrate(root, max_samples=1_000_000))

    assert "## Coverage" in text
    assert compacted.isoformat() in text and fragmented.isoformat() in text
    assert "compacted" in text.lower()
    # The k table carries the basis, and the report states why the floor exists + that the fitted
    # quantiles are kept for the Phase-4 (G4) live-vs-paper recalibration.
    assert "| basis |" in text
    assert "G4" in text and "only tighten" in text.lower()


def test_cli_wires_the_bounding_flags(corpus, tmp_path):
    out_yaml = tmp_path / "fill_model.yaml"
    stem = tmp_path / "reports" / "calib"
    rc = cfm.main([
        "--parquet-root", str(corpus), "--out", str(out_yaml), "--report", str(stem),
        "--days", "1", "--symbols-per-day", "2", "--max-samples", "10000",
    ])

    assert rc == 0
    loaded = yaml.safe_load(out_yaml.read_text(encoding="utf-8"))
    assert loaded["source"]["symbols"] == 2
    assert stem.with_suffix(".md").exists() and stem.with_suffix(".json").exists()
    assert "## Coverage" in stem.with_suffix(".md").read_text(encoding="utf-8")


# --- (g) determinism over tied timestamps (2026-09-10 run finding) --------------------------------
#
# WHY: two back-to-back runs of the SAME command over the SAME 22 compacted sessions returned
# different numbers (open p75 0.0659 then 0.0656, p90 0.3071 then 0.3070) on an identical sample
# count. The tape carries several prints at one `exchange_ts`, and neither the sampling row_number
# nor the ASOF fill lookup ordered them totally - so "the tick at t" and "the first tick at/after
# t + 700 ms" were whichever row the scan happened to hand over. A calibration whose output moves
# between runs cannot be reviewed, diffed, or reproduced from `source.days`; this is the same total-
# ordering discipline WO-P3-3 pins for the replay stream.

TIE_HI = 10.0             # the second print at each instant sits this far above the first


def _tied_stream(symbol: str, token: int, start: datetime, n_ts: int) -> pd.DataFrame:
    """Two prints at EVERY timestamp: `lo` and `lo + TIE_HI`, no L1.

    With no L1 the half-spread is 0, so the residual is exactly |fill - mid| and the tie-break is
    directly readable off the numbers: sampling `lo` and filling at the next instant's `hi` gives
    TIE_HI, while any other pairing gives ~0.
    """
    ts, ltp = [], []
    for i in range(n_ts):
        t = start + timedelta(milliseconds=SPACING_MS * i)
        minute = (t.replace(second=0, microsecond=0)
                  - start.replace(second=0, microsecond=0)).seconds // 60
        lo = BASE + (EPS if (minute % 2) else 0.0)
        ts += [t, t]
        ltp += [lo, lo + TIE_HI]
    zeros = [0.0] * len(ts)
    return _frame(symbol, token, ts, ltp, zeros, zeros)


def test_tied_timestamps_resolve_to_a_deterministic_sample_and_fill(tmp_path):
    """A stride over tied prints must take the FIRST in a total order, and fill at the last print.

    200 instants x 2 prints = 400 rows; `max_samples=200` forces stride 2, which under a total
    ordering (exchange_ts, ltp, bid, ask) selects exactly the 200 `lo` rows. Each is filled at the
    next instant's representative print, pinned to the highest at that instant - so every residual
    is TIE_HI and the median normalised sample is TIE_HI / (sigma_1m). Sampling `hi`, or filling at
    the next `lo`, collapses the residual to ~0 instead.
    """
    root = tmp_path / "parquet"
    _write_partition(root, DAY, _tied_stream("TIEBREAK", 301, _at(10, 0), 200))

    out = cfm.calibrate(root, max_samples=200, days=None)

    # 200 instants x 750 ms from 10:00:00 spans 149.25 s, i.e. the minutes 10:00 / 10:01 / 10:02;
    # each minute closes on its highest print (the same tie-break the fill lookup uses).
    closes = np.array([BASE + (EPS if (m % 2) else 0.0) + TIE_HI for m in range(3)])
    sigma_ret = float(np.std(closes[1:] / closes[:-1] - 1.0, ddof=1))
    expected = TIE_HI / (sigma_ret * BASE)

    cell = out.report["buckets"]["mid"]
    assert cell["n"] == 199        # 200 sampled; the last instant has no tick 700 ms later
    assert out.report["samples_dropped"]["no_forward_fill"] == 1
    assert cell["p50"] == pytest.approx(expected, rel=0.05)
    assert cell["p50"] > 1.0              # not the ~0 that a mis-ordered tie-break produces

    # And the whole thing is reproducible: same corpus, same command, same numbers.
    again = cfm.calibrate(root, max_samples=200, days=None)
    assert again.report["buckets"] == out.report["buckets"]
    assert again.model.model_dump(exclude={"generated_at"}) == \
        out.model.model_dump(exclude={"generated_at"})
