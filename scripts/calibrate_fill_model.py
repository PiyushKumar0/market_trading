#!/usr/bin/env python
"""Fill-model calibration (2026-09-10).

The PaperBroker's fill model is `slippage = half-spread + k * sigma_1m`; both terms come from this
account's recorded tape, not a literature constant. The script produces `config/fill_model.yaml`
(non-protected config: platform-written, owner informed) plus the evidence report behind every number.

Design rules:

* **k per time-of-day bucket, never one global k.** The corpus is window-biased (the engine runs
  mostly in the morning), so a blended k understates the close / square-off period. Buckets are
  pinned: open 09:15-10:00, mid 10:00-14:30, close 14:30-15:30.
* **k = the p75 of the normalised samples, not the mean** - a mean fill model flatters the ledger.
  p50/p90 are reported alongside.
* **A calibration may only tighten the model.** The shipped k is `max(fitted p75, the consumer's
  default for that bucket)` (`CONSUMER_DEFAULT_K`, mirrored from `engine.paper.fill_model`). The
  residual `max(|fill - mid| - half_spread, 0)` is zero-inflated (over 700 ms a liquid name usually
  has not left its quote), and the corpus is window-biased and survivorship-tainted (only names the
  universe builder admitted); such evidence cannot justify charging LESS slippage than the
  uncalibrated model - that would be silent optimism in what the paper-to-live gate reads.
* **The fitted quantiles ship anyway**: each bucket carries `p50`, `p75`, `p90`, `n` and a `basis`
  in `{fitted, floored_to_default, thin, degenerate}`, so the config is its own evidence and the
  live-vs-paper fill-deviation tracker can recalibrate against it.
* **A thin bucket is not fitted**: under `THIN_BUCKET_MIN_SAMPLES` (500) a p75 is noise; it ships
  the consumer default with `basis: thin`.
* **A degenerate bucket is not fitted**: a fitted p50 <= 0 means the estimator collapsed onto the
  zero-inflated mass; it ships the default with `basis: degenerate` and the report says the
  estimator (higher quantile, signed adverse-only residual, longer horizon) may need re-pinning.
* **half-spread is never 0 and is published as a PERCENT of mid.** L1 is absent on backfilled /
  WARMING spans, so a non-L1 tick is charged the symbol-day's own median half-spread, else the
  day's corpus median, else 0 (0 only inflates the residual attributed to k, so it errs
  conservative). `symbols[].median_half_spread_pct` is a percent; the consumer
  `engine.paper.fill_model.half_spread()` divides by 100. `tests/unit/test_fill_model.py` loads the
  shipped YAML through the real consumer loader so the two cannot drift.
* **The per-symbol median is NOT clamped up to the tick fallback.** `half_spread_fallback_ticks x
  tick_size` is an ignorance default for unmeasured symbols, not a physical limit, and many names
  quote one tick wide. The consumer floors the calibrated branch at HALF A TICK instead. The report
  counts symbols under the fallback both ways ("Half-spread vs fallback").
* **A run that cannot responsibly replace the previous file says so**: two loud stderr warnings,
  never a hard stop - (a) fewer than half the candidate sessions were read; (b) the new `symbols`
  table is smaller than the one being replaced (measured half-spreads would disappear).

It reads Parquet directly: `market.duckdb` is held by the live engine, so the script opens its own
in-memory DuckDB over `data/parquet/ticks/date=*/symbol=*/*.parquet` and can run beside a session.
The archive is compacted concurrently, so a day whose files vanish mid-scan is retried and, if it
still fails, recorded in `days_skipped` rather than silently dropped.

The run is bounded because an un-compacted session holds thousands of tiny fragments per symbol
(17-33 s to read one symbol-day). Controls: `--days N` (most recent N sessions), `--symbols-per-day N`
(deterministic every-k-th stride over the SORTED symbol list, never a head slice, which would
calibrate the alphabet), `--compacted-only`. Whatever is skipped is named in `days_skipped`, and the
report carries a per-day COVERAGE table so the owner reads what was actually measured.

Every step is DuckDB SQL; the only Python touching per-tick data is a numpy percentile over the
sampled residuals - a per-row Python loop here is a measured pathology.

Method:

1. Session ticks only (09:15 <= t < 15:30, exchange_ts in IST).
2. half-spread pct per tick = (ask - bid) / 2 / mid * 100 (a PERCENT) on ticks with `ask > bid > 0`;
   published per symbol as an exact corpus median (accumulated as a rounded-value frequency table,
   so the median is exact across all days without holding every tick in memory) with the corpus-wide
   distribution reported as the fallback reference.
3. sigma_1m per symbol-day = stdev of close-to-close returns of the minute resample (last ltp per
   minute), expressed in price units at the decision tick as `sigma_ret * mid_t`.
4. Slippage samples: every Nth in-session tick per symbol-day (N chosen so the whole run stays under
   `--max-samples`); decision mid `m_t = (bid+ask)/2` (or ltp with no L1); fill = the first ltp with
   `exchange_ts >= t + 700 ms` (an ASOF forward join); sample = `max(|fill - m_t| - half_spread_t, 0)`
   normalised by `sigma_1m` at t. Samples with no forward tick, or with sigma_1m == 0 (a flat tape
   cannot normalise), are dropped and counted.
5. k[bucket] = max(p75 of the normalised samples in that bucket, `CONSUMER_DEFAULT_K[bucket]`).

Standalone / blocking offline research tool - it places no orders, imports nothing from
`engine.oms` / `engine.risk`, and is not reachable from the RECOMMEND pipeline.

Exit codes: 0 = calibrated (outputs written); 2 = no readable tick partitions under
`--parquet-root`; 3 = every candidate session was skipped, so NOTHING was written (a run with no
evidence must not replace the previous per-symbol half-spreads with an empty table); `days_skipped`
is printed with reasons.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
import time as _time
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any, Literal

_REPO_SRC = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
if _REPO_SRC not in sys.path:  # pragma: no cover - loose-script shim
    sys.path.insert(0, _REPO_SRC)

import duckdb  # noqa: E402
import numpy as np  # noqa: E402
import yaml  # noqa: E402
from pydantic import BaseModel, ConfigDict  # noqa: E402

from engine.core.clock import IST  # noqa: E402

# --- pinned constants (the YAML contract; changing one changes the consumer's contract) ----------

SCHEMA_VERSION = 1
LATENCY_MS = 700                    # plan 3.2.9 simulated order latency
HALF_SPREAD_FALLBACK_TICKS = 2      # plan 3.2.9 "tick_size x multiple" fallback, never 0
THIN_BUCKET_MIN_SAMPLES = 500       # under this a p75 is noise -> consumer default instead
K_QUANTILE = 75.0                   # conservative, not the mean
#: The consumer's own defaults (`engine.paper.fill_model.DEFAULT_K_EDGE / DEFAULT_K_MID`), mirrored
#: rather than imported: this script must not import `engine.paper`. They are the FLOOR on k; keep
#: in step with `engine.paper.fill_model`.
CONSUMER_DEFAULT_K: dict[str, float] = {"open": 1.0, "mid": 0.5, "close": 1.0}
SESSION_START = "09:15:00"
SESSION_END = "15:30:00"
#: (name, start, end) - end-exclusive, by exchange_ts time of day in IST.
BUCKETS: tuple[tuple[str, str, str], ...] = (
    ("open", "09:15", "10:00"),
    ("mid", "10:00", "14:30"),
    ("close", "14:30", "15:30"),
)
DEFAULT_MAX_SAMPLES = 1_000_000
#: Quantum of the per-symbol half-spread frequency table, as a FRACTION of mid. Half-spreads are
#: tick-quantised, so 1e-6 of mid loses nothing real and makes the per-symbol median an exact
#: frequency-table computation; finer blows up the Counter cardinality.
HS_FRACTION_ROUND = 6
#: Decimals kept on the published PERCENT (`median_half_spread_pct`); lossless against the histogram.
HS_PCT_ROUND = 6
#: Decimals of the per-symbol reference-price frequency table (report only, never shipped). Rounded
#: to the rupee: a finer table's cardinality is every traded price, and the only question asked is
#: whether a half-spread lands over or under a 10-paisa fallback.
PRICE_ROUND = 0
#: Reference price and tick of the report's "half-spread vs fallback" comparison; the same
#: reference `tests/unit/test_fill_model.py` uses.
FALLBACK_REFERENCE_LTP = 1000.0
FALLBACK_REFERENCE_TICK = 0.05
_DAY_RETRIES = 3                    # the live engine compacts the archive under us
_RETRY_SLEEP_S = 1.0                # backoff between re-globs (monkeypatched to 0 in unit tests)
EXIT_NO_PARTITIONS = 2
EXIT_NO_SESSIONS = 3


def _tod(column: str) -> str:
    """SQL for the IST time-of-day of a TIMESTAMPTZ column.

    The double cast is needed: DuckDB refuses TIMESTAMPTZ -> TIME directly; via TIMESTAMP the instant
    renders in the session time zone, which `calibrate()` pins to Asia/Kolkata.
    """
    return f"CAST(CAST({column} AS TIMESTAMP) AS TIME)"


# --- the YAML contract, as a strict model --------------------------------------------------------
# The consumer (engine.paper) has its own loader for this contract; this model stops the producer
# emitting a shape the contract disallows. extra="forbid": a typo'd key must fail loudly.

class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class SourceModel(_Strict):
    """Provenance of the calibration - which days/symbols/ticks produced these numbers."""

    days: list[str]
    symbols: int
    ticks: int


#: How the shipped k was arrived at: `fitted` = p75 cleared the consumer default; `floored_to_default`
#: = a real fit below the default, so the default ships; `thin` / `degenerate` = no usable fit (too
#: few samples / p50 collapsed onto the zero mass), default ships, quantiles are evidence only.
Basis = Literal["fitted", "floored_to_default", "thin", "degenerate"]


class BucketModel(_Strict):
    """One time-of-day bucket: the shipped k, the `basis` that produced it, and the fitted
    quantiles kept verbatim as recalibration evidence."""

    start: str
    end: str
    k: float
    basis: Basis
    n: int
    p50: float
    p75: float
    p90: float


class SymbolModel(_Strict):
    """Per-symbol half-spread fallback, as a PERCENT of mid (the consumer divides by 100).
    `n_ticks` counts the L1 ticks the median was taken over."""

    median_half_spread_pct: float
    n_ticks: int


class FillModel(_Strict):
    """`config/fill_model.yaml` - the pinned contract, version 1."""

    version: Literal[1]
    generated_at: datetime
    source: SourceModel
    latency_ms: int
    buckets: dict[str, BucketModel]
    half_spread_fallback_ticks: int
    symbols: dict[str, SymbolModel]


@dataclass
class Calibration:
    """The calibrated model plus the evidence report that justifies it."""

    model: FillModel
    report: dict[str, Any]


@dataclass
class _Accumulator:
    """Cross-day accumulators. Nothing here grows with the tick count except `bucket_z`, which is
    bounded by `--max-samples`; the spread stat is a frequency table, not a sample."""

    spread_hist: dict[str, Counter] = field(default_factory=lambda: defaultdict(Counter))
    #: symbol -> rounded-price -> tick count. Report-only (the fallback comparison below), never
    #: shipped in the YAML; bounded by the distinct rupee price levels each name traded at.
    price_hist: dict[str, Counter] = field(default_factory=lambda: defaultdict(Counter))
    bucket_z: dict[str, list[np.ndarray]] = field(default_factory=lambda: defaultdict(list))
    hour_hist: Counter = field(default_factory=Counter)
    symbols: set[str] = field(default_factory=set)
    days: list[str] = field(default_factory=list)
    days_skipped: list[dict[str, str]] = field(default_factory=list)
    #: one row per session actually read - the honest coverage statement for a bounded run.
    coverage: list[dict[str, Any]] = field(default_factory=list)
    session_ticks: int = 0
    l1_ticks: int = 0
    sampled: int = 0
    dropped_no_fill: int = 0
    dropped_no_sigma: int = 0

    def merge(self, other: _Accumulator) -> None:
        """Fold a completed session's accumulator into the run's.

        A session is accumulated aside and merged only on success: `_process_day` mutates as it
        goes and a DuckDB failure is retried, so writing in place would double-count a half-processed day.
        """
        for symbol, hist in other.spread_hist.items():
            self.spread_hist[symbol].update(hist)
        for symbol, hist in other.price_hist.items():
            self.price_hist[symbol].update(hist)
        for name, blocks in other.bucket_z.items():
            self.bucket_z[name].extend(blocks)
        self.hour_hist.update(other.hour_hist)
        self.symbols |= other.symbols
        self.days += other.days
        self.days_skipped += other.days_skipped
        self.coverage += other.coverage
        self.session_ticks += other.session_ticks
        self.l1_ticks += other.l1_ticks
        self.sampled += other.sampled
        self.dropped_no_fill += other.dropped_no_fill
        self.dropped_no_sigma += other.dropped_no_sigma


# --- corpus discovery ----------------------------------------------------------------------------

def discover_days(parquet_root: Path) -> list[date]:
    """The `date=YYYY-MM-DD` partitions under `<root>/ticks`, ascending. Malformed names are
    ignored rather than fatal - the archive also holds `.compact_spill` scratch directories."""
    days: list[date] = []
    tick_root = Path(parquet_root) / "ticks"
    if not tick_root.is_dir():
        return days
    for entry in sorted(tick_root.iterdir()):
        if not entry.is_dir() or not entry.name.startswith("date="):
            continue
        try:
            days.append(date.fromisoformat(entry.name.removeprefix("date=")))
        except ValueError:
            continue
    return days


def _day_dir(parquet_root: Path, day: date) -> Path:
    return Path(parquet_root) / "ticks" / f"date={day.isoformat()}"


def discover_symbols(parquet_root: Path, day: date) -> list[str]:
    """The `symbol=SYM` partitions under one session, sorted. Cheap: one directory listing."""
    day_dir = _day_dir(parquet_root, day)
    if not day_dir.is_dir():
        return []
    return sorted(
        entry.name.removeprefix("symbol=")
        for entry in day_dir.iterdir()
        if entry.is_dir() and entry.name.startswith("symbol=")
    )


def select_symbols(symbols: list[str], limit: int | None) -> list[str]:
    """Bound a session to EXACTLY `limit` symbols, spread over the SORTED list.

    A spread, not a head slice (which would calibrate the alphabet's first names). Index-rounding,
    not a fixed integer stride `symbols[::ceil(M/limit)]`, which under-delivers (M=5, limit=4 gives
    3). `round(i * (M-1) / (limit-1))` lands exactly `limit` strictly increasing indices (spacing
    >= 1 when limit <= M), keeps both endpoints, and is deterministic. The dedup is belt-and-braces.
    """
    if limit is None or limit <= 0 or len(symbols) <= limit:
        return list(symbols)
    if limit == 1:
        return [symbols[0]]
    span = len(symbols) - 1
    picked = [symbols[round(i * span / (limit - 1))] for i in range(limit)]
    return list(dict.fromkeys(picked))


def _is_compacted(parquet_root: Path, day: date, symbols: list[str]) -> bool:
    """True when every selected symbol-day is a single `compact-ticks.parquet`.

    Early-exits at the second entry: counting thousands of fragments costs real disk time beside a
    live session.
    """
    day_dir = _day_dir(parquet_root, day)
    for symbol in symbols:
        seen: list[str] = []
        try:
            with os.scandir(day_dir / f"symbol={symbol}") as entries:
                for entry in entries:
                    seen.append(entry.name)
                    if len(seen) > 1:
                        return False
        except OSError:                                   # compaction removed it mid-scan
            return False
        if seen != ["compact-ticks.parquet"]:
            return False
    return bool(symbols)


def _day_globs(parquet_root: Path, day: date, symbols: list[str] | None) -> list[str]:
    """Glob(s) for a session: the whole day when unbounded, else one per selected symbol.

    Naming the selected symbol dirs (not filtering in SQL) makes `--symbols-per-day` an I/O bound:
    skipped symbols' files are never opened.
    """
    day_dir = _day_dir(parquet_root, day)
    if symbols is None:
        return [(day_dir / "symbol=*" / "*.parquet").as_posix()]
    return [(day_dir / f"symbol={symbol}" / "*.parquet").as_posix() for symbol in symbols]


# --- per-day calibration pass --------------------------------------------------------------------

def _load_day(
    con: duckdb.DuckDBPyConnection, parquet_root: Path, day: date, symbols: list[str] | None
) -> int:
    """Materialise one session's in-session ticks into `day_ticks`; return the row count.

    Materialised once because every later step reads the same rows (one file-open pass, not five).
    """
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE day_ticks AS
        SELECT tradingsymbol AS symbol,
               exchange_ts,
               CAST(ltp AS DOUBLE) AS ltp,
               CASE WHEN bid > 0 AND ask > bid THEN CAST(bid AS DOUBLE) END AS bid,
               CASE WHEN bid > 0 AND ask > bid THEN CAST(ask AS DOUBLE) END AS ask
        FROM read_parquet(?)
        WHERE ltp > 0
          AND {_tod('exchange_ts')} >= TIME '{SESSION_START}'
          AND {_tod('exchange_ts')} <  TIME '{SESSION_END}'
        """,
        [_day_globs(parquet_root, day, symbols)],
    )
    return int(con.execute("SELECT count(*) FROM day_ticks").fetchone()[0])


def _bucket_case(column: str) -> str:
    """The pinned time-of-day bucketing, as a SQL CASE over `column`'s time part."""
    (_, _, open_end), (_, _, mid_end), _ = BUCKETS
    return (
        f"CASE WHEN {_tod(column)} < TIME '{open_end}:00' THEN 'open'"
        f"     WHEN {_tod(column)} < TIME '{mid_end}:00' THEN 'mid'"
        f"     ELSE 'close' END"
    )


def _process_day(
    con: duckdb.DuckDBPyConnection,
    parquet_root: Path,
    day: date,
    per_day_budget: int,
    acc: _Accumulator,
    *,
    symbols: list[str] | None,
    available: int,
    compacted: bool,
    started: float,
) -> None:
    n_ticks = _load_day(con, parquet_root, day, symbols)
    if n_ticks == 0:
        acc.days_skipped.append({"day": day.isoformat(), "reason": "no_session_ticks"})
        return

    l1_ticks = int(
        con.execute("SELECT count(*) FILTER (WHERE bid IS NOT NULL) FROM day_ticks").fetchone()[0]
    )
    acc.session_ticks += n_ticks
    acc.l1_ticks += l1_ticks
    acc.days.append(day.isoformat())
    coverage_row: dict[str, Any] = {
        "day": day.isoformat(),
        "symbols_available": available,
        "symbols_used": len(symbols) if symbols is not None else available,
        "compacted": compacted,
        "ticks": n_ticks,
        "l1_ticks": l1_ticks,
        "elapsed_s": 0.0,      # filled at the end of this pass, once the day is actually done
    }
    acc.coverage.append(coverage_row)
    for symbol, in con.execute("SELECT DISTINCT symbol FROM day_ticks").fetchall():
        acc.symbols.add(symbol)
    for hour, count in con.execute(
        "SELECT CAST(extract('hour' FROM exchange_ts) AS INTEGER), count(*) FROM day_ticks GROUP BY 1"
    ).fetchall():
        acc.hour_hist[int(hour)] += int(count)

    # (2) half-spread frequency table, per symbol (exact corpus median later), as a PERCENT of mid.
    for symbol, value, count in con.execute(
        f"""
        SELECT symbol,
               round((ask - bid) / 2.0 / ((ask + bid) / 2.0), {HS_FRACTION_ROUND}) * 100.0,
               count(*)
        FROM day_ticks WHERE bid IS NOT NULL GROUP BY 1, 2
        """
    ).fetchall():
        acc.spread_hist[symbol][float(value)] += int(count)

    # Per-symbol price level, for the report's half-spread-vs-fallback comparison only (a percent
    # of mid needs the traded price to be compared with a fixed-rupee fallback).
    for symbol, value, count in con.execute(
        f"SELECT symbol, round(ltp, {PRICE_ROUND}), count(*) FROM day_ticks GROUP BY 1, 2"
    ).fetchall():
        acc.price_hist[symbol][float(value)] += int(count)

    # Non-L1 fallback ladder: the symbol-day's own median, else the day's median; both as a FRACTION
    # of mid (not the published percent), applied at the decision tick's mid.
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE day_hs AS
        SELECT symbol, median((ask - bid) / 2.0 / ((ask + bid) / 2.0)) AS hs_pct
        FROM day_ticks WHERE bid IS NOT NULL GROUP BY 1
        """
    )
    day_median = con.execute(
        "SELECT median((ask - bid) / 2.0 / ((ask + bid) / 2.0)) FROM day_ticks WHERE bid IS NOT NULL"
    ).fetchone()[0]
    # Last rung 0.0: charging 0 spread attributes the whole move to k, inflating rather than flattering it.
    day_median_sql = "0.0" if day_median is None else repr(float(day_median))

    # (3) sigma_1m per symbol-day, from the minute resample (last ltp per minute).
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE day_sigma AS
        SELECT symbol, stddev_samp(r) AS sigma_ret, count(*) AS n_ret
        FROM (
            SELECT symbol, c / lag(c) OVER (PARTITION BY symbol ORDER BY m) - 1.0 AS r
            FROM (
                -- ORDER BY carries `ltp` as a tiebreak: several prints share one exchange_ts, and
                -- `last(... ORDER BY exchange_ts)` alone would pick an arbitrary one per run.
                SELECT symbol, date_trunc('minute', exchange_ts) AS m,
                       last(ltp ORDER BY exchange_ts, ltp) AS c
                FROM day_ticks GROUP BY 1, 2
            )
        )
        WHERE r IS NOT NULL
        GROUP BY 1
        """
    )

    # (4) sampled decision ticks: a per-symbol every-Nth (keeps the run under --max-samples without
    # biasing a symbol or hour). Anchored at row 1 (`(rn - 1) % stride`): `rn % stride = 0` samples
    # nothing from a symbol with fewer than `stride` ticks, silently deleting the illiquid tail.
    stride = max(1, math.ceil(n_ticks / max(1, per_day_budget)))
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE day_samples AS
        SELECT symbol, exchange_ts, mid, hs_price,
               exchange_ts + INTERVAL {LATENCY_MS} MILLISECOND AS target_ts
        FROM (
            SELECT symbol, exchange_ts,
                   CASE WHEN bid IS NOT NULL THEN (bid + ask) / 2.0 ELSE ltp END AS mid,
                   CASE WHEN bid IS NOT NULL THEN (ask - bid) / 2.0 END AS hs_price,
                   -- TOTAL order, not just exchange_ts: the tape carries several prints at one
                   -- instant, so ordering on the timestamp alone makes "the tick at t" whichever
                   -- row the scan handed over - the calibration then moves between runs
                   -- (measured 2026-09-10: open p75 0.0659 then 0.0656 on identical inputs).
                   row_number() OVER (
                       PARTITION BY symbol ORDER BY exchange_ts, ltp, bid, ask
                   ) AS rn
            FROM day_ticks
        )
        WHERE (rn - 1) % {stride} = 0
        """
    )

    # One fill candidate per (symbol, instant): ASOF over several prints at one exchange_ts would
    # return an arbitrary row. The HIGHEST print is the representative - a value rule, reproducible
    # across scan orders.
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE day_fills AS
        SELECT symbol, exchange_ts, max(ltp) AS ltp FROM day_ticks GROUP BY 1, 2
        """
    )

    # The ASOF forward join IS the 700 ms latency: for each decision tick it finds the EARLIEST tick
    # at or after t + 700 ms, which is the first price the order could actually have touched.
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE day_z AS
        SELECT {_bucket_case('s.exchange_ts')} AS bucket,
               f.ltp AS fill,
               g.sigma_ret AS sigma_ret,
               CASE WHEN f.ltp IS NOT NULL AND g.sigma_ret > 0 THEN
                   greatest(
                       abs(f.ltp - s.mid)
                       - coalesce(s.hs_price, h.hs_pct * s.mid, {day_median_sql} * s.mid),
                       0.0
                   ) / (g.sigma_ret * s.mid)
               END AS z
        FROM day_samples s
        ASOF LEFT JOIN day_fills f
             ON s.symbol = f.symbol AND s.target_ts <= f.exchange_ts
        LEFT JOIN day_sigma g ON g.symbol = s.symbol
        LEFT JOIN day_hs h ON h.symbol = s.symbol
        """
    )
    total, no_fill, no_sigma = con.execute(
        """
        SELECT count(*),
               count(*) FILTER (WHERE fill IS NULL),
               count(*) FILTER (WHERE fill IS NOT NULL AND coalesce(sigma_ret, 0) <= 0)
        FROM day_z
        """
    ).fetchone()
    acc.sampled += int(total)
    acc.dropped_no_fill += int(no_fill)
    acc.dropped_no_sigma += int(no_sigma)

    for name, _, _ in BUCKETS:
        block = con.execute(
            "SELECT z FROM day_z WHERE z IS NOT NULL AND bucket = ?", [name]
        ).fetchnumpy()["z"]
        if len(block):
            acc.bucket_z[name].append(np.asarray(block, dtype="float64"))

    coverage_row["elapsed_s"] = round(_time.perf_counter() - started, 2)


# --- aggregation ---------------------------------------------------------------------------------

def _weighted_median(hist: Counter) -> float:
    """Exact lower median of a value->count frequency table (values already tick-quantised)."""
    items = sorted(hist.items())
    total = sum(n for _, n in items)
    run = 0
    for value, n in items:
        run += n
        if run * 2 >= total:
            return float(value)
    return float(items[-1][0])


def calibrate(
    parquet_root: Path | str,
    *,
    max_samples: int = DEFAULT_MAX_SAMPLES,
    days: int | None = None,
    symbols_per_day: int | None = None,
    compacted_only: bool = False,
    exclude: set[date] | None = None,
    progress: Any = None,
) -> Calibration:
    """Calibrate the fill model over the Parquet tick archive under `parquet_root`.

    `days` keeps only the most recent N sessions (None = the whole corpus). `symbols_per_day` bounds
    each session to a deterministic index-rounding spread over its sorted symbol list (None = all).
    `compacted_only` skips any session still held as loose fragments - the I/O-modesty switch for
    running beside a live trading session. `exclude` drops named sessions BEFORE the `days` cut -
    the caller passes today when a session is still in progress, because a half-recorded day is a
    pure open-bucket sample and would tilt the corpus. `progress`, when given, is called as
    `progress(day, index, total, elapsed_s)` after each session.
    """
    root = Path(parquet_root)
    all_days = [d for d in discover_days(root) if not exclude or d not in exclude]
    if days is not None:
        all_days = all_days[-days:]
    if not all_days:
        raise FileNotFoundError(f"no date=* tick partitions under {root / 'ticks'}")

    per_day_budget = max(1, max_samples // len(all_days))
    acc = _Accumulator()
    con = duckdb.connect()
    try:
        con.execute("SET TimeZone='Asia/Kolkata'")   # CAST(TIMESTAMPTZ AS TIME) is session-tz bound
        for index, day in enumerate(all_days, start=1):
            started = _time.perf_counter()
            available = discover_symbols(root, day)
            chosen = select_symbols(available, symbols_per_day)
            compacted = _is_compacted(root, day, chosen)
            if compacted_only and not compacted:
                acc.days_skipped.append({"day": day.isoformat(), "reason": "not_compacted"})
                if progress is not None:
                    progress(day, index, len(all_days), _time.perf_counter() - started)
                continue
            for attempt in range(1, _DAY_RETRIES + 1):
                # Accumulated aside and merged only on success - see `_Accumulator.merge`.
                scratch = _Accumulator()
                try:
                    _process_day(
                        con, root, day, per_day_budget, scratch,
                        symbols=chosen if symbols_per_day else None,
                        available=len(available),
                        compacted=compacted,
                        started=started,
                    )
                    acc.merge(scratch)
                    break
                except duckdb.Error as exc:
                    # The live engine compacts this archive: a globbed file can be gone or
                    # half-written by the time it is opened. Re-glob and retry; give up on ONE
                    # DAY (named in `days_skipped`), not the run.
                    # `duckdb.Error`, not IOException: a torn fragment raises InvalidInputException
                    # ("No magic bytes found"). A genuine SQL defect also lands in `days_skipped`,
                    # for every day, with the DuckDB message - loud enough to diagnose.
                    if attempt == _DAY_RETRIES:
                        acc.days_skipped.append(
                            {"day": day.isoformat(), "reason": f"duckdb_error: {exc}"[:200]}
                        )
                    else:
                        _time.sleep(_RETRY_SLEEP_S)
            if progress is not None:
                progress(day, index, len(all_days), _time.perf_counter() - started)
    finally:
        con.close()

    return _finalise(
        acc, days_requested=days, symbols_per_day=symbols_per_day, compacted_only=compacted_only,
        days_candidate=len(all_days),
    )


def _finalise(
    acc: _Accumulator,
    *,
    days_requested: int | None = None,
    symbols_per_day: int | None = None,
    compacted_only: bool = False,
    days_candidate: int | None = None,
) -> Calibration:
    buckets: dict[str, BucketModel] = {}
    bucket_report: dict[str, dict[str, Any]] = {}
    for name, start, end in BUCKETS:
        blocks = acc.bucket_z.get(name, [])
        z = np.concatenate(blocks) if blocks else np.empty(0, dtype="float64")
        n = int(z.size)
        raw_p50, raw_p75, raw_p90 = (
            (float(np.percentile(z, 50)), float(np.percentile(z, K_QUANTILE)), float(np.percentile(z, 90)))
            if n else (0.0, 0.0, 0.0)
        )
        # Rounding is publication only: the classifier reads the raw quantiles (a true p50 of 3e-5
        # rounded first would be mislabelled `degenerate`, a finding escalated to the owner).
        p50, p75, p90 = round(raw_p50, 4), round(raw_p75, 4), round(raw_p90, 4)
        default = CONSUMER_DEFAULT_K[name]
        # k policy (module docstring for the why): thin / degenerate -> the consumer default;
        # otherwise max(fit, default). The floor compares the ROUNDED p75, the value that would
        # ship, so a `fitted` bucket never publishes a k equal to the default it supposedly cleared.
        if n < THIN_BUCKET_MIN_SAMPLES or not math.isfinite(raw_p75):
            basis, k = "thin", default
        elif raw_p50 <= 0.0:
            basis, k = "degenerate", default
        elif p75 <= default:
            basis, k = "floored_to_default", default
        else:
            basis, k = "fitted", p75
        buckets[name] = BucketModel(
            start=start, end=end, k=k, basis=basis, n=n, p50=p50, p75=p75, p90=p90,
        )
        bucket_report[name] = {
            "start": start, "end": end, "n": n,
            "p50": p50, "p75": p75, "p90": p90,
            "k": k, "basis": basis, "consumer_default_k": default,
            "share_of_samples": round(n / acc.sampled, 4) if acc.sampled else 0.0,
        }

    symbols: dict[str, SymbolModel] = {}
    for symbol in sorted(acc.spread_hist):
        hist = acc.spread_hist[symbol]
        symbols[symbol] = SymbolModel(
            median_half_spread_pct=round(_weighted_median(hist), HS_PCT_ROUND),
            n_ticks=int(sum(hist.values())),
        )

    # How many measured symbols come in under the ignorance fallback (`half_spread_fallback_ticks x
    # tick_size`), counted twice: at one reference price (compares the SPREADS) and at each symbol's
    # own median price (what the paper broker actually resolves).
    fallback_price = HALF_SPREAD_FALLBACK_TICKS * FALLBACK_REFERENCE_TICK
    median_price = {
        symbol: _weighted_median(acc.price_hist[symbol])
        for symbol in symbols
        if acc.price_hist.get(symbol)
    }
    below_reference = sum(
        1 for cell in symbols.values()
        if FALLBACK_REFERENCE_LTP * cell.median_half_spread_pct / 100.0 < fallback_price
    )
    below_real = sum(
        1 for symbol, cell in symbols.items()
        # A symbol with no price level is measured at the reference, counted rather than dropped.
        if median_price.get(symbol, FALLBACK_REFERENCE_LTP) * cell.median_half_spread_pct / 100.0
        < fallback_price
    )
    hs_vs_fallback = {
        "reference_ltp": FALLBACK_REFERENCE_LTP,
        "reference_tick": FALLBACK_REFERENCE_TICK,
        "fallback_ticks": HALF_SPREAD_FALLBACK_TICKS,
        "fallback_price": fallback_price,
        "n_symbols": len(symbols),
        "below_at_reference_ltp": below_reference,
        "below_at_median_price": below_real,
        "median_price": {s: round(p, 2) for s, p in sorted(median_price.items())},
    }

    # Percentiles of the per-symbol medians, in the same PERCENT unit as the published field.
    medians = np.array([s.median_half_spread_pct for s in symbols.values()], dtype="float64")
    hs_summary = {
        "n_symbols": int(medians.size),
        "p10": round(float(np.percentile(medians, 10)), 6) if medians.size else 0.0,
        "p25": round(float(np.percentile(medians, 25)), 6) if medians.size else 0.0,
        "p50": round(float(np.percentile(medians, 50)), 6) if medians.size else 0.0,
        "p75": round(float(np.percentile(medians, 75)), 6) if medians.size else 0.0,
        "p90": round(float(np.percentile(medians, 90)), 6) if medians.size else 0.0,
    }

    model = FillModel(
        version=SCHEMA_VERSION,
        generated_at=datetime.now(IST),
        source=SourceModel(days=list(acc.days), symbols=len(acc.symbols), ticks=acc.session_ticks),
        latency_ms=LATENCY_MS,
        buckets=buckets,
        half_spread_fallback_ticks=HALF_SPREAD_FALLBACK_TICKS,
        symbols=symbols,
    )

    no_l1 = acc.session_ticks - acc.l1_ticks
    report: dict[str, Any] = {
        "generated_at": model.generated_at.isoformat(),
        "schema_version": SCHEMA_VERSION,
        "latency_ms": LATENCY_MS,
        "days": list(acc.days),
        "days_skipped": list(acc.days_skipped),
        "coverage": list(acc.coverage),
        # Sessions this run was pointed at after --days/exclude: the denominator of the publish-time
        # "read enough to replace the file?" warning. Not `days_requested` (the flag) nor len(days).
        "days_candidate": len(acc.days) if days_candidate is None else days_candidate,
        "days_requested": days_requested,
        "symbols_per_day": symbols_per_day,
        "compacted_only": compacted_only,
        "symbols": len(acc.symbols),
        "session_ticks": acc.session_ticks,
        "l1_ticks": acc.l1_ticks,
        "no_l1_ticks": no_l1,
        "no_l1_share": (no_l1 / acc.session_ticks) if acc.session_ticks else 0.0,
        "sampled_ticks": acc.sampled,
        "samples_dropped": {
            "no_forward_fill": acc.dropped_no_fill,
            "no_sigma": acc.dropped_no_sigma,
        },
        "buckets": bucket_report,
        "half_spread_pct": hs_summary,
        "half_spread_pct_unit": "percent_of_mid",
        "half_spread_vs_fallback": hs_vs_fallback,
        "hour_histogram": {str(h): acc.hour_hist[h] for h in sorted(acc.hour_hist)},
        "thin_bucket_min_samples": THIN_BUCKET_MIN_SAMPLES,
        "consumer_default_k": dict(CONSUMER_DEFAULT_K),
        "k_quantile": K_QUANTILE,
        "caveats": _caveats(acc, bucket_report, symbols_per_day=symbols_per_day,
                            compacted_only=compacted_only),
    }
    return Calibration(model=model, report=report)


def _skipped_text(days_skipped: list[dict[str, str]]) -> str:
    """``day (reason); day (reason)`` - the skip list as a sentence fragment, never a repr."""
    return "; ".join(f"{row['day']} ({row['reason']})" for row in days_skipped)


def _caveats(
    acc: _Accumulator,
    bucket_report: dict[str, dict[str, Any]],
    *,
    symbols_per_day: int | None = None,
    compacted_only: bool = False,
) -> list[str]:
    """The caveat block: the ways this calibration can be wrong."""
    if acc.hour_hist:
        top = sorted(acc.hour_hist.items(), key=lambda kv: -kv[1])[:3]
        total = sum(acc.hour_hist.values()) or 1
        hours = ", ".join(f"{h:02d}:00 ({n / total:.0%})" for h, n in top)
    else:
        hours = "n/a"
    by_basis = {
        basis: [n for n, cell in bucket_report.items() if cell["basis"] == basis]
        for basis in ("fitted", "floored_to_default", "thin", "degenerate")
    }
    fitted, floored = by_basis["fitted"], by_basis["floored_to_default"]
    thin, degenerate = by_basis["thin"], by_basis["degenerate"]
    defaulted = [n for n, cell in bucket_report.items() if cell["basis"] != "fitted"]
    caveats = []
    if symbols_per_day or compacted_only or acc.days_skipped:
        used = sum(row["symbols_used"] for row in acc.coverage)
        avail = sum(row["symbols_available"] for row in acc.coverage)
        per_session = [row["symbols_used"] for row in acc.coverage]
        span = (f"{min(per_session)}-{max(per_session)} symbols per session"
                if per_session else "no session read")
        not_compacted = sum(1 for row in acc.days_skipped if row["reason"] == "not_compacted")
        caveats.append(
            "BOUNDED RUN, NOT THE WHOLE CORPUS: this calibration read "
            f"{len(acc.days)} session(s), {used} symbol-days in total ({span}), out of "
            f"{avail} symbol-days available in those same sessions"
            + (f" (--symbols-per-day {symbols_per_day}, a deterministic index-rounding spread over "
               "the sorted symbol list)" if symbols_per_day else "")
            + (f"; {not_compacted} further session(s) were skipped as not_compacted "
               "(--compacted-only: still held as loose fragments, too slow to read beside a live "
               "session)" if not_compacted else "")
            # Prose, not a repr: the owner reads this before trusting any number.
            + (f"; sessions skipped: {_skipped_text(acc.days_skipped)}" if acc.days_skipped else "")
            + ". The per-day Coverage table states exactly what was read. Numbers below describe "
            "THAT slice; widen the bounds and re-run when the archive is fully compacted."
        )
    caveats.append(
        "A CALIBRATION MAY ONLY TIGHTEN: the shipped k for a bucket is max(fitted p75, the "
        "consumer's own default - open 1.0 / mid 0.5 / close 1.0). WHY the floor exists: the "
        "residual max(|fill - mid| - half_spread, 0) is zero-inflated (over 700 ms a liquid name "
        "usually has not left its own quote), and it is measured over a corpus that is "
        "window-biased and survivorship-tainted - evidence of that shape cannot justify charging "
        "the paper broker LESS slippage than it charges with no calibration at all, which is "
        "exactly the silent optimism that the paper-to-live gate reads. The fitted p50/p75/p90 and n "
        "ship in the YAML beside every k regardless of which rule applied ('basis'), because they "
        "are the baseline the live-vs-paper deviation tracker (the first live-ramp gate) "
        "recalibrates against. This run, by basis: "
        + "; ".join(
            f"{basis} = {', '.join(names) if names else 'none'}"
            for basis, names in (
                ("fitted (the fit shipped)", fitted),
                ("floored_to_default (a real fit, below the default)", floored),
                ("thin (too few samples to fit)", thin),
                ("degenerate (p50 collapsed to 0)", degenerate),
            )
        )
        + "."
    )
    if degenerate:
        caveats.append(
            "ESTIMATOR, NOT DATA: the pinned estimator (p75 of the residual |fill - mid| - "
            f"half_spread, floored at 0) is DEGENERATE for {', '.join(degenerate)} - its p50 is 0, "
            "i.e. more than half the samples are exactly 0, because over 700 ms a liquid name "
            "usually has not left its own quote. Those buckets ship the consumer default rather "
            "than a quantile of a spike at zero. READ THIS AS A DECISION FOR THE OWNER before "
            "the paper ledger is used as gate evidence: either accept that 700 ms slippage "
            "beyond the spread really is ~0 for these names (the p90 column shows how far the tail "
            "reaches), or re-pin the estimator - a higher quantile, a signed adverse-only residual, "
            "or a longer horizon - and re-run."
        )
    return caveats + [
        "WINDOW BIAS: the engine is not up for the whole session (plan 2.6), so the corpus is not a "
        f"uniform sample of the trading day. Tick mass by hour: {hours}. A bucket whose hours are "
        "under-represented has a k fitted on a minority of the day"
        + (f"; buckets shipping the consumer default rather than their fit: {', '.join(defaulted)}."
           if defaulted else "; every bucket shipped its own fit."),
        "SURVIVORSHIP: the symbol set is whatever the universe builder admitted on each recorded day "
        f"({len(acc.symbols)} distinct symbols over {len(acc.days)} sessions), i.e. liquid, "
        "in-universe names. Half-spreads and k for a name that was never in the universe - or that "
        "entered it later - are NOT covered; the per-symbol table is a fallback for names present "
        "here, not a market-wide estimate.",
        f"THE {LATENCY_MS} ms ASSUMPTION: fills are read off the first tick at or after t + "
        f"{LATENCY_MS} ms. That latency is an assumption, not a measurement of this account's "
        "order round-trip - it has never been measured against a live order. If real latency is "
        "worse, the residuals here understate slippage; k is a p75 partly to absorb that.",
        "NOT A LIVE-DEVIATION MEASUREMENT: every number here comes from the tape alone. Only the "
        "live-vs-paper deviation tracker (the first live-ramp gate) can confirm the model.",
    ]


# --- outputs --------------------------------------------------------------------------------------

_DEFAULT_K_TEXT = " / ".join(f"{name} {CONSUMER_DEFAULT_K[name]}" for name, _, _ in BUCKETS)

_YAML_HEADER = f"""\
# config/fill_model.yaml - PaperBroker fill model. GENERATED by
# scripts/calibrate_fill_model.py; do not hand-edit - re-run the script instead.
# Non-protected config: platform-written, owner informed. Evidence: data/reports/fill_model_calibration_*.md
#
#   slippage = half_spread + k[bucket] * sigma_1m
#
# k: A CALIBRATION MAY ONLY TIGHTEN THE MODEL. The shipped `buckets[].k` is
#   max(fitted p75, the consumer's own default for that bucket: {_DEFAULT_K_TEXT}),
# where the fit is the p75 of the residual |fill - mid| - half_spread normalised by sigma_1m over
# the recorded tick archive with a {LATENCY_MS} ms latency applied. WHY the floor exists: that
# residual is floored at 0 and is zero-inflated (over 700 ms a liquid name usually has not left its
# own quote), and the corpus behind it is window-biased (the engine is not up for the whole session)
# and survivorship-tainted (in-universe names only). Evidence of that shape cannot justify
# charging LESS slippage than the uncalibrated model already charges - that is the silent optimism
# that the paper-to-live gate reads.
#
# `buckets[].basis` says which rule produced k:
#   fitted             - the p75 cleared the default and replaced it
#   floored_to_default - a real fit, but below the default, so the DEFAULT ships
#   thin               - fewer than {THIN_BUCKET_MIN_SAMPLES} samples; a quantile would be noise
#   degenerate         - the fitted p50 is <= 0, so the quantiles measure the estimator, not the tape
#
# `buckets[].p50/p75/p90/n` are the fitted quantiles, kept here VERBATIM whatever the basis: they
# are the baseline the live-vs-paper fill-deviation tracker (the first live-ramp gate)
# recalibrates against, so the config carries its own evidence. The report explains each one.
#
# `symbols[].median_half_spread_pct` is a PERCENT of mid (the consumer divides by 100) and is REAL
# evidence of that name's book, so the consumer (engine.paper.fill_model.half_spread) prefers it to
# the ignorance fallback and floors it at HALF A TICK - the physical minimum a book can quote -
# i.e. max(ltp x pct / 100, tick_size / 2); many liquid names sit BELOW the 2-tick fallback because
# their books are one tick wide, and that is correct, not optimistic (round-4 policy, 2026-09-10).
#
# `source.days` lists the sessions actually read - a run may be bounded (--days / --symbols-per-day
# / --compacted-only); the report's Coverage table states what was skipped and why. A run that reads
# no session writes NOTHING and exits {EXIT_NO_SESSIONS} rather than overwriting this file with defaults.
#
# `symbols[].median_half_spread_pct` is a PERCENT of mid (0.0127 == 0.0127% == ~1.3 bps), the unit
# `engine.paper.fill_model.half_spread()` reads - it divides by 100. `n_ticks` counts the L1 ticks
# the median was taken over (ticks without L1 are excluded from the spread stat); a symbol absent
# from `symbols` has no L1 in the corpus and must use `half_spread_fallback_ticks` x tick_size,
# never 0.
"""


def write_yaml(model: FillModel, path: Path | str) -> None:
    """Serialise the model to `path` in the pinned key order, with the provenance header."""
    payload = model.model_dump(mode="json")
    ordered = {
        "version": payload["version"],
        "generated_at": payload["generated_at"],
        "source": payload["source"],
        "latency_ms": payload["latency_ms"],
        "buckets": {name: payload["buckets"][name] for name, _, _ in BUCKETS},
        "half_spread_fallback_ticks": payload["half_spread_fallback_ticks"],
        "symbols": payload["symbols"],
    }
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(
        _YAML_HEADER + yaml.safe_dump(ordered, sort_keys=False, default_flow_style=False),
        encoding="utf-8",
    )


def render_report_md(calibration: Calibration) -> str:
    report = calibration.report
    lines: list[str] = [
        "# Fill-model calibration",
        "",
        f"Generated {report['generated_at']} | schema v{report['schema_version']} | "
        f"latency {report['latency_ms']} ms | "
        f"k = max(p{int(K_QUANTILE)} of the normalised residual, the consumer default)",
        "",
        "## Corpus",
        "",
        f"- Sessions: **{len(report['days'])}**"
        + (f" ({report['days'][0]} .. {report['days'][-1]})" if report["days"] else ""),
        f"- Symbols: **{report['symbols']}**",
        f"- In-session ticks: **{report['session_ticks']:,}**"
        f" (L1 present on {report['l1_ticks']:,}; **{report['no_l1_share']:.2%} lack L1**)",
        f"- Decision ticks sampled: **{report['sampled_ticks']:,}**"
        f" (dropped: {report['samples_dropped']['no_forward_fill']:,} with no forward fill,"
        f" {report['samples_dropped']['no_sigma']:,} with sigma_1m = 0)",
    ]
    coverage = report["coverage"]
    used = sum(row["symbols_used"] for row in coverage)
    avail = sum(row["symbols_available"] for row in coverage)
    per_session = [row["symbols_used"] for row in coverage]
    not_compacted = [r["day"] for r in report["days_skipped"] if r["reason"] == "not_compacted"]
    other_skips = [r for r in report["days_skipped"] if r["reason"] != "not_compacted"]
    lines += [
        f"- Coverage: **{used:,} symbol-days** read across {len(coverage)} session(s) "
        f"(of {avail:,} symbol-days available in those sessions)"
        + (f", **{min(per_session)}-{max(per_session)} symbols per session**" if per_session else ""),
        f"- Sessions skipped as `not_compacted`: **{len(not_compacted)}**"
        + (f" ({', '.join(not_compacted)})" if not_compacted else ""),
    ]
    if other_skips:
        lines.append(f"- Sessions skipped for other reasons: {_skipped_text(other_skips)}")
    lines += [
        f"- Bounds: `--days {report['days_requested']}`, "
        f"`--symbols-per-day {report['symbols_per_day']}`, "
        f"`--compacted-only {str(bool(report['compacted_only'])).lower()}`",
        "",
        "## Coverage (what was actually read - see caveat 1 if this is a bounded run)",
        "",
        "| session | symbols used / available | compacted | ticks | L1 ticks | read s |",
        "|---|---:|---|---:|---:|---:|",
    ]
    for row in coverage:
        lines.append(
            f"| {row['day']} | {row['symbols_used']:,} / {row['symbols_available']:,} | "
            f"{'yes' if row['compacted'] else 'NO (fragmented)'} | {row['ticks']:,} | "
            f"{row['l1_ticks']:,} | {row['elapsed_s']} |"
        )
    lines += [
        "",
        "## k per time-of-day bucket",
        "",
        f"Shipped k = max(fitted p{int(K_QUANTILE)}, the consumer default) - a calibration may only "
        "TIGHTEN the model, never loosen it (see caveat below). The fitted quantiles are kept "
        "verbatim here and in the YAML as the baseline for the live-vs-paper "
        "fill-deviation recalibration (the first live-ramp gate).",
        "",
        "| bucket | window | n | p50 | p75 | p90 | default k | shipped k | basis |",
        "|---|---|---:|---:|---:|---:|---:|---:|---|",
    ]
    for name, _, _ in BUCKETS:
        cell = report["buckets"][name]
        lines.append(
            f"| {name} | {cell['start']}-{cell['end']} | {cell['n']:,} | {cell['p50']} | "
            f"{cell['p75']} | {cell['p90']} | {cell['consumer_default_k']} | "
            f"**{cell['k']}** | {cell['basis']} |"
        )
    hs = report["half_spread_pct"]
    fb = report["half_spread_vs_fallback"]
    lines += [
        "",
        "## Half-spread distribution (per-symbol medians, PERCENT of mid)",
        "",
        "`median_half_spread_pct` is a percent (0.0127 == 0.0127% == ~1.3 bps) - the unit "
        "`engine.paper.fill_model.half_spread()` reads, which divides the field by 100.",
        "",
        "| n symbols | p10 | p25 | p50 | p75 | p90 |",
        "|---:|---:|---:|---:|---:|---:|",
        f"| {hs['n_symbols']:,} | {hs['p10']} | {hs['p25']} | {hs['p50']} | {hs['p75']} | {hs['p90']} |",
        "",
        "### Half-spread vs fallback",
        "",
        f"The model's ignorance default - what a symbol gets when nothing was ever measured on it - "
        f"is `half_spread_fallback_ticks x tick_size` = {fb['fallback_ticks']} x "
        f"Rs {fb['reference_tick']} = **Rs {fb['fallback_price']:.2f}**. Measured against it, "
        f"**{fb['below_at_reference_ltp']} of {fb['n_symbols']}** shipped symbols resolve BELOW that "
        f"fallback at a reference ltp of Rs {fb['reference_ltp']:,.0f}, and "
        f"**{fb['below_at_median_price']} of {fb['n_symbols']}** at their own corpus-median price. "
        "Those are real one-tick-wide books, not errors - the fallback multiple is an assumption "
        "about the unknown, not a physical limit. That is why "
        "`engine.paper.fill_model.half_spread()` floors the CALIBRATED branch at HALF A TICK (the "
        "tightest a real book can quote) rather than at this fallback: clamping a measurement up to "
        "an ignorance default would discard the measurement (round 4, 2026-09-10).",
        "",
        f"Ticks without L1: **{report['no_l1_share']:.2%}** - excluded from the spread stat, still "
        "sampled for k against the deterministic fallback half-spread (never 0).",
        "",
        "## Tick mass by hour (the window-bias evidence)",
        "",
        "| hour | ticks |",
        "|---|---:|",
    ]
    for hour, count in report["hour_histogram"].items():
        lines.append(f"| {int(hour):02d}:00 | {count:,} |")
    lines += ["", "## Caveats (read these before trusting a number above)", ""]
    lines += [f"{i}. {text}" for i, text in enumerate(report["caveats"], start=1)]
    lines.append("")
    return "\n".join(lines)


def _existing_symbol_count(path: Path | str) -> int | None:
    """Symbols in the file about to be replaced; None for a first run or an unparseable file
    (a warning input, not a precondition)."""
    try:
        raw = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError):
        return None
    if not isinstance(raw, dict) or not isinstance(raw.get("symbols"), dict):
        return None
    return len(raw["symbols"])


def _publish_warnings(report: dict[str, Any], out_path: Path | str) -> list[str]:
    """The two ways a run can be a bad REPLACEMENT for the file it is about to overwrite.

    Warnings, not a hard stop: a deliberately bounded run is legitimate and refusing would make the
    bounding flags useless on a trading day; doing it silently is what is unacceptable (a thin run
    replaces the archive's k, and dropped symbols fall back to `half_spread_fallback_ticks x
    tick_size`, loosening the model). The exit code stays 0.
    """
    warnings: list[str] = []
    read = len(report["days"])
    candidate = int(report.get("days_candidate") or read)
    if candidate and read * 2 < candidate:
        warnings.append(
            f"WARNING: this run read {read} of {candidate} candidate session(s) - under half the "
            "corpus it was pointed at. The k values and per-symbol half-spreads about to be written "
            "describe THAT slice, not the archive. Widen --days / drop --compacted-only and re-run "
            "before treating this as the account's calibration. Sessions skipped: "
            + (_skipped_text(report["days_skipped"]) or "none")
        )
    previous = _existing_symbol_count(out_path)
    new = report["half_spread_vs_fallback"]["n_symbols"]
    if previous is not None and new < previous:
        warnings.append(
            f"WARNING: the new symbols table is SMALLER than the file it replaces - {new} symbol(s) "
            f"vs {previous} symbol(s) in {out_path}. Each symbol that disappears loses its measured "
            "non-L1 half-spread and falls back to half_spread_fallback_ticks x tick_size, which "
            "LOOSENS the fill model silently. Re-run over the wider corpus if that is not intended."
        )
    return warnings


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--parquet-root", default="data/parquet",
                        help="root holding ticks/date=*/symbol=*/*.parquet (never market.duckdb)")
    parser.add_argument("--out", default="config/fill_model.yaml")
    parser.add_argument("--report", default="data/reports/fill_model_calibration",
                        help="output stem; .md and .json are written")
    parser.add_argument("--max-samples", type=int, default=DEFAULT_MAX_SAMPLES)
    parser.add_argument("--days", type=int, default=None, help="use only the most recent N sessions")
    parser.add_argument("--symbols-per-day", type=int, default=None,
                        help="bound each session to exactly N symbols (a deterministic "
                             "index-rounding spread over the sorted symbol list, spanning both "
                             "ends); default = every symbol")
    parser.add_argument("--compacted-only", action="store_true",
                        help="skip sessions still held as loose fragments - keeps the run "
                             "I/O-modest beside a live trading session")
    parser.add_argument("--exclude-day", action="append", default=[], metavar="YYYY-MM-DD",
                        help="drop a session (repeatable); pass today when it is still in progress")
    args = parser.parse_args(argv)
    exclude = {date.fromisoformat(d) for d in args.exclude_day}

    def _progress(day: date, index: int, total: int, elapsed: float) -> None:
        print(f"[{index}/{total}] {day.isoformat()} in {elapsed:.1f}s", file=sys.stderr, flush=True)

    started = _time.perf_counter()
    try:
        calibration = calibrate(
            args.parquet_root, max_samples=args.max_samples, days=args.days,
            symbols_per_day=args.symbols_per_day, compacted_only=args.compacted_only,
            exclude=exclude, progress=_progress,
        )
    except FileNotFoundError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return EXIT_NO_PARTITIONS

    if not calibration.report["days"]:
        # Nothing is written: an all-defaults model with an EMPTY `symbols` table would silently
        # delete the previous calibration's per-symbol half-spreads.
        print(
            f"ERROR: no session was read - nothing written to {args.out} "
            f"(the previous calibration, if any, is left intact).",
            file=sys.stderr,
        )
        for row in calibration.report["days_skipped"]:
            print(f"  skipped {row['day']}: {row['reason']}", file=sys.stderr)
        if not calibration.report["days_skipped"]:
            print("  (no session was skipped either - the corpus held no in-session ticks)",
                  file=sys.stderr)
        return EXIT_NO_SESSIONS

    # Printed BEFORE the write, so the warning reads as a statement about what is about to happen.
    for warning in _publish_warnings(calibration.report, args.out):
        print(warning, file=sys.stderr, flush=True)

    write_yaml(calibration.model, args.out)
    stem = Path(args.report)
    stem.parent.mkdir(parents=True, exist_ok=True)
    stem.with_suffix(".json").write_text(
        json.dumps(calibration.report, indent=2, sort_keys=False), encoding="utf-8"
    )
    stem.with_suffix(".md").write_text(render_report_md(calibration), encoding="utf-8")

    print(f"wrote {args.out}")
    print(f"wrote {stem.with_suffix('.md')} + {stem.with_suffix('.json')}")
    print(f"elapsed {_time.perf_counter() - started:.1f}s")
    for name, _, _ in BUCKETS:
        cell = calibration.report["buckets"][name]
        print(
            f"  {name:<5} {cell['start']}-{cell['end']}  n={cell['n']:<9,} "
            f"p50={cell['p50']:<8} p75={cell['p75']:<8} p90={cell['p90']:<8} "
            f"default={cell['consumer_default_k']:<4} k={cell['k']:<8} basis={cell['basis']}"
        )
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI
    raise SystemExit(main())
