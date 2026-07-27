"""ValidationPipeline (§3.2.10 / §6.4 step 2, E2) — anchored walk-forward + skfolio CPCV + the
multiple-testing-deflated promotion decision.

Semantics pinned by the plan:

* **Anchored walk-forward** — 6 months train / 1 month test, rolling: the train window is always
  anchored at the start of the data; each successive fold's 1-month test slice rolls forward. With
  fixed candidate params (no re-fit inside Phase 1) the folds are successive out-of-sample months —
  reported per fold, never pooled into the promotion rule.
* **CPCV** — ``skfolio.model_selection.CombinatorialPurgedCV`` (purge 5 d, embargo 5 d, §6.4) over
  the **cost-adjusted daily return series** of the candidate. Fold expectancy = mean net daily
  return over the fold's test observations; a fold passes iff that expectancy is **> 0 after costs**.
* **fold_pass_min(N)** (§6.4 step 2 / §9.1, exact boundaries): 60% for N ≤ 10, 70% for
  11 ≤ N ≤ 30, 80% for N > 30 — monotone deflation in the trial count N.
* **Trial count N** = the optimizer-reported count of every configuration evaluated
  (``SweepReport.trial_count_n``, §6.4 step 1) — NOT the count of surfaced ``param_sets`` rows.
  A report with **no cited N is not promotable** (E2). Promotable iff the CPCV fold-pass fraction
  ≥ ``fold_pass_min(N)`` (§9.1) — plus, when a champion max-DD is supplied (Phase 2 seam), max DD
  ≤ 1.25× champion's (§6.4 step 2).
* **Persistence** — every validated candidate is logged to SQLite ``param_sets``
  (``status='candidate'``, ``validation_report`` JSON citing N, ``evaluated_at``) for audit, and a
  report artifact (md + json) is written via :mod:`engine.learning.reports`.

The N here is per-validation-run; the §6.4 rolling-window / per-``feature_set_version`` N
bookkeeping belongs to ``ChampionChallenger`` (Phase 2), which passes the windowed N in via
:class:`ParamSet`. skfolio is imported function-level only (native import-order guard,
``engine._preload``); the pipeline itself is pandas/numpy + stdlib.
"""

from __future__ import annotations

import asyncio
import calendar as _calendar
import json
import sqlite3
from collections.abc import Callable, Sequence
from datetime import date, datetime
from pathlib import Path

import numpy as np
import pandas as pd
from pydantic import BaseModel, ConfigDict, Field
from ulid import ULID

from engine.core.clock import Clock
from engine.core.log import get_logger

_log = get_logger("engine.learning.validate")

#: §6.4 step 2 pinned defaults: purge 5 d, embargo 5 d. Fold counts are not plan-pinned;
#: 6 folds × 2 test folds gives C(6,2) = 15 combinatorial splits (constructor knobs).
CPCV_PURGE_DAYS = 5
CPCV_EMBARGO_DAYS = 5
CPCV_N_FOLDS = 6
CPCV_N_TEST_FOLDS = 2
WF_TRAIN_MONTHS = 6
WF_TEST_MONTHS = 1
CHAMPION_MAX_DD_MULT = 1.25            # §6.4 step 2: max DD ≤ 1.25× champion's


# --------------------------------------------------------------------------- fold_pass_min (§6.4/§9.1)
def fold_pass_min(n: int) -> float:
    """Minimum CPCV fold-pass fraction for trial count ``n`` (§6.4 step 2, exact §9.1 boundaries).

    60% for N ≤ 10, 70% for 11 ≤ N ≤ 30, 80% for N > 30 — tightens monotonically with N
    (lightweight multiple-testing deflation). ``n`` must be ≥ 0.
    """
    if n < 0:
        raise ValueError(f"trial count N must be >= 0, got {n}")
    if n <= 10:
        return 0.60
    if n <= 30:
        return 0.70
    return 0.80


# --------------------------------------------------------------------------- models
class ParamSet(BaseModel):
    """A candidate parameter set entering validation (§3.2.10 / §6.4 step 1).

    ``trial_count_n`` is the optimizer-reported evaluated-config count for this strategy's window
    (``SweepReport.trial_count_n``; Phase-2 ``ChampionChallenger`` supplies the rolling-window N).
    ``None`` ⇒ the resulting report is not promotable (E2). ``sweep_stats`` optionally carries the
    sweep's trade-level stats for report context (never part of the promotion rule).
    """

    model_config = ConfigDict(frozen=True)

    param_set_id: str = Field(default_factory=lambda: str(ULID()))
    strategy_id: str
    params: dict[str, float]
    trial_count_n: int | None = None
    sweep_stats: dict[str, float | None] | None = None


class WalkForwardFold(BaseModel):
    """One anchored walk-forward fold (train always starts at data start; ends are exclusive)."""

    model_config = ConfigDict(frozen=True)

    fold: int
    train_start: date
    train_end: date
    test_start: date
    test_end: date
    n_obs: int
    expectancy_pct: float | None          # mean cost-adjusted daily return over the test slice, %
    total_return_pct: float | None        # compounded net return over the test slice, %


class CPCVFold(BaseModel):
    """One combinatorial purged CV split; ``passed`` iff out-of-sample expectancy > 0 after costs."""

    model_config = ConfigDict(frozen=True)

    split: int
    n_train_obs: int
    n_test_obs: int
    expectancy_pct: float | None
    passed: bool


class ValidationReport(BaseModel):
    """§6.4 step 2 output. MUST cite the trial count N — a report with no cited N is not promotable.

    Statistics are floats deliberately (they are statistics, not ledger money — same argument as
    ``engine.strategy.indicators``); nothing here is ever a persisted price. ``max_drawdown_pct``
    is a positive magnitude (17.0 ⇒ a −17% peak-to-trough).
    """

    model_config = ConfigDict(frozen=True)

    strategy_id: str
    param_set_id: str
    params: dict[str, float]
    trial_count_n: int | None                       # the cited N (§6.4); None ⇒ not promotable
    fold_pass_min: float | None                     # fold_pass_min(N); None when N absent
    data_start: date | None
    data_end: date | None
    n_obs: int
    expectancy_pct: float | None                    # mean cost-adjusted daily return, %
    total_return_pct: float | None
    max_drawdown_pct: float | None
    walk_forward: list[WalkForwardFold]
    cpcv: list[CPCVFold]
    cpcv_fold_pass_fraction: float | None
    promotable: bool
    reasons: list[str]                              # every reason the report is NOT promotable
    sweep_stats: dict[str, float | None] | None = None
    notes: list[str] = Field(default_factory=list)  # documented approximations, honest caveats
    generated_at: datetime


# --------------------------------------------------------------------------- pure split functions
def _add_months(d: date, months: int) -> date:
    y, m = divmod(d.month - 1 + months, 12)
    y += d.year
    m += 1
    return date(y, m, min(d.day, _calendar.monthrange(y, m)[1]))


def walk_forward_splits(
    dates: Sequence[date], *, train_months: int = WF_TRAIN_MONTHS, test_months: int = WF_TEST_MONTHS
) -> list[tuple[date, date, date, date]]:
    """Anchored walk-forward boundaries over observation ``dates`` (§6.4: 6 m train / 1 m test).

    Returns ``(train_start, train_end, test_start, test_end)`` tuples with exclusive ends and
    ``train_end == test_start``; the train window is always anchored at the first date. Folds whose
    test window contains no observations are skipped. Pure and deterministic (§9.6): the same date
    set always yields byte-identical splits.
    """
    ds = sorted(set(dates))
    if not ds:
        return []
    start, last = ds[0], ds[-1]
    out: list[tuple[date, date, date, date]] = []
    k = 0
    while True:
        test_start = _add_months(start, train_months + k * test_months)
        if test_start > last:
            break
        test_end = _add_months(test_start, test_months)
        if any(test_start <= d < test_end for d in ds):
            out.append((start, test_start, test_start, test_end))
        k += 1
    return out


def cpcv_splits(
    n_obs: int,
    *,
    n_folds: int = CPCV_N_FOLDS,
    n_test_folds: int = CPCV_N_TEST_FOLDS,
    purge: int = CPCV_PURGE_DAYS,
    embargo: int = CPCV_EMBARGO_DAYS,
) -> list[tuple[np.ndarray, np.ndarray]]:
    """Combinatorial purged CV index splits via skfolio (§6.4: purge 5 d, embargo 5 d).

    Returns ``(train_indices, test_indices)`` pairs over ``range(n_obs)`` daily observations.
    skfolio already purges/embargoes; as an invariant enforcement (the §9.1 no-overlap property)
    every train index within ``purge`` observations before or ``embargo`` after any test block is
    dropped again here — belt and braces, so the guarantee cannot regress with a skfolio upgrade.
    Empty list when ``n_obs`` is too small to form meaningful folds — skfolio requires
    ``purge + embargo < (n_obs // n_folds) − 1``; below that a valid purged CPCV cannot be formed, so
    we return ``[]`` (⇒ no CPCV folds ⇒ not promotable, §6.4 step 2) rather than raise.
    """
    if n_obs < n_folds * 2:
        return []
    min_fold_size = n_obs // n_folds
    if purge + embargo >= min_fold_size - 1:            # skfolio's own precondition (avoid ValueError)
        return []
    from skfolio.model_selection import CombinatorialPurgedCV  # function-level: engine._preload guard

    cv = CombinatorialPurgedCV(
        n_folds=n_folds, n_test_folds=n_test_folds, purged_size=purge, embargo_size=embargo
    )
    out: list[tuple[np.ndarray, np.ndarray]] = []
    for train, test in cv.split(np.zeros((n_obs, 1))):
        train = np.sort(np.asarray(train, dtype=np.int64))
        chunks = [np.asarray(c, dtype=np.int64) for c in (test if isinstance(test, list | tuple) else [test])]
        keep = np.ones(train.shape, dtype=bool)
        for c in chunks:
            lo, hi = int(c.min()), int(c.max())
            keep &= ~((train >= lo - purge) & (train <= hi + embargo))
        out.append((train[keep], np.sort(np.concatenate(chunks))))
    return out


# --------------------------------------------------------------------------- promotion rule (§6.4/§9.1)
def promotion_decision(
    n: int | None,
    fold_pass_fraction: float | None,
    *,
    max_dd_pct: float | None = None,
    champion_max_dd_pct: float | None = None,
) -> tuple[bool, list[str]]:
    """The deterministic §6.4 step-2 pass rule. Returns ``(promotable, reasons_not_promotable)``.

    * no cited N ⇒ not promotable (E2);
    * no CPCV folds ⇒ not promotable (nothing was validated out-of-sample);
    * promotable iff fold-pass fraction ≥ ``fold_pass_min(N)`` (§9.1) — and, when a champion max-DD
      is provided (Phase-2 ``ChampionChallenger`` seam), max DD ≤ 1.25× champion's.
    """
    reasons: list[str] = []
    if n is None:
        reasons.append(
            "trial count N is ABSENT — a report with no cited N is not promotable (E2/§6.4 step 2)"
        )
    if fold_pass_fraction is None:
        reasons.append("CPCV produced no folds (insufficient observations) — not validated out-of-sample")
    if n is not None and fold_pass_fraction is not None:
        need = fold_pass_min(n)
        if fold_pass_fraction < need:
            reasons.append(
                f"CPCV fold-pass fraction {fold_pass_fraction:.1%} < fold_pass_min(N={n}) = {need:.0%}"
            )
    if champion_max_dd_pct is not None and max_dd_pct is not None:
        cap = CHAMPION_MAX_DD_MULT * champion_max_dd_pct
        if max_dd_pct > cap:
            reasons.append(
                f"max drawdown {max_dd_pct:.2f}% > {CHAMPION_MAX_DD_MULT}x champion's "
                f"({champion_max_dd_pct:.2f}%) = {cap:.2f}% (§6.4 step 2)"
            )
    return (not reasons, reasons)


#: (strategy_id, params) -> cost-adjusted daily net return series (ascending date index).
ReturnsProvider = Callable[[str, dict[str, float]], "pd.Series"]

#: n_obs -> list of (train_indices, test_indices); injectable for the offline test tier.
Splitter = Callable[[int], list[tuple[np.ndarray, np.ndarray]]]


class ValidationPipeline:
    """§3.2.10 ``ValidationPipeline`` (E2): walk-forward + CPCV over cost-adjusted returns.

    Parameters
    ----------
    returns_provider:
        ``(strategy_id, params) -> pd.Series`` of cost-adjusted DAILY net returns (the
        ``SweepRunner.returns_for`` closure in production; a synthetic series in tests).
    clock:
        The single source of "now" (§3.2) — stamps ``generated_at`` / ``evaluated_at``.
    conn:
        SQLite connection for the ``param_sets`` audit row (``None`` ⇒ persistence skipped —
        offline analysis only). ``learning`` writes only its own tables (R4, §3.2.10).
    reports_dir:
        Directory for the md+json report artifact (``None`` ⇒ artifact skipped).
    splitter:
        CPCV splitter override (defaults to skfolio :func:`cpcv_splits` with the §6.4 knobs).
    champion_max_dd_provider:
        ``strategy_id -> champion max-DD %`` (Phase-2 ``ChampionChallenger`` seam); ``None`` values
        skip the 1.25× DD comparison (no champion exists in Phase 1).
    """

    def __init__(
        self,
        *,
        returns_provider: ReturnsProvider,
        clock: Clock,
        conn: sqlite3.Connection | None = None,
        reports_dir: str | Path | None = None,
        splitter: Splitter | None = None,
        champion_max_dd_provider: Callable[[str], float | None] | None = None,
        cpcv_n_folds: int = CPCV_N_FOLDS,
        cpcv_n_test_folds: int = CPCV_N_TEST_FOLDS,
        purge_days: int = CPCV_PURGE_DAYS,
        embargo_days: int = CPCV_EMBARGO_DAYS,
        wf_train_months: int = WF_TRAIN_MONTHS,
        wf_test_months: int = WF_TEST_MONTHS,
    ) -> None:
        self._returns_provider = returns_provider
        self._clock = clock
        self._conn = conn
        self._reports_dir = Path(reports_dir) if reports_dir is not None else None
        self._champion_max_dd = champion_max_dd_provider or (lambda _sid: None)
        self._wf_train_months = wf_train_months
        self._wf_test_months = wf_test_months
        if splitter is None:
            def splitter(n_obs: int) -> list[tuple[np.ndarray, np.ndarray]]:
                return cpcv_splits(
                    n_obs,
                    n_folds=cpcv_n_folds,
                    n_test_folds=cpcv_n_test_folds,
                    purge=purge_days,
                    embargo=embargo_days,
                )
        self._splitter = splitter

    # ------------------------------------------------------------------ public surface (§3.2.10)
    async def validate(self, strategy_id: str, params: ParamSet) -> ValidationReport:
        """Validate a candidate (pinned §3.2.10 signature). CPU-bound work is executor-offloaded
        (`asyncio.to_thread`) so the in-engine caller never blocks the loop (§2.2)."""
        return await asyncio.to_thread(self.validate_sync, strategy_id, params)

    def validate_sync(self, strategy_id: str, params: ParamSet) -> ValidationReport:
        """Synchronous core — the standalone CLI (``scripts/backtest.py``) calls this directly."""
        if params.strategy_id != strategy_id:
            raise ValueError(
                f"strategy_id mismatch: argument {strategy_id!r} vs ParamSet {params.strategy_id!r}"
            )
        rets = self._returns_provider(strategy_id, dict(params.params))
        rets = pd.Series(rets, dtype="float64").sort_index()
        report = self._build_report(strategy_id, params, rets)
        self._persist(report)
        self._write_artifacts(report)
        _log.info(
            "validation_done",
            strategy=strategy_id,
            param_set_id=report.param_set_id,
            n=report.trial_count_n,
            fold_pass_fraction=report.cpcv_fold_pass_fraction,
            promotable=report.promotable,
        )
        return report

    # ------------------------------------------------------------------ internals
    def _build_report(self, strategy_id: str, params: ParamSet, rets: pd.Series) -> ValidationReport:
        n_obs = int(len(rets))
        dates = [ts.date() if hasattr(ts, "date") else ts for ts in rets.index]
        values = rets.to_numpy(dtype=float)

        expectancy = float(values.mean() * 100.0) if n_obs else None
        total_return: float | None = None
        max_dd: float | None = None
        if n_obs:
            equity = np.cumprod(1.0 + values)
            total_return = float((equity[-1] - 1.0) * 100.0)
            max_dd = float(-np.min(equity / np.maximum.accumulate(equity) - 1.0) * 100.0)

        wf_folds = self._walk_forward(dates, values)
        cpcv_folds, pass_fraction = self._cpcv(values)

        n = params.trial_count_n
        champion_dd = self._champion_max_dd(strategy_id)
        promotable, reasons = promotion_decision(
            n, pass_fraction, max_dd_pct=max_dd, champion_max_dd_pct=champion_dd
        )
        return ValidationReport(
            strategy_id=strategy_id,
            param_set_id=params.param_set_id,
            params=dict(params.params),
            trial_count_n=n,
            fold_pass_min=fold_pass_min(n) if n is not None else None,
            data_start=dates[0] if dates else None,
            data_end=dates[-1] if dates else None,
            n_obs=n_obs,
            expectancy_pct=expectancy,
            total_return_pct=total_return,
            max_drawdown_pct=max_dd,
            walk_forward=wf_folds,
            cpcv=cpcv_folds,
            cpcv_fold_pass_fraction=pass_fraction,
            promotable=promotable,
            reasons=reasons,
            sweep_stats=params.sweep_stats,
            generated_at=self._clock.now(),
        )

    def _walk_forward(self, dates: list[date], values: np.ndarray) -> list[WalkForwardFold]:
        splits = walk_forward_splits(
            dates, train_months=self._wf_train_months, test_months=self._wf_test_months
        )
        date_arr = np.array(dates)
        folds: list[WalkForwardFold] = []
        for i, (tr_s, tr_e, te_s, te_e) in enumerate(splits):
            mask = (date_arr >= te_s) & (date_arr < te_e)
            sl = values[mask]
            folds.append(
                WalkForwardFold(
                    fold=i,
                    train_start=tr_s,
                    train_end=tr_e,
                    test_start=te_s,
                    test_end=te_e,
                    n_obs=int(mask.sum()),
                    expectancy_pct=float(sl.mean() * 100.0) if len(sl) else None,
                    total_return_pct=float((np.prod(1.0 + sl) - 1.0) * 100.0) if len(sl) else None,
                )
            )
        return folds

    def _cpcv(self, values: np.ndarray) -> tuple[list[CPCVFold], float | None]:
        splits = self._splitter(len(values))
        folds: list[CPCVFold] = []
        for i, (train_idx, test_idx) in enumerate(splits):
            sl = values[np.asarray(test_idx, dtype=np.int64)]
            expectancy = float(sl.mean() * 100.0) if len(sl) else None
            passed = expectancy is not None and expectancy > 0.0  # > 0 after costs, strictly (§6.4)
            folds.append(
                CPCVFold(
                    split=i,
                    n_train_obs=int(len(train_idx)),
                    n_test_obs=int(len(test_idx)),
                    expectancy_pct=expectancy,
                    passed=passed,
                )
            )
        if not folds:
            return [], None
        return folds, sum(f.passed for f in folds) / len(folds)

    def _persist(self, report: ValidationReport) -> None:
        """Audit row per §6.4 step 1: every surfaced candidate is a ``param_sets`` row
        (``status='candidate'``); the deflating N lives INSIDE the cited report JSON."""
        if self._conn is None:
            return
        self._conn.execute(
            "INSERT INTO param_sets (param_set_id, strategy_id, params, status, validation_report,"
            " evaluated_at, enabled) VALUES (?,?,?,?,?,?,1)"
            " ON CONFLICT(param_set_id) DO UPDATE SET params=excluded.params, status=excluded.status,"
            " validation_report=excluded.validation_report, evaluated_at=excluded.evaluated_at",
            (
                report.param_set_id,
                report.strategy_id,
                json.dumps(report.params, sort_keys=True),
                "candidate",
                report.model_dump_json(),
                self._clock.now().isoformat(),
            ),
        )

    def _write_artifacts(self, report: ValidationReport) -> None:
        if self._reports_dir is None:
            return
        from engine.learning import reports  # function-level: avoids a module-import cycle

        reports.write_report(report, self._reports_dir)
