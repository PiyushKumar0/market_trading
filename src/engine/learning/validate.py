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
* **Margin floor** (WO-3, 2026-08-13) — a fold-pass FRACTION says how often the edge was positive,
  never by how much. Four rsi2 runs passed at exactly 12/15 = 80.0% with every passing split under
  0.02%/day (median ≈ 0.0006%/day): statistically "positive", economically indistinguishable from
  zero. So promotion now ALSO requires the **median passing-split expectancy ≥ cost_floor /
  MARGIN_FLOOR_DAYS per day** — see :data:`MARGIN_FLOOR_DAYS` for the constant's derivation. The
  ``fold_pass_fraction`` comparison deliberately stays strict-``<`` (silently flipping it to ``<=``
  would move a documented boundary invisibly; the margin floor is the real fix).
* **Winner stability** (WO-3) — the sweep's winning config changed three times across near-identical
  grid densities. The report records whether the adjacent density picked the same winner as a
  **flag** (:class:`WinnerStability`), NOT an auto-fail: instability is evidence about the ranking
  surface's flatness, and turning it into a hard gate would silently discard genuinely robust
  strategies whose neighbouring configs are near-ties.
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
from collections.abc import Callable, Mapping, Sequence
from datetime import date, datetime
from decimal import Decimal
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

#: WO-3 margin floor: the median PASSING CPCV split must earn at least ``cost_floor / 20`` per day,
#: where ``cost_floor`` is the strategy's full round-trip friction (fees + spread, WO-2) at the
#: sweep's sizing — ``CostModel.breakeven_pct(reference_notional, product)``.
#:
#: DERIVATION OF THE 20. It is the §7.1 swing holding cap / the §6.3 ``rebalance_days`` upper bound —
#: 20 trading sessions ≈ one month. Read it as: **a position must, over the horizon it is actually
#: held, earn at least the one round trip it costs to hold it.** Spread the round trip evenly across
#: those 20 sessions and the per-day bar is ``cost_floor/20``. An edge that merely covers its own
#: costs once per holding period sits exactly ON the floor; anything below it is paying the broker
#: and the spread to take risk.
#:
#: CALIBRATION AGAINST THE RECORDED NUMBERS (IMPROVEMENT_SPEC Part III / F3):
#:   * CNC ₹20k cost floor = 0.2992% fees + 0.0200% spread = 0.3192% ⇒ floor 0.01596 %/day.
#:   * rsi2's four "promotable" runs: median passing split ≈ 0.0006 %/day ⇒ ~27× BELOW the floor.
#:     Even the best recorded baseline (mom, max passing split 0.0058 %/day) is ~3× below it.
#:     Every recorded pre-WO-2 promotion therefore fails, which is the intent.
#:   * A genuinely cost-clearing edge passes: a swing rule that nets one round trip (0.3192%) per
#:     20-session holding period lands exactly on 0.01596 %/day; anything better clears it.
#: Intraday strategies (``orb``) round-trip far more often than once per 20 sessions, so for them
#: this floor is a LOWER bound rather than the true bar — honest, and the sweep's own C3 cost gate
#: plus the fold-pass rule carry that case. Overridable per call, never silently.
MARGIN_FLOOR_DAYS = 20

#: The sweep sizing the cost floor is quoted at when the caller supplies none (WO-2 (iii)).
_DEFAULT_REFERENCE_NOTIONAL = Decimal("20000")


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
    #: WO-3 margin floor: full round-trip friction (fees + spread) at the sweep's sizing, in percent
    #: (``SweepReport.cost_floor_pct``). ``None`` ⇒ the pipeline derives it from the CostModel.
    cost_floor_pct: float | None = None
    #: WO-3 winner-stability flag inputs (all optional; absent ⇒ "not assessed", never a fail).
    grid_density: str | None = None
    adjacent_density: str | None = None
    adjacent_winner: dict[str, float] | None = None


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


class WinnerStability(BaseModel):
    """WO-3 (b): did the ADJACENT grid density select the same winning config? A FLAG, never a fail.

    ``stable is None`` means "not assessed" — no adjacent-density winner was supplied. An unstable
    winner is reported (report note + this structured field) so a reader can weigh it; the promotion
    rule deliberately ignores it, because near-ties on a flat ranking surface are not by themselves
    evidence of a bad strategy — they are evidence about the SURFACE.
    """

    model_config = ConfigDict(frozen=True)

    grid_density: str | None
    adjacent_density: str | None
    winner: dict[str, float]
    adjacent_winner: dict[str, float] | None
    stable: bool | None
    differing_params: list[str] = Field(default_factory=list)

    def note(self) -> str:
        """One-line rendering for ``ValidationReport.notes`` (so it lands in the md artifact)."""
        if self.stable is None:
            return (
                "Winner stability (WO-3): NOT ASSESSED — no adjacent-grid-density winner was "
                "supplied for comparison. Flag only; it never affects the promotion verdict."
            )
        if self.stable:
            return (
                f"Winner stability (WO-3): STABLE — the {self.adjacent_density!r} grid density "
                f"selected the same winning config as {self.grid_density!r}."
            )
        return (
            f"Winner stability (WO-3): UNSTABLE — the {self.adjacent_density!r} grid density "
            f"selected a DIFFERENT winner (differs on: {', '.join(self.differing_params)}). "
            "Flag only, not an auto-fail: it says the ranking surface is flat around the optimum, "
            "so treat the specific parameter values as weakly identified."
        )


def winner_stability(
    winner: Mapping[str, float],
    adjacent_winner: Mapping[str, float] | None,
    *,
    grid_density: str | None = None,
    adjacent_density: str | None = None,
) -> WinnerStability:
    """Compare a sweep winner with the adjacent grid density's winner (pure, §9.6).

    ``adjacent_winner is None`` ⇒ ``stable=None`` ("not assessed"). Parameters present in only one of
    the two dicts count as differing (a density that adds an axis genuinely changed the winner).
    """
    win = {k: float(v) for k, v in winner.items()}
    if adjacent_winner is None:
        return WinnerStability(
            grid_density=grid_density, adjacent_density=adjacent_density,
            winner=win, adjacent_winner=None, stable=None, differing_params=[],
        )
    adj = {k: float(v) for k, v in adjacent_winner.items()}
    differing = sorted(k for k in set(win) | set(adj) if win.get(k) != adj.get(k))
    return WinnerStability(
        grid_density=grid_density, adjacent_density=adjacent_density,
        winner=win, adjacent_winner=adj, stable=not differing, differing_params=differing,
    )


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
    #: WO-3 margin floor inputs/outputs — median expectancy of the PASSING splits, the cost floor it
    #: is measured against, and the resulting per-day bar (all %; None ⇒ not evaluable).
    cpcv_median_passing_expectancy_pct: float | None = None
    cost_floor_pct: float | None = None
    margin_floor_pct_per_day: float | None = None
    #: WO-3 winner-stability FLAG (never part of the promotion rule).
    winner_stability: WinnerStability | None = None
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
def margin_floor_pct_per_day(
    cost_floor_pct: float | None, *, margin_floor_days: int = MARGIN_FLOOR_DAYS
) -> float | None:
    """The WO-3 per-day expectancy floor = ``cost_floor_pct / margin_floor_days`` (see
    :data:`MARGIN_FLOOR_DAYS` for the derivation). ``None`` in ⇒ ``None`` out."""
    if cost_floor_pct is None:
        return None
    if margin_floor_days < 1:
        raise ValueError(f"margin_floor_days must be >= 1, got {margin_floor_days}")
    return float(cost_floor_pct) / float(margin_floor_days)


def promotion_decision(
    n: int | None,
    fold_pass_fraction: float | None,
    *,
    max_dd_pct: float | None = None,
    champion_max_dd_pct: float | None = None,
    median_passing_expectancy_pct: float | None = None,
    cost_floor_pct: float | None = None,
    margin_floor_days: int = MARGIN_FLOOR_DAYS,
) -> tuple[bool, list[str]]:
    """The deterministic §6.4 step-2 pass rule. Returns ``(promotable, reasons_not_promotable)``.

    * no cited N ⇒ not promotable (E2);
    * no CPCV folds ⇒ not promotable (nothing was validated out-of-sample);
    * fold-pass fraction ≥ ``fold_pass_min(N)`` (§9.1) — comparison stays **strict-<**, deliberately
      unchanged by WO-3 (moving a documented boundary silently is a semantics trap);
    * **margin floor (WO-3)**: median PASSING-split expectancy ≥ ``cost_floor_pct /
      margin_floor_days`` per day. FAIL-CLOSED — no cost floor supplied ⇒ not promotable, exactly
      like a missing N: a floor that silently no-ops when the caller forgets to pass it would
      re-open the near-zero-margin hole it exists to close;
    * and, when a champion max-DD is provided (Phase-2 ``ChampionChallenger`` seam),
      max DD ≤ 1.25× champion's.

    Winner stability is NOT here on purpose — WO-3 makes it a report-level flag, not a gate.
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
    # ---- WO-3 margin floor: "how often positive" is not "positive enough to be worth trading" ----
    floor = margin_floor_pct_per_day(cost_floor_pct, margin_floor_days=margin_floor_days)
    if floor is None:
        reasons.append(
            "margin floor NOT EVALUATED — no per-trade cost floor supplied (WO-3). A margin floor "
            "that silently skips is no floor; fail closed."
        )
    elif median_passing_expectancy_pct is None:
        reasons.append(
            f"margin floor NOT EVALUATED — no passing CPCV splits to take a median of; the floor is "
            f"{floor:.5f}%/day (= cost floor {cost_floor_pct:.4f}% / {margin_floor_days} sessions)"
        )
    elif median_passing_expectancy_pct < floor:
        reasons.append(
            f"median passing-split expectancy {median_passing_expectancy_pct:.5f}%/day < margin "
            f"floor {floor:.5f}%/day (= round-trip cost floor {cost_floor_pct:.4f}% / "
            f"{margin_floor_days} sessions, WO-3): the edge is positive but not economically "
            "distinguishable from zero at these costs"
        )
    if champion_max_dd_pct is not None and max_dd_pct is not None:
        cap = CHAMPION_MAX_DD_MULT * champion_max_dd_pct
        if max_dd_pct > cap:
            reasons.append(
                f"max drawdown {max_dd_pct:.2f}% > {CHAMPION_MAX_DD_MULT}x champion's "
                f"({champion_max_dd_pct:.2f}%) = {cap:.2f}% (§6.4 step 2)"
            )
    return (not reasons, reasons)


_COST_FLOOR_MEMO: dict[str, float | None] = {}


def default_cost_floor_pct(strategy_id: str) -> float | None:
    """Round-trip friction (fees + spread, WO-2) at the ₹20,000 sweep sizing, in percent.

    The WO-3 margin floor is a multiple of this. Derived from the SAME ``CostModel`` the sweeps and
    the live gate use, so the floor can never drift from the costs the returns were charged. Unknown
    strategies fall back to **CNC** (delivery — the dearer surface, and the product every non-``orb``
    baseline and the filings rules trade), so an unmapped strategy gets the STRICTER floor.

    Returns ``None`` if the cost model cannot be built at all (missing/broken ``config/costs.yaml``);
    :func:`promotion_decision` then fails closed with an explicit reason rather than skipping the
    floor. Memoized per strategy — imports are function-level (``engine._preload`` discipline).
    """
    if strategy_id in _COST_FLOOR_MEMO:
        return _COST_FLOOR_MEMO[strategy_id]
    try:
        from engine.learning.sweep import PRODUCT_BY_STRATEGY
        from engine.strategy.cost_model import CostModel

        product = PRODUCT_BY_STRATEGY.get(strategy_id, "CNC")
        floor = float(
            CostModel.from_config().breakeven_pct(_DEFAULT_REFERENCE_NOTIONAL, product)
        )
    except Exception as exc:                                    # noqa: BLE001 — reported, not raised
        _log.warning("cost_floor_unavailable", strategy=strategy_id, error=str(exc))
        floor = None
    _COST_FLOOR_MEMO[strategy_id] = floor
    return floor


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
    cost_floor_provider:
        ``strategy_id -> round-trip friction %`` for the WO-3 margin floor. Defaults to
        :func:`default_cost_floor_pct` (the CostModel at the ₹20,000 sweep sizing), so the floor is
        enforced even when the caller passes nothing; a ``ParamSet.cost_floor_pct`` (the sweep's own
        measured floor) overrides it per candidate.
    margin_floor_days:
        The WO-3 constant (default :data:`MARGIN_FLOOR_DAYS` = 20) — see its derivation.
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
        cost_floor_provider: Callable[[str], float | None] | None = None,
        margin_floor_days: int = MARGIN_FLOOR_DAYS,
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
        self._cost_floor = cost_floor_provider or default_cost_floor_pct
        self._margin_floor_days = margin_floor_days
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
        cpcv_folds, pass_fraction, median_passing = self._cpcv(values)

        n = params.trial_count_n
        champion_dd = self._champion_max_dd(strategy_id)
        cost_floor = (
            params.cost_floor_pct
            if params.cost_floor_pct is not None
            else self._cost_floor(strategy_id)
        )
        floor = margin_floor_pct_per_day(cost_floor, margin_floor_days=self._margin_floor_days)
        stability = winner_stability(
            params.params,
            params.adjacent_winner,
            grid_density=params.grid_density,
            adjacent_density=params.adjacent_density,
        )
        promotable, reasons = promotion_decision(
            n,
            pass_fraction,
            max_dd_pct=max_dd,
            champion_max_dd_pct=champion_dd,
            median_passing_expectancy_pct=median_passing,
            cost_floor_pct=cost_floor,
            margin_floor_days=self._margin_floor_days,
        )
        notes = [stability.note()]
        if floor is not None:
            notes.append(
                f"Margin floor (WO-3): median passing-split expectancy must be >= {floor:.5f}%/day "
                f"(= round-trip friction {cost_floor:.4f}% at the ₹{_DEFAULT_REFERENCE_NOTIONAL} "
                f"sweep sizing, incl. the measured spread, spread over {self._margin_floor_days} "
                f"sessions). Observed: "
                + ("no passing splits" if median_passing is None else f"{median_passing:.5f}%/day")
                + "."
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
            cpcv_median_passing_expectancy_pct=median_passing,
            cost_floor_pct=cost_floor,
            margin_floor_pct_per_day=floor,
            winner_stability=stability,
            promotable=promotable,
            reasons=reasons,
            sweep_stats=params.sweep_stats,
            notes=notes,
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

    def _cpcv(self, values: np.ndarray) -> tuple[list[CPCVFold], float | None, float | None]:
        """Returns ``(folds, pass_fraction, median_passing_expectancy_pct)``.

        The median is over the PASSING splits only (WO-3): it answers "when this edge worked, by how
        much?" — the question a pass FRACTION cannot answer, and the one four boundary-exact rsi2
        promotions turned out to answer with ~0.0006%/day.
        """
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
            return [], None, None
        passing = [f.expectancy_pct for f in folds if f.passed and f.expectancy_pct is not None]
        median_passing = float(np.median(passing)) if passing else None
        return folds, sum(f.passed for f in folds) / len(folds), median_passing

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
