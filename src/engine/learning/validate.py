"""ValidationPipeline — anchored walk-forward + skfolio CPCV + the
multiple-testing-deflated promotion decision.

Semantics:

* **Anchored walk-forward** — 6 months train / 1 month test, rolling: the train window is always
  anchored at the start of the data; each successive fold's 1-month test slice rolls forward. With
  fixed candidate params (no re-fit inside this pipeline) the folds are successive out-of-sample months —
  reported per fold, never pooled into the promotion rule.
* **CPCV** — ``skfolio.model_selection.CombinatorialPurgedCV`` (purge 5 d, embargo 5 d) over
  the **cost-adjusted daily return series** of the candidate. Fold expectancy = mean net daily
  return over the fold's test observations; a fold passes iff that expectancy is **> 0 after costs**.
* **fold_pass_min(N)** (exact boundaries): 60% for N ≤ 10, 70% for
  11 ≤ N ≤ 30, 80% for N > 30 — monotone deflation in the trial count N.
* **Trial count N** = the optimizer-reported count of every configuration evaluated
  (``SweepReport.trial_count_n``) — NOT the count of surfaced ``param_sets`` rows.
  A report with **no cited N is not promotable**. Promotable iff the CPCV fold-pass fraction
  ≥ ``fold_pass_min(N)`` — plus, when a champion max-DD is supplied (seam; ``ChampionChallenger`` not yet built), max DD
  ≤ 1.25× champion's.
* **Margin floor** — a fold-pass FRACTION says how often the edge was positive, never by how much,
  so promotion ALSO requires the **median passing-split expectancy ≥ cost_floor /
  MARGIN_FLOOR_DAYS per day** (see :data:`MARGIN_FLOOR_DAYS`). The ``fold_pass_fraction``
  comparison stays strict-``<``: flipping it to ``<=`` would move a documented boundary invisibly.
* **Winner stability** — the report records whether the adjacent grid density picked the same
  winner as a **flag** (:class:`WinnerStability`), NOT an auto-fail: instability is evidence about
  the ranking surface's flatness, and a hard gate would discard robust strategies whose
  neighbouring configs are near-ties.
* **Persistence** — every validated candidate is logged to SQLite ``param_sets``
  (``status='candidate'``, ``validation_report`` JSON citing N, ``evaluated_at``) for audit, and a
  report artifact (md + json) is written via :mod:`engine.learning.reports`.

The N here is per-validation-run; the rolling-window / per-``feature_set_version`` N
bookkeeping belongs to ``ChampionChallenger`` (not yet built; autonomous promotion is
online learning, also not built), which passes the windowed N in via
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

#: Pinned defaults: purge 5 d, embargo 5 d. Fold counts are not pinned;
#: 6 folds × 2 test folds gives C(6,2) = 15 combinatorial splits (constructor knobs).
CPCV_PURGE_DAYS = 5
CPCV_EMBARGO_DAYS = 5
CPCV_N_FOLDS = 6
CPCV_N_TEST_FOLDS = 2
WF_TRAIN_MONTHS = 6
WF_TEST_MONTHS = 1
CHAMPION_MAX_DD_MULT = 1.25            # max DD ≤ 1.25× champion's

#: Margin floor: the median PASSING CPCV split must earn at least ``cost_floor / 20`` per day,
#: where ``cost_floor`` is the strategy's full round-trip friction (fees + spread) at the
#: sweep's sizing — ``CostModel.breakeven_pct(reference_notional, product)``.
#:
#: The 20 is the risk-limit swing holding cap / the envelope ``rebalance_days`` upper bound (≈ one
#: month): **a position must, over the horizon it is actually held, earn at least the one round trip
#: it costs to hold it**, so the per-day bar is ``cost_floor/20``. An edge that merely covers its
#: costs once per holding period sits exactly ON the floor (CNC ₹20k: 0.2992% fees + 0.0200% spread
#: = 0.3192% ⇒ 0.01596 %/day).
#: Intraday strategies (``orb``) round-trip far more often, so for them this is a LOWER bound; the
#: sweep's cost gate plus the fold-pass rule carry that case. A POSITIONAL leg (``trend``) has cap
#: ``max_holding.position_trading_days`` = 120, so 20 overstates its per-day bar 6×: the denominator
#: must be the horizon the strategy is actually held for. Overridable per call, never silently.
MARGIN_FLOOR_DAYS = 20

#: The sweep sizing the cost floor is quoted at when the caller supplies none.
_DEFAULT_REFERENCE_NOTIONAL = Decimal("20000")


# --------------------------------------------------------------------------- fold_pass_min
def fold_pass_min(n: int) -> float:
    """Minimum CPCV fold-pass fraction for trial count ``n`` (exact boundaries).

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
class RealizedHold(BaseModel):
    """The sweep's MEASURED holding distribution + open/closed trade split for one config.

    **Reporting only — nothing here reaches :func:`promotion_decision`.** The margin floor's
    denominator is a *horizon*, and 20 (swing) / 120 (positional) are CAPS; a floor quoted at a cap
    can read far more comfortable than at the realized median hold, so the measured numbers travel
    with the verdict and :func:`realized_hold_floor_cells` re-bases the floor on them.

    Durations are in SESSIONS. The caller must not build one from an intraday sweep, whose
    ``hold_bars_*`` are 1-minute bars (``SweepReport.bar_unit`` says which).
    """

    model_config = ConfigDict(frozen=True)

    n_trades: int
    n_closed: int
    n_open: int
    #: The sweep's per-trade net return over ALL trades (open ones at unrealized mark-to-market, no
    #: exit leg paid) and over CLOSED round trips only. The CLOSED one is the headline and the
    #: statistic the grid winner is ranked on.
    expectancy_per_trade_pct: float | None = None
    expectancy_per_trade_closed_pct: float | None = None
    mean_sessions: float | None = None
    median_sessions: float | None = None
    p90_sessions: float | None = None
    mean_sessions_closed: float | None = None
    median_sessions_closed: float | None = None
    p90_sessions_closed: float | None = None


class MarginFloorCell(BaseModel):
    """The margin floor re-based on ONE measured holding period — REPORTING ONLY.

    ``headroom_x`` is the observed median passing-split expectancy divided by this cell's floor. The
    verdict is decided at the registered denominator alone: a threshold taken from a statistic
    measured on the run being judged would make the bar a function of the result.
    """

    model_config = ConfigDict(frozen=True)

    label: str
    hold_sessions: float
    margin_floor_pct_per_day: float
    headroom_x: float | None = None

class ParamSet(BaseModel):
    """A candidate parameter set entering validation.

    ``trial_count_n`` is the optimizer-reported evaluated-config count for this strategy's window
    (``SweepReport.trial_count_n``; the not-yet-built ``ChampionChallenger`` would supply the rolling-window N).
    ``None`` ⇒ the resulting report is not promotable. ``sweep_stats`` optionally carries the
    sweep's trade-level stats for report context (never part of the promotion rule).
    """

    model_config = ConfigDict(frozen=True)

    param_set_id: str = Field(default_factory=lambda: str(ULID()))
    strategy_id: str
    params: dict[str, float]
    trial_count_n: int | None = None
    sweep_stats: dict[str, float | None] | None = None
    #: Margin floor: full round-trip friction (fees + spread) at the sweep's sizing, in percent
    #: (``SweepReport.cost_floor_pct``). ``None`` ⇒ the pipeline derives it from the CostModel.
    cost_floor_pct: float | None = None
    #: Winner-stability flag inputs (all optional; absent ⇒ "not assessed", never a fail).
    grid_density: str | None = None
    adjacent_density: str | None = None
    adjacent_winner: dict[str, float] | None = None
    #: REPORTING-ONLY inputs: the measured holding distribution behind these returns, and whether the
    #: population is a survivorship-tainted proxy. Rendered and persisted; NEITHER feeds
    #: :func:`promotion_decision`.
    realized_hold: RealizedHold | None = None
    population_is_survivorship_tainted_proxy: bool | None = None
    #: The sweep's mechanics stamp (``SweepReport.mechanics``), carried into the artifact so a verdict
    #: names the mechanics its returns came from. ``None`` ⇒ pre-stamp sweep or no sweep; never a
    #: promotion input.
    sweep_mechanics: str | None = None
    #: False when ``params`` are NOT a grid winner — the sweep ranked nothing (every config closed no
    #: round trip, or the open/closed split was unreadable) and the caller fell back to the envelope
    #: DEFAULTS. REPORTING ONLY (gating on it would change the promotion rule); it exists so a
    #: defaults verdict never RENDERS as a grid-winner verdict. ``None`` ⇒ the caller ran no sweep.
    params_are_grid_winner: bool | None = None


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
    """Winner stability: did the ADJACENT grid density select the same winning config? A FLAG, never a fail.

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
                "Winner stability: NOT ASSESSED — no adjacent-grid-density winner was "
                "supplied for comparison. Flag only; it never affects the promotion verdict."
            )
        if self.stable:
            return (
                f"Winner stability: STABLE — the {self.adjacent_density!r} grid density "
                f"selected the same winning config as {self.grid_density!r}."
            )
        return (
            f"Winner stability: UNSTABLE — the {self.adjacent_density!r} grid density "
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
    """Compare a sweep winner with the adjacent grid density's winner (pure).

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
    """Validation output. MUST cite the trial count N — a report with no cited N is not promotable.

    Statistics are floats deliberately (they are statistics, not ledger money — same argument as
    ``engine.strategy.indicators``); nothing here is ever a persisted price. ``max_drawdown_pct``
    is a positive magnitude (17.0 ⇒ a −17% peak-to-trough).
    """

    model_config = ConfigDict(frozen=True)

    strategy_id: str
    param_set_id: str
    params: dict[str, float]
    trial_count_n: int | None                       # the cited N; None ⇒ not promotable
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
    #: Margin floor inputs/outputs — median expectancy of the PASSING splits, the cost floor it
    #: is measured against, and the resulting per-day bar (all %; None ⇒ not evaluable).
    cpcv_median_passing_expectancy_pct: float | None = None
    cost_floor_pct: float | None = None
    margin_floor_pct_per_day: float | None = None
    #: The denominator the floor was taken at, in sessions (:data:`MARGIN_FLOOR_DAYS` unless the
    #: caller overrode it), so the artifact alone reproduces the floor.
    margin_floor_days: int | None = None
    #: REPORTING-ONLY block: the measured holding distribution, the floor re-based on it, and the
    #: survivorship flag. None of it feeds :func:`promotion_decision`.
    realized_hold: RealizedHold | None = None
    realized_hold_cells: list[MarginFloorCell] = Field(default_factory=list)
    population_is_survivorship_tainted_proxy: bool | None = None
    #: ``SweepReport.mechanics`` — reporting only. ``None`` ⇒ pre-stamp sweep or a provider that
    #: stamps nothing.
    sweep_mechanics: str | None = None
    #: ``ParamSet.params_are_grid_winner`` mirrored onto the verdict: False ⇒ envelope DEFAULTS, not a
    #: sweep winner. Reporting only; rendered as a banner and named in the notes.
    params_are_grid_winner: bool | None = None
    #: Winner-stability FLAG (never part of the promotion rule).
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
    """Anchored walk-forward boundaries over observation ``dates`` (6 m train / 1 m test).

    Returns ``(train_start, train_end, test_start, test_end)`` tuples with exclusive ends and
    ``train_end == test_start``; the train window is always anchored at the first date. Folds whose
    test window contains no observations are skipped. Pure and deterministic: the same date
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
    """Combinatorial purged CV index splits via skfolio (purge 5 d, embargo 5 d).

    Returns ``(train_indices, test_indices)`` pairs over ``range(n_obs)`` daily observations.
    skfolio already purges/embargoes; as an invariant enforcement (the no-overlap property)
    every train index within ``purge`` observations before or ``embargo`` after any test block is
    dropped again here — belt and braces, so the guarantee cannot regress with a skfolio upgrade.
    Empty list when ``n_obs`` is too small to form meaningful folds — skfolio requires
    ``purge + embargo < (n_obs // n_folds) − 1``; below that a valid purged CPCV cannot be formed, so
    we return ``[]`` (⇒ no CPCV folds ⇒ not promotable) rather than raise.
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


# --------------------------------------------------------------------------- promotion rule
def margin_floor_pct_per_day(
    cost_floor_pct: float | None, *, margin_floor_days: int = MARGIN_FLOOR_DAYS
) -> float | None:
    """The per-day expectancy margin floor = ``cost_floor_pct / margin_floor_days`` (see
    :data:`MARGIN_FLOOR_DAYS` for the derivation). ``None`` in ⇒ ``None`` out."""
    if cost_floor_pct is None:
        return None
    if margin_floor_days < 1:
        raise ValueError(f"margin_floor_days must be >= 1, got {margin_floor_days}")
    return float(cost_floor_pct) / float(margin_floor_days)


#: The REPORTING-ONLY horizons :func:`realized_hold_floor_cells` re-bases the floor on, as
#: ``(label, RealizedHold attribute)``; closed-only too because an open trade contributes its AGE
#: at the window edge, not a realized hold.
_REALIZED_HOLD_CELLS: tuple[tuple[str, str], ...] = (
    ("realized MEDIAN hold (all trades)", "median_sessions"),
    ("realized MEAN hold (all trades)", "mean_sessions"),
    ("realized MEDIAN hold (closed trades only)", "median_sessions_closed"),
    ("realized MEAN hold (closed trades only)", "mean_sessions_closed"),
)


def realized_hold_floor_cells(
    realized_hold: RealizedHold | None,
    cost_floor_pct: float | None,
    median_passing_expectancy_pct: float | None,
) -> list[MarginFloorCell]:
    """The margin floor re-based on each MEASURED holding period — REPORTING ONLY.

    Same arithmetic as :func:`margin_floor_pct_per_day`, only the denominator changes: a CAP is an
    upper bound on the hold, so a floor taken at the cap is the loosest bar. ``headroom_x`` is the
    observed median passing-split expectancy over the cell's floor.

    Pure and total: missing inputs and non-positive holds (``cost_floor / 0`` is no bar) yield fewer
    cells, never an exception — a reporting path must not be able to fail a validation run.
    """
    if realized_hold is None or cost_floor_pct is None:
        return []
    cells: list[MarginFloorCell] = []
    for label, attr in _REALIZED_HOLD_CELLS:
        sessions = getattr(realized_hold, attr, None)
        if sessions is None or not np.isfinite(sessions) or float(sessions) <= 0.0:
            continue
        floor = float(cost_floor_pct) / float(sessions)
        headroom = (
            None
            if median_passing_expectancy_pct is None or floor <= 0.0
            else float(median_passing_expectancy_pct) / floor
        )
        cells.append(
            MarginFloorCell(
                label=label,
                hold_sessions=float(sessions),
                margin_floor_pct_per_day=floor,
                headroom_x=headroom,
            )
        )
    return cells


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
    """The deterministic pass rule. Returns ``(promotable, reasons_not_promotable)``.

    * no cited N ⇒ not promotable;
    * no CPCV folds ⇒ not promotable (nothing was validated out-of-sample);
    * fold-pass fraction ≥ ``fold_pass_min(N)`` — comparison stays **strict-<**, deliberately
      unchanged by the margin floor (moving a documented boundary silently is a semantics trap);
    * **margin floor**: median PASSING-split expectancy ≥ ``cost_floor_pct /
      margin_floor_days`` per day. FAIL-CLOSED — no cost floor supplied ⇒ not promotable, exactly
      like a missing N: a floor that silently no-ops when the caller forgets to pass it would
      re-open the near-zero-margin hole it exists to close;
    * and, when a champion max-DD is provided (``ChampionChallenger`` seam, not yet built),
      max DD ≤ 1.25× champion's.

    Winner stability is NOT here on purpose — it is a report-level flag, not a gate.
    """
    reasons: list[str] = []
    if n is None:
        reasons.append(
            "trial count N is ABSENT — a report with no cited N is not promotable"
        )
    if fold_pass_fraction is None:
        reasons.append("CPCV produced no folds (insufficient observations) — not validated out-of-sample")
    if n is not None and fold_pass_fraction is not None:
        need = fold_pass_min(n)
        if fold_pass_fraction < need:
            reasons.append(
                f"CPCV fold-pass fraction {fold_pass_fraction:.1%} < fold_pass_min(N={n}) = {need:.0%}"
            )
    # ---- margin floor: "how often positive" is not "positive enough to be worth trading" ----
    floor = margin_floor_pct_per_day(cost_floor_pct, margin_floor_days=margin_floor_days)
    if floor is None:
        reasons.append(
            "margin floor NOT EVALUATED — no per-trade cost floor supplied. A margin floor "
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
            f"{margin_floor_days} sessions): the edge is positive but not economically "
            "distinguishable from zero at these costs"
        )
    if champion_max_dd_pct is not None and max_dd_pct is not None:
        cap = CHAMPION_MAX_DD_MULT * champion_max_dd_pct
        if max_dd_pct > cap:
            reasons.append(
                f"max drawdown {max_dd_pct:.2f}% > {CHAMPION_MAX_DD_MULT}x champion's "
                f"({champion_max_dd_pct:.2f}%) = {cap:.2f}%"
            )
    return (not reasons, reasons)


_COST_FLOOR_MEMO: dict[str, float | None] = {}


def default_cost_floor_pct(strategy_id: str) -> float | None:
    """Round-trip friction (fees + spread) at the ₹20,000 sweep sizing, in percent.

    The margin floor is a multiple of this. Derived from the SAME ``CostModel`` the sweeps and
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
    """``ValidationPipeline``: walk-forward + CPCV over cost-adjusted returns.

    Parameters
    ----------
    returns_provider:
        ``(strategy_id, params) -> pd.Series`` of cost-adjusted DAILY net returns (the
        ``SweepRunner.returns_for`` closure in production; a synthetic series in tests).
    clock:
        The single source of "now" — stamps ``generated_at`` / ``evaluated_at``.
    conn:
        SQLite connection for the ``param_sets`` audit row (``None`` ⇒ persistence skipped —
        offline analysis only). ``learning`` writes only its own tables.
    reports_dir:
        Directory for the md+json report artifact (``None`` ⇒ artifact skipped).
    splitter:
        CPCV splitter override (defaults to skfolio :func:`cpcv_splits` with the default knobs).
    champion_max_dd_provider:
        ``strategy_id -> champion max-DD %`` (``ChampionChallenger`` seam, not yet built); ``None`` values
        skip the 1.25× DD comparison (no champion exists yet).
    cost_floor_provider:
        ``strategy_id -> round-trip friction %`` for the margin floor. Defaults to
        :func:`default_cost_floor_pct` (the CostModel at the ₹20,000 sweep sizing), so the floor is
        enforced even when the caller passes nothing; a ``ParamSet.cost_floor_pct`` (the sweep's own
        measured floor) overrides it per candidate.
    margin_floor_days:
        The margin-floor constant (default :data:`MARGIN_FLOOR_DAYS` = 20) — see its derivation.
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

    # ------------------------------------------------------------------ public surface
    async def validate(self, strategy_id: str, params: ParamSet) -> ValidationReport:
        """Validate a candidate (pinned signature). CPU-bound work is executor-offloaded
        (`asyncio.to_thread`) so the in-engine caller never blocks the loop."""
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
        # Built AFTER promotion_decision and fed only into the artifact: the verdict stays a function
        # of the registered denominator, never of a horizon measured on the run it judges.
        hold_cells = realized_hold_floor_cells(params.realized_hold, cost_floor, median_passing)
        notes = [stability.note()]
        if floor is not None:
            notes.append(
                f"Margin floor: median passing-split expectancy must be >= {floor:.5f}%/day "
                f"(= round-trip friction {cost_floor:.4f}% at the ₹{_DEFAULT_REFERENCE_NOTIONAL} "
                f"sweep sizing, incl. the measured spread, spread over {self._margin_floor_days} "
                f"sessions). Observed: "
                + ("no passing splits" if median_passing is None else f"{median_passing:.5f}%/day")
                + "."
            )
        notes.extend(self._realized_hold_notes(params.realized_hold, hold_cells))
        if params.params_are_grid_winner is False:
            # Reporting only, NOT a promotion gate; it must never render silently, because the cited
            # N, sweep context and stability flag all describe a grid the params did not come from.
            notes.append(
                "PARAMS ARE NOT A GRID WINNER: the sweep ranked NO configuration (every config "
                "closed no round trip inside the window, or its open/closed split was unreadable), "
                "so these params are the parameter-envelope DEFAULTS substituted by the caller. The "
                "cited trial count N, the sweep context and the winner-stability flag below all "
                "describe a grid that selected nothing — read this verdict as a validation of the "
                "DEFAULT config at this window, never as a swept result."
            )
        if params.population_is_survivorship_tainted_proxy:
            notes.append(
                "SURVIVORSHIP (2026-09-12): the population behind these returns is a "
                "survivorship-tainted proxy — a present-day symbol list applied BACKWARDS, because "
                "this platform stores no point-in-time index membership. Every LEVEL quoted here "
                "(expectancy, total return, the sweep's per-trade figures) is biased HIGH by an "
                "amount that cannot be measured from what is stored; comparisons against other runs "
                "on the SAME list are unaffected. Carried in the JSON as "
                "`population_is_survivorship_tainted_proxy: true`."
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
            margin_floor_days=self._margin_floor_days,
            realized_hold=params.realized_hold,
            realized_hold_cells=hold_cells,
            population_is_survivorship_tainted_proxy=(
                params.population_is_survivorship_tainted_proxy
            ),
            sweep_mechanics=params.sweep_mechanics,
            params_are_grid_winner=params.params_are_grid_winner,
            winner_stability=stability,
            promotable=promotable,
            reasons=reasons,
            sweep_stats=params.sweep_stats,
            notes=notes,
            generated_at=self._clock.now(),
        )

    def _realized_hold_notes(
        self, hold: RealizedHold | None, cells: Sequence[MarginFloorCell]
    ) -> list[str]:
        """Reporting-only notes: the measured hold, the re-based floors, open-vs-closed trades."""
        if hold is None:
            return []
        notes: list[str] = []
        if cells:
            rebased = "; ".join(
                f"{c.label} {c.hold_sessions:.2f} sessions => {c.margin_floor_pct_per_day:.5f}%/day"
                + ("" if c.headroom_x is None else f" ({c.headroom_x:.2f}x headroom)")
                for c in cells
            )
            notes.append(
                "Margin floor at the MEASURED holding period (2026-09-12) — REPORTING ONLY: the "
                f"verdict above is decided at the registered {self._margin_floor_days}-session "
                "denominator and nothing here moves it. A denominator is a HORIZON, and 20 (swing) "
                "and 120 (positional) are risk-limit CAPS, so a floor quoted at a cap is the loosest bar "
                f"the horizon can justify. Re-based on this run's own trades: {rebased}."
            )
        if hold.n_trades:
            all_txt = (
                "—" if hold.expectancy_per_trade_pct is None
                else f"{hold.expectancy_per_trade_pct:+.4f}%"
            )
            closed_txt = (
                "—" if hold.expectancy_per_trade_closed_pct is None
                else f"{hold.expectancy_per_trade_closed_pct:+.4f}%"
            )
            notes.append(
                f"Per-trade expectancy, open/closed split: {hold.n_trades} trades = "
                f"{hold.n_closed} closed + {hold.n_open} still OPEN at the window edge. Mean net "
                f"return per trade over CLOSED round trips {closed_txt} — since the sweep-mechanics fix "
                "(2026-09-13) that is the sweep headline AND the statistic the grid winner was "
                f"ranked on — vs {all_txt} over ALL trades, reported beside it. An open trade "
                "contributes unrealized mark-to-market and has not paid an exit leg, so where the "
                "two differ the all-trades figure is the more optimistic of the pair."
            )
        return notes

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

        The median is over the PASSING splits only: "when this edge worked, by how much?" — the
        question a pass FRACTION cannot answer.
        """
        splits = self._splitter(len(values))
        folds: list[CPCVFold] = []
        for i, (train_idx, test_idx) in enumerate(splits):
            sl = values[np.asarray(test_idx, dtype=np.int64)]
            expectancy = float(sl.mean() * 100.0) if len(sl) else None
            passed = expectancy is not None and expectancy > 0.0  # > 0 after costs, strictly
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
        """Audit row: every surfaced candidate is a ``param_sets`` row
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
