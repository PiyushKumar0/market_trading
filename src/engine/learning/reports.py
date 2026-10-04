"""Walk-forward + CPCV report renderer (§6.4 step 2, §8.2, C9) — markdown + JSON artifacts.

Renders a :class:`~engine.learning.validate.ValidationReport` (and, for the sweep leg, a
:class:`~engine.learning.sweep.SweepReport`) to ``data/reports/<strategy>_<ts>.md`` + ``.json``.

Honesty contract (C9 — a negative expectancy is a VALID deliverable, never massaged):

* the **promotion verdict and every reason it is not promotable** are the first thing in the report;
* a **negative cost-adjusted expectancy is surfaced in a prominent banner**, not buried in a table;
* the **cited trial count N** and the ``fold_pass_min(N)`` bar it must clear are always shown — an
  absent N is called out as "NOT PROMOTABLE (no cited N)", exactly the §6.4 step-2 rule.

The JSON artifact is the machine-readable record (``model_dump_json`` — dates/Decimals ISO/string
serialized); the ``param_sets.validation_report`` column persists the same report body (validate.py).
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from engine.learning.sweep import SweepReport
    from engine.learning.validate import ValidationReport


@dataclass(frozen=True)
class ReportArtifacts:
    """Paths written for one report."""

    markdown: Path
    json: Path


def _pct(value: float | None, dp: int = 3) -> str:
    return "—" if value is None else f"{value:+.{dp}f}%"


def _num(value: float | None, dp: int = 2) -> str:
    """A plain (unsigned) number — holding durations are counts, never signed returns."""
    return "—" if value is None else f"{value:.{dp}f}"


def _slug_ts(generated_at) -> str:
    return generated_at.strftime("%Y%m%dT%H%M%S")


def _margin_floor_txt(report: ValidationReport) -> str:
    """The WO-3 floor with the DENOMINATOR it was taken at — the horizon is a protocol choice
    (20 sessions for a swing leg, the §7.1 120 for a positional one), so a report that printed only
    the resulting %/day would not be reproducible from the artifact alone."""
    if report.margin_floor_pct_per_day is None:
        return "not evaluated (no round-trip cost floor supplied — fail-closed, see Reasons)"
    denom = (
        ""
        if report.margin_floor_days is None or report.cost_floor_pct is None
        else (
            f" (= round-trip cost floor {report.cost_floor_pct:.4f}% / "
            f"{report.margin_floor_days} sessions)"
        )
    )
    return f"{report.margin_floor_pct_per_day:.5f}%/day{denom}"


# --------------------------------------------------------------------------- validation report
def render_markdown(report: ValidationReport) -> str:
    """Render a ``ValidationReport`` to markdown with the promotion verdict + negatives up top (C9)."""
    lines: list[str] = []
    lines.append(f"# Validation report — `{report.strategy_id}`")
    lines.append("")
    lines.append(f"_Generated {report.generated_at.isoformat()} · param_set `{report.param_set_id}`_")
    lines.append("")
    # WO-M (2026-09-13): the sweep-mechanics stamp travels with the VERDICT too — a verdict is only
    # comparable to another verdict produced the same way. Emitted ONLY when the ParamSet carries a
    # stamp: this pipeline also validates returns that never came out of SweepRunner at all (the
    # event-study harnesses build their own series and charge their own fills/costs), and printing
    # "PRE-2026-09-13 (unstamped)" on one of those would assert three sweep biases it cannot have.
    # Absence is the tell here, exactly as it is on the SweepReport side — with the difference that
    # on THIS side absence means "pre-fix sweep OR no sweep", which COMMANDS.md says in words.
    if report.sweep_mechanics:
        lines.append(f"_Sweep mechanics: `{report.sweep_mechanics}`_")
        lines.append("")

    # ---- verdict banner (first, always) -----------------------------------------------------
    if report.promotable:
        lines.append("## VERDICT: PROMOTABLE")
    else:
        lines.append("## VERDICT: NOT PROMOTABLE")
        lines.append("")
        lines.append("Reasons:")
        for r in report.reasons:
            lines.append(f"- {r}")
    lines.append("")

    # ---- defaults-fallback banner ------------------------------------------------------------
    # A verdict on params the grid never ranked must not render like a verdict on a grid winner.
    # Sits with the verdict, above every other statistic, because it changes what ALL of them mean.
    if report.params_are_grid_winner is False:
        lines.append(
            "> **THESE PARAMS ARE NOT A GRID WINNER.** The sweep ranked no configuration (none "
            "closed a round trip inside the window, or the open/closed split was unreadable), so "
            "the §6.3 envelope DEFAULTS were validated instead. The trial count, sweep context and "
            "winner-stability flag below describe a grid that selected nothing."
        )
        lines.append("")

    # ---- honest negative-expectancy banner (C9) ---------------------------------------------
    if report.expectancy_pct is not None and report.expectancy_pct < 0.0:
        lines.append(
            f"> **NEGATIVE COST-ADJUSTED EXPECTANCY: {report.expectancy_pct:+.4f}% / day.** "
            "Reported honestly (C9) — a negative result is a valid deliverable, not massaged away."
        )
        lines.append("")

    # ---- multiple-testing header ------------------------------------------------------------
    n_txt = "ABSENT (⇒ not promotable, E2)" if report.trial_count_n is None else str(report.trial_count_n)
    bar_txt = "n/a" if report.fold_pass_min is None else f"{report.fold_pass_min:.0%}"
    frac_txt = (
        "n/a"
        if report.cpcv_fold_pass_fraction is None
        else f"{report.cpcv_fold_pass_fraction:.1%}"
    )
    lines.append("## Multiple-testing discipline (§6.4)")
    lines.append("")
    lines.append(f"- **Cited trial count N**: {n_txt}")
    lines.append(f"- **Required CPCV fold-pass (`fold_pass_min(N)`)**: {bar_txt}")
    lines.append(f"- **Observed CPCV fold-pass fraction**: {frac_txt}")
    lines.append(f"- **Margin floor (WO-3)**: {_margin_floor_txt(report)}")
    lines.append(
        "- **Observed median passing-split expectancy**: "
        + (
            "no passing splits"
            if report.cpcv_median_passing_expectancy_pct is None
            else f"{report.cpcv_median_passing_expectancy_pct:+.5f}%/day"
        )
    )
    lines.append("")

    # ---- R2 realized-hold cells (REPORTING ONLY, 2026-09-12) ---------------------------------
    # The bar above is quoted at a CAP (20 swing / 120 positional). These rows re-base the same
    # round-trip floor on the horizon this run was actually held for, so "passes the floor" can
    # never be read as comfortable without the reader seeing at what horizon.
    if report.realized_hold_cells:
        lines.append("### Margin floor at the MEASURED holding period (reporting only, R2)")
        lines.append("")
        lines.append(
            f"Not part of the promotion rule — the verdict above stands on the registered "
            f"{report.margin_floor_days}-session denominator. Headroom = observed median "
            "passing-split expectancy ÷ that cell's floor."
        )
        lines.append("")
        lines.append("| horizon | sessions | margin floor | headroom |")
        lines.append("|:--------|---------:|-------------:|---------:|")
        for c in report.realized_hold_cells:
            head = "—" if c.headroom_x is None else f"{c.headroom_x:.2f}×"
            lines.append(
                f"| {c.label} | {c.hold_sessions:.2f} | "
                f"{c.margin_floor_pct_per_day:.5f}%/day | {head} |"
            )
        lines.append("")
    if report.realized_hold is not None and report.realized_hold.n_trades:
        h = report.realized_hold
        lines.append("### Holding period + open-trade split (reporting only, R2)")
        lines.append("")
        lines.append(
            f"- Trades: {h.n_trades}  ·  closed: {h.n_closed}  ·  **still open at the window "
            f"edge: {h.n_open}**"
        )
        lines.append(
            f"- Per-trade net return — CLOSED round trips only (the sweep headline and ranking "
            f"statistic since WO-M): {_pct(h.expectancy_per_trade_closed_pct, 4)}  ·  ALL trades "
            f"incl. open marks (reporting only): {_pct(h.expectancy_per_trade_pct, 4)}"
        )
        lines.append(
            f"- Holding sessions, all trades — mean {_num(h.mean_sessions)} · median "
            f"{_num(h.median_sessions)} · p90 {_num(h.p90_sessions)}"
        )
        lines.append(
            f"- Holding sessions, closed only — mean {_num(h.mean_sessions_closed)} · median "
            f"{_num(h.median_sessions_closed)} · p90 {_num(h.p90_sessions_closed)}"
        )
        lines.append(
            "- An OPEN trade contributes its AGE at the window edge, not a realized hold, and its "
            "return is unrealized mark-to-market with no exit leg paid."
        )
        lines.append("")
    if report.population_is_survivorship_tainted_proxy:
        lines.append(
            "> **SURVIVORSHIP: the population is a tainted proxy** — a present-day symbol list "
            "applied backwards (no point-in-time index membership is stored anywhere in this "
            "platform). Every LEVEL above is biased HIGH by an unmeasurable amount; comparisons "
            "against other runs on the same list are unaffected."
        )
        lines.append("")

    # ---- summary stats ----------------------------------------------------------------------
    lines.append("## Cost-adjusted summary")
    lines.append("")
    span = (
        f"{report.data_start} → {report.data_end}"
        if report.data_start and report.data_end
        else "—"
    )
    lines.append(f"- Observations: {report.n_obs}  ·  span: {span}")
    lines.append(f"- Expectancy (mean net daily return): {_pct(report.expectancy_pct, 4)}")
    lines.append(f"- Total net return: {_pct(report.total_return_pct)}")
    lines.append(
        f"- Max drawdown: {'—' if report.max_drawdown_pct is None else f'{report.max_drawdown_pct:.2f}%'}"
    )
    lines.append("")
    lines.append("### Parameters")
    lines.append("")
    for k in sorted(report.params):
        lines.append(f"- `{k}` = {report.params[k]}")
    lines.append("")

    # ---- CPCV per-fold table ----------------------------------------------------------------
    lines.append("## CPCV folds (skfolio CombinatorialPurgedCV — purge 5d / embargo 5d)")
    lines.append("")
    if report.cpcv:
        lines.append("| split | test obs | expectancy | pass (>0 after costs) |")
        lines.append("|------:|---------:|-----------:|:----------------------|")
        for f in report.cpcv:
            lines.append(
                f"| {f.split} | {f.n_test_obs} | {_pct(f.expectancy_pct, 4)} | "
                f"{'PASS' if f.passed else 'fail'} |"
            )
    else:
        lines.append("_No CPCV folds — insufficient observations to validate out-of-sample._")
    lines.append("")

    # ---- walk-forward per-fold table --------------------------------------------------------
    lines.append("## Anchored walk-forward (6m train / 1m test, rolling)")
    lines.append("")
    if report.walk_forward:
        lines.append("| fold | test window | obs | expectancy | total |")
        lines.append("|-----:|:------------|----:|-----------:|------:|")
        for f in report.walk_forward:
            lines.append(
                f"| {f.fold} | {f.test_start} → {f.test_end} | {f.n_obs} | "
                f"{_pct(f.expectancy_pct, 4)} | {_pct(f.total_return_pct)} |"
            )
    else:
        lines.append("_No walk-forward folds — span shorter than one 6m train + 1m test._")
    lines.append("")

    # ---- sweep context + notes --------------------------------------------------------------
    if report.sweep_stats:
        lines.append("## Sweep context (informational — not part of the promotion rule)")
        lines.append("")
        for k in sorted(report.sweep_stats):
            v = report.sweep_stats[k]
            lines.append(f"- `{k}`: {'—' if v is None else v}")
        lines.append("")
    if report.notes:
        lines.append("## Notes / documented approximations")
        lines.append("")
        for note in report.notes:
            lines.append(f"- {note}")
        lines.append("")
    return "\n".join(lines)


def write_report(report: ValidationReport, reports_dir: str | Path) -> ReportArtifacts:
    """Write ``<strategy>_<ts>.md`` + ``.json`` under ``reports_dir`` and return their paths."""
    out = Path(reports_dir)
    out.mkdir(parents=True, exist_ok=True)
    stem = f"{report.strategy_id}_{_slug_ts(report.generated_at)}"
    md_path = out / f"{stem}.md"
    json_path = out / f"{stem}.json"
    md_path.write_text(render_markdown(report), encoding="utf-8")
    json_path.write_text(report.model_dump_json(indent=2), encoding="utf-8")
    return ReportArtifacts(markdown=md_path, json=json_path)


# --------------------------------------------------------------------------- sweep report
def render_sweep_markdown(report: SweepReport) -> str:
    """Render a ``SweepReport`` — per-config stats + the cited trial count N (§6.4 step 1)."""
    lines: list[str] = []
    lines.append(f"# Sweep report — `{report.strategy_id}`")
    lines.append("")
    lines.append(f"_Generated {report.generated_at.isoformat()}_")
    lines.append("")
    lines.append(f"## Trial count N = {report.trial_count_n}")
    lines.append("")
    lines.append(
        "This is the §6.4-step-1 multiple-testing input: **every configuration evaluated** "
        "(grid cardinality), not the count of surfaced candidates. The ValidationReport cites it."
    )
    lines.append("")
    span = f"{report.data_start} → {report.data_end}" if report.data_start else "—"
    lines.append(
        f"- Product: {report.product}  ·  density: {report.grid_density}  ·  symbols: "
        f"{report.n_symbols}  ·  span: {span}"
    )
    lines.append(
        f"- Reference notional: ₹{report.reference_notional}  ·  modelled per-side fee: "
        f"{report.per_side_fee_pct:.4f}%"
    )
    lines.append(f"- Best params (by CLOSED-trade expectancy): {report.best_params}")
    # WO-M (2026-09-13): the one-line mechanics stamp. A sweep number is only meaningful with the
    # three settings it was produced under, and a pre-fix artifact carries no stamp at all — which
    # is how the two are told apart.
    lines.append(f"- **Mechanics**: `{report.mechanics or 'PRE-2026-09-13 (unstamped)'}`")
    lines.append("")

    # ---- R2 (2026-09-12): what the winning config's trades actually looked like --------------
    # The per-config table below reports one per-trade expectancy and no horizon at all, which is
    # how a run could be promoted against a floor spread over 120 sessions without anyone knowing
    # the median trade was held for 33 — and without the reader seeing that some of those "trades"
    # were still open. Both go here, for the config the run selected. Since WO-M the headline is
    # the CLOSED-trade figure and the all-trades one is what is reported beside it.
    best_stat = next(
        (s for s in report.stats if report.best_params is not None and s.params == report.best_params),
        None,
    )
    if best_stat is not None and best_stat.n_trades:
        unit = report.bar_unit
        lines.append("## Winning config — holding period + open-trade split (R2, reporting only)")
        lines.append("")
        lines.append(
            f"- Trades: {best_stat.n_trades}  ·  closed: {best_stat.n_closed}  ·  **still open at "
            f"the window edge: {best_stat.n_open}**"
        )
        lines.append(
            f"- Per-trade net return — CLOSED round trips only (the headline, and the statistic "
            f"this winner was ranked on, WO-M): {_pct(best_stat.expectancy_pct, 4)}  ·  ALL trades "
            f"incl. open marks (reporting only): {_pct(best_stat.expectancy_all_pct, 4)}"
        )
        win_c = "—" if best_stat.win_rate_closed is None else f"{best_stat.win_rate_closed:.1%}"
        win_a = "—" if best_stat.win_rate is None else f"{best_stat.win_rate:.1%}"
        lines.append(
            f"- Win rate — CLOSED round trips (same population as the headline above): {win_c}  ·  "
            f"ALL trades incl. open marks: {win_a}"
        )
        lines.append(
            f"- Holding {unit}s, all trades — mean {_num(best_stat.hold_bars_mean)} · median "
            f"{_num(best_stat.hold_bars_median)} · p90 {_num(best_stat.hold_bars_p90)}"
        )
        lines.append(
            f"- Holding {unit}s, closed only — mean {_num(best_stat.hold_bars_mean_closed)} · "
            f"median {_num(best_stat.hold_bars_median_closed)} · p90 "
            f"{_num(best_stat.hold_bars_p90_closed)}"
        )
        lines.append(
            "- An OPEN trade's duration is its AGE at the window edge, not a realized hold, and "
            "its return is unrealized mark-to-market with no exit leg paid. Every configuration's "
            "own figures are in the JSON artifact."
        )
        lines.append("")
    if report.population_is_survivorship_tainted_proxy:
        lines.append(
            "> **SURVIVORSHIP: the population is a tainted proxy** — a present-day symbol list "
            "applied backwards (no point-in-time index membership is stored anywhere in this "
            "platform). Every LEVEL below is biased HIGH by an unmeasurable amount; comparisons "
            "against other runs on the same list are unaffected."
        )
        lines.append("")

    lines.append("## Per-configuration stats")
    lines.append("")
    lines.append(
        "| params | trades | closed | win% (CLOSED) | win% (all) | expectancy (CLOSED) | "
        "expectancy (all) | total | Sharpe | maxDD |"
    )
    lines.append(
        "|:-------|-------:|-------:|--------------:|-----------:|--------------------:|"
        "-----------------:|------:|-------:|------:|"
    )
    for s in report.stats:
        params = ", ".join(f"{k}={s.params[k]}" for k in sorted(s.params))
        win = "—" if s.win_rate is None else f"{s.win_rate:.0%}"
        # WO-M follow-up: the closed-only win rate sits FIRST, beside the closed-only expectancy it
        # shares a population with — quoting "win rate X%, expectancy Y%" off this row must not mix
        # a mark-to-market hit rate with a realized per-trade mean.
        win_closed = "—" if s.win_rate_closed is None else f"{s.win_rate_closed:.0%}"
        sharpe = "—" if s.sharpe is None else f"{s.sharpe:.2f}"
        mdd = "—" if s.max_drawdown_pct is None else f"{s.max_drawdown_pct:.2f}%"
        lines.append(
            f"| {params} | {s.n_trades} | {s.n_closed} | {win_closed} | {win} | "
            f"{_pct(s.expectancy_pct, 4)} | {_pct(s.expectancy_all_pct, 4)} | "
            f"{_pct(s.total_return_pct)} | {sharpe} | {mdd} |"
        )
    lines.append("")
    # WO-M (iii): the ranked column is the CLOSED one; the all-trades column is the pre-fix
    # headline, kept visible so the size of that bias is readable per config instead of asserted.
    lines.append(
        "_`expectancy (CLOSED)` is the ranked/promotion statistic (mean net return over round trips "
        "that closed inside the window); `expectancy (all)` adds positions still open at the window "
        "edge at unrealized mark-to-market with no exit leg paid — the pre-2026-09-13 headline. "
        "`win% (CLOSED)` is over the SAME round trips as `expectancy (CLOSED)`; `win% (all)` counts "
        "every trade, open ones included. Neither win rate is a promotion input. A `—` in the "
        "CLOSED columns means the config closed no round trip, so it was not rankable._"
    )
    lines.append("")
    if report.notes:
        lines.append("## Notes / documented approximations")
        lines.append("")
        for note in report.notes:
            lines.append(f"- {note}")
        lines.append("")
    return "\n".join(lines)


def write_sweep_report(report: SweepReport, reports_dir: str | Path) -> ReportArtifacts:
    """Write ``<strategy>_sweep_<ts>.md`` + ``.json`` under ``reports_dir``."""
    out = Path(reports_dir)
    out.mkdir(parents=True, exist_ok=True)
    stem = f"{report.strategy_id}_sweep_{_slug_ts(report.generated_at)}"
    md_path = out / f"{stem}.md"
    json_path = out / f"{stem}.json"
    md_path.write_text(render_sweep_markdown(report), encoding="utf-8")
    json_path.write_text(report.model_dump_json(indent=2), encoding="utf-8")
    return ReportArtifacts(markdown=md_path, json=json_path)


__all__ = [
    "ReportArtifacts",
    "render_markdown",
    "render_sweep_markdown",
    "write_report",
    "write_sweep_report",
]
