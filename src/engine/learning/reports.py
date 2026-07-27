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


def _slug_ts(generated_at) -> str:
    return generated_at.strftime("%Y%m%dT%H%M%S")


# --------------------------------------------------------------------------- validation report
def render_markdown(report: ValidationReport) -> str:
    """Render a ``ValidationReport`` to markdown with the promotion verdict + negatives up top (C9)."""
    lines: list[str] = []
    lines.append(f"# Validation report — `{report.strategy_id}`")
    lines.append("")
    lines.append(f"_Generated {report.generated_at.isoformat()} · param_set `{report.param_set_id}`_")
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
    lines.append(f"- Best params (by expectancy): {report.best_params}")
    lines.append("")
    lines.append("## Per-configuration stats")
    lines.append("")
    lines.append("| params | trades | win% | expectancy | total | Sharpe | maxDD |")
    lines.append("|:-------|-------:|-----:|-----------:|------:|-------:|------:|")
    for s in report.stats:
        params = ", ".join(f"{k}={s.params[k]}" for k in sorted(s.params))
        win = "—" if s.win_rate is None else f"{s.win_rate:.0%}"
        sharpe = "—" if s.sharpe is None else f"{s.sharpe:.2f}"
        mdd = "—" if s.max_drawdown_pct is None else f"{s.max_drawdown_pct:.2f}%"
        lines.append(
            f"| {params} | {s.n_trades} | {win} | {_pct(s.expectancy_pct, 4)} | "
            f"{_pct(s.total_return_pct)} | {sharpe} | {mdd} |"
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
