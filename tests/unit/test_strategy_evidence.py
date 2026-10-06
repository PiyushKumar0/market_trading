"""Strategy evidence registry and hi52 forward progress."""

from __future__ import annotations

import importlib.util
import json
import sys
from datetime import date, timedelta
from pathlib import Path

from engine.learning.evidence import evidence_line, load_evidence
from engine.learning.hi52_forward import (
    PROMOTION_DATE,
    Series,
    Signal,
    forward_line,
    forward_progress,
)

_REGISTRY = Path(__file__).resolve().parents[2] / "config" / "strategy_evidence.yaml"


def _write_hi52_report(reports: Path) -> None:
    cell = {"n": 1794, "hit_rate_net": 0.5819, "median_net": 1.7144}
    doc = {"constructs": {"discrete_fresh_cross": {
        "cells": {"all": {"horizons": {"20": cell}}},
        "cpcv": {"20": {"fold_pass_fraction": 0.8667}},
    }}}
    (reports / "backtest_hi52_v2_2026-09-09.json").write_text(json.dumps(doc), encoding="utf-8")


def test_shipped_registry_loads_with_every_strategy():
    assert set(load_evidence(_REGISTRY).strategies) == {"hi52", "brk20", "ins"}


def test_line_renders_exactly(tmp_path):
    _write_hi52_report(tmp_path)
    assert evidence_line("hi52", tmp_path, _REGISTRY) == (
        "backtest (fixed T+20 hold, no stop): n 1794, hit 58.2%, median net 1.71%, "
        "CPCV 86.7% fold pass — shipped stop untested (study C1)"
    )


def test_missing_key_drops_only_its_part(tmp_path):
    (tmp_path / "backtest_brk20_2026-09-12.json").write_text(
        json.dumps({"variants": {"V2_limit_at_H20_N5": {"cells": {"all": {"horizons": {
            "20": {"n": 4898, "median_net": 0.9168}}}}}}}), encoding="utf-8")
    assert evidence_line("brk20", tmp_path, _REGISTRY) == (
        "backtest (limit fill within 5 sessions, fixed T+20 hold, no stop): "
        "n 4898, median net 0.92%"
    )


def test_missing_strategy_or_report_is_none(tmp_path):
    assert evidence_line("nope", tmp_path, _REGISTRY) is None
    assert evidence_line("hi52", tmp_path, _REGISTRY) is None


def test_forward_progress_matches_the_script():
    spec = importlib.util.spec_from_file_location(
        "mt_hi52_forward_verdict", Path(__file__).resolve().parents[2] / "scripts" / "hi52_forward_verdict.py"
    )
    fv = importlib.util.module_from_spec(spec)
    sys.modules["mt_hi52_forward_verdict"] = fv
    spec.loader.exec_module(fv)

    def series(symbol: str, n: int) -> Series:
        days = [PROMOTION_DATE + timedelta(days=i) for i in range(n)]
        return Series(symbol, days, [100.0] * n, [101.0] * n)

    signals = [Signal(PROMOTION_DATE, s) for s in ("OLD", "YOUNG", "NOBARS")]
    bars = {"OLD": series("OLD", 25), "YOUNG": series("YOUNG", 10)}
    doc = fv.build_report(
        signals, bars, cost_pct=0.0, as_of=date(2026, 12, 31), start=PROMOTION_DATE
    )
    k, m = forward_progress(signals, bars)
    assert (k, m) == (doc["horizons"]["20"]["n"], doc["population"]["signals_journalled"]) == (1, 3)
    assert forward_line(k, m) == "forward test: 1 of 20 matured (3 signals since 2026-09-12)"
