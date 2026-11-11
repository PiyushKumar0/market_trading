import importlib.util
import sys
from decimal import Decimal
from pathlib import Path

import pytest

from engine.core.config import config_dir, load_yaml
from engine.risk.limits import LimitTable
from engine.strategy import hi52_edge as he
from engine.strategy.scanners import hi52


def _table(base):
    raw = load_yaml(config_dir() / "limits.yaml")
    raw["capital_base_inr"] = base
    return LimitTable.model_validate(raw)


def test_the_derivation_reproduces_the_registered_1_53_at_the_registration_base():
    assert he.derived_edge_pct(_table(40000)) == Decimal("1.53")


def test_a_smaller_base_derives_a_smaller_edge():
    assert he.derived_edge_pct(_table(20000)) < Decimal("1.53") < he.derived_edge_pct(_table(80000))


def test_the_live_map_caps_hi52_at_its_derivation_and_never_raises_it():
    reg = {hi52.STRATEGY_ID: Decimal("1.53"), "ins": Decimal("1.58")}
    cut = he.StrategyEdges(reg, table_fn=lambda: _table(20000))
    raise_ = he.StrategyEdges(reg, table_fn=lambda: _table(80000))
    assert cut[hi52.STRATEGY_ID] == he.derived_edge_pct(_table(20000))
    assert raise_[hi52.STRATEGY_ID] == Decimal("1.53")
    assert cut["ins"] == Decimal("1.58") and dict(cut).keys() == reg.keys()


def test_edge_for_table_applies_the_same_rule_at_a_given_table():
    reg = {hi52.STRATEGY_ID: Decimal("1.53"), "ins": Decimal("1.58")}
    edges = he.StrategyEdges(reg, table_fn=lambda: _table(40000))
    assert edges.edge_for_table(hi52.STRATEGY_ID, _table(20000)) == he.derived_edge_pct(_table(20000))
    assert edges.edge_for_table(hi52.STRATEGY_ID, _table(80000)) == Decimal("1.53")
    assert edges.edge_for_table("ins", _table(20000)) == Decimal("1.58")
    assert edges[hi52.STRATEGY_ID] == Decimal("1.53")          # the live read is untouched
    unwired = he.StrategyEdges(reg)                             # no table_fn: the given table still caps
    assert unwired.edge_for_table(hi52.STRATEGY_ID, _table(20000)) == he.derived_edge_pct(_table(20000))


def test_edge_for_table_keeps_the_registered_edge_when_the_derivation_fails():
    bad = _table(40000).model_copy(update={"capital_base_inr": Decimal(0)})
    edges = he.StrategyEdges({hi52.STRATEGY_ID: Decimal("1.53")})
    assert edges.edge_for_table(hi52.STRATEGY_ID, bad) == Decimal("1.53")


def test_a_failing_derivation_keeps_the_registered_edge():
    def boom():
        raise RuntimeError("limits unverified")

    edges = he.StrategyEdges({hi52.STRATEGY_ID: Decimal("1.53")}, table_fn=boom)
    assert edges[hi52.STRATEGY_ID] == Decimal("1.53")


def test_the_registered_edge_is_this_script_s_derivation(tmp_path):
    """The edge hi52 was registered at and the kill criterion's cost floor come from one derivation
    (`registered_edge_pct()`), and a change to limits.yaml, hi52's stop or the cost tables fails here. hi52 is
    demoted, so the shipped settings.yaml registers no edge."""
    import re

    from engine.core.config import load_settings

    script = Path(__file__).resolve().parents[2] / "scripts" / "hi52_forward_verdict.py"
    spec = importlib.util.spec_from_file_location("mt_hi52_forward_verdict", script)
    fv = importlib.util.module_from_spec(spec)
    sys.modules["mt_hi52_forward_verdict"] = fv
    spec.loader.exec_module(fv)
    limits = config_dir() / "limits.yaml"

    # Pinned at the registration inputs (base 40,000), not the live file, so a capital change does not
    # break this pin (the runtime cap in StrategyEdges follows live limits).
    pinned = tmp_path / "limits.yaml"
    pinned.write_text(
        re.sub(r"^capital_base_inr:.*$", "capital_base_inr: 40000",
               limits.read_text(encoding="utf-8"), flags=re.M),
        encoding="utf-8",
    )
    derived = fv.registered_edge_pct(limits_path=pinned)
    assert derived == Decimal("1.53")                                   # 2.0x gap mult
    assert load_settings().hi52.expected_edge_pct is None               # demoted: the shipped key is absent
    # …and it IS gross-minus-floor, rounded DOWN: rounding an edge UP is the one direction that can
    # buy a candidate a cost-gate pass it did not earn.
    floor = fv.cost_floor_pct(Decimal("6000"))                          # Rs 36,000 x 2% / (2.0 x 6%)
    assert floor == pytest.approx(Decimal("0.498167"), abs=Decimal("1e-6"))
    assert fv.MEASURED_T20_MEDIAN_GROSS_PCT - floor > derived          # 1.5354 -> 1.53
    assert fv.MEASURED_T20_MEDIAN_GROSS_PCT - floor - derived < Decimal("0.01")
