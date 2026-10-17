from decimal import Decimal

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
