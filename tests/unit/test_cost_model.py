"""CostModel (§3.2.5, C1-C4): the C3 worked examples asserted TO THE PAISA, parameterized from
``config/costs.yaml``; the DP multi-scrip case (C4); min_viable_qty shrink/reject boundaries; banded
tick rounding via ``InstrumentStore.round_to_tick`` (A10); and the zero-hardcoded-rates guarantee
(rates flow from the yaml, §9.1)."""

from __future__ import annotations

import copy
from decimal import Decimal

import pytest
import yaml

from engine.broker.instruments import Instrument, InstrumentStore
from engine.core.config import config_dir
from engine.strategy.cost_model import CostModel, CostRates, load_cost_rates


@pytest.fixture(scope="module")
def raw_yaml() -> dict:
    return yaml.safe_load((config_dir() / "costs.yaml").read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def rates() -> CostRates:
    return load_cost_rates()


@pytest.fixture(scope="module")
def model(rates) -> CostModel:
    return CostModel(rates)


@pytest.fixture
def instruments(clock) -> InstrumentStore:
    store = InstrumentStore(clock)
    store.seed([
        Instrument(tradingsymbol="ABC", instrument_token=1, exchange="NSE", segment="NSE",
                   tick_size=Decimal("0.05"), lot_size=1, instrument_type="EQ"),
        Instrument(tradingsymbol="XYZ", instrument_token=2, exchange="NSE", segment="NSE",
                   tick_size=Decimal("0.10"), lot_size=1, instrument_type="EQ"),  # banded tick (A10)
    ])
    return store


@pytest.fixture
def sizing_model(rates, instruments) -> CostModel:
    return CostModel(rates, instruments)   # edge_multiple_min default 2.0 (§7.1 min_viable_size)


# ------------------------------------------------------------------ C3 worked examples, to the paisa
# Hand-derived from the 2026-06-11 config/costs.yaml rates (contract-note rounding: each component
# exact-then-paisa-quantized, GST on the RAW brokerage+txn+SEBI bases, total = sum of quantized
# components). If a re-scrape changes costs.yaml these literals MUST be re-derived alongside it.
WORKED_EXAMPLES = {
    # (product, notional, n_scrips): {component: paisa-exact Decimal}
    ("MIS", Decimal("20000"), 1): {           # intraday Rs 20k round trip ~= Rs 21 (C3)
        "brokerage": Decimal("12.00"),        # 2 x min(20000 x 0.03%, 20)
        "stt": Decimal("5.00"),               # 20000 x 0.025% sell-side only
        "txn": Decimal("1.23"),               # 2 x 20000 x 0.00307% = 1.228
        "sebi": Decimal("0.04"),              # 2 x (20000/1cr) x 10
        "stamp": Decimal("0.60"),             # 20000 x 0.003% buy side
        "gst": Decimal("2.39"),               # 18% x (12 + 1.228 + 0.04) = 2.38824
        "dp": Decimal("0.00"),                # MIS: no delivery, no DP
    },
    ("CNC", Decimal("20000"), 1): {           # delivery Rs 20k ~= Rs 60 (STT + DP dominated, C3)
        "brokerage": Decimal("0.00"),
        "stt": Decimal("40.00"),              # 0.1% buy + 0.1% sell
        "txn": Decimal("1.23"),
        "sebi": Decimal("0.04"),
        "stamp": Decimal("3.00"),             # 0.015% buy side
        "gst": Decimal("0.23"),               # 18% x (0 + 1.228 + 0.04)
        "dp": Decimal("15.34"),               # per scrip per SELL day
    },
    ("MIS", Decimal("100000"), 1): {          # 5x-leveraged MIS Rs 1L notional ~= Rs 83 (C3)
        "brokerage": Decimal("40.00"),        # per-order cap binds: 2 x min(30, 20)
        "stt": Decimal("25.00"),
        "txn": Decimal("6.14"),
        "sebi": Decimal("0.20"),
        "stamp": Decimal("3.00"),
        "gst": Decimal("8.34"),               # 18% x (40 + 6.14 + 0.20)
        "dp": Decimal("0.00"),
    },
}
WORKED_TOTALS = {
    ("MIS", Decimal("20000"), 1): (Decimal("21.26"), Decimal("0.106300")),
    ("CNC", Decimal("20000"), 1): (Decimal("59.84"), Decimal("0.299200")),
    ("MIS", Decimal("100000"), 1): (Decimal("82.68"), Decimal("0.082680")),
}


@pytest.mark.parametrize("key", sorted(WORKED_EXAMPLES, key=str))
def test_c3_worked_examples_to_the_paisa(model, key):
    product, notional, n = key
    bd = model.round_trip(notional, product, n_scrips_sell_day=n)
    assert bd.components == WORKED_EXAMPLES[key]
    total, breakeven = WORKED_TOTALS[key]
    assert bd.total_cost == total
    assert bd.breakeven_pct == breakeven
    # Contract-note invariant: the breakdown always adds up.
    assert sum(bd.components.values(), Decimal("0")) == bd.total_cost


def test_c3_reference_roundtrips_parameterized_from_yaml(model, raw_yaml):
    """The yaml's own reference_roundtrips block (C3: ~21 / ~60 / ~83) reproduces from the model."""
    refs = raw_yaml["reference_roundtrips"]
    assert len(refs) == 3
    for ref in refs:
        bd = model.round_trip(Decimal(ref["notional_inr"]), ref["product"])
        assert bd.total_cost.quantize(Decimal("1")) == Decimal(ref["expected_total_inr"]), ref["label"]
        assert abs(bd.breakeven_pct - Decimal(str(ref["breakeven_pct"]))) <= Decimal("0.001"), ref["label"]


def test_breakeven_pct_matches_round_trip(model):
    for product, notional in (("MIS", Decimal("20000")), ("CNC", Decimal("50000"))):
        assert model.breakeven_pct(notional, product) == model.round_trip(notional, product).breakeven_pct


def test_breakeven_non_increasing_in_notional(model):
    """Flat components amortize — the monotonicity min_viable_qty's binary search relies on."""
    for product in ("MIS", "CNC"):
        b = [model.breakeven_pct(Decimal(n), product) for n in (5_000, 20_000, 66_667, 200_000, 1_000_000)]
        assert all(x >= y for x, y in zip(b, b[1:], strict=False))


# ------------------------------------------------------------------ C4 multi-scrip DP + brokerage cap
def test_dp_charged_per_scrip_per_sell_day(model):
    """C4: splitting a small delivery book across scrips multiplies the flat DP charge."""
    one = model.round_trip(Decimal("20000"), "CNC", n_scrips_sell_day=1)
    two = model.round_trip(Decimal("20000"), "CNC", n_scrips_sell_day=2)
    assert one.components["dp"] == Decimal("15.34")
    assert two.components["dp"] == Decimal("30.68")
    # Proportional charges are unaffected by the split; only DP moves for zero-brokerage CNC.
    for k in ("brokerage", "stt", "txn", "sebi", "stamp", "gst"):
        assert one.components[k] == two.components[k]
    assert two.total_cost == Decimal("75.18")
    assert two.total_cost - one.total_cost == Decimal("15.34")


def test_mis_brokerage_cap_applies_per_order_per_slice(model):
    """The Rs 20/order intraday cap binds per SLICE order, so splitting raises brokerage."""
    whole = model.round_trip(Decimal("200000"), "MIS", n_scrips_sell_day=1)
    split = model.round_trip(Decimal("200000"), "MIS", n_scrips_sell_day=2)
    assert whole.components["brokerage"] == Decimal("40.00")   # 2 orders x capped 20
    assert split.components["brokerage"] == Decimal("80.00")   # 4 orders x capped 20 (slice 1L -> 30 -> 20)
    assert whole.components["dp"] == split.components["dp"] == Decimal("0.00")


# ------------------------------------------------------------------ min_viable_qty (§7.1, R1/C3)
def test_min_viable_qty_small_edge_needs_size_and_boundary_is_exact(sizing_model, model):
    """CNC with a 0.5% edge: the flat DP charge forces size; result is the EXACT boundary qty."""
    qty = sizing_model.min_viable_qty("ABC", Decimal("100.00"), Decimal("99.00"), Decimal("100.50"), "CNC")
    assert qty > 1
    edge = sizing_model.expected_edge_pct(Decimal("100.00"), Decimal("99.00"), Decimal("100.50"))
    required = sizing_model.edge_multiple_min
    assert edge >= required * model.breakeven_pct(Decimal(qty) * Decimal("100.00"), "CNC")
    assert edge < required * model.breakeven_pct(Decimal(qty - 1) * Decimal("100.00"), "CNC")
    # Sanity band: DP 15.34 must shrink below ~0.0275% of notional -> around Rs 51-62k.
    assert 45_000 < qty * 100 < 70_000


def test_min_viable_qty_generous_edge_is_qty_one(sizing_model):
    """A 1% MIS edge clears 2x the ~0.106% breakeven at any size (shrink never rejects here)."""
    assert sizing_model.min_viable_qty(
        "ABC", Decimal("100.00"), Decimal("99.00"), Decimal("101.00"), "MIS") == 1


def test_min_viable_qty_rejects_below_proportional_floor(sizing_model):
    """CNC proportional costs alone are ~0.2225% -> a 0.4% edge < 2x floor is unviable at ANY size
    (the §7.1 shrink-then-recheck path must end in reject, R1/C3)."""
    assert sizing_model.min_viable_qty(
        "ABC", Decimal("100.00"), Decimal("99.00"), Decimal("100.40"), "CNC") == 0


def test_min_viable_qty_wrong_side_target_rejects(sizing_model):
    # Long setup (stop below entry) with target BELOW entry -> non-positive edge -> 0.
    assert sizing_model.min_viable_qty(
        "ABC", Decimal("100.00"), Decimal("99.00"), Decimal("99.50"), "MIS") == 0
    # Short setup mirrored: stop above entry, target above entry -> 0.
    assert sizing_model.min_viable_qty(
        "ABC", Decimal("100.00"), Decimal("101.00"), Decimal("100.50"), "MIS") == 0


def test_min_viable_qty_short_side_mirrors_long(sizing_model):
    long_q = sizing_model.min_viable_qty("ABC", Decimal("100.00"), Decimal("99.00"), Decimal("100.50"), "CNC")
    short_q = sizing_model.min_viable_qty("ABC", Decimal("100.00"), Decimal("101.00"), Decimal("99.50"), "CNC")
    assert long_q == short_q


def test_min_viable_qty_incoherent_levels_raise(sizing_model):
    with pytest.raises(ValueError):
        sizing_model.min_viable_qty("ABC", Decimal("100.00"), Decimal("100.00"), Decimal("101.00"), "MIS")


def test_min_viable_qty_requires_instrument_store(model):
    with pytest.raises(RuntimeError):
        model.min_viable_qty("ABC", Decimal("100.00"), Decimal("99.00"), Decimal("101.00"), "MIS")


# ------------------------------------------------------------------ banded tick rounding (A10)
def test_min_viable_qty_snaps_levels_to_the_instrument_tick(sizing_model):
    """Off-grid levels are snapped via InstrumentStore.round_to_tick BEFORE the edge math — the
    result equals the on-grid call exactly (never inline tick assumptions)."""
    on_grid = sizing_model.min_viable_qty(
        "ABC", Decimal("100.00"), Decimal("99.00"), Decimal("100.50"), "CNC")
    off_grid = sizing_model.min_viable_qty(
        "ABC", Decimal("100.02"), Decimal("99.01"), Decimal("100.52"), "CNC")   # tick 0.05 grid
    assert on_grid == off_grid


def test_banded_tick_instrument_uses_its_own_tick(sizing_model, instruments):
    """XYZ trades on a Rs 0.10 band: 100.52 snaps to 100.50 there, but to 100.50 vs 100.55
    differently than ABC's 0.05 band would (A10 — per-instrument, price-banded)."""
    assert instruments.round_to_tick("XYZ", Decimal("100.54")) == Decimal("100.50")
    assert instruments.round_to_tick("ABC", Decimal("100.54")) == Decimal("100.55")
    q_xyz = sizing_model.min_viable_qty("XYZ", Decimal("100.04"), Decimal("99.04"), Decimal("100.54"), "CNC")
    q_ref = sizing_model.min_viable_qty("XYZ", Decimal("100.00"), Decimal("99.00"), Decimal("100.50"), "CNC")
    assert q_xyz == q_ref


# ------------------------------------------------------------------ zero hardcoded rates (C2)
def test_rates_flow_from_yaml_not_code(raw_yaml):
    """Doubling the intraday sell STT in a modified rates dict shifts stt (and total) EXACTLY by
    the delta — proof every rate is read from config/costs.yaml, none hardcoded (C2)."""
    base = CostModel(CostRates.from_dict(raw_yaml)).round_trip(Decimal("20000"), "MIS")
    mutated = copy.deepcopy(raw_yaml)
    mutated["stt"]["intraday"]["sell_pct"] = 2 * float(raw_yaml["stt"]["intraday"]["sell_pct"])
    bumped = CostModel(CostRates.from_dict(mutated)).round_trip(Decimal("20000"), "MIS")
    assert bumped.components["stt"] == 2 * base.components["stt"]
    assert bumped.total_cost - base.total_cost == base.components["stt"]
    for k in ("brokerage", "txn", "sebi", "stamp", "gst", "dp"):   # STT is not a GST base
        assert bumped.components[k] == base.components[k]


def test_unknown_gst_base_is_a_hard_error(raw_yaml):
    mutated = copy.deepcopy(raw_yaml)
    mutated["gst"]["applies_to"] = list(raw_yaml["gst"]["applies_to"]) + ["mystery_levy"]
    with pytest.raises(ValueError, match="mystery_levy"):
        CostRates.from_dict(mutated)


# ------------------------------------------------------------------ validation + edge helpers
def test_money_convention_rejects_floats(model):
    with pytest.raises(TypeError):
        model.round_trip(20000.0, "MIS")            # float notional is never money (§3.2)


def test_product_and_n_scrips_validation(model):
    with pytest.raises(ValueError):
        model.round_trip(Decimal("20000"), "NRML")
    with pytest.raises(ValueError):
        model.round_trip(Decimal("20000"), "MIS", n_scrips_sell_day=0)
    with pytest.raises(ValueError):
        model.round_trip(Decimal("-1"), "MIS")


def test_with_edge_populates_edge_multiple(model):
    bd = model.round_trip(Decimal("20000"), "MIS")
    assert bd.expected_edge_pct == 0 and bd.edge_multiple == 0     # unevaluated on plain round_trip
    enriched = model.with_edge(bd, Decimal("0.5"))
    assert enriched.expected_edge_pct == Decimal("0.5")
    assert enriched.edge_multiple == (Decimal("0.5") / bd.breakeven_pct).quantize(Decimal("0.0001"))
