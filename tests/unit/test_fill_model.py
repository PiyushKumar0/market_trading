"""PaperBroker fill-model config + math (WO-P3-2, 2026-09-10; plan 3.2.9 / 8.4 addendum).

The fill model is the conservatism contract of the paper tier (R9): every knob here exists so a
paper fill is never *better* than a live fill would plausibly have been. These tests pin:

* the time-of-day bucket boundaries (open 09:15-10:00 / mid 10:00-14:30 / close 14:30-15:30) --
  start-inclusive, end-exclusive, with out-of-session times clamped to the CONSERVATIVE edge
  bucket rather than silently getting k=0;
* half-spread resolution order (L1 depth -> per-symbol median -> tick-size fallback) and the
  never-zero invariant (a zero half-spread is what makes a restart/backfill day non-conservative,
  plan 3.2.9);
* the ``estimated`` flag that marks every non-L1 half-spread;
* slippage sign: it is always paid BY the taker (BUY fills higher, SELL fills lower);
* the loader with ``config/fill_model.yaml`` ABSENT (conservative defaults) and PRESENT (the schema
  the WO-P3-4 calibration job writes).
"""

from __future__ import annotations

from datetime import time
from decimal import Decimal
from pathlib import Path

import pytest

from engine.paper.fill_model import (
    DEFAULT_FALLBACK_TICKS,
    DEFAULT_LATENCY_MS,
    FILL_MODEL_FILENAME,
    FillModelConfig,
    load_fill_model,
)

TICK = Decimal("0.05")


@pytest.fixture
def cfg() -> FillModelConfig:
    return FillModelConfig()


# --------------------------------------------------------------------------- buckets
@pytest.mark.parametrize(
    ("at", "expected"),
    [
        (time(9, 15), "open"),          # start inclusive
        (time(9, 59, 59), "open"),
        (time(10, 0), "mid"),           # boundary belongs to the LATER bucket
        (time(14, 29, 59), "mid"),
        (time(14, 30), "close"),
        (time(15, 29, 59), "close"),
        (time(15, 30), "close"),        # at/after the last end -> clamped to close (k=1.0), never k=0
        (time(9, 0), "open"),           # pre-open -> clamped to open (k=1.0)
        (time(23, 59), "close"),
    ],
)
def test_bucket_boundaries(cfg: FillModelConfig, at: time, expected: str) -> None:
    assert cfg.bucket_for(at) == expected


def test_default_k_is_conservative_at_the_edges(cfg: FillModelConfig) -> None:
    # open/close are the thin, violent buckets: k=1.0. Mid is the calm one: k=0.5 (plan 3.2.9).
    assert cfg.k_for("open") == Decimal("1.0")
    assert cfg.k_for("mid") == Decimal("0.5")
    assert cfg.k_for("close") == Decimal("1.0")
    # An unknown bucket name must never silently become 0 -- it takes the most conservative k.
    assert cfg.k_for("nonsense") == Decimal("1.0")


# --------------------------------------------------------------------------- half spread
def test_half_spread_from_l1(cfg: FillModelConfig) -> None:
    hs, estimated = cfg.half_spread("TCS", Decimal("99.95"), Decimal("100.05"), Decimal("100"), TICK)
    assert hs == Decimal("0.05")
    assert estimated is False


def test_half_spread_falls_back_when_book_absent(cfg: FillModelConfig) -> None:
    hs, estimated = cfg.half_spread("TCS", None, None, Decimal("100"), TICK)
    assert hs == DEFAULT_FALLBACK_TICKS * TICK == Decimal("0.10")
    assert estimated is True


@pytest.mark.parametrize(
    ("bid", "ask"),
    [
        (Decimal("100.00"), Decimal("100.00")),   # locked book -> would give 0
        (Decimal("100.05"), Decimal("100.00")),   # crossed book -> would give a NEGATIVE half spread
        (None, Decimal("100.05")),                # one-sided
        (Decimal("99.95"), None),
    ],
)
def test_half_spread_never_zero_or_negative(cfg: FillModelConfig, bid, ask) -> None:
    hs, estimated = cfg.half_spread("TCS", bid, ask, Decimal("100"), TICK)
    assert hs > 0
    assert estimated is True


def test_half_spread_uses_per_symbol_median_before_tick_fallback() -> None:
    cfg = FillModelConfig.model_validate(
        {"symbols": {"TCS": {"median_half_spread_pct": 0.03}}}
    )
    hs, estimated = cfg.half_spread("TCS", None, None, Decimal("100"), TICK)
    assert hs == Decimal("0.03")          # 0.03% of 100
    assert estimated is True
    # A symbol with no calibrated median still gets the tick fallback, never 0.
    hs2, est2 = cfg.half_spread("INFY", None, None, Decimal("100"), TICK)
    assert (hs2, est2) == (Decimal("0.10"), True)


def test_half_spread_median_of_zero_is_rejected_and_falls_back() -> None:
    # A calibration row that came out 0 must NOT disarm the conservatism (plan 3.2.9 "never 0").
    cfg = FillModelConfig.model_validate({"symbols": {"TCS": {"median_half_spread_pct": 0.0}}})
    hs, estimated = cfg.half_spread("TCS", None, None, Decimal("100"), TICK)
    assert hs == Decimal("0.10")
    assert estimated is True


def test_calibrated_median_is_floored_at_half_a_tick_not_at_the_two_tick_fallback() -> None:
    """The calibrated branch's floor is HALF A TICK -- the physical floor, not the 2-tick default.

    Manager decision, 2026-09-10 (WO-P3-4 round 4). The measured per-symbol median tracks the real
    L1 book to the paisa, and on this account's corpus most books are ONE tick wide, so a calibrated
    median that lands under ``half_spread_fallback_ticks x tick_size`` is evidence, not an error --
    the 2-tick fallback is an ignorance default for symbols with no measurement at all. What a
    calibrated median may never do is resolve to something no book could quote: a market cannot be
    tighter than one tick, so half of one tick is the floor.

    The bite is a symbol calibrated at a HIGH price and evaluated at a LOW ltp (a split, a collapse,
    or simply a stale calibration): ``pct``-of-ltp then shrinks below a tick and the fill model would
    otherwise charge a sub-paisa half-spread.
    """
    cfg = FillModelConfig.model_validate({"symbols": {"TCS": {"median_half_spread_pct": 0.001}}})
    # 0.001% of 100 = 0.001, which is a fiftieth of a tick -- no real book quotes that.
    hs, estimated = cfg.half_spread("TCS", None, None, Decimal("100"), TICK)
    assert hs == TICK / Decimal(2) == Decimal("0.025")
    assert estimated is True
    # Above the floor the measurement is used verbatim -- the floor clamps, it does not replace.
    hs2, est2 = cfg.half_spread("TCS", None, None, Decimal("10000"), TICK)
    assert (hs2, est2) == (Decimal("0.1"), True)          # 0.001% of 10,000


# --------------------------------------------------------------------------- slippage
def test_slippage_is_signed_against_the_taker(cfg: FillModelConfig) -> None:
    hs = Decimal("0.05")
    sigma = Decimal("0.20")
    buy = cfg.slippage("TCS", "BUY", "mid", sigma, hs)
    sell = cfg.slippage("TCS", "SELL", "mid", sigma, hs)
    # mid k = 0.5 -> 0.05 + 0.5*0.20 = 0.15
    assert buy == Decimal("0.15")
    assert sell == Decimal("-0.15")
    assert buy == -sell


def test_slippage_bucket_k_scales_sigma(cfg: FillModelConfig) -> None:
    hs = Decimal("0.05")
    sigma = Decimal("0.20")
    assert cfg.slippage("TCS", "BUY", "open", sigma, hs) == Decimal("0.25")   # k=1.0
    assert cfg.slippage("TCS", "BUY", "close", sigma, hs) == Decimal("0.25")  # k=1.0
    assert cfg.slippage("TCS", "BUY", "mid", sigma, hs) == Decimal("0.15")    # k=0.5


def test_slippage_never_favours_the_taker(cfg: FillModelConfig) -> None:
    # Even with a zero sigma the half-spread is still paid -- slippage is never 0 (never free).
    assert cfg.slippage("TCS", "BUY", "mid", Decimal("0"), Decimal("0.05")) == Decimal("0.05")
    assert cfg.slippage("TCS", "SELL", "mid", Decimal("0"), Decimal("0.05")) == Decimal("-0.05")


def test_slippage_rejects_an_unknown_side(cfg: FillModelConfig) -> None:
    with pytest.raises(ValueError, match="transaction_type"):
        cfg.slippage("TCS", "LONG", "mid", Decimal("0.2"), Decimal("0.05"))


# --------------------------------------------------------------------------- loader
def test_load_without_the_yaml_returns_conservative_defaults(tmp_path) -> None:
    cfg = load_fill_model(tmp_path / "fill_model.yaml")
    assert cfg.latency_ms == DEFAULT_LATENCY_MS == 700
    assert cfg.half_spread_fallback_ticks == DEFAULT_FALLBACK_TICKS == 2
    assert cfg.symbols == {}
    assert cfg.k_for("open") == Decimal("1.0")
    assert cfg.k_for("mid") == Decimal("0.5")
    assert cfg.k_for("close") == Decimal("1.0")
    assert cfg.bucket_for(time(9, 30)) == "open"


def test_load_reads_the_calibrated_yaml(tmp_path) -> None:
    path = tmp_path / "fill_model.yaml"
    path.write_text(
        "\n".join(
            [
                "version: 1",
                "generated_at: '2026-09-10T18:00:00+05:30'",
                "latency_ms: 900",
                "buckets:",
                "  open: {start: '09:15', end: '10:00', k: 1.4}",
                "  mid: {start: '10:00', end: '14:30', k: 0.7}",
                "  close: {start: '14:30', end: '15:30', k: 1.2}",
                "half_spread_fallback_ticks: 3",
                "symbols:",
                "  TCS: {median_half_spread_pct: 0.02}",
                "  INFY: {median_half_spread_pct: 0.04}",
            ]
        ),
        encoding="utf-8",
    )
    cfg = load_fill_model(path)
    assert cfg.version == 1
    assert cfg.generated_at == "2026-09-10T18:00:00+05:30"
    assert cfg.latency_ms == 900
    assert cfg.half_spread_fallback_ticks == 3
    assert cfg.k_for("mid") == Decimal("0.7")
    assert cfg.bucket_for(time(10, 0)) == "mid"
    # Floats in YAML must land as exact Decimals, not binary noise (money/price math is Decimal).
    assert cfg.k_for("open") == Decimal("1.4")
    hs, estimated = cfg.half_spread("TCS", None, None, Decimal("1000"), TICK)
    assert hs == Decimal("0.2")           # 0.02% of 1000
    assert estimated is True
    # The uncalibrated symbol takes the file's fallback ticks (3), not the default 2.
    hs2, _ = cfg.half_spread("WIPRO", None, None, Decimal("1000"), TICK)
    assert hs2 == Decimal("0.15")


def test_load_defaults_missing_blocks_within_a_partial_yaml(tmp_path) -> None:
    path = tmp_path / "fill_model.yaml"
    path.write_text("version: 1\nlatency_ms: 700\n", encoding="utf-8")
    cfg = load_fill_model(path)
    assert cfg.k_for("open") == Decimal("1.0")
    assert cfg.half_spread_fallback_ticks == 2
    assert cfg.symbols == {}


def test_latency_timedelta(cfg: FillModelConfig) -> None:
    assert cfg.latency().total_seconds() == pytest.approx(0.7)


# --------------------------------------------------------------------------- loader default path
# ``load_fill_model()`` with NO argument is how the engine actually calls it: it resolves
# ``config_dir() / fill_model.yaml``. That branch is the one that ships, so both of its outcomes --
# the file absent (the state of the repo until WO-P3-4 runs) and present -- are pinned here.
def test_default_path_resolves_config_dir_and_defaults_when_the_file_is_absent(
    tmp_path, monkeypatch
) -> None:
    monkeypatch.setattr("engine.core.config.config_dir", lambda: tmp_path)
    assert not (tmp_path / FILL_MODEL_FILENAME).exists()
    cfg = load_fill_model()
    assert cfg.latency_ms == DEFAULT_LATENCY_MS
    assert cfg.half_spread_fallback_ticks == DEFAULT_FALLBACK_TICKS
    assert cfg.symbols == {}
    assert cfg.k_for("mid") == Decimal("0.5")


def test_default_path_reads_the_file_when_present(tmp_path, monkeypatch) -> None:
    (tmp_path / FILL_MODEL_FILENAME).write_text(
        "latency_ms: 1234\nsymbols:\n  TCS: {median_half_spread_pct: 0.05}\n", encoding="utf-8"
    )
    monkeypatch.setattr("engine.core.config.config_dir", lambda: tmp_path)
    cfg = load_fill_model()
    assert cfg.latency_ms == 1234
    hs, estimated = cfg.half_spread("TCS", None, None, Decimal("200"), TICK)
    assert (hs, estimated) == (Decimal("0.1"), True)      # 0.05% of 200


def test_a_non_mapping_yaml_is_refused(tmp_path) -> None:
    path = tmp_path / FILL_MODEL_FILENAME
    path.write_text("- 1\n- 2\n", encoding="utf-8")
    with pytest.raises(ValueError, match="YAML mapping"):
        load_fill_model(path)


# --------------------------------------------------------------------- shipped-config cross-check
def test_the_shipped_calibration_speaks_this_module_s_units() -> None:
    """The SHIPPED ``config/fill_model.yaml`` must mean by ``median_half_spread_pct`` what this
    module reads, for EVERY calibrated symbol (WO-P3-4 round 4, 2026-09-10).

    The producer (``scripts/calibrate_fill_model.py``) and this consumer are separate programs with
    separate loaders against one hand-written contract, and round 2 shipped them disagreeing by a
    factor of 100: the calibrator wrote a FRACTION of mid while ``half_spread()`` below divides by
    100. Every unit test on either side passed - each was self-consistent - and the paper broker
    would simply have charged 1/100th of the measured spread on every non-L1 tick, the exact
    silent-optimism failure R9 exists to prevent. Only a test that loads the real artefact through
    the real loader can catch that, so this one does.

    Round 4 replaces what this used to assert. It sampled three symbols and required each to resolve
    at or above the 2-tick fallback -- a property the artefact does NOT have and should not have:
    the 2-tick fallback is the ignorance default for a symbol with no measurement, while a large
    minority of the shipped symbols were measured with books ONE tick wide and therefore sit below
    it. Asserting the fallback as a floor would have forced the next calibration to inflate real
    measurements to keep this green. What IS pinned here is the contract the consumer implements
    over every calibrated row: the resolved half-spread is estimated, strictly positive, never
    tighter than half a tick (the physical floor - no book quotes inside the tick), and exactly
    ``max(ltp x pct / 100, tick / 2)``.
    """
    shipped = Path(__file__).resolve().parents[2] / "config" / FILL_MODEL_FILENAME
    if not shipped.exists():
        pytest.skip(
            f"{shipped} is absent - it is written by scripts/calibrate_fill_model.py (WO-P3-4); "
            "run the calibration to exercise this cross-contract check"
        )

    cfg = load_fill_model(shipped)
    assert cfg.symbols, f"expected calibrated symbols in {shipped}, got none"

    ltp, tick = Decimal("1000"), Decimal("0.05")
    floor = tick / Decimal(2)
    pcts: list[Decimal] = []
    for symbol, sym_cfg in cfg.symbols.items():
        pct = sym_cfg.median_half_spread_pct
        hs, estimated = cfg.half_spread(symbol, None, None, ltp, tick)
        assert estimated is True, symbol
        assert hs > 0, (symbol, pct, hs)
        assert hs >= floor, (symbol, pct, hs, floor)
        assert hs == max(ltp * pct / Decimal(100), floor), (symbol, pct, hs)
        pcts.append(pct)

    # UNIT GUARD (this is the 100x check, and it belongs on the population, not on one row): the
    # field is a PERCENT of mid, so a market-wide median half-spread must sit inside a plausibility
    # band of [0.001, 1.0] percent - 0.1 bp to 100 bp. A calibrator that reverted to emitting a
    # FRACTION drives the median to ~1e-4 and breaks the lower edge; one that multiplied by 100
    # twice drives it to ~1.3 and breaks the upper edge. A per-symbol assertion cannot say this,
    # because either error keeps every row internally consistent.
    median_pct = sorted(pcts)[len(pcts) // 2]
    assert Decimal("0.001") <= median_pct <= Decimal("1.0"), (median_pct, len(pcts))
