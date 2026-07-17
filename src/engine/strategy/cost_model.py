"""Round-trip transaction-cost model (§3.2.5, C1–C4) — the single source of truth for trade friction.

Used IDENTICALLY by backtests, the Tier-2 gate (§7.1 ``min_viable_size``), and recommendations (C3).
Every rate comes from ``config/costs.yaml`` (C2) — there are ZERO hardcoded rates in this module; the
yaml is re-scraped before each release (``scripts/rescrape_costs.py``) and the §9.1 unit tests assert
the C3 worked examples to the paisa against it:

* intraday ₹20,000 round trip ≈ ₹21 (breakeven ~0.106%)
* delivery ₹20,000 ≈ ₹60 (dominated by ₹40 STT + ₹15.34 DP)
* 5× MIS ₹1,00,000 notional ≈ ₹83 (~0.083% of notional = ~0.41% of capital per turn)

Rounding contract (the "to the paisa" convention): each charge component is computed exactly in
``Decimal`` from the yaml rates, GST is computed on the RAW (unquantized) bases named by
``gst.applies_to``, then every component is quantized to the paisa (``ROUND_HALF_UP``) and
``total_cost`` is the sum of the QUANTIZED components — contract-note style, so the breakdown always
adds up. ``breakeven_pct`` = 100 × total/notional, quantized to 6 dp.

Multi-scrip model (C4): ``n_scrips_sell_day`` splits ``notional`` across that many equal-notional
scrips sold the same day. The DP charge is PER SCRIP PER SELL DAY (delivery sells only), so splitting
a small delivery book across scrips is disproportionately expensive; brokerage is per ORDER, so the
MIS ₹20/order cap is applied per slice. Proportional charges (STT/txn/SEBI/stamp) are unaffected by
the split.
"""

from __future__ import annotations

from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict

from engine.broker.instruments import InstrumentStore
from engine.core.config import config_dir
from engine.core.log import get_logger
from engine.intelligence.schemas import CostBreakdown

_log = get_logger("engine.strategy.cost_model")

Product = Literal["MIS", "CNC"]

_PAISA = Decimal("0.01")
_PCT_Q = Decimal("0.000001")          # breakeven_pct precision (6 dp of a percent)
_EDGE_Q = Decimal("0.0001")           # edge_multiple precision
_CRORE = Decimal("10000000")
_HUNDRED = Decimal("100")

#: Upper bound for the min-viable-qty search. Breakeven is non-increasing in notional (flat
#: components amortize), so a size not viable at this qty is not viable at any realistic size.
_QTY_CEILING = 1_000_000_000

#: yaml ``gst.applies_to`` names -> CostBreakdown component keys (§3.4 pins the component vocabulary).
_GST_BASE_KEYS = {
    "brokerage": "brokerage",
    "exchange_txn_charge": "txn",
    "sebi_charge": "sebi",
    "stt": "stt",
    "stamp_duty": "stamp",
    "dp_charge": "dp",
}


def _dec(value: Any) -> Decimal:
    """yaml scalar → exact Decimal. Floats go through ``str()`` (shortest-repr round-trip) so a yaml
    literal like ``0.00307`` becomes exactly ``Decimal("0.00307")`` — never a binary-float artifact."""
    if isinstance(value, Decimal):
        return value
    if isinstance(value, bool) or value is None:
        raise TypeError(f"expected a number, got {value!r}")
    return Decimal(str(value))


class CostRates(BaseModel):
    """Typed, frozen view of ``config/costs.yaml`` (C2). All ``*_pct`` fields are PERCENTAGES
    (``0.03`` means 0.03%), exactly as published on the Zerodha charges page."""

    model_config = ConfigDict(frozen=True)

    schema_version: int
    verified_on: str
    source: str
    brokerage_delivery_pct: Decimal
    brokerage_delivery_flat_inr: Decimal
    brokerage_intraday_pct: Decimal
    brokerage_intraday_cap_inr: Decimal
    stt_delivery_buy_pct: Decimal
    stt_delivery_sell_pct: Decimal
    stt_intraday_buy_pct: Decimal
    stt_intraday_sell_pct: Decimal
    txn_nse_pct_per_side: Decimal
    sebi_per_crore_inr: Decimal
    stamp_delivery_buy_pct: Decimal
    stamp_intraday_buy_pct: Decimal
    gst_pct: Decimal
    gst_base_components: tuple[str, ...]          # component keys GST applies to (from gst.applies_to)
    dp_per_scrip_per_sell_day_inr: Decimal
    reference_roundtrips: tuple[dict, ...] = ()   # the C3 worked examples the tests assert against

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> CostRates:
        """Map the nested costs.yaml structure onto the flat typed model. Unknown ``gst.applies_to``
        entries are a hard error — a re-scrape that introduces a new charge class must be modelled
        deliberately, never silently dropped (C2)."""
        gst_bases = []
        for name in raw["gst"]["applies_to"]:
            if name not in _GST_BASE_KEYS:
                raise ValueError(f"costs.yaml gst.applies_to has unknown charge class {name!r}")
            gst_bases.append(_GST_BASE_KEYS[name])
        sebi = raw["sebi_charge"]
        if sebi.get("applies_to", "both_sides") != "both_sides":
            raise ValueError("costs.yaml sebi_charge.applies_to changed — update CostModel deliberately")
        return cls(
            schema_version=int(raw["schema_version"]),
            verified_on=str(raw["verified_on"]),
            source=str(raw["source"]),
            brokerage_delivery_pct=_dec(raw["brokerage"]["delivery_cnc"]["pct"]),
            brokerage_delivery_flat_inr=_dec(raw["brokerage"]["delivery_cnc"]["flat_inr"]),
            brokerage_intraday_pct=_dec(raw["brokerage"]["intraday_mis"]["pct"]),
            brokerage_intraday_cap_inr=_dec(raw["brokerage"]["intraday_mis"]["cap_inr"]),
            stt_delivery_buy_pct=_dec(raw["stt"]["delivery"]["buy_pct"]),
            stt_delivery_sell_pct=_dec(raw["stt"]["delivery"]["sell_pct"]),
            stt_intraday_buy_pct=_dec(raw["stt"]["intraday"]["buy_pct"]),
            stt_intraday_sell_pct=_dec(raw["stt"]["intraday"]["sell_pct"]),
            txn_nse_pct_per_side=_dec(raw["exchange_txn_charge"]["nse_pct_per_side"]),
            sebi_per_crore_inr=_dec(sebi["per_crore_inr"]),
            stamp_delivery_buy_pct=_dec(raw["stamp_duty"]["delivery_buy_pct"]),
            stamp_intraday_buy_pct=_dec(raw["stamp_duty"]["intraday_buy_pct"]),
            gst_pct=_dec(raw["gst"]["pct"]),
            gst_base_components=tuple(gst_bases),
            dp_per_scrip_per_sell_day_inr=_dec(raw["dp_charge"]["per_scrip_per_sell_day_inr"]),
            reference_roundtrips=tuple(raw.get("reference_roundtrips") or ()),
        )


def load_cost_rates(path: Path | None = None) -> CostRates:
    """Load + type ``config/costs.yaml`` (default: the repo config dir)."""
    p = Path(path) if path is not None else config_dir() / "costs.yaml"
    raw = yaml.safe_load(p.read_text(encoding="utf-8"))
    rates = CostRates.from_dict(raw)
    _log.info("cost_rates_loaded", path=str(p), verified_on=rates.verified_on)
    return rates


class CostModel:
    """C1/C2/C3 — single source of truth for round-trip costs, breakeven, and minimum viable size.

    Parameters
    ----------
    rates:
        The typed ``config/costs.yaml`` view (:func:`load_cost_rates`).
    instruments:
        The daily :class:`InstrumentStore` — :meth:`min_viable_qty` snaps every level with its
        ``round_to_tick`` (A10 banded tick sizes; never inline tick math). Optional so pure
        round-trip/breakeven math (backtest pricing) needs no broker surface.
    edge_multiple_min:
        §7.1 ``min_viable_size`` multiple (default 2.0). The live value is the ONE learner-movable
        gate knob and comes from the hash-verified ``limits.yaml``/``envelope.yaml`` (R4) — the
        composition root passes it in; this class never reads protected config itself.
    """

    def __init__(
        self,
        rates: CostRates,
        instruments: InstrumentStore | None = None,
        *,
        edge_multiple_min: Decimal = Decimal("2.0"),
    ) -> None:
        self._rates = rates
        self._instruments = instruments
        self._edge_multiple_min = _dec(edge_multiple_min)
        if self._edge_multiple_min <= 0:
            raise ValueError("edge_multiple_min must be positive")

    @classmethod
    def from_config(
        cls,
        path: Path | None = None,
        instruments: InstrumentStore | None = None,
        *,
        edge_multiple_min: Decimal = Decimal("2.0"),
    ) -> CostModel:
        """Build from ``config/costs.yaml`` (default path: the repo config dir)."""
        return cls(load_cost_rates(path), instruments, edge_multiple_min=edge_multiple_min)

    @property
    def rates(self) -> CostRates:
        return self._rates

    @property
    def edge_multiple_min(self) -> Decimal:
        return self._edge_multiple_min

    # ------------------------------------------------------------------ pinned surface (§3.2.5)
    def round_trip(
        self, notional: Decimal, product: Literal["MIS", "CNC"], n_scrips_sell_day: int = 1
    ) -> CostBreakdown:
        """Full round-trip (buy + sell) cost for ``notional`` under ``product`` (C1/C3).

        ``n_scrips_sell_day`` models the C4 multi-scrip case: the notional split across that many
        equal slices sold the same day (DP charged per scrip; brokerage capped per order). The
        ``expected_edge_pct``/``edge_multiple`` fields are 0 on this call ("not evaluated") — the
        sizing/gate path fills them via :meth:`with_edge`.
        """
        product = self._validate_product(product)
        notional = self._validate_notional(notional)
        if not isinstance(n_scrips_sell_day, int) or n_scrips_sell_day < 1:
            raise ValueError(f"n_scrips_sell_day must be an int >= 1, got {n_scrips_sell_day!r}")
        components = self._components(notional, product, n_scrips_sell_day)
        total = sum(components.values(), Decimal("0"))
        return CostBreakdown(
            notional=notional.quantize(_PAISA, rounding=ROUND_HALF_UP),
            total_cost=total,
            breakeven_pct=(total / notional * _HUNDRED).quantize(_PCT_Q, rounding=ROUND_HALF_UP),
            expected_edge_pct=Decimal("0"),
            edge_multiple=Decimal("0"),
            components=components,
        )

    def breakeven_pct(self, notional: Decimal, product: str) -> Decimal:
        """Percentage move needed to cover a single-scrip round trip at ``notional`` (C3)."""
        return self.round_trip(self._validate_notional(notional), self._validate_product(product)).breakeven_pct

    def min_viable_qty(
        self, symbol: str, entry: Decimal, stop: Decimal, target: Decimal, product: str
    ) -> int:
        """Smallest qty whose expected edge covers ``edge_multiple_min`` × breakeven (§7.1, R1/C3).

        Levels are first snapped to ``symbol``'s banded tick via ``InstrumentStore.round_to_tick``
        (A10). Direction is inferred from the stop (stop below entry ⇒ long, above ⇒ short) and
        ``expected_edge_pct`` is the §3.2.5-PINNED unweighted formula — (first target − entry)/entry,
        applied as the favorable-move magnitude in that direction. It DELIBERATELY excludes any
        hit-probability weighting (simple, auditable); **changing this formula is a strategy-logic
        change requiring owner approval (R4)** — do not "improve" it here.

        Returns 0 when NO size is viable (edge ≤ 0, or below the proportional-cost floor) — the
        caller must reject (§7.1 ``min_viable_size``: gate shrink-then-recheck ends in reject).
        Raises ``ValueError`` for incoherent levels (stop == entry) and ``RuntimeError`` if built
        without an :class:`InstrumentStore`.
        """
        if self._instruments is None:
            raise RuntimeError("min_viable_qty requires an InstrumentStore (tick rounding, A10)")
        product = self._validate_product(product)
        entry = self._instruments.round_to_tick(symbol, entry)
        stop = self._instruments.round_to_tick(symbol, stop)
        target = self._instruments.round_to_tick(symbol, target)
        if entry <= 0:
            raise ValueError(f"entry must be positive, got {entry}")
        edge_pct = self.expected_edge_pct(entry, stop, target)
        if edge_pct <= 0:
            return 0
        required = self._edge_multiple_min

        def viable(qty: int) -> bool:
            return edge_pct >= required * self.breakeven_pct(qty * entry, product)

        if not viable(_QTY_CEILING):
            return 0
        lo, hi = 1, _QTY_CEILING
        while lo < hi:                      # breakeven is non-increasing in notional ⇒ predicate monotone
            mid = (lo + hi) // 2
            if viable(mid):
                hi = mid
            else:
                lo = mid + 1
        return lo

    # ------------------------------------------------------------------ gate/sizing helpers
    def expected_edge_pct(self, entry: Decimal, stop: Decimal, target: Decimal) -> Decimal:
        """The §3.2.5-pinned unweighted expected edge, in percent (see :meth:`min_viable_qty`).

        Long (stop < entry): 100 × (target − entry)/entry; short (stop > entry): the same magnitude
        with the sides mirrored, 100 × (entry − target)/entry. A target on the wrong side of entry
        yields a non-positive edge (⇒ never viable). ``stop == entry`` is incoherent ⇒ ``ValueError``.
        """
        if entry <= 0:
            raise ValueError(f"entry must be positive, got {entry}")
        if stop == entry:
            raise ValueError("stop == entry gives no direction; levels are incoherent")
        if stop < entry:                    # long
            return (target - entry) / entry * _HUNDRED
        return (entry - target) / entry * _HUNDRED

    def with_edge(self, breakdown: CostBreakdown, expected_edge_pct: Decimal) -> CostBreakdown:
        """Return a copy of ``breakdown`` with the edge fields populated (GateVerdict.cost, §3.4)."""
        multiple = (
            (expected_edge_pct / breakdown.breakeven_pct).quantize(_EDGE_Q, rounding=ROUND_HALF_UP)
            if breakdown.breakeven_pct > 0
            else Decimal("0")
        )
        return breakdown.model_copy(
            update={"expected_edge_pct": expected_edge_pct, "edge_multiple": multiple}
        )

    # ------------------------------------------------------------------ internals
    @staticmethod
    def _validate_product(product: str) -> Product:
        p = str(product).upper()
        if p not in ("MIS", "CNC"):
            raise ValueError(f"product must be 'MIS' or 'CNC', got {product!r}")
        return p  # type: ignore[return-value]

    @staticmethod
    def _validate_notional(notional: Decimal) -> Decimal:
        if isinstance(notional, float):
            raise TypeError("notional must be Decimal, never float (§3.2 money convention)")
        if isinstance(notional, int):
            notional = Decimal(notional)
        if not isinstance(notional, Decimal):
            raise TypeError(f"notional must be Decimal, got {type(notional).__name__}")
        if notional <= 0:
            raise ValueError(f"notional must be positive, got {notional}")
        return notional

    def _components(self, notional: Decimal, product: Product, n_scrips: int) -> dict[str, Decimal]:
        """Raw charge math (exact Decimal), then per-component paisa quantization (module contract)."""
        r = self._rates
        slice_notional = notional / n_scrips
        if product == "MIS":
            per_order = min(slice_notional * r.brokerage_intraday_pct / _HUNDRED, r.brokerage_intraday_cap_inr)
            brokerage = 2 * n_scrips * per_order
            stt = notional * (r.stt_intraday_buy_pct + r.stt_intraday_sell_pct) / _HUNDRED
            stamp = notional * r.stamp_intraday_buy_pct / _HUNDRED
            dp = Decimal("0")
        else:
            per_order = slice_notional * r.brokerage_delivery_pct / _HUNDRED + r.brokerage_delivery_flat_inr
            brokerage = 2 * n_scrips * per_order
            stt = notional * (r.stt_delivery_buy_pct + r.stt_delivery_sell_pct) / _HUNDRED
            stamp = notional * r.stamp_delivery_buy_pct / _HUNDRED
            dp = n_scrips * r.dp_per_scrip_per_sell_day_inr     # per scrip per SELL day (C4)
        txn = 2 * notional * r.txn_nse_pct_per_side / _HUNDRED
        sebi = 2 * (notional / _CRORE) * r.sebi_per_crore_inr
        raw = {"brokerage": brokerage, "stt": stt, "txn": txn, "sebi": sebi, "stamp": stamp, "dp": dp}
        gst = r.gst_pct / _HUNDRED * sum((raw[k] for k in r.gst_base_components), Decimal("0"))
        ordered = {k: raw[k] for k in ("brokerage", "stt", "txn", "sebi", "stamp")} | {"gst": gst, "dp": dp}
        return {k: v.quantize(_PAISA, rounding=ROUND_HALF_UP) for k, v in ordered.items()}
