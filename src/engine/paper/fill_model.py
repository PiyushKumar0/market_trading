"""Conservative paper fill model: config + slippage math (§3.2.9, R9).

WO-P3-2, 2026-09-10 (§8.4 Phase-3 decomposition addendum).

The whole point of the paper tier is that a paper fill must never be *better* than a live fill
plausibly would have been — the §8.5 gate reads paper expectancy, so an optimistic fill model is a
way to lie to the gate. Three knobs carry that conservatism, and each is pinned here:

1. **Slippage = half-spread + k·σ₁ₘ, signed against the taker.** A BUY always pays more than the
   print, a SELL always receives less. There is no path that returns a favourable or zero
   adjustment.
2. **k is per time-of-day bucket, not global** (§3.2.9). The recorded-tick corpus is
   window-biased — the engine runs mostly in the morning window (§2.6) — so a single global k
   would understate the open/close slippage that the G4 go-live gate actually measures. Defaults
   are open 1.0 / mid 0.5 / close 1.0 until WO-P3-4's calibration replaces them.
3. **The half-spread is NEVER zero.** L1 depth is absent across backfilled/WARMING spans (§2.6);
   a zero half-spread there would make restart days silently non-conservative. Resolution order is
   L1 → per-symbol calibrated median (floored at ``tick_size / 2``, the tightest a real book can
   quote) → ``half_spread_fallback_ticks × tick_size``, and everything below L1 is flagged
   ``estimated`` so downstream can see it. Note the two floors are different on purpose: the
   fallback multiple is an *ignorance* default for an unmeasured symbol, while the calibrated
   median is a real measurement and is clamped only to physical reality (WO-P3-4 round 4).

**Config file.** ``config/fill_model.yaml`` is written by WO-P3-4 (``scripts/calibrate_fill_model.py``)
and is OPTIONAL: with the file absent this module returns the conservative defaults above, so the
paper tier is usable before any calibration exists. The schema (pinned; the calibrator writes it):

.. code-block:: yaml

    version: 1
    generated_at: '2026-09-10T18:00:00+05:30'
    latency_ms: 700
    buckets:
      open:  {start: '09:15', end: '10:00', k: 1.0}
      mid:   {start: '10:00', end: '14:30', k: 0.5}
      close: {start: '14:30', end: '15:30', k: 1.0}
    half_spread_fallback_ticks: 2
    symbols:
      TCS: {median_half_spread_pct: 0.02}

All prices/multipliers are :class:`~decimal.Decimal` (§3.2 money convention). YAML floats are
converted via ``Decimal(str(v))`` so ``k: 1.4`` is exactly ``Decimal("1.4")``, never binary noise.
This module imports only stdlib + pydantic/yaml (plus ``engine.core.config`` lazily, to resolve the
default config path): its engine dependency is ``engine.core`` ALONE, as §3.2.9 requires.
``tests/unit/test_import_graph.py`` enforces that per module (round 3, 2026-09-10) — the tier-wide
statement it used to make here was too strong, because §8.4's WO-P3-3 harness (``replay.py``) is
sanctioned to import ``engine.marketdata`` as well. That allowance is one module wide; ``broker``,
``surface``, ``__init__`` and this file stay on ``core``, and none of the five may ever reach
``engine.intelligence`` (R1) or ``engine.broker`` (the pykiteconnect chain).
"""

from __future__ import annotations

from datetime import time, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator

#: Simulated broker round-trip before an order can fill (§3.2.9). 700 ms is the plan's pinned value.
DEFAULT_LATENCY_MS = 700
#: Half-spread fallback when neither L1 depth nor a calibrated per-symbol median exists.
DEFAULT_FALLBACK_TICKS = 2
#: k for the thin/violent edges of the session. Deliberately 2x the mid-session default.
DEFAULT_K_EDGE = Decimal("1.0")
#: k for the calm middle of the session.
DEFAULT_K_MID = Decimal("0.5")
#: Canonical config file name under ``config/`` (written by WO-P3-4, absent until then).
FILL_MODEL_FILENAME = "fill_model.yaml"

_SIDES = ("BUY", "SELL")


def _to_decimal(value: Any) -> Any:
    """YAML float/int/str -> exact Decimal. Passed through pydantic as a *before* validator so the
    binary float never reaches the model (``Decimal(0.02)`` is a 50-digit approximation)."""
    if isinstance(value, Decimal):
        return value
    if isinstance(value, (int, float, str)):
        return Decimal(str(value))
    return value


def _to_time(value: Any) -> Any:
    """``'09:15'`` / ``'14:30:00'`` -> :class:`datetime.time`.

    Parsed here rather than left to pydantic's datetime parser so the accepted forms are pinned by
    this module and not by a transitive library's behaviour.
    """
    if isinstance(value, str):
        parts = value.split(":")
        if len(parts) not in (2, 3):
            raise ValueError(f"bucket time must be 'HH:MM' or 'HH:MM:SS', got {value!r}")
        return time(int(parts[0]), int(parts[1]), int(parts[2]) if len(parts) == 3 else 0)
    return value


class BucketCfg(BaseModel):
    """One time-of-day slippage bucket: ``[start, end)`` with its own k multiplier."""

    model_config = ConfigDict(frozen=True)

    start: time
    end: time
    k: Decimal

    _parse_time = field_validator("start", "end", mode="before")(_to_time)
    _parse_k = field_validator("k", mode="before")(_to_decimal)


class SymbolCfg(BaseModel):
    """Per-symbol calibrated half-spread, as a PERCENT of price (``0.02`` == 0.02% == 2 bps)."""

    model_config = ConfigDict(frozen=True)

    median_half_spread_pct: Decimal

    _parse_pct = field_validator("median_half_spread_pct", mode="before")(_to_decimal)


def _default_buckets() -> dict[str, BucketCfg]:
    # NSE continuous session 09:15-15:30 split per §3.2.9. Intervals are start-inclusive /
    # end-exclusive so 10:00:00 belongs to `mid` and 14:30:00 to `close` — exactly once each.
    return {
        "open": BucketCfg(start=time(9, 15), end=time(10, 0), k=DEFAULT_K_EDGE),
        "mid": BucketCfg(start=time(10, 0), end=time(14, 30), k=DEFAULT_K_MID),
        "close": BucketCfg(start=time(14, 30), end=time(15, 30), k=DEFAULT_K_EDGE),
    }


class FillModelConfig(BaseModel):
    """The fill model's tunables. Defaults ARE the conservative model (no calibration required)."""

    model_config = ConfigDict(frozen=True)

    version: int = 1
    generated_at: str | None = None
    latency_ms: int = Field(default=DEFAULT_LATENCY_MS, ge=0)
    buckets: dict[str, BucketCfg] = Field(default_factory=_default_buckets)
    half_spread_fallback_ticks: int = Field(default=DEFAULT_FALLBACK_TICKS, gt=0)
    symbols: dict[str, SymbolCfg] = Field(default_factory=dict)

    # -- time ---------------------------------------------------------------------------------
    def latency(self) -> timedelta:
        """Simulated broker latency as a timedelta (an order is not fillable before now+latency)."""
        return timedelta(milliseconds=self.latency_ms)

    def _ordered_buckets(self) -> list[tuple[str, BucketCfg]]:
        """Buckets sorted by ``start`` — never trusting YAML/dict insertion order, because the
        clamping rules below (`before the first`, `after the last`) depend on real ordering."""
        return sorted(self.buckets.items(), key=lambda kv: kv[1].start)

    def bucket_for(self, t: time) -> str:
        """Bucket name for wall time ``t``; ``[start, end)``.

        Out-of-session times are CLAMPED to the nearest edge bucket (open before the first start,
        close at/after the last end) rather than falling through to "no bucket". A missing bucket
        would mean k=0, i.e. slippage collapsing to the bare half-spread — the non-conservative
        direction — for exactly the ticks (pre-open prints, post-close corrections, a shortened
        muhurat session) where the true k is highest.
        """
        ordered = self._ordered_buckets()
        if not ordered:                      # defensive: an emptied `buckets:` block
            return "close"
        for name, cfg in ordered:
            if cfg.start <= t < cfg.end:
                return name
        if t < ordered[0][1].start:
            return ordered[0][0]
        return ordered[-1][0]

    def k_for(self, bucket: str) -> Decimal:
        """k multiplier for a bucket name. An unknown name takes the MOST conservative k present —
        never 0 (a typo in a calibrated yaml must not disarm the model)."""
        cfg = self.buckets.get(bucket)
        if cfg is not None:
            return cfg.k
        if not self.buckets:
            return DEFAULT_K_EDGE
        return max(b.k for b in self.buckets.values())

    # -- spread -------------------------------------------------------------------------------
    def half_spread(
        self,
        symbol: str,
        bid: Decimal | None,
        ask: Decimal | None,
        ltp: Decimal,
        tick_size: Decimal,
    ) -> tuple[Decimal, bool]:
        """Half-spread in PRICE units plus an ``estimated`` flag.

        Resolution order (§3.2.9):

        1. **L1** — both sides present and ``ask > bid``: ``(ask - bid) / 2``, ``estimated=False``.
           A locked (``ask == bid``) or crossed (``ask < bid``) book is treated as absent depth, not
           as a zero/negative spread — those arise from stale or partial depth frames, and either
           would make the fill favourable. No floor is applied: a real book cannot quote inside the
           tick, so what L1 says IS the physical answer.
        2. **Per-symbol calibrated median** —
           ``max(ltp × median_half_spread_pct / 100, tick_size / 2)``, ``estimated=True``. A
           calibrated 0 is rejected and falls through.
        3. **Tick fallback** — ``half_spread_fallback_ticks × tick_size``, ``estimated=True``.

        WHY the calibrated branch is floored at HALF A TICK and not at the rung below it (manager
        decision, WO-P3-4 round 4, 2026-09-10): ``half_spread_fallback_ticks × tick_size`` is an
        *ignorance* default for a symbol nothing was ever measured on, not a physical floor. The
        calibrated median is a measurement of this account's own L1 book, accurate to the paisa, and
        on the shipped corpus a large minority of symbols quote ONE tick wide and so legitimately
        resolve below the 2-tick default. Clamping a measurement up to an ignorance default would
        discard real evidence. What the measurement may never produce is a spread no market could
        quote, and the tightest quotable market is one tick wide — hence ``tick_size / 2``. The
        clamp bites when a symbol calibrated at a high price is evaluated at a much lower ``ltp``
        (a split, a collapse, a stale calibration), where percent-of-price shrinks below a tick.

        The result is asserted ``> 0``: a zero half-spread is the single change that would make the
        model non-conservative on every restart/backfill day (§3.2.9, "never 0").
        """
        if bid is not None and ask is not None and ask > bid:
            return (ask - bid) / Decimal(2), False

        sym_cfg = self.symbols.get(symbol)
        if sym_cfg is not None and sym_cfg.median_half_spread_pct > 0 and ltp > 0:
            estimate = ltp * sym_cfg.median_half_spread_pct / Decimal(100)
            if estimate > 0:
                return max(estimate, tick_size / Decimal(2)), True

        fallback = Decimal(self.half_spread_fallback_ticks) * tick_size
        assert fallback > 0, (
            f"half-spread fallback must be > 0 (ticks={self.half_spread_fallback_ticks}, "
            f"tick_size={tick_size}) — a zero half-spread breaks R9 conservatism"
        )
        return fallback, True

    # -- slippage -----------------------------------------------------------------------------
    def slippage(
        self,
        symbol: str,
        side: str,
        bucket: str,
        sigma_1m: Decimal,
        half_spread: Decimal,
    ) -> Decimal:
        """Signed price adjustment the taker pays: ``+(half_spread + k·σ₁ₘ)`` for BUY, ``-`` for SELL.

        The caller adds this to the reference price, so the sign IS the conservatism: a buyer's fill
        moves up, a seller's moves down, in every bucket and for every sigma. ``sigma_1m`` is
        supplied in price units by the caller (``PaperBroker.SigmaEstimator``) — this function does
        no estimation of its own so replay and calibration can drive it with recorded values.

        ``symbol`` is unused today; it is in the signature because WO-P3-4 may calibrate k per
        liquidity class, and changing the call shape later would touch every caller.
        """
        if side not in _SIDES:
            raise ValueError(f"transaction_type must be one of {_SIDES}, got {side!r}")
        magnitude = half_spread + self.k_for(bucket) * sigma_1m
        return magnitude if side == "BUY" else -magnitude


def load_fill_model(path: str | Path | None = None) -> FillModelConfig:
    """Load ``config/fill_model.yaml`` if it exists, else the conservative defaults.

    The file is generated by WO-P3-4 and is intentionally NOT required: the paper tier must be
    runnable (and testable) before any tick corpus has been calibrated. A partial file is merged
    over the defaults key-by-key, so adding ``latency_ms`` alone does not wipe the bucket table.
    """
    if path is None:
        # Imported lazily: engine.core.config resolves MT_CONFIG_DIR / the repo root, and keeping
        # the import inside the function keeps this module importable from a bare script.
        from engine.core.config import config_dir

        resolved = config_dir() / FILL_MODEL_FILENAME
    else:
        resolved = Path(path)

    if not resolved.exists():
        return FillModelConfig()

    raw = yaml.safe_load(resolved.read_text(encoding="utf-8")) or {}
    if not isinstance(raw, dict):
        raise ValueError(f"{resolved} must contain a YAML mapping, got {type(raw).__name__}")
    return FillModelConfig.model_validate(raw)
