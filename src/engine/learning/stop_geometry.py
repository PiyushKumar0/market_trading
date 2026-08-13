"""WO-17 — PRE-REGISTERED diagnostic: stop geometry / adverse-excursion recovery on intraday longs.

**C-alpha-experiment (diagnostic). STOPS FOR OWNER REVIEW. Promotion is NOT sought.** This module is
research machinery only: nothing here is imported by a scanner, the gate, the OMS or any scheduled
job; it writes NO ``param_sets`` row and — unlike :mod:`engine.learning.vwap_reversion` — it does not
call :class:`engine.learning.validate.ValidationPipeline` **at all**. WO-17: *"Promotion is NOT sought
— the deliverable is the stop-policy evidence table; any live stop-guidance change that follows is a
separate owner-approved change (analyst prompt / plan §6.1), never automatic."*

THE PRE-REGISTRATION (IMPROVEMENT_SPEC.md WO-17, quoted — this module may not deviate from it)
-----------------------------------------------------------------------------------------------
    Evidence: owner hypothesis — allowing a deeper adverse dip on an intraday buy may let noise-hit
    positions recover to a positive sell, vs the current tight stop realizing the dip as a loss.
    The hindsight replay (07-29..31) recorded "the declines dodged 8 stops"; each stop-out
    realizes the full round-trip cost floor.
    Null hypothesis (stated up front): on a driftless path, ALL stop geometries have equal pre-cost
    expected value (optional stopping) — any measured difference between stop widths is evidence
    of post-dip CONDITIONAL drift (genuine recovery tendency after adverse excursion), which is
    exactly what the owner's hypothesis asserts and what this study measures.
    Pre-registration: population = the orb entry-signal stream (the highest-volume recorded
    intraday long family) over the full 1m window, entries next-1m-bar-open per WO-2; per entry,
    simulate a FIXED axis of stop widths {1.0, 1.5, 2.5, 4.0} x ATR_10m (WO-10's stop unit,
    prior-session-seeded per WO-10b so morning entries are stoppable) plus NO-STOP
    (session-end square-off only); TWO exit ladders per width, both fixed: (a) orb's own
    1.5R target + stop, (b) no target — exit at session end or stop. 10 configs total; per
    config report net expectancy at Rs20k MIS incl. spread, win rate, and the two diagnostic
    quantities the hypothesis lives on: DODGED-WINNER FRACTION (of trades stopped at width w,
    the share that would have ended positive by session end) and the MAE distribution of
    eventual winners (what dip depth winners actually survive).

The axes are **FIXED, never swept**: 5 stop widths x 2 exit ladders = exactly 10 configurations. That
is what contains the multiplicity, together with the no-promotion rule.

DESIGN REQUIREMENTS (manager-specified, beyond the WO text)
-----------------------------------------------------------
1. **The SAME entries orb would have taken.** The population is not a re-derivation of the breakout
   rule: :func:`orb_signal_stream` calls :func:`engine.learning.sweep._signals_orb` — *the sweep's own
   orb builder* — so the entry definition cannot drift from the baseline it is meant to describe. orb
   admits at most ONE entry per (symbol, session) (its builder ``break``s on the first qualifying
   breakout), which is also why "one position per symbol at a time" is satisfied by construction and
   why the entry set is **identical across all 10 configurations**: the geometry changes the EXIT,
   never the entry. A study whose populations differed per config would be comparing different trades.
2. **Fills: next-1m-bar OPEN, never the signal bar** (WO-2). The breakout is read from bar *t*'s
   COMPLETED close and the entry fills at bar *t+1*'s **open**; a stop or target trigger observed on
   bar *i* fills at bar *i+1*'s open. A signal on a session's last usable bar is DROPPED.
3. **Costs.** ``product = MIS``, per-trade notional Rs20,000, charge =
   ``CostModel.breakeven_pct(20000, "MIS")`` — WO-2's corrected surface **INCLUDING spread** — levied
   in full on EVERY simulated trade, including the ones the stop cut short (WO-17's own words: "each
   stop-out realizes the full round-trip cost floor"). Reused from
   :func:`engine.learning.vwap_reversion.cost_floor_pct_for`, not re-derived.
4. **The stop unit is WO-10's, prior-session-seeded per WO-10b.**
   :func:`engine.learning.vwap_reversion.session_atr_10m` and the seeding chain are IMPORTED, not
   copied — the two experiments must measure the same ATR or the shared-unit claim is false. See
   :data:`SEED_ATR_PRIOR_SESSION` for the single switch point.
5. **Report.** JSON + MD to ``data/reports/stop_geometry_<ts>.{json,md}``: the 10-config table, both
   diagnostics, the null hypothesis verbatim, a plain-language answer to the owner's question, the
   :data:`BANNER`, and a ``modelling_notes`` block reprinting every interpretation.
6. **Unit-tested on synthetic 1m fixtures only** (``tests/unit/test_stop_geometry.py``); nothing here
   opens the store.

INTERPRETATIONS — where the WO text was under-determined
--------------------------------------------------------
Recorded in :data:`PRE_REGISTRATION_INTERPRETATIONS` and reprinted in the report so a reader can audit
them. They are named constants, not inline literals, so changing one is a visible diff.

* **"orb's own 1.5R target" under NO-STOP** — the one genuinely ambiguous phrase, because ladder (a)
  pairs a target with a NO-STOP width where "R" has no stop to be measured against. Two readings:
  (i) R = orb's OWN risk unit — ``stop_range_frac x (signal close - opening-range low)``, the number
  orb's builder already stamps as its ``tp_stop`` fraction (``rr_target x risk/price``, rr_target =
  1.5 by §6.3 default); (ii) R = the WO-17 stop width being tested. **This module implements (i)**,
  for three reasons: the WO says "orb's OWN 1.5R target" (orb's R, not the study's); reading (ii) is
  undefined for the NO-STOP row, i.e. it cannot produce the pre-registered 10 configs at all; and
  reading (ii) would move the TARGET whenever the STOP moves, confounding the exact comparison the
  owner asked for. Under (i) the target is one fixed level per entry and the stop axis varies alone.
  :data:`TARGET_SOURCE` is the switch point. **FLAGGED FOR THE MANAGER.**
* **"would have ended positive by session end"** — measured on the REFERENCE PATH: the same entry
  held with NO stop and NO target to the session-end square-off, i.e. exactly the ``(NO-STOP,
  no-target)`` configuration. "Positive" is read as **NET of the full round trip** (a gross-positive
  exit that does not cover its own costs is not a recovery worth having); the GROSS variant is
  reported alongside so the reading is auditable, never hidden.
* **MAE (maximum adverse excursion)** — measured over the bars whose lows a stop could actually have
  triggered on: the fill bar through the bar BEFORE the session-end exit fill. That range is chosen
  so the two diagnostics are exactly consistent: a stop at level ``S`` is hit **if and only if** the
  path's minimum low reaches ``S``, so "MAE >= w x ATR" and "stopped at width w" are the same event
  (pinned by a test). Reported in BOTH percent of the entry fill and multiples of that entry's
  ATR_10m, at p25/p50/p75/p90.
* **Simultaneous stop and target inside one 1m bar** — 1m bars carry no intrabar path, so the ADVERSE
  event is assumed first: the stop wins the tie. The house convention (vwap_reversion, sweep).
* **Session-end square-off** — at the session's LAST bar's OPEN (MIS obligation), matching
  vwap_reversion's ``session_end`` handling; there is no timeout rung in WO-17's ladders.
* **Entries with no defined ATR_10m are dropped for EVERY config, including NO-STOP.** The stop unit
  is also the reporting unit (MAE in ATR multiples), and — decisively — an entry admitted to the
  NO-STOP config but not to the stopped ones would break requirement 1's common population. WO-10b's
  seeding is what makes this cheap: with a prior-session seed the ATR exists from 09:25, so ordinary
  orb morning breakouts are stoppable; only a symbol's FIRST session in the window falls back to the
  unseeded 140-minute warm-up and loses its entry.
* **orb parameters** — the §6.3 envelope DEFAULTS (``config/envelope.yaml``), read through
  :func:`engine.learning.sweep.load_envelope` rather than hardcoded: orb_minutes 30, vol_mult 1.5,
  stop_range_frac 1.0, rr_target 1.5. WO-17 says "the orb entry-signal stream", not "the best orb grid
  point", and the default IS the champion baseline the sweep always unions in.
* **Status-quo reference row** — orb's own geometry (its range-anchored stop + its 1.5R target) is
  simulated as ONE extra clearly-labelled row so "beat the current tight stop margins" has a
  comparator. It is **NOT** one of the pre-registered 10, is excluded from the 10-config table, and is
  switched off by :data:`INCLUDE_STATUS_QUO_REFERENCE` = ``False``.

The module top level is pandas/numpy/pydantic + stdlib (``MarketStore``/``CostModel`` are
TYPE_CHECKING-only, like :mod:`engine.learning.sweep`), so the pure test tier imports it cheaply.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, Any

import numpy as np
import pandas as pd
from pydantic import BaseModel, ConfigDict, Field

from engine.core.clock import IST, Clock
from engine.core.log import get_logger
from engine.learning.reports import ReportArtifacts

# The orb entry definition is IMPORTED from the sweep, never re-derived (design requirement 1).
# These names are module-private in ``sweep`` only because nothing outside it needed them before;
# importing them is deliberate — a local re-implementation of the breakout rule is exactly the drift
# this study cannot afford. PROPOSED (not done here, this module does not own sweep.py): re-export
# ``_signals_orb`` / ``_Frames`` under public names so the seam is declared rather than borrowed.
from engine.learning.sweep import (
    _ORB_ENTRY_END as ORB_ENTRY_END,
)
from engine.learning.sweep import (
    _ORB_ENTRY_START as ORB_ENTRY_START,
)
from engine.learning.sweep import (
    _Frames,
    _signals_orb,
    load_envelope,
)

# The stop unit and the cost charge are WO-10's, imported so the two experiments cannot diverge.
from engine.learning.vwap_reversion import (
    ATR_PERIOD,
    ATR_RESAMPLE_MINUTES,
    ATR_WARMUP_MINUTES,
    compute_session_features,
    cost_floor_pct_for,
    session_slices,
)

if TYPE_CHECKING:                                    # heavy/optional surfaces stay out of the top level
    from engine.learning.vwap_reversion import SessionFeatures
    from engine.marketdata.store import MarketStore
    from engine.strategy.cost_model import CostModel

_log = get_logger("engine.learning.stop_geometry")

# --------------------------------------------------------------------------- pre-registered constants
#: Report identity. Deliberately absent from ``sweep.PRICE_BASELINES`` and ``PRODUCT_BY_STRATEGY`` —
#: this is a diagnostic, not a baseline, and must not be reachable from any promotion path.
EXPERIMENT_ID = "stop_geometry"

#: The banner every report leads with (design requirement 5).
BANNER = "C-CATEGORY: STOPS FOR OWNER REVIEW - no live wiring, promotion NOT sought"

#: WO-17 costs: MIS, Rs20,000 per trade, WO-2's corrected surface INCLUDING spread.
PRODUCT = "MIS"
REFERENCE_NOTIONAL: Decimal = Decimal("20000")

#: THE stop axis — FIXED, never swept. ``None`` is the NO-STOP rung (session-end square-off only).
STOP_ATR_MULTIPLES: tuple[float, ...] = (1.0, 1.5, 2.5, 4.0)
STOP_AXIS: tuple[float | None, ...] = (*STOP_ATR_MULTIPLES, None)

#: THE exit ladders — FIXED, never swept. ``True`` = ladder (a) "orb's own 1.5R target + the stop";
#: ``False`` = ladder (b) "no target — exit at session end or stop".
LADDERS: tuple[bool, ...] = (True, False)

#: 5 widths x 2 ladders = the pre-registered configuration count.
N_CONFIGS = len(STOP_AXIS) * len(LADDERS)

#: WO-17's null hypothesis, VERBATIM from IMPROVEMENT_SPEC.md. Reprinted at the top of every report:
#: the study measures a departure from THIS, and a reader must see it before any number.
NULL_HYPOTHESIS = (
    "Null hypothesis (stated up front): on a driftless path, ALL stop geometries have equal pre-cost "
    "expected value (optional stopping) - any measured difference between stop widths is evidence of "
    "post-dip CONDITIONAL drift (genuine recovery tendency after adverse excursion), which is exactly "
    "what the owner's hypothesis asserts and what this study measures."
)

#: The owner's question, restated so the report answers the question that was actually asked.
OWNER_QUESTION = (
    "Does allowing a DEEPER adverse dip on an intraday buy (a wider stop), which lets noise-hit "
    "positions recover to a positive sell, beat the current tight stop margins?"
)

#: WO-10b: the stop unit's ATR seeds from the PRIOR session, so morning orb entries are stoppable.
#: THE SWITCH POINT for the seeded-vs-unseeded stop unit (WO-17 depends on WO-10b's shared unit).
#: ``False`` reproduces WO-10's unseeded 140-minute warm-up, which would drop nearly every orb entry
#: (orb enters just after its 30-minute opening range, i.e. ~09:45 — long before an unseeded 11:35).
SEED_ATR_PRIOR_SESSION = True

#: The ladder-(a) target source. ``"orb_rr_target"`` = orb's OWN 1.5R target, i.e. the ``tp_stop``
#: fraction orb's own builder stamps (``rr_target x stop_range_frac x (close - range_low) / close``),
#: applied to the entry FILL price per the house ``stop_entry_price='fillprice'`` convention. THE
#: SWITCH POINT for the one flagged pre-registration ambiguity (see the module docstring).
TARGET_SOURCE = "orb_rr_target"

#: Simulate orb's own geometry (range-anchored stop + 1.5R target) as a clearly-labelled comparator.
#: NOT one of the pre-registered 10; excluded from the 10-config table. ``False`` removes it entirely.
INCLUDE_STATUS_QUO_REFERENCE = True

#: WO-2: what the report's ``fill_mechanics`` field declares.
FILL_MECHANICS = "next_bar_open"

#: The reported MAE quantiles (design requirement 5 / WO-17 "the MAE distribution").
MAE_QUANTILES: tuple[float, ...] = (25.0, 50.0, 75.0, 90.0)

#: Where a config's stop level comes from.
STOP_SOURCE_ATR = "atr_10m"              # w x ATR_10m below the entry fill (the pre-registered axis)
STOP_SOURCE_NONE = "none"                # NO-STOP
STOP_SOURCE_ORB_RANGE = "orb_range"      # orb's own risk unit — the status-quo reference row ONLY

EXIT_REASONS: tuple[str, ...] = ("stop", "target", "session_end")

PRE_REGISTRATION_INTERPRETATIONS: tuple[str, ...] = (
    "POPULATION: the orb entry-signal stream, produced by engine.learning.sweep._signals_orb - the "
    "SWEEP'S OWN orb builder, imported and called, never re-derived here. orb admits at most ONE "
    "entry per (symbol, session) (its builder breaks on the first qualifying breakout), so 'one "
    "position per symbol at a time' holds by construction and the entry set is IDENTICAL across all "
    "10 configurations: the geometry changes the EXIT, never the entry. orb parameters = the SS6.3 "
    "envelope DEFAULTS read from config/envelope.yaml (orb_minutes 30, vol_mult 1.5, stop_range_frac "
    "1.0, rr_target 1.5) - WO-17 says 'the orb entry-signal stream', not 'the best orb grid point', "
    "and the default is the champion baseline every sweep unions in.",
    "'ORB'S OWN 1.5R TARGET' UNDER NO-STOP -- THE ONE FLAGGED AMBIGUITY. Ladder (a) pairs a target "
    "with a NO-STOP width, where 'R' has no stop to be measured against. Reading (i): R = ORB'S own "
    "risk unit, stop_range_frac x (signal close - opening-range low), which orb's builder already "
    "stamps as its tp_stop fraction (rr_target x risk/price, rr_target = 1.5 by SS6.3 default). "
    "Reading (ii): R = the WO-17 stop width under test. THIS MODULE IMPLEMENTS (i) (TARGET_SOURCE = "
    "'orb_rr_target'): the WO says orb's OWN 1.5R target; reading (ii) is UNDEFINED for the NO-STOP "
    "row and so cannot produce the pre-registered 10 configs at all; and reading (ii) would move the "
    "TARGET every time the STOP moves, confounding the exact comparison the owner asked for. Under "
    "(i) the target is one fixed level per entry and the stop axis varies alone. The target level is "
    "entry_fill x (1 + tp_frac), anchored at the FILL per the house stop_entry_price='fillprice' "
    "convention WO-2 pinned.",
    "'WOULD HAVE ENDED POSITIVE BY SESSION END' is measured on the REFERENCE PATH: the same entry "
    "held with NO stop and NO target to the session-end square-off - exactly the (NO-STOP, no-target) "
    "configuration, so the diagnostic and the config table are the same simulation. 'Positive' is "
    "read as NET of the full round trip (a gross-positive exit that cannot cover its own costs is not "
    "a recovery worth having); the GROSS variant is reported alongside so the reading is auditable.",
    "MAE (maximum adverse excursion) is measured over the bars whose LOWS a stop could actually have "
    "triggered on: the fill bar through the bar BEFORE the session-end exit fill. That range makes "
    "the two diagnostics exactly consistent - a stop at level S is hit IF AND ONLY IF the path's "
    "minimum low reaches S, so 'MAE >= w x ATR_10m' and 'stopped at width w' are the SAME event (a "
    "unit test pins this). Reported in BOTH percent of the entry fill and multiples of that entry's "
    "own ATR_10m, at p25/p50/p75/p90.",
    "SIMULTANEOUS STOP AND TARGET inside one 1m bar: 1m bars carry no intrabar path, so the ADVERSE "
    "event is assumed first - the stop wins the tie. The house convention (vwap_reversion, sweep).",
    "SESSION-END SQUARE-OFF at the session's LAST bar's OPEN (MIS obligation), matching "
    "vwap_reversion's 'session_end' exit. WO-17's ladders carry no timeout rung, so every unstopped, "
    "un-targeted trade ends here.",
    "ENTRIES WITH NO DEFINED ATR_10m ARE DROPPED FOR EVERY CONFIG, INCLUDING NO-STOP. The stop unit "
    "is also the reporting unit (MAE in ATR multiples) and, decisively, an entry admitted to NO-STOP "
    "but not to the stopped configs would break the common-population requirement that makes the 10 "
    "rows comparable. WO-10b's prior-session seeding is what makes this cheap: seeded, the ATR exists "
    "from 09:25, so ordinary orb morning breakouts are stoppable; only a symbol's FIRST session in "
    "the window falls back to the unseeded 140-minute warm-up and loses its entry (fail-closed).",
    "STOP UNIT = WO-10's ATR_10m, PRIOR-SESSION-SEEDED per WO-10b, IMPORTED from "
    "engine.learning.vwap_reversion (session_atr_10m via compute_session_features(atr_seed=...)) - "
    "not copied, so the 'shared unit' claim WO-17 makes is true by construction. Wilder ATR(14) on "
    "session-local 10-MINUTE buckets, published with a one-bucket availability lag (no lookahead); "
    "the seeded recursion continues from the prior session's final bucket value with the first "
    "bucket's true range computed high-low only (no overnight gap in the stop unit).",
    "STATUS-QUO REFERENCE ROW: orb's own geometry (its range-anchored stop + its 1.5R target) is "
    "simulated as ONE extra row so 'beat the current tight stop margins' has a comparator. It is NOT "
    "one of the pre-registered 10, is excluded from the 10-config table, and is removed entirely by "
    "INCLUDE_STATUS_QUO_REFERENCE = False.",
    "PROMOTION: NOT SOUGHT. ValidationPipeline is never constructed or called; no CPCV, no "
    "fold_pass_min, no WO-3 margin floor, no param_sets row, no validation artifact. WO-17: 'the "
    "deliverable is the stop-policy evidence table; any live stop-guidance change that follows is a "
    "separate owner-approved change (analyst prompt / plan SS6.1), never automatic.'",
)


# --------------------------------------------------------------------------- configuration
@dataclass(frozen=True, slots=True)
class StopConfig:
    """One evaluated geometry: where the stop comes from, and whether the 1.5R target is armed."""

    stop_source: str                    # STOP_SOURCE_ATR | STOP_SOURCE_NONE | STOP_SOURCE_ORB_RANGE
    stop_atr_mult: float | None         # the axis point when ``stop_source == STOP_SOURCE_ATR``
    use_target: bool                    # True = ladder (a) orb's own 1.5R target; False = ladder (b)
    pre_registered: bool = True         # False marks the status-quo reference row

    @property
    def stop_label(self) -> str:
        if self.stop_source == STOP_SOURCE_NONE:
            return "NO-STOP"
        if self.stop_source == STOP_SOURCE_ORB_RANGE:
            return "orb range stop"
        return f"{self.stop_atr_mult:g}x ATR_10m"

    @property
    def ladder_label(self) -> str:
        return "1.5R target + stop" if self.use_target else "no target"

    @property
    def label(self) -> str:
        return f"{self.stop_label} / {self.ladder_label}"

    @property
    def params(self) -> dict[str, Any]:
        """The report JSON view. ``None`` (never ``NaN``) when the width does not apply."""
        return {
            "stop_source": self.stop_source,
            "stop_atr_mult": None if self.stop_atr_mult is None else float(self.stop_atr_mult),
            "use_target": bool(self.use_target),
        }


def grid_configs() -> list[StopConfig]:
    """The pre-registered 5 x 2 = 10 configurations, in axis order (widest-first is NOT the order)."""
    out: list[StopConfig] = []
    for mult in STOP_AXIS:
        for use_target in LADDERS:
            out.append(
                StopConfig(
                    stop_source=STOP_SOURCE_ATR if mult is not None else STOP_SOURCE_NONE,
                    stop_atr_mult=mult,
                    use_target=use_target,
                )
            )
    return out


def status_quo_config() -> StopConfig:
    """orb AS IT STANDS: its range-anchored stop + its 1.5R target. NOT a pre-registered config."""
    return StopConfig(
        stop_source=STOP_SOURCE_ORB_RANGE, stop_atr_mult=None, use_target=True, pre_registered=False
    )


def all_configs() -> list[StopConfig]:
    """The 10 pre-registered configs, plus the status-quo reference row when it is switched on."""
    configs = grid_configs()
    if INCLUDE_STATUS_QUO_REFERENCE:
        configs.append(status_quo_config())
    return configs


#: The reference path every diagnostic is measured against: NO stop, NO target, held to session end.
REFERENCE_CONFIG = StopConfig(stop_source=STOP_SOURCE_NONE, stop_atr_mult=None, use_target=False)


def orb_default_params(envelope: Mapping[str, dict[str, Any]] | None = None) -> dict[str, float]:
    """orb's §6.3 envelope DEFAULTS, read from ``config/envelope.yaml`` (never hardcoded here).

    Returns the bare (un-namespaced) parameter names ``_signals_orb`` consumes: ``orb_minutes``,
    ``vol_mult``, ``stop_range_frac``, ``rr_target``. Reading them through
    :func:`engine.learning.sweep.load_envelope` means this study and the sweep cannot disagree about
    what "the orb baseline" is.
    """
    env = load_envelope() if envelope is None else dict(envelope)
    prefix = "orb."
    rows = {k[len(prefix):]: v for k, v in env.items() if k.startswith(prefix)}
    if not rows:
        raise ValueError("no §6.3 envelope parameters for strategy 'orb'")
    return {name: float(spec["default"]) for name, spec in sorted(rows.items())}


def orb_per_side_fee(cost_model: CostModel) -> float:
    """½ x round-trip STATUTORY-fee breakeven at Rs20,000 MIS, as a fraction — orb's ``fee`` input.

    Mirrors ``SweepRunner._per_side_fee`` (sweep.py:331-340) exactly: FEES ONLY
    (``fee_breakeven_pct``), because ``_signals_orb`` consumes it solely to compute its C3
    sub-cost-floor suppression (``min_risk_frac = 4 x fee``). Passing the spread-inclusive
    ``breakeven_pct`` here would silently tighten orb's entry filter and change the population.
    """
    be_pct = float(cost_model.fee_breakeven_pct(REFERENCE_NOTIONAL, PRODUCT))
    return be_pct / 100.0 / 2.0


# --------------------------------------------------------------------------- the orb entry stream
def orb_signal_stream(
    symbol: str, frame: pd.DataFrame, *, params: Mapping[str, float], fee: float
) -> pd.DataFrame:
    """The orb entry signals for ONE symbol's multi-session 1m frame — built by orb's OWN builder.

    ``frame`` is a single symbol's ascending, tz-aware IST 1m OHLCV frame (optionally carrying the
    ``auction_open`` column the sweep unions into the opening range). Returns a frame indexed by the
    **SIGNAL bar's** timestamp (the bar whose CLOSE cleared the opening range — the entry FILLS one
    bar later, requirement 2) with two columns:

    * ``stop_frac``  — orb's own ``sl_stop`` fraction: ``stop_range_frac x (close - range_low)/close``;
    * ``target_frac`` — orb's own ``tp_stop`` fraction: ``rr_target x`` that, i.e. THE 1.5R TARGET.

    Nothing about the breakout rule (opening range, volume multiple, entry window, the C3
    sub-cost-floor skip, the one-entry-per-session ``break``) is re-implemented here:
    :func:`engine.learning.sweep._signals_orb` is called directly, so this study's population is the
    sweep's population by construction (design requirement 1).
    """
    cols = {f: pd.DataFrame({symbol: frame[f]}) for f in ("close", "high", "low", "open", "volume")}
    auction = (
        pd.DataFrame({symbol: frame["auction_open"]}) if "auction_open" in frame.columns else None
    )
    frames = _Frames(intraday=True, auction_open=auction, **cols)
    sig = _signals_orb(frames, params, fee)
    entries = sig.entries[symbol].to_numpy(dtype=bool)
    pos = np.flatnonzero(entries)
    idx = pd.DatetimeIndex(frame.index)
    # sl_stop/tp_stop are DataFrames here (orb always stamps per-signal fractions); mypy sees the
    # union declared on ``_Signals``, so read them defensively rather than asserting a type.
    sl = np.asarray(sig.sl_stop[symbol].to_numpy(dtype="float64")) if sig.sl_stop is not None else None
    tp = np.asarray(sig.tp_stop[symbol].to_numpy(dtype="float64")) if sig.tp_stop is not None else None
    return pd.DataFrame(
        {
            "stop_frac": sl[pos] if sl is not None else np.full(len(pos), np.nan),
            "target_frac": tp[pos] if tp is not None else np.full(len(pos), np.nan),
        },
        index=idx[pos],
    )


# --------------------------------------------------------------------------- one entry's price path
@dataclass(frozen=True, slots=True)
class EntryContext:
    """ONE orb entry, with every price path the 10 configurations need — built once, reused by all.

    Positional convention (requirement 2). ``e`` is the FILL bar (signal bar + 1) and ``hard`` is the
    session's LAST bar, whose OPEN is the MIS square-off price. A stop/target trigger is looked for on
    bars ``e .. hard-1`` (:attr:`trigger_high` / :attr:`trigger_low`) and fills at the NEXT bar's open
    (:attr:`fill_open`, bars ``e+1 .. hard``, index-aligned with the trigger arrays). The last element
    of :attr:`fill_open` is therefore the session-end square-off price for a path that never triggers.
    """

    symbol: str
    session: date
    signal_ts: pd.Timestamp
    entry_ts: pd.Timestamp
    entry_price: float
    atr_10m: float                      # the stop unit, read at the SIGNAL bar (no lookahead)
    orb_stop_frac: float                # orb's own risk/price — the status-quo reference row's stop
    orb_target_frac: float              # orb's own rr_target x risk/price — THE 1.5R target
    trigger_ts: pd.DatetimeIndex        # bars e .. hard-1
    trigger_high: np.ndarray
    trigger_low: np.ndarray
    fill_ts: pd.DatetimeIndex           # bars e+1 .. hard
    fill_open: np.ndarray

    @property
    def worst_low(self) -> float:
        """Lowest low over the bars a stop could have triggered on (the MAE anchor)."""
        return float(np.nanmin(self.trigger_low))

    @property
    def mae_pct(self) -> float:
        """Maximum adverse excursion, percent of the entry FILL. Never negative (clipped at 0)."""
        return max(0.0, (self.entry_price - self.worst_low) / self.entry_price * 100.0)

    @property
    def mae_atr_mult(self) -> float:
        """Maximum adverse excursion in multiples of THIS entry's ATR_10m — the stop-axis unit."""
        return max(0.0, (self.entry_price - self.worst_low) / self.atr_10m)

    @property
    def session_end_price(self) -> float:
        return float(self.fill_open[-1])


def build_entry_context(
    features: SessionFeatures,
    signal_pos: int,
    *,
    orb_stop_frac: float,
    orb_target_frac: float,
) -> EntryContext | None:
    """Assemble the shared price path for one orb signal, or ``None`` if the entry is not bookable.

    Rejects (each fail-closed, none of them silent in aggregate — the report counts them):

    * a signal on the session's LAST usable bar — there is no bar left to fill it in (requirement 2);
    * a non-finite / non-positive fill or square-off price;
    * a session with no bar left after the fill to exit into;
    * **a non-finite or non-positive ATR_10m at the signal bar** — dropped for EVERY configuration,
      including NO-STOP, so all 10 rows describe the same trades (see the interpretations block).
    """
    n = features.n
    t = int(signal_pos)
    e = t + 1
    hard = n - 1                                     # the session's LAST bar: the MIS square-off
    if e >= n or hard <= e:
        return None                                  # no fill bar, or no bar left to exit into
    atr = float(features.atr_10m[t])
    if not np.isfinite(atr) or atr <= 0.0:
        return None                                  # no stop unit ⇒ this entry is not in the study
    entry_price = float(features.open[e])
    if not np.isfinite(entry_price) or entry_price <= 0.0:
        return None
    if not np.isfinite(float(features.open[hard])):
        return None
    trig = slice(e, hard)                            # trigger bars e .. hard-1
    fill = slice(e + 1, hard + 1)                    # their fills, e+1 .. hard (index-aligned)
    with np.errstate(invalid="ignore"):
        worst_low = float(np.nanmin(features.low[trig])) if np.isfinite(features.low[trig]).any() else np.nan
    if not np.isfinite(worst_low):
        return None                                  # no usable low ⇒ MAE undefined ⇒ not bookable
    return EntryContext(
        symbol=features.symbol,
        session=features.session,
        signal_ts=features.ts[t],
        entry_ts=features.ts[e],
        entry_price=entry_price,
        atr_10m=atr,
        orb_stop_frac=float(orb_stop_frac),
        orb_target_frac=float(orb_target_frac),
        trigger_ts=features.ts[trig],
        trigger_high=features.high[trig],
        trigger_low=features.low[trig],
        fill_ts=features.ts[fill],
        fill_open=features.open[fill],
    )


# --------------------------------------------------------------------------- one simulated trade
@dataclass(frozen=True, slots=True)
class StopTrade:
    """One simulated long round trip under ONE geometry, carrying its own NO-STOP reference path.

    The reference fields are what make the DODGED-WINNER diagnostic a property of a trade rather than
    a join across two result sets: every stopped trade already knows what its own unstopped twin did.
    """

    symbol: str
    session: date
    signal_ts: pd.Timestamp
    entry_ts: pd.Timestamp
    exit_ts: pd.Timestamp
    entry_price: float
    exit_price: float
    stop_price: float | None
    target_price: float | None
    exit_reason: str                    # "stop" | "target" | "session_end"
    holding_minutes: float
    atr_10m: float
    mae_pct: float                      # of the entry fill
    mae_atr_mult: float                 # in ATR_10m units
    gross_return_pct: float
    net_return_pct: float               # gross - the FULL round-trip cost floor (fees + spread)
    #: The SAME entry held with NO stop and NO target to the session-end square-off.
    reference_gross_pct: float
    reference_net_pct: float


def stop_level(ctx: EntryContext, config: StopConfig) -> float | None:
    """The stop PRICE for ``config`` on ``ctx``, or ``None`` for the NO-STOP rung.

    Anchored at the entry FILL, matching the house ``stop_entry_price='fillprice'`` convention WO-2
    pinned (sweep.py:643-644): the signal bar's close is no longer the price paid once fills moved to
    the next bar's open.
    """
    if config.stop_source == STOP_SOURCE_NONE:
        return None
    if config.stop_source == STOP_SOURCE_ORB_RANGE:
        return ctx.entry_price * (1.0 - ctx.orb_stop_frac)
    return ctx.entry_price - float(config.stop_atr_mult) * ctx.atr_10m


def target_level(ctx: EntryContext, config: StopConfig) -> float | None:
    """The 1.5R target PRICE for ladder (a), or ``None`` for ladder (b).

    ``TARGET_SOURCE == "orb_rr_target"`` (the flagged interpretation): R is ORB'S OWN risk unit, so
    the target is ``entry_fill x (1 + tp_frac)`` where ``tp_frac`` is the fraction orb's own builder
    stamped (``rr_target x stop_range_frac x (close - range_low) / close``). It does NOT depend on the
    stop width under test, which is what lets the NO-STOP rung carry a target at all and keeps the
    stop axis unconfounded.
    """
    if not config.use_target:
        return None
    if TARGET_SOURCE != "orb_rr_target":             # pragma: no cover - the switch point, documented
        raise ValueError(f"unknown TARGET_SOURCE {TARGET_SOURCE!r}")
    frac = ctx.orb_target_frac
    if not np.isfinite(frac) or frac <= 0.0:
        return None                                  # orb stamped no usable target ⇒ ladder degrades
    return ctx.entry_price * (1.0 + frac)


def simulate_entry(ctx: EntryContext, config: StopConfig, *, cost_floor_pct: float) -> StopTrade:
    """Walk ONE entry under ONE geometry. Pure and deterministic (§9.6).

    * the stop/target trigger is looked for on bars ``e .. hard-1`` and the exit fills at the NEXT
      bar's OPEN (requirement 2);
    * **adverse-first**: a bar that touches BOTH levels is booked as a STOP — 1m bars carry no
      intrabar path, so the adverse event is assumed first (the house convention);
    * no trigger ⇒ the session-end square-off at the last bar's OPEN, reason ``session_end``;
    * the FULL round trip is charged on every trade, stop-outs included.
    """
    stop_price = stop_level(ctx, config)
    target_price = target_level(ctx, config)

    with np.errstate(invalid="ignore"):
        stop_hit = (
            ctx.trigger_low <= stop_price
            if stop_price is not None
            else np.zeros(len(ctx.trigger_low), dtype=bool)
        )
        target_hit = (
            ctx.trigger_high >= target_price
            if target_price is not None
            else np.zeros(len(ctx.trigger_high), dtype=bool)
        )
    trigger = stop_hit | target_hit
    if bool(trigger.any()):
        k = int(np.argmax(trigger))
        exit_price = float(ctx.fill_open[k])
        exit_ts = ctx.fill_ts[k]
        exit_reason = "stop" if bool(stop_hit[k]) else "target"    # adverse wins the tie
    else:
        exit_price = ctx.session_end_price
        exit_ts = ctx.fill_ts[-1]
        exit_reason = "session_end"

    gross = (exit_price - ctx.entry_price) / ctx.entry_price * 100.0
    ref_gross = (ctx.session_end_price - ctx.entry_price) / ctx.entry_price * 100.0
    return StopTrade(
        symbol=ctx.symbol,
        session=ctx.session,
        signal_ts=ctx.signal_ts,
        entry_ts=ctx.entry_ts,
        exit_ts=exit_ts,
        entry_price=ctx.entry_price,
        exit_price=exit_price,
        stop_price=stop_price,
        target_price=target_price,
        exit_reason=exit_reason,
        holding_minutes=float(pd.Timestamp(exit_ts).value - pd.Timestamp(ctx.entry_ts).value) / 6.0e10,
        atr_10m=ctx.atr_10m,
        mae_pct=ctx.mae_pct,
        mae_atr_mult=ctx.mae_atr_mult,
        gross_return_pct=gross,
        net_return_pct=gross - float(cost_floor_pct),
        reference_gross_pct=ref_gross,
        reference_net_pct=ref_gross - float(cost_floor_pct),
    )


def entry_contexts_for_symbol(
    symbol: str,
    frame: pd.DataFrame,
    *,
    orb_params: Mapping[str, float],
    orb_fee: float,
    seed_atr_prior_session: bool = SEED_ATR_PRIOR_SESSION,
) -> tuple[list[EntryContext], list[date]]:
    """Every bookable orb entry for one symbol, plus the sessions that had bars.

    The WO-10b seeding chain is order-dependent, so sessions are walked in date order and each seeds
    the next from :attr:`SessionFeatures.atr_10m_final` — the same chain
    ``vwap_reversion.simulate_symbol`` drives, using the same public entry points (a session that
    yields no ATR leaves the carry untouched; a symbol's first session has no seed and falls back to
    the unseeded warm-up).

    Signals are matched to sessions by **TIMESTAMP**, never by position:
    :func:`~engine.learning.vwap_reversion.compute_session_features` drops out-of-session bars, so
    positional indices into the raw frame and into the feature arrays are not interchangeable.
    """
    if not frame.index.is_monotonic_increasing:
        frame = frame.sort_index()                   # the seeding chain is order-dependent
    signals = orb_signal_stream(symbol, frame, params=orb_params, fee=orb_fee)
    contexts: list[EntryContext] = []
    sessions: list[date] = []
    seed: float | None = None
    for start, stop in session_slices(pd.DatetimeIndex(frame.index)):
        feats = compute_session_features(
            symbol, frame.iloc[start:stop], atr_seed=seed if seed_atr_prior_session else None
        )
        if feats is None:
            continue
        sessions.append(feats.session)
        if seed_atr_prior_session and feats.atr_10m_final is not None:
            seed = feats.atr_10m_final
        day = signals[pd.DatetimeIndex(signals.index).date == feats.session]
        if not len(day):
            continue
        sig_ts = pd.Timestamp(day.index[0])          # orb books at most ONE entry per symbol-session
        loc = int(feats.ts.searchsorted(sig_ts))
        if loc >= feats.n or feats.ts[loc] != sig_ts:
            continue                                 # the signal bar was clamped out of the session
        ctx = build_entry_context(
            feats,
            loc,
            orb_stop_frac=float(day["stop_frac"].iloc[0]),
            orb_target_frac=float(day["target_frac"].iloc[0]),
        )
        if ctx is not None:
            contexts.append(ctx)
    return contexts, sessions


# --------------------------------------------------------------------------- accumulation
def _is_dodged_winner(trade: StopTrade) -> bool:
    """THE DODGED-WINNER TEST (WO-17's first diagnostic) — one definition, in one place.

    A trade is a DODGED WINNER when the geometry **stopped it out** and the SAME entry, held with NO
    STOP and NO TARGET to the session-end square-off, would have ended **NET-positive after the full
    round-trip cost**. That is exactly the owner's hypothesis made countable: the dip was noise, the
    position would have recovered to a positive sell, and the stop converted it into a realized loss
    plus a full round trip of friction.

    "Positive" is NET, not gross: a recovery that does not cover its own costs is not a recovery worth
    having. :func:`_is_dodged_winner_gross` reports the gross reading alongside so the choice is
    auditable rather than hidden.
    """
    return trade.exit_reason == "stop" and trade.reference_net_pct > 0.0


def _is_dodged_winner_gross(trade: StopTrade) -> bool:
    """The GROSS reading of :func:`_is_dodged_winner` — reported for audit, never the headline."""
    return trade.exit_reason == "stop" and trade.reference_gross_pct > 0.0


@dataclass
class ConfigAccumulator:
    """Streaming aggregate for ONE geometry over the whole population."""

    config: StopConfig
    n_trades: int = 0
    net_returns: list[float] = field(default_factory=list)
    gross_returns: list[float] = field(default_factory=list)
    exit_counts: dict[str, int] = field(default_factory=lambda: dict.fromkeys(EXIT_REASONS, 0))
    n_dodged_winners: int = 0
    n_dodged_winners_gross: int = 0

    def add(self, trade: StopTrade) -> None:
        self.n_trades += 1
        self.net_returns.append(trade.net_return_pct)
        self.gross_returns.append(trade.gross_return_pct)
        self.exit_counts[trade.exit_reason] = self.exit_counts.get(trade.exit_reason, 0) + 1
        if _is_dodged_winner(trade):
            self.n_dodged_winners += 1
        if _is_dodged_winner_gross(trade):
            self.n_dodged_winners_gross += 1

    @property
    def n_stopped(self) -> int:
        return self.exit_counts.get("stop", 0)

    @property
    def expectancy_pct(self) -> float | None:
        """MEAN PER-TRADE NET RETURN after the full round-trip cost, in percent."""
        return float(np.mean(self.net_returns)) if self.net_returns else None

    @property
    def gross_expectancy_pct(self) -> float | None:
        return float(np.mean(self.gross_returns)) if self.gross_returns else None

    @property
    def win_rate(self) -> float | None:
        """Fraction of trades whose NET return is > 0."""
        if not self.net_returns:
            return None
        return float(np.mean(np.asarray(self.net_returns) > 0.0))

    @property
    def dodged_winner_fraction(self) -> float | None:
        """Of the trades this geometry STOPPED, the share whose unstopped twin ended net-positive."""
        return None if self.n_stopped == 0 else self.n_dodged_winners / self.n_stopped

    @property
    def dodged_winner_fraction_gross(self) -> float | None:
        return None if self.n_stopped == 0 else self.n_dodged_winners_gross / self.n_stopped


def _quantiles(values: Sequence[float]) -> dict[str, float] | None:
    """``{"p25":…, "p50":…, "p75":…, "p90":…}`` (numpy linear interpolation), or ``None`` if empty."""
    if not len(values):
        return None
    arr = np.asarray(values, dtype="float64")
    return {f"p{q:g}": float(np.percentile(arr, q)) for q in MAE_QUANTILES}


# --------------------------------------------------------------------------- report models
class MaeDistribution(BaseModel):
    """The MAE distribution of ONE population slice, in both units (WO-17's second diagnostic)."""

    model_config = ConfigDict(frozen=True)

    label: str
    n: int
    mean_pct: float | None
    #: p25/p50/p75/p90 of the maximum adverse excursion, in PERCENT of the entry fill price.
    pct: dict[str, float] | None
    #: …and in multiples of that entry's own ATR_10m — the stop axis' own unit.
    atr_mult: dict[str, float] | None


class ConfigResult(BaseModel):
    """One row of the evidence table."""

    model_config = ConfigDict(frozen=True)

    label: str
    stop_source: str
    stop_atr_mult: float | None
    use_target: bool
    pre_registered: bool
    n_trades: int
    win_rate: float | None
    expectancy_pct: float | None            # mean per-trade NET return, %
    gross_expectancy_pct: float | None
    exit_counts: dict[str, int]
    n_stopped: int
    #: DIAGNOSTIC 1 — of the trades STOPPED here, the share whose unstopped twin ended net-positive.
    dodged_winner_fraction: float | None
    dodged_winner_fraction_gross: float | None
    n_dodged_winners: int


class StopGeometryReport(BaseModel):
    """The single self-contained WO-17 deliverable (json + md). Nothing else is written."""

    model_config = ConfigDict(frozen=True)

    banner: str = BANNER
    experiment_id: str = EXPERIMENT_ID
    #: ``COMPLETED`` | ``NO_DATA`` | ``NO_ENTRIES``
    status: str
    null_hypothesis: str = NULL_HYPOTHESIS
    owner_question: str = OWNER_QUESTION
    requested_start: date
    requested_end: date
    data_start: date | None
    data_end: date | None
    n_symbols: int
    symbols: list[str]
    n_sessions: int
    n_entries: int                          # the COMMON population every config was run on
    product: str = PRODUCT
    reference_notional: str = str(REFERENCE_NOTIONAL)
    cost_floor_pct: float
    fill_mechanics: str = FILL_MECHANICS
    orb_params: dict[str, float]
    seed_atr_prior_session: bool = SEED_ATR_PRIOR_SESSION
    n_configs: int = N_CONFIGS
    configs: list[ConfigResult] = Field(default_factory=list)
    #: DIAGNOSTIC 2 — winners first (the pre-registered slice), then the audit/context slices.
    mae_distributions: list[MaeDistribution] = Field(default_factory=list)
    reference_win_rate: float | None = None         # share of entries net-positive under NO-STOP
    plain_language_answer: list[str] = Field(default_factory=list)
    modelling_notes: list[str] = Field(default_factory=list)
    generated_at: datetime

    @property
    def pre_registered_configs(self) -> list[ConfigResult]:
        return [c for c in self.configs if c.pre_registered]


# --------------------------------------------------------------------------- modelling notes
def modelling_notes(cost_floor_pct: float, *, spread_pct: float | None = None) -> list[str]:
    """Every pre-registered choice, in the report (design requirement 5). Pure; no I/O."""
    widths = ", ".join(f"{m:g}" for m in STOP_ATR_MULTIPLES)
    notes = [
        f"PRE-REGISTRATION: IMPROVEMENT_SPEC.md WO-17. Stop axis = {{{widths}}} x ATR_10m PLUS "
        f"NO-STOP; exit ladders = (a) orb's own 1.5R target + the stop, (b) no target (session-end "
        f"square-off or the stop). {len(STOP_AXIS)} widths x {len(LADDERS)} ladders = {N_CONFIGS} "
        "configurations, FIXED, never swept - that plus the no-promotion rule is what contains the "
        "multiplicity.",
        f"POPULATION: the orb entry-signal stream over the full available 1m window, entries at the "
        f"NEXT 1m bar's OPEN (WO-2). orb's entry window is {ORB_ENTRY_START:%H:%M}-"
        f"{ORB_ENTRY_END:%H:%M} and its builder takes at most ONE breakout per symbol-session, so "
        "every configuration is evaluated on the SAME entries - the geometry changes only the exit.",
        f"STOP UNIT: Wilder ATR({ATR_PERIOD}) on session-local {ATR_RESAMPLE_MINUTES}-MINUTE buckets "
        "(WO-10's unit, manager ruling 2026-08-13), PRIOR-SESSION-SEEDED per WO-10b so morning "
        f"entries are stoppable: seeded, the ATR exists one bucket in ({ATR_RESAMPLE_MINUTES} min) "
        f"instead of after the unseeded {ATR_WARMUP_MINUTES}-minute warm-up. IMPORTED from "
        "engine.learning.vwap_reversion, not copied - WO-17 calls it a shared unit and this keeps "
        f"that true. seed_atr_prior_session = {SEED_ATR_PRIOR_SESSION}.",
        f"COSTS (WO-2 corrected surface, INCLUDING the measured spread): product {PRODUCT}, per-trade "
        f"notional Rs{REFERENCE_NOTIONAL}, full round-trip friction {cost_floor_pct:.4f}% = "
        f"CostModel.breakeven_pct(Rs{REFERENCE_NOTIONAL}, '{PRODUCT}'). Charged IN FULL on EVERY "
        "simulated trade, stop-outs included - WO-17: 'each stop-out realizes the full round-trip "
        "cost floor'. net = gross - the floor.",
        "FILLS: NEXT-1m-BAR OPEN, never the signal bar. The breakout is read from bar t's COMPLETED "
        "close and the entry fills at bar t+1's OPEN; a stop or target observed on bar i fills at bar "
        "i+1's OPEN. A signal on a session's LAST usable bar is dropped, never carried into the next "
        "session. The session-end square-off is the session's LAST bar's OPEN (MIS obligation).",
        "PROMOTION IS NOT SOUGHT (WO-17, verbatim): 'the deliverable is the stop-policy evidence "
        "table; any live stop-guidance change that follows is a separate owner-approved change "
        "(analyst prompt / plan SS6.1), never automatic.' ValidationPipeline is never called - no "
        "CPCV, no fold_pass_min, no WO-3 margin floor, no param_sets row, no validation artifact.",
        "SCOPE: C-category diagnostic. No live wiring; STOPS FOR OWNER REVIEW. A null result (all "
        "geometries equal) is a valid, reportable deliverable (C9) and is reported as measured.",
    ]
    if spread_pct is not None:
        notes.append(
            f"Spread provenance: config/costs.yaml spread_pct = {spread_pct:.4f}% (full quoted "
            "spread; NIFTY200 median over 48 symbol-days - IMPROVEMENT_SPEC.md Part III). A round "
            "trip pays it once."
        )
    notes.append("--- INTERPRETATIONS where the WO text was under-determined (auditable, not implicit) ---")
    notes.extend(PRE_REGISTRATION_INTERPRETATIONS)
    return notes


# --------------------------------------------------------------------------- plain-language answer
def plain_language_answer(
    configs: Sequence[ConfigResult], mae: Sequence[MaeDistribution], *, cost_floor_pct: float
) -> list[str]:
    """WO-17's acceptance item: "a plain-language answer to the owner's question".

    Derived mechanically from the measured rows so it cannot drift from the table above it. It answers
    the question that was asked - wider vs tighter - and says plainly when the answer is "no
    difference", which under the pre-registered null is the expected outcome.
    """
    lines: list[str] = [f"QUESTION: {OWNER_QUESTION}", ""]
    traded = [c for c in configs if c.pre_registered and c.n_trades > 0 and c.expectancy_pct is not None]
    if not traded:
        lines.append(
            "ANSWER: UNANSWERABLE on this data - no orb entry in the window survived to a simulated "
            "trade, so no geometry was measured. This is a data/coverage statement, not evidence "
            "about stops."
        )
        return lines

    best = max(traded, key=lambda c: c.expectancy_pct or 0.0)
    worst = min(traded, key=lambda c: c.expectancy_pct or 0.0)
    stopped_rows = [c for c in traded if c.stop_atr_mult is not None and not c.use_target]
    stopped_rows.sort(key=lambda c: c.stop_atr_mult or 0.0)
    tightest = stopped_rows[0] if stopped_rows else None
    widest = stopped_rows[-1] if stopped_rows else None
    spread = (best.expectancy_pct or 0.0) - (worst.expectancy_pct or 0.0)

    lines.append(
        f"Every geometry was run on the SAME {traded[0].n_trades} orb entries; only the exit differed. "
        f"Best net expectancy: {best.label} at {best.expectancy_pct:+.5f}%/trade. Worst: "
        f"{worst.label} at {worst.expectancy_pct:+.5f}%/trade. Spread across the 10 pre-registered "
        f"geometries: {spread:.5f} percentage points per trade, against a round-trip cost floor of "
        f"{cost_floor_pct:.4f}%."
    )
    if tightest is not None and widest is not None:
        delta = (widest.expectancy_pct or 0.0) - (tightest.expectancy_pct or 0.0)
        direction = "BETTER" if delta > 0 else ("WORSE" if delta < 0 else "IDENTICAL")
        lines.append(
            f"Directly on the owner's axis (stop-only ladder, no target): widening the stop from "
            f"{tightest.stop_atr_mult:g}x ATR_10m to {widest.stop_atr_mult:g}x ATR_10m is {direction} "
            f"by {delta:+.5f}%/trade ({tightest.expectancy_pct:+.5f}% -> "
            f"{widest.expectancy_pct:+.5f}%). Stop-outs fell from {tightest.n_stopped} to "
            f"{widest.n_stopped} of {tightest.n_trades} entries."
        )
        if tightest.dodged_winner_fraction is not None:
            lines.append(
                f"DODGED WINNERS: at the tight {tightest.stop_atr_mult:g}x stop, "
                f"{tightest.dodged_winner_fraction:.1%} of stop-outs "
                f"({tightest.n_dodged_winners}/{tightest.n_stopped}) would have ended the session "
                f"NET-POSITIVE had they simply been left alone - that is the owner's 'noise-hit "
                f"positions recover' effect, counted."
            )
        if widest.dodged_winner_fraction is not None:
            lines.append(
                f"At the wide {widest.stop_atr_mult:g}x stop the same figure is "
                f"{widest.dodged_winner_fraction:.1%} ({widest.n_dodged_winners}/{widest.n_stopped}) "
                "- a wider stop stops fewer trades, but the ones it does stop are the deeper, less "
                "recoverable dips."
            )
    winners = next((m for m in mae if m.label == "eventual winners (NO-STOP net-positive)"), None)
    if winners is not None and winners.atr_mult is not None and winners.n:
        lines.append(
            f"WHAT DIP DEPTH WINNERS SURVIVE: of the {winners.n} entries that ended net-positive "
            f"under NO-STOP, the worst adverse excursion was {winners.atr_mult['p50']:.2f}x ATR_10m "
            f"at the median and {winners.atr_mult['p90']:.2f}x at p90 "
            f"({winners.pct['p50']:.3f}% / {winners.pct['p90']:.3f}% of entry price). A stop tighter "
            "than the p90 figure is, by construction, cutting off roughly a tenth of the eventual "
            "winners."
        )
    lines.append("")
    lines.append(
        "READ THIS AGAINST THE NULL: under optional stopping a driftless path gives every geometry "
        "the SAME pre-cost expectancy, so a spread near zero is the EXPECTED result and is evidence "
        "of no post-dip conditional drift - not evidence that the harness failed. Only a spread that "
        "is large relative to the cost floor is evidence for the owner's hypothesis."
    )
    lines.append(
        "SCOPE: this is a diagnostic. It does NOT authorise a live stop change; WO-17 requires that "
        "to be a separate owner-approved change to the analyst prompt / plan SS6.1."
    )
    return lines


# --------------------------------------------------------------------------- the experiment runner
#: ``symbol -> 1m OHLCV frame or None``. Injectable so the offline test tier never opens the store.
FrameLoader = Callable[[str], "pd.DataFrame | None"]


class StopGeometryExperiment:
    """Runs the WO-17 diagnostic and returns the report. Blocking/standalone by design.

    Parameters
    ----------
    loader:
        ``symbol -> 1m OHLCV frame`` (tz-aware IST index, columns open/high/low/close/volume, plus
        ``auction_open`` when the store supplies it). :meth:`from_store` builds the production loader
        over ``MarketStore.get_bars_1m_frame`` — the same read path ``SweepRunner``'s intraday branch
        and WO-10's harness use (read-only).
    cost_floor_pct:
        The FULL round-trip friction at Rs20,000 MIS, in percent (fees + spread) — from
        :func:`engine.learning.vwap_reversion.cost_floor_pct_for`.
    orb_fee:
        orb's per-side STATUTORY fee fraction, the ``fee`` argument its builder consumes for the C3
        sub-cost-floor suppression — from :func:`orb_per_side_fee`.
    """

    def __init__(
        self,
        *,
        loader: FrameLoader,
        cost_floor_pct: float,
        orb_fee: float,
        clock: Clock,
        orb_params: Mapping[str, float] | None = None,
        spread_pct: float | None = None,
    ) -> None:
        self._loader = loader
        self._cost_floor_pct = float(cost_floor_pct)
        self._orb_fee = float(orb_fee)
        self._clock = clock
        self._orb_params = dict(orb_params) if orb_params is not None else orb_default_params()
        self._spread_pct = spread_pct

    # ------------------------------------------------------------------ construction helpers
    @classmethod
    def from_store(
        cls,
        store: MarketStore,
        cost_model: CostModel,
        clock: Clock,
        *,
        start: date,
        end: date,
        orb_params: Mapping[str, float] | None = None,
    ) -> StopGeometryExperiment:
        """Production wiring: the 1m bulk read path ``SweepRunner`` uses, READ-ONLY (never writes)."""

        def loader(symbol: str) -> pd.DataFrame | None:
            start_dt = datetime.combine(start, time(0, 0), tzinfo=IST)
            end_dt = datetime.combine(end + timedelta(days=1), time(0, 0), tzinfo=IST)
            df = store.get_bars_1m_frame(symbol, start_dt, end_dt)
            return df if len(df) else None

        return cls(
            loader=loader,
            cost_floor_pct=cost_floor_pct_for(cost_model),
            orb_fee=orb_per_side_fee(cost_model),
            clock=clock,
            orb_params=orb_params,
            spread_pct=float(cost_model.spread_pct),
        )

    # ------------------------------------------------------------------ the run
    def run(self, symbols: Sequence[str], *, start: date, end: date) -> StopGeometryReport:
        """One pass: build the common entry population, then walk all configs over every entry."""
        syms = list(dict.fromkeys(symbols))
        configs = all_configs()
        accs = {cfg: ConfigAccumulator(config=cfg) for cfg in configs}
        sessions_seen: set[date] = set()
        seen: list[str] = []
        n_entries = 0
        # MAE slices, keyed by the NO-STOP reference verdict (diagnostic 2).
        mae_winners: list[tuple[float, float]] = []
        mae_losers: list[tuple[float, float]] = []

        for sym in syms:
            frame = self._loader(sym)
            if frame is None or len(frame) == 0:
                continue
            seen.append(sym)
            contexts, sessions = entry_contexts_for_symbol(
                sym, frame, orb_params=self._orb_params, orb_fee=self._orb_fee
            )
            sessions_seen.update(sessions)
            for ctx in contexts:
                n_entries += 1
                reference = simulate_entry(ctx, REFERENCE_CONFIG, cost_floor_pct=self._cost_floor_pct)
                bucket = mae_winners if reference.net_return_pct > 0.0 else mae_losers
                bucket.append((ctx.mae_pct, ctx.mae_atr_mult))
                for cfg in configs:
                    accs[cfg].add(simulate_entry(ctx, cfg, cost_floor_pct=self._cost_floor_pct))

        span = (min(sessions_seen), max(sessions_seen)) if sessions_seen else (None, None)
        status = "COMPLETED"
        if not seen:
            status = "NO_DATA"
        elif n_entries == 0:
            status = "NO_ENTRIES"

        results = [self._result(accs[cfg]) for cfg in configs]
        mae = self._mae_distributions(mae_winners, mae_losers)
        answer = plain_language_answer(results, mae, cost_floor_pct=self._cost_floor_pct)
        reference_win_rate = (
            len(mae_winners) / n_entries if n_entries else None
        )
        _log.info(
            "stop_geometry_done",
            n_symbols=len(seen),
            n_entries=n_entries,
            n_configs=len(results),
            status=status,
        )
        return StopGeometryReport(
            status=status,
            requested_start=start,
            requested_end=end,
            data_start=span[0],
            data_end=span[1],
            n_symbols=len(seen),
            symbols=seen,
            n_sessions=len(sessions_seen),
            n_entries=n_entries,
            cost_floor_pct=round(self._cost_floor_pct, 6),
            orb_params=dict(self._orb_params),
            configs=results,
            mae_distributions=mae,
            reference_win_rate=reference_win_rate,
            plain_language_answer=answer,
            modelling_notes=modelling_notes(self._cost_floor_pct, spread_pct=self._spread_pct),
            generated_at=self._clock.now(),
        )

    # ------------------------------------------------------------------ internals
    @staticmethod
    def _result(acc: ConfigAccumulator) -> ConfigResult:
        cfg = acc.config
        return ConfigResult(
            label=cfg.label,
            stop_source=cfg.stop_source,
            stop_atr_mult=cfg.stop_atr_mult,
            use_target=cfg.use_target,
            pre_registered=cfg.pre_registered,
            n_trades=acc.n_trades,
            win_rate=acc.win_rate,
            expectancy_pct=acc.expectancy_pct,
            gross_expectancy_pct=acc.gross_expectancy_pct,
            exit_counts=dict(sorted(acc.exit_counts.items())),
            n_stopped=acc.n_stopped,
            dodged_winner_fraction=acc.dodged_winner_fraction,
            dodged_winner_fraction_gross=acc.dodged_winner_fraction_gross,
            n_dodged_winners=acc.n_dodged_winners,
        )

    @staticmethod
    def _mae_distributions(
        winners: Sequence[tuple[float, float]], losers: Sequence[tuple[float, float]]
    ) -> list[MaeDistribution]:
        """Winners FIRST — that is the pre-registered slice; the rest is labelled context."""
        slices = [
            ("eventual winners (NO-STOP net-positive)", list(winners)),
            ("eventual losers (NO-STOP net-negative) [context]", list(losers)),
            ("all entries [context]", [*winners, *losers]),
        ]
        out: list[MaeDistribution] = []
        for label, rows in slices:
            pcts = [p for p, _ in rows]
            mults = [m for _, m in rows]
            out.append(
                MaeDistribution(
                    label=label,
                    n=len(rows),
                    mean_pct=float(np.mean(pcts)) if pcts else None,
                    pct=_quantiles(pcts),
                    atr_mult=_quantiles(mults),
                )
            )
        return out


# --------------------------------------------------------------------------- report rendering
def _pct(value: float | None, dp: int = 5) -> str:
    return "—" if value is None else f"{value:+.{dp}f}%"


def _frac(value: float | None) -> str:
    return "—" if value is None else f"{value:.1%}"


def _q(row: dict[str, float] | None, key: str, dp: int = 3) -> str:
    return "—" if row is None else f"{row[key]:.{dp}f}"


def render_markdown(report: StopGeometryReport) -> str:
    """Render the WO-17 evidence table — banner, null hypothesis, table, diagnostics, answer (C9)."""
    r = report
    lines: list[str] = []
    lines.append(f"# WO-17 diagnostic — stop geometry / adverse-excursion recovery (`{r.experiment_id}`)")
    lines.append("")
    lines.append(f"> **{r.banner}**")
    lines.append("")
    lines.append(f"_Generated {r.generated_at.isoformat()} · pre-registered in IMPROVEMENT_SPEC.md WO-17_")
    lines.append("")

    lines.append("## The pre-registered NULL HYPOTHESIS (stated before any number)")
    lines.append("")
    lines.append(f"> {r.null_hypothesis}")
    lines.append("")

    if r.status == "NO_DATA":
        lines.append("## STATUS: NO DATA — no requested symbol had 1m bars in the window")
        lines.append("")
        lines.append("Nothing was measured; no verdict here means anything. Check the window/universe.")
        lines.append("")
    elif r.status == "NO_ENTRIES":
        lines.append("## STATUS: NO ENTRIES — bars were read, but orb signalled no bookable entry")
        lines.append("")
        lines.append(
            "Every configuration scored zero trades, so no geometry was measured. This is a "
            "population statement, not evidence about stops."
        )
        lines.append("")
    else:
        lines.append("## STATUS: COMPLETED")
        lines.append("")

    # ---- run context ---------------------------------------------------------------------------
    span = f"{r.data_start} → {r.data_end}" if r.data_start else "—"
    params = ", ".join(f"{k}={v:g}" for k, v in sorted(r.orb_params.items()))
    lines.append("## Run")
    lines.append("")
    lines.append(f"- Requested window: {r.requested_start} → {r.requested_end}  ·  resolved: {span}")
    lines.append(f"- Symbols with bars: {r.n_symbols}  ·  sessions: {r.n_sessions}")
    lines.append(
        f"- **Common population: {r.n_entries} orb "
        f"{'entry' if r.n_entries == 1 else 'entries'}** — every configuration was run on the "
        "SAME entries; only the exit differs (orb params: " + params + ")"
    )
    lines.append(
        f"- Costs: {r.product}, ₹{r.reference_notional}/trade, full round-trip friction "
        f"**{r.cost_floor_pct:.4f}%** (fees **+ spread**, WO-2) charged on EVERY trade, stop-outs "
        "included"
    )
    lines.append(f"- Fills: `{r.fill_mechanics}` (never the signal bar)  ·  configs: {r.n_configs}")
    lines.append(
        f"- Stop unit: Wilder ATR({ATR_PERIOD}) on session-local {ATR_RESAMPLE_MINUTES}-minute "
        f"buckets, prior-session-seeded (WO-10b) = `{r.seed_atr_prior_session}`"
    )
    if r.reference_win_rate is not None:
        lines.append(
            f"- Reference path (NO-STOP, no target, held to session end): "
            f"**{r.reference_win_rate:.1%}** of entries ended NET-positive"
        )
    lines.append("")

    # ---- the 10-config table --------------------------------------------------------------------
    lines.append("## The 10 pre-registered configurations")
    lines.append("")
    lines.append(
        "| stop | ladder | trades | win% | net expectancy/trade | gross/trade | stop-outs | "
        "targets | session-end | DODGED-WINNER frac |"
    )
    lines.append("|:-----|:-------|-------:|-----:|---------------------:|------------:|----------:|--------:|------------:|-------------------:|")
    for c in r.pre_registered_configs:
        lines.append(
            f"| {c.label.split(' / ')[0]} | {c.label.split(' / ')[1]} | {c.n_trades} | "
            f"{_frac(c.win_rate)} | {_pct(c.expectancy_pct)} | {_pct(c.gross_expectancy_pct)} | "
            f"{c.n_stopped} | {c.exit_counts.get('target', 0)} | "
            f"{c.exit_counts.get('session_end', 0)} | {_frac(c.dodged_winner_fraction)} |"
        )
    lines.append("")

    reference_rows = [c for c in r.configs if not c.pre_registered]
    if reference_rows:
        lines.append("### Status-quo comparator — NOT one of the pre-registered 10")
        lines.append("")
        lines.append(
            "orb's geometry as it stands today (its opening-range-anchored stop + its 1.5R target). "
            "Included only so \"beat the current tight stop margins\" has something to be beaten."
        )
        lines.append("")
        lines.append("| geometry | trades | win% | net expectancy/trade | stop-outs | DODGED-WINNER frac |")
        lines.append("|:---------|-------:|-----:|---------------------:|----------:|-------------------:|")
        for c in reference_rows:
            lines.append(
                f"| {c.label} | {c.n_trades} | {_frac(c.win_rate)} | {_pct(c.expectancy_pct)} | "
                f"{c.n_stopped} | {_frac(c.dodged_winner_fraction)} |"
            )
        lines.append("")

    # ---- diagnostic 1 ----------------------------------------------------------------------------
    lines.append("## Diagnostic 1 — DODGED-WINNER FRACTION, per stop width")
    lines.append("")
    lines.append(
        "_Of the trades a geometry **stopped out**, the share whose SAME entry — held with no stop "
        "and no target to the session-end square-off — would have ended **net-positive** after the "
        "full round trip. The gross column is the same count without the cost charge, shown so the "
        "net reading is auditable._"
    )
    lines.append("")
    lines.append("| stop | ladder | stop-outs | dodged winners | fraction (net) | fraction (gross) |")
    lines.append("|:-----|:-------|----------:|---------------:|---------------:|-----------------:|")
    for c in r.configs:
        if c.n_stopped == 0:
            continue
        tag = "" if c.pre_registered else " *(comparator)*"
        lines.append(
            f"| {c.label.split(' / ')[0]}{tag} | {c.label.split(' / ')[1]} | {c.n_stopped} | "
            f"{c.n_dodged_winners} | {_frac(c.dodged_winner_fraction)} | "
            f"{_frac(c.dodged_winner_fraction_gross)} |"
        )
    lines.append("")

    # ---- diagnostic 2 ----------------------------------------------------------------------------
    lines.append("## Diagnostic 2 — MAE distribution: what dip depth do winners actually survive")
    lines.append("")
    lines.append(
        "_Maximum adverse excursion measured over the bars a stop could have triggered on (the fill "
        "bar through the bar before the session-end exit fill), so \"MAE ≥ w × ATR_10m\" and "
        "\"stopped at width w\" are the same event. The first row is WO-17's pre-registered slice._"
    )
    lines.append("")
    lines.append("| slice | n | mean % | p25 | p50 | p75 | p90 | p25 ATR | p50 ATR | p75 ATR | p90 ATR |")
    lines.append("|:------|--:|-------:|----:|----:|----:|----:|--------:|--------:|--------:|--------:|")
    for m in r.mae_distributions:
        mean = "—" if m.mean_pct is None else f"{m.mean_pct:.3f}"
        lines.append(
            f"| {m.label} | {m.n} | {mean} | {_q(m.pct, 'p25')} | {_q(m.pct, 'p50')} | "
            f"{_q(m.pct, 'p75')} | {_q(m.pct, 'p90')} | {_q(m.atr_mult, 'p25', 2)} | "
            f"{_q(m.atr_mult, 'p50', 2)} | {_q(m.atr_mult, 'p75', 2)} | {_q(m.atr_mult, 'p90', 2)} |"
        )
    lines.append("")
    lines.append("_% columns are percent of the entry fill price; ATR columns are multiples of that entry's own ATR_10m._")
    lines.append("")

    # ---- the answer ------------------------------------------------------------------------------
    lines.append("## Plain-language answer to the owner's question")
    lines.append("")
    for line in r.plain_language_answer:
        lines.append(line if line else "")
    lines.append("")

    # ---- modelling notes -------------------------------------------------------------------------
    lines.append("## modelling_notes — every pre-registered choice")
    lines.append("")
    for note in r.modelling_notes:
        lines.append(f"- {note}")
    lines.append("")
    lines.append(f"> **{r.banner}**")
    lines.append("")
    return "\n".join(lines)


def write_report(report: StopGeometryReport, reports_dir: str | Path) -> ReportArtifacts:
    """Write ``stop_geometry_<ts>.md`` + ``.json`` under ``reports_dir`` (design requirement 5)."""
    out = Path(reports_dir)
    out.mkdir(parents=True, exist_ok=True)
    stem = f"{EXPERIMENT_ID}_{report.generated_at.strftime('%Y%m%dT%H%M%S')}"
    md_path = out / f"{stem}.md"
    json_path = out / f"{stem}.json"
    md_path.write_text(render_markdown(report), encoding="utf-8")
    json_path.write_text(report.model_dump_json(indent=2), encoding="utf-8")
    return ReportArtifacts(markdown=md_path, json=json_path)


__all__ = [
    "BANNER",
    "EXPERIMENT_ID",
    "EXIT_REASONS",
    "FILL_MECHANICS",
    "INCLUDE_STATUS_QUO_REFERENCE",
    "LADDERS",
    "MAE_QUANTILES",
    "NULL_HYPOTHESIS",
    "N_CONFIGS",
    "OWNER_QUESTION",
    "PRE_REGISTRATION_INTERPRETATIONS",
    "PRODUCT",
    "REFERENCE_CONFIG",
    "REFERENCE_NOTIONAL",
    "SEED_ATR_PRIOR_SESSION",
    "STOP_ATR_MULTIPLES",
    "STOP_AXIS",
    "STOP_SOURCE_ATR",
    "STOP_SOURCE_NONE",
    "STOP_SOURCE_ORB_RANGE",
    "TARGET_SOURCE",
    "ConfigAccumulator",
    "ConfigResult",
    "EntryContext",
    "MaeDistribution",
    "StopConfig",
    "StopGeometryExperiment",
    "StopGeometryReport",
    "StopTrade",
    "all_configs",
    "build_entry_context",
    "cost_floor_pct_for",
    "entry_contexts_for_symbol",
    "grid_configs",
    "modelling_notes",
    "orb_default_params",
    "orb_per_side_fee",
    "orb_signal_stream",
    "plain_language_answer",
    "render_markdown",
    "simulate_entry",
    "status_quo_config",
    "stop_level",
    "target_level",
    "write_report",
]
