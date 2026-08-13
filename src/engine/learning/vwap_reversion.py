"""WO-10 — PRE-REGISTERED experiment: intraday VWAP-deviation reversion at the 15–60-min horizon.

**C-alpha-experiment. STOPS FOR OWNER REVIEW. No live wiring of any kind** (WO-10's own words). This
module is research machinery only: nothing here is imported by a scanner, the gate, the OMS or any
scheduled job, and the experiment deliberately writes NO ``param_sets`` row (see "Persistence" below)
so no downstream promotion path can ever pick its output up.

THE PRE-REGISTRATION (IMPROVEMENT_SPEC.md WO-10, quoted — this module may not deviate from it)
-----------------------------------------------------------------------------------------------
    Hypothesis (pre-registered): NIFTY200 symbols stretched >= X% from session VWAP (X swept over
    a SMALL fixed grid {1.0, 1.5, 2.0}) with relative volume < 1.5x revert toward VWAP within
    15-60 min by more than 2x the full cost floor (fees + measured per-tier spread), long side
    only, entries 10:00-14:00, exits at VWAP-touch / 60-min timeout / fixed stop 1.5x the
    10-min ATR.
    Dataset/window: 1m bars, full available history (1m from 2025-07-10), universe = the historical
    NIFTY200 list per COMMANDS.md; costs = WO-2's corrected surface INCLUDING spread; fills at
    next-bar open (never the signal bar).
    Promotion bar: house CPCV + fold_pass_min + WO-3's margin floor, N = the grid cardinality.
    Abort criterion: if the UNCONDITIONED 15-60-min reversion base rate after costs is <= 0 in the
    first full-window pass, stop -- do not tune the condition until something clears zero.

WO-10b — THE MORNING-WINDOW MODE (owner-directed 2026-08-14, after WO-10's abort)
---------------------------------------------------------------------------------
WO-10 aborted at stage 1 (unconditioned base rate −0.12742%/trade ≈ the cost floor, gross ≈ 0) — but
its 10m-ATR warm-up meant only **11:36–14:00** was ever measured; the 10:00–11:35 morning window was
never observed. WO-10b is pre-registered as a MODE of this same harness, not a fork (quoted):

    the stop unit's ATR seeds from the PRIOR session — the Wilder ATR(14) recursion continues from
    the previous session's final 10m-ATR value, with the current session's FIRST bucket's true range
    computed high−low only (no prev-close term: the overnight gap must never enter the stop unit); a
    symbol with no prior-session ATR produces no trades until its own warm-up completes (fail-closed,
    same rule as WO-10). Entry window 10:00–14:00 as originally registered — now effective from
    10:00; results additionally reported split 10:00–11:35 vs 11:36–14:00 so the morning increment is
    visible against WO-10's answer. Same two-stage structure, same abort criterion on the SAME
    windows it measures, same grid, same costs/fills/exits. No other change to the WO-10
    pre-registration.

Switched by ``--seed-atr-prior-session`` / ``VwapReversionExperiment(seed_atr_prior_session=True)``.
**Default OFF ⇒ WO-10 behaviour is byte-identical** — the flag supplies an ATR seed and nothing else;
no other code path branches on it. The seeding is a per-SYMBOL chain, so sessions are walked in DATE
ORDER (:func:`simulate_symbol` sorts rather than assumes). The abort criterion remains the OVERALL
stage-1 number; the window splits are REPORTING ONLY and no decision reads them.

The grid is the **stretch axis only** (3 points ⇒ N = 3). ``rel_vol_max`` (1.5), the entry window
(10:00–14:00), the exit ladder (VWAP-touch / 60-min timeout / 1.5 × 10-min ATR stop) and the long-only
direction are **FIXED constants, never swept** — that is what keeps the multiplicity contained, which
is the whole point of a pre-registration. Adding an axis here silently would invalidate the N.

DESIGN REQUIREMENTS (manager-specified, beyond the WO text — each stated here as required)
-----------------------------------------------------------------------------------------
1. **TWO-STAGE, per the abort criterion.** Stage 1 measures the UNCONDITIONED reversion base rate
   after costs over the full window — every symbol-session, **no stretch and no relative-volume
   condition** (:data:`STAGE1_STRETCH_MIN_PCT` = 0.0, :data:`STAGE1_REL_VOL_MAX` = ``None``); the
   entry window, the exit ladder, the fill mechanics and the cost charge are IDENTICAL to stage 2, so
   stage 1 is exactly "the same trade with the condition switched off". If that base rate is <= 0 (or
   there were no unconditioned trades at all to take a base rate of), the run writes an **ABORTED**
   report stating the number and STOPS — exit code 0, clearly labelled, stage 2 never computed.
   Stage 2 runs the 3 × 1 grid (stretch axis only) and only then.
2. **Fills: next-bar OPEN, never the signal bar.** The stretch/rel-vol condition is evaluated on bar
   *t*'s COMPLETED values; the entry fills at bar *t+1*'s **open**. Exits work the same way: a
   VWAP-touch or stop trigger observed on bar *i* fills at bar *i+1*'s open. A signal on a session's
   last usable bar is DROPPED (there is no bar left in that session to fill it in) — never carried
   into the next session. Every timestamp comparison is session-aware: bars are grouped by IST
   session date and clamped to 09:15 <= t < 15:30 before anything is computed.
   The ONE clock-driven exit — the 60-minute timeout — is defined by TIMESTAMP, not bar count: the
   fill is the open of the first bar at or after ``entry_ts + 60 min``, i.e. the position is held for
   exactly 60 minutes and the last bar fully held (the trigger bar) is the one before it. Those two
   readings coincide exactly; the house precedent for a clock-driven exit filling on its own schedule
   is ``sweep.py``'s forced MIS square-off (``_Signals.shift_exits=False``).
3. **VWAP = session-cumulative.** ``sum(typical_price x volume) / sum(volume)`` accumulated from the
   session's FIRST bar, where typical price = (H+L+C)/3. This is exactly what
   :func:`engine.strategy.indicators.vwap` already computes ("cumulative from the FIRST input bar…
   the caller owns session slicing"), so it is REUSED unchanged — this module owns only the session
   slicing. There is no divergence to declare.
4. **Costs.** ``product = MIS``, per-trade notional ₹20,000, and the charge is
   ``CostModel.breakeven_pct(20000, "MIS")`` — WO-2's corrected surface **INCLUDING the measured
   spread** (``round_trip`` = fees + spread; NOT ``fee_breakeven_pct``, which is the spread-excluded
   contract-note anchor the vectorbt sweeps use because they charge spread separately as slippage).
   The FULL round trip is charged on EVERY simulated trade: ``net_return_pct = gross_return_pct −
   cost_floor_pct``. There is no vectorbt here, so there is nothing to double-count.
5. **Report.** JSON + MD to ``data/reports/vwap_reversion_<ts>.{json,md}`` in the sweep-report style,
   carrying the stage-1 base rate, per-grid-point trade count / expectancy / win rate, the CPCV fold
   results + margin-floor verdict + ``promotable``, a ``modelling_notes`` block stating every
   pre-registered choice and every interpretation made, and the :data:`BANNER` at the top.
6. **Unit-tested on synthetic 1m fixtures only** (``tests/unit/test_vwap_reversion.py``) — the trigger
   threshold, the next-bar-open fill (pinned so a same-bar fill produces a detectably different
   number), each of the three exits, the entry-window boundary, the session-last-bar drop, and the
   abort path.

PROMOTION — the house rule, applied unchanged
---------------------------------------------
Each grid point produces a **cost-adjusted DAILY net-return series** in exactly the shape
:class:`engine.learning.validate.ValidationPipeline` consumes (``pd.Series`` of floats, ascending
``datetime.date`` index, one observation per session): per (symbol, session) the net trade returns are
compounded, and the strategy's daily return is the cross-sectional MEAN over the symbols that had bars
that session (0 for a symbol that was flat) — the same equal-weight convention
``SweepRunner._daily_returns`` uses. The pipeline therefore applies CPCV + ``fold_pass_min(N=3)`` +
the WO-3 margin floor with NO modification. ``ParamSet.cost_floor_pct`` is passed explicitly as the
**MIS** round trip so the floor cannot silently fall back to ``default_cost_floor_pct``'s CNC default
(``PRODUCT_BY_STRATEGY`` has no ``vwap_reversion`` row, by design — adding one would be live wiring).

**Persistence: none.** The pipeline is constructed with ``conn=None`` and ``reports_dir=None``, so it
writes no ``param_sets`` candidate row and no separate validation artifact. The single self-contained
experiment report is the entire deliverable — "STOP for owner review after the experiment report; no
live wiring of any kind".

INTERPRETATIONS — where the WO text was under-determined
--------------------------------------------------------
Recorded here, in :data:`PRE_REGISTRATION_INTERPRETATIONS`, and reprinted in every report's
``modelling_notes`` so a reader can audit them against the pre-registration. They are named constants,
not inline literals, precisely so that changing one is a visible diff.

* **"base rate … <= 0"** — a *rate* (a win fraction) is bounded below by 0 and could essentially never
  trip a "<= 0" abort, so the abort quantity must be SIGNED: it is the **mean per-trade net return
  after the full round-trip cost, in percent** (:func:`Stage1Result.base_rate_pct`). The win fraction
  is reported alongside it for context but is NOT the abort criterion.
* **"unconditioned"** — the task pins the relaxation to "no stretch/volume condition", so the entry
  window, exit ladder, direction and costs are held fixed. The residual condition ``close < vwap``
  is the DIRECTION requirement, not a tuning knob: a long whose exit is a VWAP touch is already
  exited if it enters above VWAP, so bars at or above VWAP are not reversion candidates at all.
* **"relative volume"** — the house definition, reused from the ``orb`` baseline: the bar's volume
  divided by the median of the :data:`REL_VOL_WINDOW` (20) bars BEFORE it, within the session
  (``indicators.rolling_median_volume`` shifted one bar). A bar with a non-finite or zero median is
  not eligible while the filter is active (``orb`` skips those identically).
* **"the 10-min ATR"** (manager ruling, 2026-08-13 — a PRE-REGISTRATION decision made BEFORE any real
  data run) — Wilder ATR(:data:`ATR_PERIOD` = 14) computed on session-local **10-MINUTE** bars: the
  1m frame is aggregated to 10-minute OHLC within the session, ATR(14) runs on that series, and the
  result is carried onto the 1m grid for the stop calculation at entry. **Not** ATR(10) on 1m bars:
  that measures 1-MINUTE noise scale (median ~0.07% per the audit's measurement), putting the stop at
  ~0.10% — BELOW the 0.126% cost floor. A stop smaller than the round-trip cost is the exact ORB
  death geometry (IMPROVEMENT_SPEC F-series / plan §6.1 orb history), so that reading would pre-doom
  the experiment by construction. The 10-minute-timescale reading yields a stop proportionate to the
  15–60-min holding horizon and above the floor. The stop level is
  ``entry_fill_price − 1.5 × ATR_10m(at the signal bar)`` — anchored at the FILL price, matching the
  house ``stop_entry_price='fillprice'`` convention WO-2 pinned.
  **Warm-up consequence, stated deliberately**: ATR(14) on 10-minute bars needs 14 completed buckets,
  and a bucket's ATR is only available from the NEXT bucket (no lookahead), so on a 09:15 session the
  first bar carrying a defined ATR is **11:35** and the first possible entry FILL is **11:36**. Bars
  before that produce **NO trade** rather than an unstopped one. The pre-registered 10:00 window
  opening is therefore DOMINATED by this warm-up: no entry can ever fill between 10:00 and 11:35, and
  the effective entry window is 11:36–14:00.
* **"entries 10:00–14:00"** — the constraint is on the ENTRY, and under next-bar-open mechanics the
  entry IS the fill, so the **fill** timestamp must satisfy ``10:00 <= t <= 14:00`` (inclusive both
  ends). Signal bars therefore run 09:59–13:59.
* **"revert … by more than 2x the full cost floor"** — this is the hypothesis' effect-size claim, not
  a gate: WO-10 names the promotion bar separately (CPCV + ``fold_pass_min`` + the WO-3 margin
  floor). The ``2 x cost floor`` comparison is REPORTED per grid point
  (``GridPointResult.clears_2x_cost_floor``) and never enforced.
* **Overlap / re-entry** — one position per symbol at a time; a signal that fires while a position is
  open is ignored, and the next entry can fill no earlier than the bar after the exit fill. Without
  this an unconditioned pass would book ~240 massively overlapping "trades" per symbol-session and
  the expectancy would be a statistic about nothing.
* **Simultaneous stop and VWAP touch inside one bar** — 1m bars carry no intrabar path, so the
  ADVERSE event is assumed first: the stop wins the tie.
* **Session ends before the timeout** — the position is squared off at the session's LAST bar's open
  (MIS obligation) and the exit is labelled ``session_end``, distinct from ``timeout``. With entries
  capped at 14:00 and a 60-min timeout this cannot occur on a full session; it exists so degenerate
  or truncated sessions have defined, visible behaviour instead of a silent drop.

The module top level is pandas/numpy/pydantic + stdlib only (``MarketStore``/``CostModel`` are
TYPE_CHECKING-only, like :mod:`engine.learning.sweep`), so the pure test tier imports it cheaply.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd
from pydantic import BaseModel, ConfigDict, Field

from engine.core.clock import IST, Clock
from engine.core.log import get_logger
from engine.learning.reports import ReportArtifacts
from engine.learning.validate import ParamSet, ValidationPipeline, ValidationReport
from engine.strategy.indicators import rolling_median_volume, wilder_atr
from engine.strategy.indicators import vwap as _session_vwap

if TYPE_CHECKING:                                    # heavy/optional surfaces stay out of the top level
    from engine.marketdata.store import MarketStore
    from engine.strategy.cost_model import CostModel

_log = get_logger("engine.learning.vwap_reversion")

# --------------------------------------------------------------------------- pre-registered constants
#: Report/param-set identity. Deliberately absent from ``sweep.PRICE_BASELINES`` and
#: ``sweep.PRODUCT_BY_STRATEGY`` — this is an experiment, not a baseline, and must not be reachable
#: from any promotion path (WO-10: "no live wiring of any kind").
EXPERIMENT_ID = "vwap_reversion"

#: The banner every report leads with (design requirement 5).
BANNER = "C-CATEGORY: STOPS FOR OWNER REVIEW - no live wiring"

#: WO-10 costs: MIS, ₹20,000 per trade, WO-2's corrected surface INCLUDING spread.
PRODUCT = "MIS"
REFERENCE_NOTIONAL: Decimal = Decimal("20000")

#: THE grid — stretch axis ONLY, 3 points ⇒ trial count N = 3 (``fold_pass_min(3)`` = 60%).
STRETCH_GRID: tuple[float, ...] = (1.0, 1.5, 2.0)

#: FIXED, never swept (see the module docstring): the pre-registered condition and exit ladder.
REL_VOL_MAX = 1.5                       #: "relative volume < 1.5x"
REL_VOL_WINDOW = 20                     #: house 20-bar volume median (orb's ``_MEDIAN_WINDOW``)
ENTRY_WINDOW_START = time(10, 0)        #: entry FILL must be at or after this IST time
ENTRY_WINDOW_END = time(14, 0)          #: …and at or before this one
TIMEOUT_MINUTES = 60                    #: "60-min timeout" — the upper end of the 15–60-min horizon
STOP_ATR_MULT = 1.5                     #: "fixed stop 1.5x the 10-min ATR"
DIRECTION = "long_only"                 #: WO-10: "long side only" (§1.4.9 shorts gate is shut)

#: "the 10-min ATR" (manager ruling 2026-08-13, pre-registered before any data run): the ATR is
#: measured on **10-MINUTE** bars aggregated from the session's 1m frame, at Wilder period 14 — NOT
#: ATR(10) on 1m bars, which measures 1-minute noise scale (~0.07% median) and would put the stop
#: (~0.10%) BELOW the 0.126% round-trip cost floor. A stop smaller than its own round trip is the ORB
#: death geometry (IMPROVEMENT_SPEC F-series / §6.1 orb history); see the module docstring.
ATR_RESAMPLE_MINUTES = 10
ATR_PERIOD = 14

#: Minutes from a session's FIRST bar before the 10m ATR(14) exists on the 1m grid, in UNSEEDED
#: (WO-10) mode. 14 completed 10-minute buckets, and a bucket's ATR is only available from the NEXT
#: bucket (no lookahead) ⇒ 14 x 10 = 140 min. On a 09:15 session: first defined ATR bar 11:35, first
#: possible FILL 11:36. Bars before it produce NO trade rather than an unstopped one, so this
#: warm-up — not the pre-registered 10:00 opening — is what bounds the entry window from below.
#: WO-10b's prior-session seeding removes it (one bucket instead: ATR from 09:25).
ATR_WARMUP_MINUTES = ATR_RESAMPLE_MINUTES * ATR_PERIOD

#: WO-10b (owner-directed 2026-08-14, pre-registered): the split boundary for reporting the MORNING
#: increment against WO-10's midday answer. Entries whose FILL is at or before this time are the
#: morning split (10:00–11:35 — the window WO-10's warm-up could never reach); later fills are the
#: midday split (11:36–14:00 — exactly what WO-10 measured). REPORTING ONLY, never a gate.
MORNING_SPLIT_END = time(11, 35)
SPLIT_MORNING = "morning_1000_1135"
SPLIT_MIDDAY = "midday_1136_1400"

#: Stage 1 (the abort criterion): the SAME trade with the stretch and relative-volume conditions off.
STAGE1_STRETCH_MIN_PCT = 0.0
STAGE1_REL_VOL_MAX: float | None = None

#: NSE session bounds — every bar is clamped to ``[09:15, 15:30)`` IST before anything is computed.
SESSION_OPEN = time(9, 15)
SESSION_CLOSE = time(15, 30)

#: WO-2: what the report's ``fill_mechanics`` field declares.
FILL_MECHANICS = "next_bar_open"

#: The hypothesis' effect-size claim ("by more than 2x the full cost floor") — REPORTED, never gated.
HYPOTHESIS_EDGE_MULTIPLE = 2.0

_ENTRY_LO_MIN = ENTRY_WINDOW_START.hour * 60 + ENTRY_WINDOW_START.minute
_ENTRY_HI_MIN = ENTRY_WINDOW_END.hour * 60 + ENTRY_WINDOW_END.minute
_MORNING_END_MIN = MORNING_SPLIT_END.hour * 60 + MORNING_SPLIT_END.minute
_OPEN_MIN = SESSION_OPEN.hour * 60 + SESSION_OPEN.minute
_CLOSE_MIN = SESSION_CLOSE.hour * 60 + SESSION_CLOSE.minute

EXIT_REASONS: tuple[str, ...] = ("vwap_touch", "stop", "timeout", "session_end")

#: Every place the WO text was under-determined and this module had to choose. Reprinted verbatim into
#: each report's ``modelling_notes`` (design requirement 5) so the choices are auditable, not implicit.
PRE_REGISTRATION_INTERPRETATIONS: tuple[str, ...] = (
    "ABORT QUANTITY: 'the unconditioned 15-60-min reversion base rate after costs <= 0' is read as a "
    "SIGNED number — the MEAN PER-TRADE NET RETURN after the full round-trip cost, in percent — "
    "because a win FRACTION is bounded below by zero and could never trip a '<= 0' abort. The win "
    "fraction is reported next to it for context and is NOT the criterion.",
    "'UNCONDITIONED' = the stretch threshold set to 0.0% and the relative-volume filter switched OFF; "
    "the entry window, exit ladder, long-only direction, fill mechanics and cost charge are IDENTICAL "
    "to stage 2. The residual 'close < session VWAP' is the DIRECTION requirement (a long whose exit "
    "is a VWAP touch is already exited if entered at or above VWAP), not a tuning knob.",
    f"'RELATIVE VOLUME' = the house orb definition: bar volume / median volume of the "
    f"{REL_VOL_WINDOW} bars BEFORE it, within the session (indicators.rolling_median_volume, shifted "
    "one bar). Bars whose median is zero or non-finite are ineligible while the filter is active "
    "(orb skips those identically). Stage 1 does not apply the filter at all.",
    f"'THE 10-MIN ATR' = Wilder ATR(period={ATR_PERIOD}) computed on session-local "
    f"{ATR_RESAMPLE_MINUTES}-MINUTE bars: the 1m frame is aggregated to {ATR_RESAMPLE_MINUTES}-minute "
    "OHLC within the session, ATR(14) runs on that series, and the value is carried onto the 1m grid "
    f"for the stop at entry. Stop level = entry FILL price - {STOP_ATR_MULT} x ATR_10m at the SIGNAL "
    "bar (anchored at the fill, matching the house stop_entry_price='fillprice' convention WO-2 "
    "pinned). MANAGER RULING 2026-08-13, a PRE-REGISTRATION decision made BEFORE any real data run. "
    "REASON (verbatim): ATR(10) on 1m bars measures 1-minute noise scale - median ~0.07% per the "
    "audit's measurement - putting the stop at ~0.10%, BELOW the 0.126% cost floor; a stop smaller "
    "than the round-trip cost is the exact ORB death geometry (IMPROVEMENT_SPEC F-series / plan "
    "§6.1 orb history), so that reading would pre-doom the experiment by construction. The "
    "10-minute-timescale reading yields a stop proportionate to the 15-60-min holding horizon and "
    "above the floor. SESSION-LOCAL: buckets are anchored at the session's own first bar and never "
    "carry across sessions, so no overnight gap enters the true range.",
    f"10m-ATR WARM-UP: ATR({ATR_PERIOD}) on {ATR_RESAMPLE_MINUTES}-minute bars needs {ATR_PERIOD} "
    "completed buckets, and a bucket's ATR is available only from the NEXT bucket (no lookahead), so "
    f"the first 1m bar carrying a defined ATR is {ATR_WARMUP_MINUTES} minutes after the session's "
    "first bar = 11:35 on a 09:15 session, and the first possible entry FILL is 11:36. Bars before "
    "that produce NO TRADE rather than an unstopped one - an entry we cannot stop is not a trade this "
    "experiment is willing to book. CONSEQUENCE, STATED DELIBERATELY: the pre-registered 10:00 window "
    "opening is DOMINATED by this warm-up - no entry can ever fill between 10:00 and 11:35, so the "
    "EFFECTIVE entry window is 11:36-14:00 (~145 of the nominal 241 minutes). Sessions with fewer "
    f"than {ATR_PERIOD} complete {ATR_RESAMPLE_MINUTES}-minute buckets produce no trades at all.",
    f"'ENTRIES {ENTRY_WINDOW_START:%H:%M}-{ENTRY_WINDOW_END:%H:%M}' constrains the ENTRY, and under "
    "next-bar-open mechanics the entry IS the fill: the FILL timestamp must satisfy "
    f"{ENTRY_WINDOW_START:%H:%M} <= t <= {ENTRY_WINDOW_END:%H:%M} (inclusive). Signal bars therefore "
    "run 09:59-13:59.",
    f"'REVERT ... BY MORE THAN {HYPOTHESIS_EDGE_MULTIPLE:g}x THE FULL COST FLOOR' is the hypothesis' "
    "effect-size claim, not a gate: WO-10 names the promotion bar separately (house CPCV + "
    "fold_pass_min + the WO-3 margin floor). The multiple is REPORTED per grid point "
    "(clears_2x_cost_floor) and never enforced.",
    "OVERLAP: one position per symbol at a time. A signal firing while a position is open is ignored; "
    "the next entry can fill no earlier than the bar AFTER the exit fill. Without this, an "
    "unconditioned pass would book ~240 overlapping 'trades' per symbol-session.",
    "SIMULTANEOUS STOP AND VWAP TOUCH inside one 1m bar: 1m bars carry no intrabar path, so the "
    "ADVERSE event is assumed first — the stop wins the tie.",
    "SESSION ENDS BEFORE THE TIMEOUT: squared off at the session's LAST bar's open (MIS) and labelled "
    "'session_end', distinct from 'timeout'. Unreachable on a full session given the 14:00 entry cap; "
    "it exists so truncated sessions have defined, visible behaviour rather than a silent drop.",
    "PERSISTENCE: none. The ValidationPipeline runs with conn=None and reports_dir=None — no "
    "param_sets candidate row, no separate validation artifact, nothing a promotion path can pick up.",
)


# --------------------------------------------------------------------------- configuration
@dataclass(frozen=True, slots=True)
class ReversionConfig:
    """One evaluated configuration. ``rel_vol_max=None`` ⇒ the relative-volume filter is OFF (stage 1).

    ``stretch_min_pct`` is the minimum distance BELOW session VWAP, in percent of VWAP, that a bar's
    close must reach for the bar to be a signal.
    """

    stretch_min_pct: float
    rel_vol_max: float | None

    @property
    def label(self) -> str:
        rv = "off" if self.rel_vol_max is None else f"<{self.rel_vol_max:g}x"
        return f"stretch>={self.stretch_min_pct:g}% relvol{rv}"

    @property
    def params(self) -> dict[str, float]:
        """The ``ParamSet.params`` view. ``rel_vol_max`` is a FIXED constant carried for the record,
        never a swept axis; it is omitted entirely when the filter is off so no ``NaN`` can reach the
        report JSON (``NaN`` is not valid JSON)."""
        params = {"stretch_min_pct": float(self.stretch_min_pct)}
        if self.rel_vol_max is not None:
            params["rel_vol_max"] = float(self.rel_vol_max)
        return params


STAGE1_CONFIG = ReversionConfig(stretch_min_pct=STAGE1_STRETCH_MIN_PCT, rel_vol_max=STAGE1_REL_VOL_MAX)


def grid_configs() -> list[ReversionConfig]:
    """The pre-registered 3 x 1 grid — stretch axis only; ``len()`` is the trial count N (§6.4 step 1)."""
    return [ReversionConfig(stretch_min_pct=x, rel_vol_max=REL_VOL_MAX) for x in STRETCH_GRID]


# --------------------------------------------------------------------------- per-session features
@dataclass(frozen=True, slots=True)
class SessionFeatures:
    """Everything one symbol-session needs, as positional numpy arrays (index i = the i-th 1m bar).

    Built once per (symbol, session) and reused across every configuration evaluated in that pass —
    none of these depend on a swept parameter.
    """

    symbol: str
    session: date
    ts: pd.DatetimeIndex
    ts_i8: np.ndarray                   # int64 ns, for exact timestamp arithmetic (timeout)
    minute_of_day: np.ndarray           # int, for the session-aware entry-window comparison
    open: np.ndarray
    high: np.ndarray
    low: np.ndarray
    close: np.ndarray
    volume: np.ndarray
    vwap: np.ndarray                    # session-cumulative (indicators.vwap over THIS session only)
    #: The STOP unit: Wilder ATR(14) on session-local 10-MINUTE bars, carried onto the 1m grid with a
    #: one-bucket availability lag (no lookahead). NaN through the warm-up ⇒ those bars cannot signal.
    atr_10m: np.ndarray
    rel_vol: np.ndarray                 # volume / median(previous REL_VOL_WINDOW bars)
    stretch_pct: np.ndarray             # (vwap - close) / vwap * 100 — POSITIVE means below VWAP
    #: WO-10b: this session's FINAL 10m-bucket ATR — what the NEXT session seeds its recursion from.
    #: ``None`` ⇒ this session produced no ATR at all, so the chain stays fail-closed.
    atr_10m_final: float | None = None

    @property
    def n(self) -> int:
        return len(self.ts)


def session_slices(index: pd.DatetimeIndex) -> list[tuple[int, int]]:
    """Positional ``[start, stop)`` bounds of each IST session in an ascending 1m index.

    Sessions are contiguous runs of equal IST calendar date (the store returns bars ascending), so
    this is a run-length split — no groupby, no calendar dependency. Pure (§9.6).
    """
    if len(index) == 0:
        return []
    codes, _ = pd.factorize(pd.Index([pd.Timestamp(ts).date() for ts in index]))
    codes = np.asarray(codes)
    boundaries = [0, *(np.flatnonzero(codes[1:] != codes[:-1]) + 1).tolist(), len(codes)]
    return [(boundaries[i], boundaries[i + 1]) for i in range(len(boundaries) - 1)]


def _seeded_wilder_atr(
    bar_hi: np.ndarray, bar_lo: np.ndarray, bar_cl: np.ndarray, period: int, seed: float
) -> np.ndarray:
    """Wilder ATR CONTINUING from ``seed`` — the prior session's final 10m-ATR value (WO-10b).

    THE PRE-REGISTERED SEEDING RULE (IMPROVEMENT_SPEC WO-10b, quoted): *"the Wilder ATR(14) recursion
    continues from the previous session's final 10m-ATR value, with the current session's FIRST
    bucket's true range computed high−low only (no prev-close term: the overnight gap must never
    enter the stop unit)"*.

    So there is no SMA seeding period at all: ``atr[0] = (seed x (period-1) + (high[0] - low[0])) /
    period`` and the ordinary Wilder recursion from there. The first bucket's ``high - low`` form is
    the same convention :func:`engine.strategy.indicators.wilder_atr` documents for a series' first
    bar ("the first bar (no previous close) uses plain high−low"); it is recomputed here only because
    the seeded recursion needs the TR array, which ``wilder_atr`` does not expose. Excluding the
    prev-close term is load-bearing, not incidental: with it, an overnight gap of G would put
    ``|high[0] − prev_session_close| ≈ G`` into the stop unit and a gap-down morning would silently
    widen every stop that day.
    """
    n = len(bar_cl)
    tr = np.empty(n, dtype="float64")
    tr[0] = bar_hi[0] - bar_lo[0]                    # NO prev-close term — the overnight gap is excluded
    if n > 1:
        prev_close = bar_cl[:-1]
        tr[1:] = np.maximum.reduce([
            bar_hi[1:] - bar_lo[1:],
            np.abs(bar_hi[1:] - prev_close),
            np.abs(bar_lo[1:] - prev_close),
        ])
    out = np.empty(n, dtype="float64")
    prev = float(seed)
    for i in range(n):
        prev = (prev * (period - 1) + tr[i]) / period
        out[i] = prev
    return out


def session_atr_10m(
    high: np.ndarray,
    low: np.ndarray,
    close: np.ndarray,
    minute_of_day: np.ndarray,
    *,
    seed: float | None = None,
    resample_minutes: int = ATR_RESAMPLE_MINUTES,
    period: int = ATR_PERIOD,
) -> tuple[np.ndarray, float | None]:
    """Wilder ATR(``period``) on session-local ``resample_minutes``-bar OHLC, on the 1m grid.

    Returns ``(atr_on_the_1m_grid, this_session's_final_bucket_ATR)``. The second element is what the
    NEXT session passes back in as ``seed`` (WO-10b); ``None`` when this session never produced an
    ATR at all, which keeps the chain fail-closed.

    The manager's 2026-08-13 pre-registration ruling (see the module docstring): "the 10-min ATR" is
    measured at the 10-MINUTE timescale, not as ATR(10) over 1m bars. Steps, all session-local:

    1. **Bucket** the session's 1m bars into ``resample_minutes``-wide buckets anchored at the
       session's OWN FIRST BAR (not a fixed wall-clock grid), so a late-opening or truncated session
       still yields whole buckets and nothing is carried in from another session.
    2. **Aggregate** each bucket to OHLC — ``high = max``, ``low = min``, ``close = last`` — via
       run-boundary ``reduceat`` (buckets are contiguous runs of an ascending index). A bucket that no
       bar falls into simply does not exist; the series is over the buckets PRESENT, in order.
    3. **Wilder ATR(period)** on that aggregated series. Two modes:

       * ``seed is None`` (WO-10, the default) — :func:`engine.strategy.indicators.wilder_atr` reused
         unchanged: SMA-seeded over the first ``period`` buckets, so the first ATR lands on bucket
         ``period-1`` and fewer than ``period`` buckets yields nothing at all;
       * ``seed`` given (WO-10b) — :func:`_seeded_wilder_atr`, which continues the recursion from the
         prior session's final value and therefore has an ATR on bucket 0.
    4. **Availability lag**: bucket *k*'s ATR is only KNOWN once bucket *k* has closed, so it is
       published to the 1m bars of bucket *k+1* onward. Without this shift a bar at 11:26 would be
       using its own bucket's 11:34 high — lookahead, and exactly the class of defect WO-2 removed.
       This applies identically in both modes; seeded, it costs one bucket (ATR from 09:25), not 140
       minutes.

    Result: ``NaN`` through the warm-up — ``resample_minutes x period`` minutes unseeded, one bucket
    seeded — then the stop unit for every later bar. Bars with ``NaN`` here cannot signal: an entry we
    could not stop is not booked. Pure and deterministic (§9.6).
    """
    n = len(close)
    out = np.full(n, np.nan, dtype="float64")
    if n == 0:
        return out, None
    bucket = (minute_of_day - minute_of_day[0]) // int(resample_minutes)
    starts = np.flatnonzero(np.concatenate(([True], bucket[1:] != bucket[:-1])))
    n_buckets = len(starts)
    if seed is None and n_buckets < period:
        return out, None                             # too few buckets for ATR(period) — no stop, no trade
    bar_hi = np.maximum.reduceat(np.asarray(high, dtype="float64"), starts)
    bar_lo = np.minimum.reduceat(np.asarray(low, dtype="float64"), starts)
    last_pos = np.concatenate((starts[1:] - 1, [n - 1]))
    bar_cl = np.asarray(close, dtype="float64")[last_pos]

    if seed is None:
        atr_buckets = wilder_atr(bar_hi, bar_lo, bar_cl, period).to_numpy(dtype="float64")
    else:
        atr_buckets = _seeded_wilder_atr(bar_hi, bar_lo, bar_cl, period, float(seed))
    # Step 4: publish bucket k's ATR to bucket k+1 onward (never to its own bars).
    available = np.concatenate(([np.nan], atr_buckets[:-1]))
    run_id = np.zeros(n, dtype=np.int64)
    run_id[starts[1:]] = 1
    final = float(atr_buckets[-1]) if np.isfinite(atr_buckets[-1]) else None
    return available[np.cumsum(run_id)], final


def compute_session_features(
    symbol: str, frame: pd.DataFrame, *, atr_seed: float | None = None
) -> SessionFeatures | None:
    """Derive the per-bar features for ONE session's 1m bars (session-aware; pure, deterministic).

    ``frame`` is a single symbol's OHLCV frame for a single IST session, ascending, tz-aware IST.
    Bars outside ``[09:15, 15:30)`` are dropped FIRST so the session VWAP, the session-local ATR and
    the volume median can never be contaminated by a pre-open or after-hours row. Returns ``None`` if
    fewer than two in-session bars survive (nothing can be both signalled and filled).

    ``atr_seed`` is WO-10b's prior-session 10m-ATR carry (``None`` ⇒ WO-10's unseeded behaviour,
    byte-identical). ONLY the stop unit is affected — VWAP, stretch, relative volume, the entry
    window, the exit ladder, the fills and the costs are untouched by the mode.
    """
    if len(frame) == 0:
        return None
    idx = pd.DatetimeIndex(frame.index)
    minute_of_day = np.asarray([t.hour * 60 + t.minute for t in idx], dtype=np.int64)
    in_session = (minute_of_day >= _OPEN_MIN) & (minute_of_day < _CLOSE_MIN)
    if not in_session.any():
        return None
    keep = np.flatnonzero(in_session)
    frame = frame.iloc[keep]
    idx = pd.DatetimeIndex(frame.index)
    minute_of_day = minute_of_day[keep]
    if len(frame) < 2:
        return None

    high, low, close = frame["high"], frame["low"], frame["close"]
    volume = frame["volume"]
    vwap_s = _session_vwap(high, low, close, volume)          # requirement 3: reused unchanged
    high_np = high.to_numpy(dtype="float64")
    low_np = low.to_numpy(dtype="float64")
    close_np = close.to_numpy(dtype="float64")
    # The STOP unit — Wilder ATR(14) on session-local 10-MINUTE bars (manager ruling 2026-08-13),
    # NOT ATR(10) on 1m bars: see :func:`session_atr_10m` and the module docstring. ``atr_seed``
    # carries WO-10b's prior-session value; ``None`` reproduces WO-10 exactly.
    atr_10m, atr_10m_final = session_atr_10m(
        high_np, low_np, close_np, minute_of_day, seed=atr_seed
    )
    med = rolling_median_volume(volume, REL_VOL_WINDOW).shift(1)
    v = volume.to_numpy(dtype="float64")
    med_np = med.to_numpy(dtype="float64")
    with np.errstate(invalid="ignore", divide="ignore"):
        rel_vol = np.where(np.isfinite(med_np) & (med_np > 0.0), v / med_np, np.nan)
        vwap_np = vwap_s.to_numpy(dtype="float64")
        stretch = np.where(vwap_np > 0.0, (vwap_np - close_np) / vwap_np * 100.0, np.nan)
    return SessionFeatures(
        symbol=symbol,
        session=idx[0].date(),
        ts=idx,
        ts_i8=idx.asi8,
        minute_of_day=minute_of_day,
        open=frame["open"].to_numpy(dtype="float64"),
        high=high_np,
        low=low_np,
        close=close_np,
        volume=v,
        vwap=vwap_np,
        atr_10m=atr_10m,
        rel_vol=rel_vol,
        stretch_pct=stretch,
        atr_10m_final=atr_10m_final,
    )


# --------------------------------------------------------------------------- one simulated trade
@dataclass(frozen=True, slots=True)
class Trade:
    """One simulated long round trip. Prices are floats (research statistics, never a ledger price)."""

    symbol: str
    session: date
    signal_ts: pd.Timestamp
    entry_ts: pd.Timestamp
    exit_ts: pd.Timestamp
    entry_price: float
    exit_price: float
    stop_price: float
    exit_reason: str
    holding_minutes: float
    stretch_pct: float                  # at the signal bar
    rel_vol: float                      # at the signal bar (NaN when the filter was off/undefined)
    gross_return_pct: float
    net_return_pct: float               # gross - the FULL round-trip cost floor (fees + spread)


def simulate_session(
    features: SessionFeatures, config: ReversionConfig, *, cost_floor_pct: float
) -> list[Trade]:
    """Simulate ``config`` over ONE symbol-session. Pure and deterministic (§9.6).

    Mechanics (design requirement 2, pinned by ``tests/unit/test_vwap_reversion.py``):

    * signal on bar ``t`` (its CLOSE is at least ``stretch_min_pct`` below the session VWAP, and — when
      the filter is active — its relative volume is under ``rel_vol_max``);
    * entry fills at bar ``t+1``'s **OPEN**, and only if that bar's timestamp is inside the
      10:00–14:00 entry window. ``t = n-1`` (the session's last usable bar) is DROPPED;
    * the stop level is ``entry_fill − STOP_ATR_MULT × ATR_10m[t]`` — Wilder ATR(14) on session-local
      10-MINUTE bars (:func:`session_atr_10m`), anchored at the FILL price. Bars inside that ATR's
      warm-up (the session's first 140 minutes ⇒ before 11:35) have ``NaN`` there and are therefore
      NOT eligible: an entry we could not stop is not booked;
    * from the entry bar onward, the first bar whose LOW <= stop (adverse, wins ties) or whose HIGH >=
      that bar's running session VWAP is the trigger; the exit fills at the NEXT bar's OPEN;
    * the 60-minute timeout is a TIMESTAMP: the exit fills at the open of the first bar at or after
      ``entry_ts + TIMEOUT_MINUTES`` — exactly 60 minutes of holding — and no trigger later than that
      is considered. If the session ends first, the exit is the session's last bar's open
      (``session_end``, an MIS obligation);
    * one position at a time: the next signal is only considered from the exit-fill bar onward.

    Every trade is charged the FULL round trip: ``net = gross − cost_floor_pct`` (requirement 4).
    """
    n = features.n
    trades: list[Trade] = []
    if n < 2:
        return trades

    with np.errstate(invalid="ignore"):
        eligible = (
            (features.stretch_pct >= config.stretch_min_pct)
            & (features.close < features.vwap)
            & np.isfinite(features.atr_10m)     # NaN through the 10m-ATR warm-up ⇒ no stop ⇒ no trade
            & (features.atr_10m > 0.0)
        )
        if config.rel_vol_max is not None:
            eligible = eligible & np.isfinite(features.rel_vol) & (features.rel_vol < config.rel_vol_max)
    # NaN never survives a comparison as True, so ``eligible`` is already a clean bool mask: a bar
    # whose VWAP/ATR/rel-vol is undefined (session warm-up) is simply not a signal.

    timeout_ns = np.int64(TIMEOUT_MINUTES) * np.int64(60_000_000_000)
    cursor = 0
    for t in np.flatnonzero(eligible):
        t = int(t)
        if t < cursor:
            continue                                     # still in a position (no overlap)
        e = t + 1
        if e >= n:
            continue                                     # session's LAST usable bar: nothing to fill in
        if not (_ENTRY_LO_MIN <= int(features.minute_of_day[e]) <= _ENTRY_HI_MIN):
            continue                                     # outside the pre-registered entry window
        entry_price = float(features.open[e])
        if not np.isfinite(entry_price) or entry_price <= 0.0:
            continue
        stop_price = entry_price - STOP_ATR_MULT * float(features.atr_10m[t])

        # --- the hard (clock-driven) exit: exactly TIMEOUT_MINUTES after the entry FILL -----------
        hard = int(np.searchsorted(features.ts_i8, features.ts_i8[e] + timeout_ns, side="left"))
        hard_reason = "timeout"
        if hard >= n:
            hard, hard_reason = n - 1, "session_end"     # forced MIS square-off at the last bar
        if hard <= e:
            continue                                     # no bar left to exit into — drop the signal

        # --- the condition-driven exits: trigger on bar i, fill at bar i+1's OPEN -----------------
        seg = slice(e, hard)                             # trigger bars e .. hard-1 (fill lands <= hard)
        with np.errstate(invalid="ignore"):
            stop_hit = features.low[seg] <= stop_price
            touch = features.high[seg] >= features.vwap[seg]
        trigger = stop_hit | touch
        if trigger.any():
            k = int(np.argmax(trigger))
            exit_idx = e + k + 1
            exit_reason = "stop" if bool(stop_hit[k]) else "vwap_touch"   # adverse wins the tie
        else:
            exit_idx, exit_reason = hard, hard_reason
        exit_price = float(features.open[exit_idx])
        if not np.isfinite(exit_price):
            continue

        gross = (exit_price - entry_price) / entry_price * 100.0
        trades.append(
            Trade(
                symbol=features.symbol,
                session=features.session,
                signal_ts=features.ts[t],
                entry_ts=features.ts[e],
                exit_ts=features.ts[exit_idx],
                entry_price=entry_price,
                exit_price=exit_price,
                stop_price=stop_price,
                exit_reason=exit_reason,
                holding_minutes=float(features.ts_i8[exit_idx] - features.ts_i8[e]) / 6.0e10,
                stretch_pct=float(features.stretch_pct[t]),
                rel_vol=float(features.rel_vol[t]),
                gross_return_pct=gross,
                net_return_pct=gross - float(cost_floor_pct),
            )
        )
        cursor = exit_idx                                # flat again only from the exit-fill bar on
    return trades


def simulate_symbol(
    symbol: str,
    frame: pd.DataFrame,
    configs: Sequence[ReversionConfig],
    *,
    cost_floor_pct: float,
    seed_atr_prior_session: bool = False,
) -> tuple[dict[ReversionConfig, list[Trade]], list[date]]:
    """Simulate every ``config`` over one symbol's multi-session 1m frame in a single pass.

    Returns ``({config: trades}, sessions_with_bars)``. The session list is the denominator input for
    the equal-weight daily series (a symbol that had bars but no trade contributes a 0.0 return).

    ``seed_atr_prior_session`` (WO-10b) turns the stop unit into a **per-symbol chain**: each session
    seeds its 10m-ATR recursion from the previous session's final value, so sessions MUST be walked in
    date order — the frame is sorted here rather than assumed, because a chain fed out of order would
    produce plausible-looking but wrong stops with no visible symptom. Day 1 of a symbol's history has
    no prior ATR and therefore falls back to the unseeded warm-up (fail-closed, WO-10's rule); a
    session that yields no ATR at all leaves the carry untouched rather than clearing it.
    """
    out: dict[ReversionConfig, list[Trade]] = {c: [] for c in configs}
    sessions: list[date] = []
    if seed_atr_prior_session and not frame.index.is_monotonic_increasing:
        frame = frame.sort_index()                   # the seeding chain is order-dependent (see above)
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
        for cfg in configs:
            out[cfg].extend(simulate_session(feats, cfg, cost_floor_pct=cost_floor_pct))
    return out, sessions


# --------------------------------------------------------------------------- accumulation → daily series
def split_of(entry_ts: pd.Timestamp) -> str:
    """WO-10b reporting split for a trade, by its ENTRY-FILL time (pure).

    :data:`SPLIT_MORNING` for fills at or before :data:`MORNING_SPLIT_END` (10:00–11:35 — the window
    WO-10's unseeded ATR warm-up could never reach), :data:`SPLIT_MIDDAY` after it (11:36–14:00 —
    exactly what WO-10 measured). The boundary is on the FILL, consistently with the entry window.
    """
    minute = entry_ts.hour * 60 + entry_ts.minute
    return SPLIT_MORNING if minute <= _MORNING_END_MIN else SPLIT_MIDDAY


@dataclass
class ConfigAccumulator:
    """Streaming aggregate for one configuration — bounded memory over a 200-symbol full-window pass.

    ``sum_by_date`` accumulates each symbol's COMPOUNDED net return for that session; the equal-weight
    daily series divides it by the number of symbols that HAD BARS that session (flat symbols
    contribute 0.0), mirroring ``SweepRunner._daily_returns``.
    """

    config: ReversionConfig
    n_trades: int = 0
    net_returns: list[float] = field(default_factory=list)
    gross_returns: list[float] = field(default_factory=list)
    exit_reasons: Counter = field(default_factory=Counter)
    sum_by_date: dict[date, float] = field(default_factory=dict)
    #: WO-10b split accounting, keyed by :data:`SPLIT_MORNING` / :data:`SPLIT_MIDDAY` (by ENTRY FILL
    #: time). REPORTING ONLY — the abort criterion is the OVERALL number, as pre-registered.
    split_net: dict[str, list[float]] = field(default_factory=dict)
    split_gross: dict[str, list[float]] = field(default_factory=dict)

    def add_symbol_session(self, session: date, trades: Sequence[Trade]) -> None:
        if not trades:
            return
        compounded = 1.0
        for tr in trades:
            self.n_trades += 1
            self.net_returns.append(tr.net_return_pct)
            self.gross_returns.append(tr.gross_return_pct)
            self.exit_reasons[tr.exit_reason] += 1
            key = split_of(tr.entry_ts)
            self.split_net.setdefault(key, []).append(tr.net_return_pct)
            self.split_gross.setdefault(key, []).append(tr.gross_return_pct)
            compounded *= 1.0 + tr.net_return_pct / 100.0
        self.sum_by_date[session] = self.sum_by_date.get(session, 0.0) + (compounded - 1.0)

    # ---- statistics -------------------------------------------------------------------------
    @property
    def expectancy_pct(self) -> float | None:
        """MEAN PER-TRADE NET RETURN AFTER THE FULL ROUND-TRIP COST, IN PERCENT.

        For :data:`STAGE1_CONFIG` this is THE WO-10 abort quantity — the "unconditioned 15-60-min
        reversion base rate after costs" whose ``<= 0`` stops the whole experiment. ``None`` when no
        trade was taken at all (also an abort: there is no base rate to clear zero with).
        """
        return float(np.mean(self.net_returns)) if self.net_returns else None

    @property
    def gross_expectancy_pct(self) -> float | None:
        return float(np.mean(self.gross_returns)) if self.gross_returns else None

    @property
    def win_rate(self) -> float | None:
        """Fraction of trades with net return > 0. Context only — NEVER the abort criterion."""
        if not self.net_returns:
            return None
        return float(np.mean(np.asarray(self.net_returns) > 0.0))

    def daily_returns(self, symbols_by_date: Mapping[date, int]) -> pd.Series:
        """The cost-adjusted DAILY net-return series ``ValidationPipeline`` consumes (§6.4 step 2).

        Float values on an ascending ``datetime.date`` index, one observation per session that had ANY
        symbol with bars — exactly the shape and semantics ``SweepRunner.returns_for`` hands the
        pipeline, so CPCV + ``fold_pass_min`` + the WO-3 margin floor apply unchanged.
        """
        days = sorted(symbols_by_date)
        if not days:
            return pd.Series(dtype="float64")
        values = [self.sum_by_date.get(d, 0.0) / max(1, symbols_by_date[d]) for d in days]
        return pd.Series(values, index=pd.Index(days), dtype="float64")

    def window_splits(self) -> list[WindowSplitStat]:
        """WO-10b: the same per-trade statistics, cut by ENTRY-FILL time into the morning window
        WO-10 could never reach and the midday window it actually measured.

        Both splits are always emitted (``n_trades=0`` when empty) so the morning increment reads
        directly against WO-10's answer without the reader having to notice a missing row. Pure
        reporting — :func:`promotion_decision` and the abort criterion never see this.
        """
        out: list[WindowSplitStat] = []
        for key, label in (
            (SPLIT_MORNING, f"{ENTRY_WINDOW_START:%H:%M}-{MORNING_SPLIT_END:%H:%M}"),
            (SPLIT_MIDDAY, f"11:36-{ENTRY_WINDOW_END:%H:%M}"),
        ):
            net = self.split_net.get(key, [])
            gross = self.split_gross.get(key, [])
            out.append(
                WindowSplitStat(
                    split=key,
                    label=label,
                    n_trades=len(net),
                    net_expectancy_pct=float(np.mean(net)) if net else None,
                    gross_expectancy_pct=float(np.mean(gross)) if gross else None,
                    win_rate=float(np.mean(np.asarray(net) > 0.0)) if net else None,
                )
            )
        return out


# --------------------------------------------------------------------------- report models
class WindowSplitStat(BaseModel):
    """WO-10b: per-trade statistics for one entry-time split. REPORTING ONLY, never a gate."""

    model_config = ConfigDict(frozen=True)

    split: str                          # SPLIT_MORNING | SPLIT_MIDDAY
    label: str                          # human-readable clock range
    n_trades: int
    net_expectancy_pct: float | None    # mean per-trade NET return within this split, %
    gross_expectancy_pct: float | None
    win_rate: float | None


class Stage1Result(BaseModel):
    """The abort-criterion pass: the UNCONDITIONED reversion base rate after costs (requirement 1)."""

    model_config = ConfigDict(frozen=True)

    stretch_min_pct: float
    rel_vol_max: float | None
    n_trades: int
    n_symbol_sessions: int
    #: THE ABORT QUANTITY — mean per-trade NET return after the full round trip, in percent.
    #: ``<= 0`` (or ``None`` = no trades) stops the experiment before stage 2 is computed.
    base_rate_pct: float | None
    gross_expectancy_pct: float | None
    win_rate: float | None              # context only
    exit_reasons: dict[str, int]
    cost_floor_pct: float
    aborted: bool
    abort_reason: str | None
    #: WO-10b: the same base rate cut 10:00–11:35 vs 11:36–14:00 (by entry-fill time). Populated ONLY
    #: in seeded mode — unseeded, the morning split is empty by construction (the ATR warm-up), so a
    #: split table would be a row of zeros pretending to be a measurement. The ABORT CRITERION IS THE
    #: OVERALL ``base_rate_pct`` in both modes, exactly as pre-registered; these are never a gate.
    window_splits: list[WindowSplitStat] = Field(default_factory=list)


class GridPointResult(BaseModel):
    """One stage-2 grid point: trade stats + the house promotion verdict, unmodified."""

    model_config = ConfigDict(frozen=True)

    stretch_min_pct: float
    rel_vol_max: float | None
    n_trades: int
    win_rate: float | None
    expectancy_pct: float | None            # mean per-trade NET return, %
    gross_expectancy_pct: float | None
    exit_reasons: dict[str, int]
    #: Hypothesis effect-size check — REPORTED, never gated (see the interpretations block).
    clears_2x_cost_floor: bool | None
    validation: ValidationReport | None
    promotable: bool
    reasons: list[str] = Field(default_factory=list)


class VwapReversionReport(BaseModel):
    """The single self-contained WO-10 deliverable (json + md). Nothing else is written."""

    model_config = ConfigDict(frozen=True)

    banner: str = BANNER
    experiment_id: str = EXPERIMENT_ID
    #: ``ABORTED_STAGE1`` | ``COMPLETED`` | ``NO_DATA``
    status: str
    requested_start: date
    requested_end: date
    data_start: date | None
    data_end: date | None
    n_symbols: int
    symbols: list[str]
    n_sessions: int
    product: str = PRODUCT
    reference_notional: str = str(REFERENCE_NOTIONAL)
    cost_floor_pct: float
    fill_mechanics: str = FILL_MECHANICS
    #: WO-10b mode. ``False`` = WO-10 as shipped (unseeded ATR, effective window 11:36–14:00);
    #: ``True`` = prior-session-seeded ATR (effective window 10:00–14:00 from day 2 of each symbol).
    seed_atr_prior_session: bool = False
    #: ``"WO-10"`` | ``"WO-10b"`` — which pre-registration this report answers.
    experiment_variant: str = "WO-10"
    trial_count_n: int
    stage1: Stage1Result | None
    stage2_ran: bool
    grid: list[GridPointResult] = Field(default_factory=list)
    modelling_notes: list[str] = Field(default_factory=list)
    generated_at: datetime


# --------------------------------------------------------------------------- modelling notes
def modelling_notes(
    cost_floor_pct: float,
    *,
    spread_pct: float | None = None,
    seed_atr_prior_session: bool = False,
) -> list[str]:
    """Every pre-registered choice, in the report (design requirement 5). Pure; no I/O."""
    variant = "WO-10b" if seed_atr_prior_session else "WO-10"
    notes = [
        f"VARIANT: {variant}. "
        + (
            "WO-10b (owner-directed 2026-08-14, pre-registered in IMPROVEMENT_SPEC.md after WO-10's "
            "abort): PRIOR-SESSION ATR SEEDING is ON. Everything else — hypothesis, grid, costs, "
            "fills, exits, entry window, two-stage structure and abort criterion — is WO-10 "
            "unchanged; this run measures the 10:00-11:35 morning window WO-10's ATR warm-up could "
            "never reach."
            if seed_atr_prior_session
            else "WO-10 as originally pre-registered: the 10m ATR warms up from scratch each session "
            "(no prior-session seeding), so the EFFECTIVE entry window is 11:36-14:00. Byte-identical "
            "to the shipped WO-10 harness; --seed-atr-prior-session was NOT passed."
        ),
        f"PRE-REGISTRATION: IMPROVEMENT_SPEC.md WO-10. Grid = the STRETCH axis only "
        f"{{{', '.join(f'{x:g}' for x in STRETCH_GRID)}}}% ⇒ trial count N = {len(STRETCH_GRID)}. "
        f"rel_vol < {REL_VOL_MAX:g}x, entries "
        f"{ENTRY_WINDOW_START:%H:%M}-{ENTRY_WINDOW_END:%H:%M}, long-only, and the exit ladder "
        f"(VWAP-touch / {TIMEOUT_MINUTES}-min timeout / {STOP_ATR_MULT:g} x "
        f"ATR({ATR_PERIOD}) on session-local {ATR_RESAMPLE_MINUTES}m bars "
        "stop) are FIXED CONSTANTS, never swept — that is what contains the multiplicity.",
        f"STOP SCALE (manager ruling 2026-08-13, pre-registered BEFORE any data run): the "
        f"'{ATR_RESAMPLE_MINUTES}-min ATR' is Wilder ATR({ATR_PERIOD}) on session-local "
        f"{ATR_RESAMPLE_MINUTES}-MINUTE bars aggregated from the 1m frame, carried onto the 1m grid "
        "with a one-bucket availability lag (no lookahead). NOT ATR(10) on 1m bars: that measures "
        "1-minute noise scale (~0.07% median measured), putting the stop at ~0.10% — below the "
        f"{cost_floor_pct:.4f}% cost floor, which is the ORB death geometry (IMPROVEMENT_SPEC "
        "F-series / §6.1 orb history) and would pre-doom the experiment by construction. WARM-UP "
        f"CONSEQUENCE: the first bar with a defined ATR is {ATR_WARMUP_MINUTES} min after the "
        "session's first bar (11:35 on a 09:15 session; first possible FILL 11:36), and earlier bars "
        "produce NO TRADE rather than an unstopped one — so the EFFECTIVE entry window is 11:36-"
        f"{ENTRY_WINDOW_END:%H:%M}, not {ENTRY_WINDOW_START:%H:%M}-{ENTRY_WINDOW_END:%H:%M}.",
        "TWO-STAGE (WO-10 abort criterion): stage 1 measures the UNCONDITIONED base rate after costs "
        "over the full window (stretch threshold 0.0%, relative-volume filter OFF, everything else "
        "identical). If it is <= 0 the run STOPS and stage 2 is never computed — 'do not tune the "
        "condition until something clears zero'.",
        f"FILLS: NEXT-1m-BAR OPEN (WO-2), never the signal bar. The condition is read from bar t's "
        "COMPLETED values and the entry fills at bar t+1's OPEN; a VWAP-touch or stop trigger observed "
        "on bar i fills at bar i+1's OPEN. A signal on a session's LAST usable bar is dropped, never "
        f"carried into the next session. The {TIMEOUT_MINUTES}-min timeout is a TIMESTAMP exit — the "
        f"open of the first bar at or after entry_ts + {TIMEOUT_MINUTES}min, i.e. exactly "
        f"{TIMEOUT_MINUTES} minutes held (house precedent for a clock-driven exit: sweep.py's forced "
        "MIS square-off, which likewise fills on its own schedule).",
        "SESSION AWARENESS: bars are grouped by IST session date and clamped to "
        f"[{SESSION_OPEN:%H:%M}, {SESSION_CLOSE:%H:%M}) before ANY feature is computed, so the session "
        "VWAP, the session-local ATR and the volume median can never be contaminated by a pre-open or "
        "after-hours row, and no comparison crosses a session boundary.",
        "VWAP = session-cumulative sum(typical_price x volume)/sum(volume) from the session's FIRST "
        "bar, typical price = (H+L+C)/3 — engine.strategy.indicators.vwap REUSED UNCHANGED (it is "
        "already cumulative-from-first-bar and delegates session slicing to the caller). No divergence "
        "from the house indicator to declare.",
        f"COSTS (WO-2 corrected surface, INCLUDING the measured spread): product {PRODUCT}, per-trade "
        f"notional ₹{REFERENCE_NOTIONAL}, full round-trip friction "
        f"{cost_floor_pct:.4f}% = CostModel.breakeven_pct(₹{REFERENCE_NOTIONAL}, '{PRODUCT}') — fees "
        "PLUS spread, not the spread-excluded fee_breakeven_pct the vectorbt sweeps use. The FULL "
        "round trip is charged on EVERY simulated trade (net = gross - the floor); there is no "
        "vectorbt here, so nothing is double-counted.",
        "PROMOTION: the house rule applied UNCHANGED — each grid point's cost-adjusted DAILY net "
        "return series (per symbol-session compounded, then the cross-sectional MEAN over the symbols "
        "that had bars that session, 0 for a flat name — SweepRunner._daily_returns' convention) is "
        f"fed to ValidationPipeline: skfolio CPCV + fold_pass_min(N={len(STRETCH_GRID)}) + the WO-3 "
        "margin floor, with cost_floor_pct passed explicitly as the MIS round trip so the floor cannot "
        "fall back to the CNC default.",
        "LONG-ONLY (WO-10 'long side only'; the §1.4.9 shorts gate is shut). Per-symbol equal weight; "
        "no §7.1 portfolio limits, position caps or sizing are modelled — those are the gate/paper "
        "layer's job (Phase 2/3), deliberately outside a raw-edge experiment.",
        "SCOPE: this is a C-category experiment. It has NO live wiring, writes NO param_sets row, and "
        "STOPS FOR OWNER REVIEW. A negative or aborted result is a valid deliverable (C9) and is "
        "reported as measured, never massaged.",
    ]
    if seed_atr_prior_session:
        notes.insert(
            2,
            "PRIOR-SESSION ATR SEEDING (WO-10b, pre-registered verbatim): 'the Wilder ATR(14) "
            "recursion continues from the previous session's final 10m-ATR value, with the current "
            "session's FIRST bucket's true range computed high-low only (no prev-close term: the "
            "overnight gap must never enter the stop unit); a symbol with no prior-session ATR "
            "produces no trades until its own warm-up completes (fail-closed, same rule as WO-10).' "
            "Implementation: a per-SYMBOL chain walked in DATE ORDER — session k's final 10m-bucket "
            "ATR seeds session k+1, so the availability lag costs one bucket (ATR from 09:25) instead "
            f"of {ATR_WARMUP_MINUTES} minutes, and the entry window is the registered "
            f"{ENTRY_WINDOW_START:%H:%M}-{ENTRY_WINDOW_END:%H:%M} from day 2 of each symbol's history "
            "onward. Day 1 falls back to the unseeded warm-up. ONLY the stop unit changes; VWAP, "
            "stretch, relative volume, fills, exits and costs are untouched.",
        )
        notes.insert(
            3,
            f"WINDOW SPLITS (WO-10b reporting): the stage-1 base rate is reported OVERALL and cut by "
            f"ENTRY-FILL time into {ENTRY_WINDOW_START:%H:%M}-{MORNING_SPLIT_END:%H:%M} (the morning "
            f"increment, unreachable under WO-10) and 11:36-{ENTRY_WINDOW_END:%H:%M} (exactly what "
            "WO-10 measured), so the increment reads directly against WO-10's answer. THE ABORT "
            "CRITERION IS THE OVERALL NUMBER, as pre-registered — the splits are REPORTING, never "
            "gates, and no decision in this run reads them.",
        )
    if spread_pct is not None:
        notes.append(
            f"Spread provenance: config/costs.yaml spread_pct = {spread_pct:.4f}% (full quoted "
            "spread; NIFTY200 median over 48 symbol-days, per-tier 0.0135/0.0180/0.0219%, p75 "
            "0.0303% — IMPROVEMENT_SPEC.md Part III). A round trip pays it once."
        )
    notes.append("--- INTERPRETATIONS where the WO text was under-determined (auditable, not implicit) ---")
    notes.extend(PRE_REGISTRATION_INTERPRETATIONS)
    return notes


# --------------------------------------------------------------------------- the experiment runner
#: ``symbol -> 1m OHLCV frame or None``. Injectable so the offline test tier never opens the store.
FrameLoader = Callable[[str], "pd.DataFrame | None"]


class VwapReversionExperiment:
    """Runs the two-stage WO-10 experiment and returns the report. Blocking/standalone by design.

    Parameters
    ----------
    loader:
        ``symbol -> 1m OHLCV frame`` (tz-aware IST index, columns open/high/low/close/volume).
        :meth:`from_store` builds the production loader over ``MarketStore.get_bars_1m_frame`` — the
        same bulk read path ``SweepRunner``'s intraday branch uses (read-only, float frames).
    cost_floor_pct:
        The FULL round-trip friction at ₹20,000 MIS, in percent (fees + spread). Use
        :func:`cost_floor_pct_for` to derive it from the house ``CostModel``.
    clock:
        The single source of "now" (§3.2) — stamps ``generated_at``.
    splitter:
        Optional CPCV splitter override, forwarded to ``ValidationPipeline`` (the offline test tier
        injects a deterministic splitter instead of importing skfolio).
    seed_atr_prior_session:
        WO-10b mode. ``False`` (default) is WO-10 exactly — no code path differs, the ATR seed is
        simply never supplied. ``True`` chains each symbol's 10m-ATR across sessions so the morning
        window becomes measurable.
    """

    def __init__(
        self,
        *,
        loader: FrameLoader,
        cost_floor_pct: float,
        clock: Clock,
        splitter: Callable[[int], list[tuple[np.ndarray, np.ndarray]]] | None = None,
        spread_pct: float | None = None,
        seed_atr_prior_session: bool = False,
    ) -> None:
        self._loader = loader
        self._cost_floor_pct = float(cost_floor_pct)
        self._clock = clock
        self._splitter = splitter
        self._spread_pct = spread_pct
        self._seed_atr = bool(seed_atr_prior_session)

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
        splitter: Callable[[int], list[tuple[np.ndarray, np.ndarray]]] | None = None,
        seed_atr_prior_session: bool = False,
    ) -> VwapReversionExperiment:
        """Production wiring: the 1m bulk read path ``SweepRunner`` uses, READ-ONLY (never writes)."""

        def loader(symbol: str) -> pd.DataFrame | None:
            start_dt = datetime.combine(start, time(0, 0), tzinfo=IST)
            end_dt = datetime.combine(end + timedelta(days=1), time(0, 0), tzinfo=IST)
            df = store.get_bars_1m_frame(symbol, start_dt, end_dt)
            return df if len(df) else None

        return cls(
            loader=loader,
            cost_floor_pct=cost_floor_pct_for(cost_model),
            clock=clock,
            splitter=splitter,
            spread_pct=float(cost_model.spread_pct),
            seed_atr_prior_session=seed_atr_prior_session,
        )

    # ------------------------------------------------------------------ the two-stage run
    def run(self, symbols: Sequence[str], *, start: date, end: date) -> VwapReversionReport:
        """Stage 1 (abort criterion) then — only if it clears zero — the pre-registered 3 x 1 grid."""
        syms = list(dict.fromkeys(symbols))
        notes = modelling_notes(
            self._cost_floor_pct,
            spread_pct=self._spread_pct,
            seed_atr_prior_session=self._seed_atr,
        )

        stage1_acc, symbols_by_date, seen = self._pass([STAGE1_CONFIG], syms)
        stage1_acc = stage1_acc[STAGE1_CONFIG]
        n_symbol_sessions = sum(symbols_by_date.values())
        span = (min(symbols_by_date), max(symbols_by_date)) if symbols_by_date else (None, None)

        if not symbols_by_date:
            return self._report(
                status="NO_DATA", requested=(start, end), span=span, symbols=seen,
                symbols_by_date=symbols_by_date, stage1=None, grid=[], stage2_ran=False, notes=notes,
            )

        base_rate = stage1_acc.expectancy_pct
        abort_reason = self._abort_reason(base_rate, stage1_acc.n_trades)
        stage1 = Stage1Result(
            stretch_min_pct=STAGE1_CONFIG.stretch_min_pct,
            rel_vol_max=STAGE1_CONFIG.rel_vol_max,
            n_trades=stage1_acc.n_trades,
            n_symbol_sessions=n_symbol_sessions,
            base_rate_pct=base_rate,
            gross_expectancy_pct=stage1_acc.gross_expectancy_pct,
            win_rate=stage1_acc.win_rate,
            exit_reasons=dict(sorted(stage1_acc.exit_reasons.items())),
            cost_floor_pct=self._cost_floor_pct,
            aborted=abort_reason is not None,
            abort_reason=abort_reason,
            # WO-10b only: unseeded, the morning split is empty by construction (the ATR warm-up),
            # so an all-zero split table would masquerade as a measurement.
            window_splits=stage1_acc.window_splits() if self._seed_atr else [],
        )
        _log.info(
            "vwap_reversion_stage1",
            base_rate_pct=base_rate,
            n_trades=stage1_acc.n_trades,
            aborted=stage1.aborted,
        )
        if stage1.aborted:
            # WO-10: "stop — do not tune the condition until something clears zero." Stage 2 is not
            # computed at all, so no grid statistic can leak into the record.
            return self._report(
                status="ABORTED_STAGE1", requested=(start, end), span=span, symbols=seen,
                symbols_by_date=symbols_by_date, stage1=stage1, grid=[], stage2_ran=False, notes=notes,
            )

        configs = grid_configs()
        accs, symbols_by_date2, seen2 = self._pass(configs, syms)
        grid = [
            self._validate_grid_point(accs[cfg], symbols_by_date2)
            for cfg in configs
        ]
        span2 = (min(symbols_by_date2), max(symbols_by_date2)) if symbols_by_date2 else span
        return self._report(
            status="COMPLETED", requested=(start, end), span=span2, symbols=seen2 or seen,
            symbols_by_date=symbols_by_date2 or symbols_by_date, stage1=stage1, grid=grid,
            stage2_ran=True, notes=notes,
        )

    # ------------------------------------------------------------------ internals
    @staticmethod
    def _abort_reason(base_rate: float | None, n_trades: int) -> str | None:
        """The WO-10 abort test, in one place. ``None`` ⇒ stage 2 may run."""
        if n_trades == 0 or base_rate is None:
            return (
                "ABORTED: the unconditioned pass produced ZERO trades over the full window, so there "
                "is no reversion base rate to clear zero with. Fail closed (WO-10 abort criterion)."
            )
        if base_rate <= 0.0:
            return (
                f"ABORTED: the UNCONDITIONED 15-{TIMEOUT_MINUTES}-min reversion base rate after costs "
                f"is {base_rate:+.5f}% per trade (<= 0). WO-10: 'stop — do not tune the condition "
                "until something clears zero.' The stretch grid was NOT evaluated."
            )
        return None

    def _pass(
        self, configs: Sequence[ReversionConfig], symbols: Sequence[str]
    ) -> tuple[dict[ReversionConfig, ConfigAccumulator], dict[date, int], list[str]]:
        """One read pass over ``symbols``, evaluating every ``config`` on each symbol's frame.

        Returns ``({config: accumulator}, {session: n_symbols_with_bars}, symbols_with_bars)``.
        Symbol-at-a-time so a 200-name full-window pass never holds more than one symbol's frame.
        """
        accs = {c: ConfigAccumulator(config=c) for c in configs}
        symbols_by_date: dict[date, int] = {}
        seen: list[str] = []
        for sym in symbols:
            frame = self._loader(sym)
            if frame is None or len(frame) == 0:
                continue
            seen.append(sym)
            by_config, sessions = simulate_symbol(
                sym,
                frame,
                configs,
                cost_floor_pct=self._cost_floor_pct,
                seed_atr_prior_session=self._seed_atr,   # WO-10b: per-symbol chain, date order
            )
            for d in sessions:
                symbols_by_date[d] = symbols_by_date.get(d, 0) + 1
            for cfg in configs:
                per_session: dict[date, list[Trade]] = {}
                for tr in by_config[cfg]:
                    per_session.setdefault(tr.session, []).append(tr)
                for d, trs in per_session.items():
                    accs[cfg].add_symbol_session(d, trs)
        return accs, symbols_by_date, seen

    def _validate_grid_point(
        self, acc: ConfigAccumulator, symbols_by_date: Mapping[date, int]
    ) -> GridPointResult:
        """Run the HOUSE promotion pipeline over this grid point's daily series, unmodified."""
        series = acc.daily_returns(symbols_by_date)
        pipeline = ValidationPipeline(
            returns_provider=lambda _sid, _params: series,
            clock=self._clock,
            conn=None,                       # no param_sets row — WO-10: no live wiring of any kind
            reports_dir=None,                # the experiment report is the ONLY artifact
            splitter=self._splitter,
        )
        params = ParamSet(
            strategy_id=EXPERIMENT_ID,
            params=acc.config.params,
            trial_count_n=len(STRETCH_GRID),          # N = the grid cardinality (§6.4 step 1)
            sweep_stats={
                "n_trades": float(acc.n_trades),
                "win_rate": acc.win_rate,
                "net_expectancy_pct_per_trade": acc.expectancy_pct,
                "gross_expectancy_pct_per_trade": acc.gross_expectancy_pct,
            },
            # Explicit so the WO-3 floor uses the MIS round trip these returns were charged, never
            # default_cost_floor_pct's CNC fallback for an unmapped strategy.
            cost_floor_pct=self._cost_floor_pct,
        )
        report = pipeline.validate_sync(EXPERIMENT_ID, params)
        expectancy = acc.expectancy_pct
        return GridPointResult(
            stretch_min_pct=acc.config.stretch_min_pct,
            rel_vol_max=acc.config.rel_vol_max,
            n_trades=acc.n_trades,
            win_rate=acc.win_rate,
            expectancy_pct=expectancy,
            gross_expectancy_pct=acc.gross_expectancy_pct,
            exit_reasons=dict(sorted(acc.exit_reasons.items())),
            clears_2x_cost_floor=(
                None if expectancy is None
                else expectancy >= HYPOTHESIS_EDGE_MULTIPLE * self._cost_floor_pct
            ),
            validation=report,
            promotable=report.promotable,
            reasons=list(report.reasons),
        )

    def _report(
        self,
        *,
        status: str,
        requested: tuple[date, date],
        span: tuple[date | None, date | None],
        symbols: Sequence[str],
        symbols_by_date: Mapping[date, int],
        stage1: Stage1Result | None,
        grid: Sequence[GridPointResult],
        stage2_ran: bool,
        notes: Sequence[str],
    ) -> VwapReversionReport:
        return VwapReversionReport(
            status=status,
            requested_start=requested[0],
            requested_end=requested[1],
            data_start=span[0],
            data_end=span[1],
            n_symbols=len(symbols),
            symbols=list(symbols),
            n_sessions=len(symbols_by_date),
            cost_floor_pct=round(self._cost_floor_pct, 6),
            seed_atr_prior_session=self._seed_atr,
            experiment_variant="WO-10b" if self._seed_atr else "WO-10",
            trial_count_n=len(STRETCH_GRID),
            stage1=stage1,
            stage2_ran=stage2_ran,
            grid=list(grid),
            modelling_notes=list(notes),
            generated_at=self._clock.now(),
        )


def cost_floor_pct_for(cost_model: CostModel) -> float:
    """FULL round-trip friction (fees + spread, WO-2) at ₹20,000 MIS, in percent (requirement 4).

    ``breakeven_pct`` — deliberately NOT ``fee_breakeven_pct``: the latter excludes the spread because
    the vectorbt sweeps charge it separately as slippage; this harness has no vectorbt, so the spread
    must come from the cost model or it is not charged at all.
    """
    return float(cost_model.breakeven_pct(REFERENCE_NOTIONAL, PRODUCT))


# --------------------------------------------------------------------------- report rendering
def _pct(value: float | None, dp: int = 4) -> str:
    return "—" if value is None else f"{value:+.{dp}f}%"


def render_markdown(report: VwapReversionReport) -> str:
    """Render the experiment report — banner first, then the abort criterion, then the grid (C9)."""
    r = report
    lines: list[str] = []
    lines.append(
        f"# {r.experiment_variant} experiment — intraday VWAP-deviation reversion "
        f"(`{r.experiment_id}`)"
    )
    lines.append("")
    lines.append(f"> **{r.banner}**")
    lines.append("")
    lines.append(
        f"_Generated {r.generated_at.isoformat()} · pre-registered in IMPROVEMENT_SPEC.md "
        f"{r.experiment_variant}_"
    )
    lines.append("")

    # ---- status banner (first, always) -------------------------------------------------------
    if r.status == "ABORTED_STAGE1":
        lines.append("## STATUS: ABORTED AT STAGE 1 — the pre-registered abort criterion tripped")
        lines.append("")
        lines.append(
            "The stretch grid was **NOT evaluated**. WO-10: _\"if the UNCONDITIONED 15-60-min "
            "reversion base rate after costs is <= 0 in the first full-window pass, stop — do not "
            "tune the condition until something clears zero.\"_"
        )
    elif r.status == "NO_DATA":
        lines.append("## STATUS: NO DATA — no requested symbol had 1m bars in the window")
        lines.append("")
        lines.append("Nothing was measured; no verdict here means anything. Check the window/universe.")
    else:
        lines.append("## STATUS: COMPLETED — stage 1 cleared zero; the 3-point stretch grid was evaluated")
    lines.append("")

    # ---- run context ---------------------------------------------------------------------------
    span = f"{r.data_start} → {r.data_end}" if r.data_start else "—"
    lines.append("## Run")
    lines.append("")
    lines.append(f"- Requested window: {r.requested_start} → {r.requested_end}  ·  resolved: {span}")
    lines.append(f"- Symbols with bars: {r.n_symbols}  ·  sessions: {r.n_sessions}")
    lines.append(
        f"- Costs: {r.product}, ₹{r.reference_notional}/trade, full round-trip friction "
        f"**{r.cost_floor_pct:.4f}%** (fees **+ spread**, WO-2) charged on EVERY trade"
    )
    lines.append(f"- Fills: `{r.fill_mechanics}` (never the signal bar)  ·  trial count N = {r.trial_count_n}")
    if r.seed_atr_prior_session:
        lines.append(
            f"- Stop unit: {STOP_ATR_MULT:g} x Wilder ATR({ATR_PERIOD}) on session-local "
            f"{ATR_RESAMPLE_MINUTES}-minute bars, **PRIOR-SESSION SEEDED (WO-10b)** — each symbol's "
            "ATR recursion continues from the previous session's final value, with the first bucket's "
            "TR taken high−low only so the overnight gap never enters the stop. Effective entry "
            f"window: the registered **{ENTRY_WINDOW_START:%H:%M}–{ENTRY_WINDOW_END:%H:%M}** from day "
            "2 of each symbol's history (day 1 falls back to the unseeded warm-up, fail-closed)."
        )
    else:
        lines.append(
            f"- Stop unit: {STOP_ATR_MULT:g} x Wilder ATR({ATR_PERIOD}) on session-local "
            f"{ATR_RESAMPLE_MINUTES}-minute bars (manager ruling 2026-08-13), **unseeded (WO-10)**. "
            f"Its {ATR_WARMUP_MINUTES}-min warm-up dominates the pre-registered "
            f"{ENTRY_WINDOW_START:%H:%M} opening, so the **effective entry window is "
            f"11:36–{ENTRY_WINDOW_END:%H:%M}** — earlier bars produce NO trade rather than an "
            "unstopped one."
        )
    lines.append("")

    # ---- stage 1: the abort criterion -----------------------------------------------------------
    if r.stage1 is not None:
        s = r.stage1
        lines.append("## Stage 1 — UNCONDITIONED reversion base rate after costs (the abort criterion)")
        lines.append("")
        lines.append(
            f"No stretch condition (threshold {s.stretch_min_pct:g}%), no relative-volume filter "
            f"({'OFF' if s.rel_vol_max is None else s.rel_vol_max}); identical entry window, exit "
            "ladder, fill mechanics and costs."
        )
        lines.append("")
        lines.append(
            f"> ### BASE RATE = {_pct(s.base_rate_pct, 5)} per trade, after costs\n"
            "> _Definition: the MEAN PER-TRADE NET RETURN (gross minus the full round-trip cost "
            "floor), in percent — a SIGNED quantity, because the criterion is `<= 0`. The win "
            "fraction below is context, never the criterion._"
        )
        lines.append("")
        lines.append(f"- Trades: {s.n_trades}  ·  symbol-sessions scanned: {s.n_symbol_sessions}")
        lines.append(f"- Gross expectancy (before costs): {_pct(s.gross_expectancy_pct, 5)}")
        lines.append(
            f"- Win rate (net > 0): {'—' if s.win_rate is None else f'{s.win_rate:.1%}'}"
            f"  ·  cost floor: {s.cost_floor_pct:.4f}%"
        )
        if s.exit_reasons:
            lines.append("- Exit reasons: " + ", ".join(f"`{k}` {v}" for k, v in s.exit_reasons.items()))
        lines.append("")
        if s.window_splits:
            lines.append("### Entry-window splits (WO-10b) — REPORTING ONLY, never a gate")
            lines.append("")
            lines.append(
                "The abort criterion above is the OVERALL number, exactly as pre-registered. This cut "
                "exists so the **morning increment** WO-10's ATR warm-up could not reach reads "
                "directly against the midday window WO-10 actually measured."
            )
            lines.append("")
            lines.append("| entry window | trades | win% | net expectancy/trade | gross/trade |")
            lines.append("|:-------------|-------:|-----:|---------------------:|------------:|")
            for sp in s.window_splits:
                win = "—" if sp.win_rate is None else f"{sp.win_rate:.1%}"
                note = " _(unreachable under WO-10)_" if sp.split == SPLIT_MORNING else " _(= WO-10's window)_"
                lines.append(
                    f"| {sp.label}{note} | {sp.n_trades} | {win} | "
                    f"{_pct(sp.net_expectancy_pct, 5)} | {_pct(sp.gross_expectancy_pct, 5)} |"
                )
            lines.append("")
        if s.aborted:
            lines.append(f"**{s.abort_reason}**")
            lines.append("")

    # ---- stage 2: the grid ------------------------------------------------------------------------
    lines.append("## Stage 2 — the pre-registered 3 x 1 stretch grid")
    lines.append("")
    if not r.stage2_ran:
        lines.append("_NOT RUN — stage 1's abort criterion tripped (see above). Nothing was tuned._")
        lines.append("")
    else:
        lines.append("| stretch >= | rel-vol | trades | win% | net expectancy/trade | gross/trade | >= 2x cost floor |")
        lines.append("|-----------:|--------:|-------:|-----:|---------------------:|------------:|:-----------------|")
        for g in r.grid:
            win = "—" if g.win_rate is None else f"{g.win_rate:.1%}"
            clears = "—" if g.clears_2x_cost_floor is None else ("YES" if g.clears_2x_cost_floor else "no")
            relvol = "off" if g.rel_vol_max is None else f"<{g.rel_vol_max:g}x"
            lines.append(
                f"| {g.stretch_min_pct:g}% | {relvol} | {g.n_trades} | {win} | "
                f"{_pct(g.expectancy_pct, 5)} | {_pct(g.gross_expectancy_pct, 5)} | {clears} |"
            )
        lines.append("")
        lines.append(
            "_The `>= 2x cost floor` column is the HYPOTHESIS' effect-size claim, reported for the "
            "record. It is NOT a gate — the promotion bar is the house rule below._"
        )
        lines.append("")

        # ---- promotion verdicts -------------------------------------------------------------
        lines.append("## Promotion — house CPCV + fold_pass_min + the WO-3 margin floor (unmodified)")
        lines.append("")
        for g in r.grid:
            v = g.validation
            lines.append(f"### stretch >= {g.stretch_min_pct:g}% — {'PROMOTABLE' if g.promotable else 'NOT PROMOTABLE'}")
            lines.append("")
            if v is None:
                lines.append("_No validation report._")
                lines.append("")
                continue
            frac = "n/a" if v.cpcv_fold_pass_fraction is None else f"{v.cpcv_fold_pass_fraction:.1%}"
            bar = "n/a" if v.fold_pass_min is None else f"{v.fold_pass_min:.0%}"
            floor = (
                "n/a" if v.margin_floor_pct_per_day is None
                else f"{v.margin_floor_pct_per_day:.5f}%/day"
            )
            med = (
                "no passing splits" if v.cpcv_median_passing_expectancy_pct is None
                else f"{v.cpcv_median_passing_expectancy_pct:+.5f}%/day"
            )
            lines.append(f"- Observations (sessions): {v.n_obs}  ·  N cited: {v.trial_count_n}")
            lines.append(f"- Daily expectancy: {_pct(v.expectancy_pct, 5)}  ·  total: {_pct(v.total_return_pct, 3)}")
            lines.append(
                f"- Max drawdown: "
                f"{'—' if v.max_drawdown_pct is None else f'{v.max_drawdown_pct:.2f}%'}"
            )
            lines.append(f"- CPCV fold-pass: **{frac}** vs required **{bar}** ({len(v.cpcv)} splits)")
            lines.append(f"- WO-3 margin floor: {floor}  ·  median passing split: {med}")
            if v.cpcv:
                lines.append("")
                lines.append("| split | test obs | expectancy | pass (>0 after costs) |")
                lines.append("|------:|---------:|-----------:|:----------------------|")
                for f in v.cpcv:
                    lines.append(
                        f"| {f.split} | {f.n_test_obs} | {_pct(f.expectancy_pct, 5)} | "
                        f"{'PASS' if f.passed else 'fail'} |"
                    )
            if g.reasons:
                lines.append("")
                lines.append("Reasons NOT promotable:")
                for reason in g.reasons:
                    lines.append(f"- {reason}")
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


def write_report(report: VwapReversionReport, reports_dir: str | Path) -> ReportArtifacts:
    """Write ``vwap_reversion_<ts>.md`` + ``.json`` under ``reports_dir`` (design requirement 5)."""
    out = Path(reports_dir)
    out.mkdir(parents=True, exist_ok=True)
    stem = f"{EXPERIMENT_ID}_{report.generated_at.strftime('%Y%m%dT%H%M%S')}"
    md_path = out / f"{stem}.md"
    json_path = out / f"{stem}.json"
    md_path.write_text(render_markdown(report), encoding="utf-8")
    json_path.write_text(report.model_dump_json(indent=2), encoding="utf-8")
    return ReportArtifacts(markdown=md_path, json=json_path)


__all__ = [
    "ATR_PERIOD",
    "ATR_RESAMPLE_MINUTES",
    "ATR_WARMUP_MINUTES",
    "BANNER",
    "MORNING_SPLIT_END",
    "SPLIT_MIDDAY",
    "SPLIT_MORNING",
    "WindowSplitStat",
    "split_of",
    "ENTRY_WINDOW_END",
    "ENTRY_WINDOW_START",
    "EXPERIMENT_ID",
    "FILL_MECHANICS",
    "PRE_REGISTRATION_INTERPRETATIONS",
    "PRODUCT",
    "REFERENCE_NOTIONAL",
    "REL_VOL_MAX",
    "REL_VOL_WINDOW",
    "STAGE1_CONFIG",
    "STOP_ATR_MULT",
    "STRETCH_GRID",
    "TIMEOUT_MINUTES",
    "ConfigAccumulator",
    "GridPointResult",
    "ReversionConfig",
    "SessionFeatures",
    "Stage1Result",
    "Trade",
    "VwapReversionExperiment",
    "VwapReversionReport",
    "compute_session_features",
    "cost_floor_pct_for",
    "grid_configs",
    "modelling_notes",
    "render_markdown",
    "session_atr_10m",
    "session_slices",
    "simulate_session",
    "simulate_symbol",
    "write_report",
]
