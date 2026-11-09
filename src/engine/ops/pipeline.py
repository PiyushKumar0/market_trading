"""The RECOMMEND pipeline — signal ⇒ analyst ⇒ gate ⇒ owner.

Lives in ``engine.ops`` because it is the only package allowed to import everything: it
wires Tier-1 (``intelligence``), Tier-2 (``risk``), the store, the notifier and the state DB into one
flow. Nothing here places a broker order — in RECOMMEND the platform places **zero** API orders
including GTTs; the ``manual_checklist`` on every recommendation is the mechanism that transfers
protective-order responsibility to the human explicitly. The paper branch hands a paper-approved
proposal to the paper OrderManager seam, which only reaches a PaperBroker.

Two objects:

* :class:`RecommendationBook` — persistence + human-outcome capture. It owns the
  ``recommendations`` / ``learning_ledger`` / ``positions`` rows and implements the three coroutines
  the Telegram ``RecoBook`` seam depends on (``take``/``close``/``veto``), plus ``expire_stale``.
  The **non-fill is training signal**: a recommendation that reaches ``valid_until`` unconfirmed is
  labelled ``no_action`` in the ledger so attribution is not biased toward only the trades the owner
  happened to take.
* :class:`RecommendationPipeline` — the trigger handlers. ``on_signal_candidate`` (trigger a) is
  the entry path and is **window-gated**; ``on_bar`` (trigger b) and ``time_exit_check`` (the
  time exit) are risk-reducing and therefore **never** window-gated.

Conventions that are load-bearing here:

* **No LLM-originated time.** ``valid_until`` is platform-stamped by :meth:`_ttl` from
  ``Clock``/``NSECalendar``; any value the model emits is discarded by ``parse_intraday``.
* **Fail to no-proposal.** A governor block, a schema failure, a timeout or an SDK death all
  resolve to "no recommendation + owner alert" — never to a salvaged/guessed action.
* **Exits never depend on an LLM.** :meth:`time_exit_check` builds its ``ExitAction``
  deterministically in Python and never calls the harness.
* **Money is ``Decimal``, timestamps are tz-aware IST strings** in every row written here.
* **The analyst slot goes to the best pending candidate, not the earliest.** Published candidates
  queue; each slot the forward cap grants is spent on the highest per-strategy score QUANTILE
  waiting (``_forward_key``), and the counter that bounds those slots lives in the day-slot journal
  so a mid-day restart resumes the day's quota instead of refilling it. Rollback:
  ``admission_mode="arrival"``.
* **A slot is spent only by something that was actually looked at.** The queue is process memory
  and a restart drops it, but every exit that leaves a candidate unjudged hands its prescreen
  admission slot back: a restart (``_rearm_forward_orphans``), the window closing under a queued
  candidate (``_flush_forwards_at_window_close``), and a level-pinned candidate whose level walked
  outside the entry-sanity band (``_outside_entry_band``). An analyst call that returned no verdict
  also gives back its forward charge (``_refund_forward``).
* **The queue is drained on a CADENCE, not on arrival.** An arriving candidate only enqueues;
  :meth:`RecommendationPipeline.drain_forward_queue` — pulsed from the scheduler — spends at most one
  slot every ``FORWARD_PACING_MIN`` minutes on the best pending candidate, because ranking can only
  rank what has accumulated. Rollback: ``forward_drain_mode="immediate"``.
* **The trigger paths carry state instead of re-reading it.** ATR(14,1m) is seeded from the store
  once per symbol per day and advanced one Wilder step per bar
  (:meth:`RecommendationPipeline._atr_1m`), reseeding only when a bar's minute is not contiguous with
  the last one seen; the weekly sector map is read once per trading date
  (:meth:`RecommendationPipeline._sector_of`). ``hot_path_read_hygiene`` logs the reads
  avoided/performed once per session.
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from datetime import date, datetime, timedelta
from decimal import ROUND_FLOOR, ROUND_HALF_UP, Decimal
from pathlib import Path
from time import perf_counter
from typing import Any

import numpy as np
from ulid import ULID

from engine.core.calendar import NSECalendar
from engine.core.clock import Clock
from engine.core.config import RecommendSettings
from engine.core.contracts import (
    EnterAction,
    ExitAction,
    GateVerdict,
    ModifyStopAction,
    Recommendation,
)
from engine.core.db import transaction
from engine.core.enums import Mode, RiskState
from engine.core.log import get_logger
from engine.core.recommendations import recommendation_expired
from engine.core.scope import scope_sql
from engine.core.types import Bar
from engine.features.snapshots import FEATURE_SET_VERSION
from engine.intelligence.harness import ACCOUNT_FAIL_REASONS
from engine.intelligence.schemas import (
    NoActionOutput,
    intraday_guidance_json_schema,
    parse_intraday,
)
from engine.learning.evidence import evidence_line
from engine.learning.hi52_forward import (
    PROMOTION_DATE,
    forward_line,
    forward_progress,
    load_series,
    load_signal_rows,
)
from engine.notify import catalog
from engine.notify.catalog import CatalogMessage, MessageKind
from engine.notify.episodes import AlertEpisodes
from engine.ops.holdings_reconcile import MissingHolding, positions_missing_from_holdings
from engine.ops.holds import HoldFn, exit_session, is_time_exit
from engine.ops.scorecard import symbol_history_line
from engine.ops.warmup import CLASS_INTRADAY, blocker_symbol
from engine.strategy.cost_model import CostModel
from engine.strategy.indicators import _true_range, wilder_atr
from engine.strategy.scanners import hi52, ins
from engine.strategy.types import RawLevels, SignalCandidate, round_to_tick

_log = get_logger("engine.ops.pipeline")

NotifyFn = Callable[[CatalogMessage], Awaitable[None]]
#: ``submit(proposal, paper_verdict)``: place the paper CNC entry (the paper OrderManager seam).
PaperSubmitFn = Callable[[EnterAction, GateVerdict], Awaitable[Any]]

#: The agent this pipeline drives. Its ``agents.yaml`` key, the governor's spend key and the
#: ``agent_calls.agent_id`` are all this one string.
INTRADAY_AGENT_ID = "intraday_analyst"

#: ``agent_id`` stamped on a DETERMINISTIC (no-LLM) proposal — the time exit.
#: Distinct from the analyst id so the ledger can separate model decisions from platform decisions.
PLATFORM_AGENT_ID = "platform"

#: Entry-recommendation TTL for an intraday candidate, minutes [tunable]. Every swing/position
#: recommendation is stamped to today's session close (see :meth:`RecommendationPipeline._ttl`).
TTL_INTRADAY_MIN = 20

#: ``/taken`` says so when the fill carries more stop risk than the ``per_trade_risk`` verdict
#: approved. ``take`` never gates on ``valid_until``, so a rec can be confirmed the morning after it
#: expired while its zone, stop and qty stay the PRIOR session's (:meth:`RecommendationBook.take`).
#: The gate sizes a swing/position on ``overnight_gap_mult`` × the stop distance
#: (``_rule_per_trade_risk``), so for those styles the basis IS that number, read through the book's
#: ``overnight_gap_mult_fn`` seam, and the call-out fires only once the approved budget is exceeded.
#: This constant is the FALLBACK materiality threshold (× the zone-edge-to-stop distance) for an
#: intraday fill, whose verdict carries no gap allowance, and for a book wired without the seam.
#: A WARNING, never a refusal: a position the owner really opened must be tracked. Not a limit.
TAKEN_STOP_RISK_WARN_MULT = Decimal("1.25")

#: Trigger (b) stop-proximity: fire when the LTP is within this many ATR(14,1m) of the stop.
STOP_PROXIMITY_ATR_MULT = Decimal("0.5")

#: One position event per position per this many minutes — a tick stream sitting at 0.49×ATR would
#: otherwise bill an analyst call per bar.
POSITION_EVENT_DEBOUNCE_MIN = 60

#: ATR(14,1m) inputs: Wilder period and how many trailing 1m bars to read for it.
ATR_PERIOD = 14
ATR_BAR_TAIL = 60

#: The 1m bar cadence. A bar exactly this far past the last one CONTINUES the Wilder recursion;
#: anything further ahead is a gap and reseeds from the store.
_BAR_STEP = timedelta(minutes=1)

#: The per-session hot-path counters behind the ``hot_path_read_hygiene`` log line.
_HOT_PATH_COUNTERS = (
    "atr_store_reads",      # _atr_1m calls that had to read bars_1m (seed or gap reseed)
    "atr_incremental",      # _atr_1m calls served from carried state — the reads AVOIDED
    "atr_gap_reseeds",      # of the store reads, how many were non-contiguity reseeds
    "sector_store_reads",   # sector-map reads performed
    "sector_cache_hits",    # sector-map reads avoided
)
_HOT_PATH_TIMERS = ("atr_store_read_ms", "atr_incremental_ms", "sector_store_read_ms")

#: The deterministic time-exit thesis. A constant, not model text: an exit must survive
#: with the LLM dead, so nothing on this path may be generated.
TIME_STOP_THESIS = (
    "Deterministic time exit: this position has reached the end of its hold. "
    "No model was consulted — exits and stops never depend on an LLM response."
)

#: Forward-selection modes. ``ranked`` = the priority queue below;
#: ``arrival`` = first-come-first-served, kept ONLY as the config rollback.
FORWARD_MODES = ("ranked", "arrival")

#: How many quantile BANDS the per-strategy score distribution is cut into for forward selection.
#: Quintiles: coarse enough that two candidates of comparable standing inside their own strategies
#: are treated as equals (and ordered by fired_at), fine enough to separate a strategy's best from
#: its middling ones. A finer grid would spuriously rank a 0.71 above a 0.69 across strategies.
QUANTILE_BANDS = 5

#: Smallest score population that can RANK a candidate at all. The empirical CDF of a ONE-element
#: population can only return 1.0 (the candidate is measured against itself and takes the TOP band
#: by arithmetic), and a TWO-element one can only say 0.5 or 1.0 — one comparison is not a
#: distribution. Three is the smallest population where "top band" means the candidate stood above
#: more than one rival. Below it, :meth:`RecommendationPipeline._quantile_band` returns the MIDDLE
#: band instead — which also covers the EMPTY population a journal-hydrated day can present.
MIN_RANK_POPULATION = 3

#: Hard ceiling on the pending-forward queue. The prescreen publication cap (settings
#: ``max_candidates_per_day``) already bounds it below this; the ceiling exists so a misconfigured
#: cap cannot turn a refused-candidate pointer list into unbounded process memory.
MAX_PENDING_FORWARDS = 100

#: Strategies whose proposal is a LIMIT pinned to a structural LEVEL rather than to the live price,
#: so a candidate that waits in the queue while the price walks away from that level is a guaranteed
#: ``entry_sanity_band`` gate reject that already spent an analyst slot. Only these strategies are
#: band-screened at the forward slot — a price-relative leg's entry tracks the LTP, so the same
#: screen there would drop candidates the gate would have approved.
BAND_SKIP_STRATEGIES = frozenset({"brk20"})

#: Forward-DRAIN trigger modes. ``paced`` = the queue is drained on a fixed cadence by
#: :meth:`RecommendationPipeline.drain_forward_queue`; ``immediate`` = an arriving candidate drains
#: its own slot inline, kept ONLY as the rollback.
FORWARD_DRAIN_MODES = ("paced", "immediate")

#: Minutes between paced drains — the window over which candidates ACCUMULATE before one of them is
#: chosen. Strictly above the 60 s scheduler pulse that drives it; small against
#: ``TTL_INTRADAY_MIN`` (20) so a queued candidate gets several chances before its levels go stale;
#: and drains span ``prescreen_cap_per_day`` × ``FORWARD_PACING_MIN`` minutes, compared against the
#: owner-set trade window (``trade_window_state``): whichever is shorter binds. A LONGER interval
#: buys a better-populated queue at the cost of latency to the analyst.
FORWARD_PACING_MIN = 3

#: Wall-clock ceiling on ONE gate-context build inside :meth:`RecommendationPipeline._gate_and_persist`
#: (seconds). The proposal row is already written when the build is awaited; a stalled market store
#: with no deadline would freeze the drain tick (no verdict, no alert, no later candidate). 90 s is
#: orders of magnitude above a healthy build and below the FORWARD_PACING_MIN cadence, so it can only
#: fire on a real stall.
_GATE_CONTEXT_DEADLINE_S = 90.0

#: A proposal still carrying no verdict this many minutes after it was written is ORPHANED.
#: Comfortably above ``_GATE_CONTEXT_DEADLINE_S`` so the timeout path's own alert always lands first
#: and the sweep is the BACKSTOP (a crash between the two writes, a stall nothing else caught) rather
#: than a duplicate of it.
_ORPHAN_TTL_MIN = 10

#: Oldest orphan that still pages the owner, minutes (24 h). A permanent orphan can never be
#: resolved (no verdict is ever invented), so without a cap it would re-announce on every boot and
#: day-roll forever. Past it: INFO log once per process-day, nightly-review counting unchanged, no
#: owner page.
_ORPHAN_ALERT_MAX_AGE_MIN = 24 * 60

#: How often the orphan sweep actually reads the DB, minutes. The 60 s forward-drain tick invokes it
#: on every pulse; this throttle decides which pulses do the work.
_ORPHAN_SWEEP_INTERVAL_MIN = 5

#: Strategies whose stop is re-anchored to the delivered entry at the rule's percentage.
REANCHOR_STRATEGIES = frozenset({hi52.STRATEGY_ID, ins.STRATEGY_ID})

#: Ceiling on collecting one card's extras; must stay <= ``_GATE_CONTEXT_DEADLINE_S``.
_EXTRAS_DEADLINE_S = 15.0

#: Calendar days of ``bars_1d`` read for the card's ATR(14, 1d).
_ATR_1D_LOOKBACK_DAYS = 45


class GateContextTimeout(Exception):
    """One gate-context build blew ``_GATE_CONTEXT_DEADLINE_S`` (see the constant).

    Carries the symbol AND the ``proposal_id`` because :meth:`RecommendationPipeline._gate_and_persist`
    has ALREADY written the proposal row by the time the build is awaited: the raise leaves a real
    orphan behind, deliberately. Fabricating a verdict to tidy it up would put a decision the gate
    never made into the audit trail (a verdict row is a claim that the gate judged this
    proposal); :meth:`RecommendationPipeline.sweep_orphaned_proposals` is what closes the loop.
    """

    def __init__(self, symbol: str, proposal_id: str) -> None:
        super().__init__(
            f"gate context for {symbol} did not build within {_GATE_CONTEXT_DEADLINE_S:.0f}s "
            f"(proposal {proposal_id} is persisted with no verdict)"
        )
        self.symbol = symbol
        self.proposal_id = proposal_id


@dataclass(frozen=True)
class _AtrState:
    """Carried ATR(14,1m) for one symbol — the state that replaces a per-bar store read.

    ``atr`` is a plain float on purpose: it is the exact ``float64`` :func:`wilder_atr` would have
    produced at ``last_ts`` over the same anchored window, and the recursion that advances it uses
    the identical expression, so the incremental and store-read values agree BIT FOR BIT (pinned by
    ``test_incremental_atr_equals_the_store_read_at_every_bar``). The previous bar's H/L/C are kept
    because the next bar's true range is defined against them.
    """

    day: date                   # the bar day this state belongs to — a new day always reseeds
    last_ts: datetime           # minute of the newest bar folded in
    last_high: float
    last_low: float
    last_close: float
    atr: float


@dataclass(frozen=True)
class _PendingForward:
    """One published-but-not-yet-evaluated candidate waiting for an analyst slot."""

    candidate: SignalCandidate
    fired_at: datetime          # when the PIPELINE received it (platform clock — never LLM)
    seq: int                    # arrival sequence: the last, always-unique deterministic tie-break
    expires_at: datetime        # the candidate's own TTL horizon; past it the levels are stale
    front: bool = False         # a re-queued failed evaluation, ahead of every fresh arrival


@dataclass
class RecExtras:
    """Card extras read from stores and reports; any part may be missing."""

    stop_atr_mult: Decimal | None = None
    evidence: list[str] = dataclass_field(default_factory=list)


#: ``owner_approvals.kind`` per action type (``owner_approval_required``).
_APPROVAL_KIND: Mapping[str, str] = {
    "enter": "entry",
    "exit": "exit",
    "modify-stop": "stop_widen",
    "modify-target": "target_extend",
    "cancel": "cancel",
}

#: ``learning_ledger`` columns this module writes itself — a caller-supplied value for one of them is
#: ignored rather than allowed to fight the book for ownership of the row's identity.
_LEDGER_RESERVED = frozenset({"entry_id", "rec_id", "is_paper", "created_at"})

_PAISA = Decimal("0.01")
_HUNDRED = Decimal(100)


# --------------------------------------------------------------------------- small helpers
def _dec(value: Any) -> Decimal:
    """Any scalar → exact Decimal; floats via ``str()`` so no binary-float artifact reaches a price."""
    if isinstance(value, Decimal):
        return value
    if value is None or value == "":
        return Decimal(0)
    return Decimal(str(value))


def _money(value: Decimal) -> Decimal:
    return value.quantize(_PAISA, rounding=ROUND_HALF_UP)


def _floor_div(numerator: Decimal, denominator: Decimal) -> int:
    """``floor(numerator / denominator)`` clamped at 0 — a non-positive denominator yields 0 (fail
    to zero, never "unbounded")."""
    if denominator <= 0 or numerator <= 0:
        return 0
    return int((numerator / denominator).to_integral_value(rounding=ROUND_FLOOR))


def _sql(value: Any) -> Any:
    """Coerce a Python value to something sqlite3 can bind (Decimal→str, bool→int, datetime→ISO)."""
    if value is None or isinstance(value, (int, float, str, bytes)):
        return int(value) if isinstance(value, bool) else value
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _json_payload(obj: Any) -> str:
    return json.dumps(obj, separators=(",", ":"), sort_keys=True, default=str)


def _product_of(style: str) -> str:
    """Intraday ⇒ MIS, swing/position ⇒ CNC. The one place the mapping is written."""
    return "MIS" if style == "intraday" else "CNC"


# =========================================================================== the book
class RecommendationBook:
    """Persistence + human-outcome capture for RECOMMEND.

    Implements the Telegram ``RecoBook`` seam: :meth:`take` / :meth:`close` / :meth:`veto` each return
    an owner-facing summary line and raise :class:`ValueError` — with a message written for the owner —
    on an unknown ``rec_id`` or an invalid state transition.

    **Where the money lands.** ``positions.realized_pnl`` is written GROSS and ``positions.costs``
    separately, because :class:`~engine.risk.exposure.ExposureTracker` computes platform equity as
    ``Σ(realized_pnl − costs)``; writing the already-net figure there would subtract costs twice from
    every risk-limit floor-ladder reading. The NET figure is not lost — ``learning_ledger`` carries
    ``gross_pnl`` and ``net_pnl`` explicitly and ``recommendations.outcome`` carries both.

    Confirmed recommended positions count toward every risk limit for free: ``ExposureTracker`` and
    ``GateContextBuilder`` already read ``origin IN ('platform','recommended')``, so writing the row
    is the whole integration.
    """

    def __init__(self, conn: sqlite3.Connection, clock: Clock, cost_model: CostModel, *,
                 overnight_gap_mult_fn: Callable[[], Any] | None = None,
                 calendar: NSECalendar | None = None, veto_window_sessions: int = 3) -> None:
        self._conn = conn
        self._clock = clock
        self._costs = cost_model
        self._calendar = calendar
        self._veto_window_sessions = veto_window_sessions
        self._ledger_cols: frozenset[str] | None = None
        #: The ``per_trade_risk.overnight_gap_mult`` the gate sized swing/position entries on,
        #: read at call time from the hash-verified limits. Unwired ⇒
        #: the ``/taken`` drift check falls back to :data:`TAKEN_STOP_RISK_WARN_MULT`.
        self._overnight_gap_mult_fn = overnight_gap_mult_fn

    @property
    def cost_model(self) -> CostModel:
        """The shared cost model — the pipeline prices exit/adjust recommendations through it."""
        return self._costs

    # ------------------------------------------------------------------ delivery
    def deliver(self, rec: Recommendation, *, ledger_fields: Mapping[str, Any]) -> None:
        """Persist a delivered recommendation + its OPEN learning-ledger row.

        The ledger row is opened here with every attribution field known at decision time and NULL
        outcome fields; it is closed by :meth:`close` (a real outcome), :meth:`veto` or
        :meth:`expire_stale` (both ``no_action``). Sending the message is the caller's job — a row
        that exists only after a successful Telegram send would lose the trade on a network blip.
        """
        now = self._clock.now()
        fields = {k: v for k, v in ledger_fields.items() if k not in _LEDGER_RESERVED}
        known = self._ledger_columns()
        unmapped = sorted(k for k in fields if k not in known)
        if unmapped:
            # No column ⇒ no silent drop: the value goes to the structured log so the audit trail
            # keeps it until a migration gives it a home (``catalyst_ref`` is the current case).
            _log.warning(
                "ledger_field_unmapped", rec_id=rec.rec_id, fields=unmapped,
                values={k: str(fields[k]) for k in unmapped},
            )
        writable = {k: v for k, v in fields.items() if k in known}
        columns = ["entry_id", "rec_id", "is_paper", "created_at", *writable]
        values = [str(ULID()), rec.rec_id, 0, now.isoformat(), *(_sql(v) for v in writable.values())]
        placeholders = ", ".join("?" for _ in columns)
        with transaction(self._conn):
            self._conn.execute(
                "INSERT INTO recommendations (rec_id, payload, delivered_at) VALUES (?, ?, ?)",
                (rec.rec_id, _json_payload(rec.model_dump(mode="json")), now.isoformat()),
            )
            self._conn.execute(
                f"INSERT INTO learning_ledger ({', '.join(columns)}) VALUES ({placeholders})",
                tuple(values),
            )
        _log.info(
            "recommendation_delivered", rec_id=rec.rec_id, kind=rec.kind, instrument=rec.instrument,
            side=rec.side, qty=rec.qty, verdict=rec.gate.verdict,
        )

    # ------------------------------------------------------------------ owner confirms a fill
    async def take(self, rec_id: str, qty: int, price: Decimal) -> str:
        """``/taken <rec_id> <qty> <price>`` — create the ``origin='recommended'`` position.

        The platform never adopts a position as ``recommended`` without this owner confirmation.

        Nothing here re-gates the fill, and ``take`` accepts an already-expired rec (the owner may
        confirm the morning after), so the reported price can be a gap away from the zone the
        risk-gate ``entry_sanity_band`` and ``per_trade_risk`` verdicts were computed on. The fill is
        recorded regardless — the owner is the authority on what they executed, and a live position
        the ledger does not know about is the worse failure — but a fill whose distance to the
        recommendation's stop exceeds the sized-for distance by :data:`TAKEN_STOP_RISK_WARN_MULT` is
        said out loud in the reply and at WARNING in the log, because the ``stop`` written on the
        position row below is the recommendation's, not one re-derived from this price.
        """
        if qty <= 0:
            raise ValueError(f"qty must be positive, got {qty}")
        price = _dec(price)
        if price <= 0:
            raise ValueError(f"price must be positive, got {price}")
        row = self._rec_row(rec_id)
        if row["human_action"] and row["human_action"] != "expired":
            raise ValueError(
                f"recommendation {rec_id} is already '{row['human_action']}' — /taken applies only "
                "to an open (or expired-unrecorded) recommendation"
            )
        # ``expired`` → ``taken`` is legal: the owner may record an intraday fill in the evening,
        # after the EOD sweep labelled the row expired. Expiry marks "no longer actionable", not
        # "never happened". dismissed/closed/taken stay refused.
        data = self._rec_payload(row)
        if (data.get("kind") or "entry") != "entry":
            # /taken on an exit/adjust rec would OPEN a new position on the closing side — a
            # phantom short after exiting a long. Exits are reported via /closed.
            raise ValueError(
                f"recommendation {rec_id} is kind='{data.get('kind')}' — /taken records ENTRY fills "
                "only; report an executed exit with /closed <entry_rec_id> <price>"
            )
        now = self._clock.now()
        position_id = str(ULID())
        targets = data.get("targets") or []
        with transaction(self._conn):
            self._conn.execute(
                "INSERT INTO positions (position_id, symbol, side, style, product, qty, avg_entry, "
                "stop, target, state, protection_state, is_paper, origin, opened_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'OPEN', NULL, 0, 'recommended', ?)",
                (
                    position_id, data.get("instrument"), data.get("side"), data.get("style"),
                    data.get("product"), int(qty), str(price),
                    _sql(data.get("stop")), _sql(targets[0] if targets else None), now.isoformat(),
                ),
            )
            self._conn.execute(
                "UPDATE recommendations SET human_action='taken', human_fill_price=?, "
                "skip_reason=NULL, skip_reason_at=NULL WHERE rec_id=?",
                (str(price), rec_id),
            )
            # outcome_label/closed_at reset to NULL: an expired→taken rec was already stamped
            # 'no_action' by expire_stale, and close()'s UPDATE matches `outcome_label IS NULL` only.
            self._conn.execute(
                "UPDATE learning_ledger SET entry_px=?, qty=?, position_id=?, "
                "outcome_label=NULL, closed_at=NULL WHERE rec_id=?",
                (str(price), int(qty), position_id, rec_id),
            )
        _log.warning("recommendation_taken", rec_id=rec_id, position_id=position_id, qty=qty,
                     price=str(price))
        summary = (
            f"recorded {data.get('side')} {data.get('instrument')} x{qty} @ {price} "
            f"(position {position_id}). The protective orders are yours to place — the platform "
            "places none in RECOMMEND."
        )
        if data.get("gtt_instruction"):
            summary += (
                f"\n{data['gtt_instruction']}\nreply /protected {data.get('instrument')} once placed"
            )
        drift = self._stop_risk_drift(data, price)
        if drift is None:
            return summary
        planned, approved, actual, gap, wrong_side = drift
        _log.warning(
            "recommendation_taken_off_zone", rec_id=rec_id, position_id=position_id,
            price=str(price), stop=str(data.get("stop")), qty=qty,
            planned_unit_risk=str(planned), approved_unit_risk=str(approved),
            actual_unit_risk=str(actual), overnight_gap_mult=None if gap is None else str(gap),
            stop_wrong_side=wrong_side, entry_zone=data.get("entry_zone"), style=data.get("style"),
        )
        if wrong_side:
            return summary + (
                f" CHECK THE STOP: {price} is on the WRONG SIDE of the recommended stop "
                f"{data.get('stop')} for a {data.get('side')} — that stop would trigger at once. The "
                "position is recorded with the recommendation's stop verbatim; re-place the "
                "protective order before anything else. Nothing re-gated at capture."
            )
        allowance = (
            f"{gap}x the {_money(planned)} zone-edge-to-stop distance, the overnight-gap allowance"
            if gap is not None else f"the {_money(planned)} zone-edge-to-stop distance"
        )
        return summary + (
            f" CHECK THE SIZE: {price} is {_money(actual)} from the recommended stop "
            f"{data.get('stop')}, against the {_money(approved)} per share the size was approved on "
            f"({allowance}) — about ₹{_money(Decimal(qty) * actual)} of stop risk on {qty} shares "
            f"versus ₹{_money(Decimal(qty) * approved)} approved. The zone, the stop and the size are "
            "the ones the gate stood behind when the recommendation was priced; nothing re-gated at "
            "capture."
        )

    # ------------------------------------------------------------------ owner reports the close
    async def close(self, rec_id: str, price: Decimal) -> str:
        """``/closed <rec_id> <price>`` — close the position and label the ledger outcome."""
        price = _dec(price)
        if price <= 0:
            raise ValueError(f"price must be positive, got {price}")
        # Prefer the ENTRY row (has entry_px) — /closed may arrive with an EXIT rec's id when the
        # owner replies to a delivered exit recommendation.
        ledger = self._conn.execute(
            "SELECT * FROM learning_ledger WHERE rec_id=? "
            "ORDER BY (entry_px IS NULL), created_at LIMIT 1", (rec_id,)
        ).fetchone()
        if ledger is None:
            raise ValueError(f"no learning-ledger row for recommendation {rec_id}")
        position = None
        if ledger["position_id"]:
            position = self._conn.execute(
                "SELECT * FROM positions WHERE position_id=? AND state='OPEN'",
                (ledger["position_id"],),
            ).fetchone()
        if position is None:
            raise ValueError(
                f"recommendation {rec_id} has no OPEN position — confirm the fill with /taken first"
            )

        now = self._clock.now()
        qty = int(ledger["qty"] or position["qty"] or 0)
        entry = _dec(ledger["entry_px"] or position["avg_entry"])
        side = str(position["side"] or "BUY").upper()
        product = str(position["product"] or _product_of(str(position["style"] or "swing")))
        direction = Decimal(1) if side == "BUY" else Decimal(-1)
        gross = _money((price - entry) * Decimal(qty) * direction)
        # Approximation: the round trip is priced on the EXIT notional rather than entry+exit legs
        # separately; second-order, and it keeps one cost number reconcilable with the gate's CostModel.
        costs = _money(self._costs.round_trip(_dec(qty) * price, product).total_cost)
        net = _money(gross - costs)
        outcome_label = "win" if net > 0 else ("loss" if net < 0 else "scratch")
        holding_minutes = self._holding_minutes(position["opened_at"], now)

        with transaction(self._conn):
            self._conn.execute(
                "UPDATE positions SET state='CLOSED', close_reason='manual_owner', closed_at=?, "
                # realized_pnl is GROSS on purpose — ExposureTracker computes equity as
                # Σ(realized_pnl − costs); writing net here would charge the costs twice.
                "realized_pnl=?, costs=? WHERE position_id=?",
                (now.isoformat(), str(gross), str(costs), position["position_id"]),
            )
            # Label EVERY still-open ledger row tied to this position (entry row + any exit/adjust
            # rec rows), so a close reported against an exit-rec id never orphans the entry row.
            self._conn.execute(
                "UPDATE learning_ledger SET exit_px=?, costs=?, gross_pnl=?, net_pnl=?, "
                "holding_minutes=?, close_reason='manual_owner', outcome_label=?, closed_at=? "
                "WHERE (entry_id=? OR position_id=?) AND outcome_label IS NULL",
                (
                    str(price), str(costs), str(gross), str(net), holding_minutes,
                    outcome_label, now.isoformat(), ledger["entry_id"], position["position_id"],
                ),
            )
            self._conn.execute(
                "UPDATE recommendations SET human_action='closed', outcome=? WHERE rec_id=?",
                (
                    _json_payload({"exit_price": str(price), "gross": str(gross), "net": str(net)}),
                    rec_id,
                ),
            )
        _log.warning("recommendation_closed", rec_id=rec_id, position_id=position["position_id"],
                     gross=str(gross), costs=str(costs), net=str(net), outcome=outcome_label)
        return (
            f"closed {position['symbol']} x{qty} @ {price}: gross ₹{gross}, costs ₹{costs}, "
            f"net ₹{net} ({outcome_label})"
        )

    # ------------------------------------------------------------------ owner declines
    async def veto(self, rec_id: str, reason: str | None = None) -> str:
        """``/veto <rec_id> [reason]`` — the owner declines. A live rec is recorded as ``dismissed`` + a
        ``no_action`` label: a decline and a silent non-fill are different facts, both training signal.
        An expired entry rec keeps ``expired`` and gains only the reason (its ledger row is
        already ``no_action``); a second reason is refused, never overwrites the first, and so is one
        given after the veto window the reminder runs in (:meth:`_in_veto_window`): by then the
        outcome may be visible, and a reason chosen in hindsight would taint the skip record."""
        row = self._rec_row(rec_id)
        now = self._clock.now()
        if self._expired(row, now) and reason is not None and self._kind(row) == "entry":
            if row["skip_reason"]:
                raise ValueError(f"reason already recorded: {row['skip_reason']}")
            data = self._rec_payload(row)
            if not self._in_veto_window(data, now):
                raise ValueError(
                    f"too late to record a reason (expired {str(data.get('valid_until'))[:10]})"
                )
            with transaction(self._conn):
                self._conn.execute(
                    "UPDATE recommendations SET skip_reason=?, skip_reason_at=? WHERE rec_id=?",
                    (reason, now.isoformat(), rec_id),
                )
            _log.warning("recommendation_vetoed", rec_id=rec_id, reason=reason, expired=True)
            return f"recommendation {rec_id} stays expired — reason '{reason}' recorded."
        if row["human_action"]:
            raise ValueError(
                f"recommendation {rec_id} is already '{row['human_action']}' — nothing to veto"
            )
        with transaction(self._conn):
            self._conn.execute(
                "UPDATE recommendations SET human_action='dismissed', skip_reason=?, skip_reason_at=? "
                "WHERE rec_id=?",
                (reason, None if reason is None else now.isoformat(), rec_id),
            )
            self._conn.execute(
                "UPDATE learning_ledger SET outcome_label='no_action', closed_at=? WHERE rec_id=?",
                (now.isoformat(), rec_id),
            )
        _log.warning("recommendation_vetoed", rec_id=rec_id, reason=reason)
        return f"recommendation {rec_id} dismissed — recorded as a no_action outcome."

    def decision_candidates(
        self, symbol: str | None, now: datetime, *, include_skipped: bool = False
    ) -> list[dict[str, Any]]:
        """Undecided ENTRY recs (optionally for one symbol), oldest first: live, or ``expired`` with no
        ``skip_reason`` and ``valid_until`` inside the last ``veto_window_sessions`` sessions.
        ``include_skipped`` also admits an expired rec that has a ``skip_reason`` (``take`` clears it).
        Without a calendar no expired rec qualifies."""
        rows = self._conn.execute(
            "SELECT rec_id, payload, delivered_at, human_action FROM recommendations "
            "WHERE delivered_at IS NOT NULL AND (human_action IS NULL OR (human_action='expired'"
            + ("" if include_skipped else " AND skip_reason IS NULL")
            + ")) ORDER BY delivered_at"
        ).fetchall()
        wanted = None if symbol is None else symbol.strip().upper()
        out: list[dict[str, Any]] = []
        for row in rows:
            data = self._rec_payload(row)
            instrument = str(data.get("instrument") or "")
            if self._kind(row) != "entry" or (wanted is not None and instrument.upper() != wanted):
                continue
            if self._expired(row, now) and not self._in_veto_window(data, now):
                continue
            out.append({
                "rec_id": str(row["rec_id"]), "instrument": instrument,
                "side": str(data.get("side") or "?"), "qty": int(data.get("qty") or 0),
                "kind": "entry", "delivered_at": row["delivered_at"],
                "valid_until": data.get("valid_until"), "strategy_id": data.get("strategy_id"),
            })
        return out

    def _in_veto_window(self, data: dict[str, Any], now: datetime) -> bool:
        if self._calendar is None:
            return False
        try:
            expiry = datetime.fromisoformat(str(data["valid_until"])).date()
        except (KeyError, ValueError):
            return False
        try:
            return now.date() <= self._calendar.add_sessions(expiry, self._veto_window_sessions)
        except ValueError:   # past the verified calendar horizon
            return now.date() <= expiry + timedelta(days=7)

    @classmethod
    def _kind(cls, row: sqlite3.Row) -> str:
        return str(cls._rec_payload(row).get("kind") or "entry")

    @classmethod
    def _expired(cls, row: sqlite3.Row, now: datetime) -> bool:
        """Swept to ``expired``, or past ``valid_until`` and not yet swept (the 15:45 sweep lags)."""
        return row["human_action"] == "expired" or (
            row["human_action"] is None
            and recommendation_expired(cls._rec_payload(row).get("valid_until"), now)
        )

    # ------------------------------------------------------------------ TTL sweep
    def expire_stale(self, now: datetime) -> list[str]:
        """Expire every unconfirmed recommendation past ``valid_until``; returns their rec_ids.

        The unbiased training signal: a non-fill is labelled ``no_action`` rather than dropped, so
        attribution is not computed only over the trades the owner happened to take. Idempotent — an
        already-actioned row is filtered out by ``human_action IS NULL``.
        """
        rows = self._conn.execute(
            "SELECT rec_id, payload FROM recommendations WHERE human_action IS NULL"
        ).fetchall()
        stale: list[str] = []
        for row in rows:
            try:
                data = json.loads(row["payload"] or "{}")
            except (TypeError, ValueError, json.JSONDecodeError):
                continue                        # unparseable payload: leave it alone, log-and-skip
            if recommendation_expired(data.get("valid_until"), now):
                stale.append(str(row["rec_id"]))
        if not stale:
            return []
        stamp = now.isoformat()
        with transaction(self._conn):
            for rec_id in stale:
                self._conn.execute(
                    "UPDATE recommendations SET human_action='expired' WHERE rec_id=?", (rec_id,)
                )
                self._conn.execute(
                    "UPDATE learning_ledger SET outcome_label='no_action', closed_at=? WHERE rec_id=?",
                    (stamp, rec_id),
                )
        _log.info("recommendations_expired", count=len(stale), rec_ids=stale)
        return stale

    def reminder_rows(self, now: datetime) -> list[dict[str, Any]]:
        """Expired decision candidates (see :meth:`decision_candidates`) not yet reminded."""
        pending = {
            r[0] for r in self._conn.execute(
                "SELECT rec_id FROM recommendations "
                "WHERE human_action='expired' AND reminder_sent_at IS NULL"
            )
        }
        return [c for c in self.decision_candidates(None, now) if c["rec_id"] in pending]

    def mark_reminded(self, rec_ids: list[str], now: datetime) -> None:
        with transaction(self._conn):
            self._conn.executemany(
                "UPDATE recommendations SET reminder_sent_at=? WHERE rec_id=?",
                [(now.isoformat(), rec_id) for rec_id in rec_ids],
            )

    # ------------------------------------------------------------------ owner confirms the stop order
    async def protect(self, symbol_or_rec: str) -> str:
        """``/protected <symbol|rec_id>`` — stamp ``owner_protected_at`` on the OPEN recommended
        position, found through its learning-ledger rec link."""
        arg = symbol_or_rec.strip()
        rows = self._conn.execute(
            "SELECT DISTINCT p.position_id, p.symbol, p.owner_protected_at FROM positions p "
            "JOIN learning_ledger l ON l.position_id = p.position_id "
            "WHERE p.state='OPEN' AND p.origin='recommended' AND (p.symbol=? OR l.rec_id=?)",
            (arg.upper(), arg),
        ).fetchall()
        if not rows:
            raise ValueError(f"no open recommended position for {arg}")
        if len(rows) > 1:
            raise ValueError(f"{arg} matches {len(rows)} open positions — send the rec_id instead")
        row = rows[0]
        if row["owner_protected_at"]:
            return f"{row['symbol']} already confirmed protected at {row['owner_protected_at']}."
        now = self._clock.now()
        with transaction(self._conn):
            self._conn.execute(
                "UPDATE positions SET owner_protected_at=? WHERE position_id=?",
                (now.isoformat(), row["position_id"]),
            )
        _log.warning("position_protected", position_id=row["position_id"], symbol=row["symbol"])
        return f"{row['symbol']} marked protected (owner-confirmed) at {now:%Y-%m-%d %H:%M}."

    # ------------------------------------------------------------------ internals
    def _ledger_columns(self) -> frozenset[str]:
        if self._ledger_cols is None:
            rows = self._conn.execute("PRAGMA table_info(learning_ledger)").fetchall()
            self._ledger_cols = frozenset(str(r[1]) for r in rows)
        return self._ledger_cols

    def _rec_row(self, rec_id: str) -> sqlite3.Row:
        row = self._conn.execute(
            "SELECT rec_id, payload, human_action, skip_reason FROM recommendations WHERE rec_id=?",
            (rec_id,)
        ).fetchone()
        if row is None:
            raise ValueError(f"unknown recommendation {rec_id}")
        return row

    @staticmethod
    def _rec_payload(row: sqlite3.Row) -> dict[str, Any]:
        try:
            data = json.loads(row["payload"] or "{}")
        except (TypeError, ValueError, json.JSONDecodeError):
            return {}
        return data if isinstance(data, dict) else {}

    @staticmethod
    def _holding_minutes(opened_at: Any, now: datetime) -> int | None:
        try:
            opened = datetime.fromisoformat(str(opened_at))
        except (TypeError, ValueError):
            return None
        if opened.tzinfo is None:
            return None
        return int((now - opened).total_seconds() // 60)

    def _overnight_gap_mult(self) -> Decimal | None:
        """The gate's ``overnight_gap_mult`` through the seam, or None (unwired / unreadable / ≤ 0).
        This drives a message, never a decision, so a failed read degrades to the fallback."""
        if self._overnight_gap_mult_fn is None:
            return None
        try:
            gap = _dec(self._overnight_gap_mult_fn())
        except Exception:  # noqa: BLE001 - a limits read failing here must not fail the fill capture
            _log.exception("taken_gap_mult_unreadable")
            return None
        return gap if gap > 0 else None

    def _stop_risk_drift(
        self, data: Mapping[str, Any], price: Decimal,
    ) -> tuple[Decimal, Decimal, Decimal, Decimal | None, bool] | None:
        """``(planned, approved, actual, gap, wrong_side)`` when a ``/taken`` fill drifted, else None.

        ``planned`` is the widest stop distance the delivered ``entry_zone`` could have produced (the
        zone edge FURTHEST from the stop), so a fill anywhere inside the band the gate stood behind
        is never called out; ``approved`` is the per-share stop distance the ``per_trade_risk``
        verdict budgeted: for a swing/position ``overnight_gap_mult`` × planned
        (``_rule_per_trade_risk`` sizes ``qty = budget / (gap × unit_risk)``), returned as ``gap``;
        for an intraday fill, or a book wired without the seam, ``planned`` itself and ``gap`` None.
        ``actual`` is the distance at the reported fill.

        Fires in two cases. **Wrong side** (``wrong_side=True``): a BUY filled at or below the stop, a
        SELL at or above it — the gap went THROUGH the stop, the distance is meaningless and the check
        is unconditional. **Drift**: ``actual`` exceeds ``approved`` when the gap is known, or
        :data:`TAKEN_STOP_RISK_WARN_MULT` × planned on the fallback. None whenever the payload cannot
        be read: this drives a message, never a decision, so an unreadable payload stays silent.
        """
        zone = data.get("entry_zone")
        if not isinstance(zone, (list, tuple)) or len(zone) != 2 or data.get("stop") in (None, ""):
            return None
        try:
            stop = _dec(data.get("stop"))
            planned = max(abs(_dec(zone[0]) - stop), abs(_dec(zone[1]) - stop))
            actual = abs(price - stop)
        except (TypeError, ValueError, ArithmeticError):
            return None
        if planned <= 0:
            return None
        side = str(data.get("side") or "").upper()
        if (side == "BUY" and price <= stop) or (side == "SELL" and price >= stop):
            return planned, planned, actual, None, True
        gap = self._overnight_gap_mult() if str(data.get("style") or "intraday") != "intraday" else None
        if gap is not None:
            approved, threshold = planned * gap, planned * gap
        else:
            approved, threshold = planned, planned * TAKEN_STOP_RISK_WARN_MULT
        if actual <= threshold:
            return None
        return planned, approved, actual, gap, False


# =========================================================================== the pipeline
class RecommendationPipeline:
    """Turns deterministic triggers into gate-approved recommendations.

    Every constructor argument is a seam that already exists elsewhere in the tree; this class adds
    ORDER and GATING, not new policy. The gating order in :meth:`on_signal_candidate` is deliberate:
    the cheap deterministic checks (mode, risk state, kill, window) run before the governor, and the
    governor runs before anything that costs a token.
    """

    def __init__(
        self,
        assembler: Any,
        harness: Any,
        agent_defs: Mapping[str, Any],
        gate: Any,
        ctx_builder: Any,
        book: RecommendationBook,
        mode_manager: Any,
        kill_switch: Any,
        governor: Any,
        exposure: Any,
        limits_engine: Any,
        notify: NotifyFn | None,
        clock: Clock,
        calendar: NSECalendar,
        conn: sqlite3.Connection,
        store: Any,
        rearm: Callable[[str, str], bool] | None = None,
        funnel_raw: Callable[[date], Mapping[str, int]] | None = None,
        claim_slot: Callable[[str, str], bool] | None = None,
        take_displaced: Callable[[], Sequence[tuple[str, str]]] | None = None,
        decline: Callable[[str, str], bool] | None = None,
        ltp_fn: Callable[[str], Decimal | None] | None = None,
        warmup_status_fn: Callable[[], Any] | None = None,
        admission_mode: str = "ranked",
        forward_drain_mode: str = "paced",
        hold_fn: HoldFn | None = None,
        round_tick_fn: Callable[[str, Decimal], Decimal] | None = None,
        recommend: RecommendSettings | None = None,
        strategy_edge_pct: Mapping[str, Decimal] | None = None,
        reports_dir: Path | None = None,
        paper_ctx_builder: Any = None,
        paper_submit: PaperSubmitFn | None = None,
        paper_enabled_fn: Callable[[], bool] | None = None,
    ) -> None:
        if paper_enabled_fn is not None and (paper_ctx_builder is None or paper_submit is None):
            raise ValueError("paper_enabled_fn needs paper_ctx_builder and paper_submit")
        if admission_mode not in FORWARD_MODES:
            raise ValueError(f"admission_mode must be one of {FORWARD_MODES}, got {admission_mode!r}")
        if forward_drain_mode not in FORWARD_DRAIN_MODES:
            raise ValueError(
                f"forward_drain_mode must be one of {FORWARD_DRAIN_MODES}, got {forward_drain_mode!r}"
            )
        self._assembler = assembler
        self._harness = harness
        self._agent_defs = agent_defs
        self._gate = gate
        self._ctx_builder = ctx_builder
        self._book = book
        self._mode = mode_manager
        self._kill = kill_switch
        self._governor = governor
        self._exposure = exposure
        self._limits = limits_engine
        self._notify = notify
        self._clock = clock
        self._calendar = calendar
        self._conn = conn
        self._store = store
        #: (symbol, strategy_id) -> re-arm the prescreen's day dedupe after an analyst
        #: INFRASTRUCTURE failure; wired to SignalPreScreen.rearm.
        self._rearm = rearm
        #: ``day -> {strategy_id: raw fires}`` (wired to ``SignalPreScreen.raw_counts``) plus the
        #: last set actually written to ``funnel_raw_counts`` and the day it belongs to; the
        #: "changed since the last flush" test keeps the 60 s drain tick from rewriting an unchanged
        #: row set. See :meth:`_flush_funnel_raw`.
        self._funnel_raw = funnel_raw
        #: Prescreen cap-displacement seam, both wired to ``SignalPreScreen``:
        #: ``claim_slot`` is the atomic compare-and-set this pipeline MUST pass before spending an
        #: analyst call — ``False`` means a better candidate took this pair's admission slot while it
        #: waited in the queue, so forwarding it anyway would breach the cap by one real call.
        #: ``take_displaced`` pulls the evictions the pre-screen decided on a scan worker thread, so
        #: the queue surgery and the journal correction happen HERE, on the event loop, where
        #: ``_pending_forwards`` may actually be touched. Unwired ⇒ every claim succeeds and no
        #: eviction ever arrives.
        self._claim_slot = claim_slot
        self._take_displaced = take_displaced
        #: (symbol, strategy_id) -> the analyst RAN and said no_action, so the pair is displaceable
        #: again (wired to SignalPreScreen.decline). Not a re-arm: the slot stays spent.
        self._decline = decline
        #: symbol -> live LTP (wired to the tick cache in ``engine.ops.main``). Read by the entry
        #: band screen in :meth:`_take_forward_slot` and as the MARKET anchor in
        #: :meth:`_reanchored_stop`; unwired ⇒ neither. Never a gate input: the gate reads its own
        #: LTP from GateContext.
        self._ltp_fn = ltp_fn
        #: (strategy_id, style) -> hold sessions (``engine.ops.holds``); unwired ⇒ no exit date.
        self._hold_fn = hold_fn
        self._round_tick = round_tick_fn or (lambda _symbol, price: round_to_tick(price))
        self._recommend = recommend or RecommendSettings()
        #: The registered-edge map the RiskGate is built with; shown only on targetless recs.
        # By reference, never copied: a live `StrategyEdges` re-derives hi52's cap when the limits move.
        self._strategy_edge: Mapping[str, Decimal] = (
            strategy_edge_pct if strategy_edge_pct is not None else {}
        )
        #: Backtest reports the evidence line reads; unwired ⇒ no backtest line.
        self._reports_dir = reports_dir
        #: Paper seams; ``paper_enabled_fn`` None ⇒ paper is off and no paper verdict is written.
        self._paper_ctx_builder = paper_ctx_builder
        self._paper_submit = paper_submit
        self._paper_enabled_fn = paper_enabled_fn
        #: The warm-up snapshot the GATE reads (wired to the same ``warmup_status_snapshot`` in
        #: ``engine.ops.main``), consulted per COVERAGE CLASS at both analyst-call commit points
        #: (:meth:`on_signal_candidate` and :meth:`_take_forward_slot`), to keep an intraday
        #: candidate the gate would refuse for intraday coverage from spending a call; unwired ⇒ no
        #: screen. Never a gate input: the gate reads the snapshot itself.
        self._warmup_status_fn = warmup_status_fn
        #: (symbol, strategy_id, commit point) keys whose intraday warm-up refusal is already logged
        #: today, plus the day that set belongs to — one line per key per day.
        self._intraday_warmup_logged: set[tuple[str, str, str]] = set()
        self._intraday_warmup_logged_day: date | None = None
        self._funnel_raw_day: date | None = None
        self._funnel_raw_flushed: dict[str, int] = {}
        #: position_id -> when its last trigger-b event fired (in-process debounce).
        self._last_position_event: dict[str, datetime] = {}
        #: position_id -> the trading date its "sold outside the ledger" skip was last logged
        #: on. The SKIP itself is re-decided from the journal on every event; only the log line is
        #: deduped. In-process like every other memo here — a restart logs it once more.
        self._sold_outside_logged: dict[str, date] = {}
        #: The same dedupe for the time-exit skip; a separate memo, because the position-event path
        #: logging a position must not silence the time exit's line (or the reverse).
        self._time_exit_skip_logged: dict[str, date] = {}
        #: Owner-alert cadence for "Intraday analyst unavailable", per ``(agent, reason)``. The
        #: no-action behaviour is unchanged (a Tier-1 failure is still no-proposal, still logged every
        #: time); only how often the owner is told changes. A successful analyst call closes the
        #: episode, so the next outage is heard immediately.
        self._agent_alert_episodes = AlertEpisodes()
        #: Analyst forward cap — per-day count of candidates that reached the harness.
        #: Hydrated from the day-slot journal on every day roll, so a mid-day restart
        #: RESUMES the day's analyst quota instead of refilling it.
        self._forwarded_day: date | None = None
        self._forwarded_count = 0
        self._forward_mode = admission_mode
        #: The priority queue: published candidates that have not been evaluated yet.
        self._pending_forwards: list[_PendingForward] = []
        self._forward_seq = 0
        #: When the queue was last drained. ``None`` ⇒ the first tick drains; from then on the
        #: anchor advances every tick that passes the guard, dispatched or not — the interval IS
        #: the accumulation window, so an empty one still spends its place in the cadence.
        self._drain_mode = forward_drain_mode
        self._last_forward_drain: datetime | None = None
        #: signal_ids whose evaluation blew up once and were re-queued for it. Bounds the
        #: retry to ONE per candidate per day; rolled with the rest of the forward state in
        #: :meth:`_roll_forward_day`, so it can never outlive the queue it refers to.
        self._requeued_forwards: set[str] = set()
        #: signal_ids whose forward charge has already been handed back, so ONE charge can never be
        #: undone twice (:meth:`_refund_forward`). Rolled with the day like ``_requeued_forwards``,
        #: and DISCARDED the moment :meth:`_take_forward_slot` charges the same candidate again — the
        #: memo is "this charge is already refunded", so a re-drained front entry can still be
        #: refunded for the second charge it makes.
        self._refunded_forwards: set[str] = set()
        #: Which window CLOSE has already been flushed, keyed on the window's END rather than on the
        #: day: ``NSECalendar.trade_window`` re-reads the owner's sticky ``trade_window_state`` row on
        #: every call, so a window the owner moves or re-opens after a close is a NEW close that has
        #: to flush again. ``_window_close_flushed_day`` is the sibling latch for a day with NO window
        #: at all (no end timestamp to key on). Both are stamped rather than bools because the flush
        #: fires on the path that RETURNS BEFORE :meth:`_roll_forward_day`.
        self._window_close_flushed_at: datetime | None = None
        self._window_close_flushed_day: date | None = None
        #: proposal_ids the orphan sweep has already alerted on, plus the day that set
        #: belongs to and when the sweep last read the DB. Rolled daily by :meth:`_roll_orphan_day`
        #: exactly like ``_requeued_forwards`` — a per-PROCESS memo, so a restart re-alerts a
        #: standing orphan once (silence is what the orphan sweep exists to end).
        self._orphans_alerted: set[str] = set()
        self._orphans_alerted_day: date | None = None
        self._last_orphan_sweep: datetime | None = None
        #: strategy_id -> today's observed scores, the population the forward quantile ranks in.
        self._day_scores: dict[str, list[float]] = {}
        #: Hot-path caches, all rolled together by :meth:`_roll_hot_path_day`.
        #: symbol -> carried ATR(14,1m); bounded by the open-recommended-position count, since
        #: :meth:`on_bar` returns before the ATR for any symbol without one.
        self._atr_state: dict[str, _AtrState] = {}
        self._sector_cache: dict[str, str] | None = None
        self._sector_cache_day: date | None = None
        self._hot_path_day: date | None = None
        self._hot_counts: dict[str, int] = dict.fromkeys(_HOT_PATH_COUNTERS, 0)
        self._hot_ms: dict[str, float] = dict.fromkeys(_HOT_PATH_TIMERS, 0.0)

    def _rearm_slot(self, candidate: SignalCandidate) -> None:
        """Hand the (symbol, strategy) day slot back after a never-evaluated drop."""
        self._rearm_slot_pair(candidate.symbol, candidate.strategy_id)

    def _rearm_slot_pair(self, symbol: str, strategy_id: str) -> None:
        """:meth:`_rearm_slot` by PAIR — the displacement path knows which pair lost its slot but
        may not still hold the candidate object. The ``rearm`` call is a harmless no-op there (the
        pre-screen already dropped the pair from its dedupe set)."""
        try:
            self._conn.execute(
                "UPDATE prescreen_day_slots SET evaluated=0 WHERE d=? AND symbol=? AND strategy_id=?",
                (self._clock.today().isoformat(), symbol, strategy_id),
            )
        except Exception as exc:  # noqa: BLE001 - journaling is resilience bookkeeping, never a gate
            _log.warning("day_slot_journal_failed", op="rearm", symbol=symbol,
                         strategy_id=strategy_id, error=str(exc))
        if self._rearm is not None:
            self._rearm(symbol, strategy_id)

    def _journal_slot(self, candidate: SignalCandidate, d: date) -> None:
        """Day-slot journal: a received publication spends the (symbol, strategy) slot for day ``d``
        — the conservative default for EVERY handler path; :meth:`_rearm_slot` flips it back for the
        never-evaluated drops. Boot rehydration (``engine.ops.main``) rebuilds the prescreen's
        in-memory dedupe/caps from these rows, so a restart does not reset the
        ``max_candidates_per_day`` bound. A journal failure degrades to in-memory-only, never blocks
        the candidate.

        Stamps ``unsizeable`` from ``candidate.raw_levels.stop``: this write runs BEFORE
        :meth:`on_signal_candidate`'s stopless short-circuit, and a structurally-unforwardable
        candidate (no stop ⇒ max_qty_by_risk is always 0) must not journal as one genuinely refused a
        forward slot — the funnel's ``best_unforwarded_score`` (the starvation reading) would read it
        as the day's best starved idea. The marker is a fact about the candidate, so it is set
        unconditionally here.
        """
        unsizeable = candidate.raw_levels.stop is None
        try:
            self._conn.execute(
                "INSERT INTO prescreen_day_slots "
                "(d, symbol, strategy_id, published_at, evaluated, score, unsizeable) "
                "VALUES (?, ?, ?, ?, 1, ?, ?) "
                "ON CONFLICT(d, symbol, strategy_id) "
                "DO UPDATE SET evaluated=1, published_at=excluded.published_at, "
                "score=excluded.score, unsizeable=excluded.unsizeable",
                (d.isoformat(), candidate.symbol, candidate.strategy_id,
                 self._clock.now().isoformat(), float(candidate.score), int(unsizeable)),
            )
        except Exception as exc:  # noqa: BLE001 - journaling is resilience bookkeeping, never a gate
            _log.warning("day_slot_journal_failed", op="publish", symbol=candidate.symbol,
                         strategy_id=candidate.strategy_id, error=str(exc))

    def _journal_forward(self, candidate: SignalCandidate, d: date) -> None:
        """Charge one analyst forward to ``candidate``'s day slot.

        A COUNTER, not a flag: a pair re-armed after an analyst INFRASTRUCTURE failure can
        legitimately be forwarded again, and every attempt that produced a VERDICT is real spend
        against the forward cap. An attempt that produced none is undone one unit at a time by
        :meth:`_refund_forward` — hence a counter and an arithmetic decrement. Upserts rather than
        updates so a lost publish-journal row cannot swallow the forward record. Journal failure
        degrades to in-memory-only counting, never blocks the call.
        """
        try:
            self._conn.execute(
                "INSERT INTO prescreen_day_slots "
                "(d, symbol, strategy_id, published_at, evaluated, score, forwarded) "
                "VALUES (?, ?, ?, ?, 1, ?, 1) "
                "ON CONFLICT(d, symbol, strategy_id) "
                "DO UPDATE SET forwarded = prescreen_day_slots.forwarded + 1",
                (d.isoformat(), candidate.symbol, candidate.strategy_id,
                 self._clock.now().isoformat(), float(candidate.score)),
            )
        except Exception as exc:  # noqa: BLE001 - journaling is resilience bookkeeping, never a gate
            _log.warning("day_slot_journal_failed", op="forward", symbol=candidate.symbol,
                         strategy_id=candidate.strategy_id, error=str(exc))

    def _journal_decision(
        self, candidate: SignalCandidate, d: date, decision: str, confidence: float | None,
        call_id: str | None, proposal_id: str | None,
    ) -> None:
        """Record the analyst's FIRST successful verdict on the day slot.

        Write-once: a failed-evaluation re-queue or a re-arm evaluates the same row again, and the
        later answer must not replace the one the verdict study counts. Journal failure never blocks
        the call.
        """
        key = (d.isoformat(), candidate.symbol, candidate.strategy_id)
        try:
            cur = self._conn.execute(
                "UPDATE prescreen_day_slots SET analyst_decision = ?, analyst_confidence = ?, "
                "analyst_call_id = ?, proposal_id = ? "
                "WHERE d = ? AND symbol = ? AND strategy_id = ? AND analyst_decision IS NULL",
                (decision, confidence, call_id, proposal_id, *key),
            )
            if cur.rowcount == 0 and self._conn.execute(
                "SELECT 1 FROM prescreen_day_slots WHERE d = ? AND symbol = ? AND strategy_id = ?", key,
            ).fetchone() is None:
                _log.warning("day_slot_journal_failed", op="decision", reason="no_row",
                             symbol=candidate.symbol, strategy_id=candidate.strategy_id)
        except Exception as exc:  # noqa: BLE001 - journaling is resilience bookkeeping, never a gate
            _log.warning("day_slot_journal_failed", op="decision", symbol=candidate.symbol,
                         strategy_id=candidate.strategy_id, error=str(exc))

    def _journal_unsizeable_qty_zero(self, candidate: SignalCandidate, d: date) -> None:
        """Flip the day slot's ``unsizeable`` marker after :meth:`_max_qty_by_risk` computes 0 for a
        candidate that DOES carry a stop.

        :meth:`_journal_slot` stamped ``unsizeable`` from ``stop is None`` alone; this covers the
        sibling case where the per-trade-risk budget cannot afford even one share at this stop's
        absolute distance. Same funnel category (same column, same value): the funnel only answers
        "was this candidate ever eligible for a slot". A plain UPDATE, because the row is already
        there. Journal failure degrades to in-memory-only, never blocks the candidate.
        """
        try:
            self._conn.execute(
                "UPDATE prescreen_day_slots SET unsizeable=1 WHERE d=? AND symbol=? AND strategy_id=?",
                (d.isoformat(), candidate.symbol, candidate.strategy_id),
            )
        except Exception as exc:  # noqa: BLE001 - journaling is resilience bookkeeping, never a gate
            _log.warning("day_slot_journal_failed", op="unsizeable_qty_zero", symbol=candidate.symbol,
                         strategy_id=candidate.strategy_id, error=str(exc))

    # ------------------------------------------------------------------ forward queue
    def _roll_forward_day(self, d: date, *, in_flight: tuple[str, str] | None = None) -> None:
        """Roll the forward-cap day, hydrating the counter and the score population from the
        journal, then hand back every ORPHANED admission slot the journal still holds. Called on
        every candidate AND on every :meth:`drain_forward_queue` tick — the second caller is what
        makes the sweep reach a restart on a day where no candidate ever publishes (the pre-screen's
        boot rehydration keeps the orphaned pairs deduped, so they cannot publish themselves). The
        DB read happens once per day change, and therefore exactly once after a restart.

        THE ORPHAN RE-ARM. The queue is process memory: a restart drops it while the journal rows
        keep ``evaluated=1, forwarded=0``, so the pairs would stay deduped for the rest of the day
        with nothing ever looking at them. Those rows are the same never-evaluated fact
        :meth:`_expire_forwards` refunds — published, no analyst call, no ``agent_calls`` row, no
        verdict — so they re-arm.

        Safe because of WHERE it runs: at a day change the in-memory queue is empty by construction
        (fresh process, or the ``_pending_forwards.clear()`` below), so no row this sweep re-arms can
        still be held by a live queue pointer. The ONE exception is ``in_flight``: the caller in
        :meth:`on_signal_candidate` journals the arriving candidate BEFORE it rolls the day, so that
        pair's row is already ``evaluated=1, forwarded=0`` and would otherwise be re-armed out from
        under the candidate currently being admitted.

        ``unsizeable=1`` rows are excluded: nothing is waiting for them today (no stop, or no
        affordable quantity at this stop), so re-arming would only re-publish a guaranteed drop.
        """
        if self._forwarded_day == d:
            return
        self._forwarded_day = d
        self._pending_forwards.clear()
        self._forward_seq = 0
        self._requeued_forwards.clear()          # the retry budget is per DAY, like the queue
        self._refunded_forwards.clear()          # so is the refunded-charge memo
        self._window_close_flushed_at = None     # one flush per window CLOSE, never per
        self._window_close_flushed_day = None    #         process — both latches roll with the day
        self._forwarded_count, self._day_scores = self._hydrate_forward_state(d)
        self._rearm_forward_orphans(d, in_flight)

    def _rearm_forward_orphans(self, d: date, in_flight: tuple[str, str] | None) -> None:
        """Re-arm every ``evaluated=1, forwarded=0, unsizeable=0`` pair for ``d``.

        THE PREDICATE IS WIDER THAN THE INTENT, KNOWINGLY. Those three columns cannot tell
        "nobody ever looked at it" from "refused by budget policy": a candidate dropped at arrival by
        a GOVERNOR BLOCK leaves the same row, and :meth:`on_signal_candidate` deliberately does not
        re-arm that one. This sweep re-arms it anyway, accepted as BOUNDED: it runs once per day
        change, so a governor-blocked pair re-publishes at most once per restart, and by then the
        block has usually lifted. Separating the two states needs a "refused by policy" marker the
        journal schema does not have.

        Never raises: an unreadable journal costs the re-arm, never the trigger path.
        """
        try:
            rows = self._conn.execute(
                "SELECT symbol, strategy_id FROM prescreen_day_slots "
                "WHERE d=? AND evaluated=1 AND forwarded=0 AND unsizeable=0",
                (d.isoformat(),),
            ).fetchall()
        except Exception as exc:  # noqa: BLE001 - a journal read never blocks the trigger path
            _log.warning("day_slot_journal_failed", op="forward_orphans", d=d.isoformat(),
                         error=str(exc))
            return
        pairs = [(str(r["symbol"]), str(r["strategy_id"])) for r in rows]
        pairs = [p for p in pairs if p != in_flight]
        if not pairs:
            return
        for symbol, strategy_id in pairs:
            self._rearm_slot_pair(symbol, strategy_id)
        _log.info("forward_queue_orphans_rearmed", d=d.isoformat(), count=len(pairs),
                  pairs=[f"{sym}/{sid}" for sym, sid in pairs])

    def _hydrate_forward_state(self, d: date) -> tuple[int, dict[str, list[float]]]:
        """(forwarded-so-far, per-strategy score population) for ``d``, read from the journal."""
        count = 0
        scores: dict[str, list[float]] = {}
        try:
            rows = self._conn.execute(
                "SELECT strategy_id, score, forwarded FROM prescreen_day_slots WHERE d=?",
                (d.isoformat(),),
            ).fetchall()
        except Exception as exc:  # noqa: BLE001 - a journal read never blocks the trigger path
            _log.warning("day_slot_journal_failed", op="hydrate_forward", d=d.isoformat(),
                         error=str(exc))
            return 0, {}
        for row in rows:
            count += int(row["forwarded"] or 0)
            if row["score"] is not None:
                scores.setdefault(str(row["strategy_id"]), []).append(float(row["score"]))
        if count:
            _log.info("forward_count_hydrated", d=d.isoformat(), forwarded=count,
                      strategies=sorted(scores))
        return count, scores

    def _quantile_band(self, candidate: SignalCandidate) -> int:
        """``candidate``'s standing WITHIN ITS OWN STRATEGY's day, as a quintile band (0..4).

        ``score`` semantics are per-scanner: orb's 0.9 and rsi2's 0.9 are not the same statement,
        so raw scores must never be compared across strategies. The empirical CDF of that
        strategy's own day is comparable — it says "top of what this strategy produced today",
        which means the same thing for every strategy.

        AN UNMEASURED POPULATION IS NOT A TOP ONE. The population is never EMPTY at ranking time:
        :meth:`on_signal_candidate` appends the arriving score to ``_day_scores`` before it enqueues,
        so a strategy's first candidate of the day ranks against ``[own score]`` — ``quantile = 1.0``,
        the candidate measured against itself. :data:`MIN_RANK_POPULATION` is the floor that closes
        it, checked against the population the caller presents (own score included). Below the floor
        the honest standing is the MIDDLE band: a first arrival still competes on ``fired_at``, it
        just no longer wins by arithmetic.
        """
        population = self._day_scores.get(candidate.strategy_id) or []
        if len(population) < MIN_RANK_POPULATION:
            return QUANTILE_BANDS // 2       # nothing to rank against ⇒ the middle, not the top
        score = float(candidate.score)
        quantile = sum(1 for s in population if s <= score) / len(population)
        return min(QUANTILE_BANDS - 1, int(quantile * QUANTILE_BANDS))

    def _forward_key(self, entry: _PendingForward) -> tuple[int, int, datetime, int]:
        """THE FORWARD SELECTION RULE.

        At each analyst slot, forward the highest-priority published-but-unevaluated candidate:

          0. FRONT entries first — a candidate whose evaluation blew up mid-call is owed the
             very next slot, not a place in the ranking it already won once;
          1. per-strategy score QUANTILE BAND, descending — rank within that strategy's own day,
             never the raw score (scores are comparable inside a strategy, not across them);
          2. inside one band, ``fired_at`` ASCENDING — comparable standing means the older setup
             goes first, because it is the one closest to going stale;
          3. arrival sequence, ascending — the final tie-break, so the order is total and the same
             on every replay (two candidates can share a timestamp; they cannot share a seq).

        ``min()`` over this key is the queue pop; the negated band makes "higher band" sort first.
        The same key run through ``max()`` picks the overflow eviction, so a front entry is also the
        LAST thing a full queue drops — deliberate: it is the only entry we know is mid-retry.
        """
        return (0 if entry.front else 1, -self._quantile_band(entry.candidate),
                entry.fired_at, entry.seq)

    def _enqueue_forward(self, candidate: SignalCandidate, *, front: bool = False) -> None:
        """Put a candidate into the pending-forward queue, replacing any earlier entry for the same
        (symbol, strategy): a re-published pair is ONE waiting candidate at its latest levels, not
        two competing copies of itself.

        ``front=True`` is the re-queue primitive: the entry sorts ahead of every fresh arrival
        in BOTH selection modes, so a failed evaluation is retried at the next slot rather than
        re-entering the competition. It re-stamps ``fired_at`` and the TTL exactly as a fresh
        publication would — the levels are the ones the failed call was assembled from, and one
        pacing interval of extra life is what a retry costs.

        An overflow eviction re-arms the dropped candidate's prescreen admission slot on the rule
        :meth:`_expire_forwards` documents: an entry that leaves this queue without passing through
        :meth:`_take_forward_slot` was never evaluated, so its slot is not spent. That includes a
        ``front`` entry: the re-queue REFUNDS the forward charge its blown-up attempt made, so it
        carries no charge either.
        """
        now = self._clock.now()
        self._forward_seq += 1
        key = (candidate.symbol, candidate.strategy_id)
        self._pending_forwards = [
            p for p in self._pending_forwards
            if (p.candidate.symbol, p.candidate.strategy_id) != key
        ]
        self._pending_forwards.append(_PendingForward(
            candidate=candidate, fired_at=now, seq=self._forward_seq,
            expires_at=self._ttl(candidate.style), front=front,
        ))
        if len(self._pending_forwards) > MAX_PENDING_FORWARDS:
            worst = max(self._pending_forwards, key=self._forward_key)
            self._pending_forwards.remove(worst)
            _log.warning("forward_queue_overflow", dropped=worst.candidate.signal_id,
                         symbol=worst.candidate.symbol,
                         strategy_id=worst.candidate.strategy_id, limit=MAX_PENDING_FORWARDS,
                         front=worst.front)
            self._rearm_slot(worst.candidate)

    def _expire_forwards(self, now: datetime) -> None:
        """Drop queued candidates past their own TTL horizon, handing the prescreen day slot back
        to every one of them that was never evaluated.

        A TTL expiry never CHARGED the forward cap: entries leave this queue through
        :meth:`_take_forward_slot`, which pops an entry *before* it journals a forward, so anything
        still queued has had no analyst call, no ``agent_calls`` row and no gate verdict. That is the
        never-evaluated case :meth:`~engine.strategy.prescreen.SignalPreScreen.rearm` exists for.

        Scope, deliberately narrow: this refunds the prescreen ADMISSION slot only. The forward
        cap and ``_forwarded_count`` are untouched.

        A ``front`` entry (the re-queue of an evaluation that blew up mid-call) re-arms like every
        other never-evaluated exit: :meth:`_handle_forward_failure` REFUNDS the charge before it
        re-queues, so it holds no charge and no verdict. (The front-retry LOST branch keeps its
        charge and never re-queues, so nothing here reaches it.)
        """
        live: list[_PendingForward] = []
        for stale in self._pending_forwards:
            if stale.expires_at > now:
                live.append(stale)
                continue
            _log.info("forward_queue_expired", signal_id=stale.candidate.signal_id,
                      symbol=stale.candidate.symbol, strategy_id=stale.candidate.strategy_id,
                      score=stale.candidate.score, queued_at=stale.fired_at.isoformat(),
                      front=stale.front)
            self._rearm_slot(stale.candidate)
        self._pending_forwards = live

    def _flush_forwards_at_window_close(self, d: date, closed_at: datetime | None) -> None:
        """Hand the prescreen admission slot back to everything the CLOSED window stranded.

        :meth:`_drain_one_forward` returns on a closed window without touching the queue, and
        :meth:`_expire_forwards` only runs inside :meth:`_take_forward_slot`, which that early
        return never reaches. A candidate queued near the close would otherwise sit in a dead queue,
        holding a day slot nothing will ever look at.

        Same never-evaluated accounting as :meth:`_expire_forwards`, ``front`` entries included (a
        front retry carries no forward charge).

        Once per CLOSE, not once per day: ``closed_at`` is the window end this flush answers, and a
        window the owner moves or re-opens later in the session presents a different end and
        flushes again. ``closed_at=None`` is the no-window day, latched on the date instead.
        """
        if not self._pending_forwards:
            return
        if closed_at is None:
            if self._window_close_flushed_day == d:
                return
        elif self._window_close_flushed_at == closed_at:
            return
        dropped = list(self._pending_forwards)
        if closed_at is None:
            self._window_close_flushed_day = d
        else:
            self._window_close_flushed_at = closed_at
        self._pending_forwards = []
        for entry in dropped:
            self._rearm_slot(entry.candidate)
        _log.warning("forward_queue_window_closed", d=d.isoformat(), count=len(dropped),
                     closed_at=None if closed_at is None else closed_at.isoformat(),
                     best_score=max(float(p.candidate.score) for p in dropped),
                     pairs=[f"{p.candidate.symbol}/{p.candidate.strategy_id}" for p in dropped],
                     front=sum(1 for p in dropped if p.front))

    def _apply_displacements(self) -> None:
        """Act on the prescreen admission slots the pre-screen re-assigned.

        The pre-screen decides displacement on a scan worker thread and can only record the verdict;
        both things it IMPLIES must happen on the event loop: the evicted candidate leaves
        ``_pending_forwards`` (it must never reach the analyst), and its day-slot journal row goes
        back to ``evaluated=0`` — the never-evaluated accounting :meth:`_expire_forwards` uses.

        This is the tidy path, not the safe one. Safety is :meth:`SignalPreScreen.claim_slot`, which
        catches an entry already popped by a drain tick between the decision and this sweep. A
        displaced entry that survives both is harmless — its TTL expiry re-arms it.

        ``front`` entries (front retries) are skipped defensively: the pre-screen marked the pair
        EVALUATED at :meth:`_take_forward_slot`'s claim (the forward-charge refund does not undo that
        mark), so it could not have chosen one as a victim — but the queue would be corrupted if that
        stopped holding. A pair RETAINED by that skip is not re-armed either: the two walks below
        must agree, or the retained entry would keep its queue pointer while its slot was handed
        back as free.

        Never raises: a telemetry-adjacent hand-off must not be able to kill the trigger path.
        """
        if self._take_displaced is None:
            return
        try:
            pairs = list(self._take_displaced())
        except Exception as exc:  # noqa: BLE001 - a broken hand-off costs tidiness, never a trade
            _log.warning("displacement_apply_failed", error=str(exc))
            return
        if not pairs:
            return
        victims = set(pairs)
        live: list[_PendingForward] = []
        retained: set[tuple[str, str]] = set()
        for entry in self._pending_forwards:
            key = (entry.candidate.symbol, entry.candidate.strategy_id)
            if key in victims:
                if not entry.front:
                    _log.info("forward_queue_displaced", signal_id=entry.candidate.signal_id,
                              symbol=entry.candidate.symbol,
                              strategy_id=entry.candidate.strategy_id,
                              score=entry.candidate.score, queued_at=entry.fired_at.isoformat(),
                              reason="admission slot reassigned to a better candidate")
                    continue
                retained.add(key)
            live.append(entry)
        self._pending_forwards = live
        for pair in pairs:
            if pair in retained:
                continue
            self._rearm_slot_pair(*pair)

    def _best_pending_score(self) -> float | None:
        """Highest score sitting unforwarded in the queue — the live starvation reading."""
        if not self._pending_forwards:
            return None
        return max(float(p.candidate.score) for p in self._pending_forwards)

    def _take_forward_slot(self, cap: int | None) -> SignalCandidate | None:
        """Spend one analyst slot on the best pending candidate, or return None if none is due.

        THE COMMIT POINT for two different budgets, which is why the claim lives here and nowhere
        else. Popping the entry spends the forward cap; :meth:`SignalPreScreen.claim_slot`
        simultaneously commits the prescreen ADMISSION slot to a real evaluation, making it permanent.
        A ``False`` claim means a better same-strategy candidate took that admission slot while this
        one waited — forwarding it anyway would spend the reassigned slot twice — so the entry is
        dropped and the loop tries the next-best rather than stalling a whole pacing interval.

        The claim runs under the PRE-SCREEN's lock, which makes it atomic against the displacement
        decision: claim-then-displace leaves the pair evaluated and undisplaceable,
        displace-then-claim returns ``False`` here. Neither order can produce two spends.

        TWO guaranteed-reject screens run BEFORE the claim and DEFER rather than drop — the entry
        band (a level-pinned candidate whose level has walked outside the risk gate's
        ``entry_sanity_band``) and the intraday warm-up class (:meth:`_intraday_warmup_short`; the
        arrival screen cannot cover a candidate queued while coverage was complete). Both are live
        readings that can come back true minutes later, and ``brk20``/``orb`` re-publish at most once
        a day, so a candidate re-armed out of the queue would have nothing to re-publish it. It stays
        queued (its own TTL is still its exit) and the loop moves to the next-best. Screening before
        the claim keeps it displaceable while it waits: ``claim_slot`` marks a pair permanently
        evaluated, a promise only a candidate actually being dispatched may make.
        """
        self._expire_forwards(self._clock.now())
        if cap is not None and self._forwarded_count >= int(cap):
            return None
        deferred: list[_PendingForward] = []
        try:
            while self._pending_forwards:
                if self._forward_mode == "arrival":
                    # The front flag outranks arrival order here too — the rollback mode must
                    # not quietly lose the re-queue guarantee.
                    entry = min(self._pending_forwards,
                                key=lambda p: (0 if p.front else 1, p.fired_at, p.seq))
                else:
                    entry = min(self._pending_forwards, key=self._forward_key)
                self._pending_forwards.remove(entry)
                if (self._outside_entry_band(entry.candidate)
                        or self._intraday_warmup_short(entry.candidate, at="forward_slot")):
                    deferred.append(entry)
                    continue
                if not self._claim_forward_slot(entry.candidate):
                    _log.info("forward_slot_displaced", signal_id=entry.candidate.signal_id,
                              symbol=entry.candidate.symbol,
                              strategy_id=entry.candidate.strategy_id, score=entry.candidate.score,
                              reason="admission slot was reassigned to a better candidate")
                    self._rearm_slot(entry.candidate)
                    continue
                self._forwarded_count += 1
                # A NEW charge re-opens the refund for this signal_id (the front retry is the one
                # candidate that can be charged twice in a day).
                self._refunded_forwards.discard(entry.candidate.signal_id)
                if self._forwarded_day is not None:
                    self._journal_forward(entry.candidate, self._forwarded_day)
                return entry.candidate
            return None
        finally:
            # Every exit puts the screened-out entries back, including the one that returns a
            # candidate: a deferral is "not this tick", never "off the queue".
            self._pending_forwards.extend(deferred)

    def _outside_entry_band(self, candidate: SignalCandidate) -> bool:
        """True when ``candidate``'s entry level is already outside the ``entry_sanity_band``
        against the live LTP — a guaranteed gate reject that must not cost an analyst slot.

        Scoped to :data:`BAND_SKIP_STRATEGIES` (the LIMIT-at-level legs).

        Fails OPEN in every unknown: no ``ltp_fn``, no tick for the symbol, a non-positive LTP, an
        unreadable limit table or a raising seam all mean "we cannot say this is a guaranteed
        reject", and the safe direction for that is to forward it and let the gate decide.

        A PROXY for :meth:`~engine.risk.gate.RiskGate._rule_entry_sanity_band`, not a copy of it. The
        deviation formula, the per-product band and the ``dev <= band`` edge (so ``dev == band`` is a
        PASS) are the rule's, byte for byte. Two inputs are not, both in the skip direction: the gate
        bands the ANALYST's ``action.entry_price`` while this bands the SCANNER's ``raw_levels.entry``
        (nothing pins the two together — the structural-coherence guard checks identity fields, not
        price), and the gate exempts ``entry_type == "MARKET"`` outright while this has no such
        exemption. The assumption that makes the proxy sound is the one that defines
        :data:`BAND_SKIP_STRATEGIES`: a LIMIT-at-level leg's proposal restates the level. That is why
        the caller DEFERS a skipped candidate instead of dropping it — a proxy may cost a tick, never
        the day.
        """
        if self._ltp_fn is None or candidate.strategy_id not in BAND_SKIP_STRATEGIES:
            return False
        # One try over the whole computation: a seam that raises, a limit store that will not verify
        # and an LTP that will not convert are the same fact — the screen cannot decide, so it does
        # not get to withhold the evaluation.
        try:
            raw_ltp = self._ltp_fn(candidate.symbol)
            if raw_ltp is None:
                return False
            ltp = _dec(raw_ltp)
            if ltp <= 0:
                return False
            band = _dec(self._entry_band_pct(_product_of(candidate.style)))
            entry = _dec(candidate.raw_levels.entry)
            dev = abs(entry - ltp) / ltp * _HUNDRED
        except Exception as exc:  # noqa: BLE001 - an undecidable band never withholds an evaluation
            _log.warning("forward_band_screen_failed", symbol=candidate.symbol,
                         strategy_id=candidate.strategy_id, error=str(exc))
            return False
        if dev <= band:
            return False
        _log.info("forward_skipped_outside_band", signal_id=candidate.signal_id,
                  symbol=candidate.symbol, strategy_id=candidate.strategy_id,
                  entry=str(entry), ltp=str(ltp),
                  dev=str(dev.quantize(_PAISA, rounding=ROUND_HALF_UP)), band=str(band))
        return True

    def _claim_forward_slot(self, candidate: SignalCandidate) -> bool:
        """Ask the pre-screen to commit this pair's admission slot. Unwired ⇒ ``True``. A RAISING
        claim is also ``True``: the prescreen seam failing must degrade to "forward it anyway",
        never to silently withholding analyst calls."""
        if self._claim_slot is None:
            return True
        try:
            return bool(self._claim_slot(candidate.symbol, candidate.strategy_id))
        except Exception as exc:  # noqa: BLE001 - a broken claim seam never withholds an evaluation
            _log.warning("forward_slot_claim_failed", symbol=candidate.symbol,
                         strategy_id=candidate.strategy_id, error=str(exc))
            return True

    def _forward_cap(self) -> int | None:
        """Today's analyst forward cap, or ``None`` when the governor does not publish one."""
        cap_fn = getattr(self._governor, "prescreen_forward_cap", None)
        return cap_fn() if cap_fn is not None else None

    # ------------------------------------------------- raw funnel counters, persisted
    def _flush_funnel_raw(self) -> None:
        """UPSERT today's per-strategy RAW pre-screen counts into ``funnel_raw_counts``.

        The top of the funnel (what the scanners PRODUCED, before dedupe and the prescreen caps)
        needs a DB home so a restart does not zero it for the nightly ``funnel_utilization`` line.
        It rides the 60 s drain tick (off the scan path), at most one flush per tick and only when
        the counts CHANGED since the last one.

        ABSOLUTE values, never increments. The in-memory counter is hydrated from these rows on the
        pre-screen's first day roll (``SignalPreScreen._load_raw_counts_locked``), so it already
        holds the day's running total across every process — an UPSERT is idempotent, and a
        duplicated or replayed flush cannot double-count.

        Never raises: telemetry must not be able to break trading. A failed flush logs
        ``funnel_raw_flush_failed`` and leaves the previously-persisted rows as they were.
        """
        if self._funnel_raw is None:
            return
        try:
            d = self._clock.today()
            counts = {str(k): int(v) for k, v in self._funnel_raw(d).items()}
            if d == self._funnel_raw_day and counts == self._funnel_raw_flushed:
                return                              # nothing fired since the last tick
            if counts:                              # a day-roll to an empty set writes nothing
                with transaction(self._conn):
                    for strategy_id, fires in counts.items():
                        self._conn.execute(
                            "INSERT INTO funnel_raw_counts (d, strategy_id, fires) "
                            "VALUES (?, ?, ?) "
                            "ON CONFLICT(d, strategy_id) DO UPDATE SET fires=excluded.fires",
                            (d.isoformat(), strategy_id, fires),
                        )
            self._funnel_raw_day = d
            self._funnel_raw_flushed = counts
        except Exception as exc:  # noqa: BLE001 - a telemetry write never costs the drain a tick
            _log.warning("funnel_raw_flush_failed", error=str(exc))

    # -------------------------------------------------- the orphaned-proposal sweep
    async def _sweep_orphans_if_due(self) -> None:
        """Run :meth:`sweep_orphaned_proposals` at most once per ``_ORPHAN_SWEEP_INTERVAL_MIN``.

        The anchor advances on every sweep that passes the guard — dispatched or not — exactly like
        :attr:`_last_forward_drain`.
        """
        now = self._clock.now()
        last = self._last_orphan_sweep
        if last is not None and now - last < timedelta(minutes=_ORPHAN_SWEEP_INTERVAL_MIN):
            return
        self._last_orphan_sweep = now
        await self.sweep_orphaned_proposals()

    async def sweep_orphaned_proposals(self) -> int:
        """Announce every proposal that has carried NO verdict for ``_ORPHAN_TTL_MIN`` minutes.

        The backstop for a stalled gate-context build. :meth:`_gate_and_persist` writes the proposal
        row and only THEN builds the gate context, so anything that kills the path between those two
        points (a store stall, a crash, a killed process) strands a proposal that is not a decline,
        not an agent failure, not a rejection — a row with nothing attached to it. This sweep is
        the measure that notices it.

        Each ``proposal_id`` is announced ONCE per process-day (:attr:`_orphans_alerted`, rolled by
        :meth:`_roll_orphan_day`) — but only while it is YOUNGER than
        :data:`_ORPHAN_ALERT_MAX_AGE_MIN` (24 h). A permanent orphan is unresolvable by design (no
        verdict is ever invented). Past the cap it is logged at INFO once per process-day and counted
        by the nightly review, never paged. Returns how many orphans were announced OR quietly noted.
        Never raises — a watchdog that can kill the tick it rides on is not a watchdog.
        """
        now = self._clock.now()
        self._roll_orphan_day(self._clock.today())
        cutoff = now - timedelta(minutes=_ORPHAN_TTL_MIN)
        try:
            rows = self._conn.execute(
                "SELECT p.proposal_id AS proposal_id, p.action AS action, "
                "p.created_at AS created_at "
                "FROM proposals p LEFT JOIN verdicts v "
                f"ON v.proposal_id = p.proposal_id AND {scope_sql('real', 'v')} "
                "WHERE v.verdict_id IS NULL AND p.created_at < ? "
                "ORDER BY p.created_at",
                (cutoff.isoformat(),),
            ).fetchall()
        except Exception as exc:  # noqa: BLE001 - the watchdog never costs the drain a tick
            _log.warning("orphan_sweep_failed", error=str(exc))
            return 0
        announced = 0
        for row in rows:
            proposal_id = str(row["proposal_id"])
            if proposal_id in self._orphans_alerted:
                continue
            # Memoised BEFORE the send, so a failed notification cannot turn one orphan into an
            # alert on every subsequent sweep (``_send`` is best-effort by design).
            self._orphans_alerted.add(proposal_id)
            announced += 1
            action = str(row["action"] or "")
            age_min = self._orphan_age_min(row["created_at"], now)
            if age_min is not None and age_min > _ORPHAN_ALERT_MAX_AGE_MIN:
                # History, not news (see _ORPHAN_ALERT_MAX_AGE_MIN): visible in the log and the
                # nightly review, but it never pages the owner again.
                _log.info("proposal_orphaned_stale", proposal_id=proposal_id, action=action,
                          age_min=age_min, created_at=str(row["created_at"]))
                continue
            _log.error("proposal_orphaned", proposal_id=proposal_id, action=action,
                       age_min=age_min, created_at=str(row["created_at"]))
            await self._send(CatalogMessage(
                kind=MessageKind.LIMIT_BREACH,
                title=f"Proposal orphaned: no gate verdict ({action or 'unknown'})",
                body=(
                    f"Proposal {proposal_id} ({action or 'unknown action'}) has been persisted for "
                    f"{'?' if age_min is None else age_min} minutes with no verdict row. The gate "
                    "never finished judging it, so nothing was recommended and nothing was "
                    "decided — the proposal is evidence of a funnel stall, not of a rejection "
                    "(after the 2026-08-21 gate-context freeze). No verdict is invented "
                    "for it; check the log around its created_at."
                ),
                severity="warning",
                data={
                    "rule_id": "proposal_orphaned",
                    "proposal_id": proposal_id,
                    "action": action,
                    "age_min": str(age_min),
                    "value": f"{age_min} min with no verdict",
                    "limit": f"a proposal must reach a verdict within {_ORPHAN_TTL_MIN} min",
                },
            ))
        return announced

    def _roll_orphan_day(self, d: date) -> None:
        """Drop the announced-orphan memo on a date change — the sibling of the
        ``_requeued_forwards`` clear in :meth:`_roll_forward_day`, and what bounds this set."""
        if self._orphans_alerted_day == d:
            return
        self._orphans_alerted_day = d
        self._orphans_alerted.clear()

    @staticmethod
    def _orphan_age_min(created_at: Any, now: datetime) -> float | None:
        """Minutes since ``created_at`` (an ISO string from the row), or None if it will not parse.
        An unparseable timestamp still gets an alert — the orphan is the finding, not its age."""
        try:
            created = datetime.fromisoformat(str(created_at))
        except (TypeError, ValueError):
            return None
        if created.tzinfo is None:
            created = created.replace(tzinfo=now.tzinfo)
        return round((now - created).total_seconds() / 60.0, 1)

    # ------------------------------------------------------------ paced drain
    async def drain_forward_queue(self) -> bool:
        """Spend at most one analyst slot, at most once per ``FORWARD_PACING_MIN`` minutes.

        Pulsed every 60 s from the composition root (``forward_drain_tick``); THIS method owns the
        cadence; the tick owns only the pulse. Returns True when a candidate reached the analyst.
        Pacing is what gives the ranking rule a population to rank (draining inline on arrival
        leaves one entry in the queue at every slot).

        The tick ALSO carries the pre-screen's raw-counter flush — run before the drain-mode check,
        so the ``immediate`` rollback keeps its telemetry (:meth:`_flush_funnel_raw`) — and the
        orphan sweep (:meth:`_sweep_orphans_if_due`): both are watchdogs on the funnel and must
        still run when the funnel has stopped moving.

        The day ROLL rides this tick too: :meth:`_roll_forward_day`'s other callers (an arriving
        candidate, the drain) never reach a fresh process with no candidates, so without it the boot
        orphan re-arm would wait on a thin day forever.
        """
        self._roll_forward_day(self._clock.today())
        self._flush_funnel_raw()
        # Settle reassigned admission slots here too: the batch admission path
        # (``run_scan_sweep`` → ``prescreen.admit``) can displace without any candidate reaching
        # :meth:`on_signal_candidate`, and this is the one cadence that always runs.
        self._apply_displacements()
        await self._sweep_orphans_if_due()
        if self._drain_mode != "paced":
            return False
        now = self._clock.now()
        last = self._last_forward_drain
        if last is not None and now - last < timedelta(minutes=FORWARD_PACING_MIN):
            return False
        self._last_forward_drain = now
        return await self._drain_one_forward()

    async def _drain_one_forward(self) -> bool:
        """Dispatch the single best pending candidate; False when none is due.

        ONE per call, never a burst: draining the whole remaining budget at once is the inline drain
        again, just batched.

        Every gate :meth:`on_signal_candidate` applies at arrival is re-applied HERE because time
        has passed since the candidate was queued: the window can have closed, the owner can have
        killed or frozen, the budget can have run out. A closed gate leaves the queue untouched and
        re-arms nothing at that moment; the queued candidate's only exit is its own TTL, which hands
        the prescreen admission slot back (:meth:`_expire_forwards`). The forward cap is given back
        only where a charge bought no verdict at all (:meth:`_refund_forward`).

        Two re-checks are per CANDIDATE rather than per tick, so they live at the commit point: the
        entry band and the intraday warm-up class, both in :meth:`_take_forward_slot`, which defers
        the screened entry and takes the next-best.

        The one gate that is NOT "come back next tick" is the window having CLOSED — no later tick
        can drain what is queued, so that branch flushes the queue once
        (:meth:`_flush_forwards_at_window_close`). Mode / FROZEN / kill keep the queue untouched:
        those lift, a closed window does not.
        """
        if not self._pending_forwards:
            return False
        if self._mode.mode() not in (Mode.RECOMMEND, Mode.AUTO):
            return False
        if self._mode.risk_state() != RiskState.NORMAL:
            return False
        if self._kill.is_killed():
            return False
        d = self._clock.today()
        window = self._window(d)
        now = self._clock.now()
        if window is None:
            # No window at all for ``d`` — a non-trading day the queue survived into. Latched on the
            # date, because there is no window end to key the flush on.
            self._flush_forwards_at_window_close(d, None)
            return False
        if now > window[1]:
            # The window is behind us: nothing queued can be drained again under THIS window. The
            # owner can still re-open one later in the session, which is a different close.
            self._flush_forwards_at_window_close(d, window[1])
            return False
        if now < window[0]:
            return False                 # not open YET: the queue is still live, leave it alone
        self._roll_forward_day(d)
        decision = self._governor.can_invoke(INTRADAY_AGENT_ID)
        if not decision.allowed:
            # Unlike the arrival path (where a block DROPS the candidate to keep a blocked window
            # from building a backlog), a block here simply skips this tick: the queue is already
            # bounded and drained at most once per pacing interval, so retrying is one call every
            # FORWARD_PACING_MIN minutes, not a stampede when the block lifts.
            _log.warning("forward_drain_governor_blocked", queued=len(self._pending_forwards),
                         reason=getattr(decision, "reason", None))
            return False
        cap = self._forward_cap()
        chosen = self._take_forward_slot(cap)
        if chosen is None:
            _log.info("forward_drain_no_slot", forwarded=self._forwarded_count,
                      cap=None if cap is None else int(cap),
                      queued=len(self._pending_forwards),
                      best_unforwarded_score=self._best_pending_score())
            return False
        # Which candidate the analyst saw, and what was still waiting behind it, must be
        # answerable without a DB read.
        _log.info("forward_drained", signal_id=chosen.signal_id, symbol=chosen.symbol,
                  strategy_id=chosen.strategy_id, score=chosen.score,
                  forwarded=self._forwarded_count, cap=None if cap is None else int(cap),
                  queued=len(self._pending_forwards),
                  best_unforwarded_score=self._best_pending_score())
        await self._evaluate_forward_guarded(chosen, d)
        return True

    # ------------------------------------------------------------ the drain never loses one
    async def _evaluate_forward_guarded(self, candidate: SignalCandidate, d: date) -> None:
        """Evaluate ``candidate``, and make a blown-up evaluation VISIBLE and RECOVERABLE.

        An ``asyncio.CancelledError`` raised inside the analyst-call transport would otherwise
        propagate through the drain and kill the APScheduler tick, after the candidate was already
        popped and charged a forward but before any ``agent_calls`` row exists — invisible to every
        DB measure.

        The decision table:

        * **first failure for a signal_id** — re-queue at the FRONT and log ``forward_evaluation_requeued``
          at WARNING. A ``CancelledError`` is then RE-RAISED: shutdown cancellation must propagate or
          the engine cannot stop. A generic exception is swallowed so the tick survives for the rest
          of the queue.
        * **second failure** — no re-queue; log ``forward_evaluation_lost`` at ERROR, then the same
          re-raise/swallow split. One retry, not a loop: a candidate that fails twice is failing for
          a reason a third attempt will not fix, and a self-refilling queue would spend the day's
          whole analyst cap on one broken symbol.

        The RE-QUEUE branch refunds the charge :meth:`_take_forward_slot` made, because the front
        entry will be drained again and charged AGAIN — otherwise one candidate that blew up mid-call
        costs two slots of the day's forward cap. The LOST branch does not refund: nothing will charge
        for that candidate again (one candidate, one net charge). A lost candidate is never marked
        evaluated by any path here.

        This guard wraps the DRAIN only. ``immediate`` mode (the paced-drain rollback, where an
        arriving candidate evaluates itself inline on the bus handler) has no scheduler tick to kill.
        """
        try:
            await self._evaluate_forward(candidate, d)
        except asyncio.CancelledError:
            self._handle_forward_failure(candidate, "CancelledError", d)
            raise                                # shutdown cancellation always propagates
        except Exception as exc:                 # noqa: BLE001 - the tick must outlive one candidate
            self._handle_forward_failure(candidate, type(exc).__name__, d)

    def _handle_forward_failure(
        self, candidate: SignalCandidate, error_class: str, d: date
    ) -> None:
        """Re-queue once, then let it go loudly. Never raises: it is the failure path."""
        fields = {
            "signal_id": candidate.signal_id,
            "symbol": candidate.symbol,
            "strategy_id": candidate.strategy_id,
            "error_class": error_class,
        }
        if candidate.signal_id in self._requeued_forwards:
            _log.error("forward_evaluation_lost", **fields)
            return
        self._requeued_forwards.add(candidate.signal_id)
        self._refund_forward(candidate, d, reason=f"requeued_{error_class}")
        self._enqueue_forward(candidate, front=True)
        _log.warning("forward_evaluation_requeued", queued=len(self._pending_forwards), **fields)

    def _refund_forward(self, candidate: SignalCandidate, d: date, *, reason: str) -> None:
        """Give back the one forward charge :meth:`_take_forward_slot` made.

        The charge and the journal row are written BEFORE the analyst call, so an infrastructure
        failure — timeout, SDK death, transport cancellation — spends a slot on a call that produced
        no verdict, no ``agent_calls`` row and no proposal. This refunds the analyst quota for that
        fact only — NOTHING EVALUATED IT. A governor block, a no_action verdict and a gate rejection
        are real outcomes and never reach here.

        Exactly one unit, floored at zero on both counters: the in-memory count can legitimately be
        0 when a restart lost the increment the journal still holds, and the journal column is a
        COUNTER of attempts (:meth:`_journal_forward`) that must never go negative. Never raises —
        a failed refund leaves the cap over-charged, the safe side.

        ONE REFUND PER CHARGE, enforced here rather than by the call order: two call sites can fire
        for the same charge (:meth:`_evaluate_forward`'s failure branch, and
        :meth:`_handle_forward_failure` when anything in that branch raises on the way out).
        Refunding twice would hand the day an analyst call the cap did not grant, so the signal_id is
        memoed and the repeat is a logged no-op. The memo is cleared when
        :meth:`_take_forward_slot` charges the same candidate again.
        """
        if candidate.signal_id in self._refunded_forwards:
            _log.info("forward_cap_refund_skipped", signal_id=candidate.signal_id,
                      symbol=candidate.symbol, strategy_id=candidate.strategy_id, reason=reason,
                      forwarded=self._forwarded_count)
            return
        self._refunded_forwards.add(candidate.signal_id)
        self._forwarded_count = max(0, self._forwarded_count - 1)
        try:
            self._conn.execute(
                "UPDATE prescreen_day_slots SET forwarded = MAX(forwarded - 1, 0) "
                "WHERE d=? AND symbol=? AND strategy_id=?",
                (d.isoformat(), candidate.symbol, candidate.strategy_id),
            )
        except Exception as exc:  # noqa: BLE001 - journaling is resilience bookkeeping, never a gate
            _log.warning("day_slot_journal_failed", op="refund_forward", symbol=candidate.symbol,
                         strategy_id=candidate.strategy_id, error=str(exc))
        _log.info("forward_cap_refunded", signal_id=candidate.signal_id, symbol=candidate.symbol,
                  strategy_id=candidate.strategy_id, reason=reason,
                  forwarded=self._forwarded_count)

    def _intraday_warmup_short(self, candidate: SignalCandidate, *, at: str) -> bool:
        """Is this an INTRADAY-style candidate facing a short intraday warm-up class right now?

        PER SYMBOL: only a hole attributed to THIS candidate's symbol — or one attributable to no
        symbol at all — refuses it.

        Reads the SAME snapshot the gate context reads, so the pre-screen answer and the gate's
        ``warmup_ready`` verdict can never disagree — the point is only to learn it before the
        analyst call. Daily-class candidates are not this screen's business: they never read a
        1-minute bar. An unwired seam, a status with no per-class answer, or a failing read ⇒ False:
        the screen is a cost optimisation and the GATE remains the enforcement (it fails closed on the
        same snapshot), so failing open costs at most one analyst call.

        Asked at BOTH commit points, because coverage is a live reading and the two are minutes to
        hours apart: ``at="arrival"`` (:meth:`on_signal_candidate`, where the answer re-arms the day
        slot) and ``at="forward_slot"`` (:meth:`_take_forward_slot`, where it defers the queued entry
        instead — a hole that opens after a candidate is queued is invisible to the arrival screen)."""
        if self._warmup_status_fn is None or candidate.style != "intraday":
            return False
        try:
            status = self._warmup_status_fn()
            ready_for = getattr(status, "ready_for", None)
            short = ready_for is not None and not ready_for(CLASS_INTRADAY, candidate.symbol)
        except Exception as exc:  # noqa: BLE001 - the gate still enforces; never lose the candidate
            _log.warning("warmup_snapshot_read_failed", symbol=candidate.symbol, error=str(exc))
            return False
        if not short:
            return False
        # One line per (symbol, strategy, commit point) per DAY: a market-wide hole would otherwise
        # log on every publication and every drain retry.
        today = self._clock.today()
        if self._intraday_warmup_logged_day != today:
            self._intraday_warmup_logged_day = today
            self._intraday_warmup_logged.clear()
        key = (candidate.symbol, candidate.strategy_id, at)
        if key not in self._intraday_warmup_logged:
            self._intraday_warmup_logged.add(key)
            # The lines that refused THIS symbol; none attributed ⇒ show the class's first few.
            lines = list(getattr(status, "blockers_by_class", {}).get(CLASS_INTRADAY, []))
            mine = [b for b in lines if blocker_symbol(b) == candidate.symbol]
            _log.info("signal_candidate_intraday_warmup", signal_id=candidate.signal_id,
                      symbol=candidate.symbol, strategy_id=candidate.strategy_id, at=at,
                      blockers=mine or lines[:4],
                      note="intraday coverage short for this symbol — no analyst call"
                           "; re-armed at arrival, left queued at the forward slot")
        return True

    # ================================================================== trigger (a): entries
    async def on_signal_candidate(self, candidate: SignalCandidate) -> None:
        """Candidate trigger. Also usable directly as a ``signal.candidate`` bus handler.

        Entry-seeking calls fire ONLY inside the owner-set trade window (risk limits
        ``trade_window``); every earlier check is cheaper still. A governor block fails to
        no-proposal — silently, because "we did not call" is already an ``agent_calls`` row.

        Under the default ``paced`` drain mode this method ADMITS a candidate to the queue and stops
        there; the analyst call happens on the next :meth:`drain_forward_queue` tick, which re-checks
        every gate below. The gates still run here so a candidate that cannot be evaluated at all is
        never queued, and the never-evaluated day-slot re-arms fire at the moment the drop happens.
        """
        # Drops where the candidate was NEVER EVALUATED hand the prescreen day slot back so a
        # still-true condition is waiting when conditions change (window opens, freeze lifts, mode
        # returns). The prescreen charges its caps once per pair, so the re-arm/re-publish cycle can
        # never exhaust a day cap. Deliberate NON-re-arms: a governor block (budget policy), the
        # forward cap (the analyst quota was spent on real evaluations), and an unsizeable candidate
        # (no stop ⇒ nothing to wait for today). A cap-refused candidate is QUEUED rather than
        # dropped; if no slot ever opens, its TTL expiry hands the day slot back
        # (:meth:`_expire_forwards`).
        #
        # FIRST, settle any admission slot the pre-screen reassigned — very possibly THIS candidate's
        # arrival caused one. Doing it before the journal write keeps the two rows from fighting.
        self._apply_displacements()
        self._journal_slot(candidate, self._clock.today())
        if self._mode.mode() not in (Mode.RECOMMEND, Mode.AUTO):
            self._rearm_slot(candidate)
            return
        if self._mode.risk_state() != RiskState.NORMAL:
            self._rearm_slot(candidate)
            return
        if self._kill.is_killed():
            self._rearm_slot(candidate)
            return
        if self._intraday_warmup_short(candidate, at="arrival"):
            # The risk state does not freeze on an intraday-only coverage hole, so the refusal is per
            # candidate and happens HERE, ahead of the analyst call (the gate would reject on
            # `warmup_ready` after the call was spent). Never-evaluated ⇒ the day slot goes back:
            # coverage heals mid-session and the candidate must be able to re-publish.
            self._rearm_slot(candidate)
            return
        d = self._clock.today()
        window = self._window(d)
        if window is None or not (window[0] <= self._clock.now() <= window[1]):
            _log.info("signal_candidate_out_of_window", signal_id=candidate.signal_id,
                      symbol=candidate.symbol)
            self._rearm_slot(candidate)
            return
        if candidate.raw_levels.stop is None:
            # No stop level ⇒ max_qty_by_risk is 0 ⇒ a guaranteed no_action — never spend an
            # analyst call on it. The day slot stays consumed: no stop will appear today.
            _log.info("signal_candidate_unsizeable", signal_id=candidate.signal_id,
                      symbol=candidate.symbol, strategy_id=candidate.strategy_id,
                      reason="no stop level — cannot size, analyst call would be wasted")
            return
        max_qty = self._max_qty_by_risk(candidate)
        if max_qty <= 0:
            # A real stop, but the per-trade-risk budget cannot afford even ONE share at this
            # stop's distance — the gate could never approve any quantity, the same
            # wasted-analyst-call case as the stopless check. Same funnel category (unsizeable,
            # never queued); the day slot stays consumed.
            self._journal_unsizeable_qty_zero(candidate, d)
            _log.info("candidate_unsizeable_qty_zero", signal_id=candidate.signal_id,
                      symbol=candidate.symbol, strategy_id=candidate.strategy_id,
                      entry=str(candidate.raw_levels.entry), stop=str(candidate.raw_levels.stop),
                      budget=str(self._risk_budget_inr(candidate)))
            return
        # ``in_flight``: _journal_slot above already wrote THIS pair as evaluated=1/forwarded=0, so
        # without the exclusion the boot orphan sweep would re-arm the candidate it is admitting.
        self._roll_forward_day(d, in_flight=(candidate.symbol, candidate.strategy_id))
        decision = self._governor.can_invoke(INTRADAY_AGENT_ID)
        if not decision.allowed:
            # A budget block is deliberate policy, not a missed slot: the candidate is neither
            # re-armed nor queued (queueing it would let a blocked window build a backlog that
            # hammers the admission gate the moment the block lifts).
            _log.warning("signal_candidate_governor_blocked", signal_id=candidate.signal_id,
                         reason=getattr(decision, "reason", None))
            return
        # Forward cap (agents.yaml prescreen_cap_per_day at DG0; floor(base × degraded_forward_cap_frac)
        # at DG1+): the governor owns the number, this counter the enforcement. Counts only calls
        # that reach the harness; the coarse prescreen settings cap still bounds candidate
        # PUBLICATION. The slot goes to the best PENDING candidate rather than to whoever arrived
        # while budget remained (see _forward_key): the arriving candidate joins the queue, then the
        # queue is drained — so a candidate refused at a full cap stays available for a slot that
        # opens later (a degrade-tier recovery raises the cap mid-day).
        self._day_scores.setdefault(candidate.strategy_id, []).append(float(candidate.score))
        self._enqueue_forward(candidate)
        if self._drain_mode == "paced":
            # The arriving candidate NEVER drains its own slot: it waits for the next drain tick and
            # competes with everything else that arrived inside the interval.
            _log.info("signal_candidate_queued", signal_id=candidate.signal_id,
                      symbol=candidate.symbol, strategy_id=candidate.strategy_id,
                      score=candidate.score, queued=len(self._pending_forwards),
                      forwarded=self._forwarded_count)
            return
        cap = self._forward_cap()
        chosen = self._take_forward_slot(cap)
        if chosen is None:
            _log.info("signal_candidate_forward_cap", signal_id=candidate.signal_id,
                      forwarded=self._forwarded_count, cap=None if cap is None else int(cap),
                      queued=len(self._pending_forwards),
                      best_unforwarded_score=self._best_pending_score())
            return
        if chosen.signal_id != candidate.signal_id:
            # The slot went to a better-standing candidate that was refused earlier; logged so
            # which candidate the analyst saw is answerable without a DB read.
            _log.info("forward_queue_preempted", arrived=candidate.signal_id,
                      forwarded=chosen.signal_id, symbol=chosen.symbol,
                      strategy_id=chosen.strategy_id, score=chosen.score,
                      queued=len(self._pending_forwards))
        await self._evaluate_forward(chosen, d)

    async def _evaluate_forward(self, candidate: SignalCandidate, d: date) -> None:
        """Spend the analyst call on ``candidate``: assemble, call, coherence-check, gate, deliver.

        Everything from here down is identical for both drain modes — the mode decides only WHEN a
        queued candidate arrives at this method, never what happens to it once it does.
        """
        entry_ref = _dec(candidate.raw_levels.entry)
        product = _product_of(candidate.style)
        max_qty_by_risk = self._max_qty_by_risk(candidate)
        actx = self._assembler.for_signal(
            candidate,
            equity=self._exposure.equity(),
            headroom_lines=self._headroom_lines(d),
            open_positions_summary=self._positions_summary(),
            cost_line=self._cost_line(entry_ref, max_qty_by_risk, product),
            max_qty_by_risk=max_qty_by_risk,
            sector_exposure_line=self._sector_exposure_line(d),
        )
        proposal_id = str(ULID())
        # ENTRY evaluation: this is the stamp the delivered entry recommendation
        # actually carries — :meth:`build_recommendation` only falls back to its own.
        valid_until = self._ttl(candidate.style)
        result = await self._harness.run_single_shot(
            self._agent_def(),
            actx,
            lambda raw: parse_intraday(
                raw, proposal_id=proposal_id, agent_id=INTRADAY_AGENT_ID,
                valid_until=valid_until, inputs_digest=actx.inputs_digest,
            ),
            json_schema=intraday_guidance_json_schema(),
        )
        if not result.ok:
            # Analyst INFRASTRUCTURE failure: the candidate was never evaluated, so hand its
            # once-per-day publication back AND give back the forward charge _take_forward_slot made
            # before the call. governor_blocked is deliberate budget policy, not an outage: it
            # re-arms nothing and refunds nothing (the slot the governor refused was never spent).
            #
            # The alert goes FIRST so the refund is made exactly once: this method runs inside
            # :meth:`_evaluate_forward_guarded`'s try, and an owner-notify seam that raises would
            # land in :meth:`_handle_forward_failure`, which refunds again for its re-queue. The
            # ordering is belt to :meth:`_refund_forward`'s braces (the charge is memoed by signal_id).
            await self._alert_agent_failed("signal_candidate", result)
            if result.reason != "governor_blocked":
                self._rearm_slot(candidate)
                self._refund_forward(candidate, d, reason=str(result.reason or "agent_failed"))
            return
        self._note_agent_ok()
        payload = result.payload
        if isinstance(payload, NoActionOutput):
            _log.info("signal_candidate_no_action", signal_id=candidate.signal_id,
                      reason=payload.reason)
            self._journal_decision(candidate, d, "no_action", payload.confidence, result.call_id, None)
            # A declined evaluation holds no position and so holds no slot: hand the pair back to
            # cap displacement. The journal row stays evaluated=1: the analyst ran, this is not a re-arm.
            if self._decline is not None:
                self._decline(candidate.symbol, candidate.strategy_id)
            return

        # STRUCTURAL COHERENCE (the structural half of "the analyst disposes of THIS candidate"):
        # every symbol-scoped GateContext fact is resolved for candidate.symbol, and the TTL/product
        # derive from candidate.style, so an LLM that substitutes any identity field would be judged
        # on another instrument's facts. A mismatch is a hallucination — dropped like
        # schema-invalid output, no proposal row, owner alerted.
        if payload.action == "enter":
            mismatches = {
                name: (got, want)
                for name, got, want in (
                    ("tradingsymbol", payload.tradingsymbol, candidate.symbol),
                    ("side", payload.side, candidate.side),
                    ("style", payload.style, candidate.style),
                    ("signal_id", payload.signal_id, candidate.signal_id),
                    ("strategy_id", payload.strategy_id, candidate.strategy_id),
                    ("features_snapshot_id", payload.features_snapshot_id,
                     candidate.features_snapshot_id or payload.features_snapshot_id),
                )
                if got != want
            }
            if mismatches:
                _log.warning("agent_output_incoherent", signal_id=candidate.signal_id,
                             mismatches={k: [str(g), str(w)] for k, (g, w) in mismatches.items()})
                await self._send(CatalogMessage(
                    kind=MessageKind.LIMIT_BREACH,
                    title="Analyst output dropped (identity mismatch)",
                    body=(
                        f"The intraday analyst answered candidate {candidate.signal_id} "
                        f"({candidate.symbol}) with different identity fields "
                        f"({', '.join(sorted(mismatches))}) — dropped like schema-invalid output"
                        "; nothing was recommended."
                    ),
                    severity="warning",
                    data={"signal_id": candidate.signal_id,
                          "mismatches": {k: [str(g), str(w)] for k, (g, w) in mismatches.items()}},
                ))
                return
            self._journal_decision(
                candidate, d, "enter", payload.confidence, result.call_id, payload.proposal_id)
        else:
            _log.warning("analyst_decision_unclassified", signal_id=candidate.signal_id,
                         action=payload.action)

        # The analyst sometimes returns a LIMIT proposal with entry_price null, which the gate would
        # reject unpriced; the scanner's own level was already pre-screened at the forward slot, so
        # default from it rather than drop the row.
        if payload.action == "enter" and payload.entry_type == "LIMIT" and payload.entry_price is None:
            level = candidate.raw_levels.entry      # RawLevels.entry is never None (types.py)
            payload = EnterAction.model_validate({**payload.model_dump(), "entry_price": level})
            _log.info(
                "enter_limit_price_defaulted", signal_id=candidate.signal_id,
                symbol=candidate.symbol, strategy_id=candidate.strategy_id, entry_price=str(level),
            )
        stop_anchor = None
        if payload.action == "enter":
            payload, stop_anchor = self._one_exit(payload, candidate)

        side = str(getattr(payload, "side", candidate.side))
        try:
            verdict, gate_ctx = await self._gate_and_persist(
                payload, candidate.symbol, side, candidate.style, d,
            )
        except GateContextTimeout as exc:
            # Handled HERE, not left to _evaluate_forward_guarded, whose front re-queue is the wrong
            # move for a stalled store (the retry would hit the same dead store and spend a second
            # analyst call). Re-arming the day slot lets the pair re-publish once the store answers.
            # No paper verdict either: the proposal stays a real orphan for the orphan-sweep alert.
            await self._handle_gate_context_timeout(candidate, exc)
            return
        if verdict.verdict == "owner_approval_required":
            await self._request_owner_approval(payload, verdict)
        elif verdict.verdict in ("approve", "shrink") and payload.action == "enter":
            await self._deliver_entry(payload, verdict, gate_ctx, candidate, entry_ref, stop_anchor, d)
        await self._paper_branch(payload, candidate.symbol, side, candidate.style, d)

    async def _deliver_entry(
        self, payload: EnterAction, verdict: GateVerdict, gate_ctx: Any, candidate: SignalCandidate,
        entry_ref: Decimal, stop_anchor: Decimal | None, d: date,
    ) -> None:
        # Same reference the gate priced the proposal on: the LIMIT price when given, else the live
        # LTP, else the candidate's trigger level (never a guess above all three).
        reference = _dec(getattr(gate_ctx, "ltp", None) or entry_ref)
        extras = await self._collect_extras(payload, reference, d)
        rec = self.build_recommendation(
            payload, verdict, reference, extras, raw_levels=candidate.raw_levels,
            stop_anchor=stop_anchor,
        )
        self._book.deliver(
            rec,
            ledger_fields={
                "strategy_id": payload.strategy_id,
                "param_set_id": None,
                "feature_set_version": FEATURE_SET_VERSION,
                "features_snapshot_id": payload.features_snapshot_id,
                "thesis": payload.thesis,
                "confidence": payload.confidence,
                "agent_id": payload.agent_id,
                "proposal_id": payload.proposal_id,
                "verdict_id": verdict.verdict_id,
                "baseline_signal": 1,
                "llm_filter_decision": "confirmed",
                "catalyst_ref": candidate.catalyst_ref,
            },
        )
        # The payload states the live print next to a (possibly level-anchored) entry.
        live_ltp = getattr(gate_ctx, "ltp", None)
        await self._send(catalog.recommendation_message(
            rec, ltp=_dec(live_ltp) if live_ltp is not None else None,
        ))

    async def _paper_branch(self, payload: Any, symbol: str, side: str, style: str, d: date) -> None:
        """Judge the same proposal against the paper book (one ``is_paper=1`` verdict) and hand an
        approve/shrink to the paper submit seam. CNC entries only. Never raises into the real
        path, whose outcome is already final."""
        enabled, submit = self._paper_enabled_fn, self._paper_submit
        if (
            enabled is None
            or submit is None
            or payload.action != "enter"
            or _product_of(payload.style) != "CNC"
        ):
            return
        try:
            if not enabled():
                return
            verdict, _ = await self._evaluate_and_persist_verdict(
                payload, self._paper_ctx_builder, True, symbol, side, style, d
            )
            _log.info("paper_verdict", proposal_id=payload.proposal_id, symbol=symbol,
                      verdict=verdict.verdict, verdict_id=verdict.verdict_id)
            if verdict.verdict in ("approve", "shrink"):
                await submit(payload, verdict)
        except Exception:  # noqa: BLE001 - paper never costs the real path anything
            _log.exception("paper_branch_failed", proposal_id=payload.proposal_id, symbol=symbol)

    async def _handle_gate_context_timeout(
        self, candidate: SignalCandidate, exc: GateContextTimeout
    ) -> None:
        """The gate-context-timeout landing: say it loudly, give the slot back, leave the orphan alone.

        No verdict is fabricated: the proposal row survives with nothing attached to it and
        :meth:`sweep_orphaned_proposals` notices — an invented ``reject`` would read forever after as
        a decision the gate made. Never raises: it IS the failure path.
        """
        _log.error(
            "gate_context_timeout", signal_id=candidate.signal_id, symbol=candidate.symbol,
            strategy_id=candidate.strategy_id, proposal_id=exc.proposal_id,
            deadline_s=_GATE_CONTEXT_DEADLINE_S,
        )
        # Same treatment as an analyst INFRASTRUCTURE failure: the candidate was never judged, so
        # its once-per-day publication goes back to the prescreen.
        self._rearm_slot(candidate)
        await self._send(CatalogMessage(
            kind=MessageKind.LIMIT_BREACH,
            title=f"Gate context timed out ({candidate.symbol})",
            body=(
                f"The gate context for {candidate.symbol} did not build within "
                f"{_GATE_CONTEXT_DEADLINE_S:.0f}s, so candidate {candidate.signal_id} was never "
                f"judged and nothing was recommended. Proposal {exc.proposal_id} is persisted "
                "with no verdict — the market store is the suspect (2026-08-21). The "
                f"({candidate.symbol}, {candidate.strategy_id}) day slot has been handed back so "
                "the setup can re-publish; deterministic exits, stops and square-offs are "
                "unaffected."
            ),
            severity="critical",
            data={
                "rule_id": "gate_context_timeout",
                "signal_id": candidate.signal_id,
                "symbol": candidate.symbol,
                "strategy_id": candidate.strategy_id,
                "proposal_id": exc.proposal_id,
                "value": f"{_GATE_CONTEXT_DEADLINE_S:.0f}s deadline exceeded",
                "limit": "a gate-context build must answer within the deadline",
            },
        ))

    async def _handle_gate_context_timeout_position(
        self, position: sqlite3.Row, action: Any, exc: GateContextTimeout
    ) -> None:
        """The same gate-context-timeout landing for the two position-management paths (the
        position-event trigger and the time exit). No verdict is fabricated and the orphan is left
        for :meth:`sweep_orphaned_proposals`; there is no day slot to hand back (neither path is a
        prescreen publication). Shares the ERROR event name with the candidate handler. Never raises.
        """
        position_id = str(position["position_id"])
        symbol = str(position["symbol"])
        _log.error(
            "gate_context_timeout", position_id=position_id, symbol=symbol, action=action.action,
            proposal_id=exc.proposal_id, deadline_s=_GATE_CONTEXT_DEADLINE_S,
        )
        # The deterministic time exit is the only proposal here the platform builds itself.
        if str(action.agent_id) == PLATFORM_AGENT_ID:
            next_step = (
                "the check finishes the other due positions, then fails the time_exit_check job so "
                "the catch-up retries it (30-min sweep / next startup), re-issuing every due exit"
            )
        else:
            next_step = (
                "the stop-proximity debounce stamp was taken BEFORE the gate call, so the next "
                f"re-evaluation of this position waits the full {POSITION_EVENT_DEBOUNCE_MIN}-minute "
                "window"
            )
        await self._send(CatalogMessage(
            kind=MessageKind.LIMIT_BREACH,
            title=f"Gate context timed out ({symbol})",
            body=(
                f"The gate context for {symbol} did not build within "
                f"{_GATE_CONTEXT_DEADLINE_S:.0f}s, so the {action.action} recommendation for "
                f"position {position_id} was never judged and nothing was recommended. Proposal "
                f"{exc.proposal_id} is persisted with no verdict — the market store is the suspect"
                f". Next: {next_step}. Any stop the owner already placed on this position "
                "stands regardless."
            ),
            severity="critical",
            data={
                "rule_id": "gate_context_timeout",
                "position_id": position_id,
                "symbol": symbol,
                "action": action.action,
                "proposal_id": exc.proposal_id,
                "value": f"{_GATE_CONTEXT_DEADLINE_S:.0f}s deadline exceeded",
                "limit": "a gate-context build must answer within the deadline",
            },
        ))

    # ================================================================== recommendation assembly
    def build_recommendation(
        self, action: EnterAction, verdict: GateVerdict, entry_ref: Decimal,
        extras: RecExtras | None = None, *, raw_levels: RawLevels | None = None,
        stop_anchor: Decimal | None = None,
    ) -> Recommendation:
        """Build the owner payload from an approved/shrunk ``enter`` proposal. No I/O.

        ``entry_zone`` for a LIMIT proposal is the single proposed price on both sides — the price is
        the instruction. For a MARKET proposal there is no price yet, so the zone spans from the
        engine's entry reference to the ``entry_sanity_band`` edge for the product (+1% MIS /
        +2% CNC): that band is exactly the price range the gate would still have accepted, so a fill
        anywhere inside it is a fill the platform stands behind, and a fill outside it is the owner's
        signal to skip.
        """
        qty = int(verdict.approved_qty if verdict.approved_qty is not None else action.quantity)
        product = _product_of(action.style)
        if action.entry_type == "LIMIT" and action.entry_price is not None:
            zone = (_dec(action.entry_price), _dec(action.entry_price))
        else:
            band = _dec(self._entry_band_pct(product)) / _HUNDRED
            zone = (_money(entry_ref), _money(entry_ref * (Decimal(1) + band)))
        targets = [_dec(action.target_price)] if action.target_price is not None else []
        notional = _money(Decimal(qty) * zone[0])
        cost = verdict.cost or self._book.cost_model.round_trip(notional or zone[0], product)
        stop = _dec(action.stop_price)
        created_at = self._clock.now()
        hold = self._hold_fn(action.strategy_id, action.style) if self._hold_fn else None
        gtt = self._gtt_instruction(action) if product == "CNC" else None
        extras = extras or RecExtras()
        adverse_entry = zone[1] if action.side == "BUY" else zone[0]
        return Recommendation(
            rec_id=str(ULID()),
            created_at=created_at,
            valid_until=action.valid_until or self._ttl(action.style),
            kind="entry",
            instrument=action.tradingsymbol,
            side=action.side,
            style=action.style,
            product=product,
            entry_zone=zone,
            stop=stop,
            targets=targets,
            qty=qty,
            notional=notional,
            thesis=action.thesis,
            confidence=action.confidence,
            short_flag_higher_tail_risk=action.side == "SELL",
            gate=verdict,
            cost=cost,
            manual_checklist=self._entry_checklist(action, zone, product, gtt),
            strategy_id=action.strategy_id,
            proposal_id=action.proposal_id or None,
            entry_type=action.entry_type,
            reference_entry=raw_levels.entry if raw_levels else None,
            reference_stop=raw_levels.stop if raw_levels else None,
            stop_anchor=stop_anchor,
            hold_sessions=hold,
            exit_session=exit_session(self._calendar, created_at, hold) if hold else None,
            exit_kind="stop_target" if targets else "time",
            risk_inr=_money(Decimal(qty) * abs(adverse_entry - stop)),
            stop_atr_mult=extras.stop_atr_mult,
            gtt_instruction=gtt,
            evidence=list(extras.evidence),
            registered_edge_pct=None if targets else self._strategy_edge.get(action.strategy_id),
        )

    def _entry_checklist(
        self, action: EnterAction, zone: tuple[Decimal, Decimal], product: str,
        gtt: str | None = None,
    ) -> list[str]:
        """The protective-order checklist — the whole mechanism by which protection becomes the
        human's job in RECOMMEND. Never truncated, never optional."""
        stop = _dec(action.stop_price)
        entry = zone[0]
        alert = _money(stop + (entry - stop) / 2)
        items = [
            f"enter {action.side} {action.tradingsymbol} ({product}) in {zone[0]}-{zone[1]} "
            f"({action.entry_type.lower()})",
        ]
        if product == "MIS":
            items.append(f"after entry fills, place SL-M at {stop}")
        else:
            items.append(gtt or self._gtt_instruction(action))
        items.append(f"set alert at {alert}")
        if product == "MIS":
            window = self._window(self._clock.today())
            end = window[1].strftime("%H:%M") if window else "the trade-window end"
            items.append(f"square off by {end}; 15:10 is only a session backstop")
        return items

    def _gtt_instruction(self, action: EnterAction) -> str:
        """The CNC protective-GTT checklist line. CNC is long-only, so the stop leg is a
        sell below the entry."""
        stop = _dec(action.stop_price)
        offset = _dec(self._recommend.gtt_limit_offset_pct) / _HUNDRED
        limit = self._round_tick(action.tradingsymbol, stop * (Decimal(1) - offset))
        why = " so a fast drop still fills" if limit < stop else ""
        if action.target_price is None:
            return (f"after entry fills, place a single-trigger GTT: sell if price falls to "
                    f"₹{stop}; limit ₹{limit}{why}")
        return (f"after entry fills, place a GTT OCO: stop ₹{stop} (limit ₹{limit}{why}) / "
                f"target ₹{_dec(action.target_price)}")

    def _one_exit(
        self, payload: EnterAction, candidate: SignalCandidate
    ) -> tuple[EnterAction, Decimal | None]:
        """Stop re-anchoring and one exit per rec: a time-exit strategy with a
        registered edge loses any analyst-set target, so the gate prices that edge. Without one
        (rsi2) the target stays: dropping it would make every entry fail the cost gate. Also returns the
        anchor the stop was re-anchored to, ``None`` when the stop was left as proposed."""
        update: dict[str, Any] = {}
        anchor = None
        if payload.strategy_id in REANCHOR_STRATEGIES and (
            moved := self._reanchored_stop(payload, candidate)
        ):
            update["stop_price"], anchor = moved
        if (
            is_time_exit(payload.strategy_id) and payload.strategy_id in self._strategy_edge
            and payload.target_price is not None
        ):
            _log.info("target_dropped", signal_id=candidate.signal_id, symbol=candidate.symbol,
                      strategy_id=payload.strategy_id, target=str(payload.target_price))
            update["target_price"] = None
        if not update:
            return payload, None
        return EnterAction.model_validate({**payload.model_dump(), **update}), anchor

    def _reanchored_stop(
        self, payload: EnterAction, candidate: SignalCandidate
    ) -> tuple[Decimal, Decimal] | None:
        """``(stop, anchor)`` with ``stop = anchor × (1 − frac)``, ``frac = 1 − raw_stop/raw_entry``;
        the anchor is the LIMIT price, else the live mark. ``None`` = leave the stop as proposed."""
        raw = candidate.raw_levels
        if raw.stop is None or raw.entry <= 0:
            return None
        frac = Decimal(1) - _dec(raw.stop) / _dec(raw.entry)
        if frac <= 0:
            return None
        if payload.entry_type == "LIMIT":
            anchor = payload.entry_price
        else:
            try:
                anchor = self._ltp_fn(candidate.symbol) if self._ltp_fn is not None else None
            except Exception as exc:  # noqa: BLE001 - a broken mark leaves the stop as proposed
                _log.warning("stop_reanchor_no_mark", symbol=candidate.symbol, error=repr(exc))
                return None
        if anchor is None or anchor <= 0:
            return None
        anchor = _dec(anchor)
        stop = self._round_tick(candidate.symbol, anchor * (Decimal(1) - frac))
        # entry_move_pct bounds the per_trade_risk shrink of a qty sized on the raw geometry.
        _log.info(
            "stop_reanchored", signal_id=candidate.signal_id, symbol=candidate.symbol,
            strategy_id=payload.strategy_id, anchor=str(anchor), raw_entry=str(raw.entry),
            raw_stop=str(raw.stop), proposed_stop=str(payload.stop_price), stop=str(stop),
            entry_move_pct=str(_money((anchor / _dec(raw.entry) - 1) * _HUNDRED)),
        )
        return stop, anchor

    async def _collect_extras(
        self, action: EnterAction, reference: Decimal, d: date
    ) -> RecExtras:
        """Card extras under ``_EXTRAS_DEADLINE_S``; a timeout keeps whatever was collected."""
        extras = RecExtras()
        try:
            await asyncio.wait_for(
                self._fill_extras(extras, action, reference, d), _EXTRAS_DEADLINE_S
            )
        except TimeoutError:
            _log.warning("rec_extras_timeout", symbol=action.tradingsymbol,
                         strategy_id=action.strategy_id, deadline_s=_EXTRAS_DEADLINE_S)
        return extras

    async def _fill_extras(
        self, extras: RecExtras, action: EnterAction, reference: Decimal, d: date
    ) -> None:
        symbol = action.tradingsymbol
        try:
            extras.evidence.append(symbol_history_line(self._conn, symbol))
        except Exception as exc:  # noqa: BLE001 - an extra never blocks the card
            _log.warning("rec_extras_failed", part="history", symbol=symbol, error=repr(exc))
        try:
            line = (evidence_line(action.strategy_id, self._reports_dir)
                    if self._reports_dir is not None else None)
            if line:
                extras.evidence.append(line)
        except Exception as exc:  # noqa: BLE001 - an extra never blocks the card
            _log.warning("rec_extras_failed", part="evidence", symbol=symbol, error=repr(exc))
        try:
            bars = await self._store.arun(
                self._store.get_bars_1d, symbol,
                d - timedelta(days=_ATR_1D_LOOKBACK_DAYS), d - timedelta(days=1),
            )
            if len(bars) >= ATR_PERIOD:
                atr = float(wilder_atr([b.high for b in bars], [b.low for b in bars],
                                       [b.close for b in bars], ATR_PERIOD).iloc[-1])
                entry = _dec(action.entry_price) if action.entry_price is not None else reference
                if np.isfinite(atr) and atr > 0:
                    extras.stop_atr_mult = (
                        abs(entry - _dec(action.stop_price)) / Decimal(str(atr))
                    ).quantize(Decimal("0.1"), rounding=ROUND_HALF_UP)
        except Exception as exc:  # noqa: BLE001 - an extra never blocks the card
            _log.warning("rec_extras_failed", part="stop_atr_mult", symbol=symbol, error=repr(exc))
        if action.strategy_id != hi52.STRATEGY_ID:
            return
        try:
            signals = load_signal_rows(self._conn, start=PROMOTION_DATE, as_of=d)
            series = await self._store.arun(
                load_series, self._store._fetchall,  # noqa: SLF001 - the store's own locked read
                [s.symbol for s in signals], start=PROMOTION_DATE, end=d,
            )
            extras.evidence.append(forward_line(*forward_progress(signals, series)))
        except Exception as exc:  # noqa: BLE001 - an extra never blocks the card
            _log.warning("rec_extras_failed", part="hi52_forward", symbol=symbol, error=repr(exc))

    # ================================================================== trigger (b): position events
    async def on_bar(self, bar: Bar) -> None:
        """Position-event trigger — stop-proximity on an open RECOMMENDED position.

        Fires whenever the engine is up, in ANY mode, and can only produce risk-reducing output: an
        exit or a stop tighten. Never window-gated — the window gates entries only.
        """
        positions = self._conn.execute(
            "SELECT * FROM positions WHERE state='OPEN' AND origin='recommended' AND symbol=?",
            (bar.symbol,),
        ).fetchall()
        if not positions:
            return
        atr = self._atr_1m(bar)
        if atr is None or atr <= 0:
            return
        now = self._clock.now()
        ltp = _dec(bar.close)
        for position in positions:
            if not self._near_stop(position, ltp, atr):
                continue
            position_id = str(position["position_id"])
            last = self._last_position_event.get(position_id)
            if last is not None and now - last < timedelta(minutes=POSITION_EVENT_DEBOUNCE_MIN):
                continue
            self._last_position_event[position_id] = now
            await self._run_position_event(position, ltp, atr)

    def _near_stop(self, position: sqlite3.Row, ltp: Decimal, atr: Decimal) -> bool:
        """Long: within 0.5×ATR ABOVE the stop. Short: mirrored (within 0.5×ATR BELOW it)."""
        stop = position["stop"]
        if stop in (None, ""):
            return False
        distance = ltp - _dec(stop)
        if str(position["side"] or "BUY").upper() == "SELL":
            distance = -distance
        return distance <= STOP_PROXIMITY_ATR_MULT * atr

    # ------------------------------------------------------------------ exit hygiene screens
    def _skip_position_event(self, position: sqlite3.Row) -> bool:
        """True when a position event on ``position`` could only repeat something already said.

        Both screens below suppress repeat noise, and NEITHER relaxes a risk rule:

        (a) **sold outside the ledger** — two consecutive broker observations showing the position is
            not held AT ALL (journal, ``require_zero=True``). The platform cannot recommend an
            exit from a position that does not exist; the owner is still told once a day by the
            reconcile's own alert and the day plan's "sold outside the ledger" block, both naming the
            ``/closed`` reply that ends it. Logged once per position per day.

            ``require_zero`` is load-bearing: the journal's wide predicate is ``held < tracked``,
            which also catches a PARTIAL exit — a position that still carries real exposure and still
            needs its stop watched — so only "the broker holds nothing" may buy silence here. (Shares
            pledged for margin count as held via Kite's ``collateral_quantity``.) The day plan keeps
            the wide reading — it TELLS the owner rather than going quiet. A position that IS gone
            is excluded from the gate's position/sector counts on the same journal
            (``GateContextBuilder``'s ``missing_holdings_fn``).

        (b) **repeat exit** — an exit recommendation for this position was already delivered today,
            is STILL LIVE, and the position row's stop and qty are unchanged since. Scoped to the day,
            so a stop that stays breached still produces exactly one exit recommendation per
            session, which keeps the position inside ``gate._exiting_symbols``'s three-calendar-day
            window and so off the position/sector counts. A moved stop or a changed quantity is NEW
            information and passes, and so does an earlier exit that has EXPIRED unactioned — see
            :meth:`_delivered_exit_today`.

        Both screens suppress the whole event, which on (b) also costs a possible ``ModifyStopAction``.
        Intended: (b) only holds while a LIVE "close the whole position at market" instruction is
        standing, and a full exit dominates a tighter stop. Once that exit expires, is dismissed, or
        stops describing the row, the adjust path is available again.
        """
        position_id = str(position["position_id"])
        today = self._clock.today()
        missing = self._sold_outside_ledger(today)
        if position_id in missing:
            if self._sold_outside_logged.get(position_id) != today:
                self._sold_outside_logged[position_id] = today
                # Logged once a day rather than silently absorbed. Going quiet costs no capacity: the
                # gate leaves this position out of the position/sector counts on the SAME journal
                # predicate (``GateContextBuilder``'s ``missing_holdings_fn``, also require_zero=True).
                _log.warning(
                    "position_event_skipped_sold_outside_ledger",
                    position_id=position_id, symbol=str(position["symbol"] or ""),
                    effect="no analyst call and no exit recommendation while the broker holds none "
                           "of it; reply /closed to settle the ledger. The gate already leaves it "
                           "out of the position/sector counts.",
                )
            return True
        repeated = self._delivered_exit_today(position, today)
        if repeated is not None:
            _log.info(
                "position_event_skipped_repeat_exit",
                position_id=position_id, symbol=str(position["symbol"] or ""),
                rec_id=repeated, stop=str(position["stop"] or ""), qty=int(position["qty"] or 0),
            )
            return True
        return False

    def _sold_outside_ledger(self, today: date) -> dict[str, MissingHolding]:
        """Journal read (:func:`positions_missing_from_holdings`), never cached for the day.

        ``require_zero=True``: on THIS path the answer buys silence on a risk-reducing output, so
        only "the broker holds nothing on every day of the run" qualifies. A partial exit or a
        pledged holding reads short but still has exposure to protect (see :meth:`_skip_position_event`).

        The hourly reconcile REWRITES today's observation (last write of the day wins), so a position
        that comes back into holdings must stop being skipped at once — a day-keyed cache would hold
        the morning's answer until midnight. A read failure degrades to "nothing is missing"
        (fail-to-zero): an unreadable journal must never silence the exit path.
        """
        try:
            return positions_missing_from_holdings(self._conn, today, require_zero=True)
        except sqlite3.Error as exc:
            _log.warning("sold_outside_ledger_read_failed", error_type=type(exc).__name__,
                         error=str(exc)[:200])
            return {}

    def _delivered_exit_today(self, position: sqlite3.Row, today: date) -> str | None:
        """``rec_id`` of today's latest exit recommendation for ``position`` if it STILL STANDS.

        "Still stands" is two conditions, and both are needed:

        1. **It still describes the position.** The delivered payload's ``stop`` and ``qty`` equal
           the position row's current ones. Those two are the whole content of an exit recommendation
           (:meth:`_manage_recommendation` copies both straight off the row), so identical values
           mean an identical message.
        2. **It is still an instruction the owner can act on.** An exit that the TTL sweep has
           already expired (``human_action='expired'``, or ``valid_until`` passed and the sweep has
           not run yet) is gone from ``/pending``; suppressing on its strength would leave an
           intraday position with a breached stop and ONE exit instruction that died
           ``TTL_INTRADAY_MIN`` minutes after it arrived. A swing/position exit is stamped to TODAY's
           session close, so this changes nothing there. ``taken``/``dismissed``/``closed`` DO
           suppress: the owner engaged with the message.

        Only the MOST RECENT exit of the day is judged: an older matching one behind a newer
        differing one is history, not a repeat.

        ``delivered_at`` is bounded as a lexicographic ISO-8601 range, the convention every other
        day-scoped read here uses; a NULL ``delivered_at`` falls outside it and is ignored.
        """
        position_id = str(position["position_id"])
        try:
            rows = self._conn.execute(
                "SELECT r.rec_id AS rec_id, r.payload AS payload, r.human_action AS human_action "
                "FROM recommendations r "
                "JOIN learning_ledger l ON l.rec_id = r.rec_id "
                "WHERE l.position_id = ? AND r.delivered_at >= ? AND r.delivered_at < ? "
                "ORDER BY r.delivered_at DESC",
                (position_id, today.isoformat(), (today + timedelta(days=1)).isoformat()),
            ).fetchall()
        except sqlite3.Error as exc:
            # Fail-to-zero, like :meth:`_sold_outside_ledger`: this read only WITHHOLDS a
            # risk-reducing output, so a database that cannot answer must not decide. Raising would
            # also abandon every remaining position on this bar (``on_bar``'s per-position loop).
            _log.warning("delivered_exit_read_failed", position_id=position_id,
                         error_type=type(exc).__name__, error=str(exc)[:200])
            return None
        now = self._clock.now()
        for row in rows:
            try:
                data = json.loads(row["payload"] or "{}")
            except (TypeError, ValueError, json.JSONDecodeError):
                continue
            if not isinstance(data, dict) or data.get("kind") != "exit":
                continue
            action = str(row["human_action"] or "") or None
            if action == "expired" or (
                action is None and recommendation_expired(data.get("valid_until"), now)
            ):
                return None                  # nothing live was said today: say it again
            try:
                same = (
                    int(data.get("qty") or 0) == int(position["qty"] or 0)
                    and _dec(data.get("stop")) == _dec(position["stop"])
                )
            except (TypeError, ValueError, ArithmeticError):
                # An unparseable payload cannot prove the message would be identical, so it does not
                # suppress one. This screen fails OPEN: the exit recommendation is the safe side.
                return None
            return str(row["rec_id"]) if same else None
        return None

    async def _run_position_event(self, position: sqlite3.Row, ltp: Decimal, atr: Decimal) -> None:
        # Exit hygiene: every position-event entry point converges here, so the two "tells the owner
        # nothing new" screens live here rather than in :meth:`on_bar`. Both run BEFORE the governor
        # admission so a suppressed event costs neither an analyst call nor a governor decision.
        if self._skip_position_event(position):
            return
        decision = self._governor.can_invoke(INTRADAY_AGENT_ID)
        if not decision.allowed:
            _log.warning("position_event_governor_blocked", position_id=position["position_id"],
                         reason=getattr(decision, "reason", None))
            return
        row = {k: position[k] for k in position.keys()}
        detail = f"LTP {ltp} is within {STOP_PROXIMITY_ATR_MULT} x ATR(14,1m)={atr} of the stop."
        actx = self._assembler.for_position_event(
            row, "stop_proximity", detail, ltp_line=f"last price: {ltp}"
        )
        proposal_id = str(ULID())
        style = str(position["style"] or "intraday")
        # A position event may only yield an exit or a stop tighten, so this stamp keeps TODAY's
        # close: the one-exit-per-session cadence needs yesterday's to be dead.
        valid_until = self._ttl(style)
        result = await self._harness.run_single_shot(
            self._agent_def(),
            actx,
            lambda raw: parse_intraday(
                raw, proposal_id=proposal_id, agent_id=INTRADAY_AGENT_ID,
                valid_until=valid_until, inputs_digest=actx.inputs_digest,
            ),
            json_schema=intraday_guidance_json_schema(),
        )
        if not result.ok:
            await self._alert_agent_failed("position_event", result)
            return
        self._note_agent_ok()
        action = result.payload
        if isinstance(action, NoActionOutput):
            return
        if not isinstance(action, (ExitAction, ModifyStopAction)):
            # a position event may only yield risk-reducing output. The prompt says so; this is
            # the structural half of the same rule.
            _log.warning("position_event_illegal_action", action=getattr(action, "action", None),
                         position_id=position["position_id"])
            return

        try:
            verdict, _ = await self._gate_and_persist(
                action, str(position["symbol"]), str(position["side"] or "BUY"), style,
                self._clock.today(),
            )
        except GateContextTimeout as exc:
            # Unguarded this would propagate out of on_bar, i.e. out of the tick handler for every
            # symbol, not just this position.
            await self._handle_gate_context_timeout_position(position, action, exc)
            return
        if verdict.verdict == "owner_approval_required":
            await self._request_owner_approval(action, verdict, symbol=str(position["symbol"]))
            return
        if verdict.verdict not in ("approve", "shrink"):
            return
        rec = self._manage_recommendation(action, verdict, position, ltp)
        self._book.deliver(rec, ledger_fields=self._manage_ledger_fields(action, verdict, position))
        await self._send(catalog.recommendation_message(rec, ltp=ltp))

    # ================================================================== time exit
    async def time_exit_check(self, d: date) -> int:
        """Time exit: an exit recommendation for every OPEN recommended position whose exit
        session is on or before ``d``, re-issued each session until ``/closed``.

        DETERMINISTIC: the harness is never called. Returns how many exit recommendations were
        issued. A ``GateContextTimeout`` on one position is alerted and the loop continues; it is
        re-raised after the loop so the job records a failure and the catch-up retries.

        A position the broker holds NONE of on every observed day (:meth:`_sold_outside_ledger`) is
        skipped: an exit order for shares that are gone is not a recommendation. A
        partial holding still exits, and the owner's ``/closed`` is what ends the skip.
        """
        table = self._limits.load()
        caps = {
            "swing": int(table.limits.max_holding.swing_trading_days),
            "position": int(table.limits.max_holding.position_trading_days),
        }
        rows = self._conn.execute(
            "SELECT * FROM positions WHERE state='OPEN' AND origin='recommended'"
        ).fetchall()
        sold_outside = self._sold_outside_ledger(d)
        issued = 0
        timed_out: GateContextTimeout | None = None
        for position in rows:
            style = str(position["style"] or "")
            cap = caps.get(style)
            if cap is None:                       # intraday is squared off by the window, not by age
                continue
            due = self._exit_due(position, style, cap)
            if due is None or due > d:
                continue
            position_id = str(position["position_id"])
            if position_id in sold_outside:
                if self._time_exit_skip_logged.get(position_id) != d:
                    self._time_exit_skip_logged[position_id] = d
                    _log.warning("time_exit_skipped_sold_outside_ledger", position_id=position_id,
                                 symbol=str(position["symbol"] or ""), exit_session=due.isoformat())
                continue
            action = ExitAction(
                action="exit",
                proposal_id=str(ULID()),
                agent_id=PLATFORM_AGENT_ID,
                thesis=TIME_STOP_THESIS,
                confidence=1.0,
                valid_until=max(self._ttl(style), self._clock.now() + timedelta(minutes=TTL_INTRADAY_MIN)),
                inputs_digest="",
                position_id=str(position["position_id"]),
                exit_type="MARKET",
                reason="time_stop",
            )
            try:
                verdict, _ = await self._gate_and_persist(
                    action, str(position["symbol"]), str(position["side"] or "BUY"), style, d
                )
            except GateContextTimeout as exc:
                # One stalled position may not take the check down with it: the due positions
                # BEHIND it still have to be looked at. Re-raised after the loop.
                await self._handle_gate_context_timeout_position(position, action, exc)
                timed_out = timed_out or exc
                continue
            if verdict.verdict not in ("approve", "shrink"):
                continue
            rec = self._manage_recommendation(
                action, verdict, position, None, checklist=[self._time_exit_line(position, due, d)]
            )
            self._book.deliver(
                rec, ledger_fields=self._manage_ledger_fields(action, verdict, position)
            )
            await self._send(catalog.recommendation_message(rec))
            issued += 1
            _log.warning("time_exit_recommended", position_id=position["position_id"],
                         style=style, exit_session=due.isoformat(), rec_id=rec.rec_id)
        if timed_out is not None:
            raise timed_out
        return issued

    def _exit_due(self, position: sqlite3.Row, style: str, cap: int) -> date | None:
        """The position's exit session: the entry rec's payload ``exit_session``, else the hold
        counted from the rec's delivery (never from ``opened_at``); with no entry rec, the first
        session past the style ``cap`` counted from ``opened_at``. ``None`` = not computable yet.
        Under the live calendar (include_special=False) special sessions (muhurat) are closed days,
        so they are not counted as held sessions."""
        entry = next(
            (row for row in self._conn.execute(
                "SELECT r.payload, r.delivered_at, l.strategy_id FROM learning_ledger l "
                "JOIN recommendations r ON r.rec_id = l.rec_id "
                "WHERE l.position_id = ? AND r.delivered_at IS NOT NULL ORDER BY r.delivered_at",
                (position["position_id"],),
            ) if RecommendationBook._kind(row) == "entry"),
            None,
        )
        if entry is not None:
            data = RecommendationBook._rec_payload(entry)
            try:
                return date.fromisoformat(str(data["exit_session"]))
            except (KeyError, ValueError):
                pass
            strategy_id = str(entry["strategy_id"] or data.get("strategy_id") or "")
            hold = self._hold_fn(strategy_id, style) if self._hold_fn else None
            if hold is not None:
                try:
                    delivered = datetime.fromisoformat(str(entry["delivered_at"]))
                    return exit_session(self._calendar, delivered, hold)
                except ValueError:
                    pass
        try:
            opened = datetime.fromisoformat(str(position["opened_at"])).date()
            return self._calendar.add_sessions(opened + timedelta(days=1), cap)
        except ValueError:
            return None

    def _time_exit_line(self, position: sqlite3.Row, due: date, d: date) -> str:
        now = self._clock.now()
        session = self._calendar.session(d)
        before_close = session is not None and now < session.close
        head = f"{position['symbol']} x{int(position['qty'] or 0)} — "
        if due == d and before_close:
            return head + f"sell before the close on {due.isoformat()}"
        inside = before_close and session is not None and session.open <= now
        return head + f"overdue since {due.isoformat()}: sell {'now' if inside else 'at the next open'}"

    # ================================================================== shared machinery
    async def _gate_and_persist(
        self, action: Any, symbol: str, side: str, style: str, d: date
    ) -> tuple[GateVerdict, Any]:
        """Persist the proposal, build the gate context, evaluate, persist the verdict.

        Both rows exist whatever the verdict — a rejection IS the audit trail. The context is
        returned alongside so a caller can price off the SAME facts the gate judged on. Publishing
        the verdict on the bus is the caller's wiring; this class holds no bus handle.

        The context build — the ONLY store-touching await on this path — carries a deadline.
        Nothing else here is wrapped: :meth:`_persist_proposal`/:meth:`_persist_verdict` are local
        SQLite writes and ``self._gate.evaluate`` is pure CPU, so a stall can only ever be the build.
        Blowing it raises :class:`GateContextTimeout` and leaves the proposal row orphaned on
        purpose (see that class); the caller decides what the trigger path does about it.
        """
        self._persist_proposal(action)
        return await self._evaluate_and_persist_verdict(
            action, self._ctx_builder, False, symbol, side, style, d
        )

    async def _evaluate_and_persist_verdict(
        self, action: Any, ctx_builder: Any, is_paper: bool, symbol: str, side: str, style: str,
        d: date,
    ) -> tuple[GateVerdict, Any]:
        """Build ``ctx_builder``'s context under the gate-context deadline, evaluate, persist the verdict in
        the builder's scope. The proposal row must already exist."""
        try:
            gate_ctx = await asyncio.wait_for(
                ctx_builder.build(symbol, side, style, d), _GATE_CONTEXT_DEADLINE_S
            )
        except TimeoutError as exc:      # 3.12: asyncio.TimeoutError IS the builtin TimeoutError
            raise GateContextTimeout(symbol, str(action.proposal_id)) from exc
        verdict = self._gate.evaluate(action, gate_ctx)
        self._persist_verdict(verdict, is_paper)
        return verdict, gate_ctx

    def _persist_proposal(self, action: Any) -> None:
        self._conn.execute(
            "INSERT INTO proposals (proposal_id, agent_id, action, payload, inputs_digest, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (
                action.proposal_id, action.agent_id, action.action,
                _json_payload(action.model_dump(mode="json")), action.inputs_digest,
                self._clock.now().isoformat(),
            ),
        )

    def _persist_verdict(self, verdict: GateVerdict, is_paper: bool) -> None:
        self._conn.execute(
            "INSERT INTO verdicts (verdict_id, proposal_id, verdict, payload, evaluated_at, is_paper) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (
                verdict.verdict_id, verdict.proposal_id, verdict.verdict,
                _json_payload(verdict.model_dump(mode="json")), verdict.evaluated_at.isoformat(),
                int(is_paper),
            ),
        )

    async def _request_owner_approval(
        self, action: Any, verdict: GateVerdict, *, symbol: str | None = None
    ) -> None:
        """Route a gate ``owner_approval_required`` verdict to the owner. The row is the
        pending decision; the message is the prompt. Nothing is applied until ``/approve``.

        ``symbol`` is the caller's: an ``ExitAction``/``ModifyStopAction`` carries only a
        ``position_id``, and a bare ULID is not a thing an owner can approve. Persisted in the row
        as well as printed, so an approval read back later still names the instrument.
        """
        approval_id = str(ULID())
        payload = {
            "action": action.action,
            "proposal_id": action.proposal_id,
            "verdict_id": verdict.verdict_id,
            "reasons": "; ".join(verdict.reasons) or "gate routed this to the owner",
        }
        named = getattr(action, "tradingsymbol", None)
        if named is None and symbol is not None:
            payload["symbol"] = symbol        # above the ids below: what, then which row
            named = symbol
        for field in ("tradingsymbol", "position_id", "new_stop", "new_target", "order_id"):
            value = getattr(action, field, None)
            if value is not None:
                payload[field] = str(value)
        self._conn.execute(
            "INSERT INTO owner_approvals (approval_id, kind, payload, status, requested_at) "
            "VALUES (?, ?, ?, 'pending', ?)",
            (
                approval_id, _APPROVAL_KIND.get(action.action, action.action),
                _json_payload(payload), self._clock.now().isoformat(),
            ),
        )
        _log.warning("owner_approval_requested", approval_id=approval_id, action=action.action,
                     proposal_id=action.proposal_id)
        await self._send(CatalogMessage(
            kind=MessageKind.LIMIT_BREACH,
            title=f"Owner approval required: {action.action}" + (f" {named}" if named else ""),
            body="\n".join([
                f"approval {approval_id} is pending — reply /approve {approval_id} or "
                f"/reject {approval_id}.",
                *(f"  {k}: {v}" for k, v in payload.items()),
            ]),
            severity="warning",
            data={"approval_id": approval_id, "action": action.action,
                  "proposal_id": action.proposal_id, "verdict_id": verdict.verdict_id},
        ))

    def _manage_recommendation(
        self, action: Any, verdict: GateVerdict, position: sqlite3.Row, ltp: Decimal | None,
        *, checklist: list[str] | None = None,
    ) -> Recommendation:
        """Build an ``exit``/``adjust`` recommendation for an already-open recommended position.

        These carry a position reference + urgency rather than entry fields, but the payload
        model is one shape — so the entry zone degenerates to the reference price and the size is the
        position's own, never a new one.

        ``side`` on an ``exit`` is the side of the order the OWNER must place (the closing side), not
        the position's — the message says "SELL RELIANCE" for a long being closed, which is what the
        human types into their terminal. An ``adjust`` places no new order, so it keeps the position's
        side. Closing never adds tail risk, so the short flag is off on an exit.
        """
        adjust = isinstance(action, ModifyStopAction)
        reference = _dec(ltp if ltp is not None else position["avg_entry"])
        qty = int(position["qty"] or 0)
        style = str(position["style"] or "swing")
        product = str(position["product"] or _product_of(style))
        held_side = str(position["side"] or "BUY").upper()
        rec_side = held_side if adjust else ("SELL" if held_side == "BUY" else "BUY")
        stop = _dec(action.new_stop) if adjust else _dec(position["stop"])
        notional = _money(Decimal(qty) * reference)
        checklist = checklist or (
            [f"move the stop on {position['symbol']} to {stop} (modify the existing SL-M/GTT)"]
            if adjust
            else [f"exit at market: close {position['symbol']} x{qty} now"]
        )
        return Recommendation(
            rec_id=str(ULID()),
            created_at=self._clock.now(),
            valid_until=action.valid_until or self._ttl(style),
            kind="adjust" if adjust else "exit",
            instrument=str(position["symbol"]),
            side=rec_side,                                  # type: ignore[arg-type]
            style=style,                                    # type: ignore[arg-type]
            product=product,                                # type: ignore[arg-type]
            entry_zone=(reference, reference),
            stop=stop,
            targets=[],
            qty=qty,
            notional=notional,
            thesis=action.thesis,
            confidence=action.confidence,
            short_flag_higher_tail_risk=adjust and held_side == "SELL",
            gate=verdict,
            cost=verdict.cost or self._book.cost_model.round_trip(notional or reference, product),
            manual_checklist=checklist,
        )

    def _manage_ledger_fields(
        self, action: Any, verdict: GateVerdict, position: sqlite3.Row
    ) -> dict[str, Any]:
        return {
            "position_id": str(position["position_id"]),
            "thesis": action.thesis,
            "confidence": action.confidence,
            "agent_id": action.agent_id,
            "proposal_id": action.proposal_id,
            "verdict_id": verdict.verdict_id,
            "baseline_signal": 0,
            "llm_filter_decision": action.action,
        }

    # ------------------------------------------------------------------ deterministic pre-computes
    def _risk_budget_inr(self, candidate: SignalCandidate) -> Decimal:
        """₹ risk budget for one entry of ``candidate`` under ``per_trade_risk`` — the numerator
        :meth:`_max_qty_by_risk` divides by the stop distance. Split out so
        ``candidate_unsizeable_qty_zero`` can log WHY a candidate was unsizeable."""
        table = self._limits.load()
        ptr = table.limits.per_trade_risk
        pct = _dec(ptr.intraday_pct if candidate.style == "intraday" else ptr.swing_position_pct)
        return pct / _HUNDRED * self._exposure.equity()

    def _max_qty_by_risk(self, candidate: SignalCandidate) -> int:
        """``equity × per_trade_risk% / unit_risk``, floored at an int ≥ 0 — the GATE's formula.

        ``unit_risk`` = |entry − stop|, ×``overnight_gap_mult`` for swing/position exactly as the
        ``per_trade_risk`` rule charges it. This number is quoted to the analyst, and prompt
        rule 7 binds proposals to it — so it must MIRROR the gate, not approximate it: omitting the
        gap multiplier made every swing proposal arrive oversized and rejected on ``per_trade_risk``.
        A candidate with no stop level is unpriceable for risk ⇒ 0, never "unbounded"; a swing whose
        gap-charged unit risk exceeds the whole budget reads 0 here and takes the unsizeable path
        before spending an analyst call.
        """
        stop = candidate.raw_levels.stop
        if stop is None:
            return 0
        budget = self._risk_budget_inr(candidate)
        unit = abs(_dec(candidate.raw_levels.entry) - _dec(stop))
        if candidate.style != "intraday":
            unit *= _dec(self._limits.load().limits.per_trade_risk.overnight_gap_mult)
        return _floor_div(budget, unit)

    def _entry_band_pct(self, product: str) -> float:
        band = self._limits.load().limits.entry_sanity_band
        return band.mis_pct if product == "MIS" else band.cnc_pct

    def _ttl(self, style: str) -> datetime:
        """Platform-stamped ``valid_until`` (the model never emits a time).

        Intraday: ``now + TTL_INTRADAY_MIN``. Swing/position, including entries: TODAY's session
        close (owner directive: an undecided recommendation closes automatically at the end of the
        day and frees the gate's pending count that same evening, so untaken entry recs cannot hold
        the CNC ``max_open_positions`` cap across sessions).

        Never mints an ALREADY-DEAD TTL: on a day whose own session has already closed it falls back
        to ``now + TTL_INTRADAY_MIN`` instead, because the gate fails a proposal closed on
        ``valid_until <= now`` (``_rule_proposal_stale``) and a dead stamp would silently drop a
        risk-reducing exit the owner should have seen.
        """
        now = self._clock.now()
        if style == "intraday":
            return now + timedelta(minutes=TTL_INTRADAY_MIN)
        session = self._calendar.session(self._clock.today())
        if session is None or session.close <= now:
            return now + timedelta(minutes=TTL_INTRADAY_MIN)
        return session.close

    def _window(self, d: date) -> tuple[datetime, datetime] | None:
        try:
            return self._calendar.trade_window(d)
        except ValueError:               # not a trading day ⇒ no window ⇒ no entry-seeking calls
            return None

    # ------------------------------------------------------------------ hot-path read hygiene
    def _atr_1m(self, bar: Bar) -> Decimal | None:
        """ATR(14,1m) for ``bar``'s symbol, CARRIED FORWARD instead of re-read per bar.

        The state machine, in the order the branches are tested:

        * **no state / new bar day / the bar's minute is more than one minute past the last one
          seen ⇒ SEED (or RESEED) from the store.** The gap branch is load-bearing: a missed bar
          means the carried ATR is missing a true range, and continuing the recursion would bake
          that hole in for the rest of the session. A bar day change reseeds for the same reason
          (09:15 is not the minute after 15:29).
        * **exactly one minute past ⇒ advance the recursion** (:meth:`_advance_atr`).
        * **the same minute, or older ⇒ return the carried value untouched.** A re-published or
          replayed bar is already folded in; folding it again would double-count its true range.
        * **still behind after a seed** (the store lags the bus by more than a minute) ⇒ the
          store's own value is returned and the next bar reseeds. The floor is "one read per bar",
          never a stale carry.

        Returns None when the store cannot supply ``ATR_PERIOD`` bars; :meth:`on_bar` treats it as
        "no reading, no event".
        """
        started = perf_counter()
        self._roll_hot_path_day(self._clock.today())
        state = self._atr_state.get(bar.symbol)
        if (
            state is None
            or state.day != bar.ts_minute.date()
            or bar.ts_minute - state.last_ts > _BAR_STEP
        ):
            seeded = True
            state = self._seed_atr(bar, gap=state is not None)
            if state is None:
                return None
        else:
            seeded = False
        if bar.ts_minute - state.last_ts == _BAR_STEP:
            state = self._advance_atr(state, bar)
        self._atr_state[bar.symbol] = state
        if not seeded:
            self._hot_counts["atr_incremental"] += 1
            self._hot_ms["atr_incremental_ms"] += (perf_counter() - started) * 1000.0
        else:
            self._hot_ms["atr_store_read_ms"] += (perf_counter() - started) * 1000.0
        return _dec(state.atr)

    def _seed_atr(self, bar: Bar, *, gap: bool) -> _AtrState | None:
        """Anchor the recursion on a store read: ATR(14,1m) over the ``ATR_BAR_TAIL`` bars ending
        at ``bar``'s minute, plus the H/L/C the next true range is measured against.

        The window is anchored on ``bar.ts_minute`` rather than on ``Clock.now()`` so the seed
        describes the bar's own neighbourhood and a replay produces the same number as the live
        session did.
        """
        self._hot_counts["atr_store_reads"] += 1
        if gap:
            self._hot_counts["atr_gap_reseeds"] += 1
        try:
            bars = self._store.get_bars_1m(
                bar.symbol,
                bar.ts_minute - timedelta(minutes=ATR_BAR_TAIL),
                bar.ts_minute + _BAR_STEP,
            )
        except Exception:                # noqa: BLE001 - a missing store read never breaks a bar tick
            _log.exception("atr_read_failed", symbol=bar.symbol)
            return None
        tail = list(bars)[-ATR_BAR_TAIL:]
        if len(tail) < ATR_PERIOD:
            return None
        series = wilder_atr(
            [b.high for b in tail], [b.low for b in tail], [b.close for b in tail], ATR_PERIOD
        )
        last = series.iloc[-1] if len(series) else None
        if last is None or last != last:      # NaN check
            return None
        anchor = tail[-1]
        if gap:
            _log.info("atr_reseeded_on_gap", symbol=bar.symbol, bar_ts=bar.ts_minute.isoformat(),
                      anchor_ts=anchor.ts_minute.isoformat(), bars=len(tail))
        return _AtrState(
            day=bar.ts_minute.date(), last_ts=anchor.ts_minute, last_high=float(anchor.high),
            last_low=float(anchor.low), last_close=float(anchor.close), atr=float(last),
        )

    @staticmethod
    def _advance_atr(state: _AtrState, bar: Bar) -> _AtrState:
        """One Wilder step: ``atr = (atr*(n−1) + TR) / n`` — the same expression, on the same
        ``float64`` values, that :func:`wilder_atr` runs internally, so carrying the state forward
        and re-reading the whole window from the anchor produce the identical number.

        ``indicators._true_range`` is module-private by convention and called here ON PURPOSE: the
        live incremental step and the vectorized backtest path must share ONE definition of true
        range, not a second local copy of ``max(H−L, |H−C₋₁|, |L−C₋₁|)``.
        """
        tr = float(_true_range(
            np.array([state.last_high, float(bar.high)]),
            np.array([state.last_low, float(bar.low)]),
            np.array([state.last_close, float(bar.close)]),
        )[1])
        return _AtrState(
            day=state.day, last_ts=bar.ts_minute, last_high=float(bar.high),
            last_low=float(bar.low), last_close=float(bar.close),
            atr=(state.atr * (ATR_PERIOD - 1) + tr) / ATR_PERIOD,
        )

    def _sector_of(self, d: date) -> dict[str, str] | None:
        """``symbol -> sector`` for ``d``, read from the store ONCE per trading date.

        The sector map is a WEEKLY snapshot that cannot move inside a session. Two things are
        deliberately NOT cached, because that would freeze a transient into the rest of the day: a
        read that raised (returns None ⇒ "unavailable", and the next candidate retries), and an EMPTY
        snapshot (the weekly job may still land today).
        """
        self._roll_hot_path_day(self._clock.today())
        if self._sector_cache is not None and self._sector_cache_day == d:
            self._hot_counts["sector_cache_hits"] += 1
            return self._sector_cache
        self._hot_counts["sector_store_reads"] += 1
        started = perf_counter()
        try:
            rows = self._store.get_sector_map(as_of=d)
        except Exception:                # noqa: BLE001 - a thinner context, never a failed call
            _log.warning("sector_map_unavailable", d=d.isoformat())
            return None
        finally:
            self._hot_ms["sector_store_read_ms"] += (perf_counter() - started) * 1000.0
        mapping = {
            str(r.get("symbol")): str(r.get("sector")) for r in rows if r.get("symbol")
        }
        if mapping:
            self._sector_cache, self._sector_cache_day = mapping, d
        return mapping

    def _roll_hot_path_day(self, d: date) -> None:
        """Flush the day's hot-path counters and drop every per-day cache on a date change.

        Clearing :attr:`_atr_state` here is what bounds it: a symbol that stops carrying a position
        never lingers past the session, and every symbol reseeds exactly once on the new day.
        """
        if self._hot_path_day == d:
            return
        if self._hot_path_day is not None:
            self.log_hot_path_stats("day_roll")
        self._hot_path_day = d
        self._atr_state.clear()
        self._sector_cache = None
        self._sector_cache_day = None
        self._hot_counts = dict.fromkeys(_HOT_PATH_COUNTERS, 0)
        self._hot_ms = dict.fromkeys(_HOT_PATH_TIMERS, 0.0)

    def hot_path_stats(self) -> dict[str, Any]:
        """The current session's hot-path read ledger — reads avoided vs reads performed."""
        counts = dict(self._hot_counts)
        return {
            "d": self._hot_path_day.isoformat() if self._hot_path_day else None,
            **counts,
            **{k: round(v, 3) for k, v in self._hot_ms.items()},
            "store_reads_avoided": counts["atr_incremental"] + counts["sector_cache_hits"],
            "store_reads_performed": counts["atr_store_reads"] + counts["sector_store_reads"],
        }

    def log_hot_path_stats(self, reason: str = "session") -> None:
        """The one structured line quantifying the hot-path carry in the live log."""
        _log.info("hot_path_read_hygiene", reason=reason, **self.hot_path_stats())

    # ------------------------------------------------------------------ context lines (never raise)
    def _headroom_lines(self, d: date) -> list[str]:
        table = self._limits.load()
        lim = table.limits
        counts = self._exposure.open_position_counts()
        return [
            f"open positions {counts.total}/{lim.max_open_positions.total} "
            f"(MIS {counts.mis}/{lim.max_open_positions.max_mis}, "
            f"CNC {counts.cnc}/{lim.max_open_positions.max_cnc})",
            f"entry recommendations issued today {self._entry_recs_today(d)}/"
            f"{lim.max_new_trades_day.count}",
            f"deployed capital {self._exposure.deployed_capital()} of "
            f"{lim.capital_cap.max_deployed_capital_inr}",
            f"day MTM {self._exposure.day_mtm(d)} vs soft floor {lim.daily_loss_soft.day_mtm_pct}% "
            f"of {table.capital_base_inr}",
        ]

    def _positions_summary(self) -> str:
        """Open-position line for the signal context, MINUS the positions the broker no longer holds.

        A position the journal reads EMPTY on two sessions (:meth:`_sold_outside_ledger`) comes OUT
        of the counts, so the analyst does not reason about a book the owner does not hold, and is
        re-stated on a trailing line naming what the broker showed and the reply that ends it.

        Same predicate as the position-event screen, ``require_zero=True``: only "the broker held
        NOTHING on every day of the run" leaves the book. A partial exit still has exposure the
        analyst must reason about, so it stays in the counts.

        The headroom lines (:meth:`_headroom_lines`) deliberately still COUNT these positions: they
        read ``ExposureTracker.open_position_counts()`` directly, the raw count with no journal
        adjustment, while the gate's own admission math excludes them (``GateContextBuilder``'s
        ``missing_holdings_fn``, see :meth:`_skip_position_event`).

        ``sessions`` is the SHORTEST zero-run among the named positions, so the line never claims more
        evidence than every name on it has; the assembler stamps "(as of HH:MM)" onto the last line it
        is given (``intelligence/context.py``), and the one journal read happens at that instant.

        Every read here degrades to the plain counts. A context line on the analyst path may never
        raise, and an unreadable journal must not edit the book.
        """
        counts = self._exposure.open_position_counts()
        today = self._clock.today()
        gone = self._sold_outside_ledger(today)               # guarded, never cached for the day
        rows: list[sqlite3.Row] = []
        if gone:
            try:
                # The origin scope MUST match the counter being adjusted (``ExposureTracker``'s
                # platform-origin open rows): a position outside it was never in ``counts``, so
                # subtracting it would understate the book.
                rows = [
                    row for row in self._conn.execute(
                        "SELECT position_id, symbol, product FROM positions "
                        f"WHERE state='OPEN' AND {scope_sql('real', has_origin=True)}"
                    ).fetchall()
                    if str(row["position_id"]) in gone
                ]
            except sqlite3.Error as exc:
                _log.warning("positions_summary_sold_outside_read_failed",
                             error_type=type(exc).__name__, error=str(exc)[:200])
                rows = []
        total = max(counts.total - len(rows), 0)
        mis = max(counts.mis - sum(1 for r in rows if str(r["product"] or "").upper() == "MIS"), 0)
        cnc = max(counts.cnc - sum(1 for r in rows if str(r["product"] or "").upper() == "CNC"), 0)
        main = "none open" if total == 0 else f"{total} open (MIS {mis}, CNC {cnc})"
        if not rows:
            return main
        symbols = ", ".join(sorted({str(r["symbol"] or "?") for r in rows}))
        # ``gone`` is the SAME dict that selected ``rows`` above (one journal read), so every
        # row's position id is guaranteed a key here.
        sessions = [gone[str(r["position_id"])].zero_sessions for r in rows]
        return (
            f"{main}\nsold outside the ledger (broker holds 0 on "
            f"{min(sessions)} sessions, awaiting /closed): {symbols}"
        )

    def _sector_exposure_line(self, d: date) -> str:
        sector_of = self._sector_of(d)
        if sector_of is None:            # read failed ⇒ a thinner context, never a failed call
            return "unavailable"
        counts = self._exposure.per_sector_open(sector_of)
        if not counts:
            return "no open positions"
        return ", ".join(f"{sector} {n}" for sector, n in sorted(counts.items()))

    def _cost_line(self, entry_ref: Decimal, qty: int, product: str) -> str:
        notional = _dec(qty) * entry_ref if qty > 0 else entry_ref
        try:
            cost = self._book.cost_model.round_trip(notional, product)
        except (ValueError, ArithmeticError):
            return "unavailable"
        return (
            f"round trip on notional {cost.notional} ({product}): total {cost.total_cost}, "
            f"breakeven {cost.breakeven_pct}%"
        )

    def _entry_recs_today(self, d: date) -> int:
        """Entry recommendations ISSUED today — the RECOMMEND-mode ``max_new_trades_day`` basis: in
        RECOMMEND no position is opened, so counting positions would never bind."""
        rows = self._conn.execute("SELECT payload FROM recommendations").fetchall()
        n = 0
        for row in rows:
            try:
                data = json.loads(row["payload"] or "{}")
            except (TypeError, ValueError, json.JSONDecodeError):
                continue
            if data.get("kind") == "entry" and str(data.get("created_at") or "")[:10] == d.isoformat():
                n += 1
        return n

    # ------------------------------------------------------------------ notification seam
    def _agent_def(self) -> Any:
        try:
            return self._agent_defs[INTRADAY_AGENT_ID]
        except KeyError as exc:          # a missing def is a wiring bug, not a runtime condition
            raise KeyError(f"agent def '{INTRADAY_AGENT_ID}' is not loaded (agents.yaml)") from exc

    def _note_agent_ok(self) -> None:
        """A successful analyst call closes its alert episodes — the next failure is news
        again, not a repeat of the last outage."""
        self._agent_alert_episodes.reset(INTRADAY_AGENT_ID)

    async def _alert_agent_failed(self, trigger: str, result: Any) -> None:
        """Every Tier-1 failure resolves to no-proposal + an owner alert. Best-effort — a failed
        send must not turn a no-proposal into an exception on the trigger path.

        The episode tracker throttles the ALERT, never the failure handling: the
        ``intraday_agent_failed`` WARNING below still fires on every occurrence, and the caller still
        returns no-proposal. Repeats of the same ``(agent, reason)`` inside
        :data:`~engine.notify.episodes.REPEAT_AFTER` are logged as suppressed. The key deliberately
        excludes ``trigger``: a position event and a candidate failing on the same reason are one
        outage.
        """
        _log.warning("intraday_agent_failed", trigger=trigger, reason=getattr(result, "reason", None),
                     detail=getattr(result, "detail", None), call_id=getattr(result, "call_id", None))
        reason = str(getattr(result, "reason", "unknown"))
        if reason in ACCOUNT_FAIL_REASONS:
            _log.info("intraday_agent_alert_throttled", trigger=trigger, reason=reason,
                      note="account-wide failure: the harness alerts it once for every agent")
            return
        if not self._agent_alert_episodes.should_alert(
            (INTRADAY_AGENT_ID, reason), self._clock.now()
        ):
            _log.info("intraday_agent_alert_throttled", trigger=trigger, reason=reason,
                      note="repeat inside the episode window; logged, not alerted")
            return
        await self._send(CatalogMessage(
            kind=MessageKind.LIMIT_BREACH,
            title="Intraday analyst unavailable",
            body=(
                f"{trigger}: the analyst call failed ({getattr(result, 'reason', 'unknown')}). "
                "No proposal was produced and nothing was recommended. Deterministic exits, "
                "stops and square-offs are unaffected."
            ),
            severity="warning",
            data={
                "rule_id": "agent_failed", "trigger": trigger,
                "value": str(getattr(result, "reason", "unknown")),
                "limit": "a Tier-1 failure resolves to no-proposal + alert",
                "call_id": str(getattr(result, "call_id", "")),
            },
        ))

    async def _send(self, message: CatalogMessage) -> None:
        if self._notify is None:
            return
        try:
            await self._notify(message)
        except Exception:                # noqa: BLE001 - a notification never breaks the pipeline
            _log.exception("pipeline_notify_failed", kind=str(message.kind))


__all__ = [
    "ATR_BAR_TAIL",
    "ATR_PERIOD",
    "BAND_SKIP_STRATEGIES",
    "FORWARD_DRAIN_MODES",
    "FORWARD_MODES",
    "FORWARD_PACING_MIN",
    "INTRADAY_AGENT_ID",
    "MAX_PENDING_FORWARDS",
    "MIN_RANK_POPULATION",
    "QUANTILE_BANDS",
    "PLATFORM_AGENT_ID",
    "POSITION_EVENT_DEBOUNCE_MIN",
    "STOP_PROXIMITY_ATR_MULT",
    "TIME_STOP_THESIS",
    "TTL_INTRADAY_MIN",
    "GateContextTimeout",
    "NotifyFn",
    "RecommendationBook",
    "RecommendationPipeline",
]
