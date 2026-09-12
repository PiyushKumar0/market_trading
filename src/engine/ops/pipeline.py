"""The RECOMMEND pipeline (§3.6, §5.2, §7.1 ``max_holding``) — signal ⇒ analyst ⇒ gate ⇒ owner.

Lives in ``engine.ops`` because it is the only package allowed to import everything (§3.2.12): it
wires Tier-1 (``intelligence``), Tier-2 (``risk``), the store, the notifier and the state DB into one
flow. Nothing here places a broker order — in RECOMMEND the platform places **zero** API orders
including GTTs (B7); the ``manual_checklist`` on every recommendation is the mechanism that transfers
protective-order responsibility to the human explicitly.

Two objects:

* :class:`RecommendationBook` — persistence + human-outcome capture (§3.6). It owns the
  ``recommendations`` / ``learning_ledger`` / ``positions`` rows and implements the three coroutines
  the Telegram ``RecoBook`` seam depends on (``take``/``close``/``veto``), plus ``expire_stale``.
  The **non-fill is training signal**: a recommendation that reaches ``valid_until`` unconfirmed is
  labelled ``no_action`` in the ledger so attribution is not biased toward only the trades the owner
  happened to take (§3.6/§6.5).
* :class:`RecommendationPipeline` — the trigger handlers. ``on_signal_candidate`` (§5.2 trigger a) is
  the entry path and is **window-gated**; ``on_bar`` (trigger b) and ``check_aged_positions`` (§7.1
  ``max_holding``) are risk-reducing and therefore **never** window-gated (R3); ``heartbeat``
  (trigger c) is window-gated and can only refresh the regime note.

Conventions that are load-bearing here:

* **No LLM-originated time (§3.2).** ``valid_until`` is platform-stamped by :meth:`_ttl` from
  ``Clock``/``NSECalendar``; any value the model emits is discarded by ``parse_intraday``.
* **Fail to no-proposal (D7).** A governor block, a schema failure, a timeout or an SDK death all
  resolve to "no recommendation + owner alert" — never to a salvaged/guessed action.
* **Exits never depend on an LLM (R1).** :meth:`check_aged_positions` builds its ``ExitAction``
  deterministically in Python and never calls the harness.
* **Money is ``Decimal``, timestamps are tz-aware IST strings** in every row written here.
* **The analyst slot goes to the best pending candidate, not the earliest** (§5.2(a), WO-1
  2026-08-13). Published candidates queue; each slot the §5.6 forward cap grants is spent on the
  highest per-strategy score QUANTILE waiting (``_forward_key``), and the counter that bounds those
  slots lives in the day-slot journal so a mid-day restart resumes the day's quota instead of
  refilling it. Rollback: ``admission_mode="arrival"``.
* **A slot is spent only by something that was actually looked at** (D1, 2026-09-11). The queue
  itself is process memory and a restart still drops it — but every exit that leaves a candidate
  unjudged now hands its §3.2.5 admission slot back: a restart (``_rearm_forward_orphans``), the
  window closing under a queued candidate (``_flush_forwards_at_window_close``), and a level-pinned
  candidate whose level walked outside the entry-sanity band (``_outside_entry_band``). An analyst
  call that returned no verdict also gives back its §5.2(a) forward charge (``_refund_forward``).
* **The queue is drained on a CADENCE, not on arrival** (§5.2(a), 2026-08-14). An arriving candidate
  only ever enqueues; :meth:`RecommendationPipeline.drain_forward_queue` — pulsed from the scheduler
  — spends at most one slot every ``FORWARD_PACING_MIN`` minutes on the best pending candidate.
  Ranking can only rank what has accumulated: on the first live day under WO-1 the inline drain left
  exactly one candidate in the queue at every slot, so "forward the best pending" was satisfied
  vacuously and all 12 slots were gone by 09:20 on scores 0.14–0.66 while 0.83/1.00/1.00 arrived
  later and never got one. Rollback: ``forward_drain_mode="immediate"``.
* **The trigger paths carry state instead of re-reading it** (§3.2 hot-path invariant 7, WO-8
  2026-08-13). ATR(14,1m) is seeded from the store once per symbol per day and advanced one Wilder
  step per bar (:meth:`RecommendationPipeline._atr_1m`), reseeding only when a bar's minute is not
  contiguous with the last one seen; the weekly sector map is read once per trading date
  (:meth:`RecommendationPipeline._sector_of`). ``hot_path_read_hygiene`` logs the resulting
  reads-avoided/performed ledger once per session.
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import ROUND_FLOOR, ROUND_HALF_UP, Decimal
from time import perf_counter
from typing import Any

import numpy as np
from ulid import ULID

from engine.core.calendar import NSECalendar
from engine.core.clock import Clock
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
from engine.core.types import Bar
from engine.features.snapshots import FEATURE_SET_VERSION
from engine.intelligence.schemas import (
    NoActionOutput,
    intraday_guidance_json_schema,
    parse_intraday,
)
from engine.notify import catalog
from engine.notify.catalog import CatalogMessage, MessageKind
from engine.notify.episodes import AlertEpisodes
from engine.ops.holdings_reconcile import positions_missing_from_holdings
from engine.strategy.cost_model import CostModel
from engine.strategy.indicators import _true_range, wilder_atr
from engine.strategy.types import SignalCandidate

_log = get_logger("engine.ops.pipeline")

NotifyFn = Callable[[CatalogMessage], Awaitable[None]]

#: The §5.2 agent this pipeline drives. Its ``agents.yaml`` key, the governor's spend key and the
#: ``agent_calls.agent_id`` are all this one string.
INTRADAY_AGENT_ID = "intraday_analyst"

#: ``agent_id`` stamped on a DETERMINISTIC (no-LLM) proposal — the §7.1 ``max_holding`` time stop.
#: Distinct from the analyst id so the ledger can separate model decisions from platform decisions.
PLATFORM_AGENT_ID = "platform"

#: Entry-recommendation TTL for an intraday candidate, minutes [tunable]. Swing/position entries stay
#: valid to the session close instead (see :meth:`RecommendationPipeline._ttl`).
TTL_INTRADAY_MIN = 20

#: §5.2 trigger (b) stop-proximity: fire when the LTP is within this many ATR(14,1m) of the stop.
STOP_PROXIMITY_ATR_MULT = Decimal("0.5")

#: One position event per position per this many minutes — a tick stream sitting at 0.49×ATR would
#: otherwise bill an analyst call per bar.
POSITION_EVENT_DEBOUNCE_MIN = 60

#: ATR(14,1m) inputs: Wilder period and how many trailing 1m bars to read for it (§5.2 b).
ATR_PERIOD = 14
ATR_BAR_TAIL = 60

#: The 1m bar cadence. A bar exactly this far past the last one CONTINUES the Wilder recursion;
#: anything further ahead is a gap and reseeds from the store (WO-8 (i), §3.2 hot-path invariant 7).
_BAR_STEP = timedelta(minutes=1)

#: The per-session hot-path counters behind the ``hot_path_read_hygiene`` log line (WO-8 (iii)).
_HOT_PATH_COUNTERS = (
    "atr_store_reads",      # _atr_1m calls that had to read bars_1m (seed or gap reseed)
    "atr_incremental",      # _atr_1m calls served from carried state — the reads AVOIDED
    "atr_gap_reseeds",      # of the store reads, how many were non-contiguity reseeds
    "sector_store_reads",   # sector-map reads performed
    "sector_cache_hits",    # sector-map reads avoided
)
_HOT_PATH_TIMERS = ("atr_store_read_ms", "atr_incremental_ms", "sector_store_read_ms")

#: The deterministic time-stop thesis (§7.1 ``max_holding``). A constant, not model text: R1 says an
#: exit must survive with the LLM dead, so nothing on this path may be generated.
TIME_STOP_THESIS = (
    "Deterministic time stop: this position has exceeded the §7.1 max_holding age for its style. "
    "No model was consulted — exits and stops never depend on an LLM response (R1)."
)

#: §5.2(a) forward-selection modes (WO-1, 2026-08-13). ``ranked`` = the priority queue below;
#: ``arrival`` = the pre-WO-1 first-come-first-served order, kept ONLY as the config rollback.
FORWARD_MODES = ("ranked", "arrival")

#: How many quantile BANDS the per-strategy score distribution is cut into for forward selection.
#: Quintiles: coarse enough that two candidates of comparable standing inside their own strategies
#: are treated as equals (and ordered by fired_at), fine enough to separate a strategy's best from
#: its middling ones. A finer grid would spuriously rank a 0.71 above a 0.69 across strategies.
QUANTILE_BANDS = 5

#: Smallest score population that can RANK a candidate at all (D1 (d), 2026-09-11). The empirical
#: CDF of a ONE-element population can only return 1.0: the strategy's first candidate of the day is
#: measured against itself and takes the TOP band by arithmetic rather than by standing (live
#: 2026-09-10). A TWO-element one is barely better — the CDF can only say 0.5 or 1.0, so the second
#: candidate of the day still reaches the top band by out-scoring exactly one other observation, and
#: one comparison is not a distribution. Three is the smallest population where "top band" means the
#: candidate stood above more than one rival, so it is the floor. Below it,
#: :meth:`RecommendationPipeline._quantile_band` returns the MIDDLE band instead — which also covers
#: the EMPTY population a journal-hydrated day can present.
MIN_RANK_POPULATION = 3

#: Hard ceiling on the pending-forward queue. The §3.2.5 publication cap (settings
#: ``max_candidates_per_day`` — 48 since 2026-08-18, was 20) already bounds it below this; the
#: ceiling exists so a misconfigured publication cap cannot turn a refused-candidate pointer list
#: into unbounded process memory.
MAX_PENDING_FORWARDS = 100

#: Strategies whose proposal is a LIMIT pinned to a structural LEVEL rather than to the live price,
#: so a candidate that waits in the queue while the price walks away from that level is a guaranteed
#: ``entry_sanity_band`` gate reject (D1 (e), 2026-09-11: 6 of 15 ``brk20`` proposals died there and
#: each one had already spent an analyst slot). Only these strategies are band-screened at the
#: forward slot — a price-relative leg's entry tracks the LTP, so the same screen there would drop
#: candidates the gate would have approved.
BAND_SKIP_STRATEGIES = frozenset({"brk20"})

#: §5.2(a) forward-DRAIN trigger modes (2026-08-14). ``paced`` = the queue is drained on a fixed
#: cadence by :meth:`RecommendationPipeline.drain_forward_queue`; ``immediate`` = the pre-2026-08-14
#: behaviour in which an arriving candidate drained its own slot inline, kept ONLY as the rollback.
FORWARD_DRAIN_MODES = ("paced", "immediate")

#: Minutes between paced drains — the window over which candidates ACCUMULATE before one of them is
#: chosen. Constrained from three sides: strictly above the 60 s scheduler pulse that drives it (or
#: the cadence would just be the pulse); small against ``TTL_INTRADAY_MIN`` (20) so a queued
#: candidate gets several chances before its own levels go stale; and cap × pacing must fit the
#: trade window (12 slots at DG0 ⇒ 36 min of drains, 4 at DG1+ ⇒ 12 min). A LONGER interval buys a
#: better-populated queue at the cost of latency to the analyst — 3 min was chosen against the
#: 2026-08-14 arrival rate (66 candidates by 10:13 ⇒ ~2 per pacing interval at the open).
FORWARD_PACING_MIN = 3

#: Wall-clock ceiling on ONE gate-context build inside :meth:`RecommendationPipeline._gate_and_persist`
#: (seconds). WO-24a, THE INCIDENT (2026-08-21 09:56:40): the platform's first-ever proposal reached
#: ``_gate_and_persist``, and at that exact moment the market store stopped answering — the feature
#: snapshots, the ``warmup_refresh`` interval job and this context read all stalled together and
#: stayed stalled for 14 minutes, until a restart. The ``await`` on the builder had no deadline, so
#: the drain tick sat inside it: the proposal row was already written, no verdict was ever written,
#: no alert was ever sent and no later candidate was ever evaluated. A funnel FREEZE, not a failure.
#: 90 s is orders of magnitude above a healthy build (a handful of point reads) and below the
#: FORWARD_PACING_MIN cadence that drives the drain, so it can only fire on a real stall.
_GATE_CONTEXT_DEADLINE_S = 90.0

#: A proposal still carrying no verdict this many minutes after it was written is ORPHANED (WO-24c).
#: Comfortably above ``_GATE_CONTEXT_DEADLINE_S`` so the timeout path's own alert always lands first
#: and the sweep is the BACKSTOP (a crash between the two writes, a stall nothing else caught) rather
#: than a duplicate of it.
_ORPHAN_TTL_MIN = 10

#: Oldest orphan that still pages the owner, minutes (24 h). A permanent orphan can never be
#: resolved (no verdict is ever invented), so without a cap it re-announced on every boot and every
#: day-roll forever — the owner received the 2026-08-21 GVT&D orphan's alert three times (Fri 15:39,
#: Sat 00:04, Mon 08:11) before this cap (2026-08-24). Past it: INFO log once per process-day,
#: nightly-review counting unchanged, no owner page — an orphan older than a day is history, not news.
_ORPHAN_ALERT_MAX_AGE_MIN = 24 * 60

#: How often the orphan sweep actually reads the DB, minutes. The 60 s forward-drain tick invokes it
#: on every pulse; this throttle decides which pulses do the work (WO-24c).
_ORPHAN_SWEEP_INTERVAL_MIN = 5


class GateContextTimeout(Exception):
    """One gate-context build blew ``_GATE_CONTEXT_DEADLINE_S`` (WO-24a — see the constant).

    Carries the symbol AND the ``proposal_id`` because :meth:`RecommendationPipeline._gate_and_persist`
    has ALREADY written the proposal row by the time the build is awaited: the raise leaves a real
    orphan behind, deliberately. Fabricating a verdict to tidy it up would put a decision the gate
    never made into the audit trail (§3.4 — a verdict row is a claim that the gate judged this
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
    """Carried ATR(14,1m) for one symbol — the state that replaces a per-bar store read (WO-8 (i)).

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
    """One published-but-not-yet-evaluated candidate waiting for an analyst slot (WO-1 (ii))."""

    candidate: SignalCandidate
    fired_at: datetime          # when the PIPELINE received it (platform clock, §3.2 — never LLM)
    seq: int                    # arrival sequence: the last, always-unique deterministic tie-break
    expires_at: datetime        # the candidate's own §5.2 TTL horizon; past it the levels are stale
    front: bool = False         # WO-20d: a re-queued failed evaluation, ahead of every fresh arrival


#: ``owner_approvals.kind`` per action type (§3.4 ``owner_approval_required``).
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
    """O3: intraday ⇒ MIS, swing/position ⇒ CNC. The one place the mapping is written."""
    return "MIS" if style == "intraday" else "CNC"


# =========================================================================== the book (§3.6)
class RecommendationBook:
    """Persistence + human-outcome capture for RECOMMEND (§3.6).

    Implements the Telegram ``RecoBook`` seam: :meth:`take` / :meth:`close` / :meth:`veto` each return
    an owner-facing summary line and raise :class:`ValueError` — with a message written for the owner —
    on an unknown ``rec_id`` or an invalid state transition.

    **Where the money lands.** ``positions.realized_pnl`` is written GROSS and ``positions.costs``
    separately, because :class:`~engine.risk.exposure.ExposureTracker` computes platform equity as
    ``Σ(realized_pnl − costs)``; writing the already-net figure there would subtract costs twice from
    every §7.1 floor-ladder reading. The NET figure is not lost — ``learning_ledger`` carries
    ``gross_pnl`` and ``net_pnl`` explicitly and ``recommendations.outcome`` carries both (§6.5).

    Confirmed recommended positions count toward every §7.1 limit for free: ``ExposureTracker`` and
    ``GateContextBuilder`` already read ``origin IN ('platform','recommended')``, so writing the row
    is the whole integration.
    """

    def __init__(self, conn: sqlite3.Connection, clock: Clock, cost_model: CostModel) -> None:
        self._conn = conn
        self._clock = clock
        self._costs = cost_model
        self._ledger_cols: frozenset[str] | None = None

    @property
    def cost_model(self) -> CostModel:
        """The shared cost model — the pipeline prices exit/adjust recommendations through it."""
        return self._costs

    # ------------------------------------------------------------------ delivery
    def deliver(self, rec: Recommendation, *, ledger_fields: Mapping[str, Any]) -> None:
        """Persist a delivered recommendation + its OPEN learning-ledger row (§3.6/§6.5).

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
        """``/taken <rec_id> <qty> <price>`` — create the ``origin='recommended'`` position (§3.6).

        The platform never adopts a position as ``recommended`` without this owner confirmation.
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
        # ``expired`` → ``taken`` is legal (2026-08-26, the FIRST live /taken): the owner executed
        # the HDFCAMC entry intraday and recorded it in the evening — by then the 15:45 sweep had
        # already labelled the row expired, and both command forms refused the platform's first real
        # fill. Expiry marks "no longer actionable", not "never happened"; the owner is the
        # authority on what they executed while it was live. dismissed/closed/taken stay refused.
        data = self._rec_payload(row)
        if (data.get("kind") or "entry") != "entry":
            # 2026-07-28 review: /taken on an exit/adjust rec would OPEN a new position on the
            # closing side — a phantom short after exiting a long. Exits are reported via /closed.
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
                "UPDATE recommendations SET human_action='taken', human_fill_price=? WHERE rec_id=?",
                (str(price), rec_id),
            )
            # outcome_label/closed_at reset to NULL (2026-09-02 review): an expired→taken rec has
            # already been stamped 'no_action' by expire_stale, and close()'s completion UPDATE
            # matches `outcome_label IS NULL` only — without the reset the real trade's outcome is
            # permanently mislabeled a non-event. On a never-expired rec both are already NULL.
            self._conn.execute(
                "UPDATE learning_ledger SET entry_px=?, qty=?, position_id=?, "
                "outcome_label=NULL, closed_at=NULL WHERE rec_id=?",
                (str(price), int(qty), position_id, rec_id),
            )
        _log.warning("recommendation_taken", rec_id=rec_id, position_id=position_id, qty=qty,
                     price=str(price))
        return (
            f"recorded {data.get('side')} {data.get('instrument')} x{qty} @ {price} "
            f"(position {position_id}). The protective orders are yours to place — the platform "
            "places none in RECOMMEND (B7)."
        )

    # ------------------------------------------------------------------ owner reports the close
    async def close(self, rec_id: str, price: Decimal) -> str:
        """``/closed <rec_id> <price>`` — close the position and label the ledger outcome (§3.6/§6.5)."""
        price = _dec(price)
        if price <= 0:
            raise ValueError(f"price must be positive, got {price}")
        # Prefer the ENTRY row (has entry_px) — /closed may legitimately arrive with an EXIT rec's id
        # when the owner replies to a delivered exit recommendation (2026-07-28 review).
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
        # v1 approximation (documented): the round trip is priced on the EXIT notional rather than
        # entry+exit legs separately. Slippage between the two legs is second-order against a ~0.4%
        # round trip and this keeps one cost number reconcilable with the CostModel the gate used.
        costs = _money(self._costs.round_trip(_dec(qty) * price, product).total_cost)
        net = _money(gross - costs)
        outcome_label = "win" if net > 0 else ("loss" if net < 0 else "scratch")
        holding_minutes = self._holding_minutes(position["opened_at"], now)

        with transaction(self._conn):
            self._conn.execute(
                "UPDATE positions SET state='CLOSED', close_reason='manual_owner', closed_at=?, "
                # realized_pnl is GROSS on purpose — ExposureTracker computes equity as
                # Σ(realized_pnl − costs); writing net here would charge the costs twice (§7.1).
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
    async def veto(self, rec_id: str) -> str:
        """``/veto <rec_id>`` — the owner declines. Recorded as ``dismissed`` + a ``no_action`` label:
        a decline and a silent non-fill are different facts, both training signal (§6.5)."""
        row = self._rec_row(rec_id)
        if row["human_action"]:
            raise ValueError(
                f"recommendation {rec_id} is already '{row['human_action']}' — nothing to veto"
            )
        now = self._clock.now()
        with transaction(self._conn):
            self._conn.execute(
                "UPDATE recommendations SET human_action='dismissed' WHERE rec_id=?", (rec_id,)
            )
            self._conn.execute(
                "UPDATE learning_ledger SET outcome_label='no_action', closed_at=? WHERE rec_id=?",
                (now.isoformat(), rec_id),
            )
        _log.warning("recommendation_vetoed", rec_id=rec_id)
        return f"recommendation {rec_id} dismissed — recorded as a no_action outcome (§6.5)."

    # ------------------------------------------------------------------ TTL sweep
    def expire_stale(self, now: datetime) -> int:
        """Expire every unconfirmed recommendation past ``valid_until``; returns how many (§3.6).

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
            return 0
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
        return len(stale)

    # ------------------------------------------------------------------ internals
    def _ledger_columns(self) -> frozenset[str]:
        if self._ledger_cols is None:
            rows = self._conn.execute("PRAGMA table_info(learning_ledger)").fetchall()
            self._ledger_cols = frozenset(str(r[1]) for r in rows)
        return self._ledger_cols

    def _rec_row(self, rec_id: str) -> sqlite3.Row:
        row = self._conn.execute(
            "SELECT rec_id, payload, human_action FROM recommendations WHERE rec_id=?", (rec_id,)
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


# =========================================================================== the pipeline (§5.2)
class RecommendationPipeline:
    """Turns deterministic triggers into gate-approved recommendations (§5.2/§3.6).

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
        admission_mode: str = "ranked",
        forward_drain_mode: str = "paced",
    ) -> None:
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
        #: INFRASTRUCTURE failure (owner-directed 2026-07-29); wired to SignalPreScreen.rearm.
        self._rearm = rearm
        #: ``day -> {strategy_id: raw fires}`` (wired to ``SignalPreScreen.raw_counts``) plus the
        #: last set actually written to ``funnel_raw_counts`` and the day it belongs to. The
        #: comparison is the "changed since the last flush" test that keeps the 60 s drain tick from
        #: rewriting an unchanged row set all afternoon (2026-08-21). See :meth:`_flush_funnel_raw`.
        self._funnel_raw = funnel_raw
        #: §3.2.5 cap-displacement seam (2026-08-27), both wired to ``SignalPreScreen``:
        #: ``claim_slot`` is the atomic compare-and-set this pipeline MUST pass before spending an
        #: analyst call — ``False`` means a better candidate took this pair's admission slot while it
        #: waited in the queue, so forwarding it anyway would breach the cap by one real call.
        #: ``take_displaced`` pulls the evictions the pre-screen decided on a scan worker thread, so
        #: the queue surgery and the journal correction happen HERE, on the event loop, where
        #: ``_pending_forwards`` may actually be touched. Unwired ⇒ every claim succeeds and no
        #: eviction ever arrives, which is exactly the pre-2026-08-27 behaviour.
        self._claim_slot = claim_slot
        self._take_displaced = take_displaced
        #: (symbol, strategy_id) -> the analyst RAN and said no_action, so the pair is displaceable
        #: again (2026-09-04; wired to SignalPreScreen.decline). Not a re-arm: the slot stays spent.
        self._decline = decline
        #: symbol -> live LTP (wired to the tick cache in ``engine.ops.main``). Read ONLY by the
        #: D1 (e) band screen in :meth:`_take_forward_slot`; unwired ⇒ no screen, which is the
        #: pre-2026-09-12 behaviour. Never a gate input: the gate reads its own LTP from GateContext.
        self._ltp_fn = ltp_fn
        self._funnel_raw_day: date | None = None
        self._funnel_raw_flushed: dict[str, int] = {}
        #: position_id -> when its last §5.2(b) event fired (in-process debounce).
        self._last_position_event: dict[str, datetime] = {}
        #: WO-D2: position_id -> the trading date its "sold outside the ledger" skip was last logged
        #: on. The SKIP itself is re-decided from the §3.6 journal on every event (a position that
        #: reappears in holdings must resume producing events the same hour); only the log line is
        #: deduped, so twelve hourly ticks leave one line per position instead of twelve. In-process
        #: like every other memo here — a restart logs it once more, which is the harmless side.
        self._sold_outside_logged: dict[str, date] = {}
        #: Owner-alert cadence for "Intraday analyst unavailable", per ``(agent, reason)`` (WO-25b).
        #: One alert per failure meant one alert per heartbeat for as long as the SDK stayed down —
        #: those repeats were a large share of the 203-deep notification backlog on 2026-08-24. The
        #: D7 behaviour is unchanged (a Tier-1 failure is still no-proposal, still logged every time);
        #: only how often the owner is told the same thing changes. A successful analyst call closes
        #: the episode, so the next outage is heard immediately.
        self._agent_alert_episodes = AlertEpisodes()
        #: §5.2(a) analyst forward cap — per-day count of candidates that reached the harness (§5.6).
        #: Hydrated from the day-slot journal on every day roll (WO-1 (iv)), so a mid-day restart
        #: RESUMES the day's analyst quota instead of refilling it.
        self._forwarded_day: date | None = None
        self._forwarded_count = 0
        self._forward_mode = admission_mode
        #: The §5.2(a) priority queue: published candidates that have not been evaluated yet.
        self._pending_forwards: list[_PendingForward] = []
        self._forward_seq = 0
        #: When the queue was last drained (2026-08-14). ``None`` ⇒ the first tick drains; from then
        #: on the anchor advances every tick that passes the guard, dispatched or not — the interval
        #: IS the accumulation window, so an empty one still spends its place in the cadence.
        self._drain_mode = forward_drain_mode
        self._last_forward_drain: datetime | None = None
        #: WO-20d: signal_ids whose evaluation blew up once and were re-queued for it. Bounds the
        #: retry to ONE per candidate per day; rolled with the rest of the forward state in
        #: :meth:`_roll_forward_day`, so it can never outlive the queue it refers to.
        self._requeued_forwards: set[str] = set()
        #: D1 (d): signal_ids whose §5.2(a) forward charge has already been handed back, so ONE
        #: charge can never be undone twice (:meth:`_refund_forward`). Rolled with the day like
        #: ``_requeued_forwards``, and DISCARDED the moment :meth:`_take_forward_slot` charges the
        #: same candidate again — the memo is "this charge is already refunded", not "this candidate
        #: has had its one refund of the day", so a re-drained front entry can still be refunded for
        #: the second charge it makes.
        self._refunded_forwards: set[str] = set()
        #: D1 (b): which window CLOSE has already been flushed, keyed on the window's END rather
        #: than on the day. ``NSECalendar.trade_window`` re-reads the owner's sticky
        #: ``trade_window_state`` row on every call (§3.2.7 runtime setter), so a window the owner
        #: moves or re-opens after a close (live 2026-08-18: 10:00–10:30 widened to 10:10–15:30
        #: at 09:57) is a NEW close that has to flush again — a day-keyed latch would disable the
        #: flush for the rest of that day and strand the second window's queue silently.
        #: ``_window_close_flushed_day`` is the sibling latch for a day with NO window at all, where
        #: there is no end timestamp to key on. Both are stamped rather than bools because the flush
        #: fires on the path that RETURNS BEFORE :meth:`_roll_forward_day`.
        self._window_close_flushed_at: datetime | None = None
        self._window_close_flushed_day: date | None = None
        #: WO-24c: proposal_ids the orphan sweep has already alerted on, plus the day that set
        #: belongs to and when the sweep last read the DB. Rolled daily by :meth:`_roll_orphan_day`
        #: exactly like ``_requeued_forwards`` — a per-PROCESS memo, so a restart re-alerts a
        #: standing orphan once (the right side to be wrong on: silence is what WO-24 is about).
        self._orphans_alerted: set[str] = set()
        self._orphans_alerted_day: date | None = None
        self._last_orphan_sweep: datetime | None = None
        #: strategy_id -> today's observed scores, the population the forward quantile ranks in.
        self._day_scores: dict[str, list[float]] = {}
        #: WO-8 hot-path caches, all rolled together by :meth:`_roll_hot_path_day`.
        #: symbol -> carried ATR(14,1m); bounded by the open-recommended-position count, since
        #: :meth:`on_bar` returns before the ATR for any symbol without one.
        self._atr_state: dict[str, _AtrState] = {}
        self._sector_cache: dict[str, str] | None = None
        self._sector_cache_day: date | None = None
        self._hot_path_day: date | None = None
        self._hot_counts: dict[str, int] = dict.fromkeys(_HOT_PATH_COUNTERS, 0)
        self._hot_ms: dict[str, float] = dict.fromkeys(_HOT_PATH_TIMERS, 0.0)

    def _rearm_slot(self, candidate: SignalCandidate) -> None:
        """Hand the (symbol, strategy) day slot back after a never-evaluated drop (2026-07-29)."""
        self._rearm_slot_pair(candidate.symbol, candidate.strategy_id)

    def _rearm_slot_pair(self, symbol: str, strategy_id: str) -> None:
        """:meth:`_rearm_slot` by PAIR — the displacement path (2026-08-27) knows which pair lost its
        slot but does not necessarily still hold the candidate object, and a pre-screen displacement
        is the same never-evaluated fact this write has always recorded. The ``rearm`` call is a
        harmless no-op there (the pre-screen already dropped the pair from its dedupe set)."""
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
        """§3.2.5 day-slot journal (2026-08-04): a received publication spends the (symbol, strategy)
        slot for day ``d`` — the conservative default for EVERY handler path; :meth:`_rearm_slot`
        flips it back for the never-evaluated drops. Boot rehydration (``engine.ops.main``) rebuilds
        the prescreen's in-memory dedupe/caps from these rows, so a restart no longer resets the
        20/day bound. A journal failure degrades to the old in-memory-only behavior, never blocks
        the candidate.

        Stamps ``unsizeable`` from ``candidate.raw_levels.stop`` (2026-08-14 fix): this write happens
        BEFORE :meth:`on_signal_candidate`'s stopless short-circuit even runs, so without the marker a
        structurally-unforwardable candidate (no stop ⇒ max_qty_by_risk is always 0, §7.1) journals
        indistinguishably from one that was genuinely refused a forward slot — the funnel's
        ``best_unforwarded_score`` (WO-9's starvation reading) then reads a `mom` 1.0 that never had a
        stop as the day's best starved idea. The marker is a fact about the candidate, not about which
        branch of the caller it happened to fall through, so it is set unconditionally here.
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
        """Charge one analyst forward to ``candidate``'s day slot (WO-1 (iv)).

        A COUNTER, not a flag: a pair re-armed after an analyst INFRASTRUCTURE failure (2026-07-29)
        can legitimately be forwarded again, and every attempt that produced a VERDICT is real spend
        against the §5.2(a) cap. Since D1 (c) an attempt that produced none is undone one unit at a
        time by :meth:`_refund_forward` — which is why the column has to stay a counter and why the
        undo is an arithmetic decrement rather than a flag flip. Upserts rather than updates so a
        lost publish-journal row cannot silently swallow the forward record. Journal failure degrades
        to in-memory-only counting, never blocks the call.
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

    def _journal_unsizeable_qty_zero(self, candidate: SignalCandidate, d: date) -> None:
        """Flip the day slot's ``unsizeable`` marker after :meth:`_max_qty_by_risk` computes 0 for a
        candidate that DOES carry a stop (2026-08-14 extension of the stopless case below).

        :meth:`_journal_slot` already ran earlier in :meth:`on_signal_candidate` and stamped
        ``unsizeable`` from ``stop is None`` alone — that misses the sibling case a real stop can
        still land in: the §7.1 per-trade-risk budget cannot afford even one share at this stop's
        absolute distance (live 2026-08-14: OFSS entry 11366.0/stop 10911.35 and SOLARINDS entry
        19187.00/stop 18050.00, both real stops, both max_qty_by_risk 0 on a ₹20,000 account). Same
        funnel category as the stopless case — reuses the same column and the same value
        (``unsizeable=1``), not a distinct reason code: the funnel only needs to answer "was this
        candidate ever eligible for a slot", and the answer is identically no for both. A plain UPDATE
        rather than the upsert :meth:`_journal_slot` uses, because the row is already there. Degrades
        to in-memory-only (never blocks the candidate) on a journal failure, same as every other write
        here.
        """
        try:
            self._conn.execute(
                "UPDATE prescreen_day_slots SET unsizeable=1 WHERE d=? AND symbol=? AND strategy_id=?",
                (d.isoformat(), candidate.symbol, candidate.strategy_id),
            )
        except Exception as exc:  # noqa: BLE001 - journaling is resilience bookkeeping, never a gate
            _log.warning("day_slot_journal_failed", op="unsizeable_qty_zero", symbol=candidate.symbol,
                         strategy_id=candidate.strategy_id, error=str(exc))

    # ------------------------------------------------------------------ §5.2(a) forward queue (WO-1)
    def _roll_forward_day(self, d: date, *, in_flight: tuple[str, str] | None = None) -> None:
        """Roll the forward-cap day, hydrating the counter and the score population from the
        journal, then hand back every ORPHANED admission slot the journal still holds. Called on
        every candidate AND on every :meth:`drain_forward_queue` tick — the second caller is what
        makes the sweep reach a restart on a day where no candidate ever publishes (the pre-screen's
        boot rehydration keeps the orphaned pairs deduped, so they cannot publish themselves). The
        DB read happens once per day change, and therefore exactly once after a restart.

        THE ORPHAN RE-ARM (D1 (a), 2026-09-11). This method clears ``_pending_forwards`` and
        rehydrates only the COUNTER, and the owner restarts the engine 2–3 times per session: every
        restart silently dropped the queue while the journal rows kept ``evaluated=1, forwarded=0``,
        so the pairs stayed deduped for the rest of the day and nothing ever looked at them. ~70
        admitted candidates burned that way since 08-17; 210 of 520 slots all-time were never
        forwarded. Those rows are the same never-evaluated fact :meth:`_expire_forwards` refunds —
        published, no analyst call, no ``agent_calls`` row, no verdict — so they re-arm on exactly
        the 2026-07-29 rule.

        Safe because of WHERE it runs: a roll happens once per day change, and at that instant the
        in-memory queue is empty by construction — on a fresh process it has never been filled, and
        on an in-process day change the ``_pending_forwards.clear()`` below empties it before the
        sweep reads. So no row this sweep re-arms can still be held by a live queue pointer (and the
        entries that clear DOES drop belong to the PREVIOUS day, whose rows this query never
        matches). The ONE exception is ``in_flight``: the caller in
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
        self._requeued_forwards.clear()          # WO-20d: the retry budget is per DAY, like the queue
        self._refunded_forwards.clear()          # D1 (d): so is the refunded-charge memo
        self._window_close_flushed_at = None     # D1 (b): one flush per window CLOSE, never per
        self._window_close_flushed_day = None    #         process — both latches roll with the day
        self._forwarded_count, self._day_scores = self._hydrate_forward_state(d)
        self._rearm_forward_orphans(d, in_flight)

    def _rearm_forward_orphans(self, d: date, in_flight: tuple[str, str] | None) -> None:
        """Re-arm every ``evaluated=1, forwarded=0, unsizeable=0`` pair for ``d`` (D1 (a)).

        THE PREDICATE IS WIDER THAN THE INTENT, KNOWINGLY (D1 (e), 2026-09-12). Those three columns
        cannot tell "nobody ever looked at it" from "refused by budget policy": a candidate dropped
        at arrival by a GOVERNOR BLOCK leaves the same row, and :meth:`on_signal_candidate`
        deliberately does not re-arm that one (a re-publishing backlog would hammer the admission
        gate the moment the block lifts). This sweep re-arms it anyway, and that is accepted as
        BOUNDED: it runs once per day change, so at the owner's 2–3 restarts a session a
        governor-blocked pair can re-publish 2–3 extra times a day — not a loop — and by restart
        time the block has usually lifted, which makes the re-publish the right outcome rather than
        a wasted one. Separating the two states needs a "refused by policy" marker the journal schema
        does not have; adding one is a schema change, not a predicate change.

        Never raises: an unreadable journal costs the re-arm, never the trigger path (D7).
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

        AN UNMEASURED POPULATION IS NOT A TOP ONE (D1 (d), 2026-09-11). THE MECHANISM, named
        precisely, because the obvious reading of it is wrong: the population is never EMPTY at
        ranking time. :meth:`on_signal_candidate` appends the arriving score to ``_day_scores`` one
        line before it enqueues (and a day roll rehydrates the scores and clears the queue in the
        same breath), so every queued entry is ranked against a population that already contains at
        least its own score. A strategy's first candidate of the day was therefore ranked against
        ``[own score]`` — ``quantile = 1/1 = 1.0``, band ``QUANTILE_BANDS - 1``: the candidate
        measured against itself. That, not an empty set, is how a lone late ``orb`` candidate took
        band 4 on 2026-09-10 and outranked a ``hi52`` BHEL at 0.9705 that was genuinely the top of
        its own measured day.

        :data:`MIN_RANK_POPULATION` is the floor that closes it, and it is checked against the
        population the caller actually presents — own score included — so the singleton and the
        two-element cases it was written for are the ones it catches, with the empty case subsumed.
        Below the floor the honest standing is the MIDDLE band: a first arrival still competes on
        ``fired_at`` and still beats anything ranked below the middle, it just no longer wins by
        arithmetic against a measured population.
        """
        population = self._day_scores.get(candidate.strategy_id) or []
        if len(population) < MIN_RANK_POPULATION:
            return QUANTILE_BANDS // 2       # nothing to rank against ⇒ the middle, not the top
        score = float(candidate.score)
        quantile = sum(1 for s in population if s <= score) / len(population)
        return min(QUANTILE_BANDS - 1, int(quantile * QUANTILE_BANDS))

    def _forward_key(self, entry: _PendingForward) -> tuple[int, int, datetime, int]:
        """THE FORWARD SELECTION RULE (§5.2(a), WO-1 (ii)).

        At each analyst slot, forward the highest-priority published-but-unevaluated candidate:

          0. FRONT entries first (WO-20d) — a candidate whose evaluation blew up mid-call is owed the
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

        ``front=True`` is the WO-20d re-queue primitive: the entry sorts ahead of every fresh arrival
        in BOTH selection modes, so a failed evaluation is retried at the next slot rather than
        re-entering the competition. It re-stamps ``fired_at`` and the TTL exactly as a fresh
        publication would — the levels are the ones the failed call was assembled from, and one
        pacing interval of extra life is what a retry costs.

        An overflow eviction re-arms the dropped candidate's §3.2.5 admission slot on exactly the
        rule :meth:`_expire_forwards` documents: an entry that leaves this queue without passing
        through :meth:`_take_forward_slot` was never evaluated by anything, so its slot is not spent.
        Since D1 (c) that includes a ``front`` entry: the re-queue REFUNDS the forward charge its
        blown-up attempt made, so a front entry carries no charge either and the old exclusion would
        leave exactly the ``evaluated=1, forwarded=0`` orphan D1 (a) exists to prevent.
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
        """Drop queued candidates past their own §5.2 TTL horizon, handing the §3.2.5 day slot back
        to every one of them that was never evaluated (2026-08-27).

        THE BUG this replaced. The old rationale here was "expiry does NOT re-arm its day slot — the
        forward cap has never re-armed (2026-07-29: the analyst quota is spent on real evaluations)",
        and it is factually wrong about this case. It conflates two different counters. The §5.2(a)
        forward cap is refunded only for a charge that bought no verdict (:meth:`_refund_forward`,
        D1 (c)) — and a TTL expiry never CHARGED one at all: entries leave this
        queue through :meth:`_take_forward_slot`, which pops an entry *before* it journals a forward,
        so anything still queued has had no analyst call, no ``agent_calls`` row and no gate verdict.
        Nobody judged it. That is exactly the never-evaluated case
        :meth:`~engine.strategy.prescreen.SignalPreScreen.rearm` exists for, and aging out of a paced
        queue is architecturally identical to the analyst INFRASTRUCTURE failure it was written for.

        Live 2026-08-27: SHRIRAMFIN and POLICYBZR (both ``orb``, admitted 09:46:0x, ``evaluated=1``
        and ``forwarded=0`` in ``prescreen_day_slots``, zero rows anywhere in ``agent_calls``) aged
        out unseen and permanently burned 2 of ``orb``'s 7 daily admission slots on candidates
        nothing had looked at.

        Scope, deliberately narrow: this refunds the §3.2.5 ADMISSION slot only. The §5.2(a) forward
        cap and ``_forwarded_count`` are untouched.

        A ``front`` entry — the WO-20d re-queue of an evaluation that blew up mid-call — used to be
        EXCLUDED, on the premise that a forward had already been charged for it. Since D1 (c) that
        premise is false: :meth:`_handle_forward_failure` REFUNDS the charge before it re-queues,
        because the front entry is charged again when it is re-drained. So a front entry that ages
        out now holds no charge, has no ``agent_calls`` row and no verdict, and leaving its
        admission slot spent produced exactly the ``evaluated=1, forwarded=0`` orphan signature
        D1 (a) exists to eliminate — one the same-process day can never sweep, since the orphan
        sweep runs only on a day change. It re-arms like every other never-evaluated exit. (The
        WO-20d LOST branch keeps its charge and never re-queues, so nothing here reaches it.)
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
        """Hand the §3.2.5 admission slot back to everything the CLOSED window stranded (D1 (b)).

        THE STRANDING. :meth:`_drain_one_forward` returns on a closed window without touching the
        queue, and :meth:`_expire_forwards` only ever runs inside :meth:`_take_forward_slot` — which
        that early return never reaches. A candidate queued near the close therefore neither
        forwards nor expires: it sits in a dead queue until the process ends, holding a day slot
        nothing will ever look at. Live 2026-09-10: six entries stranded at 12:26, including the
        ``hi52`` BHEL candidate at 0.9705, the day's best.

        Same never-evaluated accounting as :meth:`_expire_forwards`, ``front`` entries included and
        for the same D1 (c) reason: a WO-20d retry carries no forward charge any more, so leaving it
        in a dead queue would strand it exactly as this method exists to prevent.

        Once per CLOSE, not once per day: ``closed_at`` is the window end this flush answers, and a
        window the owner moves or re-opens later in the session (§3.2.7) presents a different end and
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
        """Act on the §3.2.5 admission slots the pre-screen re-assigned (2026-08-27).

        The pre-screen decides displacement on a scan worker thread and can only record the verdict;
        the two things that verdict IMPLIES both live here and both must happen on the event loop:
        the evicted candidate leaves ``_pending_forwards`` (it no longer holds a slot, so it must
        never reach the analyst), and its day-slot journal row goes back to ``evaluated=0`` — the
        same never-evaluated accounting :meth:`_expire_forwards` uses, because it is the same fact.

        This is the tidy path, not the safe one. Safety is :meth:`SignalPreScreen.claim_slot`, which
        catches the entry already popped by a drain tick that ran between the pre-screen's decision
        and this sweep. A displaced entry that somehow survives both is still harmless — its TTL
        expiry re-arms it exactly as before.

        ``front`` entries (WO-20d retries) are skipped defensively: the pre-screen marked the pair
        EVALUATED at :meth:`_take_forward_slot`'s claim (the D1 (c) refund gives back the forward
        charge, not that mark), so it could not have chosen one as a displacement victim — but the
        queue is the thing that would be corrupted if that ever stopped holding.
        A pair RETAINED by that skip is not re-armed either: the two walks below have to agree, or
        the retained entry would keep its queue pointer while its slot was handed back as free.

        Never raises (D7): a telemetry-adjacent hand-off must not be able to kill the trigger path.
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
                              reason="admission slot reassigned to a better candidate (§3.2.5)")
                    continue
                retained.add(key)
            live.append(entry)
        self._pending_forwards = live
        for pair in pairs:
            if pair in retained:
                continue
            self._rearm_slot_pair(*pair)

    def _best_pending_score(self) -> float | None:
        """Highest score sitting unforwarded in the queue — the live starvation reading (WO-9)."""
        if not self._pending_forwards:
            return None
        return max(float(p.candidate.score) for p in self._pending_forwards)

    def _take_forward_slot(self, cap: int | None) -> SignalCandidate | None:
        """Spend one analyst slot on the best pending candidate, or return None if none is due.

        THE COMMIT POINT for two different budgets, which is why the claim lives here and nowhere
        else. Popping the entry spends the §5.2(a) forward cap; :meth:`SignalPreScreen.claim_slot`
        simultaneously commits the §3.2.5 ADMISSION slot to a real evaluation, making it permanent
        (2026-08-27). A ``False`` claim means a better same-strategy candidate took that admission
        slot while this one waited — forwarding it anyway would spend the reassigned slot twice — so
        the entry is dropped and the loop tries the next-best, rather than returning ``None`` and
        stalling a whole pacing interval on a candidate that no longer exists.

        The claim runs under the PRE-SCREEN's lock, which is what makes it atomic against the
        displacement decision itself: claim-then-displace leaves the pair evaluated and undisplaceable,
        displace-then-claim returns ``False`` here. Neither order can produce two spends.

        The D1 (e) band screen runs BEFORE the claim and DEFERS rather than drops: a level-pinned
        candidate whose level has already walked outside the §7.1 ``entry_sanity_band`` is a
        guaranteed gate reject, so it must not spend an analyst slot at THIS tick — but the level is
        a live reading that can come back inside the band minutes later, and ``brk20`` originates
        only from :meth:`run_scan_sweep`, so a candidate re-armed out of the queue has nothing to
        re-publish it for the rest of the day. It therefore stays queued (its own TTL is still its
        exit) and the loop moves to the next-best. Screening before the claim keeps it displaceable
        while it waits: :meth:`SignalPreScreen.claim_slot` marks a pair permanently evaluated, which
        is a promise only a candidate actually being dispatched may make.
        """
        self._expire_forwards(self._clock.now())
        if cap is not None and self._forwarded_count >= int(cap):
            return None
        deferred: list[_PendingForward] = []
        try:
            while self._pending_forwards:
                if self._forward_mode == "arrival":
                    # WO-20d: the front flag outranks arrival order here too — the rollback mode must
                    # not quietly lose the re-queue guarantee.
                    entry = min(self._pending_forwards,
                                key=lambda p: (0 if p.front else 1, p.fired_at, p.seq))
                else:
                    entry = min(self._pending_forwards, key=self._forward_key)
                self._pending_forwards.remove(entry)
                if self._outside_entry_band(entry.candidate):
                    deferred.append(entry)
                    continue
                if not self._claim_forward_slot(entry.candidate):
                    _log.info("forward_slot_displaced", signal_id=entry.candidate.signal_id,
                              symbol=entry.candidate.symbol,
                              strategy_id=entry.candidate.strategy_id, score=entry.candidate.score,
                              reason="admission slot was reassigned to a better candidate (§3.2.5)")
                    self._rearm_slot(entry.candidate)
                    continue
                self._forwarded_count += 1
                # A NEW charge re-opens the D1 (d) refund for this signal_id: the memo says "the
                # charge that was made is already refunded", and this is a different charge (the
                # WO-20d front retry is the one candidate that can be charged twice in a day).
                self._refunded_forwards.discard(entry.candidate.signal_id)
                if self._forwarded_day is not None:
                    self._journal_forward(entry.candidate, self._forwarded_day)
                return entry.candidate
            return None
        finally:
            # Every exit puts the band-skipped entries back, including the one that returns a
            # candidate: a deferral is "not this tick", never "off the queue".
            self._pending_forwards.extend(deferred)

    def _outside_entry_band(self, candidate: SignalCandidate) -> bool:
        """True when ``candidate``'s entry level is already outside the §7.1 ``entry_sanity_band``
        against the live LTP — a guaranteed gate reject that must not cost an analyst slot (D1 (e)).

        Scoped to :data:`BAND_SKIP_STRATEGIES` (the LIMIT-at-level legs). Live 2026-09-11: 6 of 15
        ``brk20`` proposals died on ``entry_sanity_band``, every one of them after spending a slot
        at a degraded cap where four slots is the whole day.

        Fails OPEN in every unknown: no ``ltp_fn``, no tick for the symbol, a non-positive LTP, an
        unreadable limit table or a raising seam all mean "we cannot say this is a guaranteed
        reject", and the D7 direction for that is to forward it and let the gate decide.

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
        """Ask the pre-screen to commit this pair's admission slot. Unwired ⇒ ``True`` (the
        pre-2026-08-27 behaviour). A RAISING claim is also ``True``: the §3.2.5 seam failing must
        degrade to "forward it anyway", never to silently withholding analyst calls (D7)."""
        if self._claim_slot is None:
            return True
        try:
            return bool(self._claim_slot(candidate.symbol, candidate.strategy_id))
        except Exception as exc:  # noqa: BLE001 - a broken claim seam never withholds an evaluation
            _log.warning("forward_slot_claim_failed", symbol=candidate.symbol,
                         strategy_id=candidate.strategy_id, error=str(exc))
            return True

    def _forward_cap(self) -> int | None:
        """Today's §5.6 analyst forward cap, or ``None`` when the governor does not publish one."""
        cap_fn = getattr(self._governor, "prescreen_forward_cap", None)
        return cap_fn() if cap_fn is not None else None

    # ------------------------------------------------- WO-9 raw funnel counters, persisted (2026-08-21)
    def _flush_funnel_raw(self) -> None:
        """UPSERT today's per-strategy RAW pre-screen counts into ``funnel_raw_counts``.

        THE BUG. The top of the WO-9 funnel — what the scanners PRODUCED, before dedupe and the
        §3.2.5 caps — was the one funnel number with no DB home, so every engine restart zeroed it
        and the 22:35 ``funnel_utilization`` line reported ``raw=None`` ("unmeasured") for the whole
        day. That happened on 4 of the last 6 trade days, which is WO-9's own "the analyst never saw
        it vs nothing fired" ambiguity, re-introduced by the restart.

        WHY HERE. The 60 s drain tick already runs all session for exactly this class of paced,
        non-urgent work, and it is the one live-engine cadence the pre-screen counters can be read
        from off the scan path. At most one flush per tick, and only when the counts CHANGED since
        the last one — the counters move at most once per bar, so an unchanged afternoon costs a
        dict comparison and no write at all.

        ABSOLUTE values, never increments. The in-memory counter is itself hydrated from these rows
        on the pre-screen's first day roll (``SignalPreScreen._load_raw_counts_locked``), so what it
        holds IS the day's running total across every process that ran the day — an UPSERT of it is
        idempotent, and a duplicated or replayed flush cannot double-count.

        Never raises (D7): telemetry must not be able to break trading. A failed flush logs
        ``funnel_raw_flush_failed`` and leaves the previously-persisted rows exactly as they were —
        the review then reads a slightly stale raw count, which still beats "unmeasured".
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

    # -------------------------------------------------- WO-24c: the orphaned-proposal sweep
    async def _sweep_orphans_if_due(self) -> None:
        """Run :meth:`sweep_orphaned_proposals` at most once per ``_ORPHAN_SWEEP_INTERVAL_MIN``.

        The drain tick pulses every 60 s; re-reading the proposals⋈verdicts join every minute buys
        nothing that an orphan already ``_ORPHAN_TTL_MIN`` minutes old will not still prove five
        minutes later. The anchor advances on every sweep that passes the guard — dispatched or
        not — exactly like :attr:`_last_forward_drain`.
        """
        now = self._clock.now()
        last = self._last_orphan_sweep
        if last is not None and now - last < timedelta(minutes=_ORPHAN_SWEEP_INTERVAL_MIN):
            return
        self._last_orphan_sweep = now
        await self.sweep_orphaned_proposals()

    async def sweep_orphaned_proposals(self) -> int:
        """Announce every proposal that has carried NO verdict for ``_ORPHAN_TTL_MIN`` minutes.

        WO-24c — the backstop half of the 2026-08-21 funnel freeze. :meth:`_gate_and_persist`
        writes the proposal row and only THEN builds the gate context, so anything that kills the
        path between those two points (the WO-24a store stall, a crash, a killed process) strands a
        proposal that no measure the platform has would ever mention again: it is not a decline, not
        an agent failure, not a rejection — it is a row with nothing attached to it. This sweep is
        that missing measure.

        Today's real orphan ``01M0H8ZXM3PYNAVXF54A5TDGV0`` — the platform's FIRST-EVER proposal,
        stranded at 09:56:40 on 2026-08-21 — will be caught by the first live sweep after this
        ships. That is expected and correct, not a false positive: the incident really did leave it
        there, and the point of the sweep is that such a row can never again sit unnoticed.

        Each ``proposal_id`` is announced ONCE per process-day (:attr:`_orphans_alerted`, rolled by
        :meth:`_roll_orphan_day`) — but only while it is YOUNGER than
        :data:`_ORPHAN_ALERT_MAX_AGE_MIN` (24 h). A permanent orphan is unresolvable by design (no
        verdict is ever invented), and before the cap the same 2026-08-21 orphan paged the owner on
        every boot and day-roll (three times over one weekend). Past the cap it is logged at INFO
        once per process-day and counted by the nightly review, never paged: an orphan older than a
        day is history, not news. Returns how many orphans were announced OR quietly noted. Never
        raises — a watchdog that can kill the tick it rides on is not a watchdog.
        """
        now = self._clock.now()
        self._roll_orphan_day(self._clock.today())
        cutoff = now - timedelta(minutes=_ORPHAN_TTL_MIN)
        try:
            rows = self._conn.execute(
                "SELECT p.proposal_id AS proposal_id, p.action AS action, "
                "p.created_at AS created_at "
                "FROM proposals p LEFT JOIN verdicts v ON v.proposal_id = p.proposal_id "
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
                    "(WO-24c, after the 2026-08-21 gate-context freeze). No verdict is invented "
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

    # ------------------------------------------------------------ §5.2(a) paced drain (2026-08-14)
    async def drain_forward_queue(self) -> bool:
        """Spend at most one analyst slot, at most once per ``FORWARD_PACING_MIN`` minutes.

        Pulsed every 60 s from the composition root (``forward_drain_tick``); THIS method owns the
        cadence, exactly as :meth:`heartbeat` owns its own admission while ``heartbeat_tick`` owns
        only the pulse. Returns True when a candidate reached the analyst.

        Why the drain had to leave the arrival path (live 2026-08-14, the first day under WO-1): an
        arriving candidate was enqueued and then immediately drained, so the queue held exactly one
        entry at every slot and "take the best pending" decided nothing. All 12 slots were spent
        five minutes into the session on rsi2 scores [0.14, 0.17, 0.27, 0.46, 0.48, 0.66] while the
        day's best unforwarded candidates (rsi2 0.83, orb 1.00, mom 1.00) arrived later and never
        got one. Pacing is what gives the ranking rule a population to rank.

        The tick ALSO carries the pre-screen's raw-counter flush (2026-08-21) — run before the
        drain-mode check, so the ``immediate`` rollback keeps its telemetry. See
        :meth:`_flush_funnel_raw`. The WO-24c orphan sweep rides the same tick on the same terms
        (see :meth:`_sweep_orphans_if_due`): both are watchdogs on the funnel, and a funnel that has
        stopped moving is exactly when they have to still run.

        The day ROLL rides this tick too (D1 (a)). :meth:`_roll_forward_day`'s other two callers are
        an arriving candidate and the drain below, and neither reaches a fresh process that has no
        candidates: the drain returns on an empty queue and the pre-screen's boot rehydration keeps
        the orphaned pairs deduped, so nothing publishes them. The boot orphan re-arm would then
        wait for a pair the dedupe has NOT swallowed — on a thin day, forever. This tick always
        runs, so the once-per-day sweep always happens. It is also the roll that fires on a day
        where no candidate ever arrives.
        """
        self._roll_forward_day(self._clock.today())
        self._flush_funnel_raw()
        # 2026-08-27: settle reassigned admission slots here too. The batch admission path
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

        ONE per call, never a burst: draining the whole remaining budget the moment it is available
        is the inline drain again, just batched — the day's later candidates would still be facing
        an exhausted cap.

        Every gate :meth:`on_signal_candidate` applies at arrival is re-applied HERE because time
        has passed since the candidate was queued: the window can have closed, the owner can have
        killed or frozen, the budget can have run out. A closed gate leaves the queue untouched and
        re-arms nothing AT THAT MOMENT — a queued candidate is a REFUSED one we kept a pointer to,
        and its only exit is its own TTL. Since 2026-08-27 that exit DOES hand the §3.2.5 admission
        slot back (:meth:`_expire_forwards`), because a candidate that ages out unseen was never
        evaluated; the §5.2(a) forward cap is given back only where a charge bought no verdict at all
        (:meth:`_refund_forward`, D1 (c)), never for a real evaluation.

        The one gate that is NOT "come back next tick" is the window having CLOSED for the day
        (D1 (b)): no later tick can ever drain what is queued, so that branch flushes the queue once
        (:meth:`_flush_forwards_at_window_close`) instead of stranding it. Mode / FROZEN / kill keep
        the queue untouched exactly as before — those lift, a closed window does not.
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
        decision = self._governor.can_invoke(INTRADAY_AGENT_ID, "signal")
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
        # "Which candidate did the analyst actually see, and what was still waiting behind it" must
        # be answerable without a forensic DB read (WO-9) — and under pacing the answer is the whole
        # point of the change.
        _log.info("forward_drained", signal_id=chosen.signal_id, symbol=chosen.symbol,
                  strategy_id=chosen.strategy_id, score=chosen.score,
                  forwarded=self._forwarded_count, cap=None if cap is None else int(cap),
                  queued=len(self._pending_forwards),
                  best_unforwarded_score=self._best_pending_score())
        await self._evaluate_forward_guarded(chosen, d)
        return True

    # ------------------------------------------------------------ WO-20d: the drain never loses one
    async def _evaluate_forward_guarded(self, candidate: SignalCandidate, d: date) -> None:
        """Evaluate ``candidate``, and make a blown-up evaluation VISIBLE and RECOVERABLE.

        THE INCIDENT (2026-08-18, KALYANKJIL). An ``asyncio.CancelledError`` raised inside the
        analyst-call transport propagated out of :meth:`_evaluate_forward`, through the drain, and
        killed the APScheduler tick. The candidate had already been popped from the queue and
        charged a forward in the journal, and the call never reached the point where an
        ``agent_calls`` row is written — so the day's record said ``forwarded=1`` and nothing else
        anywhere. Invisible to every DB measure we have: not a decline, not a failure, just gone.

        The decision table (WO-20d):

        * **first failure for a signal_id** — re-queue at the FRONT and log ``forward_evaluation_requeued``
          at WARNING. A ``CancelledError`` is then RE-RAISED: shutdown cancellation must propagate or
          the engine cannot stop. A generic exception is swallowed so the tick survives for the rest
          of the queue.
        * **second failure** — no re-queue; log ``forward_evaluation_lost`` at ERROR, then the same
          re-raise/swallow split. One retry, not a loop: a candidate that fails twice is failing for
          a reason a third attempt will not fix, and a self-refilling queue would spend the day's
          whole analyst cap on one broken symbol.

        Journalling is no longer untouched (D1 (c), 2026-09-11). The RE-QUEUE branch refunds the
        charge :meth:`_take_forward_slot` made, because the front entry it enqueues will be drained
        again and charged AGAIN — without the refund one candidate that blew up mid-call costs two
        slots out of a cap that is four a day at DG1+. The LOST branch does not refund: nothing will
        charge for that candidate again, and WO-20d's reading still holds for the attempt that ends
        there — one candidate, one net charge. A lost candidate is never marked evaluated by any
        path here.

        This guard wraps the DRAIN only. ``immediate`` mode (the WO-1 rollback, where an arriving
        candidate evaluates itself inline on the bus handler) is out of scope: the incident is a
        scheduler-tick death, and that path has no tick to kill.
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
        """Re-queue once, then let it go loudly (WO-20d). Never raises: it is the failure path."""
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
        """Give back the one §5.2(a) forward charge :meth:`_take_forward_slot` made (D1 (c)).

        THE BURN (2026-09-11). The charge and the journal row are written BEFORE the analyst call,
        so an infrastructure failure — timeout, SDK death, transport cancellation — spent a slot on
        a call that produced no verdict, no ``agent_calls`` row and no proposal: 110 charges burned
        that way. At the DG1+ cap of four, one timeout is a quarter of the day. The §3.2.5 admission
        slot was already refunded on this path (2026-07-29); this is the sibling refund for the
        analyst quota, and it is scoped to the same fact — NOTHING EVALUATED IT. A governor block, a
        no_action verdict and a gate rejection are all real outcomes and never reach here.

        Exactly one unit, floored at zero on both counters: the in-memory count can legitimately be
        0 when a restart lost the increment the journal still holds, and the journal column is a
        COUNTER of attempts (:meth:`_journal_forward`) that must never go negative. Never raises —
        a failed refund leaves the cap conservative (over-charged), which is the safe side.

        ONE REFUND PER CHARGE, enforced here rather than by the call order (D1 (d), 2026-09-12).
        Two call sites can fire for the same charge: :meth:`_evaluate_forward`'s failure branch, and
        :meth:`_handle_forward_failure` when anything in that branch raises on the way out (the
        owner-notify seam is the live candidate). Refunding twice for one charge hands the day an
        analyst call the §5.2(a) cap did not grant — the one direction of this change that is not
        conservative — so the signal_id is memoed and the repeat is a logged no-op. The memo is
        cleared when :meth:`_take_forward_slot` charges the same candidate again, so the WO-20d
        retry's second charge is still refundable.
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

    # ================================================================== trigger (a): entries
    async def on_signal_candidate(self, candidate: SignalCandidate) -> None:
        """§5.2 trigger (a). Also usable directly as a ``signal.candidate`` bus handler.

        Entry-seeking calls fire ONLY inside the owner-set trade window (§1.4 item 11 / §7.1
        ``trade_window``); every earlier check is cheaper still. A governor block fails to
        no-proposal (D7) — silently, because "we did not call" is already an ``agent_calls`` row.

        Under the default ``paced`` drain mode this method ADMITS a candidate to the queue and stops
        there; the analyst call happens on the next :meth:`drain_forward_queue` tick, which re-checks
        every gate below. The gates still run here so a candidate that cannot be evaluated at all is
        never queued, and so the never-evaluated day-slot re-arms (2026-07-29) keep firing at the
        moment the drop happens rather than a pacing interval later.
        """
        # Drops where the candidate was NEVER EVALUATED hand the prescreen day slot back
        # (2026-07-29 owner decision) so a still-true condition is waiting when conditions change
        # (window opens, freeze lifts, mode returns). The prescreen charges its caps once per pair,
        # so the re-arm/re-publish cycle can never exhaust a day cap. Deliberate NON-re-arms:
        # a governor block (budget policy), the forward cap (the analyst quota was spent on real
        # evaluations), and an unsizeable candidate (no stop ⇒ nothing to wait for today). A
        # cap-refused candidate is QUEUED rather than dropped, so it is not stranded either: if no
        # slot ever opens for it, its TTL expiry hands the day slot back (:meth:`_expire_forwards`,
        # 2026-08-27).
        #
        # FIRST, settle any admission slot the pre-screen reassigned — very possibly THIS candidate's
        # arrival is what caused one, since publication follows admission immediately. Doing it before
        # the journal write below keeps the two rows from fighting over the same table.
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
        d = self._clock.today()
        window = self._window(d)
        if window is None or not (window[0] <= self._clock.now() <= window[1]):
            _log.info("signal_candidate_out_of_window", signal_id=candidate.signal_id,
                      symbol=candidate.symbol)
            self._rearm_slot(candidate)
            return
        if candidate.raw_levels.stop is None:
            # No stop level ⇒ max_qty_by_risk is 0 ⇒ a guaranteed no_action — never spend an
            # analyst call on it (2026-07-29 owner decision; today: `mom` until ledger-driven
            # rebalance state lands). The day slot stays consumed: no stop will appear today.
            _log.info("signal_candidate_unsizeable", signal_id=candidate.signal_id,
                      symbol=candidate.symbol, strategy_id=candidate.strategy_id,
                      reason="no stop level — cannot size, analyst call would be wasted")
            return
        max_qty = self._max_qty_by_risk(candidate)
        if max_qty <= 0:
            # A real stop, but the §7.1 per-trade-risk budget cannot afford even ONE share at this
            # stop's absolute distance (2026-08-14 extension, live: OFSS/SOLARINDS both had genuine
            # stops and still priced to 0 shares against a ₹20,000 account) — the gate could never
            # approve any quantity, so this is the same wasted-analyst-call case the stopless check
            # above already avoids. Same funnel category (unsizeable, never queued); the day slot
            # stays consumed exactly as the stopless drop leaves it.
            self._journal_unsizeable_qty_zero(candidate, d)
            _log.info("candidate_unsizeable_qty_zero", signal_id=candidate.signal_id,
                      symbol=candidate.symbol, strategy_id=candidate.strategy_id,
                      entry=str(candidate.raw_levels.entry), stop=str(candidate.raw_levels.stop),
                      budget=str(self._risk_budget_inr(candidate)))
            return
        # ``in_flight``: _journal_slot above already wrote THIS pair as evaluated=1/forwarded=0, so
        # without the exclusion the boot orphan sweep would re-arm the candidate it is admitting.
        self._roll_forward_day(d, in_flight=(candidate.symbol, candidate.strategy_id))
        decision = self._governor.can_invoke(INTRADAY_AGENT_ID, "signal")
        if not decision.allowed:
            # A budget block is deliberate policy, not a missed slot: the candidate is neither
            # re-armed nor queued (queueing it would let a blocked window build a backlog that
            # hammers the admission gate the moment the block lifts).
            _log.warning("signal_candidate_governor_blocked", signal_id=candidate.signal_id,
                         reason=getattr(decision, "reason", None))
            return
        # §5.2(a) forward cap (agents.yaml prescreen_cap_per_day at DG0 — 48 since 2026-08-18,
        # was 12 from 2026-08-11, 6 before; 4 at DG1+ — §5.6): the governor owns the number, this
        # counter the enforcement
        # (2026-07-28 review: it had no consumer, so a volatile day could burn 20 analyst calls).
        # Counts only calls that reach the harness; the coarse prescreen settings cap still bounds
        # candidate PUBLICATION. Since WO-1 the slot goes to the best PENDING candidate rather than
        # to whoever arrived while budget remained (see _forward_key): the arriving candidate joins
        # the queue, then the queue is drained — so a candidate refused at a full cap stays
        # available for a slot that opens later (a degrade-tier recovery raises the cap mid-day)
        # instead of being dropped on the floor.
        self._day_scores.setdefault(candidate.strategy_id, []).append(float(candidate.score))
        self._enqueue_forward(candidate)
        if self._drain_mode == "paced":
            # 2026-08-14: the arriving candidate NEVER drains its own slot. It waits for the next
            # drain tick and competes there with everything else that arrived inside the interval —
            # the accumulation without which the ranking rule ranks a set of one.
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
            # The whole point of the queue: the slot went to a better-standing candidate that was
            # refused earlier. Logged because "which candidate did the analyst actually see" must
            # be answerable without a forensic DB read (WO-9).
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
            # once-per-day publication back (owner-directed 2026-07-29 — six candidates burned by a
            # broken analyst could not re-publish in the repaired window) AND give back the §5.2(a)
            # forward charge _take_forward_slot made before the call (D1 (c), 2026-09-11 — 110
            # charges burned on calls that produced no verdict). governor_blocked is a deliberate
            # budget policy, not an outage: it re-arms nothing and refunds nothing, because the slot
            # the governor refused was never spent in the first place.
            #
            # The alert goes FIRST so that the refund is made exactly once. This whole method runs
            # inside :meth:`_evaluate_forward_guarded`'s try, and an owner-notify seam that raises
            # would otherwise land in :meth:`_handle_forward_failure`, which refunds again for the
            # re-queue it makes — two units undone for one charge, and the day would then make one
            # analyst call more than the cap allows. Since D1 (d) the ordering is belt to
            # :meth:`_refund_forward`'s braces: the charge is memoed by signal_id, so a raise from
            # ANY statement between the refund and the return (not just this one) still costs the
            # cap exactly one unit.
            await self._alert_agent_failed("signal_candidate", result)
            if result.reason != "governor_blocked":
                self._rearm_slot(candidate)
                self._refund_forward(candidate, d, reason=str(result.reason or "agent_failed"))
            return
        self._note_agent_ok()
        payload = result.payload
        if isinstance(payload, NoActionOutput):
            if payload.regime_note:
                self._assembler.set_regime_note(payload.regime_note)
            _log.info("signal_candidate_no_action", signal_id=candidate.signal_id,
                      reason=payload.reason)
            # A declined evaluation holds no position and so holds no slot: hand the pair back to
            # cap displacement (2026-09-04 — five no_action verdicts locked every orb slot on 09-03).
            # The journal row stays evaluated=1: the analyst ran, this is not the 07-29 re-arm.
            if self._decline is not None:
                self._decline(candidate.symbol, candidate.strategy_id)
            return

        # STRUCTURAL COHERENCE (R1 — the structural half of "the analyst disposes of THIS candidate"):
        # every symbol-scoped GateContext fact is resolved for candidate.symbol, and the TTL/product
        # derive from candidate.style, so an LLM that substitutes any identity field would be judged
        # on another instrument's facts (2026-07-28 review: gate approved a hijacked out-of-universe
        # symbol). A mismatch is a hallucination — dropped like schema-invalid output (D7), no
        # proposal row, owner alerted.
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
                        f"({', '.join(sorted(mismatches))}) — dropped like schema-invalid output "
                        "(D7); nothing was recommended."
                    ),
                    severity="warning",
                    data={"signal_id": candidate.signal_id,
                          "mismatches": {k: [str(g), str(w)] for k, (g, w) in mismatches.items()}},
                ))
                return

        try:
            verdict, gate_ctx = await self._gate_and_persist(
                payload, candidate.symbol, str(getattr(payload, "side", candidate.side)),
                candidate.style, d,
            )
        except GateContextTimeout as exc:
            # WO-24a: handled HERE, deliberately not left to _evaluate_forward_guarded. That guard
            # re-queues a blown-up evaluation at the FRONT of the queue (WO-20d), which is exactly
            # the wrong move for this failure: the store is the thing that is stalled, so the retry
            # would take the next slot, hit the same dead store and spend a second analyst call to
            # learn nothing. Re-arming the day slot instead hands the pair back to the prescreen, so
            # it re-publishes on its own terms once the store answers again.
            await self._handle_gate_context_timeout(candidate, exc)
            return
        if verdict.verdict == "owner_approval_required":
            await self._request_owner_approval(payload, verdict)
            return
        if verdict.verdict not in ("approve", "shrink") or payload.action != "enter":
            return

        # Same reference the gate priced the proposal on: the LIMIT price when given, else the live
        # LTP, else the candidate's trigger level (never a guess above all three).
        reference = _dec(getattr(gate_ctx, "ltp", None) or entry_ref)
        rec = self.build_recommendation(payload, verdict, reference)
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
        # WO-4 (iii): the payload states the live print next to a (possibly level-anchored) entry.
        live_ltp = getattr(gate_ctx, "ltp", None)
        await self._send(catalog.recommendation_message(
            rec, ltp=_dec(live_ltp) if live_ltp is not None else None,
        ))

    async def _handle_gate_context_timeout(
        self, candidate: SignalCandidate, exc: GateContextTimeout
    ) -> None:
        """The WO-24a landing: say it loudly, give the slot back, and leave the orphan alone.

        No verdict is fabricated. The proposal row survives with nothing attached to it, and
        :meth:`sweep_orphaned_proposals` is the thing that notices — an invented ``reject`` here
        would read, forever after, as a decision the gate made (§3.4). Never raises: it IS the
        failure path, and the drain tick has to survive to reach the next candidate.
        """
        _log.error(
            "gate_context_timeout", signal_id=candidate.signal_id, symbol=candidate.symbol,
            strategy_id=candidate.strategy_id, proposal_id=exc.proposal_id,
            deadline_s=_GATE_CONTEXT_DEADLINE_S,
        )
        # Same treatment as an analyst INFRASTRUCTURE failure (2026-07-29): the candidate was never
        # judged, so its once-per-day publication goes back to the prescreen.
        self._rearm_slot(candidate)
        await self._send(CatalogMessage(
            kind=MessageKind.LIMIT_BREACH,
            title=f"Gate context timed out ({candidate.symbol})",
            body=(
                f"The gate context for {candidate.symbol} did not build within "
                f"{_GATE_CONTEXT_DEADLINE_S:.0f}s, so candidate {candidate.signal_id} was never "
                f"judged and nothing was recommended (D7). Proposal {exc.proposal_id} is persisted "
                "with no verdict — the market store is the suspect (WO-24a, 2026-08-21). The "
                f"({candidate.symbol}, {candidate.strategy_id}) day slot has been handed back so "
                "the setup can re-publish; deterministic exits, stops and square-offs are "
                "unaffected (R1)."
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
        """The same WO-24a landing for the two position-management paths (trigger (b) and the §7.1
        sweep). No verdict is fabricated and the orphan is left for :meth:`sweep_orphaned_proposals`,
        exactly as on the candidate path; there is no day slot to hand back here, because neither
        path is a prescreen publication. Shares the ERROR event name with the candidate handler so
        one grep finds every timeout. Never raises: it IS the failure path.
        """
        position_id = str(position["position_id"])
        symbol = str(position["symbol"])
        _log.error(
            "gate_context_timeout", position_id=position_id, symbol=symbol, action=action.action,
            proposal_id=exc.proposal_id, deadline_s=_GATE_CONTEXT_DEADLINE_S,
        )
        # The deterministic time stop is the only proposal here the platform builds itself.
        if str(action.agent_id) == PLATFORM_AGENT_ID:
            next_step = (
                "the sweep finishes the other aged positions, then fails the reco_expire job so "
                "the catch-up retries it (30-min sweep / next startup); it is idempotent by age"
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
                f"{exc.proposal_id} is persisted with no verdict — the market store is the suspect "
                f"(WO-24a). Next: {next_step}. Any stop the owner already placed on this position "
                "stands regardless (R3)."
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
        self, action: EnterAction, verdict: GateVerdict, entry_ref: Decimal
    ) -> Recommendation:
        """Build the §3.6 owner payload from an approved/shrunk ``enter`` proposal.

        ``entry_zone`` for a LIMIT proposal is the single proposed price on both sides — the price is
        the instruction. For a MARKET proposal there is no price yet, so the zone spans from the
        engine's entry reference to the §7.1 ``entry_sanity_band`` edge for the product (+1% MIS /
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
        return Recommendation(
            rec_id=str(ULID()),
            created_at=self._clock.now(),
            valid_until=action.valid_until or self._ttl(action.style),
            kind="entry",
            instrument=action.tradingsymbol,
            side=action.side,
            style=action.style,
            product=product,
            entry_zone=zone,
            stop=_dec(action.stop_price),
            targets=targets,
            qty=qty,
            notional=notional,
            thesis=action.thesis,
            confidence=action.confidence,
            short_flag_higher_tail_risk=action.side == "SELL",
            gate=verdict,
            cost=cost,
            manual_checklist=self._entry_checklist(action, zone, product),
        )

    def _entry_checklist(
        self, action: EnterAction, zone: tuple[Decimal, Decimal], product: str
    ) -> list[str]:
        """The B7/R3 protective-order checklist — the whole mechanism by which protection becomes the
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
            target = _dec(action.target_price) if action.target_price is not None else None
            items.append(
                f"after entry fills, place GTT OCO stop {stop}"
                + (f" / target {target}" if target is not None else " (no target — stop-only GTT)")
            )
        items.append(f"set alert at {alert}")
        if product == "MIS":
            window = self._window(self._clock.today())
            end = window[1].strftime("%H:%M") if window else "the trade-window end"
            items.append(f"square off by {end}; 15:10 is only a session backstop")
        return items

    # ================================================================== trigger (b): position events
    async def on_bar(self, bar: Bar) -> None:
        """§5.2 trigger (b) — stop-proximity on an open RECOMMENDED position.

        Fires whenever the engine is up, in ANY mode, and can only produce risk-reducing output: an
        exit or a stop tighten (R3). Never window-gated — the window gates entries only.
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

    # ------------------------------------------------------------------ WO-D2: exit hygiene screens
    def _skip_position_event(self, position: sqlite3.Row) -> bool:
        """True when a §5.2(b) event on ``position`` could only repeat something already said.

        THE 2026-09-11 finding: two positions the owner sold outside the ledger on 08-26 stayed OPEN
        to 09-11 and cost 182 position-event analyst calls and 68 exit recommendations — 6 to 10 a
        day, every one of them the same sentence about the same two names. On 09-08 the month's one
        genuine ins entry (JINDALSTEL) arrived as notification 2 of 8; the other seven were those
        repeats. Both screens below are about that noise, and NEITHER relaxes a risk rule:

        (a) **sold outside the ledger** — two consecutive broker observations showing the position is
            not held AT ALL (§3.6 journal, ``require_zero=True``). The platform cannot recommend an
            exit from a position that does not exist; the owner is still told about it, once a day,
            by the reconcile's own alert and by the day plan's "sold outside the ledger" block, both
            of which name the ``/closed`` reply that ends it. Logged once per position per day so the
            log carries the fact without re-stating it every hour.

            ``require_zero`` is load-bearing and not a detail: the journal's wide predicate is
            ``held < tracked``, which also catches a PARTIAL exit (held 3 of a tracked 7) — a
            position that still carries real exposure and still needs its stop watched — so only
            "the broker holds nothing" may buy silence here. (Shares pledged for margin count as
            held via Kite's ``collateral_quantity``, so a pledge never reaches either predicate.)
            The day plan keeps the wide reading — it TELLS the owner rather than going quiet. A
            position that IS gone stops producing exit recommendations, so the gate excludes it from
            the position/sector counts on the same journal (``GateContextBuilder``'s
            ``missing_holdings_fn``) rather than through ``_exiting_symbols``'s 3-day window.

        (b) **repeat exit** — an exit recommendation for this position was already delivered today,
            is STILL LIVE, and the position row's stop and qty are unchanged since. The first exit of
            every session still fires: this screen is scoped to the day, so a stop that stays
            breached still produces exactly one exit recommendation per session, which is what keeps
            the position inside ``gate._exiting_symbols``'s three-calendar-day window and therefore
            off the §7.1 position/sector counts. A moved stop or a changed quantity is NEW
            information and passes, and so does an earlier exit that has EXPIRED unactioned — see
            :meth:`_delivered_exit_today`.

        Both screens suppress the whole event, which on (b) also costs a possible ``ModifyStopAction``
        (the other R3 output a position event may return). That is intended rather than overlooked:
        (b) only holds while a LIVE "close the whole position at market" instruction the owner has
        not acted on is already standing, and a full exit dominates a tighter stop on the same
        position — there is nothing a tighten protects that closing does not. The moment that exit
        expires, is dismissed, or stops describing the row, the adjust path is available again.
        """
        position_id = str(position["position_id"])
        today = self._clock.today()
        missing = self._sold_outside_ledger(today)
        if position_id in missing:
            if self._sold_outside_logged.get(position_id) != today:
                self._sold_outside_logged[position_id] = today
                # KNOWN INTERACTION, deliberately logged rather than silently absorbed: with no exit
                # recommendation being delivered, this position ages out of ``gate._exiting_symbols``
                # (a 3-calendar-day window over DELIVERED exit recs, gate.py:1170) about three days
                # from now, and then starts counting against §7.1 ``max_open_positions`` /
                # ``per_sector_exposure`` again until the owner replies /closed or the §7.1
                # ``max_holding`` sweep begins issuing its deterministic daily exit. That is a
                # CAPACITY cost, never a risk one (the caps get tighter, not looser) — and teaching
                # the gate this predicate would relax a §7.1 limit on the strength of a broker
                # heuristic, which is an owner decision, not this screen's to make. WO-D2 open
                # question 1.
                _log.warning(
                    "position_event_skipped_sold_outside_ledger",
                    position_id=position_id, symbol=str(position["symbol"] or ""),
                    effect="no analyst call and no exit recommendation while the broker holds none "
                           "of it; reply /closed to settle the ledger. After ~3 days with no "
                           "delivered exit rec it re-enters the O16 position/sector counts.",
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

    def _sold_outside_ledger(self, today: date) -> set[str]:
        """§3.6 journal read (:func:`positions_missing_from_holdings`), never cached for the day.

        ``require_zero=True``: on THIS path the answer buys silence on a risk-reducing output, so
        only "the broker holds nothing on every day of the run" qualifies. A partial exit or a
        pledged holding reads short but still has exposure to protect (see :meth:`_skip_position_event`).

        The hourly reconcile REWRITES today's observation (last write of the day wins), so a position
        that comes back into holdings at 13:00 must stop being skipped at 13:00 — a day-keyed cache
        would hold the morning's answer until midnight. A read failure degrades to "nothing is
        missing" (D7 fail-to-zero): an unreadable journal must never silence the exit path.
        """
        try:
            return positions_missing_from_holdings(self._conn, today, require_zero=True)
        except sqlite3.Error as exc:
            _log.warning("sold_outside_ledger_read_failed", error_type=type(exc).__name__,
                         error=str(exc)[:200])
            return set()

    def _delivered_exit_today(self, position: sqlite3.Row, today: date) -> str | None:
        """``rec_id`` of today's latest exit recommendation for ``position`` if it STILL STANDS.

        "Still stands" is two conditions, and both are needed:

        1. **It still describes the position.** The delivered payload's ``stop`` and ``qty`` equal
           the position row's current ones. Those two are the whole content of an exit recommendation
           (:meth:`_manage_recommendation` copies both straight off the row), so identical values
           mean an identical message.
        2. **It is still an instruction the owner can act on.** An exit that the §3.6 TTL sweep has
           already expired (``human_action='expired'``, or ``valid_until`` passed and the sweep has
           simply not run yet) said its piece and is gone from ``/pending``; suppressing the next
           event on its strength would leave an intraday position whose stop stayed breached all
           session with ONE exit instruction that died 20 minutes after it arrived
           (``TTL_INTRADAY_MIN``). A swing/position exit is stamped to the session close, so this
           changes nothing there — it restores the intraday cadence only. ``taken``/``dismissed``/
           ``closed`` DO suppress: the owner engaged with the message, and repeating it is the noise
           WO-D2 exists to remove.

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
            # D7 fail-to-zero, and the same posture as :meth:`_sold_outside_ledger`: this read exists
            # only to WITHHOLD a risk-reducing output, so a database that cannot answer it must not
            # decide the question. Raising here would also abandon every remaining position on this
            # bar — ``on_bar`` awaits this inside its per-position loop.
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
        # WO-D2 exit hygiene. THE one place every position-event entry point converges on, so the two
        # "this call would tell the owner nothing new" screens live HERE rather than in :meth:`on_bar`
        # — a later scheduled/hourly position review that reaches this method inherits them for free.
        # Both run BEFORE the governor admission so a suppressed event costs neither an analyst call
        # nor a §5.6 budget decision.
        if self._skip_position_event(position):
            return
        decision = self._governor.can_invoke(INTRADAY_AGENT_ID, "position_event")
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
            # R3: a position event may only yield risk-reducing output. The prompt says so; this is
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
            # WO-24a on trigger (b): unguarded this came out of on_bar, i.e. out of the tick handler
            # for every symbol, not just this position.
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

    # ================================================================== §7.1 max_holding
    async def check_aged_positions(self, d: date) -> int:
        """§7.1 ``max_holding`` zombie protection — run EOD and as a startup catch-up (§2.6).

        DETERMINISTIC: the ``ExitAction`` is built in Python and the harness is never called. R1 —
        an exit that only happens when a model answers is not an exit. Returns how many exit
        recommendations were issued. A ``GateContextTimeout`` on one position is alerted and the
        sweep continues, but it is re-raised once the loop is done so the job's watermark records a
        failure and the catch-up retries — a swallowed timeout would defer a §7.1 exit by a day.
        """
        table = self._limits.load()
        caps = {
            "swing": int(table.limits.max_holding.swing_trading_days),
            "position": int(table.limits.max_holding.position_trading_days),
        }
        rows = self._conn.execute(
            "SELECT * FROM positions WHERE state='OPEN' AND origin='recommended'"
        ).fetchall()
        issued = 0
        timed_out: GateContextTimeout | None = None
        for position in rows:
            style = str(position["style"] or "")
            cap = caps.get(style)
            if cap is None:                       # intraday is squared off by the window, not by age
                continue
            age = self._session_age(position["opened_at"], d)
            if age is None or age <= cap:
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
                # One stalled position may not take the sweep down with it: the aged positions
                # BEHIND it still have to be looked at (WO-24a). Re-raised after the loop.
                await self._handle_gate_context_timeout_position(position, action, exc)
                timed_out = timed_out or exc
                continue
            if verdict.verdict not in ("approve", "shrink"):
                continue
            rec = self._manage_recommendation(action, verdict, position, None)
            self._book.deliver(
                rec, ledger_fields=self._manage_ledger_fields(action, verdict, position)
            )
            await self._send(catalog.recommendation_message(rec))
            issued += 1
            _log.warning("max_holding_exit_recommended", position_id=position["position_id"],
                         style=style, age_sessions=age, cap=cap, rec_id=rec.rec_id)
        # The §7.1 sweep is this class's own end-of-session hook, so it is where the WO-8 read
        # ledger gets emitted — one line per session, no new wiring in the composition root.
        self.log_hot_path_stats("eod")
        if timed_out is not None:
            raise timed_out
        return issued

    # ================================================================== trigger (c): heartbeat
    async def heartbeat(self) -> None:
        """§5.2 trigger (c) — regime context only, in-window only, may never propose."""
        d = self._clock.today()
        window = self._window(d)
        if window is None or not (window[0] <= self._clock.now() <= window[1]):
            return
        decision = self._governor.can_invoke(INTRADAY_AGENT_ID, "heartbeat")
        if not decision.allowed:
            return
        actx = self._assembler.for_heartbeat(
            regime_lines=self._headroom_lines(d),
            open_positions_summary=self._positions_summary(),
        )
        proposal_id = str(ULID())
        valid_until = self._ttl("intraday")
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
            await self._alert_agent_failed("heartbeat", result)
            return
        self._note_agent_ok()
        payload = result.payload
        if isinstance(payload, NoActionOutput) and payload.regime_note:
            self._assembler.set_regime_note(payload.regime_note)

    # ================================================================== shared machinery
    async def _gate_and_persist(
        self, action: Any, symbol: str, side: str, style: str, d: date
    ) -> tuple[GateVerdict, Any]:
        """Persist the proposal, build the gate context, evaluate, persist the verdict (R8).

        Both rows exist whatever the verdict — a rejection IS the audit trail (§3.4). The context is
        returned alongside so a caller can price off the SAME facts the gate judged on. Publishing
        the verdict on the bus is the caller's wiring; this class holds no bus handle.

        WO-24a: the context build — the ONLY store-touching await on this path — carries a deadline.
        Nothing else here is wrapped: :meth:`_persist_proposal`/:meth:`_persist_verdict` are local
        SQLite writes and ``self._gate.evaluate`` is pure CPU, so a stall can only ever be the build.
        Blowing it raises :class:`GateContextTimeout` and leaves the proposal row orphaned on
        purpose (see that class); the caller decides what the trigger path does about it.
        """
        self._persist_proposal(action)
        try:
            gate_ctx = await asyncio.wait_for(
                self._ctx_builder.build(symbol, side, style, d), _GATE_CONTEXT_DEADLINE_S
            )
        except TimeoutError as exc:      # 3.12: asyncio.TimeoutError IS the builtin TimeoutError
            raise GateContextTimeout(symbol, str(action.proposal_id)) from exc
        verdict = self._gate.evaluate(action, gate_ctx)
        self._persist_verdict(verdict)
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

    def _persist_verdict(self, verdict: GateVerdict) -> None:
        self._conn.execute(
            "INSERT INTO verdicts (verdict_id, proposal_id, verdict, payload, evaluated_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (
                verdict.verdict_id, verdict.proposal_id, verdict.verdict,
                _json_payload(verdict.model_dump(mode="json")), verdict.evaluated_at.isoformat(),
            ),
        )

    async def _request_owner_approval(
        self, action: Any, verdict: GateVerdict, *, symbol: str | None = None
    ) -> None:
        """Route a gate ``owner_approval_required`` verdict to the owner (§3.4). The row is the
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
        self, action: Any, verdict: GateVerdict, position: sqlite3.Row, ltp: Decimal | None
    ) -> Recommendation:
        """Build an ``exit``/``adjust`` recommendation for an already-open recommended position.

        §3.6: these carry a position reference + urgency rather than entry fields, but the payload
        model is one shape — so the entry zone degenerates to the reference price and the size is the
        position's own, never a new one.

        ``side`` on an ``exit`` is the side of the order the OWNER must place (the closing side), not
        the position's — the message says "SELL RELIANCE" for a long being closed, which is what the
        human types into their terminal. An ``adjust`` places no new order, so it keeps the position's
        side. Closing never adds tail risk, so the C8 short flag is off on an exit.
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
        checklist = (
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
        """₹ risk budget for one entry of ``candidate`` under §7.1 ``per_trade_risk`` — the numerator
        :meth:`_max_qty_by_risk` divides by the stop distance. Split out (2026-08-14, the
        qty-zero-at-admission extension below) so ``candidate_unsizeable_qty_zero`` can log WHY a
        candidate was unsizeable, not just the fact — same formula, no new arithmetic."""
        table = self._limits.load()
        ptr = table.limits.per_trade_risk
        pct = _dec(ptr.intraday_pct if candidate.style == "intraday" else ptr.swing_position_pct)
        return pct / _HUNDRED * self._exposure.equity()

    def _max_qty_by_risk(self, candidate: SignalCandidate) -> int:
        """``equity × per_trade_risk% / unit_risk``, floored at an int ≥ 0 — the GATE's formula (§7.1).

        ``unit_risk`` = |entry − stop|, ×``overnight_gap_mult`` for swing/position exactly as the
        §7.1 ``per_trade_risk`` rule charges it. This number is quoted to the analyst, and prompt
        rule 7 binds proposals to it — so it must MIRROR the gate, not approximate it: the original
        version omitted the gap multiplier ("informational, not an authorisation"), and the gate's
        first three live verdicts (2026-08-26) all rejected on ``per_trade_risk`` because every
        swing proposal arrived sized ~2.5× over the real budget. A quoted cap the gate always
        shrinks is not information, it is a trap. A candidate with no stop level is unpriceable for
        risk ⇒ 0, never "unbounded"; a swing whose gap-charged unit risk exceeds the whole budget
        now reads 0 here and takes the unsizeable path before spending an analyst call.
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
        """Platform-stamped ``valid_until`` (§3.2 — the model never emits a time).

        Intraday: ``now + TTL_INTRADAY_MIN`` — a breakout read goes stale in minutes. Swing/position:
        today's session close, because the decision is about the day, not the minute; past the close
        (or on a non-trading day) it falls back to the intraday TTL rather than minting a dead one.
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

    def _session_age(self, opened_at: Any, d: date) -> int | None:
        """TRADING sessions elapsed since ``opened_at`` (exclusive) through ``d`` (inclusive)."""
        try:
            opened = datetime.fromisoformat(str(opened_at)).date()
        except (TypeError, ValueError):
            return None
        if opened >= d:
            return 0
        sessions = 0
        probe = opened
        while probe < d:
            probe = probe + timedelta(days=1)
            if self._calendar.is_trading_day(probe):
                sessions += 1
        return sessions

    # ------------------------------------------------------------------ WO-8 hot-path read hygiene
    def _atr_1m(self, bar: Bar) -> Decimal | None:
        """ATR(14,1m) for ``bar``'s symbol, CARRIED FORWARD instead of re-read per bar (WO-8 (i)).

        Until 2026-08-13 this issued one ``get_bars_1m`` per bar per symbol carrying an open
        recommended position — a store read on the tick-driven bar path for a number that changes
        by one Wilder step. The state machine, in the order the branches are tested:

        * **no state / new bar day / the bar's minute is more than one minute past the last one
          seen ⇒ SEED (or RESEED) from the store.** The gap branch is the load-bearing one: a
          missed bar means the carried ATR is missing a true range, and silently continuing the
          recursion would bake that hole in for the rest of the session. Reseeding re-reads the
          real bars — including the ones this process never saw — so a gap costs one store read
          and nothing else. A bar day change reseeds for the same reason (§3.2: 09:15 is not the
          minute after 15:29).
        * **exactly one minute past ⇒ advance the recursion** (:meth:`_advance_atr`).
        * **the same minute, or older ⇒ return the carried value untouched.** A re-published or
          replayed bar is already folded in and the state is at least as fresh as the bar; folding
          it again would double-count its true range.
        * **still behind after a seed** (the store lags the bus by more than a minute) ⇒ the
          store's own value is returned, exactly as the pre-WO-8 code did, and the next bar
          reseeds. Degrading to "one read per bar" is the floor here, never a stale carry.

        Returns None when the store cannot supply ``ATR_PERIOD`` bars — unchanged behaviour, and
        :meth:`on_bar` treats it as "no reading, no event".
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
        session did — the pre-WO-8 code read a now-anchored window, which differs whenever the bar
        path runs behind the clock.
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

        ``indicators._true_range`` is module-private by convention, not by contract, and it is
        called here ON PURPOSE: the live incremental step and the vectorized backtest path must
        share ONE definition of true range. Re-deriving ``max(H−L, |H−C₋₁|, |L−C₋₁|)`` locally
        would be a second copy of §6.1 math that nothing forces to stay in step.
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
        """``symbol -> sector`` for ``d``, read from the store ONCE per trading date (WO-8 (ii)).

        The §2.4 sector map is a WEEKLY snapshot; reading it once per candidate was a per-analyst-
        call store read of a value that cannot move inside a session. Two things are deliberately
        NOT cached, because caching them would freeze a transient into the rest of the day: a read
        that raised (returns None ⇒ "unavailable", and the next candidate retries), and an EMPTY
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
        except Exception:                # noqa: BLE001 - a thinner context, never a failed call (D7)
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
        """The current session's hot-path read ledger — reads avoided vs reads performed (WO-8)."""
        counts = dict(self._hot_counts)
        return {
            "d": self._hot_path_day.isoformat() if self._hot_path_day else None,
            **counts,
            **{k: round(v, 3) for k, v in self._hot_ms.items()},
            "store_reads_avoided": counts["atr_incremental"] + counts["sector_cache_hits"],
            "store_reads_performed": counts["atr_store_reads"] + counts["sector_store_reads"],
        }

    def log_hot_path_stats(self, reason: str = "session") -> None:
        """The one structured line that makes WO-8's before/after quantifiable in the live log."""
        _log.info("hot_path_read_hygiene", reason=reason, **self.hot_path_stats())

    # ------------------------------------------------------------------ context lines (D7: never raise)
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
        counts = self._exposure.open_position_counts()
        if counts.total == 0:
            return "none open"
        return f"{counts.total} open (MIS {counts.mis}, CNC {counts.cnc})"

    def _sector_exposure_line(self, d: date) -> str:
        sector_of = self._sector_of(d)
        if sector_of is None:            # read failed ⇒ a thinner context, never a failed call (D7)
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
        """Entry recommendations ISSUED today — the Phase-2 ``max_new_trades_day`` basis (§3.6): in
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
        """A successful analyst call closes its alert episodes (WO-25b) — the next failure is news
        again, not a repeat of the last outage."""
        self._agent_alert_episodes.reset(INTRADAY_AGENT_ID)

    async def _alert_agent_failed(self, trigger: str, result: Any) -> None:
        """D7: every Tier-1 failure resolves to no-proposal + an owner alert. Best-effort — a failed
        send must not turn a no-proposal into an exception on the trigger path.

        WO-25b throttles the ALERT, never the failure handling: the ``intraday_agent_failed`` WARNING
        below still fires on every occurrence, and the caller still returns no-proposal. Repeats of the
        same ``(agent, reason)`` inside :data:`~engine.notify.episodes.REPEAT_AFTER` are logged as
        suppressed instead of buzzing the owner again — a dead SDK produced one "Intraday analyst
        unavailable" per trigger for hours, which is queue-building noise, not new information. The
        key deliberately excludes ``trigger``: a heartbeat and a candidate failing on the same reason
        are one outage, and the alert body already says the analyst is unavailable.
        """
        _log.warning("intraday_agent_failed", trigger=trigger, reason=getattr(result, "reason", None),
                     detail=getattr(result, "detail", None), call_id=getattr(result, "call_id", None))
        reason = str(getattr(result, "reason", "unknown"))
        if not self._agent_alert_episodes.should_alert(
            (INTRADAY_AGENT_ID, reason), self._clock.now()
        ):
            _log.info("intraday_agent_alert_throttled", trigger=trigger, reason=reason,
                      note="repeat inside the episode window; logged, not alerted (WO-25b)")
            return
        await self._send(CatalogMessage(
            kind=MessageKind.LIMIT_BREACH,
            title="Intraday analyst unavailable",
            body=(
                f"{trigger}: the analyst call failed ({getattr(result, 'reason', 'unknown')}). "
                "No proposal was produced and nothing was recommended (D7). Deterministic exits, "
                "stops and square-offs are unaffected (R1)."
            ),
            severity="warning",
            data={
                "rule_id": "agent_failed", "trigger": trigger,
                "value": str(getattr(result, "reason", "unknown")),
                "limit": "a Tier-1 failure resolves to no-proposal + alert (D7)",
                "call_id": str(getattr(result, "call_id", "")),
            },
        ))

    async def _send(self, message: CatalogMessage) -> None:
        if self._notify is None:
            return
        try:
            await self._notify(message)
        except Exception:                # noqa: BLE001 - a notification never breaks the pipeline (R8)
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
