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
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Awaitable, Callable, Mapping
from datetime import date, datetime, timedelta
from decimal import ROUND_FLOOR, ROUND_HALF_UP, Decimal
from typing import Any

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
from engine.core.types import Bar
from engine.features.snapshots import FEATURE_SET_VERSION
from engine.intelligence.schemas import (
    NoActionOutput,
    intraday_guidance_json_schema,
    parse_intraday,
)
from engine.notify import catalog
from engine.notify.catalog import CatalogMessage, MessageKind
from engine.strategy.cost_model import CostModel
from engine.strategy.indicators import wilder_atr
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

#: The deterministic time-stop thesis (§7.1 ``max_holding``). A constant, not model text: R1 says an
#: exit must survive with the LLM dead, so nothing on this path may be generated.
TIME_STOP_THESIS = (
    "Deterministic time stop: this position has exceeded the §7.1 max_holding age for its style. "
    "No model was consulted — exits and stops never depend on an LLM response (R1)."
)

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
        if row["human_action"]:
            raise ValueError(
                f"recommendation {rec_id} is already '{row['human_action']}' — /taken applies only to "
                "an open recommendation"
            )
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
            self._conn.execute(
                "UPDATE learning_ledger SET entry_px=?, qty=?, position_id=? WHERE rec_id=?",
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
                valid_until = datetime.fromisoformat(str(data.get("valid_until") or ""))
            except (TypeError, ValueError, json.JSONDecodeError):
                continue                        # unparseable payload: leave it alone, log-and-skip
            if valid_until.tzinfo is not None and valid_until < now:
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
    ) -> None:
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
        #: position_id -> when its last §5.2(b) event fired (in-process debounce).
        self._last_position_event: dict[str, datetime] = {}
        #: §5.2(a) analyst forward cap — per-day count of candidates that reached the harness (§5.6).
        self._forwarded_day: date | None = None
        self._forwarded_count = 0

    # ================================================================== trigger (a): entries
    async def on_signal_candidate(self, candidate: SignalCandidate) -> None:
        """§5.2 trigger (a). Also usable directly as a ``signal.candidate`` bus handler.

        Entry-seeking calls fire ONLY inside the owner-set trade window (§1.4 item 11 / §7.1
        ``trade_window``); every earlier check is cheaper still. A governor block fails to
        no-proposal (D7) — silently, because "we did not call" is already an ``agent_calls`` row.
        """
        if self._mode.mode() not in (Mode.RECOMMEND, Mode.AUTO):
            return
        if self._mode.risk_state() != RiskState.NORMAL:
            return
        if self._kill.is_killed():
            return
        d = self._clock.today()
        window = self._window(d)
        if window is None or not (window[0] <= self._clock.now() <= window[1]):
            _log.info("signal_candidate_out_of_window", signal_id=candidate.signal_id,
                      symbol=candidate.symbol)
            return
        decision = self._governor.can_invoke(INTRADAY_AGENT_ID, "signal")
        if not decision.allowed:
            _log.warning("signal_candidate_governor_blocked", signal_id=candidate.signal_id,
                         reason=getattr(decision, "reason", None))
            return
        # §5.2(a) forward cap (≤6 candidates/day to the analyst at DG0, 4 at DG1+ — §5.6): the
        # governor owns the number, this counter the enforcement (2026-07-28 review: it had no
        # consumer, so a volatile day could burn 20 analyst calls). Counts only calls that reach
        # the harness; the coarse prescreen settings cap still bounds candidate PUBLICATION.
        if self._forwarded_day != d:
            self._forwarded_day, self._forwarded_count = d, 0
        cap_fn = getattr(self._governor, "prescreen_forward_cap", None)
        cap = cap_fn() if cap_fn is not None else None
        if cap is not None and self._forwarded_count >= int(cap):
            _log.info("signal_candidate_forward_cap", signal_id=candidate.signal_id,
                      forwarded=self._forwarded_count, cap=int(cap))
            return
        self._forwarded_count += 1

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
            await self._alert_agent_failed("signal_candidate", result)
            return
        payload = result.payload
        if isinstance(payload, NoActionOutput):
            if payload.regime_note:
                self._assembler.set_regime_note(payload.regime_note)
            _log.info("signal_candidate_no_action", signal_id=candidate.signal_id,
                      reason=payload.reason)
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

        verdict, gate_ctx = await self._gate_and_persist(
            payload, candidate.symbol, str(getattr(payload, "side", candidate.side)),
            candidate.style, d,
        )
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
        await self._send(catalog.recommendation_message(rec))

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
        atr = self._atr_1m(bar.symbol)
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

    async def _run_position_event(self, position: sqlite3.Row, ltp: Decimal, atr: Decimal) -> None:
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
        action = result.payload
        if isinstance(action, NoActionOutput):
            return
        if not isinstance(action, (ExitAction, ModifyStopAction)):
            # R3: a position event may only yield risk-reducing output. The prompt says so; this is
            # the structural half of the same rule.
            _log.warning("position_event_illegal_action", action=getattr(action, "action", None),
                         position_id=position["position_id"])
            return

        verdict, _ = await self._gate_and_persist(
            action, str(position["symbol"]), str(position["side"] or "BUY"), style,
            self._clock.today(),
        )
        if verdict.verdict == "owner_approval_required":
            await self._request_owner_approval(action, verdict)
            return
        if verdict.verdict not in ("approve", "shrink"):
            return
        rec = self._manage_recommendation(action, verdict, position, ltp)
        self._book.deliver(rec, ledger_fields=self._manage_ledger_fields(action, verdict, position))
        await self._send(catalog.recommendation_message(rec))

    # ================================================================== §7.1 max_holding
    async def check_aged_positions(self, d: date) -> int:
        """§7.1 ``max_holding`` zombie protection — run EOD and as a startup catch-up (§2.6).

        DETERMINISTIC: the ``ExitAction`` is built in Python and the harness is never called. R1 —
        an exit that only happens when a model answers is not an exit. Returns how many exit
        recommendations were issued.
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
            verdict, _ = await self._gate_and_persist(
                action, str(position["symbol"]), str(position["side"] or "BUY"), style, d
            )
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
        """
        self._persist_proposal(action)
        gate_ctx = await self._ctx_builder.build(symbol, side, style, d)
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

    async def _request_owner_approval(self, action: Any, verdict: GateVerdict) -> None:
        """Route a gate ``owner_approval_required`` verdict to the owner (§3.4). The row is the
        pending decision; the message is the prompt. Nothing is applied until ``/approve``."""
        approval_id = str(ULID())
        payload = {
            "action": action.action,
            "proposal_id": action.proposal_id,
            "verdict_id": verdict.verdict_id,
            "reasons": "; ".join(verdict.reasons) or "gate routed this to the owner",
        }
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
            title=f"Owner approval required: {action.action}",
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
    def _max_qty_by_risk(self, candidate: SignalCandidate) -> int:
        """``equity × per_trade_risk% / |entry − stop|``, floored at an int ≥ 0 (§7.1).

        Informational context for the analyst, not an authorisation: the gate re-derives its own cap
        and additionally charges ``overnight_gap_mult`` on swing/position, so the approved size can be
        smaller than this. A candidate with no stop level is unpriceable for risk ⇒ 0, never
        "unbounded".
        """
        stop = candidate.raw_levels.stop
        if stop is None:
            return 0
        table = self._limits.load()
        ptr = table.limits.per_trade_risk
        pct = _dec(ptr.intraday_pct if candidate.style == "intraday" else ptr.swing_position_pct)
        budget = pct / _HUNDRED * self._exposure.equity()
        return _floor_div(budget, abs(_dec(candidate.raw_levels.entry) - _dec(stop)))

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

    def _atr_1m(self, symbol: str) -> Decimal | None:
        """ATR(14,1m) over the trailing ``ATR_BAR_TAIL`` bars, or None when there is not enough."""
        now = self._clock.now()
        try:
            bars = self._store.get_bars_1m(
                symbol, now - timedelta(minutes=ATR_BAR_TAIL), now + timedelta(minutes=1)
            )
        except Exception:                # noqa: BLE001 - a missing store read never breaks a bar tick
            _log.exception("atr_read_failed", symbol=symbol)
            return None
        tail = list(bars)[-ATR_BAR_TAIL:]
        if len(tail) < ATR_PERIOD:
            return None
        series = wilder_atr(
            [b.high for b in tail], [b.low for b in tail], [b.close for b in tail], ATR_PERIOD
        )
        last = series.iloc[-1] if len(series) else None
        if last is None or last != last:      # NaN check without importing numpy here
            return None
        return _dec(float(last))

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
        try:
            sector_of = {
                str(r.get("symbol")): str(r.get("sector"))
                for r in self._store.get_sector_map(as_of=d)
                if r.get("symbol")
            }
        except Exception:                # noqa: BLE001 - a thinner context, never a failed call (D7)
            _log.warning("sector_map_unavailable", d=d.isoformat())
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

    async def _alert_agent_failed(self, trigger: str, result: Any) -> None:
        """D7: every Tier-1 failure resolves to no-proposal + an owner alert. Best-effort — a failed
        send must not turn a no-proposal into an exception on the trigger path."""
        _log.warning("intraday_agent_failed", trigger=trigger, reason=getattr(result, "reason", None),
                     detail=getattr(result, "detail", None), call_id=getattr(result, "call_id", None))
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
    "INTRADAY_AGENT_ID",
    "PLATFORM_AGENT_ID",
    "POSITION_EVENT_DEBOUNCE_MIN",
    "STOP_PROXIMITY_ATR_MULT",
    "TIME_STOP_THESIS",
    "TTL_INTRADAY_MIN",
    "NotifyFn",
    "RecommendationBook",
    "RecommendationPipeline",
]
