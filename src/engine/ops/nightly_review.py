"""Nightly Post-Trade Reviewer job (§5.5 / §4.4 ``nightly_review``, 21:00 on trading days).

DESIGN DEVIATION (architect decision, Phase-2 v1 — see the module docstring of
:mod:`engine.intelligence.agents.nightly` and the note beside ``nightly_reviewer`` in
``config/agents.yaml``). §5.5 specifies an AGENTIC reviewer that pulls its own evidence through
read-only MCP tools. v1 is SINGLE-SHOT over a context assembled deterministically HERE: at Phase-2
volume (a handful of recommendations a day, no auto trades) the entire reviewable day fits in one
prompt, so the tool loop adds token cost and an unverified SDK MCP surface for no analytical gain.
The §5.5 agentic shape and its toolset land in Phase 3. The §5.5 OUTPUT CONTRACT is unchanged, so the
upgrade replaces only the harness entry point — never the ``nightly_reviews`` row shape or its readers.

Three properties this module is responsible for:

* **The suggestible set is the envelope, and nothing else (R4/§6.3).** The context shows the model the
  ``envelope.yaml`` parameter names WITH their bounds, and every returned suggestion is re-checked
  against that same list after validation: an unknown name or an out-of-bounds value is DROPPED and
  logged. Suggestions are evidence for §6.4 step 1 and for the owner's ``POST /config/params`` — no
  code path in this module (or downstream of it) applies one.
* **Assembly is deterministic and side-effect free.** :func:`build_review_context` is a pure read: it
  never calls an LLM, never writes, and renders every temporal value as precomputed text (§3.2 — the
  model never computes a date). A day with nothing in it renders as explicit "none" lines rather than
  an empty prompt, so "quiet day" and "broken query" are distinguishable in the audit snapshot (R8).
* **A failure is visible.** A governor block, a harness failure or an unverifiable envelope all resolve
  to a logged no-review — never a partial row, never a raised exception into the scheduler.
"""

from __future__ import annotations

import gzip
import json
import sqlite3
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Any

from engine.core.calendar import NSECalendar
from engine.core.clock import Clock
from engine.core.config import config_dir, load_yaml
from engine.core.db import transaction
from engine.core.log import get_logger
from engine.intelligence.agents import nightly
from engine.intelligence.context import AssembledContext
from engine.intelligence.governor import BudgetGovernor
from engine.intelligence.harness import AgentDef, AgentHarness
from engine.intelligence.schemas import NightlyReview, ParamSuggestion
from engine.notify.catalog import CatalogMessage, MessageKind
from engine.ops.jobs import AdvisoryOutcome

_log = get_logger("engine.ops.nightly_review")

NotifySink = Callable[[CatalogMessage], Awaitable[None]]

#: The protected config that defines the ONLY suggestible parameter set (§6.3/R4).
ENVELOPE_FILE = "envelope.yaml"

#: agents.yaml trigger name for this agent (its only one) — the governor's admission class.
CALL_CLASS = "schedule"

_UNKNOWN = "unknown"
_NONE = "none"
_NO_TRADES = "no trades closed today"

_HEADER = "== TRADING DAY REVIEW (volatile) =="
_TRADES_TITLE = "closed trades (learning ledger)"
_RECS_TITLE = "recommendations delivered"
_ENVELOPE_TITLE = "suggestible parameters (envelope.yaml - the ONLY names you may propose)"

#: WO-9 funnel section heading (also the log event's human anchor).
FUNNEL_TITLE = "signal funnel utilization (3.2.5 admission -> 5.2(a) analyst slots)"

#: Analyst-stage section heading (2026-08-14). The funnel above stops at "evaluated"; on the first
#: live day under the ranked funnel that number was 12 and proposals were 0, and NOTHING in the
#: review said why — the reviewer flagged exactly that blind spot. This section is the why.
DECLINES_TITLE = "analyst decisions on forwarded candidates (5.2(a) evaluated -> declined)"

#: The §5.2(a) analyst id and its candidate-trigger call class, as written to ``agent_calls``.
_SIGNAL_AGENT_ID = "intraday_analyst"
_SIGNAL_TRIGGER = "signal_candidate"

#: How many of the day's declines are quoted, newest first, and how far each reason is truncated.
#: Five is the reviewer's working set — enough to see whether the declines RHYME ("premature",
#: "no volume") without turning the funnel section into a transcript; the full text of every call
#: is in ``agent_calls`` for anyone who needs it (R8).
DECLINE_REASONS_SHOWN = 5
DECLINE_REASON_CHARS = 200

#: How :meth:`~engine.intelligence.context.ContextAssembler.for_signal` renders the candidate — one
#: compact key-sorted JSON object on its own line. It is the ONLY place a decline's subject survives
#: (see :func:`_call_identity`), so this prefix is a real coupling and not a convenience.
_CANDIDATE_LINE_PREFIX = "candidate: "

_UNMEASURED = "unmeasured"


# --------------------------------------------------------------------------- small render helpers
def _txt(value: Any) -> str:
    """A context field as text; a NULL column renders as ``unknown``, never as ``None``."""
    return _UNKNOWN if value is None else str(value)


def _flag(value: Any) -> str:
    """An INTEGER 0/1 ledger flag as yes/no (``unknown`` when the column was never written)."""
    if value is None:
        return _UNKNOWN
    return "yes" if int(value) else "no"


def _section(title: str, lines: Sequence[str]) -> str:
    """``title: none`` when empty — an absent section must be visibly absent, not missing (D7)."""
    if not lines:
        return f"{title}: {_NONE}"
    return "\n".join([f"{title}:", *lines])


def _day_prefix(d: date) -> str:
    """Match an ISO-8601 IST timestamp column on its DATE prefix.

    Every timestamp in the state DB is a Clock-produced IST string (§4.2), so the first ten
    characters ARE the IST date — no tz arithmetic in SQL, and no naive datetime anywhere.
    """
    return d.isoformat()


# --------------------------------------------------------------------------- deterministic readers
def _closed_trades(conn: sqlite3.Connection, d: date) -> list[sqlite3.Row]:
    """Learning-ledger rows CLOSED on ``d`` (§6.5) with the position's symbol when it still exists."""
    return conn.execute(
        """
        SELECT l.entry_id, l.rec_id, l.strategy_id, l.qty, l.entry_px, l.exit_px, l.net_pnl,
               l.outcome_label, l.close_reason, l.holding_minutes, l.is_paper, l.confidence,
               l.ex_date_effect, l.flagged_day, l.regime_label, l.thesis, p.symbol AS symbol
        FROM learning_ledger l
        LEFT JOIN positions p ON p.position_id = l.position_id
        WHERE l.closed_at IS NOT NULL AND substr(l.closed_at, 1, 10) = ?
        ORDER BY l.closed_at, l.entry_id
        """,
        (_day_prefix(d),),
    ).fetchall()


def _delivered_recommendations(conn: sqlite3.Connection, d: date) -> list[sqlite3.Row]:
    """§3.6 recommendations DELIVERED on ``d``, with whatever the owner did about them."""
    return conn.execute(
        """
        SELECT rec_id, payload, human_action, human_fill_price
        FROM recommendations
        WHERE delivered_at IS NOT NULL AND substr(delivered_at, 1, 10) = ?
        ORDER BY delivered_at, rec_id
        """,
        (_day_prefix(d),),
    ).fetchall()


def _trade_lines(rows: Sequence[sqlite3.Row]) -> list[str]:
    lines: list[str] = []
    for r in rows:
        lines.append(
            f"  - entry_id={_txt(r['entry_id'])} symbol={_txt(r['symbol'])} "
            f"strategy={_txt(r['strategy_id'])} qty={_txt(r['qty'])} "
            f"entry={_txt(r['entry_px'])} exit={_txt(r['exit_px'])} net={_txt(r['net_pnl'])} "
            f"outcome={_txt(r['outcome_label'])} close_reason={_txt(r['close_reason'])} "
            f"hold_min={_txt(r['holding_minutes'])} paper={_flag(r['is_paper'])} "
            f"ex_date={_flag(r['ex_date_effect'])} flagged_day={_flag(r['flagged_day'])} "
            f"regime={_txt(r['regime_label'])} rec_id={_txt(r['rec_id'])}"
        )
        lines.append(f"    thesis: {_txt(r['thesis'])}")
    return lines


def _rec_lines(rows: Sequence[sqlite3.Row]) -> list[str]:
    lines: list[str] = []
    for r in rows:
        payload: dict[str, Any] = {}
        try:
            loaded = json.loads(r["payload"] or "{}")
            payload = loaded if isinstance(loaded, dict) else {}
        except (TypeError, ValueError):
            # A malformed payload costs this line its detail, never the whole review (D7).
            _log.warning("nightly_rec_payload_unparseable", rec_id=r["rec_id"])
        lines.append(
            f"  - rec_id={_txt(r['rec_id'])} {_txt(payload.get('side'))} "
            f"{_txt(payload.get('instrument'))} qty={_txt(payload.get('qty'))} "
            f"human_action={_txt(r['human_action'])} fill={_txt(r['human_fill_price'])}"
        )
    return lines


def _verdict_lines(conn: sqlite3.Connection, d: date) -> list[str]:
    """Proposal/verdict counts for ``d`` plus rejects grouped by the rule_id that FAILED (§7.1).

    The failing rule is the actionable half of a rejection — "3 rejects" says nothing, "3 rejects,
    all per_trade_risk" says the sizing model and the limit disagree.
    """
    day = _day_prefix(d)
    proposals = conn.execute(
        "SELECT COUNT(*) AS n FROM proposals WHERE substr(created_at, 1, 10) = ?", (day,)
    ).fetchone()["n"]
    rows = conn.execute(
        "SELECT verdict, payload FROM verdicts WHERE substr(evaluated_at, 1, 10) = ?", (day,)
    ).fetchall()

    by_verdict: dict[str, int] = {}
    by_rule: dict[str, int] = {}
    for r in rows:
        verdict = r["verdict"] or _UNKNOWN
        by_verdict[verdict] = by_verdict.get(verdict, 0) + 1
        if verdict != "reject":
            continue
        try:
            payload = json.loads(r["payload"] or "{}")
        except (TypeError, ValueError):
            continue
        for check in (payload.get("checks") or []) if isinstance(payload, dict) else []:
            if isinstance(check, dict) and not check.get("passed"):
                rule = str(check.get("rule_id") or _UNKNOWN)
                by_rule[rule] = by_rule.get(rule, 0) + 1

    counts = " ".join(f"{v}={n}" for v, n in sorted(by_verdict.items())) or _NONE
    lines = [f"  proposals: {proposals}", f"  verdicts: {counts}"]
    if by_rule:
        lines.append("  reject reasons by failing rule_id:")
        # Most frequent first: the rule that killed the most proposals is the one worth reviewing.
        lines.extend(
            f"    - {rule}: {n}" for rule, n in sorted(by_rule.items(), key=lambda kv: (-kv[1], kv[0]))
        )
    else:
        lines.append(f"  reject reasons by failing rule_id: {_NONE}")
    return lines


def _agent_failure_lines(conn: sqlite3.Connection, d: date) -> list[str]:
    """Tier-1 calls that FAILED on ``d`` (R8 audit) — process evidence the reviewer must see."""
    rows = conn.execute(
        """
        SELECT agent_id, fail_reason, COUNT(*) AS n
        FROM agent_calls
        WHERE ok = 0 AND substr(at, 1, 10) = ?
        GROUP BY agent_id, fail_reason
        ORDER BY agent_id, fail_reason
        """,
        (_day_prefix(d),),
    ).fetchall()
    return [f"  - {_txt(r['agent_id'])} {_txt(r['fail_reason'])}: {r['n']}" for r in rows]


def _day_spend(conn: sqlite3.Connection, d: date) -> Decimal:
    """LLM spend booked on ``d`` (D6). Summed in PYTHON: ``SUM()`` on a TEXT money column coerces to
    float and reintroduces exactly the binary-float error decimal-as-string exists to keep out (§8.1)."""
    rows = conn.execute(
        "SELECT cost_usd FROM budget_ledger WHERE substr(at, 1, 10) = ?", (_day_prefix(d),)
    ).fetchall()
    total = Decimal(0)
    for r in rows:
        try:
            total += Decimal(r["cost_usd"] or "0")
        except InvalidOperation:
            _log.warning("nightly_spend_unparseable", value=r["cost_usd"])
    return total.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)


def _envelope_lines(bounds: Mapping[str, Any]) -> list[str]:
    lines: list[str] = []
    for name in sorted(bounds):
        b = bounds[name] if isinstance(bounds[name], Mapping) else {}
        lines.append(
            f"  - {name}: min={_txt(b.get('min'))} max={_txt(b.get('max'))} "
            f"default={_txt(b.get('default'))}"
        )
    return lines


# --------------------------------------------------------------------------- funnel telemetry (WO-9)
def _round(value: float) -> float:
    """Scores are 0..1 informational strengths; two decimals is all the precision they carry."""
    return round(float(value), 2)


def _quantile(sorted_values: Sequence[float], q: float) -> float:
    """Linear-interpolated quantile over an ASCENDING list (no numpy on the reporting path)."""
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return _round(sorted_values[0])
    pos = q * (len(sorted_values) - 1)
    lo = int(pos)
    hi = min(lo + 1, len(sorted_values) - 1)
    frac = pos - lo
    return _round(sorted_values[lo] + (sorted_values[hi] - sorted_values[lo]) * frac)


@dataclass(frozen=True)
class StrategyFunnel:
    """One strategy's slice of the day's funnel (WO-9)."""

    strategy_id: str
    raw: int | None                              # None = the prescreen counters were not wired
    published: int
    forwarded: int
    unsizeable: int                               # no stop level — never eligible for a slot (2026-08-14)
    published_scores: tuple[float, ...]          # ascending
    forwarded_scores: tuple[float, ...]          # ascending
    best_unforwarded_score: float | None

    @property
    def quantiles(self) -> tuple[float, float, float]:
        """(p25, median, p75) of this strategy's PUBLISHED scores — the distribution the forwarded
        set has to beat for WO-1's live acceptance ("forwarded ≥ published median")."""
        return (
            _quantile(self.published_scores, 0.25),
            _quantile(self.published_scores, 0.50),
            _quantile(self.published_scores, 0.75),
        )

    def line(self) -> str:
        p25, med, p75 = self.quantiles
        raw = _UNMEASURED if self.raw is None else str(self.raw)
        best = _NONE if self.best_unforwarded_score is None else f"{self.best_unforwarded_score}"
        forwarded_scores = (
            _NONE if not self.forwarded_scores
            else ",".join(str(s) for s in self.forwarded_scores)
        )
        return (
            f"  - {self.strategy_id}: raw {raw} published {self.published} "
            f"forwarded {self.forwarded} unsizeable {self.unsizeable} | "
            f"published scores p25/med/p75 {p25}/{med}/{p75} "
            f"| forwarded scores {forwarded_scores} | best unforwarded {best}"
        )


@dataclass(frozen=True)
class FunnelSummary:
    """The whole day's origination funnel, assembled from the day-slot journal + the audit tables.

    Every number here was previously reconstructable only by forensic DB reads (WO-9's evidence).
    ``best_unforwarded_score`` is the point of the exercise: it is the direct, single-number answer
    to "did a good candidate never reach the analyst?" — starvation, measured rather than argued.
    """

    d: date
    raw: int | None
    published: int
    forwarded: int
    unsizeable: int                               # no stop level — never eligible for a slot (2026-08-14)
    evaluated: int
    proposals: int
    verdicts: Mapping[str, int]
    best_unforwarded_score: float | None
    by_strategy: tuple[StrategyFunnel, ...] = field(default=())

    def lines(self) -> list[str]:
        if not self.published:
            return ["  nothing published today"]
        verdicts = " ".join(f"{k}={v}" for k, v in sorted(self.verdicts.items())) or _NONE
        raw = _UNMEASURED if self.raw is None else str(self.raw)
        best = _NONE if self.best_unforwarded_score is None else f"{self.best_unforwarded_score}"
        return [
            f"  raw {raw} -> published {self.published} -> forwarded {self.forwarded} -> "
            f"evaluated {self.evaluated} -> proposals {self.proposals}",
            # 2026-08-14: a structurally-unsizeable candidate (no stop) was never eligible for a
            # forward slot — reporting it separately keeps "best unforwarded score" a pure starvation
            # reading instead of conflating it with candidates that could never have been forwarded.
            f"  unsizeable (no stop level, never queued): {self.unsizeable}",
            f"  gate verdicts: {verdicts}",
            f"  best unforwarded score: {best}",
            *(s.line() for s in self.by_strategy),
        ]

    def log_fields(self) -> dict[str, Any]:
        """Flat, JSON-renderable fields for the one structured EOD log line."""
        return {
            "d": self.d.isoformat(),
            "raw": self.raw,
            "published": self.published,
            "forwarded": self.forwarded,
            "unsizeable": self.unsizeable,
            "evaluated": self.evaluated,
            "proposals": self.proposals,
            "verdicts": dict(sorted(self.verdicts.items())),
            "best_unforwarded_score": self.best_unforwarded_score,
            "raw_by_strategy": {s.strategy_id: s.raw for s in self.by_strategy},
            "published_by_strategy": {s.strategy_id: s.published for s in self.by_strategy},
            "forwarded_by_strategy": {s.strategy_id: s.forwarded for s in self.by_strategy},
            "unsizeable_by_strategy": {s.strategy_id: s.unsizeable for s in self.by_strategy},
            "published_score_quantiles": {
                s.strategy_id: list(s.quantiles) for s in self.by_strategy
            },
            "forwarded_scores": {
                s.strategy_id: list(s.forwarded_scores) for s in self.by_strategy
            },
            "best_unforwarded_by_strategy": {
                s.strategy_id: s.best_unforwarded_score for s in self.by_strategy
            },
        }


def read_funnel_raw_counts(conn: sqlite3.Connection, d: date) -> dict[str, int]:
    """``{strategy_id: raw fires}`` persisted for ``d`` in ``funnel_raw_counts``, or ``{}``.

    The restart-proof half of the funnel's top row (2026-08-21): the pre-screen's raw counters are
    flushed here by the forward-drain tick and re-loaded from here whenever a fresh process rolls
    onto the day, so these rows hold the day's total across EVERY process that ran it. An unreadable
    table costs the raw row, never the review (D7) — the same direction the day-slot read takes.
    Also the loader the composition root hands the pre-screen, which is why it lives at module scope.
    """
    try:
        rows = conn.execute(
            "SELECT strategy_id, fires FROM funnel_raw_counts WHERE d = ? ORDER BY strategy_id",
            (_day_prefix(d),),
        ).fetchall()
    except sqlite3.Error as exc:
        _log.warning("funnel_journal_unreadable", d=_day_prefix(d), table="funnel_raw_counts",
                     error=f"{type(exc).__name__}: {exc}")
        return {}
    return {str(r["strategy_id"]): int(r["fires"]) for r in rows}


def build_funnel_summary(
    conn: sqlite3.Connection,
    d: date,
    raw_by_strategy: Mapping[str, int] | None = None,
) -> FunnelSummary:
    """Assemble ``d``'s funnel from ``prescreen_day_slots`` + the proposal/verdict/agent-call audit.

    A pure read, like the rest of this module. The pre-admission scanner count comes from
    ``funnel_raw_counts`` (2026-08-21) in PREFERENCE to ``raw_by_strategy``: the latter is whatever
    the live pre-screen happens to hold, and a mid-day restart zeroes it — the raw row then read
    "unmeasured" for the whole day on 4 of the last 6 trade days before the table existed. The
    in-process value stays as the FALLBACK for a day with no persisted rows (a pre-2026-08-21 day,
    or a session whose counters never flushed), because a number we did not measure and a number
    that was zero are different facts (D7) — with neither source, the raw row renders "unmeasured".
    """
    day = _day_prefix(d)
    try:
        rows = conn.execute(
            "SELECT symbol, strategy_id, score, forwarded, unsizeable FROM prescreen_day_slots "
            "WHERE d = ? ORDER BY strategy_id, symbol",
            (day,),
        ).fetchall()
    except sqlite3.Error as exc:
        # An unreadable journal costs the funnel section, never the review (D7).
        _log.warning("funnel_journal_unreadable", d=day, error=f"{type(exc).__name__}: {exc}")
        rows = []

    published: dict[str, list[float]] = {}
    forwarded_scores: dict[str, list[float]] = {}
    forwarded_calls: dict[str, int] = {}
    unforwarded: dict[str, list[float]] = {}
    unsizeable_counts: dict[str, int] = {}
    strategies: list[str] = []
    for row in rows:
        sid = str(row["strategy_id"])
        if sid not in published:
            published[sid] = []
            strategies.append(sid)
        is_unsizeable = bool(row["unsizeable"])
        if is_unsizeable:
            unsizeable_counts[sid] = unsizeable_counts.get(sid, 0) + 1
        n_forwarded = int(row["forwarded"] or 0)
        forwarded_calls[sid] = forwarded_calls.get(sid, 0) + n_forwarded
        if row["score"] is None:                  # pre-WO-1 rows carry no score — counted, not faked
            continue
        score = _round(row["score"])
        published[sid].append(score)
        if is_unsizeable:
            # 2026-08-14: journalled before the pipeline's stopless short-circuit even runs — never
            # eligible for a forward slot, so it must not pollute the starvation reading below.
            continue
        (forwarded_scores if n_forwarded else unforwarded).setdefault(sid, []).append(score)

    # Persisted first, in-process second (2026-08-21): the table is the only source that survives a
    # restart, and the review runs at 22:35 — hours after any bounce the day happened to take.
    raw_map = read_funnel_raw_counts(conn, d) or dict(raw_by_strategy or {})
    slices = tuple(
        StrategyFunnel(
            strategy_id=sid,
            raw=raw_map.get(sid),
            published=len(
                [r for r in rows if str(r["strategy_id"]) == sid]
            ),
            forwarded=forwarded_calls.get(sid, 0),
            unsizeable=unsizeable_counts.get(sid, 0),
            published_scores=tuple(sorted(published.get(sid, ()))),
            forwarded_scores=tuple(sorted(forwarded_scores.get(sid, ()))),
            best_unforwarded_score=max(unforwarded[sid]) if unforwarded.get(sid) else None,
        )
        for sid in strategies
    )
    every_unforwarded = [s for scores in unforwarded.values() for s in scores]
    proposals, verdicts = _proposal_verdict_counts(conn, day)
    return FunnelSummary(
        d=d,
        raw=sum(raw_map.values()) if raw_map else None,
        published=len(rows),
        forwarded=sum(forwarded_calls.values()),
        unsizeable=sum(unsizeable_counts.values()),
        evaluated=_analyst_evaluations(conn, day),
        proposals=proposals,
        verdicts=verdicts,
        best_unforwarded_score=max(every_unforwarded) if every_unforwarded else None,
        by_strategy=slices,
    )


def _analyst_evaluations(conn: sqlite3.Connection, day: str) -> int:
    """Forwarded candidates the analyst actually ANSWERED — the step between "we spent a slot" and
    "a proposal exists". Heartbeat/position-event calls are a different trigger and excluded."""
    row = conn.execute(
        "SELECT COUNT(*) AS n FROM agent_calls "
        "WHERE agent_id = ? AND trigger = ? AND ok = 1 AND substr(at, 1, 10) = ?",
        (_SIGNAL_AGENT_ID, _SIGNAL_TRIGGER, day),
    ).fetchone()
    return int(row["n"] if row is not None else 0)


# --------------------------------------------------------------------------- analyst stage (2026-08-14)
def _loads_object(raw: Any) -> dict[str, Any] | None:
    """JSON text -> dict, or ``None`` for anything that is not a JSON object (D7: a malformed row is
    COUNTED, never raised — one unreadable analyst output must not cost the owner their review)."""
    try:
        loaded = json.loads(raw or "")
    except (TypeError, ValueError):
        return None
    return loaded if isinstance(loaded, dict) else None


def _candidate_from_context(blob: Any) -> dict[str, Any]:
    """The ``SignalCandidate`` dict out of an archived prompt (``agent_calls.context_gz``).

    Needed because ``agent_calls`` has no identity columns and a DECLINE's payload has none either:
    ``NoActionOutput`` is ``{action, reason, regime_note, confidence?, thesis?}`` with
    ``extra="forbid"``, so an ``ok=1`` no_action row cannot legally carry a tradingsymbol. The
    prompt is where the subject lives — ``for_signal`` writes the whole candidate as one
    ``candidate: {...}`` line — and the prompt is archived verbatim for replay (R8). Unreadable or
    absent ⇒ ``{}``.
    """
    if not blob:
        return {}
    try:
        text = gzip.decompress(bytes(blob)).decode("utf-8", errors="replace")
    except Exception as exc:  # noqa: BLE001 - a corrupt blob costs one identity, never the review
        _log.warning("nightly_context_unreadable", error=f"{type(exc).__name__}: {exc}")
        return {}
    for line in text.splitlines():
        if line.startswith(_CANDIDATE_LINE_PREFIX):
            return _loads_object(line[len(_CANDIDATE_LINE_PREFIX):]) or {}
    return {}


def _call_identity(row: sqlite3.Row, payload: Mapping[str, Any] | None) -> tuple[str, str]:
    """``(symbol, strategy_id)`` for one analyst call — from the output when it carries identity (an
    ``enter`` proposal does), else from the archived prompt, else ``unknown`` on both."""
    symbol = (payload or {}).get("tradingsymbol")
    strategy = (payload or {}).get("strategy_id")
    if symbol and strategy:
        return str(symbol), str(strategy)
    candidate = _candidate_from_context(row["context_gz"])
    return (
        str(symbol or candidate.get("symbol") or _UNKNOWN),
        str(strategy or candidate.get("strategy_id") or _UNKNOWN),
    )


def _hhmm(at: Any) -> str:
    """``HH:MM`` from an ISO-8601 IST timestamp column — the same no-tz-arithmetic rule as
    :func:`_day_prefix`: the string IS IST, so slicing it is exact."""
    text = str(at or "")
    return text[11:16] if len(text) >= 16 else _UNKNOWN


@dataclass(frozen=True)
class AnalystDeclines:
    """What the analyst DID with the candidates the funnel spent slots on (2026-08-14).

    The funnel ends at ``evaluated``; on 2026-08-14 that was 12 against 0 proposals and the review
    could not say whether the analyst was being disciplined or the candidates were structurally
    wrong. ``by_strategy`` answers "is one scanner producing setups the analyst always refuses?" and
    the quoted reasons answer "and does it always refuse them for the same reason?".
    """

    d: date
    evaluated: int
    declined: int
    proposed: int
    unparseable: int
    by_strategy: Mapping[str, int]
    recent: tuple[str, ...]

    def lines(self) -> list[str]:
        if not self.evaluated:
            return []                       # -> the section renders "none" (a day with no calls)
        by_strategy = " ".join(
            f"{sid}={n}" for sid, n in sorted(self.by_strategy.items(), key=lambda kv: (-kv[1], kv[0]))
        ) or _NONE
        lines = [
            f"  evaluated {self.evaluated} -> declined {self.declined} / proposed {self.proposed}"
            + (f" (unreadable output: {self.unparseable})" if self.unparseable else ""),
            f"  declines by strategy: {by_strategy}",
        ]
        if self.recent:
            lines.append(f"  most recent declines (newest first, {DECLINE_REASON_CHARS} chars):")
            lines.extend(f"    - {entry}" for entry in self.recent)
        else:
            lines.append(f"  most recent declines: {_NONE}")
        return lines


def build_analyst_declines(conn: sqlite3.Connection, d: date) -> AnalystDeclines:
    """Assemble ``d``'s analyst-stage picture from ``agent_calls``. A pure read, like the rest of
    this module, and total over the day's SUCCESSFUL signal-candidate calls — the failed ones are
    already their own section (:func:`_agent_failure_lines`), and conflating "the analyst said no"
    with "the analyst never answered" is precisely the confusion this block exists to end."""
    day = _day_prefix(d)
    try:
        rows = conn.execute(
            "SELECT at, output_json, context_gz FROM agent_calls "
            "WHERE agent_id = ? AND trigger = ? AND ok = 1 AND substr(at, 1, 10) = ? "
            "ORDER BY at, rowid",
            (_SIGNAL_AGENT_ID, _SIGNAL_TRIGGER, day),
        ).fetchall()
    except sqlite3.Error as exc:
        # An unreadable audit costs this section, never the review (the module's D7 convention).
        _log.warning("declines_audit_unreadable", d=day, error=f"{type(exc).__name__}: {exc}")
        rows = []

    declined = proposed = unparseable = 0
    by_strategy: dict[str, int] = {}
    quoted: list[str] = []
    for row in rows:
        payload = _loads_object(row["output_json"])
        if payload is None:
            unparseable += 1
            continue
        if str(payload.get("action") or "") != "no_action":
            proposed += 1
            continue
        declined += 1
        symbol, strategy_id = _call_identity(row, payload)
        by_strategy[strategy_id] = by_strategy.get(strategy_id, 0) + 1
        reason = " ".join(str(payload.get("reason") or _UNKNOWN).split())
        quoted.append(f"{_hhmm(row['at'])} {symbol} ({strategy_id}): {reason[:DECLINE_REASON_CHARS]}")

    return AnalystDeclines(
        d=d,
        evaluated=len(rows),
        declined=declined,
        proposed=proposed,
        unparseable=unparseable,
        by_strategy=by_strategy,
        # Newest first: the last thing the analyst refused is the one the owner still remembers.
        recent=tuple(reversed(quoted[-DECLINE_REASONS_SHOWN:])),
    )


def _proposal_verdict_counts(conn: sqlite3.Connection, day: str) -> tuple[int, dict[str, int]]:
    proposals = conn.execute(
        "SELECT COUNT(*) AS n FROM proposals WHERE substr(created_at, 1, 10) = ?", (day,)
    ).fetchone()["n"]
    counts: dict[str, int] = {}
    for row in conn.execute(
        "SELECT verdict, COUNT(*) AS n FROM verdicts WHERE substr(evaluated_at, 1, 10) = ? "
        "GROUP BY verdict",
        (day,),
    ).fetchall():
        counts[str(row["verdict"] or _UNKNOWN)] = int(row["n"])
    return int(proposals), counts


# --------------------------------------------------------------------------- envelope bounds (R4)
def load_envelope_bounds(store: Any | None) -> dict[str, Any]:
    """The ``envelope.yaml`` ``parameters`` table: name -> ``{min, max, default, used_by}`` (§6.3).

    ``store`` is a :class:`~engine.core.protected_store.ProtectedStore`. The bounds are read
    HASH-VERIFIED at the point of use, exactly as the gate reads its limits (§2.4 item 1). A
    verification failure yields an EMPTY table rather than an unverified fallback: with no verified
    envelope there is no legitimate suggestible set, so every suggestion is dropped and the review
    still runs (fail to zero, never fail closed — D7). With no store wired at all (dev/tests) the
    file is read directly, which is a convenience path and is logged as one.
    """
    loader = getattr(store, "load_verified", None)
    if loader is not None:
        try:
            table = (loader(ENVELOPE_FILE) or {}).get("parameters") or {}
        except Exception as exc:  # noqa: BLE001 - an unverifiable envelope must not kill the review
            _log.warning("nightly_envelope_unverified", error=f"{type(exc).__name__}: {exc}")
            return {}
        return dict(table)
    try:
        table = (load_yaml(config_dir() / ENVELOPE_FILE) or {}).get("parameters") or {}
    except Exception as exc:  # noqa: BLE001
        _log.warning("nightly_envelope_unreadable", error=f"{type(exc).__name__}: {exc}")
        return {}
    _log.warning("nightly_envelope_unprotected_read", detail="no ProtectedStore wired; read from disk")
    return dict(table)


# --------------------------------------------------------------------------- context assembly
def build_review_context(
    conn: sqlite3.Connection,
    store: Any | None,
    d: date,
    envelope_names: Mapping[str, Any] | None = None,
    funnel: FunnelSummary | None = None,
) -> str:
    """The VOLATILE review block for ``d`` — a pure, deterministic read of the state DB.

    Pure in the sense that matters here: no LLM, no network, no writes, and no ``datetime.now()`` —
    given the same rows and the same envelope it returns the same bytes. ``envelope_names`` is the
    already-loaded bounds table; passing it lets the caller guarantee that the list the model was
    SHOWN and the list its suggestions are re-checked against are the same object (they must be, or
    the drop filter would be enforcing a rule the model was never told). When omitted it is loaded
    from ``store`` via :func:`load_envelope_bounds`.

    The trading date itself is NOT in here: it belongs to the caller's stable block (D8 ordering).
    """
    bounds = load_envelope_bounds(store) if envelope_names is None else envelope_names

    trades = _closed_trades(conn, d)
    recs = _delivered_recommendations(conn, d)

    parts = [_HEADER]
    parts.append(
        f"{_TRADES_TITLE}: {_NO_TRADES}" if not trades else _section(_TRADES_TITLE, _trade_lines(trades))
    )
    parts.append(_section(_RECS_TITLE, _rec_lines(recs)))
    parts.append(_section("proposals and gate verdicts", _verdict_lines(conn, d)))
    # WO-9: the reviewer cannot reason about "was the day's best idea even looked at?" from
    # delivered recommendations alone — the funnel above them is where candidates are lost.
    parts.append(_section(
        FUNNEL_TITLE,
        (funnel if funnel is not None else build_funnel_summary(conn, d)).lines(),
    ))
    # 2026-08-14: the funnel's last number is "evaluated", and 12 evaluated -> 0 proposals is a
    # DECISION the review has to be able to see, not a gap it has to infer from silence.
    parts.append(_section(DECLINES_TITLE, build_analyst_declines(conn, d).lines()))
    parts.append(_section("agent call failures", _agent_failure_lines(conn, d)))
    parts.append(f"llm spend today: ${_day_spend(conn, d)}")
    parts.append(_section(_ENVELOPE_TITLE, _envelope_lines(bounds)))
    return "\n".join(parts)


# --------------------------------------------------------------------------- suggestion filter (R4)
def _filter_suggestions(
    suggestions: Sequence[ParamSuggestion], bounds: Mapping[str, Any]
) -> tuple[list[ParamSuggestion], list[tuple[ParamSuggestion, str]]]:
    """Split suggestions into (kept, dropped-with-reason) against the envelope table (§6.3/R4).

    Nothing here applies a value — this only decides what is worth PERSISTING as a suggestion for
    §6.4 step 1 and the owner's ``/config/params`` view. A name outside the envelope or a value
    outside its bounds could never be legally set, so carrying it forward would only invite a human
    to try.
    """
    kept: list[ParamSuggestion] = []
    dropped: list[tuple[ParamSuggestion, str]] = []
    for s in suggestions:
        raw = bounds.get(s.parameter)
        if not isinstance(raw, Mapping):
            dropped.append((s, "not_an_envelope_parameter"))
            continue
        lo, hi = raw.get("min"), raw.get("max")
        if lo is None or hi is None:
            dropped.append((s, "parameter_has_no_bounds"))
            continue
        if not (float(lo) <= s.proposed_value <= float(hi)):
            dropped.append((s, "out_of_bounds"))
            continue
        kept.append(s)
    return kept, dropped


# --------------------------------------------------------------------------- the job
class NightlyReviewJob:
    """Runs the §5.5 nightly review for one trading day and persists it to ``nightly_reviews``.

    Parameters
    ----------
    store:
        :class:`~engine.core.protected_store.ProtectedStore` — the hash-verified source of the
        ``envelope.yaml`` bounds that define the suggestible set (R4). May be ``None`` in dev.
    conn:
        The SQLite state connection: the ledger, recommendations, proposals/verdicts, agent-call
        audit and budget ledger the review is assembled from, and where the review row lands.
    assembler:
        Accepted for construction symmetry with the other Tier-1 jobs and RESERVED for the Phase-3
        agentic upgrade. v1 assembles its own context here — the review has no market-data block, so
        nothing in :class:`~engine.intelligence.context.ContextAssembler` applies to it yet.
    harness / agent_defs / governor:
        The single SDK call site, the loaded ``agents.yaml`` definitions, and the §5.6 admission +
        metering authority.
    clock / calendar:
        The only sources of "now" and of trading-day facts (§3.2 no-naive-datetime).
    notify:
        Owner sink for the ``DAILY_SUMMARY`` message. Unwired ⇒ the review still persists.
    funnel_raw:
        Optional ``day -> {strategy_id: raw candidate count}`` reader (WO-9), normally
        ``SignalPreScreen.raw_counts``. Since 2026-08-21 raw scanner output IS persisted
        (``funnel_raw_counts``) and :func:`build_funnel_summary` prefers those rows — this reader is
        the fallback for a day that has none, which is what makes an un-flushed or pre-2026-08-21
        day still readable. Neither source ⇒ the row reads "unmeasured", never "0".
    """

    def __init__(
        self,
        store: Any | None,
        conn: sqlite3.Connection,
        assembler: Any | None,
        harness: AgentHarness,
        agent_defs: Mapping[str, AgentDef],
        governor: BudgetGovernor,
        clock: Clock,
        calendar: NSECalendar,
        notify: NotifySink | None = None,
        funnel_raw: Callable[[date], Mapping[str, int]] | None = None,
    ) -> None:
        self._store = store
        self._conn = conn
        self._assembler = assembler
        self._harness = harness
        self._defs = dict(agent_defs)
        self._governor = governor
        self._clock = clock
        self._calendar = calendar
        self._notify = notify
        self._funnel_raw = funnel_raw

    # ------------------------------------------------------------------ run (§5.5)
    async def run(self, d: date) -> AdvisoryOutcome:
        """Review day ``d``. ``RAN`` only when a review was validated AND persisted.

        Never raises into the scheduler: a missing definition, a governor block and a harness failure
        each resolve to a logged non-``RAN`` outcome that leaves ``nightly_reviews`` untouched for
        ``d`` — so a retry (the §2.6 date-keyed catch-up) sees an un-reviewed day, not a half-reviewed
        one. WO-14 (c): the outcome distinguishes ``BLOCKED`` (the governor declined — a correct,
        non-retryable outcome; a blind retry loop here spends LLM budget) from ``FAILED`` (roster or
        harness failure — retryable, and the retry re-enters the governor check below).
        """
        # WO-9: the one structured EOD funnel line, emitted BEFORE every early return. The
        # measurement must not depend on the LLM call it measures — a governor-blocked night is
        # exactly the night you want the funnel numbers for.
        funnel = self._log_funnel(d)

        agent_def = self._defs.get(nightly.AGENT_ID)
        if agent_def is None:
            _log.error("nightly_review_no_agent_def", agent=nightly.AGENT_ID, d=d.isoformat())
            return AdvisoryOutcome.FAILED   # roster gap = harness failure: retryable (WO-14 c)

        decision = self._governor.can_invoke(nightly.AGENT_ID, CALL_CLASS)
        if not decision.allowed:
            # Checked BEFORE assembly so a blocked night costs no queries; the harness re-checks and
            # would persist its own governor_blocked audit row if we called anyway (R8).
            _log.warning(
                "nightly_review_blocked",
                d=d.isoformat(),
                tier=decision.tier.value,
                reason=decision.reason,
            )
            return AdvisoryOutcome.BLOCKED

        bounds = load_envelope_bounds(self._store)
        context = AssembledContext.build(
            system_prompt=nightly.SYSTEM_PROMPT,
            stable_block="\n".join(
                ["== DAY CONTEXT (stable) ==", f"trading date: {d.isoformat()} ({d.strftime('%A')})"]
            ),
            volatile_block=build_review_context(self._conn, self._store, d, bounds, funnel=funnel),
            call_class=CALL_CLASS,
        )
        result = await self._harness.run_single_shot(
            agent_def,
            context,
            nightly.parse_output,
            json_schema=nightly.output_json_schema(),
            call_class=CALL_CLASS,
        )
        if not result.ok:
            _log.warning(
                "nightly_review_failed",
                d=d.isoformat(),
                reason=result.reason,
                detail=(result.detail or "")[:300],
                call_id=result.call_id,
            )
            await self._send(self._failure_message(d, result.reason, result.detail))
            return AdvisoryOutcome.FAILED

        review: NightlyReview = result.payload
        kept, dropped = _filter_suggestions(review.param_suggestions, bounds)
        for suggestion, why in dropped:
            # Logged individually: a model repeatedly proposing a knob it was never offered is a
            # prompt problem, and only the per-suggestion record makes that visible.
            _log.warning(
                "nightly_suggestion_dropped",
                d=d.isoformat(),
                parameter=suggestion.parameter,
                proposed_value=suggestion.proposed_value,
                reason=why,
            )
        stored = review.model_copy(update={"param_suggestions": kept})
        self._persist(d, stored)

        trades = len(_closed_trades(self._conn, d))
        recs = len(_delivered_recommendations(self._conn, d))
        _log.info(
            "nightly_review_persisted",
            d=d.isoformat(),
            trading_day=self._calendar.is_trading_day(d),
            call_id=result.call_id,
            trades=trades,
            recommendations=recs,
            lessons=len(stored.lessons),
            process_errors=len(stored.process_errors),
            suggestions=len(kept),
            suggestions_dropped=len(dropped),
        )
        await self._send(self._summary_message(d, stored, trades, recs, len(dropped)))
        return AdvisoryOutcome.RAN

    # ------------------------------------------------------------------ funnel telemetry (WO-9)
    def _log_funnel(self, d: date) -> FunnelSummary:
        """Build ``d``'s funnel summary and emit it as ONE structured log line. Never raises: a
        reporting read must not be able to cost the owner their nightly review (D7)."""
        raw: Mapping[str, int] | None = None
        if self._funnel_raw is not None:
            try:
                raw = self._funnel_raw(d)
            except Exception as exc:  # noqa: BLE001 - an unwired/broken counter costs one row
                _log.warning("funnel_raw_unavailable", d=d.isoformat(),
                             error=f"{type(exc).__name__}: {exc}")
        summary = build_funnel_summary(self._conn, d, raw)
        _log.info("funnel_utilization", **summary.log_fields())
        return summary

    # ------------------------------------------------------------------ persistence
    def _persist(self, d: date, review: NightlyReview) -> None:
        """One row per trading day. REPLACE, not append: a re-run for ``d`` (catch-up or a manual
        redo) must supersede the earlier review, and every call that produced either is already
        preserved verbatim in ``agent_calls`` (R8)."""
        payload = json.dumps(review.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
        with transaction(self._conn):
            self._conn.execute(
                "INSERT OR REPLACE INTO nightly_reviews (d, payload, created_at) VALUES (?, ?, ?)",
                (d.isoformat(), payload, self._clock.now().isoformat()),
            )

    # ------------------------------------------------------------------ owner messages (§3.2.11)
    def _summary_message(
        self, d: date, review: NightlyReview, trades: int, recs: int, dropped: int
    ) -> CatalogMessage:
        """The end-of-day owner digest (``DAILY_SUMMARY``, R8 catalog).

        The suggestion COUNT ships, not the values: the owner applies a parameter through
        ``POST /config/params`` after the §6.4 pipeline has had a look, never off a phone notification.
        """
        return CatalogMessage(
            kind=MessageKind.DAILY_SUMMARY,
            title=f"Nightly review {d.isoformat()}",
            body=(
                f"{review.summary}\n"
                f"trades closed {trades} · recommendations {recs} · lessons {len(review.lessons)} · "
                f"process errors {len(review.process_errors)} · "
                f"parameter suggestions {len(review.param_suggestions)} (dropped {dropped})\n"
                "Suggestions are suggestions: the owner sets parameters via /config/params (R4)."
            ),
            severity="info",
            data={
                "d": d.isoformat(),
                "trades_closed": trades,
                "recommendations": recs,
                "lessons": len(review.lessons),
                "process_errors": len(review.process_errors),
                "param_suggestions": len(review.param_suggestions),
                "param_suggestions_dropped": dropped,
                "attributions": len(review.trade_attributions),
            },
        )

    def _failure_message(self, d: date, reason: str | None, detail: str | None) -> CatalogMessage:
        """The owner expects a nightly message; its silent absence would read as "quiet day" rather
        than "the reviewer did not run". Same catalog kind, warning severity, no review body."""
        return CatalogMessage(
            kind=MessageKind.DAILY_SUMMARY,
            title=f"Nightly review {d.isoformat()} unavailable",
            body=(
                f"The nightly reviewer produced no review ({reason or 'unknown'}). "
                f"{(detail or '')[:200]}\n"
                "The trading day is unaffected; the review can be re-run for this date."
            ),
            severity="warning",
            data={"d": d.isoformat(), "reason": reason, "detail": (detail or "")[:500]},
        )

    async def _send(self, msg: CatalogMessage) -> None:
        if self._notify is None:
            return
        try:
            await self._notify(msg)
        except Exception as exc:  # noqa: BLE001 - notification is best-effort; the row is committed
            _log.warning("nightly_review_notify_failed", error=f"{type(exc).__name__}: {exc}")
