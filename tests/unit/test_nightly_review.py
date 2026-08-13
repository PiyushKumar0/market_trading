"""Nightly Post-Trade Reviewer (§5.5): deterministic context assembly, the R4 suggestion filter,
and the persist/notify contract.

The harness is faked (the SDK is never reachable from the unit tier) but the VALIDATOR the job hands
it is the real one, so a payload that would not survive ``NightlyReview`` cannot pass these tests
either. The governor and the agent definitions are the real ones loaded from the shipped
``config/agents.yaml`` — "the nightly reviewer is admitted at DG0 and blocked at DG4" is a property of
that config, not of a stub.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from typing import Any

import pytest

from engine.core.calendar import NSECalendar
from engine.core.config import config_dir, load_yaml
from engine.intelligence.agents import nightly
from engine.intelligence.governor import BudgetGovernor
from engine.intelligence.harness import AgentDef, AgentResult, load_agent_defs
from engine.notify.catalog import CatalogMessage, MessageKind
from engine.ops.nightly_review import (
    FUNNEL_TITLE,
    NightlyReviewJob,
    build_funnel_summary,
    build_review_context,
    load_envelope_bounds,
)

D = date(2026, 6, 17)          # the conftest FIXED_NOW trading day
EMPTY_DAY = date(2026, 6, 18)

# A two-row slice of the real envelope.yaml table — the ONLY suggestible set for these tests (§6.3).
BOUNDS: dict[str, Any] = {
    "cat.rr_target": {"min": 1.0, "max": 3.0, "default": 1.5, "used_by": "cat"},
    "orb.orb_minutes": {"min": 15, "max": 45, "default": 30, "used_by": "orb"},
}

REVIEW = {
    "lessons": ["ORB entries taken after the range re-broke gave back the edge."],
    "param_suggestions": [
        {"parameter": "cat.rr_target", "proposed_value": 2.0, "evidence_refs": ["LL-1"]}
    ],
    "process_errors": [],
    "trade_attributions": [
        {"entry_id": "LL-1", "verdict": "thesis_wrong", "note": "Range broke back through entry."}
    ],
    "summary": "One losing ORB trade; the thesis did not hold and the stop did its job.",
}


# --------------------------------------------------------------------------- fakes
class FakeProtectedStore:
    """Stands in for ProtectedStore: hash verification is its own module's test (§2.4)."""

    def __init__(self, parameters: dict[str, Any] | None = None, error: Exception | None = None) -> None:
        self._parameters = BOUNDS if parameters is None else parameters
        self._error = error
        self.loads: list[str] = []

    def load_verified(self, name: str) -> dict[str, Any]:
        self.loads.append(name)
        if self._error is not None:
            raise self._error
        return {"schema_version": 1, "parameters": self._parameters}


@dataclass
class _Call:
    agent_def: AgentDef
    context: Any
    json_schema: dict[str, Any] | None
    call_class: str | None


class FakeHarness:
    """Runs the job's REAL validator over a scripted raw response, or returns a scripted failure."""

    def __init__(self, raw: str | None = None, failure: AgentResult | None = None) -> None:
        self.raw = raw
        self.failure = failure
        self.calls: list[_Call] = []

    async def run_single_shot(
        self,
        agent_def: AgentDef,
        context: Any,
        validate: Any,
        *,
        json_schema: dict[str, Any] | None = None,
        call_class: str | None = None,
    ) -> AgentResult:
        self.calls.append(
            _Call(agent_def=agent_def, context=context, json_schema=json_schema, call_class=call_class)
        )
        if self.failure is not None:
            return self.failure
        return AgentResult.Ok(call_id="CALL-1", payload=validate(self.raw), attempts=1)


# --------------------------------------------------------------------------- fixtures
@pytest.fixture
def calendar(clock) -> NSECalendar:
    return NSECalendar(config_dir() / "calendar", clock, strict=False)


@pytest.fixture
def agent_defs() -> dict[str, AgentDef]:
    return load_agent_defs(load_yaml(config_dir() / "agents.yaml"))


@pytest.fixture
def gov(conn, clock, calendar) -> BudgetGovernor:
    return BudgetGovernor(conn, clock, calendar, load_yaml(config_dir() / "agents.yaml"))


@pytest.fixture
def sent() -> list[CatalogMessage]:
    return []


@pytest.fixture
def notify(sent):
    async def _notify(msg: CatalogMessage) -> None:
        sent.append(msg)

    return _notify


def make_job(conn, gov, clock, calendar, agent_defs, harness, notify=None, store=None) -> NightlyReviewJob:
    return NightlyReviewJob(
        store if store is not None else FakeProtectedStore(),
        conn,
        None,                       # assembler: reserved for the Phase-3 agentic upgrade
        harness,                    # type: ignore[arg-type]
        agent_defs,
        gov,
        clock,
        calendar,
        notify=notify,
    )


def reviews(conn) -> list:
    return conn.execute("SELECT * FROM nightly_reviews ORDER BY d").fetchall()


# --------------------------------------------------------------------------- seeding
def seed_day(conn, d: date = D) -> None:
    """One closed trade, one delivered rec, two proposals with one reject, two failed calls, spend."""
    ts = f"{d.isoformat()}T11:02:00+05:30"
    conn.execute(
        """
        INSERT INTO positions (position_id, symbol, side, style, product, qty, state, origin)
        VALUES ('POS-1', 'RELIANCE', 'BUY', 'intraday', 'MIS', 10, 'CLOSED', 'platform')
        """
    )
    conn.execute(
        """
        INSERT INTO learning_ledger
            (entry_id, position_id, rec_id, is_paper, strategy_id, thesis, confidence, entry_px,
             exit_px, qty, net_pnl, holding_minutes, close_reason, ex_date_effect, flagged_day,
             regime_label, outcome_label, created_at, closed_at)
        VALUES ('LL-1', 'POS-1', 'REC-1', 0, 'orb', 'Opening-range break on above-average volume.',
                0.62, '1420.50', '1405.00', 10, '-165.00', 42, 'stop', 0, 0, 'trend_down', 'loss',
                ?, ?)
        """,
        (f"{d.isoformat()}T10:20:00+05:30", ts),
    )
    conn.execute(
        """
        INSERT INTO recommendations (rec_id, payload, delivered_at, human_action, human_fill_price)
        VALUES ('REC-1', ?, ?, 'taken', '1421.00')
        """,
        (
            json.dumps({"instrument": "RELIANCE", "side": "BUY", "qty": 10}),
            f"{d.isoformat()}T09:40:00+05:30",
        ),
    )
    for pid in ("P-1", "P-2"):
        conn.execute(
            """
            INSERT INTO proposals (proposal_id, agent_id, action, payload, inputs_digest, created_at)
            VALUES (?, 'intraday_analyst', 'enter', '{}', 'dig', ?)
            """,
            (pid, f"{d.isoformat()}T10:15:00+05:30"),
        )
    conn.execute(
        """
        INSERT INTO verdicts (verdict_id, proposal_id, verdict, payload, evaluated_at)
        VALUES ('V-1', 'P-1', 'approve', ?, ?)
        """,
        (json.dumps({"checks": [{"rule_id": "per_trade_risk", "passed": True}]}),
         f"{d.isoformat()}T10:15:01+05:30"),
    )
    conn.execute(
        """
        INSERT INTO verdicts (verdict_id, proposal_id, verdict, payload, evaluated_at)
        VALUES ('V-2', 'P-2', 'reject', ?, ?)
        """,
        (
            json.dumps(
                {
                    "checks": [
                        {"rule_id": "per_trade_risk", "passed": False},
                        {"rule_id": "margin_buffer", "passed": True},
                    ]
                }
            ),
            f"{d.isoformat()}T10:16:01+05:30",
        ),
    )
    for call_id in ("C-1", "C-2"):
        conn.execute(
            """
            INSERT INTO agent_calls (call_id, agent_id, trigger, model, ok, fail_reason, at)
            VALUES (?, 'intraday_analyst', 'signal_candidate', 'sonnet-4.6', 0, 'schema_invalid', ?)
            """,
            (call_id, f"{d.isoformat()}T10:16:00+05:30"),
        )
    for cost in ("0.0021", "0.0400"):
        conn.execute(
            """
            INSERT INTO budget_ledger (agent_id, model, at, in_tokens, out_tokens, cost_usd, month)
            VALUES ('intraday_analyst', 'sonnet-4.6', ?, 1000, 100, ?, ?)
            """,
            (f"{d.isoformat()}T10:16:00+05:30", cost, d.strftime("%Y-%m")),
        )


# =========================================================================== build_review_context
def test_context_renders_the_seeded_day(conn) -> None:
    # §5.5: the reviewer sees the ledger, the recommendations, the gate's rejects BY FAILING RULE,
    # the failed Tier-1 calls, the day's spend and the envelope — assembled deterministically here.
    seed_day(conn)
    text = build_review_context(conn, None, D, BOUNDS)

    assert text.startswith("== TRADING DAY REVIEW (volatile) ==")
    assert "closed trades (learning ledger):" in text
    assert (
        "  - entry_id=LL-1 symbol=RELIANCE strategy=orb qty=10 entry=1420.50 exit=1405.00 "
        "net=-165.00 outcome=loss close_reason=stop hold_min=42 paper=no ex_date=no "
        "flagged_day=no regime=trend_down rec_id=REC-1"
    ) in text
    assert "    thesis: Opening-range break on above-average volume." in text
    assert "recommendations delivered:" in text
    assert "  - rec_id=REC-1 BUY RELIANCE qty=10 human_action=taken fill=1421.00" in text
    assert "proposals and gate verdicts:" in text
    assert "  proposals: 2" in text
    assert "  verdicts: approve=1 reject=1" in text
    assert "  reject reasons by failing rule_id:" in text
    assert "    - per_trade_risk: 1" in text
    # A rule that PASSED inside a rejected verdict is not a reject reason.
    assert "margin_buffer" not in text
    assert "agent call failures:" in text
    assert "  - intraday_analyst schema_invalid: 2" in text
    assert "llm spend today: $0.0421" in text
    assert "suggestible parameters (envelope.yaml - the ONLY names you may propose):" in text
    assert "  - cat.rr_target: min=1.0 max=3.0 default=1.5" in text
    assert "  - orb.orb_minutes: min=15 max=45 default=30" in text
    # No date anywhere in the volatile block: the trading date lives in the caller's stable block (D8).
    assert D.isoformat() not in text


def test_context_empty_day_says_so_explicitly(conn) -> None:
    # D7: a quiet day must be DISTINGUISHABLE from a broken query — explicit "none"/"no trades" lines,
    # never an empty prompt.
    seed_day(conn)                                  # seeded on D, so EMPTY_DAY must read clean
    text = build_review_context(conn, None, EMPTY_DAY, BOUNDS)

    assert "closed trades (learning ledger): no trades closed today" in text
    assert "recommendations delivered: none" in text
    assert "  proposals: 0" in text
    assert "  verdicts: none" in text
    assert "  reject reasons by failing rule_id: none" in text
    assert "agent call failures: none" in text
    assert "llm spend today: $0.0000" in text
    # The suggestible set is a property of the envelope, not of the day.
    assert "  - cat.rr_target: min=1.0 max=3.0 default=1.5" in text


def test_context_is_byte_stable_for_the_same_rows(conn) -> None:
    seed_day(conn)
    assert build_review_context(conn, None, D, BOUNDS) == build_review_context(conn, None, D, BOUNDS)


def test_envelope_bounds_come_from_the_protected_store(conn) -> None:
    store = FakeProtectedStore()
    assert load_envelope_bounds(store) == BOUNDS
    assert store.loads == ["envelope.yaml"]


def test_unverifiable_envelope_yields_an_empty_suggestible_set() -> None:
    # R4: with no VERIFIED envelope there is no legitimate suggestible set — the review still runs
    # (fail to zero), but every suggestion it makes will be dropped.
    store = FakeProtectedStore(error=RuntimeError("envelope.yaml hash mismatch"))
    assert load_envelope_bounds(store) == {}


# =========================================================================== the job
@pytest.mark.asyncio
async def test_valid_review_is_persisted_and_summarised(conn, gov, clock, calendar, agent_defs, notify, sent) -> None:
    seed_day(conn)
    harness = FakeHarness(raw=json.dumps(REVIEW))
    job = make_job(conn, gov, clock, calendar, agent_defs, harness, notify=notify)

    assert await job.run(D) is True

    rows = reviews(conn)
    assert len(rows) == 1
    assert rows[0]["d"] == D.isoformat()
    payload = json.loads(rows[0]["payload"])
    assert payload["summary"] == REVIEW["summary"]
    assert payload["lessons"] == REVIEW["lessons"]
    assert payload["trade_attributions"] == REVIEW["trade_attributions"]
    assert rows[0]["created_at"] == clock.now().isoformat()

    # The call went out as the shipped single-shot def under the schedule admission class (§5.5 v1).
    assert len(harness.calls) == 1
    call = harness.calls[0]
    assert call.agent_def.agent_id == "nightly_reviewer"
    assert call.agent_def.shape == "single_shot"
    assert call.call_class == "schedule"
    assert call.json_schema == nightly.output_json_schema()
    # D8 ordering: byte-stable system prompt, stable date block first, volatile review last.
    assert call.context.system_prompt == nightly.SYSTEM_PROMPT
    assert call.context.stable_block.splitlines()[1] == "trading date: 2026-06-17 (Wednesday)"
    assert call.context.prompt().startswith(call.context.stable_block)
    assert call.context.prompt().endswith(call.context.volatile_block)
    assert "entry_id=LL-1" in call.context.volatile_block

    assert len(sent) == 1
    msg = sent[0]
    assert msg.kind == MessageKind.DAILY_SUMMARY
    assert msg.severity == "info"
    assert REVIEW["summary"] in msg.body
    assert msg.data["trades_closed"] == 1
    assert msg.data["recommendations"] == 1
    assert msg.data["param_suggestions"] == 1
    assert msg.data["param_suggestions_dropped"] == 0


@pytest.mark.asyncio
async def test_out_of_envelope_and_out_of_bounds_suggestions_are_dropped(
    conn, gov, clock, calendar, agent_defs, notify, sent
) -> None:
    # R4/§6.3: the model may only suggest names it was SHOWN, inside the bounds it was shown. A knob
    # outside the envelope (here a risk limit) or a value outside its range never reaches
    # GET /config/params — and neither is ever APPLIED by any path in this module.
    review = dict(REVIEW)
    review["param_suggestions"] = [
        {"parameter": "cat.rr_target", "proposed_value": 2.0, "evidence_refs": ["LL-1"]},
        {"parameter": "orb.orb_minutes", "proposed_value": 45.0, "evidence_refs": ["LL-1"]},
        {"parameter": "limits.max_daily_loss", "proposed_value": 9000.0, "evidence_refs": ["LL-1"]},
        {"parameter": "cat.rr_target", "proposed_value": 9.0, "evidence_refs": ["LL-1"]},
        {"parameter": "orb.orb_minutes", "proposed_value": 14.0, "evidence_refs": ["LL-1"]},
    ]
    harness = FakeHarness(raw=json.dumps(review))
    job = make_job(conn, gov, clock, calendar, agent_defs, harness, notify=notify)

    assert await job.run(D) is True

    kept = json.loads(reviews(conn)[0]["payload"])["param_suggestions"]
    assert [(s["parameter"], s["proposed_value"]) for s in kept] == [
        ("cat.rr_target", 2.0),
        ("orb.orb_minutes", 45.0),     # the inclusive upper bound is IN bounds
    ]
    assert sent[0].data["param_suggestions"] == 2
    assert sent[0].data["param_suggestions_dropped"] == 3
    # Everything else in the review survives a dropped suggestion — one bad knob name must not void
    # a sound day's lessons.
    assert json.loads(reviews(conn)[0]["payload"])["lessons"] == REVIEW["lessons"]


@pytest.mark.asyncio
async def test_no_envelope_means_every_suggestion_is_dropped(
    conn, gov, clock, calendar, agent_defs, notify, sent
) -> None:
    store = FakeProtectedStore(error=RuntimeError("envelope.yaml hash mismatch (R4)"))
    harness = FakeHarness(raw=json.dumps(REVIEW))
    job = make_job(conn, gov, clock, calendar, agent_defs, harness, notify=notify, store=store)

    assert await job.run(D) is True
    assert json.loads(reviews(conn)[0]["payload"])["param_suggestions"] == []


@pytest.mark.asyncio
async def test_governor_block_makes_no_call_and_writes_no_row(
    conn, gov, clock, calendar, agent_defs, notify, sent
) -> None:
    # §5.6 DG4 (credit exhausted / SDK billing error) => zero SDK calls. The block is checked before
    # any context assembly, so a blocked night costs nothing at all.
    gov.note_billing_error("test billing error")
    harness = FakeHarness(raw=json.dumps(REVIEW))
    job = make_job(conn, gov, clock, calendar, agent_defs, harness, notify=notify)

    assert await job.run(D) is False
    assert harness.calls == []
    assert reviews(conn) == []


@pytest.mark.asyncio
async def test_harness_failure_persists_nothing_and_still_tells_the_owner(
    conn, gov, clock, calendar, agent_defs, notify, sent
) -> None:
    # D7: schema-invalid after retries resolves to no review + an owner message. A silent absence
    # would read to the owner as "quiet day" rather than "the reviewer did not run".
    harness = FakeHarness(
        failure=AgentResult.Failed("schema_invalid", "not valid JSON", call_id="CALL-9", attempts=3)
    )
    job = make_job(conn, gov, clock, calendar, agent_defs, harness, notify=notify)

    assert await job.run(D) is False
    assert reviews(conn) == []
    assert len(sent) == 1
    assert sent[0].kind == MessageKind.DAILY_SUMMARY
    assert sent[0].severity == "warning"
    assert sent[0].data["reason"] == "schema_invalid"


@pytest.mark.asyncio
async def test_rerun_replaces_the_row_for_that_day(conn, gov, clock, calendar, agent_defs) -> None:
    # The §2.6 date-keyed catch-up may replay a missed day, and the owner may redo one. One row per
    # trading day; every call that produced either review stays in agent_calls (R8).
    first = FakeHarness(raw=json.dumps(REVIEW))
    assert await make_job(conn, gov, clock, calendar, agent_defs, first).run(D) is True

    second_review = dict(REVIEW, summary="Re-run: the stop was correct, the entry timing was not.")
    second = FakeHarness(raw=json.dumps(second_review))
    assert await make_job(conn, gov, clock, calendar, agent_defs, second).run(D) is True

    rows = reviews(conn)
    assert len(rows) == 1
    assert json.loads(rows[0]["payload"])["summary"] == second_review["summary"]


@pytest.mark.asyncio
async def test_review_persists_without_a_notify_sink(conn, gov, clock, calendar, agent_defs) -> None:
    harness = FakeHarness(raw=json.dumps(REVIEW))
    job = make_job(conn, gov, clock, calendar, agent_defs, harness, notify=None)
    assert await job.run(D) is True
    assert len(reviews(conn)) == 1


@pytest.mark.asyncio
async def test_missing_agent_def_is_a_logged_no_review(conn, gov, clock, calendar) -> None:
    harness = FakeHarness(raw=json.dumps(REVIEW))
    job = make_job(conn, gov, clock, calendar, {}, harness)
    assert await job.run(D) is False
    assert harness.calls == []
    assert reviews(conn) == []


# =========================================================================== WO-9 funnel telemetry
def seed_funnel(conn, d: date = D) -> None:
    """A day-slot journal shaped like a real starved session: orb published three and got two
    analyst slots, rsi2 published two and got one, and the day's best UNFORWARDED score (0.85)
    outranks one of the candidates that was actually evaluated."""
    rows = [
        # symbol,   strategy, score, forwarded
        ("TCS", "orb", 0.90, 1),
        ("INFY", "orb", 0.70, 1),
        ("WIPRO", "orb", 0.85, 0),
        ("SBIN", "rsi2", 0.40, 1),
        ("ITC", "rsi2", 0.20, 0),
    ]
    for symbol, strategy_id, score, forwarded in rows:
        conn.execute(
            "INSERT INTO prescreen_day_slots "
            "(d, symbol, strategy_id, published_at, evaluated, score, forwarded) "
            "VALUES (?, ?, ?, ?, 1, ?, ?)",
            (d.isoformat(), symbol, strategy_id, f"{d.isoformat()}T10:00:00+05:30",
             score, forwarded),
        )
    for call_id in ("OK-1", "OK-2"):
        conn.execute(
            "INSERT INTO agent_calls (call_id, agent_id, trigger, model, ok, at) "
            "VALUES (?, 'intraday_analyst', 'signal_candidate', 'sonnet-4.6', 1, ?)",
            (call_id, f"{d.isoformat()}T10:05:00+05:30"),
        )
    conn.execute(
        "INSERT INTO agent_calls (call_id, agent_id, trigger, model, ok, at) "
        "VALUES ('HB-1', 'intraday_analyst', 'heartbeat', 'sonnet-4.6', 1, ?)",
        (f"{d.isoformat()}T10:40:00+05:30",),
    )


def test_funnel_summary_aggregates_the_whole_day(conn) -> None:
    """WO-9: the funnel is raw -> published -> forwarded -> evaluated -> proposals -> verdicts, and
    the number that says "a good candidate never reached the analyst" is best_unforwarded_score."""
    seed_day(conn)
    seed_funnel(conn)
    summary = build_funnel_summary(conn, D, raw_by_strategy={"orb": 40, "rsi2": 6})

    assert summary.raw == 46
    assert summary.published == 5
    assert summary.forwarded == 3
    assert summary.evaluated == 2                    # ok signal_candidate calls; heartbeat excluded
    assert summary.proposals == 2                    # from seed_day
    assert summary.verdicts == {"approve": 1, "reject": 1}
    assert summary.best_unforwarded_score == 0.85

    orb = {s.strategy_id: s for s in summary.by_strategy}["orb"]
    assert (orb.raw, orb.published, orb.forwarded) == (40, 3, 2)
    assert orb.published_scores == (0.70, 0.85, 0.90)
    assert orb.forwarded_scores == (0.70, 0.90)
    assert orb.best_unforwarded_score == 0.85
    # p25 / median / p75 of [0.70, 0.85, 0.90], linearly interpolated.
    assert orb.quantiles == (0.77, 0.85, 0.88)


def test_funnel_summary_of_an_empty_day_is_explicitly_empty(conn) -> None:
    """A quiet day and a broken query must stay distinguishable (the module's D7 convention)."""
    summary = build_funnel_summary(conn, EMPTY_DAY)
    assert (summary.published, summary.forwarded, summary.evaluated) == (0, 0, 0)
    assert summary.raw is None                       # never measured is not the same as zero
    assert summary.best_unforwarded_score is None
    assert summary.by_strategy == ()
    assert summary.lines() == ["  nothing published today"]


def test_funnel_section_is_in_the_review_context(conn) -> None:
    seed_day(conn)
    seed_funnel(conn)
    text = build_review_context(conn, None, D, BOUNDS)
    assert f"{FUNNEL_TITLE}:" in text
    assert "published 5 -> forwarded 3 -> evaluated 2 -> proposals 2" in text
    assert "best unforwarded score: 0.85" in text
    assert "- orb: raw unmeasured published 3 forwarded 2" in text


def test_funnel_log_fields_are_flat_scalars(conn) -> None:
    """The EOD line is a STRUCTURED log record — its fields have to survive JSON rendering."""
    seed_funnel(conn)
    fields = build_funnel_summary(conn, D, raw_by_strategy={"orb": 40}).log_fields()
    assert fields["d"] == D.isoformat()
    assert fields["published"] == 5
    assert fields["best_unforwarded_score"] == 0.85
    assert fields["published_by_strategy"] == {"orb": 3, "rsi2": 2}
    assert json.dumps(fields)                        # no Decimal/date/Row leaks into the log line


@pytest.mark.asyncio
async def test_run_emits_the_eod_funnel_line_even_when_it_produces_no_review(
    conn, gov, clock, calendar
) -> None:
    """WO-9 acceptance: the funnel line is emitted BEFORE any early return, so a governor-blocked
    or agent-def-less night still leaves the day's funnel numbers in the log — the measurement must
    not depend on the LLM call it is measuring."""
    seed_funnel(conn)
    job = make_job(conn, gov, clock, calendar, {}, FakeHarness(raw=json.dumps(REVIEW)))
    captured: list[Any] = []
    real = job._log_funnel
    job._log_funnel = lambda d: captured.append(real(d))

    assert await job.run(D) is False                 # no agent def -> earliest possible return
    assert captured and captured[0].forwarded == 3
    assert captured[0].best_unforwarded_score == 0.85
