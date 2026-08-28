"""``cat_reversal`` SHADOW (§2.7, 2026-08-27) — the scanner, and the one property that must not fail.

Reversal DETECTION lives in the digest and is tested next to the rule it extends
(``test_catalyst_digest.py``, the "story-level REVERSAL detection" block). This file covers what the
detection feeds: the scanner's event definition and levels, the shared caps, the validation-ledger
keying — and, first and at length, the safety property.

THE SAFETY PROPERTY, AND WHY IT IS TESTED DIRECTLY RATHER THAN BY ANALOGY
-------------------------------------------------------------------------
A shadow strategy accumulates a validation population and must reach RECOMMEND exactly never until
its §8.6 owner gate. Building this file is what showed that the INDIRECT argument (ship no
``expected_edge_pct``, let C3's targetless branch reject everything) has a hole an analyst-supplied
``target_price`` walks straight through — the argument is set out in full at ``risk/gate.py``
:data:`~engine.risk.gate._SHADOW_NO_EDGE`, the enforcement point.

So the property is not inherited from an absence: ``cat_reversal`` is registered in
``ops.main.NO_EDGE_SHADOW_STRATEGIES``, which rejects it BEFORE ``target_price`` is read. The tests
below attack that property the way a bug would: a maximally favourable target, a sweep of targets
across four orders of magnitude, a maximally permissive gate context, and finally the real pipeline
with an analyst that returns a confident enter — asserting nothing is delivered and nothing is
journalled as a recommendation.
"""

from __future__ import annotations

import json
from decimal import Decimal
from typing import Any

import pytest
import yaml

from engine.core.calendar import NSECalendar
from engine.core.clock import Clock
from engine.core.config import Settings, config_dir
from engine.core.contracts import EnterAction
from engine.notify.catalog import MessageKind
from engine.ops import main as ops_main
from engine.ops.pipeline import RecommendationBook
from engine.risk import gate as gate_module
from engine.risk.gate import RiskGate
from engine.risk.limits import LimitTable
from engine.strategy.contracts import UNKNOWN_CONTRACT, contract_text
from engine.strategy.cost_model import CostModel
from engine.strategy.prescreen import SignalPreScreen
from engine.strategy.scanners import cat, cat_reversal
from engine.strategy.types import RawLevels, ScanContext, SignalCandidate
from tests.unit.test_reco_pipeline import (
    CALENDAR_DIR,
    LIMITS_YAML,
    NOW,
    SYMBOL,
    FakeHarness,
    Notifier,
    StubLimits,
    Ticker,
    make_pipeline,
    passing_ctx,
    publish_candidate,
)

STRATEGY = cat_reversal.STRATEGY_ID

#: Any trading day — the cap tests are about counters, not the calendar.
CAP_DAY = NOW.date()


# --------------------------------------------------------------------------- fixtures
@pytest.fixture(scope="module")
def limit_table() -> LimitTable:
    return LimitTable.model_validate(yaml.safe_load(LIMITS_YAML.read_text(encoding="utf-8")))


@pytest.fixture(scope="module")
def cost_model() -> CostModel:
    return CostModel.from_config()          # the REAL config/costs.yaml (C1/C3)


@pytest.fixture
def pclock() -> Clock:
    return Clock(time_source=Ticker(NOW))


@pytest.fixture
def calendar(pclock: Clock, conn) -> NSECalendar:
    return NSECalendar(CALENDAR_DIR, pclock, sqlite_conn=conn)


@pytest.fixture
def book(conn, pclock: Clock, cost_model: CostModel) -> RecommendationBook:
    return RecommendationBook(conn, pclock, cost_model)


@pytest.fixture
def shadow_gate(limit_table: LimitTable, cost_model: CostModel, pclock: Clock) -> RiskGate:
    """The gate EXACTLY as the composition root builds it — the shipped limits, the shipped cost
    surface, and the shipped shadow registry. Not a hand-made set: a test that passed its own literal
    ``{"cat_reversal"}`` would keep passing after someone dropped the id from ``ops.main``."""
    return RiskGate(
        StubLimits(limit_table), cost_model, pclock,
        no_edge_shadow_strategies=ops_main.NO_EDGE_SHADOW_STRATEGIES,
    )


def row(**overrides: Any) -> cat_reversal.WatchlistRow:
    """An eligible reversal row: originating, long, age 1, reversal_of set, with a prior close."""
    base: dict[str, Any] = {
        "entry_id": "01ENTRY",
        "symbol": SYMBOL,
        "grade": "originating",
        "direction": "long",
        "event_age_sessions": 1,
        "materiality": 0.75,
        "reversal_of": "c-stale",
        "reference_close": Decimal("100"),
    }
    return cat_reversal.WatchlistRow(**{**base, **overrides})


def enter(**overrides: Any) -> EnterAction:
    """A ``cat_reversal`` enter proposal as the analyst would emit it (levels from the scanner)."""
    base: dict[str, Any] = {
        "action": "enter",
        "thesis": "DIPAM formally denied the stake-sale story that drove the prior week's markdown.",
        "confidence": 0.95,
        "proposal_id": "01PROPOSAL",
        "agent_id": "intraday_analyst",
        "valid_until": NOW.replace(hour=15),
        "inputs_digest": "d" * 16,
        "tradingsymbol": SYMBOL,
        "exchange": "NSE",
        "side": "BUY",
        "style": "swing",
        "entry_type": "LIMIT",
        "entry_price": Decimal("100"),
        "stop_price": Decimal("95"),
        "target_price": None,
        "quantity": 10,
        "signal_id": "01SIGNAL",
        "strategy_id": STRATEGY,
        "features_snapshot_id": "01SNAP",
    }
    return EnterAction(**{**base, **overrides})


# ===========================================================================================
# THE SAFETY PROPERTY — a cat_reversal signal can never be approved, and never be recommended
# ===========================================================================================
def test_a_maximally_strong_cat_reversal_signal_is_still_rejected(shadow_gate: RiskGate) -> None:
    """**The single most important test in this change.**

    Everything is stacked in the proposal's favour: a 20x-breakeven target (an edge no real strategy
    produces), a 0.95-confidence thesis, a 1-unit ask that no cap can shrink, and a gate context in
    which every other §7.1 enter rule passes. It is still rejected, and the reason names the shadow —
    not the missing target, not a cap.

    If this test ever fails, ``cat_reversal`` has become able to spend real money on an unvalidated
    rule, which is precisely the failure the shadow design exists to make impossible."""
    verdict = shadow_gate.evaluate(
        enter(target_price=Decimal("2000"), quantity=1), passing_ctx()
    )

    assert verdict.verdict == "reject"
    assert verdict.approved_qty == 0
    check = gate_check(verdict, "min_viable_size")
    assert check.passed is False
    assert check.value == gate_module._SHADOW_NO_EDGE
    # The reject is the SHADOW one, not the targetless one it would otherwise have been.
    assert check.value != gate_module._NO_TARGET


@pytest.mark.parametrize(
    "target",
    [None, Decimal("100.01"), Decimal("101"), Decimal("110"), Decimal("150"),
     Decimal("1000"), Decimal("100000")],
)
def test_no_target_price_whatsoever_can_buy_a_cat_reversal_an_approval(
    shadow_gate: RiskGate, target: Decimal | None
) -> None:
    """The hole this registry closes, swept shut. Across four orders of magnitude of analyst-supplied
    target — including the ``None`` the scanner actually emits — the verdict is reject every time.

    Without the registry the ``None`` case would reject (C3's targetless branch) and EVERY other case
    would reach the real edge arithmetic, where a large enough target passes. That asymmetry is the
    bug: it makes a safety property contingent on what a language model chose to emit."""
    verdict = shadow_gate.evaluate(enter(target_price=target, quantity=1), passing_ctx())
    assert verdict.verdict == "reject", f"target={target} produced {verdict.verdict}"
    assert verdict.approved_qty == 0
    assert gate_check(verdict, "min_viable_size").value == gate_module._SHADOW_NO_EDGE


def test_the_shadow_reject_is_not_shrinkable(shadow_gate: RiskGate) -> None:
    """``min_viable_size`` is deliberately absent from ``SHRINKABLE_RULES``, so the shrink loop can
    never retry a shadow candidate down to a size that "passes". Asserted against the shipped
    constant rather than the observed behaviour, so a future edit to that set trips this test."""
    assert "min_viable_size" not in gate_module.SHRINKABLE_RULES
    verdict = shadow_gate.evaluate(enter(target_price=Decimal("500"), quantity=500), passing_ctx())
    assert verdict.verdict == "reject"
    assert verdict.approved_qty == 0


def test_cat_reversal_is_registered_in_the_composition_roots_shadow_set() -> None:
    """The property above is worth nothing if the live gate is not built with the registry. This is
    the wiring assertion; ``shadow_gate`` consumes the same constant the composition root passes."""
    assert cat_reversal.STRATEGY_ID in ops_main.NO_EDGE_SHADOW_STRATEGIES


def test_cat_reversal_registers_no_expected_edge_anywhere() -> None:
    """Belt AND braces: the registry is the enforcement, but the absence of an edge must hold too.

    An ``expected_edge_pct`` on the config model would let a future edit wire it into
    ``strategy_expected_edge_pct`` and quietly satisfy C3 through the targetless branch. There is no
    such field, and adding one is the §8.6 owner gate."""
    settings = Settings()
    assert not hasattr(settings.cat_reversal, "expected_edge_pct")
    # ...and the two shadows keep separate knobs, so tuning one cannot move the other.
    assert settings.cat_reversal.stop_pct == 5.0
    assert "stop_pct" in cat_reversal.DEFAULT_PARAMS
    assert cat_reversal.DEFAULT_PARAMS is not cat.DEFAULT_PARAMS


async def test_a_confident_analyst_enter_is_never_delivered_as_a_recommendation(
    conn, pclock, calendar, book, limit_table, cost_model
) -> None:
    """END-TO-END, through the real pipeline: the other half of "can never reach RECOMMEND".

    The gate tests prove no approval is reachable; this proves the pipeline's delivery path is
    actually downstream of that verdict. The analyst returns a high-confidence ``enter`` WITH a
    generous target (the exact behaviour that defeats the indirect argument), and after a full drain:
    no recommendation row exists, no owner message was sent, and the verdict on record is a reject.

    ``RecommendationPipeline`` delivers only when ``verdict.verdict in ("approve", "shrink")``, so an
    unreachable approval is an unreachable recommendation — but asserted here rather than argued."""
    notify = Notifier()
    harness = FakeHarness({
        "action": "enter",
        "thesis": "Official denial resolves the overhang; the markdown was priced on the rumour.",
        "confidence": 0.95,
        "tradingsymbol": SYMBOL,
        "exchange": "NSE",
        "side": "BUY",
        "style": "swing",
        "entry_type": "LIMIT",
        "entry_price": "100",
        "stop_price": "95",
        "target_price": "150",            # the analyst volunteers a very rich target
        "quantity": 10,
        "signal_id": "01SIGNAL",
        "strategy_id": STRATEGY,
        "features_snapshot_id": "01SNAP",
    })
    gate = RiskGate(
        StubLimits(limit_table), cost_model, pclock,
        no_edge_shadow_strategies=ops_main.NO_EDGE_SHADOW_STRATEGIES,
    )
    pipeline, _ = make_pipeline(
        conn=conn, clock=pclock, calendar=calendar, book=book, harness=harness, gate=gate,
        ctx=passing_ctx(), limits=StubLimits(limit_table), notify=notify,
    )

    await publish_candidate(pipeline, SignalCandidate(
        signal_id="01SIGNAL", strategy_id=STRATEGY, symbol=SYMBOL, side="BUY", style="swing",
        raw_levels=RawLevels(entry=Decimal("100"), stop=Decimal("95"), target=None),
        score=1.0, features_snapshot_id="01SNAP", catalyst_ref="01ENTRY",
    ))

    # The analyst WAS called (its thesis is a validation covariate — that part is intended)...
    assert len(harness.calls) == 1
    # ...and nothing was recommended.
    assert conn.execute("SELECT COUNT(*) AS n FROM recommendations").fetchone()["n"] == 0
    assert conn.execute("SELECT COUNT(*) AS n FROM learning_ledger").fetchone()["n"] == 0
    assert not [m for m in notify.messages if m.kind == MessageKind.RECOMMENDATION]
    # The verdict on record is a reject, and it says why in the shadow's own words.
    verdicts = conn.execute("SELECT verdict, payload FROM verdicts").fetchall()
    assert [v["verdict"] for v in verdicts] == ["reject"]
    checks = json.loads(verdicts[0]["payload"])["checks"]
    c3 = [c for c in checks if c["rule_id"] == "min_viable_size"]
    assert len(c3) == 1
    assert c3[0]["passed"] is False
    assert c3[0]["value"] == gate_module._SHADOW_NO_EDGE


# ===========================================================================================
# Event definition — the rule's own filter
# ===========================================================================================
def test_the_event_is_cat_v2s_plus_the_reversal_requirement() -> None:
    """``grade='originating'`` ∧ ``direction='long'`` ∧ ``age <= 1`` ∧ ``reversal_of`` set. The first
    three are ``cat`` v2's event verbatim; nothing is RELAXED for a reversal candidate."""
    assert cat_reversal.is_eligible(row()) is True
    assert cat_reversal.is_eligible(row(grade="context")) is False
    assert cat_reversal.is_eligible(row(direction="short")) is False
    assert cat_reversal.is_eligible(row(direction=None)) is False
    assert cat_reversal.is_eligible(row(event_age_sessions=2)) is False
    assert cat_reversal.is_eligible(row(event_age_sessions=None)) is False


def test_an_ordinary_originating_row_is_not_a_cat_reversal_candidate() -> None:
    """The discriminator. A row that satisfies every one of ``cat`` v2's conditions but reversed
    nothing is exactly what ``cat`` originates on and must NOT enter this shadow's population — or the
    two studies measure the same thing and the comparison at the §8.6 gate is meaningless."""
    assert cat_reversal.is_eligible(row(reversal_of=None)) is False
    assert cat_reversal.sweep_watchlist([row(reversal_of=None)]) == []
    # ...while `cat` itself still originates on it, untouched by this change.
    assert cat.is_eligible(cat.WatchlistRow(
        entry_id="01ENTRY", symbol=SYMBOL, grade="originating", direction="long",
        event_age_sessions=1, materiality=0.75, reference_close=Decimal("100"),
    )) is True


def test_an_empty_reversal_ref_is_not_a_reversal() -> None:
    """A store that renders SQL NULL as ``''`` must not be readable as "this reversed something"."""
    assert cat_reversal.is_eligible(row(reversal_of="")) is False


def test_single_shot_semantics_match_cats_age_bound() -> None:
    """The age bound IS the once-per-story rule: an age-2 re-grade never re-originates, so one story
    contributes one signal to the validation population and cannot be double-counted."""
    assert cat_reversal.MAX_EVENT_AGE_SESSIONS == 1
    assert cat_reversal.sweep_watchlist([row(event_age_sessions=2)]) == []


# ===========================================================================================
# Levels / score — the ins mechanics mirror
# ===========================================================================================
def test_levels_are_the_ins_mechanics_with_cat_reversals_own_stop() -> None:
    """entry = round_to_tick(reference_close); stop = entry x (1 - stop_pct/100); target = None."""
    cand = cat_reversal.scan_entry(row(), params={"stop_pct": 5.0})
    assert cand is not None
    assert cand.strategy_id == STRATEGY
    assert cand.side == "BUY"
    assert cand.style == "swing"
    assert cand.raw_levels.entry == Decimal("100.00")
    assert cand.raw_levels.stop == Decimal("95.00")
    assert cand.raw_levels.target is None            # no target, ever — the exit is TIME
    assert cand.catalyst_ref == "01ENTRY"            # the §6.5 audit link back to the graded row


def test_the_stop_comes_from_cat_reversals_own_parameter_not_cats() -> None:
    """Two experiments, two knobs. If these were shared, tuning ``cat`` mid-shadow would silently
    move ``cat_reversal``'s levels — and with them the population it is accumulating."""
    cand = cat_reversal.scan_entry(row(), params={"stop_pct": 8.0})
    assert cand is not None and cand.raw_levels.stop == Decimal("92.00")
    # `cat`'s default is untouched by that, and vice versa.
    assert cat.DEFAULT_PARAMS["stop_pct"] == 5.0


def test_score_is_the_reversal_clusters_weighted_materiality_clamped() -> None:
    assert cat_reversal.scan_entry(row(materiality=0.75)).score == pytest.approx(0.75)
    assert cat_reversal.scan_entry(row(materiality=4.2)).score == 1.0
    assert cat_reversal.scan_entry(row(materiality=-1.0)).score == 0.0
    assert cat_reversal.scan_entry(row(materiality=None)).score == 0.0
    assert cat_reversal.scan_entry(row(materiality=float("nan"))).score == 0.0


@pytest.mark.parametrize(
    "ref", [None, Decimal("0"), Decimal("-10"), Decimal("NaN"), "not-a-number"]
)
def test_a_bad_reference_price_fails_to_no_candidate(ref: Any) -> None:
    """§3.2.5 fail-to-zero: a malformed price costs this candidate and nothing else — never raises."""
    assert cat_reversal.scan_entry(row(reference_close=ref)) is None


def test_tick_rounding_degeneracy_fails_to_no_candidate() -> None:
    """A stop that does not survive tick rounding as strictly below the entry emits nothing, rather
    than shipping ``stop >= entry`` into the §7.1 ``levels_coherent`` check (the brk20/ins/cat rule)."""
    assert cat_reversal.scan_entry(row(reference_close=Decimal("0.05")),
                                   params={"stop_pct": 0.01}) is None
    assert cat_reversal.scan_entry(row(), params={"stop_pct": 0}) is None
    assert cat_reversal.scan_entry(row(), params={"stop_pct": 100}) is None


def test_sweep_is_deterministically_ordered() -> None:
    """§9.6: score desc, symbol asc — the same convention as cat/ins/brk20."""
    cands = cat_reversal.sweep_watchlist([
        row(symbol="BBB", materiality=0.5, entry_id="e2"),
        row(symbol="AAA", materiality=0.9, entry_id="e1"),
        row(symbol="CCC", materiality=0.5, entry_id="e3"),
        row(symbol="DDD", reversal_of=None, entry_id="e4"),      # filtered: not a reversal
    ])
    assert [c.symbol for c in cands] == ["AAA", "BBB", "CCC"]


# ===========================================================================================
# Caps — SHARED with `cat`, not added to
# ===========================================================================================
def test_cat_reversal_entries_charge_the_same_catalyst_budget_as_cat() -> None:
    """``catalyst_guard.max_catalyst_entries_day`` is keyed on the candidate's ``catalyst_ref``, not on
    ``strategy_id`` — so adding this strategy does NOT widen the platform's total news-originated
    entry surface. With the cap at 2 and the per-strategy caps deliberately loosened out of the way,
    one ``cat`` entry plus one ``cat_reversal`` entry exhausts it and a third catalyst-bearing
    candidate is refused, exactly as a third ``cat`` entry would be."""
    prescreen = SignalPreScreen(
        [], lambda bar: ScanContext(), None,
        max_candidates_per_day=20,
        max_per_strategy_day={"default": 5},
        catalyst_cap_fn=lambda: 2,
    )

    def cand(strategy_id: str, symbol: str) -> SignalCandidate:
        return SignalCandidate(
            signal_id=f"sig-{symbol}", strategy_id=strategy_id, symbol=symbol, side="BUY",
            style="swing", raw_levels=RawLevels(entry=Decimal("100"), stop=Decimal("95")),
            score=0.8, catalyst_ref=f"ref-{symbol}",
        )

    admitted = prescreen.admit(
        [cand("cat", "AAA"), cand(STRATEGY, "BBB"), cand(STRATEGY, "CCC")], CAP_DAY
    )
    assert [c.symbol for c in admitted] == ["AAA", "BBB"]     # the third is over the SHARED cap
    # ...and it stays bound for the rest of the day, whichever leg asks next.
    assert prescreen.admit([cand("cat", "DDD")], CAP_DAY) == []


def test_one_story_charges_the_shared_budget_once_across_both_legs() -> None:
    """The budget counts DISTINCT ``catalyst_ref``, not admissions (2026-08-28).

    One ``catalyst_watchlist`` row projects into a ``cat`` candidate and a ``cat_reversal`` candidate
    that SHARE its ``entry_id`` as their ``catalyst_ref`` — one news event, deliberately entered
    twice as two experiments. Counting admissions charged that single story 2 of the day's 2 entries
    and starved the next genuine one; the test above never caught it because it gives every candidate
    a ref of its own. Keying the cap on the ref is also what ``settings.yaml`` and §2.7 have always
    claimed it does."""
    prescreen = SignalPreScreen(
        [], lambda bar: ScanContext(), None,
        max_candidates_per_day=20,
        max_per_strategy_day={"default": 5},
        catalyst_cap_fn=lambda: 2,
    )

    def cand(strategy_id: str, symbol: str, ref: str) -> SignalCandidate:
        return SignalCandidate(
            signal_id=f"sig-{strategy_id}-{symbol}", strategy_id=strategy_id, symbol=symbol,
            side="BUY", style="swing",
            raw_levels=RawLevels(entry=Decimal("100"), stop=Decimal("95")),
            score=0.8, catalyst_ref=ref,
        )

    both_legs = prescreen.admit(
        [cand("cat", "HINDZINC", "story-1"), cand(STRATEGY, "HINDZINC", "story-1")], CAP_DAY
    )
    assert [c.strategy_id for c in both_legs] == ["cat", STRATEGY]   # both admitted, ONE charge
    # So the day's SECOND story still fits, and only a third is over the shared cap.
    assert [c.symbol for c in prescreen.admit([cand("cat", "TITAN", "story-2")], CAP_DAY)] \
        == ["TITAN"]
    assert prescreen.admit([cand("cat", "LT", "story-3")], CAP_DAY) == []


def test_cat_reversal_has_its_own_prescreen_slot_budget() -> None:
    """The per-strategy admission sub-cap is a DIFFERENT, non-shared budget (it rations analyst
    spend, not exposure), so it gets its own line — defaulted to 2, matching ``cat``."""
    caps = yaml.safe_load(
        (config_dir() / "settings.yaml").read_text(encoding="utf-8")
    )["strategy"]["prescreen"]["max_per_strategy_day"]
    assert caps["cat_reversal"] == 2
    assert caps["cat"] == 2          # unchanged by this addition


# ===========================================================================================
# Validation ledger — its own series, never mixed with `cat` v2's
# ===========================================================================================
async def test_signals_are_journalled_under_their_own_strategy_id(
    conn, pclock, calendar, book, limit_table, cost_model
) -> None:
    """The shadow's validation population is the funnel journal — ``prescreen_day_slots`` rows written
    at ADMISSION by ``pipeline._journal_slot`` — which is the same mechanism ``cat`` v2's population
    uses. Keyed by ``strategy_id``, so the two studies read disjoint row sets and can never mix.

    Journalling must happen even though the candidate is destined for a gate reject: the reject IS the
    shadow, and a population that only recorded approvals would record nothing at all."""
    harness = FakeHarness({"action": "no_action", "reason": "reversal quality is unconvincing",
                           "regime_note": "index balancing"})
    gate = RiskGate(
        StubLimits(limit_table), cost_model, pclock,
        no_edge_shadow_strategies=ops_main.NO_EDGE_SHADOW_STRATEGIES,
    )
    pipeline, _ = make_pipeline(
        conn=conn, clock=pclock, calendar=calendar, book=book, harness=harness, gate=gate,
        ctx=passing_ctx(), limits=StubLimits(limit_table),
    )

    await publish_candidate(pipeline, SignalCandidate(
        signal_id="01SIGNAL", strategy_id=STRATEGY, symbol=SYMBOL, side="BUY", style="swing",
        raw_levels=RawLevels(entry=Decimal("100"), stop=Decimal("95"), target=None),
        score=0.75, features_snapshot_id="01SNAP", catalyst_ref="01ENTRY",
    ))

    rows = conn.execute(
        "SELECT strategy_id, symbol, evaluated, score, unsizeable FROM prescreen_day_slots"
    ).fetchall()
    assert [(r["strategy_id"], r["symbol"]) for r in rows] == [(STRATEGY, SYMBOL)]
    assert rows[0]["evaluated"] == 1
    assert rows[0]["unsizeable"] == 0                 # a real stop exists, so the row is sizeable
    assert rows[0]["score"] == pytest.approx(0.75)
    # `cat` v2's series is untouched — its clock cannot be polluted by this strategy's signals.
    assert not [r for r in rows if r["strategy_id"] == "cat"]


def test_the_two_shadows_pre_register_different_horizons() -> None:
    """Pinned so a later edit cannot quietly re-point one study at the other's horizons (which would
    re-open multiplicity for whichever window is open). ``cat_reversal`` measures T+5/T+10: T+5 is the
    resolved-uncertainty mechanism's own horizon, T+10 is the one horizon shared with ``cat`` v2 so
    the two populations remain comparable at the §8.6 review."""
    doc = cat_reversal.__doc__ or ""
    assert "T+5 and T+10" in doc
    assert "T+20 is deliberately NOT pre-registered" in doc
    assert cat_reversal.DEFAULT_PARAMS["hold_sessions"] == 10
    # `cat` v2's own pre-registration is untouched.
    assert "T+10/T+20" in (cat.__doc__ or "")
    assert cat.DEFAULT_PARAMS["hold_sessions"] == 20


# ===========================================================================================
# Wiring
# ===========================================================================================
def test_the_analyst_gets_a_contract_for_this_leg() -> None:
    """A new leg MUST land in ``STRATEGY_CONTRACTS`` in the same commit that ships it, or its
    candidates reach the analyst with ``UNKNOWN_CONTRACT`` and judge in no frame at all."""
    text = contract_text(STRATEGY)
    assert text != UNKNOWN_CONTRACT
    assert "REVERSAL" in text
    assert "SHADOW" in text              # the analyst is told its verdict cannot become a trade


def gate_check(verdict, rule_id: str):
    matches = [c for c in verdict.checks if c.rule_id == rule_id]
    assert len(matches) == 1, f"expected exactly one {rule_id} check, got {len(matches)}"
    return matches[0]


# ===========================================================================================
# `cat` v2 shares the same hole and the same closure (2026-08-28)
# ===========================================================================================
# `cat` v2 rested on the same indirect argument, with the same hole (this file's module docstring),
# and is now registered in `NO_EDGE_SHADOW_STRATEGIES` alongside `cat_reversal` — closing it the same
# way. This mirrors `test_a_confident_analyst_enter_is_never_delivered_as_a_recommendation` above
# with one strategy_id swapped, and is kept in this file because it exercises the SAME registry
# property rather than anything specific to `cat`'s scanner mechanics (those live in
# `test_cat_scanner.py`).
async def test_a_confident_cat_analyst_enter_is_never_delivered_as_a_recommendation(
    conn, pclock, calendar, book, limit_table, cost_model
) -> None:
    """END-TO-END, through the real pipeline, for the OTHER shadow strategy.

    Same shape as the ``cat_reversal`` version: the analyst returns a high-confidence ``enter`` WITH
    a generous target for a `cat` candidate, and after a full drain — no recommendation row, no
    learning_ledger row, no owner message, and the verdict on record is a reject naming the shadow
    (not the targetless branch `cat` would otherwise have fallen into)."""
    notify = Notifier()
    harness = FakeHarness({
        "action": "enter",
        "thesis": "Rating upgrade confirms the re-rating thesis the desk has been building.",
        "confidence": 0.95,
        "tradingsymbol": SYMBOL,
        "exchange": "NSE",
        "side": "BUY",
        "style": "swing",
        "entry_type": "LIMIT",
        "entry_price": "100",
        "stop_price": "95",
        "target_price": "150",            # the analyst volunteers a very rich target
        "quantity": 10,
        "signal_id": "01SIGNAL",
        "strategy_id": cat.STRATEGY_ID,
        "features_snapshot_id": "01SNAP",
    })
    gate = RiskGate(
        StubLimits(limit_table), cost_model, pclock,
        no_edge_shadow_strategies=ops_main.NO_EDGE_SHADOW_STRATEGIES,
    )
    pipeline, _ = make_pipeline(
        conn=conn, clock=pclock, calendar=calendar, book=book, harness=harness, gate=gate,
        ctx=passing_ctx(), limits=StubLimits(limit_table), notify=notify,
    )

    await publish_candidate(pipeline, SignalCandidate(
        signal_id="01SIGNAL", strategy_id=cat.STRATEGY_ID, symbol=SYMBOL, side="BUY", style="swing",
        raw_levels=RawLevels(entry=Decimal("100"), stop=Decimal("95"), target=None),
        score=1.0, features_snapshot_id="01SNAP", catalyst_ref="01ENTRY",
    ))

    # The analyst WAS called (its thesis is a validation covariate — that part is intended)...
    assert len(harness.calls) == 1
    # ...and nothing was recommended.
    assert conn.execute("SELECT COUNT(*) AS n FROM recommendations").fetchone()["n"] == 0
    assert conn.execute("SELECT COUNT(*) AS n FROM learning_ledger").fetchone()["n"] == 0
    assert not [m for m in notify.messages if m.kind == MessageKind.RECOMMENDATION]
    # The verdict on record is a reject, and it says why in the shadow's own words.
    verdicts = conn.execute("SELECT verdict, payload FROM verdicts").fetchall()
    assert [v["verdict"] for v in verdicts] == ["reject"]
    checks = json.loads(verdicts[0]["payload"])["checks"]
    c3 = [c for c in checks if c["rule_id"] == "min_viable_size"]
    assert len(c3) == 1
    assert c3[0]["passed"] is False
    assert c3[0]["value"] == gate_module._SHADOW_NO_EDGE
    assert c3[0]["value"] != gate_module._NO_TARGET
