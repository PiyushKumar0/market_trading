"""``cat_reversal`` shadow: the scanner's event and levels, the shared caps, validation-ledger keying,
and the safety property that a shadow strategy never reaches RECOMMEND before its owner promotion gate.

Reversal detection is tested in ``test_catalyst_digest.py``. The safety property is tested directly,
not inherited from the absence of ``expected_edge_pct``: an analyst-supplied ``target_price`` defeats
the cost gate's targetless branch (see ``risk/gate.py`` ``_SHADOW_NO_EDGE``). Enforcement is the
registration in ``ops.main.NO_EDGE_SHADOW_STRATEGIES``, which rejects before ``target_price`` is read.
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
    return CostModel.from_config()          # the REAL config/costs.yaml


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
    """The gate as the composition root builds it, with the shipped shadow registry (not a literal set,
    which would keep passing after the id is dropped from ``ops.main``)."""
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
    """Everything favours the proposal (20x-breakeven target, 0.95 confidence, 1-unit ask, a context where
    every other rule passes), yet it is rejected with the shadow reason, not the missing-target or a cap."""
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
    """Across four orders of magnitude of analyst target, including the scanner's ``None``, the verdict is
    reject; without the registry a large enough target would pass the edge arithmetic."""
    verdict = shadow_gate.evaluate(enter(target_price=target, quantity=1), passing_ctx())
    assert verdict.verdict == "reject", f"target={target} produced {verdict.verdict}"
    assert verdict.approved_qty == 0
    assert gate_check(verdict, "min_viable_size").value == gate_module._SHADOW_NO_EDGE


def test_the_shadow_reject_is_not_shrinkable(shadow_gate: RiskGate) -> None:
    """``min_viable_size`` is absent from ``SHRINKABLE_RULES``, so the shrink loop cannot retry a shadow candidate to a passing size."""
    assert "min_viable_size" not in gate_module.SHRINKABLE_RULES
    verdict = shadow_gate.evaluate(enter(target_price=Decimal("500"), quantity=500), passing_ctx())
    assert verdict.verdict == "reject"
    assert verdict.approved_qty == 0


def test_cat_reversal_is_registered_in_the_composition_roots_shadow_set() -> None:
    """Wiring: the live gate is built with this registry."""
    assert cat_reversal.STRATEGY_ID in ops_main.NO_EDGE_SHADOW_STRATEGIES


def test_cat_reversal_registers_no_expected_edge_anywhere() -> None:
    """No ``expected_edge_pct`` field exists; adding one would be the owner promotion gate."""
    settings = Settings()
    assert not hasattr(settings.cat_reversal, "expected_edge_pct")
    # ...and the two shadows keep separate knobs, so tuning one cannot move the other.
    assert settings.cat_reversal.stop_pct == 5.0
    assert "stop_pct" in cat_reversal.DEFAULT_PARAMS
    assert cat_reversal.DEFAULT_PARAMS is not cat.DEFAULT_PARAMS


async def test_a_confident_analyst_enter_is_never_delivered_as_a_recommendation(
    conn, pclock, calendar, book, limit_table, cost_model
) -> None:
    """End to end through the real pipeline: a confident ``enter`` with a generous target leaves no
    recommendation row, no owner message, and a reject on record (delivery requires approve/shrink)."""
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

    # The analyst is called (its thesis is a validation covariate); nothing is recommended.
    assert len(harness.calls) == 1
    assert conn.execute("SELECT COUNT(*) AS n FROM recommendations").fetchone()["n"] == 0
    assert conn.execute("SELECT COUNT(*) AS n FROM learning_ledger").fetchone()["n"] == 0
    assert not [m for m in notify.messages if m.kind == MessageKind.RECOMMENDATION]
    verdicts = conn.execute("SELECT verdict, payload FROM verdicts").fetchall()
    assert [v["verdict"] for v in verdicts] == ["reject"]
    checks = json.loads(verdicts[0]["payload"])["checks"]
    size_checks = [c for c in checks if c["rule_id"] == "min_viable_size"]
    assert len(size_checks) == 1
    assert size_checks[0]["passed"] is False
    assert size_checks[0]["value"] == gate_module._SHADOW_NO_EDGE


# ===========================================================================================
# Event definition — the rule's own filter
# ===========================================================================================
def test_the_event_is_cat_v2s_plus_the_reversal_requirement() -> None:
    """``grade='originating'`` ∧ ``direction='long'`` ∧ ``age <= 1`` ∧ ``reversal_of`` set; the first three are ``cat`` v2's event verbatim."""
    assert cat_reversal.is_eligible(row()) is True
    assert cat_reversal.is_eligible(row(grade="context")) is False
    assert cat_reversal.is_eligible(row(direction="short")) is False
    assert cat_reversal.is_eligible(row(direction=None)) is False
    assert cat_reversal.is_eligible(row(event_age_sessions=2)) is False
    assert cat_reversal.is_eligible(row(event_age_sessions=None)) is False


def test_an_ordinary_originating_row_is_not_a_cat_reversal_candidate() -> None:
    """A row meeting every ``cat`` v2 condition but reversing nothing must stay out, or the two studies measure the same thing."""
    assert cat_reversal.is_eligible(row(reversal_of=None)) is False
    assert cat_reversal.sweep_watchlist([row(reversal_of=None)]) == []
    assert cat.is_eligible(cat.WatchlistRow(
        entry_id="01ENTRY", symbol=SYMBOL, grade="originating", direction="long",
        event_age_sessions=1, materiality=0.75, reference_close=Decimal("100"),
    )) is True


def test_an_empty_reversal_ref_is_not_a_reversal() -> None:
    """A store that renders SQL NULL as ``''`` must not be readable as "this reversed something"."""
    assert cat_reversal.is_eligible(row(reversal_of="")) is False


def test_single_shot_semantics_match_cats_age_bound() -> None:
    """The age bound is the once-per-story rule: an age-2 re-grade never re-originates."""
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
    assert cand.catalyst_ref == "01ENTRY"            # the audit link back to the graded row


def test_the_stop_comes_from_cat_reversals_own_parameter_not_cats() -> None:
    """Two experiments, two knobs: tuning ``cat`` must not move ``cat_reversal``'s levels."""
    cand = cat_reversal.scan_entry(row(), params={"stop_pct": 8.0})
    assert cand is not None and cand.raw_levels.stop == Decimal("92.00")
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
    """Fail-to-zero: a malformed price costs this candidate and nothing else — never raises."""
    assert cat_reversal.scan_entry(row(reference_close=ref)) is None


def test_tick_rounding_degeneracy_fails_to_no_candidate() -> None:
    """A stop not strictly below the entry after tick rounding emits nothing (the brk20/ins/cat rule)."""
    assert cat_reversal.scan_entry(row(reference_close=Decimal("0.05")),
                                   params={"stop_pct": 0.01}) is None
    assert cat_reversal.scan_entry(row(), params={"stop_pct": 0}) is None
    assert cat_reversal.scan_entry(row(), params={"stop_pct": 100}) is None


def test_sweep_is_deterministically_ordered() -> None:
    """Score desc, symbol asc — the same convention as cat/ins/brk20."""
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
    """``catalyst_guard.max_catalyst_entries_day`` is keyed on ``catalyst_ref``, not ``strategy_id``, so this
    strategy does not widen the news-originated entry surface: with the cap at 2 and per-strategy caps
    loosened, one ``cat`` plus one ``cat_reversal`` entry exhausts it."""
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
    assert prescreen.admit([cand("cat", "DDD")], CAP_DAY) == []


def test_one_story_charges_the_shared_budget_once_across_both_legs() -> None:
    """The budget counts distinct ``catalyst_ref``, not admissions: one watchlist row yields a ``cat`` and a
    ``cat_reversal`` candidate sharing its ``entry_id`` as ref, and that one story is charged once."""
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
    """The per-strategy admission sub-cap rations analyst spend, not exposure, so it is not shared."""
    caps = yaml.safe_load(
        (config_dir() / "settings.yaml").read_text(encoding="utf-8")
    )["strategy"]["prescreen"]["max_per_strategy_day"]
    assert caps["cat_reversal"] == 2
    assert caps["cat"] == 2


# ===========================================================================================
# Validation ledger — its own series, never mixed with `cat` v2's
# ===========================================================================================
async def test_signals_are_journalled_under_their_own_strategy_id(
    conn, pclock, calendar, book, limit_table, cost_model
) -> None:
    """The validation population is the ``prescreen_day_slots`` rows written at admission
    (``pipeline._journal_slot``), keyed by ``strategy_id``, so it is journalled even though the gate rejects it."""
    harness = FakeHarness({"action": "no_action", "reason": "reversal quality is unconvincing"})
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
    assert not [r for r in rows if r["strategy_id"] == "cat"]


def test_the_two_shadows_pre_register_different_horizons() -> None:
    """Horizons are pre-registered: ``cat_reversal`` T+5 (its mechanism's own) and T+10 (shared with ``cat`` v2); re-pointing re-opens multiplicity."""
    doc = cat_reversal.__doc__ or ""
    assert "T+5 and T+10" in doc
    assert "T+20 is deliberately NOT pre-registered" in doc
    assert cat_reversal.DEFAULT_PARAMS["hold_sessions"] == 10
    assert "T+10/T+20" in (cat.__doc__ or "")
    assert cat.DEFAULT_PARAMS["hold_sessions"] == 20


# ===========================================================================================
# Wiring
# ===========================================================================================
def test_the_analyst_gets_a_contract_for_this_leg() -> None:
    """A leg without a ``STRATEGY_CONTRACTS`` entry reaches the analyst with ``UNKNOWN_CONTRACT``."""
    text = contract_text(STRATEGY)
    assert text != UNKNOWN_CONTRACT
    assert "REVERSAL" in text
    assert "SHADOW" in text              # the analyst is told its verdict cannot become a trade


def gate_check(verdict, rule_id: str):
    matches = [c for c in verdict.checks if c.rule_id == rule_id]
    assert len(matches) == 1, f"expected exactly one {rule_id} check, got {len(matches)}"
    return matches[0]


# ===========================================================================================
# `cat` v2 is registered in the same shadow set
# ===========================================================================================
# Mirrors the cat_reversal end-to-end test with the strategy_id swapped; it exercises the registry
# property, not cat's scanner mechanics (those live in `test_cat_scanner.py`).
async def test_a_confident_cat_analyst_enter_is_never_delivered_as_a_recommendation(
    conn, pclock, calendar, book, limit_table, cost_model
) -> None:
    """End to end for ``cat``: no recommendation, ledger row or owner message, and a reject naming the shadow."""
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

    assert len(harness.calls) == 1
    assert conn.execute("SELECT COUNT(*) AS n FROM recommendations").fetchone()["n"] == 0
    assert conn.execute("SELECT COUNT(*) AS n FROM learning_ledger").fetchone()["n"] == 0
    assert not [m for m in notify.messages if m.kind == MessageKind.RECOMMENDATION]
    verdicts = conn.execute("SELECT verdict, payload FROM verdicts").fetchall()
    assert [v["verdict"] for v in verdicts] == ["reject"]
    checks = json.loads(verdicts[0]["payload"])["checks"]
    size_checks = [c for c in checks if c["rule_id"] == "min_viable_size"]
    assert len(size_checks) == 1
    assert size_checks[0]["passed"] is False
    assert size_checks[0]["value"] == gate_module._SHADOW_NO_EDGE
    assert size_checks[0]["value"] != gate_module._NO_TARGET
