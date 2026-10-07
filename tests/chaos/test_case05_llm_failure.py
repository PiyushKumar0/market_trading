"""§9.4 chaos case 5 — LLM timeout / garbage output / credit (quota) exhaustion.

Plan row (IMPLEMENTATION_PLAN.md §9.4, case 5), "Must hold", verbatim:

    no-proposal + alert (D7); DG4 ⇒ same path; exits/square-offs unaffected (R1)

Composition (as ``engine.ops.main`` wires the Phase-2 decision plane, main.py:446-553 / 718-806): the
REAL ``AgentHarness`` → REAL ``BudgetGovernor`` (pinned worked-example budget, the §5.6 ladder) → REAL
``RecommendationPipeline`` + ``RecommendationBook`` + ``RiskGate`` (hash-verified ``LimitsEngine`` over
a tmp copy of the shipped ``config/limits.yaml``) + real ``ContextAssembler`` over a tmp ``MarketStore``,
real ``ModeManager``/``KillSwitch``/``ExposureTracker``, and a REAL ``TelegramBot`` journalling every
owner message into ``notifications`` (attached to the bus like main.py:3894; main's ``alert`` and
``notify`` closures reproduced). Faked, and only at the boundary:

* the Claude Agent SDK — ``query``/options injected into the harness (the harness's own seam), each
  test scripting what the SDK does: hang, emit prose / out-of-schema JSON, raise a billing error, or
  report the subscription session limit;
* the Telegram transport (``app.bot.send_message``) — records what reached the owner's phone;
* the gate-context DATA seam (``GateContextBuilder`` needs the live tick cache + instruments dump):
  a builder returning the all-rules-pass context the gate suite uses — the gate itself is real.

Clauses asserted:

* **timeout ⇒ no-proposal + alert (D7)** — ``test_llm_timeout_fails_to_no_proposal_and_alerts``
* **garbage output ⇒ no-proposal + alert; ≤ 2 schema retries, prose never salvaged (D7)** —
  ``test_garbage_output_fails_to_no_proposal_and_alerts`` (prose / out-of-schema JSON)
* **credit/quota exhaustion ⇒ no-proposal + alert; a billing error latches DG4 ⇒ zero further SDK
  calls** — ``test_quota_exhaustion_fails_to_no_proposal_and_alerts`` (SDK billing error / the
  subscription "session limit" result of 2026-09-03)
* **DG4 ⇒ same path** (spend ≥ the weekly credit): tier change Telegram-alerted, every Tier-1 path
  refused with ZERO SDK calls, no proposal — ``test_dg4_takes_the_same_no_proposal_path``
* **deterministic tiers unaffected (R1)**: mode/risk-state/kill untouched by every failure above, and
  the §7.1 ``max_holding`` time-stop EXIT recommendation is still issued with the SDK dead AND DG4 —
  ``test_deterministic_exit_unaffected_with_the_llm_dead`` (+ the tier asserts in every test)
* control (non-vacuity): the same world with a healthy SDK DOES produce a recommendation —
  ``test_healthy_llm_control_produces_a_recommendation``

Skipped: **square-offs unaffected** — ``SquareOffScheduler`` / window-end MIS square-off; MIS is out of
paper scope (plan §1.3), so no platform position exists to square off.
"""

from __future__ import annotations

import asyncio
import json
import shutil
from dataclasses import dataclass
from datetime import datetime, time, timedelta
from decimal import Decimal
from typing import Any

import pytest

from engine.core.calendar import NSECalendar
from engine.core.clock import IST, Clock
from engine.core.config import config_dir
from engine.core.enums import Actor, DegradeTier, Mode, RiskState
from engine.core.eventbus import EventBus
from engine.core.protected_store import ProtectedStore
from engine.core.types import OwnerConfirmation, TradeWindow
from engine.intelligence.context import ContextAssembler
from engine.intelligence.governor import BudgetGovernor, TokenUsage
from engine.intelligence.harness import AgentDef, AgentHarness
from engine.marketdata.store import MarketStore
from engine.notify.catalog import CatalogMessage, MessageKind
from engine.notify.telegram import TelegramBot
from engine.ops.pipeline import RecommendationBook, RecommendationPipeline
from engine.risk.exposure import ExposureTracker
from engine.risk.gate import GateContext, RiskGate
from engine.risk.kill import KillSwitch
from engine.risk.limits import LimitsEngine
from engine.risk.mode import ModeManager
from engine.strategy.cost_model import CostModel
from engine.strategy.types import RawLevels, SignalCandidate
from tests.chaos.conftest import MIS_OUT_OF_PAPER_SCOPE
from tests.unit.test_budget_governor import PINNED_CFG

NOW = datetime(2026, 6, 17, 10, 5, tzinfo=IST)        # Wed, inside the seeded 10:00–10:30 window
OWNER_CHAT = 4242
OWNER_OK = OwnerConfirmation(actor=Actor.OWNER, confirmed=True, note="chaos case 5 seed")
CAPITAL = Decimal("20000")
USAGE = {"input_tokens": 1500, "output_tokens": 300}


# =========================================================================== the SDK boundary
@dataclass
class _Options:
    """Stand-in for ``ClaudeAgentOptions``: every knob the harness probes for (§5.1/D10)."""

    model: str | None = None
    system_prompt: str | None = None
    setting_sources: list[str] | None = None
    max_output_tokens: int | None = None
    max_turns: int | None = None
    allowed_tools: list[str] | None = None
    disallowed_tools: list[str] | None = None
    output_schema: dict[str, Any] | None = None


class _Block:
    def __init__(self, **kw: Any) -> None:
        self.__dict__.update(kw)


class _AssistantMessage:
    def __init__(self, text: str) -> None:
        self.content = [_Block(text=text)]
        self.usage = None


class _StructuredMessage:
    """The CLI's json_schema path: a forced ``StructuredOutput`` tool call carries the payload."""

    def __init__(self, payload: dict[str, Any]) -> None:
        self.content = [_Block(name="StructuredOutput", input=payload)]
        self.usage = None


class _ResultMessage:
    """Shaped like the SDK ResultMessage (cumulative usage; ``is_error`` for a CLI-reported failure)."""

    def __init__(self, *, usage: dict[str, int] | None = None, is_error: bool = False,
                 result: str | None = None) -> None:
        self.usage = usage
        self.total_cost_usd = 0.0
        self.is_error = is_error
        self.result = result
        self.subtype = "success"


class _SDK:
    """Injected in place of ``claude_agent_sdk.query``. Per call it plays the next script (the last
    one repeats): a float sleeps (a hung CLI), an exception is raised mid-stream, anything else is
    yielded as an SDK message."""

    def __init__(self, *scripts: list[Any]) -> None:
        self.scripts = list(scripts)
        self.calls = 0

    def __call__(self, *, prompt: str, options: Any) -> Any:
        self.calls += 1
        return self._stream(self.scripts[min(self.calls, len(self.scripts)) - 1])

    @staticmethod
    async def _stream(script: list[Any]) -> Any:
        for item in script:
            if isinstance(item, (int, float)):
                await asyncio.sleep(item)
            elif isinstance(item, BaseException):
                raise item
            else:
                yield item


# =========================================================================== the Telegram boundary
class _Transport:
    """``app.bot`` stand-in: what actually reached the owner's phone."""

    def __init__(self) -> None:
        self.sent: list[str] = []

    async def send_message(self, chat_id: int, text: str) -> None:
        self.sent.append(text)


class _App:
    def __init__(self, bot: _Transport) -> None:
        self.bot = bot
        self.updater = None


# =========================================================================== world
def _passing_ctx(**overrides: Any) -> GateContext:
    """Every §7.1 enter rule passes (the gate suite's baseline, test_reco_pipeline.passing_ctx)."""
    base: dict[str, Any] = {
        "now": NOW, "mode": Mode.RECOMMEND, "risk_state": RiskState.NORMAL,
        "trade_window": TradeWindow(start=time(9, 30), end=time(15, 0), squareoff_buffer_min=5),
        "session_open": time(9, 15), "session_close": time(15, 30), "equity": CAPITAL,
        "day_mtm_pct": Decimal("0"), "sector_of": {"RELIANCE": "ENERGY"}, "ltp": Decimal("100"),
        "tick_age_s": 1.0, "index_tick_age_s": 1.0, "in_universe": True, "mis_candidate": True,
        "is_fno": True, "warmup_ready": True, "regime_ready": True, "clock_skew_ok": True,
    }
    return GateContext(**{**base, **overrides})


class _GateContextSeam:
    """The gate-context DATA seam (see module docstring) — returns a fixed all-pass context."""

    def __init__(self, ctx: GateContext) -> None:
        self.ctx = ctx

    async def build(self, symbol: str, side: str, style: str, d: Any) -> GateContext:
        return self.ctx


def _candidate(symbol: str = "RELIANCE", signal_id: str = "01SIGNAL") -> SignalCandidate:
    return SignalCandidate(
        signal_id=signal_id, strategy_id="orb", symbol=symbol, side="BUY", style="intraday",
        raw_levels=RawLevels(entry=Decimal("100"), stop=Decimal("99"), target=Decimal("103")),
        score=0.8, features_snapshot_id="01SNAP",
    )


ENTER_JSON: dict[str, Any] = {
    "action": "enter",
    "thesis": "Opening-range breakout with volume confirmation and a tight invalidation level.",
    "confidence": 0.7, "tradingsymbol": "RELIANCE", "exchange": "NSE", "side": "BUY",
    "style": "intraday", "entry_type": "LIMIT", "entry_price": "100", "stop_price": "99",
    "target_price": "103", "quantity": 10, "signal_id": "01SIGNAL", "strategy_id": "orb",
    "features_snapshot_id": "01SNAP",
}


class _World:
    """One engine's Phase-2 decision plane over temp stores, wired the way main.py wires it."""

    def __init__(self, conn, tmp_path, sdk: _SDK, *, timeout_s: float = 45.0) -> None:
        self.conn = conn
        self.ticker = [NOW]
        self.clock = Clock(time_source=lambda: self.ticker[0])
        self.bus = EventBus()
        self.calendar = NSECalendar(config_dir() / "calendar", self.clock, sqlite_conn=conn)
        self.mode = ModeManager(conn, self.clock, self.bus, self.calendar)
        self.kill = KillSwitch(conn, self.clock, self.bus)
        # Protected limits: a tmp copy of the SHIPPED limits.yaml, owner-registered (hash-verified load).
        cfg = tmp_path / "config"
        cfg.mkdir(parents=True)
        shutil.copyfile(config_dir() / "limits.yaml", cfg / "limits.yaml")
        protected = ProtectedStore(cfg, conn, self.clock)
        protected.register_initial("limits.yaml", OWNER_OK)
        self.limits = LimitsEngine(protected)
        self.governor = BudgetGovernor(conn, self.clock, self.calendar, PINNED_CFG, bus=self.bus)

        # Owner channel: a real journalling TelegramBot whose transport is the fake phone.
        self.phone = _Transport()
        self.bot = TelegramBot("t", owner_chat_id=OWNER_CHAT, clock=self.clock, mode_manager=self.mode,
                               kill_switch=self.kill, bus=self.bus, governor=self.governor, conn=conn)
        self.bot._app = _App(self.phone)
        self.bot.attach_bus(self.bus)              # main.py:3894 — budget/risk/kill/mode alerts

        async def alert(severity: str, message: str) -> None:        # main.py:480-492
            await self.bot.send(CatalogMessage(
                kind=MessageKind.LIMIT_BREACH, title="alert", body=message,
                severity=severity if severity in ("info", "warning", "critical") else "warning",
            ))

        async def notify(msg: CatalogMessage) -> None:               # main.py:494-505
            await self.bot.send(msg)

        self.sdk = sdk
        defs = {"intraday_analyst": AgentDef(
            agent_id="intraday_analyst", model="sonnet-4.6", shape="single_shot",
            tools_enabled=False, allowed_tools=[], max_output_tokens=1200, timeout_s=timeout_s,
        )}
        self.harness = AgentHarness(defs, self.governor, self.clock, conn, query_fn=sdk,
                                    alert=alert, options_cls=_Options)
        self.store = MarketStore(tmp_path / "market.duckdb", tmp_path / "parquet", self.clock).open()
        cost_model = CostModel.from_config()
        self.book = RecommendationBook(conn, self.clock, cost_model)
        self.gate = RiskGate(self.limits, cost_model, self.clock)
        self.ctx_seam = _GateContextSeam(_passing_ctx())
        self.pipeline = RecommendationPipeline(
            ContextAssembler(self.store, conn, self.clock, self.calendar), self.harness, defs,
            self.gate, self.ctx_seam, self.book, self.mode, self.kill, self.governor,
            ExposureTracker(conn, self.clock, CAPITAL), self.limits, notify, self.clock,
            self.calendar, conn, self.store,
        )

    async def start_recommend(self) -> None:
        await self.mode.request_transition(Mode.RECOMMEND, Actor.OWNER)

    def advance(self, **kw: Any) -> None:
        self.ticker[0] = self.ticker[0] + timedelta(**kw)

    async def offer(self, cand: SignalCandidate) -> bool:
        """What the bus + the 60 s forward-drain tick do: admit the candidate, then drain one slot."""
        await self.pipeline.on_signal_candidate(cand)
        return await self.pipeline.drain_forward_queue()

    # ------------------------------------------------------------------ observations
    def count(self, table: str) -> int:
        return int(self.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])

    def owner_messages(self) -> list[tuple[str, str, str]]:
        rows = self.conn.execute(
            "SELECT kind, title, body FROM notifications ORDER BY created_at, rowid"
        ).fetchall()
        return [(r["kind"], r["title"], r["body"]) for r in rows]

    def agent_calls(self) -> list[tuple[int, str | None]]:
        return [(r["ok"], r["fail_reason"]) for r in self.conn.execute(
            "SELECT ok, fail_reason FROM agent_calls ORDER BY rowid").fetchall()]

    def assert_no_proposal(self) -> None:
        assert self.count("proposals") == 0
        assert self.count("verdicts") == 0
        assert self.count("recommendations") == 0

    def assert_owner_alerted(self, reason: str) -> None:
        """D7: the owner hears of the failure — the pipeline's "Intraday analyst unavailable" and the
        harness's per-call Failed alert, both journalled AND on the phone."""
        msgs = self.owner_messages()
        unavailable = [m for m in msgs if m[1] == "Intraday analyst unavailable"]
        assert unavailable and reason in unavailable[0][2], msgs
        assert "Deterministic exits, stops and square-offs are unaffected (R1)" in unavailable[0][2]
        assert any(m[1] == "alert" and f"failed: {reason}" in m[2] for m in msgs), msgs
        assert any("Intraday analyst unavailable" in t for t in self.phone.sent)

    def assert_deterministic_tiers_untouched(self) -> None:
        """R1: an LLM failure changes nothing in Tier 2/3 — no mode change, no freeze, no kill."""
        assert self.mode.mode() == Mode.RECOMMEND
        assert self.mode.risk_state() == RiskState.NORMAL
        assert not self.kill.is_killed()
        assert self.count("risk_state_causes") == 0


@pytest.fixture
async def world_factory(conn, tmp_path):
    made: list[_World] = []

    async def _make(sdk: _SDK, **kw: Any) -> _World:
        w = _World(conn, tmp_path / f"w{len(made)}", sdk, **kw)
        made.append(w)
        await w.start_recommend()
        return w

    yield _make
    for w in made:
        w.store.close()


# =========================================================================== control
async def test_healthy_llm_control_produces_a_recommendation(world_factory) -> None:
    """Non-vacuity: with a healthy SDK this exact world turns the candidate into a delivered
    recommendation — so every "no proposal" below is the failure path, not broken wiring."""
    w = await world_factory(_SDK([_StructuredMessage(ENTER_JSON), _ResultMessage(usage=USAGE)]))

    assert await w.offer(_candidate()) is True
    assert w.sdk.calls == 1
    assert w.count("proposals") == 1 and w.count("recommendations") == 1
    assert w.agent_calls() == [(1, None)]
    assert any(k == MessageKind.RECOMMENDATION.value for k, _, _ in w.owner_messages())


# =========================================================================== D7 failure modes
async def test_llm_timeout_fails_to_no_proposal_and_alerts(world_factory) -> None:
    """A hung SDK call is abandoned at the agent timeout — one call, no retry, no proposal, alert."""
    w = await world_factory(_SDK([5.0, _StructuredMessage(ENTER_JSON)]), timeout_s=0.1)

    assert await w.offer(_candidate()) is True           # the analyst WAS reached, then timed out
    assert w.sdk.calls == 1                              # D7: a timeout never retries
    assert w.agent_calls() == [(0, "timeout")]           # R8: the failed call is still audited
    w.assert_no_proposal()
    w.assert_owner_alerted("timeout")
    w.assert_deterministic_tiers_untouched()


@pytest.mark.parametrize(
    "garbage",
    [
        _AssistantMessage("Sure! RELIANCE looks strong — BUY 10 at 100 with a stop at 99. ```json\n"
                          + json.dumps(ENTER_JSON) + "\n```"),
        _StructuredMessage({"action": "buy_now", "tradingsymbol": "RELIANCE", "quantity": 1000}),
    ],
    ids=["prose_wrapped_json", "out_of_schema_json"],
)
async def test_garbage_output_fails_to_no_proposal_and_alerts(world_factory, garbage) -> None:
    """Prose (even around valid JSON) and out-of-schema JSON are schema violations: at most TWO
    fresh retries (3 SDK calls, each audited), never a salvaged action, then no-proposal + alert."""
    w = await world_factory(_SDK([garbage, _ResultMessage(usage=USAGE)]))

    await w.offer(_candidate())
    assert w.sdk.calls == 3                               # 1 + MAX_SCHEMA_RETRIES(2)
    assert w.agent_calls() == [(0, "schema_invalid")] * 3
    w.assert_no_proposal()
    w.assert_owner_alerted("schema_invalid")
    w.assert_deterministic_tiers_untouched()


@pytest.mark.parametrize(
    ("sdk_failure", "reason", "latches_dg4"),
    [
        ([RuntimeError("Claude Code returned an error result: Credit balance is too low")],
         "credit_exhausted", True),
        ([_ResultMessage(is_error=True, result="You've hit your session limit · resets 2:30pm")],
         "overloaded", False),
    ],
    ids=["sdk_billing_error", "subscription_session_limit"],
)
async def test_quota_exhaustion_fails_to_no_proposal_and_alerts(
    world_factory, sdk_failure, reason, latches_dg4
) -> None:
    """Credit/quota exhaustion is a no-retry D7 failure. A BILLING error additionally latches DG4 for
    the quota window (§5.6: the ledger can read healthy at the moment the credit ran out), so the next
    candidate is refused by the governor with no SDK call at all; the transient session limit does not."""
    w = await world_factory(_SDK(sdk_failure))

    await w.offer(_candidate())
    assert w.sdk.calls == 1
    assert w.agent_calls() == [(0, reason)]
    w.assert_no_proposal()
    w.assert_owner_alerted(reason)
    w.assert_deterministic_tiers_untouched()

    assert (w.governor.degrade_tier() == DegradeTier.DG4) is latches_dg4
    if latches_dg4:
        w.advance(minutes=4)                               # past FORWARD_PACING_MIN, still in window
        await w.offer(_candidate("TCS", "01SIGNAL2"))
        assert w.sdk.calls == 1                            # DG4: zero SDK calls
        w.assert_no_proposal()
        w.assert_deterministic_tiers_untouched()


async def test_dg4_takes_the_same_no_proposal_path(world_factory) -> None:
    """DG4 by spend (window spend ≥ the weekly credit): the tier change is Telegram-alerted (R8/§5.6)
    and every Tier-1 path — a queued candidate at the drain, a new arrival, the heartbeat — resolves
    to no proposal WITHOUT an SDK call. The governor-blocked refusal is audited (R8)."""
    w = await world_factory(_SDK([_StructuredMessage(ENTER_JSON), _ResultMessage(usage=USAGE)]))
    await w.pipeline.on_signal_candidate(_candidate())    # queued BEFORE the budget runs out

    # Another agent's billing exhausts the $100 pinned weekly credit (34M sonnet input tokens = $102).
    tier = await w.governor.record("weekly_researcher", "sonnet-4.6",
                                   TokenUsage(in_tokens=34_000_000, out_tokens=0))
    assert tier == DegradeTier.DG4
    budget_alerts = [m for m in w.owner_messages() if m[0] == MessageKind.BUDGET_WARNING.value]
    assert budget_alerts and "DG4" in budget_alerts[0][1], w.owner_messages()
    assert any("DG4" in t for t in w.phone.sent)

    assert await w.pipeline.drain_forward_queue() is False   # queued candidate: blocked at the drain
    w.advance(minutes=4)
    await w.offer(_candidate("TCS", "01SIGNAL2"))              # new arrival: blocked at admission
    await w.pipeline.heartbeat()                               # trigger (c): blocked too
    assert w.sdk.calls == 0                                    # zero SDK calls under DG4
    w.assert_no_proposal()
    w.assert_deterministic_tiers_untouched()

    # A direct harness call under DG4 is refused before the SDK and still leaves an audit row (R8).
    ctx = ContextAssembler(w.store, w.conn, w.clock, w.calendar).for_heartbeat(
        regime_lines=[], open_positions_summary="none")
    result = await w.harness.run_single_shot(w.harness.defs["intraday_analyst"], ctx, json.loads)
    assert not result.ok and result.reason == "governor_blocked"
    assert w.agent_calls()[-1] == (0, "governor_blocked") and w.sdk.calls == 0


# =========================================================================== R1: exits unaffected
async def test_deterministic_exit_unaffected_with_the_llm_dead(world_factory, conn) -> None:
    """R1: the §7.1 ``max_holding`` time-stop is built in Python — with the SDK raising on every call
    AND the ladder at DG4, an aged swing position still gets its gate-approved EXIT recommendation
    delivered to the owner, and the SDK is never called."""
    w = await world_factory(_SDK([RuntimeError("claude CLI process died (exit 1)")]))
    await w.offer(_candidate())                                  # the LLM is dead...
    w.assert_owner_alerted("sdk_error")
    w.governor.note_billing_error("chaos: credit exhausted")    # ...and the ladder is at DG4
    assert w.governor.degrade_tier() == DegradeTier.DG4
    calls_before = w.sdk.calls

    position_id = "01AGEDPOSITION"
    conn.execute(
        "INSERT INTO positions (position_id, symbol, side, style, product, qty, avg_entry, stop, "
        "state, origin, opened_at) VALUES (?, 'RELIANCE', 'BUY', 'swing', 'CNC', 10, '100', '95', "
        "'OPEN', 'recommended', ?)",
        (position_id, datetime(2026, 3, 2, 10, 0, tzinfo=IST).isoformat()),   # ≫ 20 sessions back
    )
    w.ctx_seam.ctx = _passing_ctx(positions_known=frozenset({position_id}))

    issued = await w.pipeline.time_exit_check(w.clock.today())

    assert issued == 1
    assert w.sdk.calls == calls_before                           # exits never touch Tier 1 (R1)
    rec = json.loads(conn.execute("SELECT payload FROM recommendations").fetchone()[0])
    assert rec["kind"] == "exit" and rec["qty"] == 10 and rec["instrument"] == "RELIANCE"
    proposal = json.loads(conn.execute("SELECT payload FROM proposals").fetchone()[0])
    assert proposal["reason"] == "time_stop" and proposal["agent_id"] == "platform"
    assert conn.execute("SELECT verdict FROM verdicts").fetchone()[0] in ("approve", "shrink")
    assert any(k == MessageKind.RECOMMENDATION.value for k, _, _ in w.owner_messages())
    w.assert_deterministic_tiers_untouched()


@pytest.mark.skip(reason=(
    "square-offs unaffected (R1): the window-end MIS square-off / 15:05-15:10 backstop is the "
    "SquareOffScheduler. " + MIS_OUT_OF_PAPER_SCOPE
))
async def test_square_off_unaffected_with_the_llm_dead() -> None:
    """R1: with the SDK dead / DG4, the scheduled square-off still fires on time."""
