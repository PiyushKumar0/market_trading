"""AgentHarness (§3.2.6/§5.1): the single SDK call site — admission, retries, metering, audit.

The Claude Agent SDK is NEVER imported here. Both SDK seams (``query`` and the options class) are
injected, so these tests exercise the real harness logic against fake message streams — which is also
how the harness is meant to be usable at the unit-test tier at all (the SDK spawns a CLI subprocess).

Fake message objects deliberately use the SDK's own field names (``input_tokens``,
``cache_read_input_tokens``, ``total_cost_usd``) so the defensive usage extraction is tested against the
shape it will actually meet, not against our internal ``TokenUsage`` names.
"""

from __future__ import annotations

import asyncio
import gzip
import json
from dataclasses import dataclass, field
from datetime import timedelta
from decimal import Decimal
from typing import Any

import pytest
from ulid import ULID

from engine.core.calendar import NSECalendar
from engine.core.config import config_dir, load_yaml
from engine.core.enums import DegradeTier
from engine.intelligence.governor import BudgetGovernor, TokenUsage
from engine.intelligence.harness import (
    MODEL_API_IDS,
    AgentDef,
    AgentHarness,
    RunCaps,
    load_agent_defs,
)
from engine.intelligence.schemas import parse_and_stamp

# --------------------------------------------------------------------------- fakes


@dataclass
class FakeOptions:
    """Stand-in for ClaudeAgentOptions exposing every knob the harness probes for."""

    model: str | None = None
    system_prompt: str | None = None
    setting_sources: list[str] | None = None
    max_output_tokens: int | None = None
    max_turns: int | None = None
    allowed_tools: list[str] | None = None
    disallowed_tools: list[str] | None = None
    output_schema: dict[str, Any] | None = None


@dataclass
class NoToolOptions:
    """An options surface with NO tool knob at all (a hypothetical SDK release)."""

    model: str | None = None
    system_prompt: str | None = None
    setting_sources: list[str] | None = None


class FakeAssistantMessage:
    def __init__(self, text: str, usage: dict[str, int] | None = None) -> None:
        self.content = [type("Block", (), {"text": text})()]
        self.usage = usage


class FakeStructuredToolMessage:
    """Assistant message whose content is a StructuredOutput TOOL call (the CLI's json_schema path):
    the validated payload is the tool call's ``input``, not assistant text."""

    def __init__(self, payload: dict[str, Any], usage: dict[str, int] | None = None) -> None:
        block = type("ToolUseBlock", (), {})()
        block.name = "StructuredOutput"
        block.input = payload
        self.content = [block]
        self.usage = usage


class FakeResultMessage:
    """Shaped like the SDK's ResultMessage: cumulative usage + its own cost estimate."""

    def __init__(self, usage: dict[str, int] | None = None, total_cost_usd: float | None = 0.0) -> None:
        self.usage = usage
        self.total_cost_usd = total_cost_usd


@dataclass
class _Call:
    prompt: str
    options: Any


class FakeQuery:
    """Injected in place of ``claude_agent_sdk.query``.

    Each script is a list of messages; a float entry sleeps (timeout tests) and an exception entry is
    raised from inside the stream (SDK-failure tests). The last script repeats once exhausted.
    """

    def __init__(self, *scripts: list[Any]) -> None:
        self.scripts = list(scripts)
        self.calls: list[_Call] = []
        self.yielded = 0
        self.closed = 0

    def __call__(self, *, prompt: str, options: Any) -> Any:
        self.calls.append(_Call(prompt=prompt, options=options))
        script = self.scripts[min(len(self.calls) - 1, len(self.scripts) - 1)]
        return self._stream(script)

    async def _stream(self, script: list[Any]) -> Any:
        try:
            for item in script:
                if isinstance(item, (int, float)):
                    await asyncio.sleep(item)
                elif isinstance(item, BaseException):
                    raise item
                else:
                    self.yielded += 1
                    yield item
        finally:
            self.closed += 1


@dataclass
class FakeContext:
    """Duck-typed AssembledContext (ContextAssembler lands separately)."""

    prompt_text: str = "DAY PLAN / CANDIDATE / VOLATILE BLOCK LAST"
    system_prompt: str = "SYSTEM PROMPT (byte-stable)"
    inputs_digest: str = "digest-abc123"
    call_class: str = "signal"

    def prompt(self) -> str:
        return self.prompt_text


# --------------------------------------------------------------------------- config / fixtures

TEST_AGENTS: dict[str, Any] = {
    "agents": {
        "intraday_analyst": {
            "model": "sonnet-4.6",
            "shape": "single_shot",
            "tools_enabled": False,
            "max_output_tokens": 1200,
            "timeout_s": 45,
        },
        "nightly_reviewer": {
            "model": "sonnet-4.6",
            "shape": "agentic",
            "tools_enabled": True,
            "allowed_tools": ["mcp__state__query"],
            "max_turns": 3,
            "max_output_tokens": 6000,
            "timeout_s": 5,
            "run_budget_tokens": {"billed_in_max": 120000, "billed_out_max": 500},
        },
    }
}

ENTER_PAYLOAD = {
    "action": "enter",
    "tradingsymbol": "RELIANCE",
    "exchange": "NSE",
    "side": "BUY",
    "style": "intraday",
    "entry_type": "LIMIT",
    "entry_price": "1420.50",
    "stop_price": "1405.00",
    "target_price": "1455.00",
    "quantity": 10,
    "signal_id": "SIG-1",
    "strategy_id": "orb",
    "features_snapshot_id": "FS-1",
    "thesis": "Opening-range break on above-average volume with a stop below the range low.",
    "confidence": 0.62,
}
ENTER_JSON = json.dumps(ENTER_PAYLOAD)

USAGE_SDK = {
    "input_tokens": 1200,
    "output_tokens": 300,
    "cache_read_input_tokens": 7000,
    "cache_creation_input_tokens": 0,
}


@pytest.fixture
def calendar(clock) -> NSECalendar:
    return NSECalendar(config_dir() / "calendar", clock, strict=False)


@pytest.fixture
def real_cfg() -> dict:
    return load_yaml(config_dir() / "agents.yaml")


@pytest.fixture
def gov(conn, clock, calendar, real_cfg) -> BudgetGovernor:
    return BudgetGovernor(conn, clock, calendar, real_cfg)


@pytest.fixture
def defs() -> dict[str, AgentDef]:
    return load_agent_defs(TEST_AGENTS)


@pytest.fixture
def alerts() -> list[tuple[str, str]]:
    return []


@pytest.fixture
def alert(alerts):
    async def _alert(severity: str, message: str) -> None:
        alerts.append((severity, message))

    return _alert


def make_harness(defs, gov, clock, conn, query_fn, alert=None, options_cls=FakeOptions) -> AgentHarness:
    return AgentHarness(
        defs, gov, clock, conn, query_fn=query_fn, alert=alert, options_cls=options_cls
    )


def enter_validator(clock):
    """The real §8.1 client-side contract: parse + platform-stamp (never LLM-stamped time)."""

    def _validate(raw: str) -> Any:
        return parse_and_stamp(
            raw,
            proposal_id=str(ULID()),
            agent_id="intraday_analyst",
            valid_until=clock.now() + timedelta(minutes=15),
            inputs_digest="digest-abc123",
        )

    return _validate


def strict_json_validator(raw: str) -> dict:
    data = json.loads(raw)
    if data.get("action") != "no_action":
        raise ValueError("action must be 'no_action'")
    return data


def rows(conn) -> list:
    return conn.execute("SELECT * FROM agent_calls ORDER BY rowid").fetchall()


# --------------------------------------------------------------------------- load_agent_defs (§9.1)
def test_tools_enabled_without_explicit_allowlist_is_refused() -> None:
    # §5.1 D5/D10: `tools_enabled: true` NEVER means "SDK defaults" — the harness refuses to run an
    # agent definition that lacks an explicit allowlist. This is the §9.1 allowlist test.
    cfg = {
        "agents": {
            "rogue": {"model": "sonnet-4.6", "shape": "agentic", "tools_enabled": True, "max_turns": 4}
        }
    }
    with pytest.raises(ValueError, match="allowed_tools"):
        load_agent_defs(cfg)


def test_single_shot_defs_get_empty_allowlist(real_cfg) -> None:
    # Against the SHIPPED config: every single-shot agent runs with no tools (D5).
    single_shot = {k: v for k, v in real_cfg["agents"].items() if v["shape"] == "single_shot"}
    assert single_shot, "agents.yaml has no single_shot agents — fixture is vacuous"
    loaded = load_agent_defs({"agents": single_shot})
    assert set(loaded) == set(single_shot)
    for agent_def in loaded.values():
        assert agent_def.allowed_tools == []
        assert agent_def.tools_enabled is False
        assert agent_def.api_model == MODEL_API_IDS[agent_def.model]


def test_shipped_roster_loads_with_weekly_parked(real_cfg) -> None:
    # The shipped roster loads whole: nightly_reviewer is single-shot in Phase-2 v1 (agents.yaml
    # deviation note), and weekly_researcher is `enabled: false` until its §5.5 read-only MCP
    # allowlist is wired (Phase 5) — a parked def is SKIPPED, never silently runnable (D5/D10).
    defs = load_agent_defs(real_cfg)
    assert "weekly_researcher" not in defs
    nightly = defs["nightly_reviewer"]
    assert nightly.shape == "single_shot"
    assert nightly.tools_enabled is False
    assert nightly.allowed_tools == []


def test_disabled_def_with_missing_allowlist_does_not_poison_load() -> None:
    cfg = {"agents": {
        "ok": {"model": "haiku-4.5", "shape": "single_shot", "tools_enabled": False},
        "parked": {"enabled": False, "model": "opus-4.8", "shape": "agentic", "tools_enabled": True,
                   "max_turns": 5, "run_budget_tokens": {"billed_in_max": 1000, "billed_out_max": 100}},
    }}
    defs = load_agent_defs(cfg)
    assert set(defs) == {"ok"}


def test_unknown_model_name_is_loud() -> None:
    cfg = {"agents": {"x": {"model": "sonnet-9.9", "shape": "single_shot", "tools_enabled": False}}}
    with pytest.raises(Exception, match="unknown model name"):
        load_agent_defs(cfg)


def test_single_shot_may_not_declare_tools() -> None:
    cfg = {
        "agents": {
            "x": {
                "model": "haiku-4.5",
                "shape": "single_shot",
                "tools_enabled": False,
                "allowed_tools": ["WebSearch"],
            }
        }
    }
    with pytest.raises(Exception, match="single-shot"):
        load_agent_defs(cfg)


def test_agentic_def_requires_caps() -> None:
    cfg = {
        "agents": {
            "x": {
                "model": "opus-4.8",
                "shape": "agentic",
                "tools_enabled": True,
                "allowed_tools": ["mcp__x__y"],
                "max_turns": 5,
            }
        }
    }
    with pytest.raises(Exception, match="run_budget_tokens"):
        load_agent_defs(cfg)


# --------------------------------------------------------------------------- happy path
async def test_single_shot_ok_persists_call_and_meters_budget(defs, gov, clock, conn) -> None:
    fake = FakeQuery([FakeAssistantMessage(ENTER_JSON), FakeResultMessage(USAGE_SDK, 0.031)])
    harness = make_harness(defs, gov, clock, conn, fake)

    result = await harness.run_single_shot(
        defs["intraday_analyst"], FakeContext(), enter_validator(clock), json_schema={"type": "object"}
    )

    assert result.ok and result.reason is None
    assert result.attempts == 1
    assert result.payload.tradingsymbol == "RELIANCE"
    assert result.payload.quantity == 10
    # Platform-stamped, never LLM-stamped (§5.1 no LLM-originated time).
    assert result.payload.valid_until == clock.now() + timedelta(minutes=15)
    assert result.usage == TokenUsage(in_tokens=1200, out_tokens=300, cache_read=7000)

    # --- audit row (R8) ---
    (row,) = rows(conn)
    assert row["call_id"] == result.call_id
    assert row["agent_id"] == "intraday_analyst"
    assert row["trigger"] == "signal"              # context.call_class
    assert row["model"] == "sonnet-4.6"            # yaml name — joins to budget_ledger.model
    assert row["inputs_digest"] == "digest-abc123"
    assert row["ok"] == 1 and row["fail_reason"] is None
    assert (row["in_tokens"], row["out_tokens"], row["cache_read"]) == (1200, 300, 7000)
    assert Decimal(row["cost_usd"]) == gov.price("sonnet-4.6", result.usage)
    assert row["at"] == clock.now().isoformat()
    assert row["duration_ms"] >= 0
    snapshot = gzip.decompress(row["context_gz"]).decode()
    assert "SYSTEM PROMPT (byte-stable)" in snapshot and "VOLATILE BLOCK LAST" in snapshot
    assert json.loads(row["output_json"])["tradingsymbol"] == "RELIANCE"

    # --- governor ledger (D6) ---
    (ledger,) = conn.execute("SELECT * FROM budget_ledger").fetchall()
    assert ledger["agent_id"] == "intraday_analyst"
    assert ledger["model"] == "sonnet-4.6"
    assert (ledger["in_tokens"], ledger["out_tokens"], ledger["cache_read"]) == (1200, 300, 7000)
    assert gov.month_spend() == gov.price("sonnet-4.6", result.usage)


async def test_options_carry_model_setting_sources_and_empty_allowlist(defs, gov, clock, conn) -> None:
    fake = FakeQuery([FakeAssistantMessage(ENTER_JSON), FakeResultMessage(USAGE_SDK)])
    harness = make_harness(defs, gov, clock, conn, fake)

    await harness.run_single_shot(defs["intraday_analyst"], FakeContext(), enter_validator(clock),
                                  json_schema={"type": "object"})

    opts = fake.calls[0].options
    assert opts.model == "claude-sonnet-4-6"       # D4: explicit API id, never an SDK default
    assert opts.setting_sources == []              # D10: load NOTHING from the filesystem
    assert opts.allowed_tools == []                # D5: single-shot agents get no tools
    assert "WebSearch" in opts.disallowed_tools and "Bash" in opts.disallowed_tools
    assert opts.max_output_tokens == 1200
    # The schema knob was sent, so the StructuredOutput tool round-trip gets its extra turn
    # (observed live 2026-07-28: max_turns=1 + json_schema => error_max_turns on every call).
    assert opts.max_turns == 4
    assert opts.system_prompt == "SYSTEM PROMPT (byte-stable)"
    assert opts.output_schema == {"type": "object"}
    # System prompt went in the options, so the prompt is exactly the assembled context block (D8).
    assert fake.calls[0].prompt == FakeContext().prompt_text


async def test_usage_extraction_from_result_message_only(defs, gov, clock, conn) -> None:
    # Assistant messages carry no usage; the ResultMessage carries the cumulative billed usage.
    fake = FakeQuery(
        [
            FakeAssistantMessage(ENTER_JSON),
            FakeResultMessage(
                {
                    "input_tokens": 900,
                    "output_tokens": 250,
                    "cache_read_input_tokens": 4096,
                    "cache_creation_input_tokens": 2048,
                },
                total_cost_usd=0.0271,
            ),
        ]
    )
    harness = make_harness(defs, gov, clock, conn, fake)

    result = await harness.run_single_shot(defs["intraday_analyst"], FakeContext(), enter_validator(clock))

    assert result.usage == TokenUsage(in_tokens=900, out_tokens=250, cache_read=4096, cache_write=2048)
    (ledger,) = conn.execute("SELECT * FROM budget_ledger").fetchall()
    assert (ledger["cache_read"], ledger["cache_write"]) == (4096, 2048)
    # Our D4 pricing owns the ledger; the SDK's own estimate is never written to it.
    assert Decimal(ledger["cost_usd"]) == gov.price("sonnet-4.6", result.usage)


async def test_missing_usage_fields_default_to_zero_and_book_nothing(defs, gov, clock, conn) -> None:
    fake = FakeQuery([FakeAssistantMessage(ENTER_JSON)])   # no usage anywhere
    harness = make_harness(defs, gov, clock, conn, fake)

    result = await harness.run_single_shot(defs["intraday_analyst"], FakeContext(), enter_validator(clock))

    assert result.ok
    assert result.usage == TokenUsage(in_tokens=0, out_tokens=0)
    assert conn.execute("SELECT COUNT(*) c FROM budget_ledger").fetchone()["c"] == 0
    assert rows(conn)[0]["in_tokens"] == 0


# --------------------------------------------------------------------------- schema retries (D7)
async def test_schema_invalid_twice_then_valid(defs, gov, clock, conn, alert, alerts) -> None:
    good = json.dumps({"action": "no_action", "reason": "regime unclear"})
    fake = FakeQuery(
        [FakeAssistantMessage('{"action": "enter"}'), FakeResultMessage(USAGE_SDK)],
        [FakeAssistantMessage("not json at all"), FakeResultMessage(USAGE_SDK)],
        [FakeAssistantMessage(good), FakeResultMessage(USAGE_SDK)],
    )
    harness = make_harness(defs, gov, clock, conn, fake, alert=alert)

    result = await harness.run_single_shot(defs["intraday_analyst"], FakeContext(), strict_json_validator)

    assert result.ok and result.attempts == 3
    assert result.payload == {"action": "no_action", "reason": "regime unclear"}
    assert len(fake.calls) == 3                       # 1 + 2 retries (D7)

    # Each retry is a FRESH query whose prompt carries the previous validation error verbatim.
    assert "Previous output failed validation" not in fake.calls[0].prompt
    assert "action must be 'no_action'" in fake.calls[1].prompt
    assert "not valid JSON" in fake.calls[2].prompt
    assert "Emit ONLY valid JSON matching the schema." in fake.calls[1].prompt
    # Retries do not compound: the tail is appended to the ORIGINAL context, not to the last retry.
    assert fake.calls[2].prompt.startswith(FakeContext().prompt_text)
    assert fake.calls[2].prompt.count("Previous output failed validation") == 1

    # One audit row per SDK call (each has its own prompt/usage/cost — R8 replayability).
    audit = rows(conn)
    assert [r["ok"] for r in audit] == [0, 0, 1]
    assert [r["fail_reason"] for r in audit] == ["schema_invalid", "schema_invalid", None]
    assert "Previous output failed validation" in gzip.decompress(audit[1]["context_gz"]).decode()
    assert conn.execute("SELECT COUNT(*) c FROM budget_ledger").fetchone()["c"] == 3
    assert not alerts                                  # the run succeeded; no owner alert


async def test_schema_invalid_three_times_fails(defs, gov, clock, conn, alert, alerts) -> None:
    fake = FakeQuery([FakeAssistantMessage('{"action": "enter"}'), FakeResultMessage(USAGE_SDK)])
    harness = make_harness(defs, gov, clock, conn, fake, alert=alert)

    result = await harness.run_single_shot(defs["intraday_analyst"], FakeContext(), strict_json_validator)

    assert not result.ok
    assert result.reason == "schema_invalid"
    assert "action must be 'no_action'" in result.detail
    assert len(fake.calls) == 3                       # never a 4th (D7)
    audit = rows(conn)
    assert len(audit) == 3 and all(r["ok"] == 0 for r in audit)
    assert audit[-1]["call_id"] == result.call_id
    assert alerts and alerts[0][0] == "error" and "schema_invalid" in alerts[0][1]


async def test_prose_response_is_never_salvaged(defs, gov, clock, conn) -> None:
    # D7: an answer that MIXES prose with a fence is a schema violation, not something to regex out.
    fenced = f"Here is my answer:\n```json\n{ENTER_JSON}\n```"
    fake = FakeQuery([FakeAssistantMessage(fenced), FakeResultMessage(USAGE_SDK)])
    harness = make_harness(defs, gov, clock, conn, fake)

    result = await harness.run_single_shot(defs["intraday_analyst"], FakeContext(), enter_validator(clock))

    assert not result.ok and result.reason == "schema_invalid"
    assert "not valid JSON" in result.detail


async def test_lone_fenced_block_is_unwrapped(defs, gov, clock, conn) -> None:
    """2026-07-29: when the structured-output knob silently disengages (union schema), the model
    answers in a BARE fenced block — that is the JSON in the CLI's habitual framing, not prose.
    A whole-string fence unwraps; anything around it still fails (previous test)."""
    fake = FakeQuery([FakeAssistantMessage(f"```json\n{ENTER_JSON}\n```"), FakeResultMessage(USAGE_SDK)])
    harness = make_harness(defs, gov, clock, conn, fake)

    result = await harness.run_single_shot(defs["intraday_analyst"], FakeContext(), enter_validator(clock))

    assert result.ok
    assert result.payload.tradingsymbol == "RELIANCE"


# --------------------------------------------------------------------------- structured output (CLI json_schema path)
async def test_structured_tool_payload_wins_over_trailing_text(defs, gov, clock, conn) -> None:
    # The CLI fulfils json_schema via a StructuredOutput TOOL call; the payload is the tool input.
    # Any assistant text after the tool ack is commentary and must never shadow the payload.
    fake = FakeQuery(
        [
            FakeStructuredToolMessage(ENTER_PAYLOAD, USAGE_SDK),
            FakeAssistantMessage("I have provided the structured output."),
            FakeResultMessage(USAGE_SDK, 0.02),
        ]
    )
    harness = make_harness(defs, gov, clock, conn, fake)

    result = await harness.run_single_shot(
        defs["intraday_analyst"], FakeContext(), enter_validator(clock), json_schema={"type": "object"}
    )

    assert result.ok
    assert result.payload.tradingsymbol == "RELIANCE"
    (row,) = rows(conn)
    assert json.loads(row["output_json"])["tradingsymbol"] == "RELIANCE"


async def test_no_schema_means_no_turn_bump(defs, gov, clock, conn) -> None:
    fake = FakeQuery([FakeAssistantMessage(ENTER_JSON), FakeResultMessage(USAGE_SDK)])
    harness = make_harness(defs, gov, clock, conn, fake)

    await harness.run_single_shot(defs["intraday_analyst"], FakeContext(), enter_validator(clock))

    assert fake.calls[0].options.max_turns == 1        # no schema knob sent => single turn as ever
    assert fake.calls[0].options.output_schema is None


async def test_schema_without_knob_keeps_single_turn(defs, gov, clock, conn) -> None:
    # An options surface with max_turns but NO schema knob: the schema is not sent, so the turn
    # cap must stay 1 — a bump would let a no-tools agent idle through a second turn for nothing.
    @dataclass
    class NoSchemaKnob:
        model: str | None = None
        system_prompt: str | None = None
        setting_sources: list[str] | None = None
        max_turns: int | None = None
        allowed_tools: list[str] | None = None
        disallowed_tools: list[str] | None = None

    fake = FakeQuery([FakeAssistantMessage(ENTER_JSON), FakeResultMessage(USAGE_SDK)])
    harness = make_harness(defs, gov, clock, conn, fake, options_cls=NoSchemaKnob)

    result = await harness.run_single_shot(
        defs["intraday_analyst"], FakeContext(), enter_validator(clock), json_schema={"type": "object"}
    )

    assert result.ok
    assert fake.calls[0].options.max_turns == 1


async def test_output_format_fallback_mapping_and_turn_bump(defs, gov, clock, conn) -> None:
    # The shipped SDK (0.2.x) exposes `output_format`, not `output_schema` — the schema is wrapped
    # and the tool round-trip still needs its extra turn.
    @dataclass
    class OutputFormatOptions:
        model: str | None = None
        system_prompt: str | None = None
        setting_sources: list[str] | None = None
        max_turns: int | None = None
        allowed_tools: list[str] | None = None
        disallowed_tools: list[str] | None = None
        output_format: dict[str, Any] | None = None

    fake = FakeQuery(
        [FakeStructuredToolMessage(ENTER_PAYLOAD, USAGE_SDK), FakeResultMessage(USAGE_SDK)]
    )
    harness = make_harness(defs, gov, clock, conn, fake, options_cls=OutputFormatOptions)

    result = await harness.run_single_shot(
        defs["intraday_analyst"], FakeContext(), enter_validator(clock), json_schema={"type": "object"}
    )

    assert result.ok
    opts = fake.calls[0].options
    assert opts.output_format == {"type": "json_schema", "schema": {"type": "object"}}
    assert opts.max_turns == 4


async def test_thinking_capped_to_zero_when_knob_exists(defs, gov, clock, conn) -> None:
    # The CLI defaults to extended thinking — billed at output rates and measured to add 15-50s to
    # single-shot scoring calls (2026-07-28). The harness pins it off wherever the knob exists.
    @dataclass
    class ThinkingOptions:
        model: str | None = None
        system_prompt: str | None = None
        setting_sources: list[str] | None = None
        max_turns: int | None = None
        allowed_tools: list[str] | None = None
        disallowed_tools: list[str] | None = None
        max_thinking_tokens: int | None = None

    fake = FakeQuery([FakeAssistantMessage(ENTER_JSON), FakeResultMessage(USAGE_SDK)])
    harness = make_harness(defs, gov, clock, conn, fake, options_cls=ThinkingOptions)

    await harness.run_single_shot(defs["intraday_analyst"], FakeContext(), enter_validator(clock))

    assert fake.calls[0].options.max_thinking_tokens == 0


# --------------------------------------------------------------------------- timeout / SDK failures
async def test_timeout_fails_without_retry(defs, gov, clock, conn, alert, alerts) -> None:
    quick = defs["intraday_analyst"].model_copy(update={"timeout_s": 0.05})
    fake = FakeQuery([5.0, FakeAssistantMessage(ENTER_JSON)])      # sleeps well past the timeout
    harness = make_harness(defs, gov, clock, conn, fake, alert=alert)

    result = await harness.run_single_shot(quick, FakeContext(), enter_validator(clock))

    assert not result.ok and result.reason == "timeout"
    assert len(fake.calls) == 1                        # timeouts are never retried (D7)
    assert fake.closed == 1                            # the stream was closed, not leaked
    (row,) = rows(conn)
    assert row["ok"] == 0 and row["fail_reason"] == "timeout"
    assert conn.execute("SELECT COUNT(*) c FROM budget_ledger").fetchone()["c"] == 0
    assert alerts and "timeout" in alerts[0][1]


@pytest.mark.parametrize(
    ("exc", "reason"),
    [
        (RuntimeError("HTTP 429 Too Many Requests"), "overloaded"),
        (RuntimeError("api_error: model is overloaded"), "overloaded"),
        (RuntimeError("your credit balance is too low"), "credit_exhausted"),
        (RuntimeError("billing error: subscription lapsed"), "credit_exhausted"),
        (RuntimeError("CLI subprocess died"), "sdk_error"),
    ],
)
async def test_sdk_exceptions_map_to_failure_reasons(defs, gov, clock, conn, exc, reason) -> None:
    fake = FakeQuery([exc])
    harness = make_harness(defs, gov, clock, conn, fake)

    result = await harness.run_single_shot(defs["intraday_analyst"], FakeContext(), enter_validator(clock))

    assert not result.ok and result.reason == reason
    assert len(fake.calls) == 1                        # no retry on transport failures (D7)
    (row,) = rows(conn)
    assert row["ok"] == 0 and row["fail_reason"] == reason


async def test_credit_exhausted_trips_dg4(defs, gov, clock, conn) -> None:
    fake = FakeQuery([RuntimeError("credit balance exhausted")])
    harness = make_harness(defs, gov, clock, conn, fake)

    result = await harness.run_single_shot(defs["intraday_analyst"], FakeContext(), enter_validator(clock))

    assert result.reason == "credit_exhausted"
    # The ledger only sees calls that SUCCEEDED, so the billing error is the only DG4 signal (§5.6).
    assert gov.degrade_tier() == DegradeTier.DG4
    assert gov.can_invoke("intraday_analyst").allowed is False


# --------------------------------------------------------------------------- governor admission
async def test_governor_block_makes_no_sdk_call(defs, gov, clock, conn, alert, alerts) -> None:
    gov.note_billing_error("test")                     # => DG4 => zero SDK calls
    fake = FakeQuery([FakeAssistantMessage(ENTER_JSON), FakeResultMessage(USAGE_SDK)])
    harness = make_harness(defs, gov, clock, conn, fake, alert=alert)

    result = await harness.run_single_shot(defs["intraday_analyst"], FakeContext(), enter_validator(clock))

    assert not result.ok and result.reason == "governor_blocked"
    assert result.detail == "DG4_zero_sdk_calls"
    assert fake.calls == []                            # the SDK was never touched
    (row,) = rows(conn)                                # …but the refusal is still auditable (R8)
    assert row["ok"] == 0 and row["fail_reason"] == "governor_blocked"
    assert row["in_tokens"] == 0 and Decimal(row["cost_usd"]) == Decimal(0)
    assert conn.execute("SELECT COUNT(*) c FROM budget_ledger").fetchone()["c"] == 0
    assert alerts and "governor_blocked" in alerts[0][1]


async def test_call_class_drives_admission(defs, gov, clock, conn, real_cfg, calendar) -> None:
    # A heartbeat is refused from DG2 up while a signal call is still allowed (§5.6).
    async def spend(usd: str) -> None:
        await gov.record("weekly_researcher", "haiku-4.5", TokenUsage(in_tokens=int(Decimal(usd) * 10**6), out_tokens=0))

    await spend("86")                                  # > 85% of the $100 credit => DG2
    assert gov.degrade_tier() == DegradeTier.DG2
    fake = FakeQuery([FakeAssistantMessage(ENTER_JSON), FakeResultMessage(USAGE_SDK)])
    harness = make_harness(defs, gov, clock, conn, fake)

    blocked = await harness.run_single_shot(
        defs["intraday_analyst"], FakeContext(call_class="heartbeat"), enter_validator(clock)
    )
    assert blocked.reason == "governor_blocked" and blocked.detail == "DG2_heartbeats_off"
    assert fake.calls == []

    allowed = await harness.run_single_shot(
        defs["intraday_analyst"], FakeContext(call_class="signal"), enter_validator(clock)
    )
    assert allowed.ok and len(fake.calls) == 1


# --------------------------------------------------------------------------- tool-knob guarantees
async def test_missing_tool_knob_refuses_tools_enabled_agent(defs, gov, clock, conn) -> None:
    # Uniform D7 shape: the SDK-surface refusal is an ordinary Failed (callers degrade to
    # no-proposal + alert), never an exception leaning on bus isolation.
    fake = FakeQuery([FakeAssistantMessage("{}"), FakeResultMessage(USAGE_SDK)])
    harness = make_harness(defs, gov, clock, conn, fake, options_cls=NoToolOptions)

    result = await harness.run_agentic(defs["nightly_reviewer"], "review the day")

    assert not result.ok and result.reason == "sdk_error"
    assert "allowed-tools" in (result.detail or "")
    assert fake.calls == []


async def test_missing_tool_knob_refuses_single_shot_too(defs, gov, clock, conn) -> None:
    """2026-07-28 review F10: an options surface with NO tool knob means the SDK's DEFAULT builtin
    toolset would run — refused for EVERY shape (fail closed), not warned past for single-shot."""
    fake = FakeQuery([FakeAssistantMessage(ENTER_JSON), FakeResultMessage(USAGE_SDK)])
    harness = make_harness(defs, gov, clock, conn, fake, options_cls=NoToolOptions)

    result = await harness.run_single_shot(defs["intraday_analyst"], FakeContext(), enter_validator(clock))

    assert not result.ok
    assert result.reason == "sdk_error"
    assert fake.calls == []          # refused BEFORE any SDK call went out


async def test_system_prompt_falls_back_into_the_prompt_when_unsupported(defs, gov, clock, conn) -> None:
    @dataclass
    class Minimal:
        model: str | None = None
        setting_sources: list[str] | None = None
        allowed_tools: list[str] = field(default_factory=list)

    fake = FakeQuery([FakeAssistantMessage(ENTER_JSON), FakeResultMessage(USAGE_SDK)])
    harness = make_harness(defs, gov, clock, conn, fake, options_cls=Minimal)

    result = await harness.run_single_shot(defs["intraday_analyst"], FakeContext(), enter_validator(clock))

    assert result.ok
    # Stable system block FIRST, volatile context after it (D8 ordering survives the fallback).
    assert fake.calls[0].prompt.startswith("SYSTEM PROMPT (byte-stable)\n\n")
    assert fake.calls[0].prompt.endswith(FakeContext().prompt_text)


# --------------------------------------------------------------------------- agentic runs (§5.5)
async def test_agentic_run_aborts_when_billed_tokens_exceed_the_run_budget(defs, gov, clock, conn, alert, alerts) -> None:
    turn = {"input_tokens": 1000, "output_tokens": 300}
    fake = FakeQuery(
        [
            FakeAssistantMessage("turn one", turn),
            FakeAssistantMessage("turn two", turn),      # cumulative out = 600 > billed_out_max 500
            FakeAssistantMessage(json.dumps({"ok": True}), turn),
            FakeResultMessage({"input_tokens": 3000, "output_tokens": 900}),
        ]
    )
    harness = make_harness(defs, gov, clock, conn, fake, alert=alert)

    result = await harness.run_agentic(defs["nightly_reviewer"], "review 2026-06-17")

    assert not result.ok and result.reason == "sdk_error"
    assert "run budget exceeded" in result.detail
    assert fake.yielded == 2                            # consumption stopped mid-stream
    assert fake.closed == 1
    assert len(fake.calls) == 1                         # not retried
    (row,) = rows(conn)
    assert row["ok"] == 0 and row["fail_reason"] == "sdk_error"
    assert (row["in_tokens"], row["out_tokens"]) == (2000, 600)
    # Tokens burned before the abort are still metered (D6) — the run was billed for them.
    (ledger,) = conn.execute("SELECT * FROM budget_ledger").fetchall()
    assert ledger["out_tokens"] == 600
    assert alerts


async def test_agentic_run_within_budget_returns_ok(defs, gov, clock, conn) -> None:
    payload = json.dumps({"summary": "flat day", "param_suggestions": []})
    fake = FakeQuery(
        [
            FakeAssistantMessage("thinking", {"input_tokens": 500, "output_tokens": 40}),
            FakeAssistantMessage(payload, {"input_tokens": 600, "output_tokens": 120}),
            FakeResultMessage({"input_tokens": 1100, "output_tokens": 160}, total_cost_usd=0.006),
        ]
    )
    harness = make_harness(defs, gov, clock, conn, fake)

    result = await harness.run_agentic(defs["nightly_reviewer"], "review 2026-06-17", validate=json.loads)

    assert result.ok
    assert result.payload == {"summary": "flat day", "param_suggestions": []}
    assert result.usage == TokenUsage(in_tokens=1100, out_tokens=160)
    opts = fake.calls[0].options
    assert opts.max_turns == 3                          # hard turn cap from the def (D5)
    assert opts.allowed_tools == ["mcp__state__query"]  # exactly the §5.5 allowlist
    assert "mcp__state__query" not in opts.disallowed_tools
    assert "Bash" in opts.disallowed_tools
    assert opts.setting_sources == []


async def test_agentic_retries_are_bounded_by_the_same_run_budget(defs, gov, clock, conn) -> None:
    # D7 retries apply to agentic runs too, but the §5.5 cap is CUMULATIVE over the run: the second
    # attempt's tokens land on top of the first, so a retry can never buy a fresh budget.
    fake = FakeQuery(
        [
            FakeAssistantMessage("not json", {"input_tokens": 800, "output_tokens": 200}),
            FakeResultMessage({"input_tokens": 800, "output_tokens": 200}),
        ],
        [
            FakeAssistantMessage("still not json", {"input_tokens": 900, "output_tokens": 350}),
            FakeResultMessage({"input_tokens": 900, "output_tokens": 350}),
        ],
    )
    harness = make_harness(defs, gov, clock, conn, fake)

    result = await harness.run_agentic(defs["nightly_reviewer"], "task", validate=json.loads)

    assert not result.ok and result.reason == "sdk_error"
    assert "billed_out=550/500" in result.detail
    assert len(fake.calls) == 2                         # attempt 2 aborted on the carried budget
    audit = rows(conn)
    assert [r["fail_reason"] for r in audit] == ["schema_invalid", "sdk_error"]


async def test_agentic_caps_override_the_def(defs, gov, clock, conn) -> None:
    fake = FakeQuery(
        [
            FakeAssistantMessage("{}", {"input_tokens": 10, "output_tokens": 5}),
            FakeResultMessage({"input_tokens": 10, "output_tokens": 5}),
        ]
    )
    harness = make_harness(defs, gov, clock, conn, fake)

    result = await harness.run_agentic(
        defs["nightly_reviewer"], "task", RunCaps(max_turns=2, billed_in_max=50_000, timeout_s=3)
    )

    assert result.ok
    assert fake.calls[0].options.max_turns == 2


async def test_shape_guards(defs, gov, clock, conn) -> None:
    harness = make_harness(defs, gov, clock, conn, FakeQuery([]))
    with pytest.raises(ValueError, match="run_single_shot"):
        await harness.run_single_shot(defs["nightly_reviewer"], FakeContext(), json.loads)
    with pytest.raises(ValueError, match="run_agentic"):
        await harness.run_agentic(defs["intraday_analyst"], "task")


# --------------------------------------------------------------------------- D11 sdk smoke
async def test_run_sdk_smoke_happy_path(defs, gov, clock, conn) -> None:
    from engine.intelligence.harness import run_sdk_smoke

    fake = FakeQuery([FakeAssistantMessage('{"ok": true}'),
                      FakeResultMessage({"input_tokens": 40, "output_tokens": 6})])
    harness = make_harness(defs, gov, clock, conn, fake)
    detail = await run_sdk_smoke(harness)
    assert "SDK round-trip ok" in detail
    row = conn.execute("SELECT agent_id, ok FROM agent_calls ORDER BY at DESC LIMIT 1").fetchone()
    assert (row["agent_id"], row["ok"]) == ("sdk_smoke", 1)


async def test_run_sdk_smoke_exercises_the_structured_output_path(defs, gov, clock, conn) -> None:
    # D11 must smoke-test the path production agents actually use: every production call sends a
    # json_schema, so the smoke call sends one too (2026-07-28: a schema-less smoke passed WARN-only
    # while every real call failed on the untested structured-output path).
    from engine.intelligence.harness import run_sdk_smoke

    fake = FakeQuery([FakeStructuredToolMessage({"ok": True}),
                      FakeResultMessage({"input_tokens": 40, "output_tokens": 6})])
    harness = make_harness(defs, gov, clock, conn, fake)
    detail = await run_sdk_smoke(harness)
    assert "SDK round-trip ok" in detail
    opts = fake.calls[0].options
    assert opts.output_schema is not None              # the schema went out with the smoke call
    assert opts.max_turns == 4                         # tool choreography headroom included


async def test_run_sdk_smoke_raises_on_wrong_payload(defs, gov, clock, conn) -> None:
    from engine.intelligence.harness import run_sdk_smoke

    fake = FakeQuery([FakeAssistantMessage('{"ok": false}')])
    harness = make_harness(defs, gov, clock, conn, fake)
    with pytest.raises(RuntimeError, match="schema_invalid"):
        await run_sdk_smoke(harness)
