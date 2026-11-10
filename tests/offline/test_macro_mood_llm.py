"""scripts/macro_mood_llm.py: the study's pinned-model LLM wrapper. A fake ``query_fn`` only — never the SDK."""

from __future__ import annotations

import asyncio
import copy
import importlib.util
import sys
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest

_ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location("mt_macro_mood_llm", _ROOT / "scripts" / "macro_mood_llm.py")
mml = importlib.util.module_from_spec(_spec)
sys.modules["mt_macro_mood_llm"] = mml
_spec.loader.exec_module(mml)

from engine.core.config import config_dir, load_yaml  # noqa: E402
from engine.intelligence.harness import ISOLATION_ENV  # noqa: E402

HAIKU, SONNET = "claude-haiku-5-5", "claude-sonnet-5-5"
SCHEMA = {"type": "object", "properties": {"mood": {"type": "number"}}, "required": ["mood"]}
SESSION_LIMIT = "You've hit your session limit · resets 3pm (Asia/Calcutta)"


@dataclass
class FakeOptions:
    """The shipped SDK's ClaudeAgentOptions knobs the harness probes for (no SDK import)."""

    model: str | None = None
    fallback_model: str | None = None
    system_prompt: str | None = None
    setting_sources: list[str] | None = None
    max_turns: int | None = None
    tools: list[str] | None = None
    allowed_tools: list[str] = field(default_factory=list)
    disallowed_tools: list[str] = field(default_factory=list)
    output_format: dict[str, Any] | None = None
    effort: str | None = None
    env: dict[str, str] = field(default_factory=dict)
    strict_mcp_config: bool = False
    extra_args: dict[str, Any] = field(default_factory=dict)
    cli_path: str | None = None


class FakeResult:
    """Stand-in for ``ResultMessage``: the class name carries "Result", as the harness's check expects."""

    def __init__(self, structured_output: Any = None, *, is_error: bool = False, result: str | None = None,
                 subtype: str = "success", errors: list[str] | None = None) -> None:
        self.structured_output = structured_output
        self.is_error = is_error
        self.result = result
        self.subtype = subtype
        self.errors = errors or []
        self.total_cost_usd = 0.001
        self.usage = {"input_tokens": 10, "output_tokens": 5}


class FakeAssistant:
    def __init__(self, payload: dict[str, Any]) -> None:
        block = type("ToolUseBlock", (), {})()
        block.name, block.input = "StructuredOutput", payload
        self.content = [block]


class FakeQuery:
    """Injected ``query``: each script is a list of messages; an exception entry is raised from inside the
    stream, a float entry sleeps. The last script repeats once exhausted."""

    def __init__(self, *scripts: list[Any]) -> None:
        self.scripts = list(scripts)
        self.calls: list[tuple[str, Any]] = []
        self.closed = 0

    def __call__(self, *, prompt: str, options: Any) -> Any:
        self.calls.append((prompt, options))
        return self._stream(self.scripts[min(len(self.calls) - 1, len(self.scripts) - 1)])

    async def _stream(self, script: list[Any]) -> Any:
        try:
            for item in script:
                if isinstance(item, BaseException):
                    raise item
                if isinstance(item, float):
                    await asyncio.sleep(item)
                    continue
                yield item
        finally:
            self.closed += 1


def ok(payload: dict[str, Any] | None = None) -> list[Any]:
    payload = {"mood": 0.4} if payload is None else payload
    return [FakeAssistant(payload), FakeResult(payload)]


@pytest.fixture
def agents_cfg(tmp_path) -> dict[str, Any]:
    """The pinned agents.yaml with ``llm.cli_path`` at a stand-in file, so the pinned-CLI check passes."""
    cli = tmp_path / "claude.exe"
    cli.write_bytes(b"")
    cfg = copy.deepcopy(load_yaml(config_dir() / "agents.yaml"))
    cfg.setdefault("llm", {})["cli_path"] = str(cli)
    return cfg


@pytest.fixture(autouse=True)
def _no_api_key(monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)


def make(agents_cfg, fake, max_calls: int = 5):
    return mml.StudyLLM(max_calls=max_calls, query_fn=fake, options_cls=FakeOptions, agents_cfg=agents_cfg)


async def call(llm, model: str = HAIKU, prompt: str = "PROMPT"):
    return await llm.call(model=model, system_prompt="SYSTEM", prompt=prompt, json_schema=SCHEMA)


# --------------------------------------------------------------------------- pinned options, the cap, the stop
async def test_options_carry_pinned_model_and_isolation(agents_cfg):
    fake = FakeQuery(ok())
    llm = make(agents_cfg, fake)

    res = await call(llm, model=SONNET)

    prompt, options = fake.calls[0]
    assert options.model == SONNET
    assert options.fallback_model is None
    assert options.tools == []
    assert options.allowed_tools == []
    assert options.setting_sources == []
    assert options.env == ISOLATION_ENV
    assert options.strict_mcp_config is True
    assert options.cli_path == agents_cfg["llm"]["cli_path"]
    assert options.system_prompt == "SYSTEM"
    assert options.output_format == {"type": "json_schema", "schema": SCHEMA}
    assert prompt == "PROMPT"
    assert res.ok and res.output == {"mood": 0.4} and res.error is None and res.model == SONNET
    assert llm.results == [res]


def test_cli_path_is_the_resolved_pinned_cli(agents_cfg):
    llm = make(agents_cfg, FakeQuery(ok()))
    assert isinstance(llm.cli_path, str) and llm.cli_path == agents_cfg["llm"]["cli_path"]
    del agents_cfg["llm"]["cli_path"]
    assert make(agents_cfg, FakeQuery(ok())).cli_path == mml.BUNDLED_CLI


async def test_cap_raises_after_max_calls(agents_cfg):
    fake = FakeQuery(ok())
    llm = make(agents_cfg, fake, max_calls=2)

    first, second = await call(llm), await call(llm)
    with pytest.raises(mml.CallCapReached):
        await call(llm)

    assert first.ok and second.ok
    assert len(fake.calls) == 2
    assert llm.results == [first, second]
    assert llm.stopped is False


@pytest.mark.parametrize("script", [
    pytest.param([FakeResult(None, is_error=True, result=SESSION_LIMIT)], id="session-limit-error-result"),
    pytest.param([RuntimeError("Claude Code returned an error result: 429 rate_limit_error")], id="rate-limit-raise"),
    pytest.param([RuntimeError("Not logged in · Please run /login")], id="auth-raise"),
    pytest.param([FakeResult(None, is_error=True, result="Credit balance is too low")], id="credit-error-result"),
])
async def test_session_limit_stops_all_later_calls(agents_cfg, script):
    fake = FakeQuery(script, ok())
    llm = make(agents_cfg, fake)

    first = await call(llm)
    with pytest.raises(mml.LLMStopped):
        await call(llm)
    with pytest.raises(mml.LLMStopped):
        await call(llm)

    assert not first.ok and first.output is None and first.error
    assert llm.stopped is True
    assert len(fake.calls) == 1                       # no retry, no later query
    assert llm.results == [first]


async def test_result_error_text_is_failure_not_output(agents_cfg):
    fake = FakeQuery([FakeResult({"mood": 0.9}, is_error=True, result="model refused the schema")], ok())
    llm = make(agents_cfg, fake)

    bad = await call(llm)
    good = await call(llm)

    assert not bad.ok and bad.output is None
    assert "model refused the schema" in bad.error
    assert llm.stopped is False                       # an ordinary failure does not stop the night
    assert good.ok and llm.results == [bad, good]


# --------------------------------------------------------------------------- edges
async def test_session_limit_with_subtype_success_and_empty_text_reads_the_reason(agents_cfg):
    # The SDK raises "error result: success" AFTER the result message; the reason is read from the message
    # first and the stream is closed there, so the SDK's text-less exception never replaces it.
    fake = FakeQuery([FakeResult(None, is_error=True, result=SESSION_LIMIT),
                      RuntimeError("Claude Code returned an error result: success")])
    llm = make(agents_cfg, fake)

    res = await call(llm)

    assert "session limit" in res.error and llm.stopped is True
    assert fake.closed == 1


async def test_exception_after_a_partial_answer_is_a_failure_without_output(agents_cfg):
    fake = FakeQuery([FakeAssistant({"mood": 0.4}), ConnectionError("pipe closed")], ok())
    llm = make(agents_cfg, fake)

    res = await call(llm)

    assert not res.ok and res.output is None and "ConnectionError" in res.error
    assert llm.stopped is False and fake.closed == 1
    assert (await call(llm)).ok


@pytest.mark.parametrize("script", [
    pytest.param([FakeResult(None)], id="no-structured-output"),
    pytest.param([FakeResult([{"mood": 0.4}])], id="list-not-dict"),
    pytest.param([FakeResult('{"mood": 0.4}')], id="string-not-dict"),
    pytest.param([FakeAssistant({"mood": 0.4})], id="no-result-message"),
])
async def test_success_needs_a_dict_structured_output(agents_cfg, script):
    llm = make(agents_cfg, FakeQuery(script))

    res = await call(llm)

    assert not res.ok and res.output is None and res.error
    assert llm.stopped is False


async def test_timeout_is_a_failure_that_does_not_stop(agents_cfg):
    fake = FakeQuery([5.0, *ok()], ok())
    llm = make(agents_cfg, fake)
    llm.timeout_s = 0.05

    res = await call(llm)

    assert not res.ok and "timeout" in res.error and llm.stopped is False
    assert fake.closed == 1
    assert (await call(llm)).ok


async def test_timeout_comes_from_the_news_analyst_def(agents_cfg):
    llm = make(agents_cfg, FakeQuery(ok()))
    assert llm.timeout_s == float(agents_cfg["agents"]["news_analyst"]["timeout_s"])


async def test_stopped_outranks_the_cap(agents_cfg):
    llm = make(agents_cfg, FakeQuery([RuntimeError("usage limit reached")]), max_calls=1)

    await call(llm)

    with pytest.raises(mml.LLMStopped):
        await call(llm)


async def test_zero_cap_makes_no_call(agents_cfg):
    fake = FakeQuery(ok())
    llm = make(agents_cfg, fake, max_calls=0)

    with pytest.raises(mml.CallCapReached):
        await call(llm)
    assert fake.calls == [] and llm.results == []


@pytest.mark.parametrize("bad", [-1, True, 1.5])
def test_max_calls_must_be_a_non_negative_int(agents_cfg, bad):
    with pytest.raises(ValueError):
        mml.StudyLLM(max_calls=bad, query_fn=FakeQuery(ok()), options_cls=FakeOptions, agents_cfg=agents_cfg)


async def test_an_agents_yaml_model_name_is_sent_as_its_api_id(agents_cfg):
    fake = FakeQuery(ok())
    llm = make(agents_cfg, fake)

    res = await call(llm, model="haiku-5.5")

    assert fake.calls[0][1].model == HAIKU and res.model == HAIKU


@pytest.mark.parametrize("model", ["", "haiku", "claude-haiku-5", "Claude-Haiku-5-5"])
async def test_an_unknown_model_id_is_refused_before_any_call(agents_cfg, model):
    fake = FakeQuery(ok())
    llm = make(agents_cfg, fake, max_calls=1)

    with pytest.raises(ValueError):
        await call(llm, model=model)
    assert fake.calls == []
    assert (await call(llm)).ok                       # the refusal did not spend the cap


def test_a_stray_api_key_is_refused(agents_cfg, monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    with pytest.raises(RuntimeError, match="ANTHROPIC_API_KEY"):
        make(agents_cfg, FakeQuery(ok()))


async def test_a_missing_pinned_cli_refuses_without_spending_the_cap(agents_cfg, tmp_path):
    agents_cfg["llm"]["cli_path"] = str(tmp_path / "gone" / "claude.exe")
    fake = FakeQuery(ok())
    llm = make(agents_cfg, fake, max_calls=1)

    with pytest.raises(RuntimeError, match="pinned Claude Code CLI missing"):
        await call(llm)
    assert fake.calls == []


async def test_result_stamps_ist_start_and_duration(agents_cfg):
    llm = make(agents_cfg, FakeQuery(ok()))

    res = await call(llm)

    assert res.at.utcoffset() == timedelta(hours=5, minutes=30)
    assert res.duration_s >= 0


async def test_concurrent_calls_never_exceed_the_cap(agents_cfg):
    fake = FakeQuery([0.01, *ok()])
    llm = make(agents_cfg, fake, max_calls=3)

    out = await asyncio.gather(*(call(llm, prompt=f"P{i}") for i in range(5)), return_exceptions=True)

    assert sum(isinstance(o, mml.LLMResult) and o.ok for o in out) == 3
    assert sum(isinstance(o, mml.CallCapReached) for o in out) == 2
    assert len(fake.calls) == 3 and len(llm.results) == 3
