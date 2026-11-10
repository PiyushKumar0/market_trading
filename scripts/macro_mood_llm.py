"""Macro mood study (``edge/macro_mood``): pinned-model Claude calls for the nightly scorer, outside the engine.

Options come from the engine's own harness (``_build_options`` on the ``news_analyst`` def: no tools, no
settings, isolation env, strict MCP, pinned CLI) with only the model swapped, so a study call is isolated
exactly as a live analyst call is. Results go to the caller's ``results`` list and nowhere else: nothing here
touches the engine's stores or its budget governor.

Every call draws on the subscription quota the live engine uses, so the wrapper never retries, refuses past
``max_calls`` a night, and stops for good on the first quota or login failure (a session or usage limit, a 429,
exhausted credit, a dead login): every later call raises :class:`LLMStopped` without querying.

``call`` may be awaited concurrently: the stop and cap checks and the count all happen before its first
await, so concurrent calls never exceed ``max_calls``; a call already in flight when another one stops the
wrapper still runs to its own end.
"""

from __future__ import annotations

import asyncio
import dataclasses
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

_REPO_SRC = str(Path(__file__).resolve().parents[1] / "src")
if _REPO_SRC not in sys.path:  # pragma: no cover - loose-script shim
    sys.path.insert(0, _REPO_SRC)

from engine.core.clock import Clock  # noqa: E402
from engine.core.config import config_dir, load_yaml, repo_root  # noqa: E402
from engine.intelligence.harness import (  # noqa: E402
    MODEL_API_IDS,
    AgentHarness,
    _attr,
    _classify,
    _closing,
    _error_result_text,
    _is_result_message,
    load_agent_roster,
    resolve_cli_path,
)

IST = ZoneInfo("Asia/Kolkata")
#: The roster def whose isolation, allowlist, output cap and timeout every study call inherits.
BASE_AGENT = "news_analyst"
#: ``_classify`` reasons that mean the account cannot serve calls right now; retrying only burns more quota.
STOP_REASONS = frozenset({"overloaded", "credit_exhausted", "auth_failed"})
#: Model ids a call may name; anything else would reach the CLI unvetted.
KNOWN_MODELS = frozenset(MODEL_API_IDS.values())


class LLMHalt(RuntimeError):
    """A call refused without querying."""


class CallCapReached(LLMHalt):
    """The night's ``max_calls`` is spent."""


class LLMStopped(LLMHalt):
    """An earlier call hit a quota or login failure; no further call is made this run."""


@dataclass
class LLMResult:
    ok: bool
    output: dict | None
    error: str | None
    model: str
    at: datetime            # the call's start, IST
    duration_s: float


class StudyLLM:
    """``max_calls`` queries at most, no retries, a permanent stop on the first quota or login failure.

    ``options_cls`` and ``agents_cfg`` exist for tests: the defaults are the SDK's options class (imported
    lazily by the harness) and ``config/agents.yaml``.
    """

    def __init__(self, *, max_calls: int, query_fn: Any = None, options_cls: type | None = None,
                 agents_cfg: dict[str, Any] | None = None) -> None:
        if isinstance(max_calls, bool) or not isinstance(max_calls, int) or max_calls < 0:
            raise ValueError(f"max_calls must be a non-negative int, got {max_calls!r}")
        # A stray key outranks the CLI's OAuth login and bills metered API instead of the subscription.
        if os.environ.get("ANTHROPIC_API_KEY"):
            raise RuntimeError("ANTHROPIC_API_KEY is set: it would bill the metered API, not the subscription")
        cfg = load_yaml(config_dir() / "agents.yaml") if agents_cfg is None else agents_cfg
        defs = load_agent_roster(cfg).defs
        if BASE_AGENT not in defs:
            raise RuntimeError(f"agents.yaml has no runnable {BASE_AGENT} def to take call options from")
        self._def = defs[BASE_AGENT]
        self._harness = AgentHarness(defs, None, Clock(), None, cli_path=resolve_cli_path(cfg, repo_root()),
                                     options_cls=options_cls)   # option building needs no governor or db
        self._query_fn = query_fn
        self.max_calls = max_calls
        self.timeout_s = self._def.timeout_s
        self.calls_made = 0
        self.stopped = False
        self.stop_reason: str | None = None
        self.results: list[LLMResult] = []

    async def call(self, *, model: str, system_prompt: str, prompt: str, json_schema: dict) -> LLMResult:
        api_model = MODEL_API_IDS.get(model, model)
        if api_model not in KNOWN_MODELS:
            raise ValueError(f"unknown model {model!r}; known ids: {sorted(KNOWN_MODELS)}")
        if self.stopped:
            raise LLMStopped(self.stop_reason or "stopped")
        if self.calls_made >= self.max_calls:
            raise CallCapReached(f"{self.max_calls} calls made")
        # A missing pinned CLI or an options surface without an isolation knob raises here, before the
        # count: a setup fault, not a call.
        options, prefix = self._harness._build_options(self._def, system_prompt=system_prompt,
                                                       json_schema=json_schema, max_turns=1)
        options = dataclasses.replace(options, model=api_model)
        self.calls_made += 1

        at = datetime.now(IST)
        started = time.monotonic()
        output: dict | None = None
        error: str | None = None
        try:
            output, error = await asyncio.wait_for(self._consume(prefix + prompt, options), self.timeout_s)
        except TimeoutError:
            error = f"timeout: no complete response within {self.timeout_s}s"
        except Exception as exc:  # noqa: BLE001 - every SDK failure is one failed call, classified below
            reason, detail = _classify(exc)
            error = f"{reason}: {detail}"
            if reason in STOP_REASONS and not self.stopped:
                self.stopped, self.stop_reason = True, error
        result = LLMResult(ok=error is None, output=output if error is None else None, error=error,
                           model=api_model, at=at, duration_s=time.monotonic() - started)
        self.results.append(result)
        return result

    async def _consume(self, prompt: str, options: Any) -> tuple[dict | None, str | None]:
        """``(output, None)`` on a dict structured output, ``(None, why)`` otherwise; an error result raises."""
        query = self._query_fn
        if query is None:
            import claude_agent_sdk  # noqa: PLC0415 - heavy; only the real query path needs it

            query = claude_agent_sdk.query
        result: Any = None
        async with _closing(query(prompt=prompt, options=options)) as stream:
            async for message in stream:
                # Read before the SDK's own exception replaces the reason with "error result: success".
                text = _error_result_text(message)
                if text is not None:
                    raise RuntimeError(f"Claude Code returned an error result: {text}")
                if _is_result_message(message):
                    result = message
        if result is None:
            return None, "no result message"
        structured = _attr(result, "structured_output")
        if not isinstance(structured, dict):
            return None, f"no structured output (got {type(structured).__name__})"
        return structured, None
