"""AgentHarness (§3.2.6, §5.1) — the ONLY module in the platform that calls the Claude Agent SDK.

Everything the plan pins about *how* a Tier-1 call is made lives here, in one place, so the guarantees
are auditable in code rather than prose:

- **Shape (D5).** Intraday agents are single-shot: one fresh ``query()`` per invocation, context fully
  pre-assembled by ``ContextAssembler`` (the model never fetches its own data), one response. Agentic
  multi-turn is permitted only for the nightly/weekly agents, under a hard ``max_turns`` AND a
  cumulative billed-token run budget enforced here mid-run (§5.5).
- **Tool allowlisting is explicit and mandatory (D5/D10).** Every call passes an explicit allowed-tools
  list — empty for single-shot agents, the §5.5 read-only set for the agentic ones — and explicitly
  disables the SDK's built-ins. ``tools_enabled: true`` NEVER means "SDK defaults": a definition with
  tools enabled and no explicit allowlist is refused at load time (:func:`load_agent_defs`), and an
  options surface that exposes no tool knob at all is refused at call time. §9.1 tests both.
- **``setting_sources=[]`` always (D10).** The service runs from a different working directory and must
  not pick up a stray CLAUDE.md/settings. If the options surface cannot express that, we refuse to call
  rather than call unconstrained.
- **Model is always explicit (D4).** ``config/agents.yaml`` names the model; :data:`MODEL_API_IDS` maps
  that name to the API id. An unknown name is a loud load-time error — never a silent SDK default
  (which is Opus on Max plans, 5× Haiku rates).
- **Failure semantics (D7).** Schema-invalid retries at most twice, each a FRESH query with the
  validation error appended; timeout / 429 / credit / SDK death do not retry. Everything resolves to
  ``AgentResult.Failed`` + an owner alert. Model prose is NEVER salvaged by regex or fence-stripping —
  a non-JSON response is a schema violation, full stop.
- **Structured output (2026-07-28).** When a ``json_schema`` is sent, the CLI fulfils it by forcing a
  ``StructuredOutput`` TOOL call whose *input* is the validated payload; that round-trip counts as a
  second turn, so the turn cap gets +1 exactly when the schema knob went out. The tool input is the
  authoritative answer (it can never be fenced or wrapped in prose), and client-side validation
  remains authoritative on top of it (§8.1). Extended thinking is pinned off (``max_thinking_tokens=0``)
  wherever the knob exists — it bills at output rates and measured +15-50s latency per call.
- **No LLM-originated time (§5.1).** The harness stamps nothing from model output. Temporal stamping
  belongs to the ``validate`` callback the caller supplies (e.g. ``schemas.parse_and_stamp``), which
  overwrites ``valid_until``/ids from ``Clock``.
- **Auditability (R8).** Every SDK call — including each retry, whose prompt differs — persists its own
  ``agent_calls`` row: inputs digest, gzipped system+prompt snapshot, raw output, usage, priced cost,
  duration. A governor-blocked invocation persists a row too (with no SDK call made), so "we did not
  call" is as visible in the audit trail as "we called and it failed".

Billed usage flows into :class:`~engine.intelligence.governor.BudgetGovernor` only when a call actually
reported some — a timed-out call books nothing, which is exactly the systematic under-count the
governor's docstring calls out and the reason the degrade ladder trips well below the credit.
"""

from __future__ import annotations

import asyncio
import gzip
import json
import re
import sqlite3
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from decimal import Decimal
from typing import Any, Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from ulid import ULID

from engine.core.clock import Clock
from engine.core.db import transaction
from engine.core.log import get_logger
from engine.intelligence.governor import BudgetGovernor, TokenUsage
from engine.notify.episodes import AlertEpisodes

_log = get_logger("engine.intelligence.harness")

# agents.yaml model NAME -> API model id (D4). The yaml name is also the governor's pricing key, so the
# two never drift apart; an unmapped name fails loud rather than reaching the SDK as a default.
MODEL_API_IDS: dict[str, str] = {
    "haiku-4.5": "claude-haiku-4-5",
    "sonnet-4.6": "claude-sonnet-4-6",
    "opus-4.8": "claude-opus-4-8",
    # Claude 5 family (owner roster migration 2026-08-03 — the unmapped 'sonnet-5' darkened the
    # whole LLM tier from the 12:33 boot until this entry landed; see load_agent_roster for the
    # one-bad-def quarantine that keeps a repeat from taking every agent down).
    "sonnet-5": "claude-sonnet-5",
    "opus-5": "claude-opus-5",
    "fable-5": "claude-fable-5",
}

# The SDK's built-in tools, explicitly disabled on every call (§5.1) — "no filesystem, no network, no
# order surface" is enforced in the options, not assumed from an empty allowlist.
BUILTIN_TOOLS: tuple[str, ...] = (
    "Bash",
    "BashOutput",
    "Edit",
    "Glob",
    "Grep",
    "KillShell",
    "NotebookEdit",
    "Read",
    "Task",
    "TodoWrite",
    "WebFetch",
    "WebSearch",
    "Write",
)

MAX_SCHEMA_RETRIES = 2      # D7: schema-invalid retries at most twice => at most 3 SDK calls per run
DEFAULT_TIMEOUT_S = 45.0    # D7 single-shot default; the pre-open planner overrides to 60 in agents.yaml

RETRY_TAIL = (
    "Previous output failed validation: {err}. Emit ONLY valid JSON matching the schema."
)

FailReason = Literal[
    "schema_invalid",
    "timeout",
    "sdk_error",
    "overloaded",
    "governor_blocked",
    "credit_exhausted",
]

AlertCallback = Callable[[str, str], Awaitable[None]]   # (severity, message) — ops idiom
QueryFn = Callable[..., AsyncIterator[Any]]

_ZERO_USAGE = TokenUsage(in_tokens=0, out_tokens=0)


# --------------------------------------------------------------------------- agent definitions
class RunCaps(BaseModel):
    """Hard caps for one agentic RUN (D5/§5.5) — also the shape of ``run_budget_tokens`` in agents.yaml.

    ``billed_*_max`` are cumulative over the whole run (all turns, all retries), which is the only
    reading that makes them a real stop: a per-turn cap would let 12 turns spend 12×.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    billed_in_max: int | None = Field(default=None, gt=0)
    billed_out_max: int | None = Field(default=None, gt=0)
    max_turns: int | None = Field(default=None, gt=0)
    timeout_s: float | None = Field(default=None, gt=0)


class AgentDef(BaseModel):
    """One agents.yaml agent, validated into the shape the harness will actually call with.

    ``allowed_tools`` has NO default on purpose (D5/D10): the allowlist must be an explicit decision at
    every construction site, not something a forgotten yaml key silently supplies.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    agent_id: str
    model: str                      # agents.yaml NAME (the governor's pricing key), not the API id
    shape: Literal["single_shot", "agentic"]
    tools_enabled: bool
    allowed_tools: list[str]        # MANDATORY (D5/D10)
    max_output_tokens: int | None = Field(default=None, gt=0)
    timeout_s: float = Field(default=DEFAULT_TIMEOUT_S, gt=0)
    max_turns: int | None = Field(default=None, gt=0)
    run_budget_tokens: RunCaps | None = None

    @field_validator("model")
    @classmethod
    def _known_model(cls, v: str) -> str:
        if v not in MODEL_API_IDS:
            raise ValueError(
                f"unknown model name {v!r} in agents.yaml; known: {sorted(MODEL_API_IDS)} "
                "(an unmapped model would reach the SDK as its DEFAULT — Opus on Max plans, D4)"
            )
        return v

    @model_validator(mode="after")
    def _check_shape(self) -> AgentDef:
        if self.shape == "single_shot":
            if self.allowed_tools:
                raise ValueError(
                    f"{self.agent_id}: single-shot agents run with NO tools (D5); "
                    f"allowed_tools must be empty, got {self.allowed_tools}"
                )
        else:
            if self.max_turns is None:
                raise ValueError(f"{self.agent_id}: agentic agents require an explicit max_turns (D5)")
            if self.run_budget_tokens is None:
                raise ValueError(
                    f"{self.agent_id}: agentic agents require run_budget_tokens (§5.5 cumulative cap)"
                )
        if self.tools_enabled and not self.allowed_tools:
            raise ValueError(
                f"{self.agent_id}: tools_enabled with an empty allowed_tools list is meaningless — "
                "'tools_enabled: true' never means SDK defaults (D5/D10)"
            )
        return self

    @property
    def api_model(self) -> str:
        return MODEL_API_IDS[self.model]


def load_agent_defs(cfg: dict[str, Any]) -> dict[str, AgentDef]:
    """Build :class:`AgentDef` objects from a loaded ``agents.yaml`` mapping.

    Refuses (raises ``ValueError``) any definition with ``tools_enabled: true`` and no explicit
    ``allowed_tools`` list — the §9.1 allowlist guarantee (D5/D10). Refusing at LOAD time means an
    unallowlisted agent cannot exist in the roster at all, rather than failing at 21:00 on the night it
    is first scheduled.
    """
    out: dict[str, AgentDef] = {}
    for agent_id, raw in (cfg.get("agents") or {}).items():
        if not isinstance(raw, dict):
            raise ValueError(f"agents.{agent_id}: expected a mapping, got {type(raw).__name__}")
        if not raw.get("enabled", True):
            # A def the roster deliberately parks (weekly_researcher until Phase 5, §8.7). Skipping at
            # load keeps the allowlist refusal strict for everything that CAN run.
            continue
        tools_enabled = bool(raw.get("tools_enabled", False))
        if "allowed_tools" in raw:
            allowed = raw["allowed_tools"]
            if not isinstance(allowed, list):
                raise ValueError(f"agents.{agent_id}.allowed_tools must be a list, got {allowed!r}")
            allowed_tools = [str(t) for t in allowed]
        elif tools_enabled:
            raise ValueError(
                f"agents.{agent_id}: tools_enabled: true with NO allowed_tools list — the harness "
                "refuses to run an agent definition lacking an explicit allowlist (D5/D10, §5.1)"
            )
        else:
            allowed_tools = []
        budget = raw.get("run_budget_tokens")
        out[agent_id] = AgentDef(
            agent_id=agent_id,
            model=raw["model"],
            shape=raw["shape"],
            tools_enabled=tools_enabled,
            allowed_tools=allowed_tools,
            max_output_tokens=raw.get("max_output_tokens"),
            timeout_s=float(raw.get("timeout_s", DEFAULT_TIMEOUT_S)),
            max_turns=raw.get("max_turns"),
            run_budget_tokens=RunCaps(**budget) if budget else None,
        )
    return out


class RosterLoad(BaseModel):
    """Outcome of :func:`load_agent_roster`: the runnable defs plus every QUARANTINED def with its
    refusal reason. A quarantined agent cannot run (same guarantee as the strict loader) — but it no
    longer takes the other agents down with it."""

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    defs: dict[str, AgentDef]
    quarantined: dict[str, str]


def load_agent_roster(cfg: dict[str, Any]) -> RosterLoad:
    """Per-def tolerant roster load for the COMPOSITION ROOT (2026-08-03 incident: one unmapped
    model name — ``sonnet-5`` before the 5-family entries existed — made :func:`load_agent_defs`
    raise and DISABLED the entire LLM tier for two boots, with every LLM job then no-op'ing behind
    success watermarks).

    Each definition validates independently: an invalid one is EXCLUDED and logged loud (the
    §9.1/D5 guarantee is preserved — a def that fails validation is not runnable), while the valid
    rest of the roster stays live (D7: failures degrade to less capability, never more). The strict
    :func:`load_agent_defs` remains for tests/tooling that want refusal semantics.
    """
    defs: dict[str, AgentDef] = {}
    quarantined: dict[str, str] = {}
    for agent_id, raw in (cfg.get("agents") or {}).items():
        try:
            loaded = load_agent_defs({"agents": {agent_id: raw}})
        except Exception as exc:  # noqa: BLE001 - each def is judged alone; the reason is preserved
            quarantined[agent_id] = str(exc)
            _log.error("agent_def_quarantined", agent=agent_id, error=str(exc)[:300])
            continue
        defs.update(loaded)
    if quarantined:
        _log.error(
            "agent_roster_partial", loaded=sorted(defs), quarantined=sorted(quarantined),
        )
    return RosterLoad(defs=defs, quarantined=quarantined)


# --------------------------------------------------------------------------- results
class AgentResult(BaseModel):
    """``Ok(payload, usage, call_id)`` | ``Failed(reason, detail, call_id)`` as one frozen model.

    ``call_id`` is the LAST attempt's ``agent_calls`` row (each retry has its own row, R8).
    """

    model_config = ConfigDict(frozen=True)

    ok: bool
    call_id: str
    payload: Any = None
    usage: TokenUsage | None = None
    reason: FailReason | None = None
    detail: str | None = None
    raw_text: str | None = None
    attempts: int = 0

    @classmethod
    def Ok(  # noqa: N802 - mirrors the plan's ADT notation (§3.2.6)
        cls,
        *,
        call_id: str,
        payload: Any,
        usage: TokenUsage | None = None,
        raw_text: str | None = None,
        attempts: int = 1,
    ) -> AgentResult:
        return cls(ok=True, call_id=call_id, payload=payload, usage=usage, raw_text=raw_text, attempts=attempts)

    @classmethod
    def Failed(  # noqa: N802
        cls,
        reason: FailReason,
        detail: str = "",
        *,
        call_id: str,
        usage: TokenUsage | None = None,
        raw_text: str | None = None,
        attempts: int = 0,
    ) -> AgentResult:
        return cls(
            ok=False,
            call_id=call_id,
            reason=reason,
            detail=detail,
            usage=usage,
            raw_text=raw_text,
            attempts=attempts,
        )


class AssembledContextLike(Protocol):
    """What the harness needs from an assembled context (duck-typed).

    ``ContextAssembler`` (§3.2.6) lands separately; the harness deliberately does NOT import it, so the
    two can be built and tested independently. ``call_class`` is read defensively with a ``schedule``
    default — it is the governor's admission class, not part of the prompt.
    """

    system_prompt: str
    inputs_digest: str

    def prompt(self) -> str: ...


# --------------------------------------------------------------------------- SDK-surface helpers
_OPTION_FIELDS: dict[type, set[str]] = {}


def _option_fields(cls: type) -> set[str]:
    """Field names the options class exposes (annotations + dataclass/pydantic fields + a probe).

    Same introspection the Phase-0 smoke test uses (§8.1): the options class has been renamed and
    re-shaped across SDK releases, so every knob is probed rather than assumed.
    """
    cached = _OPTION_FIELDS.get(cls)
    if cached is not None:
        return cached
    names: set[str] = set()
    for base in getattr(cls, "__mro__", (cls,)):
        names.update(getattr(base, "__annotations__", {}) or {})
    names.update(getattr(cls, "__dataclass_fields__", {}) or {})
    names.update(getattr(cls, "model_fields", {}) or {})
    try:
        probe = cls()
    except Exception:  # noqa: BLE001 - some versions require args; annotations already cover us
        probe = None
    if probe is not None:
        names.update(n for n in vars(probe) if not n.startswith("_"))
    _OPTION_FIELDS[cls] = names
    return names


def _first_field(cls: type, candidates: tuple[str, ...]) -> str | None:
    fields = _option_fields(cls)
    return next((name for name in candidates if name in fields), None)


def _attr(obj: Any, name: str) -> Any:
    if isinstance(obj, dict):
        return obj.get(name)
    return getattr(obj, name, None)


def _int_field(raw: Any, *names: str) -> int:
    for name in names:
        value = _attr(raw, name)
        if value is None:
            continue
        try:
            return max(0, int(value))
        except (TypeError, ValueError):
            continue
    return 0


def _message_text(message: Any) -> str:
    """Assistant text of one SDK message, defensively across message shapes."""
    content = _attr(message, "content")
    if isinstance(content, str):
        return content
    if isinstance(content, (list, tuple)):
        parts = [t for block in content if isinstance(t := _attr(block, "text"), str)]
        if parts:
            return "".join(parts)
    for name in ("text", "result"):
        value = _attr(message, name)
        if isinstance(value, str) and value:
            return value
    return ""


_FENCE_RE = re.compile(r"^```[a-zA-Z0-9_-]*\s*\n(.*)\n?```$", re.DOTALL)


def _unwrap_lone_fence(text: str) -> str:
    """The whole-string fence unwrap ``_validate`` documents: ```` ```json\\n{...}\\n``` ```` → ``{...}``.

    Anything around the fence — prose before, commentary after, a second block — fails the match
    and the text passes through untouched (and then fails JSON parsing as it should, D7).
    """
    match = _FENCE_RE.match(text)
    return match.group(1).strip() if match else text


def _structured_output(message: Any) -> str | None:
    """JSON of a ``StructuredOutput`` tool call carried by this message, if any.

    The CLI fulfils an output schema by forcing a ``StructuredOutput`` TOOL call whose *input* is the
    schema-validated payload — that input is the authoritative answer, and any assistant text around
    it is commentary. ``_consume`` therefore always prefers this over trailing text.
    """
    content = _attr(message, "content")
    if not isinstance(content, (list, tuple)):
        return None
    payload: str | None = None
    for block in content:
        if _attr(block, "name") == "StructuredOutput":
            data = _attr(block, "input")
            if isinstance(data, dict):
                payload = json.dumps(data)
    return payload


def _is_result_message(message: Any) -> bool:
    return _attr(message, "total_cost_usd") is not None or "Result" in type(message).__name__


def _extract_usage(message: Any) -> TokenUsage | None:
    raw = _attr(message, "usage")
    if raw is None:
        return None
    return TokenUsage(
        in_tokens=_int_field(raw, "in_tokens", "input_tokens"),
        out_tokens=_int_field(raw, "out_tokens", "output_tokens"),
        cache_read=_int_field(raw, "cache_read", "cache_read_input_tokens"),
        cache_write=_int_field(raw, "cache_write", "cache_creation_input_tokens"),
    )


def _add_usage(a: TokenUsage, b: TokenUsage) -> TokenUsage:
    return TokenUsage(
        in_tokens=a.in_tokens + b.in_tokens,
        out_tokens=a.out_tokens + b.out_tokens,
        cache_read=a.cache_read + b.cache_read,
        cache_write=a.cache_write + b.cache_write,
    )


def _max_usage(a: TokenUsage, b: TokenUsage) -> TokenUsage:
    return TokenUsage(
        in_tokens=max(a.in_tokens, b.in_tokens),
        out_tokens=max(a.out_tokens, b.out_tokens),
        cache_read=max(a.cache_read, b.cache_read),
        cache_write=max(a.cache_write, b.cache_write),
    )


class _UsageAccumulator:
    """Billed usage of one SDK call, assembled from whatever the stream reports.

    A result message's usage is CUMULATIVE for the call, while per-turn messages report their own slice.
    Taking the field-wise max of (sum of turn messages, result message) is correct for both shapes and
    never double-counts a turn that the result message already includes.
    """

    __slots__ = ("_turns", "_result", "sdk_cost_usd")

    def __init__(self) -> None:
        self._turns = _ZERO_USAGE
        self._result: TokenUsage | None = None
        self.sdk_cost_usd: Decimal | None = None

    def add(self, message: Any) -> None:
        usage = _extract_usage(message)
        if _is_result_message(message):
            if usage is not None:
                self._result = usage
            cost = _attr(message, "total_cost_usd")
            if cost is not None:
                try:
                    self.sdk_cost_usd = Decimal(str(cost))
                except Exception:  # noqa: BLE001 - a malformed SDK cost must not kill the call
                    self.sdk_cost_usd = None
        elif usage is not None:
            self._turns = _add_usage(self._turns, usage)

    def total(self) -> TokenUsage:
        if self._result is None:
            return self._turns
        return _max_usage(self._turns, self._result)


class _RunBudget:
    """Cumulative billed-token cap for one agentic run (§5.5), carried across retries within the run.

    Cache reads/writes are billed input, so they count against ``billed_in_max`` — a cap that ignored
    them would be a cap on the cheapest part of the bill.
    """

    __slots__ = ("in_max", "out_max", "carried")

    def __init__(self, in_max: int | None, out_max: int | None) -> None:
        self.in_max = in_max
        self.out_max = out_max
        self.carried = _ZERO_USAGE

    def charge(self, usage: TokenUsage) -> None:
        self.carried = _add_usage(self.carried, usage)

    def exceeded(self, current: TokenUsage) -> bool:
        billed_in = (
            self.carried.in_tokens
            + self.carried.cache_read
            + self.carried.cache_write
            + current.in_tokens
            + current.cache_read
            + current.cache_write
        )
        billed_out = self.carried.out_tokens + current.out_tokens
        return (self.in_max is not None and billed_in > self.in_max) or (
            self.out_max is not None and billed_out > self.out_max
        )

    def describe(self, current: TokenUsage) -> str:
        billed_in = (
            self.carried.in_tokens + self.carried.cache_read + self.carried.cache_write
            + current.in_tokens + current.cache_read + current.cache_write
        )
        billed_out = self.carried.out_tokens + current.out_tokens
        return f"run budget exceeded: billed_in={billed_in}/{self.in_max} billed_out={billed_out}/{self.out_max}"


@asynccontextmanager
async def _closing(stream: Any) -> Any:
    """Close the SDK's async iterator on every exit path (timeout, budget abort, error)."""
    try:
        yield stream
    finally:
        aclose = getattr(stream, "aclose", None)
        if aclose is not None:
            try:
                await aclose()
            except Exception:  # noqa: BLE001 - best-effort cleanup; never masks the real outcome
                pass


def _classify(exc: BaseException) -> tuple[FailReason, str]:
    """Map an SDK exception to a D7 failure reason (all of which resolve to no-action + alert)."""
    detail = f"{type(exc).__name__}: {exc}"
    lowered = detail.lower()
    if "429" in lowered or "overload" in lowered or "rate limit" in lowered:
        return "overloaded", detail
    if "credit" in lowered or "billing" in lowered or "insufficient funds" in lowered:
        return "credit_exhausted", detail
    return "sdk_error", detail


# --------------------------------------------------------------------------- the harness
class AgentHarness:
    """Invokes Tier-1 agents. Nothing else in the platform may import the Claude Agent SDK."""

    def __init__(
        self,
        defs: dict[str, AgentDef],
        governor: BudgetGovernor,
        clock: Clock,
        conn: sqlite3.Connection,
        *,
        query_fn: QueryFn | None = None,
        alert: AlertCallback | None = None,
        options_cls: type | None = None,
    ) -> None:
        self.defs = dict(defs)
        self._governor = governor
        self._clock = clock
        self._conn = conn
        # Both SDK entry points are injectable and lazily imported otherwise: claude_agent_sdk spawns a
        # CLI subprocess on import-adjacent paths and must never be pulled into the unit-test tier.
        self._query_fn = query_fn
        self._options_cls = options_cls
        self._alert = alert
        #: Owner-alert cadence per ``(agent_id, reason)`` (WO-25b): the FIRST failure of a kind alerts,
        #: repeats inside the window are logged only, and a successful call closes the episode so the
        #: next failure is heard again. The D7 behaviour is untouched — every failure still resolves to
        #: no-action and is still logged; what changes is how many times the owner's phone buzzes for
        #: one broken thing (an SDK outage used to alert on every call for hours).
        self._alert_episodes = AlertEpisodes()

    # ------------------------------------------------------------------ public API
    async def run_single_shot(
        self,
        agent_def: AgentDef,
        context: AssembledContextLike,
        validate: Callable[[str], Any],
        *,
        json_schema: dict[str, Any] | None = None,
        call_class: str | None = None,
    ) -> AgentResult:
        """One fresh ``query()`` (D5/D10), validated client-side, metered and persisted.

        ``validate`` is the caller's schema contract — e.g. ``schemas.parse_and_stamp`` for an
        ``ActionProposal``, or a DayPlan validator. It also owns every platform stamp: the harness
        never derives a time or an id from model output (§5.1).
        """
        if agent_def.shape != "single_shot":
            raise ValueError(f"{agent_def.agent_id}: run_single_shot called on a {agent_def.shape} def (D5)")
        trigger = call_class or getattr(context, "call_class", None) or "schedule"
        return await self._run(
            agent_def,
            trigger=trigger,
            system_prompt=getattr(context, "system_prompt", "") or "",
            base_prompt=context.prompt(),
            inputs_digest=getattr(context, "inputs_digest", "") or "",
            validate=validate,
            json_schema=json_schema,
            max_turns=1,
            timeout_s=agent_def.timeout_s,
            budget=None,
        )

    async def run_agentic(
        self,
        agent_def: AgentDef,
        task: str,
        caps: RunCaps | None = None,
        *,
        validate: Callable[[str], Any] | None = None,
        system_prompt: str = "",
        inputs_digest: str = "",
        call_class: str = "schedule",
        json_schema: dict[str, Any] | None = None,
    ) -> AgentResult:
        """Multi-turn run for the nightly/weekly agents only (D5), under ``max_turns`` + a cumulative
        billed-token run budget that is checked mid-stream and carried across retries."""
        if agent_def.shape != "agentic":
            raise ValueError(f"{agent_def.agent_id}: run_agentic called on a {agent_def.shape} def (D5)")
        effective = self._effective_caps(agent_def, caps)
        return await self._run(
            agent_def,
            trigger=call_class,
            system_prompt=system_prompt,
            base_prompt=task,
            inputs_digest=inputs_digest,
            validate=validate,
            json_schema=json_schema,
            max_turns=effective.max_turns,
            timeout_s=effective.timeout_s or agent_def.timeout_s,
            budget=_RunBudget(effective.billed_in_max, effective.billed_out_max),
        )

    def _effective_caps(self, agent_def: AgentDef, caps: RunCaps | None) -> RunCaps:
        base = agent_def.run_budget_tokens or RunCaps()
        max_turns = (caps.max_turns if caps else None) or base.max_turns or agent_def.max_turns
        # No wall-clock timeout is declared for agentic agents in agents.yaml (the token budget is the
        # real lever), so derive one: the single-shot timeout per turn. An unbounded await would wedge
        # the scheduler on an SDK subprocess that never returns.
        timeout_s = (caps.timeout_s if caps else None) or base.timeout_s
        if timeout_s is None:
            timeout_s = agent_def.timeout_s * float(max_turns or 1)
        return RunCaps(
            billed_in_max=(caps.billed_in_max if caps else None) or base.billed_in_max,
            billed_out_max=(caps.billed_out_max if caps else None) or base.billed_out_max,
            max_turns=max_turns,
            timeout_s=timeout_s,
        )

    # ------------------------------------------------------------------ core loop
    async def _run(
        self,
        agent_def: AgentDef,
        *,
        trigger: str,
        system_prompt: str,
        base_prompt: str,
        inputs_digest: str,
        validate: Callable[[str], Any] | None,
        json_schema: dict[str, Any] | None,
        max_turns: int | None,
        timeout_s: float,
        budget: _RunBudget | None,
    ) -> AgentResult:
        decision = self._governor.can_invoke(agent_def.agent_id, trigger)
        if not decision.allowed:
            # NO SDK call — but the audit trail still records that the agent was asked and refused (R8).
            call_id = self._persist(
                agent_def,
                trigger=trigger,
                inputs_digest=inputs_digest,
                system_prompt=system_prompt,
                prompt=base_prompt,
                output=decision.reason or "blocked",
                ok=False,
                fail_reason="governor_blocked",
                usage=None,
                cost=Decimal(0),
                duration_ms=0,
            )
            return await self._fail(
                agent_def, "governor_blocked", decision.reason or "blocked", call_id=call_id, attempts=0
            )

        try:
            options, prefix = self._build_options(
                agent_def, system_prompt=system_prompt, json_schema=json_schema, max_turns=max_turns
            )
        except RuntimeError as exc:
            # SDK-surface refusal (missing tool/setting knob, D5/D10): an ordinary Failed so every
            # trigger degrades to no-proposal + alert (D7) instead of leaning on bus isolation.
            call_id = self._persist(
                agent_def,
                trigger=trigger,
                inputs_digest=inputs_digest,
                system_prompt=system_prompt,
                prompt=base_prompt,
                output=str(exc),
                ok=False,
                fail_reason="sdk_error",
                usage=None,
                cost=Decimal(0),
                duration_ms=0,
            )
            return await self._fail(agent_def, "sdk_error", str(exc), call_id=call_id, attempts=0)

        prompt = base_prompt
        last_error = ""
        last_call_id = ""
        last_usage: TokenUsage | None = None
        for attempt in range(1, MAX_SCHEMA_RETRIES + 2):
            accumulator = _UsageAccumulator()   # survives cancellation: partial usage is still billed
            started = time.perf_counter()
            aborted = False
            text = ""
            failure: tuple[FailReason, str] | None = None
            try:
                text, aborted = await asyncio.wait_for(
                    self._consume(prefix + prompt, options, accumulator, budget), timeout_s
                )
            except TimeoutError:
                failure = ("timeout", f"no complete response within {timeout_s}s")
            except (asyncio.CancelledError, KeyboardInterrupt):
                raise
            except Exception as exc:  # noqa: BLE001 - every SDK failure maps to a D7 reason
                failure = _classify(exc)

            usage = accumulator.total()
            last_usage = usage
            duration_ms = int((time.perf_counter() - started) * 1000)
            if budget is not None:
                budget.charge(usage)
            if aborted:
                failure = ("sdk_error", budget.describe(_ZERO_USAGE) if budget else "run budget exceeded")

            payload: Any = None
            if failure is None:
                ok, payload, last_error = self._validate(text, validate)
                if not ok:
                    failure = ("schema_invalid", last_error)

            call_id = self._persist(
                agent_def,
                trigger=trigger,
                inputs_digest=inputs_digest,
                system_prompt=system_prompt,
                prompt=prompt,
                output=text if text else (failure[1] if failure else ""),
                ok=failure is None,
                fail_reason=failure[0] if failure else None,
                usage=usage,
                cost=self._price(agent_def, usage),
                duration_ms=duration_ms,
            )
            last_call_id = call_id
            await self._meter(agent_def, usage, accumulator.sdk_cost_usd)

            if failure is None:
                _log.info(
                    "agent_call_ok",
                    agent=agent_def.agent_id,
                    call_id=call_id,
                    attempt=attempt,
                    in_tokens=usage.in_tokens,
                    out_tokens=usage.out_tokens,
                )
                # A working agent closes every open alert episode it owns (WO-25b): the next failure,
                # whatever its reason, is a NEW incident and must reach the owner immediately.
                self._alert_episodes.reset(agent_def.agent_id)
                return AgentResult.Ok(
                    call_id=call_id, payload=payload, usage=usage, raw_text=text, attempts=attempt
                )

            reason, detail = failure
            _log.warning(
                "agent_call_failed",
                agent=agent_def.agent_id,
                call_id=call_id,
                attempt=attempt,
                reason=reason,
                detail=detail[:300],
            )
            if reason == "credit_exhausted":
                # The ledger only ever sees calls that SUCCEEDED, so it can read healthy at the exact
                # moment the credit ran out — the billing error is the only signal (§5.6 DG4).
                self._governor.note_billing_error(detail[:200])
            if reason != "schema_invalid":
                return await self._fail(
                    agent_def, reason, detail, call_id=call_id, usage=usage, raw_text=text, attempts=attempt
                )
            # D7: a FRESH query carrying the validation error — never a second pass at parsing prose.
            prompt = f"{base_prompt}\n\n{RETRY_TAIL.format(err=last_error)}"

        return await self._fail(
            agent_def,
            "schema_invalid",
            last_error,
            call_id=last_call_id,
            usage=last_usage,
            attempts=MAX_SCHEMA_RETRIES + 1,
        )

    def _validate(
        self, text: str, validate: Callable[[str], Any] | None
    ) -> tuple[bool, Any, str]:
        """Parse the final assistant text as JSON and hand it to the caller's validator.

        No prose salvage (D7): a response that MIXES prose with JSON is a schema violation and
        earns a retry, not a rescue. ONE deterministic unwrap is permitted (2026-07-29): a response
        that is EXACTLY a single ```-fenced block and nothing else is that JSON in the CLI's
        habitual framing, not prose — observed live when the structured-output knob silently
        disengages (union schemas) and the model answers in a bare fenced block. The unwrap is a
        whole-string match; any surrounding text still fails.
        """
        stripped = text.strip()
        if not stripped:
            return False, None, "empty response (no assistant text)"
        stripped = _unwrap_lone_fence(stripped)
        try:
            json.loads(stripped)
        except ValueError as exc:
            return False, None, f"not valid JSON: {exc}"
        if validate is None:
            return True, json.loads(stripped), ""
        try:
            return True, validate(stripped), ""
        except (asyncio.CancelledError, KeyboardInterrupt):
            raise
        except Exception as exc:  # noqa: BLE001 - any validator rejection is a schema failure
            return False, None, f"{type(exc).__name__}: {exc}"[:500]

    async def _consume(
        self,
        prompt: str,
        options: Any,
        accumulator: _UsageAccumulator,
        budget: _RunBudget | None,
    ) -> tuple[str, bool]:
        """Drive the SDK stream to completion; return ``(final assistant text, aborted_on_budget)``."""
        query = self._query()
        texts: list[str] = []
        structured: list[str] = []
        aborted = False
        async with _closing(query(prompt=prompt, options=options)) as stream:
            async for message in stream:
                text = _message_text(message)
                if text:
                    texts.append(text)
                payload = _structured_output(message)
                if payload is not None:
                    structured.append(payload)
                accumulator.add(message)
                if budget is not None and budget.exceeded(accumulator.total()):
                    aborted = True
                    break
        # A StructuredOutput payload outranks trailing text: after the tool ack the model may emit
        # closing prose, and texts[-1] would hand that prose to the validator.
        final = structured[-1] if structured else (texts[-1] if texts else "")
        return final, aborted

    # ------------------------------------------------------------------ SDK plumbing
    def _query(self) -> QueryFn:
        if self._query_fn is not None:
            return self._query_fn
        import claude_agent_sdk  # noqa: PLC0415 - lazy: heavy dep, never imported by the test tier

        return claude_agent_sdk.query

    def _options_class(self) -> type:
        if self._options_cls is not None:
            return self._options_cls
        import claude_agent_sdk  # noqa: PLC0415 - lazy (see _query)

        cls = getattr(claude_agent_sdk, "ClaudeAgentOptions", None) or getattr(
            claude_agent_sdk, "ClaudeCodeOptions", None
        )
        if cls is None:
            raise RuntimeError(
                "claude_agent_sdk exposes no ClaudeAgentOptions/ClaudeCodeOptions — cannot set model, "
                "setting_sources or the tool allowlist (D5/D10); refusing to call"
            )
        self._options_cls = cls
        return cls

    def _build_options(
        self,
        agent_def: AgentDef,
        *,
        system_prompt: str,
        json_schema: dict[str, Any] | None,
        max_turns: int | None,
    ) -> tuple[Any, str]:
        """Build the call options; return ``(options, prompt_prefix)``.

        ``prompt_prefix`` is non-empty only when the options surface has no system-prompt knob, in which
        case the byte-stable system block is prepended to the prompt — preserving the D8 stable-first
        ordering rather than dropping the system prompt.
        """
        cls = self._options_class()
        fields = _option_fields(cls)
        kwargs: dict[str, Any] = {"model": agent_def.api_model, "setting_sources": []}   # D10: always
        if "setting_sources" not in fields:
            raise RuntimeError(
                f"{cls.__name__} exposes no setting_sources knob — the service must not inherit stray "
                "CLAUDE.md/settings from the filesystem (D10); refusing to call"
            )

        prefix = ""
        if system_prompt:
            if "system_prompt" in fields:
                kwargs["system_prompt"] = system_prompt
            else:
                prefix = f"{system_prompt}\n\n"

        if agent_def.max_output_tokens is not None:
            name = _first_field(cls, ("max_output_tokens", "max_tokens"))
            if name:
                kwargs[name] = agent_def.max_output_tokens

        # The CLI defaults to extended thinking, billed at output rates; measured 2026-07-28 it added
        # 15-50s to single-shot scoring calls for no schema benefit. Pinned off wherever the knob
        # exists. (If a Phase-3 agentic agent wants thinking, that becomes an agents.yaml knob.)
        name = _first_field(cls, ("max_thinking_tokens",))
        if name:
            kwargs[name] = 0

        # Explicit allowlist on EVERY call (D5/D10) — empty for single-shot. An options surface with
        # NO tool knob is refused for EVERY shape (2026-07-28 review): an SDK release that renames
        # the knob would otherwise run single-shot agents with the SDK's DEFAULT built-in toolset
        # (Bash/file/network) on the box holding the broker credentials — fail closed, never open.
        tool_field = _first_field(cls, ("allowed_tools", "tools"))
        if tool_field is not None:
            kwargs[tool_field] = list(agent_def.allowed_tools)
        else:
            raise RuntimeError(
                f"{cls.__name__} exposes no allowed-tools knob — cannot pin {agent_def.agent_id}'s "
                "explicit allowlist (empty for single-shot); refusing to call (D5/D10, §5.1)"
            )
        if "disallowed_tools" in fields:
            kwargs["disallowed_tools"] = [t for t in BUILTIN_TOOLS if t not in agent_def.allowed_tools]

        schema_sent = False
        if json_schema is not None:
            if "output_schema" in fields:
                kwargs["output_schema"] = json_schema
                schema_sent = True
            elif "output_format" in fields:
                kwargs["output_format"] = {"type": "json_schema", "schema": json_schema}
                schema_sent = True
            # Absent either knob the schema simply is not sent — client-side validation is authoritative
            # in every case anyway (§8.1 locked convention).

        if max_turns is not None and "max_turns" in fields:
            # The CLI fulfils a sent schema via a StructuredOutput TOOL choreography that consumes
            # turns of its own: the tool round-trip, a CLI-side schema-validation retry when the
            # payload misses, and a closing text turn after the ack (observed 2026-07-28: a clean run
            # is 2 turns, one schema retry makes 3 — a 1-turn cap killed every compliant call with
            # error_max_turns). +3 covers ~two schema retries; with no other tools allowed the extra
            # turns cannot be spent on anything else, and exhausting them still fails loudly.
            kwargs["max_turns"] = max_turns + 3 if schema_sent else max_turns

        try:
            return cls(**kwargs), prefix
        except TypeError as exc:
            raise RuntimeError(f"cannot build {cls.__name__} with {sorted(kwargs)}: {exc}") from exc

    # ------------------------------------------------------------------ metering / persistence
    def _price(self, agent_def: AgentDef, usage: TokenUsage | None) -> Decimal:
        if usage is None:
            return Decimal(0)
        return self._governor.price(agent_def.model, usage)

    async def _meter(
        self, agent_def: AgentDef, usage: TokenUsage, sdk_cost_usd: Decimal | None
    ) -> None:
        """Book billed usage into the governor ledger (D6). Zero-usage calls book nothing: a call that
        died before reporting usage would otherwise add a $0 row that says nothing."""
        if not any((usage.in_tokens, usage.out_tokens, usage.cache_read, usage.cache_write)):
            return
        await self._governor.record(agent_def.agent_id, agent_def.model, usage)
        if sdk_cost_usd is not None:
            # Ledger cost stays OURS (D4 rates, Decimal) — the SDK figure is logged for the §10.1
            # monthly console reconciliation, never written to the ledger.
            _log.debug(
                "agent_call_sdk_cost", agent=agent_def.agent_id, sdk_cost_usd=str(sdk_cost_usd)
            )

    def _persist(
        self,
        agent_def: AgentDef,
        *,
        trigger: str,
        inputs_digest: str,
        system_prompt: str,
        prompt: str,
        output: str,
        ok: bool,
        fail_reason: str | None,
        usage: TokenUsage | None,
        cost: Decimal,
        duration_ms: int,
    ) -> str:
        """Append one ``agent_calls`` row — one per SDK call, retries included (R8 replayability)."""
        call_id = str(ULID())
        blob = gzip.compress(f"{system_prompt}\n\n{prompt}".encode())
        used = usage or _ZERO_USAGE
        with transaction(self._conn):
            self._conn.execute(
                """
                INSERT INTO agent_calls
                    (call_id, agent_id, trigger, model, inputs_digest, context_gz, output_json, ok,
                     fail_reason, in_tokens, out_tokens, cache_read, cache_write, cost_usd,
                     duration_ms, at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    call_id,
                    agent_def.agent_id,
                    trigger,
                    agent_def.model,            # yaml name — joins to budget_ledger.model
                    inputs_digest,
                    blob,
                    output,
                    1 if ok else 0,
                    fail_reason,
                    used.in_tokens,
                    used.out_tokens,
                    used.cache_read,
                    used.cache_write,
                    str(cost),                  # TEXT column: decimal-as-string (§8.1), never a float
                    duration_ms,
                    self._clock.now().isoformat(),
                ),
            )
        return call_id

    async def _fail(
        self,
        agent_def: AgentDef,
        reason: FailReason,
        detail: str,
        *,
        call_id: str,
        usage: TokenUsage | None = None,
        raw_text: str | None = None,
        attempts: int = 0,
    ) -> AgentResult:
        """Build the D7 ``Failed`` result and alert the owner ONCE PER EPISODE (WO-25b).

        The failure itself is unchanged — ``agent_call_failed`` is logged by the caller for every
        single occurrence, and the returned result is identical. What is throttled is the OWNER ALERT:
        the first failure of a ``(agent, reason)`` alerts, further ones inside
        :data:`~engine.notify.episodes.REPEAT_AFTER` are logged as suppressed, and any successful call
        by that agent closes the episode (see :meth:`_run`). A dead SDK used to raise one owner alert
        per call for as long as it stayed dead, which is how a queue 200 deep gets built.
        """
        result = AgentResult.Failed(
            reason, detail, call_id=call_id, usage=usage, raw_text=raw_text, attempts=attempts
        )
        if self._alert is not None:
            if not self._alert_episodes.should_alert(
                (agent_def.agent_id, str(reason)), self._clock.now()
            ):
                _log.info(
                    "agent_alert_throttled",
                    agent=agent_def.agent_id,
                    reason=reason,
                    call_id=call_id,
                    note="repeat inside the episode window; logged, not alerted (WO-25b)",
                )
                return result
            try:
                await self._alert(
                    "error",
                    f"agent {agent_def.agent_id} failed: {reason} ({detail[:200]}) call_id={call_id}",
                )
            except Exception as exc:  # noqa: BLE001 - alerting is best-effort; never masks the failure
                _log.warning("agent_alert_failed", agent=agent_def.agent_id, error=str(exc))
        return result


# --------------------------------------------------------------------------- D11 smoke (S3.2.12)
class _SmokeContext:
    """Minimal AssembledContextLike for the self-test round-trip (no assembler dependency)."""

    system_prompt = 'You are a connectivity check. Reply with ONLY the JSON object {"ok": true}.'
    inputs_digest = "sdk-smoke"
    call_class = "schedule"

    def prompt(self) -> str:
        return "ping"


SMOKE_AGENT_DEF = AgentDef(
    agent_id="sdk_smoke",
    model="haiku-4.5",
    shape="single_shot",
    tools_enabled=False,
    allowed_tools=[],
    max_output_tokens=32,
    timeout_s=30.0,
)

# Every production agent sends a json_schema, so the smoke call sends one too — D11 must exercise
# the structured-output path the real calls take, not a text-only path nothing else uses
# (2026-07-28: a schema-less smoke passed while every schema call failed on the untested path).
SMOKE_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {"ok": {"type": "boolean"}},
    "required": ["ok"],
    "additionalProperties": False,
}


async def run_sdk_smoke(harness: AgentHarness) -> str:
    """One cheap Haiku round-trip through the FULL harness path (options, allowlist, structured
    output, validation, metering, audit row) — the D11 self-test call. Raises on any failure; the
    self-test maps that to WARN (LLM availability is never a safety input, D7)."""

    def _validate(raw: str) -> Any:
        data = json.loads(raw)
        if not isinstance(data, dict) or data.get("ok") is not True:
            raise ValueError(f"unexpected smoke payload: {raw[:80]}")
        return data

    result = await harness.run_single_shot(
        SMOKE_AGENT_DEF, _SmokeContext(), _validate, json_schema=SMOKE_SCHEMA
    )
    if not result.ok:
        raise RuntimeError(f"{result.reason}: {result.detail}")
    usage = result.usage
    tokens = f"{usage.in_tokens}in/{usage.out_tokens}out" if usage is not None else "usage n/a"
    return f"SDK round-trip ok ({tokens}, call {result.call_id})"
