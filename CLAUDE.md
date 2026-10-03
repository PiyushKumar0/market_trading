# CLAUDE.md

**Before any substantive work, read `FABLE_HANDOFF.md`** — the methodology transfer that supplements this file. Where the two conflict, `FABLE_HANDOFF.md` wins.

## Model policy — Fable is the manager, not the workhorse

The manager role runs on Fable. Fable is the most expensive model in the stack, so its time goes exclusively to work that requires judgement; everything else is delegated to cheaper models via the Agent tool.

Cost order, per token and in subscription-quota draw: Fable → Opus → Sonnet → Haiku. Route by this order; current prices are on the platform.claude.com pricing page if a decision ever needs the actual ratio.

### Fable does itself (judgement work — never delegate)

- Architecture, design, and trade-off decisions
- Diagnosis: forming hypotheses and naming the root cause of a bug. Evidence *gathering* (log digging, repro scans) is delegable; the verdict is not.
- Code review verdicts, audits, security reasoning
- Planning, task decomposition, and writing the specs subagents execute
- Interpreting ambiguous requirements; final synthesis and decisions on everything subagents return

### Delegate down — cheapest model that clears the bar

Subagents inherit the session model (Fable) **and the session effort level** by default — so grunt work bills at Fable rates and thinks at `xhigh`. **Delegate through the project agent types below**: each pins both `model` and `effort` in `.claude/agents/<name>.md` and carries the repo's hard rules (no engine control, no protected-store edits, git stays with the manager, tests inline).

| Work | Agent type (`subagent_type`) | Model / effort |
|---|---|---|
| Implementation where the spec leaves real decisions open (design gaps, failure modes the spec can't enumerate), multi-file refactors, tests for complex logic | `designer-impl` | `opus` / `high` |
| Implementation fully determined by the spec: boilerplate, mechanical edits, routine fixes with an already-named root cause, running test suites and reporting results | `implementer` | `sonnet` / `medium` |
| Token-hungry, low-judgement work: log digging, big-document reading, research sweeps, codebase scans, bulk extraction | `log-digger` (read-only) | `haiku` / inherited |

The definitions use the family aliases, which resolve to the newest model in each family — no version pin to maintain.

Where no agent type fits — browser use, escalating `log-digger` work a tier up, Haiku-rung material too big for 200K — use a general-purpose agent and **pass an explicit `model`**; it still inherits the session effort. Never pass a `model` override to one of the typed agents: the definition's pairing is the point.

Routing rules:

- **Unlisted work:** route by analogy — judgement stays on Fable, execution goes to the cheapest tier you're confident can clear the bar. Not confident? Start one tier up: a failed cheap attempt (attempt + audit + redo) costs more than a successful mid-tier one.
- **Do it inline instead** when the brief plus the audit would cost more than the work itself: single commands, quick test runs, a file read, a couple of greps, edits touching ≤2 files that need no investigation. If the delegation brief would be longer than the expected diff or output, delegating is a loss.
- Haiku has a 200K context window (others 1M). Material that plausibly won't fit goes to Sonnet — that's a capability constraint, not an escalation, but the task still holds that rung; no bouncing back down.
- Independent delegations go out in one message so they run in parallel.
- If delegation tools aren't available in a session, do the work directly and say so.

### Write specs like a manager

Ambiguity in a delegation prompt pushes judgement down to the model worst at it. Every brief states: the exact files/paths in scope, what the output must contain, the format to return it in, and what "done" looks like. Require **verifiable pointers** in the output — file:line references, quoted excerpts, exact command output — never bare paraphrase; the audit depends on them. If you can't write that spec, the task isn't ready to delegate — think first.

### Revision gate — audit everything that comes back

Auditing means checking the work against the spec, not re-doing it: spot-check a handful of the returned pointers (open a cited file:line, re-run one quoted command), then check scope coverage and edge cases. Do not re-read the full source material a Haiku agent digested — if output can only be trusted by re-doing the work, the spec failed to demand verifiable pointers; fix the spec.

A **miss** = the output fails any element of the written spec. New defects found on a retried output count as the next miss, not a fresh first one.

1. **First miss:** send the *same* agent back via SendMessage (it keeps its context) with a punch list — the specific defects and what correct looks like. Not a restated task.
2. **Second miss by that agent:** escalate one tier (haiku → sonnet → opus → Fable does it itself) — without asking. Escalation spawns a **fresh agent with no memory of the failure**, so the new brief must include the full original spec, the punch list, and any salvaged partial results — the higher tier fixes, it doesn't restart. Skip tiers when the failure mode is a judgement problem rather than an execution problem.
3. **Cut-loss:** at most 4 delegated attempts per task across all tiers. Hit the cap, or watch two tiers fail on the same task — Fable does it itself.

> Judge the output, not the price tag. If a cheaper model's work misses the bar, escalate without asking.

Escalation is one-way per task — never bounce a task back down. And a clean result from a cheap model is a clean result; don't re-do work on Fable just because it was done cheaply.

## Operations — the live engine shares this account's Claude subscription

The engine's analysts (`mt-engine`) draw on the same subscription session window as every Claude Code session on this machine. On 2026-09-03 a session-limit hit blacked out the intraday and news analysts 12:42–14:32 IST (logged by the SDK as "error result: success"). **On trading days, keep agent fan-outs, workflows, review swarms and delegated full-suite runs out of 09:15–15:30 IST**; inline work is fine. If you see "You've hit your session limit", assume the engine is dark too and check its log.
