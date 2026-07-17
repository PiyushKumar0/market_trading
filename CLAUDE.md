# CLAUDE.md

## 2026-07-13: Fable retired — Opus is now the manager

Fable access ended 2026-07-12. The manager role in the policy below passes to Opus unchanged: everything under "Fable does itself" is now Opus's own work, done inline. The delegation rungs below Opus are unchanged (Sonnet for spec-determined execution, Haiku for token-hungry low-judgement work); implementation formerly routed to `"opus"` is now done inline by the manager, or delegated to Sonnet when the spec is complete enough. **Before any substantive work, read `FABLE_HANDOFF.md`** — the methodology transfer written on Fable's last session. Where the text below still says "Fable", read "Opus"; where the two documents conflict, `FABLE_HANDOFF.md` wins.

## Model policy — Fable is the manager, not the workhorse

This session runs on Claude Fable 5. Fable is the most expensive model in the stack, so its time goes exclusively to work that requires judgement; everything else is delegated to cheaper models via the Agent tool.

Cost ladder (per MTok in/out, as of 2026-07): Fable $10/$50 → Opus $5/$25 → Sonnet $3/$15 → Haiku $1/$5. If prices drift, only the ordering matters.

### Fable does itself (judgement work — never delegate)

- Architecture, design, and trade-off decisions
- Diagnosis: forming hypotheses and naming the root cause of a bug. Evidence *gathering* (log digging, repro scans) is delegable; the verdict is not.
- Code review verdicts, audits, security reasoning
- Planning, task decomposition, and writing the specs subagents execute
- Interpreting ambiguous requirements; final synthesis and decisions on everything subagents return

### Delegate down — cheapest model that clears the bar

Subagents inherit the session model (Fable) by default — **always pass an explicit `model` when delegating**, or grunt work bills at Fable rates.

| Work | Model |
|---|---|
| Implementation where the spec leaves real decisions open (design gaps, failure modes the spec can't enumerate), multi-file refactors, tests for complex logic | `"opus"` |
| Implementation fully determined by the spec: boilerplate, mechanical edits, routine fixes with an already-named root cause, running test suites and reporting results | `"sonnet"` |
| Token-hungry, low-judgement work: log digging, big-document reading, research sweeps, browser use, codebase scans, bulk extraction | `"haiku"` |

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
