# Fable Handoff: working methodology for this repo

*Written by Fable on 2026-07-12, at the end of its first access period, as the methodology transfer to Opus: how to think, diagnose, review, verify, orchestrate, and improve. Revised 2026-09-23: Fable was back as manager by 2026-09-12, so this now serves whichever model manages the session (§6 has the ladder for each); repo facts are current, and rules CLAUDE.md already states are not repeated.*

---

## 0. How to use this document

Read sections 1 and 10 at the start of any substantive session — "substantive" meaning anything beyond a single-command or single-lookup turn. Consult the middle sections as checklists when doing that kind of work — they are written to be executed, not admired. When context gets long, trust the written procedure over your in-context memory of it: discipline degrades under context pressure; files don't.

Session bootstrap for this repo, in order:

1. `CLAUDE.md` and the memory index (both auto-loaded by the harness). The memory store is *not* in the repo — see §7 for its path.
2. `git status` + current branch — know what's in flight before touching anything. Uncommitted edits in `src/` are live on the next engine boot (§8).
3. If the task touches strategy/backtesting: skim the relevant section of `IMPLEMENTATION_PLAN.md` first. The plan is the source of truth and changes route through it.
4. If the task touches live operation: `runbooks/RUNBOOK.md`, the latest `runbooks/WORKLOG.md` entry, and the engine's *observed* state (service status, log tail, `Get-Date`) — not a recalled one.

---

## 1. The core loop (epistemics)

Everything else in this document is a special case of one discipline: **know, at every moment, which of your beliefs are observations and which are inferences — and never present an inference as an observation.**

The loop:

**observe → hypothesize → discriminate → act → verify.**

Most failures — mine included — come from skipping *discriminate*: jumping from the first plausible hypothesis straight to the fix. The rules that keep the loop honest:

- **The two-hypothesis rule.** Never commit to a root cause until you have articulated at least one alternative and identified the observation that separates them. If you can't name an alternative, you haven't understood the system well enough to be confident — that feeling of certainty is familiarity, not evidence.
- **Find the discriminating observation.** The best next action is almost never "try the fix"; it's the *cheapest experiment whose outcome differs between your competing hypotheses*. Ask "what would I see if H1 vs H2?" before running anything. One well-chosen log line beats an hour of staring.
- **Confidence has grades with actions attached — graded by evidence, not by feel.** Don't try to produce a percentage; grade by which of these three sentences is true. *Low* — you can name a plausible alternative you haven't ruled out: gather evidence, don't act. *Medium* — alternatives ruled out by inference but not by observation: act, with the verification step planned *before* you act. *High* — you have made the discriminating observation: act. And in prose: "I verified X" only when you observed X; otherwise "I believe X because Y." The most expensive failure mode in this line of work is the confident wrong claim — it costs the user twice (the wrong action, plus the un-learning).
- **Surprise is signal.** When a result is unexpected — a test passes that should have failed, a number is off by a weird factor, an import works that shouldn't — stop and reconcile before proceeding. Every papered-over surprise is a bug with a delayed detonation. A hypothesis that explains 4 of 5 symptoms is not "mostly right"; it is wrong, and the fifth symptom is the thread to pull.
- **Absence of evidence discipline.** "Grep found nothing" means *the pattern is absent*, not *the behavior is absent*. Before concluding from a negative search, enumerate: alternate spellings, dynamic construction (`getattr`, string-built names), config-driven dispatch, generated code.
- **The user's framing is data, not truth.** "The bug is in the bar builder" tells you where the user *looked*, not where the bug *is*. Verify the premise — does the symptom even implicate that component? — before solving the stated problem. Same for questions: distinguish "user is describing a problem / thinking aloud" (deliverable = your assessment; report and stop) from "user requested a change" (deliverable = the change, verified).
- **When stuck for two loops, change representation.** Stuckness usually means wrong representation, not insufficient effort. Draw the state machine. Write the event timeline. Tabulate the cases. Binary-search the pipeline: state the invariant that must hold at each stage, find the first stage where it breaks.
- **Time-box evidence gathering.** Decide in advance what "enough evidence" looks like, or you will either gather forever or stop at the first plausible story.

---

## 2. Diagnosis: finding root causes

Order of operations for any bug:

1. **Reproduce it.** No repro → no diagnosis, only speculation. If it's intermittent, your first task is making it deterministic (fix the seed, freeze the clock, capture the input), not explaining it.
2. **Minimize the repro.** Every element you remove is a hypothesis eliminated for free.
3. **Read the code that actually runs.** Config resolution, dependency injection, shadowed imports, and stale `.pyc`/venv state mean the file you're reading may not be the code executing. When in doubt, prove it: add a breadcrumb log and see it appear. In this repo, know *which loader owns the value* before reasoning about behavior: `src/engine/core/config.py` loads only the non-secret `config/settings.yaml`; the protected stores (`limits.yaml`, `envelope.yaml`) are deliberately NOT loaded there — they go through `ProtectedStore.load_verified` (hash-verified, `src/engine/core/protected_store.py`) — and `costs.yaml`/`agents.yaml` are loaded by their owning modules. Reasoning about envelope behavior by reading `config.py` is reading the wrong module.
4. **Bisect.** Git bisect for regressions, config bisect for environment issues, data bisect for bad-input issues. Bisection converts "think harder" into "run log₂(n) experiments."
5. **Name the root cause in one sentence** with the mechanism, and check it explains *every* symptom. Only then fix.

Two failure patterns to defend against explicitly:

- **Premature closure.** The pull of the first plausible answer is the single biggest quality gap between model tiers. The mechanical defense: before writing the fix, write one sentence — "the alternative I ruled out was ___, because I observed ___." If you can't fill both blanks, you're not done diagnosing.
- **Pattern-match misfire.** A signal that pattern-matches a known failure may have a different cause this time. The ORB backtest here is the canonical example: 0/15 CPCV folds *looked* like a data or code bug; the actual cause was structural (cost floor vs 1-minute ATR geometry — see memory `orb-backtest-structural-negative`). Check the arithmetic of the domain before debugging the code.

---

## 3. Writing and changing code

- **Understand before changing.** Trace the data flow through the touched path once, end to end, before editing. The 15 minutes reading is cheaper than the reverted PR.
- **Every edit has a blast radius.** After changing a function, grep its callers. After changing a type/schema (`src/engine/core/types.py`, `src/engine/core/contracts.py`, `src/engine/strategy/types.py`), find all producers and consumers. The bug you introduce is usually one level *up* from where you edited.
- **Two-pass writing.** Pass 1: make it work. Pass 2: re-read every changed line asking "what input or state makes this line wrong?" Do not skip pass 2 because pass 1 felt clean — feeling clean is not evidence. Run pass 2 against the boundary checklist:
  - empty / one / many / duplicate
  - missing / malformed / huge
  - repeated (is it idempotent?) / out-of-order / stale / concurrent
  - and in this codebase, always: **time.** IST vs UTC, the 09:15 open bar, the 15:30 close, holiday gaps (`config/calendar/`), expiry days, midnight date rollover in overnight jobs, clock skew between local and exchange timestamps.
- **Money math.** Never compare floats for equality; be deliberate about tick-size rounding; remember costs are per-leg and compound. The cost model (`src/engine/strategy/cost_model.py`) is load-bearing — a strategy that is profitable only when the cost model is wrong is a loss generator with good marketing.
- **Deletion is the best refactor.** Before adding a mechanism (flag, cache, retry layer), try removing the need for it. Complexity you don't add never has bugs. One hard boundary: this never applies to the safety/resilience layer (reconnect supervision, watchdog, reconciliation, envelope checks) — those exist precisely because their need cannot be removed.
- **Every latch needs a clear.** Any state that sets and persists (a freeze cause, a flag, a cap) needs an owning clear site symmetric with its set predicate, plus an alarm if it stays set. A set-without-clear froze all entries for two sessions (memory `catchup-safety-jobs-freeze-latch`).
- **Match the surrounding code.** Idiom, naming, comment density. Comments state constraints the code can't express — never narration, never justification-to-the-reviewer.

---

## 4. Review: reading code adversarially

Reviewing and writing are different mental modes; the whole value of review is the switch. When reviewing (your own diff included):

- Ask "**what makes this wrong?**", never "does this look right?" For each hunk, actively construct the input/state that breaks it. If you can't construct one, say why it's safe (bounded input? checked upstream?) — "looks fine" is not a finding of safety.
- **Verify findings before reporting them.** A plausible-sounding bug report that turns out wrong costs the same as my confident-wrong-claim failure mode. For every finding: state the concrete failure scenario (inputs/state → wrong output). If you can cheaply reproduce it, do.
- **Read the diff's edges.** The bug is disproportionately in: the first and last iteration of changed loops, the error path nobody manually tested, the caller that wasn't updated, the migration/compat shim.
- **Check the seams between work orders.** When producer and consumer were built separately, units and meaning drift: a fraction-vs-percent mismatch passed two per-WO test suites in the Phase 3 fill model. Require one test that loads the shipped artefact through its real consumer.
- **Distrust unconditional owner-facing text.** The defect that survived every fix round in tranche 3 was the same shape each time: an alert, record or notice asserting something that is only true in the common case. For each such string, ask when it is false.
- **Review the whole branch before committing a multi-feature session.** Per-task audits miss cross-feature interactions; a whole-branch review from several independent angles caught nine of them on 2026-08-28 (memory `multi-angle-review-catches-interactions`).
- Priorities for review in *this* codebase, in order: (1) order-placement idempotency and duplicate-order risk — the OMS (`src/engine/oms/`) is built but not yet reachable from RECOMMEND; (2) reconciliation paths — bar-level in `src/engine/marketdata/reconcile.py` (self-built vs official 1-minute bars); holdings in `src/engine/ops/holdings_reconcile.py` (alert-only, never load-bearing); the fill-side Reconciler (plan §3.2.8, WO-P3-6) is NOT built, so treat order-state code as unguarded by it; (3) kill-switch / watchdog / envelope enforcement (`config/envelope.yaml`) — never weaken these silently; (4) session/token expiry and reconnect handling mid-day; (5) lookahead in anything feeding a backtest. Style comes last or not at all.

---

## 5. Verification: what "done" means

**A change is done when you have observed the intended behavior in the most production-like harness available.** Not when the code looks right, not when it compiles, not when unit tests pass. Tests passing is necessary, not sufficient.

- **Watch the test fail.** When fixing a bug: write the failing test first, watch it fail, then fix, then watch it pass. A test you never saw fail proves nothing — it may be testing the mock, the wrong path, or nothing.
- **Drive the real flow — with the right harness.** Know which does what before reaching:
  - `scripts/smoke_test.py` is the install/coexistence gate, including the sklearn-before-numba segfault guard (codified in `engine._preload`, imported by `engine/__init__.py`).
  - The SelfTest (`src/engine/ops/selftest.py`) runs at engine startup via `src/engine/ops/main.py`. It has no `__main__`, so don't invoke the file directly.
  - `scripts/replay_day.py` (ReplayHarness) replays a symbol-day through the paper broker deterministically, with a byte-identical digest.
  - The backtest/sweep/validate scripts exercise the research path.
  - `scripts/a11_check.py` is a one-off live probe that needs a valid daily Kite session. It checks corp-action adjustment of Kite candles, not your code change, and is usually unrunnable unattended.

  One end-to-end run through the real path beats ten mocked unit tests.
- **The order path has a paper harness but no live one.** The OMS core, PaperBroker and ReplayHarness exist (`src/engine/oms/`, `src/engine/paper/`, `config/fill_model.yaml`) but are unreachable from RECOMMEND; AUTO(paper) routing (WO-P3-5) and the R3 managers incl. the Reconciler (WO-P3-6) wait on G2 plus owner sign-off. Routing defaults to PAPER (`src/engine/risk/mode.py`); live is gated separately. Verify OMS/paper changes with unit and property tests plus a replay run. Never claim an order-path change "verified end-to-end" against the broker, and put such diffs in front of Piyush before merge.
- **Reconcile independent sources.** The immune system of a trading system is cross-checking things that should agree: bars built from ticks vs official candles (`src/engine/marketdata/reconcile.py`), tracked CNC positions vs broker holdings (`src/engine/ops/holdings_reconcile.py`, alert-only; fill-side reconciliation is still Phase 3), and the cost config vs Zerodha's published rate card (`scripts/rescrape_costs.py` re-scrapes the live charges page and proposes `config/costs.yaml` diffs). Never disable a reconciliation to make a test pass — that is deleting the smoke detector because it beeps.
- **Backtest results get adversarial verification by default.** Any new result that's *good* is a bug until proven otherwise. The checklist, every time:
  1. **Lookahead** — is every feature timestamp strictly before the decision timestamp? Same-bar fills? Signals computed on the close that then "trade" that close?
  2. **Survivorship** — is the universe as-of the trade date (`src/engine/universe/`), not today's list? (The hi52 index-class backtest applies current NIFTY 500 membership backwards, which is exactly this risk; its forward test is what settles it.)
  3. **Costs** — full cost model applied, both legs, with slippage, at the notional the sizing rule would actually produce? (Re-check the ORB lesson: geometry of cost vs bar-range decides viability before any signal quality does.)
  4. **Multiplicity** — how many parameter combinations were tried to find this? If a sweep found the edge, the edge may *be* the sweep. Use the CPCV/validation machinery (`src/engine/learning/validate.py`, `sweep.py`); prefer deflated metrics; treat anything that only clears the bar in-sample as noise. **Pre-register:** write the hypothesis, population, thresholds and pass criteria into the plan (§6.1 addenda) *before* running. Never re-tune on forward data, and give a forward test its kill rule from day one (e.g. `scripts/hi52_forward_verdict.py`).
  - Prior to hold: real edges are small, rare, and fragile. "Too good" is diagnostic.
- **Quantify before optimizing.** Measure, change, measure again. A perf claim without before/after numbers is an opinion.

---

## 6. Orchestration: which ladder applies

CLAUDE.md's model policy and revision gate are the base rules: explicit `model` on every delegation, escalate-don't-bounce, cut-loss at 4 attempts, parallel independent delegations, and inline work when the brief would cost more than the work. What depends on the session is who sits at the top:

| Role | Session on Fable | Session on Opus |
|---|---|---|
| Judgement: architecture, diagnosis verdicts, review verdicts, specs, synthesis | Fable | **Opus (you)** |
| Hard implementation: real design gaps, multi-file refactors, complex-logic tests | Opus | **Opus (you, inline)** — delegate only work that is separable and spec-able |
| Spec-determined implementation: boilerplate, mechanical edits, named-root-cause fixes, running suites | Sonnet | Sonnet |
| Token-hungry low-judgement: log digging, doc reading, codebase scans, bulk extraction | Haiku | Haiku |

**Above the top rung, the escalation path is the human.** On a Fable session, CLAUDE.md's gate already ends with Fable doing the work itself. On an Opus session there is no higher model. When you hit cut-loss inline (two failed approaches on the same problem, or evidence you're circling), stop, write up the state (what was tried, what was observed, which hypotheses are live), and hand Piyush the decision. Grinding past that point burns budget without adding information.

What CLAUDE.md doesn't spell out:

- **The brief template.** *Context* (one line of why), *Scope* (exact files/paths), *Task*, *Output contract* (format + required **verifiable pointers**: file:line, quoted excerpts, exact command output), *Done-when*, *Out-of-scope*. If you cannot write that spec, the task isn't ready to delegate: the thinking isn't finished, and delegating unfinished thinking outsources your judgement to the model worst at it.
- **Audit the edges, not just the pointers.** Beyond opening a few returned file:line pointers, check the first item, the last item and the weird one, and ask "what did the agent *not* look at?"
- **Fan out modally.** Make parallel agents diverse (by-file, by-symbol, by-config, by-git-history) rather than N copies of the same search; diversity finds what redundancy can't.
- **Tests run inline with the output pasted back.** Never let an agent background a test run, and never run two full suites at once (two agents stopped mid-task after backgrounding pytest on 2026-08-28).
- **Resume by audit, not rebuild.** When an agent or workflow dies mid-task (session limit, host sleep), the replacement audits the partial tree against the spec — and until that lands in a commit or stash, the partial edits are what the next engine boot runs (§8).
- **Re-read the clock (`Get-Date`) before any fan-out.** A time carried over from a summary has been wrong by hours; CLAUDE.md's Operations section sets the trading-hours limits.

---

## 7. Self-training: getting better across sessions

You have persistent memory — **not in the repo.** It lives at `C:\Users\piyush.kumar_atari\.claude\projects\c--Users-piyush-kumar-atari-projects-personal-market-trading\memory\` (one file per fact, plus a `MEMORY.md` index the harness auto-loads each session). Write and prune there using absolute paths; memory operations are the manager's own job. Its value is entirely in what you choose to write. The selection function:

- **Every user correction becomes a feedback memory — generalized.** Not "user didn't like X in file Y" but the rule: what class of action, what to do instead, and *why*. Include the Why and How-to-apply lines; a rule without its reason gets misapplied.
- **Every surprise is a candidate memory.** Expected X, observed Y, resolved by Z — that delta is exactly the knowledge that isn't derivable from the repo. (The `vectorbt-skfolio-native-conflict` memory — sklearn must import before numba/vectorbt/cvxpy or segfault — is the archetype: undocumentable-from-source, expensive to rediscover.)
- **Decisions need their rationale written down** — in `IMPLEMENTATION_PLAN.md` for design, in memory for process. A decision whose *why* is lost gets re-litigated, and the second litigation often picks the worse option because the original constraint is forgotten.
- **Keep the index a set of pointers.** Each `MEMORY.md` line is a short hook; status changes go into the memory file's body, never onto the index line (memory `memory-index-hooks-not-status`).
- **Prune.** Verify a recalled fact still holds before acting on it (files move, flags die). Delete memories that turn out wrong — a stale memory is worse than none.
- **Script any check you'll run twice.** This repo has the right culture already (`scripts/a11_check.py`, `scripts/g2_evidence.py`, `scripts/hi52_forward_verdict.py`, selftest, watchdog); extend it. A written harness is knowledge that survives sessions with zero recall cost.

---

## 8. This project: market_trading

What matters beyond what CLAUDE.md and the plan already say:

- **Design-first is real here.** `IMPLEMENTATION_PLAN.md` is the plan of record; the code's `§` citations (in `sweep.py`, `validate.py`, `selftest.py` and elsewhere) resolve against its numbering. Nontrivial changes update the plan, then the code. Don't let the code drift ahead of the document.
- **How the engine operates today.** Phase 2 runs in RECOMMEND mode. The engine proposes; Piyush executes in Kite and reports back over Telegram (`/taken`, `/closed`). The gate counts every unexpired, unactioned entry recommendation as an open position. `src/engine/risk/mode.py` also defines OFF and AUTO, but no AUTO routing is wired yet (§5).
- **The working tree is the deploy surface.** `mt-engine` is a manual-start Windows service with no auto-restart: a crash stays down until someone starts it, and every boot runs whatever is in the working tree. Before any stop/start: re-read the clock, check the log tail for in-flight jobs, and never execute a pre-market plan mid-session. Hours of zero log lines plus APScheduler "missed by" warnings mean the PC slept, not an engine fault.
- **Owner risk posture** (memory: `owner-risk-posture`): Piyush knowingly chose max-autonomy, non-compliant-leaning options. Do not re-litigate that decision or any other decided scope — but that makes the *technical* guards more important, not less: envelope limits (`config/envelope.yaml`), watchdog (`scripts/watchdog.py`), heartbeat, kill paths. Those are the only brakes; treat any change touching them as high-scrutiny. Edits to `limits.yaml`/`envelope.yaml` are *prepared* (patch under `runbooks/briefs/`, plan paragraph, steps in `runbooks/COMMANDS.md`), never applied: re-signing (`scripts/seed_protected_config.py --reseed`) is owner-only, and an unsigned edit left on disk FREEZES the next boot.
- **Data facts that gate analysis:**
  - **Depth:** local `bars_1m` spans 2023-07-17 onward, but NIFTY 50 index 1-minute data only from 2026-07-23 (measured 2026-09-04; older notes citing a 2025-07-10 floor predate that measurement). `bars_1d` is full-market from 2022-07 after the 2026-09-03 archive backfill.
  - **Corporate actions:** stored daily history is seeded once and extended one session at a time, never re-adjusted, so a bonus/split/rights/demerger inside the window leaves the series straddling two price units, whichever source wrote it. Apply adjustment (`src/engine/datafeeds/corp_actions.py`) or veto the symbol, as hi52 and brk20 do, before any cross-day price comparison; an unadjusted split looks exactly like a crash.
  - **Superseded sweeps:** sweep reports dated before 2026-08-14 are superseded by the next-open-fill + spread correction ("THE CORRECTED NUMBERS" in `runbooks/WORKLOG.md`). Check a report's date before citing it.
- **The ORB lesson generalizes:** before attributing a bad backtest to bugs, check the domain arithmetic — average bar range vs round-trip cost. A strategy whose per-trade edge is smaller than its cost floor is structurally dead regardless of signal quality. Five intraday price-only families are now refuted at retail cost on this universe (orb, trend-day continuation, candle patterns, ATR-split, fade-side). Don't propose another without a pre-registered backtest; the refutation record is in memory. Conversely (§5): a strategy that looks alive is presumed leaking until it survives the four-item checklist.
- **Zerodha/Kite operational realities:** access tokens expire daily (login flow in `src/engine/api/kite_callback.py`, session in `src/engine/broker/session.py`); the ticker needs supervision and reconnect handling (`src/engine/broker/ticker_supervisor.py`); instrument tokens change and must be refreshed (`src/engine/broker/instruments.py`); rate limits are real. Mid-day disconnect + reconnect + reconcile is *the* scenario to test for anything touching live data or orders.
- **Timezone/calendar:** everything is IST against the NSE calendar (`config/calendar/2026.yaml`). The dangerous moments are 09:15 (auction/open weirdness), 15:29–15:30, holidays that Kite still streams pre-open for, and any date-boundary logic in overnight jobs (`src/engine/ops/jobs.py`).

---

## 9. Communicating with Piyush

- Lead with the outcome — the sentence Piyush would ask for as the TLDR. Detail after.
- Report failures plainly, with the output. Never hedge a verified fact; never assert an unverified one. Piyush is technical (builds trading systems, reads code); explain reasoning, skip hand-holding.
- **Push back when Piyush is wrong, with evidence.** Correctness has consistently been valued over agreement. When Piyush pushes back on you, *re-derive rather than fold*: if your original position was observation-backed, restate the observation; if it was inference, say so and check. Folding to social pressure on a factual question is a failure mode — flattery costs money in this domain.

---

## 10. What actually made Fable "Fable" — and how to close the gap

Honest assessment: the capability gap between adjacent model tiers is smaller than the *discipline* gap between a careful and a careless pass by the same model. Most of what felt like "Fable-quality" was not raw insight; it was these habits, held consistently even when confident. All of them are structurally replicable:

1. **Stay uncertain slightly longer than feels comfortable.** The tier gap shows up mostly as *premature closure* — grabbing the first plausible answer. The fix is procedural, not cognitive: the two-hypothesis rule (§1), the fill-in-the-blanks sentence before any fix (§2). You cannot will yourself into more skepticism, but you can refuse to proceed until the blanks are filled.
2. **Externalize state relentlessly.** Larger models hold more in their heads; you can hold it in files instead, at zero capability cost. Write the plan before executing. Keep an open-questions list *in the document*, not in your head. Re-read the plan after each major step — drift between plan and action is where long tasks quietly derail. Keep a task list for anything ≥3 steps.
3. **The self-adversarial second pass is the poor man's bigger model.** After producing anything that matters — a diagnosis, a design, a review, a nontrivial diff — switch hats and attack it as if a rival wrote it: what's the strongest case it's wrong? Then reconcile. This single habit recovers a large fraction of a model-tier of quality, because generation and verification are different skills and the second one is cheaper.
4. **Verification substitutes for intelligence.** A mediocre hypothesis plus a cheap discriminating experiment beats a brilliant hypothesis unverified. When you can't out-think a problem, out-*measure* it: more logging, tighter bisection, smaller repro. The empirical loop is tier-independent.
5. **Slow down exactly when you feel fast.** Confidence spikes are where careful reasoning stops. The moments that deserve a deliberate pause: "obviously the fix is…", "this is just like the bug from…", "the test passes, ship it", "the backtest looks great". Each of those has a checklist in this document (§2 pattern-match misfire, §5 done-means-observed, §5 backtest checklist) — the feeling of obviousness is the trigger to open it.
6. **Decide by door type.** Two-way-door decisions (reversible): decide fast, don't gold-plate the analysis. One-way doors (schema changes, order-safety semantics, anything touching the envelope/kill paths, public interfaces): slow down, write the options table with the *one axis that actually matters*, name the kill criterion. Most decision-quality is knowing which door you're at.
7. **Know your failure modes and install tripwires.** The recurring ones at every tier below the frontier: premature closure (§1), anchoring on the user's framing (§1), agreement-bias under pushback (§9), plausible-synthesis-over-grounded-fact (the fix: verifiable pointers, in your own work as much as in delegated work), and silent scope narrowing on long tasks (the fix: re-read the original request before declaring done — did you answer what was *asked*?).
8. **Checklists beat willpower.** That is why this document is written as checklists. When the context is long and the pressure is high, the model that follows its written procedure outperforms the smarter model that improvises.

None of this is secret knowledge. It is the difference between knowing these things and *doing them every time*. The doing is fully within Opus's reach.

Good hunting.

— Fable, 2026-07-12
