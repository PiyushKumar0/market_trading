# AUDIT_PROMPT.md — Full-System Audit & Improvement Specification

<!--
OPERATOR NOTES (for Piyush — not part of the agent prompt)

How to run: start a FRESH Claude Code session at the repo root and say:
    Read AUDIT_PROMPT.md and execute it end to end.
Use the strongest model available for the session; the audit is judgement-heavy.
Evidence-gathering subtasks can be delegated down per CLAUDE.md's routing ladder.

Output: the agent writes IMPROVEMENT_SPEC.md at the repo root. That file is the
handoff artifact for the implementation agent (its contract is §7 below — the
implementer needs no other context). Review Part I of the spec yourself before
handing Part II to the implementer; C-category items commit you to experiments.

Written 2026-08-04, grounded in a five-way subsystem mapping of the codebase at
commit 6ad78d1 (branch phase2). File pointers below were verified at that commit;
if the tree has moved on, trust the grep, not the line number.

════════════════ EVERYTHING BELOW THIS LINE IS THE PROMPT ════════════════
-->

## 0. Mission

You are auditing a personal NSE/Zerodha trade-recommendation engine (Python,
Windows, ~₹20,000 capital, Phase 2 "RECOMMEND" — it messages the owner
buy/sell/stop recommendations via Telegram; it places no orders). The owner's
problem statement, verbatim in spirit:

> The engine does not successfully produce profit-making intraday buy/sell
> recommendations. Find what is architecturally, logically, mathematically, or
> conceptually wrong or improvable — research, algorithms, models, assumptions —
> across the whole system, with special depth on the intraday path. No bias, no
> pre-existing notion, no unnecessary criticism. Then specify the required
> changes so precisely that a separate implementation agent can execute them.

Your deliverable is a single file, `IMPROVEMENT_SPEC.md`, whose exact contract
is in §7. You do not implement anything. You diagnose, verify, decide, and
specify.

Before substantive work, read `CLAUDE.md` and `FABLE_HANDOFF.md` at the repo
root. FABLE_HANDOFF.md §1–§5 is the epistemic standard this audit is held to;
§5's backtest checklist (lookahead / survivorship / costs / multiplicity) is
mandatory for every quantitative claim you make or evaluate.

## 1. Ground rules

1. **Observations vs inferences.** Every finding states which it is. "I verified
   X" only when you observed X (file:line, command output, reproduced number).
   Otherwise "I believe X because Y."
2. **Two-hypothesis rule.** No root-cause claim until you have articulated at
   least one alternative and the observation that discriminates them. This
   applies to the big question (§4.1) most of all.
3. **The recorded evidence is data, not gospel — in both directions.** §3 lists
   the project's own experimental record. Those experiments may themselves be
   flawed: audit their designs (windows, cost assumptions, fill assumptions,
   selection effects) before inheriting their conclusions. A refuted hypothesis
   with a defective test is unrefuted. Symmetrically, the one recorded positive
   (insider_net_buy) gets the same adversarial scrutiny as the negatives.
4. **The owner's framing is data, not truth.** "Doesn't produce profitable
   intraday recommendations" bundles two distinct symptoms — verify which is
   real before solving either: (a) the funnel produces ~zero recommendations at
   all; (b) recommendations, when produced or hindsight-simulated, lose money.
   These have different root causes and different fixes.
5. **Materiality bar.** Report only findings that change a decision, a number,
   or a risk. No style notes, no "could be cleaner", no criticism for
   completeness' sake. What you audit and find *sound*, say so once, in one
   line, in the Sound-as-is register — that is a real result and prevents
   re-litigation.
6. **No silent scope narrowing.** The mandate is the whole system (architecture,
   data, signals, math, validation, costs, LLM layer, ops efficiency) with
   extra depth intraday — not intraday only.
7. **Safety layer is read-only.** Envelope (`config/envelope.yaml`), limits
   (`config/limits.yaml`), kill paths, watchdog, gate invariants: you may flag
   defects in them, but no work order may weaken them. If you believe a limit is
   miscalibrated, the work order is "present analysis to owner", never "change
   the limit".
8. **House validation discipline is binding.** No strategy/parameter change may
   be specified as "implement and ship". Every alpha-affecting work order ships
   as a pre-registered experiment through the existing machinery (sweep → CPCV →
   `fold_pass_min` promotion gate, §3.4) or through an explicitly specified new
   harness, with the promotion bar and the abort criterion written down before
   the experiment runs.

## 2. Orientation — the system as built

(A prior mapping pass produced this; spot-check pointers as you go. Repo root:
this directory. All times IST; NSE calendar in `config/calendar/`.)

**Signal path (live):** ticks → 1m bars on an event bus (`topic "bar.1m"`) →
`SignalPreScreen` (`src/engine/strategy/prescreen.py`) runs registered scanners
per bar off-thread → candidates deduped per `(symbol, strategy_id)` per day,
global cap `max_candidates_per_day=20`, optional per-strategy cap → surviving
candidates trigger the LLM analyst gate (`src/engine/ops/pipeline.py`,
`on_signal_candidate`, with a daily analyst-call forward cap, observed 6/day) →
Claude analyst (`src/engine/intelligence/agents/intraday.py`, single-shot,
byte-stable system prompt, context in `src/engine/intelligence/context.py`) →
on `enter`: structural-coherence check, then deterministic Tier-2
`RiskGate.evaluate` (`src/engine/risk/gate.py`) which does authoritative sizing
and cost viability (`CostModel.min_viable_qty`, `edge_multiple_min`) →
`Recommendation` payload → Telegram (`src/engine/notify/`). Owner closes the
loop with `/taken`, `/closed`, `/veto`; outcomes land in the learning ledger.

**Scanners** (`src/engine/strategy/scanners/`, params via envelope injection,
defaults in each file's `DEFAULT_PARAMS`):

| id | style | rule (one line) | notable |
|---|---|---|---|
| `orb` | MIS intraday, 1m bars | close beyond first-30-min range on ≥1.5× 20-bar median volume; stop = opposite range edge; target 1.5R; entries 09:30–14:30 | 0/15 CPCV twice (§3); kept as LLM-free control |
| `brk20` | CNC swing, daily, batch (not per-bar) | close(y) > 20-day high, fresh cross, ≥1.2× volume; stop = broken level; 2R target | added 2026-08-04 after BPCL miss; `entry` = *yesterday's* close — a price no longer tradeable |
| `rsi2` | CNC swing | RSI(2)<10 above 200-DMA in index uptrend; 4% stop; exit RSI>65 or 10 days | indicator series deliberately includes the current bar's provisional close, and entry = that same close |
| `trend` | CNC position | 20/50 EMA cross + ADX>20; 2.5×ATR trail | same provisional-close construct on the EMA series |
| `mom` | CNC swing | 4-week momentum rank top-2, 15-day rebalance | rank precomputed in context provider |

The planned `cat` (news-catalyst) scanner does not exist yet; the news layer
currently feeds watchlists/context only. No scanner consults the cost model at
signal time — cost first appears as an informational line in the LLM context and
authoritatively at the gate.

**Backtest/validation** (`src/engine/learning/`): vectorbt sweeps
(`sweep.py`) over envelope-derived Cartesian grids; winner = lexicographic
(expectancy, total return, −maxDD) among in-sample results; that single winner
then goes to `validate.py`: anchored walk-forward 6m/1m (reported only, not
gating) + skfolio CPCV (6 folds choose 2 = 15 splits, purge/embargo 5d), fold
passes iff mean net daily return > 0, promotion needs `fold_pass_min(N)` (60%
for N≤10, 70% ≤30, 80% above; N = grid cardinality). Costs in the sweep are a
**single constant proportional fee** = ½ × breakeven% at a ₹20,000 reference
notional, both legs (`sweep.py:_per_side_fee`) — an acknowledged approximation
of the exact per-order Decimal `CostModel` (`src/engine/strategy/cost_model.py`)
used live. Only `orb/rsi2/trend/mom` have sweep harnesses; filings/news legs
were tested by separate event studies (`scripts/event_study.py`,
`scripts/validate_insider.py`), not CPCV. **No event-driven ReplayHarness
exists** (`tests/replay/` is empty; Phase-3 aspiration).

**Costs** (`config/costs.yaml`, contract-note-verified to the paisa
2026-07-28): MIS round trip at ₹20k ≈ ₹21 (0.106%); CNC at ₹20k ≈ ₹60 (0.30%,
DP-charge-dominated); 5× MIS ₹1L ≈ ₹83 (0.083% of notional = 0.41% of capital).
Fixed platform costs ≈ ₹550–675/month ≈ 2.75–3.4% of capital monthly. **The cost
model contains no bid-ask spread or slippage component anywhere** (verify).

**Data:** 1-minute history floor is 2025-07-10 (hard); daily history deeper.
Universe ~NIFTY200 with liquidity-ranked live watchlist recently raised 50→100
symbols (the BPCL breakout at rank 96 was invisible to per-bar scanners — that
miss is what motivated `brk20`). Corp-action adjustment must precede any
cross-day comparison (`src/engine/datafeeds/corp_actions.py`).

**Governing docs:** `IMPLEMENTATION_PLAN.md` is the design source of truth
(§1.2 honest economics, §3.6 payload, §5.2/5.3 agents, §6.1 strategies, §6.3
envelope, §8 phase gates); `runbooks/WORKLOG.md` is the dated experimental
record. The project is deliberately design-first: code follows plan.

## 3. The empirical record — claims to verify, then build on

Dated results from `runbooks/WORKLOG.md` (re-verify quotes yourself; audit the
experiment designs, not just the numbers):

1. **ORB v1 & v2:** 0/15 CPCV folds, twice (2026-07-12/13). v1 diagnosis: cost
   floor vs ATR(14,1m) stop geometry — stops smaller than costs. v2 (structural
   range-edge stops): still 0/15; *gross* drift of breakouts negative in the
   window. Parked as honest-negative control.
2. **E1 (2026-07-17):** catalyst-conditioned ORB refuted — insider-cluster
   −0.039% gross (n=177), results-window −0.029% (n=826), unconditioned +0.028%
   (n=43,474), all win rates ≈41–43%.
3. **RSI2 economics (2026-07-17):** 102 trades, 70% win, **+0.58% net/trade** —
   the swing leg is alive; catalyst-veto idea unanswerable (n=1).
4. **Filings event studies (2026-07-17):** `insider_net_buy` +0.75%/+1.61% net
   T+10/T+20 (n=110 position-days), robust across year slices and person
   categories; results-PEAD gross-positive but sub-cost (n=1,352); "+1% move
   confirms catalyst" refuted a third time (n=266, net negative).
5. **Hindsight replay (07-29→31):** mechanically taking all Telegram-channel
   sweep setups: **−₹790 net**; engine-evaluated crossings: −₹797; the
   filter stack's declines dodged 8 stops. Conclusion recorded: intraday
   touch-entries net-negative at retail costs; the stack earns its keep by
   saying no.
6. **Live funnel (2026-08-03):** 817 raw candidates → caps → 8 analyst calls →
   8× `no_action` → 0 recommendations. Flagged as a watch item, not a defect.
   2026-07-29: published=1, pending=10, suppressed=7; a +₹51 proposal was
   killed by confidence 0.54 < 0.55.
7. **Cost model verified against a real contract note** to the paisa
   (2026-07-28) — fees, not slippage.
8. **G1 news entity-resolution gate passed 96%** (2026-08-04) after three
   remediation cycles; news layer scores live but originates nothing yet.

## 4. What to audit

### 4.1 The intraday question (the core of this audit)

The owner wants profitable intraday recommendations. The record above says
every intraday *momentum* hypothesis tested so far is negative — gross, before
costs, in the tested windows — while swing legs clear costs. You must deliver a
reasoned verdict, with the discriminating evidence named, choosing (or
apportioning) between:

- **H1 — Defects:** the intraday pipeline has correctness/measurement defects
  (see seeds, §4.2) that make a viable edge look dead or keep it from surfacing
  (funnel starvation, mis-costing, stop geometry, universe blindness).
- **H2 — Wrong strategy class:** intraday viability exists but not in breakout
  /momentum form at this cost floor. Candidate families the record has *not*
  tested: intraday mean-reversion (e.g. VWAP-deviation reversion), range/
  compression patterns, auction/gap dynamics at the open, close-drive effects,
  liquidity/imbalance signals. Which, if any, have plausible per-trade gross
  edge > 2× the cost floor at feasible holding periods, given 1m bars and a
  100-symbol watchlist? Support with the repo's own data before proposing.
- **H3 — Structural non-viability at this configuration:** at ₹20k capital
  (≈₹60k–1L MIS buying power), 0.106% fee floor + spread + slippage, NIFTY200
  liquidity, and a human-in-the-loop execution latency, no realistic intraday
  edge clears costs. If so: state the arithmetic cleanly, name the parameters
  that would flip it (capital, product, holding period, execution quality), and
  redirect the intraday budget to where the evidence points.

Do the domain arithmetic **first** (FABLE_HANDOFF §2: cost floor vs bar-range
geometry; add a spread estimate — measure typical NIFTY200 spreads from stored
tick/bar data if possible), because it bounds which hypotheses are worth
pursuing. Your verdict here shapes the whole spec: be explicit about which
outcome the evidence supports and how confident you are, in the graded terms of
FABLE_HANDOFF §1.

### 4.2 Seeded questions from the preliminary scan

Starting points, not conclusions — verify each yourself; none is pre-judged,
and your audit must not be limited to them:

1. **Slippage/spread absence.** `costs.yaml` and `CostModel` appear to model
   statutory fees only. Backtests fill at signal-bar closes with no spread.
   Direction of bias: backtests *optimistic* — which strengthens, not weakens,
   the recorded negatives, but also mis-prices the live gate's viability check.
   Where should spread live (cost model? fill model?), and what number does the
   stored tick data support?
2. **Live/backtest semantic parity.** Live `rsi2`/`trend` compute indicators on
   completed dailies *plus the current 1m bar's provisional close* and enter at
   that close. Does `sweep.py`'s daily-bar signal builder reproduce exactly
   this timing, or does it enter at the completed daily close — and is either
   fill achievable in reality (an order placed after observing close(t) cannot
   fill at close(t))? Quantify the gap if present; this class of bug decides
   whether the +0.58%/trade RSI2 number is real.
3. **`brk20` stale entry.** Candidate's `entry` is yesterday's close — the
   recommendation states a price that no longer exists. What should the entry
   semantics be (next-open? limit-at-level? band?), and what does the
   stated-vs-achievable gap cost?
4. **Funnel design under caps.** 817 candidates/day compete for 20 candidate
   slots and ~6–8 analyst calls. How are the survivors chosen — score-ranked or
   effectively arrival-ordered? Dedupe-first-wins? If the best candidate of the
   day structurally cannot reach the analyst, that alone explains symptom (a)
   of §1.4. Trace `prescreen.py` admission order and the forward-cap selection.
5. **Analyst gate calibration.** 8/8 `no_action` days; a positive-outcome
   proposal killed at confidence 0.54 vs 0.55 threshold. Is the LLM
   confidence scale calibrated against realized outcomes at all (learning
   ledger has the data)? Is `no_action` the model's true judgement or an
   artifact of prompt/context (e.g. cost line dominating, missing catalyst
   context, conservative system-prompt wording)?
6. **Sweep cost approximation.** Constant proportional fee calibrated at ₹20k
   both legs vs the true fixed+capped structure: for which (strategy, notional)
   pairs does the approximation change a CPCV fold verdict? (CNC's ₹15.34 DP
   flat charge is savage at small notionals; MIS's ₹20 cap matters above
   ₹66,667.)
7. **Winner-selection multiplicity.** The in-sample lexicographic winner is the
   only config CPCV-validated; `fold_pass_min(N)` is the sole multiplicity
   control. Is that bar adequate for the grid sizes actually swept (check
   recorded N), or does it need deflated-Sharpe / rank-stability across folds?
8. **Validation-window poverty.** 1m data starts 2025-07-10 (~13 months). CPCV
   with 6 folds on ~250 sessions, purge+embargo 10d: how much independent
   information do 15 splits really carry, and does the 2025-H2 negative-drift
   regime dominate every fold (the ORB "0/15" may partly be "one regime,
   sampled 15 times")? What would a regime-stratified or daily-bar-extended
   design add?
9. **Watchlist blindness.** Rank-96 BPCL was invisible pre-fix; cap is now 100
   of ~200 eligible. What is the measured opportunity cost of the remaining
   blindness, and is per-bar scanning of the full universe feasible (batch
   daily rules like brk20 sidestep it — should more legs be batch)?
10. **Dead/aspirational machinery.** No ReplayHarness (empty `tests/replay/`),
    no position-vs-broker reconciler, `cat` scanner unbuilt, mom
    rebalance-due state gap ("every day rebalance-due until ledger lands").
    Which absences actively distort current results vs merely await Phase 3?
11. **Efficiency.** LLM spend ($1.44/day observed) vs value delivered; token
    budget governor calibration; the 100-symbol 1m backfill cost; DuckDB reads
    on the scan hot path; the parquet store holds ~2.1M files totalling
    ~3.9GB (≈2KB/file — assess small-file read amplification). Secondary to
    correctness — quantify before proposing.

### 4.3 Dimensions checklist (breadth pass)

Beyond the seeds, sweep each dimension at least once; one line in the spec's
register even if healthy: architecture & process model; event-bus/threading
correctness under load; data integrity (bar building, reconciliation, corp
actions, calendar/time edges — 09:15 open, 15:29–15:30, expiry days);
indicator math (`indicators.py` — Wilder smoothing, warm-up handling); cost
model completeness; sizing math; risk-gate logic; LLM context assembly
(staleness, token discipline, what the analyst *can't* see that it needs);
learning-ledger integrity (is outcome capture unbiased?); news layer's signal
value as currently wired; test suite adequacy (1,170 green — what do they *not*
cover?); ops/failure modes already logged in WORKLOG (WS 403 loop, suspend
wakeups) only where they corrupt decisions, not mere availability.

## 5. Method requirements

- Read `runbooks/WORKLOG.md` and the load-bearing plan sections (§1.2, §3.6,
  §5.2–5.3, §6.1–6.3, §8) in full before forming any verdict. The plan already
  concedes base rates (71% of retail intraday traders lose; success metric for
  Phases 0–3 is process quality and learning-per-rupee, not profit) — your
  audit sharpens that stance with evidence; it does not need to rediscover it.
- Every quantitative claim you rely on gets re-derived or re-run where
  feasible; every file-behavior claim gets a file:line pointer you actually
  opened. FABLE_HANDOFF §5's backtest checklist applies to every experiment you
  evaluate *and every experiment you design*.
- For each major finding, write the one-sentence root cause with mechanism,
  plus the alternative you ruled out and the observation that ruled it out
  (FABLE_HANDOFF §2). Findings that fail this test go to an "unverified leads"
  appendix, clearly separated.
- Self-adversarial pass before writing the spec: attack your own verdict on
  §4.1 as if a rival wrote it; reconcile in writing (one paragraph in Part I).

## 6. What you may run (and must not)

- **May:** read everything except `.env`/secrets; run the unit suite
  (`python -m pytest tests/unit -q` via `.venv`); run offline research CLIs
  (`scripts/backtest.py`, `scripts/event_study.py`, `scripts/smoke_test.py`);
  query the DuckDB/Parquet stores read-only; re-run sweeps/CPCV with modified
  *analysis* code in a scratch copy if needed for verification.
- **Import-order trap:** `sklearn` must import before `numba`/`vectorbt`/
  `cvxpy` or the process segfaults; importing `engine` first handles it
  (`engine._preload`). Use PowerShell, not bash (`sleep`-loop and PATH bugs on
  this machine).
- **Must not:** touch live broker session/tokens, the engine service, protected
  stores, `state.db` writes, or anything under `config/`; no `git` mutations;
  no order placement of any kind; no edits outside `IMPROVEMENT_SPEC.md` and a
  scratch directory. Long-running sweeps: cap wall-clock, note what you didn't
  run — silent truncation reads as coverage.

## 7. Deliverable contract — `IMPROVEMENT_SPEC.md`

One file, repo root, three parts. The implementation agent will receive Part II
with **no other context** — every work order must be self-contained.

**Part I — Findings & verdict (for the owner).**
1. TLDR: ≤10 lines, leading with the §4.1 verdict and the 3 highest-impact
   changes.
2. The intraday verdict: H1/H2/H3 apportionment, the discriminating evidence,
   confidence grade, and the self-adversarial reconciliation paragraph.
3. Findings, ranked by expected impact, each: what was observed (pointers /
   reproduced numbers), root cause with the ruled-out alternative, which work
   order addresses it.
4. Sound-as-is register: what was audited and found healthy (one line each).
5. Considered-and-rejected: ideas you evaluated and dropped, with the reason —
   this stops the implementer (and future audits) from re-proposing them.
6. Unverified leads: anything plausible you could not confirm, marked clearly.

**Part II — Work orders (for the implementer).** Ordered execution roadmap
(dependency-aware, P0 first), then one block per work order:

```
WO-<n>: <imperative title>
Category: A-correctness | B-validation-methodology | C-alpha-experiment |
          D-architecture-efficiency | E-observability
Priority: P0 (wrong today) | P1 (materially improves decisions) | P2 (hygiene)
Evidence: file:line pointers + the numbers that motivated it (from Part I)
Change: exact behavior before → after; files to touch; config/envelope keys;
        schema/plan implications. Mechanism, not aspiration — "improve X" is
        banned; state the rule/formula/threshold and its justification.
Plan amendment: which IMPLEMENTATION_PLAN.md sections must be updated (this
        repo is design-first; the implementer updates the plan, then the code).
Acceptance: verifiable done-criteria, including tests to add and the exact
        commands whose output proves completion.
Validation (C-category, mandatory): pre-registered experiment — hypothesis,
        dataset/window, cost/fill assumptions (spread included), promotion bar
        (default: house CPCV + fold_pass_min unless you specify and justify a
        better harness), and the abort criterion. The implementer runs the
        experiment and STOPS for owner review before any live wiring.
Effort: S / M / L.  Depends-on: WO-ids.
Risk: blast radius, safety-layer interactions (none may weaken §1.7 items),
        rollback note.
```

Rules: every C-category WO is gated behind its experiment; A/B fixes that
change backtest semantics state which recorded results they invalidate and
whether re-running them is itself a WO; nothing in Part II may contradict
Part I's verdict (if H3 wins, C-category intraday WOs must be the
capital/product/horizon changes that follow from it, not more breakout tuning).

**Part III — Appendix.** Commands run with outputs (COMMANDS.md convention),
data/windows used, anything the implementer must not rediscover.

## 8. Suggested execution shape (adapt freely)

Orient (docs + WORKLOG, ~fast) → domain arithmetic for §4.1 → verify the §4.2
seeds in parallel where independent → breadth pass §4.3 → deep-dive whatever
the evidence promotes → self-adversarial pass → write `IMPROVEMENT_SPEC.md`.
Track state in a todo list; externalize intermediate findings to scratch files
as you go (context discipline). If you can delegate (Agent tool available),
route evidence-gathering down per CLAUDE.md's ladder and keep every verdict at
your own tier. Timebox: if a seed resists verification after two genuine
attempts, log it as an unverified lead and move on — an honest "could not
confirm" beats a plausible guess.
