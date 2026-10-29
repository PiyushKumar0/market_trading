# Review actions: implementation plan (2026-10-29)

> **For agentic workers:** implement task by task with superpowers:subagent-driven-development (recommended)
> or superpowers:executing-plans. Steps use `- [ ]` checkboxes. Read `FABLE_HANDOFF.md` §1, §3 and §5 first.

**Goal:** ship the recommendations of the TradingAgents review (`runbooks/tradingagents-review-2026-10-27.md`,
"the report"). Measure every published scanner signal and every analyst decision against outcomes. Pre-register
the verdicts that decide the analyst's role and retire losing strategies. Apply the C2 regime filter. Remove LLM
work and LLM-fed gates that nothing measures. Make the owner channel produce decision data.

**Architecture:**
- **Scoring.** One nightly hindsight job scores every published brk20, hi52 and ins signal two ways, into a new
  `signal_outcomes` table: at the rule's own entry, and at the next open after the analyst's decision. C2-blocked
  brk20 crossings are scored too.
- **Journals.** The day-slot journal records the analyst's first decision. `agent_calls` records a prompt
  version.
- **Verdicts.** One pure module turns the rows into fixed-cohort, pre-registered verdicts. A CLI, the dashboard
  and the weekly summary read it.
- **Registration date R.** Every change to the analyst's prompt or to a verdict population lands before R, so
  every population starts clean.

**Tech stack:** Python 3.12, SQLite migrations (`src/engine/core/migrations`), DuckDB `MarketStore`,
python-telegram-bot ≥21 (async), pydantic v2, pytest. React + TypeScript dashboard (`node --test`).

**Spec:** the report. Citations are its section numbers (§9 #2, A3) or `file:line` at `c358c5f`.

---

## Global constraints

- **Protected stores.** Never edit `config/limits.yaml` or `config/envelope.yaml`.
  - T10 prepares a patch; the owner applies it with COMMANDS "Protected config".
  - The code that tolerates the change ships first. An unsigned edit FREEZES the next boot.
  - Never stash, checkout or revert a dirty `config/limits.yaml`. Since O19 it is the owner's signed capital
    change.
  - Never commit `config/limits.yaml` from the work branch.
- **Running pre-registered tests stay untouched.** That covers:
  - the hi52 forward test: its population, construct and `scripts/hi52_forward_verdict.py`;
  - D14/C5: `paper.no_stop_strategies`, its horizon and `scripts/no_stop_verdict.py`;
  - the `cat` and `cat_reversal` shadow windows and thresholds;
  - every completed study script (for example `scripts/backtest_regime_filter.py`).
- **Migrations are additive.** Numbers are pre-assigned: `0018` (T1), `0019` (T2), `0020` (T3), `0021` (T13).
  - Renumber to stay contiguous if tasks run out of order.
  - Each migration gets an upgrade-from-a-populated-predecessor test shaped like
    `tests/unit/test_migration_0017.py`.
  - Update `tests/unit/test_migrations.py` if it pins the applied set.
- **The hindsight boundary.** `signal_outcomes` is read only by owner surfaces, verdict code and research. A
  scanner, threshold, gate, prompt or promotion path never reads it, mirroring `rec_outcomes`
  (`src/engine/ops/rec_outcomes.py:1-6`). T3 pins this with a test.
- **Money and time.** Money is `Decimal` or TEXT. Timestamps are tz-aware IST from `Clock`.
- **Threads.** SQLite writes stay on the event-loop thread. A worker thread returns data and the loop writes it
  (`main.py:2359-2364`).
- **Read-only tools.** They open state.db as `file:...?mode=ro` and never open the live `data/market.duckdb`.
- **Imports.** Every new or touched script imports the worktree's `src`, either through the shim in
  `scripts/no_stop_verdict.py:47-49` or through `$env:PYTHONPATH`.
- **Code style.** Match the surrounding code: docstrings carry the why and the constraint, with no narration.
- **Shell and commits.** Use PowerShell. Write scripts that contain non-ASCII with the Write tool. Commit through
  `git commit -F <file>`, one commit per task, with no AI co-author trailer.
- **The worktree.** Work in a named worktree outside `%TEMP%`, for example `..\mt_review` on branch
  `wip/review-actions` from `phase3`. Its `.venv` and `dashboard\node_modules` are junctions into the live tree.
  - Run tests only with `.\.venv\Scripts\python.exe -m pytest`.
  - Never run `uv run`, `uv sync` or `uv pip` there (memory `worktree-venv-imports-live-tree`). Put this rule in
    every agent brief.
- **Market hours** (09:15–15:30 IST on trading days):
  - none of these without the owner's go-ahead that day: an agent fan-out, a full gate, a
    `scripts/replay_agents.py` run, an engine restart;
  - re-read `Get-Date` before each.
- **Test tiers** (COMMANDS "Tests / verification"):
  - production code: `tests/unit` and `tests/chaos` (the gate);
  - research-only code: `tests/offline`;
  - dashboard logic: `dashboard/tests` (`npm test`).

## Calls this plan makes for the owner (check these first)

| # | Question | This plan's choice | Why | Alternative |
|---|---|---|---|---|
| D-A | Where C2 applies | In the brk20 batch sweep, behind `settings.yaml brk20.regime_filter: true` | C2 measured the regime at the signal session (`scripts/backtest_regime_filter.py:140`). A gate rule would judge 5-session retests at later sessions, spend an analyst call on every candidate it must reject, and need a reseed for a strategy rule | A `limits.yaml` gate rule (the wording in IMPLEMENTATION_PLAN §3) |
| D-B | Signal ledger | A new `signal_outcomes` table plus decision columns on `prescreen_day_slots`. `shadow_trades` stays reserved | `shadow_trades` is the Phase-5 per-param-set book (`0001_initial.sql:246-265`). Published signals already live in `prescreen_day_slots`, the hi52 kill-rule population. **Narrowing:** cap-refused and same-day-deduped fires are not ledgered, because the published set is what the strategy delivers. C2-blocked crossings ARE ledgered, so C2's blocked arm keeps gathering forward evidence (the 2026 held-out slice contradicted it) | Ledger every raw fire, which means hooking the prescreen accept path |
| D-C | Kill-rule construct | **brk20:** a touch fill at the level on y+1..y+5 (V2-5's window), exit at the close of the 20th session counting the fill session. **ins:** entry at the open of the publication session, exit the same way. No stops. Cost at the §7.1 notional at the floor-rung equity, fixed when the row is first scored | A kill rule tests the quantity the edge was measured on (handoff §5). The 20-session exit is C2's arm (ii) and the live time-exit convention; V2-5 itself measured close(fill+20) | Next open for both |
| D-D | Kill thresholds | DEMOTE iff the look's median net T+20 ≤ 0. Look 1 is the first 20 fills, as a fixed cohort; look 2 is the first 40 and supersedes it | hi52's shape without its hit-rate clause. brk20's registered T+20 hit rate is 54%, so a "<50%" clause at n=20 fires on noise | Add hit < 50% |
| D-E | Analyst verdict | A fixed cohort: the first 30 two-arm (day, strategy) cells in date order, formed when all their rows are final. Mean approved − declined net T+20 (next open) ≤ 0 ⇒ ADVISORY for hi52 and ins. Look 2 is 60 cells | Report §9 #2, made exact. The fixed cohort prevents optional stopping. The cell unit removes the strategy-mix confound (§6.2) | A running mean read at ≥30 cells |
| D-F | `analyst_confidence_min` | Remove it: code tolerates its absence, then the owner patch | It is uncalibrated, the model never sees it, and it sits between the model's two favourite values (§6.2) | Keep it until a calibrated floor exists |
| D-G | Heartbeat | Retire it. A deterministic, dated index-regime line replaces the model-written note | 517 calls whose only output fed the same agent. The note was a self-amplifying prompt surface (`context.py:166-179`) | Keep it on a cheaper tier (B2) |
| D-H | Planner `no_trade_today` | Remove it | Never set in 60 plans, and no code reads it. A market pause belongs to a pre-registered deterministic rule (T17) | Keep it |
| D-I | trend | Park it: `max_per_strategy_day.trend: 0` | No validated edge; its contract tells the analyst so; 34 forwards, mostly declined (§6.3) | Keep forwarding |
| D-J | Market-pause study | hi52 and ins, with rule "core equal-weight 20-session return < 0". Each strategy's regime-filter family counts C2 too, so N = 2 and α = 0.025 | brk20 already carries C2. hi52 and ins already took one regime trial in C2, so the bar must see it | All three strategies |
| D-K | One-tap skip | Telegram inline buttons on the entry card and the 15:45 reminder; the first reason recorded stands | The card is where the owner reads the rec | Dashboard buttons as well |
| D-L | A4, as-of research reads | Deferred. Two finished studies read `earnings_calendar`, which has no knowledge time: the event-study PEAD legs (`scripts/event_study.py:606-632`) and the hvol earnings split (`scripts/backtest_hvol.py:612`). They stand with that caveat: PEAD dates each event at its own release, and the hvol split is a diagnostic. A known-at column only covers rows ingested after it ships | Its value starts only with new data. Trigger: the next study whose result depends on when an earnings date became known | Add the known-at column now |
| D-M | The live brk20 retest window | Align it to V2-5 (T5b). Today it offers the level on y+1..y+6, one session more than the registered y+1..y+5 | `_expiry` adds `retest_sessions` full sessions to `signal_d`, and `signal_d` is the publication day y+1 (`retest.py:182-183`, `:474-483`), not the crossing session its comments assume | Keep 6 sessions and re-register the mechanism |

## What happens to every recommendation

| Report item | Disposition |
|---|---|
| §9 #1 C2 brk20 regime gate | T5 (D-A) |
| §9 #1 keep D14 running to look 1 | No change to D14; T19 notes the population effects |
| §9 #1 market-wide pause | T17: a pre-registered study. Wiring only on SUPPORTED, by owner decision |
| §9 #2 analyst-value verdict | T1, T2, T3, T4, T19. Advisory mode is a conditional spec |
| §9 #3 confidence floor | T10 plus the owner patch |
| §9 #4 measured-edge pricing | The label changes now (T11). Pricing is a conditional spec keyed to the pooled C5 label |
| §9 #5 kill rules for brk20 and ins | T3, T4 |
| §9 #6 signal ledger | T1, T3, and T5's blocked arm (D-B) |
| §9 #7 LLM work without consumers | Heartbeat T6; `no_trade_today` T8; trend T9. Nightly unread fields: not planned, since they cost $12 over four months, no prompt reads them, and the reviewer's model is the owner's 2026-08-18 call |
| §9 #8 trial ledger + bar | T16 |
| §9 #9 exit-rec noise | Done by WO-D2 on 2026-09-12: one exit per session, none once holdings show zero (`pipeline.py:2774-2929`). The remaining gap is T14 |
| §9 #9 one-tap skip reasons | T13 |
| §9 #9 same-ticker history (B3) | T12 |
| A1 decision scoring | Analyst: T4 and T15. News and planner: T18 |
| A2 absent ≠ empty | T7 |
| A3 prompt provenance | T2 |
| A4 as-of research reads | Deferred (D-L) |
| B1 mechanical declines → rules | Conditional on the analyst verdict reading KEEP |
| B2 cheaper tier | Moot after T6 |
| §7.10 rsi2/trend construct drift | trend is parked (T9). rsi2 goes with #4; it has been regime-blocked since 08-28 |
| Found: exit/adjust cards print "edge 0x" | T11 |
| Found: catalyst, earnings and breakout sections print "none" when their job never ran | T7 |
| Found: the `reply_keyboard` seam was never wired; no `reply_markup` anywhere in `src/` | T13 |
| Found: the live retest window is one session longer than V2-5 | T5b (D-M) |
| Found: re-tapping a reason on an expired rec overwrites it (`pipeline.py:651-658`) | T13 |

## Sequencing

```
Phase 1 measure (no behaviour change):  T1 → T2 → T3 → T4
Phase 2 before R (prompt/population):   T5 → T5b, T6 → T7; T8, T9, T10      (T7's tests pin T6's regime line)
Phase 3 owner channel:                  T11, T12, T13, T14
Phase 4 standing numbers:               T15 (needs T4)
Phase 5 research governance (offline):  T16 → T17 (runs after T19), T18
Phase 6 ship:                           T19 → T20 (R is set before the merge)
```

- **What R is.** R is the first regular session that the deployed tip runs whole. Deploys happen outside
  09:15–15:30, so R is the first session after the planned deploy.
- **Where R is written.** It is fixed in T20 Step 3 in two places: `engine.learning.forward_verdicts.REGISTERED_FROM`
  and the IMPLEMENTATION_PLAN entries. If the deploy slips, R moves in the same commit.
- **The limits patch and R.** The owner's patch (T10) leaves the analyst's prompt and the verdict populations
  unchanged, so R does not wait for it. It does change D14's rec population, so T19 records the patch date in the
  D14 entry.
- **Suggested routing** (CLAUDE.md model policy):
  - `designer-impl`: T3, T4, T5, T6, T7, T13, T15, T17, T18;
  - `implementer`: T1, T2, T5b, T8, T10, T11, T12, T14, and the T16 module;
  - `log-digger`: the T16 ledger backfill, with pointers;
  - manager inline: T9, T19, T20.

## Review focus

1. **Restart or re-queue inside the session.** Re-arms and WO-20d re-queues must never write a second decision.
   The first successful decision stands (T1).
2. **Calendar and data holes.** Muhurat 2026-11-08 sits in `config/calendar/2026.yaml:65` and arrives as a
   backfilled bar. Other holes: halts, a missing anchor bar, corporate actions before or inside the hold, the
   calendar horizon (`verified_through` 2026-12-31). Each must resolve to `unscorable`, `void_ca` or `open`. A fill
   or a frozen net must never move after the fact (T3).
3. **Fixed cohorts.** For kill looks and analyst cells alike, membership comes from fill or decision order. A
   look forms only when every member is final (memory `pinned-looks-cut-from-fixed-cohort`, T4).
4. **Callbacks.** A non-owner chat, a forged or stale command, a double tap, or a tap on a decided rec is refused
   or answered. No state changes twice, and one rec's tap never removes another rec's buttons (T13).
5. **Ordering, regime and drift.**
   - A table without `analyst_confidence_min` loads before the owner patch exists (T10).
   - A missing or stale NIFTY 50 bar blocks brk20 with a log line. Neither case raises into the sweep (T5).
   - A changed prompt template, contract or model inside the verdict window is excluded and counted, never
     pooled (T2, T4).

---

## Phase 1: measure

### T1. Record the analyst's first decision on the day-slot journal

**Why:** the analyst's enter or decline exists only inside `agent_calls` blobs (report §10). The verdict
(§9 #2), the confidence diagnostics and the scorecard need it as a plain column.

**Files:**
- Create: `src/engine/core/migrations/0018_prescreen_analyst_decision.sql`.
- Modify: `src/engine/ops/pipeline.py`, at `_evaluate_forward` (`:2231-2369`) and beside `_journal_forward`
  (`:1142-1165`).
- Test: `tests/unit/test_reco_pipeline.py`, `tests/unit/test_migration_0018.py`.

**Interfaces:**
- Produces `prescreen_day_slots.analyst_decision` (`'enter'` | `'no_action'` | NULL).
- Produces `.analyst_confidence` (REAL | NULL).
- Produces `.analyst_call_id` (TEXT, joins `agent_calls.call_id`).
- Produces `.proposal_id` (TEXT, joins `proposals` and `verdicts`).

- [ ] **Step 1: migration**

```sql
-- The intraday analyst's FIRST successful verdict on a forwarded candidate (review §9 #2,
-- 2026-10-29) — the report's unit ("first evaluation of each signal"). A failed call writes nothing; a
-- WO-20d re-queue or re-arm that evaluates the same row again never overwrites the first decision.
ALTER TABLE prescreen_day_slots ADD COLUMN analyst_decision TEXT
    CHECK (analyst_decision IN ('enter', 'no_action'));
ALTER TABLE prescreen_day_slots ADD COLUMN analyst_confidence REAL;
ALTER TABLE prescreen_day_slots ADD COLUMN analyst_call_id TEXT;
ALTER TABLE prescreen_day_slots ADD COLUMN proposal_id TEXT;
```

- [ ] **Step 2: failing tests**, using the existing pipeline fakes:
  - `test_no_action_decision_is_journalled`: the row reads `('no_action', <confidence or None>, result.call_id, None)`.
  - `test_enter_decision_is_journalled_before_the_gate`: the row reads `('enter', 0.6, call_id, proposal_id)`.
    Do this both when the gate rejects and when the context build raises `GateContextTimeout`.
  - `test_first_decision_stands`: write a decision, then let an exception after the journal write make the
    WO-20d guard re-queue the row (`pipeline.py:1948`). A second successful evaluation must leave the first.
  - `test_failure_then_success_writes_once`.
  - `test_unjournalled_outputs`:
    - an identity mismatch (`pipeline.py:2304-2334`) and an agent failure leave the columns NULL;
    - an exit, modify or cancel answered to a candidate (`schemas.py:188-191`) also leaves NULL and logs
      `analyst_decision_unclassified`.
  - The migration upgrade test from a populated 0017 database.
- [ ] **Step 3: run them; they FAIL.**
  `.\.venv\Scripts\python.exe -m pytest tests/unit/test_reco_pipeline.py tests/unit/test_migration_0018.py -q -k "decision or 0018 or unjournalled"`
- [ ] **Step 4: implement** `_journal_decision(self, candidate: SignalCandidate, d: date, decision: str, confidence: float | None, call_id: str | None, proposal_id: str | None) -> None`.
  - The write is `UPDATE prescreen_day_slots SET ... WHERE d=? AND symbol=? AND strategy_id=? AND analyst_decision IS NULL`.
  - Rowcount 0 is normal after a first decision. Log `day_slot_journal_failed op=decision reason=no_row` only
    when the row does not exist at all.
  - Exceptions are logged in the `_journal_forward` style and never raised.
  - Call it in the `NoActionOutput` branch, before `self._decline(...)`.
  - Call it after the identity check and before `_gate_and_persist`, when `payload.action == "enter"`.
  - The arguments come from `payload.confidence`, `result.call_id` (`harness.py:346-355`) and
    `_evaluate_forward`'s own `d`.
- [ ] **Step 5: run them to PASS, then run every `tests/unit/test_reco_pipeline*.py`; commit.**

### T2. Prompt version on every agent call (A3)

**Why:**
- The analyst verdict must not pool prompts or models.
- `inputs_digest` differs on every call.
- The system prompt cannot be cut back out of `context_gz`, because it contains blank lines and the blob joins
  the two parts with `"\n\n"` (`harness.py:1198`).
- Fixed prompt text also lives in the assembler, outside the system prompt and the contract: the stable-block
  wording (`context.py:371-378`), the structural-absence note (`:509-523`) and the rel_volume legend (`:456-459`).

**Files:**
- Create: `src/engine/core/migrations/0019_agent_calls_prompt_version.sql`. It runs
  `ALTER TABLE agent_calls ADD COLUMN prompt_version TEXT;`, with a comment that this is the template identity,
  unlike the per-call `inputs_digest`.
- Create: `tests/unit/golden/for_signal_template.txt`.
- Modify: `src/engine/intelligence/context.py`, `src/engine/intelligence/harness.py`.
- Test: `tests/unit/test_context_assembler.py`, `tests/unit/test_agent_harness.py`,
  `tests/unit/test_migration_0019.py`.

**Interfaces:**
- `context.PROMPT_TEMPLATE_VERSION: str`. Bump it whenever the assembler's fixed wording changes.
- `AssembledContext.template: str`. `build()` always sets it to
  `PROMPT_TEMPLATE_VERSION + "\x00" + extra`, where `extra` is the strategy contract on signal calls and `""`
  otherwise. Contexts built by hand (`nightly_review.py:922`) get the version through the same `build()`.
- `harness._prompt_version(system_prompt: str, template: str) -> str`. It returns the first 16 hex characters of
  `sha256(system_prompt + "\x00" + template)`; `run_agentic` has no context and passes `template=""`.
- `agent_calls.prompt_version`. T4 keys the verdict on `(prompt_version, agent_calls.model)`.

- [ ] **Step 1: failing tests:**
  - `test_signal_context_carries_its_contract_as_template`: two brk20 candidates give the same template; brk20
    and hi52 differ.
  - `test_prompt_version_stable_across_calls_and_moves_with_contract_or_version`.
  - `test_every_persisted_call_has_a_prompt_version`. Cover a single-shot success, a `governor_blocked` row and
    `run_agentic` (no context object, so template `""`).
  - `test_for_signal_fixed_text_matches_golden`.
    - It renders `for_signal` on fixed synthetic inputs and compares the result with the golden file.
    - The golden file's first line records `PROMPT_TEMPLATE_VERSION`, and the test asserts it equals the
      constant.
    - Any change to the assembler's wording therefore fails until both the golden file and the version are
      updated.
  - The migration upgrade test.
- [ ] **Step 2: implement.**
  - Change the signature to `AssembledContext.build(..., template: str = "")`.
  - `for_signal` renders the contract body once and passes it as `template`.
  - `run_single_shot` passes `getattr(context, "template", "")` down. `_run` and `_persist` take the version and
    add it to the INSERT (`harness.py:1203-1227`).
  - The harness never imports the assembler (`harness.py:397-404`). It reads only
    `getattr(context, "template", "")`, which already carries the version.
- [ ] **Step 3: run to PASS; commit.**

### T3. Nightly signal outcomes (§9 #5, #6)

**Why:**
- Kill rules and the analyst verdict need a forward outcome for every published signal, not only the ~1-in-10
  that became a rec.
- One nightly writer keeps the verdicts runnable from state.db while the engine runs. The hi52 script, by
  contrast, needs the DuckDB export.
- It gives the dashboard a standing number from a single implementation of the measurement.

**Files:**
- Create: `src/engine/core/migrations/0020_signal_outcomes.sql`, `src/engine/ops/signal_outcomes.py`.
- Modify:
  - `src/engine/ops/jobs.py`: add `JOB_SIGNAL_OUTCOMES = "signal_outcomes"` beside `:157`.
  - `src/engine/core/config.py`: add `JobTimesCfg.signal_outcomes_ist = time(18, 40)`.
  - `config/settings.yaml`: add `jobs.signal_outcomes_ist: "18:40"`.
  - `src/engine/ops/main.py`:
    - add the job to `PHASE2_JOB_IDS` (`:299-303`) and `POST_ARM_JOB_IDS` (`:321-324`);
    - add `JobSpec(..., DATE_KEYED, order=36)` beside `:513`, and move `weekly_summary` to `order=37`;
    - construct the job beside `:1629` and map it at `:1665`.
- Test: `tests/unit/test_signal_outcomes.py`, `tests/unit/test_migration_0020.py`.

**Interfaces:**
- `SignalOutcomesJob(conn, store, calendar, cost_model, limits_engine, settings, clock, was_run).run(d) -> SignalOutcomesResult(d, unfinished, scored)`.
- Consumes: `exit_sim.simulate`, `Pending` and `Held`; `benchmark.bench_pct`; `hi52_edge.sizing_notional`
  (keyword-only); `brk20_resting_levels`; `brk20_regime_blocked` (written by T5); `universe_ew_returns` (read
  only; `RecOutcomesJob` remains its only writer).

- [ ] **Step 1: migration**

```sql
-- Hindsight outcome of every published brk20/hi52/ins signal (review §9 #5/#6, 2026-10-29). HINDSIGHT:
-- read by owner surfaces, verdict code and research only — never by a scanner, threshold, gate, prompt
-- or promotion path (the rec_outcomes rule). Constructs:
--   rule         — the strategy's measured construct: hi52/ins open of d; brk20 a touch of the resting level
--                  on signal_d..signal_d+4 sessions (V2-5's y+1..y+5), first publication only; no stop/target
--   rule_blocked — the same brk20 construct for a crossing C2 blocked (brk20_regime_blocked)
--   next_open    — open of the first session after d (strictly after the analyst's decision), every row
-- T+k exits at the close of the k-th regular session counting the fill session as 1 (muhurat excluded).
-- cost_pct is fixed when a row is first scored; fill and net_* freeze once set; a later pass may only
-- fill bench_t20/excess_t20.
CREATE TABLE signal_outcomes (
    d           TEXT NOT NULL,
    symbol      TEXT NOT NULL,
    strategy_id TEXT NOT NULL,
    construct   TEXT NOT NULL CHECK (construct IN ('rule', 'rule_blocked', 'next_open')),
    status      TEXT NOT NULL CHECK (status IN ('open', 'closed', 'unfilled', 'void_ca', 'unscorable')),
    fill_d      TEXT,
    fill_px     TEXT,
    exit_d      TEXT,               -- the T+20 exit session, or the voiding ex-date for void_ca
    cost_pct    REAL,
    net_t5      REAL,
    net_t10     REAL,
    net_t20     REAL,
    bench_t20   REAL,
    excess_t20  REAL,
    updated_at  TEXT NOT NULL,
    PRIMARY KEY (d, symbol, strategy_id, construct)
);

-- C2's blocked arm (T5 writes it on the loop thread; one row per blocked crossing per signal session).
CREATE TABLE brk20_regime_blocked (
    symbol      TEXT NOT NULL,
    signal_d    TEXT NOT NULL,      -- the sweep session y+1, as brk20_resting_levels.signal_d
    entry       TEXT NOT NULL,
    stop        TEXT,
    target      TEXT,
    score       REAL NOT NULL,
    index_close REAL,
    sma50       REAL,
    sma50_then  REAL,
    reason      TEXT NOT NULL,      -- 'down' | 'undefined' | 'index_bar_missing'
    created_at  TEXT NOT NULL,
    PRIMARY KEY (symbol, signal_d)
);
```

- [ ] **Step 2: failing tests** in `tests/unit/test_signal_outcomes.py`. Use the fixture style of
  `tests/unit/test_rec_outcomes.py:35-90` (frozen `Clock`, temp `MarketStore`, `NSECalendar(strict=False)`) and
  insert rows directly.
  - `test_hi52_rule_fills_at_the_open_of_d_and_exits_at_the_20th_close`: net_t20 =
    (close of the 20th regular session / open(d) − 1) × 100 − cost_pct.
  - `test_next_open_fills_at_the_open_after_d`.
  - `test_brk20_rule_touch_fill_inside_v2_5_window`:
    - fills at `min(open, level)` on the first bar with `low <= level` among signal_d .. signal_d+4 sessions;
    - a first touch on signal_d+5 is `unfilled`.
  - `test_brk20_rule_blocked_scored_like_rule` (from `brk20_regime_blocked`).
  - `test_brk20_reoffer_row_uses_latest_resting_row`:
    - a journal row with `d > signal_d` gets only `next_open`, costed from the latest resting row with
      `signal_d < d <= expires_d`;
    - a brk20 row with no resting row (every row before 2026-09-12) is `unscorable`.
  - `test_muhurat_bar_is_ignored`: a Sunday 2026-11-08 bar inside a hold or ahead of a `next_open` fill changes
    nothing. Bars are kept only where `calendar.session(b.d)` exists and `not session.is_muhurat`, as in
    `rec_outcomes._regular_sessions`.
  - `test_filled_row_never_moves`:
    - backfill a missing mid-hold bar after the row filled;
    - the re-score starts from `Held(stored fill_px, at=fill session close)`, so `fill_d`/`fill_px` and every
      frozen `net_*` stay put.
  - `test_cost_fixed_at_first_scoring`: change the capital between two runs; an open row keeps its `cost_pct`.
  - `test_closed_row_only_gains_bench`.
  - `test_corporate_actions_void`:
    - an unadjusted-kind ex-date (`hi52.UNADJUSTED_KINDS`) inside (fill, exit] gives `void_ca`;
    - for brk20, one in (signal_d, fill] or inside an unfilled window also gives `void_ca`;
    - `exit_d` = that ex-date.
  - `test_missing_anchor_bar_is_unscorable`: a market construct with no bar on its anchor session never borrows
    a later open.
  - `test_calendar_horizon_leaves_row_open`: `add_sessions` raising past `verified_through` keeps that row
    `open` and scores the others.
  - `test_unfinished_until_daily_bars_ran`.
  - `test_cost_pct_floor_rung`: with the conftest capital, base ₹40,000 gives a floor rung of ₹36,000.
    `sizing_notional(Decimal("36000"), position_pct=Decimal("2.0"), gap_mult=Decimal("2.0"), stop_pct=Decimal("6"))`
    = ₹6,000, then `breakeven_pct(6000, "CNC")`. The notional is capped at `cnc_notional_inr` and at
    `max_deployed_capital_inr`.
  - `test_only_signal_strategies_are_scored` (a `cat` row is ignored).
  - `test_no_decision_path_reads_signal_outcomes`: grep `src/engine/strategy`, `src/engine/risk`,
    `src/engine/intelligence` and `src/engine/ops/pipeline.py` for `signal_outcomes`; there must be no hits.
- [ ] **Step 3: implement** `signal_outcomes.py`, mirroring `RecOutcomesJob` (`rec_outcomes.py:64-102`): its
  HINDSIGHT docstring, its `unfinished` gate on `JOB_DAILY_BARS`, and its skip when a later day has already
  been scored.
  - **Exit-sim conventions** (the traps are at `exit_sim.py:118-126` and `:156-158`):
    - Call `simulate` WITHOUT `add_sessions`.
    - Pass only regular-session bars (the muhurat filter above).
    - Pass `end = d` (a date: the run session is covered).
    - Horizon `k − 1`.
  - **The constructs:**
    - hi52/ins `rule`: `Pending("market", None, placed=d - timedelta(days=1), until=d)`, bars from d.
    - brk20 `rule` / `rule_blocked`: `Pending("touch", level, placed=signal_d, until=calendar.add_sessions(signal_d, 4))`,
      bars from signal_d.
    - `next_open`: `Pending("market", None, placed=d, until=calendar.add_sessions(d, 1))`, bars from the day
      after d.
    - Map `unfilled` to `unscorable` for the market constructs.
  - **Rows that already filled** re-score from `Held`, so the fill and its frozen nets never move.
  - **Status** follows the T+20 outcome. A row is final when it is `closed` with a bench, or when its fill
    precedes `EW_BACKFILL_FROM`, or when it is `unfilled`, `void_ca` or `unscorable`.
  - **Cost:** `cost_pct = breakeven_pct(min(sizing_notional(floor_equity, position_pct=..., gap_mult=..., stop_pct=...), cnc_cap, deployed_cap), "CNC")`.
    - `floor_equity = capital_base_inr × (1 + equity_floor_rung.equity_pct_of_base/100)`, from the verified
      table at the row's first scoring.
    - Stop: hi52 `DEFAULT_PARAMS["stop_pct"]`; ins `settings.ins.stop_pct`; brk20 `(entry − stop)/entry` of its
      resting or blocked row.
  - **Bench:** `bench_t20 = bench_pct(ew, fill_d, exit_d, "time", calendar)` and
    `excess_t20 = net_t20 − bench_t20`.
  - **Reads and writes:**
    - Read each symbol's bars once (`store.arun(store.get_bars_1d, symbol, start, d)`).
    - Wrap each row in try/except: one bad row is logged and never kills the run.
    - Write only `signal_outcomes`, in one transaction.
- [ ] **Step 4: wire `main.py`** as listed under Files.
- [ ] **Step 5: run** `test_signal_outcomes.py test_migration_0020.py test_ops_main_wiring.py test_catchup_runner.py`
  **to PASS; commit.**

**Done when:**
- The first run scores the journal history.
- A re-run is idempotent.
- The first nightly run after the deploy logs `signal_outcomes_scored`.

### T4. Pre-registered forward verdicts: brk20 and ins kill rules, analyst value (§9 #2, #5)

**Files:**
- Create: `src/engine/learning/forward_verdicts.py`, `scripts/forward_verdicts.py` (with the src shim).
- Test: `tests/unit/test_forward_verdicts.py`. It runs in the gate tier, because `/scorecard` reaches the module.

**Interfaces** (T15 consumes these):
- `REGISTERED_FROM: date | None = None`. It is only the CLI's and the scorecard's default argument. T20 sets it.
- `@dataclass(frozen=True) SignalRow(d, symbol, strategy_id, construct, status, fill_d, exit_d, net_t5, net_t10, net_t20, excess_t20, analyst_decision, analyst_confidence, prompt_version, model)`.
- `load_rows(conn, *, strategies, construct, start: date, as_of: date) -> list[SignalRow]`:
  - joins `prescreen_day_slots` to `signal_outcomes` and to `agent_calls` (`prompt_version`, `model`) on
    `analyst_call_id`;
  - for `rule_blocked`, reads `brk20_regime_blocked` in place of the journal;
  - keeps `d <= as_of`; a row with `fill_d > as_of` reads as unfilled; one with `exit_d > as_of` reads as `open`.
- `kill_looks(rows, sizes=(20, 40)) -> list[Look]`, where
  `Look(size, formed, n_closed, n_void, median_net_t20, hit_rate, mean_net_t20, median_excess_t20)`.
- `kill_verdict(looks, *, registered_from: date | None) -> tuple[str, str]`, returning DEMOTE, HOLD,
  INSUFFICIENT or UNREGISTERED.
- `analyst_cells(rows) -> list[Cell]`, where `Cell(d, strategy_id, n_enter, n_decline, final, diff)`.
  `diff = mean(enter net_t20) − mean(no_action net_t20)` over the closed rows. It is None when an arm has none.
- `analyst_looks(cells, sizes=(30, 60)) -> list[CellLook]` and `analyst_verdict(looks, *, registered_from) -> tuple[str, str]`,
  returning ADVISORY, KEEP, INSUFFICIENT or UNREGISTERED.
- `registered_keys(rows) -> dict[str, tuple[str, str]]`: per strategy, the (prompt_version, model) of its first
  decision on or after R. Rows with another key are excluded and counted.
- Diagnostics: `confidence_buckets(rows)` and `c2_arms(rule_rows, blocked_rows)`.

```python
def kill_looks(rows, sizes=(20, 40)):
    # FIXED cohort by fill order (memory pinned-looks-cut-from-fixed-cohort): membership is decided by
    # (fill_d, symbol, d) among FILLED rows, never by finishing order; a look forms only when every member
    # is final. A void is final but carries no return.
    filled = sorted((r for r in rows if r.fill_d is not None), key=lambda r: (r.fill_d, r.symbol, r.d))
    ...
# analyst_looks: cells exist from DECISIONS (>=1 enter and >=1 no_action decided that day for that
# strategy), ordered by (d, strategy_id); a cell is final when every decided row in it is final
# (closed/void_ca/unscorable); look k = the first k cells, formed when all k are final; the statistic is
# the mean diff over its usable cells (both arms with a closed outcome), with unusable cells reported.
```

- [ ] **Step 1: failing tests:**
  - `test_late_finisher_never_reorders_a_look`
  - `test_look_waits_for_every_member`
  - `test_void_is_final_and_excluded`
  - `test_demote_at_median_exactly_zero`
  - `test_hold_above_zero`
  - `test_look2_supersedes_look1`
  - `test_insufficient_below_20_filled`
  - `test_unregistered_without_registered_from`
  - `test_cell_membership_from_decisions_not_outcomes`
  - `test_cell_waits_for_its_late_row`
  - `test_unusable_cell_reported_not_averaged`
  - `test_advisory_at_mean_exactly_zero`
  - `test_other_prompt_or_model_excluded_and_counted`
  - `test_as_of_bounds_d_fill_and_exit`
- [ ] **Step 2: implement.** Pure functions; no I/O outside `load_rows`; no clock.
- [ ] **Step 3: the CLI.** `scripts/forward_verdicts.py --rule {brk20,ins,analyst} [--db data/state.db] [--as-of YYYY-MM-DD] [--json PATH]`.
  - It is read-only (`mode=ro`) and always exits 0 (`hi52_forward_verdict.py:84-86`).
  - It prints the verdict line, then look progress (`look 1: 13 of 20 formed`), then the labelled diagnostics:
    T+5/T+10, hit rate, excess, fill rate, the C2 arms for brk20, confidence buckets and the excluded keys.
  - Its docstring quotes THE RULE from "Pre-registration texts".
- [ ] **Step 4: run to PASS; commit.**

---

## Phase 2: changes that must land before R

### T5. One index-regime predicate; C2 applied to brk20; the blocked arm recorded (§9 #1, D-A, D-B)

**Why:**
- C2 came out `supported` for brk20 (IMPLEMENTATION_PLAN §3) and is not applied yet.
- rsi2 carries the predicate twice (`rsi2.py:71-82`, `:140-148`). A third live copy would drift.
- The C2 script keeps its own copy. It is a completed registered study and stays byte-identical.

**Files:**
- Create: `src/engine/strategy/regime.py`.
- Modify:
  - `src/engine/strategy/scanners/rsi2.py`;
  - `src/engine/core/config.py`: `Brk20Cfg.regime_filter: bool = False`, documented as the rollback line;
  - `config/settings.yaml`: `brk20.regime_filter: true`, with a comment citing C2 2026-10-07, the contradicting
    2026 held-out slice and D-A;
  - `src/engine/ops/main.py`: the window sweep at `:2136-2152`, the worker return at `:2320`, and the loop-thread
    write beside `:2365-2367`.
- Test:
  - `tests/unit/test_regime.py`;
  - `tests/unit/test_brk20_regime_gate.py`;
  - the existing rsi2, prescreen, retest and scan-sweep suites;
  - `tests/offline/test_research_filters.py` (the C2 predicate tests live at `:46-62`).

**Interfaces** (T6 consumes these):
- `INDEX_SYMBOL = "NIFTY 50"`, `INDEX_SMA = 50`, `RISING_LOOKBACK = 20`.
- `@dataclass(frozen=True) IndexRegime(close: float, sma: float, sma_then: float, last_d: date)`, with:
  - `up -> bool`: strict `close > sma > sma_then`;
  - `blocked_leg -> str | None`: `"close_below_sma50"` | `"sma50_not_rising"` | None.
- `index_regime(closes: Sequence[float], last_d: date) -> IndexRegime | None`. It returns None below 70 closes
  or on NaN.
- In `main.py`:
  `_brk20_regime_gate(enabled: bool, frame, expected_last: date) -> tuple[bool, IndexRegime | None, str | None]`,
  returning (keep, regime, reason).
  - `reason` is `"down"`, `"undefined"` or `"index_bar_missing"`.
  - The index bar must be dated `expected_last = calendar.previous_trading_day(today)`, matching C2's "no bar
    dated y ⇒ undefined" (`backtest_regime_filter.py:52-55`).

- [ ] **Step 1: failing tests:**
  - `test_up_when_close_above_rising_sma`
  - `test_blocked_leg_close_below_sma50`
  - `test_close_equal_sma_is_not_up`
  - `test_flat_sma_is_not_up`
  - `test_insufficient_history_is_none` (69 closes)
  - `test_nan_is_none`
  - `test_gate_disabled_keeps_everything`
  - `test_gate_blocks_when_down_undefined_or_index_bar_stale`
  - `test_gate_read_failure_blocks_brk20_only_and_never_raises`
  - `test_blocked_crossings_written_on_loop_thread_in_window_only`
  - (offline) `test_live_predicate_agrees_with_c2_regime_on_non_nan_series`
- [ ] **Step 2: implement `regime.py`.** Point rsi2 at it; behaviour and its `rsi2_regime_blocked` fields stay
  unchanged. Leave `scripts/backtest_regime_filter.py` untouched.
- [ ] **Step 3: wire the sweep.**
  - In `_collect_and_scan`, when the filter is on and inside a try/except:
    1. read `store.get_bars_1d_frame(regime.INDEX_SYMBOL, today − 150 days, yesterday)`;
    2. apply the gate;
    3. on block, log `brk20_regime_blocked`
       (`d, reason, close, sma50, rising, blocked_leg, blocked=len(brk20_raw)`), move `brk20_raw` into the
       returned `blocked` list, and set `brk20_raw = []`;
    4. if anything raises, block brk20 with `reason="undefined"`, log it, and let hi52, ins, cat and
       cat_reversal run on.
  - Add `regime_up` to `brk20_sweep`.
  - On the loop thread after the worker returns, and only when `batch_in_window`, `INSERT OR IGNORE` the blocked
    crossings into `brk20_regime_blocked`.
  - Blocked crossings never rest, since only admitted ones do.
- [ ] **Step 4: fix the sweep tests.** The pinned config now carries `regime_filter: true`. Tests that drive the
  sweep and expect brk20 must either seed ≥70 rising `NIFTY 50` closes dated through the previous session, or
  build settings with `regime_filter=False`. Find them with `grep brk20_sweep|_scan_sweep tests/`.
- [ ] **Step 5: run to PASS; commit.**

### T5b. Align the live brk20 retest window to V2-5 (D-M)

**Why:** V2-5 rests the level on y+1..y+5 (`backtest_brk20.py:620-621`). The live book publishes on
`signal_d` = y+1 (`retest.py:182-183`) and sets `expires_d` five sessions later (`:474-483`). It therefore offers
y+1..y+6.

- [ ] **Step 1: failing test** `test_expiry_is_n_minus_1_sessions_after_signal_d`. Cases:
  - N=5 gives signal_d + 4 sessions, across a weekend and a holiday;
  - N=1 gives signal_d (no re-offer);
  - the due window `signal_d < today <= expires_d` offers exactly y+2..y+5.
- [ ] **Step 2: implement.**
  - `_expiry` walks `max(self._sessions, 1) - 1` trading days.
  - Fix the comments that call `signal_d` the crossing session: `retest.py`, the `retest_sessions` comment in
    `settings.yaml`, the `Brk20Cfg` docstring, and the brk20 contract's "up to 5 sessions earlier" line in
    `contracts.py:74` (that last edit changes the prompt, so it lands before R).
  - Leave migration 0014's text as history.
  - Rows already resting at the deploy keep their stored `expires_d`. They age out within a week.
  - If T2's golden file renders a brk20 candidate, regenerate it: the contract text is part of `template`.
- [ ] **Step 3: run** `tests/unit/test_retest.py` **to PASS; commit.**

### T6. Retire the heartbeat; a dated, deterministic regime line (§9 #7, D-G)

**Why:**
- 517 heartbeat calls (~$54) fed a model-written note back to the same agent.
- Candidate no_actions wrote it too (`pipeline.py:2286-2288`).
- The facts T5 computes serve the analyst and the planner at no cost.

**Files:**
- Modify:
  - `src/engine/ops/pipeline.py`: delete `heartbeat()` (`:3106-3137`) and the `set_regime_note` call
    (`:2287-2288`); fix the docstring (`:21-22`).
  - `src/engine/ops/main.py`: delete `heartbeat_tick`/`hb_holder` (`:1905-1920`), the scheduler entry
    (`:3001-3004`) and the plumbing.
  - `src/engine/intelligence/governor.py`: delete `heartbeat_interval_min` (`:435-446`), the heartbeat clause in
    `can_invoke` (`:430-431`), and `DEGRADED_HEARTBEAT_MIN` if it is otherwise unused.
  - `src/engine/intelligence/context.py`:
    - delete `set_regime_note`, `_regime_note*`, `REGIME_NOTE_MAX_CHARS`, `for_heartbeat` and
      `_regime_note_age_text`, plus its call sites;
    - replace the note line in `_stable_block` (`:371-378`);
    - add the same line to `for_planner`'s stable block (`:310`);
    - bump `PROMPT_TEMPLATE_VERSION` and its golden file (T2).
  - `src/engine/intelligence/schemas.py`: delete `"regime_note"` from `intraday_guidance_json_schema` (`:236`),
    `NoActionOutput.regime_note` (`:168`) and the heartbeat sentence in its docstring.
  - `config/agents.yaml`: delete `heartbeat_min`; set `triggers: [signal_candidate, position_event]`.
  - Owner copy that would become false:
    - `dashboard/src/copy/glossary.ts:97-104` (the DG1/DG2 "check-ins" sentences);
    - `dashboard/src/lib/alerts.ts:278`.
  - `scripts/replay_agents.py`:
    - add the src shim (`no_stop_verdict.py:47-49`);
    - `sample --intraday` keeps only signal-candidate prompts (the prompt contains `candidate: {`), because
      `classify()` (`:85-92`) also takes heartbeats.
- Test:
  - Rewrite `tests/chaos/test_case05_llm_failure.py:418-440` with `for_position_event`, keeping its R8
    audit-row assertion.
  - `tests/unit/test_agent_harness.py:986-1005` (the `DG2_heartbeats_off` case).
  - `tests/unit/test_budget_governor.py`.
  - The heartbeat cases in `test_reco_pipeline.py` and `test_context_assembler.py`.
  - Glossary or alerts tests in `dashboard/tests` if they pin the copy.

**Interfaces:**
- `ContextAssembler._regime_text(d) -> str`. It is cached under the key (d, last index date, last EW date): a
  morning catch-up that writes the missing EW row changes the line once, and otherwise the stable block stays
  byte-identical through the day (D8). The renderings:
  - up: `index regime (deterministic, completed sessions through 2026-10-28): NIFTY 50 close 25100.00 above SMA50 24500.00; SMA50 rising over 20 sessions`
  - down: `... close 24100.00 below SMA50 24500.00; SMA50 not rising over 20 sessions`
  - at: `close 24500.00 at SMA50 24500.00`
  - no index bar for the previous session: `index regime: unavailable (no NIFTY 50 bar for <prev session>)`
  - fewer than 70 closes: `index regime: unavailable (<n> of 70 NIFTY 50 sessions)`
  - read failure (warn once): `index regime: unavailable (index read failed)`
  - EW part: `; core equal-weight return over the 20 sessions through <last EW date>: -3.42%`. When the window
    does not end at the previous session, or has fewer than 20 non-NULL rows, it reads
    `; core equal-weight 20-session return: unavailable (<reason>)`.

- [ ] **Step 1: failing tests:**
  - `test_regime_line_renderings`, covering every form above;
  - `test_ew_line_stale_after_missed_evening_is_unavailable`;
  - `test_stable_block_changes_only_when_inputs_advance`;
  - `test_guidance_schema_has_no_regime_note`;
  - `test_no_heartbeat_job_or_call_class`;
  - the rewritten chaos and harness cases.
- [ ] **Step 2: implement.** Then grep for leftovers: `heartbeat` in `src/engine/{ops/pipeline,ops/main,intelligence}`
  and `regime_note|check-in` in `src/` and `dashboard/src`. Keep `ops/heartbeat.py`: the liveness writer is
  unrelated.
- [ ] **Step 3: validate `agents.yaml`** with COMMANDS' `load_agent_roster` one-liner: `quarantined={}`.
- [ ] **Step 4: run** the touched unit, chaos and dashboard tests **to PASS; commit.**
- [ ] **Step 5 (outside market hours, before T20): the replay smoke.**
  - From the worktree, with `$env:PYTHONPATH="<worktree>\src"`, run `.\.venv\Scripts\python.exe scripts\replay_agents.py sample --intraday 3 --preopen 2`
    and then `run --baseline`.
  - First assert that `engine.__file__` lies under the worktree.
  - Expected: 0 `schema_invalid` from the new parsers (T6 and T8).
  - Record the run in the WORKLOG (memory `measure-model-output-before-filtering`).

### T7. Absent is not empty: markers and prompt-integrity tests (A2)

**Why:** each of these sections reads as "no news" when the data is actually missing, yet rule 14 tells the
analyst absent news is neutral. That only works if absence is labelled.
- `_catalyst_text` prints "watchlist entry: none for this symbol today" even when the 08:35 digest never ran
  (`context.py:564-570`).
- The planner's watchlist and earnings sections print "none" for a job that never ran
  (`preopen_planner.py:332-344`).
- The breakout section prints "none" when yesterday's bars are missing (`:256-301`).
- An empty headroom list leaves a header with nothing under it (`context.py:210-211`).

**Files:**
- `src/engine/intelligence/context.py`:
  - add `digest_ran_fn: Callable[[date], bool] | None = None`; unwired means the old wording, for tests and
    replay;
  - an empty headroom list renders `  - none`.
- `src/engine/ops/preopen_planner.py`: add `last_success: Callable[[str], date | None] | None = None`.
- `src/engine/ops/main.py`: wire both through late-binding lambdas over `catch_up.last_success_date`
  (`jobs.py:385-390`), as at `main.py:1629-1631`.
  - Not `was_run`: it counts a terminal give-up (`skipped`) as resolved (`jobs.py:366-373`).
- Create `tests/unit/test_prompt_integrity.py`.

- [ ] **Step 1: failing table-driven tests.** For each section, three renderings: source absent →
  `unavailable (<reason>)`; present but empty → an explicit `none ...`; present → the value.
  - `for_signal`: features, bars, swing daily evidence, session aggregates, the catalyst watchlist (digest not
    run → `watchlist entry: unavailable (today's catalyst digest has not run)`), sentiment for symbol, sector
    and market, risk headroom, last price, day plan, index regime.
  - `for_planner`:
    - movers;
    - breakouts: no prior-session bars → `unavailable (no completed daily bars for <d1>)`;
    - digest;
    - watchlist: `unavailable (catalyst digest has not run for <d>)`;
    - earnings: the last success is older than the 3rd session before d, or never →
      `unavailable (earnings calendar not refreshed since <date or never>)`;
    - surveillance, positions, review summary.
  - `for_news_batch`: an empty theme vocabulary renders `none`.
  - The guard: in an all-absent context, no line ends in `:` without content beneath it.
- [ ] **Step 2: implement.** A failed read renders `unavailable` and never raises. Bump `PROMPT_TEMPLATE_VERSION`
  and its golden file.
- [ ] **Step 3: run** `test_prompt_integrity.py test_context_assembler.py test_preopen_planner*.py` **to PASS;
  commit.**

### T8. Planner: drop `no_trade_today` (§9 #7, D-H)

**Files:** `src/engine/intelligence/schemas.py` (`DayPlan`, `:439-449`), `src/engine/intelligence/agents/preopen.py`
(rules 8–9, `:54-60`), and the docstring at `src/engine/intelligence/context.py:308`. The tests that build DayPlan
with the field are `tests/unit/test_agent_defs.py:224,279,288` and `tests/unit/test_preopen_planner.py:105`.

- [ ] **Step 1: write `test_dayplan_has_no_no_trade_today`**: the field must be absent from
  `DayPlan.model_json_schema()["properties"]`. Update the four tests above.
- [ ] **Step 2: implement.**
  - Rule 8 becomes: "If MARKET conditions genuinely do not support trading, say so in the regime read and the
    warnings. A quiet day is a legitimate plan, and an empty focus list beats a padded one."
  - Rule 9 starts: "Never warn about platform status: ...".
  - Stored `day_plans` rows are never re-parsed through `DayPlan`; only `preopen.parse_output` parses.
- [ ] **Step 3: run to PASS; commit.** T6's replay smoke covers `--preopen 2`.

### T9. Park trend (§9 #7, D-I)

- [ ] **Step 1:** set `trend: 0` in `config/settings.yaml` (`:410`) with this comment: parked 2026-11 (review
  §9 #7); 34 forwards since August; no validated edge; re-enable only behind a pre-registered hypothesis, on the
  orb park's terms.
- [ ] **Step 2: run** `tests/unit/test_prescreen.py` and the settings-loading tests **to PASS; commit.**
- **Verification:** the scanner keeps running, but `prescreen_strategy_disabled strategy_id=trend` is logged only
  on a day trend actually fires (`prescreen.py:1080-1090`). The deploy check is the loaded settings value; the log
  line confirms it on the first firing day.

### T10. `analyst_confidence_min` becomes optional; prepare the owner patch (§9 #3, D-F)

**Files:** `src/engine/risk/limits.py:259`, `src/engine/risk/gate.py:619-627`, `src/engine/api/app.py:783-793`.
Tests: `test_risk_gate.py`, `test_limits_engine.py`, `test_api_routes.py`.

- [ ] **Step 1: failing tests:**
  - `test_table_without_analyst_confidence_min_loads`.
  - `test_confidence_rule_passes_na_without_floor`: value `n/a`, limit `no floor configured (review §9 #3)`,
    passed. The rule id stays in `DOCUMENTED_ENTER_RULES`.
  - `test_floor_still_binds_when_set`.
  - `test_params_omit_absent_floor`. The POST 409 stays.
  - Make every floor test independent of the live file:
    - conftest copies the LIVE `config/limits.yaml` (`tests/conftest.py:36-55`);
    - these read it and assert 0.55: `test_risk_gate.py:54,75,268-270`, `test_limits_engine.py:108,173` and
      `test_api_routes.py:589`;
    - give them an explicit floor through `table.model_copy(update={"analyst_confidence_min": 0.55})` or a
      fixture file;
    - run the suite once against a scratch copy without the key (`MT_PIN_SOURCE`, `tests/conftest.py:40-47`).
- [ ] **Step 2: implement** `analyst_confidence_min: float | None = None`.
- [ ] **Step 3: create `runbooks/patches/limits-drop-analyst-confidence-min.patch`** (removes `limits.yaml:183-184`
  and the sentence on line 10):
  1. edit `config/limits.yaml` in the worktree;
  2. `git diff -- config/limits.yaml > runbooks/patches/limits-drop-analyst-confidence-min.patch`;
  3. `git checkout -- config/limits.yaml`;
  4. run `git apply --check` from the live root.

  Never apply it. Its hunks avoid the capital lines, so a later dashboard capital change does not break it.
  `config/envelope.yaml:62` mentions the key only in a comment; leave it.
- [ ] **Step 4: run to PASS; commit.**

---

## Phase 3: the owner channel

### T11. Say what the cost multiple is (§9 #4 label)

**Why:** the card prints `edge {edge_multiple}x` (`catalog.py:1192-1195`). For a target-priced rec that is the
distance to the analyst's target over breakeven, with no hit probability; on exit and adjust recs it reads
`edge 0x`.

- [ ] **Step 1: failing tests:**
  - `test_cost_line_registered_edge`: entry with `registered_edge_pct` →
    `cost: breakeven B% · total ₹T · edge Nx breakeven`.
  - `test_cost_line_target_priced`: entry with targets →
    `cost: breakeven B% · total ₹T · target distance D% = Nx breakeven (earned only if the target is hit; not an expected edge)`,
    where D = `rec.cost.expected_edge_pct`.
  - `test_cost_line_exit_has_no_edge_clause`.
- [ ] **Step 2: implement** `_cost_line(rec: Recommendation) -> str`. **Run to PASS; commit.**

### T12. Same-ticker history on the card (B3)

**Files:** `src/engine/ops/scorecard.py` (an owner-surface reader under the hindsight rule) and
`src/engine/ops/pipeline.py` (`_fill_extras`, `:2697-2733`).

**Interfaces:** `symbol_history_line(conn, symbol: str, *, limit: int = 3) -> str`.

- [ ] **Step 1: failing tests:**
  - The line format: `same-ticker history (hindsight): 2026-09-10 brk20 closed -4.12% (stop); 2026-08-27 hi52 open`.
    It shows up to 3 earlier ENTRY recs, newest first.
  - Status words: closed shows net and exit reason; then `open`, `did not fill`, `voided (corporate action)`, and
    `not scored` for anything else.
  - With no earlier recs: `same-ticker history: no earlier recommendation on <SYMBOL>`.
  - It runs before the hi52-only block. A read failure is logged and the card still ships.
- [ ] **Step 2: implement.**
  - Query `recommendations` ⋈ `learning_ledger` ⟕ `rec_outcomes` on `json_extract(payload,'$.instrument')`,
    kind entry.
  - Put the line first in `extras.evidence`; evidence is the card's elastic budget (`catalog.py:1202-1212`).
- [ ] **Step 3: run to PASS; commit.**

### T13. One-tap skip reasons (§9 #9, D-K)

**Why:**
- `skip_reason` is NULL on all 112 recs.
- `CatalogMessage.reply_keyboard` was never rendered by any transport (`catalog.py:14-17`, `:256`, `:1240-1245`).
- The expired-entry branch of `veto()` overwrites an existing reason (`pipeline.py:651-658`), and a rec_id
  bypasses the typed path's filter (`telegram.py:1428`).

**Files:**
- Create `src/engine/core/migrations/0021_notification_buttons.sql`:
  `ALTER TABLE notifications ADD COLUMN buttons TEXT;  -- JSON button rows, re-attached by the outbox drainer`.
- Modify:
  - `src/engine/notify/catalog.py`:
    - rename `reply_keyboard` to `buttons` (INLINE buttons whose `command` runs on tap);
    - entry cards get six veto buttons in place of the `/taken` prefill, which cannot be one tap because a fill
      needs a price;
    - `rec_decision_reminder` gets per-rec rows;
    - `rec_fill_suspected` keeps its rows (`:462-466`, no production caller); `taken` joins the allow-list only
      when that caller lands.
  - `src/engine/notify/telegram.py`.
  - `src/engine/ops/pipeline.py` (`veto`).
  - The leak tests listing `"reply_keyboard="` switch to `"buttons="`: `test_market_store.py:927`,
    `test_notify_owner.py:26`, `test_telegram_commands.py:1283,1566`.

**Interfaces:**
- Buttons are `{"text": label, "command": "/veto <rec_id> <reason>"}`. A command is ≤64 bytes and doubles as
  the callback data.
- `_BUTTON_COMMANDS = frozenset({"veto"})`.

- [ ] **Step 1: failing tests:**
  - `test_entry_card_has_six_veto_buttons_by_rec_id`: rows of 3; labels `skip: market` … `skip: other`; every
    command ≤64 bytes.
  - `test_exit_card_has_no_buttons`.
  - `test_reminder_rows_label_the_symbol_and_cap_at_4_recs`.
  - `test_send_journals_buttons_and_attaches_markup_to_last_part_only`.
  - `test_drainer_retry_reattaches_buttons`.
  - `test_veto_refuses_a_second_reason`: the book raises `ValueError("reason already recorded: market")`, and
    the typed path behaves the same.
  - `test_owner_tap_vetoes_once_and_removes_only_that_recs_row`: `book.veto(rec_id, "market")` is called once;
    the markup is rebuilt without that rec's row; `answer()` is called.
  - `test_refused_tap_answers_with_alert_and_keeps_markup`.
  - `test_edit_failure_still_answers`.
  - `test_non_owner_tap_ignored_and_logged`.
  - `test_unlisted_or_malformed_command_refused`.
  - The migration upgrade test.
- [ ] **Step 2: implement.**
  - In `veto()`, the expired-entry branch refuses when `skip_reason` is already set.
  - `_journal_insert` stores `json.dumps(msg.buttons)`. `_deliver(text, notification_id, buttons=None)` and
    `_send_text(app, text, reply_markup=None)` put `InlineKeyboardMarkup` on the last part only.
  - `_drain_outbox_once` selects and forwards `buttons`.
  - `CallbackQueryHandler(_owner_only(self, self._on_button))`, where `_owner_only` checks
    `update.effective_chat.id` (`telegram.py:1875-1902`).
  - `_on_button` parses the command and checks the allow-list, then calls the book:
    - on success, it rebuilds the markup without that rec's row, inside try/except;
    - on a `ValueError`, it answers with `show_alert=True`;
    - either way, it always calls `answer()`.
  - The typed `/veto` footer stays as the fallback.
- [ ] **Step 3: run** `test_telegram_commands.py test_notifications_journal.py test_notify_owner.py test_market_store.py test_reco_pipeline.py test_migration_0021.py`
  **to PASS; commit.**
- **RUNBOOK (T19):** a tap made while the engine is down is dropped at the next start
  (`drop_pending_updates=True`, `telegram.py:581`). Re-tap, or type `/veto`.

### T14. Time exits skip positions sold outside the ledger (§9 #9)

- [ ] **Step 1: failing test** `test_time_exit_skips_a_position_sold_outside_the_ledger`.
  - The journal shows zero holdings for two sessions, so no rec is issued.
  - One `time_exit_skipped_sold_outside_ledger` line is logged per position per day.
  - A partial holding still exits.
- [ ] **Step 2: implement.** In `time_exit_check` (`pipeline.py:2997-3059`), read `_sold_outside_ledger(d)` once
  per run and skip the positions it names. Use a separate `_time_exit_skip_logged` memo: `_sold_outside_logged`
  (`:1027`) belongs to the position-event path.
- [ ] **Step 3: run to PASS; commit.**

---

## Phase 4: standing numbers

### T15. Scorecard, dashboard card and weekly line (A1)

**Files:**
- Modify:
  - `src/engine/ops/scorecard.py`: `scorecard()` gains `"signals": signals_block(conn)`.
  - `src/engine/ops/weekly_summary.py`: it also waits for `JOB_SIGNAL_OUTCOMES`.
  - `src/engine/notify/catalog.py`: `weekly_summary(..., analyst_line: str | None)`.
  - `dashboard/src/types.ts`, `dashboard/src/views/Performance.tsx`, `dashboard/fixture_server.mjs`.
- Create: `dashboard/src/lib/signals.ts`, `dashboard/src/views/parts/SignalScorecard.tsx`, `dashboard/tests/signals.test.ts`.

**Interfaces:**
- `signals_block(conn) -> dict` never raises. On error it returns `{"error": str}`.
- Its shape is `{"window": "from <R>" | "pre-registration history", "strategies": {sid: {...}}}`, where each
  strategy holds:
  - `published`;
  - `rule`: closed count, median net T+20, hit rate, median excess;
  - `blocked`: brk20 only, the C2 arm diagnostic;
  - `kill_rule`: verdict and progress;
  - `analyst`: enter count, decline count, cells formed, mean diff, verdict.
- hi52's rule row is labelled `diagnostic`. Its costing differs from `scripts/hi52_forward_verdict.py`
  (`:341-351`), whose verdict stays authoritative.
- The weekly line depends on the verdict:
  - `Analyst (pre-registered from R): look 1 formed 12 of 30 cells — interim, decides nothing.`
  - Once formed: `Analyst verdict (look 1, 30 cells): ADVISORY — mean approved − declined at T+20 -0.8 pp.`
  - If unregistered, the line is left out.

- [ ] **Step 1: failing tests:**
  - Python: the block over seeded rows; `UNREGISTERED`; the error path; the weekly wording in all three states;
    the weekly job waiting for both jobs.
  - TS (`lib/signals.ts`): verdict tone, progress text, the em dash for None.
- [ ] **Step 2: implement** a second Card, "Signals and the analyst, scored in hindsight", with the note "Every
  published signal is scored whatever the analyst or you decided. Interim numbers decide nothing; the verdicts are
  pre-registered." Reuse the classes from `Scorecard.tsx`.
- [ ] **Step 3: verify** with `npm test` and `npm run build`. Run the fixture-server check through chrome-devtools
  (memory `dashboard-ui-change-verification`): a 390 px snapshot and `tests/dom-checks.js`. No engine restart.
- [ ] **Step 4: run the Python tests to PASS; commit.**

---

## Phase 5: research governance (offline)

### T16. Research trial ledger and registration bar (§9 #8)

**Why:** `fold_pass_min` steps on one sweep's grid size (`validate.py:105-117`), and `validate_insider` declares
N = 1. Nothing sees the families the program has tried.

**Files:**
- Create: `config/research_trials.yaml` and `src/engine/learning/multiplicity.py`.
- Tests: `tests/unit/test_research_trials.py` (it pins production config, so it runs in the gate) and
  `tests/offline/test_multiplicity.py`.

**Interfaces:**
- `Trial(id, date, family, hypothesis, population, construct, variants: int, result, report)`, where `result` is
  `supported` | `refuted` | `null` | `insufficient` | `registered` | `grandfathered`.
- `load_trials(path=None) -> list[Trial]`.
- `family_trials(trials, family) -> int`.
- `permutation_pvalue(sample, population, *, draws=10_000, seed) -> float`: the share of same-size random
  subsamples (without replacement) whose median ≥ the sample's.
- `registration_alpha(n_trials, alpha=0.05) -> float`: `alpha / max(1, n_trials)`.

- [ ] **Step 1: backfill the ledger.** Route this to `log-digger`; the manager spot-checks pointers.
  - Sources: the memory "Strategy evidence" entries, `git show 417da77:IMPLEMENTATION_PLAN.md` §6.1, and
    `data/reports/*` stems.
  - One entry per family or variant, each citing its report or memory: orb, tdc, candles, ATR-split, fade,
    short-horizon momentum, hvol, trend floor, hi52 v1/v2, brk20 entry mechanism (3), C1 (3), C2 (3), C3, the
    insider legs, results-PEAD.
  - ins and hi52 are `registered` or `grandfathered`.
- [ ] **Step 2: failing tests:**
  - every registered edge in `settings.yaml` has a `registered` or `grandfathered` entry;
  - ids are unique and dates are ISO;
  - `permutation_pvalue` is deterministic per seed, ≈0.5 on a random subsample, and 0 on the top-n subsample;
  - `registration_alpha(2) == 0.025`.
- [ ] **Step 3: implement; run** `tests/unit/test_research_trials.py tests/offline/test_multiplicity.py` **to PASS;
  commit.**
- **The invariant** (T19 puts it in IMPLEMENTATION_PLAN §2). A new `expected_edge_pct`, or a promotion out of
  shadow, must:
  - cite a ledger entry;
  - pass CPCV at `fold_pass_min(N_family)`;
  - have a permutation p < `registration_alpha(N_family)` against same-size random subsamples of its matched
    population.

### T17. Market-wide pause study (§9 #1, D-J)

**Files:** create `scripts/backtest_market_pause.py`. It reuses `backtest_regime_filter`'s `build_population`,
`measure`, `report`, `cli` and `render_text`, as C3 does. Test: `tests/offline/test_backtest_market_pause.py`.

- [ ] **Step 1: failing tests:**
  - the EW rules: |r| > 25% dropped, structural corporate-action days skipped (`benchmark.py:21-56`);
  - EW20 compounds the 20 sessions ending at y, never later;
  - `decide()` boundaries, including p < 0.025 and `fold_pass_min(2) == 0.60`;
  - INSUFFICIENT at n < 30.
- [ ] **Step 2: implement.** The EW series uses the current core list applied backwards, resolved the way
  `research_common.hi52_population` resolves it, and is labelled survivorship-tainted. The trial is recorded in
  T16's ledger as family `regime_filter/<strategy>`.
- [ ] **Step 3: run** the offline test **to PASS; commit.** Tests on synthetic data may run before registration;
  a run on real data may not.
- [ ] **Step 4 (outside market hours, only after T19 has committed the pre-registration):** run on the newest
  `data/research/market_<date>.duckdb` and write `data/reports/market_pause_<date>.*`. Record the verdict in
  memory and IMPLEMENTATION_PLAN §3. A SUPPORTED result goes to the owner as a decision and wires nothing.

### T18. News and planner scorecard (A1)

**Files:** create `scripts/llm_scorecard.py` (with the src shim) and `tests/offline/test_llm_scorecard.py`.

- [ ] **Step 1: failing tests:**
  - materiality tercile × direction buckets;
  - next-open T+5/T+10 excess against the EW over the same window, never raw (the hvol lesson);
  - planner focus bias long vs avoid → T+1/T+5 excess;
  - empty inputs render `none`.
- [ ] **Step 2: implement.**
  - The script is read-only. From state.db it reads `day_plans` and `agent_calls`, the latter for reliability
    by agent, fail reason and week. From a DuckDB export it reads `catalyst_watchlist` and `bars_1d`.
  - It prints the analyst summary through `forward_verdicts`.
  - It writes `data/reports/llm_scorecard_<date>.{json,md}`.
  - Every section is labelled "interim, decides nothing".
- [ ] **Step 3: run to PASS; commit.**

---

## Phase 6: ship

### T19. Plan of record, runbooks, pre-registrations

- [ ] **IMPLEMENTATION_PLAN §3 ("Pending verdicts and owner decisions"):**
  - add the four texts below as entries, with `<R>` left for T20;
  - rewrite the C2 entry: applied on R in the brk20 sweep (D-A), blocked arm journalled;
  - D14 entry:
    - from R, brk20 recs are regime-ON only and use the 5-session retest window;
    - from the owner's patch date, there is no confidence floor;
    - the paired D stays valid;
    - read the slices split at R and at the patch date by hand. `scripts/no_stop_verdict.py` is untouched.
  - add the conditional specs below.
- [ ] **IMPLEMENTATION_PLAN:** the T16 invariant goes in §2; A4 (D-L) and its trigger go in §8 "Deferred".
- [ ] **COMMANDS:** add:
  - "Forward verdicts (brk20, ins, analyst) — read-only, safe with the engine running";
  - a signal-outcomes row count;
  - the protected-config step for T10's patch;
  - "LLM scorecard", replacing the 10-27 section;
  - the T6 replay smoke, with its `PYTHONPATH` check.
- [ ] **RUNBOOK:** add:
  - `brk20_regime_blocked`: its reasons, and the rollback (`regime_filter: false` plus a restart);
  - "the heartbeat is retired";
  - one-tap skip buttons, including taps dropped while the engine is down.
- [ ] **Commit.**

### T20. Review, merge, deploy, register

- [ ] **Step 1 (outside market hours):** a whole-branch multi-angle review: line-by-line, removed-behaviour and a
  cross-file tracer (memory `multi-angle-review-catches-interactions`). Keep a finding-to-owner table and tick
  every row.
- [ ] **Step 2:** run the full gate (`.\.venv\Scripts\python.exe -m pytest -n 8 --dist loadfile -q`), the offline
  tier, `npm test` and T6's replay smoke.
- [ ] **Step 3:**
  1. rebase on `phase3`;
  2. set R in `forward_verdicts.REGISTERED_FROM` and in every `<R>`, then commit;
  3. the owner approves the merge.
- [ ] **Step 4:** deploy per COMMANDS "Deploy a reviewed tip" (backup `pre_0018.db`). The boot must show:
  - migrations 0018–0021 applied;
  - `engine_ready`;
  - no `analyst_heartbeat_tick` job.
- [ ] **Step 5:** `scripts/forward_verdicts.py --rule analyst` prints `INSUFFICIENT` with `look 1: 0 of 30`, not
  `UNREGISTERED`.
- [ ] **Step 6:** check the first evening and window:
  - `signal_outcomes_scored` and its rows;
  - a `brk20_sweep` line with `regime_up` (and, if down, `brk20_regime_blocked` with rows in its table);
  - the Performance card renders.
- [ ] **Step 7:**
  - write the WORKLOG entry;
  - update memory: analyst-filter, TradingAgents review, C2, and the brk20 entry mechanism (the y+6 finding);
  - hand over T10's patch;
  - delete this file once every task has shipped.

---

## Pre-registration texts (into IMPLEMENTATION_PLAN §3 at T19)

**Analyst-value verdict (registered <T19 date>, population from R).**
- **Population:** `prescreen_day_slots` rows of brk20, hi52 and ins with `d >= R` and a recorded first
  `analyst_decision`, whose call carries the strategy's registered key `(prompt_version, model)`. The registered
  key is the one on that strategy's first decision on or after R. Rows with another key are excluded and counted.
  A change to the analyst prompt, a population contract or the model after R therefore stops the population from
  growing. Accrual resumes only through an owner-approved re-registration with a new R.
- **Outcome:** `signal_outcomes` `next_open`:
  - entry at the open of the first regular session after d;
  - exit at the close of the 20th regular session, counting the entry session;
  - net of one CNC round trip at the §7.1 notional at the floor-rung equity, fixed at the row's first scoring.
- **Cells:** a (d, strategy) cell exists once at least one enter and at least one no_action were decided. Cells are
  ordered by (d, strategy_id). A cell is final when all its decided rows are final. Its value is the mean net of
  its closed enters minus the mean net of its closed no_actions. A cell with an arm and no closed outcome is
  reported and not averaged.
- **Looks:** look 1 is the first 30 cells, formed when all 30 are final. Look 2 is the first 60 and supersedes it.
- **Decision:** a formed look's mean ≤ 0 ⇒ **ADVISORY**; > 0 ⇒ **KEEP**. Below that, INSUFFICIENT.
- **Tool:** `scripts/forward_verdicts.py --rule analyst`. Interim readings decide nothing.
- **Diagnostics:** T+5/T+10, per-strategy cells, and confidence buckets.
- **Duration:** at least three months, since brk20 originates only in regime-ON sessions.

**brk20 kill rule (registered <date>, population from R).**
- **Population:** the brk20 crossings admitted on or after R (`brk20_resting_levels` rows), under C2.
- **Construct** (`signal_outcomes` `rule`):
  - entry: a limit resting at the broken level on `signal_d` (y+1, the session after the crossing session y)
    through `signal_d` + 4 sessions (V2-5);
  - fill: on the first regular daily bar whose low touches the level, at min(open, level). Touch fills are
    optimistic, as in C2;
  - exit: at the close of the 20th regular session, counting the fill session (C2's arm (ii); V2-5 itself
    measured close(fill+20)). No stop, no target;
  - voids: a structural ex-date between `signal_d` and the exit voids the crossing;
  - cost: one CNC round trip at the §7.1 notional implied by the signal's own stop at the floor-rung equity,
    fixed at the row's first scoring.
- **Looks:** look 1 is the first 20 filled crossings by (fill date, symbol), formed when all 20 are final. Look 2
  is the first 40 and supersedes it.
- **Verdict:** DEMOTE iff the look's median net over its closed members ≤ 0.
- **Scope:** this rule judges the signal's drift. The card's stop and target are D14/C5's question.
- **Demotion:** add brk20 to `ops.main.NO_EDGE_SHADOW_STRATEGIES` in an owner-visible commit.
- **Diagnostic only:** the C2 blocked arm (`rule_blocked`) against the admitted arm.
- **Tool:** `scripts/forward_verdicts.py --rule brk20`.

**ins kill rule (registered <date>, population from R).**
- **Looks and verdict:** as brk20.
- **Construct:** entry at the open of the publication session d, which is the validated next-session open after
  the disclosure. Exit at the close of the 20th regular session. No stop. Cost at the §7.1 notional for the 5%
  stop, fixed at first scoring.
- **Demotion:** add ins to `NO_EDGE_SHADOW_STRATEGIES`, which wins over its registered edge (`gate.py:1046-1049`).
- **Tool:** `scripts/forward_verdicts.py --rule ins`.

**Market-wide pause study (registered <date>, run before any wiring).**
- **Strategies:** hi52 and ins.
- **Rule:** at signal session y, PAUSE if the core equal-weight return compounded over the 20 sessions ending at y
  is < 0; otherwise TRADE.
- **Population and construct:** C2's populations, construct and cost (`scripts/backtest_regime_filter.py`), on a
  research snapshot.
- **EW series:** the daily mean of close/prev_close − 1 over the current core-eligible list (a
  survivorship-tainted proxy). |r| > 25% and structural corporate-action days are excluded.
- **Family:** per strategy, the regime-filter family is C2 plus this rule, so N = 2.
- **Decision:** SUPPORTED iff all hold:
  - TRADE and PAUSE each have n ≥ 30;
  - TRADE is CPCV-promotable at `fold_pass_min(2)`;
  - TRADE median net > max(0, FULL median);
  - PAUSE median net ≤ 0;
  - the permutation p < 0.025, where p is the share of 10,000 same-size random subsamples of FULL (seed
    20261029) whose median ≥ TRADE's median.

  INSUFFICIENT if a cohort has n < 30; NOT SUPPORTED otherwise. SUPPORTED goes to the owner.

## Conditional specs (into IMPLEMENTATION_PLAN §3 at T19; build only on their trigger)

- **ADVISORY build** (trigger: the analyst verdict reads ADVISORY).
  - For hi52 and ins, an analyst no_action or a failed call no longer ends the candidate.
  - The pipeline builds the proposal deterministically and the gate judges it unchanged. The proposal:
    - enters at MARKET;
    - takes the rule's stop re-anchored at the live mark (`_reanchored_stop`, D3);
    - has no target;
    - sizes at `max_qty_by_risk`;
    - carries `agent_id = PLATFORM_AGENT_ID`, with the analyst's reason as advice in the thesis.
  - brk20 is unaffected (no registered edge). The build gets its own plan.
- **#4 measured-edge pricing** (trigger: C5 look 1, a POOLED label over hi52, ins and brk20,
  `no_stop_verdict.py:8-19`).
  - A brk20-specific change needs its own pre-registered rule, entered in the T16 ledger as a new trial. The T16
    bar applies; brk20 is not grandfathered.
  - If that rule supports time-only brk20:
    - brk20 ships time-only T+20;
    - its registered edge is re-measured on the shipped construct (20 sessions counting the fill, not V2-5's
      close(fill+20)) minus the floor-rung cost floor;
    - the stop becomes a disaster stop used for sizing only.
  - rsi2 and trend have no measured edge for their shipped constructs. They are parked or shadowed, never priced
    from an analyst target.
- **B1 study** (trigger: the analyst verdict reads KEEP).
  1. Classify the no_action reasons via `analyst_call_id`.
  2. Pre-register deterministic screens for the mechanical classes ("last price below the broken level", "a buy
     limit there would be marketable").
  3. Ship a screen only if its cohort did worse than same-day approved neighbours.

## Not planned, with reasons

- **The nightly reviewer's unread fields:** they cost $12 over four months, no prompt reads them, and its model
  is the owner's 2026-08-18 call.
- **Analyst calls on `cat` and `cat_reversal`:** about 0.5 a day, and the shadow verdicts measure published slots
  rather than the analyst.
- **Sizing taken from the analyst:** the gate already caps quantity and only shrinks it (R1). Moving size out of
  Tier 1 is an architecture call the report did not ask for.
- **`learning_ledger` mae/mfe/regime_label and champion/challenger:** Phase 5 (IMPLEMENTATION_PLAN §7).

## Review record (builder check, 2026-10-29)

An independent Opus reviewer checked T1–T20 against the code and returned 21 findings.

**Adopted, and where:**
- F1 (the analyst verdict had no fixed cohort): T4 and the pre-registration.
- F2 (muhurat and backfill-moved fills): T3.
- F3 (V2-5 window and horizon): D-C, T3, T5b.
- F4 (cost frozen): T3.
- F5 (template and model drift): T2, T4.
- F6 (stale index bar, sweep isolation): T5.
- F7 (EW staleness, renderings): T6.
- F8 (veto idempotency and markup): T13.
- F9 (replay imported the live tree): T6.
- F10 (C5 label is pooled): conditional #4.
- F11 (blocked arm unmeasured): D-B, T3, T5.
- F12–F19 (missed tests and consumers): T1, T3, T4, T5, T6, T8, T10, T14, T15.
- F20 (A4 premise wrong): D-L.
- F21 (docs, verification, N counts C2): T9, T19, D-J.

**Declined:** merging T12 into T15. A reviewer could accept the card change and reject the dashboard change, so
they stay separate tasks.
