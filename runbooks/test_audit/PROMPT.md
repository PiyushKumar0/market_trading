# TEST_AUDIT_PROMPT.md — Test-Suite Audit, Pruning & Speed-up

<!--
OPERATOR NOTES (for Piyush — not part of the agent prompt)

How to run: start a FRESH Claude Code session at the repo root (manager model per CLAUDE.md) and say:
    Read TEST_AUDIT_PROMPT.md and execute it end to end.
It is a multi-session job. To resume after a session ends:
    Read TEST_AUDIT_PROMPT.md and resume from runbooks/test_audit/STATE.md on wip/test-audit.

When: the inventory, batch and review phases fan out agents on the subscription quota the engine's
analysts share. Start on an evening or a weekend; on a trading day the agent asks before any fan-out
between 09:15 and 15:30 IST.

Output: commits on branch wip/test-audit in the worktree ..\mt_testaudit (the agent never merges or
pushes), plus runbooks/test_audit/REPORT.md and LEDGER.md on that branch. Read REPORT.md's "Owner
decisions" first, then say "merge" when satisfied.

Decided in this prompt (edit before running if you disagree):
  - Production code (src/, scripts/, config/, web/, dashboard/src) is read-only. Production
    changes that would help come back as proposals.
  - The live .venv is never touched. The worktree builds its own venv, and pytest-xdist may join
    the dev extra on the branch; you sync the live venv after merging.
  - Tests of closed research studies and finished one-off scripts shrink only with your OK.

Grounded in a measurement of phase3 @ 3e6b811 on 2026-10-18 01:17 IST (live tree, live venv; numbers
in §2). If the tree has moved on, trust the grep, not the number.

═══════════════ EVERYTHING BELOW THIS LINE IS THE PROMPT ═══════════════
-->

## 0. Mission

You are auditing, then rewriting, the test suite of a personal NSE trading engine (Python 3.12, pytest, Windows). The live engine runs from this repository's working tree. The owner's request, verbatim in spirit:

> Audit, review, revise and remove test coverage. Remove test cases that are not required, redundant, excessive, or simply do not make sense. Then optimise the tests — parameterise them, remove unwanted asserts and patching — and speed up the whole suite run.

The work is done when three things exist:
1. A branch, `wip/test-audit`, whose suite still protects every behavior worth protecting that today's suite protects, in less wall-clock time and fewer lines.
2. A ledger that makes every removal checkable: what went, why, and what still catches the defect it was written for.
3. A report with before/after measurements and the decisions only the owner can make.

The measure is protection per second of runtime, not test count. Don't delete toward a number. Each deletion claims that nothing of value was lost, and the ledger exists so the claim can be checked. Where the evidence says keep, keep it: "audited, sound as-is" is a real result.

Before substantive work, read `CLAUDE.md` and `FABLE_HANDOFF.md` (§1, §5, §6 and §10 in full). FABLE_HANDOFF §3's boundary checklist and §4's review priorities tell you which tests deserve the most scrutiny.

## 1. Ground rules

1. **Production code is read-only.** You may edit:
   - `tests/**` and the root `conftest.py`;
   - `pyproject.toml`: only the `[tool.pytest.ini_options]` table and the `dev` extra;
   - `uv.lock`, only through `uv`;
   - `dashboard/tests/**`;
   - the §7 artefacts.

   Why: the engine boots from the working tree (NSSM restarts it 15 s after a crash, with whatever is on disk), and test-only changes are the one kind that can't reach it. If a test can only be fixed or sped up through a production change (a seam, an injectable wait, a lazy import), write it up as a proposal.
2. **Work in a worktree with its own venv; never edit the live tree.** Fault injection (rule 4) edits `src/` temporarily. In the live tree, a crash-restart during that window would boot the mutant. Setup is in §6, Phase 0.
3. **Never modify the live `.venv`.** It is the running engine's interpreter.
   - Only you (the manager) run `uv`, only inside the worktree, and only after checking three things: the worktree's `.venv` is a real directory, not a junction; `UV_PROJECT_ENVIRONMENT` is unset; `VIRTUAL_ENV` is unset.
   - Memory `worktree-venv-imports-live-tree` forbids uv in worktrees because past worktrees junctioned the live venv. A real venv is the one condition under which uv is safe there.
   - Audit-only tools (pytest-cov) go into the worktree venv with `uv pip install --python .\.venv\Scripts\python.exe …`, never into the lock. Without `--python`, `uv pip` follows `VIRTUAL_ENV`.
   - A dependency the suite will keep (pytest-xdist) goes into the `dev` extra on the branch, via `uv add --optional dev …` in the worktree.
   - The owner syncs the live venv only after merging, so the plain `pytest` command must keep working without the new dependency: no `-n` in `addopts`.
4. **Removal needs evidence, scaled to what the test protects** (§3 tiers). "Probably covered elsewhere" is not evidence. For Tier B:
   1. Inject the fault the removed test was written to catch into the worktree's `src/`.
   2. Watch the surviving test fail, and paste the failure into the ledger.
   3. Revert.

   A survivor you never saw fail proves nothing (FABLE_HANDOFF §5).
5. **Speed comes from removing waste, never from testing less of the same thing.** Don't lower hypothesis `max_examples`, shrink a replay corpus, drop the second run of a two-runs-one-digest check, or loosen a timeout an assertion depends on. Fair game: shared immutable setup, template databases, fake time, parallelism, and deleting redundant tests.
6. **Run tests inline and one suite at a time.** Never background a run, and paste the real output. Use the worktree's `.\.venv\Scripts\python.exe -m pytest`, never `uv run`. Use PowerShell, not Bash. Concurrent suites cause spurious failures in timing-sensitive tests (`test_notifications_journal.py::test_drain_loop_survives_a_bad_pass`, the catalyst-digest tests), so record timings only from runs with nothing else running. The one exception is the deliberate parallelism experiment in §5.
7. **Fan-outs:**
   - Run `Get-Date` before every wave.
   - On a trading day, launch no agents between 09:15 and 15:30 IST unless the owner OKs it for that day (CLAUDE.md, Operations). Ask; the answer holds for one day only.
   - Inline work is fine at any time.
   - Keep waves to ≤4 concurrent agents: the engine draws on the same quota.
8. **Off limits:**
   - `mt-engine`;
   - `data/` (the live `state.db` and `market.duckdb`);
   - `config/limits.yaml` and `config/envelope.yaml`. A dirty `limits.yaml` in the live tree is the owner's signed capital change: never stash, restore or commit it.

   Tests stay off the network.
9. **Git:** commit on `wip/test-audit` only, with `git commit -F <message file>` and no co-author trailer. Never push, and never merge into `phase3`: the owner merges after reading the report. Every commit leaves the suite green, so the branch stays bisectable.

## 2. Orientation — the suite as measured

These figures are from phase3 @ 3e6b811 on 2026-10-18, measured in the live tree. Re-measure in Phase 0; use these for calibration only.

**Size**
- 173 test files and about 67k lines.
- About 3,500 `def test_` functions, which expand to 4,463 collected items; about 12.6k assert lines.
- `tests/unit` holds 153 files (flat), `tests/chaos` 17, `tests/replay` 2 and `tests/property` 1.
- `dashboard/tests` holds 8 TypeScript files, run by `node --test` (`npm test`).

**Runtime**
- One full `pytest` run (testpaths = `tests`) took 762 s (12m42s): 4,444 passed, 19 skipped, 4 warnings.
- Collection alone is 42 s, and a bare `import engine` is 7.4 s (the sklearn preload).
- The host has 22 logical cores (Core Ultra 7 155H); the suite uses one.
- The 80 slowest call/setup phases total about 236 s. The other ~4,380 tests average about 0.11 s each, so the long tail of per-test fixture cost outweighs the outliers.
- The ten slowest call/setup phases:
  - `test_import_graph.py::test_oms_and_paper_never_reach_the_live_broker_transitively`: 22.8 s
  - `test_research_studies.py::test_brk20_reference_arm_reproduces_the_registered_baseline`: 16.3 s
  - `test_single_instance.py::test_subprocess_is_refused_while_held_then_kernel_releases`: 15.3 s (spawns a subprocess that imports `engine`)
  - replay session setups: 14.1 s, 11.4 s and 5.9 s
  - `test_market_backup.py` export tests: 11.3 s, 7.4 s and 4.8 s
  - `test_stop_path.py::test_hard_exit_forces_exit_past_a_wedged_nondaemon_thread`: 9.8 s
  - `test_filings_experiments.py` e1/e2: 7.9 s and 4.3 s
  - `test_filings_pit_fresh.py::test_both_surfaces_fail_degrades_and_warns`: 6.0 s
  - `test_sweep_smoke.py`: 4.3 s, 3.4 s and 2.9 s
- COMMANDS.md calls `tests/unit` "the full suite (several minutes)".

**Shared fixtures** (`tests/conftest.py`)
- `pytest_configure` copies `config/` to a temp dir with the O19 capital pinned (`PINNED_CAPITAL`) and points `MT_CONFIG_DIR` at it, so no test depends on the live capital. The `MT_PIN_SOURCE` hook is the manual proof of that.
- `clock` is frozen at `FIXED_NOW` (Wed 2026-06-17 10:05 IST).
- `conn` creates a new SQLite file and runs all 18 migrations, once per test; about 780 tests request it directly.
- `bus`.

Two more conftests:
- `tests/replay/conftest.py` builds session-scoped synthetic days and warms DuckDB up, so the perf smoke times the warm path.
- `tests/chaos/conftest.py` auto-marks `chaos` and defines the skip reasons (`PHASE3_GATED` …). Their visibility in every run is deliberate.

**Idioms**
- No `unittest.mock` anywhere: `monkeypatch` (227 `setattr` calls) plus hand-written fakes.
- 238 `parametrize`, `caplog` on about 650 lines, 351 `pytest.raises`.
- 48 `inspect.getsource` source-text pins, 39 of them in `test_ops_main_wiring.py`. They pin closures inside `engine.ops.main:run`, a composition root no test can boot, and are its only wiring guard.
- Property tests use hypothesis with `max_examples=200, deadline=None`.
- `asyncio_mode = auto`.

**Import-order trap.** sklearn must load before numba, vectorbt or cvxpy, or the process segfaults. `engine._preload` handles this, so `engine` must be imported before any of those in tests and conftests.

**Largest files**
- `test_reco_pipeline.py`: 3.8k lines, 144 tests
- `test_ops_main_wiring.py`: 3.4k lines, 142 tests
- `test_telegram_commands.py`: 1.4k lines, 102 tests
- then `test_risk_gate.py`, `test_paper_broker.py`, `test_prescreen.py` and `test_ticker_supervisor.py` (41 patches)

**Likely overlap**
- Work-order files beside their module's main file: `test_reco_pipeline_wo24.py`, `test_backfill_wo24.py`, `test_health_wo24.py`, `test_alert_episodes_wo25b.py`, `test_store_isolation_wo26a.py`. Each pins a dated incident, so they are Tier B.
- Per-migration files `test_migration_0015/0016/0017.py` beside `test_migrations.py`.
- `test_hi52.py` and `test_hi52_edge.py`.

**Research and one-off script tests**
- `test_backtest_{brk20,candles,cli,fade,hi52,hvol,tdc}.py`
- `test_event_study_filings.py`, `test_research_{filters,studies}.py`, `test_filings_experiments.py`
- `test_sweep_{signals,smoke}.py`, `test_validate_insider.py`, `test_dedupe_news_script.py`
- the verdict scripts: `test_hi52_forward_verdict.py`, `test_no_stop_verdict.py`, `test_g2_evidence_budget_window.py`

Several of these back studies that are closed and refuted (fade, candles, tdc, hvol). The owner keeps the refuted-strategy record visible, and these scripts are how it is reproduced.

## 3. Protection tiers

**Tier A: keep. Refactor or speed up only if the assertions stay the same.**
- Every `tests/chaos` case and its skip markers. There is one file per plan §9.4 row, mapped from `runbooks/CHAOS_DRILLS.md`.
- `tests/replay` (golden-day digest determinism, paper end-to-end) and `tests/property` (the OMS state machine, plan §9.2).
- Architecture guards: `test_live_impossibility.py`, `test_import_graph.py`, `test_scope_isolation.py`, `test_api_route_auth.py` and `test_single_instance.py`.
- Tests that pin pre-registered or frozen values (forward-test kill rules, verdict thresholds, shadow thresholds, registered baselines). Their job is to make a silent re-tune fail.
- The conftest capital pin and its `MT_PIN_SOURCE` hook.

**Tier B: removal needs fault-injection evidence (§1 rule 4).**
- The safety and resilience layer:
  - kill switch, watchdog, heartbeat;
  - limits engine, protected store, envelope enforcement, risk gate, warm-up gate;
  - ticker supervision and reconnect, token/session expiry, catch-up runner;
  - reconciliation (bars, holdings, paper);
  - every latch, both its set and its clear (FABLE_HANDOFF §3).
- The order path: OMS state and store, order manager, paper broker, fill model, protection and exit managers, placement contracts, idempotency.
- Money and time: cost model, money, capital, position book, exposure, calendar and clock, IST/UTC handling, session boundaries.
- Seam tests that drive a producer through its real consumer. Unit tests on either side don't subsume them (FABLE_HANDOFF §4).
- Any test whose name, docstring or comment cites an incident, a work order, a dated WORKLOG entry, a memory or a plan §. Somebody paid for that regression once.
- Owner-facing text: assertions that a Telegram, alert or dashboard message is conditional, or that pin copy the owner decided.
- Migration tests.

**Tier C: everything else.** Removal needs a ledger row with its code and a pointer. For R2, the pointer is the survivor's nodeid plus the assertion that covers the removed test.

When a test fits several tiers, the strictest applies. For example, a research-script test that pins a registered baseline is Tier A, not R8.

A test that nothing flags and no candidate sheet questions is kept without a ledger row. List its file under "Sound as-is".

## 4. What to change

Every ledger row carries exactly one code.

**Removals**
- **R1 Duplicate**: same behavior, same input class and same assertions as another test. Check WO files against their main file first. Keep the clearer test, and move over any assertion that only the removed one has.
- **R2 Subsumed**: other tests drive the same path and assert a superset. Name the survivors and the assertions that cover it.
  - Necessary condition: the removed test's per-test footprint (Phase 1) lies inside the survivors' combined footprint.
  - A line outside it needs a one-line justification (incidental logging, for example). Otherwise the claim is false.
- **R3 Vacuous**: the test can't fail for a real defect. Signs:
  - no assertion on the code under test;
  - asserting that a fake returned what it was told;
  - asserting a value the test itself set, or one the previous line already guarantees;
  - testing a test helper that nothing else relies on.
- **R4 Change-detector**: pins implementation text, or an incidental constant with no behavioral meaning. Examples: a source-text pin where a behavioral test of the same seam exists; log wording nothing consumes; a config value echoed back. These are NOT R4:
  - frozen or pre-registered values;
  - interfaces: Telegram commands, API routes, the DB schema, and any structlog event or field that a script, the watchdog or the dashboard reads (grep `scripts/`, `src/` and `dashboard/src` before deciding);
  - copy the owner decided.
- **R5 Third-party**: the test checks pydantic, DuckDB, YAML, httpx or the stdlib rather than this code.
- **R6 Dead**: the code path, setting or feature is gone or unreachable. Before concluding that, rule out `getattr` and string-built names, config-driven dispatch, and callers in `scripts/` (FABLE_HANDOFF §1, absence of evidence).
- **R7 Excessive**: more cases than the behavior has equivalence classes. Keep one case per class plus the boundaries, using FABLE_HANDOFF §3's list:
  - empty / one / many / duplicate;
  - missing / malformed / huge;
  - repeated / out-of-order / stale;
  - time: 09:15, 15:29–15:30, holidays, midnight, IST vs UTC.
- **R8 Finished-script tests (owner decision)**: tests of a research CLI whose study has a recorded verdict, or of a one-off script whose job is done. Propose one smoke test per script, run on its tiny fixture: the script runs, the output schema holds, and one known number reproduces. If the script itself should go, that is a production proposal. List each case with the lines and seconds it saves, and don't execute it without the owner's OK.

**Revisions**
- **P1 Parametrize**: tests that share a body shape and differ only in literals become one `@pytest.mark.parametrize` with `ids` that read as the case. Don't merge cases that need different setup, or whose merged body would branch on the case. Two clear tests are better than one test with an `if`.
- **P2 Trim asserts**: remove an assert that repeats another, that a stronger assert in the same test implies (`len(x) == 2` before `x == [a, b]`), or that pins an incidental, brittle detail (exact log wording, float formatting) unrelated to what the test is named for and not part of an interface. Keep guard asserts that stop a vacuous pass (`assert rows, "fixture produced no rows"`).
- **P3 Trim patches**: remove a monkeypatch whose target the test never reaches, whose replacement equals the real behavior, or that duplicates an existing fixture or fake.
  - Prove "never reached" by observation: make the patched callable raise, and the test must still pass.
  - Never remove a patch that isolates the test from the network (Kite, NSE/BSE, Telegram, the Claude SDK, httpx), `data/`, the engine's API port, keyring, the real clock, real sleeps or subprocesses. Without such a patch the test may still pass while doing harm.
- **P4 Fixtures**: hoist repeated setup into fixtures. Widen a fixture's scope only for immutable setup; mutable state stays function-scoped. Move a test between files only when that enables a merge: reorganizing for its own sake costs blame history and buys nothing.
- **P5 Repair**: fix a test whose name or docstring claims a behavior its body doesn't check. Either make it check the claim (and watch it fail under the fault), or rename it to what it does check. Also de-flake tests that depend on the real clock, on timing or on ordering.

## 5. Speed: measure first, then attack in order

The gate is `pytest` (testpaths = `tests`: unit, chaos, replay and property) plus `npm test` in `dashboard`. Report numbers for the gate and for `tests/unit`, the documented inner loop.

**Measure in the worktree's own venv**, the environment every later number will come from:
- two full runs with `--durations=0 --junitxml=…`;
- collection time (`--collect-only -q`);
- an import profile (`-X importtime`) if collection stays large.

The engine shares this host, so interleave before and after runs and quote medians. Every optimization gets its own before/after measurement: "a perf claim without before/after numbers is an opinion" (FABLE_HANDOFF §5). If fixture time is dominated by file creation in temp dirs, real-time antivirus scanning may be the cause. Report it with the evidence; an exclusion is the owner's call.

**Likely order of payoff** (check each against the baseline):
1. **Per-test SQLite migration.** `conn` creates a file and runs 18 migrations for each of about 780 tests.
   - Migrate one template per session and copy it per test (file copy or the `sqlite3` backup API); isolation stays per test.
   - Close or checkpoint the template before copying, so no WAL contents are left behind.
   - Prove equivalence: the copy's `sqlite_master` and `PRAGMA user_version` must match those of a freshly migrated database.
   - `test_migrations.py` keeps running the migrations from scratch.
2. **DuckDB `MarketStore` setup.** There are 78 constructions in tests, and each one runs the schema DDL. Use the same template-copy approach wherever a test only needs an empty store with the schema.
3. **Real waiting.** Look for nonzero `asyncio.sleep`, `wait_for` timeouts that are meant to expire, tenacity retry waits, polling loops and real thread or process timeouts. Replace them with the frozen clock, zero-wait strategies patched in through a fixture, or event-driven synchronization.
4. **Parallelism.** pytest-xdist is the biggest structural lever on a 22-core host, but each worker pays about 40–50 s of collection. Try it in the worktree venv:
   - `-n auto` against `-n 4` and `-n 8`, and `--dist loadfile` against `load`, three runs each;
   - fix what breaks on the test side, and group what can't run concurrently (`xdist_group` with `--dist loadgroup`);
   - adopt it only if the suite is green across three consecutive parallel runs.

   Known risks:
   - `pytest_configure` sets `MT_CONFIG_DIR` in the controller, and the workers inherit and share that one pinned copy.
   - The replay perf smoke and the timing-sensitive tests.
   - Any test that writes outside `tmp_path`.
5. **The top of `--durations`.** Look at each of the 30 slowest tests individually: outliers usually have a one-line cause. Tier A tests are in scope if the assertions stay the same. For example, `test_import_graph.py` can parse each file once per session. Do this after the removals, so you don't optimize tests you're about to delete.
6. **Replay tier.** Where several tests only read one replay's results, share a session-scoped result. The two-run digest check stays two runs.
7. **Collection and imports.** Move heavy test-side imports (vectorbt, numba, skfolio, river, the Claude SDK) into the tests that use them, provided `engine` is still imported first.

Optional, once the real wins have landed: register a `slow` marker and document a fast inner-loop command in COMMANDS.md. The full suite stays the gate.

## 6. Execution shape

**State.** Keep `runbooks/test_audit/STATE.md` on the branch, recording the phase, the batches done and the open questions, and commit it with every batch. If a session dies mid-task, the next one resumes by auditing STATE.md, LEDGER.md and `git log` against this prompt (FABLE_HANDOFF §6). Raw outputs too big to commit go to `..\mt_build_notes\test_audit\`. Never put them in `%TEMP%`, which this machine wipes during standby.

**Phase 0: set up and take the baseline (inline).**
1. Run `Get-Date`. Run `git status` in the live tree (leave anything dirty alone) and `git rev-parse phase3`.
2. Create the worktree: `git worktree add -b wip/test-audit C:\Users\piyush.kumar_atari\projects\personal\mt_testaudit phase3`.
3. In the worktree, run the §1 rule 3 checks, then `uv sync --frozen --extra dev`.
4. Verify the setup:
   - `.\.venv\Scripts\python.exe -c "import engine; print(engine.__file__)"` prints a path under `mt_testaudit`;
   - the live tree's `.venv\Lib\site-packages\_editable_impl_market_trading.pth` still reads `…\market_trading\src`.
5. If the sync can't complete (offline, a build failure), stop using uv entirely:
   - run pytest with the live interpreter by absolute path from the worktree root (the root conftest puts the worktree's `src` first; confirm with the `engine.__file__` check);
   - park the xdist and coverage steps for the owner.
6. `npm test` in the worktree's `dashboard` should need no packages: the tests, and the `src` modules they import, use only `node:` builtins and local files. If it fails on a missing package, junction `node_modules` as memory `worktree-venv-imports-live-tree` describes, and unlink it before the worktree is removed.
7. Run the §5 baseline. Save the collected nodeids (`--collect-only -q`) as `runbooks/test_audit/baseline_nodeids.txt`; Phase 5 reconciles against this file. A test that passes in the live tree but fails in the worktree depends on untracked state, which is a finding in itself.

**Phase 1: inventory (delegate; you audit).**
- **Inventory script** (`implementer`): `runbooks/test_audit/tools/inventory.py` writes `inventory.csv`, one row per collected test, with:
  - the nodeid and file:line;
  - a tier guess, which you confirm;
  - baseline duration, assert count and `monkeypatch` targets;
  - `caplog` and `getsource` flags, markers, and whether it is parametrized;
  - citations: WO ids, `§x.y`, dates, memory names;
  - an AST shape hash of the body with literals normalized. Tests with identical hashes are P1 candidates.
- **Coverage footprints** (you): run the suite once with per-test contexts (`pytest-cov`: `--cov=engine --cov-context=test`).
  - Keep only the `run` phase, so session fixtures don't skew the footprints.
  - The result is the necessary-condition check for R2, and a source of candidates (footprint subsets within a file).
  - The run is slow: do it once, with nothing else running.
  - `sys.monitoring` is the no-install fallback.
- **Candidate sheets** (`log-digger`, ≤4 at a time, one file family each, sized at about 6k test lines or less per agent for Haiku's 200K window; the two giant files get one agent each). For every test the sheet gives:
  - one line on what it does;
  - its key asserts, quoted with line numbers;
  - its patches and citations;
  - any flags from §4;
  - any suspected duplicate or subsumer, with a pointer.

  The sheets describe; they don't decide. The agents return them in their reports, and you save them under `..\mt_build_notes\test_audit\`.

**Phase 2: structural speed-ups first (§5 items 1–4).** These make every later run faster. They are conftest changes that touch hundreds of tests, so design them yourself, delegate the implementation to `designer-impl`, and measure each one on its own.

**Phase 3: verdicts (yours; never delegated).**
- Work from the biggest payoff down: WO-file pairs, shape-hash groups, footprint subsets, the source pins, the two giant files, then the rest.
- Each verdict records a tier, a code, a survivor pointer and a decision. Write it into LEDGER.md before making any edit.
- If a verdict resists two genuine attempts, keep the test and log an unverified lead.
- Finish with a self-adversarial pass over the ledger: what is the strongest case that a removal lost something?

**Phase 4: execute in batches.** Use one batch per file family: risk & gate; OMS & paper; ops wiring; data feeds; news & intelligence; Telegram & API; research scripts; the rest. For each batch:
1. Write the spec: the files, the ledger rows, the parametrize `ids`, and what stays.
2. Delegate per CLAUDE.md: `implementer` for mechanical work, `designer-impl` where restructuring needs design (for example, pulling `test_reco_pipeline.py`'s repeated setup into fixtures).
3. Audit the diff against the ledger.
4. Run the batch's files, then commit. If the batch touched a conftest or a shared helper (`tests/chaos/_*_rig.py`, `tests/replay/fixtures/`, anything another file imports), run the full suite before committing.

Rules for delegated work in this phase:
- Tier B fault injection is delegable too. Give the agent the exact mutation (file:line, before → after), the test that must fail, and the revert as an exact edit back (agents don't run git). Afterwards, check that `git -C <worktree> status --short src` is clean.
- Batches that touch disjoint files may run in parallel. Each agent runs only its own files; only you run the full suite.
- Every brief names the worktree's absolute path and says three things: never edit under `market_trading\`, never run `uv`, and run tests inline with the worktree interpreter.

When the batches are done, finish §5 items 5–7 (outliers, the replay tier, imports).

**Phase 5: verify.**
- Do two full serial runs, plus three parallel runs if xdist was adopted. All must be green, with the baseline's skip/xfail set minus removed tests, and no new warnings (the baseline has 4).
- Run `-m chaos`, `tests/replay`, `tests/property` and `npm test` explicitly.
- Reconcile with a script: baseline nodeids minus ledger removals, with ledger renames applied, must equal the final nodeids. A nodeid that is missing without a ledger row is a silent removal: fix it.
- For every fixture whose scope widened, run each affected file alone, and the affected set in reverse order.
- Run `ruff check tests`.
- Review the whole branch from four angles in parallel (memory `multi-angle-review-catches-interactions`). Verify each finding before acting on it.
  1. Protection lost: re-inject the fault for a sample of Tier B removals and about 10% of Tier C R1/R2 removals.
  2. Hermeticity: no removed patch opened the network, `data/` or the real clock, and no test writes outside `tmp_path`.
  3. Fixture-scope leakage and order dependence.
  4. Parametrization: every case runs, every id matches its inputs, and no two cases are identical.

**Phase 6: report and hand off** (§7).
- If `phase3` has moved since Phase 0, rebase the branch onto it and re-run the gate.
- Leave the worktree in place for the owner's merge.
- In REPORT.md, give the clean-up steps (`git worktree remove`; if any junction was created, unlink it with `cmd /c rmdir` first, and never `Remove-Item -Recurse`).
- If xdist joined the dev extra, give the live-venv sync step too, and confirm that `git diff phase3 -- uv.lock` touches only the new packages.

## 7. Deliverables

All of these go on branch `wip/test-audit`.

1. **Commits**: one per batch. Each lists the ledger rows it applies, and each leaves the suite green.
2. **`runbooks/test_audit/LEDGER.md`**: one row per removed, merged, renamed or parametrized test, with these columns:
   - the nodeid(s);
   - the tier;
   - the code;
   - the evidence: the survivor's nodeid and the assertion that covers the removed test (file:line), plus, for Tier B, the fault-injection command and the failing line;
   - the commit.
3. **`runbooks/test_audit/REPORT.md`**:
   - **TLDR** (≤8 lines): collected tests, lines and median wall-clock, before → after, for both the gate and `tests/unit`; then the three biggest wins.
   - **Speed table**: each optimization, its before/after seconds, and how it was measured.
   - **Changes by code and tier**, with counts.
   - **Owner decisions**, each with its evidence and a recommendation:
     - the live-venv sync for xdist;
     - production seams, such as a testable wiring seam to replace the `ops.main:run` source pins, or a lighter import path for the single-instance subprocess;
     - R8 shrinks;
     - anything else that touches Tier A or B.
   - **Sound as-is**: the files audited and left alone, one line each, so nobody re-audits them.
   - **Considered and rejected**, and **unverified leads**.
4. **`runbooks/COMMANDS.md`**: the updated test commands (inner loop, full gate, parallel run, and the inventory and footprint tools).
5. **`runbooks/WORKLOG.md`**: an entry in the house format (`## YYYY-MM-DD HH:MM — title (owner: "…")`) with the headline numbers.

Your final chat message gives the TLDR, the path to REPORT.md and the list of owner decisions.
