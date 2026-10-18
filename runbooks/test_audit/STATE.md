# Test audit — state

Resume rule (PROMPT.md §6): audit this file, LEDGER.md and `git log` against PROMPT.md before doing anything.

## Setup (Phase 0, 2026-10-18 01:42–)

- Worktree `C:\Users\piyush.kumar_atari\projects\personal\mt_testaudit`, branch `wip/test-audit`, based on
  phase3 @ 3e6b811. The prompt is copied to `runbooks/test_audit/PROMPT.md` (the live tree's copy is untracked).
- **No worktree venv.** `uv sync --frozen --extra dev` in the worktree was denied by the Claude Code auto-mode
  classifier ("Modify Shared Resources", 01:45). Per PROMPT §6 Phase 0 step 5, uv is not used at all:
  - pytest runs with the LIVE interpreter by absolute path, from the worktree root:
    `C:\Users\piyush.kumar_atari\projects\personal\market_trading\.venv\Scripts\python.exe -m pytest ...`
    (never installs into it). The root conftest puts the worktree's `src` first; verified with
    `mt_build_notes\test_audit\tools\engine_origin_check.py`: `ENGINE_FILE ...\mt_testaudit\src\engine\__init__.py`.
  - The live `.pth` still reads `...\market_trading\src` (checked after every setup step).
  - **Parked for the owner:** pytest-xdist (parallelism, §5 item 4) and pytest-cov. Footprints use the
    `sys.monitoring` fallback instead: `runbooks/test_audit/tools/footprint.py` (+ `footprint_query.py`).
  - Subprocess-spawning tests run `sys.executable` = the live interpreter, whose `.pth` imports the LIVE tree's
    `engine` unless the test sets PYTHONPATH — relevant to fault injection (a mutant in the worktree is invisible
    to such a subprocess). Check per test before relying on one as a fault-injection survivor.
- mt-engine was **Stopped** at 01:45 (observed, not touched): baseline timings were taken on a quiet host.
- Run harness: `mt_build_notes\test_audit\tools\run_suite.ps1 -Label <x> [-PytestArgs @(...)]`; every run appends
  a line to `mt_build_notes\test_audit\runs\runs.log` (label, HEAD, wall-clock, pytest summary).
- Candidate sheets: general-purpose agents pinned to `model: haiku` that WRITE their sheet to
  `mt_build_notes\test_audit\sheets\` (log-digger cannot write files; returning sheets through the manager's
  context would cost Opus output tokens to save them). Same tier as PROMPT §6 names; only the delivery differs.

## Phase

- [x] Phase 0 — baseline (5788c34)
- [x] Phase 1 — inventory (`mt_build_notes\test_audit\inventory.csv` + `shape_groups.txt`), per-test footprints
  (`mt_build_notes\test_audit\footprints.sqlite`, scoped sys.monitoring, ~4% overhead; full run `footprint_2`:
  4,456 tests, 31,145 engine+script lines), candidate sheets G01–G13, G12b, G15, G16 (chaos) done
  (`mt_build_notes\test_audit\sheets\`), G17 (replay/property/backtest_cli+brk20+hi52) in flight; G14 (refuted
  research scripts — R8 domain) not run: the R8 table in `notes_owner_decisions.md` is the owner's call.
- [~] Phase 2 — structural speed-ups: conn template (0d9433f), single-instance child (46323a0), backoff seams +
  import-graph cache + 501-symbol batch seed (54a3b39). Rejected: DuckDB template (~34 ms x ~600 stores ≈ 20 s
  for ~50 fixture files + an equivalence guard). Parked: xdist (no worktree venv). Measurements: `runs\runs.log`
  (labels `ab_*`), A/B against the detached baseline worktree `C:\Users\piyush.kumar_atari\projects\personal\mt_testaudit_base`
  (5788c34; no venv, no junctions — remove with `git worktree remove` when done).
- [~] Phase 3 — verdicts: LEDGER rows 1–45. Rejected candidates and why: `mt_build_notes\test_audit\notes_rejected.md`;
  owner-decision inputs: `notes_owner_decisions.md`. §5 items 5–7: the 30 slowest reviewed; import floor
  (`engine._preload` → sklearn, ~5.5 s per process) and vectorbt JIT are production-side (proposals 5, 6).
- [~] Phase 4 — Batch A (rows 1–14) committed 4219e5d; Batch B (rows 15–27) 9126968 (its 28-min
  `test_ops_main_wiring.py` run was a lid-close Modern Standby, Kernel-Power 506/507 "Reason: Lid"
  09:57:32–10:25:13 — the file's baseline is 11.9 s). Batch C (rows 28–45) committed with this file's update:
  two implementers in parallel, C1 in this worktree and C2 in a second worktree
  `C:\Users\piyush.kumar_atari\projects\personal\mt_testaudit_c2` (branch `wip/test-audit-c2` at 9126968; its diff
  was applied here as `mt_build_notes\test_audit\patches\batchC2.patch` — remove that worktree and branch at the
  end). Every row fault-injected (P1 row 37 excepted), `src` clean.
- [ ] Phase 5 — verify (two serial full runs + interleaved base runs, chaos/replay/property/npm, reconcile_nodeids.py,
  ruff, scope-leak checks, 4-angle review); parallelism experiment with tools/shard_run.py.
- [ ] Phase 6 — report

## Baseline (Phase 0)

Measured in the worktree at 3e6b811 with the live interpreter; raw outputs in `mt_build_notes\test_audit\runs\`.

- Collection: 4,463 items; 35.1 / 39.9 s by pytest's clock, 46.0 / 50.6 s wall (warm `.pyc`).
- **Full run 1** (01:50, engine stopped, no agents for the first ~9 min): 4,443 passed, 20 skipped, 4 warnings,
  **622.5 s** pytest / 636.8 s wall. The timing reference.
- Full run 2 (02:01): same outcome, 1,028 s — **invalid for timing**: Modern Standby (Reason: Idle Timeout)
  02:11:58–02:18:07 froze one setup (`test_weekly_summary.py::...[d4-False]`) for 364.9 s, plus a 46 s doze at
  02:06. Everything else matched run 1 within a few percent.
- Skip set = the live tree's 19 + `test_research_studies.py::test_brk20_reference_arm_reproduces_the_registered_baseline`,
  which needs `data/research/market_*.duckdb` and `data/reports/backtest_brk20_2026-09-12.json` (live, untracked).
  It passes in the live tree (16.3 s there) and cannot run hermetically — a finding, left as is (Tier A).
- Run 1 phases: setup 262.6 s, call 257.8 s, teardown 41.1 s. The tail: 1,843 setups in [0.05, 0.2) s = 188.6 s.
  `conn` is requested directly by 1,247 tests (133.8 s of setup); 717 tests without `conn` still spend
  ≥ 0.05 s in setup (122.9 s; mostly DuckDB MarketStore fixtures).
- Fixture micro-benchmark (medians of 30, `tools\bench_fixtures.py`): SQLite connect + 18 migrations 102.4 ms vs
  template copy + connect 20.6 ms; DuckDB MarketStore open+close 118.3 ms fresh vs 84.0 ms from a template copy.

**Host standby.** With the owner away this laptop enters Modern Standby within ~30 s of inactivity ("Idle
Timeout"), freezing pytest and stalling agents. `mt_build_notes\test_audit\tools\keep_awake.ps1` holds a
process-scoped `SetThreadExecutionState(ES_CONTINUOUS|ES_SYSTEM_REQUIRED|ES_DISPLAY_REQUIRED)` (no setting
changed; released on exit). It cannot and must not stop a lid close: 02:22:13 "Reason: Lid" suspended the session
until 07:50. `run_suite.ps1` logs `standby_entries` per run; a run with entries > 0 is not a timing sample.

## Batches done

- Batch A — LEDGER rows 1–14 — 4219e5d (7 removals, 6 repairs, 1 de-flake; every Tier B fault-injected).
- Batch B — LEDGER rows 15–27 — 9126968 (3 removals, 9 repairs incl. one Tier A strengthening (row 17), 1 fixture
  consolidation; every Tier B fault-injected).
- Batch C — LEDGER rows 28–45 — the commit after 9126968 (3 removals incl. 1 vacuous source pin replaced by a
  behavioural round trip, 13 repairs incl. 3 Tier A (rows 31, 32, 36), 1 merge-parametrize, 1 assert trim).

## Open questions

- Owner: the worktree venv (uv sync) was refused by the auto-mode classifier — xdist and pytest-cov are parked.
