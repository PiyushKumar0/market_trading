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

- [x] Phase 0 — baseline (commit "test audit: Phase 0 baseline ...")
- [ ] Phase 1 — inventory (done: `mt_build_notes\test_audit\inventory.csv`), footprints, candidate sheets
  (G01 done; others in flight — families in `mt_build_notes\test_audit\briefs\families.txt`)
- [ ] Phase 2 — structural speed-ups
- [ ] Phase 3 — verdicts (LEDGER.md)
- [ ] Phase 4 — batches
- [ ] Phase 5 — verify
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

(none)

## Open questions

- Owner: the worktree venv (uv sync) was refused by the auto-mode classifier — xdist and pytest-cov are parked.
