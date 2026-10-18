# Test audit — report (2026-10-18)

Branch `wip/test-audit` (worktree `C:\Users\piyush.kumar_atari\projects\personal\mt_testaudit`), cut from `phase3`
@ 3e6b811. Every change is a row in [LEDGER.md](LEDGER.md); the run history is in [STATE.md](STATE.md). Production
code (`src/`, `scripts/`, `config/`, `web/`, `dashboard/src`), `pyproject.toml` and `uv.lock` are unchanged.

## TLDR

- **Gate** (`pytest`, all four tiers): 4,463 → 4,456 tests; 84,240 → 84,208 lines; median 622.5 → 534.7 s
  serial (−14%; back-to-back pairs −10% and −27%, busy host); every run green.
- **`tests/unit`**: 4,333 → 4,326 tests; 76,017 → 75,947 lines; per-test time −18% to −31% (one noisy unit-only pair: 589 → 561 s).
- **Biggest wins:** `conn` from a session-migrated SQLite template (102 → 21 ms per test, 1,247 tests);
  `test_import_graph.py` 62.8 → 3.3 s (Tier A, same assertions); the single-instance child 18.8 → 5.7 s.
- **Protection up:** 28 tests that passed while blind to the fault they name were repaired, each shown to FAIL
  under it; 13 removed (survivors fault-checked); 17 copy-paste groups parametrized; 1 change withdrawn on review.
- **Parallel today:** 8 file-shards (`tools/shard_run.py`, no xdist) ran the gate green in 223-340 s.

## Speed table

Method: interleaved A/B against a detached baseline worktree (`mt_testaudit_base` @ 5788c34 = phase3 tests), the
live interpreter for both, pytest's own "in N s" unless stated; a run with a Modern Standby entry is not a timing
sample (the harness logs `standby_entries`). The host was busy (the owner was using it) for most rounds, so
absolute numbers carry ±15% noise; the per-change A/B rows were taken back-to-back, three rounds each, medians.

| change | commit | measured on | before | after |
|---|---|---|---|---|
| `conn` copies a session-migrated SQLite template instead of migrating per test (18 migrations) | 0d9433f | micro-benchmark, 30 reps | 102.4 ms per `conn` | 20.6 ms |
| ″ (1,247 tests request `conn` directly) | 0d9433f | smoke subset (reco, telegram, oms_store, position_book, chaos), setup-phase sum | 44.0 s | 28.6 s |
| single-instance child imports a bare `engine` package, skipping the sklearn preload | 46323a0 | `test_single_instance.py`, A/B x3 | 18.84 s | 5.70 s |
| `test_import_graph.py` reads `repo_root()` once and caches per-module lookups (Tier A, same assertions) | 54a3b39 | the file, A/B x3 | 62.82 s | 3.29 s |
| `instant_backoff` on the two tests that waited out real NSE/BSE retry backoff | 54a3b39 | the two tests, A/B x3 | 12.16 s | 2.32 s |
| 501-symbol seed written in one upsert | 54a3b39 | the wide-index test, A/B x3 | 21.64 s | 7.19 s |
| removals and merges (Batches A-D: 13 removed, 16 pairs parametrized) | 4219e5d…4056462 | — | ms-level tests | negligible |
| full gate (`pytest -q`), back-to-back pairs | 7b1f4f6, 10331d7 | pytest's clock | 596.1 s (round c), 759.4 s (round d); 622.5 s at Phase 0 (quiet host) — median 622.5 s | 534.7 s (c), 557.5 s (d), 451.0 s (c, its base run void: lid standby) — median 534.7 s (−14%) |
| same pairs, sum of per-test time (`--durations=0`) | ″ | collection and reporting excluded | 536.3 s (c), 680.4 s (d) | 462.7 s (c, −14%), 491.2 s (d, −28%) |
| unit tier inside those runs | ″ | per-test time | 463.6 s (c), 579.2 s (d) | 381.7 s (c, −18%), 397.5 s (d, −31%) |
| `pytest tests/unit -q` alone, one pair | 10331d7 | pytest's clock | 589.4 s | 560.6 s (−5%; untouched files ran ~40 s slower in this audit run — host noise, e.g. test_market_backup 24.7 → 43.3 s) |
| parallel: `tools/shard_run.py`, test files packed onto N plain pytest processes | 10331d7 | wall-clock, the whole gate | serial median 534.7 s | 4 shards 472.5 s; 8 shards 340.2 s, 238.9 s and 223.1 s (the documented command, repo weights) — all green, 4,456 tests |

Where the full-suite time went (sum of per-test durations, `--durations=0`; collection and reporting excluded):
base (`full_base_c_2`) setup 244.1 / call 256.8 / teardown 35.4 s = 536.3 s; audit (`full_audit_c_1`) 163.5 / 207.8 /
25.4 s = 396.7 s. By tier: unit 463.6 → 321.9 s; replay 38.5 → 38.4 s; chaos 33.5 → 35.6 s (Tier A, untouched).
Largest file-level drops: test_single_instance 22.8 → 4.4 s, test_import_graph 18.2 → 1.4, test_reco_pipeline
21.2 → 11.6 (template), test_telegram_commands 11.9 → 4.9, test_filings_pit_fresh 10.7 → 3.8, test_universe_builder
9.3 → 4.3.

What is left, and why it is not test-side (details under Owner decisions D2):
- **The per-process floor.** A one-file pytest run costs ~15 s wall on this host before its first test, ~9.5 s of
  it the first `import engine`: `engine/__init__` imports `engine._preload`, which imports sklearn (8.40 s
  cumulative, `-X importtime`; sklearn alone adds 5.1-5.9 s in a fresh process). Production proposal 5.
- **vectorbt JIT.** ~23 s sits in five `needs_heavy_deps` tests whose call time is ~100% numba compilation
  (cProfile: `compile_extra` 15.0 of 15.5 profiled seconds): 17 `vectorbt/portfolio/nb.py` kernels lack
  `cache=True`. Library-level. The inner loop skips them with `-m "not needs_heavy_deps"`.
- **File-system latency.** Defender real-time protection is on; `os.stat` 494 us, `Path.resolve` 3.9 ms, a 4 KB
  create+write+close 1.9 ms (medians of 200). Every per-test temp file pays it — the template copy still costs
  20.6 ms. An AV/EDR exclusion is probably the biggest remaining lever.
- **Parallelism.** Without xdist, `tools/shard_run.py` packs test files onto N plain pytest processes (each pins
  its own config copy and basetemp): 8 shards ran the gate green in 223-340 s, 4 shards in 472 s (every process
  pays the ~15 s floor, and they contend for the AV-filtered disk). No shared-state failure appeared across 4 or
  8 concurrent processes.

## Changes by code and tier

62 ledger rows ([LEDGER.md](LEDGER.md) has the evidence and commit for each): 61 applied, 1 withdrawn (row 32,
see Verification).

| code | meaning | A | B | C | total |
|---|---|---|---|---|---|
| R1 | duplicate removed | 0 | 3 | 1 | 4 |
| R2 | subsumed, removed (survivor fault-checked) | 0 | 6 | 0 | 6 |
| R3 | vacuous, removed / replaced | 0 | 0 | 2 | 2 |
| R4 | change-detector (source pin) replaced by its behavioural twin | 0 | 1 | 0 | 1 |
| P1 | copy-paste tests parametrized (one-for-one nodeid mapping) | 0 | 13 | 4 | 17 |
| P2 | redundant asserts trimmed | 0 | 1 | 0 | 1 |
| P4 | fixtures consolidated / added | 0 | 1 | 1 | 2 |
| P5 | repaired: the test did not catch what its name claims | 3 | 21 | 4 | 28 |

Collected: 4,463 → 4,456 (`reconcile_nodeids.py`: RECONCILED — 50 nodeids removed or renamed away, 45 added; no
silent change). Test code: 84,240 → 84,208 lines (`tests/` + root conftest), `tests/unit` 76,017 → 75,947. Net
lines barely move because the 28 repairs ADD assertions; the large line cut is the R8 owner decision (D3,
~3,000-4,100 lines).

**The most valuable output is the P5 column.** Each of those 28 tests passed while blind to the fault its own
name or docstring describes, and each repair was shown to FAIL under that fault (the original was shown to PASS
under it). Examples:
- the risk gate's shrink-then-recheck and hard-failure-beats-shrink were still sized for the pre-O16 Rs20,000 cap,
  so no shrink was ever pending (rows 15-16);
- a protective SL-M SELL at exactly its trigger was untested — `<` for `<=` passed the suite (row 19);
- the bhavcopy composition-root regression test exercised a hand-written mirror, not the real closure (row 8);
- the nightly review's "tripwire" pin could never fail: its needle `candidate: ` is in `for_signal`'s own signature
  (row 30, found by the implementer's false-alarm check);
- the live `ins` rule's trailing window: an off-by-one (11 sessions) passed every test (row 34);
- the C5 no-stop kill rule's rec_id tie-break of the pinned looks was never exercised (row 31);
- the pre-screen's restart hydration of the §2.7 catalyst budget could be dropped with all asserts green
  (row 40); a declined candidate's "stays seen" was unasserted (row 45);
- mom's ex-date skip window reaches day 28 inclusive; the provider test stopped at day 27 (row 35).

## Owner decisions

Each item: what, the evidence, my recommendation. Nothing below is applied on the branch.

### D1 — Parallel runs need the worktree venv first (pytest-xdist, pytest-cov)
- `uv sync --frozen --extra dev` in the worktree was refused by the Claude Code auto-mode permission classifier
  (2026-10-18 01:45, "Modify Shared Resources"). Per the prompt (Phase 0 step 5) no uv was used: the audit ran on
  the live interpreter by absolute path (the root conftest puts the worktree's `src` first — verified), and xdist
  and pytest-cov were never installed. Footprints used the `sys.monitoring` fallback.
- Evidence for the payoff: the shard experiment (speed table): 8 concurrent pytest processes ran the whole gate
  green three times (340.2, 238.9, 223.1 s) and 4 processes once (472.5 s) — no cross-process shared state
  surfaced, which is the main xdist risk, and the gate drops to ~4-6 min.
- Recommendation: use the shard runner now for the pre-commit gate (`runbooks/test_audit/tools/shard_run.py -n 8
  --weights runbooks/test_audit/shard_weights.csv`, see COMMANDS.md) — it needs nothing installed. Adopt
  pytest-xdist only after a trial: add it to the `dev` extra, `uv lock`, `uv sync --extra dev` in the live venv
  (check that `git diff phase3 -- uv.lock` touches only the new packages), then `pytest -n 8 --dist loadfile` three
  times green before trusting it; it schedules per test (better balance than file packing). pytest-cov the same
  way, if wanted.

### D2 — Production seams and fixes (proposals; the audit could not touch production code)
1. **Cache `engine.core.config.repo_root()`** (`functools.cache`; a pure function of `__file__`). ~17 ms per call on
   this host; `load_settings()` calls it twice; test_import_graph spent 57 of 69 profiled seconds in 3,342 calls
   before its test-side fix. Do it — one line, every caller benefits.
2. **`MarketStore.compact_tick_partitions` is dead in production** (review 2026-10-11 #97 kept it). Delete the
   method with its tests, or keep both — not one without the other.
3. **Move `_hard_exit` out of `engine.ops.main`** to a light module: `test_stop_path.py::test_hard_exit_forces_exit_past_a_wedged_nondaemon_thread`
   (10.5 s, the 3rd-slowest test) spawns a child that imports the whole composition root to reach it, and a stub
   child would break the preload order. Do it with the next `ops.main` change.
4. **A testable wiring seam for `engine.ops.main:run`.** 29 of 30 source pins in test_ops_main_wiring.py have no
   behavioural twin; the chaos case04 wiring pin and the unpinned `displacement_margin` hand-off (`ops/main.py:1078`)
   are the same gap. The highest-value refactor on this list for the suite's honesty.
5. **Lazy `engine._preload`.** Keep the chokepoint but defer it: `engine/__init__` installs a `sys.meta_path` hook that
   imports sklearn just before the first `numba`/`llvmlite`/`vectorbt`/`cvxpy`/`skfolio` import. Saves ~5.5 s per
   process that never loads them (every inner-loop pytest run, short scripts); neutral for the full suite and the
   service. No safe test-side equivalent (stubbing the preload would re-open the segfault). Do it, with the existing
   smoke test as the guard.
6. **vectorbt JIT** (informational): see the speed section. Library-level.
7. **Shared helpers for the backtest scripts.** The purged-k-fold fallback, the CNC cost path and the
   missing/locked-database guards are copied into `scripts/backtest_{brk20,hi52,candles,tdc,fade}.py`, so the same
   test exists 2-4 times (six strict shape groups); each pins its own script's copy, so none is a removable duplicate
   today. A `scripts/_backtest_common.py` would let one test each replace ~12.
8. **Host: an AV/EDR exclusion** for the repo and `%TEMP%\pytest-of-<user>` (owner/IT; needs admin to measure).

### D3 — R8: shrink the finished-research test files (owner OK required)
Refuted, closed studies (FABLE_HANDOFF §8; memory `hvol-abnormal-volume-refuted`). The scripts stay — they reproduce
the refutation record — and each test file would shrink to ONE smoke test on its synthetic fixture (the script
runs, the output schema holds, one known number reproduces). These four files were deliberately not sheet-audited.

| script | test file | tests | lines | seconds (baseline) |
|---|---|---|---|---|
| scripts/backtest_fade.py | test_backtest_fade.py | 45 | 932 | 3.4 |
| scripts/backtest_candles.py | test_backtest_candles.py | 50 | 836 | 2.4 |
| scripts/backtest_tdc.py | test_backtest_tdc.py | 37 | 755 | 1.7 |
| scripts/backtest_hvol.py | test_backtest_hvol.py | 101 | 521 | 2.1 |
| total | | 233 | 3,044 | 9.6 |

Also a candidate, listed apart because the harness is an ENGINE module: `test_vwap_reversion.py` (41 collected,
1,085 lines, 1.1 s) pins `engine.learning.vwap_reversion`, whose WO-10 and WO-10b both ABORTED AT STAGE 1 by their
own pre-registration (WORKLOG 2026-08-14); its three G12 P5 flags were deliberately not repaired. Not R8: the
brk20/hi52 backtests (live strategies), backtest.py CLI, sweep/validation, calibrate_fill_model, event_study,
filings_experiments, research_filters/studies, the verdict scripts.
Recommendation: approve for the four refuted scripts (~2.8k lines, ~9.6 s); decide vwap_reversion with its module.

### D4 — One merge the permission system refused (your call)
`tests/unit/test_risk_gate.py::test_modify_stop_unknown_position_rejects` and
`::test_modify_target_unknown_position_rejects` are a copy-paste pair (G04 sheet L1127/L1166) — the planned merge:
`test_modify_on_an_unknown_position_rejects[stop|target]` with `(builder, value)` = `(_modify_stop, "97")` /
`(_modify_target, "105")` and `verdict == "reject"` for both. The Claude Code auto-mode classifier refused the
deletion half ("[Security Test Removal]"); it was not retried or worked around, and the file is unchanged. Allow
the edit (an Edit permission rule) or apply it by hand — or keep the pair; it costs ~6 lines and no time.

### D5 — Changes touching Tier A (strengthen or refactor only; please glance at each)
- Row 17 `test_scope_isolation.py::test_paper_risk_state_takes_the_worst_halt_and_fails_closed` — a third case
  `[FROZEN-cleared-KILLED-ignored]`; the two existing ids and assertions unchanged.
- Row 31 `test_no_stop_verdict.py::test_looks_ordered_by_fill_date_then_rec_id` (C5 kill rule) — an added
  same-fill-date block pins the rec_id tie-break; existing asserts unchanged.
- Row 32 (the hi52 kill criterion's registration-equity test) was changed and then WITHDRAWN: the file is
  byte-identical to phase3 again (see Verification).
- Row 36 `test_stop_geometry.py::test_promotion_is_never_sought_the_module_cannot_reach_the_validation_pipeline`
  (WO-17 import guard) — now also rejects `from engine.learning import validate` and relative forms.
- Structural, same assertions: `test_import_graph.py` (cached lookups, 54a3b39), `test_single_instance.py` (stub
  child, 46323a0 — which also stops the child importing the LIVE tree's `engine` under the documented run recipe).

### D6 — Tier B changes
All 13 removals' survivors and all repaired assertions on Tier B paths were fault-injected (the mutation, the
failing line and the revert are in each ledger row); the Phase 5 review re-injected six of them independently
(all six survivors failed as recorded).

### D7 — Findings outside the suite's reach
- **Coverage gap:** paper GTT legs fire on a TOUCH (`broker.py:1334`, `tick.ltp <= trigger` / `>=`), but every GTT
  test fires on a CROSS (e.g. `test_paper_broker.py:1107`): a `<`/`>` regression on the protective leg passes the
  suite. A ~10-line test parametrized over the two legs closes it. Not added: no existing test claims it.
- **Unhermetic test:** `test_research_studies.py::test_brk20_reference_arm_reproduces_the_registered_baseline`
  needs `data/research/market_*.duckdb` and `data/reports/backtest_brk20_2026-09-12.json` (live, untracked): it
  passes in the live tree (16.3 s) and skips in any clean checkout. Vendor a tiny fixture, or accept a
  live-tree-only check.

## Verification (Phase 5)

- **Full serial runs, all green:** round c (7b1f4f6) audit 4,436 passed / 20 skipped / 4 warnings twice; round d
  (10331d7) audit 4,436 passed / 20 skipped / 4 warnings, base 4,443 / 20 / 4. The skip set is the baseline's 20
  (19 Phase-3/real-side gated chaos rows + the live-data brk20 reference arm); the 4 warnings are the baseline's 4.
- **Tiers run explicitly:** `-m chaos` 69 passed, 19 skipped; `tests/replay` 40 passed; `tests/property` 2 passed
  (max_examples untouched); `npm test` in `dashboard` 92/92 (no packages needed, so no `node_modules` junction).
- **Nodeid reconciliation** (`tools/reconcile_nodeids.py` against `baseline_nodeids.txt` and the final
  `--collect-only`): RECONCILED — 4,463 → 4,456; 50 baseline nodeids removed or renamed away and 45 added, every
  one ledgered; no silent removal.
- **`ruff check tests conftest.py runbooks/test_audit/tools`:** clean (085a24e fixed 4 pre-existing findings in
  untouched test files and the audit's own inventory tool).
- **Fixture-scope checks** (the session-scoped `migrated_db_template` behind `conn` is the one widened scope): the
  whole suite in REVERSED collection order green (4,436 passed, 20 skipped; `-p reverse_order`), and each of the 91
  test files that use `conn` run ALONE — 91/91 green (2,480 passed), none in standby.
- **Four-angle review** (memory `multi-angle-review-catches-interactions`), every finding verified by the manager:
  1. *Protection lost* — an implementer re-injected the recorded fault for five Tier B removals (rows 2, 5, 18, 25,
     44) and one Tier C (row 3): all six survivors PASS clean and FAIL under the fault, as the ledger records.
  2. *Hermeticity* — no changed or added test reaches the network, the live `data/`, the real clock or a path
     outside `tmp_path`; `instant_backoff` covers a superset of every removed `_sleep` patch; the single-instance
     stub child also stops the child importing the LIVE tree's `engine` (it did, under the documented run recipe).
  3. *Fixture scope / order* — no path by which one test's writes reach another (template, caches, merged-case
     parameters). Its side note disproved ledger row 32: `tests/conftest.py:20-55` already pins the capital in a
     session copy of `config/`, so the row-32 re-pin was redundant — **withdrawn and reverted** (10331d7).
  4. *Parametrization* — 19 parametrizations: every case runs, ids match values, no duplicate case, no assertion
     or docstring rationale lost against the originals at 3e6b811.
- **Self-adversarial pass over the removals** (the strongest case that a removal lost something): rows 1, 3, 13,
  23, 43, 44 have survivors with identical or superset assertions; rows 2 and 30 replace source pins (row 30's
  could never fail) with behavioural tests; rows 4, 5, 25 differ only in an input that a fault run showed the
  survivor still catches; row 18's only uncovered lines were generic logging plumbing exercised suite-wide; row
  10 asserted a fake's return value. None of them carried protection the suite lost.

## Sound as-is

The files audited by a candidate sheet and left alone (128 files; per-test sheets in
`..\mt_build_notes\test_audit\sheets\`). Not sheeted: the four R8 files (D3).

- `tests/chaos/test_case03_websocket_drop.py` — 5 collected; sheet G16; no finding survived verification
- `tests/chaos/test_case04_token_expiry.py` — 5 collected; sheet G16; no finding survived verification
- `tests/chaos/test_case05_llm_failure.py` — 9 collected; sheet G16; no finding survived verification
- `tests/chaos/test_case06_broker_rejection_storm.py` — 4 collected; sheet G16; no finding survived verification
- `tests/chaos/test_case11_gtt_fired_rejected_or_unfilled.py` — 2 collected; sheet G16; no finding survived verification
- `tests/chaos/test_case13_holiday_start.py` — 4 collected; sheet G16; no finding survived verification
- `tests/chaos/test_case14_clock_skew.py` — 8 collected; sheet G16; no finding survived verification
- `tests/chaos/test_case15_clean_stop_same_day_restart.py` — 2 collected; sheet G16; no finding survived verification
- `tests/chaos/test_case16_offline_across_eod_window.py` — 2 collected; sheet G16; no finding survived verification
- `tests/chaos/test_case17_offline_multi_day.py` — 6 collected; sheet G16; no finding survived verification
- `tests/chaos/test_case18_cold_start_warmup.py` — 3 collected; sheet G16; no finding survived verification
- `tests/chaos/test_case19_scheduled_start_missed.py` — 3 collected; sheet G16; no finding survived verification
- `tests/chaos/test_case20_news_layer_down.py` — 10 collected; sheet G16; no finding survived verification
- `tests/chaos/test_case21_adversarial_headlines.py` — 9 collected; sheet G16; no finding survived verification
- `tests/chaos/test_case22_lifecycle_notifications.py` — 4 collected; sheet G16; no finding survived verification
- `tests/chaos/test_case23_shutdown_races_tick_flush.py` — 4 collected; sheet G16; no finding survived verification
- `tests/chaos/test_phase3_gated_cases.py` — 8 collected; sheet G16; no finding survived verification
- `tests/property/test_oms_state_machine.py` — 2 collected; sheet G17; no finding survived verification
- `tests/replay/test_golden_day.py` — 34 collected; sheet G17; no finding survived verification
- `tests/replay/test_paper_e2e.py` — 6 collected; sheet G17; no finding survived verification
- `tests/unit/test_agent_defs.py` — 35 collected; sheet G11; no finding survived verification
- `tests/unit/test_agent_harness.py` — 68 collected; sheet G11; no finding survived verification
- `tests/unit/test_alert_episodes_wo25b.py` — 18 collected; sheet G03; no finding survived verification
- `tests/unit/test_api_auth.py` — 3 collected; sheet G03; no finding survived verification
- `tests/unit/test_api_route_auth.py` — 4 collected; sheet G03; no finding survived verification
- `tests/unit/test_backfill.py` — 26 collected; sheet G09; no finding survived verification
- `tests/unit/test_backfill_bhavcopy.py` — 12 collected; sheet G09; no finding survived verification
- `tests/unit/test_backtest_brk20.py` — 33 collected; sheet G17; no finding survived verification
- `tests/unit/test_backtest_cli.py` — 23 collected; sheet G17; no finding survived verification
- `tests/unit/test_backtest_hi52.py` — 32 collected; sheet G17; no finding survived verification
- `tests/unit/test_bar_builder.py` — 32 collected; sheet G09; no finding survived verification
- `tests/unit/test_benchmark.py` — 16 collected; sheet G12; no finding survived verification
- `tests/unit/test_brk20.py` — 24 collected; sheet G13; no finding survived verification
- `tests/unit/test_broker_surface.py` — 5 collected; sheet G05; no finding survived verification
- `tests/unit/test_budget_governor.py` — 64 collected; sheet G11; no finding survived verification
- `tests/unit/test_calendar_horizon.py` — 8 collected; sheet G08; no finding survived verification
- `tests/unit/test_calibrate_fill_model.py` — 35 collected; sheet G05; no finding survived verification
- `tests/unit/test_cat_reversal.py` — 33 collected; sheet G13; no finding survived verification
- `tests/unit/test_cat_scanner.py` — 25 collected; sheet G13; no finding survived verification
- `tests/unit/test_catalyst_digest.py` — 59 collected; sheet G11; no finding survived verification
- `tests/unit/test_catchup_runner.py` — 49 collected; sheet G08; no finding survived verification
- `tests/unit/test_contracts_placement.py` — 16 collected; sheet G05; no finding survived verification
- `tests/unit/test_cost_model.py` — 29 collected; sheet G05; no finding survived verification
- `tests/unit/test_db_transaction.py` — 7 collected; sheet G08; no finding survived verification
- `tests/unit/test_dedupe_news_script.py` — 1 collected; sheet G15; no finding survived verification
- `tests/unit/test_exit_manager.py` — 29 collected; sheet G06; no finding survived verification
- `tests/unit/test_exit_sim.py` — 24 collected; sheet G06; no finding survived verification
- `tests/unit/test_exposure_tracker.py` — 40 collected; sheet G04; no finding survived verification
- `tests/unit/test_features.py` — 35 collected; sheet G12; no finding survived verification
- `tests/unit/test_feed_freshness.py` — 4 collected; sheet G10; no finding survived verification
- `tests/unit/test_filings_events.py` — 7 collected; sheet G10; no finding survived verification
- `tests/unit/test_filings_experiments.py` — 17 collected; sheet G15; no finding survived verification
- `tests/unit/test_fill_model.py` — 31 collected; sheet G05; no finding survived verification
- `tests/unit/test_g2_evidence_budget_window.py` — 5 collected; sheet G15; no finding survived verification
- `tests/unit/test_headline_clusterer.py` — 18 collected; sheet G11; no finding survived verification
- `tests/unit/test_health_wo24.py` — 9 collected; sheet G08; no finding survived verification
- `tests/unit/test_heartbeat.py` — 4 collected; sheet G07; no finding survived verification
- `tests/unit/test_hi52.py` — 15 collected; sheet G13; no finding survived verification
- `tests/unit/test_hi52_edge.py` — 6 collected; sheet G13; no finding survived verification
- `tests/unit/test_holds.py` — 24 collected; sheet G04; no finding survived verification
- `tests/unit/test_import_graph.py` — 19 collected; sheet G04; no finding survived verification
- `tests/unit/test_indicators.py` — 20 collected; sheet G12; no finding survived verification
- `tests/unit/test_ins_crossings_job.py` — 27 collected; sheet G10; no finding survived verification
- `tests/unit/test_ins_scanner.py` — 12 collected; sheet G10; no finding survived verification
- `tests/unit/test_instruments.py` — 14 collected; sheet G07; no finding survived verification
- `tests/unit/test_keep_awake.py` — 6 collected; sheet G02; no finding survived verification
- `tests/unit/test_kill_switch.py` — 4 collected; sheet G04; no finding survived verification
- `tests/unit/test_kite_client.py` — 6 collected; sheet G07; no finding survived verification
- `tests/unit/test_limits_engine.py` — 14 collected; sheet G04; no finding survived verification
- `tests/unit/test_live_impossibility.py` — 8 collected; sheet G04; no finding survived verification
- `tests/unit/test_log.py` — 2 collected; sheet G03; no finding survived verification
- `tests/unit/test_market_store.py` — 43 collected; sheet G09; no finding survived verification
- `tests/unit/test_migration_0015.py` — 6 collected; sheet G08; no finding survived verification
- `tests/unit/test_migration_0016.py` — 7 collected; sheet G08; no finding survived verification
- `tests/unit/test_migration_0017.py` — 1 collected; sheet G08; no finding survived verification
- `tests/unit/test_mode_manager.py` — 20 collected; sheet G04; no finding survived verification
- `tests/unit/test_money.py` — 20 collected; sheet G04; no finding survived verification
- `tests/unit/test_news_scoring.py` — 36 collected; sheet G11; no finding survived verification
- `tests/unit/test_notify_owner.py` — 14 collected; sheet G03; no finding survived verification
- `tests/unit/test_oms_state.py` — 162 collected; sheet G05; no finding survived verification
- `tests/unit/test_oms_store.py` — 19 collected; sheet G05; no finding survived verification
- `tests/unit/test_order_manager.py` — 35 collected; sheet G05; no finding survived verification
- `tests/unit/test_order_surface.py` — 17 collected; sheet G05; no finding survived verification
- `tests/unit/test_paper_composition.py` — 12 collected; sheet G06; no finding survived verification
- `tests/unit/test_paper_control.py` — 8 collected; sheet G06; no finding survived verification
- `tests/unit/test_paper_no_stop.py` — 8 collected; sheet G06; no finding survived verification
- `tests/unit/test_paper_notifications.py` — 10 collected; sheet G03; no finding survived verification
- `tests/unit/test_paper_reconciler.py` — 6 collected; sheet G06; no finding survived verification
- `tests/unit/test_paper_risk.py` — 17 collected; sheet G06; no finding survived verification
- `tests/unit/test_paper_runtime.py` — 36 collected; sheet G06; no finding survived verification
- `tests/unit/test_paper_summary.py` — 10 collected; sheet G06; no finding survived verification
- `tests/unit/test_position_book.py` — 27 collected; sheet G05; no finding survived verification
- `tests/unit/test_post_login_recovery.py` — 19 collected; sheet G07; no finding survived verification
- `tests/unit/test_preopen_planner.py` — 16 collected; sheet G08; no finding survived verification
- `tests/unit/test_protected_config_status.py` — 8 collected; sheet G04; no finding survived verification
- `tests/unit/test_protected_store.py` — 29 collected; sheet G04; no finding survived verification
- `tests/unit/test_protection_manager.py` — 36 collected; sheet G06; no finding survived verification
- `tests/unit/test_protection_reminders.py` — 12 collected; sheet G03; no finding survived verification
- `tests/unit/test_rec_outcomes.py` — 14 collected; sheet G15; no finding survived verification
- `tests/unit/test_reco_pipeline_wo24.py` — 13 collected; sheet G01; no finding survived verification
- `tests/unit/test_reconcile.py` — 18 collected; sheet G09; no finding survived verification
- `tests/unit/test_replay_agents.py` — 3 collected; sheet G11; no finding survived verification
- `tests/unit/test_research_filters.py` — 29 collected; sheet G15; no finding survived verification
- `tests/unit/test_research_studies.py` — 22 collected; sheet G15; no finding survived verification
- `tests/unit/test_retention_and_log_levels.py` — 9 collected; sheet G03; no finding survived verification
- `tests/unit/test_risk_causes.py` — 20 collected; sheet G04; no finding survived verification
- `tests/unit/test_scan_sweep_message.py` — 3 collected; sheet G03; no finding survived verification
- `tests/unit/test_scanners.py` — 40 collected; sheet G13; no finding survived verification
- `tests/unit/test_scorecard.py` — 6 collected; sheet G15; no finding survived verification
- `tests/unit/test_sector_map.py` — 27 collected; sheet G10; no finding survived verification
- `tests/unit/test_session_prep.py` — 17 collected; sheet G08; no finding survived verification
- `tests/unit/test_session_verify_token.py` — 9 collected; sheet G07; no finding survived verification
- `tests/unit/test_single_instance.py` — 5 collected; sheet G02; no finding survived verification
- `tests/unit/test_store_isolation_wo26a.py` — 10 collected; sheet G09; no finding survived verification
- `tests/unit/test_strategy_contracts.py` — 22 collected; sheet G12; no finding survived verification
- `tests/unit/test_strategy_evidence.py` — 5 collected; sheet G12; no finding survived verification
- `tests/unit/test_sweep_signals.py` — 8 collected; sheet G15; no finding survived verification
- `tests/unit/test_tick_compaction.py` — 15 collected; sheet G09; no finding survived verification
- `tests/unit/test_tick_frames.py` — 17 collected; sheet G07; no finding survived verification
- `tests/unit/test_time_exit_check.py` — 16 collected; sheet G06; no finding survived verification
- `tests/unit/test_token_check.py` — 7 collected; sheet G07; no finding survived verification
- `tests/unit/test_universe_builder.py` — 19 collected; sheet G10; no finding survived verification
- `tests/unit/test_validate_insider.py` — 6 collected; sheet G15; no finding survived verification
- `tests/unit/test_validation.py` — 39 collected; sheet G15; no finding survived verification
- `tests/unit/test_vwap_reversion.py` — 41 collected; sheet G12; no finding survived verification
- `tests/unit/test_watchdog.py` — 24 collected; sheet G07; no finding survived verification
- `tests/unit/test_weekly_summary.py` — 16 collected; sheet G03; no finding survived verification
- `tests/unit/test_why.py` — 13 collected; sheet G03; no finding survived verification

## Considered and rejected

Every sheet flag below was checked against the code (and, where stated, a fault run) and kept. Sheet precision:
the Haiku sheets had high recall of suspicions but low precision on R1/R2/R3 (most overlap claims failed on input
class or path); the "claim not exercised" P5 flags were the most productive category.

- G01 R2 `test_reco_pipeline.py::test_a_dismissed_rec_cannot_be_taken` (:1474) vs `:1205` — NOT subsumed: :1474 reaches
  'dismissed' through the real `veto()` path; :1205 sets it by SQL. A veto() writing a state take() does not refuse is
  caught only by :1474.
- G01 R3 `test_reco_pipeline.py:3240` — one weak `>= 0.0` assert inside a test that asserts the whole hot-path stats
  dict; the `_ms` keys feed the session log line. Not vacuous.
- G01 overlap `:3171` vs `:3216` — :3171 alone asserts the real store's read count (`store.bar_reads == 1` after 12
  bars, the F9 defect); :3216 asserts the derived stats counter.
- G02 P1 `test_ops_main_wiring.py:662` vs `:685` — different assertions (job_runs row vs was_run) and distinct
  incident rationale (2026-08-12 live bug); a merge would add assertions or mash two narratives.
- G02 P5 "load_settings is lru_cached, :904 mutation leaks" — FALSE: `load_settings()` (config.py:662) returns a
  fresh Settings per call; the `@lru_cache` at :678 is on `get_settings()`.
- G03 R2 `test_telegram_commands.py::test_recommendation_gap_direction[2500 case]` vs `:1333` — the duplicated case
  completes a 3-row sign-convention table (on / above / below the level); removal saves ~0 s and breaks the spec.
- G03 R2 `test_telegram_commands.py::test_budget_tier_reuses_the_existing_budget_kind` (:1571) — budget_tier hard-codes
  BUDGET_WARNING (catalog.py:913) so it is path-subsumed by the table case (:1543, DG0->DG1), but it is the only
  DG1->DG2 input guarding the docstring's "one event, one kind" invariant; saves 4 lines / 0 s. Kept.
- G09 R1 `test_bar_builder.py:844` vs `:699` (also loose shape group 86ab06a95dd8) — NOT a duplicate: :699 is the
  09-03 post-close (15:40) storm, :844 the 2026-09-09 MID-SESSION (11:47) in-child reconnect; session time is its own
  class, and only :844 shows the grace (not the post-close state) suppresses the page.
- G09 R6 `MarketStore.compact_tick_partitions` — dead in production (only tests call it; review 2026-10-11 #97 kept it,
  documented). OWNER DECISION: delete the method and its tests together (test_market_store.py:571 + the two lock-hold
  coverage uses at :503/:541), or keep both.
- G08 R3 `test_db_transaction.py:64` — NOT vacuous: pins the 2026-10-18 fix (a bare ROLLBACK after SQLite already
  rolled back used to raise "no transaction is active" and mask the real error).
- G08 R3 `test_calendar_clock.py:148` — the "value the test set" is `WEEKDAY_SPECIAL.weekday() == 1`, a guard assert
  on the test data (PROMPT §4 P2 keeps those); the rest is behavior.
- G08 R3 `test_health_wo24.py:257` — one weak "the pulse survived" assert (`:274`) inside a 13-assert episode test.
- G08 R2 `test_catchup_runner.py:193` vs `:211` — :193 pins the 2026-08-18 live bug (MON raised, TUE still ran,
  jobs_failed, was_run(MON) False); :211's first pass asserts only the watermark. Different assertions.
- G08 SLOW `test_health_monitor.py` funnel tests (0.7-1.5 s each) — 121/130 `mon.check()` calls on a fake clock to
  cross the real 120-minute threshold; CPU, not waiting. Shrinking the threshold would change what is tested.
- G03 `test_notifications_journal.py:272` vs `test_telegram_commands.py:1639` — 1 ms tests, different purposes (:272
  guards the pre-WO-24d no-journal path the other file's fixtures rely on). Kept.
- G03 R4 `test_notifications_journal.py:768` — not a source pin: properties of the shipped HTML artefact (calls the
  real route, bearer-authed, no off-box URL) — the page's offline/security posture.
- G02 P5 `test_ops_main_wiring.py:1440` — honest by design: behavioral half checks get_news semantics, the source pin
  ties the production call to those kwargs (docstring says so).
- G06 design question `test_paper_broker.py:1355` (`place_gtt(...) == gid + 1` after a restore that skipped a
  TRIGGERED row with id gid+1) — NOT a production defect: `restore` only promises never to re-mint a RESTORED id
  (`broker.py:925-928`); boot seeds the counter past EVERY stored paper id, any state, via `seeded_counters`
  (`paper_runtime.py:190-200`, Q4.2a), pinned by `test_paper_runtime.py::test_counters_seed_past_every_stored_paper_id_even_terminal_ones`.
- G07 R3/P5 `test_tick_frames.py:441` `test_supervisor_rejects_bad_handshake` — `ticks == []` (:460) cannot fail (no
  tick is sent), but the real guards hold (link closed `data == b""`, `sup._child_writer is None`). A meaningful
  repair needs a tick queued behind the bad hello; every mutation that would make it reachable also stops the
  server closing the link, so the client's `reader.read(1)` hangs in both versions — no clean discrimination. Kept.
- G05 R2 `test_oms_state.py:231/:244` vs `:217` — `:217` pins the literal table, `:231/:244` name invariants that
  survive an intentional (re-pinned) table edit. Kept.
- G05 R2 `test_order_manager.py:254[response-first]` vs `:229` — same input class, subset asserts, but it completes
  the 2-row "either order" table and costs ~0.1 s / 0 lines. Kept.
- Replay tier sharing (PROMPT §5 item 6): ~4 golden-day tests read one default replay and ~2 read the stub-broker
  variant; each replay is ~0.37 s (baseline durations), so sharing saves ~1.5 s of a ~600 s suite — not worth the
  Tier A churn. The replay tier's cost is its session fixtures (synthetic day ~4 s, two-day archive ~8.9 s,
  paper e2e module setup ~10.4 s).
- Unverified lead: `write_synthetic_day` took 8.4 s (profiled) / 13.0 s (plain) wall on the loaded host, but cProfile
  accounts for 0.21 s — the time is outside the profiled Python frames (native DuckDB/parquet writes or AV scanning
  of the files). Not pursued.
- Sheet precision so far: of ~15 R1/R2/R3 flags verified, ~3 held. Sheets are an index, not evidence.
- `test_market_backup.py` interrupt trio (6.5/6.2/3.7 s) — the 10M/5M-row tables are what keep the EXPORT running
  when the interrupt lands; shrinking them trades determinism for speed. Kept.
- `test_stop_path.py::test_hard_exit_forces_exit_past_a_wedged_nondaemon_thread` (10.5 s) — the child imports
  `engine.ops.main` (numba/vectorbt chain), so a stub-package child would break the _preload order. Production
  seam proposal instead (move `_hard_exit` to a light module).

### G12 (strategy a) — verified 2026-10-18 ~09:50–10:05
- R2 `test_vwap_reversion.py::test_vwap_touch_exit` (L299) "subsumed by" `test_exit_fills_at_the_bar_after_its_trigger_bar`
  (L278) — different input class: L299 touches on the HIGH only (close 99.8 < VWAP 100.0 <= high 100.05), L278's
  trigger bar CLOSES above VWAP (100.5). A close-based trigger passes L278 and fails L299. Same lines, so footprints
  cannot see it. Kept.
- P5 `test_benchmark.py::test_previous_session_skips_muhurat` (L74) — the skip under test is `benchmark.py:41`
  (`while calendar.session(prev).is_muhurat`); L76 is the precondition that the CALENDAR does not skip 11-08. Without
  the loop, prev = 11-08 has no bars and `compute_ew_return` raises `ValueError("no usable returns ...")`
  (`benchmark.py:55`), so the test discriminates. Kept (reasoned from the code; not fault-run).
- R7 `test_benchmark.py::test_bench_pct_window` case L108 (`({}, 12-31, 12-31, "stop", 0)`) vs L104 (TUE same-day) —
  same path today (the loop is never entered), but L108 sits on the calendar horizon with L107/L109: an
  implementation that looks one session ahead before the bound check (`add_sessions(12-31)` -> ValueError -> None)
  passes L104 and fails L108. One parameter line. Kept.
- P5 `test_features.py::test_concurrent_snapshots_collapse_into_one_day_context_read` (L790) — the second thread
  starts as soon as the first is inside the read, which it then holds for 0.3 s; thread start is milliseconds, so
  the double-checked lock (`features/engine.py:593-596`) is raced on every normal run. A vacuous pass needs a
  > 0.3 s host stall and can never fail falsely. Sound as-is, fault-verified in Batch C: with
  `with threading.Lock():` (a fresh lock per call = no mutual exclusion) the test FAILED — `E Left contains one
  more item: datetime.date(2026, 6, 17)` (two reads); reverted → PASS.
- P5 `test_strategy_contracts.py::test_each_contract_is_non_empty_and_states_its_evidence_status` (L33) — the label
  check is what the name claims; the per-leg values that carry decisions are pinned by their own tests (hi52
  "FORWARD TEST" L67, ins "ONLY validated edge" L45, orb "evidence status: exploratory" at
  `test_context_assembler.py:754`). A status-vocabulary assert would pin prompt copy. Kept.
- P5 x3 `test_vwap_reversion.py` (L232, L239, L446) — aborted experiment (WO-10/10b); moved to the owner's R8 table.

### G13 (strategy b), G15 (research scripts), G12b (prescreen) — verified 2026-10-18 ~11:00–11:45
Accepted into Batch C: ledger rows 31-45 (G15: 31, 32, 34, 37-39; G13: 33, 35, 36; G12b: 40-45; G12: 30).
Rejected:
- G13 P5 `test_cat_scanner.py::test_tick_rounding_boundary_the_smallest_price_that_still_emits` (L132) — the
  docstring's "smallest reference price" is loose (a raw 0.53 rounds its entry up to 0.55 and emits), but both
  sides of the boundary ON the tick grid are pinned (0.50 refuses, 0.55 emits with a one-tick stop); a rounding
  regression moves one of them. `cat` is SHADOW. Kept.
- G13 P5 x5 `test_stop_geometry.py` (L355 next-open fill, L432 ATR units, L461 inclusive tie, L368 last usable
  bar, L565 orb one-entry-per-session) — the harness of the concluded WO-17 exit-geometry study (verdict
  2026-10-07, promotion explicitly not sought); tightening its fixtures protects nothing that trades, and L565 is
  the refuted orb population. The one ARCHITECTURE claim in that file (L689, unreachable promotion path) IS
  repaired (row 36).
- G15 P5 `test_sweep_signals.py::test_orb_cost_floor_boundary_passes` (L108) — the boundary of the C3 floor for
  `orb`, a strategy refuted twice at retail cost (memory `orb-backtest-structural-negative`; parked 2026-09-12).
- G15 P5 `test_validate_insider.py::test_single_held_session_charges_both_legs` (L71-76) — the gapless fixture
  cannot tell next_open from close_t there, but the convention is ONE branch (`k == ks[0]`,
  `validate_insider.py:136`) shared by every hold length, and it is discriminated by
  `test_next_open_is_the_default_and_excludes_the_entry_overnight_gap` (L90) and
  `test_next_open_only_rebases_the_first_held_session` (L115). The comment overclaims; the protection exists.
- G15 P5 `test_scorecard.py` (L37-51, "taken counts the void rec") — wrong: `actions` counts `human_action`
  ("closed" maps to taken), so the two "taken" are `a` and `e`; the void row is filtered in SQL
  (`scorecard.py:29`). Kept.
- G15 P5 `test_hi52_forward_verdict.py` L364 (cost floor frozen to 1e-6) — intended: that test's own docstring makes
  it the pin that must fail on a cost-table change (L342-344). Kept. (L374 IS repaired: row 32.)
- G15 R4/"may assert nothing" `test_hi52_forward_verdict.py:422-426` render loop — the rows are right-aligned
  `f"{'T+' + str(k):>8}"` (`hi52_forward_verdict.py:487`), so `.strip().startswith("T+5")` matches and the loop
  asserts on all three rows.
- G12b R3 `test_prescreen.py::test_parked_strategy_contributes_no_pending_arm_levels` (L583) — `accepted == []`
  is a trivially true precondition, but the test's protection is its two pending assertions (L593, L596). Kept.
- G12b P5 `test_prescreen.py::test_sweep_suppresses_pending_for_pairs_that_already_published` (L247) — the
  docstring's "a re-break cannot re-enter" is the motivation; the claim under test (pending muted for a published
  pair) is asserted at L266, and the second-sweep case is `test_sweep_scans_with_normal_dedupe_and_reports_pending`.
- G12b P5 `test_prescreen.py::test_burst_ties_keep_arrival_order_and_admission_stays_deterministic` (L424) — any
  deterministic tie-break satisfies §9.6 replay; arrival order and alphabetical order coincide in this fixture
  (A before B), a cosmetic distinction. Kept.

### G16 (chaos suite, Tier A, plan §9.4) — verified 2026-10-18 ~12:30; no ledger rows
- 12 "R3 skip-only placeholders" (case03 L451, case04 L252, …, phase3_gated L25-26): the §9.4 rows gated to
  Phase 3 / the real side (`REAL_SIDE_ONLY`), part of the baseline skip set; they document which rows are open.
  Kept.
- Overlap suspicions between cases (case03 L404 vs L335, case05's shared no-proposal tail, case15/16/17/22
  restart asserts): each is its own numbered §9.4 failure case on a shared rig — Tier A, kept by definition.
- SLOW (14): all but two are predicate-bounded polls (`_until`, `wait_for` on events) that end as soon as the
  condition holds. case03 L415 is a 50 ms settle (negligible). case23 L248 `threading.Timer(1.0, release.set)`
  is the discrimination margin of the CD-6 drain test: the held flush must still be pending when a no-drain
  `stop()` reaches `store.close()`; a shorter timer lets that regression pass on a slow host (PROMPT: never
  loosen a timing margin an assertion depends on). Kept.
- R4 `test_case04_token_expiry.py::test_replicated_wiring_matches_the_composition_root` (L258): pins eight full
  statements of `engine.ops.main.run` (specific, not vacuous) because the chaos rig replicates those closures —
  the same class as the ops.main source pins; production proposal 4 (wiring seam) would retire it. Kept.

### G17 (replay, property, backtest_cli/brk20/hi52) — verified 2026-10-18 ~12:45; no ledger rows
- R1 x9 between test_backtest_brk20.py and test_backtest_hi52.py (and the candles/tdc/fade copies): each test
  pins ITS script's copy of a copied helper — not duplicates of the code under test. Production proposal 7.
- Replay overlaps (golden_day L261/L276; L225/L251/L298/L871 reading one no-broker run): §5 item 6 replay sharing,
  already rejected (~0.37 s per replay, ~1.5 s total, Tier A churn). paper_e2e L473/L488: different runs (`on` vs
  `no_stop`), different input classes. property L145/L393: L393 adds eviction/expiry — Tier A hypothesis, kept.
- P2 x10 / P5 in the replay files: Tier A (refactor only, same assertions) — trimming asserts is out of scope; the
  golden-day 50 ms heartbeat sleeps are documented bounds.

## Unverified leads

- `test_backtest_cli.py::test_default_index_symbol_engages_the_rsi2_regime_filter` (L35) /
  `::test_empty_index_symbol_is_the_explicit_all_regime_optout` (L44): the parser default is pinned, but the
  hand-off to `SweepRunner` (`scripts/backtest.py:408`) and the `or None` normalisation (`:361`) are not — the
  `_Runner` stub discards its kwargs, and L49 recomputes `or None` in the test. Record the stub's kwargs and assert
  `index_symbol` (research CLI; Tier C).
- `test_backtest_hi52.py::test_v2_veto_line_documents_the_gap_count_containment` (L736): greps the disclosure
  text; the containment behaviour it describes (a trigger move above the smooth cap books as `smooth`, never
  `gap`) has no test with a v2 trigger day above 0.07.
- `test_cat_scanner.py::test_defaults_match_the_shipped_settings_block` (L238) claims DEFAULT_PARAMS matches
  settings.yaml's `cat:` block but compares literals and never reads the file. The only discriminating fault is a
  `config/settings.yaml` edit, which the audit rules exclude from fault injection — so not repaired. Fix: compare
  against `load_settings()` (cat v2 is a SHADOW with frozen thresholds, so drift matters).
- `test_filings_experiments.py` e1/e2 "honest_empty" (L253-261, L288-296): the `honest n=0` check sits under
  `if res["n_trades_total"] == 0`, so if the fixture ever yields trades it silently stops checking. Pin
  `n_trades_total == 0` after confirming today's value.
- `test_hi52_forward_verdict.py:393` freezes the Rs 8,000 floor at 0.4343 ± 1e-3 although the module docstring says
  costs are "never a frozen constant" — a costs.yaml re-scrape breaks it for the wrong reason; compare with
  `fv.cost_floor_pct(Decimal("8000"))` instead (Tier A file: owner's call).
- `test_prescreen.py::test_displacement_margin_is_an_owner_knob_in_settings` (L1045): the hand-off of the
  settings value into the pre-screen (`ops/main.py:1078`) is not pinned anywhere — needs the ops.main wiring seam
  (production proposal 4).

## Method and environment notes

- **No worktree venv.** pytest ran with `…\market_trading\.venv\Scripts\python.exe` by absolute path from the
  worktree root; the root conftest puts the worktree's `src` first (verified with an `engine.__file__` check). The
  live venv was never modified; its `.pth` still reads the live `src`.
- **Host standby.** With nobody at the keyboard the laptop enters Modern Standby within ~30 s ("Idle Timeout");
  a process-scoped `SetThreadExecutionState` helper (`mt_build_notes\test_audit\tools\keep_awake.ps1`) held it off.
  Lid closes cannot be (and were not) blocked: they froze three timing runs (flagged and discarded) and one 28-min
  agent run (09:57-10:25).
- **Footprints** (R2's necessary condition) came from a `sys.monitoring` plugin (`tools/footprint.py`, scoped
  LINE events on `src/engine` and `scripts`, ~4% overhead): 4,456 tests, 31,145 lines.
- **Fault injection** was always temporary, in a worktree's `src`/`scripts`, reverted by the exact reverse edit,
  with `git status --short src scripts` empty after every revert.
- **Permission-system refusals** (none worked around): `uv sync` in the worktree (01:45); a source edit I tried to
  make myself in the baseline worktree for a fault check (that check was then run by an implementer in the audit
  worktree's sanctioned flow); the risk-gate test deletion in D4.
- **Delegation.** Candidate sheets: Haiku (17 sheets, ~4,400 tests). Mechanical batches: Sonnet implementers with
  exact edits and faults. Batch D's merges: an Opus designer-impl. Every verdict, every ledger row and every
  review finding was decided and verified by the manager.

## Clean-up

- Keep `C:\Users\piyush.kumar_atari\projects\personal\mt_testaudit` (branch `wip/test-audit`) for the merge.
  `phase3` was at 3e6b811 when the branch was cut; it had not moved at hand-off (checked 14:19), so no rebase was needed.
- Remove the two helper worktrees (no junctions were created in any of them; never `Remove-Item -Recurse`):
  ```powershell
  git worktree remove C:\Users\piyush.kumar_atari\projects\personal\mt_testaudit_base   # detached @ 5788c34, clean
  git worktree remove C:\Users\piyush.kumar_atari\projects\personal\mt_testaudit_c2     # its work is on wip/test-audit
  git branch -D wip/test-audit-c2
  ```
- `..\mt_build_notes\test_audit\` holds the raw material (runs, sheets, inventory, footprints, briefs, patches);
  delete it when no longer needed.
