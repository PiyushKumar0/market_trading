# WORKLOG — autonomous operations log

Entries before 2026-09-01 are in git: `git show 417da77:runbooks/WORKLOG.md`.

## 2026-10-23 15:35 — LLM tier dark ~1.5 sessions: the shared Claude login was blanked; auth alerts collapsed to one episode (owner: "Getting oauth claude failure alerts in the engine. Check and fix")

- **Symptom:** every agent call failed `auth_failed` ("OAuth session expired and could not be refreshed") from the
  10-22 11:41 boot until 10-23 14:32: 103 failures on 10-22 and 70 on 10-23, including `evaluated: 0` in the 10-22
  funnel. That sent about 70 owner messages. The two owner restarts (10-22 11:40, 10-23 12:59) could not help.
- **Root cause:** the stored claude.ai login in `~/.claude/.credentials.json`, shared by the engine's CLI and every
  interactive Claude Code, had both tokens blanked. The last PASS was 10-21 18:12, the last agent_call_ok 10-21 21:47,
  and the 10-22 11:41 boot's `claude_login` check (it runs before `sdk_smoke`) already read blank. The engine made no
  CLI call in that window, so a non-engine process blanked it: a refresh rejected for one of the four long-lived
  VS Code CLI 2.1.301 processes (up since 10-19 / 10-21), or a server revocation that one of them discovered. Which of
  the two is unconfirmed: no CLI debug logs exist, and reading the credentials file is refused by the auto-mode classifier.
- **Recovery:** the owner's VS Code `/login` at 14:32:54 rewrote the file (refresh expiry now 2026-11-19 19:02 IST).
  The engine needed no restart, since each call spawns a fresh CLI: agent_call_ok from 14:35:59 (news 32/32), and the
  15:01 catch-up ran `preopen_planner:2026-10-23` + `nightly_review:2026-10-22`. `intraday_analyst` was already past
  its heartbeat window, so its first post-login call is next session.
- **Owner decisions:** keep the login shared (engine-owned `CLAUDE_CONFIG_DIR` and 1-year `setup-token` both
  declined); collapse the auth alerts.
- **Fix (alerts only):** `ACCOUNT_FAIL_REASONS = {auth_failed}` is one account-wide episode in the harness: the first
  failure from any agent alerts with the re-login command, a reminder follows every 2 h, and the first good call sends
  one "Claude login restored". The pipeline's "Intraday analyst unavailable", the planner's job alert and the nightly
  "review unavailable" skip those reasons. The failure handling, logs and catch-up retries are unchanged.
- **Tests:** 8 new tests, seen failing first. Full gate: 3876 passed, 19 skipped. Not deployed: the next engine boot
  picks it up.

## 2026-10-21 18:20 — No ticks all session: a stale boot-probe verdict marked the fresh Kite token dead (3b0a5a0)

- **Symptom (owner):** the Paper tab read "blocked: today's paper setup hasn't finished yet" all day with the engine up.
- **Root cause:** the 09:06:47 boot binds `/kite/callback` before `SessionManager.verify_token` runs. The probe went
  out on the 10-19 token, the owner's login landed at 09:07:21 mid-probe, and the rejection that came back at 09:07:28
  set `_rejected` on the fresh token. `token_valid()` then read False all day: post-login recovery skipped the ticker
  (`ticker_resume_skipped no valid Kite token`), a needless "Kite login required" prompt went out at 09:07:29, no tick
  arrived, paper prep (which starts only on an in-session tick) never ran, and the 18:07 `bar_reconcile` skipped as
  `no_valid_token`. Alternative ruled out: a bad new token. Historical calls (09:07:28), holdings (09:08:03) and
  instruments (09:08:26) all succeeded on it after the rejection.
- **Fix:** a probe whose token was replaced by a login mid-flight returns `superseded` and leaves state to the login.
  The paper entry guard now names its blocker: construction failed / prep running / waiting for an in-session tick.
  The dashboard maps each text.
- **Tests:** race reproduced for both probe outcomes, seen failing first. Full gate 3868 passed / 19 skipped;
  dashboard 92/92 + build.
- **Deploy:** clock 18:11, log tail held only news polling and health pulses; stop 18:11:14 → boot 18:11:45. Verified:
  `token_probe valid`, `ticker_resumed tokens=302`, `startup_complete needs_login=false frozen=[]`; live `/paper`
  `entry_guard` = "paper session prep is waiting for an in-session tick". Catch-up re-ran `bar_reconcile:2026-10-21`
  (~440 Kite calls ok) until one 7 s `ReadTimeout` from api.kite.trade. The 30-min `catchup_sweep` resumes it.
- **Not fixed (latent, same shape):** the mid-day breaker `on_token_rejected` takes no token, so a live call in flight
  on the old token that fails after a re-login would mark the new token dead too. The fix threads the request's token
  through `KiteClient._call` → hook. Separately, health treats feed `STOPPED` in session as intentional (`problems=[]`
  all day). A stopped feed during a RECOMMEND session paged nothing beyond the one login prompt.

## 2026-10-19 15:00 — Finished docs, research files and stale state removed (owner: "for files and their content which have been implemented, there should not be a need to keep them … remove those content rather than just marking them as done or moving to archive")

- **Rule now:** the code is the record of what is built; `IMPLEMENTATION_PLAN.md` holds only unbuilt work, pending
  verdicts, known defects and open questions, and a shipped item's text is deleted. Everything removed is at
  `git show 417da77:<path>` (CLAUDE.md "Docs"); code comments citing `§x.y` / `Qx.y` / `WO-` resolve there.
- **Repo:** `IMPLEMENTATION_PLAN.md` 620 KB → 15 KB. Deleted: PLAN_PROMPT, AUDIT_PROMPT, IMPROVEMENT_SPEC, the
  runbooks' designs, plans, reviews, audit verification, QuantSync comparison and boot-log investigation,
  `runbooks/briefs/` (5 briefs, 4 applied patches), `runbooks/test_audit/`, `scripts/setup_token.ps1`,
  `config/universe/nifty200_seed.csv`; untracked `TEST_AUDIT_PROMPT.md` and `extract_telegram.py`. RUNBOOK,
  COMMANDS and CHAOS_DRILLS keep recurring procedures only; this log drops entries before 09-01.
- **Fixed in place:** FABLE_HANDOFF's "explicit `model` on every delegation" (contradicted CLAUDE.md's typed-agent
  rule and won by precedence); CLAUDE.md's effort wording; README (O19, setup steps); the rec card's evidence caveat,
  "shipped stop untested (study C1)" → "the shipped stop turned it negative (study C1)" (C1 ran 10-07 after the
  wording was written; test updated).
- **data/:** `data/reports` 250 → 14 files (kept: the three registered evidence reports, the `$syms` sweep file, the
  fill-model calibration, the C1/C2 reports behind pending owner decisions); deleted one-off backups `pre_0015`,
  `pre_0016`, `pre_paper_golive`, `news_preimage_20260803T022317`, three `dashboard-dist-pre-*`, and
  `universe/nifty200_cached.csv`. Kept `pre_0017`, `pre_o19_20261017`, engine-pruned backups and logs.
- **Machine and git:** worktrees `mt_testaudit` and `mt_testaudit_base` removed (no junctions, clean); 22 local
  `wip/*` branches deleted (11 merged; the 11 `wip/cap-*` patch-equivalent in phase3 per `git cherry`); one-time
  scheduled task `mt-engine-deploy-restart-20260805` unregistered. Not pushed.
- **Memory:** 71 → 57 notes (15 deleted as closed or duplicate, stale statuses corrected, a new owner-rule note).
- **Engine:** untouched. The 07:02 boot ran last night's merged code (`native_preload` PASS); the host slept
  07:45–10:05 (tick silence 8,423 s, network errors on resume), not a code fault.
- **Found, not changed:** the watchdog scheduled task is not registered, so nothing alerts when the engine is down
  (IMPLEMENTATION_PLAN Q14); `costs.yaml` fees `verified_on: 2026-06-11` with no recorded re-scrape.

## 2026-10-19 00:10 — A boot crash now logs its traceback (owner: "Fix the issue of boot crash leaving no traceback")

- `engine.ops.main.main()`: `except Exception: _log.exception("engine_crashed")` before the `finally: hard_exit(code)`;
  `os._exit` had skipped the interpreter's traceback print, so a crash in `run()` left only `exit_clean` (REPORT D7).
  Exit code stays 1. New test in `tests/unit/test_stop_path.py`; it fails on the old code. Takes effect at the next
  mt-engine restart (still running the pre-merge code).

## 2026-10-18 23:42 — `wip/test-audit` merged into phase3 (owner: "merge changes to phase3")

- Fast-forward 3e6b811 → f9b9c0c (no merge commit; nothing pushed). The live tree imports the merged engine
  (`engine.ops.main.compose` present, import guard armed).
- mt-engine was left Running on the OLD code: the boot-path changes (compose / _boot / _shutdown, lazy sklearn guard,
  stop_path) take effect at the next restart — do it outside 09:15-15:30 and check `engine_ready` and the self-test's
  `native_preload` PASS.

## 2026-10-18 23:15 — Test audit: owner decisions applied on `wip/test-audit`, not merged (owner: "Proceed with D1, D2, D4, D5. Review and validate. … For D3, implement the change, and also keep such test cases separate from other rerunnable ones …")

- **Branch `wip/test-audit`** (worktree `..\mt_testaudit`, phase3 still 3e6b811): 23 commits on top of the
  audit. Report: `runbooks/test_audit/REPORT.md` "Owner decisions applied"; every test change with its evidence in
  `runbooks/test_audit/LEDGER.md` rows 63-115. Nothing pushed or merged.
- **Done:**
  - D1: pytest-xdist in the dev extra; it replaces the shard runner.
  - D2.1/D2.2/D2.3/D2.5/D2.7: repo_root cache; dead compaction method deleted; light stop-path module; lazy sklearn
    guard; one copy of the backtest helpers.
  - D2.4: `run()` split into compose/boot/shutdown; every source pin on it is now a behavioural test (76 mutants
    caught).
  - D2.8: the Defender exclusion is written down in COMMANDS, not applied.
  - D3: `tests/offline` holds the tests of non-production code, run on demand; four refuted studies keep one
    smoke test each.
  - D4: a risk-gate pair merged. D5: rows re-validated by fault injection.
- **Numbers:** the gate (a bare `pytest`) is 3,884 tests, all green; `tests/offline` 358 (357 passed, 1 skipped).
  `pytest -n 8 --dist loadfile` median 177 s against the baseline suite's 271 s (−35%); serial 677 s against 768 s
  (one 1,139 s run right after a standby resume, reported as host noise). A pytest process starts in ~4 s instead of
  ~15. Every test change reconciles against the baseline nodeids (4,463 → 4,242); mypy and ruff: no new finding.
- **Review:** three independent reviewers; every finding verified and fixed.
  - The first lazy guard imported sklearn under CPython's global import lock (stall + deadlock reproduced; fixed).
  - Production behaviour had been left pinned only offline (back in the gate).
  - The reconcile tool could miss a dropped parametrized case (made strict).
  - Eight replaced pins had let a mutant through (closed).
- **Live venv:** pytest-xdist 3.8.0 + execnet 2.1.2 installed with `uv pip install` (the editable install
  untouched).
- **Not touched:** `mt-engine`, `data/`, `config/limits.yaml`, `config/envelope.yaml`, the live tree.
- **Found, not changed:** a crash in `run()` exits without a traceback, because `main()`'s `os._exit` comes first
  (REPORT D7).

## 2026-10-18 15:45 — Test audit: `wip/test-audit` ready for review, not merged (owner: "Read @TEST_AUDIT_PROMPT.md and work on optimising the test coverage")

- **Branch `wip/test-audit`** (worktree `..\mt_testaudit`, on phase3 3e6b811 — phase3 has not moved): tests and
  runbooks only; `src/`, `scripts/`, `config/`, pyproject and uv.lock untouched. Report:
  `runbooks/test_audit/REPORT.md`; every change with its evidence: `runbooks/test_audit/LEDGER.md` (62 rows).
- **Numbers:** 4,463 → 4,456 tests; 84,240 → 84,208 test lines; the gate's serial median 622.5 → 534.7 s (−14%;
  back-to-back pairs −10% and −27% on a busy host), 223-340 s as 8 file-shards; every run green (4,436 passed, 20
  skipped — the baseline skip set — and the baseline's 4 warnings).
- **What changed:** 13 tests removed (every survivor fault-checked), 28 repaired (each passed while blind to the
  fault its name claims; each repair shown to fail under it), 17 copy-paste groups parametrized, 1 assert trim;
  speed-ups: `conn` from a session-migrated SQLite template, cached import-graph lookups, a stub single-instance
  child, a shared `instant_backoff`, a batched 501-symbol seed.
- **Verification:** the reversed-order full run and the 91 `conn` files alone all green; nodeids reconciled; ruff
  clean; chaos/replay/property and the dashboard's `npm test` run explicitly; a four-angle review found nothing,
  and its side note withdrew one of my changes (row 32 — tests/conftest.py already pins the capital).
- **Owner decisions** (REPORT): D1 xdist needs the venv (`uv sync` in the worktree was refused by the permission
  classifier; the shard runner works without it); D2 eight production proposals (lazy sklearn preload ≈ −5.5 s per
  pytest start, an ops.main wiring seam, the `repo_root()` cache, …); D3 shrink four refuted-research test files
  (≈ −3,000 lines); D4 a risk-gate test merge the permission system refused; D7 an untested paper GTT touch trigger.
- **Not touched:** `mt-engine` (Stopped all session, as found), `data/`, `config/limits.yaml`, the live venv.
  Helper worktrees `mt_testaudit_base` and `mt_testaudit_c2` (+ branch `wip/test-audit-c2`) remain until review —
  removal commands in REPORT "Clean-up".

## 2026-10-18 00:45 — limits.yaml comments patch applied, transaction() fixed, build worktrees removed (owner: "1. Apply limits.yaml patch 2. Fix the transactions() bug 3. Remove extra unnecessary worktrees")

- **limits.yaml comments patch (058e36e):**
  - applied with the engine stopped, after a read-only `git apply --check`;
  - comments only: the parsed YAML and the LimitTable are identical, and the file is LF;
  - reseed: `5d8b52cb` → `2308f007`; `protected_config_status` OK and `ProtectedStore.verify` True;
  - envelope unchanged (`67d9c1ea`).
- **`core.db.transaction()` (8045d60):**
  - The defects:
    - a failed COMMIT left the transaction open on the shared connection, so every later BEGIN failed (the kill switch's persist included);
    - a ROLLBACK after SQLite's own rollback masked the real error ("no transaction is active").
  - The fix: COMMIT moved inside the `try`; ROLLBACK runs only while `in_transaction`; a failing ROLLBACK is logged CRITICAL and never raised. BEGIN stays outside the `try`.
  - Tests: 7 new, on real SQLite (a deferred FK fails the COMMIT); 3 of them fail on the old code. `test_oms_store`'s proxy forwards `in_transaction`.
  - An independent opus review approved it: none of the 47 call sites depends on the old behaviour, and its minors are folded in.
  - Full suite 4443 passed, 20 skipped (10m03s, run alone).
  - Boot check 00:31: selftest all PASS (`protected_store:limits.yaml` on the new signature), `startup_complete`, `engine_ready`.
  - Its one ERROR was a Telegram `TimedOut` on a shutdown-time send (network, unrelated).
  - Engine **stopped** again at 00:40 (owner's state).
- **Worktrees removed:** `mt_cap_probe`, `mt_cap_t1`, `mt_cap_t2`, `mt_cap_t3`, `mt_cap_t4`, `mt_cap_t5`, `mt_cap_t5b`, `mt_cap_t5c`, `mt_cap_t6`, `mt_capital`, `mt_nostop` and `mt_wip`.
  - Every one was clean and fully contained in `phase3` (`git cherry`: 0 unpicked). The probe's only change was its throwaway 60k `limits.yaml`.
  - Each `.venv` and `node_modules` junction was unlinked with `rmdir` first; the live venv and `node_modules` were verified intact.
  - Their branches remain (all contained in `phase3`).

## 2026-10-17 23:50 — O19 capital change from the dashboard merged into phase3 (owner: "merge the changes to phase3")

- **Built** on `wip/capital-change` (17 commits on 179cd45, now fast-forwarded into `phase3` at 2b74d77):
  - the "Change capital" dialog (System tab, Risk limits);
  - `GET/POST /config/capital`;
  - base-agnostic brakes in P&L space, with no new table;
  - one live base through `LimitsEngine.reload()`;
  - the hi52 edge capped at the live capital;
  - a hardened, compare-and-swap `ProtectedStore.owner_update`.

  Spec: `runbooks/design-2026-10-16-dashboard-capital-change.md`.
- **Review:** every task was reviewed. Then a four-angle whole-branch review ran, followed by one fix wave and a scoped re-review.
  - The critical catch: after an out-of-session restart, the brake check had priced open positions at entry. It now uses the live price, else the last stored 1m close, else a 409 refusal.
  - Also fixed:
    - `test_capital.py` no longer reads the live limits.yaml;
    - the docs say a revert KILLS when a real position is open;
    - the rollback recipe keeps the signed file;
    - the comments patch applies after a change;
    - POST no longer waits on Telegram.
- **Validation:** the full suite at 2b74d77 gave 4436 passed, 20 skipped (11m00s, run alone). Dashboard 92/92 and `tsc` are clean. No browser check was possible: the MCP browser servers were unavailable.
- **Deploy:**
  - The engine had been stopped by the owner at 02:14.
  - Backups: `data\backups\pre_o19_20261017.db` (integrity ok, 41 tables, limits 5d8b52cb, envelope 67d9c1ea) and `data\backups\dashboard-dist-pre-o19-20261017`.
  - `phase3` was fast-forwarded and `dist` rebuilt.
  - The boot at 23:47:
    - selftest all PASS (`token_valid` WARN: weekend, login needed), including `protected_store:limits.yaml`;
    - `paper_composed`, `startup_complete`, `engine_ready`;
    - 0 ERROR lines;
    - `/config/capital` and `/paper` answer 401 without a token.
  - **The engine was stopped again** to keep the owner's state (ruling R14). Start it before Monday's session.
- **Open:**
  - The owner-applied `limits.yaml` comments patch (COMMANDS, "Capital change from the dashboard").
  - Recommended as a separate change: `engine.core.db.transaction()` does not roll back when COMMIT raises.
  - Rulings R1–R21 are in the build ledger, `..\mt_build_notes\sdd-capital\progress.md`.
  - The task worktrees `..\mt_cap_*` and `..\mt_capital` remain. They hold `.venv` and `node_modules` junctions: remove each junction with `rmdir` first, never `Remove-Item -Recurse`.

## 2026-10-16 18:10 — Review findings fixed, cleanup pass, P&L percentages (committed, NOT deployed)

- **/code-review of the 30 commits ahead of origin (14 findings):**
  - **Fixed:** reconcile retries a symbol logged with zero official bars; the C5 verdict looks are cut from the first 20/40 *filled* recs and form only when all are eligible (plan "C5 correction 2026-10-16", at eligible n = 4, before any look formed); a ticker child that outlives a denied terminate has its link closed; `/rearm` integrity guard; equity-chart crosshair clamp; re-arm toast; `TIME_ONLY_POLICY` constant; unknown `paper.no_stop_strategies` ids logged at boot; the login warning prints the resolved CLI path.
  - **Skipped:**
    - The auth regex was replayed over all 547 logged agent failures (91 log files): it matched exactly the 15 real auth failures.
    - The two surveillance filters are identical today.
    - The double universe read is about 1 read in 750 per sweep.
    - The blocking `close()` is the last shutdown step.
  - **Withdrawn after the cleanup review:** finding #4 (resend a failed reconcile alert). The production `notify` sink catches every exception and the Telegram outbox retries, so the fix was dead code.
- **Cleanup pass (4 review angles):**
  - The ticker link is closed once, in the read-loop teardown.
  - One `IntegrityError` handler in the API answers 503 on every route; it also fixes `/risk/headroom` and `/config/params`.
  - The Telegram owner guard replies `command failed: …` instead of going silent.
  - The no-stop id check uses `SCANNER_REGISTRY` plus the batch legs; `rsi2` was a false positive.
- **P&L percentages (owner-approved design):** every ₹ P&L figure carries its percent (`engine/core/money.py` `pct_of`).
  - Account figures are % of capital base; trades are % of entry value; open positions stay labelled "before costs".
  - Covered: the dashboard, Telegram `/pnl` and `/limits`, and the Scorecard paper net.
- **Validation:** failing-first tests for each fix. Full suite 4283 passed, 19 skipped (13m36s, run alone). Dashboard 79/79, `tsc` clean.
- **Not deployed:** the engine still runs 24bbe7c code, and `dashboard\dist` is not rebuilt. Both wait for the capital-change build (`runbooks/design-2026-10-16-dashboard-capital-change.md`).
- **Found during the build:** on 2026-09-07 the mid-session base raise (20k → 40k at 12:55) moved recorded day MTM from −₹185 to +₹19,816 for the rest of the session. The −5% day-loss rung was effectively off that afternoon. The capital-change design fixes the cause (day loss and drawdown computed in P&L space).

## 2026-10-15 16:32 — D14 paper no-stop test (C5) and the ticker spawn-failure fix deployed (owner: "Go with option C and validate")

- **Evidence before building (disclosed in C5):**
  - On the same live recs the no-stop exit was not better so far: at T+10 the paired median was −0.21 pp (6 of 12 better); at T+20, 0 of 3 better (2 worse, 1 tie).
  - Time-only excess over the EW benchmark was about 0 (T+5 −0.50%, T+10 +0.04%) while the benchmark fell 6.7% from 08-27 to 10-14.
  - The 10-14 status claim that "the backtest and the live record agree" overstated the live evidence. The owner has been told.
- **Pre-registered** C5 in `IMPLEMENTATION_PLAN.md` §6.1 (7f46bb9) before any code. A clarification followed before the deploy, at eligible n = 3: looks pinned to the first 20 and 40 eligible recs by fill date (look 2 supersedes), disclosure corrected, restart and end semantics defined.
- **Built** on `wip/paper-no-stop` (worktree `..\mt_nostop`), 5b8b7bf:
  - Migration 0017 adds `positions.exit_policy`, and `paper.no_stop_strategies` is `[hi52, ins, brk20]`.
  - A listed strategy's paper position opens with no stop or target, `time_only` and `TIME_EXIT_ONLY`, but only when its exit session is known. Fills from 12-04 keep their stop until `2027.yaml` loads.
  - ProtectionManager skips these positions. A NULL stop without the policy still fails closed.
  - Copy updated in `GET /paper`, the dashboard and `/positions`. New `scripts\no_stop_verdict.py`.
- **Ticker supervisor** (e0d3018, promised 10-14):
  - The incident: on 10-12 at 20:12, CreateProcess was denied in Modern Standby, and ticker supervision died silently.
  - Now a failed spawn goes STALE and retries with backoff. A denied terminate, the `start()` path and the backoff overflow are fixed too.
- **Validation:**
  - Failing-first tests. Full suite 4252 passed, 20 skipped (10m40s, run alone).
  - Dashboard 76/76, `tsc` clean, fixture-server DOM sweep clean on every tab at desktop and phone width.
  - Three-lens Opus review found 3 medium and a set of low findings, all fixed: unknown exit date, denied terminate, and optional stopping in the verdict's labels.
  - The verdict run read-only on the live DB reproduces the interim figures and prints INSUFFICIENT (3 of 20).
- **Q4.13 go-live checks complete:** 6 sessions (10-08 → 10-15) clean.
  - 0 real orders, 0 Kite order/GTT calls, 0 PAPER_ALERT.
  - Prep ran once per session plus once per gap, never failed.
  - Each paper fill got its GTT within 0.02 s (MOTILALOFS 10-13, PVRINOX 10-15).
- **Deploy:**
  - 16:29:31 `exit_clean`. Backup `data\backups\pre_0017.db`: integrity ok, 41 tables, schema 0016.
  - In the stop window, the hi52 forward verdict printed INSUFFICIENT: T+20 n 3, median net −7.89%, hit 0%; T+10 n 14, −5.12%; raw returns, no benchmark.
  - `phase3` fast-forwarded 7f46bb9 → e0d3018 and `dist` rebuilt.
  - Boot: `migrations_applied_on_boot [0017]` at 16:30:36, `paper_composed`, selftest all PASS, `startup_complete` 16:32:36, feed HEALTHY 16:32:38, `engine_ready` 16:32:43. Post-arm jobs had no failures; 0 ERROR lines since 16:30; `exit_policy` present; `GET /paper` without a token answers 401.
- **Open:**
  - MOTILALOFS (hi52) and PVRINOX (brk20) opened before the deploy and keep their stops; they are outside (P). The first test position is the next hi52/ins/brk20 paper fill from 10-16.
  - Pre-existing: a running ticker never receives a new Kite token (moot with daily restarts).
  - The worktree `..\mt_nostop` remains, with `.venv` and `node_modules` junctions; never `Remove-Item -Recurse` it.

## 2026-10-12 00:55 — Envelope #6 applied + re-signed; stale NSSM env cleared (owner: "Run the envelope patch. I have run the stale service variable remove command")

- **NSSM:** owner ran `nssm reset mt-engine AppEnvironmentExtra`; verified — the Parameters key has no `AppEnvironmentExtra` (takes effect at the next service start).
- **Envelope #6:** pre-flight (Mon 00:53, engine Stopped, clean tree, `git apply --check` exit 0) → `git apply runbooks\briefs\envelope_retire_v1_cat_2026-10-11.patch` (removes the unread `cat.confirm_vol_mult` + `cat.max_hold_days`) → `seed_protected_config.py --yes --reseed --note "review 2026-10-11 #6 …"`: envelope.yaml `67d9c1ea…` (new), limits.yaml `5d8b52cb…` (unchanged). The auto-mode classifier allowed the reseed this time (it refused one on 2026-09-23).
- **Verified before any boot:** the self-test's `protected_store:*` check on the real config + state.db (read-only) → both PASS; `load_cat_params` still returns its full key set; 215 envelope-dependent tests green on the live tree. Engine NOT started.

## 2026-10-12 00:05 — The 11 open gaps from the stale-logic pass fixed (owner: "Fix the issues you had flagged but not done like: Analyst calls still receive extra context and the other gaps")

- **Method:** 8 read-only investigators (one live-tested the CLI) → manager decisions → 6 worktree-isolated implementers + 1 dashboard agent → 3-way patch merge → full suite → 8-lens review → 2 fix rounds, each adversarially re-verified (incl. a 648-case old-vs-new decision-table probe of catch-up reporting). Engine stopped throughout (Sunday).
- **D10 context leak — fixed.** `setting_sources=[]` never stopped the cwd-keyed auto-memory (MEMORY.md, ~4.7K tokens/call) or the account's claude.ai connectors. Harness now sends `ISOLATION_ENV` (`CLAUDE_CODE_DISABLE_AUTO_MEMORY=1`, `ENABLE_CLAUDEAI_MCP_SERVERS=false`) + `strict_mcp_config`, refusing the real SDK without them. Live-verified (transcript 6a5e5387): no memory, no connectors, tools = [StructuredOutput]. **Residual:** the owner's email still arrives as `session_context` — CLI 2.1.288 has no opt-out while the stored claude.ai login is used.
- **OAuth — decided Option B:** the pinned CLI's stored login IS the design (documented; DPAPI injection would let a never-used token override a working login with no fallback). New: selftest `claude_login` check + daily `claude_login_check` 08:10 job (WARN ≤7 days before refresh expiry / blank / missing) and `fail_reason=auth_failed` with the re-login command. Facts surfaced: the login is the **Atari Team seat shared with interactive Claude Code**; refresh token expires **~2026-11-08 05:03 IST**.
- **Muhurat — live engine skips special sessions** (`NSECalendar(include_special=False)` in the engine, the ins CLI and the watchdog predicate). Before: Sun 2026-11-08 would have run the whole job set incl. the LLM pre-open chain on the 09:15–10:15 placeholder. Per-day BarBuilder bounds deliberately not wired (constants hold on every day the engine runs). COMMANDS has the post-11-08 bhavcopy backfill.
- **Reconcile — retry + a live data-loss bug.** Re-runs were wiping the drift audit (09-15: 119 symbols; 08-28/10-05/10-08: every symbol, scheduled-vs-sweep race). Now: single-flight lock, per-(day, symbol) resume, bars + log row in ONE transaction per symbol, `alerted` set only after the alert is handed off (a resume re-alerts the unsent), token-abort ⇒ `ok=False` ⇒ swept after re-login, dead token ⇒ no broker call. Lost audit rows for those four days are NOT repaired.
- **DuckDB backup — new `market_backup` 22:00 job:** `EXPORT DATABASE` on a store cursor, no store lock (measured 43.9 s / 2.11 GB / 25 tables; IMPORT round-trip identical). CompactionLane; never starts 06:00–15:45 on a trading day; 6 h hold after a failure; keeps newest 3 + weekly ×4 with a 3× free-disk guard. On-time lane fires no longer send catch-up reports (they would have sent ~2 false "Missed-job catch-up complete" a night); missed days and recoveries still report. New `scripts/rewind_after_market_restore.py` (dry-run default, `--engine-crashed` lock-checked) + rewritten RUNBOOK restore.
- **A8 held-position alert — implemented:** `SURVEILLANCE_MIGRATION` (critical) for every OPEN real position on GSM/ASM/T2T/ESM, once per (position, list) via the journal dedupe; covers offline gaps.
- **Config:** HeadlineClusterer reads the envelope row `cat.max_event_age_days` (settings key removed; calendar-day units kept — byte-identical during the cat v2 shadow). `weekly_drawdown.rolling_sessions` now flows to the ladder (5 = unchanged; <1 clamps with one ERROR). Envelope #6 retirement prepared as an owner patch.
- **Also fixed:** `protected_store_integrity` freeze had no clear site (a botched reseed froze entries forever) — a clean boot now clears it. Dashboard dropped the four retired v1 cat level columns (dist backed up to `data/backups/dashboard-dist-pre-cat-levels-20261011`, rebuilt, headless-Chrome verified).
- **Validation:** full suite 4218 passed / 19 skipped / 0 failed (24 min, alone); dashboard 76/76; live CLI check above.
- **Owner steps:** (1) apply `runbooks/briefs/envelope_retire_v1_cat_2026-10-11.patch` + reseed (COMMANDS protected-config); (2) elevated `nssm reset mt-engine AppEnvironmentExtra` (COMMANDS); (3) re-login the CLI before ~11-08 05:03 IST; (4) muhurat backfill after 11-08. **Open:** the email residual; whether the engine should run on the Team seat; `job_daily_bars` records success even when every fetch failed (pre-existing, found today). Engine not restarted — the next boot deploys all of this.

## 2026-10-11 18:25 — Stale-logic review verified and fixed (owner: "Verify each finding against current code. Fix only still-valid issues … keep changes minimal, and validate")

- **Scope:** all 185 entries of `runbooks/review-2026-10-11-stale-logic.md` (treated as claims, not instructions), re-verified against the tree. **167 fixed, 15 deliberate and 2 refuted re-confirmed, 1 owner item** (#6, envelope.yaml rows with no reader — protected store, not edited). Per-finding ledger: `runbooks/review-2026-10-11-resolution.md`. Uncommitted: 143 files, mostly comments/docs; limits.yaml / envelope.yaml untouched; another session's in-flight Haiku 5.5 hunks (agents.yaml, harness.py, smoke_test.py, two tests) preserved as found.
- **Runtime changes (all verified):**
  - #87 (high) harness sends `tools=[]` — `allowed_tools` only auto-approves in SDK 0.2.159, so analysts had been offered Skill/ToolSearch/Workflow + deferred Cron*/SendMessage. One live sdk_smoke call through the pinned CLI (throwaway DB): StructuredOutput still works; transcript tool list = `[StructuredOutput]` only. Deny list extended. Takes effect on the next engine boot.
  - #56 (high) gate `surveillance` rule and `/why` read the A8 `surveillance_*` codes from `universe_daily` (the never-written `instruments_daily.surveillance` column stays NULL; plan §4.3 annotated). No verdict can change: a flagged row is never eligible.
  - #58 (high) KiteClient reads pass `intent="read"`: paced, never capped or counted against the 70/day entry budget (latent; live routing unbuilt).
  - #116 unverified-limits boot fallback base ₹20,000 → ₹40,000 (pinned by a test against limits.yaml); #113 STARTUP_REPORT no longer lists the halt-ladder re-eval as deferred (it runs); #61 `NSECalendar.ex_dates` raises instead of a silent `[]` (no callers).
  - **Prompt text:** #78 retired v1 cat levels (confirm_trigger/invalidation/stop/target bands) no longer rendered to the intraday analyst or pre-open planner (still persisted for the dashboard); #81 batch-leg note lists brk20/hi52/ins/cat/cat_reversal; #145/#155 hi52 contract says "NIFTY 500 core". Population measures (hi52 forward test, cat v2 shadow) are scanner-level, unaffected.
  - No-op at shipped config: ins `stop_pct` default 6 → 5; brk20 `rr_target` no longer `int()`-truncated; unused `source` param / `no_exit_routine` removed. Scripts: smoke_test checks the pinned CLI (offline, no engine import); nssm_install sentinel machinery removed (not run). Dashboard: glossary `platform`, PaperTables state names.
- **#24 settled:** the 2026-09-15 01:14 "crash" was a host reboot (Application log: nssm `SHUTDOWN control`; EventLog 6006/6005), owner start 10:06. NSSM restarts non-zero exits after 15 s; reboots stay down (DEMAND start). FABLE_HANDOFF §8 corrected.
- **Validation:** full suite 4049 passed / 19 skipped / 0 failed (13m11s, alone); chaos 69 passed / 19 skipped (CHAOS_DRILLS status updated); dashboard `npm test` 74/74; ruff clean on every added line. Whole-diff review (7 lenses) found 0 high, 27 wording defects — all fixed.
- **Open (owner decisions, not done here):** D10 leak — every engine SDK call still gets this repo's auto-memory, the owner's email and the account's claude.ai MCP connectors despite `setting_sources=[]`; OAuth-token injection is documented but unimplemented (implement vs adopt the CLI login; plan §2.2 annotated); muhurat 2026-11-08 session bounds unwired (BarBuilder/reconcile use 09:15–15:30); a token-aborted reconcile day is never retried; DuckDB has no backup path; A8 held-position migration alert unimplemented; `cat.max_event_age_days` split across settings/envelope; `weekly_drawdown.rolling_sessions` unread (mirror test added); envelope #6; dashboard still labels the v1 cat levels "Trigger price"/"Cancel price"; installed service still carries `MT_RESTART_SENTINEL` (`nssm reset mt-engine AppEnvironmentExtra`).
- Engine not restarted.

## 2026-10-09 03:00 — `hvol` abnormal-volume study: REFUTED (owner: "Do we consider stocks … high trade volumes?" → "Yes test it and validate")

- **Answer to the question:** no rule originates on volume today; volume is only a confirmation gate in `orb` / `brk20` / `hi52` and analyst context.
- **Pre-registered** in `IMPLEMENTATION_PLAN.md` (`hvol`, commit a0935b7) before any code; amendment A1 (exclude ETFs — 388 symbols, 62 of them above the ₹5cr floor; current NIFTY 500 list) committed b6df9fb before the full run. Harness `scripts/backtest_hvol.py` built by a designer-impl agent, audited (signal windows, benchmark, bootstrap), committed 275fbaf.
- **Run** 02:51 on `data/research/market_2026-10-07.duckdb` (engine untouched): T+20 n=7,574, mean net +0.50% but matched-benchmark excess **−0.41% [−0.65, −0.17]**, median gross −0.12% < 0.32% cost. 4/8 criteria fail → **REFUTED**. All variants negative excess; results-day spikes −1.10%; smaller names worse.
- **Verified** by an independent manager-written implementation: n, mean/median gross and the benchmark mean reproduced exactly.
- No origination change. Delivery-% leg not tested (no stored delivery data).

## 2026-10-08 20:45 — O18: universe → NIFTY Total Market (owner: "Increase the universe to Nifty 1000")

- **Premise check:** NSE has no Nifty 1000 (`ind_nifty1000list.csv` → 404). Owner picked **NIFTY Total Market** (~750) over "all criteria-passing NSE stocks" (~918), and **kept the tick cap at 300**.
- **What:** eligible index → Total Market; a NIFTY 500 **core** list stamps `universe_daily.core_index`. hi52 / ins / cat / cat_reversal / the rec benchmark read `get_core_eligible_symbols` (their evidence population); brk20, the gate and the news/batch layer read the full eligible set. Plan of record: `IMPLEMENTATION_PLAN.md` O18 (after O15).
- **Validation:** failing-first checks on the ins and benchmark pins (both fail against the old call sites); full suite 3932 passed, 19 skipped; ruff clean.
- **Deploy 20:42–20:46:** commit 5237342. `nssm stop` → `scripts/build_universe.py --date 2026-10-09` (migration `universe_daily_column_added core_index`; `universe_built` index 745, **eligible 687** (was 478), core_eligible 478, watchlist 300, mis_candidates 198 (was 207), extended 237 (was 440), source download, not degraded) → `nssm start`. Boot clean: selftest all PASS, `startup_complete` 20:45:39, ticker 302 tokens, `engine_ready` 20:45:45, RECOMMEND, NORMAL, 0 ERROR/CRITICAL. The 08:30 job rebuilds 10-09 (replace-write).
- **Open (owner):** the 300-name tick cap now ranks over Total Market, so high-turnover microcaps displace some NIFTY 500 names (mis_candidates −9). Not counted directly: the engine holds the DB. Offer: rank ticks over the core set only.

## 2026-10-08 19:50 — Floor re-arm shipped (owner: "Go with implementing first option and have the re-arm button on dashboard as well")

- **What:** `/rearm`, `POST /risk/rearm` and the dashboard alert's Re-arm button clear each `floor_<rung>` cause the engine's book is back above (`ExposureTracker.rearm_floors`). A still-breached rung stays latched; the kill switch (`/kill_reset`) and a forced-down mode are untouched. `/risk/headroom` reports `floors` (cause → re-armable). Plan of record: `IMPLEMENTATION_PLAN.md` RiskState "Floor re-arm".
- **Validation:**
  - failing-first tests: exposure, API route, Telegram command, dashboard alert;
  - full suite 3925 passed, 19 skipped; dashboard 74 pass;
  - browser (ALERTS fixture): button, confirm dialog, toast, alert after re-arm; layout sweep clean at 1440 and 320 px.
- **Deploy 19:50–19:53:** commit 3037181, restart. Boot clean: `engine_ready` 19:53:32, RECOMMEND, NORMAL, not killed, `crash_recovered: false`, 0 ERROR/CRITICAL. `POST /risk/rearm` answers 401 without a token, and no causes are active, so the button stays hidden.

## 2026-10-08 18:06 — Dashboard redesign shipped (owner: "update the design of the dashboard to make it more user friendly … Review and validate each process"; chose "Tabbed workspace")

- **What changed:**
  - The single 12-panel page is now six tabs: Overview, Recommendations, Paper trading, Performance, Activity, System.
  - A top bar holds the health pill, the engine mode (Off / Recommend), the paper autopilot switch and the trading hours.
  - Copy is plain language, with each engine term in a toggletip. Cards come in three tiers: primary, secondary, simulated.
  - Same routes and the same three owner writes; no engine code touched.
  - Docs: spec `runbooks/design-2026-10-08-dashboard-redesign.md` (§15 gives every deviation and review fix with its reason), plan `runbooks/plan-2026-10-08-dashboard-redesign.md`, and the plan-of-record amendment in `IMPLEMENTATION_PLAN.md` §3.2.11.
- **Process:**
  - Worktree `..\mt_dash`, branch `wip/dashboard-redesign`. The live `dist` was untouched until the deploy.
  - Built by me plus three agents, one each for the Performance, Activity and System views.
- **Reviews:**
  - Three whole-branch reviews on 9f965ad:
    - copy against the engine's code: 15 wrong or overstated claims;
    - runtime: 14 findings;
    - data parity: every old field reachable, with small gaps.
  - All of these are fixed in 9b47ec7.
  - A fresh review of that fix diff found 12 more, all real and all fixed before the commit.
- **Validation:**
  - 73 unit tests, run under IST and America/New_York. `tsc -b` clean. Lighthouse accessibility 100.
  - In-page sweep checks: no sideways scroll, nothing spilling out of a card, targets ≥ 24 px, contrast ≥ 4.5:1. Clean for the default, ALERTS and EMPTY scenarios at 1440, 390 and 320 px.
  - Checked in the browser: skip link, tab focus, focus kept through a confirm, the Auto-mode control, failed-route states, hints on scroll.
- **Before/after:** measured at 1440 px on the same fixture data, before the review round.
  - Page height: 2,097 px (+730 px folded) → 1,071 px.
  - Chips: 132 → 9. Words: 885 → 185.
  - Overview: 11 panels → 3 cards.
- **Deploy 18:06–18:08, no engine restart** (StaticFiles reads `dist` on each request):
  - Previous `dist` backed up to `data\backups\dashboard-dist-pre-redesign-20261008`. Rollback: copy it back over `dashboard\dist`.
  - `phase3` fast-forwarded e6a10f9 → b86760a: 13 commits, touching only `dashboard/`, `runbooks/` and `IMPLEMENTATION_PLAN.md`. Then `npm run build` in the main tree.
  - Verified:
    - the served index.html names `assets/index-B_1eGNpf.js` (200) and its CSS (200);
    - `GET /mode` without a token answers 401;
    - the live page makes 4 requests, all same-origin, and the console is clean;
    - `mt-engine` was Running throughout.
- **Engine gap found, not fixed (owner decision):** a real `floor_*` cause has no clear path.
  - The causes are `floor_weekly_drawdown` and `floor_equity_floor_rung` (CLOSE_ONLY) and `floor_cumulative_floor` (KILLED), set at exposure.py:500/504.
  - No clear path names them:
    - `/resume_entries` clears only the owner pause and the rejection storm (telegram.py:1303-1304);
    - kill reset clears only "kill" (main.py:647);
    - the others are lifecycle, catch-up, token, feed-stale, and the daily causes at self-test.
  - The engine's own note says "owner re-arm only" (exposure.py:492-493). The dashboard now says these stops don't lift by themselves.
- **Next:** the owner reloads the dashboard (Ctrl+F5) to load the new bundle.

## 2026-10-08 — Dashboard paper toggle + paper go-live (owner: "Add button to toggle paper trade from dashboard itself"; chose "Toggle + go-live tonight")

- **Owner decisions:**
  - Asked whether to also go live, the owner chose toggle + go-live, with the autopilot left OFF until they turn it ON. This is recorded as **D13** in the 10-06 plan §0.
  - The host then slept 02:30–07:41, so the restart moved past the 09:15 open.
  - 09:33: "Run agents now" (market-hours agents approved for today).
  - 13:2x: "Trading windows has been closed so you can proceed now". That was the go-ahead for a mid-session restart, waiving the RUNBOOK's outside-09:15–15:30 rule for this one go-live.
- **Process:** plan `mt_build_notes\paper_toggle_plan.md`.
  - Spec review: 4 lenses plus a critic, 37 findings; plan v2 records each disposition.
  - Build: two agents in parallel.
  - Diff review: 3 lenses plus an adversarial verifier, 8 findings. 5 real-low ones are fixed, 1 was refuted, and 2 high ones were the same environment hazard, below.
- **b2dcee2 toggle:**
  - `POST /paper {enabled}` is single-step under the bearer token, like `POST /mode` OFF/RECOMMEND (D8: no real-order path). ON answers 409 unless the autopilot is built.
  - `PaperControl.set_enabled` writes only a change, and audits it to `config_audit` in the same transaction (`via` dashboard|telegram).
  - The Paper panel has a turn ON / turn OFF button. ON asks a confirm; OFF asks only when resting paper entries would be cancelled. The guard chip shows only while ON.
  - The OFF wording is aligned across Telegram, RUNBOOK and COMMANDS.
  - The rollback floor is now an observable `is_paper` row-count check.
  - Fixture screenshots: the go-live view, the ON/OFF round trip, and the OFF confirm.
- **Environment hazard, fixed by the owner at 13:10:**
  - `mt_wip\.venv` is a junction to the live venv. A review agent's `uv run pytest` (10:59) re-pointed the live editable install at `mt_wip\src`.
  - The next engine start would have run uncommitted code with `mt_wip\config` and a fresh empty database.
  - My `uv pip install --no-deps -e market_trading` was refused by the permission classifier; the owner ran it. Verified: the .pth points at `market_trading\src`, and a fresh interpreter imports from there.
  - Rule: never `uv run`/`sync` in a worktree.
- **d822d20 go-live:**
  - `paper.subsystem_enabled: true`. The shipped-settings test pin is now flag-agnostic.
  - Tests: 1055 settings-touching tests passed with the flag on; full suite on d822d20: 3921 passed, 20 skipped.
- **Deploy, 13:49–13:52:**
  - Pre-flight: 0 non-terminal orders, 0 paper rows, 0 real orders; the ancestor check passed; the .pth was correct.
  - Backup `data\backups\pre_paper_golive.db`: integrity ok, 41 tables, schema 0016.
  - One command: stop, `phase3` fast-forwarded 4842427 → d822d20, dashboard built, start.
  - Boot:
    - 13:50:13 `paper_runtime_constructed failures=[]`, then `paper_composed`;
    - 13:50:15 `job_scheduled paper_tick`;
    - 13:52:15 `paper_prep_started` (since None) → `paper_prep_done failed=False`, on the first in-session tick;
    - 13:52:22 `engine_ready`; post-arm reported nothing failed.
  - Health: 0 ERROR/CRITICAL lines, 0 paper_alert/step/compose failures, 0 Kite order/GTT calls.
  - API: `GET /paper` and `POST /paper` answer 401 without a token; the served bundle matches `dist`.
  - Real side unchanged: RECOMMEND, NORMAL, not killed, 0 real orders.
  - Paper: OFF (`changed_at` NULL), `last_observed_at` advancing; first `paper_equity_snapshots` row 13:52 at ₹40,000 (the capital base).
- **Engine left running,** per the owner's go-live decision (the 10-07 stop instruction was scoped to that build).
- **Next:**
  - The owner reloads the dashboard (Ctrl+F5) and presses turn ON in the Paper panel.
  - From the first ON, the Q4.13 five-session checks run daily (RUNBOOK §Go-live step 3).
  - Built-but-OFF writes flat equity snapshots each session; the first epoch's curve starts at go-live.

## 2026-10-07 01:28– — Implementing plan v5 (owner: "Implement and validate the plan … Avoid redundancy, over complication or unnecessary comments. Keep test cases concise")

- **Where:** worktree `..\mt_wip`, branches `wip/m0` → `wip/m1` → … (one per milestone, plan §3.2). The live tree (`phase3`, engine running) is untouched until a milestone deploys.
- **M0 (wip/m0):** Q0.1 plan-of-record notes in `IMPLEMENTATION_PLAN.md` (22 §s, D8 verbatim + D9–D12 in §8.4, stale hi52 1.47 text now points at `settings.yaml`); Q0.2 `NSECalendar.add_sessions`; Q0.3 migration 0015; Q0.4 `CALENDAR_HORIZON` monitor (boot + daily `feed_freshness`), sent once per horizon value.
- **Manager decision beyond the plan:** "outbox dedupe key" had no mechanism, so 0015 adds `notifications.dedupe_key` (partial UNIQUE index) and `CatalogMessage.dedupe_key`; a keyed message journals and sends at most once. Q4.10's paper alerts reuse it.
- **Deferred:** the Q0.2 `bars_1d` muhurat / DR-drill check and the Q5.1 snapshot need `market.duckdb`, which the running engine holds; tick compaction was mid-pass at 01:42, so both move to the evening stop window.
- **M1 + M2 (wip/m1 → tip `wip/m2` = 3977dfc):** every M1/M2 task (Q1.1–Q1.12, Q2.1–Q2.7), built by file-disjoint agent waves; each wave committed only after a full suite on a frozen copy (`..\mt_test`). Manager decisions:
  - rsi2 keeps its analyst target. Only time-exit strategies with a registered edge (hi52, ins) drop it; dropping it for rsi2 would fail every rsi2 entry at C3.
  - `/veto` treats a rec past `valid_until` that the 15:45 sweep has not reached yet as expired.
  - The C1 pre-registration was clarified before any run: the veto window is per strategy (hi52 400 days).
- **Review:** a 5-lens whole-branch review found 13 real findings (5 medium, 8 low; none refuted; none touch orders or risk). All 13 are fixed with failing-first tests. Mediums:
  - the exit card said a long's stop was "above entry";
  - the "re-anchored" clause printed for recs that were never re-anchored;
  - nightly counts mixed vetoes into the expiries;
  - protection reminder 2 skipped a session after an early-morning `/taken`;
  - the EW benchmark lost the muhurat leg.
- **Validation:** full suite on 3977dfc: 3545 passed, 22 skipped, chaos included. Dashboard checked on the fixture server (three changed panels; console clean apart from the fixture's `/ws/live` 404).
- **M5 so far:** C1–C3 pre-registered in §6.1 (2208daa, clarified d21a0ea) before the study scripts existed (d09d543); nothing has run on real data yet.
- **M3 (`wip/m3` = c7609d0):** Q3.1 live-impossibility pins, Q3.2 scope isolation, Q3.3 migration 0016.
  - `KiteClient(orders_enabled=False)` refuses every order and GTT call, before the guard. `ModeManager(paper_only=True)` refuses AUTO and live routing. The AST and import-closure pins are hardened after review.
  - Unique-index precheck on the live `state.db`, read-only at 03:50: 0 orders, 0 gtts.
  - Focused 3-lens review: 0 high or medium findings. The real-low ones are fixed.
- **M4 (`wip/m4` tip):** Q4.0–Q4.12 and Q4.14.
  - Built: OrderManager, PositionBook, ProtectionManager, ExitManager, session prep and Reconciler, paper equity and halts, the pipeline paper branch, every component composed under `paper.subsystem_enabled` (ships **false**), notifications, surfaces, the end-to-end replay (Kite double fails on any order call), and the RUNBOOK section.
  - A 6-lens whole-branch review found 24 real findings, 0 high. All are fixed: restart mark seeding, resting entries cancelled when blocked, working paper entries counted in the D10 capital cap, cost double-count, alert dedupe, the scorecard label regression and others.
  - Manager decisions:
    - A late corporate-action void is priced at the pre-ex mark.
    - Broker-filled paper closes charge fees only, recorded at plan Q4.4.
- **Not done by design:** Q4.13 go-live needs owner sign-off after reviewing M3/M4. M3/M4 are not merged into the live tree.
- **Deploy M0–M2, 2026-10-07 15:47–15:53:**
  - Stopped `mt-engine` after the 15:45 expiry job; `exit_clean`.
  - `state.db` → `data\backups\pre_0015.db` (online backup API; integrity ok, 36 tables, 102 recs, schema 0014).
  - Research snapshot `data\research\market_2026-10-07.duckdb`: 9 tables, every source/output count equal, 2.4 s.
  - `phase3` fast-forwarded 012c452 → 3977dfc (`wip/m2`). Started: `migrations_applied_on_boot [0015_rec_feedback.sql]`, selftest ok, `engine_ready` 15:53:29, no ERROR lines; `time_exit_check` caught up for 10-07 (no open positions).
  - Live dashboard `dist` rebuilt.
- **M5 research (snapshot, after 15:53; RUN records in `IMPLEMENTATION_PLAN.md` §6.1):**
  - Before the runs the brk20 baseline reproduced the registered report (test passes).
  - **C1:** brk20 `neither`, hi52 `(iii)` (2.5×ATR14 catastrophe stop), ins `neither`. Every shipped stop destroys the measured edge (T+20 median net: brk20 −1.47% vs +0.70% time-only; hi52 −1.41% vs +1.70%; ins −3.42% vs +0.93%).
  - **C2:** brk20 regime gate `supported` (2026 held-out contradicts); hi52 `not supported`; ins `insufficient`.
  - **C3:** several filters are `supported` by the registered rule, but that rule cannot separate a filter from a random subsample, so this is not evidence.
  - Protocol correction: ins first ran on a 730-day default window; it was re-run on the registered window. Both are reported.
  - Nothing changes live: every live consequence is owner work.
- **Engine stopped 16:05 at the owner's request** ("Turn off the trade engine once this entire process is done").
  - It is on the M0–M2 tree (3977dfc); tonight's EOD jobs, including the first `rec_outcomes` run and its 2026-08-26 backfill, catch up at the next boot.
  - The `stop_forced` line after `engine_stopped` is the known NSSM stop-grace pattern.
- **Q0.2 `bars_1d` check (snapshot):** muhurat (2024-11-01, 2025-10-21) and DR-drill Saturdays (2024-01-20, 03-02, 05-18) carry full daily rows, the same counts as regular sessions. So:
  - the rec/paper calendar convention skips those sessions (D2: not counted; a stop touched there is ignored);
  - the research convention counts them by bar position, as the registered backtests did.
- **Ops notes:**
  - Host in Modern Standby about 03:5x–06:26; the `%TEMP%` session scratchpad was wiped during it (nothing lost; state moved outside `%TEMP%`).
  - 06:32 the engine was stop/started from outside this session (SIGINT, then a clean boot at 06:34 on the unchanged live tree).
  - An orphaned chrome-devtools automation Chrome from 10-06 held the MCP profile and was closed.
- **Deploy M3 + M4, 16:15–16:18** (owner: "Merge M3 and M4, deploy and validate"):
  - `state.db` → `data\backups\pre_0016.db`.
  - `phase3` fast-forwarded to `wip/m4` (5f496be). Boot: `migrations_applied_on_boot [0016_paper_book.sql]` 16:16:19, `engine_ready` 16:18:34.
  - Read-only DB checks (`mt_build_notes\validate_m4.py`): the paper tables exist, `verdicts.is_paper` is present, the entry-verdict unique index is present, `paper_state` is disabled, and there are 0 paper rows.
  - Full suite on 5f496be: 3915 passed, 20 skipped.
  - Paper stays unbuilt (`paper.subsystem_enabled: false`); Q4.13 go-live is owner work.
- **First `rec_outcomes` run:**
  - The host was in Modern Standby 17:21–20:14 and missed the 18:00–19:30 EOD jobs.
  - The 30-min catch-up sweep (20:16–20:30) replayed all of them for 10-07 with no failures, including bhavcopy, daily_bars, rec_outcomes, the filings jobs and ins_crossings.
  - `rec_outcomes` scored 34/34 delivered entry recs: 12 unfilled, 11 closed, 11 open, all filled ones on the `1m_post_delivery` basis.
  - `universe_ew_returns` covers 2026-08-26 → 10-07 (29 sessions). The watermark is success and there were no warnings.
  - The 20:20 "catch-up complete (with failures)" Telegram is the compaction lane's `tick_compact:2026-10-06` (budget exhausted, degraded); it retries on its own.
- **Paper panel** (owner: "add a separate paper panel to the dashboard. Plan it out, review them without bias and then implement them"), a4a4e4c:
  - Plan `mt_build_notes\paper_panel_plan.md`. Three independent reviewers (truthfulness, data correctness, simplicity) were folded in before any code.
  - High findings fixed:
    - the first epoch has `epoch_started_at` NULL, so a plain `>=` filter would have blanked the panel at go-live;
    - unfilled entry orders have no positions row, so their symbol comes from the proposal;
    - the entries chip ignored the RECOMMEND-pipeline gates, so it now uses the engine's own guard plus the header mode;
    - stale equity and paused halts were invisible, so they are now labelled.
  - `GET /paper` returns a read-only summary. The API sees the stack only through `PaperView`, and `PAPER_EPOCH_SQL` is shared with ExposureTracker.
  - The curve uses primary-key lookups: 1.7 ms at a year of minute snapshots, against 130–350 ms for a GROUP BY.
  - The Positions panel drops paper rows, and the Scorecard paper columns are labelled all-epoch.
  - Validation:
    - suite 3918 passed;
    - fixture screenshots of the built and not-built states;
    - the summary run read-only on the live DB returns the not-built stub.
- **Deploy a4a4e4c, 20:32–20:35:**
  - Stopped (`engine_stopped` 20:32:16); `phase3` fast-forwarded 5f496be → a4a4e4c; live `dist` rebuilt. No migration.
  - Started: `engine_ready` 20:35:04, post-arm ok, no ERROR lines. `GET /paper` without a token answers 401, and the new bundle is served.
- **Engine stopped 20:35:37** (owner's standing instruction). `features_daily`, `nightly_review` and `tick_compact` for 10-07 catch up at the next boot.

## 2026-10-06 14:xx–23:4x — Plan for measure / honest card / paper autopilot / research (owner: "Plan the entire change process … Reiterate through the plan repeatedly … until you are sure it is implementation ready"); docs only, nothing deployed

- **Owner decisions:** D1–D12, recorded in the plan's §0:
  - recs and paper in parallel, fully isolated
  - D2 time exit at the close of the 20th session
  - D3 stop re-anchor
  - `/veto` reasons plus a 15:45 reminder
  - `/protected` confirmation
  - paper go-ahead with no real orders
  - paper pauses while the real side is frozen
  - paper uses the real `limits.yaml` caps
  - deferred: the regime tag, `GET /why`, C4 and the B2 digest
- **Plan:** `runbooks/plan-2026-10-06-measure-card-paper-research.md`, v5.
  - M0: plan of record, calendar helper, migration 0015, horizon monitor
  - M1: owner surface
  - M2: measurement
  - M3: live-impossibility pins, migration 0016, scope isolation
  - M4: paper autopilot
  - M5: pre-registered research
- **How:**
  - Six read-only subsystem maps, then four adversarial review rounds (5, 3, 2 and 1 reviewers; 76 → 71 → 29 → 9 findings).
  - The manager verified every finding against the code before accepting it. Ledgers: `data/reports/quantsync_2026-10-06/plan_review_round{2,3,4}.md`.
  - Every fan-out ran after the 15:30 close; the first started at 15:43.
- **Code facts surfaced** (implementation inputs; nothing changed):
  - A non-NORMAL real risk state stops the pipeline before the analyst (`ops/pipeline.py:1959-1961`).
  - `calendar.ex_dates` is a stub that returns `[]`.
  - PaperBroker fills a marketable LIMIT at its limit price.
  - The live scheduler ignores `fire_day`; only `sector_map` has a cron.
  - Nothing reads `feature_flags.paper_only`.
  - `/openapi.json` is unauthenticated.
  - Kite minute and daily candles come corporate-action adjusted (A11).
- **Ops:** the host was in Modern Standby 16:28–21:32 and stalled the round-2 agents, which were resumed via SendMessage. The engine stayed `Running`, the governor at DG0, with no session-limit errors.
- **Next:** the owner reviews the plan. Implementation starts at M0 on `wip/m0` once they give the go-ahead.

## 2026-10-06 11:2x–14:xx — QuantSync competitor research (owner: "research and analyse what it is doing better than our platform"); docs only, nothing deployed

- **What ran:**
  - Inline exploration of https://rsitradingplatform.vercel.app (chrome-devtools, public GETs, snapshots saved).
  - NSE UDiFF bhavcopy 2024-12-02 → 2026-10-05 downloaded to a scratch copy. `market.duckdb` was never opened; the engine was live.
  - Our `state.db` read only via `?mode=ro`.
  - After the owner's trade window closed (12:30; owner, verbatim: "Trade window has closed now, you can continue with your task") a 7-agent read-only verification workflow ran. The manager spot-checked every claim it overturned.
- **Report:** `runbooks/quantsync-comparison-2026-10-06.md`. Artefacts in `data/reports/quantsync_2026-10-06/` (gitignored).
- **Verdict on QuantSync:**
  - Their "54.7% / PF 1.98 verified" record is a backfilled simulation: EOD-close fills, exits booked a session late, late-booked stops, no costs, constant "ML 70%".
  - Under our execution their selection has no typical-trade edge: T+20 net median −0.69%. Inside NIFTY 500 (n=76) the median is −0.93%.
  - What they do better is product: decision-ready cards, a visible track record, any-stock analysis, a trades-only channel.
- **Our gaps found:**
  - The owner took the first 2 entry recs (both losses; HDFCAMC unprotected, −8.3%), then 0 of 28, while present at the dashboard on rec mornings.
  - Nothing prices untaken recs (`shadow_trades` empty). Over the same windows our recs' −1.72% equals the market's −1.65% (beta).
  - The brk20 1R/2R and hi52 6% exits were never backtested (all evidence is fixed-horizon).
  - Card defects: "(FAILED)" on approved shrinks, a self-contradictory OCO line, no exit date on T+20 recs, BHEL 10-06 shipped with a 9.6% stop on a 6% rule.
  - 95% of the 528 owner-channel messages since 09-14 are ops traffic.
  - No regime filter outside rsi2.
- **Proposed (none built):**
  - A1 rec-outcome scorecard (new table; never `positions`/`learning_ledger`/`shadow_trades`).
  - A2 skip reasons.
  - B1 honest card; B2 channel split; B3 protection/exit loop; B4 `/why`.
  - C1–C4 pre-registered research.
  - D1–D3 owner decisions (AUTO-paper go-ahead; the evening-GTT plan is NOT recommended as drafted; remote dashboard not now).

## 2026-10-05 00:5x — G2 C6 broker side attested by the owner; gate G2 complete

- Owner, verbatim: "No GTT or API orders have been placed".
- Recorded as C6's broker-side check. The platform side was already MET: `orders` and
  `order_events` are empty, and order-opening calls are refused outside AUTO.
- Every G2 criterion is now met or owner-decided:
  - C2a, C3, C6, C7: met.
  - C8, C9: signed 09-29.
  - C1: accepted at its status 09-29.
  - C4, C5: signed 09-29.
- Phase 3 wiring (WO-P3-5 AUTO(paper) routing, WO-P3-6 R3 managers) still needs the owner's
  sign-off to start. The `phase2` push also needs the owner's go-ahead (phase end).

## 2026-10-04 01:2x–02:xx — disk and repo cleanup; log/backup retention (owner chose from a cleanup survey); code NOT deployed

- **Survey:** `data/` held 22 GB — 9.6 GB backups (two 08-03 pre-remediation DuckDB snapshots, 7.8 GB;
  251 state snapshots, 1.8 GB, never pruned), 3.2 GB logs (half of it NSSM `service.err` copies of
  every engine.log line: core.log sent all levels to stderr; NSSM rotates at 10 MB and never deletes),
  3.4 GB / 1.38 M tick fragments (the 09-29 compaction lane is working through them — no action).
- **Done now (data/disk, owner-approved):** deleted the two 08-03 DuckDB snapshots (7.6 GB); archived
  the 78,554 non-JSON lines (uvicorn tracebacks/access lines, deprecation warnings) of 610 rotated
  `service.*-*.log` files into `data/logs/service-nonjson-archive.log` (6.4 MB), then deleted them
  (1.6 GB).
- **Code (takes effect at the next engine restart):** core.log gives stderr WARNING+ only when a log
  file exists (service.err's documented job is pre-logger output and crashes; uvicorn's own stderr
  handlers are unaffected). The nightly `job_backup` (never the shutdown backup, inside NSSM's stop
  grace) prunes state snapshots — all for 14 days, newest per ISO week to 90 days, newest always
  kept — and rotated service logs older than 90 days.
- **Repo/Claude:** removed the unused root `package.json`/`package-lock.json`/`node_modules` (Node
  `sqlite3`, no tracked JS used it; the dashboard has its own), the empty stray
  `market_trading/data/state.db`, merged local branches `phase0`/`phase1` (`phase1-preremove-backup`
  is unmerged — kept). agents.yaml header now says `max_output_tokens` is unsent. The synced `design`
  plugin (7 MCP servers needing auth) is disabled for this project in `.claude/settings.json`. Two
  stale memories pruned (project overview, resolved news-origination gap); their links reworded.
- **`scripts/replay_agents.py`** (owner-requested): the 10-03 model checks as a repo tool. `sample` pins
  live calls from the SDK transcripts (earliest transcript per prompt, so replays never pass as live);
  `run` replays them with harness-built production options as `--baseline` and a `--model`/`--effort`/
  `--cli-path` candidate, validates with the agents' real parsers, writes `data/reports/replays/<run>/`;
  refuses during market hours on trading days. Smoke run (8 calls, effort low vs production) ok.
  Its first sample also showed the news analyst already live on Sonnet 5.5: the Saturday 19:00–22:00
  sweep scored 6/6, 6/6 and 24/24 clusters at 21:20–21:36 on 10-03, none dropped.

## 2026-10-03 16:2x–18:xx — roster sonnet-5 → sonnet-5.5, SDK 0.2.103 → 0.2.159, engine-owned pinned CLI 2.1.288 (owner: "need to update them to latest version"; route, roster and news effort chosen by the owner)

- **Why the SDK and CLI move too:** the engine ran the CLI bundled with claude-agent-sdk 0.2.103, which
  is Claude Code 2.1.179; Sonnet 5.5 needs CLI ≥ 2.1.284. No SDK release since 0.2.159 (09-23) ships a
  Windows wheel — 0.2.160–0.2.163 are sdist-only, install with an EMPTY `_bundled/`, and the SDK then
  falls back to whatever `claude` is on PATH (auto-updating, or absent for the service). 0.2.159
  bundles 2.1.281, which logs `unrecognized_model` for claude-sonnet-5-5 and passes it through.
  Owner chose the pinned-copy route: SDK 0.2.159 + `llm.cli_path` → `data/run/claude-cli/2.1.288/
  claude.exe` (copy of the global 2.1.288, SHA-256 84304F7D4B0CD0EB…, runs standalone). The harness
  refuses the call if the file is missing or the SDK has no `cli_path` knob (fail closed, D7).
- **Sonnet 5.5 migration facts (platform docs):** forced `tool_choice` returns 400 (the CLI's
  StructuredOutput path survived: every replay returned the schema payload); `thinking: disabled`
  returns 400 (the engine sends none since de58a13); more refusal categories incl. `general_harms`
  (0 refusals in 21 Sonnet 5.5 calls); effort levels recalibrated; min cacheable prompt 512 tokens;
  $2/$10.
- **Replay evidence** (live engine prompts pinned by transcript file, written before 13:00 today):
  - production (Sonnet 5, bundled 2.1.179, current effort) vs 5.5, 10 prompts: valid 9/10 vs 10/10 —
    production hit `error_max_turns` on one news batch; 5.5 is 2–4× faster with 50–75% fewer output
    tokens; on the one live intraday entry, production emitted `entry_price: null` (the 09-21 defect),
    5.5 a priced entry.
  - **news materiality shifts down on 5.5** at low, medium and CLI-default effort alike: high-
    materiality (≥0.5) clusters per batch 2–3 vs live 4–6; |Δmateriality| vs live ~0.10 (production
    ~0.05). Of 12 clusters crossing the 0.5 line, most were Sonnet 5 over-rating unlisted names
    (Carlsberg India IPO, Manipal Payment), generic US-market wraps and promotional copy — 5.5 reads
    closer to the rubric. **This changes the cat v2 shadow population from deploy day**; the owner
    took news_analyst to 5.5 knowingly (09-24 hold on scorer-input changes noted in the ask). The
    cat v2 verdict must split its sample at the deploy date.
  - intraday: on the LT cat-shadow candidate, 5.5 entered at conf 0.4 in one run and declined in the
    other (citing the day plan's explicit trade-through warning); production and live entered at 0.55.
    Watch the cat-shadow evaluation count after deploy.
  - deploy candidate end-to-end (branch harness → SDK 0.2.159 → pinned 2.1.288 → claude-sonnet-5-5):
    6/6 valid, 21–34 s.
- **Change:** MODEL_API_IDS + pricing for sonnet-5.5 / opus-5.5 / fable-5.1 (and the missing
  sonnet-4.6 / opus-4.8 prices; a new test asserts every mapped name is priced — an unpriced name
  fails in governor.price AFTER the call is billed). Roster all `sonnet-5.5`; news_analyst `effort`
  removed (owner: CLI default). `harness.resolve_cli_path` (relative to `repo_root()`, not
  `config_dir().parent`, which `MT_CONFIG_DIR` can move) + `AgentHarness(cli_path=...)`, which also
  sends `DISABLE_AUTOUPDATER=1` (the SDK sets none, and the pin is a full Claude Code build). Boot
  logs `agent_cli`, or `agent_cli_missing` at ERROR when the pinned file is absent.
- **Pre-deploy review (owner-requested, engine stopped 17:43):** the branch was re-verified through the
  REAL harness path — `run_single_shot` with the real roster, a governor priced from the real
  agents.yaml, the real validators and the pinned CLI: news, intraday and preopen all ok on attempt 1,
  `agent_calls`/`budget_ledger` rows on `sonnet-5.5` with nonzero priced cost; the D11 smoke (Haiku)
  ok. Unit + chaos suites touching the harness/main: 632 passed. `news_clusters.scorer_model` reads
  `sonnet-5.5` from deploy on — the cat v2 verdict can split its sample on that column.
- **Deploy is atomic:** the code needs the 0.2.159 venv AND the pinned CLI. Stop engine → merge →
  `uv sync --extra dev` → start → check `agent_cli present=true` and the selftest roster line.

## 2026-10-03 13:4x–15:xx — analyst thinking depth: per-agent `effort` replaces the dead `max_thinking_tokens=0` pin (owner: "Validate and verify the suggested changes and then apply them"); NOT deployed

- **Finding (prompt audit, `/claude-api prompt-audit`):** the harness pinned thinking off with
  `max_thinking_tokens=0`, which claude-agent-sdk 0.2.103 marks deprecated (`thinking`/`effort`
  replace it). Sonnet 5 thinks anyway: 34 of the last 36 engine transcripts carry thinking blocks
  (hidden text, 2.5–29 K-char signatures); one news call billed 5,328 output tokens for ~8.5 K chars
  of visible output. Also found: `max_output_tokens` never reaches the SDK (no such option field), so
  the agents.yaml values are unenforced — reported, not changed.
- **Replay validation** (real engine prompts from the SDK transcripts, Sonnet 5, ~25 calls, Saturday):
  - effort only (old prompt): output tokens −22..−34% (news 6.8–7.0 K vs 8.6–9.1 K; preopen
    5.9–6.9 K vs 9.0–9.2 K), latency −15%; every output schema-valid; preopen focus list present 2/2;
    news materiality inside the current setup's own run-to-run spread (≥0.5 count 4–8 either way).
  - the audit's prompt rewrites ("Answer only through the supplied output schema…" + an unnumbered
    thesis length) were **withdrawn**: with them the preopen planner omitted `focus` entirely in 2 of
    3 runs (0 of 2 without), and the thesis change was untestable (all intraday replays no_action).
  - Caveat: replays write sdk-py transcripts into the harvested folder, so later "samples" were
    re-runs of one prompt per agent — read the numbers as repeated runs, not independent prompts.
- **Change:** `AgentDef.effort` (Literal low…max; refused on haiku, D9), loaded from agents.yaml
  and sent as the SDK `effort` option; `max_thinking_tokens` is no longer sent. agents.yaml:
  news_analyst `low`, intraday/preopen/nightly `medium`. Stale "100% cache miss" note removed from
  `news_analyst.py`/agents.yaml (latest news call read 14,253 cached tokens). All four SYSTEM_PROMPTs
  byte-identical to HEAD (verified).
- **Tests:** 3 new effort tests watched failing on the old harness, then 193 passed across
  agent_harness/agent_defs/context_assembler/nightly_review/news_scoring; ruff clean on touched files.
- **Deploy:** not done — the running engine keeps the old options until its next boot, which picks
  this up from the working tree. Watch after deploy: news `agent_calls` output tokens and duration,
  preopen `focus` length, intraday no_action rate vs the prior week.

## 2026-09-29 12:4x–23:0x — compaction lane, compaction stop, tick-flush queue fix (owner: "Work on the proposed fix, and validate them"); c526e81 + 8481667, deployed 23:04 (owner: "You can deploy your changes")

- **Diagnosis** (logs, `job_runs` and the ticks tree, all read-only):
  - Every compaction run since 09-19 was killed by a restart before it finished;
    `tick_compaction_done` was last logged 09-19 01:19. Runs manage ~50 symbol-days per 35–130 min.
  - Every `tick_compact` watermark from 09-22 to 09-28 is a 22:30 fire that found a catch-up
    compaction in flight: `skipped_in_flight` returned `ok=True` and was recorded as success.
  - The tick writer flushed every 3.2 s (984 flushes an hour, ~630 ticks and ~280 files each at
    221 ticks/s), so symbol-days held 2–4 K fragments against the ~375 WO-7 designed for. Every
    tick event handled before the flush thread took the lock queued one more flush (`aflush_ticks`
    sees a running flush, not a queued one), and each queued flush wrote whatever had been staged
    since. Behind that, the 2,000-tick cap tripped every ~9 s, so the 60 s interval never governed.
  - Every stop taken while a compaction ran (09-25 01:13, 09-26 15:58, 09-28 16:24, 09-29 10:09)
    lacks `exit_clean`; the six stops without one logged it within a second. `asyncio.run` waits
    up to 300 s for executor threads before `_hard_exit`, so NSSM killed the process at 30 s. This
    is the unexplained 09-25 01:14 forced stop.
- **Fix (c526e81):**
  - `CompactionLane`: `tick_compact` in its own `CatchUpRunner`. It is started behind the post-arm
    one-shot and after each 30-min sweep, runs one pass at a time, starts none inside the WO-21
    window, and is cancelled on shutdown. The main runner's registry no longer holds it.
  - `compact_ticks(stop=...)`, polled before each symbol-day: the WO-21 window or a stop signal.
  - An unfinished run (skipped for an in-flight run, or stopped) records no watermark, in the
    scheduled fire and in the date-keyed catch-up alike.
  - A queued flush re-checks under the lock that its batch is still due. `max_buffered_ticks`
    2,000 → 30,000 (peak minute 24,332 ticks; ~2.3 KB a tick, so a 60 s batch is ~30–55 MB).
  - The catch-up runner's per-pass `exclude` lost its only callers and is removed.
- **Independent review (Opus agent, after the close).** Three findings:
  - F1: the funnel-alarm change (below) blinds the alarm. Accepted; commit dropped.
  - F2: a stop during a backlog symbol-day can still pass NSSM's grace. Accepted as known, see
    limits.
  - F3: a post-midnight run can compact date D before D's session tail is flushed at the next
    morning's first tick. That leaves a fragment compaction refuses to merge, so every later run
    fails. Fixed in 8481667: `job_tick_compact` writes the buffered ticks first. The trigger
    predates the change; nothing in the logs or `job_runs` shows it ever fired.
- **Validation:**
  - Full suite in the worktree at c526e81: 3,231 passed, 21 skipped (Phase-3-gated). After
    8481667 the affected files were re-run: 185 passed, 3 skipped.
  - Watched fail first: the queued-flush regression test fails with the re-check disabled.
  - Load harness (real EventBus/BarBuilder/MarketStore, 221 ticks/s over 300 symbols, 130 s):
    old code 57 flushes (2.1 s apart, ~440 ticks, 56 fragments per symbol); new code 2 flushes
    (59.6 s apart, ~13.3 K ticks, 2 fragments per symbol); no tick lost.
- **Deploy.**
  - At 16:0x the auto-mode permission check refused the `phase2` fast-forward as a production
    deploy. The owner then approved it.
  - 23:02: `git merge --ff-only wip/compaction-lane` (phase2 → 0ef93a5); the import check of the
    live tree passed.
  - 23:04:04: stop. The old code was mid-compaction, so after `engine_stopped` at 23:04:17 NSSM
    logged `stop_forced` at 23:04:35 — the mechanism this fix removes, as predicted.
  - 23:05:03: boot, `crash_recovered: false`, self-test ok. Boot pass clean (23:06:53).
    `post_arm_jobs_fired` lists news_chain, catalyst_digest, preopen_planner, results_line_items
    and feed_freshness, with no `tick_compact`. `engine_ready` at 23:07:10.
  - The compaction lane's first pass ran right behind post-arm (outside the WO-21 window) and
    found nothing: at 22:40 the old code had recorded `tick_compact:2026-09-29` as caught up while
    its own run was still in flight — the false success this fix stops. The backlog (09-22
    remainder, 09-23 → 09-29, 2–4 K fragments per symbol-day) resumes at the 09-30 22:30 run.
  - The worktree and `wip/compaction-lane` are removed.
- **Still to observe:**
  - Next session: `ticks_flushed` about once a minute at ~11–24 K ticks.
  - While compaction runs: `catch_up_complete scope=all` every 30 min, and `tick_compaction_stopped`
    at 08:45 if a run is still going.
- **Known limits:**
  - Until the backlog drains (~5 nights), a stop during a symbol-day of 2–4 K fragments (28–47 s
    measured) can still pass NSSM's 30 s grace. Harmless: the store is closed and STOPPED
    committed first.
  - `tick_buffer_backlog` (a log line only) now needs 4 × 30,000 ticks.
  - Compaction catch-ups send their own CATCHUP_REPORT.
- **Funnel alarm, owner-reported 15:08 (`funnel_zero_in_session`): a false alarm, left as is.**
  - Last forward 11:44. A JSWINFRA brk20 slot at 12:06 waited for its retest, skipped by a live
    drain as `forward_skipped_outside_band`. The window shut at 12:31
    (`forward_queue_window_closed`), and the alarm, counting market-session minutes, paged from 14:07.
  - Counting only in-window minutes was built and withdrawn (review F1): with a 120-min threshold
    and a ~2 h window, a real wedge starting mid-window could never page.
  - The alarm needs a signal that separates "held for price" from "wedged"; that is the owner's
    decision.

## 2026-09-29 13:4x — G2 C1, C4, C5 accepted by the owner

- Owner, verbatim:
  - "Consider C1 and as it is, the current status is not an issue."
  - "Consider C4, C5 signed off."
- Recorded as owner decisions at the 10:52 measurement:
  - **C1** accepted at its status: 29/44 sessions, 7/9 since 09-17.
  - **C4** signed: 2 executions, both closed.
  - **C5** signed: 21 rec sessions as the collector counts them, 12 of them with an entry rec.
- **G2 now waits only on C6's broker-side half:** the owner's Kite order-book audit. The platform
  side is MET.

## 2026-09-29 10:5x — G2 C8 + C9 signed off by the owner; G2 re-measured; the 09-29 C1 miss traced to a catch-up blocked by tick compaction (owner: "G2 sign off has been done")

- **Sign-off record.** Read back from the owner's review page store (`signoff/g2`, version 8;
  private page https://claude.ai/artifact/9HkBwiuaurxfBAb3aPWNFF):
  - **C8 adopted 10:49:39 IST** as the two G2 weekly samples: the 09-24 04:3x review below.
    - One re-judge: EUROPRATIK (09-24, 56% stake in Fabwood) went from doubtful to **valid**.
    - Every other event stands as Claude judged it. No note.
    - Precision with the owner's call: week of 09-07 **2/5 events valid (40%)**; week of 09-21
      **7/8 (87.5%)**.
  - **C9 signed 10:49:50 IST.** All 85 payloads reviewed carry a gate verdict with its checks, cost
    math and a manual checklist:
    - 17 entries: full 30-rule gate, 3-step checklist.
    - 68 exits: `position_known` check only, 1-step checklist, edge 0. That is by design (R3):
      exits are never gated on a §7.1 limit.
    - The 5 delivered since the review are complete too: MEESHO, JYOTICNC, TITAN and CARBORUNIV
      (09-25), PATANJALI (09-29).
  - The three resolver fixes the C8 review proposed stay HELD until the `cat` v2 shadow verdict
    (owner decision 09-24).
- **G2 re-measured 10:52** (`scripts/g2_evidence.py`, 44 sessions 07-29..09-29):
  - **C1 NOT-MET:** 29/44 = 65.9%; 7/9 since 09-17. 09-28 and 09-29 both missed (below).
  - **C4 NOT-MET:** 0 taken, 2 closed. 87 recs expired untaken, 1 pending (PATANJALI).
  - **C5:** 21/44 = MET as the collector counts (any rec). But only **12** of those sessions had
    an entry rec: 9 are exit-only days (08-28 → 09-10), the repeated HDFCAMC/HINDZINC close
    alerts. Whether exit-only days count is the owner's decision (raised 09-24, still open).
  - **MET:** C2a (99.9%), C3 (90/90), C6 platform side, C7 ($29.84 of $200 this quota week).
  - **Signed:** C8 and C9, above.
  - **Owner-manual, unrecorded:** the Kite Console order-book audit (C6), and the confirmation
    that the rec weeks were continuous operation (C5).
- **09-28 miss, the known late-boot mechanism.**
  - Boot 10:00:33. The post-arm pass ran the news chain for ~30 min (Monday backlog) before the
    digest.
  - Window set at 10:01:35 for 10:20, then at 10:18:34 for 10:25.
  - Digest 10:32:01, 7 min after the window opened.
- **09-29 miss, a new mechanism: tick compaction held the catch-up lock.**
  - The log is silent 08:11 → 09:45, so the host slept through the 08:35 digest (the pre-open
    sleep the owner accepted on 09-21).
  - The catch-up that exists to recover a missed digest never ran. Every sweep from 21:51 (09-28)
    to 09:51 (09-29) logged `catch_up_skipped_in_flight`: passes are single-flight
    (`CatchUpRunner._pass_lock`, `ops/jobs.py:427`).
  - The pass in flight was the post-arm one-shot of the 21:27 boot (09-28). It included
    `tick_compact` and never logged completion. At 22:30 the scheduled compaction slot skipped
    as already in flight.
  - At 10:07 that compaction was still on **2026-09-22** (250 symbol-days; ~50 per 35 min
    awake).
  - The owner restarted at 10:09. The in-session post-arm pass skips `tick_compact`, so the
    digest landed at 10:16:01. The window, set at 10:09:03, opened at 10:15: missed by 1 min.
  - Same pattern at 09-28 16:24: the owner's stop cancelled `job_tick_compact` running inside
    the 16:00 sweep.
  - Inference, not observed: without the lock, the 09:51 sweep would likely have landed the
    digest before the 10:10 window. After the restart, news chain → digest took 2.5 min.
- **Two open issues.** Neither is fixed; a fix needs the owner's go-ahead and a boot outside
  session hours.
  1. Any unbounded job inside a catch-up pass blocks every time-critical catch-up for as long as it
     runs. That is the 08-10 wedge shape (`ops/jobs.py` module docstring), now via compaction.
  2. Compaction is ~7 days behind: it was on 09-22 on 09-29.

## 2026-09-25 01:14–01:30 — index fix live; owner ran the news dedupe

- **Stop and boot:**
  - The owner's stop at 01:13:59 was clean: the store closed and `engine_stopped` was logged at +19 s.
  - `scripts/dedupe_news.py` ran; its summary printed on the owner's console.
  - The engine booted at 01:15:08 on fe47338, so `init_schema` dropped the secondary indexes:
    crash_recovered false, self-test ok, `boot_contract_ok` at 01:22:11.
  - The first news poll after boot fetched 530 URLs (527 unique) and inserted 0.
- **The process outlived `engine_stopped` by 10 s and NSSM killed it at its 30 s limit**
  (`stop_forced`, signal 21). The store was already closed and the stop recorded, so the next boot
  was not crash-recovered and the log line's hint ("state stays RUNNING") did not apply. The four
  stops of 09-24 exited within 2 s; the log does not say what held this one.
- **CD-6 budget corrected:**
  - Stop-to-backup took 11.2 s (2.8–5.8 s on 09-24), so "a ~6 s backup, worst case 26 s" was wrong.
  - The `main.py` comment and CHAOS_DRILLS now give the measured 13–20 s stops. A wedged flush on
    top can pass 30 s and end in a kill, which leaves a WAL for the next boot to replay.
- **Post-boot slow statements** (5–13 s, one `store_stalled`) match earlier boots: `universe_daily`
  upserts took 5–21 s, `sentiment_agg` 8.6 s, and the same `news_clusters` query 5.4 s. They are not
  caused by the dropped indexes.
- **Validated 01:35 on the live store** (owner stop 01:33, clean, no WAL; opened read-only):
  - No secondary index.
  - `news`: 27,661 rows, 27,621 urls. No (url, cluster) pair is stored twice and no unclustered
    duplicate is left. 39 urls keep one copy per distinct cluster, by design.
  - No duplicate from the old code's last run (19:01–01:14) survived. The boot run added no rows.
  - `insider_trades` range and symbol filters equal full-scan counts; all 549 BSE rows are visible.
  - Primary keys are intact on `news`, `insider_trades`, `news_clusters` and `catalyst_watchlist`.
  - Engine restarted 01:36: not crash-recovered, self-test ok.

## 2026-09-24 19:00–19:50 — DuckDB secondary-index damage fixed in code; live apply is owner-run (owner: "Work on the issues found and fix them accordingly")

- **Diagnosis** (store copy taken at 19:01 after a clean stop):
  - In each table the SAME rows are missing from both secondary indexes: 7,044 for `news`, 420 for
    `insider_trades`. They are whole ingest runs on scattered days from 07-21 (insider) and 08-04
    (news).
  - Every primary key is complete: rows equal distinct keys in all 23 keyed tables, including those
    re-upserted daily.
  - The 17:45 note's UPDATE hypothesis does not hold, since the lost rows are insert batches.
  - Nor does a plain kill-and-replay: in DuckDB 1.5.4, rows replayed from the WAL after `os._exit`
    are found by every index.
  - The losses start mid-session and span clean and crash-recovered boots, so the trigger is not
    identified.
- **Impact:**
  - `ins`: re-running the crossing rule over 07-21 → 09-23 on the rows each run had matches every
    crossing journalled since the job went live (08-18). The index damage cost no signal.
  - The missing NSE feed did cost signals. With the NSE history, `ins` would have crossed
    BERGEPAINT 09-08, ASAHIINDIA 09-09 and JSL 09-10 (it journalled JSL on 09-22), NAVA on 09-15
    rather than 09-17, and not ECLERX 09-16. Today's run, the first with NSE rows, saw filings from
    103 eligible symbols (22 before).
  - `news`: 4,094 duplicate rows. 4,054 sit in the same cluster as an earlier copy. 40 formed new
    clusters (re-fetched old stories, all scored); 10 watchlist rows cite them, all graded `context`,
    none `originating`.
- **Fix (a968ca7):**
  - `init_schema` no longer creates the five secondary indexes and drops them from an existing
    store. `test_market_store` asserts none exists.
  - Without the url index each dedup insert is a ~10 ms scan and a poll re-submits ~430 known URLs
    (4.4 s). So the poll now fetches the stored subset of its URLs in one scan (11 ms) and inserts
    only the rest; `insert_news` keeps its `NOT EXISTS` guard.
  - `scripts/dedupe_news.py` deletes the duplicates: per url, the earliest copy in each cluster
    stays, and an unclustered copy stays only when no clustered copy exists. It rolls back if any url
    or member-backed cluster would vanish.
- **Validation:**
  - Unit, property, replay and chaos: 3,219 passed, 21 skipped (Phase-3-gated).
  - On the store copy: indexes dropped and 4,054 rows deleted, with the same 27,372 urls and 22,677
    member-backed clusters; the re-run is a no-op.
  - Synthetic keep-rule test (`test_dedupe_news_script`).
- **Not applied live:**
  - The session's permission check refused the live row deletion as a mass delete. It had earlier
    refused the live index change.
  - The index drop applies on the engine's next boot of this tree; the dedupe is an owner command
    (COMMANDS, "News duplicate cleanup").
  - Until the reboot, the running engine still dedups through the damaged url index.

## 2026-09-24 09:30–17:45 — NSE insider feed integrated, chaos defects CD-1/2/4/6/7 fixed (owner: "Integrate NSE insider feed, then work on fix for the CD issues. Validate them."); 1e69389..d084f34 deployed 17:36

- **What shipped** (plan §2.8.5 addendum 2026-09-24 later, `runbooks/CHAOS_DRILLS.md` §2):
  - `filings_pit` reads NSE's PIT V2.0 route `corporates-pit-gg` and fetches each constituent filing's
    XBRL. One failed XBRL is skipped and retried by later runs inside a 7-day margin; the store pairs
    the BSE and NSE copies of each trade and keeps the earlier broadcast.
  - CD-1: an in-session feed STALE latches `feed_stale` FROZEN, and the next HEALTHY clears it.
  - CD-2: a boot clock-skew freeze holds through the warm-up lift.
  - CD-4: CATALYST_DISABLED is sent when no digest has run by an in-window sweep.
  - CD-6: shutdown drains bus deliveries for up to 5 s before the store closes.
  - CD-7: a flush cut off by close restages its unwritten ticks.
- **Review round** (two independent reviewers, then fixes: 26a3769, d084f34):
  - The CD-1 latch awaited the owner's Telegram page inside the feed-health publisher. In an outage
    that stalls the frame reader until the silence monitor kills a healthy child; it now runs
    detached.
  - CD-4 checked `digest_status`, which reads a late run yesterday as fresh.
  - The old 10 s drain could overrun NSSM's 30 s stop grace.
  - One bad XBRL stopped the whole insider run.
  - `filings_events` carried a second, looser cross-source dedup that could drop another person's
    trade.
- **Validation:**
  - Chaos suite: 64 passed, 21 skipped (Phase-3-gated), 0 xfailed.
  - Unit, property and replay: 3,153 passed.
  - `tests/replay/test_golden_day.py::test_run_never_blocks_the_event_loop` failed twice under load
    (1.07 s gap, 1.0 s bound) and passed on re-run. Its path uses neither `EventBus` nor the tick
    flush; the base commit's own call time varied between 1.3 and 3.1 s.
- **Deploy 17:07–17:36** (engine stopped 17:07, clean stop): fast-forwarded `phase2`, then ran
  `FilingsPitJob` once on the live store for 04-25 → 09-24:
  - ok, not degraded; 1,769 NSE rows written; newest broadcast 16:40 today.
  - The pre-05-03 NSE corpus is unchanged: 44,187 rows, identical id hash.
  - Census: no stale feed.
  - Boot: engine started 17:36, `selftest ok`, `boot_contract_ok` 17:43.
- **Pairing verified against Python:**
  - 524 BSE rows; 505 have an exact NSE twin; 505 pairs collapse to one row each (88 kept from NSE,
    417 kept from BSE).
  - `get_insider_trades` returns exactly the set a Python re-implementation of the rule does,
    filtered and unfiltered.
  - The 19 unpaired BSE rows differ in person name ("(revised)" suffix) or pledge wording ("Pledge
    Invoke" vs "Invoke"). None is an open-market buy, so `ins` is unaffected.
- **Found: DuckDB ART secondary indexes have lost entries (NOT fixed; owner decision).** Only
  index-served lookups are wrong (`count_if` full scans agree with the data). A sweep of every index
  on a copy of the store found two damaged tables:
  - `insider_trades.idx_insider_broadcast`: a plain `broadcast_dt >= ?` from 05-27 returned 1,621
    of 2,041 rows, with 420 of 524 BSE rows missing. Before today `get_insider_trades` filtered
    exactly that way, so `ins_crossings` may have been reading only a fraction of the BSE feed.
    Unmeasured: when the loss began. Today's code filters outside the pairing subquery (no index
    use) and was verified exact.
  - `news.idx_news_url` and `idx_news_published`: 7,044 of 31,245 rows are missing from lookups.
    `insert_news` dedups with `NOT EXISTS (... url = ?)`, so the same URL was stored again:
    4,094 duplicate rows from 3,377 URLs, 56 in July, 2,371 in August, 1,667 in September; 39 of
    those URLs span two clusters. Origination is unaffected: the copies share one domain, and
    origination needs 2.
  - Likely cause: UPDATEs on indexed tables (`set_news_cluster` on every clustering pass; the old
    `insider_trades` DO UPDATE before 07-23). On a copy, DROP + CREATE INDEX restores exact lookups
    with the data untouched. For `news` that is temporary while the UPDATEs continue.
  - The live reindex was refused by the session's permission check (live-store surgery beyond the
    request), so it is left to the owner. The proposed fix: drop the four secondary indexes (the
    tables are 31k–46k rows, a scan is cheap) with a boot migration, and dedupe `news`.
- **Known, documented, not changed:** revision filings stored as their own rows; CATALYST_DISABLED
  can repeat once per restart on a no-digest day; the CD-7 restage makes a late flush's loss
  explicit but cannot write it.
- **Not pushed** (phase2 is unpushed; push waits for owner permission).

## 2026-09-24 09:00–09:15 — NSE insider trades found on a new endpoint (owner: "Look for new endpoint"); probe only, nothing integrated

- **How found:** loaded NSE's insider-trading page in Chrome and captured its traffic. The page no
  longer calls `corporates-pit`. It calls `/api/corporates-pit-gg?index=equities` (plus `index=sme`
  507 rows and `index=invitsreits` 19 rows). Its table config was last modified 2026-09-18.
- **What it holds:**
  - 2,814 equity filings, 03-May-2026 → 24-Sep 08:04; it starts the day after the old route's
    last row (02-May).
  - 542 symbols, 114 of them in the NIFTY-500 universe.
  - 2,760 filings under Reg 7(2) and 54 under Reg 7(3); 36 are revisions.
  - `from_date`/`to_date` and `symbol` filters work. July returned 247 in one call, identical to
    the unfiltered list, and nothing comes back before 05-03.
  - On 09-23: 33 filings, 8 in-universe from 7 issuers (BSE feed that day: 12 rows from 6
    issuers).
- **Trade details are in the per-filing XBRL** (nsearchives, `in-bse-co` taxonomy, one context
  per disclosure). Verified on NAUKRI (4 disclosures) and MARINE (Reg 7(3)).
  - Fields: person, category, qty, value ₹, Buy/Sell, mode ("Market Purchase"/"Market Sale"), trade
    from/to dates, intimation date, exchange executed.
  - Pre/post holdings are FRACTIONS.
  - The filing carries the BSE scrip code too, so both exchanges receive the same filing.
- **Engine path works:** `nse_get` on the listing returned 200 with the same 33 rows for 09-23.
- **Why it is not wired yet:** `ins_crossings` sums every `insider_trades` row in its 10-session
  window. Each source's id hashes its own broadcast time, so the same trade from both exchanges
  would be stored twice and double the net-buy sum. A cross-exchange dedupe (keeping the EARLIER
  broadcast, the point-in-time-correct one) must land with it.
- **Backfill size:** 737 in-universe filings since 05-03, 393 of them in the 05-03 → 07-18 hole
  that neither source covers.

## 2026-09-24 05:40–06:30 — full-universe ISIN map, feed-freshness census, BSE User-Agent (owner: "Proceed with fixing the issues…"); b052f30; engine still stopped (owner stop 04:06)

- **Context:** the owner concluded the platform now had complete data. A census of every feed's newest
  data disagreed, and the two proposed fixes were approved: rebuild the ISIN map for the full
  NIFTY 500, and add a staleness alarm covering every feed.
- **Found while fixing:**
  - **BSE refuses the old UA.** Between 05:51 and 06:10 BSE's API host answered 403 to the
    `Chrome/126` UA every feed sent, on the scrip master, an SHP endpoint and the insider-disclosure
    endpoint (`getCorp_Regulation_ng`). The same request with `Chrome/153` got 200. The engine's own
    fetch at 19:00 on 09-23 had passed (a 5,044-row master), so the block began overnight.
    NSE (API and archives) and the news RSS hosts accept both UAs. Unfixed, tonight's 19:00 insider
    feed (the `ins` rule's only live source) would have got 403.
  - **SHP was skipping ~300 names.** `filings_shp` counts a symbol missing from `symbol_isin` as
    out-of-universe. The map had held the 200 names of 07-17 since then, so from O15 (09-04) every
    SHP filing from the other NIFTY-500 names was dropped silently. Nothing in a decision path reads
    `shp_quarterly`, so no live decision used the gap.
- **Built (plan §2.8.5 addendum 2026-09-24):**
  - `isin_map` is now a scheduled run-latest job at 18:40. The BSE bulk master is fetched only while
    some code is missing, and a stored code wins. Rows are re-stamped daily, so a code left behind by
    a rename resolves to the newest symbol. The per-symbol PeerSmartSearch resolver is deleted.
  - `feed_freshness` runs at 21:30, post-arm. It checks 10 feeds against per-feed lag limits and
    sends one alert per stale streak.
  - One `BROWSER_USER_AGENT` constant replaces four hard-coded UAs.
  - The 45 s master timeout now applies to both readers of the master.
- **Validation:**
  - Scratch worktree, then cherry-pick.
  - Full suite: 3203 passed, 21 skipped, 5 xfailed. The last two edits (rename ordering, per-streak
    alerts) came after that run, so the 1,178 tests touching the changed modules were re-run: green.
  - **Live, engine stopped:**
    - Census before: stale `insider_nse` (newest 05-02, 145 days) and `isin_map` (07-17,
      48 sessions).
    - `IsinMapJob` run: 500 constituents, 497 with a BSE code. The 3 without are BSE, CDSL and
      DUMMYHEG, none BSE-listed, the same 3 measured on 09-12. No code or ISIN is shared between
      symbols.
    - Census after: only `insider_nse` stale. Every EOD feed is at lag 1 (09-23 data), news at 0.
  - The non-quarter-end SHP dates (126 of 2,423 symbol-dates, e.g. ADANIENT 07-07) look like the
    SEBI event-driven SHP filings; they only make the census more lenient.
- **Open / not fixed:**
  - NSE PIT route dead upstream since 05-02; needs a new source.
  - BSE `Isdefault=2` ~25-row cap.
  - 13 `insider_trades` rows with source-typo dates.
  - SHP history for the 298 newly mapped names: COMMANDS "Backfills", a weekend job.
  - Chaos defects CD-1, CD-2, CD-4, CD-6 and CD-7.
- **Engine:** still stopped since the owner's 04:06 stop; the next boot runs b052f30.

## 2026-09-24 05:00–05:30 — results data integrated (owner: "Probe and find sources to integrate this data, then work into making our system work correctly"); 221d43c; live listing backfilled with the engine stopped (owner stop 04:06)

- **Source found by capturing NSE's own page traffic:**
  - `api/integrated-filing-results?type=Integrated Filing- Financials` holds 26.8k filings from the
    Mar-2025 quarter on (the old endpoint ends at Dec-2024);
  - it pages up to 500 rows and filters by broadcast date;
  - each row links an XBRL file. Revenue/PAT tags were verified per taxonomy (INDAS, NBFC_INDAS,
    NONINDAS, BANKING, LI, GI).
- **Owner decision (asked):** hold both the News Analyst's use of real revenue and the three
  resolver-alias fixes until the `cat` v2 shadow verdict (~mid-Oct), because both change the frozen
  shadow population. Recorded in the plan §2.8 addendum.
- **Built:**
  - Integrated Filing leg in `filings_results`;
  - staleness alarm (newest period >150 days ⇒ degraded + alert) — the guard that was missing for
    18 months;
  - new `results_line_items` job (19:30, post-arm, ≤300 paced XBRL fetches, quarterly revenue + PAT,
    consolidated preferred);
  - `line_items_at` column with a legacy migration;
  - listing re-upserts that never blank parsed values;
  - backfill `--skip-integrated` leg.
  Consumers are not wired.
- **Validation:**
  - Built in a scratch worktree, then cherry-picked, so the live tree never held partial code.
  - Tests on verbatim NSE fixtures; mutation checks on the preserve rule and quarter selection.
  - Full suite green: 3199 passed / 21 skipped / 5 xfailed.
  - On a 5.5 GB COPY of the live DuckDB with real NSE: migration applied, Jul–Sep listing 4,158
    filings, revenue/PAT correct for 8 issuers across taxonomies.
- **Live:**
  - `backfill_filings.py seed --from 2025-01-01 --skip-pit --skip-results --skip-shp`: 21 windows,
    23,779 filings, 0 failed, 151 s.
  - `results_filings` 24,747 → 48,467 rows; newest period 2024-12-31 → 2026-06-30.
  - News-universe coverage per quarter: 855–905 of 917.
  - Line items: 0/≈4,400 attempted; they fill nightly (~15 runs).
  - The engine stays stopped (owner's 04:06 stop); the migration also runs at its next boot, as a
    no-op, since the backfill already applied it.

## 2026-09-24 04:4x — revenue/profit sources probed (owner: "Is there a way to obtain revenue and profit records?"); results-filings feed found starved

- **Real figures are obtainable (probed live, read-only):**
  - the XBRL links already stored in `results_filings` (tags `RevenueFromOperations`,
    `ProfitLossForPeriod`, in ₹);
  - NSE `results-comparision?symbol=` (in ₹ lakh).
  Both give BDL Q3 FY25 revenue ₹832.1 cr and PAT ₹147.1 cr, and the two agree exactly.
- **Both stop at the Dec-2024 quarter.** Current results are filed as "Integrated Filing- Financial" (our
  news store: TEMPSENS, quarter ended 30-Jun-2026). The endpoint for that format is not identified yet.
- **The `filings_results` job starves silently:**
  - it records success daily, but has written 5 rows since the July seed;
  - max `period_end` is 2024-12-31, and `revenue`/`pat` are NULL in every row (§2.8.4 stage 2 unbuilt);
  - its own query for 01-08..23-09-2026 returns 3 old-period rows.
- **Consequence:** the News Analyst's materiality rubric ("order ≥10% of annual revenue") has no revenue
  input, so its scores rest on model memory. Nothing fixed yet; it needs the owner's decision.

## 2026-09-24 04:3x — G2 criterion 8: watchlist-precision review, two weekly samples (performed by Claude on owner request "Can't you perform this check on your own?"; OWNER ADOPTION PENDING — the plan names the owner as reviewer)

Source: every `catalyst_watchlist` row with grade `originating` since 09-07, joined to its `news_clusters` headlines
(read-only DuckDB while the engine was stopped by the owner at 04:06). Question per event: is this a genuine,
company-specific, material catalyst for THIS symbol? Size calls use my approximate knowledge of company scale
(not checked against filings). Week of 09-14..09-18 produced ZERO originating rows (the digest ran daily,
159–226 context rows), so it cannot serve as a sample.

**Week 1 — 09-07..09-11: 9 rows, 5 distinct events → 2/5 events clearly valid (40%; 4/9 rows).**
- ✅ CEIGALL (09-07, 09-08) — ₹5,300 cr REC power-transmission LoI, larger than a year's revenue.
- ✅ ADANIENT (09-09, 09-10) — Adani Airports raises $1 bn primary equity (BlackRock, Temasek); stock +6%.
- ⚠ IIFL (09-08, 09-09) — "Blackstone eyes up to 20%", "Fairfax may exit": unconfirmed sources report,
  and the direction is ambiguous (an exit is an overhang).
- ❌ ADANIPOWER (09-08) — LoI for GVK's 330 MW hydro plant: small next to the company's fleet. The NSE-filing
  cluster scored 0.85 on its wording ("Successful Resolution Applicant"); the press cluster scored 0.4.
- ❌ PFIZER (09-08, 09-09) — WRONG COMPANY: the seller is Pfizer **Inc** (US parent); the buyer Novartis India
  (NOVARTIND, `not_in_index`) is who the deal matters to. Resolver alias `pfizer` → PFIZER; direction "long" unsupported.

**Week 2 — 09-21..09-24: 10 rows, 8 distinct events → 6/8 events clearly valid (75%; 8/10 rows).**
- ✅ BEML (09-21) — ₹5,400 cr NHSRCL high-speed-rail order.
- ✅ ENGINERSIN (09-23, 09-24) — ₹4,300 cr Dangote Kenya refinery contract. One of its five clusters is spurious
  (see resolver finding 1) but the row stands.
- ✅ PERSISTENT (09-23, 09-24) — Nagarro takeover successful, 83.25% stake (transformational acquisition).
- ✅ BDL (09-24) — ₹811 cr MoD contract.
- ✅ OSWALPUMPS (09-24) — ₹273–297 cr Telangana rooftop-solar order.
- ✅ SKIPPER (09-24) — ₹797 cr T&D orders.
- ⚠ EUROPRATIK (09-24) — 56% controlling stake in Fabwood Solutions; no deal size in any headline, so
  materiality cannot be judged.
- ❌ DALBHARAT (09-22) — WRONG COMPANY: the filings are Himadri's (`[NSE:HSCL]`) scheme with Dalmia Bharat
  **Refractories**; alias `dalmia bharat` → DALBHARAT matched the prefix of the other company's name.

**Verdict:** precision 40% → 75% week-on-week. Every miss is one of three kinds, and all but one are upstream of
the model:
1. **Resolver: generic seeded alias.** `engineers` → ENGINERSIN (seed) attached a GPTINFRA filing ("Alcon
   Builders **and Engineers** … L1 in ₹21 cr order") to ENGINERSIN.
2. **Resolver: wrong group or parent entity.** `dalmia bharat` (DALBHARAT) matched "Dalmia Bharat
   Refractories". `pfizer` (PFIZER) matched "Pfizer Inc".
3. **Model: over-scoring filing wording.** Exchange-filing wording scored as company-transforming regardless
   of relative size (ADANIPOWER).

Owner: adopt this as the two G2 weekly samples, or re-judge any row. Candidate fixes (none applied):
- stoplist the generic stripped alias `engineers`;
- curate the longer names (`dalmia bharat refractories`, `pfizer inc`) onto non-tradeable entries, so span
  subsumption refuses them instead of attaching them to the cement or India-listed company;
- in the news-analyst rubric, score materiality relative to company size.

## 2026-09-24 early morning (owner: "Proceed with the suggested changes. Validate them") — CD-3 + CD-5 fixed (47af14c); O17 applied by owner (e148a4e); first clean stop since 09-02; engine restarted 03:39, ready 03:40:39

- **Owner steps observed.** O17 combined patch applied + reseeded ~03:04 (selftest
  `protected_store:limits.yaml` PASS); the working tree matched the patch exactly
  (`git apply --reverse --check` clean) and was committed as e148a4e. NSSM `AppStopMethodConsole`
  = 30000; the owner's 03:03 stop was still cut at +1.31 s because NSSM reads it at service start.
- **CD-3 (chaos case 16).** Safety-critical catch-up only checked TODAY's fire-time, so a missed
  18:30 `earnings_calendar` run was never replayed (live: no run recorded for 09-22) and an evening
  failure held FROZEN through the next session. `CatchUpRunner._governing_day`: verify the latest
  fire at or before the next entries-open moment — the previous evening's for an EOD job; a pre-open
  job (instruments 08:15) stays with the scheduler, so pre-login/weekend boots never re-run and page.
  Freeze-notify dedup keyed (job, run_for), pruned by age (two jobs failing for different governing
  days would otherwise re-alert every sweep). Behaviour note: an NSE event-calendar outage now
  freezes entries until a 30-min retry succeeds — the plan's safety-critical rule, previously
  unenforced for this job in the morning.
- **CD-5 (chaos case 21).** Model entity strings reached the resolver unchecked. Measured first on
  26,133 live entities (1,371 calls): 99.2% verbatim once HTML-unescaped — without unescaping,
  `L&amp;T`/`M&amp;M` headlines would have lost L&T/M&M; the rest are macro terms plus entities
  attached to the WRONG cluster of a batch (KKR on "SBI Q1 Preview"). `_write_back` now drops strings
  not found whole-word in the headline the model was shown (logged `news_entities_not_in_headline`).
  Accepted loss: expansions like "Vi" → Vodafone Idea (contract is verbatim; §5.5 alias loop).
- **Validation.** New tests seen failing on the old code (4 catch-up cases; 3 injection variants),
  passing now. Unit+property+replay 1349 + 1777 passed; chaos 59 passed / 21 skipped / 5 xfailed
  (CD-1/2/4/6/7 still pinned); ruff clean. Plan §2.6 step 5 and §2.7 step 3: one clause each.
- **Deploy + live drill (case 15) PASS.** Stop 03:39:53 → backup + `shutdown` 03:39:59, STOPPED,
  `last_clean_stop_at` 03:39:59 (first since 09-02) → `engine_ready` 03:40:39, `crash_recovered:
  false`, NORMAL; boot verified Wed's earnings watermark (no re-run) and left Thu's 08:15 instruments
  to the scheduler.

## 2026-09-23 evening (owner: "implement the suggested fix [stop handler]. Also implement Failure drills … and New position caps (O17)") — stop-signal grace (f4667f3), NSSM stop-grace config (bb5fe12), chaos suite + CHAOS_DRILLS.md (a4ab744), O17 combined patch prepared; engine restarted twice, running since 21:33

- **G2 re-measured 20:00 (`scripts/g2_evidence.py`, 40 sessions):** still NOT met — C1 digest-before-window
  27/40 = 67.5% (8/10 = 80% since 09-09), C4 owner-executed 0 taken / 2 closed, C5 18/40 rec-sessions.
  C2/C3/C6/C7 met. `phase2` fully pushed (241 ahead of main).
- **Forced stops, diagnosed.** Every stop since 09-02 logged `stop_forced` 0.85–1.4 s after
  `stop_requested`, mid shutdown-backup, so `last_clean_stop_at` stayed 2026-09-02 and every boot ran
  crash recovery. Not a store stall. f4667f3: the stop handler ignores repeat signals for 10 s and logs
  `signum`. Live drill 21:29 on f4667f3 (case 15): repeat ignored (`signum 21` = SIGBREAK at +1.33 s)
  but the process still died at STOPPING — NSSM `AppStopMethodConsole` is 1500 ms; it then closes the
  console (CTRL_CLOSE_EVENT) and Windows terminates the engine. bb5fe12: `nssm_install.ps1` sets 30000.
  **Owner action:** `nssm set mt-engine AppStopMethodConsole 30000` from an elevated shell (COMMANDS.md
  "Service logs"); this session's shell is not elevated (`OpenService(): Access is denied`). Re-drill
  case 15 afterwards.
- **Engine ops.** Stop 20:18 (old code, forced as expected) → start requested ~20:19 but it sat in a
  permission prompt until 21:23 (PC in Modern Standby 20:34–21:08) → `engine_ready` 21:29:18
  (crash-recovered, caught up backup/deals/features_daily/nightly_review for 09-23) → drill stop 21:29:46
  → `engine_ready` 21:33:13, NORMAL, integrity ok.
- **Chaos suite (a4ab744; three Opus builds under my spec, audited: pointers for CD-1/2/3/5 opened and
  confirmed, full suite re-run by me).** `pytest -m chaos`: 56 passed, 21 skipped, 9 xfailed, ~90 s.
  Every §9.4 case has a file or a Phase-3 placeholder. Seven product defects pinned as strict xfails,
  none fixed: CD-3 missed EOD `earnings_calendar` never caught up (jobs.py:370/595; high), CD-5
  model-emitted entity strings resolved into symbols (news_scoring.py:336/345; high), CD-1 feed STALE
  never latches FROZEN, CD-2 warm-up lift clears a skew freeze, CD-4 stale digest sends no
  CATALYST_DISABLED, CD-6/7 teardown drain + wedged flush. `runbooks/CHAOS_DRILLS.md` holds the map,
  the defects, Phase-2 live-drill steps, the §10.6 soak schedule and the evidence log.
- **O17 NOT applied — owner step.** The classifier refused `git apply` on the live `config/limits.yaml`
  (Modify Shared Resources) even with the explicit directive; limits.yaml untouched, signed file intact.
  Built the full change in a scratch worktree instead: limits + hi52 `expected_edge_pct` 1.47 → 1.53
  (derived: floor-rung notional ₹6,000, cost floor 0.4982%) + ins stop comment (at 2.0× on ₹20k: 4/5/6%
  pass 2.88/2.52/2.23×, 7/8% reject) + 8 tests that encoded 2.5×/₹8k + plan O17 status. Full unit suite
  in the worktree: 3076 passed (Opus build, audited; 476 touched-file tests re-run by me). Exported as
  `runbooks/briefs/o17_combined_2026-09-23.patch`; `git apply --check` clean on HEAD. Owner steps in
  COMMANDS.md "Protected config".
- **Also:** `.gitignore`'s `secrets.*` matched `src/engine/core/secrets.py` (key names only), so it was
  never committed and a fresh clone could not import `engine`; un-ignored and added (bb5fe12).

## 2026-09-23 (owner: "Trade recommendations should close automatically and free the gate count at the end of the day if no trade decision were made") — entry TTL back to today's close, WO-V withdrawn (b7d174d); engine restarted 01:33 IST, boot verified

- **Status check first (owner: "today's trade has been completed, check status", 09-22 session).**
  Fixes from 09-21 verified live: 8 LIMIT proposals priced from the scanner level over two sessions
  (`enter_limit_price_defaulted`: EIHOTEL, BEML, MOTHERSON, DALBHARAT, PFOCUS, YESBANK, USHAMART,
  PVRINOX), 0 sector/UNCLASSIFIED rejects in 19 verdicts, digest before the window on 09-21 (03:14 vs
  09:30) and 09-22 (09:58 vs 10:05; boot 09:43 after the token expired at 09:41 ahead of the owner's
  login). Delivered: SYRMA, KPIL, SPLPETRO (09-21), PFOCUS (09-22); owner took none. Dominant reject
  became `max_open_positions` (12 of 19): the gate counts unexpired unactioned entry recs as open, the
  CNC cap is 4, and WO-V's two-session validity kept the prior day's four untaken recs pending
  ("total 0+4 pending+1 = 5 … CNC <= 4"). Health: five forced stops in two days, `store_stalled`
  ×5, `boot_seed_timeout` on the 09-22 09:43 boot, an 85-line Kite connectivity burst 09-21 09:55.
- **Change (b7d174d; Sonnet build under my spec, audited; stale comments fixed by me).**
  `RecommendationPipeline._ttl`: swing/position = TODAY's session close for every kind including
  entries; the `entry` switch and `_next_actionable_session` (muhurat walk, horizon fallback) removed;
  the never-dead-stamp floor kept. `expire_stale` (15:45) and the gate's pending screen read the
  stamped `valid_until` and needed nothing; Telegram/dashboard date-aware rendering and
  `RecommendationBook.take`'s overnight drift check stay (generic). Tests: five WO-V TTL cases
  deleted, three adapted, one added. Plan: the WO-V paragraph carries the dated withdrawal, with the
  accepted loss named (a consumed-once `ins` signal the owner did not look at that day).
- **Validation.** `tests/unit` 3074 passed + 1 failed on a 13-minute loaded run —
  `test_single_instance…kernel_releases`, a subprocess-spawn test; passes alone in 73 s (flaky under
  load, unrelated). ruff clean. Restart 01:32 (idle; stop clean in 4 s this time), `engine_boot`
  01:33:08, token valid, selftest 01:33:50, `daily_gap_done` 01:35:30, `catch_up_complete` 01:37:47,
  `engine_ready` **01:37:57**. Boot hydration for 09-23 runs behind the post-arm one-shot as on 09-21.
- **Effect from today:** PFOCUS (stamped 09-22, valid to 09-23 15:30) is the last two-session rec;
  every entry rec delivered from now on expires at 15:30 and frees its CNC slot the same evening.

## 2026-09-21 (owner: "Start working on the recommended fixes. Validate them … do not over-optimise"; pre-open keep-awake explicitly excluded) — boot-time early hydration, LIMIT price default, sector-map industry fallback (ccea311); engine restarted 02:38 IST, boot verified, boot hydration observed live

- **Fix 1 — early hydration also fires at boot (`EarlyHydration.on_boot`).** Evidence 09-16: engine
  booted 06:58, no Kite login before the open, PC asleep ~08:00–10:00 (health_check cadence → 0,
  "Run time of job … was missed" bursts on wake), digest 10:18 vs a 10:15 window. Hydration was a
  login-only hook, so a boot without a login hydrated nothing. Now dispatched once at boot right after
  the post-arm one-shot (same gates: trading day, before open, armed, recovery done; same watermarks;
  cancelled at shutdown). Plan §2.6 addendum sentence. Pre-open keep-awake NOT built — owner: the
  laptop is inactive while travelling in the morning, so it is not the lever.
- **Fix 2 — LIMIT proposals without a price were being rejected as "no usable LTP".** Diagnosis: the
  gate reads LTP and tick-age from ONE cache (`last_ticks`, main.py `mark_price`/`tick_age_s`) and the
  tick-age guard passed on every such reject, so the LTP was not the problem. `proposals` rows: 7 of 13
  LIMIT proposals since 09-16 (sonnet-5 intraday analyst) carried `entry_price=null` (TATAINVEST, PWL×2,
  PAYTM, BHEL, YESBANK, BLUESTARCO); `EnterAction.entry_price` is Optional with only a comment saying
  "required if LIMIT". The pipeline now defaults it to `candidate.raw_levels.entry` (already
  band-screened at the forward slot) and logs `enter_limit_price_defaulted`; `entry_sanity_band`
  distinguishes "no entry price" from "no usable LTP", and the priced rules say "unpriceable (no entry
  reference)" instead of blaming MARKET. That the default is what the analyst means: PAYTM 09-17 09:42
  entry=null vs 09-18 09:49 entry=1758.90 with identical stop/target.
- **Fix 3 — sector map classifies from NSE's Industry column.** Under NIFTY 500 the ten sectoral
  indices covered ~170 names: 750 of 913 snapshot rows were UNCLASSIFIED (cap 1; pending recs count),
  so ACMESOLAR (09-16 10:35, pending two sessions) blocked AEGISVOPAK, PWL, PAYTM and ECLERX. New rung
  after index scrape + overrides: the Industry label from `data/universe/index_cached.csv` (seed
  fallback), aliased onto the index names where natural (Healthcare→PHARMA, Power/Oil Gas→ENERGY, …),
  else a normalised bucket (CAPITAL_GOODS, CHEMICALS, …). The news keyword vocabulary stays the ten
  index names (`set_sector_map` filter) so SERVICES/DIVERSIFIED never tag headlines. Today's snapshot
  rebuilt offline with `scripts/run_sector_map.py` (new; engine stopped): 926 rows, UNCLASSIFIED
  750→426 (the remainder = extended non-index names, never gate-approvable), industry_classified=337.
  Plan §4.4 job 13 sentence; COMMANDS.md entry.
- **Deliberately not done: digest ahead of the news-backlog drain on late boots.** The honest version
  is faster/parallel LLM scoring of the backlog (a digest run before the drain is empty, and the G2
  collector records the later run anyway). Observed live on this boot: `engine_ready` 02:39:06,
  universe 02:39:43, then one news-analyst batch every ~90 s — 19 batches for the weekend backlog —
  `catalyst_digest` **03:14:27** (2,891 clusters, 1 originating / 177 context), `preopen_planner_ran`
  03:16:19, `early_hydration_pass reason=early_boot` all five jobs `ran` 03:16:19: **boot → digest
  36 min.** On a 09:30 boot that latency IS the C1 miss; the cheap lever is the owner's window offset
  after boot (≥ 30–40 min), or parallel news scoring if he wants it engineered.
- **Rec-rate note (from the 09-18 assessment):** 8–10 recs/day → ~1/session after 09-11 is orb being
  parked on 09-12, not the 09-15 roster change — brk20/hi52 confidence ≈ 0.55 under opus-5 and sonnet-5
  alike (proposals table) against the owner-only 0.55 floor.
- **Validation.** Sonnet/Opus builds under my specs, audited on pointers (one dead guard removed);
  `tests/unit` **3079 passed** (6:04); ruff clean. `Stop-Service` 02:37:11 (idle; stop again
  `stop_forced` — still open), sector rebuild 02:37, `Start-Service` 02:38, `engine_boot` 02:38:24,
  token valid, selftest complete, `engine_ready` **02:39:06** (42 s). Commit ccea311 + this note.

## 2026-09-18 (owner: "Fix point 1 and 3" → "Apply the fix and validate them") — upstream-confirmed no-trade minutes (b342240) + sonnet-5 pricing (4eac3c3); engine restarted 15:33 IST, boot verified; live-validated on DEEPAKNTR at 13:17 — under a boot I did not perform

- **Point 3 (4eac3c3).** `agents.yaml` `sonnet-5` 3/15 → 2/10 per MTok (Anthropic first-party rate;
  every enabled agent bills at this key since 09-15, so the governor's proxy ran ~50% high).
  Allocations left at the $200 sizing done at the old rate — the ladder now trips later; owner re-base
  if wanted. Parses; `test_agent_defs` + `test_budget_governor` 95 passed. Read at boot.
- **Point 1 (b342240) — design.** Not synthetic bars: `bars_1m.src` is a `CHECK` baked into the
  4.9 GB table. A fact table `bars_1m_no_trade(symbol, ts_minute, confirmed_at, src='kite_empty')`
  (CREATE IF NOT EXISTS) is written ONLY by `BackfillJob.warmup_gap(confirm_until=…)` under four
  guards — (A) Kite returned candles for the span, (B) Kite published a LATER candle for the symbol
  (`m < max(returned)`: a truncated day is an outage, not quiet), (C) `m < min(confirm_until,
  now−2min)` with callers clamping to `session.close`, (D) the minute is not missing for
  ≥ `max(3, ceil(0.05·answered))` swept symbols (correlated ⇒ feed gap ⇒ WARNING, never confirmed);
  a token-abort discards pending. `MarketStore.coverage_gaps` unions the table in. Feature inputs are
  unchanged by construction — no `bars_1m` row is ever written; only the gate's verdict changes.
  Repair budget: `_GAP_REPAIR_MAX_REQUESTS_PER_DAY = 900` (3 sweeps × 300) replaces 3 attempts/day.
  Opus build under my spec, audited on every pointer; ruff clean; targeted 263 / full 3066 passed.
  Whether thin names belong in intraday at all is an eligibility question — left to that layer.
- **What actually happened overnight (recorded because it matters).** My first launch of this build
  (00:30) was interrupted by the harness mid-run; the agent had already landed D1/D3/D4 and a
  single-phase D2 (guards A+C only) in the working tree. At **01:39** the engine received a
  `stop_requested` → `stop_forced` that I did not issue, stayed down 7.5 h, and **booted at 09:09:07**
  (`off_duration_s 26987`, `engine_ready` 09:10:49 — four minutes before the open) by a start I did
  not perform. That boot loaded the un-audited partial tree and ran it for the whole session. The
  second launch (14:14 — my error: I batched `Get-Date` with the fan-out, so a build ran in-session;
  memory updated) audited the partial work and completed guards B and D. Lesson: **the working tree
  is the live deploy surface** — anything else that boots the engine ships whatever is on disk.
- **Live validation (positive) — DEEPAKNTR, on the partial tree.** 13:12:34 `orb:DEEPAKNTR bars
  236/237` (a quiet minute at ~13:10); 13:12:44 the repair scanned only (hole inside the 2-minute
  trim, free); **13:17:46 `warmup_gap_done … no_trade_confirmed=1`** + `warmup_gap_repair attempt=2
  fetched=1 requests=1 requests_today=1` — one Kite request, the day's candles came back minus that
  minute, confirmed; **13:18:33 `warmup_intraday_ready`**. Guards B and D would both have passed (a
  later candle existed; one symbol). Yesterday the same symbol stayed `374/375` until the close.
- **Live validation (negative) — the audited code.** `Restart-Service` 15:33:07 (closed, nothing in
  flight, next job 15:45). `engine_boot` 15:33:24, `market_store_opened` 15:33:44, token valid,
  `agent_roster PASS (4 defs)`, `daily_gap_done skipped_covered=298 failed=0`; the boot minute leg
  15:35:51 `warmup_gap_done symbols=300 to=15:34:06 bars_written=0 no_trade_confirmed=0
  no_trade_correlated_skipped=0 failed=0` — the 13:17 row was already in the table so the minute was
  no longer a gap (no re-fetch, live), and the `session.close` clamp kept the 15:30–15:34 post-close
  minutes out (300 symbols × 4 minutes would otherwise have hit guard D). `scheduler_started` +
  `engine_ready` **15:36:17** (173 s). Risk state NORMAL, no active causes. Stop again `stop_forced`.
- **Still open:** the store/event-loop stall (every stop this week `stop_forced`; the 01:39 stop is
  unexplained); who/what boots the engine at 09:09 and from which tree — an owner decision on pinning
  the service to a commit rather than the dev working tree.

## 2026-09-17 (owner: "Fix the 1st issue and make the readiness per symbol. This is present in other gates as well, not only in intra-day") — per-SYMBOL warm-up readiness shipped (709eb5e); only REGIME/unattributable freeze now; engine restarted 18:50 IST, boot verified

- **Diagnosis (owner report 09-16 22:36: "getting blocked on intraday warmup", `orb:PTCIL bars
  374/375`).** From the tick archive, PTCIL printed no trade at 13:36 on 09-16 — the ONE in-session
  minute RELIANCE traded and PTCIL did not; the blocker first appeared 13:37:38 as `261/262`, one
  minute later. A tradeless minute has no Kite candle, so it is unfillable (the 16:44 and 22:36 fills
  fetched and wrote 0 bars); the 3/day self-heal budget was already spent on morning transients
  (`warmup_gap_repair_exhausted` 13:37:39, one second after the real hole). Same shape MRF 09-15
  (12:06/12:18/13:16/13:34), MRF 09-10 (10:42), SHYAMMETL 09-09 (14:29) — thin, high-priced names.
  Entries were NOT frozen (risk state NORMAL; ORB parked, today's 14 candidates all `swing`), but
  `ready_for(cls)` was CLASS-WIDE and both per-candidate consumers asked it per candidate, so one
  symbol's hole refused every intraday candidate in the book — and the DAILY class, being a freezing
  class, had done worse on 09-15 (OLAELEC `193/200` ⇒ 12-hour global freeze).
- **Change (commit 709eb5e; Opus build under my spec — 13 files, audited on pointers, semantics
  probed on the live blocker strings, my own ruff + targeted re-run).** `WarmupStatus.ready_for(cls,
  symbol=None)` — class-wide meaning kept for the freeze/lift/notice code; with a symbol, False iff a
  line in that class is attributed to it or cannot be attributed to any symbol (fail closed).
  `blocker_symbol(line)` is an anchored regex over exactly the three `_evaluate` renderings (`M&M`,
  `BAJAJ-AUTO`, `GVT&D`, `NIFTY 50` verified). `_freezes_entries` ⇒ REGIME + unattributable only
  (`_FREEZING_CLASSES`); DAILY joins INTRADAY on the per-symbol side; lift predicate
  `_freezing_classes_ready` = `ready_for(REGIME)`. Gate `_warmup_ready(style, symbol)`; pipeline
  pre-screen per symbol; `reapply_warmup_gate` outcome `intraday_short_` → `short_`. Owner notice
  generalised to INTRADAY + DAILY (`_log_class_transition`, symbol count + first three, silent while a
  freezing class holds). Startup step 6 appends `warmup_<cls>_not_ready` per short non-freezing class
  + one `warmup_short_not_frozen` line. `SelfTest._check_warmup_ready` (the 09-13 registered residue)
  split: FAIL+FROZEN only on raise / no `ready_for` / REGIME-or-unattributable; per-symbol ⇒ WARN.
  Plan: §2.6 step 6, a dated per-SYMBOL addendum after the 09-13 one, the FROZEN cause list, §7.1
  `warmup_ready` row. 13 existing tests re-pointed (each classified "encoded old semantics" vs
  "invariant, adapted" in the build report), 8 added. ruff clean; `tests/unit` **3055 passed** (8:11).
- **Registered risk (in the plan):** a market-wide daily hole no longer pages `WARMUP_FROZEN`; the
  DAILY transition notice ("short for N symbol(s)") and the STARTUP_REPORT `warm-up short:` line carry
  it, and every swing/position candidate is still refused per symbol — missed origination, never an
  entry on thin data. **Not done:** a tradeless minute still counts as a hole (the symbol stays refused
  for that session); synthesising zero-volume bars / upstream-confirmed-empty minutes, and not
  charging the repair budget for the just-closed-minute transient, are separate decisions.
- **Deploy.** `Restart-Service` 18:50:45 (bhavcopy 18:00, daily_bars 18:07, filings_pit 18:35 all
  complete; nothing in flight; next job 19:00). `engine_boot` 18:51:09, token valid, `agent_roster
  PASS (4 defs)`, `daily_gap_done symbols=300 skipped_covered=298 failed=0` (window 2025-11-25 →
  2026-09-16), **`warmup_short_not_frozen classes=["intraday"]`** naming DEEPAKNTR 374/375, KIMS
  370/375, PTCIL 372/375 (three tradeless-minute names today — each now refuses only itself),
  `warmup_intraday_not_ready count=3` once, `scheduler_started` + `engine_ready` **18:53:39** (150 s).
  Risk state NORMAL, no active causes. The stop again ended in `stop_forced` (memory:
  warmup-newcomer-daily-gap item 3 — store/event-loop stall, still undiagnosed).

## 2026-09-16 (owner: "Fix the 1st issue") — newcomer DAILY-history repair shipped (fdd4d83); roster opus→sonnet committed (90becc7); engine restarted 16:42 IST, boot verified

- **Context — the 2026-09-15 freeze.** Engine crashed ~01:14 (8h51m outage, missed the open), booted
  10:06; the catch-up `universe_build` added OLAELEC/ACUTAAS/ANANDRATHI at 10:09:45 and the newcomer
  fill wrote only session minutes. OLAELEC carried a 57-session `bars_1d` hole (2025-09-09..2025-12-01),
  6 sessions of it inside the 200-window ⇒ `rsi2/trend/mom:OLAELEC daily bars 193/200` ⇒ DAILY class
  ⇒ FROZEN-for-entries 10:09:51 → 22:13:54. Not a young listing (listed 2024-08-09). The engine also
  wedged at 16:06 (six hours of log silence, every evening job skipped, forced stop at 21:58). The
  owner's `backfill.py` run wrote nothing (`market.duckdb` mtime stayed 15:26 — exit-2 lock error while
  the engine was up); I re-ran `run --interval day --from 2025-09-09 --to 2025-12-01 --symbols OLAELEC`
  with the engine stopped (57 bars, 0 failed ⇒ 200/200, zero gaps since listing), restarted 22:06;
  catch-up replayed the missed evening (`daily_bars:2026-09-15` included); the 22:13:41
  `boot_incomplete` CRITICAL was a false alarm (437 s boot, `engine_ready` at 22:13:55).
- **Fix (commit fdd4d83, Sonnet build under my spec, audited on pointers + my own re-run).**
  `BackfillJob.daily_gap(symbols, sessions)`: coverage checked from the store FIRST (covered symbol =
  zero Kite requests, `skipped_covered`), writes filtered to the missing dates via
  `_write_candles(only_dates=…)` so a `src='bhavcopy'` row is never clobbered, NOT checkpointed (a
  monotonic checkpoint skips exactly these backward holes). `WarmupGate.daily_window()` and module
  `recent_sessions()` make the repair and the gate share one window definition. `job_universe` fills
  newcomers' daily history at ANY hour (the minute fill keeps its mid-session guard, fail-closed).
  `regime_and_warmup_backfill` (boot + post-login) gained a whole-watchlist daily leg. Tests: 5 new
  `daily_gap` cases (incl. the bhavcopy row surviving), window helpers, post-login leg, a source-level
  pin that the daily fill sits outside `clock.now() > session.open`. ruff clean; `tests/unit`
  **3046 passed** (5:01).
- **Roster (commit 90becc7).** `opus-5 → sonnet-5` on intraday_analyst, preopen_planner,
  weekly_researcher (owner-directed 09-15); timeouts kept. Live since the 09-15 22:06 boot.
- **Deploy.** `Restart-Service mt-engine` 16:42:20 (post-close, no job in flight; stop again needed
  `stop_forced`). `engine_boot` 16:42:38, token valid, `agent_roster PASS (4 defs)`,
  **`daily_gap_done symbols=300 sessions=200 bars_written=0 skipped_covered=298 failed=0`** at
  16:42:57 (the 2 fetched = young listings ICICIAMC/MEESHO, no pre-listing candles), `catch_up_complete`
  16:44:38 (nothing missed), `scheduler_started` + `engine_ready` **16:45:00** (142 s). Risk state
  NORMAL, no active causes. The 18:00/18:05 evening jobs fire from the new process.
- **Still open (memory: warmup-newcomer-daily-gap):** the 420 s `boot_incomplete` watchdog false-alarms
  after a large catch-up and its hint says "restart" — check for `engine_ready` first; the recurring
  store/event-loop stall (3 in 2 days, every stop since ends `stop_forced`) is undiagnosed and is
  what actually caused the day. `agents.yaml` still prices `sonnet-5` at $3/$15 (current $2/$10) — the
  governor over-estimates spend ~50% now that every agent bills at that key; owner re-sizing call.

## 2026-09-13 (owner: "Implement all of the pending changes") — tranche 3: warm-up scoping, per-source watermark, sweep mechanics, two-session validity; O17 sizing caps prepared

- **Pending list executed** (from the 09-11 report and the 09-12 recaps): per-class warm-up scoping
  (plan change), the NSE per-source insider watermark, the pre-registered vectorbt sweep-mechanics
  fix with its re-runs, two-session validity for swing entries, and the sizing caps. Deliberately not
  done: `holdings_observations` retention (a few KB a year) — noted, not built.
- **Sizing caps (O17) — PREPARED, NOT APPLIED.** I edited `config/limits.yaml` (gap mult 2.5→2.0,
  per-stock ₹8k→₹12k) and ran the re-sign; the auto-mode classifier refused
  `seed_protected_config.py --reseed` (shared-resource write), and the flow is owner-only by design
  (R4). An unsigned edit on disk would fail the next boot's integrity self-test, so I restored the
  signed file (verified: loader parses gap 2.5 / cap 8000), saved the change as
  `runbooks/briefs/o17_limits_2026-09-13.patch` (`git apply --check` clean), wrote the decision and
  arithmetic as plan §7.1 O17 (DECIDED, PENDING), and put the three owner steps in COMMANDS.md.
  Lesson recorded in memory: prepare protected-store edits, never leave one unsigned.
- **Build (engine stopped 01:22 Sun; workflow `wf_545b6008-303` lost W/N/M builders and both V
  reviewers to the session limit at ~01:40; `wf_a47bc5bd-0f0` from 02:31 finished W/N/M on the
  partial tree and reviewed V; 19 agents, done 12:20):** N passed after one fix round (the
  builder's single 180-day clamped request became the repo's proven ≤31-day chunking, and the plan's
  false "no PIT backfill entry point" sentence now names `scripts/backfill_filings.py`). W, M and V
  each survived their fix round with one re-review major — a second miss at Opus — so I fixed those
  inline: **W** — the 60 s intraday notice asserted "entries are NOT frozen" unconditionally; it is
  now silent while a freezing class (daily/regime/unattributable) is also short (WARMUP_FROZEN owns
  that state), is computed AFTER the lift it describes, and its memo is scoped to the IST day so the
  midnight rollover cannot manufacture a "coverage restored" (two new tests). **M** — the fixer had
  re-run all five cells 11:53–11:58 on the completed item (i) and corrected the record (cell B is
  PROMOTABLE at 66.7% / +0.000741%/day, not negative as the 03:11 pass said; cell A@120 passes exactly
  on the 60% bar with all nine configs negative per closed trade; verdict unchanged — `trend` is not
  a promotion candidate), but its residue bullet called the within-bar path "inert on all three
  legs", which is false for `trend` (`sl_trail=True`: vectorbt checks the trail against the prior
  bar's high-water and only then ratchets with the current high — verified in `portfolio/nb.py`);
  the plan, `STOP_EVALUATION`'s docstring and the modelling notes now disclose that, the arming-from-
  the-bar-after-the-fill residue, and that the decomposition's intermediate rows came from a
  throwaway script. **V** — the fixer had restored the never-mint-a-dead-TTL floor and scoped the
  extension to `entry=True`; the re-review showed the "no consumer needs a change" claim was false for
  both surfaces the owner browses (bare HH:MM everywhere), so the Telegram listing now carries the
  day on both stamps when a row outlives its day and the dashboard's `valid till` chip carries its
  date with a live day's fold starting open; and the new `/taken` drift check fired at 1.25× the stop
  distance when the gate had sized the swing on 2.5× (`overnight_gap_mult`) — it now reads that
  multiplier through a `RecommendationBook` seam wired from the hash-verified limits, fires only past
  the approved budget, calls out a fill on the wrong side of the stop unconditionally, and the
  registered costs name the notional-side verdicts and every `pending_rec_symbols` consumer.
- **Verification:** full unit suite on the settled tree **3036 passed** (6 m 11 s, `--tb=short`,
  engine Stopped); the six files touched by the inline fixes re-run first (440 passed); ruff clean
  on every file touched today (only the pre-existing HEAD findings remain elsewhere); dashboard
  `tsc -b && vite build` clean, then the BUILT bundle checked through `fixture_server.mjs` in an
  isolated chrome-devtools context on two ports — default: yesterday's fold starts OPEN because it
  holds the live JINDALSTEL card whose `valid till` chip reads `2026-09-14 15:30:00`; `NO_TODAY=1`:
  the panel says "no deliveries today · 1 still actionable from an earlier session" instead of the
  old empty state. The vectorbt claims behind the M correction were read in
  `.venv/.../vectorbt/portfolio/nb.py` (stop check ~2025–2034 precedes the ratchet ~2046–2051), not
  taken from the reviewer. `config/limits.yaml` untouched (loader parses gap 2.5 / cap 8000).
- **Deploy:** committed as `c60a527` (31 files); clock re-observed 12:57 Sun (no session);
  `Start-Service mt-engine` 12:57, boot verified from the log — self-test all PASS incl.
  `protected_store:limits.yaml` / `envelope.yaml`, `startup_report` NORMAL / nothing frozen,
  `engine_ready` 12:58:12, no ERROR/CRITICAL; `warmup_intraday_ready` logged once as a first
  observation with no owner notice (Sunday: no session, the new day-scoped memo behaving as designed).
- **Config applied on this restart:** none beyond code; O17 awaits the owner's reseed.

## 2026-09-12 evening (owner: "Proceed with the rest of the recommended changes that you deem will improve the engine's performance") — tranche 2: hi52 promoted as a forward test, brk20 retest re-arm, insider-feed coverage, intraday fade/ATR refutations

- **Scope decided by the manager under the owner's blanket authorisation:** hi52 v2 promotion (with a
  written kill rule — the owner had asked "will it yield better results?"; answer: yes in expectation,
  modest ₹, survivorship-tainted backtest, so promotion IS the forward soak), the brk20 multi-session
  retest re-arm the 09-12 backtest selected (V2-5), the insider-feed issuer starvation (diagnose, fix if
  local), two exit-hygiene residues, and the two intraday hypotheses the reviewer raised (ATR-conditioned
  population; short/fade side) as pre-registered backtests. Deliberately NOT done: sizing caps (owner
  risk decision, protected store), per-class warmup scoping (plan change; the freeze-lift sweep already
  recovers the batch legs), the vectorbt sweep-mechanics fix (research infra, pre-registered as its own WO).
- **Build (engine stopped 17:47 Sat; workflow `wf_08611cfe-523`, 17 agents, then `wf_156b3f23-5b8` after
  the session limit killed H's fixer and R's builder at ~21:2x; reset 21:30):**
  **WO-H hi52 v2 promoted** — the v2 filters (smooth approach, no gap day) now GATE in `hi52.scan_daily`
  (owner-only thresholds, `VETO_SMOOTH`/`VETO_GAP` on the sweep line); `hi52.expected_edge_pct: 1.47`
  registered on the ins seam by ONE derivation that lives in code (`scripts/hi52_forward_verdict.py
  registered_edge_pct()`: measured T+20 median GROSS 2.0336 − CostModel round trip at the §7.1-sized
  notional `equity × 2% / (2.5 × 6%)` ≈ ₹4.8–5.3k ⇒ 0.56% ⇒ 1.47 rounded DOWN); `Hi52Cfg.expected_edge_pct`
  is `float | None = None` so DELETING the key really un-registers it (demotion = re-add to
  `NO_EDGE_SHADOW_STRATEGIES` + delete the key, stated in all four homes); hi52 joins the ONE ranked batch
  admission (cap allocation) and the batch is PUBLISHED in leg order (brk20, ins, cat, cat_reversal, hi52)
  so cross-strategy raw scores never set the forward queue's fired_at tie-break; `_HI52` contract rewritten
  (approach shape is NOT a decline ground; decline grounds mirror ins; FORWARD TEST, survivorship stated);
  kill rule in the plan §8.6 addendum and mechanical in `scripts/hi52_forward_verdict.py` (population from
  2026-09-14, entry = open of the journal day d, exits close(d+k−1), DEMOTE if n ≥ 20 and (T+20 median net
  ≤ 0 or hit < 50%); T+5 printed as a diagnostic only). Two reviews + fix + re-review PASS (246 tests).
  **WO-R brk20 retest re-arm** — migration 0014 `brk20_resting_levels`; `strategy/retest.py`
  `RestingLevelBook` (record on ADMISSION, idempotent per (symbol, signal_d); expires after 5 trading
  sessions; `due()` re-offers a level whose live price is back inside the CNC band, once per symbol per
  day, with a fresh signal_id and the original levels/score); wired on the 60 s drain tick OFF-LOOP
  (`asyncio.to_thread` — the body takes the prescreen lock then the store lock), only after today's
  in-window sweep has published (`sweep_ready`), with a not-offerable screen (held / pending entry rec /
  A12 ex-date / no longer eligible) and a fresh-tick test (`stale_data_guard.max_tick_age_s`); resting
  symbols join the ticker subscription set. Reviews found a blocker (on-loop lock hold) + 2 majors (no
  one-trade-per-level screen; unbounded tick age), fixed; re-review left one major (the offer bound spent
  before admission ⇒ an offer into a boot-time freeze silenced the level for the day) — **taken inline
  (Fable, second miss):** `_retest_active` = sweep window AND risk NORMAL AND not killed AND no sweep in
  flight; plus the BACKWARD half of the corp-action veto (`brk20_unadjusted` → `unadjusted` skip reason).
  **WO-I insider-feed coverage** — cause was LOCAL: `symbol_isin` had 200 rows (as_of 07-17) vs 480 eligible
  since O15, no refresh job ⇒ 281 issuers unmappable, their rows dropped at resolution. Fix: per-run scrip
  map = stored ∪ (index CSV ⋈ BSE bulk master `ListofScripData/w`), stored-wins, per-stage funnel on the
  run line; NSE `corporates-pit` confirmed stale UPSTREAM (05-02) and `filings_pit`'s whole-table watermark
  recorded as the reason a revived route could not backfill. Two reviews + fix; re-review left 2 majors —
  **taken inline (Fable, second miss):** coverage FLOOR (`MAP_COVERAGE_FLOOR_PCT` 0.90 with a 5-name
  NSE-only allowance) instead of a presence bit, an empty master body takes the 15-min cooldown and is
  NOT memoised for the day, NaN considerations read as no value, duplicate-ISIN constituents counted apart
  from `shadowed` (85 tests green).
  **WO-X** `_positions_summary` excludes journal-gone positions (trailing "sold outside the ledger" line);
  post-login report carries the observed count. PASS.
  **R3 (pre-registered, RUN):** daily-ATR% tercile split on the tdc and candle harnesses — the rebuttal is
  partly real on GROSS (tdc H1 top tercile +0.082% vs +0.035% pooled; B +0.103%) and REFUTED on NET in every
  cell (top tercile costs 0.182% vs 0.151% — high-ATR names are the cheap ones; best cell −0.10% net,
  t −1.65, CPCV 13%); candles show no gross gradient at all. Audit PASS.
  **R4 (pre-registered, RUN):** short/fade side — F1 failed-breakout fade n=5,491 gross +0.050% vs cost
  0.153% (net −0.103%, t −7.5, CPCV 0/15); F2 gap fade n=1,819 gross +0.026% vs 0.178%; every split
  negative; the only positive n≥200 cell (F2 held to 15:15 on a rising tape, +0.15%, t 1.94) fails the
  registered t > 2 bar and is a split of a reporting-only variant. The fade is real and one third of its
  toll, exactly as the breakout was. Audit PASS. Intraday now has FIVE refuted families.
- **Verification:** per-WO suites green; full unit suite on the settled tree **2984 passed, 4 warnings,
  4m48s** (2026-09-13 00:0x). ruff across every modified file: only the five findings that exist at
  HEAD (three B905 in hi52.py/test_hi52.py, two pre-existing F401). Live
  wiring check: shadow set {cat, cat_reversal}; edge map {ins 1.58, hi52 1.47}; leg order
  (brk20, ins, cat, cat_reversal, hi52); `settings.brk20.retest_sessions` 5.
- **Config applied on this restart:** settings.yaml `hi52.expected_edge_pct: 1.47`, `brk20.retest_sessions:
  5`; migration 0014 at boot. Kill-rule review: run `scripts/hi52_forward_verdict.py` at 20 and 40 promoted
  signals (first review ~mid-October at 1–2 signals/week).

## 2026-09-11/12 (owner: verify the external drought audit → "Proceed with the suggested changes"; weekly quota governor) — recommendation-drought tranche

- **Verification of the 2026-09-10 external diagnostic** (`runbooks/audit_verification_2026-09-11.md`,
  refutation workflow `wf_0dbf2a35-c61`, 10 refuters + critic): the audit's counts held (71 recs = 68
  repeat exits + 3 entries; 0 intraday ever), its causes were mostly overstated or stale (band sole
  cause 3/15 brk20; sizing sole cause 1/23; sentiment rail fixed 09-04; the 97.4% no_action framing
  counted heartbeats and position calls; prompt/confidence remedies rejected). What it missed and we
  measured: the governor's DG1 per-agent trip (news_analyst $79.23 of a monthly $80) cut the forward
  cap 48→4 on 09-10 for the rest of the month; exit outputs failed schema on 100% of first attempts
  since 09-03 (`reason` free-text on the wire vs the five-code Literal); batch legs re-armed into
  boot-time freezes were never re-published (08-28, 08-31, 09-04 lost whole); the forward queue died
  with every owner restart (~70 candidates); ins is fed by a 22-issuer BSE feed; orb took 44% of
  forwards for 0 enters; rsi2 is regime-gated since 08-28 (NIFTY 50 < 50-DMA). Reviewer rebuttal
  re-derived: rsi2's "80% CPCV" is the July report superseded 08-14; shorts are not disabled (orb
  emits SELL; §1.4.9 gates AUTO); intraday mean reversion was tested (WO-10b, aborted).
- **Owner 2026-09-12: the SDK bills the subscription's WEEKLY quota, reset Thursday 14:00 IST** —
  governor re-keyed on that window (below); monthly framing retired.
- **Build (engine stopped 00:53 Sat, weekend idle; workflow `wf_2d635b12-214`, 25 agents, then
  `wf_9eb3af02-0c6` after the session limit killed three fixers and the D2 builder at ~01:47):**
  WO-A weekly governor (window key = start-Thursday date, spend by `budget_ledger.at` range + index
  0012, half-session boundary Thursdays, UNMEASURABLE pace = None skips the pace rungs, DG1 on pace
  only, per-agent >85% degrades that agent alone, degraded cap 0.67×base, DG4 latch per window;
  /budget API + Telegram + BudgetPanel + g2_evidence on the window; plan §5.6/§11.2/T6). WO-B
  `exit_reason` enum on the wire + sanitizer mapping + codes named in the position-event context.
  WO-C freeze-lift sweep (engine_ready gate, single-flight lock, lift queues behind an in-flight
  sweep, ins consumed only under NORMAL) + admitted batch symbols in the ticker set. WO-D1 forward
  queue: orphan re-arm on boot roll, window-close flush per window END, cap refund memoed per
  candidate, front entries re-armed, MIN_RANK_POPULATION 3, brk20 band screen deferring (ltp_fn).
  WO-D2 exit hygiene: `holdings_observations` (0013), two-session missing predicate with
  `require_zero`, position-event screens (sold-outside-ledger; one exit per session), day-plan
  "sold outside the ledger" block. WO-E orb parked (`orb: 0` = admit nothing), `ins_feed_coverage_low`,
  `rsi2_regime_blocked`. Every WO: two adversarial reviews + one fix round.
- **Taken inline (Fable, revision-gate escalations):** nightly funnel keeps a row for a strategy that
  fired but never published (E's second miss); the lift's in-flight test is `_sweep_lock.locked`,
  never a phase-mark inference with a grace latch (C's second miss — a raising sweep never stamps
  `done_at`); brk20 report wording: the matched "price effect" cell is price-AND-timing for fills at
  delay ≥ 2 (R1's second miss; artifacts regenerated, decision unchanged); Kite `collateral_quantity`
  counts as held (D2's second miss — a fully pledged position read held 0 and would have silenced its
  own exits); gate excludes positions the broker shows empty on two sessions from the position/sector
  counts via `missing_holdings_fn` (D2's accepted gap: no exit rec ⇒ aged out of `_exiting_symbols`
  ⇒ re-took a slot). Dashboard dist rebuilt after A's re-review.
- **Research (evidence only):** R1 `scripts/backtest_brk20.py` (pre-registered): V1 next-open dead at
  T+5/T+10, +0.63% at T+20; V2-5 limit-at-level 59% fill, T+10 +0.05%, T+20 +0.92%, wins the
  registered rule; matched cohorts show mostly selection. R2 trend at `--margin-floor-days 120`:
  passes the cap floor (3.9×) but 1.07× at the realized median hold (33 sessions) and FAILS (0.72×)
  in the 2024→2026-09 cell; three sweep-mechanics biases (close-only daily stops, uncharged stop
  slippage, open trades in expectancy) pre-registered as a follow-up WO. Not promotion candidates.
- **Verification:** per-WO suites green after fixes; full unit suite on the settled tree **2831 passed,
  4 warnings, 6m08s** (an earlier run showed 22 failures in the wiring module that were my own
  mid-run edits to main.py landing under it — the module passed 219/219 in isolation and the clean
  re-run was green). ruff: only
  pre-existing findings remain (app.py ×4, g2_evidence B007, two B905 and one F401 at HEAD).
- **Config applied on this restart:** agents.yaml weekly credit $200 (intraday 90 / news 90 /
  preopen 8 / nightly 5 / researcher 2 / reserve 5; ~2× the peak week of 09-03) + quota_window;
  settings.yaml `orb: 0`. Migrations 0012/0013 apply at boot.

## 2026-09-10 (owner: "work on the tasks that are doable right now, with proper validation … the end goal of this service") — Phase 3 foundation tranche

- **Decomposition recorded** in the plan §8.4 (2026-09-10 addendum): WO-P3-1 OMS core, WO-P3-2
  PaperBroker v1, WO-P3-3 ReplayHarness v1, WO-P3-4 fill-model calibration = the tranche buildable
  before G2 (Tier-3 code RECOMMEND never reaches — §8.3's zero-API-orders constraint untouched);
  WO-P3-5 AUTO(paper) routing and WO-P3-6 the R3 managers gated on G2 + owner sign-off. Evidence
  base: three sonnet readers (plan spec, code seams, test infra) — KiteClient's order/GTT surface is
  duck-typed (no Protocol), the state.db orders/order_events/positions/gtts tables already exist,
  every Phase-3 hook is a documented `None` seam, the tick archive holds 26 sessions
  (2026-08-05→09-09, ~300 symbols/day, L1 bid/ask on every tick), and `MarketStore.get_ticks` reads
  through the locked DuckDB connection — so the replay and the calibration read Parquet with their
  own in-memory DuckDB and never open `market.duckdb`.
- **Build round (01:25 launch, 4 Opus builders + 8 two-lens reviewers):** all four builders green
  on their own tests (OMS 110 incl. 2 Hypothesis properties; PaperBroker 71; replay 16 + a real
  RELIANCE symbol-day replayed twice to a byte-identical digest; calibration 10). Six reviews
  returned `fix_needed` with real defects — OMS: a replayed OPEN frame moved CANCEL_PENDING back to
  ACKED, a qty-modified SL-M's own fill raised, tautological table tests, the property generator
  never started uncorrelated; PaperBroker: participation cap per tick (probe 101–130 % of a
  minute's tape), GTT legs validated only at fire time inside `on_tick`, a volume reset removed the
  cap, no session-boundary lapse; Replay: the day sort was not total (≈28 tied groups per
  symbol-day differing in bid/ask), the postback seam matched nothing the broker exposes, the whole
  day materialised twice (~17 GB for 8×10⁶ ticks), an `async run()` with no await. Decisions
  ratified in the plan §8.4 addendum. Fix round with re-reviews scheduled for 15:31; the owner
  asked why wait, heard the shared-subscription trade-off (12 Opus agents ≈ 1.7M tokens inside
  the session window, the 09-03 blackout shape) and said "run it now" — launched 11:0x with a
  monitor on the engine's LLM-failure count. **Outcome:** the session limit tripped ~12:10 (reset
  14:00); the engine's analyst calls failed from 12:13 (83 `agent_call_failed` by 13:57, the 09-03
  shape) and two replay reviewers died on the limit; the owner said "ignore engine issues due to
  claude session limit". Round 2 (4 fixers + 6 surviving reviews) closed the round-1 majors and
  found: a BLOCKER (the calibrator wrote the per-symbol half-spread as a fraction, the fill model
  reads a percent — every fallback fill charged 1/100th of the spread), the fitted k for the open
  bucket 15× below the uncalibrated default (estimator zero-inflated; policy set: a calibration may
  only tighten), a zero-session run overwriting the YAML with defaults, the modify-intent gate too
  narrow, the A13 rule missing the minute-boundary case, and the real-broker golden test being
  thread-timing dependent. Round 3 launched 14:1x on the same authorisation.
- **Round 3 (12/12 agents):** replay PASSED both lenses (scripted-action seam makes replay actions
  a function of the stream — six runs incl. three under CPU hogs, one digest; per-slice yields;
  RELIANCE symbol-day 9.1–10.4 s warm); calibration and PaperBroker closed their majors; the OMS
  property suite went RED (the new terminal guard on `correlate` vs the driver) and two OMS
  semantics gaps remained (a stale-but-grown frame still reverted amended fields via the fill door;
  `record_noop` accepted an unannotated accretion self-edge). **Taken inline (Fable, attempt cap):**
  `apply_update` splits the two decisions (fill lands, stale fields ride the event as
  `fields_reported`), `transition` gained an `annotations` pass-through, `record_noop` requires a
  `NOOP_FLAGS` annotation, the property driver pins that `correlate` refuses terminal orders and
  that no generated frame ever yields an `illegal_transition` no-op — 197 OMS tests green across
  four Hypothesis seeds. Round 4 (2 fixers + 2 reviews) on the PaperBroker guard gaps and the
  consumer half-tick floor launched 15:0x — **both PASSED** (minors: the "failed" latch made
  conditional and a stale docstring fixed inline; the YAML header now states the half-tick floor).
- **Final gate + artefacts:** unit suite minus the tranche files 2452 passed (9:18), property +
  replay 33 passed, paper/calibration 134, OMS 197, ruff clean over every touched file. Calibration
  regenerated 15:23 (22 compacted sessions, 5 skipped `not_compacted`, 150 s): all three buckets
  ship the consumer defaults (basis `degenerate`; open p75 0.0673 / p90 0.3065 recorded), 200
  per-symbol half-spread medians in PERCENT (p50 0.0127 %), and the report states that 57 of 200
  names sit below the 2-tick fallback at the Rs 1000 reference — the evidence behind the half-tick
  consumer floor. **No engine restart needed** for this tranche (nothing is wired into the live
  path; the `OrderUpdateFrame` move is a re-export) — it deploys with the next idle restart.
  Attempt accounting per the revision gate: 4 delegated rounds per work order (the cap), with the
  OMS semantics closed by Fable inline at the cap.
- **First session on yesterday's code (verified from the log, 09-10):** boot 09:06:34 after the
  overnight sleep; `brk20_sweep` 09:32:35 scanned=480 cands=6 gap_floor_vetoes=5 unadjusted=0 and
  `hi52_sweep` 09:33:15 scanned=480 cands=6 unadjusted_vetoes=31 (eligible scope + both vetoes live);
  `tick_lag_watch_grace_armed` on each of four ticker respawns 09:07–09:21 (a real in-session lag
  pair at 09:18:48 during the boot backfill paged once, correctly); the per-day announcements poll
  returned the day's rows from 00:02; `store_slow_statement` named the boot-window holders —
  `bulk_write:instruments_daily:101427` 16.0 s, `copy_ticks:NH` 16.0 s, `upsert_rows:sentiment_agg`
  7.1 s, `SELECT * FROM instruments_daily` 6.3 s — the 09:17/09:18 `store_stalled` pulses now have
  named statements. **Early hydration ran once, wrongly late:** the owner logged in 09:06:51 (before
  the open, gate passed), the hook waited for arming (09:08:04), then `hydrate_ahead` queued 36 min
  on the pass lock behind the post-arm one-shot draining the overnight news backlog and ran at
  09:44:20 in-session (every job `already_run`, harmless). Fixed inline, red-first: `hydrate_ahead`
  takes `not_after` (the session open) and re-checks it once the lock is HELD
  (`early_hydration_skipped_not_after`); 129 tests green across the three touched files. Deploys
  at the next idle restart.
- **Process note, self-reported:** the build round was launched at 01:25 (idle window) but the PC
  slept; the last three agents resumed with the box at ~09:48 and were still running inside the
  session when I checked at 10:49 — stopped immediately (no engine LLM errors seen). A workflow
  that can outlive the idle window must be bounded to end before 09:15 or be stopped by a guard.

## 2026-09-09 (owner: "work on the owner-decision tasks and engineering follow-ups as per your best understanding; early hydration when I log in at ~06:30") — inline batch shipped; delegated batch designed

- **Owner decisions taken (delegated, "best understanding"):** hi52 origination restricted to the
  ELIGIBLE universe (the 09-03 full-market backtest: index-class edge only; extended −0.26%/49% at
  T+20) — shadow clock restarts 2026-09-10; hi52 **v2 pre-registered** in the plan (N=2; smooth
  up_day_frac ≥0.55 ∧ max_day_move ≤0.07 over 20 sessions, trigger-day |move| ≤5%, index population;
  thresholds fixed from the 09-03 medians). tdc thin cells: no action. WO-12 wake task: superseded
  by early hydration (a laptop that is asleep cannot be woken by its own task).
- **Inline batch (commit 6507fd7, tests green, ruff clean):** brk20 trailing corp-action veto
  (35-calendar-day window, `unadjusted_vetoes` on `brk20_sweep`); hi52 scope; NSE announcements
  polled per day (`from_date/to_date`, probe: 745 rows for 08-09 vs the bare endpoint's 20) —
  the 20-row cap noted on 09-04 is gone; backtest survivorship caveat corrected. Hygiene: stash
  `context-daily-tail` dropped (superseded by 334d992); AUDIT_PROMPT.md committed (8ec4c5f); the two
  transcript scrapers stay untracked (owner's scratch).
- **Store-stall 09-08 11:48 (evidence, sonnet reader + my read of `_flush_locked`):** the thread
  dump is sequential and cannot name the holder (feature_snapshot INSERT vs tick-flush COPY, both
  past the acquire line); `_lock` is already per-statement and `_tick_stage` is already TEMP, so no
  structural fix is indicated — per-statement timing (`store_slow_statement` ≥5 s) ships instead so
  the next stall names its statement. Backlog by day: 09-03 0 backlog events / 1 stall pulse,
  **09-04 46 / 17** (watchlist 200), 09-08 14 / 7 (watchlist 300) — the class is intraday load,
  NOT the O16 watchlist raise.
- **Tick-lag 09-03 15:40 burst:** 14 ERROR/recovered pairs in 83 ms + 6 pages — Kite's connect-time
  snapshot ticks (last-trade stamps ~15:31) inside the deliberately-watched 15:30–15:45 buffer flap
  the single global episode. Design: a 30 s reconnect grace armed from the `feed.health` bus
  (WARMING/HEALTHY transitions); no wire-schema change.
- **News resolve lock:** `score_news` held `news_chain_lock` across every chunked LLM call → 14
  `news_resolve_timeout` on 09-07 (9 in one second at 10:12:40). Design: same lock, hold only the
  store read and a CONDITIONAL write-back (never re-create a merged-away cluster).
- **Early hydration (SHIPPED 3452ad3):** `CatchUpRunner.hydrate_ahead` + an `EarlyHydration`
  login hook (trading day, login before that day's session open, waits for `scheduler.start()`
  and for `PostLoginRecovery.completed`, re-checks the open after the waits) runs surveillance →
  universe → news chain → digest → planner ahead of their clock and records today's watermarks;
  one owner summary line. **Design reversal during review:** the draft made `_scheduled_runner`
  skip watermarked jobs — that pinned a 06:30 digest/plan and a pre-08:00 instruments map for the
  whole day (07:00–09:00 filings are the largest catalyst class), so the scheduled fires stay
  UNCONDITIONAL and the watermark guards the sleep case only (wake at 09:30 keeps the 06:30 run).
  `instruments` excluded (Kite's dump refreshes ~08:00 IST); sleep-case residual recorded (universe
  stays on the 06:30 map that day). Owner-visible: on an awake-PC early-login day a second
  pre-open plan message arrives at 08:50.
- **Delegated batch — three rounds, all reviewed:** round 1 (5 implementers + 10 two-lens
  reviewers) found the two spec errors above plus: the conditional write-back re-emitting the
  pre-LLM snapshot (a poll merge reverted — reproduced), no scoring single-flight (chain batch vs
  5-min tick), the store telemetry logging under the re-entrant lock and splitting one read into
  two events, the reconnect grace blind to in-child KiteTicker reconnects, the v2 report citing
  N=1 and N=2 in one document. Round 2 (5 fixers + 10 reviewers) fixed those; round 3 (5 small
  fixers) closed the residuals (reconnect signal made in-band via a `connect_seq` on the heartbeat
  emitted BEFORE re-subscribe; five hand-rolled store holds instrumented; gate re-checked after the
  waits; depth-counted recovery event; v2 note scoping + coupled gap thresholds). Full unit suite
  **2222 passed** (3:52) + hi52 harness 32 passed; ruff clean on every touched file (one
  pre-existing F401 in test_ops_main_wiring.py:1824 left alone). Commits: 47b33bc tick-lag grace +
  connect_seq, dc73e28 news lock, 3452ad3 early hydration, 93d8bb0 store telemetry, 141a10f hi52
  v2 harness. bar_reconcile failed 15:50:03 on a Kite `RemoteDisconnected` and the 16:15 sweep
  re-ran it clean at 16:17:19 (self-healed as designed).
- **hi52 v1/v2 backtests RUN 16:30:48–16:31:33 (engine off; 24 s + 21 s — the 25-min figure I had
  in mind was the 09-03 archive backfill, not the harness):** same window 2020→2026-09-09, 3,205
  symbols. v1 discrete 16,121 trades: geometry dead T+5/T+10, +0.10% at T+20; index cell T+20
  +1.54%/56.4%. **v2 discrete 1,794 trades (vetoes smooth 11,405 / gap 813 / not_index 2,625):
  viable at T+10 (+0.62%/54.0%) and T+20 (+1.71%/58.2%), CPCV promotable at T+10 (80%) and T+20
  (86.7%) under N=2.** Reading recorded in the plan: the index restriction carries the edge; the
  smooth/no-gap filters trim a third of the index population without raising the median (smooth
  +1.48% vs jumpy +1.82% inside v2). §8.6 unchanged — shadow soak on the eligible population from
  09-10 + owner sign-off before any RECOMMEND wiring. Reports: `data/reports/backtest_hi52_2026-09-09.json`,
  `backtest_hi52_v2_2026-09-09.json`.
- **Deploy 7b60046 (code 141a10f) at 16:32:13,** engine off 16:30:28→16:32:13 for the two runs, no
  job in flight (next scheduled 18:05 daily_bars): boot verified — token valid 16:32:37,
  `startup_complete` 16:34:33, `engine_ready` 16:34:38, zero ERROR lines. First early-hydration
  morning = the next trading day the owner logs in before 09:15. 10 commits unpushed (push is the
  owner's call at phase end).
- **Process miss, self-reported:** launched the 5-implementer workflow at **14:36 IST on a trading
  day** on a stale clock assumption (the compaction summary said ~00:1x). Caught by a `Get-Date`
  three minutes later, stopped before any file was touched; engine log shows no LLM pressure.
  Rule re-affirmed: `Get-Date` immediately before any fan-out, not just before service actions.
  At 14:44 the owner said "you can restart the engine and continue your work now" — taken as
  their call on both counts (trade-off stated: the swarm shares the subscription window with
  the analysts for the last ~45 min); the workflow relaunched 14:54.
- **Deploy 6507fd7 (owner-authorized mid-session restart 14:44:49):** boot verified — token
  valid 14:45:22, `startup_complete` 14:47:14, `engine_ready` 14:47:20, `boot_contract_ok`
  14:52:21 (420 s). Boot-window noise, all self-healed: `store_stalled` 14:48:37 for 60 s during
  the warm-up gap repair (the known post-boot class, same shape as 09-08 10:14); Telegram
  `TimedOut` ×2 at 14:49 with `notification_retry_delivered` 14:50:28; one intraday_analyst
  `schema_invalid` at 14:48:39 followed by a clean call at 14:48:57 and a delivered rec (a schema
  retry, NOT the subscription limit). The window-open sweep did NOT re-fire (the window state is
  sticky across a restart, so there is no INACTIVE→ACTIVE edge today): the brk20 veto and the
  hi52 scope take effect at the 09-10 window open.

## 2026-09-09 (00:0x, owner: "trade for Monday has been done — what is the status now?") — G2 re-measured; ledger vs holdings discrepancy

- **G2 collector (2026-07-29..09-09, 31 sessions):** digest-before-open 19/31 = **61.3%** (falling —
  09-07 and 09-08 both booted after 10:00 because the PC slept: Sun 21:00→Mon 10:12, Mon 18:00→20:42,
  Mon 23:0x→Tue 10:08, Tue 16:08→20:03, Tue 23:08→~00:00); schema-valid 99.9% (MET); delivered in time
  57/57 (MET); **owner-executed taken=2 closed=0 (NOT MET — no `/taken` or `/closed` since 08-27; if a
  trade was executed on Monday it is not in the ledger)**; sessions with ≥1 rec **10/31** (08-26→09-08,
  ten consecutive trading days; 20 needed ⇒ ~09-22 at the earliest); zero API orders (MET); budget
  $122.37/$550 MTD (MET); watchlist-precision + payload samples still unrecorded.
- **Ledger vs holdings:** the 09-07 holdings reconcile (06b6860) reported on 09-08 10:11 and 11:08
  **HDFCAMC tracked 3 / held 0 and HINDZINC tracked 7 / held 0** — both positions are gone from the
  Kite holdings but still OPEN in `positions`, so the engine issued 8 exit recs for them on 09-07 and 7
  on 09-08 (all expired), the two CNC slots stay charged, and their outcomes never reached the ledger.
  They need `/closed <rec_id|ticker> <price>` (the sale price) — that also produces the first two
  captured outcomes for G2 criterion 4.
- **O16 first day (09-08):** verdicts 8 approve + 1 reject; the ONE entry recommendation of the two
  sessions — **JINDALSTEL BUY qty 5, zone 1149.80–1172.80, stop 1090.50, ₹5,749 notional (`ins`
  leg, gate approve, 10:27, valid to 15:30) — expired unactioned.** Monday: 0 entry recs (window
  10:30–12:50 after a 10:13 boot; 4+4 exits). NIFTY 500 builds ran on both boots (09-07: 480 eligible
  / 200 watchlist; 09-08: 481 / 300 after the O16 watchlist raise); hi52 `unadjusted_vetoes` 69 on
  both days (the veto is live and backed).
- **store_stalled paged for real on its first armed day (d2c5c8c):** 09-08 10:14:57 (ping unanswered
  55 s, consecutive=2, during the in-session boot's ~300-name warm-up backfill; recovered 10:15:24
  after 100 s) and 11:49:00 (74 s, mid-session, no boot in progress; recovered 11:49:xx). Both
  delivered, both recovered once. The 11:48 one is a genuine mid-session stall of the WO-24 class —
  worth a look at what held the store lock (features/compaction?) before it repeats.
- **All 09-08 EOD jobs succeeded** (bhavcopy 20:08 … nightly_review 21:02, tick_compact 22:38) after
  the 20:03 wake. Branch phase2 is in sync with origin (pushed by the owner/peer); the 09-07/09-08
  sessions shipped O16 (5e4416a, d2a9077), holdings reconcile (06b6860), dashboard day folds (78827e3).
- **Pending, in priority order:** (1) keep the PC awake / register the wake task — every late boot
  costs the open and the digest criterion; (2) `/closed` the two phantom positions; (3) record any
  executed rec with `/taken`; (4) the owner-manual samples; (5) Phase 3 build (unchanged: 0 files).

## 2026-09-08 (11:19–11:4x, owner: "date separation on recommendations / decision log; swap notifications and news") — dashboard day folds + panel order

- **Owner-reported:** the recommendations card lists every delivery the route returns (latest 100)
  with only a time-of-day, so cards from different sessions were indistinguishable and the list grows
  without bound; the decision log has the same problem; notifications should sit above news/catalyst.
- **Change (dashboard only, no engine code):** `ui.tsx` gained `istDay` / `todayIst` (Intl,
  `Asia/Kolkata` — the browser's tz is irrelevant), `groupByDay` (newest day first, undated bucket
  last, server order kept inside a day), `useDayFolds` (today open, earlier days folded; click
  overrides are session-local; defaults re-derive each render so the midnight rollover moves the open
  fold on the next poll) and the `DayFold` header button. Recommendations and decision log render one
  fold per day — header = date · weekday · `today` chip · count, aside "N today · M shown", a
  "no … today" line when today is empty; the decision log keeps a single sticky column header (one
  `<tbody>` per day). `App.tsx`: NotificationsPanel now precedes NewsPanel (reverses 2026-08-21).
- **Verified:** `npm run build` clean (tsc + vite, dist 11:28). Page driven in Chrome against the new
  dev-only `dashboard/fixture_server.mjs` (dist + canned routes across today / −1 / −3 / −4 days, a
  UTC-stamped row, an undated row): today's fold open in both ledgers, earlier days folded
  newest-first, undated last; the `…T19:30:00Z` row folded under the correct IST day; the
  delivered_at-null row folded under its payload `created_at` day; clicking 2026-09-07 expanded it and
  the fold survived four 10 s polls (11:29:51 → 11:31:21); panel order … Live events → Notifications
  → News/catalyst; console clean apart from the fixture's missing `/ws/live`. `NO_TODAY=1` run
  (11:33): asides "0 today · 7 shown" / "0 today · 5 shown", "no recommendations today" / "no
  decisions today" lines, every day folded, zero cards / rows rendered.
- **Ops:** static rebuild only — the engine serves `dashboard/dist` from disk, so the change shows on
  browser reload; no restart, engine untouched mid-session (window open).

## 2026-09-08 (00:15–00:2x, owner: "can't the new universe be built now?") — offline build + restart

- `scripts/build_universe.py --date 2026-09-08` with the engine stopped (00:16:16 → build 00:16:59 →
  start): **NIFTY 500 (499) → eligible 481, watchlist 300, mis_candidates 208, extended 427, source
  download, degraded=false.** Boot: `selftest_complete` ok 00:17:40, `catch_up_complete` 00:17:41,
  `startup_complete` 00:17:44, ticker spawned/handshake with **302 tokens** (300 + NIFTY 50 + VIX) —
  the 300-name watchlist is subscribed overnight, so the 08:30 build (idempotent replace-write with
  the morning's fresh lists) should be a no-change and the open needs no resubscribe/backfill of new
  names. Miss on my side: the in-flight probe (90 s of kite/compaction events) did not see the 22:30
  `tick_compact` still running at 00:16 — the stop cancelled it (job exception at 00:16:17); the
  post-arm/catch-up path re-runs it overnight (recovery is idempotent per partition). Verified below.

## 2026-09-07 (10:4x–13:xx) — status; swing "missing link" investigation; O16 (exiting positions off the buy caps, caps 6/2/4, base ₹40,000)

- **Status 10:42:** PC slept Sun 21:00 → Mon 10:12 (log gap; ticker silence 47,521 s on wake), owner
  restart 10:13, login 10:16, window 10:30–12:50, NORMAL 10:24. First NIFTY 500 build 10:14:33: index
  500, eligible 480, watchlist 200, extended 428. brk20 already originating tail names (NIACL, TEJASNET,
  ACMESOLAR, WOCKPHARMA); declined-displacement fired twice (GLAND/INDIGO → DIVISLAB/BOSCHLTD). NSE
  feed 4 polls / 43 filings; Telegram back (0 failures). 14 `news_resolve_timeout`: post-sleep scoring
  backlog held the chain lock (news_analyst back-to-back 10:18–10:41) — self-cleared 10:41; design note:
  scoring should not hold the resolve lock.
- **Owner: "stocks doing well not recommended — find the missing link" (IFCI, MOREPENLAB, PCJEWELLER,
  SHYAMMETL, WELCORP).** Membership: IFCI/SHYAMMETL eligible only since today; WELCORP ASM; MOREPENLAB
  ASM + outside NIFTY 500; PCJEWELLER outside. None ever had a slot. **The link is the swing gate +
  capacity:** last 15 sessions the analyst said `enter` 16× (brk20 11, rsi2 4, cat 1) → gate 14 rejects +
  2 shrinks; the 2 shrinks are HDFCAMC/HINDZINC, now through their stops with **39 expired exit
  recommendations**, holding CNC 2/2 since 08-27 (today CEIGALL declined 4× for capacity). Reject
  reasons (verdict payloads): `per_trade_risk` 14/14 (2.5× gap mult × stop vs 2% of ₹20k = ₹400),
  `min_viable_size` 9 (null-target legs "unverifiable"; tiny qty vs 2× breakeven), `entry_sanity_band`
  5 (brk20 retest limits 3–5% below LTP vs 2% CNC band), capacity/sector 5, confidence 2. rsi2's 21
  analyst timeouts were all on 08-24 (incident day) — not structural.
- **Owner-directed O16 (three changes):** (a) "keep sell recommendations separate from the buy limits"
  → `_exiting_symbols` / `_active_counts` (gate.py): OPEN positions with an exit rec delivered within
  3 calendar days are excluded from `max_open_positions` and `per_sector_exposure` counts (still
  one-per-symbol, still deployed cash; ledger line names the exclusion); tests
  `test_exiting_positions_do_not_occupy_position_or_sector_slots`,
  `test_max_open_positions_ledger_names_the_exiting_exclusion`; gate+pipeline+wiring 335 passed.
  (b) "raise the cap to 4" → `max_open_positions` 6 total / 2 MIS / 4 CNC; (c) "increase budget to
  40000" → `capital_base_inr` + `max_deployed_capital_inr` 40,000. (b)+(c) are protected-store edits:
  applied tonight with the engine STOPPED (the gate re-verifies the file hash on reload — a live edit
  would trip IntegrityError), then `seed_protected_config.py --reseed`, then restart. Plan §7.1 row +
  O16 paragraph written.
- **Owner: "update the engine right now and restart it."** Re-observed 12:55:02 (window closed 12:50,
  last analyst call 12:50:05, nothing in flight) → `nssm stop` 12:55:12 → limits.yaml edited (base
  40,000, deployed cap 40,000, caps 6/2/4, derived ₹ comments) + settings `universe_max_watchlist`
  200 → 300 → both validated through the engine's loaders (`LimitTable` typed parse ok) →
  `seed_protected_config.py --yes --reseed` (limits.yaml sig 8a9f762e…) → `start` 12:56:50 →
  selftest 12:57:23 all green incl. `protected_store:limits.yaml` and `equity_halt_ladder (equity
  40000.00)` → `catch_up_complete` 12:58:31, `startup_complete` 12:58:35, ticker 12:58:37, feed
  HEALTHY 12:59:33 (one warm-up store stall), risk NORMAL (no freeze on this boot). Commits 5e4416a
  (gate rule) + d2a9077 (limits/settings). Note: `risk_counters_rebuild` shows day_baseline 19,339.45
  vs equity 40,000 → today's `day_mtm` reads +₹20,660 (the base jump, not P&L); harmless to the
  negative-only halt rungs, resets with tomorrow's baseline — watch the nightly review's P&L line.
  The owner window (10:30–12:50) is closed, so no entry can originate today unless the owner reopens
  it (`/trade_window`, CNC entries allowed until 15:00). Evening cron now carries only the
  holdings-reconcile build + restart.
- **Afternoon outcome:** owner reopened the window 15:16–15:30 (set 15:15:44) — past the 15:00 CNC
  entry cutoff, so no entry could originate; 8 exit recs (HDFCAMC/HINDZINC) approved and expired; no
  errors since the restart. Tomorrow is the first real test of O16.
- **Holdings reconcile built (Opus, red-first; plan §3.6 paragraph, brief
  `runbooks/briefs/holdings_reconcile_brief_2026-09-07.md`):** `src/engine/ops/holdings_reconcile.py`
  (`run()` :142; held = quantity + t1_quantity summed per symbol :262; T+1 skip = sessions strictly
  between open and today :195; once-per-position-per-day alert :243; entry rec id via the ledger, exit
  id fallback :221; broker failure → `holdings_reconcile_failed` warning, never raises), catalog kind
  `POSITION_NOT_IN_HOLDINGS` + renderer (catalog.py:122/:405, never-expire outbox set in telegram.py:192),
  hourly `holdings_reconcile` job window-gated 09:20–15:30 on trading days (main.py:2117, tick :1385),
  post-login ladder step `_step_holdings` non-load-bearing (post_login.py:366). Manager audit clean;
  added `AND COALESCE(is_paper,0)=0` to the scope (agent's own recommendation — paper positions can
  never be in real holdings). 16 new tests; holdings + post-login modules 28 passed; ruff clean.
- **Miss caught by the agent's full-suite run:** 12 existing tests pinned the old ₹20,000 base and
  3/2/2 caps (test_limits_engine, test_lifecycle_selftest, test_api_routes, test_telegram_commands,
  7× test_risk_gate) — I committed the limits (d2a9077) without re-running the suite after the edit.
  Fixture repair delegated (Sonnet), preferring values read from the loaded LimitTable; full suite
  re-run before the evening restart (cron 18:52).
- **Evening deploy (holdings reconcile + fixtures, commit 06b6860, full suite 2,183 re-run by me):**
  the host slept again ~18:00 → 20:42 (zero log lines for two hours; 12 "missed by" warnings at
  20:42:59; ticker respawn 20:47), so the 18:05 EOD jobs never ran. In-flight probe clean (0 busy
  events) → `nssm stop` 20:49:59 → `start` → `holdings_reconcile` job scheduled 20:50:28, selftest
  ok 20:50:34 (protected_store verified), `catch_up_complete` 20:53:08 replaying **10 missed jobs**
  (earnings_calendar, corp_actions, filings_shp, bhavcopy, daily_bars, deals, filings_pit,
  filings_pit_fresh, ins_crossings, filings_results — all 2026-09-07), `catchup_safety_jobs` cleared,
  `startup_complete` 20:53:13, ticker 20:53:16, feed HEALTHY 20:53:17, post-arm jobs complete.
  Tomorrow readiness: settings validated with `universe_max_watchlist=300` (applies at the 08:30 build;
  expect a longer first warm-up while ~100 newly watched names backfill); holdings reconcile runs at the
  first post-login recovery. **Ops note for the owner:** the PC slept Sunday 21:00→Monday 10:12 and
  again Monday 18:00→20:42; each sleep skips the scheduled jobs (replayed at the next boot/sweep) and,
  on a trading morning, costs the open — a power-plan/wake-timer fix, not an engine one.

## 2026-09-06 (01:2x–02:xx, owner question "what is pending for the next phase?") — G2 re-measured; a false "lost session" finding made and RETRACTED

- **G2 collector (read-only, 2026-07-29..09-06, 28 sessions):** digest-before-open 19/28 = 67.9%
  (NOT MET; 09-02/03/04 all "AFTER" because the engine was booted at 09:3x and the post-boot digest
  landed 1–7 min after the 09:50 window open — an operating-pattern miss, not code); News Analyst
  schema-valid 99.9% (MET); recs delivered in time 41/41 (MET); owner-executed taken=2 closed=0
  (NOT MET); sessions with ≥1 rec 8/28 (NOT MET — 08-26→09-04, eight consecutive trading days);
  zero API orders (MET); budget $77.90/$550 MTD (MET); watchlist-precision and payload samples
  still have no record (owner-manual). HDFCAMC and HINDZINC still OPEN (since 08-26/08-27), never
  `/closed`.
- **RETRACTED (my error, told to the owner and corrected here within the hour):** the first version
  of this entry called 2026-09-05 a lost Friday session. **09-05 was a SATURDAY.** Every 09-04
  (Friday) EOD job succeeded on schedule (`job_runs`: bhavcopy 18:00, daily_bars 18:06, filings
  18:35–19:00, deals 20:30, features 20:45, backup 21:00, nightly_review 23:04), and 09-05 has no
  rows because none were due. The laptop's Modern Standby 04:36→20:53 on the 5th and the nine
  "run time was missed" APScheduler warnings at 09:47 concerned weekend polls only. The correct
  residual observation: a machine that is in standby at session open would never get the pulse
  that engages the in-session keep-awake, and the watchdog task cannot fire on a sleeping PC — a
  weekday shape to guard against, not something that happened. The weekday check
  (`(Get-Date).DayOfWeek`) goes before any "lost session" claim from now on.
- **Data is current through Friday 09-04's close.** The only Monday-specific load is the first
  NIFTY 500 `universe_daily` build (`universe_build` last ran 09-04 09:42 under NIFTY 200; the
  08:30 job is calendar-guarded, so it skipped Saturday/Sunday and the 21:00 Saturday boot did not
  catch it up). `data/universe/index_cached.csv` does not exist yet; the committed seed has 499 EQ
  symbols. `sector_map` last ran 08-30 (its own cadence).
- **Owner-directed "load the new data now" (01:4x):** `scripts/build_universe.py --date 2026-09-07`
  (new, wired as `job_universe`: margins refresh → surveillance → instruments hydrated from the stored
  snapshot → `UniverseBuilder.build`), run with the engine stopped 01:41:44 (idle, health pulses
  only) → **first NIFTY 500 build in 42 s: index source=download, eligible 480, watchlist 200,
  mis_candidates 164, extended 423, degraded=False**; `universe_daily` rows for 2026-09-07 written
  (replace-write) and `data/universe/index_cached.csv` created. Engine restarted 01:42:30 →
  `startup_complete` 01:43:00 NORMAL, `post_arm_jobs_complete` failed=[]. What this buys Monday:
  the boot reads the NIFTY 500 watchlist from the 09-07 row from the first minute (ticker
  subscriptions + warm-up set), and the first run of the new code path happened on a quiet Sunday;
  the scheduled 08:30 build still fires (idempotent, ~1 min). What it does NOT buy: a late Monday
  boot still pays the 09:15→boot 1m warm-up backfill (~15 min for 200 names) — that is the real
  late-start cost and it cannot be pre-loaded. Data was already current through Friday's close.
- **Pending for Phase 3 is unchanged in kind:** `src/engine/oms`, `src/engine/paper`, `tests/chaos`,
  `tests/replay`, `tests/property` all still 0 files; 21 commits unpushed on phase2; stash
  `context-daily-tail` (superseded by 334d992) and three untracked root files still present.

## 2026-09-04 (16:3x–17:xx, owner-directed "extend the eligible universe to nifty 500") — O15 implemented; gate membership gap found and closed

- **Decision recorded first** (plan §6.1 O15, commit 7bc900d): scope over the same-day evidence, with
  the evidence written down; watchlist cap stays 200 (load), swing/batch legs read the full eligible
  set; of the owner's cited names only IDEA and NIACL are in NIFTY 500 (PCJEWELLER, JINDWORLD, PAISALO
  are outside it). Brief: `runbooks/briefs/nifty500_universe_brief_2026-09-04.md`.
- **Implementation (Opus, red-first 11 → green, full suite 2,162):** `universe.index_name/index_source_url/
  index_seed_path` (config.py:433–441, `extra="forbid"` on `UniverseCfg` only so a retired key fails at
  boot); seed `config/universe/nifty500_seed.csv` from one live fetch (500 rows → 499 EQ symbols;
  HFCL is series BE and correctly dropped); builder generic (`_load_index` builder.py:346,
  `EXCL_INDEX="not_in_index"` :79, `universe_built` carries `index_name`/`index_size`, cache
  `data/universe/index_cached.csv`, isin_map.py follows); store reads both index markers and the
  replace deletes both (store.py:633/640/1428–1431/1471); sector map runs over the batch universe via
  a bounded look-back helper (main.py:813, call :1025 — Sunday has no universe row, so a literal
  same-day read would have been a permanent no-op); type alias `IndexSource`; fixture renamed.
- **Audit finding, closed inline (red-first):** `RiskGate._universe_row` read `included_only=True`
  (gate.py:1379) — exact only while the cap did not bind; under O15 every candidate from the ~200
  eligible-but-capped names would have been rejected as out-of-universe, making the widening a no-op
  for swing recommendations. New `_eligible_universe_row` (gate.py, before `GateContextBuilder`)
  accepts `included` rows and `watchlist_cap`-only rows, rejects `not_in_index`/`not_nifty200` and any
  real exclusion; test `test_gate_universe_membership_is_the_eligible_set_not_the_focus_watchlist`;
  gate + pipeline + digest modules 315 passed. Plan O15 paragraph and builder docstring updated.
- **Residue (agent-flagged, applied on my instruction):** `scripts/backfill.py`, `scripts/backfill_filings.py`
  (would have raised on the retired attribute), `scripts/backtest_hi52.py` cache path (+ `--index-csv`
  alias), RUNBOOK.md and sector_overrides.yaml prose. Learning-module provenance strings that record a
  NIFTY200-measured spread are deliberately untouched.
- **Deploy:** committed 5cdd2eb (21 files); deploy-gate full suite re-run by me on that tree: 2,163
  passed (6:48). Evening jobs done (deals ingested 20:30:00), nothing in flight → `nssm stop` 20:31:48
  → `start` → `catch_up_complete` 20:33:50, `catchup_safety_jobs` cleared, `startup_complete` 20:33:57,
  ticker handshake 20:34:03, feed HEALTHY 20:34:33. `universe_build` armed for Monday 08:30 — the first
  NIFTY 500 build happens then (no rebuild at boot: today's watermark is done). The boot again
  reported crash-recovery after a clean nssm stop (lifecycle row still RUNNING at exit) — unchanged,
  harmless. Telegram polling/sends still failing at the network level.
- **Shadow populations:** `cat`/`cat_reversal`/`hi52` verdicts must be computed before/after the first
  NIFTY 500 build (Monday 08:30). Watch items Monday: `universe_built index_name="NIFTY 500"
  index_size≈499 eligible≈350–450 watchlist=200`, `watchlist_cap` rows non-empty, brk20/ins/hi52
  sweeps over the wider set, gate `in_universe=True` on a capped name when one is proposed.

## 2026-09-04 (15:0x–15:4x, owner: "can't we track the candles?" + "improve the coverage") — candle battery RUN and REFUTED; coverage audit; NSE announcements feed implemented

- **Candle / price-action battery (Opus, brief `runbooks/briefs/candles_backtest_brief_2026-09-04.md`,
  plan §6.1 pre-registration):** `scripts/backtest_candles.py` + `tests/unit/test_backtest_candles.py`
  (41 passed, re-run by me) + `data/reports/backtest_candles_2026-09-04.json`. Manager audit: 5m buckets
  09:15-anchored and filtered before the VWAP window (backtest_candles.py:435–503), E1 stop/target on
  bars strictly after entry with stop-wins-ties and a refused stop ≥ entry (:895–935), E2 trail fills
  at the next 1m open (:938–973), cost byte-identical to tdc (:976); two trades hand-verified against
  raw bars (BANKBARODA 2023-07-17 R1|E1 target; ALKEM 2023-07-17 R3|E2 trail). **Verdict: REFUTED in
  all 10 cells and every split** (755 sessions; n ≈ 3,600–3,760/cell; mean net −0.14…−0.18%; t −8.9…
  −20.6; CPCV 0/6 everywhere). Decisive: mean GROSS is −0.03…+0.01% per cell — no edge exists for
  cost to erode; candle rules are noise at 5m resolution here. Plan §6.1 verdict appended.
- **Coverage audit (Sonnet, verifiable pointers):** eight live domains (the 08-04 "ET-only" picture is
  superseded), 13,609 headlines/30d, 14.2% resolve a symbol, median ingest lag ~85 min; 47% of top-10
  movers had any cluster naming them; of ten 09-03/09-04 movers, five (GMRAIRPORT, CGPOWER, APLAPOLLO,
  PFC, TATASTEEL) had no press headline AND no NSE filing — catalyst-silent. Probes: NSE corporate
  announcements live via `nse_get` with native `symbol`/`sm_isin` (carried UNITDSPR's Reg-30 09-03 16:30
  and HINDZINC's LoI ~2 h before the press); BSE announcements live; Business Standard RSS alive again;
  Moneycontrol RSS still frozen (2024-04); GDELT 429s any concurrent same-IP request. Plan §2.7 addendum
  (commit 6069dcc) + brief `runbooks/briefs/nse_announcements_feed_brief_2026-09-04.md`; implementation
  delegated to Opus (main tree; the worktree isolation refused a stale locked worktree, left in place
  while its holder pid lives). KPI: share of top-10 movers with a catalyst in corpus (47% baseline).
- **NSE announcements feed + Business Standard RSS implemented (Opus, red-first, brief
  `runbooks/briefs/nse_announcements_feed_brief_2026-09-04.md`):** `NewsIngest._fetch_nse_ann` (news.py:393)
  via `nse_get` on the §2.8 ISIN job's endpoint; rows → `[NSE:<SYMBOL>] <desc>: <attchmntText>` with
  `source_domain=nseindia.com`, exchange timestamps (`sort_date` → `exchdisstime` → `an_dt` → Clock),
  attachment URL or a deterministic `seq_id` URL as the dedupe key; owner-editable `drop_subjects` on
  `desc` only; `EXCHANGE_TOKEN_RE` + token resolution before alias matching under the same universe
  check (news_pipeline.py:139/:706–729); token-bearing member promotes itself to cluster
  representative (:409); scheduler job `news_poll_nse_ann` gated on `enabled` (main.py:2037–2040);
  `NseAnnouncementsCfg` (config.py:161); settings block (settings.yaml:152–162) + `bs_markets`/
  `bs_companies` (verified live 09-04); fixture `tests/unit/fixtures/news/nse_announcements.json` from
  one live probe. Manager audit of every cited seam: clean. Tests: red 10 → green; touched modules 189
  passed (re-run by me); full suite 2,157 (agent) and re-run as the deploy gate below. Known limits:
  the endpoint returns the newest 20 rows per call (a 15:30 filing rush can exceed 20 in 5 min — a
  paged/from-to variant is the lever); `news.drop_title_patterns` also applies to filing titles
  (0/20 collisions live). KPI to watch: share of top-10 movers with a catalyst in corpus (47% baseline).
- **Deploy:** full suite re-run as the gate (2,157 passed, 4:27) → commit fefa761 → `nssm start`
  16:03:04 on a clean tree (service Stopped, 0 market_trading processes). Boot: `catch_up_complete`
  16:06:14, `catchup_safety_jobs` cleared 16:06:15, `startup_complete` 16:06:23, ticker handshake
  16:06:24, pulses HEALTHY; risk FROZEN on `warmup_ready` post-close as on the 09-03 15:40 boot (rolls
  at midnight; no session until Monday). New jobs armed: `news_poll_nse_ann`, `news_poll_bs_markets`,
  `news_poll_bs_companies`. **First exchange poll 16:08:36–16:08:55: 20 rows → 17 kept, 3 dropped
  (administrative subjects), 17 inserted** (`news_nse_ann_filtered`, `news_polled feeds=nse_ann`).
  Telegram still ConnectTimeout at boot (network-level, unchanged). Monday watch items: the 08:35
  digest carrying `nseindia.com` as a corroborating domain, `[NSE:…]` clusters resolving with
  `symbols` set, and the mover-catalyst KPI.

## 2026-09-04 (14:03–14:4x, owner: "I have stopped the engine, continue; validate without bias") — `tdc` backtest RUN and REFUTED; movers attribution; engine stopped 14:04 by me, restart scheduled 15:36

- **Engine state on arrival:** service still Running at 14:03 with `store_stalled`, `late_tick_past_grace`
  and `warmup_refresh` skips — the 30-min catch-up sweep had started a tick-compaction recovery
  in-session at 11:39 (`tick_compaction_recovered 2026-09-02/NHPC`; the in-session gate covers only the
  post-boot path). No stop/command event since 13:30: the owner's stop never reached the engine (Telegram
  ConnectTimeout all day). Owner intent explicit → `nssm stop` 14:04:22, 0 processes at 14:04:34.
  Follow-up filed: gate the sweep-path `tick_compact` in-session too.
- **Backtest (Opus delegation, brief `runbooks/briefs/tdc_backtest_brief_2026-09-04.md`):**
  `scripts/backtest_tdc.py` + `tests/unit/test_backtest_tdc.py` (30 passed, re-run by me) +
  `data/reports/backtest_tdc_2026-09-04.json`. Manager audit: bars ≤ T filtered before the window
  functions (backtest_tdc.py:364), acceptance vs running VWAP (:382), entry = next bar OPEN (:677),
  VWAP-loss exit at the FOLLOWING open (:684), squareoff at the last bar ≤ 15:15 (:685), cost =
  `CostModel.breakeven_pct` + 1 tick/side (:694); two hand-verified trades against raw bars.
  Clarifications I issued mid-run (recorded as protocol notes, not sweeps): equal-weight universe
  return as index proxy before 2026-07-23; catalyst_at_T split; full 2023-07 history with the plan
  window reported as its own cell.
- **Verdict:** REFUTED, every variant × split (plan §6.1 `tdc` paragraph carries the numbers). H1 n=3,370,
  mean net −0.129%, t=−4.63, CPCV 0/15; mean GROSS +0.035% vs cost 0.164%. Two thin positive cells on
  record (breadth ≥ 50% trend days, n≈110; catalyst_at_T true, n≈46) — future pre-registrations, not
  findings. No origination change (§8.6).
- **Attribution (scratch `attribution.py`, 23 sessions, top-10 movers/day, n=230):** 47% had a catalyst
  in our corpus, 16% a sector-wide move, 4% an index up-day, 46% none of the three. Hindsight shape:
  +1.45% by 11:00, +1.04% after, 83% still up after 11:00. **Ex-ante** (no hindsight, 20 sessions):
  every name up ≥ +1% at 11:00 (n=329) averaged −0.014% to 15:15 (win 47%); ≥ +2% (n=82) −0.24% (win
  40%). Today to 14:02: metals sector-wide (+1.8% median; HINDZINC govt_program catalyst 17:24 prior
  evening), names up ≥ 1% at 11:00 (n=25) −0.32% after (win 32%); the day's leaders (APLAPOLLO, PFC,
  TATASTEEL) moved AFTER 11:00 and were not selectable by price at 11:00. Leaders rotate intraday.
- Restart: moved to 17:50 (the candle battery + coverage probe run on the stopped engine until then;
  the 18:05 EOD jobs still run on schedule).
- **Sweep-path in-session compaction veto closed (red-first):** `_catchup_sweep_once` (main.py) applies
  `post_arm_exclusions(path="sweep")` to every 30-min catch-up pass, so a missed `tick_compact` is no
  longer replayed inside a live session (today's 11:39 recovery → afternoon store stalls / late ticks).
  Test `test_catchup_sweep_vetoes_tick_compact_in_session`; wiring + lifecycle modules 90 passed; plan
  WO-21 fix (b) note appended.
- **Owner follow-ups launched (engine stopped):** pre-registered candle/price-action battery (5 rules
  × 2 exits, brief `runbooks/briefs/candles_backtest_brief_2026-09-04.md`, plan §6.1 paragraph) and a
  news-coverage audit + source probe (Sonnet) — results in the next entry.

## 2026-09-04 (13:40–13:5x, owner question) — "would the flagged tickers be recommended under the updated logic?" → NO; `tdc` pre-registered, backtest tonight

- **Replay evidence (scratch `replay_alloc_0903.py`):** 09-03's in-window `orb` stream (3,953 arrivals from
  the log) through the deployed pre-screen (`decline()` + displacement, tranche-faithful via
  `_admit_batch_locked(at=…)`, pipeline modelled as one forward per 3 min with every verdict no_action):
  9 admissions, 4 displacements, **6 forwards** (INDUSINDBK, COROMANDEL, TATACONSUM, INDUSTOWER,
  IDFCFIRSTB, TATACOMM) vs 5 on the day. Of the six flagged names only IDFCFIRSTB reaches the analyst —
  as on the day. BSE/KEI/GMRAIRPORT/UNITDSPR/CGPOWER are never admitted: the `orb` score (single-bar
  volume-ratio squash) ranks spike names above steady climbers and 0.10 over ~0.9 incumbents is
  unreachable. Even if admitted, ORB's OR-anchored geometry is what the analyst declines (hindsight
  replay 09-03: the 11 ORB candidates −4.78% net at ORB geometry; the six held to 15:15 5/6 positive).
- **Diagnosis:** the flagged names are trend-day continuation setups; no leg in the book targets that
  shape, and `orb` cannot be tuned into one without a sweep the methodology forbids.
- **Solution, evidence-gated:** `tdc` pre-registered in IMPLEMENTATION_PLAN §6.1 (paragraph before
  WO-20): acceptance above OR high + 15 closes above VWAP, `rel_volume_tod` ≥ 1.5, RS ≥ +1.0% vs
  NIFTY at 11:00; entry next open; VWAP-loss stop or 15:15; variant B hold-to-close; robustness
  variants reported never selected; splits by index regime and breadth; promotion iff mean net > 0,
  t > 2, n ≥ 200, CPCV ≥ 60% positive. Brief: `runbooks/briefs/tdc_backtest_brief_2026-09-04.md`.
  Run needs the DuckDB writer stopped → scheduled 15:41 (engine idle, owner-permitted DB-task stop,
  Opus delegation, engine restarted after). A refutation means no origination change and the honest
  answer stays "not capturable at retail costs".
- Geometry-at-live-price check for the six could not complete: the 09-03 tick partition was being
  compacted mid-query (files moving); not needed for the verdict.

## 2026-09-04 (owner-directed "apply the fixes and validate them") — declined-incumbent displacement, measured digest for the planner, SDK result text; deploy held to after close

Red-first, inline (the subscription session limit was exhausted until 06:10, so no delegation).

- **(a) Declined incumbents are displaceable** — `SignalPreScreen.decline()` (prescreen.py), called
  from the pipeline's `no_action` branch (pipeline.py, wired in main.py). The pair stays SEEN and its
  journal row stays `evaluated=1` (not the 07-29 re-arm); eviction of a declined incumbent is charged
  to the day displacement budget only (`_displacement_scan_locked` / `_commit_displacement_locked`,
  log field `displaced_declined`, event `prescreen_slot_declined`). Hydrated pairs unchanged.
- **(b) Digest rail shared** — `sentiment_rail_note()` in context.py, now with the per-cluster mean
  ("raw -9.48 across 759 clusters, mean -0.012 per cluster"); `preopen_planner._digest_lines` uses it.
  `sentiment_agg.value` untouched (cat shadow thresholds).
- **(c) Harness** — `_error_result_text()` raises the CLI's own `result` text from an `is_error`
  result before the SDK's bare "error result: success"; `_classify` maps session/usage-limit and
  `rate_limit` text to `overloaded`. Process rule added to CLAUDE.md (no agent fan-outs 09:15–15:30).
- **Plan:** §6.1 addendum before WO-20; settings.yaml comment; tests: test_prescreen (+3),
  test_reco_pipeline (no_action → decline hook + journal stays evaluated=1), test_context_assembler
  (+1, exact string updated), test_preopen_planner (+1), test_agent_harness (+1 test, +2 params).
- **Validation:** red confirmed (`AttributeError: no attribute 'decline'`); touched modules 327 passed;
  full unit suite **2060 passed** (5:27).
- **Deploy:** first held (10:50 IST, session open, owner window 09:50–12:50; morning boot 09:39 after a
  02:26–09:39 stop, warm-up FROZEN until 10:37:32), then **owner-directed "deploy it now"**: re-observed
  11:06:42 (last analyst call ok 11:04:08, nothing in flight), `nssm stop` 11:07:33 → `start` 11:07:40 →
  `startup_complete` 11:09:30 on 8c398c7 (`catchup_safety_jobs` cleared 11:09:25, feed HEALTHY, prescreen
  hydrated). Warm-up FROZEN 11:09:27 on one missing COROMANDEL 1m bar (store_stalled ×2 during the
  restart backfill, 11:10–11:11); the §2.6 self-repair fired at 11:15:18 (attempt 1, 1 bar written) and
  risk went NORMAL 11:16:13 — entries blocked 8m40s. Boot reported "crash-recovered" despite the clean
  nssm stop (lifecycle row still RUNNING at exit) — harmless, noted. First `prescreen_slot_declined`
  will appear on the next no_action verdict.
- **Observed today, not caused by this change:** Telegram sends failing since the 09:39 boot
  (`httpx.ConnectTimeout`, 14 sends + set_commands; TCP 443 to api.telegram.org connects but an
  HTTPS HEAD times out from PowerShell too — machine/network-level, owner not receiving alerts);
  `store_stalled` ×2 during the in-session warm-up backfill; hi52 first sweep on adjusted history:
  800 scanned, 13 candidates, 3 admitted (JINDWORLD, EDELWEISS, MANALIPETC), `unadjusted_vetoes` 62.
  Today's orb tranche-1 verdicts so far: ASHOKLEY/GODFRYPHLP/RELIANCE all no_action (chase/stalled).

## 2026-09-04 (02:10–02:5x, owner question) — 09-03 movers forensics: CGPOWER/UNITDSPR/BSE/GMRAIRPORT/KEI/IDFCFIRSTB (+BRIGADE/JYOTICNC/INOXWIND); no code change

Read-only investigation (engine running, DuckDB locked; evidence = `engine.log.2026-09-03`, `state.db`,
raw tick Parquet `data/parquet/ticks/date=2026-09-03`, Claude Code transcripts). Scratch scripts:
`forensics_0903b.py`, `verdicts_0903.py`, `intraday_0903b.py`, `replay_0903.py` (session scratchpad).

- **Membership.** Six are NIFTY200; BRIGADE/JYOTICNC/INOXWIND are MIS-eligible and off every
  surveillance list, so they belong to the extended 600 by criteria — advisory/hi52 only, no ticker
  subscription (0 tick files), not RECOMMEND-able by the 09-01 design. hi52 sweep 09:50: 800 scanned,
  0 candidates (pre-backfill history; fresh-cross required).
- **Funnel, first clean session on tranches 3/5/7 + displacement:** ORB fires 4,090 across 159 names
  → 10 slots (3 at 09:50–09:52, 2 at 11:30) → 6 forwards (INDUSINDBK ×2 after the max-turns re-arm,
  COROMANDEL, IREDA, ENRIN, IDFCFIRSTB) → 5 no_action → 0 entries; 3,984 `prescreen_cap_suppressed`;
  5 `prescreen_slot_displaced` (INDUSINDBK>BLUESTARCO, IREDA>CGPOWER, COROMANDEL>COLPAL,
  ENRIN>ASHOKLEY, IDFCFIRSTB>BEL) — the mechanism works as specified. All six in-universe names fired
  ORB inside the 09:50–12:50 window (best scores UNITDSPR 0.967, IDFCFIRSTB 0.917, GMRAIRPORT 0.895,
  KEI 0.825, CGPOWER 0.812, BSE 0.811); only IDFCFIRSTB reached the analyst (declined 11:32).
- **Cause 1 — declined evaluations lock the cap.** `rearm` is infra-failure-only (prescreen.py:522,
  owner-directed 07-29) and displacement targets unevaluated incumbents only (prescreen.py:934), so
  `displaceable=0` from 09:52 (3 slots) and 11:30:07 (5 slots) for the rest of the window; 6 of the
  48 daily forwards used. Tranche boundaries admit whoever fires in that minute (ASHOKLEY 0.42, BEL
  0.50 at 11:30:05). The owner's 09-02 ask ("push new candidates without the hard cap") is NOT met yet.
- **Cause 2 — sentiment SUM rails; planner renders it bare.** `sentiment_agg` market = clip(Σ…)
  (news_pipeline.py:1081): 09-03 value −1.000 from raw −9.48 across 759 clusters (≈ −0.012/cluster,
  i.e. neutral); 09-02 also −1.000. `ContextAssembler._sentiment_line` carries the WO-22 wording, but
  `preopen_planner._digest_lines` (preopen_planner.py:282) emits "market: -1.000" with no measure →
  day plan "risk-off, failed breakouts fade hard" → heartbeat regime notes repeat "pinned at its floor"
  → every ORB verdict cites a hostile tape. Tape (raw ticks): NIFTY +0.34% open, high 09:48, close
  −0.12%; breadth 104 up / 97 down, 45 up ≥1%, 44 closed in the top quartile and up — the planner's
  "chop, narrow stock-specific bid" was defensible, its stated reason was an artifact. The nightly
  reviewer flagged the same suspicion independently.
- **Cause 3 — ORB geometry on momentum names.** Tick-level hindsight replay (entry at decision price,
  first stop/target touch else 15:15 squareoff, 0.126% cost): the five declined trades net −3.5%
  (IDFCFIRSTB +0.86% the only winner); all 11 candidates −4.78%. The owner's six bought at first fire
  and held to squareoff: 5/6 positive (+0.73…+2.03% net), CGPOWER −0.49% (run over by 09:50).
  Momentum-day capture needs a hold/trail structure the book does not have and has never tested; the
  OR-anchored target/stop makes any post-trigger fill sub-1R (analyst's stated reason on 3 of 5).
  Analyst latency at the open 4–8 min (serialized ~60 s calls; IREDA "failed in place" by 09:59).
- **LLM outage 12:42–14:32 IST (31 calls, then again 19:07–19:28):** the SDK reports a limit-hit
  result (subtype `success`, `is_error`, empty `errors[]`) as "Claude Code returned an error result:
  success" and drops the text (claude_agent_sdk/_internal/query.py:306). Claude Code transcripts in
  the project dir show "hit your session limit · resets 2:30pm" from 12:42 IST and "resets 7:30pm" at
  19:07; this session's research agents were refused at 02:21 with "resets 6:10am". The engine and the
  dev sessions share one subscription window. Impact 09-03: two hourly position reviews and the news
  analyst dark for ~1h50m; no signal candidate lost (window closed 12:50, cap already full).
- **Morning (not a cause):** Kite daily token expiry dropped the websocket 07:17:55 (1006, then 403);
  supervisor killed the child 09:34:23 (8,174 s silence), TokenException → FROZEN + login prompt,
  stop_forced 09:34:59, crash-recovery boot 09:35:47, owner login 09:36:02, window set 09:36:43,
  NORMAL 09:45:27. Pre-open plan ran 09:53 (after window open); first analyst call saw "no day plan".
- **Recommended (not implemented — owner's call):** (1) make no_action incumbents displaceable
  (margin 0.10, per-symbol cooldown, bounded by the 48/day forward cap); (2) replace the clipped SUM
  with a bounded mean/z-score and give the planner the WO-22 measure; (3) pre-register a
  trend-continuation intraday backtest (acceptance + pullback entry, trailing exit, breadth gate) on
  bars_1m before any origination change; (4) log the SDK result text on failure and keep heavy agent
  work off 09:15–15:30 IST.

## 2026-09-03 (15:38–15:41) — deploy-by-start: d2c5c8c (store-stall paging) + 15bff68 (hi52 ex-date veto) + c0b39eb live; boot verified

- **Coordinated with the backfill session** (cross-session): held my planned 15:42 restart when the
  owner stopped the engine at 15:06 for the DuckDB-writer backfill; started only on its RELEASE
  message after probing the store free myself (read-only connect OK, bars_1d 2,086,808 rows —
  consistent with the 1.80M-row archive write on top of the Kite-official rows).
- **Boot 15:38→15:40:** scheduler_started 15:40:04, post_arm fired news_chain/catalyst_digest/
  preopen_planner with 0 failed (tick_compact correctly deferred in-window to 22:30), feed HEALTHY,
  `store_stalled` absent from out-of-session pulses as designed. The 15:06–15:30 bar tail recovers
  via the §2.6 warmup backfill; tonight's EOD jobs run on their normal schedule.
- **WATCH ITEM — boot-into-buffer false lag episode:** at 15:40:02 the tick-lag watchdog fired an
  ERROR burst (`tick_processing_lagging`, lag ~494–526s) on Kite's connect-time snapshot ticks
  stamped with the ~15:31 close prints, while the watch window was still armed (session_close+15m
  = 15:45). It self-closed within ~1s (`tick_processing_recovered`, lag 26s) — under the paging
  debounce, so no owner page — but any post-close boot inside the 15:30–15:45 buffer will repeat
  this noise. Candidate fix for a future WO: suppress the first lag evaluation until N live ticks
  after a (re)connect, or exclude connect-snapshot ticks from the lag clock. Log-noise only today.
- First armed evaluation of `store_stalled` and hi52's `unadjusted_vetoes` (expected >0, backtest
  vetoed ~3%) is tomorrow's window_open sweep.

- **Backfill (`scripts/backfill_bhavcopy.py --from 2022-07-01 --to 2026-08-14 --pace-s 0.6`, 15:08→15:32):**
  1,506 calendar dates attempted, 1,022 sessions ingested, 484 holidays, **0 failed, no streak
  abort**; 1,801,620 bars_1d rows written, 243,085 cross-checked against existing rows, 47,505
  mismatched — the adjusted-vs-raw unit difference on names with corp actions, i.e. the B1 premise
  correction measured. bars_1d now: 2022 229k rows/1,981 symbols (H2), 2023 450k/2,187, 2024
  470k/2,310, 2025 533k/2,555, 2026 406k/2,905. Corp-actions leg 2021-05-27→2026-08-14: 55 windows,
  12,829 rows (min window 38 rows in the year-end lull, median 149 — no capped windows); the table
  now holds **819 structural rows** (split 274, bonus 273, rights 209, demerger 63) 2021-06→2026-09,
  ~150–200/yr, so the live veto's 400-day lookback is fully backed. Report:
  `data/reports/bhavcopy_archive_backfill.json`.
- **Backtest (`scripts/backtest_hi52.py` with the veto, `e148bb2`; 3,199 symbols, 1,035 sessions,
  window 2020→2026-09-03 ⇒ effective 2022-07; 16,042 discrete trades vs 2,545 on 09-01; vetoes:
  485 signals discrete / 6,063 symbol-days rank).** Report: `data/reports/backtest_hi52_2026-09-03.json`.
  - **Geometry (discrete fresh cross): DEAD at T+5 (median gross −0.45%) and T+10 (−0.18%), viable
    at T+20 by +0.09% only** (+0.41% vs the 0.32% CNC floor). 09-01 (index class only): viable at all
    three, T+20 +2.26%.
  - **CPCV: T+10 and T+20 "promotable" by the house rule (73.3% fold pass, median passing
    0.144%/day vs 0.016 floor), T+5 fails (40%).** The pooled pass is carried by one cell:
  - **Population split at T+20 (median net / hit / n):** index_member_proxy **+1.89% / 58.4% / 2,120**
    (09-01: +1.94% / 59.0% / 2,545 — reproduced); **extended_non_index_proxy −0.26% / 49.1% / 13,922**,
    and negative at T+5/T+10. The WELCORP/DYCL-class motivation for scanning the extended universe
    has no edge after costs on 4 years of full-market history. The fresh-cross effect here is an
    index-class effect.
  - **Frog-in-the-Pan now points the way the literature says:** smooth_approach T+20 +1.14% / 55.6%
    vs jumpy −0.57% / 48.0% — the 09-01 in-sample INVERSION on the index class does not survive the
    full market (descriptive cut: full-sample medians, not knowable at signal time). gap_days_excluded
    +0.78% / 53.4% vs gap_days_only −1.19%.
  - Rank construct: top decile viable only at T+20 (+0.37% gross), CPCV 33–50% ⇒ not promotable;
    long–short spread +0.75% at T+20 (bottom decile −1.34%) — the academic sign appears on the full
    cross-section, but the short leg is not executable (CNC).
  - **§8.6 verdict: no promotion, and the shadow's ORIGINATION SCOPE is now the question for the
    owner** — as an extended-universe originator hi52 measures no edge; the evidence supports an
    index-class rule with a smooth-approach / no-gap-day filter (a pre-registered v2 would need a
    signal-time definition of "smooth"). Options: restrict origination to the eligible universe;
    pre-register v2; or drop. Not shipped — owner decision.
  - Caveats carried: index split is a survivorship-tainted proxy (current membership); bars
    survivorship is now PARTIAL (the archive carries since-delisted names' history, but a name
    delisted inside the horizon books no trade — the report's "delisted names absent" line is stale
    and should say so); N=1, no sweep; CNC ₹20k costs incl. spread.
- **Process:** the backtest-veto change was built red-first and Opus-reviewed (two should-fixes
  applied: structural vs all-kinds coverage; the rank window ends ON the rebalance day so its veto
  calls with d+1); 19 tests. Peers held the restart 15:08→15:4x and were released after the run.

## 2026-09-03 (12:0x–15:1x, owner-directed "start with your recommended fixes") — store-stall page, hi52 unadjusted-history veto, bhavcopy archive backfill tool

- **Item 3 shipped (`d2c5c8c`, deploys at next boot):** the WO-24b store stall is an owner-facing
  `store_stalled` health problem when the ping is unanswered on 2 consecutive pulses, or while a
  probe abandoned by the WO-26a re-probe still hangs — riding the WO-25b episode cadence, in
  session only. Two Opus reviews caught three lifecycle defects in my first cut, all fixed: the
  count only reset on a SUCCESSFUL ping (a store that started raising after a stall would page
  "unanswered for Ns" forever — the catchup_safety_jobs latch class); the re-probe anomaly reset
  the count and flapped the episode every 6 pulses with a false all-clear while the pool lost
  threads; and the 22:30 compaction holds the store lock for minutes on a healthy engine (nightly
  false page) — hence in-session only, like feed_stale. 4 new tests; fixtures moved to the
  watchdog test module (import cycle). Plan §3.2.12 line added.
- **Item 7 shipped (`15bff68`) — and the 09-02 premise corrected:** the deferral assumed Kite-official
  history is corp-action adjusted. It is adjusted at FETCH time (A11), but the stored series is
  seeded once and extended one session a day (`job_daily_bars` fetches `[d, d]`; `BackfillJob`
  never re-fetches past a checkpoint), so an ex-date after the seed leaves the watchlist names
  straddling two units too — and those 195 deep-history names are the ONLY ones that can pass
  `min_sessions` before ~mid-Jan 2027. My first cut vetoed only extended names (the population that
  cannot fire); the review caught it. Now: every symbol with a bonus/split/rights/demerger ex-date
  inside the 400-day frame window sits out (`hi52.unadjusted_history` over `corp_actions`, counted
  `unadjusted_history`), and `classify_purpose` learned the rescaling vocabulary the allow-list
  let through as `other` (consolidation, "sub division", demerger, capital reduction). AGM/EGM
  stay `other` on purpose. **Precondition:** `corp_actions` holds windowed rows only since
  2026-08-14 (call-date-only before) — the veto under-counts until the archive tool below runs
  `--skip-bhavcopy` over the lookback. Residuals: the count is taken before the `min_sessions`
  gate; `backtest_hi52.py` does not apply the veto yet.
- **Item 6 built, NOT run (committed with this entry):** `scripts/backfill_bhavcopy.py` — every
  calendar date 2022-07-01..2026-07-12, LEGACY archive URL before 2024-07-08 / UDiFF from it
  (both probed today; 2022-2023 are legacy-only), fallback on 404, 404-on-both = holiday, persisted
  through `BhavcopyJob._persist` (Kite rows never overwritten); corp-actions leg over ≤35-day
  windows from 400 days before `--from` (NSE serves 2022/2023 history — probed). Opus review
  applied: weekday filter removed (NSE's Saturday DR drills / Muhurat Sunday / Budget Saturday would
  have been dropped silently), 7-day holiday-streak guard un-checkpoints an archive outage, zero-row
  windows fail, failed dates + per-window rows in the report, pre-flight on a missing store file.
  12 tests. **Run order in COMMANDS.md** (engine off): corp-actions lookback first, then the full
  history, then `backtest_hi52.py` once it applies the veto.
- **Process notes:** the account's session limit killed four Opus agents at 12:4x (reset 14:30);
  the store-stall re-verification was done by hand, the rest re-launched after the reset. Full
  suite 2035 (item 3) → 2044 (item 7) passed with the engine live; the script's tests run in
  isolation (a new file the engine never imports). Both peer sessions were asked to hold the
  post-window restart until the src commits landed (12:41) and released at 15:0x.
- **Open from the 09-02 list:** the deferred-refactor list (owner call). New follow-ups: run the
  archive tool (engine-off evening), then make `backtest_hi52.py` apply `unadjusted_history`; the
  `symbols_scanned` field counts vetoed symbols (candidates + vetoes reconcile, as brk20's does).

## 2026-09-03 (00:4x–11:5x, owner-directed "start with the fixes") — four follow-ups from the 09-02 phase audit, red-first + two-lens review

- **Shipped (deploys at next boot; dashboard dist already rebuilt 11:3x and served from disk):**
  (1) `_request_owner_approval` takes a keyword-only `symbol`; the §5.2(b) caller passes
  `position["symbol"]`, persisted in `owner_approvals.payload` and printed in the title/body beside
  the raw position_id — the prompt no longer asks the owner to approve a bare ULID. (2)
  `GateContextTimeout` contained on both position paths: the position event alerts and returns; the
  §7.1 `max_holding` sweep alerts per position, finishes the aged positions BEHIND the stalled one,
  then **re-raises** so `reco_expire`'s watermark records a failure and the catch-up retries it.
  (3) `EntityResolver.load` fails CLOSED on an all-excluded `universe_daily` (rows exist, none
  eligible → empty frozenset + `universe_empty_fail_closed`); "no rows" keeps the pre-08:30
  "unknown" semantics. (4) `RecommendationsPanel`: the `/decisions` fallback and its `decisions`
  prop deleted — a card's own embedded gate verdict is its only provenance.
- **Method:** 3 implementers (disjoint files, failing test pasted before each change), 6 adversarial
  reviews (correctness + minimality per diff, per the owner's no-redundancy directive). Two
  should-fix findings ruled and applied: (a) the sweep guard as first written SWALLOWED the timeout,
  so `job_reco_expire` returned None → success watermark → a §7.1 exit deferred a full day while the
  alert promised a catch-up retry (both reviewers converged; fix = re-raise after the loop, test
  inverted to `pytest.raises`); (b) the dashboard fallback, even narrowed to subject + action↔kind,
  can BY CONSTRUCTION only ever return some other proposal's reasons (own reasons win whenever
  present; when the own list is empty there is nothing to find) — deleted rather than refined.
  Nits applied: `str(d)` in the warning, comment trim, mojibake in three new docstrings.
- **Accepted residuals (recorded, not built):** a total-failure universe build persists NO rows
  (builder.py:300) and still reads as "unknown" at the resolver — the digest's `in_universe` gate
  (news_pipeline.py:1203/1457) is the trading-side backstop and already fails closed on an empty
  set; the resolver's two reads are not one snapshot across the 08:30 build boundary (one poll's
  clusters could resolve out_of_universe; the next poll reloads).
- **Self-inflicted, caught, fixed:** a mojibake-repair script passed through a PowerShell ASCII
  here-string had its replacement keys degrade to `?` and rewrote two new `??` operators in the
  dashboard sources to `?`; caught by the on-disk-change notice, reverted, rebuilt (tsc clean).
  Lesson in memory: non-ASCII scripts go through the Write tool or `\u` escapes.
- **Validation:** `test_reco_pipeline.py` 64, `test_reco_pipeline_wo24.py` 12,
  `test_entity_resolver.py` 32, ruff clean, `npm run build` clean; **full suite 2029 passed**
  (6m34s with the engine live). **Deploy:** committed at 11:5x mid-session, engine NOT restarted
  (window open); rides the post-12:50 restart the morning session already planned for 36608b3.
- **Follow-ups still open from the 09-02 list:** store-stall owner alert (item 3), extended-name
  corp-action exposure before mid-Jan 2027 (item 7, must precede item 6), hi52 bhavcopy archive
  backfill (item 6), the deferred-refactor list (owner call).

## 2026-09-03 (morning, owner-reported log errors) — wire-schema thesis maxLength REVERTED; drain-skip benign

- **Owner flagged two log lines:** a forward_drain_tick "skipped: maximum number of running
  instances (1)" and an intraday_analyst `sdk_error: Reached maximum number of turns (4)` (09:51,
  INDUSINDBK, $0.33, candidate re-armed, "Intraday analyst unavailable" alert).
- **Diagnosis (three converging observations):** (i) FIRST-EVER intraday max-turns in the log
  history — every prior one (07-28 → 09-02) was the multi-turn news_analyst/nightly_reviewer — one
  session after the 09-02 deploy advertised `maxLength: 600` on `thesis` in the wire schema;
  (ii) the failed call emitted 4,430 output tokens in 61s vs ~750-1,950 normal (several attempts
  inside one call — the runtime re-prompting on its own schema check, each bounce a turn); the next
  ok call paid 4,339 too; (iii) since the deploy the client clamp fired 0× and string_too_long
  vanished despite a 10-in-9 overflow morning just before — overflow was being intercepted
  UPSTREAM, at the terminal layer (sdk_error has no D7 retry). The "prevention" half converted a
  free, always-converging client fix into a token tax plus occasional terminal failure.
- **Fix:** maxLength removed from the wire schema (deletion; `_contract_max_len` gone); the clamp
  is the sole mechanism; test inverted to pin "no prose field advertises maxLength". Schema tests
  30/30. The drain-tick skip is benign: `_drain_one_forward` awaits the analyst inline, a >60s call
  overlaps one 60s tick, APScheduler's single-instance guard skips it, pacing resumes next tick.
- **Deploy:** window was OPEN (owner set 09:50–12:50 at 09:36; 6 slots by 10:00) on the first clean
  session of the new allocation — restart deferred to just after 12:50, not mid-window.

## 2026-09-03 (00:1x–00:2x, owner-executed restart + push; owner-directed ledger repair)

- **Owner restarted the engine paired with `npm run build` (dist 00:12:11) and pushed phase2
  (origin in sync at 2184a2a, 0/0).** Boot verified from engine.log: `engine_boot` 00:13:27 →
  `catch_up_complete` load-bearing clean (off 2077 s) → `startup_report` mode=RECOMMEND
  risk=NORMAL **crash_recovered=false** (clean STOPPED commit this time) frozen=[] blockers=[] →
  `startup_complete` 00:13:52 → `engine_ready` 00:13:54 → `post_arm_jobs_complete` failed=[]
  (news_chain / catalyst_digest / preopen_planner / tick_compact). Health pulse HEALTHY,
  problems=[]. The four post-11:58 commits (020f9f8, 4ec0e1a, 174963f, 2184a2a) are now live;
  first armed session for the orb cap-release schedule + hi52 contract = 09-03.
- **Ledger repair (one row, owner-directed "fix the ledger"):** HDFCAMC rec
  `01M0YEV6301CGP57ESYNTY2F9W` was /taken AFTER `expire_stale` had stamped its ENTRY row
  `01M0YEV6327D902NQ5K3C3HA3Z` outcome_label='no_action' (closed_at 08-26 15:45); `close()` completes
  only `WHERE outcome_label IS NULL` (pipeline.py:494), so the real P&L could never land. The 09-02
  `take()` un-expire fix (2184a2a) is prospective only. Applied at 00:2x IST with the engine idle
  (health pulses only): `BEGIN IMMEDIATE; UPDATE learning_ledger SET outcome_label=NULL,
  closed_at=NULL WHERE entry_id=? AND outcome_label='no_action' AND exit_px IS NULL AND position_id
  IN (SELECT position_id FROM positions WHERE state='OPEN')` — rowcount 1, verified before/after;
  a generic scan (entry rows of OPEN positions with a non-NULL label) now returns 0. HINDZINC's
  entry row was already NULL. Script recorded in COMMANDS.md ("One-off ledger repair").
- **Status recap from the 09-02 phase audit (`scripts/g2_evidence.py`, workflow-verified):** G2
  NOT met on digest-before-open (73%), owner executions (2/5, 0 closed), rec-days (6/20); Phase-2
  code scope 19/19 shipped; Phase-3 (§8.4) = 2 shipped / 6 seams / 5 partial / 9 absent.

## 2026-09-02 (late evening, owner-directed) — /code-review fixes applied: 7 surgical, refactors deferred

- **Review pipeline:** 8 finders → 29 candidates → 29 adversarial verifiers → 22 confirmed,
  6 refuted, 1 re-corrected by the manager (the corp-action refutation holds for Kite-adjusted
  watchlist history only — the 600 extended names are bhavcopy-raw with no self-heal; hi52's
  proximity window inherits phantom highs once they reach min_sessions ~mid-Jan 2027, and the
  archive-backfill plan inherits the same exposure. DEFERRED with that horizon).
- **Fixed (each test-pinned unless noted):** (1) learning-ledger corruption — take() now un-expires
  outcome_label/closed_at so an expired→taken→closed trade records its real outcome (was
  permanently 'no_action'); (2) hi52 STRATEGY_CONTRACTS entry (the same-commit rule violation —
  IDFCFIRSTB was evaluated under the generic frame); (3) catchup_safety_jobs set/clear symmetry on
  the 30-min sweep path, extracted to a tested helper, clear fires only on an ACTIVE latch (no
  no-op WARNING churn) — completes the 09-01 boot-only fix; (4) five pre-WO-26a store wrappers
  to_thread→_off + a source-level pin that no shared-executor call site remains; (5) funnel_zero
  alarm quiet on a deliberate cap=0 (known cap authoritative; only cap=None falls back to the
  zero-forwarded shape); (6) bar-builder placeholders can no longer evict real cached ranges (the
  WO-25a cache survives stale-minute late-tick bursts); (7) _shadow_catalyst NaN comment corrected
  (the old text was backwards and would have misled a refactor).
- **Post-fix single-lens review:** no blockers; its two items (untested closure branching, no-op
  clear churn) drove the helper extraction in (3). Notably it REFUTED my own framing: the sweep
  path already froze mid-session via per-job data_freshness causes — (3) adds only the symmetric
  aggregate, no behavior expansion.
- **Deferred per the owner's no-over-engineering directive (recorded, not lost):** proximity-math
  consolidation (+ dead rolling_max_high), shared backoff helper, generic column-migration helper,
  dashboard polling hook, batch-leg scanner interface, bulk bars_1d fetches (hi52 sweep + builder
  medians), event-study stats sharing, Telegram subject resolver + split-retry dedup, hydrate
  catalyst-ref undercount (needs a journal schema change), inert hold_sessions knob.
- Full suite green (count in commit). Deploys at next boot.

## 2026-09-02 (evening, owner-directed) — orb slot allocation: score de-saturation + cap release schedule

- **Owner question ("why not JSWENERGY/KALYANKJIL/VMM/IDEA/OIL today") forensics:** all five are
  NIFTY200, all FIRED orb, all refused at the strategy-day cap — spent entirely in the window-open
  burst (10:50:07), three at score 1.0 (IDEA 36 suppressions, VMM 27, OIL 8). Third straight session
  of the same signature (08-27 KALYANKJIL, 09-01 PERSISTENT/HCLTECH/INFY). External validation:
  none of the five was a clean orb/brk20/hi52 shape (rating-filing drifts, spike-fades, macro
  creep) — the misses were defensible, the ALLOCATION was not.
- **Root cause:** orb score = min(1, vol_ratio/3) clamps most post-range bars to a 1.0 tie, so
  WO-1 ranking + 08-27 displacement had nothing to discriminate with, and first-fire-wins spent the
  cap in minute one.
- **Shipped (deploys next boot):** (i) orb score → vol_ratio/(vol_ratio+3) — monotone squash, 1/3
  at threshold, asymptote 1 never reached, no ties (ranking heuristic, no edge claim, not
  learnable); (ii) `cap_release_schedule` (owner knob): orb 3@10:00 / 5@11:30 / 7@13:00 cumulative
  tranches keyed to the candidate's ENTRY time (ts+1m, the file convention) — effective cap =
  min(flat, tranche), schedule can only hold capacity back, batch admit stays flat, §9.6
  Clock-free. Deliberately NOT raised: the orb cap (24 straight analyst declines of the class).
- **Two-lens review caught 1 blocking + 2 should-fix + 3 accepted-documented, all resolved:**
  displacement budget was flat-cap-derived (a busy tranche-1 could burn the day's churn allowance —
  now tranche-aware, test-pinned); tranche boundary used bar-close not entry time (fixed);
  duplicate "9:00"/"09:00" keys silently collapsed (now a loud error). Accepted + documented:
  ≥(1−margin)-score incumbents are undisplaceable (rare ≥27× volume prints, was MOST fires before);
  muhurat sessions open past the last release get the flat cap (45-min session can't stagger);
  nightly-review score medians will step down across the deploy boundary (formula rescale, not a
  quality regression — this line is the marker).
- Full suite green (count in commit). Plan: slot-allocation addendum added before WO-20.

## 2026-09-02 (12:35, engine running mid-session, NOT restarted) — decision log printed position ULIDs for the owner's exits

- **Reported (owner, screenshot):** the dashboard's Decision log showed `01M0ZKFMN1T15X3JZMKCDYQDW4`
  as the subject of every `intraday_analyst exit` row while enters showed the ticker. Cause:
  `ExitAction`/`Modify*Action` carry only `position_id` and `CancelAction` only `order_id`
  (`engine.core.contracts`); `/decisions` returned the raw payload and `DecisionsPanel` printed
  whichever id it had. Verified against state.db (read-only): all 16 exit proposals resolve —
  `01M0ZK…` = HDFCAMC, `01M110…` = HINDZINC, both `origin=recommended` (the owner's own positions).
- **Fix (red-first, `test_decisions_subject_resolves_position_and_order_ids_to_symbols`):**
  `/decisions` now carries `subject` — enter → tradingsymbol; exit/modify-* → `positions.symbol`
  via position_id; cancel → `orders.position_id` → symbol; an id with no row stays visible as the
  id, never blank (`_decision_subjects`, two batched lookups). `DecisionsPanel` shows `subject`,
  falls back to the `/positions` snapshot on an engine that predates the field, and keeps the raw
  id in the cell tooltip (provenance). `test_api_routes.py`: 44 passed.
- **Deploy:** `npm run build` 12:32 — `dashboard/dist` is served from disk by the running engine
  (StaticFiles), so the FRONTEND fix is live now through the snapshot fallback (verified `/`
  serves `index-BEzBKlj4.js`). The BACKEND `subject` field lands at the NEXT engine restart —
  deliberately not restarted at 12:3x with HDFCAMC/HINDZINC OPEN and exit chains firing (08-18
  rule: a cosmetic change never buys a mid-session service action).
- **Post-review (`/simplify`, 4 angles, 13:1x):** the client-side positions-snapshot fallback was
  REMOVED from source — a second, weaker copy (no cancel leg) of a rule the engine owns, reachable
  only in the rebuild-without-restart window; `_decision_subjects` collapsed to one local lookup
  helper + a coalescing chain (same six asserted cases). `dashboard/dist` was deliberately NOT
  rebuilt, so the served bundle keeps the stopgap until the restart (harmless: it prefers the
  server `subject`). **Pair the post-close restart with `npm run build`** to bring dist in line.
  **Named follow-ups (behaviour, not cleanup — out of this diff):** (1) `RecommendationsPanel`
  matches provenance on `d.proposal.tradingsymbol`, so exit/adjust cards never find their gate
  reasons — should match on `subject` with an action↔kind guard; (2) the Telegram owner-approval
  prompt (`pipeline.py` `_request_owner_approval`) prints the bare `position_id` — the same defect
  class on the higher-stakes channel; stamp `position["symbol"]` at write time there.

## 2026-09-02 (midday hotfix, owner-reported analyst failures) — prose-overflow clamp + sweep-crash fix, deployed 11:58

- **Fault 1 (reported):** exit-path analyst calls died `string_too_long` on `exit.thesis` — the flat
  wire schema advertised `thesis` uncapped (the 600 limit lived only in the client-side prose note);
  retries re-invite the same verbosity so they cannot converge (the WO-21 argument, value-shaped).
  Impact: 9 failed calls (~$1.55); 10:4x chains recovered on retry, the 11:43-11:45 chains were
  TERMINAL — both refreshed HDFCAMC/HINDZINC exit recs lost that cycle. Fix (`e567eb9`, red-first):
  wire schema now advertises maxLength derived from the contract; `_sanitize_guidance_extras` clamps
  ADVERTISED prose to the MATCHED model's declared cap (`guidance_prose_clamped`); min_length/enum/
  numeric enforcement untouched.
- **Fault 2 (found while verifying, self-inflicted):** the 09-01 eligible-pin filter used `.get()`
  on cat/cat_reversal WatchlistRow NamedTuples → `window_open_sweep_failed` at 10:51 killed today's
  batch admission (ins/cat) AND hi52's first sweep. It shipped inline without a test on the real row
  type. Fix (`e3dc1b0`): tested `_watchlist_rows_for_symbols` helper, attribute access, regression
  test on the real NamedTuples. Lesson: review-fixes get red-first tests too.
- **Deploy + live verification:** suite 2007 green → clean stop/start 11:57-11:58 → re-fired
  window_open sweep 12:01 SUCCEEDED: `hi52_sweep symbols_scanned=800 candidates=1 admitted=1` —
  hi52's FIRST production shadow candidate (IDFCFIRSTB) + a cat candidate (TMCV) admitted; both
  refreshed exit recommendations delivered 12:01:13/12:01:19 (attempt-2 convergence observed:
  with the length-half clamped, the model self-corrects the remaining `exit.reason` enum slip on
  retry — that enum was the hidden second error in the morning's 2-error chains; deliberately NOT
  coerced, R1). First wide universe build also confirmed in production: `extended=600` at 10:38.

## 2026-09-01 (21:48, engine stopped 21:47) — hi52 pre-registered backtest: first run + verdict

- **Run:** `scripts/backtest_hi52.py` vs market.duckdb (window 2020→2026-09-01 ⇒ effective
  2022-07-12 start, 1027 sessions, 2731 symbols; params byte-identical to the live shadow, N=1, no
  sweep). Report: `data/reports/backtest_hi52_2026-09-01.json`.
- **Discrete fresh-cross (the live hi52 rule): GEOMETRY VIABLE at T+5/T+10/T+20 and CPCV
  PROMOTABLE at all three** — T+20 median NET +1.94% / mean +2.78% / 59.0% hit / n=2545;
  fold_pass 93.3% vs 60% bar, median passing 0.126%/day vs 0.016 floor. Nominally stronger than
  `ins` (+1.58% net T+20).
- **The academic rank construct did NOT transfer:** top-decile monthly rank not promotable
  (33-50% fold pass), and the long-short spread is NEGATIVE (−0.9..−1.4%) — the cross-section
  here mean-reverts at these horizons; the EVENT (fresh cross) carries the signal, not static
  proximity.
- **Frog-in-the-Pan splits INVERTED in-sample:** jumpy approaches and gap-day entries OUTPERFORMED
  smooth/quiet ones (T+20 mean net 3.10 vs 2.03; gap-only 4.12) — the planned quiet-approach
  filter is not supported and will NOT be added.
- **Critical caveat, verified post-run (depth query):** bhavcopy full-market history begins
  2026-07-13 (~35 sessions); only 195 symbols have ≥378 sessions (kite_official, watchlist-scoped,
  2022-07-12→). The measured population is therefore the deep-history ~index class; the
  EXTENDED-name thesis (WELCORP/DYCL class — the original motivation) is UNTESTED (n=22, a
  survivorship-odd sliver). Plus the standing caveats: delisted names absent (optimistic), a
  bull-heavy window, current-membership index proxy.
- **Verdict (§8.6):** no promotion — shadow soak continues and now covers exactly what the
  backtest cannot (the true forward population incl. extended names, from tomorrow's first wide
  08:30 build). **Named follow-up:** historical bhavcopy archive backfill (2022→2026-07, date-keyed
  job over NSE archives) to re-run the study genuinely full-market; re-run also naturally
  strengthens as daily bhavcopy accumulates (~126 extended sessions by mid-Jan 2027 without
  backfill).

## 2026-09-01 (evening, owner-approved "go ahead") — §3.2.4 batch-universe extended leg + hi52 shadow + proximity features

- **Evidence first (3-agent workflow, movers validation):** all six out-of-universe movers the owner
  flagged (ENGINERSIN +7.2%, SSWL +14.9%, DYCL +10.1%, CAPLIPOINT +8.1%, WELCORP +6.9%, AEROFLEX
  +5.4%) printed/approached fresh 52wk/ATH on 09-01, four within ~2-7% of their high at the prior
  close; the six NIFTY200 movers sat 62-87% of their highs (no breakout shape; four died at the orb
  strategy-day cap, ITC never fired, RELIANCE forwarded → no_action). bars_1d already full-market
  (bhavcopy unfiltered) — widening needed no new ingestion.
- **Shipped:** (1) §3.2.4 extended leg — criteria-passing non-index rows (MIS ∩ EQUITY_L EQ-master ∩
  not-surveillance ∩ ₹5cr, top 600 by median) persisted included=False/['not_nifty200'] behind
  data.batch_universe_enabled (rollback = flag off; replace-write clears stale rows on retry);
  equity master now retained from the same EQUITY_L download (cached, reuse-on-failure).
  (2) get_batch_universe_symbols view; the 3 inline eligibility-predicate copies centralized into
  store methods. News resolver + catalyst digest + pre-open breakout advisory → batch view; brk20/ins
  actionable legs deliberately stay on the eligible view; BOTH cat legs' origination pinned to the
  eligible set (frozen WO-18 verdict populations + shared catalyst budget stay uncontaminated).
  (3) `hi52` shadow scanner (8th rule, NO_EDGE at birth): 0.95×52wk-high fresh-cross + vol confirm
  over the batch universe, prescreen cap 3, Frog-in-the-Pan diagnostics journaled; §6.1 addendum
  pre-registers the full backtest protocol. (4) prox_52wk_high/prox_20d_high daily features +
  indicators.rolling_max_high.
- **Three-lens review before commit caught 5 should-fixes, all fixed:** stale-extended-row rollback
  gap (→ replace_universe_daily delete-then-insert); hi52 sweep cost/timing on the live path +
  score-clustering slot competition (→ leg moved AFTER the actionable admit, own second admit call =
  leftover capacity only, window_open-only); ex_map horizon hardcoded to brk20 (→ max of both);
  cat shadow-population skew from the news widening (→ origination pinned to eligible).
- **Validation:** full suite green pre-review (1989) and re-run post-fixes (count in commit); tick
  watchlist + risk gate verified untouched by two independent mechanisms (included_only + NO_EDGE).
  Deploys at next boot; first extended build tomorrow 08:30.

## 2026-09-01 (afternoon, owner-directed follow-up) — origination-liveness alarms + §2.6/§3.2.12 plan addenda

- **Shipped the two alarms the latch incident called for**, on the always-on 60s health pulse
  (`HealthMonitor._check_origination`) riding the WO-25b episode cadence (change alerts at once,
  unchanged reminds every 30 min, recovery announced once):
  `entries_frozen_in_session` — armed mode (RECOMMEND/AUTO) ∧ risk_state≠NORMAL ≥30 contiguous
  in-session minutes (grace covers the ~17-19 min legitimate morning warm-up freezes; active causes +
  state + duration ride the Telegram text). `funnel_zero_in_session` — no forward PROGRESS (the
  day-cumulative forward count static while published slots grow past the last-progress baseline)
  ≥120 contiguous in-session minutes while NORMAL, gated on remaining §5.6 forward capacity.
- **Three-lens adversarial review before commit** (state-machine / safety-interaction /
  time-session) caught 1 blocking + 1 should-fix, both fixed: (i) v1 used `forwarded==0`, which goes
  permanently mute after the day's first forward (cumulative counter) — replaced with
  baseline-progress stall detection + governor-cap gate (spent cap = quiet by design; unreadable
  cap narrows to the zero-forwarded shape); (ii) alert text carried no diagnostic detail — added a
  `problem_details` side-channel that rides the message, never the episode identity. Time-session
  lens: zero findings (30-min grace clears every documented legitimate boot-freeze duration).
- **Validation:** all new tests written red-first; health file 27 passed; full suite green
  (count in commit). Wiring probe reads prescreen_day_slots + `governor.prescreen_forward_cap()`
  on the shared conn (same-loop discipline verified by the review's safety lens).
- **Deploy (owner-directed "do it now", executed just past close):** restart request landed at
  15:29:05 IST — inside the close minute, one of the flagged dangerous moments — so held to
  15:32:30, then clean stop 15:32:53 (2s, only news polling in flight) → start 15:33 on e9e1376.
  **Boot verified 15:34:** `startup_complete` mode=RECOMMEND frozen=[] needs_login=false,
  crash_recovered=false (clean stop properly recorded this time); `catch_up_complete` clean → the
  e462c1b clear branch ran as the designed idempotent no-op (`risk_cause_cleared
  catchup_safety_jobs was_active=false`, resolved NORMAL); health pulse beating on the new build
  (STOPPED during boot → HEALTHY, problems=[]) with ZERO `origination_watch_failed` events —
  `_check_origination` runs clean every pulse, correctly inert out-of-session. First armed
  evaluation window: tomorrow 09:15 IST. IMPLEMENTATION_PLAN updated: §2.6 step-5
  freeze-latch-symmetry + origination-liveness addendum; §3.2.12 HealthMonitor summary block.

## 2026-09-01 (mid-session hotfix, owner-directed) — catchup_safety_jobs freeze latch: 2 sessions of silent zero-origination

- **Found while investigating the JINDALSAW/BALRAMCHIN miss (out-of-universe, separate writeup):**
  `catchup_safety_jobs` (FROZEN, detail `data_freshness:instruments`) was set 08-31 09:59:33 by the
  step-5 belt-and-suspenders freeze (lifecycle.py) and had **no clear site anywhere** — a later
  successful catch-up pass cleared nothing, and the latch survives reboots via `risk_state_causes`.
  Effect: `_drain_one_forward`/`_evaluate_forward` require risk_state==NORMAL, so 08-31 saw
  432 fires → 17 prescreen slots → 0 forwarded → 0 entry proposals (4 exit recs only, exits bypass
  the gate), and 09-01 repeated it (881 orb fires, 13 slots, 0 forwarded) until this fix. The
  09-01 boot's `catch_up_complete` was fully clean (`failed:[], frozen:[]`) — the latch was stale.
- **Fix:** step-5 inverse branch in `SessionLifecycle.startup` — a clean catch-up pass (no
  frozen_reasons, not killed, latch wired) clears `catchup_safety_jobs` via the cause ledger
  (idempotent, cause-scoped; owner_pause/floor/warm-up causes untouched — same
  clear-only-what-was-re-verified rule as the warm-up lift). Freeze side unchanged.
- **Tests:** watched the repro fail first (stale latch survived clean startup), then green: 3 new
  (`test_startup_clears_stale_catchup_safety_latch`, `..._respects_other_causes`,
  `..._freezes_on_catchup_safety_failure` pins the fail-closed side). Full suite 1960 passed.
- **Deploy:** owner-directed mid-session restart ("fix right now — impacting current trade cycle"),
  clock re-observed 11:54 IST, log tail checked for in-flight jobs before stop (routine tick flushes
  only). Clean stop 11:55 → start 11:56 (commit e462c1b live). **Boot verified:** 11:57:43
  `catch_up_complete (failed:[], frozen:[])` → `risk_cause_cleared catchup_safety_jobs
  was_active=true` → `risk_state_changed FROZEN→NORMAL`; by 12:12 open causes = [], mode/risk =
  RECOMMEND/NORMAL; forwarding resumed — `signal_candidate_queued` BHEL/NAUKRI/IDEA and
  `forward_drained` CIPLA 12:06:25, BHEL 12:09:29 — first candidates to reach the analyst since
  Friday. Boot flagged crash_recovered=true (NSSM stop beat the clean-STOPPED commit — benign,
  every startup is a full recovery by design).
- **Follow-ups (not shipped here):** frozen-during-session-hours alarm + funnel-zero alarm
  (slots>0, forwarded=0) so a silent zero-origination day pages; IMPLEMENTATION_PLAN §2.6 step-5
  addendum for the clear branch.
