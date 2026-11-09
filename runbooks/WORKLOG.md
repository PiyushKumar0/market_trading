# WORKLOG — autonomous operations log

Newest first. About two weeks of entries are kept; older ones are in git history.

## 2026-11-09 16:20 — Leverage start key renamed after the close (owner: "Rename now"); engine restarted on the day's renames

- **Commit `8bc45af`:** `max_leverage`'s start key is `live_auto_start_x` in limits.yaml, the `MaxLeverage` model and
  the loader test; nothing reads it until live routing is built. The cap's comment no longer names an overlay file that
  was never built.
- **Deploy:** close jobs done (reco_expire 15:45, bar_reconcile 15:52), nothing in flight; stopped 16:06:20
  (`exit_clean`); unit gate 4262 passed / 19 skipped, offline tier 521 passed; reseeded 16:17:22 (limits data equals HEAD
  apart from the key, envelope unchanged, `protected_config_status.py` OK for both); started 16:17:48 on `8bc45af`, so
  `c87703c` is live too. Selftest all PASS with both `protected_store` checks, frozen []; catch-up failed nothing; ticker
  303 tokens; `engine_ready` 16:20:01, `boot_contract_ok` 135.1 s (a post-close boot first re-checks the day's minute
  bars, 300 symbols); no ERROR lines. Ruff's two findings at `limits.py:299` are at HEAD too.

## 2026-11-09 09:40 — Identifier cleanup and the brk20 report re-key; overnight outage was the lid closing

- **Owner, answered ~09:2x:** rename the signed store's leverage start key to `live_auto_start_x`; keep migration
  0003's filename (applied migrations are tracked by filename); re-key the registered 2026-09-12 brk20 report.
- **Commit `c87703c`:** `forward_verdicts`' regime-gate arms are `RegimeGateArms` / `regime_gate`; the offline tests load
  the study scripts as `exit_geometry` / `regime_filter` / `selection_filters`; the exit-geometry cost-of-stop keys
  name the arms; dead section-sign folds removed; migration 0003's filename no longer repeated in its header or
  `causes.py`; this log and FABLE_HANDOFF say "the working branch". 175 affected tests pass; ruff clean.
- **brk20 report:** re-keyed to the script's current names; all 1811 numbers unchanged; original bytes in
  `data\backups\backtest_brk20_2026-09-12.original.json`; the rec card's brk20 evidence line renders identically.
- **Outage 02:02–09:03 (not the deploy):** the 01:31 boot ran 30 min with no errors. At 02:01:58 the ticker lost its
  connection and the engine took a graceful SIGINT stop (`exit_clean` 02:02:07); Kernel-Power 506 at 02:02:22, "entering
  Modern Standby, Reason: Lid"; asleep 06:52–09:03. Started 09:03 on `8c6fdea` (the 01:31 code); Kite login 09:04:17,
  post-login recovery ok. `c87703c` goes live at the next restart.

## 2026-11-09 01:35 — Cleanup finished: study labels named, analyst prompts and protected configs cleaned (owner: "Apply now", "Clean both now"); engine restarted 01:31

- **Commits:** `97bf032` study labels and the remaining citations (four Sonnet agents renamed the research labels, the
  rest inline; JSON keys, CLI choices and report text follow, while stored reports keep their keys); `4f62873` this
  log's labels; `2b1d50e` analyst prompts and the re-registration; `3cf3c21` protected configs.
- **Prompts:** the intraday strategy contracts (template `2026-11-09.1`), the news schema descriptions and the pre-open
  and nightly schema wording lost their citations; no instruction, field or enum changed. The analyst-value verdict is
  re-registered from 2026-11-09 (`forward_verdicts.ANALYST_REGISTERED_FROM`); the kill rules keep 2026-11-02. The cat v2
  and cat_reversal plan entries record the wording-only news change inside their windows.
- **Ledger:** `config/research_trials.yaml` names studies; three memory pointers broken by the memory renames fixed;
  the registered rules point at `scripts/forward_verdicts.py`.
- **Deploy:** nothing in flight; stopped 01:30:30 (`exit_clean`); patch applied and reseeded (parsed data identical,
  signatures match the files); started 01:31:25. Selftest all PASS, both `protected_store` checks included; frozen [];
  ticker 303 tokens; `engine_ready` 01:32:02; `boot_contract_ok` 37.1 s; no ERROR lines.
- **Verified:** unit gate 4261 passed / 19 skipped plus one failure that passes alone (the replay heartbeat test's 1 s
  wall-clock bound under 8 workers); offline tier 521 passed; ruff findings are the ones at HEAD.

## 2026-11-09 00:10 — Repo-wide cleanup of retired plan citations; memory, docs and this log pruned (owner: "clean up redundant memory, comments, text and work history")

- **Commits:** `50ca381` comments and docstrings (4,581 blocks in 415 files, comment-only: same Python AST with
  docstrings blanked, same YAML/TOML data); `3448212` runtime strings (464 blocks, same AST with string text blanked)
  plus renamed scripts, tests and identifiers; `0cd0c2a` docs; `2814b77` the owner's 11-08 muhurat timings, which were
  on disk but uncommitted. Engine not restarted: the new wording (alerts, log hints) goes live at the next boot.
- **Verified:** LLM fingerprint (system prompts, output schemas, template version) identical; unit gate 4260 passed /
  19 skipped; offline + chaos 591 / 19; dashboard tsc + 95 node tests; ruff findings unchanged (13, pre-existing).
- **Renamed scripts:** `kite_candle_adjustment_check.py`, `entity_resolution_sample.py`,
  `recommend_gate_evidence.py`, `kite_candle_latency_probe.py` (were named after plan IDs).
- **Left for the owner, finished in the entry above:** the analyst prompt text, the protected-config comments and the
  research study labels.
- **Memory:** 66 files → 60 (merged, renamed, condensed; backup `~/.claude/backups/memory-before-cleanup-2026-11-08.zip`);
  index 12.9 KB → 8.3 KB. **This log:** entries before 2026-10-26 dropped; the one still-open item (ranking the tick cap
  over the NIFTY 500 core) moved to IMPLEMENTATION_PLAN's open questions. **graphify:** the stale 10-28 graph moved to
  `~/.claude/backups/graphify-out-2026-10-28`.

## 2026-11-08 00:28 — Claude login death 11-06 19:06 → owner re-login 11-08 00:16 (owner: "Check the claude login failure")

- **Outage:** last good engine call 11-06 15:29:19; from 19:06:15 every call failed `auth_failed` "OAuth session
  expired and could not be refreshed" (22 auth_failed, 4 sdk_error, 1 timeout in `agent_calls`). All four alerts
  delivered (11-06 19:06, 21:10; 11-07 06:39; 11-08 00:13). Market closed throughout, so the loss is the 11-06 evening
  news scoring (~280 headlines) and the 11-06 nightly review (`job_runs` failed; date-keyed catch-up retries it, gives
  up after 7 days).
- **Fix (owner):** `.credentials.json` re-created 00:16:06 (VS Code session started 00:14). `check_claude_login` PASS,
  refresh expiry 2026-12-05 05:41 IST; access token expires 08:16:05 (8 h lifetime). One `-p` call through the pinned
  `data/run/claude-cli/2.1.288/claude.exe` returned a normal result in 15 s.
- **Evidence:** host in Modern Standby 17:02:17–21:09:20 (Kernel-Power 506/507). The 19:06 call, the first after the
  old 8 h access token expired, started 19:02:52 inside that window and hung 203 s. In the same seconds Kite
  backfill got `RemoteDisconnected`, BSE `ReadTimeout`, Telegram sends failed and the store stalled 174 s. Awake
  21:09–22:00 with the news poll fetching (pending 270→280), calls still failed in ~1.5 s, so the grant was dead and
  not just unreachable. Every transcript from 11-06 12:09 until the re-login is `sdk-py`; the only interactive
  activity that day was VS Code sessions resumed 09:47–09:50. The CLI prints the same message for a blank login
  (10-22), so the message can't tell a blank file from a refused one.
- **Verdict (probable, not proven):** most likely the 19:03 refresh was accepted server-side but the reply was lost
  on the standby network, leaving a used refresh token on disk. Not excluded: a refresh race with the 09:47 VS Code
  process, or server-side revocation. No CLI debug log exists to settle it. "Pinned 2.1.288 can no longer refresh" is
  unlikely (the 11-06 08:39 call followed a 10.7 h gap, so 2.1.288 refreshed then); the first refresh after 08:16
  today settles it.
- **Side finding:** orphaned CLI PID 32428 (parent engine python 7944) has been alive since 11-07 00:40:14. It is the
  child of the nightly_reviewer call that hit `timeout` at 00:46:14, which `_closing`/`aclose` did not end. Kill was
  refused by the auto-mode classifier; left for the owner (`Stop-Process -Id 32428 -Force`) or the next engine restart.

## 2026-11-05 20:05 — Keep-awake lapse check: per-pulse check and re-arm, deployed (owner: "Make the required changes, review and validate")

- **Trigger:** the owner asked why the PC sleeps on AC while the engine's keep-awake is engaged. On 11-05 one process
  engaged it 09:15:30 and released it 15:30:30, yet the host idle-slept 5 times 10:08–13:39 (lid open, AC, 96 min),
  each about 15 min after the screen went off; two lid closes added 38 min.
- **Evidence** (`..\mt_build_notes\section3\engaged_spans.ps1 -Since 2026-10-01`): every lid-open AC idle sleep while
  engaged (10-26, 10-28, 10-30, 11-02, 11-05) followed an earlier in-session sleep in the same process. Before any
  such sleep, idle spans without audio held 23–99 min on 14 days. Kernel-Power 566 `Reason=12` marks these as idle-timer
  sleeps, the same transition the 11-03 15:30:01 release set off. Unexplained: 10-26 held 57 min after a 6 s lid sleep,
  and the idle sleep fires 5–20 min after screen-off although AC "Sleep after" is 60 min. Ruled out: the calling thread
  (event loop), restarts, MDM or GP power policy, audio.
- **Change** `448cbaf`: each engaged pulse reads this thread's flags (`SetThreadExecutionState(0)`, measured to be a
  pure query) and the system-wide state (`CallNtPowerInformation(SystemExecutionState)`, no admin). A missing
  `ES_SYSTEM_REQUIRED` is re-asserted, then cleared and set if that did not take. New lines: `keep_awake_lapsed`
  (WARNING, once per episode), `keep_awake_restored`, `keep_awake_state` every 5 pulses, `keep_awake_probe_blind`; the
  engage line carries `system_state_before`, `system_state`, `own_visible`. The health pulse now guards the call.
- **Review** (code-review, high): 10 findings. Fixed 6: the two DLLs resolve separately, a failed read never triggers
  the clear, re-register unless the read is proven blind, `own_visible` recorded, a stale `process_memory` reference,
  one episode-close path. Declined 2: the real-Win32 tests stay (no CI; `test_process_memory` precedent), and
  `PowerCreateRequest` waits for the verdict, since its status is no more readable without admin. Documented 1: a lost
  registration stays invisible while another process holds the bit.
- **Validation:** 21 keep-awake tests; the 10 first written all failed against the HEAD module. A real-Win32 run
  showed `own_visible: true` and caught a simulated lapse (thread flags cleared), which the same-flags re-assert
  repaired. Full gate 4260 passed, 19 skipped. ruff and mypy clean on the changed modules; the `health.py:389` mypy
  error predates this (`e9e13762`).
- **Deploy:** engine idle (20:01 sweep caught up nothing). Restart 20:05:01, boot 20:05:24, `startup_complete` 20:07:24
  (`integrity_ok`, frozen []), scheduler 20:07:30, health `problems: []`. The one ERROR is the old process's
  `warmup_refresh` CancelledError at stop, as at the 08:49 and 19:00 stops.
- **Verify next session (11-06):**
  - The 09:15 `keep_awake_engaged` line must show `own_visible: true` (`0x00000000` → `0x00000001`): the first proof
    that the read sees the service's request. `false` with `keep_awake_probe_blind` leaves only thread checks.
  - For each in-session sleep, run `engaged_spans.ps1 -Since 2026-11-06`. A `keep_awake_lapsed` after a resume means
    Windows drops the request; the re-arm is then the fix, and no idle sleep should follow it. `keep_awake_state` lines
    with the bit present before an idle sleep mean Windows ignores it; the next options (AC sleep Never, display on,
    a periodic idle-timer reset) are the owner's.

## 2026-11-05 18:40 — Co-movement cap same-sector only (owner: "Same-sector only", after "This correlation limit in its current form is not useful")

- **Trigger:** paper rejected TIPSMUSIC (brk20) at 09:53 on `co_movement_cap` alone: 20d return corr 0.7051 with SYNGENE,
  reopened at 08:59 (entry above). The same pair also blocked paper TIPSMUSIC on 11-02 and 11-03, and real SYNGENE
  recs on 11-02 and 11-03 (the TIPSMUSIC rec was pending).
- **Premise checked, and my earlier guess was wrong:** the market did not cause the match. On 10-12 and 10-28 both rose
  7-12% while NIFTY 50 fell (−0.70%, −0.22%). The residual correlation is 0.65-0.71 against NIFTY 50, MONIFTY500 and the
  EW universe, and 0 of 300 universe stocks correlate above 0.7 with TIPSMUSIC.
- **Evidence for the change** (backup `market-20261105T015123`, current universe, 46 non-overlapping 20-session windows
  2023-02 to 2026-10, 1.75M pair-windows): cross-sector pairs above 0.7 had a median of 0.38 in the next window, with 10%
  still above 0.7 (unflagged pairs 0.22 and 2.3%). Same-sector pairs above 0.7 had 0.53 and 24% (unflagged 0.29 and 5.2%).
  NIFTY-residual flags do no better across sectors (0.36, 9%).
- **History:** 12 rejects over 5 pairs. Four pairs were cross-sector (TIPSMUSIC-SYNGENE ×7, JSWINFRA-MEESHO, HAL-MOTILALOFS,
  BSOFT-BHARTIARTL); only JSWSTEEL-JINDALSTEL (METAL) was same-sector. Each partner was confirmed by recomputing the logged
  correlation exactly.
- **Change** `9cbd83e`: `_co_movement_peers` keeps the held and pending symbols of the candidate's sector, plus every pair
  where either sector is unknown. Limit 0.7 and the 20d window are unchanged. On the live paper book, TIPSMUSIC is
  compared with nothing, JSWSTEEL with JINDALSTEL, SYNGENE with AUROPHARMA. 2 new tests (the classified case failed on
  the base tree); full gate 4245 passed, 19 skipped.
- **Gives up:** group companies in different sectors (JSWINFRA SERVICES, JSWSTEEL METAL).
- **Deploy:** waited for the post-session tick compaction (it hit its budget at 19:00:00 and resumes on the next run)
  and the 19:00 filings ingest. Engine stopped 19:01:09. The `limits.yaml` `co_movement_cap` comment now names the
  scope: comment only, parsed YAML identical to HEAD, reseeded (sha256 2430682d...), status exit 0. Boot 19:01:50 →
  `boot_contract_ok` 19:04:13: `integrity_ok`, risk NORMAL, frozen [], no failed jobs, 70 s down.
- **Verify next session:** the first co-movement verdicts must name the sector scope in the ledger text, and a
  cross-sector candidate must no longer fail `co_movement_cap`.

## 2026-11-05 08:58 — Paper voids: session-prep fix, caps 30/2/28, all 7 voided paper trades restored (owner: "Make the fix and restore the trades"; "Raise max cap to 30 and restore the trades"; "Do it now")

- **Finding:** all 7 paper voids to date were paper no-stop test time-only positions (no stop, no target, exit session 11-17 or later),
  which no bar of the gap could close; prep voided them only because their bars could not be fetched.
  - 10-28 RAIN, NYKAA and 10-30 BHARTIARTL: `downtime_uncovered`.
  - 11-04 SYNGENE, TVSMOTOR, AUROPHARMA, JINDALSTEL: `prep_incomplete`. The host slept through the morning and resumed
    at 10:29; prep started at once, before the network was back (Telegram sends failing until 10:30:31). The first Kite
    historical call took 93 s (10:29:36–10:31:09), the 120 s deadline cancelled the second, and `catch_up` and
    `corp_actions` both failed (`engine.log` 2090/2172). The window began 11-03 13:46: the silent connected feed that
    4d822db fixed.
  - The no-stop verdict reads `rec_outcomes`, not the paper book, so no verdict sample was lost.
- **Fix** `3381137`: catch-up skips a position no walk can close (`paper_prep_walk_skipped`); the ex-date rule still
  runs first. 4 new tests, each failing on the base tree; default gate 4243 passed, 19 skipped. It went live with the
  owner's 08:49 dashboard restart (the working tree), before the commit.
- **Owner capital change** (dashboard 08:49): 70000 → 120000, committed alone as `dd144c8`.
- **Caps** `2b16551`: `max_open_positions` 6/2/4 → 30/2/28 (total = MIS + CNC). The parsed YAML
  differs from HEAD only in those two keys. Reseeded with the engine stopped; `protected_config_status` exit 0. Tests
  `f78a8b1`. The caps bind real recommendations as well as paper.
- **Restore:** engine stopped 08:57:59. Backup `data\backups\manual\state-20261105-0858-pre-restore.db` (SQLite backup
  API, integrity ok). One transaction with row-count checks: 7 positions back to `OPEN` with `close_reason`,
  `close_basis`, `closed_at`, `realized_pnl` and `costs` NULL, as an open paper row carries them; their 7 `void`
  `learning_ledger` rows deleted. None of the 7 symbols was held. They had no GTTs and no exit orders, so nothing else
  changed. `paper_equity_snapshots` stay as history.
- **Boot** 08:59:25 (104 s down): `integrity_ok`, risk NORMAL, frozen [], `needs_login` false. Paper book 9 open (₹1.04
  lakh notional against the ₹1.2 lakh cap), 0 voids.
- **Prep verified** 09:15:00–09:15:01: `paper_prep_walk_skipped` names all 9 positions, `paper_prep_complete failed []`,
  no void, no late corporate action, no risk change by 09:15:25.
- **Rollback:** with the engine stopped, copy the backup over `data\state.db`; `git revert 2b16551` plus a reseed for
  the caps.

## 2026-11-05 01:40 — Fibonacci retracement study run: pullback clustering and the deeper brk20 entry supported, the pullback edge null, the brk20 confluence filter refuted; nothing wired (owner: "Do test with the retracement logic as well included with the analysis, especially for swing trades")

- **Question:** stock picking uses Fibonacci retracement nowhere (no scanner, rule or prompt). The analyst cited a
  61.8% retracement once, as commentary (PATANJALI brk20, 2026-09-29).
- **Registered** `73e6ef1` before any code or data run: the pullback edge `edge/fib_pullback` (N 3), pullback
  clustering `price_structure/fib` (N 1), the confluence filter `selection_filter/{brk20,hi52,ins}` (N 6, 6, 1), the
  deeper brk20 entry `entry_mechanism/brk20` (N 6). The rules live in the script.
- **Built** `ba0540f`: `scripts/backtest_fib.py` and 90 synthetic tests, by one Opus agent started 12:35 in market
  hours on the owner's "Now". Audited; 112 passed, ruff clean.
- **Run** 2026-11-04 15:31–15:33, as registered (COMMANDS "Fibonacci retracement study"). Report
  `data\reports\fib_retracement_2026-11-02.{json,md}`.
  - Pullback edge NOT SUPPORTED (null): limits at 38.2/50/61.8% did no better than the neighbouring non-Fibonacci depths
    (permutation p 0.36/0.49/0.25). Every depth had median net +0.95% to +1.65% over the whole sample; in 2026 the
    depths up to 56% are negative, the deeper ones about +0.2%.
  - Pullback clustering SUPPORTED: pullbacks ended within ±1 pp of a Fibonacci level 639 times against 558 expected (ratio 1.145,
    CI [1.033, 1.266]; placebo 1.004). It comes from 50% (1.24) and 61.8% (1.38); 38.2% has none (1.03).
  - Confluence filter: brk20 REFUTED (confluence n 411, median net −1.40% against FULL +0.61%, p 1.0); hi52 (n 13) and ins (n 7)
    INSUFFICIENT.
  - Deeper brk20 entry SUPPORTED, winner 61.8%: median net +1.53% (n 978, fill rate 12%) against the five-session
    limit's +0.61% (n 5,025, 59%).
- **Post-hoc checks (descriptive; they change no verdict).** Scripts and logs in `..\mt_build_notes\section3\fib_*`.
  - Both scripts reproduce the registered numbers exactly. Seven sampled trades and legs (2022–2026), re-derived
    from raw `bars_1d` by independent code, all match.
  - The clustering excess survives a symbol-clustered bootstrap (CI [1.041, 1.260]) and legs ≥ ₹50 wide (1.12, placebo 0.98), and beats 97.7% of
    random centre triplets. But it is no sharper at ±0.5 pp (1.09, placebo 1.10), and it fades by year: 1.24 in
    2023, 1.03 in 2025. The excess is 81 legs out of 13,707.
  - The deeper entry is a depth effect, not a Fibonacci one: median net rises with depth through the non-Fibonacci
    depths, and 67.8% (+2.00%, n 701) beats 61.8%. Strict trade-through fills change almost nothing (61.8% +1.50%,
    five-session limit +0.60%). 61.8% led the five-session limit in every signal year, by +1.56, +0.87, +1.52, +0.49 and +0.15 pp (2022–2026); in 2026
    every depth lost 1.9–2.7%.
  - The five-session limit's trades on the signals the 61.8% limit filled had median net −2.93% (n 968); the rest
    +1.39% (n 4,057). Net per admitted signal: five-session limit +1.02%, 61.8% +0.33%.
  - Confluent brk20 fills sit at least 4.3% (median 8.6%) under the 60-session high; the rest a median 4.1%.
- **Recorded:** the six ledger rows (null, supported, refuted, insufficient, insufficient, supported); IMPLEMENTATION_PLAN "Pending verdicts"
  holds the owner decision (whether a deeper brk20 entry gets its own registration once the kill rule reads);
  memory `fib-retracement-study-2026-11-04`. The analyst prompt is untouched: a change would stop the analyst-value
  population.
- **Not mine, left alone:** at 01:35 the live tree had uncommitted edits to `src/engine/oms/prep.py` (01:24) and
  `tests/unit/test_paper_no_stop.py` (01:22) from another session (paper no-stop test time-only positions skip the prep
  walk). The engine loads them at its next boot.

## 2026-11-03 20:50 — Silent connected feed: respawn fix built, reviewed and deployed (autonomy directive; diagnosis in the 16:20 entry)

- **Change (`4d822db`, built on `wip/feed-respawn`, fast-forwarded onto the working branch):**
  - In session, a DEGRADED feed silent past `ticker.tick_silence_respawn_s` (300 s) respawns the child. Silence is
    timed from the latest of the last tick, the last HEALTHY entry and the session open.
  - At most `ticker.max_tick_silence_respawns` (3) respawns per episode. A tick, or a new session date, starts a new
    episode.
  - `feed_degraded` is one-shot per episode. `feed_silent_unrecovered` (warning) fires once when the cap is spent.
  - These respawns publish no STALE, so they freeze nothing and page no risk change (`feed_health_to_latch`
    freezes only on STALE).
  - The DEGRADED and respawn lines log the child's `ws_connected` and its own `last_tick_age_s`.
  - A silence now costs about 5 min before the first fresh connection; on 11-03 it lasted 104 min.
- **The brief was wrong on one point:** `_last_tick_at` IS reset on every respawn (`_reset_spawn_stamps`), so the
  escalation's "since" is kept in episode state.
- **Validation:**
  - 14 new supervisor tests, each failing on the base tree.
  - Unit suite: 4,127 passed (7:16, with the engine's evening catch-up running).
  - Chaos cases 03/13/15/22: 12 passed, 3 designed skips.
  - ruff clean. mypy: no new errors on the touched lines.
  - The builder's own full run took 3 h 51 min across a 16:34–20:16 host sleep and had 2 timing failures
    (`test_native_preload`, `test_why`). Both pass alone and in the clean run.
- **Deploy:**
  - 20:42: the evening catch-up had finished at 20:31:20 with nothing failed (the host slept 16:34–20:16, so the
    18:00–20:30 jobs ran from 20:16); the 21:00 jobs had not started; the editable install pointed at the live tree.
  - Stopped 20:42:36 (31 s), started. Selftest ok; load-bearing catch-up ran `features_daily`, nothing failed or
    froze; `startup_report` NORMAL, integrity ok; the ticker connected (303 tokens); `engine_ready` 20:45:43;
    `boot_contract_ok` 148.2 s.
  - Worktree removed (junction unlinked first); branch deleted.
- **First live check:** the next session. A silence episode logs `ticker_tick_silence_respawn attempt=n/3`.

## 2026-11-03 16:20 — `/taken` fix deployed; market-pause study: hi52 NOT SUPPORTED, ins INSUFFICIENT; today's in-session feed losses diagnosed (owner: "Re-check the status of the system …"; chose "Cleanup, Deploy /taken fix, Market-pause study")

- **Cleanup:** `738a8d4` (committed before the session) deleted the shipped review-actions plan and the
  TradingAgents report.
- **Deploy (`8158a9a` + `738a8d4`):**
  - 15:53:29 checks: the 15:50 reconcile had logged `reconcile_done` ok at 15:51:56; the next job was bhavcopy at
    18:00 and `tick_compact` at 22:30; neither commit carries a migration or a dashboard change.
  - Stopped 15:53:47 (17 s), started. Selftest ok; catch-up caught up, failed and froze nothing;
    `startup_report` NORMAL, integrity ok; `engine_ready` 15:56:11; `boot_contract_ok` 125.2 s.
- **Market-pause study** (registered 2026-10-29, run as registered):
  - Snapshot built from the 11-02 nightly export with the engine running
    (`..\mt_build_notes\section3\snapshot_from_export.py`; `bars_1d` 2,194,052 rows).
  - Command: `scripts\backtest_market_pause.py --db data\research\market_2026-11-02.duckdb --end 2026-11-02`.
    Report: `data\reports\market_pause_2026-11-02.{json,md}`.
  - hi52 NOT SUPPORTED. TRADE n 1,456: median net +2.06%, CPCV 86.7%, p 0.0007, against FULL +1.67%. But PAUSE
    (n 383) has median net +0.20%, and the rule needs ≤ 0. The 2026 diagnostic split runs the other way: FULL n 306
    −0.79%, TRADE −0.95%, PAUSE +0.11%.
  - ins INSUFFICIENT: PAUSE n 13 < 30; TRADE (n 30) not CPCV-promotable; p 0.46.
  - Nothing is wired. The registration text left the plan with this commit (`git show 738a8d4:IMPLEMENTATION_PLAN.md`);
    the rule stays in the script's docstring.
- **Feed loss today: 167 of 375 session minutes had no live ticks.**
  - 09:15–10:17 was host sleep. Kernel-Power 506/507: lid closed on battery at 07:36, asleep until 10:17:33.
    Keep-awake cannot help with a lid close. The same morning sleep cost 21–99 session minutes on 11 sessions since
    10-01.
  - 13:46–15:30 was a silent connected feed. The ticker child heartbeated every second, its WebSocket stayed
    connected (no reconnect, error or close), REST calls succeeded and the token was valid, but no tick arrived.
    - The supervisor went DEGRADED at 13:48:09, sent one "Feed degraded" warning, and never reconnected (by design:
      only heartbeat silence respawns). The 15:53 restart ended it.
    - The same state occurred 11-02 14:51–15:22 (it ended when Kite dropped the TCP connection; the child reconnected
      and ticks resumed within 1 s), 10-07 13:24–14:25 and 09-30 13:23–13:55. Every tick silence since 09-15 ended
      with a fresh connection.
    - Fix specified in IMPLEMENTATION_PLAN "Known defects and test gaps" "Silent connected feed".
  - The 15:51 reconcile refilled most minute bars (`offline_bars` 49,155 of 65,827 compared; orb coverage 272 → 360
    of 375) and sent `reconcile_drift` (91 flagged). The boot's "INTRADAY coverage short" warning is the same gap, on
    orb, which is parked.
  - The 15:43 `store_stalled` came from host sleep after the session (keep-awake released at 15:30:01, sleep phase
    from 15:30:10), not a store fault.
  - Paper: none of the four open positions exits before 11-27, and JINDALSTEL (ins) filled at 10:28 while the feed was
    live.
- **Correction to the 11-03 status report:**
  - Counting every 506 → 507 span as downtime overstated in-session standby. The engine runs through the screen-off
    phase; only the sleep phase (`SleepEntered`, the last `SleepDurationInUs` of a span) suspends it
    (`..\mt_build_notes\section3\session_sleep.ps1`).
  - With keep-awake engaged and the laptop on AC (battery flat across the span), lid-open idle sleep still happened:
    11-02 13:04–14:07, 10-30 11:48–12:14 and 10-26 15:06–15:30. Today the same request held until its 15:30:01
    release. So `ES_SYSTEM_REQUIRED` alone is unreliable on this host, and no request override is set
    (`powercfg /requestsoverride` is empty). The host-power choice stays with the owner.

## 2026-11-03 11:10 — `/taken JINDALSTEL` refused as ambiguous: fixed and validated, not yet deployed (owner: "Fix the issue, review and validate.")

- **What happened:** at ~10:30 `/taken jindalstel 12 1102.80` replied "2 recommendations match JINDALSTEL — nothing done".
  The second match was `01M1ZP2K8JS6W1F8A1HW6EN0A6`, delivered and expired 2026-09-08, listed as a bare "10:27".
  The owner then recorded today's rec by its full id.
- **Cause:**
  - `/taken` resolved a ticker against the open recs plus EVERY expired rec, with no age bound.
  - `/veto`'s rule that a single live rec wins over expired ones did not apply to `/taken`.
  - `_rec_line` added the day only when delivery and expiry fell on different days.
- **Fix:**
  - `/taken` now resolves against `/veto`'s set: `decision_candidates`, live or expired inside the 3-session veto window.
    The new `include_skipped` flag keeps expired recs that already carry a skip reason, because `take` clears it.
    The single-live rule now applies to `/taken` too. The `decision` and `include_expired` flags are gone.
  - Side effect, intended: a lapsed entry the 15:45 sweep has not labelled yet is now `/taken`-able by ticker.
    Before, it became resolvable only after the sweep.
  - A listed row carries its day unless both its stamps are today.
- **Validated:**
  - Unit suite: 4113 passed. ruff is clean. mypy shows no new errors in the touched lines.
  - The new `/taken` tests failed before the fix: an out-of-window rec resolved, live plus expired gave the ambiguous list,
    and an earlier day's rec rendered a bare "10:27".
  - Replayed the real resolver and book over a snapshot of `state.db`, with today's row reopened in the snapshot only:
    `jindalstel` resolves to `01M6FWB6VEVQ4N9HPED5VPBFX1`, and `tipsmusic` (Mon expired plus today live) resolves to today's rec.
- **Deploy:** live on the next `mt-engine` start. No mid-session restart.

## 2026-11-02 17:09 — Tick compaction, failing since 10-26: diagnosed, fixed, deployed and validated (owner: "Tick compaction has been failing for more than 4 days now. Check, review, validate and fix.")

- **What failed:**
  - Every `tick_compact` run from 10-26 22:45 ended ok=False: 300 `tick_compaction_ambiguous` symbol-days of
    2026-10-23, and from 11-01 also 301 of 2026-10-30. The watermarks for 10-26 to 10-30 stayed failed.
  - The 30-min sweeps re-ran every failed day each pass, up to 46 runs a day (13,879 warning lines on 10-29).
  - Retention is gated on a clean run, so it stopped; tick partitions back to 09-24 stayed on disk.
  - The 10-30 entry's "they retry on their own" was wrong: the same 300 symbol-days failed identically on every run.
- **Cause:**
  - The host slept through the 10-23 22:30 slot (the log stops at 19:31). The 10-24 01:04 catch-up ran with
    today=10-24, so it compacted the 10-23 partition itself.
  - Every ticker reconnect re-sends each instrument's last state, still stamped with its last session (17:13–17:34).
    BarBuilder persists it into that date's partition. 3,032 one-row fragments landed after the compaction
    (ULIDs 10-24 02:22 to 10-26 04:17).
  - The 10-24 ones were exact copies, which the repair path absorbs. From Sunday 22:26 they carry volume, average
    price and bid/ask 0, so they were not inside the compacted file and `_absorbed` refused them.
  - 10-30 repeated it: a retry compacted it at 10-31 00:30, then 776 echo fragments arrived 11-01 23:27 to 11-02 00:17.
    The failing watermark's round-the-clock retries make every next Friday compact early.
  - The echoes are old: normally compacted Fridays hold them (10-09: 296 rows, 10-16: 1,500), merged at a compaction
    that ran after them. None was ever written in session: 0 late fragments created 09:00–15:30, and 0 backward
    day rollovers among 556 in 91 logs.
- **Fix** (`40c7475` + review fixes `9fe13e1`, branch `wip/tick-echo`):
  - Fragments beside a compacted file are reconciled by multiset difference (EXCEPT ALL).
    - Rows the file lacks are merged in by the same write-then-swap. A row on both sides keeps the larger count,
      never the sum. Leftovers of an interrupted unlink are deleted as before.
    - Only two `compact-` files are still refused.
  - The staged file is fsynced before every swap.
  - `tick_compaction_done` carries `merged`.
  - A compacted file left with only zero-byte fragments now resolves clean. It failed on `read_parquet([])` before.
  - `40c7475` also made BarBuilder drop previous-day ticks. The independent review showed that it judged by processing
    time, so a backlog past midnight (the 08-24 lag hit nine hours) would drop real ticks for good. `9fe13e1` removed it.
- **Checks before deploy:**
  - 8 new tests, each watched failing; 2 mutation checks caught (plain EXCEPT; appending whole fragments).
  - Full gate in the worktree: 4220 passed, 19 skipped.
  - The new `compact_ticks` on copies of 50 real stuck symbol-days: ok, every distinct row kept, 0 mismatches.
  - One independent review (opus): 6 minor findings, 5 fixed, 1 moot after the guard's removal.
- **Deploy:**
  - The working branch fast-forwarded `faaddcd` to `9fe13e1` at 16:58, before the stop: both changed modules are imported at
    boot, and a refused merge would not have stranded the engine down.
  - Stopped 16:58:36–16:58:50, started 16:59:50. Self-test ok (protected stores PASS, nothing frozen).
  - `startup_report` RECOMMEND, `engine_ready` 17:01:13.
- **Validation (post-arm lane pass, 17:01–17:08):**
  - `tick_compact` 10-26..10-30 caught up, `failed: []`. Run 10-26 merged all 300 symbol-days of 10-23 (3,032 fragments);
    run 10-30 merged all 301 of 10-30 (776 fragments).
  - No `ambiguous`, no `symbol_day_failed`, and no ERROR since boot.
  - Retention ran for the first time since 10-26: 6 tick partitions (09-24..10-01), 15,072 `corrections_log` rows.
    The oldest partition is now 10-05.
  - `job_runs` 10-26..10-30: success, `first_failed_at` cleared.
  - 10-23 and 10-30 are one file per symbol (318 and 312). 360ONE 10-23 holds 10,837 rows: 10,826 plus 11
    zero-volume echo copies, the duplicate of its last print kept once.
- **Host:** Modern Standby 15:32–16:24 froze the first gate run and the review agent (Kernel-Power 506/507). Both
  resumed. `keep_awake.ps1 -Hours 2` held 16:26–17:1x.
- **Docs:** COMMANDS gains "Tick compaction status".
- **Still to see:**
  - Tonight's 22:30 `tick_compact` (11-02) with ok=True.
  - The first Saturday-after-an-early-compaction run logs `merged` > 0 and stays ok.
- **Not done:** no `git push`. Echoes are still persisted (harmless: calibration reads 09:15–15:30 with `ask > bid > 0`).
  A previous-day echo arriving mid-session would roll BarBuilder's day back; never observed, left for the owner.

## 2026-10-30 16:58 — Confidence floor dropped from limits.yaml and re-signed; engine restarted and validated (owner: "Trade window has ended. You can run the script now and validate")

- **Before the stop:**
  - At 16:13 the compaction lane was mid-pass. It ended at 16:18:48 with the backlog `tick_compact` runs 10-26 to
    10-29 degraded, as at 10-29 22:04; they retry on their own.
  - The 16:12 catch-up had failed `bar_reconcile:2026-10-30` on a Kite connection drop after a standby gap.
  - Pre-sign check on a scratch copy: the patched YAML and `LimitTable` equal the signed file's apart from the key.
- **Apply:**
  - The engine stopped at 16:19:27 (`exit_clean`, no open positions, `state-20261030T161912.db` written).
  - `git apply` of the patch: 3 deletions. The file is LF, 9017 bytes, sha256 `18c063f2…`, the bytes the scratch
    check passed.
  - The reseed signed `limits.yaml` as `18c063f2…`; `envelope.yaml` is unchanged. `protected_config_status.py`
    exits 0.
  - The engine's own reader on a read-only state.db: both files verify, `analyst_confidence_min` is None, capital
    is 70000.
  - Committed alone as `f1a9e42`.
- **Boot:**
  - Started at 16:20:21. Self-test at 16:20:59: `protected_store:limits.yaml` and `:envelope.yaml` PASS, nothing
    frozen.
  - The boot catch-up re-ran `bar_reconcile:2026-10-30`, which passed this time. It took most of the 277 s boot.
  - `startup_report` reads RECOMMEND / NORMAL with integrity ok. `engine_ready` at 16:25:25, and no ERROR.
- **Checks:**
  - `GET /config/params` on the live engine: HTTP 200, 21 envelope parameters, no `analyst_confidence_min`.
  - The full gate on the live tree, whose conftest copies `config/`, so it ran on the key-less file: 4213 passed,
    19 skipped.
  - The next live session is 2026-11-02, so R is also the first session without the floor.
- **Docs:** the applied patch is deleted; `f1a9e42` is the record. COMMANDS loses the owner step, and its rollback
  note now reverts `f1a9e42`. IMPLEMENTATION_PLAN "Pending verdicts" needs one paper no-stop test split, at the registration date (2026-11-02).
- **Still to see:** the first gate line `analyst_confidence_min` n/a, at the 2026-11-02 session.
- **Not done:** no `git push`.

## 2026-10-30 09:09 — Review actions merged and deployed (owner: "Merge and deploy"; asked about timing at 08:57, the owner chose "Now, before 09:15")

- **Deploy:**
  - The engine stopped at 09:06:27, and `data\backups\pre_0018.db` was written.
  - The working branch fast-forwarded from `aab0133` to `1ca2dc7`.
  - The engine started at 09:07:42, so it was down for 75 s. The research snapshot was skipped to keep the stop
    short.
- **Boot:**
  - `migrations_applied_on_boot` lists 0018–0021.
  - Self-test ok: not killed, no login needed, nothing frozen.
  - Catch-up had no failures. `startup_report` reads RECOMMEND / NORMAL with integrity ok.
  - `engine_ready` at 09:07:33.
  - The ticker reconnected with 303 tokens, and the feed was HEALTHY at 09:07:34.
  - Since boot there is no `analyst_heartbeat` line and no ERROR.
- **Checks:**
  - `scripts\forward_verdicts.py --rule analyst` reads INSUFFICIENT, 0 of 30 cells, registered from 2026-11-02. R
    holds, because the deploy came before the 2026-11-02 open.
  - The dashboard was rebuilt (`npm run build`), and the bundle carries the new Performance card.
- **Still to see:**
  - The first window-open `brk20_sweep` line with `regime_up`. If the regime is down, `brk20_regime_blocked` rows
    should also appear.
  - Tonight's 18:40 `signal_outcomes_scored`.
  - The next weekly summary's analyst line.
  - The Performance card on the live dashboard.
- **Not done:**
  - The confidence-floor patch: optional, and the owner's to apply (COMMANDS "Protected config").
  - The market-pause study run, which needs a research snapshot at a later engine stop.
  - No `git push`.

## 2026-10-30 01:50 — Review actions built and validated on `wip/review-actions`: not merged, not deployed (owner: "Implement and validate the planned changes … You can start implementation right now, without waiting for market closure. Trade window has been closed.")

- **Branch:** `wip/review-actions` in the worktree `..\mt_review`, on the working branch at `aab0133` (a fast-forward). It covers all 21
  tasks of the review-actions plan.
- **How it was built:**
  - Ten file-locality packages. Each was built by an implementer agent and checked by a separate reviewer, with one
    or two fix rounds.
  - Three of the packages ran in a second worktree and were cherry-picked in with no conflicts.
  - A whole-branch review from three angles followed: line-by-line, removed behaviour, and a cross-file tracer. Its
    findings are fixed or listed under "Left open".
- **What changed:**
  - Migrations: 0018 (analyst decision journal), 0019 (prompt_version), 0020 (`signal_outcomes`,
    `brk20_regime_blocked`) and 0021 (notification buttons).
  - The nightly `signal_outcomes` job runs at 18:40.
  - The pre-registered verdicts: `engine.learning.forward_verdicts` and `scripts/forward_verdicts.py`.
  - One NIFTY 50 regime predicate. The brk20 regime filter applies at brk20 origination (`brk20.regime_filter: true`). The retest window
    is now y+1..y+5.
  - The heartbeat is retired in favour of a deterministic regime line, and `no_trade_today` is dropped.
  - Prompts now say "unavailable (<reason>)" when a source is absent.
  - trend is parked.
  - `analyst_confidence_min` is optional. The owner patch is prepared, not applied.
  - The card gains a cost line and a same-ticker history line.
  - Time exits skip positions sold outside the ledger.
  - One-tap skip buttons, with a 3-session reason window.
  - The scorecard gains a signals block, the dashboard a Performance card, and the weekly summary an analyst line
    (soft dependency).
  - A research trial ledger with the registration bar.
  - Scripts: the market-pause study (registered, not run) and the LLM scorecard.
- **Registered:**
  - IMPLEMENTATION_PLAN "Pending verdicts" holds the analyst-value verdict, the brk20 and ins kill rules and the market-pause study,
    registered 2026-10-29.
  - They were amended on 2026-10-30, before the registration date. The amendment covers renames, terminal rules for delisted and halted
    names, a failed-data guard, the scope of the prompt key, and the brk20 regime filter rollback.
  - The registration date = 2026-11-02 (`forward_verdicts.REGISTERED_FROM`). A deploy after 09:15 IST on 11-02 moves it.
- **Validation at the tip:**
  - Full gate: 4213 passed, 19 skipped. Offline tier: 430 passed, 1 skipped. Dashboard `npm test`: 95/95.
  - Replay smoke: 10/10 valid, 0 schema_invalid, intraday actions match live 3/3 (`mt_review\data\reports\replays\20261030T000235`).
  - The Performance card at 390 px on the fixture server; dom-checks clean.
- **Found during the build:**
  - A build agent wrote an empty untracked `tests/unit/test_prompt_integrity.py` into the LIVE tree at 23:09. It
    would have aborted the deploy's ff-merge. It is removed; live `git status` shows only `graphify-out/`.
  - A worktree replay needs the pinned CLI junctioned in. COMMANDS is updated.
  - The host sat in Modern Standby 17:48–21:21 with the keep-awake helper running (lid or power button), and the
    agents stalled.
- **Owner:**
  - Approve the merge and deploy per COMMANDS "Deploy a reviewed tip":
    - `git status` is clean;
    - back up `pre_0018.db`;
    - ff-merge;
    - rebuild the dashboard;
    - the boot log shows 0018–0021 and `engine_ready`.
  - Then, optionally, apply `runbooks/patches/limits-drop-analyst-confidence-min.patch` (COMMANDS "Protected config").
- **Left open:**
  - The brk20 regime filter labels on the planner and `/why` previews.
  - The regime line's store read on the event loop in position events.
  - A double-crossing brk20 re-offer is costed from the latest resting row.
  - The market-pause study run. It needs a fresh research snapshot, taken with the engine stopped.
- **Not touched:** mt-engine (running throughout) and both protected stores. The live tree was touched only to remove
  the stray file.

## 2026-10-29 15:40 — Implementation plan for the review's recommendations: plan only, nothing built or deployed (owner: "Based on the recommended changes of the report, Plan the entire change process …")

- **Plan:** 21 tasks in six phases.
  - It covers four migrations (0018–0021), one nightly hindsight job (`signal_outcomes`), pre-registered verdicts
    and kill rules, the brk20 regime filter, the heartbeat's retirement, absent-vs-empty markers, one-tap skip
    reasons, a dashboard card, a trial ledger, and two research scripts.
  - Every change to the analyst's prompt or to a verdict population lands before one registration date.
- **Read from the code; this changes the report's recommendations:**
  - The exit-rec noise was already fixed (2026-09-12). One gap remains: `time_exit_check` lacks
    the sold-outside screen.
  - The live brk20 retest window offers y+1..y+6, one session more than the registered five-session limit's y+1..y+5
    (`retest.py:182-183`, `:474-483`).
  - Owner-facing wording problems, none fixed yet:
    - catalyst, earnings and breakout prompt sections print "none" when their job never ran;
    - exit and adjust cards print "edge 0x";
    - re-tapping a reason on an expired rec overwrites it.
  - The `reply_keyboard` seam was never wired.
- **Review:** one read-only Opus builder check (owner: "Run it now", asked at 14:13 during market hours). It
  returned 21 findings, one of them a blocker: the analyst verdict had no fixed cohort. All were folded in except
  merging two of the tasks.
- **Owner:** the plan's open calls. Nothing is implemented until the owner approves the plan.
- **Not touched:** mt-engine (running), `src/`, `config/` and both protected stores.

## 2026-10-27 13:10 — TradingAgents review and system check: report only, nothing deployed (owner: "Review the provided trading agent branch … Check where we can improve our system or what we are currently doing wrong")

- **Report:** deleted when its work shipped. Evidence scripts are in
  `data/reports/tradingagents_review_2026-10-27/`; COMMANDS has a new "LLM analyst scorecard" entry.
- **Method:**
  - TauricResearch/TradingAgents v0.7.0 (e8f5503) was cloned read-only into the session scratchpad, and its core was
    read in full.
  - Our side: read-only queries of state.db (`mode=ro`), with forward returns from the 10-26 nightly export. The live
    DuckDB was never opened.
- **Agents** (owner: "Run agents now", asked at 12:21, inside the 10:30–14:30 trade window):
  - one Sonnet research pass covering the paper, independent evaluations and all 1,547 GitHub issues and PRs;
  - three read-only maps: the LLM layer, the decision pipeline, and learning plus data;
  - a state.db pull sent to `log-digger` came back BLOCKED. Its definition forbids DB access (memory
    `log-digger-no-db-access`), so those queries ran inline instead.
- **Headlines:**
  - TradingAgents is a research scaffold. It models no costs and has no risk limits or universe. Its paper's result
    (AAPL +26.6%, Sharpe 8.21, one quarter) has never been reproduced, and costed independent tests find no edge over
    buy-and-hold.
  - On our side, the intraday analyst's enter/decline showed no selection skill on 585 candidates. Within-day,
    approved minus declined was about 0 at T+5 and T+10, and −1.5 pp at T+20.
  - The gate's 0.55 confidence floor is never shown to the model.
  - For brk20, rsi2 and trend, the gate's edge check prices the LLM's own target, and the card shows the result as
    "edge Nx".
  - Live closed recs: 2 of 24 hit, median net −4.5%, median excess −2.8%, while the equal-weight benchmark fell
    11.8%.
  - brk20 and ins have no forward kill rule.
- **Not touched:** mt-engine (running), the code, `config/` and both protected stores.
- **Owner:** the report's owner decisions. None was applied.

## 2026-10-26 07:05 — Section-3 fixes merged (c887080) and deployed; boot verified (owner: "Merge the changes and deploy")

- **Before:** the clock read 06:57, the log tail held only news polling and health pulses, and the tree was clean.
  `protected_config_status` showed both stores OK. The branch has no migration, dependency or config change, and
  the editable install pointed at the live `src`. The engine was on the old code from a 06:50 restart; its token
  probe was rejected then.
- **Deploy:**
  - Stopped at 06:59:01; `exit_clean` at 06:59:06.
  - Merged `wip/section3` with `--no-ff`. That gives the merge commit c887080 and keeps the branch hashes cited
    below; the working branch had gained only the WORKLOG commit bfb17f1. The tree matches the reviewed branch plus the
    WORKLOG.
  - Dashboard: 92/92 tests, rebuilt; the new bundle carries the `rejection_storm` label. The old dist is in
    `data\backups\dashboard-dist-pre-section3-20261026`.
  - Started at 07:02:15.
- **Boot:**
  - `engine_boot` 07:02:24, `paper_composed`, `token_probe valid`.
  - Selftest: 13 PASS, including `native_preload` and both `protected_store` checks; `sdk_smoke` SKIP (deduped
    for the day).
  - `startup_complete` with needs_login=false and frozen=[]; `scheduler_started` and `engine_ready` at 07:02:50.
  - `boot_contract_ok` at elapsed 20.9 s. That is the new watchdog: the old one could log it only after 420 s.
  - `ticker_resumed tokens=302`, feed HEALTHY, 0 ERROR/CRITICAL lines.
  - The dashboard serves the new bundle, and `/paper` answers 401 without a token.
- **Cleanup:** the `..\mt_s3` worktree was removed (junctions unlinked first; the live venv and node_modules were
  verified intact), and the `wip/section3` branch deleted. Not pushed.

## 2026-10-26 05:10 — The plan's open fixes built on `wip/section3`, reviewed and validated; NOT merged (owner asked for them built, reviewed and validated)

- **Branch `wip/section3`** (worktree `..\mt_s3`): 11 commits on the working branch at 3f82c40 (8678428..193457f). Nothing is
  merged, pushed or deployed. It touches the paper OMS (`on_reject`), so it waits for owner review.
  - 8678428 DG4: a billing error publishes its tier change on `budget.state`.
  - deaa2dd, fc0b647 `daily_bars`: `BackfillReport.ok` is the watermark verdict. Retryable failures and an
    all-unmapped token map fail the day; one unmapped symbol does not.
  - aa62d90, fc0b647 boot watchdog: the arming contract is due 180 s after startup recovery returns, with a
    3,600 s startup ceiling. The old 420 s rule falsely paged on 09-15, 10-20 and 10-23. One page per phase,
    truthful texts, and an all-clear after a page.
  - 6f163ef: the mid-day token breaker ignores a superseded token (the 10-21 race, live path).
  - d2d8cd7, fc0b647 health: `feed_stopped_in_session` (armed, in session, 30 contiguous minutes), labelled
    during startup recovery.
  - c29ab67 paper rejection storm: ≥3 broker rejects in 60 s latch a daily FROZEN paper halt. The real path
    waits for live AUTO.
  - 196d4ec, d80a87c `scripts/cat_shadow_verdict.py`: the pre-registered `cat` v2 rule, with entry after the
    news, duplicate stories, renames and voids. `store.symbol_renames` is extracted, unchanged.
  - 434f14d: tests closing the plan's test gaps.
  - 193457f: IMPLEMENTATION_PLAN, RUNBOOK and COMMANDS.
- **Build:** five agents (2 Opus, 3 Sonnet), each in its own worktree, plus two fixes inline.
  - Audits found a UTF-8 BOM and fully mangled non-ASCII in two agent-written test files; both repaired.
  - The `cat` script went back twice: first for a copied cost function, then for the review's fixes.
  - The agent worktrees and branches are removed.
- **Review:** three independent reviewers covered the whole branch (line by line, cross-file, behaviour and text).
  - Fixed:
    - `cat` lookahead: a post-open age-0 story was entered at open(d), before the news.
    - duplicate stories;
    - renamed symbols;
    - the void window running past the hold;
    - an empty token map recording success;
    - the feed alarm during a slow startup;
    - the startup page text and the all-clear;
    - RUNBOOK gaps.
  - Recorded in IMPLEMENTATION_PLAN "Known defects and test gaps" instead: the 08:40 `token_check` superseded-token page, the DG4 latch's
    restart gap, and the `cat` scanner's double origination.
- **Validation:**
  - full gate 3,956 passed, 19 skipped (3 min 11 s);
  - changed offline tests 117 passed;
  - dashboard 92/92, both `tsc` clean.
  - One gate run under agent load failed `test_run_never_blocks_the_event_loop` (a 2.9 s loop stall). It
    passes alone and passed both later gates.
- **`cat` verdict smoke run** (live state.db read-only, plus a scratch copy of the 10-24 market export):
  INSUFFICIENT, 16 admitted signals.
  - 2 duplicates: TCS 08-31 repeats an 08-28 story; PERSISTENT 09-24 repeats 09-23.
  - 7 of 14 measured.
  - Interim only, decides nothing: T+20 median net −10.5% (n=7); excess vs EW −4.9%.
- **Not touched:** `mt-engine` (running, healthy), the live tree's code, `data/`, `config/limits.yaml`,
  `config/envelope.yaml`.
- **Host:** the keep-awake helper was refused by the auto-mode classifier, and the host slept ~03:42–04:18.
- **Owner:**
  - Merge and deploy, outside 09:15–15:30.
  - The `cat` verdict's review-driven clarifications (entry timing; duplicates, including against the
    excluded 08-18..28 span) were made after interim numbers had been seen: accept or amend them before the
    cohort forms.
  - The cost floor's equity basis: the smallest snapshot in the window is ₹19,154, from the 20k-base era, so
    the floor is 0.64%.

## 2026-10-26 01:15 — ICICIPRULI's history moved to ICICILIFE (stage 2, f7e5bb6); HEG left in place by design

- **Built:** `scripts/rekey_symbol.py` and `MarketStore.rekey_symbol`, plus rename-aware `isin_map`,
  `filings_pit` and `filings_pit_fresh`. Tests: 3910 passed, 19 skipped.
- **Rehearsed on a copy:** the 10-23 16:00 export plus a backup-API copy of state.db, pointed at with
  `MT_DATA_DIR`.
  - The dry run's counts matched the apply's, and a second run moved nothing.
  - Monday's universe built on the moved copy: ICICILIFE is included, F&O, core, and MIS-eligible (through
    the Zerodha file's ICICIPRULI key). Its liquidity median is ₹54.7 cr over the full history.
  - The alert read "the index list already names ICICILIFE".
- **Live run:**
  - Engine stopped 01:12:43 (exit_clean); `--apply` ran; engine started 01:13:44; `engine_ready` 01:14:50.
  - Moved, with nothing dropped: bars_1d 1068 (ICICILIFE now holds 1069, including its own 10-23 bar),
    bars_1m 11134, feature_snapshots 8577, features_daily 31, sentiment 25, earnings 18, catalyst 14,
    news 13, results 12, sector 9, corp actions 6, aliases 2, insider 2, isin 1, backfill checkpoint 1.
  - Left as records: instruments_daily 74, universe_daily 35, reconcile_log 30, corrections_log 71.
  - Undo copy: `data\backups\rekey-ICICIPRULI-ICICILIFE-20261026T011324`.
- **Boot:** `startup_report` shows risk NORMAL, integrity ok, not killed, no failed jobs. The warm-up reports
  `rsi2/trend/mom:ICICIPRULI daily bars 0/200` (daily class, which does not freeze), because the 10-23
  universe still lists ICICIPRULI. The 08:30 build switches the set to ICICILIFE, which covers all
  200 sessions (measured on the copy with the engine's own session enumerator).
- **HEG→HEGAM was not moved (departs from the spec's done-when):**
  - 10 sessions, 2026-09-07 to 09-21, have no bar under either symbol: the suspension around the demerger
    (ex 9-07).
  - Moving HEG's 1037 bars would turn HEGAM, now a quiet young listing, into a 200-session series with
    a hole. The warm-up would refuse it until mid-2027.
  - hi52 vetoes HEGAM across the demerger either way.
  - The script now refuses any rename with such a gap (`rename_trading_gap`).
- **Found and fixed while validating:**
  - NSE switched both index lists to ICICILIFE over the weekend, so the alert, which fired only on a
    remap, would never have fired. It now fires on the first build that carries the new symbol where
    the previous build carried the old one, and says what the history needs.
  - `filings_pit` filtered on the lagging CSV, so ICICI Life's NSE filings since 10-23 were dropped as
    out-of-universe. Its 7-day retry margin re-lists them on the next run.
  - `isin_map` kept re-stamping ICICIPRULI.
  - The Zerodha MIS file still named ICICIPRULI at 00:59.
- **Check after the 08:30 build:**
  - `universe_built` has ICICILIFE eligible.
  - One "Symbol renamed: ICICIPRULI → ICICILIFE" alert arrives.
  - `warmup_not_ready` no longer names ICICIPRULI.
