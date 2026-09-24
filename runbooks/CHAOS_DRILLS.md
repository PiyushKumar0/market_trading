# Chaos drills (plan §9.4, §9.5, §10.6)

This file holds three things:
1. the automated chaos suite (`tests/chaos/`): what each §9.4 case asserts today;
2. the live drills run against the real engine, with the soak schedule;
3. the evidence log: every drill run, pass or fail.

The stop/start set (cases 15–19) is G3 gate evidence (§8.5 item 4). Record every live run in the
evidence log below.

## 1. Automated suite

```powershell
.venv\Scripts\python.exe -m pytest tests\chaos -m chaos -q -rs -rx    # ~90 s
```

Run it weekly and before every phase gate (§9.6). Every file under `tests/chaos` gets the `chaos`
marker automatically. The flags list skip reasons (`-rs`) and pinned defects (`-rx`).

**How to read the results.**
- **Passing tests** compose the real components the way `engine/ops/main.py` wires them, over a
  temp database and a movable clock. Only the external edge is faked: Kite, the Claude SDK, feed
  HTTP, Telegram, and NTP.
- **Skips** name what they wait for. `PHASE3_GATED` means the clause needs WO-P3-5/P3-6 components
  (ProtectionManager, GTTManager, Reconciler, SquareOffScheduler, AUTO routing). Other skips mean
  the behaviour is not built yet.
- **`xfail(strict=True)`** marks a real defect (the CD-n list in §2). The test asserts the plan's
  behaviour, so fixing the defect makes it XPASS, which fails the run until the marker is removed.

Status as of 2026-09-24: 64 passed, 21 skipped, 0 xfailed (every pinned defect, CD-1 to CD-7, fixed).

| # | Case | File | Asserted today | Skipped / pending | Defects |
|---|---|---|---|---|---|
| 1, 2a, 2b, 7–12 | Positions, protection, circuit, broker outage, GTT, RMS | `test_phase3_gated_cases.py` | — | all Phase-3-gated | — |
| 3 | WebSocket drop | `test_case03_websocket_drop.py` | 10 s silence → STALE + kill/respawn + in-session `feed_stale` FROZEN cause, paged by its two risk-state changes; a slow page never stalls the feed; entries refused from 5 s tick age; resume only when HEALTHY and fresh | positions broker-protected (P3) | CD-1 (fixed) |
| 4 | Token expiry mid-day | `test_case04_token_expiry.py` | FROZEN `kite_token_rejected` + one page with login link; opening order refused before broker; re-login clears | protection resting (P3) | — |
| 5 | LLM timeout / garbage / quota | `test_case05_llm_failure.py` | no proposal + alert per failure mode; DG4 same path; deterministic time-stop exit still delivered | square-off unaffected (P3) | — |
| 6 | Rejection storm / 429s | `test_case06_broker_rejection_storm.py` | 429s paced, never amplified; entry budget exhausted → risk-reducing lane still reaches broker | ≥3 rejects/60 s ⇒ FROZEN (unbuilt: `CAUSE_REJECTION_STORM` is never set); retry/backoff (unbuilt, WO-P3-5) | — |
| 13 | Holiday start | `test_case13_holiday_start.py` | no session, no LLM calls, jobs skip and are not replayed, heartbeat-only health | — | — |
| 14 | Clock skew > 2 s | `test_case14_clock_skew.py` | boot skew → FROZEN (held through the warm-up lift) + gate `clock_skew` refusal; resync + restart reopens | mid-session skew detection (skew is boot-only, accepted for Phase 2) | CD-2 (fixed) |
| 15 | Clean stop + same-day restart | `test_case15_clean_stop_same_day_restart.py` | repeat stop signals ignored; STOPPED + `last_clean_stop_at` committed; one ENGINE_STOPPED; restart not crash-recovered; catch-up exact | restart reconcile (P3) | live drill failed 09-23, see §3 |
| 16 | Offline across EOD | `test_case16_offline_across_eod_window.py` | each missed EOD job caught up once, idempotently, incl. the EOD safety-critical earnings run | champ/chall eval (unbuilt) | — |
| 17 | Offline multi-day | `test_case17_offline_multi_day.py` | per-trading-day re-run (weekend + holiday skipped); candle gap backfilled; warm-up enforced | ex-date GTT repair (P3) | — |
| 18 | Cold start too close to window | `test_case18_cold_start_warmup.py` | regime shortfall freezes until candles land; intraday-only shortfall refuses per candidate; daily-only per symbol | — | — |
| 19 | Scheduled start missed | `test_case19_scheduled_start_missed.py` | one SCHEDULED_START_MISSED after grace, edge-triggered; none on weekend/holiday | open MIS rides backstop (P3) | — |
| 20 | News down / DG3 pre-open | `test_case20_news_layer_down.py` | empty-but-fresh digest rung; digest failure, or no digest yet today at an in-window sweep → one CATALYST_DISABLED a day; no FROZEN; non-`cat` ranking identical; self-restores next day | — | CD-4 (fixed) |
| 21 | Prompt-injection headlines (entry side) | `test_case21_adversarial_headlines.py` | schema-invalid scores dropped; single-source ⇒ `context` at most; syndicated PR = one cluster; other symbols unaffected, incl. model-emitted foreign entity strings | exit side (P3) | — |
| 22 | Lifecycle notifications | `test_case22_lifecycle_notifications.py` | (a) clean stop: one ENGINE_STOPPED, watchdog silent; (b) crash: one ENGINE_DOWN, crash-recovered restart; (c) killed while off: silent | capital protection (P3) | — |
| 23 | Shutdown races tick flush | `test_case23_shutdown_races_tick_flush.py` | no deadlock; bus drained before store close; a late or wedged flush restages its unwritten batch with an explicit log | — | CD-6, CD-7 (fixed) |

Note on the plan text: `engine_lifecycle` has no `intend_to_run` column. The watchdog and the
tests read `state` instead: RUNNING or STOPPING means "intends to run", STOPPED means an
intentional off.

## 2. Defects found by the suite

A new defect is pinned by an `xfail(strict=True)` test and listed here as open. None is open.

Fixed (2026-09-24):
- **CD-1** — feed staleness never latched a FROZEN cause or paged anyone. `engine.risk.causes.feed_health_to_latch`
  (subscribed in `main.py`) now sets `feed_stale` on the supervisor's STALE transition in market hours
  and clears it on the next HEALTHY. The owner hears it from the two risk-state changes, which carry
  the cause; there is no separate feed page. The latch update runs as its own task, because
  publishing a risk-state change waits on the Telegram send (up to 95 s) and must not hold up the
  feed's health publisher. Out of hours nothing latches: the tick-age rule already refuses entries,
  and a laptop waking overnight must not page. The 60 s health poll almost never saw STALE, because
  the respawn moves the state on within the same cycle. `limits.feed_heartbeat_silence_s` is still
  enforced through `ticker.heartbeat_silence_kill_s`; the case-3 rig asserts the two are equal.
- **CD-2** — the warm-up lift cleared a boot clock-skew freeze. `SessionLifecycle` keeps the boot's
  skew verdict and the lift holds on it; only a resync plus restart reopens entries (Phase-2 design).
- **CD-4** — a missing digest that did not raise sent no CATALYST_DISABLED. `cat` reads only
  today's watchlist, so after each in-window sweep `main.py` checks that a digest has run today
  (every run stamps `sentiment_agg`); a late run yesterday still reads "fresh" by age, so the check
  is on the date, not `digest_status`. `_CatalystDisabledAlert` sends at most one a day per process,
  shared with the digest-failure path; a restart on such a day can send one more.
- **CD-6** — teardown closed the store with bus deliveries still running. `EventBus` tracks what
  `publish` schedules, and `main.py` awaits `bus.drain(_SHUTDOWN_BUS_DRAIN_S = 5 s)` before
  `store.close()`. Measured stops take 13–20 s (the backup alone 3–11 s). A wedged flush, which the
  store waits up to 15 s on, can push a stop past NSSM's 30 s grace; the kill then leaves a WAL for
  the next boot to replay.
- **CD-7** — a flush still writing when `close()` gave up raised and lost its batch. It now restages
  the partitions it had not written, with `tick_flush_skipped_store_closed`.
- **CD-3** — a missed EOD `earnings_calendar` run was never caught up or flagged: catch-up only
  looked at *today's* fire-time. `CatchUpRunner._governing_day` now resolves the run the next
  entries read (the previous evening's for an EOD job); a pre-open job's run stays with the scheduler.
- **CD-5** — model-emitted entity strings were resolved with no check they appear in the headline.
  `NewsScoringJob._write_back` now drops strings not found whole-word (HTML-unescaped) in the
  headline the model was shown. Measured on 26,133 live entities: 99.2% verbatim; the dropped rest
  are mostly macro terms, plus entities attached to the wrong cluster of a batch.

Other gaps the suite surfaced (not pinned as defects):
- **No budget-tier message on billing-error DG4.** A billing-error DG4 is never published on
  `budget.state`: `harness.py:843` calls `note_billing_error`, and `raise_billing_error` has no
  caller.
- **Distinct PRs merged into one cluster.** The headline clusterer merged three different
  companies' PRs that shared a template into one cluster (template glue; the boilerplate list only
  covers earnings templates).
- **Dropped scores leave no record.** Per-cluster score drops write no audit row:
  `parse_cluster_scores` says it logs the drop but does not.

## 3. Live drills

### Phase 2 (RECOMMEND, no platform positions): drillable now

**Case 15 / 22a: planned stop and restart.** Run outside 09:15–15:30, when no job is in flight;
check the `engine.log` tail first.
```powershell
Get-Date; Stop-Service mt-engine            # or scripts\nssm_install.ps1 -Action stop
Select-String data\logs\engine.log -Pattern 'stop_requested|stop_signal_repeat_ignored|stop_forced|"event": "shutdown"' | Select-Object -Last 5
.venv\Scripts\python.exe -c "import sqlite3;c=sqlite3.connect('file:data/state.db?mode=ro',uri=True);print(c.execute('select state,last_clean_stop_at from engine_lifecycle').fetchone())"
Start-Service mt-engine
```
**Pass** requires all of the following:
- state = STOPPED, with `last_clean_stop_at` at the stop time;
- no `stop_forced` line;
- exactly one ENGINE_STOPPED on Telegram;
- after restart, STARTUP_REPORT has `crash_recovered: false`.

**Cases 16 / 17: offline across EOD / multi-day.** These happen naturally whenever the engine is
off overnight or over a weekend. On the next boot, check that STARTUP_REPORT `jobs_caught_up`
lists one entry per missed trading day.

**Case 22b: crash while running.** `Stop-Process -Id <engine pid> -Force`. NSSM restarts it after
15 s. **Pass** requires STARTUP_REPORT `crash_recovered: true` and exactly one ENGINE_DOWN, and the
ENGINE_DOWN half needs the watchdog scheduled task from `scripts/schedule_tasks.ps1`. Owner-run.

**Case 19** is not drillable until scheduled starts (`scripts/schedule_tasks.ps1`) are in use.

**Cases 3 and 4** can be drilled in market hours at a quiet moment:
- case 3: Wi-Fi off for about 30 s;
- case 4: let the daily token expire before login.

Both are optional in Phase 2, because the automated suite covers them and no positions are at
risk.

### Phase 3 soak schedule (§10.6: 30-session paper-AUTO soak)

| Soak week | Cases |
|---|---|
| 1 | 1, 2a, 2b |
| 2 | 3, 4 |
| 3 | 5, 6 |
| 4 | 11, 12 |
| 5 | 13, 14 |
| 6 | 15, 16, 17, 18, 19 (stop/start set, G3-blocking) + the dead-platform drill (§10.6 card) |
| Quarterly after G3 | dead-platform drill + cases 1, 3, 11, 15, 17 |

## 4. Evidence log

Add a row for every live drill, including failures.

| Date (IST) | Case | Procedure | Result | Evidence |
|---|---|---|---|---|
| 2026-09-23 21:29 | 15 | `Stop-Service mt-engine` on commit f4667f3 (repeat-signal grace) | **FAIL**: state stuck at STOPPING, `last_clean_stop_at` still 2026-09-02 | `stop_requested` signum 2 at 21:29:46.83. `stop_signal_repeat_ignored` signum 21 (SIGBREAK) at +1.33 s. No `shutdown` line after it: the process was terminated. Cause: NSSM `AppStopMethodConsole` = 1500 ms (the default). After 1.5 s NSSM closes the console window, CTRL_CLOSE_EVENT follows, and Windows ends the process mid shutdown-backup. Fix: `nssm set mt-engine AppStopMethodConsole 30000` from an elevated shell (owner). `nssm_install.ps1` now sets it. Re-drill after it is applied. |
| 2026-09-24 03:39 | 15 | `Stop-Service mt-engine` + `Start-Service` (deploy of 47af14c), NSSM `AppStopMethodConsole` = 30000 in force since the 03:04 service start | **PASS** | `stop_requested` 03:39:53.80, `backup_written` + `shutdown` 03:39:59.64 (≈6 s, beyond the old 1.5 s window), no repeat signal, no `stop_forced`. `engine_lifecycle` STOPPED, `last_clean_stop_at` 2026-09-24T03:39:59 (first since 09-02). Restart: STARTUP_REPORT `crash_recovered: false`, NORMAL, `engine_ready` 03:40:39. Note: the owner's 03:03 stop was still cut at +1.31 s — NSSM reads its stop settings at service start. |
