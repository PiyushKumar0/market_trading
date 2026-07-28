# WORKLOG — autonomous operations log

## 2026-07-28 (overnight — PHASE 2 IMPLEMENTED on branch `phase2`, ~30 commits, 567→1149 tests)

- **Owner directed "move ahead with the next phase" (2026-07-27 14:24). Phase 2 (RECOMMEND live,
  §8.3) is now code-complete on `phase2`**: contracts moved to `engine/core/contracts.py` (R1
  import-graph), BudgetGovernor (D6 ladder + D4 pricing), LimitsEngine (hash-verified §7.1 reader),
  ExposureTracker (equity/day counters/floor ladder), CatalystDigestJob (§2.7 step 5),
  order-surface guard on KiteClient (B7), RiskGate + GateContextBuilder (full §7.1 table,
  monotone actions, shrink loop, news-free ctx), AgentHarness (single SDK call site, allowlist
  enforcement, D7 ladder, agent_calls audit), ContextAssembler + intraday/preopen/news agent defs,
  features v2, Telegram command surface + RiskStateLatch cause ledger, API routes + WS hub +
  React dashboard v1 (dashboard/dist, `npm run build`), RecommendationPipeline + Book
  (deliver//taken//closed/veto/expiry→no_action, stop-proximity + max_holding events),
  NewsScoringJob (fan-out purity), PreopenPlannerJob (day_plans), nightly reviewer v1,
  LiveScanContextProvider + live SignalPreScreen wiring, full composition-root wiring incl.
  equity minute-tick + floor-ladder application through the latch, sdk_smoke selftest (D11,
  deduped/day), migrations 0003+0004. Engine NOT restarted — live capture session #3 ran
  untouched; deploying `phase2` is an owner decision.
- **Incident during validation: full-suite pytest wedged twice (idle-await, 0% CPU).** Root cause:
  `test_order_surface.py` built `RateLimiter(clock)` on the FROZEN conftest clock — the token
  bucket refills off `clock.now()`, so the third order call in one test waited forever on a refill
  that never came (also why that subagent never returned its report). Fix: `burst=100` per the
  established `test_rate_limiter.py` frozen-clock idiom; the wedged runs also explain the two
  lost wakeups (machine suspend gaps 15:18→22:59).
- **Deviations/decisions logged**: nightly reviewer v1 single-shot (plan §5.5 note); digest
  `invalidation=prior_close` + collapsed bands (plan §3.2.4 note); contracts location (plan §3.3
  note); `positions.realized_pnl` is GROSS with costs separate (ExposureTracker convention — must
  hold for the Phase-3 OMS writer); Phase-2 `max_new_trades_day` counts entry RECOMMENDATIONS.
- **Known Phase-2 gaps (deliberate, tracked)**: `nifty50_fn`/`expiry_day_fn` unwired ⇒ the
  expiry-day NIFTY50-MIS `no_trade_windows` leg is inert until Phase 3 (harmless for a 10:00–10:30
  window); clock-skew gate verdict is boot-scoped; `mom` treats every day as rebalance-due until
  ledger-driven state lands (bounded by dedupe + forward caps + analyst veto); B4 flagged a
  potential look-ahead if `daily_snapshot` is re-run for PAST days once sentiment history
  accumulates (Phase-5 replay must add a day-scoped sentiment read); catalog lacks dedicated
  AGENT_FAILED/OWNER_APPROVAL kinds (LIMIT_BREACH reused); REC_FILL_SUSPECTED auto-match needs the
  Phase-3 reconciler (manual /taken until then). G2 operational evidence (4 weeks of recs, ≥5
  owner-executed, weekly watchlist reviews) starts accruing once the owner deploys + enables
  RECOMMEND. **Push to origin awaits owner approval (phase-end rule).**

- **C2 contract-note verification PASSED** (owner-supplied real Zerodha note, 4 BSE CNC trades,
  ₹8,402 turnover, ₹8.38 charges): brokerage 0 ✓, SEBI exact ✓, GST exact ✓, stamp ✓ (rupee
  rounding), STT ✓ after two real-world rules — per-scrip whole-rupee rounding and ETF buy-side
  exemption (SILVERBEES; no ETFs in platform universe); txn charge delta = BSE vs the model's
  pinned NSE rate (platform routes NSE). DP correctly absent (ledger-side at settlement).
  Cost model verified against actual billing; no costs.yaml change needed. G1 item CLOSED.

- **Q15 candle-latency measured live** (11:03–11:22 IST, 60 samples, 3 symbols): official 1m
  candles available **p50 0.4s / p90 4.1s / p99 8.2s** after minute close →
  `data/reports/q15_latency.json`. The §14 Q15 assumption (gap-backfill/warm-up can rely on
  prompt official candles) holds with wide margin. G1 item CLOSED. (First attempt failed on a
  momentary DNS blip — the live feed rode through it untouched; only fresh connections failed.)
- Session #3 capturing since ~10:18 (13–14k ticks/window, zero drops, keep-awake engaged).
  Remaining G1: ticker/news session counters (auto-accruing), owner contract-note check (C2),
  owner phase-end push approval. Watch-items: TITAN 0/x intraday warmup line (new-to-watchlist?),
  GROWW young-classifier quirk resurfaced — both entries-only, check at next store window.

## 2026-07-23 (day — FIRST LIVE CAPTURE + sleep-wedge fix `1aa2744`)

- **FIRST LIVE SESSION IN PLATFORM HISTORY**: WS connected on first attempt post-`9fc20dc`
  (api_key fix); 09:15→12:18 flawless — ~15–16k ticks/5min, 260 bars/5min (52 syms × 5), zero
  drops. G1 ticker session #1 (partial).
- **13:41 incident: PC slept mid-session; feed wedged after resume.** First sleep: heartbeat-kill
  respawned correctly. Second sleep landed mid-WARMING, where the stale-kill is suppressed and no
  timeout existed → wedged forever (ticks=0, 12:44→13:41+). Fix `1aa2744`: bounded WARMING
  (warming_timeout_s=60, capped backoff, one-shot FEED_WEDGED escalation, counters reset on
  HEALTHY) = generic sleep/resume recovery; + KeepAwake (ES_SYSTEM_REQUIRED during NSE sessions,
  health-loop-driven, opt-out knob). 566 tests green. Owner also advised: AC power plan should not
  sleep during market hours (G0 power checklist).

## 2026-07-23 (overnight — TWO root causes closed: DuckDB FATAL + the zero-ticks-ever mystery)

- **01:04 DuckDB FATAL** (`Failed to delete all rows from index` → connection invalidated for the
  rest of the boot): triggered by the filings_pit_fresh catch-up re-upserting identical
  content-hash rows via ON CONFLICT DO UPDATE. Fix `d00526c`: `_CONTENT_HASH_PK_TABLES` route to
  DO NOTHING (identical id ⇒ identical row; never touches the index delete path). Store file
  intact (FATAL protects on-disk state; verified open+counts after restart).
- **ZERO-TICKS ROOT CAUSE FOUND AND FIXED (`9fc20dc`)** — the stderr drain surfaced it live:
  every KiteTicker WS upgrade 400-BadRequests (`kws.reconnecting` forever). Cause:
  `TickerSupervisor` constructed WITHOUT `api_key` → silent `""` default → child dialed
  `wss://ws.kite.trade?api_key=` since the first boot. REST unaffected (KiteConnect binds its own
  key) — why every other credential check passed. Fix: `SessionManager.api_key()` wired through;
  `start()` now REFUSES loud on empty api_key. Whole causal chain of the live-capture failure is
  now closed: empty api_key → WS 400 → no ticks → heartbeat-HEALTHY mask → zero self bars.
- Also seen working in the owner's 01:31 log: post-login recovery clean, instruments persist
  113,636 rows without fault (d00526c verified live). GROWW blocker persists (170/200, the 07-20
  hole) — forensics still open. **Operator: restart onto `9fc20dc` BEFORE 09:15.**

## 2026-07-23 (pre-market — tickless-HEALTHY defect fixed, `30ca64f`)

- **07-22 session verdict**: whole fix chain proved live (token probe → login link → post-login
  ticker resume 10:50, HEALTHY through close, clean respawn 11:37, clean stop) EXCEPT zero
  self-built bars again (reconcile 0 compared / 18,750 offline). Root cause of the silence:
  **child stderr piped but never drained** — every KiteTicker diagnostic discarded; heartbeats
  (same socket/framing as ticks ⇒ downstream chain proven by an end-to-end test) kept HEALTHY
  with no ticks. Fix: stderr/stdout drained into engine.log (`ticker_child_output`); in-session
  tick-silence ⇒ DEGRADED + Telegram within ~120 s (calendar-aware, off-hours unchanged);
  `feed_stats` every 5 min (ticks/drops/bars). The actual tick-outage trigger becomes visible
  next session. Also fixed my own miss: settings test not updated for the gdelt 900→1800 change.
  554 tests green. **Operator: restart onto `30ca64f` before 09:15.**

## 2026-07-21 (evening boot ~20:03 — EOD catch-up verdict + one open diagnostic)

- **Owner-reported 404/503s: benign, E5 held.** NSE-only: corp_actions 404 (recovered within the
  run — present in caught_up), deals bulk/block 503 (NSE evening maintenance; date-keyed →
  auto-retry next boot). BSE fully healthy: filings_pit_fresh wrote 18 fresh insider rows for
  07-21 (cap-subdivision fired correctly), filings_shp 191 rows/47 syms, filings_results 686
  events, bhavcopy 2,390 symbols 0 mismatches. Catch-up completed the 07-21 EOD set.
- **OPEN DIAGNOSTIC — GROWW warm-up blocker (168/200, young_excluded empty):** NOT the young-rule
  evening race first hypothesized (the window already excludes today). GROWW's 07-20 daily bar is
  missing after both bhavcopy and the daily_bars catch-up ran — genuine no-trade day, suspension,
  or a symbol-level fetch gap. Store forensics at next off-window (check bars_1d GROWW 07-18..21 +
  whether GROWW traded on 07-20). Impact tonight: none (market closed). Impact tomorrow: NONE for
  live capture — FROZEN blocks entries only; ticker/data capture run regardless (and mode is OFF).
  If the gap is real+chronic, the design question is whether one symbol's daily gap should freeze
  globally vs exclude-and-report (plan §7.1 wording is global-conservative) — decide with data.

## 2026-07-21 (end-of-day validation — run SUCCESSFUL, one milestone deferred)

- **Owner stopped the engine 16:02 IST; end-of-day validation against expectations:**
  ✓ instruments_daily complete + healed (07-21: 112,997 rows / 233 indices; 07-20 also complete);
  ✓ regime dailies current through 07-21 (NIFTY 50 + INDIA VIX, 997 rows each);
  ✓ full session minute bars present (18,750 = 50×375, honestly src='kite_official'; reconcile
  correctly excluded all as offline, 0 drift / 0 compared);
  ✓ **post-login recovery hook PROVEN LIVE** (`ticker_resumed tokens=52` at 15:34:00, feed HEALTHY
  in 3 s); ✓ graceful-stop fix proven (shutdown guards → backup → exit_clean, state STOPPED);
  ✓ morning jobs success; EOD jobs (18:00+) hadn't fired by the 16:02 stop → catch-up next boot
  (by design). ✗ LIVE tick capture still 0/5 G1 sessions — timing, not defect: the ticker was only
  up 00:14–00:40 and 15:34–16:02, never during 09:15–15:30 (incident chain owned the session).
- **Tomorrow needs nothing special**: engine up before 09:15 (any order vs login — the hook covers
  both), leave it through 15:30+ → first genuine live-capture session.

## 2026-07-21 (late afternoon — torn-snapshot HEAL confirmed live + persist made ~1,300× faster)

- **Heal verified in production (15:01:25 IST):** the degraded-snapshot escalation fired on the
  owner's 14:49 restart — hydrated the torn day (expected one-last-time warning burst), detected
  `indices=0`, live-refreshed, and persisted 112,997 rows with all 233 indices. Immediately after:
  regime backfill `NIFTY 50`+`INDIA VIX` `failed=0` (first success all day) and warm-up gap filled
  17,350 bars `failed=0`. Incident chain closed; warnings will not recur.
- **Fourth finding: the persist itself took ~11.5 min holding the store writer lock.**
  `_upsert_rows` ran DuckDB `executemany` at ~76–128 rows/s; instruments_daily is ~113k rows/day —
  so the 08:15 daily job AND any heal starved every store consumer for ~12–25 min (this is also
  what the 13:33 post-login backfill silently queued behind before the 13:34 hang). **Fix:** batches
  ≥2,000 rows route through `_bulk_write`'s registered-view `INSERT…SELECT` (dtype=object frame,
  Decimals stringified + cast per value against the real column type, pk last-wins dedupe, single
  statement = torn-write-safe). Benchmarked on a tmp store: **113k rows in 1.11 s** (~102k rows/s)
  vs 24.8 min extrapolated for executemany. 546 tests green (fidelity: sub-paisa Decimals, NULL
  index rows, int/bool round-trip, update path, mid-batch-abort atomicity).
- Note: the vectorize workflow's 4 subagents died on the session usage limit (resets 17:30 IST);
  implemented inline per the delegation-unavailable rule.

## 2026-07-21 (afternoon — stop-hang zombie + hydrate data-loss pair, diagnosed + FIXED)

- **Incident 3 (13:34 IST): Ctrl-C logged `engine_interrupted` but the process never exited** —
  a zombie holding `engine.lock` + heartbeat (had to taskkill 46332). Two causes: (a) signal
  handlers were installed only AFTER `lifecycle.startup()` — the entire boot had no graceful stop,
  so Ctrl-C mid-startup raised a raw KeyboardInterrupt with no teardown; (b) interpreter exit joins
  wedged executor workers forever (`concurrent.futures` atexit). **Fix:** handlers install right
  after the lock acquire — FIRST Ctrl-C = graceful stop (honoured even mid-boot), SECOND = forced
  `os._exit(130)` (state stays RUNNING → next boot crash-recovers, by design); `main()` now ends in
  a `_hard_exit` backstop that logs lingering non-daemon threads then `os._exit`s — a wedged worker
  can never zombify the engine again (subprocess-proven in tests).
- **Incident 4 (13:57 IST): 8,072 `hydrate_row_skipped` warnings + `indices=0` + regime backfill
  `unknown_token`.** THREE stacked causes, the third found only by adversarial review + DB
  forensics after the first implementation pass mis-attributed it: (1) `instruments_daily.tick_size
  DECIMAL(10,2)` truncated sub-paisa CDS ticks (0.0025→0.00) → gt=0 rejection on hydrate — column
  widened to DECIMAL(18,6) + one-shot idempotent migration; (2) **the 07-21 snapshot was TORN**: the
  10:32 instance was taskkilled mid-upsert during the double-run window, and `snapshot_rows` appends
  the 233 index rows LAST — exactly the regime tokens were lost (10:40 boot even hydrated a
  half-written 52,854-row snapshot, on record); `_upsert_rows` is now wrapped in one explicit
  transaction (all-or-nothing; a kill leaves the PRIOR complete day); (3) the F2 ladder blindly
  trusted `MAX(d)` — it now detects a DEGRADED snapshot (`index_count==0`; a real dump always has
  indices) and escalates to live refresh + persist when a session exists, healing the stored day in
  place. **No manual data repair needed: the first post-fix boot self-heals the torn 07-21 day.**
  544 tests green.

## 2026-07-21 (morning login lockout — fourth cold-start-family defect, diagnosed + FIXED)

- **Incident (owner report ~10:45 IST): browser could not reach `localhost:8400` for the daily
  login; engine "running" but not taking the key.** Diagnosis from engine.log + process/port scan:
  (1) yesterday's expired token passed the self-test — `token_valid()` is behavioural (present ∧
  not-yet-rejected), no live call had failed at check time — so `needs_login=false`, NO login link
  sent, and startup marched into the warm-up backfill where **all 50 symbols failed TokenException**
  (`token_rejected` count 0: KiteClient never notified the session). (2) The uvicorn server hosting
  the ONLY browser login route (`/kite/callback`, :8400) binds AFTER `lifecycle.startup()` — startup
  wedged, port never bound. (3) TWO engine instances were running concurrently (10:35 + 10:40
  starts) contending on state.db/Telegram; killed both, port freed, owner restarted + logged in.
- **Fix shipped (this commit), three parts + review hardening:** (a) `SessionManager.verify_token()`
  — live `kc.profile()` probe at every boot; a rejected/absent token now sends the login link
  IMMEDIATELY and the broker-touching recovery no-ops until login (PostLoginRecovery re-runs it).
  (b) Login callback binds BEFORE startup recovery; `_serve_api` binds the socket itself
  (uvicorn's own bind paths `sys.exit(1)` — adversarial review caught that escaping the serve task
  and killing the loop; reproduced end-to-end), `SO_EXCLUSIVEADDRUSE` on Windows, returns None +
  critical alert on failure, engine continues (Telegram `/token` fallback). (c) TokenException
  circuit breaker: KiteClient → `session.on_token_rejected` (idempotent) → freeze + critical alert
  + fresh login link (the invalidation seam existed but was NEVER wired); backfill/warmup/reconcile
  loops abort remaining symbols on the first TokenException instead of grinding. 529 tests green
  (43 new/extended incl. the occupied-port case).
- **Follow-up CLOSED same day (second commit): OS-level single-instance lock.** Root cause of the
  double-run: the §2.6 step-0 guard is a check-then-act on the `engine_lifecycle` DB row whose
  RUNNING commit lands only deep into boot — a boot wedged before it (the 10:35 one) is invisible,
  and two simultaneous boots can both pass the read (TOCTOU). Fix: `InstanceLock` — an exclusive
  kernel file lock on `<data_dir>/engine.lock` acquired at the TOP of `run()`, before sqlite/DuckDB/
  Telegram/:8400; a second instance refuses with **exit 3** + `single_instance_refused_lock` (holder
  pid named). Kernel releases on ANY process death (crash/taskkill) — no stale-lock reaping; the DB
  guard stays as secondary crash-detection. Windows subtlety (empirically verified): the locked
  byte 0 is MANDATORY-unreadable to other handles, so the pid record lives at offset 1. Wedge
  choreography: wedged holder → new starts cleanly refused (NSSM retries throttled ~15s, log-only)
  → watchdog force-kills the wedge → kernel frees the lock → next start acquires. 534 tests green
  (5 new incl. a subprocess kernel-release proof).

## 2026-07-20 (evening — owner stopped the engine ~21:28 IST; date corrected from a mislabeled 07-21 entry)

- **Deferred G1 store checks ran** (engine-off window): `instruments_daily` EMPTY confirmed (0 rows
  ever). **NEW FINDING — zero LIVE minute bars have ever been captured**: bars_1m max = 2026-07-16
  (backfill), sessions 07-17/07-20 captured nothing despite logins; NIFTY 50/INDIA VIX dailies
  stale since 07-10. reconcile_log has ONE day (07-16, vs backfilled bars). News corpus: 369
  headlines / 343 clusters (span to 2026-07-20). CORRECTION to 07-20 entry: the G1 ticker-session
  counter is at ZERO, not 1.
- **Third cold-start-family defect: NO post-login hook exists** (kite_callback only stores the
  token; grep confirms no on_login listener anywhere). Ticker start / regime backfill / warmup-gap
  backfill / warm-up re-eval run once at boot — a pre-login boot never recovers. This blocks ALL
  live G1 evidence. Fix building: PostLoginRecovery hook (guarded steps, same lifecycle gate, both
  login paths, idempotent, Telegram-visible). **Interim operator procedure: LOGIN FIRST, then
  start the engine** — a token-valid boot runs everything in one shot.

## 2026-07-20

- **Warm-up-frozen incident diagnosed + FIXED (`5d34e77`)**: owner's Telegram ping ("insufficient
  contiguous bar coverage") was a cold-start defect, not a data problem. `instruments_daily` was
  never written (Phase-1 TODO never landed) and the token map is in-memory, so any restart after
  the 08:15 instruments job booted token-less → unknown_token storm → warmup 0/77 → FROZEN.
  Fix: persist-on-refresh + startup hydrate (pre-login capable) + live-refresh fallback +
  `instruments_unavailable` explicit branch. Also: young listings (GROWW/TMCV 167/200 — zero-gap
  coverage since listing but structurally short of the lookback) no longer freeze warmup forever;
  excluded AND reported (`young_excluded`). 503 tests green. **Operator action: restart the engine
  (the 10:32 session still carries the dead token map; tonight's EOD jobs would fail in it).**
- **Offline G1 sweep run** (engine-safe half): suite 503 green; smoke test 22/22; cost model
  0-drift vs Zerodha live page (12/12 rates); calendar 2026 verified-through 2026-12-31; protected
  hashes MATCH; checkpoints healthy; job_runs shows clean catch-up incl. 07-16/17 replay. Live
  evidence accumulating: today counts toward ticker/news session tallies (morning session).
  Pending store-lock window: reconcile_log drift history, news-corpus inventory (watcher armed).

## 2026-07-19

- **Fresh insider-disclosure source FOUND (stage-3 unblock)**: NSE PIT embargo re-verified
  structural (~70 days rolling: boundary 2026-05-02 → 2026-05-10 over 3 days; no param variant
  unlocks it) and NSE announcements carry only trading-window closures — but BSE
  `getCorp_Regulation_ng/w` (browser network capture) serves fully structured PIT rows SAME-DAY
  (person/category/txn-type/mode/qty/VALUE/pre-post-%/timestamps/xbrl). Isdefault=1 = rolling
  ~100-row latest view; Isdefault=2 date-filtered, ~25-row cap, YYYYMMDD params. Plan §2.8 source
  table updated. Probe artifacts: scratchpad bse_insider_probe*.py + bse_insider_out/*.json.
- **Stage-3 build COMPLETE + validated** (494 tests green): `filings_pit_fresh` (BSE
  getCorp_Regulation_ng feed, 19:00 date-keyed job; id-prefix `bse:` source tagging — no ALTER
  path in the store; "Equity Shares" label + Pledge txn-type quirks fixture-pinned) ran LIVE:
  13 in-universe fresh rows same-day (111 out-of-universe skips expected; one day capped at 25
  even per-day — pagination-less endpoint, logged limitation). `FilingsEventBuilder` library
  (pure; cross-source dedup preferring NSE). **§6.4 validation (`scripts/validate_insider.py`,
  N=1 pre-registered): `insider` PROMOTABLE — CPCV 60.0%/60%, expectancy +0.0361%/day, 732 obs,
  620 position-days, 110 events** — param_set 01KXXNRT2M8TJWZXRT1836PB8A persisted. Highest
  expectancy on the platform. Remaining for live: Phase-2 digest/scanner wiring + §8.6 OWNER GATE
  (explicitly not self-served). Catalyst-watchlist writing stays deferred to Phase 2's digest.

Owner directive 2026-07-17: every substantive operation gets an entry — **what / why / outcome /
artifact pointer**. Recurring command lines live in [COMMANDS.md](COMMANDS.md). Newest entries at
the top of each dated section.

---

## 2026-07-17

- **E1–E3 VERDICTS** (single-pass, as pre-registered; reports `experiment_e{1,2,3}_20260717T*.md`):
  - **E3 insider_net_buy robustness: PASSED** — T+20 net mean positive in 4/4 year slices
    (2023H2 +4.02 / 2024 +0.54 / 2025 +1.81 / 2026H1 +0.21%), 3/3 person categories (promoter
    +1.13 the consistent core), 3/3 liquidity terciles. Caveats: 2024 median negative (45.7% hit);
    biggest gains in the LOW-liquidity tercile ⇒ slippage sensitivity to watch in paper. → stage 3
    proceeds.
  - **E1 catalyst-conditioned ORB: REFUTED** — conditioned breakout cohorts are WORSE than
    unconditioned (insider-cluster A: −0.039% gross, 40.7% win, n=177; results-T+1/2 B: −0.029%,
    41.5%, n=826; unconditioned C: +0.028%, 43.0%, n=43,474). The insider edge is slow (T+10/20)
    and does NOT express as day-of breakout continuation. Intraday slot has now failed every
    hypothesis (momentum, structural-stop momentum, catalyst-conditioned momentum; fade sub-cost)
    — **ORB/intraday parked with the question closed**; any revival = new owner-specced concept.
  - **E2 RSI2 catalyst-veto: UNANSWERABLE (n=1)** — only 1 of 102 champion trades carried adverse
    filings context (it won), 0 favorable. The flag rate on deep-oversold quality dips is ~1% —
    no historical basis to build a filings-based veto. Decision: do NOT build it; RSI2 improvement
    rides on Phase-2 concentration/sizing + live-corpus (news) features when they exist.
- **universe_daily 50/200 anomaly RESOLVED — designed behavior**: `data.universe_max_watchlist: 50`
  (settings.yaml:75) caps the active intraday watchlist; 149/150 exclusions on 2026-07-16 are
  `watchlist_cap` (liquidity-ranked overflow) + 1 `surveillance_asm`. Note: 2026-07-10's build
  produced 0 included (degraded first run — engine was mostly off that week; self-heals daily).
  Follow-up: none needed; historical tools must pass `--symbols` (recorded in COMMANDS.md).
- **Phase-1 checkpoint committed**: `a48f099` (134 files, +27,512). Push withheld per directive
  item 7 — owner permission required at phase end.
- **Pre-registered next experiments** (single-pass each, §6.4 N-accounting, verdicts stand as
  found): (E1) **catalyst-conditioned ORB** — H: breakouts WITH a fresh exchange-verified event
  (insider-buy cluster ≤5 sessions old, or results T+1/T+2) trend, unconditioned ones fade;
  discriminator = per-trade gross of the conditioned subset vs the known −0.02% unconditioned
  base; implemented as an offline analysis over ORB-v2 entry signals × filings flags (no sweep
  change until it passes). (E2) **RSI2 catalyst-veto** — H: dips WITH adverse filings/news context
  (insider selling, pledge increase, negative sentiment) are the losing tail of the 70%-win
  distribution; discriminator = win-rate/expectancy split of the 102 historical trades by context
  flag. (E3) **insider_net_buy robustness slices** — year-by-year, person-category
  (promoter vs employee), liquidity tier; stage-3 gate input.
- **Owner autonomy granted** — full iterate/test/update autonomy; push-to-remote requires owner
  permission at phase end; live-origination enablement (§8.6/R4) and money-spending remain
  owner-only. This log + COMMANDS.md created as required by the directive.
- **Pledge-delta stage-2 verdict: INCONCLUSIVE** — after the broadcast_dt fix + SHP re-backfill
  (13,403 rows, 3,985/4,011 promoter rows timestamped), only 2 strictly-consecutive non-null pairs
  crossed ±5pp (both moved opposite the folk thesis). Root cause of 24→2: BSE stores unpledged as
  NULL; derivation treats NULL as missing, not zero. Stage-3 pin: NULL≡0 when the promoter row
  exists. Verdict recorded in plan §2.8.4. → `data/reports/event_study.md`
- **SHP broadcast_dt defect fixed inline** (delegated agent 529'd twice): declaration-row
  `Fld_AuthoriseDate` fallback in `parse_shp_detail` + 3 pinning tests + `--redo-shp` flag;
  457 tests green; ~3.5h re-backfill run. → `src/engine/datafeeds/filings_shp.py`
- **Stage-2 event study (200 symbols, 2023-08→2026-07)**: `insider_net_buy` PASSED (T+10 net
  +0.75%, T+20 +1.61%, n=110, broad-based) — first cost-clearing edge on the platform;
  `results_filing` gross-positive but sub-cost (n=1,352) — feature material only; `cat`-style
  +1% confirmation refuted a 3rd time (n=266, net negative all horizons). → plan §2.8.4 verdict
  paragraph, `data/reports/event_study.md`
- **RSI2 economics re-derived**: the +0.0006%/day headline is equal-weight dilution; per-trade the
  best config is 102 trades / 70% win / +0.58% NET per trade (~+25-30%/yr on deployed capital
  before slippage). Improvement path = concentration (Phase-2/3 sizing) + catalyst-veto features,
  NOT signal surgery. → `data/reports/rsi2_sweep_20260716T180922.md` line 76

## 2026-07-16

- **3-year filings backfill** (NSE PIT/results/event-calendar windows + BSE SHP per-symbol loops +
  ISIN map): 45,496 PIT rows (boundary: NSE serves nothing after ~2026-05-02 — daily feed needs
  announcements-category fresh source), 25,047 results rows (thin after 2025-03), 13.4k SHP rows,
  200/200 ISINs, 199/200 BSE scrip codes. One crash fixed: `≤` in a print under cp1252.
  → `data/reports/filings_backfill_report.json`
- **§2.8 stage-1 filings data layer implemented** (delegated, audited): 4 DuckDB tables
  (symbol_isin, insider_trades, shp_quarterly, results_filings), 4 feed modules, bse_http helper,
  3 jobs (18:35/18:45/18:50), seed CLI, 26 tests. Audit caught nothing structural; my own harness
  bug briefly mis-flagged the PIT module (module was correct — `json.loads(resp.content)`).
- **Plan §2.8 written (owner decision O14)** — source verdicts (NSE primary; BSE SHP/pledge history
  + redundancy; **Tickertape REJECTED** — ToS + no disclosure timestamps + fragility; **Kite N/A**
  — no fundamentals surface, no ISIN), staged rollout with evidence gates, edge cases pinned.
- **Source research workflow** (5 live probes): NSE corporates-pit / financial-results /
  announcements / event-calendar (history ≥ Jan 2023, broadcast timestamps) verified; BSE SHP
  stack discovered via browser capture (per-category pledge data + quarter index); BSE error page
  masquerades as 200+HTML. → probe scripts in session scratchpad, evidence in plan §2.8 table
- **`universe_daily` anomaly flagged**: only 50/200 symbols included on 2026-07-16 (event study
  picked it up via its no-args default). Investigation pending.
- **Full-window revalidation after cmd.exe footgun**: `--symbols $syms` from cmd passed the literal
  string → 0-symbol run. CLI now hard-errors on unexpanded `$`/`%` symbols + warns on 0-bar
  universes. Proper rerun: orb still 0/15 (structural), rsi2/trend/mom promotable.
  → `data/reports/orb_20260716T180901.md` etc.

## 2026-07-12/13 (summary — pre-log)

- ORB v1 diagnosis (0/15 CPCV): honest negative — cost floor vs ATR(14,1m) noise-scale stops;
  vectorbt semantics audited (SL-before-TP; NaN-price orders silently ignored — square-off moved
  to symbol's last real bar). §6.1 v2 (owner-directed): `stop_range_frac` range-edge stop +
  C3 cost floor; envelope reseeded (protected hash `6e10b2…`). v2 still 0/15 on three windows —
  breakouts have negative GROSS drift here; ORB parked as honest-negative control.
- rsi2 `max_hold_days` time-exits modelled in the sweep (previously a no-op axis).
- Minute-bar history extended 2025-07-10 → 2023-07-17 (`backfill_minute_years` default was 1y).
- Backtest CLI: span-shortfall warning added.
