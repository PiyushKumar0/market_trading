# WORKLOG — autonomous operations log

## 2026-07-28 (night — fixes DEPLOYED at 21:09; boot verified clean)

- **Repair executed with owner approval** (service ACL denies unelevated stop; owner accepted the
  UAC `Restart-Service mt-engine`): rewound the 52 poisoned day-interval `backfill_checkpoints`
  rows (`through_date 2026-07-28 → 2026-07-20`; the monotonic checkpoint records REQUESTED-through,
  not observed-through, so days requested intraday/pre-bar advance past bars that were never
  written — that is how the 27th went missing under a "complete through 28th" checkpoint) and
  cleared the `daily_bars` job_runs rows for 27th+28th.
- **21:09:26 boot on the fixed code — every fix verified in production:** selftest ALL-PASS with
  `sdk_smoke` OK attempt 1 through the real structured-output path (9in/57out, 5.3 s);
  `catchup_sweep` armed; regime day backfill wrote exactly the 12 missing index bars
  (NIFTY 50 + VIX × 6 sessions) and the daily_bars catch-up 50 more across the watchlist;
  `warmup_not_ready` (which logged every 60 s for 2 h) went SILENT after the bars landed —
  warm-up READY. The `warmup_ready` FROZEN cause stays latched by design until the post-login
  reapply — tomorrow's daily login lifts it before open (mode is OFF anyway, owner's call to raise).
- **21:00 nightly_review fired on the OLD code** (before the restart): the reviewer generated
  1,593 tokens ($0.125) and died on `error_max_turns` — the exact fixed bug; job recorded success
  so tonight's review is skipped. First fixed-code review runs tomorrow 21:00.
- **News backlog draining live**: 11 `news_analyst` OK calls in the first 12 min (forced batch
  loops chunks back-to-back, ~40 s/call, every one attempt-1, ~$0.037/call) — the whole 1,050
  backlog clears tonight.
- **FOLLOW-ON ROOT CAUSE FIXED: checkpoint advance recorded REQUESTED-through, not
  observed-through** (`backfill.py` advanced to `chunk_end` even on a zero-candle span). This is
  the poisoning mechanism itself — and it would RECUR tomorrow: the pre-open regime backfill
  requests through "today" before today's bar exists, checkpoints it complete, and the 18:05
  daily_bars then skips it forever. Fix: advance to `min(chunk_end, max observed candle date)`;
  empty chunk leaves the checkpoint alone (`backfill_chunk_empty`) and the next pass re-requests
  it. 2 regression tests; suite 1,116 green. **Deployed pending one more service restart** —
  the 21:09 process predates this fix; it must be restarted before tomorrow's owner login
  (else the 29th gets poisoned at the first post-login fetch and Thursday freezes again).
- Morning checklist for 2026-07-29: owner login → post-login reapply lifts `warmup_ready`;
  pre-open news batch must complete before the 08:35 digest (remainder of the backlog, if any);
  preopen_planner 08:50 on fixed code should persist the first real DayPlan; enable RECOMMEND is
  an owner decision (G2).
- **21:50 second restart DONE (owner-accepted UAC)** — the observed-through checkpoint fix is
  live before tomorrow's login; no contingency needed. Boot clean: selftest ok (`sdk_smoke` SKIP,
  correctly deduped per trading day), `catchup_sweep` armed, regime day backfill truthfully
  no-op. Remaining news backlog resumes on the scoring cadence (19–22 h sweep tonight, pre-open
  batch tomorrow). Engine state: `mode=OFF`, `FROZEN(warmup_ready)` until the post-login lift.

## 2026-07-28 (evening — PHASE-2 FIRST DEPLOY validated; agent-harness structured-output fix)

- **Owner deployed `phase2` at 19:04 (first boot on the new code; migrations 0003+0004 applied).**
  Boot otherwise clean: selftest ok (one WARN, below), token probe valid, WS connected first try
  (52 tokens), feed HEALTHY, all missed evening jobs caught up (daily_bars 27+28, bhavcopy,
  corp_actions/deals for the 27th, backup). Engine sticky state `mode=OFF, risk=FROZEN(warmup_ready)`
  — mode stays OFF until the owner enables RECOMMEND (G2).
- **CRITICAL FOUND+FIXED: every production agent call failed on first live contact.** Two distinct
  mechanisms, diagnosed by replaying the exact stored `agent_calls.context_gz` payloads through the
  SDK: (1) `sdk_smoke` (the only schema-less call) got fenced ```json — WARN only; (2) every REAL
  call sends a json_schema, which the CLI fulfils via a **StructuredOutput TOOL round-trip** — the
  harness pinned `max_turns=1`, so compliant calls died (`error_max_turns`), and on top the CLI's
  default **extended thinking** pushed real durations (measured: 32s thinking-off, 48-80s+ with) past
  the 45/60s timeouts → the observed zero-token "timeouts" (news_analyst 0/1050 scored,
  preopen_planner no day-plan). Fix `harness.py`: +3 turn headroom exactly when the schema knob is
  sent (observed anatomy: tool turn + CLI-side schema-retry + closing text), payload extracted from
  the StructuredOutput tool input (outranks trailing prose; D7 "no fence-stripping" stands
  untouched), `max_thinking_tokens=0` pinned, smoke call now sends a schema so D11 exercises the
  REAL path. `agents.yaml` timeouts recalibrated to ~3× measured (intraday/news 120s, planner 180s,
  nightly 300s). Verified end-to-end against the live SDK with the actual failed news batch:
  success, 3 turns, 69s, all 30 clusters extracted. New pinned tests in `test_agent_harness.py`.
- **Warm-up frozen all session — root cause: 2026-07-27 18:05 `daily_bars` never ran** (machine
  slept 17:56→evening; boot-time catch-up is the ONLY catch-up, and there was no boot until 19:04).
  The whole universe lacked the 27th bar → `NIFTY 50 199/200`, `INDIA VIX 19/20`, and GROWW failed
  the young-listing exemption. Two fixes: (1) `catchup_sweep` interval job (30 min, watermark-
  deduped ⇒ idempotent) so sleep/resume gaps self-heal without a restart; (2) young-listing check
  now judges coverage INSIDE the session window only — the old `total == since_listing` compared
  against a span that includes TODAY'S evening bar, flipping every young listing back to a blocker
  each evening (the "GROWW quirk" watch-item, now closed). Regression tests pinned for both...
  NIFTY 50/VIX still 199/200 post-catch-up at 19:35 — bars_1d hole to verify+repair at restart
  (store is single-writer; can't inspect while the engine holds it).
- **Surveillance `sms` source RETIRED**: NSE removed `/api/unsolicited-sms` (hard 404; sibling
  reportGSM/ASM still serve → removal, not anti-bot; probed variants all 404). A permanently-dead
  source would re-fire the critical "degraded" alert every refresh for a list §3.2.4 never consumed.
  Field kept as the seam for a replacement.
- **Today's market session (old instance) had a broken feed ALL DAY**: ticker child WS upgrade
  403-Forbidden loop (stale daily token in the long-running process), HEALTHY→DEGRADED cycles
  every ~2 min, ~zero live ticks 09:15–15:30; all 18,750 bars came from official-candle backfill
  (reconcile compared 0). The 19:04 restart on a fresh token connected first try. Gap to consider
  for Phase 3: a WS 4xx-loop should escalate to the token-rejected freeze + login prompt instead of
  respawning for hours. Also chronic (pre-existing): Telegram polling/send failures throughout the
  day on the old instance (httpx connect errors — network blips), gdelt ReadTimeouts (34/day).

## 2026-07-28 (day — ADVERSARIAL REVIEW of `phase2`: 26 findings, 20 fixed, 6 accepted-with-notes)

- **6-dimension adversarial review (order-safety/gate-math/R1/wiring/money/concurrency) + 2-skeptic
  verification over the whole phase2 diff.** 26 unique findings; the verifier fleet was cut short by
  the session usage limit, so unverified ones were triaged by hand. ALL FIXED (each with a pinned
  regression test):
  **criticals** — (1) post-login warm-up lift wrote `risk_state=NORMAL` directly, erasing standing
  causes (defeated `/pause_entries` and a floor rung the selftest itself applied): every freeze now
  routes through the `risk_state_causes` ledger and the lift clears only its own causes; (2)
  `clear_cause` re-armed NORMAL over out-of-ledger freezes (e.g. token-rejected): now preserves any
  more-restrictive out-of-ledger state, and the token freeze is itself a cause auto-cleared on
  re-login; (3) LLM could substitute `tradingsymbol`/`side`/`style` in an EnterAction and be judged
  on the CANDIDATE's facts (gate approved a hijacked out-of-universe symbol): structural coherence
  guard drops mismatched payloads pre-gate (D7); (4) ticker subscription omitted held-position
  symbols (a dropped-from-universe holding marked at `avg_entry`, blinding the floor ladder): held
  symbols now always subscribed; (5) a stalled dashboard socket could wedge the KILL sequence
  (`apublish` awaited the WS relay): broadcasts are now fire-and-forget.
  **majors** — `daily_loss_soft/hard` had NO enforcement locus → `evaluate_day_loss`/`apply_day_loss`
  wired into the equity minute-tick + startup selftest, with day-scoped causes auto-cleared next
  session; gate approved an inverted BUY (stop above entry scored as a healthy short) → new
  `levels_coherent` rule; `capital_cap` mixed units (new MIS leg at notional/3 vs open legs at 1×) →
  full notional both sides until Phase-3 margin accounting; `edge_multiple_min` was hard-coded 2.0 →
  read from envelope_state/limits.yaml at boot; sector/correlation caps ignored pending recs (never
  bound in RECOMMEND) → pending recs charged; the ≤6/day analyst forward cap + DG1 4/day rung had no
  consumer → enforced in the pipeline; 08:30 universe rebuild never re-subscribed the feed nor the
  warm-up gate → both refreshed; news polls/scorer raced on shared cluster rows → serialized behind
  one lock; `/taken` on an exit rec would OPEN a phantom position → kind guard; `/closed` via an
  exit-rec id orphaned the entry ledger row → labels every open row for the position; model-authored
  regime note re-entered prompts unbounded → clamped + labeled; harness ran single-shot with SDK
  DEFAULT tools if the options class lost its tool knob → refuses for every shape (uniform Failed).
- **Accepted with notes (conservative direction or Phase-2 volume)**: day-baseline uses the newest
  prior snapshot (multi-day drift lands in today's MTM — over-freezes, never under); can_invoke is
  TOCTOU across concurrent bars (bounded by the forward cap); per-bar snapshot minting is
  store-lock-heavy at scale (watch-item ≥100 symbols); `on_bar` does small sync reads on the loop
  (0–3 tracked positions); `regime_data_ready` freezes all entries, not only regime strategies
  (stricter than plan wording).

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
