# RUNBOOK — market_trading (operations)

Specification: §10 of [IMPLEMENTATION_PLAN.md](../IMPLEMENTATION_PLAN.md). This is the operational
companion. **Phase 0** content is below; later phases extend it (full daily checklists, per-chaos-case
recovery, backup/restore) as those capabilities land.

The platform is up **only during active periods** and may be deliberately stopped in between (§2.6).
Being off outside an active period is **NORMAL, not a fault**. Every startup is a full recovery —
capital protection is broker-resident (R3), so an open position is protected with the engine and PC dead.

## Morning login flow (R6, §10.2) — the one daily human touchpoint

The Kite access token expires ~06:00 IST daily (A5). Each trading day:

1. Start the engine (manual/demand or the wake Scheduled Task — same code path, §2.6). The startup
   **live-probes the token against the broker** (2026-07-21 fix — a stale token can no longer
   masquerade as valid) and FREEZES entries while invalid (open positions stay broker-protected).
   The `/kite/callback` login port (:8400) is bound **before** the startup recovery, so the browser
   redirect works even while recovery is still running; a port-bind failure raises a critical
   Telegram alert (use the `/token` fallback then).
2. The engine sends the **Kite login link** to your Telegram immediately after the probe finds the
   token rejected/absent (also shown in the dashboard). Open it via whichever login method you
   registered (see below). If the token dies **mid-day**, the first rejected broker call freezes
   entries and re-sends the login link automatically (same recovery as the morning flow).
3. After you clear Kite's TOTP screen, Kite redirects the browser to `…/kite/callback?request_token=...`
   (the only unauthenticated route; it can only complete a login). The engine exchanges the checksum and
   stores the access token in Windows Credential Manager (DPAPI). Entries un-FREEZE once the token is valid.
4. **Fallback** if the redirect can't reach the callback: copy the `request_token` from the URL and send
   `/token <request_token>` to the Telegram bot (or paste it into the dashboard).

### Login method — pick ONE and register its exact redirect URL

Kite allows a **single** registered Redirect URL per app, and it **must match exactly**. Choose the
method that fits how you log in:

| Method | Where you open the login link | Register this redirect URL |
|---|---|---|
| **PC-based** (simplest; no networking setup) | a browser **on the PC** running the engine | `http://localhost:8400/kite/callback` |
| **Phone-based** (the daily one-touch flow, §10.2) | the Telegram link **on your phone**, same Wi-Fi/LAN | `http://<PC-LAN-IP>:8400/kite/callback` |

`localhost` works for PC-based login because the browser and the engine are on the same machine — **no
LAN IP and no static-lease needed**. Phone-based login needs the PC's LAN IP because `localhost` on the
phone resolves to the phone itself. To switch methods later, just update the registered URL on the Kite
app. The `/token` fallback (step 4) works regardless of which URL is registered.

## Planned stop / start (§2.6, §10.8) — the new normal

- **Start:** `mt-engine` (NSSM) or `python -m engine.ops.main`. Runs the full §2.6 recovery + catch-up,
  then idles in the sticky mode (OFF on a fresh install — safe).
- **Only ONE instance can run** (2026-07-21 fix): startup takes an exclusive kernel lock on
  `data/engine.lock` before touching any store. A second start refuses with **exit code 3** and logs
  `single_instance_refused_lock` (naming the holder pid) — it is NOT an error in the running engine.
  If the holder is a wedged boot, the watchdog force-kills it and the lock frees itself on process
  death (never delete `engine.lock` by hand; the lock lives in the kernel, not the file).
- **Stop:** stop the NSSM service (or Ctrl-C). **First Ctrl-C = graceful** (honoured at the next safe
  point, even mid-startup); **second Ctrl-C = forced hard exit** (2026-07-21 fix — state stays RUNNING
  so the next boot crash-recovers; positions stay broker-protected, R3). A wedged internal worker can
  no longer leave a zombie process holding the lock. The shutdown guard (§2.6) will, from Phase 3,
  flatten an open MIS before window-end / verify CNC GTTs / cancel working entries — never leaving the
  PC dead with an unprotected position or a resting entry order.
- A clean self-initiated stop is **not** auto-restarted (NSSM startup is manual/demand; restart-on-failure
  is gated to an "I intend to run" sentinel, §2.2).

## Dead-platform manual fallback (R10, §10.6) — pre-stage BEFORE Phase 4

If the platform and PC are dead with open positions, capital is still protected by the resting SL-M/GTT
(R3). To intervene manually:

1. **Kite app / web** — flatten positions manually.
2. **Kite Console kill switch** — blocks the account's API + trading.
3. **Call & Trade desk** — phone number on a **printed card kept by the desk** (fill in below).

```
Zerodha Call & Trade: __________________   Client ID: __________   (print + keep offline)
```

## Windows service setup (E7/D11, §10.7)

- **Install the `mt-engine` service** — run every step from an **elevated (Administrator) PowerShell**
  (service create + `nssm set` require it):
  1. **Install NSSM** (not bundled): `winget install NSSM.NSSM` (or download from https://nssm.cc). Use the
     **win64** binary.
  2. **Copy `nssm.exe` to a stable path first.** winget drops it in a *version-stamped* package folder
     (`…\WinGet\Packages\NSSM.NSSM_…\nssm-2.24-…\win64\nssm.exe`) and does **not** add it to PATH. The
     installed service's binary path becomes the exact `nssm.exe` you register with, so a later
     `winget upgrade` would move that folder and **break the service** — copy it out of the winget dir first,
     e.g. `Copy-Item …\win64\nssm.exe C:\tools\nssm\nssm.exe` (optionally add `C:\tools\nssm` to PATH).
  3. **Create the venv first** (`uv sync`) — the service runs `.\.venv\Scripts\python.exe -m engine.ops.main`;
     install preflight fails without it.
  4. **Apply** (idempotent — re-running reconfigures; startup = manual/demand, §2.2/§2.6):
     `scripts\nssm_install.ps1 -Action install -Confirm -NssmPath C:\tools\nssm\nssm.exe`.
     **`-Confirm` is mandatory** — without it the script only DRY-RUNS (prints the plan, changes nothing).
     Omit `-NssmPath` if `nssm.exe` is on PATH; preview safely by omitting `-Confirm` first.
  - Control: `scripts\nssm_install.ps1 -Action status | start | stop | remove` (`remove` also needs `-Confirm`).
- Mint the Claude SDK OAuth token: `scripts/setup_token.ps1` (stores it in DPAPI; the engine injects it
  into the SDK CLI child at startup, §2.2). Ensure **no** stray `ANTHROPIC_API_KEY` (it silently outranks
  the OAuth token, D2 — the startup self-test asserts its absence).
- Power: disable Modern Standby network throttling; set Windows Update active hours; a wake-capable
  Scheduled Task wakes the PC for scheduled active periods incl. EOD jobs.

## Phase-0 verification checklist (Gate G0, §8.1)

- [ ] `uv run python scripts/smoke_test.py` green (A4 install smoke test).
- [ ] `uv run python scripts/a11_check.py` run; record `data.minute_candles_adjusted` in settings + §14 Q10.
- [ ] Secrets seeded (`scripts/dpapi_set.py`); `git grep` shows no secret values in the repo.
- [ ] Protected store seeded (`scripts/seed_protected_config.py`) after reviewing limits/envelope.
- [ ] Full morning login completes and un-FREEZEs entries — phone flow in < 60 s, **or** PC-based
      login via the `http://localhost:8400/kite/callback` registration.
- [ ] Kill stickiness: set `/kill` → restart service → order path blocked on startup.
- [ ] Claude Max ToS for headless use confirmed, or metered `ANTHROPIC_API_KEY` fallback configured (T8).

> ⚠ Before Phase 1: **verify `config/calendar/2026.yaml`** against the official NSE holiday circular and
> set `verified: true` + `verified_through` — the calendar ships `verified: false` and strict mode refuses
> to trade until you do (R6, "no calendar, no trading").

---

# Phase 1 — data plane operations (§8.2, gate G1)

Phase 1 adds the **data plane**: the ticker→bar pipeline, all §4.4 daily jobs, the news pipeline data
side, the feature engine, the cost model, the four price baselines + validation, and the §2.6
cold-start/watchdog machinery. Everything is wired in the composition root `engine.ops.main`; the data
jobs are **catch-up-eligible** (a missed fire-time is replayed on the next startup via the `job_runs`
watermarks, §2.6). Broker-touching jobs (instruments dump, backfill, reconcile, ticker) run only once
Kite credentials exist — a fresh install stays runnable with entries FROZEN until login.

## Daily job schedule (§10.1, IST, trading days per calendar)

These are **fire-times**, not a liveness assumption — each is also replayed as a startup catch-up if its
time passed while the engine was off (`CatchUpRunner`, driven by the SAME `JobRegistry` the scheduler
uses). Times are the `jobs:` block in `config/settings.yaml`.

| Time | Job (`job_runs` id) | Class | Notes |
|---|---|---|---|
| 08:15 | `instruments` | safety-critical | Kite dump + tick sizes (A10); stale ⇒ FROZEN-for-entries |
| 08:20 | `surveillance` | safety-critical | GSM/ASM/T2T/ESM (A8); reuse-yesterday on source failure |
| 08:25 | `news_chain` | run-latest | backfill → cluster → resolve (never entry-blocking, §2.7) |
| 08:30 | `universe_build` | run-latest | NIFTY200 ∩ MIS ∩ ¬surveillance ∩ ≥₹5cr (A8/C7) |
| 08:30 Sun | `sector_map` | run-latest | weekly `sector_map`+`theme_map` (fires Sunday, not a trading day) |
| 15:50 | `bar_reconcile` | date-keyed | self-vs-official 1m drift (A13); one run per missed day |
| 18:00 | `bhavcopy` | date-keyed | UDiFF cross-check/fill of `bars_1d` |
| 18:05 | `daily_bars` | date-keyed | nightly incremental official-candle backfill (watchlist + NIFTY 50 + India VIX) |
| 18:30 | `earnings_calendar` | safety-critical | results/board-meeting dates (R2/O13) |
| 20:15 | `corp_actions` | run-latest | ex-dates/splits/bonuses (A12 data; moved 18:15→20:15 2026-07-24, NSE evening 503s) |
| 20:30 | `deals` | date-keyed | bulk/block deals → `flagged_instrument_days` (moved 18:45→20:30 2026-07-24) |
| 20:45 | `features_daily` | date-keyed | §6.2 v1 feature snapshot for the day's universe (moved 18:50→20:45 2026-08-18 — MUST stay after `deals`/`corp_actions`, it reads both) |
| 21:00 | `backup` | run-latest | SQLite `state.db` snapshot to `data/backups/` (§10.5) |

Live (interval, not calendar-gated, run whenever the engine is up): `bar_advance` (5 s bar
finalization), `health_check` (`lifecycle.watchdog_poll_s`), and the news polls — one `news_poll_<name>`
job per `news.feeds.rss` entry at its own `poll_s` (2026-08-04 seed: `et` 300 s, `livemint_markets` /
`livemint_companies` 900 s) plus `news_poll_gdelt` (`news.gdelt_poll_s`=3600 s).

## Historical backfill procedure (A2, checkpointed & resumable)

The initial minute history is a multi-evening job — throttled to ≤3 req/s inside `KiteClient`, chunked
per Kite's per-request caps, and **checkpointed per `(symbol, interval)`** in `state.db`
`backfill_checkpoints`, so a re-run resumes exactly where it stopped (never re-fetches).

- **Warm-up / gap fill on startup is automatic** — every startup computes its off-span and backfills
  the intraday minute gap (`src='gap_backfilled'`, excluded from reconcile drift) plus the NIFTY 50 /
  India VIX daily history the regime features need (§2.6 step 4 / `regime_data_ready`).
- **Seed the full history once** (offline, after login so a token exists). Drive `BackfillJob.run` for
  the universe: minute interval for intraday depth (`data.backfill_minute_years`, default 1y) and day
  interval for the daily baselines (`data.backfill_daily_years`, default 2y). Re-run freely — the
  checkpoint makes it idempotent. Watch the `backfill_run_done` / `backfill_chunk_failed` log events;
  a failed symbol stays behind its checkpoint and resumes next run.
- **A11 (no re-adjustment):** Kite minute+daily candles are already corp-action adjusted
  (`data.minute_candles_adjusted=true`) — candles are written exactly as fetched.
- **Baselines / validation:** `scripts/backtest.py <orb|rsi2|trend|mom|all> --from YYYY-MM-DD --to
  YYYY-MM-DD [--index-symbol "NIFTY 50"] [--grid-density …] [--reports-dir …]` runs the vectorbt sweep
  → skfolio CPCV/walk-forward → writes `param_sets` rows + a report (verdict + honest negatives, C9).
  The §2.7 event-study proxy backtest is `scripts/event_study.py` (earnings + bhavcopy gap/volume
  events → `data/reports/event_study.{md,json}`). Both honor `MT_DATA_DIR`.

## Intraday official-candle latency (§14 Q15)

Measures how long after a minute closes its official Kite candle becomes fetchable (the reconcile /
warm-up freshness assumption). Run during market hours after login:

```
python scripts/q15_candle_latency.py [--minutes 15] [--symbols … ] [--poll-s 2.0] [--out data/reports/q15_latency.json]
```

Records per-minute availability + percentile latencies to `data/reports/q15_latency.json`. Confirm the
`warmup_ready` FROZEN fallback holds if candles are late (a start too close to the window stays FROZEN).

## Pre-open start (tick coverage) (§14 Q14, IMPROVEMENT_SPEC.md F8)

**Start the engine by 09:05 IST on trading days.** Raw tick capture — and therefore everything
`data/parquet/ticks` carries (bid/ask, depth, cumulative volume) — begins only once the ticker
subprocess is up; a late start loses the opening session **permanently**. Unlike `bars_1m`, which
the §2.6 warm-up gap-fill can reconstruct OHLCV-only from Kite's historical MINUTE-CANDLE API
(`src='gap_backfilled'`), raw ticks have **no backfill path** — Kite exposes no historical tick/quote
API, so a tick the engine was not up to receive is gone for good, independent of any code fix.

**Measured evidence (F8):** first captured bar was **09:19 IST on 2026-08-12** and **09:52 IST on
2026-08-11** — both late-start days. On each, the opening ~30 minutes (09:15 auction-to-open onward,
the most information-dense stretch of the session) are absent from tick storage. Consequence: any
opening-range strategy (§6.1 `orb`-class) and any open-session spread measurement are blind exactly
there — this also **understated** the audit's own opening-range estimate (only 24/48 measured
symbol-days had a usable capture start; see `IMPROVEMENT_SPEC.md` Part III, "Measurement provenance
(spread/range, W2b)").

This is a start-**time** requirement, not a missing capability — every start already runs the
identical §2.6 recovery/catch-up path regardless of clock time; starting earlier only means the
ticker subscribes and writes before 09:15 instead of after.

**Mitigation — pick one:**
1. **Manual (default):** start the engine (service or `python -m engine.ops.main`) by 09:05 IST
   yourself, same discipline as the morning login flow above.
2. **Scheduled (owner option, plan §14 Q14):** Q14 recorded manual-start-primary **deliberately** —
   this note documents the already-built option, it does not change that default. Register the
   wake-capable engine-start Scheduled Task (§10.7 below): `\.scripts\schedule_tasks.ps1
   -WithEngineStarts`, with a `lifecycle.active_period_starts` time at or before 09:05 IST. The
   watchdog (`scripts/watchdog.py`) already alerts (`SCHEDULED_START_MISSED`) if a scheduled start
   fails to fire, so a missed pre-open wake is caught the same day.

Neither option is registered by default — this note changes no config and creates no Scheduled Task.

## Out-of-band watchdog + Scheduled Tasks (§2.2/§10.7)

The engine's `HealthMonitor` dies with the process and cannot detect its own death, so a **separate
non-wake** process watches it. `scripts/watchdog.py` opens `state.db` **read-only**, keeps its own
debounce in `data/watchdog_state.json`, and raises `SCHEDULED_START_MISSED` (an expected
`lifecycle.active_period_starts` start did not occur) or `ENGINE_DOWN` (`state != STOPPED` and the pid
is dead, or wedged past `catchup_grace_s` — alert first, then force-kill so NSSM restarts it).
`state == STOPPED` is **silent** (intentional off is normal). A total power loss takes the watchdog with
it (§2.2 limitation).

- Register both tasks (elevated PowerShell, **run-as the mt-engine service account** so the per-user
  DPAPI bot token decrypts):
  - `\.scripts\schedule_tasks.ps1` — watchdog only (1-min repetition).
  - `\.scripts\schedule_tasks.ps1 -WithEngineStarts` — also add wake-capable engine-start tasks, one
    per `lifecycle.active_period_starts` time (covers EOD jobs that fall outside a wake window).
  - `\.scripts\schedule_tasks.ps1 -TaskUser "PC\mtsvc"` — set the run-as account explicitly.
- Requires the `telegram_bot_token` DPAPI secret + `telegram.owner_chat_id` in settings, else the
  watchdog exits code 2 (it has nothing to alert through).

## News-feed verification checklist (`[VERIFY Phase-1]` URLs — feeds move / are anti-bot [likely])

The G1 gate exercises these live for ≥5 sessions. Confirm each returns parseable data under the client's
browser-shaped headers; a per-source failure degrades gracefully (reuse-yesterday / skip) but should be
re-pointed, not left broken. Feed set is `config_audit`-tracked (owner-only changes).

- [x] **ET Markets RSS** — `news.feeds.rss.et` (300 s poll). Verified live 2026-08-04.
- [x] **Livemint markets/companies RSS** — `news.feeds.rss.livemint_*` (900 s). Verified live 2026-08-04.
      (Moneycontrol RSS RETIRED 2026-08-04: whole MC feed ecosystem frozen since ~2024-04 — newest
      pubDate ~832 days old, 391 polls with zero inserts. Do not re-add without a fresh probe.)
- [x] **Hindu BusinessLine markets/companies, CNBC-TV18 market, NDTV Profit** —
      `news.feeds.rss.hbl_* / cnbctv18_market / ndtvprofit` (900 s). Verified live 2026-08-05
      (corroboration-pool widening 2→5 domains; both Livemint feeds are ONE domain). Rejected
      2026-08-05: financialexpress (malformed XML), zeebiz + business-standard (WAF 403),
      businesstoday (no parseable pubDates).
- [ ] **GDELT DOC 2.0** — `GDELT_DOC_URL` + `news.feeds.gdelt_doc_query`, client-side filtered to
      `GDELT_DOMAIN_ALLOWLIST` (module-pinned in `datafeeds/news.py`; widening it is a code change).
- [ ] **MIS leverage** — `KITE_MIS_MARGINS_URL` = `https://api.kite.trade/margins/equity` (fail-closed:
      empty ⇒ no symbol MIS-eligible).
- [ ] **Surveillance** (`datafeeds`… `universe/surveillance.py`): NSE GSM/ASM/ESM report APIs, T2T from
      `EQUITY_L.csv` (series BE/BZ), unsolicited-SMS list.
- [ ] **NIFTY200 membership** — `universe.nifty200_source_url` (archives host); falls back to
      `data/universe/nifty200_cached.csv` then the committed `config/universe/nifty200_seed.csv`.
- [ ] **Bhavcopy** — `BHAVCOPY_URL_TEMPLATE` (UDiFF, archives host).
- [ ] **Corp actions** — `https://www.nseindia.com/api/corporates-corporate-actions?index=equities`.
- [ ] **Event calendar** — `https://www.nseindia.com/api/event-calendar`.
- [ ] **Bulk/block deals** — `NSE_BULK_DEALS_URL_TEMPLATE` / `NSE_BLOCK_DEALS_URL_TEMPLATE`.
- [ ] **Sector/theme** — `SECTOR_SOURCES` sectoral-index constituent CSVs (first-wins order is pinned).

Entity-resolution precision (G1): spot-check a 50-headline sample — ≥95% of resolved symbols correct and
ambiguous names correctly UNmatched (`unresolved_entities` log, §9.1).

## Gate G1 evidence checklist (§8.2)

- [ ] **Ticker stability:** ticker runs **5 consecutive sessions** without manual intervention
      (auto-respawn allowed, counted + alerted).
- [ ] **Reconcile drift:** bar reconciliation within §3.2.3 thresholds on **≥95% of symbol-days**
      (offline spans backfilled from official candles are excluded from the drift denominator, §4.4 job 2).
- [ ] **Backfill complete** for the universe (minute + daily), checkpoint table shows no stuck symbols.
- [ ] **Cold-start recovery:** start the engine after a **simulated multi-day gap** — it backfills the
      candle gap (`warmup_gap`) and catches up every missed data job via `job_runs` watermarks; the
      startup/recovery report leads with the `crash-recovered` notice where applicable (chaos case 22).
- [ ] **Q15 latency measured** (`scripts/q15_candle_latency.py`) and the `warmup_ready` FROZEN fallback
      verified.
- [ ] **Baselines:** all four price baselines produce **walk-forward + CPCV reports** with cost-adjusted
      expectancy and **honest negative results if negative** (C9).
- [ ] **Cost model** spot-checked against an actual Zerodha contract note of a manual trade (C2); re-run
      `scripts/rescrape_costs.py` before release and diff `config/costs.yaml`.
- [ ] **News pipeline** ingesting + clustering + resolving on live feeds for **≥5 sessions**, with the
      50-headline entity-resolution precision spot-check above (≥95%).

---

# Phase 2 — RECOMMEND live operations (§8.3, gate G2)

## Gate G2 evidence checklist (§8.3)

Run the collector first — it computes every machine-checkable bar below in one pass and prints a
MET / NOT-MET / N-A column plus the per-session digest detail and the budget table:

```
.venv\Scripts\python.exe scripts\g2_evidence.py [--from 2026-07-29] [--to YYYY-MM-DD]
                                                [--month YYYY-MM] [--json data\reports\g2_evidence.json]
```

It opens `data/state.db` **read-only** (`file:…?mode=ro`) and never touches `data/market.duckdb`, so
it is safe to run against the live engine. It reads DB rows only — no engine log parsing, no engine
import. Read the **SOURCES + COVERAGE CAVEATS** block it prints before quoting any number: several
bars are judged against owner-set state (the trade window) that moved during the day.

- [ ] **Digest before window-open:** "catalyst digest + watchlist produced before window-open on
      **≥90% of sessions**". Evidence: `scripts/g2_evidence.py` criterion 1 (source:
      `job_runs(job_id='catalyst_digest').last_success_at` vs the `config_audit`
      `trade_window_state` value in force at the digest instant). ⚠ Judge the per-session detail
      table by hand where the **WINDOW RESET LATER SAME DAY** flag is set — on those days the owner
      moved the window after the digest landed, so the pass/fail depends on which value you count.
- [ ] **News Analyst schema validity:** "News Analyst batches schema-valid on **≥95% of calls**".
      Evidence: `scripts/g2_evidence.py` criterion 2a (source: `agent_calls`, **one row per SDK
      call, retries included** — the conservative denominator). Criterion 2b reports the Intraday
      Analyst for context; §8.3 sets no bar on it, but a low reading is a live defect to fix.
- [ ] **Recommendation delivery timeliness:** "**≥90%** of recommendations delivered before their
      validity window opens". Evidence: `scripts/g2_evidence.py` criterion 3 (source:
      `recommendations.delivered_at` vs the payload's `valid_until`; the payload carries no
      `valid_from`, so the window opens at `created_at ≤ delivered_at` by construction).
- [ ] **Payload completeness:** "daily recommendations with **gate verdicts + cost math + manual
      checklist in every payload**". `[owner-manual]` — sample delivered `recommendations.payload`
      rows and confirm `gate`, `cost`, and `manual_checklist` are populated on each.
- [ ] **Duration:** "**4 weeks** of daily recommendations". Evidence: `scripts/g2_evidence.py`
      criterion 5 counts sessions with ≥1 delivered recommendation against a 20-session bar;
      `[owner-manual]` sign-off that the 4 weeks were *continuous operating* weeks, not a padded
      count across outages.
- [ ] **Owner manual executions:** "owner has executed **≥5 recommendations manually** and outcomes
      captured in the ledger". Evidence: `scripts/g2_evidence.py` criterion 4 counts
      `recommendations.human_action='taken'` (and `'closed'`); `[owner-manual]` — the executions
      themselves, reported back through Telegram `/taken <rec_id> <qty> <price>` and
      `/closed <rec_id> <price>` so the `learning_ledger` row closes with a real outcome (§6.5).
- [ ] **Budget discipline (D6, re-scoped 2026-08-13):** the plan's original bar ("within **10%**
      of console-reconciled spend") assumed a monthly-credit billing model that doesn't exist —
      owner clarification 2026-08-13: SDK usage bills against the Claude subscription's **weekly
      usage limits** (Anthropic's June-15 notice paused the credit change), so there is no console
      dollar figure to reconcile. The check is now: `scripts/g2_evidence.py` criterion 7 —
      ledger arithmetic sane and month-to-date spend within the **self-imposed** allocations in
      `config/agents.yaml` (the DG-ladder input); `[owner-manual]` — subscription usage-limit
      headroom not under pressure during the soak.
- [ ] **Zero API orders:** "zero API orders placed (**broker order book empty of platform orders** —
      audited)". Evidence: `scripts/g2_evidence.py` criterion 6 proves the platform side
      (`orders` / `order_events` empty); `[owner-manual]` — the broker-side audit in Kite Console
      (Orders → all/GTT tabs for the period). The engine cannot evidence its own absence there.
- [ ] **Watchlist precision:** "owner has reviewed watchlist precision on **at least two weekly
      samples** (are `originating` entries genuinely material catalysts?)". `[owner-manual]` — sample
      the dashboard news/watchlist panel or `catalyst_watchlist` grade=`originating` rows for two
      separate weeks and record the verdict (same shape as the G1 50-headline sample above).
