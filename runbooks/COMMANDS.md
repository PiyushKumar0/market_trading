# COMMANDS — recurring command reference

Recorded per the 2026-07-17 owner directive so recurring tasks need no re-discovery. All commands
run from the repo root in **PowerShell** (NOT cmd.exe — `$syms` expansion silently fails there;
the backtest CLI now hard-errors on it). The market store (DuckDB) allows ONE writer: the engine,
a backtest, or a backfill — never concurrently.

## Stuck engine / login port dead (2026-07-21 incident pattern)

```powershell
# Who holds :8400 + which engine processes exist (two instances = the wedge pattern):
netstat -ano | Select-String ":8400"
Get-CimInstance Win32_Process -Filter "Name='python.exe' OR Name='pythonw.exe'" |
    Select-Object ProcessId,ParentProcessId,CreationDate,CommandLine | Format-List
# Kill a wedged engine TREE (venv shim + child; $pid is reserved in PowerShell — use another name):
taskkill /F /T /PID <procId>
# Startup triage from the structured log:
Get-Content data\logs\engine.log -Tail 60
Select-String -Path data\logs\engine.log -Pattern 'token_probe|api_bind_failed|engine_ready|needs_login|token_rejected' |
    Select-Object -Last 15
```

Post-fix behaviour: boot probes the token live (`token_probe` outcome in the log), sends the login
link immediately on `rejected`/`absent`, and binds `/kite/callback` BEFORE the startup recovery;
`api_bind_failed` + a critical Telegram alert = port held by another process (use `/token`).
A second engine start now refuses outright — **exit 3** + `single_instance_refused_lock` naming the
holder pid (kernel lock on `data\engine.lock`; frees itself when the holder dies — kill the wedged
holder pid, never delete the file).

## Service logs (NSSM mode)

```powershell
Get-Content data\logs\engine.log -Tail 50 -Wait        # structured log (all launch modes) — primary
Get-Content data\logs\service.err.log -Tail 50 -Wait   # NSSM-captured stderr: + raw tracebacks/early-boot crashes
Get-Content data\logs\service.out.log -Tail 50         # NSSM-captured stdout: stray prints
Get-WinEvent -ProviderName nssm -MaxEvents 20 | Format-Table TimeCreated, Message -Wrap  # start/stop/crash-restart/throttle
scripts\nssm_install.ps1 -Action status                # service config incl. ObjectName + log paths
```

## Universe symbol list (the canonical 200-name set for historical runs)

```powershell
$syms = ((Get-Content data\reports\orb_sweep_20260712T033313.json | ConvertFrom-Json).symbols) -join ','
```

## Backtests (sweep + CPCV validation, §8.2)

```powershell
# all four price baselines, canonical window (2026-H1 held out until needed):
uv run python scripts\backtest.py all --from 2024-01-01 --to 2025-12-31 --index-symbol "NIFTY 50" --symbols $syms
# one strategy, finer grid:
uv run python scripts\backtest.py rsi2 --grid-density medium --from 2024-01-01 --to 2025-12-31 --index-symbol "NIFTY 50" --symbols $syms
# positional leg measured against ITS holding cap (R2, 2026-09-12) — WO-3 floor = cost_floor/120:
uv run python scripts\backtest.py trend --from 2024-01-01 --to 2025-12-31 --index-symbol "NIFTY 50" --symbols $syms --margin-floor-days 120
```
Reports → `data/reports/<strat>_<ts>.md` (+ sweep). ORB leg ≈ 90 min on the full 498-session window.
`--margin-floor-days` defaults to 20 (the §7.1 swing cap); set it per-leg, never on an `all` run — the
value used is printed in the validation report's "Multiple-testing discipline" block.
Since 2026-09-12 (R2) both reports also carry the leg's MEASURED holding period — mean/median/p90
sessions, closed-only beside all-trades, the open-trade count, and the same round-trip floor re-based
on each of those horizons with its headroom multiple. Read the verdict against those rows: the
registered denominator is a §7.1 CAP, so the floor at the cap is the loosest bar the horizon allows.
Reporting only — the promotion rule is unchanged.

## Event study (§2.7 proxy + §2.8.4 filings legs)

```powershell
uv run python scripts\event_study.py --from 2023-08-01 --to 2026-07-16 --symbols $syms
```
→ `data/reports/event_study.{md,json}`. Without `--symbols` it uses TODAY'S live universe_daily
(correct for live, wrong for history). `--skip-filings-legs` = pre-§2.8.4 behavior.

## Backfills

```powershell
# bars (needs valid Kite session — morning login first):
uv run python scripts\backfill.py seed --skip-daily --minute-years 3 --reset-checkpoints
# filings (public NSE/BSE APIs, no Kite needed; checkpointed/resumable; SHP leg ≈ 3.5 h):
uv run python scripts\backfill_filings.py seed --from 2023-07-01
uv run python scripts\backfill_filings.py seed --from 2023-07-01 --skip-pit --skip-results --redo-shp
```

## Protected config (after ANY owner-directed edit to limits.yaml / envelope.yaml)

```powershell
uv run python scripts\seed_protected_config.py --yes --reseed --note "<why, citing the directive>"
```

## Tests / verification

```powershell
uv run pytest tests/unit -q                     # full suite (~457 tests, ~2 min)
uv run pytest tests/unit/test_filings_feeds.py tests/unit/test_sweep_signals.py -q
```

After ANY edit to `config/agents.yaml`, validate through the engine's own loader BEFORE the
restart that would apply it (an unmapped model name darkens the whole LLM tier — 2026-08-03):

```powershell
.venv\Scripts\python.exe -c "from engine.core.config import load_yaml, config_dir; from engine.intelligence.harness import load_agent_roster; r = load_agent_roster(load_yaml(config_dir() / 'agents.yaml')); print(r)"
# want: every enabled agent in defs with the intended model/timeout, and quarantined={}
```

## Store inspection (read-only; safe while engine is OFF, fails if any writer holds the lock)

```powershell
uv run python -c "import duckdb; con = duckdb.connect(r'data\market.duckdb', read_only=True); print(con.execute('show tables').fetchall())"
```
Coverage checks: see scratch patterns in WORKLOG entries (bars/filings min/max/count queries).

## Engine lock etiquette

- Engine holds `data/market.duckdb` read-write for its lifetime; check before long runs:
  `Get-CimInstance Win32_Process -Filter "Name = 'python.exe'" | Where-Object { $_.CommandLine -match 'engine.ops.main' }`
- A backtest/backfill blocks engine startup for its duration — schedule long runs outside
  trading/EOD-job hours.

## Phase-2 surfaces (2026-07-28)

```powershell
# Dashboard build (served by the engine at / from dashboard/dist):
cd dashboard; npm install; npm run build; cd ..
# Dashboard preview WITHOUT the engine (dev only, 2026-09-08): dist + canned rows across several IST
# days at http://127.0.0.1:8499/, any token passes; $env:NO_TODAY='1' drops today's ledger rows.
cd dashboard; node fixture_server.mjs; cd ..
# Phase-2 test slices:
uv run pytest tests/unit/test_risk_gate.py tests/unit/test_reco_pipeline.py -q     # gate + pipeline
uv run pytest tests/unit/test_budget_governor.py tests/unit/test_agent_harness.py -q  # LLM plumbing
uv run pytest tests/unit/test_catalyst_digest.py tests/unit/test_news_scoring.py -q   # news layer
```
- RECOMMEND flow needs: mode RECOMMEND (`/mode RECOMMEND`), valid trade window, warm-up ready,
  and the Claude OAuth token present (else the LLM tier is disabled and only scanners run).
- **Budget window is the subscription's QUOTA WEEK (2026-09-12):** Thursday 14:00 IST → next Thursday
  14:00 IST (`llm.quota_window` in agents.yaml; `weekly_credit_usd` + weekly per-agent allocations).
  `/budget` (Telegram or `GET /budget`) prints the window key (the start Thursday's date), the
  window spend per agent vs allocation, the tier and the live forward cap. DG1 trips on PACE only;
  an agent past 85% of its own allocation degrades only itself (cap = 0.67 × base), at 100% it is
  hard-stopped. A change to agents.yaml applies on the next engine boot — validate with the loader
  one-liner above first. The month column in `budget_ledger` is written but read by nothing.
- Owner outcome capture: `/taken <rec_id> <qty> <price>`, `/closed <rec_id> <price>`, `/veto <rec_id>`.

## News-feed health (2026-08-04, after the MC-retirement remediation)

```powershell
# Engine service restart (NSSM; SCM stop registers as crash_recovered=true in startup_report — harmless):
Restart-Service mt-engine

# Per-feed poll health from logs (fetched/unique/inserted; a feed whose standalone polls NEVER
# insert is dead or frozen upstream — the MC failure shape):
Select-String -Path data\logs\engine.log -Pattern '"news_polled"' | ForEach-Object { $_.Line | ConvertFrom-Json } |
  Group-Object { $_.feeds -join ',' } | ForEach-Object { "{0}: polls={1} inserted={2}" -f $_.Name, $_.Count, (($_.Group | Measure-Object inserted -Sum).Sum) }

# Corpus corroboration health (needs engine OFF, or run against a data\backups\*.duckdb copy):
# distribution of distinct source domains per scored cluster — if ~all are 1, min_source_domains:2
# can never pass and n_originating is structurally 0 (see memory: news-feed-starvation-zero-origination):
uv run python -c "import duckdb, collections; con = duckdb.connect(r'data\market.duckdb', read_only=True); rows = con.execute(\"select source_domains from news_clusters where sentiment is not null\").fetchall(); print(collections.Counter(len(r[0] or []) for r in rows))"

# RSS liveness probe (a feed whose newest pubDate is days old is frozen upstream — retire it):
# session scratchpad probe_feeds.py pattern; quick single-feed check:
uv run python -c "import httpx, xml.etree.ElementTree as ET; r = httpx.get('https://www.livemint.com/rss/markets', headers={'User-Agent': 'Mozilla/5.0'}, follow_redirects=True, timeout=30); print(r.status_code, [i.findtext('pubDate') for i in ET.fromstring(r.content).findall('.//item')][:3])"
```

## Gate G2 evidence (Phase 2 exit gate, §8.3)

```powershell
.venv\Scripts\python.exe scripts\g2_evidence.py --json data\reports\g2_evidence.json   # read-only state.db; safe with engine up
```
Read the SOURCES + COVERAGE CAVEATS block before quoting; RUNBOOK "Gate G2 evidence checklist" lists the owner-manual halves.

## One-off ledger repair (2026-09-03) — expired→taken entry row stuck at 'no_action'

Symptom: a recommendation /taken AFTER it expired keeps `expire_stale`'s `outcome_label='no_action'`
on its ENTRY row, and `pipeline.close()` completes only `WHERE outcome_label IS NULL`, so the real
outcome never lands. Fixed prospectively in `take()` (commit 2184a2a); rows taken before that need
this repair. Run with the engine idle (health pulses only in the log tail). Find candidates first:

```powershell
.venv\Scripts\python.exe -c "import sqlite3; c=sqlite3.connect('file:data/state.db?mode=ro', uri=True); print(c.execute(\"SELECT l.entry_id, p.symbol, l.outcome_label, l.closed_at FROM learning_ledger l JOIN positions p ON p.position_id=l.position_id WHERE p.state='OPEN' AND l.strategy_id IS NOT NULL AND l.outcome_label IS NOT NULL\").fetchall())"
# then, per entry_id, inside BEGIN IMMEDIATE with rowcount==1 asserted (script pattern: session scratchpad ledger_fix_hdfcamc.py):
#   UPDATE learning_ledger SET outcome_label=NULL, closed_at=NULL
#   WHERE entry_id=? AND outcome_label='no_action' AND exit_px IS NULL
#     AND position_id IN (SELECT position_id FROM positions WHERE state='OPEN')
```

## Universe build outside the engine (2026-09-06) — e.g. the first build after an index change

Engine OFF (single DuckDB writer); no Kite session needed (index list, margins, surveillance are public
downloads; the F&O map hydrates from the stored `instruments_daily` snapshot). Replace-writes the day's
`universe_daily` rows + `data/universe/index_cached.csv`; the scheduled 08:30 job on that date still runs
if the engine is up and rebuilds the same rows (idempotent). Exit 1 = built but DEGRADED (read the log).

```powershell
.venv\Scripts\python.exe scripts\build_universe.py --date 2026-09-07
```

## Bhavcopy archive backfill (2026-09-03) — full-market bars_1d + corp-action history, 2022-07 → 2026-07-12

Engine must be OFF (single DuckDB writer); ~1,000 sessions at ~1-2 s each, checkpointed per date in
`filings_backfill_checkpoints` (feed `bhavcopy_archive`; `corp_actions_archive` per ≤35-day window), so
it is safe to interrupt and resume across evenings. Kite-official rows are never overwritten (A11).

```powershell
.venv\Scripts\python.exe scripts\backfill_bhavcopy.py --status          # read-only progress; safe with the engine live
# FIRST (precondition for the live hi52 unadjusted-history veto, 2026-09-03): corp-action history over the
# veto's 400-day lookback — the daily job only holds windowed rows since 2026-08-14 (call-date-only before):
.venv\Scripts\python.exe scripts\backfill_bhavcopy.py --from 2025-06-01 --to 2026-08-14 --skip-bhavcopy
.venv\Scripts\python.exe scripts\backfill_bhavcopy.py                   # default 2022-07-01..2026-07-12, both legs
.venv\Scripts\python.exe scripts\backfill_bhavcopy.py --from 2024-07-01 --to 2024-12-31 --skip-corp-actions
```
Then re-run `scripts\backtest_hi52.py` (engine still off) — and note the backtest must apply the same
unadjusted-history veto the live sweep applies (`hi52.unadjusted_history` over `corp_actions`), which
the corp-actions leg makes possible for the 2022→2026 population.

## Phase 3 foundation (2026-09-10) — replay a recorded day / recalibrate the paper fill model

Both read the tick Parquet archive with their OWN in-memory DuckDB and never open `market.duckdb`,
so they are safe beside the live engine (I/O-modest; prefer compacted days).

```powershell
# Replay one recorded symbol-day through the real BarBuilder (scratch store, ULID-masked digest):
.venv\Scripts\python.exe scripts\replay_day.py --day 2026-09-09 --symbols RELIANCE [--out report.json]
# Recalibrate config\fill_model.yaml (bounded, compacted sessions only; exit 3 = nothing read, file untouched):
.venv\Scripts\python.exe scripts\calibrate_fill_model.py --parquet-root data\parquet --out config\fill_model.yaml --report data\reports\fill_model_calibration_<date> --compacted-only [--days N --symbols-per-day N]
# Phase-3 test tiers:
.venv\Scripts\python.exe -m pytest tests\unit\test_oms_state.py tests\unit\test_oms_store.py tests\property -q   # OMS core + 9.2 properties
.venv\Scripts\python.exe -m pytest tests\unit\test_paper_broker.py tests\unit\test_fill_model.py tests\unit\test_broker_surface.py -q
.venv\Scripts\python.exe -m pytest tests\replay -q                                                              # ~2 min, golden day
```
Policy pins (plan §3.2.9 / §8.4 addendum): a calibration may only tighten the fill model; the
calibrated half-spread is a PERCENT of mid floored at half a tick in the consumer.

## hi52 backtest registrations (2026-09-09) — v1 and the pre-registered v2, engine OFF

```powershell
.venv\Scripts\python.exe scripts\backtest_hi52.py --out data\reports\backtest_hi52_<date>.json                     # v1 (default)
.venv\Scripts\python.exe scripts\backtest_hi52.py --registration v2 --out data\reports\backtest_hi52_v2_<date>.json  # v2: smooth + no-gap + index population, N=2
```
v2 is a SEPARATE pre-registration (plan §6.1 hi52 addendum, thresholds fixed from the 09-03 medians),
never a knob on v1: report the two side by side, never pooled; `fold_pass_min(2)` = 60% applies to v2.
Since the 2026-09-12 promotion the v1 path scans `hi52.V1_PARAMS` (the live defaults with the three
v2 thresholds neutralized) — v1 stays reproducible while the LIVE rule gates on them.

## hi52 forward-test verdict (2026-09-12 promotion) — the kill criterion, engine OFF

```powershell
.venv\Scripts\python.exe scripts\hi52_forward_verdict.py                                        # as of today
.venv\Scripts\python.exe scripts\hi52_forward_verdict.py --as-of 2026-10-31 --json data\reports\hi52_forward_<date>.json
.venv\Scripts\python.exe scripts\hi52_forward_verdict.py --notional 8000                        # what-if only: prints OVERRIDE
```
Read-only against `state.db` + `market.duckdb`; ALWAYS exits 0 (the VERDICT line carries the answer,
`UNAVAILABLE` if a store could not be read — a running engine holds the DuckDB lock). Population =
every published `hi52` signal from 2026-09-14; entry = the OPEN of the journal day itself (= the
backtest's anchor: the journal day is the session after the trigger, so this measures the same
quantity the registered edge was measured as — the live fill lands later that morning, so realized
results trail the metric by that intraday drift), exit = the close of the k-th SESSION of the hold
(entry session first), net of one CNC round trip at the §7.1-SIZED notional
(`swing_position_pct` / (`overnight_gap_mult` × the 6% stop) = equity/7.5, taken at the SMALLEST
`equity_snapshots` reading in the window — ₹5,234 ⇒ 0.54% at the 2026-09-11 equity, and a
drawn-down snapshot anywhere in the window gives a smaller notional and a HIGHER floor, which can
only make DEMOTE easier; `--equity` overrides the equity and `--notional` skips the derivation
outright and prints OVERRIDE, and the derivation prints with the verdict either way, so check which
notional and floor it used before quoting a number). T+5 and T+10 print as DIAGNOSTICS and are
labelled on their own rows — only T+20 votes. The registered `hi52.expected_edge_pct` (1.47) comes
from the SAME derivation in this script (`registered_edge_pct()`, charged at the `equity_floor_rung`
book) and a unit test pins the two together, so re-deriving by hand is never necessary.
The 2026-09-14 boundary is an
ASSUMPTION about the deploy date, printed as such — pass `--from` if the tranche shipped later. **Rule: DEMOTE if n ≥ 20 AND
(median net at T+20 ≤ 0 OR hit rate at T+20 < 50%); HOLD otherwise; INSUFFICIENT below 20.** Run it
at 20 and at 40 signals (plan §8.6 "hi52 KILL CRITERION"). A DEMOTE is two edits in one commit:
re-add `hi52` to `NO_EDGE_SHADOW_STRATEGIES` **and** delete `hi52.expected_edge_pct` from
`settings.yaml`. First-20-signal outcomes are validation, not income — nothing else moves in
response to them.

## brk20 entry-mechanism backtest (R1, 2026-09-12) — three registered entry variants, engine OFF

```powershell
.venv\Scripts\python.exe scripts\backtest_brk20.py --out data\reports\backtest_brk20_<date>.json --verify-window 25
```
Writes the `.md` report beside the JSON, same stem. `--verify-window K` re-scans the first K symbols
with the full row prefix and aborts on any disagreement with the bounded scan window — cheap, so run
it. The population is the CURRENT eligible universe read from `universe_daily` and applied backwards
(survivorship-tainted proxy); an empty `universe_daily` is a refusal, not a degraded run. `--symbols`
OVERRIDES that population and stamps the document as a smoke run — never quote a `--symbols` run as
the registered study. Trial count is fixed at N=3 (V1 next-open, V2 limit-at-H20 within 3 and within
5 sessions); there is deliberately no fill-window or rule-parameter flag, because one would turn N=3
into N=k silently.

Re-run 2026-09-12 12:02 with the audit corrections (same command, same data, every headline number
reproduced to 4 dp). What the artifacts now carry, and how to read them:

- **The three variants share a SIGNAL population but NOT a trade set** (8,292 / 4,230 / 4,898 trades):
  a V2 limit fills only when price returns to the level. Every pooled V1-vs-V2 number is therefore a
  comparison PER FILLED TRADE across two different event sets. The old "one shared population … same
  event set" line is gone from the report; do not reintroduce it.
- **STEP 3B / `matched_cohorts`** is the decomposition that makes the mechanisms comparable: V1
  re-quoted on exactly the cohort each V2 variant filled (the entry **PRICE** effect) and on the
  cohort it never filled (the **SELECTION** effect), with n, median/mean gross and net and hit rate
  per horizon per cell, plus the same split per margin tercile. Descriptive, not a fourth trial.
- **Every cell prints its own fill rate** (`n_signals_in_cell` / `fill_rate_in_cell`). An "in every
  margin tercile" claim is only readable off `v2_beats_matched_v1_in_every_margin_tercile`, never off
  the unmatched pooled rows.
- **Reported promotability is CPCV AND geometry** (2026-09-12 amendment): a cell that passes the
  mean-based CPCV gate while its MEDIAN net is ≤ 0 prints `GATES DISAGREE` and reads NOT promotable.
  Both decision-rule outcomes are printed — the registered gate (`decision`) and the tightened one
  (`decision_under_tightened_reporting_rule`) — with an explicit "does the tightening change the
  registered outcome?" line. On the 2026-09-12 run it does not.
