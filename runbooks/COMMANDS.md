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

## Deploy a reviewed tip — outside 09:15–15:30 only

```powershell
Get-Date; Get-Content data\logs\engine.log -Tail 20          # no in-flight EOD job / compaction
git status --short       # an untracked file the tip adds aborts the ff-merge: remove strays first
Stop-Service mt-engine
# 1. Backup before the first boot on a new migration (online backup API, as _snapshot_backup does);
#    replace <NNNN> with the next migration number (check `Get-ChildItem data/backups -Filter pre*`).
#    An existing file of the same name is OVERWRITTEN:
.venv\Scripts\python.exe -c "import sqlite3; s=sqlite3.connect('data/state.db'); d=sqlite3.connect('data/backups/pre_<NNNN>.db'); s.backup(d); d.close(); s.close()"
# 2. Fast-forward the live tree to the reviewed tip (stop here if it aborts; the engine stays down):
git merge --ff-only <reviewed tip>
# 3. Optional, same stop: a research snapshot with the new tip's table list (copies the study tables READ_ONLY,
#    prints row counts, exit 0 = all match)
.venv\Scripts\python.exe scripts\research_snapshot.py
# 4. Rebuild the dashboard when the tip touches dashboard\src (dist is not in git; served from disk, no restart):
cd dashboard; npm run build; cd ..
Start-Service mt-engine
Select-String -Path data\logs\engine.log -Pattern 'migrations_applied_on_boot|migration_failed|engine_ready|FROZEN' | Select-Object -Last 5
```

Rollback (in this order; the restart comes last):
1. Commit an uncommitted `config/limits.yaml` first: it is a dashboard capital change (O19), signed in state.db.
2. Note the tip you are leaving: `$from = git rev-parse HEAD`.
3. `git reset --hard <previous tip>`. Do NOT restart yet. Migrations are additive; never drop columns.
4. Check whether `config/limits.yaml` differs between the two tips:
   `git diff --quiet <previous tip> $from -- config/limits.yaml` (exit 1 means it differs).
   - If it differs, put the signed bytes back: `git checkout $from -- config/limits.yaml`, then commit them
     (`git commit -m "..." -- config/limits.yaml`). Skip this and the boot FREEZES, or KILLS with a real
     position open (see "Capital change from the dashboard").
   - This keeps the capital as signed. To take the capital back too, reseed after the restart ("Protected config").
   - Below the review-actions tip (2026-10-29), older code requires `analyst_confidence_min`, which `f1a9e42`
     removed, and fails at boot without it. After the commit above, `git revert --no-commit f1a9e42` puts the key
     back on top of the signed bytes; have the owner reseed before the restart.
5. Restart.

A failed migration is recovered from `data\backups\pre_00NN.db`. Below the paper autopilot (M3) only while no `is_paper=1` row exists
in orders, positions, verdicts, gtts or learning_ledger (older code counts paper rows as real money; query in
RUNBOOK "Rollback floor"); otherwise paper OFF or `paper.subsystem_enabled: false` and a restart.

## Service logs (NSSM mode)

```powershell
Get-Content data\logs\engine.log -Tail 50 -Wait        # structured log (all launch modes) — primary
Get-Content data\logs\service.err.log -Tail 50 -Wait   # NSSM-captured stderr: WARNING+ lines, raw tracebacks, early-boot crashes (pre-2026-10-04 non-JSON lines: service-nonjson-archive.log)
Get-Content data\logs\service.out.log -Tail 50         # NSSM-captured stdout: stray prints
Get-WinEvent -ProviderName nssm -MaxEvents 20 | Format-Table TimeCreated, Message -Wrap  # start/stop/crash-restart/throttle
scripts\nssm_install.ps1 -Action status                # service config incl. ObjectName + log paths
```

NSSM's stop grace is `AppStopMethodConsole 30000` (its 1.5 s default killed the engine mid-shutdown, so every stop
recorded as a crash); check it with `nssm get mt-engine AppStopMethodConsole` after any service reinstall.

## Re-login of the engine's Claude CLI (2026-10-11) — after an `auth_failed` alert or a `claude_login` WARN

Run as the service user `piyush.kumar_atari` (the engine uses the pinned CLI's stored claude.ai login,
shared with the owner's interactive Claude Code). `claude_login_check` (08:10 IST)
warns from 7 days before `refreshTokenExpiresAt`; next hard expiry 2026-11-08 05:03 IST.

```powershell
& "C:\Users\piyush.kumar_atari\projects\personal\market_trading\data\run\claude-cli\2.1.288\claude.exe" auth login --claudeai
& "C:\Users\piyush.kumar_atari\projects\personal\market_trading\data\run\claude-cli\2.1.288\claude.exe" auth status --json   # verify: logged in
```

## DuckDB restore drill (quarterly) — safe with the engine running

Reads only the newest `data\backups\market-*` export; needs ~5.7 GB free in `%TEMP%`, takes ~5 min.
Run from the repo root.

```powershell
$snap = Get-ChildItem data\backups -Directory -Filter 'market-*' | Where-Object { $_.Name -notlike '*.tmp' } | Sort-Object Name | Select-Object -Last 1
$scratch = "$env:TEMP\mt_restore_drill.duckdb"; if (Test-Path $scratch) { Remove-Item $scratch }
.venv\Scripts\python.exe -c "import duckdb,sys; c=duckdb.connect(sys.argv[2]); c.execute('SET memory_limit=' + chr(39) + '8GB' + chr(39)); c.execute('IMPORT DATABASE ' + chr(39) + sys.argv[1].replace(chr(92), '/') + chr(39)); [print(t, c.execute('SELECT count(*) FROM ' + t).fetchone()[0]) for (t,) in c.execute('SELECT table_name FROM duckdb_tables() ORDER BY 1').fetchall()]" $snap.FullName $scratch
```
Compare the printed counts with the live store through `POST /db/query` (`SELECT count(*) FROM <table>`);
rows written after the export's stamp are expected to differ. Then `Remove-Item $scratch`. The real restore
(engine stopped or crashed; IMPORT + `scripts\rewind_after_market_restore.py`) is in RUNBOOK "Restore
market.duckdb from backup".

## Universe symbol list (the canonical 200-name set for historical runs)

```powershell
$syms = ((Get-Content data\reports\orb_sweep_20260712T033313.json | ConvertFrom-Json).symbols) -join ','
```

## Backtests (sweep + CPCV validation)

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

**Sweep mechanics (WO-M, 2026-09-13).** Every sweep artifact now carries a one-line `Mechanics:`
stamp — `stops=intrabar_ohlc · stop_exit_price=stopmarket · expectancy=closed_trades_only ·
fills=next_bar_open` — and a validation artifact carries the same stamp **when its returns came out
of the sweep**. Read the absence of a stamp differently on the two:

- **A sweep artifact** (`*_sweep_*.md/json`) with no stamp, or the words `PRE-2026-09-13
  (unstamped)`, came off the pre-fix mechanics — daily stops decided on the close and filled there,
  stop exits charged no spread, and open positions inside the ranked per-trade expectancy, all three
  in the strategy's favour. Its numbers are **not** comparable term-by-term with a stamped one and
  must never be pooled with them. `SweepRunner` always stamps, so on this side absence *is* pre-fix.
- **A validation artifact** (`<strat>_<ts>.md/json`) with no `Sweep mechanics:` line means "pre-fix
  sweep **or** no sweep at all" — `ValidationPipeline` also validates return series that never went
  through `SweepRunner` (the event-study harnesses, e.g. `scripts\validate_insider.py`, build their
  own series and charge their own fills/costs, so none of the three sweep settings applies to them).
  Never read an unstamped verdict as an assertion that the three pre-fix biases are in it; check
  whether a `*_sweep_*` artifact of the same run exists before concluding anything.

A verdict whose params the grid never ranked carries a **THESE PARAMS ARE NOT A GRID WINNER** banner
(and `params_are_grid_winner: false` in the JSON) — that is the §6.3 envelope DEFAULTS being
validated because no config closed a round trip; it is not a swept result. The `expectancy (CLOSED)`
column is the ranked/promotion statistic; `expectancy (all)` is the pre-fix headline, kept beside it,
and `win% (CLOSED)` is over the same round trips as `expectancy (CLOSED)`. In the persisted
`param_sets.validation_report`, the pre-fix key names `sweep_expectancy_pct` and `win_rate` are
RETIRED rather than redefined — a query for them returns pre-2026-09-13 rows only; the post-fix rows
carry `sweep_expectancy_closed_pct` / `sweep_expectancy_all_pct` / `win_rate_closed` / `win_rate_all`.
`orb` is parked (0/15 twice); its leg costs ≈ 90 min.

## Event study (proxy + filings legs)

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
# Integrated Filing results listing only (every result since the Mar-2025 quarter; ~2.5 min, engine OFF).
# Run 2026-09-24 from 2025-01-01: 23,779 filings. Quarterly revenue/PAT then fill nightly via the
# results_line_items job (19:30, <=300 XBRL fetches per run) — no backfill command needed for them.
uv run python scripts\backfill_filings.py seed --from 2025-01-01 --skip-pit --skip-results --skip-shp
# SHP history for the 298 names the daily isin_map job first mapped on 2026-09-24 (none has an SHP
# row; filings_shp had been skipping them since 09-04). Per-symbol checkpoints skip the 199 already
# seeded. Engine OFF; expect ~1.5x the 3.5 h the 199-name seed took, so a weekend job (resumable).
uv run python scripts\backfill_filings.py seed --from 2023-07-01 --skip-pit --skip-results --skip-integrated
# NSE insider trades when the daily filings_pit window was clamped (its warning names the uncovered
# days). Days from 2026-05-03 come from the PIT V2.0 route and checkpoint as `pit_gg`; earlier days
# from the old route. Engine OFF. The 2026-09-24 live catch-up (04-25 -> 09-24) needed no backfill:
# filings_pit's own run covered it (1,769 rows, ~22 min).
uv run python scripts\backfill_filings.py seed --from 2026-05-03 --skip-results --skip-integrated --skip-shp
```

## Protected config (after ANY owner-directed edit to limits.yaml / envelope.yaml)

```powershell
uv run python scripts\seed_protected_config.py --yes --reseed --note "<why, citing the directive>"
```

This flow is not for a mismatch you did not cause. If the boot's `protected_store` check FAILs after a stash,
checkout, restore or rollback reverted a dashboard capital change, a reseed would sign the old bytes and silently
undo the change. Use the restore recipe under "Capital change from the dashboard" below.

The flow is owner-only by design (R4): the platform PREPARES the edit and hands it over, and runs the reseed only
on the owner's explicit instruction. If a reseed errors, do NOT start the engine: `git checkout --
config/envelope.yaml` and re-run it. A later clean boot clears a `protected_store_integrity` freeze by itself
(lifecycle step 1c), but an unsigned file still FREEZES the boot it is found on. A prepared change follows this
shape (outside 09:15-15:30, from the repo root, with a clean `git status`; apply it inside the repo, because
`config/*.yaml` is `eol=lf` and a CRLF copy changes the hash):

```powershell
Stop-Service mt-engine
git apply --check <patch>                                        # must print nothing
git apply <patch>
.venv\Scripts\python.exe scripts\seed_protected_config.py --yes --reseed --note "<why, citing the directive>"
Start-Service mt-engine                                          # boot must show selftest protected_store:<file> PASS
git add -u; git commit -m "<directive> applied (owner): patch + reseed"
```

## Capital change from the dashboard (O19, 2026-10-16)

The dashboard's "Change capital" button (System tab, Risk limits card) rewrites three `config/limits.yaml`
lines and re-signs the file in one step. The lines are `capital_base_inr`, `max_deployed_capital_inr` and
`cnc_notional_inr`; the two caps scale with the base. Nothing needs reseeding afterwards. It leaves
`config/limits.yaml` **modified in git**, so commit it on its own before any other git operation:

```powershell
git diff --stat -- config/limits.yaml                            # expect: 1 file, 3 lines changed
git commit -m "Capital <old> -> <new> via the dashboard (O19, owner)" -- config/limits.yaml   # commits this file only
```

Never `git stash`, `git checkout -- config/limits.yaml`, `git restore`, `git reset --hard` or an editor's
"discard changes" while that change is uncommitted. Roll back below a capital-change commit only as the
Rollback note at the top says. Each of these puts the old bytes back under the new signature, and the next boot
fails `protected_store_integrity`:
- with no real position open, it FREEZES;
- with one open, it KILLS: it flattens, and the kill stays until `/kill_reset` + `/confirm`.

Do not reseed to "fix" it: a reseed signs the old bytes and silently undoes the capital change. Put the signed
bytes back instead. The tool never signs anything; it writes back only the snapshot that already hashes to the
recorded signature:

```powershell
.venv\Scripts\python.exe scripts\protected_config_status.py                          # read-only; exit 0 OK, 1 mismatch, 2 error
Stop-Service mt-engine
.venv\Scripts\python.exe scripts\protected_config_status.py --restore limits.yaml    # the signed bytes go back on disk
Start-Service mt-engine                                          # boot must show selftest protected_store:limits.yaml PASS
# A boot that KILLED (protected_store_integrity_live_book) stays killed after the restore: /kill_reset, then /confirm.
git commit -m "Capital <old> -> <new> via the dashboard (O19, owner)" -- config/limits.yaml   # the restored change is uncommitted again
```

The reseed flow above needs a clean `git status`. After a dashboard capital change, commit `limits.yaml`
first. A reseed that swept the change into an unrelated commit would hide it.

## Tests / verification

The gate is a bare `pytest` (`testpaths`: unit + chaos + replay + property): everything the engine process and its
scheduled tasks run, and the operational tools that write live stores or config (bhavcopy backfill, the restore
rewind, protected-config status, fill-model calibration). `tests/offline` holds the tests of NON-production code —
research, backtest, verdict and one-off scripts, and the `engine.learning` modules only they reach — and runs on
demand: after changing one of those, or `scripts/backtest_common.py`. A test that pins production behaviour belongs
in the gate even when it goes through a script. A pytest process starts in ~4 s (scikit-learn loads only when
numba/vectorbt/cvxpy/skfolio is imported); the vectorbt tests (`needs_heavy_deps`, ~24 s of JIT per process) are
all offline. Put several files in ONE run rather than looping over them.

```powershell
# Inner loop: one file or a few, in one run
uv run pytest tests/unit/test_filings_feeds.py tests/unit/test_prescreen.py -q
# Any run can skip the native-stack (numba / vectorbt) tests
uv run pytest tests/offline -q -m "not needs_heavy_deps"
# The gate in parallel (pytest-xdist: 8 workers, each takes whole files) — run inline, never two gates at once
uv run pytest -n 8 --dist loadfile -q
# The gate serial, and its tiers on their own
uv run pytest -q
uv run pytest tests/unit -q
uv run pytest -m chaos -q
uv run pytest tests/replay -q
uv run pytest tests/property -q
# The offline tier
uv run pytest tests/offline -n 8 --dist loadfile -q
cd dashboard; npm test; cd ..
```

Optional host speed-up — an owner/IT decision, from an ELEVATED PowerShell (the audit did not apply it). Defender
real-time scanning costs ~2 ms per temp file and ~0.5 ms per `stat` here. The narrowest useful exclusion is the venv
(read on every import) and pytest's temp root; never `data/`, which holds downloaded third-party files. A managed
laptop may refuse local exclusions (tamper protection / Intune), and an excluded folder is not scanned — weigh that.

```powershell
Add-MpPreference -ExclusionPath 'C:\Users\piyush.kumar_atari\projects\personal\market_trading\.venv', 'C:\Users\piyush.kumar_atari\AppData\Local\Temp\pytest-of-piyush.kumar_atari'
Get-MpPreference | Select-Object -ExpandProperty ExclusionPath     # confirm; then time the gate again
Remove-MpPreference -ExclusionPath 'C:\Users\piyush.kumar_atari\projects\personal\market_trading\.venv', 'C:\Users\piyush.kumar_atari\AppData\Local\Temp\pytest-of-piyush.kumar_atari'   # undo
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

## Engine lock etiquette

- Engine holds `data/market.duckdb` read-write for its lifetime; check before long runs:
  `Get-CimInstance Win32_Process -Filter "Name = 'python.exe'" | Where-Object { $_.CommandLine -match 'engine.ops.main' }`
- A backtest/backfill blocks engine startup for its duration — schedule long runs outside
  trading/EOD-job hours.

## Dashboard and Telegram surfaces

```powershell
# Dashboard build (served by the engine at / from dashboard/dist; live on the next page load, no restart):
cd dashboard; npm install; npm run build; cd ..
# Dashboard unit tests (pure modules, Node's own runner; 2026-10-08):
cd dashboard; npm test; cd ..
# Dashboard preview WITHOUT the engine (dev only, 2026-09-08): dist + canned rows across several IST
# days at http://127.0.0.1:8499/, any token passes. Scenarios (one per process; $env:PORT picks the port):
# NO_TODAY (no ledger rows today), PAPER_OFF, PAPER_IDLE, ALERTS (the loud state), EMPTY (first boot).
cd dashboard; node fixture_server.mjs; cd ..
$env:PORT='8497'; $env:ALERTS='1'; node dashboard\fixture_server.mjs
# Layout/contrast sweep: paste dashboard\tests\dom-checks.js into the page console (README "Tests").
```
- RECOMMEND flow needs: mode RECOMMEND (`/mode RECOMMEND`), valid trade window, warm-up ready,
  and a live Claude CLI login (no code gates on it: a dead login makes every LLM call fail with
  `auth_failed` while the scanners keep running; re-login steps above).
- **Budget window is the subscription's QUOTA WEEK (2026-09-12):** Thursday 14:00 IST → next Thursday
  14:00 IST (`llm.quota_window` in agents.yaml; `weekly_credit_usd` + weekly per-agent allocations).
  `/budget` (Telegram or `GET /budget`) prints the window key (the start Thursday's date), the
  window spend per agent vs allocation, the tier and the live forward cap. DG1 trips on PACE only;
  an agent past 85% of its own allocation degrades only itself (cap = 0.67 × base), at 100% it is
  hard-stopped. A change to agents.yaml applies on the next engine boot — validate with the loader
  one-liner above first. The month column in `budget_ledger` is written but read by nothing.
- Owner outcome capture: `/taken <rec_id> <qty> <price>`, `/closed <rec_id> <price>`, `/veto <rec_id>`.
- `/rearm` (or the dashboard alert's Re-arm, `POST /risk/rearm`, 2026-10-08): lifts each floor stop
  (`floor_weekly_drawdown`, `floor_equity_floor_rung`, `floor_cumulative_floor`) the engine's book is back
  above; one still breached stays latched. A kill switch still needs `/kill_reset` + `/confirm`, and the
  mode a floor forced down stays where it is.
- `/protected <symbol|rec_id>`: confirm the stop order for a taken position. Until then `/positions`
  shows "UNPROTECTED (unconfirmed)" and `protection_reminder_tick` (300 s) sends at most two critical
  reminders (30 min after `/taken`, deferred to 08:00 if outside 08:00-22:00; again once the next
  session opens). Reminders stop on `/protected`, a close, or a zero holdings journal.
- `/why <symbol>`: read-only report (universe status, hi52/brk20 distance to trigger, surveillance and
  results-day flags, latest insider/pledge filings, last rec, open positions, last verdict). Each
  DuckDB read times out at 5 s and prints "<section>: unavailable"; no LLM, no `/scan_now`.
- `/paper on|off|status|reset`: paper autopilot control. `on` and `reset` need `/confirm <phrase>` and are
  refused while an unexpired challenge is pending; `on` also needs `paper.subsystem_enabled: true` in
  settings.yaml. `off`: no new entries; resting paper entry orders are cancelled within one paper tick in
  session (≤30 s; outside it they lapse at the next open; a partial fill keeps its filled part); exits and GTTs
  continue. The dashboard's "Paper autopilot" switch in the top bar
  (`POST /paper`) sets the same flag single-step; `GET /paper` returns control state and the paper book.

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

## Universe build outside the engine (2026-09-06) — e.g. the first build after an index change

Engine OFF (single DuckDB writer); no Kite session needed (index list, margins, surveillance are public
downloads; the F&O map hydrates from the stored `instruments_daily` snapshot). Replace-writes the day's
`universe_daily` rows + `data/universe/index_cached.csv`; the scheduled 08:30 job on that date still runs
if the engine is up and rebuilds the same rows (idempotent). Exit 1 = built but DEGRADED (read the log).

```powershell
.venv\Scripts\python.exe scripts\build_universe.py --date 2026-09-07
```

## Symbol rename: move a renamed symbol's history (2026-10-26)

After a "Symbol renamed: OLD → NEW" alert from the universe build (NSE renamed the symbol, Kite kept the
token). Engine OFF: the script holds `engine.lock` for the whole run, and even the dry run opens DuckDB. The
dry run executes the move and rolls it back, so its counts are what `--apply` commits. `--apply` writes an
undo copy to `data\backups\rekey-OLD-NEW-<stamp>\` first. It is refused while a live position names OLD, or
while OLD has a BSE insider row younger than 3 days (the message gives the re-run date).

```powershell
.venv\Scripts\python.exe scripts\rekey_symbol.py ICICIPRULI ICICILIFE           # dry run
.venv\Scripts\python.exe scripts\rekey_symbol.py ICICIPRULI ICICILIFE --apply
# rehearse on a copy: $env:MT_DATA_DIR = '<dir holding market.duckdb + state.db>'; then unset it
```

## Bhavcopy archive backfill — full-market bars_1d + corp-action history

Engine must be OFF (single DuckDB writer); ~1-2 s per session, checkpointed per date in
`filings_backfill_checkpoints` (feed `bhavcopy_archive`; `corp_actions_archive` per ≤35-day window), so
it is safe to interrupt and resume across evenings. Kite-official rows are never overwritten (A11).

```powershell
.venv\Scripts\python.exe scripts\backfill_bhavcopy.py --status          # read-only progress; safe with the engine live
.venv\Scripts\python.exe scripts\backfill_bhavcopy.py --from <YYYY-MM-DD> --to <YYYY-MM-DD> [--skip-corp-actions | --skip-bhavcopy]
```

**Muhurat session 2026-11-08 (Sunday) — run after the day, any evening from 2026-11-08 night, engine OFF**
(the live engine treats special sessions as closed, so its own jobs skip the day; plan NSECalendar
special-sessions amendment):

```powershell
.venv\Scripts\python.exe scripts\backfill_bhavcopy.py --from 2026-11-08 --to 2026-11-08 --skip-corp-actions
# verify (read-only): the run's own summary line, then the unit's checkpoint row and the bars it wrote
#   state.db:      SELECT through_date FROM filings_backfill_checkpoints WHERE feed='bhavcopy_archive' AND unit='2026-11-08'
#   market.duckdb: SELECT count(*) FROM bars_1d WHERE d='2026-11-08'  -- comparable to 2025-10-21 (2296 rows); 0 = treated as a holiday
# (--status prints only totals and the first/last unit, so it cannot tell an ingested unit from a 'holiday' one)
```
`--skip-corp-actions` because that leg would otherwise sweep 400 days before `--from` in new window units
(the live 20:15 `corp_actions` job already covers that window). If the archive 404s on both formats the unit
is checkpointed `holiday`: delete that row (`filings_backfill_checkpoints` feed=`bhavcopy_archive`,
unit=`2026-11-08`) and retry a day later.

## Replay a recorded day / recalibrate the paper fill model

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

## hi52 backtest registrations — v1 and the pre-registered v2, engine OFF

```powershell
.venv\Scripts\python.exe scripts\backtest_hi52.py --out data\reports\backtest_hi52_<date>.json                     # v1 (default)
.venv\Scripts\python.exe scripts\backtest_hi52.py --registration v2 --out data\reports\backtest_hi52_v2_<date>.json  # v2: smooth + no-gap + index population, N=2
```
v2 is a SEPARATE pre-registration (thresholds fixed from the 09-03 medians),
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
(`swing_position_pct` / (`overnight_gap_mult` × the 6% stop) = equity/6 since O17 (2.0×; equity/7.5 at 2.5× before 2026-09-24), taken at the
SMALLEST `equity_snapshots` reading in the window — ≈₹6,543 at the 2026-09-11 equity (₹39,256.65 / 6), and a
drawn-down snapshot anywhere in the window gives a smaller notional and a HIGHER floor, which can
only make DEMOTE easier; `--equity` overrides the equity and `--notional` skips the derivation
outright and prints OVERRIDE, and the derivation prints with the verdict either way, so check which
notional and floor it used before quoting a number). T+5 and T+10 print as DIAGNOSTICS and are
labelled on their own rows — only T+20 votes. The registered `hi52.expected_edge_pct` (1.53 since O17; 1.47 before) comes
from the SAME derivation in this script (`registered_edge_pct()`, charged at the `equity_floor_rung`
book) and a unit test pins the two together, so re-deriving by hand is never necessary.
The 2026-09-14 boundary is an
ASSUMPTION about the deploy date, printed as such — pass `--from` if the tranche shipped later. **Rule: DEMOTE if n ≥ 20 AND
(median net at T+20 ≤ 0 OR hit rate at T+20 < 50%); HOLD otherwise; INSUFFICIENT below 20.** Run it
at 20 and at 40 signals. A DEMOTE is two edits in one commit:
re-add `hi52` to `NO_EDGE_SHADOW_STRATEGIES` **and** delete `hi52.expected_edge_pct` from
`settings.yaml`. First-20-signal outcomes are validation, not income — nothing else moves in
response to them.

## C5 no-stop verdict (D14, 2026-10-15) — read-only, safe with the engine running

```powershell
.venv\Scripts\python.exe scripts\no_stop_verdict.py                                    # as of today -> data\reports\no_stop_verdict_<date>.json
.venv\Scripts\python.exe scripts\no_stop_verdict.py --market-db data\market.duckdb    # adds the paper counterfactual: engine OFF only
```

- **DECISION:** the signal-level verdict over `rec_outcomes` entry recs of hi52, ins and brk20 that are closed
  under the shipped rule and have a T+20 no-stop outcome. It is pinned to two looks: look 1 = the first 20 filled
  recs by fill date, formed once all 20 are eligible; look 2 = the first 40, which supersedes look 1 (correction
  2026-10-16). Until look 1 forms it reads INSUFFICIENT. Otherwise:
  - NO-STOP SUPPORTED if D > 0 and E > 0;
  - STOP NOT THE PROBLEM if D ≤ 0;
  - NO EDGE EITHER WAY if D > 0 and E ≤ 0.

  E = median `excess_t20`; D = median of `net_t20 − net_pct`.
- **Decide nothing alone:** the `running` line (any other n), the confirmation slice (fills from 2026-10-16) and
  the paper (P) section.
- **Restarts:** the horizon or strategy set of the decision restarts the test; `paper.no_stop_strategies`
  restarts only (P).
- **Exit status:** always exits 0. It reads `rec_outcomes` as stored; `--as-of` bounds only the (P) walk.

## Forward verdicts (brk20, ins, analyst) — read-only, safe with the engine running

```powershell
.venv\Scripts\python.exe scripts\forward_verdicts.py --rule brk20
.venv\Scripts\python.exe scripts\forward_verdicts.py --rule ins
.venv\Scripts\python.exe scripts\forward_verdicts.py --rule analyst
# optional: --as-of YYYY-MM-DD (default today IST), --db <state.db> (default data\state.db), --json <path> (also writes the reading)
# the VERDICT line prints first; populations start at R = REGISTERED_FROM (2026-11-02), so INSUFFICIENT until a look
# forms (UNREGISTERED only with REGISTERED_FROM = None); UNAVAILABLE on a DB error;
# exit 0 always; the rows holding an unformed look are listed
```

## Signal outcomes row count — read-only, safe with the engine running

```powershell
.venv\Scripts\python.exe -c "import sqlite3; c=sqlite3.connect('file:data/state.db?mode=ro', uri=True); print(c.execute('select construct, status, count(*) from signal_outcomes group by construct, status order by construct, status').fetchall())"
```

## LLM scorecard — read-only

```powershell
# market data: a research snapshot that includes catalyst_watchlist, made with the engine stopped
# ("Deploy a reviewed tip" step 2; scripts\research_snapshot.py)
.venv\Scripts\python.exe scripts\llm_scorecard.py
# optional: --state-db <state.db> (default data\state.db), --market-db <snapshot> (default the newest data\research\market_*.duckdb),
#           --as-of YYYY-MM-DD (default today IST), --out <json path>
# writes data\reports\llm_scorecard_<as-of>.json and the .md beside it; every section reads "interim, decides nothing"
```

## Market-wide pause study (registered 2026-10-29) — read-only on a snapshot, outside 09:15–15:30

```powershell
$snap = (Get-ChildItem data\research\market_*.duckdb | Sort-Object Name -Descending | Select-Object -First 1).FullName
.venv\Scripts\python.exe scripts\backtest_market_pause.py --db $snap
# optional: --end YYYY-MM-DD, --out <json path>; --symbols / --max-symbols are smoke runs only
# refuses (exit 2) while REGISTERED is None, and during market hours on a trading day
```

A SUPPORTED result goes to the owner and wires nothing.

## Replay smoke after a prompt change — outside market hours (it spends quota)

```powershell
# from the worktree that holds the change; the venv imports the LIVE tree unless PYTHONPATH points here
$env:PYTHONPATH = '<worktree>\src'
.venv\Scripts\python.exe -c "import engine; print(engine.__file__)"   # must print a path under the worktree
$tr = "$HOME\.claude\projects\C--Users-piyush-kumar-atari-projects-personal-market-trading"
# a worktree's data\ holds no pinned CLI (--cli-path overrides only the candidate): junction the live one in,
# and rmdir the junction before removing the worktree
New-Item -ItemType Directory -Force data\run | Out-Null
cmd /c mklink /J data\run\claude-cli C:\Users\piyush.kumar_atari\projects\personal\market_trading\data\run\claude-cli
.venv\Scripts\python.exe scripts\replay_agents.py sample --intraday 3 --preopen 2 --transcripts-dir $tr   # prints the samples file
.venv\Scripts\python.exe scripts\replay_agents.py run --samples <samples file> --baseline --transcripts-dir $tr
# run refuses in market hours on a trading day (--force overrides; do not); results in data\reports\replays\<run>\
```

Judge the run on `schema_invalid` in the REPLAYED outputs. Old live answers that carry `regime_note` show as
live-column parser rejections; that is expected.

## cat v2 shadow verdict (WO-18, clock restarted 2026-08-28) — engine OFF

```powershell
.venv\Scripts\python.exe scripts\cat_shadow_verdict.py                                   # as of today
.venv\Scripts\python.exe scripts\cat_shadow_verdict.py --as-of 2026-12-15 --json data\reports\cat_shadow_<date>.json
.venv\Scripts\python.exe scripts\cat_shadow_verdict.py --notional 8000                   # what-if only: prints OVERRIDE
```
Read-only against `state.db` + `market.duckdb` (a running engine holds the DuckDB lock: `UNAVAILABLE`);
always exits 0; `--as-of` bounds both stores (config files are read as they are today). The rule and every
convention are in the script's docstring.
- **Population:** published `cat` signals from 2026-08-29; 2026-08-18..28 is always excluded. Journal
  symbols follow NSE renames, whether or not `scripts\rekey_symbol.py` ran.
- **Measure:** entry = open of the journal day when the story was first seen before 09:15 IST that day,
  else the next session's open; exit = close of the k-th session from the entry; net of one CNC round
  trip at the §7.1-sized notional with `cat.stop_pct` (5%).
- **Verdict:** INSUFFICIENT until ≥30 sessions, ≥20 admitted signals and a fully measured cohort (the
  first 20 signals by day and symbol that are neither void nor duplicate; a store hole holds it). Then
  **RETIRE** iff the cohort median net is ≤ cost × H / 20 at BOTH T+10 and T+20 (the WO-3 margin
  floor), else **OWNER REVIEW**. The report changes nothing either way.
- **Voids and duplicates:** a bonus/split/rights/demerger ex-date inside a signal's hold voids it (stored
  bars are never re-adjusted); a signal sharing a news cluster with an earlier signal of the same symbol is
  the same story twice. Both are listed and leave the cohort.
- **Decide nothing:** the running cells, the O15 / sonnet-5.5 scorer segments, and the excess over the
  equal-weight benchmark.
- After a verdict, the held work in IMPLEMENTATION_PLAN §3 ("Waiting on the `cat` v2 verdict") is due.

## brk20 entry-mechanism backtest (R1) — three registered entry variants, engine OFF

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

How to read the artifacts:

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
  registered outcome?" line.

## Sector map rebuild outside the engine - after a classification change, engine OFF

The `sector_map` job is Sunday-only, so a new override or a classifier change otherwise waits a week. Same
`SectorMapJob` the engine wires, same store.

```powershell
Stop-Service mt-engine                                   # single-writer DuckDB
.venv\Scripts\python.exe scripts\run_sector_map.py       # as_of today; prints the bucket counts
Start-Service mt-engine
```

`--dry-run` reports the current snapshot without writing.
