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
```
Reports → `data/reports/<strat>_<ts>.md` (+ sweep). ORB leg ≈ 90 min on the full 498-session window.

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
# Phase-2 test slices:
uv run pytest tests/unit/test_risk_gate.py tests/unit/test_reco_pipeline.py -q     # gate + pipeline
uv run pytest tests/unit/test_budget_governor.py tests/unit/test_agent_harness.py -q  # LLM plumbing
uv run pytest tests/unit/test_catalyst_digest.py tests/unit/test_news_scoring.py -q   # news layer
```
- RECOMMEND flow needs: mode RECOMMEND (`/mode RECOMMEND`), valid trade window, warm-up ready,
  and the Claude OAuth token present (else the LLM tier is disabled and only scanners run).
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
