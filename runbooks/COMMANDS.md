# COMMANDS — recurring command reference

Recorded per the 2026-07-17 owner directive so recurring tasks need no re-discovery. All commands
run from the repo root in **PowerShell** (NOT cmd.exe — `$syms` expansion silently fails there;
the backtest CLI now hard-errors on it). The market store (DuckDB) allows ONE writer: the engine,
a backtest, or a backfill — never concurrently.

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
