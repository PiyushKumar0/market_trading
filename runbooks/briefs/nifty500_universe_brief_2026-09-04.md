# Brief — eligible universe → NIFTY 500 (O15, owner-directed 2026-09-04)

Design authority: IMPLEMENTATION_PLAN.md §6.1, paragraph "**O15 — eligible universe widened to
NIFTY 500**" (immediately before the `tdc` pre-registration paragraph). Read it first; binding.

## Scope (exactly this)
1. **Config** (`src/engine/core/config.py`, `UniverseCfg` ~line 419; `config/settings.yaml` universe
   block): replace `nifty200_source_url` / `nifty200_seed_path` with `index_name: str = "NIFTY 500"`,
   `index_source_url` (default `https://archives.nseindia.com/content/indices/ind_nifty500list.csv`)
   and `index_seed_path` (default `config/universe/nifty500_seed.csv`). No aliases for the old keys:
   settings.yaml is versioned and updated in the same change; a stale key must fail loudly at boot
   (pydantic extra=forbid if that is the model's convention — check), never silently keep NIFTY200.
2. **Seed file** `config/universe/nifty500_seed.csv`: fetch the live CSV ONCE
   (`https://archives.nseindia.com/content/indices/ind_nifty500list.csv`, browser User-Agent; 501 rows,
   header `Company Name,Industry,Symbol,Series,ISIN Code`), prepend the same three-line `#` comment
   convention as `config/universe/nifty200_seed.csv` (adapted: NIFTY 500, 2026-09-04), and confirm the
   builder's parser reads it to ~500 symbols. Keep `nifty200_seed.csv` in place (history), unreferenced.
3. **Builder** (`src/engine/universe/builder.py`): make the index generic — `_load_index()` (rename of
   `_load_nifty200`), `Universe.index_source` (rename of `nifty200_source`; keep the Literal),
   runtime cache `data/universe/index_cached.csv` (rename; `src/engine/datafeeds/isin_map.py:170-171`
   reads the cache + seed paths — update it to the new config fields/paths), log events renamed
   `index_download_failed` / `index_fallback_unreadable` / `index_cache_write_failed`, the `universe_built`
   log carries `index_name` and `index_size` (drop the `nifty200` field), the fallback alert title says
   "NIFTY 500 download failed — using fallback" via `index_name`. `EXCL_INDEX = "not_in_index"`.
   Module docstring updated to the O15 rule. The focus cap (`EXCL_CAP`, `universe_max_watchlist`)
   is UNCHANGED and now binds (eligible > 200): verify the existing top-N-by-median-traded-value path.
4. **Store** (`src/engine/marketdata/store.py` ~629-633, ~1411-1456): `_EXCL_INDEX = "not_in_index"`
   plus `_EXCL_INDEX_LEGACY = "not_nifty200"`; `get_batch_universe_symbols` accepts either marker;
   `replace_universe_daily` deletes day-d rows carrying either marker before the upsert. Comment why
   (rows written 2026-09-01…09-04 carry the legacy marker; nothing rewrites history).
5. **Sector map over the batch universe**: `src/engine/ops/main.py` ~line 1002 passes
   `watchlist_symbols()` to `sector_map.run(...)`; change it to the day's batch universe symbols
   (`store.get_batch_universe_symbols(today)`, falling back to `watchlist_symbols()` if that is
   empty) so widened eligible names are classified — otherwise they land in the UNCLASSIFIED
   bucket whose §7.1 sector cap is 1. Check `sector_map.run`'s cost with ~800 symbols (it is a
   set-membership pass over cached sectoral CSVs; confirm, do not assume).
6. **Tests** (red-first): `tests/unit/test_universe_builder.py` (24 references — rename, plus a new
   test that a 500-name index with cap 200 yields exactly 200 included rows chosen by median traded
   value and `watchlist_cap` exclusions for the rest, and that extended candidates exclude the
   index), `tests/unit/test_market_store.py` (legacy + new marker both read; replace deletes both),
   config tests (new field names; old key rejected), the wiring test for the sector-map source if one
   exists, and `tests/unit/test_backtest_hi52.py` if its 3 references are more than docstrings.
   Run the touched modules inline, then the FULL unit suite once, inline; paste the summary lines.
7. **Docs in code only**: docstrings/comments that say NIFTY200 as the rule (builder, config, store,
   settings.yaml comments) become "the configured index (NIFTY 500 since 2026-09-04, O15)". Do not
   edit IMPLEMENTATION_PLAN.md, WORKLOG.md, limits.yaml, envelope.yaml.

## Constraints
- The engine service is running (post-close); never start/stop it; never write to
  `data/market.duckdb`. A deploy restart is the manager's step.
- No new dependencies; no behavioural change to caps, guard, gate, analyst, prescreen.
- Do not commit. Leave changes in the working tree.

## Output contract
Final message = (a) commands + pasted outputs (touched modules, full suite summary line, the seed
parse count); (b) file:line pointers for the config fields, `_load_index`, `EXCL_INDEX`/legacy
handling in the store, the sector-map call, and each renamed log event; (c) the first 3 data rows
of the seed file; (d) decisions the plan left open, and anything not done with the reason.
