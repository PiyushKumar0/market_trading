# WORKLOG — autonomous operations log

Owner directive 2026-07-17: every substantive operation gets an entry — **what / why / outcome /
artifact pointer**. Recurring command lines live in [COMMANDS.md](COMMANDS.md). Newest entries at
the top of each dated section.

---

## 2026-07-17

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
