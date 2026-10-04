# Brief — `tdc` (trend-day continuation) pre-registered backtest, 2026-09-04

Owner question (2026-09-04): "will the tickers flagged yesterday be recommended under the updated
logic? If not, find the solution." Answer: no (see IMPLEMENTATION_PLAN §6.1 `tdc` pre-registration,
the paragraph immediately before WO-20). This brief executes the pre-registered backtest. The
protocol in the plan is BINDING: no parameter sweeps, no selection among variants; report all.

## Deliverables
1. `scripts/backtest_tdc.py` — standalone CLI, same conventions as `scripts/backtest_hi52.py`
   (read it first: CostModel usage, `engine.learning.validate.cpcv_splits`, report JSON shape,
   the `engine` import-order guard `import engine` before numba/vectorbt). Pre-registered params
   live in one `PARAMS` dict at the top; variants in `VARIANTS`.
2. `tests/unit/test_backtest_tdc.py` — synthetic-bars tests for: OR/VWAP/acceptance computation,
   `rel_volume_tod` median, RS gate, entry at next open, VWAP-loss exit at following open, 15:15
   squareoff, cost application, breadth split. Run inline, paste the pytest output.
3. `data/reports/backtest_tdc_2026-09-04.json` + a printed summary table (H1, B, each variant ×
   each split): n, mean net %, median, win rate, t-stat, CPCV positive-split share, promotable?.

## Rule (from the plan, restated)
At T = 11:00 IST bar close, for each eligible NIFTY200 symbol (use `universe_daily` included rows
for that date; fall back to the symbol set present in bars_1m if universe_daily is absent for
early dates — say which dates fell back):
- (i) close > session VWAP (typical price × volume, cumulative from 09:15) AND close > OR high
  (max high 09:15–09:29 bars) AND the last 15 one-minute closes (10:45..10:59) are all > VWAP.
- (ii) rel_volume_tod ≥ 1.5: cumulative volume 09:15..T ÷ median over the prior 20 sessions of the
  cumulative volume to the same minute; require ≥ 10 valid prior sessions else skip the symbol-day.
- (iii) return from open (T close ÷ 09:15 open − 1) ≥ +1.0% AND ≥ NIFTY 50 return from open + 1.0%.
- Rank passers by (return from open − NIFTY return from open); take the top 5 per day.
- Entry: the 11:01 bar OPEN. Exit H1: first 1m close < VWAP(T) × 0.9975 → exit at the NEXT bar
  open; else 15:15 close. Variant B: exit at 15:15 close, no stop.
- Costs: `CostModel.breakeven_pct` for an MIS round trip at the entry notional (use the model's
  MIS product path; check `engine.strategy.cost_model`), plus 1 tick slippage each side.
- Robustness variants (report, never select): T=10:30, T=12:00, RVOL 1.2, RVOL 2.0, stop 0.5%.
- Splits: index regime (NIFTY return from open at T ≥ 0 vs < 0); breadth (share of the eligible
  universe with T close > OR high ≥ 0.5 vs < 0.5).
- Metrics: n, mean/median net %, win rate, t-stat (mean / (sd/√n)), CPCV via `cpcv_splits` with the
  same settings hi52 used; promotable iff mean net > 0, t > 2, n ≥ 200, CPCV positive share ≥ 0.6.

## Data access
DuckDB `data/market.duckdb` — single-writer: the engine MUST be stopped before you open it (the
manager stops/starts it; do not touch the service yourself). Open read-only. Tables: `bars_1m`
(symbol, ts_minute, open, high, low, close, volume), `bars_1d`, `universe_daily`. 1m history
starts 2025-07-10. Include "NIFTY 50" for RS. Sessions: use `engine.core.calendar.NSECalendar`.

## Output contract
Return: (a) the exact commands run and their pasted outputs (pytest, the backtest CLI); (b) the
summary table; (c) `file:line` pointers for PARAMS, the acceptance test, the exit rule, and the
cost application; (d) any deviation from the protocol, stated explicitly (a deviation without a
reason is a miss). No promotion language beyond the rule's boolean.
