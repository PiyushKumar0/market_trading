# Brief — pre-registered CANDLE / price-action battery (intraday, MIS), 2026-09-04

Owner question: "can't we track the candles and trade based on their movements?" This battery answers
it with evidence. It is PRE-REGISTERED: five long-only rules × two exit styles = 10 cells, every cell
reported, none selected, no parameter sweeps. The `tdc` refutation (plan §6.1, `scripts/backtest_tdc.py`)
is the immediate precedent and the harness to extend — reuse its DuckDB feature extraction style, cost
model, CPCV, report shape, and its look-ahead discipline.

## Deliverables
1. `scripts/backtest_candles.py` — CLI, `RULES`/`EXITS`/`PARAMS` dicts at the top exactly as below.
2. `tests/unit/test_backtest_candles.py` — synthetic-bar tests: 5m aggregation from 1m (09:15-anchored
   buckets), each rule's trigger (one positive + one negative case each), VWAP, entry at the next 1m
   open, stop/target fills, trailing exit, 15:15 squareoff, one-trade-per-symbol-day, cost application.
   Run inline: `.venv\Scripts\python.exe -m pytest -q tests/unit/test_backtest_candles.py`; paste output.
3. `data/reports/backtest_candles_2026-09-04.json` + printed summary table (rule × exit × split).

## Data
`data/market.duckdb` READ-ONLY (engine stopped by the manager — never start/stop it). `bars_1m`
(symbol, ts_minute tz-aware, open/high/low/close DECIMAL, volume), `universe_daily` (included) with
the `bars_1m` symbol set as fallback, `news_clusters`/`catalyst_watchlist` for the catalyst split,
`sector_map` not needed. Sessions = distinct `bars_1m` dates with >150 symbols (as tdc did). Window:
full history to 2026-09-03. Index: "NIFTY 50" 1m where present (2026-07-23+), else the equal-weight
universe return as proxy (as tdc).

## Definitions
- 5m bars: aggregate 1m bars into buckets [09:15,09:20), [09:20,09:25), … (open=first, high=max,
  low=min, close=last, volume=sum). A bucket with fewer than 3 one-minute bars is invalid (skip).
- VWAP: session typical-price VWAP from 1m bars, evaluated at the 5m bar's close minute.
- medvol20: median volume of the prior 20 valid 5m bars of the SAME session (fewer than 8 → skip).
- Signal window: 5m bars whose close minute is in [09:45, 14:00]. One trade per symbol per day: the
  FIRST rule trigger of the day for that rule. Max 5 trades per rule per day, ranked by the bar's
  volume ÷ medvol20 (descending, then symbol).
- Entry: the OPEN of the 1m bar after the signal bar's last minute.
- Costs: `CostModel.breakeven_pct(Decimal("20000"), "MIS")` + 1 tick each side (as tdc).

## RULES (long only, evaluated on the completed 5m bar; all require close > VWAP unless stated)
- R1 momentum_burst: close > max(high) of the prior 6 five-min bars; volume ≥ 2.0 × medvol20;
  close ≥ low + 0.75 × (high − low).
- R2 three_soldiers: three consecutive bullish bars (close > open), each close > the prior bar's HIGH;
  third bar's volume ≥ 1.5 × medvol20.
- R3 engulf_pullback: the session high was set ≥ 3 bars ago; the 3 bars before the current one each
  closed below the prior close (a pullback) and all stayed above VWAP; the current bar is bullish with
  open ≤ prior close and close ≥ prior open (body engulf); volume ≥ 1.2 × medvol20.
- R4 vwap_hold_buy: the prior 6 bars all closed above VWAP; the current bar's low ≤ VWAP × 1.0015
  (a touch/retest) and it closes above VWAP with close > open.
- R5 vwap_reclaim: the prior 6 bars all closed BELOW VWAP; the current bar closes above VWAP;
  volume ≥ 1.5 × medvol20. (This is the one rule where prior bars are below VWAP by construction.)

## EXITS
- E1 fixed: stop = signal bar low (R5: stop = VWAP at signal); target = entry + 2 × (entry − stop);
  checked on 1m bars after entry (low ≤ stop → fill at stop; high ≥ target → fill at target; if both in
  one bar, the stop wins); else 15:15 close.
- E2 trail: exit at the next 1m open after the first completed 5m bar that closes below the PRIOR
  5m bar's low; else 15:15 close. (No target.)

## Splits (reported for every cell): all; index_up/down at the signal minute; breadth at 11:00 ≥ 0.5
vs < 0.5 (share of eligible universe above its OR high, as tdc); catalyst_at_T true/false (tdc's
definition, 2026-07-10 onward). Metrics: n, mean net %, median, win rate, t-stat, CPCV positive-split
share (tdc's CPCV settings), promotable = mean net > 0 AND t > 2 AND n ≥ 200 AND CPCV share ≥ 0.6.
Also report each cell's mean GROSS % and mean cost %.

## Output contract
Final message = (a) commands + pasted outputs (pytest, CLI); (b) the summary table; (c) file:line for
RULES, the 5m aggregation, entry fill, E1 fill logic, E2 trail, cost; (d) two hand-verified trades
against raw 1m bars (one E1 target/stop, one E2 trail); (e) deviations with reasons; (f) data caveats.
No promotion language beyond the boolean. Modify no file other than the two new files and the report.
