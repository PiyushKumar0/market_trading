# Plan — paper no-stop exit test (2026-10-15)

Owner decision **D14** (2026-10-15): "Go with option C and validate". Option C was one of three answers to the
exit-geometry question in the 2026-10-14 status: test exits without stops **in the paper book only**. The
recommendation cards and every real-money path keep their stops.

## 1. Why

- Study C1 (2026-10-07, `IMPLEMENTATION_PLAN.md` §6.1) found that, in backtest, every shipped stop turns its
  strategy's registered time-only edge negative (T+20 median net: hi52 −1.41% vs +1.70% with no stop, brk20
  −1.47% vs +0.70%, ins −3.42% vs +0.93%).
- Live hindsight (`rec_outcomes`, read 2026-10-15): 14 of the 15 closed entry recs were stopped out.
- **Seen before this plan, and disclosed in the pre-registration:** on the same recs, the no-stop exit was not
  better so far (T+10 paired median −0.21 pp, better in 6 of 12; T+20 worse in 3 of 3). Time-only excess over
  the equal-weight NIFTY 500 benchmark was about zero (T+5 −0.50%, n 22; T+10 +0.04%, n 14), during a period
  when the benchmark fell 6.7% (2026-08-27 → 10-14). So the live record does not yet show that stops are the
  loss mechanism. The 2026-10-14 status said it did; that overstated the live evidence.

## 2. What changes

Only paper positions opened after the deploy, for the strategies listed in `paper.no_stop_strategies`
(`[hi52, ins, brk20]`, the three with C1 evidence):

- **No stop and no target.** No paper GTT is placed. The position sells near the close of its exit session
  (D2: the 20th session, the fill session counting as 1), through the existing ExitManager time exit.
- **Unchanged:** entry selection, the gate, and sizing. The gate still sizes off the proposal's stop distance, so
  a paper position has the same quantity as the card. The 2%-of-equity risk figure then no longer bounds the
  position's loss; the 20-session hold does.
- **Unchanged brakes:** the paper equity floors (rolling-5-session 8% drawdown → no new entries; −10% → exit
  all; −15% → killed), corporate-action voids, and catch-up voids all still apply. A floor exit is part of
  what is being measured.
- **Not touched:** recommendation cards, `rec_outcomes`, the gate, real positions, `limits.yaml`,
  `envelope.yaml`. Other strategies (rsi2, mom, trend, cat) keep their geometry.
- Positions opened before the deploy (MOTILALOFS, 2026-10-13) keep their stop. The policy is fixed when a
  position opens and is never changed mid-trade.

## 3. Design

- `paper.no_stop_strategies` in `settings.yaml` (`PaperSettings`, default empty, so code defaults and tests are
  unchanged). An empty list ends the test for new positions, without a code change.
- Migration 0017: `positions.exit_policy TEXT` (NULL = the stop/target geometry; `'time_only'`).
- `PositionBook._open`: for a listed strategy with a known hold, it writes `stop = NULL`, `target = NULL`,
  `exit_policy = 'time_only'` and protection state `TIME_EXIT_ONLY`. An unknown hold keeps the stop, logged as
  an error; a time-only position must always have a time exit.
- `ProtectionManager`: a `time_only` row gets no GTT and no failure. A NULL stop **without** the policy still
  fails closed, as today. That keeps a data defect from passing as a test position.
- Session prep needs no change: a NULL stop and target replay as a time-only walk through `exit_sim`.
- The proposal keeps the shipped stop and target, so the counterfactual is computable for every paper fill.
- `GET /paper` adds `exit_policy` per position and the configured list. The dashboard shows "No stop (test)"
  and a one-line note on the Paper tab, and the daily-loss-hard hint no longer claims every position keeps a stop.

## 4. Measurement and verdict

Pre-registered in `IMPLEMENTATION_PLAN.md` §6.1, "C5 NO-STOP FORWARD TEST", before the deploy. In short:

- **Signal level, the decision source:** `rec_outcomes` scores every delivered entry rec twice, under the shipped
  rule (`net_pct`) and with no stop to T+20 (`net_t20`, `excess_t20`). The verdict is taken at 20 recs with
  both, then again at 40.
- **Portfolio level, diagnostic:** the paper book's realized results under the no-stop policy, against the
  same fills under the shipped stop, the benchmark, slot-blocked entries and floor events.
- **Tool:** `scripts/no_stop_verdict.py`, read-only.

## 5. Validation

1. Failing-first unit tests:
   - a listed strategy opens with no stop or target, with the policy set;
   - an unlisted strategy keeps its levels;
   - an unknown hold keeps the stop;
   - ProtectionManager places no GTT for a time-only position and leaves it open with the mark far below the
     shipped stop;
   - a NULL stop without the policy still fails;
   - session prep's replay ignores a would-be stop and closes on the exit session.
2. An end-to-end paper test: a time-only position held through a price fall below the shipped stop stays open,
   then sells on its exit session near the close.
3. The verdict script run read-only on the live `state.db`. It must report INSUFFICIENT and reproduce the interim
   figures in §1.
4. Full suite (alone, inline). Dashboard `npm test` and a fixture-server check.
5. A whole-branch review from several angles before the commit.
6. Deploy outside 09:15–15:30 with a `state.db` backup. Verify after boot: migration 0017 applied, paper composed,
   no ERROR lines. On the first time-only paper fill: no GTT row, protection state `TIME_EXIT_ONLY`, exit session set.

## 6. Rollback

- **End the test for new positions:** `paper.no_stop_strategies: []`, then restart. Open time-only positions keep
  running to their exit session.
- **Rolling the code below this change:** the old ProtectionManager treats a time-only position's NULL stop as a
  protection failure and exits it at market. That is acceptable in paper, but close or void open time-only
  positions first if a clean record matters.
