# Recommendation Drought Audit: verification of the 2026-09-10 external diagnostic

Prepared 2026-09-11, 02:00 to 04:00 IST, against branch `phase2` at `b7eccce`, `data/state.db`, `data/market.duckdb` and `data/logs/engine.log*`. Every claim below was checked against the code that runs and the stored evidence, then handed to ten independent refuters plus a completeness critic (workflow `wf_0dbf2a35-c61`); their corrections are folded in. The two held positions (HDFCAMC, HINDZINC) were closed by the owner at 01:12 IST on 2026-09-11, after the external audit was written.

## 1. Summary

The audit's counts are correct. Its causal story is mostly overstated or stale, and it misses the constraint that is binding right now: the budget governor cut the analyst forward cap from 48 to 4 per day on 2026-09-10 and holds it there until the month rolls, and the news analyst is one or two calls from a hard stop on its own allocation. Nothing in the audit's remediation list touches that.

| Ground truth (2026-07-10 to 2026-09-10) | Value |
|---|---|
| Recommendations delivered | 71 |
| Of which repeat exits on HDFCAMC / HINDZINC | 68 (42 / 26), all expired unactioned |
| Entry recommendations | 3 (HDFCAMC brk20 net −₹695, HINDZINC brk20 net −₹48, JINDALSTEL ins expired) |
| Intraday entry recommendations ever | 0 |
| Entry proposals reaching the gate | 24 (brk20 15, rsi2 4, orb 2 on 07-31, cat 2, ins 1); 23 carry a verdict |
| Gate outcome on entries | 1 approve, 2 shrink, 20 reject |
| Analyst enter rate on real candidate decisions | 24 of 349 (6.9%); brk20 15 of 41 forwards (37%) |
| Analyst forward cap on 2026-09-10 | 4 (was 48 on 09-04 to 09-09) |

## 2. Verdicts on the audit's six blockers

| # | Audit finding | Verdict | What the code and data show |
|---|---|---|---|
| 1 | cat, cat_reversal, hi52 locked in shadow; rsi2, trend rejected for no target; mom dropped for no stop | True, by design | `ops/main.py:2708` `NO_EDGE_SHADOW_STRATEGIES`; `risk/gate.py:996-1006`; `ops/pipeline.py:1445`. Plan §8.6 and `IMPLEMENTATION_PLAN.md:250` forbid inventing an edge number to pass the C3 check. hi52 v2 is CPCV-promotable (`data/reports/backtest_hi52_v2_2026-09-09.json`: T+20 median net +1.71%, fold pass 86.7%) and only owner sign-off gates it. |
| 2 | brk20 retest limit vs the 2% CNC entry band is "mathematically guaranteed" to reject every retest | Real, overstated | Band at `risk/gate.py:911-930`, `config/limits.yaml:139-142`. Of 15 brk20 proposals, 6 died on margin (IDEA 5.24%, PAYTM 4.01%, SAIL 2.88%, KOTAKBANK 4.65%, SYRMA 3.96%, CHENNPETRO 6.30%), 8 passed, 2 were delivered. The band was the sole reject cause for 3 (IDEA, SYRMA, CHENNPETRO), which are the top-3 brk20 scores. COCHINSHIP failed because the analyst emitted a LIMIT with no price, not for lack of an LTP. `IMPLEMENTATION_PLAN.md:1343` records the band rejection as a deliberately accepted consequence. Score is `min(1, 0.5 + 5 × margin)` (`scanners/brk20.py:234`), so high scores and band rejection are correlated (44 concordant vs 4 discordant pairs), but LTP drift at evaluation time rescued a third of the predicted failures. |
| 3 | Sizing strangulation: 2.5× gap multiplier and ₹8,000 per-stock cap shrink quantities to 0 | Arithmetic right, impact small | Budget is 2% of live equity, not of the base (`gate.py:718`): ₹783.62 to ₹785.03 in September, so one share needs a stop distance under ₹313 (not ₹320). 24 of the 480 eligible names closed above ₹8,000 on 09-10 (5%). Sizing to zero was the sole reject cause in 1 of 23 verdicts (DIVISLAB 09-09). HAL and ADANIENT also priced to 0 but were already hard-rejected on other rules. 14 of the 18 `per_trade_risk` failures were shrinks with a non-zero cap, killed by other rules. |
| 4 | Zombie positions blocked capacity and poisoned the day plan | Partly true | Positions stayed OPEN 08-26/27 to 09-11 01:12. On 08-27 five brk20 proposals failed `max_open_positions` ("total 1+1 pending+1 = 3; CNC 1+1+1 = 3", cap 2 then); for NMDC, MOTILALOFS and POLICYBZR the only non-shrinkable failures were the position count and the UNCLASSIFIED sector cap, so capacity was the but-for cause for those three. O16 (09-07) raised CNC to 4 and excluded exiting positions (`gate.py:1170-1205`); no verdict after that failed on capacity. 11 day plans attributed live overnight risk to the two names. The real cost was 182 position-event analyst calls and the single ins entry (JINDALSTEL, 09-08) landing as item 2 of 8 notifications that day, the other 7 being repeat exits. Holdings reconcile is alert-only by design (`ops/holdings_reconcile.py:14-18`, plan §3.6) and sent 6 alerts from 09-08. |
| 5 | Market sentiment clipped at −1.000 and read as macro collapse | Stale | Fixed 2026-09-04 in commit 8c398c7: `intelligence/context.py:82-109` `sentiment_rail_note` (raw sum, cluster count, per-cluster mean), rendered by both the planner (`ops/preopen_planner.py:287,294`) and the analyst context. The last three day plans quote the rail note. Residual: the frozen feature vector line still carries the bare clipped value (`context.py:453`). |
| 6 | Analyst over-tuned for caution; soften rules 11/15, lower the confidence floor to 0.45 | Reject as framed | The "97.4% no_action" divides by 919 calls, of which 314 are heartbeats that cannot enter and 182 are position events that can only reduce risk. Deduplicating retries gives 349 entry decisions, 24 enters. brk20 pass-through is 37%; rsi2 4 of 80; orb 0 of 101 in the counter window. The floor (`analyst_confidence_min: 0.55`) was the sole reject cause in 1 verdict (GRANULES 0.44). ORB declines are correct: ORB tested 0/15 CPCV twice, tdc and the candle battery were refuted on 09-04, and the audit's proposed VWAP reclaim was already run (WO-10b, `data/reports/vwap_reversion_20260814T022003.md`) and aborted at −0.126% per trade. ORB already uses a 30-minute range (`scanners/orb.py:87`). |

## 3. What the audit missed

Ranked by recommendation volume lost. Each item was pointer-verified and survived an adversarial refutation pass.

1. **Governor DG1 trap.** `intelligence/governor.py:277-284`: DG1 trips when any single agent exceeds 85% of its own `budget_allocations_usd`. `governor.py:316-322`: `prescreen_forward_cap()` returns 48 only at DG0 and a flat `DEGRADED_PRESCREEN_CAP = 4` otherwise. Month spend is monotone, so the tier never recovers before the month rolls. news_analyst (allocation $80, `config/agents.yaml:52`) crossed $68 at 2026-08-28 13:34 and again at 2026-09-09 13:52 (`budget_ledger`). Engine log `forward_drained` cap values: 48 on 09-04 to 09-09, 4 on 09-10, with 45 `forward_drain_no_slot` ticks from 09:50 to 12:26 while 6 to 9 candidates queued and the best unforwarded score sat at 0.97 for two and a half hours. news_analyst ended 09-10 at $79.23 of $80 with an average call cost of $0.26; `can_invoke` returns `agent_allocation_exhausted` at 100%, which would silence the catalyst digest and the cat legs for the rest of September. The ledger dollar figure is a governor proxy on a subscription plan (`agents.yaml:12-13`); intraday_analyst itself was at $70.72 of $280.

2. **Exit outputs fail schema on every first attempt.** The flat wire schema advertises `reason` as free text (`intelligence/schemas.py:227`) while `core/contracts.py:107` requires `Literal["thesis_invalidated", "target_neared", "risk_event", "time_stop", "other"]`; the sanitizer only drops keys and clamps lengths (`schemas.py:313-327`). Replaying all stored position-event outputs through the real parser gives exactly one error each, `literal_error` at `exit.reason`: 85 rows from 08-27 to 09-10, and a 100% first-attempt failure rate since 09-03 (47 of 47). The retry converges on attempt 2 because `harness.py:852` echoes the pydantic error, the only place the model ever sees the five codes, and every retry picked `thesis_invalidated`, the first code in the message. Cost since 09-03: $11.86, 67,868 output tokens, a median 20.7 s added to every exit decision. 10 exits were terminally lost before 09-03.

3. **Batch-leg candidates are lost on freezes.** brk20, hi52, cat and ins are produced by `run_scan_sweep` (`ops/main.py:1492`), which runs only on the trade-window INACTIVE to ACTIVE edge (`main.py:1808-1823`) or `/scan_now`; the edge predicate omits risk state. A candidate arriving while `risk_state` is FROZEN is re-armed (`ops/pipeline.py:1432-1434`) but nothing re-publishes it, because unlike bar-driven scanners there is no next bar. Every owner restart re-applies the warmup freeze at boot, and the boot-time sweep lands inside it. Lost entirely: 08-28 (warmup freeze 09:55 to 10:52, three pairs re-armed at 10:13:58), 08-31 (instrument-freshness freeze all day, five published), 09-04 (three boots, three sweeps at 09:50 / 10:32 / 11:11, all inside freezes, five pairs re-armed, zero forwarded).

4. **The forward queue is process memory and the drain has silent exits.** `_roll_forward_day` clears `_pending_forwards` with no rehydrate path (`pipeline.py:836-840`); the owner restarts the engine two to three times per session (`stop_requested` at 09-04 11:07:33 and 09-07 12:55, not crashes), so every restart drops the queue with no log, no re-arm, and burned day slots. Roughly 70 admitted candidates since 08-17; 210 of 520 slots all-time are admitted-but-never-forwarded. `_drain_one_forward` returns False with no log when the window is closed (`pipeline.py:1313-1316`) and `_expire_forwards` only runs inside `_take_forward_slot`, so queued candidates at window close neither forward nor expire (09-10: six stranded at 12:26, including hi52 BHEL at 0.9705). Failed analyst calls charge the forward cap before the call (`pipeline.py:1067-1069`) and the cap is never refunded (`strategy/prescreen.py:536-537`): 110 such charges on record; at cap 4 one timeout is a quarter of the day. `_quantile_band` returns the top band for any strategy's first candidate of the day (`pipeline.py:872-874`), so a lone late orb candidate took the last slot on 09-10 over a higher hi52 candidate.

5. **ins is symbol-starved.** `market.duckdb insider_trades`: `bse:` 391 rows over 22 distinct symbols since 2026-06-23; `nse:` 44,187 rows over 1,565 symbols ending 2026-05-02. The validated edge (+0.75 / +1.61% net at T+10 / T+20) was measured on the NSE corpus. `ins_crossings_run` on 09-10 logs universe 480, symbols_with_filings 22, the same 14 to 22 as when the universe was 200. Two crossings ever went live (HCLTECH 08-18 declined, JINDALSTEL 09-07 delivered and expired). The starvation telemetry counts rows per day, not distinct issuers, so a 22-issuer feed reads as healthy.

6. **ORB consumes 44% of analyst forwards for zero entries.** Since 08-17, 101 of 229 forwards were orb; every entry proposal since the forwarded counter existed came from a swing leg. orb also churns: TTL 20 min against a 3-minute pacing interval re-arms and re-publishes the same pair repeatedly (CIPLA 09-10 queued three times, expired three times).

7. **rsi2 is regime-gated, not broken.** `scanners/rsi2.py:56-64` requires NIFTY 50 close above its 50-DMA and the 50-DMA rising. `bars_1d 'NIFTY 50'` closed below the 50-DMA on every session from 08-27 to 09-10 (the DMA-rising leg still passes), and the scanner reads completed sessions, so the last productive day was 08-27 (16 slots, its cap). The stock leg alone would pass 13 to 32 names a day. trend's rarity is baseline golden-cross scarcity, not the regime.

8. **Latent: 180 eligible symbols are unticked.** The gate's only price source is the tick cache (`main.py:445-447`, `gate.py:1401`); the subscription set is the 300 `included` rows, while the eligible set is 480 (`universe_daily` 09-10: 300 included plus 180 `watchlist_cap`). `KiteClient.ltp` has zero callers. 27 of 84 brk20 slots were on such symbols. It has not fired because the analyst declines those candidates for lack of bars; if it did, `stale_data_guard` ("symbol no feed") would reject first.

Also verified: the warmup gate is global by the plan's effect column (`IMPLEMENTATION_PLAN.md:1514`, "FROZEN (entries)"), so a one-bar 1-minute hole on one orb symbol (09-04 11:09: `orb:COROMANDEL bars 113/114`) freezes swing legs that read daily bars. Freezes are applied at boot and post-login recovery, not by the running gate, which only lifts them.

## 4. Action plan

### A. Before Monday's open, configuration only

- Raise `news_analyst` in `config/agents.yaml budget_allocations_usd` (owner-tunable, not a protected store) so it is not the binding agent; it is at $79.23 of $80.
- Decouple the DG1 any-agent trigger from the intraday forward cap, or make the degraded cap proportional rather than a flat 4. The per-agent allocation is already a hard stop on its own (`governor.py:296-297`), so one agent's proxy overspend should not cut the entry funnel twelve-fold.

### B. Code fixes this week, no owner decision needed

- Advertise a closed `exit_reason` enum on the wire schema, map it to `reason` for exit actions in `_sanitize_guidance_extras`, and name the five codes in the position-event context. Enums elsewhere in the flat schema do not trigger the union text-mode degradation.
- Re-run the batch sweep, or re-publish re-armed batch pairs, on FROZEN to NORMAL inside the trade window.
- Rehydrate the forward queue from journal rows (evaluated=1, forwarded=0, within TTL) on boot; at window close expire and re-arm instead of returning silently; refund the forward cap on infrastructure failures; return the empty-population quantile as median rather than top band.
- Skip forwarding brk20 candidates already outside the band at forward time, and add a retest re-arm so a level that returns to within the band over the next few sessions re-fires. Do not widen the band: a 6% below-market limit with same-day validity is unfillable, as the plan states.
- Park orb origination (zero forwards or shadow) until an intraday hypothesis clears the cost floor.
- Log distinct issuer count on the ins starvation line.

### C. Owner decisions

- **Promote hi52 v2 now.** CPCV-promotable at T+10 (80%) and T+20 (86.7%) under the pre-registered rule; RECOMMEND mode places no orders, so this is a reversible door; the shadow soak restarted 09-10 can run in parallel as the kill criterion (demote if forward T+20 median net is below zero after 20 signals). Caveats: the backtest's index split applies current membership backwards (survivorship-tainted, stated in the report notes); the live scanner treats the v2 filters as diagnostics (`scanners/hi52.py:69-70`), so promotion means making them gating, registering the T+20 net edge the way ins does (`settings.yaml ins.expected_edge_pct`), removing hi52 from `NO_EDGE_SHADOW_STRATEGIES`, and ticking hi52 candidates so the gate has a price.
- **Restore insider coverage.** Find out why the BSE fetch reaches 22 issuers, and probe an NSE PIT sibling route as with the deals fix. Do not lower the ₹1 Cr crossing threshold; it defines the validated population.
- **Exit hygiene.** Suppress repeat exit recommendations on an unchanged position; after holdings show zero for two sessions, drop the position from the day plan and the exit loop while still asking for the `/closed` price. Consider multi-session validity for swing entries (JINDALSTEL expired at 15:30 the same day).
- **Sizing knobs** are low leverage on the evidence. The flat ₹15.34 DP charge makes small notionals expensive (breakeven 0.43% at ₹8k, about 0.75% at ₹3k), which argues for fewer, larger positions; a ₹10k to 12k per-stock cap has a cost argument, but it is a protected-store risk decision.
- **Warmup scoping** is a plan change: the effect column says all entries; daily-bar swing legs should not be blocked by a 1-minute-bar hole.

### D. Rejected from the audit

- Registering invented expected edges for rsi2 and trend: forbidden by plan §8.6 and it defeats the C3 check.
- The 70/30 prompt rewrite and "find the single best opportunity each day": introduces the quota the design rejects; the analyst is not the bottleneck for brk20 and is correct on orb.
- Lowering the confidence floor to 0.45: sole cause in 1 of 23 verdicts.
- Widening the entry band: the plan's reasoning holds; fix origination timing instead.
- The intraday redesign list: VWAP reclaim tested and aborted, 30-minute ORB already in place, trend-day continuation and candle rules refuted.

## 5. Amendments after the reviewer's rebuttal (2026-09-11)

The external reviewer replied to this report. Three of its points are right and change the plan; four are wrong on the current evidence.

**Accepted, plan amended:**

- **Intraday is not parked, ORB is.** Section 4.B's "park orb" stands (three long-only price families refuted), but 4.D wrongly implied nothing intraday is left to test. Two hypotheses are genuinely untested in this book and can be pre-registered cheaply on the existing harnesses: (a) an ATR- or beta-conditioned population (daily ATR% terciles as a split in the tdc, candle and VWAP harnesses), and (b) the short or fade side, which no intraday backtest has ever run (every sweep was long-only under §1.4.9). The ORB v2 finding of a negative-gross fade in 2025-H2 is the prior for (b).
- **brk20: decide the entry mechanism by backtest before touching the scanner or the gate.** brk20 has no backtest at all; the LIMIT-AT-LEVEL entry was a WO-4 fix for a sizing and rendering bug, not an evidence-based choice, and "market on confirmation" has no evidence for 20-session highs either. Adapt `scripts/backtest_hi52.py` (20-session high fresh cross, next-open entry versus a retest limit filled within N sessions, T+5/10/20, same costs and CPCV). The forward-skip in 4.B only saves analyst slots under the current gate and should follow that decision, not precede it. The retest re-arm in 4.B is the pullback tracker the reviewer says is missing.
- **trend deserves a horizon-appropriate margin floor.** The 2026-08-14 corrected validation passes 93.3% of CPCV folds and fails only the WO-3 floor, which divides one round trip by 20 sessions; trend holds up to 120 sessions, so the floor overstates what it must earn per day. Re-run with the floor scaled to the strategy's holding cap before calling it dead. Sweep expectancy at the best cell is +3.6% per trade on 303 trades, in-sample, N=9.

**Rejected, with the evidence:**

- "rsi2 passed CPCV at 80% in the platform's own reports." Those are the July reports, superseded on 2026-08-14 when fills moved to next-open and the spread was charged. The current validation (`data/reports/rsi2_20260814T005050.md`) reads NOT PROMOTABLE: fold pass 60% against 80%, median passing split 0.00147% per day against a floor of 0.01596%, and the WORKLOG records "the +0.58% per trade edge is GONE under honest fills". Registering an edge for rsi2 would be fabricating one; ins's 1.58% is the measured T+20 net drift from a pre-registered protocol, which is the standard rsi2 does not meet.
- "Shorting is disabled by default." orb emits SELL candidates (`scanners/orb.py:32-33`), the schema and gate accept SELL with mirrored geometry, and the analyst declined 49 ORB shorts on quality grounds and 1 on policy. The §1.4.9 gate binds AUTO in Phase 5 (risk M3/C8: an MIS short into an upper circuit becomes auction short delivery, a risk the owner also carries when executing manually). The real gap is that the short side has never been backtested.
- "Indian large caps intraday mean-revert." WO-10b tested VWAP-deviation reversion at 15 to 60 minutes over 440,384 trades: gross +0.00078% per trade, net −0.126%, aborted at stage 1. The candle battery's VWAP-hold and VWAP-reclaim rules at 5 minutes were refuted the same way. Mean reversion in this universe is a multi-session effect, not an intraday one.
- "The 93% rejection rate is defended as good judgement." It is not. The report disputes the audit's denominator and its remedy (the prompt and the floor), not the conclusion that the end-to-end funnel is too tight. The levers are origination population (hi52 promotion, ins coverage, rsi2's regime), the DG1 cap, and brk20's entry mechanics.

Note on 4.A: raising the news analyst allocation resets the tier only once the governor re-reads `agents.yaml`, so restart the engine after the edit.

## 6. Evidence sources

- `data/state.db` (read-only): `recommendations`, `proposals`, `verdicts`, `agent_calls`, `prescreen_day_slots`, `budget_ledger`, `config_audit`, `day_plans`, `notifications`, `positions`.
- `data/market.duckdb` (read-only): `bars_1d` (`NIFTY 50`), `universe_daily`, `insider_trades`.
- `data/logs/engine.log.2026-08-28`, `.2026-08-31`, `.2026-09-04`, `.2026-09-10`: `brk20_sweep`, `prescreen_rearmed`, `forward_drained`, `forward_drain_no_slot`, `agent_call_failed`, `warmup_not_ready`, `stop_requested`.
- Refutation run: workflow `wf_0dbf2a35-c61` (10 refuters, 1 critic, 346 tool uses); all ten claims held with the corrections incorporated above.
