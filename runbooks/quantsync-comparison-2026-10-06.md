# QuantSync (rsitradingplatform.vercel.app) vs market_trading — competitive analysis, 2026-10-06

*Owner-requested research ("what is it doing better than our platform, where are we lacking, how do we
overcome it"). The manager drafted it inline during market hours. It was then adversarially verified after
the owner's trade window closed: 7 read-only agents covered code claims, owner decisions, and quant, owner-outcome,
safety and completeness lenses, and the manager spot-checked every claim the agents overturned. Evidence
artefacts (competitor JSON snapshots, JS bundle, audit CSVs, scripts) are copied to
`data/reports/quantsync_2026-10-06/` (gitignored, local only).*

---

## 1. TL;DR

1. **QuantSync is better at product, not at evidence.** Its value is the surface:
   - decision-ready trade cards
   - an always-visible track record
   - one-tap analysis of any stock
   - a channel that is about trades rather than operations

   Its substance does not survive an audit against official NSE bars.
2. **Their "54.7% win rate, PF 1.98, verified" record is a backfilled simulation:**
   - 258 of 373 trades carry batch IDs going back to 2025-01.
   - 55% of entries equal the official NSE close (a 15:00–15:30 VWAP) to the paisa.
   - Exits are computed levels booked a session late.
   - Stops are booked at the stop price weeks after a close below it.
   - There are no costs.
   - V2's "ML confidence 70%" is the same constant on every pick.
3. **Their stock selection has no typical-trade edge we could use.** We re-ran it under our execution (next open, fixed horizon, our CNC costs):
   - T+20 median net is −0.69%, with a 47% hit rate.
   - Against the equal-weight liquid universe, the median excess is +0.28% (95% CI −1.58 to +1.73).
   - The mean excess is a right tail of illiquid ₹1–5 cr non-index small caps.
   - **Inside NIFTY 500 (n=76) the median is −0.93%.** Nothing here justifies widening O15 or following their calls.
4. **Our real problem, exposed by the comparison, is that recommendations are not acted on and nothing measures what they would have done.**
   - The owner took the first 2 entry recs (both losses: HDFCAMC −8.3% with no protective order, HINDZINC), then 0 of the next 28.
   - On rec days the owner was at the dashboard 15–30 min before the recs arrived, so absence is not the established cause.
   - `shadow_trades` is empty. Nothing prices untaken recs.
   - 95% of the 528 Telegram messages since 09-14 are ops traffic.
5. **In this window, skipping was right.** Our 22 fillable recs lost −1.72% (rule-based, mean) against −1.65% for the median liquid stock over the same windows. That is market beta, not bad selection. Raising the take rate is therefore NOT the goal. Measuring the decision, making the card honest, and cutting the noise is.
6. **Two genuine engineering findings about us:**
   - The stop/target geometry shipped on brk20 and hi52 recs was never backtested; all our evidence is fixed-horizon holds.
   - brk20/hi52/ins have no deterministic market-regime filter. rsi2 is the only scanner with one.

## 2. What QuantSync is

- **Product:** a mobile-first React/Vite SPA on Vercel ("QuantSync — Dual-Engine Trading Terminal", v2.4.1). Tabs: Home / DNA / AI BuySell / Breakout / Portfolio.
- **Backend:** a VPS (`quantsync-api.duckdns.org:8443`) serving REST plus a `wss` live-market stream, proxied through Vercel `/api/*`, with SQLite-style stores. The Firebase SDK is bundled, plus TradingView embeds, WhatsApp share and html2canvas share cards.
- **Data:** daily OHLCV (5y), NSE delivery %, Mansfield RS vs NIFTY 50, and fundamentals scraped from screener.in (P&L, balance sheet, cash flow, ratios, shareholding QoQ).
- **Engines and config:**
  - V2 "AI Pro" (an "ML" gate, `minMLConfidence` 62) and V3 "AI Sniper / chart patterns".
  - `maxEntriesPerDay` 2, `capitalPerTradePct` 12.5, `maxSlots` 25, universe 1,318 (screener rows 2,331, heavily small/micro-cap).
  - Ledger families: Apex 105, Breakout 55, Leader 51, Gap 41, Darvas 37, 52W_HIGH 31, HTF 26, Flag 16, Base 11.
- **UX patterns:**
  - "What to buy today?" cards: score, pattern, pivot/CMP/target/stop with % distances, R:R, volume multiple, 3–4 thesis bullets.
  - A plain-words "how it works".
  - A per-stock "DNA" report answering 7 fixed investor questions, including a **"chasing danger — wait for a pullback"** warning and an **ATR "routine swing" translator**.
  - A near-breakout radar with "% to trigger" and cap-tier filters.
  - Explicit call expiry ("CALL EXPIRED · SKIP").
  - An equity curve and closed-trade ledger with a "₹10k per call" calculator.
  - A screener self-accuracy endpoint, pre/post-market reports, sector heatmap, RSS news with impact tags.
- **Real:** their live index stream. Their Nifty 22,676.70 matched our recorded tick (22,676.75) on 10-06.

## 3. Their track record, audited against official NSE bhavcopy (365 matchable trades)

| Claim / check | Finding |
|---|---|
| Forward record? | No. Trade-ID schemes: 258 `V2_TRD_<n>` (entries 2025-01-02 → 2026-09-03), 103 `V3_V3_ACT_<SYM>` (2026-01 → 09-18), 10 `V2_ACT_` (from 07-20), 2 dated live-style IDs (from 09-23). The V2 equity curve starts at ₹3.00L on 2025-01-02 and V3 at ₹3.00L on 2026-01-01. |
| Entries | 55% equal the official close to within 0.01% (V3_ACT 98% within 0.25%). The official close is a 30-min VWAP and cannot be hit by a real order, so these are simulated EOD-close fills. They are also credited with the breakout-day overnight gap: next open +0.54% median vs their entry close, against a +0.37% base rate for liquid 2x-volume breakouts. |
| Exits | 181/373 are not on a 1-paisa grid, and identical ROIs repeat across stocks (+1.5% ×37, −4.5% ×19, +9.78% ×15). 65/252 trailing exits sit outside the exit-day range, but 51 of those sit inside the **previous** session's range. They are computed levels booked a session late (d−1-only 39 vs d+1-only 4, p≈3e-8), not invented prices. |
| Stops | 18/44 STOP_LOSS exits were breached on an earlier day. In 9 of them the **close** was below the stop 2–44 sessions before the booked exit, yet the loss was booked at the stop price. Losses are understated. |
| Costs | None (exact +15.00% / 0.00% exits). 17% of trades are in names under ₹1 cr/day ADV, 40% under ₹5 cr. |
| Headline stats | 54.7% / PF 1.98 is server-side, rupee-based. The closed tab's 55.5% / 2.23 recomputes client-side, counting breakeven exits as wins and using a %-sum PF: different definitions, not different data. The payload also carries `totalReturnPct` 25.59 and 151.19. The equity curve shows `maxDrawdownPct` 0 next to ₹10,024 (recomputed −2.86%). Hero NAV does not reconcile with the two curves. |
| "ML confidence" | Every V2 active pick has `mlProbability` 70.0, `riskScore` 25, a composite score of 69 or 75, and byte-identical "why" bullets. The ML gate is decorative. "Checks 5 years of similar setups" and the Monte Carlo panel are copy or "Coming Soon". |
| Other fixtures and defects | Activity feed = a fixture shipped in the JS bundle ("NIFTY 26,420"). FII/DII "+₹1,240/+₹890 Cr" is constant. Alert toggles are inert divs. The share card hardcodes "VERIFIED BSE AUDITED SETUP". The "today's scanner report" is dated 09-21. Screener self-accuracy ignores its window (48% = 98/204, with 71% "inconclusive"). The SUNFLAG DNA report labels the 0.618-fib pocket ₹298.7–303.2 a "20-EMA pullback" (20-EMA ₹399.9), puts its stop (₹407.67) above that pocket, scores DNA 87 against a pillar sum of 69, and calls the balance sheet both "capital destruction risk" and "Grade A rock solid". |

**Verdict:** a backtest-quality ledger presented as a verified live record. The most recent rows (entries
08-26 → 09-28) are cleaner: all exits are inside the day's range.

## 4. Their stock selection under OUR execution

The rule: buy the next open after their entry date, hold a fixed horizon, our CNC CostModel at their notional, corporate-action-robust returns. Their data was re-run under it.

| Measure (T+20, n=350) | Value |
|---|---|
| Net median / mean / hit | −0.69% / +2.52% / 47.4% |
| Excess vs equal-weight liquid mean | median +0.28% (95% CI −1.58 … +1.73), beat 50.9% (sign test p=0.79); mean +2.93%, but **+0.32% without the top 20 picks** |
| Excess vs small/mid-cap ETFs | HDFCSML250 +0.12%, MID150BEES −0.16% (median) |
| **Inside NIFTY 500 (n=76)** | **net median −0.93%, median excess −0.86%** |
| Where the mean excess lives | ₹1–5 cr ADV non-index names and the backfilled V3_ACT block |

Same six-week window as our recs: one identical rule (next open → T+5 close, our costs, vs the median liquid stock). Our recs show excess median −0.03% (n=21) and theirs +1.83% (n=30). The difference of +3.45 pp has a 95% CI of −1.77 to +9.40 (Mann–Whitney p=0.30): **not significant**.

**Verdict:** no usable selection edge. A lottery-like right tail in illiquid small caps that our O15 universe and
cost model cannot trade, and that O15 (owner-decided) deliberately excludes.

## 5. What QuantSync genuinely does better (and the honest version for us)

| Their pattern | Why it works for a user | Our honest version |
|---|---|---|
| One-glance card: levels, % distances, R:R, plain-language why, anti-chase warning | Decision in 10 seconds | Card redesign §7-B1, on OUR evidence |
| Always-visible track record | Trust, or at least feedback | Rec-outcome scorecard §7-A1, real fills and costs, labelled hindsight |
| Call expiry shown ("CALL EXPIRED · SKIP") | No ambiguity | We expire at the close already; say so plainly on the card |
| Any-stock report in seconds | Answers "why not X?" | `/why SYMBOL` §7-B4: the engine's own view, deterministic |
| A channel about trades | Attention goes to decisions | Split ops traffic off the owner channel §7-B2 |
| Plain-language risk coaching (ATR swing, patience horizon) | Fewer panic exits | Stop in % **and** ATRs plus the exit date on every card |

Things they show that we already do better: costs, point-in-time filings, pre-registration and kill rules,
a deterministic risk gate and envelope, reconciliation, honest negative results. Their push alerts are unproven: the
toggles are inert and no subscription route exists. Telegram already covers that.

## 6. Our gaps, verified

**6.1 The take-rate problem is not what it looked like.**
- 30 entry recs 08-26 → 10-06. The owner took the first 2 (HDFCAMC 08-26 −₹695 net, HINDZINC 08-27 −₹48), then 0 of the next 28 (26 expired, 2 pending today).
- HDFCAMC had no recorded protective order (`protection_state` None). It fell through its −2.3% stop to an −8.3% exit while 68 exit recs repeated, up to 6 a day, and 21 of them came after holdings already showed 0 shares.
- On rec days the owner set the trade window from the LAN dashboard 15–30 min before recs arrived (e.g. 10-06: window 09:57, recs 10:24/10:28).
- Competing explanations, none measurable today (`/veto` takes no reason and has never been used):
  - distrust after the HDFCAMC loss
  - low conviction (every card says "confidence 0.55")
  - trade too small to bother (BHEL: ₹4,005 notional × 1.53% registered edge ≈ ₹61 expected vs ₹385 to the stop)
  - a market read as cautious (the day plans said so)
  - absence
- Over the same windows our filled recs lost −1.72% vs the median liquid stock −1.65%, so skipping cost nothing.

**6.2 No outcome measurement of what we recommend.**
- `shadow_trades` has 0 rows and no writer.
- `learning_ledger` has exit prices only for the 2 owner closes.
- `scripts/hi52_forward_verdict.py` is the only untaken-inclusive measure: hi52 only, report-only, and it cannot run while the engine holds `market.duckdb`.
- The Phase-2 plan said RECOMMEND recs would be tracked by a paper ledger (`IMPLEMENTATION_PLAN.md:40`). Not built.
- WO-P3-5 does not fill this gap. It is AUTO(paper) routing of gate-approved proposals (`IMPLEMENTATION_PLAN.md:1693`), not a shadow book of RECOMMEND output.

**6.3 The exit plan we ship is not the one we validated.**
- brk20: stop = H20 − R, target = +2R (`src/engine/strategy/scanners/brk20.py:34-35, 208, 233`). The median stop is 0.81 ATR, and 71% are under 1 ATR.
- hi52: a 6% disaster stop with no target (`hi52.py:87-89`).
- Every backtest exits on fixed horizons with no stop (`scripts/backtest_brk20.py:74`, `scripts/backtest_hi52.py:66`, `scripts/hi52_forward_verdict.py:328`). The only stop-geometry study (WO-17) covered intraday orb.
- 9 of 22 fillable recs stopped out, 2 on the same day (SONACOMS −0.9%, ELECON −2.1%).
- The WO-19 gap floor is a veto constant, not a validation.

**6.4 Card defects (`src/engine/notify/catalog.py:918-1000`, `src/engine/ops/pipeline.py:2367-2371`).**
- "(FAILED)" prints on the per-trade-risk line of a rec the gate approved with a shrink.
- The checklist says "place GTT OCO stop X (no target — stop-only GTT)", which contradicts itself.
- hi52/ins cards say "targets (none)" with no exit date, though the validated exit is the T+20 close.
- The uncalibrated LLM confidence is the only conviction signal.
- BHEL 10-06: the sweep listed entry ₹427.90 and stop ₹402.25. The rec came 20 min later at ₹445.00 with the stop unchanged, i.e. a **9.6%** stop on a 6% rule, sized down by the gate.

**6.5 Channel noise.** Since 09-14, 528 owner-channel messages, of which 27 are recs (5%):
- `limit_breach` 151
- `catchup_report` 51
- `startup_report` 41
- `engine_started` 41
- `feed_degraded` 30

**6.6 Smaller gaps.**
- No deterministic regime filter on brk20/hi52/ins. rsi2 has NIFTY 50 > rising 50-DMA (`rsi2.py:71-82`), and the gate's regime rule is data readiness only (`risk/gate.py:936-938`).
- The pre-open day plan is generated every morning but only failures notify the owner (`ops/preopen_planner.py:207-212`; no MessageKind).
- No on-demand per-symbol explanation.
- The dashboard is an ops console: no performance view, no charts.

## 7. Recommendations, prioritised

All code work follows house discipline:
- a `wip/<topic>` branch, merged complete and tested
- engine restarts only outside 09:15–15:30
- new modules in `engine.ops`/`engine.learning`, never `engine.risk`/`engine.oms`
- every new HTTP route carries `_: Owner`
- any inline Telegram button gets an explicitly owner-guarded callback handler (today only `CommandHandler`s are wrapped, `notify/telegram.py:920-922`)

**A. Measure the decision before optimising it (read-only, no LLM, within autonomy)**
- **A1 Rec-outcome scorecard.**
  - New migration-created `rec_outcomes` table, written by an in-engine EOD job: per-request MarketStore cursor on a worker thread, CatchUpRunner run-latest, not safety-critical.
  - Per rec: fill / no-fill on the day's bars, rule-based exit, T+5/T+10/T+20 net via CostModel, and excess vs the equal-weight liquid mean (not a median).
  - Per-strategy scorecard on a dashboard GET route, plus one line in the weekly summary ("your skips: N recs, ₹X").
  - It **must never write** `positions`, `learning_ledger` or `shadow_trades`. Those feed the halt ladder, `consecutive_losses`, gate capacity, market-hours LLM position events and §6.4 promotions. Test invariant: their row counts are unchanged.
  - Label it "hindsight, not platform equity". Never feed it back into hi52 thresholds.
- **A2 Skip reasons.** `/veto SYMBOL <reason>` with a fixed reason set (price ran / don't trust / too small / market weak / away / other), plus a reminder at expiry. This is the only way to tell the 6.1 explanations apart.

**B. Make the card honest, and the channel about trades (presentation only)**
- **B1 Card.**
  - Line 1: action, qty, price, ₹ at risk, **exit date** (T+20 for time-exit strategies).
  - Line 2: evidence. Backtest n, hit and median net, plus forward progress (hi52 k/20), from a cached report. This replaces the displayed "confidence 0.55"; confidence stays in the payload and the gate.
  - Line 3: ₹ expected value.
  - Then: invalidation, stop in % and ATRs, regime tag.
  - Fix "(FAILED)" on approved shrinks ("shrunk 14→9") and the OCO contradiction.
  - Flag or refuse when the entry has moved more than X% from the rule's reference while the stop stayed anchored (BHEL).
  - **Keep the B7 checklist and failed gate rules inline. Never put token-bearing links in Telegram.**
  - Charts later, if at all: matplotlib Agg on a worker thread, `send_photo` to `owner_chat_id`.
- **B2 Channel split.** Recs, exits and P&L stay in the owner chat. Ops kinds (`limit_breach`, catch-up/startup reports, feed alerts, login prompts) go to an ops chat or an end-of-day digest.
- **B3 Protection and exit loop.**
  - After `/taken`, prompt for protective-order confirmation.
  - One escalating exit alert per position, silenced once holdings show 0.
  - Auto-prompt `/closed` when the holdings journal shows the shares gone.
- **B4 `/why SYMBOL`.**
  - A deterministic, read-only, owner-guarded `_COMMANDS` row plus a GET route.
  - It shows universe status and exclusion reasons, each scanner's distance to trigger, surveillance and results-day flags, point-in-time filings, recent levels, and why there is or isn't a rec.
  - Never reuse `/scan_now` (it originates). No LLM during market hours.

**C. Evidence work (pre-registered, offline or engine-stopped, after 15:30, nothing live until it reports)**
- **C1 Exit geometry.**
  - What to test: brk20 1R/2R and hi52 6% vs a time exit with a catastrophe stop (≥2–2.5 ATR), T+20, on bars_1d 2022-07 onward with intrabar stop evaluation.
  - A live change afterwards is one owner-approved commit: geometry, re-derived `expected_edge_pct` and pinned tests together. The gate sizes off the shipped stop, so interim re-rendering is unsafe.
- **C2 Regime filter.**
  - What to test: the rsi2-style index filter on the brk20/hi52/ins populations (N = 1–2, CPCV), with Sept 2026 excluded from any fitting.
  - If adopted: a reject-only gate rule, its threshold in `limits.yaml` via the owner reseed, `DOCUMENTED_ENTER_RULES` updated.
- **C3 Selection filters (lower; label it "expect null").** One deflated batch, N=6, on the hi52/brk20 populations: RS63 vs NIFTY 50, trend template, ATR contraction, Stage-2, acc/dist days, delivery % (needs NSE `sec_bhavdata_full`). QuantSync's data gives no in-universe support (§4).
- **C4 Late-session entry vs next open (lowest).**
  - What to test: on bars_1m only, since the official close is a VWAP and cannot be filled.
  - The base-rate gain is below +0.37%, about one round-trip cost.
  - Deploying it needs AUTO plus a `cnc_entry_end` reseed, which loosens a brake.

**D. Owner decisions**
- **D1 WO-P3-5 / P3-6 (AUTO-paper).** Given 2 manual executions in 6 weeks, this is the larger lever. It produces a real paper record through the real gate, OMS and paper broker. It is gated on the owner's sign-off and on ProtectionManager.
- **D2 Evening "GTT plan" — not recommended as drafted.**
  - Owner-placed overnight entry GTTs escape every engine brake: kill, FROZEN, the halt ladder, and the 08:00 surveillance/results checks.
  - Multi-session validity re-creates the 09-21/22 cap lock that the owner withdrew on 09-23.
  - An unattended fill sits unprotected until the OCO is placed.
  - No GTT has ever been placed on record.
  - If wanted at all: a watch-only evening list (no qty/stop), or an AMO for next-open strategies paired with a pre-09:15 "cancel if" re-validation.
- **D3 Remote dashboard access — not now.** The owner already reaches the dashboard on rec mornings. Today's single bearer token is full authority (mode, kill, params, `/db/query`), and the app binds `0.0.0.0`. It would need a read-scoped token and a route-enumeration auth test first.

**Not recommended:**
- Forward-testing or following QuantSync's calls. No in-universe edge; third-party, ToS and point-in-time problems (the Tickertape precedent, `IMPLEMENTATION_PLAN.md:299`).
- Widening the universe to chase their right tail (O15 decided; extended-name hi52 thesis refuted; illiquid).

## 8. Do not copy

- Simulated ledgers labelled "verified"
- EOD-close fills
- Gross P&L
- Stats that change definition between pages
- Constant "ML confidence" and star ratings
- Templated text that contradicts itself
- Hardcoded market data and fixtures
- Inert settings toggles
- Micro-caps without a slippage model
- Two engines long the same name (20 overlapping same-symbol pairs)
- Any signal sharing beyond the owner (B5)

## 9. Method, caveats, reproducibility

**Sources:**
- Their public site and API, read with ordinary GETs and saved as snapshots on 10-06 (no auth, no load).
- NSE UDiFF bhavcopy 2024-12-02 → 2026-10-05, downloaded to a scratch copy. `market.duckdb` was never opened: the engine was running.
- Our `state.db`, read-only URI only.
- Our recorded ticks, via in-memory DuckDB over Parquet.

**Caveats:**
- Their ledger is backfilled over a universe defined today, so historical excess is survivorship-inflated. The direction makes the verdict conservative.
- UDiFF `prev_close` is not corporate-action adjusted. The quant check used CA-robust returns, and the draft's "3 winners fell >16%" finding was withdrawn as split/bonus artefacts.
- Our sample is 22 recs in one six-week decline: the minimum detectable mean is about 2.3 pp. It supports "beta, not selection", not any edge claim.

**Artefacts:** competitor snapshots, `ledger_audit.csv`, the scripts and the agent outputs are in `data/reports/quantsync_2026-10-06/`. The bhavcopy can be re-fetched with its `fetch_bhav.py`.

## 10. Questions for the owner

1. Why have the last 28 recs gone untaken: distrust after HDFCAMC, size, market view, or availability? One line decides whether B1 or A2 comes first.
2. Go-ahead for A1, A2 and B1–B4 (autonomy scope, deployed outside market hours)?
3. Decision on D1 (WO-P3-5/P3-6 AUTO-paper)?
