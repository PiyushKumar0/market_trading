# IMPROVEMENT_SPEC.md — Full-System Audit Findings & Work Orders

*Produced 2026-08-13 per AUDIT_PROMPT.md. Evidence gathered by 17 delegated read-only agents
(3 waves + breadth pass) against the live tree at commit 6c8f764; verdicts, apportionment, and
every work order authored at manager tier. All numbers below are measured or reproduced, with
sources in Part III. The audit ran concurrently with (and cross-references) the 2026-08-13
watermark fix (commits f203b48…3cee8f3), which was a live incident, not an audit product.*

---

# Part I — Findings & verdict (for the owner)

## 1. TLDR

1. **The intraday verdict is an apportionment, not a single letter: symptom (a) — zero
   recommendations ever — is H1 (defects, fixable); symptom (b) — intraday momentum loses —
   is H2-confirmed-by-arithmetic at 1-minute scale and left open at 30-min+ scale.**
2. Measured on 48 symbol-days of stored ticks: NIFTY200 median quoted spread **0.018%**; MIS
   round-trip cost floor incl. spread **0.1243%** = **2.05× the median 1-minute bar range** —
   1m-granularity momentum (ORB's class) is structurally dead regardless of signal quality.
   The same floor is only **18% of the median opening range** and **8% of the day range**.
3. The funnel has never given the analyst the day's best candidates: admission is pure
   arrival-order FIFO at both caps, the computed `SignalCandidate.score` is **never read**,
   55% of publication slots went to the parked-negative `orb`, and the analyst forward counter
   resets on every restart. The only two `enter` proposals ever made (07-31) were killed by the
   since-fixed `is_fno` derivation bug — the gate has never rejected a proposal on merit.
4. **Every recorded daily-strategy backtest number (rsi2 +0.58%, trend, mom) is unsafe**: the
   sweep fills trades at the same close the signal is computed on (verified into vectorbt
   source), all 12 of rsi2's passing CPCV splits sit below 0.02%/day expectancy, its four
   promotions all landed exactly on the 80.0% boundary, and the winner config is unstable
   across grid densities. Not refuted — unproven, pending the corrected re-run (WO-2).
5. Highest-impact changes: **WO-1** (score-ranked funnel + per-strategy caps), **WO-2**
   (next-bar-open fills + spread in the cost model + full re-validation), **WO-10** (the one
   pre-registered intraday experiment the arithmetic still permits: 15–60-min VWAP-deviation
   reversion) — in that order, WO-2 before any strategy conclusion is trusted again.

## 2. The intraday verdict (§4.1)

**H1 — defects: CONFIRMED as the dominant cause of symptom (a).** Mechanisms, each
code-verified: (i) FIFO admission at the 20/day publication cap and at the analyst forward
cap, score computed but never consulted — on 2026-08-11 the cap filled in an 11-second burst,
55% of slots to `orb`, and the owner's own log recorded "the cap, not signal quality, was
binding"; (ii) the `is_fno` per-derivative-row derivation left `mis_candidates` **empty every
day** until 2026-07-31 11:06, killing both BAJFINANCE and HINDALCO — the only enter proposals
in platform history — an hour before the fix landed (since then 96/100 of the watchlist is
MIS-eligible, so this is history, not a live blocker); (iii) two schema-contract classes
burned 46 intraday analyst calls before being fixed (07-29 union fallback, 08-12 no_action
contract; today runs 24/25). The analyst itself is NOT a defect: its 96 declines carry
substantive, evidence-based theses, and the hindsight replay proved a zero-recommendation
week correct (−₹790 if taken).

**H2 — wrong strategy class: CONFIRMED at 1-minute horizon by measured arithmetic.** Cost
floor 0.1243% vs median 1m range 0.0606% (ratio 2.05×, stable across both measured sessions,
worse in every liquidity tier than the top tier's 1.61×). ORB's five consecutive 0/15 CPCV
runs across every window and both stop designs are the arithmetic showing up in data — parked
correctly. At 30-min-to-day horizons the arithmetic does **not** exclude viability (floor =
0.18× opening range, 0.08× day range); the families the record has never tested (VWAP-deviation
reversion, gap dynamics) remain open questions, gated behind a corrected harness.

**H3 — structural non-viability at this configuration: PARTIALLY CONFIRMED, at the edges.**
Fixed platform costs ₹550–675/month ≈ **2.75–3.4% of capital monthly** — the portfolio must
gross ~40%/yr before variable costs just to stand still; this is independent of any signal and
is the plan's own recorded stance (§1.2). At 5× MIS (₹1L notional) the fee floor falls to
~0.083% + spread ≈ 0.10% — still ~1.7× the 1m range, so leverage does not rescue 1m-scale
trading. Parameters that would flip H3: longer holding periods (the direction the evidence
already points), more capital (dilutes the fixed floor), or pre-open engine starts + better
fill quality (only matters once an edge exists).

**Confidence (graded per FABLE_HANDOFF §1):** High on the arithmetic and funnel mechanics
(measured / code-observed); High on "recorded swing numbers are methodologically unsafe"
(mechanism verified in vectorbt source; magnitude unmeasured until WO-2 runs); Medium on
whether any intraday family at 15–60-min horizon clears the floor (untested — that is WO-10's
question, not an assertion).

**Self-adversarial reconciliation.** The strongest case against H1-primacy: even a perfect
funnel feeds the same analyst that declined 96 of 98 parsed candidates, and the record says
those declines were right — so fixing the funnel may only produce better-quality silence.
Accepted, and priced in: the verdict claims the funnel *suppressed the experiment*, not that
recommendations were lost. Symptom (a) currently confounds "no good candidates exist" with
"good candidates never reached the analyst"; WO-1 + WO-9 remove the confound, and WO-2 decides
whether any strategy deserves entries at all. Both outcomes are decision-grade; neither is
promised. Conversely, the strongest case against H2/H3 — "the negatives are all artifacts of a
mis-costed harness" — fails in the only direction that matters: every identified harness error
(same-bar fills, zero spread, zero slippage) biases backtests *optimistic*, so the recorded
negatives are floors, not artifacts.

## 3. Findings, ranked by expected impact

**F1 — Funnel admission ignores quality entirely (A, → WO-1, WO-9).** `prescreen.py` dedupe is
first-wins per (symbol, strategy); the 20/day publication cap and the analyst forward cap are
arrival-ordered; `SignalCandidate.score` is computed, logged, and read by nothing; per-strategy
sub-cap present but disabled in live config; `_forwarded_count` is process-memory (mid-day
restart refills the quota). Ruled-out alternative: "the caps are the binding defect" — raising
6→12 on 08-11 changed nothing because ordering, not capacity, decides who gets a slot.

**F2 — Sweep fill mechanics contain same-bar-close lookahead (B, → WO-2).** rsi2/trend/mom
signals are computed on day *t*'s completed close and filled at that same close
(`from_signals` with `price=None` → `np.inf` → same-row close, verified in installed vectorbt
source `portfolio/nb.py:1375-80`); no order submitted after observing close(t) can fill at
close(t). mom's sweep ranking additionally includes day *t* itself while live ranks through
*t−1*. Ruled-out alternative: "fee mispricing explains the positives" — refuted by W2c: fees
were calibrated at ₹20k but charged on ~₹100k books (pessimistic direction), so mechanics and
margins, not fees, carry the doubt.

**F3 — The swing promotions rest on near-zero margins at an exact boundary (B, → WO-2, WO-3).**
rsi2: four promotable runs, each exactly 12/15 = 80.0% == `fold_pass_min(N)` (passes only via
strict `<`), all 12 passing splits < 0.02%/day (min 0.00012), the 3 failing splits exactly 0.0
(no trades), and the winner config changed three times across near-identical runs. trend
(14/15) and mom (15/15) pass with more room but the same fill mechanics and sub-0.02%/day
median margins. These numbers cannot support live-capital decisions until re-run corrected.

**F4 — No spread or slippage anywhere in the cost surface (B, → WO-2).** CostModel/costs.yaml
are statutory-fee-only; the sweep charges zero slippage; the live gate's viability check uses
the same surface. `learning_ledger.slippage_entry/exit` columns exist and are never written.
Now measurable: stored ticks carry populated bid/ask (unused by any code path); measured
median spread 0.018%, p75 0.030%, closing-auction artifact rows must be filtered.

**F5 — brk20's stated entry is yesterday's close, and LIMIT sizing uses it (A, → WO-4).** The
analyst sees a fresher last-price line but nothing instructs an override; on the LIMIT path
the gate sizes off stale `action.entry_price` (live LTP used only for the sanity band); the
Telegram payload renders the stale price verbatim. Only the MARKET path sizes off live LTP.

**F6 — Late-tick amendment can silently rewrite official candles (A, → WO-5).**
`_handle_late_tick` is blind to `bar.src`: a stray tick for an already-reconciled day rewrites
`kite_official` high/low in place (src preserved, no re-heal — reconcile never revisits a
checkpointed day), plus a TOCTOU window against the 15:50 reconcile write. Post-15:30 ticks
also build ordinary bars that sit outside reconcile's comparison window forever.

**F7 — Analyst context is session-blind by mid-day (E, → WO-6).** Hard-coded 30×1m bar tail;
day plan (08:50) served hours later with no staleness marker; no day-high/low/VWAP-distance
aggregates; regime note and features carry no as-of stamps. Token metering itself is
disciplined.

**F8 — Tick capture starts when the engine starts (E, → WO-12).** Measured: first captured
bar 09:19 on 08-12, 09:52 on 08-11 — the opening 30 minutes, the most information-dense
window, are absent from tick storage on any late-start day (this also understated the audit's
own opening-range estimate). Any opening-range strategy or open-session spread work needs
pre-open starts.

**F9 — Parquet small-file pathology and hot-path reads (D, → WO-7, WO-8).** 752,150 files /
1.37 GB for ONE day of ticks (≈1.9 KB/file, ~7.5K files per symbol-day); `_atr_1m` performs a
per-bar `get_bars_1m` store read; sector-map read per candidate. Correctness unaffected;
read amplification and future replay cost are real.

**F10 — Trend warm-up floor under-serves young listings (A-minor, → WO-11).** `_MIN_DAILIES=60`
vs EMA(50) seed convergence: 9.07% residual seed weight at 60 bars (0.25% at 150). A 60–150
session listing can emit a live golden-cross with material seed noise — plausibly the worklog's
"GROWW young-classifier quirk".

**F11 — mom has no rebalance-day state (A-minor, known v1 gap, → WO-13).**
`mom_sessions_since_rebalance` is hard-coded None ⇒ every day is rebalance-due live, while the
sweep rebalances on schedule — live/backtest cadence mismatch on top of F2.

*Closed during this session (recorded here so the spec is complete): the job-watermark
degraded-return class (green-stamped failures, 9 jobs) — root-caused and fixed end-to-end,
commits f203b48…3cee8f3, 1219 tests green; deploy pending an owner-run service restart.*

## 4. Sound-as-is register (audited, healthy — one line each)

- Indicator math (RSI/EMA/ATR/ADX): matches independent from-scratch recomputation to 0.0 at
  every index; live and sweep share the identical functions; no divergent copy exists.
- Statutory cost model: exact Decimal per-component math, contract-note-verified to the paisa.
- Sweep MIS fee approximation: within ±0.01bps of exact below the ₹66,667 brokerage-cap point.
- Pre-open exclusion + auction handling: timestamp-based, auction price kept as a side field,
  open = first in-session print — pinned by tests (plan §9.3 wording is looser than the code).
- Cumulative-volume restatement guard: glitches contribute 0, high-water baseline kept, pinned.
- Close-bar flush: dual-triggered (opportunistic + unconditional 5s timer backstop).
- Reconcile drift math: §3.2.3 thresholds implemented strict->, boundary-tested, offline spans
  excluded from the denominator.
- RiskGate: 29-rule table with completeness test, monotone actions, news-free Tier-2 context,
  `analyst_confidence_min` governance (owner-only, API-rejected on the envelope route).
- Analyst prompt/discipline: byte-stable system prompt, no-quota no_action framing, absent-news
  = neutral; declines read as judgement, not artifact.
- News layer: entity resolution owner-scored 96%; story-level corroboration; single-source can
  never grade `originating`; bounded chain + orphan re-sweep now pinned by tests.
- Budget governor: ledger maths + degrade ladder (DG1 110%/85% → DG4) implemented as configured.
- §2.6 catch-up machinery: watermark-driven replay correct (now that results are forwarded).

## 5. Considered and rejected

- **Raising the analyst forward cap further** — capacity was never the binding defect after
  6→12; ordering is (F1). Re-propose only after WO-1 telemetry shows quality candidates missing
  slots again.
- **Lowering `analyst_confidence_min` 0.55** — engaged exactly once in history (BAJFINANCE,
  which independently failed eligibility); calibration data (3 scored no_actions) far too thin;
  the floor is owner-governed by design.
- **Softening catalyst corroboration / `catalyst_guard`** — owner-fixed guards, structurally
  sound, and the starvation issue was feed-side (fixed 08-05); no change.
- **Clusterer algorithm acceleration (quick_ratio prefilter)** — already declined 08-10 with
  reviewer concurrence (golden-file output change); stands.
- **Treating `bool False` job returns as watermark failures** — preopen_planner/nightly_review
  retry semantics involve LLM spend; needs its own design (WO-14), not a blanket rule.
- **Per-bar scanning of the full ~200 universe** — tick subscription cost + the measured
  arithmetic argue for batch daily rules (brk20's shape) as the blindness fix, not wider
  per-bar scanning.

## 6. Unverified leads (clearly separated; none load-bearing for the verdict)

- Magnitude of the rsi2/trend live-vs-backtest gap — quantifiable only via WO-2's re-run.
- Re-sweep convergence corner: a cluster aging past `max_event_age_days` mid-pass could in
  principle diverge on re-sweep (test pins the exercised boundary; interleaving-dependent).
- `date=1970-01-01` orphan tick partition (102 symbols) — provenance unknown.
- Negative-spread closing-auction tick rows (223/19,874 on RELIANCE 08-12) — filter rule
  needed wherever bid/ask gets consumed; cause unexamined.
- Safety-critical catch-up freeze-notify has no dedup (pre-existing; fires per attempt).
- Live incidence of post-15:30 ticks and of late-ticks-after-reconcile (F6 reachability is
  code-proven; frequency unmeasured).

---

# Part II — Work orders (for the implementer)

Execution roadmap (dependency-aware): **WO-1 → WO-2 → WO-3 → WO-9** form the critical path
(funnel fairness, then trustworthy numbers, then honest promotion, then measurement). WO-4,
WO-5, WO-6 are independent P1s. WO-10 strictly after WO-2. WO-7/8/11/12/13/14 as capacity
allows. Every C-category order stops for owner review after its experiment; nothing here may
weaken envelope/limits/kill/watchdog paths.

```
WO-1: Rank funnel admission by score; enable per-strategy caps; journal the forward counter
Category: A-correctness   Priority: P0
Evidence: prescreen.py admission (first-wins dedupe, arrival-order caps); pipeline.py
  forward-cap FIFO; SignalCandidate.score unread by any decision; settings.yaml per-strategy
  cap blank; WORKLOG 08-11 (66 candidates vs 6 evaluations; 55% of slots = orb); day-slot
  journal exists for publications but not the forward counter.
Change: (i) publication admission per bar-batch becomes score-descending selection instead of
  arrival order, preserving the existing per-(symbol,strategy) dedupe and the 20/day cap;
  (ii) analyst forwarding selects the highest-score published candidate not yet evaluated at
  each analyst slot, not the earliest — implemented as a small priority queue keyed
  (score desc, fired_at asc) drained by the existing forward-cap pacing; (iii) enable
  per-strategy publication caps in settings.yaml with a starting split that structurally
  prevents any single strategy exceeding 40% of the day's slots (orb explicitly capped;
  exact per-strategy values are an owner knob, not learner-movable); (iv) persist the
  forward count into the existing day-slot journal so restarts resume, not reset.
  Scores are comparable within a strategy but not across; cross-strategy selection uses
  per-strategy score quantile (rank within that strategy's day) as the sort key, falling
  back to fired_at inside a quantile band — state this rule in code and plan.
Plan amendment: §3.2.5 (admission rule), §5.2(a) (forward selection), §6.3 note (per-strategy
  caps owner-only).
Acceptance: unit tests — burst of N>cap candidates admits the top-scored per the stated rule,
  not the earliest; restart mid-day preserves forward count (journal-hydrated); per-strategy
  cap binds; an orb flood cannot exceed its cap while rsi2 slots remain. Live: next session's
  funnel telemetry (WO-9) shows forwarded-candidate score distribution ≥ published median.
Effort: M   Depends-on: —
Risk: changes which candidates reach the analyst (that is the point); no gate/OMS surface
  touched; rollback = config flag to arrival-order.

WO-2: Correct the sweep's fill mechanics and cost surface; re-run and re-validate everything
Category: B-validation-methodology   Priority: P0
Evidence: same-bar-close fills verified into vectorbt source (price=None → inf → same-row
  close); zero spread/slippage anywhere; sizing/calibration mismatch (init_cash=100000 vs fee
  calibrated at reference_notional=20000); measured spread medians (top/mid/tail =
  0.0135/0.0180/0.0219%, p75 0.0303%); mom sweep ranking includes day t.
Change: (i) daily-strategy sweeps fill at NEXT session's open: pass an explicit price frame
  (open shifted −1 session) to from_signals for entries and exits, or equivalently shift
  signals +1 and fill at open — pick one, document it in the module docstring and every
  report's modelling_notes; (ii) add a spread component to the cost surface: new
  config/costs.yaml key spread_pct (default 0.02, the measured overall median rounded up)
  applied per leg as half-spread in CostModel round-trip AND as vectorbt slippage in sweeps;
  keep it a plain config value (not learner-movable); (iii) fix the sizing mismatch: size
  sweep trades at fixed cash = the live per-trade notional (₹20k) so the constant fee is
  calibrated at the traded size, or compute per-order exact fees — either, stated; (iv) mom
  ranking excludes day t (t−1 window, matching live); (v) re-run all four sweeps + CPCV on
  the canonical window and re-issue reports.
Plan amendment: §6.4 (validation semantics — fill timing + spread), §14 add the measured
  spread numbers with source.
Acceptance: a regression test pins next-open fill (a synthetic series where same-close vs
  next-open fills differ materially produces the next-open number); CostModel unit tests
  extended for spread; re-run reports exist for orb/rsi2/trend/mom with modelling_notes
  stating the new mechanics. THE RE-RUN'S RESULTS INVALIDATE: rsi2 +0.58%/trade, trend
  3.73%, mom 4.05%, and all four promotability verdicts — prior reports remain on disk as
  history, marked superseded in the WORKLOG entry.
Effort: M   Depends-on: —
Risk: research-layer only; no live-path change except CostModel spread (which TIGHTENS the
  gate's viability check — direction is conservative); rollback = revert config + reports.

WO-3: Harden the promotion rule against boundary-exact, near-zero-margin passes
Category: B-validation-methodology   Priority: P1
Evidence: four rsi2 promotions all exactly at 80.0% (strict-< pass); all passing splits
  < 0.02%/day; winner instability across grid densities (three different winners).
Change: promotion_decision additionally requires (a) median passing-split expectancy ≥ a
  floor expressed as a multiple of the strategy's per-trade cost at the sweep's sizing
  (default: cost_floor/20 per day — calibrate so today's rsi2 fails and a genuinely
  cost-clearing edge passes; document the chosen constant), and (b) a winner-stability note:
  the validate report records whether the same winner was selected at the adjacent grid
  density; instability is a report-level flag, not an auto-fail. fold_pass_fraction
  comparison stays strict-< (changing it silently is a semantics trap — the margin floor is
  the real fix).
Plan amendment: §6.4 promotion rule.
Acceptance: unit tests at the exact boundaries (12/15 with sub-floor margins ⇒ not
  promotable; 12/15 with clear margins ⇒ promotable); re-issued reports show the flag.
Effort: S   Depends-on: WO-2 (run once, on corrected numbers)
Risk: research-layer; makes promotion strictly harder; no rollback concern.

WO-4: brk20 entry semantics — recommend a level, size off a live reference
Category: A-correctness   Priority: P1
Evidence: entry = yesterday's close; gate LIMIT sizing uses stale action.entry_price; payload
  renders it verbatim; only the MARKET path uses live LTP.
Change: brk20 candidates carry entry_type=limit-at-level with entry = the broken 20-day-high
  LEVEL (the actionable trigger), never yesterday's close; the gate's sizing for ANY LIMIT
  proposal uses max(entry_price, live LTP) as the risk-reference when long (min when short)
  so size can never be computed off a price better than obtainable; the payload states both
  the level and the current price. The entry_sanity_band hard-reject stays.
Plan amendment: §6.1 brk20 row; §3.6 payload note.
Acceptance: unit tests — brk20 candidate entry equals the level; gate sizing uses the live
  reference when LTP has run past the stated entry; payload renders both prices.
Effort: S-M   Depends-on: —
Risk: touches gate sizing input selection (Tier-2) — table-driven tests per §9.1 required;
  monotonicity untouched (sizing reference can only shrink size, never enlarge).

WO-5: Guard official candles from late-tick amendment; exclude post-close ticks
Category: A-correctness   Priority: P1
Evidence: _handle_late_tick amends regardless of bar.src (kite_official rows rewritten in
  place, src preserved, no re-heal); read-decide-write TOCTOU vs the 15:50 reconcile thread;
  no post-15:30 exclusion symmetric to the pre-open one.
Change: (i) a late tick targeting a bar whose src != 'self' is recorded to corrections_log
  (amended=false, reason='official_bar_untouchable') and the bar is left intact; (ii) ticks
  with ts.time() > session_close are dropped-and-counted exactly like pre-open ticks (same
  side-channel logging); (iii) the self-bar amendment path re-reads inside a single store
  transaction (or compare-and-swaps on a row version) to close the TOCTOU.
Plan amendment: §3.2.3 (amendment rules), §9.1 bullet for the new tests.
Acceptance: tests — late tick on a kite_official bar leaves it byte-identical + logs; post-
  close tick builds no bar; existing self-bar amendment tests still green.
Effort: S-M   Depends-on: —
Risk: data-integrity layer; strictly narrows what can mutate stored bars; rollback trivial.

WO-6: Analyst context — session-scale aggregates, as-of stamps, staleness markers
Category: E-observability (analyst input quality)   Priority: P1
Evidence: hard-coded 30×1m tail; 08:50 day plan served without age; no day H/L/VWAP-distance;
  no as-of stamps on regime/features blocks.
Change: add to the volatile block: session aggregates line (day high/low, %-from-open,
  %-from-VWAP, session elapsed), an as-of stamp on every block, and an explicit age line on
  the day plan ("authored HH:MM, N h ago"). Token cost bounded (~4 lines); no new tables.
Plan amendment: §5.2 context contents list.
Acceptance: context unit test asserts the new lines + stamps; byte-stable system prompt
  unchanged (D8 cache discipline intact); live token delta measured < 5%.
Effort: S   Depends-on: —
Risk: prompt-cache invalidation on the volatile block only (already volatile); none further.

WO-7: Tick store compaction
Category: D-architecture-efficiency   Priority: P2
Evidence: 752,150 files / 1.37 GB per day (~1.9 KB/file); 2.1M files total and growing.
Change: nightly EOD job compacts each date's per-symbol fragments into one parquet file per
  symbol-day (write-new + atomic swap + fragment delete, checkpointed via job_runs like any
  date-keyed job); tick writer batches larger flush units going forward (target ≥ 1 MB or
  60 s per file, whichever first). Readers already glob partitions — no read-path change.
Plan amendment: §4.3 storage note.
Acceptance: compaction job test on a fixture partition (row-identical before/after, fragment
  count 1 after); live: next-day file count per symbol-day == 1; replay read of a compacted
  day byte-identical rows.
Effort: M   Depends-on: —
Risk: storage layer only; swap is atomic per symbol-day; rollback = stop the job (fragments
  and compacted files are equivalent to readers).

WO-8: Hot-path read hygiene
Category: D-architecture-efficiency   Priority: P2
Evidence: _atr_1m does store.get_bars_1m per bar; sector map read per candidate.
Change: maintain ATR incrementally per watched symbol from the bar stream (seeded once at
  warmup), falling back to the store read only on gap; cache the sector map per day (it
  changes weekly). Quantify before/after with a one-line timing log around a session.
Plan amendment: §3.2 hot-path invariants note.
Acceptance: equivalence test incremental-vs-store ATR over a recorded day (exact); timing
  log shows per-bar store reads eliminated in steady state.
Effort: S-M   Depends-on: —
Risk: hot path — equivalence test is the guard; rollback flag to the store-read path.

WO-9: Funnel utilization telemetry (makes WO-1's effect measurable)
Category: E-observability   Priority: P1 (ships with WO-1)
Evidence: today the funnel's behavior was reconstructable only by forensic DB reads.
Change: one structured EOD log line + nightly-review section: raw candidates, published
  (per strategy, with score quantiles), forwarded (scores), evaluated, proposals, gate
  verdicts; plus a "best unforwarded score" field — the direct measure of starvation.
Plan amendment: §10.3 reporting note.
Acceptance: unit test on the aggregator; line visible in the next live EOD.
Effort: S   Depends-on: WO-1 (shares the journal)
Risk: none (read-only aggregation).

WO-10: Pre-registered experiment — intraday VWAP-deviation reversion at 15–60-min horizon
Category: C-alpha-experiment   Priority: P1 (the only new intraday alpha work sanctioned)
Evidence: the measured arithmetic leaves 30-min+ horizons open (floor = 0.18× opening range);
  intraday mean-reversion is the one family the record has never tested; hindsight replay
  showed touch-entry momentum negative, not reversion.
Hypothesis (pre-registered): NIFTY200 symbols stretched ≥ X% from session VWAP (X swept over
  a SMALL fixed grid {1.0, 1.5, 2.0}) with relative volume < 1.5× revert toward VWAP within
  15–60 min by more than 2× the full cost floor (fees + measured per-tier spread), long side
  only, entries 10:00–14:00, exits at VWAP-touch / 60-min timeout / fixed stop 1.5× the
  10-min ATR.
Dataset/window: 1m bars, full available history (2024-01-01 floor for dailies; 1m from
  2025-07-10), universe = the historical NIFTY200 list per COMMANDS.md; costs = WO-2's
  corrected surface INCLUDING spread; fills at next-bar open (never the signal bar).
Promotion bar: house CPCV + fold_pass_min + WO-3's margin floor, N = the grid cardinality
  (9 combinations max — keep it small on purpose).
Abort criterion: if the UNCONDITIONED 15–60-min reversion base rate after costs is ≤ 0 in the
  first full-window pass, stop — do not tune the condition until something clears zero.
STOP for owner review after the experiment report; no live wiring of any kind.
Effort: M   Depends-on: WO-2
Risk: research-only; the risk is multiplicity — contained by the fixed small grid and the
  pre-registered abort.

WO-11: Trend warm-up floor for young listings
Category: A-correctness   Priority: P2
Evidence: _MIN_DAILIES=60 vs EMA(50) seed convergence (9.07% residual at 60 bars, 0.25% at
  150); WarmupGate young-listing exemption lets these through.
Change: raise TrendScanner's floor to 150 completed dailies (or gate on computed residual
  seed weight < 1%); state the constant's derivation in the code comment.
Plan amendment: §6.1 trend row note.
Acceptance: unit test at 149/150 boundary; young-listing fixture emits nothing at 60.
Effort: S   Depends-on: —
Risk: fewer trend signals on young names (intended).

WO-12: Pre-open engine start so tick capture covers the open
Category: E-observability / ops   Priority: P2
Evidence: first captured bar 09:19 (08-12) and 09:52 (08-11); opening 30 min absent from
  tick storage on late-start days; opening-range work and open-session spread measurement
  are blind exactly where the information density peaks.
Change: document the pre-open start requirement in RUNBOOK (start by 09:05 IST); optionally
  enable the §14 Q14 wake-capable scheduled task for trading days (owner's call — Q14
  recorded manual-start-primary deliberately; this only adds the documented option).
Plan amendment: none (Q14 already covers it) — RUNBOOK only.
Acceptance: RUNBOOK section; if the task is enabled, one observed 09:0x auto-start.
Effort: S   Depends-on: — (owner decision on the scheduled task)
Risk: none.

WO-13: mom rebalance-day state
Category: A-correctness   Priority: P2
Evidence: mom_sessions_since_rebalance hard-coded None ⇒ live treats every day as
  rebalance-due; sweep rebalances every 15 sessions — cadence mismatch.
Change: persist last-rebalance session in the day-slot journal (or a one-row table); live
  mom fires only when due; context provider supplies the real value.
Plan amendment: §6.1 mom row (removes the recorded v1 gap note).
Acceptance: unit test — not-due day emits nothing; due day emits; restart preserves state.
Effort: S   Depends-on: —
Risk: fewer (correct) mom signals.

WO-14: Design decision — bool-returning advisory jobs and the watermark
Category: A-correctness (design)   Priority: P2
Evidence: preopen_planner/nightly_review return False (governor-blocked or harness-failed)
  without raising; wrappers discard it; _job_result_ok deliberately ignores bare bools —
  their runs green-stamp regardless. A blind retry loop would burn LLM budget.
Change: PRESENT OPTIONS to the owner, implement the chosen one: (a) status quo documented;
  (b) forward with a governor-aware retry cap (e.g. one retry after 30 min, only if the
  governor admits it); (c) failed-watermark + retry only for harness-failure, never for
  governor-block. Recommendation: (c) — a governor block is a correct outcome, not a failure.
Plan amendment: §2.6 job-classes note.
Acceptance: per chosen option; tests pin whichever semantics land.
Effort: S   Depends-on: owner decision
Risk: bounded LLM spend if (b)/(c) — the cap is the guard.

WO-15: §2.6 boot ordering — never-load-bearing chain behind scheduler arming
Category: D-architecture (resilience)   Priority: P1
Evidence: the news chain (backfill→cluster→score→digest) runs inside lifecycle.startup
  BEFORE scheduler.start() (main.py boot tail); the 08-10 wedge starved ALL scheduled work
  for 8h including the catchup_sweep that exists to self-heal; scheduler guard is calendar-
  only (no recovery-awareness), so naive early arming is unsafe; digest staleness already
  degrades cat safely (digest_stale_max_h).
Change: (i) boot catch-up runs load-bearing data steps only (instruments, surveillance,
  universe, candle gap-fill, watermark-driven EOD replays); the news chain + catalyst_digest
  + preopen_planner move to immediately-after-scheduler.start() one-shot jobs (same code,
  new firing point); (ii) CatchUpRunner gains a single-flight asyncio lock so the 30-min
  sweep can never race a still-running pass (required by the reordering; APScheduler
  max_instances=1 already serializes sweep-vs-sweep); (iii) the already-filed bounded+
  observable-steps proposal applies to the steps that REMAIN in boot.
Plan amendment: §2.6 (boot ordering), §2.7 (chain firing point).
Acceptance: boot-wiring tests — scheduler armed before the chain fires; chain failure/wedge
  cannot delay engine_ready (bounded by its own 600s resolve cap post-arming); single-flight
  test (sweep during in-flight pass = no-op + log); a simulated news-backlog boot reaches
  engine_ready in load-bearing time only.
Effort: M   Depends-on: owner go (this is the standing §2.6 structural question, answered)
Risk: boot path (three recent wedges = well-tested territory now); the digest fires minutes
  later than today on normal mornings (identical to current behavior in wall-clock terms —
  it ran inside boot anyway); rollback = firing-point flag.
```

```
WO-10b: Morning-window VWAP-reversion variant (owner-directed 2026-08-14, after WO-10's abort)
Category: C-alpha-experiment   Priority: P1
Evidence: WO-10 aborted at stage 1 (unconditioned base rate −0.12742%/trade ≈ the cost floor;
  gross ≈ 0) but its 10m-ATR warm-up meant ONLY 11:36–14:00 was measured — the 10:00–11:35
  morning window, where reversion dynamics are typically strongest, was never observed.
Hypothesis: identical to WO-10, measured on the morning window the original could not reach.
Change (pre-registered): the stop unit's ATR seeds from the PRIOR session — the Wilder ATR(14)
  recursion continues from the previous session's final 10m-ATR value, with the current
  session's FIRST bucket's true range computed high−low only (no prev-close term: the overnight
  gap must never enter the stop unit); a symbol with no prior-session ATR produces no trades
  until its own warm-up completes (fail-closed, same rule as WO-10). Entry window 10:00–14:00 as
  originally registered — now effective from 10:00; results additionally reported split
  10:00–11:35 vs 11:36–14:00 so the morning increment is visible against WO-10's answer. Same
  two-stage structure, same abort criterion on the SAME windows it measures, same grid, same
  costs/fills/exits. No other change to the WO-10 pre-registration.
Acceptance: report generated; stage-1 base rate reported overall AND for the morning split;
  STOPS FOR OWNER REVIEW; no live wiring, no param_sets row.
Effort: S (variant of the shipped harness)   Depends-on: WO-10 (shipped)
Risk: research-only; the seeding rule is itself pre-registered above, not tuned.

WO-16: Re-check insider_net_buy under corrected mechanics (owner-directed 2026-08-14)
Category: B-validation-methodology   Priority: P0 (it is the last recorded positive)
Evidence: insider_net_buy +0.75%/+1.61% net T+10/T+20 (n=110 position-days, 2026-07-17) and the
  2026-07-19 CPCV stage-2 (+0.0361%/day, 60% folds) were produced BEFORE the WO-2 corrections;
  the filings PIT source carries a ~70-day content embargo (memory: PIT ~2.5 months stale).
Change: (i) AUDIT the event-study/validated mechanics first, before any re-run: entry timing vs
  the event timestamp (same-bar/next-open?), the cost surface used, and — critically — WHICH
  date anchors the event: the insider's TRANSACTION date or the public DISCLOSURE/broadcast
  date. A backtest entering on transaction dates that were not knowable until weeks later is
  lookahead of the worst kind and invalidates the result regardless of costs. (ii) Re-run with:
  disclosure-date anchoring (if not already), next-session-open entries, the corrected CNC cost
  surface including spread (0.3192% round trip at ₹20k), and the WO-3 margin floor applied to
  the CPCV stage. (iii) Report old vs new side by side; state plainly which recorded claims
  survive.
Acceptance: the audit findings with file:line pointers; the re-run report; a one-line verdict:
  insider_net_buy SURVIVES / DOES NOT SURVIVE corrected mechanics.
Effort: S-M   Depends-on: —
Risk: research-only.

WO-17: Stop-geometry / adverse-excursion recovery study on intraday longs (owner-directed 2026-08-14)
Category: C-alpha-experiment (diagnostic — promotion NOT sought)   Priority: P1
Evidence: owner hypothesis — allowing a deeper adverse dip on an intraday buy may let noise-hit
  positions recover to a positive sell, vs the current tight stop realizing the dip as a loss.
  The hindsight replay (07-29..31) recorded "the declines dodged 8 stops"; each stop-out
  realizes the full round-trip cost floor.
Null hypothesis (stated up front): on a driftless path, ALL stop geometries have equal pre-cost
  expected value (optional stopping) — any measured difference between stop widths is evidence
  of post-dip CONDITIONAL drift (genuine recovery tendency after adverse excursion), which is
  exactly what the owner's hypothesis asserts and what this study measures.
Pre-registration: population = the orb entry-signal stream (the highest-volume recorded
  intraday long family) over the full 1m window, entries next-1m-bar-open per WO-2; per entry,
  simulate a FIXED axis of stop widths {1.0, 1.5, 2.5, 4.0} × ATR_10m (WO-10's stop unit,
  prior-session-seeded per WO-10b so morning entries are stoppable) plus NO-STOP
  (session-end square-off only); TWO exit ladders per width, both fixed: (a) orb's own
  1.5R target + stop, (b) no target — exit at session end or stop. 10 configs total; per
  config report net expectancy at ₹20k MIS incl. spread, win rate, and the two diagnostic
  quantities the hypothesis lives on: DODGED-WINNER FRACTION (of trades stopped at width w,
  the share that would have ended positive by session end) and the MAE distribution of
  eventual winners (what dip depth winners actually survive). Promotion is NOT sought — the
  deliverable is the stop-policy evidence table; any live stop-guidance change that follows is
  a separate owner-approved change (analyst prompt / plan §6.1), never automatic.
Acceptance: report with the 10-config table + the two diagnostics + a plain-language answer to
  the owner's question; STOPS FOR OWNER REVIEW.
Effort: M   Depends-on: WO-10b's seeded ATR (shared unit)
Risk: research-only; multiplicity contained by the fixed axes and the no-promotion rule.

WO-18: `cat` confirmation-mechanic review + shadow start (owner-directed 2026-08-18)
Category: B-validation-methodology   Priority: P1
Evidence (fuel measurement, engine-down DuckDB window 2026-08-18 01:08-01:11 IST, queries in
  scratchpad measure_cat_fuel.py; catalyst_watchlist 480 rows 2026-07-29..08-17):
  - Originating-grade flow since the 2026-08-05 corroboration amendment: 5 rows / 3 DISTINCT
    stories in 9 sessions (HAL rating_change 08-07, LT order_win 08-14, RELIANCE m_and_a
    08-17; the age-2 rows are same-story re-grades). Rate 0.33 stories/session, 0.56
    rows/session — MARGINAL vs the pre-registered >=0.5/day bar, accepted because the
    event-class mix is 100% NON-earnings: exactly the classes the three recorded refutations
    (all earnings-anchored) never tested.
  - All 5 originating rows are top-100-watchlist symbols (1m data exists), all direction=long.
  - Gate-miss structure of the 390 post-amendment context rows: materiality < 0.70 is the
    dominant single binder (only 9 rows had materiality >= 0.70); 37 rows failed ONLY the
    materiality floor. Thresholds are NOT moved (multiplicity — the ins lesson); near-miss
    counts are diagnostics only.
  - Story-level domain-union >= 2 holds for ~19% of resolved stories (52/268) — passable,
    binding, working as designed.
  - Resolver leak (separate from cat): most "LG Electronics" clusters resolve NO symbol (the
    dump legal name is "lg electronics india"; the press writes "LG Electronics") — e.g. the
    08-13/08-14 LGEINDIA Q1 story: 2 resolved of ~8 clusters. Curated-alias suggestion:
    { alias: "LG Electronics", tradingsymbol: LGEINDIA }. Aliases are owner-set by contract
    (config/aliases.yaml header) — SUGGESTED here, deliberately NOT applied.
Decision (the plan-line-278 owner review, executed): the +1% intraday price/volume confirmation
  is RETIRED for the shadow — refuted 3x on proxies (net -0.17..-0.44%), and
  catalyst-conditioning made intraday ORB worse; the drift is a 2-4-week phenomenon. `cat` v2
  shadow mirrors the validated `ins` mechanics instead: EOD-graded originating row (event age
  <= 1 session, single shot, no re-fire) -> next-session sweep -> swing candidate anchored on
  reference_close (prior-session bhavcopy close), disaster stop 5%, target None, exit = the
  20-session max-holding path, score = row materiality. NO cat.expected_edge_pct is configured:
  the C3 cost gate therefore fail-closed-rejects every cat candidate — correct for an
  unvalidated edge; the validation population is the prescreen-admitted signal stream, not gate
  outcomes. Full plan text: IMPLEMENTATION_PLAN.md §2.7 2026-08-18 amendment.
Acceptance (kill criteria pre-registered): shadow runs >= 30 sessions AND >= 20 admitted
  signals; then measure T+10/T+20 net drift (next-session-open fills, spread-inclusive CNC
  costs, WO-3 margin floor) of the signal population. Net <= the cost floor at BOTH horizons =>
  `cat` origination is retired and the watchlist stays context-only permanently. Clearing it =>
  owner review at the §8.6 gate (R4) before any live wiring. At the measured rate the earliest
  verdict is ~mid-October 2026; sustained < 0.2 signals/session for 3 consecutive weeks is a
  STARVATION finding and triggers an early owner check-in instead of silent accumulation.
Effort: S (scanner mirrors ins.py; caps/guards pre-exist)   Depends-on: —
Risk: shadow-only; Phase 2 is structurally paper/RECOMMEND, C3 rejects by construction, and
  catalyst_guard.max_catalyst_entries_day (2) + max_per_strategy_day cat: 2 bound the slots.
Day-0 addendum (2026-08-18, ~03:15 — BEFORE the first shadow sweep, so the population is not
  contaminated mid-stream): the resolver-universe bug found the same night (EntityResolver +
  CatalystDigestJob loaded included_only=True = the top-100 WATCHLIST, silently dropping every
  eligible-but-sub-cap symbol at resolution) was fixed before any cat signal existed. Two
  consequences for reading this WO's evidence: (i) the measured 0.33-0.56/day fuel rate was
  observed UNDER the bug and is a LOWER bound — sub-cap news (roughly half the eligible universe)
  could not reach originating grade at all; the starvation thresholds stay valid as lower bounds.
  (ii) "all 5 originating rows are top-100-watchlist symbols" is EXPLAINED by the bug, not a
  property of the flow. The shadow's population definition from signal #1 onward is the FIXED
  eligible universe (store.get_universe_eligible_symbols).
```

---

# Part III — Appendix

**Measurement provenance (spread/range, W2b):** 48 symbol-days = 24 symbols (8 top / 8 mid /
8 tail by tick-derived liquidity proxy) × sessions 2026-08-11 and 2026-08-12, from
data/parquet/ticks via in-memory duckdb read_parquet with exact date=/symbol= globs
(market.duckdb never opened; pyarrow absent from venv). Continuous session 09:15–15:20
filtered; bid≤0/ask≤0/ask<bid rows dropped (223 closing-auction negative-spread rows on
RELIANCE 08-12 among them). Headline numbers: spread median 0.0180% (0.0135/0.0180/0.0219 by
tier, p75 0.0303%); 1m range median 0.0606%; ATR(14,1m) 0.0692%; opening range 0.708%
(measurable on 24/48 symbol-days — capture started 09:19/09:52, see F8); day range 1.502%;
floor/1m = 2.05× (1.98 on 08-12, 2.07 on 08-11). Fees verified against the engine's own
CostModel (MIS ₹20k round-trip 0.1063%).

**Cost curve (seed 6):** exact-vs-approx breakeven at 5k…100k both products via a scratch
script importing engine first; MIS ±0.01bps below cap-crossover ₹66,667, +2.36bps at ₹100k;
CNC −23.02bps at ₹5k → +6.14bps at ₹100k (DP-charge geometry).

**Sweep/validation record (seeds 7-8):** 17 report pairs read from data/reports/. ORB 5 runs
all 0/15 (windows 119/247/498 sessions). rsi2 four promotable runs all exactly 12/15=80.0%,
winners {3,10,80,4}→{3,2,65,6}→{15,2,65,6} across densities; newest
(rsi2_20260716T180922.json) passing splits min 0.0001236 / median ~0.0006 %/day, failing
splits exactly 0.0. trend 14/15 (failing split −0.0024), mom 15/15 (max 0.0058 %/day).
CPCV = 6 folds choose 2, purge/embargo 5 sessions (positional, despite the DAYS naming);
walk-forward is monthly, descriptive-only (no pass field, never gates).

**Analyst record (seed 5, state.db read-only):** agent_calls intraday_analyst 273 total →
212 ok (210 no_action / 2 enter) + 61 failed (46 schema_invalid, 13 timeout, 2 sdk_error);
signal-candidate-only: 140 → 96 no_action, 2 enter, 42 failed. Both enters 2026-07-31
(10:02, 10:07), both gate-rejected on instrument_eligible (is_fno=False pre-fix-7fdf7fe;
BAJFINANCE also confidence 0.54<0.55). Five verbatim no_action theses quoted in the wave-1
output — all substantive.

**MIS eligibility timeline (w2a):** universe_built mis_candidates: 0/50 at 09:04 on 07-31
(pre-fix) → 49/50 at 11:07 (post-fix commit 7fdf7fe, which names both killed proposals);
96/100 on 08-11, 08-12, 08-13. Eligibility = watchlist ∩ is_fno; watchlist entry already
requires Kite MIS-leverage-file eligibility (fail-closed on unknown symbols).

**G2 evidence collector first run (2026-08-13 15:13):** digest-before-window-open 8/12 =
66.7% (bar ≥90%); news analyst 346/346 schema-valid; intraday 227/273 = 83.2% attempt-level
(24/25 on 08-13); recommendations 0 rows ever; MTD spend $31.29 vs $120 (console-diff band
$28.16–$34.42). Command: `.venv\Scripts\python.exe scripts\g2_evidence.py`.

**Windows/floors:** 1m history floor 2025-07-10 (extended once to 2023-07-17 per WORKLOG
07-12/13 — only the newest orb run used the wider window); dailies from 2024-01-01 in all
recorded sweeps; full-universe tick coverage only since 2026-08-04.

**What was NOT run:** no sweeps or CPCV re-runs (WO-2 owns that); no market.duckdb access
(engine held the writer lock throughout — bar-level range stats were tick-reconstructed
instead); no live broker/API calls beyond one HTTP HEAD to the public NSE archives URL.
