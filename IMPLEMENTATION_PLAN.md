# IMPLEMENTATION_PLAN.md — what is left to build

This file holds only what is not built yet: the gates ahead, the verdicts and owner decisions still pending, and the
open questions. Built work is described by the code. When an item here ships, delete its text; do not mark it done.

## Where things stand

- RECOMMEND mode passed its readiness gate on 2026-10-05. The engine recommends; the owner trades by hand.
- The paper autopilot has been live since 2026-10-08 (owner: "Implement the auto paper trade, though it should not
  generate actual trade execution."). Paper trades CNC only. A real order is structurally impossible:
  `feature_flags.paper_only` refuses AUTO and live routing, and `KiteClient` refuses every order and GTT call unless
  built with `orders_enabled=True`, which production never does.
- The paper-to-live gate's clock has not started. It needs MIS and live-order machinery that the CNC-only paper path
  does not have: SquareOffScheduler, MIS SL-M protection, the shutdown guard and its CNC-GTT verification.

## Invariants the remaining work must keep

- **Three tiers:** Claude proposes, the deterministic RiskGate disposes, the deterministic OMS executes. The platform
  stays safe with the LLM tier dead.
- **Broker-resident protection:** every live position has a protective order resting at the broker (SL-M for MIS,
  GTT OCO for CNC), so protection survives process, PC and token death.
- **Kill stickiness:** a kill is persisted before any order action and checked on every startup; only the owner's
  two-step flow resets it.
- **Single user:** recommendations and execution serve the owner's own account only. Distributing them would make the
  owner an unregistered Research Analyst or algo provider, a SEBI violation.
- **Order throttle:** at most 1 order call per second sustained (burst 2) through the single limiter in
  `engine.broker`, so the platform stays an unregistered retail algo.
- **Success is not profit before live AUTO:** process quality, capital preservation and a measured record of what
  works. Profit is tracked once live AUTO runs and targeted only in the online-learning stage.
- **Research registration bar:** a new `expected_edge_pct`, or a promotion out of shadow, must cite an entry in
  `config/research_trials.yaml`, pass CPCV at `fold_pass_min(N_family)`, and have a permutation
  p < `registration_alpha(N_family)` (`engine.learning.multiplicity`) against same-size random subsamples of its
  matched population.

## Pending verdicts and owner decisions

- **hi52 forward test.** Mechanical verdict: `scripts/hi52_forward_verdict.py` (kill rule and pins in the script and
  `settings.yaml` `hi52`).
- **Paper no-stop exit test** (hi52, ins, brk20 paper positions carry no stop and exit at session 20). Verdict at the
  pinned looks 20 and 40: `scripts/no_stop_verdict.py`; operation and rollback in RUNBOOK "No-stop exit test". From
  the registration date below, brk20 recs are regime-ON only and use the 5-session retest window, and there is no
  confidence floor (removed 2026-10-30 after the close, so the registration date is its first session too). The
  paired stop-vs-no-stop difference stays valid; read the slices split at the registration date by hand.
  `scripts/no_stop_verdict.py` is untouched. **Look 1 formed by 2026-11-03: STOP NOT THE PROBLEM** (no-stop excess
  −1.34 pp, paired difference −2.02 pp, median net T+20 −6.8% over the first 20 fills). Owner (2026-11-03): the test
  runs on to look 2, which supersedes it.
- **`cat` v2 shadow verdict — not yet run.** Mechanical verdict: `scripts/cat_shadow_verdict.py` (conventions in the
  script; COMMANDS "cat v2 shadow verdict"). Pre-registered: at ≥30 sessions and ≥20 admitted signals, measure T+10 and
  T+20 net drift at spread-inclusive CNC costs with the margin floor. Net ≤ the cost floor at both horizons ⇒ `cat`
  origination is retired and the watchlist stays context-only; clears ⇒ owner review for live (the `cat` live-origination
  conditions under "Live AUTO"). Thresholds (`cat.materiality_min` and the rest) stay frozen for the whole window; moving
  one restarts the clock. The clock restarted on 2026-08-28 (signals 2026-08-18..28 excluded). Split the sample at the
  first NIFTY 500 universe build (2026-09-04) and at the news scorer's switch to `sonnet-5.5`
  (`news_clusters.scorer_model`). On 2026-11-09 the owner accepted a wording-only change inside the window: the News
  Analyst's schema descriptions lost their retired-plan citations (no field, enum or rubric changed).
- **`cat_reversal` shadow verdict.** Pre-registered: at ≥60 sessions and ≥15 admitted signals, T+5 and T+10 net at
  next-open fills, spread-inclusive CNC costs and the margin floor; net ≤ the cost floor at both ⇒ retired; clears ⇒
  owner review. Fewer than 1 admitted signal per 20 sessions across the first 60 triggers an early owner check-in.
  The same 2026-11-09 wording-only News Analyst change applies here.
- **Waiting on the `cat` v2 verdict (owner, 2026-09-24):** wiring the results line items (`revenue`, `pat`) into the
  News Analyst and the three resolver alias fixes (`engineers`, `dalmia bharat`, `pfizer`) land together right after
  it. Switching the clusterer's `cat.max_event_age_days` from calendar days to sessions is also decided after it.
- **Exit-geometry study (run 2026-10-07):** every shipped stop turned its strategy's measured edge negative. Owner work:
  hi52's switch to a 2.5×ATR14 catastrophe stop (its +0.22% median net would not clear the cost check once the edge is
  re-derived). brk20 and ins came out `neither`: no change.
- **Regime-filter study (run 2026-10-07):** brk20 `supported`; the 2026 held-out slice contradicts it. From the
  registration date it applies at brk20 origination in the batch sweep (`settings.yaml` `brk20.regime_filter`; `false`
  plus a restart rolls it back), and resting levels are not re-judged. Blocked crossings are journalled in
  `brk20_regime_blocked` and scored as the `rule_blocked` arm, a diagnostic only. hi52 not supported; ins insufficient.
- **Selection-filter study (run 2026-10-07):** not evidence. A filter gate needs a new pre-registration with a
  permutation test against same-size random subsamples.
- **Fibonacci study (run 2026-11-04, `scripts/backtest_fib.py`):** the Fibonacci-pullback edge is null, the 50%/61.8%
  pullback clustering is supported but tiny, brk20 confluence is refuted (hi52 and ins insufficient), and the deeper
  brk20 entry is supported (the limit at 61.8% of the breakout leg: median net +1.53% against the shipped 5-session
  limit's +0.61%). Post-hoc, the deeper entry's win is a depth effect, not a Fibonacci one (67.8% did better); it fills
  12% of signals against 59%, and in 2026 every depth lost about 2%. Owner decision: whether a deeper brk20 entry gets
  its own registration once the brk20 kill rule reads. Nothing is wired.
- **Spread re-measure:** `costs.yaml spread_pct: 0.02` was measured on 2026-08-13 on the NIFTY 200 tick corpus.
  Provisional rule: re-measure (never tune) when the universe changes materially or quarterly, whichever is sooner. The
  universe moved to NIFTY Total Market on 2026-10-08 (owner decision).
- **Fee schedule:** `costs.yaml` says `verified_on: 2026-06-11`; the rule is to re-scrape before each release
  (`scripts/rescrape_costs.py`).

**Forward rules pre-registered 2026-10-29.** The registration date is 2026-11-02, the first session that opened after
the deploy (`engine.learning.forward_verdicts.REGISTERED_FROM`). A deploy after 09:15 IST on 2026-11-02 moves it to the
first session after it, here and in the constant. Each rule has a `registered` row in `config/research_trials.yaml`.
The owner re-registered the analyst-value verdict on 2026-11-09: cleaning retired-plan citations out of the analyst
prompts moved every strategy's `prompt_version`, so its registration date is 2026-11-09
(`forward_verdicts.ANALYST_REGISTERED_FROM`); the rows accrued since 2026-11-02 under the old keys no longer count. A
deploy after 09:15 IST on 2026-11-09 moves it to 2026-11-11 (11-10 is a holiday). The kill rules keep 2026-11-02.

**Clarifications added 2026-10-30, before the registration date, for the three forward rules below.**
- A signal's symbol is followed through NSE renames (`symbol_renames`).
- A symbol with no daily bar on 5 consecutive regular sessions on which NIFTY 50 has one is finalised on the
  5th. A filled row closes at its last available close, and every unreached horizon takes that return, net of
  the fixed cost. An unfilled row is unscorable.
- A session on which NIFTY 50 has a bar and the symbol has none, with later bars for the symbol, takes the
  symbol's last earlier close once it is 5 sessions old (a halt is a zero-return session). Until then the row
  waits.
- While a `daily_bars` day is failed and not yet repaired, no missing bar on or after it is judged.
- A brk20 row scored as `rule` is a first publication, whatever its resting row's symbol.
- `prompt_version` in the registered key covers the system prompt, the context template, the response schema and
  the effort.
- Rolling back the regime filter after the registration date (`brk20.regime_filter: false`) ends the brk20 kill
  population: the rollback commit sets `forward_verdicts.KILL_POPULATION_UNTIL["brk20"]` to the last session the
  filter ran. Resuming it needs an owner-approved re-registration with a new registration date.

**Analyst-value verdict (registered 2026-10-29, re-registered 2026-11-09; population from its registration date).**
- **Population:** `prescreen_day_slots` rows of brk20, hi52 and ins with `d` on or after the registration date and a
  recorded first `analyst_decision`, whose call carries the strategy's registered key `(prompt_version, model)`. The
  registered key is the one on that strategy's first decision on or after the registration date. Rows with another key
  are excluded and counted. A change to the analyst prompt, a population contract or the model after the registration
  date therefore stops the population from growing. Accrual resumes only through an owner-approved re-registration
  with a new registration date.
- **Outcome:** `signal_outcomes` `next_open`:
  - entry at the open of the first regular session after d;
  - exit at the close of the 20th regular session, counting the entry session;
  - net of one CNC round trip at the per-trade risk-sizing notional at the floor-rung equity, fixed at the row's
    first scoring.
- **Cells:** a (d, strategy) cell exists once at least one enter and at least one no_action were decided. Cells are
  ordered by (d, strategy_id). A cell is final when all its decided rows are final. Its value is the mean net of
  its closed enters minus the mean net of its closed no_actions. A cell with an arm and no closed outcome is
  reported and not averaged.
- **Looks:** look 1 is the first 30 cells, formed when all 30 are final. Look 2 is the first 60 and supersedes it.
- **Decision:** a formed look's mean ≤ 0 ⇒ **ADVISORY**; > 0 ⇒ **KEEP**. Below that, INSUFFICIENT.
- **Tool:** `scripts/forward_verdicts.py --rule analyst`. Interim readings decide nothing.
- **Diagnostics:** T+5/T+10, per-strategy cells, and confidence buckets.
- **Duration:** at least three months, since brk20 originates only in regime-ON sessions.

**brk20 kill rule (registered 2026-10-29, population from the registration date).**
- **Population:** the brk20 crossings admitted on or after the registration date (`brk20_resting_levels` rows), under
  the regime filter.
- **Construct** (`signal_outcomes` `rule`):
  - entry: a limit resting at the broken level on `signal_d` (y+1, the session after the crossing session y)
    through `signal_d` + 4 sessions (the shipped 5-session resting limit);
  - fill: on the first regular daily bar whose low touches the level, at min(open, level). Touch fills are
    optimistic, as in the regime-filter study;
  - exit: at the close of the 20th regular session, counting the fill session (the regime-filter study's time-only
    arm; the 5-session-limit backtest itself measured close(fill+20)). No stop, no target;
  - voids: a structural ex-date between `signal_d` and the exit voids the crossing;
  - cost: one CNC round trip at the risk-sizing notional implied by the signal's own stop at the floor-rung equity,
    fixed at the row's first scoring.
- **Looks:** look 1 is the first 20 filled crossings by (fill date, symbol), formed when all 20 are final. Look 2
  is the first 40 and supersedes it.
- **Verdict:** DEMOTE iff the look's median net over its closed members ≤ 0.
- **Scope:** this rule judges the signal's drift. The card's stop and target are the paper no-stop test's question.
- **Demotion:** add brk20 to `ops.main.NO_EDGE_SHADOW_STRATEGIES` in an owner-visible commit.
- **Diagnostic only:** the regime filter's blocked arm (`rule_blocked`) against the admitted arm.
- **Tool:** `scripts/forward_verdicts.py --rule brk20`.

**ins kill rule (registered 2026-10-29, population from the registration date).**
- **Looks and verdict:** as brk20.
- **Construct:** entry at the open of the publication session d, which is the validated next-session open after
  the disclosure. Exit at the close of the 20th regular session. No stop. Cost at the risk-sizing notional for the
  5% stop, fixed at first scoring.
- **Demotion:** add ins to `NO_EDGE_SHADOW_STRATEGIES`, which wins over its registered edge in the risk gate.
- **Tool:** `scripts/forward_verdicts.py --rule ins`.

**Conditional specs (build only on their trigger).**
- **ADVISORY build** (trigger: the analyst verdict reads ADVISORY).
  - For hi52 and ins, an analyst no_action or a failed call no longer ends the candidate.
  - The pipeline builds the proposal deterministically and the gate judges it unchanged. The proposal:
    - enters at MARKET;
    - takes the rule's stop re-anchored at the live mark (`_reanchored_stop`);
    - has no target;
    - sizes at `max_qty_by_risk`;
    - carries `agent_id = PLATFORM_AGENT_ID`, with the analyst's reason as advice in the thesis.
  - brk20 is unaffected (no registered edge). The build gets its own plan.
- **Measured-edge pricing** (trigger: the no-stop verdict's look 1, a POOLED label over hi52, ins and brk20). Look 1
  read STOP NOT THE PROBLEM, which gives no basis for a time-only change; this waits for look 2.
  - A brk20-specific change needs its own pre-registered rule, entered in the trial ledger as a new trial. The
    research registration bar applies; brk20 is not grandfathered.
  - If that rule supports time-only brk20:
    - brk20 ships time-only T+20;
    - its registered edge is re-measured on the shipped construct (20 sessions counting the fill, not the
      backtest's close(fill+20)) minus the floor-rung cost floor;
    - the stop becomes a disaster stop used for sizing only.
  - rsi2 and trend have no measured edge for their shipped constructs. They are parked or shadowed, never priced
    from an analyst target.
- **No-action reason study** (trigger: the analyst verdict reads KEEP).
  1. Classify the no_action reasons via `analyst_call_id`.
  2. Pre-register deterministic screens for the mechanical classes ("last price below the broken level", "a buy
     limit there would be marketable").
  3. Ship a screen only if its cohort did worse than same-day approved neighbours.

## Known defects and test gaps

- A running ticker never receives a new Kite token (moot while the engine restarts daily).
- The 08:40 `token_check` sends a "token invalid, re-login" page when a login lands while its probe is in flight;
  the mid-day breaker ignores such a superseded token, this job does not.
- A billing-error DG4 latch lives in memory only: an engine restart clears it without a DG4 → DG0 alert, and the
  dashboard's DG4 hint says it lasts until the weekly reset.
- `cat` scanner: a story first seen on d before d's digest originates at age 0 on d and again at age 1 on d+1
  (prescreen dedupes per day only), which `cat.py` itself calls a double count. The verdict script drops the repeat;
  fixing the scanner changes the shadow population mid-window, so it is an owner decision.
- Left open by the 2026-10-30 review build: the pre-open planner and `/why` preview brk20 levels without saying the
  regime filter blocks a crossing in a regime-down session; the deterministic regime line reads the store on the
  event loop when a position event renders it; a brk20 re-offer row of a symbol that crossed twice is costed from the
  latest resting row covering its day, which need not be the crossing it re-offers.
- Per-cluster news-score drops are counted in the scoring log but leave no audit row.
- The headline clusterer can merge different companies' PRs that share a template into one cluster (the
  boilerplate list covers earnings templates only).
- Rejection storm, real path: paper latches its own daily `rejection_storm` halt at ≥3 broker rejects in 60 s, but
  `CAUSE_REJECTION_STORM` on the real latch (owner `/resume_entries`) and the ≥3-storms-per-session kill land with the
  live AUTO OMS. Clock skew is checked at boot only, not mid-session.
- Dashboard capital change: a held symbol that stopped storing bars blocks every capital change with a 409 (its 10-day
  bar lookback); no unit test pins the warm-before-copy rule on the twin rows (only NULL-P&L rows care).
- `tests/offline/test_research_studies.py`'s brk20 reference arm needs the untracked research snapshot and report, so
  it skips in a clean checkout. A committed fixture closes it: a trimmed snapshot with a regenerated reference cell, or
  synthetic bars with the cell computed from them (the registered cell is a full-universe number, so a trimmed
  snapshot cannot reproduce it).

## Paper-to-live gate (paper → live AUTO)

All must hold, evidenced from persisted data:

1. **Duration and volume:** ≥30 consecutive trading sessions of paper AUTO; ≥25 completed round trips across ≥2 styles.
2. **Safety invariants:** zero gate bypasses (every order row chains to a verdict row); no open position without
   broker-resident protection for >2 s p95 / >10 s max; zero unhandled process errors.
3. **Risk conformance:** every `limits.yaml` limit enforced when tested (chaos suite green; the circuit, outage and
   auction cases 7–10 are CI/replay only, never live-fired); max paper drawdown ≤6%; daily-loss halts fire correctly.
4. **Process quality:** reconciliation clean on ≥98% of passes; on every MIS day with the engine up at window end, the
   position is flat by trade-window end via `run_window_squareoff()`, with the 15:05–15:10 and broker 15:25 sweeps
   firing only as no-op backstops (an overdue square-off caught at startup, or a broker square-off across a recorded
   offline interval, is expected and reconciled); startup recovery and catch-up clean after one intentional stop/start
   and one simulated multi-day gap (chaos cases 15–19); the budget governor never hits DG3 unexpectedly.
5. **Economics honesty:** cost-adjusted paper expectancy reported to the owner. Profitability is not a gate condition,
   but the owner acknowledges the number before going live.
6. **Ops readiness:** the owner has run the dead-platform drill (see "Live AUTO") once; a static IP is whitelisted; a
   legal review is obtained or waived in writing by the owner.

The soak's drill schedule and evidence log are in `runbooks/CHAOS_DRILLS.md`.

## Live AUTO, small

**Before the first live order:**
- **Static IP** whitelisted on developers.kite.trade (default: the ISP's static-IP add-on, ~₹1,500/yr; else a small
  VPS order proxy). Kite allows one secondary IP.
- **GTT IP check:** whether GTT endpoints are IP-checked is unknown. Probing would place a real GTT, so the paper stage
  skipped it; answer it before any reliance on GTT.
- **Dead-platform fallback:** pre-stage RUNBOOK "Dead-platform manual fallback" (the printed card with the Call & Trade
  number filled in, plus a pinned Telegram message). Drill once before the paper-to-live gate, then quarterly.
- **Legal/compliance review** before AUTO (cheap insurance; consequences at this scale are mostly access-level).

**Scope:** live routing behind the tightest limits — 1.0× leverage, MIS longs and CNC swing only, at most 2 open
positions and 3 new trades a day — as `config/limits_overlay.live.yaml` in the same protected store, loaded
most-restrictive-wins over `limits.yaml` and lifted only through the owner-confirmed flow at each ramp gate. Also the
live Reconciler and the live broker-resident-protection managers (SquareOffScheduler, MIS SL-M, the shutdown guard), a
live-vs-paper fill-deviation tracker feeding the fill model, and a daily owner summary with the deviation report.

**`cat` live origination** (separate from the live-ramp gate): `cat` stays paper/RECOMMEND-only in live AUTO until ≥60
recorded news sessions, the shadow verdict clears (see "Pending verdicts"), zero `catalyst_guard` violations in the
soak, and explicit owner approval as new strategy logic. Until then the live overlay pins `cat` disabled for live
routing.

**Live-ramp gate (→ online learning):** 4 weeks live; zero broker-resident-protection violations in production;
fill-model deviation median <0.05% of notional with the model recalibrated; every halt and downgrade that fired behaved
per spec; the owner signs ramp step 1 (leverage 1× → 2×).

## Online learning and ramp completion

**Scope:** champion/challenger promotions may affect live AUTO within the envelope (owner-approved); the River filter
active on live entries; the weekly researcher in full agentic mode; leverage ramp 2× → 3× (platform cap) and the
MIS-short decision (open question below), each gated on 4 more clean weeks, drawdown within limits and no rollback
events.

**Steady-state gate:** 4 consecutive weeks with no owner intervention beyond daily login and approvals; all promotions
and rollbacks autonomous within the envelope and logged; a quarterly cadence of restore drill, dead-platform re-drill and
chaos re-run.

## Out of scope and deferred

**Will not be built:** F&O trading; anything multi-user or signal distribution; a mobile app; reliance on T+0
settlement; BSE trading; social-media sentiment; full-article news scraping (headline-level only: cost and
prompt-injection surface); paid news APIs; sub-minute news-reaction trading.

**Deferred:** VPS/cloud migration (after the steady-state gate); the pay-as-you-go API-key swap for the LLM (the
`agents.yaml` seam exists); MIS shorts (an owner decision in the online-learning stage); leverage above 3×; BSE data
cross-checks; an MIS same-day variant of `cat` (a challenger only after the swing variant's live conditions pass);
`GET /why`; the late-session study; the ops digest and the Telegram channel split; a known-at column on
`earnings_calendar` (two finished studies read it without one: the event-study PEAD legs and the hvol earnings split,
which stand with that caveat; a column only covers rows ingested after it ships). Trigger: the next study whose result
depends on when an earnings date became known.

**VPS migration path:**
1. Copy as is: repo, `config/`, DuckDB/Parquet, SQLite state, dashboard build.
2. Re-provision: secrets into the new host's credential store (DPAPI is machine-bound); `auth login --claudeai` as the
   service user (the engine uses the CLI's stored login); the VPS IP staged as Kite's secondary IP before cutover;
   service install (NSSM on Windows, systemd on Linux — the Windows-only seams are `engine.core.secrets` and the
   `engine.ops` power/service handling).
3. Known regression: NSE scrapes are blocked from datacenter IPs — route them through the home connection, or accept
   Kite-only enrichment.
4. Login: the dashboard and `/kite/callback` move behind Tailscale/VPN for the phone redirect, and the Kite app's
   redirect URL is updated.
5. Cutover: run AUTO+paper on the VPS ≥5 sessions, then move live routing in a maintenance window with the kill switch
   armed and the home machine kept warm for rollback.

## Open questions for the owner

Each has a provisional default that the build assumes.

| Question | Provisional default |
|---|---|
| Can the home ISP provide a static IP? | Yes, the ISP add-on; else a small static-IP VPS order proxy |
| Enable scripted TOTP auto-login (against Zerodha policy)? | Off: manual phone-tap login |
| When, if ever, to enable MIS shorts? | Not before the online-learning stage; F&O-only, 14:00 cutoff, 14:45 square-off, half size |
| Buy a UPS for the PC? | Recommended, not assumed; broker-resident protection keeps positions protected through power loss |
| Mirror nightly backups off-machine? | Local only today; the decision and ledger history is irreplaceable, so a mirror is recommended |
| Are GTT endpoints IP-checked? | Assume yes; answer before live AUTO relies on GTT |
| Dashboard exposure? | LAN bind + bearer token; only `/kite/callback` unauthenticated |
| Telegram as the only push channel? | Yes, plus the dashboard; the broker's own alerts stay independent |
| Rank the 300-name tick cap over the NIFTY 500 core instead of all of NIFTY Total Market? | Total Market, as built (high-turnover microcaps can displace NIFTY 500 names; switching is a one-line change) |
| How does the engine start for active periods? | Manual owner start. The watchdog scheduled task (`scripts/schedule_tasks.ps1`) is not registered today, so a missed start or a dead engine raises no alert. Running the service from a pinned checkout, or with delayed auto-start so a host reboot doesn't wait for a human, is the owner's call |
