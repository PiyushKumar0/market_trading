# IMPLEMENTATION_PLAN.md — what is left to build

This file holds only what is not built yet: the gates ahead, the verdicts and owner decisions still pending, and the
open questions. Built work is described by the code. When an item here ships, delete its text; do not mark it done.

The full original plan of record (all phases, rationale, the A–E fact sheet, the O owner decisions and R requirements)
and the other removed design and research documents are in git at commit `417da77`, for example
`git show 417da77:IMPLEMENTATION_PLAN.md`. Code comments cite that plan's section numbers (`§2.6`) and plan v5's IDs
(`Q4.7`, `D8`: `git show 417da77:runbooks/plan-2026-10-06-measure-card-paper-research.md`).

## 1. Where things stand (2026-10-19)

- Phase 2 (RECOMMEND) passed gate G2 on 2026-10-05. The engine recommends; the owner trades by hand.
- The paper autopilot has been live since 2026-10-08 (owner D8: "Implement the auto paper trade, though it should not
  generate actual trade execution."). Paper trades CNC only. A real order is structurally impossible:
  `feature_flags.paper_only` refuses AUTO and live routing, and `KiteClient` refuses every order and GTT call unless
  built with `orders_enabled=True`, which production never does.
- The G3 clock has not started. G3 needs MIS and live-order machinery that the CNC-only paper path does not have:
  SquareOffScheduler, MIS SL-M protection, the shutdown guard and its CNC-GTT verification.

## 2. Invariants the remaining work must keep

- **R1, three tiers:** Claude proposes, the deterministic RiskGate disposes, the deterministic OMS executes. The
  platform stays safe with the LLM tier dead.
- **R3, broker-resident protection:** every live position has a protective order resting at the broker (SL-M for MIS,
  GTT OCO for CNC), so protection survives process, PC and token death.
- **R10, kill stickiness:** a kill is persisted before any order action and checked on every startup; only the owner's
  two-step flow resets it.
- **B5, single user:** recommendations and execution serve the owner's own account only. Distributing them would make
  the owner an unregistered Research Analyst or algo provider, a SEBI violation.
- **Order throttle:** at most 1 order call per second sustained (burst 2) through the single limiter in
  `engine.broker`, so the platform stays an unregistered retail algo (B2/B3).
- **C9, success is not profit before Phase 4:** process quality, capital preservation and a measured record of what
  works. Profit is tracked in Phase 4 and targeted only in Phase 5.
- **Research registration bar (review §9 #8):** a new `expected_edge_pct`, or a promotion out of shadow, must cite an
  entry in `config/research_trials.yaml`, pass CPCV at `fold_pass_min(N_family)`, and have a permutation
  p < `registration_alpha(N_family)` (`engine.learning.multiplicity`) against same-size random subsamples of its
  matched population.

## 3. Pending verdicts and owner decisions

- **hi52 forward test.** Mechanical verdict: `scripts/hi52_forward_verdict.py` (kill rule and pins in the script and
  `settings.yaml` `hi52`).
- **D14 paper no-stop test** (hi52, ins, brk20 paper positions carry no stop and exit at session 20). Verdict at the
  pinned looks 20 and 40: `scripts/no_stop_verdict.py`; operation and rollback in RUNBOOK "No-stop exit test". From R,
  brk20 recs are regime-ON only and use the 5-session retest window; from the owner's patch date
  (`runbooks/patches/limits-drop-analyst-confidence-min.patch`) there is no confidence floor. The paired D stays valid;
  read the slices split at R and at the patch date by hand. `scripts/no_stop_verdict.py` is untouched.
- **`cat` v2 shadow verdict — not yet run.** Mechanical verdict: `scripts/cat_shadow_verdict.py` (conventions in the
  script; COMMANDS "cat v2 shadow verdict"). Pre-registered: at ≥30 sessions and ≥20 admitted signals, measure T+10 and
  T+20 net drift at spread-inclusive CNC costs with the WO-3 margin floor. Net ≤ the cost floor at both horizons ⇒
  `cat` origination is retired and the watchlist stays context-only; clears ⇒ owner review for live (Phase 4 `cat`
  gate). Thresholds (`cat.materiality_min` and the rest) stay frozen for the whole window; moving one restarts the
  clock. The clock restarted on 2026-08-28 (signals 2026-08-18..28 excluded). Split the sample at the first NIFTY 500
  universe build (O15) and at the news scorer's switch to `sonnet-5.5` (`news_clusters.scorer_model`).
- **`cat_reversal` shadow verdict.** Pre-registered: at ≥60 sessions and ≥15 admitted signals, T+5 and T+10 net at
  next-open fills, spread-inclusive CNC costs and the WO-3 margin floor; net ≤ the cost floor at both ⇒ retired;
  clears ⇒ owner review. Fewer than 1 admitted signal per 20 sessions across the first 60 triggers an early owner
  check-in.
- **Waiting on the `cat` v2 verdict (owner, 2026-09-24):** wiring the results line items (`revenue`, `pat`) into the
  News Analyst and the three resolver alias fixes (`engineers`, `dalmia bharat`, `pfizer`) land together right after
  it. Switching the clusterer's `cat.max_event_age_days` from calendar days to sessions is also decided after it.
- **C1 exit geometry (run 2026-10-07):** every shipped stop turned its strategy's measured edge negative. Owner work:
  hi52's switch to a 2.5×ATR14 catastrophe stop (its +0.22% median net would not clear the cost check once the edge is
  re-derived). brk20 and ins came out `neither`: no change.
- **C2 regime filter (run 2026-10-07):** brk20 `supported`; the 2026 held-out slice contradicts it. From R it applies
  at brk20 origination in the batch sweep (`settings.yaml` `brk20.regime_filter`; `false` plus a restart rolls it back),
  and resting levels are not re-judged. Blocked crossings are journalled in `brk20_regime_blocked` and scored as the
  `rule_blocked` arm, a diagnostic only. hi52 not supported; ins insufficient.
- **C3 selection filters (run 2026-10-07):** not evidence. A filter gate needs a new pre-registration with a
  permutation test against same-size random subsamples.
- **Spread re-measure:** `costs.yaml spread_pct: 0.02` was measured on 2026-08-13 on the NIFTY 200 tick corpus.
  Provisional rule: re-measure (never tune) when the universe changes materially or quarterly, whichever is sooner. The
  universe moved to NIFTY Total Market on 2026-10-08 (O18).
- **Fee schedule:** `costs.yaml` says `verified_on: 2026-06-11`; the rule is to re-scrape before each release
  (`scripts/rescrape_costs.py`).

**Pre-registered 2026-10-29 (review actions).** R = `<R>`, the first trading day after the deploy
(`engine.learning.forward_verdicts.REGISTERED_FROM`). Each rule has a `registered` row in `config/research_trials.yaml`.

**Amendment 2026-10-30, before R, for the three forward rules below.**
- A signal's symbol is followed through NSE renames (`symbol_renames`).
- A symbol with no daily bar on 5 consecutive regular sessions on which NIFTY 50 has one is finalised on the
  5th. A filled row closes at its last available close, and every unreached horizon takes that return, net of
  the fixed cost. An unfilled row is unscorable.
- `prompt_version` in the registered key covers the system prompt, the context template, the response schema and
  the effort.
- Rolling back the C2 filter after R (`brk20.regime_filter: false`) ends the brk20 kill population. Resuming it
  needs an owner-approved re-registration with a new R.

**Analyst-value verdict (registered 2026-10-29, population from R).**
- **Population:** `prescreen_day_slots` rows of brk20, hi52 and ins with `d >= R` and a recorded first
  `analyst_decision`, whose call carries the strategy's registered key `(prompt_version, model)`. The registered
  key is the one on that strategy's first decision on or after R. Rows with another key are excluded and counted.
  A change to the analyst prompt, a population contract or the model after R therefore stops the population from
  growing. Accrual resumes only through an owner-approved re-registration with a new R.
- **Outcome:** `signal_outcomes` `next_open`:
  - entry at the open of the first regular session after d;
  - exit at the close of the 20th regular session, counting the entry session;
  - net of one CNC round trip at the §7.1 notional at the floor-rung equity, fixed at the row's first scoring.
- **Cells:** a (d, strategy) cell exists once at least one enter and at least one no_action were decided. Cells are
  ordered by (d, strategy_id). A cell is final when all its decided rows are final. Its value is the mean net of
  its closed enters minus the mean net of its closed no_actions. A cell with an arm and no closed outcome is
  reported and not averaged.
- **Looks:** look 1 is the first 30 cells, formed when all 30 are final. Look 2 is the first 60 and supersedes it.
- **Decision:** a formed look's mean ≤ 0 ⇒ **ADVISORY**; > 0 ⇒ **KEEP**. Below that, INSUFFICIENT.
- **Tool:** `scripts/forward_verdicts.py --rule analyst`. Interim readings decide nothing.
- **Diagnostics:** T+5/T+10, per-strategy cells, and confidence buckets.
- **Duration:** at least three months, since brk20 originates only in regime-ON sessions.

**brk20 kill rule (registered 2026-10-29, population from R).**
- **Population:** the brk20 crossings admitted on or after R (`brk20_resting_levels` rows), under C2.
- **Construct** (`signal_outcomes` `rule`):
  - entry: a limit resting at the broken level on `signal_d` (y+1, the session after the crossing session y)
    through `signal_d` + 4 sessions (V2-5);
  - fill: on the first regular daily bar whose low touches the level, at min(open, level). Touch fills are
    optimistic, as in C2;
  - exit: at the close of the 20th regular session, counting the fill session (C2's arm (ii); V2-5 itself
    measured close(fill+20)). No stop, no target;
  - voids: a structural ex-date between `signal_d` and the exit voids the crossing;
  - cost: one CNC round trip at the §7.1 notional implied by the signal's own stop at the floor-rung equity,
    fixed at the row's first scoring.
- **Looks:** look 1 is the first 20 filled crossings by (fill date, symbol), formed when all 20 are final. Look 2
  is the first 40 and supersedes it.
- **Verdict:** DEMOTE iff the look's median net over its closed members ≤ 0.
- **Scope:** this rule judges the signal's drift. The card's stop and target are D14/C5's question.
- **Demotion:** add brk20 to `ops.main.NO_EDGE_SHADOW_STRATEGIES` in an owner-visible commit.
- **Diagnostic only:** the C2 blocked arm (`rule_blocked`) against the admitted arm.
- **Tool:** `scripts/forward_verdicts.py --rule brk20`.

**ins kill rule (registered 2026-10-29, population from R).**
- **Looks and verdict:** as brk20.
- **Construct:** entry at the open of the publication session d, which is the validated next-session open after
  the disclosure. Exit at the close of the 20th regular session. No stop. Cost at the §7.1 notional for the 5%
  stop, fixed at first scoring.
- **Demotion:** add ins to `NO_EDGE_SHADOW_STRATEGIES`, which wins over its registered edge (`gate.py:1046-1049`).
- **Tool:** `scripts/forward_verdicts.py --rule ins`.

**Market-wide pause study (registered 2026-10-29, run before any wiring).**
- **Strategies:** hi52 and ins.
- **Rule:** at signal session y, PAUSE if the core equal-weight return compounded over the 20 sessions ending at y
  is < 0; otherwise TRADE.
- **Population and construct:** C2's populations, construct and cost (`scripts/backtest_regime_filter.py`), on a
  research snapshot.
- **EW series:** the daily mean of close/prev_close − 1 over the current core-eligible list (a
  survivorship-tainted proxy). |r| > 25% and structural corporate-action days are excluded.
- **Family:** per strategy, the regime-filter family is C2 plus this rule, so N = 2.
- **Decision:** SUPPORTED iff all hold:
  - TRADE and PAUSE each have n ≥ 30;
  - TRADE is CPCV-promotable at `fold_pass_min(2)`;
  - TRADE median net > max(0, FULL median);
  - PAUSE median net ≤ 0;
  - the permutation p < 0.025, where p is the share of 10,000 same-size random subsamples of FULL (seed
    20261029) whose median ≥ TRADE's median.

  INSUFFICIENT if a cohort has n < 30; NOT SUPPORTED otherwise. SUPPORTED goes to the owner.
- **Tool:** `scripts/backtest_market_pause.py` on a fresh research snapshot (its `REGISTERED` holds this date).

**Conditional specs (build only on their trigger).**
- **ADVISORY build** (trigger: the analyst verdict reads ADVISORY).
  - For hi52 and ins, an analyst no_action or a failed call no longer ends the candidate.
  - The pipeline builds the proposal deterministically and the gate judges it unchanged. The proposal:
    - enters at MARKET;
    - takes the rule's stop re-anchored at the live mark (`_reanchored_stop`, D3);
    - has no target;
    - sizes at `max_qty_by_risk`;
    - carries `agent_id = PLATFORM_AGENT_ID`, with the analyst's reason as advice in the thesis.
  - brk20 is unaffected (no registered edge). The build gets its own plan.
- **#4 measured-edge pricing** (trigger: C5 look 1, a POOLED label over hi52, ins and brk20,
  `no_stop_verdict.py:8-19`).
  - A brk20-specific change needs its own pre-registered rule, entered in the trial ledger as a new trial. The §2
    registration bar applies; brk20 is not grandfathered.
  - If that rule supports time-only brk20:
    - brk20 ships time-only T+20;
    - its registered edge is re-measured on the shipped construct (20 sessions counting the fill, not V2-5's
      close(fill+20)) minus the floor-rung cost floor;
    - the stop becomes a disaster stop used for sizing only.
  - rsi2 and trend have no measured edge for their shipped constructs. They are parked or shadowed, never priced
    from an analyst target.
- **B1 study** (trigger: the analyst verdict reads KEEP).
  1. Classify the no_action reasons via `analyst_call_id`.
  2. Pre-register deterministic screens for the mechanical classes ("last price below the broken level", "a buy
     limit there would be marketable").
  3. Ship a screen only if its cohort did worse than same-day approved neighbours.

## 4. Known defects and test gaps

- A running ticker never receives a new Kite token (moot while the engine restarts daily).
- The 08:40 `token_check` sends a "token invalid, re-login" page when a login lands while its probe is in flight;
  the mid-day breaker ignores such a superseded token, this job does not.
- A billing-error DG4 latch lives in memory only: an engine restart clears it without a DG4 → DG0 alert (R8), and the
  dashboard's DG4 hint says it lasts until the weekly reset.
- `cat` scanner: a story first seen on d before d's digest originates at age 0 on d and again at age 1 on d+1
  (prescreen dedupes per day only), which `cat.py` itself calls a double count. The verdict script drops the repeat;
  fixing the scanner changes the shadow population mid-window, so it is an owner decision.
- Per-cluster news-score drops are counted in the scoring log but leave no audit row.
- The headline clusterer can merge different companies' PRs that share a template into one cluster (the
  boilerplate list covers earnings templates only).
- Rejection storm, real path: paper latches its own daily `rejection_storm` halt at ≥3 broker rejects in 60 s, but
  `CAUSE_REJECTION_STORM` on the real latch (owner `/resume_entries`) and the ≥3-storms-per-session kill land with the
  Phase-4 live OMS. Clock skew is checked at boot only, not mid-session.
- O19 capital change: a held symbol that stopped storing bars blocks every capital change with a 409 (the 10-day
  lookback of ruling R16); no unit test pins the warm-before-copy rule on the twin rows (only NULL-P&L rows care).
- `tests/offline/test_research_studies.py`'s brk20 reference arm needs the untracked research snapshot and report, so
  it skips in a clean checkout. A committed fixture closes it: a trimmed snapshot with a regenerated reference cell, or
  synthetic bars with the cell computed from them (the registered cell is a full-universe number, so a trimmed
  snapshot cannot reproduce it).

## 5. Gate G3 — paper → live AUTO

All must hold, evidenced from persisted data:

1. **Duration and volume:** ≥30 consecutive trading sessions of paper AUTO; ≥25 completed round trips across ≥2 styles.
2. **Safety invariants:** zero gate bypasses (every order row chains to a verdict row); no open position without
   broker-resident protection for >2 s p95 / >10 s max (R3); zero unhandled process errors.
3. **Risk conformance:** every `limits.yaml` limit enforced when tested (chaos suite green; the circuit, outage and
   auction cases 7–10 are CI/replay only, never live-fired); max paper drawdown ≤6%; daily-loss halts fire correctly.
4. **Process quality:** reconciliation clean on ≥98% of passes; on every MIS day with the engine up at window end, the
   position is flat by trade-window end via `run_window_squareoff()`, with the 15:05–15:10 and broker 15:25 sweeps
   firing only as no-op backstops (an overdue square-off caught at startup, or a broker square-off across a recorded
   offline interval, is expected and reconciled); startup recovery and catch-up clean after one intentional stop/start
   and one simulated multi-day gap (chaos cases 15–19); the budget governor never hits DG3 unexpectedly.
5. **Economics honesty (C9):** cost-adjusted paper expectancy reported to the owner. Profitability is not a gate
   condition, but the owner acknowledges the number before going live.
6. **Ops readiness:** the owner has run the dead-platform drill (section 6) once; a static IP is whitelisted (Q2);
   a legal review is obtained or waived in writing by the owner.

The soak's drill schedule and evidence log are in `runbooks/CHAOS_DRILLS.md`.

## 6. Phase 4 — AUTO live, small

**Before the first live order:**
- **Static IP** whitelisted on developers.kite.trade (default: the ISP's static-IP add-on, ~₹1,500/yr; else a small
  VPS order proxy). Kite allows one secondary IP.
- **GTT IP check (B7):** whether GTT endpoints are IP-checked is unknown. Plan v5 dropped the Phase-3 probe because it
  would place a real GTT; answer it before any reliance on GTT.
- **Dead-platform fallback:** pre-stage RUNBOOK "Dead-platform manual fallback" (the printed card with the Call & Trade
  number filled in, plus a pinned Telegram message). Drill once before G3, then quarterly.
- **Legal/compliance review** before AUTO (cheap insurance; consequences at this scale are mostly access-level).

**Scope:** live routing behind the tightest limits — 1.0× leverage, MIS longs and CNC swing only, at most 2 open
positions and 3 new trades a day — as `config/limits_overlay.phase4.yaml` in the same protected store, loaded
most-restrictive-wins over `limits.yaml` and lifted only through the owner-confirmed flow at each ramp gate. Also the
live Reconciler and the live R3 managers (SquareOffScheduler, MIS SL-M, the shutdown guard), a live-vs-paper
fill-deviation tracker feeding the fill model, and a daily owner summary with the deviation report.

**`cat` live origination** (separate from G4): `cat` stays paper/RECOMMEND-only in live AUTO until ≥60 recorded news
sessions, the shadow verdict clears (section 3), zero `catalyst_guard` violations in the soak, and explicit owner
approval as new strategy logic (R4). Until then the Phase-4 overlay pins `cat` disabled for live routing.

**Gate G4 (→ Phase 5):** 4 weeks live; zero R3 violations in production; fill-model deviation median <0.05% of
notional with the model recalibrated; every halt and downgrade that fired behaved per spec; the owner signs ramp
step 1 (leverage 1× → 2×).

## 7. Phase 5 — learning online and ramp completion

**Scope:** champion/challenger promotions may affect live AUTO within the envelope (O4/R4); the River filter active on
live entries; the weekly researcher in full agentic mode; leverage ramp 2× → 3× (platform cap) and the MIS-short
decision (Q6), each gated on 4 more clean weeks, drawdown within limits and no rollback events.

**Gate G5 (steady state):** 4 consecutive weeks with no owner intervention beyond daily login and approvals; all
promotions and rollbacks autonomous within the envelope and logged; a quarterly cadence of restore drill,
dead-platform re-drill and chaos re-run.

## 8. Out of scope and deferred

**Will not be built:** F&O trading; anything multi-user or signal distribution (B5); a mobile app; reliance on T+0
settlement; BSE trading; social-media sentiment; full-article news scraping (headline-level only: cost and
prompt-injection surface); paid news APIs; sub-minute news-reaction trading (O12).

**Deferred:** VPS/cloud migration (O7, after G5); the pay-as-you-go API-key swap for the LLM (the `agents.yaml` seam
exists); MIS shorts (Phase-5 owner gate); leverage above 3×; BSE data cross-checks; an MIS same-day variant of `cat`
(a challenger only after the swing variant's live gate passes); `GET /why`; the C4 late-session study; the B2 ops
digest and the Telegram channel split; A4, a known-at column on `earnings_calendar` (two finished studies read it
without one: the event-study PEAD legs and the hvol earnings split, which stand with that caveat; a column only covers
rows ingested after it ships). Trigger: the next study whose result depends on when an earnings date became known.

**VPS migration path (O7):**
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

## 9. Open questions for the owner

Each has a provisional default that the build assumes.

| # | Question | Provisional default |
|---|---|---|
| Q2 | Can the home ISP provide a static IP? | Yes, the ISP add-on; else a small static-IP VPS order proxy |
| Q3 | Enable scripted TOTP auto-login (against Zerodha policy)? | Off: manual phone-tap login |
| Q6 | When, if ever, to enable MIS shorts? | Not before the Phase-5 gate; F&O-only, 14:00 cutoff, 14:45 square-off, half size |
| Q7 | Buy a UPS for the PC? | Recommended, not assumed; R3 keeps positions protected through power loss |
| Q8 | Mirror nightly backups off-machine? | Local only today; the decision and ledger history is irreplaceable, so a mirror is recommended |
| Q9 | Are GTT endpoints IP-checked? | Assume yes; answer before Phase 4 relies on GTT |
| Q11 | Dashboard exposure? | LAN bind + bearer token; only `/kite/callback` unauthenticated |
| Q12 | Telegram as the only push channel? | Yes, plus the dashboard; the broker's own alerts stay independent |
| Q14 | How does the engine start for active periods? | Manual owner start. The watchdog scheduled task (`scripts/schedule_tasks.ps1`) is not registered today, so a missed start or a dead engine raises no alert |
