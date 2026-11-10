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

**Macro mood-signal study (proposed 2026-11-09; registered by the commit that adds this entry, after the owner signs
off the thresholds).** The owner's question: do global and domestic mood signals known before the 09:15 open predict
Indian returns over short swings, enough to profit after costs? Owner, 2026-11-09: "Go with the proposed test
pipeline." The motivating case is the 2026-10-29 → 11-06 turn (memory `macro-turn-2026-11-anatomy`,
`us-lead-priced-at-open`), and it is excluded from every sample. Evidence only; nothing is wired into the engine.

- **Family:** `edge/macro_mood`, ten hypotheses (six backtests, four forward), so N = 10:
  `registration_alpha(10)` = 0.005 and `fold_pass_min(10)` = 60%. Thresholds are fixed by this entry; changing one
  is a new registration. Owner decisions on 2026-11-10:
  - "Use LLM to map story to sectors and wide the matching as well", plus a test of whether article summaries and
    text add leverage over headlines;
  - add one combined score defined before any result exists ("Yes").
- **Hypotheses.** D is the trading session the trade enters on.
  - **US big-fall rebound:** the S&P 500 sessions dated u with D_prev ≤ u < D (D_prev = the previous NIFTY session)
    compound to ≤ −2.0% ⇒ buy the NIFTY at D's open, sell at D's close (MIS cost).
  - **US rally follow-through:** the same compounded S&P return is ≥ +1.0% ⇒ buy at D's open, exit at the close of
    the 5th session counting D (CNC cost). This is the owner's "India follows the US the next day" in tradeable form.
  - **Oil drop:** the latest Brent front-month settlement dated before D is ≤ −5.0% below the settlement three Brent
    sessions earlier ⇒ buy at D's open, hold 5 sessions (CNC).
  - **Foreign short-covering:** the FII long share of index futures, long / (long + short) contracts in NSE's
    participant-wise open interest, rose over the 3 sessions ending D−1 by more than the 90th percentile of its
    3-session changes over the 250 sessions before that ⇒ buy the NIFTY at D's open, hold 5 (CNC). A share, unlike a
    net contract count, does not jump when the index lot size is revised.
  - **Breadth thrust:** ≥ 75% of the liquid universe closed up on D−1 ⇒ buy the NIFTY at D's open, hold 5 (CNC).
    The liquid universe as of D−1 is every full-market `bars_1d` stock (delisted names included) whose median
    close × volume over the 20 sessions to D−1 is ≥ ₹10 cr. It excludes names with a corporate-action ex-date on D−1,
    and ETFs by the hvol study's `is_etf` test (NSE's ETF list plus the `(BEES|ETF|IETF)$` suffix).
  - **Combined mood score:** the owner's question of whether combining signals beats running them separately,
    answered with no fitted weights.
    - Inputs: three values known before D's open. They are the compounded S&P return of the US hypotheses, minus
      the Brent 3-session change of the oil-drop hypothesis, and the 3-session change in the FII long share.
    - Each is z-scored against its own previous 250 sessions (mean and SD), and the score is the mean of the three
      z-scores. All three are required.
    - Rule: a score in the top 10% of its previous 250 sessions' scores ⇒ buy the NIFTY at D's open, hold 5 (CNC).
    - Pre-declared comparison (decides no verdict): each input alone, under the same top-10%-of-own-z rule and the
      same sample, as diagnostic arms. **Combining helps** iff the score's mean net per trade exceeds every arm's.
  - **News tone, headlines (forward only):** tone(D) is the mean of sentiment × materiality over the `market`-scope
    scores that News Analyst calls returned in D's news window.
    - The window runs from 09:00 IST on the previous regular session to 09:00 IST on D, inclusive below and exclusive
      above, so weekend and holiday news counts for the next session.
    - The first draft said "the 24 hours before 09:00 on D", which would have dropped every weekend's news. It was
      amended on 2026-11-10, before the first forward session and before any forward data existed.
    - Source: `state.db` `agent_calls` (`at` and the parsed `output_json`), taking the first successful score per
      cluster. The table is append-only, so it keeps the state as it stood at 09:00. `news_clusters` would not: its
      `first_seen` and `sectors` are rewritten when headlines merge later.
    - Rule: tone(D) in the top third of its previous sessions (at most 60, and only once at least 20 exist) ⇒ buy the
      NIFTY at D's open, hold 5 (CNC).
    - The registered key is `(model, prompt_version)` on the first News Analyst call on or after the registration
      date. Calls with another key are excluded and counted, so a scorer change stops accrual until the owner
      re-registers, as with the analyst-value verdict.
  - **Policy push, headlines (forward only):** a candidate is a cluster the News Analyst scored in D's news window
    (from `agent_calls`) with `event_type` `govt_program` or `sector_policy`, sentiment ≥ +0.4 and materiality ≥ 0.6.
    - Mapping: its groups are the union of two maps, both computed from the headline text in that call's context.
      - Theme map: a whole-word match of a `config/themes.yaml` keyword (the resolver's rule) maps to that theme's
        frozen stock list.
      - LLM map: the pinned mapper model assigns groups from a closed vocabulary, the `sector_map` labels plus the
        theme names. It is told to map a named company to its own group.
    - Trade: for each group with at least one liquid member, buy its liquid members equal-weight at D's open and hold
      10 sessions. A sector group's members come from the `sector_map` snapshot in force on D−1; a theme's come from
      its frozen list. Each leg pays one CNC round trip at the research notional. One position per group at a time.
    - Judged on the basket's return minus the equal-weight liquid universe's over the same window.
  - **News tone, content (forward only):** the news-tone rule applied to the study's own scores of the captured
    articles, using variant C (below) in place of the News Analyst's headline scores. Same `market` scope, window
    and percentile rule.
  - **Policy push, content (forward only):** the policy-push rule applied to variant C. Candidates use the same
    `event_type`, sentiment and materiality bars. The mapper sees the headline, summary and body. The theme match
    runs on the headline and summary.
- **Theme stock lists:** frozen in `config/research/macro_mood/theme_lists.yaml`. Each theme holds its NSE thematic
  index's constituents on 2026-11-10, with each source URL and sha256:
  - defence (22);
  - ev_mobility (37, Nifty EV & New Age Automotive);
  - railways (17, Nifty India Railways PSU);
  - infra_capex (30);
  - psu_disinvestment (24, CPSE ∪ PSE);
  - data_ai (50, Nifty India Digital);
  - make_in_india (77, Nifty India Manufacturing).

  renewables has no NSE index, so it has no list; its stories reach ENERGY or UTILITIES through the LLM map. The
  engine's own theme lists in `themes.yaml` stay empty; filling them is a separate owner decision.
- **Mapper:**
  - The prompt is `config/research/macro_mood/mapper_prompt.md`, used verbatim; each call logs its sha256.
  - Its closed vocabulary is the 23 `sector_map` labels of the 2026-11-08 snapshot (UNCLASSIFIED excluded) plus the
    seven listed themes. Output is `{"groups": [...]}`.
  - The parser keeps at most the first three in-vocabulary names and drops the rest. The schema carries no length
    caps (memory `wire-schema-prose-caps-harmful`).
- **Content capture and scoring (study-side; the engine is untouched):**
  - **Capture:** `scripts/news_content_capture.py` runs every 15 minutes as an owner-visible Windows scheduled task
    (logged in WORKLOG). It polls the RSS feeds in `settings.yaml` `news.feeds.rss` as they stand at registration.
    It writes to `data/research/news_content.sqlite`:
    - url as the key, feed, title, summary (the RSS description with tags stripped), published_at and
      first_captured_at;
    - the article body, fetched once at first capture: JSON-LD `articleBody` when present, else the page's `<p>`
      text, cut to its first 400 words, plus fetched_at and the HTTP status.
  - Bodies are never re-fetched, because articles are edited after publication. Probed 2026-11-10:
    - every feed carries 20–60-word summaries (ET on 21 of 50 items);
    - article pages open for 5 of 6 outlets;
    - NDTV Profit pages answer 403, so its items keep the summary only.
  - At most one page request every 2 seconds.
  - **Scoring:** `scripts/news_content_score.py` runs nightly at 22:00 IST, outside market hours.
    - Items are clustered with the engine's `HeadlineClusterer` at its pinned threshold, and each cluster is scored
      in two blind variants, in separate calls: **H**, the representative headline only, and **C**, the headline plus
      its summary plus its body.
    - Both use the News Analyst's system prompt verbatim and its output schema, with batches of 30.
    - Every call is stored append-only with its time, model and prompt hash. Scoring after the fact is safe on two
      conditions: inputs are restricted to items captured before 09:00 IST on D, and the pinned models' training ends
      before the forward window. The second is checked against the published model cutoffs at registration.
  - **Models (pinned at registration):** `claude-haiku-5-5` for H and C scoring, `claude-sonnet-5-5` for the mapper.
    - Anthropic's models page (fetched 2026-11-10) gives both a training-data cutoff of June 2026, which is before the
      forward window.
    - Retirement is committed no sooner than 2027-09-28 (Sonnet 5.5) and 2027-10-07 (Haiku 5.5). A pinned model
      retired before a hypothesis's look forms makes it insufficient unless the owner re-registers.
    - Calls go through the Claude Agent SDK path the engine already uses (the shared subscription, never the paid
      API). They keep their own call log and never write to the engine's `agent_calls` or budget ledger.
    - Expected use is about 20 Haiku calls and 2 Sonnet calls a night.
    - The batch is capped at 40 calls a night and stops at the first rate-limit or session-limit error, with no
      retries, so it cannot starve the engine's analysts on the shared subscription.
    - A model change stops the content hypotheses' accrual until the owner re-registers.
- **Headlines versus content (the owner's leverage question; pre-declared, decides no trading verdict):**
  - The comparison uses the study's own H and C scores over the same clusters, so the model is held fixed.
  - Tone: the rank correlation between tone(D) and the NIFTY's open(D) → close(D+4) return, C minus H, with a
    moving-block bootstrap 95% CI (block = 5 sessions).
  - Policy: the mean 10-session excess per event, C-identified minus H-identified.
    - The 95% CI comes from a bootstrap that resamples whole entry sessions, because same-session events overlap.
    - Each side needs at least 10 events; with fewer, the answer is "not enough data". Both details were added on
      2026-11-10, before any data.
  - For each, the answer is **content adds leverage** if the CI lies above 0, **content hurts** if it lies below,
    and **no measurable difference** otherwise. It is read when the content hypotheses are judged, or at 18 months.
- **Trade mechanics:**
  - Long only. The NIFTY 50 index price stands in for an index ETF.
  - One position at a time per hypothesis (per group for policy push); a signal inside an open holding window is
    skipped.
  - Time exit, no stop. Exits fill at the official close, a 15:00–15:30 VWAP that no order gets exactly. That is
    accepted because no intraday index data exists before 2026-07-23.
  - A stock leg (the high-beta basket diagnostic, policy push) with a structural ex-date inside its window is dropped
    and counted.
  - Costs: `CostModel.round_trip` at `research_common.NOTIONAL` with equity rates. That is conservative for an ETF,
    whose STT is lower; the ETF rate card is not in `costs.yaml`.
- **Timing (no lookahead):**
  - A US or Brent session dated u is known before D's open iff u < D.
  - NSE publishes D−1's participant open interest that evening.
  - Breadth uses D−1 closes.
  - Headline news uses News Analyst calls whose `at` is before 09:00 IST on D.
  - Content news uses items with `published_at` and `first_captured_at` before 09:00 IST on D. A body counts only if
    its `fetched_at` is also before 09:00; otherwise C uses the summary alone.
- **Samples:**
  - US big-fall rebound and US rally follow-through are confirmed on 2008-01-01 → 2022-07-13 only.
    2022-07-14 → 2026-08-31 was already examined, so it is reported as seen and never counts as evidence.
  - Oil drop: 2008-01-01 → 2026-08-31.
  - Foreign short-covering: 2012-01-02, the data's first date, → 2026-08-31. Signals can start once 253 sessions of
    history exist, for the 250-session percentile and the 3-session change.
  - Breadth thrust: 2022-07-14 → 2026-08-31, the stock-data floor.
  - Combined mood score: confirmed on its first signal-capable session → 2022-07-13. That session comes once 250
    previous scores exist, about early 2014, given the FII data's 2012 start. 2022-07-14 → 2026-08-31 is reported as
    seen, because its US input was examined there.
  - Every backtest excludes 2026-09-01 → 2026-11-09, the motivating case, which is reported separately.
  - The two headline hypotheses run forward from the registration date, the first regular session after the
    registering commit. 2026-07-16 up to that date is reported as seen.
  - The two content hypotheses start at the first regular session after both the registering commit and the
    capture's first complete day (09:00 to 09:00, from its log).
- **External data:**
  - Sources: Yahoo daily chart series (`^NSEI` OHLC, `^GSPC` and `BZ=F` closes) and NSE's participant-wise F&O
    open interest.
  - A fetch script stores them once under `data/research/external/` with a manifest (URL, fetch time, rows, sha256).
    The verdict run reads only stored files.
  - Yahoo bar dates come from `meta.exchangeTimezoneName` through `zoneinfo`, never the fixed `meta.gmtoffset`. That
    field holds the current offset, and with it every US-summer `BZ=F` bar (stamped at New York midnight) lands a day
    early, a one-day lookahead. A test pins it: the bar stamped 2026-10-30 04:00 UTC is the 2026-10-30 session.
  - NSE participant-wise open interest exists from 2012-01-02 (probed 2026-11-09). Its header spelling and padding
    vary by year, so the parser matches columns by normalised name.
  - The `^NSEI` data gate:
    - Opens must equal our `bars_1d` NIFTY 50 opens over their overlap.
    - Before the overlap, a day whose open equals the previous close or lies outside [low, high] is dropped from
      the open-anchored arms and counted.
    - A calendar year that loses more than 10% of its sessions fails the gate for that year.
  - `BZ=F` rolls between contract months: an oil-drop signal whose 3-session window spans an ICE Brent expiry is
    flagged. The verdict is computed with and without flagged signals; if the two disagree, the result is null.
  - The expiry calendar changed in 2016, so it is built per era (corrected 2026-11-10, before any data was seen;
    the first draft gave only the post-2016 rule):
    - up to the February 2016 contract: the business day immediately preceding the 15th calendar day before the
      delivery month;
    - from the March 2016 contract (first expiry 2016-01-29): the last business day of the second month before
      delivery.
  - Each expiry maps to the last `BZ=F` session on or before it. A signal is flagged when its compared sessions
    meet the two sessions before an expiry through the one after, because Yahoo's roll timing is unknown.
- **Verdict per hypothesis:**
  - **Supported** iff all of these hold on its confirmatory sample:
    - at least 30 trades;
    - mean net > 0 and median net > 0;
    - `permutation_pvalue` < 0.005 (0.05/10). Its population is every session's same-construct gross return over the same
      sample; for policy push, every (group, session) 10-session excess;
    - CPCV fold-pass fraction ≥ `fold_pass_min(10)` on the net series, through the shared `cpcv_report` helper with
      purge = embargo = the holding period. At small n the helper falls back to contiguous purged folds; that is
      accepted. The margin floor (`promotable`) is reported as a diagnostic, because promotion re-applies the full
      research registration bar;
    - mean net > 0 in both halves of the sample, split at its median trade date;
    - mean net > 0 with the 3 best trades removed.
  - **Refuted** iff at least 30 trades and mean net ≤ 0.
  - **Insufficient** iff fewer than 30 trades, or the data gate fails.
  - Otherwise **null**.
  - The forward hypotheses are judged once, at their 30th closed trade; interim readings decide nothing. One with
    fewer than 30 closed trades 18 months after the registration date is insufficient. A session with no scored
    `market` cluster has no tone and cannot signal.
- **Diagnostics (decide nothing):**
  - 1-, 3- and 10-session horizons.
  - A high-beta stock-basket arm for 2022-07 → 2026-08: the top quintile by 250-session NIFTY beta among the 200
    names with the highest traded value, as of D−1.
  - Splits by NIFTY regime (`engine.strategy.regime`) and by year.
  - A 2.5×ATR(14) catastrophe-stop arm, an ETF-rate cost arm, and Nasdaq in place of the S&P for the two US
    hypotheses.
- **Tools and records:**
  - Scripts:
    - `scripts/fetch_macro_external.py`;
    - `scripts/macro_mood_study.py` (backtests; reports through `research_common.write_reports`);
    - `scripts/news_content_capture.py` and `scripts/news_content_score.py` (the content pipeline, which also runs the
      mapper for both policy hypotheses);
    - `scripts/macro_mood_forward.py` (forward looks and the headlines-versus-content comparison: read-only, always
      exit 0).
  - The registering commit adds three things: ten `config/research_trials.yaml` rows with result `registered`, the
    frozen theme lists, and the mapper prompt. Each row takes its verdict when it lands; the verdicts go to memory,
    and this entry is deleted once all ten have landed.
  - Tests pin the timing rules (Yahoo dates, the 09:00 cut-offs), the non-overlap rule, the cost application and the
    mapper's closed vocabulary.
- **Promotion:** a supported hypothesis earns nothing automatically. Using it as a recommendation source needs its own
  owner decision, plan and the research registration bar.

**News impact study (proposed 2026-11-10; registered by the commit that adds its six trial rows, after the owner
signs off the thresholds and the mapper prompt).** The owner's thesis: events hit particular stocks, so
recommendations should start from news, with supporting data and the following sessions' prices deciding entry and
exit. Owner, 2026-11-10: "Go ahead with the recommended way to test them", widened in the same message:
- news about a sector or a type of business reaches the companies in that domain, not only the companies it names
  ("government's push into EV future could mean good news for companies in the car electric wiring harness work");
- global events (Fed rate changes, war, oil) reach particular sectors or the whole Indian market.

The market-wide effect of news tone is the macro mood study's forward test, so it is not repeated here. Evidence
only; nothing is wired into the engine. The `cat` v2 shadow verdict (one gated slice of the stock hypothesis) stays
separate and is unaffected.
- **Family:** `edge/news_impact`, six hypotheses, so N = 6: `registration_alpha(6)` = 0.05/6 and `fold_pass_min(6)` =
  60%. Thresholds are fixed by this entry; changing one is a new registration.
- **Source:** the engine's live News Analyst scores. For each cluster this is the first successful `news_analyst` call
  in `state.db` `agent_calls` that scored it: its `at`, the parsed `output_json` and the cluster's headline from that
  call's `context_gz`. These scores were made at the time, before any outcome existed. `news_clusters` is not used for
  timing or text, because merges rewrite its `first_seen`, `symbols` and `sectors`. Three scorer models served the
  window (haiku-4.5, sonnet-5, then sonnet-5.5 from 2026-10-03); they are pooled, with a per-model split as a
  diagnostic. Calls begin 2026-07-28 21:15.
- **Definitions:**
  - t = the first score's `at`. Entry session D = the first regular session with t before 09:15 IST on D.
  - P0 = the close of the last regular session that closed (15:30 IST) before t.
  - Material: materiality ≥ 0.6. Positive: sentiment ≥ +0.4. Negative: sentiment ≤ −0.4.
  - `pump_promo_suspect` stories are never used. `dividend_corp_action` stories are left out of the stock
    hypotheses, because ex-dividend drops are not in the stored bars.
  - Liquid universe as of D−1: the macro mood study's rule (median close × volume ≥ ₹10 cr over the 20 sessions to
    D−1, ETFs out, a corporate-action ex-date on D−1 out).
  - Sector snapshot: the `sector_map` snapshot of 2026-11-08 for every D−1 before that date, and the snapshot in force
    on D−1 after it.
    - Earlier snapshots used an 11-label scheme until 2026-09-21 and left most names `UNCLASSIFIED` (641 of 804 on
      09-06). The 11-08 snapshot uses the 23-label scheme the mapper's vocabulary is drawn from.
    - A stock's sector is not an outcome, so reading it from a later snapshot adds no hindsight. A name that left the
      engine's universe before 11-08 has no sector there, so its story makes no stock trade, and it is counted.
    - Fixed 2026-11-10, before any outcome was computed.
  - Sector peers: the liquid members of the stock's sector in that snapshot, the stock itself excluded. At least 5
    are required, and `UNCLASSIFIED` has none.
  - Group members: a sector group's liquid members from that snapshot; a theme's liquid members from the macro mood
    study's frozen `config/research/macro_mood/theme_lists.yaml`. A basket needs at least 3.
  - β: the OLS slope of a stock's or an equal-weight basket's daily close-to-close returns on the equal-weight liquid
    universe's, over the 250 sessions to D−1, with at least 200 pairs.
  - Returns run from open(entry session) to close(exit session), the exit session being the hold's last, entry
    session counted as 1. Sector excess = trade return − the peers' equal-weight return over the same window. β-excess
    = trade return − β × the liquid universe's equal-weight return over it.
  - Net = excess − one CNC round trip at `research_common.NOTIONAL` (`CostModel.round_trip`, spread included), charged
    per leg, so an equal-weight basket pays one round trip.
  - One position per stock (per group, for baskets) at a time within each hypothesis: a signal inside an open hold is
    skipped. Several stories on one stock or group for the same D make one signal.
  - A leg is dropped and counted when it has an unadjusted corporate action (`hi52.UNADJUSTED_KINDS`) with an ex-date
    from its entry session through its exit, as in the macro mood study's baskets, or a missing entry or exit bar. For
    the pullback arm the window starts after P0's session, because a split between P0 and the fill would misprice the
    limit.
- **Symbols:**
  - Stock stories: the engine's `EntityResolver` over the first score's headline, with that score's `entities` as extra
    texts. The resolver uses the alias table and `sector_map` of the database copy the run reads, and no universe
    filter (liquidity is applied after). Alias fixes made since are therefore applied backwards, a correction rather than
    an outcome-driven choice. Every resolved liquid symbol is a candidate.
  - Mapped companies: their names go through the same resolver as extra texts with an empty headline. An unmatched or
    ambiguous name is dropped and counted; the resolver never guesses.
- **Mapper (the owner's domain and global points):** stories whose first score has materiality ≥ 0.6 and one of:
  - **domain:** scope `sector` or `theme`, or scope `market` with event type `govt_program`, `sector_policy` or
    `regulatory_policy`;
  - **global:** scope `market` with event type `global_market`, `macro_data` or `disruption`.

  Mapping rules:
  - The prompt is `config/research/news_impact/mapper_prompt.md`, used verbatim. It sees only the headline, never a
    score.
  - It returns, per story:
    - the direction for the whole Indian market (−1, 0 or +1);
    - up to 3 groups from a closed vocabulary (the 23 `sector_map` labels of the 2026-11-08 snapshot plus the seven
      frozen themes), each with a direction;
    - up to 5 NSE-listed companies by name, the most directly exposed first, each with a direction. Suppliers whose
      main business serves the affected industry count, which is the wiring-harness case.
  - The parser keeps the first 3 in-vocabulary groups and the first 5 companies. A group or company direction outside
    {−1, +1} drops that item; a market value outside {−1, 0, +1} becomes 0. Both are counted.
  - Model `claude-sonnet-5-5`, pinned. Its training data ends June 2026, before every story in the study.
  - Batches of 20 stories, through the macro mood study's `StudyLLM` on the shared subscription, never the paid API.
  - At most 40 calls a night. Calls are refused 09:15–15:30 IST on weekdays, re-checked before every call. The run
    stops at the first quota or login failure, with no retries.
  - Each call is stored append-only with its time, model, prompt sha256 and raw output in
    `data/research/news_impact.sqlite`, never in `agent_calls`.
  - Expected use: about 80 calls for sample A and about as many for sample B. A look stays "waiting" while any story
    it needs is unmapped.
- **Hypotheses:**
  - **News reading carries information:** across all stock-story observations, the Spearman rank correlation between
    x and the 10-session sector excess y is above 0.
    - Observations come from every stock story with a resolved liquid symbol, any materiality or sentiment, one per
      (D, symbol). x is the mean sentiment × materiality of that (D, symbol)'s stories.
    - A bootstrap that resamples whole entry sessions (2,000 draws, fixed seed) gives a percentile CI at
      1 − 0.05/6.
    - **Supported** iff the CI's lower bound is above 0 with at least 300 observations. **Refuted** iff ρ ≤ 0.
      **Insufficient** below 300. Otherwise **null**.
  - **Stock news, next open:** a material positive stock story ⇒ buy the stock at D's open, hold 10 sessions. Judged
    on sector excess.
  - **Stock news, pullback entry** (the owner's "watch the next trades to judge entry"): the same signals.
    - A limit buy at P0 rests over sessions D to D+4. It fills on the first session whose low is at or below P0, at
      min(that session's open, P0). An unfilled signal makes no trade.
    - Hold 10 sessions from the fill session, judged on sector excess. The peers are measured from that session's
      open, an approximation stated in the report.
    - Fills happen only on pullbacks, so the arm mixes timing and selection, as in the brk20 entry study.
  - **Domain news, named companies:** each company a domain story maps +1 ⇒ buy at D's open, hold 10, judged on
    β-excess. A company that another domain story for the same D maps −1 is skipped.
  - **Domain news, groups:** each group that a domain story for D maps +1, and none maps −1 ⇒ buy the group's liquid
    members equal-weight at D's open, hold 10, judged on β-excess.
  - **Global news, sectors:** each group that a global story for D maps +1, and none maps −1 ⇒ buy the group's liquid
    members equal-weight at D's open, hold 5, judged on β-excess.
- **Samples:**
  - **A** (history): entry sessions 2026-07-29 → 2026-10-23, read from `data/research/full_market_2026-11-09.duckdb`
    and `state.db` bounded at 2026-11-09.
  - **Excluded:** entries 2026-10-26 → 2026-11-10, the window of the November rally that motivated the owner's news
    questions. They are reported separately and never counted.
  - **Seen:** on 2026-11-10 the `cat` v2 interim reading showed 10-session outcomes of 16 `cat` signals, a gated subset
    of the stock-news population, entered 2026-09-02 → 2026-10-27. They stay in A, disclosed. No other outcome in A
    has been looked at.
  - **B** (forward confirmation): entry sessions 2026-11-11 → 2027-02-26, judged once after the last hold ends, on a
    fresh whole-file database copy. B judges every hypothesis A did not refute. The final label is B's. A hypothesis A
    refutes stops there.
- **Verdict per trade hypothesis, on each sample** (the macro mood study's rule at this family's N):
  - **Supported** iff all of these hold:
    - at least 30 trades;
    - mean net > 0 and median net > 0;
    - `permutation_pvalue` < 0.05/6. The sample's median gross excess is compared with same-size draws from the
      population: the same construct applied to every eligible liquid stock (baskets: every eligible group) entering on
      the sample's own entry sessions;
    - CPCV fold-pass fraction ≥ `fold_pass_min(6)` on the net series, with purge = embargo = the hold, through the
      shared `cpcv_report` helper;
    - mean net > 0 in both halves, split at the median entry date;
    - mean net > 0 with the 3 best trades removed.
  - **Refuted** iff at least 30 trades and mean net ≤ 0. **Insufficient** iff fewer than 30 trades or the data gate
    fails. Otherwise **null**.
- **Diagnostics (decide nothing):**
  - Horizons 1, 3, 5, 10 and 20 sessions for every hypothesis.
  - **Supporting data (the owner's second step), for the stock and named-company hypotheses.** Each split is reported
    with a permutation p against same-size random subsets of that hypothesis's own trades (10,000 draws, fixed seed).
    A split with p < 0.01 is a candidate for its own forward registration, never a filter. The splits:
    - volume on the last session before entry ≥ 2× its 20-session median;
    - close(D−1) above its 50-session average;
    - the engine's NIFTY regime up at D−1;
    - an open-gap ≥ +2% on D;
    - materiality ≥ 0.8;
    - two or more source domains among the cluster's headlines ingested before t;
    - scorer model;
    - event type, for types with at least 10 trades.
  - **The negative side (exit or avoid value; the study is long-only):** the sector excess after material negative
    stock stories, and the β-excess of companies and groups mapped −1, each with a session-block CI.
  - For global stories, the event-type split, and the rank correlation between the mapped market direction and the
    NIFTY's open(D) → close(D+4).
  - The pullback arm's fill rate; the excluded window's results; per-hypothesis counts of dropped stories, unresolved
    names and voided legs.
- **Tools and records:**
  - `scripts/news_impact_map.py` runs the mapper over the stories a sample needs.
  - `scripts/news_impact_study.py` computes a sample's verdicts and diagnostics. It is read-only, exits 0, and writes
    `data/reports/news_impact_<sample>_<stamp>.{json,md}` through `research_common.write_reports`.
  - The registering commit adds six `config/research_trials.yaml` rows with result `registered`, and the mapper prompt
    is frozen from then on. Each row takes A's label when A lands and B's when B lands. Verdicts go to memory, and this
    entry is deleted once B lands.
  - Tests pin the timing (t, D, P0), the pullback fill, the non-overlap rule, the benchmarks, the cost application and
    the mapper parser's vocabulary and caps.
- **Promotion:** as for the macro mood study, a supported hypothesis earns nothing automatically.

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
