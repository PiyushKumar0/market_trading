# TradingAgents review and system check (2026-10-27)

Owner request (2026-10-27): review TauricResearch/TradingAgents in depth, say what it does well and badly and what we
can take from it, and do a comprehensive check of our own system: where we can improve and what we are doing wrong.

Reviewed: TradingAgents `main` at e8f5503 (v0.7.0, 2026-10-16; 493 commits), cloned read-only. Our side: the code on
`phase3` at 424d486, plus read-only queries of `data/state.db` and of the nightly export
`data/backups/market-20261026T224305` (bars through 2026-10-26). TradingAgents pointers are `TA:path:line` against
that commit; ours are repo-relative.

## 0. Bottom line

1. **TradingAgents is a well-engineered research scaffold, not a trading system.** It runs twelve LLM roles over one
   ticker and one date: four analysts, a bull/bear debate, a research manager, a trader, a three-way risk debate and a
   portfolio manager. The output is a five-tier rating. It models no costs. It has no universe screening, no hard risk
   limits and no executable order, and its only evaluation is a mean 5-day alpha by rating, with no costs and no
   confidence intervals. Its own README calls the figures "indicative rather than repeatable". The paper's headline
   (AAPL +26.6% with a Sharpe of 8.21, over one quarter, without costs) has never been reproduced. Independent costed
   tests find no consistent edge over buy-and-hold. The maintainers keep the model's own memory of history open as "a
   fundamental limitation" (§4).
2. **What is worth copying is its defensive LLM engineering, not its architecture.** Over 2026 its changelog is a
   catalogue of LLM failure modes found and fixed. Most of those defences we already have in some form. A few are
   worth porting (section 8): systematically scoring every LLM decision against outcomes, absent-vs-empty markers,
   and per-call prompt provenance.
3. **Do not port the multi-agent debate.** Our own data answers the question it raises. On 585 scanner candidates
   since July, the intraday analyst's approvals did no better than its declines. Within the same day the difference
   is about zero at T+5 and T+10, and negative at T+20. Stacking eleven more LLM roles on a judge with no measured
   selection skill would multiply cost, latency and failure surface for nothing we can show.
4. **Our binding problem is evidence, not engineering.** Live, 24 closed recommendations have produced 2 winners: a
   median net of −4.5% and a median excess of −2.8% against the equal-weight benchmark, in a market that fell 11.8%.
   Every strategy is long-only, and no live rule pauses entries in a falling market. The cards ship stops that our
   own C1 study says turn the edge negative. The LLM judge is a mandatory, unmeasured, sometimes-dark gate, and the
   learning machinery is table-only.

The ranked findings are in section 7 and the recommended owner decisions in section 9.

## 1. How TradingAgents works (v0.7.0)

### 1.1 The graph

```
            ┌─ Market analyst ─────┐ tools: OHLCV, indicators, verified snapshot
            ├─ Sentiment analyst ──┤ pre-fetched Yahoo news + StockTwits + Reddit, no tools
 START ─────┼─ News analyst ───────┤ tools: ticker news, global news, FRED macro, Polymarket
 (ticker,   ├─ Fundamentals ───────┤ tools: profile, statements (SEC EDGAR as filed), insider trades
  date)     └─ Memory Log step ────┘ settles past decisions, loads "lessons" (runs beside the analysts)
                      │  all four reports
                      ▼
            Bull ⇄ Bear  (quick model, max_debate_rounds × 2 turns, fixed order: Bull first)
                      ▼
            Research Manager (deep model) → ResearchPlan: 5-tier rating + rationale   [sees the debate only]
                      ▼
            Trader (quick) → TraderProposal: Buy/Hold/Sell, entry, stop, sizing text  [+ market report]
                      ▼
            Aggressive → Conservative → Neutral  (quick, max_risk_discuss_rounds × 3 turns)
                      ▼
            Portfolio Manager (deep) → PortfolioDecision: 5-tier rating                [+ lessons]
                      ▼
            decision appended to a markdown memory log; settled later on 5-day alpha
```

- Wiring: `TA:tradingagents/graph/setup.py:142-187`. Analysts run in parallel beside the Memory Log step (:157-166).
  The debate order is fixed (`TA:tradingagents/graph/conditional_logic.py:12-33`).
- Model tiers: the quick model serves the analysts, researchers, debaters and trader; the deep model serves the
  research manager and portfolio manager (`TA:tradingagents/graph/setup.py:126-140`). Any provider can serve
  either tier (`TA:tradingagents/default_config.py:101-107`).
- Defaults: 1 debate round, 1 risk round, 20 tool rounds per analyst, a 5-day outcome window, and `.NS` benchmarked
  against `^NSEI` (`TA:tradingagents/default_config.py:133-137, 180-183`).

### 1.2 Who sees what

| Node | Inputs | Output | Pointer |
|---|---|---|---|
| Analysts | their own tools, clamped to the run date | a "very detailed" prose report plus a markdown table | `TA:tradingagents/agents/analysts/*.py` |
| Bull / Bear | all four reports, the debate history, the opponent's last turn | rhetoric ("deliver a compelling bull argument") | `TA:tradingagents/agents/researchers/bull_researcher.py:31-49` |
| Research Manager | **the debate history only**, not the reports | rating, rationale, actions | `TA:tradingagents/agents/managers/research_manager.py:17-51` |
| Trader | the plan, the market report, the portfolio | Buy/Hold/Sell, optional entry, stop and sizing text | `TA:tradingagents/agents/trader/trader.py:23-91` |
| Risk debaters | the trader's plan, all four reports, the history | assigned stances ("champion high-reward…") | `TA:tradingagents/agents/risk_mgmt/aggressive_debator.py:32-46` |
| Portfolio Manager | the plan, the trader's plan, the risk debate, lessons | final rating, summary, thesis, optional target | `TA:tradingagents/agents/managers/portfolio_manager.py:26-76` |

### 1.3 Memory and evaluation

- **Memory:** each run appends its decision to a markdown log. A later run settles any decision whose window has
  traded: it computes the raw and alpha return over 5 trading days, from the first close after the decision date,
  against the regional index (`TA:tradingagents/memory/settlement.py:52-103`). It then asks the quick model for a
  2–4 sentence "lesson" (`TA:tradingagents/memory/reflection.py:13-34`). The portfolio manager reads the last 5
  same-ticker decisions with their reflections, plus 3 cross-ticker lessons (`TA:tradingagents/memory/log.py:93-130`).
  The lessons are filtered to those resolved by the run date (:105-107).
- **Evaluation:** `run_backtest` runs the graph over a ticker×date grid. `summarize` reports, per rating, the count,
  the directional hit rate and the mean alpha (`TA:tradingagents/backtest.py:88-126`). The module states its own
  scope: "It is not a portfolio simulator, and must not grow one", with no quantity, fill or cash ledger (:9-14).

### 1.4 What its changelog says it learned

Its 2026 releases record the same classes of defect we have hit, often found by outside users:

- **Look-ahead, repeatedly:**
  - statements dated by period rather than filing date;
  - insider trades dated by trade date rather than filing date;
  - FRED served at today's vintage;
  - social and news feeds unwindowed;
  - memory lessons from outcomes not yet known;
  - present-day company profiles in past runs;
  - and, only on 2026-10-16 (#1518), decisions entered at the decision-day close using news published after that
    close (`TA:CHANGELOG.md:22-26, 244-283`).

  Every backtest made with v0.6.0 or earlier carries that last bias.
- **LLM fabrication:**
  - an invented opponent argument when a debater opened against an empty slot (#1176);
  - invented social posts under prompt pressure (#557);
  - the wrong company inferred from a chart (#814);
  - invented price levels (#830);
  - lessons invented from an empty memory (#572);
  - an unreadable rating coerced to a tradeable Hold (#1170);
  - a quoted "Consensus rating: Buy" read as the decision (#1466).

### 1.5 Cost profile

With defaults, one ticker-date costs four tool-looping analysts, then 2 debate calls, 1 deep call, 1 trader call, 3
risk calls, 1 deep call, and a reflection for each settled decision.
- The paper reports "11 LLM calls & 20+ tool calls/prediction".
- Users measured 16–22 calls with about 0% prompt-cache hits
  ([#750](https://github.com/TauricResearch/TradingAgents/issues/750)), and about $0.12 per decision on small OpenAI
  models (#168).
- Every debate prompt carries all four reports plus the growing history.

At that price TradingAgents is a tool for analysing a ticker someone has already chosen; it cannot screen a universe.
It has no screening, ranking or universe concept at all: a search of `tradingagents/` for universe, rank, screener and
watchlist finds nothing.

## 2. What TradingAgents does well

These are mostly LLM-engineering defences, and each was earned by a real failure:

1. **Point-in-time enforcement at the tool boundary.**
   - Every dated tool takes the run date from graph state (`InjectedState`), so the model cannot ask for a later
     date (`TA:tradingagents/agents/tools.py:1-5`). `as_of` / `as_of_window` clamp what it does ask for
     (`TA:tradingagents/dataflows/date_window.py:78-105`).
   - Data that cannot be dated is withheld from past runs rather than served:
     - present-day profiles;
     - statements without a filing date;
     - insider trades dated by trade date (:108-170).
   - US statements come from SEC EDGAR as filed, restatements excluded.
2. **Absent is not empty.**
   - A report that was never produced renders as "not available, not an empty finding"
     (`TA:tradingagents/agents/context.py:190-201`).
   - A feed that never observed the window says so rather than implying "no news" (`date_window.py:44-68`).
   - A debater with no opponent yet is told to open, not to rebut (`context.py:37-48`, #1176).
   - Tests pin all three (`TA:tests/test_prompt_integrity.py`, `TA:tests/test_debate_opening.py`).
3. **Unreadable output is never a tradeable default.** A rating that cannot be parsed becomes `REVIEW`, never Hold
   (`TA:tradingagents/agents/rating.py:26-30, 96-103`).
4. **Numbers are grounded in a deterministic snapshot.** The market analyst must quote exact prices and indicator
   values from a verified, non-LLM snapshot (`TA:tradingagents/dataflows/vendors/yahoo/snapshot.py:1-49`). The
   instrument's identity is resolved once, deterministically, and is not inferred from the chart
   (`TA:tradingagents/agents/context.py:61-169`).
5. **Data that should not be generated is pre-fetched.** The sentiment analyst gets its posts in the prompt and has
   no tools, so it cannot "find" posts that do not exist (`TA:tradingagents/agents/analysts/sentiment_analyst.py:1-18`).
6. **Bounded loops.** After `max_tool_rounds` an analyst is told to write its report from what it has, and that turn
   ends it (`TA:tradingagents/graph/setup.py:53-90`).
7. **Provenance.** Every report and state log records the version, providers, models, analysts, rounds and vendors
   (`TA:tradingagents/graph/trading_graph.py:255-275`). The checkpoint key hashes the whole configuration (:140-165).
8. **Typed decisions with fallbacks.** The three decision nodes use native structured output, with a free-text
   fallback (`TA:tradingagents/agents/structured.py:42-95`). Field validators null out percentage-as-price and range
   answers instead of failing the whole decision (`TA:tradingagents/agents/schemas.py:33-58`).
9. **Engineering hygiene.**
   - About 100 test files, including architecture tests: vendor libraries may only be imported by the data layer
     (`TA:tests/test_layering.py`).
   - CI and checkpoint resume.
   - Honest README sections on reproducibility and non-determinism.
10. **Decision scoring as a first-class tool.** "One run gives one decision, which cannot tell you whether the system
    decides well" (`TA:README.md:366`). The backtest grid exists so that every rating is scored against what
    happened. This is the idea most worth taking (section 8, A1).

## 3. Where TradingAgents falls short

Each point was checked against the code at e8f5503.

1. **No costs anywhere.** `tradingagents/` contains no commission, slippage, spread or fee: the only "spread" hit is
   the FRED 10y–2y series. The prompts push toward action: "conflict alone is not a reason to Hold"
   (`TA:tradingagents/agents/managers/research_manager.py:36`, `portfolio_manager.py:66`, `schemas.py:104-111`).
   That raises turnover, and turnover is exactly the cost it does not count. On our universe, cost geometry alone
   killed five strategy families (memory `orb-backtest-structural-negative`).
2. **Evaluation that cannot show an edge.**
   - The scoring window is 5 trading days, while the portfolio manager writes theses like "3–6 months". The reflection
     prompt admits the mismatch (`TA:tradingagents/memory/reflection.py:18-21`).
   - It reports a mean alpha per rating, with no confidence interval, no significance test and no baseline rule.
   - It takes one model sample per cell, and the news and social inputs are not archived
     (`TA:tradingagents/backtest.py:121-125`).
   - A positive mean alpha from this tool cannot be told apart from noise or beta.
3. **The model's own memory is a look-ahead the tools cannot clamp.** All the point-in-time work bounds what the tools
   return. Nothing bounds what the model already knows: a backtest dated before the model's training cutoff is scored
   on outcomes the model may have read about. The only mention is the prompt anchoring "now" to the run date
   (`TA:CHANGELOG.md:372-373`, #836). For our use this is decisive: any LLM-as-decider backtest on 2024–2026 dates with
   a current model is contaminated.
4. **Debate measures rhetoric, not truth.**
   - Each side is told to win: bull "deliver a compelling bull argument", aggressive "champion high-reward"
     (`TA:tradingagents/agents/researchers/bull_researcher.py:31-49`, `risk_mgmt/aggressive_debator.py:32-46`).
   - The judge is told to pick "the stronger case".
   - Speaking order is fixed, with the bull first and the neutral last (`TA:tradingagents/graph/conditional_logic.py:12-33`).
     The prompt asks the judge to ignore order, but nothing randomises it.
   - Neither judge is checked against outcomes.
5. **Information is lost at each hand-off.** The research manager never sees the analyst reports, only the debate
   about them (`TA:tradingagents/agents/managers/research_manager.py:17-51`). The trader gets the market report back
   only because of #1167. A fact neither debater mentions cannot reach the decision.
6. **Risk management is text.** The three risk roles argue about the trader's plan. There is no position limit,
   exposure cap, stop enforcement, loss limit or kill switch anywhere in the package. "Position sizing" is free text,
   e.g. "5% of portfolio" (`TA:tradingagents/agents/schemas.py:180-183`).
7. **Learning from noise.** A lesson is one 5-day outcome of one stock, turned into a "concrete lesson to apply to the
   next similar analysis" (`TA:tradingagents/memory/reflection.py:28-31`). Nothing pools, tests or retires a lesson.
   With single-stock 5-day volatility of about 4–5%, a sample of one is almost pure noise. The design invites
   superstition, and nothing measures whether lessons help.
8. **Selection is outsourced to the user.** It analyses whatever ticker it is given. A user who backtests the names
   they find interesting has chosen the sample.
9. **Data quality.** yfinance is an unofficial scrape. StockTwits and Reddit return recent items only, so historical
   sentiment is not point-in-time; the module says so itself (`TA:tradingagents/agents/analysts/sentiment_analyst.py:12-13`).

## 4. External evidence on TradingAgents

Gathered by a read-only research pass (papers in full text; all 1,547 issues and PRs searched; 26 threads read).
The paper's Table 1 and its footnote, issue #805 and issue #1196 were re-checked at source for this review.

### 4.1 The paper ([arXiv 2412.20138](https://arxiv.org/abs/2412.20138), v7, 2025-06-03)

- **Setup:**
  - Period: 2024-01-01 to 03-29 (§5.1). Table 1 covers three stocks: AAPL, GOOGL and AMZN.
  - Models: gpt-4o-mini or gpt-4o as the quick model, o1-preview as the deep one (§4.3).
  - Baselines: buy-and-hold, MACD, KDJ&RSI, ZMR and SMA.
- **Headline (Table 1):**

  | Stock | TradingAgents return | Sharpe | Max drawdown | Buy-and-hold return |
  |---|---|---|---|---|
  | AAPL | +26.62% | 8.21 | 0.91% | −5.23% |
  | GOOGL | +24.36% | | | +7.78% |
  | AMZN | +23.21% | | | +17.1% |

- **Never reported, in any of the seven versions:** transaction costs, seeds, the number of runs, confidence
  intervals, position sizing, any ablation, or a limitations section.
- **The only caveat** is a footnote added in v4: "We benchmarked TradingAgents over 3 months due to intensive LLM and
  tool use (11 LLM calls & 20+ tool calls/prediction). The highest Sharpe Ratio exceeds our expected empirical
  range."
- **Version drift:** v1 stated a June–November 2024 window and v2 onward state January–March 2024, while Table 1 is
  identical in all seven versions.
- **Outsiders get a different benchmark:** their recomputations of AAPL buy-and-hold come out around −7.5% to −7.6%,
  not −5.23% ([#137](https://github.com/TauricResearch/TradingAgents/issues/137),
  [#168](https://github.com/TauricResearch/TradingAgents/issues/168)).
- **No paper-run code was ever released.** #119 and #137 were closed as "addressed in the current version".
- **The public code leaked future data along at least seven paths, fixed March–October 2026:** price end dates (#475),
  yfinance news (#1007), Reddit and StockTwits (#1220), memory lessons (#1251), FRED vintages (#1275), the fundamentals
  snapshot (#1300) and post-close news (#1518). Whether the paper's own runs carried them is unknown.

### 4.2 Independent evaluations

| Study | What was tested | Costs | Result |
|---|---|---|---|
| Alpha Illusion, [arXiv 2605.16895](https://arxiv.org/abs/2605.16895) (position paper) | one-year reproduction, 2025-01 to 2026-01, TSLA/NVDA/KO/XOM/MSTR, $100K | yes: commission, spread, impact, tokens | TradingAgents $106.4K gross, **$102.3K net**, vs buy-and-hold $104.8K; Sharpe 0.43 gross, 0.22 net; "net buy-and-hold still beats both agents on 4 of 5 tickers" |
| Agent Market Arena, [arXiv 2510.11695](https://arxiv.org/abs/2510.11695) | live, 2025-08 to 09; a TradingAgents-role agent on 5 LLM backbones | none | below buy-and-hold on TSLA and ETH in every configuration; TSLA ranges from −38.7% to +21.9% depending on the backbone |
| Profit Mirage, [arXiv 2510.07920](https://arxiv.org/abs/2510.07920) | 5 LLM agents on GPT-4o, before vs after its training cutoff | per their method section | TradingAgents' total return decays 50.18% and its Sharpe 55.68% across the cutoff, the memorisation signature (the authors sell a rival method) |
| FINSABER, [arXiv 2505.07078](https://arxiv.org/abs/2505.07078) (KDD'26) | FinMem and FinAgent, the same class of system, on the S&P 500 including delisted names, 2004–2024 | yes | buy-and-hold has the best Sharpe; "neither LLM agent generates statistically significant alpha" (all p > 0.34) |
| Survey, [arXiv 2603.27539](https://arxiv.org/abs/2603.27539) | 12 systems scored against minimum evaluation standards | n/a | TradingAgents scores 1/5: "extraordinary Sharpe of 5.60 to 8.21 based solely on a three-month bullish window" |
| [arXiv 2504.14765](https://arxiv.org/abs/2504.14765) | LLM recall of financial data | n/a | "Instructions to respect historical boundaries fail to prevent recall-level accuracy"; no recall after the cutoff |

No independent study reproduces the paper's result. Every costed, longer test found no consistent edge over
buy-and-hold.

### 4.3 What the issue tracker shows

- **Model-level look-ahead is acknowledged and unfixable in code.**
  [#805](https://github.com/TauricResearch/TradingAgents/issues/805), "Temporal Knowledge Leakage in LLM
  Backtesting", has been open since 2026-05-11. The maintainers call it "a separate, fundamental limitation a code
  change can't fully eliminate".
- **Not reproducible.** #168 ran AAPL on the paper's dates with the default models: "Very bad results -25% in
  returns". The maintainers' answer: backtest returns "aren't guaranteed to match any published figure".
- **The debate pushes toward action.** In [#1196](https://github.com/TauricResearch/TradingAgents/issues/1196) the
  full graph rated AAPL Overweight in 16 of 20 runs, against 6 of 20 for a single call (Fisher p = 0.0036). At
  temperature 0 it was 6 of 10 against 0 of 10.
  - The fix over-corrected: one deployment went 86–89% Hold (#1321).
  - Same-day reruns flip between Overweight and Underweight (#1239, #1517).
- **Cost and latency:**
  - The paper: 11 LLM calls and 20+ tool calls per decision.
  - #750: 16–22 calls per run, with about 0% prompt-cache hits.
  - #168, user-reported: about $0.12 per decision on o4-mini and gpt-4o-mini.
  - #111: a 13-minute single run on gemini-2.5-flash.
- **Trading-R1** ([arXiv 2509.11420](https://arxiv.org/abs/2509.11420)), the authors' follow-on, fine-tunes Qwen3-4B on
  data from 2024-01 to 2025-05, and its test window (June–August 2024) sits inside that span.
  - It compares against other LLMs only, with no buy-and-hold row.
  - Thirteen months on, neither the model nor the data has been released.

### 4.4 What it means for us

The one published edge is unreproduced. It was measured without costs, on a single bullish quarter, using models
whose training may cover that window. Every independent test with costs and longer horizons finds no consistent edge.
A user measured the signature mechanism, the debate, and it skews toward action. All of this agrees with what our own
data says in §6.2: more LLM deliberation is not evidence of more edge.

## 5. Side by side

| Dimension | TradingAgents v0.7.0 | Our engine | Who is ahead |
|---|---|---|---|
| What decides a trade | a chain of LLM roles ending in a portfolio manager's rating | deterministic scanners originate; one LLM approves or declines; a deterministic gate disposes | ours: every origination is a testable rule |
| Universe | whatever ticker the user passes | NIFTY Total Market eligible set; the NIFTY 500 core is the evidence population | ours |
| LLM calls per decision | 11 LLM plus 20+ tool calls (paper); 16–22 calls (users, #750) | 1 intraday-analyst call, plus shared news scoring | ours: roughly 11–22× fewer calls per decision |
| Adversarial reasoning | bull/bear debate plus a three-way risk debate | none | theirs exists but is unmeasured |
| Risk control | debate text | 25+ gate rules, envelope, kill switch, hash-verified protected store | ours |
| Costs | none | Zerodha rate card, spread and fill model; cost-floor check | ours |
| Strategy evaluation | mean 5-day alpha by rating; no CI, no costs | pre-registered verdicts, CPCV, EW benchmark, pinned looks, kill rules | ours, with the gaps in §6.4 |
| Scoring the LLM's own decisions | every decision settled against alpha | **none** (`llm_filter_decision` hard-coded "confirmed", `src/engine/ops/pipeline.py:2396`) | theirs: the idea to take |
| Memory | one 5-day outcome per lesson, fed to the portfolio manager | nightly summary into the next day's planner; everything else table-only | neither is measured |
| Point-in-time | clamped at every tool; undatable data withheld | `broadcast_dt` knowledge time for filings, an after-hours rule, revisions dropped; some research reads use "latest" (§6.5) | similar intent; theirs is more structural, ours has better Indian data |
| Structured output | Pydantic, with a free-text fallback | strict JSON schema, 2 retries, fail closed | ours is stricter, and pays for it in schema failures (§6.2) |
| Reproducibility | inputs not archived; outputs non-deterministic | every call's prompt (`context_gz`) and output archived; replay script | ours |
| Provenance | full run settings in every report | model per call; prompt recoverable from `context_gz` | theirs is slightly better (A3) |
| Operations | CLI, Docker, checkpoint resume | Windows service, watchdog, reconciliation, Telegram, dashboard, paper OMS | ours, by far |
| Size | about 5K lines of core Python, about 100 test files | about 62K engine lines, 69K test lines, 21K script lines | ours carries far more operational weight |

## 6. Our system: the check

### 6.1 Live results (state.db, read-only, 2026-10-27)

**Market context:** the core equal-weight benchmark fell **−11.77%** over the 41 sessions from 2026-08-26 to 10-26
(`universe_ew_returns`).

**Delivered recommendations:** 112 recs in all, 44 entries and 68 exits.
- The owner took 2 entries, both in August; the other 110 expired.
- `skip_reason` is NULL on all 112, so the D5 reason capture has never been used.
- All 68 exit recs expired. They were about the only two positions ever taken: HDFCAMC (42) and HINDZINC (26), up to
  10 a day between 08-27 and 09-10. 21 of them came after the broker holdings already showed the position sold
  (memory `rec-take-rate-diagnosis`).

**Hindsight scoring of entries** (`rec_outcomes`): 44 rows, of which 24 are closed, 4 open and 16 unfilled.

| Strategy | Closed | Hits | Median net | Median excess vs EW | Exits | No-stop T+20 median (n) | No-stop excess T+20 |
|---|---|---|---|---|---|---|---|
| brk20 | 12 | 2 | −2.37% | −1.81% | 9 stop, 2 target, 1 time | −7.04% (6) | −0.68% |
| hi52 | 7 | 0 | −6.55% | −4.82% | 6 stop, 1 time | −7.31% (3) | −2.28% |
| ins | 5 | 0 | −5.68% | −2.93% | 4 stop, 1 time | −6.33% (5) | −0.22% |
| **all** | **24** | **2** | **−4.53%** | **−2.81%** | 19 stop | | |

- The shipped exits lost money in absolute terms and also against the falling benchmark.
- Holding to T+20 without the stop would have lost even more in raw terms, because the market fell. Against the
  benchmark, the no-stop excess is close to zero.
- The sample is small and covers one regime: no edge claim either way. Nothing in it supports the registered edges
  either.

**Paper book** (live since 10-08):
- Realised −₹184. Of 4 filled entries, 1 was stopped (MOTILALOFS, −₹365), 1 closed on a GTT-failure exit
  (PVRINOX, +₹251) and 2 are open on time exits (RAIN, BHARTIARTL).
- Maximum drawdown −1.05%.
- Too early to say anything.

### 6.2 The LLM layer

**Roster** (agent map; `config/agents.yaml:100-169`). Every role runs on `sonnet-5.5`, single-shot, with no tools:

| Role | Job | Consumer |
|---|---|---|
| intraday analyst | judges each forwarded candidate and position event; every 20 min, a heartbeat | gate |
| news analyst | scores headline clusters | the deterministic digest, which drives the shadow `cat` legs and context lines |
| pre-open planner | writes the day plan | the analyst's prompt only |
| nightly reviewer | writes the evening summary | the next day's planner prompt only |

**Spend and reliability** (`agent_calls`, July–October):
- **Volume and spend:** 4,385 calls and $562.88 of the self-imposed budget. That splits into intraday $200, news
  $327, planner $22, nightly $12 and smoke $1.
- **Intraday success rate:** 60% in July, then 77%, 80% and 79%. The largest failure class is `schema_invalid`, with
  185 calls from July to September; timeouts and SDK errors follow.
- **Latency:** the median intraday latency is about 30 s per candidate.
- **The 10-22/23 outage:** the shared login was blanked and every role failed `auth_failed`. On 10-22 the funnel
  evaluated nothing: origination stops whenever the LLM tier is dark.

**Where the intraday analyst's 1,170 successful calls went:**

| Call type | Calls | Spend | Note |
|---|---|---|---|
| heartbeat | 517 | about $54 | can only return no_action; the output is a regime note fed back to the same agent |
| candidate evaluation | 585 | | 188 enter, 397 decline (68% declined) |
| position event | 68 | | exit proposals |

**Does the analyst's judgement add value?** It was measured on 585 unique candidates, at first evaluation, with the
same rules for both arms:
- entry at the open of the next session;
- exit at the close of session h;
- windows containing a recorded corporate action dropped.

| Comparison | T+5 | T+10 | T+20 |
|---|---|---|---|
| Within-day paired, approved − declined | +0.04 pp (approved ahead on 16 of 28 days) | −0.04 pp (13 of 23) | −1.50 pp (5 of 15) |
| brk20, approved vs declined | −2.67% vs −2.60% (n 113/99) | −4.96% vs −2.75%, diff −2.20, CI [−4.16, −0.30] | −6.58% vs −3.96% (n 46/56) |
| hi52, approved vs declined | −2.14% vs −4.93% (n 20/12) | −3.82% vs −0.86% | −9.74% vs −6.49% |

- **The result:** no evidence that the analyst's enter/decline separates better from worse candidates. At brk20's
  own horizon, the approved arm did worse.
- **Caveats:** one falling regime; bootstrap CIs that ignore same-day correlation, so they are too narrow; brk20's
  live entry is a limit at the level, not next-open. The pooled all-strategy comparison is confounded by the mix of
  strategies, so it is not used.

**The confidence floor.** The gate rejects any proposal with confidence below 0.55 (`config/limits.yaml:184`,
`src/engine/risk/gate.py:619-627`). The model is never told that threshold. A search of the intelligence package and
the strategy contracts finds no 0.55 confidence anywhere.
- The model writes coarse values: for brk20, 0.50 fifty-four times and 0.55 forty-three times. So the floor falls
  exactly between its two favourite round numbers.
- Within enter proposals, the ≥0.55 half did better at T+5 and T+10: brk20 −1.53% vs −3.82% (n 57/56), then −3.76% vs
  −6.88%. At T+20 it did worse: −7.54% vs −3.52%, with only 11 below the floor. There is no within-day control.
- The `limits.yaml` comment says the floor is "owner-tuned to hit-rate", but no code anywhere compares confidence with
  outcomes.

**The gate and the analyst's proposals.** Of 187 real entry proposals the gate rejected 143 (76%), shrank 35 and
approved 9, so 44 recs were delivered.
- Rejected proposals usually fail several rules: per-trade risk 91, per-stock exposure 90, confidence 79, minimum
  viable size 40, open positions 31, entry sanity band 27, stale data 25.
- The analyst proposes quantities and levels the gate then cuts. Rule 7 asks it to stay within the
  `max_qty_by_risk` quoted to it, and it often does not.

### 6.3 Decision pipeline (decision map)

- **Flow:**
  1. Daily and intraday scanners: brk20, hi52, ins, `cat` legs (batch); rsi2, trend, mom, orb (bar-driven).
  2. Pre-screen: ranks by score, then strategy and day caps; 48 forwarded a day at most.
  3. Forward queue: one analyst call every 3 minutes (`src/engine/ops/pipeline.py:229`).
  4. Analyst.
  5. Gate.
  6. Card, then the owner; or the paper branch, judged on its own book (`pipeline.py:2406-2429`).
- **Funnel since August** (`funnel_raw_counts`, `prescreen_day_slots`):
  - brk20 fired 454 times, which gave 367 slots, 286 evaluated and 236 forwarded.
  - hi52 fired 142 times, 32 of them forwarded.
  - ins fired 12 times. The trend scanner is still forwarding candidates (34) that the analyst is told have "no
    validated net edge" and usually declines.
- **The live construct differs from the tested one for rsi2 and trend.** Live, rsi2 exits on a 10-session hold, not
  its RSI exit. trend has no live trailing stop: the initial trail level stays fixed, with a 120-session cap. The RSI
  exit and the trailing stop exist only in `src/engine/learning/sweep.py`.
- **The economics check for targeted strategies prices an LLM-chosen number.** For strategies without a registered
  edge (brk20, rsi2, trend) the cost check takes `expected_edge_pct = 100 × (target − entry) / entry` from the
  analyst's target (`src/engine/risk/gate.py:1062-1067`, `src/engine/strategy/cost_model.py:348-360`). That is the
  full distance to the target with no hit probability: a cost-geometry floor, not an expected value.
  - A 2R target clears a 0.3% round trip by construction.
  - brk20 hit its target 2 times in 12.
  - The card prints this number on its cost line as "edge 4.13x" (the BHARTIARTL brk20 card, 10-26) with no caveat.
    The "(the risk check's figure) — not a forecast" note is printed only beside a registered edge
    (`src/engine/notify/catalog.py:1186-1195`). So the owner sees a multiple that is true only if the target is hit.
- **The LLM proposes the quantity** (`src/engine/core/contracts.py:97`); the gate only shrinks it. Sizing is
  deterministic arithmetic the model is asked to repeat.

### 6.4 Learning and evaluation (learning map)

**Strong:**
- CPCV with C(6,2) = 15 splits and 5-day purge and embargo (`src/engine/learning/validate.py:65-69`).
- Strict net-of-cost pass rules and a margin floor.
- Pre-registered forward rules with pinned looks (`scripts/hi52_forward_verdict.py`, `scripts/no_stop_verdict.py`,
  `scripts/cat_shadow_verdict.py`).
- A hindsight-only outcome table with an equal-weight benchmark (`src/engine/ops/rec_outcomes.py:3`).
- Evidence lines on the card, read from the report and never typed in.

**Weak:**
- **Multiple-testing control is a step function.** The fold-pass bar is 60%, 70% or 80% by trial count N
  (`validate.py:105-117`), and N is the grid size of one sweep (`src/engine/learning/sweep.py:525`).
  `validate_insider` declares N = 1 (`scripts/validate_insider.py:8-9, 257`), although the insider leg was chosen
  from a multi-leg event study and its robustness slices. There is no deflated Sharpe, PBO or permutation test.
  Across the research program dozens of families and variants were tried, and the bar does not see them.
- **Forward kill rules exist for hi52 only.** brk20 (12 of the 24 closed recs, 2 hits) and ins (0 of 5) are tested
  only for stop vs no-stop (`scripts/no_stop_verdict.py`). A "STOP NOT THE PROBLEM" verdict cannot retire either one.
- **The online-learning foundations are table-only or absent.**
  - `param_sets` are never promoted: 49 rows, all `candidate`.
  - Champion/challenger, River and drift detection do not exist.
  - `model_registry`, `shadow_trades` and `envelope_state` hold 0 rows each.
  - The scanners still run on code defaults; `params_from_envelope` is unwired (`src/engine/strategy/scanners/base.py:118-127`).
  - `learning_ledger` columns `mae`, `mfe`, `slippage_*` and `regime_label` are never written.
- **The nightly reviewer's structured output is read by nobody.** In 59 reviews it has made 0 `param_suggestions`.
  `lessons`, `process_errors` and `trade_attributions` are stored and counted, never read. Only `summary` reaches
  anything, the next day's planner prompt (`src/engine/ops/preopen_planner.py:444-460`).
- **No LLM output is ever scored against outcomes:** not the news analyst's materiality or sentiment, not the planner's
  focus list, not the analyst's decisions or confidence.

### 6.5 Data and point-in-time (data map)

**Strong, and better than TradingAgents for this market:**
- Filings carry the exchange dissemination time (`broadcast_dt`), and every as-of join keys on it, never the period
  label (`src/engine/marketdata/store.py:610-611`, `src/engine/datafeeds/filings_pit.py:11-14`).
- A broadcast after 15:30 belongs to the next session (`src/engine/datafeeds/insider_crossings.py:63-91`).
- Revised results are dropped rather than back-dated (`src/engine/datafeeds/filings_results.py:216-218`).
- The digest reads history strictly before d (`src/engine/datafeeds/news_pipeline.py:1618-1625`).
- Headlines only, forced untrusted, and symbols are never LLM-assigned.

**Possible early-visibility reads.** These are harmless live, but they matter for any replay or backtest that uses
them:
- Features read the latest sentiment digest and the current theme and alias maps, not those as of d
  (`src/engine/features/engine.py:625-630`, `news_pipeline.py:581-589`).
- The earnings calendar has no knowledge time, so `results_today` uses dates that may have been announced later
  (`store.py:2393-2401`).
- Insider reads key on `broadcast_dt`, not `ingested_at`. A backtest therefore assumes instant knowledge, while the
  live feed has gone weeks without ingesting (memory `ins-feed-symbol-starvation`) (`store.py:2489-2494`).
- A universe rebuilt for a past day uses today's lists (`src/engine/universe/builder.py:291-293, 422`). That is
  survivorship, and the repo states it is uncorrectable with stored data (`config/settings.yaml:258-259`).

## 7. What we are doing wrong (ranked by consequence)

1. **Live expectancy is negative, and nothing in the live path responds to the regime.**
   - Every strategy is long-only and always on. In an 11.8% equal-weight fall, the closed recs went 2 for 24, with a
     −4.5% median net and a −2.8% median excess.
   - The cards still ship stops that C1 measured as turning each edge negative; 19 of the 24 exited at their stops.
   - The planner's `no_trade_today` has never been set (60 of 60 plans), and no code reads it.
   - C2's reject-only regime gate for brk20 came out "supported" on 2026-10-07 and is still not applied.

   The open owner decisions (C1, C2, D14) dominate expectancy far more than any architectural question in this
   review.
2. **The LLM judge is a mandatory gate with no measured selection skill, and nobody measures it.**
   - It declines 68% of candidates, and the declines did as well as the approvals (§6.2).
   - Its confidence feeds a hard floor it has never been shown, set between its two favourite round numbers.
   - It costs about 30 s per candidate and fails one call in five.
   - When it is dark, origination stops (10-22/23). R1 keeps the platform safe with the LLM dead; it does not keep
     it working.
   - The prompt has accumulated rules that forbid specific decline grounds: rules 12–15, and the hi52 contract's
     "approach shape is NOT a decline ground". That is what a judge looks like when its declines need fencing in.
   - The ledger records every delivered entry as `"confirmed"`, so the filter's own record cannot be audited.
3. **The gate's economics check on brk20, rsi2 and trend prices a number the LLM chose.** The "expected edge" is the
   unweighted distance to the analyst's target (§6.3). A deterministic gate checking an LLM-supplied, probability-free
   number is weaker than R1 implies. The card then shows the owner that number as "edge Nx" with no caveat.
4. **brk20 and ins have no forward kill rule.** brk20 is the busiest live strategy and has gone 2 for 12; ins has gone
   0 for 5. The handoff's own rule is "give a forward test its kill rule from day one"; today only hi52 has one.
5. **Registered edges are probably overstated.**
   - Multiple-testing control is a step on one sweep's grid size, and the insider validation runs with N = 1.
   - hi52's evidence population applies today's NIFTY 500 membership backwards.
   - brk20's backtest touch fills are optimistic (memory `exit-geometry-study-2026-10-07`).
   - Nothing deflates for the dozens of families and variants the program has tried. The forward record so far is
     consistent with overstatement (small n, one regime).
6. **Evidence accrues more slowly than it could.**
   - Signal-level forward measurement exists only for hi52 and `cat`. The stop decision waits on 20 delivered recs.
   - brk20 fired 454 times for 29 delivered recs, and `shadow_trades`, built for exactly this, is empty.
   - A deterministic ledger of every scanner fire would give about 10× the sample and a clean control arm for the
     LLM filter.
7. **LLM work with no consumer.**
   - 517 heartbeat calls whose only output is a note to the same agent.
   - The planner's `focus` and `no_trade_today`, which only the prompt renders.
   - The nightly reviewer's lessons, attributions and suggestions, which nobody reads.
   - trend candidates forwarded to an analyst that is told they have no validated edge.

   Each one costs quota, latency and attention. None has ever been measured.
8. **The learning loop is a schema, not a loop.** Champion/challenger, River, drift detection and parameter promotion
   do not exist, and their tables are empty. That is fine for Phase 2, but the cheap loops that must come first do not
   exist either: scoring the LLM, and a shadow ledger.
9. **The owner channel produces no decision data.**
   - No rec has been taken since 08-27, and no skip reason has ever been recorded.
   - The two positions that were taken drew 68 exit recs between them, up to 10 a day, 21 of them after the holdings
     showed the position sold.
   - The take/skip decision, which the take-rate memory said to measure first, still cannot be measured.
10. **The live construct differs from the tested one for rsi2 and trend.** The live exits are not the backtested ones
    (§6.3). Research reads also have "latest, not as-of" paths (§6.5). Both are lower priority, because neither
    strategy has a registered edge.

The pattern behind items 2–8: the engineering is careful and well defended, but the evidence-generating loops are
thin. October's WORKLOG is almost entirely platform work: features, operational fixes and reviews. Meanwhile every
forward verdict (hi52, D14, `cat`) still reads INSUFFICIENT. The highest-return work now generates and judges evidence
faster: shadow ledger, scorecards, kill rules. More pipeline is not that work.

## 8. What to take from TradingAgents

**Adopt** (cheap, clearly useful):

- **A1. Score every LLM decision against outcomes, routinely.** This is TradingAgents' best idea: "one run gives one
  decision". Done properly it means:
  - within-strategy and within-day paired comparisons;
  - net of costs, with CIs;
  - covering the analyst's enter/decline and confidence calibration, the news analyst's materiality and sentiment by
    event type, and the planner's focus list.

  The scripts behind §6.2 are the start (§10). This turns item 2 from an opinion into a standing number.
- **A2. Absent is not empty, enforced by tests.** Audit each section of the analyst, planner and news prompts for
  blank renderings, and pin them the way `TA:tests/test_prompt_integrity.py` does. We have rule 14 and
  `sentiment_available`; we have not checked every section.
- **A3. Prompt provenance per call.** Store a hash of the system prompt and contract text in `agent_calls`, so that
  scorecards split by prompt version. The prompts have changed many times since July.
- **A4. Make as-of structural for research reads.** TradingAgents clamps the date at the tool, so no caller can get it
  wrong. Our equivalent is a store API for replay and backtest code that requires an as-of and enforces it on the
  §6.5 reads.

**Adapt** (only behind a measured before/after):

- **B1. Turn the analyst's mechanical decline reasons into deterministic rules.** Most sampled declines are mechanical:
  "the last price is already below the broken level", "a buy limit there would be marketable", "no daily series in
  context". Checks like these belong in the scanner or pre-screen, where they are free, instant and testable.
- **B2. A cheaper tier for text-only roles** (heartbeat notes, the nightly summary), if they are kept at all. This is
  TradingAgents' quick/deep split, and matches the owner's guidance that Sonnet and Opus go where output drives
  action.
- **B3. Same-ticker history on the card, for the owner.** TradingAgents feeds the last five decisions on a ticker to
  the model. Showing the owner the last few recs on the symbol, and how they ended, is deterministic and useful.

**Reject:**

- **C1. Multi-agent debate.** It takes 11–22 LLM calls per decision against our one. Neither its evidence nor ours shows
  any value, and a user measured it pushing ambiguous cases toward Overweight (#1196). Its "risk management" is
  rhetoric, where ours is limits.
- **C2. The LLM as the decider** (rating to trade). Our registered edges come from deterministic rules, and our data
  shows no selection skill in the LLM.
- **C3. One-outcome "lessons" injected into prompts.** That is learning from noise (§3.7).
- **C4. Action-forcing prompts** ("conflict alone is not a reason to Hold"). They raise turnover against a cost floor
  that has already killed five strategy families here.
- **C5. Social sentiment and yfinance.** Social sentiment is out of scope by owner decision; both feeds are
  unarchivable or unofficial.

## 9. Recommended owner decisions

In order of expected value. "Door" means reversibility.

| # | Decision | Why | Effort | Door |
|---|---|---|---|---|
| 1 | Apply C2's brk20 reject-only regime gate. Keep D14 running to look 1 (about mid-November). Pre-register a market-wide entry pause for all three strategies, tested on the stored backtest populations before use | live expectancy (§7.1); a reject-only gate can only cut exposure | a `limits.yaml` patch plus a pre-registration | two-way |
| 2 | Pre-register an analyst-value verdict: forwarded brk20/hi52/ins candidates; within-day paired T+20 net, approved − declined; if the mean is ≤ 0 after ≥ 30 two-arm days, the analyst becomes advisory for strategies with registered edges (writes the thesis, cannot veto) | §6.2, §7.2 | a script plus a plan entry; the mode switch is small | two-way |
| 3 | Remove `analyst_confidence_min`, or replace it with a floor calibrated on outcomes | §6.2: an uncalibrated hard reject | a protected `limits.yaml` patch | two-way |
| 4 | Price brk20, rsi2 and trend from a measured edge for the construct actually shipped, as hi52 and ins already are, never from the analyst's target. brk20's only measured edge is the no-stop T+20 construct, so this follows the C1/D14 decision. Until then, label the card's "edge Nx" as the target distance, not an edge | §7.3 | settings plus a gate path that already exists; the label is one line | two-way |
| 5 | Add forward kill rules for brk20 and ins, shaped like hi52's | §7.4 | one script each | two-way |
| 6 | Wire `shadow_trades` as a signal-level ledger of every scanner fire: no LLM, no slots, next-open entry, fixed horizon, costs | §7.6: about 10× the sample, and the control arm for #2 | moderate | two-way |
| 7 | Stop LLM calls without consumers: heartbeats (or a deterministic note), unread planner fields, unread nightly fields, trend forwarding | §7.7 | small | two-way |
| 8 | A research trial ledger, and DSR/PBO or a permutation bar for any new registered edge | §7.5 | moderate | two-way |
| 9 | Card and channel: one live exit rec per position until it is acted on or invalidated, and none once holdings show the position sold; one-tap skip reasons; same-ticker history (B3) | §7.9 | small | two-way |

Not recommended: porting the debate, social sentiment, LLM-written lessons, or any new LLM role before #2 has a
verdict.

## 10. Method, caveats, reproduction

- **Read-only throughout.** The engine was untouched. state.db was read with `mode=ro`. Forward returns came from the
  2026-10-26 nightly parquet export, never the live DuckDB.
- **Evidence scripts:** `data/reports/tradingagents_review_2026-10-27/`. Each takes the state.db path and, where needed,
  the export directory:
  - `evidence.py`, `evidence2.py`, `evidence3.py`: usage, recs, outcomes, funnel and gate;
  - `llm_filter_value.py`: approve vs decline;
  - `confidence_value.py`: the confidence split;
  - `gate_funnel.py`: real verdicts on entry proposals;
  - `exit_recs.py`: exit recs by position.

  Run them with `.venv\Scripts\python.exe -I <script> data/state.db data/backups/market-<stamp>`.
- **Caveats:**
  - One falling regime.
  - Small n per strategy.
  - Bootstrap CIs assume independent draws, and same-day draws are correlated, so they are too narrow. The within-day
    comparison is the robust read.
  - "First evaluation of each signal" is the unit.
  - Approved means the analyst said enter, whatever the gate did next.
  - The call counts and costs in §1.5 come from the paper and from user reports, not from a run of ours.
- **Not covered here:** the OMS and paper-broker internals, broker session and token handling, the ticker supervisor,
  and the dashboard. Each has its own review history in the WORKLOG, and none of them bears on the TradingAgents
  comparison. This check covers strategy, LLM, evaluation and data.
- **Agent maps:** three read-only mapping passes covered the LLM layer, the decision pipeline, and learning plus data.
  Their key pointers were spot-checked against the code before use: `pipeline.py:2396`, `gate.py:619-627`,
  `gate.py:1032-1081`, `cost_model.py:348-360`, `validate.py:105-117`, `rec_outcomes.py:1-6`, and the absence of any
  `no_trade_today` reader.
