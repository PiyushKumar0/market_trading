# PROMPT — Produce `IMPLEMENTATION_PLAN.md` for a Self-Learning AI Trading Platform (Indian Equities, Zerodha)

> Copy everything below this line into a fresh Claude instance (Claude Code preferred, opened in the project
> directory). The deliverable of that session is a single file: `IMPLEMENTATION_PLAN.md`.

---

## Your role

You are a senior systems architect with deep experience in (a) retail algorithmic trading systems on Indian
markets, (b) LLM-agent architectures on the Claude Agent SDK, and (c) operating unattended software on a
single Windows machine. You are designing for a technically capable owner who will have AI agents execute
the plan — so the plan must be precise enough that an implementing agent makes no significant architectural
decisions on its own.

## Mission

Write `IMPLEMENTATION_PLAN.md` — a complete, phased implementation plan for a personal, self-learning,
AI-driven trading platform for Indian equities (NSE) on Zerodha Kite Connect. **Produce the plan document
only — no application code.** Code-level content is limited to: module/file layout, interface signatures,
data schemas, and config formats.

## How to work

- Produce the document in this session without waiting on the owner. Do not stop to ask clarifying
  questions — route genuine unknowns to the Open Questions section (§14), each with the provisional
  default the plan assumes.
- Write `IMPLEMENTATION_PLAN.md` incrementally, section by section, with file edits — do not attempt the
  whole document in one response. Expect roughly 10,000–20,000 words; that is calibration, not a cap or a
  target to pad toward.
- If you have web access: spend at most ~15 minutes re-verifying the most volatile load-bearing facts, in
  this priority order: D3 (Agent SDK credit), D1 (SDK versions), A7 (square-off time), A9 (static-IP
  rules), A2 (rate limits). If fresh evidence contradicts the fact sheet, prefer the fresh evidence, note
  the discrepancy inline next to the fact-ID citation, and adjust dependent decisions. Without web access,
  carry the sheet forward as-is.

## Non-negotiable owner decisions — IDs O1–O10 (do NOT relitigate these)

These were made knowingly, with trade-offs understood. Design what was asked; where a decision carries
regulatory or safety risk, flag it plainly in the plan's risk register — but do not redesign around it.

- **O1. Capital: ₹20,000 hard cap.** The platform must never deploy more (intraday leverage on top of it
  is allowed — see trading scope).
- **O2. Three operating modes:**
  - **RECOMMEND** — the platform is a recommendation engine. It produces fully-specified trade
    recommendations (instrument, direction, entry, stop-loss, target, size, product type, thesis,
    confidence) delivered via Telegram + dashboard. The human executes manually — possibly on a different
    platform. This is the first mode to go live. In this mode the platform places **zero orders of any
    kind via the API** (see B7 — even human-approved API orders are algo orders).
  - **AUTO** — the platform places, manages, and exits orders itself via the Kite Connect API. Gated
    behind paper-trading acceptance criteria (you define them).
  - **OFF** — data collection and learning continue; no recommendations, no *position-opening* orders.
    Risk-reducing orders for already-open platform-tracked positions remain permitted in every mode (R3).
- **O3. Trading styles: intraday (MIS, with leverage), swing (CNC, days–weeks), position (CNC,
  weeks–months).** All three must be architecturally supported from day one and live in RECOMMEND mode
  from its go-live. AUTO mode may — and should — ramp styles and leverage stepwise through the gated
  roadmap; that ramp is sequencing, not relitigation of this decision.
- **O4. Continuous online self-learning is in scope** — the system improves its own trading quality from
  its trade outcomes, market analysis, and sentiment. The owner explicitly accepted the risks of online
  learning influencing live AUTO trading. The safety-envelope requirements (R4) still apply.
- **O5. Manual override always available:** the owner can veto/cancel any pending action, close any
  position, force mode changes, and trade manually on the same account at any time. The platform must
  reconcile gracefully with positions it didn't open — and with broker/RMS actions on positions it did.
- **O6. AI runtime: Claude Agent SDK authenticated via the owner's Claude Max subscription** (OAuth token
  via `claude setup-token`), not a pay-as-you-go API key, as the default. Design the budget governor so an
  API key can be swapped in later via config.
- **O7. Deployment: owner's local Windows 11 PC first** (on during market hours). The plan must include a
  later migration path to an always-on VPS, but the first deployment phase targets the local machine.
- **O8. Alerts/control surface: Telegram bot (primary, two-way) + web dashboard (TypeScript/React).**
- **O9. Engine language: Python.**
- **O10. Capital preservation is the paramount objective** — ahead of returns. A best-effort drawdown
  floor, not a guarantee, but every design decision must respect it.

## Owner's current state (Phase 0 starting point)

- Funded Zerodha account; Kite Connect developer app already created; ₹500/month Connect (data) plan active.
- Claude Max subscription active (tier — 5x vs 20x — to be confirmed at implementation time; design for the
  $100/month Agent SDK credit of Max 5x as the conservative default, parameterized in config). The Max
  subscription is a pre-existing sunk cost the owner holds independently of this project: show it in the
  cost table as a footnoted sunk line, not an attributable monthly cost — but the Agent SDK credit it
  includes is the binding LLM budget.
- Windows 11 Pro PC, available during market hours (09:15–15:30 IST, Mon–Fri, exchange trading days).
- No existing codebase. Empty repository.

---

## VERIFIED FACT SHEET (web-verified 2026-06-11)

Treat these as ground truth for the plan — they override your training data; several changed recently and
stale knowledge is wrong. Rows marked `[likely]` were verified from strong secondary sources;
`[unverified]` rows must become explicit Phase-0/1 verification tasks. Re-verification policy is in
"How to work" above. Cite fact IDs (and O/R IDs) when a design decision depends on them.

### A. Zerodha Kite Connect (current)

| # | Fact |
|---|------|
| A1 | Pricing: order/account APIs are **free** (Personal plan); live WebSocket + historical candles cost **₹500/month** (Connect plan). The old ₹2,000 historical add-on was abolished Feb 2025 `[likely]` — historical data is included. |
| A2 | REST rate limits: quote **1 req/s**, historical **3 req/s**, orders **10 req/s**; plus 400 orders/min, **5,000 orders/day**, max **25 modifications per order**. These caps count **all order-API requests — place + modify + cancel — per client account across all apps**, not just new placements. |
| A3 | Live quotes at scale must come from the **KiteTicker WebSocket** (the 1 req/s REST quote limit is unusable for polling): 3,000 instruments/connection, 3 connections/API key. Order updates are pushed on the same socket (`type: "order"`) — Zerodha recommends this over HTTP postbacks for individuals. |
| A4 | `pykiteconnect` is at **v5.2.0** (Apr 2026). KiteTicker is **Twisted-based, not asyncio** (pins `autobahn[twisted]==19.11.2`); the Twisted reactor cannot restart in-process. Consequences: run the ticker as a **supervised subprocess** (killable/respawnable), never in the main asyncio process; and the ancient dependency pins have an unverified Python-version ceiling — the plan must pin one tested interpreter (e.g., 3.11/3.12; River needs ≥3.11, E3) and include a Phase-0 install smoke test across pykiteconnect + claude-agent-sdk + River + DuckDB. |
| A5 | The access token **expires at 06:00 IST daily** (regulatory). A fresh interactive login (request-token → checksum exchange) is needed every trading day. Zerodha's stated policy: manual login once a day is mandatory; **scripted TOTP auto-login is widely practiced but violates Zerodha policy** (documented risk: account/API suspension). |
| A6 | Order varieties: `regular`, `amo`, `co`, `iceberg` (2–50 legs), `auction`. Products: `CNC`, `MIS`, `NRML`, `MTF`. **GTT** (single + two-leg OCO, 1-year validity) is the bracket-order replacement; BO is discontinued. |
| A7 | Equity MIS auto-square-off is **15:25 IST** (changed from 15:20 on 26 Dec 2025), ₹50+GST per auto-squared position. The platform must self-square well before (e.g., 15:10–15:15) — never rely on broker square-off. |
| A8 | Equity MIS intraday leverage: **max 5× (20% margin)**, per-stock — many stocks get less or none. Stocks in **T2T/GSM/ASM/unsolicited-SMS surveillance lists get NO intraday product** (CNC only). These lists change overnight → refresh eligible-stock/leverage data daily before open. Zerodha RMS can also **force-close platform-opened positions intraday** (MTM margin erosion at leverage, surveillance migration) — broker-initiated exits are a first-class OMS event, not an anomaly. |
| A9 | Since 1 Apr 2026 (SEBI framework): API **order** endpoints require a **whitelisted static IP** (1 primary + 1 optional secondary, configured on developers.kite.trade); orders from other IPs are rejected. **Data endpoints are exempt.** Market & SL-M orders must carry **non-zero market protection** (use −1 for auto); order slicing capped at 10. |
| A10 | NSE tick sizes are **price-banded** (₹0.01 below ₹250, up to ₹5.00 above ₹20,000) — not flat ₹0.05. Always round prices to the per-instrument `tick_size` from the daily instruments dump. |
| A11 | `[unverified]` Kite daily candles are corporate-action adjusted; **intraday minute candles are NOT retroactively adjusted**. Confirm empirically (compare across a known split) before building the minute-bar stitching job; the plan should schedule this as an explicit Phase-0/1 task. Historical backfill is throttled by the 3 req/s limit (A2). |
| A12 | **GTT is a one-shot trigger, not a guaranteed stop**: when the trigger fires it places a regular **LIMIT order once** — a gap through the trigger can leave the limit unfilled with the trigger consumed; the triggered order can also be **rejected** (margin, band, surveillance move). GTTs are **not adjusted for corporate actions** (an ex-date drop will spuriously fire them). Trigger ≠ fill; no re-arm. |
| A13 | KiteTicker delivers **throttled snapshots (~1/sec per instrument), not raw ticks**, carrying **cumulative day volume** — bar volume must be built from cumulative-volume deltas, and self-built bars will diverge slightly from Kite's official historical candles (reconcile daily, alert on drift). |
| A14 | **Pre-open session 09:00–09:15** (order entry to 09:08, then auction matching): pre-open ticks are indicative auction prices and must be excluded from bar construction; the opening price comes from the auction; AMO orders release into it. |

### B. SEBI retail algo framework (live since 1 Apr 2026)

| # | Fact |
|---|------|
| B1 | SEBI circular of Feb 2025 ("Safer participation of retail investors in algorithmic trading") became **fully mandatory 1 Apr 2026**; it is in force now. |
| B2 | Threshold: **10 orders per second** (per client, per segment, per exchange). **At/below 10 OPS: no exchange registration needed**; API orders are auto-tagged with a generic exchange algo ID. Above 10 OPS: the strategy must be registered with each exchange via the broker (strategy writeup + RMS writeup + auditor certificate `[likely]`; weeks of lead time; fees passed through). |
| B3 | For **registered** algos, any logic modification requires fresh exchange approval — continuous self-learning is incompatible with registration. For **unregistered (≤10 OPS)** algos there is no registration and **no re-approval mechanism** — parameter changes carry no filing requirement. **Design consequence: the platform must be architected to stay ≤10 OPS at all times (self-throttling well below the cap, counting place+modify+cancel, A2), which keeps the self-learning loop out of the registration regime.** |
| B4 | A pure **recommendation engine with manual human order entry is entirely outside the algo framework** (controls attach to API order flow; manual Kite web/mobile trading is unaffected). RECOMMEND mode therefore has no static-IP or algo-ID implications — **but only if it truly places no API orders** (B7). |
| B5 | The registered-algo "family" scope is self, spouse, dependent children, dependent parents. **Distributing signals or execution to anyone else makes the owner an unregistered Research Analyst / algo provider — a SEBI violation.** The plan must state this boundary; the platform is single-user. |
| B6 | Non-compliance consequences at retail scale are primarily access-level: order rejections, HTTP 429, API/algo-ID suspension, exchange kill-switch. The broker is the liable principal. |
| B7 | **ALL Kite Connect API orders are algo-tagged and IP-checked — including GTT placement and "semi-manual" flows where a human approves each order before API submission.** The B4 exemption covers only fully manual entry on Kite web/mobile. Consequence: RECOMMEND mode must place zero API orders of any kind (protective orders are part of the human's manual execution checklist); any one-tap-execute Telegram button converts the platform into an algo system requiring the static IP (A9). Whether GTT endpoints specifically are IP-checked is unconfirmed — verify before any pre-AUTO GTT use. |

### C. Trade economics at ₹20,000 (Zerodha, verified rates)

| # | Fact |
|---|------|
| C1 | Brokerage: delivery **₹0**; intraday **0.03% or ₹20/order, whichever lower** (₹20 binds only above ₹66,667/order — a ₹20,000 order pays ₹6, not ₹20). |
| C2 | Component rates (verified 2026-06-11 — the cost-model module must use these, parameterized in config): STT **0.1% both sides** (delivery) / **0.025% sell-side** (intraday); NSE transaction charge **0.00307% per side** (⚠ stale-knowledge trap: training data says 0.00325% or 0.00297% — both wrong now; effective date of the 0.00307% revision unconfirmed); SEBI fee ₹10/crore both sides; stamp duty buy-side **0.015%** (delivery) / **0.003%** (intraday); GST **18%** on brokerage + transaction + SEBI charges; DP charge **₹15.34 per scrip per sell day** (delivery). Re-scrape zerodha.com/charges before each release — broker fees now change mid-year. |
| C3 | Worked round-trips: **intraday ₹20,000 notional ≈ ₹21 (breakeven 0.106%)**; **delivery ₹20,000 ≈ ₹60 (breakeven 0.30%** — dominated by ₹40 STT + ₹15.34 DP); **5× MIS ₹1,00,000 notional ≈ ₹83 (breakeven 0.083% of notional = 0.41% of capital per round trip)**. Friction compounds: **10 leveraged round trips/month ≈ 4.1% of capital in costs alone**. The strategy layer must enforce a minimum expected-edge threshold per trade that clears friction with margin; the cost model is a first-class module used identically in backtests, recommendations, and live decisions. |
| C4 | The ₹15.34/scrip DP sell charge makes **multi-scrip diversification disproportionately expensive for small delivery positions** — position sizing must account for it (avoid ₹2,000 positions across 10 scrips). |
| C5 | Settlement: **T+1** default. Optional T+0 exists (top-500 stocks, ~13:30 cutoff `[likely]`) but has negligible adoption — don't design around same-day cash recycling for delivery. |
| C6 | Peak-margin regime: 4 random intraday snapshots; shortfall penalties 0.5–1% escalating to 5% + GST, surfacing ~T+6. The risk gate must check available margin (margins API) before every order. |
| C7 | Price bands: non-F&O stocks have **hard 2/5/10/20% daily circuits**; F&O-list stocks have no static band but a **10% dynamic band flexed in 5% steps**. Index circuit breakers (10/15/20%) halt the whole market, with reopening auctions. **A band-locked stock cannot be exited: the order book is one-sided, so stop-losses and square-offs become unexecutable** — see R3. This is why MIS entries should prefer F&O-list (dynamic-band) stocks. |
| C8 | **Short-delivery tail risk (worst single scenario for this account):** an MIS short that cannot be covered (connectivity loss, broker RMS failure, or the stock locking at **upper** circuit — buy-back literally impossible) becomes a **short delivery settled through the exchange auction**, with penalties that can reach ~20% over close. On 5×-leveraged ₹1,00,000 notional that single event can exceed the entire ₹20,000 capital. The plan must define an explicit shorting policy (see trading scope). |
| C9 | **Base-rate honesty (must appear in the plan's executive summary):** SEBI's own study: **71% of retail intraday equity traders lose money** (80% for >500 trades/yr); 91% of F&O traders lost in FY25; academic evidence puts persistently profitable day traders below 1%. Fixed costs (₹500/mo data + ~₹1,500/yr static IP + compute) are **3–5% of this account's capital per month** (Max subscription excluded as sunk — see Owner's current state). Frame Phase 1 as a learning/proof phase whose success metric is process quality and capital preservation, NOT profit. |

### D. Claude Agent SDK on a Max subscription (current — major change 4 days from fact-check date)

| # | Fact |
|---|------|
| D1 | The SDK is the **Claude Agent SDK**: PyPI `claude-agent-sdk` (v0.2.95, Python ≥3.10); npm `@anthropic-ai/claude-agent-sdk` (v0.3.170). Provides the full agent loop: tools, hooks, subagents, MCP, sessions, structured outputs. The Python SDK **spawns the Claude Code CLI (Node) as a subprocess** — Node + the CLI are runtime dependencies. |
| D2 | Max-subscription auth for **your own** agents is officially supported: `claude setup-token` mints a 1-year OAuth token → `CLAUDE_CODE_OAUTH_TOKEN`. (Offering your subscription to third parties is banned — irrelevant here, single-user.) **Subscription OAuth has no raw Messages-API path: every call goes through the Agent SDK / Claude Code harness, whose system-prompt and tool-definition overhead must be included in per-call cost modeling.** Beware: a stray `ANTHROPIC_API_KEY` env var silently outranks the OAuth token. |
| D3 | **From 15 June 2026, Agent SDK usage on subscription plans stops sharing interactive limits and draws from a monthly "Agent SDK credit": $100/mo on Max 5x, $200/mo on Max 20x, metered at standard API token rates.** When exhausted, SDK calls stop (or overflow to pay-as-you-go if enabled). An unmanaged agent can go dark mid-month. |
| D4 | Model pricing (per MTok in/out): Haiku 4.5 $1/$5; Sonnet 4.6 $3/$15; Opus 4.8 $5/$25; Fable 5 $10/$50. **The SDK defaults to Opus on Max plans — an agent that doesn't set `model` burns credit at 5× Haiku rates.** Naive budget anchor: a fixed 5-minute loop over the trading day ≈ 1,575 runs/month ≈ $0.06/run *if the loop got the entire $100 credit — which it must not* (nightly/weekly research agents need their share; an agentic Opus research run realistically costs ~$1). The real per-agent budget comes from the §11 cost worksheet. |
| D5 | **Required invocation shape — event-triggered, not polling:** deterministic scanners (cheap, rule-based, Tier-2-adjacent) run continuously and invoke Tier-1 LLM analysis only on (a) signal candidates passing a pre-screen, (b) position events (fill, stop-approach, news hit on a held instrument), (c) a sparse heartbeat (~every 15–30 min) for regime context. Expect ~10–30 intraday LLM calls/day, not 75. **Intraday Tier-1 calls are SINGLE-SHOT: no tools (disabled in SDK options), minimal custom system prompt, context fully pre-assembled by deterministic Python, structured-output schema, one response.** Agentic multi-turn tool use is permitted only for nightly/weekly research agents, with hard per-run token/turn caps. |
| D6 | **Token-budget governor:** there is **no credit-balance API** — the governor meters locally by reading per-call usage (input/output/cache-read/cache-write tokens) from SDK result messages, pricing at standard rates, persisting a running ledger (SQLite), reconciling monthly. Degrade ladder must be **cache-aware** (D8) — prefer shrinking context / event-only invocation over lengthening intervals or switching models — and each degrade tier maps to a **mode capability**: below a defined analysis-quality tier, AUTO stops opening new positions (manage-only) and notifies the owner; only full-quality tiers may originate AUTO entries. If pay-as-you-go overflow is enabled, a hard monthly spend cap applies. Credit-exhausted SDK errors resolve to the same safe state as LLM-unavailable. |
| D7 | **Any Tier-1 call failure — schema-invalid after retries, timeout, CLI subprocess death, rate limit/overload, credit exhaustion — resolves to no-proposal/no-action plus an alert.** Exits, stops, and square-offs are never contingent on a successful LLM response (R1). Never parse trade decisions from free prose. |
| D8 | **Prompt-caching reality:** the Agent SDK does not expose cache-control breakpoints or TTL selection — caching is harness-managed. The real levers: byte-stable system prompt (no interpolated timestamps/mode flags), volatile market data placed last, model consistency, and call cadence within the ~5-minute cache TTL. A cache-miss run costs ~2.5–3× a cache-hit run; switching models invalidates the cache; Haiku 4.5's minimum cacheable prefix is 4,096 tokens (a leaner prompt never caches). Cost worksheets must price cache-hit and cache-miss cases separately. |
| D9 | Model/effort tiering: Haiku 4.5 or Sonnet 4.6 for intraday single-shot calls; Opus 4.8 for weekly deep review; nightly research on Sonnet unless the §11 worksheet shows Opus fits. **Verify every cost-control knob against the actual SDK surface (model, thinking config, max turns/output) — the `effort` parameter is not available on Haiku 4.5.** Make tiering config-driven. |
| D10 | Session strategy: **fresh query per invocation** for intraday agents (stable system prompt + pre-assembled context), sessions only where multi-turn state genuinely helps (nightly research). The SDK loads CLAUDE.md/filesystem settings **only when explicitly configured** (`setting_sources`) — a trap when running as a service from a different working directory. Specify what lives in the SDK system prompt vs per-call context. |
| D11 | Windows-service specifics for the SDK: Node + a **pinned** Claude Code CLI version installed for the service account; `CLAUDE_CODE_OAUTH_TOKEN` injected via the service environment (NSSM `AppEnvironmentExtra` — services don't inherit user env vars); `claude setup-token` run as the same user the service runs as; CLI auto-update disabled/pinned; a startup self-test performing one cheap SDK call before market open. |

### E. Recommended stack (verified current; justify in the plan or substitute with reasoning)

| # | Fact |
|---|------|
| E1 | Backtesting: **vectorbt v1.0** (Apache-2.0 + Commons Clause) for fast vectorized parameter sweeps; **NautilusTrader** (actively developed, event-driven, Rust core) for realistic event-driven intraday simulation — note it has **no Kite/NSE adapter**; a custom data adapter is needed. `backtrader` is dead (last release 2023) — do not use. `backtesting.py` (AGPL) acceptable for simple single-asset studies. |
| E2 | Overfitting control: walk-forward as baseline plus **Combinatorial Purged Cross-Validation** (purge + embargo) — open-source implementation in **skfolio** (BSD-3). `mlfinlab` is no longer open source — do not plan around it. |
| E3 | Online learning: **River** (BSD-3, **Python ≥3.11**) for incremental models + drift detection. Champion/challenger with shadow-mode evaluation is the accepted safe-deployment pattern (no canonical OSS framework — design it at application level). |
| E4 | Storage on one Windows box: **DuckDB (+ Parquet) for bars/analytics, SQLite for transactional state** (orders, positions, decisions, config, budget ledger, kill-state). TimescaleDB is overkill — do not introduce a server DB. |
| E5 | Supplementary free data: NSE **UDiFF bhavcopy** (new URL format since Jul 2024 — old URLs 404), India VIX from NSE, NSE earnings/board-meeting calendar and ex-date feeds, bulk/block-deal lists (EOD), `jugaad-data` (maintained) — but NSE blocks datacenter IPs and changes anti-bot measures `[likely]`; treat scraping as best-effort enrichment, with Kite as the load-bearing source. yfinance: backup EOD only — never in the live path. |
| E6 | Sentiment sources: Economic Times RSS (working), Moneycontrol RSS (bot-protected; polite polling), **GDELT DOC 2.0** (free, 15-min updates; **~3-month rolling window** — deep sentiment backfill needs BigQuery/file dumps; score headlines with your own LLM, not GDELT's dictionary tone). **X/Twitter API is pay-per-use only since Feb 2026 — off the table.** |
| E7 | Windows ops: run as a service via **NSSM**; prevent sleep with `SetThreadExecutionState`; Windows 11 Modern Standby can throttle background network `[likely]` — document the power-plan setup; set Windows Update active hours; plan crash-recovery (the bot WILL be killed by an update eventually). |

---

## Required architecture properties — IDs R1–R10 (hard requirements)

The plan must realize all of these and explain how.

- **R1. Three-tier decision separation (the core safety property):**
  - **Tier 1 — Intelligence (Claude agents, non-deterministic):** market analysis, signal generation,
    sentiment scoring, trade theses, post-trade review, strategy research. *Proposes only.*
  - **Tier 2 — Risk gate (pure deterministic Python, no LLM):** validates **every Tier-1-originated
    action**, not just entries. The plan must enumerate all LLM-originated action types as
    schema-validated objects — `enter`, `exit`, `modify-stop`, `modify-target`, `cancel` — each with
    explicit monotone rules: **stops may only tighten autonomously; protective orders are never cancelled
    without a validated replacement; widening a stop or extending a target requires owner approval.** For
    position-opening/enlarging actions the gate may only reject or shrink — never enlarge or initiate.
    After any shrink, the gate re-runs the cost/breakeven model (C2/C3) and rejects if the shrunk size is
    below minimum viable. **Risk-reducing exits (stop execution, scheduled square-offs, kill-switch
    flatten) are deterministic Tier-2/3 actions requiring no proposal — always permitted, in every mode.**
    Every input the gate consumes (sector map, surveillance lists, MIS leverage, tick sizes, margins, LTP,
    universe membership) comes exclusively from deterministic broker/exchange sources — never from LLM
    output. **RECOMMEND-mode recommendations pass the same gate before delivery** (same limit table, same
    sizing, same cost check); the gate verdict and headroom ship inside the recommendation payload.
  - **Tier 3 — Execution/OMS (deterministic):** order placement, a state machine per order (partial
    fills, rejections, modifications ≤25, retries, **broker-initiated exits as a first-class terminal
    state**, A8), position tracking, reconciliation against broker state, square-off scheduling, GTT
    lifecycle management (R3).
  - The LLM tier must be *removable*: if it dies or the budget governor halts it (D6/D7), Tiers 2–3 keep
    managing open positions safely.
- **R2. Deterministic risk limits (Tier 2 config; the plan proposes concrete starting values):**
  per-trade max risk (% of capital) — with **overnight positions sized on gap-adjusted loss** (assume
  2–3× stop distance or the stock's daily band, whichever is worse); max daily loss (halt for the day —
  define halt semantics: freeze vs flatten, realized + open MTM basis); max weekly drawdown (forced
  AUTO→RECOMMEND); **cumulative drawdown floor** (equity below X% of ₹20,000 → OFF pending owner review);
  consecutive-loss cool-down and max new trades/day (churn protection, C3); max open positions; max
  leverage; per-stock and per-sector exposure caps; no-trade windows (first/last N minutes, results days
  via the earnings calendar, expiry-day index distortions); **stale-data guard** (max tick age before
  entries halt + feed-heartbeat alert — the Twisted subprocess can stall silently, A4); entry-price sanity
  band vs live LTP; **reject entries within N% of the instrument's circuit band** (C7); max holding period
  per style (no zombie CNC positions locking the capital); order-rate self-throttle far below 10 OPS and
  5,000/day counting place+modify+cancel (A2/B3); margin check before every order (C6). **The
  online-learning system has no write access to these limits, to the kill-switch, or to its own envelope
  bounds** — see R4.
- **R3. Broker-resident protection (capital preservation must survive platform death):** every MIS fill
  is immediately followed by a **resting broker-side SL-M order** (non-zero market protection, A9) that
  lives at the exchange independent of the platform; software may only tighten/trail it (within the
  25-modification cap, A2). Every CNC position gets a GTT OCO immediately after fill confirmation
  **(AUTO/paper modes only — B7; in RECOMMEND the protective orders are part of the human's manual
  checklist)**, with **GTT lifecycle monitoring**: detect trigger-fired-but-order-rejected/unfilled
  (A12), auto re-arm or escalate to immediate market exit + owner alert; verify GTTs exist and are active
  at every reconciliation pass; adjust/recreate GTTs before ex-dates on held positions (A12). Define
  behavior for **band-locked exits** (C7): detect a locked instrument (LTP pinned at band, one-sided
  depth), queue at band price, alert owner, and for MIS longs evaluate MIS→CNC conversion as a fallback
  (noting margin usually won't permit it at 5×). **Mode invariant: risk-reducing orders for
  platform-tracked positions are permitted in every mode, including OFF**; risk-triggered downgrades
  enter a defined close-only state; re-arming AUTO after a risk-triggered downgrade requires explicit
  owner action, never timer expiry. **Required chaos invariant: an open leveraged position remains
  stop-protected with the platform process dead and the session token expired.**
- **R4. Self-learning with a safety envelope:** continuous learning is in scope (O4), but every change
  passes: white-box parameter envelope (learnable parameters and allowed ranges explicitly enumerated in
  a table; nothing outside it is learnable) → validation on held-out/walk-forward data (E2) → shadow mode
  (challenger on live data, paper-only, alongside champion) → promotion gate (you define metrics) →
  auto-rollback triggers (you define). **The envelope bounds table is Tier-2-owned config living in the
  protected store, not learning-system config.** Single-user Windows cannot give real process isolation,
  so specify a concrete mechanism: either a separate Windows account with ACLs denying the learner write
  access to the limits+bounds store, or the gate verifying a hash/signature on the limits+bounds file at
  every load, changes possible only via an owner-confirmed flow. The learning ledger records every
  trade's thesis, features, outcome, and attribution (tagging ex-date effects so dividend drops aren't
  attributed to strategy, A12) so nightly/weekly review has structured material. Strategy *logic* changes
  (new code) always require explicit owner approval; only envelope parameters move autonomously.
- **R5. Mode semantics & manual override:** OFF/RECOMMEND/AUTO transitions, who can trigger them (owner
  via Telegram/dashboard; risk gate can force downgrades per R2/R3), what happens to open positions on
  each transition (constrained by the R3 mode invariant), and how externally-originated changes — owner
  manual trades AND broker/RMS force-closures (A8) — are detected (order updates on the WebSocket +
  periodic reconciliation) and handled (default: track but don't manage, alert; configurable).
- **R6. Daily session lifecycle & time discipline:** the 06:00 token expiry (A5) means a daily login.
  Design the morning workflow with the **device problem solved explicitly**: the owner will likely tap
  the Telegram login link on their phone, so the Kite app's registered redirect URL must resolve
  somewhere useful — e.g., a LAN-reachable endpoint on the dashboard server — with a manual fallback
  (paste the `request_token` into Telegram/dashboard); the redirect URL must match the Kite app
  registration. Degraded behavior if the owner is late: no new entries; open positions remain protected
  by broker-resident orders (R3). Document the scripted-TOTP alternative as an explicitly owner-accepted
  policy violation (A5) — behind a config flag, default off. **Time discipline:** NTP/clock-skew check at
  startup and periodically (refuse new entries beyond N seconds skew); all scheduling timezone-aware IST
  regardless of system locale; an **NSE trading-calendar module** (holidays, muhurat evening session,
  special/shortened sessions, annual refresh) consumed by the scheduler, session lifecycle, square-off
  timing, and budget governor — without it, roughly 15 days/year misbehave and the LLM loop burns credit
  on closed markets.
- **R7. Static IP dependency (AUTO mode only):** RECOMMEND mode needs none (B4/B7). The AUTO phase
  requires a static IP whitelisted with Zerodha (A9) — compare an ISP static-IP add-on (~₹1,500/yr) vs
  routing orders through a small static-IP VPS; data can keep flowing from anywhere.
- **R8. Observability:** every decision (proposal, gate verdict + reason, order event, learning update,
  budget state) is persisted and queryable; the dashboard shows live positions, P&L, risk-limit headroom,
  agent budget spend, decision log, learning status; Telegram delivers recommendations, fills, limit
  breaches, kill-switch events, budget warnings, and daily/weekly summaries.
- **R9. Paper-trading parity with a conservative fill model:** paper mode uses the same code path as live
  (same proposals, same gate, same OMS state machine). The fill model must be specified conservatively:
  limit fills require trade-through (not touch), stop fills are gap-aware (open-below-stop fills at the
  open, not the stop), spread + latency slippage calibrated from recorded tick data, partial fills, and
  injected broker rejections/429s. AUTO is unlocked only by meeting paper acceptance criteria you define
  (duration, trade count, max drawdown, process-error count) — and once live, **live-vs-paper fill
  deviation is tracked and fed back into the fill model** (Phase-gate requirement).
- **R10. Control-plane security:** Telegram commands accepted only from the owner's chat ID, with a
  two-step confirmation for destructive/mode-up commands (kill-reset, →AUTO); the dashboard binds to
  localhost or requires token auth; secrets (Kite API secret, daily access token, Claude OAuth token,
  Telegram bot token) stored via Windows DPAPI/Credential Manager or encrypted store — never plaintext in
  repo or config. **Kill-switch:** Tier-2-owned, but with (1) a documented dead-platform kill procedure
  using broker-native mechanisms (Kite's own kill switch on console, Call & Trade desk — pre-staged in
  the runbook), (2) kill/halt state persisted in SQLite and checked before any order on startup — sticky
  across crash/reboot/NSSM restart (an auto-restart must never resurrect trading after a kill), (3)
  owner-manual reset only, via an authenticated channel.

## Trading scope details

- **Venue: NSE-only for trading** (BSE data optional for cross-checks) — liquidity and a single
  instrument-token namespace; dual-venue routing is wasted design surface at this capital.
- **Universe:** define a liquid-stock universe selection procedure (e.g., NIFTY 100/200 ∩ MIS-eligible ∩
  not in surveillance lists, minimum median traded value, **F&O-list membership preferred for MIS
  candidates** — dynamic bands, C7), refreshed daily pre-open. At ₹20k, prioritize liquidity and tick
  economics (A10, C4).
- **Shorting policy (the plan must decide one explicitly, citing C8):** recommended default — MIS shorts
  disabled at AUTO go-live; if/when enabled by phase gate, F&O-list stocks only (no hard upper circuits),
  with earlier square-off cutoffs and tighter size caps than longs. RECOMMEND mode may flag short ideas
  separately marked as higher-tail-risk.
- **Intraday (MIS):** leverage up to the per-stock MIS cap (≤5×), but the platform applies its own
  tighter cap initially — **the leverage envelope is [1×, platform cap], platform cap initially 3×,
  raisable only by owner approval at a phase gate — the cap itself is never learnable** (R4). Entries
  stop by a cutoff (e.g., 14:30); self-square-off by ~15:10 (A7); every fill instantly protected by a
  broker-resident SL-M (R3).
- **Swing/position (CNC):** GTT OCO protection per R3 (AUTO/paper); gap-adjusted overnight sizing (R2);
  T+1 awareness for capital recycling (C5); DP-charge-aware sizing (C4); ex-date handling (A12, R3);
  decide whether nightly-staged entries use AMO (released into the pre-open auction, A14) or morning
  confirmation.
- **Recommendation content (RECOMMEND mode):** executable by a human on any platform: instrument, side,
  entry zone, stop, target(s), size in shares and ₹, product type, time validity, thesis (3–5 lines),
  confidence, the gate verdict + headroom (R1), the trade's specific cost/breakeven math (C2/C3), and the
  manual protective-order checklist (B7/R3).

## What `IMPLEMENTATION_PLAN.md` must contain (output spec)

1. **Executive summary** — incl. the honest economics framing (C9) and what "success" means per phase.
2. **System architecture** — component diagram (Mermaid), process model on Windows (main engine, ticker
   subprocess, dashboard server, Telegram bot, scheduler), data flow, the three-tier boundary (R1), and
   the trust boundaries (R4 isolation mechanism, R10 control plane).
3. **Module specifications** — for each module: responsibility, public interface (signatures), key data
   types, dependencies. Include the full action-object schemas (`enter`/`exit`/`modify-stop`/
   `modify-target`/`cancel` per R1), `GateVerdict`, and the order/position state machine including
   broker-initiated terminal states.
4. **Data architecture** — DuckDB/Parquet/SQLite schemas; ingestion jobs: ticks → bars (cumulative-volume
   deltas, pre-open exclusion, daily reconciliation vs official candles — A13/A14), historical backfill
   within 3 req/s, UDiFF bhavcopy, instruments dump (tick sizes, A10), surveillance lists (A8),
   **trading calendar (R6)**, **earnings/board-meeting calendar and ex-date feed** (R2/R3),
   **bulk/block-deal lists** (flag affected instrument-days in the feature store — block prints fake
   volume breakouts), corporate actions (A11 verification task), news/sentiment (E6 — treat scraped news
   as untrusted input to Tier 1; the gate and churn limits are the containment); retention policy.
5. **Agent design (Tier 1)** — each agent's job, trigger (event-triggered per D5 — not polling), model +
   verified knobs (D9), single-shot vs agentic shape (D5), session strategy (D10), input context assembly,
   output schema, cache strategy under D8's constraints, and failure semantics (D7). Include the intraday
   event-triggered analyst, pre-open planner (grounded in A14 mechanics), nightly post-trade review,
   weekly strategy research. Specify the token-budget governor (D6) with concrete numbers for a $100/mo
   credit and the degrade-tier → mode-capability mapping.
6. **Strategy & learning design** — initial strategy set (rule-based baselines the LLM layer enhances, so
   there are measurable non-LLM baselines), feature set, the learnable-parameter envelope table
   (parameter, range, default — Tier-2-owned per R4), champion/challenger lifecycle, validation
   methodology (E2), learning-ledger schema, rollback triggers.
7. **Risk & compliance** — Tier 2 limit table with starting values (every R2 item); kill-switch design
   (R10); compliance posture summary (B1–B7) with the owner-accepted risk items (TOTP automation if
   enabled; online learning steering live AUTO orders) flagged plainly; the family-only boundary (B5); a
   recommendation to obtain professional legal review before enabling AUTO; the ≤10 OPS-by-construction
   statement (B3).
8. **Phased roadmap with acceptance gates** — sequenced phases from empty repo to AUTO live, each with
   scope, deliverables, demo/acceptance criteria, and explicit gate conditions. Expected shape (adjust
   with justification): Phase 0 foundations (skeleton, config, secrets, token workflow, install smoke
   test per A4, A11 verification) → Phase 1 data + backtesting + baseline strategies → Phase 2 RECOMMEND
   live (paper ledger + Telegram + dashboard; zero API orders, B7) → Phase 3 AUTO in paper (full OMS
   shadow incl. R3 protective-order logic) → Phase 4 AUTO live, small (static IP, tightest limits,
   leverage ramp per O3, live-vs-paper fill tracking per R9) → Phase 5 learning loop fully online +
   leverage/style ramp completion. Estimate effort in sessions, where one session ≈ a focused 2–3 hour
   AI-agent implementation session.
9. **Testing strategy** — unit (risk gate exhaustively, every R2 limit); property-based tests for the OMS
   state machine; replay tests from recorded tick data (incl. a results-day gap through a stop); paper
   soak-test definition; and chaos cases asserting the R3 invariant, at minimum: process kill with open
   leveraged MIS position; Windows-Update reboot mid-session; WebSocket drop mid-position; token expiry
   mid-day; LLM timeout/garbage/credit-exhaustion; broker rejection storm; **lower-circuit lock with an
   open leveraged MIS long; upper-circuit lock against an MIS short (un-coverable at 15:10) and the
   broker square-off also failing → auction-settled short delivery, T+1 debit accounting + alerts (C8)**;
   **index circuit-breaker halt with open MIS positions (freeze entries, recompute square-off against the
   curtailed session)**; **full Kite/Zerodha outage ≥30 min with open positions**; GTT fired-but-rejected
   (A12); broker RMS force-close before the platform's stop (A8); platform starts on an exchange holiday
   (R6); clock skew beyond tolerance (R6).
10. **Operations runbook** — daily open/close checklists; the morning login flow (R6 incl. device
    fallback); monitoring; what each alert means; recovery procedures for every chaos case in §9
    including the **pre-staged manual fallback path (Kite app flatten, Kite kill switch, Call & Trade
    number)**; backup/restore of state; Windows service setup (E7 + D11).
11. **Cost & budget table** — monthly ₹ fixed costs vs. capital (Max subscription as footnoted sunk
    line); and a **per-agent monthly LLM cost worksheet**: each agent × calls/day × tokens/call, priced
    at cache-hit and cache-miss rates (D8), summing within the $100 credit (D3/D4), with the degrade
    ladder thresholds derived from it.
12. **Risk register** — technical, market, regulatory, and AI-behavior risks with likelihood/impact/
    mitigation; owner-accepted items explicitly marked as accepted.
13. **Out of scope / deferred** — explicitly: F&O/derivatives trading, multi-user anything, signal
    distribution (B5), cloud migration (deferred), mobile app, T+0 settlement reliance, BSE trading.
14. **Open questions for the owner** — each with the provisional default the plan assumes and builds on;
    a question flags a default the owner may want to change, never a hole in the plan.

## Plan quality bar

- Every architectural choice states its rationale and cites the IDs it depends on (facts A7/B3/D5…, owner
  decisions O1–O10, requirements R1–R10).
- Concrete over abstract: real numbers (limits, budgets, thresholds, schedules), real schemas, real file
  paths. Where a number is a starting point to be tuned, mark it `[tunable]` and put it in the envelope
  table if learnable.
- An implementing AI agent should be able to execute each phase without making architectural decisions;
  ambiguity is a defect.
- Honesty over salesmanship: where the evidence says something is unlikely to be profitable (C9), the plan
  says so and optimizes for learning per rupee of tuition instead.
- Length: as long as needed for the above, but no padding, no generic explanations of what trading terms
  mean, no boilerplate.
