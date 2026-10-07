# Plan — recommendation measurement, honest card, paper autopilot, exit/regime research (2026-10-06, v5)

*Scope approved by the owner from the QuantSync comparison (`runbooks/quantsync-comparison-2026-10-06.md` §7):*
- *item 1, measure (A1, A2)*
- *item 2, card and surface (B1, B3, B4; B2's channel split deferred by the owner)*
- *item 3, pre-registered research (C1–C3; C4 deferred, D12)*
- *"implement the auto paper trade, though it should not generate actual trade execution" (D8)*

*How it was built:*
- *six read-only subsystem maps*
- *round 1: a five-lens adversarial review, 76 findings → v2*
- *round 2: a three-lens review, 71 findings → v3*
- *round 3: a two-lens review, 29 findings → v4*
- *round 4: a focused review of the v4 changes, 9 findings → v5*

*Every finding was checked against the code before it was accepted. The resolution ledgers are `data/reports/quantsync_2026-10-06/plan_review_round{2,3,4}.md`.*

*Status: **PLAN — nothing built.***

---

## 0. Owner decisions (2026-10-06)

| # | Decision |
|---|---|
| D1 | Keep recommendations **and** paper-trade in parallel. Paper is **fully isolated**: it has its own book, gate check, equity, caps and halt states, and the real brakes and kill switch never react to paper. |
| D2 | The time exit follows the evidence. **Sell at the close of the 20th session of the hold; the entry session is session 1.** The card shows the date. In RECOMMEND the alert goes out on that session; paper sells near the close. brk20 keeps its stop and target, with the 20-session cap as a backstop. |
| D3 | For hi52/ins, **re-anchor the stop to the delivered entry** at the rule's percentage. BHEL on 10-06 would have had ₹418.30, not ₹402.25. This applies to recs and to paper. |
| D4 | Paper alerts: entry fill; exit fill with P&L; protection failure; and a paper P&L line in the evening summary. |
| D5 | `/veto SYMBOL <reason>` with codes `market, price, size, trust, away, other`. It works up to 3 sessions after a rec expires. One reminder at 15:45 lists the day's undecided recs. |
| D6 | `/protected SYMBOL` confirms the stop order after `/taken`. Reminders follow if it isn't confirmed. Until then the position is flagged *unprotected*. |
| D7 | The selection-filter study runs without delivery %. |
| D8 | "Implement the auto paper trade, though it should not generate actual trade execution." This is the WO-P3-5/P3-6 go-ahead, with a hard no-real-order condition. |
| D9 | When the real side is frozen, killed or OFF (for example `/pause_entries`, a real halt, a stale feed), paper gets no new trades either: the pipeline stops before the analyst. Paper exits and protection keep running. |
| D10 | Paper capital and caps are the `limits.yaml` values (₹40,000 base, 4 CNC slots, ₹12,000 per stock, 2 per sector), counted separately from real positions. |
| D11 | The regime tag stays off the card until study C2 reports. `GET /why` is deferred; the `/why` Telegram command ships. |
| D12 | The C4 late-session study and the B2 ops digest are deferred along with the channel split. |

## 1. Design decisions

1. **The paper autopilot is separate from AUTO mode.**
   - D1 needs recs to keep flowing while paper runs, and the mode is single-valued. D8 forbids live trading, so AUTO's only remaining meaning would be "live".
   - `feature_flags.paper_only` is already `true` (`settings.yaml:503-504`) but nothing reads it. This plan makes it the switch that refuses `/mode AUTO` and `Routing.LIVE`.
   - Paper has its own persisted switch, `/paper` (M4).
   - `paper.subsystem_enabled` decides whether any paper object is built. It ships `false` and becomes `true` only at go-live (Q4.13), so no deploy starts paper trading.
2. **A real order is structurally impossible.** Today the only safeguard is that nothing calls the order methods. The guard passes `risk_reducing` intents in every mode and never consults routing (`ops/main.py:623-638`, `broker/kite_client.py:153-185`). The Q3.1 pins:
   - `KiteClient` refuses every order and GTT call unless it is built with `orders_enabled=True`, which nothing outside the tests passes.
   - Paper managers accept only a `PaperBroker`.
   - The import closure of `engine.oms`/`engine.paper` never reaches `engine.broker`.
   - AST pins cover order-method call sites and raw `KiteConnect(` construction.
   - Paper postbacks travel on their own topic.
   - The §8.4 GTT IP-check probe is dropped: it would place a real GTT.
3. **Paper trades CNC only.**
   - Live originators are CNC swing (hi52, ins, brk20, rsi2, mom) and CNC position (trend, `strategy/scanners/trend.py:54`). No MIS rec has ever been delivered.
   - The paper path refuses MIS and fails closed.
   - SquareOffScheduler and MIS SL-M are deferred, and the G3 clock is not started.
4. **Isolation is by scope, through one helper.**
   - Paper rows sit in the existing tables with `is_paper=1`, plus `origin='platform'` on `positions`, whose CHECK has no paper value (`0001_initial.sql:76`).
   - `scope_sql(scope, alias, has_origin)` is the only predicate source. A test fails on the literal origin list anywhere else in code (Q3.2).
   - Paper ledger rows carry `rec_id = NULL`, so the rec_id-keyed writers (`ops/pipeline.py:482-648`) never touch them.
   - `held_symbols()` becomes real-only, because it also drives the brk20 retest skip and the sweep's held flag (`ops/main.py:905-917, 2051`). A new `feed_symbols()` covers real and paper, for the ticker only.
5. **Coupling runs one way (D9).**
   - Paper never writes `mode_state`, `kill_state` or `risk_state_causes`, never supplies a real `flatten` callback, and has no `SAFETY_CRITICAL` job.
   - Any non-NORMAL real risk state, mode OFF or a kill stops the pipeline before the analyst (`ops/pipeline.py:1956-1964`; the drain repeats it at `:1719-1724`). Paper therefore gets no proposals then. That is intended (D9) and adds no quota draw.
   - For a proposal already in flight when the real state changes, the paper gate uses the worse of the real state and paper's own halt state.
   - Paper exits and protection always run.
6. **Paper exits are deterministic, with no LLM.**
   - One exit routine serves every live cause: protection, the D2 time exit, the equity-floor flatten and failure handling.
   - Unobserved time and corporate actions close by bookkeeping.
   - Paper makes no position-event analyst calls, so it draws no quota (the §6.4 precedent).
7. **Unobserved time is reconciled, never ignored.** A real GTT fires at the broker while the engine is down or deaf; paper must not do better than that. The owner boots the PC at 09:30–10:00 on most days (`ops/main.py:1527-1528`), so this is the daily case, not an edge case.
   - **Construction.** When the broker is built, local state is restored synchronously: id counters, stranded order rows and the GTT book (Q4.1).
   - **Session prep.** The bridge records the last tick it forwarded. Before forwarding the first tick of each session, and the first after more than 5 unobserved session minutes (restart, host sleep, feed outage), it runs session prep (Q4.7). Prep walks backfilled 1m bars for the open positions through `exit_sim`, then runs the corporate-action check.
   - **No login signal needed.** Ticks flow only once the ticker runs after login, so prep never runs without a token.
   - **Paper waits for prep.** Paper entries and `paper_tick` run only while the current session's prep has completed and none is due or running.
   - **The Reconciler stays separate.** It is its own pass, never a lifecycle hook (`ops/lifecycle.py:374, 433-458`).
8. **Two measurement sources, complementary and labelled.**
   - `rec_outcomes` scores every entry rec with one rule, plus fixed T+5/T+10/T+20 nets.
   - The paper book measures realistic portfolio execution under D10's caps.
   - `exit_sim` owns the exit conventions and walks a stream of bars. Recs, the paper catch-up and research each pass their own stream and convention.
   - Nothing in `rec_outcomes` feeds a scanner, threshold or promotion. The hi52 verdict stays with its forward-test code.
9. **The card is honest and evidence-based.**
   - The evidence line cites a report and says how that backtest exited.
   - ₹-expected appears only where `settings.yaml` registers an edge (hi52 1.53, ins 1.58). brk20's `expected_edge_pct` is target geometry, so it is not shown.
   - There is no regime tag and no regime field (D11; nothing would read it).
10. **Horizon and calendar.**
    - N is each strategy's live hold, from one map built in the composition root (Q1.1). Intraday strategies have none, and N is never above the style cap (swing 20, position 120; `limits.yaml:151-152`).
    - The exit kind is stop/target when the rec carries a target, else time. Time-exit strategies drop an analyst-set target (Q1.2).
    - Muhurat and special sessions are not counted. Jobs run at fixed times on regular sessions.
    - An exit date or horizon beyond the loaded calendars is *pending* and is computed once the calendar lands. The Q0.4 monitor is the only alert for that condition.
11. **Research runs on a snapshot, outside market hours.** One evening engine stop exports a research DuckDB. Every study runs against it, and only outside 09:15–15:30.

## 2. Work breakdown

Tiers: `impl` = Sonnet implementer · `design` = Opus designer-impl · `mgr` = manager inline.

Every task carries its own tests, and its `_COMMANDS`/COMMANDS/RUNBOOK lines where it adds a command. Order-path and risk-layer diffs (M3/M4) get a whole-branch multi-angle review and the owner's review before merge.

### M0 — Plan of record and foundations (no behaviour change)

**Q0.1 Plan-of-record amendments** (`mgr`)
- *Why:* design-first; the code cites § numbers.
- *Change:* add dated 2026-10-06 notes:

  | § | Amendment |
  |---|---|
  | §2.6/§10.8 | The shutdown guard and its CNC-GTT verification stay Phase 4 (real orders); paper has none. Paper session prep runs on the first forwarded tick, not on a login hook |
  | §3.2.8 | Paper orders and GTTs persisted and restored; `scope_sql`; paper topic; `engine.oms` import rule |
  | §3.2.9 | PaperBroker: `topic` kwarg, seeded id counters, GTT-leg tag, `delete_gtt` refusing non-active GTTs, `restore`, marketable-LIMIT fills |
  | §3.5.1 | Two audited edges, `DRAFT→REJECTED` and `VALIDATED→REJECTED` (abandoned before the broker call) |
  | §3.5.2 | `CloseReason` + `RISK_FLATTEN`, `VOID` |
  | §3.5.3 | AUTO/LIVE refused while `paper_only`; paper autopilot is separate state |
  | §3.6/§7 | Card contract; `/veto` reasons; `/protected` |
  | §4.2 | 0015/0016 schema |
  | §4.4 and the RUNBOOK daily-job table | New jobs and ticks; the `fire_day` runner check |
  | §5.5 | Nightly paper line; counter fix |
  | §6.1 | D2 and D3 change the documented exits, but not the hi52 forward-test measurement |
  | §6.4 | `rec_outcomes` is not a shadow |
  | §7.1 | Paper halt ladder mirrors §7.1 (CNC keeps protection except at the equity floor) |
  | §8.4 | WO-P3-5/P3-6 amended: paper autopilot, CNC-only, GTT probe dropped |
  | §8.5/§10.6 | G3 clock not started |
  | §9.2 | R3 paper property |
  | §9.4/§9.5 | Chaos classification for paper (Q4.5) |

  Also:
  - Fix the stale hi52 text (`IMPLEMENTATION_PLAN.md:1401` still says 1.47 / 2.5×).
  - Record D8 verbatim, and D9–D12.
  - Credit B3's existing parts: WO-D2's sold-outside-ledger silence and the `POSITION_NOT_IN_HOLDINGS` `/closed` prompt.
- *Acceptance:* every listed § carries the note; WORKLOG entry.

**Q0.2 `NSECalendar.add_sessions(d, n, *, count_special=False)`** (`impl`)
- *Why:* five consumers need "the n-th counted session after d".
- *Change:* add the public helper.
  - If d is not itself a counted session, counting starts at the next counted session: `add_sessions(d, 0)` is the first counted session on or after d.
  - Calendar `ValueError` propagates.
  - `retest._expiry` is **not** migrated (it is live). An equivalence test pins it against `add_sessions(..., count_special=True)` over every 2026 session date. Signal dates are always sessions.
  - Retest counts the 2026-11-08 muhurat; the plan records that one-session divergence.
  - Verify how `bars_1d` represents the 2024-11-01 and 2025-10-21 muhurat sessions and the 2024-01-20/03-02/05-18 DR-drill Saturdays. Record whether `exit_sim` must skip them.
- *Tests:* holidays, weekend, muhurat skip, d on a non-session day, n=0, horizon raise.

**Q0.3 Migration `0015_rec_feedback.sql`** (`impl`)
- *Why:* durable S1/S2 state; in-memory dedupes re-fire after a restart.
- *Change:*
  - `recommendations`: add `skip_reason TEXT CHECK (skip_reason IN ('market','price','size','trust','away','other'))`, `skip_reason_at TEXT` and `reminder_sent_at TEXT`.
  - `positions`: add `owner_protected_at TEXT` and `protection_reminders INTEGER NOT NULL DEFAULT 0`. The migration sets `protection_reminders = 2` on positions already OPEN; they are grandfathered, and there are none today.
  - New tables: `rec_outcomes` (PK `rec_id`; columns in Q2.3) and `universe_ew_returns(d PK, ret, n)`.
- *Tests:*
  - per table, with upsert and CHECK rejection
  - upgrade from 0014 with populated rows: monkeypatch `migrations.discover()` to stop at 0014, then apply; rows preserved, defaults applied
  - `EXPECTED_TABLES` covers 0004–0015
  - old-schema reader: pre-0015 queries run on the migrated DB

**Q0.4 Calendar horizon monitor** (`impl` + owner ops)
- *Why:* a year with no calendar file has no trading days (`core/calendar.py:104-107`). Every exit date after 2026-12-31 is uncomputable until `2027.yaml` exists, and NSE usually publishes in mid-December.
- *Change:*
  - A boot and nightly check raises `CALENDAR_HORIZON` (non-critical) when the verified horizon is closer than `clock.calendar_horizon_alert_sessions` (40).
  - It sends once per horizon value: outbox dedupe key `calendar_horizon:<verified horizon>`, so daily boots don't repeat it.
  - It is the **only** alert for this condition. Consumers show "pending" and log: Q1.5, Q1.6, Q2.3, Q4.4, Q4.6, and the recommendations API, which computes a pending exit session on read.
  - Dated ops task: add `config/calendar/2027.yaml` once NSE's circular is out. Check for it from 2026-12-01.
- *Tests:* fires once per horizon value across boots; nothing once the horizon extends.

### M1 — Owner surface (S2; ships first, independent of paper)

**Q1.1 Contract fields, hold map, extras** (`impl`)
- *Why:* the card, the time exit and the scorecard need fields the payload lacks (`core/contracts.py:178-197`).
- *Change:*
  - New optional `Recommendation` fields with defaults; stored payloads are never re-validated:

    | Group | Fields |
    |---|---|
    | Identity and entry | `strategy_id`, `proposal_id`, `entry_type`, `reference_entry`, `reference_stop` |
    | Exit | `hold_sessions`, `exit_session` (None = pending), `exit_kind` |
    | Risk, stop order and evidence | `risk_inr`, `stop_atr_mult`, `gtt_instruction`, `evidence` |
  - **Hold map.** `ops/holds.py: build_hold_fn(settings)` is built once in the composition root and injected as `hold_fn(strategy_id, style) -> int | None` into the pipeline, `time_exit_check`, `rec_outcomes` and the paper PositionBook. Values:

    | Strategy | Hold |
    |---|---|
    | ins, cat, cat_reversal | `settings.<x>.hold_sessions`, the live values (`ops/main.py:1833-1836, 1866, 1888`) |
    | hi52 | `hi52.DEFAULT_PARAMS["hold_sessions"]` |
    | rsi2 | `max_hold_days` (it runs on its class defaults, `ops/main.py:849`) |
    | any other swing or position strategy | the style cap |
    | intraday | None |

    - No hold exceeds its style cap.
    - Update hi52's "documentation only" comment (`hi52.py:146`).
    - Tests pin the shipped values (hi52 20, rsi2 10, brk20/mom 20, trend 120, orb None, and ins/cat/cat_reversal as their settings) and assert every hold ≤ its cap.
  - **Extras.** `build_recommendation(..., extras: RecExtras | None = None)`. `_evaluate_forward` collects the extras under `asyncio.wait_for`, with a deadline no longer than `_GATE_CONTEXT_DEADLINE_S`; store reads go through `store.arun`.
    - On timeout the card goes out without the missing extras, and the timeout is logged (a hang is not an exception; `ops/pipeline.py:215-217`, the WO-24a stall).
    - The renderer stays pure.
  - **Tick rounding.** Inject `round_tick_fn` into `RecommendationPipeline`, wired to `instruments.round_to_tick`. On `UnknownInstrument` it falls back to the scanner's `strategy.types.round_to_tick` and logs the fallback (`broker/instruments.py:379-389`).
- *Tests:* an old payload parses; fields round-trip; GET shape unchanged; a pending exit session; an extras timeout still delivers the card.

**Q1.2 Stop re-anchoring (D3) and one exit per rec** (`impl`, manager review)
- *Change:* in `_evaluate_forward`, after LIMIT defaulting and before both gates (`ops/pipeline.py:2160-2168`):
  - **`{hi52, ins}`:** `frac = 1 − raw_stop/raw_entry`, then `stop = round_tick_fn(sym, anchor × (1 − frac))`.
    - The anchor is the LIMIT price. For MARKET it is the mark price the gate reads; a MARKET with no mark is unpriceable at the gate anyway (`gate.py:438-440`).
    - Log `stop_reanchored`.
  - **Time-exit strategies** (those the hold map gives a declared hold: hi52, ins, cat, cat_reversal, rsi2): drop any analyst-set `target_price` and log `target_dropped`. The gate then prices the registered edge (`gate.py:1048-1053`), and the card and checklist describe one exit.
  - brk20 is untouched.
- *Tests:*
  - BHEL → 418.30, and qty 14 passes `per_trade_risk` at anchor 445.00
  - a qty at the raw-geometry maximum may shrink by at most `anchor/raw_entry − 1` (logged)
  - MARKET
  - `frac ≤ 0` or a missing level → no change
  - the target drop
  - brk20 unchanged
- *Note:* the hi52 forward test (prescreen signals, time exit) is unaffected.

**Q1.3 Checklist wording and the `recommend:` settings block** (`impl`)
- *Why:* the current "GTT OCO … stop-only GTT" contradicts itself (`ops/pipeline.py:2366-2371`). Zerodha, verified 10-06:
  - a single-trigger GTT serves a stop-only exit
  - OCO pairs stop and target on holdings
  - a GTT lasts 1 year
  - a fired GTT places a one-time limit order
- *Change:*
  - Stop only: "place a single-trigger GTT: sell if price falls to ₹S; limit ₹L".
  - With a target: "GTT OCO: stop ₹S (limit ₹L) / target ₹T".
  - `L = round_tick(S × (1 − recommend.gtt_limit_offset_pct/100))`, explained as "so a fast drop still fills".
  - The line is stored as `gtt_instruction` for the `/taken` reply.
  - Create `RecommendSettings` (`extra='forbid'`) and the `recommend:` block. Q1.7–Q1.9 add their own fields, and Q4.5 reads the same offset.
- *Tests:* a new stop-only CNC test; the OCO pin (`test_reco_pipeline.py:1185`); the shipped settings load through the real consumer.

**Q1.4 Evidence registry** (`impl`)
- *Change:*
  - `config/strategy_evidence.yaml`, through a model with `extra='forbid'`. Per strategy it holds a one-line summary, `measured_exit` (e.g. "fixed 20-session hold, no stop"), and a report path:key for each value: n, hit rate, median net at the horizon, CPCV.
  - Sources: `data/reports/backtest_hi52_v2_2026-09-09.json`, `backtest_brk20_2026-09-12.json` and `event_study_20260814T023244.json`. Edges are read from `settings.yaml`, never copied.
  - The line reads: "backtest (fixed T+20 hold, no stop): n …, hit …, median net … — shipped stop untested (study C1)".
  - hi52 forward line: "forward test: k of 20 matured (m signals since 2026-09-12)".
    - k and m come from the verdict script's own measure path.
    - That path, `PROMOTION_DATE` and `MIN_SIGNALS` move into `engine/learning/hi52_forward.py`, which `scripts/hi52_forward_verdict.py` imports.
    - The card computes k and m during extras collection, under Q1.1's deadline.
- *Tests:* the shipped file loads through the real consumer; missing-strategy fallback; k and m equal the script's numbers on a fixture.
- *Acceptance:* every value's path:key is verified in review.

**Q1.5 Card rewrite** (`impl`; the manager reviews every owner-facing string)
- *Change — lines, each printed only when its data is present:*
  1. Action, qty, price, ₹ at risk "to the stop" (qty × (zone high − stop)), and the exit:
     - "sell by the close on <date> (session N)", or
     - "stop ₹S / target ₹T; time cap <date>", or
     - "exit date pending (NSE 2027 calendar)", or
     - "stop ₹S / target ₹T; time cap pending (NSE 2027 calendar)".
  2. Stop as % and as × daily ATR, plus the rule reference and the entry move when re-anchored.
  3. Evidence line (Q1.4).
  4. ₹-expected, registered edges only: "registered edge 1.53% net ≈ ₹X — not a forecast".
  5. Thesis.
  6. Gate result. A shrink reads "shrunk 14→9 (bound by per_trade_risk)", taken from the gate's own "bound by" reason (`gate.py:496-497`). `(FAILED)` stays for non-shrink failures.
  7. Cost.
  8. B7 checklist, never truncated.
  9. Entry-rec footer, always last: `record: /taken SYM QTY <price> · decline: /veto SYM <market|price|size|trust|away|other>`.
- Exit and adjust cards keep their current footer.
- Confidence is dropped from the text but stays in the payload and the gate.
- RUNBOOK gets a short "reading the card" section, built on the BHEL example.
- *Tests:*
  - the pins at `test_telegram_commands.py:925-937, 970-972, 1045, 1081`; the exit/adjust footer pins at `:1054, 1063` stay
  - ≤4096 chars (plain text, no parse mode; `telegram.py:623-627`)
  - footer last; checklist complete; every non-shrink failure present
  - render does no I/O
  - both pending forms

**Q1.6 Time-exit alignment (D2) and the `fire_day` runner check** (`design`)
- *Change:*
  - **Exit session:**
    - Read from the payload.
    - For legacy rows: `add_sessions(session_of(delivered_at), N−1)`, with N from `hold_fn`, never from `opened_at` (HDFCAMC was `/taken` at 23:26).
    - With no resolvable entry rec, fall back to today's `opened_at` age rule.
    - A pending date is recomputed on each run.
  - **Job:** RUN_LATEST `time_exit_check` at a fixed 09:30, with order 5, ahead of the post-arm news chain (`ops/main.py:241`).
    - `fire_day`: a regular (non-special) trading session.
    - Listed in `POST_ARM_JOB_IDS` and pinned in the `test_ops_main_wiring.py:262-271` table.
    - It replaces `check_aged_positions` inside `job_reco_expire`. `log_hot_path_stats("eod")` stays in `job_reco_expire`; the GateContextTimeout re-raise moves with the check (`pipeline.py:2702-2704`).
  - **Runner check (ships here):** `_scheduled_runner` returns early when `spec.fire_day` is set and false for today.
    - Today every registry job fires on every armed trading day; only `sector_map` has a weekday cron, keyed on its job id (`ops/main.py:2463-2520`).
    - The catch-up already honours `fire_day`, which there *replaces* the trading-day test (`ops/jobs.py:779-782`). Every `fire_day` must therefore include "is a regular trading session" itself.
  - **Selection:** OPEN `origin='recommended'` positions with today ≥ `exit_session`. Paper rows are `origin='platform'`, so this is real-only.
    - It re-issues the time exit every session until `/closed`, including when the holdings journal shows zero shares. The time exit is the deterministic backstop (pinned by `test_reco_pipeline.py:1908-1922`), and `POSITION_NOT_IN_HOLDINGS` already prompts `/closed`.
  - **Wording,** dated because recs never expire in the outbox:
    - "sell before the close on <date>" when today == `exit_session`
    - otherwise "overdue since <date>: sell at the next open", or "…now" inside the session
  - **Liveness:**
    - Catch-up replays a missed fire, and E5 alerts a failing run.
    - A wedged event loop delays every job, this one included, until recovery. The heartbeat runs on its own thread and cannot see that (`ops/heartbeat.py:3-6`). This is an accepted risk; no extra check.
- *Tests:*
  - the N-th session across holidays and muhurat
  - a late `/taken`; a pending date
  - catch-up after a 14:00 boot and after a 16:00 boot
  - re-issue on the next session; holdings at zero still alert; no entry rec
  - a position-style position
  - the runner check: `sector_map` unaffected; nothing fires on the 2026-11-08 muhurat Sunday
  - pins:
    - `test_ops_main_wiring.py:247, 262-271`
    - `test_ins_crossings_job.py:557-575`
    - `test_case05_llm_failure.py:466`
    - `test_reco_pipeline.py:1487`, and `:1922`, rewritten against `time_exit_check` with the same assertion
    - `test_reco_pipeline_wo24.py:371`
    - chaos case 16's no-job window (`:105`)
  - docstrings `hi52.py:104`, `ins.py:59`, `cat.py:64`, `cat_reversal.py:76`, and the text at `pipeline.py:2274-2277`

**Q1.7 `/veto` with reason (D5)** (`impl`)
- *Change:*
  - **Entry recs only.** Candidates are the undecided entry recs for SYM that are live, or expired with `valid_until` inside the last `recommend.veto_window_sessions` (3) sessions.
    - Exactly one candidate ⇒ act on it.
    - Several ⇒ the existing WO-29 reply listing them with full ids, and no action (`notify/telegram.py:1213-1225`). An id argument resolves exactly.
    - One shared helper builds this candidate list, and the resolver gets it instead of `include_expired=True`. That path adds every expired rec, with no window, kind or `skip_reason` filter (`notify/telegram.py:1236-1241`).
  - `/veto SYM <reason>` records the reason. A bare `/veto SYM` on an entry rec replies with `/veto SYM <market|price|size|trust|away|other>` and records nothing.
  - A live rec becomes `dismissed` with the reason. An expired rec stays `expired`, with `skip_reason` set.
  - `take()` clears `skip_reason`. Only an expired rec can be taken after a veto; a dismissed one is refused, as today (`pipeline.py:442-446`).
  - Exit and adjust recs keep today's bare `/veto` (dismiss, no reason).
  - `RecoBook.veto(rec_id, reason)`, `_USAGE_VETO`, the help row and the test fakes change in one commit.
- *Tests:*
  - the codes; a bare `/veto` records nothing
  - inside and outside the window
  - a live and an expired candidate for one symbol ⇒ the WO-29 list, no action
  - an exit rec unaffected
  - `/taken` after a veto on an expired rec
  - `learning_ledger` `no_action` behaviour unchanged

**Q1.8 15:45 decision reminder (D5)** (`impl`)
- *Change:*
  - `expire_stale` returns the expired `rec_id`s; callers use `len()`. Pins: `test_reco_pipeline.py:1377-1378, 3334`.
  - `REC_DECISION_REMINDER` (non-critical) lists every expired candidate from Q1.7's helper that has no `reminder_sent_at`. That is: `kind='entry'`, `human_action='expired'`, no `skip_reason`, and `valid_until` inside the veto window.
  - Each rec gets the six-code line with no default, and `reminder_sent_at` is stamped.
  - A run outside `recommend.reminder_window_start`–`reminder_window_end` (08:00–22:00) sends nothing; the same recs qualify at the next run.
- *Tests:* zero, one and many recs; exit recs excluded; a late catch-up followed by the next 15:45 run; a rec that expired two sessions earlier, during an outage, is still reminded; idempotent.

**Q1.9 `/protected` and reminders (D6)** (`impl`)
- *Change:*
  - The `/taken` reply reprints the stored `gtt_instruction`, then "reply /protected SYM once placed".
  - `/protected SYM` (new `_COMMANDS` row and handler) resolves the OPEN `origin='recommended'` position through its learning-ledger rec link and sets `owner_protected_at`.
  - **Reminders:** `PROTECTION_REMINDER` with `severity='critical'`, which alone makes it non-expiring (`telegram.py:1888-1892`), so no `CRITICAL_KINDS` entry. The text carries its as-of time.
    - Sent by a `protection_reminder_tick` interval job every 300 s, a new `_arm_live_jobs` keyword (`ops/main.py:2535`).
    - Reminder 1 goes at +`recommend.protection_first_reminder_min` (30). Outside the reminder window it is deferred to its start.
    - Reminder 2 goes once the next session has opened.
    - The `protection_reminders` counter dedupes.
    - Positions the holdings journal confirms at zero are skipped.
  - `/positions` shows "owner-confirmed protected <ts>" or "UNPROTECTED (unconfirmed)". Today it prints "unprotected" for every NULL `protection_state` (`telegram.py:1372`).
  - COMMANDS and RUNBOOK lines.
- *Tests:* confirmation before reminder 1; at most two; a restart mid-sequence; a late-night `/taken`; a closed position; zero holdings; a grandfathered position.

**Q1.10 `/why SYMBOL`** (`impl`)
- *Change:* a deterministic, read-only `_COMMANDS` row. A late-wired reader `set_why_fn` (the `set_scan_sweep_fn` precedent) lives in `ops/why.py`. It shows:
  - universe status and exclusion reasons
  - hi52/brk20 distance to trigger
  - surveillance and results-day flags
  - latest insider and pledge filings, point-in-time, with dates
  - the last rec, with its outcome or skip reason
  - open positions
  - the last verdict's reasons (Q4.11 adds the paper label)
- *Constraints:* store reads through `store.arun` with a timeout; output under 4096 chars; no LLM; never `/scan_now`. COMMANDS and RUNBOOK lines.
- *Tests:* each section present and absent; unknown symbol; the cap; owner-only; zero harness calls; nothing enqueued.

**Q1.11 Nightly closed-trade counter** (`impl`)
- *Why:* `_closed_trades` counts no-action expiries as closed trades (`ops/nightly_review.py:127-140`).
- *Change:* exclude `no_action` and report expiries separately. The real-only scoping of the review's ledger and verdict reads lands in Q3.2, once `verdicts.is_paper` exists.
- *Tests:* a fixture with closes and expiries.

**Q1.12 Dashboard: recommendations and positions** (`impl`)
- *Change:* new payload fields in `types.ts` and `RecommendationsPanel` (the confidence chip stays as detail); protection status in `PositionsPanel`; fixture rows; README.
- *Acceptance (dashboard standard):* `npm run build` passes; a fixture-server screenshot of each changed panel; zero console errors.

### M2 — Measurement (S1)

**Q2.1 `engine/learning/exit_sim.py`** (`design`)
- *Change:* one pure simulator.
  - **Inputs:**
    - an ordered stream of bars, each tagged `1m` or `daily`
    - a start state: either a pending entry (order type, price, fill rule, entry window) or a position already open (price, time)
    - stop and target
    - the horizon: an exit session, or a bar count from the fill
    - an end time
  - **Returns:** `exit` (date, price, reason), `void_ca`, `open` or `unfilled`.

  | Case | Recs | Paper catch-up (Q4.7) | Research |
  |---|---|---|---|
  | Bars | 1m for the delivery session after `delivered_at`, daily afterwards | 1m over the unobserved session minutes | daily |
  | Sessions | calendar sessions; a missing bar stops the walk (outcome `open`) | the unobserved minutes only | the symbol's own bar positions (house convention, `hi52_forward_verdict.py:42-44`) |
  | Start | pending entry, delivery session only | open position | the population's registered entry |
  | LIMIT BUY fill | trade-through `low < limit`, at `min(open, limit)` | — | the registered rule (touch `low ≤ level` for brk20) |
  | MARKET | open of the first 1m bar after `delivered_at`, else the delivery close | — | registered (next open) |
  | Fill basis | `1m_post_delivery` or `daily`, stored. Daily bias, stated: a LIMIT tested against the whole session low includes prints before delivery | — | — |
  | Stop/target per bar | stop on `low ≤ stop` at `min(open, stop)`; target on `high ≥ target` at `max(open, target)`; both in one bar ⇒ stop. On the daily-basis fill session, not evaluated (stated) | same | same |
  | Time exit | close of `exit_session` | same | the registered offset (brk20: `close(fill+20)`) |
  | T+k | close of the k-th session, counting the fill session as 1 (D2; the hi52 forward measure path matches, `hi52_forward_verdict.py:326-327`) | — | registered |
  | Corporate action | `hi52.UNADJUSTED_KINDS` ex-date ∈ (fill_d, exit_d] ⇒ `void_ca` | the caller voids before walking (Q4.7) | same as recs |
  | Costs | `CostModel.round_trip(notional, product)` at the rec notional | at the entry notional (Q4.4) | ₹20,000 reference notional (house; `backtest_hi52.py:285`); C1 also at the §7.1 sizing notional |
  | Fixed horizons | k = 5, 10, 20 with `stop=None` | — | — |
- *Tests:* one per convention, including:
  - gap-through
  - double touch
  - exact touch, under trade-through and under touch
  - MARKET; daily-only fill
  - missing middle bar
  - ex-date after the exit
  - unfilled
  - a stream mixing 1m and daily bars
  - an open-position start mid-session; an end time mid-session

**Q2.2 Equal-weight benchmark** (`impl`)
- *Change:*
  - `compute_ew_return(d)` is the mean of `close(d)/close(prev) − 1` over `get_universe_eligible_symbols` (`marketdata/store.py:1671-1684`), taken from the latest `universe_daily` row on or before d. This follows the lookback at `ops/main.py:993-998`; a day the engine was off has no row of its own.
  - It is a gap only when no row exists within 5 sessions.
  - Excluded: names with a `UNADJUSTED_KINDS` ex-date on d, and names with |ret| > 25% (UDiFF `prev_close` is unadjusted).
  - **Bench window:**
    - time exit: fill_d+1 … exit_d
    - intrasession stop or target exit: fill_d+1 … exit_d−1
    - a window with any missing day gets `bench_pct = NULL` and is counted; it is never silently shortened
    - the convention is printed next to `excess_pct`
- *Tests:* a split name is excluded; a missing universe row uses the previous one; a gap day ⇒ NULL; same-day fill-and-stop; an empty universe ⇒ `ok=False`.

**Q2.3 `rec_outcomes` EOD job** (`design`)
- *Change:*
  - DATE_KEYED at 18:30, order 35, after bhavcopy and `daily_bars` (18:05, order 30). Listed in `POST_ARM_JOB_IDS`.
  - **Per run:**
    - Write `universe_ew_returns` for d; the first run backfills from 2026-08-26.
    - Score every non-final **entry** rec up to d, upserting:
      - `rec_outcomes(rec_id PK, strategy_id, entry_type, fill_basis, status [unfilled|open|closed|void_ca|unscorable], fill_d, fill_px, exit_d, exit_px, exit_reason, gross_pct, cost_pct, net_pct, net_t5, net_t10, net_t20, bench_pct, excess_pct, excess_t20, updated_at)`
    - The walk runs only up to d. An exit session or T+k horizon the calendar can't compute yet counts as not reached: status `open`, the horizon NULL, logged.
    - Owner action and skip reason are joined at read time, not copied.
  - **Old-rec contract:**
    - `strategy_id` comes from `learning_ledger` by `rec_id`; if absent, the rec is `unscorable`.
    - `entry_type` comes from the proposal payload; otherwise a degenerate zone means LIMIT.
    - N comes from `hold_fn`; the exit kind from whether a target is present; levels from the payload.
    - The 30 historical entry recs are backfilled on the first run.
  - DuckDB reads go through `store.arun`; SQLite writes happen on the loop thread.
  - While `daily_bars` for d is unrecorded or failed, the run returns `unfinished`: no watermark is recorded and it is replayed later (`ops/jobs.py:71-75`).
    - Once `daily_bars` for d is ok or skipped, the run scores what exists; a missing bar leaves the rec `open`.
    - The bound matters because `unfinished` has no give-up of its own (`ops/jobs.py:723-725`).
    - `ok=False` is only for real failures.
  - **Labelled hindsight. Never read by any scanner, threshold or promotion code.**
- *Tests:*
  - **row counts of `positions`, `learning_ledger` and `shadow_trades` are unchanged**
  - idempotent
  - late bars ⇒ `unfinished` with no alert; a missing middle bar
  - a pending horizon
  - old recs; exit recs ignored
  - registration pins

**Q2.4 Scorecard route** (`impl`)
- *Change:* `GET /scorecard` (`_: Owner`). Per strategy:
  - recs: n, filled, closed, hit rate, median/mean net, net T+20, mean excess, owner actions by reason, and the `daily`-basis and `unscorable` counts
  - paper (filled in by Q4.11; a stub until then): closed trades, hit rate, net, open positions

  Void outcomes are excluded from both. Labelled "hindsight on official bars — not platform equity".
- *Tests:* `test_api_routes.py`, including the stub shape.

**Q2.5 Scorecard panel** (`impl`)
- *Change:* `ScorecardPanel` with its own 60 s hook, plus a fixture route and README.
- *Acceptance:* as Q1.12.

**Q2.6 Weekly summary** (`impl`)
- *Why:* `WEEKLY_SUMMARY` has no producer (`notify/catalog.py:68`).
- *Change:*
  - DATE_KEYED at 19:00, ordered after `rec_outcomes`, listed in `POST_ARM_JOB_IDS`, using Q1.6's runner check.
  - `fire_day(d)`: d is a regular session, and `add_sessions(d, 1)` falls in a later ISO week. A calendar raise counts as last.
  - It returns `unfinished` only while `rec_outcomes` for d is unrecorded or failed. Once that is ok or skipped, it sends with what exists.
  - **Content,** at most five lines:
    - delivered, taken, skipped by reason, expired
    - "skipped ₹X" over closed skipped recs, labelled hindsight
    - cumulative net and excess per strategy
    - the paper line (Q4.11)
  - Update the `WEEKLY_SUMMARY` docstring.
- *Tests:* fires on the week's last regular session only; never on a weekend or the 2026-11-08 muhurat Sunday; a missed Friday replayed on Monday; no recs.

**Q2.7 Route-enumeration auth test** (`impl`)
- *Change:*
  - Set `openapi_url=None`; today `/openapi.json` is unauthenticated (`api/app.py:214`).
  - The test walks `create_app().routes`, including `_IncludedRouter`:
    - every `APIRoute` depends on `_require_owner`, except the allowlist `/kite/callback`, `/notifications-ui`, `/notifications-ui/`
    - any non-APIRoute HTTP route outside the allowlist fails
    - `/ws/live` without a token closes with 1008
    - the static mount, when present, is last

### M3 — Paper foundations: pins, schema, isolation (no paper trading)

**Q3.1 Live-impossibility pins (D8)** (`mgr`, high scrutiny)
- *Change:*
  - **(a) `KiteClient(..., orders_enabled: bool = False)`.**
    - While False, `_order_call` raises `OrderSurfaceViolation` before the guard and the limiter, for all six order/GTT methods.
    - Extract the guard to a module-level `make_order_guard(...)`. Point `test_order_surface.py` and chaos case 06 at it instead of their copies.
    - Tests that place orders pass `orders_enabled=True` in-test: `test_kite_client.py:94-95`, chaos `case04:220` and `case06:171-174`, plus any others a grep finds.
  - **(b) `ModeManager(..., paper_only)`** refuses AUTO and `Routing.LIVE` with `ModeRefused`. Telegram `_cmd_mode` replies before any challenge (`telegram.py:1025`). The API route already refuses AUTO with 409 (`api/app.py:722`); a test pins it.
  - **(c) Import graph.** `engine.oms` may import only `engine.core`, `engine.oms` and `engine.paper`. A transitive-closure test proves `engine.oms`/`engine.paper` never reach `engine.broker` (`tests/unit/test_import_graph.py:36-40`). Cross-layer needs arrive as injected callables: `round_trip_fn`, `round_tick_fn`, `hold_fn`, `corp_actions_fn`, `bars_fn`, `replay_fn`, `notify_fn`.
  - **(d) Boot.** A persisted `mode=AUTO` while `paper_only` is downgraded to RECOMMEND, which nulls routing (`risk/mode.py:109-118`), with a MODE_CHANGE alert.
  - **(e) Paper manager constructors** raise `TypeError` unless `isinstance(broker, PaperBroker)`. A wiring test with a fake `KiteClient` asserts the composition.
  - **(f) AST pins over `src/engine` and `scripts`:**
    - Every mutating pykiteconnect method (place/modify/cancel/exit order, place/modify/delete GTT, `convert_position`, the MF order/SIP methods) is called only in `broker/kite_client.py`, `engine/paper` and `engine/oms`.
    - `KiteConnect(` is constructed only at `broker/session.py:301`, `scripts/a11_check.py:110` and `scripts/q15_candle_latency.py:124`.
    - `kite_connect()` is called only at `ops/main.py:642` and `scripts/backfill.py:456`.
    - `orders_enabled=True` appears only under `tests/`.
- *Tests:* one per pin.

**Q3.3 Migration `0016_paper_book.sql`** (`impl`)
- *Change:*
  - `verdicts.is_paper INTEGER NOT NULL DEFAULT 0`.
  - `gtts`: keep `gtt_id`, `position_id`, `state`, `trigger_low` (stop) and `trigger_high` (target; NULL for single). Add `is_paper, symbol, side, product, qty, stop_limit, target_limit, last_price, created_at`. Paper ids are 900001+.
  - `positions`: add `strategy_id TEXT`, `exit_session TEXT` and `close_basis TEXT`, set for paper and NULL for real.
  - A partial UNIQUE index on `orders(verdict_id) WHERE role='entry'`. The header records the precheck: no duplicates, and `orders` is empty today.
  - `paper_state`, a single row: `enabled, changed_at, changed_by, last_observed_at, epoch_started_at, reset_requested_at`.
  - `paper_equity_snapshots`, with the same columns as `equity_snapshots`.
  - `paper_halts(cause PK, rung, set_at, cleared_at, latched)`.
- *Tests:* per table; the unique index; upgrade with rows; old-schema reader; `EXPECTED_TABLES` adds 0016.

**Q3.2 Scope isolation (D1)** (`design`, high scrutiny)
- *Change:*
  - **`scope_sql(scope, alias=None, *, has_origin=False)`:**
    - real = `COALESCE(a.is_paper,0)=0`, plus `a.origin IN ('platform','recommended')` when `has_origin`
    - paper = `a.is_paper=1`, plus `a.origin='platform'` when `has_origin`
    - A test executes every combination against the migrated schema for `positions`, `orders`, `learning_ledger`, `verdicts` and `gtts`.
    - An AST test fails on the literal origin list in a code string outside this module. It ignores docstrings and comments; the literal appears in docstrings at `ops/pipeline.py:363` and `ops/holdings_reconcile.py:11`.
    - Today the literal recurs in code at `risk/exposure.py:43, 227`, `risk/gate.py:1528`, `ops/lifecycle.py:782`, `ops/main.py:1006`, `ops/pipeline.py:3213` and `ops/holdings_reconcile.py:110`.
  - **`ExposureTracker(scope)`:** every query, including `realized_net_closed_on`, `consecutive_losses`, the snapshot table and `weekly_drawdown_peak`. The paper tracker:
    - has no ModeManager, KillSwitch or latch
    - raises on `apply_*`
    - counts only rows after `paper_state.epoch_started_at`
    - skips `void` outcomes in `consecutive_losses`, as it skips `no_action`; voids still count in equity (Q4.4)
  - **`GateContextBuilder(scope)`:**

    | Field | Real | Paper |
    |---|---|---|
    | positions | real rows | paper rows |
    | orders, known/protective ids | real only | paper only |
    | pending | unexpired recs | paper entry orders, non-terminal, `filled_qty=0` |
    | entries today | entry recs | paper entry orders submitted today |
    | exiting | unchanged | paper positions in `PENDING_EXIT` |
    | gone (holdings journal) | unchanged | empty |
    | margins | unchanged (n/a) | n/a; `capital_cap` binds |
    | `risk_state` | real | worse of real and paper halts (§1.5) |

  - **Real-only readers:**
    - nightly review: ledger and verdicts (`nightly_review.py:127-140, 201, 673`)
    - the WO-24c orphan-proposal sweep (`pipeline.py:1575-1578`)
    - `/decisions` and `/verdicts`: they return `is_paper`, one row per proposal, with the real and paper verdicts as separate fields
    - Telegram `/positions`, with a separate paper section
    - `preopen_planner.py:398`
    - every literal site above
  - **Symbols:** `held_symbols()` becomes real-only. `feed_symbols()` (real ∪ paper positions, working orders and ACTIVE GTTs) feeds only `ticker_tokens()`.
- *Tests — invariants in both directions:*
  - Paper rows, including a partially filled paper entry, change no real output. That covers equity, day MTM, counts, sector counts, deployed capital, `consecutive_losses`, pending, `entry_recs_today`, retest skip reasons, sweep rows, nightly counts and the orphan alert.
  - Real rows, including a real exit rec on a paper-held symbol, change no paper output.
  - `/taken`, `/closed`, `/veto` and `expire_stale` leave paper rows byte-identical.
  - Legacy fixtures stay real.
  - Every paper position, working-order and ACTIVE-GTT symbol is in `ticker_tokens()`.

### M4 — Paper autopilot (S4)

**Q4.0 Paper control and settings** (`impl`)
- *Change:*
  - `ops/paper_control.py` over `paper_state`.
  - `_COMMANDS` rows for `/paper on|off|status|reset`:
    - `on` and `reset` are two-step via `_issue_challenge`. They are refused only while an **unexpired** challenge is pending: the slot is single and is cleared only on `/confirm` (`telegram.py:1084-1086, 1112-1124`).
    - `/paper on` refuses unless the autopilot is built (`paper.subsystem_enabled`).
    - `off`: no new entries; exits and GTTs continue.
    - `status`: enabled, epoch equity, open positions, day P&L, halts, and the counters (reconcile mismatches, voids, late corporate actions).
    - `reset` records `reset_requested_at`. Q4.8 carries it out: exit all, then a new epoch once the book is flat. The scorecard keeps the full record.
  - `GET /paper` (`_: Owner`) returns control state only.
  - `PaperSettings` (`extra='forbid'`): `subsystem_enabled` (false), `seed`, `exit_minutes_before_close` (10), `exit_working_timeout_min` (5).
  - Capital and caps are the `limits.yaml` values, read-only (D10).
  - COMMANDS and RUNBOOK lines.
- *Tests:*
  - the two-step; refusal while a challenge is pending, and after it expires
  - refusal without the autopilot
  - reset records the request; its execution is tested in Q4.8
  - persistence; shipped settings; a typo'd key fails

**Q4.1 Broker construction and the market-data bridge** (`design`)
- *Change:*
  - **Construction:** only when `paper.subsystem_enabled`, synchronously, before the scheduler arms (`ops/main.py:2174`). The broker gets the fill model, the seed, `available_margin` = the capital base, and `topic=PAPER_ORDER_UPDATE_TOPIC`; today the topic is hard-coded to the real one (`paper/broker.py:473`). `ReplayHarness` filters on the broker's topic.
    - The broker's tick-size lookup is lazy, because the InstrumentStore is hydrated later in boot (`ops/main.py:2223`).
  - **Construction steps,** each guarded. A failure leaves the subsystem degraded and sends `PAPER_ALERT`.
    1. Seed the id counters from the MAX over **all** `is_paper=1` order and GTT ids, whatever their state (Q4.2a). No order, however early, can then reuse an id.
    2. Close stranded order rows through legal edges. Entries and exits never survive a restart:
       - ACKED or PARTIALLY_FILLED ⇒ LAPSED; MODIFY_PENDING is first resolved by `filled_qty`
       - SUBMITTED or CANCEL_PENDING ⇒ CANCELLED (`oms/state.py:251, 281`)
       - DRAFT or VALIDATED ⇒ REJECTED (the edges Q4.3 adds)
    3. Restore the ACTIVE GTTs of open paper positions (Q4.2a).
    4. Mark session prep due, from the persisted `last_observed_at`.
  - **Bridge:**
    - Async bus handlers forward in-session `tick` and finalized `bar.1m` events to `PaperBroker.on_tick/on_bar`. Bus isolation already exists (`core/eventbus.py:81-85`).
    - It records `last_observed_at` = max(previous, the forwarded tick's exchange time). The max matters because a reconnect re-delivers older snapshot ticks (`broker/ticker_supervisor.py:804-806`).
    - `paper_tick` persists it every 30 s, and a completed prep sets it to the prep's window end.
    - It forwards nothing while session prep (Q4.7) is due or running. Ticks that arrive meanwhile are dropped.
  - **Gating:** paper entries (`paper_enabled_fn`, the paper order guard) and all of `paper_tick`'s work run only while this session's prep has completed and no prep is due or running. Otherwise a resumed process would evaluate halts on fresh marks before the gap's stops were applied.
- *Tests:*
  - counters seeded with only terminal rows present
  - every stranded state reaches a terminal state legally
  - nothing trades and no halt is evaluated while a prep is due or running
  - an older snapshot tick never moves `last_observed_at` back
  - golden-day performance and loop-block extended
  - no paper frame on `order.update`; a real `order.update` changes no paper row
  - a raising paper handler leaves tick and bar consumers running

**Q4.2 PaperBroker changes** (`design`)
- *Change:*
  - (a) **Ids and restore.** `PaperBroker(..., order_seq, gtt_seq)` takes the seeded counters. Without them, ids repeat after a restart and frames are absorbed as terminal duplicates (`paper/broker.py:405-406, 416-422`; `oms/store.py:296-298`). `restore(gtt_rows)` restores ACTIVE GTTs with their stored `last_price`, from plain dicts, so `engine.paper` stays core-only.
  - (b) A fired leg carries `tag='gtt:<id>:<leg>'`.
  - (c) `delete_gtt` raises on any non-active GTT, as Kite does; today it pops any status (`:852-861`).
  - (d) **Marketable LIMIT,** decided once, on the order's first eligible tick:
    - A LIMIT that trades through on that tick is marketable. Every fill of it prices at the market (`_market_price`, with slippage), capped at its limit, while ticks stay through the limit. The participation cap still sizes each fill (`:1158-1161`).
    - A LIMIT that does not is resting, and keeps trade-through-at-limit, the §9.3 conservatism case (`:1170-1211`).
    - Without this, every stop-out books the GTT limit, about 1% worse than live, and paper disagrees with `exit_sim`. Golden digests are re-pinned.
- *Tests:*
  - restore digest; the leg tag
  - delete refused on a triggered GTT
  - marketable vs resting fills, BUY and SELL
  - a marketable order filled across two minutes under the participation cap prices both fills at market
  - a fired leg fills near the market

**Q4.3 OrderManager** (`engine.oms`, `design`)
- *Change:*
  - **`submit(proposal, verdict)`:** CNC paper only; MIS is refused. It persists the order before the broker call, with the unique index giving idempotency, and derives the intent from the role.
  - **Paper `order_guard` on the broker:** an entry needs paper enabled and the effective state NORMAL; `risk_reducing` always passes.
  - **One consumer task drains an `asyncio.Queue` of paper frames.** State transitions finish before the first `await`; notifications go out after commit.
  - **Leg adoption:** a frame with an unknown broker id and a `gtt:<id>:<leg>` tag is adopted. `OrderStore.insert` creates a `gtt_leg` order for the gtts row's position, then the normal path runs. It never goes to `PendingCorrelation`.
  - **Abandoned orders:** implement the two §3.5.1 edges, `DRAFT→REJECTED` and `VALIDATED→REJECTED`, in `oms/state.py`'s transition table. Update the `test_oms_state.py:176-205` pin.
- *Tests:*
  - the two new edges, and no others
  - postback before response, in both orders
  - two back-to-back partial fills with an awaiting notifier produce one GTT with the right qty
  - duplicate submit; injected rejection
  - leg adoption

**Q4.4 PositionBook** (`design`)
- *Change:*
  - **First fill** creates the position: paper scope, OPEN, `PROTECTION_PENDING`. It sets `strategy_id` and `exit_session = add_sessions(session_of(first fill), N−1)` (NULL means pending), with N from `hold_fn`. Partial fills accrete.
  - **Oversell refused:** an exit SELL above the open qty minus working sells is refused, so paper CNC never goes short.
  - **Close** records:
    - gross `realized_pnl`, plus costs via `round_trip_fn` at the entry notional (*2026-10-07, M4 review:* a close priced by PaperBroker fills charges fees only, round trip minus its `spread` component, because the fill model already put the half-spread into both fill prices; bookkeeping closes priced off bars keep the full round trip)
    - a learning_ledger row: `is_paper=1`, `rec_id=NULL`, `proposal_id`, `verdict_id`, `strategy_id`
    - `close_reason` from the §3.5.2 vocabulary, including `RISK_FLATTEN` and `VOID`; update the `test_oms_state.py:112` pin
    - `close_basis`
  - **`close_bookkeeping(position, price, at, reason, basis)`** closes without a broker order, for the catch-up and voids. It first cancels the position's working orders, a working leg included, and deletes its ACTIVE GTT, verifying both; a restored GTT left armed would fire on the next tick against a closed position.
  - **VOID** realizes at the mark the paper tracker last carried for the position. Paper equity therefore stays continuous with the day baseline and the weekly peak (`risk/exposure.py:192, 212, 240`). Its ledger row is labelled `void`. The scorecard, the hit rate and `consecutive_losses` skip it.
- *Tests:*
  - partial then full; costs; ledger row
  - a bookkeeping close removes the GTT and working orders first
  - a void keeps equity continuous and is skipped by the scorecard and streaks
  - a sell above open-minus-working is refused
  - every UPDATE asserts `is_paper=1`

**Q4.5 ProtectionManager — state machine** (`design`)
- *States:*
  - `ACTIVE`: an ACTIVE GTT.
  - `EXIT_WORKING`: the GTT is TRIGGERED and its `gtt_leg` order is still working; the position is `PENDING_EXIT`.
  - `PROTECTION_FAILED`: also covers the broker's `failed` GTT status.
  - closed.
- **Placement.** On every entry fill, place or modify one GTT: OCO with a target, else single.
  - trigger = the stop
  - limit = `round_tick(stop × (1 − recommend.gtt_limit_offset_pct/100))`
  - `last_price` = `avg_entry`, so `stop < last_price < target` holds by construction. This deliberately differs from Kite.
- **Always read `broker.gtts()` status first.**
  - TRIGGERED with the leg working ⇒ EXIT_WORKING. Never re-place, never send a second exit, and cancel any working entry.
  - A leg still unfilled after `paper.exit_working_timeout_min`, or at close − 10 min, is cancelled (CANCELLED verified).
  - A TRIGGERED GTT whose leg is terminal is spent. Any open quantity re-enters the exit routine (Q4.6).
- **Gap-through.** A (re)arm with LTP ≤ stop is replaced by the exit routine (`stop_gap_through`).
- **verify_all** runs after every qty-changing update and in each reconcile pass, and skips `PENDING_EXIT`. It re-places a missing ACTIVE GTT up to 3 times, then sets `PROTECTION_FAILED` ⇒ the exit routine plus `PAPER_ALERT`.
- **Exit retries.** An exit order that ends terminal (CANCELLED, REJECTED or LAPSED) with an unfilled residual and no platform cancel intent is retried up to 3 times in-session. Paper lapses arrive as CANCELLED (`paper/broker.py:1097`).
  - After the third failure the position is `PROTECTION_FAILED` with a `PAPER_ALERT`.
  - The exit is retried at the next session's first in-session tick.
- **In-session only.** Exits are placed only in session; outside it they queue for the first in-session tick.
- *Tests:*
  - **GTT and leg mechanics:** delete refused on a triggered GTT; a leg resting below its limit; a lapse; re-arm below the stop; a reconcile during a leg fire; a rejected exit; a residual after a leg fill; a spent GTT with open quantity reaches a MARKET sell; an entry still resting when the GTT triggers
  - **Real-row isolation:** a same-symbol real `origin='recommended'` row stays byte-identical
  - **R3 property:** within N ticks an open paper position has an ACTIVE GTT or is `PENDING_EXIT` with a working exit; otherwise `PROTECTION_FAILED` is handled
  - **Chaos classification:** each `PHASE3_GATED` clause is either
    - rewritten for paper: case 11 (a fired leg rejected or unfilled ⇒ leg timeout ⇒ exit routine, `gtt_failure_exit`), and case 17's GTT clause (an ex-date in an unobserved window ⇒ VOID), or
    - kept skipped with a new reason: lifecycle hooks reserved for Phase 4 (case 15's reconcile clause), broker-resident R3 (case 03), or real-broker-only (cases 10, 12), or
    - MIS ("MIS out of paper scope")

**Q4.6 ExitManager** (`design`)
- *Change:*
  - **One exit routine,** `exit_position(position, reason)`, for every live cause: time, equity-floor flatten, `PROTECTION_FAILED`, overdue, residual. Steps:
    1. If the position already has a non-terminal exit order, send nothing more.
    2. Read `gtts()`.
       - An ACTIVE GTT is deleted and the deletion verified.
       - A TRIGGERED GTT whose leg is still working hands over to EXIT_WORKING.
       - A spent one is skipped.
    3. Cancel any working entry.
    4. Mark the position `PENDING_EXIT`.
    5. Send a MARKET sell of the open qty.
    6. On close, assert that no ACTIVE GTT remains.

    Whenever an exit order or a leg ends with quantity still open, the position re-enters the routine.
  - **Time exit:** at `session(exit_session).close − paper.exit_minutes_before_close`.
    - A pending `exit_session` is recomputed on each tick.
    - A missed exit goes at the first in-session moment (`TIME_STOP`, overdue noted).
    - A calendar raise is only logged and counted; Q0.4 alerts.
  - **Corporate actions.** `corp_actions_fn` reads `corp_actions` through the store (`store.get_corp_actions`, `marketdata/store.py:1896`; the scan's reader is `ops/scan_context.py:349`). It never uses the `calendar.ex_dates` stub, which always returns `[]` (`core/calendar.py:174-176`).
    - **Entry:** the paper branch refuses an entry when a `UNADJUSTED_KINDS` ex-date falls in (entry session, exit_session], or when any future one is announced while `exit_session` is pending.
    - **Open position:** session prep (Q4.7) runs the daily check, and `paper_tick` re-checks every 15 min in session.
    - **Late find:** an ex-date found for today voids an open position at once (Q4.4). A position that already closed on the ex-date is re-priced at its last carried pre-ex mark and re-labelled `VOID`, which removes the phantom P&L.
  - **Equity-floor flatten** (Q4.8) runs the exit routine for every open paper position. Nothing else flattens paper; a real kill or real halt never does.
  - **Scheduling:** one `paper_tick` interval job: 30 s, `guard=False`, self-gated to sessions for exits, a new `_arm_live_jobs` keyword, and active only while the current session's prep has completed and none is due or running (Q4.1). It is not a JobSpec. It drives:
    - time exits, overdue handling and the 15-min corporate-action re-check
    - the paper equity snapshot and halts (Q4.8)
    - the close − 5 min reconcile (Q4.7)
    - persisting `last_observed_at`
- *Tests:*
  - **Time exits:** the time exit; the triggered-GTT race; a time exit while a partial entry rests.
  - **Corporate actions:** entry refusal using a real `corp_actions` row, bounded below by the entry session; a late find voids an open position; a close made on the ex-date is re-labelled.
  - **Other paths:** flatten while an exit is already working; overdue; calendar raise.
  - **Scope:** every selection is paper-scope. A same-symbol real position stays byte-identical and gets no broker order.

**Q4.7 Session prep and Reconciler** (`design`)
- **Session prep.** The bridge calls it (Q4.1) before forwarding the first tick of each session, and the first tick after more than 5 unobserved session minutes. Deadline 120 s; each step guarded.
  1. **Catch-up** for every open paper position, `PENDING_EXIT` included, over the unobserved session minutes in (`last_observed_at`, prep start]:
     - A `UNADJUSTED_KINDS` ex-date in (window start, today] ⇒ VOID (Q4.4), without walking. Kite's minute and daily candles come corporate-action adjusted at fetch (A11, `marketdata/backfill.py:12`), so a walk across an ex-date would compare adjusted prices with unadjusted stops.
     - Otherwise `bars_fn` backfills 1m bars for those symbols over the unobserved session minutes.
       - It calls the coverage-checked `BackfillJob.warmup_gap` once per session segment, because its coverage check assumes a within-session range (`marketdata/store.py:1495`).
       - It reads the bars back with `aget_bars_1m`.
       - This also covers names outside the watchlist; the post-login backfill covers only the watchlist (`ops/post_login.py:217, 232`).
     - The walk ends at prep start − 2 min (the backfill's `confirm_until` convention), because the forming minute has no candle yet. That tail lies inside the 5-minute tolerance.
     - `replay_fn` walks those bars (Q2.1, start state "open"). An exit ⇒ `close_bookkeeping` at its price and reason (basis `downtime_replay`).
     - A minute with no candle inside a successful fetch had no trades (`marketdata/backfill.py:292-294`). Only a symbol whose fetch fails is VOIDed (basis `downtime_uncovered`).
  2. **Corporate-action check** for survivors: an ex-date today ⇒ VOID (Q4.4; the bookkeeping close removes the GTT first).
  3. **Pending exits:** survivors still `PENDING_EXIT` re-enter the exit routine (basis `restart_late`).
  - A step that fails or runs out of time VOIDs the positions it did not finish, logged and counted. Forwarding always resumes afterwards.
  - On completion, prep sets `last_observed_at` to its window end (Q4.1).
- **Reconcile passes,** after the first forwarded tick per paper symbol each session, and at close − 5 min. Each compares OMS non-terminal paper orders and the `gtts` rows with the broker book:
  - a non-terminal row the broker reports differently is fed through `apply_update` (R5; `paper/broker.py:48-50`), then counted
  - tagged legs are adopted
  - `PENDING_EXIT` is respected
  - unprotected positions go to ProtectionManager
  - true orphans are cancelled

  Mismatches are logged and counted, never raised into the lifecycle. Not a SessionLifecycle hook.
- *Tests:*
  - **Prep triggers:**
    - a boot before login ⇒ prep runs on the first tick after login
    - a mid-session restart with a valid token ⇒ prep on the first tick
    - host sleep inside a running process ⇒ prep on the first tick after the gap
  - **Catch-up outcomes:**
    - a dip through the stop during the gap that recovers ⇒ bookkeeping stop exit
    - a bonus in the window ⇒ VOID, no walk, no GTT fire
    - a symbol outside the watchlist is backfilled
    - a tradeless minute inside a successful fetch is not a gap; a failed fetch ⇒ VOID
    - a prep on a session's first tick doesn't void anything for the forming minute
    - a prep timeout ⇒ VOID, and forwarding resumes
  - **Reconcile and isolation:**
    - a lost postback re-applied by the reconcile
    - a raising prep or reconcile leaves the lifecycle unaffected
    - a same-symbol real row untouched

**Q4.8 Paper equity and halts** (`design`)
- *Change:*
  - **Tracker:** the paper-scope `ExposureTracker`, using the `limits.yaml` capital base, scoped to the epoch.
  - **Tick work:** `paper_tick` writes `paper_equity_snapshots`, evaluates `evaluate_floors` and `evaluate_day_loss`, and latches `paper_halts` only. It never runs inside `equity_tick`.
  - **Rungs mirror §7.1** (`IMPLEMENTATION_PLAN.md:1591-1596`):

    | Rung | Paper action |
    |---|---|
    | daily soft | entries blocked for the rest of the day |
    | daily hard | CLOSE_ONLY; CNC keeps its GTT (no flatten) |
    | weekly drawdown | CLOSE_ONLY |
    | equity floor | exit all (the Q4.6 routine) + CLOSE_ONLY |
    | cumulative floor | paper KILLED; the breach always includes the equity-floor rung (`risk/exposure.py:388-396`), so its exit-all runs first |
    | consecutive losses | the paper gate rule, through the paper tracker |

  - **Clearing:** daily causes auto-clear the next session. The others latch until a reset.
  - **Reset,** requested by `/paper reset` (Q4.0) and persisted as `reset_requested_at`:
    - every open paper position runs the Q4.6 routine
    - on the first flat book after the request, a new epoch starts (`epoch_started_at`), paper halts clear and the broker's available margin is re-seeded
    - the request survives a restart
  - **Effective paper entry state** = the worse of the real risk state and the paper halts. The real state only matters for proposals already in flight (§1.5).
- *Tests:*
  - **a paper floor breach leaves `mode_state`, `risk_state_causes` and `kill_state` untouched**
  - a raising paper tracker still lets the real `apply_*` run
  - a reset after a floor breach exits the book and opens the epoch once flat, also across a restart, and does not re-latch
  - auto-clear
  - a real non-NORMAL state during an in-flight proposal blocks the paper entry

**Q4.9 Pipeline paper branch (D1)** (`design`, high scrutiny)
- *Change:*
  - Split `_gate_and_persist` into `_persist_proposal` and `_evaluate_and_persist_verdict(action, ctx_builder, is_paper)`.
  - Restructure `_evaluate_forward` so the real branch never returns out of the method. Today it returns early on `owner_approval_required` and on non-approve verdicts (`ops/pipeline.py:2182-2186`).
  - **`_paper_branch`** runs after any real outcome **except** analyst no-action, analyst failure, incoherence, MIS, or a real `GateContextTimeout`. That last proposal must stay a real orphan so the WO-24c alert fires. The branch:
    - evaluates the same proposal with the paper builder (`is_paper=1` verdict)
    - applies the corporate-action entry refusal
    - on approve or shrink, calls `OrderManager.submit`
  - New optional constructor kwargs: `paper_ctx_builder`, `paper_submit`, `paper_enabled_fn`. None means paper is off, so existing tests are unchanged.
  - Paper failures are caught and logged and never touch rec delivery. Re-anchoring (Q1.2) happens before both gates.
- *Tests:* a real reject still lets paper evaluate; paper off; paper rejects while real approves; MIS refused; a real gate timeout ⇒ no paper verdict; zero LLM calls added.

**Q4.10 Paper notifications (D4)** (`impl`)
- *Change:*
  - **Fills:** paper fills reuse the existing `FILL` kind (non-critical), with a "PAPER" title and `is_paper` in its data. Update the `FILL` docstring (`catalog.py:48-50`).
    - Entry completion: qty and average price; dedupe `paper_fill:<order_id>`.
    - Position close: price, P&L, reason and basis; dedupe `paper_close:<position_id>`.
  - **`PAPER_ALERT`:** a new kind, `severity='critical'`, for protection failure, including a failed construction (Q4.1). Dedupe `paper_alert:<position_id or "construct">:<attempt>`.
  - **Everything else** (halts, reconcile mismatches, voids, late corporate actions) goes to the log, `/paper status` and the nightly `paper:` line: "paper: day ₹X · since <epoch> ₹Y · open N · halts …". If the line fails to compute it is omitted, and the real summary still sends.
  - The paper path never calls the real alert sink.
- *Tests:* catalog render; criticality; dedupe; the summary line with and without activity.

**Q4.11 Paper surfaces** (`impl`)
- *Change:*
  - `/positions` gets a paper section.
  - Dashboard: Decisions shows the real and paper verdicts; Positions shows the paper tag.
  - `/why` labels a paper verdict.
  - The paper halves of `/scorecard` and the weekly summary.
- *Acceptance:* as Q1.12, plus `test_api_routes.py` for the scorecard's paper shape.

**Q4.12 End-to-end replay** (`design`)
- *Change:* `ReplayHarness`, on the paper topic, drives proposal → paper gate → OMS → fill → GTT → exit through the real composition. A `KiteClient` double fails the test on any order method. Pin the golden digest and run the R3 property test.
- *Acceptance:*
  - The full suite and `pytest -m chaos` pass inline, outside 09:15–15:30.
  - Then the multi-angle branch review, then the owner's review of the order-path diff.
  - Then deploy with `subsystem_enabled: false`.

**Q4.13 Paper go-live** (`mgr`, owner sign-off)
- *Change:* set `paper.subsystem_enabled: true`, restart outside market hours, then `/paper on`.
- *Checks for the first 5 paper sessions:*
  - zero `PAPER_ALERT`
  - every paper fill has an ACTIVE `gtts` row within 60 s (query given)
  - `SELECT count(*) FROM orders WHERE COALESCE(is_paper,0)=0` stays 0
  - zero Kite order/GTT calls and zero `OrderSurfaceViolation` in the engine log (grep given)
  - session prep ran once per session, plus once after each gap, with outcomes logged
  - a daily WORKLOG evidence line
- The floor-isolation evidence is Q4.8's test plus the Q4.12 replay, never a production breach. The owner signs off.

**Q4.14 Paper RUNBOOK section** (`impl`, manager review)
- *Change:* a RUNBOOK paper section covering:
  - what `/paper on|off|reset` do
  - the halt-rung table, with an owner action per rung
  - start behaviour: construction restore (Q4.1) → session prep on the first tick (Q4.7) → reconcile
  - **while the engine is down or deaf no paper stop fires; on the next tick the catch-up applies what the bars show, or voids the position**
- Command rows and COMMANDS entries already land with Q4.0.

### M5 — Research (S3; runs alongside M1–M4 once Q2.1 is merged)

**Q5.1 Research snapshot** (`mgr`)
- *Change:*
  - `scripts/research_snapshot.py`, run during one evening engine stop. Beforehand: re-read the clock, check the log tail, confirm no in-flight jobs.
  - It attaches the source with DuckDB `READ_ONLY`. Never `MarketStore.open()`, which runs DDL (`backtest_hi52.py:127-129`).
  - It copies the tables the imported study helpers actually read, enumerated in the script from those helpers: `bars_1d` (with "NIFTY 50"/"INDIA VIX"), `corp_actions`, `universe_daily`, `insider_trades`, `earnings_calendar`, `results_filings`, `shp_quarterly`.
  - Output: `data/research/market_<date>.duckdb`.
- *Acceptance:* row counts recorded and matching the source; engine downtime under 5 min; boot verified (no `migration_failed`, no freeze); COMMANDS and WORKLOG entries.

**Q5.2 Pre-registrations** (`mgr`)
- *Change:* three §6.1 paragraphs, **committed before any study script runs** (proved by git log order). Each fills this template, taken from `IMPLEMENTATION_PLAN.md:1391`:
  1. Header, with "written BEFORE the run, evidence only, wires nothing".
  2. Motivation and the live cost measured.
  3. Population, with the survivorship-proxy label.
  4. Window and held-out segment.
  5. Signal builder imported, not re-implemented.
  6. Corporate-action / unadjusted-history veto.
  7. Fill convention, with its optimism caveat carried verbatim (brk20 fills on touch).
  8. Horizon and anchor: from the fill, with any deliberate divergence stated.
  9. Stop/target anchor per arm.
  10. Sizing: equal notional (₹20,000), percent returns, no compounding; C1 is also reported at the §7.1 sizing notional.
  11. Arms, with reference arms marked.
  12. Family N, pinned before any run and independent of run order:
      - one trial = one arm on one strategy's population, and each strategy is its own family
      - extra horizons and the second notional are diagnostics, not trials
      - a reference arm that re-measures an already-counted construct adds nothing
  13. CPCV: purge = embargo = 20; `fold_pass_min` from `validate.py`'s N table at the pinned N: 60% for N ≤ 10, 70% for 11–30, 80% above 30 (`validate.py:107-116`).
  14. Geometry first: cost vs range with the WO-3 margin floor, printed before any signal-quality claim.
  15. Point-in-time: every new filter reads only bars up to the signal-session close.
  16. Decision rule, including `neither`, plus the 2026-09-12 reporting amendment (median net > 0).
  17. Artefact names and run commands.
  18. What would change live, and who approves it.

  The three studies:
  - **C1 — exit geometry.**
    - Populations: brk20 V2-N5 fills, hi52 v2, ins events.
    - Arm (i), shipped geometry: the stop is anchored at the research fill (next open). Live's analyst-LIMIT/LTP anchor cannot be reproduced offline, and the paragraph says so.
    - Arm (ii), time only, T+20: a **reference** arm. It re-measures the registered construct and adds no trial. It is not deployable, because §7.1 cannot size it without a stop.
    - Arm (iii): time plus a 2.5×ATR14 catastrophe stop.
    - brk20 is reported at both the D2 hold and the registered `close(fill+20)`.
  - **C2 — regime filter.**
    - The rsi2 rule, read at the signal session over completed index closes.
    - The same admitted events are split on/off: a matched cohort, with no regeneration.
    - The September 2026 down-leg is reported separately, with a low-power caveat.
  - **C3 — selection filters.** "Expect null." Five filters on the hi52 and brk20 populations, deflated together:
    - RS63 vs NIFTY 50
    - trend template
    - ATR contraction
    - Stage-2
    - acc/dist days

**Q5.3–Q5.5 Study scripts** (`design`)
- *Change:* `scripts/backtest_exit_geometry.py`, `backtest_regime_filter.py` and `backtest_selection_filters.py`.
  - They import the live builders and helpers.
  - They use `exit_sim` with research conventions (bar positions; the registered fill rule).
  - They call `cpcv_splits`/`promotion_decision` directly. **No `ValidationPipeline`** (it writes `param_sets`).
  - They read only the snapshot and run only outside 09:15–15:30.
  - Output: JSON+MD under `data/reports/`; prints are ASCII-only.
- *Tests:* the arm definitions; a `--max-symbols` smoke run; brk20 baseline reproduction against its registered report.

**Q5.6 Record and decide** (`mgr`)
- *Change:* append the RUN records to the paragraphs, plus WORKLOG and memory.
- Any live change is separate, owner-approved work:
  - A geometry change needs a re-derived `expected_edge_pct` and its pins.
  - A regime gate is reject-only, with its threshold in `limits.yaml` via the owner reseed and `DOCUMENTED_ENTER_RULES` updated.

## 3. Delivery

**3.1 Dependencies (task ← prerequisites; acyclic)**

| Task(s) | Prerequisites |
|---|---|
| Q0.2, Q0.3, Q0.4 | Q0.1 |
| Q1.1 | Q0.2, Q0.3 |
| Q1.2, Q1.3, Q1.4, Q1.6, Q1.10 | Q1.1 |
| Q1.7, Q1.9 | Q0.3, Q1.3 |
| Q1.5 | Q1.2, Q1.3, Q1.4, Q1.7 |
| Q1.8 | Q1.7 |
| Q1.11 | none |
| Q1.12 | Q1.5, Q1.9 |
| Q2.1 | Q0.2 |
| Q2.2 | Q0.3 |
| Q2.3 | Q1.1, Q2.1, Q2.2 |
| Q2.4 | Q2.3 |
| Q2.5, Q2.7 | Q2.4 |
| Q2.6 | Q1.6, Q1.7, Q2.3 |
| Q3.1 | none |
| Q3.3 | Q0.3 |
| Q3.2 | Q3.3 |
| Q4.0 | Q3.3 |
| Q4.2 | Q3.1, Q3.3 |
| Q4.3 | Q4.2 |
| Q4.1 | Q4.0, Q4.2, Q4.3 |
| Q4.4 | Q4.3, Q3.2, Q1.1 |
| Q4.5 | Q4.4, Q1.3 |
| Q4.6 | Q4.5, Q0.2 |
| Q4.7 | Q4.1, Q4.4, Q4.6, Q2.1 |
| Q4.8 | Q3.2, Q4.0, Q4.6 |
| Q4.9 | Q1.2, Q3.2, Q4.0, Q4.1, Q4.3, Q4.6, Q4.8 |
| Q4.10 | Q4.1, Q4.3–Q4.8 |
| Q4.11 | Q4.10, Q1.10, Q2.4, Q2.6 |
| Q4.12 | Q4.0–Q4.11 |
| Q4.13, Q4.14 | Q4.12 |
| Q5.1 | none |
| Q5.2 | Q2.1 |
| Q5.3–Q5.5 | Q5.1, Q5.2 (committed), Q2.1 |
| Q5.6 | the runs |

Each milestone deploys on its own. M3 changes no behaviour, and M4 ships with `subsystem_enabled: false`.

**3.2 Branches and tests**
- One `wip/<milestone>` branch per milestone, never a detached worktree.
- The full suite and `pytest -m chaos` run inline, never in the background, and outside 09:15–15:30.

**3.3 Deploy and rollback**
1. **Backup.** Stop the engine. Before the first boot on 0015 and on 0016, back up `state.db` to `data/backups/pre_00NN.db` with Python's online backup API. This is the method `_snapshot_backup` uses (`ops/main.py:3971`); the one-liner goes in COMMANDS.
2. **Rollback.** Migrations are additive and each runs in one transaction. Rollback = revert on the `wip` branch + restart. Never drop columns. A failed migration is recovered from its `pre_00NN` backup.
3. **Rollback floor.** Once `/paper on` has run, never roll back below the M3 tip. Older code counts paper rows as real money (`risk/exposure.py:43`) and could latch real floors. Use `/paper off` or `paper.subsystem_enabled: false` instead.
4. **Restarts** happen only outside 09:15–15:30: re-read the clock and the log tail, stop and start in one command, then verify the boot.
5. **Deploy steps** for each milestone go into COMMANDS.

**3.4 State.db invariant (every new writer)**
- All SQLite access runs on the event-loop thread.
- No `await` between `BEGIN` and `COMMIT`; there is one shared autocommit connection (`core/db.py:26`).
- `store.arun` is for DuckDB only.
- PaperBroker callbacks never touch SQLite.
- Review greps for `await` inside `with transaction(`.

**3.5 Jobs**
- **Registration:** `JOB_*` constant; `JobTimesCfg` field and the `jobs:` key; PHASE2 ids; the `build_job_registry` JobSpec; the RUNBOOK daily-job table; the `test_ops_main_wiring.py` tables; the chaos rigs (cases 13, 15, 16, 17, 19).
- **Post-arm:** jobs that never bear load for entries (`time_exit_check` at order 5, `rec_outcomes`, `weekly_summary`) go in `POST_ARM_JOB_IDS`.
- **`fire_day`:** every `fire_day` includes "is a regular trading session", because the catch-up uses it instead of the trading-day test. The live runner honours it from Q1.6.
- **Data not ready:** a job whose upstream data isn't ready returns `unfinished` (no watermark, replayed later), never `ok=False`.
  - It does so only while that upstream job is unrecorded or failed. Once upstream is ok or skipped, it proceeds with what exists.
- **Interval ticks:** `paper_tick` and `protection_reminder_tick` are new `_arm_live_jobs` keywords, wired and tested at `test_ops_main_wiring.py:628-690`.
- **Alerting:** every job follows E5 (one alert per failing streak, cleared on success).

**3.6 Settings**

| Block | Knobs | Added by |
|---|---|---|
| `clock:` (`ClockCfg`, `core/config.py:132-134`) | `calendar_horizon_alert_sessions` (40) | Q0.4 |
| `recommend:` (new, `extra='forbid'`) | `gtt_limit_offset_pct` (1.0); `veto_window_sessions` (3); `protection_first_reminder_min` (30); `reminder_window_start` ("08:00"); `reminder_window_end` ("22:00") | Q1.3, Q1.7, Q1.8, Q1.9 |
| `paper:` (new, `extra='forbid'`) | `subsystem_enabled` (false), `seed`, `exit_minutes_before_close` (10), `exit_working_timeout_min` (5) | Q4.0 |

`Settings` silently ignores unknown keys, so each block gets a test that loads the shipped file through its real consumer.

**3.7 Test pins that move** (update them in the same commit as the behaviour)
- `test_telegram_commands.py:925-937, 970-972, 1045, 1081`
- `test_reco_pipeline.py:1185, 1377-1378, 1487, 1922, 3334`; :1922 is rewritten against `time_exit_check` with the same assertion
- `test_reco_pipeline_wo24.py:371`
- `test_ins_crossings_job.py:557-575`
- `test_case05_llm_failure.py:466`
- chaos case 16 `:105`
- `test_order_surface.py`, chaos `case06` (guard copy at `:130-148` and `:171-174`), `case04:220`, `test_kite_client.py:94-95`
- `test_import_graph.py:36-40`
- `test_oms_state.py:112, 176-205`
- `test_ops_main_wiring.py:247, 262-271, 628-690`
- the chaos lifecycle rigs and the `PHASE3_GATED` cases
- `test_migrations.py` `EXPECTED_TABLES`

## 4. Risks and mitigations

| Risk | Mitigation |
|---|---|
| A real order through any path | Q3.1 (a)–(f); paper topic; Q4.12's failing `KiteClient` double; Q4.13 production checks |
| Paper contaminating real money state, or real money censoring paper | `scope_sql` and the literal-ban test; both-way invariants; separate `paper_halts`; paper ledger `rec_id=NULL`; real-only `held_symbols`; the §3.3 rollback floor |
| Double exit, a short, inverted or lost protection | Q4.5 state machine; one exit routine, one working exit at a time, with spent-GTT handling; bookkeeping closes clear the GTT and working orders first; oversell nets working sells; Kite-like `delete_gtt`; `last_price = avg_entry`; gap-through ⇒ exit; leg adoption |
| Unobserved time: restarts, host sleep, feed outages | Q4.1 construction (counters, legal edges, GTT restore) and gating; Q4.7 session prep (catch-up on backfilled bars, else void; ex-date void without walking) |
| Corporate-action phantom fills | Store-sourced ex-dates; entry refusal; prep check before forwarding; 15-min re-check; late-find void or re-label |
| Paper fills systematically worse or better than live | Q4.2(d) marketable-LIMIT rule; the catch-up shares `exit_sim` conventions with `rec_outcomes` |
| Paper blocking real stops or boots | No lifecycle hooks; prep runs on the bridge with a deadline; never raises into the lifecycle; separate ticks |
| Untrue card text | Data-presence conditions; dated wording; measured-exit label; pending forms; manager review of every string |
| `rec_outcomes` read as P&L or tuning input | "Hindsight" labels; never read by scanners or thresholds; brake-table row-count invariant |
| Calendar horizon | Q0.4 monitor (the only alert, once per horizon value); "pending" handling in every consumer |
| Research multiplicity, low power, non-reproducible baselines | Q5.2 template with pinned family N; research conventions in `exit_sim`; baseline-reproduction test |
| Quota and market hours | No new LLM calls; fan-outs, full suites and studies outside 09:15–15:30 |

## 5. Out of scope (deferred, stated)

- Live order routing; the GTT IP-check probe.
- MIS paper and SquareOffScheduler.
- The Telegram channel split and the B2 ops digest (D12).
- C4 late-session entry (D12).
- `GET /why` and the card's regime tag (D11).
- Inline buttons; remote dashboard access; charts; delivery %.
- Following or forward-testing QuantSync.
- Re-litigating O15.
- Any protected-store change without an owner reseed.
