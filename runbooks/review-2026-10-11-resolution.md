# Resolution of review-2026-10-11-stale-logic.md

Every finding was re-verified against the working tree on 2026-10-11 before any edit (the review
was treated as claims, not instructions). "fixed" = the minimal edit that makes code/docs true;
behaviour changes and open owner decisions are listed in the WORKLOG entry of the same date.
Severity column: the review's own grouping (deliberate/refuted = its verifier's non-defect verdicts,
re-checked here).

| # | Severity | Outcome | Note |
|---|---|---|---|
| 56 | high | fixed | src/engine/risk/gate.py:1330-1346 new _SURVEILLANCE_PREFIX + _surveillance_codes(rows, symbol), which reads the surveillance_* exclusion codes off the universe_daily row; src/engine/risk/gate.py:1465-1466 build() fetches universe_daily once and feeds both _… |
| 58 | high | fixed | src/engine/broker/rate_limiter.py:22-24 module docstring: added the intent="read" lane; src/engine/broker/rate_limiter.py:69 Intent = Literal["entry", "risk_reducing", "read"] |
| 87 | high | fixed | harness sends tools=[] (allowed_tools only auto-approves) + extended deny list; live pinned-CLI call: tools offered = [StructuredOutput] only; 2 tests added |
| 1 | medium | fixed | config/agents.yaml:21-22 the overflow comment now reads 'overflow_cap_usd is NOT enforced by any code: the governor caps only at weekly_credit_usd (DG4). Implement a cap before enabling overflow.' |
| 11 | medium | fixed | config/settings.yaml:203-205 now says the key is the 'HeadlineClusterer near-duplicate assignment window ONLY (calendar days)' and that the cat digest horizon is the envelope.yaml row cat.max_event_age_days (sessions); src/engine/core/config.py:309-311 same… |
| 13 | medium | fixed | src/engine/core/config.py:371 InsCfg.stop_pct default 6.0 -> 5.0; config/settings.yaml:280-283 now reads 'At §7.1 sizing (notional <= equity x 2% / (2.0 x 5%) = equity/5, ₹8,000 at the ₹40,000 base) the CostModel CNC round trip is ~0.43% => edge multiple ~3… |
| 20 | medium | fixed | dashboard/src/lib/recs.ts:26-28; dashboard/src/ui/DayFold.tsx:4 |
| 23 | medium | fixed | FABLE_HANDOFF §4/§5: OMS live for paper via the autopilot; live Reconciler/AUTO unbuilt |
| 24 | medium | fixed | event log: 09-15 01:14 was a host reboot (SHUTDOWN control, EventLog 6006/6005); FABLE_HANDOFF §8 + memory corrected |
| 26 | medium | fixed | CHAOS_DRILLS skip legend lists the six conftest reasons; billing-error pointer names the harness branch, not a line; status counts re-run 2026-10-11 |
| 27 | medium | fixed | runbooks/CHAOS_DRILLS.md table: split into rows '1,2a,2b,7,8,9' (MIS out of paper scope), '10,12' (real-broker-only), new case 11 row (test_case11_gtt_fired_rejected_or_unfilled.py); case 3 cell -> broker-resident R3; case 5 -> MIS out of paper scope (test_… |
| 28 | medium | fixed | runbooks/COMMANDS.md:184-195 replaced the pending O17 block with 'Nothing pending; last applied: O17 (e148a4e, 2026-09-24, owner)' and a generic <patch>/<file> command shape |
| 30 | medium | fixed | runbooks/COMMANDS.md:~386-387 'equity/6 since O17 (2.0x; equity/7.5 at 2.5x before 2026-09-24)', example now about ₹6,543 (dropped the 0.54% figure rather than re-derive it); runbooks/COMMANDS.md:~392 'hi52.expected_edge_pct (1.53 since O17; 1.47 before)' |
| 33 | medium | fixed | runbooks/RUNBOOK.md universe_build row now cites universe.index_name (O18) and the NIFTY 500 core; runbooks/RUNBOOK.md index-membership checklist item rewritten with the total-market chain plus a separate core-index chain |
| 37 | medium | fixed | runbooks/RUNBOOK.md corp actions checklist item now names NSE_CORP_ACTIONS_URL with the camelCase URL and the date-window note |
| 42 | medium | fixed | scripts/hi52_forward_verdict.py:49-52 now gives 2.0× since O17 (2.5× at registration), equity/6, and ≈₹6,543 -> 0.4769%; scripts/hi52_forward_verdict.py:63-64 now gives ₹6,000 ⇒ 0.4982% ⇒ 2.0336 − 0.4982 = **1.53** (pre-O17 ₹4,800 ⇒ 1.47) |
| 43 | medium | fixed | scripts/nssm_install.ps1:27-39 (design decision 2); scripts/nssm_install.ps1 $RunDir/$Sentinel vars, Install 'Restart' host line and (2) comment, removed AppEnvironmentExtra (4) block, fallback comment, Stop-Engine comment, Show-Status sentinel block |
| 47 | medium | fixed | scripts/smoke_test.py:~237-262 (_resolve_engine_cli, check_engine_cli); scripts/smoke_test.py main(): check renamed 'engine Claude Code CLI --version (D1)' |
| 50 | medium | fixed | src/engine/api/app.py:55-56 comment now names <repo>/dashboard/dist as the build target and web/dist as the notifications page |
| 52 | medium | fixed | runbooks/RUNBOOK.md:~29 removed '(or paste it into the dashboard)' |
| 54 | medium | fixed | src/engine/broker/instruments.py:45-47 header now describes the C7 join refresh() performs; src/engine/broker/instruments.py:104 dropped 'Phase-0 skeleton' |
| 55 | medium | fixed | src/engine/broker/instruments.py:117-118 (same hunk as #54) |
| 57 | medium | fixed | src/engine/broker/kite_client.py:27-29 replaced 'Phase-0 skeleton' paragraph with the D8 orders_enabled=False status and the unwired C6 margins_fn; src/engine/broker/kite_client.py:283-284 margins() docstring: only the daily token probe calls it; pre-place… |
| 61 | medium | fixed | src/engine/core/calendar.py:10-11 module docstring: ex_dates is unimplemented (raises); consumers read MarketStore.get_corp_actions; src/engine/core/calendar.py:197-203 ex_dates now raises NotImplementedError("use MarketStore.get_corp_actions; implement bef… |
| 64 | medium | fixed | src/engine/core/config.py:417-422 now reads 'equity/6 ... the 2.0× overnight gap multiplier (O17; 2.5× before it) ... ≈₹6,667 ... ≈₹6,543 ... ₹6,000 ... 2.0336 − 0.4982 = 1.5354 ⇒ **1.53** ... (pre-O17: ₹4,800, 1.47)' |
| 67 | medium | fixed | scripts/dpapi_set.py:47 label text; scripts/nssm_install.ps1:9-10, decision 4, fallback comment |
| 68 | medium | fixed | src/engine/datafeeds/corp_actions.py:1-9 header now cites settings.jobs.corp_actions_ist (20:15 since 2026-07-24) and lists the live consumers, leaving only GTTManager.adjust_for_ex_date and the NSECalendar.ex_dates startup scan as still to land; src/engine… |
| 70 | medium | fixed | src/engine/datafeeds/earnings_calendar.py:4 now 'Two consumers (live in Phase 2; the features engine and pre-open planner also read it):' |
| 78 | medium | fixed | [prompt text] src/engine/intelligence/context.py:67-72 _WATCHLIST_KEYS drops confirm_trigger/invalidation/stop_band_*/target_band_*; src/engine/ops/preopen_planner.py:331-338 _watchlist_lines now renders only symbol grade event_type; docstring updated |
| 81 | medium | fixed | [prompt text] src/engine/intelligence/context.py:~519-521 _structural_absence_lines now ' rules (brk20, hi52, ins, cat, cat_reversal) deliberately originate for eligible symbols,' / ' watchlist or not.'; src/engine/intelligence/context.py:201 swing comment… |
| 82 | medium | fixed | src/engine/intelligence/governor.py:7-9 reworded as a governor PROXY with no console figure to reconcile; under-count is why the ladder trips below the credit |
| 92 | medium | fixed | src/engine/marketdata/bar_builder.py:126-127 SESSION_OPEN_IST comment: no override is passed; regular constants every day; special sessions not wired; src/engine/marketdata/bar_builder.py:131-132 SESSION_CLOSE_IST comment: same |
| 94 | medium | fixed | src/engine/marketdata/reconcile.py:3-6 module docstring: catch-up is driven by the JOB_RECONCILE job_runs DATE_KEYED watermark; reconcile_log is the per-(day, symbol) audit trail; has_reconcile_entry/reconciled_days have no production caller; src/engine/mar… |
| 97 | medium | fixed | src/engine/marketdata/store.py:32-35 module docstring: nightly JOB_TICK_COMPACT (engine.marketdata.tick_compact.compact_ticks, own connection) does the compaction; compact_tick_partitions has no production caller; src/engine/marketdata/store.py:2412-2416 co… |
| 103 | medium | fixed | src/engine/notify/telegram.py:338 summary -> 'Show quota-week-to-date LLM spend (Thu 14:00 IST window) per agent vs the weekly allocation and the degrade tier.' |
| 104 | medium | fixed | src/engine/notify/telegram.py:~437 _LedgerRec.valid_until comment; src/engine/notify/telegram.py:~1975 _rec_line docstring |
| 110 | medium | fixed | src/engine/ops/jobs.py:113-114 freeze-seam comment; src/engine/ops/jobs.py:251-252 registry=None docstring |
| 113 | medium | fixed | src/engine/ops/lifecycle.py:370-372 removed (comment + report.deferred_steps.append("equity_halt_ladder_reeval ...")) |
| 116 | medium | fixed | src/engine/ops/main.py:~221-223 _FALLBACK_CAPITAL_BASE_INR = Decimal("40000"); src/engine/ops/main.py:~593-600 fallback uses the constant; comment and warning hint reworded |
| 118 | medium | fixed | src/engine/ops/main.py:~265-266 PHASE2_JOB_IDS comment; src/engine/ops/main.py:~447-448 build_job_registry comment |
| 121 | medium | fixed | src/engine/ops/main.py:~4372-4374 _snapshot_backup docstring |
| 122 | medium | fixed | src/engine/ops/news_scoring.py:23-24 cadence sentence reworded (30 default, 60 at DG2+ or when news_analyst is over its own share); src/engine/ops/news_scoring.py:~129 'queue → the Tier-1 news analyst (model per agents.yaml) → news_clusters ...' |
| 127 | medium | fixed | pipeline.py comments state the proportional DG1+ cap; the pacing note compares cap × pacing with the owner-set window (live 09:30–14:30, so the cap binds) |
| 129 | medium | fixed | src/engine/ops/preopen_planner.py:37-38 parenthetical replaced with the 'none only when no review recorded' wording |
| 132 | medium | fixed | src/engine/ops/selftest.py:18-21 module docstring |
| 134 | medium | fixed | src/engine/paper/broker.py:7-9 docstring reworded per review; src/engine/oms/state.py:5-6 docstring now 'Nothing here places an order; it is data + one pure function (shared by the paper OMS and the risk gate's terminal-state check)' |
| 139 | medium | fixed | src/engine/risk/exposure.py:42-44 comment names the limits.yaml weekly_drawdown.rolling_sessions mirror; tests/unit/test_limits_engine.py:110-116 new test_weekly_drawdown_window_mirrors_the_signed_table (loads the real limits.yaml through LimitsEngine) |
| 140 | medium | fixed | src/engine/risk/exposure.py:84-85 OpenCounts → 'total / max_mis / max_cnc caps in limits.yaml'; src/engine/risk/exposure.py:119 capital_base comment → '(O1, capital_base_inr)' |
| 141 | medium | fixed | src/engine/risk/exposure.py:405 'equity < base × (1 − 15%)'; (plus the #140 hunks) |
| 143 | medium | fixed | src/engine/risk/mode.py:12-15 module docstring names RiskStateLatch as the single writer; the square-off-on-shrink callback is unwired; src/engine/risk/mode.py:161-163 set_risk_state docstring: durable setter only; route causes through set_cause/clear_cause |
| 150 | medium | fixed | src/engine/strategy/scanners/base.py:1 and :19-23 registry docs rewritten (four per-bar baselines register; brk20/ins/hi52/cat/cat_reversal enter via SignalPreScreen.admit); src/engine/strategy/scanners/base.py:~86 _candidate docstring: catalyst_ref unused… |
| 151 | medium | fixed | src/engine/strategy/scanners/base.py:~117-122 params_from_envelope docstring now says intended Phase-5 seam, NOT wired today; src/engine/strategy/scanners/__init__.py:~52-54 build_enabled_scanners docstring likewise |
| 152 | medium | fixed | src/engine/strategy/scanners/brk20.py:23-24 'owner-tunable via the §6.3 envelope' -> 'a frozen code constant: envelope.yaml has no brk20 rows today, and plan §6.1 lists them as future §6.3 rows'; src/engine/strategy/scanners/brk20.py:81 DEFAULT_PARAMS comme… |
| 155 | medium | fixed | [prompt text] src/engine/strategy/scanners/hi52.py:21-23, :27, :70-72 now say CORE (NIFTY 500) eligible subset, contrasted with brk20, and cite get_core_eligible_symbols (O18, 2026-10-08; the full eligible set 2026-09-10..10-08); src/engine/strategy/contrac… |
| 156 | medium | fixed | src/engine/strategy/scanners/ins.py:48 docstring 'default **6%**' -> 'default **5%**'; src/engine/strategy/scanners/ins.py:107 DEFAULT_PARAMS stop_pct 6.0 -> 5.0 |
| 160 | medium | fixed | src/engine/universe/leverage.py:5-9 UniverseBuilder is the only consumer; the instruments_daily mis_* columns stay NULL; dropped '[VERIFY]-marked URL'; src/engine/universe/leverage.py:34 endpoint marker replaced by 'Parsed live every session since 2026-07-1… |
| 162 | medium | fixed | src/engine/universe/surveillance.py:9-15 module docstring: indirect refusal, no direct gate seam; src/engine/universe/surveillance.py:67-71 SurveillanceLists docstring: unconfirmed ⊆ flagged(), nothing reads the field directly |
| 163 | medium | fixed | src/engine/universe/surveillance.py:17-20 'OPEN GAP (plan §2.6 step 5 / §4.4 job 5): that alert is unimplemented … Positions live in engine.oms.positions'; src/engine/universe/surveillance.py:101-102 new_entries docstring marks the OPEN GAP |
| 173 | medium | fixed | TEST_AGENTS on sonnet-5.5 (+effort), PINNED_CFG prices sonnet-5.5; haiku-5.5 asserts left to the other session's in-flight Haiku 5.5 change |
| 181 | medium | fixed | tests/unit/test_risk_gate.py:219-224 INS_ACTION comment: 66 is the plan's gap-unaware sizing, the gate cap is floor(400/12)=33, and INS_ACTION is an over-ask the gate shrinks |
| 2 | low | fixed | config/agents.yaml:44-46 now reads 'billed at this key from the 09-15 roster move until 2026-10-03 (now sonnet-5.5, same rate; this key prices historical rows)' |
| 4 | low | fixed | config/agents.yaml:96-97 added: '`triggers` and `schedule_ist` are documentary — no code reads them: call classes are code constants and fire times are settings.yaml jobs.* (preopen_planner_ist, nightly_review_ist).' |
| 5 | low | fixed | config/aliases.yaml:14-16 LLOYDSME note: appended that since O15 it is a NIFTY 500 member and the alias stays as a press short form; config/aliases.yaml:72-74 VIKRAMSOLR note: appended that since O18 it is a NIFTY Total Market member |
| 6 | low | skipped — owner item | valid (cat.confirm_vol_mult / cat.max_hold_days have no reader) but envelope.yaml is owner-signed; change via the protected-config flow |
| 8 | low | fixed | config/settings.yaml:40 '(HealthMonitor)' -> '(HeartbeatWriter thread, ops/heartbeat.py)' |
| 9 | low | fixed | config/settings.yaml:54 token_expiry_ist marked 'NOT read by code'; config/settings.yaml:56 instruments_refresh_ist -> 'NOT read by code; the schedule is jobs.instruments_ist' |
| 10 | low | fixed | config/settings.yaml:121-122 now reads '(universe.index_name: NIFTY Total Market since O18, 2026-10-08; NIFTY 500 under O15 before it)'; src/engine/core/config.py:164-165 same wording in the DataCfg comment |
| 12 | low | fixed | config/settings.yaml:229-231 filings header rewritten as research/validation inputs only, pointing at ins.threshold_inr; config/settings.yaml:234-235 insider_min_value_over_adv annotated 'NOT applied anywhere' |
| 14 | low | fixed | config/settings.yaml:477-478 now reads 'dry-run wall-time ESTIMATE only (scripts/backfill.py); real pacing is broker/rate_limiter.py _HISTORICAL_RATE_PER_S (A2), not this key'; src/engine/core/config.py:503 BackfillCfg comment mirrored |
| 15 | low | fixed | src/engine/core/config.py:560 corp_actions_ist default time(18,15) -> time(20,15); src/engine/core/config.py:564 deals_ist default time(18,45) -> time(20,30) |
| 16 | low | fixed | config/settings.yaml:532-533 annotated 'NOT read by code: the Sunday cadence is hard-coded (ops.main._is_sunday + its CronTrigger)'; src/engine/core/config.py:576 same annotation |
| 17 | low | fixed | config/settings.yaml:539 totp enabled annotated 'RESERVED: no code reads this key; flipping it has no effect'; config/settings.yaml:543 onedrive_backup_mirror annotated 'RESERVED: no code reads it; true does nothing' |
| 18 | low | fixed | config/universe/nifty500_seed.csv:1-2 header now reads 'NIFTY 500 CORE-index seed fallback (O18, ...)' and cites universe.core_index_source_url; config/universe/nifty500_seed.csv:7 index_cached.csv -> core_index_cached.csv |
| 19 | low | fixed | dashboard/src/copy/glossary.ts:377 added platform: { label: 'Engine', hint: 'Proposals the engine raises itself, such as the time exit.' } |
| 22 | low | fixed | dashboard/src/views/parts/PaperTables.tsx:24-30 ORDER_STATE keyed on SUBMITTED/ACKED/PARTIALLY_FILLED/CANCEL_PENDING/MODIFY_PENDING |
| 25 | low | fixed | README.md:8 ID list now O1...O18, R1...R10, plus D*, Q*, WO-*; README.md:~22-25 Status rewritten (Phase 2 RECOMMEND; Phase 3 OMS core/PaperBroker/ReplayHarness/paper autopilot, no AUTO) and capital figure ₹40,000 |
| 29 | low | fixed | runbooks/COMMANDS.md:~199 full-suite comment now 'several minutes; run inline, never two at once'; runbooks/COMMANDS.md:~30-40 milestone deploy: backup named pre_<next migration number>.db and 'git merge --ff-only <reviewed tip>' instead of wip/m2; rollback… |
| 31 | low | fixed | runbooks/RUNBOOK.md:~118-122 replaced the 'ships verified: false' warning with the verified-through-2026-12-31 status and the 2027.yaml to-do |
| 32 | low | fixed | runbooks/RUNBOOK.md:~135-139 header now says the table lists build_job_registry specs and names which times are constants / JobsCfg defaults; runbooks/RUNBOOK.md table: added catalyst_digest 08:35, token_check 08:40 (not a registry job, main.py:311), preope… |
| 34 | low | fixed | runbooks/RUNBOOK.md:~242 and ~261-264 replaced with `.\scripts\schedule_tasks.ps1`; added 'from the repo root' once |
| 35 | low | fixed | runbooks/RUNBOOK.md news checklist: rejected list now 'zeebiz (WAF 403)' plus business-standard adopted note; added BS RSS and NSE announcements checklist rows |
| 36 | low | fixed | runbooks/RUNBOOK.md surveillance checklist item: SMS list marked RETIRED, nothing to verify; index/seed lines fixed with #33 |
| 40 | low | fixed | scripts/backfill_filings.py:21-22 module docstring BSE caveat reworded; scripts/backfill_filings.py:~224 _seed_shp docstring reworded |
| 41 | low | fixed | scripts/backfill.py:75 'engine.ops.main (lines 101-102)' -> 'engine.ops.main' |
| 44 | low | fixed | scripts/nssm_install.ps1:8-10 (synopsis); scripts/nssm_install.ps1:~51-56 (decision 4) |
| 46 | low | fixed | scripts/setup_token.ps1:5-17 header; scripts/setup_token.ps1 Step 1 comment, Step 2 heading/comments/messages, Step 2b message, Step 3 comment, closing banner |
| 48 | low | fixed | scripts/smoke_test.py check_claude_agent_sdk docstring and knob_fields/required/assert; scripts/smoke_test.py module docstring claude-agent-sdk bullet |
| 49 | low | fixed | scripts/smoke_test.py _live_haiku_call options construction and return detail |
| 51 | low | fixed | src/engine/api/app.py:261-263 banner: real SQLite/collaborator reads that degrade to an empty shape when the collaborator is None; Owner-gated (R10) |
| 53 | low | fixed | src/engine/api/routes/__init__.py:1 docstring: reserved and empty; every route lives in engine.api.app (plus kite_callback) |
| 59 | low | fixed | src/engine/broker/session.py:121-124 login_url docstring: redirect_path is stored but read by nothing; route fixed at /kite/callback |
| 60 | low | fixed | src/engine/broker/session.py:245-246 mark_success docstring: only the boot probe calls it; _last_success is informational; src/engine/broker/session.py:239-240 token_valid docstring: dropped '_last_success /' from the behavioural signals |
| 62 | low | fixed | src/engine/core/config.py:109-113 LifecycleCfg docstring: 'land in Phase 1' replaced by the actual readers (ops/heartbeat.py, scripts/watchdog.py, which raises ENGINE_DOWN); src/engine/core/config.py:121 '--- Phase-1 active-period ...' -> '--- active-period… |
| 63 | low | fixed | src/engine/core/config.py:164-165; config/settings.yaml:121-122 |
| 65 | low | fixed | src/engine/core/config.py:560; src/engine/core/config.py:564 |
| 66 | low | fixed | src/engine/core/contracts.py:21-22 docstring now cites ``engine.intelligence.schemas.parse_and_stamp``; src/engine/core/contracts.py:82 comment: stamp_proposal() -> parse_and_stamp() |
| 69 | low | fixed | src/engine/datafeeds/deals.py:1 now says settings.jobs.deals_ist, 20:30 since 2026-07-24 |
| 71 | low | fixed | src/engine/datafeeds/filings_events.py:1-5 docstring rewritten: origination lives in ins_crossings, module mapping consumed live, FilingsEventBuilder and row_source are test-only helpers |
| 72 | low | fixed | src/engine/datafeeds/filings_pit_fresh.py:8-10 replaced with 'Never entry-blocking (§2.8 rule iii), but on the origination path ... §2.7 catalyst wiring does not read them.' |
| 73 | low | fixed | src/engine/datafeeds/filings_pit_fresh.py:~117 FRESH_WINDOW_DAYS comment reworded as the only missed-day recovery path; filings_pit_fresh.py fresh_url docstring, FilingsPitFreshJob docstring and the 'Surface 1' comment: 'rolling' -> 'same-day latest' |
| 74 | low | fixed | src/engine/datafeeds/filings_shp.py:53 comment now 'endpoints and shape live-verified 2026-07-16 (module docstring)'; src/engine/datafeeds/filings_shp.py:~66 deleted unused SOURCE_NSE = "nse" |
| 75 | low | fixed | src/engine/datafeeds/ins_crossings.py:154-155 comment reworded to CORE-eligible universe (O18); src/engine/datafeeds/ins_crossings.py:~167 reason now 'no core-eligible universe rows for the run day (no build, or core list unavailable)' |
| 76 | low | fixed | src/engine/datafeeds/news_pipeline.py:97-98 TODO replaced with 'Owner-reviewed in Phase 1 (G1 gate, 2026-08-04); further additions arrive only via the §5.5 owner-approval loop.' |
| 77 | low | fixed | src/engine/datafeeds/news_pipeline.py:482-485 resolver universe param doc now says BATCH universe vs the focus watchlist (data.universe_max_watchlist); src/engine/datafeeds/news_pipeline.py:~1189 digest docstring bullet says 'today's BATCH universe (eligibl… |
| 80 | low | fixed | src/engine/intelligence/agents/__init__.py:1-2 docstring replaced per the review's suggested text |
| 83 | low | fixed | docstring now says +3 turns when the schema knob goes out |
| 84 | low | fixed | DEFAULT_TIMEOUT_S comment: every enabled agent overrides it via agents.yaml timeout_s |
| 85 | low | fixed | AssembledContextLike + test FakeContext docstrings name engine.intelligence.context |
| 88 | low | fixed | harness comment: SDK cost debug-logged for comparison only, no console reconciliation |
| 89 | low | fixed | src/engine/learning/benchmark.py:22-23 docstring says CORE (NIFTY 500) eligible symbols (O18); src/engine/learning/benchmark.py:39 and :55 error texts now say 'core-eligible symbols' |
| 90 | low | fixed | src/engine/learning/stop_geometry.py:376 'SweepRunner._per_side_fee (sweep.py)'; src/engine/learning/stop_geometry.py:560 pinned stop_entry_price="fillprice" in the SweepRunner portfolio build, sweep.py |
| 91 | low | fixed | src/engine/learning/validate.py:18, :37, :175, :524, :628 'Phase 2' replaced with 'not yet built (plan §6.4; autonomous promotion is Phase 5)'; src/engine/learning/__init__.py:1 now lists what the package holds and says the ledger/River/champion-challenger… |
| 93 | low | fixed | src/engine/marketdata/bar_builder.py:339-340 comment: flushed every flush_interval_s, 60 s default since WO-7; src/engine/marketdata/bar_builder.py:259-260 persist_raw_ticks doc: store.stage_tick, flushed off-loop by aflush_ticks |
| 95 | low | fixed | src/engine/marketdata/store.py:1541-1543 export_bars_1m_month docstring: no production caller; no job schedules the monthly archive |
| 96 | low | fixed | src/engine/marketdata/store.py:1697-1698 get_universe_eligible_symbols docstring: "today's top-N focus watchlist" (data.universe_max_watchlist) |
| 98 | low | fixed | src/engine/notify/catalog.py FEED_STALE docstring: no production caller; staleness pages via RISK_STATE_CHANGE cause feed_stale; src/engine/notify/catalog.py backfill_report and rec_fill_suspected docstrings: 'No production caller yet' reserved notes |
| 99 | low | fixed | src/engine/notify/catalog.py STRATEGY_LABELS: +hi52, brk20, ins, cat_reversal |
| 100 | low | fixed | src/engine/notify/catalog.py paper_alert docstring |
| 101 | low | fixed | src/engine/notify/telegram.py:19-20 'engine.ops.main composes them (§3.2.12)'; src/engine/notify/telegram.py:~641 dropped 'catalog model lands in Phase 2' |
| 105 | low | fixed | src/engine/oms/reconcile.py:42-44 docstring per review |
| 106 | low | fixed | src/engine/oms/updates.py:109-110 comment reworded |
| 107 | low | fixed | src/engine/ops/health.py:12-14 module docstring; src/engine/ops/health.py:~598-601 method docstring |
| 108 | low | fixed | src/engine/ops/heartbeat.py:121-123 |
| 109 | low | fixed | src/engine/ops/holdings_reconcile.py:43-47 |
| 112 | low | fixed | src/engine/ops/lifecycle.py:8 shape line; src/engine/ops/lifecycle.py:20-21 steps 2-3 |
| 115 | low | fixed | src/engine/ops/main.py:8 'Phase-1 shape' to 'Current shape'; src/engine/ops/main.py:16-23 docstring |
| 117 | low | fixed | src/engine/ops/main.py:~844 comment; src/engine/ops/main.py:~3430 comment |
| 120 | low | fixed | src/engine/ops/main.py:~3984-3987 _hi52_daily_leg docstring; src/engine/ops/main.py:~4021 loop comment |
| 123 | low | fixed | src/engine/ops/nightly_review.py:523 22:35 -> 21:00; tests/unit/test_nightly_review.py:629 and :688 22:35 -> 21:00 |
| 124 | low | fixed | src/engine/ops/paper_risk.py:54-57 no_exit_routine removed |
| 125 | low | fixed | src/engine/ops/paper_runtime.py:152-153 alert docstring |
| 128 | low | fixed | src/engine/ops/pipeline.py:~2246 comment; src/engine/ops/pipeline.py:~2946 comment |
| 130 | low | fixed | src/engine/ops/process_memory.py:~63-75 read() and _resolve |
| 131 | low | fixed | src/engine/ops/scan_context.py:81-82; src/engine/ops/scan_context.py:~86-91 gap bullet |
| 135 | low | fixed | src/engine/paper/broker.py:~65, ~74, ~253, ~936, ~1349-1351, ~1374; tests/unit/test_paper_broker.py:1212-1214, 1230, 1247 |
| 136 | low | fixed | src/engine/paper/fill_model.py:15-16 defaults described as a FLOOR; src/engine/paper/fill_model.py:24-26 'generated by scripts/calibrate_fill_model.py', optional |
| 137 | low | fixed | src/engine/paper/fill_model.py:~72 comment '(generated by scripts/calibrate_fill_model.py; optional, defaults when absent)' |
| 138 | low | fixed | src/engine/paper/replay.py:62-65 scope paragraph (schedule()/at_tick() verified at :527/:549); src/engine/paper/replay.py:~308 comment '# provenance only (plan-pinned signature); no consumer' |
| 142 | low | fixed | src/engine/risk/gate.py:290-291 comment 'Informational only: no rule reads these; window bounds are validated against the session when the owner sets them (ModeManager.validate_window).' |
| 144 | low | fixed | src/engine/risk/mode.py:96-97 'No production caller: the gate's mode_risk_state/trade_window rules evaluate this predicate themselves; only chaos case 13 asserts it.' |
| 145 | low | fixed | [prompt text] src/engine/strategy/contracts.py:5 'fits exactly one of the legs (seven at the time)'; src/engine/strategy/contracts.py:25-26 evidence-status sentence now 'one validated (ins), one forward test (hi52), the rest exploratory (cat/cat_reversal as… |
| 146 | low | fixed | src/engine/strategy/indicators.py:135-136 wilder_atr docstring now names the atr14_1m / atr14_1d features and the rec card's stop-in-ATR extra |
| 147 | low | fixed | src/engine/strategy/indicators.py:~226 rolling_max_high docstring now 'Research primitive, no production caller; the live prox_* features use features.engine._prox_high' (that function exists at features/engine.py:251) |
| 148 | low | fixed | src/engine/strategy/prescreen.py:195-197 DEFAULT_CAP_KEY comment reworded to 'a third of the day as shipped, settings.yaml'; tests/unit/test_prescreen.py:465 docstring likewise |
| 149 | low | fixed | src/engine/strategy/retest.py:251 '(~480 symbols)' -> '(NIFTY Total Market since O18, 2026-10-08)' |
| 153 | low | fixed | src/engine/strategy/scanners/brk20.py:234 `int(p["rr_target"])` -> `Decimal(str(p["rr_target"]))`; tests/unit/test_brk20.py:85-92 added test_fractional_rr_target_is_not_truncated (rr_target=1.5 -> target 104.50) |
| 154 | low | fixed | src/engine/strategy/scanners/cat.py:57-59 now reads '5 because ... overnight_gap_mult arithmetic applies to a swing entry (2.5x when chosen, 2.0x since O17) — the same 6 -> 5 correction made to ins'; src/engine/strategy/scanners/cat_reversal.py:72-73 '(2.5x… |
| 157 | low | fixed | src/engine/universe/builder.py:16 added '(Cap 200 -> 300 on 2026-09-07; kept at 300 by O18 — data.universe_max_watchlist.)'; src/engine/universe/builder.py:277-279 comment names the configured index (NIFTY Total Market since O18) and data.universe_max_watch… |
| 158 | low | fixed | src/engine/universe/builder.py:16, :277-279 |
| 159 | low | fixed | src/engine/universe/builder.py:244 and :396 removed the unused `source` parameter; tests/unit/test_risk_gate.py:1575-1585 asserts the EXCL_* codes match the gate prefix and SurveillanceLists.reasons_for output |
| 161 | low | fixed | src/engine/universe/leverage.py:201-203 comment: kind reused at warning severity, does not freeze entries, FEED_DEGRADED is the tick-feed kind, never expires in the outbox |
| 164 | low | fixed | tests/chaos/test_case04_token_expiry.py:26-28 docstring, import, skip reason at ~251; tests/chaos/conftest.py: REAL_SIDE_ONLY constant |
| 165 | low | fixed | tests/chaos/test_case16_offline_across_eod_window.py:40-42 docstring and skip reason ~163 |
| 166 | low | fixed | tests/chaos/test_case17_offline_multi_day.py:~42 module docstring and prep docstring ~325 |
| 168 | low | fixed | tests/chaos/conftest.py:3-5,16,~22; tests/chaos/test_case22_lifecycle_notifications.py:23-28 docstring, import, skip ~213 |
| 169 | low | fixed | tests/chaos/test_case23_shutdown_races_tick_flush.py:13 |
| 170 | low | fixed | tests/replay/test_paper_e2e.py:482-483 |
| 171 | low | fixed | tests/unit/test_agent_defs.py:25 import nightly; :38 AGENT_MODULES gains nightly; :91-93 the AGENT_ID set gains nightly_reviewer; :1 docstring widened to §5.2–§5.5 |
| 174 | low | fixed | FakeContext call_class is the real signal_candidate |
| 175 | low | fixed | tests/unit/test_features.py:1-4 docstring → v2 (sentiment/catalyst block); tests/unit/test_features.py:107 comment: production never writes instruments_daily.surveillance; seeded to pin the read |
| 177 | low | fixed | tests/unit/test_lifecycle_selftest.py:262-264 |
| 178 | low | fixed | tests/unit/test_market_store.py lines 1,3,42,49,88,108,123,274,276,289,348,359,563,564,583,641,647,679,686,702,914: Â§ -> §, â€” -> — |
| 179 | low | fixed | tests/unit/test_news_scoring.py:45 model sonnet-5.5; :49 max_output_tokens=1500; :528, :669, :714 assertions now 'sonnet-5.5' |
| 180 | low | fixed | tests/unit/test_ops_main_wiring.py:107-113 _all_noop_fns; tests/unit/test_ops_main_wiring.py:~389 docstring |
| 182 | low | fixed | tests/unit/test_telegram_commands.py:662-664 docstring; dashboard/tests/recs.test.ts:32 test name |
| 183 | low | fixed | tests/unit/test_universe_builder.py:577-578 '(a fixture cap of 200, the O15-era value; settings.yaml has been 300 since 2026-09-07)' |
| 184 | low | fixed | ticker/main.py:123-128 _log docstring rewritten (plain [mt-ticker] line, drained verbatim into ticker_child_output at WARNING); TODO(Phase 1) comment removed |
| 185 | low | fixed | ticker/main.py:656-658 comment: warn (do not exit); the supervisor enforces the per-spawn secret in _handle_child_connection |
| 3 | deliberate | skipped — deliberate | Still deliberate |
| 7 | deliberate | skipped — deliberate | Still deliberate |
| 38 | deliberate | skipped — deliberate | Still holds: scripts/a11_check.py:4 reads 'One-off Phase-0/1 task ...' as a dated historical one-off tool, and it prints runtime output rather than making a standing claim |
| 39 | deliberate | skipped — deliberate | Still holds: backfill_filings.py:2 'stage 1 — ingest + backfill only' still correctly classifies a script that only ingests, which is true today |
| 45 | deliberate | skipped — deliberate | Still holds: rescrape_costs.py:93 is a regex-anchor example of the page's wording ('NSE: 0.00297%'), and the script only prints a diff for human review and never writes costs.yaml |
| 79 | deliberate | skipped — deliberate | Deliberate classification still holds: features/engine.py:79-81 still reads 'defaults to Tuesday per the 2025 NSE expiry-day move [VERIFY Phase-1]' |
| 86 | deliberate | skipped — deliberate | probe branches for knobs the SDK lacks remain the documented fail-soft design |
| 102 | deliberate | skipped — deliberate | Still holds: telegram.py:278 'command is live as of Phase 2' and :364 'that section is normally empty'; every _COMMANDS row is live=True and the stub path is a documented seam |
| 111 | deliberate | skipped — deliberate | Still holds: JOB_CORP_ACTIONS_GTT is a reserved watermark id (jobs.py:126 comment 'ex-date GTT repair (A12)'), the plan's taxonomy still lists it, and no runtime text claims it runs |
| 114 | deliberate | skipped — deliberate | Still holds: lifecycle's latch branch clears only its own causes, RiskStateLatch does the arbitration, and the direct setter is a documented latch-less compat path (_freeze docstring) |
| 119 | deliberate | skipped — deliberate | Still holds: _log_intraday_class_transition is a documented compat shim for tests (17 references in test_ops_main_wiring.py) and diverges nowhere from _log_class_transition |
| 133 | deliberate | skipped — deliberate | Deliberate classification still holds: why.py _levels still reports level distance only ('hi52: arms on a close >= {arm:.2f} …', unchanged by this work), as the Q1.10 spec asked |
| 167 | deliberate | skipped — deliberate | Still a pricing pin: agents.yaml prices every MODEL_API_IDS name and the tests' arithmetic depends on those rates (case 05 '34M sonnet input tokens = $102' is the sonnet-4.6 $3/MTok rate) |
| 172 | deliberate | skipped — deliberate | FakeOptions is a superset stand-in by design; the real 0.2.x surface is now also pinned (SdkToolsOptions) |
| 176 | deliberate | skipped — deliberate | lifecycle.py documents the latch-less path as kept for older tests; still holds |
| 21 | refuted | skipped — not valid | Refutation holds: AiBudget.tsx:83 'forward cap (still 32)' is about Minimal/Stopped, where config/agents.yaml DG1 degraded_forward_cap_frac: 0.67 gives int(48*0.67)=32 |
| 126 | refuted | skipped — not valid | Refuted classification still holds: the pipeline.py comment about mom being stopless until ledger-driven rebalance state lands still matches the plan, and a ledger-driven rebalance has not landed |
