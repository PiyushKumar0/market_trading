# Dashboard capital change: design v2 (2026-10-16)

**Status:**
- v1 was approved by the owner on 2026-10-16, with the base-agnostic brake fix (owner: "fix the logic, no new table").
- v2 folds in an independent adversarial review (19 findings, 4 HIGH, each checked against the code) and the manager's own review.
- What v2 changes is listed in §10.

**Request (owner, 2026-10-16):** "Add option on the dashboard to update budget". The budget is the trading capital. Real and paper capital move together.

## 1. Owner decisions (2026-10-16)

| # | Decision | Choice |
|---|---|---|
| D1 | What the edit sets | One figure, `limits.yaml` `capital_base_inr`. The real book and the paper book already size off it (`ops/main.py` `_compose_paper`: `capital_base_fn=lambda: limits.load().capital_base_inr`). |
| D2 | What moves with it | The two rupee caps scale in proportion: `capital_cap.max_deployed_capital_inr` and `per_stock_exposure.cnc_notional_inr`. **Corrected rationale:** ₹12,000 is not a "30% of base" rule. O16 left the per-symbol cap fixed when the base doubled (the C4 concentration guard), and O17 set ₹12,000 on cost geometry so that it "binds only on tight stops". Scaling keeps both of those properties: per-symbol concentration stays 30% of capital, and the cap stays about 1.8× the 6%-stop risk-sized notional. A fixed cap would loosen concentration on a cut and bind more often on a raise. This reverses the O16 precedent on purpose, and O19 records that. |
| D3 | When it applies | Live, outside market hours: refused on a trading day between the session's open and close (the engine calendar, special sessions excluded). |
| D4 | Paper account | Same epoch. Open paper positions, including the D14 trades, stay open. |
| D5 | Guardrails | A confirm dialog showing old → new; one edit at most doubles or halves the capital; every change is audited and announced on Telegram. |

## 2. One source for the base (review F2)

**Today the base lives in three copies:**
- the limits table (read by the floors and by the gate's `day_mtm_pct`);
- the real `ExposureTracker._capital_base`, copied at boot (`main.py:646`) and read by `equity()` and `evaluate_day_loss`;
- the paper tracker plus `PaperRisk._capital_base`, copied at paper composition.

If a live apply updated them one by one and any step after the reload failed or was skipped, the half-state would be dangerous. Example: a 40k → 80k raise with a stale real tracker has equity ₹39,257 against an ₹68,000 cumulative floor, which triggers a KILL and mode OFF at the next tick.

**Fix (logic, no new state):**
- `ExposureTracker` and `PaperRisk` take `capital_base` as a value **or a zero-argument function**.
- Composition passes `limits_capital_base`, which is `limits_engine.load().capital_base_inr`. That load is cached, so the call is O(1).
- When the limits table is unverified (the boot is FROZEN), it falls back to `_FALLBACK_CAPITAL_BASE_INR`. That is display only; its comment is updated to say so and that it can lag a dashboard change.
- After a change, every reader sees the new base on its next call, so the live apply is just `LimitsEngine.reload()`.
- No setters, and no new write-capable object crosses into the API. The paper broker's static margin stub is not updated: the paper gate passes `available_margin=None` (`gate.py:1537`).

## 3. Base-agnostic brakes

### 3.1 Day loss and drawdown in P&L space

**The problem (observed).** Equity = base + realized net + open MTM. The day-loss baseline and the 5-session drawdown peak compare that against equity stored in past snapshots, which carries the base it was taken at. On 2026-09-07 the base rose from 20k to 40k at 12:56 (config_audit id 123), mid-session. Recorded day MTM went from −₹185.05 to +₹19,816.45 until the close, so the −5% rung was effectively off that afternoon.

**The fix:** compare P&L to P&L, then add the current base.
- The day baseline P&L is the last snapshot before `d`, taking `realized_pnl + open_mtm`. With no prior snapshot it is P&L now minus today's realized net, as today.
- The drawdown peak is the current base + the max of (`realized_pnl + open_mtm`) over the window.
- The interfaces stay in equity terms. `set_day_baseline(equity)` stores `equity − current base`, so `selftest.py:454`, `paper_risk.py:193` and `exposure.py:443` are unchanged.
- **A NULL P&L column** (the schema allows it; there are 0 such rows today) falls back to `equity − current base`. That reproduces today's result for that row, so a data defect can never change a brake.
- **The data supports it.** 17,199 real and 2,173 paper rows, 0 NULL. equity − realized − open equals the base on every row: 20,000 until 2026-09-07 12:54, then 40,000.

### 3.2 Refuse a change that would trip a brake (review F1, and the manager's notes)

The floors compare **lifetime** P&L against the base, so a cut while in loss can trip them: equity floor rung ⇒ go flat + CLOSE_ONLY; cumulative floor ⇒ kill + mode OFF. On paper, both floor causes flatten the book and latch until `/paper reset`, which ends the D14 epoch. Avoiding market hours only postpones this, because the boot self-test and the first tick evaluate them.

**Rule.** The preview and the POST run the engine's own `evaluate_floors` (equity floor, cumulative floor, weekly drawdown) and `evaluate_day_loss`, for **both** scopes, at the proposed base. They do this on a copy of the tracker with the proposed base. A change is refused (422) if any rung would be **newly** tripped, meaning tripped at the proposed base but not at the current one. The refusal names each rung and its ₹ line.

A rung that is already tripped does not block. So raising capital to climb back above a latched floor's line stays possible, and the real floor still needs `/rearm`.

### 3.3 hi52's registered edge stays honest (review F4)

`settings.yaml hi52.expected_edge_pct` (1.53) is the output of a derivation at the equity floor rung, base × 0.9. That makes it a function of `capital_base_inr`: at a 20k base it derives 1.27, at 80k 1.66. The gate copies it once at boot. A test pins settings == derivation at the live `limits.yaml`, so any capital change would fail the suite.

**Fix:**
- Move the derivation (`sizing_notional`, `cost_floor_pct`, the measured constant, the edge function) from `scripts/hi52_forward_verdict.py` into the engine. The script imports it, so there is still one derivation.
- The gate uses `min(registered, derived at the live limits)` for hi52. A cut lowers the edge automatically; a raise never lifts it above the registered value. Raising the registered number stays the owner's call.
- The test pins the registered value to the derivation at the **registration inputs** (base 40,000, the O17 limits). That keeps its guard against stop and cost-table edits, and the runtime rule carries the rest.
- Disclosure: the hi52 kill criterion charges costs at the smallest equity in its window, so a cut during the forward test leans it toward DEMOTE. That is its stated conservative design, and it is recorded in O19.

## 4. Writing limits.yaml (review F3, F6, F7, F8)

- **Read once, verified.** Read the bytes once and require their sha256 to equal the recorded signature. Otherwise answer 503 "protected config unverified" and write nothing. This means a file edited without a reseed is never laundered into a signed one. The new text is built only from those bytes.
- **Edit three lines.** Exactly three lines change: the numeric values of `capital_base_inr`, `max_deployed_capital_inr` and `cnc_notional_inr`. Each key must occur exactly once, or the request is refused. The existing trailing comment is **kept**, so the O1/O16/O17 provenance stays, and gets ` · owner via dashboard YYYY-MM-DD: OLD → NEW` appended. The file is LF-only and the edit keeps LF.
- **Scaled caps** are `floor(cap × new / old)` in whole rupees. Flooring never loosens an upper bound.
- **Structural check.** The parsed new file must equal the parsed old file with exactly those three paths changed, each to an `int` equal to the intended value. The new text must also validate as `LimitTable`. This catches the YAML pitfalls: duplicate keys keep the last value silently, and `6E+4` parses as a string.
- **Hardened `ProtectedStore.owner_update`**, for the dashboard path and the seed script alike:
  - an optional `expected_sha256`, so the signature UPDATE is a compare-and-swap (rowcount 1, else refuse);
  - the bytes go to a temp file, which is moved into place with `os.replace` inside the transaction, before COMMIT;
  - on any exception, the old bytes are restored and the temp file removed.

  So no exit path leaves a new unsigned file, or a file the signature doesn't match. The only exception is a hard crash between `os.replace` and COMMIT, which fails closed as today: the next boot is FROZEN.
- **Audit.** `OwnerConfirmation.note` carries the full old → new table. It lands in `config_audit.diff`, which `GET /config/audit` already shows, because `protected_config.content_snapshot` keeps only the latest file.
- **Git.** `config/limits.yaml` is git-tracked, so a change leaves it modified. A stash, checkout or reset would revert the bytes. The next boot would then FREEZE with a flat real book, or KILL with a real position open (lifecycle step 1c; sticky until `/kill_reset`). That second case was a correction from the 2026-10-17 final review.
  - The toast and the Telegram message say "commit config/limits.yaml".
  - COMMANDS gets a recipe that restores the file from `protected_config.content_snapshot`.
  - The manager never stashes `config/`.

## 5. Applying a change

`POST /config/capital {capital_inr, expected_current_inr}` runs in an `async def` route, synchronously from the read to the reload, with no await in between. No tick can interleave, and the shared SQLite connection is used only from the loop thread.

**Pricing (final review 2026-10-17, ruling R16).** The one await before the read prefetches prices. Each held position is priced at its live mark, else at the close of its last stored 1m bar. The live caches are empty after an out-of-session restart, and pricing at entry would hide open losses from §3.2.

**Refusals:**
- **409** in the session window. Also when the engine calendar does not cover today (it fails open otherwise, review F16), when the mode is AUTO (in AUTO a raise would resize live orders in one POST; review F13), or when `expected_current_inr` ≠ the current base (a stale dialog or a second device; review F14). Also when a held position has no price at all, so §3.2 cannot be checked (final review 2026-10-17).
- **422** when the value is not a whole number, is outside [old ÷ 2, old × 2], equals the current base, or would newly trip a brake (§3.2).
- **503** when the file is unverified.

**The apply:**
1. Build and check the new text (§4).
2. Hardened `owner_update` with `expected_sha256`.
3. `LimitsEngine.reload()`: every reader now sees the new base and caps (§2), and the hi52 edge re-derives on its next read (§3.3).
4. Telegram `CAPITAL_CHANGED`: old → new for every figure plus "commit config/limits.yaml".
5. The response carries the same table.

**Known, accepted:** a worker thread that writes on the shared connection without BEGIN during the millisecond transaction would join it (`ins_crossings` 19:15, review F17). Its rows are lost only if this transaction rolls back.

## 6. API (owner bearer token)

**`GET /config/capital?proposed=<inr>`** returns the current and, when given, the proposed figures, all computed by the engine:
- capital, max deployed and per-symbol CNC cap;
- equity, real and paper;
- risk per trade, intraday and swing, as **% × equity**, because that is what the gate charges (review F10);
- the day-loss lines;
- the equity floor and the cumulative floor lines, each with its distance from equity;
- the hi52 edge the gate would use;
- `allowed` and `refusal`, including the rungs a proposal would trip.

**`POST /config/capital`** behaves as described in §5.

## 7. Dashboard

- **Entry point.** A "Change capital" button on the Risk limits card opens a dialog. Its input is labelled "Starting capital (the base every limit is a percent of)", and it shows the resulting equity (base + lifetime P&L) so the base is not mistaken for cash on hand (review F19). The dialog shows the engine's old → new table and the refusal text; Confirm is enabled only when `allowed`. The POST carries `expected_current_inr`.
- **Paper history in P&L space (review F5).** `GET /paper` takes the live base. "Paper equity" = live base + latest P&L, and "Started with" = live base. Each curve point = live base + that point's `realized_pnl + open_mtm`, with the stored equity as a fallback for a NULL row. So the chart has no fake cliff at a change, and its base line is right before the next session's first snapshot. The chart's labels say "at today's starting capital".
- The toast includes "commit config/limits.yaml".

## 8. Tests

- **Brakes:**
  - P&L-space day baseline and drawdown, real and paper, across a restart.
  - A NULL-column row equals today's result.
  - A replay of every stored real and paper snapshot through old and new code is identical except the windows spanning 2026-09-07 (expected, quoted).
  - The 09-07 raise replays as about −₹185, not +₹19,816.
- **Single source:** after `reload()` every reader (real tracker, paper tracker, `PaperRisk`, gate) sees the new base, with no setter.
- **Breach refusal:** a cut in lifetime loss trips the equity floor and is refused with the rung named; the same for cumulative, drawdown and day loss, in both scopes. An already-tripped rung does not block a raise.
- **`owner_update`:** CAS mismatch refused; fault injection in the transaction leaves the old bytes and the old signature in place; the temp file is cleaned up; the seed script path is unchanged.
- **Rewrite:** three lines changed and everything else byte-identical; comments kept and appended; a duplicate key refused; the int check (`6E+4` refused); floor rounding; an unverified file gives 503 and nothing is written.
- **Window and mode:** session open and close inclusive; one second either side; a holiday; the calendar not covering today; AUTO refused; `expected_current_inr` mismatch gives 409.
- **hi52:** the gate uses min(registered, derived): a cut lowers it, a raise keeps it. The pin holds at the registration inputs, and the script still produces the same report.
- **Real path:** `ProtectedStore` + `LimitsEngine` on a temp copy of the shipped `limits.yaml` and a temp DB; apply 40k → 60k; `load_verified` passes; the gate's risk per trade and `day_mtm_pct` move; the next day's baseline is correct.
- **Fixtures (review F9).** Tests that store equity-only snapshots (`test_exposure_tracker`, `test_paper_risk`, `test_scope_isolation`) are rewritten to consistent rows. The NULL fallback keeps the NULL-P&L fixtures meaning what they meant.
- **Dashboard:** helper tests, plus a `fixture_server.mjs` scenario with the dialog both allowed and refused.

## 9. Not in scope; disclosures

- Percentage limits, `envelope.yaml`, the re-arm rules, a Telegram `/capital` command, and broker funds. The base is the platform's figure; raising it does not add money to Kite.
- **Stale ₹ figures in other `limits.yaml` comments** (e.g. "equity ≤ ₹36,000 ⇒ GO FLAT"). They mislead after a change. One owner-signed, comments-only edit would make them base-relative. That is an owner follow-up, prepared as a patch, never applied by the manager.
- **C5 / D14 confound.** A capital change during the test shifts the notional, and so the cost %, of later recs. The verdict report should list capital-change dates from `config_audit`. This is disclosed in O19; the test's rule does not change.

## 10. What v2 changed from v1

The review's findings, one line each:
- F1: brake refusal (§3.2).
- F2: one source for the base, which removes the setters and the paper boundary crossing (§2).
- F3: verified read, CAS and atomic write (§4).
- F4: the hi52 edge (§3.3).
- F5: paper history in P&L space (§7).
- F6: git recipe (§4).
- F7: audit note (§4).
- F8: structural check and floor rounding (§4).
- F9: fixtures and the 09-07 carve-out (§8).
- F10: risk per trade at equity (§6).
- F11: removed by F2.
- F12: fallback comment (§2).
- F13: AUTO refusal (§5).
- F14: expected-current (§5).
- F15 and F18: disclosures (§9).
- F16: calendar coverage (§5).
- F17: accepted (§5).
- F19: label and equity shown (§7).
- D2's rationale corrected (§1).

## 11. Plan of record

`IMPLEMENTATION_PLAN.md` §7.1 gets **O19**, before any code:
- capital changeable from the dashboard under D1–D5, with D2's corrected rationale and the O16 reversal;
- one source for the base;
- P&L-space day loss and drawdown;
- the brake-refusal rule;
- the hi52 min rule;
- the write-path hardening;
- the disclosures.

The build is high-scrutiny (a safety-brake input) and gets a whole-branch multi-angle review before it merges.
