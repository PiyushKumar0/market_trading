# Brief — holdings reconcile for tracked CNC positions (owner-directed 2026-09-07)

Design authority: IMPLEMENTATION_PLAN.md §3.6, paragraph "**Holdings reconcile for tracked CNC
positions**" (right after "Tracked human positions"). Binding.

## Scope
1. New module `src/engine/ops/holdings_reconcile.py`: `HoldingsReconcileJob(conn, kite, clock,
   calendar, notify, *, min_age_sessions=2)` with `async def run(self) -> HoldingsReconcileResult`
   (`checked`, `flagged: list[str]`, `skipped_young`, `error: str | None`).
   - Fetch `await kite.holdings()` (see `src/engine/broker/kite_client.py:265` and how the ISIN/
     instruments code handles its exceptions; a `TokenException`/any error ⇒ log
     `holdings_reconcile_failed` WARNING with the error type, return `error=...`, never raise).
   - Held quantity per tradingsymbol = `quantity + t1_quantity` (ints; missing keys ⇒ 0).
   - Tracked positions: `SELECT position_id, symbol, qty, opened_at FROM positions WHERE state='OPEN'
     AND product='CNC' AND origin IN ('platform','recommended')`. Skip those opened fewer than
     `min_age_sessions` completed trading sessions ago (use `engine.core.calendar.NSECalendar`, count
     sessions strictly between `opened_at.date()` and today) → `skipped_young`.
   - Flag when held < tracked qty. For each flagged position find the ENTRY rec id:
     `SELECT rec_id FROM learning_ledger WHERE position_id=? AND entry_px IS NOT NULL ORDER BY created_at LIMIT 1`
     (fall back to any ledger row for the position; if none, say so in the alert and give the
     position_id). Alert once per position per trading day (in-memory `{position_id: date}`; a
     restart re-alerts once, accepted). Log `position_not_in_holdings` (symbol, position_id, tracked,
     held, entry_rec_id) every run it is flagged, alert only once/day. Log `holdings_reconcile_done`
     (checked, flagged, skipped_young) each run.
2. Notification: add `MessageKind.POSITION_NOT_IN_HOLDINGS = "position_not_in_holdings"` to
   `src/engine/notify/catalog.py` (mirror the REC_FILL_SUSPECTED entry's shape, severity warning)
   and a builder function that renders: symbol, tracked qty vs held qty, and the exact reply
   `/closed <entry_rec_id> <price>` ("price = your actual exit price"). Register the kind wherever
   the Telegram layer whitelists kinds (`src/engine/notify/telegram.py` ~line 188 lists
   REC_FILL_SUSPECTED — follow that pattern).
3. Wiring in `src/engine/ops/main.py`: (a) an interval job `holdings_reconcile` every 3600 s whose
   body returns immediately unless `calendar.is_trading_day(today)` and 09:20 ≤ now.time() ≤ 15:30;
   (b) one run at the end of the post-login recovery ladder (`src/engine/ops/post_login.py` —
   read its step list and add a non-load-bearing step that never fails the ladder). The job must
   never touch risk state, the gate, or the prescreen.
4. Tests (red-first, inline, pasted): `tests/unit/test_holdings_reconcile.py` with a fake kite
   (holdings list), an in-memory SQLite conn seeded via the repo's migrations (see how other ops
   tests build `conn`), a fixed clock/calendar: (i) absent symbol ⇒ flagged + alert text carries the
   entry rec id and `/closed`; (ii) partial (held 3 of 7) ⇒ flagged; (iii) fully held ⇒ not flagged;
   (iv) opened yesterday ⇒ skipped_young, not flagged; (v) second run same day ⇒ no second alert,
   log still emitted; (vi) broker error ⇒ `error` set, nothing flagged, no raise; (vii) MIS and
   external positions ignored. Wiring test in `tests/unit/test_ops_main_wiring.py` if the module has
   a pattern for asserting armed jobs (it does — see `test_live_interval_jobs_are_armed`).
   Then the FULL unit suite once, inline; paste the summary line.

## Constraints
- Engine is running (post-close); never start/stop it; do not write to data/market.duckdb or
  data/state.db. No new dependencies. Do not commit. Do not edit IMPLEMENTATION_PLAN.md / WORKLOG.md
  / limits.yaml / envelope.yaml.

## Output contract
Final message = (a) commands + pasted outputs incl. the full-suite summary; (b) file:line pointers
for the job's run(), the held-quantity rule, the T+1 skip, the per-day dedupe, the catalog kind and
renderer, the scheduler wiring and the post-login step; (c) decisions the plan left open; (d)
anything not done, with the reason.
