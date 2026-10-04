-- ------------------------- §3.6 holdings reconcile: a DURABLE record of what the broker held
-- 2026-09-12 (WO-D2, exit hygiene). The §3.6 reconcile already asked the right question hourly
-- ("does the broker still hold this tracked position?") and logged the right answer — but it kept
-- that answer only in an in-memory ``{position_id: date}`` dedupe, so nothing outside the job could
-- ever read it. The cost of that was measured on 2026-09-11: two positions the owner sold outside
-- the ledger on 08-26 stayed OPEN until 09-11 and produced 68 exit recommendations (6-10 a day),
-- 182 position-event analyst calls, and eleven day plans that reasoned about their "overnight risk".
-- The single ins entry of the month (JINDALSTEL, 09-08) arrived as notification 2 of 8 that day;
-- the other seven were repeats of those two exits.
--
-- One row per (position, trading day) is the smallest durable fact that fixes it: two consecutive
-- observation days that read SHORT are the evidence the position-event path and the pre-open planner
-- need to stop spending calls and prompt space on a position that no longer exists
-- (``holdings_reconcile.positions_missing_from_holdings``).
--
-- ``PRIMARY KEY (position_id, d)`` with an upsert = LAST WRITE OF THE DAY WINS. The job runs hourly,
-- and the answer can legitimately change inside one day (a T+1 leg settles, or the owner sells at
-- 14:00): the day's verdict must be the most recent observation, never the first one. Quantities are
-- stored as INTEGER (holdings are whole shares) rather than the TEXT the price columns use.
--
-- This table is NOT a risk input and does NOT close anything. §3.6 keeps the reconcile alert-only for
-- CLOSING a human-owned position — only the owner's ``/closed`` does that (§6.5). What the table
-- authorises is SILENCE: not making a call, not rendering a line.
CREATE TABLE holdings_observations (
    position_id TEXT NOT NULL,
    d           TEXT NOT NULL,              -- IST trading date, ISO-8601 (sorts chronologically)
    tracked_qty INTEGER NOT NULL,           -- positions.qty at observation time
    held_qty    INTEGER NOT NULL,           -- Σ(quantity + t1_quantity + collateral_quantity) across the broker's rows
    observed_at TEXT NOT NULL,              -- IST timestamp of the winning write for that day
    PRIMARY KEY (position_id, d)
);

-- Every reader is "the most recent N observation days, no older than a lookback" — a date RANGE
-- followed by a per-position ordering. The primary key already serves the per-position half; this
-- index serves the range scan that comes first when the table has many positions' history in it.
CREATE INDEX idx_holdings_observations_d ON holdings_observations (d);
