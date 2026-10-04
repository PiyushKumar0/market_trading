-- ------------------------------------------------------------------ §6.1 `ins` pending candidates
-- Owner-directed 2026-08-17 (the first evidence-first origination leg). `ins` fires on EOD filings
-- data, but candidates must be ADMITTED during the next session (through SignalPreScreen.admit, so
-- the §3.2.5 dedupe/caps bind identically). That gap — EOD compute, next-morning admission, with a
-- possible engine restart, machine sleep or a §2.6 catch-up replay in between — is exactly the class
-- of gap that made the mom rebalance marker (0006) and the prescreen day-slot journal (0004/0005)
-- necessary: process memory does not survive it. So the crossing is journalled here at compute time.
--
-- One row per (for_session, symbol) — the PK is what makes the EOD job IDEMPOTENT per day: a re-run
-- (catch-up replay, a manual --once, the periodic missed-job sweep) upserts the same row instead of
-- queueing a duplicate candidate. `consumed` is what makes the MORNING admission restart-safe: the
-- sweep marks rows consumed in the same transaction that admits them, so a second sweep the same day
-- (or a sweep after a mid-session restart) re-admits nothing. Both bounds are structural, not timing.
--
-- Columns:
--   for_session       the trading day on which this candidate is to be admitted = the session whose
--                     OPEN is the validated fill (NSECalendar.next_trading_day of the crossing).
--   crossing_session  the session at which the trailing-10-session net-BUY sum crossed the floor —
--                     the PIT event date, derived from DISCLOSURE broadcast timestamps, never the
--                     insider's transaction date (a transaction-date anchor is the lookahead WO-16
--                     was commissioned to rule out).
--   trailing_value    the crossing's trailing-window open-market BUY value in ₹, TEXT because it is a
--                     Decimal (§8.1 money convention: money never round-trips through a float).
--   contributing_filings_n  how many eligible filings summed into that value (owner context).
--   reference_close   the crossing session's CLOSE — the pre-open entry reference the morning
--                     admission anchors levels on (scanners/ins.py: "which price anchors what").
--                     Journalled rather than re-read so one pending row always yields one identical
--                     candidate (§9.6 replay determinism). TEXT for the same Decimal reason.
--   consumed          0 = awaiting admission; 1 = a sweep has admitted it. Never deleted: the row is
--                     the audit trail of what the EOD job found, and a consumed-but-suppressed
--                     candidate (dedupe/cap) must stay distinguishable from one that never existed.
CREATE TABLE ins_pending (
    for_session            TEXT    NOT NULL,
    symbol                 TEXT    NOT NULL,
    crossing_session       TEXT    NOT NULL,
    trailing_value         TEXT    NOT NULL,
    contributing_filings_n INTEGER NOT NULL DEFAULT 0,
    reference_close        TEXT    NOT NULL,
    consumed               INTEGER NOT NULL DEFAULT 0,
    created_at             TEXT    NOT NULL,
    consumed_at            TEXT,
    PRIMARY KEY (for_session, symbol)
);

-- The morning admission's only read: today's unconsumed rows.
CREATE INDEX ins_pending_unconsumed ON ins_pending (for_session, consumed);
