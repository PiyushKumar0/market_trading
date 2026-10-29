-- The intraday analyst's FIRST successful verdict on a forwarded candidate (review §9 #2,
-- 2026-10-29) — the report's unit ("first evaluation of each signal"). A failed call writes nothing; a
-- WO-20d re-queue or re-arm that evaluates the same row again never overwrites the first decision.
ALTER TABLE prescreen_day_slots ADD COLUMN analyst_decision TEXT
    CHECK (analyst_decision IN ('enter', 'no_action'));
ALTER TABLE prescreen_day_slots ADD COLUMN analyst_confidence REAL;
ALTER TABLE prescreen_day_slots ADD COLUMN analyst_call_id TEXT;
ALTER TABLE prescreen_day_slots ADD COLUMN proposal_id TEXT;
