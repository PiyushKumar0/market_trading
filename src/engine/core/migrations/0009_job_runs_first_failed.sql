-- ------------------------------------------------------------ §2.6 catch-up: failing-streak clock
-- 2026-08-18: date-keyed catch-up gained a give-up path (a day still failing after days of retries
-- is marked ``skipped`` — terminal — instead of retrying and alerting forever). The give-up must be
-- keyed on how long the day has been FAILING, never on the day's calendar age: a cold boot after a
-- long off-gap replays old dates on their FIRST-ever attempt, and an age-keyed rule would abandon
-- them permanently on one burst of NSE 503s (review-confirmed by execution before this shipped).
-- ``first_failed_at`` starts the streak clock on a day's first recorded failure, survives repeat
-- failures unchanged, and clears on success. Additive: existing rows get NULL — their clock starts
-- on their next failed attempt.
ALTER TABLE job_runs ADD COLUMN first_failed_at TEXT;
