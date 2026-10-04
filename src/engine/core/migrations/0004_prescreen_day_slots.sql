-- ---------------------------------------------------------------- §3.2.5 prescreen day-slot journal
-- 2026-08-04 (owner-directed): the once-per-day (symbol, strategy) dedupe and the daily candidate
-- caps lived in process memory only, so every restart reset them — observed same day: ~54
-- publications against the 20/day bound across two mid-session restarts, plus duplicate candidate
-- notifications. One row per (session day, symbol, strategy) publication; ``evaluated`` follows the
-- 2026-07-29 rearm semantics: 1 = the day slot is spent (evaluated, or deliberately refused —
-- governor/forward-cap/unsizeable); 0 = never evaluated (in-flight loss or re-armed) — may
-- re-publish within the already-paid cap quota. Boot rehydration: engine.ops.main.
CREATE TABLE prescreen_day_slots (
    d            TEXT NOT NULL,
    symbol       TEXT NOT NULL,
    strategy_id  TEXT NOT NULL,
    published_at TEXT NOT NULL,
    evaluated    INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (d, symbol, strategy_id)
);
CREATE INDEX idx_prescreen_slots_day ON prescreen_day_slots(d);
