-- ------------------------------------------------- §3.2.5/§5.2(a) day-slot journal: score + forwards
-- WO-1 (2026-08-13). Two gaps the 2026-08-04 journal left open:
--   (1) `_forwarded_count` (the §5.2(a) analyst forward cap) was process memory, so a mid-day
--       restart REFILLED the day's analyst quota instead of resuming it — the same class of bug
--       0004 fixed for the publication caps. `forwarded` counts analyst calls charged to this
--       (day, symbol, strategy) pair: a COUNTER, not a flag, because a pair re-armed after an
--       analyst INFRASTRUCTURE failure (2026-07-29) can legitimately be forwarded again and each
--       attempt is real spend against the cap.
--   (2) `SignalCandidate.score` was read by no decision and stored nowhere, so the funnel could
--       only be reconstructed by forensic log reads (WO-9). Recording it here makes
--       published-score quantiles, forwarded scores and "best UNFORWARDED score" — the direct
--       measure of starvation — a plain query over one table.
-- Additive and non-destructive: rows written before this migration keep score NULL (rendered
-- "unknown", never guessed) and forwarded 0.
ALTER TABLE prescreen_day_slots ADD COLUMN score REAL;
ALTER TABLE prescreen_day_slots ADD COLUMN forwarded INTEGER NOT NULL DEFAULT 0;
