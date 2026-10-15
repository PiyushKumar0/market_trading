-- ------------------------- paper exit policy (D14, plan 2026-10-15)
-- Additive only. NULL = the stop/target geometry; 'time_only' = a paper position of a
-- `paper.no_stop_strategies` strategy, opened with no stop and no target, that exits on the D2 time exit.
-- Fixed when the position opens; real positions keep NULL.
ALTER TABLE positions ADD COLUMN exit_policy TEXT;
