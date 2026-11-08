-- ------------------------- paper exit policy
-- Additive only. NULL = the stop/target geometry; 'time_only' = a paper position of a
-- `paper.no_stop_strategies` strategy, opened with no stop and no target, that exits on the time exit.
-- Fixed when the position opens; real positions keep NULL.
ALTER TABLE positions ADD COLUMN exit_policy TEXT;
