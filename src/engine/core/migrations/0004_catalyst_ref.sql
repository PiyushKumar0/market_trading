-- 0004 — learning_ledger.catalyst_ref (§6.5): the catalyst_watchlist entry id a `cat` trade
-- originated from (NULL for every other strategy) — the news→trade audit chain (R8, §2.7).
-- Omitted from 0001 by oversight; RecommendationBook.deliver() already writes it when present
-- (PRAGMA-filtered), so adding the column completes the chain with no code change.

ALTER TABLE learning_ledger ADD COLUMN catalyst_ref TEXT;
