-- The prompt TEMPLATE identity of a call (system prompt + the assembler's fixed wording + the
-- strategy contract), unlike inputs_digest, which differs on every call. The verdict keys on
-- (prompt_version, model) so it never pools prompts. NULL on rows written before this migration.
ALTER TABLE agent_calls ADD COLUMN prompt_version TEXT;
