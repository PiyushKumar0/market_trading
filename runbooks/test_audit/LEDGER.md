# Test audit — ledger

One row per removed, merged, renamed or parametrized test (PROMPT.md §7.2). A verdict is written here
BEFORE the edit; the commit column is filled when the batch lands.

Codes (PROMPT §4): R1 duplicate · R2 subsumed · R3 vacuous · R4 change-detector · R5 third-party ·
R6 dead · R7 excessive · R8 finished-script (owner decision) · P1 parametrize · P2 trim asserts ·
P3 trim patches · P4 fixtures · P5 repair.
Tiers (PROMPT §3): A keep (refactor only, same assertions) · B removal needs fault injection · C other.

Evidence conventions:
- **R1/R2:** the survivor's nodeid and the assertion that covers the removed one (file:line at the commit's
  parent), plus the footprint check (`footprint_query.py check`): lines of the removed test outside the
  survivors' union — `outside=0`, or each line justified.
- **Tier B:** the mutation (file:line, before → after), the surviving test that failed, its failing line, and
  the revert (`git -C <worktree> status --short src` clean afterwards).
- **P1:** the new parametrized nodeids replace the listed ones one-for-one (ids given); no assertion dropped.

## Verdicts

| # | nodeid(s) | tier | code | evidence | commit |
|---|-----------|------|------|----------|--------|
