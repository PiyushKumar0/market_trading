# News Impact Study Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** build the read-only tooling for the pre-registered news impact study: a first-score loader and market
panel, a capped nightly LLM mapper, and the sample A / B verdict script.

**Architecture:** three scripts beside the macro mood study's, reusing its tested helpers instead of copying them.
`news_impact_common.py` holds the constants, loaders, timing rules, benchmarks, trade constructors and the verdict
rule. `news_impact_map.py` runs the mapper through `macro_mood_llm.StudyLLM` into its own SQLite store.
`news_impact_study.py` builds each hypothesis's events and trades and writes the report. Nothing touches the engine,
`agent_calls` or `market.duckdb`.

**Tech Stack:** Python 3.12, duckdb, sqlite3, numpy, pandas, pytest (offline tier).

**Spec:** `IMPLEMENTATION_PLAN.md`, the entry "News impact study". Every threshold, window, rule and diagnostic is
defined there. Read the entry before each task. Where this plan and the entry disagree, the entry wins.

## Global Constraints

- Read-only against `data/state.db` (`file:...?mode=ro` URI) and the DuckDB copy (`read_only=True`). The only file
  any script writes is `data/research/news_impact.sqlite` (the mapper) and the reports under `data/reports/`.
- Never call the paid API: LLM calls go only through `macro_mood_llm.StudyLLM`. Tests never make a real LLM call (inject
  `query_fn`).
- Reuse, don't copy: `macro_mood_common` (`liquid_universe`, `load_etf_symbols`, `cost_pct`, `TradeRow`,
  `non_overlapping`, `_as_date`), `macro_mood_forward` (`load_calendar`, `regular_sessions`, `load_theme_lists`,
  `Unavailable`), `news_content_score` (`_parse_ts`, `_context_headlines`, `call_refusal`, `VOCAB`, `STOP_PREFIXES`),
  `research_common` (`write_reports`, `NOTIONAL`, `bb.load_structural_ex_dates`, `bb.cpcv_report`),
  `engine.learning.multiplicity.permutation_pvalue` and `registration_alpha`, `engine.learning.validate.fold_pass_min`,
  `engine.datafeeds.news_pipeline.EntityResolver`/`NewsCluster`, `engine.intelligence.agents.news_analyst.parse_output`.
- Do not modify any `macro_mood_*` or `news_content_*` script: the macro mood study is registered and running.
- Scripts load siblings the way `news_content_score.py` does (`_load_sibling`), and put `src` on `sys.path` the same way.
- Tests live in `tests/offline/test_news_impact_*.py`, use synthetic DuckDB/SQLite fixtures in `tmp_path`, and never
  read `data/`. Run them with `.venv\Scripts\python.exe -m pytest tests/offline/test_news_impact_*.py -q` (PowerShell).
- Docstrings and comments state the reason in words; no citations of plans, task numbers or decision numbers.
- No git operations: the manager commits.

## Review Focus

1. A story first scored at 09:14:59 IST enters that day; one at 09:15:00 enters the next session; a Saturday score
   enters Monday (or the next regular session after a holiday).
2. P0 for a score at 11:00 on a session day is the previous session's close, not that day's (that day had not closed).
3. A stock whose sector has fewer than 5 liquid peers, or is `UNCLASSIFIED`, makes no stock trade and is counted.
4. A pullback limit never fills on a session whose low stays above P0; a session that opens below P0 fills at the open.
5. A mapper answer with an out-of-vocabulary group, a direction of 2, more than 3 groups or more than 5 companies is
   trimmed per the parser rule, never raised.

---

### Task 1: common data layer — `scripts/news_impact_common.py`

**Files:**
- Create: `scripts/news_impact_common.py`
- Test: `tests/offline/test_news_impact_common.py`

**Interfaces (produces):**
- Constants, values verbatim from the spec: `FAMILY = "edge/news_impact"`, `N_TRIALS = 6`,
  `ALPHA = registration_alpha(6)`, `FOLD_PASS_MIN = fold_pass_min(6)`, `MIN_TRADES = 30`, `MIN_INFO_OBS = 300`,
  `MATERIAL = 0.6`, `POSITIVE = 0.4`, `NEGATIVE = -0.4`, `HOLD = {"stock_next_open": 10, "stock_pullback": 10,
  "domain_companies": 10, "domain_groups": 10, "global_sectors": 5, "info": 10}`, `PULLBACK_SESSIONS = 5`,
  `MIN_PEERS = 5`, `MIN_BASKET = 3`, `BETA_SESSIONS = 250`, `BETA_MIN_PAIRS = 200`, `BOOT_DRAWS = 2000`,
  `SPLIT_DRAWS = 10000`, `SEED = 20261110`, `ENTRY_CUTOFF = time(9, 15)`, `CLOSE_TIME = time(15, 30)`,
  `SAMPLES = {"A": (date(2026, 7, 29), date(2026, 10, 23)), "excluded": (date(2026, 10, 26), date(2026, 11, 10)),
  "B": (date(2026, 11, 11), date(2027, 2, 26))}`, `NEVER_TYPES = {"pump_promo_suspect"}`,
  `STOCK_SKIP_TYPES = {"pump_promo_suspect", "dividend_corp_action"}`,
  `DOMAIN_MARKET_TYPES = {"govt_program", "sector_policy", "regulatory_policy"}`,
  `GLOBAL_TYPES = {"global_market", "macro_data", "disruption"}`, `HORIZONS = (1, 3, 5, 10, 20)`.
- `@dataclass(frozen=True) class Story: cluster_id: str; at: datetime; call_id: str; model: str | None;
  scope: str; event_type: str; sentiment: float; materiality: float; entities: tuple[str, ...];
  headline: str | None`.
- `load_first_scores(state_db: Path, limit: datetime) -> tuple[list[Story], dict[str, int]]`: the first ok
  `news_analyst` score of every cluster among calls with `at < limit`, found across all calls before any filter (as
  `news_content_score.headline_candidates` does), the headline from that call's `context_gz`; counts of unparsed
  calls and missing headlines.
- `def story_kind(s: Story) -> str | None`: `"stock"`, `"domain"`, `"global"` or None, per the spec's scope and
  event-type rules (the domain and global kinds also require `materiality >= MATERIAL`; `pump_promo_suspect` → None).
- `entry_session(t: datetime, sessions: Sequence[date]) -> date | None` and
  `pre_news_session(t: datetime, sessions: Sequence[date]) -> date | None` (the session whose close gives P0).
- `build_resolver(conn) -> EntityResolver`: aliases from the copy's `entity_aliases` with the curated-override rule of
  `EntityResolver.load`, the latest `sector_map` snapshot, `theme_map` keywords, universe None.
  `stock_symbols(resolver, s: Story) -> frozenset[str]` (headline as representative, entities as extra texts) and
  `company_symbols(resolver, names: Sequence[str]) -> tuple[list[str | None], int]` (one entry per name, None when
  unresolved or ambiguous; the int counts the Nones).
- `class Panel` and `load_panel(conn, start: date, as_of: date, cal) -> Panel`: regular sessions (NIFTY 50 bar dates
  that are regular sessions in `cal`), per-symbol open/high/low/close/volume matrices from `start`, structural
  ex-dates, `sector_map` snapshots, theme lists, ETF symbols.
- `liquid(p, d_prev) -> frozenset[str]`, `peers(p, sym, d_prev) -> frozenset[str]`,
  `group_members(p, group, d_prev) -> frozenset[str]`, `universe_daily(p, s: date) -> float | None` (the equal-weight
  close-to-close return of the names liquid as of the previous session, ex-date legs out), and
  `beta(p, names: frozenset[str], d_prev: date) -> float | None` (equal-weight basket of `names`, spec's window and
  pair minimum).
- `@dataclass(frozen=True) class Trade: key: str; signal_date: date; entry_date: date; exit_date: date;
  gross: float; bench: float; excess: float; net: float; legs: int = 1; fill_price: float | None = None`.
- `stock_trade(p, sym, d, hold, bench: Literal["sector", "beta"]) -> Trade | str`,
  `pullback_trade(p, sym, d, p0_session, hold) -> Trade | str` and `basket_trade(p, group, d, hold) -> Trade | str`.
  A str is the reason no trade exists: `no_prior_session`, `not_liquid`, `few_peers`, `no_beta`, `voided`,
  `no_bar`, `open` (the exit is past the data), `unfilled`, `small_basket`.
- `keep_non_overlapping(signals: Sequence[tuple[str, date]], sessions, hold) -> list[tuple[str, date]]`: one
  position per key at a time, greedily by date (`macro_mood_common.non_overlapping` per key).
- `cost() -> float` = one CNC round trip at the research notional, and
  `verdict(trades: Sequence[Trade], population_gross: Sequence[float], hold: int) -> dict`: the macro mood study's
  rule with this family's `MIN_TRADES`, `ALPHA` and `FOLD_PASS_MIN`, on `t.net`, with the permutation test on gross
  excess (`t.excess`) against `population_gross`.

- [ ] **Step 1: Write the failing tests** in `tests/offline/test_news_impact_common.py`. At least:
  `test_entry_session_cutoff` (09:14:59 → same day; 09:15:00 → next session; a Saturday → the next regular session;
  a holiday skipped), `test_pre_news_session` (11:00 on a session day → the previous session; 16:00 → that day;
  08:00 → the previous session), `test_first_score_wins` (a cluster scored twice keeps the earlier call's score and
  headline), `test_story_kind` (one case per spec rule, including a `market` story with `govt_program` → domain and a
  `market` story with `other` → None), `test_stock_trade_sector_excess` (a 3-session synthetic panel with known opens
  and closes: gross, the peers' mean, excess and net = excess − cost), `test_few_peers` (4 peers → `few_peers`),
  `test_pullback_fill` (low above P0 for two sessions, then an open below P0 → filled at that open; hold counted from
  the fill session), `test_pullback_unfilled`, `test_basket_beta_excess` (β computed from a synthetic history where the
  basket is exactly 2× the universe → β = 2.0 and excess ≈ 0), `test_voided_leg` (a structural ex-date inside the hold
  → `voided`), `test_non_overlap_per_key`, `test_resolver_curated_override`, and `test_verdict_labels` (refuted,
  insufficient, null cases).
- [ ] **Step 2: Run the tests; expect failures** (module missing).
- [ ] **Step 3: Implement** the interfaces above. Build the panel once per run and cache liquidity, universe returns
  and betas by session; the A sample needs about 60 entry sessions × ~700 liquid names.
- [ ] **Step 4: Run the tests; expect all pass,** then run `tests/offline/test_macro_mood_*.py` to show nothing there
  broke.

### Task 2: the mapper — `scripts/news_impact_map.py`

**Files:**
- Create: `scripts/news_impact_map.py`
- Test: `tests/offline/test_news_impact_map.py`

**Interfaces:**
- Consumes: `news_impact_common` (`load_first_scores`, `story_kind`, `entry_session`, `SAMPLES`, regular sessions),
  `macro_mood_llm.StudyLLM`, `news_content_score.call_refusal`, `news_content_score.VOCAB`.
- Produces: `DEFAULT_DB = data/research/news_impact.sqlite`; tables `llm_calls (at TEXT, model TEXT, prompt_sha256
  TEXT, cluster_ids TEXT /*JSON list*/, ok INTEGER, error TEXT, raw_output TEXT)` and `mappings (cluster_id TEXT
  PRIMARY KEY, call_rowid INTEGER, market INTEGER, groups TEXT /*JSON [[name, dir], ...]*/, companies TEXT /*JSON
  [[name, dir], ...]*/)`; `MAPPER_MODEL = "claude-sonnet-5-5"`, `BATCH = 20`, `MAX_CALLS_PER_NIGHT = 40`,
  `PROMPT_PATH = config/research/news_impact/mapper_prompt.md`, `MAPPER_PROMPT_SHA256` (the prompt file's sha256 at
  the time of writing; a test holds them equal, so an edit to the prompt stops the suite);
  `parse_mapping(item: dict) -> tuple[int, list[tuple[str, int]], list[tuple[str, int]]] | None`;
  `stories_to_map(stories, sessions, sample: str) -> list[Story]` (domain and global stories whose entry session is in
  the sample window, oldest first); `read_mappings(db: Path) -> dict[str, tuple[int, list, list]]`;
  `main(argv) -> int` with `--sample {A,excluded,B}`, `--state-db`, `--db`, `--max-calls` (default and ceiling 40),
  `--dry-run` (prints how many stories are mapped, unmapped and how many calls remain tonight; no call).

Rules (from the spec): one call per batch of up to 20 unmapped stories, numbered by `cluster_id` with the headline
only. The JSON schema has no length caps. `call_refusal(now)` is checked before every call, and a refusal stops the
run cleanly (exit 0, logged). The night's budget is 40 calls per IST date, counted from this store's own `llm_calls`.
The first error starting with one of `STOP_PREFIXES` stops the run; there are no retries. A story the model left out
stays unmapped for the next night. A lock file prevents two runs at once (as the content scorer does). The parser
keeps the first 3 in-vocabulary groups and the first 5 companies with a non-empty name. A direction outside {−1, +1}
(market: {−1, 0, +1}) drops that item; a bad market value becomes 0 and is counted. Exit 0 always, except 2 for a
prompt-hash mismatch or an unreadable store.

- [ ] **Step 1: Write the failing tests:** `test_prompt_hash_pinned`, `test_parse_mapping_trims` (4 groups, one
  out-of-vocabulary, a direction of 2, 6 companies → 3 groups and 5 companies, bad items dropped),
  `test_stories_to_map_window` (only domain and global kinds, entry session inside the window),
  `test_market_hours_refusal_stops_before_call` (an injected clock at 10:00 on a weekday makes no call),
  `test_cap_and_stop` (a fake `query_fn` returning an `overloaded` error stops after one call; a 40-call budget used
  today refuses), `test_store_append_only` (a second run maps only the remainder, never rewriting a mapping).
- [ ] **Step 2: Run; expect failures.**
- [ ] **Step 3: Implement.** Inject the clock and `query_fn` for tests.
- [ ] **Step 4: Run; expect all pass.** Then run `--dry-run --sample A` against the real stores and paste its output
  in the report: it makes no call and reads only.

### Task 3: the study — `scripts/news_impact_study.py`

**Files:**
- Create: `scripts/news_impact_study.py`
- Test: `tests/offline/test_news_impact_study.py`

**Interfaces:**
- Consumes: everything in `news_impact_common`; `news_impact_map.read_mappings`.
- Produces: `run(sample: str, state_db: Path, db: Path, mappings_db: Path, as_of: date) -> dict` and
  `render(doc: dict) -> str`; `main(argv) -> int` with `--sample {A,B}`, `--db`, `--state-db`, `--mappings`,
  `--as-of`, `--out` (default `data/reports/news_impact_<sample>_<stamp>.json`). Read-only; exit 0, or 2 on a refusal
  (missing store, data gate).

Rules (from the spec):
- If any story the sample needs (domain or global, entry session in the sample window or the excluded window) is
  unmapped, the report says "waiting: N stories unmapped" and computes no hypothesis.
- **The six hypotheses,** as the spec words them: `info`, `stock_next_open`, `stock_pullback`, `domain_companies`,
  `domain_groups`, `global_sectors`. Signals are per (key, D); several stories on one key for the same D make one
  signal. A key that another same-kind story for that D maps −1 is skipped.
- **Population for each trade hypothesis:** the same constructor applied to every eligible key (stocks: every liquid
  stock with enough peers or a β; groups: every vocabulary group with a basket) on the sample's own entry sessions.
  Gross excess values feed `verdict`.
- **`info`:** Spearman ρ and a whole-entry-session bootstrap percentile CI at `1 − ALPHA`; labels per the spec.
- **Diagnostics:** horizons; the supporting-data splits with `permutation_pvalue` against same-size subsets of the
  hypothesis's own trades (`SPLIT_DRAWS`); the negative side; the global market-direction correlation with the
  NIFTY's open(D) → close(D+4); fill rate; per-model and per-event-type splits; drop counts; the excluded window run
  through the same hypotheses and reported apart.
- The report lists every trade (key, signal date, entry, exit, gross, bench, excess, net), and a header with the
  windows, cost, N, alpha and the store paths.

- [ ] **Step 1: Write the failing tests:** `test_waiting_when_unmapped`, `test_signal_dedupe_same_day`,
  `test_conflicting_direction_skips`, `test_info_spearman_known` (a synthetic sample where y = x gives ρ = 1 and a CI
  above 0), `test_population_same_sessions`, `test_excluded_window_not_counted`, `test_report_roundtrip` (render runs
  on a small doc).
- [ ] **Step 2: Run; expect failures.**
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run all `tests/offline/test_news_impact_*.py` and `test_macro_mood_*.py`; expect all pass.** Do not run
  the study against real data: outcomes stay unseen until the owner signs off the registration.
