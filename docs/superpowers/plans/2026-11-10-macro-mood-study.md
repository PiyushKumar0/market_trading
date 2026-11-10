# Macro Mood-Signal Study Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the research scripts that run the ten pre-registered macro mood-signal hypotheses: six backtests now, and four forward tests with their article capture, nightly LLM scoring and sector mapping.

**Architecture:** Research scripts live under `scripts/` and never touch the engine.
- **Shared core:** `macro_mood_common.py` holds the registered constants, the timing rules, the trade construct and the verdict.
- **Backtests:** run once over stored external data and the research DuckDB copy.
- **Forward tests:** a scheduled capture job and a nightly scorer write a study-owned SQLite file. A forward script reads that file, the engine's append-only `agent_calls` (read-only) and a market snapshot.

**Tech Stack:** Python 3.12 (project `.venv`), pandas, numpy, duckdb, sqlite3, PyYAML, stdlib `urllib`/`xml`/`zoneinfo`, `claude_agent_sdk` (through the engine's option builder), pytest.

**Spec:** `IMPLEMENTATION_PLAN.md`, entry "Macro mood-signal study" (registered in commit 9722e00). The `config/research_trials.yaml` rows `edge-macro_mood_*-20261110`, `config/research/macro_mood/theme_lists.yaml` and `config/research/macro_mood/mapper_prompt.md` are frozen by that commit; never edit them.

## Global Constraints

- Every threshold, window, sample date and pass criterion comes verbatim from the spec. Changing one is a new registration; a task that seems to need a change stops and reports.
- Family `edge/macro_mood`, N = 10, `registration_alpha(10)` = 0.005, `fold_pass_min(10)` = 0.60, at least 30 trades.
- **Data access:**
  - Never write to `data/state.db` or `data/market.duckdb`.
  - Read `state.db` through `file:...?mode=ro`, and DuckDB through `research_common.open_snapshot`, which refuses the live store.
  - Backtests and the scorer refuse market hours (`research_common.market_hours_refusal`). The capture job is exempt: it touches no engine store and no LLM.
- No engine change, no new dependency, never `uv sync`/`uv add`. Only stdlib and what `.venv` already has.
- **Returns and costs:** returns are in percent units (1.25 means +1.25%), matching `research_common`. Costs come from `CostModel.from_config().breakeven_pct(research_common.NOTIONAL, product)`: product "MIS" for the same-day arm, "CNC" for everything else.
- **Timing:**
  - Yahoo bar dates use `zoneinfo.ZoneInfo(meta["exchangeTimezoneName"])`, never `meta["gmtoffset"]`.
  - All engine-side times are IST (`Asia/Kolkata`). The news cut-off is 09:00:00 IST on D, exclusive.
- **Repo practice:** tests go in `tests/offline/`, run with `.venv\Scripts\python.exe -m pytest tests\offline\<file> -q -p no:cacheprovider`. Shell is PowerShell. Commit with `git commit -F <msgfile>` and no co-author trailer. Comments state constraints, never history.
- **LLM calls:**
  - Models `claude-haiku-5-5` (H and C scoring) and `claude-sonnet-5-5` (mapper), through the engine's option builder.
  - At most 40 calls a night. Stop at the first rate-limit, session-limit or auth failure, with no retries.
  - Never write to the engine's `agent_calls` or budget tables.

## Review Focus

1. **DST weeks.** A Yahoo bar stamped at New York midnight in the weeks around the March and November switches must land on its own session date, never the day before. The test is in Task 1.
2. **Missing sessions.**
   - A US holiday leaves no new US close before D, so D has no US signal. An Indian holiday makes D compound every US session since the previous NIFTY session.
   - A participant-OI gap (a 404 day) voids any 3-session change that spans it.
   - Tests are in Task 1 and Task 4.
3. **Corporate actions and ETFs.** A split ex-dated D−1 must not count as a "down" stock in breadth, and an ETF never enters the liquid universe or a basket. The test is in Task 4.
4. **Feed and page failures.** A feed that 500s, malformed XML, a 403 or paywalled page, a duplicate URL, or a re-published item with an edited title must not crash the capture. Each URL is stored once and its body fetched once. A 403 keeps the summary. The test is in Task 2.
5. **Cut-off boundaries.** An `agent_calls.at` or capture time of exactly 09:00:00 IST belongs to the *next* session's window, while 08:59:59 belongs to D. A session with no scored market cluster has no tone. Tests are in Task 6 and Task 7.

---

### Task 1: Shared core (`macro_mood_common.py`)

**Files:**
- Create: `scripts/macro_mood_common.py`
- Test: `tests/offline/test_macro_mood_common.py`

**Interfaces:**
- **Consumes:**
  - from `research_common`: `NOTIONAL`, `Refusal`, `open_snapshot`, `market_hours_refusal`, `write_reports`, `default_out`;
  - `backtest_brk20.cpcv_report(rows, horizon, cost_pct, trial_count)`, where rows carry `.fill_date` and `.net[horizon]` (see `research_common.cpcv`);
  - `engine.learning.multiplicity.permutation_pvalue(sample, population, *, draws=10_000, seed)`;
  - `backtest_hvol.is_etf(symbol, etf_symbols)`.
- **Produces:**
  - Registered constants, exactly:
    - `FAMILY = "edge/macro_mood"`, `N_TRIALS = 10`, `ALPHA = 0.005`, `FOLD_PASS_MIN = 0.60`, `MIN_TRADES = 30`, `PERM_SEED = 20261110`;
    - `US_FALL_PCT = -2.0`, `US_RISE_PCT = 1.0`, `OIL_DROP_PCT = -5.0`, `OIL_SESSIONS = 3`;
    - `FII_SESSIONS = 3`, `FII_WINDOW = 250`, `FII_PCTL = 90`;
    - `BREADTH_MIN_PCT = 75.0`, `LIQUID_MIN_TV = 1e8`, `LIQUID_WINDOW = 20`;
    - `Z_WINDOW = 250`, `COMBINED_TOP = 0.10`;
    - `HOLD = {"us_rebound": 1, "us_follow": 5, "oil_drop": 5, "fii_short_cover": 5, "breadth_thrust": 5, "combined": 5, "news_tone": 5, "policy": 10}`;
    - `PRODUCT` ("MIS" only for us_rebound);
    - `EXCLUDED = (date(2026,9,1), date(2026,11,9))`;
    - `SAMPLES` (the spec's confirm and seen windows per hypothesis);
    - `REGISTERED_FROM = date(2026,11,11)`.
  - `yahoo_daily(path: Path) -> pd.DataFrame`: index is the exchange-local `date`; columns are open, high, low, close as floats; null-close bars are dropped.
  - `us_overnight(nifty_days: Sequence[date], close: pd.Series) -> pd.Series`: indexed by NIFTY day D. The value is the compounded % return of the series' sessions u with D_prev ≤ u < D, or NaN when there are none.
  - `brent_change(nifty_days: Sequence[date], close: pd.Series, n: int = 3) -> pd.DataFrame`: indexed by D, with columns:
    - `pct`: the latest Brent close dated before D versus the one n Brent sessions earlier, in %;
    - `roll_flag`: True if the n+1 compared sessions meet [e−2, e+1] for any mapped ICE Brent expiry session e.
      Expiries come from the per-era calendar in the spec: before the March 2016 contract, the business day before the 15th calendar day before delivery; from it, the last weekday of month M−2. Each maps to the last BZ=F session on or before it.
  - `trailing_pctl_signal(x: pd.Series, window: int, pctl: float) -> pd.Series[bool]`: True where x exceeds the given percentile of its previous `window` values (strictly earlier). False while fewer than `window` values exist.
  - `zscore_trailing(x: pd.Series, window: int) -> pd.Series`: uses the mean and sample SD of the previous `window` values; NaN before that.
  - `@dataclass(frozen=True) TradeRow(signal_date: date, entry_date: date, exit_date: date, gross: float, net: float, flagged: bool = False)`.
  - `index_trade(px: pd.DataFrame, sessions: Sequence[date], d: date, hold: int) -> tuple[date, float] | None`: gross % from open(d) to the close of the hold-th session counting d, as `(exit_date, gross)`. None when the bars run out.
  - `non_overlapping(signal_days: Sequence[date], sessions: Sequence[date], hold: int) -> list[date]`: greedy, in date order. A signal is skipped while a position opened at an earlier kept signal is still held.
  - `cost_pct(product: str) -> float`.
  - `verdict(trades: list[TradeRow], population_gross: Sequence[float], hold: int, cost: float) -> dict`: returns `label` ∈ {"supported","refuted","insufficient","null"} plus n, mean_net, median_net, perm_p, fold_pass, half1_mean, half2_mean, drop_top3_mean and margin_floor_promotable (a diagnostic).
  - `liquid_universe(conn, d_prev: date, etf_symbols: frozenset[str]) -> set[str]`.

- [ ] **Step 1: Write the failing tests**

```python
def test_bzf_bar_stamped_new_york_midnight_in_summer_is_its_own_session(tmp_path):
    # 2026-10-30 04:00 UTC = 00:00 EDT -> session 2026-10-30, not 10-29
    df = yahoo_daily(write_chart(tmp_path, tz="America/New_York", ts=[1793332800], close=[117.64]))
    assert df.index[0] == date(2026, 10, 30)

def test_dst_switch_weeks_keep_session_dates(tmp_path):
    # midnight-local stamps on 2026-03-06 (EST) and 2026-03-09 (EDT)
    df = yahoo_daily(write_chart(tmp_path, tz="America/New_York",
                                 ts=[local_midnight_utc("2026-03-06"), local_midnight_utc("2026-03-09")], close=[1, 2]))
    assert list(df.index) == [date(2026, 3, 6), date(2026, 3, 9)]

def test_us_overnight_compounds_us_sessions_between_nifty_sessions():
    nifty = [date(2026, 11, 3), date(2026, 11, 5)]          # 11-04 an Indian holiday in this fixture
    spx = pd.Series([100.0, 101.0, 99.99], index=[date(2026, 11, 2), date(2026, 11, 3), date(2026, 11, 4)])
    r = us_overnight(nifty, spx)
    assert r[date(2026, 11, 5)] == pytest.approx((99.99 / 100.0 - 1) * 100)   # u = 11-03 and 11-04

def test_us_overnight_nan_when_no_new_us_close():
    nifty = [date(2026, 11, 26), date(2026, 11, 27)]          # US Thanksgiving 11-26: no US session
    spx = pd.Series([100.0], index=[date(2026, 11, 25)])
    assert math.isnan(us_overnight(nifty, spx)[date(2026, 11, 27)])

def test_brent_change_uses_latest_settlement_before_d_and_flags_month_end():
    brent = pd.Series([113.88, 115.32, 114.88, 117.64, 104.77],
                      index=[date(2026, 10, 27), date(2026, 10, 28), date(2026, 10, 29), date(2026, 10, 30), date(2026, 11, 2)])
    out = brent_change([date(2026, 11, 2), date(2026, 11, 3)], brent, n=3)
    assert out.loc[date(2026, 11, 2), "pct"] == pytest.approx((117.64 / 113.88 - 1) * 100)   # 11-02 itself excluded
    assert bool(out.loc[date(2026, 11, 2), "roll_flag"]) is True                               # 10-30 = last weekday of Oct
    assert out.loc[date(2026, 11, 3), "pct"] == pytest.approx((104.77 / 115.32 - 1) * 100)

def test_trailing_pctl_signal_uses_only_strictly_earlier_values():
    x = pd.Series([1.0] * 250 + [1.0, 5.0])
    sig = trailing_pctl_signal(x, window=250, pctl=90)
    assert not sig.iloc[249] and not sig.iloc[250] and sig.iloc[251]

def test_non_overlapping_skips_signals_inside_hold():
    s = sessions(10)
    assert non_overlapping([s[0], s[2], s[5], s[6]], s, hold=5) == [s[0], s[5]]

def test_index_trade_counts_entry_session_as_one():
    px = frame(opens=[100.0, 102.0], closes=[101.0, 103.0], days=sessions(2))
    assert index_trade(px, sessions(2), sessions(2)[0], hold=1) == (sessions(2)[0], pytest.approx(1.0))
    assert index_trade(px, sessions(2), sessions(2)[0], hold=2) == (sessions(2)[1], pytest.approx(3.0))
    assert index_trade(px, sessions(2), sessions(2)[1], hold=2) is None

def test_verdict_labels():
    pop = [0.0] * 1000
    assert verdict(trades(29, net=2.0), pop, 5, 0.3)["label"] == "insufficient"
    assert verdict(trades(30, net=-0.1), pop, 5, 0.3)["label"] == "refuted"
    assert verdict(trades(40, gross=3.0, net=2.7), pop, 5, 0.3)["label"] == "supported"
    assert verdict(trades(40, gross=3.0, net=2.7, second_half_net=-1.0), pop, 5, 0.3)["label"] == "null"
```

- [ ] **Step 2: Run them; expect failures (`ModuleNotFoundError: macro_mood_common`).**

Run: `.venv\Scripts\python.exe -m pytest tests\offline\test_macro_mood_common.py -q -p no:cacheprovider`

- [ ] **Step 3: Implement the module to the Interfaces above.**
  - `verdict` builds the CPCV rows as `SimpleNamespace(fill_date=t.entry_date, net={hold: t.net})` and reads the fold-pass fraction from `cpcv_report`.
  - Halves are the first ⌊n/2⌋ trades by entry date versus the rest.
  - `perm_p` is `permutation_pvalue([t.gross ...], population_gross, seed=PERM_SEED)`.
  - `liquid_universe` computes the 20-session median of `close*volume` from `bars_1d`. It drops `is_etf` names (NSE list `data/research/nse_etf_list_2026-10-09.csv`) and names with a `corp_actions.ex_date == d_prev`.

- [ ] **Step 4: Run the tests; expect PASS.**

- [ ] **Step 5: Commit** (`scripts/macro_mood_common.py`, the test file), with the message "Macro mood study: shared constants, timing rules and verdict".

---

### Task 2: Content capture (`news_content_capture.py`)

Build this second, so content starts accruing as early as possible.

**Files:**
- Create: `scripts/news_content_capture.py`
- Test: `tests/offline/test_news_content_capture.py` (fixtures under `tests/offline/fixtures/news_content/`: two RSS XML files, one JSON-LD article page, one `<p>`-only page)

**Interfaces:**
- **Produces:** the SQLite schema in `data/research/news_content.sqlite`, created if absent:
  - `items(url TEXT PRIMARY KEY, feed TEXT, title TEXT, summary TEXT, published_at TEXT, first_captured_at TEXT NOT NULL)`;
  - `bodies(url TEXT PRIMARY KEY, fetched_at TEXT NOT NULL, http_status INTEGER, source TEXT CHECK(source IN ('jsonld','p','none')), text TEXT)`;
  - `capture_runs(run_at TEXT, feed TEXT, items_seen INTEGER, items_new INTEGER, error TEXT)`.
  - Times are ISO-8601 with the +05:30 offset.
- `FEEDS: dict[str, str]`: the nine RSS URLs exactly as in `config/settings.yaml` `news.feeds.rss` on 2026-11-10 (et, livemint_markets, livemint_companies, hbl_markets, hbl_companies, cnbctv18_market, ndtvprofit, bs_markets, bs_companies). This is a frozen copy, not read from settings.
- `parse_feed(xml_bytes: bytes) -> list[dict]` with keys url, title, summary, published_at. Tags are stripped and HTML entities unescaped; published_at is IST or None.
- `extract_body(html: str) -> tuple[str, str]` returns (source, first 400 words). It uses JSON-LD `articleBody` from a `NewsArticle`-like object; else it joins the `<p>` blocks of at least 8 words; else `("none", "")`.
- `capture(db: Path, fetch: Callable[[str], tuple[int, bytes]], now: Callable[[], datetime]) -> dict`: one pass over FEEDS. It inserts new urls (the first capture wins and an edited title never overwrites), then fetches each new url's page once, sleeping at least 2 s between page requests. Returns counts.
- CLI: `news_content_capture.py [--db PATH]`, which always exits 0 and records errors in `capture_runs`.
- **Overlap guard:** a run first creates `news_content.lock` beside the db, with exclusive create. If the lock exists and is under 30 minutes old, the run exits 0 without work. An older lock is treated as stale and replaced.

- [ ] **Step 1: Write the failing tests:**
  - `test_parse_feed_strips_tags_and_keeps_summary`;
  - `test_extract_body_prefers_jsonld_then_paragraphs_then_none`;
  - `test_capture_stores_each_url_once_and_never_refetches`, where a second pass with an edited title leaves `items.title` and `bodies.fetched_at` unchanged;
  - `test_capture_403_page_keeps_summary_and_records_status`;
  - `test_feed_500_and_malformed_xml_are_logged_not_raised`;
  - `test_body_cut_to_400_words`;
  - `test_fresh_lock_skips_run_and_stale_lock_is_replaced`.
- [ ] **Step 2: Run them; expect failures.**
- [ ] **Step 3: Implement to the Interfaces.** Use the `urllib.request` fetch with the `Mozilla/5.0` user agent (the default injected `fetch`) and `xml.etree.ElementTree`.
- [ ] **Step 4: Run the tests; expect PASS.** Then do one live dry run: `.venv\Scripts\python.exe scripts\news_content_capture.py --db $env:TEMP\nc_probe.sqlite` should print per-feed counts greater than 0 for at least 5 feeds.
- [ ] **Step 5: Commit** with the message "Macro mood study: RSS summary and article capture".

---

### Task 3: External data fetch (`fetch_macro_external.py`)

**Files:**
- Create: `scripts/fetch_macro_external.py`
- Test: `tests/offline/test_fetch_macro_external.py` (fixtures: three participant-OI CSVs copied from the 2012, 2022 and 2026 header variants)

**Interfaces:**
- **Produces** under `data/research/external/`:
  - `yahoo/<symbol>.json`: the raw chart JSON for `^NSEI`, `^GSPC`, `^IXIC` and `BZ=F`, with `period1` = 2007-01-01 and `period2` = the run time;
  - `nse_oi/fao_participant_oi_DDMMYYYY.csv`, one file per NIFTY session in the `^NSEI` file from 2012-01-02 to the last session;
  - `manifest.json`, mapping each file to its url, fetched_at, bytes and sha256, plus `missing` (dates that answered 404).
- `parse_participant_oi(csv_bytes: bytes) -> dict | None` returns `{"fut_index_long": int, "fut_index_short": int}` for the FII/FPI row. Column names are matched after lowercasing and collapsing whitespace and tabs. The client type is in {"FII","FPI"}. Returns None when the row is absent.
- `fii_long_share(oi_dir: Path) -> pd.Series`: indexed by date, valued long/(long+short).
- CLI: `fetch_macro_external.py [--out DIR] [--resume]`.
  - At least 1 s between NSE requests, with a browser user agent and an `https://www.nseindia.com/` Referer.
  - Without `--resume` it refuses (exit 2) to overwrite an existing manifest; with `--resume` it fetches only the files the manifest lacks.

- [ ] **Step 1: Write the failing tests:**
  - `test_parse_oi_header_variants` (all three fixtures give the same keys);
  - `test_parse_oi_missing_fii_row_returns_none`;
  - `test_long_share_from_parsed_rows`;
  - `test_manifest_refuses_overwrite_without_resume`.
- [ ] **Step 2: Run them; expect failures.**
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run the tests; expect PASS.**
- [ ] **Step 5: Commit** with the message "Macro mood study: external data fetch with manifest".

---

### Task 4: Backtests (`macro_mood_study.py`)

**Files:**
- Create: `scripts/macro_mood_study.py`
- Test: `tests/offline/test_macro_mood_study.py` (synthetic DuckDB plus synthetic Yahoo files built in `tmp_path`)

**Interfaces:**
- **Consumes:** Task 1 (everything) and Task 3 (`yahoo/*.json`, `fii_long_share`).
- **Produces:**
  - `signals(name: str, ctx: StudyContext) -> pd.DataFrame`, indexed by D, with `signal: bool`, `flagged: bool` and `value: float`. Names are `us_rebound`, `us_follow`, `oil_drop`, `fii_short_cover`, `breadth_thrust` and `combined`, plus `combined_arm_us`, `combined_arm_oil` and `combined_arm_fii`.
  - `run(ctx) -> dict`, the report document.
  - CLI: `macro_mood_study.py --db data/research/full_market_2026-11-09.duckdb --external data/research/external`, which writes `data/reports/macro_mood_<ts>.json` and `.md` through `write_reports`. Exit 0 means ran; exit 2 means refused (market hours, the live store, a missing manifest file).
- **Signal definitions:** the spec, verbatim.
  - **Breadth:** the share of `liquid_universe(conn, D−1)` names whose D−1 close exceeds their D−2 close.
  - **Combined:** the mean of `zscore_trailing` of `us_overnight`, of −`brent_change.pct` and of the 3-session change in `fii_long_share`, all three required. It signals in the top 10% of its previous 250 values.
  - **NIFTY prices:** `^NSEI` before 2022-07-12 and `bars_1d` "NIFTY 50" from then on.
  - **`nsei_gate(yahoo: pd.DataFrame, db_nifty: pd.DataFrame) -> dict`** returns `ok`, `clean_days`, `dropped_by_year` and `failed_years`.
    - `ok` is False when any overlapping open differs from `bars_1d`. That makes both US hypotheses and the combined score insufficient.
    - Before the overlap, a day whose open equals the previous close or lies outside [low, high] is dropped from the open-anchored arms.
    - A year losing more than 10% of its sessions is excluded and reported.
- **Per hypothesis:**
  - Kept signals in each window go through `non_overlapping`, then become trades.
  - The population is every session's same-construct gross return in the same window.
  - The verdict is computed on the confirm window. Seen windows are reported under "seen (not evidence)". `EXCLUDED` is never sampled; the motivating window's trades are listed separately.
  - **Oil drop:** the verdict is computed on all signals and on unflagged signals only. If the two labels differ, the reported label is "null".
  - **Combined:** each arm is reported with its mean net. `combining_helps` is True iff the combined mean net exceeds every arm's.
- **Diagnostics block** (labelled as deciding nothing):
  - horizons 1, 3 and 10;
  - a split by `engine.strategy.regime.index_regime` on closes through D−1;
  - a split by year;
  - a 2.5×ATR(14) stop arm (exit at the stop when a daily low touches it);
  - an ETF-cost arm (the equity STT share of the CNC round trip replaced by 0.001% on the sell leg);
  - Nasdaq in place of the S&P for both US hypotheses;
  - a high-beta basket arm for 2022-07-14 → 2026-08-31: the top quintile by 250-session NIFTY beta among the 200 names with the highest median traded value as of D−1, equal-weight, one CNC round trip per leg, legs with an ex-date in their window dropped.

- [ ] **Step 1: Write the failing tests:**
  - `test_breadth_counts_only_liquid_non_etf_names_and_ignores_split_on_d_minus_1`;
  - `test_us_rebound_trade_is_same_day_open_to_close_with_mis_cost`;
  - `test_excluded_window_never_sampled`;
  - `test_oil_drop_flag_disagreement_reports_null`;
  - `test_combined_requires_all_three_inputs`;
  - `test_fii_signal_voided_across_missing_oi_day`;
  - `test_report_marks_seen_windows_not_evidence`;
  - `test_refuses_live_store_path`;
  - `test_nsei_gate_drops_bad_opens_and_excludes_a_year_over_10pct`;
  - `test_nsei_gate_overlap_mismatch_makes_us_hypotheses_insufficient`.
- [ ] **Step 2: Run them; expect failures.**
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run the tests; expect PASS.**
- [ ] **Step 5: Commit** with the message "Macro mood study: the six registered backtests".

---

### Task 5: Study LLM wrapper (`macro_mood_llm.py`)

**Files:**
- Create: `scripts/macro_mood_llm.py`
- Test: `tests/offline/test_macro_mood_llm.py` (with an injected fake `query_fn`; it never imports the real SDK)

**Interfaces:**
- **Consumes:** the option-building pattern of `scripts/replay_agents.py:200-206`.
  - Load the roster with `load_agent_roster(load_yaml(config_dir()/"agents.yaml")).defs` and get `cli_path` from `resolve_cli_path(cfg, repo_root())`.
  - Build options with `AgentHarness(defs, None, Clock(), None, cli_path=cli_path)._build_options(defs["news_analyst"], system_prompt=..., json_schema=..., max_turns=1)`, then `dataclasses.replace(options, model=<api id>)`.
  - Read results from `ResultMessage.structured_output`. A result error text, or an exception classified by `engine.intelligence.harness._classify`, is a failure.
- **Produces:**
  - `@dataclass LLMResult(ok: bool, output: dict | None, error: str | None, model: str, at: datetime, duration_s: float)`.
  - `class StudyLLM`:
    - `__init__(self, *, max_calls: int, query_fn=None)`;
    - `async def call(self, *, model: str, system_prompt: str, prompt: str, json_schema: dict) -> LLMResult`.
    - A cap of `max_calls` raises `CallCapReached`.
    - The first failure classified as rate-limit, session-limit or auth sets `self.stopped = True`. Every later call raises `LLMStopped`, with no retry.
    - Each result is appended to the caller's call log, never to the engine's tables.

- [ ] **Step 1: Write the failing tests:**
  - `test_options_carry_pinned_model_and_isolation`, where the options' model equals the passed api id and `tools == []`;
  - `test_cap_raises_after_max_calls`;
  - `test_session_limit_stops_all_later_calls`;
  - `test_result_error_text_is_failure_not_output`.
- [ ] **Step 2: Run them; expect failures.**
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run the tests; expect PASS.**
- [ ] **Step 5: Commit** with the message "Macro mood study: pinned-model LLM wrapper with a nightly cap".

---

### Task 6: Nightly scoring and mapping (`news_content_score.py`)

**Files:**
- Create: `scripts/news_content_score.py`
- Test: `tests/offline/test_news_content_score.py`

**Interfaces:**
- **Consumes:**
  - Task 2's tables;
  - Task 5's `StudyLLM`;
  - the engine's `HeadlineClusterer(sim_threshold=0.75).cluster(headlines)` over `engine.datafeeds.news.Headline(title, source_domain, url, published_at)`;
  - `news_analyst.SYSTEM_PROMPT`, `news_analyst.output_json_schema()` and `news_analyst.parse_output(raw)`;
  - the frozen `config/research/macro_mood/mapper_prompt.md` and `theme_lists.yaml`;
  - `state.db` `agent_calls` (`agent_id='news_analyst'`, `ok=1`), read-only.
- **Produces** tables in the same SQLite file:
  - `clusters(cluster_key TEXT PRIMARY KEY, first_captured_at TEXT, representative_url TEXT, member_urls TEXT)`;
  - `llm_calls(call_id TEXT PRIMARY KEY, at TEXT, purpose TEXT CHECK(purpose IN ('score_H','score_C','map_headline','map_content')), model TEXT, prompt_sha256 TEXT, ok INTEGER, error TEXT)`;
  - `scores(cluster_key TEXT, variant TEXT CHECK(variant IN ('H','C')), scope TEXT, event_type TEXT, sentiment REAL, materiality REAL, novelty REAL, call_id TEXT, PRIMARY KEY(cluster_key, variant))`;
  - `mappings(candidate_key TEXT, source TEXT CHECK(source IN ('headline','study_h','content')), groups TEXT, call_id TEXT, PRIMARY KEY(candidate_key, source))`.
    - A `headline` candidate's key is the engine's `cluster_id`.
    - A `study_h` candidate is a study cluster whose H score meets the policy bars. It is mapped from the headline only and feeds the headlines-versus-content comparison.
    - A `content` candidate is a study cluster whose C score meets the bars.
- **Function and constants:**
  - `run_night(content_db: Path, state_db: Path, llm: StudyLLM, now: datetime) -> dict`.
  - `VOCAB`: the 23 sector labels and 7 themes exactly as in `mapper_prompt.md`, in that order.
  - The mapper schema is `{"type":"object","properties":{"groups":{"type":"array","items":{"type":"string","enum":VOCAB}}},"required":["groups"],"additionalProperties":false}`, with no length caps.
- **Prompt layout:**
  - H uses exactly the engine's News Analyst layout (`ContextAssembler.for_news_batch`, `src/engine/intelligence/context.py:348-361`) over the eight `themes.yaml` theme names.
  - C adds `   summary: <summary>` and `   excerpt: <body text>` lines under each headline. The excerpt is used only if `bodies.fetched_at` is earlier than the cluster's scoring cut-off.
  - Batches hold at most 30 clusters.
- **Night order:**
  1. Cluster new items.
  2. Score H then C for unscored clusters (Haiku).
  3. Map headline candidates from `agent_calls` scored since the last run, with `event_type` in {govt_program, sector_policy}, sentiment ≥ 0.4 and materiality ≥ 0.6, using the headline from that call's context (Sonnet).
  4. Map the same bars on C scores, with headline, summary and excerpt (Sonnet).
  5. Map the same bars on the study's H scores, headline only (Sonnet), into `source='study_h'`.
- **Stopping and priority:** the night stops on `CallCapReached` or `LLMStopped` and resumes from what is left the next night.
  - Priority is forward-window work first: clusters and candidates from 09:00 IST on 2026-11-10 onward, oldest first.
  - The seen-period backlog comes after that: headline candidates from 2026-07-16 until then, reported as seen.
  - The mapper takes one story per call. The parser keeps at most the first three in-vocabulary groups.
  - Refuse market hours.
- CLI: `news_content_score.py [--content-db PATH] [--state-db data/state.db] [--max-calls 40]`.

- [ ] **Step 1: Write the failing tests** (with a fake `StudyLLM`):
  - `test_h_and_c_prompts_differ_only_by_summary_and_excerpt_lines`;
  - `test_excerpt_omitted_when_body_fetched_after_cutoff`;
  - `test_headline_candidates_read_from_agent_calls_only_ok_news_analyst`;
  - `test_mapper_keeps_first_three_in_vocab_groups`;
  - `test_night_stops_at_cap_and_resumes_next_night`;
  - `test_never_writes_state_db` (the db opened `mode=ro`; a write attempt raises).
- [ ] **Step 2: Run them; expect failures.**
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run the tests; expect PASS.**
- [ ] **Step 5: Commit** with the message "Macro mood study: nightly H/C scoring and policy mapping".

---

### Task 7: Forward evaluation (`macro_mood_forward.py`)

**Files:**
- Create: `scripts/macro_mood_forward.py`
- Test: `tests/offline/test_macro_mood_forward.py`

**Interfaces:**
- **Consumes:**
  - Task 1's verdict, trade and non-overlap functions and its constants;
  - Task 6's tables;
  - `state.db` `agent_calls` (read-only);
  - a market snapshot with `bars_1d`, `sector_map`, `corp_actions` and the NIFTY 50 rows;
  - `theme_lists.yaml`.
- **Produces:** `evaluate(state_db: Path, content_db: Path, snapshot: Path, as_of: date) -> dict` and the CLI `macro_mood_forward.py --state-db ... --content-db ... --db <snapshot>`. It is read-only, always exits 0 and prints `UNAVAILABLE` for an unreadable store.
- **Window boundaries:**
  - Tone(D) and policy candidates for D use D's news window, [09:00 IST on the previous regular session, 09:00 IST on D), inclusive below and exclusive above, so weekend and holiday news counts. The time is `agent_calls.at` for headlines and `clusters.first_captured_at` for content.
  - **Headline key:** the `(model, prompt_version)` of the first ok News Analyst call at or after 2026-11-11 00:00 IST. A call in any window with another key is excluded and counted.
  - The content start is the first regular session after the capture's first complete 09:00→09:00 day in `capture_runs`.
  - **Pinned-model health:** the report shows each pinned model's last ok call date. A pinned model retired before a look forms makes that hypothesis insufficient unless the owner re-registers.
- **Signals and trades:**
  - News tone signals on the top third of its previous sessions, using at most 60 of them and needing at least 20.
  - Policy-push trades:
    - Each group basket holds the liquid members as of D−1: from `sector_map` for sectors, from the frozen list for themes.
    - Legs are equal-weight, each paying one CNC round trip. A leg with an ex-date in its window is dropped.
    - The trade is judged on excess over the equal-weight liquid universe. There is one position per group at a time.
  - **Verdict timing:** a verdict forms at the 30th closed trade. With fewer than 30 closed trades at `REGISTERED_FROM` + 18 months, the label is insufficient. Before either, the report says "accruing n/30".
- **Comparison** (decides nothing):
  - Tone: the Spearman rank correlation of tone(D) with NIFTY open(D)→close(D+4), for C minus H, with a moving-block bootstrap 95% CI (block 5, 2,000 draws, seed `PERM_SEED`).
  - Policy: the mean 10-session excess of C-identified events (`mappings.source='content'`) minus H-identified ones (`source='study_h'`). The 95% CI resamples whole entry sessions, and each side needs at least 10 events, else "not enough data".
  - The answer is "content adds leverage", "content hurts" or "no measurable difference", per the spec.

- [ ] **Step 1: Write the failing tests:**
  - `test_cutoff_0900_exclusive_boundary`;
  - `test_session_without_market_scores_has_no_tone`;
  - `test_other_prompt_version_excluded_and_counted`;
  - `test_policy_basket_excess_over_ew_universe_with_leg_costs`;
  - `test_accruing_until_30_closed_then_verdict`;
  - `test_insufficient_after_18_months`;
  - `test_comparison_ci_answer_mapping`.
- [ ] **Step 2: Run them; expect failures.**
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run the tests; expect PASS.**
- [ ] **Step 5: Commit** with the message "Macro mood study: forward looks and the headlines-versus-content comparison".

---

### Task 8: Operate (manager only, outside 09:15–15:30 IST)

**Files:**
- Modify: `runbooks/COMMANDS.md`: add a "Macro mood study" section with the four run commands and the two scheduled-task commands.
- Modify: `runbooks/WORKLOG.md`: add an entry when the tasks are created and the backtests run.

- [ ] **Step 1:** Run `fetch_macro_external.py`. Check that the manifest covers `^NSEI`, `^GSPC`, `^IXIC` and `BZ=F`, and the OI dates 2012-01-02 → last session, with `missing` listed.
- [ ] **Step 2:** Run `macro_mood_study.py` against `data/research/full_market_2026-11-09.duckdb`. Record each backtest label in its ledger row's `result` and `report`, and in memory `macro-mood-study`. The owner sees the report before any label is called evidence.
- [ ] **Step 3:** Create two scheduled tasks, with the exact `schtasks /Create` lines in COMMANDS.md:
  - `MT-NewsContentCapture`, every 15 minutes, all days;
  - `MT-NewsContentScore`, daily at 22:00.

  Both run the project `.venv` python under the account that owns the engine's Claude CLI login. If creation needs the owner's password, hand the owner the commands.
- [ ] **Step 4:** After the first full capture day, confirm `capture_runs` shows every feed polled. After the first 22:00 run, confirm `llm_calls` has ok rows and at most 40 calls.
