# Brief — NSE corporate announcements as a catalyst source + Business Standard RSS (2026-09-04)

Design authority: IMPLEMENTATION_PLAN.md §2.7, the paragraph "**Exchange announcements as a
first-class catalyst source**" (immediately before "### 2.8"). Read it first; it is binding.

## Scope (exactly this, nothing more)
1. `src/engine/datafeeds/news.py` — a new feed `nse_ann`:
   - Config: `news.feeds.nse_announcements: {enabled: bool, poll_s: int, drop_subjects: list[str]}`
     in `config/settings.yaml` (defaults: enabled true, poll_s 300, drop_subjects = a conservative
     administrative list: "Trading Window", "Loss of Share Certificate", "Compliances-Certificate",
     "Newspaper Publication", "Analysts/Institutional Investor Meet", "Change in Registrar",
     "Book Closure" — owner-editable; a subject is dropped on case-insensitive substring match).
     Add the typed field to the `NewsCfg` model in `src/engine/core/config.py` (find `NewsCfg`).
   - Fetch through the repo's NSE client (`src/engine/core/nse_http.py`: read `nse_get` — its
     cookie priming, retry and whether it is sync; the ISIN job in `src/engine/datafeeds/isin_map.py`
     already calls this exact endpoint, `NSE_ANNOUNCEMENTS_URL`, and parses `symbol`/`sm_isin` —
     reuse rather than duplicate). Endpoint: `https://www.nseindia.com/api/corporate-announcements?index=equities`.
     Inspect the live payload once (the engine is stopped; a single request is fine) and pin the
     field names you use in a module constant with a comment showing one sample row.
   - Each item → `Headline(title=f"[NSE:{symbol}] {desc}: {subject-or-attachment-text}"[:300],
     source_domain="nseindia.com", url=<attachment URL if present, else a deterministic
     announcement URL built from the item's sequence id>, published_at=<exchange timestamp, IST>)`.
     Dedupe stays URL-based (existing `_dedupe`).
   - Wire the feed key into `poll()`'s valid keys and the per-feed scheduler jobs in
     `src/engine/ops/main.py` (find where `news.feeds.rss` names become jobs; `nse_ann` gets its
     own cadence from `poll_s`). Health logging must use the existing `news_polled` /
     `news_feed_error` events with `feed="nse_ann"`.
2. `src/engine/datafeeds/news_pipeline.py` — explicit exchange token:
   - `EntityResolver.resolve` (line ~649): before alias matching, extract tokens matching
     `\[NSE:([A-Z0-9&\-]+)\]` from the representative (and from `extra_texts`); each token resolves
     to that symbol directly, subject to the SAME universe check (out-of-universe → the existing
     `UnresolvedEntity(reason="out_of_universe")` path). Alias matching still runs on the rest of
     the text. The token must never be matched as a free-text alias (strip it before alias matching).
   - `HeadlineClusterer` (line ~300-409): when a token-bearing headline joins or forms a cluster,
     it becomes the representative (so a filing merged with its press coverage stays resolvable).
     If the representative-selection code makes this awkward, the acceptable alternative is to
     pass member titles carrying tokens as `extra_texts` at resolution time — pick ONE, say why.
3. `config/settings.yaml` — add Business Standard RSS `bs_markets`
   (`https://www.business-standard.com/rss/markets-106.rss`) and `bs_companies`
   (`https://www.business-standard.com/rss/companies-101.rss`) at poll_s 900 with a
   "verified live 2026-09-04 (HTTP 200, newest item <20 min)" comment, and amend the 2026-08-05
   comment that rejected business-standard (WAF 403) to say it was re-probed live 2026-09-04.
4. Tests (inline, pasted output): `tests/unit/test_news_ingest*.py` (find the existing news tests
   and follow their fixture style) — parse a captured announcements payload (store a small JSON
   fixture under tests/fixtures/), drop-subject filtering, title/url/published_at shape, the feed
   key in `poll()`; `tests/unit/test_news_pipeline*.py` — token resolution (in-universe,
   out-of-universe, token stripped before alias matching, plain headlines unchanged), representative
   preference; config loading of the new block; the scheduler wiring test if one exists for feeds.
   Then run the FULL unit suite once inline and paste the summary line.

## Constraints
- Design-first: every decision the plan does not settle, state it in your final message.
- No new dependencies. No changes to `catalyst_guard`, `limits.yaml`, `envelope.yaml`.
- The engine service is stopped; never start or stop it. Do not write to `data/market.duckdb`.
- You are in an isolated git worktree: commit nothing; leave the changes in the working tree.

## Output contract
Final message = (a) every command + pasted output (tests, the full suite summary); (b) file:line
pointers for the fetcher, the token regex and resolution branch, the representative rule, the
config fields, the scheduler wiring; (c) the one sample payload row you pinned; (d) decisions you
made that the plan left open; (e) anything you could not do, with the reason.
