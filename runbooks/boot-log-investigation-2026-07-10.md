# Boot-Log Investigation — 2026-07-10

**Context:** `uv run python -m engine.ops.main` on the dev box
**Boot state:** `mode=OFF`, `risk_state=FROZEN`, `integrity_ok=false`, `off_duration_s≈6328`
**Frozen at startup:** `protected_store:limits.yaml`, `protected_store:envelope.yaml`, `data_freshness:instruments`, `warmup_ready`

---

## TL;DR

The `frozen=[...]` list has four entries but **at least four independent roots** — this is **not one cascade**. The investigation surfaced **two critical code bugs**, one lower-severity feed code bug, one transient feed failure, and two one-time owner setup steps.

> **Login-token note:** none of these issues are caused by the missing daily Kite login. Credentials (`KITE_API_KEY`) are seeded — a real `KiteClient` was built (no `kite_client_absent` warning), which is why A1 raises `AttributeError` rather than the `RuntimeError("Kite client unavailable")` from `_require_kite`. `token_valid` even reported **PASS**, because `SessionManager` tracks validity *behaviourally* off a stored token (a Phase-0 leftover in the DPAPI store) and only marks it dead on a live 403 — which never fired this boot because no authenticated call reached the network. The instruments *dump* endpoint is a public bulk download and does not require the daily token; the token only becomes load-bearing once you pull live quotes / historical EQ candles / place orders.

---

## Issue summary

| # | Issue | Type | Severity | Self-heals? |
|---|-------|------|----------|-------------|
| **A1** | `data_freshness:instruments` freeze | Code bug | 🔴 critical | No — needs fix |
| **A2** | `backfill_unknown_token` NIFTY 50 / INDIA VIX → warmup never ready | Code bug (**independent of A1**) | 🔴 critical | No — needs fix |
| **A3** | `corp_actions` 404 | Code bug | 🟠 warning (blocks Phase 3 A12) | No — deterministic |
| **A4 / C1** | `bhavcopy` ReadTimeout | Transient + minor code weakness | 🟡 low | Yes — via §2.6 catch-up |
| **B1** | `protected_store:limits.yaml` + `envelope.yaml` FAIL | Owner seeding step | benign | Run seed script |
| **B2** | `regime:NIFTY 50 calendar horizon < 200 sessions` | Config step | benign | Add prior-year calendars |

---

## Causal map

```
protected_store:limits/envelope  ── B1  (empty protected_config table)     [independent → seed]
data_freshness:instruments       ── A1  (missing KiteClient.instruments)   [independent → code]
     └─cascade→ empty store: UnknownInstrument everywhere, no ticker subs  (self-resolves w/ A1)
warmup_ready                     ── A2 (index tick=0) + B2 (only 2026 cal)  [independent → code + config]
corp_actions 404                 ── A3  (no cookie priming)                 [independent → code]
bhavcopy timeout                 ── C1  (transient, self-heals)             [ignore]
```

**Key correction to first-glance reading:** the `backfill_unknown_token` / warmup warnings look downstream of A1 (empty store), and *this boot they are over-determined*. But the index-resolution failure (A2) is a **separate, durable bug** — it persists even against a fully healthy instruments refresh. `warmup_ready` therefore requires **A1 + A2 + B2** all landed, not just A1.

---

## A1 — `KiteClient` missing `instruments()` passthrough → instruments freeze 🔴

**Symptom**
```
ERROR engine.ops.jobs safety_critical_catchup_failed job_id=instruments
AttributeError: 'KiteClient' object has no attribute 'instruments'
  jobs.py:272 _run_safety_critical → main.py:338 job_instruments
  → instruments.py:96 raw = kite_client.instruments()
```

**Root cause**
`KiteClient` (`src/engine/broker/kite_client.py`) is the single typed surface over pykiteconnect and implements passthroughs for `orders`/`positions`/`holdings`/`margins`/`gtt`/`historical`/`ltp` via `_call`/`_order_call`, but **no `instruments()` method was ever added** (the market-data section ends at `ltp()` ~L273). `InstrumentStore.refresh` (`instruments.py:96`) calls `raw = kite_client.instruments()` and only checks `hasattr(raw, "__await__")` on the *next* line — so the missing-attribute lookup on line 96 raises before the defensive await can run. `_require_kite()` returns the real wrapper (not the raw `KiteConnect`), so the `AttributeError` (vs. the `RuntimeError` `_require_kite` raises when `kite is None`) confirms login/credentials were fine and the method is simply absent. In `_run_safety_critical` (`jobs.py:271-288`) the exception maps to `reason = f"data_freshness:{spec.job_id}"` = `data_freshness:instruments` → `self._freeze(reason)`.

**Downstream (D1/D2 — self-resolve once A1 lands):** the failed refresh leaves `InstrumentStore` empty, so `by_symbol`/`round_to_tick` raise `UnknownInstrument` for every symbol (gate/OMS can't price or size), `token_for_symbol`→`None` so `ticker_tokens()` yields no subscriptions, and `symbol_for_token`→`None` so `TickerSupervisor` drops every frame.

**Fix** — add the passthrough to `KiteClient` (do **not** touch `instruments.py`/`main.py`, and do **not** reach into `kite._kc.instruments()` from `refresh` — that bypasses the R5 limiter/logging chokepoint):

```python
async def instruments(self, exchange: str | None = None) -> list:
    """Full daily instruments dump for `exchange` (all if None).

    Once-per-day reference download consumed by InstrumentStore.refresh (§3.2.2, A10).
    pykiteconnect's instruments() is a bulk CSV dump, not an order/quote/historical
    API call, but it is still funnelled through the single limiter chokepoint using
    the conservative "quote" bucket — a once-a-day acquire never stalls.
    """
    return await self._call(
        "quote", "instruments", lambda: self._kc.instruments(exchange=exchange)
    )
```

Two constraints dictate this exact form:
- **Must be `async`** so `refresh`'s `hasattr(raw, "__await__")` guard (`instruments.py:97`) awaits the coroutine correctly.
- **Must route through `"quote"`** — `RateLimiter.EndpointClass` is `Literal["quote","historical","orders"]` and indexes `self._buckets[endpoint_class]`; `"instruments"` would `KeyError`.

**Evidence**
- `src/engine/broker/kite_client.py:253` — market-data section ends at `ltp()`; no `instruments()` anywhere.
- `src/engine/broker/instruments.py:96` — attribute lookup raises before the L97 `__await__` check.
- `src/engine/ops/main.py:338` — `job_instruments` → `_require_kite()` returns the real wrapper.
- `src/engine/broker/rate_limiter.py:65` — `EndpointClass` literal + `_buckets` indexing forces the `"quote"` bucket.
- `src/engine/ops/jobs.py:279` — maps the exception to `data_freshness:instruments` → freeze.

---

## A2 — Index rows (`NIFTY 50` / `INDIA VIX`) rejected by `tick_size gt=0` → warmup never ready 🔴

**Symptom**
```
WARNING engine.marketdata.backfill backfill_unknown_token symbol="NIFTY 50"
WARNING engine.marketdata.backfill backfill_unknown_token symbol="INDIA VIX"
INFO backfill_run_done interval=day symbols=2 bars_written=0 failed=2
WARNING engine.ops.warmup warmup_not_ready
  blockers=["regime:NIFTY 50 calendar horizon < 200 sessions","regime:INDIA VIX daily bars 0/20"]
```

**Root cause**
Kite's full dump **does** include the two indices — `NIFTY 50` (token `256265`) and `INDIA VIX` (token `264969`) — but on the NSE **INDICES** segment they are non-tradable and carry `tick_size=0` / `lot_size=0`. `InstrumentStore._row_to_instrument` (`instruments.py:203-212`) builds an `Instrument` whose fields are `tick_size: Decimal = Field(gt=0)` and `lot_size: int = Field(gt=0)` (L63-64). `lot_size` is salvaged by `int(get("lot_size") or 1)` (→1), but `tick_size=0` triggers a Pydantic `ValidationError`. That error **subclasses `ValueError`**, so `refresh`'s `except (KeyError, ValueError, TypeError)` (`instruments.py:105-108`) swallows it, increments `skipped`, logs `instrument.row_skipped`, and never indexes the two rows. Consequently `token_for_symbol("NIFTY 50" / "INDIA VIX")` returns `None` → `BackfillJob.run` (`backfill.py:160-169`) hits the `token is None` branch → `backfill_unknown_token`, `failed=2`, `bars_written=0`.

The parser was reproduced live: RELIANCE parsed, both indices SKIPPED with `ValidationError`. There is **no** hardcoded index-token map, no INDICES special-case, and no alternate resolution path anywhere.

**Independence from A1:** this is a **separate bug**, not a consequence of the refresh failing. Even with a fully successful `instruments.refresh()`, the two index tokens would still not resolve. The `symbols=2 failed=2` count reflects an empty cold-start watchlist (`watchlist_symbols()` returned `[]`), leaving only the two indices in the batch — and those two fail regardless of refresh health.

**Warmup blockers (downstream of A2 + B2):**
- `INDIA VIX daily bars 0/20` — direct effect of the never-written VIX bars (A2).
- `NIFTY 50 calendar horizon < 200 sessions` — `_recent_sessions(200)` returns `None` because only `config/calendar/2026.yaml` is loaded (~130 sessions Jan–Jul 2026 < 200); this calendar-horizon check (B2) masks that NIFTY 50 daily bars are also 0.

**Fix** — give indices a token-resolution path that does **not** require a positive tick:
- In `InstrumentStore`, when a dump row is an index (`segment=="INDICES"`, or `tick_size<=0`), record its token in a dedicated `_index_tokens: dict[str,int]` (+ reverse map) instead of dropping it; have `token_for_symbol()` / `symbol_for_token()` fall back to that map.
- **Keep** `tick_size=Field(gt=0)` on tradable `Instrument`s and **keep** `round_to_tick` raising `UnknownInstrument` for indices — you never price/size/route an index, so that fail-closed behaviour is correct and should stay.
- Kite `historical` accepts index tokens (returns OHLC with `volume=0`, which `_write_candles` handles), so once the token resolves the daily bars get written.
- Weaker fallback: a static `{"NIFTY 50":256265,"INDIA VIX":264969}` map — but harvesting from the dump is more robust.

**Evidence**
- `src/engine/broker/instruments.py:63` — `tick_size = Field(gt=0)` rejects index rows (`tick_size=0`).
- `src/engine/broker/instruments.py:105` — `except (KeyError, ValueError, TypeError)` swallows the `ValidationError`.
- `src/engine/broker/instruments.py:162` — `token_for_symbol` returns `None` for unindexed indices.
- `src/engine/marketdata/backfill.py:160` — `token None` → `backfill_unknown_token`.
- `src/engine/ops/main.py:384` — `job_daily_bars` backfills `[*watchlist, INDEX_SYMBOL, VIX_SYMBOL]`.
- `src/engine/ops/warmup.py:110,146` — regime checks + the `calendar horizon < n` branch.
- `config/calendar/2026.yaml` — only the 2026 calendar present.

---

## A3 — `corp_actions` 404 (no NSE cookie priming) 🟠

**Symptom**
```
WARNING engine.datafeeds.corp_actions corp_actions_fetch_failed d=2026-07-10
  error="HTTPStatusError: Client error '404 Not Found' for url
  'https://www.nseindia.com/api/corporates-corporate-actions?index=equities'"
```

**Root cause**
The URL (`corp_actions.py:36`) is the correct, well-known endpoint and the request carries a browser UA + Referer (`_NSE_HEADERS`), but **no cookies**. NSE's `/api/*` JSON endpoints mint anti-bot cookies (`nsit`/`nseappid`/`bm_sv`/`ak_bmsc`) only when the homepage `https://www.nseindia.com/` is fetched first; without them the edge returns a **deliberately-misleading 404** (not 403). The shared client (`main.py:254`, `httpx.AsyncClient(follow_redirects=True)`) is bare — no default headers, no cookie-jar priming — and there is **no homepage-priming step anywhere** in the codebase. `deals.py:33` already documents this host as "cookie/UA-gated". Corp actions is forward-looking, so this is not "data unpublished" and not transient — it is a **deterministic 404 that will NOT self-heal on catch-up**.

**Fix**
1. Give the shared client at `main.py:254` default browser headers + split timeout:
   ```python
   http = httpx.AsyncClient(
       follow_redirects=True,
       headers={
           "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                         "(KHTML, like Gecko) Chrome/126.0 Safari/537.36",
           "Accept-Language": "en-US,en;q=0.9",
       },
       timeout=httpx.Timeout(30.0, connect=10.0),
   )
   ```
2. Add a one-time cookie-priming call before any NSE `/api/*` fetch, reusing the same client's cookie jar:
   `await http.get("https://www.nseindia.com/", headers=_NSE_HEADERS, timeout=10.0)` — call once on the first corp_actions/deals/earnings/surveillance run, and re-prime + retry once on a 401/404.
3. Wrap both feed fetches in a bounded exponential-backoff retry (2–3 attempts).

Keep the existing graceful-degradation path (`DATA_FRESHNESS_FROZEN` alert, no crash) as the final fallback.

**Priority:** not urgent in Phase 1 (data-only; consumers land Phase 3), but **must be fixed before Phase 3** — A12 GTT ex-date adjustment consumes corp_actions.

**Evidence**
- `src/engine/datafeeds/corp_actions.py:36` — URL is correct (matches `runbooks/RUNBOOK.md:209`, `test_datafeeds_eod.py:163`); not a stale-URL problem.
- `src/engine/datafeeds/corp_actions.py:174` — GET fires with headers but no cookie priming.
- `src/engine/ops/main.py:254` — shared client is bare; no homepage-priming step exists.
- `src/engine/datafeeds/deals.py:33` — sibling feed's comment confirms the host is cookie/UA-gated.

---

## A4 / C1 — `bhavcopy` ReadTimeout (transient) 🟡

**Symptom**
```
WARNING engine.datafeeds.bhavcopy bhavcopy_fetch_failed d=2026-07-10
  url="https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_20260710_F_0000.csv.zip"
  error="ReadTimeout: "
```

**Root cause**
Primarily a **genuine transient** ReadTimeout. The URL template (`bhavcopy.py:42`) is the correct current UDiFF scheme and the error is a timeout (not a 404), so the file resolved and is not unpublished (boot 18:23 IST is ~3h post-close on a trading day — `config/calendar/2026.yaml` confirms 2026-07-10 is not a holiday). `nsearchives.nseindia.com` is notoriously slow/throttled right after the ~18:00 file drop; the 30s timeout with no retry lost the race once. **Contributing code weakness:** `bhavcopy.py:173` issues the GET with **no headers**, so the bare client sends httpx's default `python-httpx` UA, which the archives host tarpits.

**Fix / action:** the browser-UA + retry hardening from A3 (steps 1 & 3) covers this. Otherwise **no action required** — it degrades gracefully and **self-heals on the §2.6 date-keyed catch-up**.

**Evidence**
- `src/engine/datafeeds/bhavcopy.py:173` — GET sends no headers → default UA tarpitted; 30s timeout, no retry.
- `src/engine/datafeeds/bhavcopy.py:42` — URL template is the current UDiFF scheme; error is ReadTimeout, not 404.
- `config/calendar/2026.yaml:29` — 2026-07-10 is a trading day; rules out "data unpublished".

---

## B1 — Protected config not seeded (`limits.yaml` + `envelope.yaml`) ⚙️

**Symptom**
```
WARNING engine.ops.selftest selftest_check check=protected_store:limits.yaml
  status=FAIL detail="hash mismatch or unregistered (R4) — run scripts/seed_protected_config.py"
  implies=frozen_entries
(… same for envelope.yaml)
selftest_complete ok=false frozen=["protected_store:limits.yaml","protected_store:envelope.yaml"]
```

**Root cause**
The registered-hash authority is the SQLite `protected_config` table (schema `migrations/0001_initial.sql:152`). A live query of `data/state.db` shows that table has **0 rows** — the one-time owner seeding has never been run on this machine. `selftest.py:183` → `store.verify(name)` → `load_verified()` → `_signature()` returns `None` → raises `IntegrityError("… unregistered")` (`protected_store.py:79-80`) → `verify()` returns `False` → check emitted as FAIL/FROZEN. This is the **"unregistered/fresh" branch**, NOT an edit-after-registration mismatch and NOT a missing hash store (the table exists, it's just empty). This is the **intended safe first-run state** (fresh install → unregistered → refuse to load → FROZEN-on-flat-book is the safe no-trading default, O2/R10) — not tampering, not a code defect.

**Downstream (D3):** because `envelope.yaml` is unverified, the learner's `envelope_state.bounds_sha256` cannot match a recorded hash, so envelope-bounded learner writes stay blocked until seeding.

**Remediation (owner command — safe):**
```
python scripts/seed_protected_config.py            # interactive: prints SHA-256, type "yes"
python scripts/seed_protected_config.py --yes       # non-interactive
```
`register_initial` reads each file and inserts its hash + content snapshot via the owner-confirmed write path; it does **not** change semantic config. After it runs, `load_verified` passes and `integrity_ok` becomes true on the next boot.
- **Do NOT use `--reseed`** here (that is only for re-registering after a deliberate owner edit).
- **Note:** the write path rewrites both files CRLF→LF and registers the LF hash (repo has `core.autocrlf=true`, so no spurious git diff now). **Optional hardening:** add `config/*.yaml text eol=lf` to a `.gitattributes` so a future checkout cannot re-materialize CRLF and re-trigger the mismatch.

**Evidence**
- `data/state.db` — `protected_config` table has 0 rows (never seeded).
- `src/engine/core/protected_store.py:79` — unregistered branch raises `IntegrityError`; `verify()` → `False`.
- `src/engine/ops/selftest.py:183` — emits FAIL/FROZEN; detail matches the boot-log line verbatim.
- `scripts/seed_protected_config.py:138` — `register_initial(...)` signs current on-disk bytes; skips already-registered unless `--reseed`.
- `src/engine/core/protected_store.py:105` — `owner_update` writes LF bytes (CRLF→LF note above).

---

## B2 — Only the 2026 calendar present ⚙️

**Symptom:** `regime:NIFTY 50 calendar horizon < 200 sessions` (one of the two warmup blockers).

**Root cause:** only `config/calendar/2026.yaml` exists (~130 trading days Jan–Jul 2026 < 200). `warmup.py:146` `_missing_daily` returns the `calendar horizon < n sessions` branch when `_recent_sessions(200)` is `None`.

**Fix:** add prior-year NSE calendar files (`config/calendar/2024.yaml`, `config/calendar/2025.yaml`) so the gate can enumerate ≥200 sessions.

---

## Prioritized action list

**Fix in code (in order):**
1. **A1** — add `KiteClient.instruments()` (snippet above). Clears `data_freshness:instruments` and the empty-store cascade (D1/D2). Highest priority.
2. **A2** — index token-resolution path in `InstrumentStore` (dedicated `_index_tokens` map; keep `Field(gt=0)` on tradables and keep `round_to_tick` fail-closed for indices). Required to clear the VIX warmup blocker.
3. **A3/A4** — NSE session priming + browser headers + bounded retry on the shared client. Not urgent for Phase 1; **must land before Phase 3** (A12).

**Commands the owner runs:**
1. `python scripts/seed_protected_config.py` (interactive; or `--yes`) — clears both `protected_store:*` freezes, flips `integrity_ok` true next boot. **Not** `--reseed`. Optional: add `.gitattributes` `config/*.yaml text eol=lf`.
2. Add `config/calendar/2024.yaml` + `2025.yaml` for the 200-session horizon.

**Ignore / no action (working as designed):**
- **bhavcopy ReadTimeout (C1)** — transient; self-heals on the §2.6 catch-up.
- **`mode=OFF`, `risk_state=FROZEN`** — expected on a dev boot with no live login (FROZEN-on-flat-book is the safe no-trading default, O2/R10).

**Ordering note:** `warmup_ready` remains frozen until **A1 + A2 + B2** are all in place — sequence those three before expecting the engine to reach warmup-ready.

---

*Investigation method: 4 parallel code-reading investigators (one per error cluster) + synthesis, all grounded in the cited files and a live `data/state.db` query. Line numbers reflect the working tree at boot time (2026-07-10).*
