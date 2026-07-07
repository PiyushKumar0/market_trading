# market_trading

Personal, single-user, capital-preservation-first AI trading platform for Indian equities
(NSE) on a Zerodha Kite Connect account. Runs on the owner's Windows 11 PC.

**The authoritative design is [`IMPLEMENTATION_PLAN.md`](IMPLEMENTATION_PLAN.md).** This README
is a pointer + setup guide, not a spec. Every load-bearing decision in the code cites the plan's
fact/decision/requirement IDs (`A1…E7`, `O1…O10`, `R1…R10`).

## The one safety property to remember

**Three-tier separation (R1):** Claude *proposes* (Tier 1, non-deterministic), a deterministic
risk gate *disposes* (Tier 2, no LLM), a deterministic OMS *executes* (Tier 3). The platform must
stay safe with the LLM tier completely dead. `engine.risk` and `engine.oms` import **nothing** from
`engine.intelligence` — a unit test asserts the import graph (§2.3, §9.1).

And: **broker-resident protection (R3)** — every live position is protected by an order resting at
the exchange (SL-M for MIS, GTT OCO for CNC), so capital protection survives process/PC/token death.

## Status

**Phase 0 — Foundations** (skeleton, config, secrets, session workflow, calendar/clock, sticky
mode/kill state, SQLite migrations v1, install smoke test). See §8.1 of the plan for the gate.

This is NOT a profit project in its early phases. Per SEBI's own data, ~71% of retail intraday
traders lose money; fixed costs alone are ~3–4% of the ₹20,000 capital per month (C9, §1.2). The
Phase 0–3 success metric is **process quality + capital preservation**, not profit.

## Setup (owner, one time)

The plan pins **Python 3.12.x** and uses [`uv`](https://docs.astral.sh/uv/). The dev machine here
has 3.11.9; install 3.12 via uv:

```powershell
# 1. Install uv (https://docs.astral.sh/uv/getting-started/installation/), then:
uv python install 3.12
uv sync --extra dev            # creates .venv, installs locked deps (writes uv.lock)

# 2. Seed secrets into Windows Credential Manager (DPAPI, R10) — never on disk:
uv run python scripts/dpapi_set.py

# 3. Mint the Claude Agent SDK OAuth token as the service user (D2):
#    (run the CLI bundled with claude-agent-sdk; path recorded during the smoke test)
#    claude setup-token   -> store output via scripts/dpapi_set.py (key: claude_code_oauth_token)

# 4. Phase-0 install smoke test (A4) — imports + minimally runs every heavy dep:
uv run python scripts/smoke_test.py

# 5. A11 empirical check (are Kite minute candles corp-action adjusted?) — records §14 Q10:
uv run python scripts/a11_check.py

# 6. Apply SQLite migrations v1:
uv run python -m engine.core.migrations

# 7. Run the engine (manual/demand start; same code path as a scheduled start, §2.6):
uv run mt-engine
```

Install the engine as an NSSM service with `scripts/nssm_install.ps1` (E7/D11) — see the runbook.

## Tests

Pure-Python tiers (core, schemas, risk-state, calendar/clock) run without the heavy deps:

```powershell
uv run pytest                         # or: .venv\Scripts\python.exe -m pytest
uv run pytest -m "not needs_heavy_deps"
```

## Layout

See §3.1 of the plan. Briefly: `src/engine/{core,broker,marketdata,universe,datafeeds,features,`
`strategy,intelligence,risk,oms,paper,learning,notify,api,ops}`, `ticker/` (Twisted subprocess, A4),
`config/` (incl. the **protected store** `limits.yaml`/`envelope.yaml`, R4), `scripts/`, `tests/`,
`runbooks/`, `dashboard/` (React, O8). Runtime `data/` is gitignored.

## Compliance & scope (read before AUTO)

Single-user only. Distributing signals/execution to anyone outside self/spouse/dependent
children/parents is a SEBI violation (B5). The platform stays **≤10 orders/second by construction**
(B3, §7.3). RECOMMEND mode places **zero** API orders (B7). Obtain legal review before enabling AUTO
(§7.3). Owner-accepted risk items are flagged in §7.3 / §12 of the plan.
