# market_trading

Personal, single-user, capital-preservation-first AI trading platform for Indian equities
(NSE) on a Zerodha Kite Connect account. Runs on the owner's Windows 11 PC.

The code is the description of what is built. [`IMPLEMENTATION_PLAN.md`](IMPLEMENTATION_PLAN.md) holds only what
is left to build: the gates ahead, pending verdicts and open questions.

## The one safety property to remember

**Three-tier separation:** Claude *proposes* (Tier 1, non-deterministic), a deterministic
risk gate *disposes* (Tier 2, no LLM), a deterministic OMS *executes* (Tier 3). The platform must
stay safe with the LLM tier completely dead. `engine.risk` and `engine.oms` import **nothing** from
`engine.intelligence` — a unit test asserts the import graph.

And: **broker-resident protection** — every live position is protected by an order resting at
the exchange (SL-M for MIS, GTT OCO for CNC), so capital protection survives process/PC/token death.

## Status

**RECOMMEND** (the engine proposes, the owner executes in Kite and reports via Telegram), plus the paper
autopilot (live since 2026-10-08, paper only). There is no AUTO/live order routing.

This is NOT a profit project in its early phases. Per SEBI's own data, ~71% of retail intraday
traders lose money. The success metric until live trading is **process quality + capital preservation**, not profit.

## Setup (owner, one time)

Python **3.12.x** with [`uv`](https://docs.astral.sh/uv/):

```powershell
# 1. Install uv (https://docs.astral.sh/uv/getting-started/installation/), then:
uv python install 3.12
uv sync --extra dev            # creates .venv, installs locked deps (writes uv.lock)

# 2. Seed secrets into Windows Credential Manager (DPAPI) — never on disk:
uv run python scripts/dpapi_set.py

# 3. Place a Claude Code CLI of the pinned version at `llm.cli_path` in config/agents.yaml
#    (data/run/claude-cli/<ver>/claude.exe; data/ is gitignored, so copy it in) and sign it in
#    as the service user (COMMANDS "Re-login of the engine's Claude CLI").

# 4. Install smoke test — imports + minimally runs every heavy dep:
uv run python scripts/smoke_test.py

# 5. Apply SQLite migrations:
uv run python -m engine.core.migrations

# 6. Run the engine (manual/demand start; same code path as a scheduled start):
uv run mt-engine
```

Install the engine as an NSSM service with `scripts/nssm_install.ps1` — see the runbook.

## Tests

```powershell
uv run pytest -n 8 --dist loadfile      # the gate; or: .venv\Scripts\python.exe -m pytest
uv run pytest -m "not needs_heavy_deps"
```

`tests/offline` (research and one-off scripts) runs on demand only; see COMMANDS "Tests".

## Layout

`src/engine/{core,broker,marketdata,universe,datafeeds,features,strategy,intelligence,risk,oms,paper,`
`learning,notify,api,ops}`, `ticker/` (the Kite WebSocket in a Twisted subprocess: its reactor cannot restart
in-process), `config/` (incl. the owner-signed **protected store** `limits.yaml`/`envelope.yaml`), `scripts/`,
`tests/`, `runbooks/`, `dashboard/` (React). Runtime `data/` is gitignored.

## Compliance & scope (read before AUTO)

Single-user only. Distributing signals/execution to anyone outside self/spouse/dependent
children/parents is a SEBI violation. The platform stays **≤10 orders/second by construction**.
RECOMMEND mode places **zero** API orders. Obtain legal review before enabling AUTO.
