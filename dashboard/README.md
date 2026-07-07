# dashboard/ — React + TypeScript + Vite (O8)

The owner's web dashboard (§3.1, O8/R8). **Not built in Phase 0** — the engine's FastAPI app
(`engine.api.app`) serves the built static output from here once it exists, and exposes the read APIs
(`/positions`, `/orders`, `/decisions`, `/verdicts`, `/risk/headroom`, `/budget`, `/mode`,
`/config/trade_window`, …) plus the `/ws/live` stream that this front-end consumes (all bearer-token
auth'd except the Kite login callback, R10).

Planned (Phase 2+): live positions + P&L, risk-limit headroom, agent budget spend, the decision log
(proposals → gate verdicts → orders), learning status, and the owner control plane (mode changes,
trade-window edit, kill switch — destructive actions two-step confirmed).

Scaffold when starting the front-end:

```bash
npm create vite@latest . -- --template react-ts
npm install && npm run build      # build output is served by engine.api at the LAN dashboard URL
```
