# dashboard/ — React + TypeScript + Vite (O8)

The owner's LAN web dashboard (§3.1, §3.2.11, O8/R8). The engine's FastAPI app (`engine.api.app`)
serves the built static output of this package and exposes the read APIs the front-end consumes.

## Build

```powershell
cd dashboard
npm install
npm run build      # -> dashboard/dist  (served by the engine at http://<lan-ip>:8400/)
npm run dev        # optional: Vite dev server; see "Dev server" below
```

`engine.api.app._mount_dashboard_if_present` looks for `<repo>/dashboard/dist` FIRST (resolved from
`engine.core.config.repo_root()`, not the process working directory — the engine normally runs as a
service), then the legacy `web/dist` fallbacks. No build ⇒ the API just runs headless.

`dist/` and `node_modules/` are git-ignored: the build is a local artifact, rebuilt after any change.

## Runtime shape

Single page, dark, compact grid. Everything is self-contained — hand-rolled CSS, no UI framework, no
webfont, **no CDN link of any kind**: the LAN page must render with zero external requests (R10).
`vite.config.ts` sets `base: './'` so assets resolve under the engine's static mount.

**Auth.** The engine's `DASHBOARD_TOKEN` is entered once and kept in `localStorage['mt_token']`; it
goes on every request as `Authorization: Bearer`, and on the `/ws/live` handshake as `?token=`
(a WebSocket handshake cannot carry a header — `_ws_authorized` accepts either).

**Polling.** One 10 s cycle fetches, in parallel: `/mode`, `/positions`, `/decisions`,
`/recommendations`, `/risk/headroom`, `/budget`, `/config/trade_window`, `/news/watchlist`. Results
merge per-key, so one unwired collaborator never blanks the console. `/ws/live` runs alongside and
appends relayed state-change frames (mode / risk / kill / trade-window / budget) to the events feed;
the engine's 15 s `ping` keepalive is filtered out.

**Panels.** Status header (mode / routing / risk_state, trade window, degrade tier, KILLED banner) ·
Recommendations (thesis, entry zone, stop/targets, qty, gate verdict, manual checklist, human-action
chip) · Positions · Decision log (proposal → verdict → cited rules) · Risk headroom · Agent budget ·
Trade-window editor · Live events · Notifications (the day's Telegram transcript) · News/catalyst
watchlist (originating vs context + digest freshness). The two ledgers — recommendations and the
decision log — are folded by IST day (`ui.tsx` `useDayFolds`): today's fold starts open, earlier
days start folded and open on click (owner-directed 2026-09-08).

**Owner writes** are limited to `POST /mode` (OFF / RECOMMEND) and `POST /config/trade_window`.
Deliberately absent, and not an oversight: →AUTO and kill-switch reset are owner TWO-STEP
transitions with no dashboard path (Telegram `/mode AUTO` + `/confirm`, `/kill_reset` + `/confirm`),
and recommendation outcomes (`/taken`, `/closed`) are Telegram-only (§3.5.3/§7.2/R10).

## Dev server

`npm run dev` serves on another port, so point it at the engine by setting the API base once in the
browser console, then reload:

```js
localStorage.setItem('mt_api_base', 'http://127.0.0.1:8400')
```

Unset it (or leave it unset) and the page talks to its own origin — which is what happens when the
engine serves `dist/`.

## Fixture preview (no engine)

`node fixture_server.mjs` (after `npm run build`) serves `dist/` on http://127.0.0.1:8499/ with canned
rows for every read route, spread across several IST days — a UTC-stamped row and an undated one
included — so the day folds and the panel order can be checked in a browser. Any token passes the
gate; there is no `/ws/live`, so the events feed reads "closed". `NO_TODAY=1` drops today's ledger
rows to show the "no … today" state. Dev-only: nothing from it is shipped.

## Tests

No JS unit tests in v1: every panel is a pure render over a typed API response, and the API contract
itself is covered by `tests/unit/test_api_routes.py`. `npm run build` runs `tsc -b` first, so a shape
drift between `src/types.ts` and a panel fails the build.
