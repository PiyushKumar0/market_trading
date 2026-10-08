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

A tabbed, plain-language console (redesign 2026-10-08; design spec
`runbooks/design-2026-10-08-dashboard-redesign.md`). Dark only, system fonts, hand-rolled CSS on design
tokens (`src/styles/tokens.css`, every colour pair contrast-measured), no UI framework, no webfont,
**no CDN link of any kind**: the LAN page must render with zero external requests (R10).
`vite.config.ts` sets `base: './'` so assets resolve under the engine's static mount.

**Layout.** A navbar (health pill = the worst active alert, engine mode Off | Recommend, paper autopilot
switch, trading-hours editor, data freshness, sign-out) over six tabs kept in the URL hash
(`#overview` … `#system`, so reload and Back work). Words first: every engine value is translated in
`src/copy/glossary.ts` (label + one-line hint + the engine's own term), so Telegram and the logs can
still be matched. Pure logic lives in `src/lib/` (`format` — rupees grouped on the string, every time
in IST; `alerts` — what needs attention, the health pill and tab badges; `recs`; `paper`).

**Auth.** The engine's `DASHBOARD_TOKEN` is entered once and kept in `localStorage['mt_token']`; it
goes on every request as `Authorization: Bearer`, and on the `/ws/live` handshake as `?token=`
(a WebSocket handshake cannot carry a header — `_ws_authorized` accepts either).

**Polling.** One 10 s cycle fetches, in parallel: `/mode`, `/positions`, `/decisions`,
`/recommendations`, `/risk/headroom`, `/budget`, `/config/trade_window`, `/news/watchlist`, `/paper`.
Results merge per-key, so one unwired collaborator never blanks the console. `/paper` rides this cycle
rather than its own because the panel reads the header mode from the same poll. `/scorecard` and
`/notifications` each poll on their own 60 s hook. `/ws/live` runs alongside and
appends relayed state-change frames (mode / risk / kill / trade-window / budget) to the events feed;
the engine's 15 s `ping` keepalive is filtered out.

**Tabs.**
- *Overview* — "Needs attention" (alerts, worst first, each with what to do), *Act now* (only LIVE
  recommendations, as trade tickets: the plain instruction, levels, reasoning, your steps), your real
  positions (the stop-loss-confirmed column second, so it is never scrolled out of view) beside the
  simulated paper snapshot, and a *Today* strip linking to the other tabs.
- *Recommendations* — the full ledger, folded by IST day (owner-directed 2026-09-08: today open,
  earlier days folded, a day still holding a live recommendation open too); rows open into tickets.
- *Paper trading* — status in words, paper limits in force, four figures, the equity-by-session chart
  (crosshair, ←/→, table view), open positions, waiting orders, closed trades, health counters.
  Everything here is simulated and says so once; paper cards carry a dashed violet border.
- *Performance* — the per-strategy scorecard: hindsight-scored recommendations and paper trades.
- *Activity* — risk checks (the decision log, day-folded, real and paper verdicts side by side),
  Telegram messages (above News — owner-directed 2026-09-08 — chronological, pinned to the newest,
  with delivery truth), the news watchlist, live updates in sentences (raw payload one click away).
- *System* — engine status, risk limits as meters, the AI usage budget (week, level, per analyst).

**Owner writes** are limited to `POST /mode` (OFF / RECOMMEND), `POST /config/trade_window` and
`POST /paper` (paper autopilot ON / OFF, single-step; ON always asks, OFF asks only when waiting paper
entry orders would be cancelled; ON is refused 409 unless the autopilot is built) and `POST /risk/rearm`
(the alert's Re-arm button: lifts each floor stop the account is back above; a still-breached one stays,
and so does a kill switch). They go through an
in-page dialog with action-named buttons, and every result is a toast; the value shown always comes
from the next poll, never from an optimistic guess. Switching the engine Off asks first. Deliberately
absent, and not an oversight: →AUTO, kill-switch reset and paper reset are owner TWO-STEP transitions
with no dashboard path (Telegram `/mode AUTO`, `/kill_reset`, `/paper reset`, each + `/confirm`), and
recommendation outcomes (`/taken`, `/closed`) are Telegram-only (§3.5.3/§7.2/R10).

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
included — so the day folds and every tab can be checked in a browser. The live recommendations
cover all three kinds (an exit, a stop-loss adjust, a shrunk entry), and the risk checks carry
reasons in the gate's own "rule: value (limit …)" form plus one bare rule ID. Any token passes the
gate; there is no `/ws/live`, so Live updates reads "Reconnecting" (and the console logs the 404). `NO_TODAY=1` drops today's ledger
rows to show the "no … today" state. `/paper` answers a built, ON book by default (a daily FROZEN halt,
one unmarked position, an unfilled entry order, a void among the closes); `PAPER_OFF=1` answers the
not-built stub (subsystem disabled, nothing stored); `PAPER_IDLE=1` answers the go-live view (built,
OFF, prep not run, nothing stored). `ALERTS=1` answers the loud state (risk KILLED, a 20-character
unprotected symbol, failed and pending notifications, DG3 budget, a stale watchlist digest, near-full
headroom, a ~600-character thesis); `EMPTY=1` answers a first boot (every route its stub/empty shape).
`POST /paper` flips the served fixture's `enabled` with the
engine's 422 (non-boolean) and 409 (ON while not built) answers. `POST /mode` (`OFF`/`RECOMMEND` 200,
`AUTO` 409, else 422) and `POST /config/trade_window` (200, or 422 with nothing changed) change the
served value; any other non-GET on a route answers 405. Dev-only: nothing from it is shipped.

## Tests

- `npm test` — `node --test` over `tests/*.test.ts` (Node 22 strips the types; no test dependency):
  the pure modules — rupee grouping and signs, IST days and times, the glossary and its fallbacks
  (risk-check reasons, the Telegram reply per recommendation kind), recommendation helpers (per-kind
  instruction, quantity cut), alert derivation (including "no false alarm" stub shapes), paper status,
  and which URL hashes are tabs.
- `npm run build` runs `tsc -b` first (app, vite config and tests), so a shape drift between
  `src/types.ts` and a view fails the build. The API contract itself is covered by
  `tests/unit/test_api_routes.py`.
- `tests/dom-checks.js` — layout and accessibility assertions evaluated in the page (paste into the
  console, or chrome-devtools `evaluate_script`) against the fixture server: no sideways scroll, nothing
  sticking out of a card, every target ≥ 24 px, text contrast ≥ 4.5:1 on the composited background.
  Run it per scenario (default, `ALERTS=1`, `EMPTY=1`, `NO_TODAY=1`, `PAPER_OFF=1`, `PAPER_IDLE=1`) at
  1440, 1024, 390 and 320 px wide.
