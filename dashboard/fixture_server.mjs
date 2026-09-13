// Dev-only fixture server: eyeball the BUILT dashboard without the engine.
//
//   cd dashboard; npm run build; node fixture_server.mjs          # http://127.0.0.1:8499/
//   NO_TODAY=1 node fixture_server.mjs                            # every ledger row is from an earlier day
//
// Serves dist/ at / and answers the read routes with canned rows spread across several IST days
// (today, yesterday, older, a UTC-stamped row, an undated row), so the per-day folds, the IST day
// derivation and the panel order can be checked in a browser. Any bearer token is accepted; there is
// no /ws/live, so the events feed shows "closed" and retries — expected. Never shipped: dist/ is built
// from src/ only.
import http from 'node:http'
import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const DIST = path.join(path.dirname(fileURLToPath(import.meta.url)), 'dist')
const PORT = Number(process.env.PORT ?? 8499)
const NO_TODAY = process.env.NO_TODAY === '1'

const istFmt = new Intl.DateTimeFormat('en-CA', {
  timeZone: 'Asia/Kolkata',
  year: 'numeric',
  month: '2-digit',
  day: '2-digit',
})
function istDay(at) {
  const p = istFmt.formatToParts(at)
  const g = (t) => p.find((x) => x.type === t).value
  return `${g('year')}-${g('month')}-${g('day')}`
}
function shift(day, days) {
  const at = new Date(`${day}T12:00:00+05:30`)
  at.setUTCDate(at.getUTCDate() + days)
  return istDay(at)
}
const today = istDay(new Date())
const d1 = shift(today, -1)
const d3 = shift(today, -3)
const d4 = shift(today, -4)

function rec(id, day, hms, inst, side, kind, action, extra = {}) {
  return {
    rec_id: id,
    recommendation: {
      rec_id: id,
      created_at: day ? `${day}T${hms}+05:30` : null,
      valid_until: day ? `${day}T15:30:00+05:30` : null,
      kind,
      instrument: inst,
      side,
      style: 'swing',
      product: 'CNC',
      entry_zone: ['2445.0', '2445.0'],
      stop: '2607.60',
      targets: [],
      qty: 3,
      notional: '7335.00',
      thesis: `Fixture thesis for ${inst} on ${day ?? 'undated'} — ${kind} ${side}.`,
      confidence: 0.86,
      gate: { verdict: 'approve', reasons: [], original_qty: 3 },
      manual_checklist: ['exit at market: close now'],
      ...extra,
    },
    delivered_at: day ? `${day}T${hms}+05:30` : null,
    human_action: action,
    human_fill_price: null,
    outcome: null,
  }
}

let recommendations = [
  rec('r-t1', today, '10:15:03', 'HDFCAMC', 'SELL', 'exit', null),
  rec('r-t2', today, '09:41:10', 'TATASTEEL', 'BUY', 'enter', null),
  rec('r-y1', d1, '13:00:19', 'HDFCAMC', 'SELL', 'exit', 'expired'),
  rec('r-y2', d1, '13:00:12', 'HINDZINC', 'SELL', 'exit', 'expired'),
  rec('r-y3', d1, '10:02:00', 'CEIGALL', 'BUY', 'enter', 'dismissed'),
  // WO-V (2026-09-13): a swing ENTRY delivered YESTERDAY, still pending, valid to TOMORROW's close —
  // yesterday's fold must start open and the `valid till` chip must carry the date.
  rec('r-live', d1, '14:47:00', 'JINDALSTEL', 'BUY', 'entry', null, {
    valid_until: `${shift(today, 1)}T15:30:00+05:30`,
  }),
  // UTC stamp late on d(-4) UTC = early d(-3) IST: must fold under d(-3).
  { ...rec('r-utc', d3, '01:00:00', 'UTCCHECK', 'BUY', 'enter', 'expired'), delivered_at: `${d4}T19:30:00Z` },
  rec('r-old', d4, '11:11:11', 'OLDNAME', 'BUY', 'enter', 'taken'),
  // delivered_at missing, payload created_at present: folds under created_at's day (d4).
  { ...rec('r-nodeliv', d4, '12:00:00', 'NODELIV', 'BUY', 'enter', null), delivered_at: null },
  // both missing: the undated bucket, last.
  rec('r-undated', null, '00:00:00', 'UNDATED', 'BUY', 'enter', null),
]

function dec(id, day, hms, agent, action, subject, verdict, reasons) {
  return {
    proposal_id: id,
    agent_id: agent,
    action,
    proposal: { action, agent_id: agent, tradingsymbol: subject },
    subject,
    created_at: day ? `${day}T${hms}+05:30` : null,
    verdict_id: verdict ? `${id}-v` : null,
    verdict,
    reasons,
    evaluated_at: day ? `${day}T${hms}+05:30` : null,
  }
}

let decisions = [
  dec('p-t1', today, '10:14:58', 'swing_analyst', 'exit', 'HDFCAMC', 'approve', []),
  dec('p-t2', today, '09:41:01', 'swing_analyst', 'enter', 'TATASTEEL', 'reject', ['per_trade_risk']),
  dec('p-y1', d1, '13:00:10', 'swing_analyst', 'exit', 'HDFCAMC', 'approve', []),
  dec('p-y2', d1, '13:00:05', 'swing_analyst', 'exit', 'HINDZINC', 'approve', []),
  dec('p-y3', d1, '10:01:50', 'swing_analyst', 'enter', 'CEIGALL', 'reject', ['max_open_positions', 'per_sector_exposure']),
  dec('p-o1', d4, '11:11:00', 'intraday_analyst', 'enter', 'OLDNAME', 'shrink', ['per_trade_risk']),
  dec('p-u1', null, '00:00:00', 'intraday_analyst', 'enter', 'UNDATED', null, []),
]

if (NO_TODAY) {
  const dayOf = (iso) => (iso ? istDay(new Date(iso)) : '')
  recommendations = recommendations.filter((r) => dayOf(r.delivered_at ?? r.recommendation.created_at) !== today)
  decisions = decisions.filter((d) => dayOf(d.created_at) !== today)
}

// The governor's quota week (§5.6): Thursday 14:00 IST → Thursday 14:00 IST, keyed by its START
// Thursday. Mirrors engine.intelligence.governor._window_key — wall-clock and calendar-blind, and the
// reset-day MORNING still belongs to the window that ends. Computed live so the BudgetPanel chips show
// a plausible current week on any dev box rather than a frozen date.
const THURSDAY = 4 // Date#getUTCDay
function quotaWindow(at = new Date()) {
  const day = istDay(at)
  // hourCycle h23, not hour12:false: some ICU builds render midnight as "24:00" under en-GB, which
  // would compare ABOVE "14:00" and put the small hours of a Thursday in the wrong week.
  const hhmm = new Intl.DateTimeFormat('en-GB', {
    timeZone: 'Asia/Kolkata',
    hour: '2-digit',
    minute: '2-digit',
    hourCycle: 'h23',
  }).format(at)
  // Noon IST is the same calendar date in UTC, so getUTCDay() of it is that IST date's weekday.
  const dow = new Date(`${day}T12:00:00+05:30`).getUTCDay()
  let back = (dow - THURSDAY + 7) % 7
  if (back === 0 && hhmm < '14:00') back = 7
  const start = shift(day, -back)
  return { key: start, start: `${start}T14:00:00+05:30`, end: `${shift(start, 7)}T14:00:00+05:30` }
}

// Mid-week spend against the shipped weekly allocations, with `sdk_smoke` billing WITHOUT an
// allocation on purpose: the union row is the thing GET /budget added, so the fixture has to exercise
// it (the rows must add up to window_spend_usd, 81.6487).
const win = quotaWindow()
const budgetFixture = {
  budget: {
    window_key: win.key,
    window_start: win.start,
    window_end: win.end,
    window_spend_usd: '81.6487',
    credit_usd: '200',
    per_agent_spend_usd: {
      intraday_analyst: '31.4062',
      news_analyst: '44.8125',
      preopen_planner: '3.2500',
      nightly_reviewer: '1.9800',
      weekly_researcher: '0',
      reserve: '0',
      sdk_smoke: '0.2000',
    },
    allocations_usd: {
      intraday_analyst: '90',
      news_analyst: '90',
      preopen_planner: '8',
      nightly_reviewer: '5',
      weekly_researcher: '2',
      reserve: '5',
    },
    forward_cap: 48,
  },
  degrade_tier: 'DG0',
}

const routes = {
  '/mode': { mode: 'RECOMMEND', routing: 'paper', risk_state: 'NORMAL' },
  '/positions': { positions: [], as_of: new Date().toISOString() },
  '/decisions': { decisions },
  '/recommendations': { recommendations },
  '/risk/headroom': { headroom: {} },
  '/budget': budgetFixture,
  '/config/trade_window': { trade_window: { start: '09:30', end: '15:00', squareoff_buffer_min: 10 } },
  '/news/watchlist': {
    d: today,
    watchlist: [],
    digest: { as_of: null, age_h: null, stale_max_h: null, status: 'missing' },
  },
  '/notifications': {
    d: today,
    rows: [
      {
        notification_id: 'n1',
        created_at: `${today}T09:30:00+05:30`,
        kind: 'boot',
        severity: 'info',
        title: 'fixture boot',
        body: 'hello',
        status: 'delivered',
        attempts: 1,
        delivered_at: `${today}T09:30:01+05:30`,
        last_error: null,
        last_attempt_at: null,
      },
      {
        notification_id: 'n2',
        created_at: `${today}T10:15:03+05:30`,
        kind: 'recommendation',
        severity: 'warning',
        title: 'fixture rec',
        body: 'exit HDFCAMC',
        status: 'delivered',
        attempts: 1,
        delivered_at: `${today}T10:15:04+05:30`,
        last_error: null,
        last_attempt_at: null,
      },
    ],
  },
}

const types = { '.html': 'text/html', '.js': 'text/javascript', '.css': 'text/css', '.svg': 'image/svg+xml' }

http
  .createServer((req, res) => {
    const url = new URL(req.url, `http://127.0.0.1:${PORT}`)
    if (url.pathname in routes) {
      res.writeHead(200, { 'content-type': 'application/json' })
      res.end(JSON.stringify(routes[url.pathname]))
      return
    }
    const rel = url.pathname === '/' ? 'index.html' : url.pathname.slice(1)
    const file = path.join(DIST, rel)
    if (file.startsWith(DIST) && fs.existsSync(file) && fs.statSync(file).isFile()) {
      res.writeHead(200, { 'content-type': types[path.extname(file)] ?? 'application/octet-stream' })
      fs.createReadStream(file).pipe(res)
      return
    }
    res.writeHead(404)
    res.end('not found')
  })
  .listen(PORT, '127.0.0.1', () =>
    console.log(
      `fixture server on http://127.0.0.1:${PORT}/  today=${today} d1=${d1} d3=${d3} d4=${d4}${NO_TODAY ? '  (NO_TODAY)' : ''}`,
    ),
  )
