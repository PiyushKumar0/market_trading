// Dev-only fixture server: eyeball the BUILT dashboard without the engine.
//
//   cd dashboard; npm run build; node fixture_server.mjs          # http://127.0.0.1:8499/
//   NO_TODAY=1 node fixture_server.mjs                            # every ledger row is from an earlier day
//   PAPER_OFF=1 node fixture_server.mjs                           # /paper = the not-built stub
//   PAPER_IDLE=1 node fixture_server.mjs                          # /paper = built, OFF, no rows (go-live)
//   ALERTS=1 node fixture_server.mjs                              # KILLED, 20-char unprotected symbol, failed/pending notifications, DG3, stale watchlist, near-full headroom, long thesis
//   EMPTY=1 node fixture_server.mjs                               # first boot: every route answers its stub/empty shape
//
// Serves dist/ at / and answers the read routes with canned rows spread across several IST days
// (today, yesterday, older, a UTC-stamped row, an undated row), so the per-day folds, the IST day
// derivation and the panel order can be checked in a browser. POST /paper flips the served paper
// fixture's `enabled` (422 / 409 as the engine answers). POST /mode ('OFF'|'RECOMMEND' 200, 'AUTO' 409,
// else 422) and POST /config/trade_window (200 / 422 unchanged) change the served value; any other
// non-GET on a route answers 405. Any bearer token is accepted; there is
// no /ws/live, so the events feed shows "closed" and retries — expected. Never shipped: dist/ is built
// from src/ only.
import http from 'node:http'
import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const DIST = path.join(path.dirname(fileURLToPath(import.meta.url)), 'dist')
const PORT = Number(process.env.PORT ?? 8499)
const NO_TODAY = process.env.NO_TODAY === '1'
const PAPER_OFF = process.env.PAPER_OFF === '1'
const PAPER_IDLE = process.env.PAPER_IDLE === '1'
const ALERTS = process.env.ALERTS === '1'
const EMPTY = process.env.EMPTY === '1'

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
      // An exit's or adjust's verdict carries no proposed quantity (gate.py), an entry's does.
      gate: { verdict: 'approve', reasons: [], original_qty: kind === 'exit' || kind === 'adjust' ? null : 3 },
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
  // Synthetic edge case: valid_until on the next day (the engine stamps today's close since WO-V's
  // 2026-09-23 withdrawal), so this exit stays live at any hour of the day.
  rec('r-t1', today, '10:15:03', 'HDFCAMC', 'SELL', 'exit', null, { valid_until: `${shift(today, 1)}T15:30:00+05:30` }),
  // An adjust places NO order and keeps the held side (pipeline.py _manage_recommendation): the card
  // must read as a stop-loss move, never as "Buy 10 shares".
  rec('r-adj', today, '11:05:00', 'COALINDIA', 'BUY', 'adjust', null, {
    valid_until: `${shift(today, 1)}T15:30:00+05:30`,
    entry_zone: ['131.20', '131.20'],
    stop: '128.50',
    qty: 10,
    notional: '1312.00',
    thesis: 'Price is 1.5 ATR above the entry; trailing the stop locks in most of the breakout.',
    manual_checklist: ['move the stop on COALINDIA to 128.50 (modify the existing SL-M/GTT)'],
  }),
  rec('r-t2', today, '09:41:10', 'TATASTEEL', 'BUY', 'entry', null, {
    strategy_id: 'hi52',
    hold_sessions: 20,
    exit_session: shift(today, 28),
    exit_kind: 'time',
    risk_inr: '1250.00',
    stop_atr_mult: '2.5',
    evidence: ['52-week high close 171.40 on volume 1.8x 20d average', 'NIFTY 200 member'],
    registered_edge_pct: '1.20',
  }),
  // Hold set, calendar not yet resolved: exit reads "pending"; no evidence / edge lines.
  rec('r-t3', today, '09:44:00', 'SAIL', 'BUY', 'entry', null, {
    strategy_id: 'brk20',
    hold_sessions: 10,
    risk_inr: '900.00',
    stop_atr_mult: '2.0',
  }),
  rec('r-y1', d1, '13:00:19', 'HDFCAMC', 'SELL', 'exit', 'expired'),
  rec('r-y2', d1, '13:00:12', 'HINDZINC', 'SELL', 'exit', 'expired'),
  rec('r-y3', d1, '10:02:00', 'CEIGALL', 'BUY', 'enter', 'dismissed'),
  // Synthetic edge case (WO-V's next-session entry TTL was withdrawn 2026-09-23, so the engine no
  // longer stamps this): an ENTRY delivered YESTERDAY, still pending, valid to TOMORROW's close —
  // yesterday's fold must start open and the `valid till` chip must carry the date.
  rec('r-live', d1, '14:47:00', 'JINDALSTEL', 'BUY', 'entry', null, {
    valid_until: `${shift(today, 1)}T15:30:00+05:30`,
    // A shrunk entry: the card's quantity-cut note and its reasons with the engine's own figures.
    gate: {
      verdict: 'shrink',
      reasons: [
        'per_trade_risk: 10 x 162.40 = 1624.00 [1.5x stop distance from 2445.00 (daily-band leg unavailable in ' +
          'Phase 2 — gap mult only)] (limit <= 1.5% of equity 40000.00 = 600.00)',
        'shrink: qty 10 -> 3 (bound by per_trade_risk)',
      ],
      original_qty: 10,
    },
  }),
  // UTC stamp late on d(-4) UTC = early d(-3) IST: must fold under d(-3).
  { ...rec('r-utc', d3, '01:00:00', 'UTCCHECK', 'BUY', 'enter', 'expired'), delivered_at: `${d4}T19:30:00Z` },
  rec('r-old', d4, '11:11:11', 'OLDNAME', 'BUY', 'enter', 'taken'),
  // delivered_at missing, payload created_at present: folds under created_at's day (d4).
  { ...rec('r-nodeliv', d4, '12:00:00', 'NODELIV', 'BUY', 'enter', null), delivered_at: null },
  // both missing: the undated bucket, last.
  rec('r-undated', null, '00:00:00', 'UNDATED', 'BUY', 'enter', null),
]

function dec(id, day, hms, agent, action, subject, verdict, reasons, paper = null) {
  return {
    is_paper: false,
    paper_verdict: paper && {
      verdict_id: `${id}-pv`, verdict: paper[0], reasons: paper[1], evaluated_at: `${day}T${hms}+05:30`, is_paper: true,
    },
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

// Reasons are what gate.py writes: "<rule_id>: <value> (limit <limit>)" for a failed rule, plus
// "shrink: qty A -> B (bound by …)"; a bare rule id (p-o2) stands for rows journalled before that.
let decisions = [
  dec('p-t1', today, '10:14:58', 'swing_analyst', 'exit', 'HDFCAMC', 'approve', []),
  // Real rejects, paper approves: the two verdicts sit side by side. A shrinkable rule alone only
  // rejects when nothing is approvable (gate.py "shrink floor").
  dec('p-t2', today, '09:41:01', 'swing_analyst', 'enter', 'TATASTEEL', 'reject', [
    'per_trade_risk: 10 x 812.40 = 8124.00 [1.5x stop distance from 2445.00 (daily-band leg unavailable in ' +
      'Phase 2 — gap mult only)] (limit <= 1.5% of equity 40000.00 = 600.00)',
    'shrink floor: implied max qty 0 — nothing approvable',
  ], ['approve', []]),
  // Paper verdicts come from the same gate, so their reasons are the same rule sentences.
  dec('p-t3', today, '09:40:00', 'swing_analyst', 'enter', 'SAIL', 'approve', [], ['reject', [
    'max_open_positions: total 3+0 pending+1 = 4; CNC 3+0+1 = 4 (limit total <= 3; MIS <= 2; CNC <= 3)',
  ]]),
  dec('p-y1', d1, '13:00:10', 'swing_analyst', 'exit', 'HDFCAMC', 'approve', []),
  dec('p-y2', d1, '13:00:05', 'swing_analyst', 'exit', 'HINDZINC', 'approve', []),
  dec('p-y3', d1, '10:01:50', 'swing_analyst', 'enter', 'CEIGALL', 'reject', [
    'max_open_positions: total 5+1 pending+1 = 7; CNC 5+1+1 = 7 (limit total <= 6; MIS <= 2; CNC <= 6)',
    'per_sector_exposure: Capital Goods: 2 open (limit < 2)',
  ]),
  dec('p-o1', d4, '11:11:00', 'intraday_analyst', 'enter', 'OLDNAME', 'shrink', [
    'per_trade_risk: 10 x 210.00 = 2100.00 [stop distance from 980.00] (limit <= 1.5% of equity 40000.00 = 600.00)',
    'shrink: qty 10 -> 2 (bound by per_trade_risk)',
  ]),
  dec('p-o2', d4, '10:20:00', 'intraday_analyst', 'enter', 'OLDNAME', 'reject', ['daily_loss_soft']),
  dec('p-u1', null, '00:00:00', 'intraday_analyst', 'enter', 'UNDATED', null, []),
]

// ALERTS: a long, realistic thesis on the live TATASTEEL entry (exercises wrapping / truncation).
const LONG_THESIS =
  'TATASTEEL closed at a fresh 52-week high on 1.8x its 20-day average volume, with the move led by ' +
  'flat-steel realisations and a narrowing European loss after the Netherlands restructuring. Domestic ' +
  'demand is firm into the infrastructure season, coking coal has eased for three straight weeks, and ' +
  'the stock is a NIFTY 200 member with no results due inside the 20-session hold. The breakout is ' +
  'ATR-sized rather than gap-driven, so the stop sits 2.5 ATR under the entry and risks about 1250 ' +
  'rupees; size is fixed by that risk budget. Failure mode: a close back under the breakout level within three sessions.'
if (ALERTS) {
  recommendations = recommendations.map((r) =>
    r.rec_id === 'r-t2' ? { ...r, recommendation: { ...r.recommendation, thesis: LONG_THESIS } } : r,
  )
}

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
// ALERTS: the week's quota almost gone — DG3, no forward cap. Per-agent spend sums to 191.2000.
if (ALERTS) {
  budgetFixture.degrade_tier = 'DG3'
  budgetFixture.budget = {
    ...budgetFixture.budget,
    window_spend_usd: '191.2000',
    per_agent_spend_usd: {
      intraday_analyst: '88.4000',
      news_analyst: '89.6000',
      preopen_planner: '7.9000',
      nightly_reviewer: '4.3000',
      weekly_researcher: '0.8000',
      reserve: '0',
      sdk_smoke: '0.2000',
    },
    forward_cap: 0,
  }
}

function pos(id, symbol, protectedAt) {
  return {
    position_id: id,
    symbol,
    side: 'BUY',
    style: 'swing',
    product: 'CNC',
    qty: 3,
    avg_entry: '2445.00',
    stop: '2321.00',
    target: null,
    state: 'OPEN',
    protection_state: null,
    is_paper: 0,
    origin: 'recommended',
    close_reason: null,
    opened_at: `${d1}T10:05:00+05:30`,
    closed_at: null,
    realized_pnl: null,
    costs: null,
    owner_protected_at: protectedAt,
    protection_reminders: protectedAt ? 0 : 2,
  }
}

const scorecardFixture = {
  label: 'hindsight on official bars — not platform equity',
  bench: { time: 'time: fill+1..exit', intrasession: 'intrasession: fill+1..exit-1' },
  strategies: {
    hi52: {
      recs: {
        n: 12, filled: 9, closed: 6, hit_rate: 0.5, median_net: 0.8, mean_net: 1.1, net_t20: 1.6,
        mean_excess: 0.4, actions: { taken: 3, dismissed: 4, expired: 5 },
        skip_reasons: { 'too far': 2, 'no cash': 1 }, daily_basis: 2, unscorable: 1,
      },
      paper: { closed: 4, hit_rate: 0.75, net: 1840.5, open: 2 },
    },
    unattributed: {
      recs: {
        n: 3, filled: 0, closed: 0, hit_rate: null, median_net: null, mean_net: null, net_t20: null,
        mean_excess: null, actions: { open: 3 }, skip_reasons: {}, daily_basis: 0, unscorable: 0,
      },
      paper: { closed: 0, hit_rate: null, net: null, open: 0 },
    },
  },
}

// GET /paper (plan "Response contract"). Six sessions on consecutive weekdays ending today; the epoch
// opened on the first. The figures tie out: pnl = realized + open_mtm and capital_base = equity - pnl
// on the latest snapshot; realized includes the void, the totals do not. The latest snapshot is 10:31:
// HINDZINC lost its mark after that, so snapshots (and paper halts) are paused and its LLM exit waits
// (PENDING_EXIT). Today's 1840.60 loss
// against yesterday's close latched the daily soft halt, so the entry guard reports FROZEN.
function weekdays(day, n) {
  let d = day
  for (let left = Math.abs(n); left > 0; ) {
    d = shift(d, Math.sign(n))
    if (![0, 6].includes(new Date(`${d}T12:00:00+05:30`).getUTCDay())) left -= 1
  }
  return d
}
const sessions = [5, 4, 3, 2, 1, 0].map((n) => weekdays(today, -n))
const at = (day, hms) => `${day}T${hms}+05:30`
const curveEquity = ['100042.10', '100318.65', '100905.40', '101420.75', '101953.90', '100113.30']

const paperBuilt = {
  enabled: true,
  changed_at: at(sessions[0], '09:02:41'),
  changed_by: 'telegram',
  epoch_started_at: at(sessions[0], '09:20:00'),
  reset_requested_at: null,
  subsystem_enabled: true,
  built: true,
  prep_ready: true,
  entry_guard: 'effective paper state is FROZEN',
  unmarked: ['HINDZINC'],
  halts: [{ cause: 'daily_loss_soft', rung: 'FROZEN', set_at: at(today, '10:29:00'), latched: false }],
  equity: {
    at: at(today, '10:31:00'),
    equity: '100113.30',
    realized_pnl: '-119.20',
    open_mtm: '232.50',
    day_mtm: '-1840.60',
    positions_open: 2,
    pnl: '113.30',
    capital_base: '100000.00',
  },
  curve: sessions.map((d, i) => ({
    d,
    at: i === sessions.length - 1 ? at(d, '10:31:00') : at(d, '15:29:00'),
    equity: curveEquity[i],
  })),
  positions: [
    {
      position_id: '01K6ZQ3N8V2C4D5E6F7G8H9J0K', symbol: 'JINDALSTEL', product: 'CNC', qty: 10,
      avg_entry: '1012.40', stop: '968.00', target: null, state: 'OPEN', protection_state: 'PROTECTED',
      strategy_id: 'hi52', exit_session: weekdays(sessions[2], 20), opened_at: at(sessions[2], '10:05:12'),
      mark: '1031.75', unrealized: '193.50',
    },
    {
      position_id: '01K71B7R2M3N4P5Q6R7S8T9V0W', symbol: 'HINDZINC', product: 'CNC', qty: 30,
      avg_entry: '468.20', stop: '452.00', target: '497.00', state: 'PENDING_EXIT', protection_state: 'PROTECTED',
      strategy_id: 'brk20', exit_session: weekdays(sessions[3], 10), opened_at: at(sessions[3], '13:12:40'),
      mark: null, unrealized: null,
    },
  ],
  // Unfilled entry: no positions row yet, so symbol/strategy come from the proposal payload.
  orders: [
    {
      order_id: '01K74D9X5Y6Z7A8B9C0D1E2F3G', position_id: '01K74D9X4H5J6K7M8N9P0Q1R2S', symbol: 'TATAPOWER',
      strategy_id: 'brk20', role: 'entry', side: 'BUY', qty: 45, filled_qty: 0, price: '412.30',
      trigger_price: null, state: 'ACKED', created_at: at(today, '09:47:05'),
    },
  ],
  closed: [
    {
      position_id: '01K6YA1B2C3D4E5F6G7H8J9K0M', symbol: 'TATASTEEL', strategy_id: 'brk20', qty: 40,
      entry_px: '168.20', exit_px: '176.85', net_pnl: '307.50', close_reason: 'target', close_basis: 'fill',
      outcome_label: 'win', closed_at: at(sessions[4], '15:16:30'),
    },
    {
      position_id: '01K6XB2C3D4E5F6G7H8J9K0M1N', symbol: 'SAIL', strategy_id: 'brk20', qty: 60,
      entry_px: '131.40', exit_px: '127.10', net_pnl: '-289.20', close_reason: 'stop', close_basis: 'fill',
      outcome_label: 'loss', closed_at: at(sessions[3], '11:48:02'),
    },
    {
      position_id: '01K6WC3D4E5F6G7H8J9K0M1N2P', symbol: 'CEIGALL', strategy_id: 'hi52', qty: 25,
      entry_px: '302.00', exit_px: '296.50', net_pnl: '-137.50', close_reason: 'void',
      close_basis: 'corp_action_relabel', outcome_label: 'void', closed_at: at(sessions[1], '14:02:11'),
    },
  ],
  totals: { closed: 2, wins: 1, voids: 1, void_net: '-137.50' },
  counters: { reconcile_mismatches: 0, voids: 0, late_corp_actions: 0 },
}

const paperOff = {
  enabled: false,
  changed_at: null,
  changed_by: null,
  epoch_started_at: null,
  reset_requested_at: null,
  subsystem_enabled: false,
  built: false,
  prep_ready: null,
  entry_guard: 'not built',
  unmarked: [],
  halts: [],
  equity: null,
  curve: [],
  positions: [],
  orders: [],
  closed: [],
  totals: { closed: 0, wins: 0, voids: 0, void_net: null },
  counters: null,
}

// The go-live view: built, autopilot OFF, prep not yet run, nothing stored.
const paperIdle = {
  ...paperOff,
  subsystem_enabled: true,
  built: true,
  prep_ready: false,
  entry_guard: 'paper session prep has not completed',
  counters: { reconcile_mismatches: 0, voids: 0, late_corp_actions: 0 },
}

const headroom = (over = {}) => ({
  equity: '512340.55',
  day_mtm: '2140.25',
  open_positions: { total: 2, mis: 0, cnc: 2 },
  consecutive_losses: 1,
  deployed_capital: '14670.00',
  caps: {
    max_deployed_capital_inr: '200000',
    max_open_positions_total: 5,
    max_open_positions_mis: 2,
    max_open_positions_cnc: 5,
    consecutive_losses_max_per_session: 3,
    max_new_trades_day: 6,
  },
  ...over,
})

function wl(id, symbol, grade, direction, eventType, o) {
  return {
    entry_id: id,
    d: today,
    symbol,
    grade,
    direction,
    event_type: eventType,
    cluster_refs: [],
    materiality: o.materiality,
    source_domain_count: o.sources,
    event_age_h: o.age_h,
    event_age_sessions: o.sessions,
    confirm_trigger: o.confirm,
    invalidation: o.invalid,
    stop_band_low: o.stop[0],
    stop_band_high: o.stop[1],
    target_band_low: o.target[0],
    target_band_high: o.target[1],
    expires_at: at(shift(today, 1), '15:30:00'),
  }
}
const wlBel = wl('w1', 'BEL', 'originating', 'long', 'order_win', {
  materiality: 0.82, sources: 4, age_h: 3.2, sessions: 0, confirm: '312.40', invalid: '298.00',
  stop: ['296.50', '299.00'], target: ['328.00', '335.00'],
})
const wlKpit = wl('w2', 'KPITTECH', 'originating', 'long', 'results_beat', {
  materiality: 0.74, sources: 3, age_h: 7.5, sessions: 1, confirm: '1348.00', invalid: '1296.00',
  stop: ['1290.00', '1300.00'], target: ['1410.00', '1440.00'],
})
const wlIrctc = wl('w3', 'IRCTC', 'context', null, 'rating_upgrade', {
  materiality: 0.41, sources: 2, age_h: 5.1, sessions: 0, confirm: '812.50', invalid: '789.00',
  stop: ['786.00', '791.00'], target: ['835.00', '850.00'],
})
const digestAt = (hms, age_h, status) => ({ as_of: at(today, hms), age_h, stale_max_h: 6, status })

const routes = {
  // risk/mode.py reports routing only in AUTO.
  '/mode': { mode: 'RECOMMEND', routing: null, risk_state: ALERTS ? 'KILLED' : 'NORMAL' },
  '/positions': {
    positions: [
      pos('pos-1', 'JINDALSTEL', `${d1}T10:20:00+05:30`),
      pos('pos-2', 'TATASTEEL', null),
      // The position the live stop-loss adjust (r-adj) belongs to.
      { ...pos('pos-4', 'COALINDIA', `${d1}T10:40:00+05:30`), qty: 10, avg_entry: '124.80', stop: '119.60' },
      ...(ALERTS ? [pos('pos-3', 'VERYLONGSYMBOLNAME20', null)] : []),
    ],
    as_of: new Date().toISOString(),
  },
  '/paper': PAPER_OFF ? paperOff : PAPER_IDLE ? paperIdle : paperBuilt,
  '/scorecard': scorecardFixture,
  '/decisions': { decisions },
  '/recommendations': { recommendations },
  '/risk/headroom': {
    headroom: ALERTS
      ? headroom({
          deployed_capital: '185000.00',
          open_positions: { total: 4, mis: 0, cnc: 4 },
          consecutive_losses: 2,
          floors: { floor_cumulative_floor: true },   // the floor that set KILLED, account back above it
        })
      : headroom(),
  },
  '/budget': budgetFixture,
  '/config/trade_window': { trade_window: { start: '09:30', end: '15:00', squareoff_buffer_min: 10 } },
  '/news/watchlist': ALERTS
    ? { d: today, watchlist: [wlBel, wlKpit, wlIrctc], digest: digestAt('05:05:00', 9.5, 'stale') }
    : { d: today, watchlist: [wlBel, wlIrctc], digest: digestAt('13:50:00', 0.6, 'fresh') },
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
      ...(ALERTS
        ? [
            {
              notification_id: 'n3',
              created_at: at(today, '11:02:00'),
              kind: 'limit_breach',
              severity: 'critical',
              title: 'Daily loss limit hit',
              body: 'day MTM -5.2% <= -5% soft line',
              status: 'failed',
              attempts: 9,
              delivered_at: null,
              last_error: 'ConnectTimeout: api.telegram.org timed out after 10s',
              last_attempt_at: at(today, '11:40:00'),
            },
            {
              notification_id: 'n4',
              created_at: at(today, '12:15:00'),
              kind: 'recommendation',
              severity: 'warning',
              title: 'BUY SAIL',
              body: 'Buy 3 SAIL at 131.40',
              status: 'pending',
              attempts: 3,
              delivered_at: null,
              last_error: 'ConnectTimeout',
              last_attempt_at: at(today, '12:20:00'),
            },
          ]
        : []),
    ],
  },
}

// First boot: every route answers its stub/empty shape (/paper keeps its POST handler's object).
if (EMPTY) {
  Object.assign(routes, {
    '/mode': { mode: null, routing: null, risk_state: null },
    '/positions': { positions: [], as_of: null },
    '/decisions': { decisions: [] },
    '/recommendations': { recommendations: [] },
    '/risk/headroom': { headroom: {} },
    '/budget': { budget: {}, degrade_tier: null },
    '/config/trade_window': { trade_window: null },
    '/news/watchlist': {
      d: null,
      watchlist: [],
      digest: { as_of: null, age_h: null, stale_max_h: null, status: null },
    },
    '/paper': { enabled: false },
    '/notifications': { d: today, rows: [] },
    '/scorecard': {
      label: 'hindsight on official bars — not platform equity',
      bench: {},
      strategies: {},
    },
  })
}

const types = { '.html': 'text/html', '.js': 'text/javascript', '.css': 'text/css', '.svg': 'image/svg+xml' }

function json(res, status, body) {
  res.writeHead(status, { 'content-type': 'application/json' })
  res.end(JSON.stringify(body))
}

// POST /paper as the engine answers it: 422 unless a JSON boolean, 409 for ON while not built.
function setPaper(req, res) {
  let raw = ''
  req.on('data', (chunk) => (raw += chunk))
  req.on('end', () => {
    let body = null
    try {
      body = JSON.parse(raw)
    } catch {
      /* falls through to the 422 */
    }
    const paper = routes['/paper']
    if (typeof body?.enabled !== 'boolean') return json(res, 422, { detail: [{ msg: 'enabled must be a boolean' }] })
    if (body.enabled && !paper.built) {
      return json(res, 409, {
        ok: false,
        note: 'paper subsystem not built (paper.subsystem_enabled is false or construction failed)',
      })
    }
    const changed = paper.enabled !== body.enabled
    paper.enabled = body.enabled
    json(res, 200, { ok: true, enabled: body.enabled, changed })
  })
}

function readJson(req, then) {
  let raw = ''
  req.on('data', (chunk) => (raw += chunk))
  req.on('end', () => {
    let body = null
    try {
      body = JSON.parse(raw)
    } catch {
      /* body stays null: the handler answers 422 */
    }
    then(body)
  })
}

// POST /mode as the engine answers it: OFF / RECOMMEND apply, AUTO is Telegram-only (409), else 422.
function setMode(req, res) {
  readJson(req, (body) => {
    const mode = body?.mode
    if (mode === 'OFF' || mode === 'RECOMMEND') {
      routes['/mode'] = { ...routes['/mode'], mode }
      return json(res, 200, { ok: true, mode })
    }
    if (mode === 'AUTO') return json(res, 409, { ok: false, note: 'two-step via Telegram /mode AUTO + /confirm' })
    json(res, 422, { detail: [{ msg: 'unknown mode' }] })
  })
}

// POST /config/trade_window: HH:MM start < end and a null / non-negative-integer buffer, else 422 unchanged.
function setTradeWindow(req, res) {
  readJson(req, (body) => {
    const hhmm = /^\d{2}:\d{2}$/
    const buf = body?.squareoff_buffer_min
    const ok =
      typeof body?.start === 'string' &&
      typeof body?.end === 'string' &&
      hhmm.test(body.start) &&
      hhmm.test(body.end) &&
      body.start < body.end &&
      (buf === null || (Number.isInteger(buf) && buf >= 0))
    if (!ok) return json(res, 422, { detail: 'invalid trade window (rejected, unchanged)' })
    routes['/config/trade_window'] = {
      trade_window: { start: body.start, end: body.end, squareoff_buffer_min: buf },
    }
    json(res, 200, { ok: true })
  })
}

// POST /risk/rearm as the engine answers it: clears the floors back above their line; a kill stays.
function rearm(req, res) {
  const h = routes['/risk/headroom'].headroom
  const floors = h.floors ?? {}
  const cleared = Object.keys(floors).filter((c) => floors[c])
  const still = Object.keys(floors).filter((c) => !floors[c])
  h.floors = Object.fromEntries(still.map((c) => [c, false]))
  json(res, 200, { ok: true, cleared, still_breached: still, risk_state: routes['/mode'].risk_state })
}

const posts = { '/paper': setPaper, '/mode': setMode, '/config/trade_window': setTradeWindow, '/risk/rearm': rearm }

http
  .createServer((req, res) => {
    const url = new URL(req.url, `http://127.0.0.1:${PORT}`)
    if (req.method === 'POST' && url.pathname in posts) return posts[url.pathname](req, res)
    if (url.pathname in routes) {
      if (req.method !== 'GET') return json(res, 405, { detail: 'Method Not Allowed' })
      return json(res, 200, routes[url.pathname])
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
      `fixture server on http://127.0.0.1:${PORT}/  today=${today} d1=${d1} d3=${d3} d4=${d4}${NO_TODAY ? '  (NO_TODAY)' : ''}${PAPER_OFF ? '  (PAPER_OFF)' : ''}${PAPER_IDLE ? '  (PAPER_IDLE)' : ''}${ALERTS ? '  (ALERTS)' : ''}${EMPTY ? '  (EMPTY)' : ''}`,
    ),
  )
