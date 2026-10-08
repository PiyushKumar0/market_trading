import { test } from 'node:test'
import assert from 'node:assert/strict'
import type { NotificationRow, PaperSummary, PositionRow, Snapshot } from '../src/types.ts'
import { ALL_KEYS, deriveAlerts, health, tabBadges } from '../src/lib/alerts.ts'
import type { AlertInput } from '../src/lib/alerts.ts'

const NOW = Date.parse('2026-10-08T09:00:00Z')

const EMPTY: Snapshot = {
  mode: null,
  positions: null,
  decisions: null,
  recommendations: null,
  headroom: null,
  budget: null,
  tradeWindow: null,
  watchlist: null,
  paper: null,
}

const HEALTHY: Snapshot = {
  ...EMPTY,
  mode: { mode: 'RECOMMEND', routing: 'paper', risk_state: 'NORMAL' },
  positions: { positions: [], as_of: null },
  tradeWindow: { trade_window: { start: '09:30', end: '15:00', squareoff_buffer_min: 10 } },
  budget: { budget: {}, degrade_tier: 'DG0' },
  watchlist: { d: '2026-10-08', watchlist: [], digest: { as_of: null, age_h: null, stale_max_h: null, status: 'fresh' } },
}

function input(over: Partial<AlertInput> = {}, snap: Partial<Snapshot> = {}): AlertInput {
  return {
    snapshot: { ...HEALTHY, ...snap },
    failedKeys: [],
    unauthorized: false,
    polled: true,
    notifications: [],
    ackedFailureId: null,
    ...over,
  }
}

function position(over: Partial<PositionRow> = {}): PositionRow {
  return {
    position_id: 'p1', symbol: 'TATASTEEL', side: 'BUY', style: 'swing', product: 'CNC', qty: 3,
    avg_entry: '2445.00', stop: '2321.00', target: null, state: 'OPEN', protection_state: null, is_paper: 0,
    origin: 'recommended', close_reason: null, opened_at: null, closed_at: null, realized_pnl: null, costs: null,
    owner_protected_at: null, protection_reminders: 0, ...over,
  }
}

function note(id: string, status: NotificationRow['status'], attempts = 1): NotificationRow {
  return {
    notification_id: id, created_at: '2026-10-08T09:30:00+05:30', kind: 'recommendation', severity: 'info',
    title: id, body: null, status, attempts, delivered_at: null, last_error: null, last_attempt_at: null,
  }
}

const ids = (i: AlertInput) => deriveAlerts(i).map((a) => a.id)

test('nothing loaded yet means no alerts', () => {
  assert.deepEqual(deriveAlerts(input({ polled: false }, EMPTY)), [])
})

test('a healthy engine raises nothing', () => {
  assert.deepEqual(deriveAlerts(input()), [])
  assert.deepEqual(health([]), { level: 'ok', text: 'All normal' })
})

test('the kill switch is a critical alert and names the pill', () => {
  const alerts = deriveAlerts(input({}, { mode: { mode: 'RECOMMEND', routing: null, risk_state: 'KILLED' } }))
  assert.equal(alerts[0].id, 'killed')
  assert.equal(alerts[0].level, 'critical')
  assert.equal(alerts[0].title, 'Kill switch is on — all trading is stopped')
  assert.deepEqual(health(alerts), { level: 'critical', text: 'Kill switch on' })
})

test('risk states map to their levels', () => {
  const frozen = deriveAlerts(input({}, { mode: { mode: 'RECOMMEND', routing: null, risk_state: 'FROZEN' } }))
  assert.equal(frozen[0].level, 'warning')
  const closeOnly = deriveAlerts(input({}, { mode: { mode: 'RECOMMEND', routing: null, risk_state: 'CLOSE_ONLY' } }))
  assert.equal(closeOnly[0].level, 'critical')
})

test('an unprotected recommended position is named', () => {
  const alerts = deriveAlerts(input({}, { positions: { positions: [position()], as_of: null } }))
  assert.equal(alerts.length, 1)
  assert.equal(alerts[0].level, 'warning')
  assert.equal(alerts[0].title, 'TATASTEEL has no confirmed stop-loss')
  assert.match(alerts[0].body, /\/protected TATASTEEL/)
})

test('protected, closed, external and paper positions raise nothing', () => {
  const rows = [
    position({ position_id: 'a', owner_protected_at: '2026-10-07T10:20:00+05:30' }),
    position({ position_id: 'b', state: 'CLOSED' }),
    position({ position_id: 'c', origin: 'external' }),
    position({ position_id: 'd', is_paper: 1 }),
  ]
  assert.deepEqual(deriveAlerts(input({}, { positions: { positions: rows, as_of: null } })), [])
})

test('several unprotected positions fold into one alert', () => {
  const rows = [position({ position_id: 'a' }), position({ position_id: 'b', symbol: 'SAIL' })]
  const alerts = deriveAlerts(input({}, { positions: { positions: rows, as_of: null } }))
  assert.equal(alerts.length, 1)
  assert.equal(alerts[0].title, '2 positions have no confirmed stop-loss')
  assert.match(alerts[0].body, /TATASTEEL, SAIL/)
})

test('every route failing means the engine is unreachable', () => {
  const alerts = deriveAlerts(input({ failedKeys: [...ALL_KEYS] }))
  assert.equal(alerts[0].id, 'unreachable')
  assert.equal(alerts[0].level, 'critical')
})

test('some routes failing lists them in words', () => {
  const alerts = deriveAlerts(input({ failedKeys: ['positions', 'budget'] }))
  assert.equal(alerts[0].id, 'partial')
  assert.equal(alerts[0].level, 'warning')
  assert.equal(alerts[0].title, "Some data couldn't load: positions, AI budget")
})

test('a rejected token asks to sign out and hides route failures', () => {
  const alerts = deriveAlerts(input({ unauthorized: true, failedKeys: [...ALL_KEYS] }))
  assert.deepEqual(alerts.map((a) => a.id), ['token'])
  assert.equal(alerts[0].action, 'signout')
})

test('an unknown news freshness is not reported as stale', () => {
  const unknown = { d: null, watchlist: [], digest: { as_of: null, age_h: null, stale_max_h: null, status: null } }
  assert.deepEqual(ids(input({}, { watchlist: unknown })), [])
  const stale = { ...unknown, digest: { ...unknown.digest, status: 'stale' as const } }
  const alerts = deriveAlerts(input({}, { watchlist: stale }))
  assert.equal(alerts[0].id, 'news')
  assert.equal(alerts[0].level, 'notice')
  assert.equal(alerts[0].title, 'News summary is stale')
})

test('AI budget levels escalate', () => {
  const at = (tier: string) => deriveAlerts(input({}, { budget: { budget: {}, degrade_tier: tier } }))
  assert.deepEqual(at('DG1'), [])
  assert.equal(at('DG2')[0].level, 'notice')
  assert.equal(at('DG3')[0].level, 'warning')
  assert.equal(at('DG4')[0].level, 'critical')
})

test('the paper stub and a disabled paper book raise nothing', () => {
  assert.deepEqual(ids(input({}, { paper: { enabled: false } })), [])
})

function paper(over: Partial<PaperSummary> = {}): PaperSummary {
  return {
    enabled: true, changed_at: null, changed_by: null, epoch_started_at: null, reset_requested_at: null,
    subsystem_enabled: true, built: true, prep_ready: true, entry_guard: null, unmarked: [], halts: [],
    equity: null, curve: [], positions: [], orders: [], closed: [],
    totals: { closed: 0, wins: 0, voids: 0, void_net: null }, counters: null, ...over,
  }
}

test('a paper halt is a notice that names the limit', () => {
  const halts = [{ cause: 'daily_loss_soft', rung: 'FROZEN', set_at: '2026-10-08T10:29:00+05:30', latched: false }]
  const alerts = deriveAlerts(input({}, { paper: paper({ halts }) }))
  assert.equal(alerts[0].id, 'paper-halt')
  assert.equal(alerts[0].level, 'notice')
  assert.equal(alerts[0].title, 'Paper autopilot paused: daily loss limit (soft)')
})

test('a paper build failure is a notice', () => {
  const alerts = deriveAlerts(input({}, { paper: paper({ built: false, enabled: false }) }))
  assert.equal(alerts[0].id, 'paper-build')
})

test('failed Telegram messages warn until acknowledged, then again on a newer failure', () => {
  const rows = [note('n1', 'delivered'), note('n2', 'failed'), note('n3', 'failed')]
  const first = deriveAlerts(input({ notifications: rows }))
  assert.equal(first[0].id, 'telegram-failed')
  assert.equal(first[0].level, 'warning')
  assert.equal(first[0].title, '2 Telegram messages were not delivered')
  assert.equal(first[0].action, 'ack-telegram')
  assert.equal(first[0].ackId, 'n3')
  assert.deepEqual(ids(input({ notifications: rows, ackedFailureId: 'n3' })), [])
  const later = [...rows, note('n4', 'failed')]
  const again = deriveAlerts(input({ notifications: later, ackedFailureId: 'n3' }))
  assert.equal(again[0].title, '1 Telegram message was not delivered')
})

test('Telegram retries are a warning', () => {
  const alerts = deriveAlerts(input({ notifications: [note('n1', 'pending', 3)] }))
  assert.equal(alerts[0].id, 'telegram-retrying')
  assert.equal(alerts[0].title, 'Telegram delivery is retrying (1)')
})

test('engine off is a notice; missing hours a warning only once the mode is known', () => {
  const off = deriveAlerts(input({}, { mode: { mode: 'OFF', routing: null, risk_state: 'NORMAL' } }))
  assert.equal(off[0].id, 'mode-off')
  assert.equal(off[0].level, 'notice')
  const noHours = deriveAlerts(input({}, { tradeWindow: { trade_window: null } }))
  assert.equal(noHours[0].id, 'no-hours')
  const stub = deriveAlerts(input({}, { tradeWindow: { trade_window: null }, mode: { mode: null, routing: null, risk_state: null } }))
  assert.deepEqual(stub.map((a) => a.id), [])
})

test('alerts sort critical, then warning, then notice', () => {
  const alerts = deriveAlerts(
    input(
      { notifications: [note('n1', 'pending', 2)] },
      {
        mode: { mode: 'OFF', routing: null, risk_state: 'KILLED' },
        budget: { budget: {}, degrade_tier: 'DG2' },
      },
    ),
  )
  assert.deepEqual(alerts.map((a) => a.level), ['critical', 'warning', 'notice', 'notice'])
  assert.deepEqual(health(alerts).level, 'critical')
})

test('health counts what needs attention, or the notices alone', () => {
  const warn = deriveAlerts(input({}, { mode: { mode: 'RECOMMEND', routing: null, risk_state: 'FROZEN' } }))
  assert.deepEqual(health(warn), { level: 'warning', text: '1 needs attention' })
  const notes = deriveAlerts(input({}, { mode: { mode: 'OFF', routing: null, risk_state: 'NORMAL' } }))
  assert.deepEqual(health(notes), { level: 'notice', text: '1 notice' })
})

test('tab badges', () => {
  const recs = {
    recommendations: [
      {
        rec_id: 'r1', delivered_at: null, human_action: null, human_fill_price: null, outcome: null,
        recommendation: { valid_until: '2026-10-08T15:30:00+05:30' },
      },
    ],
  }
  const b = tabBadges(input({ notifications: [note('n1', 'failed')] }, { recommendations: recs, budget: { budget: {}, degrade_tier: 'DG3' } }), NOW)
  assert.deepEqual(b.recommendations, { text: '1', tone: 'accent', label: '1 waiting for you' })
  assert.deepEqual(b.activity, { text: '1', tone: 'crit', label: '1 Telegram message not delivered' })
  assert.deepEqual(b.system, { text: '!', tone: 'warn', label: 'needs attention' })
  assert.equal(b.paper, undefined)
  assert.deepEqual(tabBadges(input(), NOW), {})
})

test('a limit at 90 % or more flags the System tab', () => {
  const headroom = {
    headroom: {
      deployed_capital: '95000', open_positions: { total: 1, mis: 0, cnc: 1 }, consecutive_losses: 0,
      caps: {
        max_deployed_capital_inr: '100000', max_open_positions_total: 5, max_open_positions_mis: 2,
        max_open_positions_cnc: 5, consecutive_losses_max_per_session: 3, max_new_trades_day: 5,
      },
    },
  }
  assert.deepEqual(tabBadges(input({}, { headroom }), NOW).system, { text: '!', tone: 'warn', label: 'needs attention' })
})
