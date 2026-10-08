/**
 * What needs the owner's attention, derived from the data already polled (design spec §5). Pure and
 * DOM-free (`node --test`). The navbar health pill, the Overview "Needs attention" card and the tab
 * badges all read from here, so they can never disagree.
 *
 * Levels: critical (trading stopped or the page is blind) → warning (act soon) → notice (worth
 * knowing, no action). A stale news digest and an Off engine are notices on purpose: they can last all
 * day and must not cry wolf.
 */
import { haltTerm } from '../copy/glossary.ts'
import type { Headroom, NotificationRow, PaperResponse, PaperSummary, PositionRow, Snapshot } from '../types.ts'
import { liveRecs } from './recs.ts'

export type Level = 'critical' | 'warning' | 'notice'
export type TabId = 'overview' | 'recommendations' | 'paper' | 'performance' | 'activity' | 'system'

export interface Alert {
  id: string
  level: Level
  title: string
  body: string
  /** The tab that shows the detail. */
  tab?: TabId
  action?: 'signout' | 'ack-telegram' | 'rearm'
  /** For `ack-telegram`: every failed notification id shown now, to remember as seen. */
  ackIds?: string[]
}

export type Health = { level: Level | 'ok' | 'pending'; text: string }

export interface AlertInput {
  snapshot: Snapshot
  /** Snapshot keys whose latest fetch failed. */
  failedKeys: string[]
  unauthorized: boolean
  /** At least one poll cycle has completed. */
  polled: boolean
  /** Today's Telegram journal, chronological (oldest first), as served. */
  notifications: NotificationRow[]
  /** Failed notifications the owner already acknowledged ("Got it"). A set, not a position in the
   *  list: the engine re-queues a failed row with a dedupe key, which moves it out of the failed set. */
  ackedFailureIds: string[]
}

export interface Badge {
  text: string
  tone: 'accent' | 'warn' | 'crit'
  /** Read by screen readers after the tab name. */
  label: string
}

/** Every key the 10 s poll fetches, in fetch order (shared with `usePoll`). */
export const ALL_KEYS = [
  'mode',
  'positions',
  'decisions',
  'recommendations',
  'headroom',
  'budget',
  'tradeWindow',
  'watchlist',
  'paper',
] as const satisfies readonly (keyof Snapshot)[]

const KEY_NAMES: Record<(typeof ALL_KEYS)[number], string> = {
  mode: 'engine status',
  positions: 'positions',
  decisions: 'risk checks',
  recommendations: 'recommendations',
  headroom: 'risk limits',
  budget: 'AI budget',
  tradeWindow: 'trading hours',
  watchlist: 'news watchlist',
  paper: 'paper autopilot',
}

const RANK: Record<Level, number> = { critical: 0, warning: 1, notice: 2 }
const plural = (n: number, one: string, many: string) => (n === 1 ? one : many)
const lowerFirst = (s: string) => (s ? s[0].toLowerCase() + s.slice(1) : s)

export function isPaperSummary(p: PaperResponse | null | undefined): p is PaperSummary {
  return !!p && 'built' in p
}

function unprotected(rows: PositionRow[]): PositionRow[] {
  return rows.filter((p) => !p.is_paper && p.origin === 'recommended' && p.state === 'OPEN' && !p.owner_protected_at)
}

function failedRows(rows: NotificationRow[]): NotificationRow[] {
  return rows.filter((r) => r.status === 'failed')
}

/** What "Got it" acknowledges: this failure of this row. telegram.py re-queues a failed keyed message
 *  by rewriting the SAME row (new created_at), so a second failure gets a new key and warns again. */
const ackKey = (r: NotificationRow) => `${r.notification_id}@${r.created_at ?? ''}`

function retryingRows(rows: NotificationRow[]): NotificationRow[] {
  return rows.filter((r) => r.status === 'pending' && (r.attempts ?? 0) > 0)
}

export function deriveAlerts(i: AlertInput): Alert[] {
  if (i.unauthorized) {
    return [
      {
        id: 'token',
        level: 'critical',
        title: 'The dashboard token was rejected',
        body: 'Sign out and enter the current token.',
        action: 'signout',
      },
    ]
  }

  const out: Alert[] = []
  const add = (a: Alert) => out.push(a)
  const s = i.snapshot
  const risk = s.mode?.risk_state ?? null
  const tier = s.budget?.degrade_tier ?? null

  // ---- critical
  const failed = new Set(i.failedKeys)
  const allFailed = ALL_KEYS.every((k) => failed.has(k))
  if (i.polled && allFailed) {
    add({
      id: 'unreachable',
      level: 'critical',
      title: "Can't reach the engine",
      body: 'No part of the page could refresh. Check that the engine is running; the page retries every 10 seconds.',
    })
  }
  // A floor stop (weekly drawdown, equity floor, total-loss floor) stays until the owner re-arms it,
  // which the engine allows only once the account is back above that floor's line (POST /risk/rearm).
  const floors = Object.values(s.headroom?.headroom.floors ?? {})
  const rearmable = floors.some(Boolean)
  const floorNote = !floors.length
    ? ''
    : rearmable
      ? ' The account is back above its floor: re-arm to allow new trades again.'
      : ' A floor stop stays until the account is back above its line; then re-arm it here or with /rearm.'
  if (risk === 'KILLED') {
    add({
      id: 'killed',
      level: 'critical',
      title: 'Kill switch is on — no new trades',
      body:
        'Open positions are not closed for you; exit alerts still arrive. Reset on Telegram: /kill_reset, then ' +
        `/confirm with the phrase the bot sends.${floorNote}`,
      tab: 'system',
      action: rearmable ? 'rearm' : undefined,
    })
  }
  if (risk === 'CLOSE_ONLY') {
    add({
      id: 'close-only',
      level: 'critical',
      title: 'Closing only — no new positions',
      body: `A risk limit allows exits only. A daily-loss stop lifts at the next day’s engine start.${floorNote}`,
      tab: 'system',
      action: rearmable ? 'rearm' : undefined,
    })
  }
  if (tier === 'DG4') {
    add({
      id: 'ai-stopped',
      level: 'critical',
      title: 'AI analysis is stopped for the week',
      // governor.py: DG4 = spend at the weekly credit OR a billing error in this window; both end with it.
      body: 'The weekly AI budget is used up, or the AI provider reported a billing error. AI calls resume Thursday 14:00 IST.',
      tab: 'system',
    })
  }

  // ---- warning
  if (risk === 'FROZEN') {
    add({
      id: 'frozen',
      level: 'warning',
      title: 'New positions are paused',
      body:
        'A risk check is blocking new entries until its cause clears. Exits and stop-losses still work. ' +
        'If you paused entries yourself, /resume_entries on Telegram resumes them.',
      tab: 'system',
    })
  }
  const bare = unprotected(s.positions?.positions ?? [])
  if (bare.length === 1) {
    const sym = bare[0].symbol
    // pipeline.py checklists: delivery (CNC) protects with a GTT, intraday (MIS) with an SL-M order.
    const order = bare[0].product === 'MIS' ? 'SL-M stop-loss order' : 'GTT stop-loss'
    add({
      id: 'unprotected',
      level: 'warning',
      title: `${sym} has no confirmed stop-loss`,
      body: `Place its ${order} in Kite, then reply /protected ${sym} on Telegram.`,
      tab: 'overview',
    })
  } else if (bare.length > 1) {
    add({
      id: 'unprotected',
      level: 'warning',
      title: `${bare.length} positions have no confirmed stop-loss`,
      body: `${bare.map((p) => p.symbol).join(', ')} — place each stop-loss order in Kite (a GTT for delivery), then reply /protected SYMBOL on Telegram.`,
      tab: 'overview',
    })
  }
  const failures = failedRows(i.notifications)
  if (failures.length) {
    const acked = new Set(i.ackedFailureIds)
    const fresh = failures.filter((r) => !acked.has(ackKey(r)))
    if (fresh.length) {
      add({
        id: 'telegram-failed',
        level: 'warning',
        title: `${fresh.length} Telegram ${plural(fresh.length, 'message was', 'messages were')} not delivered`,
        body: plural(fresh.length, 'You may have missed it — read it in Activity.', 'You may have missed them — read them in Activity.'),
        tab: 'activity',
        action: 'ack-telegram',
        ackIds: failures.map(ackKey),
      })
    }
  }
  const retrying = retryingRows(i.notifications)
  if (retrying.length) {
    add({
      id: 'telegram-retrying',
      level: 'warning',
      title: `Telegram delivery is retrying (${retrying.length})`,
      body: "The engine keeps retrying. Check Activity if it doesn't clear.",
      tab: 'activity',
    })
  }
  if (i.polled && !allFailed && failed.size) {
    const names = ALL_KEYS.filter((k) => failed.has(k)).map((k) => KEY_NAMES[k])
    add({
      id: 'partial',
      level: 'warning',
      title: `Some data couldn't load: ${names.join(', ')}`,
      // A route that never answered has no last values: its card says it couldn't load (ui/Loading).
      body: "Their cards keep the last values received, or say they couldn't load. The page retries every 10 seconds.",
    })
  }
  if (s.tradeWindow && s.tradeWindow.trade_window === null && s.mode?.mode) {
    add({
      id: 'no-hours',
      level: 'warning',
      title: 'No trading hours are set',
      body: 'New entries are blocked until you set trading hours (top bar).',
    })
  }
  if (tier === 'DG3') {
    add({
      id: 'ai-minimal',
      level: 'warning',
      title: 'AI analysts are paused',
      body:
        'Weekly AI spend is near the limit: no new recommendations and no AI stop-loss warnings until Thursday 14:00 ' +
        'IST. Planned-date exit alerts and your own stop orders in Kite still work.',
      tab: 'system',
    })
  }

  // ---- notice
  if (s.mode?.mode === 'OFF') {
    add({
      id: 'mode-off',
      level: 'notice',
      title: 'The engine is off',
      body:
        'No new trade ideas; exit alerts for your open positions still arrive. Switch to Recommend in the top bar to ' +
        'resume.',
    })
  }
  if (tier === 'DG2') {
    add({
      id: 'ai-saving',
      level: 'notice',
      title: 'AI is in saving mode',
      body: 'Background AI check-ins are off and fewer scanner candidates reach the AI, to stay in budget. Recommendations continue.',
      tab: 'system',
    })
  }
  const digest = s.watchlist?.digest.status ?? null
  if (digest === 'stale' || digest === 'missing') {
    add({
      id: 'news',
      level: 'notice',
      title: `News summary is ${digest}`,
      body: 'News-catalyst candidates (still in validation) pause until it refreshes; no recommendation depends on it.',
      tab: 'activity',
    })
  }
  const p = isPaperSummary(s.paper) ? s.paper : null
  if (p && p.built && p.enabled && p.halts.length) {
    add({
      id: 'paper-halt',
      level: 'notice',
      title:
        p.halts.length === 1
          ? `Paper autopilot paused: ${lowerFirst(haltTerm(p.halts[0].cause).label)}`
          : `Paper autopilot paused: ${p.halts.length} limits active`,
      body: 'Simulated trading only — your real account is not affected.',
      tab: 'paper',
    })
  }
  if (p && p.subsystem_enabled && !p.built) {
    add({
      id: 'paper-build',
      level: 'notice',
      title: 'Paper autopilot failed to start',
      body: 'Nothing is simulated until the engine restarts. The paper alert in Telegram messages says why.',
      tab: 'paper',
    })
  }

  return out
    .map((a, n) => ({ a, n }))
    .sort((x, y) => RANK[x.a.level] - RANK[y.a.level] || x.n - y.n)
    .map(({ a }) => a)
}

/** `polled` false = no poll cycle has answered yet: "All normal" would be a claim about nothing. */
export function health(alerts: Alert[], polled = true): Health {
  if (!alerts.length) return polled ? { level: 'ok', text: 'All normal' } : { level: 'pending', text: 'Connecting…' }
  if (alerts.some((a) => a.id === 'killed')) return { level: 'critical', text: 'Kill switch on' }
  const level = alerts[0].level
  const urgent = alerts.filter((a) => a.level !== 'notice').length
  if (urgent) return { level, text: `${urgent} ${plural(urgent, 'needs', 'need')} attention` }
  return { level: 'notice', text: `${alerts.length} ${plural(alerts.length, 'notice', 'notices')}` }
}

const toNum = (v: string | number | null | undefined): number | null => {
  if (v === null || v === undefined || v === '') return null
  const x = Number(v)
  return Number.isFinite(x) ? x : null
}

/** Any risk limit at 90 % of its cap or more (geometry only — nothing printed comes from this). */
export function nearLimit(h: Headroom | null | undefined): boolean {
  const caps = h?.caps
  if (!h || !caps) return false
  const pairs: [number | null, number | null][] = [
    [toNum(h.deployed_capital), toNum(caps.max_deployed_capital_inr)],
    [toNum(h.open_positions?.total), toNum(caps.max_open_positions_total)],
    [toNum(h.open_positions?.mis), toNum(caps.max_open_positions_mis)],
    [toNum(h.open_positions?.cnc), toNum(caps.max_open_positions_cnc)],
    [toNum(h.consecutive_losses), toNum(caps.consecutive_losses_max_per_session)],
  ]
  return pairs.some(([used, cap]) => used !== null && cap !== null && cap > 0 && used / cap >= 0.9)
}

export function tabBadges(i: AlertInput, nowMs: number): Partial<Record<TabId, Badge>> {
  const s = i.snapshot
  const out: Partial<Record<TabId, Badge>> = {}

  const live = liveRecs(s.recommendations?.recommendations ?? [], nowMs).length
  if (live) out.recommendations = { text: String(live), tone: 'accent', label: `${live} waiting for you` }

  const p = isPaperSummary(s.paper) ? s.paper : null
  if (p && ((p.built && p.enabled && p.halts.length > 0) || (p.subsystem_enabled && !p.built))) {
    out.paper = { text: '!', tone: 'warn', label: 'needs attention' }
  }

  const failed = failedRows(i.notifications).length
  const retrying = retryingRows(i.notifications).length
  if (failed || retrying) {
    const parts = [
      failed ? `${failed} Telegram ${plural(failed, 'message', 'messages')} not delivered` : '',
      retrying ? `${retrying} retrying` : '',
    ].filter(Boolean)
    out.activity = { text: String(failed + retrying), tone: failed ? 'crit' : 'warn', label: parts.join(', ') }
  }

  const tier = s.budget?.degrade_tier ?? null
  if (tier === 'DG4') out.system = { text: '!', tone: 'crit', label: 'needs attention' }
  else if (tier === 'DG2' || tier === 'DG3' || nearLimit(s.headroom?.headroom)) {
    out.system = { text: '!', tone: 'warn', label: 'needs attention' }
  }
  return out
}
