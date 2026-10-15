/**
 * The dashboard's copy deck: every engine value → a plain label, a one-line hint, and the engine's own
 * term (kept so Telegram messages and logs can still be matched). One place, so the same thing is
 * called the same everywhere. Pure and DOM-free (`node --test` runs it).
 *
 * Sources, read 2026-10-08: risk/mode.py (modes, risk states, trade window), core/types.py
 * (TradeWindow), plan §5.6 + intelligence/governor.py (degrade ladder), risk/gate.py (verdicts, rule
 * IDs), ops/paper_risk.py + RUNBOOK paper halt table (halts), ops/scorecard.py (scorecard),
 * notify/telegram.py (`/resume_entries`), notify/catalog.py (message kinds), strategy/scanners/*.py
 * (strategy names). Unknown values fall back to the humanised raw value — never an exception.
 */
import { humanize } from '../lib/format.ts'

export interface Term {
  /** What the owner reads. */
  label: string
  /** One plain sentence; shown behind the ⓘ toggletip. */
  hint?: string
  /** The engine's own value, shown in the hint as "Engine term". */
  term?: string
}

type Entry = { label: string; hint?: string }
type Table = Record<string, Entry>

const DASH: Term = { label: '—' }

function lookup(table: Table, v: string | null | undefined, fallback: (v: string) => string = humanize): Term {
  if (v === null || v === undefined || v === '') return DASH
  const hit = Object.prototype.hasOwnProperty.call(table, v) ? table[v] : undefined
  return hit ? { ...hit, term: v } : { label: fallback(v) || v, term: v }
}

// ------------------------------------------------------------------------------------ engine state

// Verified against the engine on 2026-10-08 (copy-accuracy review): in Recommend and Off the engine
// places NO orders in Kite (telegram.py, pipeline.py "the platform places none in RECOMMEND"); exit and
// stop-warning alerts fire in every mode; Auto is refused while settings say paper-only (mode.py).
const MODE: Table = {
  OFF: {
    label: 'Off',
    hint:
      'No new trade ideas. The engine still watches your open positions and sends exit alerts on Telegram; you place ' +
      'the exits in Kite. Paper positions keep their simulated exits.',
  },
  RECOMMEND: {
    label: 'Recommend',
    hint: 'The engine proposes trades and sends them to you on Telegram; you place them in Kite yourself.',
  },
  AUTO: {
    label: 'Auto',
    hint:
      'Auto-trading. It is switched off in this version (paper-only settings) and cannot be turned on from the ' +
      'dashboard; the engine does not place orders in your Kite account.',
  },
}
export const modeTerm = (v: string | null | undefined): Term => lookup(MODE, v)

const ROUTING: Table = {
  paper: { label: 'Paper (simulated)', hint: 'In Auto, orders would go to the simulated paper account — never to Kite.' },
  live: { label: 'Live account', hint: 'Live routing is switched off in this version (paper-only settings).' },
}
export const routingTerm = (v: string | null | undefined): Term => lookup(ROUTING, v)

const RISK: Table = {
  NORMAL: { label: 'Normal', hint: 'No engine-wide pause; each new trade is still checked by the risk rules.' },
  FROZEN: {
    label: 'New positions paused',
    hint:
      'A risk check is blocking new entries until its cause clears. Exits and stop-losses still work. ' +
      'If you paused entries yourself, /resume_entries on Telegram resumes them.',
  },
  CLOSE_ONLY: {
    label: 'Closing only',
    hint:
      'Exits only, no new positions. A daily-loss stop lifts at the next day’s engine start. A weekly-drawdown or ' +
      'equity-floor stop stays until the account is back above its line and you re-arm it (here or /rearm).',
  },
  KILLED: {
    label: 'Kill switch on',
    hint:
      'No new trades, real or paper. Open positions are not closed for you; exit alerts still arrive. Reset on ' +
      'Telegram: /kill_reset, then /confirm with the phrase the bot sends (within 2 minutes). If the total-loss floor ' +
      'set it, that stop also needs a re-arm once the account is back above its line.',
  },
}
export const riskTerm = (v: string | null | undefined): Term => lookup(RISK, v)

const TIER: Table = {
  DG0: {
    label: 'Normal',
    hint: 'AI analysts run normally (an analyst that has used up its own weekly share still pauses; see Spend by analyst).',
  },
  DG1: {
    label: 'Pacing',
    hint:
      'AI spending is ahead of the weekly pace: check-ins slow to every 45 minutes and about a third fewer scanner ' +
      'candidates reach the AI each day. Each one is still analysed the same way.',
  },
  // governor.py: heartbeats stop from DG2; the forward cap is cut at every level above DG0.
  DG2: {
    label: 'Saving',
    hint:
      'Background AI check-ins are off and about a third fewer scanner candidates reach the AI each day, to stay ' +
      'within the weekly budget. Each one is still analysed the same way.',
  },
  DG3: {
    label: 'Minimal',
    hint:
      'AI analysts are paused until Thursday 14:00 IST: no new recommendations and no AI stop-loss warnings. ' +
      'Planned-date exit alerts and your own stop orders in Kite still work.',
  },
  DG4: {
    label: 'Stopped',
    hint: 'No AI calls until the weekly budget resets (Thursday 14:00 IST), or a billing error was reported.',
  },
}
export const tierTerm = (v: string | null | undefined): Term => lookup(TIER, v)

// ------------------------------------------------------------------------------------ risk checks

const VERDICT: Table = {
  approve: { label: 'Approved', hint: 'Passed every risk check.' },
  shrink: { label: 'Approved, smaller', hint: 'Passed after the quantity was reduced to fit a limit.' },
  reject: { label: 'Blocked', hint: 'Failed a risk check — the reasons say which one.' },
  owner_approval_required: { label: 'Needs your OK', hint: 'Allowed only with your explicit approval.' },
}
export function verdictTerm(v: string | null | undefined): Term {
  if (v === null || v === undefined || v === '') {
    return { label: 'Waiting for check', hint: 'The risk check has not answered yet.' }
  }
  return lookup(VERDICT, v)
}

/** Appendix A of the design spec: a reason is only cited when its rule FAILED, so each reads as the problem. */
const RULE: Table = {
  mode_risk_state: { label: "Engine mode or risk state doesn't allow new trades" },
  kill_state: { label: 'Kill switch is on' },
  proposal_stale: { label: 'The proposal was too old when it was checked' },
  levels_coherent: { label: "Entry, stop-loss and target don't fit together" },
  analyst_confidence_min: { label: 'Analyst confidence below the minimum' },
  trade_window: { label: 'Outside your trading hours' },
  no_trade_windows: { label: 'Inside a no-trade period' },
  min_residual_window: { label: 'Not enough time left today for an intraday trade' },
  instrument_eligible: { label: "Stock isn't eligible to trade" },
  surveillance: { label: 'Stock is under exchange surveillance' },
  capital_cap: { label: 'Would go over the capital limit' },
  per_trade_risk: { label: 'Risk per trade over the limit' },
  daily_loss_soft: { label: "Today's loss limit reached" },
  daily_loss_hard: { label: "Today's hard loss limit reached" },
  consecutive_losses: { label: 'Too many losses in a row today' },
  max_new_trades_day: { label: 'Daily new-trade limit reached' },
  max_open_positions: { label: 'Too many open positions (waiting recommendations count too)' },
  per_stock_exposure: { label: 'Too much in one stock' },
  per_sector_exposure: { label: 'Too much in one sector' },
  co_movement_cap: { label: 'Too many positions that move together' },
  max_leverage: { label: 'Leverage over the limit' },
  stale_data_guard: { label: 'Price data is out of date' },
  warmup_ready: { label: 'Not enough price history loaded yet' },
  regime_data_ready: { label: 'Market index data (NIFTY 50 / India VIX) missing' },
  clock_skew: { label: 'Computer clock is out of sync' },
  entry_sanity_band: { label: 'Entry price too far from the live price' },
  circuit_proximity: { label: 'Stock is close to its circuit limit' },
  margin_buffer: { label: 'Not enough margin' },
  order_rate: { label: 'Broker order-rate limit reached' },
  order_modifications: { label: 'Too many changes to one order' },
  min_viable_size: { label: "Expected gain doesn't beat costs, or the strategy has no validated edge yet" },
  weekly_drawdown: { label: 'Weekly loss limit reached' },
  equity_floor_rung: { label: 'Account fell below an equity floor' },
  cumulative_floor: { label: 'Total loss floor reached' },
}
export const ruleTerm = (ruleId: string): Term => lookup(RULE, ruleId)

/** Non-rule prefixes the gate also writes into a verdict's reasons. */
const REASON_PREFIX: Table = {
  shrink: { label: 'Quantity reduced' },
}

export interface Reason extends Term {
  /** The engine's own value/limit text, shown unchanged (never re-formatted). */
  detail?: string
}

/**
 * One risk-check reason in words. The gate writes a failed rule as "<rule_id>: <value> (limit <limit>)"
 * (gate.py), plus lines like "shrink: qty 10 -> 3 (…)": the prefix becomes the plain label and the rest
 * is kept verbatim — humanising it would strip the minus signs and decimal points from the numbers.
 * A bare rule ID (older rows) maps to its label; any other sentence is shown as sent.
 */
export function reasonTerm(reason: string): Reason {
  const text = reason.trim()
  const m = /^([a-z][a-z0-9_]*):\s*(.+)$/.exec(text)
  if (m) {
    const [, id, detail] = m
    const own = (t: Table) => (Object.prototype.hasOwnProperty.call(t, id) ? t[id] : undefined)
    const hit = own(RULE) ?? own(REASON_PREFIX)
    return { label: hit ? hit.label : humanize(id), detail, term: id }
  }
  if (/^[a-z][a-z0-9_]*$/.test(text)) return ruleTerm(text)
  return { label: text }
}

const PROPOSAL_ACTION: Table = {
  enter: { label: 'Open' },
  exit: { label: 'Exit' },
  'modify-stop': { label: 'Move stop-loss' },
  'modify-target': { label: 'Move target' },
  cancel: { label: 'Cancel order' },
}
export const proposalActionTerm = (v: string | null | undefined): Term => lookup(PROPOSAL_ACTION, v)

// ------------------------------------------------------------------------------------ recommendations

const ACTION: Table = {
  taken: { label: 'You took it', hint: 'You confirmed on Telegram that you placed it.' },
  expired: { label: 'Expired', hint: 'Its validity ran out with no action recorded.' },
  dismissed: { label: 'You skipped it', hint: 'You chose not to take it.' },
  closed: { label: 'Closed', hint: 'You reported the position closed.' },
}
export function actionTerm(v: string | null | undefined): Term {
  if (v === null || v === undefined || v === '') {
    return {
      label: 'Waiting for you',
      hint: 'No action recorded yet. Reply on Telegram when you act: /taken if you placed it, /veto if you are skipping it.',
    }
  }
  return lookup(ACTION, v)
}

const KIND: Table = {
  entry: { label: 'New position' },
  enter: { label: 'New position' },
  exit: { label: 'Exit' },
  adjust: { label: 'Adjust', hint: "A change to an open position's stop-loss or target." },
}
export const kindTerm = (v: string | null | undefined): Term => lookup(KIND, v)

const STYLE: Table = {
  swing: { label: 'Swing', hint: 'Held for days to weeks.' },
  intraday: { label: 'Intraday', hint: 'Opened and closed the same day.' },
  position: { label: 'Position', hint: 'Held for weeks to months.' },
}
export const styleTerm = (v: string | null | undefined): Term => lookup(STYLE, v)

const PRODUCT: Table = {
  CNC: { label: 'Delivery', hint: 'CNC: shares held overnight in your demat account.' },
  MIS: { label: 'Intraday', hint: 'MIS: intraday margin product, squared off before the market closes.' },
}
export const productTerm = (v: string | null | undefined): Term => lookup(PRODUCT, v)

// ------------------------------------------------------------------------------------ positions

const ORIGIN: Table = {
  platform: { label: 'Engine', hint: 'Opened by the engine itself.' },
  external: { label: 'Outside engine', hint: 'Found in your Kite account; not opened through the engine.' },
  recommended: { label: 'Recommendation', hint: 'You opened it from a recommendation.' },
}
export const originTerm = (v: string | null | undefined): Term => lookup(ORIGIN, v)

const PROTECTION: Table = {
  PROTECTED: { label: 'Protected', hint: 'A stop-loss order is in place.' },
  PROTECTION_PENDING: { label: 'Being protected', hint: 'The stop-loss order is being placed.' },
  PROTECTION_FAILED: {
    label: 'Protection failed',
    hint: "The simulated stop-loss couldn't be kept, so the autopilot exits the position at market.",
  },
  TIME_EXIT_ONLY: {
    label: 'No stop (test)',
    hint: 'Paper exit test: this position has no stop-loss or target by design. It sells near the close of its planned exit day.',
  },
}
export const protectionTerm = (v: string | null | undefined): Term => lookup(PROTECTION, v)

const POSITION_STATE: Table = {
  OPEN: { label: 'Open' },
  PENDING_EXIT: { label: 'Exiting', hint: 'An exit is in progress.' },
  CLOSED: { label: 'Closed' },
}
export const positionStateTerm = (v: string | null | undefined): Term => lookup(POSITION_STATE, v)

// ------------------------------------------------------------------------------------ paper

const HALT: Table = {
  daily_loss_soft: {
    label: 'Daily loss limit (soft)',
    hint: "Today's paper loss passed the soft limit: new paper trades pause until the next session.",
  },
  daily_loss_hard: {
    label: 'Daily loss limit (hard)',
    hint:
      "Today's paper loss passed the hard limit: no new paper trades until the next session. Open paper positions " +
      'are not closed.',
  },
  floor_weekly_drawdown: {
    label: 'Weekly drawdown limit',
    hint: 'New paper trades are blocked until you reset paper trading (/paper reset on Telegram).',
  },
  floor_equity_floor_rung: {
    label: 'Equity floor',
    hint: 'Every paper position is being closed; stays until /paper reset on Telegram.',
  },
  floor_cumulative_floor: {
    label: 'Total loss floor',
    hint: 'Paper trading is stopped and its positions are closed; stays until /paper reset on Telegram.',
  },
  reset_pending: {
    label: 'Reset in progress',
    hint: 'Closing every paper position; a fresh paper account starts once the book is flat.',
  },
}
export const haltTerm = (cause: string): Term => lookup(HALT, cause)

const RUNG: Table = {
  FROZEN: { label: 'new paper trades paused' },
  CLOSE_ONLY: { label: 'closing only' },
  KILLED: { label: 'stopped' },
}
export const rungTerm = (v: string | null | undefined): Term => lookup(RUNG, v, (x) => humanize(x).toLowerCase())

const OUTCOME: Table = {
  win: { label: 'Win' },
  loss: { label: 'Loss' },
  scratch: { label: 'Flat', hint: 'Exactly zero after costs.' },
  void: {
    label: 'Voided',
    hint:
      "Closed at its last price because the result couldn't be measured fairly (for example engine downtime with no " +
      'price data, or a corporate action). Its P&L stays in paper equity; it is left out of win rates.',
  },
}
export const outcomeTerm = (v: string | null | undefined): Term => lookup(OUTCOME, v)

const CLOSE_REASON: Table = {
  target: { label: 'Target hit' },
  stop: { label: 'Stop-loss hit' },
  time: { label: 'Time exit', hint: 'The planned exit date was reached.' },
  time_stop: { label: 'Time exit', hint: 'The planned exit date was reached.' },
  risk_flatten: { label: 'Closed by a paper loss limit' },
  gtt_failure_exit: { label: 'Stop order failed; closed at market' },
  void: { label: 'Voided', hint: "Closed at its last price because the result couldn't be measured fairly." },
  square_off: { label: 'Squared off', hint: 'Closed before the market close.' },
  broker_rms: { label: 'Closed by the broker (RMS)' },
  manual_owner: { label: 'Closed by you' },
}
/** `basis` refines a reason the engine shares between causes: every paper flatten closes as
 *  risk_flatten (oms/exits.py), and a /paper reset passes the basis "paper_reset" (paper_risk.py). */
export function closeReasonTerm(v: string | null | undefined, basis?: string | null): Term {
  if (v === 'risk_flatten' && basis === 'paper_reset') return { label: 'Closed by your paper reset', term: v }
  return lookup(CLOSE_REASON, v)
}

// ------------------------------------------------------------------------------------ names

const STRATEGY: Table = {
  hi52: { label: '52-week-high continuation', hint: 'Buys stocks closing near their 52-week high.' },
  brk20: { label: '20-day-high breakout', hint: 'Buys a daily close above the 20-day high.' },
  ins: { label: 'Insider buying', hint: 'Follows net buying by company insiders.' },
  cat: {
    label: 'News catalyst',
    hint: 'Scored news events. Still in validation: the risk check blocks its entries, so it is never recommended yet.',
  },
  cat_reversal: {
    label: 'News reversal',
    hint: "A reversal in a news story's direction. Still in validation: the risk check blocks its entries.",
  },
  mom: { label: '4-week momentum', hint: 'Buys the strongest 4-week performers.' },
  orb: { label: 'Opening-range breakout', hint: "Intraday: trades a break of the opening minutes' range." },
  rsi2: { label: 'RSI-2 mean reversion', hint: 'Buys short dips in stocks above their 200-day average.' },
  trend: { label: '20/50 EMA trend', hint: 'Follows a 20/50-day moving-average cross.' },
  unattributed: { label: 'Not linked to a strategy', hint: "The engine couldn't tie these to a strategy." },
}
/** Strategy IDs are names: an unknown one is shown as-is, not humanised. */
export const strategyName = (id: string | null | undefined): Term => lookup(STRATEGY, id, (x) => x)

const AGENT: Table = {
  intraday_analyst: { label: 'Intraday analyst' },
  news_analyst: { label: 'News analyst' },
  preopen_planner: { label: 'Pre-open planner' },
  nightly_reviewer: { label: 'Nightly reviewer' },
  weekly_researcher: { label: 'Weekly researcher' },
  platform: { label: 'Engine', hint: 'Proposals the engine raises itself, such as the time exit.' },
  swing_analyst: { label: 'Swing analyst' },
  reserve: { label: 'Reserve', hint: 'Spare budget not assigned to an analyst.' },
  sdk_smoke: { label: 'Health check', hint: 'A small test call that checks the AI connection works.' },
}
export const agentName = (id: string | null | undefined): Term => lookup(AGENT, id)

// ------------------------------------------------------------------------------------ news + Telegram

const DIGEST: Table = {
  fresh: { label: 'Up to date', hint: 'The news summary is recent enough to use.' },
  stale: {
    label: 'Stale',
    hint: 'The news summary is too old: news-catalyst ideas (still in validation) pause until it refreshes. No recommendation depends on it.',
  },
  missing: {
    label: 'Missing',
    hint: 'There is no news summary today: news-catalyst ideas (still in validation) pause. No recommendation depends on it.',
  },
}
export function digestTerm(v: string | null | undefined): Term {
  if (v === null || v === undefined || v === '') {
    return { label: 'Unknown', hint: "The news store isn't connected, so freshness is unknown." }
  }
  return lookup(DIGEST, v)
}

const GRADE: Table = {
  originating: {
    label: 'Can start a trade idea',
    hint: 'May start a news-catalyst (cat) trade idea. While cat is in validation, the risk check blocks it.',
  },
  context: { label: 'Background only', hint: 'Shown for context; never starts a trade.' },
}
export const gradeTerm = (v: string | null | undefined): Term => lookup(GRADE, v)

export function deliveryTerm(row: { status: string | null; attempts: number | null }): Term {
  if (row.status === 'delivered') return { label: 'Delivered', term: 'delivered' }
  if (row.status === 'failed') {
    return { label: 'Not delivered', hint: 'Telegram never confirmed delivery and the engine gave up.', term: 'failed' }
  }
  const attempts = row.attempts ?? 0
  return attempts > 0
    ? { label: `Retrying (${attempts})`, hint: 'Telegram has not confirmed delivery yet; the engine keeps retrying.', term: 'pending' }
    : { label: 'Queued', hint: 'Waiting to be sent.', term: 'pending' }
}

const SEVERITY: Table = {
  info: { label: 'Info' },
  warning: { label: 'Warning' },
  critical: { label: 'Critical' },
}
export const severityTerm = (v: string | null | undefined): Term =>
  v === null || v === undefined || v === '' ? { label: 'Info' } : lookup(SEVERITY, v)

const MESSAGE_KIND: Table = {
  login_prompt: { label: 'Kite login needed' },
  limit_breach: { label: 'Risk or safety alert' },
  budget_warning: { label: 'AI budget' },
  paper_alert: { label: 'Paper alert' },
  surveillance_migration: { label: 'Held stock on a surveillance list' },
}
export const messageKindTerm = (v: string | null | undefined): Term => lookup(MESSAGE_KIND, v)

// ------------------------------------------------------------------------------------ field labels

/** Labels for figures shown beside a value; the hint says what the number means. */
export const FIELD = {
  entry: { label: 'Entry', hint: 'The price, or price range, to enter at.' },
  stop: { label: 'Stop-loss', hint: 'Exit if the price reaches this level.' },
  // pipeline.py: an exit carries positions.stop, written once at /taken; a followed adjust never updates it.
  stopOnRecord: {
    label: 'Stop-loss on record',
    hint: 'The stop-loss recorded when you confirmed the trade with /taken. If you have moved it in Kite since, your order in Kite is the one that counts.',
  },
  target: { label: 'Target', hint: 'Take profit at this level.' },
  amount: { label: 'Amount', hint: 'Quantity × entry price.' },
  maxLoss: {
    label: 'Loss at the stop (approx.)',
    // pipeline.py: the adverse end of the zone — its top for a buy, its bottom for a sell.
    hint:
      'From the far end of the entry range (the top for a buy, the bottom for a sell) to the stop-loss, before costs. ' +
      'A gap past the stop overnight can lose more.',
  },
  plannedExit: {
    label: 'Latest exit date',
    hint:
      'If the stop-loss or target has not closed it by then, the engine tells you on Telegram to sell on this date ' +
      '(the entry day counts as session 1). You place the sale.',
  },
  validUntil: { label: 'Valid until', hint: 'After this the recommendation expires.' },
  confidence: { label: 'Analyst confidence', hint: "The AI analyst's own score from 0 to 1. It is not a probability of profit." },
  edge: {
    label: 'Historical edge',
    hint:
      'The net gain per trade the engine registered for this strategy from its research (for hi52, a cautious ' +
      'median-based figure). The risk check uses it to confirm a trade beats its costs. It is not a forecast.',
  },
  stopDistance: {
    label: 'Stop distance',
    hint: "How far the stop-loss is from the entry, in multiples of the stock's average daily range (ATR).",
  },
  fill: { label: 'Your fill', hint: 'The price you reported on Telegram.' },
} satisfies Record<string, Term>

const VETO_REASONS = 'market, price, size, trust, away or other' // telegram.py VETO_REASONS

/**
 * What to send on Telegram for a live recommendation, by kind — the same three footers the engine
 * prints (notify/catalog.py): an entry is recorded with /taken or declined with /veto; an exit is
 * reported with /closed (the bot refuses /taken on it); an adjust places no order, so only /veto.
 */
export function actionHint(
  rec: { kind?: string | null; instrument?: string; qty?: number; side?: string } | null | undefined,
): string {
  const sym = rec?.instrument || 'SYMBOL'
  if (rec?.kind === 'exit') {
    const verb = rec.side === 'BUY' ? 'buy back' : 'sell'
    return `After you ${verb} in Kite, reply /closed ${sym} PRICE (PRICE = your fill). Not exiting? Reply /veto ${sym}.`
  }
  if (rec?.kind === 'adjust') {
    return `Move your existing stop-loss order in Kite to the new level; there's nothing to buy or sell. To decline, reply /veto ${sym}.`
  }
  return `Placed it? Reply /taken ${sym} ${rec?.qty ?? 'QTY'} PRICE (PRICE = your fill). Not taking it? Reply /veto ${sym} REASON (${VETO_REASONS}).`
}

// ------------------------------------------------------------------------------------ fixed copy

export interface DialogCopy {
  title: string
  body: string
  confirm: string
  cancel: string
}

export const COPY = {
  modeOff: {
    title: 'Turn the engine off?',
    body:
      'No new trade ideas. The engine still watches your open positions and sends exit alerts on Telegram. You can ' +
      'switch back any time.',
    confirm: 'Turn off',
    cancel: 'Keep recommending',
  } satisfies DialogCopy,
  autoToRecommend: {
    title: 'Leave Auto?',
    body:
      'The engine goes back to sending you recommendations instead of trading by itself. Exit alerts keep coming. ' +
      'Auto cannot be switched back on from the dashboard.',
    confirm: 'Switch to Recommend',
    cancel: 'Keep Auto',
  } satisfies DialogCopy,
  autoOff: {
    title: 'Turn the engine off?',
    body:
      'No new trade ideas and no auto-trading. The engine still watches open positions and sends exit alerts. Auto ' +
      'cannot be switched back on from the dashboard.',
    confirm: 'Turn off',
    cancel: 'Keep Auto',
  } satisfies DialogCopy,
  rearm: {
    title: 'Re-arm the floor stop?',
    body:
      'The account is back above the floor, so the stop it set can be lifted and new trades are allowed again. ' +
      'A kill switch still needs /kill_reset on Telegram, and the engine mode stays as it is.',
    confirm: 'Re-arm',
    cancel: 'Keep stopped',
  } satisfies DialogCopy,
  paperOn: {
    title: 'Turn on the paper autopilot?',
    body:
      'Every delivery (CNC) entry the analyst proposes is checked against the paper account, and approved ones are ' +
      'simulated — including some the real risk check blocks. No real order is ever placed.',
    confirm: 'Turn on',
    cancel: 'Keep off',
  } satisfies DialogCopy,
  paperOff(waiting: number, prepReady: boolean | null): DialogCopy {
    const orders = `${waiting} waiting paper entry order${waiting === 1 ? '' : 's'}`
    const fate = prepReady ? 'will be cancelled within 30 seconds' : 'will lapse at the next market open'
    return {
      title: 'Turn off the paper autopilot?',
      body: `No new paper trades will start. ${orders} ${fate} (a partly filled order keeps its filled part). Open paper positions keep their exits.`,
      confirm: 'Turn off',
      cancel: 'Keep on',
    }
  },
  paperAbout:
    "Simulates the analyst's delivery (CNC) trades with paper money, without risk. It never places a real order.",
  paperEntriesOpen: 'Delivery (CNC) swing entries the analyst proposes in Recommend mode, inside your trading hours.',
  noAnswer: (what: string) =>
    `No answer from the engine in time. The ${what} may still change; the top bar shows the engine's value after the next refresh.`,
  hoursTitle: 'Trading hours (IST)',
  hoursBody: 'New trades are only proposed inside these hours. Exits are never blocked.',
  hoursBufferLabel: 'Stop intraday entries this many minutes before the end',
  hoursRejected:
    'Not saved — the engine rejected these hours (start must be before end, inside market hours, with room for ' +
    'intraday entries). Your current hours are unchanged.',
  gateTitle: 'Connect to your trading engine',
  gateBody: "Enter the dashboard token. It's kept only in this browser.",
  gateHint:
    "The engine's DASHBOARD_TOKEN (Windows DPAPI store). It is sent as a Bearer header on every request (and in the " +
    'live-updates link).',
} as const
