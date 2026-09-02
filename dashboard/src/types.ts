/**
 * Wire types for the engine's dashboard API (`src/engine/api/app.py`, §3.2.11 / O8 / R8).
 *
 * These mirror the ACTUAL route return shapes, not the Python domain models. Two conventions matter:
 *  - every money / price / level value crosses as a STRING (§8.1 decimal-as-string) — never do
 *    arithmetic on them here, the dashboard is a read-only view;
 *  - every route degrades to a stub shape when its collaborator is unwired, so the optional/empty
 *    variants below are normal operating states, not errors.
 */

// --------------------------------------------------------------------------- GET /mode
export interface ModeResponse {
  mode: string | null // OFF | RECOMMEND | AUTO
  routing: string | null // paper | live (AUTO only)
  risk_state: string | null // NORMAL | FROZEN | CLOSE_ONLY | KILLED
}

// --------------------------------------------------------------------------- GET /positions
/** `SELECT * FROM positions` (migration 0001), all origins, OPEN first. */
export interface PositionRow {
  position_id: string
  symbol: string
  side: string | null
  style: string | null
  product: string | null
  qty: number | null
  avg_entry: string | null
  stop: string | null
  target: string | null
  state: string | null
  protection_state: string | null
  is_paper: number
  origin: string // platform | external | recommended
  close_reason: string | null
  opened_at: string | null
  closed_at: string | null
  realized_pnl: string | null
  costs: string | null
}

export interface PositionsResponse {
  positions: PositionRow[]
  as_of: string | null
}

// --------------------------------------------------------------------------- GET /decisions
/** The stored `ActionProposal` union (engine.core.contracts): the identifying field depends on
 *  `action` — enter carries `tradingsymbol`, exit/modify-* a `position_id`, cancel an `order_id`. */
export interface ProposalPayload {
  action?: string
  agent_id?: string
  thesis?: string
  confidence?: number
  tradingsymbol?: string
  position_id?: string
  order_id?: string
  side?: string
  style?: string
  entry_type?: string
  entry_price?: string | null
  stop_price?: string
  target_price?: string | null
  new_stop?: string
  new_target?: string | null
  quantity?: number
  strategy_id?: string
  [key: string]: unknown
}

export interface DecisionRow {
  proposal_id: string
  agent_id: string | null
  action: string | null
  proposal: ProposalPayload
  /** Resolved server-side: the tradingsymbol for enter, the position's symbol for exit / modify-* /
   *  cancel (via position_id, or order_id → its position); the raw id when that row is gone. */
  subject: string | null
  created_at: string | null
  verdict_id: string | null
  verdict: string | null // approve | shrink | reject | owner_approval_required
  reasons: string[]
  evaluated_at: string | null
}

export interface DecisionsResponse {
  decisions: DecisionRow[]
}

// --------------------------------------------------------------------------- GET /recommendations
export interface GateCheck {
  rule_id: string
  passed: boolean
  value: string
  limit: string
  headroom: string
}

export interface GatePayload {
  verdict?: string
  original_qty?: number | null
  approved_qty?: number | null
  checks?: GateCheck[]
  reasons?: string[]
  mode?: string
  risk_state?: string
  degrade_tier?: string
}

/** The delivered `Recommendation` (§3.6) as stored in `recommendations.payload`. */
export interface RecommendationPayload {
  rec_id?: string
  created_at?: string
  valid_until?: string
  kind?: string
  instrument?: string
  side?: string
  style?: string
  product?: string
  entry_zone?: [string, string]
  stop?: string
  targets?: string[]
  qty?: number
  notional?: string
  thesis?: string
  confidence?: number
  short_flag_higher_tail_risk?: boolean
  gate?: GatePayload
  manual_checklist?: string[]
}

export interface RecommendationRow {
  rec_id: string
  recommendation: RecommendationPayload | null
  delivered_at: string | null
  human_action: string | null // taken | expired | dismissed | closed
  human_fill_price: string | null
  outcome: unknown | null
}

export interface RecommendationsResponse {
  recommendations: RecommendationRow[]
}

// --------------------------------------------------------------------------- GET /risk/headroom
export interface HeadroomCaps {
  max_deployed_capital_inr: string
  max_open_positions_total: number
  max_open_positions_mis: number
  max_open_positions_cnc: number
  consecutive_losses_max_per_session: number
  max_new_trades_day: number
}

/** `{}` when `exposure` is unwired — every field is therefore optional. */
export interface Headroom {
  equity?: string
  day_mtm?: string
  open_positions?: { total: number; mis: number; cnc: number }
  consecutive_losses?: number
  deployed_capital?: string
  caps?: HeadroomCaps
}

export interface HeadroomResponse {
  headroom: Headroom
}

// --------------------------------------------------------------------------- GET /budget
/** `{}` when `governor` is unwired. Spend/allocation are USD strings (§5.6). */
export interface BudgetBody {
  month_spend_usd?: string
  per_agent_spend_usd?: Record<string, string>
  allocations_usd?: Record<string, string>
}

export interface BudgetResponse {
  budget: BudgetBody
  degrade_tier: string | null // DG0..DG4
}

// --------------------------------------------------------------------------- GET/POST /config/trade_window
export interface TradeWindow {
  start: string // "HH:MM" IST
  end: string // "HH:MM" IST
  squareoff_buffer_min: number | null
}

export interface TradeWindowResponse {
  trade_window: TradeWindow | null
}

// --------------------------------------------------------------------------- GET /news/watchlist
/** One `catalyst_watchlist` row (§2.7 step 5(ii)); levels are DETERMINISTIC §6.1 values as strings. */
export interface WatchlistRow {
  entry_id: string
  d: string
  symbol: string
  grade: string // originating | context
  direction: string | null
  event_type: string | null
  cluster_refs: string[] | null
  materiality: number | null
  source_domain_count: number | null
  event_age_h: number | null
  event_age_sessions: number | null
  confirm_trigger: string | null
  invalidation: string | null
  stop_band_low: string | null
  stop_band_high: string | null
  target_band_low: string | null
  target_band_high: string | null
  expires_at: string | null
}

/** `status` is null only when the market store is unwired — an UNKNOWN freshness is never one of
 *  the three real `digest_status` values. */
export interface DigestFreshness {
  as_of: string | null
  age_h: number | null
  stale_max_h: number | null
  status: 'fresh' | 'stale' | 'missing' | null
}

export interface WatchlistResponse {
  d: string | null
  watchlist: WatchlistRow[]
  digest: DigestFreshness
}

// --------------------------------------------------------------------------- GET /notifications
/** One row of the `notifications` journal (migration 0011) — every `TelegramBot.send` writes one
 *  BEFORE the wire attempt, so `status` is the delivery truth, not an intent. */
export interface NotificationRow {
  notification_id: string
  created_at: string | null
  kind: string | null
  severity: 'info' | 'warning' | 'critical' | null
  title: string | null
  body: string | null
  status: 'pending' | 'delivered' | 'failed' | null
  attempts: number | null
  delivered_at: string | null
  last_error: string | null
  last_attempt_at: string | null
}

/** `d` is the IST day the engine resolved (today when the request omits `?d=`); rows are
 *  CHRONOLOGICAL (created_at ASC) and this console never re-sorts them. */
export interface NotificationsResponse {
  d: string | null
  rows: NotificationRow[]
}

// --------------------------------------------------------------------------- WS /ws/live
/** Frames relayed off the bus by `WSHub.broadcast`, plus the `hello` handshake and 15 s `ping`. */
export interface LiveFrame {
  kind: string
  payload: Record<string, unknown>
  at: string | null
}

/** One rendered row of the events feed (a frame plus the local sequence it arrived in). */
export interface LiveEvent extends LiveFrame {
  seq: number
}

// --------------------------------------------------------------------------- everything, one poll
export interface Snapshot {
  mode: ModeResponse | null
  positions: PositionsResponse | null
  decisions: DecisionsResponse | null
  recommendations: RecommendationsResponse | null
  headroom: HeadroomResponse | null
  budget: BudgetResponse | null
  tradeWindow: TradeWindowResponse | null
  watchlist: WatchlistResponse | null
}

export type ConnState = 'connecting' | 'open' | 'closed'
