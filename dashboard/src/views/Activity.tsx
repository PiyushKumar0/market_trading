/**
 * Activity: what the engine decided and said today, in the order the owner asked for it: the risk
 * checks, then Telegram, then the news and live-update reference material. The jump buttons are real
 * buttons, not hash links: the app keeps the open tab in `location.hash`, so a link would switch tabs.
 */
import type { NotificationsState } from '../hooks'
import type { ConnState, DecisionsResponse, LiveEvent, WatchlistResponse } from '../types'
import { LiveUpdates } from './parts/LiveUpdates'
import { NewsWatchlist } from './parts/NewsWatchlist'
import { RiskChecks } from './parts/RiskChecks'
import { TelegramMessages } from './parts/TelegramMessages'
import './activity.css'

export interface ActivityProps {
  decisions: DecisionsResponse | null
  notifications: NotificationsState
  events: LiveEvent[]
  conn: ConnState
  watchlist: WatchlistResponse | null
}

const JUMPS: { id: string; label: string }[] = [
  { id: 'act-risk', label: 'Risk checks' },
  { id: 'act-telegram', label: 'Telegram messages' },
  { id: 'act-news', label: 'News watchlist' },
  { id: 'act-live', label: 'Live updates' },
]

function jump(id: string) {
  const el = document.getElementById(id)
  if (!el) return
  const reduced = window.matchMedia('(prefers-reduced-motion: reduce)').matches
  el.scrollIntoView({ block: 'start', behavior: reduced ? 'auto' : 'smooth' })
  el.focus({ preventScroll: true })
}

export function Activity(props: ActivityProps) {
  return (
    <>
      <h1 className="view-title">Activity</h1>
      <nav className="act-jumps" aria-label="On this page">
        {JUMPS.map((j) => (
          <button key={j.id} type="button" className="btn btn-quiet" onClick={() => jump(j.id)}>
            {j.label}
          </button>
        ))}
      </nav>
      <div className="act-stack">
        <RiskChecks decisions={props.decisions} />
        <TelegramMessages {...props.notifications} />
        <NewsWatchlist data={props.watchlist} />
        <LiveUpdates events={props.events} conn={props.conn} />
      </div>
    </>
  )
}
