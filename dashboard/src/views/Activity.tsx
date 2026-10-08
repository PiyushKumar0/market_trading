/** Activity tab — STUB, replaced in Task 9. Props are final. */
import type { NotificationsState } from '../hooks'
import type { ConnState, DecisionsResponse, LiveEvent, WatchlistResponse } from '../types'
import { EmptyState } from '../ui/Card'

export interface ActivityProps {
  decisions: DecisionsResponse | null
  notifications: NotificationsState
  events: LiveEvent[]
  conn: ConnState
  watchlist: WatchlistResponse | null
}

export function Activity(props: ActivityProps) {
  void props
  return (
    <>
      <h1 className="view-title">Activity</h1>
      <EmptyState>This section is being rebuilt.</EmptyState>
    </>
  )
}
