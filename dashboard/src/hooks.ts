/** Data plumbing for the dashboard: the 10 s REST poll cycle and the `/ws/live` event stream. */
import { useCallback, useEffect, useRef, useState } from 'react'
import { ApiError, apiGet, liveSocketUrl } from './api'
import type {
  BudgetResponse,
  ConnState,
  DecisionsResponse,
  HeadroomResponse,
  LiveEvent,
  LiveFrame,
  ModeResponse,
  PositionsResponse,
  RecommendationsResponse,
  Snapshot,
  TradeWindowResponse,
  WatchlistResponse,
} from './types'

export const POLL_INTERVAL_MS = 10_000

/** Most recent frames kept in the events feed — a LAN console, not an archive (the audit trail is
 *  the engine's own tables). */
const MAX_EVENTS = 200

const EMPTY_SNAPSHOT: Snapshot = {
  mode: null,
  positions: null,
  decisions: null,
  recommendations: null,
  headroom: null,
  budget: null,
  tradeWindow: null,
  watchlist: null,
}

/**
 * Poll every read route every 10 s.
 *
 * Routes are fetched in PARALLEL and merged per-key: one failing endpoint (an unwired collaborator,
 * a route added after this build) must not blank the whole console. A 401 is different — it means
 * the token is wrong, so it is surfaced to the caller which drops back to the token gate.
 */
export function usePoll(token: string): {
  snapshot: Snapshot
  error: string | null
  unauthorized: boolean
  lastPollAt: string | null
  refresh: () => void
} {
  const [snapshot, setSnapshot] = useState<Snapshot>(EMPTY_SNAPSHOT)
  const [error, setError] = useState<string | null>(null)
  const [unauthorized, setUnauthorized] = useState(false)
  const [lastPollAt, setLastPollAt] = useState<string | null>(null)
  const [nonce, setNonce] = useState(0)

  const refresh = useCallback(() => setNonce((n) => n + 1), [])

  useEffect(() => {
    if (!token) return
    let cancelled = false

    async function cycle() {
      const results = await Promise.allSettled([
        apiGet<ModeResponse>('/mode'),
        apiGet<PositionsResponse>('/positions'),
        apiGet<DecisionsResponse>('/decisions'),
        apiGet<RecommendationsResponse>('/recommendations'),
        apiGet<HeadroomResponse>('/risk/headroom'),
        apiGet<BudgetResponse>('/budget'),
        apiGet<TradeWindowResponse>('/config/trade_window'),
        apiGet<WatchlistResponse>('/news/watchlist'),
      ])
      if (cancelled) return

      const keys: (keyof Snapshot)[] = [
        'mode',
        'positions',
        'decisions',
        'recommendations',
        'headroom',
        'budget',
        'tradeWindow',
        'watchlist',
      ]
      const failures: string[] = []
      const fresh: Record<string, unknown> = {}
      let denied = false

      // Merge is computed OUTSIDE the state updater: React may invoke an updater more than once, and
      // the failure list must be built exactly once per cycle.
      results.forEach((r, i) => {
        const key = keys[i]
        if (r.status === 'fulfilled') {
          fresh[key] = r.value
        } else {
          const reason = r.reason as unknown
          if (reason instanceof ApiError && reason.status === 401) denied = true
          failures.push(`${key}: ${reason instanceof Error ? reason.message : String(reason)}`)
        }
      })

      setSnapshot((prev) => ({ ...prev, ...fresh }))
      setUnauthorized(denied)
      setError(failures.length ? failures.join(' | ') : null)
      setLastPollAt(new Date().toTimeString().slice(0, 8))
    }

    void cycle()
    const timer = window.setInterval(() => void cycle(), POLL_INTERVAL_MS)
    return () => {
      cancelled = true
      window.clearInterval(timer)
    }
  }, [token, nonce])

  return { snapshot, error, unauthorized, lastPollAt, refresh }
}

/**
 * Subscribe to `/ws/live` and append every relayed state-change frame to the events feed.
 *
 * The engine's own 15 s `ping` keepalive is dropped (it carries no state); `hello` is kept so the
 * feed shows the stream came up. A closed socket retries after 5 s — the engine restarts daily and
 * the console is expected to survive that unattended.
 */
export function useLiveEvents(token: string): { events: LiveEvent[]; conn: ConnState } {
  const [events, setEvents] = useState<LiveEvent[]>([])
  const [conn, setConn] = useState<ConnState>('closed')
  const seq = useRef(0)

  useEffect(() => {
    if (!token) return
    let closed = false
    let socket: WebSocket | null = null
    let retry = 0

    function open() {
      if (closed) return
      setConn('connecting')
      socket = new WebSocket(liveSocketUrl())
      socket.onopen = () => setConn('open')
      socket.onmessage = (msg) => {
        let frame: LiveFrame
        try {
          frame = JSON.parse(String(msg.data)) as LiveFrame
        } catch {
          return
        }
        if (frame.kind === 'ping') return
        seq.current += 1
        const event: LiveEvent = { ...frame, seq: seq.current }
        setEvents((prev) => [event, ...prev].slice(0, MAX_EVENTS))
      }
      socket.onclose = () => {
        setConn('closed')
        if (!closed) retry = window.setTimeout(open, 5000)
      }
      socket.onerror = () => socket?.close()
    }

    open()
    return () => {
      closed = true
      window.clearTimeout(retry)
      socket?.close()
    }
  }, [token])

  return { events, conn }
}
