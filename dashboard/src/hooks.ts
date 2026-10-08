/** Data plumbing for the dashboard: the 10 s REST poll cycle and the `/ws/live` event stream. */
import { useCallback, useEffect, useRef, useState } from 'react'
import { ApiError, apiGet, liveSocketUrl } from './api'
import { ALL_KEYS } from './lib/alerts.ts'
import type {
  ConnState,
  LiveEvent,
  LiveFrame,
  NotificationRow,
  NotificationsResponse,
  ScorecardResponse,
  Snapshot,
} from './types'

export const POLL_INTERVAL_MS = 10_000

/** The read route behind each snapshot key (fetched in parallel, merged per key). */
const ROUTES: Record<(typeof ALL_KEYS)[number], string> = {
  mode: '/mode',
  positions: '/positions',
  decisions: '/decisions',
  recommendations: '/recommendations',
  headroom: '/risk/headroom',
  budget: '/budget',
  tradeWindow: '/config/trade_window',
  watchlist: '/news/watchlist',
  paper: '/paper',
}

/** The notification journal is a day's transcript, not a live tape — a minute of latency on a row
 *  that was already delivered to Telegram costs nothing, so it rides its own slow cycle instead of
 *  adding a tenth route to the 10 s poll. */
export const NOTIFICATIONS_INTERVAL_MS = 60_000
export const SCORECARD_INTERVAL_MS = 60_000

/** Most recent frames kept in the events feed — a LAN console, not an archive (the audit trail is
 *  the engine's own tables). */
const MAX_EVENTS = 200

/** No frame at all (the engine pings every 15 s) for this long = the socket is dead. */
const STALE_SOCKET_MS = 45_000

const EMPTY_SNAPSHOT: Snapshot = {
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
  /** Snapshot keys whose fetch failed in the latest cycle (empty on a clean cycle). */
  failedKeys: string[]
  unauthorized: boolean
  /** ISO-8601 instant of the latest completed cycle; null until the first one completes. */
  lastPollAt: string | null
  /** ISO-8601 instant of the latest cycle in which at least one route answered — what "Updated" means. */
  lastOkAt: string | null
  refresh: () => void
} {
  const [snapshot, setSnapshot] = useState<Snapshot>(EMPTY_SNAPSHOT)
  const [error, setError] = useState<string | null>(null)
  const [failedKeys, setFailedKeys] = useState<string[]>([])
  const [unauthorized, setUnauthorized] = useState(false)
  const [lastPollAt, setLastPollAt] = useState<string | null>(null)
  const [lastOkAt, setLastOkAt] = useState<string | null>(null)
  const [nonce, setNonce] = useState(0)

  const refresh = useCallback(() => setNonce((n) => n + 1), [])

  useEffect(() => {
    if (!token) return
    let cancelled = false
    let inFlight = false

    async function cycle() {
      // One cycle at a time: a slow engine must not stack nine more requests every 10 s.
      if (inFlight) return
      inFlight = true
      let results: PromiseSettledResult<unknown>[]
      try {
        // ALL_KEYS is the one list of polled keys (the alerts module counts failures against it).
        results = await Promise.allSettled(ALL_KEYS.map((key) => apiGet<unknown>(ROUTES[key])))
      } finally {
        inFlight = false
      }
      if (cancelled) return

      const failures: string[] = []
      const failed: string[] = []
      const fresh: Record<string, unknown> = {}
      let denied = false

      // Merge is computed OUTSIDE the state updater: React may invoke an updater more than once, and
      // the failure list must be built exactly once per cycle.
      results.forEach((r, i) => {
        const key = ALL_KEYS[i]
        if (r.status === 'fulfilled') {
          fresh[key] = r.value
        } else {
          const reason = r.reason as unknown
          if (reason instanceof ApiError && reason.status === 401) denied = true
          failed.push(key)
          failures.push(`${key}: ${reason instanceof Error ? reason.message : String(reason)}`)
        }
      })

      const now = new Date().toISOString()
      setSnapshot((prev) => ({ ...prev, ...fresh }))
      setUnauthorized(denied)
      setFailedKeys(failed)
      setError(failures.length ? failures.join(' | ') : null)
      setLastPollAt(now)
      if (failed.length < ALL_KEYS.length) setLastOkAt(now)
    }

    void cycle()
    const timer = window.setInterval(() => void cycle(), POLL_INTERVAL_MS)
    return () => {
      cancelled = true
      window.clearInterval(timer)
    }
  }, [token, nonce])

  return { snapshot, error, failedKeys, unauthorized, lastPollAt, lastOkAt, refresh }
}

/** What one `/notifications` cycle leaves behind for the panel to render. */
export interface NotificationsState {
  d: string | null
  rows: NotificationRow[]
  loading: boolean
  error: string | null
  unauthorized: boolean
  lastRefreshAt: string | null
}

/**
 * Poll `GET /notifications` every 60 s for the engine's CURRENT IST day.
 *
 * `?d=` is deliberately omitted: the engine defaults to today off its own clock, so the day shown is
 * the engine's, never the browser's — a browser-derived date would read as yesterday for anything
 * before 05:30 IST on a UTC-set machine.
 *
 * A failed cycle KEEPS the rows already on screen (same posture as `usePoll`'s per-key merge): a
 * blipped request must not blank the day's transcript. A 401 is surfaced separately — that is a token
 * problem the owner has to act on, not a transient.
 */
export function useNotifications(token: string): NotificationsState {
  const [state, setState] = useState<NotificationsState>({
    d: null,
    rows: [],
    loading: true,
    error: null,
    unauthorized: false,
    lastRefreshAt: null,
  })

  useEffect(() => {
    if (!token) return
    let cancelled = false

    async function cycle() {
      const at = () => new Date().toISOString() // shown in IST by the view, never the browser's zone
      try {
        const res = await apiGet<NotificationsResponse>('/notifications')
        if (cancelled) return
        setState({
          d: res.d,
          rows: res.rows ?? [],
          loading: false,
          error: null,
          unauthorized: false,
          lastRefreshAt: at(),
        })
      } catch (e) {
        if (cancelled) return
        setState((prev) => ({
          ...prev,
          loading: false,
          error: e instanceof Error ? e.message : String(e),
          unauthorized: e instanceof ApiError && e.status === 401,
          lastRefreshAt: at(),
        }))
      }
    }

    void cycle()
    const timer = window.setInterval(() => void cycle(), NOTIFICATIONS_INTERVAL_MS)
    return () => {
      cancelled = true
      window.clearInterval(timer)
    }
  }, [token])

  return state
}

/** `GET /scorecard` every 60 s — hindsight aggregates move only when an outcome row is written. A
 *  failed cycle keeps the data already on screen. */
export interface ScorecardState {
  data: ScorecardResponse | null
  error: string | null
  unauthorized: boolean
}

export function useScorecard(token: string): ScorecardState {
  const [state, setState] = useState<ScorecardState>({ data: null, error: null, unauthorized: false })

  useEffect(() => {
    if (!token) return
    let cancelled = false

    async function cycle() {
      try {
        const data = await apiGet<ScorecardResponse>('/scorecard')
        if (!cancelled) setState({ data, error: null, unauthorized: false })
      } catch (e) {
        if (cancelled) return
        setState((prev) => ({
          ...prev,
          error: e instanceof Error ? e.message : String(e),
          unauthorized: e instanceof ApiError && e.status === 401,
        }))
      }
    }

    void cycle()
    const timer = window.setInterval(() => void cycle(), SCORECARD_INTERVAL_MS)
    return () => {
      cancelled = true
      window.clearInterval(timer)
    }
  }, [token])

  return state
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
    let lastFrameAt = Date.now()

    function open() {
      if (closed) return
      setConn('connecting')
      socket = new WebSocket(liveSocketUrl())
      socket.onopen = () => {
        lastFrameAt = Date.now()
        setConn('open')
      }
      socket.onmessage = (msg) => {
        lastFrameAt = Date.now() // the ping counts here: it is the liveness signal
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

    // A half-open socket (engine host asleep, network gone) never fires `close`: three missed 15 s
    // pings mean it is dead — say so at once and start over, instead of showing "live" forever.
    const watchdog = window.setInterval(() => {
      if (socket && socket.readyState === WebSocket.OPEN && Date.now() - lastFrameAt > STALE_SOCKET_MS) {
        const dead = socket
        socket = null
        dead.onclose = null
        dead.onmessage = null
        dead.onerror = null
        try {
          dead.close()
        } catch {
          /* already gone */
        }
        setConn('closed')
        if (!closed) retry = window.setTimeout(open, 5000)
      }
    }, 5000)

    open()
    return () => {
      closed = true
      window.clearTimeout(retry)
      window.clearInterval(watchdog)
      socket?.close()
    }
  }, [token])

  return { events, conn }
}
