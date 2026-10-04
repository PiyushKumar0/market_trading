/**
 * Panel 9 — the day's owner notifications (`GET /notifications`, WO-24d), the same journal rows every
 * `TelegramBot.send` writes before it touches the wire. Read as a TRANSCRIPT: chronological ascending,
 * newest at the bottom, history above — the engine returns `created_at ASC` and this panel never
 * re-sorts, so what is shown is the order the engine actually raised them in.
 *
 * `status` is delivery truth off the retry outbox, not intent: a `pending` row with attempts is a
 * message Telegram has NOT acknowledged yet, and a `failed` row carries the transport's own error.
 * That distinction is the whole point of the panel — a silent Telegram is exactly when the owner
 * needs to see what never arrived.
 */
import { useEffect, useRef } from 'react'
import type { NotificationRow } from '../types'
import type { NotificationsState } from '../hooks'
import { Chip, Empty, Panel, hhmmss } from './ui'
import type { Tone } from './ui'

/** Distance from the bottom (px) still counted as "reading the newest" — a click on a `<details>`
 *  toggle shifts the scroll by a few px and must not un-pin the transcript. */
const PIN_SLACK_PX = 24

function severityTone(severity: string | null): Tone {
  switch (severity) {
    case 'critical':
      return 'bad'
    case 'warning':
      return 'warn'
    default:
      return 'neutral' // info, and anything a later engine build invents
  }
}

function statusTone(status: string | null): Tone {
  switch (status) {
    case 'delivered':
      return 'ok'
    case 'failed':
      return 'bad'
    default:
      return 'info' // pending — queued for the retry drainer
  }
}

/** "delivered" / "retrying ×3" / "failed" — attempts are only interesting while delivery is still
 *  outstanding, so they ride on the pending badge. */
function statusLabel(row: NotificationRow): string {
  if (row.status === 'delivered') return 'delivered'
  if (row.status === 'failed') return 'failed'
  const attempts = row.attempts ?? 0
  return attempts > 0 ? `retrying ×${attempts}` : 'pending'
}

/** Hover text for the delivery badge: the failure reason where there is one, otherwise the delivery
 *  stamp — the same facts the expanded row shows, for a reader who does not want to expand. */
function statusHint(row: NotificationRow): string {
  if (row.status === 'delivered') return `delivered ${row.delivered_at ?? '—'} on attempt ${row.attempts ?? 0}`
  if (row.last_error) return row.last_error
  return row.status === 'failed' ? 'expired without delivery' : 'queued for retry'
}

function NotificationLine({ row }: { row: NotificationRow }) {
  const body = row.body ?? ''
  // A failed row's error is worth expanding even when the message itself carried no body.
  const failure = row.status !== 'delivered' && row.last_error ? row.last_error : null
  const expandable = body.length > 0 || failure !== null

  return (
    <div className="nt">
      <span className="at">{hhmmss(row.created_at)}</span>
      <Chip v={row.severity ?? 'info'} tone={severityTone(row.severity)} />
      <span className="title">{row.title || '(no title)'}</span>
      <span className="spacer" />
      <span className="kind">{row.kind ?? '—'}</span>
      <Chip v={statusLabel(row)} tone={statusTone(row.status)} title={statusHint(row)} />
      {expandable ? (
        <details>
          <summary>{body.length > 0 ? `body (${body.length} chars)` : 'last error'}</summary>
          {body.length > 0 ? <pre className="body">{body}</pre> : null}
          {failure ? <pre className="body err">{failure}</pre> : null}
        </details>
      ) : null}
    </div>
  )
}

export function NotificationsPanel({ d, rows, loading, error, unauthorized, lastRefreshAt }: NotificationsState) {
  const bodyRef = useRef<HTMLDivElement>(null)
  // Chat-log posture: follow the newest row, but STOP following the moment the reader scrolls up into
  // the history — a 60 s refresh must never yank them back down mid-read.
  const pinned = useRef(true)

  useEffect(() => {
    const el = bodyRef.current
    if (!el) return
    const onScroll = () => {
      pinned.current = el.scrollHeight - el.scrollTop - el.clientHeight <= PIN_SLACK_PX
    }
    el.addEventListener('scroll', onScroll, { passive: true })
    return () => el.removeEventListener('scroll', onScroll)
  }, [])

  useEffect(() => {
    const el = bodyRef.current
    if (el && pinned.current) el.scrollTop = el.scrollHeight
  }, [rows])

  const aside = `${d ?? '—'} · ${rows.length} row${rows.length === 1 ? '' : 's'}${
    lastRefreshAt ? ` · refreshed ${lastRefreshAt}` : ''
  }`

  return (
    <Panel title="Notifications" aside={aside} wide bodyRef={bodyRef}>
      {unauthorized ? (
        <div className="err">token rejected (401) — sign out (header, top right) and re-enter the dashboard token</div>
      ) : error ? (
        <div className="err">notifications unavailable: {error}</div>
      ) : null}

      {loading && rows.length === 0 ? (
        <div className="dim">loading…</div>
      ) : rows.length === 0 && !unauthorized ? (
        <Empty what={`notifications journalled on ${d ?? 'today'}`} />
      ) : (
        <div className="notifs">
          {rows.map((row) => (
            <NotificationLine key={row.notification_id} row={row} />
          ))}
        </div>
      )}
    </Panel>
  )
}
