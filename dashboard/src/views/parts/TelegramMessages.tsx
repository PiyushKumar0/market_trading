/**
 * Telegram messages: the day's notification journal (`GET /notifications`), the same rows every
 * `TelegramBot.send` writes before it touches the wire. Read as a TRANSCRIPT: chronological as served,
 * newest at the bottom; this view never re-sorts, so the order is the one the engine raised them in.
 *
 * `status` is delivery truth off the retry outbox, not intent: a message Telegram has not acknowledged
 * is not "sent", and a failed one carries the transport's own error. A silent Telegram is exactly when
 * the owner needs to see what never arrived.
 */
import { useEffect, useRef } from 'react'
import { deliveryTerm, messageKindTerm, severityTerm } from '../../copy/glossary.ts'
import { dayLabel, istStamp, istTime } from '../../lib/format.ts'
import type { NotificationsState } from '../../hooks'
import type { NotificationRow } from '../../types'
import { Card } from '../../ui/Card'
import { Hint, Labelled } from '../../ui/Hint'
import { Icon } from '../../ui/Icon'
import { Pill } from '../../ui/Pill'
import type { Tone } from '../../ui/Pill'

/** Distance from the bottom (px) still counted as "reading the newest": a click on a `<details>` toggle
 *  shifts the scroll by a few px and must not un-pin the transcript. */
const PIN_SLACK_PX = 24

function severityTone(severity: string | null): Tone {
  switch (severity) {
    case 'critical':
      return 'crit'
    case 'warning':
      return 'warn'
    default:
      return 'neutral' // info, and anything a later engine build invents
  }
}

function deliveryTone(status: string | null): Tone {
  switch (status) {
    case 'delivered':
      return 'good'
    case 'failed':
      return 'crit'
    default:
      return 'note' // pending: queued for the retry drainer
  }
}

function MessageRow({ row }: { row: NotificationRow }) {
  const body = row.body ?? ''
  // A failed row's error is worth expanding even when the message itself carried no body.
  const failure = row.status !== 'delivered' && row.last_error ? row.last_error : null
  const delivery = deliveryTerm(row)
  const hint =
    row.status === 'delivered'
      ? `Delivered ${istStamp(row.delivered_at)}, attempt ${row.attempts ?? 0}`
      : (row.last_error ?? delivery.hint)

  return (
    <li className="act-msg">
      <div className="act-msg-head">
        <span className="act-msg-time">{istTime(row.created_at)}</span>
        <Pill tone={severityTone(row.severity)}>{severityTerm(row.severity).label}</Pill>
        <span className="act-msg-title">{row.title || '(no title)'}</span>
        <Labelled t={messageKindTerm(row.kind)} className="muted" />
        <span className="labelled">
          <Pill tone={deliveryTone(row.status)}>{delivery.label}</Pill>
          <Hint label="delivery" term={delivery.term}>
            {hint}
          </Hint>
        </span>
      </div>
      {body.length > 0 || failure !== null ? (
        <details className="disclosure">
          <summary>
            <Icon name="chevron-right" size={14} />
            {body.length > 0 ? 'Show message' : 'Show error'}
          </summary>
          {body.length > 0 ? <pre className="act-pre">{body}</pre> : null}
          {failure ? <pre className="act-pre act-pre-err">{failure}</pre> : null}
        </details>
      ) : null}
    </li>
  )
}

export function TelegramMessages({ d, rows, loading, error, unauthorized, lastRefreshAt }: NotificationsState) {
  const boxRef = useRef<HTMLDivElement>(null)
  // Chat-log posture: follow the newest row, but STOP following the moment the reader scrolls up into
  // the history, so a 60 s refresh never yanks them back down mid-read.
  const pinned = useRef(true)

  useEffect(() => {
    const el = boxRef.current
    if (!el) return
    const onScroll = () => {
      pinned.current = el.scrollHeight - el.scrollTop - el.clientHeight <= PIN_SLACK_PX
    }
    el.addEventListener('scroll', onScroll, { passive: true })
    return () => el.removeEventListener('scroll', onScroll)
  }, [])

  useEffect(() => {
    const el = boxRef.current
    if (el && pinned.current) el.scrollTop = el.scrollHeight
  }, [rows])

  const meta = [
    d ? dayLabel(d) : null,
    `${rows.length} ${rows.length === 1 ? 'message' : 'messages'}`,
    lastRefreshAt ? `checked ${istTime(lastRefreshAt, { seconds: true })}` : null,
  ]
    .filter(Boolean)
    .join(', ')

  return (
    <Card id="act-telegram" title="Telegram messages" meta={meta}>
      <p className="muted act-lede">Every message the engine sent you today, and whether Telegram confirmed it.</p>
      {unauthorized ? (
        <p className="act-error">Your dashboard token was rejected. Sign out (top right) and enter the current token.</p>
      ) : error ? (
        <p className="act-error">Telegram messages couldn't load: {error}</p>
      ) : null}
      {loading && rows.length === 0 ? (
        <p className="empty">Loading…</p>
      ) : rows.length === 0 && !unauthorized ? (
        <p className="empty">No Telegram messages today.</p>
      ) : null}
      {/* Always mounted so the scroll listener attaches once; it collapses to nothing while empty. */}
      <div ref={boxRef} className="act-transcript" role="region" aria-label="Telegram messages, oldest first" tabIndex={0}>
        <ul className="act-msg-list">
          {rows.map((row) => (
            <MessageRow key={row.notification_id} row={row} />
          ))}
        </ul>
      </div>
    </Card>
  )
}
