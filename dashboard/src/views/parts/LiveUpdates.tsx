/**
 * Live updates: every `/ws/live` frame the engine relays off the bus (mode, risk state, kill switch,
 * trading hours, AI budget), newest first as received. The 15 s `ping` keepalive is filtered out by the
 * socket hook. Payload shapes are the engine's event models (risk/events.py, intelligence/events.py);
 * a missing field drops its clause, never throws or prints "undefined". The raw payload stays one
 * click away on every row.
 */
import { modeTerm, riskTerm, tierTerm } from '../../copy/glossary.ts'
import { humanize, istTime } from '../../lib/format.ts'
import type { ConnState, LiveEvent } from '../../types'
import { Card } from '../../ui/Card'
import { Icon } from '../../ui/Icon'
import { Pill } from '../../ui/Pill'

const text = (v: unknown): string | null => (typeof v === 'string' && v.trim() !== '' ? v.trim() : null)

/** " by owner: reason" — either clause is dropped when the payload lacks it. */
function tail(p: Record<string, unknown>): string {
  const actor = text(p.actor)
  const reason = text(p.reason)
  return (actor ? ` by ${humanize(actor).toLowerCase()}` : '') + (reason ? `: ${reason.replace(/[.\s]+$/, '')}` : '')
}

function describe(e: LiveEvent): string {
  const p = e.payload ?? {}
  switch (e.kind) {
    case 'hello':
      return 'Connected to live updates.'
    case 'mode.changed':
      return `Mode changed from ${modeTerm(text(p.old_mode)).label} to ${modeTerm(text(p.new_mode)).label}${tail(p)}.`
    case 'risk.state':
      return `Risk state changed from ${riskTerm(text(p.old_state)).label} to ${riskTerm(text(p.new_state)).label}${tail(p)}.`
    case 'kill.state':
      return `${p.killed === true ? 'Kill switch turned on' : 'Kill switch reset'}${tail(p)}.`
    case 'trade_window.changed': {
      const start = text(p.start_ist)
      const end = text(p.end_ist)
      const buffer = typeof p.squareoff_buffer_min === 'number' ? p.squareoff_buffer_min : 0
      const hours = start && end ? `Trading hours set to ${start}–${end}` : 'Trading hours changed'
      const stop = buffer > 0 ? `, intraday entries stop ${buffer} min before the end` : ''
      const actor = text(p.actor)
      return `${hours}${stop}${actor ? ` by ${humanize(actor).toLowerCase()}` : ''}.`
    }
    case 'budget.state':
      return `AI budget level changed from ${tierTerm(text(p.old_tier)).label} to ${tierTerm(text(p.new_tier)).label}.`
    default:
      return humanize(e.kind || '') || '—'
  }
}

export function LiveUpdates({ events, conn }: { events: LiveEvent[]; conn: ConnState }) {
  const status =
    conn === 'open' ? (
      <Pill tone="good">Connected</Pill>
    ) : conn === 'connecting' ? (
      <Pill tone="note">Connecting…</Pill>
    ) : (
      <Pill tone="neutral">Reconnecting</Pill>
    )

  return (
    <Card id="act-live" tier="secondary" title="Live updates" meta={status}>
      {events.length === 0 ? (
        <p className="empty">
          No live updates since you opened this page.
          {conn === 'open' ? '' : ' Live updates are reconnecting.'}
        </p>
      ) : (
        <ul className="act-events">
          {events.map((e) => (
            <li key={e.seq} className="act-event">
              <div className="act-event-line">
                <span className="act-msg-time">{istTime(e.at, { seconds: true })}</span>
                <span>{describe(e)}</span>
              </div>
              <details className="disclosure">
                <summary>
                  <Icon name="chevron-right" size={14} />
                  Show raw data
                </summary>
                <pre className="act-pre">{JSON.stringify(e.payload, null, 2)}</pre>
              </details>
            </li>
          ))}
        </ul>
      )}
    </Card>
  )
}
