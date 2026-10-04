/**
 * Panel 8 — live events feed: every `/ws/live` frame the engine relays off the bus (mode / risk /
 * kill / trade-window / budget state changes), newest first. The 15 s `ping` keepalive is filtered
 * out by the socket hook, not here.
 */
import type { ConnState, LiveEvent } from '../types'
import { Chip, Empty, Panel, hhmmss, toneFor } from './ui'

export function EventsPanel({ events, conn }: { events: LiveEvent[]; conn: ConnState }) {
  return (
    <Panel
      title="Live events"
      aside={<Chip v={conn} tone={conn === 'open' ? 'ok' : conn === 'connecting' ? 'warn' : 'bad'} />}
    >
      {events.length === 0 ? (
        <Empty what="events yet" />
      ) : (
        <div className="events">
          {events.map((e) => (
            <div className="ev" key={e.seq}>
              <span className="at">{hhmmss(e.at)}</span>
              <Chip v={e.kind} tone={toneFor(String(e.payload.new_mode ?? e.payload.risk_state ?? ''))} />
              <span className="body">{JSON.stringify(e.payload)}</span>
            </div>
          ))}
        </div>
      )}
    </Panel>
  )
}
