/**
 * Owner trade-window editor (§3.2.7): `POST /config/trade_window` is SINGLE-step — the bearer token
 * IS the owner authentication (§2.4) — and the engine validates, persists sticky, audits and
 * publishes. A rejected window answers 422 and leaves the stored value UNCHANGED.
 */
import { useEffect, useState } from 'react'
import type { FormEvent } from 'react'
import type { TradeWindowResponse } from '../types'
import { Panel } from './ui'

export function TradeWindowForm({
  data,
  onSubmit,
}: {
  data: TradeWindowResponse | null
  onSubmit: (body: { start: string; end: string; squareoff_buffer_min: number | null }) => Promise<string>
}) {
  const current = data?.trade_window ?? null
  const [start, setStart] = useState('')
  const [end, setEnd] = useState('')
  const [buffer, setBuffer] = useState('')
  const [busy, setBusy] = useState(false)
  const [note, setNote] = useState('')

  // Seed the inputs from the engine ONCE the window is known; later polls must not fight the owner's
  // typing, so re-seeding is keyed on the stored value changing.
  useEffect(() => {
    if (!current) return
    setStart(current.start)
    setEnd(current.end)
    setBuffer(current.squareoff_buffer_min !== null ? String(current.squareoff_buffer_min) : '')
  }, [current?.start, current?.end, current?.squareoff_buffer_min])

  async function submit(e: FormEvent) {
    e.preventDefault()
    setBusy(true)
    setNote('')
    try {
      setNote(
        await onSubmit({
          start,
          end,
          squareoff_buffer_min: buffer.trim() === '' ? null : Number(buffer),
        }),
      )
    } finally {
      setBusy(false)
    }
  }

  return (
    <Panel title="Trade window" aside={current ? `${current.start}–${current.end}` : 'unset'}>
      <form className="inline" onSubmit={(e) => void submit(e)}>
        <label htmlFor="tw-start">start</label>
        <input id="tw-start" type="time" required value={start} onChange={(e) => setStart(e.target.value)} />
        <label htmlFor="tw-end">end</label>
        <input id="tw-end" type="time" required value={end} onChange={(e) => setEnd(e.target.value)} />
        <label htmlFor="tw-buf">squareoff buffer (min)</label>
        <input
          id="tw-buf"
          type="number"
          min={0}
          value={buffer}
          onChange={(e) => setBuffer(e.target.value)}
        />
        <button type="submit" disabled={busy || !start || !end}>
          set window
        </button>
      </form>
      {note ? <div className="dim" style={{ marginTop: 6 }}>{note}</div> : null}
    </Panel>
  )
}
