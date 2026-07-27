/**
 * Panel 1 — status header: mode / routing / risk_state, the owner-set trade window, the budget
 * degrade tier, and the KILLED banner.
 *
 * Mode buttons cover OFF and RECOMMEND only. →AUTO is DELIBERATELY absent: it is an owner TWO-STEP
 * transition with no dashboard path (Telegram `/mode AUTO` + `/confirm`), and `POST /mode` answers
 * 409 for it regardless of wiring (§3.5.3/R10).
 */
import { useState } from 'react'
import type { BudgetResponse, ConnState, ModeResponse, TradeWindowResponse } from '../types'
import { Chip, toneFor } from './ui'

const OWNER_MODES = ['OFF', 'RECOMMEND'] as const

export function StatusHeader({
  mode,
  tradeWindow,
  budget,
  conn,
  lastPollAt,
  error,
  onSetMode,
  onSignOut,
}: {
  mode: ModeResponse | null
  tradeWindow: TradeWindowResponse | null
  budget: BudgetResponse | null
  conn: ConnState
  lastPollAt: string | null
  error: string | null
  onSetMode: (mode: string) => Promise<void>
  onSignOut: () => void
}) {
  const [busy, setBusy] = useState(false)
  const killed = mode?.risk_state === 'KILLED'
  const win = tradeWindow?.trade_window

  async function apply(target: string) {
    setBusy(true)
    try {
      await onSetMode(target)
    } finally {
      setBusy(false)
    }
  }

  return (
    <>
      <div className="header">
        <span className="title">market_trading</span>
        <Chip k="mode" v={mode?.mode ?? '—'} tone={toneFor(mode?.mode)} />
        <Chip k="routing" v={mode?.routing ?? '—'} tone={toneFor(mode?.routing)} />
        <Chip k="risk" v={mode?.risk_state ?? '—'} tone={toneFor(mode?.risk_state)} />
        <Chip
          k="window"
          v={win ? `${win.start}–${win.end}${win.squareoff_buffer_min !== null ? ` (−${win.squareoff_buffer_min}m)` : ''}` : 'unset'}
          tone={win ? 'neutral' : 'warn'}
        />
        <Chip k="tier" v={budget?.degrade_tier ?? '—'} tone={toneFor(budget?.degrade_tier)} />
        <span className="spacer" />
        {OWNER_MODES.map((m) => (
          <button key={m} disabled={busy || mode?.mode === m} onClick={() => void apply(m)}>
            set {m}
          </button>
        ))}
        <Chip k="ws" v={conn} tone={conn === 'open' ? 'ok' : conn === 'connecting' ? 'warn' : 'bad'} />
        <Chip k="polled" v={lastPollAt ?? '—'} />
        <button onClick={onSignOut}>sign out</button>
      </div>
      {killed ? <div className="kill-banner">KILLED — kill switch engaged; reset is Telegram two-step only</div> : null}
      {error ? <div className="header err">{error}</div> : null}
    </>
  )
}
