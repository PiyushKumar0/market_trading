/** System tab, engine status: mode, risk state, order routing, trading hours and the live-update link. */
import type { Term } from '../../copy/glossary.ts'
import { modeTerm, riskTerm, routingTerm } from '../../copy/glossary.ts'
import { istTime } from '../../lib/format.ts'
import type { ConnState, ModeResponse, TradeWindowResponse } from '../../types'
import { Card } from '../../ui/Card'
import { KeyValue } from '../../ui/Figures'
import { Labelled } from '../../ui/Hint'
import { Pill } from '../../ui/Pill'
import type { Tone } from '../../ui/Pill'

const RISK_TONE: Record<string, Tone> = { NORMAL: 'good', FROZEN: 'warn', CLOSE_ONLY: 'crit', KILLED: 'crit' }

const MODE_KEY: Term = { label: 'Mode', hint: 'What the engine is allowed to do. You change it in the top bar.', term: 'mode' }
const ROUTING_KEY: Term = {
  label: 'Auto orders go to',
  hint: 'Only matters in Auto mode: whether the engine’s orders go to the simulated paper account or your real account.',
  term: 'routing',
}
const LIVE_KEY: Term = {
  label: 'Live updates',
  hint: 'Mode, risk and budget changes arrive instantly while connected. Everything else refreshes every 10 seconds.',
}

const CONN_TEXT: Record<ConnState, string> = { open: 'Connected', connecting: 'Connecting…', closed: 'Reconnecting' }

export function EngineStatus({
  mode,
  tradeWindow,
  conn,
  lastPollAt,
}: {
  mode: ModeResponse | null
  tradeWindow: TradeWindowResponse | null
  conn: ConnState
  lastPollAt: string | null
}) {
  const win = tradeWindow?.trade_window ?? null
  const buffer = win?.squareoff_buffer_min
  const hoursKey: Term = {
    label: 'Trading hours',
    hint:
      'New trades are only proposed inside these hours; intraday entries stop ' +
      `${buffer !== null && buffer !== undefined ? `${buffer} minutes` : 'a few minutes'} before the end. Exits are never blocked.`,
    term: 'trade_window',
  }
  const risk = riskTerm(mode?.risk_state)

  return (
    <Card title="Engine status">
      <KeyValue
        items={[
          { k: <Labelled t={MODE_KEY} />, v: modeTerm(mode?.mode).label },
          {
            k: <Labelled t={{ ...risk, label: 'Risk state' }} />,
            v: <Pill tone={RISK_TONE[mode?.risk_state ?? ''] ?? 'neutral'}>{risk.label}</Pill>,
          },
          { k: <Labelled t={ROUTING_KEY} />, v: routingTerm(mode?.routing).label },
          { k: <Labelled t={hoursKey} />, v: win ? `${win.start}–${win.end}` : 'Not set' },
          { k: <Labelled t={LIVE_KEY} />, v: CONN_TEXT[conn] },
          { k: 'Last refresh', v: lastPollAt ? istTime(lastPollAt, { seconds: true }) : '—' },
        ]}
      />
    </Card>
  )
}
