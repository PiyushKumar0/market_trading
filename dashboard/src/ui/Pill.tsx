/** Status pill: icon + text on a 14 % tint of the tone. The text carries the meaning; colour never does
 *  so alone (dataviz status rule). */
import type { ReactNode } from 'react'
import { Icon } from './Icon'
import type { IconName } from './Icon'

export type Tone = 'good' | 'warn' | 'crit' | 'note' | 'sim' | 'accent' | 'neutral' | 'buy' | 'sell'

export function Pill({
  tone = 'neutral',
  icon,
  children,
  title,
}: {
  tone?: Tone
  icon?: IconName
  children: ReactNode
  title?: string
}) {
  return (
    <span className={`pill pill-${tone}`} title={title}>
      {icon ? <Icon name={icon} size={14} /> : null}
      <span>{children}</span>
    </span>
  )
}

/** The BUY / SELL badge, in Kite's own colours (buy blue, sell orange). */
export function SideBadge({ side, kind }: { side: string | null | undefined; kind?: string | null }) {
  // An adjust keeps the HELD side but places no order: "Buy" there would read as an instruction.
  if (kind === 'adjust') return <span className="side side-unknown">Stop-loss</span>
  const s = (side ?? '').toUpperCase()
  if (s !== 'BUY' && s !== 'SELL') return <span className="side side-unknown">{side || '—'}</span>
  return <span className={`side side-${s.toLowerCase()}`}>{s === 'BUY' ? 'Buy' : 'Sell'}</span>
}
