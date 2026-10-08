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
export function SideBadge({ side }: { side: string | null | undefined }) {
  const s = (side ?? '').toUpperCase()
  if (s !== 'BUY' && s !== 'SELL') return <span className="side side-unknown">{side || '—'}</span>
  return <span className={`side side-${s.toLowerCase()}`}>{s === 'BUY' ? 'Buy' : 'Sell'}</span>
}
