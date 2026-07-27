/** Shared primitives for the panels: a titled panel shell, a status chip, and small formatters. */
import type { ReactNode } from 'react'

export type Tone = 'neutral' | 'ok' | 'warn' | 'bad' | 'info'

export function Panel({
  title,
  aside,
  wide,
  children,
}: {
  title: string
  aside?: ReactNode
  wide?: boolean
  children: ReactNode
}) {
  return (
    <section className={wide ? 'panel wide' : 'panel'}>
      <h2>
        <span>{title}</span>
        {aside ? <span className="dim">{aside}</span> : null}
      </h2>
      <div className="panel-body">{children}</div>
    </section>
  )
}

export function Chip({ k, v, tone = 'neutral' }: { k?: string; v: ReactNode; tone?: Tone }) {
  return (
    <span className={tone === 'neutral' ? 'chip' : `chip ${tone}`}>
      {k ? <span className="k">{k}</span> : null}
      <span>{v}</span>
    </span>
  )
}

export function Empty({ what }: { what: string }) {
  return <div className="empty">no {what}</div>
}

/** Fraction of an allocation that has been spent, as a bar; tone escalates past 80% / 100%. */
export function Meter({ fraction }: { fraction: number }) {
  const pct = Math.max(0, Math.min(1, fraction)) * 100
  const tone = fraction >= 1 ? 'bad' : fraction >= 0.8 ? 'warn' : ''
  return (
    <span className="bar">
      <span className={tone} style={{ width: `${pct}%` }} />
    </span>
  )
}

/** ISO-8601 (always tz-aware IST off the engine) → "HH:MM:SS"; the date is implicit for a session view. */
export function hhmmss(iso: string | null | undefined): string {
  if (!iso) return '—'
  const at = new Date(iso)
  return Number.isNaN(at.getTime()) ? iso : at.toTimeString().slice(0, 8)
}

export function dash(value: unknown): ReactNode {
  return value === null || value === undefined || value === '' ? <span className="dim">—</span> : String(value)
}

/** Mode / risk-state / verdict / human-action → chip tone. Anything unknown stays neutral. */
export function toneFor(value: string | null | undefined): Tone {
  switch (value) {
    case 'NORMAL':
    case 'approve':
    case 'fresh':
    case 'taken':
    case 'DG0':
    case 'OPEN':
      return 'ok'
    case 'FROZEN':
    case 'CLOSE_ONLY':
    case 'shrink':
    case 'owner_approval_required':
    case 'stale':
    case 'DG1':
    case 'DG2':
      return 'warn'
    case 'KILLED':
    case 'reject':
    case 'missing':
    case 'dismissed':
    case 'DG3':
    case 'DG4':
      return 'bad'
    case 'AUTO':
    case 'RECOMMEND':
    case 'live':
      return 'info'
    default:
      return 'neutral'
  }
}
