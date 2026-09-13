/** Shared primitives for the panels: a titled panel shell, a status chip, small formatters, and the
 *  per-day fold the two ledger panels (recommendations, decision log) split on. */
import { useState } from 'react'
import type { ReactNode, Ref } from 'react'

export type Tone = 'neutral' | 'ok' | 'warn' | 'bad' | 'info'

/** `bodyRef` hands a panel its own scroll container (`.panel-body` is the bounded, scrolling box):
 *  only the notifications transcript uses it, to keep the newest row in view. */
export function Panel({
  title,
  aside,
  wide,
  bodyRef,
  children,
}: {
  title: string
  aside?: ReactNode
  wide?: boolean
  bodyRef?: Ref<HTMLDivElement>
  children: ReactNode
}) {
  return (
    <section className={wide ? 'panel wide' : 'panel'}>
      <h2>
        <span>{title}</span>
        {aside ? <span className="dim">{aside}</span> : null}
      </h2>
      <div className="panel-body" ref={bodyRef}>
        {children}
      </div>
    </section>
  )
}

/** `title` is the native hover tooltip — for a chip whose detail (a delivery error, a timestamp) is
 *  worth reading but not worth the row width. */
export function Chip({
  k,
  v,
  tone = 'neutral',
  title,
}: {
  k?: string
  v: ReactNode
  tone?: Tone
  title?: string
}) {
  return (
    <span className={tone === 'neutral' ? 'chip' : `chip ${tone}`} title={title}>
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

/* ------------------------------------------------------------------ day folds (ledger panels) */

/** The engine's calendar is IST; the browser's need not be. Every day label on this console comes
 *  from these formatters, never from `Date#getDate`. */
const IST_DAY = new Intl.DateTimeFormat('en-CA', {
  timeZone: 'Asia/Kolkata',
  year: 'numeric',
  month: '2-digit',
  day: '2-digit',
})
const IST_WEEKDAY = new Intl.DateTimeFormat('en-GB', { timeZone: 'Asia/Kolkata', weekday: 'short' })

function istDayOf(at: Date): string {
  const parts = IST_DAY.formatToParts(at)
  const part = (type: string) => parts.find((p) => p.type === type)?.value ?? ''
  return `${part('year')}-${part('month')}-${part('day')}`
}

/** ISO-8601 → the IST calendar day ("YYYY-MM-DD") the stamp falls on. '' when the stamp is missing
 *  or unparseable: such rows fold under an "undated" header rather than disappear. */
export function istDay(iso: string | null | undefined): string {
  if (!iso) return ''
  const at = new Date(iso)
  return Number.isNaN(at.getTime()) ? '' : istDayOf(at)
}

export function todayIst(): string {
  return istDayOf(new Date())
}

/** "Tue" for a "YYYY-MM-DD" day label; '' for the undated bucket. */
function weekdayOf(day: string): string {
  if (!day) return ''
  const at = new Date(`${day}T12:00:00+05:30`)
  return Number.isNaN(at.getTime()) ? '' : IST_WEEKDAY.format(at)
}

export interface DayGroup<T> {
  day: string
  rows: T[]
}

/** Bucket rows by IST day, newest day first, the undated bucket last. Rows keep their server order
 *  inside a day — the ledger routes already answer most-recent-first and this console never re-sorts. */
export function groupByDay<T>(rows: T[], dayOf: (row: T) => string): DayGroup<T>[] {
  const buckets = new Map<string, T[]>()
  for (const row of rows) {
    const day = dayOf(row)
    const bucket = buckets.get(day)
    if (bucket) bucket.push(row)
    else buckets.set(day, [row])
  }
  return [...buckets]
    .map(([day, bucket]) => ({ day, rows: bucket }))
    .sort((a, b) => (a.day < b.day ? 1 : a.day > b.day ? -1 : 0))
}

/** Fold state for a panel split by day: today's IST day starts open, every earlier day starts
 *  folded, and a click overrides that for the rest of the page session. The default is re-derived on
 *  every render, so the midnight rollover moves the open fold to the new day on the next poll.
 *  `alsoOpen` widens the default for a panel whose rows can outlive their day (a recommendation
 *  valid into the next session, WO-V 2026-09-13): such a day starts open too, and a click still folds it. */
export function useDayFolds(
  alsoOpen?: (day: string) => boolean,
): { isOpen: (day: string) => boolean; toggle: (day: string) => void } {
  const [overrides, setOverrides] = useState<Partial<Record<string, boolean>>>({})
  const today = todayIst()
  const byDefault = (day: string) => day === today || (alsoOpen?.(day) ?? false)
  const isOpen = (day: string) => overrides[day] ?? byDefault(day)
  const toggle = (day: string) =>
    setOverrides((prev) => ({ ...prev, [day]: !(prev[day] ?? byDefault(day)) }))
  return { isOpen, toggle }
}

/** The clickable header a day-folded ledger splits on; `what` is the noun behind the row count. */
export function DayFold({
  day,
  count,
  what,
  open,
  onToggle,
}: {
  day: string
  count: number
  what: string
  open: boolean
  onToggle: () => void
}) {
  return (
    <button type="button" className="day" aria-expanded={open} onClick={onToggle}>
      <span className="caret">{open ? '▾' : '▸'}</span>
      <strong>{day || 'undated'}</strong>
      {day ? <span className="dim">{weekdayOf(day)}</span> : null}
      {day && day === todayIst() ? <Chip v="today" tone="info" /> : null}
      <span className="dim">
        {count} {what}
      </span>
    </button>
  )
}
