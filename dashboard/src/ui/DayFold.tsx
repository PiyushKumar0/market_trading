/**
 * IST day folds for the two ledgers (recommendations, risk checks) — owner directive 2026-09-08:
 * today's fold starts open, earlier days start folded and open on click. `alsoOpen` widens the default
 * for a day still holding a live recommendation (normally today's; kept date-agnostic after WO-V's 2026-09-23 withdrawal). The default is re-derived every
 * render, so the midnight rollover moves the open fold to the new day on the next poll.
 */
import { useState } from 'react'
import { dayLabel, todayIst } from '../lib/format.ts'
import { Icon } from './Icon'

export interface DayGroup<T> {
  day: string
  rows: T[]
}

/** Bucket rows by IST day, newest day first, the undated bucket last. Rows keep their server order
 *  inside a day — the ledger routes answer most-recent-first and this console never re-sorts. */
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

export function useDayFolds(alsoOpen?: (day: string) => boolean): {
  isOpen: (day: string) => boolean
  toggle: (day: string) => void
} {
  const [overrides, setOverrides] = useState<Partial<Record<string, boolean>>>({})
  const today = todayIst()
  const byDefault = (day: string) => day === today || (alsoOpen?.(day) ?? false)
  const isOpen = (day: string) => overrides[day] ?? byDefault(day)
  const toggle = (day: string) => setOverrides((prev) => ({ ...prev, [day]: !(prev[day] ?? byDefault(day)) }))
  return { isOpen, toggle }
}

/** The fold header: a real <button>, so it folds from the keyboard too. */
export function DayFold({
  day,
  count,
  noun,
  open,
  onToggle,
  extra,
}: {
  day: string
  count: number
  noun: [string, string]
  open: boolean
  onToggle: () => void
  extra?: string
}) {
  return (
    <button type="button" className="dayfold" aria-expanded={open} onClick={onToggle}>
      <Icon name={open ? 'chevron-down' : 'chevron-right'} size={16} />
      <span className="dayfold-day">{dayLabel(day)}</span>
      <span className="dayfold-count">
        {count} {count === 1 ? noun[0] : noun[1]}
        {extra ? `, ${extra}` : ''}
      </span>
    </button>
  )
}
