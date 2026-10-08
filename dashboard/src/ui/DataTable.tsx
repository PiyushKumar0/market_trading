/**
 * A real <table> that never clips: wide tables scroll sideways inside their own focusable region, and
 * under 640 px every row becomes a stacked label/value card (CSS, via each cell's `data-label`).
 */
import type { ReactNode } from 'react'
import { EmptyState } from './Card'

export interface Column<R> {
  key: string
  header: ReactNode
  /** Plain-text name for the stacked (phone) layout and the cell's label. */
  label: string
  cell: (row: R) => ReactNode
  align?: 'left' | 'right'
  /** Keep the cell on one line (prices, times). */
  nowrap?: boolean
  /** Extra class on the column's cells (e.g. `wrap-anywhere` for long symbols). */
  className?: string
}

export function DataTable<R>({
  columns,
  rows,
  rowKey,
  caption,
  empty,
  rowClass,
}: {
  columns: Column<R>[]
  rows: R[]
  rowKey: (row: R, index: number) => string
  caption: string
  empty?: ReactNode
  rowClass?: (row: R) => string | undefined
}) {
  if (rows.length === 0) return empty ? <EmptyState>{empty}</EmptyState> : null
  const cls = (c: Column<R>) =>
    [c.align === 'right' ? 'right num' : '', c.nowrap ? 'nowrap' : '', c.className ?? ''].filter(Boolean).join(' ') ||
    undefined
  return (
    <div className="table-scroll" role="region" aria-label={caption} tabIndex={0}>
      <table className="dt">
        <caption className="sr-only">{caption}</caption>
        <thead>
          <tr>
            {columns.map((c) => (
              <th key={c.key} scope="col" className={cls(c)}>
                {c.header}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, i) => (
            <tr key={rowKey(row, i)} className={rowClass?.(row)}>
              {columns.map((c) => (
                <td key={c.key} data-label={c.label} className={cls(c)}>
                  {/* One wrapper: in the stacked phone layout the label and the value are exactly two items. */}
                  <span className="cell-v">{c.cell(row)}</span>
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
