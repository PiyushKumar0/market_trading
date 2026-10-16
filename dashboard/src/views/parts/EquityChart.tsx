/**
 * Paper equity at each session's close (the last point is the latest snapshot), against a dashed,
 * labelled starting-capital line. Dataviz rules: 2 px line, solid hairline grid, end dot with a surface
 * ring, a crosshair that finds the nearest session on hover or with ←/→, and a table twin. Money
 * strings are parsed for geometry only; every printed value is the engine's own string.
 */
import { useCallback, useId, useRef, useState } from 'react'
import type { KeyboardEvent, PointerEvent } from 'react'
import { inr, istDay, istStamp, istTime, shortDate, todayIst } from '../../lib/format.ts'
import type { PaperCurvePoint } from '../../types'
import { EmptyState } from '../../ui/Card'
import { DataTable } from '../../ui/DataTable'

const H = 220
const PAD = { top: 16, right: 16, bottom: 30, left: 84 }

function niceStep(range: number, count: number): number {
  const raw = range / count
  const mag = 10 ** Math.floor(Math.log10(raw))
  const n = raw / mag
  return (n <= 1 ? 1 : n <= 2 ? 2 : n <= 2.5 ? 2.5 : n <= 5 ? 5 : 10) * mag
}

export function EquityChart({ points, base }: { points: PaperCurvePoint[]; base: string }) {
  const [w, setW] = useState(720)
  const [activeRaw, setActive] = useState<number | null>(null)
  const [asTable, setAsTable] = useState(false)
  const ro = useRef<ResizeObserver | null>(null)
  const summaryId = useId()

  // Measured in CSS pixels, so text and dots are never stretched (no preserveAspectRatio="none").
  const measure = useCallback((el: HTMLDivElement | null) => {
    ro.current?.disconnect()
    if (!el) return
    ro.current = new ResizeObserver((entries) => {
      const cw = entries[0]?.contentRect.width
      if (cw) setW(Math.max(260, Math.floor(cw)))
    })
    ro.current.observe(el)
  }, [])

  const pts = points.map((p) => ({ ...p, v: Number(p.equity) })).filter((p) => Number.isFinite(p.v))
  if (pts.length < 2) return <EmptyState>The chart starts after the second session.</EmptyState>

  const b = Number(base)
  const hasBase = Number.isFinite(b) && b > 0
  let lo = Math.min(...pts.map((p) => p.v), ...(hasBase ? [b] : []))
  let hi = Math.max(...pts.map((p) => p.v), ...(hasBase ? [b] : []))
  const floor = hasBase ? b * 0.02 : 2 // a small move never fills the whole chart
  if (hi - lo < floor) [lo, hi] = [(lo + hi - floor) / 2, (lo + hi + floor) / 2]
  const pad = (hi - lo) * 0.1
  ;[lo, hi] = [lo - pad, hi + pad]

  const plotW = w - PAD.left - PAD.right
  const plotH = H - PAD.top - PAD.bottom
  const last = pts.length - 1
  // The crosshair survives a poll; a shorter curve (a paper reset) must not index past its end.
  const active = activeRaw === null ? null : Math.min(activeRaw, last)
  const x = (i: number) => PAD.left + (plotW * i) / last
  const y = (v: number) => PAD.top + (plotH * (hi - v)) / (hi - lo)
  const step = niceStep(hi - lo, 4)
  const ticks: number[] = []
  for (let t = Math.ceil(lo / step) * step; t <= hi; t += step) ticks.push(t)
  const line = pts.map((p, i) => `${x(i)},${y(p.v)}`).join(' ')
  const area = `M${x(0)},${y(pts[0].v)} L${line.replace(/ /g, ' L')} L${x(last)},${PAD.top + plotH} L${x(0)},${PAD.top + plotH} Z`
  const today = todayIst()
  const when = (i: number) =>
    i === last && istDay(pts[i].at) === today ? `Today, ${istTime(pts[i].at)}` : shortDate(pts[i].d)

  const nearest = (clientX: number, el: Element) => {
    const rx = clientX - el.getBoundingClientRect().left
    return Math.max(0, Math.min(last, Math.round(((rx - PAD.left) / plotW) * last)))
  }
  const onMove = (e: PointerEvent<SVGSVGElement>) => setActive(nearest(e.clientX, e.currentTarget))
  const onKey = (e: KeyboardEvent<HTMLDivElement>) => {
    const cur = active ?? last
    const next =
      e.key === 'ArrowLeft' ? Math.max(0, cur - 1)
      : e.key === 'ArrowRight' ? Math.min(last, cur + 1)
      : e.key === 'Home' ? 0
      : e.key === 'End' ? last
      : null
    if (e.key === 'Escape') setActive(null)
    if (next === null) return
    e.preventDefault()
    setActive(next)
  }

  const tipLeft = active === null ? 0 : Math.max(0, Math.min(w - 172, x(active) - 86))
  const cur = active ?? last // what the keyboard reads: the crosshair's session, else the latest

  return (
    <div className="eq">
      <div className="eq-tools">
        <button type="button" className="btn btn-quiet" aria-pressed={asTable} onClick={() => setAsTable((t) => !t)}>
          {asTable ? 'Show as chart' : 'Show as table'}
        </button>
      </div>
      {asTable ? (
        <DataTable
          caption="Paper equity at each session close"
          columns={[
            { key: 'd', header: 'Session', label: 'Session', cell: (p) => shortDate(p.d) },
            { key: 'eq', header: 'Paper equity', label: 'Paper equity', align: 'right', nowrap: true, cell: (p) => inr(p.equity) },
            { key: 'at', header: 'Recorded at', label: 'Recorded at', nowrap: true, cell: (p) => istStamp(p.at) },
          ]}
          rows={pts}
          rowKey={(p) => p.at}
        />
      ) : (
        // A slider over the sessions: screen readers pass ←/→ to it (a focusable "img" stays in browse
        // mode and never gets the keys) and announce each session from aria-valuetext.
        <div
          className="eq-plot"
          ref={measure}
          role="slider"
          tabIndex={0}
          aria-label="Paper equity by session"
          aria-describedby={summaryId}
          aria-valuemin={0}
          aria-valuemax={last}
          aria-valuenow={cur}
          aria-valuetext={`${when(cur)}: ${inr(pts[cur].equity)}`}
          onKeyDown={onKey}
          onBlur={() => setActive(null)}
        >
          <svg width={w} height={H} onPointerMove={onMove} onPointerLeave={() => setActive(null)} aria-hidden="true">
            {ticks.map((t) => (
              <g key={t}>
                <line x1={PAD.left} x2={w - PAD.right} y1={y(t)} y2={y(t)} className="eq-grid" />
                <text x={PAD.left - 8} y={y(t)} className="eq-tick" textAnchor="end" dominantBaseline="middle">
                  {inr(String(Math.round(t)))}
                </text>
              </g>
            ))}
            {hasBase ? <line x1={PAD.left} x2={w - PAD.right} y1={y(b)} y2={y(b)} className="eq-base" /> : null}
            <path d={area} className="eq-area" />
            <polyline points={line} className="eq-line" />
            {active !== null ? (
              <g>
                <line x1={x(active)} x2={x(active)} y1={PAD.top} y2={PAD.top + plotH} className="eq-cross" />
                <circle cx={x(active)} cy={y(pts[active].v)} r={4.5} className="eq-dot" />
              </g>
            ) : null}
            <circle cx={x(last)} cy={y(pts[last].v)} r={4.5} className="eq-dot" />
            <text x={PAD.left} y={H - 8} className="eq-tick">
              {shortDate(pts[0].d)}
            </text>
            <text x={w - PAD.right} y={H - 8} className="eq-tick" textAnchor="end">
              {when(last)}
            </text>
          </svg>
          {active !== null ? (
            <div className="eq-tip" style={{ left: tipLeft }}>
              <strong className="eq-tip-v">{inr(pts[active].equity)}</strong>
              <span className="eq-tip-k">{when(active)}</span>
            </div>
          ) : null}
        </div>
      )}
      {!asTable ? (
        <span id={summaryId} className="sr-only">
          {`${pts.length} sessions from ${shortDate(pts[0].d)} to ${when(last)}. Use the left and right arrow keys to read each session.`}
        </span>
      ) : null}
      {!asTable ? (
        <div className="eq-legend" aria-hidden="true">
          <span className="eq-key">
            <svg width="22" height="8">
              <line x1="1" x2="21" y1="4" y2="4" className="eq-line" />
            </svg>
            Paper equity at each session close
          </span>
          {hasBase ? (
            <span className="eq-key">
              <svg width="22" height="8">
                <line x1="1" x2="21" y1="4" y2="4" className="eq-base" />
              </svg>
              Starting capital {inr(base)}
            </span>
          ) : null}
        </div>
      ) : null}
    </div>
  )
}
