/** Overview's glance at the paper autopilot: one status sentence, three figures, a sparkline. Simulated. */
import { COPY } from '../../copy/glossary.ts'
import { isPaperSummary } from '../../lib/alerts.ts'
import { inr, istStamp, signOf, signedInr } from '../../lib/format.ts'
import { paperStatus } from '../../lib/paper.ts'
import type { PaperCurvePoint, PaperResponse } from '../../types'
import { Card, EmptyState } from '../../ui/Card'
import { StatTile } from '../../ui/Figures'
import { Hint } from '../../ui/Hint'
import { Icon } from '../../ui/Icon'
import { Loading } from '../../ui/Loading'

const valueClass = (v: string | null | undefined) => {
  const s = signOf(v)
  return s === 1 ? 'pos' : s === -1 ? 'neg' : undefined
}

/** A shape-only trend line (no axes): the Paper tab has the readable chart and its table. */
export function Sparkline({ points, label }: { points: PaperCurvePoint[]; label: string }) {
  const vals = points.map((p) => Number(p.equity)).filter((v) => Number.isFinite(v))
  if (vals.length < 2) return null
  const lo = Math.min(...vals)
  const hi = Math.max(...vals)
  const span = hi - lo || 1
  const x = (i: number) => 2 + (96 * i) / (vals.length - 1)
  const y = (v: number) => 28 - (24 * (v - lo)) / span
  const last = vals.length - 1
  return (
    <svg className="spark" viewBox="0 0 100 32" preserveAspectRatio="none" role="img" aria-label={label}>
      <polyline
        points={vals.map((v, i) => `${x(i)},${y(v)}`).join(' ')}
        fill="none"
        stroke="var(--sim)"
        strokeWidth={2}
        strokeLinejoin="round"
        strokeLinecap="round"
        vectorEffect="non-scaling-stroke"
      />
      {/* A zero-length round-capped stroke stays a circle however the box stretches. */}
      <path
        d={`M${x(last)},${y(vals[last])}h0`}
        stroke="var(--sim)"
        strokeWidth={8}
        strokeLinecap="round"
        vectorEffect="non-scaling-stroke"
      />
    </svg>
  )
}

export function PaperSnapshot({
  paper,
  mode,
  onOpen,
}: {
  paper: PaperResponse | null
  mode: string | null
  onOpen: () => void
}) {
  const p = isPaperSummary(paper) ? paper : null
  const action = (
    <button type="button" className="btn btn-quiet" onClick={onOpen}>
      Open paper trading
    </button>
  )
  if (!p) {
    return (
      <Card tier="simulated" title="Paper autopilot">
        {paper ? (
          <EmptyState>The engine doesn&apos;t have paper trading connected.</EmptyState>
        ) : (
          <Loading route="paper" what="paper trading" />
        )}
      </Card>
    )
  }
  const line = paperStatus(p, mode)
  const eq = p.equity
  return (
    <Card
      tier="simulated"
      title="Paper autopilot"
      hint={<Hint label="Paper autopilot">{COPY.paperAbout}</Hint>}
      meta={eq ? `Updated ${istStamp(eq.at)}` : undefined}
      action={action}
    >
      <p className={`paper-line paper-line-${line.tone}`}>
        <Icon name={line.tone === 'good' ? 'check' : line.tone === 'warn' ? 'pause' : 'info'} />
        <span>{line.text}</span>
      </p>
      {eq ? (
        <>
          <div className="stats stats-compact">
            <StatTile label="Paper equity" value={inr(eq.equity)} caption={`Started with ${inr(eq.capital_base) ?? '—'}`} />
            <StatTile label="Net since start" value={<span className={valueClass(eq.pnl)}>{signedInr(eq.pnl) ?? '—'}</span>} />
            <StatTile label="Today" value={<span className={valueClass(eq.day_mtm)}>{signedInr(eq.day_mtm) ?? '—'}</span>} />
          </div>
          <Sparkline points={p.curve} label={`Paper equity over ${p.curve.length} sessions, now ${inr(eq.equity)}`} />
        </>
      ) : (
        <EmptyState>
          No paper equity yet.{p.built ? ' It is recorded every minute once today’s paper setup completes.' : ''}
        </EmptyState>
      )}
    </Card>
  )
}
