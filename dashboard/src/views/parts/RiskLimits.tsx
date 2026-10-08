/** System tab, risk limits: the real account's equity, P&L and open exposure against the enforced caps. */
import type { Term } from '../../copy/glossary.ts'
import { inr, signOf, signedInr } from '../../lib/format.ts'
import type { HeadroomResponse } from '../../types'
import { Card, EmptyState } from '../../ui/Card'
import { KeyValue, Meter, StatTile } from '../../ui/Figures'
import { Labelled } from '../../ui/Hint'

const MAX_TRADES: Term = {
  label: 'New trades allowed per day',
  hint: 'The most new trades the risk check lets through in one day.',
  term: 'max_new_trades_day',
}

const num = (v: string | number | undefined): number | null => {
  if (v === undefined || v === '') return null
  const n = Number(v)
  return Number.isFinite(n) ? n : null
}

export function RiskLimits({ headroom }: { headroom: HeadroomResponse | null }) {
  const h = headroom?.headroom
  const intro = <p className="muted">How close your real account is to the limits the risk check enforces.</p>

  if (!h || Object.keys(h).length === 0) {
    return (
      <Card title="Risk limits">
        {intro}
        <EmptyState>Risk limits aren&apos;t available: the engine&apos;s exposure tracker isn&apos;t connected.</EmptyState>
      </Card>
    )
  }

  const caps = h.caps
  const open = h.open_positions
  const sign = signOf(h.day_mtm)
  const dep = inr(h.deployed_capital)

  const rows: { label: string; used: number | null; shown: string | null; limit: number | null; limitShown: string | null }[] = [
    {
      label: 'Capital in use',
      used: num(h.deployed_capital),
      shown: dep,
      limit: num(caps?.max_deployed_capital_inr),
      limitShown: inr(caps?.max_deployed_capital_inr),
    },
    ...(
      [
        ['Open positions', open?.total, caps?.max_open_positions_total],
        ['Delivery (CNC) positions', open?.cnc, caps?.max_open_positions_cnc],
        ['Intraday (MIS) positions', open?.mis, caps?.max_open_positions_mis],
        ['Losses in a row today', h.consecutive_losses, caps?.consecutive_losses_max_per_session],
      ] as [string, number | undefined, number | undefined][]
    ).map(([label, used, limit]) => ({
      label,
      used: used ?? null,
      shown: used === undefined ? null : String(used),
      limit: limit ?? null,
      limitShown: limit === undefined ? null : String(limit),
    })),
  ]

  return (
    <Card title="Risk limits">
      {intro}
      <div className="stats stats-compact">
        <StatTile label="Account equity" value={inr(h.equity) ?? '—'} />
        <StatTile
          label="Today's P&L"
          value={<span className={sign === 1 ? 'pos' : sign === -1 ? 'neg' : undefined}>{signedInr(h.day_mtm) ?? '—'}</span>}
        />
      </div>
      {caps ? (
        <div className="sys-meters">
          {rows.map((r) => {
            const text = r.limitShown ? `${r.shown ?? '—'} of ${r.limitShown}` : (r.shown ?? '—')
            return <Meter key={r.label} label={r.label} used={r.used} limit={r.limit} text={text} ariaLabel={`${r.label}: ${text}`} />
          })}
        </div>
      ) : (
        <>
          <KeyValue items={rows.map((r) => ({ k: r.label, v: r.shown ?? '—' }))} />
          <p className="muted">Limit values aren&apos;t available.</p>
        </>
      )}
      {caps ? (
        <p className="sys-limit-line">
          <Labelled t={MAX_TRADES} />: <span className="num">{caps.max_new_trades_day}</span>
        </p>
      ) : null}
    </Card>
  )
}
