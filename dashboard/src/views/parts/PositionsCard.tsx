/**
 * Your real positions (every origin; paper rows belong to the Paper tab). Open ones first, as served;
 * closed ones fold away below. A recommended open position shows whether you confirmed its stop-loss
 * — the one thing on this card that can need action.
 */
import { originTerm, positionStateTerm, productTerm } from '../../copy/glossary.ts'
import { inr, istStamp, istTime } from '../../lib/format.ts'
import type { PositionRow, PositionsResponse } from '../../types'
import { Card } from '../../ui/Card'
import { DataTable } from '../../ui/DataTable'
import type { Column } from '../../ui/DataTable'
import { Hint, Labelled } from '../../ui/Hint'
import { Icon } from '../../ui/Icon'
import { Loading } from '../../ui/Loading'
import { Pill } from '../../ui/Pill'

function Protection({ p }: { p: PositionRow }) {
  if (p.origin !== 'recommended' || p.state !== 'OPEN') return <span className="muted">—</span>
  if (p.owner_protected_at) {
    return (
      <span className="labelled">
        <Pill tone="good" icon="shield">
          Protected
        </Pill>
        <Hint label="Protected">You confirmed its stop-loss order on {istStamp(p.owner_protected_at)}.</Hint>
      </span>
    )
  }
  return (
    <span className="labelled">
      <Pill tone="crit" icon="shield-off">
        Not protected
      </Pill>
      <Hint label="Not protected">
        No stop-loss confirmed yet. Place its {p.product === 'MIS' ? 'SL-M stop-loss order' : 'GTT stop-loss'} in Kite,
        then reply /protected {p.symbol} on Telegram.
      </Hint>
    </span>
  )
}

const sideWord = (side: string | null) => (side === 'BUY' ? 'Buy' : side === 'SELL' ? 'Sell' : side)

// The safety column comes second, so a narrow card can never push it out of view.
const COLUMNS: Column<PositionRow>[] = [
  {
    key: 'symbol',
    header: 'Stock',
    label: 'Stock',
    className: 'wrap-anywhere',
    cell: (p) => (
      <>
        <strong>{p.symbol}</strong>
        <span className="cell-sub">
          {sideWord(p.side)}
          {p.side && p.product ? ', ' : null}
          {p.product ? <Labelled t={productTerm(p.product)} /> : null}
        </span>
      </>
    ),
  },
  { key: 'protection', header: 'Stop-loss confirmed', label: 'Stop-loss confirmed', cell: (p) => <Protection p={p} /> },
  { key: 'qty', header: 'Qty', label: 'Qty', align: 'right', cell: (p) => p.qty ?? '—' },
  { key: 'avg', header: 'Avg price', label: 'Avg price', align: 'right', nowrap: true, cell: (p) => inr(p.avg_entry) ?? '—' },
  { key: 'stop', header: 'Stop-loss', label: 'Stop-loss', align: 'right', nowrap: true, cell: (p) => inr(p.stop) ?? '—' },
  { key: 'target', header: 'Target', label: 'Target', align: 'right', nowrap: true, cell: (p) => inr(p.target) ?? '—' },
  { key: 'state', header: 'Status', label: 'Status', cell: (p) => <Labelled t={positionStateTerm(p.state)} /> },
  { key: 'origin', header: 'Source', label: 'Source', cell: (p) => <Labelled t={originTerm(p.origin)} /> },
]

export function PositionsCard({ data }: { data: PositionsResponse | null }) {
  const real = (data?.positions ?? []).filter((p) => !p.is_paper)
  const open = real.filter((p) => p.state !== 'CLOSED')
  const closed = real.filter((p) => p.state === 'CLOSED')
  return (
    <Card
      title="Your positions"
      meta={data?.as_of ? <span title={istTime(data.as_of, { seconds: true })}>Updated {istTime(data.as_of)}</span> : undefined}
    >
      {data === null ? (
        <Loading route="positions" what="your positions" />
      ) : (
        <DataTable
          caption="Your open positions"
          columns={COLUMNS}
          rows={open}
          rowKey={(p) => p.position_id}
          empty="No open positions. Positions you record with /taken on Telegram appear here; trades made directly in Kite are not tracked."
          rowClass={(p) => (p.origin === 'recommended' && p.state === 'OPEN' && !p.owner_protected_at ? 'row-alert' : undefined)}
        />
      )}
      {closed.length ? (
        <details className="disclosure">
          <summary>
            <Icon name="chevron-right" size={14} />
            {closed.length} closed {closed.length === 1 ? 'position' : 'positions'}
          </summary>
          <DataTable caption="Your closed positions" columns={COLUMNS} rows={closed} rowKey={(p) => p.position_id} />
        </details>
      ) : null}
    </Card>
  )
}
