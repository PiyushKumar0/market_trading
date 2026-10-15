/** The paper book: open positions, waiting orders, recent closes. Simulated money throughout. */
import {
  closeReasonTerm,
  outcomeTerm,
  positionStateTerm,
  productTerm,
  protectionTerm,
  strategyName,
} from '../../copy/glossary.ts'
import { humanize, inr, istDay, istStamp, price, shortDate, signOf, signedInr } from '../../lib/format.ts'
import type { PaperClosed, PaperOrder, PaperPosition } from '../../types'
import { DataTable } from '../../ui/DataTable'
import type { Column } from '../../ui/DataTable'
import { Hint, Labelled } from '../../ui/Hint'
import { Pill } from '../../ui/Pill'
import type { Tone } from '../../ui/Pill'

const signCls = (v: string | null | undefined) => {
  const s = signOf(v)
  return s === 1 ? 'pos' : s === -1 ? 'neg' : undefined
}

const ROLE: Record<string, string> = { entry: 'Entry', exit: 'Exit', stop: 'Stop-loss', target: 'Target' }
const ORDER_STATE: Record<string, string> = {
  SUBMITTED: 'Sent',
  ACKED: 'Accepted',
  PARTIALLY_FILLED: 'Partly filled',
  CANCEL_PENDING: 'Cancelling',
  MODIFY_PENDING: 'Modifying',
}
const OUTCOME_TONE: Record<string, Tone> = { win: 'good', loss: 'crit', void: 'warn' }

function Strategy({ id }: { id: string | null }) {
  return id ? <Labelled t={strategyName(id)} /> : <span className="muted">—</span>
}

// A time-only (D14 exit test) position has no stop or target by design; say so instead of a bare dash.
const noLevel = (r: PaperPosition) => (r.exit_policy === 'time_only' ? <span className="muted">None (test)</span> : '—')

const POSITION_COLUMNS: Column<PaperPosition>[] = [
  {
    key: 'symbol',
    header: 'Stock',
    label: 'Stock',
    className: 'wrap-anywhere',
    cell: (r) => (
      <>
        <strong>{r.symbol}</strong>
        <span className="cell-sub">
          {r.product ? <Labelled t={productTerm(r.product)} /> : null}
          {r.state === 'PENDING_EXIT' ? (
            <>
              {r.product ? ', ' : ''}
              <Labelled t={positionStateTerm(r.state)} />
            </>
          ) : null}
        </span>
      </>
    ),
  },
  { key: 'strategy', header: 'Strategy', label: 'Strategy', cell: (r) => <Strategy id={r.strategy_id} /> },
  { key: 'qty', header: 'Qty', label: 'Qty', align: 'right', cell: (r) => r.qty ?? '—' },
  { key: 'entry', header: 'Entry', label: 'Entry', align: 'right', nowrap: true, cell: (r) => inr(r.avg_entry) ?? '—' },
  {
    key: 'mark',
    header: 'Price now',
    label: 'Price now',
    align: 'right',
    nowrap: true,
    cell: (r) => inr(r.mark) ?? <span className="muted">No live price</span>,
  },
  {
    key: 'unrealized',
    header: (
      <span className="labelled">
        Open P&amp;L
        <Hint label="Open P&L">Before costs. Costs are charged when the position closes.</Hint>
      </span>
    ),
    // Phones drop the header (and its hint): the stacked label has to say "before costs" itself.
    label: 'Open P&L (before costs)',
    align: 'right',
    nowrap: true,
    cell: (r) => <span className={signCls(r.unrealized)}>{signedInr(r.unrealized) ?? '—'}</span>,
  },
  { key: 'stop', header: 'Stop-loss', label: 'Stop-loss', align: 'right', nowrap: true, cell: (r) => inr(r.stop) ?? noLevel(r) },
  { key: 'target', header: 'Target', label: 'Target', align: 'right', nowrap: true, cell: (r) => inr(r.target) ?? noLevel(r) },
  {
    key: 'protection',
    header: 'Protection',
    label: 'Protection',
    cell: (r) =>
      r.protection_state === 'PROTECTION_FAILED' ? (
        <Pill tone="crit" icon="shield-off">
          {r.exit_policy === 'time_only' ? 'Exit failed' : protectionTerm(r.protection_state).label}
        </Pill>
      ) : (
        <Labelled t={protectionTerm(r.protection_state)} />
      ),
  },
  {
    key: 'exit',
    header: 'Planned exit',
    label: 'Planned exit',
    nowrap: true,
    cell: (r) => (r.exit_session ? shortDate(r.exit_session) : '—'),
  },
]

const ORDER_COLUMNS: Column<PaperOrder>[] = [
  { key: 'symbol', header: 'Stock', label: 'Stock', className: 'wrap-anywhere', cell: (o) => <strong>{o.symbol ?? '—'}</strong> },
  { key: 'strategy', header: 'Strategy', label: 'Strategy', cell: (o) => <Strategy id={o.strategy_id} /> },
  { key: 'role', header: 'Type', label: 'Type', cell: (o) => (o.role ? (ROLE[o.role] ?? humanize(o.role)) : '—') },
  { key: 'side', header: 'Side', label: 'Side', cell: (o) => (o.side === 'BUY' ? 'Buy' : o.side === 'SELL' ? 'Sell' : (o.side ?? '—')) },
  { key: 'qty', header: 'Qty', label: 'Qty', align: 'right', cell: (o) => o.qty ?? '—' },
  { key: 'filled', header: 'Filled', label: 'Filled', align: 'right', cell: (o) => o.filled_qty ?? '—' },
  { key: 'price', header: 'Price', label: 'Price', align: 'right', nowrap: true, cell: (o) => price(o.price) ?? '—' },
  { key: 'trigger', header: 'Trigger', label: 'Trigger', align: 'right', nowrap: true, cell: (o) => price(o.trigger_price) ?? '—' },
  {
    key: 'state',
    header: 'Status',
    label: 'Status',
    cell: (o) => (o.state ? <span title={`Engine term: ${o.state}`}>{ORDER_STATE[o.state] ?? humanize(o.state)}</span> : '—'),
  },
  { key: 'created', header: 'Placed', label: 'Placed', nowrap: true, cell: (o) => istStamp(o.created_at) },
]

const CLOSED_COLUMNS: Column<PaperClosed>[] = [
  { key: 'date', header: 'Closed', label: 'Closed', nowrap: true, cell: (c) => (c.closed_at ? shortDate(istDay(c.closed_at)) : '—') },
  { key: 'symbol', header: 'Stock', label: 'Stock', className: 'wrap-anywhere', cell: (c) => <strong>{c.symbol ?? '—'}</strong> },
  { key: 'strategy', header: 'Strategy', label: 'Strategy', cell: (c) => <Strategy id={c.strategy_id} /> },
  { key: 'qty', header: 'Qty', label: 'Qty', align: 'right', cell: (c) => c.qty ?? '—' },
  { key: 'entry', header: 'Entry', label: 'Entry', align: 'right', nowrap: true, cell: (c) => inr(c.entry_px) ?? '—' },
  { key: 'exit', header: 'Exit', label: 'Exit', align: 'right', nowrap: true, cell: (c) => inr(c.exit_px) ?? '—' },
  {
    key: 'net',
    header: (
      <span className="labelled">
        Net P&amp;L
        <Hint label="Net P&L">After costs.</Hint>
      </span>
    ),
    label: 'Net P&L (after costs)',
    align: 'right',
    nowrap: true,
    cell: (c) => <span className={signCls(c.net_pnl)}>{signedInr(c.net_pnl) ?? '—'}</span>,
  },
  {
    key: 'reason',
    header: 'Why it closed',
    label: 'Why it closed',
    cell: (c) => (
      <span title={c.close_basis ? `Basis: ${humanize(c.close_basis)}` : undefined}>
        <Labelled t={closeReasonTerm(c.close_reason, c.close_basis)} />
      </span>
    ),
  },
  {
    key: 'outcome',
    header: 'Result',
    label: 'Result',
    cell: (c) =>
      c.outcome_label ? (
        <Pill tone={OUTCOME_TONE[c.outcome_label] ?? 'neutral'}>{outcomeTerm(c.outcome_label).label}</Pill>
      ) : (
        '—'
      ),
  },
]

export function PaperPositions({ rows }: { rows: PaperPosition[] }) {
  return (
    <DataTable
      caption="Open paper positions"
      columns={POSITION_COLUMNS}
      rows={rows}
      rowKey={(r) => r.position_id}
      empty="No open paper positions."
      rowClass={(r) => (r.protection_state === 'PROTECTION_FAILED' ? 'row-alert' : undefined)}
    />
  )
}

export function PaperOrders({ rows }: { rows: PaperOrder[] }) {
  return <DataTable caption="Waiting paper orders" columns={ORDER_COLUMNS} rows={rows} rowKey={(o) => o.order_id} />
}

export function PaperClosedTrades({ rows }: { rows: PaperClosed[] }) {
  return (
    <DataTable
      caption="Closed paper trades"
      columns={CLOSED_COLUMNS}
      rows={rows}
      rowKey={(c, i) => c.position_id ?? String(i)}
      empty="No closed paper trades since the last reset."
    />
  )
}
