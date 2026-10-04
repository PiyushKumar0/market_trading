/**
 * Panel 4 — decision log: every Tier-1 proposal LEFT JOINed to its gate verdict (R8 provenance
 * chain). `reasons` are the rules the gate cited — for a reject/shrink those ARE the failing rules.
 *
 * Rows are split by the IST day the proposal was created on (owner-reported 2026-09-08, the same
 * complaint as the recommendations panel): today's fold starts open, earlier days start folded.
 */
import type { DecisionRow, ProposalPayload } from '../types'
import {
  Chip,
  DayFold,
  Empty,
  Panel,
  dash,
  groupByDay,
  hhmmss,
  istDay,
  todayIst,
  toneFor,
  useDayFolds,
} from './ui'

/** The identifier the proposal itself carries: enter a `tradingsymbol`, exit/modify-* a
 *  `position_id`, cancel an `order_id` (engine.core.contracts action union). The cell shows the
 *  engine-resolved `subject` and keeps this raw id in its tooltip (provenance). */
function rawId(p: ProposalPayload): string {
  return p.tradingsymbol ?? p.position_id ?? p.order_id ?? '—'
}

/** Column count of the table below — the day-fold row spans all of them. */
const COLUMNS = 6

function DecisionLine({ d }: { d: DecisionRow }) {
  return (
    <tr>
      <td className="dim">{hhmmss(d.created_at)}</td>
      <td>{dash(d.agent_id)}</td>
      <td>{dash(d.action)}</td>
      <td title={rawId(d.proposal)}>{d.subject || rawId(d.proposal)}</td>
      <td>
        {d.verdict ? <Chip v={d.verdict} tone={toneFor(d.verdict)} /> : <span className="dim">pending</span>}
      </td>
      <td style={{ whiteSpace: 'normal' }}>
        {d.reasons.length === 0 ? <span className="dim">—</span> : d.reasons.join(', ')}
      </td>
    </tr>
  )
}

export function DecisionsPanel({ rows }: { rows: DecisionRow[] }) {
  const folds = useDayFolds()
  const days = groupByDay(rows, (d) => istDay(d.created_at))
  const today = todayIst()
  const todayCount = days.find((g) => g.day === today)?.rows.length ?? 0

  return (
    <Panel
      title="Decision log"
      aside={rows.length ? `${todayCount} today · ${rows.length} shown` : undefined}
    >
      {rows.length === 0 ? (
        <Empty what="decisions" />
      ) : (
        <>
          {todayCount === 0 ? <Empty what="decisions today" /> : null}
          <table>
            <thead>
              <tr>
                <th>at</th>
                <th>agent</th>
                <th>action</th>
                <th>subject</th>
                <th>verdict</th>
                <th>rules cited</th>
              </tr>
            </thead>
            {/* One <tbody> per day: the fold header is a row of the same table, so the sticky column
                header stays the only header and the columns line up across days. */}
            {days.map((g) => (
              <tbody key={g.day || 'undated'}>
                <tr className="day">
                  <td colSpan={COLUMNS}>
                    <DayFold
                      day={g.day}
                      count={g.rows.length}
                      what="decisions"
                      open={folds.isOpen(g.day)}
                      onToggle={() => folds.toggle(g.day)}
                    />
                  </td>
                </tr>
                {folds.isOpen(g.day) ? g.rows.map((d) => <DecisionLine key={d.proposal_id} d={d} />) : null}
              </tbody>
            ))}
          </table>
        </>
      )}
    </Panel>
  )
}
