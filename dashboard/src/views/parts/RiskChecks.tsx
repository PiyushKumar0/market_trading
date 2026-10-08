/**
 * Risk checks: every Tier-1 proposal LEFT JOINed to its gate verdict (R8 provenance chain). `reasons` are
 * the rules the gate cited, so for a block or a shrink they ARE the failing rules.
 *
 * Rows are split by the IST day the proposal was created on: today's fold starts open, earlier days
 * start folded. One <tbody> per day, whose first row is the fold header, keeps the column header the
 * only header and the columns aligned across days.
 */
import { agentName, proposalActionTerm, verdictTerm } from '../../copy/glossary.ts'
import { istDay, istTime, todayIst } from '../../lib/format.ts'
import type { DecisionRow, DecisionsResponse, ProposalPayload } from '../../types'
import { Card, EmptyState } from '../../ui/Card'
import { DayFold, groupByDay, useDayFolds } from '../../ui/DayFold'
import { Labelled } from '../../ui/Hint'
import { Loading } from '../../ui/Loading'
import { Pill } from '../../ui/Pill'
import type { Tone } from '../../ui/Pill'
import { ReasonText } from './ReasonText'

/** Column count of the table below: the day-fold row spans all of them. */
const COLUMNS = 6

/** The identifier the proposal itself carries: enter a `tradingsymbol`, exit/modify-* a `position_id`,
 *  cancel an `order_id`. The cell shows the engine-resolved `subject` and keeps this id in its tooltip. */
function rawId(p: ProposalPayload): string {
  return p.tradingsymbol ?? p.position_id ?? p.order_id ?? '—'
}

function verdictTone(v: string | null): Tone {
  switch (v) {
    case 'approve':
      return 'good'
    case 'shrink':
    case 'owner_approval_required':
      return 'warn'
    case 'reject':
      return 'crit'
    default:
      return 'neutral'
  }
}

function Reasons({ rules }: { rules: string[] }) {
  return (
    <>
      {rules.map((r, i) => (
        <div key={`${i}-${r}`} className="act-reason">
          <ReasonText reason={r} />
        </div>
      ))}
    </>
  )
}

function CheckRow({ d }: { d: DecisionRow }) {
  const id = rawId(d.proposal)
  const v = verdictTerm(d.verdict)
  const pv = d.paper_verdict
  const paperRules = pv?.reasons ?? []
  return (
    <tr>
      <td data-label="Time" className="nowrap">
        <span title={istTime(d.created_at, { seconds: true })}>{istTime(d.created_at)}</span>
      </td>
      <td data-label="Analyst">
        <Labelled t={agentName(d.agent_id)} />
      </td>
      <td data-label="Proposal">
        <Labelled t={proposalActionTerm(d.action)} />
      </td>
      <td data-label="Stock" className="act-stock">
        <span title={`Engine id: ${id}`}>{d.subject || id}</span>
      </td>
      <td data-label="Result">
        <div className="act-result">
          <Pill tone={verdictTone(d.verdict)} title={v.hint}>
            {v.label}
          </Pill>
          {pv ? <Pill tone="sim">{`Paper: ${verdictTerm(pv.verdict).label}`}</Pill> : null}
        </div>
      </td>
      <td data-label="Why" className="act-why">
        {d.reasons.length === 0 && paperRules.length === 0 ? <span className="muted">—</span> : null}
        <Reasons rules={d.reasons} />
        {paperRules.length ? (
          <div className="act-why-paper">
            <span className="muted">Paper:</span>
            <Reasons rules={paperRules} />
          </div>
        ) : null}
      </td>
    </tr>
  )
}

export function RiskChecks({ decisions }: { decisions: DecisionsResponse | null }) {
  const folds = useDayFolds()
  const rows = decisions?.decisions ?? []
  const days = groupByDay(rows, (d) => istDay(d.created_at))
  const todayCount = days.find((g) => g.day === todayIst())?.rows.length ?? 0

  return (
    <Card
      id="act-risk"
      title="Risk checks"
      meta={rows.length ? `${todayCount} today, ${rows.length} shown` : undefined}
    >
      <p className="muted act-lede">The latest trades the analysts proposed, and what the risk check decided.</p>
      {decisions === null ? (
        <Loading route="decisions" what="risk checks" />
      ) : rows.length === 0 ? (
        <EmptyState>No risk checks yet. Every trade an analyst proposes appears here with the risk check's answer.</EmptyState>
      ) : (
        <>
          {todayCount === 0 ? <p className="muted act-none">No risk checks today.</p> : null}
          <div className="table-scroll" role="region" aria-label="Risk checks by day" tabIndex={0}>
            <table className="dt">
              <caption className="sr-only">Risk checks, newest day first</caption>
              <thead>
                <tr>
                  <th scope="col">Time</th>
                  <th scope="col">Analyst</th>
                  <th scope="col">Proposal</th>
                  <th scope="col">Stock</th>
                  <th scope="col">Result</th>
                  <th scope="col">Why</th>
                </tr>
              </thead>
              {days.map((g) => (
                <tbody key={g.day || 'undated'}>
                  <tr className="act-fold-row">
                    <td colSpan={COLUMNS}>
                      <DayFold
                        day={g.day}
                        count={g.rows.length}
                        noun={['check', 'checks']}
                        open={folds.isOpen(g.day)}
                        onToggle={() => folds.toggle(g.day)}
                      />
                    </td>
                  </tr>
                  {folds.isOpen(g.day) ? g.rows.map((d) => <CheckRow key={d.proposal_id} d={d} />) : null}
                </tbody>
              ))}
            </table>
          </div>
        </>
      )}
    </Card>
  )
}
