/**
 * Panel 4 — decision log: every Tier-1 proposal LEFT JOINed to its gate verdict (R8 provenance
 * chain). `reasons` are the rules the gate cited — for a reject/shrink those ARE the failing rules.
 */
import type { DecisionRow, PositionRow, ProposalPayload } from '../types'
import { Chip, Empty, Panel, dash, hhmmss, toneFor } from './ui'

/** The identifier the proposal itself carries: enter a `tradingsymbol`, exit/modify-* a
 *  `position_id`, cancel an `order_id` (engine.core.contracts action union). */
function rawId(p: ProposalPayload): string {
  return p.tradingsymbol ?? p.position_id ?? p.order_id ?? '—'
}

/** What the subject column shows. The engine resolves ids to symbols (`subject`); an engine that
 *  predates that field gets the same answer from the positions snapshot, and an id nothing resolves
 *  stays visible as itself. The raw id is kept in the cell's tooltip either way (provenance). */
function subjectOf(d: DecisionRow, symbolByPosition: Map<string, string>): string {
  if (d.subject) return d.subject
  const pid = d.proposal.position_id
  return (pid && symbolByPosition.get(pid)) || rawId(d.proposal)
}

export function DecisionsPanel({ rows, positions = [] }: { rows: DecisionRow[]; positions?: PositionRow[] }) {
  const symbolByPosition = new Map(positions.map((p) => [p.position_id, p.symbol]))
  return (
    <Panel title="Decision log" aside={rows.length ? `${rows.length}` : undefined}>
      {rows.length === 0 ? (
        <Empty what="decisions" />
      ) : (
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
          <tbody>
            {rows.map((d) => (
              <tr key={d.proposal_id}>
                <td className="dim">{hhmmss(d.created_at)}</td>
                <td>{dash(d.agent_id)}</td>
                <td>{dash(d.action)}</td>
                <td title={rawId(d.proposal)}>{subjectOf(d, symbolByPosition)}</td>
                <td>
                  {d.verdict ? (
                    <Chip v={d.verdict} tone={toneFor(d.verdict)} />
                  ) : (
                    <span className="dim">pending</span>
                  )}
                </td>
                <td style={{ whiteSpace: 'normal' }}>
                  {d.reasons.length === 0 ? <span className="dim">—</span> : d.reasons.join(', ')}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </Panel>
  )
}
