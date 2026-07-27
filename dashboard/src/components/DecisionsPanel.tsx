/**
 * Panel 4 — decision log: every Tier-1 proposal LEFT JOINed to its gate verdict (R8 provenance
 * chain). `reasons` are the rules the gate cited — for a reject/shrink those ARE the failing rules.
 */
import type { DecisionRow, ProposalPayload } from '../types'
import { Chip, Empty, Panel, dash, hhmmss, toneFor } from './ui'

/** The proposal's subject: enter carries `tradingsymbol`, exit/modify-* a `position_id`, cancel an
 *  `order_id` (engine.core.contracts action union). */
function subject(p: ProposalPayload): string {
  return p.tradingsymbol ?? p.position_id ?? p.order_id ?? '—'
}

export function DecisionsPanel({ rows }: { rows: DecisionRow[] }) {
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
                <td>{subject(d.proposal)}</td>
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
