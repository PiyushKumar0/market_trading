/**
 * Panel 5 — SDK-call budget (§5.6): per-agent spend for the current quota WEEK (Thu 14:00 IST reset,
 * not the calendar month) against its weekly allocation, plus the degrade tier the governor is
 * currently on. USD values arrive as strings; they are parsed ONLY to size the bar — the number
 * printed is always the string the engine sent.
 */
import type { BudgetResponse } from '../types'
import { Chip, Empty, Meter, Panel, toneFor } from './ui'

export function BudgetPanel({ data }: { data: BudgetResponse | null }) {
  const budget = data?.budget
  const allocations = budget?.allocations_usd ?? {}
  const spend = budget?.per_agent_spend_usd ?? {}
  // Union, not the allocation keys alone: an agent that billed this window without an allocation
  // (sdk_smoke) is inside the `week $X` aside, so it must have a row or the table contradicts it.
  const agents = Object.keys({ ...allocations, ...spend }).sort()
  const spent = budget?.window_spend_usd
  const aside = spent
    ? `week $${spent}${budget?.credit_usd ? ` / $${budget.credit_usd}` : ''}`
    : undefined

  return (
    <Panel title="Agent budget" aside={aside}>
      <div className="row" style={{ marginBottom: 6 }}>
        <Chip k="degrade tier" v={data?.degrade_tier ?? '—'} tone={toneFor(data?.degrade_tier)} />
        <Chip k="week from" v={budget?.window_key ?? '—'} />
        <Chip k="forward cap" v={budget?.forward_cap != null ? String(budget.forward_cap) : '—'} />
      </div>
      {agents.length === 0 ? (
        <Empty what="budget data" />
      ) : (
        <table>
          <thead>
            <tr>
              <th>agent</th>
              <th className="num">spent</th>
              <th className="num">alloc</th>
              <th>usage</th>
            </tr>
          </thead>
          <tbody>
            {agents.map((agent) => {
              const spent = spend[agent] ?? '0'
              const alloc = allocations[agent]
              const cap = Number(alloc)
              return (
                <tr key={agent}>
                  <td>{agent}</td>
                  <td className="num">${spent}</td>
                  <td className="num">{alloc ? `$${alloc}` : '—'}</td>
                  <td>
                    <Meter fraction={cap > 0 ? Number(spent) / cap : 0} />
                  </td>
                </tr>
              )
            })}
          </tbody>
        </table>
      )}
    </Panel>
  )
}
