/**
 * Panel 5 — SDK-call budget (§5.6): per-agent month spend against its allocation, plus the degrade
 * tier the governor is currently on. USD values arrive as strings; they are parsed ONLY to size the
 * bar — the number printed is always the string the engine sent.
 */
import type { BudgetResponse } from '../types'
import { Chip, Empty, Meter, Panel, toneFor } from './ui'

export function BudgetPanel({ data }: { data: BudgetResponse | null }) {
  const allocations = data?.budget.allocations_usd ?? {}
  const spend = data?.budget.per_agent_spend_usd ?? {}
  const agents = Object.keys(allocations).sort()

  return (
    <Panel
      title="Agent budget"
      aside={data?.budget.month_spend_usd ? `month $${data.budget.month_spend_usd}` : undefined}
    >
      <div className="row" style={{ marginBottom: 6 }}>
        <Chip k="degrade tier" v={data?.degrade_tier ?? '—'} tone={toneFor(data?.degrade_tier)} />
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
                  <td className="num">${alloc}</td>
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
