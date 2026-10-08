/** System tab — STUB, replaced in Task 10. Props are final. */
import type { BudgetResponse, ConnState, HeadroomResponse, ModeResponse, TradeWindowResponse } from '../types'
import { EmptyState } from '../ui/Card'

export interface SystemProps {
  mode: ModeResponse | null
  tradeWindow: TradeWindowResponse | null
  headroom: HeadroomResponse | null
  budget: BudgetResponse | null
  conn: ConnState
  lastPollAt: string | null
  nowMs: number
}

export function System(props: SystemProps) {
  void props
  return (
    <>
      <h1 className="view-title">System</h1>
      <EmptyState>This section is being rebuilt.</EmptyState>
    </>
  )
}
