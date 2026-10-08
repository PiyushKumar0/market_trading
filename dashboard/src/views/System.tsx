/** System tab: engine status, risk limits and the AI usage budget, in plain language. */
import type { BudgetResponse, ConnState, HeadroomResponse, ModeResponse, TradeWindowResponse } from '../types'
import { AiBudget } from './parts/AiBudget'
import { EngineStatus } from './parts/EngineStatus'
import { RiskLimits } from './parts/RiskLimits'
import './system.css'

export interface SystemProps {
  mode: ModeResponse | null
  tradeWindow: TradeWindowResponse | null
  headroom: HeadroomResponse | null
  budget: BudgetResponse | null
  conn: ConnState
  /** When a refresh last brought back any data. */
  lastOkAt: string | null
  nowMs: number
}

export function System(props: SystemProps) {
  return (
    <>
      <h1 className="view-title">System</h1>
      <EngineStatus mode={props.mode} tradeWindow={props.tradeWindow} conn={props.conn} lastOkAt={props.lastOkAt} />
      <div className="row-even">
        <RiskLimits headroom={props.headroom} />
        <AiBudget budget={props.budget} nowMs={props.nowMs} />
      </div>
    </>
  )
}
