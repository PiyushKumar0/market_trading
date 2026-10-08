/** One recommendation in the ledger: a one-line summary that opens into the full ticket. A live one
 *  starts open — it is something to act on, not history. */
import { useState } from 'react'
import { verdictTerm } from '../../copy/glossary.ts'
import { istTime } from '../../lib/format.ts'
import { actionState, instruction, isLive } from '../../lib/recs.ts'
import type { RecommendationRow } from '../../types'
import { Icon } from '../../ui/Icon'
import { Pill, SideBadge } from '../../ui/Pill'
import type { Tone } from '../../ui/Pill'
import { RecCard } from './RecCard'

const VERDICT_TONE: Record<string, Tone> = {
  approve: 'good',
  shrink: 'warn',
  reject: 'crit',
  owner_approval_required: 'warn',
}

export function RecRow({ row, nowMs }: { row: RecommendationRow; nowMs: number }) {
  const live = isLive(row, nowMs)
  const [open, setOpen] = useState(live)
  const rec = row.recommendation
  const action = actionState(row, nowMs)
  const verdict = rec?.gate?.verdict ?? null

  return (
    <div className={open ? 'rec-row open' : 'rec-row'}>
      <button type="button" className="rec-row-head" aria-expanded={open} onClick={() => setOpen((o) => !o)}>
        <Icon name={open ? 'chevron-down' : 'chevron-right'} size={16} />
        <span className="rec-row-time num" title={istTime(row.delivered_at ?? rec?.created_at, { seconds: true })}>
          {istTime(row.delivered_at ?? rec?.created_at)}
        </span>
        <SideBadge side={rec?.side} kind={rec?.kind} />
        <span className="rec-row-sym">{rec?.instrument ?? row.rec_id}</span>
        <span className="rec-row-what">{instruction(rec)}</span>
        <span className="rec-row-pills">
          <Pill tone={VERDICT_TONE[verdict ?? ''] ?? 'neutral'}>{verdictTerm(verdict).label}</Pill>
          <Pill tone={action.tone}>{action.label}</Pill>
        </span>
      </button>
      {open ? (
        <div className="rec-row-body">
          <RecCard row={row} nowMs={nowMs} variant={live ? 'live' : 'history'} />
        </div>
      ) : null}
    </div>
  )
}
