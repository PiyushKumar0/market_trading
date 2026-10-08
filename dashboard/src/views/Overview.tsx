/**
 * Overview — the three questions, in order: is everything OK (alerts), what do I need to do (live
 * recommendations), how are we doing (your positions beside the paper autopilot), then today's
 * background numbers. Only LIVE recommendations show here; the full ledger is its own tab.
 */
import type { Alert, TabId } from '../lib/alerts.ts'
import { liveRecs } from '../lib/recs.ts'
import type { NotificationRow, Snapshot } from '../types'
import { EmptyState } from '../ui/Card'
import { Loading } from '../ui/Loading'
import { AlertsCard } from './parts/AlertsCard'
import { PaperSnapshot } from './parts/PaperSnapshot'
import { PositionsCard } from './parts/PositionsCard'
import { RecCard } from './parts/RecCard'
import { TodayStrip } from './parts/TodayStrip'
import './overview.css'

export function Overview({
  snapshot,
  alerts,
  notifications,
  nowMs,
  onTab,
  onSignOut,
  onAck,
  onRearm,
  errorDetail,
}: {
  snapshot: Snapshot
  alerts: Alert[]
  notifications: NotificationRow[]
  nowMs: number
  onTab: (t: TabId) => void
  onSignOut: () => void
  onAck: (ids: string[]) => void
  onRearm: () => void
  /** The poll's own failure text ("positions: 502 Bad Gateway | …") for the load-failure alerts. */
  errorDetail: string | null
}) {
  const all = snapshot.recommendations?.recommendations ?? []
  const live = liveRecs(all, nowMs)
  const off = snapshot.mode?.mode === 'OFF'

  return (
    <>
      <h1 className="sr-only">Overview</h1>
      {alerts.length ? (
        <AlertsCard
          alerts={alerts}
          onTab={onTab}
          onSignOut={onSignOut}
          onAck={onAck}
          onRearm={onRearm}
          detail={errorDetail}
        />
      ) : null}

      <section className="act-now" aria-labelledby="act-now-title">
        <div className="section-head">
          <h2 id="act-now-title" className="section-heading">
            Act now
          </h2>
          <span className="section-count">
            {live.length ? `${live.length} live ${live.length === 1 ? 'recommendation' : 'recommendations'}` : null}
          </span>
          {all.length ? (
            <button type="button" className="btn btn-quiet" onClick={() => onTab('recommendations')}>
              All recommendations
            </button>
          ) : null}
        </div>
        {snapshot.recommendations === null ? (
          <Loading route="recommendations" what="recommendations" />
        ) : live.length ? (
          <div className="tickets">
            {live.map((row) => (
              <RecCard key={row.rec_id} row={row} nowMs={nowMs} variant="live" />
            ))}
          </div>
        ) : (
          <EmptyState>
            {off
              ? 'Nothing to act on. The engine is off: no new trade ideas, but exit alerts for open positions still arrive.'
              : 'Nothing to act on. New recommendations appear here and on Telegram.'}
          </EmptyState>
        )}
      </section>

      <div className="row-2">
        <PositionsCard data={snapshot.positions} />
        <PaperSnapshot paper={snapshot.paper} mode={snapshot.mode?.mode ?? null} onOpen={() => onTab('paper')} />
      </div>

      <TodayStrip snapshot={snapshot} notifications={notifications} onTab={onTab} />
    </>
  )
}
