/**
 * Recommendations — the full ledger (latest 100 deliveries), split by the IST day it was delivered on
 * (owner directive 2026-09-08): today's day starts open, earlier days start folded — EXCEPT a day still
 * holding a LIVE recommendation, which starts open too (WO-V 2026-09-13: a swing entry stays valid to
 * the next session's close, so an actionable one can sit under yesterday's date).
 */
import { istDay, todayIst } from '../lib/format.ts'
import { isLive } from '../lib/recs.ts'
import type { RecommendationRow, RecommendationsResponse } from '../types'
import { EmptyState } from '../ui/Card'
import { DayFold, groupByDay, useDayFolds } from '../ui/DayFold'
import { RecRow } from './parts/RecRow'
import './recommendations.css'

/** `delivered_at` is the ledger's own stamp; the payload's `created_at` only stands in for a row
 *  journalled before its delivery completed. */
const dayOf = (row: RecommendationRow) => istDay(row.delivered_at ?? row.recommendation?.created_at)

export function Recommendations({ data, nowMs }: { data: RecommendationsResponse | null; nowMs: number }) {
  const rows = data?.recommendations ?? []
  const days = groupByDay(rows, dayOf)
  const liveDays = new Set(days.filter((g) => g.rows.some((r) => isLive(r, nowMs))).map((g) => g.day))
  const folds = useDayFolds((day) => liveDays.has(day))
  const today = todayIst()
  const todayCount = days.find((g) => g.day === today)?.rows.length ?? 0
  const waiting = rows.filter((r) => isLive(r, nowMs)).length
  const liveEarlier = rows.filter((r) => isLive(r, nowMs) && dayOf(r) !== today).length

  return (
    <>
      <div className="view-head">
        <h1 className="view-title">Recommendations</h1>
        {rows.length ? (
          <p className="view-sub">
            {rows.length} shown, {todayCount} today, {waiting} waiting for you
          </p>
        ) : null}
      </div>

      {data === null ? (
        <EmptyState>Loading recommendations…</EmptyState>
      ) : rows.length === 0 ? (
        <EmptyState>No recommendations yet. Every trade idea the engine sends you on Telegram is also kept here.</EmptyState>
      ) : (
        <div className="rec-ledger">
          {todayCount === 0 ? (
            <p className="muted">
              {liveEarlier
                ? `Nothing delivered today; ${liveEarlier} from an earlier session can still be acted on.`
                : 'Nothing delivered today.'}
            </p>
          ) : null}
          {days.map((g) => {
            const liveHere = g.rows.filter((r) => isLive(r, nowMs)).length
            return (
              <section className="rec-day" key={g.day || 'undated'}>
                <DayFold
                  day={g.day}
                  count={g.rows.length}
                  noun={['recommendation', 'recommendations']}
                  open={folds.isOpen(g.day)}
                  onToggle={() => folds.toggle(g.day)}
                  extra={liveHere ? `${liveHere} still live` : undefined}
                />
                {folds.isOpen(g.day) ? (
                  <ul className="rec-rows">
                    {g.rows.map((row) => (
                      <li key={row.rec_id}>
                        <RecRow row={row} nowMs={nowMs} />
                      </li>
                    ))}
                  </ul>
                ) : null}
              </section>
            )
          })}
        </div>
      )}
    </>
  )
}
