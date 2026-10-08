/**
 * Performance tab: the per-strategy scorecard (`GET /scorecard`). Recommendation columns are hindsight
 * outcomes on official prices, never real profits; paper columns are simulated trades.
 */
import './performance.css'
import type { ScorecardState } from '../hooks'
import { Card, EmptyState } from '../ui/Card'
import { Hint } from '../ui/Hint'
import { Scorecard } from './parts/Scorecard'

export function Performance({ data, error, unauthorized }: ScorecardState) {
  const count = data ? Object.keys(data.strategies).length : 0
  const hasRows = count > 0
  return (
    <>
      <h1 className="view-title">Performance</h1>
      <Card title="Strategy performance" meta={hasRows ? `${count} ${count === 1 ? 'strategy' : 'strategies'}` : undefined}>
        {unauthorized ? (
          <p className="perf-error">Your dashboard token was rejected. Sign out (top right) and enter the current token.</p>
        ) : data === null ? (
          error ? (
            <p className="perf-error">The scorecard couldn't load: {error}. It retries every minute.</p>
          ) : (
            <p className="empty">Loading the scorecard…</p>
          )
        ) : (
          <>
            <p className="perf-note">
              Recommendations are scored in hindsight on official prices, as if each was taken at its suggested entry.
              These are not your real profits.
              <span className="labelled">
                <Hint label="Hindsight scoring" term={data.label}>
                  The engine replays each delivered entry recommendation on the official daily prices to see how it would
                  have done.
                </Hint>
              </span>
            </p>
            {hasRows ? (
              <Scorecard data={data} />
            ) : (
              <EmptyState>
                No scored recommendations yet. Results appear once delivered entry recommendations reach their exit.
              </EmptyState>
            )}
            {error ? <p className="perf-stale">The last refresh failed: {error}</p> : null}
          </>
        )}
      </Card>
    </>
  )
}
