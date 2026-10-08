/** "Needs attention": every active alert as icon + plain title + what to do, worst first (spec §5). */
import type { Alert, Level, TabId } from '../../lib/alerts.ts'
import { Hint } from '../../ui/Hint'
import { Icon } from '../../ui/Icon'
import type { IconName } from '../../ui/Icon'

const ICON: Record<Level, IconName> = { critical: 'x-circle', warning: 'alert', notice: 'info' }
const WORD: Record<Level, string> = { critical: 'Problem', warning: 'Needs attention', notice: 'Notice' }
const OPEN: Partial<Record<TabId, string>> = {
  recommendations: 'Open recommendations',
  paper: 'Open paper trading',
  performance: 'Open performance',
  activity: 'Open activity',
  system: 'Open system',
}

export function AlertsCard({
  alerts,
  onTab,
  onSignOut,
  onAck,
  detail,
}: {
  alerts: Alert[]
  onTab: (t: TabId) => void
  onSignOut: () => void
  onAck: (ids: string[]) => void
  /** Raw per-route failure text, shown behind a hint on the load-failure alerts. */
  detail?: string | null
}) {
  const urgent = alerts.some((a) => a.level !== 'notice')
  return (
    <section id="alerts" tabIndex={-1} className="alerts" aria-labelledby="alerts-title">
      <h2 id="alerts-title" className="section-heading">
        {urgent ? 'Needs attention' : 'Notices'}
      </h2>
      <ul className="alert-list">
        {alerts.map((a) => (
          <li key={a.id} className={`alert alert-${a.level}`}>
            <Icon name={ICON[a.level]} size={20} className="alert-icon" />
            <div className="alert-main">
              <p className="alert-title">
                <span className="sr-only">{WORD[a.level]}: </span>
                {a.title}
                {detail && (a.id === 'partial' || a.id === 'unreachable') ? (
                  <Hint label="What failed">{detail}</Hint>
                ) : null}
              </p>
              <p className="alert-body">{a.body}</p>
            </div>
            <div className="alert-actions">
              {a.action === 'signout' ? (
                <button type="button" className="btn" onClick={onSignOut}>
                  Sign out
                </button>
              ) : null}
              {a.tab && OPEN[a.tab] ? (
                <button type="button" className="btn btn-quiet" onClick={() => onTab(a.tab as TabId)}>
                  {OPEN[a.tab]}
                </button>
              ) : null}
              {a.action === 'ack-telegram' && a.ackIds?.length ? (
                <button type="button" className="btn btn-quiet" onClick={() => onAck(a.ackIds ?? [])}>
                  Got it
                </button>
              ) : null}
            </div>
          </li>
        ))}
      </ul>
    </section>
  )
}
