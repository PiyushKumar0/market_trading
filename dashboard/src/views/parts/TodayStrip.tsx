/** One quiet row of today's background numbers, each a link to the tab with the detail. */
import { digestTerm, tierTerm } from '../../copy/glossary.ts'
import type { TabId } from '../../lib/alerts.ts'
import { istDay, todayIst } from '../../lib/format.ts'
import type { NotificationRow, Snapshot } from '../../types'

function spentPct(snapshot: Snapshot): string | null {
  const b = snapshot.budget?.budget
  const spent = Number(b?.window_spend_usd)
  const credit = Number(b?.credit_usd)
  if (!b || !Number.isFinite(spent) || !Number.isFinite(credit) || credit <= 0) return null
  return `${Math.round((100 * spent) / credit)}%`
}

export function TodayStrip({
  snapshot,
  notifications,
  onTab,
}: {
  snapshot: Snapshot
  notifications: NotificationRow[]
  onTab: (t: TabId) => void
}) {
  const today = todayIst()
  const checks = (snapshot.decisions?.decisions ?? []).filter((d) => istDay(d.created_at) === today)
  const blocked = checks.filter((d) => d.verdict === 'reject').length
  const undelivered = notifications.filter((n) => n.status !== 'delivered').length
  const digest = snapshot.watchlist ? digestTerm(snapshot.watchlist.digest.status) : null
  const tier = snapshot.budget?.degrade_tier ?? null
  const pct = spentPct(snapshot)

  const items: { tab: TabId; k: string; v: string }[] = [
    { tab: 'activity', k: 'Risk checks', v: `${checks.length} today${blocked ? `, ${blocked} blocked` : ''}` },
    {
      tab: 'activity',
      k: 'Telegram',
      v: notifications.length
        ? `${notifications.length} sent${undelivered ? `, ${undelivered} not confirmed` : ', all delivered'}`
        : 'nothing sent yet',
    },
    { tab: 'activity', k: 'News summary', v: digest ? digest.label.toLowerCase() : '—' },
    {
      tab: 'system',
      k: 'AI budget',
      v: pct ? `${pct} used${tier ? `, ${tierTerm(tier).label.toLowerCase()}` : ''}` : tier ? tierTerm(tier).label : '—',
    },
  ]

  return (
    <section className="today" aria-labelledby="today-title">
      <h2 id="today-title" className="section-heading">
        Today
      </h2>
      <div className="today-links">
        {items.map((it) => (
          <button key={it.k} type="button" className="today-link" onClick={() => onTab(it.tab)}>
            <span className="today-k">{it.k}</span>
            <span className="today-v">{it.v}</span>
          </button>
        ))}
      </div>
    </section>
  )
}
