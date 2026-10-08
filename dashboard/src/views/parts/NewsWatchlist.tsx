/**
 * News watchlist: today's catalyst watchlist, split into news that can start a trade and background
 * news, with the freshness of the news summary the engine keys its fail-safe off (a stale or missing
 * summary switches news-catalyst entries off for the day).
 */
import { digestTerm, gradeTerm } from '../../copy/glossary.ts'
import { dayLabel, humanize, istStamp, price } from '../../lib/format.ts'
import type { WatchlistResponse, WatchlistRow } from '../../types'
import { Card } from '../../ui/Card'
import { DataTable } from '../../ui/DataTable'
import type { Column } from '../../ui/DataTable'
import { Hint, Labelled } from '../../ui/Hint'
import { Pill } from '../../ui/Pill'
import type { Tone } from '../../ui/Pill'

const range = (low: string | null, high: string | null): string => {
  const a = price(low)
  const b = price(high)
  return a && b ? `${a}–${b}` : '—'
}

/** A column header whose word opens its one-line explanation (dotted underline, no ⓘ). */
const head = (label: string, hint: string) => (
  <Hint label={label} asTerm>
    {hint}
  </Hint>
)

const COLUMNS: Column<WatchlistRow>[] = [
  { key: 'symbol', header: 'Stock', label: 'Stock', className: 'wrap-anywhere', cell: (r) => <strong>{r.symbol}</strong> },
  { key: 'event', header: 'Event', label: 'Event', cell: (r) => (r.event_type ? humanize(r.event_type) : '—') },
  {
    key: 'direction',
    header: 'Direction',
    label: 'Direction',
    cell: (r) => (r.direction === 'long' ? 'Up' : r.direction === 'short' ? 'Down' : '—'),
  },
  {
    key: 'materiality',
    header: head('Importance', 'How much the news matters, from 0 to 1.'),
    label: 'Importance',
    align: 'right',
    cell: (r) => (r.materiality != null ? r.materiality.toFixed(2) : '—'),
  },
  {
    key: 'sources',
    header: head('Sources', 'How many separate news sites reported it.'),
    label: 'Sources',
    align: 'right',
    cell: (r) => r.source_domain_count ?? '—',
  },
  {
    key: 'trigger',
    header: head('Trigger price', 'A move past this price confirms the idea.'),
    label: 'Trigger price',
    align: 'right',
    nowrap: true,
    cell: (r) => price(r.confirm_trigger) ?? '—',
  },
  {
    key: 'cancel',
    header: head('Cancel price', 'A move past this price cancels the idea.'),
    label: 'Cancel price',
    align: 'right',
    nowrap: true,
    cell: (r) => price(r.invalidation) ?? '—',
  },
  {
    key: 'stop',
    header: 'Stop range',
    label: 'Stop range',
    align: 'right',
    nowrap: true,
    cell: (r) => range(r.stop_band_low, r.stop_band_high),
  },
  {
    key: 'target',
    header: 'Target range',
    label: 'Target range',
    align: 'right',
    nowrap: true,
    cell: (r) => range(r.target_band_low, r.target_band_high),
  },
]

function Group({ grade, caption, rows }: { grade: 'originating' | 'context'; caption: string; rows: WatchlistRow[] }) {
  return (
    <div className="act-group">
      <h3 className="act-group-title">
        <Labelled t={gradeTerm(grade)} />
        <span className="act-count">{rows.length}</span>
      </h3>
      {rows.length === 0 ? (
        <p className="muted act-none">None today.</p>
      ) : (
        <DataTable caption={caption} columns={COLUMNS} rows={rows} rowKey={(r) => r.entry_id} />
      )}
    </div>
  )
}

export function NewsWatchlist({ data }: { data: WatchlistResponse | null }) {
  const rows = data?.watchlist ?? []
  const originating = rows.filter((r) => r.grade === 'originating')
  const background = rows.filter((r) => r.grade !== 'originating')
  const digest = data?.digest
  const term = digestTerm(digest?.status)
  const tone: Tone = digest?.status === 'fresh' ? 'good' : digest?.status ? 'warn' : 'neutral'

  return (
    <Card id="act-news" tier="secondary" title="News watchlist" meta={data?.d ? dayLabel(data.d) : undefined}>
      {data === null ? (
        <p className="empty">Loading…</p>
      ) : (
        <>
          <div className="act-digest">
            <span className="labelled">
              <Pill tone={tone}>{`News summary: ${term.label}`}</Pill>
              <Hint label="news summary" term={term.term}>
                {term.hint}
              </Hint>
            </span>
            {digest?.as_of ? <span className="muted">updated {istStamp(digest.as_of)}</span> : null}
            {digest?.age_h != null && digest.stale_max_h != null ? (
              <span className="muted">
                ({digest.age_h.toFixed(1)} hours old, limit {digest.stale_max_h} hours)
              </span>
            ) : null}
          </div>
          <Group grade="originating" caption="News that can start a trade" rows={originating} />
          <Group grade="context" caption="Background news" rows={background} />
        </>
      )}
    </Card>
  )
}
