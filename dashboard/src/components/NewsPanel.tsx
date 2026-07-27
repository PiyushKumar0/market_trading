/**
 * Panel 7 — today's §2.7 catalyst watchlist, split ORIGINATING (may license a `cat` entry) vs
 * CONTEXT (colour only, never originates), with the digest freshness the fail-safe ladder keys off:
 * `stale` or `missing` disables `cat` for the day.
 */
import type { WatchlistResponse, WatchlistRow } from '../types'
import { Chip, Empty, Panel, dash, toneFor } from './ui'

function Rows({ rows }: { rows: WatchlistRow[] }) {
  return (
    <table>
      <thead>
        <tr>
          <th>symbol</th>
          <th>event</th>
          <th>dir</th>
          <th className="num">matl</th>
          <th className="num">src</th>
          <th className="num">trigger</th>
          <th className="num">invalidation</th>
          <th className="num">stop band</th>
          <th className="num">target band</th>
        </tr>
      </thead>
      <tbody>
        {rows.map((r) => (
          <tr key={r.entry_id}>
            <td>{r.symbol}</td>
            <td>{dash(r.event_type)}</td>
            <td>{dash(r.direction)}</td>
            <td className="num">{r.materiality != null ? r.materiality.toFixed(2) : '—'}</td>
            <td className="num">{dash(r.source_domain_count)}</td>
            <td className="num">{dash(r.confirm_trigger)}</td>
            <td className="num">{dash(r.invalidation)}</td>
            <td className="num">
              {r.stop_band_low && r.stop_band_high ? `${r.stop_band_low}–${r.stop_band_high}` : '—'}
            </td>
            <td className="num">
              {r.target_band_low && r.target_band_high
                ? `${r.target_band_low}–${r.target_band_high}`
                : '—'}
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  )
}

export function NewsPanel({ data }: { data: WatchlistResponse | null }) {
  const rows = data?.watchlist ?? []
  const originating = rows.filter((r) => r.grade === 'originating')
  const context = rows.filter((r) => r.grade !== 'originating')
  const digest = data?.digest

  return (
    <Panel title="News / catalyst watchlist" aside={data?.d ?? undefined} wide>
      <div className="row" style={{ marginBottom: 6 }}>
        <Chip k="digest" v={digest?.status ?? 'unknown'} tone={toneFor(digest?.status)} />
        <Chip k="as of" v={digest?.as_of ?? '—'} />
        <Chip
          k="age"
          v={digest?.age_h != null ? `${digest.age_h}h / ${digest.stale_max_h ?? '—'}h` : '—'}
        />
      </div>

      <div className="dim" style={{ margin: '6px 0 2px' }}>
        originating ({originating.length}) — may license a `cat` entry
      </div>
      {originating.length === 0 ? <Empty what="originating catalysts" /> : <Rows rows={originating} />}

      <div className="dim" style={{ margin: '10px 0 2px' }}>
        context ({context.length}) — never originates
      </div>
      {context.length === 0 ? <Empty what="context catalysts" /> : <Rows rows={context} />}
    </Panel>
  )
}
