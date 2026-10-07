/**
 * Panel — per-strategy scorecard (`GET /scorecard`, plan Q2.4/Q2.5). The `recs` columns are HINDSIGHT
 * outcomes of delivered entry recommendations on official bars, never platform equity; the engine's
 * own label is shown above the table. The `paper` columns are simulated trades of the current paper
 * epoch (voids excluded), net in rupees — not hindsight.
 */
import type { ScorecardState } from '../hooks'
import { Chip, Empty, Panel, dash } from './ui'

const pct = (v: number | null) => (v === null ? null : `${v.toFixed(2)}%`)
const rupees = (v: number | null) => (v === null ? null : `₹${v.toFixed(0)}`)
const rate = (v: number | null) => (v === null ? null : `${(v * 100).toFixed(0)}%`)
const counts = (m: Record<string, number>) =>
  Object.entries(m)
    .map(([k, n]) => `${k} ${n}`)
    .join(' · ')

export function ScorecardPanel({ data, error, unauthorized }: ScorecardState) {
  const rows = Object.entries(data?.strategies ?? {})
  return (
    <Panel title="Scorecard" aside={data ? `${rows.length} strateg${rows.length === 1 ? 'y' : 'ies'}` : undefined} wide>
      {unauthorized ? (
        <div className="err">token rejected (401) — sign out (header, top right) and re-enter the dashboard token</div>
      ) : error ? (
        <div className="err">scorecard unavailable: {error}</div>
      ) : null}

      {data ? (
        <div className="row">
          <Chip v={data.label} tone="warn" />
          {Object.values(data.bench).map((b) => (
            <Chip key={b} k="bench" v={b} />
          ))}
        </div>
      ) : null}

      {rows.length === 0 ? (
        !unauthorized && <Empty what="scored recommendations" />
      ) : (
        <table>
          <thead>
            <tr>
              <th>strategy</th>
              <th className="num">recs</th>
              <th className="num">filled</th>
              <th className="num">closed</th>
              <th className="num">hit</th>
              <th className="num">median net</th>
              <th className="num">mean net</th>
              <th className="num">T+20 net</th>
              <th className="num">excess</th>
              <th>owner action</th>
              <th className="num">paper closed</th>
              <th className="num">paper hit</th>
              <th className="num">paper net ₹</th>
              <th className="num">paper open</th>
            </tr>
          </thead>
          <tbody>
            {rows.map(([sid, { recs, paper }]) => (
              <tr key={sid}>
                <td>{sid}</td>
                <td className="num">{recs.n}</td>
                <td className="num">{recs.filled}</td>
                <td className="num">{recs.closed}</td>
                <td className="num">{dash(rate(recs.hit_rate))}</td>
                <td className="num">{dash(pct(recs.median_net))}</td>
                <td className="num">{dash(pct(recs.mean_net))}</td>
                <td className="num">{dash(pct(recs.net_t20))}</td>
                <td className="num">{dash(pct(recs.mean_excess))}</td>
                <td title={counts(recs.skip_reasons)}>{dash(counts(recs.actions))}</td>
                <td className="num">{paper.closed}</td>
                <td className="num">{dash(rate(paper.hit_rate))}</td>
                <td className="num">{dash(rupees(paper.net))}</td>
                <td className="num">{paper.open}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </Panel>
  )
}
