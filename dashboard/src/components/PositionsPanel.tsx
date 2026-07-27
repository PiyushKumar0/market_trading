/** Panel 2 — all-origin positions (platform / external / recommended), OPEN first (O5/R8). */
import type { PositionsResponse } from '../types'
import { Chip, Empty, Panel, dash, hhmmss, toneFor } from './ui'

export function PositionsPanel({ data }: { data: PositionsResponse | null }) {
  const rows = data?.positions ?? []
  return (
    <Panel title="Positions" aside={data?.as_of ? `as of ${hhmmss(data.as_of)}` : undefined}>
      {rows.length === 0 ? (
        <Empty what="positions" />
      ) : (
        <table>
          <thead>
            <tr>
              <th>symbol</th>
              <th>side</th>
              <th className="num">qty</th>
              <th className="num">avg</th>
              <th className="num">stop</th>
              <th className="num">target</th>
              <th>state</th>
              <th>origin</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((p) => (
              <tr key={p.position_id}>
                <td>
                  {p.symbol}
                  {p.product ? <span className="dim"> {p.product}</span> : null}
                </td>
                <td>{dash(p.side)}</td>
                <td className="num">{dash(p.qty)}</td>
                <td className="num">{dash(p.avg_entry)}</td>
                <td className="num">{dash(p.stop)}</td>
                <td className="num">{dash(p.target)}</td>
                <td>
                  <Chip v={p.state ?? '—'} tone={toneFor(p.state)} />
                </td>
                <td>
                  {p.origin}
                  {p.is_paper ? <span className="dim"> paper</span> : null}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </Panel>
  )
}
