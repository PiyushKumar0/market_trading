/**
 * Panel 6 — risk headroom (§7.1): equity, day MTM, open-position counts by product, and the
 * consecutive-loss count, each against its `limits.yaml` cap when `limits_engine` is wired.
 */
import type { HeadroomResponse } from '../types'
import { Chip, Empty, Panel } from './ui'

function Row({ k, v, cap }: { k: string; v: string | number | undefined; cap?: string | number }) {
  return (
    <tr>
      <td className="dim">{k}</td>
      <td className="num">{v ?? '—'}</td>
      <td className="num dim">{cap !== undefined ? `/ ${cap}` : ''}</td>
    </tr>
  )
}

export function HeadroomPanel({ data }: { data: HeadroomResponse | null }) {
  const h = data?.headroom
  const caps = h?.caps
  const open = h?.open_positions
  const mtm = h?.day_mtm !== undefined ? Number(h.day_mtm) : null

  return (
    <Panel title="Risk headroom">
      {!h || Object.keys(h).length === 0 ? (
        <Empty what="exposure data" />
      ) : (
        <>
          <div className="row" style={{ marginBottom: 6 }}>
            <Chip k="equity" v={`₹${h.equity ?? '—'}`} />
            <Chip
              k="day mtm"
              v={`₹${h.day_mtm ?? '—'}`}
              tone={mtm === null || mtm === 0 ? 'neutral' : mtm > 0 ? 'ok' : 'bad'}
            />
          </div>
          <table>
            <tbody>
              <Row k="deployed capital" v={h.deployed_capital} cap={caps?.max_deployed_capital_inr} />
              <Row k="open positions" v={open?.total} cap={caps?.max_open_positions_total} />
              <Row k="open MIS" v={open?.mis} cap={caps?.max_open_positions_mis} />
              <Row k="open CNC" v={open?.cnc} cap={caps?.max_open_positions_cnc} />
              <Row
                k="consecutive losses"
                v={h.consecutive_losses}
                cap={caps?.consecutive_losses_max_per_session}
              />
              {caps ? <Row k="max new trades / day" v={caps.max_new_trades_day} /> : null}
            </tbody>
          </table>
        </>
      )}
    </Panel>
  )
}
