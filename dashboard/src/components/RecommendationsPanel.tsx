/**
 * Panel 3 — recommendations (§3.6). Each card shows the delivered artifact: thesis, entry zone,
 * stop/targets, gate-approved qty, the gate verdict, the B7/R3 manual protective-order checklist the
 * human must work through, and the owner's recorded `human_action` (taken | expired | dismissed |
 * closed — set through Telegram `/taken` `/closed`, never from here: this dashboard is read-only over
 * the recommendation ledger).
 *
 * Rows are enriched with the matching `/decisions` provenance (agent + gate reasons) where the
 * recommendation's own gate payload is absent.
 */
import type { DecisionRow, RecommendationRow } from '../types'
import { Chip, Empty, Panel, dash, hhmmss, toneFor } from './ui'

function reasonsFor(rec: RecommendationRow, decisions: DecisionRow[]): string[] {
  const own = rec.recommendation?.gate?.reasons
  if (own && own.length > 0) return own
  // Fall back to the provenance view (/decisions is ordered newest-first): the most recent judged
  // proposal on the same instrument.
  const match = decisions.find(
    (d) => d.proposal.tradingsymbol === rec.recommendation?.instrument && d.verdict !== null,
  )
  return match?.reasons ?? []
}

export function RecommendationsPanel({
  rows,
  decisions,
}: {
  rows: RecommendationRow[]
  decisions: DecisionRow[]
}) {
  return (
    <Panel title="Recommendations" aside={rows.length ? `${rows.length}` : undefined} wide>
      {rows.length === 0 ? (
        <Empty what="recommendations" />
      ) : (
        rows.map((row) => {
          const rec = row.recommendation
          const gate = rec?.gate
          const zone = rec?.entry_zone
          const reasons = reasonsFor(row, decisions)
          return (
            <div className="rec" key={row.rec_id}>
              <div className="row">
                <strong>{rec?.instrument ?? row.rec_id}</strong>
                <Chip v={rec?.side ?? '—'} tone={rec?.side === 'BUY' ? 'ok' : 'warn'} />
                <Chip k="kind" v={rec?.kind ?? '—'} />
                <Chip k="style" v={`${rec?.style ?? '—'} / ${rec?.product ?? '—'}`} />
                <Chip k="gate" v={gate?.verdict ?? '—'} tone={toneFor(gate?.verdict)} />
                <Chip
                  k="action"
                  v={row.human_action ?? 'pending'}
                  tone={row.human_action ? toneFor(row.human_action) : 'warn'}
                />
                {rec?.short_flag_higher_tail_risk ? <Chip v="short: higher tail risk" tone="bad" /> : null}
                <span className="spacer" />
                <span className="dim">delivered {hhmmss(row.delivered_at)}</span>
              </div>

              <div className="row">
                <Chip k="entry" v={zone ? `${zone[0]} – ${zone[1]}` : '—'} />
                <Chip k="stop" v={rec?.stop ?? '—'} tone="bad" />
                <Chip k="targets" v={(rec?.targets ?? []).join(' / ') || '—'} tone="ok" />
                <Chip
                  k="qty"
                  v={
                    gate?.original_qty != null && gate.original_qty !== rec?.qty
                      ? `${rec?.qty ?? '—'} (of ${gate.original_qty})`
                      : (rec?.qty ?? '—')
                  }
                />
                <Chip k="notional" v={dash(rec?.notional)} />
                <Chip k="conf" v={rec?.confidence != null ? rec.confidence.toFixed(2) : '—'} />
                {row.human_fill_price ? <Chip k="fill" v={row.human_fill_price} /> : null}
                {rec?.valid_until ? <Chip k="valid till" v={hhmmss(rec.valid_until)} /> : null}
              </div>

              <div className="thesis">{rec?.thesis ?? ''}</div>

              {reasons.length > 0 ? (
                <div className="row">
                  {reasons.map((r) => (
                    <Chip key={r} v={r} tone={toneFor(gate?.verdict)} />
                  ))}
                </div>
              ) : null}

              {(rec?.manual_checklist ?? []).length > 0 ? (
                <ul className="checklist">
                  {rec?.manual_checklist?.map((line, i) => <li key={i}>{line}</li>)}
                </ul>
              ) : null}
            </div>
          )
        })
      )}
    </Panel>
  )
}
