/**
 * Panel 3 — recommendations (§3.6). Each card shows the delivered artifact: thesis, entry zone,
 * stop/targets, gate-approved qty, the gate verdict, the B7/R3 manual protective-order checklist the
 * human must work through, and the owner's recorded `human_action` (taken | expired | dismissed |
 * closed — set through Telegram `/taken` `/closed`, never from here: this dashboard is read-only over
 * the recommendation ledger).
 *
 * Cards are split by the IST day they were delivered on (owner-reported 2026-09-08: the route
 * returns every delivery it keeps, and cards from different sessions carried only a time-of-day).
 * Today's fold starts open; earlier days start folded and open on click — EXCEPT a day still holding
 * a LIVE recommendation (pending, `valid_until` ahead), which starts open too: since WO-V (2026-09-13)
 * a swing/position entry is valid to the NEXT session's close, so an actionable card can sit under
 * yesterday's date. For the same reason the `valid till` chip carries its date when that is not today.
 *
 * Gate reasons come from the recommendation's own embedded verdict (§3.6 `gate`) — never from
 * `/decisions`: a different proposal's verdict is never this card's provenance.
 */
import type { RecommendationRow } from '../types'
import {
  Chip,
  DayFold,
  Empty,
  Panel,
  dash,
  groupByDay,
  hhmmss,
  istDay,
  todayIst,
  toneFor,
  useDayFolds,
} from './ui'

/** A pending row whose `valid_until` is still ahead: actionable NOW, whichever day delivered it. */
function isLive(row: RecommendationRow, now: number): boolean {
  const until = row.recommendation?.valid_until
  if (row.human_action || !until) return false
  const at = new Date(until).getTime()
  return !Number.isNaN(at) && at > now
}

/** "HH:MM:SS" for a same-day expiry, "YYYY-MM-DD HH:MM:SS" when the expiry is on another IST day —
 *  a bare time was unambiguous only while every live recommendation was delivered today. */
function validTill(iso: string): string {
  const day = istDay(iso)
  return day && day !== todayIst() ? `${day} ${hhmmss(iso)}` : hhmmss(iso)
}

function RecommendationCard({ row }: { row: RecommendationRow }) {
  const rec = row.recommendation
  const gate = rec?.gate
  const zone = rec?.entry_zone
  const reasons = gate?.reasons ?? []
  return (
    <div className="rec">
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
        {rec?.valid_until ? <Chip k="valid till" v={validTill(rec.valid_until)} /> : null}
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
}

export function RecommendationsPanel({ rows }: { rows: RecommendationRow[] }) {
  // `delivered_at` is the ledger's own stamp; the payload's `created_at` only stands in for a row
  // journalled before its delivery completed.
  const dayOf = (row: RecommendationRow) => istDay(row.delivered_at ?? row.recommendation?.created_at)
  const days = groupByDay(rows, dayOf)
  const today = todayIst()
  const now = Date.now()
  const liveDays = new Set(days.filter((g) => g.rows.some((row) => isLive(row, now))).map((g) => g.day))
  const folds = useDayFolds((day) => liveDays.has(day))
  const todayCount = days.find((g) => g.day === today)?.rows.length ?? 0
  const liveEarlier = rows.filter((row) => isLive(row, now) && dayOf(row) !== today).length

  return (
    <Panel
      title="Recommendations"
      aside={rows.length ? `${todayCount} today · ${rows.length} shown` : undefined}
      wide
    >
      {rows.length === 0 ? (
        <Empty what="recommendations" />
      ) : (
        <>
          {todayCount === 0 && liveEarlier === 0 ? <Empty what="recommendations today" /> : null}
          {todayCount === 0 && liveEarlier > 0 ? (
            <div className="dim">
              no deliveries today · {liveEarlier} still actionable from an earlier session
            </div>
          ) : null}
          {days.map((g) => (
            <div className="day-group" key={g.day || 'undated'}>
              <DayFold
                day={g.day}
                count={g.rows.length}
                what="recommendations"
                open={folds.isOpen(g.day)}
                onToggle={() => folds.toggle(g.day)}
              />
              {folds.isOpen(g.day)
                ? g.rows.map((row) => <RecommendationCard key={row.rec_id} row={row} />)
                : null}
            </div>
          ))}
        </>
      )}
    </Panel>
  )
}
