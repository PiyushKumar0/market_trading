/** Figure primitives: stat tile, meter, key/value list (dataviz figure contract). */
import type { ReactNode } from 'react'

/** label · value · optional signed delta · optional caption. Values use proportional figures. */
export function StatTile({
  label,
  value,
  delta,
  caption,
}: {
  label: ReactNode
  value: ReactNode
  delta?: { text: string; sign: 1 | -1 | 0 | null }
  caption?: ReactNode
}) {
  const cls = delta?.sign === 1 ? 'pos' : delta?.sign === -1 ? 'neg' : 'muted'
  return (
    <div className="stat">
      <div className="stat-label">{label}</div>
      <div className="stat-value">{value}</div>
      {delta ? <div className={`stat-delta ${cls}`}>{delta.text}</div> : null}
      {caption ? <div className="stat-caption">{caption}</div> : null}
    </div>
  )
}

/** Used against a limit. The fill carries severity (≥ 80 % warning, ≥ 100 % critical); the visible
 *  text always states the numbers, so the bar never carries meaning alone. */
export function Meter({
  label,
  used,
  limit,
  text,
  ariaLabel,
}: {
  label: ReactNode
  used: number | null
  limit: number | null
  text: ReactNode
  ariaLabel: string
}) {
  const known = used !== null && limit !== null && limit > 0
  const frac = known ? used / limit : 0
  const tone = !known ? 'none' : frac >= 1 ? 'crit' : frac >= 0.8 ? 'warn' : 'ok'
  return (
    <div className="meter">
      <div className="meter-head">
        <span className="meter-label">{label}</span>
        <span className="meter-text num">{text}</span>
      </div>
      <div
        className={`meter-track meter-${tone}`}
        role={known ? 'meter' : undefined}
        aria-label={ariaLabel}
        aria-valuemin={known ? 0 : undefined}
        aria-valuemax={known ? limit : undefined}
        aria-valuenow={known ? used : undefined}
      >
        <span className="meter-fill" style={{ width: `${Math.min(1, Math.max(0, frac)) * 100}%` }} />
      </div>
    </div>
  )
}

export function KeyValue({ items, className }: { items: { k: ReactNode; v: ReactNode }[]; className?: string }) {
  return (
    <dl className={className ? `kv ${className}` : 'kv'}>
      {items.map((it, i) => (
        <div className="kv-item" key={i}>
          <dt>{it.k}</dt>
          <dd>{it.v}</dd>
        </div>
      ))}
    </dl>
  )
}
