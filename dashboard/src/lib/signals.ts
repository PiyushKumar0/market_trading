/** The signals scorecard's verdicts and figures in words (Performance tab). Pure. */
import { pct, ratePct } from './format.ts'
import type { SignalCellLook, SignalKillLook } from '../types.ts'

export const DASH = '—'

export function verdictTone(verdict: string): 'good' | 'warn' | 'crit' | 'neutral' {
  if (verdict === 'HOLD' || verdict === 'KEEP') return 'good'
  if (verdict === 'DEMOTE') return 'crit'
  if (verdict === 'ADVISORY') return 'warn'
  return 'neutral'
}

/** The first look still forming, or the last one once every look formed. */
export function progressText(looks: (SignalKillLook | SignalCellLook)[]): string {
  const i = looks.findIndex((lk) => !lk.formed)
  if (i < 0) return `Look ${looks.length} formed`
  const lk = looks[i]
  return 'n_cells' in lk
    ? `Look ${i + 1}: ${lk.n_cells} of ${lk.size} cells formed, ${lk.n_final} final`
    : `Look ${i + 1}: ${lk.n_closed + lk.n_void} of ${lk.size} final`
}

export const pctOrDash = (v: number | null) => pct(v) ?? DASH
export const rateOrDash = (v: number | null) => ratePct(v) ?? DASH
export function ppOrDash(v: number | null): string {
  const p = pct(v)
  return p === null ? DASH : `${p.slice(0, -1)} pp`
}
