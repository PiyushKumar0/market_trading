/** Pure helpers for the "Change capital" dialog. Every printed figure is the engine's own string. */
import type { CapitalFigures, CapitalPreview } from '../types.ts'
import { ApiError, NoAnswerError } from '../api.ts'
import { COPY, ruleTerm } from '../copy/glossary.ts'
import { inr } from './format.ts'

export interface CapitalRow {
  label: string
  old: string
  new: string
}

const DASH = '—'

// Fixed order; the key is the engine's. Labels follow the dashboard's wording (redesign spec §15).
const ROWS: { key: keyof CapitalFigures; label: string; pct?: boolean }[] = [
  { key: 'capital_inr', label: 'Starting capital' },
  { key: 'max_deployed_capital_inr', label: 'Most capital in use at once' },
  { key: 'cnc_notional_inr', label: 'Most in one stock' },
  { key: 'equity_inr', label: 'Equity on record' },
  { key: 'paper_equity_inr', label: 'Paper equity' },
  { key: 'risk_intraday_inr', label: 'Risk per intraday trade' },
  { key: 'risk_swing_inr', label: 'Risk per swing trade' },
  { key: 'day_loss_soft_inr', label: "Today's loss limit" },
  { key: 'day_loss_hard_inr', label: "Today's hard loss limit" },
  { key: 'equity_floor_inr', label: 'Equity floor' },
  { key: 'cumulative_floor_inr', label: 'Total loss floor' },
  { key: 'hi52_edge_pct', label: 'Edge the gate assumes for 52-week-high trades', pct: true },
]

const show = (v: string | null | undefined, pct?: boolean): string => {
  if (v === null || v === undefined || v === '') return DASH
  return pct ? `${v}%` : (inr(v) ?? DASH)
}

/** The old -> new table. Always every row; an unknown figure, a missing proposal or no preview reads "—". */
export function capitalRows(preview: CapitalPreview | null): CapitalRow[] {
  return ROWS.map(({ key, label, pct }) => ({
    label,
    old: show(preview?.current[key], pct),
    new: show(preview?.proposed?.[key], pct),
  }))
}

/** "real:equity_floor_rung" -> "Real account: Account fell below an equity floor". */
export function tripText(id: string): string {
  const m = /^(real|paper):(.+)$/.exec(id)
  if (!m) return ruleTerm(id).label
  return `${m[1] === 'real' ? 'Real account' : 'Paper account'}: ${ruleTerm(m[2]).label}`
}

/** The engine's refusal with each `real:`/`paper:` rule id put into plain words; its ₹ line is kept. */
export function refusalText(raw: string): string {
  return raw.replace(/\b(?:real|paper):[a-z][a-z0-9_]*/g, (id) => tripText(id))
}

/**
 * What a failed Confirm tells the owner. "Not changed" is claimed only when the engine said nothing was
 * written (409/422/501, or 503 protected config unverified); a 503 after the write started, a timeout and a
 * network failure leave the outcome unknown or changed, so those never say it.
 */
export function failureText(err: unknown): string {
  if (err instanceof ApiError) {
    if (err.status === 409 || err.status === 422 || err.status === 501) return COPY.capitalRejected(refusalText(err.message))
    if (err.status === 503) {
      return err.message.startsWith('protected config unverified')
        ? COPY.capitalRejected(refusalText(err.message))
        : refusalText(err.message)
    }
  }
  if (err instanceof NoAnswerError) return COPY.capitalNoAnswer
  return COPY.capitalUnconfirmed(err instanceof Error ? err.message : String(err))
}

/** The capital a proposal is compared against: the latest preview's own base, else the capital at open. */
export function comparisonBase(latest: string | null | undefined, atOpen: number): number {
  if (latest === null || latest === undefined || latest.trim() === '') return atOpen
  const n = Math.round(Number(latest))
  return Number.isFinite(n) ? n : atOpen
}

/** "60,000" -> 60000. Whole rupees only: anything else (empty, 0, negative, decimals, exponents) is null. */
export function parseCapitalInput(text: string): number | null {
  const t = text.trim().replace(/^₹/, '').replace(/,/g, '')
  if (!/^\d+(\.0+)?$/.test(t)) return null
  const n = Number(t)
  return Number.isSafeInteger(n) && n >= 1 ? n : null
}
