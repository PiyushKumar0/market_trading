/** Recommendation helpers shared by the Overview and Recommendations tabs. Pure and DOM-free. */
import { actionTerm } from '../copy/glossary.ts'
import type { RecommendationPayload, RecommendationRow } from '../types.ts'
import { inr } from './format.ts'

export interface ActionState {
  label: string
  hint?: string
  term?: string
  tone: 'good' | 'warn' | 'neutral'
}

/** Your recorded action, or — with none recorded — "waiting" only while the recommendation is live. */
export function actionState(row: RecommendationRow, nowMs: number): ActionState {
  if (row.human_action) {
    return { ...actionTerm(row.human_action), tone: row.human_action === 'taken' ? 'good' : 'neutral' }
  }
  if (isLive(row, nowMs)) return { ...actionTerm(null), tone: 'warn' }
  return {
    label: 'No action recorded',
    hint: 'Its validity passed without an action recorded on Telegram.',
    tone: 'neutral',
  }
}

/** A pending row whose `valid_until` is still ahead: actionable NOW, whichever day delivered it
 *  (WO-V 2026-09-13: a swing entry stays valid to the next session's close). */
export function isLive(row: RecommendationRow, nowMs: number): boolean {
  const until = row.recommendation?.valid_until
  if (row.human_action || !until) return false
  const at = new Date(until).getTime()
  return !Number.isNaN(at) && at > nowMs
}

/** The live rows, in the server's (most recent first) order. */
export function liveRecs(rows: RecommendationRow[], nowMs: number): RecommendationRow[] {
  return rows.filter((row) => isLive(row, nowMs))
}

function sameLevel(a: string, b: string): boolean {
  const x = Number(a)
  const y = Number(b)
  return Number.isFinite(x) && Number.isFinite(y) ? x === y : a === b
}

/** "₹2,445.00", or "₹2,440.00 – ₹2,450.00" for a real range, or "—". */
export function entryText(rec: RecommendationPayload | null | undefined): string {
  const zone = rec?.entry_zone
  if (!zone) return '—'
  const lo = inr(zone[0])
  const hi = inr(zone[1])
  if (!lo && !hi) return '—'
  if (!lo || !hi || sameLevel(zone[0], zone[1])) return (lo ?? hi) as string
  return `${lo} – ${hi}`
}

/**
 * "The risk check cut the quantity from 10 to 3" — only on an entry, and only when the approved
 * quantity really is smaller. An exit or adjust carries the held position's own size (ops/pipeline.py
 * `_manage_recommendation`), so its gate's `original_qty` says nothing about a cut.
 */
export function quantityCut(rec: RecommendationPayload | null | undefined): { from: number; to: number } | null {
  if (!rec || rec.kind === 'exit' || rec.kind === 'adjust') return null
  const from = rec.gate?.original_qty
  const to = rec.qty
  if (from === null || from === undefined || to === null || to === undefined || from <= to) return null
  return { from, to }
}

/**
 * One sentence saying what to do, by kind (ops/pipeline.py builds exits and adjusts from the held
 * position):
 *  - entry  → "Buy 3 shares at ₹2,445.00"
 *  - exit   → "Sell all 3 shares at market": an exit's zone is the last price OR the position's own
 *             entry price, and its checklist says "exit at market" — a price here would mislead;
 *  - adjust → "Move the stop-loss on SAIL to ₹128.50": it places NO order and keeps the position's
 *             side, so a "Buy …" headline would read as doubling the position.
 */
export function instruction(rec: RecommendationPayload | null | undefined): string {
  if (!rec) return '—'
  const qty = rec.qty
  if (rec.kind === 'adjust') {
    const stop = inr(rec.stop)
    return `Move the stop-loss on ${rec.instrument ?? 'this position'}${stop ? ` to ${stop}` : ''}`
  }
  if (rec.kind === 'exit') {
    const verb = rec.side === 'BUY' ? 'Buy back' : 'Sell'
    const what = qty !== null && qty !== undefined ? ` all ${qty} ${qty === 1 ? 'share' : 'shares'}` : ''
    return `${verb}${what} at market`
  }
  const verb = rec.side === 'BUY' ? 'Buy' : rec.side === 'SELL' ? 'Sell' : 'Trade'
  const what = qty !== null && qty !== undefined ? ` ${qty} ${qty === 1 ? 'share' : 'shares'}` : ''
  const at = entryText(rec)
  return `${verb}${what}${at === '—' ? '' : ` at ${at}`}`
}
