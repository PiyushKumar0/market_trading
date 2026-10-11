import { test } from 'node:test'
import assert from 'node:assert/strict'
import type { RecommendationPayload, RecommendationRow } from '../src/types.ts'
import { actionState, entryText, instruction, isLive, liveRecs, quantityCut } from '../src/lib/recs.ts'

const NOW = Date.parse('2026-10-08T09:00:00Z') // Thu 14:30 IST

function row(over: Partial<RecommendationRow> = {}, rec: RecommendationPayload | null = {}): RecommendationRow {
  return {
    rec_id: 'r1',
    recommendation: { instrument: 'TATASTEEL', side: 'BUY', kind: 'entry', qty: 3, valid_until: '2026-10-08T15:30:00+05:30', ...rec },
    delivered_at: '2026-10-08T09:41:10+05:30',
    human_action: null,
    human_fill_price: null,
    outcome: null,
    ...over,
  }
}

test('a pending recommendation still inside its validity is live', () => {
  assert.equal(isLive(row(), NOW), true)
})

test('acted on, expired or undated recommendations are not live', () => {
  assert.equal(isLive(row({ human_action: 'taken' }), NOW), false)
  assert.equal(isLive(row({}, { valid_until: '2026-10-08T14:00:00+05:30' }), NOW), false)
  assert.equal(isLive(row({}, { valid_until: 'soon' }), NOW), false)
  assert.equal(isLive(row({}, { valid_until: undefined }), NOW), false)
  assert.equal(isLive({ ...row(), recommendation: null }, NOW), false)
})

test('a pending recommendation from an earlier day whose valid_until is still ahead is live (generic)', () => {
  const r = row({ delivered_at: '2026-10-07T14:47:00+05:30' }, { valid_until: '2026-10-09T15:30:00+05:30' })
  assert.equal(isLive(r, NOW), true)
})

test('liveRecs keeps server order and drops the rest', () => {
  const a = row({ rec_id: 'a' })
  const b = row({ rec_id: 'b', human_action: 'expired' })
  const c = row({ rec_id: 'c' })
  assert.deepEqual(liveRecs([a, b, c], NOW).map((r) => r.rec_id), ['a', 'c'])
})

test('entryText shows one price or a range', () => {
  assert.equal(entryText({ entry_zone: ['2445.00', '2445.00'] }), '₹2,445.00')
  assert.equal(entryText({ entry_zone: ['2440.00', '2450.00'] }), '₹2,440.00 – ₹2,450.00')
  assert.equal(entryText({}), '—')
  assert.equal(entryText(null), '—')
})

test('your action: waiting only while still live', () => {
  assert.deepEqual(
    [actionState(row(), NOW).label, actionState(row(), NOW).tone],
    ['Waiting for you', 'warn'],
  )
  const lapsed = actionState(row({}, { valid_until: '2026-10-08T14:00:00+05:30' }), NOW)
  assert.equal(lapsed.label, 'No action recorded')
  assert.equal(lapsed.tone, 'neutral')
  assert.ok(lapsed.hint)
  assert.equal(actionState(row({ human_action: 'taken' }), NOW).tone, 'good')
  assert.equal(actionState(row({ human_action: 'dismissed' }), NOW).label, 'You skipped it')
  assert.equal(actionState(row({ human_action: 'dismissed' }), NOW).tone, 'neutral')
})

test('an exit card says sell at market, never at a price', () => {
  // pipeline.py: an exit's zone is the last price OR the position's own entry; the checklist is "exit at market".
  assert.equal(instruction({ kind: 'exit', side: 'SELL', qty: 3, entry_zone: ['2445.00', '2445.00'] }), 'Sell all 3 shares at market')
  assert.equal(instruction({ kind: 'exit', side: 'BUY', qty: 5 }), 'Buy back all 5 shares at market')
})

test('an adjust card moves the stop and places no order', () => {
  // pipeline.py: an adjust keeps the position's side and places no new order.
  assert.equal(
    instruction({ kind: 'adjust', side: 'BUY', instrument: 'SAIL', qty: 3, stop: '128.50', entry_zone: ['130.00', '130.00'] }),
    'Move the stop-loss on SAIL to ₹128.50',
  )
})

test('the quantity-cut note is only for an entry the risk check made smaller', () => {
  const gate = (original_qty: number | null) => ({ verdict: 'shrink', reasons: [], original_qty })
  assert.deepEqual(quantityCut({ kind: 'entry', qty: 3, gate: gate(10) }), { from: 10, to: 3 })
  // Not a cut: the same size, a smaller "original", no original, or an exit/adjust (whose quantity is
  // the held position's own, pipeline.py _manage_recommendation).
  assert.equal(quantityCut({ kind: 'entry', qty: 3, gate: gate(3) }), null)
  assert.equal(quantityCut({ kind: 'entry', qty: 10, gate: gate(3) }), null)
  assert.equal(quantityCut({ kind: 'entry', qty: 3, gate: gate(null) }), null)
  assert.equal(quantityCut({ kind: 'adjust', qty: 3, gate: gate(10) }), null)
  assert.equal(quantityCut({ kind: 'exit', qty: 3, gate: gate(10) }), null)
  assert.equal(quantityCut(null), null)
})

test('instruction says what to do in one sentence', () => {
  assert.equal(instruction({ side: 'BUY', qty: 3, entry_zone: ['2445.00', '2445.00'] }), 'Buy 3 shares at ₹2,445.00')
  assert.equal(instruction({ side: 'SELL', qty: 1, entry_zone: ['100.50', '101.00'] }), 'Sell 1 share at ₹100.50 – ₹101.00')
  assert.equal(instruction({ side: 'SELL', qty: 30 }), 'Sell 30 shares')
  assert.equal(instruction({ side: 'BUY' }), 'Buy')
  assert.equal(instruction(null), '—')
})
