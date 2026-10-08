import { test } from 'node:test'
import assert from 'node:assert/strict'
import type { RecommendationPayload, RecommendationRow } from '../src/types.ts'
import { entryText, instruction, isLive, liveRecs } from '../src/lib/recs.ts'

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

test('a recommendation from yesterday valid into tomorrow is live (WO-V)', () => {
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

test('instruction says what to do in one sentence', () => {
  assert.equal(instruction({ side: 'BUY', qty: 3, entry_zone: ['2445.00', '2445.00'] }), 'Buy 3 shares at ₹2,445.00')
  assert.equal(instruction({ side: 'SELL', qty: 1, entry_zone: ['100.50', '101.00'] }), 'Sell 1 share at ₹100.50 – ₹101.00')
  assert.equal(instruction({ side: 'SELL', qty: 30 }), 'Sell 30 shares')
  assert.equal(instruction({ side: 'BUY' }), 'Buy')
  assert.equal(instruction(null), '—')
})
