import { test } from 'node:test'
import assert from 'node:assert/strict'
import type { CapitalFigures, CapitalPreview } from '../src/types.ts'
import { ApiError, NoAnswerError } from '../src/api.ts'
import { capitalRows, comparisonBase, failureText, parseCapitalInput, refusalText, tripText } from '../src/lib/capital.ts'

const figures = (over: Partial<CapitalFigures> = {}): CapitalFigures => ({
  capital_inr: '40000',
  max_deployed_capital_inr: '60000',
  cnc_notional_inr: '12000',
  equity_inr: '40113.30',
  paper_equity_inr: '40113.30',
  risk_intraday_inr: '601.70',
  risk_swing_inr: '601.70',
  day_loss_soft_inr: '-2005.67',
  day_loss_hard_inr: '-3008.50',
  equity_floor_inr: '36000',
  cumulative_floor_inr: '34000',
  hi52_edge_pct: '1.53',
  ...over,
})

const preview = (over: Partial<CapitalPreview> = {}): CapitalPreview => ({
  current: figures(),
  proposed: figures({ capital_inr: '60000', equity_floor_inr: '54000', hi52_edge_pct: '1.53' }),
  allowed: true,
  refusal: null,
  would_trip: [],
  ...over,
})

test('capitalRows gives every label in a fixed order', () => {
  const labels = capitalRows(preview()).map((r) => r.label)
  assert.deepEqual(labels, [
    'Starting capital',
    'Most capital in use at once',
    'Most in one stock',
    'Equity on record',
    'Paper equity',
    'Risk per intraday trade',
    'Risk per swing trade',
    "Today's loss limit",
    "Today's hard loss limit",
    'Equity floor',
    'Total loss floor',
    'Edge the gate assumes for 52-week-high trades',
  ])
})

test('capitalRows shows the engine figures old and new, rupees with grouping', () => {
  const rows = capitalRows(preview())
  const get = (l: string) => rows.find((r) => r.label === l)
  assert.deepEqual(get('Starting capital'), { label: 'Starting capital', old: '₹40,000', new: '₹60,000' })
  assert.equal(get('Equity floor')?.old, '₹36,000')
  assert.equal(get('Equity floor')?.new, '₹54,000')
  assert.equal(get('Equity on record')?.old, '₹40,113.30')
})

test('capitalRows prints the edge as a percent and unknown figures as a dash', () => {
  const rows = capitalRows(preview({ proposed: figures({ hi52_edge_pct: null, risk_swing_inr: null }) }))
  const get = (l: string) => rows.find((r) => r.label === l)
  assert.equal(get('Edge the gate assumes for 52-week-high trades')?.old, '1.53%')
  assert.equal(get('Edge the gate assumes for 52-week-high trades')?.new, '—')
  assert.equal(get('Risk per swing trade')?.new, '—')
})

test('capitalRows with no proposal keeps all rows, new column empty', () => {
  const rows = capitalRows(preview({ proposed: null, allowed: false }))
  assert.equal(rows.length, 12)
  assert.ok(rows.every((r) => r.new === '—'))
  assert.equal(rows[0].old, '₹40,000')
})

test('capitalRows with no preview at all is a full table of dashes', () => {
  const rows = capitalRows(null)
  assert.equal(rows.length, 12)
  assert.ok(rows.every((r) => r.old === '—' && r.new === '—'))
})

test('tripText names the account and the rule in plain words', () => {
  assert.equal(tripText('real:equity_floor_rung'), 'Real account: Account fell below an equity floor')
  assert.equal(tripText('paper:cumulative_floor'), 'Paper account: Total loss floor reached')
  assert.equal(tripText('cumulative_floor'), 'Total loss floor reached')
})

test('refusalText puts rule ids into plain words and keeps the rupee line', () => {
  const out = refusalText('would trip: real:equity_floor_rung (₹18,000.00)')
  assert.equal(out, 'would trip: Real account: Account fell below an equity floor (₹18,000.00)')
  assert.ok(!out.includes('real:') && !out.includes('equity_floor_rung'))
  const two = refusalText('would trip: real:cumulative_floor (₹1.00), paper:equity_floor_rung (₹2.00)')
  assert.ok(!/(real|paper):|_/.test(two), two)
  assert.ok(two.includes('Paper account') && two.includes('₹2.00'))
  assert.equal(refusalText('capital changed since the dialog opened'), 'capital changed since the dialog opened')
})

test('failureText claims "not changed" only for refusals where nothing was written', () => {
  for (const status of [409, 422, 501]) {
    assert.equal(
      failureText(new ApiError(status, 'would trip: real:equity_floor_rung (₹1.00)')),
      'Capital not changed: would trip: Real account: Account fell below an equity floor (₹1.00)',
      String(status),
    )
  }
  assert.equal(
    failureText(new ApiError(503, 'protected config unverified: hash mismatch')),
    'Capital not changed: protected config unverified: hash mismatch',
  )
  assert.equal(
    failureText(new ApiError(409, 'no price yet for held positions (X), so the brakes can\'t be checked; change capital after the next session\'s close')),
    'Capital not changed: no price yet for held positions (X), so the brakes can\'t be checked; change capital after the next session\'s close',
  )
})

test('failureText on a 503 after the write began never says "not changed"', () => {
  const out = failureText(new ApiError(503, 'written, but the engine could not reload the limits'))
  assert.equal(out, 'written, but the engine could not reload the limits')
  assert.ok(!out.includes('not changed'))
})

test('failureText on no answer or a network failure says the outcome is unknown', () => {
  const none = failureText(new NoAnswerError(10))
  assert.equal(
    none,
    "No answer from the engine in time. The capital may still have changed: reopen this dialog to see the engine's value.",
  )
  assert.ok(!none.includes('top bar'))
  const net = failureText(new TypeError('Failed to fetch'))
  assert.equal(net, 'The engine did not confirm the change (Failed to fetch). Reopen this dialog to see the capital now.')
  assert.ok(failureText('boom').includes('(boom)'))
  assert.ok(!net.includes('not changed'))
  // a status the engine does not use for refusals is not a "nothing written" claim either
  assert.ok(failureText(new ApiError(500, 'oops')).startsWith('The engine did not confirm the change'))
})

test('comparisonBase prefers the latest preview base and falls back to the capital at open', () => {
  assert.equal(comparisonBase('50000', 40000), 50000)
  assert.equal(comparisonBase('50000.40', 40000), 50000)
  assert.equal(comparisonBase(null, 40000), 40000)
  assert.equal(comparisonBase(undefined, 40000), 40000)
  assert.equal(comparisonBase('', 40000), 40000)
  assert.equal(comparisonBase('n/a', 40000), 40000)
})

test('parseCapitalInput reads whole rupees', () => {
  assert.equal(parseCapitalInput('60,000'), 60000)
  assert.equal(parseCapitalInput(' 60000 '), 60000)
  assert.equal(parseCapitalInput('₹1,00,000'), 100000)
  assert.equal(parseCapitalInput('60000.00'), 60000)
})

test('parseCapitalInput refuses anything else', () => {
  for (const bad of ['abc', '0', '-5', '', '   ', '60,000.5', '6E+4', '1e5', '12 000', '0x10', '99999999999999999999']) {
    assert.equal(parseCapitalInput(bad), null, JSON.stringify(bad))
  }
})
