import { test } from 'node:test'
import assert from 'node:assert/strict'
import {
  actionTerm,
  agentName,
  closeReasonTerm,
  deliveryTerm,
  digestTerm,
  haltTerm,
  kindTerm,
  modeTerm,
  originTerm,
  outcomeTerm,
  positionStateTerm,
  productTerm,
  proposalActionTerm,
  protectionTerm,
  riskTerm,
  ruleTerm,
  severityTerm,
  strategyName,
  styleTerm,
  tierTerm,
  verdictTerm,
  actionHint,
  reasonTerm,
  COPY,
} from '../src/copy/glossary.ts'

test('the re-arm toast says below-the-line only when a floor is still breached', () => {
  assert.equal(COPY.rearmResult(['floor_equity_floor_rung'], []).tone, 'good')
  assert.deepEqual(COPY.rearmResult([], ['floor_equity_floor_rung']), {
    tone: 'crit',
    text: 'Not re-armed: the account is still below the line.',
  })
  // Nothing latched any more (re-armed from Telegram before the next poll hid the button): no false alarm.
  assert.deepEqual(COPY.rearmResult([], []), { tone: 'note', text: 'Nothing to re-arm: no floor stop is set.' })
})

test('engine states translate to plain words and keep the engine term', () => {
  assert.equal(modeTerm('OFF').label, 'Off')
  assert.equal(modeTerm('RECOMMEND').label, 'Recommend')
  assert.equal(modeTerm('AUTO').label, 'Auto')
  assert.equal(riskTerm('NORMAL').label, 'Normal')
  assert.equal(riskTerm('FROZEN').label, 'New positions paused')
  assert.equal(riskTerm('CLOSE_ONLY').label, 'Closing only')
  assert.equal(riskTerm('KILLED').label, 'Kill switch on')
  assert.equal(riskTerm('FROZEN').term, 'FROZEN')
  assert.ok(riskTerm('FROZEN').hint)
})

test('AI budget levels', () => {
  assert.deepEqual(
    ['DG0', 'DG1', 'DG2', 'DG3', 'DG4'].map((t) => tierTerm(t).label),
    ['Normal', 'Pacing', 'Saving', 'Minimal', 'Stopped'],
  )
  assert.equal(tierTerm('DG2').term, 'DG2')
})

test('risk-check verdicts and your actions', () => {
  assert.equal(verdictTerm('approve').label, 'Approved')
  assert.equal(verdictTerm('shrink').label, 'Approved, smaller')
  assert.equal(verdictTerm('reject').label, 'Blocked')
  assert.equal(verdictTerm('owner_approval_required').label, 'Needs your OK')
  assert.equal(verdictTerm(null).label, 'Waiting for check')
  assert.equal(actionTerm(null).label, 'Waiting for you')
  assert.equal(actionTerm('taken').label, 'You took it')
  assert.equal(actionTerm('dismissed').label, 'You skipped it')
  assert.equal(actionTerm('expired').label, 'Expired')
  assert.equal(actionTerm('closed').label, 'Closed')
})

test('trade vocabulary', () => {
  assert.equal(kindTerm('entry').label, 'New position')
  assert.equal(kindTerm('enter').label, 'New position')
  assert.equal(kindTerm('exit').label, 'Exit')
  assert.equal(kindTerm('adjust').label, 'Adjust')
  assert.equal(productTerm('CNC').label, 'Delivery')
  assert.equal(productTerm('MIS').label, 'Intraday')
  assert.equal(styleTerm('swing').label, 'Swing')
  assert.equal(originTerm('external').label, 'Outside engine')
  assert.equal(originTerm('recommended').label, 'Recommendation')
  assert.equal(protectionTerm('PROTECTION_FAILED').label, 'Protection failed')
  assert.equal(protectionTerm('TIME_EXIT_ONLY').label, 'No stop (test)')
  assert.match(protectionTerm('TIME_EXIT_ONLY').hint ?? '', /no stop-loss or target/)
  assert.equal(positionStateTerm('PENDING_EXIT').label, 'Exiting')
  assert.equal(proposalActionTerm('modify-stop').label, 'Move stop-loss')
  assert.equal(proposalActionTerm('enter').label, 'Open')
})

test('gate rules read as the problem', () => {
  assert.equal(ruleTerm('per_trade_risk').label, 'Risk per trade over the limit')
  assert.equal(ruleTerm('max_open_positions').label, 'Too many open positions (waiting recommendations count too)')
  assert.equal(ruleTerm('per_sector_exposure').label, 'Too much in one sector')
  assert.equal(ruleTerm('per_trade_risk').term, 'per_trade_risk')
})

test('paper halts, outcomes and close reasons', () => {
  assert.equal(haltTerm('daily_loss_soft').label, 'Daily loss limit (soft)')
  assert.equal(haltTerm('floor_cumulative_floor').label, 'Total loss floor')
  assert.equal(outcomeTerm('scratch').label, 'Flat')
  assert.equal(outcomeTerm('void').label, 'Voided')
  assert.equal(closeReasonTerm('stop').label, 'Stop-loss hit')
  assert.equal(closeReasonTerm('target').label, 'Target hit')
})

test('a flatten caused by /paper reset is not called a loss limit', () => {
  // oms/exits.py closes every flatten as risk_flatten; paper_risk.py passes the basis "paper_reset".
  assert.equal(closeReasonTerm('risk_flatten', 'paper_reset').label, 'Closed by your paper reset')
  assert.equal(closeReasonTerm('risk_flatten', 'paper_reset').term, 'risk_flatten')
  assert.equal(closeReasonTerm('risk_flatten', 'equity_floor').label, 'Closed by a paper loss limit')
  assert.equal(closeReasonTerm('risk_flatten').label, 'Closed by a paper loss limit')
})

test('names of strategies and analysts', () => {
  assert.equal(strategyName('hi52').label, '52-week-high continuation')
  assert.equal(strategyName('hi52').term, 'hi52')
  assert.equal(strategyName('unattributed').label, 'Not linked to a strategy')
  assert.equal(agentName('news_analyst').label, 'News analyst')
})

test('news summary, Telegram delivery and severity', () => {
  assert.equal(digestTerm('fresh').label, 'Up to date')
  assert.equal(digestTerm('missing').label, 'Missing')
  assert.equal(digestTerm(null).label, 'Unknown')
  assert.equal(deliveryTerm({ status: 'delivered', attempts: 1 }).label, 'Delivered')
  assert.equal(deliveryTerm({ status: 'failed', attempts: 9 }).label, 'Not delivered')
  assert.equal(deliveryTerm({ status: 'pending', attempts: 3 }).label, 'Retrying (3)')
  assert.equal(deliveryTerm({ status: 'pending', attempts: 0 }).label, 'Queued')
  assert.equal(severityTerm('critical').label, 'Critical')
})

test('unknown values fall back to words and never throw', () => {
  assert.equal(riskTerm('MELTDOWN').label, 'Meltdown')
  assert.equal(riskTerm('MELTDOWN').term, 'MELTDOWN')
  assert.equal(ruleTerm('paper_open_positions').label, 'Paper open positions')
  assert.equal(strategyName('zz9').label, 'zz9')
  assert.equal(agentName('foo_bot').label, 'Foo bot')
  assert.equal(closeReasonTerm('broker_rms').label, 'Closed by the broker (RMS)')
  assert.equal(closeReasonTerm('weird_reason').label, 'Weird reason')
})

test('the Telegram reply depends on the kind of recommendation (catalog.py footers)', () => {
  assert.equal(
    actionHint({ kind: 'entry', instrument: 'TATASTEEL', qty: 3 }),
    'Placed it? Reply /taken TATASTEEL 3 PRICE (PRICE = your fill). Not taking it? Reply /veto TATASTEEL REASON (market, price, size, trust, away or other).',
  )
  assert.equal(
    actionHint({ kind: 'exit', instrument: 'HDFCAMC', side: 'SELL' }),
    'After you sell in Kite, reply /closed HDFCAMC PRICE (PRICE = your fill). Not exiting? Reply /veto HDFCAMC.',
  )
  assert.equal(
    actionHint({ kind: 'adjust', instrument: 'SAIL' }),
    "Move your existing stop-loss order in Kite to the new level; there's nothing to buy or sell. To decline, reply /veto SAIL.",
  )
  assert.match(actionHint(null), /\/taken SYMBOL QTY PRICE/)
})

test('risk-check reasons get a plain label and keep the engine numbers verbatim', () => {
  // gate.py: a failed rule is reported as "<rule_id>: <value> (limit <limit>)".
  const r = reasonTerm('daily_loss_soft: day MTM -5.123% (limit > -5.0%)')
  assert.equal(r.label, "Today's loss limit reached")
  assert.equal(r.detail, 'day MTM -5.123% (limit > -5.0%)')
  assert.equal(r.term, 'daily_loss_soft')
  assert.equal(reasonTerm('per_trade_risk').label, 'Risk per trade over the limit')
  assert.equal(reasonTerm('per_trade_risk').detail, undefined)
  const s = reasonTerm('shrink: qty 10 -> 3 (bound by per_trade_risk)')
  assert.equal(s.label, 'Quantity reduced')
  assert.equal(s.detail, 'qty 10 -> 3 (bound by per_trade_risk)')
  assert.equal(reasonTerm('paper_open_positions').label, 'Paper open positions')
  assert.equal(reasonTerm('Something odd: 3 > 2').label, 'Something odd: 3 > 2')
})

test('missing values render as a dash', () => {
  for (const f of [modeTerm, riskTerm, tierTerm, kindTerm, productTerm, originTerm, strategyName]) {
    assert.equal(f(null).label, '—')
    assert.equal(f(undefined).label, '—')
    assert.equal(f('').label, '—')
  }
})
