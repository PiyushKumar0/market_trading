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
  takenHint,
  tierTerm,
  verdictTerm,
} from '../src/copy/glossary.ts'

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
  assert.equal(positionStateTerm('PENDING_EXIT').label, 'Exiting')
  assert.equal(proposalActionTerm('modify-stop').label, 'Move stop-loss')
  assert.equal(proposalActionTerm('enter').label, 'Open')
})

test('gate rules read as the problem', () => {
  assert.equal(ruleTerm('per_trade_risk').label, 'Risk per trade over the limit')
  assert.equal(ruleTerm('max_open_positions').label, 'Too many open positions')
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

test('the /taken reply is spelled out with the symbol and quantity', () => {
  assert.equal(
    takenHint('TATASTEEL', 3),
    'After you place it in Kite, reply /taken TATASTEEL 3 PRICE on Telegram (PRICE = your fill).',
  )
  assert.equal(takenHint(undefined, undefined), 'After you place it in Kite, reply /taken SYMBOL QTY PRICE on Telegram (PRICE = your fill).')
})

test('missing values render as a dash', () => {
  for (const f of [modeTerm, riskTerm, tierTerm, kindTerm, productTerm, originTerm, strategyName]) {
    assert.equal(f(null).label, '—')
    assert.equal(f(undefined).label, '—')
    assert.equal(f('').label, '—')
  }
})
