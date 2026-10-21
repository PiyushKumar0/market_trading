import { test } from 'node:test'
import assert from 'node:assert/strict'
import type { PaperSummary } from '../src/types.ts'
import { entryGuardText, paperStatus } from '../src/lib/paper.ts'

function paper(over: Partial<PaperSummary> = {}): PaperSummary {
  return {
    enabled: true, changed_at: null, changed_by: null, epoch_started_at: null, reset_requested_at: null,
    subsystem_enabled: true, built: true, prep_ready: true, entry_guard: null, unmarked: [], halts: [],
    equity: null, curve: [], positions: [], orders: [], closed: [],
    totals: { closed: 0, wins: 0, voids: 0, void_net: null }, counters: null, ...over,
  }
}

test('entry guard reasons read as plain words', () => {
  assert.equal(entryGuardText('paper is off'), 'the autopilot is off')
  // paper_risk.py: the paper guard uses the WORSE of the real risk state and the paper halts; with no
  // paper halt in force (paperStatus checks halts first) the cause is the engine's own risk state.
  assert.equal(entryGuardText('effective paper state is FROZEN'), "the engine's own risk state (new positions paused) also stops paper entries")
  assert.equal(entryGuardText('effective paper state is RiskState.CLOSE_ONLY'), "the engine's own risk state (closing only) also stops paper entries")
  assert.equal(entryGuardText('paper session prep has not completed'), "today's paper setup hasn't finished yet")
  assert.equal(
    entryGuardText('paper session prep is waiting for an in-session tick'),
    "today's paper setup starts on the first live market tick, and none has arrived yet",
  )
  assert.equal(
    entryGuardText('paper construction failed (build_broker, restore_gtts)'),
    'paper failed to start (build_broker, restore_gtts) and stays off until the engine restarts',
  )
  assert.equal(entryGuardText('not built'), 'the autopilot is not running')
  assert.equal(entryGuardText('something new'), 'something new')
})

test('not set up, failed and off', () => {
  assert.deepEqual(paperStatus(paper({ built: false, subsystem_enabled: false, enabled: false }), 'RECOMMEND'), {
    tone: 'neutral',
    text: 'Not set up: paper trading is switched off in the engine settings.',
  })
  assert.equal(paperStatus(paper({ built: false }), 'RECOMMEND').tone, 'warn')
  assert.equal(paperStatus(paper({ enabled: false }), 'RECOMMEND').text, 'Off: not taking new paper trades. Open paper positions keep their exits.')
})

test('idle when the engine is not recommending', () => {
  assert.equal(paperStatus(paper(), 'OFF').text, 'On but idle: it follows the Recommend pipeline, and the engine is Off.')
  assert.equal(paperStatus(paper(), null).text, "On, waiting for the engine's mode.")
})

test('a halt names its effect and its limit', () => {
  const halts = [{ cause: 'daily_loss_soft', rung: 'FROZEN', set_at: '2026-10-08T10:29:00+05:30', latched: false }]
  assert.deepEqual(paperStatus(paper({ halts, entry_guard: 'effective paper state is FROZEN' }), 'RECOMMEND'), {
    tone: 'warn',
    text: 'New paper trades paused: daily loss limit (soft).',
  })
})

test('a guard without a halt, and the open state', () => {
  assert.equal(
    paperStatus(paper({ entry_guard: 'paper session prep has not completed' }), 'RECOMMEND').text,
    "On, but new paper trades are blocked: today's paper setup hasn't finished yet.",
  )
  assert.deepEqual(paperStatus(paper(), 'AUTO'), { tone: 'good', text: 'On: taking new paper trades.' })
})
