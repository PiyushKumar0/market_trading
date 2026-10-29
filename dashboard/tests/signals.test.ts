import { test } from 'node:test'
import assert from 'node:assert/strict'
import type { SignalCellLook, SignalKillLook } from '../src/types.ts'
import { DASH, ppOrDash, progressText, verdictTone } from '../src/lib/signals.ts'

const kill = (formed: boolean, final = 0, size = 20): SignalKillLook => ({
  size, formed, n_closed: final, n_void: 0, median_net_t20: null, hit_rate: null, mean_net_t20: null,
  median_excess_t20: null,
})
const cells = (formed: boolean): SignalCellLook => ({ size: 30, formed, n_cells: 12, n_final: 8, n_valued: 7, mean_diff: null })

test('verdict tone', () => {
  assert.equal(verdictTone('DEMOTE'), 'crit')
  assert.equal(verdictTone('ADVISORY'), 'warn')
  assert.equal(verdictTone('HOLD'), 'good')
  assert.equal(verdictTone('KEEP'), 'good')
  assert.equal(verdictTone('INSUFFICIENT'), 'neutral')
  assert.equal(verdictTone('UNREGISTERED'), 'neutral')
})

test('progress names the first look still forming', () => {
  assert.equal(progressText([kill(false, 7), kill(false)]), 'Look 1: 7 of 20 final')
  assert.equal(progressText([kill(true, 20), kill(false, 25, 40)]), 'Look 2: 25 of 40 final')
  assert.equal(progressText([cells(false), cells(false)]), 'Look 1: 12 of 30 cells formed, 8 final')
  assert.equal(progressText([kill(true), kill(true)]), 'Look 2 formed')
})

test('a missing figure is an em dash', () => {
  assert.equal(DASH, '—')
  assert.equal(ppOrDash(null), '—')
  assert.equal(ppOrDash(-0.84), '−0.84 pp')
})
