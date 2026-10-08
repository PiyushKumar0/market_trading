import { test } from 'node:test'
import assert from 'node:assert/strict'
import { tabOfHash } from '../src/lib/tabs.ts'

test('a tab hash opens that tab; no hash is the Overview', () => {
  assert.equal(tabOfHash('#system'), 'system')
  assert.equal(tabOfHash('#paper'), 'paper')
  assert.equal(tabOfHash(''), 'overview')
  assert.equal(tabOfHash('#'), 'overview')
})

test('a hash that is not a tab (the skip link, a typed anchor) is not a tab change', () => {
  // Runtime review #2: "#main" from "Skip to content" used to switch every tab to the Overview.
  assert.equal(tabOfHash('#main'), null)
  assert.equal(tabOfHash('#alerts'), null)
  assert.equal(tabOfHash('#System'), null)
})
