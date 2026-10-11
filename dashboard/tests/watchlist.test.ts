import { test } from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'

// node --test cannot import TSX, so this guards the component's source text.
const SRC = readFileSync(new URL('../src/views/parts/NewsWatchlist.tsx', import.meta.url), 'utf8')

test('news watchlist shows none of the retired v1 cat levels', () => {
  for (const gone of [
    'confirm_trigger',
    'invalidation',
    'stop_band_',
    'target_band_',
    'Trigger price',
    'Cancel price',
    'Stop range',
    'Target range',
  ]) {
    assert.ok(!SRC.includes(gone), `NewsWatchlist.tsx still mentions ${gone}`)
  }
})

test('news watchlist keeps its five current columns', () => {
  for (const key of ['symbol', 'event', 'direction', 'materiality', 'sources']) {
    assert.ok(SRC.includes(`key: '${key}'`), `column ${key} missing`)
  }
})
