import { test } from 'node:test'
import assert from 'node:assert/strict'
import {
  dayLabel,
  groupIndian,
  humanize,
  inr,
  istDay,
  istStamp,
  istTime,
  pct,
  pnlWithPct,
  price,
  ratePct,
  shortDate,
  signOf,
  signedInr,
  signedPct,
  todayIst,
  usd,
} from '../src/lib/format.ts'

test('groupIndian groups the last three digits, then pairs', () => {
  assert.equal(groupIndian('5'), '5')
  assert.equal(groupIndian('999'), '999')
  assert.equal(groupIndian('1000'), '1,000')
  assert.equal(groupIndian('100113'), '1,00,113')
  assert.equal(groupIndian('12345678'), '1,23,45,678')
})

test('inr keeps the engine digits and adds grouping, symbol and a true minus', () => {
  assert.equal(inr('100113.30'), '₹1,00,113.30')
  assert.equal(inr('100'), '₹100')
  assert.equal(inr('7335.00'), '₹7,335.00')
  assert.equal(inr('-1840.60'), '−₹1,840.60')
  assert.equal(inr('12345678.90'), '₹1,23,45,678.90')
})

test('a one-digit fraction is padded to paise; nothing is rounded or dropped', () => {
  // A Kite last price arrives as a float and becomes Decimal("2445.0") in the engine: "₹2,445.0"
  // reads like a typo. Adding a trailing zero is lossless; whole rupees and longer fractions stay.
  assert.equal(inr('2445.0'), '₹2,445.00')
  assert.equal(price('131.2'), '131.20')
  assert.equal(signedInr('-5.5'), '−₹5.50')
  assert.equal(inr('100'), '₹100')
  assert.equal(inr('2445.125'), '₹2,445.125')
})

test('inr answers null for a missing value instead of inventing one', () => {
  assert.equal(inr(''), null)
  assert.equal(inr(null), null)
  assert.equal(inr(undefined), null)
})

test('inr passes an unparseable string through untouched', () => {
  assert.equal(inr('n/a'), 'n/a')
})

test('signedInr marks direction and never signs zero', () => {
  assert.equal(signedInr('113.30'), '+₹113.30')
  assert.equal(signedInr('-1840.60'), '−₹1,840.60')
  assert.equal(signedInr('-0.00'), '₹0.00')
  assert.equal(signedInr('0'), '₹0')
  assert.equal(signedInr(null), null)
})

test('signedPct marks direction with the engine digits and never signs zero', () => {
  assert.equal(signedPct('3.08'), '+3.08%')
  assert.equal(signedPct('-1.25'), '−1.25%')
  assert.equal(signedPct('0.00'), '0.00%')
  assert.equal(signedPct('-0.00'), '0.00%')
  assert.equal(signedPct(null), null)
  assert.equal(signedPct(undefined), null)
  assert.equal(signedPct(''), null)
  assert.equal(signedPct('n/a'), 'n/a')
})

test('pnlWithPct puts the engine percent next to the rupees and degrades to rupees alone', () => {
  assert.equal(pnlWithPct('113.30', '0.57'), '+₹113.30 (+0.57%)')
  assert.equal(pnlWithPct('-1840.60', '-4.60'), '−₹1,840.60 (−4.60%)')
  assert.equal(pnlWithPct('0.00', '0.00'), '₹0.00 (0.00%)')
  assert.equal(pnlWithPct('113.30', null), '+₹113.30')
  assert.equal(pnlWithPct('113.30', undefined), '+₹113.30')
  assert.equal(pnlWithPct(null, '0.57'), null)
  assert.equal(pnlWithPct(undefined, undefined), null)
})

test('signOf reads the sign from the string', () => {
  assert.equal(signOf('113.30'), 1)
  assert.equal(signOf('-5'), -1)
  assert.equal(signOf('-0.00'), 0)
  assert.equal(signOf(''), null)
  assert.equal(signOf(null), null)
})

test('price groups without a currency symbol', () => {
  assert.equal(price('2607.60'), '2,607.60')
  assert.equal(price('171.40'), '171.40')
  assert.equal(price(null), null)
})

test('usd rounds to cents for display only', () => {
  assert.equal(usd('81.6487'), '$81.65')
  assert.equal(usd('0'), '$0.00')
  assert.equal(usd('200'), '$200.00')
  assert.equal(usd(undefined), null)
})

test('istDay is the IST calendar day, whatever the stamp zone', () => {
  assert.equal(istDay('2026-10-04T19:30:00Z'), '2026-10-05')
  assert.equal(istDay('2026-10-08T10:15:03+05:30'), '2026-10-08')
  assert.equal(istDay('garbage'), '')
  assert.equal(istDay(null), '')
})

test('todayIst uses the IST day of the given instant', () => {
  assert.equal(todayIst(new Date('2026-10-07T19:00:00Z')), '2026-10-08')
})

test('istTime is IST wall-clock time', () => {
  assert.equal(istTime('2026-10-08T09:13:00Z'), '14:43')
  assert.equal(istTime('2026-10-08T09:13:05Z', { seconds: true }), '14:43:05')
  assert.equal(istTime('2026-10-07T18:30:00Z'), '00:00')
  assert.equal(istTime(null), '—')
  assert.equal(istTime('garbage'), 'garbage')
})

test('istStamp shows only the time for today and the day otherwise', () => {
  const now = new Date('2026-10-08T09:00:00Z') // Thu 14:30 IST
  assert.equal(istStamp('2026-10-08T05:00:00Z', now), '10:30')
  assert.equal(istStamp('2026-10-07T07:30:00Z', now), 'Wed 7 Oct, 13:00')
  assert.equal(istStamp(null, now), '—')
})

test('dayLabel names today and yesterday', () => {
  const now = new Date('2026-10-08T09:00:00Z')
  assert.equal(dayLabel('2026-10-08', now), 'Today, Thu 8 Oct')
  assert.equal(dayLabel('2026-10-07', now), 'Yesterday, Wed 7 Oct')
  assert.equal(dayLabel('2026-10-04', now), 'Sun 4 Oct')
  assert.equal(dayLabel('', now), 'Undated')
})

test('shortDate formats a calendar day', () => {
  assert.equal(shortDate('2026-11-05'), 'Thu 5 Nov')
  assert.equal(shortDate('bad'), 'bad')
})

test('pct and ratePct', () => {
  assert.equal(pct(0.8), '0.80%')
  assert.equal(pct(-1.234, 1), '−1.2%')
  assert.equal(pct(null), null)
  assert.equal(ratePct(0.5), '50%')
  assert.equal(ratePct(2 / 3), '67%')
  assert.equal(ratePct(null), null)
})

test('humanize turns engine identifiers into words', () => {
  assert.equal(humanize('per_sector_exposure'), 'Per sector exposure')
  assert.equal(humanize('NEW_STATE'), 'New state')
  assert.equal(humanize('trade_window.changed'), 'Trade window changed')
  assert.equal(humanize(''), '')
})
