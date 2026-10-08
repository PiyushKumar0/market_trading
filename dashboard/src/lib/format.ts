/**
 * Display formatting for the dashboard. Pure and DOM-free: `node --test` runs it directly.
 *
 * Money arrives as decimal STRINGS (§8.1) and is never parsed to a float for printing — grouping and
 * sign are string operations on the engine's own digits. The one exception is the USD budget, which
 * is rounded to cents for display; callers keep the exact string for a hint.
 *
 * Every time and day is IST (the engine's calendar), whatever zone the browser runs in.
 */

const MINUS = '−'
const NUMERIC = /^([+-]?)(\d+)(\.\d+)?$/
const ZERO_DIGITS = /^0+(\.0+)?$/
const TZ = 'Asia/Kolkata'

// ------------------------------------------------------------------------------------------ money

/** "100113" -> "1,00,113": the last three digits, then pairs (Indian grouping). Digits only. */
export function groupIndian(intDigits: string): string {
  if (intDigits.length <= 3) return intDigits
  const pairs: string[] = []
  let rest = intDigits.slice(0, -3)
  while (rest.length > 2) {
    pairs.unshift(rest.slice(-2))
    rest = rest.slice(0, -2)
  }
  if (rest) pairs.unshift(rest)
  return `${pairs.join(',')},${intDigits.slice(-3)}`
}

interface Num {
  neg: boolean
  zero: boolean
  body: string // grouped digits with the original fraction (a one-digit one padded to paise), no sign
}

function num(v: string): Num | null {
  const m = NUMERIC.exec(v.trim())
  if (!m) return null
  const digits = `${m[2]}${m[3] ?? ''}`
  // ".0" -> ".00": a float-born Decimal ("2445.0") would otherwise print as "₹2,445.0". Lossless —
  // a trailing zero only; whole numbers and longer fractions keep the engine's digits.
  const frac = m[3] && m[3].length === 2 ? `${m[3]}0` : (m[3] ?? '')
  return { neg: m[1] === '-', zero: ZERO_DIGITS.test(digits), body: `${groupIndian(m[2])}${frac}` }
}

const missing = (v: string | null | undefined): v is null | undefined | '' => v === null || v === undefined || v.trim() === ''

/** "100113.30" -> "₹1,00,113.30"; "-5" -> "−₹5"; missing -> null; unparseable -> as sent. */
export function inr(v: string | null | undefined): string | null {
  if (missing(v)) return null
  const n = num(v)
  if (!n) return v
  return `${n.neg && !n.zero ? MINUS : ''}₹${n.body}`
}

/** P&L: "+₹113.30" / "−₹1,840.60"; zero carries no sign ("-0.00" -> "₹0.00"). */
export function signedInr(v: string | null | undefined): string | null {
  if (missing(v)) return null
  const n = num(v)
  if (!n) return v
  if (n.zero) return `₹${n.body}`
  return `${n.neg ? MINUS : '+'}₹${n.body}`
}

/** 1 / -1 / 0 read off the string; null when missing or unparseable. */
export function signOf(v: string | null | undefined): 1 | -1 | 0 | null {
  if (missing(v)) return null
  const n = num(v)
  if (!n) return null
  return n.zero ? 0 : n.neg ? -1 : 1
}

/** A price level: grouped, no currency symbol ("2607.60" -> "2,607.60"). */
export function price(v: string | null | undefined): string | null {
  if (missing(v)) return null
  const n = num(v)
  if (!n) return v
  return `${n.neg && !n.zero ? MINUS : ''}${n.body}`
}

/** USD budget figure rounded to cents FOR DISPLAY ONLY ("81.6487" -> "$81.65"). */
export function usd(v: string | null | undefined): string | null {
  if (missing(v)) return null
  const x = Number(v)
  if (!Number.isFinite(x)) return v
  const s = Math.abs(x).toFixed(2)
  return `${x < 0 && !ZERO_DIGITS.test(s) ? MINUS : ''}$${s}`
}

/** A value already in percent: 0.8 -> "0.80%", -1.234 (1 digit) -> "−1.2%". */
export function pct(v: number | null, digits = 2): string | null {
  if (v === null || !Number.isFinite(v)) return null
  const s = Math.abs(v).toFixed(digits)
  return `${v < 0 && !ZERO_DIGITS.test(s) ? MINUS : ''}${s}%`
}

/** A fraction as a whole percent: 0.5 -> "50%". */
export function ratePct(v: number | null): string | null {
  if (v === null || !Number.isFinite(v)) return null
  return `${Math.round(v * 100)}%`
}

// ------------------------------------------------------------------------------------------ words

/** "per_sector_exposure" -> "Per sector exposure": the fallback label for any unknown engine value. */
export function humanize(id: string): string {
  const words = id.replace(/[._-]+/g, ' ').trim().toLowerCase()
  return words ? words[0].toUpperCase() + words.slice(1) : ''
}

// ------------------------------------------------------------------------------------------ time (IST)

const DAY_FMT = new Intl.DateTimeFormat('en-CA', { timeZone: TZ, year: 'numeric', month: '2-digit', day: '2-digit' })
// hourCycle h23, not hour12:false: some ICU builds render midnight as "24:00" under en-GB.
const TIME_FMT = new Intl.DateTimeFormat('en-GB', {
  timeZone: TZ,
  hour: '2-digit',
  minute: '2-digit',
  second: '2-digit',
  hourCycle: 'h23',
})
const LABEL_FMT = new Intl.DateTimeFormat('en-GB', { timeZone: TZ, weekday: 'short', day: 'numeric', month: 'short' })
const DAY_RE = /^\d{4}-\d{2}-\d{2}$/

function part(parts: Intl.DateTimeFormatPart[], type: Intl.DateTimeFormatPartTypes): string {
  return parts.find((p) => p.type === type)?.value ?? ''
}

function parse(iso: string | null | undefined): Date | null {
  if (!iso) return null
  const at = new Date(iso)
  return Number.isNaN(at.getTime()) ? null : at
}

function dayOf(at: Date): string {
  const p = DAY_FMT.formatToParts(at)
  return `${part(p, 'year')}-${part(p, 'month')}-${part(p, 'day')}`
}

/** Noon IST on a calendar day: the same date in every zone, safe for weekday maths. */
function noonOf(day: string): Date {
  return new Date(`${day}T12:00:00+05:30`)
}

/** ISO-8601 -> the IST calendar day "YYYY-MM-DD"; '' when missing or unparseable. */
export function istDay(iso: string | null | undefined): string {
  const at = parse(iso)
  return at ? dayOf(at) : ''
}

export function todayIst(now: Date = new Date()): string {
  return dayOf(now)
}

/** "14:43" (or "14:43:05") IST; '—' when missing; an unparseable stamp is shown as sent. */
export function istTime(iso: string | null | undefined, opts: { seconds?: boolean } = {}): string {
  if (!iso) return '—'
  const at = parse(iso)
  if (!at) return iso
  const p = TIME_FMT.formatToParts(at)
  const hm = `${part(p, 'hour')}:${part(p, 'minute')}`
  return opts.seconds ? `${hm}:${part(p, 'second')}` : hm
}

/** "Thu 5 Nov" for a "YYYY-MM-DD" day; anything else as sent. */
export function shortDate(day: string): string {
  if (!DAY_RE.test(day)) return day
  const at = noonOf(day)
  if (Number.isNaN(at.getTime())) return day
  const p = LABEL_FMT.formatToParts(at)
  return `${part(p, 'weekday')} ${part(p, 'day')} ${part(p, 'month')}`
}

function shiftDay(day: string, days: number): string {
  const at = noonOf(day)
  at.setUTCDate(at.getUTCDate() + days)
  return dayOf(at)
}

/** "Today, Thu 8 Oct" / "Yesterday, Wed 7 Oct" / "Sun 4 Oct" / "Undated". */
export function dayLabel(day: string, now: Date = new Date()): string {
  if (!day) return 'Undated'
  const today = todayIst(now)
  if (day === today) return `Today, ${shortDate(day)}`
  if (day === shiftDay(today, -1)) return `Yesterday, ${shortDate(day)}`
  return shortDate(day)
}

/** "10:30" for a stamp from today (IST), "Wed 7 Oct, 13:00" otherwise. */
export function istStamp(iso: string | null | undefined, now: Date = new Date()): string {
  if (!iso) return '—'
  const day = istDay(iso)
  if (!day) return iso
  return day === todayIst(now) ? istTime(iso) : `${shortDate(day)}, ${istTime(iso)}`
}
