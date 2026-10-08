# Dashboard Redesign Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development or
> superpowers:executing-plans, task by task. Steps use `- [ ]` checkboxes. Execution split (project
> policy, CLAUDE.md + FABLE_HANDOFF §6 on an Opus session): judgement-heavy tasks (1, 3, 4, 5, 6, 7)
> run inline on Opus; spec-determined tasks (2, 8, 9, 10) go to the `implementer` agent (Sonnet) with
> a full brief written at dispatch from the code that exists by then; review via fresh reviewers.

**Goal:** Rebuild the owner dashboard as a tabbed, plain-language workspace on a verified design
system, with the same data, the same three owner writes, and no engine change.

**Architecture:** Keep the data layer (`api.ts`, `types.ts`, `hooks.ts`) and add pure logic modules
(`lib/format.ts`, `lib/alerts.ts`, `lib/recs.ts`, `copy/glossary.ts`) that are unit-tested with
`node --test`. UI = primitives (`ui/`) + shell (`shell/`) + one view per tab (`views/`), styled by
token-driven CSS split by responsibility so views can be built in parallel.

**Tech Stack:** React 19, TypeScript 6 (strict, `erasableSyntaxOnly`), Vite 8, hand-rolled CSS custom
properties, Node 22.20 `node --test` with built-in type stripping (no new dependency).

**Spec:** `runbooks/design-2026-10-08-dashboard-redesign.md` (read it first; section numbers below
refer to it).

## Global Constraints

- No external request of any kind: no CDN, no webfont, no remote image (R10).
- No change outside `dashboard/`, `runbooks/` docs, `IMPLEMENTATION_PLAN.md` (one addendum) — no
  engine/API change; `api.ts` and `types.ts` keep their shapes; `hooks.ts` only adds `failedKeys`.
- Owner writes stay exactly `POST /mode` (OFF / RECOMMEND), `POST /config/trade_window`,
  `POST /paper`; no AUTO, kill reset, paper reset, `/taken`, `/closed` paths.
- Money strings are never parsed to do arithmetic on what is printed; rupee grouping is a string
  transform (`100113.30` → `₹1,00,113.30`). USD budget prints to cents with the exact string in the hint.
- Every displayed time is IST via `Intl` with `timeZone: 'Asia/Kolkata'`; never `Date#toTimeString`.
- Owner directives preserved: IST day folds (today + any day holding a live rec start open), Telegram
  messages above News, paper ON confirms / OFF confirms only with waiting entry orders.
- Tokens exactly as spec §6 (`--bg #0b0f15` … `--control #6b7a95`); sentence case; no uppercase
  tracked labels; `tabular-nums` only in tables and axis ticks.
- Targets designed at 32 px (44 px on `pointer: coarse`); 24 px hard floor.
- No `window.confirm` / `window.alert` anywhere.
- No `Co-Authored-By` trailer on any commit.

## Review Focus

1. **Odd money strings** (`-0.00`, `100`, `81.6487`, `12345678.90`, `''`, `null`): grouping, sign and
   cents must be right with no float conversion → tests in Task 1 (`format.test.ts`).
2. **Browser not in IST / UTC-stamped rows near midnight** (`2026-10-04T19:30:00Z` is 5 Oct IST): day
   labels and times must be IST → tests in Task 1, plus the suite re-run under `TZ=America/New_York`.
3. **Stub and partial API shapes** (`headroom: {}`, `budget: {}` + `degrade_tier: null`, `/paper`
   `{enabled:false}`, digest `status: null`, mode nulls): no crash, quiet empty states, **no false
   alert** (unknown digest ≠ stale) → tests in Task 1 (`alerts.test.ts`), fixture `EMPTY=1` in Task 2.
4. **Unknown or long values** (new risk state, verdict, rule ID, notification kind; 600-char thesis;
   long symbol): humanised fallback, no clipping at 390 px → tests in Task 1 (`glossary.test.ts`),
   DOM overflow assertions in Task 12.
5. **Owner-write failures** (409 / 422 / 501 / network error; double click): a visible message with the
   engine's reason, no optimistic state change, controls busy while in flight → fixture POST routes in
   Task 2, checks in Task 4 step list and Task 12.

---

## File structure

```
dashboard/
  package.json                 + "test" script
  tsconfig.json                + reference tsconfig.test.json
  tsconfig.test.json           NEW: tests/ with node types, nodenext
  fixture_server.mjs           + scenarios ALERTS=1, EMPTY=1; POST /mode, POST /config/trade_window
  README.md                    rewritten "Runtime shape" / panels section
  index.html                   title, theme-color
  public/favicon.svg           new mark
  tests/format.test.ts  alerts.test.ts  glossary.test.ts  recs.test.ts
  src/
    main.tsx                   imports styles/*.css
    App.tsx                    shell + tab routing (rewritten)
    api.ts  types.ts           unchanged
    hooks.ts                   usePoll also returns failedKeys
    lib/format.ts              money, signs, USD, IST time/day labels
    lib/alerts.ts              deriveAlerts(), health summary, tab badges
    lib/recs.ts                isLive(), entry text, rec groupings (moved from RecommendationsPanel)
    lib/useHashTab.ts          tab <-> location.hash
    copy/glossary.ts           every label + hint + engine term (spec §9 + Appendix A)
    styles/tokens.css base.css components.css shell.css
    ui/  Icon Card Pill Hint StatTile Meter DataTable Switch Segmented Popover ConfirmDialog Toast EmptyState DayFold KeyValue
    shell/ AppBar HealthPill ModeControl PaperSwitch HoursControl Connection Tabs TokenGate
    views/ Overview Recommendations Paper Performance Activity System (+ one .css each)
    views/parts/ AlertsCard RecCard RecRow PositionsTable PaperSnapshot TodayStrip PaperStatus
                 EquityChart PaperTables Scorecard RiskChecks TelegramMessages NewsWatchlist
                 LiveUpdates EngineStatus RiskLimits AiBudget
  src/components/              DELETED in Task 11 (old panels)
```

---

### Task 1: Pure logic — format, glossary, recs, alerts (TDD, inline)

**Files:** Create `dashboard/tsconfig.test.json`, `dashboard/tests/{format,glossary,recs,alerts}.test.ts`,
`dashboard/src/lib/{format,recs,alerts}.ts`, `dashboard/src/copy/glossary.ts`; modify
`dashboard/package.json` (script), `dashboard/tsconfig.json` (reference).

**Interfaces (produced):**

```ts
// lib/format.ts — all pure, DOM-free, explicit .ts imports
export function groupIndian(intDigits: string): string            // "100113" -> "1,00,113"
export function inr(v: string | null | undefined): string | null   // "100113.30" -> "₹1,00,113.30"; "-5" -> "−₹5"; null/'' -> null
export function signedInr(v: string | null | undefined): string | null // "+₹113.30" / "−₹1,840.60" / "₹0.00"
export function signOf(v: string | null | undefined): 1 | -1 | 0 | null
export function usd(v: string | null | undefined): string | null   // "81.6487" -> "$81.65" (display only)
export function price(v: string | null | undefined): string | null // grouped, no ₹ ("2607.60" -> "2,607.60")
export function istDay(iso: string | null | undefined): string     // "YYYY-MM-DD" IST or ''
export function todayIst(now?: Date): string
export function istTime(iso: string | null | undefined, opts?: { seconds?: boolean }): string // "14:43" / '—'
export function istStamp(iso: string | null | undefined, now?: Date): string // "14:43" today, "Wed 7 Oct, 13:00" otherwise
export function dayLabel(day: string, now?: Date): string          // "Today · Thu 8 Oct" / "Yesterday · Wed 7 Oct" / "Sun 4 Oct" / "Undated"
export function shortDate(day: string): string                     // "Wed 5 Nov"
export function pct(v: number | null, digits?: number): string | null // 0.8 -> "0.80%" (value already in percent)
export function ratePct(v: number | null): string | null           // 0.5 -> "50%"
export function humanize(id: string): string                       // "per_sector_exposure" -> "Per sector exposure"

// copy/glossary.ts
export interface Term { label: string; hint?: string; term?: string }
export function modeTerm(v: string | null | undefined): Term
export function riskTerm(v: string | null | undefined): Term
export function tierTerm(v: string | null | undefined): Term
export function verdictTerm(v: string | null | undefined): Term
export function actionTerm(v: string | null | undefined): Term     // human_action incl. null -> "Waiting for you"
export function kindTerm(v: string | null | undefined): Term
export function styleTerm(v: string | null | undefined): Term
export function productTerm(v: string | null | undefined): Term
export function originTerm(v: string | null | undefined): Term
export function protectionTerm(v: string | null | undefined): Term
export function positionStateTerm(v: string | null | undefined): Term
export function haltTerm(cause: string): Term
export function outcomeTerm(v: string | null | undefined): Term
export function closeReasonTerm(v: string | null | undefined): Term
export function ruleTerm(ruleId: string): Term                     // Appendix A; fallback humanize
export function strategyName(id: string | null | undefined): Term  // "hi52" -> {label:"52-week-high continuation", term:"hi52"}
export function agentName(id: string | null | undefined): Term
export function digestTerm(v: string | null | undefined): Term
export function deliveryTerm(row: { status: string | null; attempts: number | null }): Term
export function severityTerm(v: string | null | undefined): Term
export function proposalActionTerm(v: string | null | undefined): Term
export const COPY: { /* every fixed string of spec §9 owner-write copy, keyed */ }

// lib/recs.ts
export function isLive(row: RecommendationRow, nowMs: number): boolean   // moved verbatim
export function entryText(rec: RecommendationPayload | null): string      // "₹2,445.00" or "₹2,440.00 – ₹2,450.00" or "—"
export function liveRecs(rows: RecommendationRow[], nowMs: number): RecommendationRow[]

// lib/alerts.ts
export type Level = 'critical' | 'warning' | 'notice'
export type TabId = 'overview' | 'recommendations' | 'paper' | 'performance' | 'activity' | 'system'
export interface Alert { id: string; level: Level; title: string; body: string; tab?: TabId; action?: 'signout' | 'ack-telegram' }
export interface AlertInput {
  snapshot: Snapshot; failedKeys: string[]; unauthorized: boolean; polled: boolean
  notifications: NotificationRow[]; ackedFailureId: string | null
}
export function deriveAlerts(i: AlertInput): Alert[]                // sorted critical -> warning -> notice
export function health(alerts: Alert[]): { level: Level | 'ok'; text: string }
export function tabBadges(i: AlertInput, nowMs: number): Partial<Record<TabId, { text: string; tone: 'accent' | 'warn' | 'crit' }>>
```

- [ ] **Step 1: test harness.** Create `tsconfig.test.json`:

```json
{
  "compilerOptions": {
    "tsBuildInfoFile": "./node_modules/.tmp/tsconfig.test.tsbuildinfo",
    "target": "es2023", "lib": ["ES2023"], "types": ["node"], "skipLibCheck": true,
    "module": "nodenext", "allowImportingTsExtensions": true, "verbatimModuleSyntax": true,
    "moduleDetection": "force", "noEmit": true, "strict": true,
    "noUnusedLocals": true, "noUnusedParameters": true, "erasableSyntaxOnly": true
  },
  "include": ["tests"]
}
```

Add `{ "path": "./tsconfig.test.json" }` to `tsconfig.json` references and
`"test": "node --test tests/"` to `package.json` scripts.

- [ ] **Step 2: failing tests for format.** `tests/format.test.ts`:

```ts
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { dayLabel, groupIndian, humanize, inr, istDay, istStamp, istTime, price, signedInr, usd } from '../src/lib/format.ts'

test('Indian grouping', () => {
  assert.equal(groupIndian('5'), '5')
  assert.equal(groupIndian('999'), '999')
  assert.equal(groupIndian('1000'), '1,000')
  assert.equal(groupIndian('100113'), '1,00,113')
  assert.equal(groupIndian('12345678'), '1,23,45,678')
})
test('rupees keep the engine digits, add grouping and sign', () => {
  assert.equal(inr('100113.30'), '₹1,00,113.30')
  assert.equal(inr('100'), '₹100')
  assert.equal(inr('-1840.60'), '−₹1,840.60')
  assert.equal(inr('12345678.90'), '₹1,23,45,678.90')
  assert.equal(inr(''), null)
  assert.equal(inr(null), null)
  assert.equal(signedInr('113.30'), '+₹113.30')
  assert.equal(signedInr('-1840.60'), '−₹1,840.60')
  assert.equal(signedInr('-0.00'), '₹0.00')
  assert.equal(signedInr('0'), '₹0')
  assert.equal(price('2607.60'), '2,607.60')
})
test('usd rounds for display only', () => {
  assert.equal(usd('81.6487'), '$81.65')
  assert.equal(usd('0'), '$0.00')
  assert.equal(usd(undefined), null)
})
test('IST day and time regardless of the machine zone', () => {
  assert.equal(istDay('2026-10-04T19:30:00Z'), '2026-10-05')
  assert.equal(istDay('2026-10-08T10:15:03+05:30'), '2026-10-08')
  assert.equal(istDay('garbage'), '')
  assert.equal(istTime('2026-10-08T09:13:00Z'), '14:43')
  assert.equal(istTime('2026-10-08T09:13:05Z', { seconds: true }), '14:43:05')
  assert.equal(istTime(null), '—')
})
test('stamps and day labels', () => {
  const now = new Date('2026-10-08T09:00:00Z') // 14:30 IST Thu
  assert.equal(istStamp('2026-10-08T05:00:00Z', now), '10:30')
  assert.equal(istStamp('2026-10-07T07:30:00Z', now), 'Wed 7 Oct, 13:00')
  assert.equal(dayLabel('2026-10-08', now), 'Today · Thu 8 Oct')
  assert.equal(dayLabel('2026-10-07', now), 'Yesterday · Wed 7 Oct')
  assert.equal(dayLabel('2026-10-04', now), 'Sun 4 Oct')
  assert.equal(dayLabel('', now), 'Undated')
})
test('humanize', () => {
  assert.equal(humanize('per_sector_exposure'), 'Per sector exposure')
  assert.equal(humanize('NEW_STATE'), 'New state')
})
```

- [ ] **Step 3:** `npm test` → FAIL (module not found). **Step 4:** implement `lib/format.ts` (string
  grouping: split sign / integer / fraction; `Intl.DateTimeFormat('en-GB', { timeZone: 'Asia/Kolkata',
  … , hourCycle: 'h23' })` formatters). **Step 5:** `npm test` → PASS.

- [ ] **Step 6: failing tests for glossary + recs.** `tests/glossary.test.ts`:

```ts
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { actionTerm, riskTerm, ruleTerm, strategyName, tierTerm, verdictTerm } from '../src/copy/glossary.ts'

test('known terms translate and keep the engine term', () => {
  assert.deepEqual(riskTerm('FROZEN').label, 'New positions paused')
  assert.equal(riskTerm('FROZEN').term, 'FROZEN')
  assert.equal(tierTerm('DG3').label, 'Minimal')
  assert.equal(verdictTerm('owner_approval_required').label, 'Needs your OK')
  assert.equal(actionTerm(null).label, 'Waiting for you')
  assert.equal(ruleTerm('per_trade_risk').label, 'Risk per trade over the limit')
  assert.equal(strategyName('hi52').label, '52-week-high continuation')
})
test('unknown values fall back, never throw', () => {
  assert.equal(riskTerm('MELTDOWN').label, 'Meltdown')
  assert.equal(ruleTerm('paper_open_positions').label, 'Paper open positions')
  assert.equal(strategyName('zz9').label, 'zz9')
  assert.equal(riskTerm(null).label, '—')
})
```

`tests/recs.test.ts`: `isLive` true for pending + future `valid_until`, false for acted/expired/
unparseable; `entryText` for equal and unequal zone ends and a null rec. **Step 7:** run → FAIL;
**Step 8:** implement `copy/glossary.ts` (all spec §9 rows + Appendix A) and `lib/recs.ts`;
**Step 9:** run → PASS.

- [ ] **Step 10: failing tests for alerts.** `tests/alerts.test.ts` covers: empty snapshot + not polled
  → `[]`; KILLED → first alert critical "Kill switch is on — all trading is stopped"; FROZEN → warning;
  recommended OPEN without `owner_protected_at` → warning naming the symbol; all 9 keys failed →
  critical "Can't reach the engine"; 2 keys failed → warning listing friendly names; unauthorized →
  critical with `action: 'signout'`; digest `null` → **no** news notice; digest `stale` → notice;
  `/paper` stub → no paper notice; DG2/DG3/DG4 → notice/warning/critical; failed notification newer
  than `ackedFailureId` → warning with `action: 'ack-telegram'`, same id acked → absent; mode OFF →
  notice; window null with mode loaded → warning; sort order critical → warning → notice;
  `health([])` → `{level:'ok', text:'All normal'}`. **Step 11:** FAIL; **Step 12:** implement;
  **Step 13:** PASS; also `$env:TZ='America/New_York'; npm test` → PASS.

- [ ] **Step 14: commit** `Dashboard redesign: pure format/glossary/recs/alerts modules with node tests`.

### Task 2: Fixture scenarios and owner-write routes (delegate: implementer)

**Files:** `dashboard/fixture_server.mjs`, `dashboard/README.md` (fixture section only).

**Spec for the brief:** add `ALERTS=1` (mode `RECOMMEND` + risk `KILLED`; one recommended OPEN
position unprotected; one notification `failed` with `last_error`, one `pending` with attempts 3;
budget `degrade_tier: 'DG3'` with spend 191.2 of 200; digest `stale` with `as_of`, `age_h: 9.5`,
`stale_max_h: 6`; headroom filled with caps and values incl. one ≥ 90 %; watchlist with 2
originating + 1 context rows; a 600-character thesis on one live rec; a 20-character symbol) and
`EMPTY=1` (every route answers its stub/empty shape: `recommendations: []`, `decisions: []`,
`positions: []`, headroom `{}`, budget `{budget:{}, degrade_tier:null}`, `/paper` `{enabled:false}`,
watchlist `{d:null, watchlist:[], digest:{…, status:null}}`, notifications `{d, rows:[]}`, scorecard
`{label, bench:{}, strategies:{}}`, mode `{mode:null,routing:null,risk_state:null}`, trade window
`{trade_window:null}`). Default scenario: also give headroom realistic values. Add `POST /mode`
(`OFF`/`RECOMMEND` → 200 and update the served mode; `AUTO` → 409 `{ok:false, note:"two-step via
Telegram /mode AUTO + /confirm"}`; anything else → 422) and `POST /config/trade_window` (`start <
end`, both `HH:MM`, buffer ≥ 0 or null → 200 and update; else 422 `{detail:"invalid trade window
(rejected, unchanged)"}`). Fix the generic handler so any non-GET method on a GET route answers 405
instead of the GET body. Done = each scenario starts, `curl` of every route returns the stated shape
(pasted), no `src/` file touched.

### Task 3: Tokens, base styles and UI primitives (inline)

**Files:** create `src/styles/{tokens,base,components}.css`, `src/ui/*.tsx`; modify `src/main.tsx`
(imports), `index.html` (title "Market Trading", `<meta name="theme-color" content="#0b0f15">`),
`public/favicon.svg` (new mark: rising line in a rounded square, `#6aa8ff` on `#151b25`).

**Interfaces (produced):**

```ts
// ui/Icon.tsx
export type IconName = 'check' | 'alert' | 'x' | 'info' | 'clock' | 'logout' | 'chevron-down' |
  'chevron-right' | 'arrow-right' | 'flask' | 'shield' | 'shield-off' | 'live' | 'hint'
export function Icon(p: { name: IconName; size?: number; className?: string }): JSX.Element // aria-hidden
// ui/Card.tsx
export function Card(p: { title: ReactNode; id?: string; tier?: 'primary' | 'secondary' | 'simulated';
  meta?: ReactNode; action?: ReactNode; children: ReactNode; className?: string }): JSX.Element
// ui/Pill.tsx
export type Tone = 'good' | 'warn' | 'crit' | 'note' | 'sim' | 'accent' | 'neutral'
export function Pill(p: { tone?: Tone; icon?: IconName; children: ReactNode; title?: string }): JSX.Element
// ui/Hint.tsx — toggletip
export function Hint(p: { label: string; children: ReactNode; term?: string }): JSX.Element
export function Labelled(p: { t: Term; className?: string }): JSX.Element // label text + Hint when t.hint
// ui/StatTile.tsx
export function StatTile(p: { label: ReactNode; value: ReactNode; delta?: { text: string; sign: 1 | -1 | 0 | null };
  caption?: ReactNode }): JSX.Element
// ui/Meter.tsx
export function Meter(p: { label: ReactNode; used: number | null; limit: number | null; text: ReactNode }): JSX.Element
// ui/DataTable.tsx
export interface Column<R> { key: string; header: ReactNode; cell: (r: R) => ReactNode;
  align?: 'left' | 'right'; nowrap?: boolean }
export function DataTable<R>(p: { columns: Column<R>[]; rows: R[]; rowKey: (r: R) => string;
  caption?: string; empty?: ReactNode; rowClass?: (r: R) => string | undefined }): JSX.Element
// ui/Switch.tsx, ui/Segmented.tsx, ui/Popover.tsx, ui/ConfirmDialog.tsx, ui/Toast.tsx, ui/EmptyState.tsx, ui/DayFold.tsx, ui/KeyValue.tsx
export function Switch(p: { checked: boolean; label: string; disabled?: boolean; busy?: boolean; onChange: (next: boolean) => void; describedBy?: string }): JSX.Element
export function Segmented<V extends string>(p: { label: string; value: V | null; options: { value: V; label: string }[]; disabled?: boolean; onChange: (v: V) => void }): JSX.Element
export function Popover(p: { trigger: (props: { onClick: () => void; 'aria-expanded': boolean; 'aria-controls': string }) => ReactNode; title: string; children: (close: () => void) => ReactNode }): JSX.Element
export interface ConfirmRequest { title: string; body: string; confirm: string; cancel: string; tone?: 'accent' | 'crit' }
export function useConfirm(): { confirm: (r: ConfirmRequest) => Promise<boolean>; dialog: JSX.Element }
export interface ToastMsg { id: number; tone: 'good' | 'crit' | 'note'; text: string }
export function useToasts(): { toasts: ToastMsg[]; push: (tone: ToastMsg['tone'], text: string) => void; dismiss: (id: number) => void; region: JSX.Element }
export function EmptyState(p: { children: ReactNode }): JSX.Element
export function DayFold(p: { day: string; count: number; noun: string; open: boolean; onToggle: () => void }): JSX.Element
export function useDayFolds(alsoOpen?: (day: string) => boolean): { isOpen: (day: string) => boolean; toggle: (day: string) => void } // moved verbatim
export function groupByDay<T>(rows: T[], dayOf: (r: T) => string): { day: string; rows: T[] }[]        // moved verbatim
export function KeyValue(p: { items: { k: ReactNode; v: ReactNode }[]; dense?: boolean }): JSX.Element
```

- [ ] Step 1: tokens.css exactly as spec §6 (+ spacing, radius, type, motion, `--sim-border`), base
  .css (reset, body, focus-visible ring 2 px `--accent` offset 2 px, `prefers-reduced-motion`,
  `pointer: coarse` sizes, layout container), components.css (one block per primitive).
- [ ] Step 2: primitives. Hint closes on Esc / outside click / blur-out, positions its bubble with
  `position: fixed` from the trigger rect (no clipping inside scroll containers), flips above when
  there's no room below.
- [ ] Step 3: `npm run build` → PASS (primitives unused is fine; `noUnusedLocals` is per-file).
- [ ] Step 4: commit `Dashboard redesign: tokens, base styles and UI primitives`.

### Task 4: Shell — app bar, tabs, controls, token gate (inline)

**Files:** `src/shell/*.tsx`, `src/styles/shell.css`, `src/lib/useHashTab.ts`, `src/App.tsx`,
`src/hooks.ts` (`failedKeys: string[]` added to `usePoll`'s return; computed from the same results
array, empty on a clean cycle).

**Interfaces (produced):**

```ts
export function useHashTab(): [TabId, (t: TabId) => void]       // unknown hash -> 'overview'
export function AppBar(p: { health: { level: Level | 'ok'; text: string }; onHealth: () => void;
  mode: ModeResponse | null; paper: PaperResponse | null; tradeWindow: TradeWindowResponse | null;
  conn: ConnState; lastPollAt: string | null; onSetMode: (m: 'OFF' | 'RECOMMEND') => Promise<void>;
  onSetPaper: (on: boolean) => Promise<void>; onSetHours: (b: { start: string; end: string;
  squareoff_buffer_min: number | null }) => Promise<string | null>; onSignOut: () => void;
  tab: TabId; onTab: (t: TabId) => void; badges: ReturnType<typeof tabBadges> }): JSX.Element
```

Behaviour checklist (each verified in the browser at the end of the task):
- Mode → Off asks (`COPY.modeOffDialog`); → Recommend applies directly; buttons disabled while in
  flight; 409/501/422/network → crit toast `Couldn't change the mode: <reason>`; no optimistic change
  (value comes from the next poll; `refresh()` after every write).
- Paper switch: rules of spec §4 + §9; 409 → crit toast with the engine note.
- Hours popover: seeded from the stored window when opened; Save disabled while in flight; success →
  good toast `Saved: 09:30–15:00` + close; 422 → inline error text (spec §9), popover stays open.
- Tabs: APG keyboard; badge text in `aria-label`; hash updates; Back works.
- Token gate copy per spec §9; 401 anywhere → critical alert with Sign out.

- [ ] Step 1–4: implement, `npm run build`, run fixture (`PORT=8498`), verify checklist via
  chrome-devtools (snapshot + one screenshot per width 1440/1024/390). Step 5: commit.

### Task 5: Overview (inline)

**Files:** `src/views/Overview.tsx` + `.css`, `src/views/parts/{AlertsCard,RecCard,PositionsTable,PaperSnapshot,TodayStrip}.tsx`.

- RecCard (live variant): spec §8 layout; `Details` is a `<details>`; manual checklist visible;
  shrink shows `3 (cut from 5)`; `short_flag_higher_tail_risk` → crit pill; exit `date pending`.
- PositionsTable: real rows only (`!is_paper`), OPEN first as served; protection pill per spec §9 with
  the `/protected SYMBOL` instruction in the hint; DataTable stacks under 640 px.
- PaperSnapshot: `tier="simulated"`; stub / not built → one quiet line + no tiles.
- TodayStrip: four link tiles (spec §8) that call `onTab`.
- AlertsCard: `id="alerts"` (the health pill focuses it); `action: 'signout'` renders a Sign out
  button; `action: 'ack-telegram'` renders "Got it", which writes the newest failed notification id to
  `localStorage['mt_ack_failed_notification']` (try/catch; absent storage = never acked) and App
  passes it back as `ackedFailureId`.
- [ ] Steps: build → fixture default + `ALERTS=1` + `EMPTY=1` at three widths → commit.

### Task 6: Recommendations tab (inline)

`src/views/Recommendations.tsx` + `.css`, `src/views/parts/RecRow.tsx`. Day folds via
`useDayFolds(day => liveDays.has(day))` (unchanged directive), history rows → expand to `RecCard`
(history variant: no checklist emphasis). Summary line: `N shown · N today · N waiting for you`.
- [ ] Steps: build → fixture default + `NO_TODAY=1` → commit.

### Task 7: Paper tab (inline; dataviz rules)

`src/views/Paper.tsx` + `.css`, `src/views/parts/{PaperStatus,EquityChart,PaperTables}.tsx`.
EquityChart: 2 px line, end dot 8 px with 2 px surface ring, dashed starting-capital line labelled,
hairline solid grid, crosshair snapping to the nearest session with a tooltip (value first, date
second), keyboard focus on the plot steps through points (←/→), `Show as table` toggle; fewer than 2
points → "The chart starts after the second session." Every chip of the old panel is preserved in
words (spec §10).
- [ ] Steps: build → fixture default + `PAPER_OFF=1` + `PAPER_IDLE=1` → commit.

### Task 8: Performance tab (delegate: implementer)

Brief at dispatch: `views/Performance.tsx` + `.css` + `parts/Scorecard.tsx` using `DataTable` with a
two-level header (spec §8), every column of the old ScorecardPanel (spec §10), copy from
`glossary.ts`/spec §9, 401/error states as today. Done = build green, fixture screenshots at
1440/390 pasted as paths, no file outside the listed ones touched.

### Task 9: Activity tab (delegate: implementer)

Brief at dispatch: `views/Activity.tsx` + `.css` + `parts/{RiskChecks,TelegramMessages,NewsWatchlist,LiveUpdates}.tsx`.
Order: Risk checks → Telegram messages → News watchlist → Live updates (Telegram above News is an
owner directive). Risk checks keep the one-`<tbody>`-per-day fold; reasons through `ruleTerm`; paper
verdict tagged `Paper`. Telegram keeps chronological order + pin-to-newest scrolling behaviour inside
its card (the only scroll container allowed, max-height 480 px, because it is a transcript). Live
updates: sentence per known kind (`mode.changed`, `risk.state`, `kill.state`, `trade_window.changed`,
`budget.state`, `hello`), raw payload under a `<details>`.

### Task 10: System tab (delegate: implementer)

Brief at dispatch: `views/System.tsx` + `.css` + `parts/{EngineStatus,RiskLimits,AiBudget}.tsx` with
`Meter`, `KeyValue`; spec §8 + §9 copy; empty states for unwired headroom/budget; reset countdown from
`window_end`.

### Task 11: Remove the old panels, docs (inline)

Delete `src/components/` (all old panels + `ui.tsx` after its pieces moved); rewrite README "Runtime
shape" + "Panels" sections; add a §3.2.11 addendum line to `IMPLEMENTATION_PLAN.md` pointing to the
spec; `npm run build` + `npm test` green; commit.

### Task 12: Validation sweep (inline + chrome-devtools)

Script `dashboard/tests/dom-checks.js` (evaluated in the page): no page horizontal overflow; for each
`.card`, every descendant's right edge ≤ card's right edge (+1 px) unless inside `.table-scroll`;
every `button, [role=tab], [role=switch], input, summary` ≥ 24×24; computed text colour contrast ≥ 4.5
against the nearest opaque background. Run per tab × scenario (default, NO_TODAY, PAPER_OFF,
PAPER_IDLE, ALERTS, EMPTY) × width (1440, 1024, 390). Lighthouse a11y on Overview ≥ 95.
Accessibility-review + design-critique skills on the screenshots. Fix → re-run until clean. Commit.

### Task 13: Whole-branch review (fresh reviewers)

Three angles in parallel (after market hours): (a) data parity + correctness vs the old panels
(spec §10/§11), (b) accessibility + keyboard, (c) copy accuracy vs engine semantics (spec §9 rows vs
the cited engine files). Verify every finding before fixing; re-run Task 12 after fixes.

### Task 14: Deploy (after 15:30 IST, inline)

`git -C market_trading merge --ff-only wip/dashboard-redesign` (phase3 must not have moved; else
rebase the branch first), `npm run build` in the live tree (no engine restart), confirm the served
`index.html` references the new bundle hash, WORKLOG + COMMANDS entries, commit. Push only with the
owner's permission. Remove the worktree and branch after the merge.
