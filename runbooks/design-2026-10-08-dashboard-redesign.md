# Dashboard redesign — design spec (2026-10-08)

Owner request, 2026-10-08, verbatim intent: make the dashboard friendlier, with text and design that
explain everything in plain terms (no code-level knowledge needed); proper use of space; clear
demarcation; a visible difference between important and unimportant cards; controls that fit on a
navbar get no card of their own; redundant text removed or moved behind a tooltip; understand the
flow, create a design pattern, then build; review and validate every step.

Owner decision (2026-10-08, ~15:15 IST): **tabbed workspace** (over a single prioritised page).

Scope: `dashboard/src`, the fixture server, the dashboard README, and this repo's docs. **No engine/API
change**, no new route consumed, no new owner write. Same three owner controls (mode Off/Recommend,
paper autopilot ON/OFF, trade window). AUTO, kill-switch reset, paper reset and `/taken` `/closed`
stay Telegram-only (R10, §3.5.3). The page still makes zero external requests (R10: no CDN, no
webfont).

---

## 1. The flow this page serves

Phase 2 runs in RECOMMEND. Analysts propose → the risk gate checks each proposal → an approved entry
is delivered as a recommendation on Telegram (and here) → the owner places it in Kite, places the
protective GTT, replies `/taken` and `/protected SYMBOL` → later `/closed`. In parallel the paper
autopilot simulates the analysts' CNC entries, and hindsight scoring rates every delivered entry on
official bars.

The owner comes to the page with these questions, in this order:

| # | Question | Answered by |
|---|---|---|
| 1 | Is everything OK? | Health pill (navbar) → Alerts card (top of Overview) |
| 2 | What do I need to do now? | Overview → *Act now* (live recommendations) + unprotected-position alerts |
| 3 | What do I hold? | Overview → *Your positions* |
| 4 | How is the paper autopilot doing? | Overview → paper snapshot → *Paper* tab |
| 5 | Are the strategies working? | *Performance* tab |
| 6 | Why did the engine do that? | *Activity* tab (risk checks, Telegram messages, live updates) |
| 7 | Is the news feed alive? | *Activity* → News watchlist (+ a notice when stale) |
| 8 | Are we near a limit or the AI budget? | *System* tab |

## 2. What is wrong today (measured 2026-10-08 against the fixture server)

| Problem | Evidence |
|---|---|
| No hierarchy | 12 identical panels, 132 chips on one page; a live BUY carries the same weight as the AI-budget table |
| Safety info clipped | Positions panel (≈⅓ width) cuts the `UNPROTECTED` chip off; the decision log cuts "rules cited" |
| Misleading input | Trade-window END reads "03:00" for 15:00 — the input is 92 px wide and clips "PM" |
| Nested scrolling | `.panel-body` max-height 420 px: Recommendations shows 420 of 1584 px; Paper hides its closed trades |
| Wasted space | `auto-fit minmax(420px)` grid leaves ⅔-empty rows; empty states take a full card |
| Redundancy | Trade window, websocket state and degrade tier each shown twice; live events print raw JSON |
| Jargon | DG0, routing, WS, epoch, entry guard, conf, notional, T+20, excess, `fill+1..exit`, originating/context, digest … |
| Accessibility | Label grey `#6e7681` is 3.46–3.77:1 (fails AA 4.5:1); buttons 23 px tall (< 24 px WCAG 2.2 minimum); 11 px uppercase monospace labels |
| Wrong clock | `hhmmss` formats in the *browser's* zone, while every day label is IST |

Kept because it works: the dark palette with consistent status colours, the IST day folds (owner
directive 2026-09-08), per-key poll merging (one failing route never blanks the page), delivery truth
on every Telegram message (owner directive 2026-08-21).

## 3. Information architecture

A sticky navbar and six tabs. The tab lives in the URL hash (`#overview`, `#recommendations`,
`#paper`, `#performance`, `#activity`, `#system`) so reload, bookmarks and Back work; no hash means
Overview.

| Tab | Holds | Badge (only when non-zero) |
|---|---|---|
| **Overview** | Alerts · Act now (live recommendations only) · Your positions · Paper snapshot · Today strip | — |
| **Recommendations** | Full ledger, IST day folds (today and any day holding a live rec open) | live count (accent) |
| **Paper** | Autopilot status, halts, 4 stat tiles, equity chart, positions, waiting orders, closed trades, health counters | `!` while a paper halt is active or the build failed (warning) |
| **Performance** | Per-strategy scorecard: hindsight recommendations + paper trades | — |
| **Activity** | Risk checks (decision log) · Telegram messages · News watchlist · Live updates | undelivered Telegram messages (critical/warning) |
| **System** | Engine status · Risk limits (headroom) · AI usage budget | `!` when AI level ≥ Saving (DG2) or any limit ≥ 90 % used |

Old panel → new home:

| Old panel | New home |
|---|---|
| Status header | Navbar (health pill, controls, connection) + Alerts + System → Engine status |
| Recommendations | Overview *Act now* (live only) + Recommendations tab (everything) |
| Positions | Overview *Your positions* |
| Paper autopilot | Overview snapshot + Paper tab |
| Scorecard | Performance tab |
| Decision log | Activity → Risk checks |
| Risk headroom | System → Risk limits |
| Agent budget | System → AI usage budget |
| Trade window form | Navbar → *Hours* popover |
| Live events | Activity → Live updates |
| Notifications | Activity → Telegram messages (**above** News — owner directive 2026-09-08) |
| News / catalyst watchlist | Activity → News watchlist |

## 4. Navbar

Desktop (≥ 1024 px), two rows, both sticky:

```
[mark] Market Trading  (✓ All normal)          Mode [ Off | Recommend ]  Paper autopilot (●—)  [⏱ Hours 09:30–15:00 ▾]   ● Live · 14:43   [⎋]
 Overview   Recommendations (2)   Paper   Performance   Activity (1)   System
```

Below 1024 px the brand row keeps brand + health pill + sign-out; the controls wrap onto their own
row; only the tab row stays sticky (it scrolls horizontally with edge fades on a phone).

- **Health pill** — the worst active alert wins: `✓ All normal` (good) · `! N issues` (warning) ·
  `✕ Kill switch on` / `✕ N problems` (critical). Notices alone show `i N notices`. Clicking it opens
  Overview and moves focus to the Alerts card. `aria-live="polite"` announces changes.
- **Mode** — segmented control Off | Recommend (radiogroup). AUTO is never offered; when the engine is
  in AUTO the control shows a read-only `Auto (set on Telegram)` pill instead. Switching to Off asks
  first (§8). Hint: plain meaning of each mode + auto-order routing.
- **Paper autopilot** — a switch (`role="switch"`). Turning ON always confirms; turning OFF confirms
  only when waiting paper entry orders would be cancelled (today's rule, D13). When the autopilot is
  not built, switching ON is disabled with the reason in its hint (the engine would answer 409); if it
  is stored ON while not built it reads `On (not running)` and can still be switched OFF (OFF always
  applies). Hidden entirely when `GET /paper` answers the unwired stub.
- **Hours** — button showing the trade window; opens a popover editor (§8). `Hours not set` in warning
  tone when unset.
- **Connection** — `● Live · 14:43:39` when the websocket is open, `○ Updated 14:43:39` otherwise; the
  hint says live updates reconnect on their own and data refreshes every 10 s.
- **Sign out** — icon button, label "Sign out (forget the token in this browser)".

## 5. Alerts (computed client-side, pure function, unit-tested)

One Alerts card at the top of Overview, sorted critical → warning → notice. Hidden when empty. Every
alert = icon + plain title + one-line "what it means / what to do" + optional link to the tab that
shows the detail. Colour never carries the meaning alone.

| Condition (source) | Level | Title | Body |
|---|---|---|---|
| `risk_state = KILLED` (/mode) | critical | Kill switch is on — all trading is stopped | Nothing new will be opened. Reset only on Telegram: `/kill_reset`, then `/confirm`. |
| 401 from any route | critical | The dashboard token was rejected | Sign out and enter the current token. [Sign out] |
| every poll route failed | critical | Can't reach the engine | Nothing has loaded. Check that the engine is running; the page retries every 10 s. |
| `risk_state = CLOSE_ONLY` | critical | Closing only — no new positions | A risk limit allows exits only. It clears when its cause clears. |
| Telegram row `failed` (/notifications), newer than the last one acknowledged | warning | N Telegram message(s) were not delivered | You may have missed them — read them in Activity. [Open Activity] [Got it] — "Got it" stores the newest failed row's id in `localStorage` and hides the alert until a newer failure arrives (the Activity badge and the rows stay). |
| AI level DG4 (/budget) | critical | AI analysis is stopped for the week | The weekly AI budget is used up. It refills Thursday 14:00 IST. |
| `risk_state = FROZEN` | warning | New positions are paused | A risk check is blocking new entries until its cause clears. Exits and stops still work. If you paused entries yourself, `/resume_entries` on Telegram resumes them. |
| recommended OPEN position without `owner_protected_at` (/positions) | warning | SYMBOL has no confirmed stop-loss | Place its GTT stop-loss in Kite, then reply `/protected SYMBOL` on Telegram. |
| Telegram row pending with attempts > 0 | warning | Telegram delivery is retrying (N) | The engine keeps retrying. Check Activity if it doesn't clear. |
| some (not all) poll routes failed | warning | Some data couldn't load: positions, budget … | Showing the last values received; retrying every 10 s. |
| trade window unset | warning | No trading hours are set | New entries are blocked until you set hours (top bar). |
| AI level DG3 | warning | AI analysts are paused | Weekly AI spend is near the limit: only scanner alerts until Thursday 14:00 IST. |
| mode OFF | notice | The engine is off | No new trade ideas. Exits and protective actions still run. Switch to Recommend to resume. |
| AI level DG2 | notice | AI is in saving mode | Some background AI checks are reduced to stay in budget. Recommendations continue. |
| digest `stale` / `missing` (/news/watchlist) | notice | News summary is stale / missing | News-catalyst (`cat`) entries are off until it refreshes. |
| paper halt active (/paper) | notice | Paper autopilot paused: <halt in words> | Simulated only. [Open Paper] |
| paper subsystem enabled but not built | notice | Paper autopilot failed to start | Nothing is simulated until the engine restarts. |

The Telegram conditions read the notifications journal (60 s hook); the rest read the 10 s poll.
A failed Telegram row is a *warning*, not critical, because the journal keeps it all day: one 09:30
failure must not paint the page red until midnight (alert fatigue, §14). Paper verdicts cite the same
gate rule IDs as real ones (no `paper_*` rule IDs exist in the engine), so one rule glossary
(Appendix A) serves both.

## 6. Design tokens (dark only; every pair below measured, WCAG 2.x)

| Token | Value | Use | Contrast |
|---|---|---|---|
| `--bg` | `#0b0f15` | page | — |
| `--surface-1` | `#11161e` | secondary cards | — |
| `--surface-2` | `#151b25` | primary cards | — |
| `--surface-3` | `#1c2431` | controls, hover, table header | — |
| `--border` | `#232c3a` | card hairlines (decorative) | — |
| `--control` | `#6b7a95` | input/switch/secondary-button outline | 3.60 on s3 · 3.98 on s2 · 4.18 on s1 (≥ 3) |
| `--text-1` | `#e7ecf3` | primary text, values | 13.1–16.2 |
| `--text-2` | `#b3bfce` | secondary text | 8.4–10.3 |
| `--text-3` | `#93a1b5` | labels, meta, table headers | 5.95–7.32 (old label grey: 3.46) |
| `--accent` | `#6aa8ff` | links, focus ring, selected tab, switch-on | 6.43–7.92; `#0b0f15` on it 7.92 |
| `--good` | `#4cc764` | OK / profit | pill text on own 14 % tint 6.17–6.53 |
| `--warn` | `#e6b24c` | attention | 6.81–7.19 |
| `--crit` | `#ff7b72` | problem / loss | 5.54–5.86; `#0b0f15` on it 7.62 |
| `--note` | `#79b8ff` | notices / info | 6.34–6.71 |
| `--sim` | `#b39dfa` | **simulated money** (paper) | 5.85–6.18 |

Status pills: text in the status colour on a 14 % tint of it, plus an icon. Values and labels never
wear a series colour (dataviz rule); profit/loss text may use good/crit because it is a status cue,
always with an explicit `+`/`−` sign.

**Real vs simulated.** Every paper surface carries a `Simulated` tag (flask icon, `--sim`) and a
dashed `--sim` border at 45 % opacity. Real-money cards are solid. A paper verdict in the risk-check
table carries a small `Paper` tag.

Type (system sans `system-ui, "Segoe UI", -apple-system, Roboto, sans-serif`; mono `ui-monospace,
"Cascadia Mono", Consolas, monospace` only for raw engine terms and IDs): 12 (meta, table head) · 13
(table body) · 14 (body) · 16 (card title) · 20 (section title) · 28 (stat value). Weights 400/500/600.
Line height 1.5 body, 1.25 titles. Sentence case everywhere; no uppercase-tracked labels.
`tabular-nums` only in table columns and axis ticks; stat values stay proportional (dataviz rule).

Spacing 4 · 8 · 12 · 16 · 24 · 32. Radius 6 (controls) · 10 (cards) · 999 (pills). Primary cards:
`--surface-2` + border + soft shadow; secondary cards: `--surface-1` + border, flat; alert rows: 14 %
tint + 4 px left bar. Motion 120–160 ms ease-out, none under `prefers-reduced-motion`. Content max
width 1440 px, gutters 24 px (16 px under 640 px). Interactive targets are designed at 32 px
(44 px on `pointer: coarse`); 24 px is the hard floor the validation script asserts (WCAG 2.2 2.5.8).

## 7. Components

| Component | Contract | Accessibility |
|---|---|---|
| `AppBar` | brand, health pill, controls, connection, sign-out; tab row | `<header>`; tab row is `<nav aria-label="Sections">` |
| `Tabs` | six tabs, badges, hash-synced | APG tabs: `role=tablist/tab/tabpanel`, roving tabindex, ←/→/Home/End |
| `Card` | `tier = primary \| secondary \| simulated`, title, optional meta + action slot | `<section aria-labelledby>`; one `h2` per card |
| `AlertList` | rows from `deriveAlerts()` | `role="region"` labelled "Needs attention"; icons `aria-hidden`, level as text |
| `Pill` | tone (good/warn/crit/note/sim/neutral) + icon + text | icon decorative; the text carries meaning |
| `Hint` | toggletip: ⓘ button → bubble with plain explanation + "Engine term: `X`" | button `aria-expanded`, bubble `role="tooltip"` + `aria-describedby`; hover/focus/tap; Esc closes |
| `StatTile` | label · value · optional signed delta · optional caption | value is text, never colour-only |
| `Meter` | used / limit with label; fill tone ok→warn (≥ 80 %)→crit (≥ 100 %); track a lighter step | `role="meter"` (`aria-valuenow/min/max`) + visible numbers |
| `EquityChart` | 2 px line, end dot, dashed starting-capital reference, crosshair tooltip, "Show as table" toggle | `role="img"` summary + the table twin |
| `DataTable` | columns with `align`/`priority`; wraps in a horizontal scroller with edge shadows; under 640 px rows stack as label/value cards | real `<table>`; stacked mode keeps `<th scope>` semantics via `data-label` |
| `Switch` | on/off with label | `role="switch"` + `aria-checked` |
| `Segmented` | 2–3 options | `role="radiogroup"` + arrow keys |
| `Popover` | anchored panel (trade-window editor) | focus moves in; Esc and outside click close; focus returns |
| `ConfirmDialog` | native `<dialog>` with action-labelled buttons | `showModal()` focus trap; Esc = cancel |
| `Toast` | transient result of an owner write | `role="status"`, auto-dismiss 6 s, errors persist until closed |
| `EmptyState` | one quiet line: what this is + why it is empty | plain text |
| `DayFold` | IST day header button with count | `aria-expanded` (today's directive behaviour unchanged) |

Replaces `window.confirm` / `window.alert` everywhere (the ux-copy rule: buttons say the action).

## 8. Tab layouts

**Overview** (lg: 12-column grid)
1. Alerts card (full width; hidden when empty).
2. *Act now* — live recommendations (pending and `valid_until` ahead) as primary cards,
   `repeat(auto-fill, minmax(340px, 1fr))`. Empty: "Nothing to act on. New recommendations appear
   here and on Telegram." (Mode Off: "The engine is off, so no new recommendations will arrive.")
3. *Your positions* (real, primary, 7 cols) beside the *Paper autopilot* snapshot (simulated, 5 cols):
   equity, net since start, today, closed trades, sparkline, entry status line, "Open Paper →".
4. *Today* strip — four compact secondary tiles linking to tabs: risk checks today (n, blocked n),
   Telegram (sent n, undelivered n), news summary state, AI budget used %.

**Recommendation card (live)**
```
[BUY] TATASTEEL                       New position · Swing (delivery)     Valid until 15:30
Buy 3 shares at ₹2,445.00
Stop-loss ₹2,607.60   Target —   Amount ₹7,335.00   Max loss if stopped ₹1,250.00   Planned exit Wed 5 Nov (20 sessions)
Reasoning: <thesis, full text>
Your steps: • <manual checklist, always visible on a live card>
[Risk check: Approved]  [Waiting for you]  ▸ Details (evidence, risk-check notes, confidence, historical edge, stop distance, sent 09:41)
```
History rows (Recommendations tab) collapse to one line — time · side · stock · qty @ entry · risk
check · your action — and expand to the full card.

**Paper** — status card (autopilot state, entry status in words, halts in words, counting-since,
reset requested) → 4 stat tiles → equity chart → open positions → waiting orders (only when any) →
closed trades → health counters as a quiet footer.

**Performance** — one table, grouped header: *Recommendations, scored in hindsight* (Sent, Filled,
Closed, Win rate, Median return, Average return, 20-session return, vs market, Your actions) and
*Paper trades* (Closed, Win rate, Net P&L, Open now). The engine label becomes a plain note above the
table; the bench strings move into the "vs market" hint. Strategy IDs show a plain name with the ID
beside it.

**Activity** — in-page jump links, then: Risk checks (day folds kept) → Telegram messages → News
watchlist → Live updates.

**System** — Engine status (mode, risk state, auto-order routing, trading hours, live updates, last
refresh, each with its hint) → Risk limits (equity, today's P&L, meters vs caps; "max new trades per
day" shown as a limit) → AI usage budget (week meter, level in words, week bounds + time to reset,
per-analyst meters, daily candidate cap).

## 9. Copy system

Voice: plain, direct, second person, sentence case, verbs on buttons. A visible label never shows an
engine identifier, except stock symbols and (beside their plain name) strategy IDs. Every translated
term keeps its engine name in its hint ("Engine term: `FROZEN`"), so Telegram and the logs can still be
matched. Formatting: rupees with Indian grouping done on the string (`100113.30` → `₹1,00,113.30`;
never parsed to a float); signed P&L with a true minus (`−₹1,840.60`, `+₹113.30`); USD budget shown
to cents with the exact string in the hint; times are IST `HH:MM` today and `Thu 8 Oct, 13:00`
otherwise; day folds read `Today, Thu 8 Oct`, `Yesterday, Wed 7 Oct`, `Sun 4 Oct`.

Glossary (visible label — hint — engine term). Implemented once in `src/copy/glossary.ts`; unknown
values fall back to the raw value, humanised (`per_sector_exposure` → "per sector exposure").

| Engine term | Visible label | Hint |
|---|---|---|
| mode OFF / RECOMMEND / AUTO | Off / Recommend / Auto | Off: no new trade ideas; exits and protective actions still run. Recommend: the engine proposes, you place trades in Kite. Auto: the engine places orders itself; switched on only from Telegram. |
| routing paper / live | Auto orders go to: Paper / Live account | Only matters in Auto. |
| risk NORMAL / FROZEN / CLOSE_ONLY / KILLED | Normal / New positions paused / Closing only / Kill switch on | see §5 bodies |
| degrade DG0 … DG4 | Normal / Pacing / Saving / Minimal / Stopped | DG1: spending ahead of the weekly pace, background check-ins slow down, recommendation quality unchanged. DG2: background AI checks reduced, recommendations continue. DG3: AI analysts paused, scanner alerts only. DG4: no AI calls until Thursday 14:00 IST. |
| forward cap | Candidates sent to the AI per day | the pre-screen cap at the current AI level |
| verdict approve / shrink / reject / owner_approval_required | Approved / Approved, smaller / Blocked / Needs your OK | shrink: quantity reduced to fit a limit; reject: failed a risk check |
| human_action pending / taken / expired / dismissed / closed | Waiting for you / You took it / Expired / You skipped it / Closed | set on Telegram (`/taken`, `/closed`) |
| kind entry / exit / adjust | New position / Exit / Adjust | — |
| style swing / intraday / position | Swing / Intraday / Position | held days–weeks / same day / weeks–months |
| product CNC / MIS | Delivery / Intraday | CNC: held overnight in your demat account. MIS: intraday margin, squared off before the close. |
| entry_zone / stop / targets | Entry / Stop-loss / Target | — |
| notional | Amount | quantity × entry price |
| risk_inr | Max loss if stopped | approximate loss if the stop-loss is hit |
| stop_atr_mult | Stop distance (× ATR) | in multiples of the stock's average daily range (ATR) |
| registered_edge_pct | Historical edge | average net return per trade in the strategy's registered study — not a forecast |
| confidence | Analyst confidence | the AI analyst's own 0–1 confidence — not a probability of profit |
| hold_sessions / exit_session | Planned exit | the exit date after N trading sessions; "date pending" until the calendar resolves it |
| valid_until | Valid until | the recommendation expires after this |
| manual_checklist | Your steps | what to do in Kite to place this safely |
| short_flag_higher_tail_risk | Short sale — higher risk of a large loss | — |
| origin platform / external / recommended | Engine / Outside engine / Recommendation | who opened the position |
| owner_protected_at | Protected / Not protected | you confirm with `/protected SYMBOL` after placing the GTT |
| protection_state PROTECTED / PROTECTION_PENDING / PROTECTION_FAILED | Protected / Being protected / Protection failed | paper positions |
| position state OPEN / PENDING_EXIT | Open / Exiting | — |
| paper epoch | Counting since <date> | totals restart after `/paper reset` on Telegram |
| entry guard | New paper trades: allowed / blocked — <reason> | the engine's reason string stays in the hint |
| halts (cause → words) | daily_loss_soft → daily loss limit (soft) · daily_loss_hard → daily loss limit (hard) · floor_weekly_drawdown → weekly drawdown limit · floor_equity_floor_rung → equity floor · floor_cumulative_floor → total loss floor · reset_pending → reset in progress | rung FROZEN = new paper trades paused; CLOSE_ONLY = closing only; KILLED = stopped; latched = until `/paper reset`, else clears next session |
| unmarked | No live price for X | paper equity and paper limits pause until it returns |
| realized / open_mtm | Booked / Open (before costs) | costs are charged when a position closes |
| outcome win / loss / scratch / void | Win / Loss / Flat / Voided | void: cancelled, e.g. by a corporate action; excluded from win rate |
| scorecard label / bench | (note above the table) / vs-market hint | scored in hindsight on official prices, as if taken at the suggested entry — not your real P&L |
| n / filled / closed / hit_rate / median_net / mean_net / net_t20 / mean_excess | Sent / Filled / Closed / Win rate / Median return / Average return / 20-session return / vs market | net = after costs; vs market = above an equal-weighted market basket over the same window |
| decision log | Risk checks | every proposal and what the risk check decided |
| gate rule IDs | phrased as the problem (reasons are only cited when a rule failed) — Appendix A | raw rule ID in the hint |
| notifications | Telegram messages | delivered / retrying (N) / not delivered |
| digest fresh / stale / missing | News summary: up to date / stale / missing | stale or missing switches news-catalyst entries off for the day |
| grade originating / context | Can start a trade / Background only | — |
| materiality / source_domain_count / confirm_trigger / invalidation | Importance / Sources / Trigger price / Cancel price | — |
| deployed capital / open positions / consecutive losses | Capital in use / Open positions / Losses in a row today | — |
| strategies | hi52 52-week-high continuation · brk20 20-day-high breakout · ins Insider buying · cat News catalyst · cat_reversal News reversal · mom 4-week momentum · orb Opening-range breakout · rsi2 RSI-2 mean reversion · trend 20/50 EMA trend · unattributed Not linked to a strategy | — |
| agents | intraday_analyst Intraday analyst · news_analyst News analyst · preopen_planner Pre-open planner · nightly_reviewer Nightly reviewer · weekly_researcher Weekly researcher · swing_analyst Swing analyst · reserve Reserve · sdk_smoke Health check | — |

Owner-write copy:

- Mode → Off: dialog "Turn the engine off?" — "The engine stops proposing new trades. Exits and
  protective actions keep running. You can switch back any time." [Turn off] [Keep recommending].
  → Recommend: no dialog. Failure toast: "Couldn't change the mode: <engine reason>".
- Paper → ON: "Turn on the paper autopilot?" — "Every delivery (CNC) entry the analyst proposes is
  checked against the paper account, and approved ones are simulated — including some the real risk
  check blocks. No real order is ever placed." [Turn on] [Keep off]. → OFF with N waiting entry
  orders: "Turn off the paper autopilot?" — "No new paper trades will start. N waiting paper entry
  order(s) will be cancelled within 30 seconds / will lapse at the next market open. Open paper
  positions keep their exits." [Turn off] [Keep on].
- Hours popover: "Trading hours (IST)" — "New trades are only proposed inside these hours. Exits are
  never blocked." Fields Start, End, "Stop intraday entries [N] min before the end". [Save hours]
  [Cancel]. 422 → "Not saved — the engine rejected these hours (start must be before end, inside
  market hours, with room for intraday entries). Your current hours are unchanged."
- Token gate: "Connect to your trading engine" — "Enter the dashboard token. It's kept only in this
  browser." Hint: the engine's `DASHBOARD_TOKEN` (Windows DPAPI store), sent as a Bearer header.

## 10. Data parity (nothing silently dropped)

| Old field | New place |
|---|---|
| mode, routing, risk_state | mode control + Engine status; routing in the mode hint; risk state in alerts + Engine status |
| trade window chip + form | Hours button + popover (one place) |
| degrade tier (header + budget) | System → AI usage budget (+ alert when ≥ DG2) |
| websocket chip + polled time | connection indicator |
| rec: instrument, side, kind, style/product, gate verdict, human action, short flag, delivered, exit/sessions, risk to stop, ATR multiple, registered edge, entry zone, stop, targets, qty (of original), notional, confidence, fill, valid till, thesis, evidence, gate reasons, manual checklist | all kept: headline and key figures visible; evidence, risk-check notes, confidence, edge, ATR, sent time under Details on live cards; history rows expand to the same card |
| positions: every column + owner_protected_at | kept; protection timestamp in the hint |
| paper: every chip, tile, table column, counter | kept (§8); raw entry-guard and halt strings in hints |
| scorecard: every column + label + bench + skip reasons | kept; label → note, bench → hint, skip reasons → hint on Your actions |
| decision log: time, agent, action, subject (+ raw id), both verdicts, both reason lists | kept; raw id and raw rule IDs in hints |
| headroom: every figure and cap | kept as meters / limit lines |
| budget: tier, week, forward cap, spend vs credit, per-agent spend/alloc | kept |
| live events: kind + payload | humanised sentence; raw payload under a details toggle |
| notifications: everything incl. last error | kept |
| watchlist: every column, digest age/limit | kept |

## 11. Behaviour changes (beyond layout)

1. In-page dialogs and toasts replace `window.confirm` / `window.alert`.
2. Switching the mode to Off asks first (new; the backend stays single-step).
3. Every time shown is IST (fixes `hhmmss` using the browser zone).
4. The tab is kept in the URL hash.
5. Live events render as sentences; the raw payload stays one click away.
6. `usePoll` also returns the list of failed route keys (for the partial-failure alert). Nothing else
   in `api.ts` / `hooks.ts` / `types.ts` changes shape.

## 12. Non-goals

New data (orders, verdict detail, learning status, config audit), a kill button, light theme,
webfont, charts beyond the paper equity curve, per-user settings.

## 13. Acceptance criteria and validation

- `npm run build` (strict `tsc -b` + vite) green; pure modules (`format`, `alerts`, `glossary`) have
  `node --test` unit tests (Node 22 strips types; no new dependency).
- Fixture server: default, `NO_TODAY=1`, `PAPER_OFF=1`, `PAPER_IDLE=1`, plus new `ALERTS=1`
  (KILLED, unprotected position, undelivered Telegram, DG3, stale digest, filled headroom) and
  `EMPTY=1` (first boot: nothing anywhere).
- Every tab screenshotted at 1440, 1024 and 390 px in each scenario; in-page script asserts no page
  horizontal overflow, no element wider than its card, targets ≥ 24 px, text contrast ≥ 4.5:1.
- Lighthouse accessibility audit (chrome-devtools) ≥ 95 on Overview; keyboard pass over tabs,
  switch, segmented control, popover, dialogs and hints.
- Before/after critique (design-critique skill) and accessibility review (accessibility-review skill).
- Whole-branch review from independent angles (correctness/parity, accessibility, copy accuracy
  against engine semantics) before merge.
- `git diff --stat phase3...` touches only `dashboard/` and docs.
- Deploy = rebuild `dashboard/dist` in the live tree (no engine restart; Starlette serves from disk)
  after market hours, then confirm the served bundle hash.

## 14. Risks

| Risk | Mitigation |
|---|---|
| A hint states something the engine doesn't do | every hint traces to code read on 2026-10-08 (mode.py, types.py, governor/plan §5.6, gate.py, paper_risk.py, scorecard.py, telegram.py); copy-accuracy review angle before merge |
| Alert fatigue (e.g. digest chronically missing) | stale digest and Off mode are *notices*: quiet rows, no warning tone |
| Hidden information behind tabs | alerts aggregate across tabs; badges on tabs; Overview shows everything actionable |
| Owner muscle memory | same data and controls; tab in hash; Overview mirrors the old top-of-page |
| Breaking the live page | all work in worktree `mt_dash`; live `dist` untouched until the deploy step |

## 15. Implementation notes — where the build deviated from this spec, and why

Each change below came from looking at the rendered page or from a validation finding (2026-10-08).

| # | Change | Why |
|---|---|---|
| 1 | AI level DG0 reads **Normal**, not "Full" | "41% used, full" read as "the budget is full" |
| 2 | Day labels use a comma: `Today, Thu 8 Oct` | middle-dot meta strings are a generic-template tell |
| 3 | A defined term is its own toggletip (dotted underline); the ⓘ button remains only for standalone explanations | an ⓘ after every label and header was noisy and pushed the scorecard 260 px wider than its card |
| 4 | The paper tab is labelled **Paper trading**; it says "Simulated" once at the top and its cards drop the per-card tag (Card `simTag`), keeping the dashed border | a tag on every card was redundant text |
| 5 | The live ticket grid uses `auto-fit` and a container query (two columns inside a ticket ≥ 780 px wide) | one ticket used to sit in a third of the row, leaving two thirds empty |
| 6 | Positions table: "Stop-loss confirmed" is the second column; side folds into the stock cell | with a long symbol the safety column was the one scrolled out of view |
| 7 | Overview's positions + paper row stacks below 1200 px (not 1024) | at 1024 the positions table had to scroll |
| 8 | Scorecard scrolls sideways under ~1250 px with a sticky Strategy column | 14 columns cannot fit a laptop; the row label stays visible |
| 9 | On phones (< 640 px) tables drop their header row entirely and wrap a value under its label when it can't fit | a clipped header left its hint buttons focusable but invisible; a pill can't wrap at 320 px |
| 10 | One "Loading the dashboard…" line until the first poll answers | panels popping in one by one measured a layout shift of 0.21 |
| 11 | The equity chart's starting-capital label moved into a legend under the chart; the table toggle has its own row | the label collided with the end dot; the toggle collided with the card meta |
| 12 | A lapsed recommendation with no recorded action reads **No action recorded** (`actionState`), not "Waiting for you" | after `valid_until` the old wording asked the owner to act on an expired idea |
| 13 | Hints close 200 ms after the pointer leaves, unless it moves onto the bubble | WCAG 1.4.13 (hoverable) |
| 14 | Sign colours in tables use `.dt .pos/.neg` | `.dt td` out-ranked `.pos`, so returns showed uncoloured |
| 15 | Fixture server: `ALERTS=1`, `EMPTY=1`, `POST /mode`, `POST /config/trade_window`, 405 for other methods | the alert, first-boot and write paths needed something to run against |

## Appendix A — gate rule IDs in plain words

Source: `src/engine/risk/gate.py` rule list (lines 104–133 on 2026-10-08) plus the out-of-gate causes
it names. A reason is only cited when its rule failed, so each line states the problem. Unknown IDs
fall back to the humanised ID; the raw ID is always in the hint.

| Rule ID | Shown as |
|---|---|
| mode_risk_state | Engine mode or risk state doesn't allow new trades |
| kill_state | Kill switch is on |
| proposal_stale | The proposal was too old when it was checked |
| levels_coherent | Entry, stop-loss and target don't fit together |
| analyst_confidence_min | Analyst confidence below the minimum |
| trade_window | Outside your trading hours |
| no_trade_windows | Inside a no-trade period |
| min_residual_window | Not enough time left today for an intraday trade |
| instrument_eligible | Stock isn't eligible to trade |
| surveillance | Stock is under exchange surveillance |
| capital_cap | Would go over the capital limit |
| per_trade_risk | Risk per trade over the limit |
| daily_loss_soft | Today's loss limit reached |
| daily_loss_hard | Today's hard loss limit reached |
| consecutive_losses | Too many losses in a row today |
| max_new_trades_day | Daily new-trade limit reached |
| max_open_positions | Too many open positions |
| per_stock_exposure | Too much in one stock |
| per_sector_exposure | Too much in one sector |
| co_movement_cap | Too many positions that move together |
| max_leverage | Leverage over the limit |
| stale_data_guard | Price data is out of date |
| warmup_ready | Not enough price history loaded yet |
| regime_data_ready | Market index data (NIFTY 50 / India VIX) missing |
| clock_skew | Computer clock is out of sync |
| entry_sanity_band | Entry price too far from the live price |
| circuit_proximity | Stock is close to its circuit limit |
| margin_buffer | Not enough margin |
| order_rate | Broker order-rate limit reached |
| order_modifications | Too many changes to one order |
| min_viable_size | Trade too small to beat its costs |
| weekly_drawdown | Weekly loss limit reached |
| equity_floor_rung | Account fell below an equity floor |
| cumulative_floor | Total loss floor reached |
