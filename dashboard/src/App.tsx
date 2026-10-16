/**
 * Dashboard v2 (§3.2.11, O8/R8; redesign spec runbooks/design-2026-10-08-dashboard-redesign.md) — a
 * tabbed LAN console over the engine's read routes plus the four owner writes the dashboard is
 * allowed to make: `POST /mode` (OFF / RECOMMEND only), `POST /config/trade_window`, `POST /paper`
 * (paper autopilot ON / OFF, D13) and `POST /risk/rearm` (floor stops the account is back above).
 *
 * A fifth, `POST /config/capital` (System tab, Risk limits: "Change capital"), is the owner's starting
 * capital edit; the engine refuses it in market hours, in Auto, and when a stop would trip (O19).
 *
 * Deliberately ABSENT from this surface, and not an oversight:
 *  - →AUTO and kill-switch RESET are owner TWO-STEP transitions with no dashboard path (Telegram
 *    `/mode AUTO` + `/confirm`, `/kill_reset` + `/confirm`); the engine answers 409 for both (R10).
 *  - Paper reset stays Telegram two-step (`/paper reset` + `/confirm`).
 *  - Recommendation outcomes (`/taken`, `/closed`) are Telegram-only — this console reads the
 *    recommendation ledger, it never writes to it.
 */
import { useState } from 'react'
import type { JSX } from 'react'
import { ApiError, NoAnswerError, apiPost, getToken, setToken } from './api'
import { COPY } from './copy/glossary.ts'
import { useLiveEvents, useNotifications, usePoll, useScorecard } from './hooks'
import { deriveAlerts, health, tabBadges } from './lib/alerts.ts'
import type { AlertInput, TabId } from './lib/alerts.ts'
import { inr } from './lib/format.ts'
import type { CapitalChanged } from './types'
import { useHashTab } from './lib/useHashTab.ts'
import { AppBar } from './shell/AppBar'
import { HoursControl, ModeControl, PaperSwitch } from './shell/Controls'
import { TokenGate } from './shell/TokenGate'
import { useConfirm } from './ui/Dialog'
import { Icon } from './ui/Icon'
import { FailedRoutes } from './ui/Loading'
import { useToasts } from './ui/Toast'
import { Activity } from './views/Activity'
import { Overview } from './views/Overview'
import { Paper } from './views/Paper'
import { Performance } from './views/Performance'
import { Recommendations } from './views/Recommendations'
import { System } from './views/System'

/** Failed Telegram messages the owner acknowledged ("Got it"); per browser, best effort, capped. */
const ACK_KEY = 'mt_ack_failed_notifications'
const ACK_CAP = 200

function readAck(): string[] {
  try {
    const raw = JSON.parse(localStorage.getItem(ACK_KEY) ?? '[]') as unknown
    return Array.isArray(raw) ? raw.filter((x): x is string => typeof x === 'string') : []
  } catch {
    return []
  }
}

function writeAck(ids: string[]): void {
  try {
    localStorage.setItem(ACK_KEY, JSON.stringify(ids.slice(-ACK_CAP)))
  } catch {
    /* storage blocked: the alert simply stays */
  }
}

const reason = (e: unknown) => (e instanceof Error ? e.message : String(e))

export default function App() {
  const [token, setTokenState] = useState(getToken())
  const poll = usePoll(token)
  const live = useLiveEvents(token)
  const notifications = useNotifications(token)
  const scorecard = useScorecard(token)
  const [tab, setTab] = useHashTab()
  const { confirm, dialog } = useConfirm()
  const { push, region } = useToasts()
  const [acked, setAcked] = useState<string[]>(readAck)

  function saveToken(next: string) {
    setToken(next)
    setTokenState(next)
  }

  if (!token) return <TokenGate onSet={saveToken} />

  async function setMode(mode: 'OFF' | 'RECOMMEND') {
    try {
      const r = await apiPost<{ ok: boolean }>('/mode', { mode })
      if (r.ok) push('good', mode === 'OFF' ? 'Engine turned off.' : 'Engine set to Recommend.')
      else push('crit', "The engine didn't change the mode.")
    } catch (e) {
      // 409 (two-step only) / 501 (unwired) are answers, not crashes — say so and move on. No answer
      // at all is not a refusal: the engine may still apply it (api.ts).
      push('crit', e instanceof NoAnswerError ? COPY.noAnswer('mode') : `Couldn't change the mode: ${reason(e)}`)
    }
    poll.refresh()
  }

  async function setPaper(enabled: boolean) {
    try {
      await apiPost<{ ok: boolean }>('/paper', { enabled })
      push('good', enabled ? 'Paper autopilot turned on.' : 'Paper autopilot turned off.')
    } catch (e) {
      // 409 = ON while the autopilot is not built; nothing was written.
      push(
        'crit',
        e instanceof NoAnswerError ? COPY.noAnswer('paper autopilot') : `Couldn't change the paper autopilot: ${reason(e)}`,
      )
    }
    poll.refresh()
  }

  async function rearm() {
    if (!(await confirm(COPY.rearm))) return
    try {
      const r = await apiPost<{ cleared: string[]; still_breached: string[] }>('/risk/rearm', {})
      const res = COPY.rearmResult(r.cleared, r.still_breached)
      push(res.tone, res.text)
    } catch (e) {
      push('crit', e instanceof NoAnswerError ? COPY.noAnswer('risk state') : `Couldn't re-arm: ${reason(e)}`)
    }
    poll.refresh()
  }

  async function setHours(body: { start: string; end: string; squareoff_buffer_min: number | null }) {
    try {
      await apiPost<{ ok: boolean }>('/config/trade_window', body)
      push('good', `Trading hours saved: ${body.start}–${body.end}`)
      return null
    } catch (e) {
      // 422 = rejected by ModeManager validation; the stored window is UNCHANGED (§3.2.7).
      if (e instanceof ApiError && e.status === 422) return COPY.hoursRejected
      return e instanceof NoAnswerError ? COPY.noAnswer('trading hours') : `Not saved: ${reason(e)}`
    } finally {
      poll.refresh()
    }
  }

  // The dialog is modal, so a toast behind it would not be seen: a refusal rejects and the dialog prints
  // it (409 in market hours / Auto / a stale dialog, 422 out of bounds or a stop that would trip, 503/501).
  async function changeCapital(capital: number, expected: number) {
    try {
      const r = await apiPost<CapitalChanged>('/config/capital', { capital_inr: capital, expected_current_inr: expected })
      push('good', COPY.capitalSaved(inr(r.new.capital_inr) ?? String(capital)))
    } finally {
      poll.refresh()
    }
  }

  const input: AlertInput = {
    snapshot: poll.snapshot,
    failedKeys: poll.failedKeys,
    unauthorized: poll.unauthorized || notifications.unauthorized || scorecard.unauthorized,
    polled: poll.lastPollAt !== null,
    notifications: notifications.rows,
    ackedFailureIds: acked,
  }
  const alerts = deriveAlerts(input)
  const nowMs = Date.now()
  const badges = tabBadges(input, nowMs)
  const killed = alerts.some((a) => a.id === 'killed')
  const unreachable = alerts.some((a) => a.id === 'unreachable')

  const ack = (ids: string[]) => {
    const next = [...new Set([...acked, ...ids])]
    writeAck(next)
    setAcked(next)
    // The "Got it" button unmounts with its alert: keep keyboard focus in the alerts area (or the page).
    window.requestAnimationFrame(() => (document.getElementById('alerts') ?? document.getElementById('main'))?.focus())
  }

  const showAlerts = () => {
    setTab('overview')
    window.requestAnimationFrame(() => document.getElementById('alerts')?.focus())
  }

  // In-page links to another tab (an alert's button, "All recommendations", "Open paper trading"):
  // the clicked button leaves with the old view, so focus moves to the new panel instead of <body>.
  // The tab bar itself keeps focus on the tab (WAI-ARIA tabs pattern), so it uses setTab directly.
  const openTab = (t: TabId) => {
    setTab(t)
    window.requestAnimationFrame(() => document.getElementById(`panel-${t}`)?.focus({ preventScroll: true }))
  }

  const s = poll.snapshot
  const views: Record<TabId, () => JSX.Element> = {
    overview: () => (
      <Overview
        snapshot={s}
        alerts={alerts}
        notifications={notifications.rows}
        nowMs={nowMs}
        onTab={openTab}
        onSignOut={() => saveToken('')}
        onAck={ack}
        onRearm={() => void rearm()}
        errorDetail={poll.error}
      />
    ),
    recommendations: () => <Recommendations data={s.recommendations} nowMs={nowMs} />,
    paper: () => <Paper paper={s.paper} mode={s.mode?.mode ?? null} />,
    performance: () => <Performance {...scorecard} />,
    activity: () => (
      <Activity
        decisions={s.decisions}
        notifications={notifications}
        events={live.events}
        conn={live.conn}
        watchlist={s.watchlist}
      />
    ),
    system: () => (
      <System
        mode={s.mode}
        tradeWindow={s.tradeWindow}
        headroom={s.headroom}
        budget={s.budget}
        conn={live.conn}
        lastOkAt={poll.lastOkAt}
        nowMs={nowMs}
        onChangeCapital={changeCapital}
        onCapitalLateFailure={(message) => push('crit', message)}
      />
    ),
  }

  return (
    <>
      {/* Not a hash link: "#main" is not a tab, and following it would add a history entry. */}
      <a
        className="skip-link"
        href="#main"
        onClick={(e) => {
          e.preventDefault()
          document.getElementById('main')?.focus()
        }}
      >
        Skip to content
      </a>
      <AppBar
        health={health(alerts, poll.lastPollAt !== null)}
        onHealth={showAlerts}
        controls={
          <>
            <ModeControl mode={poll.snapshot.mode} onSetMode={setMode} confirm={confirm} />
            <PaperSwitch paper={poll.snapshot.paper} onSetPaper={setPaper} confirm={confirm} />
            <HoursControl
              data={poll.snapshot.tradeWindow}
              onSave={setHours}
              onLateFailure={(message) => push('crit', message)}
            />
          </>
        }
        conn={live.conn}
        lastOkAt={poll.lastOkAt}
        unreachable={unreachable}
        onSignOut={() => saveToken('')}
        tab={tab}
        onTab={setTab}
        badges={badges}
      />
      <main id="main" className="page" tabIndex={-1}>
        {killed && tab !== 'overview' ? (
          <div className="kill-banner" role="alert">
            <Icon name="x-circle" />
            <strong>Kill switch is on — no new trades.</strong>
            <span>Open positions are not closed for you. The Overview says how to reset it.</span>
          </div>
        ) : null}
        <FailedRoutes.Provider value={poll.failedKeys}>
          <div role="tabpanel" id={`panel-${tab}`} aria-labelledby={`tab-${tab}`} className="view" tabIndex={-1}>
            {/* Until the first poll answers there is nothing true to show: one quiet line, then the
                whole page at once instead of panels jumping in one by one. */}
            {poll.lastPollAt === null ? <p className="page-loading">Loading the dashboard…</p> : views[tab]()}
          </div>
        </FailedRoutes.Provider>
      </main>
      {dialog}
      {region}
    </>
  )
}
