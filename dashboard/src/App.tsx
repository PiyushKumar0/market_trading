/**
 * Dashboard v2 (§3.2.11, O8/R8; redesign spec runbooks/design-2026-10-08-dashboard-redesign.md) — a
 * tabbed LAN console over the engine's read routes plus the three owner writes the dashboard is
 * allowed to make: `POST /mode` (OFF / RECOMMEND only), `POST /config/trade_window` and `POST /paper`
 * (paper autopilot ON / OFF, D13).
 *
 * Deliberately ABSENT from this surface, and not an oversight:
 *  - →AUTO and kill-switch RESET are owner TWO-STEP transitions with no dashboard path (Telegram
 *    `/mode AUTO` + `/confirm`, `/kill_reset` + `/confirm`); the engine answers 409 for both (R10).
 *  - Paper reset stays Telegram two-step (`/paper reset` + `/confirm`).
 *  - Recommendation outcomes (`/taken`, `/closed`) are Telegram-only — this console reads the
 *    recommendation ledger, it never writes to it.
 */
import { useState } from 'react'
import { ApiError, apiPost, getToken, setToken } from './api'
import { COPY } from './copy/glossary.ts'
import { useLiveEvents, useNotifications, usePoll, useScorecard } from './hooks'
import { deriveAlerts, health, tabBadges } from './lib/alerts.ts'
import type { AlertInput, TabId } from './lib/alerts.ts'
import { TABS, useHashTab } from './lib/useHashTab.ts'
import { AppBar } from './shell/AppBar'
import { HoursControl, ModeControl, PaperSwitch } from './shell/Controls'
import { TokenGate } from './shell/TokenGate'
import { EmptyState } from './ui/Card'
import { useConfirm } from './ui/Dialog'
import { useToasts } from './ui/Toast'
import { Overview } from './views/Overview'

/** The newest failed Telegram message the owner acknowledged ("Got it"); per browser, best effort. */
const ACK_KEY = 'mt_ack_failed_notification'

function readAck(): string | null {
  try {
    return localStorage.getItem(ACK_KEY)
  } catch {
    return null
  }
}

function writeAck(id: string): void {
  try {
    localStorage.setItem(ACK_KEY, id)
  } catch {
    /* storage blocked: the alert simply stays */
  }
}

const reason = (e: unknown) => (e instanceof Error ? e.message : String(e))

function Pending({ tab }: { tab: TabId }) {
  const name = TABS.find((t) => t.id === tab)?.label ?? tab
  return (
    <>
      <h1 className="view-title">{name}</h1>
      <EmptyState>This section is being rebuilt.</EmptyState>
    </>
  )
}

export default function App() {
  const [token, setTokenState] = useState(getToken())
  const poll = usePoll(token)
  const live = useLiveEvents(token)
  const notifications = useNotifications(token)
  const scorecard = useScorecard(token)
  const [tab, setTab] = useHashTab()
  const { confirm, dialog } = useConfirm()
  const { push, region } = useToasts()
  const [acked, setAcked] = useState<string | null>(readAck)

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
      // 409 (two-step only) / 501 (unwired) are answers, not crashes — say so and move on.
      push('crit', `Couldn't change the mode: ${reason(e)}`)
    }
    poll.refresh()
  }

  async function setPaper(enabled: boolean) {
    try {
      await apiPost<{ ok: boolean }>('/paper', { enabled })
      push('good', enabled ? 'Paper autopilot turned on.' : 'Paper autopilot turned off.')
    } catch (e) {
      // 409 = ON while the autopilot is not built; nothing was written.
      push('crit', `Couldn't change the paper autopilot: ${reason(e)}`)
    }
    poll.refresh()
  }

  async function setHours(body: { start: string; end: string; squareoff_buffer_min: number | null }) {
    try {
      await apiPost<{ ok: boolean }>('/config/trade_window', body)
      poll.refresh()
      push('good', `Trading hours saved: ${body.start}–${body.end}`)
      return null
    } catch (e) {
      // 422 = rejected by ModeManager validation; the stored window is UNCHANGED (§3.2.7).
      return e instanceof ApiError && e.status === 422 ? COPY.hoursRejected : `Not saved: ${reason(e)}`
    }
  }

  const input: AlertInput = {
    snapshot: poll.snapshot,
    failedKeys: poll.failedKeys,
    unauthorized: poll.unauthorized || notifications.unauthorized || scorecard.unauthorized,
    polled: poll.lastPollAt !== null,
    notifications: notifications.rows,
    ackedFailureId: acked,
  }
  const alerts = deriveAlerts(input)
  const nowMs = Date.now()
  const badges = tabBadges(input, nowMs)

  const ack = (id: string) => {
    writeAck(id)
    setAcked(id)
  }

  const showAlerts = () => {
    setTab('overview')
    window.requestAnimationFrame(() => document.getElementById('alerts')?.focus())
  }

  return (
    <>
      <a className="skip-link" href="#main">
        Skip to content
      </a>
      <AppBar
        health={health(alerts)}
        onHealth={showAlerts}
        controls={
          <>
            <ModeControl mode={poll.snapshot.mode} onSetMode={setMode} confirm={confirm} />
            <PaperSwitch paper={poll.snapshot.paper} onSetPaper={setPaper} confirm={confirm} />
            <HoursControl data={poll.snapshot.tradeWindow} onSave={setHours} />
          </>
        }
        conn={live.conn}
        lastPollAt={poll.lastPollAt}
        onSignOut={() => saveToken('')}
        tab={tab}
        onTab={setTab}
        badges={badges}
      />
      <main id="main" className="page">
        <div role="tabpanel" id={`panel-${tab}`} aria-labelledby={`tab-${tab}`} className="view">
          {tab === 'overview' ? (
            <Overview
              snapshot={poll.snapshot}
              alerts={alerts}
              notifications={notifications.rows}
              nowMs={nowMs}
              onTab={setTab}
              onSignOut={() => saveToken('')}
              onAck={ack}
            />
          ) : (
            <Pending tab={tab} />
          )}
        </div>
      </main>
      {dialog}
      {region}
    </>
  )
}
