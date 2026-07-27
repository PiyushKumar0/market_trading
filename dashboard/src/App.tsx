/**
 * Dashboard v1 (§3.2.11, O8/R8) — a single-page LAN console over the engine's read routes plus the
 * two owner writes the dashboard is allowed to make: `POST /mode` (OFF / RECOMMEND only) and
 * `POST /config/trade_window`.
 *
 * Deliberately ABSENT from this surface, and not an oversight:
 *  - →AUTO and kill-switch RESET are owner TWO-STEP transitions with no dashboard path (Telegram
 *    `/mode AUTO` + `/confirm`, `/kill_reset` + `/confirm`); the engine answers 409 for both (R10).
 *  - Recommendation outcomes (`/taken`, `/closed`) are Telegram-only — this console reads the
 *    recommendation ledger, it never writes to it.
 */
import { useState } from 'react'
import { ApiError, apiPost, getToken, setToken } from './api'
import { useLiveEvents, usePoll } from './hooks'
import { BudgetPanel } from './components/BudgetPanel'
import { DecisionsPanel } from './components/DecisionsPanel'
import { EventsPanel } from './components/EventsPanel'
import { HeadroomPanel } from './components/HeadroomPanel'
import { NewsPanel } from './components/NewsPanel'
import { PositionsPanel } from './components/PositionsPanel'
import { RecommendationsPanel } from './components/RecommendationsPanel'
import { StatusHeader } from './components/StatusHeader'
import { TradeWindowForm } from './components/TradeWindowForm'

function TokenGate({ onSet }: { onSet: (token: string) => void }) {
  const [value, setValue] = useState('')
  return (
    <div className="gate">
      <div className="panel">
        <h2>
          <span>market_trading — dashboard token</span>
        </h2>
        <div className="panel-body">
          <p className="dim">
            The engine's <code>DASHBOARD_TOKEN</code> (Windows DPAPI store). It is kept in this
            browser's localStorage and sent as <code>Authorization: Bearer</code> on every request.
          </p>
          <form
            className="inline"
            onSubmit={(e) => {
              e.preventDefault()
              onSet(value.trim())
            }}
          >
            <input
              type="password"
              autoComplete="off"
              placeholder="bearer token"
              value={value}
              onChange={(e) => setValue(e.target.value)}
            />
            <button type="submit" disabled={!value.trim()}>
              connect
            </button>
          </form>
        </div>
      </div>
    </div>
  )
}

export default function App() {
  const [token, setTokenState] = useState(getToken())
  const { snapshot, error, unauthorized, lastPollAt, refresh } = usePoll(token)
  const { events, conn } = useLiveEvents(token)

  function saveToken(next: string) {
    setToken(next)
    setTokenState(next)
  }

  if (!token) return <TokenGate onSet={saveToken} />

  async function setMode(mode: string) {
    try {
      await apiPost<{ ok: boolean }>('/mode', { mode })
    } catch (e) {
      // 409 (two-step only) / 501 (unwired) are meaningful answers, not crashes — show and move on.
      window.alert(e instanceof ApiError ? `mode ${mode}: ${e.status} ${e.message}` : String(e))
    }
    refresh()
  }

  async function setTradeWindow(body: {
    start: string
    end: string
    squareoff_buffer_min: number | null
  }): Promise<string> {
    try {
      await apiPost<{ ok: boolean }>('/config/trade_window', body)
      refresh()
      return `set ${body.start}–${body.end}`
    } catch (e) {
      // 422 = rejected by ModeManager validation; the stored window is UNCHANGED (§3.2.7).
      return e instanceof ApiError ? `rejected (${e.status}): ${e.message}` : String(e)
    }
  }

  return (
    <div className="app">
      <StatusHeader
        mode={snapshot.mode}
        tradeWindow={snapshot.tradeWindow}
        budget={snapshot.budget}
        conn={conn}
        lastPollAt={lastPollAt}
        error={unauthorized ? 'token rejected (401) — sign out and re-enter it' : error}
        onSetMode={setMode}
        onSignOut={() => saveToken('')}
      />

      <div className="grid">
        <RecommendationsPanel
          rows={snapshot.recommendations?.recommendations ?? []}
          decisions={snapshot.decisions?.decisions ?? []}
        />
        <PositionsPanel data={snapshot.positions} />
        <DecisionsPanel rows={snapshot.decisions?.decisions ?? []} />
        <HeadroomPanel data={snapshot.headroom} />
        <BudgetPanel data={snapshot.budget} />
        <TradeWindowForm data={snapshot.tradeWindow} onSubmit={setTradeWindow} />
        <EventsPanel events={events} conn={conn} />
        <NewsPanel data={snapshot.watchlist} />
      </div>
    </div>
  )
}
