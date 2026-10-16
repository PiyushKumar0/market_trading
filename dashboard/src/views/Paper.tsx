/**
 * Paper trading — the simulated autopilot (`GET /paper`): its state in words, halts, the account in
 * four figures, equity by session, then the book. Everything here is SIMULATED; the page says so once,
 * and every card carries the dashed "simulated" border. The ON/OFF switch lives in the top bar (D13).
 * Money arrives as strings and is printed as sent.
 */
import { COPY, haltTerm, rungTerm, strategyName } from '../copy/glossary.ts'
import { isPaperSummary } from '../lib/alerts.ts'
import { inr, istDay, istStamp, shortDate, pnlWithPct, signOf, todayIst } from '../lib/format.ts'
import { entryGuardText, paperStatus } from '../lib/paper.ts'
import type { PaperResponse, PaperSummary } from '../types'
import { Card, EmptyState } from '../ui/Card'
import { KeyValue, StatTile } from '../ui/Figures'
import { Hint, Labelled } from '../ui/Hint'
import { Icon } from '../ui/Icon'
import { Loading } from '../ui/Loading'
import { Pill } from '../ui/Pill'
import { EquityChart } from './parts/EquityChart'
import { PaperClosedTrades, PaperOrders, PaperPositions } from './parts/PaperTables'
import './paper.css'

const FLATTENING = new Set(['floor_equity_floor_rung', 'floor_cumulative_floor', 'reset_pending'])

const valueClass = (v: string | null | undefined) => {
  const s = signOf(v)
  return s === 1 ? 'pos' : s === -1 ? 'neg' : undefined
}

const ALLOWED = 'Allowed'

/** "A", "A and B", "A, B and C" from strategy ids, by friendly name. */
function joinNames(ids: string[]): string {
  const names = ids.map((id) => strategyName(id).label)
  return names.length > 1 ? `${names.slice(0, -1).join(', ')} and ${names[names.length - 1]}` : names[0]
}

function entriesText(p: PaperSummary, mode: string | null): string {
  if (!p.built) return 'Not running'
  if (!p.enabled) return 'Off'
  if (mode === null) return "Waiting for the engine's mode"
  if (mode !== 'RECOMMEND' && mode !== 'AUTO') return 'Idle: it follows the Recommend pipeline'
  if (p.halts.length) return 'Paused by a paper limit (below)'
  return p.entry_guard ? `Blocked: ${entryGuardText(p.entry_guard)}` : ALLOWED
}

function Status({ p, mode }: { p: PaperSummary; mode: string | null }) {
  const line = paperStatus(p, mode)
  const stored = p.positions.length > 0 || p.orders.length > 0
  const resetting = p.halts.some((h) => h.cause === 'reset_pending')
  return (
    <Card tier="simulated" simTag={false} title="Autopilot status">
      <p className={`paper-line paper-line-${line.tone}`}>
        <Icon name={line.tone === 'good' ? 'check' : line.tone === 'warn' ? 'pause' : 'info'} />
        <span>{line.text}</span>
      </p>
      {!p.built && stored ? (
        <p className="paper-banner">
          The autopilot isn&apos;t running. {p.positions.length} paper{' '}
          {p.positions.length === 1 ? 'position is' : 'positions are'} frozen at their last values, and their exits
          aren&apos;t being simulated. When it next starts it replays or voids the gap.
        </p>
      ) : null}
      <KeyValue
        items={[
          { k: 'Autopilot', v: p.enabled ? (p.built ? 'On' : 'On, not running') : 'Off' },
          {
            k: 'New paper trades',
            v:
              entriesText(p, mode) === ALLOWED ? (
                <Labelled t={{ label: ALLOWED, hint: COPY.paperEntriesOpen }} />
              ) : (
                <span title={p.entry_guard ? `Engine term: ${p.entry_guard}` : undefined}>{entriesText(p, mode)}</span>
              ),
          },
          {
            k: (
              <span className="labelled">
                Counting since
                <Hint label="Counting since">
                  Totals on this tab restart when you reset paper trading (/paper reset on Telegram). The Performance
                  tab counts every reset.
                </Hint>
              </span>
            ),
            v: p.epoch_started_at ? shortDate(istDay(p.epoch_started_at) || p.epoch_started_at) : 'All paper history (never reset)',
          },
          ...(p.reset_requested_at ? [{ k: 'Reset requested', v: istStamp(p.reset_requested_at) }] : []),
        ]}
      />
      {p.halts.length ? (
        <div className="paper-halts">
          <h3 className="paper-subhead">Paper limits in force</h3>
          <ul>
            {p.halts.map((h) => (
              <li key={h.cause} className="paper-halt">
                <Pill tone={h.rung === 'FROZEN' ? 'warn' : 'crit'} icon="pause">
                  <Labelled t={haltTerm(h.cause)} />
                </Pill>
                <span>
                  {rungTerm(h.rung).label[0].toUpperCase() + rungTerm(h.rung).label.slice(1)}. Set {istStamp(h.set_at)};{' '}
                  {/* paper_risk.py: on the first flat book (no position, no working order) a new
                      epoch starts and every paper halt clears. */}
                  {h.cause === 'reset_pending'
                    ? 'clears by itself once no paper position or order is left; then every paper limit clears'
                    : h.latched
                      ? resetting
                        ? 'clears when the pending reset completes'
                        : 'stays until /paper reset on Telegram'
                      : 'clears next session'}
                  .
                  {FLATTENING.has(h.cause) ? ' Open paper positions are being closed.' : ''}
                </span>
              </li>
            ))}
          </ul>
        </div>
      ) : null}
      {p.built && p.prep_ready && p.unmarked.length ? (
        <p className="paper-note">
          <Icon name="alert" /> No live price for {p.unmarked.join(', ')}: paper equity and paper limits are paused until it
          returns.
        </p>
      ) : null}
    </Card>
  )
}

function Figures({ p }: { p: PaperSummary }) {
  const eq = p.equity
  const t = p.totals
  if (!eq) {
    return p.built ? (
      <EmptyState>
        No paper equity yet since the last reset. It is recorded every minute in the session once today&apos;s paper setup
        completes.
      </EmptyState>
    ) : null
  }
  const today = todayIst()
  const parts = [
    `Booked ${pnlWithPct(eq.realized_pnl, eq.realized_pnl_pct) ?? '—'}`,
    `open ${pnlWithPct(eq.open_mtm, eq.open_mtm_pct) ?? '—'} before costs`,
    ...(t.voids > 0 ? [`including voided ${pnlWithPct(t.void_net, t.void_net_pct) ?? '—'}`] : []),
  ]
  return (
    <div className="stats paper-stats">
      <StatTile label="Paper equity" value={inr(eq.equity)} caption={`Started with ${inr(eq.capital_base) ?? '—'}`} />
      <StatTile
        label="Net since start"
        value={<span className={valueClass(eq.pnl)}>{pnlWithPct(eq.pnl, eq.pnl_pct) ?? '—'}</span>}
        caption={parts.join(', ')}
      />
      <StatTile
        label="Today"
        value={<span className={valueClass(eq.day_mtm)}>{pnlWithPct(eq.day_mtm, eq.day_mtm_pct) ?? '—'}</span>}
        caption={istDay(eq.at) === today ? undefined : `For ${shortDate(istDay(eq.at) || eq.at)}`}
      />
      <StatTile
        label="Closed trades"
        value={String(t.closed)}
        caption={`${t.wins} won${t.closed ? ` (${Math.round((100 * t.wins) / t.closed)}%)` : ''}${t.voids ? `, ${t.voids} voided` : ''}`}
      />
    </div>
  )
}

export function Paper({ paper, mode }: { paper: PaperResponse | null; mode: string | null }) {
  const p = isPaperSummary(paper) ? paper : null
  return (
    <>
      <div className="view-head">
        <h1 className="view-title">Paper trading</h1>
        <Pill tone="sim" icon="flask">
          Simulated: never places a real order
        </Pill>
        <Hint label="Paper trading">{COPY.paperAbout}</Hint>
      </div>

      {paper === null ? (
        <Loading route="paper" what="paper trading" />
      ) : !p ? (
        <EmptyState>The engine doesn&apos;t have paper trading connected.</EmptyState>
      ) : !p.built && !p.positions.length && !p.orders.length && !p.equity && !p.closed.length ? (
        <>
          <Status p={p} mode={mode} />
          <EmptyState>
            {p.subsystem_enabled
              ? 'The paper autopilot failed to start, so nothing is simulated until the engine restarts.'
              : 'Nothing is simulated and no paper trades are stored.'}
          </EmptyState>
        </>
      ) : (
        <>
          <Status p={p} mode={mode} />
          <Figures p={p} />
          {p.equity ? (
            <Card tier="simulated" simTag={false} title="Paper equity by session" meta={`Updated ${istStamp(p.equity.at)}`}>
              <EquityChart points={p.curve} base={p.equity.capital_base} />
            </Card>
          ) : null}
          <Card tier="simulated" simTag={false} title="Open paper positions" meta={p.positions.length ? `${p.positions.length} open` : undefined}>
            {p.no_stop_strategies?.length ? (
              <p className="paper-counters">
                Exit test: new paper trades from {joinNames(p.no_stop_strategies)} have no stop-loss or target and sell near
                the close of their planned exit day. Recommendation cards keep their stops. The test is judged across all
                recommendations, not on this paper book.
              </p>
            ) : null}
            <PaperPositions rows={p.positions} />
          </Card>
          {p.orders.length ? (
            <Card
              tier="simulated"
              simTag={false}
              title="Waiting paper orders"
              meta={
                p.built ? undefined : (
                  <Pill tone="warn">Stranded: closed when paper trading next starts</Pill>
                )
              }
            >
              <PaperOrders rows={p.orders} />
            </Card>
          ) : null}
          <Card tier="simulated" simTag={false} title="Closed paper trades" meta="Newest 20 since the last reset">
            <PaperClosedTrades rows={p.closed} />
          </Card>
          {p.built && p.counters ? (
            <p className="paper-counters">
              <span className="labelled">
                Since the engine started: {p.counters.reconcile_mismatches ?? '—'} price mismatches,{' '}
                {p.counters.voids ?? '—'} voided, {p.counters.late_corp_actions ?? '—'} late corporate actions
                <Hint label="Paper health counters">
                  Health counters of the paper engine. Anything above zero is worth a look in the engine log.
                </Hint>
              </span>
            </p>
          ) : null}
        </>
      )}
    </>
  )
}
