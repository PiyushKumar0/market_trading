/**
 * Panel — paper autopilot (`GET /paper`): control state, halts, equity of the current epoch, its
 * per-session curve, the open book, working orders and closed trades. Everything here is SIMULATED.
 * Money arrives as strings and is printed as sent; the curve parses it for geometry only.
 */
import type { PaperCurvePoint, PaperResponse, PaperSummary } from '../types'
import { Chip, Empty, Panel, dash, istDay, toneFor, todayIst } from './ui'
import type { Tone } from './ui'

const FLATTENING = new Set(['floor_equity_floor_rung', 'floor_cumulative_floor', 'reset_pending'])
const ZERO = /^-?0+(\.0+)?$/
const IST_HM = new Intl.DateTimeFormat('en-GB', {
  timeZone: 'Asia/Kolkata',
  hour: '2-digit',
  minute: '2-digit',
  hourCycle: 'h23',
})

function hhmm(iso: string): string {
  const at = new Date(iso)
  return Number.isNaN(at.getTime()) ? iso : IST_HM.format(at)
}

/** "HH:MM" IST when the stamp is from today, else "YYYY-MM-DD HH:MM". */
function stamp(iso: string | null): string {
  if (!iso) return '—'
  const day = istDay(iso)
  if (!day) return iso
  return day === todayIst() ? hhmm(iso) : `${day} ${hhmm(iso)}`
}

const rupees = (v: string | null) => (v === null ? null : `₹${v}`)

function signed(v: string | null): string | null {
  if (v === null || v === '') return null
  if (ZERO.test(v)) return `₹${v.replace('-', '')}`
  return v.startsWith('-') ? `−₹${v.slice(1)}` : `+₹${v}`
}

function signClass(v: string | null): string | undefined {
  if (!v || ZERO.test(v)) return undefined
  return v.startsWith('-') ? 'neg' : 'pos'
}

function outcomeTone(label: string | null): Tone {
  return label === 'win' ? 'ok' : label === 'loss' ? 'bad' : label === 'void' ? 'warn' : 'neutral'
}

function Tile({ k, v, cls, sub }: { k: string; v: string | null; cls?: string; sub?: string }) {
  return (
    <div className="tile">
      <div className="k">{k}</div>
      <div className={cls ? `v ${cls}` : 'v'}>{dash(v)}</div>
      {sub ? <div className="dim">{sub}</div> : null}
    </div>
  )
}

function Curve({ points, base }: { points: PaperCurvePoint[]; base: string }) {
  const pts = points.map((p) => ({ ...p, v: Number(p.equity) })).filter((p) => Number.isFinite(p.v))
  if (pts.length < 2) return <div className="dim">curve starts after the second session</div>
  const b = Number(base)
  const ref = Number.isFinite(b) && b > 0 ? [b] : []
  let lo = Math.min(...pts.map((p) => p.v), ...ref)
  let hi = Math.max(...pts.map((p) => p.v), ...ref)
  const floor = ref.length ? b * 0.02 : 2   // a small move never fills the chart
  if (hi - lo < floor) [lo, hi] = [(lo + hi - floor) / 2, (lo + hi + floor) / 2]
  const pad = (hi - lo) * 0.12
  ;[lo, hi] = [lo - pad, hi + pad]
  const x = (i: number) => 2 + (96 * i) / (pts.length - 1)
  const y = (v: number) => (100 * (hi - v)) / (hi - lo)
  const top = pts.reduce((m, p) => (p.v > m.v ? p : m))
  const bottom = pts.reduce((m, p) => (p.v < m.v ? p : m))
  const last = pts[pts.length - 1]
  const step = 96 / (pts.length - 1)
  const tip = (p: (typeof pts)[number], i: number) =>
    `${i === pts.length - 1 && istDay(p.at) === todayIst() ? `today, ${hhmm(p.at)}` : p.d} · ₹${p.equity}`
  return (
    <div className="curve">
      <div className="ylab">
        <span className={top === bottom ? undefined : 'hi'} style={{ top: `${y(top.v)}%` }}>₹{top.equity}</span>
        {top === bottom ? null : (
          <span className="lo" style={{ top: `${y(bottom.v)}%` }}>₹{bottom.equity}</span>
        )}
      </div>
      <svg viewBox="0 0 100 100" preserveAspectRatio="none" role="img"
           aria-label={`paper equity at each session close; dashed line = capital base ₹${base}`}>
        {ref.length ? (
          <line x1={0} x2={100} y1={y(b)} y2={y(b)} style={{ stroke: 'var(--muted)' }} strokeWidth={1}
                strokeDasharray="4 3" vectorEffect="non-scaling-stroke" />
        ) : null}
        <polyline points={pts.map((p, i) => `${x(i)},${y(p.v)}`).join(' ')} fill="none" style={{ stroke: 'var(--series)' }}
                  strokeWidth={2} strokeLinejoin="round" vectorEffect="non-scaling-stroke" />
        <path d={`M${x(pts.length - 1)},${y(last.v)}h0`} style={{ stroke: 'var(--series)' }} strokeWidth={9}
              strokeLinecap="round" vectorEffect="non-scaling-stroke" />
        {pts.map((p, i) => {
          const x0 = Math.max(0, x(i) - step / 2)
          return (
            <rect key={p.at} x={x0} width={Math.min(100, x(i) + step / 2) - x0} y={0} height={100} fill="transparent">
              <title>{tip(p, i)}</title>
            </rect>
          )
        })}
      </svg>
      <div className="xlab">
        <span>{pts[0].d}</span>
        <span>{last.d}</span>
      </div>
    </div>
  )
}

function Status({ p, mode }: { p: PaperSummary; mode: string | null }) {
  const live = mode === 'RECOMMEND' || mode === 'AUTO'
  return (
    <div className="chips">
      {p.built ? null : p.subsystem_enabled ? (
        <Chip v="BUILD FAILED — off until restart" tone="bad" title="see the PAPER_ALERT notification / engine log" />
      ) : (
        <Chip v="NOT BUILT — subsystem disabled" title="paper.subsystem_enabled is false; nothing is simulated" />
      )}
      <Chip
        k="autopilot"
        v={p.enabled ? (p.built ? 'ON' : 'stored ON (inactive)') : 'OFF'}
        tone={p.enabled && p.built ? 'ok' : 'neutral'}
        title={!p.enabled && p.built ? 'no new entries; exits and GTTs continue' : undefined}
      />
      {!p.built ? null : mode === null ? (
        <Chip v="entries: mode unknown" title="the header mode has not loaded" />
      ) : !live ? (
        <Chip v={`idle — mode ${mode}`} title="paper rides the RECOMMEND pipeline" />
      ) : p.entry_guard ? (
        <Chip v={`entry guard: ${p.entry_guard}`} tone={p.prep_ready ? 'warn' : 'neutral'} />
      ) : (
        <Chip v="entry guard open" tone="ok"
              title="CNC swing entries from the RECOMMEND pipeline, inside the trade window" />
      )}
      <Chip
        v={p.epoch_started_at ? `epoch since ${istDay(p.epoch_started_at) || p.epoch_started_at}` : 'epoch: all paper history (never reset)'}
        title="this panel counts the current epoch; Scorecard paper columns count all epochs"
      />
      {p.reset_requested_at ? <Chip v={`reset requested ${stamp(p.reset_requested_at)}`} tone="warn" /> : null}
      {p.halts.map((h) => (
        <Chip
          key={h.cause}
          k={h.cause}
          v={h.rung}
          tone={toneFor(h.rung)}
          title={`set ${stamp(h.set_at)} · ${h.latched ? 'latched until /paper reset (Telegram)' : 'clears next session'}${FLATTENING.has(h.cause) ? ' · book being flattened' : ''}`}
        />
      ))}
      {p.built && p.prep_ready && p.unmarked.length ? (
        <Chip v={`equity + paper halts paused — no mark: ${p.unmarked.join(', ')}`} tone="warn" />
      ) : null}
    </div>
  )
}

function Body({ p, mode }: { p: PaperSummary; mode: string | null }) {
  const eq = p.equity
  const today = todayIst()
  const t = p.totals
  const stored = p.positions.length > 0 || p.orders.length > 0
  if (!p.built && !stored && !eq && p.closed.length === 0) {
    return (
      <>
        <Status p={p} mode={mode} />
        <div className="dim">
          {p.subsystem_enabled
            ? 'The paper stack failed to build at boot; nothing is simulated until the engine restarts.'
            : 'Nothing is simulated and no paper rows are stored.'}
        </div>
      </>
    )
  }
  return (
    <>
      <Status p={p} mode={mode} />
      {!p.built && stored ? (
        <div className="banner warn">
          autopilot not running: {p.positions.length} held paper position{p.positions.length === 1 ? ' is' : 's are'}{' '}
          frozen at the last snapshot; stops and targets are not simulated; the next build replays or voids the gap
        </div>
      ) : null}

      {eq ? (
        <>
          <div className="tiles">
            <Tile k="paper equity" v={rupees(eq.equity)} sub={`base ₹${eq.capital_base}`} />
            <Tile
              k="paper net since epoch"
              v={signed(eq.pnl)}
              cls={signClass(eq.pnl)}
              sub={`realized ${signed(eq.realized_pnl) ?? '—'} · open ${signed(eq.open_mtm) ?? '—'} gross${t.voids > 0 ? ` · incl. voids ${signed(t.void_net) ?? '—'}` : ''}`}
            />
            <Tile
              k="paper day P&L"
              v={signed(eq.day_mtm)}
              cls={signClass(eq.day_mtm)}
              sub={istDay(eq.at) === today ? undefined : `session ${istDay(eq.at) || eq.at}`}
            />
            <Tile
              k="trades this epoch"
              v={`closed ${t.closed}`}
              sub={`hit ${t.closed ? `${Math.round((100 * t.wins) / t.closed)}%` : '—'} · ${t.voids} void${t.voids === 1 ? '' : 's'}`}
            />
          </div>
          <Curve points={p.curve} base={eq.capital_base} />
        </>
      ) : p.built ? (
        <div className="dim">no equity snapshot yet this epoch — written each in-session minute after session prep</div>
      ) : null}

      <div className="caption">open positions</div>
      {p.positions.length === 0 ? (
        <Empty what="open paper positions" />
      ) : (
        <table>
          <thead>
            <tr>
              <th>symbol</th>
              <th>strategy</th>
              <th className="num">qty</th>
              <th className="num">entry</th>
              <th className="num">mark</th>
              <th className="num" title="round-trip costs are charged at close">unrealized (gross)</th>
              <th className="num">stop</th>
              <th className="num">target</th>
              <th>protection</th>
              <th>exit session</th>
            </tr>
          </thead>
          <tbody>
            {p.positions.map((r) => (
              <tr key={r.position_id}>
                <td>
                  {r.symbol}
                  {r.product ? <span className="dim"> {r.product}</span> : null}
                  {r.state === 'PENDING_EXIT' ? <> <Chip v="PENDING_EXIT" tone="warn" /></> : null}
                </td>
                <td>{dash(r.strategy_id)}</td>
                <td className="num">{dash(r.qty)}</td>
                <td className="num">{dash(r.avg_entry)}</td>
                <td className="num">{dash(r.mark)}</td>
                <td className={`num ${signClass(r.unrealized) ?? ''}`}>{dash(signed(r.unrealized))}</td>
                <td className="num">{dash(r.stop)}</td>
                <td className="num">{dash(r.target)}</td>
                <td className={r.protection_state === 'PROTECTION_FAILED' ? 'err' : undefined}>{dash(r.protection_state)}</td>
                <td>{dash(r.exit_session)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}

      {p.orders.length ? (
        <>
          <div className="caption">
            working orders {p.built ? null : <Chip v="stranded — closed at the next paper start" tone="warn" />}
          </div>
          <table>
            <thead>
              <tr>
                <th>symbol</th>
                <th>strategy</th>
                <th>role</th>
                <th>side</th>
                <th className="num">qty</th>
                <th className="num">filled</th>
                <th className="num">price</th>
                <th className="num">trigger</th>
                <th>state</th>
                <th>created</th>
              </tr>
            </thead>
            <tbody>
              {p.orders.map((o) => (
                <tr key={o.order_id}>
                  <td>{dash(o.symbol)}</td>
                  <td>{dash(o.strategy_id)}</td>
                  <td>{dash(o.role)}</td>
                  <td>{dash(o.side)}</td>
                  <td className="num">{dash(o.qty)}</td>
                  <td className="num">{dash(o.filled_qty)}</td>
                  <td className="num">{dash(o.price)}</td>
                  <td className="num">{dash(o.trigger_price)}</td>
                  <td>{dash(o.state)}</td>
                  <td>{stamp(o.created_at)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </>
      ) : null}

      <div className="caption">closed trades — this epoch, newest 20</div>
      {p.closed.length === 0 ? (
        <Empty what="closed paper trades this epoch" />
      ) : (
        <table>
          <thead>
            <tr>
              <th>date</th>
              <th>symbol</th>
              <th>strategy</th>
              <th className="num">qty</th>
              <th className="num">entry</th>
              <th className="num">exit</th>
              <th className="num">net ₹</th>
              <th>reason</th>
              <th>outcome</th>
            </tr>
          </thead>
          <tbody>
            {p.closed.map((c, i) => (
              <tr key={c.position_id ?? i}>
                <td>{dash(istDay(c.closed_at))}</td>
                <td>{dash(c.symbol)}</td>
                <td>{dash(c.strategy_id)}</td>
                <td className="num">{dash(c.qty)}</td>
                <td className="num">{dash(c.entry_px)}</td>
                <td className="num">{dash(c.exit_px)}</td>
                <td className={`num ${signClass(c.net_pnl) ?? ''}`}>{dash(signed(c.net_pnl))}</td>
                <td title={c.close_basis ?? undefined}>{dash(c.close_reason)}</td>
                <td>{c.outcome_label ? <Chip v={c.outcome_label} tone={outcomeTone(c.outcome_label)} /> : dash(null)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}

      {p.built && p.counters ? (
        <div className="dim" style={{ marginTop: 8 }}>
          since engine start: reconcile mismatches {p.counters.reconcile_mismatches ?? '—'} · voids{' '}
          {p.counters.voids ?? '—'} · late corp actions {p.counters.late_corp_actions ?? '—'}
        </div>
      ) : null}
    </>
  )
}

export function PaperPanel({ data, mode }: { data: PaperResponse | null; mode: string | null }) {
  const p = data && 'built' in data ? data : null
  const at = p?.equity?.at ?? null
  return (
    <Panel title="Paper autopilot" aside={`simulated — no real orders${at ? ` · as of ${stamp(at)}` : ''}`} wide>
      {p ? <Body p={p} mode={mode} /> : <Empty what={data ? 'paper summary (the engine answered the stub)' : 'paper data'} />}
    </Panel>
  )
}
