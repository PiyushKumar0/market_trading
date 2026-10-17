/**
 * System tab, risk limits: the engine's own book (risk/exposure.py: a configured capital base plus the
 * net P&L of trades recorded through the engine — never the Kite balance) against the enforced caps.
 */
import { useState } from 'react'
import { apiGet } from '../../api'
import { COPY } from '../../copy/glossary.ts'
import type { Term } from '../../copy/glossary.ts'
import { inr, pnlWithPct, signOf } from '../../lib/format.ts'
import type { CapitalPreview, HeadroomResponse } from '../../types'
import { Card, EmptyState } from '../../ui/Card'
import { KeyValue, Meter, StatTile } from '../../ui/Figures'
import { Labelled } from '../../ui/Hint'
import { Loading } from '../../ui/Loading'
import { CapitalDialog } from './CapitalDialog'

const MAX_TRADES: Term = {
  label: 'New trades allowed per day',
  hint: 'The most new-trade recommendations the engine sends in a day. They count when sent, whether or not you take them.',
  term: 'max_new_trades_day',
}

// The meter is exposure.open_position_counts() (position rows only); gate.py max_open_positions also
// counts recommendations still waiting for a reply, so the gate can refuse before the meter is full.
const OPEN_KEY: Term = {
  label: 'Open positions',
  hint:
    'This counts positions only. The risk check also counts recommendations still waiting for your reply, so it ' +
    'can block a new trade before this is full.',
  term: 'max_open_positions',
}

const num = (v: string | number | undefined): number | null => {
  if (v === undefined || v === '') return null
  const n = Number(v)
  return Number.isFinite(n) ? n : null
}

export function RiskLimits({
  headroom,
  onChangeCapital,
  onLateFailure,
}: {
  headroom: HeadroomResponse | null
  /** A change that fails after its dialog was closed. */
  onLateFailure: (message: string) => void
  /** Resolves when the engine applied the change; rejects with its refusal (the dialog prints it). */
  onChangeCapital: (capital: number, expected: number) => Promise<void>
}) {
  const h = headroom?.headroom
  // The capital on record, read when the button is pressed so the dialog opens on the engine's figure
  // (the headroom route carries equity, not the base).
  const [capital, setCapital] = useState<string | null>(null)
  const [opening, setOpening] = useState(false)
  const [openError, setOpenError] = useState<string | null>(null)

  async function openDialog() {
    if (opening) return
    setOpening(true)
    setOpenError(null)
    try {
      const p = await apiGet<CapitalPreview>('/config/capital')
      if (p.current.capital_inr === null) setOpenError(COPY.capitalCurrentFailed(p.refusal ?? 'the engine has no figure for it'))
      else setCapital(p.current.capital_inr)
    } catch (e) {
      setOpenError(COPY.capitalCurrentFailed(e instanceof Error ? e.message : String(e)))
    } finally {
      setOpening(false)
    }
  }

  const intro = (
    <p className="muted">
      How close the engine&apos;s own record is to the limits the risk check enforces. It starts from a configured amount
      and adds the trades recorded through the engine; it is not your Kite balance.
    </p>
  )

  // null = /risk/headroom has not answered; `{ headroom: {} }` is the engine's stub (no exposure tracker).
  if (headroom === null) {
    return (
      <Card title="Risk limits">
        {intro}
        <Loading route="headroom" what="the risk limits" />
      </Card>
    )
  }
  if (!h || Object.keys(h).length === 0) {
    return (
      <Card title="Risk limits">
        {intro}
        <EmptyState>Risk limits aren&apos;t available: the engine&apos;s exposure tracker isn&apos;t connected.</EmptyState>
      </Card>
    )
  }

  const caps = h.caps
  const open = h.open_positions
  const sign = signOf(h.day_mtm)
  const dep = inr(h.deployed_capital)

  const rows: { label: Term; used: number | null; shown: string | null; limit: number | null; limitShown: string | null }[] = [
    {
      label: { label: 'Capital in use' },
      used: num(h.deployed_capital),
      shown: dep,
      limit: num(caps?.max_deployed_capital_inr),
      limitShown: inr(caps?.max_deployed_capital_inr),
    },
    ...(
      [
        [OPEN_KEY, open?.total, caps?.max_open_positions_total],
        [{ label: 'Delivery (CNC) positions' }, open?.cnc, caps?.max_open_positions_cnc],
        [{ label: 'Intraday (MIS) positions' }, open?.mis, caps?.max_open_positions_mis],
        [{ label: 'Losses in a row today' }, h.consecutive_losses, caps?.consecutive_losses_max_per_session],
      ] as [Term, number | undefined, number | undefined][]
    ).map(([label, used, limit]) => ({
      label,
      used: used ?? null,
      shown: used === undefined ? null : String(used),
      limit: limit ?? null,
      limitShown: limit === undefined ? null : String(limit),
    })),
  ]

  return (
    <Card title="Risk limits">
      {intro}
      <div className="stats stats-compact">
        <StatTile label="Equity on record" value={inr(h.equity) ?? '—'} />
        <StatTile
          label="Today's P&L on record"
          value={<span className={sign === 1 ? 'pos' : sign === -1 ? 'neg' : undefined}>{pnlWithPct(h.day_mtm, h.day_mtm_pct) ?? '—'}</span>}
        />
      </div>
      {caps ? (
        <div className="sys-meters">
          {rows.map((r) => {
            const text = r.limitShown ? `${r.shown ?? '—'} of ${r.limitShown}` : (r.shown ?? '—')
            return (
              <Meter
                key={r.label.label}
                label={<Labelled t={r.label} />}
                used={r.used}
                limit={r.limit}
                text={text}
                ariaLabel={`${r.label.label}: ${text}`}
              />
            )
          })}
        </div>
      ) : (
        <>
          <KeyValue items={rows.map((r) => ({ k: <Labelled t={r.label} />, v: r.shown ?? '—' }))} />
          <p className="muted">Limit values aren&apos;t available.</p>
        </>
      )}
      {caps ? (
        <p className="sys-limit-line">
          <Labelled t={MAX_TRADES} />: <span className="num">{caps.max_new_trades_day}</span>
        </p>
      ) : null}
      <div className="sys-capital">
        <button type="button" className="btn" aria-disabled={opening || undefined} onClick={() => void openDialog()}>
          {COPY.capitalButton}
        </button>
        {openError ? (
          <p className="form-error" role="alert">
            {openError}
          </p>
        ) : null}
      </div>
      {capital !== null ? (
        <CapitalDialog
          current={capital}
          onClose={() => setCapital(null)}
          onConfirm={onChangeCapital}
          onLateFailure={onLateFailure}
        />
      ) : null}
    </Card>
  )
}
