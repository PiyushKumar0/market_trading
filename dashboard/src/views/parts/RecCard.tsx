/**
 * A recommendation as a trade ticket: the plain instruction is the headline ("Buy 3 shares at
 * ₹2,445.00"), the levels sit right under it, then the reasoning and your steps. Everything the old
 * card showed is still here (spec §10); the secondary facts sit under "Details".
 *
 * Gate reasons come from the recommendation's own embedded verdict (§3.6 `gate`) — never from
 * `/decisions`: another proposal's verdict is never this card's provenance.
 */
import { useState } from 'react'
import type { ReactNode } from 'react'
import {
  FIELD,
  actionHint,
  kindTerm,
  productTerm,
  strategyName,
  styleTerm,
  verdictTerm,
} from '../../copy/glossary.ts'
import { inr, istStamp, istTime, shortDate } from '../../lib/format.ts'
import { actionState, instruction, quantityCut } from '../../lib/recs.ts'
import type { RecommendationPayload, RecommendationRow } from '../../types'
import { Hint, Labelled } from '../../ui/Hint'
import { Icon } from '../../ui/Icon'
import { Pill, SideBadge } from '../../ui/Pill'
import type { Tone } from '../../ui/Pill'
import { ReasonText } from './ReasonText'

const VERDICT_TONE: Record<string, Tone> = {
  approve: 'good',
  shrink: 'warn',
  reject: 'crit',
  owner_approval_required: 'warn',
}
const LONG_THESIS = 260

function Level({ t, v }: { t: { label: string; hint?: string }; v: ReactNode }) {
  return (
    <div className="lvl">
      <dt>
        <Labelled t={t} />
      </dt>
      <dd className="num">{v}</dd>
    </div>
  )
}

/**
 * The levels row, by kind (ops/pipeline.py `_manage_recommendation`). An exit or adjust carries the
 * held position's size and a reference price — the last price or the position's own entry — with no
 * targets, so only the entry kind shows an amount, targets and a loss at the stop:
 *  - exit   → the stop-loss recorded at /taken (a followed adjust never updates it);
 *  - adjust → nothing: the new stop-loss is the headline and no order is placed.
 */
function Levels({ rec }: { rec: RecommendationPayload }) {
  if (rec.kind === 'adjust') return null
  if (rec.kind === 'exit') {
    return (
      <dl className="ticket-levels">
        <Level t={FIELD.stopOnRecord} v={inr(rec.stop) ?? '—'} />
      </dl>
    )
  }
  const targets = rec.targets ?? []
  const held = rec.hold_sessions !== null && rec.hold_sessions !== undefined
  const timeExit = rec.exit_kind === 'time' || (targets.length === 0 && held)
  const exit = rec.exit_session
    ? `${shortDate(rec.exit_session)}${held ? ` (${rec.hold_sessions} sessions)` : ''}`
    : held
      ? 'Date pending'
      : null
  return (
    <dl className="ticket-levels">
      <Level t={FIELD.stop} v={inr(rec.stop) ?? '—'} />
      <Level
        t={targets.length > 1 ? { ...FIELD.target, label: 'Targets' } : FIELD.target}
        v={targets.length ? targets.map((t) => inr(t)).join(', ') : timeExit ? 'None, exits on a date' : '—'}
      />
      <Level t={FIELD.amount} v={inr(rec.notional) ?? '—'} />
      {rec.risk_inr ? <Level t={FIELD.maxLoss} v={inr(rec.risk_inr)} /> : null}
      {exit ? <Level t={FIELD.plannedExit} v={exit} /> : null}
    </dl>
  )
}

export function RecCard({
  row,
  nowMs,
  variant = 'live',
}: {
  row: RecommendationRow
  nowMs: number
  variant?: 'live' | 'history'
}) {
  const [more, setMore] = useState(false)
  const rec = row.recommendation
  if (!rec) {
    return (
      <article className="ticket">
        <p className="muted">This recommendation ({row.rec_id}) has no stored details.</p>
      </article>
    )
  }

  const side = (rec.side ?? '').toUpperCase()
  const gate = rec.gate
  const verdict = verdictTerm(gate?.verdict)
  const action = actionState(row, nowMs)
  const cut = quantityCut(rec)
  const stripe = rec.kind === 'adjust' ? 'other' : side === 'SELL' ? 'sell' : side === 'BUY' ? 'buy' : 'other'
  const thesis = rec.thesis ?? ''
  const steps = rec.manual_checklist ?? []
  const evidence = rec.evidence ?? []
  const reasons = gate?.reasons ?? []
  const strategy = rec.strategy_id ? strategyName(rec.strategy_id) : null
  const live = variant === 'live'

  const stepsBlock = steps.length ? (
    <div className="ticket-steps">
      <h4 className="ticket-subhead">
        Your steps
        <Hint label="Your steps">What to do in Kite to place this safely, including the protective stop order.</Hint>
      </h4>
      <ol>
        {steps.map((s, i) => (
          <li key={i}>{s}</li>
        ))}
      </ol>
    </div>
  ) : null

  return (
    <article className={`ticket ticket-${stripe} ticket-${variant}`}>
      <header className="ticket-head">
        <SideBadge side={rec.side} kind={rec.kind} />
        <h3 className="ticket-symbol">{rec.instrument ?? row.rec_id}</h3>
        <span className="ticket-tags">
          <Labelled t={kindTerm(rec.kind)} />
          {rec.style ? <Labelled t={styleTerm(rec.style)} /> : null}
          {rec.product ? <Labelled t={productTerm(rec.product)} /> : null}
        </span>
        {rec.valid_until ? (
          <span className="ticket-valid">
            Valid until <strong className="num">{istStamp(rec.valid_until)}</strong>
          </span>
        ) : null}
      </header>

      <div className="ticket-body">
        <div className="ticket-main">
          <p className="ticket-instruction">{instruction(rec)}</p>
          {rec.kind === 'adjust' ? (
            <p className="ticket-note ticket-note-plain">
              No new order: change the stop-loss order you already have in Kite for this position.
            </p>
          ) : null}
          {cut ? (
            <p className="ticket-note">
              The risk check cut the quantity from {cut.from} to {cut.to} to fit a limit.
            </p>
          ) : null}
          <Levels rec={rec} />
        </div>

        {thesis || (live && steps.length) ? (
          <div className="ticket-side">
            {thesis ? (
              <div className="ticket-thesis-wrap">
                <h4 className="ticket-subhead">Reasoning</h4>
                <p className={more || thesis.length <= LONG_THESIS ? 'ticket-thesis' : 'ticket-thesis clamp'}>{thesis}</p>
                {thesis.length > LONG_THESIS ? (
                  <button
                    type="button"
                    className="btn btn-quiet btn-inline"
                    aria-expanded={more}
                    onClick={() => setMore((m) => !m)}
                  >
                    {more ? 'Show less' : 'Show all reasoning'}
                  </button>
                ) : null}
              </div>
            ) : null}
            {live ? stepsBlock : null}
          </div>
        ) : null}
      </div>

      <footer className="ticket-foot">
        <span className="ticket-status">
          <Pill tone={VERDICT_TONE[gate?.verdict ?? ''] ?? 'neutral'} icon={gate?.verdict === 'reject' ? 'x-circle' : 'shield'}>
            Risk check: {verdict.label}
          </Pill>
          {verdict.hint ? <Hint label="Risk check" term={verdict.term}>{verdict.hint}</Hint> : null}
          <Pill tone={action.tone}>{action.label}</Pill>
          <Hint label="Your action" term={row.human_action ?? undefined}>
            {action.tone === 'warn' ? actionHint(rec) : action.hint}
          </Hint>
          {rec.short_flag_higher_tail_risk ? (
            <Pill tone="crit" icon="alert">
              Short sale: higher risk of a large loss
            </Pill>
          ) : null}
        </span>
      </footer>

      <details className="disclosure ticket-details">
        <summary>
          <Icon name="chevron-right" size={14} />
          Details
        </summary>
        <div className="ticket-details-body">
          {!live ? stepsBlock : null}
          {evidence.length ? (
            <div>
              <h4 className="ticket-subhead">Evidence</h4>
              <ul>
                {evidence.map((e, i) => (
                  <li key={i}>{e}</li>
                ))}
              </ul>
            </div>
          ) : null}
          {reasons.length ? (
            <div>
              <h4 className="ticket-subhead">Risk check notes</h4>
              <ul>
                {reasons.map((r, i) => (
                  <li key={`${i}-${r}`}>
                    <ReasonText reason={r} />
                  </li>
                ))}
              </ul>
            </div>
          ) : null}
          <dl className="ticket-facts">
            {strategy ? <Level t={{ label: 'Strategy' }} v={<Labelled t={strategy} />} /> : null}
            {rec.confidence !== null && rec.confidence !== undefined ? (
              <Level t={FIELD.confidence} v={rec.confidence.toFixed(2)} />
            ) : null}
            {rec.registered_edge_pct ? <Level t={FIELD.edge} v={`${rec.registered_edge_pct}% per trade`} /> : null}
            {rec.stop_atr_mult ? <Level t={FIELD.stopDistance} v={`${rec.stop_atr_mult}× ATR`} /> : null}
            {row.human_fill_price ? <Level t={FIELD.fill} v={inr(row.human_fill_price)} /> : null}
            <Level
              t={{ label: 'Sent' }}
              v={<span title={istTime(row.delivered_at, { seconds: true })}>{istStamp(row.delivered_at)}</span>}
            />
            <Level t={{ label: 'Reference' }} v={<code className="ref">{row.rec_id}</code>} />
          </dl>
        </div>
      </details>
    </article>
  )
}
