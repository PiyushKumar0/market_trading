/**
 * The three owner controls the dashboard is allowed (README "Owner writes"): engine mode Off /
 * Recommend, the paper autopilot ON / OFF, and the trading hours. AUTO, kill reset and paper reset
 * stay Telegram two-step (R10) — there is no path to them here. Every control waits for the engine's
 * answer: the value shown always comes from the next poll, never from an optimistic guess.
 */
import { useState } from 'react'
import type { FormEvent } from 'react'
import { COPY, modeTerm, routingTerm } from '../copy/glossary.ts'
import { isPaperSummary } from '../lib/alerts.ts'
import type { ModeResponse, PaperResponse, TradeWindow, TradeWindowResponse } from '../types'
import { Switch, Segmented } from '../ui/Controls'
import type { ConfirmRequest } from '../ui/Dialog'
import { Hint } from '../ui/Hint'
import { Icon } from '../ui/Icon'
import { Pill } from '../ui/Pill'
import { Popover } from '../ui/Popover'

type Confirm = (r: ConfirmRequest) => Promise<boolean>

export function ModeControl({
  mode,
  onSetMode,
  confirm,
}: {
  mode: ModeResponse | null
  onSetMode: (m: 'OFF' | 'RECOMMEND') => Promise<void>
  confirm: Confirm
}) {
  const [busy, setBusy] = useState(false)
  const current = mode?.mode ?? null
  const routing = routingTerm(mode?.routing)
  const explain = (
    <>
      Off: {modeTerm('OFF').hint} Recommend: {modeTerm('RECOMMEND').hint} Auto: {modeTerm('AUTO').hint}
      {mode?.routing ? ` Auto orders go to: ${routing.label}.` : ''}
    </>
  )

  if (current === 'AUTO') {
    return (
      <div className="ctl">
        <span className="ctl-label">Engine</span>
        <Pill tone="accent">Auto (set on Telegram)</Pill>
        <Hint label="Engine mode" term="AUTO">
          {explain}
        </Hint>
      </div>
    )
  }

  async function pick(v: 'OFF' | 'RECOMMEND') {
    if (v === 'OFF' && !(await confirm(COPY.modeOff))) return
    setBusy(true)
    try {
      await onSetMode(v)
    } finally {
      setBusy(false)
    }
  }

  return (
    <div className="ctl">
      <span className="ctl-label" id="mode-label">
        Engine
      </span>
      <Segmented<'OFF' | 'RECOMMEND'>
        label="Engine mode"
        value={current === 'OFF' || current === 'RECOMMEND' ? current : null}
        options={[
          { value: 'OFF', label: 'Off' },
          { value: 'RECOMMEND', label: 'Recommend' },
        ]}
        disabled={busy || current === null}
        onChange={(v) => void pick(v)}
      />
      <Hint label="Engine mode" term={current ?? undefined}>
        {current === null ? "The engine's mode hasn't loaded. " : null}
        {explain}
      </Hint>
    </div>
  )
}

export function PaperSwitch({
  paper,
  onSetPaper,
  confirm,
}: {
  paper: PaperResponse | null
  onSetPaper: (on: boolean) => Promise<void>
  confirm: Confirm
}) {
  const [busy, setBusy] = useState(false)
  if (!isPaperSummary(paper)) return null // not loaded, or the engine has no paper control wired
  const p = paper
  const stateText = p.enabled ? (p.built ? 'On' : 'On (not running)') : 'Off'
  const why = p.built
    ? null
    : p.subsystem_enabled
      ? 'It failed to start, so it can only be turned on after the engine restarts.'
      : 'It is switched off in the engine settings (paper.subsystem_enabled), so it cannot be turned on here.'

  async function flip(next: boolean) {
    if (next) {
      if (!p.built || !(await confirm(COPY.paperOn))) return
    } else {
      // Unbuilt: stored orders are stranded and nothing cancels them, so there is nothing to warn about.
      const waiting = p.built ? p.orders.filter((o) => o.role === 'entry').length : 0
      if (waiting > 0 && !(await confirm(COPY.paperOff(waiting, p.prep_ready)))) return
    }
    setBusy(true)
    try {
      await onSetPaper(next)
    } finally {
      setBusy(false)
    }
  }

  return (
    <div className="ctl">
      <Switch
        checked={p.enabled}
        label="Paper autopilot"
        shortLabel="Paper"
        stateText={stateText}
        busy={busy}
        disabled={!p.enabled && !p.built}
        onChange={(next) => void flip(next)}
      />
      <Hint label="Paper autopilot">
        {COPY.paperAbout}
        {why ? ` ${why}` : ''}
      </Hint>
    </div>
  )
}

export function HoursControl({
  data,
  onSave,
}: {
  data: TradeWindowResponse | null
  onSave: (b: { start: string; end: string; squareoff_buffer_min: number | null }) => Promise<string | null>
}) {
  const w = data?.trade_window ?? null
  return (
    <Popover
      title={COPY.hoursTitle}
      trigger={(t) => (
        <button {...t} type="button" className={w ? 'btn hours-btn' : 'btn hours-btn hours-unset'}>
          <Icon name="clock" />
          <span className="hours-k">Hours</span>
          <span className="num">{w ? `${w.start}–${w.end}` : 'not set'}</span>
          <Icon name="chevron-down" size={14} />
        </button>
      )}
    >
      {(close) => <HoursForm current={w} onSave={onSave} onDone={close} />}
    </Popover>
  )
}

function HoursForm({
  current,
  onSave,
  onDone,
}: {
  current: TradeWindow | null
  onSave: (b: { start: string; end: string; squareoff_buffer_min: number | null }) => Promise<string | null>
  onDone: () => void
}) {
  // Seeded once, when the popover opens: a 10 s poll can never overwrite what is being typed.
  const [start, setStart] = useState(current?.start ?? '')
  const [end, setEnd] = useState(current?.end ?? '')
  const [buffer, setBuffer] = useState(
    current?.squareoff_buffer_min !== null && current?.squareoff_buffer_min !== undefined
      ? String(current.squareoff_buffer_min)
      : '',
  )
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string | null>(null)

  async function submit(e: FormEvent) {
    e.preventDefault()
    setBusy(true)
    setError(null)
    const failure = await onSave({
      start,
      end,
      squareoff_buffer_min: buffer.trim() === '' ? null : Number(buffer),
    })
    setBusy(false)
    if (failure) setError(failure)
    else onDone()
  }

  return (
    <form className="hours-form" onSubmit={(e) => void submit(e)}>
      <h2 className="popover-title">{COPY.hoursTitle}</h2>
      <p className="popover-body">{COPY.hoursBody}</p>
      <div className="hours-grid">
        <label className="field">
          <span className="field-label">Start</span>
          <input className="input" type="time" required value={start} onChange={(e) => setStart(e.target.value)} />
        </label>
        <label className="field">
          <span className="field-label">End</span>
          <input className="input" type="time" required value={end} onChange={(e) => setEnd(e.target.value)} />
        </label>
      </div>
      <label className="field">
        <span className="field-label">{COPY.hoursBufferLabel}</span>
        <input
          className="input input-short"
          type="number"
          min={0}
          inputMode="numeric"
          value={buffer}
          onChange={(e) => setBuffer(e.target.value)}
        />
      </label>
      {error ? (
        <p className="form-error" role="alert">
          {error}
        </p>
      ) : null}
      <div className="popover-actions">
        <button type="button" className="btn" onClick={onDone}>
          Cancel
        </button>
        <button type="submit" className="btn btn-primary" disabled={busy || !start || !end}>
          {busy ? 'Saving…' : 'Save hours'}
        </button>
      </div>
    </form>
  )
}
