/**
 * The three owner controls the dashboard is allowed (README "Owner writes"): engine mode Off /
 * Recommend, the paper autopilot ON / OFF, and the trading hours. AUTO, kill reset and paper reset
 * stay Telegram two-step (R10) — there is no path to them here. Every control waits for the engine's
 * answer: the value shown always comes from the next poll, never from an optimistic guess.
 */
import { useEffect, useRef, useState } from 'react'
import type { FormEvent } from 'react'
import { COPY, modeTerm, routingTerm } from '../copy/glossary.ts'
import { isPaperSummary } from '../lib/alerts.ts'
import type { ModeResponse, PaperResponse, TradeWindow, TradeWindowResponse } from '../types'
import { Switch, Segmented } from '../ui/Controls'
import type { ConfirmRequest } from '../ui/Dialog'
import { Hint } from '../ui/Hint'
import { Icon } from '../ui/Icon'
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

  const auto = current === 'AUTO'

  // From Auto, Off and Recommend stay available: leaving Auto is the dashboard's brake on auto-trading
  // (single-step, as the engine accepts it). Going INTO Auto stays Telegram two-step (R10).
  async function pick(v: 'OFF' | 'RECOMMEND') {
    const ask = auto ? (v === 'OFF' ? COPY.autoOff : COPY.autoToRecommend) : v === 'OFF' ? COPY.modeOff : null
    if (ask && !(await confirm(ask))) return
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
        disabled={current === null}
        busy={busy}
        onChange={(v) => void pick(v)}
        extra={
          auto ? (
            <button
              type="button"
              className="seg-btn"
              aria-pressed="true"
              disabled
              title="Auto can't be switched on or off here; pick Off or Recommend to leave it."
            >
              Auto
            </button>
          ) : null
        }
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

type SaveHours = (b: { start: string; end: string; squareoff_buffer_min: number | null }) => Promise<string | null>

export function HoursControl({
  data,
  onSave,
  onLateFailure,
}: {
  /** null = not answered yet (or the route failed): unknown, which is not the same as "not set". */
  data: TradeWindowResponse | null
  onSave: SaveHours
  /** A save that fails after the editor was closed has nowhere to show its error: raise it here. */
  onLateFailure: (message: string) => void
}) {
  const known = data !== null
  const w = data?.trade_window ?? null
  return (
    <Popover
      title={COPY.hoursTitle}
      trigger={(t) => (
        // Disabled until the hours are known: an editor opened now would start empty, and saving it
        // could overwrite hours that already exist.
        <button
          {...t}
          type="button"
          className={!known || w ? 'btn hours-btn' : 'btn hours-btn hours-unset'}
          disabled={!known}
          title={known ? undefined : "The trading hours haven't loaded yet."}
        >
          <Icon name="clock" />
          <span className="hours-k">Hours</span>
          {known ? (
            <span className="num">{w ? `${w.start}–${w.end}` : 'not set'}</span>
          ) : (
            <>
              <span aria-hidden="true">…</span>
              <span className="sr-only">not loaded yet</span>
            </>
          )}
          <Icon name="chevron-down" size={14} />
        </button>
      )}
    >
      {(close) => <HoursForm current={w} onSave={onSave} onDone={close} onLateFailure={onLateFailure} />}
    </Popover>
  )
}

function HoursForm({
  current,
  onSave,
  onDone,
  onLateFailure,
}: {
  current: TradeWindow | null
  onSave: SaveHours
  onDone: () => void
  onLateFailure: (message: string) => void
}) {
  // Esc or a click outside can close the editor while the save is still on its way.
  // (Set on every mount: StrictMode's dev re-mount would otherwise leave it false.)
  const alive = useRef(true)
  useEffect(() => {
    alive.current = true
    return () => {
      alive.current = false
    }
  }, [])
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
    if (busy) return
    setBusy(true)
    setError(null)
    const failure = await onSave({
      start,
      end,
      squareoff_buffer_min: buffer.trim() === '' ? null : Number(buffer),
    })
    if (!alive.current) {
      // Closed meanwhile: a success already has its toast, and focus must stay where the user went.
      if (failure) onLateFailure(failure)
      return
    }
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
        <button type="submit" className="btn btn-primary" disabled={!start || !end} aria-disabled={busy || undefined}>
          {busy ? 'Saving…' : 'Save hours'}
        </button>
      </div>
    </form>
  )
}
