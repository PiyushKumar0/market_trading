/**
 * "Change capital": the owner types a new starting capital; the ENGINE computes every figure it moves
 * (GET /config/capital?proposed=n, debounced) and decides whether the change is allowed. This dialog
 * prints those strings and enables Confirm only when `allowed`; it never computes a limit itself.
 */
import { useEffect, useId, useRef, useState } from 'react'
import type { FormEvent } from 'react'
import { apiGet } from '../../api'
import { COPY } from '../../copy/glossary.ts'
import { capitalRows, comparisonBase, failureText, parseCapitalInput, refusalText } from '../../lib/capital.ts'
import type { CapitalRow } from '../../lib/capital.ts'
import { inr } from '../../lib/format.ts'
import type { CapitalPreview } from '../../types'
import { DataTable } from '../../ui/DataTable'
import { Modal } from '../../ui/Dialog'

const DEBOUNCE_MS = 300

const reason = (e: unknown) => (e instanceof Error ? e.message : String(e))

/** A preview and the amount it was asked for (null = the current-only answer). */
interface Shown {
  for: number | null
  data: CapitalPreview
}

export function CapitalDialog({
  current,
  onClose,
  onConfirm,
  onLateFailure,
}: {
  /** The capital on record when the dialog opened (engine string). */
  current: string
  onClose: () => void
  /** Rejects with the engine's refusal; the dialog stays open and prints it. */
  onConfirm: (capital: number, expected: number) => Promise<void>
  /** A failure that arrives after the dialog was closed has nowhere to show: raise it as a toast. */
  onLateFailure: (message: string) => void
}) {
  const titleId = useId()
  const inputId = useId()
  const currentN = Math.round(Number(current))
  const [text, setText] = useState(Number.isFinite(currentN) ? String(currentN) : '')
  const [shown, setShown] = useState<Shown | null>(null)
  const [loadError, setLoadError] = useState<string | null>(null)
  const [retry, setRetry] = useState(0)
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const alive = useRef(true)

  const parsed = parseCapitalInput(text)
  // "Same amount" is judged against the newest capital the engine reported (a 409 refresh or a change
  // from another device moves it), not the one at open.
  const base = comparisonBase(shown?.data.current.capital_inr, currentN)
  // The amount to ask the engine about; null = the current figures only.
  const proposed = parsed !== null && parsed !== base ? parsed : null

  useEffect(() => {
    alive.current = true
    return () => {
      alive.current = false
    }
  }, [])

  useEffect(() => {
    let stale = false
    const run = () => {
      apiGet<CapitalPreview>(`/config/capital${proposed === null ? '' : `?proposed=${proposed}`}`)
        .then((data) => {
          if (stale) return
          setLoadError(null)
          setShown({ for: proposed, data })
        })
        .catch((e: unknown) => {
          if (!stale) setLoadError(reason(e))
        })
    }
    const timer = window.setTimeout(run, proposed === null ? 0 : DEBOUNCE_MS)
    return () => {
      stale = true
      window.clearTimeout(timer)
    }
  }, [proposed, retry])

  // What is on screen answers the amount in the box only once the engine has replied for it.
  const fresh = shown !== null && shown.for === proposed
  const data = shown?.data ?? null
  const expected = data ? Number(data.current.capital_inr) : NaN
  const canConfirm =
    fresh && proposed !== null && data?.proposed != null && data.allowed && Number.isFinite(expected)
  const rows: CapitalRow[] = capitalRows(data && fresh ? data : data && { ...data, proposed: null })

  async function submit(e: FormEvent) {
    e.preventDefault()
    if (busy || !canConfirm || proposed === null) return
    setBusy(true)
    setError(null)
    try {
      await onConfirm(proposed, expected)
      if (alive.current) onClose()
    } catch (err) {
      const failure = failureText(err)
      // Closed meanwhile (the busy lock makes this a safety net): the dialog has nowhere to show it.
      if (!alive.current) return onLateFailure(failure)
      setError(failure)
      setBusy(false)
      setShown(null)
      setRetry((n) => n + 1) // a stale-dialog refusal means the figures above are out of date
    }
  }

  const note =
    parsed === null
      ? COPY.capitalInvalid
      : proposed === null
        ? COPY.capitalSame
        : !fresh && !loadError
          ? COPY.capitalChecking
          : null

  return (
    <Modal labelledBy={titleId} onClose={onClose} locked={busy}>
      <form className="capital-form" onSubmit={(e) => void submit(e)}>
        <h2 id={titleId} className="dialog-title">
          {COPY.capitalTitle}
        </h2>
        <p className="dialog-body">{COPY.capitalIntro}</p>
        <label className="field" htmlFor={inputId}>
          <span className="field-label">{COPY.capitalLabel}</span>
          <input
            id={inputId}
            className="input num"
            type="text"
            inputMode="numeric"
            autoComplete="off"
            value={text}
            onChange={(e) => setText(e.target.value)}
            aria-invalid={parsed === null || undefined}
          />
        </label>
        {note ? (
          <p className="muted" role="status">
            {note}
          </p>
        ) : null}
        {fresh && proposed !== null && data?.proposed ? (
          <p className="capital-equity num">{COPY.capitalEquityAt(inr(data.proposed.equity_inr) ?? '—')}</p>
        ) : null}
        {fresh && proposed !== null && data && !data.allowed ? (
          <div className="form-error" role="alert">
            <p>{refusalText(data.refusal ?? '')}</p>
          </div>
        ) : null}
        {loadError ? (
          <p className="form-error" role="alert">
            {data ? COPY.capitalLoadFailed(loadError) : COPY.capitalCurrentFailed(loadError)}
          </p>
        ) : null}
        <DataTable
          caption="Starting capital and the limits that move with it, now and after the change"
          columns={[
            { key: 'label', header: 'Figure', label: 'Figure', cell: (r: CapitalRow) => r.label },
            { key: 'old', header: 'Now', label: 'Now', align: 'right', nowrap: true, cell: (r) => r.old },
            { key: 'new', header: 'After', label: 'After', align: 'right', nowrap: true, cell: (r) => r.new },
          ]}
          rows={rows}
          rowKey={(r) => r.label}
        />
        {error ? (
          <p className="form-error" role="alert">
            {error}
          </p>
        ) : null}
        <div className="dialog-actions">
          <button type="button" className="btn" aria-disabled={busy || undefined} onClick={() => (busy ? undefined : onClose())}>
            {COPY.capitalCancel}
          </button>
          <button
            type="submit"
            className="btn btn-primary"
            disabled={!canConfirm}
            aria-disabled={busy || undefined}
          >
            {busy ? COPY.capitalSetting : COPY.capitalConfirm}
          </button>
        </div>
      </form>
    </Modal>
  )
}
