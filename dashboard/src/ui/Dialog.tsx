/**
 * Confirmation dialog on the native <dialog> (focus trap, Esc = cancel, inert page behind). Replaces
 * `window.confirm`: the buttons name the action ("Turn off" / "Keep recommending"), never OK/Cancel.
 * The cancel button comes first in the DOM, so it is what `showModal()` focuses — the safe default.
 */
import { useCallback, useEffect, useId, useRef, useState } from 'react'
import type { JSX } from 'react'
import type { DialogCopy } from '../copy/glossary.ts'

export interface ConfirmRequest extends DialogCopy {
  tone?: 'accent' | 'crit'
}

export function useConfirm(): { confirm: (r: ConfirmRequest) => Promise<boolean>; dialog: JSX.Element } {
  const [req, setReq] = useState<(ConfirmRequest & { resolve: (ok: boolean) => void }) | null>(null)
  const ref = useRef<HTMLDialogElement>(null)
  const titleId = useId()
  const bodyId = useId()

  useEffect(() => {
    const el = ref.current
    if (req && el && !el.open) el.showModal()
  }, [req])

  const confirm = useCallback(
    (r: ConfirmRequest) =>
      new Promise<boolean>((resolve) => {
        setReq({ ...r, resolve })
      }),
    [],
  )

  const finish = (ok: boolean) => {
    req?.resolve(ok)
    ref.current?.close()
    setReq(null)
  }

  const dialog = (
    <dialog
      ref={ref}
      className="dialog"
      aria-labelledby={titleId}
      aria-describedby={bodyId}
      onCancel={(e) => {
        e.preventDefault()
        finish(false)
      }}
    >
      {req ? (
        <form
          onSubmit={(e) => {
            e.preventDefault()
            finish(true)
          }}
        >
          <h2 id={titleId} className="dialog-title">
            {req.title}
          </h2>
          <p id={bodyId} className="dialog-body">
            {req.body}
          </p>
          <div className="dialog-actions">
            <button type="button" className="btn" onClick={() => finish(false)}>
              {req.cancel}
            </button>
            <button type="submit" className={req.tone === 'crit' ? 'btn btn-danger' : 'btn btn-primary'}>
              {req.confirm}
            </button>
          </div>
        </form>
      ) : null}
    </dialog>
  )

  return { confirm, dialog }
}
