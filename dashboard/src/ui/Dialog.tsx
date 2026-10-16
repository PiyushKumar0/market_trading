/**
 * Confirmation dialog on the native <dialog> (focus trap, Esc = cancel, inert page behind). Replaces
 * `window.confirm`: the buttons name the action ("Turn off" / "Keep recommending"), never OK/Cancel.
 * The cancel button comes first in the DOM, so it is what `showModal()` focuses — the safe default.
 */
import { useCallback, useEffect, useId, useRef, useState } from 'react'
import type { JSX, ReactNode } from 'react'
import type { DialogCopy } from '../copy/glossary.ts'

export interface ConfirmRequest extends DialogCopy {
  tone?: 'accent' | 'crit'
}

/**
 * A modal with caller-supplied content (a form, a live preview) on the same native <dialog> and styles.
 * Mount it to open it; Esc calls `onClose`. The caller owns the buttons and the title (`aria-labelledby`).
 */
export function Modal({
  labelledBy,
  onClose,
  locked,
  children,
}: {
  labelledBy: string
  onClose: () => void
  /** A write is on its way: Esc is ignored (and a native close undone) until it answers. */
  locked?: boolean
  children: ReactNode
}) {
  const ref = useRef<HTMLDialogElement>(null)
  useEffect(() => {
    const el = ref.current
    // Unmounting detaches the <dialog> before this cleanup runs, so close() cannot restore focus:
    // remember the opener and return to it ourselves.
    const opener = document.activeElement instanceof HTMLElement ? document.activeElement : null
    if (el && !el.open) el.showModal()
    return () => {
      el?.close()
      if (opener?.isConnected) opener.focus()
    }
  }, [])
  return (
    <dialog
      ref={ref}
      className="dialog"
      aria-labelledby={labelledBy}
      onCancel={(e) => {
        e.preventDefault()
        if (!locked) onClose()
      }}
      // A repeated Esc with no click between is a NON-cancelable `cancel` (close-watcher anti-trap), and
      // Android Back closes natively too: the browser closes the <dialog> without telling React. While
      // locked, put it back so the pending write can still show its answer; otherwise tell the parent so
      // it unmounts (and can open the dialog again). `!el.open` skips StrictMode's dev re-mount close.
      onClose={(e) => {
        const el = e.currentTarget
        if (el.open) return
        if (locked) el.showModal()
        else onClose()
      }}
    >
      {children}
    </dialog>
  )
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
