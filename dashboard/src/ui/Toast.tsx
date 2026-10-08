/** Results of owner writes ("Saved: 09:30–15:00", "Couldn't change the mode: …"). Successes fade after
 *  6 s; failures stay until dismissed, so a refusal is never missed. Replaces `window.alert`. */
import { useCallback, useRef, useState } from 'react'
import type { JSX } from 'react'
import { Icon } from './Icon'

export interface ToastMsg {
  id: number
  tone: 'good' | 'crit' | 'note'
  text: string
}

const ICON = { good: 'check', crit: 'x-circle', note: 'info' } as const

export function useToasts(): { push: (tone: ToastMsg['tone'], text: string) => void; region: JSX.Element } {
  const [toasts, setToasts] = useState<ToastMsg[]>([])
  const seq = useRef(0)

  const dismiss = useCallback((id: number) => setToasts((all) => all.filter((t) => t.id !== id)), [])

  const push = useCallback(
    (tone: ToastMsg['tone'], text: string) => {
      seq.current += 1
      const id = seq.current
      setToasts((all) => [...all, { id, tone, text }].slice(-4))
      if (tone !== 'crit') window.setTimeout(() => dismiss(id), 6000)
    },
    [dismiss],
  )

  const region = (
    <div className="toasts" role="status" aria-live="polite">
      {toasts.map((t) => (
        <div key={t.id} className={`toast toast-${t.tone}`}>
          <Icon name={ICON[t.tone]} />
          <span className="toast-text">{t.text}</span>
          <button type="button" className="icon-btn" aria-label="Dismiss" onClick={() => dismiss(t.id)}>
            <Icon name="x" size={14} />
          </button>
        </div>
      ))}
    </div>
  )

  return { push, region }
}
