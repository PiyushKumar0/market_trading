/**
 * An anchored panel (the trading-hours editor). Focus moves into it when it opens; Esc or a click
 * outside closes it and focus returns to the trigger. Portalled with fixed coordinates and re-placed
 * on scroll, so a half-typed form is never lost to a scroll.
 */
import { useCallback, useEffect, useId, useLayoutEffect, useRef, useState } from 'react'
import type { ReactNode, Ref } from 'react'
import { createPortal } from 'react-dom'
import { place } from './float'
import type { Placement } from './float'

export interface TriggerProps {
  ref: Ref<HTMLButtonElement>
  onClick: () => void
  'aria-expanded': boolean
  'aria-controls': string
  'aria-haspopup': 'dialog'
}

export function Popover({
  title,
  trigger,
  children,
  align = 'end',
}: {
  title: string
  trigger: (p: TriggerProps) => ReactNode
  children: (close: () => void) => ReactNode
  align?: 'center' | 'end'
}) {
  const [open, setOpen] = useState(false)
  const [at, setAt] = useState<Placement | null>(null)
  const btn = useRef<HTMLButtonElement>(null)
  const panel = useRef<HTMLDivElement>(null)
  const focused = useRef(false)
  const id = useId()

  const close = useCallback((refocus = true) => {
    setOpen(false)
    if (refocus) btn.current?.focus()
  }, [])

  const reposition = useCallback(() => {
    if (btn.current && panel.current) {
      setAt(place(btn.current.getBoundingClientRect(), panel.current.getBoundingClientRect(), align))
    }
  }, [align])

  useLayoutEffect(() => {
    if (open) reposition()
    else {
      setAt(null)
      focused.current = false
    }
  }, [open, reposition])

  useEffect(() => {
    if (open && at && !focused.current) {
      focused.current = true
      panel.current?.querySelector<HTMLElement>('input, select, textarea, button')?.focus()
    }
  }, [open, at])

  useEffect(() => {
    if (!open) return
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') close()
    }
    const onDown = (e: PointerEvent) => {
      const t = e.target as Node
      if (!panel.current?.contains(t) && !btn.current?.contains(t)) close(false)
    }
    const onMove = () => window.requestAnimationFrame(reposition)
    document.addEventListener('keydown', onKey)
    document.addEventListener('pointerdown', onDown)
    window.addEventListener('scroll', onMove, true)
    window.addEventListener('resize', onMove)
    return () => {
      document.removeEventListener('keydown', onKey)
      document.removeEventListener('pointerdown', onDown)
      window.removeEventListener('scroll', onMove, true)
      window.removeEventListener('resize', onMove)
    }
  }, [open, close, reposition])

  return (
    <>
      {trigger({
        ref: btn,
        onClick: () => setOpen((o) => !o),
        'aria-expanded': open,
        'aria-controls': id,
        'aria-haspopup': 'dialog',
      })}
      {open
        ? createPortal(
            <div
              ref={panel}
              id={id}
              role="dialog"
              aria-label={title}
              className="popover"
              style={at ? { top: at.top, left: at.left } : { top: 0, left: 0, visibility: 'hidden' }}
            >
              {children(() => close())}
            </div>,
            document.body,
          )
        : null}
    </>
  )
}
