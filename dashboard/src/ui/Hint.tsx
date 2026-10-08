/**
 * Toggletip: a small ⓘ button that explains a term in plain words, plus the engine's own name for it.
 * Opens on hover, keyboard focus or tap; a click/tap pins it open; Esc, an outside tap, scrolling or
 * resizing closes it. The bubble is portalled to <body> with fixed coordinates, so no scrolling table
 * or card can clip it.
 */
import { useEffect, useId, useLayoutEffect, useRef, useState } from 'react'
import type { ReactNode } from 'react'
import { createPortal } from 'react-dom'
import type { Term } from '../copy/glossary.ts'
import { Icon } from './Icon'
import { place } from './float'
import type { Placement } from './float'

export function Hint({ label, term, children }: { label: string; term?: string; children?: ReactNode }) {
  const [open, setOpen] = useState(false)
  const pinned = useRef(false)
  const btn = useRef<HTMLButtonElement>(null)
  const bubble = useRef<HTMLSpanElement>(null)
  const [at, setAt] = useState<Placement | null>(null)
  const id = useId()

  useLayoutEffect(() => {
    if (open && btn.current && bubble.current) {
      setAt(place(btn.current.getBoundingClientRect(), bubble.current.getBoundingClientRect()))
    } else {
      setAt(null)
    }
  }, [open])

  useEffect(() => {
    if (!open) return
    const close = () => {
      pinned.current = false
      setOpen(false)
    }
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') close()
    }
    const onDown = (e: PointerEvent) => {
      const t = e.target as Node
      if (!btn.current?.contains(t) && !bubble.current?.contains(t)) close()
    }
    document.addEventListener('keydown', onKey)
    document.addEventListener('pointerdown', onDown)
    window.addEventListener('scroll', close, true)
    window.addEventListener('resize', close)
    return () => {
      document.removeEventListener('keydown', onKey)
      document.removeEventListener('pointerdown', onDown)
      window.removeEventListener('scroll', close, true)
      window.removeEventListener('resize', close)
    }
  }, [open])

  if (!children && !term) return null

  return (
    <>
      <button
        ref={btn}
        type="button"
        className="hint-btn"
        aria-label={`About ${label}`}
        aria-expanded={open}
        aria-describedby={open ? id : undefined}
        onClick={() => {
          pinned.current = !pinned.current
          setOpen(pinned.current)
        }}
        onMouseEnter={() => setOpen(true)}
        onMouseLeave={() => {
          if (!pinned.current) setOpen(false)
        }}
        onFocus={() => setOpen(true)}
        onBlur={() => {
          if (!pinned.current) setOpen(false)
        }}
      >
        <Icon name="hint" size={14} />
      </button>
      {open
        ? createPortal(
            <span
              ref={bubble}
              id={id}
              role="tooltip"
              className={`hint-bubble${at ? ` ${at.side}` : ''}`}
              style={at ? { top: at.top, left: at.left } : { top: 0, left: 0, visibility: 'hidden' }}
            >
              {children ? <span className="hint-text">{children}</span> : null}
              {term ? (
                <span className="hint-term">
                  Engine term: <code>{term}</code>
                </span>
              ) : null}
            </span>,
            document.body,
          )
        : null}
    </>
  )
}

/** A glossary term as text, with its ⓘ hint when it has an explanation. A term with only an engine
 *  name gets a native tooltip instead — no extra button for nothing to read. */
export function Labelled({ t, className }: { t: Term; className?: string }) {
  const nativeTitle = !t.hint && t.term && t.term !== t.label ? `Engine term: ${t.term}` : undefined
  return (
    <span className={className ? `labelled ${className}` : 'labelled'} title={nativeTitle}>
      {t.label}
      {t.hint ? (
        <Hint label={t.label} term={t.term}>
          {t.hint}
        </Hint>
      ) : null}
    </span>
  )
}
