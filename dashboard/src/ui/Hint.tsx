/**
 * Toggletip: a small ⓘ button that explains a term in plain words, plus the engine's own name for it.
 * Opens on hover, keyboard focus or tap; a click/tap pins it open; Esc or an outside tap closes it.
 * The bubble is portalled to <body> with fixed coordinates, so no scrolling table or card can clip it;
 * on scroll or resize it follows its button (focusing a button below the fold scrolls the page, and
 * the Telegram transcript scrolls itself), and closes once the button is out of sight.
 */
import { useEffect, useId, useLayoutEffect, useRef, useState } from 'react'
import type { ReactNode } from 'react'
import { createPortal } from 'react-dom'
import type { Term } from '../copy/glossary.ts'
import { Icon } from './Icon'
import { place } from './float'
import type { Placement } from './float'

/**
 * The button's rect while any of it can be seen, else null. It is clipped by every scrolling ancestor
 * (the Telegram transcript, a sideways table), by the window, and — for content below the header — by
 * the sticky tab bar, so a pinned bubble never points at a button that has scrolled out of sight.
 */
function visibleRect(el: HTMLElement): DOMRect | null {
  const r = el.getBoundingClientRect()
  let { top, left, bottom, right } = r
  for (let p = el.parentElement; p && p !== document.body; p = p.parentElement) {
    const cs = getComputedStyle(p)
    if (cs.overflowX === 'visible' && cs.overflowY === 'visible') continue
    const c = p.getBoundingClientRect()
    top = Math.max(top, c.top)
    left = Math.max(left, c.left)
    bottom = Math.min(bottom, c.bottom)
    right = Math.min(right, c.right)
  }
  if (!el.closest('.appbar, .tabbar')) {
    const bar = document.querySelector('.tabbar')
    if (bar) top = Math.max(top, bar.getBoundingClientRect().bottom)
  }
  top = Math.max(top, 0)
  left = Math.max(left, 0)
  bottom = Math.min(bottom, window.innerHeight)
  right = Math.min(right, window.innerWidth)
  return bottom > top && right > left ? r : null
}

export function Hint({
  label,
  term,
  children,
  asTerm = false,
}: {
  label: string
  term?: string
  children?: ReactNode
  /** Render the label itself as the trigger (dotted underline) instead of a trailing ⓘ. */
  asTerm?: boolean
}) {
  const [open, setOpen] = useState(false)
  const pinned = useRef(false)
  const btn = useRef<HTMLButtonElement>(null)
  const bubble = useRef<HTMLSpanElement>(null)
  const closing = useRef<number | null>(null)
  const [at, setAt] = useState<Placement | null>(null)
  const id = useId()

  // WCAG 1.4.13 (hoverable): leaving the button starts a short close the bubble cancels, so the
  // pointer can cross the gap and rest on the explanation.
  const cancelClose = () => {
    if (closing.current !== null) window.clearTimeout(closing.current)
    closing.current = null
  }
  const scheduleClose = () => {
    cancelClose()
    closing.current = window.setTimeout(() => {
      if (!pinned.current) setOpen(false)
    }, 200)
  }
  useEffect(() => cancelClose, [])

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
    let frame = 0
    const follow = () => {
      window.cancelAnimationFrame(frame)
      frame = window.requestAnimationFrame(() => {
        const b = btn.current
        const bb = bubble.current
        if (!b || !bb) return
        const r = visibleRect(b)
        if (r === null) close()
        else setAt(place(r, bb.getBoundingClientRect()))
      })
    }
    document.addEventListener('keydown', onKey)
    document.addEventListener('pointerdown', onDown)
    window.addEventListener('scroll', follow, true)
    window.addEventListener('resize', follow)
    return () => {
      window.cancelAnimationFrame(frame)
      document.removeEventListener('keydown', onKey)
      document.removeEventListener('pointerdown', onDown)
      window.removeEventListener('scroll', follow, true)
      window.removeEventListener('resize', follow)
    }
  }, [open])

  if (!children && !term) return asTerm ? <>{label}</> : null

  return (
    <>
      <button
        ref={btn}
        type="button"
        className={asTerm ? 'term-btn' : 'hint-btn'}
        aria-label={asTerm ? undefined : `About ${label}`}
        aria-expanded={open}
        aria-describedby={open ? id : undefined}
        onClick={() => {
          pinned.current = !pinned.current
          setOpen(pinned.current)
        }}
        onMouseEnter={() => {
          cancelClose()
          setOpen(true)
        }}
        onMouseLeave={() => {
          if (!pinned.current) scheduleClose()
        }}
        onFocus={() => setOpen(true)}
        onBlur={() => {
          if (!pinned.current) setOpen(false)
        }}
      >
        {asTerm ? label : <Icon name="hint" size={14} />}
      </button>
      {open
        ? createPortal(
            <span
              ref={bubble}
              id={id}
              role="tooltip"
              className={`hint-bubble${at ? ` ${at.side}` : ''}`}
              style={at ? { top: at.top, left: at.left } : { top: 0, left: 0, visibility: 'hidden' }}
              onMouseEnter={cancelClose}
              onMouseLeave={() => {
                if (!pinned.current) scheduleClose()
              }}
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

/** A glossary term as text. With an explanation, the word itself is the toggletip (dotted underline);
 *  a term with only an engine name gets a native tooltip instead — no button for nothing to read. */
export function Labelled({ t, className }: { t: Term; className?: string }) {
  if (t.hint) {
    return (
      <span className={className ? `labelled ${className}` : 'labelled'}>
        <Hint label={t.label} term={t.term} asTerm>
          {t.hint}
        </Hint>
      </span>
    )
  }
  const nativeTitle = t.term && t.term !== t.label ? `Engine term: ${t.term}` : undefined
  return (
    <span className={className ? `labelled ${className}` : 'labelled'} title={nativeTitle}>
      {t.label}
    </span>
  )
}
