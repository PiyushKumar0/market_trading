/** The six sections (WAI-ARIA tabs pattern: ←/→/Home/End move and select; one tab stop). A badge
 *  shows when a section holds something that needs you; screen readers hear its meaning, not "!". */
import { useRef } from 'react'
import type { KeyboardEvent } from 'react'
import type { Badge, TabId } from '../lib/alerts.ts'
import { TABS } from '../lib/useHashTab.ts'

export function Tabs({
  tab,
  onTab,
  badges,
}: {
  tab: TabId
  onTab: (t: TabId) => void
  badges: Partial<Record<TabId, Badge>>
}) {
  const refs = useRef<(HTMLButtonElement | null)[]>([])
  const idx = Math.max(0, TABS.findIndex((t) => t.id === tab))

  function onKey(e: KeyboardEvent<HTMLDivElement>) {
    const last = TABS.length - 1
    const next =
      e.key === 'ArrowRight' ? (idx === last ? 0 : idx + 1)
      : e.key === 'ArrowLeft' ? (idx === 0 ? last : idx - 1)
      : e.key === 'Home' ? 0
      : e.key === 'End' ? last
      : -1
    if (next < 0) return
    e.preventDefault()
    onTab(TABS[next].id)
    refs.current[next]?.focus()
  }

  return (
    <div className="tabs" role="tablist" aria-label="Sections" onKeyDown={onKey}>
      {TABS.map((t, i) => {
        const b = badges[t.id]
        const selected = t.id === tab
        return (
          <button
            key={t.id}
            ref={(el) => {
              refs.current[i] = el
            }}
            type="button"
            role="tab"
            id={`tab-${t.id}`}
            // Only the open tab's panel is rendered: the others would point at an id that doesn't exist.
            aria-controls={selected ? `panel-${t.id}` : undefined}
            aria-selected={selected}
            tabIndex={selected ? 0 : -1}
            className="tab"
            onClick={() => onTab(t.id)}
          >
            <span>{t.label}</span>
            {b ? (
              <>
                <span className={`badge badge-${b.tone}`} aria-hidden="true">
                  {b.text}
                </span>
                <span className="sr-only">, {b.label}</span>
              </>
            ) : null}
          </button>
        )
      })}
    </div>
  )
}
