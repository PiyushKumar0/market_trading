/**
 * The navbar: brand, the health pill (worst active alert), the three owner controls, data freshness,
 * sign-out — then the section tabs. Desktop keeps both rows on screen; below 1200 px only the tab row
 * sticks, so the controls don't eat a laptop or phone screen.
 */
import { useLayoutEffect, useRef } from 'react'
import type { ReactNode } from 'react'
import type { Badge, Health, TabId } from '../lib/alerts.ts'
import { istTime } from '../lib/format.ts'
import type { ConnState } from '../types'
import { Hint } from '../ui/Hint'
import { Icon } from '../ui/Icon'
import type { IconName } from '../ui/Icon'
import { Tabs } from './Tabs'

export function BrandMark({ size = 28 }: { size?: number }) {
  return (
    <svg width={size} height={size} viewBox="0 0 32 32" aria-hidden="true" focusable="false" className="brand-mark">
      <rect width="32" height="32" rx="8" fill="var(--surface-3)" />
      <g fill="none" stroke="var(--accent)" strokeWidth="3" strokeLinecap="round" strokeLinejoin="round">
        <polyline points="6 21.5 12.5 15 17 19 25.5 10.5" />
        <polyline points="19.5 10.5 25.5 10.5 25.5 16.5" />
      </g>
    </svg>
  )
}

const HEALTH: Record<Health['level'], { tone: string; icon: IconName }> = {
  pending: { tone: 'pending', icon: 'dot' },
  ok: { tone: 'good', icon: 'check' },
  notice: { tone: 'note', icon: 'info' },
  warning: { tone: 'warn', icon: 'alert' },
  critical: { tone: 'crit', icon: 'x-circle' },
}

export function HealthPill({ health, onClick }: { health: Health; onClick: () => void }) {
  const h = HEALTH[health.level]
  return (
    <>
      <button
        type="button"
        className={`health health-${h.tone}`}
        onClick={onClick}
        title={
          health.level === 'ok'
            ? 'Nothing needs your attention'
            : health.level === 'pending'
              ? 'Waiting for the engine to answer'
              : 'Show what needs attention'
        }
      >
        <Icon name={h.icon} />
        <span>{health.text}</span>
      </button>
      <span className="sr-only" role="status">
        {health.text}
      </span>
    </>
  )
}

export function Connection({
  conn,
  lastOkAt,
  unreachable,
}: {
  conn: ConnState
  /** When a refresh last brought back any data — never merely when one was attempted. */
  lastOkAt: string | null
  /** The latest refresh failed for every route: "Updated" would claim a refresh that didn't happen. */
  unreachable: boolean
}) {
  const live = conn === 'open'
  return (
    <span className={unreachable ? 'conn conn-down' : 'conn'}>
      <Icon name="dot" size={12} className={unreachable ? 'conn-bad' : live ? 'conn-live' : 'conn-off'} />
      <span className="conn-text">
        {unreachable ? (
          "Can't reach the engine"
        ) : lastOkAt ? (
          <>
            Updated <span className="num">{istTime(lastOkAt, { seconds: true })}</span>
          </>
        ) : (
          'Connecting…'
        )}
      </span>
      <span className="sr-only">
        {unreachable ? '' : live ? ', live updates connected' : ', live updates reconnecting'}
      </span>
      <Hint label="Data updates">
        {unreachable
          ? `${lastOkAt ? `No answer from the engine since ${istTime(lastOkAt, { seconds: true })}.` : 'The engine has not answered yet.'} The figures shown are the last ones received; the page retries every 10 seconds.`
          : live
            ? 'Live updates are connected. Everything else refreshes every 10 seconds.'
            : 'Live updates are reconnecting on their own. Data still refreshes every 10 seconds.'}
      </Hint>
    </span>
  )
}

export function AppBar({
  health,
  onHealth,
  controls,
  conn,
  lastOkAt,
  unreachable,
  onSignOut,
  tab,
  onTab,
  badges,
}: {
  health: Health
  onHealth: () => void
  controls: ReactNode
  conn: ConnState
  lastOkAt: string | null
  unreachable: boolean
  onSignOut: () => void
  tab: TabId
  onTab: (t: TabId) => void
  badges: Partial<Record<TabId, Badge>>
}) {
  const header = useRef<HTMLElement>(null)

  // The tab row sticks just under the header on wide screens: publish the header's height.
  useLayoutEffect(() => {
    const el = header.current
    if (!el) return
    const publish = () => document.documentElement.style.setProperty('--appbar-h', `${el.offsetHeight}px`)
    publish()
    const ro = new ResizeObserver(publish)
    ro.observe(el)
    return () => ro.disconnect()
  }, [])

  return (
    <>
      <header className="appbar" ref={header}>
        <div className="appbar-inner">
          <div className="appbar-brand">
            {/* Below 640 px the name is hidden and the mark is decorative: the label keeps the link named. */}
            <a
              className="brand"
              href="#overview"
              aria-label="Market Trading: go to the Overview"
              onClick={(e) => {
                e.preventDefault()
                onTab('overview')
              }}
            >
              <BrandMark />
              <span className="brand-name">Market Trading</span>
            </a>
            <HealthPill health={health} onClick={onHealth} />
          </div>
          <div className="appbar-controls">{controls}</div>
          <div className="appbar-meta">
            <Connection conn={conn} lastOkAt={lastOkAt} unreachable={unreachable} />
            <button
              type="button"
              className="icon-btn"
              aria-label="Sign out (forget the token in this browser)"
              title="Sign out"
              onClick={onSignOut}
            >
              <Icon name="logout" size={18} />
            </button>
          </div>
        </div>
      </header>
      <div className="tabbar">
        <div className="tabbar-inner">
          <Tabs tab={tab} onTab={onTab} badges={badges} />
        </div>
      </div>
    </>
  )
}
