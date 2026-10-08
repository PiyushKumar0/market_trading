/** The open tab lives in the URL hash (#overview …), so reload, bookmarks and Back all work. */
import { useCallback, useEffect, useState } from 'react'
import type { TabId } from './alerts.ts'

export const TABS: { id: TabId; label: string }[] = [
  { id: 'overview', label: 'Overview' },
  { id: 'recommendations', label: 'Recommendations' },
  { id: 'paper', label: 'Paper trading' },
  { id: 'performance', label: 'Performance' },
  { id: 'activity', label: 'Activity' },
  { id: 'system', label: 'System' },
]

const IDS = new Set<string>(TABS.map((t) => t.id))

function fromHash(): TabId {
  const h = window.location.hash.replace(/^#/, '')
  return IDS.has(h) ? (h as TabId) : 'overview'
}

export function useHashTab(): [TabId, (t: TabId) => void] {
  const [tab, setTab] = useState<TabId>(fromHash)

  useEffect(() => {
    const onHash = () => setTab(fromHash())
    window.addEventListener('hashchange', onHash)
    return () => window.removeEventListener('hashchange', onHash)
  }, [])

  const go = useCallback((t: TabId) => {
    if (window.location.hash !== `#${t}`) window.location.hash = t // a history entry: Back returns here
    setTab(t)
    window.scrollTo({ top: 0 })
  }, [])

  return [tab, go]
}
