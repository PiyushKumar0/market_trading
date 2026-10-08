/** The open tab lives in the URL hash (#overview …), so reload, bookmarks and Back all work. */
import { useCallback, useEffect, useState } from 'react'
import type { TabId } from './alerts.ts'
import { tabOfHash } from './tabs.ts'

export { TABS } from './tabs.ts'

export function useHashTab(): [TabId, (t: TabId) => void] {
  const [tab, setTab] = useState<TabId>(() => tabOfHash(window.location.hash) ?? 'overview')

  useEffect(() => {
    // Every tab opens at its top. The browser's own restore would apply the previous tab's scroll
    // offset to a view that hasn't rendered yet (Back from a short tab to a long one lands mid-page).
    if ('scrollRestoration' in window.history) window.history.scrollRestoration = 'manual'
    const onHash = () => {
      const next = tabOfHash(window.location.hash)
      if (next === null) return // not a tab: keep the one that is open
      setTab(next)
      window.scrollTo({ top: 0 })
    }
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
