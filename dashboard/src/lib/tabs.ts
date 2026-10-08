/** The six sections and how a URL hash maps to one. Pure and DOM-free (the hook lives in useHashTab). */
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

/** The tab a hash names. No hash (the page's first history entry) is the Overview; any other hash
 *  (#main, an anchor typed by hand) is not a tab at all — null. */
export function tabOfHash(hash: string): TabId | null {
  const h = hash.replace(/^#/, '')
  if (h === '') return 'overview'
  return IDS.has(h) ? (h as TabId) : null
}
