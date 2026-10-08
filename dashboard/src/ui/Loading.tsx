/**
 * The empty line of a card whose route has no data yet. While the route is still on its way it says
 * "Loading …"; once that route's latest fetch FAILED it says so instead — a card that never got an
 * answer must not claim to be loading forever. (A route that answered before and fails now keeps its
 * last data, so it never reaches this line.)
 */
import { createContext, useContext } from 'react'
import { EmptyState } from './Card'

/** The snapshot keys whose fetch failed in the latest poll cycle (usePoll `failedKeys`). */
export const FailedRoutes = createContext<readonly string[]>([])

export function Loading({ route, what }: { route: string; what: string }) {
  const failed = useContext(FailedRoutes).includes(route)
  return (
    <EmptyState>
      {failed ? `Couldn't load ${what}. The page keeps retrying every 10 seconds.` : `Loading ${what}…`}
    </EmptyState>
  )
}
