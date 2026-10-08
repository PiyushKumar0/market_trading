/** Recommendations tab — STUB, replaced in Task 6. Props are final. */
import type { RecommendationsResponse } from '../types'
import { EmptyState } from '../ui/Card'

export function Recommendations(props: { data: RecommendationsResponse | null; nowMs: number }) {
  void props
  return (
    <>
      <h1 className="view-title">Recommendations</h1>
      <EmptyState>This section is being rebuilt.</EmptyState>
    </>
  )
}
