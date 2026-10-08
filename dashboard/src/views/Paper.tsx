/** Paper trading tab — STUB, replaced in Task 7. Props are final. */
import type { PaperResponse } from '../types'
import { EmptyState } from '../ui/Card'

export function Paper(props: { paper: PaperResponse | null; mode: string | null }) {
  void props
  return (
    <>
      <h1 className="view-title">Paper trading</h1>
      <EmptyState>This section is being rebuilt.</EmptyState>
    </>
  )
}
