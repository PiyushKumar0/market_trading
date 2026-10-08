/** Placement for floating panels (hint bubbles, the hours popover): below the anchor when it fits,
 *  above otherwise, always clamped inside the viewport. `position: fixed` coordinates, so a bubble is
 *  never clipped by a scrolling table or card. */
export interface Placement {
  top: number
  left: number
  side: 'below' | 'above'
}

const GAP = 6
const MARGIN = 8

export function place(anchor: DOMRect, panel: DOMRect, align: 'center' | 'end' = 'center'): Placement {
  const vw = window.innerWidth
  const vh = window.innerHeight
  let left = align === 'end' ? anchor.right - panel.width : anchor.left + anchor.width / 2 - panel.width / 2
  left = Math.max(MARGIN, Math.min(left, vw - panel.width - MARGIN))
  const below = anchor.bottom + GAP
  const above = anchor.top - GAP - panel.height
  if (below + panel.height <= vh - MARGIN || above < MARGIN) {
    return { top: Math.max(MARGIN, Math.min(below, vh - panel.height - MARGIN)), left, side: 'below' }
  }
  return { top: above, left, side: 'above' }
}
