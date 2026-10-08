/** Inline SVG icons drawn for this dashboard (no icon font, no CDN — R10). Decorative: the text beside
 *  an icon always carries the meaning, so every icon is `aria-hidden`. */
import type { ReactNode } from 'react'

export type IconName =
  | 'check'
  | 'alert'
  | 'x'
  | 'x-circle'
  | 'info'
  | 'clock'
  | 'logout'
  | 'chevron-down'
  | 'chevron-right'
  | 'flask'
  | 'shield'
  | 'shield-off'
  | 'dot'
  | 'hint'
  | 'pause'

const PATHS: Record<IconName, ReactNode> = {
  check: <polyline points="20 6 9 17 4 12" />,
  alert: (
    <>
      <path d="M12 3.5 2.5 20h19L12 3.5Z" />
      <line x1="12" y1="10" x2="12" y2="14" />
      <line x1="12" y1="17.2" x2="12" y2="17.3" />
    </>
  ),
  x: (
    <>
      <line x1="6" y1="6" x2="18" y2="18" />
      <line x1="18" y1="6" x2="6" y2="18" />
    </>
  ),
  'x-circle': (
    <>
      <circle cx="12" cy="12" r="9" />
      <line x1="9" y1="9" x2="15" y2="15" />
      <line x1="15" y1="9" x2="9" y2="15" />
    </>
  ),
  info: (
    <>
      <circle cx="12" cy="12" r="9" />
      <line x1="12" y1="11" x2="12" y2="16.5" />
      <line x1="12" y1="7.8" x2="12" y2="7.9" />
    </>
  ),
  clock: (
    <>
      <circle cx="12" cy="12" r="9" />
      <polyline points="12 7 12 12 15.5 14" />
    </>
  ),
  logout: (
    <>
      <path d="M14 4h4.5a1.5 1.5 0 0 1 1.5 1.5v13a1.5 1.5 0 0 1-1.5 1.5H14" />
      <polyline points="9.5 16.5 5 12 9.5 7.5" />
      <line x1="5" y1="12" x2="15.5" y2="12" />
    </>
  ),
  'chevron-down': <polyline points="6 9 12 15 18 9" />,
  'chevron-right': <polyline points="9 6 15 12 9 18" />,
  flask: (
    <>
      <path d="M9 3.5h6" />
      <path d="M10 3.5v5.8L5.2 18.6A1.3 1.3 0 0 0 6.4 20.5h11.2a1.3 1.3 0 0 0 1.2-1.9L14 9.3V3.5" />
      <path d="M7.6 14.5h8.8" />
    </>
  ),
  shield: (
    <>
      <path d="M12 3 5 5.8v5.7c0 4.3 2.9 7.6 7 9.5 4.1-1.9 7-5.2 7-9.5V5.8L12 3Z" />
      <polyline points="9 12 11.2 14.2 15.2 10" />
    </>
  ),
  'shield-off': (
    <>
      <path d="M12 3 5 5.8v5.7c0 4.3 2.9 7.6 7 9.5 4.1-1.9 7-5.2 7-9.5V5.8L12 3Z" />
      <line x1="9.5" y1="9.5" x2="14.5" y2="14.5" />
      <line x1="14.5" y1="9.5" x2="9.5" y2="14.5" />
    </>
  ),
  dot: <circle cx="12" cy="12" r="4.5" fill="currentColor" stroke="none" />,
  hint: (
    <>
      <circle cx="12" cy="12" r="9" />
      <path d="M9.6 9.4a2.5 2.5 0 0 1 4.8.9c0 1.7-2.4 2.2-2.4 3.7" />
      <line x1="12" y1="17.2" x2="12" y2="17.3" />
    </>
  ),
  pause: (
    <>
      <circle cx="12" cy="12" r="9" />
      <line x1="10" y1="9" x2="10" y2="15" />
      <line x1="14" y1="9" x2="14" y2="15" />
    </>
  ),
}

export function Icon({ name, size = 16, className }: { name: IconName; size?: number; className?: string }) {
  return (
    <svg
      className={className ? `icon ${className}` : 'icon'}
      width={size}
      height={size}
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth={2}
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
      focusable="false"
    >
      {PATHS[name]}
    </svg>
  )
}
