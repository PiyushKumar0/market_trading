/**
 * The page's one container, in three tiers (design spec §6):
 *  - primary: raised surface, border, shadow — things you act on or watch;
 *  - secondary: flat, quieter — reference material;
 *  - simulated: paper money — dashed violet border and a "Simulated" tag, so it is never mistaken
 *    for the real account.
 * `id` makes the card a focus target (the health pill moves focus to the alerts card).
 */
import { useId } from 'react'
import type { ReactNode } from 'react'
import { Pill } from './Pill'

export type CardTier = 'primary' | 'secondary' | 'simulated'

export function Card({
  title,
  id,
  tier = 'primary',
  meta,
  action,
  children,
  className,
  titleTag = 'h2',
  simTag = true,
  hint,
}: {
  title: ReactNode
  /** A Hint shown beside the title — outside the heading, so it never joins the card's name. */
  hint?: ReactNode
  id?: string
  tier?: CardTier
  meta?: ReactNode
  action?: ReactNode
  children?: ReactNode
  className?: string
  titleTag?: 'h2' | 'h3'
  /** Show the "Simulated" tag on a simulated card (off where the page already says it once). */
  simTag?: boolean
}) {
  const hid = useId()
  const H = titleTag
  return (
    <section
      id={id}
      tabIndex={id ? -1 : undefined}
      className={['card', `card-${tier}`, className].filter(Boolean).join(' ')}
      aria-labelledby={hid}
    >
      <header className="card-head">
        <div className="card-heading">
          <H id={hid} className="card-title">
            {title}
          </H>
          {hint}
          {tier === 'simulated' && simTag ? (
            <Pill tone="sim" icon="flask" title="Paper trading: simulated money, never a real order">
              Simulated
            </Pill>
          ) : null}
        </div>
        {meta ? <div className="card-meta">{meta}</div> : null}
        {action ? <div className="card-action">{action}</div> : null}
      </header>
      {children}
    </section>
  )
}

/** One quiet line: what this is and why it is empty. */
export function EmptyState({ children }: { children: ReactNode }) {
  return <p className="empty">{children}</p>
}
