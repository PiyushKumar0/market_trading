/** System tab, AI usage budget: this quota week's spend, the AI level, the reset time and per-analyst spend. */
import type { Term } from '../../copy/glossary.ts'
import { agentName, tierTerm } from '../../copy/glossary.ts'
import { istDay, istTime, shortDate, usd } from '../../lib/format.ts'
import type { BudgetResponse } from '../../types'
import { Card, EmptyState } from '../../ui/Card'
import { KeyValue, Meter } from '../../ui/Figures'
import { Hint, Labelled } from '../../ui/Hint'
import { Pill } from '../../ui/Pill'
import type { Tone } from '../../ui/Pill'

const TIER_TONE: Record<string, Tone> = { DG0: 'good', DG1: 'note', DG2: 'warn', DG3: 'warn', DG4: 'crit' }

const FORWARD_KEY: Term = {
  label: 'Candidates sent to the AI per day',
  hint: 'How many scanner candidates per day can go to the AI analyst at the current level.',
  term: 'forward_cap',
}

const plural = (n: number, w: string) => `${n} ${w}${n === 1 ? '' : 's'}`

/** "Resets in 3 days 4 hours" / "Resets in 3 hours" / "Resetting now"; null without a usable end. */
function resetText(end: string | undefined, nowMs: number): string | null {
  const t = end ? Date.parse(end) : NaN
  if (Number.isNaN(t)) return null
  const left = t - nowMs
  if (left <= 0) return 'Resetting now'
  const hours = Math.floor(left / 3_600_000)
  if (hours < 1) return 'Resets in less than an hour'
  const days = Math.floor(hours / 24)
  if (days < 1) return `Resets in ${plural(hours, 'hour')}`
  return `Resets in ${plural(days, 'day')} ${plural(hours % 24, 'hour')}`
}

const stamp = (iso: string) => `${shortDate(istDay(iso))}, ${istTime(iso)}`
const usedOf = (spent: string | undefined, of: string | undefined): number | null => {
  const s = Number(spent)
  const o = Number(of)
  return Number.isFinite(s) && Number.isFinite(o) && o > 0 ? s : null
}

export function AiBudget({ budget: data, nowMs }: { budget: BudgetResponse | null; nowMs: number }) {
  const b = data?.budget
  const tier = data?.degrade_tier ?? null
  const level = tierTerm(tier)
  const levelPill = (
    <span className="sys-level">
      <Pill tone={TIER_TONE[tier ?? ''] ?? 'neutral'}>{level.label}</Pill>
      {level.hint ? <Hint label="AI level" term={level.term}>{level.hint}</Hint> : null}
    </span>
  )
  const levelKey: Term = { label: 'AI level', hint: 'How freely the AI analysts may spend this week’s budget.' }

  if (!b || Object.keys(b).length === 0) {
    return (
      <Card title="AI usage budget">
        <EmptyState>The AI budget isn&apos;t available: the budget governor isn&apos;t connected.</EmptyState>
        {tier ? <KeyValue items={[{ k: <Labelled t={levelKey} />, v: levelPill }]} /> : null}
      </Card>
    )
  }

  const allocations = b.allocations_usd ?? {}
  const spend = b.per_agent_spend_usd ?? {}
  // Union, not the allocation keys alone: an agent that billed without an allocation (sdk_smoke) is
  // inside the weekly total, so it needs a row or the list contradicts the total.
  const agents = Object.keys({ ...allocations, ...spend }).sort()
  const spentShown = usd(b.window_spend_usd)
  const creditShown = usd(b.credit_usd)
  const total = spentShown ? (creditShown ? `${spentShown} of ${creditShown}` : spentShown) : '—'
  const exact = b.window_spend_usd ? `Exact: $${b.window_spend_usd}${b.credit_usd ? ` of $${b.credit_usd}` : ''}` : null
  const reset = resetText(b.window_end, nowMs)
  const weekKey: Term = {
    label: 'Budget week',
    hint: 'The AI budget runs Thursday 14:00 to Thursday 14:00 IST.',
    term: b.window_key,
  }

  return (
    <Card title="AI usage budget">
      <Meter
        label={
          <span className="labelled">
            Spent this week
            {exact ? <Hint label="Spent this week">{exact}</Hint> : null}
          </span>
        }
        used={usedOf(b.window_spend_usd, b.credit_usd)}
        limit={Number(b.credit_usd) > 0 ? Number(b.credit_usd) : null}
        text={total}
        ariaLabel={`Spent this week: ${total}`}
      />
      <KeyValue
        items={[
          { k: <Labelled t={levelKey} />, v: levelPill },
          {
            k: <Labelled t={weekKey} />,
            v:
              b.window_start && b.window_end ? (
                <>
                  {stamp(b.window_start)} to {stamp(b.window_end)} IST
                  {reset ? <span className="sys-sub">{reset}</span> : null}
                </>
              ) : (
                (reset ?? '—')
              ),
          },
          { k: <Labelled t={FORWARD_KEY} />, v: b.forward_cap !== undefined ? b.forward_cap : '—' },
        ]}
      />
      <h3 className="sys-subhead">Spend by analyst</h3>
      {agents.length === 0 ? (
        <EmptyState>No analyst has spent anything this week.</EmptyState>
      ) : (
        <div className="sys-meters">
          {agents.map((id) => {
            const spentStr = spend[id] ?? '0'
            const alloc = allocations[id]
            const s = usd(spentStr) ?? '—'
            const text = alloc ? `${s} of ${usd(alloc)}` : `${s} (no allocation)`
            const label = agentName(id)
            return (
              <Meter
                key={id}
                label={<Labelled t={label} />}
                used={alloc ? usedOf(spentStr, alloc) : null}
                limit={alloc && Number(alloc) > 0 ? Number(alloc) : null}
                text={text}
                ariaLabel={`${label.label}: ${text}`}
              />
            )
          })}
        </div>
      )}
    </Card>
  )
}
