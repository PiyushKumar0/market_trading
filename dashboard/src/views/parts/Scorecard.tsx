/**
 * The strategy scorecard table. DataTable has one header row, so this builds the two-level header
 * (group row, then columns) by hand with the same classes. Recommendation columns are hindsight
 * outcomes on official prices; the paper columns are simulated trades.
 */
import { strategyName } from '../../copy/glossary.ts'
import { humanize, pct, ratePct, pnlWithPct, signOf } from '../../lib/format.ts'
import type { ScorecardRecs, ScorecardResponse } from '../../types'
import { Hint, Labelled } from '../../ui/Hint'

const ACTION_WORD: Record<string, string> = { taken: 'took', dismissed: 'skipped', expired: 'expired', open: 'no reply' }

const sentence = (s: string) => (s ? s[0].toUpperCase() + s.slice(1) : s)

/** "Took 3, skipped 4, expired 5, no reply 1" in the engine's order; unknown keys are humanised. */
function actionsText(actions: Record<string, number>): string | null {
  const parts = Object.entries(actions).map(([k, n]) => `${ACTION_WORD[k] ?? humanize(k).toLowerCase()} ${n}`)
  return parts.length ? sentence(parts.join(', ')) : null
}

function skipText(reasons: Record<string, number>): string | null {
  const parts = Object.entries(reasons).map(([k, n]) => `${humanize(k).toLowerCase()} (${n})`)
  return parts.length ? `Why you skipped: ${parts.join(', ')}` : null
}

/** pos / neg class from a rounded display value; zero and missing stay uncoloured. */
export function tone(v: number | null, digits = 2): string {
  if (v === null || !Number.isFinite(v)) return ''
  const s = signOf(v.toFixed(digits))
  return s === 1 ? ' pos' : s === -1 ? ' neg' : ''
}

/** A column header whose word is its own explanation (dotted underline): no ⓘ eating the width. */
export function Th({ children, hint, term }: { children: string; hint: string; term?: string }) {
  return (
    <th scope="col" className="perf-colhead">
      <Hint label={children} term={term} asTerm>
        {hint}
      </Hint>
    </th>
  )
}

const dash = (v: string | null) => v ?? '—'

export function Scorecard({ data }: { data: ScorecardResponse }) {
  const rows = Object.entries(data.strategies)
  const bench = Object.values(data.bench)
  return (
    <div className="table-scroll" role="region" aria-label="Strategy performance" tabIndex={0}>
      <table className="dt perf-table">
        <caption className="sr-only">
          Per-strategy results: recommendations scored in hindsight, and paper trades
        </caption>
        <thead>
          <tr>
            <th />
            <th colSpan={9} scope="colgroup" className="perf-group">
              Recommendations, scored in hindsight
            </th>
            <th colSpan={4} scope="colgroup" className="perf-group">
              Paper trades
            </th>
          </tr>
          <tr>
            <th scope="col" className="perf-colhead">
              Strategy
            </th>
            <Th hint="Entry recommendations delivered.">Sent</Th>
            <Th hint="Would have filled at the suggested entry on official prices.">Filled</Th>
            <Th hint="Reached their exit: stop-loss, target or date.">Closed</Th>
            <Th hint="Share of closed ones with a positive net return.">Win rate</Th>
            <Th hint="The middle net return per closed trade, after costs.">Median return</Th>
            <Th hint="Average net return per closed trade, after costs.">Average return</Th>
            {/* rec_outcomes.py scores T+20 with stop=None, target=None; the excess is net minus the
                equal-weight basket over the days held (benchmark.py). */}
            <Th hint="Average net return if each filled recommendation had simply been held for 20 sessions (the entry day counts as 1), ignoring its stop-loss and target.">
              20-session return
            </Th>
            <Th
              hint="Net return minus an equal-weight basket of the stocks the engine can trade, over the days each trade was held (the entry day is left out)."
              term={bench.length ? bench.join('; ') : undefined}
            >
              vs market
            </Th>
            <th scope="col" className="perf-colhead">
              Your actions
            </th>
            <Th hint="Closed paper trades across every paper reset; voided trades are left out.">Closed</Th>
            <th scope="col" className="perf-colhead">
              Win rate
            </th>
            <Th hint="Total net profit or loss of those paper trades.">Net P&amp;L</Th>
            <Th hint="Paper positions open right now.">Open now</Th>
          </tr>
        </thead>
        <tbody>
          {rows.map(([sid, { recs, paper }]) => {
            const label = strategyName(sid)
            const acts = actionsText(recs.actions)
            const why = skipText(recs.skip_reasons)
            return (
              <tr key={sid}>
                <td data-label="Strategy" className="perf-strategy">
                  <Labelled t={label} />
                  {label.label !== sid ? <span className="cell-sub">{sid}</span> : null}
                </td>
                <td data-label="Sent" className="right num">
                  {recs.n}
                </td>
                <td data-label="Filled" className="right num">
                  {recs.filled}
                </td>
                <td data-label="Closed" className="right num">
                  {recs.closed}
                </td>
                <td data-label="Win rate" className="right num">
                  {dash(ratePct(recs.hit_rate))}
                </td>
                <RetCell label="Median return" v={recs.median_net} />
                <RetCell label="Average return" v={recs.mean_net} />
                {/* Phones drop the header row and its hints: these labels carry the meaning alone. */}
                <RetCell label="If held 20 sessions" v={recs.net_t20} />
                <RetCell label="vs market basket" v={recs.mean_excess} />
                <td data-label="Your actions" className="perf-actions">
                  {why && acts ? (
                    <Hint label={acts} asTerm>
                      {why}
                    </Hint>
                  ) : (
                    dash(acts)
                  )}
                </td>
                <td data-label="Paper closed (all resets)" className="right num">
                  {paper.closed}
                </td>
                <td data-label="Paper win rate" className="right num">
                  {dash(ratePct(paper.hit_rate))}
                </td>
                <td data-label="Paper net P&L (after costs)" className={`right num${tone(paper.net)}`}>
                  {paper.net === null ? '—' : dash(pnlWithPct(paper.net.toFixed(2), paper.net_pct == null ? null : paper.net_pct.toFixed(2)))}
                </td>
                <td data-label="Paper open now" className="right num">
                  {paper.open}
                </td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

function RetCell({ label, v }: { label: string; v: ScorecardRecs['mean_net'] }) {
  return (
    <td data-label={label} className={`right num${tone(v)}`}>
      {dash(pct(v))}
    </td>
  )
}
