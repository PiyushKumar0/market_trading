/**
 * Every published signal scored in hindsight (`signals` of `GET /scorecard`): at the rule's own entry
 * with its kill rule, and at the next open after the analyst's call. Same classes as Scorecard.
 */
import { strategyName } from '../../copy/glossary.ts'
import { humanize } from '../../lib/format.ts'
import { DASH, pctOrDash, ppOrDash, progressText, rateOrDash, verdictTone } from '../../lib/signals.ts'
import type { SignalCellLook, SignalKillLook, SignalsBlock, SignalVerdict } from '../../types'
import { Labelled } from '../../ui/Hint'
import { Pill } from '../../ui/Pill'
import { Th, tone } from './Scorecard'

function Verdict({ v }: { v: SignalVerdict<SignalKillLook | SignalCellLook> }) {
  return (
    <>
      <Pill tone={verdictTone(v.verdict)} title={v.reason}>
        {humanize(v.verdict)}
      </Pill>
      <span className="cell-sub">{progressText(v.looks)}</span>
    </>
  )
}

/** `ret`: a return, coloured by its sign as in Scorecard. */
function Num({ label, ret, children }: { label: string; ret?: number | null; children: string | number }) {
  return (
    <td data-label={label} className={`right num${tone(ret ?? null)}`}>
      {children}
    </td>
  )
}

export function SignalScorecard({ signals }: { signals: SignalsBlock | { error: string } }) {
  if ('error' in signals) {
    return <p className="perf-error">The signal scores couldn't be computed: {signals.error}</p>
  }
  return (
    <>
      <p className="perf-note">
        Analyst verdict: <Verdict v={signals.analyst} />
        {signals.analyst.excluded ? (
          <span className="cell-sub">{signals.analyst.excluded} excluded: another prompt or model</span>
        ) : null}
      </p>
      <div className="table-scroll" role="region" aria-label="Signals and the analyst" tabIndex={0}>
        <table className="dt perf-table sig-table">
          <caption className="sr-only">Per-strategy published signals scored in hindsight, and the analyst's calls</caption>
          <thead>
            <tr>
              <th colSpan={2} />
              <th colSpan={5} scope="colgroup" className="perf-group">
                At the rule's own entry
              </th>
              <th colSpan={4} scope="colgroup" className="perf-group">
                At the next open after the analyst's call
              </th>
            </tr>
            <tr>
              <th scope="col" className="perf-colhead">
                Strategy
              </th>
              <Th hint="Signals published from the registered start.">Published</Th>
              <Th hint="Held to the close of the 20th session, after costs.">Closed</Th>
              <Th hint="Share of closed ones with a positive net return.">Win rate</Th>
              <Th hint="The middle net return of the closed ones, after costs.">Median return</Th>
              <Th hint="The middle of each closed signal's net return minus an equal-weight basket over the days it was held.">
                vs market
              </Th>
              <Th hint="The pre-registered rule that demotes the strategy. Its first look is the first 20 filled signals.">
                Kill rule
              </Th>
              <Th hint="Signals the analyst approved.">Approved</Th>
              <Th hint="Signals the analyst declined.">Declined</Th>
              <Th hint="Days with at least one approved and one declined signal: the units the verdict averages.">Cells</Th>
              <Th hint="Average over cells of the approved signals' net return minus the declined ones', in percentage points.">
                Approved − declined
              </Th>
            </tr>
          </thead>
          <tbody>
            {Object.entries(signals.strategies).flatMap(([sid, s]) => {
              const label = strategyName(sid)
              const sub = [label.label !== sid ? sid : '', s.rule.diagnostic ? 'diagnostic: its forward test decides' : '']
                .filter(Boolean)
                .join(' · ')
              const rows = [
                <tr key={sid}>
                  <td data-label="Strategy" className="perf-strategy">
                    <Labelled t={label} />
                    {sub ? <span className="cell-sub">{sub}</span> : null}
                  </td>
                  <Num label="Published">{s.published}</Num>
                  <Num label="Closed at T+20">{s.rule.closed}</Num>
                  <Num label="Win rate">{rateOrDash(s.rule.hit_rate)}</Num>
                  <Num label="Median return" ret={s.rule.median_net_t20}>
                    {pctOrDash(s.rule.median_net_t20)}
                  </Num>
                  <Num label="vs market basket" ret={s.rule.median_excess_t20}>
                    {pctOrDash(s.rule.median_excess_t20)}
                  </Num>
                  <td data-label="Kill rule">{s.kill_rule ? <Verdict v={s.kill_rule} /> : DASH}</td>
                  <Num label="Analyst approved">{s.analyst.enter}</Num>
                  <Num label="Analyst declined">{s.analyst.decline}</Num>
                  <Num label="Cells">{s.analyst.cells}</Num>
                  <Num label="Approved − declined" ret={s.analyst.mean_diff}>
                    {ppOrDash(s.analyst.mean_diff)}
                  </Num>
                </tr>,
              ]
              const b = s.blocked
              if (b) {
                rows.push(
                  <tr key={`${sid}-blocked`}>
                    <td data-label="Strategy" className="perf-strategy">
                      {label.label}, blocked by the regime filter
                      <span className="cell-sub">
                        diagnostic{b.neither ? ` · ${b.neither} more blocked for another reason` : ''}
                      </span>
                    </td>
                    <Num label="Published">{DASH}</Num>
                    <Num label="Closed at T+20">{b.n}</Num>
                    <Num label="Win rate">{rateOrDash(b.hit_rate)}</Num>
                    <Num label="Median return" ret={b.median}>
                      {pctOrDash(b.median)}
                    </Num>
                    <Num label="vs market basket">{DASH}</Num>
                    <td data-label="Kill rule">{DASH}</td>
                    <Num label="Analyst approved">{DASH}</Num>
                    <Num label="Analyst declined">{DASH}</Num>
                    <Num label="Cells">{DASH}</Num>
                    <Num label="Approved − declined">{DASH}</Num>
                  </tr>,
                )
              }
              return rows
            })}
          </tbody>
        </table>
      </div>
    </>
  )
}
