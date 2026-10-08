/**
 * One risk-check reason: the rule in plain words, then the engine's own figures exactly as it wrote
 * them (gate.py: "<rule_id>: <value> (limit <limit>)"). The rule's engine name stays in the tooltip.
 */
import { reasonTerm } from '../../copy/glossary.ts'
import { Labelled } from '../../ui/Hint'

export function ReasonText({ reason }: { reason: string }) {
  const r = reasonTerm(reason)
  return (
    <>
      <Labelled t={r} />
      {r.detail ? <span className="reason-detail">{r.detail}</span> : null}
    </>
  )
}
