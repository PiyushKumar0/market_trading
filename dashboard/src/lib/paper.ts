/** The paper autopilot's state in one plain sentence (Overview snapshot and the Paper tab). Pure. */
import { haltTerm, modeTerm, riskTerm, rungTerm } from '../copy/glossary.ts'
import type { PaperSummary } from '../types.ts'

export interface PaperLine {
  tone: 'good' | 'warn' | 'neutral'
  text: string
}

const cap = (s: string) => (s ? s[0].toUpperCase() + s.slice(1) : s)
const lower = (s: string) => (s ? s[0].toLowerCase() + s.slice(1) : s)

/** The engine's guard refusal (oms/manager.py, ops/paper_runtime.py, ops/paper_summary.py) in words;
 *  an unknown refusal is shown as sent. */
export function entryGuardText(raw: string): string {
  if (raw === 'paper is off') return 'the autopilot is off'
  if (raw === 'not built') return 'the autopilot is not running'
  if (raw === 'paper session prep has not completed') return "today's paper setup hasn't finished yet"
  // Prep starts on the first in-session tick: while none has arrived, the live feed is the thing to check.
  if (raw === 'paper session prep is waiting for an in-session tick') {
    return "today's paper setup starts on the first live market tick, and none has arrived yet"
  }
  const failed = /^paper construction failed \((.*)\)$/.exec(raw)
  if (failed) return `paper failed to start (${failed[1]}) and stays off until the engine restarts`
  // paper_risk.py: the paper guard uses the WORSE of the engine's own risk state and the paper halts.
  // Every caller shows paper halts first, so when this text is reached the cause is the engine's state.
  const m = /^effective paper state is (?:RiskState\.)?(\w+)$/.exec(raw)
  if (m) return `the engine's own risk state (${riskTerm(m[1]).label.toLowerCase()}) also stops paper entries`
  return raw
}

export function paperStatus(p: PaperSummary, mode: string | null): PaperLine {
  if (!p.built && !p.subsystem_enabled) {
    return { tone: 'neutral', text: 'Not set up: paper trading is switched off in the engine settings.' }
  }
  if (!p.built) return { tone: 'warn', text: 'Failed to start: nothing is simulated until the engine restarts.' }
  if (!p.enabled) {
    return { tone: 'neutral', text: 'Off: not taking new paper trades. Open paper positions keep their exits.' }
  }
  if (mode === null) return { tone: 'neutral', text: "On, waiting for the engine's mode." }
  if (mode !== 'RECOMMEND' && mode !== 'AUTO') {
    return {
      tone: 'neutral',
      text: `On but idle: it follows the Recommend pipeline, and the engine is ${modeTerm(mode).label}.`,
    }
  }
  if (p.halts.length) {
    const h = p.halts[0]
    const more = p.halts.length > 1 ? ` (and ${p.halts.length - 1} more)` : ''
    return { tone: 'warn', text: `${cap(rungTerm(h.rung).label)}: ${lower(haltTerm(h.cause).label)}${more}.` }
  }
  if (p.entry_guard) {
    return { tone: 'warn', text: `On, but new paper trades are blocked: ${entryGuardText(p.entry_guard)}.` }
  }
  return { tone: 'good', text: 'On: taking new paper trades.' }
}
