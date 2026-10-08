/** First visit, or after signing out: ask for the engine's dashboard token (kept in localStorage). */
import { useState } from 'react'
import { COPY } from '../copy/glossary.ts'
import { Hint } from '../ui/Hint'
import { BrandMark } from './AppBar'

export function TokenGate({ onSet }: { onSet: (token: string) => void }) {
  const [value, setValue] = useState('')
  return (
    <main className="gate">
      <div className="card card-primary gate-card">
        <div className="brand gate-brand">
          <BrandMark size={32} />
          <span className="brand-name">Market Trading</span>
        </div>
        <h1 className="gate-title">{COPY.gateTitle}</h1>
        <p className="gate-body">
          {COPY.gateBody}
          <Hint label="Dashboard token">{COPY.gateHint}</Hint>
        </p>
        <form
          className="gate-form"
          onSubmit={(e) => {
            e.preventDefault()
            onSet(value.trim())
          }}
        >
          <label className="field">
            <span className="field-label">Dashboard token</span>
            <input
              className="input"
              type="password"
              autoComplete="off"
              spellCheck={false}
              value={value}
              onChange={(e) => setValue(e.target.value)}
            />
          </label>
          <button type="submit" className="btn btn-primary" disabled={!value.trim()}>
            Connect
          </button>
        </form>
      </div>
    </main>
  )
}
