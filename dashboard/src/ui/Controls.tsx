/** Owner controls: an on/off switch and a segmented choice. */
import type { ReactNode } from 'react'

export function Switch({
  checked,
  label,
  shortLabel,
  stateText,
  disabled,
  busy,
  onChange,
  describedBy,
}: {
  checked: boolean
  label: string
  /** Shown instead of `label` under 640 px. */
  shortLabel?: string
  /** Defaults to On / Off; e.g. "On (not running)". */
  stateText?: string
  disabled?: boolean
  busy?: boolean
  onChange: (next: boolean) => void
  describedBy?: string
}) {
  return (
    <button
      type="button"
      role="switch"
      aria-checked={checked}
      aria-busy={busy || undefined}
      aria-describedby={describedBy}
      className="switch"
      disabled={disabled || busy}
      onClick={() => onChange(!checked)}
    >
      <span className="switch-track" aria-hidden="true">
        <span className="switch-thumb" />
      </span>
      {shortLabel ? (
        <span className="switch-label">
          <span className="lbl-long">{label}</span>
          <span className="lbl-short">{shortLabel}</span>
        </span>
      ) : (
        <span className="switch-label">{label}</span>
      )}
      <span className="switch-state">{stateText ?? (checked ? 'On' : 'Off')}</span>
    </button>
  )
}

/** A choice between a few options, as a group of pressed/unpressed buttons. Picking a value is an
 *  explicit click or Enter/Space — arrow keys never change the engine's mode by accident. */
export function Segmented<V extends string>({
  label,
  value,
  options,
  disabled,
  onChange,
  extra,
}: {
  label: string
  value: V | null
  options: { value: V; label: string }[]
  disabled?: boolean
  onChange: (v: V) => void
  extra?: ReactNode
}) {
  return (
    <div className="seg" role="group" aria-label={label}>
      {options.map((o) => (
        <button
          key={o.value}
          type="button"
          className="seg-btn"
          aria-pressed={value === o.value}
          disabled={disabled}
          onClick={() => {
            if (value !== o.value) onChange(o.value)
          }}
        >
          {o.label}
        </button>
      ))}
      {extra}
    </div>
  )
}
