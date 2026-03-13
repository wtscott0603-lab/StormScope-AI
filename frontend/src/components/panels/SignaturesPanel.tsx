import { ChevronDown, ChevronRight } from 'lucide-react'
import { useMemo, useState } from 'react'

import { cn } from '../../lib/cn'
import type { SignatureMarker } from '../../types/radar'

const SEVERITY_ORDER: Record<string, number> = {
  TORNADO_EMERGENCY: 5,
  TORNADO: 4,
  SEVERE: 3,
  MARGINAL: 2,
  NONE: 1,
}

const SEVERITY_STYLES: Record<string, string> = {
  TORNADO_EMERGENCY: 'border-fuchsia-500/40 bg-fuchsia-500/10 text-fuchsia-100',
  TORNADO: 'border-red-500/40 bg-red-500/10 text-red-100',
  SEVERE: 'border-orange-400/40 bg-orange-400/10 text-orange-100',
  MARGINAL: 'border-yellow-300/30 bg-yellow-300/10 text-yellow-100',
  NONE: 'border-white/10 bg-white/5 text-white/70',
}

const TYPE_SHORT: Record<string, string> = {
  TVS: 'TVS',
  TDS: 'TDS',
  ROTATION: 'ROT',
  HAIL_CORE: 'HAIL',
  HAIL_LARGE: 'LG H',
  BOW_ECHO: 'BOW',
  BWER: 'BWER',
}

function severityDotClass(severity: string) {
  if (severity === 'TORNADO_EMERGENCY') {
    return 'bg-fuchsia-400'
  }
  if (severity === 'TORNADO') {
    return 'bg-red-400'
  }
  if (severity === 'SEVERE') {
    return 'bg-orange-400'
  }
  if (severity === 'MARGINAL') {
    return 'bg-yellow-300'
  }
  return 'bg-white/30'
}

export function SignaturesPanel({ signatures }: { signatures: SignatureMarker[] }) {
  const [open, setOpen] = useState(true)

  const ordered = useMemo(
    () =>
      [...signatures].sort((left, right) => {
        const severityDelta =
          (SEVERITY_ORDER[right.severity] ?? 0) - (SEVERITY_ORDER[left.severity] ?? 0)
        if (severityDelta !== 0) {
          return severityDelta
        }
        return right.confidence - left.confidence
      }),
    [signatures],
  )

  return (
    <section className="space-y-3">
      <button
        type="button"
        onClick={() => setOpen((value) => !value)}
        className="flex w-full items-center justify-between text-left"
      >
        <span className="text-xs uppercase tracking-[0.2em] text-white/55">Signatures</span>
        <span className="flex items-center gap-2 font-mono text-[11px] text-white/45">
          {ordered.length}
          {open ? <ChevronDown className="h-4 w-4" /> : <ChevronRight className="h-4 w-4" />}
        </span>
      </button>

      {open ? (
        <div className="max-h-72 space-y-2 overflow-y-auto pr-1">
          {ordered.length ? (
            ordered.map((signature) => (
              <div
                key={`${signature.frame_id}-${signature.analyzer}-${signature.signature_type}-${signature.lat}-${signature.lon}`}
                className={cn(
                  'rounded-md border px-3 py-3',
                  SEVERITY_STYLES[signature.severity] ?? SEVERITY_STYLES.NONE,
                )}
                title={signature.description}
              >
                <div className="flex items-start justify-between gap-3">
                  <div className="min-w-0 flex-1">
                    <div className="flex items-center gap-2">
                      <span className={cn('h-2.5 w-2.5 rounded-full', severityDotClass(signature.severity))} />
                      <span className="font-mono text-[11px] uppercase tracking-[0.18em] text-white/50">
                        {TYPE_SHORT[signature.signature_type] ?? signature.signature_type}
                      </span>
                      <span className="truncate font-medium text-white">{signature.label}</span>
                    </div>
                    <div className="mt-2 h-1.5 overflow-hidden rounded-full bg-white/10">
                      <div
                        className="h-full rounded-full bg-cyan"
                        style={{ width: `${Math.max(6, Math.round(signature.confidence * 100))}%` }}
                      />
                    </div>
                    <div className="mt-2 truncate text-xs text-white/72">{signature.description}</div>
                  </div>
                  <div className="shrink-0 text-right font-mono text-[10px] text-white/48">
                    <div>{signature.confidence.toFixed(2)}</div>
                    <div>{signature.analyzer.toUpperCase()}</div>
                  </div>
                </div>
                <div className="mt-2 font-mono text-[10px] text-white/42">
                  {signature.lat.toFixed(3)}, {signature.lon.toFixed(3)}
                </div>
              </div>
            ))
          ) : (
            <div className="rounded-md border border-white/10 bg-white/5 px-3 py-4 text-sm text-white/40">
              No signatures detected.
            </div>
          )}
        </div>
      ) : null}
    </section>
  )
}
