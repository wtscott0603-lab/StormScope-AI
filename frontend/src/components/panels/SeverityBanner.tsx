import { useEffect, useMemo, useState } from 'react'

import type { SeverityLevel, SignatureMarker } from '../../types/radar'

const BANNER_STYLES: Record<'TORNADO_EMERGENCY' | 'TORNADO', { bg: string; border: string; text: string; label: string }> =
  {
    TORNADO_EMERGENCY: {
      bg: 'bg-purple-950/95',
      border: 'border-purple-500/60',
      text: 'text-purple-100',
      label: 'TORNADO EMERGENCY — DEBRIS / TVS SIGNATURE ACTIVE',
    },
    TORNADO: {
      bg: 'bg-red-950/95',
      border: 'border-red-500/60',
      text: 'text-red-200',
      label: 'TORNADO SIGNATURE DETECTED',
    },
  }

export function SeverityBanner({
  maxSeverity,
  signatures,
}: {
  maxSeverity: SeverityLevel
  signatures: SignatureMarker[]
}) {
  const [dismissed, setDismissed] = useState(false)

  useEffect(() => {
    setDismissed(false)
  }, [maxSeverity])

  const topSignatures = useMemo(() => signatures.slice(0, 3), [signatures])

  if (maxSeverity !== 'TORNADO' && maxSeverity !== 'TORNADO_EMERGENCY') {
    return null
  }

  if (dismissed) {
    return null
  }

  const style = BANNER_STYLES[maxSeverity]

  return (
    <div
      className={[
        'pointer-events-auto fixed left-0 right-0 top-0 z-50 border-b px-4 py-2 shadow-lg backdrop-blur-md',
        style.bg,
        style.border,
      ].join(' ')}
    >
      <div className="mx-auto flex max-w-screen-2xl items-center justify-between gap-4">
        <div className={`font-mono text-[11px] font-bold uppercase tracking-[0.24em] ${style.text}`}>
          {style.label}
        </div>
        <div className="hidden gap-3 text-[10px] text-white/75 lg:flex">
          {topSignatures.map((signature) => (
            <span key={`${signature.frame_id}-${signature.analyzer}-${signature.label}`} className="font-mono">
              {signature.label} @ {signature.lat.toFixed(2)}, {signature.lon.toFixed(2)}
            </span>
          ))}
        </div>
        <button
          type="button"
          onClick={() => setDismissed(true)}
          className="font-mono text-[10px] uppercase tracking-[0.2em] text-white/65 transition hover:text-white"
        >
          Dismiss
        </button>
      </div>
    </div>
  )
}
