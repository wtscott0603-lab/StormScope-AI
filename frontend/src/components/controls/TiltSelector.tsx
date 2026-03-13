import { useEffect, useState } from 'react'

import { fetchTilts } from '../../api/radar'
import { cn } from '../../lib/cn'

export function TiltSelector({
  site,
  product,
  value,
  onChange,
}: {
  site: string
  product: string
  value: number
  onChange: (tilt: number) => void
}) {
  const [tilts, setTilts] = useState<number[]>([0.5])

  useEffect(() => {
    let active = true
    void fetchTilts(site, product)
      .then((payload) => {
        if (active) {
          const nextTilts = payload.tilts.length ? payload.tilts : [0.5]
          setTilts(nextTilts)
          if (!nextTilts.some((tilt) => Math.abs(tilt - value) < 0.11)) {
            onChange(nextTilts[0])
          }
        }
      })
      .catch(() => {
        if (active) {
          setTilts([0.5])
          if (Math.abs(value - 0.5) >= 0.11) {
            onChange(0.5)
          }
        }
      })
    return () => {
      active = false
    }
  }, [onChange, product, site, value])

  return (
    <div className="space-y-3">
      <label className="text-xs uppercase tracking-[0.2em] text-white/55">Tilt</label>
      <div className="grid grid-cols-3 gap-2">
        {tilts.map((tilt) => (
          <button
            key={tilt}
            type="button"
            onClick={() => onChange(tilt)}
            className={cn(
              'rounded-md border px-3 py-2 font-mono text-sm transition-colors',
              Math.abs(value - tilt) < 0.11
                ? 'border-cyan bg-cyan/10 text-cyan'
                : 'border-white/10 bg-white/5 text-white/80 hover:border-white/30',
            )}
          >
            {tilt.toFixed(1)}°
          </button>
        ))}
      </div>
      <div className="text-xs text-white/45">
        Multi-tilt rendering is enabled for configured sweeps. Storm-object and signature defaults still favor the lowest tilt.
      </div>
    </div>
  )
}
