import { memo, useMemo, useState } from 'react'
import { ChevronDown, ChevronRight, AlertTriangle, Zap } from 'lucide-react'
import type { StormHotspot, StormSummary } from '../../types/storms'

const PRIORITY_STYLES: Record<string, string> = {
  CRITICAL: 'border-red-500/60 bg-red-950/40 text-red-300',
  HIGH:     'border-orange-500/50 bg-orange-950/30 text-orange-300',
  MODERATE: 'border-yellow-500/40 bg-yellow-950/20 text-yellow-300',
  LOW:      'border-white/15 bg-white/5 text-white/55',
  MINIMAL:  'border-white/10 bg-transparent text-white/35',
}

const PRIORITY_DOT: Record<string, string> = {
  CRITICAL: 'bg-red-400',
  HIGH:     'bg-orange-400',
  MODERATE: 'bg-yellow-400',
  LOW:      'bg-white/40',
  MINIMAL:  'bg-white/20',
}

const THREAT_COLORS: Record<string, string> = {
  tornado: 'text-red-400',
  hail:    'text-sky-400',
  wind:    'text-orange-400',
  flood:   'text-emerald-400',
}

function HeadingArrow({ heading }: { heading: number | null }) {
  if (heading === null) return <span className="text-white/30">—</span>
  return (
    <span
      className="inline-block text-white/55"
      style={{ transform: `rotate(${heading}deg)`, display: 'inline-block' }}
      aria-label={`${heading}°`}
    >
      ↑
    </span>
  )
}

export const HotspotsPanel = memo(function HotspotsPanel({
  hotspots,
  selectedStormId,
  onSelectStorm,
}: {
  hotspots: StormHotspot[]
  selectedStormId: string | null
  onSelectStorm: (stormId: string) => void
}) {
  const [open, setOpen] = useState(true)

  const sorted = useMemo(
    () => [...hotspots].sort((a, b) => b.priority_score - a.priority_score),
    [hotspots]
  )

  if (!sorted.length) return null

  return (
    <section className="space-y-3">
      <button
        type="button"
        onClick={() => setOpen(v => !v)}
        className="flex w-full items-center justify-between text-left"
      >
        <span className="flex items-center gap-1.5 text-xs uppercase tracking-[0.2em] text-white/55">
          <Zap className="h-3 w-3 text-yellow-400/70" />
          Priority Hotspots
        </span>
        <span className="flex items-center gap-2 font-mono text-[11px] text-white/45">
          {sorted.length}
          {open ? <ChevronDown className="h-4 w-4" /> : <ChevronRight className="h-4 w-4" />}
        </span>
      </button>

      {open && (
        <div className="space-y-1.5">
          {sorted.map((storm, rank) => {
            const style = PRIORITY_STYLES[storm.priority_label] ?? PRIORITY_STYLES.MINIMAL
            const dot = PRIORITY_DOT[storm.priority_label] ?? PRIORITY_DOT.MINIMAL
            const isSelected = storm.storm_id === selectedStormId
            const topScore = Math.max(...Object.values(storm.threat_scores))
            return (
              <button
                key={storm.storm_id}
                type="button"
                onClick={() => onSelectStorm(storm.storm_id)}
                className={`w-full rounded border px-2.5 py-2 text-left transition-colors ${style} ${
                  isSelected ? 'ring-1 ring-cyan/40' : 'hover:brightness-110'
                }`}
              >
                <div className="flex items-center gap-2">
                  {/* Rank + dot */}
                  <span className="flex h-5 w-5 shrink-0 items-center justify-center rounded-full bg-white/10 font-mono text-[10px] text-white/50">
                    {rank + 1}
                  </span>
                  <span className={`h-2 w-2 shrink-0 rounded-full ${dot}`} />
                  {/* Mode / threat */}
                  <span className={`font-mono text-[11px] ${THREAT_COLORS[storm.primary_threat] ?? 'text-white/55'}`}>
                    {storm.primary_threat}
                  </span>
                  <span className="font-mono text-[10px] text-white/35">
                    {storm.storm_mode !== 'unknown' ? storm.storm_mode.replace(/_/g, ' ') : ''}
                  </span>
                  <span className="ml-auto font-mono text-[10px] text-white/40">
                    {Math.round(topScore * 100)}%
                  </span>
                </div>

                <div className="mt-1 flex items-center gap-2">
                  {/* Motion */}
                  <span className="font-mono text-[10px] text-white/35">
                    <HeadingArrow heading={storm.motion_heading_deg} />
                    {storm.motion_speed_kmh != null ? ` ${Math.round(storm.motion_speed_kmh)} km/h` : ' —'}
                  </span>
                  {/* Severity */}
                  <span className="ml-auto rounded bg-white/8 px-1 font-mono text-[9px] text-white/40">
                    {storm.severity_level}
                  </span>
                  {/* Confidence */}
                  <span className="font-mono text-[9px] text-white/30">
                    {Math.round(storm.confidence * 100)}% conf
                  </span>
                </div>

                {/* Top flag */}
                {storm.top_flag && (
                  <div className="mt-1 flex items-center gap-1">
                    <AlertTriangle className="h-2.5 w-2.5 shrink-0 text-yellow-400/60" />
                    <span className="text-[10px] text-white/45">{storm.top_flag}</span>
                    {storm.impact_count > 0 && (
                      <span className="ml-auto text-[10px] text-cyan/60">
                        {storm.impact_count} location{storm.impact_count !== 1 ? 's' : ''}
                      </span>
                    )}
                  </div>
                )}
              </button>
            )
          })}
        </div>
      )}
    </section>
  )
})
