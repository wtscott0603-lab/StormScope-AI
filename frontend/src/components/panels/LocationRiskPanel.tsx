import { memo, useState } from 'react'
import { ChevronDown, ChevronRight, MapPin, AlertTriangle } from 'lucide-react'
import type { LocationRiskEntry } from '../../types/storms'

const RISK_STYLES: Record<string, { badge: string; bar: string; border: string }> = {
  HIGH:     { badge: 'bg-red-500/20 text-red-300 border-red-500/40', bar: 'bg-red-400', border: 'border-red-500/30' },
  MODERATE: { badge: 'bg-yellow-500/20 text-yellow-300 border-yellow-500/40', bar: 'bg-yellow-400', border: 'border-yellow-500/20' },
  LOW:      { badge: 'bg-white/10 text-white/55 border-white/15', bar: 'bg-white/35', border: 'border-white/10' },
  NONE:     { badge: 'bg-white/5 text-white/30 border-white/8', bar: 'bg-white/15', border: 'border-white/8' },
}

const THREAT_COLORS: Record<string, string> = {
  tornado: 'text-red-400',
  hail:    'text-sky-400',
  wind:    'text-orange-400',
  flood:   'text-emerald-400',
}

function formatEta(low: number | null, high: number | null): string {
  if (low === 0 || (low !== null && low === 0)) return 'Now / Passing'
  if (low !== null && high !== null) return `${low}–${high} min`
  if (low !== null) return `~${low} min`
  if (high !== null) return `≤${high} min`
  return 'watch'
}

export const LocationRiskPanel = memo(function LocationRiskPanel({
  entries,
  onSelectStorm,
}: {
  entries: LocationRiskEntry[]
  onSelectStorm?: (stormId: string) => void
}) {
  const [open, setOpen] = useState(true)

  const active = entries.filter(e => e.risk_level !== 'NONE')
  const inactive = entries.filter(e => e.risk_level === 'NONE')

  if (!entries.length) return null

  return (
    <section className="space-y-3">
      <button
        type="button"
        onClick={() => setOpen(v => !v)}
        className="flex w-full items-center justify-between text-left"
      >
        <span className="flex items-center gap-1.5 text-xs uppercase tracking-[0.2em] text-white/55">
          <MapPin className="h-3 w-3 text-cyan/60" />
          Location Risk
        </span>
        <span className="flex items-center gap-2 font-mono text-[11px] text-white/45">
          {active.length > 0 && (
            <span className="rounded bg-red-500/20 px-1.5 font-mono text-[10px] text-red-300">
              {active.length} threatened
            </span>
          )}
          {open ? <ChevronDown className="h-4 w-4" /> : <ChevronRight className="h-4 w-4" />}
        </span>
      </button>

      {open && (
        <div className="space-y-1.5">
          {[...active, ...inactive].map((entry) => {
            const styles = RISK_STYLES[entry.risk_level] ?? RISK_STYLES.NONE
            return (
              <div
                key={entry.location_id}
                className={`rounded border px-2.5 py-2 ${styles.border}`}
              >
                <div className="flex items-center justify-between gap-2">
                  <span className="text-sm font-medium text-white/85">{entry.location_name}</span>
                  <span className={`rounded border px-1.5 py-0.5 font-mono text-[9px] ${styles.badge}`}>
                    {entry.risk_level}
                  </span>
                </div>

                {entry.risk_level !== 'NONE' && (
                  <>
                    {/* Risk score bar */}
                    <div className="mt-1.5 h-1 w-full rounded-full bg-white/8">
                      <div
                        className={`h-1 rounded-full transition-all ${styles.bar}`}
                        style={{ width: `${Math.round(entry.risk_score * 100)}%` }}
                      />
                    </div>

                    <div className="mt-1.5 flex flex-wrap gap-x-3 gap-y-0.5 font-mono text-[10px]">
                      {/* Primary threat */}
                      {entry.primary_threat && (
                        <span className={THREAT_COLORS[entry.primary_threat] ?? 'text-white/55'}>
                          {entry.primary_threat}
                        </span>
                      )}
                      {/* ETA */}
                      {(entry.nearest_eta_low !== null || entry.nearest_eta_high !== null) && (
                        <span className="text-cyan/70">
                          ETA {formatEta(entry.nearest_eta_low, entry.nearest_eta_high)}
                        </span>
                      )}
                      {/* Storm count */}
                      {entry.threatening_storm_count > 0 && (
                        <span className="text-white/35">
                          {entry.threatening_storm_count} storm{entry.threatening_storm_count !== 1 ? 's' : ''}
                        </span>
                      )}
                      {/* Confidence */}
                      {entry.confidence != null && (
                        <span className="text-white/25">
                          {Math.round(entry.confidence * 100)}% conf
                        </span>
                      )}
                    </div>

                    {/* Top impact summary */}
                    {entry.top_impact_summary && (
                      <div className="mt-1 text-[10px] text-white/40 line-clamp-2">
                        {entry.top_impact_summary}
                      </div>
                    )}

                    {/* Event flag labels */}
                    {entry.event_flag_labels.length > 0 && (
                      <div className="mt-1 flex flex-wrap gap-1">
                        {entry.event_flag_labels.map((label) => (
                          <span
                            key={label}
                            className="flex items-center gap-0.5 rounded bg-yellow-500/10 px-1 py-0.5 text-[9px] text-yellow-300/70"
                          >
                            <AlertTriangle className="h-2 w-2" />
                            {label}
                          </span>
                        ))}
                      </div>
                    )}

                    {/* Jump to storm */}
                    {entry.top_storm_id && onSelectStorm && (
                      <button
                        type="button"
                        onClick={() => onSelectStorm(entry.top_storm_id!)}
                        className="mt-1.5 text-[10px] text-cyan/50 hover:text-cyan/80 transition-colors"
                      >
                        → focus storm
                      </button>
                    )}
                  </>
                )}

                {entry.risk_level === 'NONE' && (
                  <div className="mt-0.5 text-[10px] text-white/25">No active storm threats</div>
                )}
              </div>
            )
          })}
        </div>
      )}
    </section>
  )
})
