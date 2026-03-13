import { memo, useEffect, useState } from 'react'
import { AlertTriangle, Clock, ChevronDown, ChevronRight } from 'lucide-react'
import { fetchStormEventHistory, fetchStormPrecomputedSummary } from '../../api/storms'
import type {
  StormEventHistoryPoint,
  StormEventHistoryResponse,
  StormPrecomputedSummary,
} from '../../types/storms'

const PRIORITY_COLOR: Record<string, string> = {
  CRITICAL: '#d94f4f',
  HIGH:     '#d98c2a',
  MODERATE: '#f6f67b',
  LOW:      '#5b9bd5',
  MINIMAL:  '#5c636e',
}

const THREAT_COLOR: Record<string, string> = {
  tornado: '#d94f4f',
  hail:    '#5b9bd5',
  wind:    '#d98c2a',
  flood:   '#4caf76',
}

function formatTime(iso: string): string {
  try {
    const d = new Date(iso)
    return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', hour12: false })
  } catch {
    return iso.slice(11, 16)
  }
}

function ThreatBar({ scores }: { scores: Record<string, number> }) {
  const threats = ['tornado', 'hail', 'wind', 'flood']
  const maxScore = Math.max(...Object.values(scores), 0.01)
  return (
    <div className="flex h-3 w-full gap-px overflow-hidden rounded-sm">
      {threats.map((t) => {
        const v = scores[t] ?? 0
        return (
          <div
            key={t}
            className="h-full transition-all"
            style={{
              width: `${(v / maxScore) * 25}%`,
              backgroundColor: THREAT_COLOR[t] ?? '#5c636e',
              opacity: v > 0.05 ? 0.8 : 0.15,
              minWidth: v > 0.05 ? 2 : 0,
            }}
            title={`${t}: ${Math.round(v * 100)}%`}
          />
        )
      })}
    </div>
  )
}

function HistoryRow({ point }: { point: StormEventHistoryPoint }) {
  const dotColor = PRIORITY_COLOR[point.priority_label ?? 'MINIMAL'] ?? PRIORITY_COLOR.MINIMAL
  const topFlag = point.event_flags[0]
  return (
    <div className="flex items-start gap-2 py-1 border-b border-white/5 last:border-0">
      {/* Time + dot */}
      <div className="flex shrink-0 flex-col items-center gap-1 pt-0.5">
        <span className="font-mono text-[9px] text-white/35">{formatTime(point.scan_time)}</span>
        <span
          className="h-2 w-2 rounded-full"
          style={{ backgroundColor: dotColor }}
        />
      </div>

      {/* Content */}
      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-1.5 flex-wrap">
          {point.severity_level && point.severity_level !== 'NONE' && (
            <span className="rounded bg-white/8 px-1 font-mono text-[9px] text-white/50">
              {point.severity_level}
            </span>
          )}
          {point.primary_threat && (
            <span
              className="font-mono text-[10px]"
              style={{ color: THREAT_COLOR[point.primary_threat] ?? '#e4e6ea' }}
            >
              {point.primary_threat}
            </span>
          )}
          {point.storm_mode && point.storm_mode !== 'unknown' && (
            <span className="font-mono text-[9px] text-white/25">
              {point.storm_mode.replace(/_/g, ' ')}
            </span>
          )}
          {point.confidence != null && (
            <span className="ml-auto font-mono text-[9px] text-white/25">
              {Math.round(point.confidence * 100)}%
            </span>
          )}
        </div>

        {/* Threat bar */}
        {Object.keys(point.threat_scores).length > 0 && (
          <div className="mt-1">
            <ThreatBar scores={point.threat_scores} />
          </div>
        )}

        {/* Top flag */}
        {topFlag && (
          <div className="mt-0.5 flex items-center gap-1">
            <AlertTriangle className="h-2 w-2 shrink-0 text-yellow-400/50" />
            <span className="text-[9px] text-white/35 truncate">{topFlag.label}</span>
            {point.event_flags.length > 1 && (
              <span className="text-[9px] text-white/20">+{point.event_flags.length - 1}</span>
            )}
          </div>
        )}
      </div>
    </div>
  )
}

function SummaryBlock({ summary }: { summary: StormPrecomputedSummary }) {
  return (
    <div className="rounded border border-white/10 bg-white/3 p-3 space-y-2">
      {summary.summary_narrative && (
        <div className="text-[11px] text-white/55 leading-snug">{summary.summary_narrative}</div>
      )}

      <div className="grid grid-cols-2 gap-x-3 gap-y-1 font-mono text-[10px]">
        <div className="text-white/35">Scans tracked</div>
        <div className="text-white/65">{summary.scan_count}</div>
        {summary.peak_severity && (
          <>
            <div className="text-white/35">Peak severity</div>
            <div className="text-white/65">{summary.peak_severity}</div>
          </>
        )}
        {summary.dominant_mode && (
          <>
            <div className="text-white/35">Dominant mode</div>
            <div className="text-white/65">{summary.dominant_mode.replace(/_/g, ' ')}</div>
          </>
        )}
        {summary.max_priority_score != null && (
          <>
            <div className="text-white/35">Peak priority</div>
            <div className="text-white/65">{Math.round(summary.max_priority_score * 100)}%</div>
          </>
        )}
        {summary.peak_reflectivity != null && (
          <>
            <div className="text-white/35">Peak dBZ</div>
            <div className="text-white/65">{summary.peak_reflectivity.toFixed(1)}</div>
          </>
        )}
        {summary.max_speed_kmh != null && (
          <>
            <div className="text-white/35">Max speed</div>
            <div className="text-white/65">{Math.round(summary.max_speed_kmh)} km/h</div>
          </>
        )}
      </div>

      {/* Flag occurrence summary */}
      {summary.flag_summary.length > 0 && (
        <div className="space-y-1">
          <div className="text-[9px] uppercase tracking-[0.18em] text-white/25">Recurring flags</div>
          <div className="flex flex-wrap gap-1">
            {summary.flag_summary.slice(0, 5).map((f) => (
              <span
                key={f.flag}
                className="flex items-center gap-0.5 rounded bg-yellow-500/10 px-1.5 py-0.5 text-[9px] text-yellow-300/60"
              >
                {f.label}
                <span className="text-yellow-300/35">×{f.occurrence_count}</span>
              </span>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}

export const EventHistoryPanel = memo(function EventHistoryPanel({
  stormId,
}: {
  stormId: string | null
}) {
  const [open, setOpen] = useState(true)
  const [history, setHistory] = useState<StormEventHistoryResponse | null>(null)
  const [summary, setSummary] = useState<StormPrecomputedSummary | null>(null)
  const [loading, setLoading] = useState(false)

  useEffect(() => {
    if (!stormId) {
      setHistory(null)
      setSummary(null)
      return
    }
    let cancelled = false
    setLoading(true)
    Promise.all([
      fetchStormEventHistory(stormId, 30).catch(() => null),
      fetchStormPrecomputedSummary(stormId).catch(() => null),
    ]).then(([h, s]) => {
      if (!cancelled) {
        setHistory(h)
        setSummary(s)
        setLoading(false)
      }
    })
    return () => { cancelled = true }
  }, [stormId])

  if (!stormId) return null
  if (loading) return (
    <div className="text-[10px] text-white/25 py-2">Loading history…</div>
  )
  if (!history && !summary) return null

  return (
    <section className="space-y-3">
      <button
        type="button"
        onClick={() => setOpen(v => !v)}
        className="flex w-full items-center justify-between text-left"
      >
        <span className="flex items-center gap-1.5 text-xs uppercase tracking-[0.2em] text-white/55">
          <Clock className="h-3 w-3 text-cyan/50" />
          Storm History
        </span>
        <span className="flex items-center gap-1.5 font-mono text-[11px] text-white/35">
          {history ? `${history.point_count} scans` : ''}
          {open ? <ChevronDown className="h-4 w-4" /> : <ChevronRight className="h-4 w-4" />}
        </span>
      </button>

      {open && (
        <div className="space-y-3">
          {/* Precomputed summary block */}
          {summary && <SummaryBlock summary={summary} />}

          {/* Per-scan timeline */}
          {history && history.points.length > 0 && (
            <div className="space-y-0">
              <div className="mb-1 text-[9px] uppercase tracking-[0.18em] text-white/25">
                Per-scan timeline (newest first)
              </div>
              <div className="max-h-64 overflow-y-auto pr-1 space-y-0 scrollbar-thin">
                {history.points.map((pt) => (
                  <HistoryRow key={pt.scan_time} point={pt} />
                ))}
              </div>
              <div className="mt-1 text-[9px] text-white/15 leading-snug">
                History built server-side — continues accumulating with no frontend open.
                All flags are proxy-derived heuristics.
              </div>
            </div>
          )}
        </div>
      )}
    </section>
  )
})
