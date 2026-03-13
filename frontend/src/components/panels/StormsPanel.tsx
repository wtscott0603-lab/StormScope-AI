import { AlertTriangle, ChevronDown, ChevronRight } from 'lucide-react'
import { useMemo, useState } from 'react'

import { cn } from '../../lib/cn'
import { useStormTimeseries } from '../../hooks/useStormTimeseries'
import { EventHistoryPanel } from './EventHistoryPanel'
import type { StormSummary } from '../../types/storms'

// ── Inline SVG sparkline ──────────────────────────────────────────────────────
function Sparkline({
  values,
  width = 120,
  height = 28,
  color = '#5b9bd5',
}: {
  values: number[]
  width?: number
  height?: number
  color?: string
}) {
  if (values.length < 2) return null
  const min = Math.min(...values)
  const max = Math.max(...values)
  const range = max - min || 1
  const pts = values.map((v, i) => {
    const x = (i / (values.length - 1)) * width
    const y = height - ((v - min) / range) * (height - 4) - 2
    return `${x.toFixed(1)},${y.toFixed(1)}`
  })
  return (
    <svg
      width={width}
      height={height}
      viewBox={`0 0 ${width} ${height}`}
      fill="none"
      aria-hidden
    >
      <polyline
        points={pts.join(' ')}
        stroke={color}
        strokeWidth="1.5"
        strokeLinejoin="round"
        strokeLinecap="round"
      />
      {/* Latest value dot */}
      <circle cx={pts[pts.length - 1].split(',')[0]} cy={pts[pts.length - 1].split(',')[1]} r="2.5" fill={color} />
    </svg>
  )
}

function threatTone(threat: string) {
  if (threat === 'tornado') {
    return 'border-red-600/50 bg-red-950/40 text-red-300'
  }
  if (threat === 'hail') {
    return 'border-sky-600/40 bg-sky-950/40 text-sky-300'
  }
  if (threat === 'wind') {
    return 'border-orange-600/40 bg-orange-950/40 text-orange-300'
  }
  if (threat === 'flood') {
    return 'border-emerald-600/40 bg-emerald-950/40 text-emerald-300'
  }
  return 'border-panelBorder bg-panelHover text-textSecondary'
}

function hasNumber(value: number | null | undefined): value is number {
  return value !== null && value !== undefined
}

function formatEtaWindow(low: number | null, high: number | null) {
  if (hasNumber(low) && hasNumber(high)) {
    return `${low}-${high}m`
  }
  if (hasNumber(low)) {
    return `${low}m`
  }
  if (hasNumber(high)) {
    return `${high}m`
  }
  return 'watch'
}

function asRecord(value: unknown): Record<string, unknown> | null {
  return value && typeof value === 'object' ? (value as Record<string, unknown>) : null
}

function formatProvenanceLabel(value: string) {
  return value
    .replace(/_/g, ' ')
    .replace(/\b\w/g, (letter) => letter.toUpperCase())
}

function ReducedHodograph({ hodograph }: { hodograph: Record<string, unknown> | undefined }) {
  const points = Array.isArray(hodograph?.points)
    ? hodograph.points.filter((point): point is Record<string, unknown> => typeof point === 'object' && point !== null)
    : []
  if (!points.length) {
    return null
  }

  const stormMotion = asRecord(hodograph?.storm_motion)
  const vectors = [
    ...points.map((point) => ({
      label: String(point.label ?? ''),
      u: Number(point.u_kt ?? 0),
      v: Number(point.v_kt ?? 0),
    })),
    ...(stormMotion
      ? [
          {
            label: String(stormMotion.label ?? 'Storm Motion'),
            u: Number(stormMotion.u_kt ?? 0),
            v: Number(stormMotion.v_kt ?? 0),
            isStormMotion: true,
          },
        ]
      : []),
  ]
  const maxAbs = Math.max(25, ...vectors.flatMap((vector) => [Math.abs(vector.u), Math.abs(vector.v)]))
  const size = 132
  const center = size / 2
  const scale = (size * 0.36) / maxAbs
  const path = points
    .map((point) => {
      const x = center + Number(point.u_kt ?? 0) * scale
      const y = center - Number(point.v_kt ?? 0) * scale
      return `${x},${y}`
    })
    .join(' ')
  const lowLevelPolygon =
    points.length >= 2
      ? [
          `${center},${center}`,
          `${center + Number(points[0].u_kt ?? 0) * scale},${center - Number(points[0].v_kt ?? 0) * scale}`,
          `${center + Number(points[1].u_kt ?? 0) * scale},${center - Number(points[1].v_kt ?? 0) * scale}`,
        ].join(' ')
      : null

  return (
    <div className="rounded border border-white/10 bg-black/30 p-3">
      <div className="mb-2 font-mono text-[10px] uppercase tracking-[0.16em] text-white/40">
        Reduced Hodograph
      </div>
      <svg viewBox={`0 0 ${size} ${size}`} className="h-36 w-full rounded bg-[#081014]">
        <line x1={center} y1={8} x2={center} y2={size - 8} stroke="rgba(148,163,184,0.22)" strokeWidth="1" />
        <line x1={8} y1={center} x2={size - 8} y2={center} stroke="rgba(148,163,184,0.22)" strokeWidth="1" />
        <circle cx={center} cy={center} r={size * 0.18} fill="none" stroke="rgba(148,163,184,0.12)" strokeWidth="1" />
        <circle cx={center} cy={center} r={size * 0.30} fill="none" stroke="rgba(148,163,184,0.12)" strokeWidth="1" />
        {lowLevelPolygon ? <polygon points={lowLevelPolygon} fill="rgba(34,211,238,0.12)" stroke="rgba(34,211,238,0.22)" strokeWidth="1" /> : null}
        <polyline points={path} fill="none" stroke="#67e8f9" strokeWidth="2" />
        {points.map((point) => {
          const x = center + Number(point.u_kt ?? 0) * scale
          const y = center - Number(point.v_kt ?? 0) * scale
          return (
            <g key={String(point.label ?? `${x}-${y}`)}>
              <circle cx={x} cy={y} r="3" fill="#22d3ee" />
              <text x={x + 4} y={y - 4} fontSize="8" fill="#dbeafe">
                {String(point.label ?? '')}
              </text>
            </g>
          )
        })}
        {stormMotion ? (
          <g>
            <circle
              cx={center + Number(stormMotion.u_kt ?? 0) * scale}
              cy={center - Number(stormMotion.v_kt ?? 0) * scale}
              r="3"
              fill="#f97316"
            />
            <text
              x={center + Number(stormMotion.u_kt ?? 0) * scale + 4}
              y={center - Number(stormMotion.v_kt ?? 0) * scale - 4}
              fontSize="8"
              fill="#fed7aa"
            >
              SM
            </text>
          </g>
        ) : null}
      </svg>
      <div className="mt-2 text-[10px] text-white/45">
        Reduced 4-level hodograph using surface, 925 hPa, 850 hPa, and 500 hPa winds. The shaded low-level wedge is a quick curvature cue, not a full SRH integration.
      </div>
    </div>
  )
}

function ReducedProfile({ profile }: { profile: Record<string, unknown> | undefined }) {
  const levels = Array.isArray(profile?.levels)
    ? profile.levels.filter((level): level is Record<string, unknown> => typeof level === 'object' && level !== null)
    : []
  if (!levels.length) {
    return null
  }

  return (
    <div className="rounded border border-white/10 bg-black/30 p-3">
      <div className="mb-2 font-mono text-[10px] uppercase tracking-[0.16em] text-white/40">
        Reduced Profile
      </div>
      <div className="space-y-1">
        {levels.map((level, index) => (
          <div
            key={String(level.label ?? level.pressure_hpa ?? index)}
            className="grid grid-cols-[3rem_3.5rem_4rem_4rem_4.5rem] gap-2 rounded border border-white/10 bg-black/25 px-2 py-1 font-mono text-[10px] text-white/60"
          >
            <div className="text-white/75">{String(level.label ?? '--')}</div>
            <div>{Number(level.temperature_c ?? NaN).toString() !== 'NaN' ? `${Number(level.temperature_c).toFixed(0)}C` : '--'}</div>
            <div>{Number(level.dewpoint_c ?? NaN).toString() !== 'NaN' ? `${Number(level.dewpoint_c).toFixed(0)}C Td` : '--'}</div>
            <div>{Number(level.wind_speed_kmh ?? NaN).toString() !== 'NaN' ? `${Number(level.wind_speed_kmh).toFixed(0)} km/h` : '--'}</div>
            <div>{Number(level.wind_direction_deg ?? NaN).toString() !== 'NaN' ? `${Number(level.wind_direction_deg).toFixed(0)} deg` : '--'}</div>
          </div>
        ))}
      </div>
      {profile?.limitation ? (
        <div className="mt-2 text-[10px] text-white/45">{String(profile.limitation)}</div>
      ) : null}
    </div>
  )
}

export function StormsPanel({
  storms,
  selectedStormId,
  onSelectStorm,
}: {
  storms: StormSummary[]
  selectedStormId: string | null
  onSelectStorm: (stormId: string) => void
}) {
  const [open, setOpen] = useState(true)
  const selectedStorm = useMemo(
    () => storms.find((storm) => storm.storm_id === selectedStormId) ?? storms[0] ?? null,
    [selectedStormId, storms],
  )
  const { data: timeseries } = useStormTimeseries(selectedStorm?.storm_id ?? null, 20)

  return (
    <section className="space-y-3">
      <button type="button" onClick={() => setOpen((value) => !value)} className="flex w-full items-center justify-between text-left">
        <span className="text-xs uppercase tracking-[0.2em] text-white/55">Storm Intelligence</span>
        <span className="flex items-center gap-2 font-mono text-[11px] text-white/45">
          {storms.length}
          {open ? <ChevronDown className="h-4 w-4" /> : <ChevronRight className="h-4 w-4" />}
        </span>
      </button>

      {open ? (
        <>
          <div className="max-h-56 space-y-2 overflow-y-auto pr-1">
            {storms.length ? (
              storms.map((storm) => (
                <button
                  key={storm.storm_id}
                  type="button"
                  onClick={() => onSelectStorm(storm.storm_id)}
                  className={cn(
                    'w-full rounded-md border px-3 py-3 text-left',
                    storm.storm_id === selectedStormId ? 'border-cyan/50 bg-cyan/10' : 'border-white/10 bg-white/5',
                  )}
                >
                  <div className="flex items-start justify-between gap-3">
                    <div className="min-w-0">
                      <div className="flex items-center gap-1.5">
                        <div className="font-medium text-white">{storm.primary_threat.toUpperCase()}</div>
                        {/* Priority badge */}
                        {storm.priority_label && storm.priority_label !== 'MINIMAL' && (
                          <span className={cn(
                            'rounded px-1 font-mono text-[9px]',
                            storm.priority_label === 'CRITICAL' ? 'bg-red-500/25 text-red-300' :
                            storm.priority_label === 'HIGH'     ? 'bg-orange-500/20 text-orange-300' :
                            storm.priority_label === 'MODERATE' ? 'bg-yellow-500/15 text-yellow-300' :
                            'bg-white/10 text-white/40'
                          )}>
                            {storm.priority_label}
                          </span>
                        )}
                      </div>
                      <div className="font-mono text-[10px] text-white/45">{storm.storm_id}</div>
                    </div>
                    <span className={cn('rounded border px-2 py-1 font-mono text-[10px] uppercase tracking-[0.16em]', threatTone(storm.primary_threat))}>
                      {storm.severity_level.split('_').join(' ')}
                    </span>
                  </div>
                  <div className="mt-2 flex items-center gap-3 font-mono text-[11px] text-white/65">
                    <span>{storm.max_reflectivity.toFixed(0)} dBZ</span>
                    <span>{hasNumber(storm.motion_speed_kmh) ? `${storm.motion_speed_kmh.toFixed(0)} km/h` : 'motion pending'}</span>
                    <span>{storm.trend}</span>
                    {/* Storm mode chip */}
                    {storm.storm_mode && storm.storm_mode !== 'unknown' && (
                      <span className="ml-auto text-[9px] text-white/30">{storm.storm_mode.replace(/_/g, ' ')}</span>
                    )}
                  </div>
                  {/* Top event flag */}
                  {storm.event_flags && storm.event_flags.length > 0 && (
                    <div className="mt-1 flex items-center gap-1">
                      <AlertTriangle className="h-2.5 w-2.5 shrink-0 text-yellow-400/60" />
                      <span className="text-[10px] text-white/40">{storm.event_flags[0].label}</span>
                    </div>
                  )}
                </button>
              ))
            ) : (
              <div className="rounded-md border border-white/10 bg-white/5 px-3 py-4 text-sm text-white/40">
                No tracked storms for this site yet.
              </div>
            )}
          </div>

          {selectedStorm ? (
            <div className="space-y-3 rounded-md border border-white/10 bg-white/5 p-4">
              <div className="flex items-center justify-between gap-3">
                <div>
                  <div className="font-semibold text-white">{selectedStorm.primary_threat.toUpperCase()} storm</div>
                  <div className="font-mono text-[10px] text-white/45">{selectedStorm.storm_id}</div>
                </div>
                <div className="font-mono text-[11px] text-cyan">
                  conf {Math.round(selectedStorm.confidence * 100)}%
                </div>
              </div>

              <p className="text-sm leading-6 text-white/78">{selectedStorm.narrative}</p>

              {selectedStorm.prediction_summary ? (
                <div className="rounded border border-cyan/20 bg-cyan/10 p-3">
                  <div className="text-xs uppercase tracking-[0.2em] text-white/45">Projected Trend</div>
                  <div className="mt-1 flex items-center justify-between gap-3">
                    <div className="font-medium text-cyan">
                      {selectedStorm.prediction_summary.projected_trend ?? 'steady'}
                    </div>
                    <div className="font-mono text-[11px] text-white/60">
                      projected {selectedStorm.prediction_summary.projected_primary_threat ?? selectedStorm.primary_threat}
                      {selectedStorm.prediction_summary.projected_confidence !== null &&
                      selectedStorm.prediction_summary.projected_confidence !== undefined
                        ? ` | conf ${Math.round((selectedStorm.prediction_summary.projected_confidence ?? 0) * 100)}%`
                        : ''}
                    </div>
                  </div>
                  <div className="mt-2 grid grid-cols-3 gap-2 font-mono text-[11px] text-white/60">
                    <div className="rounded border border-white/10 bg-black/30 px-2 py-2">
                      <div className="text-white/40">Intensify</div>
                      <div>{Math.round((selectedStorm.prediction_summary.intensification_score ?? 0) * 100)}%</div>
                    </div>
                    <div className="rounded border border-white/10 bg-black/30 px-2 py-2">
                      <div className="text-white/40">Maintain</div>
                      <div>{Math.round((selectedStorm.prediction_summary.maintenance_score ?? 0) * 100)}%</div>
                    </div>
                    <div className="rounded border border-white/10 bg-black/30 px-2 py-2">
                      <div className="text-white/40">Weaken</div>
                      <div>{Math.round((selectedStorm.prediction_summary.weakening_score ?? 0) * 100)}%</div>
                    </div>
                  </div>
                  <div className="mt-2 grid grid-cols-3 gap-2 font-mono text-[11px] text-white/60">
                    <div className="rounded border border-white/10 bg-black/30 px-2 py-2">
                      <div className="text-white/40">Motion</div>
                      <div>{Math.round((selectedStorm.prediction_summary.motion_confidence ?? 0) * 100)}%</div>
                    </div>
                    <div className="rounded border border-white/10 bg-black/30 px-2 py-2">
                      <div className="text-white/40">Persist</div>
                      <div>{Math.round((selectedStorm.prediction_summary.persistence_score ?? 0) * 100)}%</div>
                    </div>
                    <div className="rounded border border-white/10 bg-black/30 px-2 py-2">
                      <div className="text-white/40">Stability</div>
                      <div>{Math.round((selectedStorm.prediction_summary.forecast_stability_score ?? 0) * 100)}%</div>
                    </div>
                  </div>
                  {selectedStorm.prediction_summary.uncertainty_factors?.length ? (
                    <div className="mt-2 space-y-1">
                      <div className="font-mono text-[10px] uppercase tracking-[0.16em] text-white/40">Uncertainty</div>
                      {selectedStorm.prediction_summary.uncertainty_factors.map((factor) => (
                        <div key={factor} className="rounded border border-white/10 bg-black/25 px-2 py-1 text-[11px] text-white/55">
                          {factor}
                        </div>
                      ))}
                    </div>
                  ) : null}
                  {selectedStorm.prediction_summary.forecast_reasoning_factors?.length ? (
                    <div className="mt-2 space-y-1">
                      <div className="font-mono text-[10px] uppercase tracking-[0.16em] text-white/40">Forecast Why</div>
                      {selectedStorm.prediction_summary.forecast_reasoning_factors.slice(0, 5).map((factor) => (
                        <div key={factor} className="rounded border border-white/10 bg-black/25 px-2 py-1 text-[11px] text-white/55">
                          {factor}
                        </div>
                      ))}
                    </div>
                  ) : null}
                </div>
              ) : null}

              <div className="grid grid-cols-2 gap-2 font-mono text-[11px] text-white/65">
                <div className="rounded border border-white/10 bg-black/30 px-2 py-2">
                  <div className="text-white/40">Motion</div>
                  <div>{hasNumber(selectedStorm.motion_heading_deg) ? `${selectedStorm.motion_heading_deg.toFixed(0)} deg` : 'pending'}</div>
                  <div>{hasNumber(selectedStorm.motion_speed_kmh) ? `${selectedStorm.motion_speed_kmh.toFixed(0)} km/h` : 'pending'}</div>
                </div>
                <div className="rounded border border-white/10 bg-black/30 px-2 py-2">
                  <div className="text-white/40">Intensity</div>
                  <div>{selectedStorm.max_reflectivity.toFixed(0)} / {selectedStorm.mean_reflectivity.toFixed(0)} dBZ</div>
                  <div>{selectedStorm.area_km2.toFixed(0)} km2</div>
                </div>
              </div>

              <div className="space-y-2">
                <div className="text-xs uppercase tracking-[0.2em] text-white/45">Explain Why</div>
                <div className="space-y-1 text-sm text-white/72">
                  {selectedStorm.reasoning_factors.map((factor) => (
                    <div key={factor} className="rounded border border-white/10 bg-black/25 px-2 py-1">
                      {factor}
                    </div>
                  ))}
                </div>
              </div>

              <div className="space-y-2">
                <div className="text-xs uppercase tracking-[0.2em] text-white/45">Environment</div>
                <div className="rounded border border-white/10 bg-black/25 p-3 text-sm text-white/72">
                  {selectedStorm.environment_summary ? (
                    <>
                      <div className="font-mono text-[11px] text-cyan">
                        {(selectedStorm.environment_summary.gridpoint_id ?? selectedStorm.environment_summary.current_station_id ?? 'ENV').toUpperCase()} | hail {Math.round((selectedStorm.environment_summary.hail_favorability ?? 0) * 100)}% | wind {Math.round((selectedStorm.environment_summary.wind_favorability ?? 0) * 100)}% | tor {Math.round((selectedStorm.environment_summary.tornado_favorability ?? 0) * 100)}%
                      </div>
                      <div className="mt-2 grid grid-cols-2 gap-2 font-mono text-[11px] text-white/60">
                        <div className="rounded border border-white/10 bg-black/30 px-2 py-2">
                          <div className="text-white/40">Thunder Now/Ahead</div>
                          <div>
                            {selectedStorm.environment_summary.forecast_probability_of_thunder ?? 0}%
                            {' / '}
                            {selectedStorm.environment_summary.ahead_probability_of_thunder ?? 0}%
                          </div>
                        </div>
                        <div className="rounded border border-white/10 bg-black/30 px-2 py-2">
                          <div className="text-white/40">QPF / Flow</div>
                          <div>{selectedStorm.environment_summary.forecast_qpf_mm ?? 0} mm</div>
                          <div>{selectedStorm.environment_summary.forecast_wind_speed_kmh ?? 0} km/h</div>
                        </div>
                        <div className="rounded border border-white/10 bg-black/30 px-2 py-2">
                          <div className="text-white/40">Model Severe</div>
                          <div>{selectedStorm.environment_summary.cape_jkg ?? 0} CAPE</div>
                          <div>{selectedStorm.environment_summary.bulk_shear_06km_kt ?? 0} kt shear</div>
                        </div>
                        <div className="rounded border border-white/10 bg-black/30 px-2 py-2">
                          <div className="text-white/40">Low-Level</div>
                          <div>{selectedStorm.environment_summary.srh_surface_925hpa_m2s2 ?? 0} Helicity Proxy</div>
                          <div>{selectedStorm.environment_summary.lcl_m ?? 0} m LCL</div>
                        </div>
                        <div className="rounded border border-white/10 bg-black/30 px-2 py-2">
                          <div className="text-white/40">Downdraft</div>
                          <div>{selectedStorm.environment_summary.dcape_jkg ?? 0} J/kg</div>
                          <div>{selectedStorm.environment_summary.dcape_is_proxy ? 'proxy estimate' : 'direct field'}</div>
                        </div>
                      </div>
                      <ReducedHodograph hodograph={asRecord(selectedStorm.environment_summary.hodograph) ?? undefined} />
                      <ReducedProfile profile={asRecord(selectedStorm.environment_summary.profile_summary) ?? undefined} />
                      {selectedStorm.environment_summary.field_provenance &&
                      Object.keys(selectedStorm.environment_summary.field_provenance).length ? (
                        <div className="mt-2 rounded border border-white/10 bg-black/30 p-3">
                          <div className="mb-2 font-mono text-[10px] uppercase tracking-[0.16em] text-white/40">
                            Field Provenance
                          </div>
                          <div className="grid grid-cols-2 gap-2 font-mono text-[10px] text-white/55">
                            {Object.entries(selectedStorm.environment_summary.field_provenance).map(([field, provenance]) => (
                              <div key={field} className="rounded border border-white/10 bg-black/25 px-2 py-1">
                                <div className="text-white/35">{field.replace(/_/g, ' ')}</div>
                                <div>{formatProvenanceLabel(provenance)}</div>
                              </div>
                            ))}
                          </div>
                        </div>
                      ) : null}
                      <div className="mt-2">{selectedStorm.environment_summary.ahead_trend ?? selectedStorm.environment_summary.limitation}</div>
                      <div className="mt-2 flex flex-wrap gap-2 font-mono text-[11px] text-white/55">
                        {selectedStorm.environment_summary.environment_confidence !== undefined &&
                        selectedStorm.environment_summary.environment_confidence !== null ? (
                          <span className="rounded border border-white/10 bg-black/30 px-2 py-1">
                            env conf {Math.round(selectedStorm.environment_summary.environment_confidence * 100)}%
                          </span>
                        ) : null}
                        {selectedStorm.environment_summary.environment_freshness_minutes !== undefined &&
                        selectedStorm.environment_summary.environment_freshness_minutes !== null ? (
                          <span className="rounded border border-white/10 bg-black/30 px-2 py-1">
                            env age {selectedStorm.environment_summary.environment_freshness_minutes}m
                          </span>
                        ) : null}
                      </div>
                      {selectedStorm.environment_summary.projected_trend ? (
                        <div className="mt-2 rounded border border-cyan/20 bg-cyan/10 px-2 py-1 font-mono text-[11px] text-cyan">
                          projected {selectedStorm.environment_summary.projected_trend}
                          {selectedStorm.environment_summary.projection_confidence !== undefined &&
                          selectedStorm.environment_summary.projection_confidence !== null
                            ? ` | conf ${Math.round(selectedStorm.environment_summary.projection_confidence * 100)}%`
                            : ''}
                        </div>
                      ) : null}
                      {selectedStorm.environment_summary.weather_summary ? (
                        <div className="mt-2 text-xs text-white/50">
                          Forecast weather: {selectedStorm.environment_summary.weather_summary}
                        </div>
                      ) : null}
                      {selectedStorm.environment_summary.limitation ? (
                        <div className="mt-2 text-xs text-white/45">{selectedStorm.environment_summary.limitation}</div>
                      ) : null}
                      {selectedStorm.environment_summary.source_notes?.length ? (
                        <div className="mt-2 space-y-1">
                          <div className="font-mono text-[10px] uppercase tracking-[0.16em] text-white/40">Source Notes</div>
                          {selectedStorm.environment_summary.source_notes.map((note) => (
                            <div key={note} className="rounded border border-white/10 bg-black/25 px-2 py-1 text-[11px] text-white/50">
                              {note}
                            </div>
                          ))}
                        </div>
                      ) : null}
                      {selectedStorm.environment_summary.operational_context ? (
                        <div className="mt-2 grid grid-cols-4 gap-2 font-mono text-[11px] text-white/55">
                          <div className="rounded border border-white/10 bg-black/30 px-2 py-2">
                            <div className="text-white/35">SPC</div>
                            <div>
                              {String(
                                ((selectedStorm.environment_summary.operational_context as Record<string, unknown>).spc as Record<string, unknown> | undefined)
                                  ?.category ?? 'none',
                              )}
                            </div>
                          </div>
                          <div className="rounded border border-white/10 bg-black/30 px-2 py-2">
                            <div className="text-white/35">MD</div>
                            <div>
                              {Number(
                                (((selectedStorm.environment_summary.operational_context as Record<string, unknown>).md as Record<string, unknown> | undefined)
                                  ?.active_discussions ?? 0),
                              )}{' '}
                              active
                            </div>
                          </div>
                          <div className="rounded border border-white/10 bg-black/30 px-2 py-2">
                            <div className="text-white/35">LSR</div>
                            <div>
                              {Number(
                                (((selectedStorm.environment_summary.operational_context as Record<string, unknown>).lsr as Record<string, unknown> | undefined)
                                  ?.nearby_reports ?? 0),
                              )}{' '}
                              nearby
                            </div>
                          </div>
                          <div className="rounded border border-white/10 bg-black/30 px-2 py-2">
                            <div className="text-white/35">Watch</div>
                            <div>
                              {String(
                                (((selectedStorm.environment_summary.operational_context as Record<string, unknown>).watch as Record<string, unknown> | undefined)
                                  ?.watch_type ?? 'none'),
                              )}
                            </div>
                          </div>
                        </div>
                      ) : null}
                      {selectedStorm.environment_summary.srv_metrics ? (
                        <div className="mt-2 rounded border border-white/10 bg-black/30 px-2 py-2 font-mono text-[11px] text-white/60">
                          SRV {String((selectedStorm.environment_summary.srv_metrics as Record<string, unknown>).motion_source ?? 'storm-relative')} | dV{' '}
                          {String((selectedStorm.environment_summary.srv_metrics as Record<string, unknown>).delta_v_ms ?? '--')} m/s
                        </div>
                      ) : null}
                    </>
                  ) : (
                    'No environment snapshot available yet.'
                  )}
                </div>
              </div>

              {/* --- Threat Component Breakdown (v13) --- */}
              {selectedStorm.threat_component_breakdown && Object.keys(selectedStorm.threat_component_breakdown).length > 0 ? (
                <div className="space-y-2">
                  <div className="text-xs uppercase tracking-[0.2em] text-white/45">Threat Component Scores</div>
                  <div className="grid grid-cols-2 gap-2 font-mono text-[11px]">
                    {(['tornado', 'hail', 'wind', 'flood'] as const).map((threat) => {
                      const score = selectedStorm.threat_scores[threat] ?? 0
                      const components = (selectedStorm.threat_component_breakdown?.[threat] ?? {}) as Record<string, number>
                      const topReasons = (selectedStorm.threat_top_reasons?.[threat] ?? []) as string[]
                      const limiting = (selectedStorm.threat_limiting_factors?.[threat] ?? []) as string[]
                      const topComponents = Object.entries(components)
                        .filter(([, v]) => v > 0)
                        .sort(([, a], [, b]) => b - a)
                        .slice(0, 3)
                      return (
                        <div key={threat} className={`rounded border px-2 py-2 ${threat === selectedStorm.primary_threat ? 'border-cyan/40 bg-cyan/5' : 'border-white/10 bg-black/20'}`}>
                          <div className="flex items-center justify-between">
                            <span className="text-white/70 capitalize">{threat}</span>
                            <span className={score >= 0.55 ? 'text-red-400' : score >= 0.3 ? 'text-yellow-400' : 'text-white/45'}>
                              {Math.round(score * 100)}%
                            </span>
                          </div>
                          {/* Mini bar */}
                          <div className="mt-1 h-1 w-full rounded-full bg-white/10">
                            <div
                              className={`h-1 rounded-full ${score >= 0.55 ? 'bg-red-400' : score >= 0.3 ? 'bg-yellow-400' : 'bg-white/30'}`}
                              style={{ width: `${Math.min(100, Math.round(score * 100))}%` }}
                            />
                          </div>
                          {topComponents.length > 0 && (
                            <div className="mt-1.5 space-y-0.5 text-[10px] text-white/40">
                              {topComponents.map(([key, val]) => (
                                <div key={key} className="flex justify-between">
                                  <span>{key.replace(/_/g, ' ')}</span>
                                  <span>{Math.round(val * 100)}%</span>
                                </div>
                              ))}
                            </div>
                          )}
                          {limiting.length > 0 && (
                            <div className="mt-1 text-[10px] text-white/28">
                              ↓ missing: {limiting.slice(0, 2).map(k => k.replace(/_/g, ' ')).join(', ')}
                            </div>
                          )}
                        </div>
                      )
                    })}
                  </div>
                  <div className="text-[10px] text-white/25">
                    Scores are proxy-derived from radar + model data. Not official operational values.
                  </div>
                </div>
              ) : null}

              {/* --- Lifecycle Summary (v13) --- */}
              {selectedStorm.lifecycle_summary && Object.keys(selectedStorm.lifecycle_summary).length > 0 ? (
                <div className="space-y-2">
                  <div className="text-xs uppercase tracking-[0.2em] text-white/45">Lifecycle Analysis</div>
                  <div className="rounded border border-white/10 bg-black/20 px-3 py-2 space-y-1 font-mono text-[11px]">
                    <div className="flex justify-between">
                      <span className="text-white/45">Intensity trend</span>
                      <span className={`${
                        String(selectedStorm.lifecycle_summary.intensity_trend).includes('rapid') ? 'text-red-400' :
                        String(selectedStorm.lifecycle_summary.intensity_trend) === 'strengthening' ? 'text-yellow-400' :
                        String(selectedStorm.lifecycle_summary.intensity_trend).includes('weak') || String(selectedStorm.lifecycle_summary.intensity_trend).includes('decay') ? 'text-sky-400' :
                        'text-white/55'
                      }`}>
                        {String(selectedStorm.lifecycle_summary.intensity_trend ?? 'uncertain').replace(/_/g, ' ')}
                        {' '}
                        <span className="text-white/30">
                          ({Math.round(Number(selectedStorm.lifecycle_summary.intensity_confidence ?? 0) * 100)}%)
                        </span>
                      </span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-white/45">Motion trend</span>
                      <span className="text-white/65">
                        {String(selectedStorm.lifecycle_summary.motion_trend ?? 'unknown').replace(/_/g, ' ')}
                      </span>
                    </div>
                    {Array.isArray(selectedStorm.lifecycle_summary.intensity_evidence) &&
                     (selectedStorm.lifecycle_summary.intensity_evidence as string[]).slice(0, 2).map((ev: string) => (
                      <div key={ev} className="text-[10px] text-white/30">{ev}</div>
                    ))}
                  </div>
                </div>
              ) : null}

              {/* --- Event Flags (v14) --- */}
              {selectedStorm.event_flags && selectedStorm.event_flags.length > 0 ? (
                <div className="space-y-2">
                  <div className="text-xs uppercase tracking-[0.2em] text-white/45">Operational Flags</div>
                  <div className="space-y-1">
                    {selectedStorm.event_flags.slice(0, 6).map((flag) => (
                      <div
                        key={flag.flag}
                        className="rounded border border-white/10 bg-black/20 px-2.5 py-1.5"
                      >
                        <div className="flex items-center gap-2">
                          <AlertTriangle className={`h-3 w-3 shrink-0 ${
                            flag.severity >= 8 ? 'text-red-400' :
                            flag.severity >= 6 ? 'text-orange-400' :
                            flag.severity >= 4 ? 'text-yellow-400' :
                            'text-white/30'
                          }`} />
                          <span className="font-mono text-[11px] text-white/75">{flag.label}</span>
                          <span className="ml-auto font-mono text-[10px] text-white/30">
                            {Math.round(flag.confidence * 100)}%
                          </span>
                        </div>
                        <div className="mt-0.5 pl-5 text-[10px] text-white/35 leading-snug">
                          {flag.rationale}
                        </div>
                      </div>
                    ))}
                    <div className="text-[10px] text-white/20">
                      Flags are proxy-derived heuristics — not official operational products.
                    </div>
                  </div>
                </div>
              ) : null}

              {/* --- Storm History Sparklines (v13) --- */}
              {timeseries && timeseries.points.length >= 3 ? (() => {
                const pts = timeseries.points
                const refs = pts.map((p) => p.max_reflectivity)
                const areas = pts.map((p) => p.area_km2)
                const latest = pts[pts.length - 1]
                const oldest = pts[0]
                const deltaRef = latest.max_reflectivity - oldest.max_reflectivity
                const refColor = deltaRef >= 5 ? '#f87171' : deltaRef <= -5 ? '#60a5fa' : '#5b9bd5'
                return (
                  <div className="space-y-2">
                    <div className="text-xs uppercase tracking-[0.2em] text-white/45">History ({pts.length} scans)</div>
                    <div className="rounded border border-white/10 bg-black/20 px-3 py-2 grid grid-cols-2 gap-3">
                      <div className="space-y-1">
                        <div className="flex items-center justify-between">
                          <span className="font-mono text-[10px] text-white/40">Max dBZ</span>
                          <span className={`font-mono text-[10px] ${deltaRef >= 5 ? 'text-red-400' : deltaRef <= -5 ? 'text-sky-400' : 'text-white/55'}`}>
                            {deltaRef >= 0 ? '+' : ''}{deltaRef.toFixed(0)} dBZ
                          </span>
                        </div>
                        <Sparkline values={refs} color={refColor} />
                      </div>
                      <div className="space-y-1">
                        <div className="flex items-center justify-between">
                          <span className="font-mono text-[10px] text-white/40">Area km²</span>
                          <span className="font-mono text-[10px] text-white/55">
                            {latest.area_km2.toFixed(0)}
                          </span>
                        </div>
                        <Sparkline values={areas} color="#4caf76" />
                      </div>
                    </div>
                  </div>
                )
              })() : null}

              <div className="space-y-2">
                <div className="text-xs uppercase tracking-[0.2em] text-white/45">Location Impacts</div>
                {selectedStorm.impacts.length ? (
                  <div className="space-y-2">
                    {selectedStorm.impacts.map((impact) => (
                      <div key={`${selectedStorm.storm_id}-${impact.location_id}`} className="rounded border border-white/10 bg-black/25 px-3 py-2 text-sm text-white/72">
                        <div className="flex items-center justify-between gap-2">
                          <span className="font-medium text-white">{impact.location_name}</span>
                          <span className="font-mono text-[11px] text-cyan">
                            {formatEtaWindow(impact.eta_minutes_low, impact.eta_minutes_high)}
                          </span>
                        </div>
                        <div className="mt-1">{impact.summary}</div>
                        {impact.details ? (
                          <div className="mt-2 grid grid-cols-2 gap-2 font-mono text-[11px] text-white/55">
                            <div className="rounded border border-white/10 bg-black/30 px-2 py-1">
                              <div className="text-white/35">Projected</div>
                              <div>{String(impact.details.projected_primary_threat ?? impact.threat_at_arrival)}</div>
                              <div>{String(impact.details.projected_trend ?? impact.trend_at_arrival)}</div>
                              {Array.isArray(impact.details.projected_secondary_threats) && impact.details.projected_secondary_threats.length ? (
                                <div className="text-[10px] text-white/40">
                                  {impact.details.projected_secondary_threats.map((value) => String(value)).join(', ')}
                                </div>
                              ) : null}
                            </div>
                            <div className="rounded border border-white/10 bg-black/30 px-2 py-1">
                              <div className="text-white/35">Risks</div>
                              <div>hail {Math.round(Number(impact.details.projected_hail_risk ?? 0) * 100)}%</div>
                              <div>wind {Math.round(Number(impact.details.projected_wind_risk ?? 0) * 100)}%</div>
                            </div>
                            <div className="rounded border border-white/10 bg-black/30 px-2 py-1">
                              <div className="text-white/35">Arrival</div>
                              <div>tor {Math.round(Number(impact.details.projected_tornado_risk ?? 0) * 100)}%</div>
                              <div>rain {Math.round(Number(impact.details.projected_heavy_rain_risk ?? 0) * 100)}%</div>
                            </div>
                            <div className="rounded border border-white/10 bg-black/30 px-2 py-1">
                              <div className="text-white/35">Uncertainty</div>
                              <div>conf {Math.round(Number(impact.details.impact_confidence ?? impact.confidence) * 100)}%</div>
                              <div>eta +/- {Number(impact.details.eta_uncertainty_minutes ?? 0)}m</div>
                            </div>
                          </div>
                        ) : null}
                        {impact.details?.arrival_environment_summary ? (
                          <div className="mt-2 text-xs text-white/45">
                            Environment at arrival: {String(impact.details.arrival_environment_summary)}
                          </div>
                        ) : null}
                        {asRecord(impact.details?.arrival_operational_summary) ? (
                          <div className="mt-2 grid grid-cols-2 gap-2 font-mono text-[11px] text-white/50">
                            <div className="rounded border border-white/10 bg-black/30 px-2 py-1">
                              <div className="text-white/35">SPC / Watch</div>
                              <div>
                                {String(asRecord(impact.details?.arrival_operational_summary)?.spc_category ?? 'none')}
                                {' / '}
                                {String(asRecord(impact.details?.arrival_operational_summary)?.watch_type ?? 'none')}
                              </div>
                            </div>
                            <div className="rounded border border-white/10 bg-black/30 px-2 py-1">
                              <div className="text-white/35">Reports / MD</div>
                              <div>
                                {Number(asRecord(impact.details?.arrival_operational_summary)?.nearby_reports ?? 0)} reports
                                {' / '}
                                {Number(asRecord(impact.details?.arrival_operational_summary)?.active_discussions ?? 0)} MD
                              </div>
                            </div>
                          </div>
                        ) : null}
                        {Array.isArray(impact.details?.reasoning_factors) && impact.details.reasoning_factors.length ? (
                          <div className="mt-2 space-y-1">
                            {impact.details.reasoning_factors.slice(0, 2).map((factor) => (
                              <div key={String(factor)} className="rounded border border-white/10 bg-black/30 px-2 py-1 text-[11px] text-white/50">
                                {String(factor)}
                              </div>
                            ))}
                          </div>
                        ) : null}
                      </div>
                    ))}
                  </div>
                ) : (
                  <div className="rounded border border-white/10 bg-black/25 px-3 py-2 text-sm text-white/45">
                    No saved-location impacts are in the current forecast path.
                  </div>
                )}
              </div>

              {/* --- Event History Timeline (v15) --- */}
              <EventHistoryPanel stormId={selectedStorm.storm_id} />
            </div>
          ) : null}
        </>
      ) : null}
    </section>
  )
}
