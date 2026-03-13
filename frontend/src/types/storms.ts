import type { GeoJsonObject } from 'geojson'

export interface StormForecastPoint {
  lat: number
  lon: number
  eta_minutes: number
  label: string
}

export interface StormImpact {
  location_id: string
  location_name: string
  eta_minutes_low: number | null
  eta_minutes_high: number | null
  distance_km: number | null
  threat_at_arrival: string
  trend_at_arrival: string
  confidence: number
  summary: string
  impact_rank: number
  details: Record<string, unknown>
}

export interface StormEnvironment {
  source?: string | null
  current_station_id?: string | null
  future_station_id?: string | null
  gridpoint_id?: string | null
  surface_temp_c?: number | null
  dewpoint_c?: number | null
  wind_speed_kt?: number | null
  forecast_probability_of_thunder?: number | null
  ahead_probability_of_thunder?: number | null
  forecast_qpf_mm?: number | null
  forecast_wind_speed_kmh?: number | null
  hail_favorability?: number | null
  wind_favorability?: number | null
  tornado_favorability?: number | null
  heavy_rain_favorability?: number | null
  convective_signal?: number | null
  intensification_signal?: number | null
  weakening_signal?: number | null
  projected_trend?: string | null
  projection_confidence?: number | null
  environment_confidence?: number | null
  environment_freshness_minutes?: number | null
  environment_ahead_delta?: Record<string, number | null>
  cape_jkg?: number | null
  cin_jkg?: number | null
  bulk_shear_06km_kt?: number | null
  bulk_shear_01km_kt?: number | null
  srh_surface_925hpa_m2s2?: number | null
  dcape_jkg?: number | null
  dcape_is_proxy?: boolean | null
  freezing_level_m?: number | null
  pwat_mm?: number | null
  lapse_rate_midlevel_cpkm?: number | null
  lcl_m?: number | null
  lfc_m?: number | null
  model_valid_at?: string | null
  ahead_model_valid_at?: string | null
  weather_summary?: string | null
  hazards?: string[]
  ahead_trend?: string | null
  limitation?: string | null
  profile_summary?: Record<string, unknown>
  field_provenance?: Record<string, string>
  source_notes?: string[]
  hodograph?: Record<string, unknown>
  srv_metrics?: Record<string, unknown>
  operational_context?: Record<string, unknown>
  volume_metrics?: Record<string, unknown>
}

export interface StormPrediction {
  intensification_score?: number | null
  weakening_score?: number | null
  maintenance_score?: number | null
  projected_trend?: string | null
  projected_primary_threat?: string | null
  projected_secondary_threats?: string[]
  projected_confidence?: number | null
  projected_threat_scores?: Record<string, number>
  forecast_reasoning_factors?: string[]
  environment_confidence?: number | null
  motion_confidence?: number | null
  persistence_score?: number | null
  forecast_stability_score?: number | null
  data_quality_score?: number | null
  uncertainty_factors?: string[]
}

export interface UncertaintyConeStep {
  eta_minutes: number
  center: { lat: number; lon: number }
  left: { lat: number; lon: number }
  right: { lat: number; lon: number }
  half_width_km: number
}

export interface StormSummary {
  storm_id: string
  site: string
  latest_frame_id: string | null
  latest_scan_time: string
  created_at?: string | null
  updated_at?: string | null
  status: string
  lifecycle_state: string
  centroid_lat: number
  centroid_lon: number
  area_km2: number
  max_reflectivity: number
  mean_reflectivity: number
  motion_heading_deg: number | null
  motion_speed_kmh: number | null
  trend: string
  primary_threat: string
  secondary_threats: string[]
  severity_level: string
  confidence: number
  threat_scores: Record<string, number>
  narrative: string
  reasoning_factors: string[]
  footprint_geojson: GeoJsonObject
  forecast_path: StormForecastPoint[]
  // v12 — uncertainty cone and convective mode
  uncertainty_cone: UncertaintyConeStep[]
  storm_mode: string
  storm_mode_confidence: number
  storm_mode_evidence: string[]
  track_uncertainty_km: number
  associated_signatures: Array<Record<string, unknown>>
  environment_summary: StormEnvironment | null
  prediction_summary: StormPrediction | null
  near_term_expectation: string
  impacts: StormImpact[]
  // v13 — threat component breakdown and lifecycle analysis
  threat_component_breakdown?: Record<string, Record<string, number>>
  threat_top_reasons?: Record<string, string[]>
  threat_limiting_factors?: Record<string, string[]>
  lifecycle_summary?: Record<string, unknown>
  // v14 — event flags and operational priority
  event_flags?: EventFlag[]
  priority_score?: number
  priority_label?: string
}

export interface StormTrackPoint {
  scan_time: string
  centroid_lat: number
  centroid_lon: number
  max_reflectivity: number
  mean_reflectivity: number
  motion_heading_deg: number | null
  motion_speed_kmh: number | null
  trend: string
}

export interface SavedLocation {
  location_id: string
  name: string
  lat: number
  lon: number
  kind: string
  created_at: string
  updated_at: string
}

export interface SavedLocationCreate {
  name: string
  lat: number
  lon: number
  kind?: string
}

export interface MetarObservation {
  station_id: string
  observation_time: string | null
  lat: number
  lon: number
  temp_c: number | null
  dewpoint_c: number | null
  wind_dir_deg: number | null
  wind_speed_kt: number | null
  wind_gust_kt: number | null
  visibility_mi: number | null
  pressure_hpa: number | null
  flight_category: string | null
  raw_text: string | null
  distance_km: number | null
}

export interface OverlayFeatureCollection {
  overlay_kind: string | null
  source: string | null
  type: 'FeatureCollection'
  fetched_at: string | null
  features: Array<Record<string, unknown>>
}

export interface EventFlag {
  flag: string
  label: string
  confidence: number
  rationale: string
  severity: number
  provenance: string
}

export interface StormHotspot {
  storm_id: string
  site: string
  priority_score: number
  priority_label: string
  severity_level: string
  primary_threat: string
  threat_scores: Record<string, number>
  storm_mode: string
  centroid_lat: number
  centroid_lon: number
  motion_heading_deg: number | null
  motion_speed_kmh: number | null
  confidence: number
  trend: string
  event_flags: EventFlag[]
  top_flag: string | null
  impact_count: number
  latest_scan_time: string | null
}

export interface LocationRiskEntry {
  location_id: string
  location_name: string
  lat: number
  lon: number
  risk_level: 'HIGH' | 'MODERATE' | 'LOW' | 'NONE'
  risk_score: number
  threatening_storm_count: number
  nearest_eta_low: number | null
  nearest_eta_high: number | null
  primary_threat: string | null
  threat_scores: Record<string, number>
  top_storm_id: string | null
  top_storm_severity: string | null
  top_impact_summary: string | null
  trend: string | null
  confidence: number | null
  event_flag_labels: string[]
}

export interface StormCompareField {
  label: string
  storm_a: unknown
  storm_b: unknown
  delta: number | null
  note: string | null
}

export interface StormCompareResponse {
  storm_a_id: string
  storm_b_id: string
  fields: StormCompareField[]
  provenance: string
}

export interface StormEventHistoryPoint {
  scan_time: string
  event_flags: EventFlag[]
  lifecycle_state: string | null
  priority_score: number | null
  priority_label: string | null
  severity_level: string | null
  primary_threat: string | null
  threat_scores: Record<string, number>
  storm_mode: string | null
  motion_heading_deg: number | null
  motion_speed_kmh: number | null
  confidence: number | null
}

export interface StormEventHistoryResponse {
  storm_id: string
  site: string
  point_count: number
  points: StormEventHistoryPoint[]
  provenance: string
}

export interface StormPrecomputedSummary {
  storm_id: string
  site: string
  computed_at: string
  scan_count: number
  first_seen: string | null
  last_seen: string | null
  peak_severity: string | null
  peak_threat_scores: Record<string, number>
  peak_reflectivity: number | null
  max_area_km2: number | null
  max_speed_kmh: number | null
  max_priority_score: number | null
  dominant_mode: string | null
  flag_summary: Array<{ flag: string; label: string; occurrence_count: number }>
  threat_trend: Array<{ scan_time: string; max_score: number; primary_threat: string }>
  motion_trend: Array<{ scan_time: string; speed_kmh: number | null; heading_deg: number | null }>
  impact_location_ids: string[]
  summary_narrative: string | null
  provenance: string
}

export interface SiteHistoryStatus {
  site: string
  last_ingest_time: string | null
  last_processing_cycle_time: string | null
  last_history_aggregation_time: string | null
  snapshot_count: number
  event_history_count: number
  precomputed_summary_count: number
  backlog_frame_count: number
  is_caught_up: boolean
  history_stale: boolean
}
