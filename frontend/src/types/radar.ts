import type { Geometry } from 'geojson'

export interface BBox {
  min_lat: number
  max_lat: number
  min_lon: number
  max_lon: number
}

export interface Frame {
  frame_id: string
  site: string
  product: string
  tilt: number
  tilts_available: number[]
  timestamp: string
  bbox: BBox
  url: string
}

export interface Site {
  id: string
  name: string
  lat: number
  lon: number
  state: string
  has_data: boolean
  last_frame_time: string | null
}

export interface SiteDetail {
  id: string
  name: string
  lat: number
  lon: number
  state: string
  elevation_m: number
  range_km: number
  last_frame_time: string | null
  available_products: string[]
}

export interface Product {
  id: string
  name: string
  description: string
  unit: string
  enabled: boolean
  available: boolean
  source_kind: string
  source_product?: string | null
}

export interface Alert {
  id: string
  event: string
  severity: string
  issued: string | null
  expires: string | null
  geometry: Geometry
}

export interface ApiConfig {
  default_site: string
  enabled_products: string[]
  update_interval_sec: number
  tile_url: string
  default_center_lat: number
  default_center_lon: number
  default_map_zoom: number
  preferred_units: string
  default_enabled_overlays: string[]
  local_station_priority: string[]
}

export interface ApiStatus {
  processor_status: 'running' | 'idle' | 'error'
  frames_cached: number
  sites_active: number
  active_storms?: number | null
  processor_last_run?: string | null
  processor_age_minutes?: number | null
  environment_snapshot_age_minutes?: number | null
  cache_status?: Record<string, { available: boolean; stale: boolean; fetched_at: string | null; age_minutes: number | null }>
  data_warnings?: string[]
  last_error: string | null
  // v15 — history freshness
  last_ingest_time?: string | null
  last_history_aggregation_time?: string | null
  history_stale?: boolean
  backlog_frame_count?: number
  is_caught_up?: boolean
  site_history_statuses?: import('./storms').SiteHistoryStatus[]
}

export interface Health {
  status: 'ok' | 'degraded'
  version: string
  processor_last_run: string | null
  processor_status: 'ok' | 'stale' | 'error' | 'never_run'
  db_ok: boolean
}

export type SeverityLevel = 'TORNADO_EMERGENCY' | 'TORNADO' | 'SEVERE' | 'MARGINAL' | 'NONE'

export interface SignatureMarker {
  signature_type: 'TVS' | 'TDS' | 'ROTATION' | 'HAIL_CORE' | 'HAIL_LARGE' | 'BOW_ECHO' | 'BWER' | string
  severity: SeverityLevel
  lat: number
  lon: number
  radius_km: number
  label: string
  description: string
  confidence: number
  metrics: Record<string, unknown>
  frame_id: string
  analyzer: string
  ran_at: string
}

export interface SignaturesResponse {
  site: string
  product: string
  frame_id: string | null
  signatures: SignatureMarker[]
  max_severity: SeverityLevel
  generated_at: string
}

export interface TiltListResponse {
  site: string
  product: string
  tilts: number[]
}

export interface CrossSectionPoint {
  lat: number
  lon: number
}

export interface CrossSectionRequest {
  site: string
  product: string
  frame_id?: string | null
  start: CrossSectionPoint
  end: CrossSectionPoint
  samples?: number
  altitude_resolution_km?: number
  max_altitude_km?: number
}

export interface CrossSectionResponse {
  site: string
  product: string
  frame_id: string
  ranges_km: number[]
  altitudes_km: number[]
  values: Array<Array<number | null>>
  start: CrossSectionPoint
  end: CrossSectionPoint
  tilts_used: number[]
  unit: string
  method: string
  limitation: string
  generated_at: string
}
