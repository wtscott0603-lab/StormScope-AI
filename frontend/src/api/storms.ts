import { apiFetch } from './client'
import type { MetarObservation, SavedLocation, SavedLocationCreate, StormEnvironment, StormImpact, StormSummary, StormTrackPoint } from '../types/storms'

export function fetchStorms(site: string) {
  return apiFetch<StormSummary[]>(`/api/v1/storms?site=${site}`)
}

export function fetchStorm(stormId: string) {
  return apiFetch<StormSummary>(`/api/v1/storms/${stormId}`)
}

export function fetchStormTrack(stormId: string) {
  return apiFetch<StormTrackPoint[]>(`/api/v1/storms/${stormId}/track`)
}

export function fetchStormEnvironment(stormId: string) {
  return apiFetch<StormEnvironment>(`/api/v1/storms/${stormId}/environment`)
}

export function fetchStormImpacts(stormId: string) {
  return apiFetch<StormImpact[]>(`/api/v1/storms/${stormId}/impacts`)
}

export function fetchSavedLocations() {
  return apiFetch<SavedLocation[]>('/api/v1/locations')
}

export function createSavedLocation(payload: SavedLocationCreate) {
  return apiFetch<SavedLocation>('/api/v1/locations', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  })
}

export function deleteSavedLocation(locationId: string) {
  return apiFetch<{ status: string }>(`/api/v1/locations/${locationId}`, {
    method: 'DELETE',
  })
}

export function fetchMetar(site: string) {
  return apiFetch<MetarObservation[]>(`/api/v1/metar?site=${site}`)
}

// ---------------------------------------------------------------------------
// v13 — Timeseries and threat breakdown endpoints
// ---------------------------------------------------------------------------

export interface StormTimeSeriesPoint {
  scan_time: string
  centroid_lat: number
  centroid_lon: number
  area_km2: number
  max_reflectivity: number
  mean_reflectivity: number
  motion_speed_kmh: number | null
  motion_heading_deg: number | null
  trend: string
  severity_level: string
  confidence: number
  threat_scores: Record<string, number>
}

export interface StormTimeSeriesResponse {
  storm_id: string
  site: string
  point_count: number
  points: StormTimeSeriesPoint[]
  provenance: string
}

export interface ThreatComponentBreakdownResponse {
  storm_id: string
  threat_scores: Record<string, number>
  component_breakdown: Record<string, Record<string, number>>
  top_reasons: Record<string, string[]>
  limiting_factors: Record<string, string[]>
  lifecycle_summary: Record<string, unknown>
  provenance: string
}

export function fetchStormTimeseries(stormId: string, limit = 20) {
  return apiFetch<StormTimeSeriesResponse>(`/api/v1/storms/${stormId}/timeseries?limit=${limit}`)
}

export function fetchStormBreakdown(stormId: string) {
  return apiFetch<ThreatComponentBreakdownResponse>(`/api/v1/storms/${stormId}/breakdown`)
}

export async function fetchStormHotspots(site: string, limit = 10) {
  return apiFetch<import('../types/storms').StormHotspot[]>(
    `/api/v1/storms/hotspots?site=${encodeURIComponent(site)}&limit=${limit}`
  )
}

export async function fetchLocationRisk(site: string) {
  return apiFetch<import('../types/storms').LocationRiskEntry[]>(
    `/api/v1/locations/risk?site=${encodeURIComponent(site)}`
  )
}

export async function fetchStormCompare(stormAId: string, stormBId: string) {
  return apiFetch<import('../types/storms').StormCompareResponse>(
    `/api/v1/storms/compare?storm_a=${encodeURIComponent(stormAId)}&storm_b=${encodeURIComponent(stormBId)}`
  )
}

export async function fetchStormEventHistory(stormId: string, limit = 60) {
  return apiFetch<import('../types/storms').StormEventHistoryResponse>(
    `/api/v1/storms/${encodeURIComponent(stormId)}/event-history?limit=${limit}`
  )
}

export async function fetchStormPrecomputedSummary(stormId: string) {
  return apiFetch<import('../types/storms').StormPrecomputedSummary>(
    `/api/v1/storms/${encodeURIComponent(stormId)}/summary`
  )
}
