import { apiFetch } from './client'
import type { OverlayFeatureCollection } from '../types/storms'

export function fetchSpcOverlays() {
  return apiFetch<OverlayFeatureCollection>('/api/v1/overlays/spc')
}

export function fetchSpcDay2Overlays() {
  return apiFetch<OverlayFeatureCollection>('/api/v1/overlays/spc_day2')
}

export function fetchSpcDay3Overlays() {
  return apiFetch<OverlayFeatureCollection>('/api/v1/overlays/spc_day3')
}

export function fetchMesoscaleDiscussions() {
  return apiFetch<OverlayFeatureCollection>('/api/v1/overlays/md')
}

export function fetchLocalStormReports() {
  return apiFetch<OverlayFeatureCollection>('/api/v1/overlays/lsr')
}

export function fetchWatchBoxes() {
  return apiFetch<OverlayFeatureCollection>('/api/v1/overlays/watch')
}
