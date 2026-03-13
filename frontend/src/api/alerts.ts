import { apiFetch } from './client'
import type { Alert } from '../types/radar'

export function fetchAlerts(state?: string) {
  const suffix = state ? `?state=${state}` : ''
  return apiFetch<Alert[]>(`/api/alerts${suffix}`)
}

export interface TriggeredAlert {
  id: number
  alert_id: string
  storm_id: string | null
  site: string
  location_id: string | null
  alert_kind: string
  severity_level: string
  title: string
  body: string
  threat_score: number | null
  triggered_at: string
  scan_time: string
  acknowledged: boolean
  acknowledged_at: string | null
}

export function fetchTriggeredAlerts(opts?: { site?: string; unacknowledgedOnly?: boolean; limit?: number }) {
  const params = new URLSearchParams()
  if (opts?.site) params.set('site', opts.site)
  if (opts?.unacknowledgedOnly) params.set('unacknowledged_only', 'true')
  if (opts?.limit) params.set('limit', String(opts.limit))
  const qs = params.toString()
  return apiFetch<TriggeredAlert[]>(`/api/v1/alerts/triggered${qs ? `?${qs}` : ''}`)
}

export function acknowledgeTriggeredAlert(alertId: string) {
  return apiFetch<{ acknowledged: boolean; alert_id: string }>(
    `/api/v1/alerts/triggered/${alertId}/acknowledge`,
    { method: 'POST' },
  )
}
