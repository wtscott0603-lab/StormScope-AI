import type { SignatureMarker } from '../types/radar'
import type { StormSummary } from '../types/storms'


export interface AlertSettings {
  enabled: boolean
  browserNotifications: boolean
  audioAlerts: boolean
  hailThreshold: number
  windThreshold: number
  floodThreshold: number
  etaWindowMinutes: number
  rapidStrengtheningThreshold: number
}


export interface TriggeredAlert {
  id: string
  title: string
  body: string
  severity: 'high' | 'medium'
}


const STORAGE_KEY = 'radarAlertSettings'
const SEEN_ALERTS_SESSION_KEY = 'radarSeenAlertIds'


export function loadAlertSettings(): AlertSettings {
  if (typeof window === 'undefined') {
    return {
      enabled: true,
      browserNotifications: false,
      audioAlerts: false,
      hailThreshold: 0.72,
      windThreshold: 0.7,
      floodThreshold: 0.7,
      etaWindowMinutes: 30,
      rapidStrengtheningThreshold: 0.68,
    }
  }
  try {
    const raw = window.localStorage.getItem(STORAGE_KEY)
    if (!raw) {
      throw new Error('missing settings')
    }
    return { ...loadAlertSettings(), ...JSON.parse(raw) } as AlertSettings
  } catch {
    return {
      enabled: true,
      browserNotifications: false,
      audioAlerts: false,
      hailThreshold: 0.72,
      windThreshold: 0.7,
      floodThreshold: 0.7,
      etaWindowMinutes: 30,
      rapidStrengtheningThreshold: 0.68,
    }
  }
}


export function saveAlertSettings(settings: AlertSettings) {
  if (typeof window === 'undefined') {
    return
  }
  window.localStorage.setItem(STORAGE_KEY, JSON.stringify(settings))
}


export function loadSeenAlertIds(): Set<string> {
  if (typeof window === 'undefined') {
    return new Set()
  }
  try {
    const raw = window.sessionStorage.getItem(SEEN_ALERTS_SESSION_KEY)
    if (!raw) {
      return new Set()
    }
    const parsed = JSON.parse(raw)
    return new Set(Array.isArray(parsed) ? parsed.map((value) => String(value)) : [])
  } catch {
    return new Set()
  }
}


export function saveSeenAlertIds(ids: Set<string>) {
  if (typeof window === 'undefined') {
    return
  }
  window.sessionStorage.setItem(SEEN_ALERTS_SESSION_KEY, JSON.stringify(Array.from(ids)))
}


export function evaluateTriggeredAlerts(
  storms: StormSummary[],
  signatures: SignatureMarker[],
  settings: AlertSettings,
): TriggeredAlert[] {
  if (!settings.enabled) {
    return []
  }

  const alerts: TriggeredAlert[] = []
  for (const signature of signatures) {
    if (signature.signature_type === 'TVS' || signature.signature_type === 'TDS') {
      alerts.push({
        id: `signature:${signature.frame_id}:${signature.signature_type}:${signature.ran_at}`,
        title: signature.signature_type === 'TDS' ? 'Debris Signature Alert' : 'Rotation Signature Alert',
        body: `${signature.label} near ${signature.lat.toFixed(2)}, ${signature.lon.toFixed(2)} (${signature.severity.toLowerCase().replace('_', ' ')})`,
        severity: 'high',
      })
    }
  }

  for (const storm of storms) {
    const detectionAnchor = storm.created_at ?? storm.latest_scan_time
    if ((storm.threat_scores.hail ?? 0) >= settings.hailThreshold) {
      alerts.push({
        id: `storm:${storm.storm_id}:${detectionAnchor}:hail`,
        title: 'Hail Threshold Reached',
        body: `${storm.storm_id} hail risk ${Math.round((storm.threat_scores.hail ?? 0) * 100)}% with ${storm.max_reflectivity.toFixed(0)} dBZ.`,
        severity: 'medium',
      })
    }
    if ((storm.threat_scores.wind ?? 0) >= settings.windThreshold) {
      alerts.push({
        id: `storm:${storm.storm_id}:${detectionAnchor}:wind`,
        title: 'Damaging Wind Threshold Reached',
        body: `${storm.storm_id} wind risk ${Math.round((storm.threat_scores.wind ?? 0) * 100)}% and moving ${storm.motion_speed_kmh?.toFixed(0) ?? '--'} km/h.`,
        severity: 'medium',
      })
    }
    if ((storm.threat_scores.flood ?? 0) >= settings.floodThreshold) {
      alerts.push({
        id: `storm:${storm.storm_id}:${detectionAnchor}:flood`,
        title: 'Heavy Rain Threshold Reached',
        body: `${storm.storm_id} heavy-rain risk ${Math.round((storm.threat_scores.flood ?? 0) * 100)}%.`,
        severity: 'medium',
      })
    }
    if ((storm.prediction_summary?.intensification_score ?? 0) >= settings.rapidStrengtheningThreshold) {
      alerts.push({
        id: `storm:${storm.storm_id}:${detectionAnchor}:trend`,
        title: 'Rapid Strengthening Signal',
        body: `${storm.storm_id} shows strengthening potential with projected ${storm.prediction_summary?.projected_trend ?? 'trend'}.`,
        severity: 'medium',
      })
    }
    for (const impact of storm.impacts) {
      const etaLow = impact.eta_minutes_low ?? impact.eta_minutes_high ?? null
      const arrivalOperationalSummary =
        impact.details && typeof impact.details === 'object'
          ? ((impact.details.arrival_operational_summary as Record<string, unknown> | undefined) ?? undefined)
          : undefined
      if (etaLow !== null && etaLow <= settings.etaWindowMinutes) {
        alerts.push({
          id: `impact:${storm.storm_id}:${impact.location_id}:${detectionAnchor}`,
          title: `${impact.location_name} ETA Alert`,
          body: `${storm.primary_threat.toUpperCase()} storm may affect ${impact.location_name} in ${etaLow} minutes. ${impact.summary}`,
          severity: storm.severity_level.includes('TORNADO') ? 'high' : 'medium',
        })
        if (arrivalOperationalSummary?.watch_type) {
          const watchType = String(arrivalOperationalSummary.watch_type)
          const pds = Boolean(arrivalOperationalSummary.watch_pds)
          alerts.push({
            id: `impact:${storm.storm_id}:${impact.location_id}:${detectionAnchor}:watch`,
            title: `${impact.location_name} Watch Context`,
            body: `${pds ? 'PDS ' : ''}${watchType} remains in effect during the projected arrival window for ${impact.location_name}.`,
            severity: watchType.toLowerCase().includes('tornado') || pds ? 'high' : 'medium',
          })
        }
      }
    }
  }

  return alerts
}
