import { useEffect, useRef, useState } from 'react'

import {
  evaluateTriggeredAlerts,
  loadAlertSettings,
  loadSeenAlertIds,
  saveAlertSettings,
  saveSeenAlertIds,
  type AlertSettings,
} from '../lib/alertMonitor'
import type { SignatureMarker } from '../types/radar'
import type { StormSummary } from '../types/storms'


function playAlertTone() {
  if (typeof window === 'undefined' || !('AudioContext' in window || 'webkitAudioContext' in window)) {
    return
  }
  const AudioCtx = window.AudioContext || (window as typeof window & { webkitAudioContext: typeof AudioContext }).webkitAudioContext
  const context = new AudioCtx()
  const oscillator = context.createOscillator()
  const gain = context.createGain()
  oscillator.type = 'sine'
  oscillator.frequency.value = 880
  gain.gain.value = 0.04
  oscillator.connect(gain)
  gain.connect(context.destination)
  oscillator.start()
  oscillator.stop(context.currentTime + 0.25)
}


export function useAlertMonitor(storms: StormSummary[], signatures: SignatureMarker[]) {
  const [settings, setSettingsState] = useState<AlertSettings>(() => loadAlertSettings())
  const seenIdsRef = useRef<Set<string>>(loadSeenAlertIds())

  useEffect(() => {
    saveAlertSettings(settings)
  }, [settings])

  useEffect(() => {
    const alerts = evaluateTriggeredAlerts(storms, signatures, settings)
    const unseen = alerts.filter((alert) => !seenIdsRef.current.has(alert.id))
    if (!unseen.length) {
      return
    }
    unseen.forEach((alert) => {
      seenIdsRef.current.add(alert.id)
      if (settings.audioAlerts) {
        playAlertTone()
      }
      if (settings.browserNotifications && typeof window !== 'undefined' && 'Notification' in window && Notification.permission === 'granted') {
        void new Notification(alert.title, { body: alert.body })
      }
    })
    saveSeenAlertIds(seenIdsRef.current)
  }, [settings, signatures, storms])

  const updateSettings = (next: Partial<AlertSettings>) => {
    setSettingsState((current) => ({ ...current, ...next }))
  }

  const requestNotificationPermission = async () => {
    if (typeof window === 'undefined' || !('Notification' in window)) {
      return false
    }
    const result = await Notification.requestPermission()
    return result === 'granted'
  }

  return {
    settings,
    updateSettings,
    requestNotificationPermission,
  }
}
