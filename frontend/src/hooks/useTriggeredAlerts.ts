import { useCallback, useEffect, useRef, useState } from 'react'
import {
  acknowledgeTriggeredAlert,
  fetchTriggeredAlerts,
  type TriggeredAlert,
} from '../api/alerts'

const POLL_INTERVAL_MS = 30_000

export function useTriggeredAlerts(site?: string, enabled = true) {
  const [alerts, setAlerts] = useState<TriggeredAlert[]>([])
  const [unacknowledgedCount, setUnacknowledgedCount] = useState(0)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const activeRef = useRef(true)

  const load = useCallback(async () => {
    try {
      if (activeRef.current) setLoading(true)
      const payload = await fetchTriggeredAlerts({ site, limit: 100 })
      if (activeRef.current) {
        setAlerts(payload)
        setUnacknowledgedCount(payload.filter((a) => !a.acknowledged).length)
        setError(null)
      }
    } catch (err) {
      if (activeRef.current) {
        setError(err instanceof Error ? err.message : 'Failed to load triggered alerts.')
      }
    } finally {
      if (activeRef.current) setLoading(false)
    }
  }, [site])

  useEffect(() => {
    activeRef.current = true
    if (!enabled) {
      setAlerts([])
      setUnacknowledgedCount(0)
      return
    }
    void load()
    const timer = window.setInterval(() => void load(), POLL_INTERVAL_MS)
    return () => {
      activeRef.current = false
      window.clearInterval(timer)
    }
  }, [enabled, load])

  const acknowledge = useCallback(
    async (alertId: string) => {
      try {
        await acknowledgeTriggeredAlert(alertId)
        setAlerts((prev) =>
          prev.map((a) =>
            a.alert_id === alertId
              ? { ...a, acknowledged: true, acknowledged_at: new Date().toISOString() }
              : a,
          ),
        )
        setUnacknowledgedCount((c) => Math.max(0, c - 1))
      } catch {
        // best-effort; next poll will reflect server state
      }
    },
    [],
  )

  const acknowledgeAll = useCallback(async () => {
    const pending = alerts.filter((a) => !a.acknowledged)
    await Promise.allSettled(pending.map((a) => acknowledgeTriggeredAlert(a.alert_id)))
    setAlerts((prev) => prev.map((a) => ({ ...a, acknowledged: true })))
    setUnacknowledgedCount(0)
  }, [alerts])

  return { alerts, unacknowledgedCount, loading, error, acknowledge, acknowledgeAll, refresh: load }
}
