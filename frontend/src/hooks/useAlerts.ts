import { useEffect, useState } from 'react'

import { fetchAlerts } from '../api/alerts'
import type { Alert } from '../types/radar'


const POLL_INTERVAL_MS = 60_000


export function useAlerts(stateCode?: string, enabled = true) {
  const [alerts, setAlerts] = useState<Alert[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    if (!enabled) {
      setAlerts([])
      return
    }

    let active = true

    async function load() {
      try {
        if (active) {
          setLoading(true)
        }
        const payload = await fetchAlerts(stateCode)
        if (active) {
          setAlerts(payload)
          setError(null)
        }
      } catch (err) {
        if (active) {
          setError(err instanceof Error ? err.message : 'Failed to load active alerts.')
        }
      } finally {
        if (active) {
          setLoading(false)
        }
      }
    }

    void load()
    const timer = window.setInterval(() => void load(), POLL_INTERVAL_MS)
    return () => {
      active = false
      window.clearInterval(timer)
    }
  }, [enabled, stateCode])

  return { alerts, loading, error }
}
