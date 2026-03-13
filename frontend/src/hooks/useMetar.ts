import { useEffect, useState } from 'react'

import { fetchMetar } from '../api/storms'
import type { MetarObservation } from '../types/storms'


const POLL_INTERVAL_MS = 120_000


export function useMetar(site: string, enabled: boolean) {
  const [observations, setObservations] = useState<MetarObservation[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    let active = true

    if (!enabled || !site) {
      setObservations([])
      setLoading(false)
      setError(null)
      return
    }

    const load = async () => {
      try {
        if (active) {
          setLoading(true)
        }
        const payload = await fetchMetar(site)
        if (!active) {
          return
        }
        setObservations(payload)
        setError(null)
      } catch (err) {
        if (!active) {
          return
        }
        setError(err instanceof Error ? err.message : 'Failed to load METAR observations')
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
  }, [enabled, site])

  return { observations, loading, error }
}
