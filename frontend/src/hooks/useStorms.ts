import { useEffect, useState } from 'react'

import { fetchStorms } from '../api/storms'
import type { StormSummary } from '../types/storms'


const POLL_INTERVAL_MS = 30_000


export function useStorms(site: string, enabled: boolean) {
  const [storms, setStorms] = useState<StormSummary[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    let active = true

    if (!enabled || !site) {
      setStorms([])
      setLoading(false)
      setError(null)
      return
    }

    const load = async () => {
      try {
        if (active) {
          setLoading(true)
        }
        const payload = await fetchStorms(site)
        if (!active) {
          return
        }
        setStorms(payload)
        setError(null)
      } catch (err) {
        if (!active) {
          return
        }
        setError(err instanceof Error ? err.message : 'Failed to load storms')
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

  return { storms, loading, error }
}
