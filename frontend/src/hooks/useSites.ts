import { useEffect, useState } from 'react'

import { fetchSites } from '../api/sites'
import type { Site } from '../types/radar'

export function useSites() {
  const [sites, setSites] = useState<Site[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    let active = true

    async function load() {
      try {
        setLoading(true)
        const payload = await fetchSites()
        if (active) {
          setSites(payload)
          setError(null)
        }
      } catch (err) {
        if (active) {
          setError(err instanceof Error ? err.message : 'Failed to load radar sites.')
        }
      } finally {
        if (active) {
          setLoading(false)
        }
      }
    }

    void load()
    return () => {
      active = false
    }
  }, [])

  return { sites, loading, error }
}
