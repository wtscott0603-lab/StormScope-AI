import { useEffect, useState } from 'react'

import { fetchStormTrack } from '../api/storms'
import type { StormTrackPoint } from '../types/storms'


export function useStormTrack(stormId: string | null, enabled: boolean) {
  const [track, setTrack] = useState<StormTrackPoint[]>([])
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    let active = true

    if (!enabled || !stormId) {
      setTrack([])
      setError(null)
      return
    }

    const load = async () => {
      try {
        const payload = await fetchStormTrack(stormId)
        if (!active) {
          return
        }
        setTrack(payload)
        setError(null)
      } catch (err) {
        if (!active) {
          return
        }
        setError(err instanceof Error ? err.message : 'Failed to load storm track')
      }
    }

    void load()
    const timer = window.setInterval(() => void load(), 30_000)
    return () => {
      active = false
      window.clearInterval(timer)
    }
  }, [enabled, stormId])

  return { track, error }
}
