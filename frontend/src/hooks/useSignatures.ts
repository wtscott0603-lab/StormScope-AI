import { useEffect, useRef, useState } from 'react'

import { fetchSignatures } from '../api/radar'
import type { SignaturesResponse } from '../types/radar'

export function useSignatures(site: string, product: string, tilt: number, enabled: boolean) {
  const [data, setData] = useState<SignaturesResponse | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const timerRef = useRef<number | null>(null)

  useEffect(() => {
    if (timerRef.current !== null) {
      window.clearInterval(timerRef.current)
      timerRef.current = null
    }

    if (!enabled || !site) {
      setData(null)
      setLoading(false)
      setError(null)
      return
    }

    let active = true

    const load = async () => {
      try {
        setLoading(true)
        const result = await fetchSignatures(site, product, tilt)
        if (!active) {
          return
        }
        setData(result)
        setError(null)
      } catch (err) {
        if (!active) {
          return
        }
        setError(err instanceof Error ? err.message : 'Failed to load signatures')
      } finally {
        if (active) {
          setLoading(false)
        }
      }
    }

    void load()
    timerRef.current = window.setInterval(() => {
      void load()
    }, 60_000)

    return () => {
      active = false
      if (timerRef.current !== null) {
        window.clearInterval(timerRef.current)
        timerRef.current = null
      }
    }
  }, [enabled, product, site, tilt])

  return { data, loading, error }
}
