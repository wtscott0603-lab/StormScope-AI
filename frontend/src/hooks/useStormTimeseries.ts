import { useEffect, useState } from 'react'
import { fetchStormTimeseries, type StormTimeSeriesResponse } from '../api/storms'

/**
 * Fetch per-scan time series for a selected storm.
 * Returns null while loading or if no storm is selected.
 * Automatically refreshes when the stormId changes.
 */
export function useStormTimeseries(stormId: string | null, limit = 20) {
  const [data, setData] = useState<StormTimeSeriesResponse | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    if (!stormId) {
      setData(null)
      return
    }

    let cancelled = false
    setLoading(true)
    setError(null)

    fetchStormTimeseries(stormId, limit)
      .then((result) => {
        if (!cancelled) {
          setData(result)
        }
      })
      .catch((err) => {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : 'Failed to fetch timeseries')
        }
      })
      .finally(() => {
        if (!cancelled) setLoading(false)
      })

    return () => { cancelled = true }
  }, [stormId, limit])

  return { data, loading, error }
}
