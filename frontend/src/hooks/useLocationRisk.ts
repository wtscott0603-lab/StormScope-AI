import { useEffect, useState } from 'react'
import { fetchLocationRisk } from '../api/storms'
import type { LocationRiskEntry } from '../types/storms'

export function useLocationRisk(site: string | null, refreshMs = 30_000) {
  const [data, setData] = useState<LocationRiskEntry[]>([])
  const [loading, setLoading] = useState(false)

  useEffect(() => {
    if (!site) {
      setData([])
      return
    }
    let cancelled = false

    const load = () => {
      setLoading(true)
      fetchLocationRisk(site)
        .then((result) => { if (!cancelled) setData(result) })
        .catch(() => {})
        .finally(() => { if (!cancelled) setLoading(false) })
    }

    load()
    const id = setInterval(load, refreshMs)
    return () => {
      cancelled = true
      clearInterval(id)
    }
  }, [site, refreshMs])

  return { data, loading }
}
