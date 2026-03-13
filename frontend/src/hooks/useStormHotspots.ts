import { useEffect, useState } from 'react'
import { fetchStormHotspots } from '../api/storms'
import type { StormHotspot } from '../types/storms'

export function useStormHotspots(site: string | null, limit = 10, refreshMs = 30_000) {
  const [data, setData] = useState<StormHotspot[]>([])
  const [loading, setLoading] = useState(false)

  useEffect(() => {
    if (!site) {
      setData([])
      return
    }
    let cancelled = false

    const load = () => {
      setLoading(true)
      fetchStormHotspots(site, limit)
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
  }, [site, limit, refreshMs])

  return { data, loading }
}
