import { useEffect, useState } from 'react'

import {
  fetchLocalStormReports,
  fetchMesoscaleDiscussions,
  fetchSpcDay2Overlays,
  fetchSpcDay3Overlays,
  fetchSpcOverlays,
  fetchWatchBoxes,
} from '../api/overlays'
import type { OverlayFeatureCollection } from '../types/storms'

const EMPTY: OverlayFeatureCollection = {
  overlay_kind: null,
  source: null,
  type: 'FeatureCollection',
  fetched_at: null,
  features: [],
}

export function useOperationalOverlays(enabled: {
  spc: boolean
  spcDay2: boolean
  spcDay3: boolean
  md: boolean
  lsr: boolean
  watch: boolean
}) {
  const [spc, setSpc] = useState<OverlayFeatureCollection>(EMPTY)
  const [spcDay2, setSpcDay2] = useState<OverlayFeatureCollection>(EMPTY)
  const [spcDay3, setSpcDay3] = useState<OverlayFeatureCollection>(EMPTY)
  const [md, setMd] = useState<OverlayFeatureCollection>(EMPTY)
  const [lsr, setLsr] = useState<OverlayFeatureCollection>(EMPTY)
  const [watch, setWatch] = useState<OverlayFeatureCollection>(EMPTY)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    let active = true

    const load = async () => {
      const results = await Promise.allSettled([
        enabled.spc ? fetchSpcOverlays() : Promise.resolve(EMPTY),
        enabled.spcDay2 ? fetchSpcDay2Overlays() : Promise.resolve(EMPTY),
        enabled.spcDay3 ? fetchSpcDay3Overlays() : Promise.resolve(EMPTY),
        enabled.md ? fetchMesoscaleDiscussions() : Promise.resolve(EMPTY),
        enabled.lsr ? fetchLocalStormReports() : Promise.resolve(EMPTY),
        enabled.watch ? fetchWatchBoxes() : Promise.resolve(EMPTY),
      ])
      if (!active) return

      const [spcRes, d2Res, d3Res, mdRes, lsrRes, watchRes] = results
      const failures: string[] = []

      if (spcRes.status === 'fulfilled') setSpc(spcRes.value)
      else if (enabled.spc) failures.push('SPC Day 1')

      if (d2Res.status === 'fulfilled') setSpcDay2(d2Res.value)
      else if (enabled.spcDay2) failures.push('SPC Day 2')

      if (d3Res.status === 'fulfilled') setSpcDay3(d3Res.value)
      else if (enabled.spcDay3) failures.push('SPC Day 3')

      if (mdRes.status === 'fulfilled') setMd(mdRes.value)
      else if (enabled.md) failures.push('mesoscale discussions')

      if (lsrRes.status === 'fulfilled') setLsr(lsrRes.value)
      else if (enabled.lsr) failures.push('local storm reports')

      if (watchRes.status === 'fulfilled') setWatch(watchRes.value)
      else if (enabled.watch) failures.push('watch boxes')

      setError(failures.length ? `Failed to load ${failures.join(', ')}.` : null)
    }

    void load()
    const timer = window.setInterval(() => void load(), 300_000)

    return () => {
      active = false
      window.clearInterval(timer)
    }
  }, [enabled.lsr, enabled.md, enabled.spc, enabled.spcDay2, enabled.spcDay3, enabled.watch])

  return { spc, spcDay2, spcDay3, md, lsr, watch, error }
}
