import { useEffect, useMemo } from 'react'
import maplibregl from 'maplibre-gl'

import type { Site } from '../../types/radar'

export function SiteMarker({
  map,
  site,
  selected,
  visible,
  onSelect,
}: {
  map: maplibregl.Map | null
  site: Site
  selected: boolean
  visible: boolean
  onSelect: (siteId: string) => void
}) {
  const element = useMemo(() => {
    const node = document.createElement('button')
    node.className = [
      'h-3 w-3 rounded-full border transition-all',
      selected ? 'border-cyan bg-cyan shadow-[0_0_18px_rgba(0,212,255,0.8)]' : 'border-white/70 bg-black/80',
    ].join(' ')
    node.title = `${site.id} · ${site.name}`
    return node
  }, [selected, site.id, site.name])

  useEffect(() => {
    if (!map || !visible) {
      return
    }
    element.onclick = () => onSelect(site.id)
    const marker = new maplibregl.Marker({ element, anchor: 'center' }).setLngLat([site.lon, site.lat]).addTo(map)
    return () => {
      marker.remove()
    }
  }, [element, map, onSelect, site.id, site.lat, site.lon, visible])

  return null
}
