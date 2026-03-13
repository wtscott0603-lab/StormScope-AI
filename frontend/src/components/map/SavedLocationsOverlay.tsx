import { useEffect, useRef } from 'react'
import maplibregl from 'maplibre-gl'

import type { SavedLocation } from '../../types/storms'

function createMarker(location: SavedLocation) {
  const element = document.createElement('button')
  element.type = 'button'
  element.style.cssText = `
    display:flex;
    align-items:center;
    justify-content:center;
    min-width:18px;
    height:18px;
    padding:0 5px;
    border-radius:999px;
    border:1px solid #39d3ff;
    background:#0a0d0fcc;
    color:#39d3ff;
    font:600 9px 'JetBrains Mono', monospace;
    letter-spacing:0.06em;
    cursor:pointer;
  `
  element.textContent = location.name.slice(0, 3).toUpperCase()
  element.title = location.name
  return element
}

export function SavedLocationsOverlay({
  map,
  locations,
  visible,
}: {
  map: maplibregl.Map | null
  locations: SavedLocation[]
  visible: boolean
}) {
  const markersRef = useRef<maplibregl.Marker[]>([])

  useEffect(() => {
    if (!map) {
      return
    }

    markersRef.current.forEach((marker) => marker.remove())
    markersRef.current = []

    if (!visible) {
      return
    }

    locations.forEach((location) => {
      const marker = new maplibregl.Marker({ element: createMarker(location), anchor: 'center' })
        .setLngLat([location.lon, location.lat])
        .setPopup(
          new maplibregl.Popup({ offset: 20 }).setHTML(`
            <div style="background:#0a0d0f;color:#dce6ea;border:1px solid #39d3ff;border-radius:6px;padding:8px 10px;font-family:'JetBrains Mono',monospace;font-size:11px;">
              <div style="font-weight:700;margin-bottom:4px;">${location.name}</div>
              <div>${location.lat.toFixed(3)}, ${location.lon.toFixed(3)}</div>
            </div>
          `),
        )
        .addTo(map)
      markersRef.current.push(marker)
    })

    return () => {
      markersRef.current.forEach((marker) => marker.remove())
      markersRef.current = []
    }
  }, [locations, map, visible])

  return null
}
