import { useEffect, useRef } from 'react'
import maplibregl from 'maplibre-gl'

import type { MetarObservation } from '../../types/storms'

function formatTemp(temp: number | null) {
  return temp === null ? '--' : `${Math.round(temp)}C`
}

export function MetarOverlay({
  map,
  observations,
  visible,
}: {
  map: maplibregl.Map | null
  observations: MetarObservation[]
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

    observations.slice(0, 18).forEach((observation) => {
      const element = document.createElement('div')
      element.style.cssText = `
        display:flex;
        flex-direction:column;
        align-items:center;
        gap:1px;
        min-width:38px;
        padding:4px 5px;
        border-radius:6px;
        border:1px solid rgba(138, 163, 173, 0.4);
        background:rgba(10, 13, 15, 0.84);
        color:#dce6ea;
        font:600 9px 'JetBrains Mono', monospace;
        line-height:1.1;
      `
      element.innerHTML = `
        <span>${observation.station_id}</span>
        <span style="color:#39d3ff">${formatTemp(observation.temp_c)}</span>
        <span style="opacity:0.6">${observation.wind_speed_kt ?? '--'}kt</span>
      `

      const marker = new maplibregl.Marker({ element, anchor: 'center' })
        .setLngLat([observation.lon, observation.lat])
        .setPopup(
          new maplibregl.Popup({ offset: 20 }).setHTML(`
            <div style="background:#0a0d0f;color:#dce6ea;border:1px solid #5f737e;border-radius:6px;padding:8px 10px;font-family:'JetBrains Mono',monospace;font-size:11px;">
              <div style="font-weight:700;margin-bottom:4px;">${observation.station_id}</div>
              <div>${formatTemp(observation.temp_c)} / ${formatTemp(observation.dewpoint_c)}</div>
              <div>${observation.wind_speed_kt ?? '--'} kt wind</div>
              <div>${observation.flight_category ?? 'UNK'} | ${observation.distance_km ?? '--'} km</div>
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
  }, [map, observations, visible])

  return null
}
