import { useEffect, useState } from 'react'
import type maplibregl from 'maplibre-gl'

import type { Site } from '../../types/radar'

function projectedSite(map: maplibregl.Map | null, site: Site | undefined) {
  if (!map || !site) {
    return null
  }
  const projected = map.project([site.lon, site.lat])
  return { x: projected.x, y: projected.y }
}

export function RadarSweepOverlay({
  map,
  site,
  visible,
}: {
  map: maplibregl.Map | null
  site: Site | undefined
  visible: boolean
}) {
  const [projected, setProjected] = useState<{ x: number; y: number } | null>(null)
  const [diameter, setDiameter] = useState(520)

  useEffect(() => {
    if (!map || !site) {
      setProjected(null)
      return
    }

    const update = () => {
      setProjected(projectedSite(map, site))
      const canvas = map.getCanvas()
      const nextDiameter = Math.max(320, Math.min(Math.min(canvas.width, canvas.height) * 0.8, 760))
      setDiameter(nextDiameter)
    }

    update()
    map.on('move', update)
    map.on('resize', update)
    return () => {
      map.off('move', update)
      map.off('resize', update)
    }
  }, [map, site])

  useEffect(() => {
    if (document.getElementById('radar-sweep-style')) {
      return
    }
    const style = document.createElement('style')
    style.id = 'radar-sweep-style'
    style.textContent = `
      @keyframes radarSweepSpin {
        from { transform: rotate(0deg); }
        to { transform: rotate(360deg); }
      }
    `
    document.head.appendChild(style)
  }, [])

  if (!visible || !projected || !site) {
    return null
  }

  return (
    <div className="pointer-events-none absolute inset-0 overflow-hidden">
      <div
        className="absolute rounded-full"
        style={{
          left: projected.x - diameter / 2,
          top: projected.y - diameter / 2,
          width: diameter,
          height: diameter,
          border: '1px solid rgba(255,255,255,0.08)',
          background:
            'radial-gradient(circle, rgba(0,212,255,0.02) 0%, rgba(0,212,255,0.01) 35%, rgba(0,0,0,0) 70%), conic-gradient(from 0deg, rgba(0,212,255,0.00) 0deg, rgba(0,212,255,0.18) 10deg, rgba(0,212,255,0.08) 24deg, rgba(0,212,255,0.00) 38deg, rgba(0,212,255,0.00) 360deg)',
          animation: 'radarSweepSpin 6s linear infinite',
          mixBlendMode: 'screen',
          opacity: 0.55,
        }}
      />
      <div
        className="absolute rounded-full border border-cyan/10"
        style={{
          left: projected.x - 4,
          top: projected.y - 4,
          width: 8,
          height: 8,
          boxShadow: '0 0 18px rgba(0, 212, 255, 0.35)',
        }}
      />
    </div>
  )
}
