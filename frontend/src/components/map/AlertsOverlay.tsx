import type { FeatureCollection } from 'geojson'
import { memo, useEffect } from 'react'
import type maplibregl from 'maplibre-gl'

import type { Alert } from '../../types/radar'

const SOURCE_ID = 'alerts-source'
const FILL_LAYER_ID = 'alerts-fill'
const LINE_LAYER_ID = 'alerts-line'

export const AlertsOverlay = memo(function AlertsOverlay({
  map,
  alerts,
  visible,
}: {
  map: maplibregl.Map | null
  alerts: Alert[]
  visible: boolean
}) {
  useEffect(() => {
    if (!map) {
      return
    }

    const data: FeatureCollection = {
      type: 'FeatureCollection',
      features: alerts.map((alert) => ({
        type: 'Feature',
        geometry: alert.geometry,
        properties: {
          id: alert.id,
          event: alert.event,
          severity: alert.severity,
        },
      })),
    }

    if (!map.getSource(SOURCE_ID)) {
      map.addSource(SOURCE_ID, { type: 'geojson', data })
      map.addLayer({
        id: FILL_LAYER_ID,
        type: 'fill',
        source: SOURCE_ID,
        paint: {
          'fill-color': [
            'match',
            ['get', 'severity'],
            'Extreme',
            '#ff5c5c',
            'Severe',
            '#ff7a2f',
            'Moderate',
            '#ffb84d',
            '#00d4ff',
          ],
          'fill-opacity': visible ? 0.12 : 0,
        },
      })
      map.addLayer({
        id: LINE_LAYER_ID,
        type: 'line',
        source: SOURCE_ID,
        paint: {
          'line-color': [
            'match',
            ['get', 'severity'],
            'Extreme',
            '#ff5c5c',
            'Severe',
            '#ff7a2f',
            'Moderate',
            '#ffb84d',
            '#00d4ff',
          ],
          'line-width': 2,
          'line-opacity': visible ? 0.9 : 0,
        },
      })
      return
    }

    const source = map.getSource(SOURCE_ID) as maplibregl.GeoJSONSource
    source.setData(data)
    map.setPaintProperty(FILL_LAYER_ID, 'fill-opacity', visible ? 0.12 : 0)
    map.setPaintProperty(LINE_LAYER_ID, 'line-opacity', visible ? 0.9 : 0)
  }, [alerts, map, visible])

  return null
}
)
