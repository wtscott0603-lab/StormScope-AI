import type { FeatureCollection } from 'geojson'
import { useEffect } from 'react'
import type maplibregl from 'maplibre-gl'


const SOURCE_ID = 'cross-section-selection'
const LINE_LAYER_ID = 'cross-section-selection-line'
const POINT_LAYER_ID = 'cross-section-selection-points'


export function CrossSectionOverlay({
  map,
  points,
}: {
  map: maplibregl.Map | null
  points: Array<{ lat: number; lon: number }>
}) {
  useEffect(() => {
    if (!map) {
      return
    }
    const features: FeatureCollection = {
      type: 'FeatureCollection',
      features: [
        ...(points.length > 1
          ? [
              {
                type: 'Feature' as const,
                geometry: {
                  type: 'LineString' as const,
                  coordinates: points.map((point) => [point.lon, point.lat]),
                },
                properties: {},
              },
            ]
          : []),
        ...points.map((point, index) => ({
          type: 'Feature' as const,
          geometry: {
            type: 'Point' as const,
            coordinates: [point.lon, point.lat],
          },
          properties: {
            label: index === 0 ? 'A' : 'B',
          },
        })),
      ],
    }

    if (!map.getSource(SOURCE_ID)) {
      map.addSource(SOURCE_ID, { type: 'geojson', data: features })
      map.addLayer({
        id: LINE_LAYER_ID,
        type: 'line',
        source: SOURCE_ID,
        filter: ['==', ['geometry-type'], 'LineString'],
        paint: {
          'line-color': '#22d3ee',
          'line-width': 2,
          'line-dasharray': [2, 2],
          'line-opacity': points.length > 1 ? 0.85 : 0,
        },
      })
      map.addLayer({
        id: POINT_LAYER_ID,
        type: 'circle',
        source: SOURCE_ID,
        filter: ['==', ['geometry-type'], 'Point'],
        paint: {
          'circle-radius': 6,
          'circle-color': '#67e8f9',
          'circle-stroke-color': '#0a0d0f',
          'circle-stroke-width': 2,
          'circle-opacity': points.length ? 0.95 : 0,
        },
      })
      return
    }

    ;(map.getSource(SOURCE_ID) as maplibregl.GeoJSONSource).setData(features)
    map.setPaintProperty(LINE_LAYER_ID, 'line-opacity', points.length > 1 ? 0.85 : 0)
    map.setPaintProperty(POINT_LAYER_ID, 'circle-opacity', points.length ? 0.95 : 0)
  }, [map, points])

  return null
}
