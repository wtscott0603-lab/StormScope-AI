import type { FeatureCollection, Feature, Polygon, Point } from 'geojson'
import { useEffect } from 'react'
import type maplibregl from 'maplibre-gl'

import type { Site } from '../../types/radar'

const RANGE_SOURCE_ID = 'range-rings'
const RANGE_LINE_LAYER_ID = 'range-rings-line'
const RANGE_LABEL_SOURCE_ID = 'range-rings-labels'
const RANGE_LABEL_LAYER_ID = 'range-rings-labels'
const NM_TO_KM = 1.852
const RANGES_NM = [50, 100, 150, 200, 250]

function destinationPoint(lat: number, lon: number, bearingDeg: number, distanceKm: number) {
  const radiusKm = 6371
  const angularDistance = distanceKm / radiusKm
  const bearing = (bearingDeg * Math.PI) / 180
  const lat1 = (lat * Math.PI) / 180
  const lon1 = (lon * Math.PI) / 180
  const lat2 = Math.asin(
    Math.sin(lat1) * Math.cos(angularDistance) +
      Math.cos(lat1) * Math.sin(angularDistance) * Math.cos(bearing),
  )
  const lon2 =
    lon1 +
    Math.atan2(
      Math.sin(bearing) * Math.sin(angularDistance) * Math.cos(lat1),
      Math.cos(angularDistance) - Math.sin(lat1) * Math.sin(lat2),
    )
  return {
    lat: (lat2 * 180) / Math.PI,
    lon: ((lon2 * 180) / Math.PI + 540) % 360 - 180,
  }
}

function rangeRingPolygon(centerLat: number, centerLon: number, radiusKm: number, steps = 180): Feature<Polygon> {
  const coordinates = Array.from({ length: steps + 1 }, (_, index) => {
    const point = destinationPoint(centerLat, centerLon, (index / steps) * 360, radiusKm)
    return [point.lon, point.lat]
  })
  return {
    type: 'Feature',
    geometry: {
      type: 'Polygon',
      coordinates: [coordinates],
    },
    properties: {
      radius_km: radiusKm,
      radius_nm: Math.round(radiusKm / NM_TO_KM),
    },
  }
}

function rangeRingLabel(centerLat: number, centerLon: number, radiusKm: number): Feature<Point> {
  const labelPoint = destinationPoint(centerLat, centerLon, 90, radiusKm)
  return {
    type: 'Feature',
    geometry: {
      type: 'Point',
      coordinates: [labelPoint.lon, labelPoint.lat],
    },
    properties: {
      label: `${Math.round(radiusKm / NM_TO_KM)} nm`,
    },
  }
}

export function RangeRingsOverlay({
  map,
  site,
  visible,
}: {
  map: maplibregl.Map | null
  site: Site | undefined
  visible: boolean
}) {
  useEffect(() => {
    if (!map || !site) {
      return
    }

    const ringCollection: FeatureCollection = {
      type: 'FeatureCollection',
      features: RANGES_NM.map((radiusNm) => rangeRingPolygon(site.lat, site.lon, radiusNm * NM_TO_KM)),
    }
    const labelCollection: FeatureCollection = {
      type: 'FeatureCollection',
      features: RANGES_NM.map((radiusNm) => rangeRingLabel(site.lat, site.lon, radiusNm * NM_TO_KM)),
    }

    if (!map.getSource(RANGE_SOURCE_ID)) {
      map.addSource(RANGE_SOURCE_ID, { type: 'geojson', data: ringCollection })
      map.addSource(RANGE_LABEL_SOURCE_ID, { type: 'geojson', data: labelCollection })
      map.addLayer({
        id: RANGE_LINE_LAYER_ID,
        type: 'line',
        source: RANGE_SOURCE_ID,
        paint: {
          'line-color': '#ffffff33',
          'line-width': 1,
          'line-dasharray': [2, 2],
          'line-opacity': visible ? 0.9 : 0,
        },
      })
      map.addLayer({
        id: RANGE_LABEL_LAYER_ID,
        type: 'symbol',
        source: RANGE_LABEL_SOURCE_ID,
        layout: {
          'text-field': ['get', 'label'],
          'text-font': ['Open Sans Regular'],
          'text-size': 10,
          'text-offset': [0.8, 0],
        },
        paint: {
          'text-color': '#cbd5e1',
          'text-halo-color': '#0a0d0f',
          'text-halo-width': 1,
          'text-opacity': visible ? 0.7 : 0,
        },
      })
      return
    }

    ;(map.getSource(RANGE_SOURCE_ID) as maplibregl.GeoJSONSource).setData(ringCollection)
    ;(map.getSource(RANGE_LABEL_SOURCE_ID) as maplibregl.GeoJSONSource).setData(labelCollection)
    map.setPaintProperty(RANGE_LINE_LAYER_ID, 'line-opacity', visible ? 0.9 : 0)
    map.setPaintProperty(RANGE_LABEL_LAYER_ID, 'text-opacity', visible ? 0.7 : 0)
  }, [map, site, visible])

  return null
}
