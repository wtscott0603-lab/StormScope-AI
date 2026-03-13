import type { Feature, FeatureCollection, Geometry } from 'geojson'
import { memo, useEffect, useMemo } from 'react'
import type maplibregl from 'maplibre-gl'

import type { StormSummary } from '../../types/storms'

const FOOTPRINT_SOURCE_ID = 'storm-footprints'
const FOOTPRINT_FILL_LAYER_ID = 'storm-footprints-fill'
const FOOTPRINT_LINE_LAYER_ID = 'storm-footprints-line'
const FORECAST_SOURCE_ID = 'storm-forecast'
const FORECAST_LAYER_ID = 'storm-forecast-line'
const CONE_SOURCE_ID = 'storm-uncertainty-cone'
const CONE_FILL_LAYER_ID = 'storm-cone-fill'
const CONE_LINE_LAYER_ID = 'storm-cone-line'
const TRAIL_SOURCE_ID = 'storm-trail'
const TRAIL_LINE_LAYER_ID = 'storm-trail-line'
const TRAIL_POINT_LAYER_ID = 'storm-trail-points'
const CENTROID_SOURCE_ID = 'storm-centroids'
const CENTROID_LAYER_ID = 'storm-centroids-layer'
const LABEL_LAYER_ID = 'storm-centroids-label'

export const StormsOverlay = memo(function StormsOverlay({
  map,
  storms,
  visible,
  selectedStormId,
  selectedStormTrack,
  showTrails,
  onSelectStorm,
}: {
  map: maplibregl.Map | null
  storms: StormSummary[]
  visible: boolean
  selectedStormId: string | null
  selectedStormTrack: Array<{
    scan_time: string
    centroid_lat: number
    centroid_lon: number
    trend: string
  }>
  showTrails: boolean
  onSelectStorm: (stormId: string) => void
}) {
  useEffect(() => {
    if (!map) {
      return
    }

    if (map.getSource(FOOTPRINT_SOURCE_ID)) {
      return
    }

    map.addSource(FOOTPRINT_SOURCE_ID, {
      type: 'geojson',
      data: { type: 'FeatureCollection', features: [] },
    })
    map.addSource(FORECAST_SOURCE_ID, {
      type: 'geojson',
      data: { type: 'FeatureCollection', features: [] },
    })
    map.addSource(CONE_SOURCE_ID, {
      type: 'geojson',
      data: { type: 'FeatureCollection', features: [] },
    })
    map.addSource(TRAIL_SOURCE_ID, {
      type: 'geojson',
      data: { type: 'FeatureCollection', features: [] },
    })
    map.addSource(CENTROID_SOURCE_ID, {
      type: 'geojson',
      data: { type: 'FeatureCollection', features: [] },
    })

    map.addLayer({
      id: FOOTPRINT_FILL_LAYER_ID,
      type: 'fill',
      source: FOOTPRINT_SOURCE_ID,
      paint: {
        'fill-color': [
          'match',
          ['get', 'severity_level'],
          'TORNADO_EMERGENCY',
          '#ff4dff',
          'TORNADO',
          '#ff5b5b',
          'SEVERE',
          '#ffb347',
          'MARGINAL',
          '#f4e04d',
          '#7d8d93',
        ],
        'fill-opacity': visible ? 0.09 : 0,
      },
    })
    map.addLayer({
      id: FOOTPRINT_LINE_LAYER_ID,
      type: 'line',
      source: FOOTPRINT_SOURCE_ID,
      paint: {
        'line-color': [
          'match',
          ['get', 'severity_level'],
          'TORNADO_EMERGENCY',
          '#ff4dff',
          'TORNADO',
          '#ff5b5b',
          'SEVERE',
          '#ffb347',
          'MARGINAL',
          '#f4e04d',
          '#7d8d93',
        ],
        'line-width': [
          'case',
          ['==', ['get', 'selected'], 1],
          3,
          1.8,
        ],
        'line-opacity': visible ? 0.95 : 0,
      },
    })
    map.addLayer({
      id: FORECAST_LAYER_ID,
      type: 'line',
      source: FORECAST_SOURCE_ID,
      paint: {
        'line-color': [
          'match',
          ['get', 'severity_level'],
          'TORNADO_EMERGENCY',
          '#ff4dff',
          'TORNADO',
          '#ff5b5b',
          'SEVERE',
          '#ffb347',
          'MARGINAL',
          '#f4e04d',
          '#7d8d93',
        ],
        'line-width': 2,
        'line-dasharray': [2, 2],
        'line-opacity': visible ? 0.82 : 0,
      },
    })
    // Uncertainty cone — semi-transparent polygon widening ahead of the storm
    map.addLayer({
      id: CONE_FILL_LAYER_ID,
      type: 'fill',
      source: CONE_SOURCE_ID,
      paint: {
        'fill-color': '#93c5fd',
        'fill-opacity': visible ? 0.12 : 0,
      },
    })
    map.addLayer({
      id: CONE_LINE_LAYER_ID,
      type: 'line',
      source: CONE_SOURCE_ID,
      paint: {
        'line-color': '#60a5fa',
        'line-width': 1,
        'line-dasharray': [3, 3],
        'line-opacity': visible ? 0.45 : 0,
      },
    })
    map.addLayer({
      id: TRAIL_LINE_LAYER_ID,
      type: 'line',
      source: TRAIL_SOURCE_ID,
      paint: {
        'line-color': '#7dd3fc',
        'line-width': 2,
        'line-opacity': showTrails && visible ? 0.7 : 0,
      },
    })
    map.addLayer({
      id: TRAIL_POINT_LAYER_ID,
      type: 'circle',
      source: TRAIL_SOURCE_ID,
      filter: ['==', ['geometry-type'], 'Point'],
      paint: {
        'circle-radius': 3.5,
        'circle-color': [
          'match',
          ['get', 'trend'],
          'strengthening',
          '#ef4444',
          'weakening',
          '#f59e0b',
          '#67e8f9',
        ],
        'circle-stroke-width': 1,
        'circle-stroke-color': '#0a0d0f',
        'circle-opacity': showTrails && visible ? 0.95 : 0,
      },
    })
    map.addLayer({
      id: CENTROID_LAYER_ID,
      type: 'circle',
      source: CENTROID_SOURCE_ID,
      paint: {
        'circle-color': [
          'match',
          ['get', 'severity_level'],
          'TORNADO_EMERGENCY',
          '#ff4dff',
          'TORNADO',
          '#ff5b5b',
          'SEVERE',
          '#ffb347',
          'MARGINAL',
          '#f4e04d',
          '#7d8d93',
        ],
        'circle-radius': [
          'case',
          ['==', ['get', 'selected'], 1],
          8,
          6,
        ],
        'circle-stroke-width': 1.5,
        'circle-stroke-color': '#0a0d0f',
        'circle-opacity': visible ? 0.95 : 0,
      },
    })
    map.addLayer({
      id: LABEL_LAYER_ID,
      type: 'symbol',
      source: CENTROID_SOURCE_ID,
      layout: {
        'text-field': ['get', 'label'],
        'text-size': 10,
        'text-font': ['Open Sans Regular'],
        'text-offset': [0, 1.3],
      },
      paint: {
        'text-color': '#dce6ea',
        'text-halo-color': '#0a0d0f',
        'text-halo-width': 1,
        'text-opacity': visible ? 0.9 : 0,
      },
    })

    const clickHandler = (event: maplibregl.MapLayerMouseEvent) => {
      const feature = event.features?.[0]
      const stormId = feature?.properties?.storm_id
      if (stormId) {
        onSelectStorm(String(stormId))
      }
    }
    const enterHandler = () => {
      map.getCanvas().style.cursor = 'pointer'
    }
    const leaveHandler = () => {
      map.getCanvas().style.cursor = ''
    }

    map.on('click', FOOTPRINT_FILL_LAYER_ID, clickHandler)
    map.on('click', CENTROID_LAYER_ID, clickHandler)
    map.on('mouseenter', FOOTPRINT_FILL_LAYER_ID, enterHandler)
    map.on('mouseleave', FOOTPRINT_FILL_LAYER_ID, leaveHandler)
    map.on('mouseenter', CENTROID_LAYER_ID, enterHandler)
    map.on('mouseleave', CENTROID_LAYER_ID, leaveHandler)

    return () => {
      map.off('click', FOOTPRINT_FILL_LAYER_ID, clickHandler)
      map.off('click', CENTROID_LAYER_ID, clickHandler)
      map.off('mouseenter', FOOTPRINT_FILL_LAYER_ID, enterHandler)
      map.off('mouseleave', FOOTPRINT_FILL_LAYER_ID, leaveHandler)
      map.off('mouseenter', CENTROID_LAYER_ID, enterHandler)
      map.off('mouseleave', CENTROID_LAYER_ID, leaveHandler)
    }
  }, [map, onSelectStorm])

  // Memoize all GeoJSON builds so they only run when the underlying data changes,
  // not on every parent rerender.
  const footprintData = useMemo<FeatureCollection>(() => ({
    type: 'FeatureCollection',
    features: storms.map((storm) => ({
      type: 'Feature',
      geometry: storm.footprint_geojson as Geometry,
      properties: {
        storm_id: storm.storm_id,
        severity_level: storm.severity_level,
        primary_threat: storm.primary_threat.toUpperCase(),
        selected: storm.storm_id === selectedStormId ? 1 : 0,
      },
    })),
  }), [storms, selectedStormId])

  const forecastData = useMemo<FeatureCollection>(() => ({
    type: 'FeatureCollection',
    features: storms
      .filter((storm) => storm.forecast_path.length > 1)
      .map((storm) => ({
        type: 'Feature',
        geometry: {
          type: 'LineString',
          coordinates: [
            [storm.centroid_lon, storm.centroid_lat],
            ...storm.forecast_path.map((point) => [point.lon, point.lat]),
          ],
        },
        properties: {
          storm_id: storm.storm_id,
          severity_level: storm.severity_level,
        },
      })),
  }), [storms])

  const coneData = useMemo<FeatureCollection>(() => {
    // Only render the cone for the selected storm (if it has one)
    const selected = storms.find((s) => s.storm_id === selectedStormId)
    if (!selected || !selected.uncertainty_cone || selected.uncertainty_cone.length < 3) {
      return { type: 'FeatureCollection', features: [] }
    }
    const cone = selected.uncertainty_cone
    // Build a polygon from the left edge (forward) + right edge (reversed)
    const leftCoords = cone.map((step: { left: { lon: number; lat: number } }) => [step.left.lon, step.left.lat])
    const rightCoords = [...cone].reverse().map((step: { right: { lon: number; lat: number } }) => [step.right.lon, step.right.lat])
    return {
      type: 'FeatureCollection',
      features: [{
        type: 'Feature',
        geometry: {
          type: 'Polygon',
          coordinates: [[
            [selected.centroid_lon, selected.centroid_lat],
            ...leftCoords,
            ...rightCoords,
            [selected.centroid_lon, selected.centroid_lat],
          ]],
        },
        properties: { storm_id: selected.storm_id },
      }],
    }
  }, [storms, selectedStormId])

  const centroidData = useMemo<FeatureCollection>(() => ({
    type: 'FeatureCollection',
    features: storms.map((storm) => ({
      type: 'Feature',
      geometry: {
        type: 'Point',
        coordinates: [storm.centroid_lon, storm.centroid_lat],
      },
      properties: {
        storm_id: storm.storm_id,
        severity_level: storm.severity_level,
        label: `${storm.primary_threat.toUpperCase()} ${storm.max_reflectivity.toFixed(0)} dBZ`,
        selected: storm.storm_id === selectedStormId ? 1 : 0,
      },
    })),
  }), [storms, selectedStormId])

  const trailFeatures = useMemo<FeatureCollection>(() => {
    const selectedStorm = storms.find((storm) => storm.storm_id === selectedStormId) ?? null
    const items: Feature<Geometry>[] =
      showTrails && selectedStorm && selectedStormTrack.length > 1
        ? [
            {
              type: 'Feature',
              geometry: {
                type: 'LineString',
                coordinates: selectedStormTrack.map((point) => [point.centroid_lon, point.centroid_lat]),
              },
              properties: {
                storm_id: selectedStorm.storm_id,
                severity_level: selectedStorm.severity_level,
              },
            },
            ...selectedStormTrack.map(
              (point, index): Feature<Geometry> => ({
                type: 'Feature',
                geometry: {
                  type: 'Point',
                  coordinates: [point.centroid_lon, point.centroid_lat],
                },
                properties: {
                  storm_id: selectedStorm.storm_id,
                  index,
                  trend: point.trend,
                },
              }),
            ),
          ]
        : []
    return { type: 'FeatureCollection', features: items }
  }, [storms, selectedStormId, selectedStormTrack, showTrails])

  useEffect(() => {
    if (!map || !map.getSource(FOOTPRINT_SOURCE_ID) || !map.getSource(FORECAST_SOURCE_ID) || !map.getSource(CENTROID_SOURCE_ID) || !map.getSource(TRAIL_SOURCE_ID) || !map.getSource(CONE_SOURCE_ID)) {
      return
    }

    ;(map.getSource(FOOTPRINT_SOURCE_ID) as maplibregl.GeoJSONSource).setData(footprintData)
    ;(map.getSource(FORECAST_SOURCE_ID) as maplibregl.GeoJSONSource).setData(forecastData)
    ;(map.getSource(CONE_SOURCE_ID) as maplibregl.GeoJSONSource).setData(coneData)
    ;(map.getSource(TRAIL_SOURCE_ID) as maplibregl.GeoJSONSource).setData(trailFeatures)
    ;(map.getSource(CENTROID_SOURCE_ID) as maplibregl.GeoJSONSource).setData(centroidData)
    map.setPaintProperty(FOOTPRINT_FILL_LAYER_ID, 'fill-opacity', visible ? 0.09 : 0)
    map.setPaintProperty(FOOTPRINT_LINE_LAYER_ID, 'line-opacity', visible ? 0.95 : 0)
    map.setPaintProperty(TRAIL_LINE_LAYER_ID, 'line-opacity', showTrails && visible ? 0.7 : 0)
    map.setPaintProperty(TRAIL_POINT_LAYER_ID, 'circle-opacity', showTrails && visible ? 0.95 : 0)
    map.setPaintProperty(FORECAST_LAYER_ID, 'line-opacity', visible ? 0.82 : 0)
    map.setPaintProperty(CONE_FILL_LAYER_ID, 'fill-opacity', visible && selectedStormId ? 0.12 : 0)
    map.setPaintProperty(CONE_LINE_LAYER_ID, 'line-opacity', visible && selectedStormId ? 0.45 : 0)
    map.setPaintProperty(CENTROID_LAYER_ID, 'circle-opacity', visible ? 0.95 : 0)
    map.setPaintProperty(LABEL_LAYER_ID, 'text-opacity', visible ? 0.9 : 0)
  }, [map, footprintData, forecastData, coneData, trailFeatures, centroidData, visible, showTrails, selectedStormId])

  return null
}
)
