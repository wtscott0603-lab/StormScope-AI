import type { FeatureCollection } from 'geojson'
import { memo, useEffect } from 'react'
import type maplibregl from 'maplibre-gl'
import type { OverlayFeatureCollection } from '../../types/storms'

// ── Source / layer IDs ────────────────────────────────────────────────────
const SRC = {
  spc: 'spc-d1',
  spcD2: 'spc-d2',
  spcD3: 'spc-d3',
  md: 'md-overlays',
  lsr: 'lsr-overlays',
  watch: 'watch-overlays',
}

// ── NWS-standard SPC categorical fill colors ─────────────────────────────
const SPC_FILL_COLOR = [
  'match', ['get', 'category'],
  'General Thunderstorms Risk', '#c1e9c1',
  'TSTM',                       '#c1e9c1',
  'Marginal Risk',              '#66a366',
  'MRGL',                       '#66a366',
  'Slight Risk',                '#f6f67b',
  'SLGT',                       '#f6f67b',
  'Enhanced Risk',              '#e6b366',
  'ENH',                        '#e6b366',
  'Moderate Risk',              '#e66666',
  'MDT',                        '#e66666',
  'High Risk',                  '#ff66ff',
  'HIGH',                       '#ff66ff',
  /* fallback — use stroke colour from payload or neutral */
  ['coalesce', ['get', 'stroke'], '#778899'],
] as unknown as maplibregl.ExpressionSpecification

const SPC_STROKE_COLOR = [
  'match', ['get', 'category'],
  'General Thunderstorms Risk', '#00cc00',
  'TSTM',                       '#00cc00',
  'Marginal Risk',              '#006600',
  'MRGL',                       '#006600',
  'Slight Risk',                '#c8c800',
  'SLGT',                       '#c8c800',
  'Enhanced Risk',              '#c78c2c',
  'ENH',                        '#c78c2c',
  'Moderate Risk',              '#c83228',
  'MDT',                        '#c83228',
  'High Risk',                  '#cc00cc',
  'HIGH',                       '#cc00cc',
  ['coalesce', ['get', 'stroke'], '#aabbcc'],
] as unknown as maplibregl.ExpressionSpecification

function toCollection(payload: OverlayFeatureCollection): FeatureCollection {
  return {
    type: 'FeatureCollection',
    features: ((payload.features ?? []) as unknown) as FeatureCollection['features'],
  }
}

function addSpcLayers(
  map: maplibregl.Map,
  sourceId: string,
  data: FeatureCollection,
  fillOpacity: number,
  lineOpacity: number,
  dashed: boolean,
) {
  if (!map.getSource(sourceId)) {
    map.addSource(sourceId, { type: 'geojson', data })
    map.addLayer({
      id: `${sourceId}-fill`,
      type: 'fill',
      source: sourceId,
      filter: ['==', ['get', 'overlay_subtype'], 'categorical'],
      paint: {
        'fill-color': SPC_FILL_COLOR,
        'fill-opacity': fillOpacity,
      },
    })
    map.addLayer({
      id: `${sourceId}-line`,
      type: 'line',
      source: sourceId,
      filter: ['==', ['get', 'overlay_subtype'], 'categorical'],
      paint: {
        'line-color': SPC_STROKE_COLOR,
        'line-width': 1.8,
        'line-opacity': lineOpacity,
        ...(dashed ? { 'line-dasharray': [4, 3] } : {}),
      },
    })
  } else {
    const src = map.getSource(sourceId) as maplibregl.GeoJSONSource
    src.setData(data)
    map.setPaintProperty(`${sourceId}-fill`, 'fill-opacity', fillOpacity)
    map.setPaintProperty(`${sourceId}-line`, 'line-opacity', lineOpacity)
  }
}

export const OperationalOverlays = memo(function OperationalOverlays({
  map,
  spc,
  spcDay2,
  spcDay3,
  md,
  lsr,
  watch,
  showSpc,
  showSpcDay2,
  showSpcDay3,
  showWatchBoxes,
  showMesoscaleDiscussions,
  showLocalStormReports,
}: {
  map: maplibregl.Map | null
  spc: OverlayFeatureCollection
  spcDay2: OverlayFeatureCollection
  spcDay3: OverlayFeatureCollection
  md: OverlayFeatureCollection
  lsr: OverlayFeatureCollection
  watch: OverlayFeatureCollection
  showSpc: boolean
  showSpcDay2: boolean
  showSpcDay3: boolean
  showWatchBoxes: boolean
  showMesoscaleDiscussions: boolean
  showLocalStormReports: boolean
}) {
  // ── Day 1 SPC ─────────────────────────────────────────────────────────
  useEffect(() => {
    if (!map) return
    addSpcLayers(map, SRC.spc, toCollection(spc), showSpc ? 0.22 : 0, showSpc ? 0.9 : 0, false)
  }, [map, spc, showSpc])

  // ── Day 2 SPC ─────────────────────────────────────────────────────────
  useEffect(() => {
    if (!map) return
    addSpcLayers(map, SRC.spcD2, toCollection(spcDay2), showSpcDay2 ? 0.15 : 0, showSpcDay2 ? 0.75 : 0, true)
  }, [map, spcDay2, showSpcDay2])

  // ── Day 3 SPC ─────────────────────────────────────────────────────────
  useEffect(() => {
    if (!map) return
    addSpcLayers(map, SRC.spcD3, toCollection(spcDay3), showSpcDay3 ? 0.10 : 0, showSpcDay3 ? 0.6 : 0, true)
  }, [map, spcDay3, showSpcDay3])

  // ── Mesoscale discussions ──────────────────────────────────────────────
  useEffect(() => {
    if (!map) return
    const data = toCollection(md)
    if (!map.getSource(SRC.md)) {
      map.addSource(SRC.md, { type: 'geojson', data })
      map.addLayer({
        id: `${SRC.md}-line`,
        type: 'line',
        source: SRC.md,
        paint: {
          'line-color': '#e0aa00',
          'line-width': 2,
          'line-dasharray': [6, 3],
          'line-opacity': showMesoscaleDiscussions ? 0.9 : 0,
        },
      })
    } else {
      ;(map.getSource(SRC.md) as maplibregl.GeoJSONSource).setData(data)
      map.setPaintProperty(`${SRC.md}-line`, 'line-opacity', showMesoscaleDiscussions ? 0.9 : 0)
    }
  }, [map, md, showMesoscaleDiscussions])

  // ── Local storm reports ───────────────────────────────────────────────
  useEffect(() => {
    if (!map) return
    const data = toCollection(lsr)
    if (!map.getSource(SRC.lsr)) {
      map.addSource(SRC.lsr, { type: 'geojson', data })
      map.addLayer({
        id: `${SRC.lsr}-pts`,
        type: 'circle',
        source: SRC.lsr,
        paint: {
          'circle-radius': 5,
          'circle-color': '#e55',
          'circle-stroke-color': '#fff',
          'circle-stroke-width': 1,
          'circle-opacity': showLocalStormReports ? 0.9 : 0,
          'circle-stroke-opacity': showLocalStormReports ? 0.9 : 0,
        },
      })
    } else {
      ;(map.getSource(SRC.lsr) as maplibregl.GeoJSONSource).setData(data)
      map.setPaintProperty(`${SRC.lsr}-pts`, 'circle-opacity', showLocalStormReports ? 0.9 : 0)
      map.setPaintProperty(`${SRC.lsr}-pts`, 'circle-stroke-opacity', showLocalStormReports ? 0.9 : 0)
    }
  }, [map, lsr, showLocalStormReports])

  // ── Watch boxes ───────────────────────────────────────────────────────
  useEffect(() => {
    if (!map) return
    const data = toCollection(watch)
    const watchColor = [
      'match', ['get', 'watch_type'],
      'Tornado Watch', '#ff3333',
      'Severe Thunderstorm Watch', '#ffa500',
      '#aaa',
    ] as unknown as maplibregl.ExpressionSpecification
    if (!map.getSource(SRC.watch)) {
      map.addSource(SRC.watch, { type: 'geojson', data })
      map.addLayer({
        id: `${SRC.watch}-fill`,
        type: 'fill',
        source: SRC.watch,
        paint: {
          'fill-color': watchColor,
          'fill-opacity': showWatchBoxes ? 0.08 : 0,
        },
      })
      map.addLayer({
        id: `${SRC.watch}-line`,
        type: 'line',
        source: SRC.watch,
        paint: {
          'line-color': watchColor,
          'line-width': 2,
          'line-dasharray': [5, 3],
          'line-opacity': showWatchBoxes ? 0.9 : 0,
        },
      })
    } else {
      ;(map.getSource(SRC.watch) as maplibregl.GeoJSONSource).setData(data)
      map.setPaintProperty(`${SRC.watch}-fill`, 'fill-opacity', showWatchBoxes ? 0.08 : 0)
      map.setPaintProperty(`${SRC.watch}-line`, 'line-opacity', showWatchBoxes ? 0.9 : 0)
    }
  }, [map, watch, showWatchBoxes])

  return null
}
)
