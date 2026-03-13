import { useEffect } from 'react'
import type maplibregl from 'maplibre-gl'

import { apiUrl } from '../../api/client'
import type { Frame } from '../../types/radar'

function coordinates(frame: Frame) {
  return [
    [frame.bbox.min_lon, frame.bbox.max_lat],
    [frame.bbox.max_lon, frame.bbox.max_lat],
    [frame.bbox.max_lon, frame.bbox.min_lat],
    [frame.bbox.min_lon, frame.bbox.min_lat],
  ] as [[number, number], [number, number], [number, number], [number, number]]
}

export function RadarOverlay({
  map,
  frame,
  opacity,
  nextFrame,
}: {
  map: maplibregl.Map | null
  frame: Frame | null
  opacity: number
  nextFrame: Frame | null
}) {
  useEffect(() => {
    if (!nextFrame) {
      return
    }
    const image = new Image()
    image.src = apiUrl(nextFrame.url)
  }, [nextFrame])

  useEffect(() => {
    if (!map) {
      return
    }
    if (!map.getLayer('radar-layer') && frame) {
      map.addSource('radar', {
        type: 'image',
        url: apiUrl(frame.url),
        coordinates: coordinates(frame),
      })
      map.addLayer({
        id: 'radar-layer',
        type: 'raster',
        source: 'radar',
        paint: {
          'raster-opacity': opacity,
          'raster-fade-duration': 0,
        },
      })
      return
    }
    if (!frame) {
      if (map.getLayer('radar-layer')) {
        map.removeLayer('radar-layer')
      }
      if (map.getSource('radar')) {
        map.removeSource('radar')
      }
      return
    }

    const source = map.getSource('radar') as maplibregl.ImageSource | undefined
    const update = () => {
      if (!source) {
        return
      }
      source.updateImage({
        url: apiUrl(frame.url),
        coordinates: coordinates(frame),
      })
      map.setPaintProperty('radar-layer', 'raster-opacity', opacity)
    }

    const preload = new Image()
    preload.onload = update
    preload.src = apiUrl(frame.url)

    return () => {
      preload.onload = null
    }
  }, [frame, map, opacity])

  useEffect(() => {
    if (map?.getLayer('radar-layer')) {
      map.setPaintProperty('radar-layer', 'raster-opacity', opacity)
    }
  }, [map, opacity])

  return null
}
