import maplibregl from 'maplibre-gl'
import 'maplibre-gl/dist/maplibre-gl.css'
import { useEffect, useRef, useState } from 'react'

import { apiUrl } from '../../api/client'
import type { Alert, Frame, SignatureMarker, Site } from '../../types/radar'
import { AlertsOverlay } from './AlertsOverlay'
import { CrossSectionOverlay } from './CrossSectionOverlay'
import { MetarOverlay } from './MetarOverlay'
import { OperationalOverlays } from './OperationalOverlays'
import { RadarOverlay } from './RadarOverlay'
import { RadarSweepOverlay } from './RadarSweepOverlay'
import { RangeRingsOverlay } from './RangeRingsOverlay'
import { SavedLocationsOverlay } from './SavedLocationsOverlay'
import { SignaturesOverlay } from './SignaturesOverlay'
import { SiteMarker } from './SiteMarker'
import { StormsOverlay } from './StormsOverlay'
import type { MetarObservation, OverlayFeatureCollection, SavedLocation, StormSummary } from '../../types/storms'

const COUNTY_SOURCE_ID = 'county-lines'
const COUNTY_LAYER_ID = 'county-line-layer'
const STATE_SOURCE_ID = 'state-lines'
const STATE_LAYER_ID = 'state-line-layer'
const COUNTY_GEOJSON_URL = apiUrl('/static/geo/us_counties.geojson')
const STATE_GEOJSON_URL = apiUrl('/static/geo/us_states.geojson')

/** All props accepted by RadarMap. Keeping this as a named interface makes
 *  it easier to extend and reduces risk of destructuring/type drift. */
export interface RadarMapProps {
  tileUrl: string
  tileAttribution: string
  defaultCenter: { lat: number; lon: number }
  defaultZoom: number
  currentFrame: Frame | null
  nextFrame: Frame | null
  alerts: Alert[]
  sites: Site[]
  selectedSite?: Site
  opacity: number
  showAlerts: boolean
  showSiteMarkers: boolean
  showCountyLines: boolean
  showRangeRings: boolean
  showSweepAnimation: boolean
  signatures: SignatureMarker[]
  showSignatures: boolean
  storms: StormSummary[]
  showStorms: boolean
  selectedStormId: string | null
  savedLocations: SavedLocation[]
  showSavedLocations: boolean
  metarObservations: MetarObservation[]
  showMetars: boolean
  spcOverlays: OverlayFeatureCollection
  spcDay2Overlays: OverlayFeatureCollection
  spcDay3Overlays: OverlayFeatureCollection
  mdOverlays: OverlayFeatureCollection
  lsrOverlays: OverlayFeatureCollection
  watchOverlays: OverlayFeatureCollection
  showSpcOutlooks: boolean
  showSpcDay2Outlooks: boolean
  showSpcDay3Outlooks: boolean
  showWatchBoxes: boolean
  showMesoscaleDiscussions: boolean
  showLocalStormReports: boolean
  selectedStormTrack: Array<{
    scan_time: string
    centroid_lat: number
    centroid_lon: number
    trend: string
  }>
  showStormTrails: boolean
  crossSectionPoints: Array<{ lat: number; lon: number }>
  crossSectionSelectionActive: boolean
  onAddCrossSectionPoint: (point: { lat: number; lon: number }) => void
  onSelectSite: (siteId: string) => void
  onSelectStorm: (stormId: string) => void
}

export function RadarMap({
  tileUrl,
  tileAttribution,
  defaultCenter,
  defaultZoom,
  currentFrame,
  nextFrame,
  alerts,
  sites,
  selectedSite,
  opacity,
  showAlerts,
  showSiteMarkers,
  showCountyLines,
  showRangeRings,
  showSweepAnimation,
  signatures,
  showSignatures,
  storms,
  showStorms,
  selectedStormId,
  savedLocations,
  showSavedLocations,
  metarObservations,
  showMetars,
  spcOverlays,
  spcDay2Overlays,
  spcDay3Overlays,
  mdOverlays,
  lsrOverlays,
  watchOverlays,
  showSpcOutlooks,
  showSpcDay2Outlooks,
  showSpcDay3Outlooks,
  showWatchBoxes,
  showMesoscaleDiscussions,
  showLocalStormReports,
  selectedStormTrack,
  showStormTrails,
  crossSectionPoints,
  crossSectionSelectionActive,
  onAddCrossSectionPoint,
  onSelectSite,
  onSelectStorm,
}: RadarMapProps) {
  const containerRef = useRef<HTMLDivElement | null>(null)
  const mapRef = useRef<maplibregl.Map | null>(null)
  const skipInitialSiteFocusRef = useRef(true)
  const [mapLoaded, setMapLoaded] = useState(false)

  useEffect(() => {
    if (!containerRef.current || mapRef.current) {
      return
    }

    const map = new maplibregl.Map({
      container: containerRef.current,
      style: {
        version: 8,
        sources: {
          basemap: {
            type: 'raster',
            tiles: [tileUrl],
            tileSize: 256,
            attribution: tileAttribution,
          },
        },
        layers: [
          {
            id: 'basemap',
            type: 'raster',
            source: 'basemap',
          },
        ],
      },
      center: [defaultCenter.lon, defaultCenter.lat],
      zoom: defaultZoom,
    })

    map.addControl(new maplibregl.NavigationControl({ showCompass: false }), 'bottom-right')
    map.on('load', () => setMapLoaded(true))
    mapRef.current = map

    return () => {
      map.remove()
      mapRef.current = null
    }
  }, [defaultCenter.lat, defaultCenter.lon, defaultZoom, tileAttribution, tileUrl])

  useEffect(() => {
    if (!mapLoaded || !mapRef.current || !selectedSite) {
      return
    }
    if (skipInitialSiteFocusRef.current) {
      skipInitialSiteFocusRef.current = false
      return
    }
    mapRef.current.flyTo({
      center: [selectedSite.lon, selectedSite.lat],
      zoom: 6.4,
      speed: 0.7,
    })
  }, [mapLoaded, selectedSite])

  useEffect(() => {
    if (!mapLoaded || !mapRef.current) {
      return
    }
    if (showCountyLines && !mapRef.current.getSource(COUNTY_SOURCE_ID)) {
      mapRef.current.addSource(COUNTY_SOURCE_ID, {
        type: 'geojson',
        data: COUNTY_GEOJSON_URL,
      })
      mapRef.current.addSource(STATE_SOURCE_ID, {
        type: 'geojson',
        data: STATE_GEOJSON_URL,
      })
      mapRef.current.addLayer({
        id: COUNTY_LAYER_ID,
        type: 'line',
        source: COUNTY_SOURCE_ID,
        paint: {
          'line-color': '#5f737e',
          'line-opacity': 0.45,
          'line-width': 0.6,
        },
      })
      mapRef.current.addLayer({
        id: STATE_LAYER_ID,
        type: 'line',
        source: STATE_SOURCE_ID,
        paint: {
          'line-color': '#90a4ae',
          'line-opacity': 0.6,
          'line-width': 1.1,
        },
      })
      return
    }
    if (mapRef.current.getLayer(COUNTY_LAYER_ID)) {
      mapRef.current.setLayoutProperty(COUNTY_LAYER_ID, 'visibility', showCountyLines ? 'visible' : 'none')
    }
    if (mapRef.current.getLayer(STATE_LAYER_ID)) {
      mapRef.current.setLayoutProperty(STATE_LAYER_ID, 'visibility', showCountyLines ? 'visible' : 'none')
    }
  }, [mapLoaded, showCountyLines])

  useEffect(() => {
    if (!mapLoaded || !mapRef.current) {
      return
    }
    const map = mapRef.current
    const clickHandler = (event: maplibregl.MapMouseEvent) => {
      if (!crossSectionSelectionActive) {
        return
      }
      onAddCrossSectionPoint({
        lat: Number(event.lngLat.lat.toFixed(4)),
        lon: Number(event.lngLat.lng.toFixed(4)),
      })
    }
    const previousCursor = map.getCanvas().style.cursor
    if (crossSectionSelectionActive) {
      map.getCanvas().style.cursor = 'crosshair'
    }
    map.on('click', clickHandler)
    return () => {
      map.off('click', clickHandler)
      map.getCanvas().style.cursor = previousCursor
    }
  }, [crossSectionSelectionActive, mapLoaded, onAddCrossSectionPoint])

  return (
    <>
      <div ref={containerRef} className="h-full w-full" />
      {mapLoaded ? (
        <>
          <RadarOverlay map={mapRef.current} frame={currentFrame} nextFrame={nextFrame} opacity={opacity} />
          <AlertsOverlay map={mapRef.current} alerts={alerts} visible={showAlerts} />
          <RangeRingsOverlay map={mapRef.current} site={selectedSite} visible={showRangeRings} />
          <OperationalOverlays
            map={mapRef.current}
            spc={spcOverlays}
            spcDay2={spcDay2Overlays}
            spcDay3={spcDay3Overlays}
            md={mdOverlays}
            lsr={lsrOverlays}
            watch={watchOverlays}
            showSpc={showSpcOutlooks}
            showSpcDay2={showSpcDay2Outlooks}
            showSpcDay3={showSpcDay3Outlooks}
            showWatchBoxes={showWatchBoxes}
            showMesoscaleDiscussions={showMesoscaleDiscussions}
            showLocalStormReports={showLocalStormReports}
          />
          <SignaturesOverlay
            map={mapRef.current}
            mapLoaded={mapLoaded}
            signatures={signatures}
            visible={showSignatures}
          />
          <StormsOverlay
            map={mapRef.current}
            storms={storms}
            visible={showStorms}
            selectedStormId={selectedStormId}
            selectedStormTrack={selectedStormTrack}
            showTrails={showStormTrails}
            onSelectStorm={onSelectStorm}
          />
          <SavedLocationsOverlay map={mapRef.current} locations={savedLocations} visible={showSavedLocations} />
          <MetarOverlay map={mapRef.current} observations={metarObservations} visible={showMetars} />
          <CrossSectionOverlay map={mapRef.current} points={crossSectionPoints} />
          <RadarSweepOverlay map={mapRef.current} site={selectedSite} visible={showSweepAnimation} />
          {sites.map((site) => (
            <SiteMarker
              key={site.id}
              map={mapRef.current}
              site={site}
              selected={site.id === selectedSite?.id}
              visible={showSiteMarkers}
              onSelect={onSelectSite}
            />
          ))}
        </>
      ) : null}
    </>
  )
}
