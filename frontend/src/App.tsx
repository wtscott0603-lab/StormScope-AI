import { useEffect, useMemo, useState } from 'react'

import { fetchConfig, fetchHealth, fetchProducts, fetchStatus } from './api/radar'
import { fetchSiteDetail } from './api/sites'
import { OpacitySlider } from './components/controls/OpacitySlider'
import { OverlayToggles } from './components/controls/OverlayToggles'
import { PlaybackBar } from './components/controls/PlaybackBar'
import { ProductSelector } from './components/controls/ProductSelector'
import { SiteSelector } from './components/controls/SiteSelector'
import { TiltSelector } from './components/controls/TiltSelector'
import { RadarMap, type RadarMapProps } from './components/map/RadarMap'
import { AlertSettingsPanel } from './components/panels/AlertSettingsPanel'
import { TriggeredAlertsPanel } from './components/panels/TriggeredAlertsPanel'
import { ComparisonPanel } from './components/panels/ComparisonPanel'
import { ControlPanel } from './components/panels/ControlPanel'
import { CrossSectionPanel } from './components/panels/CrossSectionPanel'
import { LocationsPanel } from './components/panels/LocationsPanel'
import { HotspotsPanel } from './components/panels/HotspotsPanel'
import { LocationRiskPanel } from './components/panels/LocationRiskPanel'
import { ProductLegendPanel } from './components/panels/ProductLegendPanel'
import { SeverityBanner } from './components/panels/SeverityBanner'
import { SignaturesPanel } from './components/panels/SignaturesPanel'
import { StatusBar } from './components/panels/StatusBar'
import { StormsPanel } from './components/panels/StormsPanel'
import { TopBar } from './components/panels/TopBar'
import { ErrorBanner } from './components/ui/ErrorBanner'
import { LoadingSpinner } from './components/ui/LoadingSpinner'
import { useAlertMonitor } from './hooks/useAlertMonitor'
import { useAlerts } from './hooks/useAlerts'
import { useMetar } from './hooks/useMetar'
import { useOperationalOverlays } from './hooks/useOperationalOverlays'
import { usePlayback } from './hooks/usePlayback'
import { useRadarFrames } from './hooks/useRadarFrames'
import { useSavedLocations } from './hooks/useSavedLocations'
import { useSignatures } from './hooks/useSignatures'
import { useSites } from './hooks/useSites'
import { useStormTrack } from './hooks/useStormTrack'
import { useStorms } from './hooks/useStorms'
import { useStormHotspots } from './hooks/useStormHotspots'
import { useLocationRisk } from './hooks/useLocationRisk'
import { runtimeDefaultCenter, runtimeDefaultMapZoom, runtimeTileAttribution, runtimeTileUrl } from './lib/runtimeConfig'
import { useRadarStore } from './store/radarStore'
import type { ApiConfig, ApiStatus, Health, Product, SiteDetail } from './types/radar'
import { SpcLegend } from './components/map/SpcLegend'

function isStale(frameTimestamp: string | undefined, updateIntervalSec: number | undefined) {
  if (!frameTimestamp || !updateIntervalSec) return false
  return Date.now() - new Date(frameTimestamp).getTime() > updateIntervalSec * 2 * 1000
}

function productDisplayLabel(product: string, tilt: number, products: Product[]) {
  const meta = products.find((e) => e.id === product)
  return meta?.source_kind === 'volume' ? `${product} volume` : `${product} ${tilt.toFixed(1)}°`
}

export default function App() {
  // ── Store state (batched into a single selector to cut subscriptions) ──
  const state = useRadarStore()

  const {
    selectedSite: selectedSiteId,
    selectedProduct,
    selectedTilt,
    comparisonEnabled,
    comparisonProduct,
    comparisonTilt,
    comparisonFrameOffset,
    crossSectionSelectionActive,
    crossSectionPoints,
    opacity,
    showAlerts,
    showSiteMarkers,
    showCountyLines,
    showSignatures,
    showStorms,
    showSavedLocations,
    showMetars,
    showSpcOutlooks,
    showSpcDay2Outlooks,
    showSpcDay3Outlooks,
    showWatchBoxes,
    showMesoscaleDiscussions,
    showLocalStormReports,
    showRangeRings,
    showSweepAnimation,
    showStormTrails,
    panelOpen,
    selectedStormId,
    setSelectedSite,
    setSelectedProduct,
    setSelectedTilt,
    setComparisonEnabled,
    setComparisonProduct,
    setComparisonTilt,
    setComparisonFrameOffset,
    startCrossSectionSelection,
    clearCrossSectionSelection,
    addCrossSectionPoint,
    setSelectedStormId,
    setOpacity,
    toggleAlerts,
    toggleSiteMarkers,
    toggleCountyLines,
    toggleSignatures,
    toggleStorms,
    toggleSavedLocations,
    toggleMetars,
    toggleSpcOutlooks,
    toggleSpcDay2Outlooks,
    toggleSpcDay3Outlooks,
    toggleWatchBoxes,
    toggleMesoscaleDiscussions,
    toggleLocalStormReports,
    toggleRangeRings,
    toggleSweepAnimation,
    toggleStormTrails,
    togglePanel,
  } = state

  // ── Data hooks ─────────────────────────────────────────────────────────
  const { sites, loading: sitesLoading, error: sitesError } = useSites()
  const { frames, loading: framesLoading, error: framesError, lastFetchedAt } = useRadarFrames(selectedSiteId, selectedProduct, 20)
  const { frames: comparisonFrames, loading: comparisonFramesLoading, error: comparisonFramesError } =
    useRadarFrames(selectedSiteId, comparisonProduct, 20, comparisonTilt, false)
  const playback = usePlayback(frames.length, framesLoading)

  const [config, setConfig] = useState<ApiConfig | null>(null)
  const [products, setProducts] = useState<Product[]>([])
  const [siteDetail, setSiteDetail] = useState<SiteDetail | null>(null)
  const [apiStatus, setApiStatus] = useState<ApiStatus | null>(null)
  const [health, setHealth] = useState<Health | null>(null)
  const [shellError, setShellError] = useState<string | null>(null)

  const selectedSite = useMemo(() => sites.find((s) => s.id === selectedSiteId), [selectedSiteId, sites])

  useEffect(() => {
    let active = true
    type R = [PromiseSettledResult<ApiConfig>, PromiseSettledResult<Product[]>, PromiseSettledResult<ApiStatus>, PromiseSettledResult<Health>]
    function apply(results: R, ctx: 'initialize' | 'refresh') {
      if (!active) return
      const [cfg, prods, stat, hlt] = results
      const fails: string[] = []
      if (cfg.status === 'fulfilled') setConfig(cfg.value); else fails.push('config')
      if (prods.status === 'fulfilled') setProducts(prods.value); else fails.push('products')
      if (stat.status === 'fulfilled') setApiStatus(stat.value); else fails.push('status')
      if (hlt.status === 'fulfilled') setHealth(hlt.value); else fails.push('health')
      setShellError(fails.length ? `Failed to ${ctx}: ${fails.join(', ')}.` : null)
    }
    void Promise.allSettled([fetchConfig(), fetchProducts(), fetchStatus(), fetchHealth()]).then((r) => apply(r as R, 'initialize'))
    const t = window.setInterval(
      () => void Promise.allSettled([fetchConfig(), fetchProducts(), fetchStatus(), fetchHealth()]).then((r) => apply(r as R, 'refresh')),
      20_000,
    )
    return () => { active = false; window.clearInterval(t) }
  }, [])

  useEffect(() => {
    let active = true
    void fetchSiteDetail(selectedSiteId).then((p) => { if (active) setSiteDetail(p) }).catch(() => undefined)
    return () => { active = false }
  }, [selectedSiteId])

  const { alerts, error: alertsError } = useAlerts(selectedSite?.state, showAlerts)
  const { data: signaturesData, error: signaturesError } = useSignatures(selectedSiteId, selectedProduct, selectedTilt, showSignatures)
  const { storms, error: stormsError } = useStorms(selectedSiteId, showStorms)
  const { data: hotspots } = useStormHotspots(showStorms ? selectedSiteId : null)
  const { data: locationRisk } = useLocationRisk(showStorms ? selectedSiteId : null)
  const { spc, spcDay2, spcDay3, md, lsr, watch, error: overlaysError } = useOperationalOverlays({
    spc: showSpcOutlooks,
    spcDay2: showSpcDay2Outlooks,
    spcDay3: showSpcDay3Outlooks,
    md: showMesoscaleDiscussions,
    lsr: showLocalStormReports,
    watch: showWatchBoxes,
  })
  const { locations, error: locationsError, addLocation, removeLocation } = useSavedLocations()
  const { observations: metarObservations, error: metarError } = useMetar(selectedSiteId, showMetars)
  const { track: selectedStormTrack, error: stormTrackError } = useStormTrack(selectedStormId, showStorms && showStormTrails)
  const { settings: alertSettings, updateSettings: updateAlertSettings, requestNotificationPermission } =
    useAlertMonitor(storms, signaturesData?.signatures ?? [])

  const currentFrame = frames[playback.frameIndex] ?? null
  const nextFrame = frames.length > 1 ? frames[Math.min(playback.frameIndex + 1, frames.length - 1)] : null
  const latestFrame = frames[frames.length - 1]
  const stale = isStale(latestFrame?.timestamp, config?.update_interval_sec)

  const comparisonOffsetFromLatest = Math.max(0, frames.length - 1 - playback.frameIndex) + comparisonFrameOffset
  const comparisonIndex = comparisonFrames.length ? Math.max(0, comparisonFrames.length - 1 - comparisonOffsetFromLatest) : 0
  const comparisonCurrentFrame = comparisonFrames[comparisonIndex] ?? null
  const comparisonNextFrame = comparisonFrames.length > 1 ? comparisonFrames[Math.min(comparisonIndex + 1, comparisonFrames.length - 1)] : null

  const globalError =
    shellError ?? sitesError ?? framesError ??
    (comparisonEnabled ? comparisonFramesError : null) ??
    alertsError ?? signaturesError ?? stormsError ?? stormTrackError ??
    locationsError ?? metarError ?? overlaysError ?? null

  const bannerVisible = signaturesData?.max_severity === 'TORNADO' || signaturesData?.max_severity === 'TORNADO_EMERGENCY'

  const selectedStorm = useMemo(
    () => storms.find((s) => s.storm_id === selectedStormId) ?? storms[0] ?? null,
    [selectedStormId, storms],
  )

  useEffect(() => {
    if (!storms.length) { if (selectedStormId !== null) setSelectedStormId(null); return }
    if (!selectedStormId || !storms.some((s) => s.storm_id === selectedStormId)) setSelectedStormId(storms[0].storm_id)
  }, [selectedStormId, setSelectedStormId, storms])

  // Keyboard shortcuts — global hotkeys for common radar workflow actions.
  // Ignored when focus is inside an <input>, <textarea>, or <select>.
  useEffect(() => {
    function onKey(event: KeyboardEvent) {
      const tag = (event.target as HTMLElement)?.tagName?.toLowerCase()
      if (tag === 'input' || tag === 'textarea' || tag === 'select') return
      switch (event.key) {
        case ' ':  // Space — play/pause
          event.preventDefault()
          playback.setPlaying(!playback.isPlaying)
          break
        case 'ArrowRight':
          event.preventDefault()
          playback.setFrameIndex(Math.min((frames?.length ?? 1) - 1, playback.frameIndex + 1))
          break
        case 'ArrowLeft':
          event.preventDefault()
          playback.setFrameIndex(Math.max(0, playback.frameIndex - 1))
          break
        case 'p': // Toggle panel
          togglePanel()
          break
        case 'c': // Toggle county lines
          toggleCountyLines()
          break
        case 's': // Toggle storms overlay
          toggleStorms()
          break
        case 'a': // Toggle alerts
          toggleAlerts()
          break
        default:
          break
      }
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [playback, frames, toggleAlerts, toggleCountyLines, togglePanel, toggleStorms])

  const sharedMapProps: Omit<RadarMapProps, 'currentFrame' | 'nextFrame' | 'crossSectionPoints' | 'crossSectionSelectionActive' | 'onAddCrossSectionPoint'> = {
    tileUrl: config?.tile_url ?? runtimeTileUrl(),
    tileAttribution: runtimeTileAttribution(),
    defaultCenter: { lat: config?.default_center_lat ?? runtimeDefaultCenter().lat, lon: config?.default_center_lon ?? runtimeDefaultCenter().lon },
    defaultZoom: config?.default_map_zoom ?? runtimeDefaultMapZoom(),
    alerts,
    sites,
    selectedSite,
    opacity,
    showAlerts,
    showSiteMarkers,
    showCountyLines,
    showRangeRings,
    showSweepAnimation,
    signatures: signaturesData?.signatures ?? [],
    showSignatures,
    storms,
    showStorms,
    selectedStormId: selectedStorm?.storm_id ?? null,
    savedLocations: locations,
    showSavedLocations,
    metarObservations,
    showMetars,
    spcOverlays: spc,
    spcDay2Overlays: spcDay2,
    spcDay3Overlays: spcDay3,
    mdOverlays: md,
    lsrOverlays: lsr,
    watchOverlays: watch,
    showSpcOutlooks,
    showSpcDay2Outlooks,
    showSpcDay3Outlooks,
    showWatchBoxes,
    showMesoscaleDiscussions,
    showLocalStormReports,
    selectedStormTrack,
    showStormTrails,
    onSelectSite: setSelectedSite,
    onSelectStorm: setSelectedStormId,
  }

  const primaryLabel = productDisplayLabel(selectedProduct, selectedTilt, products)
  const comparisonLabel = comparisonEnabled
    ? `${productDisplayLabel(comparisonProduct, comparisonTilt, products)}${comparisonFrameOffset > 0 ? ` · -${comparisonFrameOffset}f` : ' · now'}`
    : ''

  return (
    <div className="relative h-screen w-screen overflow-hidden bg-surface text-textPrimary">
      <SeverityBanner maxSeverity={signaturesData?.max_severity ?? 'NONE'} signatures={signaturesData?.signatures ?? []} />
      {/* Map area */}
      <div className={['absolute inset-0 grid', comparisonEnabled ? 'grid-cols-1 gap-px lg:grid-cols-2' : 'grid-cols-1'].join(' ')}>
        <div className="relative">
          <RadarMap
            {...sharedMapProps}
            currentFrame={currentFrame}
            nextFrame={nextFrame}
            crossSectionPoints={crossSectionPoints}
            crossSectionSelectionActive={crossSectionSelectionActive}
            onAddCrossSectionPoint={addCrossSectionPoint}
          />
          {/* Compact product label */}
          <div className="pointer-events-none absolute left-3 top-3 rounded border border-panelBorder bg-panel/90 px-2 py-1 font-mono text-[11px] text-textSecondary">
            {primaryLabel}
          </div>
        </div>
        {comparisonEnabled ? (
          <div className="relative border-t border-panelBorder lg:border-l lg:border-t-0">
            <RadarMap
              {...sharedMapProps}
              currentFrame={comparisonCurrentFrame}
              nextFrame={comparisonNextFrame}
              crossSectionPoints={[]}
              crossSectionSelectionActive={false}
              onAddCrossSectionPoint={() => undefined}
            />
            <div className="pointer-events-none absolute left-3 top-3 rounded border border-panelBorder bg-panel/90 px-2 py-1 font-mono text-[11px] text-textSecondary">
              {comparisonLabel}
            </div>
          </div>
        ) : null}
      </div>

      {/* HUD */}
      <div className={['pointer-events-none absolute inset-0 flex flex-col justify-between p-3', bannerVisible ? 'pt-14' : ''].join(' ')}>
        {/* Top row */}
        <div className="flex items-start justify-between gap-3">
          <div className="w-full max-w-2xl space-y-2">
            <TopBar
              siteName={selectedSite?.name ?? siteDetail?.name}
              siteId={selectedSiteId}
              product={selectedProduct}
              processorStatus={apiStatus?.processor_status ?? 'idle'}
              healthStatus={health?.status ?? 'ok'}
              healthProcessorStatus={health?.processor_status ?? 'never_run'}
              stale={stale}
            />
            {globalError ? <ErrorBanner message={globalError} /> : null}
            {(sitesLoading || framesLoading || (comparisonEnabled && comparisonFramesLoading)) ? <LoadingSpinner /> : null}
          </div>

          <ControlPanel open={panelOpen} onToggle={togglePanel}>
            <SiteSelector sites={sites} value={selectedSiteId} onChange={setSelectedSite} />
            <ProductSelector products={products} value={selectedProduct} onChange={setSelectedProduct} />
            <ProductLegendPanel selectedProduct={selectedProduct} products={products} />
            <TiltSelector site={selectedSiteId} product={selectedProduct} value={selectedTilt} onChange={setSelectedTilt} />
            <ComparisonPanel
              enabled={comparisonEnabled}
              products={products}
              compareProduct={comparisonProduct}
              compareTilt={comparisonTilt}
              frameOffset={comparisonFrameOffset}
              onToggle={setComparisonEnabled}
              onProductChange={setComparisonProduct}
              onTiltChange={setComparisonTilt}
              onOffsetChange={setComparisonFrameOffset}
            />
            {comparisonEnabled ? <ProductLegendPanel selectedProduct={comparisonProduct} products={products} /> : null}
            <PlaybackBar
              frames={frames}
              frameIndex={playback.frameIndex}
              isPlaying={playback.isPlaying}
              playbackDelayMs={playback.playbackDelayMs}
              progress={playback.progress}
              onPlayToggle={() => playback.setPlaying(!playback.isPlaying)}
              onStepBackward={playback.stepBackward}
              onStepForward={playback.stepForward}
              onJumpLatest={playback.jumpToLatest}
              onScrub={playback.setFrameIndex}
              onSpeedChange={playback.setPlaybackDelayMs}
            />
            <OpacitySlider value={opacity} onChange={setOpacity} />
            <OverlayToggles
              showAlerts={showAlerts}
              showSiteMarkers={showSiteMarkers}
              showCountyLines={showCountyLines}
              showSignatures={showSignatures}
              showStorms={showStorms}
              showSavedLocations={showSavedLocations}
              showMetars={showMetars}
              showSpcOutlooks={showSpcOutlooks}
              showSpcDay2Outlooks={showSpcDay2Outlooks}
              showSpcDay3Outlooks={showSpcDay3Outlooks}
              showWatchBoxes={showWatchBoxes}
              showMesoscaleDiscussions={showMesoscaleDiscussions}
              showLocalStormReports={showLocalStormReports}
              showRangeRings={showRangeRings}
              showSweepAnimation={showSweepAnimation}
              showStormTrails={showStormTrails}
              onToggleAlerts={toggleAlerts}
              onToggleSiteMarkers={toggleSiteMarkers}
              onToggleCountyLines={toggleCountyLines}
              onToggleSignatures={toggleSignatures}
              onToggleStorms={toggleStorms}
              onToggleSavedLocations={toggleSavedLocations}
              onToggleMetars={toggleMetars}
              onToggleSpcOutlooks={toggleSpcOutlooks}
              onToggleSpcDay2Outlooks={toggleSpcDay2Outlooks}
              onToggleSpcDay3Outlooks={toggleSpcDay3Outlooks}
              onToggleWatchBoxes={toggleWatchBoxes}
              onToggleMesoscaleDiscussions={toggleMesoscaleDiscussions}
              onToggleLocalStormReports={toggleLocalStormReports}
              onToggleRangeRings={toggleRangeRings}
              onToggleSweepAnimation={toggleSweepAnimation}
              onToggleStormTrails={toggleStormTrails}
            />
            <CrossSectionPanel
              site={selectedSiteId}
              frameId={currentFrame?.frame_id ?? latestFrame?.frame_id ?? null}
              selectedProduct={selectedProduct}
              products={products}
              points={crossSectionPoints}
              selectionActive={crossSectionSelectionActive}
              onStartSelection={startCrossSectionSelection}
              onClearSelection={clearCrossSectionSelection}
            />
            <StormsPanel
              storms={storms}
              selectedStormId={selectedStorm?.storm_id ?? null}
              onSelectStorm={setSelectedStormId}
            />
            {hotspots.length > 0 && (
              <HotspotsPanel
                hotspots={hotspots}
                selectedStormId={selectedStorm?.storm_id ?? null}
                onSelectStorm={setSelectedStormId}
              />
            )}
            <TriggeredAlertsPanel site={selectedSiteId} />
            <LocationsPanel locations={locations} onAdd={addLocation} onDelete={removeLocation} />
            {locationRisk.length > 0 && (
              <LocationRiskPanel
                entries={locationRisk}
                onSelectStorm={setSelectedStormId}
              />
            )}
            <SignaturesPanel signatures={signaturesData?.signatures ?? []} />
            <AlertSettingsPanel
              settings={alertSettings}
              onChange={updateAlertSettings}
              onRequestNotifications={requestNotificationPermission}
            />
          </ControlPanel>
        </div>

        <div className="flex items-end justify-between gap-3">
          {(showSpcOutlooks || showSpcDay2Outlooks || showSpcDay3Outlooks) ? (
            <SpcLegend showDay2={showSpcDay2Outlooks} showDay3={showSpcDay3Outlooks} />
          ) : <div />}
          <StatusBar
            frameTimestamp={latestFrame?.timestamp}
            lastFetchedAt={lastFetchedAt}
            framesCount={frames.length}
            stale={stale}
            dataWarnings={apiStatus?.data_warnings ?? []}
            historySummary={apiStatus ? {
              historyStale: apiStatus.history_stale ?? false,
              isCaughtUp: apiStatus.is_caught_up ?? true,
              backlogFrameCount: apiStatus.backlog_frame_count ?? 0,
              lastIngestTime: apiStatus.last_ingest_time ?? null,
              lastAggTime: apiStatus.last_history_aggregation_time ?? null,
            } : undefined}
          />
        </div>
      </div>
    </div>
  )
}
