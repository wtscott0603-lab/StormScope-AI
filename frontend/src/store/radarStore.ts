import { create } from 'zustand'
import { runtimeDefaultEnabledOverlays, runtimeDefaultSite } from '../lib/runtimeConfig'

interface RadarState {
  selectedSite: string
  selectedProduct: string
  selectedTilt: number
  comparisonEnabled: boolean
  comparisonProduct: string
  comparisonTilt: number
  comparisonFrameOffset: number
  crossSectionSelectionActive: boolean
  crossSectionPoints: Array<{ lat: number; lon: number }>
  frameIndex: number
  isPlaying: boolean
  playbackDelayMs: number
  opacity: number
  showAlerts: boolean
  showSiteMarkers: boolean
  showCountyLines: boolean
  showSignatures: boolean
  showStorms: boolean
  showSavedLocations: boolean
  showMetars: boolean
  showSpcOutlooks: boolean
  showSpcDay2Outlooks: boolean
  showSpcDay3Outlooks: boolean
  showWatchBoxes: boolean
  showMesoscaleDiscussions: boolean
  showLocalStormReports: boolean
  showRangeRings: boolean
  showSweepAnimation: boolean
  showStormTrails: boolean
  panelOpen: boolean
  selectedStormId: string | null
  setSelectedSite: (site: string) => void
  setSelectedProduct: (product: string) => void
  setSelectedTilt: (tilt: number) => void
  setComparisonEnabled: (enabled: boolean) => void
  setComparisonProduct: (product: string) => void
  setComparisonTilt: (tilt: number) => void
  setComparisonFrameOffset: (offset: number) => void
  startCrossSectionSelection: () => void
  clearCrossSectionSelection: () => void
  addCrossSectionPoint: (point: { lat: number; lon: number }) => void
  setFrameIndex: (index: number) => void
  setPlaying: (playing: boolean) => void
  setPlaybackDelayMs: (delayMs: number) => void
  setOpacity: (value: number) => void
  toggleAlerts: () => void
  toggleSiteMarkers: () => void
  toggleCountyLines: () => void
  toggleSignatures: () => void
  toggleStorms: () => void
  toggleSavedLocations: () => void
  toggleMetars: () => void
  toggleSpcOutlooks: () => void
  toggleSpcDay2Outlooks: () => void
  toggleSpcDay3Outlooks: () => void
  toggleWatchBoxes: () => void
  toggleMesoscaleDiscussions: () => void
  toggleLocalStormReports: () => void
  toggleRangeRings: () => void
  toggleSweepAnimation: () => void
  toggleStormTrails: () => void
  togglePanel: () => void
  setSelectedStormId: (stormId: string | null) => void
}

const defaultSite = runtimeDefaultSite()
const defaultOverlays = new Set(runtimeDefaultEnabledOverlays())

export const useRadarStore = create<RadarState>((set) => ({
  selectedSite: defaultSite.toUpperCase(),
  selectedProduct: 'REF',
  selectedTilt: 0.5,
  comparisonEnabled: false,
  comparisonProduct: 'CC',
  comparisonTilt: 0.5,
  comparisonFrameOffset: 3,
  crossSectionSelectionActive: false,
  crossSectionPoints: [],
  frameIndex: 0,
  isPlaying: false,
  playbackDelayMs: 500,
  opacity: 0.86,
  showAlerts: defaultOverlays.has('alerts'),
  showSiteMarkers: defaultOverlays.has('radar_sites'),
  showCountyLines: defaultOverlays.has('county_lines'),
  showSignatures: defaultOverlays.has('signatures') || !defaultOverlays.size,
  showStorms: defaultOverlays.has('storms') || !defaultOverlays.size,
  showSavedLocations: defaultOverlays.has('saved_locations') || !defaultOverlays.size,
  showMetars: defaultOverlays.has('metars'),
  showSpcOutlooks: defaultOverlays.has('spc'),
  showSpcDay2Outlooks: false,
  showSpcDay3Outlooks: false,
  showWatchBoxes: defaultOverlays.has('watch_boxes'),
  showMesoscaleDiscussions: defaultOverlays.has('mesoscale_discussions'),
  showLocalStormReports: defaultOverlays.has('storm_reports') || defaultOverlays.has('local_storm_reports'),
  showRangeRings: defaultOverlays.has('range_rings'),
  showSweepAnimation: defaultOverlays.has('sweep_animation'),
  showStormTrails: defaultOverlays.has('storm_trails'),
  panelOpen: true,
  selectedStormId: null,
  setSelectedSite: (site) => set({ selectedSite: site.toUpperCase(), frameIndex: 0 }),
  setSelectedProduct: (product) => set({ selectedProduct: product.toUpperCase(), frameIndex: 0 }),
  setSelectedTilt: (selectedTilt) => set({ selectedTilt, frameIndex: 0 }),
  setComparisonEnabled: (comparisonEnabled) => set({ comparisonEnabled }),
  setComparisonProduct: (comparisonProduct) => set({ comparisonProduct: comparisonProduct.toUpperCase() }),
  setComparisonTilt: (comparisonTilt) => set({ comparisonTilt }),
  setComparisonFrameOffset: (comparisonFrameOffset) => set({ comparisonFrameOffset }),
  startCrossSectionSelection: () => set({ crossSectionSelectionActive: true, crossSectionPoints: [] }),
  clearCrossSectionSelection: () => set({ crossSectionSelectionActive: false, crossSectionPoints: [] }),
  addCrossSectionPoint: (point) =>
    set((state) => {
      const nextPoints = [...state.crossSectionPoints, point].slice(0, 2)
      return {
        crossSectionPoints: nextPoints,
        crossSectionSelectionActive: nextPoints.length < 2,
      }
    }),
  setFrameIndex: (frameIndex) => set({ frameIndex }),
  setPlaying: (isPlaying) => set({ isPlaying }),
  setPlaybackDelayMs: (playbackDelayMs) => set({ playbackDelayMs }),
  setOpacity: (opacity) => set({ opacity }),
  toggleAlerts: () => set((state) => ({ showAlerts: !state.showAlerts })),
  toggleSiteMarkers: () => set((state) => ({ showSiteMarkers: !state.showSiteMarkers })),
  toggleCountyLines: () => set((state) => ({ showCountyLines: !state.showCountyLines })),
  toggleSignatures: () => set((state) => ({ showSignatures: !state.showSignatures })),
  toggleStorms: () => set((state) => ({ showStorms: !state.showStorms })),
  toggleSavedLocations: () => set((state) => ({ showSavedLocations: !state.showSavedLocations })),
  toggleMetars: () => set((state) => ({ showMetars: !state.showMetars })),
  toggleSpcOutlooks: () => set((state) => ({ showSpcOutlooks: !state.showSpcOutlooks })),
  toggleSpcDay2Outlooks: () => set((state) => ({ showSpcDay2Outlooks: !state.showSpcDay2Outlooks })),
  toggleSpcDay3Outlooks: () => set((state) => ({ showSpcDay3Outlooks: !state.showSpcDay3Outlooks })),
  toggleWatchBoxes: () => set((state) => ({ showWatchBoxes: !state.showWatchBoxes })),
  toggleMesoscaleDiscussions: () => set((state) => ({ showMesoscaleDiscussions: !state.showMesoscaleDiscussions })),
  toggleLocalStormReports: () => set((state) => ({ showLocalStormReports: !state.showLocalStormReports })),
  toggleRangeRings: () => set((state) => ({ showRangeRings: !state.showRangeRings })),
  toggleSweepAnimation: () => set((state) => ({ showSweepAnimation: !state.showSweepAnimation })),
  toggleStormTrails: () => set((state) => ({ showStormTrails: !state.showStormTrails })),
  togglePanel: () => set((state) => ({ panelOpen: !state.panelOpen })),
  setSelectedStormId: (selectedStormId) => set({ selectedStormId }),
}))

// ---------------------------------------------------------------------------
// Granular selectors — use these instead of subscribing to the whole store.
// Components that use narrow selectors will only rerender when the specific
// slice they need actually changes, dramatically reducing rerender churn.
// ---------------------------------------------------------------------------

/** Selector: site, product, tilt — used by radar fetch hooks. */
export const selectRadarKey = (s: RadarState) =>
  `${s.selectedSite}|${s.selectedProduct}|${s.selectedTilt}` as const

/** Selector: playback state — used by playback bar only. */
export const selectPlayback = (s: RadarState) => ({
  frameIndex: s.frameIndex,
  isPlaying: s.isPlaying,
  playbackDelayMs: s.playbackDelayMs,
})

/** Selector: overlay visibility flags as a stable object. */
export const selectOverlayVisibility = (s: RadarState) => ({
  showAlerts: s.showAlerts,
  showSiteMarkers: s.showSiteMarkers,
  showCountyLines: s.showCountyLines,
  showSignatures: s.showSignatures,
  showStorms: s.showStorms,
  showSavedLocations: s.showSavedLocations,
  showMetars: s.showMetars,
  showSpcOutlooks: s.showSpcOutlooks,
  showSpcDay2Outlooks: s.showSpcDay2Outlooks,
  showSpcDay3Outlooks: s.showSpcDay3Outlooks,
  showWatchBoxes: s.showWatchBoxes,
  showMesoscaleDiscussions: s.showMesoscaleDiscussions,
  showLocalStormReports: s.showLocalStormReports,
  showRangeRings: s.showRangeRings,
  showSweepAnimation: s.showSweepAnimation,
  showStormTrails: s.showStormTrails,
})

/** Selector: comparison panel config. */
export const selectComparison = (s: RadarState) => ({
  comparisonEnabled: s.comparisonEnabled,
  comparisonProduct: s.comparisonProduct,
  comparisonTilt: s.comparisonTilt,
  comparisonFrameOffset: s.comparisonFrameOffset,
})

/** Selector: cross-section selection state. */
export const selectCrossSection = (s: RadarState) => ({
  crossSectionSelectionActive: s.crossSectionSelectionActive,
  crossSectionPoints: s.crossSectionPoints,
})

/** Selector: storm panel selected storm ID — avoids rerenders on map moves. */
export const selectSelectedStormId = (s: RadarState) => s.selectedStormId

/** Selector: panel open flag only. */
export const selectPanelOpen = (s: RadarState) => s.panelOpen
