type RuntimeConfig = {
  apiBaseUrl?: string
  defaultSite?: string
  mapTileUrl?: string
  mapTileAttribution?: string
  defaultCenterLat?: number
  defaultCenterLon?: number
  defaultMapZoom?: number
  preferredUnits?: string
  defaultEnabledOverlays?: string[]
}

declare global {
  interface Window {
    __RADAR_CONFIG__?: RuntimeConfig
  }
}

function runtimeConfig(): RuntimeConfig {
  if (typeof window === 'undefined') {
    return {}
  }
  return window.__RADAR_CONFIG__ ?? {}
}

export function runtimeApiBaseUrl(): string {
  return runtimeConfig().apiBaseUrl || import.meta.env.VITE_API_BASE_URL || ''
}

export function runtimeDefaultSite(): string {
  return runtimeConfig().defaultSite || import.meta.env.VITE_DEFAULT_SITE || 'KILN'
}

export function runtimeTileUrl(): string {
  return runtimeConfig().mapTileUrl || import.meta.env.VITE_MAP_TILE_URL || 'https://tile.openstreetmap.org/{z}/{x}/{y}.png'
}

export function runtimeTileAttribution(): string {
  return runtimeConfig().mapTileAttribution || import.meta.env.VITE_MAP_TILE_ATTRIBUTION || '© OpenStreetMap contributors'
}

export function runtimeDefaultCenter(): { lat: number; lon: number } {
  const config = runtimeConfig()
  return {
    lat: Number(config.defaultCenterLat ?? import.meta.env.VITE_DEFAULT_CENTER_LAT ?? 40.0197),
    lon: Number(config.defaultCenterLon ?? import.meta.env.VITE_DEFAULT_CENTER_LON ?? -82.8799),
  }
}

export function runtimeDefaultMapZoom(): number {
  return Number(runtimeConfig().defaultMapZoom ?? import.meta.env.VITE_DEFAULT_MAP_ZOOM ?? 8.8)
}

export function runtimePreferredUnits(): string {
  return runtimeConfig().preferredUnits || import.meta.env.VITE_PREFERRED_UNITS || 'imperial'
}

export function runtimeDefaultEnabledOverlays(): string[] {
  const config = runtimeConfig()
  const overlays = config.defaultEnabledOverlays
  if (Array.isArray(overlays) && overlays.length) {
    return overlays.map((item) => String(item).toLowerCase())
  }
  const raw = import.meta.env.VITE_DEFAULT_ENABLED_OVERLAYS || 'alerts,signatures,storms,saved_locations,metars,spc,watch_boxes,storm_trails,range_rings,radar_sites,sweep_animation'
  return String(raw)
    .split(',')
    .map((item) => item.trim().toLowerCase())
    .filter(Boolean)
}
