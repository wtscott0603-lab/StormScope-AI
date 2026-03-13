#!/bin/sh
set -eu

config_json="$(jq -cn \
  --arg apiBaseUrl "${VITE_API_BASE_URL:-}" \
  --arg defaultSite "${VITE_DEFAULT_SITE:-${DEFAULT_SITE:-KILN}}" \
  --arg mapTileUrl "${VITE_MAP_TILE_URL:-https://tile.openstreetmap.org/{z}/{x}/{y}.png}" \
  --arg mapTileAttribution "${VITE_MAP_TILE_ATTRIBUTION:-© OpenStreetMap contributors}" \
  --argjson defaultCenterLat "${VITE_DEFAULT_CENTER_LAT:-${DEFAULT_CENTER_LAT:-40.0197}}" \
  --argjson defaultCenterLon "${VITE_DEFAULT_CENTER_LON:-${DEFAULT_CENTER_LON:--82.8799}}" \
  --argjson defaultMapZoom "${VITE_DEFAULT_MAP_ZOOM:-${DEFAULT_MAP_ZOOM:-8.8}}" \
  --arg preferredUnits "${VITE_PREFERRED_UNITS:-${PREFERRED_UNITS:-imperial}}" \
  --arg defaultEnabledOverlays "${VITE_DEFAULT_ENABLED_OVERLAYS:-${DEFAULT_ENABLED_OVERLAYS:-alerts,signatures,storms,saved_locations,metars,spc,watch_boxes,storm_trails,range_rings,radar_sites,sweep_animation}}" \
  '{
    apiBaseUrl: $apiBaseUrl,
    defaultSite: $defaultSite,
    mapTileUrl: $mapTileUrl,
    mapTileAttribution: $mapTileAttribution,
    defaultCenterLat: $defaultCenterLat,
    defaultCenterLon: $defaultCenterLon,
    defaultMapZoom: $defaultMapZoom,
    preferredUnits: $preferredUnits,
    defaultEnabledOverlays: ($defaultEnabledOverlays | split(",") | map(ascii_downcase | gsub("^\\s+|\\s+$"; "")) | map(select(length > 0)))
  }'
)"

printf 'window.__RADAR_CONFIG__ = %s;\n' "$config_json" > /usr/share/nginx/html/config.js
