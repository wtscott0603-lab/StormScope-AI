import { useEffect, useRef } from 'react'
import maplibregl from 'maplibre-gl'

import type { SignatureMarker } from '../../types/radar'

const TYPE_STYLES: Record<string, { ring: string; bg: string; text: string; short: string }> = {
  TVS: { ring: '#ff4747', bg: '#190304', text: '#ff7c7c', short: 'TVS' },
  TDS: { ring: '#ff4dff', bg: '#170318', text: '#ff8eff', short: 'TDS' },
  ROTATION: { ring: '#ff6a5c', bg: '#180605', text: '#ff9d92', short: 'ROT' },
  HAIL_CORE: { ring: '#58d8ff', bg: '#04141b', text: '#8ce7ff', short: 'HAIL' },
  HAIL_LARGE: { ring: '#7ae8ff', bg: '#041821', text: '#baf2ff', short: 'LG H' },
  BOW_ECHO: { ring: '#ffab3d', bg: '#1b0f04', text: '#ffc675', short: 'BOW' },
  BWER: { ring: '#ffd04a', bg: '#1b1604', text: '#ffe07c', short: 'BWER' },
}

const SEVERITY_PULSE: Record<string, boolean> = {
  TORNADO_EMERGENCY: true,
  TORNADO: true,
  SEVERE: false,
  MARGINAL: false,
  NONE: false,
}

function ensureOverlayStyles() {
  if (document.getElementById('signature-overlay-styles')) {
    return
  }

  const style = document.createElement('style')
  style.id = 'signature-overlay-styles'
  style.textContent = `
    @keyframes signaturePulse {
      0%, 100% { transform: scale(1); opacity: 0.58; }
      50% { transform: scale(1.45); opacity: 0.18; }
    }
  `
  document.head.appendChild(style)
}

function createMarkerElement(signature: SignatureMarker): HTMLElement {
  const palette = TYPE_STYLES[signature.signature_type] ?? {
    ring: '#d5dde0',
    bg: '#101315',
    text: '#ecf1f2',
    short: signature.signature_type.slice(0, 4).toUpperCase(),
  }

  const element = document.createElement('div')
  element.className = 'radar-signature-marker'
  element.style.cssText = `
    position: relative;
    width: 44px;
    height: 44px;
    display: flex;
    align-items: center;
    justify-content: center;
    cursor: pointer;
    user-select: none;
  `

  if (SEVERITY_PULSE[signature.severity]) {
    const pulse = document.createElement('div')
    pulse.style.cssText = `
      position: absolute;
      inset: -5px;
      border-radius: 999px;
      border: 2px solid ${palette.ring};
      animation: signaturePulse 1.25s ease-in-out infinite;
      opacity: 0.58;
    `
    element.appendChild(pulse)
  }

  const ring = document.createElement('div')
  ring.style.cssText = `
    position: absolute;
    inset: 0;
    border-radius: 999px;
    border: 2px solid ${palette.ring};
    background: ${palette.bg};
    box-shadow: 0 0 0 1px rgba(255,255,255,0.06), 0 0 18px ${palette.ring}22;
    opacity: 0.94;
  `
  element.appendChild(ring)

  const inner = document.createElement('div')
  inner.style.cssText = `
    position: relative;
    z-index: 1;
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    gap: 2px;
  `

  const title = document.createElement('span')
  title.textContent = palette.short
  title.style.cssText = `
    color: ${palette.text};
    font-family: 'JetBrains Mono', monospace;
    font-size: 9px;
    font-weight: 800;
    line-height: 1;
    letter-spacing: 0.08em;
  `
  inner.appendChild(title)

  const severity = document.createElement('span')
  severity.textContent = signature.severity === 'TORNADO_EMERGENCY' ? 'EMRG' : signature.severity.slice(0, 4)
  severity.style.cssText = `
    color: ${palette.text};
    font-family: 'JetBrains Mono', monospace;
    font-size: 7px;
    line-height: 1;
    opacity: 0.7;
    letter-spacing: 0.08em;
  `
  inner.appendChild(severity)

  element.appendChild(inner)
  return element
}

function popupHtml(signature: SignatureMarker): string {
  const palette = TYPE_STYLES[signature.signature_type] ?? TYPE_STYLES.ROTATION
  const lonSuffix = signature.lon <= 0 ? 'W' : 'E'
  const latSuffix = signature.lat >= 0 ? 'N' : 'S'

  return `
    <div style="
      background:#0a0d0f;
      color:#dce6ea;
      border:1px solid ${palette.ring};
      border-radius:8px;
      min-width:220px;
      padding:10px 12px;
      font-family:'JetBrains Mono', monospace;
      font-size:11px;
      line-height:1.45;
    ">
      <div style="color:${palette.text};font-size:13px;font-weight:800;letter-spacing:0.05em;margin-bottom:4px;">
        ${signature.label}
      </div>
      <div style="color:#7d8d93;font-size:10px;margin-bottom:6px;">
        ${signature.signature_type} | ${signature.severity.split('_').join(' ')} | ${signature.analyzer.toUpperCase()}
      </div>
      <div style="margin-bottom:6px;">${signature.description}</div>
      <div style="color:#93a7ae;font-size:10px;">
        Confidence ${(signature.confidence * 100).toFixed(0)}% | ${Math.abs(signature.lat).toFixed(3)}${latSuffix} ${Math.abs(signature.lon).toFixed(3)}${lonSuffix}
      </div>
    </div>
  `
}

export function SignaturesOverlay({
  map,
  mapLoaded,
  signatures,
  visible,
}: {
  map: maplibregl.Map | null
  mapLoaded: boolean
  signatures: SignatureMarker[]
  visible: boolean
}) {
  const markersRef = useRef<maplibregl.Marker[]>([])

  useEffect(() => {
    ensureOverlayStyles()
  }, [])

  useEffect(() => {
    if (!map || !mapLoaded) {
      return
    }

    markersRef.current.forEach((marker) => marker.remove())
    markersRef.current = []

    if (!visible) {
      return
    }

    signatures.forEach((signature) => {
      const element = createMarkerElement(signature)
      const popup = new maplibregl.Popup({
        offset: 24,
        closeButton: true,
        closeOnClick: false,
      }).setHTML(popupHtml(signature))

      const marker = new maplibregl.Marker({ element, anchor: 'center' })
        .setLngLat([signature.lon, signature.lat])
        .setPopup(popup)
        .addTo(map)

      markersRef.current.push(marker)
    })

    return () => {
      markersRef.current.forEach((marker) => marker.remove())
      markersRef.current = []
    }
  }, [map, mapLoaded, signatures, visible])

  return null
}
