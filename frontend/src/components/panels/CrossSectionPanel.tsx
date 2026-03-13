import { useEffect, useRef, useState } from 'react'
import { Crosshair, Eraser, Mountain, Radar } from 'lucide-react'

import { createCrossSection } from '../../api/radar'
import type { CrossSectionResponse, Product } from '../../types/radar'


const REF_COLORS: Array<{ min: number; max: number; color: string }> = [
  { min: -999, max: 5, color: 'rgba(0,0,0,0)' },
  { min: 5, max: 10, color: '#66ccff' },
  { min: 10, max: 20, color: '#0070ff' },
  { min: 20, max: 30, color: '#00be00' },
  { min: 30, max: 40, color: '#ffe600' },
  { min: 40, max: 50, color: '#ff8c00' },
  { min: 50, max: 60, color: '#ff0000' },
  { min: 60, max: 65, color: '#ff00ff' },
  { min: 65, max: 999, color: '#ffffff' },
]

function colorForReflectivity(value: number | null) {
  if (value === null || Number.isNaN(value)) {
    return 'rgba(0,0,0,0)'
  }
  return REF_COLORS.find((entry) => value >= entry.min && value < entry.max)?.color ?? '#ffffff'
}

const VEL_COLORS: Array<{ min: number; max: number; color: string }> = [
  { min: -999, max: -45, color: '#7820ff' },
  { min: -45, max: -30, color: '#305dff' },
  { min: -30, max: -10, color: '#00b9ff' },
  { min: -10, max: 10, color: 'rgba(0,0,0,0)' },
  { min: 10, max: 30, color: '#ffd54f' },
  { min: 30, max: 50, color: '#ff7a00' },
  { min: 50, max: 999, color: '#ff1f1f' },
]

const CC_COLORS: Array<{ min: number; max: number; color: string }> = [
  { min: -999, max: 0.2, color: 'rgba(0,0,0,0)' },
  { min: 0.2, max: 0.5, color: '#54008c' },
  { min: 0.5, max: 0.7, color: '#2c48c4' },
  { min: 0.7, max: 0.9, color: '#00b080' },
  { min: 0.9, max: 0.95, color: '#d0d000' },
  { min: 0.95, max: 999, color: '#ffffff' },
]

const ZDR_COLORS: Array<{ min: number; max: number; color: string }> = [
  { min: -999, max: -2.0, color: 'rgba(0,0,0,0)' },
  { min: -2.0, max: 0.0, color: '#8a2be2' },
  { min: 0.0, max: 1.0, color: '#00acca' },
  { min: 1.0, max: 3.0, color: '#50c878' },
  { min: 3.0, max: 6.0, color: '#ffb000' },
  { min: 6.0, max: 999, color: '#ff4000' },
]

function colorForProduct(product: string, value: number | null) {
  if (value === null || Number.isNaN(value)) {
    return 'rgba(0,0,0,0)'
  }
  const productId = product.toUpperCase()
  const palette =
    productId === 'VEL' || productId === 'SRV'
      ? VEL_COLORS
      : productId === 'CC'
        ? CC_COLORS
        : productId === 'ZDR'
          ? ZDR_COLORS
          : REF_COLORS
  return palette.find((entry) => value >= entry.min && value < entry.max)?.color ?? '#ffffff'
}

function CrossSectionCanvas({ data }: { data: CrossSectionResponse }) {
  const canvasRef = useRef<HTMLCanvasElement | null>(null)

  useEffect(() => {
    const canvas = canvasRef.current
    if (!canvas) {
      return
    }
    const width = 440
    const height = 220
    canvas.width = width
    canvas.height = height
    const context = canvas.getContext('2d')
    if (!context) {
      return
    }
    context.clearRect(0, 0, width, height)
    context.fillStyle = '#081014'
    context.fillRect(0, 0, width, height)

    const cols = Math.max(1, data.ranges_km.length)
    const rows = Math.max(1, data.altitudes_km.length)
    const cellWidth = width / cols
    const cellHeight = height / rows
    for (let row = 0; row < rows; row += 1) {
      for (let col = 0; col < cols; col += 1) {
        const value = data.values[row]?.[col] ?? null
        context.fillStyle = colorForProduct(data.product, value)
        context.fillRect(col * cellWidth, height - ((row + 1) * cellHeight), cellWidth + 1, cellHeight + 1)
      }
    }
    context.strokeStyle = 'rgba(255,255,255,0.18)'
    context.lineWidth = 1
    context.strokeRect(0, 0, width, height)
  }, [data])

  return <canvas ref={canvasRef} className="w-full rounded border border-white/10 bg-[#081014]" />
}

export function CrossSectionPanel({
  site,
  frameId,
  selectedProduct,
  products,
  points,
  selectionActive,
  onStartSelection,
  onClearSelection,
}: {
  site: string
  frameId: string | null
  selectedProduct: string
  products: Product[]
  points: Array<{ lat: number; lon: number }>
  selectionActive: boolean
  onStartSelection: () => void
  onClearSelection: () => void
}) {
  const [data, setData] = useState<CrossSectionResponse | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [product, setProduct] = useState(selectedProduct.toUpperCase())
  const lastRange = data && data.ranges_km.length ? data.ranges_km[data.ranges_km.length - 1] : null
  const lastAltitude = data && data.altitudes_km.length ? data.altitudes_km[data.altitudes_km.length - 1] : null
  const supportedProducts = products.filter((entry) => ['REF', 'VEL', 'CC', 'ZDR'].includes(entry.id) && entry.enabled)

  const ready = points.length === 2 && !!frameId

  useEffect(() => {
    const next = selectedProduct.toUpperCase()
    if (supportedProducts.some((entry) => entry.id === next)) {
      setProduct(next)
      return
    }
    if (!supportedProducts.some((entry) => entry.id === product) && supportedProducts[0]) {
      setProduct(supportedProducts[0].id)
    }
  }, [product, selectedProduct, supportedProducts])

  const handleGenerate = async () => {
    if (!ready || !frameId) {
      return
    }
    try {
      setLoading(true)
      const payload = await createCrossSection({
        site,
        product,
        frame_id: frameId,
        start: points[0],
        end: points[1],
      })
      setData(payload)
      setError(null)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to generate cross-section')
    } finally {
      setLoading(false)
    }
  }

  return (
    <section className="space-y-3">
      <div className="flex items-center justify-between">
        <div className="text-xs uppercase tracking-[0.2em] text-white/55">Cross Section</div>
        <div className="font-mono text-[11px] text-white/45">{product} volume slice</div>
      </div>
      <div className="space-y-2 rounded-md border border-white/10 bg-white/5 p-3 text-sm text-white/72">
        <label className="block text-[11px] uppercase tracking-[0.18em] text-white/40">Product</label>
        <select
          value={product}
          onChange={(event) => setProduct(event.target.value)}
          className="w-full rounded-md border border-white/10 bg-black/30 px-3 py-2 text-white outline-none"
        >
          {supportedProducts.map((entry) => (
            <option key={entry.id} value={entry.id}>
              {entry.id} · {entry.name}
            </option>
          ))}
        </select>
        <div className="flex gap-2">
          <button
            type="button"
            onClick={onStartSelection}
            className="inline-flex flex-1 items-center justify-center gap-2 rounded-md border border-cyan/30 bg-cyan/10 px-3 py-2 text-cyan"
          >
            <Crosshair className="h-4 w-4" />
            {selectionActive ? 'Picking...' : 'Pick A/B'}
          </button>
          <button
            type="button"
            onClick={onClearSelection}
            className="inline-flex items-center justify-center gap-2 rounded-md border border-white/10 bg-black/30 px-3 py-2 text-white/60"
          >
            <Eraser className="h-4 w-4" />
          </button>
        </div>
        <div className="grid grid-cols-2 gap-2 font-mono text-[11px]">
          <div className="rounded border border-white/10 bg-black/30 px-2 py-2">
            <div className="text-white/40">Start</div>
            <div>{points[0] ? `${points[0].lat.toFixed(3)}, ${points[0].lon.toFixed(3)}` : 'not set'}</div>
          </div>
          <div className="rounded border border-white/10 bg-black/30 px-2 py-2">
            <div className="text-white/40">End</div>
            <div>{points[1] ? `${points[1].lat.toFixed(3)}, ${points[1].lon.toFixed(3)}` : 'not set'}</div>
          </div>
        </div>
        <button
          type="button"
          onClick={() => void handleGenerate()}
          disabled={!ready || loading}
          className="inline-flex w-full items-center justify-center gap-2 rounded-md border border-white/10 bg-black/30 px-3 py-2 text-white disabled:opacity-40"
        >
          <Radar className="h-4 w-4" />
          {loading ? 'Generating...' : 'Generate Section'}
        </button>
        {error ? <div className="text-xs text-red-300">{error}</div> : null}
        {data ? (
          <div className="space-y-2">
            <CrossSectionCanvas data={data} />
            <div className="grid grid-cols-3 gap-2 font-mono text-[11px] text-white/55">
              <div className="rounded border border-white/10 bg-black/30 px-2 py-2">
                <div className="text-white/35">Tilts</div>
                <div>{data.tilts_used.join(', ')}</div>
              </div>
              <div className="rounded border border-white/10 bg-black/30 px-2 py-2">
                <div className="text-white/35">Range</div>
                <div>{lastRange?.toFixed(0) ?? '0'} km</div>
              </div>
              <div className="rounded border border-white/10 bg-black/30 px-2 py-2">
                <div className="text-white/35">Top</div>
                <div>{lastAltitude?.toFixed(0) ?? '0'} km</div>
              </div>
            </div>
            <div className="rounded border border-white/10 bg-black/30 px-3 py-2 text-xs text-white/45">
              <div className="mb-1 inline-flex items-center gap-2 text-white/55">
                <Mountain className="h-4 w-4" />
                {data.method}
              </div>
              <div>{data.limitation}</div>
            </div>
          </div>
        ) : (
          <div className="text-xs text-white/45">
            Pick two map points to generate a multi-tilt cross section for REF, VEL, CC, or ZDR using the available raw volume.
          </div>
        )}
      </div>
    </section>
  )
}
