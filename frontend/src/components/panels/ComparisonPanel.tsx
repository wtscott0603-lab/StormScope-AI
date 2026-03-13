import { Columns2 } from 'lucide-react'

import type { Product } from '../../types/radar'


export function ComparisonPanel({
  enabled,
  products,
  compareProduct,
  compareTilt,
  frameOffset,
  onToggle,
  onProductChange,
  onTiltChange,
  onOffsetChange,
}: {
  enabled: boolean
  products: Product[]
  compareProduct: string
  compareTilt: number
  frameOffset: number
  onToggle: (enabled: boolean) => void
  onProductChange: (product: string) => void
  onTiltChange: (tilt: number) => void
  onOffsetChange: (offset: number) => void
}) {
  return (
    <section className="space-y-3">
      <div className="flex items-center justify-between">
        <div className="text-xs uppercase tracking-[0.2em] text-white/55">Comparison</div>
        <label className="flex items-center gap-2 text-xs text-white/55">
          <Columns2 className="h-4 w-4 text-cyan/80" />
          <input type="checkbox" checked={enabled} onChange={(event) => onToggle(event.target.checked)} className="h-4 w-4 accent-cyan" />
        </label>
      </div>
      {enabled ? (
        <div className="space-y-2 rounded-md border border-white/10 bg-white/5 p-3 text-sm">
          <label className="block text-[11px] uppercase tracking-[0.18em] text-white/40">Compare Product</label>
          <select
            value={compareProduct}
            onChange={(event) => onProductChange(event.target.value)}
            className="w-full rounded-md border border-white/10 bg-black/30 px-3 py-2 text-white outline-none"
          >
            {products
              .filter((product) => product.enabled)
              .map((product) => (
                <option key={product.id} value={product.id}>
                  {product.id} {product.source_kind === 'volume' ? '(volume)' : ''}
                </option>
              ))}
          </select>
          <div className="grid grid-cols-2 gap-2">
            <label className="rounded-md border border-white/10 bg-black/30 px-3 py-2 text-white/75">
              <div className="text-[11px] uppercase tracking-[0.18em] text-white/40">Tilt</div>
              <input
                type="number"
                step="0.1"
                min="0.5"
                max="19.5"
                value={compareTilt}
                onChange={(event) => onTiltChange(Number(event.target.value))}
                className="mt-1 w-full bg-transparent font-mono outline-none"
              />
            </label>
            <label className="rounded-md border border-white/10 bg-black/30 px-3 py-2 text-white/75">
              <div className="text-[11px] uppercase tracking-[0.18em] text-white/40">Frame Offset</div>
              <input
                type="number"
                min="0"
                max="12"
                value={frameOffset}
                onChange={(event) => onOffsetChange(Number(event.target.value))}
                className="mt-1 w-full bg-transparent font-mono outline-none"
              />
            </label>
          </div>
          <div className="text-xs text-white/45">
            Split view compares the live pane against a second product or an older frame from the same site.
          </div>
        </div>
      ) : null}
    </section>
  )
}
