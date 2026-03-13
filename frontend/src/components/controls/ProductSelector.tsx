import { cn } from '../../lib/cn'
import type { Product } from '../../types/radar'

export function ProductSelector({
  products,
  value,
  onChange,
}: {
  products: Product[]
  value: string
  onChange: (product: string) => void
}) {
  return (
    <div className="space-y-3">
      <label className="text-xs uppercase tracking-[0.2em] text-white/55">Product</label>
      <div className="grid grid-cols-2 gap-2">
        {products.map((product) => (
          <button
            key={product.id}
            type="button"
            onClick={() => onChange(product.id)}
            disabled={!product.enabled || !product.available}
            className={cn(
              'rounded-md border px-3 py-2 text-left text-sm transition-colors',
              product.id === value
                ? 'border-cyan bg-cyan/10 text-cyan'
                : 'border-white/10 bg-white/5 text-white/80 hover:border-white/30',
              (!product.enabled || !product.available) && 'cursor-not-allowed opacity-40',
            )}
            title={
              !product.enabled
                ? 'Product is not enabled in the current processor profile.'
                : !product.available
                  ? 'Product is enabled but no processed frames are available yet.'
                  : product.description
            }
          >
            <div className="flex items-center justify-between gap-2">
              <div className="font-medium">{product.id}</div>
              {product.source_kind !== 'raw' ? (
                <span
                  className={cn(
                    'rounded px-1.5 py-0.5 font-mono text-[9px] uppercase tracking-[0.18em]',
                    product.source_kind === 'volume'
                      ? 'border border-emerald-400/20 bg-emerald-400/10 text-emerald-200'
                      : 'border border-cyan/20 bg-cyan/10 text-cyan/85',
                  )}
                >
                  {product.source_kind}
                </span>
              ) : null}
            </div>
            <div className="text-xs text-white/45">
              {product.unit}
              {product.source_product ? ` | from ${product.source_product}` : ''}
            </div>
          </button>
        ))}
      </div>
    </div>
  )
}
