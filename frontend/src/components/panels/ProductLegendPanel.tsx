import type { Product } from '../../types/radar'


const PRODUCT_NOTES: Record<string, string> = {
  ET: 'Echo tops are 18 dBZ volume-derived heights from the full scan, not a single tilt.',
  VIL: 'VIL is a volume-integrated liquid estimate derived from reflectivity. The storm engine also uses VIL density against echo-top depth for hail support.',
  KDP: 'KDP uses the raw specific differential phase field when present, otherwise a differential-phase gradient proxy. It is most useful in heavy-rain cores.',
  RR: 'Rain rate uses a reflectivity fallback and promotes R(KDP) where KDP support is available in heavy rain.',
  QPE1H: '1h QPE is a rolling radar-only accumulation estimate from recent scans and carries meaningful uncertainty.',
  HC: 'Hydrometeor classification is a rules-based V1 product with moderate rain, large drops, hail, and debris-candidate context. Snow/mixed on the 2D lowest-sweep view can still reflect melting-layer artifacts rather than true cold-season structure.',
  SRV: 'SRV is a consensus-motion scan product. Storm-specific SRV metrics in the storm panel remain more trustworthy than the map field in multi-storm regimes.',
}


export function ProductLegendPanel({
  selectedProduct,
  products,
}: {
  selectedProduct: string
  products: Product[]
}) {
  const product = products.find((entry) => entry.id === selectedProduct)
  if (!product) {
    return null
  }

  return (
    <section className="space-y-2">
      <div className="text-xs uppercase tracking-[0.2em] text-white/55">Legend / Notes</div>
      <div className="rounded-md border border-white/10 bg-white/5 p-3 text-sm text-white/72">
        <div className="flex items-center justify-between gap-3">
          <div className="font-medium text-white">{product.name}</div>
          <div className="font-mono text-[11px] text-cyan/80">
            {product.source_kind}
            {product.source_product ? ` · ${product.source_product}` : ''}
          </div>
        </div>
        <div className="mt-2">{product.description}</div>
        <div className="mt-2 text-xs text-white/45">
          {PRODUCT_NOTES[selectedProduct] ?? 'Use this product alongside storm cards and signatures for context.'}
        </div>
      </div>
    </section>
  )
}
