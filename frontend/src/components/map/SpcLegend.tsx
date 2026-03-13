const CATEGORIES = [
  { label: 'TSTM', color: '#c1e9c1', stroke: '#00cc00' },
  { label: 'MRGL', color: '#66a366', stroke: '#006600' },
  { label: 'SLGT', color: '#f6f67b', stroke: '#c8c800' },
  { label: 'ENH',  color: '#e6b366', stroke: '#c78c2c' },
  { label: 'MDT',  color: '#e66666', stroke: '#c83228' },
  { label: 'HIGH', color: '#ff66ff', stroke: '#cc00cc' },
]

export function SpcLegend({ showDay2, showDay3 }: { showDay2: boolean; showDay3: boolean }) {
  return (
    <div className="pointer-events-none rounded border border-panelBorder bg-panel/95 p-2 shadow-panel">
      <div className="mb-1 text-[9px] font-semibold uppercase tracking-widest text-textMuted">SPC Outlook</div>
      <div className="flex flex-col gap-0.5">
        {CATEGORIES.map(({ label, color, stroke }) => (
          <div key={label} className="flex items-center gap-1.5">
            <span
              className="inline-block h-2.5 w-4 rounded-sm border"
              style={{ background: color, borderColor: stroke }}
            />
            <span className="font-mono text-[10px] text-textSecondary">{label}</span>
          </div>
        ))}
      </div>
      {showDay2 && (
        <div className="mt-1.5 border-t border-panelBorder pt-1.5 text-[9px] text-textMuted">
          <span className="inline-block w-4 border-b border-dashed border-textMuted mr-1" /> Day 2 (dashed)
        </div>
      )}
      {showDay3 && (
        <div className="mt-0.5 text-[9px] text-textMuted">
          <span className="inline-block w-4 border-b border-dashed border-textMuted mr-1 opacity-50" /> Day 3 (faded)
        </div>
      )}
    </div>
  )
}
