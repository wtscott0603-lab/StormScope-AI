export function OpacitySlider({
  value,
  onChange,
}: {
  value: number
  onChange: (value: number) => void
}) {
  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between">
        <label className="text-xs uppercase tracking-[0.2em] text-white/55">Radar Opacity</label>
        <span className="font-mono text-xs text-white/50">{Math.round(value * 100)}%</span>
      </div>
      <input
        type="range"
        min={0.15}
        max={1}
        step={0.01}
        value={value}
        onChange={(event) => onChange(Number(event.target.value))}
        className="slider w-full"
      />
    </div>
  )
}
