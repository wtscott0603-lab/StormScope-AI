import { Bell, Siren } from 'lucide-react'

import type { AlertSettings } from '../../lib/alertMonitor'


function NumberField({
  label,
  value,
  step = 1,
  onChange,
}: {
  label: string
  value: number
  step?: number
  onChange: (value: number) => void
}) {
  return (
    <label className="rounded-md border border-white/10 bg-black/30 px-3 py-2 text-sm text-white/75">
      <div className="text-[11px] uppercase tracking-[0.18em] text-white/40">{label}</div>
      <input
        type="number"
        step={step}
        value={value}
        onChange={(event) => onChange(Number(event.target.value))}
        className="mt-1 w-full bg-transparent font-mono outline-none"
      />
    </label>
  )
}


export function AlertSettingsPanel({
  settings,
  onChange,
  onRequestNotifications,
}: {
  settings: AlertSettings
  onChange: (next: Partial<AlertSettings>) => void
  onRequestNotifications: () => Promise<boolean>
}) {
  return (
    <section className="space-y-3">
      <div className="flex items-center justify-between">
        <div className="text-xs uppercase tracking-[0.2em] text-white/55">Monitoring Alerts</div>
        <label className="flex items-center gap-2 text-xs text-white/55">
          <Bell className="h-4 w-4 text-cyan/80" />
          <input type="checkbox" checked={settings.enabled} onChange={(event) => onChange({ enabled: event.target.checked })} className="h-4 w-4 accent-cyan" />
        </label>
      </div>
      <div className="space-y-2 rounded-md border border-white/10 bg-white/5 p-3 text-sm text-white/72">
        <label className="flex items-center justify-between rounded-md border border-white/10 bg-black/30 px-3 py-2">
          <span>Browser notifications</span>
          <input type="checkbox" checked={settings.browserNotifications} onChange={(event) => onChange({ browserNotifications: event.target.checked })} className="h-4 w-4 accent-cyan" />
        </label>
        <button
          type="button"
          onClick={() => void onRequestNotifications()}
          className="w-full rounded-md border border-white/10 bg-black/30 px-3 py-2 text-white/70"
        >
          Request notification permission
        </button>
        <label className="flex items-center justify-between rounded-md border border-white/10 bg-black/30 px-3 py-2">
          <span className="inline-flex items-center gap-2">
            <Siren className="h-4 w-4 text-cyan/80" />
            Audio alerts
          </span>
          <input type="checkbox" checked={settings.audioAlerts} onChange={(event) => onChange({ audioAlerts: event.target.checked })} className="h-4 w-4 accent-cyan" />
        </label>
        <div className="grid grid-cols-2 gap-2">
          <NumberField label="Hail Risk" value={settings.hailThreshold} step={0.05} onChange={(value) => onChange({ hailThreshold: value })} />
          <NumberField label="Wind Risk" value={settings.windThreshold} step={0.05} onChange={(value) => onChange({ windThreshold: value })} />
          <NumberField label="Flood Risk" value={settings.floodThreshold} step={0.05} onChange={(value) => onChange({ floodThreshold: value })} />
          <NumberField label="ETA Window" value={settings.etaWindowMinutes} onChange={(value) => onChange({ etaWindowMinutes: value })} />
          <NumberField label="Strengthening" value={settings.rapidStrengtheningThreshold} step={0.05} onChange={(value) => onChange({ rapidStrengtheningThreshold: value })} />
        </div>
        <div className="text-xs text-white/45">
          Alerts are local browser-side monitors tied to tracked storms, signatures, and saved-location ETAs. They do not replace official warnings.
        </div>
      </div>
    </section>
  )
}
