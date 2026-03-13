interface OverlayToggleProps {
  showAlerts: boolean
  showSiteMarkers: boolean
  showCountyLines: boolean
  showSignatures: boolean
  showStorms: boolean
  showSavedLocations: boolean
  showMetars: boolean
  showSpcOutlooks: boolean
  showSpcDay2Outlooks: boolean
  showSpcDay3Outlooks: boolean
  showWatchBoxes: boolean
  showMesoscaleDiscussions: boolean
  showLocalStormReports: boolean
  showRangeRings: boolean
  showSweepAnimation: boolean
  showStormTrails: boolean
  onToggleAlerts: () => void
  onToggleSiteMarkers: () => void
  onToggleCountyLines: () => void
  onToggleSignatures: () => void
  onToggleStorms: () => void
  onToggleSavedLocations: () => void
  onToggleMetars: () => void
  onToggleSpcOutlooks: () => void
  onToggleSpcDay2Outlooks: () => void
  onToggleSpcDay3Outlooks: () => void
  onToggleWatchBoxes: () => void
  onToggleMesoscaleDiscussions: () => void
  onToggleLocalStormReports: () => void
  onToggleRangeRings: () => void
  onToggleSweepAnimation: () => void
  onToggleStormTrails: () => void
}

function Row({
  label,
  dot,
  checked,
  onChange,
}: {
  label: string
  dot?: string
  checked: boolean
  onChange: () => void
}) {
  return (
    <label className="flex cursor-pointer items-center justify-between py-1.5 text-sm text-textSecondary hover:text-textPrimary">
      <span className="flex items-center gap-2">
        {dot && (
          <span
            className="inline-block h-2 w-2 rounded-full"
            style={{ background: dot }}
          />
        )}
        {label}
      </span>
      <input
        type="checkbox"
        checked={checked}
        onChange={onChange}
        className="h-3.5 w-3.5 accent-accent cursor-pointer"
      />
    </label>
  )
}

function Section({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div>
      <div className="mb-1 text-[10px] font-semibold uppercase tracking-widest text-textMuted">{label}</div>
      <div className="divide-y divide-panelBorder/50">{children}</div>
    </div>
  )
}

export function OverlayToggles(props: OverlayToggleProps) {
  return (
    <div className="space-y-4">
      <div className="text-[10px] font-semibold uppercase tracking-widest text-textMuted">Overlays</div>

      <Section label="SPC Outlooks">
        <Row label="Day 1 (today)" dot="#e66666" checked={props.showSpcOutlooks} onChange={props.onToggleSpcOutlooks} />
        <Row label="Day 2 (tomorrow)" dot="#e6b366" checked={props.showSpcDay2Outlooks} onChange={props.onToggleSpcDay2Outlooks} />
        <Row label="Day 3 (+2 days)" dot="#66a366" checked={props.showSpcDay3Outlooks} onChange={props.onToggleSpcDay3Outlooks} />
      </Section>

      <Section label="Warnings & Watches">
        <Row label="NWS warning polygons" dot="#ff4444" checked={props.showAlerts} onChange={props.onToggleAlerts} />
        <Row label="Watch boxes" dot="#ff8800" checked={props.showWatchBoxes} onChange={props.onToggleWatchBoxes} />
        <Row label="Mesoscale discussions" dot="#e0aa00" checked={props.showMesoscaleDiscussions} onChange={props.onToggleMesoscaleDiscussions} />
      </Section>

      <Section label="Observations">
        <Row label="Local storm reports" dot="#e55" checked={props.showLocalStormReports} onChange={props.onToggleLocalStormReports} />
        <Row label="METARs" checked={props.showMetars} onChange={props.onToggleMetars} />
      </Section>

      <Section label="Storm Analysis">
        <Row label="Storm objects" checked={props.showStorms} onChange={props.onToggleStorms} />
        <Row label="Storm trails" checked={props.showStormTrails} onChange={props.onToggleStormTrails} />
        <Row label="Signatures (TVS/BWER)" checked={props.showSignatures} onChange={props.onToggleSignatures} />
      </Section>

      <Section label="Map">
        <Row label="County lines" checked={props.showCountyLines} onChange={props.onToggleCountyLines} />
        <Row label="Radar sites" checked={props.showSiteMarkers} onChange={props.onToggleSiteMarkers} />
        <Row label="Range rings" checked={props.showRangeRings} onChange={props.onToggleRangeRings} />
        <Row label="Saved locations" checked={props.showSavedLocations} onChange={props.onToggleSavedLocations} />
        <Row label="Radar sweep" checked={props.showSweepAnimation} onChange={props.onToggleSweepAnimation} />
      </Section>
    </div>
  )
}
