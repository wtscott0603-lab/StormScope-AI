import { type TriggeredAlert } from '../../api/alerts'
import { useTriggeredAlerts } from '../../hooks/useTriggeredAlerts'

const SEVERITY_COLORS: Record<string, string> = {
  TORNADO_EMERGENCY: 'bg-fuchsia-900 border-fuchsia-400 text-fuchsia-100',
  TORNADO: 'bg-red-900 border-red-400 text-red-100',
  SEVERE: 'bg-orange-900 border-orange-400 text-orange-100',
  MARGINAL: 'bg-yellow-900 border-yellow-400 text-yellow-100',
  NONE: 'bg-slate-800 border-slate-500 text-slate-200',
}

const KIND_LABEL: Record<string, string> = {
  tornado_emergency: 'TORNADO EMERGENCY',
  tornado_warning: 'TORNADO THREAT',
  tvs_detected: 'TVS/TDS',
  severe_storm: 'SEVERE STORM',
  marginal_storm: 'MARGINAL STORM',
  location_imminent: 'LOCATION IMPACT',
}

function timeAgo(iso: string): string {
  const diffMs = Date.now() - new Date(iso).getTime()
  const mins = Math.floor(diffMs / 60_000)
  if (mins < 1) return 'just now'
  if (mins === 1) return '1 min ago'
  if (mins < 60) return `${mins} min ago`
  const hrs = Math.floor(mins / 60)
  return `${hrs}h ${mins % 60}m ago`
}

interface AlertRowProps {
  alert: TriggeredAlert
  onAcknowledge: (id: string) => void
}

function AlertRow({ alert, onAcknowledge }: AlertRowProps) {
  const colorClass = SEVERITY_COLORS[alert.severity_level] ?? SEVERITY_COLORS.NONE
  const kindLabel = KIND_LABEL[alert.alert_kind] ?? alert.alert_kind.toUpperCase()
  const opacity = alert.acknowledged ? 'opacity-40' : 'opacity-100'

  return (
    <div className={`border rounded p-2 mb-2 text-xs transition-opacity ${colorClass} ${opacity}`}>
      <div className="flex items-start justify-between gap-2">
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 flex-wrap">
            <span className="font-bold tracking-wide text-[10px] uppercase opacity-80">{kindLabel}</span>
            {alert.threat_score != null && (
              <span className="font-mono opacity-60">{alert.threat_score.toFixed(2)}</span>
            )}
            <span className="opacity-50 text-[10px]">{timeAgo(alert.triggered_at)}</span>
          </div>
          <div className="font-semibold mt-0.5">{alert.title}</div>
          <div className="mt-0.5 opacity-80 leading-snug">{alert.body}</div>
          {alert.location_id && (
            <div className="mt-0.5 text-[10px] opacity-60">Location: {alert.location_id}</div>
          )}
        </div>
        {!alert.acknowledged && (
          <button
            onClick={() => onAcknowledge(alert.alert_id)}
            className="shrink-0 text-[10px] px-2 py-1 rounded bg-white/10 hover:bg-white/20 transition-colors"
            title="Acknowledge"
          >
            ✓
          </button>
        )}
      </div>
    </div>
  )
}

interface Props {
  site?: string
}

export function TriggeredAlertsPanel({ site }: Props) {
  const { alerts, unacknowledgedCount, loading, error, acknowledge, acknowledgeAll } =
    useTriggeredAlerts(site, true)

  const unread = alerts.filter((a) => !a.acknowledged)
  const read = alerts.filter((a) => a.acknowledged)

  return (
    <div className="flex flex-col gap-0 h-full">
      {/* Header */}
      <div className="flex items-center justify-between px-3 py-2 border-b border-slate-700">
        <div className="flex items-center gap-2">
          <span className="text-xs font-semibold text-slate-200 uppercase tracking-wide">
            Server Alerts
          </span>
          {unacknowledgedCount > 0 && (
            <span className="bg-red-600 text-white text-[10px] font-bold rounded-full px-1.5 py-0.5 leading-none">
              {unacknowledgedCount}
            </span>
          )}
        </div>
        {unacknowledgedCount > 0 && (
          <button
            onClick={acknowledgeAll}
            className="text-[10px] text-slate-400 hover:text-slate-200 transition-colors"
          >
            Acknowledge all
          </button>
        )}
      </div>

      {/* Body */}
      <div className="flex-1 overflow-y-auto p-2">
        {loading && alerts.length === 0 && (
          <p className="text-xs text-slate-500 px-1">Loading…</p>
        )}
        {error && (
          <p className="text-xs text-red-400 px-1">{error}</p>
        )}
        {!loading && !error && alerts.length === 0 && (
          <p className="text-xs text-slate-500 px-1">No triggered alerts in this retention window.</p>
        )}

        {unread.length > 0 && (
          <>
            {unread.map((a) => (
              <AlertRow key={a.alert_id} alert={a} onAcknowledge={acknowledge} />
            ))}
          </>
        )}

        {read.length > 0 && (
          <>
            {unread.length > 0 && (
              <div className="text-[10px] text-slate-600 uppercase tracking-wide mt-2 mb-1 px-1">
                Acknowledged
              </div>
            )}
            {read.map((a) => (
              <AlertRow key={a.alert_id} alert={a} onAcknowledge={acknowledge} />
            ))}
          </>
        )}
      </div>
    </div>
  )
}
