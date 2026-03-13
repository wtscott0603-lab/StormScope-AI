import { Activity, AlertTriangle, Radar, Wifi, WifiOff } from 'lucide-react'

export function TopBar({
  siteName,
  siteId,
  product,
  processorStatus,
  healthStatus,
  healthProcessorStatus,
  stale,
}: {
  siteName?: string
  siteId: string
  product: string
  processorStatus: 'running' | 'idle' | 'error'
  healthStatus: 'ok' | 'degraded'
  healthProcessorStatus: 'ok' | 'stale' | 'error' | 'never_run'
  stale: boolean
}) {
  const isError =
    processorStatus === 'error' ||
    healthProcessorStatus === 'error' ||
    healthStatus === 'degraded'
  const isWarning = stale || healthProcessorStatus === 'stale'
  const isRunning = processorStatus === 'running'

  return (
    <div className="pointer-events-auto flex items-center gap-3 rounded border border-panelBorder bg-panel px-3 py-2 shadow-panel">
      {/* Radar icon */}
      <div className="flex h-8 w-8 shrink-0 items-center justify-center rounded bg-accent/20 text-accent">
        <Radar className="h-4 w-4" />
      </div>

      {/* Site info */}
      <div className="min-w-0 flex-1">
        <div className="flex items-baseline gap-2">
          <span className="text-sm font-semibold text-textPrimary truncate">
            {siteName ?? siteId}
          </span>
          <span className="font-mono text-xs text-textMuted">{siteId}</span>
          <span className="ml-1 rounded bg-accent/15 px-1.5 py-0.5 font-mono text-xs font-medium text-accent">
            {product}
          </span>
        </div>
      </div>

      {/* Status indicators */}
      <div className="flex items-center gap-2 shrink-0">
        {isRunning && (
          <span className="flex items-center gap-1 text-xs text-ok">
            <Activity className="h-3 w-3 animate-pulse" />
            live
          </span>
        )}
        {isError && (
          <span className="flex items-center gap-1 text-xs text-danger">
            <AlertTriangle className="h-3 w-3" />
            error
          </span>
        )}
        {isWarning && !isError && (
          <span className="flex items-center gap-1 text-xs text-warning animate-blink">
            <AlertTriangle className="h-3 w-3" />
            stale
          </span>
        )}
        {!isError && !isWarning && processorStatus === 'idle' && (
          <span className="flex items-center gap-1 text-xs text-textMuted">
            <Wifi className="h-3 w-3" />
            idle
          </span>
        )}
        {healthStatus === 'degraded' && (
          <span className="text-xs text-danger">
            <WifiOff className="h-3 w-3" />
          </span>
        )}
      </div>
    </div>
  )
}
