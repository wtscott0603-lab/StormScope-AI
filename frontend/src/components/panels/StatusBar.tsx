export function StatusBar({
  frameTimestamp,
  lastFetchedAt,
  framesCount,
  stale,
  dataWarnings = [],
  historySummary,
}: {
  frameTimestamp?: string
  lastFetchedAt?: string | null
  framesCount: number
  stale: boolean
  dataWarnings?: string[]
  historySummary?: {
    historyStale: boolean
    isCaughtUp: boolean
    backlogFrameCount: number
    lastIngestTime: string | null
    lastAggTime: string | null
  }
}) {
  const historyOk = historySummary
    ? !historySummary.historyStale && historySummary.isCaughtUp
    : null
  const backlog = historySummary?.backlogFrameCount ?? 0

  function fmtAge(iso: string | null): string {
    if (!iso) return '—'
    const diffMin = Math.round((Date.now() - new Date(iso).getTime()) / 60000)
    if (diffMin < 1) return '<1m ago'
    if (diffMin < 60) return `${diffMin}m ago`
    return `${Math.round(diffMin / 60)}h ago`
  }

  return (
    <div className="pointer-events-auto flex flex-wrap items-center justify-between gap-2 rounded border border-panelBorder bg-panel/95 px-3 py-2 font-mono text-[11px] text-textMuted shadow-panel">
      <div className="flex flex-wrap items-center gap-4">
        <span>{framesCount} frames</span>
        <span>scan: {frameTimestamp ? new Date(frameTimestamp).toUTCString().replace(' GMT', 'Z') : '—'}</span>
        <span>poll: {lastFetchedAt ? new Date(lastFetchedAt).toUTCString().replace(' GMT', 'Z') : '—'}</span>

        {/* v15 — history freshness indicators */}
        {historySummary && (
          <>
            <span
              title={`Last ingest: ${fmtAge(historySummary.lastIngestTime)}\nLast aggregation: ${fmtAge(historySummary.lastAggTime)}`}
              className={historyOk ? 'text-ok' : 'text-warning'}
            >
              hist: {historyOk ? '● live' : '⚠ stale'}
            </span>
            {backlog > 0 && (
              <span className="text-warning" title={`${backlog} frames pending processing`}>
                backlog: {backlog}
              </span>
            )}
          </>
        )}

        {dataWarnings.length > 0 && (
          <span className="text-warning" title={dataWarnings.join('\n')}>
            {dataWarnings[0].length > 60 ? dataWarnings[0].slice(0, 57) + '…' : dataWarnings[0]}
          </span>
        )}
      </div>
      <span className={stale ? 'text-warning animate-blink' : 'text-ok'}>
        {stale ? '⚠ stale' : '● live'}
      </span>
    </div>
  )
}
