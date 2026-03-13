export function LoadingSpinner({ label = 'Loading radar data…' }: { label?: string }) {
  return (
    <div className="flex items-center gap-3 rounded-md border border-white/10 bg-black/60 px-4 py-3 text-sm text-white/80 backdrop-blur-md">
      <div className="h-4 w-4 animate-spin rounded-full border-2 border-white/20 border-t-cyan" />
      <span>{label}</span>
    </div>
  )
}
