export function ErrorBanner({ message }: { message: string }) {
  return (
    <div className="rounded-md border border-danger/60 bg-danger/15 px-4 py-3 text-sm text-danger shadow-panel backdrop-blur-md">
      {message}
    </div>
  )
}
