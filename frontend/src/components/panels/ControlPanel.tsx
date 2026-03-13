import type { ReactNode } from 'react'
import { ChevronLeft, ChevronRight } from 'lucide-react'

export function ControlPanel({
  open,
  onToggle,
  children,
}: {
  open: boolean
  onToggle: () => void
  children: ReactNode
}) {
  return (
    <aside className="pointer-events-auto flex">
      <button
        type="button"
        onClick={onToggle}
        className="mt-2 h-10 rounded-l border border-r-0 border-panelBorder bg-panel px-2.5 text-textSecondary hover:text-textPrimary"
        aria-label={open ? 'Collapse panel' : 'Expand panel'}
      >
        {open ? <ChevronRight className="h-3.5 w-3.5" /> : <ChevronLeft className="h-3.5 w-3.5" />}
      </button>
      {open ? (
        <div className="w-80 space-y-5 overflow-y-auto max-h-[calc(100vh-8rem)] rounded-l-none rounded-r border border-panelBorder bg-panel p-4 shadow-panel scrollbar-thin">
          {children}
        </div>
      ) : null}
    </aside>
  )
}
