import type { ReactNode } from 'react'

import { cn } from '../../lib/cn'

export function Badge({
  children,
  tone = 'default',
  pulse = false,
}: {
  children: ReactNode
  tone?: 'default' | 'cyan' | 'amber' | 'danger'
  pulse?: boolean
}) {
  return (
    <span
      className={cn(
        'inline-flex items-center rounded border px-2 py-1 font-mono text-[11px] uppercase tracking-[0.18em]',
        tone === 'default' && 'border-white/15 bg-white/5 text-white/75',
        tone === 'cyan' && 'border-cyan/40 bg-cyan/10 text-cyan',
        tone === 'amber' && 'border-amber/50 bg-amber/10 text-amber',
        tone === 'danger' && 'border-danger/50 bg-danger/10 text-danger',
        pulse && 'animate-pulseAmber',
      )}
    >
      {children}
    </span>
  )
}
