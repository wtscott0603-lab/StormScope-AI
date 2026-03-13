import { Search } from 'lucide-react'
import { useMemo, useState } from 'react'

import type { Site } from '../../types/radar'

export function SiteSelector({
  sites,
  value,
  onChange,
}: {
  sites: Site[]
  value: string
  onChange: (site: string) => void
}) {
  const [query, setQuery] = useState('')

  const filteredSites = useMemo(() => {
    if (!query.trim()) {
      return sites
    }
    const normalized = query.toLowerCase()
    return sites.filter((site) =>
      [site.id, site.name, site.state].some((field) => field.toLowerCase().includes(normalized)),
    )
  }, [query, sites])

  return (
    <div className="space-y-3">
      <label className="text-xs uppercase tracking-[0.2em] text-white/55">Radar Site</label>
      <div className="flex items-center gap-2 rounded-md border border-white/10 bg-white/5 px-3 py-2">
        <Search className="h-4 w-4 text-white/45" />
        <input
          value={query}
          onChange={(event) => setQuery(event.target.value)}
          placeholder="Search by ID, city, state"
          className="w-full bg-transparent text-sm text-white outline-none placeholder:text-white/30"
        />
      </div>
      <select
        aria-label="Radar site"
        value={value}
        onChange={(event) => onChange(event.target.value)}
        className="h-40 w-full rounded-md border border-white/10 bg-black/60 px-3 py-2 text-sm text-white outline-none"
      >
        {filteredSites.map((site) => (
          <option key={site.id} value={site.id}>
            {site.id} · {site.name} · {site.state}
          </option>
        ))}
      </select>
    </div>
  )
}
