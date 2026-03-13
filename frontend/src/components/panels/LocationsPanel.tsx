import { FormEvent, useState } from 'react'
import { Plus, Trash2 } from 'lucide-react'

import type { SavedLocation } from '../../types/storms'

export function LocationsPanel({
  locations,
  onAdd,
  onDelete,
}: {
  locations: SavedLocation[]
  onAdd: (payload: { name: string; lat: number; lon: number; kind?: string }) => Promise<void>
  onDelete: (locationId: string) => Promise<void>
}) {
  const [name, setName] = useState('')
  const [lat, setLat] = useState('')
  const [lon, setLon] = useState('')
  const [submitting, setSubmitting] = useState(false)

  const handleSubmit = async (event: FormEvent) => {
    event.preventDefault()
    const parsedLat = Number(lat)
    const parsedLon = Number(lon)
    if (!name.trim() || Number.isNaN(parsedLat) || Number.isNaN(parsedLon)) {
      return
    }
    try {
      setSubmitting(true)
      await onAdd({ name: name.trim(), lat: parsedLat, lon: parsedLon, kind: 'custom' })
      setName('')
      setLat('')
      setLon('')
    } finally {
      setSubmitting(false)
    }
  }

  return (
    <section className="space-y-3">
      <div className="text-xs uppercase tracking-[0.2em] text-white/55">Saved Locations</div>
      <form onSubmit={handleSubmit} className="grid grid-cols-2 gap-2">
        <input
          value={name}
          onChange={(event) => setName(event.target.value)}
          placeholder="Name"
          className="col-span-2 rounded-md border border-white/10 bg-white/5 px-3 py-2 text-sm text-white outline-none placeholder:text-white/30"
        />
        <input
          value={lat}
          onChange={(event) => setLat(event.target.value)}
          placeholder="Lat"
          className="rounded-md border border-white/10 bg-white/5 px-3 py-2 text-sm text-white outline-none placeholder:text-white/30"
        />
        <input
          value={lon}
          onChange={(event) => setLon(event.target.value)}
          placeholder="Lon"
          className="rounded-md border border-white/10 bg-white/5 px-3 py-2 text-sm text-white outline-none placeholder:text-white/30"
        />
        <button
          type="submit"
          disabled={submitting}
          className="col-span-2 inline-flex items-center justify-center gap-2 rounded-md border border-cyan/40 bg-cyan/10 px-3 py-2 text-sm text-cyan transition hover:bg-cyan/15 disabled:opacity-50"
        >
          <Plus className="h-4 w-4" />
          Add location
        </button>
      </form>

      <div className="space-y-2">
        {locations.length ? (
          locations.map((location) => (
            <div key={location.location_id} className="flex items-center justify-between rounded-md border border-white/10 bg-white/5 px-3 py-2">
              <div>
                <div className="text-sm text-white">{location.name}</div>
                <div className="font-mono text-[10px] text-white/45">
                  {location.lat.toFixed(3)}, {location.lon.toFixed(3)}
                </div>
              </div>
              <button
                type="button"
                onClick={() => void onDelete(location.location_id)}
                className="rounded border border-white/10 p-2 text-white/50 transition hover:text-danger"
                aria-label={`Delete ${location.name}`}
              >
                <Trash2 className="h-4 w-4" />
              </button>
            </div>
          ))
        ) : (
          <div className="rounded-md border border-white/10 bg-white/5 px-3 py-4 text-sm text-white/40">
            Add a location to receive ETA and impact estimates from tracked storms.
          </div>
        )}
      </div>
    </section>
  )
}
