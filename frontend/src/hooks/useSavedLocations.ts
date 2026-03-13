import { useEffect, useState } from 'react'

import { createSavedLocation, deleteSavedLocation, fetchSavedLocations } from '../api/storms'
import type { SavedLocation, SavedLocationCreate } from '../types/storms'


export function useSavedLocations() {
  const [locations, setLocations] = useState<SavedLocation[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const load = async () => {
    try {
      setLoading(true)
      const payload = await fetchSavedLocations()
      setLocations(payload)
      setError(null)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load saved locations')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    void load()
  }, [])

  const addLocation = async (payload: SavedLocationCreate) => {
    await createSavedLocation(payload)
    await load()
  }

  const removeLocation = async (locationId: string) => {
    await deleteSavedLocation(locationId)
    await load()
  }

  return { locations, loading, error, reload: load, addLocation, removeLocation }
}
