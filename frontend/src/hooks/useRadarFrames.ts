import { useEffect, useRef, useState } from 'react'

import { fetchFrames } from '../api/radar'
import { createAbortController, apiFetchAbortable } from '../api/client'
import { useRadarStore } from '../store/radarStore'
import type { Frame } from '../types/radar'

const POLL_INTERVAL_MS = 20_000

export function useRadarFrames(site: string, product: string, limit = 20, tiltOverride?: number, syncPlayback = true) {
  const selectedTilt = useRadarStore((state) => state.selectedTilt)
  const frameIndex = useRadarStore((state) => state.frameIndex)
  const setFrameIndex = useRadarStore((state) => state.setFrameIndex)
  const [frames, setFrames] = useState<Frame[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [lastFetchedAt, setLastFetchedAt] = useState<string | null>(null)
  const framesRef = useRef<Frame[]>([])
  const frameIndexRef = useRef(frameIndex)
  const latestFrameIdRef = useRef<string | null>(null)

  useEffect(() => { framesRef.current = frames }, [frames])
  useEffect(() => { frameIndexRef.current = frameIndex }, [frameIndex])

  useEffect(() => {
    // Create a unique abort key for this hook invocation; cancelled on site/product/tilt change
    const abortKey = `frames:${site}:${product}:${tiltOverride ?? selectedTilt}`
    latestFrameIdRef.current = null

    async function load() {
      const controller = createAbortController(abortKey)
      try {
        setLoading(true)
        const tilt = tiltOverride ?? selectedTilt
        const url = `/api/v1/radar/frames?site=${site}&product=${product}&tilt=${tilt}&limit=${limit}`
        const payload = await apiFetchAbortable<Frame[]>(url, controller.signal)
        if (payload === null) return // Aborted

        const newLatestId = payload[payload.length - 1]?.frame_id ?? null
        const dataChanged = newLatestId !== latestFrameIdRef.current || payload.length !== framesRef.current.length
        latestFrameIdRef.current = newLatestId
        setLastFetchedAt(new Date().toISOString())
        setError(null)

        if (dataChanged) {
          const previousFrames = framesRef.current
          const previousIndex = frameIndexRef.current
          const previousFrameId = previousFrames[previousIndex]?.frame_id ?? null
          const wasPinnedToLatest = previousFrames.length === 0 || previousIndex >= previousFrames.length - 1
          setFrames(payload)
          framesRef.current = payload
          if (syncPlayback) {
            if (payload.length === 0) {
              setFrameIndex(0)
            } else if (wasPinnedToLatest) {
              setFrameIndex(payload.length - 1)
            } else if (previousFrameId) {
              const idx = payload.findIndex((f) => f.frame_id === previousFrameId)
              setFrameIndex(idx >= 0 ? idx : Math.min(previousIndex, payload.length - 1))
            } else {
              setFrameIndex(Math.min(previousIndex, payload.length - 1))
            }
          }
        }
      } catch (err) {
        const message = err instanceof Error ? err.message : 'Failed to load radar frames.'
        if (message.includes('No processed frames')) setFrames([])
        setError(message)
      } finally {
        setLoading(false)
      }
    }

    void load()
    const timer = window.setInterval(() => void load(), POLL_INTERVAL_MS)
    return () => {
      window.clearInterval(timer)
    }
  }, [limit, product, selectedTilt, setFrameIndex, site, syncPlayback, tiltOverride])

  return { frames, loading, error, lastFetchedAt }
}
