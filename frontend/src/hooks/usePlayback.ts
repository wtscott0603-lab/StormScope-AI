import { useEffect, useMemo } from 'react'

import { useRadarStore } from '../store/radarStore'


export function usePlayback(frameCount: number, loading: boolean) {
  const frameIndex = useRadarStore((state) => state.frameIndex)
  const isPlaying = useRadarStore((state) => state.isPlaying)
  const playbackDelayMs = useRadarStore((state) => state.playbackDelayMs)
  const setFrameIndex = useRadarStore((state) => state.setFrameIndex)
  const setPlaying = useRadarStore((state) => state.setPlaying)
  const setPlaybackDelayMs = useRadarStore((state) => state.setPlaybackDelayMs)

  useEffect(() => {
    if (loading || frameCount === 0) {
      setPlaying(false)
      setFrameIndex(0)
      return
    }
    if (frameIndex >= frameCount) {
      setFrameIndex(frameCount - 1)
    }
  }, [frameCount, frameIndex, loading, setFrameIndex, setPlaying])

  useEffect(() => {
    if (!isPlaying || loading || frameCount <= 1) {
      return
    }
    const timer = window.setInterval(() => {
      setFrameIndex((frameIndex + 1) % frameCount)
    }, playbackDelayMs)
    return () => window.clearInterval(timer)
  }, [frameCount, frameIndex, isPlaying, loading, playbackDelayMs, setFrameIndex])

  const progress = useMemo(() => {
    if (frameCount <= 1) {
      return 0
    }
    return frameIndex / (frameCount - 1)
  }, [frameCount, frameIndex])

  return {
    frameIndex,
    isPlaying,
    playbackDelayMs,
    progress,
    setPlaying,
    setPlaybackDelayMs,
    stepBackward: () => setFrameIndex(Math.max(frameIndex - 1, 0)),
    stepForward: () => setFrameIndex(Math.min(frameIndex + 1, Math.max(frameCount - 1, 0))),
    jumpToLatest: () => setFrameIndex(Math.max(frameCount - 1, 0)),
    setFrameIndex,
  }
}
