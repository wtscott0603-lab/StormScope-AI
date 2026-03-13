import { Pause, Play, SkipBack, SkipForward } from 'lucide-react'

import type { Frame } from '../../types/radar'

const SPEED_OPTIONS = [300, 500, 800]

export function PlaybackBar({
  frames,
  frameIndex,
  isPlaying,
  playbackDelayMs,
  progress,
  onPlayToggle,
  onStepBackward,
  onStepForward,
  onJumpLatest,
  onScrub,
  onSpeedChange,
}: {
  frames: Frame[]
  frameIndex: number
  isPlaying: boolean
  playbackDelayMs: number
  progress: number
  onPlayToggle: () => void
  onStepBackward: () => void
  onStepForward: () => void
  onJumpLatest: () => void
  onScrub: (index: number) => void
  onSpeedChange: (value: number) => void
}) {
  const currentFrame = frames[frameIndex]
  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <label className="text-xs uppercase tracking-[0.2em] text-white/55">Playback</label>
        <span className="font-mono text-xs text-white/50">
          {frames.length === 0 ? '0 / 0' : `${frameIndex + 1} / ${frames.length}`}
        </span>
      </div>
      <div className="flex items-center gap-2">
        <button
          type="button"
          onClick={onStepBackward}
          className="rounded-md border border-white/10 bg-white/5 p-2 text-white/75 hover:border-white/25"
        >
          <SkipBack className="h-4 w-4" />
        </button>
        <button
          type="button"
          onClick={onPlayToggle}
          className="rounded-md border border-cyan/50 bg-cyan/10 p-2 text-cyan hover:border-cyan"
        >
          {isPlaying ? <Pause className="h-4 w-4" /> : <Play className="h-4 w-4" />}
        </button>
        <button
          type="button"
          onClick={onStepForward}
          className="rounded-md border border-white/10 bg-white/5 p-2 text-white/75 hover:border-white/25"
        >
          <SkipForward className="h-4 w-4" />
        </button>
        <button
          type="button"
          onClick={onJumpLatest}
          className="ml-auto rounded-md border border-white/10 bg-white/5 px-3 py-2 text-xs uppercase tracking-[0.18em] text-white/70 hover:border-white/25"
        >
          Latest
        </button>
      </div>
      <input
        type="range"
        min={0}
        max={Math.max(frames.length - 1, 0)}
        value={Math.min(frameIndex, Math.max(frames.length - 1, 0))}
        onChange={(event) => onScrub(Number(event.target.value))}
        className="slider w-full"
      />
      <div className="flex items-center justify-between text-xs text-white/45">
        <span>{currentFrame ? new Date(currentFrame.timestamp).toUTCString().replace('GMT', 'UTC') : 'Waiting for frames'}</span>
        <div className="flex items-center gap-2">
          <span>{Math.round(progress * 100)}%</span>
          <select
            aria-label="Playback speed"
            value={playbackDelayMs}
            onChange={(event) => onSpeedChange(Number(event.target.value))}
            className="rounded-md border border-white/10 bg-black/50 px-2 py-1 text-white/70 outline-none"
          >
            {SPEED_OPTIONS.map((speed) => (
              <option key={speed} value={speed}>
                {speed} ms
              </option>
            ))}
          </select>
        </div>
      </div>
    </div>
  )
}
