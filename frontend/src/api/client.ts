import { runtimeApiBaseUrl } from '../lib/runtimeConfig'

export function apiUrl(path: string): string {
  if (path.startsWith('http')) {
    return path
  }
  const baseUrl = runtimeApiBaseUrl().replace(/\/$/, '')
  return `${baseUrl}${path}`
}

export async function apiFetch<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(apiUrl(path), {
    ...init,
    headers: {
      Accept: 'application/json',
      ...(init?.headers ?? {}),
    },
  })

  if (!response.ok) {
    const payload = (await response.json().catch(() => null)) as { error?: string; detail?: string } | null
    const detail = payload?.detail ?? payload?.error ?? `HTTP ${response.status}`
    throw new Error(detail)
  }

  return response.json() as Promise<T>
}

// ---------------------------------------------------------------------------
// AbortController manager — cancels the previous request for a given key
// when a new one is started. Prevents stale responses from overwriting fresh
// ones when the user rapidly changes site, product, or tilt.
// ---------------------------------------------------------------------------

const _activeControllers = new Map<string, AbortController>()

/**
 * Create (or replace) an AbortController for a namespaced request key.
 * Calling this cancels any in-flight request with the same key.
 */
export function createAbortController(key: string): AbortController {
  const existing = _activeControllers.get(key)
  if (existing) {
    existing.abort()
  }
  const controller = new AbortController()
  _activeControllers.set(key, controller)
  return controller
}

/**
 * apiFetchAbortable — like apiFetch but accepts an AbortSignal.
 * AbortError is swallowed silently (it means the caller cancelled).
 * Any other error is re-thrown.
 */
export async function apiFetchAbortable<T>(
  path: string,
  signal: AbortSignal,
  init?: Omit<RequestInit, 'signal'>,
): Promise<T | null> {
  try {
    const response = await fetch(apiUrl(path), {
      ...init,
      signal,
      headers: {
        Accept: 'application/json',
        ...(init?.headers ?? {}),
      },
    })

    if (!response.ok) {
      const payload = (await response.json().catch(() => null)) as { error?: string; detail?: string } | null
      const detail = payload?.detail ?? payload?.error ?? `HTTP ${response.status}`
      throw new Error(detail)
    }

    return response.json() as Promise<T>
  } catch (err) {
    if (err instanceof DOMException && err.name === 'AbortError') {
      return null // Caller cancelled — not an error
    }
    throw err
  }
}
