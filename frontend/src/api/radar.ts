import { apiFetch } from './client'
import type { ApiConfig, ApiStatus, CrossSectionRequest, CrossSectionResponse, Frame, Health, Product, SignaturesResponse, TiltListResponse } from '../types/radar'

export function fetchConfig() {
  return apiFetch<ApiConfig>('/api/config')
}

export function fetchProducts() {
  return apiFetch<Product[]>('/api/products')
}

export function fetchFrames(site: string, product: string, limit = 20, tilt = 0.5) {
  return apiFetch<Frame[]>(`/api/radar/frames?site=${site}&product=${product}&limit=${limit}&tilt=${tilt}`)
}

export function fetchLatest(site: string, product: string, tilt = 0.5) {
  return apiFetch<Frame>(`/api/radar/latest?site=${site}&product=${product}&tilt=${tilt}`)
}

export function fetchStatus() {
  return apiFetch<ApiStatus>('/api/status')
}

export function fetchHealth() {
  return apiFetch<Health>('/health')
}

export function fetchSignatures(site: string, product: string, tilt = 0.5) {
  return apiFetch<SignaturesResponse>(`/api/radar/signatures?site=${site}&product=${product}&tilt=${tilt}`)
}

export function fetchTilts(site: string, product: string) {
  return apiFetch<TiltListResponse>(`/api/radar/tilts?site=${site}&product=${product}`)
}

export function createCrossSection(payload: CrossSectionRequest) {
  return apiFetch<CrossSectionResponse>('/api/v1/cross-section', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  })
}
