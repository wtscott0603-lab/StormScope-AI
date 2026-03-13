import { apiFetch } from './client'
import type { Site, SiteDetail } from '../types/radar'

export function fetchSites() {
  return apiFetch<Site[]>('/api/sites')
}

export function fetchSiteDetail(siteId: string) {
  return apiFetch<SiteDetail>(`/api/sites/${siteId}`)
}
