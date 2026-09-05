import { eventFeatureSummary, toDiscoveryEventPayload } from '../events';
import type { PreviewEvent, RelatedManifestCandidate } from '../types';

export function catalogSortValue(event: PreviewEvent): string {
  return event.starts_at || `${event.start_date}T${event.start_time || '23:59'}`;
}

export function toPersonalFeedCandidate(event: PreviewEvent, currentDate: string): RelatedManifestCandidate {
  return {
    ...eventFeatureSummary(event),
    status: event.ticket.status || event.status_label || 'available',
    lifecycle_status: event.lifecycle_status || 'active',
    is_free: event.ticket.is_free === true,
    base_similarity: 0.5,
    static_score: 0.5,
    reason_codes: [event.start_date < currentDate ? 'catalog:ongoing' : 'catalog:future'],
    exploration_candidate: false,
    display: toDiscoveryEventPayload(event),
  };
}
