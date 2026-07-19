import {
  eventFeatureSummary,
  eventIntersectsDateRange,
  getCurrentDate,
  getEvents,
  getPreviewBuild,
  toDiscoveryEventPayload,
} from '../../lib/events';
import type { PreviewEvent, RelatedManifestCandidate } from '../../lib/types';

const MAX_CANDIDATES = 500;
const PAGE_SIZE = 30;

function catalogSortValue(event: PreviewEvent): string {
  return event.starts_at || `${event.start_date}T${event.start_time || '23:59'}`;
}

function toPersonalFeedCandidate(event: PreviewEvent, currentDate: string): RelatedManifestCandidate {
  return {
    ...eventFeatureSummary(event),
    status: event.ticket.status || event.status_label || 'available',
    lifecycle_status: event.lifecycle_status || 'active',
    is_free: event.ticket.is_free,
    // The catalog is deliberately unranked. Client-side profile scoring supplies
    // personalization without baking visitor data into this public static file.
    base_similarity: 0.5,
    static_score: 0.5,
    reason_codes: [event.start_date < currentDate ? 'catalog:ongoing' : 'catalog:future'],
    exploration_candidate: false,
    display: toDiscoveryEventPayload(event),
  };
}

export function GET() {
  const currentDate = getCurrentDate();
  const candidates = getEvents()
    .filter((event) => (!event.lifecycle_status || event.lifecycle_status === 'active'))
    .filter((event) => eventIntersectsDateRange(event, currentDate, '9999-12-31'))
    .sort((left, right) => catalogSortValue(left).localeCompare(catalogSortValue(right)) || left.id - right.id)
    .slice(0, MAX_CANDIDATES)
    .map((event) => toPersonalFeedCandidate(event, currentDate));

  const manifest = {
    version: 1,
    schema_version: 'listing-personal-feed-v1',
    feature_schema_version: 'event-detail-related-v1',
    taxonomy_version: 'event-taxonomy-v1',
    surface: 'listing_personal_feed',
    algorithm_id: 'static_personal_feed_catalog_v1',
    generated_at: getPreviewBuild().generated_at,
    current_date: currentDate,
    preload_target: PAGE_SIZE,
    page_size: PAGE_SIZE,
    related_static: candidates,
  };

  return new Response(JSON.stringify(manifest), {
    headers: {
      'Content-Type': 'application/json; charset=utf-8',
      'Cache-Control': 'public, max-age=300, stale-while-revalidate=3600',
    },
  });
}
