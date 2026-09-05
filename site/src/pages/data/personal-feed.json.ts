import {
  eventIntersectsDateRange,
  getCurrentDate,
  getEvents,
  getPreviewBuild,
} from '../../lib/events';
import type { PreviewEvent } from '../../lib/types';
import { catalogSortValue, toPersonalFeedCandidate } from '../../lib/personalization/catalog';

const MAX_CANDIDATES = 500;
const PAGE_SIZE = 30;

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
