import {
  buildEventDateAvailability,
  eventDateManifest,
  type EventDateAvailability,
  type EventDateManifest,
} from './eventDateAvailability';
import {
  getCurrentDate,
  getEvents,
  getPreviewBuild,
  isExhibitionLikeEvent,
} from './events';

/**
 * Keep route generation, mobile cells, the public manifest and Today runtime
 * guard on the exact same date-listing inventory.
 */
export function getStaticEventDateAvailability(): EventDateAvailability {
  const dateListingEvents = getEvents().filter((event) => !isExhibitionLikeEvent(event));
  return buildEventDateAvailability(dateListingEvents, getCurrentDate());
}

export function getStaticEventDateManifest(): EventDateManifest {
  return eventDateManifest(
    getStaticEventDateAvailability(),
    getPreviewBuild().generated_at,
  );
}
