import type { PreviewEvent } from './types';

const DAY_MS = 86_400_000;
const ISO_DATE = /^\d{4}-\d{2}-\d{2}$/u;

function parseIsoDate(value: string): Date | null {
  if (!ISO_DATE.test(value)) return null;
  const date = new Date(`${value}T12:00:00Z`);
  if (!Number.isFinite(date.getTime())) return null;
  return toIsoDate(date) === value ? date : null;
}

function toIsoDate(date: Date): string {
  return date.toISOString().slice(0, 10);
}

function addDays(value: string, days: number): string {
  const date = parseIsoDate(value);
  if (!date) return value;
  return toIsoDate(new Date(date.getTime() + days * DAY_MS));
}

function endOfMonth(value: string): string {
  const date = parseIsoDate(value);
  if (!date) return value;
  return toIsoDate(new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth() + 1, 0, 12)));
}

function publicEventDate(
  event: Pick<PreviewEvent, 'start_date' | 'end_date' | 'lifecycle_status'>,
  today: string,
): string | null {
  if (event.lifecycle_status && event.lifecycle_status !== 'active') return null;
  const start = parseIsoDate(event.start_date || '') ? event.start_date : '';
  if (!start || start < today) return null;
  return start;
}

export interface EventDateAvailability {
  today: string;
  furthestEventDate: string;
  horizonEnd: string;
  availableDates: Set<string>;
  allDates: string[];
}

export interface EventDateManifest {
  schema_version: 'event-date-availability-v1';
  generated_at: string;
  current_date: string;
  furthest_event_date: string;
  horizon_end: string;
  dates: Array<{
    date: string;
    has_events: boolean;
  }>;
}

export interface TodayReviewResolution {
  state: 'current' | 'redirect' | 'stale';
  buildDate: string;
  runtimeDate: string;
  redirectDate: string | null;
}

/**
 * Build the one static calendar inventory shared by route generation and UI.
 *
 * Availability means that a generated date listing has at least one event
 * starting on that date. Multi-day spans are not expanded here: doing that
 * would publish links to empty date listings, because continuing exhibitions
 * and festivals have their own dedicated surfaces. The visible calendar
 * continues through the final day of the furthest start-date month.
 */
export function buildEventDateAvailability(
  events: Array<Pick<PreviewEvent, 'start_date' | 'end_date' | 'lifecycle_status'>>,
  today: string,
): EventDateAvailability {
  if (!parseIsoDate(today)) throw new Error(`Invalid calendar reference date: ${today}`);
  const eventDates = events
    .map((event) => publicEventDate(event, today))
    .filter((date): date is string => Boolean(date));
  const furthestEventDate = eventDates.reduce((furthest, date) => (
    date > furthest ? date : furthest
  ), today);
  const horizonEnd = endOfMonth(furthestEventDate);
  const availableDates = new Set(eventDates);

  const allDates: string[] = [];
  for (let date = today; date <= horizonEnd; date = addDays(date, 1)) {
    allDates.push(date);
  }
  return { today, furthestEventDate, horizonEnd, availableDates, allDates };
}

export function eventDateRouteDates(
  events: Array<Pick<PreviewEvent, 'start_date' | 'end_date' | 'lifecycle_status'>>,
  today: string,
): string[] {
  return [...buildEventDateAvailability(events, today).availableDates].sort();
}

/** JSON-safe static contract consumed by the calendar UI and stale-Today guard. */
export function eventDateManifest(
  availability: EventDateAvailability,
  generatedAt: string,
): EventDateManifest {
  return {
    schema_version: 'event-date-availability-v1',
    generated_at: generatedAt,
    current_date: availability.today,
    furthest_event_date: availability.furthestEventDate,
    horizon_end: availability.horizonEnd,
    dates: availability.allDates.map((date) => ({
      date,
      has_events: availability.availableDates.has(date),
    })),
  };
}

/** Resolve a real instant to the Kaliningrad civil date without using host TZ. */
export function kaliningradDate(now: Date = new Date()): string {
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Europe/Kaliningrad',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).formatToParts(now);
  const value = Object.fromEntries(parts.map((part) => [part.type, part.value]));
  return `${value.year}-${value.month}-${value.day}`;
}

/**
 * An immutable /segodnya/ page may outlive its build date. Redirect only when
 * the generated manifest proves that the actual date has a static route;
 * otherwise keep the page reachable but require an explicit stale-date label.
 */
export function resolveTodayReview(
  buildDate: string,
  runtimeDate: string,
  availableDates: Iterable<string>,
): TodayReviewResolution {
  if (!parseIsoDate(buildDate) || !parseIsoDate(runtimeDate)) {
    throw new Error(`Invalid Today review dates: ${buildDate}, ${runtimeDate}`);
  }
  if (runtimeDate === buildDate) {
    return { state:'current', buildDate, runtimeDate, redirectDate:null };
  }
  if (new Set(availableDates).has(runtimeDate)) {
    return { state:'redirect', buildDate, runtimeDate, redirectDate:runtimeDate };
  }
  return { state:'stale', buildDate, runtimeDate, redirectDate:null };
}
