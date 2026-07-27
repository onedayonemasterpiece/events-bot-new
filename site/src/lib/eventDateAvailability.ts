import type { PreviewEvent } from './types';

const DAY_MS = 86_400_000;
const ISO_DATE = /^\d{4}-\d{2}-\d{2}$/u;

function parseIsoDate(value: string): Date | null {
  if (!ISO_DATE.test(value)) return null;
  const date = new Date(`${value}T12:00:00Z`);
  return Number.isFinite(date.getTime()) ? date : null;
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

function publicEventDates(
  event: Pick<PreviewEvent, 'start_date' | 'end_date' | 'lifecycle_status'>,
  today: string,
): { start: string; end: string } | null {
  if (event.lifecycle_status && event.lifecycle_status !== 'active') return null;
  const start = ISO_DATE.test(event.start_date || '') ? event.start_date : '';
  const endCandidate = ISO_DATE.test(event.end_date || '') ? String(event.end_date) : start;
  if (!start || !endCandidate || endCandidate < start || endCandidate < today) return null;
  return { start: start < today ? today : start, end: endCandidate };
}

export interface EventDateAvailability {
  today: string;
  furthestEventDate: string;
  horizonEnd: string;
  availableDates: Set<string>;
  allDates: string[];
}

/**
 * Build the one static calendar inventory shared by route generation and UI.
 *
 * Multi-day events make every covered day available. The visible calendar
 * continues through the final day of the furthest event month, while days
 * without an event remain present only as disabled, non-link UI cells.
 */
export function buildEventDateAvailability(
  events: Array<Pick<PreviewEvent, 'start_date' | 'end_date' | 'lifecycle_status'>>,
  today: string,
): EventDateAvailability {
  if (!ISO_DATE.test(today)) throw new Error(`Invalid calendar reference date: ${today}`);
  const ranges = events
    .map((event) => publicEventDates(event, today))
    .filter((range): range is { start: string; end: string } => Boolean(range));
  const furthestEventDate = ranges.reduce((furthest, range) => (
    range.end > furthest ? range.end : furthest
  ), today);
  const horizonEnd = endOfMonth(furthestEventDate);
  const availableDates = new Set<string>();

  for (const range of ranges) {
    for (let date = range.start; date <= range.end; date = addDays(date, 1)) {
      availableDates.add(date);
    }
  }

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
