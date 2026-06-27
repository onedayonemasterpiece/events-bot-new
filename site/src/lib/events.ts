import previewData from '../data/preview-events.json';
import relatedData from '../data/preview-related.json';
import type { PreviewData, PreviewEvent, RelatedData } from './types';

export const SITE_NAME = 'Полюбить Калининград Анонсы';
export const SITE_ORIGIN = (import.meta.env.PUBLIC_SITE_ORIGIN || 'https://kenigevents.ru').replace(/\/+$/u, '');
export const BASE_PATH = (import.meta.env.BASE_URL || '/').replace(/\/$/u, '');
export const PREVIEW_BUILD_ID = import.meta.env.PUBLIC_PREVIEW_BUILD_ID || 'local';

const data = previewData as PreviewData;
const related = relatedData as RelatedData;

export function getPreviewBuild() {
  return data.build;
}

export function getEvents(): PreviewEvent[] {
  return [...data.events].sort((a, b) => {
    const av = a.starts_at || a.start_date;
    const bv = b.starts_at || b.start_date;
    return av.localeCompare(bv) || a.id - b.id;
  });
}

export function getCurrentDate(): string {
  return data.build.current_date;
}

export function getEventBySlug(slug: string): PreviewEvent | undefined {
  return data.events.find((event) => event.slug === slug);
}

export function getEventById(id: number): PreviewEvent | undefined {
  return data.events.find((event) => event.id === id);
}

export function withBase(path: string): string {
  const normalized = path.startsWith('/') ? path : `/${path}`;
  if (!BASE_PATH) return normalized;
  return `${BASE_PATH}${normalized}`;
}

export function absoluteUrl(path: string): string {
  return new URL(withBase(path), `${SITE_ORIGIN}/`).toString();
}

export function eventPath(event: Pick<PreviewEvent, 'slug'>): string {
  return `/sobytiya/${event.slug}/`;
}

export function eventHref(event: Pick<PreviewEvent, 'slug'>): string {
  return withBase(eventPath(event));
}

export function eventAbsoluteUrl(event: Pick<PreviewEvent, 'slug'>): string {
  return absoluteUrl(eventPath(event));
}

export function eventCalendarHref(event: Pick<PreviewEvent, 'slug'>): string {
  return withBase(`/sobytiya/${event.slug}/event.ics`);
}

export function isCalendarEligible(event: Pick<PreviewEvent, 'start_date' | 'end_date'>): boolean {
  return !event.end_date || event.end_date === event.start_date;
}

export function getOtherDates(event: PreviewEvent): PreviewEvent[] {
  return event.other_date_ids
    .map((id) => getEventById(id))
    .filter((candidate): candidate is PreviewEvent => Boolean(candidate));
}

export function getRelatedEvents(event: PreviewEvent, kind: 'similar' | 'explore'): PreviewEvent[] {
  const excludedIds = new Set([event.id, ...event.other_date_ids]);
  const ids = related.related[String(event.id)]?.[kind] || [];
  return ids
    .map((id) => getEventById(id))
    .filter((candidate): candidate is PreviewEvent => {
      if (!candidate) return false;
      if (excludedIds.has(candidate.id)) return false;
      if (candidate.other_date_ids.includes(event.id)) return false;
      return eventIntersectsDateRange(candidate, getCurrentDate(), '9999-12-31');
    });
}

function addDays(date: Date, days: number): Date {
  const next = new Date(date);
  next.setUTCDate(next.getUTCDate() + days);
  return next;
}

function toIsoDate(date: Date): string {
  return date.toISOString().slice(0, 10);
}

export function eventIntersectsDateRange(event: PreviewEvent, fromDate: string, toDate: string): boolean {
  const starts = event.start_date;
  const ends = event.end_date || event.start_date;
  return starts <= toDate && ends >= fromDate;
}

export function getTodayEvents(): PreviewEvent[] {
  const current = getCurrentDate();
  return getEvents().filter((event) => eventIntersectsDateRange(event, current, current));
}

export function getWeekendRange(): { start: string; end: string; label: string } {
  const current = new Date(`${getCurrentDate()}T00:00:00Z`);
  const day = current.getUTCDay();
  const daysUntilSaturday = day === 0 ? -1 : day === 6 ? 0 : 6 - day;
  const saturday = addDays(current, daysUntilSaturday);
  const sunday = addDays(saturday, 1);
  const start = toIsoDate(saturday);
  const end = toIsoDate(sunday);
  return { start, end, label: `${start} — ${end}` };
}

export function getWeekendEvents(): PreviewEvent[] {
  const range = getWeekendRange();
  return getEvents().filter((event) => eventIntersectsDateRange(event, range.start, range.end));
}

export function isExternalHttpUrl(href: string | null | undefined): boolean {
  return Boolean(href && /^https?:\/\//iu.test(href));
}

export function isTelephoneUrl(href: string | null | undefined): boolean {
  return Boolean(href && /^tel:/iu.test(href));
}

export function formatDateMachine(date: string): string {
  return date;
}

export function getCtaLabel(event: PreviewEvent): string {
  return event.ticket.label;
}

export function eventTicketActionLabel(event: PreviewEvent): string {
  if (event.ticket.kind === 'source') {
    return 'Открыть пост организатора';
  }
  if (event.ticket.kind === 'free') {
    return 'Открыть условия';
  }
  return event.ticket.label;
}
