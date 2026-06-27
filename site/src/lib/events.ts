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

const RU_MONTHS = [
  'января',
  'февраля',
  'марта',
  'апреля',
  'мая',
  'июня',
  'июля',
  'августа',
  'сентября',
  'октября',
  'ноября',
  'декабря',
];

function parseIsoDateParts(value: string): { year: number; month: number; day: number } | null {
  const match = /^(\d{4})-(\d{2})-(\d{2})$/u.exec(value || '');
  if (!match) return null;
  return { year: Number(match[1]), month: Number(match[2]), day: Number(match[3]) };
}

function formatRuDate(value: string, includeYear: boolean): string {
  const parts = parseIsoDateParts(value);
  if (!parts || parts.month < 1 || parts.month > 12) return value;
  return `${parts.day} ${RU_MONTHS[parts.month - 1]}${includeYear ? ` ${parts.year}` : ''}`;
}

export function displayDate(event: Pick<PreviewEvent, 'start_date' | 'end_date'>): string {
  const currentYear = Number(getCurrentDate().slice(0, 4));
  const start = parseIsoDateParts(event.start_date);
  const endDate = event.end_date || event.start_date;
  const end = parseIsoDateParts(endDate);
  if (!start || !end) return event.end_date && event.end_date !== event.start_date ? `${event.start_date} — до ${event.end_date}` : event.start_date;
  const crossesYear = start.year !== end.year;
  const includeStartYear = crossesYear || start.year !== currentYear;
  const includeEndYear = crossesYear || end.year !== currentYear;
  if (event.end_date && event.end_date !== event.start_date) {
    return `${formatRuDate(event.start_date, includeStartYear)} — до ${formatRuDate(event.end_date, includeEndYear)}`;
  }
  return formatRuDate(event.start_date, includeStartYear);
}

export function displayDateTime(event: Pick<PreviewEvent, 'start_date' | 'end_date' | 'display_time'>): string {
  return [displayDate(event), event.display_time].filter(Boolean).join(' · ');
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

export function getPreloadedDiscoveryEvents(event: PreviewEvent, limit = 10): PreviewEvent[] {
  const excludedIds = new Set([event.id, ...event.other_date_ids]);
  const result: PreviewEvent[] = [];
  const add = (candidate: PreviewEvent | undefined) => {
    if (!candidate) return;
    if (excludedIds.has(candidate.id)) return;
    if (candidate.other_date_ids.includes(event.id)) return;
    if (!eventIntersectsDateRange(candidate, getCurrentDate(), '9999-12-31')) return;
    if (result.some((item) => item.id === candidate.id)) return;
    result.push(candidate);
  };
  getRelatedEvents(event, 'similar').forEach(add);
  getRelatedEvents(event, 'explore').forEach(add);
  getEvents().forEach(add);
  return result.slice(0, Math.max(0, limit));
}

export interface DiscoveryEventPayloadItem {
  id: number;
  title: string;
  href: string;
  absolute_url: string;
  event_type: string | null;
  image_url: string | null;
  image_alt: string;
  image_text_mode: PreviewEvent['image_text_mode'];
  display_date: string;
  display_time: string | null;
  display_date_time: string;
  city: string | null;
  venue_name: string | null;
  place: string;
  status_label: string;
  price_label: string | null;
  likes_count: number;
  shares_count: number;
}

export function toDiscoveryEventPayload(event: PreviewEvent): DiscoveryEventPayloadItem {
  const likesCount = event.likes_count || 0;
  return {
    id: event.id,
    title: event.title,
    href: eventHref(event),
    absolute_url: eventAbsoluteUrl(event),
    event_type: event.event_type,
    image_url: event.image_url,
    image_alt: event.image_alt || `Афиша события «${event.title}»`,
    image_text_mode: event.image_text_mode,
    display_date: displayDate(event),
    display_time: event.display_time,
    display_date_time: displayDateTime(event),
    city: event.city,
    venue_name: event.venue_name,
    place: [event.city, event.venue_name].filter(Boolean).join(' · '),
    status_label: event.status_label,
    price_label: event.ticket.price_label,
    likes_count: likesCount,
    shares_count: event.shares_count ?? Math.max(0, Math.round(likesCount * 0.18)),
  };
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
