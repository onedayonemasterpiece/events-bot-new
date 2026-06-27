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

export function getOtherDates(event: PreviewEvent): PreviewEvent[] {
  return event.other_date_ids
    .map((id) => getEventById(id))
    .filter((candidate): candidate is PreviewEvent => Boolean(candidate));
}

export function getRelatedEvents(event: PreviewEvent, kind: 'similar' | 'explore'): PreviewEvent[] {
  const ids = related.related[String(event.id)]?.[kind] || [];
  return ids
    .map((id) => getEventById(id))
    .filter((candidate): candidate is PreviewEvent => Boolean(candidate) && candidate.id !== event.id);
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
