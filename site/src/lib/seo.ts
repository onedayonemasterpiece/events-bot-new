import type { PreviewEvent } from './types';
import { eventAbsoluteUrl, eventCalendarHref, isExternalHttpUrl, SITE_NAME, SITE_ORIGIN, withBase } from './events';

export function eventTitle(event: PreviewEvent): string {
  const place = [event.display_date, event.venue_name || event.city].filter(Boolean).join(', ');
  return `${event.title} — ${place} | ${SITE_NAME}`;
}

export function buildEventJsonLd(event: PreviewEvent) {
  const absolute = eventAbsoluteUrl(event);
  const location = event.venue_name || event.city ? {
    '@type': 'Place',
    name: event.venue_name || event.city,
    address: {
      '@type': 'PostalAddress',
      streetAddress: event.address || undefined,
      addressLocality: event.city || undefined,
      addressCountry: 'RU',
    },
  } : undefined;
  const offerUrl = isExternalHttpUrl(event.ticket.href) ? event.ticket.href : (isExternalHttpUrl(event.source_url) ? event.source_url : undefined);
  const offer = offerUrl ? {
    '@type': 'Offer',
    url: offerUrl,
    availability: event.ticket.status === 'sold_out'
      ? 'https://schema.org/SoldOut'
      : 'https://schema.org/InStock',
    price: event.ticket.is_free ? '0' : undefined,
    priceCurrency: event.ticket.is_free ? 'RUB' : undefined,
    validFrom: event.updated_at || undefined,
  } : undefined;

  return {
    '@context': 'https://schema.org',
    '@type': event.event_type === 'концерт' ? 'MusicEvent' : 'Event',
    name: event.title,
    description: event.meta_description || event.summary,
    startDate: event.starts_at || event.start_date,
    endDate: event.end_at || event.end_date || undefined,
    eventAttendanceMode: 'https://schema.org/OfflineEventAttendanceMode',
    eventStatus: event.lifecycle_status === 'cancelled'
      ? 'https://schema.org/EventCancelled'
      : event.lifecycle_status === 'postponed'
        ? 'https://schema.org/EventPostponed'
        : 'https://schema.org/EventScheduled',
    isAccessibleForFree: event.ticket.is_free,
    url: absolute,
    image: event.image_url ? [event.image_url] : undefined,
    location,
    offers: offer,
    organizer: event.festival ? {
      '@type': 'Organization',
      name: event.festival,
    } : undefined,
    mainEntityOfPage: {
      '@type': 'WebPage',
      '@id': absolute,
    },
    potentialAction: [
      {
        '@type': 'AddAction',
        target: new URL(eventCalendarHref(event), `${SITE_ORIGIN}/`).toString(),
        name: 'Добавить в календарь',
      },
    ],
  };
}

export function buildBreadcrumbJsonLd(event: PreviewEvent) {
  const items = [
    { name: SITE_NAME, item: new URL(withBase('/__preview/'), `${SITE_ORIGIN}/`).toString() },
    event.city ? { name: event.city, item: new URL(withBase(`/__preview/?city=${encodeURIComponent(event.city)}`), `${SITE_ORIGIN}/`).toString() } : undefined,
    { name: event.title, item: eventAbsoluteUrl(event) },
  ].filter(Boolean) as Array<{ name: string; item: string }>;

  return {
    '@context': 'https://schema.org',
    '@type': 'BreadcrumbList',
    itemListElement: items.map((item, index) => ({
      '@type': 'ListItem',
      position: index + 1,
      name: item.name,
      item: item.item,
    })),
  };
}
