import type { PreviewEvent } from './types';

export type EventFallbackArtKind = 'concert' | 'lecture';

export interface EventFallbackArt {
  kind: EventFallbackArtKind;
  src: string;
  width: 1280;
  height: 1280;
  fit: 'contain';
  provenance: string;
}

const FALLBACKS: Record<EventFallbackArtKind, EventFallbackArt> = {
  concert: {
    kind:'concert',
    src:'/assets/event-fallbacks/concert-symphonic.webp',
    width:1280,
    height:1280,
    fit:'contain',
    provenance:'docs/features/static-site-pages/symphonic concert.png',
  },
  lecture: {
    kind:'lecture',
    src:'/assets/event-fallbacks/lecture-meeting.webp',
    width:1280,
    height:1280,
    fit:'contain',
    provenance:'docs/features/static-site-pages/lecture (2).png',
  },
};

function normalizedEventType(value: unknown): string {
  return String(value || '').normalize('NFKC').toLocaleLowerCase('ru-RU').replace(/[«»"'`´’‘.,!?()[\]{}:;—–_/\\-]+/gu, ' ').replace(/\s+/gu, ' ').trim();
}

/** Presentation-only art. Callers must not add it to event media or metadata. */
export function resolveEventFallbackArt(
  event: Pick<PreviewEvent, 'event_type' | 'topics'>,
): EventFallbackArt | null {
  const eventType = normalizedEventType(event.event_type);
  const topics = new Set((event.topics || []).map((topic) => String(topic).toLocaleUpperCase('en-US')));
  if (['концерт', 'симфонический концерт', 'симфония'].includes(eventType) || topics.has('CONCERTS')) return FALLBACKS.concert;
  if (['лекция', 'встреча', 'публичная лекция'].includes(eventType) || topics.has('LECTURES') || topics.has('MEETUPS')) return FALLBACKS.lecture;
  return null;
}
