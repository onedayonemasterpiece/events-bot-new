import type { PreviewEvent } from './types';

function plainText(html: string): string {
  return html
    .replace(/<br\s*\/?>/giu, ' ')
    .replace(/<[^>]+>/gu, ' ')
    .replace(/&nbsp;|&#160;/giu, ' ')
    .replace(/&quot;|&#34;/giu, '"')
    .replace(/\s+/gu, ' ')
    .trim();
}

/**
 * Desktop-only repair for an explicit source-labelled duration that has not
 * yet reached `time_range_end` in the preview export. This never infers a
 * duration from event type or surrounding prose.
 */
export function desktopEventWithExplicitEnd(event: PreviewEvent): PreviewEvent {
  if (event.time_range_end || !event.start_time) return event;
  const text = plainText(event.description_html || '');
  const match = /продолжительность(?:\s+[а-яё-]+){0,3}\s*(?:[:—–-]|составляет)\s*(?:(\d{1,2})\s*(?:ч(?:ас(?:а|ов)?)?))?\s*(?:(\d{1,3})\s*(?:мин(?:ут(?:а|ы)?)?))?/iu.exec(text);
  if (!match || (!match[1] && !match[2])) return event;
  const durationMinutes = Number(match[1] || 0) * 60 + Number(match[2] || 0);
  if (durationMinutes <= 0 || durationMinutes > 24 * 60) return event;
  const startMatch = /^(\d{2}):(\d{2})$/u.exec(event.start_time);
  if (!startMatch) return event;
  const startMinutes = Number(startMatch[1]) * 60 + Number(startMatch[2]);
  const endMinutes = startMinutes + durationMinutes;
  const endTime = `${String(Math.floor((endMinutes % (24 * 60)) / 60)).padStart(2, '0')}:${String(endMinutes % 60).padStart(2, '0')}`;
  return { ...event, time_range_end:endTime };
}
