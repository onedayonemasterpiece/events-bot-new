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
 * Transport-only end-time projection. Explicit source timing always wins;
 * Smart Update's persisted forecast is consumed only when no explicit end or
 * source-labelled duration reached the preview export. With neither, return
 * the event unchanged so each transport surface keeps its safe fallback.
 */
export function desktopEventWithExplicitEnd(event: PreviewEvent): PreviewEvent {
  if (event.time_range_end || !event.start_time) return event;
  const text = plainText(event.description_html || '');
  const match = /продолжительность(?:\s+[а-яё-]+){0,3}\s*(?:[:—–-]|составляет)\s*(?:(\d{1,2})\s*(?:ч(?:ас(?:а|ов)?)?))?\s*(?:(\d{1,3})\s*(?:мин(?:ут(?:а|ы)?)?))?/iu.exec(text);
  const explicitDurationMinutes = match && (match[1] || match[2])
    ? Number(match[1] || 0) * 60 + Number(match[2] || 0)
    : null;
  const forecastDurationMinutes = Number(event.duration_forecast_minutes);
  const durationMinutes = explicitDurationMinutes && explicitDurationMinutes <= 24 * 60
    ? explicitDurationMinutes
    : Number.isInteger(forecastDurationMinutes)
      && forecastDurationMinutes >= 15
      && forecastDurationMinutes <= 12 * 60
      ? forecastDurationMinutes
      : null;
  if (!durationMinutes) return event;
  const startMatch = /^(\d{2}):(\d{2})$/u.exec(event.start_time);
  if (!startMatch) return event;
  const startMinutes = Number(startMatch[1]) * 60 + Number(startMatch[2]);
  const endMinutes = startMinutes + durationMinutes;
  const endTime = `${String(Math.floor((endMinutes % (24 * 60)) / 60)).padStart(2, '0')}:${String(endMinutes % 60).padStart(2, '0')}`;
  return { ...event, time_range_end:endTime };
}
