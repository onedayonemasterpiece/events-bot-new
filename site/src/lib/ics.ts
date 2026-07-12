import type { PreviewEvent } from './types';
import { eventAbsoluteUrl } from './events';
import type { EventTrainOption, EventTransportDirection, EventTransportSuggestion } from './eventTransport';
export { eventIcsDownloadFilename, transportIcsDownloadFilename } from './icsFilenames.mjs';

function pad(value: number): string {
  return String(value).padStart(2, '0');
}

export function escapeIcsText(value: string | null | undefined): string {
  return String(value || '')
    .replace(/\\/gu, '\\\\')
    .replace(/;/gu, '\\;')
    .replace(/,/gu, '\\,')
    .replace(/\r?\n/gu, '\\n');
}

function formatUtcDateTime(iso: string): string {
  const date = new Date(iso);
  if (Number.isNaN(date.getTime())) {
    throw new Error(`Invalid ISO date for ICS: ${iso}`);
  }
  return `${date.getUTCFullYear()}${pad(date.getUTCMonth() + 1)}${pad(date.getUTCDate())}T${pad(date.getUTCHours())}${pad(date.getUTCMinutes())}${pad(date.getUTCSeconds())}Z`;
}

function formatDateOnly(date: string): string {
  return date.replace(/-/gu, '');
}

function foldLine(line: string): string {
  const limit = 73;
  let out = '';
  let current = '';
  const encoder = new TextEncoder();
  for (const char of line) {
    if (encoder.encode(current + char).length > limit) {
      out += current + '\r\n ';
      current = char;
    } else {
      current += char;
    }
  }
  return out + current;
}

export function buildIcs(event: PreviewEvent): string {
  const now = new Date();
  const stamp = `${now.getUTCFullYear()}${pad(now.getUTCMonth() + 1)}${pad(now.getUTCDate())}T${pad(now.getUTCHours())}${pad(now.getUTCMinutes())}${pad(now.getUTCSeconds())}Z`;
  const lines: string[] = [
    'BEGIN:VCALENDAR',
    'VERSION:2.0',
    'PRODID:-//KenigEvents//Static Event Preview//RU',
    'CALSCALE:GREGORIAN',
    'METHOD:PUBLISH',
    'BEGIN:VEVENT',
    `UID:event-${event.id}@kenigevents.ru`,
    `DTSTAMP:${stamp}`,
  ];

  if (event.starts_at) {
    lines.push(`DTSTART:${formatUtcDateTime(event.starts_at)}`);
    if (event.end_at && event.time_range_end) {
      lines.push(`DTEND:${formatUtcDateTime(event.end_at)}`);
    }
  } else {
    lines.push(`DTSTART;VALUE=DATE:${formatDateOnly(event.start_date)}`);
  }

  lines.push(
    `SUMMARY:${escapeIcsText(event.title)}`,
    `DESCRIPTION:${escapeIcsText([event.summary, event.venue_name, event.city, eventAbsoluteUrl(event)].filter(Boolean).join('\n'))}`,
  );

  const location = [event.venue_name, event.address, event.city].filter(Boolean).join(', ');
  if (location) {
    lines.push(`LOCATION:${escapeIcsText(location)}`);
  }

  lines.push(
    `URL:${eventAbsoluteUrl(event)}`,
    'END:VEVENT',
    'END:VCALENDAR',
  );

  return `${lines.map(foldLine).join('\r\n')}\r\n`;
}

export function buildTransportIcs(
  event: PreviewEvent,
  suggestion: EventTransportSuggestion,
  direction: EventTransportDirection,
  train: EventTrainOption,
): string {
  const now = new Date();
  const stamp = `${now.getUTCFullYear()}${pad(now.getUTCMonth() + 1)}${pad(now.getUTCDate())}T${pad(now.getUTCHours())}${pad(now.getUTCMinutes())}${pad(now.getUTCSeconds())}Z`;
  const from = direction === 'outbound' ? suggestion.originStation : suggestion.destinationStation;
  const to = direction === 'outbound' ? suggestion.destinationStation : suggestion.originStation;
  const directionLabel = direction === 'outbound' ? 'К событию' : 'Обратно после события';
  const safeNumber = train.number.replace(/[^a-zа-я0-9]+/giu, '-');
  const lines = [
    'BEGIN:VCALENDAR',
    'VERSION:2.0',
    'PRODID:-//KenigEvents//Event Transport//RU',
    'CALSCALE:GREGORIAN',
    'METHOD:PUBLISH',
    'BEGIN:VEVENT',
    `UID:transport-${event.id}-${direction}-${train.serviceDate}-${safeNumber}@kenigevents.ru`,
    `DTSTAMP:${stamp}`,
    `DTSTART:${formatUtcDateTime(train.departureIso)}`,
    `DTEND:${formatUtcDateTime(train.arrivalIso)}`,
    `SUMMARY:${escapeIcsText(`${directionLabel}: электричка ${train.departure}, ${from} → ${to}`)}`,
    `DESCRIPTION:${escapeIcsText([`Событие: ${event.title}`, `${train.trainType} № ${train.number}`, `Прибытие: ${train.arrival}`].join('\n'))}`,
    `LOCATION:${escapeIcsText(from)}`,
    `URL:${eventAbsoluteUrl(event)}`,
    'BEGIN:VALARM',
    'TRIGGER:-PT30M',
    'ACTION:DISPLAY',
    `DESCRIPTION:${escapeIcsText(`Электричка в ${train.departure}: пора собираться`)}`,
    'END:VALARM',
    'END:VEVENT',
    'END:VCALENDAR',
  ];
  return `${lines.map(foldLine).join('\r\n')}\r\n`;
}
