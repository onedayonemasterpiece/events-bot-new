import type { PreviewEvent } from './types';

export type OccurrenceCollapseMode = 'none' | 'per-date' | 'per-family';

export type OccurrenceIssueCode =
  | 'dangling_link'
  | 'asymmetric_link'
  | 'inactive_member'
  | 'past_member'
  | 'range_member'
  | 'duplicate_slot'
  | 'missing_time';

export interface OccurrenceIssue {
  code: OccurrenceIssueCode;
  eventId: number;
  linkedEventId?: number;
}

export interface OccurrenceSlot {
  event: PreviewEvent;
  eventId: number;
  date: string;
  time: string | null;
  slotKey: string;
  current: boolean;
}

export interface OccurrenceDateRow {
  date: string;
  slots: OccurrenceSlot[];
}

export interface OccurrenceFamily {
  currentEventId: number;
  memberIds: number[];
  alternatives: PreviewEvent[];
  slots: OccurrenceSlot[];
  rows: OccurrenceDateRow[];
  hasAlternatives: boolean;
  issues: OccurrenceIssue[];
}

export interface OccurrencePresentation {
  family: OccurrenceFamily;
  compactLabel: string;
  railDateLine: string;
  railTimeLine: string | null;
  ariaLabel: string;
  isComplexSchedule: boolean;
}

export interface PopularEligibilityReference {
  currentDate: string;
  referenceIso: string;
}

const MONTHS_LONG = [
  'января', 'февраля', 'марта', 'апреля', 'мая', 'июня',
  'июля', 'августа', 'сентября', 'октября', 'ноября', 'декабря',
];
const MONTHS_SHORT = ['янв', 'фев', 'мар', 'апр', 'мая', 'июн', 'июл', 'авг', 'сен', 'окт', 'ноя', 'дек'];
const INELIGIBLE_POPULAR_LIFECYCLES = new Set(['cancelled', 'postponed', 'duplicate', 'merged', 'deleted', 'inactive']);

type DateParts = { year: number; month: number; day: number };

export function isExhibitionLikeEvent(
  event: Pick<PreviewEvent, 'title' | 'event_type' | 'topics'>,
): boolean {
  const haystack = [event.event_type, event.title, ...(event.topics || [])].join(' ').toLowerCase();
  return /выстав|экспозиц|музей|галере|арт[-\s]?простран|инсталляц|экзамен/u.test(haystack);
}

function parseDate(value: string): DateParts | null {
  const match = /^(\d{4})-(\d{2})-(\d{2})$/u.exec(String(value || ''));
  if (!match) return null;
  const parts = { year: Number(match[1]), month: Number(match[2]), day: Number(match[3]) };
  return parts.month >= 1 && parts.month <= 12 && parts.day >= 1 && parts.day <= 31 ? parts : null;
}

export function occurrenceTime(event: Pick<PreviewEvent, 'start_time' | 'display_time'>): string | null {
  const raw = String(event.start_time || event.display_time || '').trim();
  const match = /(\d{1,2}):(\d{2})/u.exec(raw);
  if (!match) return null;
  const hour = Number(match[1]);
  const minute = Number(match[2]);
  if (hour > 23 || minute > 59) return null;
  return `${String(hour).padStart(2, '0')}:${String(minute).padStart(2, '0')}`;
}

export function occurrenceSlotKey(event: Pick<PreviewEvent, 'id' | 'start_date' | 'start_time' | 'display_time'>): string {
  const time = occurrenceTime(event);
  // Unknown times are not equal slots: collapsing them would invent identity.
  return `${event.start_date}|${time || `unknown:${event.id}`}`;
}

function localReferenceParts(reference: Date, timezone: string): {
  date: string;
  seconds: number;
} | null {
  try {
    const parts = new Intl.DateTimeFormat('en-CA', {
      timeZone: timezone || 'Europe/Kaliningrad',
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
      hourCycle: 'h23',
    }).formatToParts(reference);
    const value = (type: Intl.DateTimeFormatPartTypes) => parts.find((part) => part.type === type)?.value || '';
    const year = value('year');
    const month = value('month');
    const day = value('day');
    const hour = Number(value('hour'));
    const minute = Number(value('minute'));
    const second = Number(value('second'));
    if (!year || !month || !day || !Number.isFinite(hour) || !Number.isFinite(minute) || !Number.isFinite(second)) return null;
    return {
      date: `${year}-${month}-${day}`,
      seconds: hour * 3600 + minute * 60 + second,
    };
  } catch {
    return null;
  }
}

/**
 * One availability contract for every Popular representation.
 *
 * A range is useful through its final calendar day, regardless of an
 * opening-day end_at. One-off events use their start instant on the current
 * day, while future calendar dates remain eligible without depending on a
 * potentially stale timestamp projection.
 */
export function isPopularEligible(
  event: Pick<
    PreviewEvent,
    | 'lifecycle_status'
    | 'title'
    | 'event_type'
    | 'topics'
    | 'start_date'
    | 'end_date'
    | 'starts_at'
    | 'start_time'
    | 'timezone'
  >,
  reference: PopularEligibilityReference,
): boolean {
  const lifecycle = String(event.lifecycle_status || '').trim().toLowerCase();
  if (INELIGIBLE_POPULAR_LIFECYCLES.has(lifecycle)) return false;

  const currentDate = String(reference.currentDate || '');
  if (!parseDate(event.start_date) || !parseDate(currentDate)) return false;

  const hasDateRange = Boolean(event.end_date && event.end_date !== event.start_date);
  if (hasDateRange) {
    if (!parseDate(event.end_date) || event.end_date < event.start_date) return false;
    if (event.end_date < currentDate) return false;
  }

  if (event.start_date > currentDate) return true;
  if (event.start_date < currentDate) {
    // An exhibition is continuously visitable through its advertised final
    // date. A past ordinary event carrying a broad/stale end_date is not.
    return hasDateRange && isExhibitionLikeEvent(event);
  }
  if (hasDateRange) return true;

  const referenceTime = Date.parse(reference.referenceIso || '');
  if (!Number.isFinite(referenceTime)) return false;

  const startsAt = Date.parse(event.starts_at || '');
  if (Number.isFinite(startsAt)) return startsAt >= referenceTime;

  const startTime = /^(\d{1,2}):(\d{2})(?::(\d{2}))?$/u.exec(String(event.start_time || '').trim());
  if (!startTime) return false;
  const hour = Number(startTime[1]);
  const minute = Number(startTime[2]);
  const second = Number(startTime[3] || 0);
  if (hour > 23 || minute > 59 || second > 59) return false;

  const localReference = localReferenceParts(new Date(referenceTime), event.timezone);
  if (!localReference) return false;
  if (event.start_date !== localReference.date) return event.start_date > localReference.date;
  return hour * 3600 + minute * 60 + second >= localReference.seconds;
}

function publicLifecycle(event: Pick<PreviewEvent, 'lifecycle_status'>): boolean {
  const status = String(event.lifecycle_status || '').trim().toLowerCase();
  return !status || status === 'active';
}

function eventOrder(left: PreviewEvent, right: PreviewEvent): number {
  return left.start_date.localeCompare(right.start_date)
    || String(occurrenceTime(left) || '99:99').localeCompare(String(occurrenceTime(right) || '99:99'))
    || left.id - right.id;
}

/**
 * Resolve one public occurrence family from explicit exported links only.
 * Title/type/venue similarity is deliberately not accepted as identity here.
 */
export function resolveOccurrenceFamily(
  current: PreviewEvent,
  catalog: PreviewEvent[],
  options: { currentDate?: string; includePast?: boolean } = {},
): OccurrenceFamily {
  const byId = new Map(catalog.map((event) => [Number(event.id), event]));
  const explicitIds = Array.from(new Set((current.other_date_ids || [])
    .map(Number)
    .filter((id) => Number.isFinite(id) && id !== current.id)));
  const issues: OccurrenceIssue[] = [];
  const candidates: PreviewEvent[] = [];

  for (const linkedId of explicitIds) {
    const candidate = byId.get(linkedId);
    if (!candidate) {
      issues.push({ code: 'dangling_link', eventId: current.id, linkedEventId: linkedId });
      continue;
    }
    const reverseIds = new Set((candidate.other_date_ids || []).map(Number));
    if (!reverseIds.has(current.id)) {
      issues.push({ code: 'asymmetric_link', eventId: current.id, linkedEventId: linkedId });
      continue;
    }
    if (!publicLifecycle(candidate)) {
      issues.push({ code: 'inactive_member', eventId: candidate.id });
      continue;
    }
    if (!options.includePast && options.currentDate && candidate.start_date < options.currentDate) {
      issues.push({ code: 'past_member', eventId: candidate.id });
      continue;
    }
    if (candidate.end_date && candidate.end_date !== candidate.start_date) {
      issues.push({ code: 'range_member', eventId: candidate.id });
      continue;
    }
    candidates.push(candidate);
  }

  const sorted = [current, ...candidates].sort(eventOrder);
  const unique = new Map<string, PreviewEvent>();
  for (const event of sorted) {
    const key = occurrenceSlotKey(event);
    const existing = unique.get(key);
    if (!existing) {
      unique.set(key, event);
      continue;
    }
    issues.push({ code: 'duplicate_slot', eventId: event.id, linkedEventId: existing.id });
    if (event.id === current.id) unique.set(key, event);
  }

  const members = [...unique.values()].sort(eventOrder);
  const slots = members.map((event) => ({
    event,
    eventId: event.id,
    date: event.start_date,
    time: occurrenceTime(event),
    slotKey: occurrenceSlotKey(event),
    current: event.id === current.id,
  }));
  for (const slot of slots) {
    if (!slot.time) issues.push({ code: 'missing_time', eventId: slot.eventId });
  }
  const grouped = new Map<string, OccurrenceSlot[]>();
  for (const slot of slots) {
    if (!grouped.has(slot.date)) grouped.set(slot.date, []);
    grouped.get(slot.date)?.push(slot);
  }
  const rows = [...grouped.entries()]
    .map(([date, rowSlots]) => ({ date, slots: rowSlots }))
    .sort((left, right) => left.date.localeCompare(right.date));
  const alternatives = members.filter((event) => event.id !== current.id);

  return {
    currentEventId: current.id,
    memberIds: members.map((event) => event.id),
    alternatives,
    slots,
    rows,
    hasAlternatives: alternatives.length > 0,
    issues,
  };
}

function joinHuman(values: string[], conjunction = 'и'): string {
  if (values.length <= 1) return values[0] || '';
  if (values.length === 2) return `${values[0]} ${conjunction} ${values[1]}`;
  return `${values.slice(0, -1).join(', ')} ${conjunction} ${values.at(-1)}`;
}

function formatSingleDate(value: string, style: 'long' | 'short', currentYear: number): string {
  const parts = parseDate(value);
  if (!parts) return value;
  const month = (style === 'short' ? MONTHS_SHORT : MONTHS_LONG)[parts.month - 1];
  return `${parts.day} ${month}${parts.year !== currentYear ? ` ${parts.year}` : ''}`;
}

function formatDateSet(values: string[], style: 'long' | 'short', currentYear: number, aria = false): string {
  const unique = Array.from(new Set(values));
  const parts = unique.map(parseDate);
  if (!unique.length) return '';
  if (parts.some((part) => !part)) return (aria ? joinHuman(unique) : unique.join(', '));
  const valid = parts as DateParts[];
  const groups = new Map<string, DateParts[]>();
  for (const part of valid) {
    const key = `${part.year}-${String(part.month).padStart(2, '0')}`;
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key)?.push(part);
  }
  const formattedGroups = [...groups.values()].map((group) => {
    const days = group.map((part) => String(part.day));
    const joinedDays = aria ? joinHuman(days) : days.join(', ');
    const month = (style === 'short' ? MONTHS_SHORT : MONTHS_LONG)[group[0].month - 1];
    return `${joinedDays} ${month}${group[0].year !== currentYear ? ` ${group[0].year}` : ''}`;
  });
  return aria ? joinHuman(formattedGroups) : formattedGroups.join(', ');
}

function dateRowText(row: OccurrenceDateRow, currentYear: number): string {
  const date = formatSingleDate(row.date, 'long', currentYear);
  const times = row.slots.map((slot) => slot.time).filter((time): time is string => Boolean(time));
  if (!times.length) return date;
  if (times.length !== row.slots.length) return `${date}, время уточняется`;
  return `${date} ${times.join(', ')}`;
}

export function formatOccurrencePresentation(family: OccurrenceFamily, currentDate: string): OccurrencePresentation {
  const currentYear = parseDate(currentDate)?.year || new Date().getUTCFullYear();
  const dates = family.rows.map((row) => row.date);
  const knownTimes = family.slots.map((slot) => slot.time).filter((time): time is string => Boolean(time));
  const everyTimeKnown = knownTimes.length === family.slots.length;
  const sameTime = everyTimeKnown && new Set(knownTimes).size === 1;
  const oneDate = family.rows.length === 1;
  let compactLabel = '';
  let railDateLine = formatDateSet(dates, 'short', currentYear);
  let railTimeLine: string | null = null;
  let ariaLabel = '';
  let isComplexSchedule = false;

  if (oneDate && everyTimeKnown) {
    const dateLong = formatSingleDate(dates[0], 'long', currentYear);
    const times = family.rows[0].slots.map((slot) => slot.time as string);
    compactLabel = `${dateLong} ${times.join(', ')}`;
    railDateLine = formatSingleDate(dates[0], 'short', currentYear);
    railTimeLine = times.join(', ');
    ariaLabel = `${dateLong} в ${joinHuman(times)}`;
  } else if (sameTime) {
    const dateLong = formatDateSet(dates, 'long', currentYear);
    const dateAria = formatDateSet(dates, 'long', currentYear, true);
    compactLabel = `${dateLong} ${knownTimes[0]}`;
    railDateLine = formatDateSet(dates, 'short', currentYear);
    railTimeLine = knownTimes[0];
    ariaLabel = `${dateAria} в ${knownTimes[0]}`;
  } else if (!knownTimes.length) {
    compactLabel = formatDateSet(dates, 'long', currentYear);
    railDateLine = formatDateSet(dates, 'short', currentYear);
    ariaLabel = formatDateSet(dates, 'long', currentYear, true);
    isComplexSchedule = family.slots.length > 1;
  } else {
    compactLabel = family.rows.map((row) => dateRowText(row, currentYear)).join('; ');
    railDateLine = formatDateSet(dates, 'short', currentYear);
    railTimeLine = everyTimeKnown ? 'разное время' : 'время уточняется';
    ariaLabel = compactLabel.replace(/; /gu, '; ');
    isComplexSchedule = true;
  }

  return { family, compactLabel, railDateLine, railTimeLine, ariaLabel, isComplexSchedule };
}

/**
 * Collapse only reciprocal explicit families. Input order determines the
 * representative: chronological surfaces should sort first; ranked surfaces
 * keep the highest-ranked first item.
 */
export function collapseOccurrenceCards(events: PreviewEvent[], mode: OccurrenceCollapseMode): PreviewEvent[] {
  if (mode === 'none') return [...events];
  const byId = new Map(events.map((event) => [event.id, event]));
  const parent = new Map(events.map((event) => [event.id, event.id]));
  const find = (value: number): number => {
    const next = parent.get(value) ?? value;
    if (next === value) return value;
    const root = find(next);
    parent.set(value, root);
    return root;
  };
  const union = (left: number, right: number) => {
    const leftRoot = find(left);
    const rightRoot = find(right);
    if (leftRoot !== rightRoot) parent.set(Math.max(leftRoot, rightRoot), Math.min(leftRoot, rightRoot));
  };
  for (const event of events) {
    for (const linkedId of (event.other_date_ids || []).map(Number)) {
      const linked = byId.get(linkedId);
      if (!linked || !(linked.other_date_ids || []).map(Number).includes(event.id)) continue;
      union(event.id, linked.id);
    }
  }
  const seen = new Set<string>();
  return events.filter((event) => {
    const family = find(event.id);
    const key = mode === 'per-date' ? `${family}|${event.start_date}` : String(family);
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

/**
 * Popular is a finite ranked selection, not a fixed-size carousel. Collapse
 * linked occurrences and exact repeated ids once, then return fewer cards
 * when there are fewer distinct families instead of repeating filler cards.
 */
export function selectPopularEventFamilies(events: PreviewEvent[], limit: number): PreviewEvent[] {
  return collapseOccurrenceCards(events, 'per-family').slice(0, Math.max(0, limit));
}
