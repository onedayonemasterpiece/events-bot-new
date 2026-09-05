import type { PreviewEvent } from './types';

export type ExhibitionPersonalBucket = 'new' | 'priority' | 'tail';
export type ExhibitionPersonalLifecycle = 'recent' | 'ending' | 'popular' | 'upcoming' | 'long';
export type ExhibitionPersonalCategory = 'Искусство' | 'История' | 'Фотография';

export interface ExhibitionPersonalItem {
  id: number;
  bucket: ExhibitionPersonalBucket;
  lifecycle: ExhibitionPersonalLifecycle;
  status: string;
  dateLabel: string;
  category: ExhibitionPersonalCategory;
  tags: string[];
  reasons: string[];
  event: PreviewEvent;
}

export interface ExhibitionPersonalProjection {
  items: ExhibitionPersonalItem[];
  newItems: ExhibitionPersonalItem[];
  priorityItems: ExhibitionPersonalItem[];
  tailItems: ExhibitionPersonalItem[];
  suppressed: {
    invalid: number;
    inactive: number;
    nonExhibition: number;
    duplicateId: number;
    duplicateTitle: number;
  };
}

const DAY_MS = 24 * 60 * 60 * 1000;
const NEW_RECENT_DAYS = 7;
const UPCOMING_DAYS = 21;
const LONG_RUNNING_DAYS = 21;
const ENDING_SOON_DAYS = 14;

const TOPIC_LABELS = new Map<string, string>([
  ['EXHIBITIONS', 'выставка'],
  ['KRAEVEDENIE_KALININGRAD_OBLAST', 'краеведение'],
  ['PERSONALITIES', 'персоналии'],
  ['FAMILY', 'семейное'],
  ['FASHION', 'костюм'],
  ['KIDS_SCHOOL', 'для детей'],
  ['PHOTOGRAPHY', 'фотография'],
  ['HISTORY', 'история'],
  ['ART', 'искусство'],
]);

function isoDateMs(value: string | null | undefined): number | null {
  if (!/^\d{4}-\d{2}-\d{2}$/u.test(value || '')) return null;
  const parsed = Date.parse(`${value}T00:00:00Z`);
  return Number.isFinite(parsed) ? parsed : null;
}

function dayDifference(left: number, right: number): number {
  return Math.round((left - right) / DAY_MS);
}

function russianDayNoun(value: number): string {
  const absolute = Math.abs(value);
  const mod100 = absolute % 100;
  const mod10 = absolute % 10;
  if (mod100 >= 11 && mod100 <= 14) return 'дней';
  if (mod10 === 1) return 'день';
  if (mod10 >= 2 && mod10 <= 4) return 'дня';
  return 'дней';
}

function displayDateValue(value: string, currentYear: number): string {
  const parsed = isoDateMs(value);
  if (parsed === null) return value;
  const date = new Date(parsed);
  const label = new Intl.DateTimeFormat('ru-RU', {
    timeZone: 'UTC',
    day: 'numeric',
    month: 'long',
    ...(date.getUTCFullYear() === currentYear ? {} : { year: 'numeric' as const }),
  }).format(date);
  return label.replace(/\s*г\.$/u, '');
}

export function normalizeExhibitionTitle(value: string): string {
  return String(value || '')
    .toLocaleLowerCase('ru')
    .replace(/^выставк[аи]\s+/u, '')
    .replace(/[«»„“”"']/gu, '')
    .replace(/[^\p{L}\p{N}]+/gu, ' ')
    .replace(/\s+/gu, ' ')
    .trim();
}

export function isExhibitionProjectionCandidate(event: Pick<PreviewEvent, 'event_type'>): boolean {
  const eventType = String(event.event_type || '').trim().toLocaleLowerCase('ru');
  // Keep the accepted donor's exact public boundary. Topic-adjacent tours,
  // lectures and concerts may carry EXHIBITIONS for discovery, but must not be
  // promoted into the exhibition entity list by the presentation layer.
  return eventType === 'выставка';
}

function categoryFor(event: Pick<PreviewEvent, 'topics'>): ExhibitionPersonalCategory {
  const topics = new Set(event.topics || []);
  if (topics.has('PHOTOGRAPHY')) return 'Фотография';
  if (topics.has('HISTORY') || topics.has('KRAEVEDENIE_KALININGRAD_OBLAST')) return 'История';
  return 'Искусство';
}

function tagsFor(event: Pick<PreviewEvent, 'topics'>, category: ExhibitionPersonalCategory): string[] {
  const mapped = (event.topics || [])
    .map((topic) => TOPIC_LABELS.get(topic))
    .filter((tag): tag is string => Boolean(tag));
  const fallback = category.toLocaleLowerCase('ru');
  return [...new Set(mapped.length ? mapped : [fallback])].slice(0, 3);
}

function statusFor({
  bucket,
  lifecycle,
  startDelta,
  daysSinceStart,
  endDelta,
}: {
  bucket: ExhibitionPersonalBucket;
  lifecycle: ExhibitionPersonalLifecycle;
  startDelta: number;
  daysSinceStart: number;
  endDelta: number | null;
}): string {
  if (startDelta > 0) {
    if (startDelta === 1) return 'Откроется завтра';
    return `Откроется через ${startDelta} ${russianDayNoun(startDelta)}`;
  }
  if (bucket === 'new') {
    if (daysSinceStart === 0) return 'Открылась сегодня';
    if (daysSinceStart === 1) return 'Открылась вчера';
    return `Открылась ${daysSinceStart} ${russianDayNoun(daysSinceStart)} назад`;
  }
  if (lifecycle === 'ending' && endDelta !== null) {
    if (endDelta === 0) return 'Последний день';
    if (endDelta === 1) return 'Закрывается завтра';
    return `Закроется через ${endDelta} ${russianDayNoun(endDelta)}`;
  }
  if (bucket === 'tail') return 'Идёт давно';
  return 'Идёт сейчас';
}

function reasonsFor({
  bucket,
  lifecycle,
  daysSinceStart,
  endDelta,
  event,
}: {
  bucket: ExhibitionPersonalBucket;
  lifecycle: ExhibitionPersonalLifecycle;
  daysSinceStart: number;
  endDelta: number | null;
  event: PreviewEvent;
}): string[] {
  if (bucket === 'new') {
    return lifecycle === 'upcoming' ? ['Скоро откроется'] : ['Недавно открылась'];
  }
  if (bucket === 'tail') return ['Полный список', 'Идёт больше трёх недель'];
  if (lifecycle === 'upcoming') return ['Откроется позже'];

  const reasons: string[] = [];
  if (lifecycle === 'ending' && endDelta !== null) {
    if (endDelta === 0) reasons.push('Заканчивается сегодня');
    else reasons.push(`Осталось ${endDelta} ${russianDayNoun(endDelta)}`);
  }
  const popularityReasons = new Set(event.popularity_reason_codes || []);
  if (popularityReasons.size > 0 || Number(event.likes_count || 0) > 0) reasons.push('Популярно в источниках');
  if (Number(event.shares_count || 0) > 0 || popularityReasons.has('frequently_shared')) reasons.push('Часто делятся');
  if (daysSinceStart >= 0 && daysSinceStart <= LONG_RUNNING_DAYS) reasons.push('Открылась недавно');
  return [...new Set(reasons.length ? reasons : ['Актуальная выставка'])].slice(0, 2);
}

function compareItemRank(left: ExhibitionPersonalItem, right: ExhibitionPersonalItem): number {
  const leftEnd = isoDateMs(left.event.end_date) ?? Number.POSITIVE_INFINITY;
  const rightEnd = isoDateMs(right.event.end_date) ?? Number.POSITIVE_INFINITY;
  if (left.bucket === 'new') {
    const leftStart = isoDateMs(left.event.start_date) ?? 0;
    const rightStart = isoDateMs(right.event.start_date) ?? 0;
    return rightStart - leftStart || right.id - left.id;
  }
  if (left.bucket === 'priority') {
    const lifecycleRank: Record<ExhibitionPersonalLifecycle, number> = {
      ending: 0,
      popular: 1,
      recent: 2,
      upcoming: 3,
      long: 4,
    };
    const lifecycleDelta = lifecycleRank[left.lifecycle] - lifecycleRank[right.lifecycle];
    if (lifecycleDelta) return lifecycleDelta;
    if (left.lifecycle === 'ending' && leftEnd !== rightEnd) return leftEnd - rightEnd;
    const engagementDelta = Number(right.event.likes_count || 0) + Number(right.event.shares_count || 0) * 3
      - Number(left.event.likes_count || 0) - Number(left.event.shares_count || 0) * 3;
    return engagementDelta || left.id - right.id;
  }
  const leftStart = isoDateMs(left.event.start_date) ?? 0;
  const rightStart = isoDateMs(right.event.start_date) ?? 0;
  return rightStart - leftStart || left.id - right.id;
}

export function projectExhibitionsPersonal(
  events: PreviewEvent[],
  currentDate: string,
): ExhibitionPersonalProjection {
  const currentMs = isoDateMs(currentDate);
  if (currentMs === null) throw new Error(`Invalid exhibitions projection date: ${currentDate}`);
  const currentYear = Number(currentDate.slice(0, 4));
  const suppressed = {
    invalid: 0,
    inactive: 0,
    nonExhibition: 0,
    duplicateId: 0,
    duplicateTitle: 0,
  };
  const seenIds = new Set<number>();
  const seenTitles = new Set<string>();
  const items: ExhibitionPersonalItem[] = [];

  for (const event of events) {
    if (event.lifecycle_status && event.lifecycle_status !== 'active') {
      suppressed.inactive += 1;
      continue;
    }
    if (!isExhibitionProjectionCandidate(event)) {
      suppressed.nonExhibition += 1;
      continue;
    }
    const startMs = isoDateMs(event.start_date);
    const endMs = event.end_date ? isoDateMs(event.end_date) : startMs;
    if (startMs === null || endMs === null || endMs < startMs || endMs < currentMs) {
      suppressed.invalid += 1;
      continue;
    }
    if (seenIds.has(event.id)) {
      suppressed.duplicateId += 1;
      continue;
    }
    seenIds.add(event.id);
    const titleKey = normalizeExhibitionTitle(event.title);
    if (!titleKey || seenTitles.has(titleKey)) {
      suppressed.duplicateTitle += 1;
      continue;
    }
    seenTitles.add(titleKey);

    const startDelta = dayDifference(startMs, currentMs);
    const daysSinceStart = Math.max(0, -startDelta);
    const endDelta = event.end_date && endMs !== null ? dayDifference(endMs, currentMs) : null;
    const recent = startDelta <= 0 && daysSinceStart <= NEW_RECENT_DAYS;
    const upcoming = startDelta > 0 && startDelta <= UPCOMING_DAYS;
    const ending = endDelta !== null && endDelta >= 0 && endDelta <= ENDING_SOON_DAYS;
    const popular = (event.popularity_reason_codes || []).length > 0 || Number(event.shares_count || 0) > 0;

    let bucket: ExhibitionPersonalBucket;
    let lifecycle: ExhibitionPersonalLifecycle;
    if (recent || upcoming) {
      bucket = 'new';
      lifecycle = upcoming ? 'upcoming' : 'recent';
    } else if (ending) {
      bucket = 'priority';
      lifecycle = 'ending';
    } else if (popular) {
      bucket = 'priority';
      lifecycle = 'popular';
    } else if (startDelta > 0) {
      bucket = 'priority';
      lifecycle = 'upcoming';
    } else if (daysSinceStart <= LONG_RUNNING_DAYS) {
      bucket = 'priority';
      lifecycle = 'recent';
    } else {
      bucket = 'tail';
      lifecycle = 'long';
    }

    const category = categoryFor(event);
    const dateValue = startDelta > 0 || !event.end_date ? event.start_date : event.end_date;
    const item: ExhibitionPersonalItem = {
      id: event.id,
      bucket,
      lifecycle,
      status: statusFor({ bucket, lifecycle, startDelta, daysSinceStart, endDelta }),
      dateLabel: `${startDelta > 0 || !event.end_date ? 'с' : 'до'} ${displayDateValue(dateValue, currentYear)}`,
      category,
      tags: tagsFor(event, category),
      reasons: reasonsFor({ bucket, lifecycle, daysSinceStart, endDelta, event }),
      event,
    };
    items.push(item);
  }

  const newItems = items.filter((item) => item.bucket === 'new').sort(compareItemRank);
  const priorityItems = items.filter((item) => item.bucket === 'priority').sort(compareItemRank);
  const tailItems = items.filter((item) => item.bucket === 'tail').sort(compareItemRank);

  return {
    items: [...newItems, ...priorityItems, ...tailItems],
    newItems,
    priorityItems,
    tailItems,
    suppressed,
  };
}


/** One unread-navigation model for every route, including SSR and restored tabs.
 * These are browser-local unread/interest decisions, not public reaction counts. */
export const EXHIBITIONS_STATE_KEY = 'ke_exhibitions_prototype_v1';
export function exhibitionsUnreadBadge(newIds: readonly (number | string)[], stored: unknown) {
  const state = stored && typeof stored === 'object' ? stored as Record<string, unknown> : {};
  const ids = (value: unknown) => new Set(Array.isArray(value) ? value.map(String) : []);
  const seen = ids(state.seenNew);
  const negative = ids(state.negative);
  const count = [...new Set(newIds.map(String))].filter(id => !seen.has(id) && !negative.has(id)).length;
  const soft = !count && state.hasVisitedExhibitions !== true && Number(state.siteVisits || 0) >= 5;
  return { count, hidden: !count && !soft, soft,
    text: count ? `${count} ${count === 1 ? 'новая' : 'новых'}` : soft ? 'загляните' : '' };
}
