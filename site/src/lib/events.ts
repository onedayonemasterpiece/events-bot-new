import previewData from '../data/preview-events.json';
import relatedData from '../data/preview-related.json';
import { eventImageUrl } from './assets';
import {
  collapseOccurrenceCards,
  formatOccurrencePresentation,
  isPopularEligible,
  resolveOccurrenceFamily,
  type OccurrenceFamily,
  type OccurrencePresentation,
  type PopularEligibilityReference,
} from './eventOccurrences';
import type {
  DiscoveryDisplayPayload,
  EventDetailRelatedManifest,
  EventFeatureSummary,
  PreviewData,
  PreviewEvent,
  RelatedData,
  RelatedManifestCandidate,
} from './types';

export { isPopularEligible };

export const SITE_NAME = 'Полюбить Калининград Анонсы';
export const SITE_ORIGIN = (import.meta.env.PUBLIC_SITE_ORIGIN || 'https://kenigevents.ru').replace(/\/+$/u, '');
export const BASE_PATH = (import.meta.env.BASE_URL || '/').replace(/\/$/u, '');
export type SiteMode = 'preview' | 'production' | 'secret_candidate';
const requestedSiteMode = String(import.meta.env.PUBLIC_SITE_MODE || 'preview');
export const SITE_MODE: SiteMode = requestedSiteMode === 'production'
  ? 'production'
  : (requestedSiteMode === 'secret_candidate' ? 'secret_candidate' : 'preview');
export const IS_PRODUCTION = SITE_MODE === 'production';
export const IS_SECRET_CANDIDATE = SITE_MODE === 'secret_candidate';
export const IS_PRODUCTION_FAMILY = IS_PRODUCTION || IS_SECRET_CANDIDATE;
export const PREVIEW_BUILD_ID = import.meta.env.PUBLIC_PREVIEW_BUILD_ID || 'local';
export const ICS_BASE_URL = (
  import.meta.env.PUBLIC_ICS_BASE_URL ||
  (import.meta.env.PUBLIC_ASSET_BASE_URL ? `${String(import.meta.env.PUBLIC_ASSET_BASE_URL).replace(/\/+$/u, '')}/ics` : '')
).replace(/\/+$/u, '');

const data = previewData as PreviewData;
const related = relatedData as RelatedData;
const previewCurrentDateOverride = String(import.meta.env.PUBLIC_STATIC_SITE_CURRENT_DATE || '').trim();
const previewGeneratedAtOverride = String(import.meta.env.PUBLIC_STATIC_SITE_REFERENCE_ISO || '').trim();
const hasCurrentDateOverride = /^\d{4}-\d{2}-\d{2}$/u.test(previewCurrentDateOverride);
const hasGeneratedAtOverride = Number.isFinite(Date.parse(previewGeneratedAtOverride));

export const RELATED_SCHEMA_VERSION = 'event-detail-related-v1' as const;
export const TAXONOMY_VERSION = 'event-taxonomy-v1' as const;
export const RELATED_SURFACE = 'event_detail_related' as const;
export const STATIC_RELATED_ALGORITHM_ID = 'static_related_v1' as const;
export const SPARSE_RELATED_ALGORITHM_ID = 'event_sparse_related_chain_v1' as const;
export const LEGACY_VECTOR_RELATED_ALGORITHM_ID = 'event_vector_related_chain_v2' as const;
export const PGVECTOR_RELATED_ALGORITHM_ID = 'event_pgvector_related_chain_v2_two_doc' as const;

export function getPreviewBuild() {
  if (!hasCurrentDateOverride && !hasGeneratedAtOverride) return data.build;
  return {
    ...data.build,
    ...(hasCurrentDateOverride ? { current_date: previewCurrentDateOverride } : {}),
    ...(hasGeneratedAtOverride ? { generated_at: previewGeneratedAtOverride } : {}),
  };
}

export function getEvents(): PreviewEvent[] {
  return [...data.events].sort((a, b) => {
    const av = a.starts_at || a.start_date;
    const bv = b.starts_at || b.start_date;
    return av.localeCompare(bv) || a.id - b.id;
  });
}

export function getCurrentDate(): string {
  return getPreviewBuild().current_date;
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

const RU_WEEKDAYS_SHORT = ['Вс', 'Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб'];

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

export function displayDateValue(value: string, currentYear = Number(getCurrentDate().slice(0, 4))): string {
  const parts = parseIsoDateParts(value);
  if (!parts) return value;
  return formatRuDate(value, parts.year !== currentYear);
}

export function displayDateRange(startDate: string, endDate: string): string {
  const currentYear = Number(getCurrentDate().slice(0, 4));
  const start = parseIsoDateParts(startDate);
  const end = parseIsoDateParts(endDate);
  if (!start || !end) return `${startDate} — ${endDate}`;
  const crossesYear = start.year !== end.year;
  if (!crossesYear && start.month === end.month) {
    return `${start.day}–${end.day} ${RU_MONTHS[end.month - 1]}${end.year !== currentYear ? ` ${end.year}` : ''}`;
  }
  if (!crossesYear) {
    return `${start.day} ${RU_MONTHS[start.month - 1]} — ${end.day} ${RU_MONTHS[end.month - 1]}${end.year !== currentYear ? ` ${end.year}` : ''}`;
  }
  return `${formatRuDate(startDate, true)} — ${formatRuDate(endDate, true)}`;
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

export function displayDateWithWeekday(event: Pick<PreviewEvent, 'start_date' | 'end_date'>): string {
  const parts = parseIsoDateParts(event.start_date);
  if (!parts) return displayDate(event);
  const date = new Date(Date.UTC(parts.year, parts.month - 1, parts.day));
  const weekday = RU_WEEKDAYS_SHORT[date.getUTCDay()];
  return weekday ? `${weekday}, ${displayDate(event)}` : displayDate(event);
}

export function displayDateTime(event: Pick<PreviewEvent, 'start_date' | 'end_date' | 'display_time'>): string {
  return [displayDate(event), event.display_time].filter(Boolean).join(' · ');
}

export function displayDateTimeWithWeekday(event: Pick<PreviewEvent, 'start_date' | 'end_date' | 'display_time'>): string {
  return [displayDateWithWeekday(event), event.display_time].filter(Boolean).join(' · ');
}

export function displayUpdatedAtKaliningrad(value: string | null | undefined): string | null {
  const raw = String(value || '').trim();
  if (!raw) return null;
  const sqliteLike = raw.match(/^(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?)(?![zZ]|[+-]\d{2}:?\d{2})/u);
  const normalized = sqliteLike
    ? `${sqliteLike[1]}T${sqliteLike[2]}Z`
    : (/^\d{4}-\d{2}-\d{2}T/u.test(raw) && !/[zZ]|[+-]\d{2}:?\d{2}$/u.test(raw)
      ? `${raw}Z`
      : raw);
  const date = new Date(normalized);
  if (!Number.isFinite(date.getTime())) return null;
  return new Intl.DateTimeFormat('ru-RU', {
    timeZone: 'Europe/Kaliningrad',
    day: '2-digit',
    month: 'long',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  }).format(date);
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

export function siteHomeHref(query = ''): string {
  return withBase(`${IS_PRODUCTION_FAMILY ? '/' : '/__preview/'}${query}`);
}

export function siteHomeAbsoluteUrl(query = ''): string {
  return new URL(siteHomeHref(query), `${SITE_ORIGIN}/`).toString();
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

export function eventCalendarHref(event: Pick<PreviewEvent, 'id' | 'slug'>): string {
  // A bearer-link candidate must be self-contained.  It may never mutate or
  // point at stable production /ics keys while it is under review.
  if (IS_SECRET_CANDIDATE) return withBase(`/sobytiya/${event.slug}/event.ics`);
  if (ICS_BASE_URL) return `${ICS_BASE_URL}/${event.id}.ics`;
  return withBase(`/sobytiya/${event.slug}/event.ics`);
}

export function eventCalendarStateExpiryDay(event: Pick<PreviewEvent, 'start_date' | 'end_date'>): number {
  const value = event.end_date || event.start_date;
  const match = /^(\d{4})-(\d{2})-(\d{2})$/u.exec(value || '');
  if (!match) return 0;
  const epochDay = Math.floor(Date.UTC(Number(match[1]), Number(match[2]) - 1, Number(match[3])) / 86400000);
  return Number.isFinite(epochDay) ? epochDay + 1 : 0;
}

export function isCalendarEligible(event: Pick<PreviewEvent, 'start_date' | 'end_date'>): boolean {
  return !event.end_date || event.end_date === event.start_date;
}

/**
 * Event-detail pages can export a complete date range. Compact listing cards
 * intentionally keep the narrower one-day rule above so their quick action
 * never implies a range the card does not explain.
 */
export function isEventDetailCalendarEligible(event: Pick<PreviewEvent, 'start_date' | 'end_date'>): boolean {
  if (!/^\d{4}-\d{2}-\d{2}$/u.test(event.start_date || '')) return false;
  if (!event.end_date) return true;
  if (!/^\d{4}-\d{2}-\d{2}$/u.test(event.end_date)) return false;
  return event.end_date >= event.start_date;
}

function isFutureStartingEvent(event: Pick<PreviewEvent, 'start_date'>): boolean {
  return event.start_date >= getCurrentDate();
}

export function getLinkedSessionIds(event: PreviewEvent): number[] {
  return getOccurrenceFamily(event).memberIds.filter((id) => id !== event.id);
}

export function collapseLinkedSessionEvents(events: PreviewEvent[]): PreviewEvent[] {
  return collapseOccurrenceCards(events, 'per-date');
}

export function getOccurrenceFamily(event: PreviewEvent): OccurrenceFamily {
  return resolveOccurrenceFamily(event, getEvents(), { currentDate: getCurrentDate() });
}

export function getOccurrencePresentation(event: PreviewEvent): OccurrencePresentation {
  return formatOccurrencePresentation(getOccurrenceFamily(event), getCurrentDate());
}

export function getOtherDates(event: PreviewEvent): PreviewEvent[] {
  return getOccurrenceFamily(event).alternatives;
}

export function getRelatedEvents(event: PreviewEvent, kind: 'similar' | 'explore'): PreviewEvent[] {
  const excludedIds = new Set(getOccurrenceFamily(event).memberIds);
  const ids = related.related[String(event.id)]?.[kind] || [];
  const ranked = ids
    .map((id) => getEventById(id))
    .filter((candidate): candidate is PreviewEvent => {
      if (!candidate) return false;
      if (excludedIds.has(candidate.id)) return false;
      return eventIntersectsDateRange(candidate, getCurrentDate(), '9999-12-31');
    });
  return collapseOccurrenceCards(ranked, 'per-family');
}

const TOPIC_TAGS: Record<string, string[]> = {
  CONCERTS: ['music', 'live_music'],
  EXHIBITIONS: ['exhibition', 'museum'],
  KRAEVEDENIE_KALININGRAD_OBLAST: ['local_history', 'tourist_friendly'],
  OPEN_AIR: ['open_air'],
  FAMILY: ['family'],
  KIDS_SCHOOL: ['kids', 'family'],
  MASTERCLASS: ['workshop'],
  HANDMADE: ['handmade'],
  FESTIVAL: ['festival'],
};

const EVENT_TYPE_TAGS: Array<[RegExp, { category: string; tags: string[] }]> = [
  [/концерт|симфони|музык/u, { category: 'music', tags: ['music', 'live_music'] }],
  [/выстав/u, { category: 'exhibition', tags: ['exhibition', 'museum'] }],
  [/спектак|театр/u, { category: 'theatre', tags: ['theatre'] }],
  [/лекц|встреч/u, { category: 'lecture', tags: ['lecture'] }],
  [/мастер|воркш/u, { category: 'workshop', tags: ['workshop'] }],
  [/ярмарк|фестив/u, { category: 'festival', tags: ['festival', 'open_air'] }],
  [/экскурс/u, { category: 'excursion', tags: ['excursion', 'tourist_friendly'] }],
  [/кин/u, { category: 'cinema', tags: ['cinema'] }],
  [/презента/u, { category: 'exhibition', tags: ['exhibition'] }],
];

function unique(values: Array<string | null | undefined>): string[] {
  return Array.from(new Set(values.filter((value): value is string => Boolean(value)).map((value) => value.trim()).filter(Boolean)));
}

function dayDistanceScore(current: PreviewEvent, candidate: PreviewEvent): number {
  const left = Date.parse(`${current.start_date}T00:00:00Z`);
  const right = Date.parse(`${candidate.start_date}T00:00:00Z`);
  if (!Number.isFinite(left) || !Number.isFinite(right)) return 0;
  const days = Math.abs(left - right) / 86400000;
  if (days <= 2) return 1;
  if (days <= 7) return 0.75;
  if (days <= 21) return 0.45;
  if (days <= 60) return 0.2;
  return 0.05;
}

export function eventCategory(event: PreviewEvent): string {
  const haystack = [event.event_type, ...(event.topics || [])].filter(Boolean).join(' ').toLowerCase();
  for (const [pattern, result] of EVENT_TYPE_TAGS) {
    if (pattern.test(haystack)) return result.category;
  }
  return event.event_type ? event.event_type.toLowerCase().replace(/\s+/gu, '_') : 'event';
}

export function eventTags(event: PreviewEvent): string[] {
  const values: string[] = [];
  const category = eventCategory(event);
  values.push(category);
  for (const topic of event.topics || []) values.push(...(TOPIC_TAGS[topic] || []));
  const type = (event.event_type || '').toLowerCase();
  for (const [pattern, result] of EVENT_TYPE_TAGS) {
    if (pattern.test(type)) values.push(...result.tags);
  }
  if (event.ticket.is_free) values.push('free');
  else values.push('ticketed');
  if (event.festival) values.push('festival');
  if (event.start_time) {
    const hour = Number(event.start_time.slice(0, 2));
    if (Number.isFinite(hour)) {
      if (hour >= 17) values.push('evening');
      if (hour < 13) values.push('daytime');
    }
  }
  return unique(values);
}

export function eventAudienceExclusionTags(_event: PreviewEvent): string[] {
  // Event-side exclusions are intentionally separate from visitor negative interests.
  // The preview fixture has no reliable exclusion facts yet, so keep this empty.
  return [];
}

export function eventFeatureSummary(event: PreviewEvent): EventFeatureSummary {
  return {
    event_id: event.id,
    title: event.title,
    category: eventCategory(event),
    tags: eventTags(event),
    audience_exclusion_tags: eventAudienceExclusionTags(event),
    city: event.city,
    location_name: event.venue_name,
    date: event.start_date,
    age_restriction: event.age_restriction,
    age_restriction_status: event.age_restriction_status,
  };
}

function relationKind(current: PreviewEvent, candidate: PreviewEvent): 'similar' | 'explore' | null {
  const entry = related.related[String(current.id)];
  if (entry?.similar?.includes(candidate.id)) return 'similar';
  if (entry?.explore?.includes(candidate.id)) return 'explore';
  return null;
}

function staticRelatedScore(current: PreviewEvent, candidate: PreviewEvent): { score: number; reasons: string[]; exploration: boolean } {
  const currentFeatures = eventFeatureSummary(current);
  const candidateFeatures = eventFeatureSummary(candidate);
  const reasons: string[] = [];
  let score = 0;
  const kind = relationKind(current, candidate);
  if (kind === 'similar') {
    score += 0.16;
    reasons.push('seed:similar');
  } else if (kind === 'explore') {
    score += 0.08;
    reasons.push('seed:explore');
  }
  if (currentFeatures.category === candidateFeatures.category) {
    score += 0.28;
    reasons.push(`same_category:${candidateFeatures.category}`);
  }
  const currentTags = new Set(currentFeatures.tags);
  const candidateTags = new Set(candidateFeatures.tags);
  const intersection = [...candidateTags].filter((tag) => currentTags.has(tag));
  const unionSize = new Set([...currentTags, ...candidateTags]).size || 1;
  const tagScore = intersection.length / unionSize;
  score += 0.24 * tagScore;
  reasons.push(...intersection.slice(0, 6).map((tag) => `tag:${tag}`));
  if (current.city && candidate.city && current.city === candidate.city) {
    score += 0.12;
    reasons.push('same_city');
  }
  const dateScore = dayDistanceScore(current, candidate);
  score += 0.12 * dateScore;
  if (dateScore >= 0.45) reasons.push('date_near');
  if (current.venue_name && candidate.venue_name && current.venue_name === candidate.venue_name) {
    score += 0.06;
    reasons.push('same_venue');
  }
  if (current.ticket.is_free === candidate.ticket.is_free) {
    score += 0.05;
    reasons.push('price_band_match');
  }
  if (candidate.lifecycle_status !== 'active') score -= 0.4;
  return {
    score: Number(Math.max(0, Math.min(1, score)).toFixed(4)),
    reasons: unique(reasons),
    exploration: kind === 'explore' || currentFeatures.category !== candidateFeatures.category,
  };
}

function eligibleRelatedCandidate(current: PreviewEvent, candidate: PreviewEvent | undefined): candidate is PreviewEvent {
  if (!candidate) return false;
  if (candidate.id === current.id) return false;
  if (getLinkedSessionIds(current).includes(candidate.id)) return false;
  if (getLinkedSessionIds(candidate).includes(current.id)) return false;
  if (candidate.lifecycle_status && candidate.lifecycle_status !== 'active') return false;
  return isFutureStartingEvent(candidate);
}

function toDiscoveryDisplayPayload(event: PreviewEvent): DiscoveryDisplayPayload {
  const likesCount = event.likes_count || 0;
  const occurrence = getOccurrencePresentation(event);
  const primaryAsset = event.image_assets?.find((asset) => asset.src === event.image_url) || event.image_assets?.[0];
  return {
    href: eventHref(event),
    absolute_url: eventAbsoluteUrl(event),
    event_type: event.event_type,
    image_url: eventImageUrl(event.image_url),
    image_alt: event.image_alt || `Афиша события «${event.title}»`,
    image_text_mode: event.image_text_mode,
    image_media_role: event.image_media_role,
    image_width: primaryAsset?.width || null,
    image_height: primaryAsset?.height || null,
    focal_y: event.focal_point?.y ?? null,
    display_date: displayDate(event),
    display_time: event.display_time,
    display_date_time: occurrence.compactLabel,
    occurrence_aria_label: occurrence.ariaLabel,
    occurrence_member_ids: occurrence.family.memberIds,
    city: event.city,
    venue_name: event.venue_name,
    place: [event.city, event.venue_name].filter(Boolean).join(' · '),
    status_label: event.status_label,
    price_label: eventAdmissionLabel(event),
    likes_count: likesCount,
    shares_count: event.shares_count ?? 0,
    calendar_href: eventCalendarHref(event),
    calendar_eligible: isCalendarEligible(event),
    age_restriction: event.age_restriction,
    age_restriction_status: event.age_restriction_status,
    age_recommendation: event.age_recommendation,
    age_recommendation_label: event.age_recommendation_label,
  };
}

export function toRelatedManifestCandidate(current: PreviewEvent, candidate: PreviewEvent): RelatedManifestCandidate {
  const scoring = staticRelatedScore(current, candidate);
  return {
    ...eventFeatureSummary(candidate),
    status: candidate.ticket.status || candidate.status_label || 'available',
    lifecycle_status: candidate.lifecycle_status,
    is_free: candidate.ticket.is_free,
    base_similarity: scoring.score,
    static_score: scoring.score,
    reason_codes: scoring.reasons,
    exploration_candidate: scoring.exploration,
    display: toDiscoveryDisplayPayload(candidate),
  };
}

function relatedChainItemFor(event: PreviewEvent, candidate: PreviewEvent): Record<string, unknown> | null {
  const chain = related.related[String(event.id)]?.chain || [];
  const item = chain.find((entry) => Number(entry.event_id) === candidate.id);
  return item || null;
}

function chainRelatedCandidate(event: PreviewEvent, candidate: PreviewEvent): RelatedManifestCandidate {
  const base = toRelatedManifestCandidate(event, candidate);
  const chainItem = relatedChainItemFor(event, candidate);
  if (!chainItem) return base;
  const score = Number(chainItem.related_score ?? chainItem.lexical_similarity ?? chainItem.vector_similarity ?? base.base_similarity);
  const lexicalSimilarity = Number(chainItem.lexical_similarity ?? score);
  const vectorSimilarity = Number(chainItem.vector_similarity ?? score);
  const deterministicScore = Number(chainItem.deterministic_score ?? base.static_score);
  return {
    ...base,
    base_similarity: Number.isFinite(score) ? score : base.base_similarity,
    static_score: Number.isFinite(score) ? score : base.static_score,
    slot_type: ['pure_related', 'adjacent_discovery', 'promo'].includes(String(chainItem.slot_type || '')) ? chainItem.slot_type as RelatedManifestCandidate['slot_type'] : undefined,
    lexical_similarity: Number.isFinite(lexicalSimilarity) && 'lexical_similarity' in chainItem ? lexicalSimilarity : undefined,
    vector_similarity: Number.isFinite(vectorSimilarity) && 'vector_similarity' in chainItem ? vectorSimilarity : undefined,
    deterministic_score: Number.isFinite(deterministicScore) ? deterministicScore : undefined,
    llm_semantic_score: Number.isFinite(Number(chainItem.llm_semantic_score)) ? Number(chainItem.llm_semantic_score) : undefined,
    llm_confidence: Number.isFinite(Number(chainItem.llm_confidence)) ? Number(chainItem.llm_confidence) : undefined,
    related_score: Number.isFinite(score) ? score : undefined,
    similarity_class: typeof chainItem.similarity_class === 'string' ? chainItem.similarity_class : undefined,
    retrieval_sources: Array.isArray(chainItem.retrieval_sources) ? chainItem.retrieval_sources.map(String) : undefined,
    reason_codes: unique([...(base.reason_codes || []), ...(Array.isArray(chainItem.reason_codes) ? chainItem.reason_codes.map(String) : [])]),
    exploration_candidate: base.exploration_candidate || chainItem.slot_type === 'adjacent_discovery' || chainItem.similarity_class === 'adjacent_discovery',
  };
}

export function getStaticRelatedCandidates(event: PreviewEvent, limit = 30): RelatedManifestCandidate[] {
  const entry = related.related[String(event.id)];
  const strictVerified = Boolean(related.strict_verified_related || entry?.strict_verified);
  const verifiedSimilarIds = strictVerified ? (entry?.similar || []) : [];
  const seededIds = verifiedSimilarIds.length
    ? verifiedSimilarIds
    : entry?.chain?.length && !strictVerified
    ? entry.chain.map((item) => Number(item.event_id)).filter((id) => Number.isFinite(id))
    : [
        ...(entry?.similar || []),
        ...(!strictVerified ? (entry?.explore || []) : []),
      ];
  const byId = new Map<number, PreviewEvent>();
  for (const id of seededIds) {
    const candidate = getEventById(id);
    if (eligibleRelatedCandidate(event, candidate)) byId.set(candidate.id, candidate);
  }
  if (!entry?.chain?.length && !strictVerified) {
    for (const candidate of getEvents()) {
      if (eligibleRelatedCandidate(event, candidate)) byId.set(candidate.id, candidate);
    }
  }
  const rankedEvents = [...byId.values()]
    .map((candidate) => chainRelatedCandidate(event, candidate))
    .sort((left, right) => right.base_similarity - left.base_similarity || left.event_id - right.event_id);
  const representatives = new Set(collapseOccurrenceCards(
    rankedEvents.map((candidate) => getEventById(candidate.event_id)).filter((candidate): candidate is PreviewEvent => Boolean(candidate)),
    'per-family',
  ).map((candidate) => candidate.id));
  return rankedEvents.filter((candidate) => representatives.has(candidate.event_id)).slice(0, Math.max(0, limit));
}

export function eventDetailRelatedAlgorithmId(): 'static_related_v1' | 'event_sparse_related_chain_v1' | 'event_pgvector_related_chain_v2_two_doc' {
  if (related.algorithm === PGVECTOR_RELATED_ALGORITHM_ID) return PGVECTOR_RELATED_ALGORITHM_ID;
  const isSparse = related.algorithm === SPARSE_RELATED_ALGORITHM_ID || related.algorithm === LEGACY_VECTOR_RELATED_ALGORITHM_ID;
  return isSparse ? SPARSE_RELATED_ALGORITHM_ID : STATIC_RELATED_ALGORITHM_ID;
}

export function buildEventDetailRelatedManifest(event: PreviewEvent, limit = 30): EventDetailRelatedManifest {
  const algorithmId = eventDetailRelatedAlgorithmId();
  return {
    version: 1,
    schema_version: RELATED_SCHEMA_VERSION,
    feature_schema_version: RELATED_SCHEMA_VERSION,
    taxonomy_version: TAXONOMY_VERSION,
    surface: RELATED_SURFACE,
    algorithm_id: algorithmId,
    generated_at: getPreviewBuild().generated_at,
    event_id: event.id,
    strategy: algorithmId === PGVECTOR_RELATED_ALGORITHM_ID
      ? 'event_pgvector_related_chain_v2_manifest'
      : (algorithmId === SPARSE_RELATED_ALGORITHM_ID ? 'event_sparse_related_chain_v1_manifest' : 'static_related_manifest_v1'),
    preload_target: 10,
    page_size: 10,
    current_event: eventFeatureSummary(event),
    related_static: getStaticRelatedCandidates(event, limit),
  };
}

export function getPreloadedDiscoveryEvents(event: PreviewEvent, limit = 10): PreviewEvent[] {
  return getStaticRelatedCandidates(event, limit)
    .map((candidate) => getEventById(candidate.event_id))
    .filter((candidate): candidate is PreviewEvent => Boolean(candidate));
}

export function getAdjacentDiscoveryEvents(event: PreviewEvent, excludeIds: Iterable<number> = [], limit = 6): PreviewEvent[] {
  const excluded = new Set([event.id, ...getLinkedSessionIds(event), ...Array.from(excludeIds)]);
  const adjacent = getStaticRelatedCandidates(event, 40)
    .filter((candidate) => candidate.slot_type === 'adjacent_discovery' || candidate.exploration_candidate || Number(candidate.base_similarity) < 0.76)
    .map((candidate) => getEventById(candidate.event_id))
    .filter((candidate): candidate is PreviewEvent => Boolean(candidate) && !excluded.has(candidate.id));
  const fallback = getEvents()
    .filter((candidate) => eligibleRelatedCandidate(event, candidate) && !excluded.has(candidate.id))
    .sort((left, right) => dayDistanceScore(event, right) - dayDistanceScore(event, left) || left.id - right.id);
  const byId = new Map<number, PreviewEvent>();
  for (const candidate of [...adjacent, ...fallback]) {
    if (!byId.has(candidate.id)) byId.set(candidate.id, candidate);
  }
  return collapseOccurrenceCards([...byId.values()], 'per-family').slice(0, Math.max(0, limit));
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
  image_media_role?: PreviewEvent['image_media_role'];
  image_width?: number | null;
  image_height?: number | null;
  focal_y?: number | null;
  display_date: string;
  display_time: string | null;
  display_date_time: string;
  occurrence_aria_label: string;
  occurrence_member_ids: number[];
  city: string | null;
  venue_name: string | null;
  place: string;
  status_label: string;
  price_label: string | null;
  likes_count: number;
  shares_count: number;
  calendar_href: string;
  calendar_eligible: boolean;
  age_restriction: PreviewEvent['age_restriction'];
  age_restriction_status: PreviewEvent['age_restriction_status'];
  age_recommendation: PreviewEvent['age_recommendation'];
  age_recommendation_label: string | null;
}

export function toDiscoveryEventPayload(event: PreviewEvent): DiscoveryEventPayloadItem {
  const likesCount = event.likes_count || 0;
  const primaryAsset = event.image_assets?.find((asset) => asset.src === event.image_url) || event.image_assets?.[0];
  const occurrence = getOccurrencePresentation(event);
  return {
    id: event.id,
    title: event.title,
    href: eventHref(event),
    absolute_url: eventAbsoluteUrl(event),
    event_type: event.event_type,
    image_url: eventImageUrl(event.image_url),
    image_alt: event.image_alt || `Афиша события «${event.title}»`,
    image_text_mode: event.image_text_mode,
    image_media_role: event.image_media_role,
    image_width: primaryAsset?.width || null,
    image_height: primaryAsset?.height || null,
    focal_y: event.focal_point?.y ?? null,
    display_date: displayDate(event),
    display_time: event.display_time,
    display_date_time: occurrence.compactLabel,
    occurrence_aria_label: occurrence.ariaLabel,
    occurrence_member_ids: occurrence.family.memberIds,
    city: event.city,
    venue_name: event.venue_name,
    place: [event.city, event.venue_name].filter(Boolean).join(' · '),
    status_label: event.status_label,
    price_label: eventAdmissionLabel(event),
    likes_count: likesCount,
    shares_count: event.shares_count ?? 0,
    calendar_href: eventCalendarHref(event),
    calendar_eligible: isCalendarEligible(event),
    age_restriction: event.age_restriction,
    age_restriction_status: event.age_restriction_status,
    age_recommendation: event.age_recommendation,
    age_recommendation_label: event.age_recommendation_label,
  };
}

function median(values: number[]): number {
  const sorted = values.filter((value) => Number.isFinite(value) && value > 0).sort((a, b) => a - b);
  if (!sorted.length) return 0;
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
}

function clamp(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, value));
}

export function eventPopularityScore(event: PreviewEvent): number {
  if (Number.isFinite(event.popularity_signal_score) && Number(event.popularity_signal_score) > 0) {
    return Number(event.popularity_signal_score);
  }
  const activeEvents = getEvents().filter((candidate) => eventIntersectsDateRange(candidate, getCurrentDate(), '9999-12-31'));
  const medianLikes = median(activeEvents.map((candidate) => candidate.source_likes_count || candidate.likes_count || 0));
  const medianShares = median(activeEvents.map((candidate) => candidate.shares_count || 0));
  const medianViews = median(activeEvents.map((candidate) => candidate.source_views_count || 0));
  const likes = event.source_likes_count || event.likes_count || 0;
  const serviceLikes = event.service_likes_count || 0;
  const shares = event.shares_count || 0;
  const views = event.source_views_count || 0;
  const sources = event.source_engagement_sources_count || 0;
  const likeScore = medianLikes > 0 ? clamp(likes / medianLikes, 0, 4) : Math.log1p(likes);
  const shareScore = medianShares > 0 ? clamp(shares / medianShares, 0, 4) : Math.log1p(shares);
  const viewScore = medianViews > 0 ? clamp(views / medianViews, 0, 4) : (views > 0 ? 1 : 0);
  const localScore = Math.log1p(serviceLikes);
  const sourceScore = Math.min(2, sources) / 2;
  return Number((likeScore * 0.38 + shareScore * 0.24 + viewScore * 0.24 + sourceScore * 0.10 + localScore * 0.04).toFixed(4));
}

function popularEligibilityReference(
  overrides: Partial<PopularEligibilityReference> = {},
): PopularEligibilityReference {
  return {
    currentDate: overrides.currentDate || getCurrentDate(),
    referenceIso: overrides.referenceIso || getPreviewBuild().generated_at,
  };
}

export function getPopularEvents(
  limit = 20,
  reference: Partial<PopularEligibilityReference> = {},
): PreviewEvent[] {
  const eligibilityReference = popularEligibilityReference(reference);
  const ranked = getEvents()
    .filter((event) => isPopularEligible(event, eligibilityReference))
    .map((event) => ({ event, score: eventPopularityScore(event) }))
    .filter((item) => item.score > 0 || (item.event.likes_count || 0) > 0 || (item.event.source_views_count || 0) > 0)
    .sort((left, right) => right.score - left.score || (left.event.starts_at || left.event.start_date).localeCompare(right.event.starts_at || right.event.start_date) || left.event.id - right.event.id)
    .map((item) => item.event);
  return collapseOccurrenceCards(ranked, 'per-family').slice(0, Math.max(0, limit));
}

/** @deprecated Use the shared isPopularEligible contract. */
export function isPopularDesktopEligible(
  event: PreviewEvent,
  referenceIso = getPreviewBuild().generated_at,
  currentDate = getCurrentDate(),
): boolean {
  return isPopularEligible(event, { currentDate, referenceIso });
}

export function getPopularDesktopEvents(
  limit = 100,
  reference: Partial<PopularEligibilityReference> = {},
): PreviewEvent[] {
  const eligibilityReference = popularEligibilityReference(reference);
  return getEvents()
    .filter((event) => isPopularEligible(event, eligibilityReference))
    .map((event) => ({ event, score: eventPopularityScore(event) }))
    .filter((item) => item.score > 0 || (item.event.likes_count || 0) > 0 || (item.event.source_views_count || 0) > 0)
    .sort((left, right) => right.score - left.score || (left.event.starts_at || left.event.start_date).localeCompare(right.event.starts_at || right.event.start_date) || left.event.id - right.event.id)
    .slice(0, Math.max(0, limit))
    .map((item) => item.event);
}

function repeatCountLabel(count: number): string {
  const mod100 = count % 100;
  const mod10 = count % 10;
  if (mod100 >= 11 && mod100 <= 14) return `${count} показов`;
  if (mod10 === 1) return `${count} показ`;
  if (mod10 >= 2 && mod10 <= 4) return `${count} показа`;
  return `${count} показов`;
}

export function popularDesktopTemporalLabel(event: PreviewEvent): string {
  const dateLabel = isMultiDayEvent(event)
    ? `Идёт до ${displayDateValue(event.end_date || event.start_date)}`
    : displayDateTime(event);
  const repeatCount = new Set((event.other_date_ids || []).filter((id) => id !== event.id)).size;
  return repeatCount > 0 ? `${dateLabel} · ещё ${repeatCountLabel(repeatCount)}` : dateLabel;
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

export function isMultiDayEvent(event: Pick<PreviewEvent, 'start_date' | 'end_date'>): boolean {
  return Boolean(event.end_date && event.end_date !== event.start_date);
}

export function isContinuingListingEvent(event: Pick<PreviewEvent, 'title' | 'event_type' | 'topics' | 'start_date' | 'end_date'>): boolean {
  if (!isMultiDayEvent(event)) return false;
  const haystack = [event.event_type, event.title, ...(event.topics || [])].join(' ').toLowerCase();
  return /выстав|экспозиц|музей|галере|фестив|ярмарк|маркет|лагер/u.test(haystack);
}

export function isExhibitionLikeEvent(event: Pick<PreviewEvent, 'title' | 'event_type' | 'topics'>): boolean {
  const haystack = [event.event_type, event.title, ...(event.topics || [])].join(' ').toLowerCase();
  return /выстав|экспозиц|музей|галере|арт[-\s]?простран|инсталляц|экзамен/u.test(haystack);
}

export function isLongRunningListingEvent(event: Pick<PreviewEvent, 'title' | 'event_type' | 'topics' | 'start_date' | 'end_date'>): boolean {
  return isMultiDayEvent(event) && (event.start_date !== event.end_date);
}

export type EventDaypart = 'morning' | 'day' | 'evening' | 'night';

export function getTomorrowDate(): string {
  return toIsoDate(addDays(new Date(`${getCurrentDate()}T00:00:00Z`), 1));
}

export function getTodayEvents(): PreviewEvent[] {
  const current = getCurrentDate();
  return getEvents().filter((event) => eventIntersectsDateRange(event, current, current));
}

export function getTodayPrimaryEvents(): PreviewEvent[] {
  const current = getCurrentDate();
  return collapseLinkedSessionEvents(
    getTodayEvents().filter((event) => event.start_date === current && !isExhibitionLikeEvent(event)),
  );
}

export function getTodayContinuingEvents(): PreviewEvent[] {
  return collapseLinkedSessionEvents(
    getTodayEvents().filter((event) => isContinuingListingEvent(event) || isExhibitionLikeEvent(event)),
  );
}

export function getOngoingExhibitionEvents(): PreviewEvent[] {
  const current = getCurrentDate();
  return collapseLinkedSessionEvents(getEvents().filter((event) => eventIntersectsDateRange(event, current, '9999-12-31') && (isExhibitionLikeEvent(event) || isContinuingListingEvent(event))));
}

export function getTomorrowEvents(): PreviewEvent[] {
  const tomorrow = getTomorrowDate();
  return collapseLinkedSessionEvents(getEvents().filter((event) => event.start_date === tomorrow && !isExhibitionLikeEvent(event)));
}

export function getDateEvents(date: string): PreviewEvent[] {
  return collapseLinkedSessionEvents(
    getEvents().filter((event) => event.start_date === date && !isExhibitionLikeEvent(event)),
  );
}

export function eventDaypart(event: Pick<PreviewEvent, 'start_time' | 'display_time'>): EventDaypart {
  const rawTime = event.start_time || event.display_time || '';
  const match = /(\d{1,2}):(\d{2})/u.exec(rawTime);
  if (!match) return 'day';
  const hour = Number(match[1]);
  if (hour >= 6 && hour < 12) return 'morning';
  if (hour >= 12 && hour < 17) return 'day';
  if (hour >= 17 && hour < 22) return 'evening';
  return 'night';
}

export function getWeekendRange(): { start: string; end: string; label: string } {
  const current = new Date(`${getCurrentDate()}T00:00:00Z`);
  const day = current.getUTCDay();
  const daysUntilSaturday = day === 0 ? -1 : day === 6 ? 0 : 6 - day;
  const saturday = addDays(current, daysUntilSaturday);
  const sunday = addDays(saturday, 1);
  const start = toIsoDate(saturday);
  const end = toIsoDate(sunday);
  return { start, end, label: displayDateRange(start, end) };
}

export function getWeekendEvents(): PreviewEvent[] {
  const range = getWeekendRange();
  return getWeekendEventsForRange(range.start, range.end);
}

export function getWeekendRangeForStart(start: string): { start: string; end: string; label: string } {
  const end = toIsoDate(addDays(new Date(`${start}T00:00:00Z`), 1));
  return { start, end, label: displayDateRange(start, end) };
}

export function getWeekendEventsForRange(start: string, end: string): PreviewEvent[] {
  return collapseLinkedSessionEvents(getEvents().filter((event) => event.start_date >= start && event.start_date <= end && !isExhibitionLikeEvent(event)));
}

export function getAvailableWeekendRanges(): Array<{ start: string; end: string; label: string; count: number }> {
  const starts = new Set<string>();
  for (const event of getEvents()) {
    if (isExhibitionLikeEvent(event)) continue;
    const date = new Date(`${event.start_date}T00:00:00Z`);
    const day = date.getUTCDay();
    if (day === 6) starts.add(event.start_date);
    else if (day === 0) starts.add(toIsoDate(addDays(date, -1)));
  }
  return [...starts].sort().map((start) => {
    const range = getWeekendRangeForStart(start);
    return { ...range, count: getWeekendEventsForRange(range.start, range.end).length };
  }).filter((range) => range.count > 0);
}

export function isExternalHttpUrl(href: string | null | undefined): boolean {
  return Boolean(href && /^https?:\/\//iu.test(href));
}

export function isTelephoneUrl(href: string | null | undefined): boolean {
  return Boolean(href && /^tel:/iu.test(href));
}

export function isTicketSoldOut(event: Pick<PreviewEvent, 'ticket' | 'status_label'>): boolean {
  const ticket = event.ticket;
  const statusText = [ticket.status, ticket.label, event.status_label, ticket.note]
    .filter(Boolean)
    .join(' ')
    .toLowerCase();
  return /sold|unavailable|not[_\s-]?available|нет\s+бил|законч|распрод/u.test(statusText);
}

export function eventActionHref(event: Pick<PreviewEvent, 'ticket' | 'status_label'>): string | null {
  if (isTicketSoldOut(event)) return null;
  return event.ticket.href || null;
}

export function formatDateMachine(date: string): string {
  return date;
}

export function getCtaLabel(event: PreviewEvent): string {
  return event.ticket.label;
}

export function eventAdmissionLabel(event: Pick<PreviewEvent, 'ticket' | 'status_label'>): string {
  const ticket = event.ticket;
  const statusText = [ticket.status, ticket.label, event.status_label, ticket.note].filter(Boolean).join(' ').toLowerCase();
  const hasRegistration = /регистрац|registration|зарегистр/u.test(statusText);
  const hasBooking = /запис|phone|телефон|коммент/u.test(statusText);
  const hasDonation = /донат|пожертв/u.test(statusText);
  if (isTicketSoldOut(event)) return 'Билеты закончились';
  if (ticket.is_free) {
    if (hasRegistration) return 'Бесплатно · регистрация';
    if (hasBooking) return 'Бесплатно · по записи';
    return 'Бесплатно · вход свободный';
  }
  if (ticket.price_label) return ticket.price_label;
  if (hasDonation) return 'За донат';
  if (ticket.kind === 'phone') return 'Запись по телефону';
  if (ticket.kind === 'ticket') return 'Билеты';
  if (/билет/u.test(statusText)) return 'Билеты';
  return event.status_label || ticket.label || 'Условия уточняются';
}

export function eventTicketActionLabel(event: PreviewEvent): string {
  if (isTicketSoldOut(event)) {
    return 'Билеты закончились';
  }
  if (event.ticket.kind === 'source') {
    return 'Открыть пост организатора';
  }
  if (event.ticket.kind === 'free') {
    return 'Источник события';
  }
  if (event.ticket.kind === 'registration') {
    return 'Зарегистрироваться';
  }
  return event.ticket.label;
}
