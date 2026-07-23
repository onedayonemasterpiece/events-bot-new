import type { EventImageAsset, PreviewEvent } from './types';
import listingMediaOverrides from '../data/listingMediaOverrides.json';

export type ListingDaypart = 'morning' | 'day' | 'evening' | 'night' | 'untimed';

export interface ListingTimeGroup {
  key: string;
  label: string;
  daypart: ListingDaypart;
  events: PreviewEvent[];
}

export type PopularListingReason = 'fast_growth' | 'frequently_shared' | 'discussed' | 'multi_source' | 'score_fallback';

export interface PopularListingGroup {
  key: PopularListingReason;
  label: string;
  events: PreviewEvent[];
}

export interface PopularDesktopListingResult {
  groups: PopularListingGroup[];
  allocatedFamilyKeys: string[];
}

export interface ListingImagePresentation {
  asset: EventImageAsset | null;
  src: string | null;
  ratio: number;
  mode: 'poster-natural' | 'photo-natural' | 'photo-crop' | 'visual-natural' | 'visual-crop' | 'unknown-natural';
  adaptiveCrop: boolean;
  objectPosition: string;
  verticalRetention: number;
}

export function exactListingTime(event: Pick<PreviewEvent, 'start_time' | 'display_time'>): string | null {
  const match = /(\d{1,2}):(\d{2})/u.exec(event.start_time || event.display_time || '');
  return match ? `${match[1].padStart(2, '0')}:${match[2]}` : null;
}

export function listingDaypart(time: string | null): ListingDaypart {
  if (!time) return 'untimed';
  const hour = Number(time.slice(0, 2));
  if (hour >= 6 && hour < 12) return 'morning';
  if (hour >= 12 && hour < 17) return 'day';
  if (hour >= 17 && hour < 22) return 'evening';
  return 'night';
}

export const LISTING_DAYPARTS: Array<{ key: Exclude<ListingDaypart, 'untimed'>; label: string }> = [
  { key: 'morning', label: 'Утро' },
  { key: 'day', label: 'День' },
  { key: 'evening', label: 'Вечер' },
  { key: 'night', label: 'Ночь' },
];

export function eventCountLabel(count: number): string {
  const mod100 = count % 100;
  if (mod100 >= 11 && mod100 <= 14) return 'событий';
  const mod10 = count % 10;
  if (mod10 === 1) return 'событие';
  if (mod10 >= 2 && mod10 <= 4) return 'события';
  return 'событий';
}

function normalizedIdentityPart(value: unknown): string {
  return String(value || '').toLocaleLowerCase('ru-RU').replace(/[^a-zа-яё0-9]+/gu, ' ').trim();
}

export function listingEventFamilyKey(event: PreviewEvent): string {
  return [
    normalizedIdentityPart(event.title),
    normalizedIdentityPart(event.event_type),
    normalizedIdentityPart(event.venue_name || event.city),
  ].join('|');
}

export function deduplicateListingEvents(items: PreviewEvent[]): PreviewEvent[] {
  const byIdentity = new Map<string, PreviewEvent>();
  for (const event of items) {
    const key = [
      normalizedIdentityPart(event.title),
      event.start_date,
      exactListingTime(event) || '',
      normalizedIdentityPart(event.venue_name),
    ].join('|');
    const current = byIdentity.get(key);
    if (!current || event.id > current.id) byIdentity.set(key, event);
  }
  return [...byIdentity.values()];
}

/**
 * Allocate behavioral Popular shelves once, in product priority order. A
 * repeating production is one discovery object here even when separate dates
 * have not yet been linked through other_date_ids.
 */
export function buildPopularListingGroups(items: PreviewEvent[], perGroup = 5): PopularListingGroup[] {
  const limit = Math.max(1, perGroup);
  const allocated = new Set<string>();
  const candidates = deduplicateListingEvents(items);
  const inputRank = new Map(candidates.map((event, index) => [event.id, index]));
  const familyKey = (event: PreviewEvent) => [
    normalizedIdentityPart(event.title),
    normalizedIdentityPart(event.event_type),
    normalizedIdentityPart(event.venue_name || event.city),
  ].join('|');
  const stableInputOrder = (left: PreviewEvent, right: PreviewEvent) => (
    (inputRank.get(left.id) ?? Number.MAX_SAFE_INTEGER) - (inputRank.get(right.id) ?? Number.MAX_SAFE_INTEGER)
  );
  const sharedEvidenceOrder = (left: PreviewEvent, right: PreviewEvent) => (
    Number(right.shares_count || 0) - Number(left.shares_count || 0)
    || Number(right.likes_count || 0) - Number(left.likes_count || 0)
    || Number(right.source_views_count || 0) - Number(left.source_views_count || 0)
    || stableInputOrder(left, right)
  );
  const definitions: Array<{
    key: PopularListingReason;
    label: string;
    matches: (event: PreviewEvent) => boolean;
    order?: (left: PreviewEvent, right: PreviewEvent) => number;
  }> = [
    { key: 'fast_growth', label: 'Быстро набирают популярность', matches: (event) => event.popularity_reason_codes?.includes('fast_growth') === true },
    { key: 'multi_source', label: 'Встречается во множестве источников', matches: (event) => event.popularity_reason_codes?.includes('multi_source') === true },
    { key: 'discussed', label: 'Активно обсуждают', matches: (event) => event.popularity_reason_codes?.includes('discussed') === true },
    { key: 'frequently_shared', label: 'Часто делятся', matches: (event) => event.popularity_reason_codes?.includes('frequently_shared') === true, order: sharedEvidenceOrder },
    { key: 'score_fallback', label: 'Популярное сейчас', matches: () => true },
  ];
  const groups: PopularListingGroup[] = [];
  for (const definition of definitions) {
    const events = candidates.filter((event) => {
      const key = familyKey(event);
      return !allocated.has(key) && definition.matches(event);
    }).sort(definition.order || stableInputOrder).slice(0, limit);
    if (!events.length || (definition.key !== 'score_fallback' && events.length < 3)) continue;
    events.forEach((event) => allocated.add(familyKey(event)));
    groups.push({ key: definition.key, label: definition.label, events });
  }
  return groups;
}

interface PopularFamily {
  key: string;
  events: PreviewEvent[];
  representative: PreviewEvent;
  rank: number;
  reasons: Set<PopularListingReason>;
}

function aggregatePopularFamilies(items: PreviewEvent[]): PopularFamily[] {
  const candidates = deduplicateListingEvents(items);
  const families = new Map<string, PopularFamily>();
  candidates.forEach((event, rank) => {
    const key = listingEventFamilyKey(event);
    const current = families.get(key);
    const reasons = new Set<PopularListingReason>(event.popularity_reason_codes || []);
    if (!current) {
      families.set(key, { key, events: [event], representative: event, rank, reasons });
      return;
    }
    current.events.push(event);
    reasons.forEach((reason) => current.reasons.add(reason));
  });
  return [...families.values()];
}

function representativeWithFamilyDates(family: PopularFamily): PreviewEvent {
  const linkedIds = new Set<number>(family.representative.other_date_ids || []);
  family.events.forEach((event) => {
    if (event.id !== family.representative.id) linkedIds.add(event.id);
    (event.other_date_ids || []).forEach((id) => {
      if (id !== family.representative.id) linkedIds.add(id);
    });
  });
  return { ...family.representative, other_date_ids: [...linkedIds].sort((left, right) => left - right) };
}

/** Desktop-only family allocation. Mobile retains buildPopularListingGroups. */
export function buildPopularDesktopListing(items: PreviewEvent[], perGroup = 5): PopularDesktopListingResult {
  const limit = Math.max(1, perGroup);
  const families = aggregatePopularFamilies(items);
  const allocated = new Set<string>();
  const stableOrder = (left: PopularFamily, right: PopularFamily) => left.rank - right.rank;
  const sharedEvidenceOrder = (left: PopularFamily, right: PopularFamily) => {
    const peak = (family: PopularFamily, key: 'shares_count' | 'likes_count' | 'source_views_count') => (
      Math.max(0, ...family.events.map((event) => Number(event[key] || 0)))
    );
    return peak(right, 'shares_count') - peak(left, 'shares_count')
      || peak(right, 'likes_count') - peak(left, 'likes_count')
      || peak(right, 'source_views_count') - peak(left, 'source_views_count')
      || stableOrder(left, right);
  };
  const definitions: Array<{
    key: PopularListingReason;
    label: string;
    matches: (family: PopularFamily) => boolean;
    order?: (left: PopularFamily, right: PopularFamily) => number;
  }> = [
    { key: 'fast_growth', label: 'Быстро набирают популярность', matches: (family) => family.reasons.has('fast_growth') },
    { key: 'multi_source', label: 'Встречается во множестве источников', matches: (family) => family.reasons.has('multi_source') },
    { key: 'discussed', label: 'Активно обсуждают', matches: (family) => family.reasons.has('discussed') },
    { key: 'frequently_shared', label: 'Часто делятся', matches: (family) => family.reasons.has('frequently_shared'), order: sharedEvidenceOrder },
    { key: 'score_fallback', label: 'Популярное сейчас', matches: () => true },
  ];
  const groups: PopularListingGroup[] = [];
  for (const definition of definitions) {
    const selected = families
      .filter((family) => !allocated.has(family.key) && definition.matches(family))
      .sort(definition.order || stableOrder)
      .slice(0, limit);
    if (!selected.length || (definition.key !== 'score_fallback' && selected.length < 3)) continue;
    selected.forEach((family) => allocated.add(family.key));
    groups.push({
      key: definition.key,
      label: definition.label,
      events: selected.map(representativeWithFamilyDates),
    });
  }
  return { groups, allocatedFamilyKeys: [...allocated] };
}

export function buildPopularDesktopPersonalizationCandidates(
  items: PreviewEvent[],
  allocatedFamilyKeys: Iterable<string>,
  limit = 30,
): PreviewEvent[] {
  const allocated = new Set(allocatedFamilyKeys);
  return aggregatePopularFamilies(items)
    .filter((family) => !allocated.has(family.key))
    .sort((left, right) => left.rank - right.rank)
    .slice(0, Math.max(0, limit))
    .map(representativeWithFamilyDates);
}

export function groupListingEvents(items: PreviewEvent[]): ListingTimeGroup[] {
  const groups = new Map<string, PreviewEvent[]>();
  for (const event of deduplicateListingEvents(items)) {
    const key = exactListingTime(event) || 'untimed';
    const group = groups.get(key) || [];
    group.push(event);
    groups.set(key, group);
  }
  return [...groups.entries()]
    .sort(([left], [right]) => {
      if (left === 'untimed') return 1;
      if (right === 'untimed') return -1;
      return left.localeCompare(right);
    })
    .map(([key, events]) => ({
      key,
      label: key === 'untimed' ? 'Время уточняется' : key,
      daypart: listingDaypart(key === 'untimed' ? null : key),
      events,
    }));
}

export function cityKey(city: string | null | undefined): string {
  return normalizedIdentityPart(city || 'Калининградская область').replace(/\s+/gu, '-');
}

function assetRatio(asset: EventImageAsset): number {
  return asset.width > 0 && asset.height > 0 ? asset.width / asset.height : 0;
}

function usableAsset(asset: EventImageAsset): boolean {
  // A 180px-wide social thumbnail cannot support the 300–400px desktop
  // listing frame without visible upscale. Source-manifest replacements are
  // applied before this gate, so reviewed video stills remain eligible.
  return Boolean(asset?.src && asset.width >= 256 && asset.height >= 180);
}

function usableLowResolutionFallback(asset: EventImageAsset): boolean {
  return Boolean(asset?.src && asset.width >= 256 && asset.height >= 160 && asset.width * asset.height >= 50_000);
}

function knownAsset(asset: EventImageAsset): boolean {
  return Boolean(
    asset?.src
    && asset.width > 0
    && asset.height > 0
    && asset.media_semantic_reason_code !== 'no_event_relevance'
  );
}

function bestWideAsset(assets: EventImageAsset[], targetRatio: number): EventImageAsset | null {
  if (!assets.length) return null;
  return [...assets].sort((left, right) => {
    const ratioDelta = Math.abs(assetRatio(left) - targetRatio) - Math.abs(assetRatio(right) - targetRatio);
    if (Math.abs(ratioDelta) > 0.01) return ratioDelta;
    return right.width * right.height - left.width * left.height;
  })[0] || null;
}

interface ListingMediaOverride {
  sourceSrc: string;
  sourcePage?: string;
  replacementSrc?: string;
  width?: number;
  height?: number;
  imageTextMode?: 'ocr_text' | 'visual_only' | 'unknown';
  cropEvidence?: string;
  noOcrReviewed?: boolean;
  objectPosition?: string;
  alt?: string;
  useNatural?: boolean;
  thumbnailSources?: Array<{ src: string; width: number; height: number }>;
}

const listingOverrideItems = (listingMediaOverrides as { items?: ListingMediaOverride[] }).items || [];

function withListingMediaOverride(event: PreviewEvent, asset: EventImageAsset | null): EventImageAsset | null {
  if (!asset) return null;
  const sourceUrls = new Set([event.source_url, ...(event.source_urls || [])].filter(Boolean));
  const item = listingOverrideItems.find((candidate) => (
    candidate.sourceSrc === asset.src
    || Boolean(candidate.sourcePage && sourceUrls.has(candidate.sourcePage))
  ));
  if (!item) return asset;
  return {
    ...asset,
    alt: item.alt || asset.alt,
    src: item.replacementSrc || asset.src,
    width: item.width || asset.width,
    height: item.height || asset.height,
    image_text_mode: item.imageTextMode || asset.image_text_mode,
    thumbnail_sources: item.thumbnailSources || asset.thumbnail_sources,
    media_semantic_status: item.cropEvidence ? 'classified' : asset.media_semantic_status,
    media_role: item.cropEvidence ? 'event_photo' : asset.media_role,
    media_role_confidence: item.cropEvidence ? 1 : asset.media_role_confidence,
    image_kind: item.cropEvidence ? 'photo' : asset.image_kind,
    safe_crop: item.cropEvidence ? true : asset.safe_crop,
    focal_point: item.cropEvidence ? { x: 0.5, y: 0.5 } : asset.focal_point,
    recommended_object_position: item.objectPosition || asset.recommended_object_position,
    listing_crop_evidence: item.cropEvidence ? 'source-reviewed' : asset.listing_crop_evidence,
    listing_no_ocr_review: item.noOcrReviewed === true,
    listing_use_natural: item.useNatural === true,
  };
}

export function selectListingImage(event: PreviewEvent, visualCropRatio = 1.5): ListingImagePresentation {
  // Preserve the measured geometry even when an asset misses the quality
  // threshold. A small known fallback must never be stretched into an
  // invented portrait frame (event 3794 is the regression contract).
  const knownAssets = (event.image_assets || [])
    .filter(knownAsset)
    .map((asset) => withListingMediaOverride(event, asset))
    .filter((asset): asset is EventImageAsset => Boolean(asset));
  const assets = knownAssets.filter(usableAsset);
  const posters = assets.filter((asset) => (
    asset.media_semantic_status === 'classified'
    && asset.media_role === 'event_identity_poster'
    && Number(asset.media_role_confidence || 0) >= 0.9
  ));
  const photos = assets.filter((asset) => (
    asset.media_semantic_status === 'classified'
    && asset.media_role === 'event_photo'
    && Number(asset.media_role_confidence || 0) >= 0.9
    && asset.safe_crop === true
    && Boolean(asset.focal_point)
  ));
  const isSourceReviewedVisual = (asset: EventImageAsset) => (
    asset.listing_crop_evidence === 'source-reviewed'
    && asset.listing_no_ocr_review === true
    && asset.image_text_mode === 'visual_only'
    && asset.media_semantic_status === 'classified'
    && asset.media_role === 'event_photo'
    && Number(asset.media_role_confidence || 0) >= 0.9
    && asset.safe_crop === true
    && Boolean(asset.focal_point)
  );
  const visualOnly = assets.filter((asset) => (
    asset.image_text_mode === 'visual_only'
    // An event may have an OCR primary poster and a separately reviewed
    // no-text photo. Only explicit per-asset source review may override the
    // event-level fail-closed marker.
    && (event.image_text_mode !== 'ocr_text' || isSourceReviewedVisual(asset))
    && asset.media_role !== 'event_identity_poster'
    && asset.media_role !== 'program_or_schedule'
    && asset.media_role !== 'attendee_information'
  ));

  // A classified wide poster keeps more of its own title readable. It wins over
  // a portrait/square poster but never over a different semantic media family.
  const selectedPoster = bestWideAsset(posters, 1.25);
  const preferredPhotos = photos.filter((asset) => assetRatio(asset) >= 1);
  const selectedPhoto = bestWideAsset(preferredPhotos.length ? preferredPhotos : photos, preferredPhotos.length ? 1.5 : 0.8);
  const preferredVisuals = visualOnly.filter((asset) => assetRatio(asset) >= 1);
  const selectedVisual = bestWideAsset(preferredVisuals.length ? preferredVisuals : visualOnly, visualCropRatio);
  // Unknown is not crop permission, but it also is not a reason to ignore a
  // wider approved candidate from the same event inventory. Prefer its
  // authored natural geometry without asserting visual_only or safe_crop.
  const unknownNaturalAssets = assets.filter((asset) => (
    asset.image_text_mode === 'unknown'
    && !['event_identity_poster', 'program_or_schedule', 'attendee_information'].includes(asset.media_role || '')
  ));
  const preferredUnknownNatural = unknownNaturalAssets.filter((asset) => assetRatio(asset) >= 1);
  const selectedUnknownNatural = bestWideAsset(
    preferredUnknownNatural.length ? preferredUnknownNatural : unknownNaturalAssets,
    preferredUnknownNatural.length ? 1.35 : 0.8,
  );
  // Assets smaller than the minimum listing frame are not silently upscaled.
  // If no usable event-relevant asset exists, the card renders the shared
  // neutral fallback; source recovery remains an ingestion concern.
  const primary = assets[0] || null;
  const lowResolutionFallback = knownAssets.find(usableLowResolutionFallback) || null;
  const sourceAsset = selectedPoster || selectedPhoto || selectedVisual || selectedUnknownNatural || primary || lowResolutionFallback;
  const asset = sourceAsset;
  const src = asset?.src || null;
  const rawRatio = asset ? assetRatio(asset) : visualCropRatio;
  const isPhoto = Boolean(
    asset
    && asset.media_semantic_status === 'classified'
    && asset.media_role === 'event_photo'
    && asset.image_kind === 'photo'
  );
  const isPoster = Boolean(asset && asset.media_semantic_status === 'classified' && asset.media_role === 'event_identity_poster');
  const sourceReviewedVisual = Boolean(asset && isSourceReviewedVisual(asset));
  const textProtected = Boolean(
    asset
    && (
      asset.image_text_mode !== 'visual_only'
      || (event.image_text_mode === 'ocr_text' && !sourceReviewedVisual)
      || ['event_identity_poster', 'program_or_schedule', 'attendee_information'].includes(asset.media_role || '')
    )
  );
  const isVisualOnly = Boolean(asset && !textProtected && asset.image_text_mode === 'visual_only');
  const hasCropEvidence = Boolean(
    isVisualOnly
    && isPhoto
    && asset?.safe_crop === true
    && asset.focal_point
    && (asset.listing_crop_evidence === 'source-reviewed' || Number(asset.media_role_confidence || 0) >= 0.9)
  );
  const cropRetention = rawRatio > 0
    ? Math.min(rawRatio / visualCropRatio, visualCropRatio / rawRatio)
    : 0;
  const visualCanCrop = Boolean(
    hasCropEvidence
    && (asset?.listing_crop_evidence === 'source-reviewed' || cropRetention >= (rawRatio < 1 ? 0.7 : 0.8))
  );
  const useNatural = Boolean(asset?.listing_use_natural);
  const visualRatio = isVisualOnly && asset
    ? (useNatural
      ? rawRatio
      : visualCanCrop
      ? visualCropRatio
      : rawRatio)
    : visualCropRatio;
  const outputRatio = isVisualOnly
    ? visualRatio
    : Math.max(0.2, Math.min(3.2, rawRatio || 0.8));
  return {
    asset,
    src,
    ratio: outputRatio,
    mode: isVisualOnly
      ? (useNatural ? 'visual-natural' : visualCanCrop ? 'visual-crop' : 'visual-natural')
      : isPhoto
        ? 'photo-natural'
        : isPoster
          ? 'poster-natural'
          : 'unknown-natural',
    adaptiveCrop: visualCanCrop && !useNatural,
    objectPosition: asset?.recommended_object_position || event.image_object_position || '50% 50%',
    verticalRetention: rawRatio > 0 && outputRatio > rawRatio ? Math.min(1, rawRatio / outputRatio) : 1,
  };
}

export function groupEventCount(groups: ListingTimeGroup[]): number {
  return groups.reduce((sum, group) => sum + group.events.length, 0);
}
