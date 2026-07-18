import type { EventImageAsset, PreviewEvent } from './types';
import listingMediaOverrides from '../data/listingMediaOverrides.json';

export type ListingDaypart = 'morning' | 'day' | 'evening' | 'night' | 'untimed';

export interface ListingTimeGroup {
  key: string;
  label: string;
  daypart: ListingDaypart;
  events: PreviewEvent[];
}

export interface ListingImagePresentation {
  asset: EventImageAsset | null;
  src: string | null;
  ratio: number;
  mode: 'poster-natural' | 'photo-natural' | 'photo-crop' | 'visual-natural' | 'visual-crop' | 'unknown-natural';
  adaptiveCrop: boolean;
  objectPosition: string;
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
    && assetRatio(asset) >= 1.85
    && assetRatio(asset) <= 2.1
    && asset.width >= 520
    && asset.height >= 250
  ));
  const photos = assets.filter((asset) => (
    asset.media_semantic_status === 'classified'
    && asset.media_role === 'event_photo'
    && Number(asset.media_role_confidence || 0) >= 0.9
    && asset.safe_crop === true
    && Boolean(asset.focal_point)
  ));
  const visualOnly = assets.filter((asset) => (
    (asset.image_text_mode || event.image_text_mode) === 'visual_only'
  ));

  // A classified wide poster keeps more of its own title readable. It wins over
  // a portrait/square poster but never over a different semantic media family.
  const selectedPoster = bestWideAsset(posters, 2);
  const preferredPhotos = photos.filter((asset) => assetRatio(asset) >= 1);
  const selectedPhoto = bestWideAsset(preferredPhotos.length ? preferredPhotos : photos, preferredPhotos.length ? 1.5 : 0.8);
  const preferredVisuals = visualOnly.filter((asset) => assetRatio(asset) >= 1);
  const selectedVisual = bestWideAsset(preferredVisuals.length ? preferredVisuals : visualOnly, visualCropRatio);
  // Assets smaller than the minimum listing frame are not silently upscaled.
  // If no usable event-relevant asset exists, the card renders the shared
  // neutral fallback; source recovery remains an ingestion concern.
  const primary = assets[0] || null;
  const sourceAsset = selectedPoster || selectedPhoto || selectedVisual || primary;
  const asset = sourceAsset;
  const src = asset?.src || null;
  const rawRatio = asset ? assetRatio(asset) : visualCropRatio;
  const isPhoto = Boolean(
    asset
    && asset.media_semantic_status === 'classified'
    && asset.media_role === 'event_photo'
    && asset.image_kind === 'photo'
  );
  const isVisualOnly = Boolean(asset && (asset.image_text_mode || event.image_text_mode) === 'visual_only');
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
  // A sufficiently large no-OCR portrait may use a conservative square floor
  // even while its finer semantic/focal review is pending. This is deliberately
  // weaker than the 3:2 source-reviewed path: it removes obsolete narrow-photo
  // columns without pretending that an unreviewed centre crop is safe at 3:2.
  const conservativeSquareCrop = Boolean(
    isVisualOnly
    && asset
    && asset.width >= 512
    && asset.height >= 512
    && rawRatio >= 0.5
    && rawRatio < 1
  );
  const visualCanCrop = Boolean(
    conservativeSquareCrop
    || (
      hasCropEvidence
      && (asset?.listing_crop_evidence === 'source-reviewed' || cropRetention >= (rawRatio < 1 ? 0.7 : 0.8))
    )
  );
  const useNatural = Boolean(asset?.listing_use_natural);
  const visualRatio = isVisualOnly && asset
    ? (useNatural
      ? rawRatio
      : visualCanCrop
      ? (conservativeSquareCrop ? 1 : visualCropRatio)
      : rawRatio)
    : visualCropRatio;
  const isPoster = Boolean(asset && asset.media_semantic_status === 'classified' && asset.media_role === 'event_identity_poster');
  return {
    asset,
    src,
    ratio: isVisualOnly
      ? visualRatio
      : isPhoto
      ? Math.max(0.2, Math.min(3.2, rawRatio || 0.8))
      : Math.max(0.2, Math.min(3.2, rawRatio || 0.8)),
    mode: isVisualOnly
      ? (useNatural ? 'visual-natural' : visualCanCrop ? 'visual-crop' : 'visual-natural')
      : isPhoto
        ? 'photo-natural'
        : isPoster
          ? 'poster-natural'
          : 'unknown-natural',
    adaptiveCrop: visualCanCrop && !useNatural,
    objectPosition: asset?.recommended_object_position || event.image_object_position || '50% 50%',
  };
}

export function groupEventCount(groups: ListingTimeGroup[]): number {
  return groups.reduce((sum, group) => sum + group.events.length, 0);
}
