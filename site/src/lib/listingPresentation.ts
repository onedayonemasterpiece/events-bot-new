import type { EventImageAsset, PreviewEvent } from './types';

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
  mode: 'poster-natural' | 'photo-natural' | 'photo-crop' | 'visual-crop' | 'unknown-natural';
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
      label: key === 'untimed' ? 'Без времени' : key,
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
  return Boolean(asset?.src && asset.width >= 180 && asset.height >= 180);
}

function bestWideAsset(assets: EventImageAsset[], targetRatio: number): EventImageAsset | null {
  if (!assets.length) return null;
  return [...assets].sort((left, right) => {
    const ratioDelta = Math.abs(assetRatio(left) - targetRatio) - Math.abs(assetRatio(right) - targetRatio);
    if (Math.abs(ratioDelta) > 0.01) return ratioDelta;
    return right.width * right.height - left.width * left.height;
  })[0] || null;
}

export function selectListingImage(event: PreviewEvent, visualCropRatio = 1.65): ListingImagePresentation {
  const assets = (event.image_assets || []).filter(usableAsset);
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
  const primary = assets[0] || null;
  const asset = selectedPoster || selectedVisual || selectedPhoto || primary;
  const src = asset?.src || event.image_url || null;
  const rawRatio = asset ? assetRatio(asset) : 0.8;
  const isPhoto = Boolean(selectedPhoto && asset === selectedPhoto);
  const isVisualOnly = Boolean(
    asset
    && asset === selectedVisual
    && (asset.image_text_mode || event.image_text_mode) === 'visual_only'
  );
  const visualRatio = isVisualOnly && asset
    ? (asset.safe_crop === true && Boolean(asset.focal_point)
      ? visualCropRatio
      : Math.min(visualCropRatio, Math.max(1.33, rawRatio)))
    : visualCropRatio;
  const isPoster = Boolean(asset && asset.media_semantic_status === 'classified' && asset.media_role === 'event_identity_poster');
  const wideCropIsSafe = isPhoto && rawRatio > 1 && Math.min(rawRatio / 1.5, 1.5 / rawRatio) >= 0.8;
  const photoRatio = rawRatio < 0.8 ? 0.8 : wideCropIsSafe ? 1.5 : Math.min(rawRatio, 2.2);
  const photoNeedsCrop = isPhoto && Math.abs(photoRatio - rawRatio) > 0.01;
  return {
    asset,
    src,
    ratio: isVisualOnly
      ? visualRatio
      : isPhoto
      ? photoRatio
      : Math.max(0.2, Math.min(3.2, rawRatio || 0.8)),
    mode: isVisualOnly
      ? 'visual-crop'
      : isPhoto
        ? (photoNeedsCrop ? 'photo-crop' : 'photo-natural')
        : isPoster
          ? 'poster-natural'
          : 'unknown-natural',
    adaptiveCrop: Boolean(isVisualOnly && asset?.safe_crop === true && asset.focal_point),
    objectPosition: asset?.recommended_object_position || event.image_object_position || '50% 50%',
  };
}

export function groupEventCount(groups: ListingTimeGroup[]): number {
  return groups.reduce((sum, group) => sum + group.events.length, 0);
}
