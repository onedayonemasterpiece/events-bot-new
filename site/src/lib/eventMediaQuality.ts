import type { EventImageAsset } from './types';

export interface EventMediaQualitySelection {
  admittedSourceIndexes: number[];
  hiddenSourceIndexes: number[];
}

const MIN_LONG_EDGE = 720;
const MIN_PIXEL_AREA = 450_000;
const MIN_QUALITY_SCORE = 10;

function isPositivelyClassifiedDocument(asset: EventImageAsset): boolean {
  return asset.media_semantic_status === 'classified'
    && Boolean(asset.media_role)
    && asset.media_role !== 'event_photo'
    && asset.media_role !== 'unknown_visual';
}

/** Pending role labels can temporarily describe real photos as unknown documents. */
function isPhotoLike(asset: EventImageAsset): boolean {
  return asset.image_text_mode === 'visual_only' && !isPositivelyClassifiedDocument(asset);
}

export function isTechnicallyStrongEventMedia(asset: EventImageAsset | undefined): boolean {
  if (!asset || !isPhotoLike(asset)) return false;
  const width = Number(asset.width || 0);
  const height = Number(asset.height || 0);
  return Math.max(width, height) >= MIN_LONG_EDGE
    && width * height >= MIN_PIXEL_AREA
    && Number(asset.quality_score || 0) >= MIN_QUALITY_SCORE;
}

export function isLowResolutionPortraitEventMedia(asset: EventImageAsset | undefined): boolean {
  if (!asset || !isPhotoLike(asset)) return false;
  const width = Number(asset.width || 0);
  const height = Number(asset.height || 0);
  return width > 0 && height > width && !isTechnicallyStrongEventMedia(asset);
}

/**
 * Weak photo renditions disappear only when the same event has a technically
 * strong photo-like alternative. If every photo is weak, preserve the originals.
 * Positively classified posters, maps and attendee documents are never removed.
 */
export function selectEventMediaByQuality(assets: EventImageAsset[]): EventMediaQualitySelection {
  const hasStrongPhoto = assets.some((asset) => isTechnicallyStrongEventMedia(asset));
  if (!hasStrongPhoto) {
    return {
      admittedSourceIndexes:assets.map((_asset, index) => index),
      hiddenSourceIndexes:[],
    };
  }

  const hiddenSourceIndexes = assets
    .map((asset, index) => isPhotoLike(asset) && !isTechnicallyStrongEventMedia(asset) ? index : -1)
    .filter((index) => index >= 0);
  const hidden = new Set(hiddenSourceIndexes);
  return {
    admittedSourceIndexes:assets.map((_asset, index) => index).filter((index) => !hidden.has(index)),
    hiddenSourceIndexes,
  };
}
