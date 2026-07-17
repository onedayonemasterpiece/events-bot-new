import type { EventImageAsset, PreviewEvent } from './types';

export type DesktopEventCandidate = 'editorial' | 'split';
export type DesktopEventMediaPolicy = 'non-ocr' | 'ocr';

export interface DesktopEventPresentation {
  candidate: DesktopEventCandidate;
  mediaPolicy: DesktopEventMediaPolicy;
  heroImageIndex: number;
  heroWidth?: number;
  heroHeight?: number;
  heroObjectPosition?: string;
  editorialRail: boolean;
  editorialMotion: 'continuous';
  editorialCropPolicy: 'exact' | 'bottom-safe';
  ocrCompanionImageIndex?: number;
  ocrCompanionLayout: 'separate' | 'arrival';
  autoRotate: boolean;
  ocrSourceIndexes: number[];
  duplicateSourceIndexes: number[];
  splitMediaFit: 'natural' | 'viewport-cover';
  splitPortraitViewer: boolean;
  splitPortraitSourceIndexes: number[];
  relatedMediaTreatment: 'hybrid';
  reason: string;
}

const EDITORIAL_MIN_WIDTH = 1280;
const EDITORIAL_MIN_HEIGHT = 720;
const EDITORIAL_MIN_RATIO = 1.25;
const NEAR_SQUARE_MAX_RATIO = 1.42;
const PORTRAIT_MAX_RATIO = 0.9;

function assetRatio(asset: EventImageAsset | undefined): number {
  if (!asset?.width || !asset?.height) return 0;
  return asset.width / asset.height;
}

function isVisual(asset: EventImageAsset | undefined): boolean {
  return asset?.image_text_mode === 'visual_only';
}

function isEditorialLandscape(asset: EventImageAsset | undefined): boolean {
  if (!asset || !isVisual(asset)) return false;
  return assetRatio(asset) >= EDITORIAL_MIN_RATIO
    && asset.width >= EDITORIAL_MIN_WIDTH
    && asset.height >= EDITORIAL_MIN_HEIGHT;
}

function isReliableIdentityPoster(asset: EventImageAsset | undefined): boolean {
  return asset?.media_semantic_status === 'classified'
    && asset.media_role === 'event_identity_poster';
}

function isReliableEditorialPhoto(asset: EventImageAsset | undefined): boolean {
  return asset?.media_semantic_status === 'classified'
    && asset.media_role === 'event_photo'
    && asset.safe_crop === true
    && asset.recommended_hero_fit === 'cover'
    && isEditorialLandscape(asset);
}

function isReliableNonIdentityDocument(asset: EventImageAsset | undefined): boolean {
  return asset?.media_semantic_status === 'classified'
    && Boolean(asset.media_role)
    && asset.media_role !== 'event_photo'
    && asset.media_role !== 'event_identity_poster';
}

function objectPosition(event: PreviewEvent, asset: EventImageAsset | undefined, bottomSafe: boolean): string {
  if (bottomSafe) return '50% 100%';
  if (asset?.recommended_object_position) return asset.recommended_object_position;
  if (event.image_object_position) return event.image_object_position;
  if (asset?.focal_point) return `${Math.round(asset.focal_point.x * 100)}% ${Math.round(asset.focal_point.y * 100)}%`;
  if (event.focal_point) return `${Math.round(event.focal_point.x * 100)}% ${Math.round(event.focal_point.y * 100)}%`;
  return '50% 50%';
}

function primaryAssetIndex(event: PreviewEvent, assets: EventImageAsset[]): number {
  if (!event.image_url) return 0;
  const exact = assets.findIndex((asset) => asset.src === event.image_url);
  return exact >= 0 ? exact : 0;
}

function distinctSourceIndexes(assets: EventImageAsset[]): number[] {
  const seen = new Set<string>();
  const out: number[] = [];
  assets.forEach((asset, index) => {
    if (!asset.src || seen.has(asset.src)) return;
    seen.add(asset.src);
    out.push(index);
  });
  return out;
}

/**
 * Route production desktop pages into the exact accepted Continuous Editorial
 * or Split component families. This is deliberately geometry + semantic-state
 * aware. It must never promote a portrait/low-resolution visual into the
 * full-width Editorial family, and it must never invent a poster companion
 * from OCR alone.
 */
export function buildDesktopEventPresentation(event: PreviewEvent): DesktopEventPresentation {
  const assets = event.image_assets || [];
  const distinctIndexes = distinctSourceIndexes(assets);
  const primaryIndex = primaryAssetIndex(event, assets);
  const primary = assets[primaryIndex];
  const primaryRatio = assetRatio(primary);
  const ocrSourceIndexes = distinctIndexes.filter((index) => !isVisual(assets[index]));
  const identityPosterIndex = distinctIndexes.find((index) => isReliableIdentityPoster(assets[index]));
  const portraitIndexes = distinctIndexes.filter((index) => isVisual(assets[index]) && assetRatio(assets[index]) < PORTRAIT_MAX_RATIO);
  const classifiedPortraitPhotoCount = portraitIndexes.filter((index) => (
    assets[index]?.media_semantic_status === 'classified'
    && assets[index]?.media_role === 'event_photo'
  )).length;
  // The accepted grouped viewer is useful once an event has a real portrait
  // family, not only when portraits are an absolute majority. Two classified
  // event photos anchor semantics; pending companions may then join by geometry.
  const splitPortraitViewer = portraitIndexes.length >= 4
    && classifiedPortraitPhotoCount >= 2
    && portraitIndexes.length >= Math.ceil(Math.max(1, distinctIndexes.length) * 0.3);

  // A landscape alternative may become hero only when a classified identity
  // poster anchors event semantics. A portrait/square visual primary alone is
  // never replaced merely because another landscape exists.
  if (identityPosterIndex !== undefined) {
    const landscapeCandidates = distinctIndexes
      .filter((index) => index !== identityPosterIndex && isEditorialLandscape(assets[index]))
      .sort((left, right) => (assets[right].width * assets[right].height) - (assets[left].width * assets[left].height));
    const heroIndex = isEditorialLandscape(primary) ? primaryIndex : landscapeCandidates[0];
    if (heroIndex !== undefined) {
      const hero = assets[heroIndex];
      const bottomSafe = assetRatio(hero) < NEAR_SQUARE_MAX_RATIO;
      return {
        candidate:'editorial',
        mediaPolicy:'ocr',
        heroImageIndex:heroIndex,
        heroWidth:hero.width,
        heroHeight:hero.height,
        heroObjectPosition:objectPosition(event, hero, bottomSafe),
        editorialRail:false,
        editorialMotion:'continuous',
        editorialCropPolicy:bottomSafe ? 'bottom-safe' : 'exact',
        ocrCompanionImageIndex:identityPosterIndex,
        ocrCompanionLayout:'arrival',
        autoRotate:false,
        ocrSourceIndexes,
        duplicateSourceIndexes:[],
        splitMediaFit:'natural',
        splitPortraitViewer:false,
        splitPortraitSourceIndexes:[],
        relatedMediaTreatment:'hybrid',
        reason:'editorial-with-classified-identity-poster',
      };
    }
  }

  // A classified non-identity document (venue announcement, attendee note,
  // schedule, map) must not monopolise the desktop hero when the same event has
  // a separately classified, crop-safe, full-resolution event photograph.
  // Keep the document in the gallery; only the desktop hero family changes.
  if (isReliableNonIdentityDocument(primary)) {
    const landscapePhotoIndex = distinctIndexes
      .filter((index) => index !== primaryIndex && isReliableEditorialPhoto(assets[index]))
      .sort((left, right) => (assets[right].width * assets[right].height) - (assets[left].width * assets[left].height))[0];
    if (landscapePhotoIndex !== undefined) {
      const hero = assets[landscapePhotoIndex];
      const bottomSafe = assetRatio(hero) < NEAR_SQUARE_MAX_RATIO;
      return {
        candidate:'editorial',
        mediaPolicy:'non-ocr',
        heroImageIndex:landscapePhotoIndex,
        heroWidth:hero.width,
        heroHeight:hero.height,
        heroObjectPosition:objectPosition(event, hero, bottomSafe),
        editorialRail:true,
        editorialMotion:'continuous',
        editorialCropPolicy:bottomSafe ? 'bottom-safe' : 'exact',
        ocrCompanionLayout:'separate',
        autoRotate:distinctIndexes.filter((index) => isReliableEditorialPhoto(assets[index])).length > 1,
        ocrSourceIndexes,
        duplicateSourceIndexes:[],
        splitMediaFit:'natural',
        splitPortraitViewer:false,
        splitPortraitSourceIndexes:[],
        relatedMediaTreatment:'hybrid',
        reason:'editorial-replaces-non-identity-document-with-classified-photo',
      };
    }
  }

  if (isEditorialLandscape(primary)) {
    const bottomSafe = primaryRatio < NEAR_SQUARE_MAX_RATIO;
    const rotationEligibleCount = distinctIndexes.filter((index) => isEditorialLandscape(assets[index])).length;
    return {
      candidate:'editorial',
      mediaPolicy:'non-ocr',
      heroImageIndex:primaryIndex,
      heroWidth:primary.width,
      heroHeight:primary.height,
      heroObjectPosition:objectPosition(event, primary, bottomSafe),
      editorialRail:distinctIndexes.length > 1,
      editorialMotion:'continuous',
      editorialCropPolicy:bottomSafe ? 'bottom-safe' : 'exact',
      ocrCompanionLayout:'separate',
      autoRotate:rotationEligibleCount > 1,
      ocrSourceIndexes,
      duplicateSourceIndexes:[],
      splitMediaFit:'natural',
      splitPortraitViewer:false,
      splitPortraitSourceIndexes:[],
      relatedMediaTreatment:'hybrid',
      reason:bottomSafe ? 'editorial-primary-near-square-bottom-safe' : 'editorial-primary-qualified-landscape',
    };
  }

  const selectedIsVisual = primary ? isVisual(primary) : event.image_text_mode === 'visual_only';
  const selectedRatio = primaryRatio || 0;
  return {
    candidate:'split',
    mediaPolicy:selectedIsVisual ? 'non-ocr' : 'ocr',
    heroImageIndex:primaryIndex,
    heroWidth:primary?.width,
    heroHeight:primary?.height,
    heroObjectPosition:objectPosition(event, primary, false),
    editorialRail:false,
    editorialMotion:'continuous',
    editorialCropPolicy:'exact',
    ocrCompanionLayout:'separate',
    autoRotate:false,
    ocrSourceIndexes,
    duplicateSourceIndexes:[],
    splitMediaFit:selectedIsVisual && selectedRatio >= 1.15 ? 'viewport-cover' : 'natural',
    splitPortraitViewer,
    // The grouped viewer must contain every rail source. Restricting it to the
    // portrait subset made a click on a landscape thumbnail open image one.
    splitPortraitSourceIndexes:splitPortraitViewer ? distinctIndexes : [],
    relatedMediaTreatment:'hybrid',
    reason:!primary && !event.image_url
      ? 'split-no-image-fallback'
      : selectedIsVisual
        ? (selectedRatio < 1.15 ? 'split-portrait-or-square-visual' : 'split-resolution-constrained-landscape')
        : 'split-document-or-unclassified-media',
  };
}
