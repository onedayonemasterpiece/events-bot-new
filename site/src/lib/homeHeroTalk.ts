import type { EventImageAsset, PreviewEvent } from './types';

const HOME_HERO_GEOMETRY_MODEL = 'gemma-4-31b-it';
const HOME_HERO_GEOMETRY_PROMPT_VERSION = 'event-image-geometry-v1';
const HOME_HERO_TARGET_RATIOS = [3.2, 3.6, 4] as const;
const clamp01 = (value: number) => Math.max(0, Math.min(1, Number.isFinite(value) ? value : .5));

function validNormalizedBox(box: EventImageAsset['valuable_region']): boolean {
  if (!box) return false;
  const { x, y, w, h } = box;
  return [x, y, w, h].every(Number.isFinite)
    && x >= 0 && y >= 0 && w > 0 && h > 0
    && x + w <= 1.000001 && y + h <= 1.000001;
}

function hasCurrentHomeHeroGeometry(asset: EventImageAsset): boolean {
  return asset.geometry_status === 'classified'
    && asset.geometry_coordinate_space === 'normalized_0_1'
    && asset.geometry_model === HOME_HERO_GEOMETRY_MODEL
    && asset.geometry_prompt_version === HOME_HERO_GEOMETRY_PROMPT_VERSION
    && Boolean(asset.current_pixel_sha256)
    && asset.current_pixel_sha256 === asset.geometry_pixel_sha256;
}

function clampFocusToFaceSafeRatios(asset: EventImageAsset, desired: { x:number; y:number }): { x:number; y:number } {
  const faces = asset.face_boxes || [];
  if (!faces.length || !(asset.width > 0) || !(asset.height > 0)) return desired;
  let minX = 0, maxX = 1, minY = 0, maxY = 1;
  const sourceRatio = asset.width / asset.height;
  for (const targetRatio of HOME_HERO_TARGET_RATIOS) {
    const visible = sourceRatio < targetRatio ? sourceRatio / targetRatio : targetRatio / sourceRatio;
    const travel = 1 - visible;
    if (travel <= 0) continue;
    for (const box of faces) {
      if (!validNormalizedBox(box)) continue;
      if (sourceRatio < targetRatio) {
        const padding = Math.max(.012, Math.min(.04, box.h * .12));
        const start = Math.max(0, box.y - padding);
        const end = Math.min(1, box.y + box.h + padding);
        minY = Math.max(minY, (end - visible) / travel);
        maxY = Math.min(maxY, start / travel);
      } else {
        const padding = Math.max(.012, Math.min(.04, box.w * .12));
        const start = Math.max(0, box.x - padding);
        const end = Math.min(1, box.x + box.w + padding);
        minX = Math.max(minX, (end - visible) / travel);
        maxX = Math.min(maxX, start / travel);
      }
    }
  }
  return {
    x: minX <= maxX ? Math.max(minX, Math.min(maxX, desired.x)) : desired.x,
    y: minY <= maxY ? Math.max(minY, Math.min(maxY, desired.y)) : desired.y,
  };
}

/** Geometry guides framing; semantic focal remains the bounded fallback. */
export function homeHeroCropFocus(asset: EventImageAsset | null | undefined): { x:number; y:number } {
  const fallback = {
    x: clamp01(Number(asset?.focal_point?.x ?? .5)),
    y: clamp01(Number(asset?.focal_point?.y ?? .5)),
  };
  if (!asset || !hasCurrentHomeHeroGeometry(asset) || !validNormalizedBox(asset.valuable_region)) return fallback;
  const region = asset.valuable_region!;
  return clampFocusToFaceSafeRatios(asset, {
    x: clamp01(region.x + region.w / 2),
    y: clamp01(region.y + region.h / 2),
  });
}

function geometryInformationScore(asset: EventImageAsset): number {
  if (!hasCurrentHomeHeroGeometry(asset) || !validNormalizedBox(asset.valuable_region)) return 0;
  const region = asset.valuable_region!;
  return Math.max(0, 1 - region.w * region.h);
}

// Restored bounded donor dependency, 4243401a4017a9efc9b43fb3482307675a60f0ec.
// The removed DateListingHero module is not reintroduced with its unused builder.
export function isHomeHeroAssetEligible(asset: EventImageAsset): boolean {
  const semanticPhoto = asset.image_kind === 'photo'
    || asset.media_role === 'event_photo' || asset.media_role === 'unknown_visual';
  return asset.image_text_mode === 'visual_only' && semanticPhoto
    && asset.safe_crop === true && asset.recommended_hero_fit === 'cover'
    && Number(asset.width) >= 1000 && Number(asset.width) * Number(asset.height) >= 1_000_000
    && Boolean(asset.focal_point) && hasCurrentHomeHeroGeometry(asset);
}
import {
  HOME_HERO_TALK_EDITORIAL,
  type HomeHeroTalkEditorial,
  type HomeHeroTalkEditorialFragment,
} from '../data/homeHeroTalkEditorial.ts';

export type HomeHeroTalkMode = 'text-only' | 'photo-mosaic';

export interface HomeHeroTalkScene {
  event: PreviewEvent;
  mode: HomeHeroTalkMode;
  asset: EventImageAsset | null;
  editorialId: string;
  fragments: HomeHeroTalkEditorialFragment[];
  copySource: 'editorial' | 'catalog-fact-fallback';
}

function hash32(value: string): number {
  let hash = 2166136261;
  for (let index = 0; index < value.length; index += 1) {
    hash ^= value.charCodeAt(index);
    hash = Math.imul(hash, 16777619);
  }
  return hash >>> 0;
}

function mutualFamilyKey(event: PreviewEvent, eventsById: Map<number, PreviewEvent>): number {
  return Math.min(
    event.id,
    ...(event.other_date_ids || []).filter((id) => eventsById.get(id)?.other_date_ids?.includes(event.id)),
  );
}

function isCurrent(event: PreviewEvent, currentDate: string): boolean {
  return (!event.lifecycle_status || event.lifecycle_status === 'active')
    && (event.end_date || event.start_date) >= currentDate;
}

function eventRank(event: PreviewEvent, seed: string): number {
  const deterministicJitter = hash32(`${seed}:${event.id}`) / 0xffffffff;
  const popularity = Number(event.popularity_signal_score || 0) * 100;
  const engagement = Math.log1p(
    Number(event.source_views_count || 0)
    + Number(event.source_likes_count || event.likes_count || 0) * 80
    + Number(event.shares_count || 0) * 150,
  );
  return popularity + engagement + deterministicJitter * 4;
}

function faceCropSafeAtRatio(asset: EventImageAsset, targetRatio: number): boolean {
  const sourceRatio = asset.width / asset.height;
  const { x: focusX, y: focusY } = homeHeroCropFocus(asset);
  if (sourceRatio < targetRatio) {
    const visible = sourceRatio / targetRatio;
    const top = (1 - visible) * focusY;
    return (asset.face_boxes || []).every((box) => {
      const padding = Math.max(.012, Math.min(.04, box.h * .12));
      return Math.max(0, box.y - padding) >= top
        && Math.min(1, box.y + box.h + padding) <= top + visible;
    });
  }
  const visible = targetRatio / sourceRatio;
  const left = (1 - visible) * focusX;
  return (asset.face_boxes || []).every((box) => {
    const padding = Math.max(.012, Math.min(.04, box.w * .12));
    return Math.max(0, box.x - padding) >= left
      && Math.min(1, box.x + box.w + padding) <= left + visible;
  });
}

function eligibleHomeHeroAsset(event: PreviewEvent): EventImageAsset | null {
  return (event.image_assets || [])
    .filter(isHomeHeroAssetEligible)
    // 75vw at a 1920px acceptance viewport must not exceed donor's 1.10 cap.
    .filter((asset) => asset.width >= 1_310)
    .filter((asset) => [3.2, 3.6, 4].every((ratio) => faceCropSafeAtRatio(asset, ratio)))
    .sort((left, right) => (
      Number(right.quality_score || 0) - Number(left.quality_score || 0)
      || geometryInformationScore(right) - geometryInformationScore(left)
      || Number(left.face_boxes?.length || 0) - Number(right.face_boxes?.length || 0)
      || right.width * right.height - left.width * left.height
      || left.src.localeCompare(right.src)
    ))[0] || null;
}

const MODE_PATTERNS: HomeHeroTalkMode[][] = [
  ['photo-mosaic', 'text-only', 'photo-mosaic', 'text-only'],
  ['photo-mosaic', 'text-only', 'text-only', 'photo-mosaic'],
  ['text-only', 'photo-mosaic', 'text-only', 'photo-mosaic'],
  ['text-only', 'text-only', 'photo-mosaic', 'text-only'],
];

export function buildHomeHeroTalkDeck(
  events: PreviewEvent[],
  currentDate: string,
  seed: string,
  limit = 4,
  editorials: HomeHeroTalkEditorial[] = HOME_HERO_TALK_EDITORIAL,
): HomeHeroTalkScene[] {
  const eventsById = new Map(events.map((event) => [event.id, event]));
  const currentByFamily = new Map<number, PreviewEvent>();
  for (const event of events) {
    if (!isCurrent(event, currentDate)) continue;
    const familyKey = mutualFamilyKey(event, eventsById);
    const previous = currentByFamily.get(familyKey);
    if (!previous || eventRank(event, seed) > eventRank(previous, seed)) currentByFamily.set(familyKey, event);
  }
  const candidateById = new Map([...currentByFamily.values()].map((event) => [event.id, event]));
  // Keep source-bound editorial fragments when their exact occurrence is current.
  // A stale seasonal bank must not erase the hero or revive expired events.
  // The fallback quotes the current catalogue title verbatim, not generated copy.
  const currentEditorialIds = new Set(editorials.filter(e => candidateById.has(e.eventId)).map(e=>e.eventId));
  const factFallbacks: HomeHeroTalkEditorial[] = [...candidateById.values()]
    .filter(event => !currentEditorialIds.has(event.id) && event.title?.trim() && event.title.length <= 110)
    .map(event => ({id:`catalog-event-${event.id}`,eventId:event.id,
      fragments:[{text:event.title,link:true,accent:true}]}));
  const candidates = [...editorials, ...factFallbacks]
    .flatMap((editorial) => {
      const exact = eventsById.get(editorial.eventId);
      if (!exact || !isCurrent(exact, currentDate)) return [];
      const familyKey = mutualFamilyKey(exact, eventsById);
      const event = currentByFamily.get(familyKey);
      if (!event || exact.id !== event.id || !candidateById.has(event.id)) return [];
      return [{ editorial, event }];
    })
    .sort((left, right) => (
      eventRank(right.event, `${seed}:${right.editorial.id}`) - eventRank(left.event, `${seed}:${left.editorial.id}`)
      || hash32(`${seed}:${left.editorial.id}`) - hash32(`${seed}:${right.editorial.id}`)
    ));
  limit = Number.isFinite(limit) ? Math.max(0, Math.min(4, Math.floor(limit))) : 4;
  const desired = MODE_PATTERNS[hash32(seed) % MODE_PATTERNS.length];
  const remaining = [...candidates];
  const scenes: HomeHeroTalkScene[] = [];

  for (let index = 0; index < Math.min(limit, candidates.length); index += 1) {
    let mode = desired[index % desired.length];
    const laterNeedsPhoto = desired.slice(index + 1, Math.min(limit, desired.length))
      .includes('photo-mosaic');
    let candidateIndex = mode === 'photo-mosaic'
      ? remaining.findIndex(({ event }) => Boolean(eligibleHomeHeroAsset(event)))
      : laterNeedsPhoto
        ? remaining.findIndex(({ event }) => !eligibleHomeHeroAsset(event))
        : 0;
    if (candidateIndex < 0 && mode === 'text-only') candidateIndex = 0;
    if (candidateIndex < 0) {
      mode = 'text-only';
      candidateIndex = 0;
    }
    if (candidateIndex < 0 || !remaining[candidateIndex]) break;
    const { event, editorial } = remaining.splice(candidateIndex, 1)[0];
    const asset = mode === 'photo-mosaic' ? eligibleHomeHeroAsset(event) : null;
    scenes.push({
      event,
      mode: asset ? mode : 'text-only',
      asset,
      editorialId: editorial.id,
      fragments: editorial.fragments,
      copySource: currentEditorialIds.has(event.id) ? 'editorial' : 'catalog-fact-fallback',
    });
  }

  return scenes;
}
