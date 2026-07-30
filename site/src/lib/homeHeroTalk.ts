import type { EventImageAsset, PreviewEvent } from './types';
import { isDateHeroAssetEligible } from './dateListingHero.ts';
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
    ...event.other_date_ids.filter((id) => eventsById.get(id)?.other_date_ids.includes(event.id)),
  );
}

function isCurrent(event: PreviewEvent, currentDate: string): boolean {
  return event.lifecycle_status === 'active'
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
  const focusX = Math.max(0, Math.min(1, Number(asset.focal_point?.x ?? .5)));
  const focusY = Math.max(0, Math.min(1, Number(asset.focal_point?.y ?? .5)));
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
    .filter(isDateHeroAssetEligible)
    // 75vw at a 1920px acceptance viewport must not exceed donor's 1.10 cap.
    .filter((asset) => asset.width >= 1_310)
    .filter((asset) => [3.2, 3.6, 4].every((ratio) => faceCropSafeAtRatio(asset, ratio)))
    .sort((left, right) => (
      Number(right.quality_score || 0) - Number(left.quality_score || 0)
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
  const candidates = editorials
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
    });
  }

  return scenes;
}
