import type { EventImageAsset, PreviewEvent } from './types';

export interface DateListingHeroSelection {
  event: PreviewEvent;
  asset: EventImageAsset;
  familyKey: number;
}

function isActive(event: PreviewEvent): boolean {
  return event.lifecycle_status === 'active';
}

function isExactDate(event: PreviewEvent, date: string): boolean {
  return event.start_date <= date && (event.end_date || event.start_date) >= date;
}

function hasFreshGeometry(asset: EventImageAsset): boolean {
  return Boolean(
    asset.current_pixel_sha256
    && asset.geometry_pixel_sha256
    && asset.current_pixel_sha256 === asset.geometry_pixel_sha256,
  );
}

export function isDateHeroAssetEligible(asset: EventImageAsset): boolean {
  const semanticPhoto = asset.image_kind === 'photo'
    || asset.media_role === 'event_photo'
    || asset.media_role === 'unknown_visual';
  return asset.image_text_mode === 'visual_only'
    && semanticPhoto
    && asset.safe_crop === true
    && asset.recommended_hero_fit === 'cover'
    && Number(asset.width) >= 1000
    && Number(asset.width) * Number(asset.height) >= 1_000_000
    && Boolean(asset.focal_point)
    && hasFreshGeometry(asset);
}

export function eligibleDateHeroAsset(event: PreviewEvent): EventImageAsset | null {
  return (event.image_assets || [])
    .filter(isDateHeroAssetEligible)
    .sort((left, right) => (
      Number(right.quality_score || 0) - Number(left.quality_score || 0)
      || right.width * right.height - left.width * left.height
      || left.src.localeCompare(right.src)
    ))[0] || null;
}

function mutualFamilyKey(event: PreviewEvent, eventsById: Map<number, PreviewEvent>): number {
  const mutual = event.other_date_ids
    .filter((id) => eventsById.get(id)?.other_date_ids.includes(event.id));
  return Math.min(event.id, ...mutual);
}

function rank(event: PreviewEvent, asset: EventImageAsset, date: string): number {
  const exactOccurrence = event.start_date === date ? 1_000_000 : 0;
  const signal = Number(event.popularity_signal_score || 0) * 10_000;
  const engagement = Math.min(100_000, Number(event.source_views_count || 0))
    + Math.min(20_000, Number(event.source_likes_count || event.likes_count || 0) * 100)
    + Math.min(20_000, Number(event.shares_count || 0) * 250);
  const quality = Number(asset.quality_score || 0) * 100;
  return exactOccurrence + signal + engagement + quality;
}

export function selectDateListingHero(events: PreviewEvent[], date: string): DateListingHeroSelection | null {
  const eventsById = new Map(events.map((event) => [event.id, event]));
  const byFamily = new Map<number, DateListingHeroSelection>();
  for (const event of events) {
    if (!isActive(event) || !isExactDate(event, date)) continue;
    const asset = eligibleDateHeroAsset(event);
    if (!asset) continue;
    const familyKey = mutualFamilyKey(event, eventsById);
    const candidate = { event, asset, familyKey };
    const previous = byFamily.get(familyKey);
    if (
      !previous
      || rank(event, asset, date) > rank(previous.event, previous.asset, date)
      || (rank(event, asset, date) === rank(previous.event, previous.asset, date) && event.id < previous.event.id)
    ) {
      byFamily.set(familyKey, candidate);
    }
  }
  return [...byFamily.values()].sort((left, right) => (
    rank(right.event, right.asset, date) - rank(left.event, left.asset, date)
    || left.event.id - right.event.id
  ))[0] || null;
}

function hash32(value: string): number {
  let hash = 2166136261;
  for (let index = 0; index < value.length; index += 1) {
    hash ^= value.charCodeAt(index);
    hash = Math.imul(hash, 16777619);
  }
  return hash >>> 0;
}

function seededUnit(seed: number): number {
  let value = seed >>> 0;
  value ^= value << 13;
  value ^= value >>> 17;
  value ^= value << 5;
  return (value >>> 0) / 0xffffffff;
}

export interface DateHeroTile {
  index: number;
  col: number;
  row: number;
  baseAlpha: number;
  entryDelay: number;
  entryDuration: number;
  exitStart: number;
  exitEnd: number;
}

export function createDateHeroTileSchedule(seed: string): DateHeroTile[] {
  const tiles = Array.from({ length: 66 }, (_, index) => {
    const unit = seededUnit(hash32(`${seed}:${index}`));
    const col = index % 11;
    const edgeAlpha = col < 2 ? 0.03 + col * 0.08 : 0.55 + unit * 0.38;
    return {
      index,
      col,
      row: Math.floor(index / 11),
      unit,
      baseAlpha: Number(Math.min(0.94, edgeAlpha).toFixed(4)),
      entryDelay: Math.round(35 + unit * 720),
      entryDuration: Math.round(260 + seededUnit(hash32(`${seed}:duration:${index}`)) * 340),
    };
  });
  const order = [...tiles].sort((left, right) => left.unit - right.unit || left.index - right.index);
  const rankByIndex = new Map(order.map((tile, rank) => [tile.index, rank]));
  return tiles.map(({ unit: _unit, ...tile }) => {
    const rank = rankByIndex.get(tile.index) || 0;
    const exitStart = 0.03 + rank / Math.max(1, tiles.length - 1) * 0.72;
    const exitDuration = 0.16 + seededUnit(hash32(`${seed}:exit:${tile.index}`)) * 0.13;
    return {
      ...tile,
      exitStart: Number(exitStart.toFixed(4)),
      exitEnd: Number(Math.min(0.98, exitStart + exitDuration).toFixed(4)),
    };
  });
}
