import type { EventImageAsset, PreviewEvent } from './types';
import { eligibleDateHeroAsset } from './dateListingHero.ts';

export type HomeHeroTalkMode = 'text-only' | 'photo-mosaic';

export interface HomeHeroTalkScene {
  event: PreviewEvent;
  mode: HomeHeroTalkMode;
  asset: EventImageAsset | null;
  eyebrow: string;
  phrase: string;
  detail: string;
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

function formatDate(event: PreviewEvent, currentDate: string): string {
  const date = event.start_date < currentDate && (event.end_date || event.start_date) >= currentDate
    ? currentDate
    : event.start_date;
  const label = new Intl.DateTimeFormat('ru-RU', {
    day: 'numeric',
    month: 'long',
    timeZone: 'Europe/Kaliningrad',
  }).format(new Date(`${date}T12:00:00+02:00`));
  return [label, event.start_time || event.display_time].filter(Boolean).join(' · ');
}

function sceneCopy(event: PreviewEvent, currentDate: string, index: number): Pick<HomeHeroTalkScene, 'eyebrow' | 'phrase' | 'detail'> {
  const today = event.start_date <= currentDate && (event.end_date || event.start_date) >= currentDate;
  const eyebrows = today
    ? ['Сегодня в городе', 'Можно успеть сегодня']
    : ['Стоит запланировать', 'Впереди в афише', 'Выберите настроение'];
  const place = event.venue_name || event.city || 'Калининградская область';
  return {
    eyebrow: eyebrows[index % eyebrows.length],
    phrase: event.title,
    detail: `${formatDate(event, currentDate)} · ${place}`,
  };
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
): HomeHeroTalkScene[] {
  const eventsById = new Map(events.map((event) => [event.id, event]));
  const deduplicated = new Map<number, PreviewEvent>();
  for (const event of events) {
    if (!isCurrent(event, currentDate)) continue;
    const familyKey = mutualFamilyKey(event, eventsById);
    const previous = deduplicated.get(familyKey);
    if (!previous || eventRank(event, seed) > eventRank(previous, seed)) deduplicated.set(familyKey, event);
  }
  const candidates = [...deduplicated.values()].sort((left, right) => (
    eventRank(right, seed) - eventRank(left, seed)
    || left.start_date.localeCompare(right.start_date)
    || left.id - right.id
  ));
  const desired = MODE_PATTERNS[hash32(seed) % MODE_PATTERNS.length];
  const remaining = [...candidates];
  const scenes: HomeHeroTalkScene[] = [];

  for (let index = 0; index < Math.min(limit, candidates.length); index += 1) {
    let mode = desired[index % desired.length];
    let candidateIndex = mode === 'photo-mosaic'
      ? remaining.findIndex((event) => Boolean(eligibleDateHeroAsset(event)))
      : 0;
    if (candidateIndex < 0) {
      mode = 'text-only';
      candidateIndex = 0;
    }
    if (candidateIndex < 0 || !remaining[candidateIndex]) break;
    const event = remaining.splice(candidateIndex, 1)[0];
    const asset = mode === 'photo-mosaic' ? eligibleDateHeroAsset(event) : null;
    scenes.push({ event, mode: asset ? mode : 'text-only', asset, ...sceneCopy(event, currentDate, index) });
  }

  return scenes;
}
