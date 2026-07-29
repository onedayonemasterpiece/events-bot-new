import assert from 'node:assert/strict';
import test from 'node:test';
import type { EventImageAsset, PreviewEvent } from './types';
import { buildHomeHeroTalkDeck } from './homeHeroTalk.ts';

const safeAsset = (src: string): EventImageAsset => ({
  src,
  width: 1600,
  height: 1000,
  alt: '',
  image_text_mode: 'visual_only',
  media_role: 'event_photo',
  image_kind: 'photo',
  recommended_hero_fit: 'cover',
  safe_crop: true,
  focal_point: { x:.5, y:.5 },
  current_pixel_sha256: `hash-${src}`,
  geometry_pixel_sha256: `hash-${src}`,
  quality_score: 10,
});

const event = (id: number, start: string, options: Partial<PreviewEvent> = {}): PreviewEvent => ({
  id,
  title: `Событие ${id}`,
  slug: `event-${id}`,
  event_type: 'концерт',
  festival: null,
  status_label: 'Билеты',
  lifecycle_status: 'active',
  starts_at: `${start}T18:00:00+02:00`,
  start_date: start,
  start_time: '18:00',
  end_date: null,
  end_at: null,
  time_range_end: null,
  timezone: 'Europe/Kaliningrad',
  display_date: start,
  display_time: '18:00',
  city: 'Калининград',
  venue_name: 'Площадка',
  address: null,
  map_query: null,
  ticket: { kind:'ticket', label:'Билеты', href:null, status:null, is_free:false, price_label:null },
  age_restriction: null,
  age_restriction_status: 'unknown',
  age_restriction_provenance: null,
  age_restriction_decision_version: null,
  age_recommendation: null,
  age_recommendation_label: null,
  source_url: null,
  telegraph_url: null,
  image_url: null,
  image_alt: '',
  image_text_mode: 'visual_only',
  image_assets: [safeAsset(`${id}.webp`)],
  summary: '',
  meta_description: '',
  description_html: '',
  topics: [],
  likes_count: id,
  source_likes_count: id,
  service_likes_count: 0,
  pushkin_card: false,
  other_date_ids: [],
  source_prod_id: id,
  data_quality_notes: [],
  updated_at: null,
  ...options,
});

test('hero deck is deterministic, current, unique and mixed when photos exist', () => {
  const events = [event(1, '2026-07-28'), event(2, '2026-07-29'), event(3, '2026-07-30'), event(4, '2026-08-01'), event(5, '2026-08-02')];
  const first = buildHomeHeroTalkDeck(events, '2026-07-29', 'immutable-build');
  const second = buildHomeHeroTalkDeck(events, '2026-07-29', 'immutable-build');
  assert.deepEqual(first.map(({ event: item, mode }) => [item.id, mode]), second.map(({ event: item, mode }) => [item.id, mode]));
  assert.equal(new Set(first.map((scene) => scene.event.id)).size, first.length);
  assert.ok(first.every((scene) => (scene.event.end_date || scene.event.start_date) >= '2026-07-29'));
  assert.ok(first.some((scene) => scene.mode === 'photo-mosaic'));
  assert.ok(first.every((scene, index) => index < 2 || !(scene.mode === 'text-only' && first[index - 1].mode === 'text-only' && first[index - 2].mode === 'text-only')));
  assert.ok(first.every((scene, index) => index === 0 || !(scene.mode === 'photo-mosaic' && first[index - 1].mode === 'photo-mosaic')));
});

test('past high-engagement and inactive events never enter hero deck', () => {
  const past = event(90, '2026-07-01', { source_views_count: 99_000_000, popularity_signal_score: 999 });
  const cancelled = event(91, '2026-08-01', { lifecycle_status: 'cancelled', source_views_count: 99_000_000 });
  const deck = buildHomeHeroTalkDeck([past, cancelled, event(1, '2026-07-30')], '2026-07-29', 'seed');
  assert.deepEqual(deck.map((scene) => scene.event.id), [1]);
});

test('mutually linked occurrences appear only once', () => {
  const first = event(10, '2026-07-30', { other_date_ids:[11] });
  const second = event(11, '2026-08-02', { other_date_ids:[10] });
  const ids = buildHomeHeroTalkDeck([first, second, event(12, '2026-08-03')], '2026-07-29', 'seed')
    .map((scene) => scene.event.id);
  assert.equal(ids.filter((id) => id === 10 || id === 11).length, 1);
});
