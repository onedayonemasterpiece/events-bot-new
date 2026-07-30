import assert from 'node:assert/strict';
import test from 'node:test';
import type { EventImageAsset, PreviewEvent } from './types';
import { createDateHeroTileSchedule, selectDateListingHero } from './dateListingHero.ts';

const asset = (overrides: Partial<EventImageAsset> = {}): EventImageAsset => ({
  src: 'https://static.kenigevents.ru/safe.webp',
  width: 1600,
  height: 1000,
  alt: '',
  image_text_mode: 'visual_only',
  media_role: 'event_photo',
  image_kind: 'photo',
  recommended_hero_fit: 'cover',
  safe_crop: true,
  focal_point: { x:.5, y:.5 },
  current_pixel_sha256: 'same',
  geometry_pixel_sha256: 'same',
  quality_score: 10,
  ...overrides,
});

const event = (id: number, start = '2026-07-30', overrides: Partial<PreviewEvent> = {}): PreviewEvent => ({
  id,
  title:`Событие ${id}`,
  slug:`event-${id}`,
  event_type:'концерт',
  festival:null,
  status_label:'Билеты',
  lifecycle_status:'active',
  starts_at:`${start}T18:00:00+02:00`,
  start_date:start,
  start_time:'18:00',
  end_date:null,
  end_at:null,
  time_range_end:null,
  timezone:'Europe/Kaliningrad',
  display_date:start,
  display_time:'18:00',
  city:'Калининград',
  venue_name:'Площадка',
  address:null,
  map_query:null,
  ticket:{ kind:'ticket', label:'Билеты', href:null, status:null, is_free:false, price_label:null },
  age_restriction:null,
  age_restriction_status:'unknown',
  age_restriction_provenance:null,
  age_restriction_decision_version:null,
  age_recommendation:null,
  age_recommendation_label:null,
  source_url:null,
  telegraph_url:null,
  image_url:null,
  image_alt:'',
  image_text_mode:'visual_only',
  image_assets:[asset()],
  summary:'',
  meta_description:'',
  description_html:'',
  topics:[],
  likes_count:id,
  source_likes_count:id,
  service_likes_count:0,
  pushkin_card:false,
  other_date_ids:[],
  source_prod_id:id,
  data_quality_notes:[],
  updated_at:null,
  ...overrides,
});

test('date hero selects an active exact-date event and returns its canonical occurrence', () => {
  const ongoing = event(1, '2026-07-20', { end_date:'2026-08-10', popularity_signal_score:100 });
  const exact = event(2, '2026-07-30', { popularity_signal_score:1 });
  assert.equal(selectDateListingHero([ongoing, exact], '2026-07-30')?.event.id, 2);
});

test('date hero fails closed for OCR, unsafe, stale geometry and wrong date', () => {
  const candidates = [
    event(1, '2026-07-29'),
    event(2, '2026-07-30', { image_assets:[asset({ image_text_mode:'ocr_text' })] }),
    event(3, '2026-07-30', { image_assets:[asset({ safe_crop:false })] }),
    event(4, '2026-07-30', { image_assets:[asset({ geometry_pixel_sha256:'old' })] }),
    event(5, '2026-07-30', { lifecycle_status:'cancelled' }),
  ];
  assert.equal(selectDateListingHero(candidates, '2026-07-30'), null);
});

test('mutual occurrence family is resolved once and tile schedule is stable', () => {
  const first = event(10, '2026-07-30', { other_date_ids:[11], popularity_signal_score:1 });
  const second = event(11, '2026-07-30', { other_date_ids:[10], popularity_signal_score:2 });
  assert.equal(selectDateListingHero([first, second], '2026-07-30')?.familyKey, 10);
  const schedule = createDateHeroTileSchedule('2026-07-30:11');
  assert.equal(schedule.length, 66);
  assert.deepEqual(schedule, createDateHeroTileSchedule('2026-07-30:11'));
  assert.equal(new Set(schedule.map((tile) => tile.exitStart)).size, 66);
  assert.ok(schedule.filter((tile) => tile.baseAlpha <= .06).length >= 30);
  const topRight = schedule.filter((tile) => tile.row <= 1 && tile.col >= 9)
    .reduce((sum, tile) => sum + tile.baseAlpha, 0) / 4;
  const bottomLeft = schedule.filter((tile) => tile.row >= 4 && tile.col <= 4)
    .reduce((sum, tile) => sum + tile.baseAlpha, 0) / 10;
  assert.ok(topRight > bottomLeft * 6);
  assert.equal(Math.max(...schedule.map((tile) => tile.baseAlpha)), .9);
});
