import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import {
  matchMedallionAlias,
  resolveEventMedallions,
} from '../src/lib/eventMedallions.ts';
import { resolveEventFallbackArt } from '../src/lib/eventFallbackArt.ts';

const manifest = JSON.parse(readFileSync(new URL('../src/data/organizerMedallions.json', import.meta.url), 'utf8')).items;

test('R02 short aliases use Unicode token boundaries', () => {
  assert.equal(matchMedallionAlias('с программой скандинавских песен', 'ММО'), null);
  assert.equal(matchMedallionAlias('Площадка: ММО', 'ММО'), 'bounded');
  assert.equal(matchMedallionAlias('ММО', 'ММО'), 'exact');
});

test('R02 event 6796 resolves only structured KAUP venue evidence, never false MMO', () => {
  const result = resolveEventMedallions({
    venue_name:'Поселение викингов Кауп',
    festival:'Большой Кауп',
    source_url:'https://vk.com/wall-48845044_24886',
    source_urls:['https://vk.com/wall-48845044_24886'],
  }, manifest);
  assert.deepEqual(result.identities.map(({ item }) => item.slug), ['kaup']);
  assert.equal(result.identities[0].evidence.field, 'venue_name');
});

test('R02 true structured MMO identity is preserved', () => {
  const result = resolveEventMedallions({
    venue_name:'ММО', festival:null, source_url:null, source_urls:[],
  }, manifest);
  assert.deepEqual(result.identities.map(({ item }) => item.slug), ['world-ocean-museum']);
  assert.equal(result.identities[0].evidence.match, 'exact');
});

test('R02 mixed first-party ticket identities fail closed', () => {
  const result = resolveEventMedallions({
    venue_name:'Филиал Третьяковской галереи',
    festival:'Pianissimo',
    source_url:'https://kaliningrad.tretyakovgallery.ru/tickets/#/buy/event/46315/2026-07-17/20:00:00',
    source_urls:[
      'https://kaliningrad.tretyakovgallery.ru/tickets/#/buy/event/46315/2026-07-17/20:00:00',
      'https://kaliningrad.tretyakovgallery.ru/tickets/#buy/event/47686/2026-07-17/16:30:00',
    ],
  }, manifest);
  assert.equal(result.failClosedReason, 'conflicting_source_identity');
  assert.deepEqual(result.identities, []);
});

test('R02 never emits more than one venue-brand medallion', () => {
  const result = resolveEventMedallions({
    venue_name:'ММО Кафедральный собор', festival:null, source_url:null, source_urls:[],
  }, manifest);
  const venueTokens = result.identities.filter(({ item }) => item.category === 'venue_brand');
  assert.ok(venueTokens.length <= 1);
  assert.equal(result.failClosedReason, 'ambiguous_venue_identity');
});

test('R07 typed fallback art is presentation-only mapping by normalized type', () => {
  assert.equal(resolveEventFallbackArt({ event_type:'КОНЦЕРТ', topics:[] })?.kind, 'concert');
  assert.equal(resolveEventFallbackArt({ event_type:'встреча', topics:[] })?.kind, 'lecture');
  assert.equal(resolveEventFallbackArt({ event_type:'экскурсия', topics:[] }), null);
});
