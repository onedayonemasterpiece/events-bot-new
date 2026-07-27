import assert from 'node:assert/strict';
import { access, readFile } from 'node:fs/promises';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

import {
  classifyEventMedallionLayout,
  matchMedallionAlias,
  resolveEventMedallions,
} from './eventMedallions.ts';

const siteRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../..');
const organizers = JSON.parse(await readFile(path.join(siteRoot, 'src/data/organizerMedallions.json'), 'utf8')).items;
const festivals = JSON.parse(await readFile(path.join(siteRoot, 'src/data/festivalMedallions.json'), 'utf8')).items;
const previewEvents = JSON.parse(await readFile(path.join(siteRoot, 'src/data/preview-events.json'), 'utf8')).events;
const organizerSlugs = new Set(organizers.map((item) => item.slug));
const eventPageFestivals = festivals.filter((item) => (
  item.category === 'festival'
  && item.slug !== 'kgd80-80-stories'
  && !organizerSlugs.has(item.slug)
));
const eventPageCatalog = [...organizers, ...eventPageFestivals];

const event = (overrides = {}) => ({
  venue_name:null,
  address:null,
  festival:null,
  organizer_names:[],
  source_url:null,
  source_urls:[],
  ...overrides,
});

const slugs = (value) => resolveEventMedallions(value, eventPageCatalog).identities.map(({ item }) => item.slug);

test('full manifests keep exact accepted inventory and reachable runtime/source assets', async () => {
  assert.equal(organizers.length, 28);
  assert.equal(festivals.length, 11);
  assert.equal(festivals.filter((item) => item.category === 'festival').length, 10);
  assert.equal(festivals.filter((item) => item.category === 'venue_brand').length, 1);

  for (const item of [...organizers, ...festivals]) {
    await access(path.join(siteRoot, 'public', item.avatarUrl));
    await access(path.resolve(siteRoot, '..', item.sourcePath));
    if (item.avatarUrl.endsWith('.svg')) {
      const fallback = item.fallbackPngUrl || item.avatarUrl.replace(/\.svg$/u, '.png');
      await access(path.join(siteRoot, 'public', fallback));
    } else if (item.fallbackPngUrl) {
      await access(path.join(siteRoot, 'public', item.fallbackPngUrl));
    }
  }
});

test('Unicode boundaries keep short aliases standalone', () => {
  assert.equal(matchMedallionAlias('с программой скандинавских песен', 'ММО'), null);
  assert.equal(matchMedallionAlias('Площадка: ММО', 'ММО'), 'bounded');
  assert.equal(matchMedallionAlias('ММО', 'ММО'), 'exact');
});

test('structured venue address resolves an expected brand without prose inference', () => {
  const result = resolveEventMedallions(event({
    venue_name:'Культурное место',
    address:'Остров Канта',
    festival:'Написано в Калининграде',
  }), eventPageCatalog);
  assert.deepEqual(result.identities.map(({ item }) => item.slug), ['kant-island']);
  assert.equal(result.identities[0].evidence.field, 'venue_address');
  assert.equal(result.identities[0].evidence.alias, 'Остров Канта');
});

test('current mumod, Dramatic Theatre, Kaup and historical Greza aliases resolve exactly', () => {
  assert.deepEqual(slugs(event({ venue_name:'Музей курортной моды' })), ['mumod']);
  assert.deepEqual(slugs(event({ venue_name:'Драматический театр' })), ['dramteatr39']);
  assert.deepEqual(slugs(event({ venue_name:'Поселение викингов Кауп' })), ['kaup']);
  assert.deepEqual(slugs(event({ venue_name:'Грёза Хутор, пос. Тихомировка, Озёрск' })), ['greza-khutor']);
});

test('structured organizers resolve Profi-Tour and Ruin Keepers without venue inference', () => {
  assert.deepEqual(slugs(event({
    venue_name:'Судостроительный завод «Янтарь»',
    organizer_names:['Профи-тур'],
  })), ['profitur']);
  assert.deepEqual(slugs(event({
    venue_name:'Железнодорожные ворота',
    festival:'Воротник',
    organizer_names:['Хранители руин'],
  })), ['ruin-keepers']);
  assert.deepEqual(slugs(event({
    venue_name:'Железнодорожные ворота',
    organizer_names:[],
  })), []);
});

test('MUMOD is the single Main medallion and leaves InlineSlot empty', () => {
  const mumodEvent = previewEvents.find((candidate) => candidate.id === 6529);
  assert.ok(mumodEvent, 'current event 6529 must remain available as the MUMOD regression');
  const resolution = resolveEventMedallions(mumodEvent, eventPageCatalog);
  const layout = classifyEventMedallionLayout(resolution);

  assert.equal(layout.main?.item.slug, 'mumod');
  assert.deepEqual(layout.secondary, []);
  assert.equal(layout.main?.evidence.field, 'venue_name');
  assert.equal(layout.main?.evidence.match, 'exact');
});

test('structured festival is Main while the resolved venue remains Secondary', () => {
  const resolution = resolveEventMedallions(event({
    venue_name:'Янтарь холл',
    festival:'ГРОЗДЬ',
  }), eventPageCatalog);
  const layout = classifyEventMedallionLayout(resolution);

  assert.equal(layout.main?.item.slug, 'grozd-festival');
  assert.deepEqual(layout.secondary.map(({ item }) => item.slug), ['yantar-hall']);
  assert.equal(layout.main?.evidence.field, 'festival');
  assert.equal(layout.secondary[0]?.evidence.field, 'venue_name');
});

test('principal layout classification consumes only fail-closed structured resolution', () => {
  const resolution = resolveEventMedallions(event({ venue_name:'ММО Кафедральный собор' }), eventPageCatalog);
  const layout = classifyEventMedallionLayout(resolution);

  assert.equal(resolution.failClosedReason, 'ambiguous_venue_identity');
  assert.equal(layout.main, undefined);
  assert.deepEqual(layout.secondary, []);
});

test('festival artwork matches only the structured festival field', () => {
  assert.deepEqual(
    slugs(event({ venue_name:'Янтарь холл', festival:'ГРОЗДЬ' })),
    ['yantar-hall', 'grozd-festival'],
  );
  assert.deepEqual(slugs(event({ festival:'МОРЕ ВНУТРИ' })), ['more-vnutri']);
  assert.deepEqual(slugs(event({ festival:'Калининград Сити Джаз' })), ['kaliningrad-city-jazz']);
  assert.deepEqual(slugs(event({ venue_name:'Толкин Фест клуб', festival:null })), []);
});

test('KGD80 keeps one event-page festival mark plus curated Znanie partner', () => {
  assert.ok(!eventPageFestivals.some((item) => item.slug === 'kgd80-80-stories'));
  const result = slugs(event({
    venue_name:'Историко-художественный музей',
    festival:'80 историй о главном',
  }));
  assert.deepEqual(result, ['history-art-museum', 'kgd80', 'znanie-russia']);
});

test('ambiguous venue identities and conflicting structured source identities still fail closed', () => {
  const ambiguous = resolveEventMedallions(event({ venue_name:'ММО Кафедральный собор' }), eventPageCatalog);
  assert.equal(ambiguous.failClosedReason, 'ambiguous_venue_identity');
  assert.deepEqual(ambiguous.identities, []);

  const conflicting = resolveEventMedallions(event({
    venue_name:'Филиал Третьяковской галереи',
    source_urls:[
      'https://kaliningrad.tretyakovgallery.ru/tickets/#/buy/event/46315/2026-07-17/20:00:00',
      'https://kaliningrad.tretyakovgallery.ru/tickets/#buy/event/47686/2026-07-17/16:30:00',
    ],
  }), eventPageCatalog);
  assert.equal(conflicting.failClosedReason, 'conflicting_source_identity');
  assert.deepEqual(conflicting.identities, []);
});
