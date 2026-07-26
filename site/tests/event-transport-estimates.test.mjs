import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import { stripTypeScriptTypes } from 'node:module';
import test from 'node:test';

const helperUrl = new URL('../src/lib/desktopEventTransport.ts', import.meta.url);
const transportUrl = new URL('../src/lib/eventTransport.ts', import.meta.url);
const medallionsUrl = new URL('../src/lib/eventMedallions.ts', import.meta.url);
const schedulesUrl = new URL('../src/data/transportSchedules.json', import.meta.url);
const componentUrl = new URL('../src/components/EventTransportSchedule.astro', import.meta.url);
const transportIcsRouteUrl = new URL('../src/pages/sobytiya/[slug]/transport/[trip].ics.ts', import.meta.url);

async function loadTypeScriptModule(url, replacements = []) {
  let source = await fs.readFile(url, 'utf8');
  for (const [from, to] of replacements) source = source.replace(from, to);
  source = source.replaceAll(/import type .*?;\n/gu, '');
  const javascript = stripTypeScriptTypes(source, { mode: 'transform' });
  return import(`data:text/javascript;base64,${Buffer.from(javascript).toString('base64')}`);
}

const helper = await loadTypeScriptModule(helperUrl);
const medallions = await loadTypeScriptModule(medallionsUrl);
const scheduleSource = await fs.readFile(schedulesUrl, 'utf8');
const transport = await loadTypeScriptModule(transportUrl, [[
  "import scheduleData from '../data/transportSchedules.json';",
  `const scheduleData = ${scheduleSource};`,
]]);

const baseEvent = {
  id: 6529,
  city: 'Зеленоградск',
  venue_name: 'Музей курортной моды',
  start_date: '2026-07-26',
  end_date: null,
  start_time: '15:00',
  time_range_end: null,
  event_type: 'мастер-класс',
  description_html: '',
};

test('persisted forecast is projected separately and yields practical same-day returns', () => {
  const projected = helper.desktopEventWithExplicitEnd({
    ...baseEvent,
    duration_forecast_minutes: 120,
  });
  assert.equal(projected.time_range_end, '17:00');
  assert.equal(projected.transport_end_basis, 'forecast');

  const suggestion = transport.getEventTransportSuggestion(projected);
  assert.ok(suggestion);
  assert.deepEqual(medallions.resolveRailTransportMedallion(suggestion), {
    slug:'rzd-lastochka',
    name:'Электропоезд «Ласточка»',
    avatarUrl:'/assets/transport/rzd-lastochka-medallion.webp',
    fallbackPngUrl:'/assets/transport/rzd-lastochka-medallion.png',
    ariaLabel:'Транспортная подсказка: электропоезд «Ласточка»',
  });
  assert.equal(suggestion.eventEndBasis, 'forecast');
  assert.equal(suggestion.eventEnd, '17:00');
  assert.equal(suggestion.returnReadyTime, '17:25');
  assert.deepEqual(suggestion.returns.map((train) => train.departure), ['17:50', '18:56']);
  assert.ok(suggestion.returns.every((train) => !train.nextDay));
});

test('rail medallion eligibility fails closed without a generated rail payload', () => {
  const suggestion = transport.getEventTransportSuggestion({
    ...baseEvent,
    id:7018,
    city:'Озёрск',
    venue_name:'центр «Крупорушка»',
  });
  assert.equal(suggestion, null);
  assert.equal(medallions.resolveRailTransportMedallion(suggestion), null);
});

test('source-labelled duration wins over a persisted forecast', () => {
  const projected = helper.desktopEventWithExplicitEnd({
    ...baseEvent,
    description_html: '<p>Продолжительность: 45 минут</p>',
    duration_forecast_minutes: 120,
  });
  assert.equal(projected.time_range_end, '15:45');
  assert.equal(projected.transport_end_basis, 'source_duration');
  assert.equal(transport.getEventTransportSuggestion(projected)?.eventEndBasis, 'explicit');
});

test('unknown duration fails closed at the same-day boundary without morning trains', () => {
  const suggestion = transport.getEventTransportSuggestion(baseEvent);
  assert.ok(suggestion);
  assert.equal(suggestion.eventEndBasis, 'schedule_cutoff');
  assert.deepEqual(suggestion.returns, []);
  assert.equal(suggestion.lastSameDayReturn?.departure, '22:45');
  assert.equal(suggestion.firstNightReturn, null);
  assert.equal(suggestion.firstNextDayReturn, null);
  assert.ok(
    transport.eventTransportCalendarEntries(suggestion)
      .every(({ train }) => !train.nextDay && train.serviceDate === baseEvent.start_date),
  );
});

test('explicit end remains authoritative', () => {
  const suggestion = transport.getEventTransportSuggestion({
    ...baseEvent,
    id: 3103,
    city: 'Светлогорск',
    venue_name: 'Янтарь холл',
    start_date: '2026-08-15',
    start_time: '18:00',
    time_range_end: '19:40',
  });
  assert.ok(suggestion);
  assert.equal(suggestion.eventEndBasis, 'explicit');
  assert.equal(suggestion.eventEnd, '19:40');
  assert.equal(suggestion.returnAccessMinutes, 30);
  assert.equal(suggestion.returnReadyTime, '20:10');
  assert.deepEqual(suggestion.returns.map((train) => train.departure), ['20:36', '21:43']);
});

test('public transport UI is neutral and contains no provider/service diagnostics', async () => {
  const source = await fs.readFile(componentUrl, 'utf8');
  assert.match(source, /Время окончания ориентировочное/u);
  assert.match(source, /проверьте его у организатора/u);
  assert.match(source, /поезд на следующий день не предлагаем/u);
  assert.doesNotMatch(source, /Gemini|Gemma|модел[ьи]|provider|прогноз ИИ|служебн/iu);
  assert.doesNotMatch(source, /data-duration-estimate|data-predicted-event-end/iu);
});

test('transport ICS paths use the same single responsive forecast projection', async () => {
  const source = await fs.readFile(transportIcsRouteUrl, 'utf8');
  assert.match(source, /const transportEvent = desktopEventWithExplicitEnd\(event\)/u);
  assert.match(source, /getEventTransportSuggestion\(transportEvent\)/u);
  assert.doesNotMatch(source, /const variants = \[event,/u);
});
