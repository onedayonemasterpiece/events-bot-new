import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import { stripTypeScriptTypes } from 'node:module';
import test from 'node:test';

const moduleUrl = new URL('../src/lib/eventTransport.ts', import.meta.url);
const schedulesUrl = new URL('../src/data/transportSchedules.json', import.meta.url);
const estimatesUrl = new URL('../src/data/event-duration-estimates.json', import.meta.url);
const componentUrl = new URL('../src/components/EventTransportSchedule.astro', import.meta.url);

async function loadEventTransport() {
  const [moduleSource, scheduleSource, estimateSource] = await Promise.all([
    fs.readFile(moduleUrl, 'utf8'),
    fs.readFile(schedulesUrl, 'utf8'),
    fs.readFile(estimatesUrl, 'utf8'),
  ]);
  let source = moduleSource
    .replace(
      "import scheduleData from '../data/transportSchedules.json';",
      `const scheduleData = ${scheduleSource};`,
    )
    .replace(
      "import durationEstimatesData from '../data/event-duration-estimates.json';",
      `const durationEstimatesData = ${estimateSource};`,
    )
    .replace("import type { PreviewEvent } from './types';", '');
  const javascript = stripTypeScriptTypes(source, { mode: 'transform' });
  return import(`data:text/javascript;base64,${Buffer.from(javascript).toString('base64')}`);
}

const eventTransport = await loadEventTransport();

const estimateEvent = {
  id: 6529,
  city: 'Зеленоградск',
  venue_name: 'Музей курортной моды',
  start_date: '2026-07-26',
  end_date: null,
  start_time: '15:00',
  time_range_end: null,
  event_type: 'мастер-класс',
};

test('6529 uses the key-driven build-time estimate and same-day returns', () => {
  const suggestion = eventTransport.getEventTransportSuggestion(estimateEvent);
  assert.ok(suggestion);
  assert.equal(suggestion.eventEnd, null, 'an estimate must not become a canonical event end');
  assert.equal(suggestion.eventEndBasis, 'llm_estimated');
  assert.deepEqual(
    {
      sourceStatus: suggestion.durationEstimate?.sourceStatus,
      canonicalEnd: suggestion.durationEstimate?.canonicalEnd,
      mostLikelyMinutes: suggestion.durationEstimate?.mostLikelyMinutes,
      plausibleMinMinutes: suggestion.durationEstimate?.plausibleMinMinutes,
      plausibleMaxMinutes: suggestion.durationEstimate?.plausibleMaxMinutes,
      conservativeRoutingMinutes: suggestion.durationEstimate?.conservativeRoutingMinutes,
      predictedEndTime: suggestion.durationEstimate?.predictedEndTime,
      returnAccessMinutes: suggestion.returnAccessMinutes,
      returnReadyTime: suggestion.returnReadyTime,
    },
    {
      sourceStatus: 'llm_estimated',
      canonicalEnd: false,
      mostLikelyMinutes: 120,
      plausibleMinMinutes: 90,
      plausibleMaxMinutes: 180,
      conservativeRoutingMinutes: 150,
      predictedEndTime: '17:30',
      returnAccessMinutes: 25,
      returnReadyTime: '17:55',
    },
  );
  assert.deepEqual(suggestion.returns.map((train) => train.departure), ['18:56', '19:43']);
  assert.ok(suggestion.returns.length >= 2);
  assert.ok(suggestion.returns.every((train) => !train.nextDay && train.serviceDate === estimateEvent.start_date));
  assert.equal(suggestion.firstNightReturn, null);
  assert.equal(suggestion.firstNextDayReturn, null);
  assert.ok(eventTransport.eventTransportCalendarEntries(suggestion).every(({ train }) => !train.nextDay));
});

test('unknown duration without an estimate fails closed at the same-day boundary', () => {
  const suggestion = eventTransport.getEventTransportSuggestion({ ...estimateEvent, id: 999_999 });
  assert.ok(suggestion);
  assert.equal(suggestion.eventEndBasis, 'schedule_cutoff');
  assert.equal(suggestion.durationEstimate, null);
  assert.deepEqual(suggestion.returns, []);
  assert.equal(suggestion.lastSameDayReturn?.departure, '22:45');
  assert.equal(suggestion.firstNightReturn, null);
  assert.equal(suggestion.firstNextDayReturn, null);
  const calendarEntries = eventTransport.eventTransportCalendarEntries(suggestion);
  assert.ok(calendarEntries.some(({ direction, train }) => direction === 'return' && train.departure === '22:45'));
  assert.ok(calendarEntries.every(({ train }) => !train.nextDay && train.serviceDate === estimateEvent.start_date));
});

test('build-time provider estimates are available to production static generation', () => {
  const suggestion = eventTransport.getEventTransportSuggestion(estimateEvent);
  assert.ok(suggestion);
  assert.equal(suggestion.eventEndBasis, 'llm_estimated');
  assert.equal(suggestion.durationEstimate?.conservativeRoutingMinutes, 150);
  assert.deepEqual(suggestion.returns.map((train) => train.departure), ['18:56', '19:43']);
});

test('3103 explicit end remains authoritative over the optional estimate path', () => {
  const suggestion = eventTransport.getEventTransportSuggestion({
    id: 3103,
    city: 'Светлогорск',
    venue_name: 'Янтарь холл',
    start_date: '2026-08-15',
    end_date: null,
    start_time: '18:00',
    time_range_end: '19:40',
    event_type: 'спектакль',
  });
  assert.ok(suggestion);
  assert.equal(suggestion.eventEndBasis, 'explicit');
  assert.equal(suggestion.eventEnd, '19:40');
  assert.equal(suggestion.durationEstimate, null);
  assert.equal(suggestion.returnAccessMinutes, 30);
  assert.equal(suggestion.returnReadyTime, '20:10');
  assert.deepEqual(suggestion.returns.map((train) => train.departure), ['20:36', '21:43']);
});

test('transport UI keeps provider mechanics private and never offers next-morning waiting', async () => {
  const source = await fs.readFile(componentUrl, 'utf8');
  assert.match(source, /Время окончания не указано/u);
  assert.match(source, /уточните продолжительность у организатора/u);
  assert.doesNotMatch(source, /Экспериментальн/u);
  assert.doesNotMatch(source, /прогноз ИИ/u);
  assert.doesNotMatch(source, /Gemini|Gemma|confidence|modelId/u);
  assert.match(source, /поезд на следующий день не предлагаем/u);
  assert.doesNotMatch(source, /Первый поезд .*следующ/u);
  assert.doesNotMatch(source, /После полуночи есть рейс/u);
});
