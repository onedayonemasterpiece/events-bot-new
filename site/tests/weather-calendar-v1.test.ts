import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { test } from 'node:test';
import {
  calendarDayDistance,
  formatAirRange,
  formatWaterTemperature,
  parseWeatherCalendarPointer,
  parseWeatherCalendarSnapshot,
  roundHalfUpOneDecimal,
  selectVisibleWeatherDay,
  shouldShowWaterTemperature,
  weatherCondition,
} from '../src/lib/weatherCalendar.ts';
import { loadWeatherCalendarSnapshot } from '../src/lib/weatherCalendarRuntime.ts';

const fixture = async (name: string) => readFile(new URL(`./fixtures/weather-calendar/${name}`, import.meta.url), 'utf8');

async function validSnapshot() {
  return parseWeatherCalendarSnapshot(JSON.parse(await fixture('weather-calendar-v1.valid.json')))!;
}

async function validPointer() {
  return parseWeatherCalendarPointer(JSON.parse(await fixture('weather-calendar-pointer-v1.valid.json')))!;
}

test('weather-calendar-v1 parses the exact ordered seven-day fixture', async () => {
  const snapshot = await validSnapshot();
  assert.ok(snapshot);
  assert.equal(snapshot.days.length, 7);
  assert.equal(snapshot.days[0].date, '2026-08-03');
  assert.equal(snapshot.days[6].date, '2026-08-09');
  assert.equal(parseWeatherCalendarSnapshot({ ...snapshot, schema: 'weather-calendar-v2' }), null);
  assert.equal(parseWeatherCalendarSnapshot({ ...snapshot, unknown: true }), null);
  assert.equal(parseWeatherCalendarSnapshot({ ...snapshot, days: [...snapshot.days].reverse() }), null);
});

test('water visibility is based on the displayed half-up tenth', async () => {
  const snapshot = await validSnapshot();
  assert.equal(roundHalfUpOneDecimal(16.04), 16);
  assert.equal(roundHalfUpOneDecimal(16.05), 16.1);
  assert.equal(shouldShowWaterTemperature(16.04), false);
  assert.equal(shouldShowWaterTemperature(16.05), true);
  assert.equal(formatWaterTemperature(snapshot.days[0].coast!), null);
  assert.equal(formatWaterTemperature(snapshot.days[1].coast!), '+16,1°');
  assert.equal(formatAirRange(snapshot.days[0].kaliningrad!), '+19…+24°');
});

test('runtime date, horizon and freshness guards fail closed', async () => {
  const snapshot = await validSnapshot();
  const now = new Date('2026-08-03T18:30:00Z');
  assert.equal(calendarDayDistance('2026-08-03', '2026-08-09'), 6);
  assert.ok(selectVisibleWeatherDay(snapshot, '2026-08-03', 'today', now));
  assert.ok(selectVisibleWeatherDay(snapshot, '2026-08-04', 'tomorrow', now));
  assert.equal(selectVisibleWeatherDay(snapshot, '2026-08-04', 'today', now), null);
  assert.ok(selectVisibleWeatherDay(snapshot, '2026-08-09', 'weekend', now));
  assert.equal(selectVisibleWeatherDay(snapshot, '2026-08-10', 'date', now), null);
  assert.equal(selectVisibleWeatherDay(snapshot, '2026-08-02', 'date', now), null);
  const stale = parseWeatherCalendarSnapshot(JSON.parse(await fixture('weather-calendar-v1.stale.json')))!;
  assert.ok(stale);
  assert.equal(selectVisibleWeatherDay(stale, '2026-08-03', 'today', now), null);
});

test('partial city, coast and sea-only days remain independently usable', async () => {
  const snapshot = await validSnapshot();
  const now = new Date('2026-08-03T18:30:00Z');
  const cityOnly = selectVisibleWeatherDay(snapshot, '2026-08-05', 'date', now);
  assert.ok(cityOnly?.kaliningrad);
  assert.equal(cityOnly?.coast, undefined);
  const coastOnly = selectVisibleWeatherDay(snapshot, '2026-08-06', 'date', now);
  assert.equal(coastOnly?.kaliningrad, undefined);
  assert.ok(coastOnly?.coast);
  const seaOnly = selectVisibleWeatherDay(snapshot, '2026-08-07', 'date', now);
  assert.equal(seaOnly?.quality, 'degraded');
  assert.equal(formatWaterTemperature(seaOnly!.coast!), '+17,4°');
});

test('WMO codes map to a coherent textual and icon vocabulary', () => {
  assert.deepEqual(weatherCondition(0), { label: 'Ясно', icon: 'clear' });
  assert.deepEqual(weatherCondition(45), { label: 'Туман', icon: 'fog' });
  assert.deepEqual(weatherCondition(80), { label: 'Ливень', icon: 'showers' });
  assert.deepEqual(weatherCondition(95), { label: 'Гроза', icon: 'thunderstorm' });
});

test('same-origin loader verifies pointer, immutable path and SHA-256', async () => {
  const pointerText = await fixture('weather-calendar-pointer-v1.valid.json');
  const snapshotText = await fixture('weather-calendar-v1.valid.json');
  const pointer = await validPointer();
  const calls: string[] = [];
  const fetchImpl = async (input: URL | RequestInfo) => {
    const url = String(input);
    calls.push(url);
    if (url.endsWith('/data/weather/v1/current.json')) return new Response(pointerText, { status: 200 });
    if (url.endsWith(`/${pointer.sha256}.json`)) return new Response(snapshotText, { status: 200 });
    return new Response('', { status: 404 });
  };
  const result = await loadWeatherCalendarSnapshot({
    pointerUrl: '/preview/data/weather/v1/current.json',
    pageUrl: 'https://kenigevents.ru/preview/segodnya/',
    fetchImpl: fetchImpl as typeof fetch,
    cryptoImpl: globalThis.crypto,
  });
  assert.equal(result.ok, true);
  assert.equal(calls.length, 2);
  assert.ok(calls.every((url) => new URL(url).origin === 'https://kenigevents.ru'));
});

test('loader blocks external pointer URLs before dispatch and rejects tampering', async () => {
  let calls = 0;
  const external = await loadWeatherCalendarSnapshot({
    pointerUrl: 'https://api.open-meteo.com/data/weather/v1/current.json',
    pageUrl: 'https://kenigevents.ru/segodnya/',
    fetchImpl: (async () => { calls += 1; return new Response('{}'); }) as typeof fetch,
    cryptoImpl: globalThis.crypto,
  });
  assert.deepEqual(external, { ok: false, reason: 'pointer_url_invalid' });
  assert.equal(calls, 0);

  const pointerText = await fixture('weather-calendar-pointer-v1.valid.json');
  const tampered = await loadWeatherCalendarSnapshot({
    pointerUrl: '/preview/data/weather/v1/current.json',
    pageUrl: 'https://kenigevents.ru/preview/segodnya/',
    fetchImpl: (async (input: URL | RequestInfo) => String(input).endsWith('current.json')
      ? new Response(pointerText)
      : new Response(`${await fixture('weather-calendar-v1.valid.json')} `)) as typeof fetch,
    cryptoImpl: globalThis.crypto,
  });
  assert.deepEqual(tampered, { ok: false, reason: 'snapshot_integrity_mismatch' });
});
