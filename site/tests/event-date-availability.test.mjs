import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';
import { buildEventDateAvailability, eventDateRouteDates } from '../src/lib/eventDateAvailability.ts';

const read = (path) => readFile(new URL(`../${path}`, import.meta.url), 'utf8');

test('calendar reaches the final day of the furthest event month and expands multi-day availability', () => {
  const availability = buildEventDateAvailability([
    { start_date:'2026-07-30', end_date:'2026-08-02', lifecycle_status:'active' },
    { start_date:'2026-10-09', end_date:null, lifecycle_status:'active' },
    { start_date:'2027-01-10', end_date:null, lifecycle_status:'cancelled' },
  ], '2026-07-27');
  assert.equal(availability.furthestEventDate, '2026-10-09');
  assert.equal(availability.horizonEnd, '2026-10-31');
  assert.deepEqual([...availability.availableDates].slice(0, 4), ['2026-07-30','2026-07-31','2026-08-01','2026-08-02']);
  assert.equal(availability.availableDates.has('2026-08-03'), false);
  assert.equal(availability.allDates.at(-1), '2026-10-31');
  assert.deepEqual(eventDateRouteDates([
    { start_date:'2026-08-03', end_date:null, lifecycle_status:'active' },
    { start_date:'2026-08-01', end_date:null, lifecycle_status:'active' },
  ], '2026-07-27'), ['2026-08-01','2026-08-03']);
});

test('calendar UI emits no anchor for empty dates and route generation uses the shared inventory', async () => {
  const [accessory, route] = await Promise.all([
    read('src/components/listings/MobileDateAccessory.astro'),
    read('src/pages/date-[date].astro'),
  ]);
  assert.match(accessory, /buildEventDateAvailability\(getEvents\(\), today\)/u);
  assert.match(accessory, /item\.href \? \([\s\S]*?<a[\s\S]*?\) : \([\s\S]*?<span[\s\S]*?aria-disabled="true"/u);
  assert.match(accessory, /data-calendar-month-next/u);
  assert.match(accessory, /data-calendar-horizon=\{availability\.horizonEnd\}/u);
  assert.doesNotMatch(accessory, /Array\.from\(\{ length: 42 \}/u);
  assert.match(route, /eventDateRouteDates\(getEvents\(\), getCurrentDate\(\)\)/u);
});
