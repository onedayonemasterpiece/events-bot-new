import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';
import {
  buildEventDateAvailability,
  eventDateManifest,
  eventDateRouteDates,
  kaliningradDate,
  resolveTodayReview,
} from '../src/lib/eventDateAvailability.ts';

const read = (path) => readFile(new URL(`../${path}`, import.meta.url), 'utf8');

test('calendar reaches the final event start month without inventing multi-day listing routes', () => {
  const availability = buildEventDateAvailability([
    { start_date:'2026-07-30', end_date:'2026-08-02', lifecycle_status:'active' },
    { start_date:'2026-10-09', end_date:null, lifecycle_status:'active' },
    { start_date:'2027-01-10', end_date:null, lifecycle_status:'cancelled' },
  ], '2026-07-27');
  assert.equal(availability.furthestEventDate, '2026-10-09');
  assert.equal(availability.horizonEnd, '2026-10-31');
  assert.deepEqual([...availability.availableDates].slice(0, 2), ['2026-07-30','2026-10-09']);
  assert.equal(availability.availableDates.has('2026-07-31'), false);
  assert.equal(availability.availableDates.has('2026-08-03'), false);
  assert.equal(availability.allDates.at(-1), '2026-10-31');
  assert.deepEqual(eventDateRouteDates([
    { start_date:'2026-08-03', end_date:null, lifecycle_status:'active' },
    { start_date:'2026-08-01', end_date:null, lifecycle_status:'active' },
  ], '2026-07-27'), ['2026-08-01','2026-08-03']);
});

test('calendar UI emits no anchor for empty dates and route generation uses the shared inventory', async () => {
  const [accessory, route, endpoint, today, inventory] = await Promise.all([
    read('src/components/listings/MobileDateAccessory.astro'),
    read('src/pages/date-[date].astro'),
    read('src/pages/data/event-dates.json.ts'),
    read('src/pages/segodnya/index.astro'),
    read('src/lib/staticEventDates.ts'),
  ]);
  assert.match(accessory, /getStaticEventDateAvailability\(\)/u);
  assert.match(accessory, /item\.href \? \([\s\S]*?<a[\s\S]*?\) : \([\s\S]*?<span[\s\S]*?aria-disabled="true"/u);
  assert.match(accessory, /data-calendar-month-next/u);
  assert.match(accessory, /data-calendar-horizon=\{availability\.horizonEnd\}/u);
  assert.doesNotMatch(accessory, /Array\.from\(\{ length: 42 \}/u);
  assert.match(route, /getStaticEventDateAvailability\(\)\.availableDates/u);
  assert.match(endpoint, /getStaticEventDateManifest\(\)/u);
  assert.match(inventory, /getEvents\(\)\.filter\(\(event\) => !isExhibitionLikeEvent\(event\)\)/u);
  assert.match(today, /id="event-date-availability"/u);
  assert.match(today, /resolveTodayReview/u);
  assert.match(today, /location\.replace\(target\)/u);
  assert.match(today, /Это сохранённая версия, а не события на сегодня/u);
});

test('Kaliningrad date and stale Today resolution are deterministic at midnight', () => {
  assert.equal(kaliningradDate(new Date('2026-07-27T21:59:59.999Z')), '2026-07-27');
  assert.equal(kaliningradDate(new Date('2026-07-27T22:00:00.000Z')), '2026-07-28');

  assert.deepEqual(resolveTodayReview('2026-07-27', '2026-07-27', ['2026-07-28']), {
    state:'current',
    buildDate:'2026-07-27',
    runtimeDate:'2026-07-27',
    redirectDate:null,
  });
  assert.deepEqual(resolveTodayReview('2026-07-27', '2026-07-28', ['2026-07-28']), {
    state:'redirect',
    buildDate:'2026-07-27',
    runtimeDate:'2026-07-28',
    redirectDate:'2026-07-28',
  });
  assert.deepEqual(resolveTodayReview('2026-07-27', '2026-07-29', ['2026-07-28']), {
    state:'stale',
    buildDate:'2026-07-27',
    runtimeDate:'2026-07-29',
    redirectDate:null,
  });
  assert.throws(
    () => resolveTodayReview('2026-02-31', '2026-03-01', []),
    /Invalid Today review dates/u,
  );
});

test('generated event-date manifest retains enabled and disabled calendar days', () => {
  const availability = buildEventDateAvailability([
    { start_date:'2026-07-28', end_date:null, lifecycle_status:'active' },
  ], '2026-07-27');
  const manifest = eventDateManifest(availability, '2026-07-27T10:00:00Z');
  assert.equal(manifest.schema_version, 'event-date-availability-v1');
  assert.equal(manifest.current_date, '2026-07-27');
  assert.deepEqual(manifest.dates.slice(0, 3), [
    { date:'2026-07-27', has_events:false },
    { date:'2026-07-28', has_events:true },
    { date:'2026-07-29', has_events:false },
  ]);
});
