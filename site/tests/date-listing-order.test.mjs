import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

import { chronologicalListingEvents } from '../src/lib/listingChronology.ts';

const event = (id, startTime) => ({
  id,
  start_date: '2026-07-26',
  start_time: startTime,
  display_time: startTime,
});

test('mobile Today completed rows stay in chronological time order', () => {
  const ordered = chronologicalListingEvents([
    event(12, '12:00'),
    event(13, '13:00'),
    event(11, '10:00'),
    event(10, '10:00'),
  ]);
  assert.deepEqual(ordered.map((item) => [item.start_time, item.id]), [
    ['10:00', 10],
    ['10:00', 11],
    ['12:00', 12],
    ['13:00', 13],
  ]);
});

test('date listing keeps mobile in one chronological stream and limits the split to desktop', async () => {
  const surface = await readFile(
    new URL('../src/components/listings/DateListingSurface.astro', import.meta.url),
    'utf8',
  );
  assert.match(
    surface,
    /const mobileChronologicalEvents = events;/u,
  );
  assert.match(
    surface,
    /sections=\{\[\s*\{ id:`\$\{kind\}-chronological`, events:mobileChronologicalEvents \},\s*\]\}/u,
  );
  assert.doesNotMatch(surface, /id:`\$\{kind\}-earlier`/u);
});
