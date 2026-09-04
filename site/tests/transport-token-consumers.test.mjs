import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const root = new URL('../', import.meta.url);
const foundationPath = new URL('../src/components/design-system/transport-foundations.css', import.meta.url);
const sources = [
  'src/components/EventTransportSchedule.astro',
  'src/components/EventBusTransportSchedule.astro',
  'src/components/KaupTransportSchedule.astro',
  'src/components/transport/DepartureBoardTimetable.astro',
  'src/components/transport/NextDepartureQueueTimetable.astro',
  'src/components/transport/RouteStripsTimetable.astro',
  'src/components/transport/TransportJourneyAlerts.astro',
  'src/components/transport/TransportRouteHeading.astro',
];

const documentedRawExceptions = new Map([
  ['src/components/EventTransportSchedule.astro', new Set(['#78342f', 'rgba(151,53,46,0.08)'])],
  ['src/components/KaupTransportSchedule.astro', new Set(['#a54821', 'rgba(255,255,255,.38)'])],
  ['src/components/transport/DepartureBoardTimetable.astro', new Set(['#6a7471', '#67716e'])],
  ['src/components/transport/NextDepartureQueueTimetable.astro', new Set(['#65706d'])],
]);

async function source(path) {
  return readFile(new URL(path, root), 'utf8');
}

test('production transport presentation consumes the canonical transport foundation roles', async () => {
  const foundations = await readFile(foundationPath, 'utf8');
  for (const path of sources) {
    const text = await source(path);
    const roles = [...text.matchAll(/var\((--ke-(?:color|elevation)-(?:train|bus|kaup|transport)-[\w-]+)/gu)]
      .map((match) => match[1]);
    assert.ok(roles.length, `${path} should consume a canonical transport role`);
    for (const role of roles) {
      assert.match(foundations, new RegExp(`${role.replace(/[.*+?^${}()|[\]\\]/gu, '\\$&')}\\s*:`), `${path} uses defined ${role}`);
    }
  }
});

test('transport consumers retain only documented non-equivalent raw colour exceptions', async () => {
  for (const path of sources) {
    const text = await source(path);
    const rawColours = [
      ...text.matchAll(/#[0-9a-fA-F]{3,8}\b|rgba?\([^)]*\)/gu),
    ].map((match) => match[0]);
    assert.deepEqual(
      new Set(rawColours),
      documentedRawExceptions.get(path) || new Set(),
      `${path} has an undocumented raw colour instead of an exact transport role`,
    );
  }
});
