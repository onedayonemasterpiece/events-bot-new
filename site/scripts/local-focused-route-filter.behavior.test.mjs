import assert from 'node:assert/strict';
import test from 'node:test';
import {
  normalizeStaticSiteFocusedEvent,
  normalizeStaticSiteRouteFamilies,
  pageClassesForFocusedSelection,
  staticSitePageClassFilterIntegration,
  validateFocusedMaterializedRoutes,
} from './page-class-build-filter.mjs';

const path = (pathname, component) => ({ pathname, route: { component } });
const fixturePaths = [
  path('/__preview/', 'src/pages/[preview]/index.astro'),
  path('/robots.txt', 'src/pages/robots.txt.ts'),
  path('/', 'src/pages/index.astro'),
  path('/segodnya/', 'src/pages/segodnya/index.astro'),
  path('/zavtra/', 'src/pages/zavtra/index.astro'),
  path('/date-2026-09-04/', 'src/pages/date-[date].astro'),
  path('/vyhodnye/', 'src/pages/vyhodnye/index.astro'),
  path('/vyhodnye/2026-09-05/', 'src/pages/vyhodnye/[start].astro'),
  path('/vystavki/', 'src/pages/vystavki/index.astro'),
  path('/festivali/', 'src/pages/festivali/index.astro'),
  path('/data/personal-feed.json/', 'src/pages/data/personal-feed.json.ts'),
  path('/sobytiya/target-event-42/', 'src/pages/sobytiya/[slug].astro'),
  path('/sobytiya/target-event-42/event.ics/', 'src/pages/sobytiya/[slug]/event.ics.ts'),
  path('/sobytiya/target-event-42/transport/train.ics/', 'src/pages/sobytiya/[slug]/transport/[schedule].ics.ts'),
  path('/data/discovery/42.json/', 'src/pages/data/discovery/[eventId].json.ts'),
  path('/sobytiya/other-event-43/', 'src/pages/sobytiya/[slug].astro'),
  path('/sobytiya/other-event-43/event.ics/', 'src/pages/sobytiya/[slug]/event.ics.ts'),
  path('/data/discovery/43.json/', 'src/pages/data/discovery/[eventId].json.ts'),
];

async function selected(pageClasses, options = {}) {
  const integration = staticSitePageClassFilterIntegration(pageClasses, options);
  assert.ok(integration);
  let wrap;
  integration.hooks['astro:build:start']({
    setPrerenderer(value) {
      wrap = value;
    },
  });
  assert.equal(typeof wrap, 'function');
  const prerenderer = wrap({
    name: 'default',
    async getStaticPaths() {
      return fixturePaths;
    },
  });
  return (await prerenderer.getStaticPaths()).map((entry) => entry.pathname);
}

test('today focused selector keeps the page, its feed dependency, and shell only', async () => {
  assert.deepEqual(pageClassesForFocusedSelection('today'), ['date', 'personal']);
  assert.deepEqual(await selected(['date', 'personal'], { routeFamilies: ['today'] }), [
    '/__preview/',
    '/robots.txt',
    '/segodnya/',
    '/data/personal-feed.json/',
  ]);
});

test('weekend focused selector keeps current and dated weekend routes', async () => {
  assert.deepEqual(await selected(['weekend', 'personal'], { routeFamilies: ['weekend'] }), [
    '/__preview/',
    '/robots.txt',
    '/vyhodnye/',
    '/vyhodnye/2026-09-05/',
    '/data/personal-feed.json/',
  ]);
});

test('exhibitions focused selector does not pull sibling collection routes', async () => {
  assert.deepEqual(await selected(['collection'], { routeFamilies: ['exhibitions'] }), [
    '/__preview/',
    '/robots.txt',
    '/vystavki/',
  ]);
});

test('one event detail keeps exact event routes and required dynamic dependencies', async () => {
  const event = normalizeStaticSiteFocusedEvent({ id: 42, slug: 'target-event-42' });
  assert.deepEqual(pageClassesForFocusedSelection([], event), ['event', 'personal']);
  assert.deepEqual(await selected(['event', 'personal'], { event }), [
    '/__preview/',
    '/robots.txt',
    '/data/personal-feed.json/',
    '/sobytiya/target-event-42/',
    '/sobytiya/target-event-42/event.ics/',
    '/sobytiya/target-event-42/transport/train.ics/',
    '/data/discovery/42.json/',
  ]);
});

test('multiple route families compose as a union without widening their page classes', async () => {
  assert.deepEqual(pageClassesForFocusedSelection(['today', 'exhibitions']), [
    'date', 'personal', 'collection',
  ]);
  assert.deepEqual(await selected(['date', 'personal', 'collection'], {
    routeFamilies: ['today', 'exhibitions'],
  }), [
    '/__preview/',
    '/robots.txt',
    '/segodnya/',
    '/vystavki/',
    '/data/personal-feed.json/',
  ]);
});

test('two named page classes preserve the existing unscoped page-class contract', async () => {
  assert.deepEqual(await selected(['date', 'weekend']), [
    '/__preview/',
    '/robots.txt',
    '/',
    '/segodnya/',
    '/zavtra/',
    '/date-2026-09-04/',
    '/vyhodnye/',
    '/vyhodnye/2026-09-05/',
  ]);
});

test('invalid or under-specified selectors fail closed', async () => {
  assert.throws(() => normalizeStaticSiteRouteFamilies('today,unknown'), /Unknown STATIC_SITE_FOCUSED_ROUTE_FAMILIES/u);
  assert.throws(() => normalizeStaticSiteFocusedEvent({ id: 0, slug: 'event' }), /positive integer/u);
  assert.throws(() => normalizeStaticSiteFocusedEvent({ id: 1, slug: '../event' }), /canonical event slug/u);
  assert.throws(
    () => staticSitePageClassFilterIntegration(['date'], { routeFamilies: ['today'] }),
    /missing required page classes: personal/u,
  );
  assert.throws(
    () => staticSitePageClassFilterIntegration(['all'], { routeFamilies: ['today'] }),
    /requires named STATIC_SITE_PAGE_CLASSES/u,
  );
  assert.throws(
    () => validateFocusedMaterializedRoutes(
      ['/__preview/', '/segodnya/', '/data/personal-feed.json/', '/vystavki/'],
      { routeFamilies: ['today'] },
    ),
    /materialized unselected routes/u,
  );
});

test('full/all keeps the canonical no-integration build path', () => {
  assert.equal(staticSitePageClassFilterIntegration(['all']), null);
});
