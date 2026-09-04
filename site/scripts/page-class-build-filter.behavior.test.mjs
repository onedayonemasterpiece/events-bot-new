import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import {
  filterPrerenderPaths,
  FOCUSED_PREVIEW_SUPPORT_ROUTES,
  normalizeStaticSiteFocusedRoutes,
  normalizeStaticSitePageClasses,
  pageClassForComponent,
  STATIC_SITE_PAGE_CLASSES,
} from './page-class-build-filter.mjs';

test('page classes are normalized and sourced only from versioned contract', () => {
  assert.deepEqual(normalizeStaticSitePageClasses('event,date,event'), ['event', 'date']);
  assert.deepEqual(normalizeStaticSitePageClasses(''), ['all']);
  assert.throws(() => normalizeStaticSitePageClasses('all,event'), /cannot combine all/u);
  const contract = JSON.parse(readFileSync(new URL('./static-site-page-classes.v1.json', import.meta.url), 'utf8'));
  assert.deepEqual(STATIC_SITE_PAGE_CLASSES, Object.keys(contract.classes));
});

test('actual Astro route owners map to stable page classes', () => {
  assert.equal(pageClassForComponent('src/pages/[preview]/index.astro'), 'shell');
  assert.equal(pageClassForComponent('src/pages/sobytiya/[slug].astro'), 'event');
  assert.equal(pageClassForComponent('src/pages/data/discovery/[eventId].json.ts'), 'event');
  assert.equal(pageClassForComponent('src/pages/segodnya/index.astro'), 'date');
  assert.equal(pageClassForComponent('src/pages/vyhodnye/[start].astro'), 'weekend');
  assert.equal(pageClassForComponent('src/pages/podborki/[slug]/index.astro'), 'collection');
  assert.equal(pageClassForComponent('src/pages/poisk/index.astro'), 'personal');
  assert.equal(pageClassForComponent('src/pages/fokus-gruppa/index.astro'), 'focus');
  assert.equal(pageClassForComponent('src/pages/partners/index.astro'), 'partner');
  assert.equal(pageClassForComponent('src/pages/lab/design-system/index.astro'), 'lab');
  assert.equal(pageClassForComponent('src/pages/sitemap.xml.ts'), null);
});

test('focused route normalization distinguishes page routes and runtime endpoints', () => {
  assert.deepEqual(
    normalizeStaticSiteFocusedRoutes('["/segodnya","/__preview/","/data/discovery/7.json"]'),
    ['/segodnya/', '/__preview/', '/data/discovery/7.json'],
  );
});

test('exact date route drops same-class neighbours but keeps required support before prerender', () => {
  const paths = [
    { pathname: '/segodnya/', route: { component: 'src/pages/segodnya/index.astro' } },
    { pathname: '/zavtra/', route: { component: 'src/pages/zavtra/index.astro' } },
    { pathname: '/date-2026-09-04/', route: { component: 'src/pages/date-[date].astro' } },
    { pathname: '/__preview/', route: { component: 'src/pages/[preview]/index.astro' } },
    { pathname: '/robots.txt', route: { component: 'src/pages/robots.txt.ts' } },
  ];
  const focused = ['/segodnya/', '/__preview/', ...FOCUSED_PREVIEW_SUPPORT_ROUTES];
  const kept = filterPrerenderPaths(paths, ['date'], focused);
  assert.deepEqual(kept.map((entry) => entry.pathname), ['/segodnya/', '/__preview/', '/robots.txt']);
});

test('exact event route keeps only canonical detail companions and preview shell', () => {
  const paths = [
    { pathname: '/sobytiya/one/', route: { component: 'src/pages/sobytiya/[slug].astro' } },
    { pathname: '/sobytiya/two/', route: { component: 'src/pages/sobytiya/[slug].astro' } },
    { pathname: '/sobytiya/one/event.ics', route: { component: 'src/pages/sobytiya/[slug]/event.ics.ts' } },
    { pathname: '/data/discovery/7.json', route: { component: 'src/pages/data/discovery/[eventId].json.ts' } },
    { pathname: '/data/discovery/8.json', route: { component: 'src/pages/data/discovery/[eventId].json.ts' } },
    { pathname: '/__preview/', route: { component: 'src/pages/[preview]/index.astro' } },
  ];
  const focused = ['/sobytiya/one/', '/sobytiya/one/event.ics', '/data/discovery/7.json', '/__preview/'];
  assert.deepEqual(
    filterPrerenderPaths(paths, ['event'], focused).map((entry) => entry.pathname),
    ['/sobytiya/one/', '/sobytiya/one/event.ics', '/data/discovery/7.json', '/__preview/'],
  );
});

test('class-wide date selection remains class-wide', () => {
  const paths = [
    { pathname: '/segodnya/', route: { component: 'src/pages/segodnya/index.astro' } },
    { pathname: '/zavtra/', route: { component: 'src/pages/zavtra/index.astro' } },
    { pathname: '/date-2026-09-04/', route: { component: 'src/pages/date-[date].astro' } },
    { pathname: '/sobytiya/one/', route: { component: 'src/pages/sobytiya/[slug].astro' } },
    { pathname: '/__preview/', route: { component: 'src/pages/[preview]/index.astro' } },
  ];
  assert.deepEqual(
    filterPrerenderPaths(paths, ['date']).map((entry) => entry.pathname),
    ['/segodnya/', '/zavtra/', '/date-2026-09-04/', '/__preview/'],
  );
});
