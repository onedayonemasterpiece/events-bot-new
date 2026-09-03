import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import {
  normalizeStaticSitePageClasses,
  pageClassForComponent,
  STATIC_SITE_PAGE_CLASSES,
} from './page-class-build-filter.mjs';

test('page classes are normalized and all cannot be combined', () => {
  assert.deepEqual(normalizeStaticSitePageClasses('event,date,event'), ['event', 'date']);
  assert.deepEqual(normalizeStaticSitePageClasses(''), ['all']);
  assert.throws(() => normalizeStaticSitePageClasses('all,event'), /cannot combine all/u);
  assert.throws(() => normalizeStaticSitePageClasses('unknown'), /Unknown STATIC_SITE_PAGE_CLASSES/u);
  assert.deepEqual(STATIC_SITE_PAGE_CLASSES, [
    'event', 'date', 'weekend', 'collection', 'personal', 'focus', 'partner', 'lab',
  ]);
});

test('the selector reads the one versioned page-class contract', () => {
  const contract = JSON.parse(readFileSync(
    new URL('./static-site-page-classes.v1.json', import.meta.url),
    'utf8',
  ));
  assert.equal(contract.schema_version, 'kenigevents_static_site_page_classes_v1');
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
