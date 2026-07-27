import assert from 'node:assert/strict';
import { access, readFile } from 'node:fs/promises';
import test from 'node:test';

const paths = {
  bge: '../scripts/static_event_bge.py',
  semantics: '../scripts/unusual_event_semantics.py',
  prototypes: '../scripts/unusual_event_prototypes.v1.json',
  classifier: '../scripts/unusual_event_classifier.v1.json',
  unusualLib: '../src/lib/unusualEvents.ts',
  unusualManifest: '../src/lib/unusualManifest.mjs',
  unusualPage: '../src/pages/neobychnoe/index.astro',
  unusualSurface: '../src/components/UnusualListingSurface.astro',
  unread: '../src/components/UnusualUnreadRuntime.astro',
  menu: '../src/components/Reference4MobileMenu.astro',
  footer: '../src/components/SiteFooter.astro',
  free: '../src/components/FreeCollectionSurface.astro',
  availability: '../src/lib/eventDateAvailability.ts',
  calendar: '../src/components/listings/MobileDateAccessory.astro',
  rail: '../src/lib/mobileListingRailMedia.mjs',
  favoritesPage: '../src/pages/izbrannoe/index.astro',
  favorites: '../src/components/FavoritesSurface.astro',
  favoritesLib: '../src/lib/favorites.mjs',
  home: '../src/pages/index.astro',
  homeFeed: '../src/components/HomeColdStartFeed.astro',
  builder: '../../kaggle/StaticSiteBuilder/static_site_builder.py',
  runner: '../../scripts/run_static_site_builder_kaggle.py',
  exporter: '../scripts/export-production-preview-data.py',
  fixture: '../../tests/fixtures/unusual_events_golden_v1.json',
};

const missing = [];
for (const [name, relative] of Object.entries(paths)) {
  try { await access(new URL(relative, import.meta.url)); }
  catch { missing.push(name); }
}
const isolatedDocsLane = ['bge', 'unusualPage', 'favoritesPage'].every((name) => missing.includes(name));
const integrationSkip = isolatedDocsLane ? `awaiting R15 integration files: ${missing.join(', ')}` : false;
const read = (name) => readFile(new URL(paths[name], import.meta.url), 'utf8');

const expectedFamilies = [
  'open_dialogue', 'participatory', 'co_creation', 'behind_scenes',
  'restricted_access', 'site_specific', 'after_hours', 'hybrid_format',
  'living_history', 'field_science', 'rare_practice', 'gastro_experience',
  'sensory_wellbeing', 'community_exchange', 'quirky_ritual',
];

test('R08 uses one hash-bound BGE vector boundary and the exact 15-family bank', { skip: integrationSkip }, async () => {
  const [bge, semantics, prototypeText, classifierText, fixtureText] = await Promise.all([
    read('bge'), read('semantics'), read('prototypes'), read('classifier'), read('fixture'),
  ]);
  const prototypes = JSON.parse(prototypeText);
  const classifier = JSON.parse(classifierText);
  const fixture = JSON.parse(fixtureText);

  assert.match(bge, /from scripts\.sync_event_search_vectors_to_supabase import[\s\S]*build_related_digest/u);
  assert.match(bge, /MODEL_ID\s*=\s*"BAAI\/bge-m3"/u);
  assert.match(bge, /MODEL_REVISION\s*=\s*"5617a9f61b028005a4858fdac845db406aefb181"/u);
  assert.match(bge, /EMBEDDING_DIM\s*=\s*1024/u);
  assert.equal((bge.match(/BGEM3FlagModel/gu) || []).length, 2, 'one import and one construction at the shared boundary');
  assert.match(bge, /"provider_calls":\s*0/u);
  assert.doesNotMatch(semantics, /FlagEmbedding|BGEM3FlagModel|snapshot_download|(?:model|encoder)\.encode\(/u);
  assert.match(semantics, /classifier_sha256 mismatch|classifier_sha256/u);

  assert.deepEqual(prototypes.families.map((family) => family.id), expectedFamilies);
  assert.deepEqual(fixture.taxonomy.families, expectedFamilies);
  for (const family of expectedFamilies) {
    const kinds = new Set(prototypes.prototypes.filter((item) => item.family === family).map((item) => item.kind));
    assert.deepEqual(kinds, new Set(['positive', 'hard_negative']), family);
  }
  assert.equal(classifier.calibration.approval_status, 'not_approved');
  assert.equal(fixture.canary_evidence, 'pending');
});

test('R01-R07 keep the cross-surface product contracts in canonical implementations', { skip: integrationSkip }, async () => {
  const [rail, free, availability, calendar, menu, favoritesPage, favorites, favoritesLib, home, homeFeed] = await Promise.all([
    read('rail'), read('free'), read('availability'), read('calendar'), read('menu'),
    read('favoritesPage'), read('favorites'), read('favoritesLib'), read('home'), read('homeFeed'),
  ]);

  assert.match(rail, /fit:\s*'cover',[\s\S]*ratio:\s*5\s*\/\s*4,[\s\S]*width:\s*140,[\s\S]*single_safe_visual_landscape_5x4/u);
  assert.match(free, /data-free-collection-medallion="large"/u);
  assert.match(free, /data-free-collection-medallion="compact"/u);
  assert.match(free, /data-free-collection-shelf/u);

  assert.match(availability, /furthestEventDate/u);
  assert.match(availability, /horizonEnd\s*=\s*endOfMonth\(furthestEventDate\)/u);
  assert.match(calendar, /data-calendar-horizon/u);
  assert.match(calendar, /const href:[\s\S]*!hasEvents[\s\S]*\?\s*null/u);
  assert.match(calendar, /aria-disabled="true"/u);

  assert.match(menu, /data-reference4-collections-open/u);
  assert.match(menu, /aria-label="Подборки"/u);
  for (const label of ['Детям', 'Необычное', 'Бесплатно', 'Клубы по интересам']) {
    assert.match(menu, new RegExp(`>${label}<`, 'u'));
  }
  assert.ok((menu.match(/>Бесплатно</gu) || []).length >= 2, 'Free remains top-level and in Collections');

  assert.match(favoritesPage, /\bnoindex\b/u);
  assert.match(favorites, /data-favorites-skeleton/u);
  assert.match(favoritesLib, /sourcePriority:\s*item\.calendarSaved\s*\?\s*0\s*:\s*1/u);
  assert.match(favoritesLib, /source:\s*item\.calendarSaved\s*\?\s*'calendar'\s*:\s*\(item\.favoriteSaved\s*\?\s*'favorite'\s*:\s*'like'\)/u);
  assert.match(favorites, /dataset\.savedSource\s*=\s*entry\.saved\.source/u);
  assert.match(favorites, /dataset\.savedRank\s*=\s*String\(index\)/u);

  assert.match(home, /data-home-page/u);
  assert.match(home, /HomeHeroTalk/u);
  assert.match(home, /HomeQuickNav/u);
  assert.match(home, /HomeColdStartFeed/u);
  assert.match(homeFeed, /data-home-feed-limit="30"/u);
  assert.match(homeFeed, /events\.slice\(0,\s*30\)/u);
  assert.doesNotMatch(homeFeed, /\bfetch\s*\(|gemini|embedding/iu);
});

test('R09-R10 fail closed around provider calls, migration and concept unread identity', { skip: integrationSkip }, async () => {
  const [builder, runner, exporter, unusualLib, unusualManifest, unusualPage, surface, unread, menu, footer] = await Promise.all([
    read('builder'), read('runner'), read('exporter'), read('unusualLib'), read('unusualManifest'), read('unusualPage'),
    read('unusualSurface'), read('unread'), read('menu'), read('footer'),
  ]);
  const buildSources = `${builder}\n${runner}\n${exporter}`;

  assert.match(buildSources, /provider_calls/iu);
  assert.match(buildSources, /unusual_events_cache/u);
  assert.match(buildSources, /unusual_events_last_good/u);
  assert.match(buildSources, /static_event_bge_vectors/u);
  assert.match(buildSources, /notify_eligible/iu);
  assert.match(buildSources, /migration/iu);
  assert.match(buildSources, /os\.replace|replace\(/u);

  assert.match(unusualPage, /\/neobychnoe\//u);
  assert.match(unusualPage, /\bnoindex\b/u);
  assert.match(unusualLib, /import unusualData from '\.\.\/data\/unusual-events\.json'/u);
  assert.match(unusualLib, /resolveUnusualFeed as resolveFeed/u);
  assert.match(surface, /data-unusual-feed-ready="true"/u);
  assert.match(surface, /data-unusual-feed-empty/u);
  assert.match(unusualManifest, /quality_gate\.status\s*!==\s*'approved'/u);
  assert.match(unusualManifest, /concepts\.has\(item\.concept_id\)/u);
  assert.match(unusualManifest, /item\.notifyEligible/u);
  assert.match(unread, /ke_unusual_seen_v1/u);
  assert.match(unread, /kenigevents:unusual-viewed/u);
  assert.match(menu, /data-unusual-nav-dot/u);
  assert.match(footer, /data-unusual-nav-dot/u);
});
