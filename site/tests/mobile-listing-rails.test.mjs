import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const read = (relativePath) => readFile(new URL(`../${relativePath}`, import.meta.url), 'utf8');

test('accepted v23 full-viewport 112px rail is tracked on every approved mobile listing surface', async () => {
  const [surface, row, menu, dates, weekend, popular, accessory] = await Promise.all([
    read('src/components/listings/MobileListingRailSurface.astro'),
    read('src/components/listings/MobileListingRailRow.astro'),
    read('src/components/Reference4MobileMenu.astro'),
    read('src/components/listings/DateListingSurface.astro'),
    read('src/components/listings/WeekendListingSurface.astro'),
    read('src/components/listings/PopularListingSurface.astro'),
    read('src/components/listings/MobileDateAccessory.astro'),
  ]);
  assert.match(surface, /\.ke-mobile-listing-rails--v23 \.event-row\{height:112px/u);
  assert.match(surface, /\.ke-mobile-listing-rails--v23 \.rail-window\{[\s\S]*width:100vw;height:112px/u);
  assert.match(surface, /\.ke-mobile-listing-rails--v23 \.track-start\{flex:0 0 5px/u);
  assert.match(surface, /\.ke-mobile-listing-rails--v23 \.event-summary\{[\s\S]*flex:0 0 296px;width:296px;height:112px/u);
  assert.match(surface, /@media \(max-width:720px\)/u);
  for (const donorClass of ['event-row', 'rail-window', 'event-summary', 'event-media', 'event-digest', 'event-medallion-slot', 'event-like-cta']) {
    assert.match(row, new RegExp(`['"]${donorClass}(?:['"]|--)`, 'u'), donorClass);
  }
  assert.match(row, /occurrenceMode === 'per-family' \? getOccurrencePresentation\(event\) : null/u);
  assert.match(row, /resolveMobileListingRailMedia\(event, image\)/u);
  assert.match(row, /data-media-state="loading"/u);
  assert.match(surface, /mobile-rail-media-skeleton/u);
  assert.match(surface, /if \(img\.complete\) decodeLoaded\(\)/u);
  assert.match(surface, /img\.addEventListener\('error', \(\) => done\(false\)/u);
  assert.doesNotMatch(menu, /\.reference4-menu__brand::before/u);
  assert.match(dates, /<MobileListingRailSurface[\s\S]*occurrenceMode="per-date"/u);
  assert.match(dates, /calendarToday=\{getCurrentDate\(\)\}/u);
  assert.match(weekend, /<MobileListingRailSurface[\s\S]*occurrenceMode="per-date"/u);
  assert.match(weekend, /calendarToday=\{currentDate \|\| start\}/u);
  assert.match(popular, /collapseOccurrenceCards\(group\.events, 'per-family'\)/u);
  assert.match(popular, /<MobileListingRailSurface[\s\S]*occurrenceMode="per-family"/u);
  assert.match(accessory, /Array\.from\(\{ length: 42 \}/u);
  assert.match(accessory, /class="date-rail"/u);
  assert.match(accessory, /class="date-calendar-trigger"/u);
  assert.match(accessory, /class="calendar-sheet"/u);
  assert.match(accessory, /class="calendar-grid"/u);
  assert.match(accessory, /getAvailableWeekendRanges\(\)/u);
  assert.match(accessory, /aria-disabled="true"/u);
  assert.doesNotMatch(accessory, /\/date-\$\{iso\}\//u);
});

test('accepted sticky hierarchy and straight 48x23 arrow are literal contracts', async () => {
  const [surface, row] = await Promise.all([
    read('src/components/listings/MobileListingRailSurface.astro'),
    read('src/components/listings/MobileListingRailRow.astro'),
  ]);
  assert.match(surface, /class="sticky-date mobile-listing-sticky-title"/u);
  assert.match(surface, /body\.is-date-pinned/u);
  assert.match(surface, /\.group-head\{position:sticky;z-index:30;top:64px/u);
  assert.match(surface, /\[data-mobile-v23-page=popular\] \.group-head\{box-sizing:border-box;height:80px;min-height:80px;padding:21px 12px 4px\}/u);
  assert.doesNotMatch(surface, /\.feed-head\{[^}]*position:sticky/u);
  assert.match(row, /<svg viewBox="0 0 48 23"><path d="M3 11\.5H40M31 2\.5L40 11\.5L31 20\.5"><\/path><\/svg>/u);
  assert.match(surface, /\.event-cue\{[^}]*width:48px;height:23px/u);
});

test('A-tail artifact is opt-in for noindex research and hard-blocked in production', async () => {
  const [weekend, row, artifact] = await Promise.all([
    read('src/components/listings/WeekendListingSurface.astro'),
    read('src/components/listings/MobileListingRailRow.astro'),
    read('src/components/listings/AmberRailArtifact.astro'),
  ]);
  assert.match(weekend, /!IS_PRODUCTION[\s\S]*PUBLIC_ENABLE_AMBER_ARTIFACT_RESEARCH === 'tail'/u);
  assert.match(weekend, /AMBER_ARTIFACT_EVENT_ID = 6939/u);
  assert.match(weekend, /events\.find\(\(event\) => event\.start_date === start\)/u);
  assert.match(row, /<button[\s\S]*class="event-like-cta"[\s\S]*<\/button>\s*\{amberArtifactTail && <AmberRailArtifact/u);
  assert.match(artifact, /\.amber-artifact\{[^}]*flex:0 0 94px;width:94px;height:112px/u);
  assert.match(artifact, /\.amber-artifact__visual\{[^}]*width:74px;height:96px/u);
  assert.match(artifact, /entry\.intersectionRatio < \.72/u);
  assert.match(artifact, /localStorage\.setItem\(storageKey, 'found'\)/u);
  assert.doesNotMatch(artifact, /sqlite|supabase|fetch\(/iu);
});

test('search, personal, exhibitions and event continuation do not inherit the listing rail', async () => {
  const paths = [
    'src/pages/poisk/index.astro',
    'src/pages/dlya-menya/index.astro',
    'src/pages/vystavki/index.astro',
    'src/pages/sobytiya/[slug].astro',
  ];
  for (const path of paths) {
    assert.doesNotMatch(await read(path), /MobileListingRailSurface/u, path);
  }
});
