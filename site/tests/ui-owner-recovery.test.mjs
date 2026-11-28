import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';

const read = (file) => readFile(new URL('../' + file, import.meta.url), 'utf8');

test('For Me opens the shared feed before account and participant tools', async () => {
  const page = await read('src/pages/dlya-menya/index.astro');
  assert.match(page, /<PersonalFeedSlot context="for-me"\s*\/>/u);
  assert.ok(page.indexOf('<PersonalFeedSlot context="for-me"') < page.indexOf('data-static-auth-login'));
  assert.ok(page.indexOf('<InterestProfile') > page.indexOf('data-focus-personal-tools'));
  assert.match(page, /data-reset-personalization/u);
  assert.match(page, /noindex/u);
});

test('For Me starts with real EventCards and does not clear them on an unavailable manifest', async () => {
  const [slot, layout] = await Promise.all([read('src/components/PersonalFeedSlot.astro'), read('src/layouts/EventLayout.astro')]);
  assert.match(slot, /isForMe \? getPopularEvents\(12\) : \[\]/u);
  assert.match(slot, /hidden=\{!isEventDetail && !isForMe\}/u);
  assert.match(slot, /<EventCard event=\{event\} variant="split-actions" mobileFlowMedia/u);
  assert.match(layout, /section\.dataset\.listingContext === 'for-me' && store\.rendered === 0 && personalFeedUsesDesktopRows\(\)/u);
  assert.match(layout, /startPersonalFeedHydration\(section, \{ eager: section\.dataset\.listingContext === 'for-me' \}\)/u);
  assert.match(layout, /else if \(!isForMe\) \{\s*section\.hidden = true;/u);
  assert.match(layout, /const EVENT_DETAIL_CONTINUATION_LIMIT = 6/u);
});

test('Today leaves its desktop date context to the existing time rail, Tomorrow/date retain it', async () => {
  assert.match(await read('src/components/listings/DateListingSurface.astro'), /dateContext=\{kind === 'today' \? undefined : \{ date: mobileDate, weekday: mobileWeekday \}\}/u);
});

test('immersive desktop detail does not reserve a second navigation header', async () => {
  const layout = await read('src/layouts/EventLayout.astro');
  assert.match(layout, /body\.hero-chrome-immersive \.site-header \{\s*position:absolute;\s*inset:0 0 auto;\s*height:auto;\s*min-height:0;\s*border:0;\s*background:transparent;/u);
});

test('single Home island wraps at intermediate widths without cloning the DOM', async () => {
  const nav = await read('src/components/HomeQuickNav.astro');
  assert.match(nav, /display: flex;\s*flex-wrap: wrap;/u);
  assert.equal((nav.match(/<nav class="home-quick-nav"/gu) || []).length, 1);
});

test('detail immersive header is not overridden by the desktop clean skin', async () => {
  const detail = await read('src/components/DesktopEventPage.astro');
  assert.match(detail, /desktop-clean-event-page:not\(\.hero-chrome-immersive\) \.site-header/u);
});

test('archive detail pages receive the same existing JSON route as current details', async () => {
  const source = await read('src/pages/data/discovery/[eventId].json.ts');
  assert.match(source, /return getEventDetailEvents\(\)\.map/u);
  assert.match(source, /buildEventDetailRelatedManifest\(event, 30\)/u);
});

test('background share asset rejection is consumed without converting image-copy to text', async () => {
  const source = await read('src/lib/service-share/controller.js');
  assert.match(source, /prepared\.pngPromise = loadAsset\(manifest\.assets\.png\)\.catch\(\(\) => null\)/u);
  assert.match(source, /throw new Error\('png_invalid'\)/u);
});

test('approved exhibition deck and frame use the existing 8px radius token', async () => {
  const source = await read('src/components/ExhibitionsPersonalSurface.astro');
  assert.match(source, /\.ex-deck \{[^}]*border-radius:var\(--ke-radius-sm\)/u);
  assert.match(source, /\.ex-deck__frame \{[^}]*border-radius:var\(--ke-radius-sm\)/u);
});
