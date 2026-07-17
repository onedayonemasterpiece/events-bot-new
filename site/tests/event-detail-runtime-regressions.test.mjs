import assert from 'node:assert/strict';
import { readFile, readdir } from 'node:fs/promises';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const siteRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

async function read(relativePath) {
  return readFile(path.join(siteRoot, relativePath), 'utf8');
}

async function readBuilt(relativePath) {
  const distRoot = path.join(siteRoot, 'dist');
  const entries = await readdir(distRoot, { withFileTypes: true });
  const previewBuilds = entries
    .filter((entry) => entry.isDirectory() && entry.name.startsWith('preview-'))
    .map((entry) => entry.name)
    .sort()
    .reverse();
  const buildRoot = process.env.PREVIEW_BUILD_ID
    ? path.join(distRoot, process.env.PREVIEW_BUILD_ID)
    : previewBuilds.length > 0
      ? path.join(distRoot, previewBuilds[0])
      : distRoot;
  return readFile(path.join(buildRoot, relativePath), 'utf8');
}

test('desktop calendar label is driven by bounded use history, not event parity', async () => {
  const layout = await read('src/layouts/EventLayout.astro');
  const route = await read('src/pages/sobytiya/[slug].astro');
  const link = await read('src/components/CalendarLink.astro');
  const panel = await read('src/components/DesktopEventActionPanel.astro');

  assert.match(layout, /const CALENDAR_USAGE_KEY = 'ke_calendar_usage_v1'/u);
  assert.match(layout, /const CALENDAR_USAGE_REGULAR_THRESHOLD = 3/u);
  assert.match(layout, /const CALENDAR_USAGE_RECENT_MS = 30 \* 24 \* 60 \* 60 \* 1000/u);
  assert.match(layout, /usage\.count < CALENDAR_USAGE_REGULAR_THRESHOLD/u);
  assert.match(layout, /Date\.now\(\) - usage\.last_used_at <= CALENDAR_USAGE_RECENT_MS/u);
  assert.match(layout, /markCalendarUsed\(\);\s+syncCalendarControls\(\);\s+try/u);
  assert.match(link, /data-calendar-adaptive-label=\{compact \? '' : undefined\}/u);
  assert.match(panel, /is-calendar-label-compact/u);
  assert.doesNotMatch(route, /compactLabelAction|event\.id % 2/u);
});

test('personal continuation loads only near a visible similar-events boundary', async () => {
  const layout = await read('src/layouts/EventLayout.astro');
  const desktop = await read('src/components/DesktopEventPage.astro');
  const route = await read('src/pages/sobytiya/[slug].astro');

  assert.match(desktop, /data-related-start data-hide-sticky-after/u);
  assert.match(route, /mobile-event-production__discovery[^>]*data-hide-sticky-after/u);
  assert.match(layout, /function visibleHideStickyMarker\(\)/u);
  assert.match(layout, /marker\.getClientRects\(\)\.length > 0/u);
  assert.match(layout, /new IntersectionObserver/u);
  assert.match(layout, /rootMargin: '320px 0px 320px 0px'/u);
  assert.match(layout, /personalFeedSignalCount\(profile\) >= 3/u);
  assert.match(layout, /const PERSONAL_FEED_CHUNK_SIZE = 6/u);
  assert.match(layout, /const PERSONAL_FEED_RENDER_LIMIT = 18/u);
  assert.match(layout, /maxSameCategory: 3, maxSameVenue: 2/u);
  assert.match(layout, /\[data-related-start\] \[data-event-card\], \[data-discovery-feed\] \[data-event-card\]/u);
});

test('dynamic recommendation media reserves geometry through load and failure', async () => {
  const layout = await read('src/layouts/EventLayout.astro');
  const events = await read('src/lib/events.ts');

  assert.match(events, /image_width\?: number \| null/u);
  assert.match(events, /image_height\?: number \| null/u);
  assert.match(layout, /event-card__media-shell--dynamic is-image-loading/u);
  assert.match(layout, /--dynamic-media-ratio/u);
  assert.match(layout, /onload="[^"]*is-image-loaded/u);
  assert.match(layout, /onerror="[^"]*is-image-missing/u);
  assert.match(layout, /prefers-reduced-motion: reduce/u);
  assert.match(layout, /aspect-ratio: var\(--dynamic-media-ratio, 4 \/ 5\)/u);
});

test('desktop telephone uses the shared non-shifting copy action', async () => {
  const panel = await read('src/components/DesktopEventActionPanel.astro');
  const desktop = await read('src/components/DesktopEventPage.astro');
  const copyAction = await read('src/components/design-system/CopyAction.astro');
  const icon = await read('src/components/Icon.astro');

  assert.match(panel, /<CopyAction[^>]*variant="inverse"/u);
  assert.match(panel, /data-desktop-phone-number/u);
  assert.doesNotMatch(panel, /<Icon name="phone" \/><span><strong>\{phoneDisplay\}/u);
  assert.doesNotMatch(panel, />Скопировать номер<\/small>/u);
  assert.match(panel, /font-size:var\(--ke-font-size-300,1rem\)/u);
  assert.doesNotMatch(desktop, /querySelectorAll<HTMLButtonElement>\('\[data-desktop-phone-copy\]'/u);
  assert.match(copyAction, /data-ke-copy-action/u);
  assert.match(copyAction, /navigator\.clipboard\?\.writeText/u);
  assert.match(copyAction, /document\.execCommand\('copy'\)/u);
  assert.match(copyAction, /data-ke-copy-status role="status" aria-live="polite"/u);
  assert.match(copyAction, /max\(44px, var\(--ke-control-min, 44px\)\)/u);
  assert.match(copyAction, /Icon name="copy"/u);
  assert.match(copyAction, /Icon name="check"/u);
  assert.match(icon, /name === 'copy'/u);
  assert.match(icon, /name === 'check'/u);
});

test('Dramatic Theatre medallion is present in the accepted manifest', async () => {
  const manifest = JSON.parse(await read('src/data/organizerMedallions.json'));
  const item = manifest.items.find((candidate) => candidate.slug === 'dramteatr39');
  assert.ok(item, 'dramteatr39 manifest item is required');
  assert.equal(item.avatarUrl, '/assets/organizers/dramteatr39.svg');
  assert.ok(item.aliases.includes('Драматический театр'));

  const built = await readBuilt('sobytiya/zhenitba-i-ekskursiya-zakulise-teatra-kaliningrad-5756/index.html');
  assert.match(built, /\/assets\/organizers\/dramteatr39\.svg/u);
  assert.match(built, /Калининградский драматический театр/u);
});
