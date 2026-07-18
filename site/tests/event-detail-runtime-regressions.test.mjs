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

test('desktop telephone remains a branded reveal-and-copy CTA', async () => {
  const panel = await read('src/components/DesktopEventActionPanel.astro');
  const desktop = await read('src/components/DesktopEventPage.astro');
  const icon = await read('src/components/Icon.astro');

  assert.match(panel, /class="desktop-prototype__primary-action desktop-prototype__phone-copy"/u);
  assert.match(panel, /data-desktop-phone-copy/u);
  assert.match(panel, /<Icon name="copy" \/>/u);
  assert.match(panel, /data-desktop-phone-label>Показать телефон/u);
  assert.match(panel, /data-phone-display=\{phoneDisplay\}/u);
  assert.match(panel, /label\.textContent = phoneDisplay/u);
  assert.match(panel, /data-desktop-phone-toast[^>]*>Номер скопирован/u);
  assert.match(panel, /data-desktop-phone-status role="status" aria-live="polite"/u);
  assert.match(panel, /navigator\.clipboard\?\.writeText/u);
  assert.match(panel, /document\.execCommand\('copy'\)/u);
  assert.match(panel, /normalizedPhoneDigits/u);
  assert.match(panel, /background: var\(--clean-accent, #b54d22\)/u);
  assert.doesNotMatch(panel, /<Icon name="phone" \/>/u);
  assert.match(panel, /font-size:var\(--ke-font-size-300,1rem\)/u);
  assert.doesNotMatch(desktop, /querySelectorAll<HTMLButtonElement>\('\[data-desktop-phone-copy\]'/u);
  assert.match(icon, /name === 'copy'/u);
});

test('desktop actions keep calendar, share and like in one invariant bottom row', async () => {
  const panel = await read('src/components/DesktopEventActionPanel.astro');
  const legacyPanel = await read('src/components/EventCtaPanel.astro');
  const lab = await read('src/pages/lab/event-desktop/examples/[scenario].astro');

  assert.match(panel, /data-desktop-action-row="calendar-share-like"/u);
  const panelRow = panel.slice(panel.indexOf('data-desktop-action-row="calendar-share-like"'));
  assert.ok(panelRow.indexOf('<CalendarLink') < panelRow.indexOf('data-native-share'));
  assert.ok(panelRow.indexOf('data-native-share') < panelRow.indexOf('data-feedback-action="like"'));
  assert.match(panel, /grid-template-columns:minmax\(0,1fr\) !important/u);
  assert.match(panel, /grid-template-rows:auto auto auto !important/u);
  assert.doesNotMatch(panel, /data-primary-action-kind="phone"\] \{/u);
  assert.match(legacyPanel, /data-event-cta-action-row="calendar-share-like"/u);
  assert.match(lab, /slug: 'cta-phone-invariant', eventId: 6551/u);
  assert.match(lab, /slug: 'cta-ticket-invariant', eventId: 5374/u);
  assert.match(lab, /slug: 'editorial-ocr-companion-arrival', eventId: 4671[^\n]*transport: true, showTransport: true/u);
  assert.match(lab, /import KaupTransportSchedule/u);
  assert.match(lab, /data-lab-mobile-transport/u);
  assert.match(lab, /<KaupTransportSchedule event=\{event\} compact \/>/u);
  assert.match(lab, /@media \(max-width:1023px\)/u);
});

test('service share prompt uses the canonical inline announcements wordmark', async () => {
  const share = await read('src/components/ServiceShareAction.astro');

  assert.match(share, /import AnnouncementsWordmark from '\.\/brand\/AnnouncementsWordmark\.astro'/u);
  assert.match(share, /aria-label="Понравились Анонсы\? Поделитесь"/u);
  assert.match(share, /<span class="sr-only">Понравились Анонсы\? Поделитесь<\/span>/u);
  assert.match(share, /<AnnouncementsWordmark class="service-share-action__wordmark" \/>/u);
  assert.match(share, /width:auto/u);
  assert.match(share, /height:1em/u);
  assert.doesNotMatch(share, />Поделиться афишей</u);
});

test('footer service prototype is isolated, cohesive and does not duplicate partnership navigation', async () => {
  const component = await read('src/components/SiteFooterPrototype.astro');
  const layout = await read('src/layouts/EventLayout.astro');
  const lab = await read('src/pages/lab/event-desktop/examples/[scenario].astro');
  const secretBuilder = await read('scripts/build-secret-candidate.mjs');

  assert.match(component, /data-footer-prototype="service-v1"/u);
  assert.equal((component.match(/>Информационное партнёрство</gu) || []).length, 1);
  assert.match(component, /Пользовательское соглашение/u);
  assert.match(component, /Политика обработки персональных данных/u);
  assert.match(component, /role="link" aria-disabled="true" data-footer-future-document/u);
  assert.match(component, /showPrompt=\{false\}/u);
  assert.match(layout, /footerVariant\?: 'current' \| 'prototype-v1'/u);
  assert.match(layout, /footerVariant === 'prototype-v1'/u);
  assert.match(lab, /slug: 'footer-service-v1', eventId: 6589[^\r\n]*footerVariant: 'prototype-v1'/u);
  assert.match(secretBuilder, /const footerPrototypeRoute = 'lab\/event-desktop\/examples\/footer-service-v1'/u);
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
