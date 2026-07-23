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

async function readBuiltEvent(eventId) {
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
  const eventRoot = path.join(buildRoot, 'sobytiya');
  const eventDirs = await readdir(eventRoot, { withFileTypes:true });
  const match = eventDirs.find((entry) => entry.isDirectory() && entry.name.endsWith(`-${eventId}`));
  assert.ok(match, `built event ${eventId} is missing`);
  return readFile(path.join(eventRoot, match.name, 'index.html'), 'utf8');
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
  const card = await read('src/components/EventCard.astro');
  const events = await read('src/lib/events.ts');

  assert.match(events, /image_width\?: number \| null/u);
  assert.match(events, /image_height\?: number \| null/u);
  assert.match(layout, /<EventCard event=\{runtimeTemplateEvent\} variant="split-actions" desktopRelatedCrop runtimeTemplate/u);
  assert.match(layout, /sourceCard\.cloneNode\(true\)/u);
  assert.match(layout, /'event-card__media-shell--dynamic'[\s\S]*imageUrl \? 'is-image-loading'/u);
  assert.match(layout, /--dynamic-media-ratio/u);
  assert.match(card, /const imageLoadHandler = desktopRelatedCrop[\s\S]*is-image-loaded/u);
  assert.match(card, /const imageErrorHandler = [\s\S]*is-image-missing/u);
  assert.match(card, /onload=\{imageLoadHandler\}/u);
  assert.match(card, /onerror=\{imageErrorHandler\}/u);
  assert.match(layout, /prefers-reduced-motion: reduce/u);
  assert.match(layout, /aspect-ratio: var\(--dynamic-media-ratio, 4 \/ 5\)/u);
});

test('desktop static continuation emits stable initial skeleton geometry', async () => {
  const card = await read('src/components/EventCard.astro');
  const desktop = await read('src/components/DesktopEventPage.astro');
  const built = await readBuilt('sobytiya/spektakl-garazh-kaliningrad-5658/index.html');

  assert.match(desktop, /packRelatedCardRows/u);
  assert.match(desktop, /desktopRelatedLayout=\{layout\}/u);
  assert.match(card, /--lab-row-media-ratio:/u);
  assert.match(card, /desktopRelatedCrop && 'event-card__media-shell--dynamic'/u);
  assert.match(card, /desktopRelatedCrop && 'is-image-loading'/u);
  assert.match(card, /aria-busy=\{desktopRelatedCrop \? 'true' : undefined\}/u);
  assert.match(card, /onload=\{imageLoadHandler\}/u);
  assert.match(card, /onerror=\{imageErrorHandler\}/u);
  assert.match(built, /data-lab-related-card="true"/u);
  assert.match(built, /event-card__media-shell--dynamic is-image-loading/u);
  assert.match(built, /aria-busy="true"/u);
  assert.match(built, /--lab-row-media-ratio:/u);
});

test('desktop medallion wrapper exposes venue ring and shadow without changing identity resolution', async () => {
  const desktop = await read('src/components/DesktopEventPage.astro');
  const medallions = await read('src/components/EventTokenMedallions.astro');

  assert.match(desktop, /\.desktop-prototype__medallions \{ min-height:0; overflow:visible; \}/u);
  assert.match(desktop, /\.desktop-prototype__medallions :global\(\.event-token-row\) \{ gap:\.55rem; overflow:visible;/u);
  assert.match(medallions, /resolveEventMedallions\(event, manifest\.items \|\| \[\]\)/u);
  assert.match(medallions, /data-identity-resolution=\{organizerResolution\.failClosedReason \|\| 'resolved'\}/u);
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

test('desktop action geometry follows the resolved media family', async () => {
  const panel = await read('src/components/DesktopEventActionPanel.astro');
  const desktop = await read('src/components/DesktopEventPage.astro');
  const legacyPanel = await read('src/components/EventCtaPanel.astro');
  const lab = await read('src/pages/lab/event-desktop/examples/[scenario].astro');

  assert.match(panel, /data-desktop-action-row="calendar-share-like"/u);
  const panelRow = panel.slice(panel.indexOf('data-desktop-action-row="calendar-share-like"'));
  assert.ok(panelRow.indexOf('<CalendarLink') < panelRow.indexOf('data-native-share'));
  assert.ok(panelRow.indexOf('data-native-share') < panelRow.indexOf('data-feedback-action="like"'));
  assert.match(panel, /family\?: 'split' \| 'editorial'/u);
  assert.match(panel, /data-action-family=\{family\}/u);
  assert.match(panel, /data-action-layout=\{family === 'split' \? 'inline' : 'stacked'\}/u);
  assert.match(panel, /data-action-layout="inline"[^}]*grid-template-columns:minmax\(112px,max-content\) minmax\(0,1fr\) auto !important/su);
  assert.match(panel, /data-action-layout="stacked"[^}]*grid-template-columns:minmax\(0,1fr\) !important/su);
  assert.match(panel, /data-action-layout="stacked"[^}]*grid-template-rows:auto auto auto !important/su);
  assert.match(desktop, /family="editorial"/u);
  assert.match(desktop, /family="split"/u);
  assert.match(desktop, /const splitFamily = panel\.dataset\.actionFamily === 'split'/u);
  assert.match(desktop, /if \(!splitFamily\) return/u);
  assert.match(desktop, /return !\(outside \|\| overlaps \|\| primaryLabelDoesNotFit \|\| overflows\)/u);
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

test('accepted service footer is global, cohesive and does not duplicate partnership navigation', async () => {
  const component = await read('src/components/SiteFooter.astro');
  const socialIcon = await read('src/components/SocialIcon.astro');
  const maxMetadata = JSON.parse(await read('public/assets/social/max-colored-official.svg.metadata.json'));
  const layout = await read('src/layouts/EventLayout.astro');
  const lab = await read('src/pages/lab/event-desktop/examples/[scenario].astro');
  const secretBuilder = await read('scripts/build-secret-candidate.mjs');
  const secretChecker = await read('scripts/check-secret-candidate.mjs');

  assert.match(component, /data-site-footer="service-v1"/u);
  assert.match(component, /site-footer--service-v1/u);
  assert.equal((component.match(/>Информационное партнёрство</gu) || []).length, 1);
  assert.match(component, /Пользовательское соглашение/u);
  assert.match(component, /Политика обработки персональных данных/u);
  assert.match(component, /role="link" aria-disabled="true" data-footer-future-document/u);
  assert.match(component, /showPrompt=\{false\}/u);
  assert.match(component, /<strong>Поделитесь<\/strong>/u);
  assert.match(component, /id="footer-share-title" aria-label="Понравились Анонсы\? Поделитесь"/u);
  assert.match(component, /min-height: 84px/u);
  assert.doesNotMatch(component, /min-height: 190px/u);
  assert.match(component, /min-height: 48px/u);
  assert.match(socialIcon, /import \{ withBase \} from '\.\.\/lib\/events'/u);
  assert.match(socialIcon, /withBase\('\/assets\/social\/max-colored-official\.svg'\)/u);
  assert.equal(maxMetadata.source_page, 'https://go.max.ru/brandbook');
  assert.equal(maxMetadata.provider, 'Official MAX brandbook');
  assert.match(layout, /import SiteFooter from '\.\.\/components\/SiteFooter\.astro'/u);
  assert.match(layout, /<SiteFooter socialLinks=\{FOOTER_SOCIAL_LINKS\} \/>/u);
  assert.doesNotMatch(layout, /footerVariant|SiteFooterPrototype/u);
  assert.match(lab, /slug: 'footer-service-v1', eventId: 5658, candidate: 'editorial'/u);
  const examples = JSON.parse(await read('src/data/desktop-event-examples.json'));
  assert.ok(examples.events.some((event) => event.id === 5658), 'footer specimen must use a frozen desktop fixture');
  assert.doesNotMatch(lab, /footerVariant/u);
  assert.match(secretBuilder, /const footerPrototypeRoute = 'lab\/event-desktop\/examples\/footer-service-v1'/u);
  assert.match(secretChecker, /data-site-footer="service-v1"/u);
  assert.doesNotMatch(secretChecker, /data-footer-prototype/u);
});

test('secret candidate keeps expiry-proof Split and Editorial CTA geometry fixtures', async () => {
  const lab = await read('src/pages/lab/event-desktop/examples/[scenario].astro');
  const examples = JSON.parse(await read('src/data/desktop-event-examples.json'));
  const secretBuilder = await read('scripts/build-secret-candidate.mjs');
  const secretChecker = await read('scripts/check-secret-candidate.mjs');
  const browserGate = await read('scripts/check-desktop-cta-geometry-playwright.sh');

  assert.match(lab, /slug: 'cta-phone-invariant', eventId: 6551, candidate: 'split'/u);
  assert.ok(examples.events.some((event) => event.id === 6551), 'Split CTA specimen must use a frozen desktop fixture');
  assert.match(secretBuilder, /const splitCtaRegressionRoute = 'lab\/event-desktop\/examples\/cta-phone-invariant'/u);
  assert.match(secretBuilder, /const registrationCtaRegressionRoute = 'lab\/event-desktop\/examples\/cta-registration-invariant'/u);
  assert.match(secretBuilder, /const freeCalendarCtaRegressionRoute = 'lab\/event-desktop\/examples\/cta-free-calendar-invariant'/u);
  assert.match(secretBuilder, /registrationCtaRegressionRoute,[\s\S]*freeCalendarCtaRegressionRoute/u);
  assert.match(secretChecker, /registration CTA regression marker missing/u);
  assert.match(secretChecker, /calendar-primary CTA regression marker missing/u);
  assert.match(secretChecker, /data-action-layout="inline"/u);
  assert.match(secretChecker, /data-action-layout="stacked"/u);
  assert.match(browserGate, /lab\/event-desktop\/examples\/cta-phone-invariant.*split/u);
  assert.match(browserGate, /lab\/event-desktop\/examples\/cta-registration-invariant.*split.*Зарегистрироваться/u);
  assert.match(browserGate, /lab\/event-desktop\/examples\/cta-free-calendar-invariant.*split.*В календарь/u);
  assert.match(browserGate, /lab\/event-desktop\/examples\/footer-service-v1.*editorial/u);
  assert.doesNotMatch(browserGate, /opera-i-dzhaz-znamensk-6876|myuzikl-alye-parusa-kaliningrad-4783/u);
});

test('Dramatic Theatre medallion is present in the accepted manifest', async () => {
  const manifest = JSON.parse(await read('src/data/organizerMedallions.json'));
  const item = manifest.items.find((candidate) => candidate.slug === 'dramteatr39');
  assert.ok(item, 'dramteatr39 manifest item is required');
  assert.equal(item.avatarUrl, '/assets/organizers/dramteatr39.svg');
  assert.ok(item.aliases.includes('Драматический театр'));

  const built = await readBuiltEvent(5756);
  assert.match(built, /\/assets\/organizers\/dramteatr39\.svg/u);
  assert.match(built, /Калининградский драматический театр/u);
});
