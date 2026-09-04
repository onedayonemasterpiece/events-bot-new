import assert from 'node:assert/strict';
import { access, readFile, readdir } from 'node:fs/promises';
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
  assert.match(layout, /entry\.isIntersecting \|\| entry\.boundingClientRect\.top < 0/u);
});

test('mobile event actions preserve a readable wide Share label', async () => {
  const mobileStyles = await read('src/components/MobileEventProductionStyles.astro');
  assert.match(mobileStyles, /\.secondary-button:not\(\[data-native-share\]\) > span:not\(\.feedback-count\) \{ display: none; \}/u);
  assert.match(mobileStyles, /> \[data-native-share\] \{[\s\S]*flex:1 1 9rem;[\s\S]*min-width:8\.5rem;/u);
  assert.match(mobileStyles, /> \[data-native-share\] \[data-share-label\] \{ display:inline; \}/u);
  assert.doesNotMatch(mobileStyles, /\.event-hero__actions > \.secondary-button > span:not\(\.feedback-count\) \{ display: none; \}/u);
});

test('free admission uses the dedicated inline medallion on mobile and desktop', async () => {
  const medallions = await read('src/components/EventTokenMedallions.astro');
  assert.match(medallions, /kind: 'badge',[\s\S]*key: 'free-admission'[\s\S]*imageUrl: '\/assets\/badges\/free-listing-medallion\.svg'/u);
  assert.match(medallions, /ariaLabel: 'Бесплатное событие: 0 рублей'/u);
  assert.match(medallions, /admissionBadge && !tokens\.slice\(0, 6\)\.includes\(admissionBadge\)/u);
  assert.match(medallions, /const desktopMedallionTokens = visibleTokens\.filter\(\(token\) => \([\s\S]*token\.kind === 'organizer'[\s\S]*token\.kind === 'source'[\s\S]*token\.kind === 'pushkin'/u);
  assert.match(medallions, /desktopMedallionTokens[\s\S]{0,260}token\.kind === 'badge'/u);
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
  assert.match(card, /const imageLoadHandler = [\s\S]*is-image-loaded/u);
  assert.match(card, /const imageErrorHandler = [\s\S]*is-image-missing/u);
  assert.match(card, /onload=\{imageLoadHandler\}/u);
  assert.match(card, /onerror=\{imageErrorHandler\}/u);
  assert.match(layout, /prefers-reduced-motion: reduce/u);
  assert.match(layout, /aspect-ratio: var\(--dynamic-media-ratio, 4 \/ 5\)/u);
});

test('desktop static continuation emits stable initial skeleton geometry', async () => {
  const card = await read('src/components/EventCard.astro');
  const desktop = await read('src/components/DesktopEventPage.astro');
  const optimizedGrid = await read('src/components/OptimizedEventCardGrid.astro');
  const adaptiveGrid = await read('src/components/AdaptiveEventCardGrid.astro');
  const built = await readBuilt('sobytiya/spektakl-garazh-kaliningrad-5658/index.html');

  assert.match(desktop, /<OptimizedEventCardGrid/u);
  assert.match(optimizedGrid, /import AdaptiveEventCardGrid from '\.\/AdaptiveEventCardGrid\.astro'/u);
  assert.match(optimizedGrid, /<AdaptiveEventCardGrid/u);
  assert.doesNotMatch(optimizedGrid, /packRelatedCardRows/u);
  assert.doesNotMatch(optimizedGrid, /desktopRelatedLayout=\{layout\}/u);
  assert.doesNotMatch(optimizedGrid, /<style>/u);
  assert.match(adaptiveGrid, /packRelatedCardRows\(events/u);
  assert.match(adaptiveGrid, /desktopRelatedLayout=\{mode === 'packed' \? layout : undefined\}/u);
  assert.match(card, /--lab-row-media-ratio:/u);
  assert.match(card, /\(desktopRelatedCrop \|\| mobileFlowMedia\) && 'event-card__media-shell--dynamic'/u);
  assert.match(card, /'is-image-loading'/u);
  assert.match(card, /aria-busy="true"/u);
  assert.match(card, /onload=\{imageLoadHandler\}/u);
  assert.match(card, /onerror=\{imageErrorHandler\}/u);
  assert.match(built, /data-lab-related-card="true"/u);
  assert.match(built, /data-optimized-event-card-grid/u);
  assert.match(built, /data-adaptive-event-card-grid/u);
  assert.match(built, /event-card__media-shell--dynamic is-image-loading/u);
  assert.match(built, /aria-busy="true"/u);
  assert.match(built, /--lab-row-media-ratio:/u);
});

test('desktop medallion wrapper exposes venue ring and shadow without changing identity resolution', async () => {
  const desktop = await read('src/components/DesktopEventPage.astro');
  const medallions = await read('src/components/EventTokenMedallions.astro');

  assert.match(desktop, /\.desktop-prototype__medallions \{ min-height:0; overflow:visible; \}/u);
  assert.match(desktop, /\.desktop-prototype__medallions :global\(\.event-token-row\) \{ gap:\.55rem; overflow:visible;/u);
  assert.match(medallions, /resolveEventMedallions\(event, \[\.\.\.organizerItems, \.\.\.eventPageFestivalItems\]\)/u);
  assert.match(medallions, /data-identity-resolution=\{organizerResolution\.failClosedReason \|\| 'resolved'\}/u);
});

test('desktop event chrome renders explicit Main and Secondary medallion slots', async () => {
  const desktop = await read('src/components/DesktopEventPage.astro');
  const medallions = await read('src/components/EventTokenMedallions.astro');
  const resolver = await read('src/lib/eventMedallions.ts');

  assert.match(desktop, /<EventTokenMedallions event=\{event\} layout="desktop-slots" allowTopSlot=\{candidate === 'editorial' && mediaPolicy === 'non-ocr'\} \/>/u);
  assert.match(medallions, /class:list=\{\['event-token-section', `event-token-section--\$\{group\.slot\}`\]\}/u);
  assert.match(medallions, /data-medallion-slot=\{group\.slot\}/u);
  assert.match(medallions, /data-medallion-role=\{token\.layoutRole \|\| 'secondary'\}/u);
  assert.match(medallions, /data-main-medallion-slug=\{resolvedMainToken\?\.slug\}/u);
  assert.match(medallions, /const desktopMedallionTokens = visibleTokens\.filter\(\(token\) => \([\s\S]*token\.kind === 'organizer'[\s\S]*token\.kind === 'source'[\s\S]*token\.kind === 'pushkin'/u);
  assert.match(medallions, /desktopMedallionTokens[\s\S]{0,260}token\.kind === 'badge'/u);
  assert.match(medallions, /const mainToken = allowTopSlot \? resolvedMainToken : undefined/u);
  assert.match(resolver, /export function classifyEventMedallionLayout/u);
  assert.match(resolver, /if \(category === 'festival_brand'\) return 400/u);
  assert.match(resolver, /if \(category === 'organizer'\) return 300/u);
  assert.match(desktop, /data-medallion-slot="top"[\s\S]*position:absolute/u);
  assert.match(desktop, /left:50%;[\s\S]*transform:translate\(-50%,-50%\)/u);
  assert.match(desktop, /\.desktop-prototype__info:has\(\[data-medallion-slot="top"\]\)/u);
});

test('desktop breadcrumbs keep semantics while becoming compact and secondary', async () => {
  const desktop = await read('src/components/DesktopEventPage.astro');
  const breadcrumbs = await read('src/components/Breadcrumbs.astro');

  assert.match(breadcrumbs, /aria-label="Хлебные крошки"/u);
  assert.match(breadcrumbs, /<ol>/u);
  assert.match(breadcrumbs, /aria-current="page"/u);
  assert.match(desktop, /desktop-prototype__breadcrumbs\.product-breadcrumbs a\) \{[\s\S]*min-height:26px !important;/u);
  assert.match(desktop, /desktop-prototype__breadcrumbs\.product-breadcrumbs a::before\) \{[\s\S]*inset:-9px -6px;/u);
  assert.match(desktop, /product-breadcrumbs:not\(\.desktop-prototype__breadcrumbs--overlay\)[\s\S]*font-size:\.7rem/u);
  assert.match(desktop, /\.desktop-clean-event--split \.desktop-prototype__info \{[\s\S]*padding-top:clamp\(\.55rem,.9vw,.85rem\)/u);
  assert.match(desktop, /max-width:min\(330px,calc\(\(100vw - var\(--editorial-side\) - 6vw\) \/ 2 - 5\.75rem\)\)/u);
});

test('desktop and mobile transport consume one persisted Smart Update duration forecast', async () => {
  const route = await read('src/pages/sobytiya/[slug].astro');
  const medallions = await read('src/components/EventTokenMedallions.astro');
  const built = await readBuiltEvent(6529);

  assert.match(route, /const transportEvent = desktopEventWithExplicitEnd\(event\)/u);
  assert.match(route, /<EventTransportSchedule event=\{transportEvent\} \/>/u);
  assert.match(route, /<EventBusTransportSchedule event=\{transportEvent\} \/>/u);
  assert.match(route, /<KaupTransportSchedule event=\{transportEvent\} compact \/>/u);
  assert.equal((built.match(/data-event-end-basis="forecast"/gu) || []).length, 2);
  assert.doesNotMatch(built, /data-event-end-basis="schedule_cutoff"/u);
  assert.match(medallions, /getEventTransportSuggestion\(desktopEventWithExplicitEnd\(event\)\)/u);
  assert.match(medallions, /kind:'program',[\s\S]*key:`transport:\$\{railTransport\.slug\}`[\s\S]*layoutRole:'secondary'/u);
  assert.equal((built.match(/rzd-lastochka-medallion\.webp/gu) || []).length, 2);
  assert.match(built, /data-medallion-slot="inline"[\s\S]*event-token--program event-token--custom event-token--rzd-lastochka[\s\S]*data-medallion-role="secondary"/u);
  for (const topSlot of built.matchAll(/<section[^>]*data-medallion-slot="top"[\s\S]*?<\/section>/gu)) {
    assert.doesNotMatch(topSlot[0], /rzd-lastochka/u);
  }
});

test('event 7018 retains its exact curated Ruin Keepers listing contract after its dated route ages out', async () => {
  const preview = JSON.parse(await read('src/data/preview-events.json'));
  const catalog = JSON.parse(await read('src/data/organizerMedallions.json'));
  const listingCard = await read('src/components/listings/ListingEventCard.astro');
  const event = preview.events.find((item) => item.id === 7018);
  const medallion = catalog.items.find((item) => item.slug === 'ruin-keepers');

  assert.ok(event, 'event 7018 must remain in the preview source projection');
  assert.equal(event.title, 'Воскресник в Озёрске');
  assert.equal(event.city, 'Озёрск');
  assert.equal(event.venue_name, 'центр «Крупорушка»');
  assert.ok(medallion, 'Ruin Keepers must remain in the accepted medallion catalog');
  assert.equal(medallion.ariaLabel, 'Организатор: Хранители руин');
  assert.equal(medallion.avatarUrl, '/assets/organizers/ruin-keepers.webp');
  assert.match(listingCard, /getListingIdentityMedallions\(event\)/u);
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
  assert.match(panel, /type DesktopActionVariant = 'editorial-side' \| 'split-inline' \| 'editorial-flow' \| 'split-flow'/u);
  assert.match(panel, /type DesktopActionState = 'non-ocr' \| 'ocr'/u);
  assert.match(panel, /data-action-family=\{family\}/u);
  assert.match(panel, /data-action-layout=\{family === 'split' \? 'inline' : 'stacked'\}/u);
  assert.match(panel, /data-action-layout="inline"[^}]*grid-template-columns:minmax\(112px,max-content\) minmax\(0,1fr\) auto !important/su);
  assert.match(panel, /data-action-layout="stacked"[^}]*grid-template-columns:minmax\(0,1fr\) !important/su);
  assert.match(panel, /data-action-layout="stacked"[^}]*grid-template-rows:auto auto auto !important/su);
  assert.match(desktop, /variant="editorial-side" state=\{mediaPolicy\}/u);
  assert.match(desktop, /variant="split-inline" state=\{mediaPolicy\}/u);
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

test('accepted service footer is global, cohesive and exposes Partners separately from the partnership CTA', async () => {
  const component = await read('src/components/SiteFooter.astro');
  const socialIcon = await read('src/components/SocialIcon.astro');
  const maxMetadata = JSON.parse(await read('public/assets/social/max-colored-official.svg.metadata.json'));
  const layout = await read('src/layouts/EventLayout.astro');
  const lab = await read('src/pages/lab/event-desktop/examples/[scenario].astro');
  const secretBuilder = await read('scripts/build-secret-candidate.mjs');
  const secretChecker = await read('scripts/check-secret-candidate.mjs');

  assert.match(component, /data-site-footer="service-v1"/u);
  assert.match(component, /site-footer--service-v1/u);
  assert.equal((component.match(/>Партнёры</gu) || []).length, 1);
  assert.equal((component.match(/>Стать партнёром</gu) || []).length, 1);
  assert.match(component, /Пользовательское соглашение/u);
  assert.match(component, /Политика обработки персональных данных/u);
  assert.match(component, /role="link" aria-disabled="true" data-footer-future-document/u);
  assert.match(component, /showPrompt=\{false\}/u);
  assert.match(component, /<strong>Поделитесь<\/strong>/u);
  assert.match(component, /id="footer-share-title" aria-label="Понравились Анонсы\? Поделитесь"/u);
  assert.match(component, /min-height: var\(--ke-footer-share-min-height\)/u);
  assert.doesNotMatch(component, /min-height: 190px/u);
  assert.match(component, /min-height: var\(--ke-footer-share-action-min-height\)/u);
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
  assert.match(browserGate, /lab\/event-desktop\/examples\/cta-free-calendar-invariant.*split.*Добавить в календарь/u);
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

test('complete recovered medallion inventory stays in the manifests and lab', async () => {
  const organizerManifest = JSON.parse(await read('src/data/organizerMedallions.json'));
  const festivalManifest = JSON.parse(await read('src/data/festivalMedallions.json'));
  const organizerSlugs = new Set(organizerManifest.items.map((item) => item.slug));
  const festivalSlugs = new Set(festivalManifest.items.map((item) => item.slug));

  assert.equal(organizerManifest.items.length, 28);
  assert.equal(festivalManifest.items.length, 11);
  for (const slug of ['mumod', 'greza-khutor', 'yantar-hall', 'ruin-keepers']) {
    assert.ok(organizerSlugs.has(slug), `${slug} must remain in the organizer inventory`);
  }
  for (const slug of ['kaliningrad-city-jazz', 'more-vnutri', 'tolkin-fest', 'kaup']) {
    assert.ok(festivalSlugs.has(slug), `${slug} must remain in the festival inventory`);
  }
  for (const item of [...organizerManifest.items, ...festivalManifest.items]) {
    await access(path.join(siteRoot, 'public', item.avatarUrl));
    if (item.fallbackPngUrl) await access(path.join(siteRoot, 'public', item.fallbackPngUrl));
  }

  const built = await readBuilt('lab/medallions/index.html');
  assert.match(built, /Организаторы и площадки — 28/u);
  assert.match(built, /Музей курортной моды/u);
  assert.match(built, /\/assets\/organizers\/mumod\.svg/u);
  assert.match(built, /Фестивали — 10/u);
  assert.match(built, /\/assets\/badges\/free-listing-medallion\.svg/u);
});
