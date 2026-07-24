import { existsSync, readFileSync, readdirSync, statSync } from 'node:fs';
import { join, resolve } from 'node:path';
import eventsData from '../src/data/preview-events.json' with { type: 'json' };
import relatedData from '../src/data/preview-related.json' with { type: 'json' };
import interestClubsData from '../src/data/interest-clubs.json' with { type: 'json' };
import busData from '../src/data/busTransportSchedules.json' with { type: 'json' };
import templateContract from '../src/data/eventTemplateContract.json' with { type: 'json' };

const siteDir = resolve(new URL('..', import.meta.url).pathname);
const distDir = join(siteDir, 'dist');
const buildId = process.env.PREVIEW_BUILD_ID || readdirSync(distDir).find((name) => name.startsWith('preview-'));
if (!buildId) throw new Error('No preview-* folder found in dist');
const root = join(distDir, buildId);
const required = [
  '__preview/index.html',
  'segodnya/index.html',
  'zavtra/index.html',
  'vyhodnye/index.html',
  'vystavki/index.html',
  'populyarnoe/index.html',
  'poisk/index.html',
  'partnerstvo/index.html',
  'partners/index.html',
  'kluby-po-interesam/index.html',
  'sitemap.xml',
  'robots.txt',
  'favicon.svg',
  'assets/transport/kppk-lastochka.webp',
  'assets/transport/bus-svgrepo-337651.svg',
  'assets/transport/romanovo-holmogorye-route-square.png',
  'assets/transport/romanovo-holmogorye-route-portrait.png',
  'brand/announcements-wordmark-ui.svg',
  'assets/icons/link-minimalistic-svgrepo.svg',
  'service-share/current/manifest.json',
  'service-share/versions/20260715-896b8af26ac6679f/manifest.json',
  'service-share/versions/20260715-896b8af26ac6679f/service-share-20260715-896b8af26ac6679f.png',
  'service-share/versions/20260715-896b8af26ac6679f/service-share-20260715-896b8af26ac6679f.webp',
  'preview-build.json',
  'lab/hero/index.html',
  'lab/hero/review/index.html',
  'lab/hero/review/5878-poster-billboard/index.html',
  'lab/hero/review/5878-poster-attached-card/index.html',
  'lab/hero/review/6322-photo-parallax-sheet/index.html',
  'lab/occurrences/index.html',
  ...templateContract.lab_scenarios.map((scenario) => `lab/event-desktop/examples/${scenario}/index.html`),
  'lab/event-mobile/index.html',
  ...['control', 'open-prose', 'action-dock', 'open-prose-action-dock', 'accepted-v2', 'accepted-v3', 'accepted-v4', 'accepted-v5', 'accepted-v6', 'accepted-v7', 'accepted-v8'].flatMap((variant) =>
    ['photo-paid', 'visual-free', 'ocr-poster'].map((scenario) =>
      `lab/event-mobile/examples/${variant}/${scenario}/index.html`,
    ),
  ),
  ...eventsData.events.flatMap((event) => [
    `sobytiya/${event.slug}/index.html`,
    `sobytiya/${event.slug}/event.ics`,
    `data/discovery/${event.id}.json`,
  ]),
  ...interestClubsData.clubs.map((club) => `kluby-po-interesam/${club.slug}/index.html`),
];
for (const rel of required) {
  const path = join(root, rel);
  if (!existsSync(path) || !statSync(path).isFile()) throw new Error(`Missing required file: ${rel}`);
}
const occurrenceLabHtml = readFileSync(join(root, 'lab/occurrences/index.html'), 'utf8');
for (const compactLabel of ['2, 9 ноября 19:00', '4 ноября 17:00, 19:00']) {
  if (!occurrenceLabHtml.includes(compactLabel)) throw new Error(`Occurrence lab misses compact label: ${compactLabel}`);
}
for (const variant of ['inline', 'rail-time-first', 'rail-date-first']) {
  if (!occurrenceLabHtml.includes(`data-occurrence-label-variant="${variant}"`)) throw new Error(`Occurrence lab misses shared label variant: ${variant}`);
}
if (!occurrenceLabHtml.includes('data-occurrence-variant="mobile"')) throw new Error('Occurrence lab misses the accepted always-visible mobile selector');
if (occurrenceLabHtml.includes('Также:')) throw new Error('Occurrence lab must not render the rejected legacy other-times copy');

const listingRoutes = ['segodnya', 'zavtra', 'vyhodnye', 'populyarnoe'];
for (const route of listingRoutes) {
  const html = readFileSync(join(root, route, 'index.html'), 'utf8');
  const stylesheetHrefs = [...html.matchAll(/<link[^>]+rel="stylesheet"[^>]+href="([^"]+)"/gu)].map((match) => match[1]);
  const bundledCss = stylesheetHrefs
    .map((href) => href.replace(/^https?:\/\/[^/]+/u, '').replace(`/${buildId}/`, ''))
    .filter((href) => href.startsWith('_astro/'))
    .map((href) => readFileSync(join(root, href), 'utf8'))
    .join('\n');
  const normalizedCss = bundledCss.replace(/\s+/gu, '');
  for (const contract of ['.ke-listing-shell', '.ke-listing-discovery-rail', '.ke-listing-card']) {
    if (!bundledCss.includes(contract)) throw new Error(`Listing route ${route} misses design-system CSS contract ${contract}`);
  }
  for (const contract of [
    '.site-header{position:sticky;top:0;',
    '.ke-listing-discovery-rail{position:sticky;top:var(--ke-site-header-bar-height);',
    '.ke-listing-card__media{position:relative;',
  ]) {
    if (!normalizedCss.includes(contract)) throw new Error(`Listing route ${route} misses compiled desktop geometry contract ${contract}`);
  }
  if (!html.includes('class="ke-listing-page')) throw new Error(`Listing route ${route} misses the shared listing-page root`);
  if (!html.includes('data-mobile-listing-rails') || !html.includes('data-mobile-listing-row')) {
    throw new Error(`Listing route ${route} misses the tracked accepted mobile event rail`);
  }
  for (const contract of [
    '@media(max-width:720px)',
    '.ke-mobile-listing-rails--v23.event-row{height:112px',
    '.ke-mobile-listing-rails--v23.rail-window{',
    'width:100vw;height:112px',
    '.ke-mobile-listing-rails--v23.track-start{flex:005px;width:5px',
    '.ke-mobile-listing-rails--v23.event-summary{',
    'flex:00296px;width:296px;height:112px',
  ]) {
    if (!normalizedCss.includes(contract)) throw new Error(`Listing route ${route} misses accepted v23 mobile rail contract ${contract}`);
  }
}
const mobileRailRow = (route, eventId) => {
  const html = readFileSync(join(root, route, 'index.html'), 'utf8');
  const start = html.indexOf(`data-event="${eventId}"`);
  const end = html.indexOf('</article>', start);
  if (start < 0 || end < 0) throw new Error(`Mobile rail canary ${eventId} is missing from ${route}`);
  return html.slice(start, end);
};
const pianissimoRail = mobileRailRow('date-2026-07-24', 5296);
for (const marker of [
  'data-mobile-rail-media-reason="single_safe_visual_landscape_5x4"',
  '--media-width:140px',
  '--rail-media-fit:cover',
  'data-image-text-mode="visual_only"',
  '--focus-x:65%',
]) {
  if (!pianissimoRail.includes(marker)) throw new Error(`Pianissimo 5296 rail crop regression: missing ${marker}`);
}
const teremokRail = mobileRailRow('vyhodnye', 6939);
for (const marker of [
  'data-mobile-rail-media-reason="reviewed_multi_visual_portrait_4x5"',
  '--media-width:90px',
  '--rail-media-fit:cover',
  'data-image-text-mode="visual_only"',
  '00450088000040066194318c30c61a8433adac94241ca7180611098703ce2949.webp',
]) {
  if (!teremokRail.includes(marker)) throw new Error(`Teremok 6939 rail crop regression: missing ${marker}`);
}
const moreRail = mobileRailRow('date-2026-08-08', 4211);
if (!moreRail.includes('data-image-text-mode="ocr_text"') || !moreRail.includes('--rail-media-fit:contain')) {
  throw new Error('More vnutri 4211 OCR media must remain fail-closed');
}
if (!moreRail.includes('/assets/festivals/more-vnutri.svg')) {
  throw new Error('More vnutri 4211 misses its structured external festival medallion');
}
for (const row of [pianissimoRail, teremokRail, moreRail]) {
  if ((row.match(/icon--heart/gu) || []).length !== 3 || !row.includes('icon__heart-outline') || !row.includes('icon__heart-solid')) {
    throw new Error('Mobile rail must use the shared hollow/solid heart component for proof, underlay and action');
  }
}
const mobileRailSurfaceSource = readFileSync(join(siteDir, 'src/components/listings/MobileListingRailSurface.astro'), 'utf8');
for (const gestureMarker of ['setDislike', 'setLikePull', 'finishLike', 'data-rail-confirm-negative', "touchmove", "pointercancel"]) {
  if (!mobileRailSurfaceSource.includes(gestureMarker)) throw new Error(`Mobile rail lost accepted gesture runtime: ${gestureMarker}`);
}
const reference4MenuSource = readFileSync(join(siteDir, 'src/components/Reference4MobileMenu.astro'), 'utf8');
if (reference4MenuSource.includes('.reference4-menu__brand::before')) {
  throw new Error('Reference-4 menu must not paint a local logo scrim over the accepted glass plane');
}
const eventLayoutSource = readFileSync(join(siteDir, 'src/layouts/EventLayout.astro'), 'utf8');
for (const headerContract of [
  "import '../styles/design-system.css';",
  '.site-header {',
  'position: sticky;',
  'top: 0;',
  'height: var(--ke-site-header-bar-height);',
  'z-index: 60;',
]) {
  if (!eventLayoutSource.includes(headerContract)) throw new Error(`Shared static header contract misses ${headerContract}`);
}

const popularHtml = readFileSync(join(root, 'populyarnoe/index.html'), 'utf8');
for (const marker of [
  'data-listing-variant="POPULAR-V26"',
  'data-desktop-popular-version="V28"',
  'data-popular-representation="desktop"',
  'data-popular-representation="mobile-large"',
  'data-popular-representation="mobile-adaptive"',
  'data-ds-component="ListingMobileDensitySwitch"',
  'data-ds-version="6"',
  'data-ds-component="PopularMobileGroupContext"',
  'data-popular-group-sentinel',
  'ke-popular-behavior__head-label',
]) {
  if (!popularHtml.includes(marker)) throw new Error(`Popular V26 misses ${marker}`);
}
const popularDesktopAt = popularHtml.indexOf('data-popular-representation="desktop"');
const popularLargeAt = popularHtml.indexOf('data-popular-representation="mobile-large"');
const popularAdaptiveAt = popularHtml.indexOf('data-popular-representation="mobile-adaptive"');
const popularDockAt = popularHtml.indexOf('data-listing-mobile-density-dock');
if (!(popularDesktopAt < popularLargeAt && popularLargeAt < popularAdaptiveAt && popularAdaptiveAt < popularDockAt)) {
  throw new Error('Popular V26 representations are not isolated in desktop / large / adaptive order');
}
const popularDesktopHtml = popularHtml.slice(popularDesktopAt, popularLargeAt);
const popularPersonalizedAt = popularDesktopHtml.indexOf('data-popular-personalized');
if (popularPersonalizedAt < 0) throw new Error('Popular desktop V28 misses the optional personalized shelf');
const popularDesktopGlobalHtml = popularDesktopHtml.slice(0, popularPersonalizedAt);
const popularLargeHtml = popularHtml.slice(popularLargeAt, popularAdaptiveAt);
const popularAdaptiveHtml = popularHtml.slice(popularAdaptiveAt, popularDockAt);
const listingIds = (html) => [...html.matchAll(/data-listing-item(?:="")?[^>]*data-event-id="(\d+)"/gu)].map((match) => match[1]);
const popularDesktopIds = listingIds(popularDesktopGlobalHtml);
const popularLargeIds = listingIds(popularLargeHtml);
const popularAdaptiveIds = listingIds(popularAdaptiveHtml);
if (popularDesktopIds.length === 0 || new Set(popularDesktopIds).size !== popularDesktopIds.length) {
  throw new Error('Popular desktop V28 ranking must be present and event-id deduplicated');
}
if (popularLargeIds.join(',') !== popularAdaptiveIds.join(',')) {
  throw new Error('Popular V26 mobile density representations must preserve identical ranked event order');
}
const popularDesktopFamilyKeys = [...popularDesktopGlobalHtml.matchAll(/data-listing-family-key="([^"]+)"/gu)].map((match) => match[1]);
if (popularDesktopFamilyKeys.length !== popularDesktopIds.length || new Set(popularDesktopFamilyKeys).size !== popularDesktopFamilyKeys.length) {
  throw new Error('Popular desktop V28 must allocate every event family only once across global shelves');
}
const popularDesktopReasons = [...popularDesktopGlobalHtml.matchAll(/data-popular-reason="([^"]+)"/gu)].map((match) => match[1]);
const expectedPopularDesktopReasonOrder = ['fast_growth', 'multi_source', 'discussed', 'frequently_shared', 'score_fallback'];
if (popularDesktopReasons.length < 3
  || popularDesktopReasons.length > expectedPopularDesktopReasonOrder.length
  || new Set(popularDesktopReasons).size !== popularDesktopReasons.length
  || popularDesktopReasons.some((reason, index) => {
    const expectedIndex = expectedPopularDesktopReasonOrder.indexOf(reason);
    const previousIndex = index > 0 ? expectedPopularDesktopReasonOrder.indexOf(popularDesktopReasons[index - 1]) : -1;
    return expectedIndex < 0 || expectedIndex <= previousIndex;
  })) {
  throw new Error(`Popular desktop V28 shelf order drifted: ${popularDesktopReasons.join(',')}`);
}
const popularDesktopGroupCounts = [...popularDesktopGlobalHtml.matchAll(/data-popular-group-count(?:="")?[^>]*>(\d+)</gu)].map((match) => Number(match[1]));
if (popularDesktopGroupCounts.length !== popularDesktopReasons.length || popularDesktopGroupCounts.some((count) => count < 3 || count > 5)) {
  throw new Error(`Popular desktop V28 shelves must remain one short evidence row (3–5 cards): ${popularDesktopGroupCounts.join(',')}`);
}
const temporalLabels = [...popularDesktopGlobalHtml.matchAll(/data-listing-temporal-status(?:="")?[^>]*>([^<]+)</gu)].map((match) => match[1]);
if (temporalLabels.length !== popularDesktopIds.length || temporalLabels.some((label) => !label.trim())) {
  throw new Error('Popular desktop V28 cards must expose compact lifecycle/date context');
}
if (!popularDesktopGlobalHtml.includes('ещё 1 показ')) {
  throw new Error('Popular desktop V28 must collapse repeated dates into one family card');
}
if (!popularDesktopIds.includes('5130')) {
  throw new Error('Popular desktop V28 regression: Break Summer Fest must remain discoverable');
}
const popularEventById = new Map(eventsData.events.map((event) => [String(event.id), event]));
const popularReference = Date.parse(eventsData.build.generated_at);
const popularEligible = (event) => {
  if (!event || ['cancelled', 'postponed', 'duplicate', 'merged', 'deleted', 'inactive'].includes(String(event.lifecycle_status || '').toLowerCase())) return false;
  if (event.end_date && event.end_date !== event.start_date) return event.end_date >= eventsData.build.current_date;
  if (event.start_date > eventsData.build.current_date) return true;
  if (event.start_date < eventsData.build.current_date) return false;
  if (!Number.isFinite(popularReference)) return false;
  const startsAt = Date.parse(event.starts_at || '');
  if (Number.isFinite(startsAt)) return startsAt >= popularReference;
  const startTime = /^(\d{1,2}):(\d{2})(?::(\d{2}))?$/u.exec(String(event.start_time || '').trim());
  if (!startTime) return false;
  const referenceTime = new Intl.DateTimeFormat('en-CA', {
    timeZone: event.timezone || 'Europe/Kaliningrad',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hourCycle: 'h23',
  }).formatToParts(new Date(popularReference));
  const part = (type) => Number(referenceTime.find((item) => item.type === type)?.value || 0);
  const eventSeconds = Number(startTime[1]) * 3600 + Number(startTime[2]) * 60 + Number(startTime[3] || 0);
  const referenceSeconds = part('hour') * 3600 + part('minute') * 60 + part('second');
  return eventSeconds >= referenceSeconds;
};
const popularIds = [...new Set([...popularDesktopIds, ...popularLargeIds, ...popularAdaptiveIds])];
const ineligiblePopularIds = popularIds.filter((id) => !popularEligible(popularEventById.get(id)));
if (ineligiblePopularIds.length !== 0) {
  throw new Error(`Popular includes ineligible event ids at build time: ${ineligiblePopularIds.join(',')}`);
}
const personalPoolHtml = popularDesktopHtml.slice(popularPersonalizedAt);
const personalIds = listingIds(personalPoolHtml);
const personalFamilies = [...personalPoolHtml.matchAll(/data-listing-family-key="([^"]+)"/gu)].map((match) => match[1]);
if (personalIds.length < 5 || personalFamilies.length !== personalIds.length || new Set(personalFamilies).size !== personalFamilies.length) {
  throw new Error('Popular desktop V28 personal candidate pool must be family-deduplicated and sufficiently deep');
}
if (personalFamilies.some((key) => popularDesktopFamilyKeys.includes(key))) {
  throw new Error('Popular desktop V28 personal candidate pool must exclude globally visible families');
}
if (!/data-popular-personalized[^>]*hidden/u.test(popularDesktopHtml)) {
  throw new Error('Popular desktop V28 personal shelf must remain absent for cold start');
}
if (!popularLargeHtml.includes('event-card--split-actions') || popularLargeHtml.includes('listing-proof')) {
  throw new Error('Popular V26 large mode must reuse canonical EventCard split-actions without listing-proof');
}
if (!popularAdaptiveHtml.includes('data-ds-component="ListingEventCard"') || !/data-popular-mobile-layout="adaptive"[^>]*hidden[^>]*inert/u.test(popularAdaptiveHtml)) {
  throw new Error('Popular V26 adaptive mode must use inactive-by-default ListingEventCard rows');
}
if (!popularHtml.includes('maximum-scale=1, user-scalable=no')) throw new Error('Popular V26 must disable page zoom only on its gesture-enabled listing route');
const popularCss = readFileSync(join(siteDir, 'src/styles/design-system.css'), 'utf8');
for (const cssContract of [
  '[data-popular-representation="mobile-adaptive"] .ke-popular-behavior__row',
  'flex-wrap: wrap',
  'right: 0;',
  'left: 0;',
  'width: 100%;',
  'width: min(calc(100% - 24px), var(--ke-content-listing-max));',
  'gap: 20px 8px;',
  'touch-action: pan-y;',
  '--ke-listing-mobile-tail-width: 28px;',
  '--ke-listing-mobile-tail-width: 44px;',
  'body:has(.ke-popular-listing) .site-nav { display: none; }',
  'position: sticky;',
  'top: var(--ke-popular-group-sticky-top);',
  'max-width: min(calc(100vw - 156px), 288px);',
  '.ke-popular-behavior__group:not(:last-child)',
  'padding-bottom: 72px;',
  'text-align: right;',
  'pointer-events: none;',
]) {
  if (!popularCss.includes(cssContract)) throw new Error(`Popular V26 CSS misses ${cssContract}`);
}
const densitySource = readFileSync(join(siteDir, 'src/components/listings/ListingMobileDensitySwitch.astro'), 'utf8');
for (const gestureContract of ['pinchDistance', "matchMedia('(max-width: 720px)')", "ratio <= 0.84 ? 'adaptive'", "ratio >= 1.16 ? 'large'", "{ passive: false }", "dataset.listingPinchReady = 'true'", 'preferredEventId', 'dataset.listingContextEventId = anchorId', "new CustomEvent('listing:density-change'"]) {
  if (!densitySource.includes(gestureContract)) throw new Error(`Popular V26 pinch/context contract misses ${gestureContract}`);
}
const groupContextSource = readFileSync(join(siteDir, 'src/components/listings/PopularMobileGroupContext.astro'), 'utf8');
for (const stickyContract of ['IntersectionObserver', 'data-popular-group-sentinel', "querySelectorAll('[data-popular-mobile-layout] [data-popular-behavior-group]')", 'observer.observe(group)', "matchMedia('(max-width: 720px)')", "addEventListener('listing:density-change'", "classList.toggle('is-stuck'", 'getBoundingClientRect']) {
  if (!groupContextSource.includes(stickyContract)) throw new Error(`Popular V26 sticky group context misses ${stickyContract}`);
}
for (const scenario of templateContract.lab_scenarios) {
  const scenarioHtml = readFileSync(join(root, `lab/event-desktop/examples/${scenario}/index.html`), 'utf8');
  if (!scenarioHtml.includes(`data-event-template-contract="${templateContract.contract_id}"`)) {
    throw new Error(`Desktop template scenario ${scenario} is not bound to ${templateContract.contract_id}`);
  }
  if (!/data-desktop-family="(?:editorial|split)"/u.test(scenarioHtml)) {
    throw new Error(`Desktop template scenario ${scenario} bypasses the accepted family renderer`);
  }
}
if (interestClubsData.schema_version !== 'interest-clubs-static-v1' || interestClubsData.projection_version !== 1) {
  throw new Error('Interest-clubs projection contract is not interest-clubs-static-v1');
}
const clubIndexHtml = readFileSync(join(root, 'kluby-po-interesam/index.html'), 'utf8');
if (!/<h1[^>]*>Клубы по интересам<\/h1>/u.test(clubIndexHtml)) throw new Error('Interest-clubs index misses one visible h1');
if (clubIndexHtml.includes('data-product-breadcrumbs') || clubIndexHtml.includes('data-product-parent-link')) throw new Error('Top-level Interest-clubs index must not spend vertical space on decorative breadcrumbs');
if (!clubIndexHtml.includes('https://schema.org') || !clubIndexHtml.includes('ItemList')) throw new Error('Interest-clubs index misses JSON-LD ItemList');
if (!clubIndexHtml.includes('/kluby-po-interesam/')) throw new Error('Interest-clubs route is absent from static navigation');
for (const club of interestClubsData.clubs) {
  const detailHtml = readFileSync(join(root, `kluby-po-interesam/${club.slug}/index.html`), 'utf8');
  if (!detailHtml.includes(`>${club.name}</h1>`)) throw new Error(`Club ${club.slug} misses visible h1`);
  if (!detailHtml.includes('id="future-meetings-title"')) throw new Error(`Club ${club.slug} misses future-meetings section`);
  if (!detailHtml.includes('data-product-breadcrumbs') || !detailHtml.includes('data-product-parent-link') || !detailHtml.includes('aria-current="page"')) throw new Error(`Club ${club.slug} misses responsive semantic product breadcrumbs`);
  if (!detailHtml.includes('BreadcrumbList') || !detailHtml.includes('application/ld+json')) throw new Error(`Club ${club.slug} misses structured data`);
  const clubPrimaryHtml = detailHtml.replace(/<section[^>]*data-pwa-install-root[^>]*>[\s\S]*?<\/section>/giu, '');
  if (/<main[^>]+hidden|<article[^>]+hidden|<section[^>]+hidden/iu.test(clubPrimaryHtml)) throw new Error(`Club ${club.slug} requires JavaScript to reveal primary content`);
}


let transportEventCount = 0;
let transportIcsTotal = 0;
for (const event of eventsData.events) {
  const eventHtmlPath = join(root, `sobytiya/${event.slug}/index.html`);
  const html = readFileSync(eventHtmlPath, 'utf8');
  if (
    !html.includes('data-mobile-event-production')
    || !html.includes('data-mobile-review-variant="accepted-v8"')
    || !html.includes('data-mobile-review-revision="v4"')
    || !html.includes('data-mobile-parallax-profile="photo-continuous-crop"')
    || !html.includes('mobile-event-production__continuation')
    || !html.includes('event-hero__meta-line--weekday-panel')
  ) {
    throw new Error(`Event ${event.id} misses the responsive accepted mobile-v8 production contract`);
  }
  const hasRail = html.includes('data-event-transport-schedule');
  const hasBus = html.includes('data-event-bus-schedule');
  if (hasRail || hasBus) {
    transportEventCount += 1;
    // Desktop and mobile are deliberately separate DOM surfaces. The shared
    // accepted desktop component can render its additive transport block
    // before the accepted mobile-v8 markup, so the mobile ordering contract
    // must be evaluated inside the mobile root rather than against the first
    // transport marker in the complete HTML document.
    const mobileAt = html.indexOf('data-production-mobile-event');
    const mobileHtml = mobileAt >= 0 ? html.slice(mobileAt) : '';
    const factsAt = mobileHtml.indexOf('>Коротко</h2>');
    const transportAt = hasRail ? mobileHtml.indexOf('data-event-transport-schedule') : mobileHtml.indexOf('data-event-bus-schedule');
    if (factsAt < 0 || transportAt < factsAt) throw new Error(`Event ${event.id} transport must follow compact event facts`);
    if (hasRail && (!mobileHtml.includes('event-transport__train') || !mobileHtml.includes('/assets/transport/kppk-lastochka.webp'))) {
      throw new Error(`Rail event ${event.id} misses the visible train illustration in the accepted mobile surface`);
    }
  }
  if (String(event.city || '').toLocaleLowerCase('ru-RU') === 'калининград' && hasRail) {
    throw new Error(`Kaliningrad event ${event.id} must not render a coastal rail schedule`);
  }
  const transportDir = join(root, `sobytiya/${event.slug}/transport`);
  const files = existsSync(transportDir) ? readdirSync(transportDir).filter((name) => name.endsWith('.ics')).sort() : [];
  const linked = [...html.matchAll(/href="[^"]*\/transport\/([^"?#]+\.ics)(?:[?#][^"]*)?"/gu)].map((match) => match[1]);
  const linkedUnique = [...new Set(linked)].sort();
  if (files.length > 6) throw new Error(`Event ${event.id} exceeds the six-file transport ICS ceiling: ${files.length}`);
  if (files.join('\n') !== linkedUnique.join('\n')) throw new Error(`Event ${event.id} transport ICS files must match interactive calendar links exactly`);
  for (const name of files) {
    if (!/^rzd-[a-z0-9-]+-\d{8}-[a-z0-9-]+\.ics$/u.test(name)) throw new Error(`Transport ICS filename is not concise semantic ASCII: ${name}`);
  }
  transportIcsTotal += files.length;
}
if (transportEventCount === 0 || transportIcsTotal === 0) throw new Error('Preview must include at least one factual transport event and linked transport ICS file');

const romanovoRoute = busData.routes.find((route) => route.id === 'romanovo-holmogorye');
const route119 = romanovoRoute?.outbound_groups.find((group) => group.routes === '119');
if (!romanovoRoute || !route119) throw new Error('Romanovo route 119 transport source is missing');
if (romanovoRoute.origin_stop !== 'Автовокзал Калининград' || route119.departure_stop !== 'Автовокзал Калининград') {
  throw new Error('Route 119 must preserve the official raw terminal departure provenance');
}
if (!route119.departures.includes('16:30') || !route119.departures.includes('17:55')) {
  throw new Error('Route 119 must preserve the raw 16:30/17:55 terminal departures used by the KAUP regression');
}
if (
  romanovoRoute.preferred_boarding?.stop_name !== 'Северный вокзал'
  || romanovoRoute.preferred_boarding?.offset_from_terminal_minutes !== 15
  || romanovoRoute.preferred_boarding?.time_is_estimated !== true
) throw new Error('Romanovo buses that serve North must prefer the estimated terminal +15 minute North boarding contract');

const busDemoEvent = eventsData.events.find((event) => event.id === 6710);
if (!busDemoEvent) throw new Error('Missing real event 6710 Romanovo bus regression event');
const busDemoHtml = readFileSync(join(root, `sobytiya/${busDemoEvent.slug}/index.html`), 'utf8');
for (const marker of [
  'Северный вокзал → Сказочное Холмогорье',
  'data-terminal-departure="08:10"',
  'data-boarding-stop="Северный вокзал"',
  '>≈ 08:25</time>',
]) {
  if (!busDemoHtml.includes(marker)) throw new Error(`Event 6710 preferred North boarding contract is missing ${marker}`);
}

const mobileReviewCases = {
  control: readFileSync(join(root, 'lab/event-mobile/examples/control/photo-paid/index.html'), 'utf8'),
  openProse: readFileSync(join(root, 'lab/event-mobile/examples/open-prose/photo-paid/index.html'), 'utf8'),
  actionDock: readFileSync(join(root, 'lab/event-mobile/examples/action-dock/photo-paid/index.html'), 'utf8'),
  combined: readFileSync(join(root, 'lab/event-mobile/examples/open-prose-action-dock/photo-paid/index.html'), 'utf8'),
  acceptedV2: readFileSync(join(root, 'lab/event-mobile/examples/accepted-v2/photo-paid/index.html'), 'utf8'),
  acceptedV3: readFileSync(join(root, 'lab/event-mobile/examples/accepted-v3/photo-paid/index.html'), 'utf8'),
  acceptedV4: readFileSync(join(root, 'lab/event-mobile/examples/accepted-v4/photo-paid/index.html'), 'utf8'),
  acceptedV5: readFileSync(join(root, 'lab/event-mobile/examples/accepted-v5/photo-paid/index.html'), 'utf8'),
  acceptedV6: readFileSync(join(root, 'lab/event-mobile/examples/accepted-v6/photo-paid/index.html'), 'utf8'),
  acceptedV7: readFileSync(join(root, 'lab/event-mobile/examples/accepted-v7/photo-paid/index.html'), 'utf8'),
  acceptedV8: readFileSync(join(root, 'lab/event-mobile/examples/accepted-v8/photo-paid/index.html'), 'utf8'),
};
for (const [name, html] of Object.entries(mobileReviewCases)) {
  if (!html.includes('data-mobile-event-review') || !html.includes('data-mobile-review-variant=')) {
    throw new Error(`Mobile event review ${name} misses its variant marker`);
  }
}
if (!mobileReviewCases.control.includes('data-prose-treatment="card"') || !mobileReviewCases.control.includes('data-actions-treatment="current"')) {
  throw new Error('Mobile event control must preserve current prose and actions');
}
if (!mobileReviewCases.openProse.includes('data-prose-treatment="open"') || !mobileReviewCases.actionDock.includes('data-actions-treatment="dock"')) {
  throw new Error('Mobile event single-factor variants must isolate open prose and grouped actions');
}
if (!mobileReviewCases.combined.includes('data-prose-treatment="open"') || !mobileReviewCases.combined.includes('data-actions-treatment="dock"')) {
  throw new Error('Mobile event combined variant must enable both treatments');
}
if (!mobileReviewCases.acceptedV2.includes('data-mobile-review-revision="v2"') || !mobileReviewCases.acceptedV2.includes('data-compact-label-action=')) {
  throw new Error('Mobile event accepted v2 must expose revision and deterministic compact-label markers');
}
if (!mobileReviewCases.acceptedV3.includes('data-mobile-review-revision="v3"') || !mobileReviewCases.acceptedV3.includes('event-hero__meta-line--weekday-panel') || !mobileReviewCases.acceptedV3.includes('event-hero__weekday')) {
  throw new Error('Mobile event accepted v3 must expose the feedback revision and weekday-first metadata panel');
}
if (!mobileReviewCases.acceptedV4.includes('data-mobile-review-revision="v4"') || !mobileReviewCases.acceptedV4.includes('event-hero__meta-line--weekday-panel') || !mobileReviewCases.acceptedV4.includes('event-hero__weekday')) {
  throw new Error('Mobile event accepted v4 must preserve the accepted weekday-first date/time panel');
}
if (!mobileReviewCases.acceptedV5.includes('data-mobile-review-variant="accepted-v5"') || !mobileReviewCases.acceptedV5.includes('data-mobile-review-revision="v4"') || !mobileReviewCases.acceptedV5.includes('event-hero__weekday') || !mobileReviewCases.acceptedV5.includes('mobile-event-review__continuation')) {
  throw new Error('Mobile event accepted v5 must preserve v4 controls and expose the gradient continuation surface');
}
if (!mobileReviewCases.acceptedV6.includes('data-mobile-review-variant="accepted-v6"') || !mobileReviewCases.acceptedV6.includes('data-mobile-review-revision="v4"') || !mobileReviewCases.acceptedV6.includes('event-hero__weekday') || !mobileReviewCases.acceptedV6.includes('mobile-event-review__continuation')) {
  throw new Error('Mobile event accepted v6 must preserve v5 controls and expose the seamless gradient continuation surface');
}
if (!mobileReviewCases.acceptedV6.includes('data-calendar-action') || !mobileReviewCases.acceptedV6.includes('data-calendar-expiry-day=') || !mobileReviewCases.acceptedV6.includes('data-calendar-label')) {
  throw new Error('Mobile event accepted v6 must expose the shared expiring calendar state contract');
}
if (!mobileReviewCases.acceptedV7.includes('data-mobile-review-variant="accepted-v7"') || !mobileReviewCases.acceptedV7.includes('data-mobile-review-revision="v4"') || !mobileReviewCases.acceptedV7.includes('data-mobile-parallax-profile="photo-velocity-matched"') || !mobileReviewCases.acceptedV7.includes('mobile-event-review__continuation')) {
  throw new Error('Mobile event accepted v7 must preserve v6 surfaces and expose the photo-velocity-matched parallax profile');
}
if (!mobileReviewCases.acceptedV7.includes('data-calendar-action') || !mobileReviewCases.acceptedV7.includes('data-calendar-expiry-day=') || !mobileReviewCases.acceptedV7.includes('data-calendar-label')) {
  throw new Error('Mobile event accepted v7 must preserve the shared expiring calendar state contract');
}
if (!mobileReviewCases.acceptedV8.includes('data-mobile-review-variant="accepted-v8"') || !mobileReviewCases.acceptedV8.includes('data-mobile-review-revision="v4"') || !mobileReviewCases.acceptedV8.includes('data-mobile-parallax-profile="photo-continuous-crop"') || !mobileReviewCases.acceptedV8.includes('mobile-event-review__continuation')) {
  throw new Error('Mobile event accepted v8 must preserve v7 surfaces and expose the continuous crop-safe parallax profile');
}
if (!mobileReviewCases.acceptedV8.includes('data-calendar-action') || !mobileReviewCases.acceptedV8.includes('data-calendar-expiry-day=') || !mobileReviewCases.acceptedV8.includes('data-calendar-label')) {
  throw new Error('Mobile event accepted v8 must preserve the shared expiring calendar state contract');
}
const mobileAcceptedV2OcrOverride = readFileSync(join(root, 'lab/event-mobile/examples/accepted-v2/visual-free/index.html'), 'utf8');
if (!mobileAcceptedV2OcrOverride.includes('data-hero-mode="poster-stage"') || !mobileAcceptedV2OcrOverride.includes('data-hero-composition="poster-billboard"')) {
  throw new Error('Mobile event accepted v2 must render the misclassified text poster without photo-cover zoom');
}
const mobileAcceptedV3OcrOverride = readFileSync(join(root, 'lab/event-mobile/examples/accepted-v3/visual-free/index.html'), 'utf8');
if (!mobileAcceptedV3OcrOverride.includes('data-hero-mode="poster-stage"') || !mobileAcceptedV3OcrOverride.includes('data-hero-composition="poster-billboard"')) {
  throw new Error('Mobile event accepted v3 must preserve the no-zoom text-poster override');
}
const mobileAcceptedV4OcrOverride = readFileSync(join(root, 'lab/event-mobile/examples/accepted-v4/visual-free/index.html'), 'utf8');
if (!mobileAcceptedV4OcrOverride.includes('data-hero-mode="poster-stage"') || !mobileAcceptedV4OcrOverride.includes('data-hero-composition="poster-billboard"')) {
  throw new Error('Mobile event accepted v4 must preserve the no-zoom text-poster override');
}
for (const variant of ['accepted-v5', 'accepted-v6', 'accepted-v7', 'accepted-v8']) {
  const html = readFileSync(join(root, `lab/event-mobile/examples/${variant}/visual-free/index.html`), 'utf8');
  if (!html.includes('data-hero-mode="poster-stage"') || !html.includes('data-hero-composition="poster-billboard"')) {
    throw new Error(`Mobile event ${variant} must preserve the no-zoom text-poster override`);
  }
}

const desktopV12Pages = {
  garage: readFileSync(join(root, 'lab/event-desktop/examples/editorial-photo-continuous/index.html'), 'utf8'),
  split: readFileSync(join(root, 'lab/event-desktop/examples/split-low-resolution/index.html'), 'utf8'),
  companion: readFileSync(join(root, 'lab/event-desktop/examples/editorial-ocr-companion-arrival/index.html'), 'utf8'),
  portraitCarousel: readFileSync(join(root, 'lab/event-desktop/examples/portrait-carousel-production/index.html'), 'utf8'),
};
for (const [name, html] of Object.entries(desktopV12Pages)) {
  if (!html.includes('data-service-share-root') || !html.includes('data-service-share-intent="image"') || !html.includes('data-service-share-intent="text"')) {
    throw new Error(`Desktop v14 ${name} fixture must expose the approved footer service-share intents`);
  }
}
if (!desktopV12Pages.garage.includes('data-auto-rotate="true"') || !desktopV12Pages.garage.includes('data-rotation-eligible="true"')) {
  throw new Error('Desktop v12 Garage fixture must expose delayed autorotation with eligible event-photo candidates');
}
if (!desktopV12Pages.garage.includes('data-clean-hero-thumb') || !desktopV12Pages.garage.includes('data-src=')) {
  throw new Error('Desktop v12 Garage fixture must keep thumbnail rail sources separate from fullscreen hero sources');
}
for (const [name, html] of Object.entries(desktopV12Pages)) {
  if (!html.includes('/p/thumb/v1/') || !html.includes(' 512w')) {
    throw new Error(`Desktop v12 ${name} fixture must use responsive immutable thumbnail derivatives`);
  }
}
if (!desktopV12Pages.garage.includes('data-thumbnail-srcset') || !desktopV12Pages.split.includes('data-thumbnail-srcset')) {
  throw new Error('Desktop v14 rails must defer responsive thumbnail sources until their cells are visible');
}
if (!desktopV12Pages.garage.includes('data-editorial-motion="continuous"') || !desktopV12Pages.companion.includes('data-editorial-motion="continuous"')) {
  throw new Error('Desktop v12 Garage and OCR companion fixtures must share continuous editorial motion');
}
const desktopV12Script = readdirSync(join(root, '_astro'))
  .find((name) => name.startsWith('DesktopEventPage.astro_astro_type_script_') && name.endsWith('.js'));
if (!desktopV12Script) throw new Error('Desktop v12 behavior bundle is missing');
const desktopV12ScriptSource = readFileSync(join(root, '_astro', desktopV12Script), 'utf8');
for (const marker of ['waiting-media', 'gallery-open', 'manual-rail-interaction', 'pointer-move', 'autoRotateReady']) {
  if (!desktopV12ScriptSource.includes(marker)) throw new Error(`Desktop v13 behavior bundle misses ${marker} idle-resume guard`);
}
if (desktopV12ScriptSource.includes('preload-failed')) throw new Error('Desktop v13 must not wait for or fail the whole autorotation set');
if (!desktopV12Pages.split.includes('desktop-portrait-viewer__heading') || !desktopV12Pages.split.includes('<time datetime=')) {
  throw new Error('Desktop v13 efficient viewer must expose event title plus date/time');
}
const portraitCarouselItems = desktopV12Pages.portraitCarousel.match(/data-efficient-viewer-item/g)?.length || 0;
if (!desktopV12Pages.portraitCarousel.includes('data-split-efficient-viewer="true"') || portraitCarouselItems !== 7) {
  throw new Error(`Production portrait-carousel lab must expose seven quality-admitted height-fit items, got ${portraitCarouselItems}`);
}
if (!desktopV12Pages.portraitCarousel.includes('Показаны 7 из 12 изображений в лучшем качестве')) {
  throw new Error('Production portrait-carousel lab must disclose quality filtering');
}
if (!desktopV12Pages.companion.includes('event-token--kaup') || !desktopV12Pages.companion.includes('/assets/festivals/kaup.svg')) {
  throw new Error('Desktop v13 OCR companion must retain the accepted Kaup venue-brand medallion');
}
if (!/data-editorial-ocr-companion[^>]*data-source-index="0"[^>]*data-hero-gallery-index="1"/u.test(desktopV12Pages.companion)) {
  throw new Error('Desktop v13 OCR companion poster must retain its exact source/gallery index mapping');
}
const kgd80Events = eventsData.events.filter((event) => String(event.festival || '').trim() === '80 историй о главном');
for (const event of kgd80Events) {
  const html = readFileSync(join(root, `sobytiya/${event.slug}/index.html`), 'utf8');
  if (!html.includes('event-token--kgd80')) throw new Error(`80 Stories event ${event.id} misses KGD80 festival medallion`);
  if (!html.includes('event-token--znanie-russia')) throw new Error(`80 Stories event ${event.id} misses curated Znanie organizer medallion`);
}
const control = eventsData.events.find((event) => event.id === 5878);
if (!control) {
  if (!eventsData.events.length) throw new Error('Preview fixture must contain at least one event');
  if (relatedData.strict_verified_related) {
    const relatedValues = Object.values(relatedData.related || {});
    if (!relatedValues.length) throw new Error('Strict related preview must contain related map entries');
    for (const entry of relatedValues) {
      for (const candidateId of entry.similar || []) {
        const chainItem = (entry.chain || []).find((item) => Number(item.event_id) === Number(candidateId));
        if (!chainItem || chainItem.llm_semantic_score === undefined || Number(chainItem.llm_semantic_score) < 0.72 || chainItem.gemma_reject) {
          throw new Error(`Strict related similar candidate is not Gemma-verified: ${candidateId}`);
        }
      }
    }
  }
  console.log(`Preview check passed without control fixture: ${eventsData.events.length} events, strict_related=${Boolean(relatedData.strict_verified_related)}`);
  process.exit(0);
}
if (control.slug !== 'pesni-sssr-svetlogorsk-5878') throw new Error(`Unexpected control slug: ${control.slug}`);
const controlHtml = readFileSync(join(root, `sobytiya/${control.slug}/index.html`), 'utf8');
const stripGeneratedCode = (html) => html.replace(/<script[\s\S]*?<\/script>/giu, '').replace(/<style[\s\S]*?<\/style>/giu, '');
const controlVisibleHtml = stripGeneratedCode(controlHtml);
if (!controlHtml.includes('noindex,nofollow,noarchive')) throw new Error('Missing preview robots meta');
if (controlHtml.includes('https://kenigevents.ru/sobytiya/pesni-sssr-svetlogorsk-5878/')) throw new Error('Production canonical leaked into preview page');
if (!controlHtml.includes(`https://kenigevents.ru/${buildId}/sobytiya/pesni-sssr-svetlogorsk-5878/`)) throw new Error('Preview canonical missing for control page');
if (/\bnull\b/.test(controlVisibleHtml)) throw new Error('Rendered HTML contains literal null outside scripts/styles');
if (controlHtml.includes('<a class="event-card"')) throw new Error('Nested-link-prone event-card anchor leaked');
if (!controlHtml.includes('data-card-href=')) throw new Error('Event cards must expose full-card navigation href');
if (!/<div class="event-card__body">\s*<a class="event-card__title"[\s\S]*?<div class="event-card__meta-row">/u.test(controlVisibleHtml)) throw new Error('Event cards must show title before time/status meta for mobile feed scanning');
if (!controlHtml.includes('event-card__media')) throw new Error('Control related cards do not expose visual media slot');
if (!controlHtml.includes('event-card__media-shell--document')) throw new Error('Text/document cards must use the width-fit, vertical-overflow-only media contract');
if (!controlHtml.includes('event-card__media-shell--cover')) throw new Error('Visual-only poster cards must keep 4:5 cover media shell');
if (controlHtml.includes('media-backdrop') || controlHtml.includes('image-backdrop') || controlHtml.includes('--poster-image') || /background-image:\s*linear-gradient\([^;]*var\(--poster-image\)|blur\(/u.test(controlHtml)) throw new Error('Duplicate/backdrop poster fill leaked into event page');
if (/data-(?:feedback|share)-count[^>]*>0<\/span>/u.test(controlHtml)) throw new Error('Zero like/share counters must be hidden, not rendered as 0');
if (/event-card__media-shell--document[\s\S]{0,500}object-fit:\s*contain/iu.test(controlHtml)) throw new Error('Document cards must scale to full width instead of contain-with-side-fields');
if (/Вход[\s\S]{0,180}По билетам/u.test(controlVisibleHtml)) throw new Error('The rejected “По билетам” copy must not be rendered as an admission value');
if (controlHtml.includes('event-card__actions')) throw new Error('Old separate card action row leaked');
if (!controlVisibleHtml.includes('data-feed-card-variant="split-actions"') || !controlVisibleHtml.includes('event-card__feedback event-card__feedback--under')) throw new Error('Control event page must use split-actions baseline cards');
if (controlVisibleHtml.includes('event-card__feedback event-card__feedback--overlay')) throw new Error('Overlay-controls cards must not appear on normal event pages');
if (!controlVisibleHtml.includes('feedback-button--calendar')) throw new Error('Split-actions baseline must expose feed calendar buttons for eligible candidates');
if (!controlHtml.includes('data-feedback-action="like"') || !controlHtml.includes('data-feedback-count')) throw new Error('Control related cards miss explicit like buttons');
if (controlHtml.includes('data-source-likes-count') || controlHtml.includes('data-service-likes-count') || controlHtml.includes('data-like-origin-label') || controlHtml.includes('feedback-origin-label') || controlHtml.includes('event-card__social-proof') || /ист\.\s*\+|из источников|в сервисе/u.test(controlHtml)) throw new Error('Technical source/service like breakdown leaked into public HTML/UI');
if (!controlHtml.includes('feedback-button--share') || !controlHtml.includes('data-share-count')) throw new Error('Control related cards miss explicit share button/count');
if (controlHtml.includes('data-share-experiment') || controlHtml.includes('Поделиться эксперимент') || controlHtml.includes('data-copy-rich-share') || controlHtml.includes('Скопировать HTML-пост')) throw new Error('Temporary share experiment buttons must not leak into production-like event UI');
if (!controlHtml.includes('data-native-share') || !controlHtml.includes('data-share-image=') || !controlHtml.includes('data-share-image-type=') || !controlHtml.includes('data-share-file-name=') || !controlHtml.includes('navigator.canShare') || !controlHtml.includes('sharePayload(button, [file])') || !controlHtml.includes('createGeneratedShareImage') || !controlHtml.includes('1080') || !controlHtml.includes('1350')) throw new Error('Control event page misses primary Web Share file/text/url path with generated 4:5 share-image fallback');
if (!controlHtml.includes('og:image:secure_url') || !controlHtml.includes('og:image:type')) throw new Error('Control event page misses strengthened Open Graph image metadata for share previews');
if (!controlHtml.includes('data-feedback-scope') || !/data-event-hero[\s\S]{0,6500}data-feedback-action="like"/u.test(controlHtml)) throw new Error('Event hero must expose a first-party like button/count for the current event');
if (!controlHtml.includes('M11.996 3.725') || controlHtml.includes('M4.2 16.1c3.45-4.8')) throw new Error('Share/repost icon must use the VK-like outline path, not the old arrow stroke');
if (!controlHtml.includes('data-feedback-action="not_interested"')) throw new Error('Control related cards miss not-interested buttons');
if (!/data-nosnippet[^>]*data-feedback-action="not_interested"|data-feedback-action="not_interested"[^>]*data-nosnippet/u.test(controlHtml) || !/data-nosnippet[^>]*data-native-share|data-native-share[^>]*data-nosnippet/u.test(controlHtml)) throw new Error('Service controls such as not-interested/share must be marked data-nosnippet');
if (!controlHtml.includes('share-label') || !controlHtml.includes('is-share-prompt')) throw new Error('Control related cards miss post-like share prompt expansion');
if (controlHtml.includes('double_tap_like_event')) throw new Error('Double-tap like must not conflict with full-card navigation');
if (!controlHtml.includes('data-event-hero') || !controlHtml.includes('data-hero-mode="poster-stage"') || !controlHtml.includes('data-hero-composition="poster-billboard"') || !controlHtml.includes('data-hero-image-text-mode="ocr_text"')) throw new Error('Control event must render OCR-safe poster-billboard decision hero');
if (!controlHtml.includes('data-hero-gallery-open="hero-gallery-5878"') || !controlHtml.includes('data-hero-gallery') || !controlHtml.includes('hero-gallery__slide--cta') || !controlHtml.includes('Смотреть похожее')) throw new Error('Event hero must expose fullscreen image gallery with final similar-event CTA slide');
if (!controlVisibleHtml.includes('<a class="hero-gallery__brand"') || !controlVisibleHtml.includes('Полюбить Калининград') || !controlVisibleHtml.includes('hero-gallery__slide') || !controlVisibleHtml.includes('hero-gallery__caption') || !controlVisibleHtml.includes('Фото события')) throw new Error('Hero gallery must keep the service tag as a navigable link and one fixed readable bottom title stripe');
if (!controlVisibleHtml.includes('event-hero__gallery-hint') || !/(Открыть фото|Фото \d+)/u.test(controlVisibleHtml)) throw new Error('Event hero must expose a visible photo-view CTA/count over the image');
if (!controlHtml.includes('data-gallery-src=') || /class="hero-gallery__image"[^>]*\ssrc=/u.test(controlHtml)) throw new Error('Fullscreen gallery images must be lazy hydrated from data-gallery-src, not eagerly loaded in hidden HTML');
if (!controlHtml.includes('data-mobile-discovery-menu') || !controlHtml.includes('mobile-discovery-menu__panel') || !controlHtml.includes('mobile-discovery-menu__links') || !controlHtml.includes('is-past-hero')) throw new Error('Immersive event pages must include mobile discovery drawer and stable after-hero state contract');
if (controlHtml.includes('mobile-discovery-menu__brand-icon') || controlHtml.includes('/brand-mark.svg')) throw new Error('Mobile discovery tag must not expose the rejected brand icon/brand-mark animation');
if (!controlHtml.includes('data-announcements-lockup="mobile"') || !controlHtml.includes('announcements-wordmark-ui.svg') || !controlVisibleHtml.includes('/zavtra/')) throw new Error('Mobile discovery/navigation must expose tomorrow link and the shared mobile lettering lockup');
if (controlHtml.includes('mobile-discovery-menu__chevron') || controlHtml.includes('⌄')) throw new Error('Mobile discovery drawer handle must not expose chevron/up/down icons');
if ((controlVisibleHtml.match(/<h1\b/giu) || []).length !== 1) throw new Error('Event page must expose exactly one visible H1');
if (!controlVisibleHtml.includes('event-hero__decision') || !controlVisibleHtml.includes('event-hero__actions')) throw new Error('Event hero must include decision block and first-screen actions in HTML');
if (controlVisibleHtml.includes('crumbs--after-hero')) throw new Error('Event detail must not render the retired mobile breadcrumb/back row');
let checkedMediaRegressionEvents = 0;
for (const [id, expectedMode] of [[5370, 'visual_only'], [6322, 'visual_only'], [4512, 'visual_only'], [3730, 'visual_only'], [4913, 'visual_only'], [5878, 'ocr_text'], [6093, 'ocr_text'], [6437, 'ocr_text'], [6438, 'ocr_text']]) {
  const item = eventsData.events.find((event) => event.id === id);
  if (!item) continue;
  checkedMediaRegressionEvents += 1;
  if (item.image_text_mode !== expectedMode) throw new Error(`Event ${id} image_text_mode must be ${expectedMode} for media regression guard`);
}
if (checkedMediaRegressionEvents < 4) throw new Error(`Media regression guard needs at least 4 present control events, got ${checkedMediaRegressionEvents}`);
const tretyakovEvent = eventsData.events.find((event) => event.id === 5370);
if (!tretyakovEvent) throw new Error('Missing 5370 ticket/paid regression event');
if (!Array.isArray(tretyakovEvent.image_assets) || tretyakovEvent.image_assets.length < 5) throw new Error('Event 5370 must carry multi-image gallery assets for hero fullscreen review');
const tretyakovHtml = readFileSync(join(root, `sobytiya/${tretyakovEvent.slug}/index.html`), 'utf8');
const tretyakovVisibleHtml = stripGeneratedCode(tretyakovHtml);
const tretyakovEyebrow = (tretyakovVisibleHtml.match(/<p class="event-hero__eyebrow">([^<]*)<\/p>/u) || [null, ''])[1];
if (!tretyakovEvent.ticket.is_free && tretyakovEvent.ticket.kind === 'ticket' && !tretyakovEvent.ticket.price_label) {
  if (/Билеты|Платный вход/u.test(tretyakovEyebrow)) throw new Error('Paid/generic admission copy must not be shown above the event title');
  if (!tretyakovVisibleHtml.includes('Билеты') || tretyakovVisibleHtml.includes('По билетам')) throw new Error('Paid/ticketed event without exported price must use the neutral “Билеты” destination copy');
}
if (
  tretyakovEvent.ticket.is_free
  || /event-hero__eyebrow[^>]*>[^<]*Бесплатно/u.test(tretyakovVisibleHtml)
  || /event-info-admission[\s\S]{0,180}Бесплатно/u.test(tretyakovVisibleHtml)
) throw new Error('Event 5370 must remain paid/ticketed in the preview fixture, not inherit the false-free source merge');
const tretyakovJsonLd = [...tretyakovHtml.matchAll(/<script type="application\/ld\+json"[^>]*>([\s\S]*?)<\/script>/giu)].map((match) => JSON.parse(match[1])).find((item) => typeof item['@type'] === 'string' && item['@type'].endsWith('Event'));
if (tretyakovJsonLd?.isAccessibleForFree !== Boolean(tretyakovEvent.ticket.is_free)) throw new Error('Event 5370 JSON-LD must expose the same free/paid state as exported production data');
if (!Array.isArray(tretyakovJsonLd?.image) || tretyakovJsonLd.image.length < 5) throw new Error('Event 5370 JSON-LD must connect gallery images to the event for SEO/GEO');
if (!tretyakovHtml.includes(`Фото ${tretyakovEvent.image_assets.length}`)) throw new Error('Event 5370 hero must show photo count CTA on the image');
const warriorEvent = eventsData.events.find((event) => event.id === 698);
if (!warriorEvent) throw new Error('Missing event 698 gallery regression event');
const warriorHtml = readFileSync(join(root, `sobytiya/${warriorEvent.slug}/index.html`), 'utf8');
const warriorVisibleHtml = stripGeneratedCode(warriorHtml);
const warriorEyebrow = (warriorVisibleHtml.match(/<p class="event-hero__eyebrow">([^<]*)<\/p>/u) || [null, ''])[1];
if (/Платный вход|Билеты/u.test(warriorEyebrow)) throw new Error('Event 698 hero eyebrow must not expose paid/generic admission copy above the title');
if (/data-gallery-src="https:\/\/files\.catbox\.moe/iu.test(warriorHtml)) throw new Error('Hero fullscreen gallery must not emit unreliable catbox slides; mirror or skip them before publishing');
const jsonLdItems = [...controlHtml.matchAll(/<script type="application\/ld\+json"[^>]*>([\s\S]*?)<\/script>/giu)].map((match) => JSON.parse(match[1]));
if (!jsonLdItems.some((item) => typeof item['@type'] === 'string' && item['@type'].endsWith('Event'))) throw new Error('Control page must contain parseable Event-class JSON-LD');
if (!jsonLdItems.some((item) => item['@type'] === 'BreadcrumbList')) throw new Error('Control page must contain parseable BreadcrumbList JSON-LD');
const eventJsonLd = jsonLdItems.find((item) => typeof item['@type'] === 'string' && item['@type'].endsWith('Event'));
if (eventJsonLd?.offers?.validFrom && !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/u.test(eventJsonLd.offers.validFrom)) {
  throw new Error(`Event JSON-LD offer validFrom must be ISO 8601 with timezone, got: ${eventJsonLd.offers.validFrom}`);
}
if (!controlHtml.includes('ke_like_share_prompt_count_v1')) throw new Error('Control page misses post-like share prompt limiter');
for (const runtimeMarker of ['ke_calendar_saved_v1', 'CALENDAR_STATE_LIMIT = 256', 'data-calendar-action', 'triggerCalendarDownload', 'syncCalendarControls']) {
  if (!controlHtml.includes(runtimeMarker)) throw new Error(`Shared calendar acknowledgement runtime misses ${runtimeMarker}`);
}
if (!controlHtml.includes('data-reset-personalization') || !controlHtml.includes('Персонализация сброшена')) throw new Error('Control page misses technical local personalization reset button/handler');
if (!controlHtml.includes('anchorEventId')) throw new Error('Control page misses stable anchored rerank logic');
if (!controlHtml.includes('sessionPinnedNotInterested')) throw new Error('Control page misses current-page not-interested plate persistence');
if (!controlHtml.includes('data-discovery-feed') || controlHtml.includes('data-personalized-feed') || !controlHtml.includes('data-discovery-src') || !controlHtml.includes('data-discovery-load-more') || !controlHtml.includes('hydrateDiscoveryFeeds')) throw new Error('Control page misses static-10 + personalization JSON discovery hydration contract');
if (!controlHtml.includes('ke_personalization_profile') || controlHtml.includes('ke_profile_id_v1') || controlHtml.includes('anon-${')) throw new Error('Control page must use compatible UUID personalization profile, not legacy/prefixed ids');
if (!controlHtml.includes('isCompatibleProfile') || !controlHtml.includes('rankEventDetailRelated') || !controlHtml.includes('served_list_id') || !controlHtml.includes('createServedListSummary')) throw new Error('Control page misses event_detail_related local-rerank/served-list contract');
if (!controlHtml.includes('event_detail_related') || !controlHtml.includes('local_related_rerank_v1_fallback')) throw new Error('Control page misses event_detail_related surface/algorithm markers');
if (!controlHtml.includes('/favicon.svg') || !controlHtml.includes('sizes="any"')) throw new Error('Control page misses scalable favicon link');
const faviconSvg = readFileSync(join(root, 'favicon.svg'), 'utf8');
if (!faviconSvg.includes('viewBox="0 0 64 64"') || !faviconSvg.includes('announcements-tag') || !faviconSvg.includes('announcements-wide-o') || !faviconSvg.includes('#98401f') || !faviconSvg.includes('fill="#fff"') || faviconSvg.includes('<rect') || /<image\b|data:image\//iu.test(faviconSvg)) throw new Error('Favicon must use the transparent terracotta tag with square top, rounded bottom and one white wide-o vector');
const announcementsWordmarkSource = readFileSync(join(siteDir, 'public/brand/announcements-wordmark-ui.svg'), 'utf8');
if ((announcementsWordmarkSource.match(/<path\b/gu) || []).length !== 1 || announcementsWordmarkSource.includes('transform=') || Buffer.byteLength(announcementsWordmarkSource) >= 2048 || !announcementsWordmarkSource.includes('fill="currentColor"')) throw new Error('Announcements wordmark must remain one compact currentColor compound path without transforms');
const footerSocialUrls = [
  'https://t.me/kenigevents',
  'https://t.me/kldevents',
  'https://vk.com/kenigeventsofficial',
  'https://vk.com/klgdevents',
  'https://vk.ru/im/channels/-239844596',
  'https://max.ru/channel_kenigevents',
];
for (const url of footerSocialUrls) {
  if (!controlHtml.includes(url)) throw new Error(`Footer social URL missing: ${url}`);
}
if (!controlHtml.includes('mailto:info@kenigevents.ru')) throw new Error('Footer contact email missing');
for (const cls of ['site-footer__social', 'social-icon--telegram', 'social-icon--vk', 'social-icon--max']) {
  if (!controlHtml.includes(cls)) throw new Error(`Footer social icon/class missing: ${cls}`);
}
if (!controlHtml.includes('data-prefetch')) throw new Error('Control page misses fast-navigation prefetch markers');
if (!controlHtml.includes('data-sticky-cta') || !controlHtml.includes('data-hide-sticky-after')) throw new Error('Control page misses sticky CTA feed-hide markers');
if (!controlHtml.includes('Смотрите дальше')) throw new Error('Control page misses single discovery feed heading');
if (controlVisibleHtml.includes('Preview A/B:') || controlVisibleHtml.includes('В HTML сразу предзагружены')) throw new Error('Normal event page must not expose preview/A-B/debug discovery copy');
if (controlHtml.includes('Похожие события') || controlHtml.includes('Попробовать другое') || controlHtml.includes('Открыть новое')) throw new Error('Control page still exposes split/exploration labels instead of one neutral discovery feed');
if (controlHtml.includes('Уточнить регистрацию')) throw new Error('Ambiguous registration CTA leaked');
if (controlVisibleHtml.includes('Telegraph')) throw new Error('Event pages must not expose Telegraph link as a user-facing source');
if (controlVisibleHtml.includes('Просмотры в источниках') || controlVisibleHtml.includes('Источники')) throw new Error('Source count/views are gated and must not be shown in public compact facts');
if (!controlVisibleHtml.includes('Все источники, упоминания и расширенная статистика события будут доступны зарегистрированным пользователям')) throw new Error('Event page must show registered-user gate notice for sources/mentions/extended stats');
if (!controlVisibleHtml.includes('event-info-block') || !controlVisibleHtml.includes('event-info-item__icon')) throw new Error('Compact facts must render icon-based event info block');
const detailsStart = controlVisibleHtml.indexOf('section-card--event-details');
const factsStart = controlVisibleHtml.indexOf('class="event-info-block"', detailsStart);
const sourceGateStart = controlVisibleHtml.indexOf('event-source-gate--section', detailsStart);
if (detailsStart < 0 || factsStart < detailsStart || sourceGateStart < factsStart) throw new Error('Source/mentions auth gate must belong to the parent details section after compact facts and optional transport');
if (controlVisibleHtml.includes('event-hero__facts')) throw new Error('Hero must not duplicate the compact facts block as a second info block');
if (controlHtml.includes('class="share-list"')) throw new Error('Duplicate share-list UI leaked');
if (/download="kenigevents-/u.test(controlHtml)) throw new Error('Calendar links still force download instead of opening .ics');
if (controlHtml.includes('cards-grid--feed')) throw new Error('Control page still uses horizontal related rail class');
if (controlHtml.includes('<details class="details-disclosure"')) throw new Error('Control description is hidden in a details disclosure');
if (controlHtml.includes('11 июля 2026')) throw new Error('Visible current-year date should omit year in event UI');
const discoveryJson = JSON.parse(readFileSync(join(root, `data/discovery/${control.id}.json`), 'utf8'));
if (discoveryJson.preload_target !== 10 || discoveryJson.page_size !== 10) throw new Error('Discovery JSON must declare 10-item preload/page contract');
if (discoveryJson.schema_version !== 'event-detail-related-v1' || discoveryJson.feature_schema_version !== 'event-detail-related-v1') throw new Error('Discovery JSON must use event-detail-related schema contract');
if (discoveryJson.taxonomy_version !== 'event-taxonomy-v1' || discoveryJson.surface !== 'event_detail_related' || !['static_related_v1', 'event_sparse_related_chain_v1', 'event_pgvector_related_chain_v1', 'event_pgvector_related_chain_v2_two_doc'].includes(discoveryJson.algorithm_id)) throw new Error('Discovery JSON misses surface/taxonomy/algorithm contract');
if (discoveryJson.algorithm_id === 'event_sparse_related_chain_v1' && (discoveryJson.strategy !== 'event_sparse_related_chain_v1_manifest' || !discoveryJson.related_static.some((item) => item.slot_type && 'lexical_similarity' in item))) throw new Error('Sparse related chain must surface honest lexical candidate evidence and slot_type in static manifests');
if (
  (discoveryJson.algorithm_id === 'event_pgvector_related_chain_v1' || discoveryJson.algorithm_id === 'event_pgvector_related_chain_v2_two_doc')
  && (
    !['event_pgvector_related_chain_v1_manifest', 'event_pgvector_related_chain_v2_manifest'].includes(discoveryJson.strategy)
    || !discoveryJson.related_static.some((item) => item.slot_type && 'vector_similarity' in item)
  )
) throw new Error('pgvector related chain must surface semantic vector evidence and slot_type in static manifests');
if (!discoveryJson.current_event || discoveryJson.current_event.event_id !== control.id) throw new Error('Discovery JSON must include current_event summary');
if (!Array.isArray(discoveryJson.related_static) || discoveryJson.related_static.length < 5) throw new Error('Discovery JSON must contain related_static candidate manifest for light client hydration');
if ('events' in discoveryJson) throw new Error('Discovery JSON must expose related_static manifest, not legacy events payload');
for (const item of discoveryJson.related_static) {
  if (item.event_id === control.id) throw new Error('Discovery JSON must not include current event');
  for (const field of ['event_id', 'title', 'category', 'tags', 'audience_exclusion_tags', 'status', 'lifecycle_status', 'base_similarity', 'reason_codes', 'display']) {
    if (!(field in item)) throw new Error(`Discovery candidate missing ${field}`);
  }
  for (const field of ['calendar_href', 'calendar_eligible']) {
    if (!(field in item.display)) throw new Error(`Discovery candidate display missing ${field}`);
  }
  if (item.display.image_url && (!(Number(item.display.image_width) > 0) || !(Number(item.display.image_height) > 0))) {
    throw new Error(`Discovery candidate ${item.event_id} misses intrinsic media geometry required by the row optimizer`);
  }
  if (!Array.isArray(item.tags) || !Array.isArray(item.audience_exclusion_tags) || !Array.isArray(item.reason_codes)) throw new Error('Discovery candidate tag/reason fields must be arrays');
  if (typeof item.base_similarity !== 'number' || item.base_similarity < 0 || item.base_similarity > 1) throw new Error('Discovery candidate base_similarity must be 0..1');
  if (/2026\b/u.test(item.display?.display_date || '') && !/2027\b/u.test(item.display?.display_date || '') && String(item.date || '').startsWith('2026-')) throw new Error('Discovery JSON display dates should omit current year unless crossing year');
}

const splitControl = eventsData.events.find((event) => event.id === 6322);
const splitHtml = splitControl ? readFileSync(join(root, `sobytiya/${splitControl.slug}/index.html`), 'utf8') : controlHtml;
const splitVisibleHtml = stripGeneratedCode(splitHtml);
if (!splitVisibleHtml.includes('data-feed-card-variant="split-actions"')) throw new Error('Split-actions baseline must render split-actions cards');
if (!splitVisibleHtml.includes('event-card__utility-row')) throw new Error('Split-actions page misses utility row inside cards');
if (!splitVisibleHtml.includes('event-card__feedback event-card__feedback--under')) throw new Error('Split-actions page misses under-card action row');
if (!splitVisibleHtml.includes('feedback-button--calendar')) throw new Error('Split-actions page must expose feed calendar buttons for one-day eligible cards');
if (!splitHtml.includes('data-feed-card-variant="split-actions"')) throw new Error('Split-actions page must keep variant marker for hydrated JSON cards');
if (splitControl && (!splitHtml.includes('icon--phone') || !splitHtml.includes('M14.05 6c.98.19'))) throw new Error('Phone CTA must use a clear vector phone/call icon');
if (controlVisibleHtml.includes('>Sitemap</a>')) throw new Error('Sitemap must not be exposed in user-facing event navigation');
const pushkinEvent = eventsData.events.find((event) => event.id === 4913);
if (pushkinEvent) {
  const pushkinHtml = readFileSync(join(root, `sobytiya/${pushkinEvent.slug}/index.html`), 'utf8');
  if (!pushkinHtml.includes('event-info-chip--pushkin') || !pushkinHtml.includes('✓') || pushkinHtml.includes('>возможна</dd>')) throw new Error('Pushkin card fact must render as a compact admission property check mark, not value copy');
}
const freeEvent = eventsData.events.find((event) => event.id === 4512);
if (freeEvent) {
  const freeHtml = readFileSync(join(root, `sobytiya/${freeEvent.slug}/index.html`), 'utf8');
  if (!freeHtml.includes('Бесплатно') || !freeHtml.includes('вход свободный') || /<dd[^>]*>\s*Бесплатно\s*<\/dd>/u.test(freeHtml)) throw new Error('Free admission must keep the word “Бесплатно” and render the free-entry subtype, not bare value copy');
}
if (!/event-card__utility-row[\s\S]*feedback-button--negative/u.test(splitVisibleHtml) || !/event-card__feedback event-card__feedback--under[\s\S]*feedback-button--share[\s\S]*feedback-button--like/u.test(splitVisibleHtml)) throw new Error('Split-actions must keep not-interested in the card utility row and share/like in the under-card row');

const ics = readFileSync(join(root, `sobytiya/${control.slug}/event.ics`), 'utf8');
for (const needle of ['BEGIN:VCALENDAR', 'VERSION:2.0', 'BEGIN:VEVENT', 'DTSTART:20260711T193000Z', 'SUMMARY:Песни СССР', 'END:VCALENDAR']) {
  if (!ics.includes(needle)) throw new Error(`Control ICS missing ${needle}`);
}
if (/^DTEND:/m.test(ics)) throw new Error('Control ICS must not include DTEND without reliable duration');
for (const event of eventsData.events) {
  if (!Number.isInteger(event.likes_count) || event.likes_count < 0) throw new Error(`Event ${event.id} has invalid likes_count`);
  if (!Number.isInteger(event.source_likes_count) || event.source_likes_count < 0) throw new Error(`Event ${event.id} has invalid source_likes_count`);
  if (!Number.isInteger(event.service_likes_count) || event.service_likes_count < 0) throw new Error(`Event ${event.id} has invalid service_likes_count`);
  if (event.likes_count !== event.source_likes_count + event.service_likes_count) throw new Error(`Event ${event.id} likes_count must equal source_likes_count + service_likes_count`);
  if (!['ocr_text', 'visual_only', 'unknown'].includes(event.image_text_mode)) throw new Error(`Event ${event.id} has invalid image_text_mode`);
  const eventIcs = readFileSync(join(root, `sobytiya/${event.slug}/event.ics`), 'utf8');
  for (const needle of ['BEGIN:VCALENDAR', 'BEGIN:VEVENT', 'UID:', 'SUMMARY:', 'URL:', 'END:VCALENDAR']) {
    if (!eventIcs.includes(needle)) throw new Error(`ICS for ${event.id} missing ${needle}`);
  }
  if (!/^DTSTART(?:;VALUE=DATE)?:/m.test(eventIcs)) throw new Error(`ICS for ${event.id} missing DTSTART`);
}
const robots = readFileSync(join(root, 'robots.txt'), 'utf8').trim();
if (robots !== 'User-agent: *\nDisallow: /') throw new Error(`Unexpected robots.txt: ${robots}`);
const sitemap = readFileSync(join(root, 'sitemap.xml'), 'utf8');
if (!sitemap.includes(`https://kenigevents.ru/${buildId}/__preview/`)) throw new Error('Sitemap misses preview index URL');
if (!sitemap.includes(`https://kenigevents.ru/${buildId}/segodnya/`)) throw new Error('Sitemap misses today listing URL');
if (!sitemap.includes(`https://kenigevents.ru/${buildId}/zavtra/`)) throw new Error('Sitemap misses tomorrow listing URL');
if (!sitemap.includes(`https://kenigevents.ru/${buildId}/vyhodnye/`)) throw new Error('Sitemap misses weekend listing URL');
if (!sitemap.includes(`https://kenigevents.ru/${buildId}/vystavki/`)) throw new Error('Sitemap misses exhibitions listing URL');
if (!sitemap.includes(`https://kenigevents.ru/${buildId}/populyarnoe/`)) throw new Error('Sitemap misses popular listing URL');
if (!sitemap.includes(`https://kenigevents.ru/${buildId}/poisk/`)) throw new Error('Sitemap misses authorized search URL');
if (!sitemap.includes(`https://kenigevents.ru/${buildId}/partnerstvo/`)) throw new Error('Sitemap misses partnership URL');
if (!sitemap.includes(`https://kenigevents.ru/${buildId}/partners/`)) throw new Error('Sitemap misses info partners URL');
if (!sitemap.includes(`https://kenigevents.ru/${buildId}/sobytiya/${control.slug}/`)) throw new Error('Sitemap misses control event URL');
if (!sitemap.includes(`https://kenigevents.ru/${buildId}/lab/hero/`)) throw new Error('Sitemap misses hero lab URL');
if (!sitemap.includes(`https://kenigevents.ru/${buildId}/lab/hero/review/`)) throw new Error('Sitemap misses hero viewport review URL');
if (!sitemap.includes(`https://kenigevents.ru/${buildId}/lab/hero/review/5878-poster-billboard/`)) throw new Error('Sitemap misses same-event hero review case URL');


const searchHtml = readFileSync(join(root, 'poisk/index.html'), 'utf8');
const searchVisibleHtml = stripGeneratedCode(searchHtml);
if (!/data-search-skeletons[^>]*\shidden(?:\s|>)/u.test(searchHtml)) throw new Error('Search skeleton must be initially hidden until a real request starts');
if (searchHtml.includes('data-product-breadcrumbs') || searchHtml.includes('data-product-parent-link')) throw new Error('Top-level Search must not render decorative breadcrumbs');
const mobileCalendarBase = String(process.env.PUBLIC_MOBILE_CALENDAR_BASE_URL || '').replace(/\/+$/u, '');
const mobileSearchBase = String(process.env.PUBLIC_MOBILE_SEARCH_BASE_URL || '').replace(/\/+$/u, '');
if (mobileCalendarBase || mobileSearchBase) {
  if (!mobileCalendarBase || !mobileSearchBase) throw new Error('Cross-preview mobile navigation requires both calendar and Search base URLs');
  for (const expectedHref of [
    `${mobileCalendarBase}/populyarnoe/`,
    `${mobileCalendarBase}/segodnya/`,
    `${mobileCalendarBase}/dlya-menya/`,
    `${mobileSearchBase}/poisk/`,
  ]) {
    if (!searchHtml.includes(`href="${expectedHref}"`)) throw new Error(`Search mobile navigation misses composed destination: ${expectedHref}`);
  }
}
if (!searchHtml.includes('data-authorized-search') || !searchHtml.includes('data-search-login') || !searchHtml.includes('custom:yandex') || !searchHtml.includes('data-supabase-url')) throw new Error('Authorized search page must render Yandex/Supabase search UI when public env is provided');
const bundledJs = readdirSync(join(root, '_astro')).filter((name) => name.endsWith('.js')).map((name) => readFileSync(join(root, '_astro', name), 'utf8')).join('\n');
if (!bundledJs.includes('flowType:"pkce"') || !bundledJs.includes('detectSessionInUrl:!1') || !bundledJs.includes('exchangeCodeForSession') || !bundledJs.includes('error_description') || !/searchParams\.delete\([^)]*\)/u.test(bundledJs) || !bundledJs.includes('hash=""')) throw new Error('Authorized search must use PKCE OAuth and clean same-page redirect URLs before Yandex login');
const bundledCss = readdirSync(join(root, '_astro')).filter((name) => name.endsWith('.css')).map((name) => readFileSync(join(root, '_astro', name), 'utf8')).join('\n');
if (!/\[hidden\][^{]*\{[^}]*display:\s*none\s*!important/iu.test(bundledCss)) throw new Error('Authorized search build must include a strong hidden rule so unauthenticated form/results/buttons stay hidden');
if (!searchVisibleHtml.includes('authorized-search__yandex-icon') || !searchVisibleHtml.includes('>Я</span>') || !searchVisibleHtml.includes('Войти через Яндекс')) throw new Error('Authorized search login button must expose recognizable Yandex branding/icon text');
if (searchVisibleHtml.includes('Пока без запроса') || searchVisibleHtml.includes('cards-grid') || /<article class="event-card/u.test(searchVisibleHtml)) throw new Error('Dedicated search page must not show prefilled static result cards before a query');
if (!searchVisibleHtml.includes('Поисковые теги') || !bundledJs.includes('ke_search_feedback_queue_v1') || !bundledJs.includes('record_event_search_feedback_v1')) throw new Error('Search page must include feedback/tag candidate UX and RPC wiring');
if (!bundledJs.includes('stream_stalled') || !bundledJs.includes('stream_rescue') || !bundledJs.includes('Поток прогресса не дошёл до браузера')) throw new Error('Authorized search must include mobile stream-stall JSON rescue fallback');
if (!bundledJs.includes('mobile_document_decode_natural') || !bundledJs.includes('naturalAspectReconciled')) throw new Error('Authorized search must reconcile missing document geometry after image decode');
if (!controlHtml.includes('/poisk/')) throw new Error('Mobile/desktop navigation must expose the authorized search page link');
if (!controlHtml.includes('/vystavki/') || !controlHtml.includes('/populyarnoe/') || !controlHtml.includes('/partnerstvo/')) throw new Error('Navigation must expose exhibitions, popular and partnership pages');
if (!controlHtml.includes('/partners/') || !controlHtml.includes('Партнёры')) throw new Error('Footer/mobile navigation must expose the Partners page link');

const partnersHtml = readFileSync(join(root, 'partners/index.html'), 'utf8');
const partnersVisibleHtml = stripGeneratedCode(partnersHtml);
for (const needle of ['Партнёры Полюбить Калининград Анонсы', 'КППК', 'Знание', '80 историй', 'Кантата', 'Акт Опус', 'ИЦАЭ Калининграда']) {
  if (!partnersVisibleHtml.includes(needle)) throw new Error(`Info partners page misses ${needle}`);
}
for (const needle of [
  'Партнёры Полюбить Калининград Анонсы',
  'АО «КППК»',
  'Просветительский фестиваль к 80-летию Калининградской области',
  'Образовательная программа фестиваля',
]) {
  if (!partnersVisibleHtml.includes(needle)) throw new Error(`Info partners page misses required heading/caption: ${needle}`);
}
for (const staleLabel of ['Пригородные маршруты', 'Просветительские события', 'Фестиваль 80-летия', 'Лекции и музыка', 'Театр и премьеры', 'КППК / РЖД', 'Информационный партнёр просветительских событий', 'Информационный партнёр театральной афиши', 'партнёр по образовательной программе']) {
  if (partnersVisibleHtml.includes(staleLabel)) throw new Error(`Info partners page must not render stale/category/wrong caption: ${staleLabel}`);
}
if (!partnersHtml.includes('/assets/partners/kppk-rzd-red.svg')) throw new Error('Info partners КППК tile must use the sourced RZD/KPPK mark, not an invented text-only logo');
const expectedPartnerUrls = ['https://www.kppk39.ru/', 'https://znanierussia.ru/', 'https://kgd80.ru/', 'https://kantatafest.ru/obrazovatelnaya-programma', 'https://actop.us/plays', 'https://klgd.myatom.ru/'];
for (const url of expectedPartnerUrls) {
  if (!partnersHtml.includes(url)) throw new Error(`Info partners page misses partner URL: ${url}`);
}
const renderedPartnerUrls = [...partnersHtml.matchAll(/<a class="partner-tile[^"]*"[^>]+href="(https?:\/\/[^"]+)"/giu)].map((match) => match[1]).sort();
const expectedSortedPartnerUrls = [...expectedPartnerUrls].sort();
if (JSON.stringify(renderedPartnerUrls) !== JSON.stringify(expectedSortedPartnerUrls)) throw new Error(`Info partners page must render exactly the approved partner URL set, got ${JSON.stringify(renderedPartnerUrls)}`);
if (!/rel="nofollow noopener noreferrer"/u.test(partnersHtml)) throw new Error('Info partners external links must be nofollow/noopener/noreferrer');
if (!partnersHtml.includes('class="partner-tile ') || !partnersHtml.includes('partner-tile__logo')) throw new Error('Info partners page must render compact full-tile partner links');
if (partnersHtml.includes('partner-card') || partnersVisibleHtml.includes('Сайт партнёра')) throw new Error('Info partners page must not render oversized partner cards or separate site CTA copy');
const compactPartnersCss = bundledCss.replace(/\s+/gu, '');
if (/\.partner-tile(?:\[[^\]]+\])?\{[^{}]*(box-shadow|border:|background:)/u.test(compactPartnersCss) || compactPartnersCss.includes('radial-gradient(circleat16%0%')) throw new Error('Info partners must stay a flat logo board without heavy card borders/backgrounds/shadows');
if (!compactPartnersCss.includes('grid-template-columns:repeat(4,minmax(0,1fr))')) throw new Error('Info partners mobile layout must keep a compact four-column bento grid');
if (!compactPartnersCss.includes('@media(min-width:980px)') || !compactPartnersCss.includes('grid-template-columns:repeat(8,minmax(0,1fr))')) throw new Error('Info partners desktop layout must keep an eight-column aspect-aware grid');
if (!partnersHtml.includes('--partner-col-start: 1') || !partnersHtml.includes('--partner-col-span: 4') || !partnersHtml.includes('--partner-row-span: 2') || !partnersHtml.includes('--partner-mobile-col-start: 3') || !partnersHtml.includes('--partner-mobile-col-span: 2')) throw new Error('Info partners tiles must keep explicit bento placement variables for greedy logo spans');
const exhibitionsHtml = readFileSync(join(root, 'vystavki/index.html'), 'utf8');
if (!exhibitionsHtml.includes('data-exhibitions-prototype') || !exhibitionsHtml.includes('data-mode-switch') || !exhibitionsHtml.includes('Новое для вас') || !exhibitionsHtml.includes('Стоит увидеть')) throw new Error('Exhibitions listing must use the accepted integrated personal presentation');
if (exhibitionsHtml.includes('listing-stack') || exhibitionsHtml.includes('data-product-breadcrumbs')) throw new Error('Exhibitions route regressed to the retired listing or decorative top-level breadcrumbs');
const popularListingHtml = readFileSync(join(root, 'populyarnoe/index.html'), 'utf8');
if (!popularListingHtml.includes('Популярное') || !popularListingHtml.includes('listing-stack')) throw new Error('Popular listing must exist as a separate section/page');
const partnershipHtml = readFileSync(join(root, 'partnerstvo/index.html'), 'utf8');
if (!partnershipHtml.includes('Стать партнёром') || !partnershipHtml.includes('Ласточка')) throw new Error('Partnership page must keep the current reference/test block');

const todayHtml = readFileSync(join(root, 'segodnya/index.html'), 'utf8');
if (/Мосийенко|Мосиенко/u.test(todayHtml)) throw new Error('Today listing must not show the false long-range Evgeny Mosiyenko lecture/exhibition item');
if (!todayHtml.includes('listing-daypart--continuing') || !todayHtml.includes('Идут сейчас')) throw new Error('Today listing must separate continuing multi-day exhibitions when they would overcrowd the fast daypart list');
const tomorrowHtml = readFileSync(join(root, 'zavtra/index.html'), 'utf8');
for (const [name, html] of [['today', todayHtml], ['tomorrow', tomorrowHtml]]) {
  for (const label of ['Утро', 'День', 'Вечер', 'Ночь']) {
    if (!html.includes(`>${label}</h3>`)) throw new Error(`${name} listing misses daypart section ${label}`);
  }
  if (!html.includes('listing-daypart') || !html.includes('listing-item__media listing-item__media--cover')) throw new Error(`${name} listing misses plaque/cropped media listing contract`);
  if (!html.includes('data-listing-filter') || !html.includes('data-listing-filter-bar') || !html.includes('listing-mode-switch') || !html.includes('role="radiogroup"') || !html.includes('data-listing-mode-button="personal"') || !html.includes('data-listing-hidden-count')) throw new Error(`${name} listing misses global All/For me personalization switch and hidden count UI`);
  const listingArticles = [...html.matchAll(/<article class="listing-item"[\s\S]*?<\/article>/giu)].map((match) => match[0]);
  if (!listingArticles.length) throw new Error(`${name} listing has no listing articles`);
  if (listingArticles.some((article) => !article.includes('data-listing-item') || !article.includes('data-linked-event-ids'))) throw new Error(`${name} listing items must expose compact ids for local personalization filter`);
  if (listingArticles.some((article) => !/<a class="listing-item__title"[\s\S]*?<div class="listing-item__meta">/u.test(article))) throw new Error(`${name} listing cards must show title before date/admission meta`);
  if (listingArticles.some((article) => {
    const hrefs = [...article.matchAll(/<a[^>]+href="(https?:\/\/[^"]+)"/giu)].map((match) => match[1]);
    return hrefs.some((href) => !href.startsWith('https://static.kenigevents.ru/ics/'));
  })) throw new Error(`${name} listing card leaks direct external http link`);
  if (listingArticles.some((article) => article.includes('Открыть пост организатора') || article.includes('Уточнить регистрацию'))) throw new Error(`${name} listing exposes source/ambiguous external CTA copy`);
}

const weekendHtml = readFileSync(join(root, 'vyhodnye/index.html'), 'utf8');
for (const [name, html] of [['today', todayHtml], ['tomorrow', tomorrowHtml], ['weekend', weekendHtml]]) {
  if (!html.includes('data-personal-feed-section') || !html.includes('data-personal-feed-slot')) throw new Error(`${name} listing misses dynamic personal-feed slot`);
  if (!html.includes('hidden') || !html.includes('Личная лента')) throw new Error(`${name} personal feed must be hidden until backend/cache returns cards`);
}
if (!controlHtml.includes('ke_listing_personal_feed_cache_v1') || !controlHtml.includes('get_listing_personal_feed_v1') || !controlHtml.includes('/rest/v1/rpc/')) throw new Error('Layout misses Supabase RPC/localStorage personal feed preparation');
if (!controlHtml.includes(`data-site-base-path="/${buildId}"`) || !controlHtml.includes('KenigEventsNormalizeInternalEventUrl') || !controlHtml.includes('rebaseInternalEventUrl')) throw new Error('Layout misses current-preview dynamic event-link rebasing contract');
if (!controlHtml.includes('cache.base_path !== CURRENT_BASE_PATH') || !controlHtml.includes('base_path: CURRENT_BASE_PATH') || !controlHtml.includes('ke_listing_personal_feed_cache_v1:${CURRENT_BASE_PATH')) throw new Error('Personal-feed cache must be scoped to and validated against the current preview base');
const authorizedSearchSource = readFileSync(join(siteDir, 'src/components/AuthorizedEventSearch.astro'), 'utf8');
if (!authorizedSearchSource.includes('currentPreviewEventUrl(display.href || candidate.href') || !authorizedSearchSource.includes('href: currentPreviewEventUrl(display.href || candidate?.href')) throw new Error('Authorized search must rebase both final and vector-preview event links to the current preview');
if (!controlHtml.includes('ke_listing_mode_v1') || !controlHtml.includes('syncListingPersonalFilter') || !controlHtml.includes('data-listing-hidden-count') || !controlHtml.includes("explicitMode || (hiddenCount > 0 ? 'personal' : 'all')") || !controlHtml.includes('hydrateListingFilterFooterGuard')) throw new Error('Layout misses local listing personalization switch/hide/footer-guard contract');
const assetBaseUrl = (process.env.PUBLIC_ASSET_BASE_URL || '').replace(/\/+$/u, '');
const icsBaseUrl = (process.env.PUBLIC_ICS_BASE_URL || (assetBaseUrl ? `${assetBaseUrl}/ics` : '')).replace(/\/+$/u, '');
const astroAssetBaseUrl = (process.env.PUBLIC_ASTRO_ASSET_BASE_URL || '')
  .replace(/\{buildId\}/g, buildId)
  .replace(/\{BUILD_ID\}/g, buildId)
  .replace(/\/+$/u, '');
if (assetBaseUrl) {
  if (controlHtml.includes('https://storage.yandexcloud.net/kenigevents/')) throw new Error('CDN-enabled HTML must not emit raw Object Storage image URLs');
  if (!controlHtml.includes(`${assetBaseUrl}/p/`)) throw new Error('CDN-enabled event HTML must emit event image URLs through PUBLIC_ASSET_BASE_URL');
  if (!JSON.stringify(eventJsonLd?.image || []).includes(`${assetBaseUrl}/p/`)) throw new Error('CDN-enabled JSON-LD Event.image must use PUBLIC_ASSET_BASE_URL');
  if (controlHtml.includes(`rel="canonical" href="${assetBaseUrl}`)) throw new Error('Canonical URL must remain on kenigevents.ru, not asset CDN');
  if (!controlHtml.includes(`href="${icsBaseUrl}/${control.id}.ics"`)) throw new Error('CDN-enabled pages must link calendar CTA to stable /ics/<event_id>.ics');
}
if (astroAssetBaseUrl) {
  if (!controlHtml.includes(`href="${astroAssetBaseUrl}/_astro/`)) throw new Error('Astro CSS/JS assets must use PUBLIC_ASTRO_ASSET_BASE_URL when enabled');
  if (controlHtml.includes(`rel="canonical" href="${astroAssetBaseUrl}`)) throw new Error('Canonical URL must remain on kenigevents.ru, not static asset CDN');
  if (!assetBaseUrl && controlHtml.includes(`${astroAssetBaseUrl}/p/`)) throw new Error('PUBLIC_ASTRO_ASSET_BASE_URL must not rewrite event media images; use PUBLIC_ASSET_BASE_URL only for a media CDN');
}
const cssFiles = readdirSync(join(root, '_astro')).filter((name) => name.endsWith('.css'));
const css = cssFiles.map((name) => readFileSync(join(root, '_astro', name), 'utf8')).join('\n');
if (/native-share-button\{display:none/u.test(css)) throw new Error('Native share button is hidden by default');
if (/media-backdrop|image-backdrop|--poster-image|background-image:\s*linear-gradient\([^;]*var\(--poster-image\)|blur\(/u.test(css)) throw new Error('Duplicate/backdrop poster fill leaked into CSS');
if (/event-card__media-shell--document[^}]*object-fit:\s*contain/iu.test(css)) throw new Error('Document card media must not use contain over a fixed frame');
if (!/#discovery-feed\{[^}]*width:\s*100vw[^}]*margin-inline:\s*calc\(50%\s*-\s*50vw\)[^}]*box-sizing:\s*border-box/iu.test(css)) throw new Error('Desktop related-event surface must be true full-bleed without creating horizontal document overflow');
if (!/#discovery-feed \.event-card__media-shell--document \.event-card__media\{[^}]*left:\s*0[^}]*width:\s*100%[^}]*height:\s*auto/iu.test(css)) throw new Error('Related document/poster cards must preserve the complete source width and clip only vertical overflow');
if (!/event-hero--poster-stage \.event-hero__image\{[^}]*object-fit:\s*contain/iu.test(css)) throw new Error('Poster-stage hero must contain OCR/text posters without crop');
if (!/event-hero--photo-cover \.event-hero__image\{[^}]*object-fit:\s*cover/iu.test(css)) throw new Error('Photo-cover hero must crop only visual-safe images');
if (!/event-hero--poster-billboard[\s\S]*?event-hero__visual[\s\S]*?width:\s*100vw/iu.test(css)) throw new Error('Poster billboard hero must make the hero visual full viewport width on mobile');
if (!/event-hero--poster-billboard\.event-hero--poster-stage \.event-hero__image[\s\S]*?\{[^}]*width:\s*100vw/iu.test(css)) throw new Error('Poster billboard hero image itself must be full viewport width on mobile');
if (!/mobile-discovery-menu__summary\{[^}]*background:\s*#98401f/iu.test(css) || !/body\.hero-chrome-immersive\.is-past-hero \.site-header/iu.test(css)) throw new Error('Immersive mobile header must use the approved solid brand tag and stable after-hero state');
if (
  !/mobile-discovery-menu\{[^}]*--drawer-rail-h[^}]*position:\s*fixed[^}]*transform:\s*translate3d\(0,\s*calc\(-1\s*\*\s*var\(--drawer-rail-h\)\s*-\s*env\(safe-area-inset-top\)\),\s*0\)/iu.test(css)
  || !/mobile-discovery-menu\[open\]\{[^}]*transform:\s*(?:translate3d\(0,\s*0,\s*0\)|translateZ\(0\))/iu.test(css)
  || !/mobile-discovery-menu__summary\{[^}]*top:\s*calc\(var\(--drawer-rail-h\)\s*\+\s*env\(safe-area-inset-top\)\s*-\s*7px\)/iu.test(css)
  || !/mobile-discovery-menu__panel\{[^}]*position:\s*absolute[^}]*width:\s*100vw[^}]*transform:\s*translateZ\(0\)[^}]*visibility:\s*hidden/iu.test(css)
  || !/@starting-style/iu.test(css)
  || !controlHtml.includes('closeMenu')
) throw new Error('Mobile discovery navigation must be a monolithic drawer: one root object slides down/up, with the handle attached to the panel and no transitional gap');
if (!/mobile-discovery-menu__panel\{[^}]*border-radius:\s*0/iu.test(css) || !/mobile-discovery-menu__links a\{[^}]*border:\s*0[^}]*border-radius:\s*0[^}]*background:\s*transparent/iu.test(css)) throw new Error('Mobile discovery drawer menu must be a flat rail with plain text links, not rounded/pill buttons');
if (/mobile-discovery-menu__brand-icon|brand-icon-mask|mobile-brand-icon-cycle|mobile-brand-text-cycle/iu.test(css)) throw new Error('Rejected brand icon animation must not remain in mobile discovery CSS');
if (/mobile-brand-title-sway|--brand-sway-x|hydrateMobileBrandSway/iu.test(`${css}\n${controlHtml}`)) throw new Error('Approved mobile brand lockup must remain static rather than sway inside the tag');
if (!/mobile-discovery-menu__lockup\{[^}]*grid-template-rows:\s*18px auto[^}]*gap:\s*6px[^}]*width:\s*104px/iu.test(css) || !/mobile-discovery-menu__summary\{[^}]*width:\s*8rem[^}]*min-height:\s*calc\(6rem\+env\(safe-area-inset-top\)\)/iu.test(css.replace(/\s+/g, ''))) throw new Error('Mobile discovery tag must preserve the approved 128×96 optical lockup geometry');
if (!/listing-item\{[^}]*grid-template-columns:\s*minmax\(132px,18%\)minmax\(0,1fr\)[^}]*padding:\s*0[^}]*overflow:\s*hidden/iu.test(css.replace(/\s+/g, '')) || !/listing-item__body\{[^}]*border-left:\s*1px solid/iu.test(css) || !/listing-item__media--cover \.listing-item__image\{[^}]*object-fit:\s*cover/iu.test(css)) throw new Error('Date listing cards must use parent-level plaque media crop with a straight separator');
if (!/event-hero--photo-cinematic-sheet\.event-hero--photo-cover \.event-hero__image[\s\S]*?event-hero--photo-parallax-sheet\.event-hero--photo-cover \.event-hero__image/iu.test(css) || !controlHtml.includes('hydrateHeroParallax')) throw new Error('Hero parallax must be enabled for visual-only cinematic/parallax heroes with reduced-motion-aware hydrator');
if (
  !/--hero-parallax-y/iu.test(css)
  || !/--hero-poster-parallax-y/iu.test(css)
  || !/--hero-poster-travel/iu.test(css)
  || !controlHtml.includes('usesGapSafePosterParallax')
  || !controlHtml.includes('usesReverseGapSafePosterParallax')
  || !controlHtml.includes('usesPhotoVelocityMatchedPosterParallax')
  || !controlHtml.includes('usesPhotoContinuousPosterParallax')
  || !controlHtml.includes('referencePhotoVelocity')
  || !controlHtml.includes('usesPhotoVelocityMatchedPosterParallax ? matchedPosterProgress : progress')
  || !controlHtml.includes('continuousPosterProgress * maxOffset * 2')
  || !controlHtml.includes('const gapSafePosterTravel = Math.min(48, Math.max(36, window.innerWidth * 0.11))')
  || !controlHtml.includes('const continuousPosterTravel = Math.min(44, Math.max(32, window.innerWidth * 0.10))')
  || !controlHtml.includes(': -maxOffset + progress * maxOffset * 2')
  || controlHtml.includes('--hero-parallax-scale')
) throw new Error('Hero parallax must preserve the accepted v8 constant-scale, crop-safe continuous motion profiles without layout gaps');
if (!/hero-gallery\{[^}]*position:\s*fixed[^}]*z-index:\s*80/iu.test(css) || !/hero-gallery__image\{[^}]*height:\s*100%[^}]*object-fit:\s*contain/iu.test(css) || !controlHtml.includes('hydrateHeroGallery') || !controlHtml.includes('data-hero-gallery-next')) throw new Error('Hero fullscreen gallery must be fixed, full-height, controlled and preserve OCR/text images with the base contain mode');
if (!/hero-gallery__image\[data-image-text-mode=["']?visual_only["']?\]\{[^}]*object-fit:\s*cover/iu.test(css) || !/hero-gallery\[data-auto-pan=forward\][^}]*gallery-pan-forward/iu.test(css) || !/hero-gallery\[data-auto-pan=backward\][^}]*gallery-pan-backward/iu.test(css) || !/hero-gallery__viewport,\s*\.hero-gallery__track\{[^}]*touch-action:\s*none/iu.test(css)) throw new Error('Hero fullscreen gallery must crop visual-only photos with cover, one-way forward pan and reverse pan for manual back gestures');
if (!/@keyframes\s*gallery-pan-forward\{(?:from|0%)\{object-position:38%center\}to\{object-position:64%center\}/u.test(css.replace(/\s+/g, '')) || !/@keyframes\s*gallery-pan-backward\{(?:from|0%)\{object-position:64%center\}to\{object-position:38%center\}/u.test(css.replace(/\s+/g, ''))) throw new Error('Fullscreen gallery pan direction must be forward 38%→64% (right-to-left visual motion) and backward 64%→38%');
if (!/hero-gallery__topbar\{[^}]*padding:\s*0/iu.test(css) || !/hero-gallery__brand\{[^}]*pointer-events:\s*auto/iu.test(css)) throw new Error('Fullscreen gallery brand tag must be top-flush and clickable');
if (!/hero-gallery__event-title\{[^}]*overflow:\s*hidden[^}]*text-overflow:\s*ellipsis/iu.test(css) || !controlHtml.includes('hero-gallery__event-title')) throw new Error('Fullscreen gallery must keep the event title in the fixed top bar');
if (!tretyakovHtml.includes('data-efficient-portrait-viewer="true"') || !tretyakovHtml.includes('const pageSpan = Math.max') || !tretyakovHtml.includes("moveEfficientGallery(activeGallery, 'backward')") || !tretyakovHtml.includes("moveEfficientGallery(activeGallery, 'forward')")) throw new Error('Efficient portrait gallery must page symmetrically in both directions while keeping its multi-image viewport');
if (!controlHtml.includes('loadGalleryMedia') || !controlHtml.includes('preloadAdjacentGalleryMedia') || !controlHtml.includes('.decode().catch') || !controlHtml.includes('nextImageIndex') || !controlHtml.includes('animationend') || !controlHtml.includes('galleryPanTimer') || !controlHtml.includes('8880') || !/gallery-pan-forward 17\.9s/iu.test(css) || !controlHtml.includes('swipeSurface') || !controlHtml.includes('touchstart') || !controlHtml.includes('touchmove') || !controlHtml.includes('pointermove') || !/gallery-pan-forward/iu.test(css)) throw new Error('Hero gallery must lazy-load but pre-decode adjacent slides, keep ~40% slower pan, and auto-advance after the shorter non-dead viewing interval plus pointer/touch swipe');
if (!controlHtml.includes('data-not-interested-plate') || !/event-card__not-interest-plate/iu.test(css)) throw new Error('Not-interested feedback must keep an explicit undo plate instead of turning the card into an accidental navigation target');
if (!controlHtml.includes('collapseRankedOccurrenceFamilies') || !controlHtml.includes('occurrence_member_ids')) throw new Error('Hydrated ranked feeds must collapse the same explicit occurrence projection as static lists');
if (/100vh/u.test(css)) throw new Error('Hero CSS must not use fragile 100vh units');
if (!/event-card--split-actions \.event-card__feedback\{[^}]*justify-content:\s*flex-end/iu.test(css)) throw new Error('Split-actions under-card row must cluster share text near the right-thumb like action');
if (!/event-card--split-actions \.event-card__feedback \.feedback-button\{[^}]*background:\s*transparent[^}]*border-color:\s*transparent/iu.test(css)) throw new Error('Split-actions under-card share/like must be icon-style, not pill buttons');
if (!/event-card--split-actions \.event-card__feedback \.feedback-button--share \.share-label\{[^}]*position:\s*static/iu.test(css)) throw new Error('Split-actions share must keep visible text under the card');
if (!/listing-filter-bar\{[^}]*position:\s*fixed[^}]*bottom:\s*0/iu.test(css) || !/listing-filter-bar\.is-visible\{[^}]*display:\s*block/iu.test(css) || !/body\.is-footer-visible \.listing-filter-bar/iu.test(css) || !/listing-mode-switch button\[aria-pressed=true\]/iu.test(css.replace(/\"/g, ''))) throw new Error('Listing personalization switch must be a fixed mobile segmented switch with footer overlap guard');
if (!/aspect-ratio:4\/5/u.test(css.replace(/\s+/g, ''))) throw new Error('Visual-only cover media must use vertical 4:5 ratio');
if (/aspect-ratio:3\/4/u.test(css.replace(/\s+/g, ''))) throw new Error('Old 3:4 visual-only ratio leaked into CSS');

const eventsById = new Map(eventsData.events.map((event) => [event.id, event]));
const romeoSecond = eventsById.get(6318);
const romeoThird = eventsById.get(6586);
const romeoReviewFamily = romeoSecond?.other_date_ids?.includes(6586)
  && romeoThird?.other_date_ids?.includes(6318);
if (romeoReviewFamily) {
  const dog = eventsById.get(6408);
  if (!dog) throw new Error('Focused occurrence review requires event 6408');
  const dogDiscovery = JSON.parse(readFileSync(join(root, 'data/discovery/6408.json'), 'utf8'));
  const romeoCandidates = dogDiscovery.related_static.filter((item) => item.event_id === 6318 || item.event_id === 6586);
  if (romeoCandidates.length !== 1) throw new Error(`6408 must project one Romeo occurrence card, got ${romeoCandidates.length}`);
  if (romeoCandidates[0]?.display?.display_date_time !== '2, 3 ноября 19:00') {
    throw new Error(`6408 Romeo card has wrong compact label: ${romeoCandidates[0]?.display?.display_date_time || '(missing)'}`);
  }
  const dogHtml = readFileSync(join(root, `sobytiya/${dog.slug}/index.html`), 'utf8');
  const romeoCardCount = [...dogHtml.matchAll(/data-event-id="(?:6318|6586)"/gu)].length;
  if (romeoCardCount !== 1 || !dogHtml.includes('data-occurrence-member-ids="6318,6586"') || !dogHtml.includes('2, 3 ноября 19:00')) {
    throw new Error('6408 generated HTML must render one reciprocal Romeo card with the complete compact label');
  }
}
const currentDate = eventsData.build?.current_date;
const exactTodayEvents = eventsData.events.filter((event) => event.start_date === currentDate);
const exactTodayTypes = new Set(exactTodayEvents.map((event) => event.event_type || 'unknown'));
if (exactTodayEvents.length < 5 || exactTodayTypes.size < 4) throw new Error(`Preview fixture must include a diverse real same-day slice for /segodnya/, got ${exactTodayEvents.length} events and ${exactTodayTypes.size} types`);
const priceLinkedEvent = eventsData.events.find((event) => event.ticket?.price_label && /^https?:\/\//iu.test(event.ticket?.href || ''));
if (priceLinkedEvent) {
  const priceHtml = readFileSync(join(root, `sobytiya/${priceLinkedEvent.slug}/index.html`), 'utf8');
  if (!priceHtml.includes('event-info-admission__main') || !priceHtml.includes(priceLinkedEvent.ticket.price_label) || !/rel="[^"]*nofollow[^"]*"/iu.test(priceHtml)) throw new Error(`Priced event ${priceLinkedEvent.id} must render its price as a nofollow ticket link, not extra CTA copy`);
}
for (const event of eventsData.events) {
  const related = relatedData.related[String(event.id)] || { similar: [], explore: [] };
  const excluded = new Set([event.id, ...event.other_date_ids]);
  for (const [kind, ids] of Object.entries(related).filter(([, value]) => Array.isArray(value))) {
    for (const id of ids) {
      const candidate = eventsById.get(id);
      if (excluded.has(id)) throw new Error(`Related ${kind} for ${event.id} includes current/other-date ${id}`);
      if (candidate?.other_date_ids.includes(event.id)) throw new Error(`Related ${kind} for ${event.id} includes reverse other-date ${id}`);
    }
  }
  if (event.ticket.kind === 'source' && !event.ticket.is_free && /Билеты в продаже/u.test(event.status_label)) {
    throw new Error(`Source-only paid event ${event.id} pretends direct ticket sale`);
  }
  if (/#/u.test(event.venue_name || '')) throw new Error(`Venue contains hashtag for ${event.id}`);
}

const badHtmlPatterns = [
  ['raw facts marker', /\*\*facts\*\*/u],
  ['raw markdown separator', /\*\*\*/u],
  ['literal escaped newline', /\\n/u],
  ['literal null', /\bnull\b/u],
  ['literal undefined', /\bundefined\b/u],
  ['literal NaN', /\bNaN\b/u],
  ['preview diagnostic copy', /Preview показывает/u],
  ['orphan variation selector', /\ufe0f/u],
  ['empty heading', /<h[1-6][^>]*>\s*<\/h[1-6]>/iu],
];
for (const event of eventsData.events) {
  const html = readFileSync(join(root, `sobytiya/${event.slug}/index.html`), 'utf8');
  const visibleHtml = stripGeneratedCode(html);
  const heroExpected = event.image_url ? (event.image_text_mode === 'visual_only' ? 'data-hero-mode="photo-cover"' : 'data-hero-mode="poster-stage"') : 'data-hero-mode="fallback-art"';
  if (!html.includes(heroExpected)) throw new Error(`Event ${event.id} hero mode mismatch for ${event.image_text_mode}`);
  if (!html.includes('data-hero-composition=')) throw new Error(`Event ${event.id} hero misses composition marker`);
  if (!html.includes(`data-hero-image-text-mode="${event.image_text_mode}"`)) throw new Error(`Event ${event.id} hero misses image_text_mode marker`);
  if (/data-(?:feedback|share)-count[^>]*>0<\/span>/u.test(html)) throw new Error(`Event ${event.id} renders zero reaction counter`);
  const ownCalendarHrefCandidates = [
    icsBaseUrl ? `${icsBaseUrl}/${event.id}.ics` : null,
    `https://static.kenigevents.ru/ics/${event.id}.ics`,
    `/sobytiya/${event.slug}/event.ics`,
  ].filter(Boolean);
  const ownCalendarHrefPattern = new RegExp(`href=\"[^\"]*(?:/ics/${event.id}\\.ics|/sobytiya/${event.slug}/event\\.ics)(?:[?#][^\"]*)?\"`, 'u');
  const hasOwnCalendarLink = ownCalendarHrefCandidates.some((href) => html.includes(href)) || ownCalendarHrefPattern.test(html);
  const calendarEligible = !event.end_date || event.end_date === event.start_date;
  if (calendarEligible && !hasOwnCalendarLink) throw new Error(`Short event ${event.id} misses own calendar link`);
  if (!calendarEligible && hasOwnCalendarLink) throw new Error(`Multi-day event ${event.id} must not expose own calendar link`);
  if (calendarEligible && (!html.includes(`data-calendar-event-id="${event.id}"`) || !html.includes('data-calendar-expiry-day=') || !html.includes('data-calendar-label'))) {
    throw new Error(`Short event ${event.id} misses shared calendar state markers`);
  }
  for (const [label, pattern] of badHtmlPatterns) {
    if (pattern.test(visibleHtml)) throw new Error(`Rendered page ${event.id} contains ${label}`);
  }
  if (!event.address && html.includes('Открыть на карте')) throw new Error(`Weak-address event ${event.id} shows map CTA`);
  if (event.ticket.kind === 'source' && !event.ticket.is_free && html.includes('Билеты в продаже')) {
    throw new Error(`Source-only page ${event.id} shows misleading ticket-sale copy`);
  }
}
console.log(`Preview checks passed for ${buildId}`);
