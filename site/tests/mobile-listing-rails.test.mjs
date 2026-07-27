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
  assert.match(row, /displayDateRange\(event\.start_date, event\.end_date\)\.replace\(\/\\s\+—\\s\+\/u, '–'\)/u);
  assert.match(row, /const scheduleAria = compactRailDateRange\s*\? \[fullDateLine, timeLine\]\.filter\(Boolean\)\.join\(', '\)/u);
  assert.match(row, /'event-date-line--range'/u);
  assert.match(row, /resolveMobileListingRailMediaItems\(event, image\)/u);
  assert.match(row, /railMediaItems\.map\(\(railMedia, mediaIndex\)/u);
  assert.match(row, /data-rail-gallery-index=\{mediaIndex\}/u);
  assert.match(row, /data-mobile-rail-gallery-count=\{railMediaItems\.length\}/u);
  assert.match(row, /import Icon from '\.\.\/Icon\.astro'/u);
  assert.equal((row.match(/<Icon name="heart" \/>/gu) || []).length, 3);
  assert.doesNotMatch(row, />♥</u);
  assert.match(row, /data-feedback-action="not_interested"/u);
  assert.match(row, /data-feedback-count/u);
  assert.match(row, /data-image-text-mode=\{railMedia\.imageTextMode\}/u);
  assert.match(row, /data-media-state="loading"/u);
  assert.match(surface, /mobile-rail-media-skeleton/u);
  assert.match(surface, /if \(img\.complete\) decodeLoaded\(\)/u);
  assert.match(surface, /img\.addEventListener\('error', \(\) => done\(false\)/u);
  assert.doesNotMatch(menu, /\.reference4-menu__brand::before/u);
  assert.match(dates, /<MobileListingRailSurface[\s\S]*occurrenceMode="per-date"/u);
  assert.match(dates, /const mobileChronologicalEvents = events;/u);
  assert.match(dates, /id:`\$\{kind\}-chronological`, events:mobileChronologicalEvents/u);
  assert.doesNotMatch(dates, /id:`\$\{kind\}-earlier`/u);
  assert.match(dates, /calendarToday=\{getCurrentDate\(\)\}/u);
  assert.match(weekend, /<MobileListingRailSurface[\s\S]*occurrenceMode="per-date"/u);
  assert.match(weekend, /calendarToday=\{currentDate \|\| start\}/u);
  assert.match(popular, /collapseOccurrenceCards\(group\.events, 'per-family'\)/u);
  assert.match(popular, /<MobileListingRailSurface[\s\S]*occurrenceMode="per-family"/u);
  assert.match(accessory, /buildEventDateAvailability\(getEvents\(\), today\)/u);
  assert.match(accessory, /class="date-rail"/u);
  assert.match(accessory, /class="date-calendar-trigger"/u);
  assert.match(accessory, /class="calendar-sheet"/u);
  assert.match(accessory, /class="calendar-grid"/u);
  assert.match(accessory, /getAvailableWeekendRanges\(\)/u);
  assert.match(accessory, /`\/date-\$\{iso\}\/`/u);
  assert.match(accessory, /aria-disabled="true"/u);
  assert.match(accessory, /item\.href \? \([\s\S]*?<a[\s\S]*?\) : \([\s\S]*?<span/u);
  assert.match(await read('src/pages/date-[date].astro'), /eventDateRouteDates\(getEvents\(\), getCurrentDate\(\)\)/u);
  assert.match(await read('src/pages/date-[date].astro'), /<DateListingSurface kind="date"/u);
});

test('accepted donor edge gestures and hollow-to-filled system heart states are executable contracts', async () => {
  const surface = await read('src/components/listings/MobileListingRailSurface.astro');
  for (const token of [
    'setDislike',
    'setLikePull',
    'finishLike',
    'touchstart',
    'touchmove',
    'pointerdown',
    'pointermove',
    'pointercancel',
    'data-rail-confirm-negative',
    'data-rail-action-toast',
  ]) assert.match(surface, new RegExp(token, 'u'), token);
  assert.match(surface, /progress >= \.86 && dx >= 140/u);
  assert.match(surface, /physical >= 120/u);
  assert.match(surface, /Math\.abs\(dx\) >= Math\.abs\(dy\) \* 1\.25/u);
  assert.match(surface, /\.rail-window\.is-settling \.event-track\{transition:transform 400ms cubic-bezier\(\.2,\.8,\.2,1\)/u);
  assert.match(surface, /\.event-like-cta\[aria-pressed=true\] \.icon__heart-outline\{display:none\}/u);
  assert.match(surface, /\.event-like-cta\[aria-pressed=true\] \.icon__heart-solid\{display:block!important\}/u);
  assert.match(surface, /if \(!event\.isTrusted\) return/u);
  assert.match(surface, /event\.stopImmediatePropagation\(\)/u);
  assert.match(surface, /\}, \{ capture:true \}\);/u);
  assert.match(surface, /const waitForPressed =/u);
  assert.doesNotMatch(surface, /setTimeout\(\(\) => \{[\s\S]{0,160}aria-pressed[\s\S]{0,160}\}, 80\)/u);
  assert.match(surface, /body\.rail-confirm-open \.mobile-bottom-nav\{opacity:0;visibility:hidden;pointer-events:none\}/u);
  assert.match(surface, /const reduced = matchMedia\('\(prefers-reduced-motion: reduce\)'\)\.matches/u);
});

test('negative swipe consent is device-local, fail-closed and granted only after the canonical action commits', async () => {
  const surface = await read('src/components/listings/MobileListingRailSurface.astro');
  assert.match(surface, /const negativeSwipeConsentKey = 'ke_rail_negative_swipe_consent_v1'/u);
  assert.match(surface, /const hasNegativeSwipeConsent = \(\) => \{[\s\S]*try \{[\s\S]*localStorage\.getItem\(negativeSwipeConsentKey\)[\s\S]*catch \(_\) \{\s*return false;/u);
  assert.match(surface, /const rememberNegativeSwipeConsent = \(\) => \{[\s\S]*try \{[\s\S]*localStorage\.setItem\(negativeSwipeConsentKey, 'true'\)[\s\S]*catch \(_\) \{\s*return false;/u);
  assert.match(surface, /const commitNegative = \(row,[\s\S]*negative\.click\(\);[\s\S]*waitForPressed\(negative, true\)\.then\(\(pressed\) => \{\s*if \(!pressed\) return false;\s*if \(rememberSwipeConsent\) rememberNegativeSwipeConsent\(\);/u);
  assert.match(surface, /const commitSwipeNegative = \(row\) => \{\s*if \(hasNegativeSwipeConsent\(\)\) \{\s*void commitNegative\(row\);/u);
  assert.match(surface, /void commitNegative\(row, \{ rememberSwipeConsent:true \}\)/u);
  assert.match(surface, /следующие свайпы будут отмечать события без подтверждения\. Любую отметку можно отменить/u);
  assert.match(surface, /showToast\('Событие отмечено как неинтересное', \(\) => \{\s*negative\.click\(\)/u);
  assert.doesNotMatch(surface, /data-rail-confirm-cancel[\s\S]{0,180}rememberNegativeSwipeConsent/u);
});

test('today mobile rail mutes only ended or one-hour-old main media and leaves desktop cards untouched', async () => {
  const [surface, row] = await Promise.all([
    read('src/components/listings/MobileListingRailSurface.astro'),
    read('src/components/listings/MobileListingRailRow.astro'),
  ]);
  assert.match(row, /data-event-starts-at=\{event\.starts_at \|\| undefined\}/u);
  assert.match(row, /data-event-end-at=\{event\.end_at \|\| undefined\}/u);
  assert.match(surface, /timeZone:'Europe\/Kaliningrad'/u);
  assert.match(surface, /const listingDayElapsed = \/\^\\d\{4\}-\\d\{2\}-\\d\{2\}\$\/u\.test\(listingDate\)[\s\S]*listingDate < kaliningradDate/u);
  assert.match(surface, /const completed = Number\.isFinite\(endMs\) && endMs <= now/u);
  assert.match(surface, /const elapsedListingDayWithoutEnd = !hasExplicitEnd && listingDayElapsed/u);
  assert.match(surface, /const startedEarlier = !hasExplicitEnd[\s\S]*kaliningradDate === listingDate[\s\S]*startMs <= now - 60 \* 60 \* 1000/u);
  assert.match(surface, /const state = completed \|\| elapsedListingDayWithoutEnd \? 'past' : startedEarlier \? 'started-earlier' : 'current'/u);
  assert.match(surface, /syncTodayTemporalMedia\(\);\s*if \(surface\.dataset\.mobileV23Page === 'today'\) setInterval\(syncTodayTemporalMedia, 60_000\)/u);
  assert.match(surface, /row\.querySelectorAll\('\.event-media'\)\.forEach\(\(media\) => media\.classList\.toggle\('is-temporally-muted', state !== 'current'\)\)/u);
  assert.match(surface, /\.ke-mobile-listing-rails--v23 \.event-media\.is-loaded\.is-temporally-muted>img\{opacity:\.46;filter:grayscale\(\.72\) saturate\(\.32\)\}/u);
  assert.doesNotMatch(surface, /\.event-row\.is-temporally-muted|\.event-medallion[^}]*filter:/u);
});

test('desktop date context occupies the existing discovery plane and is rail-locally revealed only when pinned', async () => {
  const [rail, dateSurface, styles] = await Promise.all([
    read('src/components/listings/ListingDiscoveryRail.astro'),
    read('src/components/listings/DateListingSurface.astro'),
    read('src/styles/design-system.css'),
  ]);
  assert.match(dateSurface, /dateContext=\{\{ date: mobileDate, weekday: mobileWeekday \}\}/u);
  assert.match(rail, /data-listing-rail-date-context aria-hidden="true"[\s\S]*<strong>\{dateContext\.date\}<\/strong>[\s\S]*<small>\{dateContext\.weekday\}<\/small>/u);
  assert.ok(rail.indexOf('data-listing-rail-date-context') < rail.indexOf('<ListingControls'));
  assert.match(rail, /const pinned = desktop\.matches && scrollY > 0 && Math\.abs\(railBox\.top - headerBottom\) <= 1/u);
  assert.match(rail, /rail\.classList\.toggle\('is-pinned', pinned\)/u);
  assert.match(rail, /context\?\.setAttribute\('aria-hidden', String\(!pinned\)\)/u);
  assert.doesNotMatch(rail, /document\.body\.classList/u);
  assert.match(styles, /\.ke-listing-discovery-rail--with-date-context \.ke-listing-discovery-rail__inner \{\s*grid-template-columns: 112px minmax\(0, 1fr\) max-content;/u);
  assert.match(styles, /\.ke-listing-discovery-rail\.is-pinned \.ke-listing-discovery-rail__date-context/u);
  assert.match(styles, /@media \(max-width: 980px\)[\s\S]*\.ke-listing-discovery-rail__date-context \{ display: none; \}/u);
  assert.match(styles, /\.ke-listing-discovery-rail__inner \{[\s\S]*padding-left: 304px;/u);
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
  const [weekend, currentWeekend, adjacentWeekend, row, artifact, artifactsLib] = await Promise.all([
    read('src/components/listings/WeekendListingSurface.astro'),
    read('src/pages/vyhodnye/index.astro'),
    read('src/pages/vyhodnye/[start].astro'),
    read('src/components/listings/MobileListingRailRow.astro'),
    read('src/components/listings/AmberRailArtifact.astro'),
    read('src/lib/artifacts.mjs'),
  ]);
  assert.match(currentWeekend, /isAmberArtifactResearchEnabled\(\s*SITE_MODE,\s*import\.meta\.env\.PUBLIC_ENABLE_AMBER_ARTIFACT_RESEARCH/u);
  assert.match(currentWeekend, /selectAmberArtifactEventId\(events/u);
  assert.match(currentWeekend, /PUBLIC_STATIC_RELEASE_ID[\s\S]*\|\| PREVIEW_BUILD_ID/u);
  assert.match(currentWeekend, /amberArtifactTailEventId=\{amberArtifactTailEventId\}/u);
  assert.doesNotMatch(adjacentWeekend, /amberArtifactTailEventId/u);
  assert.match(artifactsLib, /siteMode !== 'production' && flag === 'tail'/u);
  assert.match(artifactsLib, /stableArtifactHash\(`\$\{AMBER_ARTIFACT_ID\}:assignment-v1:\$\{String\(seed\)\}`\) % candidates\.length/u);
  assert.doesNotMatch(weekend, /6939|PUBLIC_ENABLE_AMBER_ARTIFACT_RESEARCH|IS_PRODUCTION/u);
  assert.match(row, /<button[\s\S]*class="event-like-cta"[\s\S]*<\/button>\s*\{amberArtifactTail && <AmberRailArtifact/u);
  assert.match(artifact, /\.amber-artifact\{[^}]*flex:0 0 94px;width:94px;height:112px/u);
  assert.match(artifact, /\.amber-artifact__visual\{[^}]*width:74px;height:96px/u);
  assert.match(artifact, /entry\.intersectionRatio < \.72/u);
  assert.match(artifact, /collectAmberArtifact/u);
  assert.match(artifact, /location\.assign\(button\.dataset\.artifactDetailUrl/u);
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

test('real-data canaries retain Pianissimo/Teremok crop evidence and More vnutri structured binding', async () => {
  const [preview, overrides, festivals] = await Promise.all([
    read('src/data/preview-events.json').then(JSON.parse),
    read('src/data/listingMediaOverrides.json').then(JSON.parse),
    read('src/data/festivalMedallions.json').then(JSON.parse),
  ]);
  const pianissimo = preview.events.find((event) => event.id === 5296);
  assert.ok(pianissimo, 'Pianissimo 5296 must be in the current real-data snapshot');
  assert.equal(pianissimo.image_assets.length, 1);
  assert.equal(pianissimo.image_assets[0].image_text_mode, 'visual_only');
  assert.equal(pianissimo.image_assets[0].safe_crop, true);

  const teremok = preview.events.find((event) => event.id === 6939);
  assert.ok(teremok, 'Teremok 6939 must be in the current real-data snapshot');
  const reviewedSrc = 'https://static.kenigevents.ru/p/dh16/00/00450088000040066194318c30c61a8433adac94241ca7180611098703ce2949.webp';
  const reviewedAsset = teremok.image_assets.find((asset) => asset.src === reviewedSrc);
  assert.ok(reviewedAsset);
  const review = overrides.items.find((item) => item.sourceSrc === reviewedSrc);
  assert.equal(review?.imageTextMode, 'visual_only');
  assert.match(review?.cropEvidence || '', /reviewed-no-ocr/u);
  assert.equal(review?.noOcrReviewed, true);
  const retainedArea = Math.min((reviewedAsset.width / reviewedAsset.height) / .8, .8 / (reviewedAsset.width / reviewedAsset.height));
  assert.ok(retainedArea >= .8, `Teremok 4:5 crop retained only ${retainedArea}`);

  const more = preview.events.find((event) => event.id === 4211);
  assert.equal(more?.festival, 'МОРЕ ВНУТРИ');
  assert.equal(more?.start_date, '2026-08-08');
  assert.equal(more?.end_date, '2026-08-09');
  const moreManifest = festivals.items.find((item) => item.slug === 'more-vnutri');
  assert.equal(moreManifest?.listingStatus, 'listing_ready');
  assert.equal(moreManifest?.listingBinding, 'festival');
});

test('packaged product smoke and local noindex red-dot matrix use separate Playwright bases', async () => {
  const playwright = await read('tests/unusual-events.playwright.mjs');
  assert.match(playwright, /UNUSUAL_EVENTS_PLAYWRIGHT_MODE \|\| 'product'/u);
  assert.match(playwright, /const runProduct = mode === 'product' \|\| mode === 'all'/u);
  assert.match(playwright, /const runLab = mode === 'lab' \|\| mode === 'all'/u);
  assert.match(playwright, /UNUSUAL_EVENTS_BASE_URL is required in product\/all mode/u);
  assert.match(playwright, /UNUSUAL_EVENTS_LAB_BASE_URL is required in lab\/all mode/u);
  assert.match(playwright, /if \(runProduct\) \{[\s\S]*route\(productBase, '\/neobychnoe\/'\)/u);
  assert.match(playwright, /if \(runLab\) \{[\s\S]*route\(labBase, `\/lab\/unusual-unread\/\$\{scenario\}\/`\)/u);
  assert.doesNotMatch(playwright, /route\(productBase, `\/lab\/unusual-unread/u);
});
