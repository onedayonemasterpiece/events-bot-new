import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { readdir } from 'node:fs/promises';
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

test('personal feed keeps listing hydration hidden and exposes a bounded event-detail continuation', async () => {
  const source = await read('src/components/PersonalFeedSlot.astro');
  assert.match(source, /'ke-personal-feed-slot'/u, 'mobile rail selector matches the generic personal-feed section');
  assert.match(source, /data-personal-feed-section/u);
  assert.match(source, /data-personal-feed-src/u);
  assert.match(source, /data-personal-feed-related-src/u);
  assert.match(source, /data-current-event-id/u);
  assert.match(source, /data-personal-feed-slot/u);
  assert.match(source, /data-personal-feed-status/u);
  assert.match(source, /!isEventDetail && <button[^>]*data-personal-feed-load-more/u);
  assert.match(source, /data-personal-feed-mode=\{isEventDetail \? 'pending' : undefined\}/u);
  assert.match(source, /aria-live="polite"/u);
  assert.match(source, /hidden=\{!isEventDetail\}/u);
  assert.match(source, /Шесть разных событий без бесконечной ленты/u);
  assert.match(source, /data-personal-feed-all-events[^>]*href=\{siteHomeHref\(\)\}>Все анонсы/u);
  assert.match(source, /data-personal-feed-mode="pending"/u);
  assert.match(source, /data-personal-feed-mode="popular_fallback"/u);
  assert.match(source, /data-personal-feed-mode="unavailable"/u);
  assert.match(source, /repeat\(3, minmax\(0, 1fr\)\)/u);
  assert.match(source, /repeat\(2, minmax\(0, 1fr\)\)/u);
  assert.match(source, /grid-template-columns: minmax\(0, 1fr\)/u);
});

test('personal feed uses contiguous desktop row packing and independent mobile media decisions', async () => {
  const [slot, layout] = await Promise.all([
    read('src/components/PersonalFeedSlot.astro'),
    read('src/layouts/EventLayout.astro'),
  ]);

  assert.match(layout, /function personalFeedUsesDesktopRows\(\)/u);
  assert.match(layout, /window\.KenigEventsPackRelatedCardRows\(store\.items\.slice\(store\.rendered, Math\.min\(store\.items\.length, end \+ 3\)\), \{\s*limit: chunk\.length,\s*rowSize: 3,/u, 'every desktop context packs a bounded three-card-row chunk with alternates');
  assert.doesNotMatch(layout, /eventDetail && typeof window\.KenigEventsPackRelatedCardRows/u, 'desktop packing is not event-detail-only');
  assert.match(layout, /rowOffset = Math\.ceil\(slot\.querySelectorAll\(':scope > \[data-event-card\]'\)\.length \/ 3\)/u, 'later desktop chunks continue after existing row coordinates');
  assert.match(layout, /layout: \{ \.\.\.layout, rowIndex: Number\(layout\.rowIndex\) \+ rowOffset \}/u);
  assert.match(layout, /if \(packed\.length < chunk\.length && nextRendered < store\.items\.length\)[\s\S]*store\.desktopPackingComplete = true/u, 'an optimizer remainder is never followed by an incomplete middle row');
  assert.match(layout, /loadMore\.hidden = Boolean\(store\.desktopPackingComplete\) \|\| store\.rendered >= store\.items\.length/u);
  assert.match(layout, /chunk\.map\(\(item\) => \(\{ item, layout: window\.KenigEventsResolveMobileEventCardMedia\(item\) \}\)\)/u, 'mobile cards use the mobile media resolver');
  assert.match(layout, /appendEventCard\(slot, item, slot\.dataset\.feedCardVariant \|\| 'split-actions', layout\)/u, 'both flows pass their resolved layout into the canonical card');
  assert.doesNotMatch(layout, /chunk\.map\(\(item\) => \(\{ item, layout: null \}\)\)/u, 'mobile never falls back to unresolved desktop-template media');

  assert.match(slot, /@media \(min-width: 1024px\) \{[\s\S]*\.personal-feed-section :global\(\[data-lab-related-card\]\)/u);
  assert.match(slot, /\.personal-feed-section :global\(\[data-lab-related-card\] \.event-card__media-shell\)[\s\S]*aspect-ratio: var\(--lab-row-media-ratio\)/u);
  assert.match(slot, /\.personal-feed-section :global\(\[data-lab-related-card\] \.event-card__body\)[\s\S]*min-height: 0/u);
  assert.doesNotMatch(slot, /\.personal-feed-section--event-detail :global\(\[data-lab-related-card\]/u, 'desktop normalization covers every personal-feed context');
});

test('event-detail continuation uses six diverse cards with an honest non-personal fallback', async () => {
  const layout = await read('src/layouts/EventLayout.astro');

  assert.match(layout, /const EVENT_DETAIL_CONTINUATION_LIMIT = 6/u);
  assert.match(layout, /function rankPopularFallbackCandidates\(manifest, profile\)/u);
  assert.match(layout, /const upcomingProximity = 1 \/ \(1 \+ daysAway \/ 7\)/u);
  assert.match(layout, /const score = 0\.68 \* popularity \+ 0\.32 \* upcomingProximity/u);
  assert.match(layout, /'catalog:upcoming_proximity'/u);
  assert.match(layout, /maxSameCategory: 3, maxSameVenue: 2/u);
  assert.match(layout, /personalFeedProfileIsReady\(profile\)/u);
  assert.match(layout, /function rankAdjacentContinuationCandidates\(manifest, profile\)/u);
  assert.match(layout, /verification_state !== 'llm_rejected'/u);
  assert.match(layout, /EVENT_CONTINUATION_RECENT_TTL_MS = 6 \* 60 \* 60 \* 1000/u);
  assert.match(layout, /readRecentContinuationIds\(\)/u);
  assert.match(layout, /recordRecentContinuation\(personalFeedCurrentEventId\(section\), ranked\)/u);
  assert.match(layout, /KenigEventsSelectEventContinuation/u);
  assert.match(layout, /currentEventId: personalFeedCurrentEventId\(section\)/u);
  assert.match(layout, /section\.dataset\.personalFeedMode = isPersonal \? 'personal' : 'popular_fallback'/u);
  assert.match(layout, /title\.textContent = isPersonal \? 'По вашим интересам' : 'Ещё события'/u);
  assert.match(layout, /if \(loadMore\) loadMore\.hidden = isEventDetail/u);
});

test('personal feed endpoint is a bounded static catalog without a backend RPC', async () => {
  const source = await read('src/pages/data/personal-feed.json.ts');
  assert.match(source, /const MAX_CANDIDATES = 500/u);
  assert.match(source, /eventIntersectsDateRange\(event, currentDate, '9999-12-31'\)/u);
  assert.doesNotMatch(source, /supabase|\/rpc\/|anon_id|session_id/iu);
});

test('built personal feed manifest is compact, public, and card-compatible', async () => {
  const payload = JSON.parse(await readBuilt('data/personal-feed.json'));
  assert.equal(payload.schema_version, 'listing-personal-feed-v1');
  assert.equal(payload.feature_schema_version, 'event-detail-related-v1');
  assert.equal(payload.surface, 'listing_personal_feed');
  assert.equal(payload.algorithm_id, 'static_personal_feed_catalog_v1');
  assert.ok(Array.isArray(payload.related_static));
  assert.ok(payload.related_static.length > 0);
  assert.ok(payload.related_static.length <= 500);

  const forbiddenKeys = new Set([
    'address',
    'description',
    'description_html',
    'email',
    'meta_description',
    'phone',
    'profile',
    'session_id',
    'source_url',
    'source_urls',
    'summary',
    'telegraph_url',
    'ticket',
    'user_id',
  ]);
  const visit = (value) => {
    if (!value || typeof value !== 'object') return;
    for (const [key, child] of Object.entries(value)) {
      assert.ok(!forbiddenKeys.has(key), `manifest must not expose ${key}`);
      visit(child);
    }
  };
  visit(payload);

  for (const candidate of payload.related_static) {
    assert.equal(candidate.lifecycle_status, 'active');
    assert.ok((candidate.date || '') <= '9999-12-31');
    assert.ok(candidate.date >= payload.current_date || candidate.reason_codes.includes('catalog:ongoing'));
    for (const key of ['event_id', 'title', 'category', 'tags', 'date', 'status', 'display']) {
      assert.ok(Object.hasOwn(candidate, key), `candidate is missing ${key}`);
    }
    for (const key of ['href', 'absolute_url', 'display_date', 'display_date_time', 'calendar_href']) {
      assert.ok(Object.hasOwn(candidate.display, key), `candidate display is missing ${key}`);
    }
  }

  assert.ok(Buffer.byteLength(JSON.stringify(payload)) < 2_000_000, 'manifest should stay below 2 MB');
});

test('runtime cards clone the canonical EventCard DOM and safely populate its interaction hooks', async () => {
  const [card, slot, layout, search] = await Promise.all([
    read('src/components/EventCard.astro'),
    read('src/components/PersonalFeedSlot.astro'),
    read('src/layouts/EventLayout.astro'),
    read('src/components/AuthorizedEventSearch.astro'),
  ]);

  assert.match(layout, /import EventCard from '\.\.\/components\/EventCard\.astro'/u);
  assert.match(layout, /<template data-event-card-template="split-actions">\s*<EventCard[^>]*variant="split-actions"[^>]*desktopRelatedCrop[^>]*runtimeTemplate/u);
  assert.match(layout, /<template data-event-card-template="overlay-controls">\s*<EventCard[^>]*variant="overlay-controls"[^>]*desktopRelatedCrop[^>]*runtimeTemplate/u);
  assert.match(card, /runtimeTemplate\?: boolean/u);
  assert.match(card, /const calendarHref = runtimeTemplate \? '#' : eventCalendarHref\(event\)/u, 'inert template never emits a stable calendar URL');
  assert.match(layout, /image_url: withBase\('\/favicon\.svg'\)/u, 'template media remains inside the active preview/candidate prefix');
  for (const hook of ['data-card-media-link', 'data-card-media-shell', 'data-card-image', 'data-card-title', 'data-card-type', 'data-card-meta', 'data-card-status', 'data-card-place']) {
    assert.match(card, new RegExp(hook, 'u'), `canonical EventCard exposes ${hook} for safe clone population`);
  }
  for (const interaction of ['data-feedback-action="not_interested"', 'data-feedback-action="like"', 'data-native-share', 'data-calendar-action']) {
    assert.match(card, new RegExp(interaction, 'u'), `canonical EventCard owns ${interaction}`);
  }
  assert.match(layout, /template\?\.content\?\.querySelector\('\[data-event-card\]'\)/u);
  assert.match(layout, /const card = sourceCard\.cloneNode\(true\)/u);
  assert.match(layout, /node\.textContent = text/u);
  assert.match(layout, /target\.appendChild\(card\)/u);
  assert.match(layout, /safeRuntimeCardUrl/u);
  assert.doesNotMatch(layout, /function eventCardHtml/u);
  assert.doesNotMatch(layout, /const (?:dislike|share|heart|calendar)Icon = '<svg/u);
  assert.doesNotMatch(layout, /insertAdjacentHTML\('beforeend',\s*(?:eventCardHtml|packed\.map)/u);
  assert.match(search, /window\.KenigEventsRenderEventCard/u, 'normal authorized-search results use the canonical renderer');
  assert.match(search, /data-search-card-render-unavailable/u, 'renderer failure is an explicit non-card status');
  assert.doesNotMatch(search, /<article class="event-card/u, 'search has no handwritten EventCard lookalike fallback');
});

test('mature personalization changes only the desktop broad heading and stays hidden on mobile', async () => {
  const [slot, layout] = await Promise.all([
    read('src/components/PersonalFeedSlot.astro'),
    read('src/layouts/EventLayout.astro'),
  ]);

  assert.match(slot, /@media \(max-width: 1023px\)[\s\S]*data-personal-feed-mode="personal"[\s\S]*display: none/u, 'mature personal continuation is never a second mobile module');
  assert.match(layout, /section\.dataset\.personalFeedMode = isPersonal \? 'personal' : 'popular_fallback'/u);
  assert.match(layout, /title\.textContent = isPersonal \? 'По вашим интересам' : 'Ещё события'/u, 'desktop labels mature personalization honestly while fallback remains broad discovery');
});

test('desktop keeps a separate finite broad-discovery section after the similar-events surface', async () => {
  const [page, desktop, slot, layout] = await Promise.all([
    read('src/pages/sobytiya/[slug].astro'),
    read('src/components/DesktopEventPage.astro'),
    read('src/components/PersonalFeedSlot.astro'),
    read('src/layouts/EventLayout.astro'),
  ]);

  assert.match(desktop, /Смотрите дальше/u, 'desktop retains the explicit similar-events block');
  const desktopPageIndex = page.indexOf('<DesktopEventPage');
  const mainEndIndex = page.indexOf('</main>');
  const broadSectionIndex = page.indexOf('<PersonalFeedSlot context="event-detail"');
  assert.ok(desktopPageIndex >= 0 && mainEndIndex > desktopPageIndex && broadSectionIndex > mainEndIndex, 'Ещё события remains a separate sibling after the desktop detail/similar surface');
  assert.match(slot, /<section[\s\S]*personal-feed-section--event-detail/u);
  assert.match(slot, /const heading = isEventDetail \? 'Ещё события'/u);
  assert.match(slot, /Шесть разных событий без бесконечной ленты/u);
  assert.match(slot, /isEventDetail && <a[^>]*data-personal-feed-all-events/u);
  assert.match(slot, /!isEventDetail && <button[^>]*data-personal-feed-load-more/u, 'event detail has no load-more control');
  assert.match(layout, /const EVENT_DETAIL_CONTINUATION_LIMIT = 6/u);
  assert.match(layout, /maxSameCategory: 3, maxSameVenue: 2/u);
  assert.match(layout, /eventIdsAlreadyOffered\(section\)/u);
});

test('broad continuation uses the canonical card variant and discovery controller context', async () => {
  const [slot, layout] = await Promise.all([
    read('src/components/PersonalFeedSlot.astro'),
    read('src/layouts/EventLayout.astro'),
  ]);

  assert.match(slot, /data-personal-feed-slot[\s\S]*data-feed-card-variant="split-actions"/u);
  assert.match(layout, /appendEventCard\(slot, item, slot\.dataset\.feedCardVariant \|\| 'split-actions', layout\)/u);
  assert.match(layout, /personalFeedStores\.set\(section, \{ items: ranked, ranked, rendered: 0, profile, manifest \}\)/u);
  assert.match(layout, /const ranked = toArray\(store\?\.ranked \|\| store\?\.items\)/u, 'candidate and rank lookup share the controller store contract');
  assert.match(layout, /if \(ranked\?\.candidate\) return ranked\.candidate/u, 'broad cards retain candidate tags for profile actions');
  assert.match(layout, /servedListByFeed\.set\(slot, createServedListSummary/u, 'broad cards receive served-list context');
  assert.match(layout, /card\?\.closest\('\[data-discovery-feed\], \[data-personal-feed-slot\]'\)/u, 'both card surfaces use the same controller lookup');
  assert.match(layout, /const \{ store \} = await controllerForCard\(card\)/u);
  assert.equal((layout.match(/document\.addEventListener\('click', async \(event\) => \{\s*const button = event\.target\.closest\('\[data-feedback-action\]'\)/gu) || []).length, 1, 'feedback retains one delegated handler');
  assert.doesNotMatch(layout, /function eventCardHtml/u, 'controller parity does not reintroduce duplicate card markup');
});

test('feedback and share preserve the finite broad set and its undo plate', async () => {
  const layout = await read('src/layouts/EventLayout.astro');
  const applyStart = layout.indexOf('async function applyFeedbackState(options = {})');
  const applyEnd = layout.indexOf('window.applyFeedbackState = applyFeedbackState;', applyStart);
  const applyBody = layout.slice(applyStart, applyEnd);

  assert.ok(applyStart >= 0 && applyEnd > applyStart);
  assert.doesNotMatch(applyBody, /hydratePersonalFeedSlots/u, 'ordinary state sync cannot replace all six broad cards');
  assert.match(layout, /syncNotInterestedPlate\(card, current\)/u);
  assert.match(layout, /action = notInterested \? 'not_interested' : 'undo_not_interested'/u);
  assert.match(layout, /await applyFeedbackState\(\{ anchorEventId: eventId \}\)/u, 'feedback/share still update the shared controller without a personal-feed rerender');
});

test('desktop-only continuation never fetches, renders or records on mobile and hydrates once after resize', async () => {
  const [slot, layout] = await Promise.all([
    read('src/components/PersonalFeedSlot.astro'),
    read('src/layouts/EventLayout.astro'),
  ]);

  assert.match(slot, /data-personal-feed-desktop-only=\{isEventDetail \? 'true' : undefined\}/u);
  assert.match(layout, /function personalFeedSectionCanHydrate\(section\)/u);
  assert.match(layout, /if \(!section \|\| personalFeedReached\.has\(section\) \|\| personalFeedHydrationInFlight\.has\(section\) \|\| !personalFeedSectionCanHydrate\(section\)\) return/u);
  assert.match(layout, /const candidates = \(sections[\s\S]*\.filter\(personalFeedSectionCanHydrate\)/u, 'mobile sections are filtered before manifest fetch');
  assert.match(layout, /if \(personalFeedSectionCanHydrate\(section\)\) renderPersonalFeedSection\(section, manifest\)/u, 'a resize during fetch cannot render unseen mobile cards');
  assert.match(layout, /if \(isEventDetail && rendered > 0\) recordRecentContinuation/u, 'only rendered desktop cards enter the recent ring');
  assert.match(layout, /personalFeedDesktopMedia\?\.addEventListener\?\.\('change'/u);
  assert.match(layout, /startPersonalFeedHydration\(section, \{ eager: true \}\)/u);
  assert.match(layout, /if \(personalFeedSectionCanHydrate\(section\)\) personalFeedReached\.add\(section\)/u, 'only a still-desktop completion makes hydration one-shot');
  assert.match(layout, /personalFeedHydrationInFlight\.delete\(section\)/u, 'a mobile transition during fetch allows the next desktop transition to retry');
});
