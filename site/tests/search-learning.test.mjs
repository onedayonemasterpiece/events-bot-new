import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { test } from 'node:test';

const read = (path) => readFileSync(new URL(`../${path}`, import.meta.url), 'utf8');
const searchPage = read('src/pages/poisk/index.astro');
const learning = read('src/components/SearchCollectionLinks.astro');
const collections = read('src/data/searchCollections.ts');
const collectionPage = read('src/pages/podborki/[slug]/index.astro');
const donor = read('src/components/AuthorizedEventSearch.astro');
const bottomNav = read('src/components/MobileBottomNav.astro');
const eventLayout = read('src/layouts/EventLayout.astro');
const mobileMenu = read('src/components/Reference4MobileMenu.astro');

test('materialized collections are real static links and examples are fill-only controls', () => {
  assert.match(learning, /href=\{withBase\(`\/podborki\/\$\{item\.slug\}\/`\)\}/u);
  assert.match(learning, /data-search-collection-link/u);
  assert.match(learning, /type="button"[\s\S]*data-search-query-fill=\{item\.phrase\}/u);
  assert.match(learning, /input\.value = query/u);
  assert.doesNotMatch(learning, /form\.requestSubmit|form\.submit|window\.location|fetch\(/u);
  assert.match(learning, /пример/u);
  assert.match(learning, /Личные сохранённые запросы[\s\S]*только после входа/u);
});

test('technical seed-tag copy is removed from the Search page', () => {
  assert.match(searchPage, /<SearchCollectionLinks\s*\/>/u);
  assert.doesNotMatch(searchPage, /Поисковые теги|seed-теги|могут стать страницами/u);
  assert.match(collections, /phrase: 'послушать хор'/u);
});

test('disabled backend keeps the honest controls without exposing a loading specimen', () => {
  assert.doesNotMatch(donor, /\{enabled && \(\s*<section id="poisk"/u);
  assert.match(donor, /Что хочется сделать\?/u);
  assert.match(donor, /rows="3"/u);
  assert.match(donor, /readonly=\{!enabled\}/u);
  assert.match(donor, /data-search-submit disabled=\{!enabled\}/u);
  assert.match(donor, /Поиск пока не запускается/u);
  assert.match(donor, /Это визуальный прототип/u);
  assert.doesNotMatch(donor, /Образец состояния загрузки результатов|authorized-search__prototype-label/u);
  assert.match(donor, /data-search-skeletons hidden aria-hidden="true"/u);
  assert.doesNotMatch(donor, /data-search-skeletons hidden=\{enabled\}/u);
});

test('configured Search accepts a draft before auth and resumes it after Yandex PKCE', () => {
  assert.match(donor, /<form class="authorized-search__form" data-search-form aria-disabled=/u);
  assert.doesNotMatch(donor, /data-search-form hidden=\{enabled\}/u);
  assert.match(donor, /const searchDraftKey = 'ke_authorized_search_draft_v1'/u);
  assert.match(donor, /saveSearchDraft\(validation\.query, true\);\s*searchStartPending = false;\s*await beginYandexLogin\(\);/u);
  assert.match(donor, /restoreSearchDraft\(\);\s*handleAuthCallback\(\)/u);
  assert.match(donor, /if \(signedIn\) await runPendingSearchDraft\(\)/u);
  assert.match(donor, /removeJsonStorage\(searchDraftKey\);\s*if \(input\) input\.value = draft\.query;\s*await runSearch/u);
});

test('materialized collection routes use canonical large EventCard without bespoke result rows', () => {
  assert.match(collectionPage, /import EventCard from/u);
  assert.match(collectionPage, /events\.map\(\(event\) => <EventCard event=\{event\} mobileFlowMedia \/>\)/u);
  assert.match(collectionPage, /data-search-collection-results/u);
  assert.match(collectionPage, /noindex/u);
  assert.doesNotMatch(collectionPage, /EventListItem|authorized-search__vector-card|search-result-row/u);
  assert.match(collections, /collapseOccurrenceCards\(matches, 'per-family'\)/u);
});

test('collection claims are explicit and derived from actual event fields', () => {
  assert.match(collections, /event\.ticket\.is_free/u);
  assert.match(collections, /event\.topics\.some/u);
  assert.match(collections, /event\.topics\.includes\('STANDUP'\)/u);
  assert.match(collections, /\/джаз\/iu\.test\(event\.title\)/u);
  assert.doesNotMatch(collections, /similar|embedding|inference/iu);
});

test('general free collection is complete, DB-export-backed and keeps ongoing events', () => {
  assert.match(collections, /slug: 'besplatnye-sobytiya'/u);
  assert.match(collections, /slug === 'besplatnye-sobytiya'[\s\S]*event\.ticket\.is_free/u);
  assert.match(collections, /\(event\.end_date \|\| event\.start_date\) >= currentDate/u);
  assert.match(collections, /return slug === 'besplatnye-sobytiya' \? collapsed : collapsed\.slice\(0, 24\)/u);
  assert.match(collectionPage, /getMaterializedSearchCollectionEvents\(collection\.slug\)/u);
});

test('empty Jazz weekend remains truthful and links only later real Jazz events', () => {
  assert.match(collections, /getMaterializedSearchCollectionDateRange/u);
  assert.match(collections, /event\.start_date > weekend\.end/u);
  assert.match(collections, /\/джаз\/iu\.test\(event\.title\)/u);
  assert.match(collectionPage, /data-search-collection-empty-window/u);
  assert.match(collectionPage, /событий с джазом в названии в актуальной выгрузке нет/u);
  assert.match(collectionPage, /data-search-collection-fallback/u);
  assert.match(collectionPage, /Это не совпадения подборки выше/u);
});

test('search progress stays backend-owned while its visible surface is the submit button', () => {
  assert.match(donor, /data-search-form/u);
  assert.match(donor, /data-search-submit/u);
  assert.match(donor, /data-search-progress role="progressbar"[^>]*aria-valuemin="0"[^>]*aria-valuemax="100"[^>]*hidden/u);
  assert.match(donor, /data-search-progress-label role="status" aria-live="polite" aria-atomic="true"/u);
  assert.match(donor, /progress\.removeAttribute\('aria-valuenow'\)/u, 'request opening and idle are indeterminate/not measurable');
  assert.match(donor, /progress\.setAttribute\('aria-valuenow', String\(progressValue\)\)/u);
  assert.match(donor, /submit\.classList\.toggle\('is-loading', isLoading\)/u);
  assert.doesNotMatch(donor, /setSearchProgress\((?:28|55|74|92),/u);
  assert.match(donor, /submit\.style\.setProperty\('--search-progress', `\$\{progressValue\}%`\)/u);
  assert.match(eventLayout, /\.authorized-search__submit::before/u);
  assert.match(eventLayout, /\.authorized-search__progress[^}]*clip-path:\s*inset\(50%\)/u, 'semantic progress remains available to assistive technology without a second visual bar');
  assert.match(eventLayout, /\.authorized-search__progress-bar[^}]*background:\s*#a54821/u);
  assert.doesNotMatch(eventLayout, /\.authorized-search__progress-bar[^}]*linear-gradient|\.authorized-search__progress-bar[^}]*15,118,110/su);
});

test('search keeps a canonical-card skeleton until final results and separates the endcap', () => {
  assert.match(donor, /authorized-search__skeleton-media/u);
  assert.match(eventLayout, /\.authorized-search__skeleton-media[^}]*aspect-ratio:\s*5\s*\/\s*4/u);
  assert.match(donor, /data-search-skeletons hidden aria-hidden="true"/u, 'generated Search starts with its skeleton hidden');
  assert.doesNotMatch(eventLayout, /data-search-enabled="false"[^}]*authorized-search__skeleton/u, 'disabled builds do not reveal a loading specimen');
  assert.match(donor, /function setSkeletonLoading\(isLoading\) \{\s*if \(skeletons\) skeletons\.hidden = !isLoading;/u);
  assert.match(donor, /setSearchLoading\(true, \{ showSkeleton: !append \}\)/u);
  assert.match(donor, /if \(isVector\) \{[\s\S]*?results\.hidden = true;[\s\S]*?return \{ itemCount:/u);
  assert.doesNotMatch(donor, /if \(isVector\) \{[\s\S]*?insertVectorPreviewHtml\(/u);
  assert.match(donor, /appendSectionHeading\('Результаты поиска', 'exact'\)/u);
  assert.match(donor, /if \(!data\?\.has_more\) \{[\s\S]*?appendFeedbackPrompt[\s\S]*?appendSectionHeading\('Ещё можно посмотреть', 'discovery'\)/u);
  assert.match(donor, /Нашли то, что искали\?/u);
  assert.match(donor, /data-search-feedback="matched">Да, нашёл/u);
  assert.match(donor, /data-search-feedback="missed">Нет, не нашёл/u);
  assert.match(donor, /По вашему запросу ничего не найдено/u);
});

test('standalone Search shares the accepted textarea and button controls at every viewport', () => {
  const mobileBreakpoint = eventLayout.indexOf('@media (max-width: 560px)');
  assert.notEqual(mobileBreakpoint, -1);
  const sharedCss = eventLayout.slice(0, mobileBreakpoint);
  const mobileCss = eventLayout.slice(mobileBreakpoint);
  assert.match(sharedCss, /\.authorized-search--standalone \.authorized-search__form textarea \{[\s\S]*?min-height:\s*82px;[\s\S]*?border-bottom:\s*2px solid var\(--text\);[\s\S]*?font-size:\s*1\.3125rem;/u);
  assert.match(sharedCss, /\.authorized-search--standalone \.authorized-search__submit \{[\s\S]*?width:\s*100%;[\s\S]*?min-height:\s*50px;[\s\S]*?border-radius:\s*8px;[\s\S]*?background:\s*#221a14;/u);
  assert.match(sharedCss, /\.authorized-search--standalone \.authorized-search__submit::before \{[\s\S]*?background:\s*#98401f;/u);
  assert.doesNotMatch(mobileCss, /\.authorized-search--standalone \.authorized-search__(?:form textarea|submit)(?:::before)?\s*\{/u, 'mobile does not fork the accepted shared controls');
});

test('search progress is monotonic, epoch guarded and reset only by its owning request', () => {
  assert.match(donor, /const searchStageOrder =/u);
  assert.match(donor, /progressValue = Math\.max\(progressValue,/u);
  assert.match(donor, /if \(nextStageRank < progressStageRank\) return/u);
  assert.match(donor, /searchEpoch/u);
  assert.match(donor, /new AbortController\(\)/u);
  assert.match(donor, /activeSearchController\?\.abort\(\)/u);
  assert.match(donor, /completionResetTimer/u);
  assert.match(donor, /resetSearchProgress\(epoch\)/u);
  assert.match(donor, /setSearchProgress\(event\.progress,[^\n]+stage: event\.stage/u, 'only streamed progress events set intermediate values');
});

test('ranked search cards use the shared mobile media contract without desktop row packing', () => {
  assert.match(donor, /window\.KenigEventsRenderEventCard/u);
  assert.match(donor, /resolveMobileEventCardMedia/u);
  assert.match(donor, /items\.map\(\(item\) => \(\{ item, layout: resolveMobileEventCardMedia\(item\) \}\)\)/u);
  assert.doesNotMatch(donor, /packRelatedCardRows\(items/u);
  assert.match(donor, /renderer\([^\n]+, 'split-actions', layout\)/u);
  assert.match(eventLayout, /function createEventCardElement\(item, variant = 'split-actions', relatedLayout = null\)/u, 'legacy two-argument callers remain valid');
  assert.match(eventLayout, /relatedLayout\.presentation !== 'flow'/u);
  assert.match(eventLayout, /else if \(relatedLayout\)[\s\S]*?cardMediaPresentation[\s\S]*?'flow'/u);
  assert.match(eventLayout, /card\.style\.removeProperty\('grid-row'\)[\s\S]*?if \(relatedGridLayout\)[\s\S]*?style\.setProperty\('grid-row'/u);
  assert.match(donor, /if \(!append\) \{[\s\S]*?results\.innerHTML = ''/u, 'append pages do not wipe previously rendered cards');
  assert.doesNotMatch(searchPage, /email|magic link|otp/iu);
});

test('mobile Search fixes donor shell without rewriting its core', () => {
  assert.match(searchPage, /<EventLayout[^>]*mobileSection="search"/u);
  assert.match(collectionPage, /<EventLayout[^>]*mobileSection="search"/u);
  assert.match(bottomNav, /aria-current=\{item\.key === current \? 'page' : undefined\}/u);
  assert.match(eventLayout, /<MobileBottomNav current=\{mobileSection\} \/>/u);
  assert.doesNotMatch(bottomNav, /body:has\(/u);
  assert.match(bottomNav, /PUBLIC_MOBILE_CALENDAR_BASE_URL/u);
  assert.match(bottomNav, /PUBLIC_MOBILE_SEARCH_BASE_URL/u);
  assert.match(bottomNav, /mobileDiscoveryHref/u);
  assert.match(eventLayout, /<Reference4MobileMenu current=\{drawerCurrent\} discoveryBases=\{mobileDiscoveryBases\} badge=\{headerBadge\} \/>/u);
  assert.match(mobileMenu, /mobileDiscoveryHref\(path, discoveryBases, BASE_PATH\)/u);
  assert.match(mobileMenu, /data-reference4-fullscreen/u);
  assert.doesNotMatch(bottomNav, /preview-20260721/u);
  assert.match(collections, /PUBLIC_SEARCH_COLLECTION_REFERENCE_DATE/u);
  assert.match(collections, /getMaterializedSearchCollectionReferenceDate\(\)/u);
});
