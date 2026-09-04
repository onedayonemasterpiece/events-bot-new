import { existsSync, readFileSync, readdirSync, statSync } from 'node:fs';
import { join, resolve } from 'node:path';
import eventsData from '../src/data/preview-events.json' with { type: 'json' };
import interestClubsData from '../src/data/interest-clubs.json' with { type: 'json' };
import productionSurfaceContract from '../src/data/design-system-production-surface-contract.v1.json' with { type: 'json' };

const siteDir = resolve(new URL('..', import.meta.url).pathname);
const distDir = join(siteDir, 'dist');
const buildId = process.env.PREVIEW_BUILD_ID || readdirSync(distDir).find((name) => name.startsWith('preview-'));
if (!buildId || !/^preview-[a-z0-9][a-z0-9._-]*$/u.test(buildId)) throw new Error(`Invalid or missing preview build id: ${buildId || '(missing)'}`);
const root = join(distDir, buildId);
const prefix = `/${buildId}`;
const previewManifest = JSON.parse(readFileSync(join(root, 'preview-build.json'), 'utf8'));

const previewHubSource = readFileSync(join(siteDir, 'src/pages/[preview]/index.astro'), 'utf8');
const ownerFacingArchetypeIds = [
  'home',
  'today',
  'tomorrow',
  'date',
  'weekend',
  'popular',
  'collections',
  'festivals',
  'exhibitions',
  'favorites',
  'search',
  'for-me',
  'focus-group',
  'artifacts',
  'interest-clubs',
  'unusual-events',
  'event-detail',
  'information',
];
const ownerFacingRegistry = /const ownerFacingArchetypes = \[([\s\S]*?)\]\s+as const;/u.exec(previewHubSource)?.[1];
if (!ownerFacingRegistry) throw new Error('Preview hub source misses ownerFacingArchetypes registry');
const sourceOwnerFacingIds = [...ownerFacingRegistry.matchAll(/\bid:\s*'([a-z0-9-]+)'/gu)].map((match) => match[1]);
if (new Set(sourceOwnerFacingIds).size !== sourceOwnerFacingIds.length) {
  throw new Error('Preview hub source contains duplicate owner-facing archetype ids');
}
if (JSON.stringify(sourceOwnerFacingIds) !== JSON.stringify(ownerFacingArchetypeIds)) {
  throw new Error(`Preview hub source archetype registry drifted: ${sourceOwnerFacingIds.join(', ')}`);
}

function file(relative) {
  const path = join(root, relative);
  if (!existsSync(path) || !statSync(path).isFile()) throw new Error(`Missing generated route/file: ${relative}`);
  return path;
}

function html(relative) {
  return readFileSync(file(relative), 'utf8');
}

function routeTarget(pathname) {
  const local = pathname.slice(prefix.length).replace(/^\/+/, '');
  if (!local) return 'index.html';
  if (local.endsWith('/')) return `${local}index.html`;
  return /\.[a-z0-9]+$/iu.test(local) ? local : `${local}/index.html`;
}

const primaryRoutes = [
  '__preview/',
  'segodnya/',
  'zavtra/',
  'vyhodnye/',
  'populyarnoe/',
  'vystavki/',
  'festivali/',
  'neobychnoe/',
  'poisk/',
  'podborki/besplatnye-sobytiya/',
  'podborki/dzhaz-na-vyhodnyh/',
  'podborki/besplatno-s-detmi/',
  'podborki/stendap-na-etoy-nedele/',
  'dlya-menya/',
  'izbrannoe/',
  'fokus-gruppa/',
  'artefakty/',
  'kluby-po-interesam/',
  'partners/',
  'partnerstvo/',
];

for (const route of primaryRoutes) {
  const content = html(`${route}index.html`);
  if (!content.includes('noindex,nofollow,noarchive')) throw new Error(`${route} is not protected by preview noindex`);
  if (!content.includes(`rel="canonical" href="https://kenigevents.ru${prefix}/`)) throw new Error(`${route} canonical escaped the immutable preview prefix`);
}

const hub = html('__preview/index.html');
if (hub.includes(`href="${prefix}/lab/`)) {
  throw new Error('Owner Preview directory must not expose non-product /lab/ routes');
}
for (const route of primaryRoutes.filter((route) => route !== '__preview/')) {
  if (!hub.includes(`href="${prefix}/${route}`)) throw new Error(`Prototype hub does not link ${route}`);
}
for (const needle of [
  'data-unified-prototype-hub',
  'Все типы страниц в одной адаптивной сборке',
  'Реальные данные',
  'В этой сборке принят только визуальный экран',
  'Реальный авторизованный поиск пока не принят',
]) {
  if (!hub.includes(needle)) throw new Error(`Prototype hub misses status contract: ${needle}`);
}

const hubMain = hub.slice(hub.indexOf('data-unified-prototype-hub'), hub.indexOf('</main>', hub.indexOf('data-unified-prototype-hub')));
const ownerFacingCount = Number(/data-owner-archetype-family-count="(\d+)"/u.exec(hubMain)?.[1]);
if (ownerFacingCount !== sourceOwnerFacingIds.length) {
  throw new Error(`Preview hub reports ${ownerFacingCount || 0} owner-facing archetypes instead of ${sourceOwnerFacingIds.length}`);
}
const ownerFacingArchetypeLinks = [...hubMain.matchAll(/<a\b[^>]*data-owner-archetype-family="([^"]+)"[^>]*>/gu)].map((match) => {
  const href = /\bhref="([^"]+)"/u.exec(match[0])?.[1];
  if (!href) throw new Error(`Owner-facing archetype ${match[1]} has no href`);
  return { id: match[1], href };
});
const productionContractArchetypeIds = [...hubMain.matchAll(/\bdata-production-contract-archetype="([^"]+)"/gu)]
  .map((match) => match[1]);
const requiredProductionContractArchetypeIds = productionSurfaceContract.archetypes
  .filter((archetype) => archetype.required)
  .map((archetype) => archetype.id);
if (new Set(productionContractArchetypeIds).size !== productionContractArchetypeIds.length) {
  throw new Error('Preview hub renders duplicate production-contract archetype representatives');
}
const missingProductionContractArchetypes = requiredProductionContractArchetypeIds
  .filter((id) => !productionContractArchetypeIds.includes(id));
const unexpectedProductionContractArchetypes = productionContractArchetypeIds
  .filter((id) => !requiredProductionContractArchetypeIds.includes(id));
if (missingProductionContractArchetypes.length || unexpectedProductionContractArchetypes.length) {
  throw new Error(`Preview hub production-contract coverage mismatch; missing=${missingProductionContractArchetypes.join(',') || 'none'} unexpected=${unexpectedProductionContractArchetypes.join(',') || 'none'}`);
}
const builtOwnerFacingIds = ownerFacingArchetypeLinks.map((link) => link.id);
if (new Set(builtOwnerFacingIds).size !== builtOwnerFacingIds.length) {
  throw new Error('Preview hub renders duplicate owner-facing archetype links');
}
const missingOwnerFacingIds = sourceOwnerFacingIds.filter((id) => !builtOwnerFacingIds.includes(id));
const unexpectedOwnerFacingIds = builtOwnerFacingIds.filter((id) => !sourceOwnerFacingIds.includes(id));
if (missingOwnerFacingIds.length || unexpectedOwnerFacingIds.length) {
  throw new Error(`Preview hub archetype coverage mismatch; missing=${missingOwnerFacingIds.join(',') || 'none'} unexpected=${unexpectedOwnerFacingIds.join(',') || 'none'}`);
}
const expectedOwnerFacingHrefs = new Map([
  ['home', `${prefix}/`],
  ['today', `${prefix}/segodnya/`],
  ['tomorrow', `${prefix}/zavtra/`],
  ['weekend', `${prefix}/vyhodnye/`],
  ['popular', `${prefix}/populyarnoe/`],
  ['collections', `${prefix}/podborki/besplatnye-sobytiya/`],
  ['festivals', `${prefix}/festivali/`],
  ['exhibitions', `${prefix}/vystavki/`],
  ['favorites', `${prefix}/izbrannoe/`],
  ['search', `${prefix}/poisk/`],
  ['for-me', `${prefix}/dlya-menya/`],
  ['focus-group', `${prefix}/fokus-gruppa/`],
  ['artifacts', `${prefix}/artefakty/`],
  ['interest-clubs', `${prefix}/kluby-po-interesam/`],
  ['unusual-events', `${prefix}/neobychnoe/`],
  ['information', `${prefix}/partners/`],
]);
for (const link of ownerFacingArchetypeLinks) {
  if (link.id === 'event-detail') {
    const eventPrefix = `${prefix}/sobytiya/`;
    const slug = link.href.startsWith(eventPrefix) && link.href.endsWith('/')
      ? link.href.slice(eventPrefix.length, -1)
      : '';
    if (!slug || slug.includes('/')) throw new Error(`Event-detail archetype has an invalid representative href: ${link.href}`);
    continue;
  }
  if (link.id === 'date') {
    const dateMatch = new RegExp(`^${prefix.replace(/[.*+?^${}()|[\]\\]/gu, '\\$&')}/date-(\\d{4}-\\d{2}-\\d{2})/$`, 'u').exec(link.href);
    if (!dateMatch) {
      throw new Error(`Date archetype has an invalid representative href: ${link.href}`);
    }
    const nextDate = new Date(`${previewManifest.currentDate}T12:00:00Z`);
    nextDate.setUTCDate(nextDate.getUTCDate() + 1);
    if ([previewManifest.currentDate, nextDate.toISOString().slice(0, 10)].includes(dateMatch[1])) {
      throw new Error(`Date archetype must not duplicate Today or Tomorrow: ${link.href}`);
    }
    const dateContent = html(routeTarget(new URL(link.href, 'https://kenigevents.ru').pathname));
    if (!dateContent.includes('data-ds-family="DateListingSurface"') || !dateContent.includes('data-ds-variant="date"')) {
      throw new Error(`Date archetype does not materialize DateListingSurface/date: ${link.href}`);
    }
    continue;
  }
  const expectedHref = expectedOwnerFacingHrefs.get(link.id);
  if (!expectedHref || link.href !== expectedHref) {
    throw new Error(`Owner-facing archetype ${link.id} must link ${expectedHref || '(no representative)'}, got ${link.href}`);
  }
}
const hubInternalLinks = [...hubMain.matchAll(/href="([^"]+)"/gu)]
  .map((match) => match[1])
  .filter((href) => href.startsWith(`${prefix}/`));
for (const href of hubInternalLinks) {
  const url = new URL(href, 'https://kenigevents.ru');
  const target = routeTarget(url.pathname);
  if (!existsSync(join(root, target))) throw new Error(`Broken prefix-local hub link: ${href} -> ${target}`);
}

const shellRoutes = ['__preview/', 'segodnya/', 'populyarnoe/', 'poisk/', 'dlya-menya/'];
for (const route of shellRoutes) {
  const content = html(`${route}index.html`);
  for (const target of ['populyarnoe/', 'segodnya/', 'poisk/', 'dlya-menya/']) {
    if (!content.includes(`href="${prefix}/${target}`)) throw new Error(`${route} shell misses mutual navigation to ${target}`);
  }
  if (!content.includes('data-mobile-bottom-nav')) throw new Error(`${route} misses the unified mobile bottom dock`);
}

const personal = html('dlya-menya/index.html');
if (!personal.includes('data-personal-prototype') || !personal.includes('недостаточно данных')) {
  throw new Error('Personal review route must remain honest about its cold-start fallback');
}
const personalMain = personal.slice(personal.indexOf('data-personal-prototype'), personal.indexOf('</main>', personal.indexOf('data-personal-prototype')));
if (personalMain.includes('data-product-breadcrumbs')) {
  throw new Error('Personal review route regressed to decorative top-level breadcrumbs');
}
const popular = html('populyarnoe/index.html');
for (const marker of [
  'data-desktop-popular-version="V28"',
  'data-popular-representation="desktop"',
  'data-popular-representation="mobile-large"',
  'data-popular-representation="mobile-adaptive"',
]) {
  if (!popular.includes(marker)) throw new Error(`Popular composite misses ${marker}`);
}

const partners = html('partners/index.html');
if (!/<(?:h1|p)[^>]*>Партнёры<\//u.test(partners) || partners.includes('>Инфопартнёры<') || !partners.includes('https://klgd.myatom.ru/') || !partners.includes('/assets/partners/icae-kaliningrad.svg')) {
  throw new Error('Partners surface does not use the accepted short public label');
}

const search = html('poisk/index.html');
if (!/data-search-skeletons[^>]*\shidden(?:\s|>)/u.test(search)) throw new Error('Search shows loading skeletons before a request');
if (search.includes('data-product-breadcrumbs') || search.includes('data-product-parent-link')) throw new Error('Top-level Search must not render decorative breadcrumbs');

const exhibitions = html('vystavki/index.html');
if (!exhibitions.includes('data-exhibitions-prototype') || !exhibitions.includes('data-mode-switch') || exhibitions.includes('listing-stack')) {
  throw new Error('Public-review Exhibitions does not use the accepted dynamic personal donor');
}

const festivals = html('festivali/index.html');
const festivalsSource = readFileSync(join(siteDir, 'src/pages/festivali/index.astro'), 'utf8');
const festivalProjection = JSON.parse(readFileSync(join(siteDir, 'src/data/festival-timeline.json'), 'utf8'));
const projectedFestivals = Array.isArray(festivalProjection.festivals) ? festivalProjection.festivals : [];
const expectedFestivalCount = projectedFestivals.length;
const projectedFestivalMonths = new Set(projectedFestivals.map((festival) => festival?.monthKey).filter(Boolean));
const expectedFestivalMonthCount = projectedFestivalMonths.size;
if (festivalProjection.schema_version !== 'festival-timeline-static-v1' || expectedFestivalCount < 1) {
  throw new Error('Festival calendar source projection is empty or unsupported');
}
if (!festivals.includes('data-festival-timeline') || !festivals.includes(`data-festival-count="${expectedFestivalCount}"`)) {
  throw new Error(`Festival calendar must render the complete ${expectedFestivalCount}-item source projection`);
}
if ((festivals.match(/data-festival-card=/gu) || []).length !== expectedFestivalCount) {
  throw new Error('Festival calendar card count does not match its curated source projection');
}
for (const festival of projectedFestivals) {
  if (!festival?.slug || !festivals.includes(`data-festival-card="${festival.slug}"`)) {
    throw new Error(`Festival calendar misses projected item: ${festival?.slug || '(missing slug)'}`);
  }
}
if ((festivals.match(/data-protected-crop-fit="cover"/gu) || []).length < expectedFestivalCount
  || (festivals.match(/data-media-source-kind=/gu) || []).length !== expectedFestivalCount
  || (festivals.match(/data-media-confidence=/gu) || []).length !== expectedFestivalCount) {
  throw new Error('Festival cards must use reviewed, provenance-bound, full-cover media');
}
if (festivals.includes('afisha80let.visit-kaliningrad.ru')
  || festivals.includes('festival-card__body')
  || festivals.includes('festival-card__source')
  || festivalsSource.includes('font-family: Georgia')
  || festivalsSource.includes('festival-footer-note')
  || festivalsSource.includes('Все страницы прототипа')) {
  throw new Error('Festival page regressed to aggregator media, split cards, off-system type or public service copy');
}
if ((festivals.match(/festival-month__shelf/gu) || []).length !== expectedFestivalMonthCount
  || !/\.festival-month__shelf\s*\{[\s\S]*?position:\s*sticky/gu.test(festivalsSource)
  || !festivalsSource.includes('font-family: var(--ke-font-sans)')) {
  throw new Error('Festival calendar month shelves drifted from its projection or shared typography contract');
}
for (const row of festivals.matchAll(/<div[^>]*data-festival-row[^>]*>/gu)) {
  const remainder = /data-row-remainder="(true|false)"/u.exec(row[0])?.[1];
  const width = /data-row-width-fraction="([0-9.]+)"/u.exec(row[0])?.[1];
  if (!remainder || !width) throw new Error('Festival row misses fullness metadata');
  if (remainder === 'false' && Math.abs(Number(width) - 1) > 0.0001) {
    throw new Error(`Non-final festival row no longer fills 100%: ${width}`);
  }
}
for (const month of projectedFestivalMonths) {
  if (!festivals.includes(`data-festival-month="${month}"`)) throw new Error(`Festival calendar misses ${month}`);
}
for (const marker of ['Точные даты уточняются', 'Предварительный период', 'data-festival-row']) {
  if (!festivals.includes(marker)) throw new Error(`Festival calendar misses honesty/layout marker: ${marker}`);
}
if (!festivals.includes(`${prefix}/assets/festivals/timeline/city-jazz.webp`)) {
  throw new Error('Festival calendar media escaped the immutable preview prefix');
}

if (interestClubsData.source !== 'sqlite-interest-clubs-v1' || interestClubsData.clubs.length < 1) {
  throw new Error('Clubs must come from the current SQLite policy projection and cannot be empty');
}
const clubs = html('kluby-po-interesam/index.html');
if (clubs.includes('data-product-breadcrumbs') || clubs.includes('Подтверждённых клубов пока нет')) {
  throw new Error('Top-level Clubs regressed to decorative breadcrumbs or an empty donor fixture');
}
for (const club of interestClubsData.clubs) {
  const detail = html(`kluby-po-interesam/${club.slug}/index.html`);
  if (!detail.includes('data-product-breadcrumbs') || !detail.includes('data-product-parent-link') || !detail.includes('aria-current="page"')) {
    throw new Error(`Deep club page ${club.slug} misses the responsive breadcrumb contract`);
  }
}

const eventPages = [];
for (const entry of readdirSync(join(root, 'sobytiya'))) {
  const path = join(root, 'sobytiya', entry, 'index.html');
  if (existsSync(path)) eventPages.push([entry, readFileSync(path, 'utf8')]);
}
const occurrencePage = eventPages.find(([, content]) => /data-occurrence-alternative-count="[1-9]\d*"/u.test(content));
if (!occurrencePage) throw new Error('Fresh real-data build does not contain an explicit mutual occurrence specimen');
if (!occurrencePage[1].includes('data-occurrence-variant="desktop"')
  || !occurrencePage[1].includes('data-occurrence-variant="mobile"')
  || !occurrencePage[1].includes('data-occurrence-variant="practical"')
  || !occurrencePage[1].includes('event-occurrences__rows')) {
  throw new Error(`Occurrence specimen ${occurrencePage[0]} misses the always-visible detail selector contract`);
}

const busPage = eventPages.find(([, content]) => content.includes('data-desktop-transport') && /автобус/iu.test(content));
const railPage = eventPages.find(([, content]) => content.includes('data-transport-direction="outbound"') && /электрич/iu.test(content));
if (!busPage) throw new Error('Fresh real-data build misses a bus-navigation event specimen');
if (!railPage) throw new Error('Fresh real-data build misses a rail-navigation event specimen');

const compatibilityPage = eventPages.find(([, content]) => /<div[^>]*data-adaptive-event-card-grid[^>]*data-optimized-event-card-grid|<div[^>]*data-optimized-event-card-grid[^>]*data-adaptive-event-card-grid/u.test(content));
if (!compatibilityPage) {
  throw new Error('Event-detail compatibility grid must expose adaptive and legacy diagnostics on the same root');
}
const breadcrumbPage = eventPages.find(([, content]) => content.includes('data-product-breadcrumbs') && content.includes('data-product-parent-link'));
if (!breadcrumbPage) {
  throw new Error('Deep event page misses desktop semantic breadcrumbs');
}
if (eventPages.some(([, content]) => content.includes('crumbs--after-hero'))) {
  throw new Error('Deep event page regressed to the retired mobile breadcrumb/back row');
}
const semanticErrorEvent = eventsData.events.find((event) => event.image_assets?.some((asset) => asset.media_semantic_status === 'error'));
if (semanticErrorEvent) {
  const semanticErrorHtml = html(`sobytiya/${semanticErrorEvent.slug}/index.html`);
  if (!semanticErrorHtml.includes('data-selected-media-semantic-status="error"') || !/data-clean-hero-image[^>]*data-protected-crop-fit="contain"/u.test(semanticErrorHtml)) {
    throw new Error(`Text-heavy semantic-error media ${semanticErrorEvent.id} is not protected by fail-closed contain`);
  }
}
const forbiddenDurationServiceCopy = [
  'Экспериментальный прогноз длительности',
  'Gemini',
  'Gemma',
  'gemini-3.1-flash-lite',
  'confidence',
  'прогноз ИИ',
];
const forecastPage = eventPages.find(([, content]) => content.includes('data-event-end-basis="forecast"'));
if (!forecastPage) throw new Error('Fresh real-data build misses a persisted Smart Update duration specimen');
const forecastBasisCount = (forecastPage[1].match(/data-event-end-basis="forecast"/gu) || []).length;
if (forecastBasisCount !== 2
  || forecastPage[1].includes('data-event-end-basis="schedule_cutoff"')
  || forbiddenDurationServiceCopy.some((copy) => forecastPage[1].includes(copy))) {
  throw new Error(`Forecast specimen ${forecastPage[0]} must show one persisted Smart Update forecast on desktop and mobile without fallback/model copy`);
}
if (!forecastPage[1].includes('data-keyboard-event-navigation-mounted')) {
  throw new Error(`Forecast specimen ${forecastPage[0]} must mount the reviewed keyboard navigation`);
}

let checkedRelatedCards = 0;
for (const [, content] of eventPages.slice(0, 40)) {
  for (const match of content.matchAll(/<article[^>]*data-lab-related-card="true"[\s\S]*?<\/article>/gu)) {
    checkedRelatedCards += 1;
    const card = match[0];
    const hasImage = /<img[^>]*data-card-image/u.test(card);
    const targetRatio = Number(/data-lab-media-ratio="([0-9.]+)"/u.exec(card)?.[1]);
    const imageSize = /<img[^>]*data-card-image[^>]*width="(\d+)"[^>]*height="(\d+)"/u.exec(card);
    const naturalRatio = imageSize ? Number(imageSize[1]) / Number(imageSize[2]) : Number.NaN;
    const exactDocumentFrame = card.includes('data-card-authoritative-fit="contain"')
      && card.includes('data-lab-media-kind="document"')
      && Number.isFinite(targetRatio)
      && Number.isFinite(naturalRatio)
      && Math.abs(targetRatio - naturalRatio) <= 0.002;
    if (hasImage && !card.includes('data-card-authoritative-fit="cover"')) {
      if (!exactDocumentFrame) throw new Error('Compact related image regressed to a field-producing non-cover fit');
    }
    if (!hasImage && !card.includes('event-card__fallback')) throw new Error('Image-less related card misses its bounded fallback surface');
    const crop = /data-lab-cover-crop="([0-9.]+)"/u.exec(card);
    if (hasImage && card.includes('data-lab-media-kind="document"') && !exactDocumentFrame && (!crop || Number(crop[1]) > 0.200001)) {
      throw new Error(`Compact OCR/document card exceeds the 20% crop budget: ${crop?.[1] || '(missing)'}`);
    }
  }
}
if (checkedRelatedCards < 12) throw new Error(`Too few real related cards checked: ${checkedRelatedCards}`);

console.log(JSON.stringify({
  ok: true,
  buildId,
  primaryRoutes: primaryRoutes.length,
  hubInternalLinks: hubInternalLinks.length,
  ownerFacingArchetypes: sourceOwnerFacingIds.length,
  ownerFacingArchetypeUrls: Object.fromEntries(ownerFacingArchetypeLinks.map((link) => [link.id, link.href])),
  eventPages: eventPages.length,
  checkedRelatedCards,
  occurrenceSpecimen: occurrencePage[0],
  busSpecimen: busPage[0],
  railSpecimen: railPage[0],
  compatibilitySpecimen: compatibilityPage[0],
  breadcrumbSpecimen: breadcrumbPage[0],
  semanticErrorSpecimen: semanticErrorEvent?.slug || null,
  forecastSpecimen: forecastPage[0],
}, null, 2));
