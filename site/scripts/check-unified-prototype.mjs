import { existsSync, readFileSync, readdirSync, statSync } from 'node:fs';
import { join, resolve } from 'node:path';
import eventsData from '../src/data/preview-events.json' with { type: 'json' };
import interestClubsData from '../src/data/interest-clubs.json' with { type: 'json' };

const siteDir = resolve(new URL('..', import.meta.url).pathname);
const distDir = join(siteDir, 'dist');
const buildId = process.env.PREVIEW_BUILD_ID || readdirSync(distDir).find((name) => name.startsWith('preview-'));
if (!buildId || !/^preview-[a-z0-9][a-z0-9._-]*$/u.test(buildId)) throw new Error(`Invalid or missing preview build id: ${buildId || '(missing)'}`);
const root = join(distDir, buildId);
const prefix = `/${buildId}`;

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
  'poisk/',
  'podborki/dzhaz-na-vyhodnyh/',
  'podborki/besplatno-s-detmi/',
  'podborki/stendap-na-etoy-nedele/',
  'dlya-menya/',
  'kluby-po-interesam/',
  'partners/',
  'partnerstvo/',
  'lab/exhibitions-personal/',
  'lab/design-system/',
  'lab/occurrences/',
  'lab/medallions/',
];

for (const route of primaryRoutes) {
  const content = html(`${route}index.html`);
  if (!content.includes('noindex,nofollow,noarchive')) throw new Error(`${route} is not protected by preview noindex`);
  if (!content.includes(`rel="canonical" href="https://kenigevents.ru${prefix}/`)) throw new Error(`${route} canonical escaped the immutable preview prefix`);
}

const hub = html('__preview/index.html');
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
if (!personal.includes('data-personal-prototype') || !personal.includes('Cold start') || !personal.includes('недостаточно оценок')) {
  throw new Error('Personal review route must remain honest about its cold-start fallback');
}
const personalMain = personal.slice(personal.indexOf('data-personal-prototype'), personal.indexOf('</main>', personal.indexOf('data-personal-prototype')));
if (!personalMain.includes('personal-page__feed-list') || !personalMain.includes('data-personal-feed-results') || !personalMain.includes('data-optimized-event-card-grid')) {
  throw new Error('Personal review route must share the optimized Event-detail large-card grid');
}
if (personalMain.includes('data-product-breadcrumbs')) {
  throw new Error('Personal review route regressed to decorative top-level breadcrumbs');
}
const personalFamilyKeys = [...personalMain.matchAll(/data-occurrence-member-ids="([^"]+)"/gu)].map((match) => match[1]);
if (personalFamilyKeys.length < 6 || new Set(personalFamilyKeys).size !== personalFamilyKeys.length) {
  throw new Error('Personal review route must render a finite per-family real-data card set');
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
if (!festivals.includes('data-festival-timeline') || !festivals.includes('data-festival-count="21"')) {
  throw new Error('Festival calendar must render the complete 21-item timeline');
}
if ((festivals.match(/data-festival-card=/gu) || []).length !== 21) {
  throw new Error('Festival calendar card count does not match its curated source projection');
}
for (const month of ['july', 'august', 'september', 'october', 'november', 'december']) {
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
  || !occurrencePage[1].includes('event-occurrences__schedule')) {
  throw new Error(`Occurrence specimen ${occurrencePage[0]} misses the always-visible detail selector contract`);
}

const busPage = eventPages.find(([, content]) => content.includes('data-desktop-transport') && /автобус/iu.test(content));
const railPage = eventPages.find(([, content]) => content.includes('data-transport-direction="outbound"') && /электрич/iu.test(content));
if (!busPage) throw new Error('Fresh real-data build misses a bus-navigation event specimen');
if (!railPage) throw new Error('Fresh real-data build misses a rail-navigation event specimen');

const event6686 = eventsData.events.find((event) => event.id === 6686);
const event6529 = eventsData.events.find((event) => event.id === 6529);
if (!event6686 || !event6529) throw new Error('Fresh real-data build misses the 6686/6529 acceptance regressions');
const event6686Html = html(`sobytiya/${event6686.slug}/index.html`);
const event6529Html = html(`sobytiya/${event6529.slug}/index.html`);
if (!event6686Html.includes('data-product-breadcrumbs') || !event6686Html.includes('data-product-parent-link')) {
  throw new Error('Deep event page misses desktop semantic breadcrumbs');
}
if (event6686Html.includes('crumbs--after-hero')) {
  throw new Error('Deep event page regressed to the retired mobile breadcrumb/back row');
}
if (!event6686Html.includes('data-selected-media-semantic-status="error"') || !/data-clean-hero-image[^>]*data-protected-crop-fit="contain"/u.test(event6686Html)) {
  throw new Error('Text-heavy semantic-error media 6686 is not protected by fail-closed contain');
}
const forbiddenDurationServiceCopy = [
  'Экспериментальный прогноз длительности',
  'Gemini',
  'Gemma',
  'gemini-3.1-flash-lite',
  'confidence',
  'прогноз ИИ',
];
const forecastBasisCount = (event6529Html.match(/data-event-end-basis="forecast"/gu) || []).length;
if (forecastBasisCount !== 2
  || event6529Html.includes('data-event-end-basis="schedule_cutoff"')
  || !event6529Html.includes('17:50')
  || !event6529Html.includes('18:56')
  || event6529Html.includes('06:42')
  || forbiddenDurationServiceCopy.some((copy) => event6529Html.includes(copy))) {
  throw new Error('6529 must show the same clean Smart Update forecast on desktop and mobile, without fallback/model copy/next-morning trains');
}
if (!event6529Html.includes('data-keyboard-event-navigation-mounted')) {
  throw new Error('6529 named preview page must mount the reviewed keyboard navigation');
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
  eventPages: eventPages.length,
  checkedRelatedCards,
  occurrenceSpecimen: occurrencePage[0],
  busSpecimen: busPage[0],
  railSpecimen: railPage[0],
}, null, 2));
