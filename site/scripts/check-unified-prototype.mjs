import { existsSync, readFileSync, readdirSync, statSync } from 'node:fs';
import { join, resolve } from 'node:path';

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

const hubInternalLinks = [...hub.matchAll(/href="([^"]+)"/gu)]
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
if (!/<(?:h1|p)[^>]*>Партнёры<\//u.test(partners) || partners.includes('>Инфопартнёры<')) {
  throw new Error('Partners surface does not use the accepted short public label');
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

let checkedRelatedCards = 0;
for (const [, content] of eventPages.slice(0, 40)) {
  for (const match of content.matchAll(/<article[^>]*data-lab-related-card="true"[\s\S]*?<\/article>/gu)) {
    checkedRelatedCards += 1;
    const card = match[0];
    if (!card.includes('data-card-authoritative-fit="cover"')) throw new Error('Compact related card regressed to a field-producing non-cover fit');
    const crop = /data-lab-cover-crop="([0-9.]+)"/u.exec(card);
    if (card.includes('data-lab-media-kind="document"') && (!crop || Number(crop[1]) > 0.200001)) {
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
