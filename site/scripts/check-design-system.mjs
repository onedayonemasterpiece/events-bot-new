import { existsSync, readFileSync, readdirSync, statSync } from 'node:fs';
import { resolve } from 'node:path';

const siteDir = resolve(new URL('..', import.meta.url).pathname);
const read = (relativePath) => readFileSync(resolve(siteDir, relativePath), 'utf8');
const css = read('src/styles/design-system.css');
const layout = read('src/layouts/EventLayout.astro');
const rootPage = read('src/pages/index.astro');
const catalog = read('src/pages/lab/design-system/index.astro');
const icon = read('src/components/Icon.astro');
const copyAction = read('src/components/design-system/CopyAction.astro');
const registry = JSON.parse(read('src/data/design-system-registry.json'));
const componentPaths = [
  'src/components/design-system/Button.astro',
  'src/components/design-system/CopyAction.astro',
  'src/components/design-system/Badge.astro',
  'src/components/design-system/Field.astro',
  'src/components/design-system/Skeleton.astro',
  'src/components/design-system/StatePanel.astro',
];

for (const path of componentPaths) {
  if (!existsSync(resolve(siteDir, path))) throw new Error(`Missing design-system component: ${path}`);
  const source = read(path);
  if (/#[0-9a-f]{3,8}\b/iu.test(source)) throw new Error(`Raw color leaked into ${path}; use semantic tokens`);
}

const requiredTokens = [
  '--ke-color-brand-600', '--ke-color-brand-tag', '--ke-color-accent-600', '--ke-color-ink', '--ke-color-muted',
  '--ke-color-canvas', '--ke-color-surface', '--ke-font-sans', '--ke-font-size-300',
  '--ke-space-1', '--ke-space-4', '--ke-space-8', '--ke-radius-md', '--ke-shadow-2',
  '--ke-duration-fast', '--ke-control-min', '--ke-content-max', '--ke-color-control-inverse',
  '--ke-color-graphite-surface', '--ke-color-graphite-text', '--ke-color-graphite-muted', '--ke-shadow-graphite',
];
for (const token of requiredTokens) {
  if (!css.includes(`${token}:`)) throw new Error(`Missing canonical design token: ${token}`);
}
if (!/--ke-control-min:\s*44px/iu.test(css)) throw new Error('Touch-target token must stay at least 44px');
if (!/@media\s*\(prefers-reduced-motion:\s*reduce\)/iu.test(css)) throw new Error('Design-system CSS misses reduced-motion behavior');
if (!layout.includes("import '../styles/design-system.css'")) throw new Error('EventLayout does not load the canonical design-system CSS');
if (!rootPage.includes("import '../styles/design-system.css'")) throw new Error('Root page does not load the canonical design-system CSS');
if (!rootPage.includes('ke-button ke-button--primary') || !rootPage.includes('ke-button ke-button--secondary')) throw new Error('Root page must use canonical design-system actions');
if (/background:\s*#(?:98401f|893719)/iu.test(layout)) throw new Error('Approved brand tag colors must come from design-system tokens, not layout-local raw colors');

for (const state of ['default', 'hover', 'focus', 'pressed', 'loading', 'disabled']) {
  if (!catalog.includes(`state: '${state}'`)) throw new Error(`Button catalog misses ${state} state`);
}
for (const iconName of ['copy', 'check']) {
  if (!icon.includes(`name === '${iconName}'`) || !catalog.includes(`'${iconName}'`)) throw new Error(`CopyAction icon inventory misses ${iconName}`);
}
for (const marker of ['data-ke-copy-action', 'navigator.clipboard?.writeText', "document.execCommand('copy')", 'data-ke-copy-status', 'aria-live="polite"', "previewState=\"success\"", "previewState=\"error\""]) {
  if (!copyAction.includes(marker) && !catalog.includes(marker)) throw new Error(`CopyAction contract misses ${marker}`);
}
if (!copyAction.includes("'ke-button--icon'") || !css.includes('.ke-button--icon { width: var(--ke-control-min)')) throw new Error('CopyAction must consume the fixed 44px icon-only button contract');
if (!css.includes('.ke-copy-action__check-icon') || !css.includes('content: "!"')) throw new Error('CopyAction success/error cannot rely on color alone');
if (!catalog.includes('<CopyAction') || !catalog.includes('variant="inverse"') || !catalog.includes('design-system/CopyAction.astro')) throw new Error('Catalog misses the real CopyAction fixtures or registry row');
for (const component of ['AnnouncementsLockup', 'CalendarLink', 'EventHero', 'EventFacts', 'EventTokenMedallions', 'DesktopEventActionPanel', 'AuthorizedEventSearch', 'Skeleton', 'EventCard', 'EventListItem', 'EventMediaRail', 'InterestClubCard', 'ListingPersonalFilter', 'PersonalFeedSlot', 'SocialIcon']) {
  if (!catalog.includes(`<${component}`)) throw new Error(`Catalog misses real product component: ${component}`);
}
for (const state of ['paid-price', 'paid-unknown', 'registration', 'free-calendar', 'free-registration', 'phone-copy', 'source', 'sold-out', 'unavailable']) {
  if (!catalog.includes(`'${state}'`)) throw new Error(`Desktop graphite CTA catalog misses ${state}`);
}
for (const state of ['anonymous', 'ready', 'progress', 'skeleton', 'results', 'empty', 'error', 'quota']) {
  if (!catalog.includes(`previewState="${state}"`)) throw new Error(`Authorized search catalog misses ${state}`);
}
if (catalog.includes('.ds-force-visible :global(.personal-feed-section[hidden])')) throw new Error('Catalog must never override the runtime personal-feed hidden contract');
for (const section of ['foundations', 'actions', 'fields', 'states', 'product-components', 'registry']) {
  if (!catalog.includes(`id="${section}"`)) throw new Error(`Catalog misses section #${section}`);
}
if (!catalog.includes('<th>Версия</th>') || !catalog.includes('Проверка / миграция')) {
  throw new Error('Design-system registry must expose component version and migration status');
}
const registryRows = [...catalog.matchAll(/<tr data-ds-component="([^"]+)" data-ds-version="(\d+)"([^>]*)>/gu)];
if (registryRows.length < 18) throw new Error(`Design-system registry has only ${registryRows.length} versioned rows`);
const registryKeys = new Set();
for (const [, component, version] of registryRows) {
  const key = `${component}@${version}`;
  if (registryKeys.has(key)) throw new Error(`Duplicate design-system component version: ${key}`);
  registryKeys.add(key);
}
for (const line of catalog.split(/\r?\n/u).filter((item) => item.includes('>deprecated</'))) {
  if (!line.includes('data-ds-replaced-by=')) throw new Error('Deprecated component version must name its replacement');
}
if (!registryKeys.has('EventCard@1') || !registryKeys.has('EventCard@2') || !catalog.includes('data-ds-replaced-by="EventCard@2"')) {
  throw new Error('EventCard v1 -> v2 migration is missing from the versioned registry');
}
if (registry.schema_version !== 'kenigevents-design-system-v2' || registry.accepted_runtime_base !== 'integration/static-event-v10-system-routing@d5dab75a') {
  throw new Error('Machine design-system registry does not identify the accepted desktop runtime baseline');
}
for (const component of registry.components) {
  if (!existsSync(resolve(siteDir, component.source))) throw new Error(`Registry source is missing: ${component.source}`);
  if (!catalog.includes(`data-ds-component="${component.name}" data-ds-version="${component.version}"`)) {
    throw new Error(`Visible registry misses machine component ${component.name}@${component.version}`);
  }
  if (component.status === 'deprecated' && (!component.replaced_by || component.production_consumers.length)) {
    throw new Error(`Deprecated ${component.name}@${component.version} needs replacement and zero production consumers`);
  }
  for (const consumer of component.production_consumers) {
    const consumerSource = read(consumer);
    const importName = component.name === 'Skeleton' ? 'Skeleton' : component.name;
    if (!consumerSource.includes(importName)) throw new Error(`${consumer} does not consume registered ${component.name}@${component.version}`);
  }
  for (const state of component.states) {
    if (!catalog.includes(state) && !read(component.source).includes(state) && !css.includes(state)) throw new Error(`${component.name}@${component.version} state is unobservable: ${state}`);
  }
}
const productionEventRoute = read('src/pages/sobytiya/[slug].astro');
const desktopPage = read('src/components/DesktopEventPage.astro');
const authorizedSearch = read('src/components/AuthorizedEventSearch.astro');
const personalFeed = read('src/components/PersonalFeedSlot.astro');
if (!productionEventRoute.includes("import DesktopEventPage") || !productionEventRoute.includes('<DesktopEventPage')) throw new Error('Production event route is not mounted on DesktopEventPage@14');
if (!desktopPage.includes("import DesktopEventActionPanel") || !desktopPage.includes('<DesktopEventActionPanel')) throw new Error('DesktopEventPage@14 is not mounted on graphite DesktopEventActionPanel@2');
if (!desktopPage.includes('--ke-color-graphite-surface') || !read('src/components/DesktopEventActionPanel.astro').includes('--ke-color-graphite-surface')) throw new Error('Accepted desktop runtime does not consume semantic graphite tokens');
if (!authorizedSearch.includes("import Skeleton") || !authorizedSearch.includes('showSkeleton: !append')) throw new Error('AuthorizedEventSearch@2 must show shared skeleton before first-page cards');
if (!personalFeed.includes("import Skeleton") || !personalFeed.includes('data-personal-feed-fixture')) throw new Error('PersonalFeedSlot loading fixture must use shared Skeleton without hidden override');
if (/tone=['"]loading['"]/u.test(catalog) || /ke-state-panel--loading/u.test(css)) throw new Error('Content loading must use Skeleton, not StatePanel spinner');
function sourceFiles(relativeDir) {
  const absoluteDir = resolve(siteDir, relativeDir);
  return readdirSync(absoluteDir).flatMap((name) => {
    const absolute = resolve(absoluteDir, name);
    const relative = `${relativeDir}/${name}`;
    return statSync(absolute).isDirectory() ? sourceFiles(relative) : /\.(?:astro|ts|js|mjs)$/u.test(name) ? [relative] : [];
  });
}
const productionSources = [...sourceFiles('src/pages'), ...sourceFiles('src/components')]
  .filter((path) => !path.includes('/lab/') && !path.endsWith('/EventCtaPanel.astro') && !path.endsWith('/EventMediaRail.astro'));
for (const legacy of ['EventCtaPanel', 'EventMediaRail']) {
  const callers = productionSources.filter((path) => new RegExp(`(?:import|<)\\s*${legacy}\\b`, 'u').test(read(path)));
  if (callers.length) throw new Error(`Deprecated ${legacy} still has production callers: ${callers.join(', ')}`);
}
const productionConsumerSources = [
  'src/pages/sobytiya/[slug].astro',
  'src/pages/[preview]/index.astro',
  'src/components/PersonalFeedSlot.astro',
  'src/components/AuthorizedEventSearch.astro',
].map(read).join('\n');
if (/variant="overlay-controls"|data-feed-card-variant="overlay-controls"/u.test(productionConsumerSources)) {
  throw new Error('Deprecated EventCard v1 remains in a production consumer');
}

function hexToRgb(hex) {
  const value = hex.replace('#', '');
  return [0, 2, 4].map((offset) => Number.parseInt(value.slice(offset, offset + 2), 16) / 255);
}
function luminance(hex) {
  return hexToRgb(hex).map((channel) => channel <= 0.03928 ? channel / 12.92 : ((channel + 0.055) / 1.055) ** 2.4)
    .reduce((sum, channel, index) => sum + channel * [0.2126, 0.7152, 0.0722][index], 0);
}
function contrast(a, b) {
  const [light, dark] = [luminance(a), luminance(b)].sort((left, right) => right - left);
  return (light + 0.05) / (dark + 0.05);
}
const contrastPairs = [
  ['brand button', '#a54821', '#ffffff'],
  ['accent button', '#0f766e', '#ffffff'],
  ['body copy', '#221a14', '#fffdf8'],
  ['muted copy', '#6d6259', '#fffdf8'],
  ['success badge', '#0f6c3d', '#e6f6e9'],
  ['warning badge', '#5a3b06', '#fff8db'],
  ['danger badge', '#a92d2d', '#fff0f0'],
  ['info badge', '#1f658d', '#e7f2f7'],
];
for (const [name, foreground, background] of contrastPairs) {
  const ratio = contrast(foreground, background);
  if (ratio < 4.5) throw new Error(`${name} contrast ${ratio.toFixed(2)} is below WCAG AA 4.5:1`);
}

console.log(`Design-system check passed: ${requiredTokens.length} core tokens, ${componentPaths.length} primitives, ${registryRows.length} versioned registry rows, ${contrastPairs.length} AA contrast pairs.`);
