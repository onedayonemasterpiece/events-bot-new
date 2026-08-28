import { existsSync, readFileSync } from 'node:fs';
import { resolve } from 'node:path';

const siteDir = resolve(new URL('..', import.meta.url).pathname);
const read = (relativePath) => readFileSync(resolve(siteDir, relativePath), 'utf8');
const css = read('src/styles/design-system.css');
const layout = read('src/layouts/EventLayout.astro');
const rootPage = read('src/pages/index.astro');
const catalog = read('src/pages/lab/design-system/index.astro');
const icon = read('src/components/Icon.astro');
const copyAction = read('src/components/design-system/CopyAction.astro');
const componentPaths = [
  'src/components/design-system/Button.astro',
  'src/components/design-system/CopyAction.astro',
  'src/components/design-system/Badge.astro',
  'src/components/design-system/Field.astro',
  'src/components/design-system/StatePanel.astro',
  'src/components/listings/ListingEventCard.astro',
  'src/components/listings/ListingDiscoveryRail.astro',
  'src/components/listings/ExactTimeTimeline.astro',
  'src/components/listings/ListingTimeNav.astro',
  'src/components/listings/ListingTimeMarker.astro',
  'src/components/listings/ListingControls.astro',
  'src/components/listings/ListingPageHeader.astro',
  'src/components/listings/WeekendTimeMatrix.astro',
  'src/components/listings/WeekendEditorialTimeline.astro',
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
  '--ke-duration-fast', '--ke-control-min', '--ke-content-max', '--ke-content-wide-max', '--ke-listing-time-gutter', '--ke-color-control-inverse',
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
for (const component of ['AnnouncementsLockup', 'CalendarLink', 'EventHero', 'EventFacts', 'EventTokenMedallions', 'EventCtaPanel', 'EventCard', 'EventListItem', 'EventMediaRail', 'InterestClubCard', 'ListingPersonalFilter', 'ListingPageHeader', 'ListingControls', 'ListingDiscoveryRail', 'ListingTimeNav', 'ListingTimeMarker', 'ExactTimeTimeline', 'WeekendEditorialTimeline', 'ListingEventCard', 'PersonalFeedSlot', 'SocialIcon']) {
  const renderedDirectly = catalog.includes(`<${component}`);
  const renderedThroughDiscovery = component === 'ListingControls'
    && catalog.includes('<ListingDiscoveryRail')
    && read('src/components/listings/ListingDiscoveryRail.astro').includes('<ListingControls');
  if (!renderedDirectly && !renderedThroughDiscovery) throw new Error(`Catalog misses real product component: ${component}`);
}
if (!catalog.includes('AuthorizedEventSearch.astro')) throw new Error('Catalog registry misses conditional AuthorizedEventSearch surface');
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
if (!registryKeys.has('ListingPersonalFilter@1') || !registryKeys.has('ListingPersonalFilter@2') || !catalog.includes('data-ds-replaced-by="ListingPersonalFilter@2"')) {
  throw new Error('ListingPersonalFilter v1 -> v2 migration is missing from the versioned registry');
}
if (!registryKeys.has('ListingControls@1') || !registryKeys.has('ListingControls@2') || !registryKeys.has('ListingControls@3') || !catalog.includes('data-ds-replaced-by="ListingControls@3"')) {
  throw new Error('ListingControls v1 -> v2 -> v3 migration is missing from the versioned registry');
}
if (!registryKeys.has('ListingTimeNav@2') || !registryKeys.has('ListingTimeMarker@1') || !catalog.includes('data-ds-replaced-by="ListingTimeNav@2"')) throw new Error('Listing time navigation/marker v2 migration is missing from the registry');
if (!registryKeys.has('ExactTimeTimeline@2') || !catalog.includes('data-ds-replaced-by="ExactTimeTimeline@2"')) throw new Error('ExactTimeTimeline v2 migration is missing from the registry');
if (!registryKeys.has('WeekendEditorialTimeline@2') || !catalog.includes('data-ds-replaced-by="WeekendEditorialTimeline@2"')) throw new Error('WeekendTimeMatrix replacement is missing from the registry');
if (!registryKeys.has('ListingDiscoveryRail@1') || !registryKeys.has('ListingDiscoveryRail@5') || !registryKeys.has('ListingDiscoveryRail@6') || !catalog.includes('data-ds-replaced-by="ListingDiscoveryRail@6"')) {
  throw new Error('ListingDiscoveryRail v5 -> v6 migration is missing from the registry');
}
if (!registryKeys.has('ListingEventCard@1') || !registryKeys.has('ListingEventCard@2') || !registryKeys.has('ListingEventCard@3') || !registryKeys.has('ListingEventCard@4') || !registryKeys.has('ListingEventCard@5') || !catalog.includes('data-ds-replaced-by="ListingEventCard@5"')) throw new Error('ListingEventCard v1 -> v2 -> v3 -> v4 -> v5 migration is missing from the registry');
if (!registryKeys.has('EventCard@1') || !registryKeys.has('EventCard@2') || !catalog.includes('data-ds-replaced-by="EventCard@2"')) {
  throw new Error('EventCard v1 -> v2 migration is missing from the versioned registry');
}
const listingProductionSources = [
  'src/pages/segodnya/index.astro', 'src/pages/zavtra/index.astro', 'src/pages/vyhodnye/index.astro',
  'src/pages/vystavki/index.astro', 'src/pages/populyarnoe/index.astro',
  'src/components/listings/ListingControls.astro',
].map(read).join('\n');
if (/<ListingPersonalFilter(?![^>]*version=\{2\})/u.test(listingProductionSources)) throw new Error('Deprecated ListingPersonalFilter v1 remains in a production consumer');
const discoveryRailConsumers = [
  'src/components/listings/DateListingSurface.astro',
  'src/components/listings/PopularListingSurface.astro',
  'src/components/listings/WeekendListingSurface.astro',
].map(read).join('\n');
if (/<ListingDiscoveryRail(?![^>]*version=\{6\})/u.test(discoveryRailConsumers)) {
  throw new Error('Deprecated ListingDiscoveryRail v5 remains in a production consumer');
}
if (!read('src/components/listings/WeekendListingSurface.astro').includes('surface="floating-island"')) {
  throw new Error('Weekend production consumer must use ListingDiscoveryRail v6 Floating Island');
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
