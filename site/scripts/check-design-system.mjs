import { existsSync, readFileSync } from 'node:fs';
import { resolve } from 'node:path';

const siteDir = resolve(new URL('..', import.meta.url).pathname);
const read = (relativePath) => readFileSync(resolve(siteDir, relativePath), 'utf8');
const css = read('src/styles/design-system.css');
const layout = read('src/layouts/EventLayout.astro');
const rootPage = read('src/pages/index.astro');
const catalog = read('src/pages/lab/design-system/index.astro');
const componentPaths = [
  'src/components/design-system/Button.astro',
  'src/components/design-system/Badge.astro',
  'src/components/design-system/Field.astro',
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
  '--ke-duration-fast', '--ke-control-min', '--ke-content-max',
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
for (const component of ['AnnouncementsLockup', 'CalendarLink', 'EventHero', 'EventFacts', 'EventTokenMedallions', 'EventCtaPanel', 'EventCard', 'EventListItem', 'EventMediaRail', 'InterestClubCard', 'ListingPersonalFilter', 'PersonalFeedSlot', 'SocialIcon']) {
  if (!catalog.includes(`<${component}`)) throw new Error(`Catalog misses real product component: ${component}`);
}
if (!catalog.includes('AuthorizedEventSearch.astro')) throw new Error('Catalog registry misses conditional AuthorizedEventSearch surface');
for (const section of ['foundations', 'actions', 'fields', 'states', 'product-components', 'registry']) {
  if (!catalog.includes(`id="${section}"`)) throw new Error(`Catalog misses section #${section}`);
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

console.log(`Design-system check passed: ${requiredTokens.length} core tokens, ${componentPaths.length} primitives, ${contrastPairs.length} AA contrast pairs.`);
