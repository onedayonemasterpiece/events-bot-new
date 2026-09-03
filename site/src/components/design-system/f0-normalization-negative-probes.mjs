import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, resolve } from 'node:path';

const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), '../../../..');
const read = (path) => readFileSync(resolve(repoRoot, path), 'utf8');

const legacyCss = read('site/src/styles/design-system.css');
const componentCss = read('site/src/components/design-system/component-foundations.css');
const foundationsCss = read('site/src/components/design-system/foundations.css');
const shellCss = read('site/src/components/design-system/shell-foundations.css');
const continuityCss = read('site/src/components/design-system/continuity-aliases.css');
const foundationsTs = read('site/src/components/design-system/foundations.ts');
const badge = read('site/src/components/design-system/Badge.astro');
const field = read('site/src/components/design-system/Field.astro');
const statePanel = read('site/src/components/design-system/StatePanel.astro');

const obsoletePrimitiveOwner = /(?:^|\n)\.ke-(?:badge|field|state-panel)(?:--|__|\s|\{|:)/u;
assert.doesNotMatch(
  legacyCss,
  obsoletePrimitiveOwner,
  'design-system.css still owns obsolete Badge, Field or StatePanel selectors',
);

for (const requiredCompatibilityAnchor of [
  '.ke-button {',
  '.ke-button--icon { width: var(--ke-control-min)',
  '.ke-copy-action__check-icon',
  'content: "!"',
]) {
  assert.ok(
    legacyCss.includes(requiredCompatibilityAnchor),
    `required class-only compatibility anchor is missing: ${requiredCompatibilityAnchor}`,
  );
}

for (const [name, source, marker] of [
  ['Badge', badge, 'class={className}'],
  ['Field', field, 'class={className}'],
  ['StatePanel', statePanel, 'ke-foundation-state-panel'],
]) {
  assert.ok(source.includes('data-ke-style-owner="component-foundations"'), `${name} misses its explicit style owner`);
  assert.ok(source.includes(marker), `${name} misses its canonical private root`);
}
for (const selector of ['.ke-foundation-badge', '.ke-foundation-field', '.ke-foundation-state-panel']) {
  assert.ok(componentCss.includes(selector), `component-foundations.css misses ${selector}`);
}

const roleBlock = foundationsTs.match(/export const ICON_SIZE_ROLES = \{([\s\S]*?)\} as const;/u)?.[1] || '';
const iconRoles = Object.fromEntries(
  [...roleBlock.matchAll(/^\s*([a-z-]+):\s*(\d+),?\s*$/gmu)]
    .map((match) => [match[1], Number(match[2])]),
);
assert.deepEqual(iconRoles, { inline: 16, control: 20, action: 24, feature: 32 });

for (const binding of [
  '.ke-button .icon { width: var(--ke-icon-size-control); height: var(--ke-icon-size-control);',
  '.ke-copy-action__icon { width: var(--ke-icon-size-control); height: var(--ke-icon-size-control);',
  '.ke-filter-chip__check { width: var(--ke-icon-size-control); height: var(--ke-icon-size-control);',
  '.ke-filter-chip__check { width: var(--ke-icon-size-inline); height: var(--ke-icon-size-inline); }',
  '.ke-listing-card__free-medallion .icon { width: var(--ke-icon-size-action); height: var(--ke-icon-size-action);',
  'grid-template-columns: var(--ke-icon-size-control) auto;',
  '.ke-listing-card__utility .icon { width: var(--ke-icon-size-control); height: var(--ke-icon-size-control);',
  'grid-template-columns: var(--ke-icon-size-inline) minmax(0, 1fr);',
  '.ke-listing-card__social-proof .icon { width: var(--ke-icon-size-inline); height: var(--ke-icon-size-inline);',
  'grid-template-rows: var(--ke-icon-size-inline) auto;',
  '.ke-listing-mobile-density-switch__icon {\n    width: var(--ke-icon-size-inline);\n    height: var(--ke-icon-size-inline);',
]) {
  assert.ok(legacyCss.includes(binding), `physical icon-role binding is missing: ${binding}`);
}
assert.doesNotMatch(
  legacyCss,
  /\.icon\s*\{[^}]*\b(?:width|height):\s*-?(?:\d*\.)?\d+(?:px|rem|em)\b/gu,
  'a design-system icon selector still owns a raw glyph dimension',
);
for (const obsoleteSlot of [
  'grid-template-columns: 20px auto;',
  'grid-template-columns: 17px minmax(0, 1fr);',
  'grid-template-rows: 14px auto;',
]) {
  assert.ok(!legacyCss.includes(obsoleteSlot), `raw icon slot remains: ${obsoleteSlot}`);
}

const canonicalSvgBlock = foundationsTs.match(/export const CANONICAL_SVG_BY_ACTION = \{([\s\S]*?)\} as const satisfies/u)?.[1] || '';
for (const action of [
  'feedback.like',
  'action.calendar',
  'action.share',
  'action.close',
  'action.disclosure',
  'navigation.previous',
  'navigation.next',
  'status.success',
]) {
  assert.ok(canonicalSvgBlock.includes(`'${action}'`), `canonical SVG registry misses ${action}`);
}

const declarations = (source) => [...source.matchAll(/^\s*(--ke-[a-z0-9-]+)\s*:/gimu)].map((match) => match[1]);
assert.deepEqual(
  declarations(continuityCss),
  ['--ke-focus-intake-spinner-size'],
  'continuity-aliases.css must not repeat aliases that already have a semantic owner',
);

const registrySources = new Map([
  ['site/src/components/design-system/foundations.css', foundationsCss],
  ['site/src/components/design-system/shell-foundations.css', shellCss],
  ['site/src/components/design-system/continuity-aliases.css', continuityCss],
]);
for (const [token, expectedOwner] of [
  ['--ke-color-background-inverse-raised', 'site/src/components/design-system/foundations.css'],
  ['--ke-service-share-icon-size', 'site/src/components/design-system/shell-foundations.css'],
  ['--ke-elevation-service-share-shortcut', 'site/src/components/design-system/shell-foundations.css'],
  ['--ke-footer-social-icon-size', 'site/src/components/design-system/shell-foundations.css'],
  ['--ke-focus-intake-spinner-size', 'site/src/components/design-system/continuity-aliases.css'],
]) {
  const owners = [...registrySources.entries()]
    .filter(([, source]) => declarations(source).includes(token))
    .map(([path]) => path);
  assert.deepEqual(owners, [expectedOwner], `${token} must have exactly one F0 declaration owner`);
}

console.log('F0 normalization negative probes passed: duplicate primitive and alias owners removed; compatibility anchors retained; physical consumers use exactly four icon roles; canonical SVG registry intact.');
