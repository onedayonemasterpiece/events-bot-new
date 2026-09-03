import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, resolve } from 'node:path';

const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), '../../../..');
const read = (path) => readFileSync(resolve(repoRoot, path), 'utf8');

const legacyCss = read('site/src/styles/design-system.css');
const componentCss = read('site/src/components/design-system/component-foundations.css');
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

console.log('F0 normalization negative probes passed: legacy primitive owners removed, compatibility anchors retained, four icon roles and canonical SVG registry intact.');
