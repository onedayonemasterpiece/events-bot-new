import assert from 'node:assert/strict';
import { existsSync, readFileSync, readdirSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(here, '../../../..');
const readRepo = (path) => readFileSync(resolve(repoRoot, path), 'utf8');
const strict = process.env.F0_REQUIRE_EXHIBITIONS_RESIDUAL_CONSUMED === '1';

const bindings = JSON.parse(readRepo('site/src/components/design-system/f0-exhibitions-residual-bindings.v1.json'));
const inventory = JSON.parse(readRepo('site/src/components/design-system/f0-exhibitions-token-inventory.v1.json'));
const productContour = readRepo('site/src/components/design-system/product-contour-foundations.css');
const foundationsCss = readRepo('site/src/components/design-system/foundations.css');
const foundationsTs = readRepo('site/src/components/design-system/foundations.ts');
const semanticIcon = readRepo('site/src/components/design-system/SemanticIcon.astro');
const exhibitions = readRepo(bindings.consumer);
const closureScriptPath = 'site/scripts/apply-a0-current-successor-consumer-closure.mjs';
const closureLibraryPath = 'site/scripts/a0-current-successor-consumer-closure-lib.mjs';
const closureScript = [closureScriptPath, closureLibraryPath]
  .filter((path) => existsSync(resolve(repoRoot, path)))
  .map(readRepo)
  .join('\n');

assert.equal(bindings.schema, 'kenigevents.f0-exhibitions-residual-bindings.v1');
assert.equal(bindings.version, '1.0.0');
assert.equal(bindings.contract_version, '1.10.0');
assert.equal(bindings.role, 'F0');
assert.equal(inventory.schema, 'kenigevents.f0-exhibitions-token-inventory.v1');
assert.equal(inventory.version, '1.0.0');
assert.equal(inventory.contract_version, '1.10.0');
assert.equal(inventory.role, 'F0');
assert.equal(bindings.consumer, 'site/src/components/ExhibitionsPersonalSurface.astro');
assert.equal(bindings.route, '/vystavki/');

assert.equal(
  productContour.split('@import "./product-theme-foundations.css";').length - 1,
  1,
  'product contour must import product-theme-foundations.css exactly once',
);
assert.equal(
  productContour.split('@import "./route-theme-foundations.css";').length - 1,
  1,
  'product contour must import route-theme-foundations.css exactly once',
);

const declarationFiles = new Set(inventory.declaration_files);
assert.deepEqual(declarationFiles, new Set([
  'site/src/components/design-system/product-theme-foundations.css',
  'site/src/components/design-system/route-theme-foundations.css',
]));

const cssFiles = readdirSync(here)
  .filter((name) => name.endsWith('.css'))
  .map((name) => ({
    path: `site/src/components/design-system/${name}`,
    source: readFileSync(resolve(here, name), 'utf8'),
  }));
const declarations = new Map();
for (const { path, source } of cssFiles) {
  for (const match of source.matchAll(/^\s*(--ke-[a-z0-9-]+)\s*:\s*([^;]+);/gimu)) {
    const entries = declarations.get(match[1]) || [];
    entries.push({ path, value: match[2].trim() });
    declarations.set(match[1], entries);
  }
}

assert.equal(new Set(inventory.required_tokens).size, inventory.required_tokens.length, 'required token inventory contains duplicates');
for (const token of inventory.required_tokens) {
  const owners = declarations.get(token) || [];
  assert.equal(owners.length, 1, `${token} must have exactly one declaration owner`);
  assert.ok(declarationFiles.has(owners[0].path), `${token} is owned outside the two canonical exhibitions registries`);
}

const bindingIds = bindings.bindings.map((binding) => binding.id);
assert.equal(new Set(bindingIds).size, bindingIds.length, 'binding IDs must be unique');
assert.ok(bindingIds.length >= 30, 'residual map is not a complete route cluster');
for (const binding of bindings.bindings) {
  assert.ok(binding.selector, `${binding.id} misses selector`);
  assert.ok(binding.current, `${binding.id} misses current source description`);
  assert.ok(binding.replacement, `${binding.id} misses replacement`);
  assert.ok(['A0', 'FR0'].includes(binding.consumer_owner), `${binding.id} has invalid consumer owner`);
  assert.ok(binding.browser_expectation, `${binding.id} misses browser expectation`);
  for (const token of (binding.replacement.match(/--ke-[a-z0-9-]+/giu) || []).filter((value) => !value.endsWith('-'))) {
    assert.ok(declarations.has(token), `${binding.id} references undeclared token ${token}`);
  }
}

const privateAliasesFromBindings = new Set(bindings.private_alias_classification.remove_as_theme_or_motion_owner);
const privateAliasesFromInventory = new Set(inventory.private_aliases_to_remove);
assert.deepEqual(privateAliasesFromBindings, privateAliasesFromInventory, 'private alias classification disagrees');
const runtimeVariablesFromBindings = new Set(
  bindings.private_alias_classification.retain_as_local_runtime_layout_state.map((item) => item.name),
);
const runtimeVariablesFromInventory = new Set(inventory.runtime_variables_to_retain);
assert.deepEqual(runtimeVariablesFromBindings, runtimeVariablesFromInventory, 'runtime layout variable classification disagrees');
assert.equal(bindings.private_alias_classification.blanket_no_ex_prefix_assertion_allowed, false);

for (const [token, role] of Object.entries(inventory.icon_roles)) {
  const [{ value }] = declarations.get(token) || [];
  assert.equal(value, `var(--ke-icon-size-${role})`, `${token} must resolve directly to ${role}`);
}
const cssRoles = Object.fromEntries(
  [...foundationsCss.matchAll(/--ke-icon-size-(inline|control|action|feature)\s*:\s*(\d+)px\s*;/gu)]
    .map((match) => [match[1], Number(match[2])]),
);
const tsRoleBlock = foundationsTs.match(/export const ICON_SIZE_ROLES = \{([\s\S]*?)\} as const;/u)?.[1] || '';
const tsRoles = Object.fromEntries(
  [...tsRoleBlock.matchAll(/^\s*([a-z-]+):\s*(\d+),?\s*$/gmu)]
    .map((match) => [match[1], Number(match[2])]),
);
const expectedRoles = { inline: 16, control: 20, action: 24, feature: 32 };
assert.deepEqual(cssRoles, expectedRoles);
assert.deepEqual(tsRoles, expectedRoles);
assert.equal(Object.keys(cssRoles).length, 4);
assert.equal(Object.keys(tsRoles).length, 4);

for (const name of ['arrow-left', 'arrow-right']) {
  assert.ok(semanticIcon.includes(`name === '${name}'`), `SemanticIcon misses ${name}`);
}
assert.equal(inventory.canonical_svg.gallery_previous, 'SemanticIcon arrow-left/control');
assert.equal(inventory.canonical_svg.gallery_next, 'SemanticIcon arrow-right/control');

for (const token of [...inventory.consumer_required_tokens.A0, ...inventory.consumer_required_tokens.FR0]) {
  assert.ok(inventory.required_tokens.includes(token), `consumer token ${token} is absent from inventory`);
}
assert.equal(bindings.technical_exceptions.length, 3);
assert.deepEqual(inventory.technical_visible_color_exceptions, [
  { value: '#000', context: 'CSS mask alpha composition only' },
]);
assert.equal(bindings.rollback.unit, 'F0-RB-EXHIBITIONS-RESIDUAL-THEME');

const styleSource = [...exhibitions.matchAll(/<style\b[^>]*>([\s\S]*?)<\/style>/giu)]
  .map((match) => match[1])
  .join('\n');
const escapeRegExp = (value) => value.replace(/[.*+?^${}()|[\]\\]/gu, '\\$&');
const selectorBlock = (selector) => styleSource.match(
  new RegExp(`${escapeRegExp(selector)}\\s*\\{([^{}]*)\\}`, 'u'),
)?.[1] || '';

const privateAliasHits = inventory.private_aliases_to_remove.filter((token) => (
  exhibitions.includes(`${token}:`) || exhibitions.includes(`var(${token})`)
));
const runtimeVariablesPresent = inventory.runtime_variables_to_retain.filter((token) => exhibitions.includes(token));
const rawIconBlocks = [
  '.ex-discussed svg',
  '.ex-signal svg',
  '.ex-action svg',
  '.ex-action--like svg',
  '.ex-action--reject svg',
].map((selector) => ({ selector, block: selectorBlock(selector) }))
  .filter(({ block }) => /(?:width|height)\s*:\s*\d+(?:px|rem|em)/u.test(block));
const canonicalGalleryNavigation = {
  previous: exhibitions.includes('<SemanticIcon name="arrow-left" role="control" />'),
  next: exhibitions.includes('<SemanticIcon name="arrow-right" role="control" />'),
};
const textGalleryNavigation = {
  previous: exhibitions.includes('<span aria-hidden="true">←</span>'),
  next: exhibitions.includes('<span aria-hidden="true">→</span>'),
};
const a0ConsumerTokensPresent = inventory.consumer_required_tokens.A0.filter((token) => exhibitions.includes(`var(${token})`));
const fr0ConsumerTokensPresent = inventory.consumer_required_tokens.FR0.filter((token) => exhibitions.includes(`var(${token})`));

const stylesWithoutMaskAlpha = styleSource
  .split(/\r?\n/u)
  .filter((line) => !/(?:-webkit-)?mask\s*:/u.test(line))
  .join('\n');
const rawVisibleColors = [...stylesWithoutMaskAlpha.matchAll(/#[0-9a-f]{3,8}\b|(?:rgba?|hsla?)\([^)]*\)/giu)]
  .map((match) => match[0].toLowerCase());

let closureScriptState = 'ABSENT';
if (closureScript) {
  const blanketPrivateAliasAssertion = closureScript.includes("assert.ok(!source.includes('--ex-')");
  const runtimeVariablesClassified = inventory.runtime_variables_to_retain.every((token) => closureScript.includes(token));
  const a0TokensNamed = inventory.consumer_required_tokens.A0.every((token) => closureScript.includes(token));
  const canonicalNavigationNamed = [
    '<SemanticIcon name="arrow-left" role="control" />',
    '<SemanticIcon name="arrow-right" role="control" />',
  ].every((signature) => closureScript.includes(signature));
  closureScriptState = blanketPrivateAliasAssertion
    ? 'SELF_CONTRADICTORY_BLANKET_PRIVATE_ALIAS_ASSERTION'
    : !runtimeVariablesClassified
      ? 'RUNTIME_LAYOUT_VARIABLES_UNCLASSIFIED'
      : !a0TokensNamed || !canonicalNavigationNamed
        ? 'INCOMPLETE_A0_CONSUMPTION'
        : 'F0_CONSUMPTION_PACKET_COMPLETE';
  if (strict) {
    assert.equal(blanketPrivateAliasAssertion, false, 'A0 script retains blanket --ex-* assertion');
    assert.equal(runtimeVariablesClassified, true, 'A0 script does not classify every retained runtime layout variable');
    assert.equal(a0TokensNamed, true, 'A0 script does not consume every required A0 exhibitions token');
    assert.equal(canonicalNavigationNamed, true, 'A0 script does not install canonical gallery navigation icons');
  }
}

if (strict) {
  assert.deepEqual(privateAliasHits, [], 'consumer retains private exhibition theme/motion aliases');
  assert.deepEqual(
    runtimeVariablesPresent.sort(),
    [...inventory.runtime_variables_to_retain].sort(),
    'consumer does not retain the exact local runtime layout variable set',
  );
  assert.deepEqual(rawIconBlocks, [], 'consumer retains local numeric UI icon dimensions');
  assert.deepEqual(canonicalGalleryNavigation, { previous: true, next: true });
  assert.deepEqual(textGalleryNavigation, { previous: false, next: false });
  assert.deepEqual(
    a0ConsumerTokensPresent.sort(),
    [...inventory.consumer_required_tokens.A0].sort(),
    'A0 exhibitions token consumption is incomplete',
  );
  assert.deepEqual(
    fr0ConsumerTokensPresent.sort(),
    [...inventory.consumer_required_tokens.FR0].sort(),
    'FR0 exhibitions frame-state token consumption is incomplete',
  );
  assert.deepEqual(rawVisibleColors, [], 'consumer retains raw visible colors outside the token registry');
}

console.log(JSON.stringify({
  contract: bindings.schema,
  strict_mode: strict,
  required_token_count: inventory.required_tokens.length,
  unique_declaration_owner_count: inventory.required_tokens.length,
  binding_count: bindings.bindings.length,
  icon_roles: expectedRoles,
  private_alias_hits: privateAliasHits,
  runtime_variables_present: runtimeVariablesPresent,
  raw_icon_blocks: rawIconBlocks,
  raw_visible_color_occurrences: rawVisibleColors.length,
  a0_consumer_token_count: {
    required: inventory.consumer_required_tokens.A0.length,
    present: a0ConsumerTokensPresent.length,
  },
  fr0_consumer_token_count: {
    required: inventory.consumer_required_tokens.FR0.length,
    present: fr0ConsumerTokensPresent.length,
  },
  canonical_gallery_navigation: canonicalGalleryNavigation,
  closure_script_state: closureScriptState,
  rollback_unit: bindings.rollback.unit,
}, null, 2));
