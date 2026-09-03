import assert from 'node:assert/strict';
import { existsSync, readFileSync, readdirSync } from 'node:fs';
import { dirname, extname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(here, '../../../..');
const readRepo = (path) => readFileSync(resolve(repoRoot, path), 'utf8');
const registry = JSON.parse(readRepo('site/src/components/design-system/f0-route-theme-bindings.v1.json'));
const semanticSeparation = JSON.parse(readRepo('site/src/components/design-system/f0-festival-semantic-separation.v1.json'));
const routeFoundationsPath = 'site/src/components/design-system/route-theme-foundations.css';
const routeFoundations = readRepo(routeFoundationsPath);
const productContour = readRepo('site/src/components/design-system/product-contour-foundations.css');
const strict = process.env.F0_REQUIRE_ROUTE_THEME_CONSUMED === '1';

assert.equal(registry.schema, 'kenigevents.f0-route-theme-bindings.v1');
assert.equal(registry.version, '1.0.0');
assert.equal(registry.role, 'F0');
assert.ok(['1.9.0', '1.10.0'].includes(registry.contract_version), 'unsupported route-theme registry contract');
assert.equal(registry.clusters.length, 3);
assert.equal(semanticSeparation.schema, 'kenigevents.f0-festival-semantic-separation.v1');
assert.equal(semanticSeparation.contract_version, '1.10.0');
assert.equal(semanticSeparation.role, 'F0');
assert.equal(
  productContour.split('@import "./route-theme-foundations.css";').length - 1,
  1,
  'product contour must import route-theme-foundations.css exactly once',
);

const tokenOnly = routeFoundations.replace(/\/\*[\s\S]*?\*\//gu, '');
const selectors = [...tokenOnly.matchAll(/([^{}]+)\{/gu)]
  .map((match) => match[1].trim())
  .filter(Boolean);
assert.deepEqual(selectors, [':root'], 'route-theme foundations must remain token-only');

const cssFiles = readdirSync(here)
  .filter((name) => name.endsWith('.css'))
  .map((name) => ({ name, source: readFileSync(resolve(here, name), 'utf8') }));
const tokenDeclarations = new Map();
for (const { name, source } of cssFiles) {
  for (const match of source.matchAll(/(--ke-[a-z0-9-]+)\s*:\s*([^;]+);/giu)) {
    const [, token, value] = match;
    const entries = tokenDeclarations.get(token) || [];
    entries.push({ file: name, value: value.trim() });
    tokenDeclarations.set(token, entries);
  }
}

const guideSurfaceToken = semanticSeparation.foundation_authority.guide_token;
const categorySurfaceToken = semanticSeparation.foundation_authority.category_token;
const requiredTokens = new Set([
  ...registry.clusters.flatMap((cluster) => cluster.required_tokens),
  guideSurfaceToken,
  categorySurfaceToken,
]);
for (const token of requiredTokens) {
  const declarations = tokenDeclarations.get(token) || [];
  assert.equal(declarations.length, 1, `${token} must have exactly one F0 declaration owner`);
}

const [{ value: guideSurfaceValue }] = tokenDeclarations.get(guideSurfaceToken);
const [{ value: categorySurfaceValue }] = tokenDeclarations.get(categorySurfaceToken);
assert.equal(
  guideSurfaceValue,
  categorySurfaceValue,
  'guide-like and category surfaces must preserve their equal current visual value',
);
assert.doesNotMatch(guideSurfaceValue, /var\(/u, 'guide-like surface must not alias the category role');
assert.doesNotMatch(categorySurfaceValue, /var\(/u, 'category surface must not alias the guide role');

const foundationsCss = readRepo('site/src/components/design-system/foundations.css');
const canonicalRoles = [...foundationsCss.matchAll(/--ke-icon-size-(inline|control|action|feature)\s*:\s*([^;]+);/gu)]
  .map((match) => [match[1], match[2].trim()]);
assert.deepEqual(Object.fromEntries(canonicalRoles), {
  inline: '16px',
  control: '20px',
  action: '24px',
  feature: '32px',
});
assert.equal(canonicalRoles.length, 4, 'the central icon-size role count must remain four');

for (const token of [
  '--ke-festival-guide-icon-size',
  '--ke-festival-like-icon-size',
  '--ke-club-detail-back-icon-size',
]) {
  const [{ value }] = tokenDeclarations.get(token) || [];
  assert.match(
    value || '',
    /^var\(--ke-icon-size-(?:inline|control|action|feature)\)$/u,
    `${token} must resolve directly to one of the four icon roles`,
  );
}

const clusterReadback = [];
for (const cluster of registry.clusters) {
  assert.ok(existsSync(resolve(repoRoot, cluster.consumer)), `missing consumer ${cluster.consumer}`);
  assert.ok(cluster.removal_boundary, `${cluster.id} misses a removal boundary`);
  assert.ok(cluster.rollback_unit, `${cluster.id} misses a rollback unit`);
  assert.ok(cluster.browser_expectations.length > 0, `${cluster.id} misses browser expectations`);

  const source = readRepo(cluster.consumer);
  const currentHits = cluster.source_signatures_current.filter((signature) => source.includes(signature));
  const migratedHits = cluster.required_substrings_after_migration.filter((signature) => source.includes(signature));
  assert.ok(
    currentHits.length >= Math.min(2, cluster.source_signatures_current.length) || migratedHits.length === cluster.required_substrings_after_migration.length,
    `${cluster.id} matches neither its current source precondition nor its migrated contract`,
  );

  if (strict) {
    for (const required of cluster.required_substrings_after_migration) {
      assert.ok(source.includes(required), `${cluster.id} strict binding is missing: ${required}`);
    }
    for (const forbidden of cluster.forbidden_substrings_after_migration) {
      assert.ok(!source.includes(forbidden), `${cluster.id} retains forbidden local ownership: ${forbidden}`);
    }
  }

  clusterReadback.push({
    id: cluster.id,
    consumer: cluster.consumer,
    current_signature_hits: currentHits.length,
    migrated_signature_hits: migratedHits.length,
    required_token_count: cluster.required_tokens.length,
    strict_status: strict ? 'PASS' : 'NOT_REQUESTED',
  });
}

const escapeRegExp = (value) => value.replace(/[.*+?^${}()|[\]\\]/gu, '\\$&');
const styleSource = (source) => [...source.matchAll(/<style\b[^>]*>([\s\S]*?)<\/style>/giu)]
  .map((match) => match[1])
  .join('\n');
const selectorBlock = (styles, selector) => styles.match(
  new RegExp(`${escapeRegExp(selector)}\\s*\\{([^{}]*)\\}`, 'u'),
)?.[1] || '';

const festival = readRepo('site/src/pages/festivali/index.astro');
const festivalStyles = styleSource(festival);
const guideLikeBlock = selectorBlock(festivalStyles, semanticSeparation.required_consumer_state.guide_selector);
const categoryBlock = selectorBlock(festivalStyles, semanticSeparation.required_consumer_state.category_selector);
assert.ok(guideLikeBlock, 'festival guide-like selector is missing');
assert.ok(categoryBlock, 'festival category-container selector is missing');

const guideUsesGuideRole = guideLikeBlock.includes(`background: var(${guideSurfaceToken});`);
const guideUsesCategoryRole = guideLikeBlock.includes(`background: var(${categorySurfaceToken});`);
const categoryUsesCategoryRole = categoryBlock.includes(`background: var(${categorySurfaceToken});`);
const categoryUsesGuideRole = categoryBlock.includes(`background: var(${guideSurfaceToken});`);
const festivalSemanticState = guideUsesGuideRole && categoryUsesCategoryRole
  ? 'SEMANTICALLY_SEPARATED'
  : categoryUsesGuideRole || guideUsesCategoryRole
    ? 'SEMANTIC_ROLE_CONFLATION'
    : 'PRE_MIGRATION_OR_PARTIAL';

const exhibitionsPath = 'site/src/components/ExhibitionsPersonalSurface.astro';
const exhibitions = readRepo(exhibitionsPath);
const exhibitionsStyles = styleSource(exhibitions);
const exhibitionsPrivateThemeAliases = [
  '--ex-bg',
  '--ex-surface',
  '--ex-raised',
  '--ex-border',
  '--ex-text',
  '--ex-muted',
  '--ex-blue',
  '--ex-orange',
  '--ex-red',
  '--ex-gray',
  '--ex-yellow',
  '--ex-purple',
  '--ex-green',
  '--ex-ease-cinematic',
  '--ex-ease-emphasis',
  '--ex-motion-fast',
  '--ex-motion-base',
];
const exhibitionsLocalRuntimeVariables = [
  '--ex-media-column',
  '--ex-row-gap',
  '--ex-surface-start',
  '--ex-rail-color',
];
const exhibitionsPrivateThemeHits = exhibitionsPrivateThemeAliases.filter((token) => (
  exhibitions.includes(`${token}:`) || exhibitions.includes(`var(${token})`)
));
const exhibitionsRuntimeVariablesPresent = exhibitionsLocalRuntimeVariables.filter((token) => exhibitions.includes(token));
const exhibitionsLocalIconSelectors = [
  '.ex-discussed svg',
  '.ex-signal svg',
  '.ex-action svg',
  '.ex-action--like svg',
  '.ex-action--reject svg',
];
const exhibitionsRawIconBlocks = exhibitionsLocalIconSelectors
  .map((selector) => ({ selector, block: selectorBlock(exhibitionsStyles, selector) }))
  .filter(({ block }) => /(?:width|height)\s*:\s*\d+(?:px|rem|em)/u.test(block));
const exhibitionsCanonicalGalleryNavigation = {
  previous: exhibitions.includes('<SemanticIcon name="arrow-left" role="control" />'),
  next: exhibitions.includes('<SemanticIcon name="arrow-right" role="control" />'),
};
const exhibitionsTextGalleryNavigation = {
  previous: exhibitions.includes('<span aria-hidden="true">←</span>'),
  next: exhibitions.includes('<span aria-hidden="true">→</span>'),
};
const exhibitionsExistingCentralBindings = [
  '--ke-color-exhibitions-kbd-line',
  '--ke-color-exhibitions-kbd-line-strong',
  '--ke-color-exhibitions-kbd-surface',
  '--ke-color-exhibitions-kbd-text',
  '--ke-color-exhibitions-kbd-shadow',
  '--ke-color-exhibitions-mode-surface',
  '--ke-color-exhibitions-mode-selected-line',
  '--ke-color-exhibitions-sticky-surface',
  '--ke-color-exhibitions-blue-glow',
  '--ke-color-exhibitions-orange-glow',
  '--ke-color-exhibitions-timeline-mid',
  '--ke-color-exhibitions-timeline-end',
  '--ke-color-exhibitions-row-start',
  '--ke-color-exhibitions-row-end',
  '--ke-elevation-exhibitions-row',
  '--ke-elevation-exhibitions-row-hover',
  '--ke-elevation-exhibitions-row-keyboard',
  '--ke-exhibitions-signal-icon-size',
  '--ke-exhibitions-action-icon-size',
  '--ke-exhibitions-gallery-arrow-icon-size',
];
const exhibitionsExistingCentralBindingsUsed = exhibitionsExistingCentralBindings.filter((token) => exhibitions.includes(`var(${token})`));

const closureScriptPath = 'site/scripts/apply-a0-current-successor-consumer-closure.mjs';
let closureScriptState = 'ABSENT';
let exhibitionsClosureScriptState = 'ABSENT';
if (existsSync(resolve(repoRoot, closureScriptPath))) {
  const closureScript = readRepo(closureScriptPath);
  const unsafeGlobalReplacement = closureScript.includes(
    "['background: rgba(165, 72, 33, 0.1);', 'background: var(--ke-color-festival-guide-like-surface);']",
  );
  const namesBothRoles = closureScript.includes(guideSurfaceToken) && closureScript.includes(categorySurfaceToken);
  closureScriptState = unsafeGlobalReplacement
    ? 'UNSAFE_GLOBAL_RAW_VALUE_REPLACEMENT'
    : namesBothRoles
      ? 'CONTEXT_SCOPED_SEMANTIC_REPLACEMENTS'
      : 'NO_FESTIVAL_SEPARATION_IMPLEMENTATION';

  const blanketExAliasAssertion = closureScript.includes("assert.ok(!source.includes('--ex-')");
  const namesExRuntimeVariables = exhibitionsLocalRuntimeVariables.some((token) => closureScript.includes(token));
  const namesExCentralIconRoles = [
    '--ke-exhibitions-signal-icon-size',
    '--ke-exhibitions-action-icon-size',
    '--ke-exhibitions-gallery-arrow-icon-size',
  ].every((token) => closureScript.includes(token));
  const namesExCanonicalNavigation = closureScript.includes('<SemanticIcon name="arrow-left" role="control" />')
    && closureScript.includes('<SemanticIcon name="arrow-right" role="control" />');
  exhibitionsClosureScriptState = blanketExAliasAssertion && exhibitionsRuntimeVariablesPresent.length > 0
    ? 'SELF_CONTRADICTORY_BLANKET_EX_ALIAS_POSTCONDITION'
    : !namesExCentralIconRoles || !namesExCanonicalNavigation
      ? 'INCOMPLETE_F0_ICON_AND_SVG_CONSUMPTION'
      : namesExRuntimeVariables
        ? 'EXPLICIT_RUNTIME_VARIABLE_PRESERVATION'
        : 'NO_EXHIBITIONS_RUNTIME_VARIABLE_CLASSIFICATION';

  if (strict) {
    assert.equal(
      unsafeGlobalReplacement,
      false,
      'A0 closure script globally maps the shared raw festival value to the guide role',
    );
    assert.equal(namesBothRoles, true, 'A0 closure script must name both distinct festival surface roles');
    assert.equal(
      blanketExAliasAssertion,
      false,
      'A0 exhibitions postcondition rejects every --ex-* token although the source intentionally retains local runtime layout variables',
    );
    assert.equal(namesExRuntimeVariables, true, 'A0 closure script must classify the retained exhibitions runtime layout variables');
    assert.equal(namesExCentralIconRoles, true, 'A0 closure script must bind exhibition glyphs to the four central icon roles');
    assert.equal(namesExCanonicalNavigation, true, 'A0 closure script must replace gallery text arrows with canonical SemanticIcon navigation');
  }
}

if (strict) {
  assert.equal(festivalSemanticState, 'SEMANTICALLY_SEPARATED');
  assert.equal(guideUsesCategoryRole, false, 'guide-like cue is bound to the category role');
  assert.equal(categoryUsesGuideRole, false, 'category container is bound to the guide-like role');
  assert.deepEqual(exhibitionsPrivateThemeHits, [], 'exhibitions retains private theme/motion aliases');
  assert.deepEqual(
    exhibitionsRuntimeVariablesPresent.sort(),
    [...exhibitionsLocalRuntimeVariables].sort(),
    'exhibitions runtime layout variables were removed or silently reclassified',
  );
  assert.deepEqual(exhibitionsRawIconBlocks, [], 'exhibitions retains raw local UI icon dimensions');
  assert.deepEqual(
    exhibitionsCanonicalGalleryNavigation,
    { previous: true, next: true },
    'exhibitions gallery navigation must use canonical SemanticIcon identities',
  );
  assert.deepEqual(
    exhibitionsTextGalleryNavigation,
    { previous: false, next: false },
    'exhibitions gallery retains text-arrow glyph duplicates',
  );
  assert.deepEqual(
    exhibitionsExistingCentralBindingsUsed.sort(),
    [...exhibitionsExistingCentralBindings].sort(),
    'exhibitions does not consume every already-published F0 palette/elevation/icon binding',
  );
}

const walk = (root) => readdirSync(root, { withFileTypes: true }).flatMap((entry) => {
  const path = resolve(root, entry.name);
  return entry.isDirectory() ? walk(path) : [path];
});
const sourceRoot = resolve(repoRoot, 'site/src');
const consumerFiles = walk(sourceRoot).filter((path) => (
  ['.astro', '.css'].includes(extname(path))
  && !path.startsWith(here)
));
const relative = (path) => path.slice(repoRoot.length + 1).replaceAll('\\', '/');
const localIconRoleOverrides = [];
for (const path of consumerFiles) {
  const source = readFileSync(path, 'utf8');
  const styles = path.endsWith('.astro') ? styleSource(source) : source;
  const matches = [...styles.matchAll(/([^{}]*\.ke-icon-role[^{}]*)\{([^{}]*)\}/giu)]
    .filter((match) => /(?:^|;)\s*(?:width|height)\s*:/iu.test(match[2]));
  if (matches.length) localIconRoleOverrides.push({ path: relative(path), count: matches.length });
}
if (strict) {
  assert.deepEqual(
    localIconRoleOverrides,
    [],
    'strict successor retains consumer-local width/height ownership for .ke-icon-role',
  );
}

const festivalTargetLiteral = festival.match(/\.festival-card__like\s*\{[\s\S]*?width:\s*clamp\(([^;]+)\);[\s\S]*?height:\s*clamp\(([^;]+)\);/u);
const festivalTargetState = festival.includes('var(--ke-festival-like-target-min)')
  ? 'CENTRAL_44PX_MIN_BOUND'
  : festivalTargetLiteral
    ? 'LOCAL_SUB_44PX_SOURCE_CONFIRMED'
    : 'UNRESOLVED';
if (strict) assert.equal(festivalTargetState, 'CENTRAL_44PX_MIN_BOUND');

console.log(JSON.stringify({
  contract: registry.schema,
  contract_compatibility: [registry.contract_version, semanticSeparation.contract_version],
  observed_successor: registry.observed_successor,
  strict_mode: strict,
  route_foundation: 'TOKEN_ONLY_PASS',
  required_token_count: requiredTokens.size,
  icon_role_count: canonicalRoles.length,
  local_icon_role_overrides: localIconRoleOverrides,
  festival_target_state: festivalTargetState,
  festival_semantic_separation: {
    state: festivalSemanticState,
    guide_token: guideSurfaceToken,
    category_token: categorySurfaceToken,
    equal_current_values: guideSurfaceValue === categorySurfaceValue,
    closure_script_state: closureScriptState,
  },
  exhibitions_closure: {
    private_theme_hits: exhibitionsPrivateThemeHits,
    retained_runtime_variables: exhibitionsRuntimeVariablesPresent,
    raw_local_icon_blocks: exhibitionsRawIconBlocks,
    canonical_gallery_navigation: exhibitionsCanonicalGalleryNavigation,
    text_gallery_navigation: exhibitionsTextGalleryNavigation,
    existing_central_binding_count: exhibitionsExistingCentralBindings.length,
    existing_central_bindings_used: exhibitionsExistingCentralBindingsUsed,
    closure_script_state: exhibitionsClosureScriptState,
  },
  clusters: clusterReadback,
  rollback_units: [
    ...registry.clusters.map((cluster) => cluster.rollback_unit),
    semanticSeparation.rollback.unit,
  ],
}, null, 2));
