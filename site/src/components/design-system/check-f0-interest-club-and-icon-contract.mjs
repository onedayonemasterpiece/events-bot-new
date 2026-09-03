import assert from 'node:assert/strict';
import { existsSync, readFileSync, readdirSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(here, '../../../..');
const readRepo = (path) => readFileSync(resolve(repoRoot, path), 'utf8');
const readOptional = (path) => existsSync(resolve(repoRoot, path)) ? readRepo(path) : '';

const foundationsCss = readRepo('site/src/components/design-system/foundations.css');
const foundationsTs = readRepo('site/src/components/design-system/foundations.ts');
const semanticIcon = readRepo('site/src/components/design-system/SemanticIcon.astro');
const productContour = readRepo('site/src/components/design-system/product-contour-foundations.css');
const clubRouteFoundations = readRepo('site/src/components/design-system/interest-club-route-foundations.css');

const expectedRoles = {
  inline: '16px',
  control: '20px',
  action: '24px',
  feature: '32px',
};
for (const [role, value] of Object.entries(expectedRoles)) {
  assert.ok(
    foundationsCss.includes(`--ke-icon-size-${role}: ${value};`),
    `missing canonical ${role} icon role`,
  );
}
assert.equal(
  [...foundationsCss.matchAll(/--ke-icon-size-(inline|control|action|feature)\s*:/gu)].length,
  4,
  'foundations.css must declare exactly four canonical icon-size roles',
);

const cssFiles = readdirSync(here)
  .filter((name) => name.endsWith('.css'))
  .map((name) => ({ name, source: readFileSync(resolve(here, name), 'utf8') }));
const declarations = new Map();
for (const { name, source } of cssFiles) {
  for (const match of source.matchAll(/(--ke-[a-z0-9-]*icon-size)\s*:\s*([^;]+);/giu)) {
    const [, token, value] = match;
    assert.ok(!declarations.has(token), `duplicate icon-size token owner: ${token}`);
    declarations.set(token, { value: value.trim(), file: name });
  }
}

const canonicalTokens = new Set(Object.keys(expectedRoles).map((role) => `--ke-icon-size-${role}`));
const resolveRole = (token, trail = []) => {
  assert.ok(!trail.includes(token), `cyclic icon-size alias: ${[...trail, token].join(' -> ')}`);
  if (canonicalTokens.has(token)) return token.replace('--ke-icon-size-', '');
  const declaration = declarations.get(token);
  assert.ok(declaration, `undefined icon-size alias: ${token}`);
  const alias = declaration.value.match(/^var\((--ke-[a-z0-9-]*icon-size)\)$/iu)?.[1];
  assert.ok(
    alias,
    `${token} in ${declaration.file} introduces non-role geometry: ${declaration.value}`,
  );
  return resolveRole(alias, [...trail, token]);
};

for (const token of declarations.keys()) resolveRole(token);

const expectedBindings = {
  '--ke-personalization-consent-icon-size': 'action',
  '--ke-club-card-arrow-icon-size': 'inline',
  '--ke-artifact-dialog-close-icon-size': 'control',
  '--ke-clubs-catalog-icon-size': 'inline',
};
for (const [token, role] of Object.entries(expectedBindings)) {
  assert.equal(resolveRole(token), role, `${token} must resolve to ${role}`);
}

assert.equal(
  productContour.split('@import "./interest-club-route-foundations.css";').length - 1,
  1,
  'product contour must import the club route foundation exactly once',
);
const tokenOnly = clubRouteFoundations.replace(/\/\*[\s\S]*?\*\//gu, '');
const selectors = [...tokenOnly.matchAll(/([^{}]+)\{/gu)]
  .map((match) => match[1].trim())
  .filter(Boolean);
assert.deepEqual(selectors, [':root'], 'club route foundation must remain token-only');

for (const fragment of [
  "'InterestProfile.consent-lock'",
  "role: 'action'",
  "'InterestClubCard.route-link'",
  "role: 'inline'",
  "'ArtifactCollection.dialog-close'",
  "role: 'control'",
  "'InterestClubsIndexRouteComposition.catalog-count'",
  "'collection.catalog': { component: './SemanticIcon.astro', name: 'catalog-grid' }",
]) {
  assert.ok(foundationsTs.includes(fragment), `foundations.ts misses ${fragment}`);
}
assert.ok(semanticIcon.includes("name === 'catalog-grid'"), 'SemanticIcon misses catalog-grid rendering');
assert.ok(semanticIcon.includes("'ke-icon-contract--four-role-v1'"), 'delegated semantic icons miss the role contract class');
assert.ok(semanticIcon.includes("'ke-icon-size-owner--foundations'"), 'delegated semantic icons miss the size-owner class');
assert.ok(semanticIcon.includes('data-ke-icon-contract="four-role-v1"'), 'local semantic icons miss role diagnostics');
assert.ok(semanticIcon.includes('data-ke-icon-size-owner="foundations.css"'), 'local semantic icons miss size-owner diagnostics');

const transitionalConsumers = [
  ['site/src/components/InterestProfile.astro', '--ke-personalization-consent-icon-size', 'action'],
  ['site/src/components/InterestClubCard.astro', '--ke-club-card-arrow-icon-size', 'inline'],
  ['site/src/components/artifacts/ArtifactCollection.astro', '--ke-artifact-dialog-close-icon-size', 'control'],
];
const pendingLocalOverrides = [];
for (const [path, token, role] of transitionalConsumers) {
  const source = readOptional(path);
  if (!source.includes(token)) continue;
  pendingLocalOverrides.push(path);
  assert.equal(resolveRole(token), role, `${path} local override is not bound to ${role}`);
}

const clubRoute = readOptional('site/src/pages/kluby-po-interesam/index.astro');
const routeCatalogState = clubRoute.includes('▥')
  ? 'A0_REPLACEMENT_PENDING'
  : clubRoute.includes('catalog-grid')
    ? 'CANONICAL_SVG_BOUND'
    : 'CATALOG_ICON_MISSING';
assert.notEqual(routeCatalogState, 'CATALOG_ICON_MISSING');

console.log(JSON.stringify({
  contract: 'F0_INTEREST_CLUB_AND_ICON_ROLE_V1',
  icon_role_count: 4,
  icon_alias_count: declarations.size - canonicalTokens.size,
  delegated_icon_diagnostics: 'CLASS_CONTRACT_PASS',
  local_icon_diagnostics: 'DATA_AND_CLASS_CONTRACT_PASS',
  pending_local_override_consumers: pendingLocalOverrides,
  route_catalog_state: routeCatalogState,
  club_route_foundation: 'TOKEN_ONLY_PASS',
}, null, 2));
