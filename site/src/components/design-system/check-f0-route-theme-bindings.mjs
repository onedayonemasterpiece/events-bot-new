import assert from 'node:assert/strict';
import { existsSync, readFileSync, readdirSync } from 'node:fs';
import { dirname, extname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(here, '../../../..');
const readRepo = (path) => readFileSync(resolve(repoRoot, path), 'utf8');
const registry = JSON.parse(readRepo('site/src/components/design-system/f0-route-theme-bindings.v1.json'));
const routeFoundationsPath = 'site/src/components/design-system/route-theme-foundations.css';
const routeFoundations = readRepo(routeFoundationsPath);
const productContour = readRepo('site/src/components/design-system/product-contour-foundations.css');
const strict = process.env.F0_REQUIRE_ROUTE_THEME_CONSUMED === '1';

assert.equal(registry.schema, 'kenigevents.f0-route-theme-bindings.v1');
assert.equal(registry.version, '1.0.0');
assert.equal(registry.role, 'F0');
assert.equal(registry.contract_version, '1.9.0');
assert.equal(registry.clusters.length, 3);
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

const requiredTokens = new Set(registry.clusters.flatMap((cluster) => cluster.required_tokens));
for (const token of requiredTokens) {
  const declarations = tokenDeclarations.get(token) || [];
  assert.equal(declarations.length, 1, `${token} must have exactly one F0 declaration owner`);
}

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
  const styleSource = path.endsWith('.astro')
    ? [...source.matchAll(/<style\b[^>]*>([\s\S]*?)<\/style>/giu)].map((match) => match[1]).join('\n')
    : source;
  const matches = [...styleSource.matchAll(/([^{}]*\.ke-icon-role[^{}]*)\{([^{}]*)\}/giu)]
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

const festival = readRepo('site/src/pages/festivali/index.astro');
const festivalTargetLiteral = festival.match(/\.festival-card__like\s*\{[\s\S]*?width:\s*clamp\(([^;]+)\);[\s\S]*?height:\s*clamp\(([^;]+)\);/u);
const festivalTargetState = festival.includes('var(--ke-festival-like-target-min)')
  ? 'CENTRAL_44PX_MIN_BOUND'
  : festivalTargetLiteral
    ? 'LOCAL_SUB_44PX_SOURCE_CONFIRMED'
    : 'UNRESOLVED';
if (strict) assert.equal(festivalTargetState, 'CENTRAL_44PX_MIN_BOUND');

console.log(JSON.stringify({
  contract: registry.schema,
  observed_successor: registry.observed_successor,
  strict_mode: strict,
  route_foundation: 'TOKEN_ONLY_PASS',
  required_token_count: requiredTokens.size,
  icon_role_count: canonicalRoles.length,
  local_icon_role_overrides: localIconRoleOverrides,
  festival_target_state: festivalTargetState,
  clusters: clusterReadback,
  rollback_units: registry.clusters.map((cluster) => cluster.rollback_unit),
}, null, 2));
