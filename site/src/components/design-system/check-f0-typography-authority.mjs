import assert from 'node:assert/strict';
import { readFileSync, readdirSync, statSync } from 'node:fs';
import { dirname, extname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(here, '../../../..');
const readRepo = (path) => readFileSync(resolve(repoRoot, path), 'utf8');
const authority = JSON.parse(readRepo('site/src/components/design-system/f0-typography-authority.v1.json'));
const primitiveCss = readRepo(authority.family.primitive_owner.path);
const foundationsCss = readRepo(authority.family.semantic_owner.path);

assert.equal(authority.schema, 'kenigevents.f0-typography-authority.v1');
assert.equal(authority.status, 'ACTIVE_CURRENT_SOURCE_BASELINE');
assert.equal(authority.family.owner_approval_claimed, false, 'source normalization must not invent an owner font decision');
assert.equal(authority.delivery.repository_font_binaries_are_launch_ui_authority, false);
assert.deepEqual(authority.delivery.authoritative_asset_paths, []);

const escapeRegExp = (value) => value.replace(/[.*+?^${}()|[\]\\]/gu, '\\$&');
const primitiveDeclaration = new RegExp(
  `${escapeRegExp(authority.family.primitive_owner.token)}\\s*:\\s*${escapeRegExp(authority.family.css_stack)}\\s*;`,
  'u',
);
assert.match(primitiveCss, primitiveDeclaration, 'primitive CSS no longer owns the exact current family stack');
assert.equal(
  (primitiveCss.match(new RegExp(`${escapeRegExp(authority.family.primitive_owner.token)}\\s*:`, 'gu')) || []).length,
  1,
  'primitive family stack must have one declaration owner',
);
assert.ok(
  foundationsCss.includes(`${authority.family.semantic_owner.token}: ${authority.family.semantic_owner.value};`),
  'semantic family alias does not match the declared authority',
);
assert.equal(
  (foundationsCss.match(new RegExp(`${escapeRegExp(authority.family.semantic_owner.token)}\\s*:`, 'gu')) || []).length,
  1,
  'semantic family alias must have one declaration owner',
);

for (const [token, weight] of Object.entries(authority.weights.core_roles)) {
  assert.ok(
    foundationsCss.includes(`${token}: ${weight};`),
    `core semantic weight drift: ${token} must remain ${weight}`,
  );
}

const walk = (root) => readdirSync(root).flatMap((name) => {
  const path = resolve(root, name);
  return statSync(path).isDirectory() ? walk(path) : [path];
});
const sourceFiles = walk(resolve(repoRoot, 'site/src'))
  .filter((path) => ['.astro', '.css', '.ts', '.mjs'].includes(extname(path)));
const relative = (path) => path.slice(repoRoot.length + 1).replaceAll('\\', '/');

const designSystemCss = sourceFiles.filter((path) => (
  relative(path).startsWith('site/src/components/design-system/')
  && path.endsWith('.css')
));
const weightDeclarations = new Map();
for (const path of designSystemCss) {
  const source = readFileSync(path, 'utf8');
  assert.doesNotMatch(source, /@font-face\b/iu, `${relative(path)} must not introduce a font delivery authority`);
  for (const match of source.matchAll(/(--ke-type-[a-z0-9-]+-weight)\s*:\s*([^;]+);/giu)) {
    const [, token, rawValue] = match;
    assert.ok(!weightDeclarations.has(token), `duplicate semantic weight owner: ${token}`);
    const value = rawValue.trim();
    assert.ok(
      /^\d{3}$/u.test(value) || /^var\(--ke-type-[a-z0-9-]+-weight\)$/u.test(value),
      `non-explicit semantic weight ${token}: ${value}`,
    );
    weightDeclarations.set(token, { value, path: relative(path) });
  }
  for (const match of source.matchAll(/font-family\s*:\s*([^;]+);/giu)) {
    const value = match[1].trim();
    assert.ok(
      value === 'inherit' || value.includes('var(--ke-font-family-sans)') || value.includes('var(--ke-font-sans)'),
      `${relative(path)} creates a second named family authority: ${value}`,
    );
  }
}
assert.ok(weightDeclarations.size >= Object.keys(authority.weights.core_roles).length, 'semantic weight inventory is incomplete');

const migrationByPath = new Map(
  authority.actual_consumer_migration_bindings.map((binding) => [binding.path, binding]),
);
const interConsumers = [];
for (const path of sourceFiles) {
  const rel = relative(path);
  if (rel === authority.family.primitive_owner.path || rel.startsWith('site/src/components/design-system/')) continue;
  const source = readFileSync(path, 'utf8');
  const styleSource = rel.endsWith('.astro')
    ? [...source.matchAll(/<style\b[^>]*>([\s\S]*?)<\/style>/giu)].map((match) => match[1]).join('\n')
    : source;
  if (!/\bInter\b/u.test(styleSource)) continue;
  interConsumers.push(rel);
  assert.ok(
    migrationByPath.has(rel),
    `unregistered consumer creates an Inter family bypass: ${rel}`,
  );
  assert.doesNotMatch(styleSource, /@font-face\b/iu, `${rel} must not create a separate font delivery owner`);
  assert.doesNotMatch(styleSource, /url\([^)]*\.(?:woff2?|ttf|otf)[^)]*\)/iu, `${rel} must not bind an arbitrary repository font binary`);
}

for (const binding of authority.actual_consumer_migration_bindings) {
  assert.equal(binding.owner, 'A0', `${binding.path} migration must stay A0-owned`);
  assert.ok(binding.target.includes('--ke-font-family-sans'), `${binding.path} target bypasses the semantic family token`);
  assert.ok(binding.removal_boundary, `${binding.path} misses an exact removal boundary`);
}

assert.equal(authority.thin_s_binding.family_source, '--ke-font-family-sans');
assert.equal(authority.thin_s_binding.weight_source_pattern, '--ke-type-*-weight');
assert.equal(authority.penpot_binding.asset_binding, 'NONE_IN_CURRENT_ASTRO_AUTHORITY');
assert.match(authority.penpot_binding.availability_boundary, /N0/u, 'Penpot substitution boundary must remain N0-accepted');
assert.equal(
  authority.pm0_item_11_state,
  'PARTIAL_UNTIL_CONSUMER_BYPASSES_ARE_REMOVED_AND_N0_ACCEPTS_THE_ASTRO_THIN_S_PENPOT_MAPPING',
);

console.log(JSON.stringify({
  contract: authority.schema,
  primary_family: authority.family.primary,
  primitive_stack_owner: authority.family.primitive_owner.path,
  semantic_family_owner: authority.family.semantic_owner.path,
  semantic_weight_owner_count: weightDeclarations.size,
  registered_direct_family_consumers: [...new Set(interConsumers)].sort(),
  repository_font_assets_active_for_launch_ui: 0,
  pm0_item_11: authority.pm0_item_11_state,
}, null, 2));
