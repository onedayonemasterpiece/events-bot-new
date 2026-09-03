import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(here, '../../../..');
const readRepo = (path) => readFileSync(resolve(repoRoot, path), 'utf8');
const decision = JSON.parse(readRepo('site/src/components/design-system/f0-interest-club-theme-decision.v1.json'));
const productContour = readRepo('site/src/components/design-system/product-contour-foundations.css');
const routeFoundations = readRepo('site/src/components/design-system/interest-club-route-foundations.css');
const cardFoundations = readRepo('site/src/components/design-system/collection-foundations.css');
const residualFoundations = readRepo('site/src/components/design-system/interest-club-card-continuity-foundations.css');
const route = readRepo('site/src/pages/kluby-po-interesam/index.astro');
const card = readRepo('site/src/components/InterestClubCard.astro');
const strict = process.env.F0_REQUIRE_CLUB_THEME_CONSUMED === '1';

assert.equal(decision.schema, 'kenigevents.f0-interest-club-theme-decision.v1');
assert.equal(decision.status, 'PRODUCT_CLASSIFICATION_CLOSED_RESIDUAL_CARD_BINDING_READY');
assert.equal(decision.decision.owner_product_decision_required, false);
assert.equal(decision.decision.club_index_intro_palette, 'INTENTIONAL_EDITORIAL_ROUTE_IDENTITY');
assert.equal(decision.decision.club_card_palette, 'CANONICAL_REUSABLE_COMPONENT_FAMILY_NOT_ROUTE_LOCAL_DECK');

assert.equal(
  productContour.split('@import "./interest-club-card-continuity-foundations.css";').length - 1,
  1,
  'product contour must import the residual club-card palette exactly once',
);

const tokenOnly = (source, label) => {
  const withoutComments = source.replace(/\/\*[\s\S]*?\*\//gu, '');
  const selectors = [...withoutComments.matchAll(/([^{}]+)\{/gu)]
    .map((match) => match[1].trim())
    .filter(Boolean);
  assert.deepEqual(selectors, [':root'], `${label} must remain token-only`);
};
tokenOnly(routeFoundations, 'interest-club route foundations');
tokenOnly(residualFoundations, 'interest-club card continuity foundations');

const declarations = new Map();
for (const [path, source] of [
  ['interest-club-route-foundations.css', routeFoundations],
  ['collection-foundations.css', cardFoundations],
  ['interest-club-card-continuity-foundations.css', residualFoundations],
]) {
  for (const match of source.matchAll(/(--ke-[a-z0-9-]+)\s*:\s*([^;]+);/giu)) {
    const [, token, value] = match;
    const items = declarations.get(token) || [];
    items.push({ path, value: value.trim() });
    declarations.set(token, items);
  }
}

for (const binding of decision.residual_card_bindings) {
  const token = binding.replacement.match(/var\((--ke-[a-z0-9-]+)\)/u)?.[1];
  assert.ok(token, `invalid replacement token for ${binding.selector}`);
  assert.equal((declarations.get(token) || []).length, 1, `${token} must have one declaration owner`);
}

const styleSource = (source) => [...source.matchAll(/<style\b[^>]*>([\s\S]*?)<\/style>/giu)]
  .map((match) => match[1])
  .join('\n');
const routeStyles = styleSource(route);
const cardStyles = styleSource(card);
const rawColorPattern = /#[0-9a-f]{3,8}\b|(?:rgba?|hsla?)\([^)]*\)/giu;

const routeTokenized = (
  route.includes('data-ds-family="InterestClubsIndexRouteComposition"')
  && route.includes("import '../../components/design-system/product-contour-foundations.css';")
  && route.includes('<SemanticIcon name="catalog-grid" role="inline" />')
  && [...routeStyles.matchAll(rawColorPattern)].length === 0
);
const knownLegacyRoute = (
  route.includes('radial-gradient(circle at 78% 18%,rgba(151,212,255,.42),transparent 31%)')
  && route.includes('<span aria-hidden="true">▥</span>')
);
assert.ok(routeTokenized || knownLegacyRoute, 'club index route matches neither the centralized successor nor its known legacy precondition');

const routeReferences = [...routeStyles.matchAll(/var\((--ke-(?:color-clubs|clubs|elevation-clubs)-[a-z0-9-]+)\)/giu)]
  .map((match) => match[1]);
if (routeTokenized) {
  assert.ok(routeReferences.length > 20, 'centralized club route exposes too few foundation references');
  for (const token of new Set(routeReferences)) {
    assert.equal((declarations.get(token) || []).length, 1, `route token ${token} must have one declaration owner`);
  }
}

const residualStates = decision.residual_card_bindings.map((binding) => ({
  current: binding.current,
  replacement: binding.replacement,
  currentPresent: cardStyles.includes(binding.current),
  replacementPresent: cardStyles.includes(binding.replacement),
}));
for (const state of residualStates) {
  assert.ok(state.currentPresent || state.replacementPresent, `card lost both ${state.current} and ${state.replacement}`);
  assert.ok(!(state.currentPresent && state.replacementPresent), `card retains duplicate current and semantic owners for ${state.current}`);
}

const cardCentralized = residualStates.every((state) => state.replacementPresent);
const cardLegacyResidual = residualStates.every((state) => state.currentPresent);
assert.ok(cardCentralized || cardLegacyResidual, 'club card residual palette is only partially migrated');

for (const token of new Set(
  [...cardStyles.matchAll(/var\((--ke-(?:color-club-card|elevation-club|club-card)-[a-z0-9-]+)\)/giu)]
    .map((match) => match[1]),
)) {
  assert.equal((declarations.get(token) || []).length, 1, `card token ${token} must have one declaration owner`);
}

const foundationsCss = readRepo('site/src/components/design-system/foundations.css');
const iconRoles = [...foundationsCss.matchAll(/--ke-icon-size-(inline|control|action|feature)\s*:\s*([^;]+);/gu)];
assert.equal(iconRoles.length, 4, 'exactly four central icon-size roles must remain');

if (strict) {
  assert.equal(routeTokenized, true, 'strict successor must keep the centralized club index route');
  assert.equal(cardCentralized, true, 'strict successor must consume all four residual club-card palette tokens');
  assert.match(route, /data-ds-version="1"/u);
  assert.match(route, /data-ds-variant="verified-directory"/u);
  assert.match(card, /data-ds-family="InterestClubCard"/u);
  assert.match(card, /<SemanticIcon name="arrow-right" role="inline" \/>/u);
  assert.equal([...routeStyles.matchAll(rawColorPattern)].length, 0, 'strict club index route retains raw colors');
  for (const binding of decision.residual_card_bindings) {
    assert.ok(!cardStyles.includes(binding.current), `strict card retains raw color ${binding.current}`);
  }
}

console.log(JSON.stringify({
  contract: decision.schema,
  strict_mode: strict,
  club_index_classification: decision.decision.club_index_intro_palette,
  route_state: routeTokenized ? 'CENTRALIZED' : 'KNOWN_LEGACY_PRECONDITION',
  card_theme_owner: decision.decision.club_card_palette_owner,
  card_residual_state: cardCentralized ? 'CENTRALIZED' : 'KNOWN_LEGACY_PRECONDITION',
  residual_binding_count: residualStates.length,
  icon_role_count: iconRoles.length,
  rollback: decision.rollback.id,
}, null, 2));
