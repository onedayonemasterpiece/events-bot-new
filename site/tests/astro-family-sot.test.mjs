import assert from 'node:assert/strict';
import { mkdtemp, mkdir, readFile, writeFile } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import test from 'node:test';
import { fileURLToPath } from 'node:url';
import {
  buildAstroFamilyGraph,
  writeAstroFamilyGraph,
} from '../scripts/generate-astro-family-consumer-graph.mjs';
import { checkAstroFamilySot } from '../scripts/check-astro-family-sot.mjs';

const siteRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const repoRoot = path.resolve(siteRoot, '..');
const registryPath = 'site/src/design-system/astro-family-registry.v1.json';
const graphPath = 'site/src/design-system/astro-family-consumers.generated.v1.json';

const requiredFamilies = [
  'EventCard', 'EventLayout', 'HomePage', 'EventDetailRouteComposition',
  'ExhibitionsRouteComposition', 'InterestClubsIndexRouteComposition',
  'MobileBottomNav', 'StaticSiteAuthRuntime', 'EventTransportSchedule',
];

async function json(file) {
  return JSON.parse(await readFile(file, 'utf8'));
}

async function writeJson(file, value) {
  await mkdir(path.dirname(file), { recursive: true });
  await writeFile(file, `${JSON.stringify(value, null, 2)}\n`, 'utf8');
}

function fixtureFamily(overrides = {}) {
  return {
    id: 'ExampleCard',
    version: 1,
    astro_root: 'site/src/components/ExampleCard.astro',
    style_owners: ['site/src/components/example-card.css'],
    variants: ['default'],
    states: ['ready'],
    nested_families: [],
    runtime_factories: [],
    runtime_clients: [],
    consumer_signals: [],
    explicit_override_exceptions: [],
    penpot_binding: null,
    ...overrides,
  };
}

async function createFixture({ family = fixtureFamily(), source, extraFamilies = [], extraFiles = {} } = {}) {
  const root = await mkdtemp(path.join(os.tmpdir(), 'astro-family-sot-'));
  const registry = {
    schema: 'kenigevents.astro-family-registry.v1',
    version: '1.0.0',
    source_root: 'site/src',
    generated_graph: graphPath,
    production_route_excludes: [],
    production_surface_contract: {
      path: 'site/src/data/production-surface-contract.json',
      required_component_family_ids: [],
      required_archetype_owners: [],
    },
    families: [family, ...extraFamilies],
  };
  const rootSource = source || `---\nimport './example-card.css';\n---\n<article data-ds-family="ExampleCard" data-ds-version="1" data-ds-variant="default" data-ds-state="ready"></article>\n`;
  await writeJson(path.resolve(root, registryPath), registry);
  await writeJson(path.resolve(root, 'site/src/data/production-surface-contract.json'), {
    component_families: [], archetypes: [],
  });
  await mkdir(path.resolve(root, 'site/src/components'), { recursive: true });
  await mkdir(path.resolve(root, 'site/src/pages'), { recursive: true });
  await writeFile(path.resolve(root, 'site/src/components/ExampleCard.astro'), rootSource, 'utf8');
  await writeFile(path.resolve(root, 'site/src/components/example-card.css'), '.example-card {}\n', 'utf8');
  await writeFile(path.resolve(root, 'site/src/pages/index.astro'), `---\nimport ExampleCard from '../components/ExampleCard.astro';\n---\n<ExampleCard />\n`, 'utf8');
  for (const [relative, content] of Object.entries(extraFiles)) {
    const absolute = path.resolve(root, relative);
    await mkdir(path.dirname(absolute), { recursive: true });
    await writeFile(absolute, content, 'utf8');
  }
  await writeAstroFamilyGraph({ repoRoot: root, registryPath });
  return { root, registry };
}

test('complete canonical production-surface registry, graph and checker agree', async () => {
  const registry = await json(path.resolve(repoRoot, registryPath));
  for (const id of requiredFamilies) assert.ok(registry.families.some((family) => family.id === id), `registry misses ${id}`);
  assert.equal(registry.families.length, 109);
  assert.ok(registry.production_surface_contract);
  for (const family of registry.families) {
    for (const field of [
      'id', 'version', 'astro_root', 'style_owners', 'variants', 'states',
      'nested_families', 'runtime_factories', 'runtime_clients', 'consumer_signals',
      'explicit_override_exceptions', 'penpot_binding',
    ]) assert.ok(Object.hasOwn(family, field), `${family.id} misses ${field}`);
  }
  const generated = await json(path.resolve(repoRoot, graphPath));
  const rebuilt = await buildAstroFamilyGraph({ repoRoot, registryPath });
  assert.deepEqual(generated, rebuilt);
  const checked = await checkAstroFamilySot({ repoRoot, registryPath });
  assert.equal(checked.family_count, registry.families.length);
  assert.equal(checked.graph_summary.family_count, registry.families.length);
  assert.ok(checked.graph_summary.protocol_consumer_edges > 0);
  assert.ok(checked.graph_summary.production_route_edges > 0);
});

test('impact command reports direct, runtime and production-route consumers', () => {
  const run = spawnSync(process.execPath, [
    path.resolve(siteRoot, 'scripts/generate-astro-family-consumer-graph.mjs'),
    '--impact',
    'EventCard',
  ], { cwd: repoRoot, encoding: 'utf8' });
  assert.equal(run.status, 0, run.stderr);
  const impact = JSON.parse(run.stdout);
  assert.equal(impact.family, 'EventCard');
  assert.ok(impact.direct_consumers.includes('site/src/components/AdaptiveEventCardGrid.astro'));
  assert.ok(impact.runtime_factories.some((entry) => entry.path === 'site/src/layouts/EventLayout.astro'));
  assert.ok(impact.runtime_consumers.some((entry) => entry.path === 'site/src/components/FavoritesSurface.astro'));
  assert.ok(impact.production_route_patterns.length > 0);
});

test('checker permits multi-identity roots but rejects duplicate style owners', async () => {
  const sibling = fixtureFamily({ id: 'SiblingRoot', style_owners: [] });
  const first = await createFixture({
    source: '<article data-ds-family="ExampleCard" data-ds-version="1" data-ds-variant="default" data-ds-state="ready"></article><section data-ds-family="SiblingRoot" data-ds-version="1" data-ds-variant="default" data-ds-state="ready"></section>\n',
    extraFamilies: [sibling],
  });
  assert.equal((await checkAstroFamilySot({ repoRoot: first.root, registryPath })).family_count, 2);

  const duplicateStyle = fixtureFamily({
    id: 'DuplicateStyle',
    astro_root: 'site/src/components/DuplicateStyle.astro',
  });
  const second = await createFixture({
    extraFamilies: [duplicateStyle],
    extraFiles: {
      'site/src/components/DuplicateStyle.astro': '<article data-ds-family="DuplicateStyle" data-ds-version="1" data-ds-variant="default" data-ds-state="ready"></article>\n',
    },
  });
  await assert.rejects(
    checkAstroFamilySot({ repoRoot: second.root, registryPath }),
    /Duplicate style owner/u,
  );
});

test('checker rejects missing source and unknown variant or state', async () => {
  const missing = await createFixture();
  const missingRegistry = structuredClone(missing.registry);
  missingRegistry.families[0].astro_root = 'site/src/components/Missing.astro';
  await writeJson(path.resolve(missing.root, registryPath), missingRegistry);
  await assert.rejects(
    checkAstroFamilySot({ repoRoot: missing.root, registryPath }),
    /missing source/u,
  );

  const unknownVariant = await createFixture({
    source: '<article data-ds-family="ExampleCard" data-ds-version="1" data-ds-variant="other" data-ds-state="ready"></article>\n',
  });
  await assert.rejects(
    checkAstroFamilySot({ repoRoot: unknownVariant.root, registryPath }),
    /unknown variant/u,
  );

  const unknownState = await createFixture({
    source: '<article data-ds-family="ExampleCard" data-ds-version="1" data-ds-variant="default" data-ds-state="unknown"></article>\n',
  });
  await assert.rejects(
    checkAstroFamilySot({ repoRoot: unknownState.root, registryPath }),
    /unknown state/u,
  );
});

test('checker rejects unregistered runtime consumers', async () => {
  const family = fixtureFamily({
    runtime_factories: [{ path: 'site/src/runtime/factory.ts', symbols: ['ExampleFactory'] }],
    runtime_clients: [{ path: 'site/src/runtime/client.ts', symbols: ['ExampleFactory'] }],
  });
  const fixture = await createFixture({
    family,
    extraFiles: {
      'site/src/runtime/factory.ts': 'export const ExampleFactory = () => null;\n',
      'site/src/runtime/client.ts': 'void ExampleFactory;\n',
      'site/src/runtime/rogue.ts': 'void ExampleFactory;\n',
    },
  });
  await assert.rejects(
    checkAstroFamilySot({ repoRoot: fixture.root, registryPath }),
    /Unregistered runtime consumer/u,
  );
});

test('checker rejects generated graph drift', async () => {
  const fixture = await createFixture();
  await writeFile(path.resolve(fixture.root, graphPath), '{}\n', 'utf8');
  await assert.rejects(
    checkAstroFamilySot({ repoRoot: fixture.root, registryPath }),
    /graph drift/u,
  );
});

test('checker does not truncate an Astro opening tag at > inside an expression', async () => {
  const fixture = await createFixture({
    family: fixtureFamily({ variants: [], states: ['ready', 'empty'], explicit_override_exceptions: [{ kind: 'dynamic_identity', attribute: 'state', values: ['ready', 'empty'], source_regex: String.raw`data-ds-state=\{items\.length > 0 \? 'ready' : 'empty'\}` }] }),
    source: `<article data-ds-state={items.length > 0 ? 'ready' : 'empty'} data-ds-family="ExampleCard" data-ds-version="1"></article>\n`,
  });
  assert.equal((await checkAstroFamilySot({ repoRoot: fixture.root, registryPath })).family_count, 1);
});

test('checker fails closed when canonical production coverage is incomplete', async () => {
  const fixture = await createFixture();
  const registry = await json(path.resolve(fixture.root, registryPath));
  await writeJson(path.resolve(fixture.root, 'site/src/data/production-surface-contract.json'), {
    component_families: [{ id: 'required', required: true, source_files: ['site/src/components/Required.astro'] }],
    archetypes: [],
  });
  registry.production_surface_contract.required_component_family_ids = ['required'];
  await writeJson(path.resolve(fixture.root, registryPath), registry);
  await assert.rejects(checkAstroFamilySot({ repoRoot: fixture.root, registryPath }), /Required production component source is unregistered/u);
});

test('checker fails closed on an unregistered source-published identity', async () => {
  const fixture = await createFixture({
    extraFiles: {
      'site/src/components/Rogue.astro': '<aside data-ds-family="RogueSurface" data-ds-version="1"></aside>\n',
    },
  });
  await assert.rejects(checkAstroFamilySot({ repoRoot: fixture.root, registryPath }), /Unregistered canonical owners: RogueSurface/u);
});

test('service-only closure is excluded but a shared product import cannot hide identities', async () => {
  const fixture = await createFixture({ extraFiles: {
    'site/src/pages/lab/index.astro': `---\nimport Demo from '../../components/ServiceOnly.astro';\nimport ExampleCard from '../../components/ExampleCard.astro';\n---\n<Demo /><ExampleCard />`,
    'site/src/components/ServiceOnly.astro': '<aside data-ds-family="ServiceDemo"></aside>',
  } });
  fixture.registry.production_route_excludes = ['site/src/pages/lab/**'];
  await writeJson(path.resolve(fixture.root, registryPath), fixture.registry);
  await writeAstroFamilyGraph({ repoRoot: fixture.root, registryPath });
  const graph = await buildAstroFamilyGraph({ repoRoot: fixture.root, registryPath });
  assert.ok(graph.product_scope.service_only_sources.includes('site/src/components/ServiceOnly.astro'));
  assert.ok(graph.product_scope.checked_sources.includes('site/src/components/ExampleCard.astro'));
  await checkAstroFamilySot({ repoRoot: fixture.root, registryPath });
  await writeFile(path.resolve(fixture.root, 'site/src/pages/index.astro'), `---\nimport Demo from '../components/ServiceOnly.astro';\n---\n<Demo />`);
  await assert.rejects(checkAstroFamilySot({ repoRoot: fixture.root, registryPath }), /Unregistered canonical owners: ServiceDemo/u);
});

test('runtime association keeps a service-imported dependency in product checks', async () => {
  const fixture = await createFixture({
    family: fixtureFamily({ runtime_clients: [{ path: 'site/src/runtime/client.ts', symbols: ['ExampleFactory'] }] }),
    extraFiles: {
      'site/src/pages/lab/index.astro': `---\nimport '../../runtime/client.ts';\n---\n<div />`,
      'site/src/runtime/client.ts': `import '../components/RuntimeChild.astro';\nvoid ExampleFactory;`,
      'site/src/components/RuntimeChild.astro': '<div data-ds-component="UnknownRuntimeChild"></div>',
    },
  });
  fixture.registry.production_route_excludes = ['site/src/pages/lab/**'];
  await writeJson(path.resolve(fixture.root, registryPath), fixture.registry);
  await assert.rejects(checkAstroFamilySot({ repoRoot: fixture.root, registryPath }), /Unregistered canonical owners: UnknownRuntimeChild/u);
});


test('published identity values cannot hide behind empty registry metadata', async () => {
  for (const field of ['variants', 'states']) {
    const fixture = await createFixture({ family: fixtureFamily({ [field]: [] }) });
    await assert.rejects(checkAstroFamilySot({ repoRoot: fixture.root, registryPath }), /empty registry metadata/u);
  }
});

test('parametric identities require an explicit checked source and value pattern', async () => {
  const source = '<article data-ds-family="ExampleCard" data-ds-version="1" data-ds-variant="default" data-ds-state={progress}></article>';
  const noContract = await createFixture({ family: fixtureFamily({ states: [] }), source });
  await assert.rejects(checkAstroFamilySot({ repoRoot: noContract.root, registryPath }), /empty metadata and no value pattern/u);
  const family = fixtureFamily({ states: [], explicit_override_exceptions: [{
    kind: 'dynamic_identity', attribute: 'state', value_pattern: '^found-[0-9]+-of-[0-9]+$',
    source_regex: 'data-ds-state=\\{progress\\}',
  }] });
  const valid = await createFixture({ family, source });
  assert.equal((await checkAstroFamilySot({ repoRoot: valid.root, registryPath })).family_count, 1);
  const drift = await createFixture({ family, source: source.replace('{progress}', '{unreviewed}') });
  await assert.rejects(checkAstroFamilySot({ repoRoot: drift.root, registryPath }), /source no longer matches/u);
});


test('product inline Astro styles require one actual source owner', async () => {
  const source = '<article data-ds-family="ExampleCard" data-ds-version="1" data-ds-variant="default" data-ds-state="ready"></article><style>article { color: red; }</style>';
  const missing = await createFixture({ source });
  await assert.rejects(checkAstroFamilySot({ repoRoot: missing.root, registryPath }), /inline Astro styles have no registered source owner/u);
  const owned = await createFixture({ source, family: fixtureFamily({ style_owners: ['site/src/components/ExampleCard.astro'] }) });
  assert.equal((await checkAstroFamilySot({ repoRoot: owned.root, registryPath })).family_count, 1);
});
