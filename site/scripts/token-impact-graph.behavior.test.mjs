import assert from 'node:assert/strict';
import { mkdtemp, mkdir, readFile, rm, writeFile } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { writeAstroFamilyGraph } from './generate-astro-family-consumer-graph.mjs';
import { impactFor, writeTokenImpactGraph } from './generate-token-impact-graph.mjs';
import { checkTokenImpactSot } from './check-token-impact-sot.mjs';

async function writeJson(file, value) {
  await mkdir(path.dirname(file), { recursive: true });
  await writeFile(file, `${JSON.stringify(value, null, 2)}\n`);
}

async function fixture() {
  const root = await mkdtemp(path.join(os.tmpdir(), 'kenig-token-impact-'));
  const source = path.join(root, 'site/src');
  const familyRegistry = {
    schema: 'kenigevents.astro-family-registry.v1',
    version: '1.0.0',
    source_root: 'site/src',
    generated_graph: 'site/src/design-system/astro-family-consumers.generated.v1.json',
    production_route_excludes: [],
    families: [],
  };
  const tokenRegistry = {
    schema: 'kenigevents.token-authority-registry.v1',
    version: '1.0.0',
    source_root: 'site/src',
    generated_graph: 'site/src/design-system/token-impact.generated.v1.json',
    astro_family_graph: familyRegistry.generated_graph,
    authority: { prefixes: ['--ke-'], shared_global_root_owners: [] },
    external_undefined_tokens: [],
    documented_alias_cycles: [],
  };
  await writeJson(path.join(source, 'design-system/astro-family-registry.v1.json'), familyRegistry);
  await writeJson(path.join(source, 'design-system/token-authority-registry.v1.json'), tokenRegistry);
  await mkdir(path.join(source, 'components'), { recursive: true });
  await mkdir(path.join(source, 'pages'), { recursive: true });
  await writeFile(path.join(source, 'components/Example.astro'), `---\nimport '../styles/tokens.css';\n---\n<div class="example">example</div>\n<style>.example { color: var(--ke-derived); }</style>\n`);
  await writeFile(path.join(source, 'pages/index.astro'), `---\nimport '../components/Example.astro';\n---\n<Example />\n`);
  await mkdir(path.join(source, 'styles'), { recursive: true });
  await writeFile(path.join(source, 'styles/tokens.css'), ':root { --ke-base: 1px; --ke-derived: var(--ke-base); }\n');
  return root;
}

async function regenerate(root) {
  await writeAstroFamilyGraph({ repoRoot: root });
  return writeTokenImpactGraph({ repoRoot: root });
}

test('token impact maps aliases through component consumers and routes', async (t) => {
  const root = await fixture();
  t.after(() => rm(root, { recursive: true, force: true }));
  const { graph } = await regenerate(root);
  await assert.doesNotReject(checkTokenImpactSot({ repoRoot: root }));
  const impact = impactFor(graph, '--ke-base');
  assert.deepEqual(impact.aliases_from, ['--ke-derived']);
  assert.deepEqual(impact.component_paths, ['site/src/components/Example.astro']);
  assert.deepEqual(impact.production_route_patterns, ['/']);
});

test('checker fails closed for conflicting global owners', async (t) => {
  const root = await fixture();
  t.after(() => rm(root, { recursive: true, force: true }));
  await writeFile(path.join(root, 'site/src/styles/other.css'), ':root { --ke-base: 2px; }\n');
  await regenerate(root);
  await assert.rejects(checkTokenImpactSot({ repoRoot: root }), /Conflicting global token owners for --ke-base/);
});

test('checker fails closed for undefined consumption and undocumented alias cycles', async (t) => {
  const root = await fixture();
  t.after(() => rm(root, { recursive: true, force: true }));
  const file = path.join(root, 'site/src/components/Example.astro');
  const original = await readFile(file, 'utf8');
  await writeFile(file, original.replace('var(--ke-derived)', 'var(--ke-missing)'));
  await regenerate(root);
  await assert.rejects(checkTokenImpactSot({ repoRoot: root }), /Undefined consumed tokens: --ke-missing/);

  await writeFile(file, original);
  await writeFile(path.join(root, 'site/src/styles/tokens.css'), ':root { --ke-base: 1px; --ke-derived: var(--ke-base); --ke-a: var(--ke-b); --ke-b: var(--ke-a); }\n');
  await regenerate(root);
  await assert.rejects(checkTokenImpactSot({ repoRoot: root }), /Undocumented token alias cycles: --ke-a\|--ke-b/);
});

test('checker rejects a stale generated impact graph', async (t) => {
  const root = await fixture();
  t.after(() => rm(root, { recursive: true, force: true }));
  await regenerate(root);
  await writeFile(path.join(root, 'site/src/styles/tokens.css'), ':root { --ke-base: 2px; }\n');
  await assert.rejects(checkTokenImpactSot({ repoRoot: root }), /Token impact graph drift/);
});
