#!/usr/bin/env node
import { access, readFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { buildAstroFamilyGraph, graphText as astroGraphText } from './generate-astro-family-consumer-graph.mjs';
import { DEFAULT_REGISTRY_PATH, DEFAULT_REPO_ROOT, buildTokenImpactGraph, graphText } from './generate-token-impact-graph.mjs';

const scriptPath = fileURLToPath(import.meta.url);
const sortedUnique = (values) => [...new Set(values)].sort((left, right) => String(left).localeCompare(String(right)));

function fail(message) {
  throw new Error(message);
}

async function exists(filePath) {
  try {
    await access(filePath);
    return true;
  } catch {
    return false;
  }
}

function aliasCycles(tokens) {
  const forward = new Map(Object.entries(tokens).map(([token, impact]) => [token, impact.aliases_to || []]));
  const settled = new Set();
  const stack = [];
  const cycles = new Map();
  function visit(token) {
    const index = stack.indexOf(token);
    if (index !== -1) {
      const members = sortedUnique(stack.slice(index));
      cycles.set(members.join('|'), members);
      return;
    }
    if (settled.has(token)) return;
    stack.push(token);
    for (const next of forward.get(token) || []) visit(next);
    stack.pop();
    settled.add(token);
  }
  for (const token of forward.keys()) visit(token);
  return [...cycles.values()].sort((left, right) => left.join('|').localeCompare(right.join('|')));
}

function directDefinitions(token, impact) {
  return impact.definitions.filter((definition) => definition.token === token);
}

function globalRootDefinitions(token, impact) {
  return directDefinitions(token, impact)
    .filter((definition) => definition.kind === 'css' && definition.scope === ':root');
}

export async function checkTokenImpactSot({
  repoRoot = DEFAULT_REPO_ROOT,
  registryPath = DEFAULT_REGISTRY_PATH,
  graphPath,
} = {}) {
  const registryText = await readFile(path.resolve(repoRoot, registryPath), 'utf8');
  const registry = JSON.parse(registryText);
  if (registry.schema !== 'kenigevents.token-authority-registry.v1') fail(`Unexpected token registry schema: ${registry.schema}`);
  if (!Array.isArray(registry.authority?.prefixes) || !registry.authority.prefixes.length) fail('Token registry has no authority prefixes');
  if (!Array.isArray(registry.authority.shared_global_root_owners)) fail('Token registry has invalid shared_global_root_owners');
  if (!Array.isArray(registry.external_undefined_tokens)) fail('Token registry has invalid external_undefined_tokens');
  if (!Array.isArray(registry.documented_alias_cycles)) fail('Token registry has invalid documented_alias_cycles');

  const expectedAstroGraph = await buildAstroFamilyGraph({ repoRoot });
  const astroGraphPath = path.resolve(repoRoot, registry.astro_family_graph);
  if (!await exists(astroGraphPath)) fail(`Astro family graph is missing: ${registry.astro_family_graph}`);
  if (await readFile(astroGraphPath, 'utf8') !== astroGraphText(expectedAstroGraph)) {
    fail('Astro family graph drift: run node site/scripts/generate-astro-family-consumer-graph.mjs --write before checking token impact');
  }

  const expected = await buildTokenImpactGraph({ repoRoot, registryPath });
  const resolvedGraphPath = graphPath || registry.generated_graph;
  const absoluteGraphPath = path.resolve(repoRoot, resolvedGraphPath);
  if (!await exists(absoluteGraphPath)) fail(`Generated token impact graph is missing: ${resolvedGraphPath}`);
  if (await readFile(absoluteGraphPath, 'utf8') !== graphText(expected)) {
    fail('Token impact graph drift: run node site/scripts/generate-token-impact-graph.mjs --write');
  }

  const external = new Map();
  for (const entry of registry.external_undefined_tokens) {
    if (!entry?.token || !entry.reason) fail('External undefined token entries require token and reason');
    if (external.has(entry.token)) fail(`Duplicate external undefined token: ${entry.token}`);
    external.set(entry.token, entry);
  }
  const unresolved = [];
  for (const [token, impact] of Object.entries(expected.tokens)) {
    const consumers = impact.direct_consumers.filter((consumer) => consumer.token === token);
    const requiresDefinition = consumers.some((consumer) => !consumer.has_fallback);
    if (!requiresDefinition || directDefinitions(token, impact).length || external.has(token)) continue;
    unresolved.push(token);
  }
  if (unresolved.length) fail(`Undefined consumed tokens: ${unresolved.join(', ')}`);
  for (const token of external.keys()) {
    const impact = expected.tokens[token];
    if (!impact || directDefinitions(token, impact).length
      || !impact.direct_consumers.filter((consumer) => consumer.token === token).some((consumer) => !consumer.has_fallback)) {
      fail(`Stale external undefined-token documentation: ${token}`);
    }
  }

  const documentedShared = new Map();
  for (const entry of registry.authority.shared_global_root_owners) {
    if (!entry?.token || !Array.isArray(entry.source_paths) || !entry.value || !entry.reason) {
      fail('Shared global root owner entries require token, source_paths, value and reason');
    }
    if (documentedShared.has(entry.token)) fail(`Duplicate shared global root owner: ${entry.token}`);
    documentedShared.set(entry.token, entry);
  }
  const authorityTokens = Object.keys(expected.tokens).filter((token) => registry.authority.prefixes.some((prefix) => token.startsWith(prefix)));
  for (const token of authorityTokens) {
    const roots = globalRootDefinitions(token, expected.tokens[token]);
    const owners = sortedUnique(roots.map((definition) => definition.path));
    if (owners.length <= 1) continue;
    const documented = documentedShared.get(token);
    const values = sortedUnique(roots.map((definition) => definition.value));
    if (!documented || JSON.stringify(owners) !== JSON.stringify(sortedUnique(documented.source_paths))
      || values.length !== 1 || values[0] !== documented.value) {
      fail(`Conflicting global token owners for ${token}: ${owners.join(', ')}`);
    }
  }
  for (const [token, documented] of documentedShared) {
    const impact = expected.tokens[token];
    const roots = impact ? globalRootDefinitions(token, impact) : [];
    if (sortedUnique(roots.map((definition) => definition.path)).length < 2) {
      fail(`Stale shared global-root owner documentation: ${token}`);
    }
  }

  const actualCycles = aliasCycles(expected.tokens);
  const documentedCycles = registry.documented_alias_cycles.map((entry) => {
    if (!Array.isArray(entry?.tokens) || entry.tokens.length < 2 || !entry.reason) fail('Documented alias cycle requires tokens and reason');
    return sortedUnique(entry.tokens);
  });
  const actualCycleKeys = new Set(actualCycles.map((cycle) => cycle.join('|')));
  const documentedCycleKeys = new Set(documentedCycles.map((cycle) => cycle.join('|')));
  const undocumented = [...actualCycleKeys].filter((cycle) => !documentedCycleKeys.has(cycle));
  const staleDocumentation = [...documentedCycleKeys].filter((cycle) => !actualCycleKeys.has(cycle));
  if (undocumented.length) fail(`Undocumented token alias cycles: ${undocumented.join(', ')}`);
  if (staleDocumentation.length) fail(`Stale documented token alias cycles: ${staleDocumentation.join(', ')}`);

  return { ok: true, ...expected.summary, unresolved_external_tokens: external.size, alias_cycle_count: actualCycles.length };
}

function parseArgs(argv) {
  const options = {};
  for (let index = 0; index < argv.length; index += 1) {
    const value = argv[index];
    if (value === '--root') options.repoRoot = path.resolve(argv[++index]);
    else if (value === '--registry') options.registryPath = argv[++index];
    else if (value === '--graph') options.graphPath = argv[++index];
    else fail(`Unknown argument: ${value}`);
  }
  return options;
}

async function main() {
  process.stdout.write(`${JSON.stringify(await checkTokenImpactSot(parseArgs(process.argv.slice(2))), null, 2)}\n`);
}

if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) {
  main().catch((error) => {
    console.error(`[token-impact-sot] ${error.message}`);
    process.exitCode = 1;
  });
}
