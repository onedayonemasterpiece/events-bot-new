#!/usr/bin/env node
import { access, readFile, readdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import {
  DEFAULT_REGISTRY_PATH,
  DEFAULT_REPO_ROOT,
  buildAstroFamilyGraph,
  graphText,
} from './generate-astro-family-consumer-graph.mjs';

const REQUIRED_FIELDS = [
  'id', 'version', 'astro_root', 'style_owners', 'variants', 'states',
  'nested_families', 'runtime_factories', 'runtime_clients', 'consumer_signals',
  'explicit_override_exceptions', 'penpot_binding',
];
const SOURCE_EXTENSIONS = new Set(['.astro', '.js', '.jsx', '.mjs', '.cjs', '.ts', '.tsx']);
const FORBIDDEN_TOP_LEVEL_KEYS = new Set([
  'role_receipts', 'comment_ids', 'branch_checkpoints', 'kaggle_runs',
  'v0_matrices', 'historical_research_catalogs', 'penpot_mutation_receipts',
]);

const scriptPath = fileURLToPath(import.meta.url);
const posix = (value) => value.split(path.sep).join('/');
const sorted = (values) => [...values].sort((left, right) => String(left).localeCompare(String(right)));
const fail = (message) => { throw new Error(message); };

async function exists(filePath) {
  try { await access(filePath); return true; } catch { return false; }
}

async function walkSource(directory, repoRoot) {
  const files = [];
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    const absolute = path.join(directory, entry.name);
    if (entry.isDirectory()) files.push(...await walkSource(absolute, repoRoot));
    else if (entry.isFile() && SOURCE_EXTENSIONS.has(path.extname(entry.name))) files.push(posix(path.relative(repoRoot, absolute)));
  }
  return files.sort((left, right) => left.localeCompare(right));
}

function duplicates(values) {
  const seen = new Set();
  return values.filter((value) => (seen.has(value) ? true : (seen.add(value), false)));
}

function exception(family, kind, attribute) {
  return family.explicit_override_exceptions.find((item) => item.kind === kind && (attribute === undefined || item.attribute === attribute));
}

function literalIdentityValues(source, attribute) {
  const escaped = attribute.replace(/[.*+?^${}()|[\]\\]/gu, '\\$&');
  const matches = [
    ...source.matchAll(new RegExp(`data-ds-${escaped}\\s*=\\s*["']([^"']+)["']`, 'gu')),
    ...source.matchAll(new RegExp(`["']data-ds-${escaped}["']\\s*:\\s*["']([^"']+)["']`, 'gu')),
  ];
  return [...new Set(matches.map((match) => match[1]))];
}

/** Find the end of an Astro/HTML opening tag without treating `>` in `{...}` as its terminator. */
export function findOpeningTagEnd(source, start) {
  let quote = null;
  let braceDepth = 0;
  for (let index = start + 1; index < source.length; index += 1) {
    const character = source[index];
    if (quote) {
      if (character === '\\') { index += 1; continue; }
      if (character === quote) quote = null;
      continue;
    }
    if (character === '"' || character === "'" || character === '`') { quote = character; continue; }
    if (character === '{') { braceDepth += 1; continue; }
    if (character === '}' && braceDepth > 0) { braceDepth -= 1; continue; }
    if (character === '>' && braceDepth === 0) return index;
  }
  return -1;
}

function identityBlock(source, id) {
  const match = new RegExp(`data-ds-family\\s*=\\s*["']${id}["']`, 'u').exec(source);
  if (!match) return null;
  const start = source.lastIndexOf('<', match.index);
  const end = start === -1 ? -1 : findOpeningTagEnd(source, start);
  return start === -1 || end === -1 ? null : source.slice(start, end + 1);
}

function readIdentityAttribute(block, attribute) {
  const match = new RegExp(`data-ds-${attribute}\\s*=\\s*(?:\"([^\"]*)\"|'([^']*)'|\\{([^}\\n]+)\\})`, 'u').exec(block);
  if (!match) return null;
  if (match[1] !== undefined || match[2] !== undefined) return { kind: 'literal', value: match[1] ?? match[2] };
  return { kind: 'dynamic', value: match[3].trim() };
}

function validateDynamicIdentity(family, attribute, source) {
  const contract = exception(family, 'dynamic_identity', attribute);
  if (!contract) fail(`${family.id} has dynamic data-ds-${attribute} without a fail-closed registry contract`);
  if (Array.isArray(contract.values) && JSON.stringify(sorted(contract.values)) !== JSON.stringify(sorted(family[`${attribute}s`]))) {
    fail(`${family.id} dynamic ${attribute} values drift from the registry`);
  }
  let expression;
  try { expression = new RegExp(contract.source_regex, 'u'); } catch (error) { fail(`${family.id} invalid ${attribute} source_regex: ${error.message}`); }
  if (!expression.test(source)) fail(`${family.id} dynamic ${attribute} source no longer matches its registry contract`);
}

function validateIdentity(family, source) {
  if (exception(family, 'identity_attribute_absent')) return;
  if (!family.astro_root.endsWith('.astro')) {
    if (!exception(family, 'non_astro_root')) fail(`${family.id} uses a non-Astro root without an explicit exception`);
    return;
  }
  const block = identityBlock(source, family.id);
  if (!block) fail(`${family.id} root misses literal data-ds-family identity`);
  if (!block.includes(`data-ds-version="${family.version}"`)) fail(`${family.id} root misses data-ds-version="${family.version}"`);
  for (const attribute of ['variant', 'state']) {
    const values = family[`${attribute}s`];
    const identity = readIdentityAttribute(block, attribute);
    if (!identity) {
      if (values.length) fail(`${family.id} declares ${attribute}s but root has no data-ds-${attribute}`);
      continue;
    }
    if (!values.length) continue;
    if (identity.kind === 'dynamic') { validateDynamicIdentity(family, attribute, source); continue; }
    const observed = attribute === 'state' ? identity.value.split(/\s+/u).filter(Boolean) : [identity.value];
    for (const value of observed) if (!values.includes(value)) fail(`${family.id} root publishes unknown ${attribute}: ${value}`);
  }
}

function validateRuntimeEntry(family, entry, kind) {
  if (!entry || typeof entry.path !== 'string' || !Array.isArray(entry.symbols) || !entry.symbols.length) fail(`${family.id} has invalid ${kind} entry`);
  if (duplicates(entry.symbols).length) fail(`${family.id} ${kind} repeats runtime symbol`);
}

function validateConsumerSignal(family, signal) {
  if (!signal || signal.kind !== 'source_marker' || typeof signal.value !== 'string' || !signal.value) fail(`${family.id} has invalid consumer signal`);
}

function validateProductionSurfaceCoverage(registry, graph, rootOwners, repoRoot) {
  const coverage = registry.production_surface_contract;
  if (!coverage || typeof coverage.path !== 'string' || !Array.isArray(coverage.required_component_family_ids) || !Array.isArray(coverage.required_archetype_owners)) {
    fail('Astro family registry misses production_surface_contract coverage');
  }
  const contract = JSON.parse(requireText(path.resolve(repoRoot, coverage.path)));
  const requiredComponents = contract.component_families.filter((family) => family.required);
  if (JSON.stringify(sorted(coverage.required_component_family_ids)) !== JSON.stringify(sorted(requiredComponents.map((family) => family.id)))) {
    fail('Production component-family coverage drifts from canonical contract');
  }
  for (const component of requiredComponents) {
    for (const source of component.source_files.filter((file) => file.endsWith('.astro'))) {
      if (!rootOwners.has(source)) fail(`Required production component source is unregistered: ${component.id}:${source}`);
    }
  }
  const requiredArchetypes = contract.archetypes.filter((archetype) => archetype.required);
  const owners = new Map(coverage.required_archetype_owners.map((owner) => [owner.id, owner]));
  if (owners.size !== coverage.required_archetype_owners.length || JSON.stringify(sorted(owners.keys())) !== JSON.stringify(sorted(requiredArchetypes.map((archetype) => archetype.id)))) {
    fail('Production archetype ownership drifts from canonical contract');
  }
  for (const archetype of requiredArchetypes) {
    const owner = owners.get(archetype.id);
    if (!Array.isArray(owner.families) || !owner.families.length) fail(`Required archetype has no Astro owner: ${archetype.id}`);
    const routePatterns = new Set(owner.families.flatMap((id) => graph.families[id]?.production_route_patterns || []));
    for (const pattern of archetype.routes) if (!routePatterns.has(pattern)) fail(`Required archetype route has no registered owner: ${archetype.id}:${pattern}`);
  }
}

function requireText(file) {
  // This intentionally uses the same filesystem content as the graph build, not a module cache.
  return globalThis.__astroFamilySotText?.get(file) || '';
}

export async function checkAstroFamilySot({ repoRoot = DEFAULT_REPO_ROOT, registryPath = DEFAULT_REGISTRY_PATH, graphPath } = {}) {
  const absoluteRegistry = path.resolve(repoRoot, registryPath);
  const registryText = await readFile(absoluteRegistry, 'utf8');
  const registry = JSON.parse(registryText);
  if (registry.schema !== 'kenigevents.astro-family-registry.v1') fail(`Unexpected registry schema: ${registry.schema}`);
  if (!Array.isArray(registry.families) || !registry.families.length) fail('Astro family registry is empty');
  for (const key of FORBIDDEN_TOP_LEVEL_KEYS) if (Object.hasOwn(registry, key)) fail(`Operational field is forbidden in Astro family registry: ${key}`);

  const sourceFiles = await walkSource(path.resolve(repoRoot, registry.source_root), repoRoot);
  const sourceByPath = new Map();
  for (const file of sourceFiles) sourceByPath.set(file, await readFile(path.resolve(repoRoot, file), 'utf8'));
  globalThis.__astroFamilySotText = new Map([[path.resolve(repoRoot, registry.production_surface_contract.path), await readFile(path.resolve(repoRoot, registry.production_surface_contract.path), 'utf8')]]);

  const familyById = new Map();
  const rootOwners = new Map();
  const styleOwners = new Map();
  const runtimeAllowed = new Map();
  for (const family of registry.families) {
    for (const field of REQUIRED_FIELDS) if (!Object.hasOwn(family, field)) fail(`${family.id || '<unknown>'} misses required field: ${field}`);
    if (!/^[A-Z][A-Za-z0-9]+$/u.test(family.id) || familyById.has(family.id)) fail(`Duplicate or invalid family id: ${family.id}`);
    if (!Number.isInteger(family.version) || family.version < 1) fail(`${family.id} has invalid version`);
    for (const field of ['style_owners', 'variants', 'states', 'nested_families', 'runtime_factories', 'runtime_clients', 'consumer_signals', 'explicit_override_exceptions']) {
      if (!Array.isArray(family[field])) fail(`${family.id}.${field} must be an array`);
    }
    familyById.set(family.id, family);
    const rootSet = rootOwners.get(family.astro_root) || new Set(); rootSet.add(family.id); rootOwners.set(family.astro_root, rootSet);
    for (const owner of family.style_owners) {
      if (styleOwners.has(owner)) fail(`Duplicate style owner ${owner}: ${styleOwners.get(owner)} and ${family.id}`);
      styleOwners.set(owner, family.id);
    }
    const requiredPaths = [family.astro_root, ...family.style_owners, ...family.explicit_override_exceptions.filter((item) => item.path).map((item) => item.path)];
    for (const entry of [...family.runtime_factories, ...family.runtime_clients]) { validateRuntimeEntry(family, entry, 'runtime'); requiredPaths.push(entry.path); }
    for (const signal of family.consumer_signals) validateConsumerSignal(family, signal);
    for (const file of requiredPaths) if (!await exists(path.resolve(repoRoot, file))) fail(`${family.id} references missing source: ${file}`);
    const rootSource = await readFile(path.resolve(repoRoot, family.astro_root), 'utf8');
    validateIdentity(family, rootSource);
    for (const nested of family.nested_families) if (!familyById.has(nested) && !registry.families.some((item) => item.id === nested)) fail(`${family.id} references unregistered nested family: ${nested}`);
    for (const entry of [...family.runtime_factories, ...family.runtime_clients]) {
      const source = await readFile(path.resolve(repoRoot, entry.path), 'utf8');
      for (const symbol of entry.symbols) {
        if (!source.includes(symbol)) fail(`${family.id} runtime symbol ${symbol} is missing from ${entry.path}`);
        const allowed = runtimeAllowed.get(symbol) || new Set(); allowed.add(entry.path); runtimeAllowed.set(symbol, allowed);
      }
    }
    for (const declaration of family.explicit_override_exceptions.filter((item) => item.kind === 'runtime_declaration')) {
      if (!Array.isArray(declaration.symbols) || !declaration.symbols.length) fail(`${family.id} has invalid runtime declaration exception`);
      const source = await readFile(path.resolve(repoRoot, declaration.path), 'utf8');
      for (const symbol of declaration.symbols) {
        if (!source.includes(symbol)) fail(`${family.id} runtime declaration ${symbol} is missing from ${declaration.path}`);
        const allowed = runtimeAllowed.get(symbol) || new Set(); allowed.add(declaration.path); runtimeAllowed.set(symbol, allowed);
      }
    }
  }
  const registeredIds = new Set(familyById.keys());
  for (const attribute of ['family', 'component']) {
    const discovered = new Set(sourceFiles.flatMap((file) => literalIdentityValues(sourceByPath.get(file), attribute)));
    for (const id of discovered) if (!registeredIds.has(id)) fail(`Unregistered canonical owners: ${id}`);
  }
  for (const [symbol, allowed] of runtimeAllowed) {
    const unknown = sourceFiles.filter((file) => sourceByPath.get(file).includes(symbol) && !allowed.has(file));
    if (unknown.length) fail(`Unregistered runtime consumer for ${symbol}: ${unknown.join(', ')}`);
  }

  const expectedGraph = await buildAstroFamilyGraph({ repoRoot, registryPath });
  validateProductionSurfaceCoverage(registry, expectedGraph, rootOwners, repoRoot);
  const resolvedGraphPath = graphPath || registry.generated_graph;
  if (!await exists(path.resolve(repoRoot, resolvedGraphPath))) fail(`Generated graph is missing: ${resolvedGraphPath}`);
  if (await readFile(path.resolve(repoRoot, resolvedGraphPath), 'utf8') !== graphText(expectedGraph)) fail('Astro family graph drift: run node site/scripts/generate-astro-family-consumer-graph.mjs --write');
  return { family_count: registry.families.length, root_count: rootOwners.size, style_owner_count: styleOwners.size, runtime_symbol_count: runtimeAllowed.size, graph_summary: expectedGraph.summary };
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

async function main() { process.stdout.write(`${JSON.stringify({ ok: true, ...await checkAstroFamilySot(parseArgs(process.argv.slice(2))) }, null, 2)}\n`); }
if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) main().catch((error) => { console.error(`[astro-family-sot] ${error.message}`); process.exitCode = 1; });
