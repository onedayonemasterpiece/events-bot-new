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
  'id',
  'version',
  'astro_root',
  'style_owners',
  'variants',
  'states',
  'nested_families',
  'runtime_factories',
  'runtime_clients',
  'explicit_override_exceptions',
  'penpot_binding',
];
const SOURCE_EXTENSIONS = new Set(['.astro', '.js', '.jsx', '.mjs', '.cjs', '.ts', '.tsx']);
const FORBIDDEN_TOP_LEVEL_KEYS = new Set([
  'role_receipts',
  'comment_ids',
  'branch_checkpoints',
  'kaggle_runs',
  'v0_matrices',
  'historical_research_catalogs',
  'penpot_mutation_receipts',
]);

const scriptPath = fileURLToPath(import.meta.url);
const posix = (value) => value.split(path.sep).join('/');
const sorted = (values) => [...values].sort((left, right) => String(left).localeCompare(String(right)));

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

async function walkSource(directory, repoRoot) {
  const result = [];
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    const absolute = path.join(directory, entry.name);
    if (entry.isDirectory()) result.push(...await walkSource(absolute, repoRoot));
    else if (entry.isFile() && SOURCE_EXTENSIONS.has(path.extname(entry.name))) {
      result.push(posix(path.relative(repoRoot, absolute)));
    }
  }
  return result.sort((left, right) => left.localeCompare(right));
}

function duplicates(values) {
  const seen = new Set();
  const repeated = new Set();
  for (const value of values) {
    if (seen.has(value)) repeated.add(value);
    seen.add(value);
  }
  return [...repeated];
}

function sameValues(left, right) {
  return JSON.stringify(sorted(left)) === JSON.stringify(sorted(right));
}

function exception(family, kind, attribute) {
  return family.explicit_override_exceptions.find((item) => (
    item.kind === kind && (attribute === undefined || item.attribute === attribute)
  ));
}

function readIdentityAttribute(source, attribute) {
  const pattern = new RegExp(`data-ds-${attribute}\\s*=\\s*(?:"([^"]*)"|'([^']*)'|\\{([^}\\n]+)\\})`, 'u');
  const match = source.match(pattern);
  if (!match) return null;
  if (match[1] !== undefined || match[2] !== undefined) {
    return { kind: 'literal', value: match[1] ?? match[2] };
  }
  return { kind: 'dynamic', value: match[3].trim() };
}

function validateDynamicIdentity(family, attribute, source, allowedValues) {
  const contract = exception(family, 'dynamic_identity', attribute);
  if (!contract) fail(`${family.id} has dynamic data-ds-${attribute} without a fail-closed registry contract`);
  if (!Array.isArray(contract.values) || !sameValues(contract.values, allowedValues)) {
    fail(`${family.id} dynamic ${attribute} values drift from the registry`);
  }
  let pattern;
  try {
    pattern = new RegExp(contract.source_regex, 'u');
  } catch (error) {
    fail(`${family.id} has invalid ${attribute} source_regex: ${error.message}`);
  }
  if (!pattern.test(source)) fail(`${family.id} dynamic ${attribute} source no longer matches its registry contract`);
}

function validateIdentity(family, source) {
  const identityAbsent = exception(family, 'identity_attribute_absent');
  const nonAstroRoot = exception(family, 'non_astro_root');
  if (!family.astro_root.endsWith('.astro')) {
    if (!nonAstroRoot) fail(`${family.id} uses a non-Astro root without an explicit exception`);
    return;
  }
  if (identityAbsent) {
    for (const attribute of ['variant', 'state']) {
      const contract = exception(family, 'dynamic_identity', attribute);
      if (contract) validateDynamicIdentity(family, attribute, source, family[`${attribute}s`]);
    }
    return;
  }

  const familyMarker = `data-ds-family="${family.id}"`;
  const versionMarker = `data-ds-version="${family.version}"`;
  if (!source.includes(familyMarker)) fail(`${family.id} root misses ${familyMarker}`);
  if (!source.includes(versionMarker)) fail(`${family.id} root misses ${versionMarker}`);

  for (const attribute of ['variant', 'state']) {
    const allowedValues = family[`${attribute}s`];
    const identity = readIdentityAttribute(source, attribute);
    if (!identity) {
      if (allowedValues.length) fail(`${family.id} declares ${attribute}s but its root has no data-ds-${attribute}`);
      continue;
    }
    if (identity.kind === 'dynamic') {
      validateDynamicIdentity(family, attribute, source, allowedValues);
      continue;
    }
    const observed = attribute === 'state'
      ? identity.value.split(/\s+/u).filter(Boolean)
      : [identity.value];
    for (const value of observed) {
      if (!allowedValues.includes(value)) fail(`${family.id} root publishes unknown ${attribute}: ${value}`);
    }
  }
}

function validateRuntimeEntry(family, entry, kind) {
  if (!entry || typeof entry.path !== 'string' || !Array.isArray(entry.symbols) || !entry.symbols.length) {
    fail(`${family.id} has an invalid ${kind} entry`);
  }
  if (duplicates(entry.symbols).length) fail(`${family.id} ${kind} repeats a runtime symbol`);
}

export async function checkAstroFamilySot({
  repoRoot = DEFAULT_REPO_ROOT,
  registryPath = DEFAULT_REGISTRY_PATH,
  graphPath,
} = {}) {
  const absoluteRegistry = path.resolve(repoRoot, registryPath);
  const registryText = await readFile(absoluteRegistry, 'utf8');
  const registry = JSON.parse(registryText);
  if (registry.schema !== 'kenigevents.astro-family-registry.v1') fail(`Unexpected registry schema: ${registry.schema}`);
  if (!Array.isArray(registry.families) || !registry.families.length) fail('Astro family registry is empty');
  for (const key of FORBIDDEN_TOP_LEVEL_KEYS) {
    if (Object.hasOwn(registry, key)) fail(`Operational field is forbidden in the Astro family registry: ${key}`);
  }

  const familyById = new Map();
  const rootOwner = new Map();
  const styleOwner = new Map();
  const sourceRoot = path.resolve(repoRoot, registry.source_root);
  const sourceFiles = await walkSource(sourceRoot, repoRoot);
  const sourceByPath = new Map();
  for (const file of sourceFiles) sourceByPath.set(file, await readFile(path.resolve(repoRoot, file), 'utf8'));

  for (const family of registry.families) {
    for (const field of REQUIRED_FIELDS) {
      if (!Object.hasOwn(family, field)) fail(`${family.id || '<unknown>'} misses required field: ${field}`);
    }
    if (!/^[A-Z][A-Za-z0-9]+$/u.test(family.id)) fail(`Invalid family id: ${family.id}`);
    if (!Number.isInteger(family.version) || family.version < 1) fail(`${family.id} has invalid version`);
    for (const field of ['style_owners', 'variants', 'states', 'nested_families', 'runtime_factories', 'runtime_clients', 'explicit_override_exceptions']) {
      if (!Array.isArray(family[field])) fail(`${family.id}.${field} must be an array`);
    }
    for (const field of ['style_owners', 'variants', 'states', 'nested_families']) {
      const repeated = duplicates(family[field]);
      if (repeated.length) fail(`${family.id}.${field} contains duplicates: ${repeated.join(', ')}`);
    }
    if (familyById.has(family.id)) fail(`Duplicate family id: ${family.id}`);
    familyById.set(family.id, family);

    if (rootOwner.has(family.astro_root)) {
      fail(`Duplicate canonical root ${family.astro_root}: ${rootOwner.get(family.astro_root)} and ${family.id}`);
    }
    rootOwner.set(family.astro_root, family.id);

    for (const owner of family.style_owners) {
      if (styleOwner.has(owner)) {
        fail(`Duplicate style owner ${owner}: ${styleOwner.get(owner)} and ${family.id}`);
      }
      styleOwner.set(owner, family.id);
    }
  }

  const runtimeAllowed = new Map();
  const runtimeDeclared = new Map();
  for (const family of registry.families) {
    const requiredPaths = [family.astro_root, ...family.style_owners];
    for (const entry of family.runtime_factories) {
      validateRuntimeEntry(family, entry, 'runtime factory');
      requiredPaths.push(entry.path);
    }
    for (const entry of family.runtime_clients) {
      validateRuntimeEntry(family, entry, 'runtime client');
      requiredPaths.push(entry.path);
    }
    for (const item of family.explicit_override_exceptions) {
      if (item.path) requiredPaths.push(item.path);
    }
    for (const file of requiredPaths) {
      if (!await exists(path.resolve(repoRoot, file))) fail(`${family.id} references missing source: ${file}`);
    }

    const rootSource = await readFile(path.resolve(repoRoot, family.astro_root), 'utf8');
    validateIdentity(family, rootSource);

    for (const nested of family.nested_families) {
      if (!familyById.has(nested)) fail(`${family.id} references unregistered nested family: ${nested}`);
    }

    for (const entry of [...family.runtime_factories, ...family.runtime_clients]) {
      const source = await readFile(path.resolve(repoRoot, entry.path), 'utf8');
      for (const symbol of entry.symbols) {
        if (!source.includes(symbol)) fail(`${family.id} runtime symbol ${symbol} is missing from ${entry.path}`);
        const allowed = runtimeAllowed.get(symbol) || new Set();
        allowed.add(entry.path);
        runtimeAllowed.set(symbol, allowed);
      }
    }
    for (const item of family.explicit_override_exceptions.filter((candidate) => candidate.kind === 'runtime_declaration')) {
      if (!Array.isArray(item.symbols) || !item.symbols.length) fail(`${family.id} has an invalid runtime declaration exception`);
      const source = await readFile(path.resolve(repoRoot, item.path), 'utf8');
      for (const symbol of item.symbols) {
        if (!source.includes(symbol)) fail(`${family.id} runtime declaration ${symbol} is missing from ${item.path}`);
        const allowed = runtimeAllowed.get(symbol) || new Set();
        allowed.add(item.path);
        runtimeAllowed.set(symbol, allowed);
        runtimeDeclared.set(symbol, item.path);
      }
    }
  }

  for (const family of registry.families) {
    if (exception(family, 'identity_attribute_absent') || !family.astro_root.endsWith('.astro')) continue;
    const hosts = sourceFiles.filter((file) => sourceByPath.get(file)?.includes(`data-ds-family="${family.id}"`));
    if (hosts.length !== 1 || hosts[0] !== family.astro_root) {
      fail(`${family.id} identity root drift: expected only ${family.astro_root}, observed ${hosts.join(', ') || 'none'}`);
    }
  }

  for (const [symbol, allowed] of runtimeAllowed) {
    const observed = sourceFiles.filter((file) => sourceByPath.get(file)?.includes(symbol));
    const unknown = observed.filter((file) => !allowed.has(file));
    if (unknown.length) fail(`Unregistered runtime consumer for ${symbol}: ${unknown.join(', ')}`);
  }

  const expectedGraph = await buildAstroFamilyGraph({ repoRoot, registryPath });
  const resolvedGraphPath = graphPath || registry.generated_graph;
  const absoluteGraph = path.resolve(repoRoot, resolvedGraphPath);
  if (!await exists(absoluteGraph)) fail(`Generated graph is missing: ${resolvedGraphPath}`);
  const committedGraph = await readFile(absoluteGraph, 'utf8');
  if (committedGraph !== graphText(expectedGraph)) {
    fail(`Astro family graph drift: run node site/scripts/generate-astro-family-consumer-graph.mjs --write`);
  }

  return {
    family_count: registry.families.length,
    root_count: rootOwner.size,
    style_owner_count: styleOwner.size,
    runtime_symbol_count: runtimeAllowed.size,
    runtime_declaration_count: runtimeDeclared.size,
    graph_summary: expectedGraph.summary,
  };
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
  const result = await checkAstroFamilySot(parseArgs(process.argv.slice(2)));
  process.stdout.write(`${JSON.stringify({ ok: true, ...result }, null, 2)}\n`);
}

if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) {
  main().catch((error) => {
    console.error(`[astro-family-sot] ${error.message}`);
    process.exitCode = 1;
  });
}
