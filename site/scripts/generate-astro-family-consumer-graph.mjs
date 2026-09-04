#!/usr/bin/env node
import { createHash } from 'node:crypto';
import { access, mkdir, readFile, readdir, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const SOURCE_EXTENSIONS = new Set(['.astro', '.css', '.js', '.jsx', '.mjs', '.cjs', '.ts', '.tsx']);
const RESOLVE_EXTENSIONS = ['', '.astro', '.css', '.js', '.jsx', '.mjs', '.cjs', '.ts', '.tsx'];
const scriptPath = fileURLToPath(import.meta.url);
export const DEFAULT_REPO_ROOT = path.resolve(path.dirname(scriptPath), '../..');
export const DEFAULT_REGISTRY_PATH = 'site/src/design-system/astro-family-registry.v1.json';

const posix = (value) => value.split(path.sep).join('/');
const sortedUnique = (values) => [...new Set(values)].sort((left, right) => left.localeCompare(right));
const digest = (value) => createHash('sha256').update(value).digest('hex');

async function exists(filePath) {
  try {
    await access(filePath);
    return true;
  } catch {
    return false;
  }
}

async function walkFiles(directory) {
  const result = [];
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    const absolute = path.join(directory, entry.name);
    if (entry.isDirectory()) result.push(...await walkFiles(absolute));
    else if (entry.isFile() && SOURCE_EXTENSIONS.has(path.extname(entry.name))) result.push(absolute);
  }
  return result.sort((left, right) => left.localeCompare(right));
}

function importSpecifiers(source) {
  const values = [];
  const patterns = [
    /(?:^|\n)\s*(?:import|export)\s+(?:[\s\S]*?\sfrom\s*)?["']([^"']+)["']/gu,
    /import\(\s*["']([^"']+)["']\s*\)/gu,
    /@import\s+(?:url\(\s*)?["']([^"']+)["']/gu,
  ];
  for (const pattern of patterns) {
    for (const match of source.matchAll(pattern)) values.push(match[1]);
  }
  return sortedUnique(values);
}

async function resolveImport(repoRoot, sourceRoot, importer, specifier) {
  if (!specifier.startsWith('.')) return null;
  const base = path.resolve(repoRoot, path.dirname(importer), specifier);
  const candidates = [];
  for (const extension of RESOLVE_EXTENSIONS) candidates.push(`${base}${extension}`);
  for (const extension of RESOLVE_EXTENSIONS.slice(1)) candidates.push(path.join(base, `index${extension}`));
  for (const candidate of candidates) {
    if (!await exists(candidate)) continue;
    const relative = posix(path.relative(repoRoot, candidate));
    if (relative === sourceRoot || relative.startsWith(`${sourceRoot}/`)) return relative;
  }
  return null;
}

function excludedRoute(file, excludes) {
  return excludes.some((pattern) => {
    if (pattern.endsWith('/**')) return file.startsWith(pattern.slice(0, -3));
    return file === pattern;
  });
}

export function routePatternForFile(file, pagesRoot = 'site/src/pages') {
  let relative = file.slice(`${pagesRoot}/`.length).replace(/\.astro$/u, '');
  if (relative === 'index') return '/';
  if (relative.endsWith('/index')) relative = relative.slice(0, -'/index'.length);
  relative = relative.split('/').map(routePatternSegment).join('/');
  return `/${relative}/`.replace(/\/+/gu, '/');
}

function transitiveConsumers(seeds, reverse) {
  const visited = new Set(seeds);
  const queue = [...seeds];
  while (queue.length) {
    const current = queue.shift();
    for (const consumer of reverse.get(current) || []) {
      if (visited.has(consumer)) continue;
      visited.add(consumer);
      queue.push(consumer);
    }
  }
  return visited;
}

function normalizeRuntimeEntries(entries = []) {
  return [...entries]
    .map((entry) => ({ path: entry.path, symbols: sortedUnique(entry.symbols || []) }))
    .sort((left, right) => left.path.localeCompare(right.path));
}

function normalizeConsumerSignals(entries = []) {
  return [...entries]
    .map((entry) => ({ kind: entry.kind, value: entry.value }))
    .sort((left, right) => left.kind.localeCompare(right.kind) || left.value.localeCompare(right.value));
}

function protocolConsumers(signals, files, contents) {
  const consumers = [];
  for (const signal of signals) {
    if (signal.kind !== 'source_marker') continue;
    for (const file of files) {
      if (contents.get(file)?.includes(signal.value)) consumers.push(file);
    }
  }
  return sortedUnique(consumers);
}

function routePatternSegment(segment) {
  return segment.replace(/\[\.\.\.[^\]]+\]/gu, '**').replace(/\[[^\]]+\]/gu, '*');
}

export async function buildAstroFamilyGraph({
  repoRoot = DEFAULT_REPO_ROOT,
  registryPath = DEFAULT_REGISTRY_PATH,
} = {}) {
  const absoluteRegistry = path.resolve(repoRoot, registryPath);
  const registryText = await readFile(absoluteRegistry, 'utf8');
  const registry = JSON.parse(registryText);
  const sourceRoot = registry.source_root;
  const absoluteSourceRoot = path.resolve(repoRoot, sourceRoot);
  const absoluteFiles = await walkFiles(absoluteSourceRoot);
  const files = absoluteFiles.map((file) => posix(path.relative(repoRoot, file)));
  const contents = new Map();
  const forward = new Map();
  const reverse = new Map();

  for (let index = 0; index < files.length; index += 1) {
    const file = files[index];
    const source = await readFile(absoluteFiles[index], 'utf8');
    contents.set(file, source);
    const resolved = [];
    for (const specifier of importSpecifiers(source)) {
      const target = await resolveImport(repoRoot, sourceRoot, file, specifier);
      if (target) resolved.push(target);
    }
    const imports = sortedUnique(resolved);
    forward.set(file, imports);
    for (const target of imports) {
      const consumers = reverse.get(target) || [];
      consumers.push(file);
      reverse.set(target, consumers);
    }
  }
  for (const [target, consumers] of reverse) reverse.set(target, sortedUnique(consumers));

  const pagesRoot = `${sourceRoot}/pages`;
  const productionPages = files.filter((file) => (
    file.startsWith(`${pagesRoot}/`)
    && file.endsWith('.astro')
    && !excludedRoute(file, registry.production_route_excludes || [])
  ));

  const families = {};
  for (const family of registry.families) {
    const runtimeFactories = normalizeRuntimeEntries(family.runtime_factories);
    const runtimeClients = normalizeRuntimeEntries(family.runtime_clients);
    const consumerSignals = normalizeConsumerSignals(family.consumer_signals);
    const componentConsumers = sortedUnique(reverse.get(family.astro_root) || []);
    const styleConsumers = sortedUnique((family.style_owners || []).flatMap((owner) => reverse.get(owner) || []));
    const signalConsumers = protocolConsumers(consumerSignals, files, contents);
    const directConsumers = sortedUnique([...componentConsumers, ...styleConsumers, ...signalConsumers]);
    const seeds = sortedUnique([
      family.astro_root,
      ...(family.style_owners || []),
      ...runtimeFactories.map((entry) => entry.path),
      ...runtimeClients.map((entry) => entry.path),
      ...signalConsumers,
    ]);
    const affected = transitiveConsumers(seeds, reverse);
    const routeSources = productionPages.filter((file) => affected.has(file));
    const productionRoutes = routeSources.map((source) => ({
      pattern: routePatternForFile(source, pagesRoot),
      source,
    })).sort((left, right) => left.pattern.localeCompare(right.pattern) || left.source.localeCompare(right.source));
    families[family.id] = {
      version: family.version,
      astro_root: family.astro_root,
      style_owners: sortedUnique(family.style_owners || []),
      direct_component_consumers: componentConsumers,
      direct_style_consumers: styleConsumers,
      protocol_consumers: signalConsumers,
      direct_consumers: directConsumers,
      runtime_factories: runtimeFactories,
      runtime_clients: runtimeClients,
      production_routes: productionRoutes,
      production_route_patterns: sortedUnique(productionRoutes.map((route) => route.pattern)),
      production_route_sources: sortedUnique(productionRoutes.map((route) => route.source)),
    };
  }

  const familyValues = Object.values(families);
  const directConsumers = familyValues.flatMap((family) => family.direct_consumers);
  const protocolConsumersCount = familyValues.flatMap((family) => family.protocol_consumers);
  const runtimeFactories = familyValues.flatMap((family) => family.runtime_factories.map((entry) => entry.path));
  const runtimeClients = familyValues.flatMap((family) => family.runtime_clients.map((entry) => entry.path));
  const routes = familyValues.flatMap((family) => family.production_route_patterns);
  const importInventory = files.map((file) => [file, forward.get(file) || []]);

  return {
    schema: 'kenigevents.astro-family-consumers.v1',
    version: '1.2.0',
    registry: registryPath,
    registry_version: registry.version,
    registry_sha256: digest(registryText),
    source_root: sourceRoot,
    import_inventory_sha256: digest(JSON.stringify(importInventory)),
    summary: {
      family_count: registry.families.length,
      source_file_count: files.length,
      import_edge_count: [...forward.values()].reduce((sum, imports) => sum + imports.length, 0),
      direct_consumer_edges: directConsumers.length,
      protocol_consumer_edges: protocolConsumersCount.length,
      runtime_factory_edges: runtimeFactories.length,
      runtime_consumer_edges: runtimeClients.length,
      production_route_edges: routes.length,
      unique_direct_consumers: new Set(directConsumers).size,
      unique_protocol_consumers: new Set(protocolConsumersCount).size,
      unique_runtime_factories: new Set(runtimeFactories).size,
      unique_runtime_consumers: new Set(runtimeClients).size,
      unique_production_routes: new Set(routes).size,
    },
    families,
  };
}

export function graphText(graph) {
  return `${JSON.stringify(graph, null, 2)}\n`;
}

export async function writeAstroFamilyGraph(options = {}) {
  const repoRoot = options.repoRoot || DEFAULT_REPO_ROOT;
  const registryPath = options.registryPath || DEFAULT_REGISTRY_PATH;
  const registry = JSON.parse(await readFile(path.resolve(repoRoot, registryPath), 'utf8'));
  const graphPath = options.graphPath || registry.generated_graph;
  const graph = await buildAstroFamilyGraph({ repoRoot, registryPath });
  const absoluteGraph = path.resolve(repoRoot, graphPath);
  await mkdir(path.dirname(absoluteGraph), { recursive: true });
  await writeFile(absoluteGraph, graphText(graph), 'utf8');
  return { graph, graphPath };
}

export function impactFor(graph, familyId) {
  const family = graph.families[familyId];
  if (!family) throw new Error(`Unknown Astro family: ${familyId}`);
  return {
    family: familyId,
    version: family.version,
    astro_root: family.astro_root,
    style_owners: family.style_owners,
    direct_consumers: family.direct_consumers,
    protocol_consumers: family.protocol_consumers,
    runtime_factories: family.runtime_factories,
    runtime_consumers: family.runtime_clients,
    production_routes: family.production_routes,
    production_route_patterns: family.production_route_patterns,
  };
}

function parseArgs(argv) {
  const options = {};
  for (let index = 0; index < argv.length; index += 1) {
    const value = argv[index];
    if (value === '--write') options.write = true;
    else if (value === '--impact') options.impact = argv[++index];
    else if (value === '--root') options.repoRoot = path.resolve(argv[++index]);
    else if (value === '--registry') options.registryPath = argv[++index];
    else throw new Error(`Unknown argument: ${value}`);
  }
  return options;
}

async function main() {
  const options = parseArgs(process.argv.slice(2));
  const result = options.write
    ? await writeAstroFamilyGraph(options)
    : { graph: await buildAstroFamilyGraph(options) };
  const output = options.impact ? impactFor(result.graph, options.impact) : result.graph.summary;
  process.stdout.write(`${JSON.stringify(output, null, 2)}\n`);
}

if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) {
  main().catch((error) => {
    console.error(`[astro-family-graph] ${error.message}`);
    process.exitCode = 1;
  });
}
