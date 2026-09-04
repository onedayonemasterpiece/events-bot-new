#!/usr/bin/env node
import { createHash } from 'node:crypto';
import { access, mkdir, readFile, readdir, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { buildAstroFamilyGraph, routePatternForFile } from './generate-astro-family-consumer-graph.mjs';

const SOURCE_EXTENSIONS = new Set(['.astro', '.css', '.js', '.jsx', '.mjs', '.cjs', '.ts', '.tsx']);
const STYLE_EXTENSIONS = new Set(['.astro', '.css']);
const RESOLVE_EXTENSIONS = ['', '.astro', '.css', '.js', '.jsx', '.mjs', '.cjs', '.ts', '.tsx'];
// A trailing hyphen is legal CSS, but this source tree uses it only for
// interpolated template prefixes (for example `--ke-space-${role}`). Treating
// those prefixes as a token would manufacture a false undefined consumer.
const TOKEN = '--[A-Za-z_](?:[A-Za-z0-9_-]*[A-Za-z0-9_])?';
const scriptPath = fileURLToPath(import.meta.url);
export const DEFAULT_REPO_ROOT = path.resolve(path.dirname(scriptPath), '../..');
export const DEFAULT_REGISTRY_PATH = 'site/src/design-system/token-authority-registry.v1.json';

const posix = (value) => value.split(path.sep).join('/');
const sortedUnique = (values) => [...new Set(values)].sort((left, right) => String(left).localeCompare(String(right)));
const digest = (value) => createHash('sha256').update(value).digest('hex');
const lineNumberAt = (source, offset) => source.slice(0, offset).split('\n').length;

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
  for (const pattern of [
    /(?:^|\n)\s*(?:import|export)\s+(?:[\s\S]*?\sfrom\s*)?["']([^"']+)["']/gu,
    /import\(\s*["']([^"']+)["']\s*\)/gu,
    /@import\s+(?:url\(\s*)?["']([^"']+)["']/gu,
  ]) {
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

function scopesAt(source, offsets) {
  const result = new Map();
  const stack = [];
  let boundary = 0;
  let offsetIndex = 0;
  for (const brace of source.matchAll(/[{}]/gu)) {
    while (offsetIndex < offsets.length && offsets[offsetIndex] < brace.index) {
      result.set(offsets[offsetIndex], stack.join(' > ') || '<top-level>');
      offsetIndex += 1;
    }
    if (brace[0] === '{') {
      const selector = source.slice(boundary, brace.index)
        .replace(/\/\*[\s\S]*?\*\//gu, '')
        .split(';').at(-1).trim().replace(/\s+/gu, ' ');
      stack.push(selector || '<anonymous>');
    } else {
      stack.pop();
    }
    boundary = brace.index + 1;
  }
  while (offsetIndex < offsets.length) {
    result.set(offsets[offsetIndex], stack.join(' > ') || '<top-level>');
    offsetIndex += 1;
  }
  return result;
}

function varReferences(value) {
  const references = [];
  const pattern = new RegExp(`var\\(\\s*(${TOKEN})(?![A-Za-z0-9_-])`, 'gu');
  for (const match of value.matchAll(pattern)) {
    const remainder = value.slice(match.index + match[0].length);
    const closing = remainder.indexOf(')');
    const inside = closing === -1 ? remainder : remainder.slice(0, closing);
    references.push({ token: match[1], has_fallback: inside.includes(',') });
  }
  return references;
}

function extractTokenCensus(source, sourcePath) {
  // Do not require a literal-only CSS value: Astro style attributes commonly
  // interpolate a value through `{...}`. The custom-property declaration is
  // still authoritative even when its value is runtime-computed.
  const declarationPattern = new RegExp(`(${TOKEN})(?![A-Za-z0-9_-])\\s*:\\s*([^;\\n\\x60}]*)`, 'gu');
  const declarationMatches = [...source.matchAll(declarationPattern)];
  const scopes = scopesAt(source, declarationMatches.map((match) => match.index));
  const definitions = [];
  const declarationRanges = [];
  for (const match of declarationMatches) {
    const start = match.index;
    const valueStart = start + match[0].indexOf(match[2]);
    const valueEnd = valueStart + match[2].length;
    const aliases = sortedUnique(varReferences(match[2]).map((reference) => reference.token));
    definitions.push({
      path: sourcePath,
      line: lineNumberAt(source, start),
      scope: scopes.get(start),
      kind: 'css',
      value: match[2].trim(),
      aliases,
      token: match[1],
    });
    declarationRanges.push([valueStart, valueEnd]);
  }

  definitions.push(...extractRuntimeDefinitions(source, sourcePath));

  const consumers = [];
  const consumerPattern = new RegExp(`var\\(\\s*(${TOKEN})(?![A-Za-z0-9_-])`, 'gu');
  for (const match of source.matchAll(consumerPattern)) {
    if (declarationRanges.some(([start, end]) => match.index >= start && match.index < end)) continue;
    const remainder = source.slice(match.index + match[0].length);
    const closing = remainder.indexOf(')');
    const inside = closing === -1 ? remainder : remainder.slice(0, closing);
    consumers.push({
      token: match[1],
      path: sourcePath,
      line: lineNumberAt(source, match.index),
      has_fallback: inside.includes(','),
    });
  }
  return { definitions, consumers };
}

function extractRuntimeDefinitions(source, sourcePath) {
  const definitions = [];
  const runtimePattern = new RegExp(`\\.style\\.setProperty\\(\\s*(['"])(${TOKEN})(?![A-Za-z0-9_-])\\1`, 'gu');
  for (const match of source.matchAll(runtimePattern)) {
    definitions.push({
      path: sourcePath,
      line: lineNumberAt(source, match.index),
      scope: '<runtime-style-property>',
      kind: 'runtime',
      value: '<runtime>',
      aliases: [],
      token: match[2],
    });
  }
  return definitions;
}

function reverseClosure(seeds, reverse) {
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

function tokenDependents(token, reverseAliases) {
  const result = reverseClosure([token], reverseAliases);
  result.delete(token);
  return sortedUnique(result);
}

function internSet(store, prefix, value) {
  const key = `${prefix}-${digest(JSON.stringify(value)).slice(0, 16)}`;
  if (!Object.hasOwn(store, key)) store[key] = value;
  return key;
}

function isProductionPage(file, sourceRoot, exclusions) {
  const pagesRoot = `${sourceRoot}/pages`;
  if (!file.startsWith(`${pagesRoot}/`) || !file.endsWith('.astro')) return false;
  return !exclusions.some((pattern) => (pattern.endsWith('/**')
    ? file.startsWith(pattern.slice(0, -3))
    : file === pattern));
}

export function graphText(graph) {
  return `${JSON.stringify(graph, null, 2)}\n`;
}

export async function buildTokenImpactGraph({
  repoRoot = DEFAULT_REPO_ROOT,
  registryPath = DEFAULT_REGISTRY_PATH,
} = {}) {
  const registryText = await readFile(path.resolve(repoRoot, registryPath), 'utf8');
  const registry = JSON.parse(registryText);
  const sourceRoot = registry.source_root;
  const astroGraph = await buildAstroFamilyGraph({ repoRoot });
  const serviceOnly = new Set(astroGraph.product_scope.service_only_sources);
  const absoluteFiles = (await walkFiles(path.resolve(repoRoot, sourceRoot)))
    .filter((file) => !serviceOnly.has(posix(path.relative(repoRoot, file))));
  const files = absoluteFiles.map((file) => posix(path.relative(repoRoot, file)));
  const forward = new Map();
  const reverse = new Map();
  const tokenDefinitions = new Map();
  const tokenConsumers = new Map();
  const aliasForward = new Map();
  const aliasReverse = new Map();

  for (let index = 0; index < files.length; index += 1) {
    const file = files[index];
    const source = await readFile(absoluteFiles[index], 'utf8');
    const imports = [];
    for (const specifier of importSpecifiers(source)) {
      const resolved = await resolveImport(repoRoot, sourceRoot, file, specifier);
      if (resolved) imports.push(resolved);
    }
    forward.set(file, sortedUnique(imports));
    for (const target of forward.get(file)) {
      const consumers = reverse.get(target) || [];
      consumers.push(file);
      reverse.set(target, consumers);
    }
    const census = STYLE_EXTENSIONS.has(path.extname(file))
      ? extractTokenCensus(source, file)
      : { definitions: extractRuntimeDefinitions(source, file), consumers: [] };
    for (const definition of census.definitions) {
      const definitions = tokenDefinitions.get(definition.token) || [];
      definitions.push(definition);
      tokenDefinitions.set(definition.token, definitions);
      for (const target of definition.aliases) {
        const targets = aliasForward.get(definition.token) || new Set();
        targets.add(target);
        aliasForward.set(definition.token, targets);
        const aliases = aliasReverse.get(target) || new Set();
        aliases.add(definition.token);
        aliasReverse.set(target, aliases);
      }
    }
    for (const consumer of census.consumers) {
      const consumers = tokenConsumers.get(consumer.token) || [];
      consumers.push(consumer);
      tokenConsumers.set(consumer.token, consumers);
    }
  }
  for (const [target, consumers] of reverse) reverse.set(target, sortedUnique(consumers));

  const astroGraphPath = registry.astro_family_graph;
  const astroGraphText = await readFile(path.resolve(repoRoot, astroGraphPath), 'utf8');
  const astroFamilyRegistry = JSON.parse(await readFile(path.resolve(repoRoot, astroGraph.registry), 'utf8'));
  const familyEntries = Object.entries(astroGraph.families);
  const tokens = {};
  const componentSets = {};
  const familySets = {};
  const routeSets = {};
  const allTokens = sortedUnique([...tokenDefinitions.keys(), ...tokenConsumers.keys()]);
  for (const token of allTokens) {
    const dependentTokens = tokenDependents(token, aliasReverse);
    const impactedTokens = [token, ...dependentTokens];
    const definitions = impactedTokens.flatMap((name) => tokenDefinitions.get(name) || []);
    const directConsumers = impactedTokens.flatMap((name) => tokenConsumers.get(name) || []);
    const seeds = sortedUnique([...definitions.map((entry) => entry.path), ...directConsumers.map((entry) => entry.path)]);
    const affected = reverseClosure(seeds, reverse);
    const affectedFamilies = familyEntries
      .filter(([, family]) => affected.has(family.astro_root)
        || family.runtime_factories.some((entry) => affected.has(entry.path))
        || family.runtime_clients.some((entry) => affected.has(entry.path)))
      .map(([id, family]) => ({
        id,
        astro_root: family.astro_root,
        production_route_patterns: family.production_route_patterns,
      }));
    const componentPaths = sortedUnique([
      ...directConsumers.map((entry) => entry.path).filter((file) => file.includes('/components/') && file.endsWith('.astro')),
      ...affectedFamilies.map((family) => family.astro_root).filter((file) => file.endsWith('.astro')),
    ]);
    const routes = sortedUnique([
      ...files.filter((file) => affected.has(file) && isProductionPage(file, sourceRoot, astroFamilyRegistry.production_route_excludes || []))
        .map((file) => routePatternForFile(file, `${sourceRoot}/pages`)),
      ...affectedFamilies.flatMap((family) => family.production_route_patterns),
    ]);
    tokens[token] = {
      definitions: definitions
        .sort((left, right) => `${left.path}:${left.line}:${left.scope}`.localeCompare(`${right.path}:${right.line}:${right.scope}`)),
      aliases_to: sortedUnique(aliasForward.get(token) || []),
      aliases_from: dependentTokens,
      direct_consumers: directConsumers
        .sort((left, right) => `${left.path}:${left.line}`.localeCompare(`${right.path}:${right.line}`)),
      component_set: internSet(componentSets, 'components', componentPaths),
      astro_family_set: internSet(familySets, 'families', affectedFamilies.sort((left, right) => left.id.localeCompare(right.id))),
      production_route_set: internSet(routeSets, 'routes', routes),
    };
  }
  const definitionCount = [...tokenDefinitions.values()].reduce((total, entries) => total + entries.length, 0);
  const consumerCount = [...tokenConsumers.values()].reduce((total, entries) => total + entries.length, 0);
  const aliasCount = [...aliasForward.values()].reduce((total, targets) => total + targets.size, 0);
  return {
    schema: 'kenigevents.token-impact.v1',
    version: '1.0.0',
    registry: registryPath,
    registry_version: registry.version,
    registry_sha256: digest(registryText),
    source_root: sourceRoot,
    source_inventory_sha256: digest(JSON.stringify([...forward.entries()])),
    astro_family_graph: astroGraphPath,
    astro_family_graph_sha256: digest(astroGraphText),
    summary: {
      token_count: allTokens.length,
      definition_count: definitionCount,
      consumer_count: consumerCount,
      alias_edge_count: aliasCount,
      family_count: familyEntries.length,
    },
    component_sets: componentSets,
    astro_family_sets: familySets,
    production_route_sets: routeSets,
    tokens,
  };
}

export async function writeTokenImpactGraph(options = {}) {
  const repoRoot = options.repoRoot || DEFAULT_REPO_ROOT;
  const registryPath = options.registryPath || DEFAULT_REGISTRY_PATH;
  const registry = JSON.parse(await readFile(path.resolve(repoRoot, registryPath), 'utf8'));
  const graph = await buildTokenImpactGraph({ repoRoot, registryPath });
  const graphPath = options.graphPath || registry.generated_graph;
  await mkdir(path.dirname(path.resolve(repoRoot, graphPath)), { recursive: true });
  await writeFile(path.resolve(repoRoot, graphPath), graphText(graph), 'utf8');
  return { graph, graphPath };
}

export function impactFor(graph, token) {
  const impact = graph.tokens[token];
  if (!impact) throw new Error(`Unknown design token: ${token}`);
  return {
    token,
    ...impact,
    component_paths: graph.component_sets[impact.component_set] || [],
    astro_families: graph.astro_family_sets[impact.astro_family_set] || [],
    production_route_patterns: graph.production_route_sets[impact.production_route_set] || [],
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
    else if (value === '--graph') options.graphPath = argv[++index];
    else throw new Error(`Unknown argument: ${value}`);
  }
  return options;
}

async function main() {
  const options = parseArgs(process.argv.slice(2));
  const result = options.write ? await writeTokenImpactGraph(options) : { graph: await buildTokenImpactGraph(options) };
  const output = options.impact ? impactFor(result.graph, options.impact) : result.graph.summary;
  process.stdout.write(`${JSON.stringify(output, null, 2)}\n`);
}

if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) {
  main().catch((error) => {
    console.error(`[token-impact-graph] ${error.message}`);
    process.exitCode = 1;
  });
}
