#!/usr/bin/env node
import {
  existsSync,
  mkdirSync,
  readFileSync,
  readdirSync,
  statSync,
  writeFileSync,
} from 'node:fs';
import { dirname, extname, join, relative, resolve, sep } from 'node:path';
import { fileURLToPath } from 'node:url';

const siteRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const repoRoot = resolve(siteRoot, '..');
const defaultContract = join(siteRoot, 'src/data/design-system-production-surface-contract.v1.json');

function valueAfter(flag, fallback = null) {
  const index = process.argv.indexOf(flag);
  return index >= 0 && process.argv[index + 1] ? process.argv[index + 1] : fallback;
}

function hasFlag(flag) {
  return process.argv.includes(flag);
}

function invariant(condition, message) {
  if (!condition) throw new Error(message);
}

function normalizePath(path) {
  return path.split(sep).join('/');
}

function readJson(path) {
  return JSON.parse(readFileSync(path, 'utf8'));
}

function walkFiles(root, predicate = () => true) {
  if (!existsSync(root)) return [];
  const files = [];
  const walk = (dir) => {
    for (const entry of readdirSync(dir, { withFileTypes: true })) {
      const path = join(dir, entry.name);
      if (entry.isDirectory()) walk(path);
      else if (entry.isFile() && predicate(path)) files.push(path);
    }
  };
  walk(root);
  return files.sort((left, right) => left.localeCompare(right));
}

function routeFromDistFile(distRoot, file) {
  const rel = normalizePath(relative(distRoot, file));
  if (!rel.endsWith('.html')) return null;
  if (rel === 'index.html') return '/';
  if (rel.endsWith('/index.html')) return `/${rel.slice(0, -'index.html'.length)}`;
  return `/${rel.slice(0, -'.html'.length)}/`;
}

function routePatternFromPageFile(file) {
  let rel = normalizePath(relative(join(siteRoot, 'src/pages'), file));
  rel = rel.replace(/\.astro$/u, '');
  rel = rel.replace(/(?:^|\/)index$/u, '');
  const segments = rel.split('/').filter(Boolean).map((segment) => {
    if (/^\[\.\.\..+\]$/u.test(segment)) return '**';
    if (/^\[.+\]$/u.test(segment)) return '*';
    return segment;
  });
  return segments.length ? `/${segments.join('/')}/` : '/';
}

function escapeRegex(value) {
  return value.replace(/[.*+?^${}()|[\]\\]/gu, '\\$&');
}

function patternRegex(pattern) {
  const parts = String(pattern).split(/(\*\*)|(\*)/gu).filter((part) => part !== undefined && part !== '');
  const source = parts.map((part) => {
    if (part === '**') return '.*';
    if (part === '*') return '[^/]+';
    return escapeRegex(part);
  }).join('');
  return new RegExp(`^${source}$`, 'u');
}

function routeMatches(pattern, route) {
  return patternRegex(pattern).test(route);
}

function routeSpecificity(pattern) {
  const wildcards = (pattern.match(/\*/gu) || []).length;
  return (pattern.length * 10) - (wildcards * 1000);
}

function validateContract(contract) {
  invariant(contract?.schema_version === 'lovekgd-design-system-production-surface-contract-v1', 'contract_schema_invalid');
  invariant(contract.selection_policy?.authority === 'accepted-production-release', 'selection_authority_invalid');
  invariant(contract.interaction_budget?.plugin_open_per_update === 1, 'plugin_open_budget_invalid');
  invariant(contract.interaction_budget?.maximum_user_actions <= 3, 'plugin_action_budget_exceeded');
  invariant(contract.interaction_budget?.primary_action === 'update-everything', 'plugin_primary_action_invalid');

  const viewportIds = new Set();
  for (const viewport of contract.evidence_viewports || []) {
    invariant(viewport.id && !viewportIds.has(viewport.id), `duplicate_viewport:${viewport.id}`);
    invariant(Number.isInteger(viewport.width) && viewport.width > 0, `viewport_width_invalid:${viewport.id}`);
    invariant(Number.isInteger(viewport.height) && viewport.height > 0, `viewport_height_invalid:${viewport.id}`);
    viewportIds.add(viewport.id);
  }
  invariant(viewportIds.size >= 5, 'viewport_matrix_too_small');

  const excludedRoutes = contract.selection_policy.exclude_route_prefixes || [];
  const excludedSources = contract.selection_policy.exclude_source_prefixes || [];
  const familyIds = new Set();
  const missingSourceFiles = [];
  for (const family of contract.component_families || []) {
    invariant(family.id && !familyIds.has(family.id), `duplicate_family:${family.id}`);
    invariant(Array.isArray(family.source_files) && family.source_files.length > 0, `family_sources_empty:${family.id}`);
    familyIds.add(family.id);
    for (const source of family.source_files) {
      invariant(!excludedSources.some((prefix) => source.startsWith(prefix)), `excluded_source_in_contract:${family.id}:${source}`);
      if (!existsSync(resolve(repoRoot, source))) missingSourceFiles.push({ family_id: family.id, source });
    }
  }

  const archetypeIds = new Set();
  for (const archetype of contract.archetypes || []) {
    invariant(archetype.id && !archetypeIds.has(archetype.id), `duplicate_archetype:${archetype.id}`);
    invariant(Array.isArray(archetype.routes) && archetype.routes.length > 0, `archetype_routes_empty:${archetype.id}`);
    invariant(Array.isArray(archetype.evidence_viewports) && archetype.evidence_viewports.length >= 2, `archetype_viewports_incomplete:${archetype.id}`);
    for (const route of archetype.routes) {
      invariant(route.startsWith('/') && route.endsWith('/'), `route_pattern_invalid:${archetype.id}:${route}`);
      invariant(!excludedRoutes.some((prefix) => route.startsWith(prefix)), `excluded_route_in_contract:${archetype.id}:${route}`);
    }
    for (const viewportId of archetype.evidence_viewports) {
      invariant(viewportIds.has(viewportId), `unknown_viewport:${archetype.id}:${viewportId}`);
    }
    archetypeIds.add(archetype.id);
  }

  invariant((contract.evidence_contract?.penpot_pages || []).length >= 4, 'evidence_pages_incomplete');
  invariant((contract.evidence_contract?.artifact_kinds || []).includes('actual'), 'actual_evidence_missing');
  invariant((contract.evidence_contract?.artifact_kinds || []).includes('approved-baseline'), 'baseline_evidence_missing');
  invariant((contract.evidence_contract?.artifact_kinds || []).includes('diff'), 'diff_evidence_missing');

  return { viewportIds, missingSourceFiles };
}

function importSpecifiers(source) {
  const values = [];
  const patterns = [
    /\bfrom\s+['"]([^'"]+)['"]/gu,
    /\bimport\s+['"]([^'"]+)['"]/gu,
  ];
  for (const pattern of patterns) {
    for (const match of source.matchAll(pattern)) values.push(match[1]);
  }
  return values;
}

function resolveImport(fromFile, specifier) {
  if (!specifier.startsWith('.')) return null;
  const raw = resolve(dirname(fromFile), specifier);
  const candidates = [
    raw,
    `${raw}.astro`,
    `${raw}.ts`,
    `${raw}.js`,
    `${raw}.mjs`,
    join(raw, 'index.astro'),
    join(raw, 'index.ts'),
    join(raw, 'index.js'),
    join(raw, 'index.mjs'),
  ];
  return candidates.find((candidate) => existsSync(candidate) && statSync(candidate).isFile()) || null;
}

function transitiveSources(entryFiles) {
  const visited = new Set();
  const queue = [...entryFiles];
  while (queue.length) {
    const file = queue.shift();
    if (!file || visited.has(file) || !existsSync(file)) continue;
    visited.add(file);
    if (!['.astro', '.ts', '.js', '.mjs'].includes(extname(file))) continue;
    const source = readFileSync(file, 'utf8');
    for (const specifier of importSpecifiers(source)) {
      const target = resolveImport(file, specifier);
      if (target && target.startsWith(join(siteRoot, 'src'))) queue.push(target);
    }
  }
  return visited;
}

function publicAssetOutputPath(sourcePath) {
  const prefix = 'site/public/';
  return sourcePath.startsWith(prefix) ? sourcePath.slice(prefix.length) : null;
}

function productionInventory(contract, distRoot) {
  const manifestPath = join(distRoot, 'static-release-manifest.json');
  const buildPath = join(distRoot, 'production-build.json');
  invariant(existsSync(manifestPath), `production_manifest_missing:${manifestPath}`);
  invariant(existsSync(buildPath), `production_build_metadata_missing:${buildPath}`);

  const manifest = readJson(manifestPath);
  const build = readJson(buildPath);
  invariant(/^[0-9a-f]{40}$/u.test(manifest.repo_sha || ''), 'manifest_repo_sha_invalid');
  invariant(manifest.repo_sha === build.repo_sha, 'release_repo_sha_mismatch');
  invariant(manifest.build_id === build.build_id, 'release_build_id_mismatch');
  invariant(manifest.run_id === build.run_id, 'release_run_id_mismatch');
  invariant(manifest.snapshot?.snapshot_id === build.snapshot_id, 'release_snapshot_id_mismatch');
  invariant(manifest.snapshot?.sha256 === build.snapshot_sha256, 'release_snapshot_sha_mismatch');

  const excludedRoutes = contract.selection_policy.exclude_route_prefixes || [];
  const routes = walkFiles(distRoot, (file) => file.endsWith('.html'))
    .map((file) => routeFromDistFile(distRoot, file))
    .filter(Boolean)
    .filter((route) => !excludedRoutes.some((prefix) => route.startsWith(prefix)));
  const uniqueRoutes = [...new Set(routes)].sort();

  const pageSources = walkFiles(join(siteRoot, 'src/pages'), (file) => file.endsWith('.astro'))
    .filter((file) => !normalizePath(relative(repoRoot, file)).startsWith('site/src/pages/lab/'))
    .map((file) => ({ file, pattern: routePatternFromPageFile(file) }));

  const matchedPageFiles = new Set();
  const routeSources = [];
  for (const route of uniqueRoutes) {
    const candidates = pageSources
      .filter((entry) => routeMatches(entry.pattern, route))
      .sort((left, right) => routeSpecificity(right.pattern) - routeSpecificity(left.pattern));
    const selected = candidates[0] || null;
    if (selected) matchedPageFiles.add(selected.file);
    routeSources.push({
      route,
      source: selected ? normalizePath(relative(repoRoot, selected.file)) : null,
      source_pattern: selected?.pattern || null,
    });
  }

  const reachable = transitiveSources(matchedPageFiles);
  const reachableRepoPaths = new Set([...reachable].map((file) => normalizePath(relative(repoRoot, file))));
  const releaseFiles = new Set((manifest.files || []).map((file) => String(file.key || file.path || '')));

  const familyResults = contract.component_families.map((family) => {
    const sources = family.source_files.map((source) => {
      const publicOutput = publicAssetOutputPath(source);
      const productionReachable = publicOutput
        ? releaseFiles.has(publicOutput)
        : reachableRepoPaths.has(source);
      return {
        source,
        exists: existsSync(resolve(repoRoot, source)),
        production_reachable: productionReachable,
        release_output: publicOutput,
      };
    });
    return {
      id: family.id,
      label: family.label,
      layer: family.layer,
      required: Boolean(family.required),
      production_reachable: sources.some((source) => source.production_reachable),
      sources,
    };
  });

  const archetypeResults = contract.archetypes.map((archetype) => {
    const patterns = archetype.routes.map((pattern) => ({
      pattern,
      matched_routes: uniqueRoutes.filter((route) => routeMatches(pattern, route)),
    }));
    return {
      id: archetype.id,
      required: Boolean(archetype.required),
      production_present: patterns.every((entry) => entry.matched_routes.length > 0),
      patterns,
      evidence_viewports: archetype.evidence_viewports,
      required_scenarios: archetype.required_scenarios || [],
    };
  });

  const missingFamilies = familyResults
    .filter((family) => family.required && !family.production_reachable)
    .map((family) => family.id);
  const missingArchetypes = archetypeResults
    .filter((archetype) => archetype.required && !archetype.production_present)
    .map((archetype) => archetype.id);
  const unmatchedRoutes = routeSources.filter((entry) => !entry.source).map((entry) => entry.route);

  return {
    schema_version: 'lovekgd-design-system-production-inventory-v1',
    contract_id: contract.contract_id,
    generated_at: new Date().toISOString(),
    release: {
      repo_sha: manifest.repo_sha,
      build_id: manifest.build_id,
      run_id: manifest.run_id,
      snapshot_id: manifest.snapshot?.snapshot_id || null,
      snapshot_sha256: manifest.snapshot?.sha256 || null,
      manifest_tree_sha256: manifest.tree_sha256 || null,
    },
    counts: {
      production_routes: uniqueRoutes.length,
      matched_source_pages: matchedPageFiles.size,
      reachable_source_files: reachableRepoPaths.size,
      component_families: familyResults.length,
      production_reachable_families: familyResults.filter((family) => family.production_reachable).length,
      archetypes: archetypeResults.length,
      production_present_archetypes: archetypeResults.filter((archetype) => archetype.production_present).length,
    },
    routes: routeSources,
    component_families: familyResults,
    archetypes: archetypeResults,
    gaps: {
      missing_required_families: missingFamilies,
      missing_required_archetypes: missingArchetypes,
      production_routes_without_source_mapping: unmatchedRoutes,
    },
  };
}

function main() {
  const contractPath = resolve(valueAfter('--contract', defaultContract));
  invariant(existsSync(contractPath), `contract_missing:${contractPath}`);
  const contract = readJson(contractPath);
  const contractCheck = validateContract(contract);
  invariant(contractCheck.missingSourceFiles.length === 0, `contract_source_files_missing:${JSON.stringify(contractCheck.missingSourceFiles)}`);

  const distArg = valueAfter('--dist');
  if (!distArg) {
    process.stdout.write(`${JSON.stringify({
      ok: true,
      mode: 'source-contract',
      contract_id: contract.contract_id,
      component_families: contract.component_families.length,
      archetypes: contract.archetypes.length,
      viewports: contract.evidence_viewports.length,
      plugin_open_per_update: contract.interaction_budget.plugin_open_per_update,
      maximum_user_actions: contract.interaction_budget.maximum_user_actions,
    })}\n`);
    return;
  }

  const distRoot = resolve(distArg);
  invariant(existsSync(distRoot), `dist_missing:${distRoot}`);
  const inventory = productionInventory(contract, distRoot);
  const outputPath = resolve(valueAfter('--out', join(repoRoot, 'artifacts/design-system/production-surface-inventory.json')));
  mkdirSync(dirname(outputPath), { recursive: true });
  writeFileSync(outputPath, `${JSON.stringify(inventory, null, 2)}\n`);
  process.stdout.write(`${JSON.stringify({ ...inventory.counts, gaps: inventory.gaps })}\nartifact=${normalizePath(relative(repoRoot, outputPath))}\n`);

  if (hasFlag('--strict-production')) {
    const gapCount = inventory.gaps.missing_required_families.length
      + inventory.gaps.missing_required_archetypes.length
      + inventory.gaps.production_routes_without_source_mapping.length;
    if (gapCount > 0) process.exitCode = 1;
  }
}

main();
