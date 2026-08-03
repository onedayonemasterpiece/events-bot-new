#!/usr/bin/env node
import { existsSync, mkdirSync, readFileSync, readdirSync, writeFileSync } from 'node:fs';
import { dirname, join, relative, resolve, sep } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const siteRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const repoRoot = resolve(siteRoot, '..');

function htmlFiles(root) {
  const result = [];
  const walk = (directory) => {
    for (const entry of readdirSync(directory, { withFileTypes: true })) {
      const path = join(directory, entry.name);
      if (entry.isDirectory()) walk(path);
      else if (entry.isFile() && entry.name.endsWith('.html')) result.push(path);
    }
  };
  walk(root);
  return result.sort();
}

function attributeValues(html, name) {
  const expression = new RegExp(`${name}=(?:"([^"]*)"|'([^']*)')`, 'giu');
  return Array.from(html.matchAll(expression), (match) => match[1] ?? match[2] ?? '');
}

function publicPath(relativePath) {
  let path = `/${relativePath.split(sep).join('/')}`;
  path = path.replace(/\/index\.html$/u, '/').replace(/\.html$/u, '/');
  return path.replace(/^\/preview-[A-Za-z0-9._-]+(?=\/)/u, '') || '/';
}

function isExcludedTestArtifact(path) {
  return path.startsWith('/lab/');
}

function collectionLike(path) {
  return /^\/(?:podborki|vystavki|festivali|neobychnoe|artefakty|kluby-po-interesam)(?:\/|$)/u.test(path);
}

export function buildPersonalizationRouteInventory(distRoot) {
  const pages = htmlFiles(distRoot).map((file) => {
    const relativePath = relative(distRoot, file).split(sep).join('/');
    const path = publicPath(relativePath);
    const html = readFileSync(file, 'utf8');
    const markerCount = attributeValues(html, 'data-p13n-runtime-marker').length;
    const surfaces = Array.from(new Set(attributeValues(html, 'data-p13n-surface').filter(Boolean)));
    const legacySurfaces = Array.from(new Set(attributeValues(html, 'data-surface').filter(Boolean)));
    const policies = Array.from(new Set(attributeValues(html, 'data-p13n-policy').filter(Boolean)));
    const pageFamilies = attributeValues(html, 'data-p13n-page-family').filter(Boolean);
    const staticReasons = attributeValues(html, 'data-p13n-static-only-reason').filter(Boolean);
    const excluded = isExcludedTestArtifact(path);
    const missing = !excluded && markerCount === 0;
    const duplicate = !excluded && markerCount > 1;
    const unknown = !excluded && (!surfaces.length || surfaces.includes('unknown') || !policies.length || policies.includes('unknown-static')) && !staticReasons.length;
    const calendar = surfaces.some((surface) => ['calendar_primary', 'today_primary', 'tomorrow_primary', 'weekend_primary'].includes(surface));
    const calendarNonIdentity = !excluded && calendar && !policies.includes('calendar-exact-only');
    const legacyPromoted = policies.some((item) => /legacy/iu.test(item))
      || attributeValues(html, 'data-p13n-target-algorithm').some((item) => /legacy/iu.test(item));
    const legacyMismatch = legacySurfaces.length > 0 && legacySurfaces.some((surface) => !surfaces.includes(surface));
    return {
      relative_path: relativePath,
      public_path: path,
      page_family: pageFamilies[0] || (excluded ? 'excluded-test-artifact' : 'unknown'),
      runtime_marker_count: markerCount,
      declared_surface_ids: surfaces,
      legacy_surface_ids: legacySurfaces,
      resolved_target_policies: policies,
      static_only_reason: staticReasons[0] || null,
      excluded,
      exclusion_reason: excluded ? 'documented-isolated-lab-artifact' : null,
      legacy_behavior_mismatch_diagnostic: legacyMismatch ? 'p13n_inventory.legacy_surface_target_mismatch' : null,
      status: excluded ? 'excluded' : (missing ? 'missing-runtime' : duplicate ? 'duplicate-runtime' : unknown ? 'unclassified' : calendarNonIdentity ? 'calendar-policy-violation' : legacyPromoted ? 'legacy-promoted' : 'ok'),
      diagnostics: [
        ...(missing ? ['p13n_inventory.missing_runtime'] : []),
        ...(duplicate ? ['p13n_inventory.duplicate_runtime'] : []),
        ...(unknown ? ['p13n_inventory.unknown_surface'] : []),
        ...(calendarNonIdentity ? ['p13n_inventory.calendar_non_identity'] : []),
        ...(legacyPromoted ? ['p13n_inventory.legacy_promoted'] : []),
        ...(legacyMismatch ? ['p13n_inventory.legacy_surface_target_mismatch'] : []),
      ],
    };
  });
  const counts = {
    public_html_total: pages.filter((page) => !page.excluded).length,
    excluded_test_html: pages.filter((page) => page.excluded).length,
    public_html_missing_runtime: pages.filter((page) => !page.excluded && page.runtime_marker_count === 0).length,
    public_html_duplicate_runtime: pages.filter((page) => !page.excluded && page.runtime_marker_count > 1).length,
    public_html_unclassified: pages.filter((page) => !page.excluded && page.diagnostics.includes('p13n_inventory.unknown_surface')).length,
    collections_unknown_surface: pages.filter((page) => !page.excluded && collectionLike(page.public_path) && page.diagnostics.includes('p13n_inventory.unknown_surface')).length,
    calendar_primary_non_identity_policy: pages.filter((page) => page.diagnostics.includes('p13n_inventory.calendar_non_identity')).length,
    legacy_policy_promoted_to_target: pages.filter((page) => page.diagnostics.includes('p13n_inventory.legacy_promoted')).length,
  };
  return { schema_version: 'personalization-route-inventory-v1', counts, pages };
}

export function personalizationInventoryFailures(inventory) {
  return Object.entries(inventory.counts)
    .filter(([key, value]) => !['public_html_total', 'excluded_test_html'].includes(key) && value !== 0)
    .map(([key, value]) => `${key}=${value}`);
}

function valueAfter(flag, fallback) {
  const index = process.argv.indexOf(flag);
  return index >= 0 && process.argv[index + 1] ? process.argv[index + 1] : fallback;
}

function main() {
  const distRoot = resolve(valueAfter('--dist', join(siteRoot, 'dist')));
  const output = resolve(valueAfter('--out', join(repoRoot, 'artifacts/personalization-route-inventory.json')));
  if (!existsSync(distRoot)) throw new Error(`personalization inventory dist not found: ${distRoot}`);
  const inventory = buildPersonalizationRouteInventory(distRoot);
  mkdirSync(dirname(output), { recursive: true });
  writeFileSync(output, `${JSON.stringify(inventory, null, 2)}\n`);
  const failures = personalizationInventoryFailures(inventory);
  process.stdout.write(`${JSON.stringify(inventory.counts)}\n`);
  process.stdout.write(`artifact=${relative(repoRoot, output).split(sep).join('/')}\n`);
  if (failures.length) {
    process.stderr.write(`personalization route inventory failed: ${failures.join(', ')}\n`);
    process.exitCode = 1;
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) main();
