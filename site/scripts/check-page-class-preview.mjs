import { existsSync, readFileSync, readdirSync, statSync } from 'node:fs';
import { join, resolve } from 'node:path';
import {
  normalizeStaticSiteFocusedEvent,
  normalizeStaticSitePageClasses,
  normalizeStaticSiteRouteFamilies,
  validateFocusedMaterializedRoutes,
} from './page-class-build-filter.mjs';

const siteDir = resolve(new URL('..', import.meta.url).pathname);
const buildId = String(process.env.PREVIEW_BUILD_ID || '').trim();
if (!/^preview-[A-Za-z0-9][A-Za-z0-9._-]*$/u.test(buildId)) {
  throw new Error('check:preview-slice requires the exact PREVIEW_BUILD_ID');
}
const pageClasses = normalizeStaticSitePageClasses(process.env.STATIC_SITE_PAGE_CLASSES || '');
if (pageClasses[0] === 'all') throw new Error('check:preview-slice requires at least one named page class');
const root = join(siteDir, 'dist', buildId);
const required = [
  '__preview/index.html',
  'robots.txt',
  'preview-build.json',
];
const representatives = {
  event: 'sobytiya',
  date: 'segodnya/index.html',
  weekend: 'vyhodnye/index.html',
  collection: 'vystavki/index.html',
  personal: 'poisk/index.html',
  focus: 'fokus-gruppa/index.html',
  partner: 'partnerstvo/index.html',
  lab: 'lab/design-system/index.html',
};
for (const item of required) {
  if (!existsSync(join(root, item))) throw new Error(`page-class preview missing ${item}`);
}

const manifest = JSON.parse(readFileSync(join(root, 'preview-build.json'), 'utf8'));
if (manifest.buildId !== buildId || JSON.stringify(manifest.pageClasses) !== JSON.stringify(pageClasses)) {
  throw new Error('page-class preview manifest identity mismatch');
}
const index = readFileSync(join(root, '__preview/index.html'), 'utf8');
if (!/name=["']robots["'][^>]+noindex/iu.test(index)) {
  throw new Error('page-class preview shell must remain noindex');
}

function checkClassRepresentatives() {
  for (const pageClass of pageClasses) {
    const relative = representatives[pageClass];
    const target = join(root, relative);
    if (!existsSync(target)) throw new Error(`page-class preview missing representative for ${pageClass}: ${relative}`);
    if (pageClass === 'event') {
      const eventPages = readdirSync(target).filter((name) => (
        statSync(join(target, name)).isDirectory() && existsSync(join(target, name, 'index.html'))
      ));
      if (!eventPages.length) throw new Error('event page-class preview contains no event page');
    }
  }
}

function routeOutputPath(routeValue) {
  const route = String(routeValue || '');
  if (!route.startsWith('/') || !route.endsWith('/') || route.includes('\\') || route.includes('?') || route.includes('#')) {
    throw new Error(`focused preview manifest contains an invalid route: ${route || '(empty)'}`);
  }
  const segments = route.split('/').filter(Boolean);
  if (segments.some((segment) => segment === '.' || segment === '..')) {
    throw new Error(`focused preview manifest route escapes output root: ${route}`);
  }
  return join(root, ...segments, 'index.html');
}

if (manifest.localFocused === true) {
  if (manifest.fullBuild !== false || manifest.publicationMode !== 'local-only') {
    throw new Error('local-focused preview must declare fullBuild=false and publicationMode=local-only');
  }
  const expectedRepoSha = String(process.env.STATIC_SITE_REPO_SHA || '').trim().toLowerCase();
  if (!/^[0-9a-f]{40}$/u.test(expectedRepoSha) || manifest.repo_sha !== expectedRepoSha) {
    throw new Error('local-focused preview repo SHA mismatch');
  }
  const snapshot = manifest.snapshot;
  if (
    !snapshot
    || snapshot.schemaVersion !== 'static_site_projection_snapshot_v1'
    || !snapshot.snapshotId
    || !/^[0-9a-f]{64}$/u.test(String(snapshot.sha256 || ''))
    || !/^[0-9a-f]{64}$/u.test(String(snapshot.manifestSha256 || ''))
  ) {
    throw new Error('local-focused preview snapshot identity is incomplete');
  }
  const selection = manifest.selection;
  if (
    !selection
    || !['page-class', 'event-detail', 'focused-routes'].includes(selection.mode)
    || JSON.stringify(selection.pageClasses) !== JSON.stringify(pageClasses)
    || !Array.isArray(selection.routeFamilies)
  ) {
    throw new Error('local-focused preview selection identity mismatch');
  }
  const expectedRouteFamilies = normalizeStaticSiteRouteFamilies(
    process.env.STATIC_SITE_FOCUSED_ROUTE_FAMILIES || '',
  );
  const expectedEvent = normalizeStaticSiteFocusedEvent(process.env.STATIC_SITE_FOCUSED_EVENT || null);
  if (
    JSON.stringify(selection.routeFamilies) !== JSON.stringify(expectedRouteFamilies)
    || JSON.stringify(selection.event) !== JSON.stringify(expectedEvent)
  ) {
    throw new Error('local-focused preview selector environment mismatch');
  }
  if (
    !Array.isArray(manifest.materializedRoutes)
    || !manifest.materializedRoutes.length
    || new Set(manifest.materializedRoutes).size !== manifest.materializedRoutes.length
    || !Array.isArray(manifest.userFacingRoutes)
    || !manifest.userFacingRoutes.length
    || new Set(manifest.userFacingRoutes).size !== manifest.userFacingRoutes.length
  ) {
    throw new Error('local-focused preview route inventory is missing or duplicated');
  }
  for (const route of manifest.materializedRoutes) {
    const output = routeOutputPath(route);
    if (!existsSync(output)) {
      throw new Error(`local-focused preview manifest claims a missing page: ${route}`);
    }
  }
  for (const route of manifest.userFacingRoutes) {
    if (!manifest.materializedRoutes.includes(route)) {
      throw new Error(`local-focused preview user-facing route is not materialized: ${route}`);
    }
  }
  if (expectedRouteFamilies.length || expectedEvent) {
    validateFocusedMaterializedRoutes(manifest.materializedRoutes, {
      routeFamilies: expectedRouteFamilies,
      event: expectedEvent,
    });
  } else {
    checkClassRepresentatives();
  }
  if (!index.includes('data-local-focused-preview')) {
    throw new Error('local-focused preview must replace the broad hub with a compact focused index');
  }
  const links = [];
  const linkPattern = /<a\b[^>]*data-focused-route=["']([^"']+)["'][^>]*href=["']([^"']+)["'][^>]*>/giu;
  for (const match of index.matchAll(linkPattern)) {
    links.push({ route: match[1], href: match[2] });
  }
  if (JSON.stringify(links.map((item) => item.route)) !== JSON.stringify(manifest.userFacingRoutes)) {
    throw new Error('local-focused preview index must link exactly the user-facing materialized routes');
  }
  for (const { route, href } of links) {
    if (href !== `/${buildId}${route}` || !existsSync(routeOutputPath(route))) {
      throw new Error(`local-focused preview index contains a non-materialized link: ${href}`);
    }
  }
  const metrics = manifest.metrics;
  if (
    !metrics
    || metrics.routeCount !== manifest.materializedRoutes.length
    || metrics.userFacingPageCount !== manifest.userFacingRoutes.length
    || !Number.isInteger(metrics.objectCount)
    || metrics.objectCount <= 0
    || !(Number(metrics.totalSeconds) >= 0)
  ) {
    throw new Error('local-focused preview metrics do not match the artifact');
  }
  console.log(
    `Local-focused preview check passed: ${buildId}; mode=${selection.mode}; `
    + `classes=${pageClasses.join(',')}; routes=${manifest.materializedRoutes.length}`,
  );
} else {
  checkClassRepresentatives();
  console.log(`Page-class preview check passed: ${buildId}; classes=${pageClasses.join(',')}`);
}
