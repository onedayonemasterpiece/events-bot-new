#!/usr/bin/env node
/**
 * Collection-specific browser gate for an immutable candidate or production root.
 * A candidate URL can contain a bearer token; reports and evidence are redacted.
 */

import { createHash } from 'node:crypto';
import {
  existsSync,
  mkdirSync,
  readFileSync,
  writeFileSync,
} from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const SCRIPT_PATH = fileURLToPath(import.meta.url);
const SITE_DIR = dirname(dirname(SCRIPT_PATH));
const DEFAULT_CONTRACT = resolve(
  SITE_DIR,
  'scripts',
  'static-collections-e2e.contract.v1.json',
);
const VIEWPORTS = Object.freeze([
  Object.freeze({ name: 'desktop', width: 1440, height: 900 }),
  Object.freeze({ name: 'mobile', width: 390, height: 844 }),
]);
const ACTION_TIMEOUT_MS = 10_000;
const NAVIGATION_TIMEOUT_MS = 18_000;

export class CollectionCheckError extends Error {
  constructor(code, message, metadata = {}) {
    super(message);
    this.name = 'CollectionCheckError';
    this.code = code;
    this.metadata = metadata;
  }
}

function invariant(condition, code, message, metadata = {}) {
  if (!condition) throw new CollectionCheckError(code, message, metadata);
}

function parseArgs(argv) {
  const result = {};
  for (let index = 0; index < argv.length; index += 1) {
    const raw = argv[index];
    invariant(raw.startsWith('--'), 'argument_invalid', `Unexpected argument: ${raw}`);
    const [name, inline] = raw.slice(2).split('=', 2);
    if (inline !== undefined) {
      result[name.replaceAll('-', '_')] = inline;
      continue;
    }
    const value = argv[++index];
    invariant(value && !value.startsWith('--'), 'argument_missing', `Missing value for --${name}`);
    result[name.replaceAll('-', '_')] = value;
  }
  return result;
}

export function normalizeRoutePath(value) {
  const raw = String(value || '').trim();
  invariant(raw.startsWith('/'), 'contract_route_invalid', 'Route must start with "/"');
  invariant(!raw.includes('..') && !raw.includes('\\'), 'contract_route_invalid', `Unsafe route: ${raw}`);
  const [pathname] = raw.split(/[?#]/u, 1);
  const collapsed = pathname.replace(/\/+/gu, '/');
  return collapsed === '/' ? '/' : `/${collapsed.replace(/^\/+|\/+$/gu, '')}/`;
}

export function normalizeResourcePath(value) {
  const raw = String(value || '').trim();
  invariant(raw.startsWith('/'), 'contract_resource_invalid', 'Resource path must start with "/"');
  invariant(!raw.includes('..') && !raw.includes('\\'), 'contract_resource_invalid', `Unsafe resource path: ${raw}`);
  const [pathname] = raw.split(/[?#]/u, 1);
  const collapsed = pathname.replace(/\/+/gu, '/');
  return collapsed === '/' ? '/' : `/${collapsed.replace(/^\/+|\/+$/gu, '')}`;
}

export function normalizeBaseUrl(value) {
  let url;
  try {
    url = new URL(String(value || ''));
  } catch (_) {
    throw new CollectionCheckError('e2e_url_invalid', 'Base URL is not a valid URL');
  }
  invariant(['http:', 'https:'].includes(url.protocol), 'e2e_url_invalid', 'Base URL must use http or https');
  url.hash = '';
  url.search = '';
  url.pathname = url.pathname.endsWith('/') ? url.pathname : `${url.pathname}/`;
  return url;
}

export function resolveRouteUrl(baseValue, routeValue) {
  const base = baseValue instanceof URL ? new URL(baseValue) : normalizeBaseUrl(baseValue);
  return new URL(normalizeRoutePath(routeValue).replace(/^\/+/, ''), base);
}

export function resolveResourceUrl(baseValue, resourceValue) {
  const base = baseValue instanceof URL ? new URL(baseValue) : normalizeBaseUrl(baseValue);
  return new URL(normalizeResourcePath(resourceValue).replace(/^\/+/, ''), base);
}

export function stripBasePrefix(baseValue, urlValue) {
  const base = baseValue instanceof URL ? baseValue : normalizeBaseUrl(baseValue);
  const url = urlValue instanceof URL ? urlValue : new URL(urlValue, base);
  let pathname = url.pathname.replace(/\/+/gu, '/');
  const prefix = base.pathname.replace(/\/+/gu, '/');
  if (prefix !== '/' && (pathname === prefix.slice(0, -1) || pathname.startsWith(prefix))) {
    pathname = pathname.slice(prefix.length - 1) || '/';
  }
  return normalizeRoutePath(pathname);
}

export function redactedBaseDescriptor(baseValue) {
  const base = baseValue instanceof URL ? baseValue : normalizeBaseUrl(baseValue);
  return {
    configured: true,
    protocol: base.protocol,
    path_prefix_depth: base.pathname.split('/').filter(Boolean).length,
    value: '[REDACTED]',
  };
}

export function redactText(value, baseValue) {
  const base = baseValue instanceof URL ? baseValue : normalizeBaseUrl(baseValue);
  let text = String(value ?? '');
  const candidates = [
    base.href,
    base.href.replace(/\/$/, ''),
    `${base.origin}${base.pathname}`,
    base.pathname,
    decodeURIComponent(base.pathname),
  ].filter((candidate) => candidate && candidate !== '/');
  for (const candidate of [...new Set(candidates)].sort((a, b) => b.length - a.length)) {
    text = text.split(candidate).join('/[REDACTED-BASE]/');
  }
  return text;
}

function redactValue(value, base) {
  if (typeof value === 'string') return redactText(value, base);
  if (Array.isArray(value)) return value.map((item) => redactValue(item, base));
  if (value && typeof value === 'object') {
    return Object.fromEntries(
      Object.entries(value).map(([key, item]) => [key, redactValue(item, base)]),
    );
  }
  return value;
}

export function duplicateValues(values) {
  const seen = new Set();
  const duplicates = new Set();
  for (const raw of values) {
    const value = String(raw || '').trim();
    if (!value) continue;
    if (seen.has(value)) duplicates.add(value);
    seen.add(value);
  }
  return [...duplicates].sort();
}

function candidatePaths(base, rawUrl) {
  let url;
  try {
    url = new URL(rawUrl, base);
  } catch (_) {
    return [];
  }
  const paths = new Set([normalizeRoutePath(url.pathname)]);
  try {
    paths.add(stripBasePrefix(base, url));
  } catch (_) {
    // Canonical production URL can legitimately omit the candidate prefix.
  }
  return [...paths];
}

export function routeIsListed(baseValue, rawUrls, routeValue) {
  const base = baseValue instanceof URL ? baseValue : normalizeBaseUrl(baseValue);
  const expected = normalizeRoutePath(routeValue);
  return rawUrls.some((raw) => candidatePaths(base, raw).includes(expected));
}

function parseLocs(xml) {
  return [...String(xml || '').matchAll(/<loc>\s*([^<]+?)\s*<\/loc>/giu)]
    .map((match) => match[1].replaceAll('&amp;', '&').trim())
    .filter(Boolean);
}

function loadContract(path) {
  invariant(existsSync(path), 'contract_missing', `Contract is missing: ${path}`);
  const contract = JSON.parse(readFileSync(path, 'utf8'));
  invariant(contract.schema_version === 'static-collections-e2e-contract-v1', 'contract_schema_invalid', 'Unsupported contract schema');
  invariant(Array.isArray(contract.routes) && contract.routes.length > 0, 'contract_routes_missing', 'Contract routes are missing');
  const labels = [];
  const paths = [];
  for (const route of contract.routes) {
    invariant(route && typeof route === 'object' && !Array.isArray(route), 'contract_route_invalid', 'Each route must be an object');
    invariant(typeof route.label === 'string' && route.label.trim(), 'contract_label_missing', 'Each route requires label');
    invariant(['public', 'shadow', 'blocked'].includes(route.state), 'contract_state_invalid', `Unsupported state for ${route.label}`);
    labels.push(route.label);
    paths.push(normalizeRoutePath(route.path));
  }
  invariant(duplicateValues(labels).length === 0, 'contract_label_duplicate', 'Contract labels must be unique');
  invariant(duplicateValues(paths).length === 0, 'contract_path_duplicate', 'Contract paths must be unique');
  return contract;
}

async function readSitemap(context, base, contract) {
  const rootUrl = resolveResourceUrl(base, contract.sitemap_path || '/sitemap-index.xml');
  const response = await context.request.get(rootUrl.href, {
    failOnStatusCode: false,
    maxRedirects: 5,
  });
  const status = response.status();
  if (status < 200 || status >= 300) {
    invariant(contract.require_sitemap === false, 'sitemap_missing', `Required sitemap returned HTTP ${status}`, { status });
    return { status, urls: [] };
  }
  const xml = await response.text();
  const locs = parseLocs(xml);
  if (!/<sitemapindex\b/iu.test(xml)) return { status, urls: locs };

  invariant(locs.length <= 20, 'sitemap_index_unbounded', `Sitemap index has ${locs.length} children`);
  const urls = [];
  for (const raw of locs) {
    let source;
    try {
      source = new URL(raw, rootUrl);
    } catch (_) {
      throw new CollectionCheckError('sitemap_child_invalid', 'Sitemap child URL is invalid');
    }
    let path = source.pathname;
    if (base.pathname !== '/' && path.startsWith(base.pathname)) {
      path = path.slice(base.pathname.length - 1);
    }
    const childUrl = resolveResourceUrl(base, path);
    const child = await context.request.get(childUrl.href, {
      failOnStatusCode: false,
      maxRedirects: 5,
    });
    invariant(
      child.status() >= 200 && child.status() < 300,
      'sitemap_child_missing',
      `Sitemap child returned HTTP ${child.status()}`,
      { status: child.status() },
    );
    urls.push(...parseLocs(await child.text()));
  }
  return { status, urls };
}

async function readInventory(context, base, contract) {
  const sitemap = await readSitemap(context, base, contract);
  const page = await context.newPage();
  const consoleErrors = [];
  const pageErrors = [];
  page.on('console', (message) => {
    if (message.type() === 'error') consoleErrors.push(message.text());
  });
  page.on('pageerror', (error) => pageErrors.push(error.message));
  const response = await page.goto(
    resolveRouteUrl(base, contract.navigation_entrypoint || '/').href,
    { waitUntil: 'domcontentloaded' },
  );
  invariant(response && response.status() < 400, 'navigation_entrypoint_missing', `Navigation entrypoint returned HTTP ${response?.status() ?? 'none'}`);
  const selector = contract.navigation_selector || '[data-static-collection-nav] a[href]';
  const root = page.locator('[data-static-collection-nav]');
  if (contract.require_navigation_contract !== false) {
    invariant(await root.count() === 1, 'navigation_contract_missing', 'data-static-collection-nav is missing');
  }
  const navigationUrls = await root.count()
    ? await page.locator(selector).evaluateAll((links) => links.map((link) => link.href))
    : [];
  await page.close();
  invariant(consoleErrors.length === 0, 'browser_console_error', `Navigation emitted ${consoleErrors.length} console error(s)`);
  invariant(pageErrors.length === 0, 'browser_page_error', `Navigation emitted ${pageErrors.length} page error(s)`);
  return {
    sitemapStatus: sitemap.status,
    sitemapUrls: sitemap.urls,
    navigationUrls,
  };
}

function listingExpectation(base, inventory, route, key) {
  const values = key === 'navigation' ? inventory.navigationUrls : inventory.sitemapUrls;
  const expected = Boolean(route[key]);
  const actual = routeIsListed(base, values, route.path);
  invariant(actual === expected, `${key}_mismatch`, `${route.label} expected ${key}=${expected}, actual=${actual}`, {
    label: route.label,
    expected,
    actual,
  });
  return actual;
}

async function inspectCards(page, context, base, route) {
  const cards = page.locator('[data-static-collection-page] [data-event-card]');
  const count = await cards.count();
  const minimum = Number(route.minimum_cards ?? (route.state === 'blocked' ? 0 : 1));
  invariant(count >= minimum, 'collection_card_shortfall', `${route.label} has ${count} cards, minimum=${minimum}`);

  const rows = await cards.evaluateAll((nodes) => nodes.map((node) => ({
    eventId: node.getAttribute('data-event-id') || '',
    familyId: node.getAttribute('data-family-id') || '',
    href: node.querySelector('a[href*="/sobytiya/"]')?.getAttribute('href') || '',
  })));
  for (const row of rows) {
    invariant(row.eventId, 'event_id_missing', `${route.label} card lacks data-event-id`);
    invariant(row.familyId, 'family_id_missing', `${route.label} card lacks data-family-id`);
    invariant(row.href, 'event_link_missing', `${route.label} card lacks canonical event link`);
  }
  const duplicateEvents = duplicateValues(rows.map((row) => row.eventId));
  const duplicateFamilies = duplicateValues(rows.map((row) => row.familyId));
  invariant(duplicateEvents.length === 0, 'duplicate_event_id', `${route.label} duplicates event IDs: ${duplicateEvents.join(', ')}`);
  invariant(duplicateFamilies.length === 0, 'duplicate_family_id', `${route.label} duplicates family IDs: ${duplicateFamilies.join(', ')}`);

  const checkedLinks = [];
  for (const href of [...new Set(rows.map((row) => row.href))]) {
    const url = new URL(href, page.url());
    const response = await context.request.get(url.href, {
      failOnStatusCode: false,
      maxRedirects: 5,
    });
    invariant(response.status() >= 200 && response.status() < 400, 'event_link_broken', `${route.label} event link returned HTTP ${response.status()}`);
    checkedLinks.push({ path: stripBasePrefix(base, url), status: response.status() });
  }
  return {
    count,
    uniqueEventIds: new Set(rows.map((row) => row.eventId)).size,
    uniqueFamilyIds: new Set(rows.map((row) => row.familyId)).size,
    checkedLinks,
  };
}

async function inspectImages(page) {
  const images = page.locator('[data-static-collection-page] [data-event-card] img:visible');
  const count = await images.count();
  let broken = 0;
  for (let index = 0; index < count; index += 1) {
    const image = images.nth(index);
    await image.scrollIntoViewIfNeeded().catch(() => {});
    await image.evaluate((node) => node.decode?.().catch(() => undefined)).catch(() => {});
    const ok = await image.evaluate((node) => {
      const fallback = node.closest('[data-card-media-shell], [data-card-media], article')
        ?.querySelector('[data-card-image-fallback]:not([hidden]), [data-card-media-fallback]:not([hidden]), .is-image-missing');
      return (node.complete && node.naturalWidth > 0) || Boolean(fallback);
    });
    if (!ok) broken += 1;
  }
  invariant(broken === 0, 'image_broken', `${broken} visible collection image(s) are broken`);
  return { visible: count, broken };
}

async function inspectViewport(browser, base, route, viewport, artifactDir) {
  const context = await browser.newContext({ viewport });
  context.setDefaultTimeout(ACTION_TIMEOUT_MS);
  context.setDefaultNavigationTimeout(NAVIGATION_TIMEOUT_MS);
  const page = await context.newPage();
  const consoleErrors = [];
  const pageErrors = [];
  page.on('console', (message) => {
    if (message.type() === 'error') consoleErrors.push(message.text());
  });
  page.on('pageerror', (error) => pageErrors.push(error.message));
  try {
    const response = await page.goto(resolveRouteUrl(base, route.path).href, {
      waitUntil: 'domcontentloaded',
    });
    const status = response?.status() ?? 0;
    const robots = String(
      await page.locator('meta[name="robots"]').getAttribute('content').catch(() => ''),
    ).toLowerCase();
    const noindex = robots.split(',').map((part) => part.trim()).includes('noindex');
    if (route.state === 'blocked' && [404, 410].includes(status)) {
      return { viewport, status, directState: 'absent', noindex };
    }

    invariant(status === 200, 'route_missing', `${route.label} returned HTTP ${status}`);
    const root = page.locator('[data-static-collection-page]');
    invariant(await root.count() === 1, 'collection_root_missing', `${route.label} requires one collection root`);
    invariant(await root.getAttribute('data-collection-label') === route.label, 'collection_label_mismatch', `${route.label} data-collection-label mismatch`);
    invariant(await root.getAttribute('data-publication-status') === route.state, 'publication_status_mismatch', `${route.label} publication status mismatch`);

    if (route.state === 'public') {
      invariant(!noindex, 'route_unexpectedly_noindex', `${route.label} public route is noindex`);
      const canonical = await page.locator('link[rel="canonical"]').getAttribute('href').catch(() => '');
      invariant(canonical, 'canonical_missing', `${route.label} public route lacks canonical`);
      invariant(candidatePaths(base, canonical).includes(normalizeRoutePath(route.path)), 'canonical_mismatch', `${route.label} canonical mismatch`);
    } else {
      invariant(noindex, 'route_unexpectedly_indexable', `${route.label} ${route.state} route must be noindex`);
    }

    const heading = (await page.locator('h1').first().textContent().catch(() => '')).trim();
    if (route.state !== 'blocked') invariant(heading, 'collection_heading_missing', `${route.label} lacks H1`);
    const cards = await inspectCards(page, context, base, route);
    const images = await inspectImages(page);
    const overflow = await page.evaluate(
      () => document.documentElement.scrollWidth - document.documentElement.clientWidth,
    );
    invariant(overflow <= 1, 'horizontal_overflow', `${route.label} overflows by ${overflow}px`);
    invariant(consoleErrors.length === 0, 'browser_console_error', `${route.label} emitted ${consoleErrors.length} console error(s)`);
    invariant(pageErrors.length === 0, 'browser_page_error', `${route.label} emitted ${pageErrors.length} page error(s)`);

    return {
      viewport,
      status,
      directState: route.state,
      noindex,
      collectionState: await root.getAttribute('data-collection-state'),
      catalogHashPresent: Boolean(await root.getAttribute('data-catalog-hash')),
      manifestHashPresent: Boolean(await root.getAttribute('data-manifest-hash')),
      cards,
      images,
      overflow,
    };
  } catch (error) {
    if (artifactDir) {
      mkdirSync(artifactDir, { recursive: true });
      const name = `${route.label.replace(/[^a-z0-9_-]+/giu, '-')}-${viewport.name}`;
      await page.screenshot({
        path: resolve(artifactDir, `${name}.png`),
        fullPage: true,
      }).catch(() => {});
      const html = redactText(await page.content().catch(() => ''), base);
      writeFileSync(resolve(artifactDir, `${name}.html`), html, 'utf8');
    }
    throw error;
  } finally {
    await context.close();
  }
}

function failureRecord(error, base, route = null, viewport = null) {
  return {
    code: error?.code || 'unexpected_error',
    message: redactText(String(error?.message || error), base),
    label: route?.label || error?.metadata?.label || null,
    route: route?.path || null,
    viewport,
    metadata: redactValue(error?.metadata || {}, base),
  };
}

async function run(browser, base, contract, artifactDir) {
  const inventoryContext = await browser.newContext();
  inventoryContext.setDefaultTimeout(ACTION_TIMEOUT_MS);
  inventoryContext.setDefaultNavigationTimeout(NAVIGATION_TIMEOUT_MS);
  let inventory;
  try {
    inventory = await readInventory(inventoryContext, base, contract);
  } finally {
    await inventoryContext.close();
  }

  const failures = [];
  const routes = [];
  for (const route of contract.routes.filter((item) => item.enabled !== false)) {
    const listing = {};
    for (const key of ['navigation', 'sitemap']) {
      try {
        listing[key] = listingExpectation(base, inventory, route, key);
      } catch (error) {
        failures.push(failureRecord(error, base, route));
      }
    }
    const viewports = [];
    for (const viewport of VIEWPORTS) {
      try {
        viewports.push(await inspectViewport(
          browser,
          base,
          route,
          viewport,
          artifactDir,
        ));
      } catch (error) {
        failures.push(failureRecord(error, base, route, viewport));
      }
    }
    routes.push({
      label: route.label,
      path: normalizeRoutePath(route.path),
      expectedState: route.state,
      listing,
      viewports,
    });
  }
  return {
    ok: failures.length === 0,
    inventory: {
      sitemapStatus: inventory.sitemapStatus,
      sitemapUrlCount: inventory.sitemapUrls.length,
      navigationUrlCount: inventory.navigationUrls.length,
    },
    routes,
    failures,
  };
}

function writeReport(path, report) {
  if (!path) return;
  mkdirSync(dirname(resolve(path)), { recursive: true });
  writeFileSync(resolve(path), `${JSON.stringify(report, null, 2)}\n`, 'utf8');
}

export async function main(argv = process.argv.slice(2)) {
  const args = parseArgs(argv);
  invariant(args.base_url, 'e2e_url_missing', '--base-url is required');
  const base = normalizeBaseUrl(args.base_url);
  const contractPath = resolve(args.contract || DEFAULT_CONTRACT);
  const contract = loadContract(contractPath);
  const artifactDir = args.artifact_dir ? resolve(args.artifact_dir) : '';
  const { chromium } = await import('playwright');
  const browser = await chromium.launch({ headless: true });
  let result;
  try {
    result = await run(browser, base, contract, artifactDir);
  } catch (error) {
    result = {
      ok: false,
      inventory: null,
      routes: [],
      failures: [failureRecord(error, base)],
    };
  } finally {
    await browser.close().catch(() => {});
  }

  const report = {
    schema_version: 'static-collections-browser-e2e-report-v1',
    generated_at: new Date().toISOString(),
    ok: result.ok,
    base: redactedBaseDescriptor(base),
    contract_sha256: createHash('sha256').update(readFileSync(contractPath)).digest('hex'),
    inventory: result.inventory,
    routes: result.routes,
    failures: result.failures,
  };
  writeReport(args.report, report);
  console.log(`Static collections browser E2E: ${JSON.stringify({
    ok: report.ok,
    routes: report.routes.length,
    failures: report.failures.map((item) => ({
      code: item.code,
      label: item.label,
      viewport: item.viewport?.name || null,
    })),
  })}`);
  if (!report.ok) process.exitCode = 1;
  return report;
}

if (process.argv[1] && resolve(process.argv[1]) === resolve(SCRIPT_PATH)) {
  main().catch((error) => {
    console.error(`Static collections browser E2E failed: ${JSON.stringify({
      code: error?.code || 'unexpected_error',
      message: String(error?.message || error),
    })}`);
    process.exitCode = 1;
  });
}
