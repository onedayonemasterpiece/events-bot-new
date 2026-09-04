import assert from 'node:assert/strict';
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import {
  buildCanonicalExporterArgs,
  listProducedPaths,
  localFocusedBrowserEpoch,
  parseLocalFocusedArgs,
  requestIsSpeculativePrefetch,
  resolvePlaywrightApi,
  resolvePageClass,
} from './run-local-focused-preview.mjs';
import { STATIC_SITE_PAGE_CLASSES } from './page-class-build-filter.mjs';

test('route and page-class modes are mutually exclusive', () => {
  assert.equal(parseLocalFocusedArgs(['--route', '/segodnya/', '--no-serve']).route, '/segodnya/');
  assert.equal(parseLocalFocusedArgs(['--page-class', 'date', '--no-serve']).pageClass, 'date');
  assert.throws(() => parseLocalFocusedArgs(['--route', '/segodnya/', '--page-class', 'date']), /mutually exclusive/u);
});

test('page-class validation delegates to canonical registry export', () => {
  for (const pageClass of STATIC_SITE_PAGE_CLASSES) assert.equal(resolvePageClass(pageClass), pageClass);
  assert.throws(() => resolvePageClass('all'), /exactly one canonical class/u);
  assert.throws(() => resolvePageClass('invented'), /Unknown STATIC_SITE_PAGE_CLASSES/u);
  const source = readFileSync(new URL('./run-local-focused-preview.mjs', import.meta.url), 'utf8');
  assert.doesNotMatch(source, /SELECTABLE_PAGE_CLASSES|new Set\(\['event'/u);
  assert.match(source, /configuredBrowser && existsSync\(configuredBrowser\)/u);
  assert.match(source, /npm', \['ci', '--no-audit', '--no-fund'\]/u);
});

test('produced path inventory excludes shared assets but includes runtime endpoints', () => {
  const root = mkdtempSync(join(tmpdir(), 'focused-paths-'));
  try {
    for (const rel of [
      'segodnya/index.html',
      '__preview/index.html',
      '_astro/app.abc.js',
      'assets/icons/ticket.svg.metadata.json',
      'service-share/current/manifest.json',
      'data/discovery/7.json',
      'sobytiya/one/event.ics',
      'preview-build.json',
      'robots.txt',
    ]) {
      const path = join(root, rel);
      mkdirSync(join(path, '..'), { recursive: true });
      writeFileSync(path, rel);
    }
    assert.deepEqual(listProducedPaths(root), [
      '/__preview/',
      '/data/discovery/7.json',
      '/robots.txt',
      '/segodnya/',
      '/sobytiya/one/event.ics',
    ]);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test('staged Playwright supports both ESM named and CommonJS default exports', () => {
  const named = { chromium: { launch: () => 'named' } };
  const commonJs = { default: { chromium: { launch: () => 'default' } } };
  assert.equal(resolvePlaywrightApi(named).chromium.launch(), 'named');
  assert.equal(resolvePlaywrightApi(commonJs).chromium.launch(), 'default');
  assert.throws(() => resolvePlaywrightApi({ default: {} }), /does not expose chromium/u);
});

test('browser smoke is pinned to the immutable build date', () => {
  const epoch = localFocusedBrowserEpoch('2026-07-23');
  assert.equal(new Date(epoch).toISOString(), '2026-07-23T10:00:00.000Z');
  assert.throws(() => localFocusedBrowserEpoch('2026-02-31'), /Invalid local focused browser date/u);
  assert.throws(() => localFocusedBrowserEpoch('today'), /Invalid local focused browser date/u);
});

test('exact-route smoke distinguishes speculative neighbour prefetch from owned requests', () => {
  const request = (resourceType, headers) => ({
    resourceType: () => resourceType,
    headers: () => headers,
  });
  assert.equal(requestIsSpeculativePrefetch(request('other', { 'sec-purpose': 'prefetch' })), true);
  assert.equal(requestIsSpeculativePrefetch(request('other', { purpose: 'prefetch' })), true);
  assert.equal(requestIsSpeculativePrefetch(request('document', { 'sec-purpose': 'prefetch' })), false);
  assert.equal(requestIsSpeculativePrefetch(request('fetch', {})), false);
});


test('real-data focused export only sends flags supported by the canonical exporter', () => {
  const args = buildCanonicalExporterArgs({ stagedSite: '/site', db: '/frozen.sqlite', outputDir: '/data', limit: 300, buildId: 'preview-local-test', sourceSha: 'a'.repeat(40), snapshotIdentity: 'b'.repeat(64), snapshotSize: 42, currentDate: '2026-09-04', currentDatetime: '2026-09-04T22:53:39+02:00', pageClass: 'focus', skipImageProbes: true });
  const source = readFileSync(new URL('./export-production-preview-data.py', import.meta.url), 'utf8');
  const supported = new Set([...source.matchAll(/add_argument\(\s*["'](--[a-z0-9-]+)["']/gu)].map(m => m[1]));
  for (const arg of args.filter(arg => arg.startsWith('--'))) assert.ok(supported.has(arg), `unsupported canonical exporter flag ${arg}`);
  assert.equal(args[args.indexOf('--db') + 1], '/frozen.sqlite');
  assert.equal(args[args.indexOf('--snapshot-sha256') + 1], 'b'.repeat(64));
  assert.equal(args[args.indexOf('--current-datetime') + 1], '2026-09-04T22:53:39+02:00');
});
