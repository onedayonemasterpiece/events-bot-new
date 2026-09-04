import assert from 'node:assert/strict';
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import {
  listProducedPaths,
  parseLocalFocusedArgs,
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
