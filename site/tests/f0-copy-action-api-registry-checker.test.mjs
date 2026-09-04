import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), '../..');
const read = (path) => readFileSync(resolve(repoRoot, path), 'utf8');

const component = read('site/src/components/design-system/CopyAction.astro');
const catalog = read('site/src/pages/lab/design-system/index.astro');
const checker = read('site/scripts/check-design-system.mjs');
const foundations = read('site/src/components/design-system/foundations.css');
const legacyCss = read('site/src/styles/design-system.css');

test('CopyAction@2 implements named size and catalog-only preview fixtures', () => {
  assert.match(component, /size\?: 'default' \| 'large';/u);
  assert.match(component, /previewState\?: 'success' \| 'error';/u);
  assert.match(component, /data-ds-size=\{size\}/u);
  assert.match(component, /data-copy-preview-state=\{previewState\}/u);
  assert.match(component, /data-copy-state-source=\{previewState \? 'catalog-preview' : 'runtime'\}/u);
  assert.match(component, /data-copy-state=\{previewState\}/u);
  assert.match(component, /data-ds-state=\{initialState\}/u);
  assert.match(component, /\.ke-copy-action--large\s*\{[^}]*--ke-copy-action-target-size: var\(--ke-size-control-large\)/su);
  assert.match(component, /width: var\(--ke-copy-action-target-size\)/u);
  assert.match(component, /height: var\(--ke-copy-action-target-size\)/u);
  assert.match(component, /width: var\(--ke-icon-size-control\)/u);
  assert.match(component, /height: var\(--ke-icon-size-control\)/u);
});

test('catalog preview provenance is removed before the real clipboard state machine runs', () => {
  const previewReset = component.indexOf('delete button.dataset.copyPreviewState');
  const runtimeOrigin = component.indexOf("button.dataset.copyStateSource = 'runtime'");
  const busy = component.indexOf("button.setAttribute('aria-busy', 'true')");
  const success = component.indexOf("button.dataset.copyState = 'success'");
  const error = component.indexOf("button.dataset.copyState = 'error'");
  assert.ok(previewReset >= 0 && runtimeOrigin > previewReset && busy > runtimeOrigin);
  assert.ok(success > busy && error > busy);
  assert.match(component, /navigator\.clipboard\?\.writeText/u);
  assert.match(component, /document\.execCommand\('copy'\)/u);
  assert.match(component, /delete button\.dataset\.copyState/u);
  assert.match(component, /button\.dataset\.dsState = 'default'/u);
  assert.match(component, /data-ke-copy-status role="status" aria-live="polite" aria-atomic="true"/u);
});

test('catalog and registry describe the implemented CopyAction@2 contract', () => {
  assert.match(catalog, /<CopyAction\b[^>]*\bsize="large"/u);
  assert.match(catalog, /<CopyAction\b[^>]*\bpreviewState="success"/u);
  assert.match(catalog, /<CopyAction\b[^>]*\bpreviewState="error"/u);
  assert.match(catalog, /data-copy-action-state-evidence="runtime-required"/u);
  const row = catalog.match(/<tr data-ds-component="CopyAction" data-ds-version="2">[\s\S]*?<\/tr>/u)?.[0] || '';
  assert.match(row, /tone="success">approved/u);
  assert.match(row, /catalog-preview success\/error fixtures/u);
  assert.match(row, /runtime clipboard success\/error\/reset/u);
  assert.doesNotMatch(catalog, /<tr data-ds-component="CopyAction" data-ds-version="1"/u);
});

test('checker requires component implementation and rejects obsolete global architecture', () => {
  assert.match(checker, /CopyAction@2 component implementation misses/u);
  assert.match(checker, /data-copy-state-source=\{previewState \? 'catalog-preview' : 'runtime'\}/u);
  assert.match(checker, /Legacy design-system\.css must not match the canonical CopyAction@2 root/u);
  assert.match(checker, /CopyAction registry must describe approved CopyAction@2/u);
  assert.doesNotMatch(checker, /CopyAction must consume the fixed 44px icon-only button contract/u);
  assert.doesNotMatch(checker, /copyAction\.includes\("'ke-button--icon'"\)/u);
  assert.doesNotMatch(checker, /css\.includes\('\.ke-copy-action__check-icon'\)/u);
});

test('foundation and style ownership remain central and exactly four-role compatible', () => {
  assert.match(component, /data-ke-style-owner="CopyAction\.astro"/u);
  assert.match(component, /data-ke-icon-role="control"/u);
  assert.match(foundations, /--ke-size-touch-target: var\(--ke-control-min\);/u);
  assert.match(foundations, /--ke-size-control-large: 52px;/u);
  assert.match(foundations, /--ke-icon-size-control: 20px;/u);
  assert.doesNotMatch(legacyCss, /\.ke-copy-action/u);
  assert.doesNotMatch(component, /(?:width|height):\s*(?:20|44|52)px\b/u);
});


test('checker follows actual import, component action and token registry boundaries', () => {
  assert.doesNotMatch(checker, /Root page does not load the canonical design-system CSS/u);
  assert.match(checker, /Root page must consume canonical design-system CSS through EventLayout/u);
  assert.doesNotMatch(checker, /Root page must use canonical design-system actions/u);
  assert.match(checker, /Root page must not bypass registered action components with raw ke-button classes/u);
  assert.doesNotMatch(checker, /Approved brand tag colors must come from design-system tokens/u);
  assert.match(checker, /Canonical brand tag paint tokens are missing/u);
});
