import assert from 'node:assert/strict';
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import test from 'node:test';

import { buildPersonalizationRouteInventory, personalizationInventoryFailures } from './check-personalization-route-inventory.mjs';

function page(root, path, marker) {
  const directory = join(root, path);
  mkdirSync(directory, { recursive: true });
  writeFileSync(join(directory, 'index.html'), `<html><body>${marker}</body></html>`);
}

const marker = (surface, policy, family, reason = '') => `<span data-p13n-runtime-marker="p13n-runtime-v1" data-p13n-surface="${surface}" data-p13n-policy="${policy}" data-p13n-page-family="${family}"${reason ? ` data-p13n-static-only-reason="${reason}"` : ''}></span>`;

test('route inventory accepts one registered runtime and explicit static-only route', () => {
  const root = mkdtempSync(join(tmpdir(), 'p13n-inventory-'));
  try {
    page(root, 'segodnya', marker('today_primary', 'calendar-exact-only', 'today'));
    page(root, 'partnerstvo', marker('static_only', 'unknown-static', 'partnership', 'non-recommendation-content'));
    const result = buildPersonalizationRouteInventory(root);
    assert.deepEqual(personalizationInventoryFailures(result), []);
    assert.equal(result.counts.public_html_total, 2);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test('route inventory fails missing, duplicate, unknown collection and calendar rerank', () => {
  const root = mkdtempSync(join(tmpdir(), 'p13n-inventory-'));
  try {
    page(root, 'missing', '');
    page(root, 'podborki/a', `${marker('unknown', 'unknown-static', 'unknown')}${marker('unknown', 'unknown-static', 'unknown')}`);
    page(root, 'segodnya', marker('today_primary', 'thematic-weak', 'today'));
    const result = buildPersonalizationRouteInventory(root);
    assert.equal(result.counts.public_html_missing_runtime, 1);
    assert.equal(result.counts.public_html_duplicate_runtime, 1);
    assert.equal(result.counts.collections_unknown_surface, 1);
    assert.equal(result.counts.calendar_primary_non_identity_policy, 1);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test('route inventory documents isolated lab exclusions', () => {
  const root = mkdtempSync(join(tmpdir(), 'p13n-inventory-'));
  try {
    page(root, 'lab/example', '');
    const result = buildPersonalizationRouteInventory(root);
    assert.equal(result.counts.excluded_test_html, 1);
    assert.deepEqual(personalizationInventoryFailures(result), []);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});
