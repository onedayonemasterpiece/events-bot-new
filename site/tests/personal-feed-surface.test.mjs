import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { readdir } from 'node:fs/promises';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const siteRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

async function read(relativePath) {
  return readFile(path.join(siteRoot, relativePath), 'utf8');
}

async function readBuilt(relativePath) {
  const distRoot = path.join(siteRoot, 'dist');
  const entries = await readdir(distRoot, { withFileTypes: true });
  const previewBuilds = entries
    .filter((entry) => entry.isDirectory() && entry.name.startsWith('preview-'))
    .map((entry) => entry.name)
    .sort()
    .reverse();
  const buildRoot = process.env.PREVIEW_BUILD_ID
    ? path.join(distRoot, process.env.PREVIEW_BUILD_ID)
    : previewBuilds.length > 0
      ? path.join(distRoot, previewBuilds[0])
      : distRoot;
  return readFile(path.join(buildRoot, relativePath), 'utf8');
}

test('personal feed component exposes a hidden reusable hydration surface', async () => {
  const source = await read('src/components/PersonalFeedSlot.astro');
  assert.match(source, /data-personal-feed-section/u);
  assert.match(source, /data-personal-feed-src/u);
  assert.match(source, /data-personal-feed-slot/u);
  assert.match(source, /data-personal-feed-status/u);
  assert.match(source, /data-personal-feed-load-more/u);
  assert.match(source, /aria-live="polite"/u);
  assert.match(source, /\bhidden\b/u);
  assert.match(source, /Для вас/u);
  assert.match(source, /По вашим интересам/u);
  assert.match(source, /repeat\(3, minmax\(0, 1fr\)\)/u);
  assert.match(source, /repeat\(2, minmax\(0, 1fr\)\)/u);
  assert.match(source, /grid-template-columns: minmax\(0, 1fr\)/u);
});

test('personal feed endpoint is a bounded static catalog without a backend RPC', async () => {
  const source = await read('src/pages/data/personal-feed.json.ts');
  assert.match(source, /const MAX_CANDIDATES = 500/u);
  assert.match(source, /eventIntersectsDateRange\(event, currentDate, '9999-12-31'\)/u);
  assert.doesNotMatch(source, /supabase|\/rpc\/|anon_id|session_id/iu);
});

test('built personal feed manifest is compact, public, and card-compatible', async () => {
  const payload = JSON.parse(await readBuilt('data/personal-feed.json'));
  assert.equal(payload.schema_version, 'listing-personal-feed-v1');
  assert.equal(payload.feature_schema_version, 'event-detail-related-v1');
  assert.equal(payload.surface, 'listing_personal_feed');
  assert.equal(payload.algorithm_id, 'static_personal_feed_catalog_v1');
  assert.ok(Array.isArray(payload.related_static));
  assert.ok(payload.related_static.length > 0);
  assert.ok(payload.related_static.length <= 500);

  const forbiddenKeys = new Set([
    'address',
    'description',
    'description_html',
    'email',
    'meta_description',
    'phone',
    'profile',
    'session_id',
    'source_url',
    'source_urls',
    'summary',
    'telegraph_url',
    'ticket',
    'user_id',
  ]);
  const visit = (value) => {
    if (!value || typeof value !== 'object') return;
    for (const [key, child] of Object.entries(value)) {
      assert.ok(!forbiddenKeys.has(key), `manifest must not expose ${key}`);
      visit(child);
    }
  };
  visit(payload);

  for (const candidate of payload.related_static) {
    assert.equal(candidate.lifecycle_status, 'active');
    assert.ok((candidate.date || '') <= '9999-12-31');
    assert.ok(candidate.date >= payload.current_date || candidate.reason_codes.includes('catalog:ongoing'));
    for (const key of ['event_id', 'title', 'category', 'tags', 'date', 'status', 'display']) {
      assert.ok(Object.hasOwn(candidate, key), `candidate is missing ${key}`);
    }
    for (const key of ['href', 'absolute_url', 'display_date', 'display_date_time', 'calendar_href']) {
      assert.ok(Object.hasOwn(candidate.display, key), `candidate display is missing ${key}`);
    }
  }

  assert.ok(Buffer.byteLength(JSON.stringify(payload)) < 2_000_000, 'manifest should stay below 2 MB');
});
