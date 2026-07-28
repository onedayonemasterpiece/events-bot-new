import assert from 'node:assert/strict';
import { mkdtempSync, mkdirSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';

import {
  loadPreviewPublicConfig,
  requirePreviewAuthorizedSearch,
} from '../scripts/preview-public-env.mjs';

test('preview build maps only browser-safe Authorized Search values from an explicit env file', () => {
  const root = mkdtempSync(join(tmpdir(), 'ke-preview-env-'));
  const siteDir = join(root, 'site');
  mkdirSync(siteDir);
  const envFile = join(root, 'preview.env');
  writeFileSync(envFile, [
    'PERSONALIZATION_SUPABASE_URL=https://project.supabase.co',
    'PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY=sb_publishable_example',
    'PERSONALIZATION_SUPABASE_SECRET_KEY=must-never-be-forwarded',
    'YANDEX_CLIENT_SECRET=must-never-be-forwarded',
    'STATIC_SITE_PUBLIC_YANDEX_AUTH_PROVIDER=custom:yandex',
    'STATIC_SITE_PUBLIC_AUTHORIZED_SEARCH_TRANSPORT=json',
  ].join('\n'));
  try {
    const config = loadPreviewPublicConfig(siteDir, {
      STATIC_SITE_PREVIEW_ENV_FILE: envFile,
    });
    assert.equal(config.configured, true);
    assert.deepEqual(config.values, {
      PUBLIC_PERSONALIZATION_SUPABASE_URL: 'https://project.supabase.co',
      PUBLIC_PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY: 'sb_publishable_example',
      PUBLIC_YANDEX_AUTH_PROVIDER: 'custom:yandex',
      PUBLIC_AUTHORIZED_SEARCH_TRANSPORT: 'json',
    });
    assert.equal('PERSONALIZATION_SUPABASE_SECRET_KEY' in config.values, false);
    assert.equal('YANDEX_CLIENT_SECRET' in config.values, false);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test('runtime public values override dotenv aliases', () => {
  const root = mkdtempSync(join(tmpdir(), 'ke-preview-env-'));
  const siteDir = join(root, 'site');
  mkdirSync(siteDir);
  const envFile = join(root, 'preview.env');
  writeFileSync(envFile, [
    'PERSONALIZATION_SUPABASE_URL=https://file.supabase.co',
    'PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY=sb_publishable_file',
  ].join('\n'));
  try {
    const config = loadPreviewPublicConfig(siteDir, {
      STATIC_SITE_PREVIEW_ENV_FILE: envFile,
      PUBLIC_PERSONALIZATION_SUPABASE_URL: 'https://runtime.supabase.co',
      PUBLIC_PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY: 'sb_publishable_runtime',
    });
    assert.equal(config.values.PUBLIC_PERSONALIZATION_SUPABASE_URL, 'https://runtime.supabase.co');
    assert.equal(config.values.PUBLIC_PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY, 'sb_publishable_runtime');
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test('strict preview gate fails before publishing a dead Search form', () => {
  const config = { configured: false, values: {} };
  assert.throws(
    () => requirePreviewAuthorizedSearch(config, { PREVIEW_REQUIRE_AUTHORIZED_SEARCH: '1' }),
    /Authorized Search is required/u,
  );
  assert.doesNotThrow(() => requirePreviewAuthorizedSearch(config, {}));
});

test('preview build rejects an unknown Authorized Search transport', () => {
  const root = mkdtempSync(join(tmpdir(), 'ke-preview-env-'));
  const siteDir = join(root, 'site');
  mkdirSync(siteDir);
  const envFile = join(root, 'preview.env');
  writeFileSync(envFile, [
    'PERSONALIZATION_SUPABASE_URL=https://project.supabase.co',
    'PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY=sb_publishable_example',
    'STATIC_SITE_PUBLIC_AUTHORIZED_SEARCH_TRANSPORT=websocket',
  ].join('\n'));
  try {
    assert.throws(
      () => loadPreviewPublicConfig(siteDir, { STATIC_SITE_PREVIEW_ENV_FILE: envFile }),
      /must be json or ndjson/u,
    );
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});
