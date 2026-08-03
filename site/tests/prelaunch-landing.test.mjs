import assert from 'node:assert/strict';
import { mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import test from 'node:test';
import { classifyBackendOperation, policyForOperation } from '../src/lib/backendOperationCatalog.ts';
import {
  PRELAUNCH_ROBOTS_DIRECTIVE,
  applyPrelaunchArtifactPolicy,
  assertPrelaunchArtifactPolicy,
  resolvePrelaunchMode,
} from '../scripts/prelaunch-build-contract.mjs';

const siteDir = dirname(dirname(fileURLToPath(import.meta.url)));
const repoRoot = resolve(siteDir, '..');

function source(path) {
  return readFileSync(join(repoRoot, path), 'utf8');
}

test('prelaunch production mode is explicit and defaults on until launch release', () => {
  assert.equal(resolvePrelaunchMode({}), true);
  assert.equal(resolvePrelaunchMode({ PUBLIC_PRELAUNCH_MODE: 'on' }), true);
  assert.equal(resolvePrelaunchMode({ PUBLIC_PRELAUNCH_MODE: '1' }), true);
  assert.equal(resolvePrelaunchMode({ PUBLIC_PRELAUNCH_MODE: 'off' }), false);
  assert.throws(
    () => resolvePrelaunchMode({ PUBLIC_PRELAUNCH_MODE: 'maybe' }),
    /PUBLIC_PRELAUNCH_MODE/u,
  );
});

test('artifact policy indexes only the root and hides every other HTML page', () => {
  const root = mkdtempSync(join(tmpdir(), 'kenigevents-prelaunch-'));
  try {
    mkdirSync(join(root, 'sobytiya', 'test'), { recursive: true });
    writeFileSync(
      join(root, 'index.html'),
      '<!doctype html><html><head><meta name="robots" content="noindex,nofollow,noarchive"></head><body><main data-prelaunch-page></main></body></html>',
    );
    writeFileSync(
      join(root, 'sobytiya', 'test', 'index.html'),
      '<!doctype html><html><head><meta name="robots" content="index,follow"></head><body></body></html>',
    );
    writeFileSync(join(root, 'robots.txt'), 'old');
    writeFileSync(join(root, 'sitemap.xml'), 'old');

    const receipt = applyPrelaunchArtifactPolicy(root, {
      enabled: true,
      siteOrigin: 'https://kenigevents.ru',
    });
    assert.deepEqual(receipt, { enabled: true, htmlCount: 2, hiddenHtmlCount: 1 });
    assert.match(readFileSync(join(root, 'index.html'), 'utf8'), /content="index,follow"/u);
    assert.match(
      readFileSync(join(root, 'sobytiya', 'test', 'index.html'), 'utf8'),
      new RegExp(`content="${PRELAUNCH_ROBOTS_DIRECTIVE}"`, 'u'),
    );
    assert.equal(
      readFileSync(join(root, 'robots.txt'), 'utf8'),
      'User-agent: *\nAllow: /$\nDisallow: /\nSitemap: https://kenigevents.ru/sitemap.xml\n',
    );
    assert.deepEqual(assertPrelaunchArtifactPolicy(root, { siteOrigin: 'https://kenigevents.ru' }), {
      htmlCount: 2,
      hiddenHtmlCount: 1,
    });
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test('landing source uses the approved brand asset and purpose-limited resilient signup RPC', () => {
  const landing = source('site/src/components/PrelaunchLanding.astro');
  const layout = source('site/src/layouts/PrelaunchLayout.astro');
  const catalog = source('site/src/lib/backendOperationCatalog.ts');

  assert.match(landing, /announcements-brand-v2-512\.png/u);
  assert.match(landing, /1 сентября 2026/u);
  assert.match(landing, /персонализированный сервис анонсов/u);
  assert.match(landing, /register_prelaunch_notification_v1/u);
  assert.match(landing, /getResilientDataClient/u);
  assert.match(landing, /launch-2026-09-01-v1/u);
  assert.match(landing, /prefers-reduced-motion/u);
  assert.match(layout, /content=\{robots\}/u);
  assert.match(catalog, /'register_prelaunch_notification_v1'/u);
});

test('prelaunch notification RPC is classified as idempotent replay', () => {
  const operation = classifyBackendOperation(
    'https://project.supabase.co/rest/v1/rpc/register_prelaunch_notification_v1',
    { method: 'POST' },
  );
  assert.equal(operation.capability, 'data');
  assert.equal(operation.semantics, 'idempotent-replay');
  assert.equal(policyForOperation(operation), 'idempotent-replay');
});

test('migration stores email in a separate RLS-protected table without public reads', () => {
  const migration = source('supabase/migrations/20260803113000_prelaunch_launch_notifications_v1.sql');
  assert.match(migration, /create table if not exists personalization\.prelaunch_launch_subscription/u);
  assert.match(migration, /enable row level security/u);
  assert.match(migration, /revoke all on personalization\.prelaunch_launch_subscription from public, anon, authenticated/u);
  assert.match(migration, /security definer/u);
  assert.match(migration, /register_prelaunch_notification_v1/u);
  assert.match(migration, /grant execute[^;]+to anon, authenticated, service_role/su);
  assert.doesNotMatch(migration, /grant\s+select[^;]+\b(?:anon|authenticated)\b/iu);
});
