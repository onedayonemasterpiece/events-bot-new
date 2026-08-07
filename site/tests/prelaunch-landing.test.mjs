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
  assert.throws(() => resolvePrelaunchMode({ PUBLIC_PRELAUNCH_MODE: 'maybe' }), /PUBLIC_PRELAUNCH_MODE/u);
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

    const receipt = applyPrelaunchArtifactPolicy(root, { enabled: true, siteOrigin: 'https://kenigevents.ru' });
    assert.deepEqual(receipt, { enabled: true, htmlCount: 2, hiddenHtmlCount: 1 });
    assert.match(readFileSync(join(root, 'index.html'), 'utf8'), /content="index,follow"/u);
    assert.match(
      readFileSync(join(root, 'sobytiya', 'test', 'index.html'), 'utf8'),
      new RegExp(`content="${PRELAUNCH_ROBOTS_DIRECTIVE}"`, 'u'),
    );
    assert.equal(
      readFileSync(join(root, 'robots.txt'), 'utf8'),
      'User-agent: *\nAllow: /$\nAllow: /_astro/\nAllow: /assets/\nAllow: /sitemap.xml\nDisallow: /\nSitemap: https://kenigevents.ru/sitemap.xml\n',
    );
    assert.deepEqual(assertPrelaunchArtifactPolicy(root, { siteOrigin: 'https://kenigevents.ru' }), {
      htmlCount: 2,
      hiddenHtmlCount: 1,
    });
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test('landing keeps the launch, form, SEO and canonical scene contracts', () => {
  const page = source('site/src/components/PrelaunchPage.astro');
  const index = source('site/src/pages/index.astro');
  const layout = source('site/src/layouts/PrelaunchLayout.astro');
  const visual = source('site/src/styles/prelaunch-page.css');

  assert.match(page, /Запуск/u);
  assert.match(page, /1 сентября/u);
  assert.match(page, /Персонализированный сервис анонсов/u);
  assert.match(page, /data-prelaunch-form/u);
  assert.match(page, /data-prelaunch-mosaic/u);
  assert.match(page, /data-prelaunch-seams/u);
  assert.match(index, /PUBLIC_PRELAUNCH_MODE/u);
  assert.match(index, /<PrelaunchPage\s*\/>/u);

  assert.match(visual, /--prelaunch-artwork-size/u);
  assert.match(visual, /--prelaunch-tile-size/u);
  assert.match(visual, /\.prelaunch__seams/u);
  assert.match(visual, /backdrop-filter/u);
  assert.match(visual, /prefers-reduced-motion/u);

  assert.match(layout, /content=\{robots\}/u);
  assert.match(layout, /noindex,nofollow,noarchive,nosnippet/u);
  assert.match(layout, /name="referrer" content="no-referrer"/u);
  assert.match(layout, /application\/ld\+json/u);
});

test('secret candidate inherits the checked production prelaunch surface', () => {
  const build = source('site/scripts/build-secret-candidate.mjs');
  const check = source('site/scripts/check-secret-candidate.mjs');
  const layout = source('site/src/layouts/PrelaunchLayout.astro');

  assert.match(build, /productionManifest\.prelaunch_mode/u);
  assert.match(build, /PUBLIC_PRELAUNCH_MODE:\s*prelaunchMode \? 'on' : 'off'/u);
  assert.match(build, /prelaunch_mode:\s*prelaunchMode/u);
  assert.match(check, /data-prelaunch-page/u);
  assert.match(check, /candidate public surface disagrees with prelaunch mode/u);
  assert.match(layout, /noindex,nofollow,noarchive,nosnippet/u);
  assert.match(layout, /name="referrer" content="no-referrer"/u);
});

test('prelaunch notification RPC is classified as idempotent replay over both routes', () => {
  const operation = classifyBackendOperation(
    'https://project.supabase.co/rest/v1/rpc/register_prelaunch_notification_v1',
    { method: 'POST' },
  );
  assert.equal(operation.capability, 'data');
  assert.equal(operation.semantics, 'idempotent-replay');
  assert.deepEqual(operation.routeSupport, ['direct', 'relay']);
  assert.equal(policyForOperation(operation), 'idempotent-replay');
});

test('migrations keep table reads closed and align consent and email validation', () => {
  const v1 = source('supabase/migrations/20260803113000_prelaunch_launch_notifications_v1.sql');
  const v2 = source('supabase/migrations/20260806163000_prelaunch_updates_consent_v2.sql');
  assert.match(v1, /create table if not exists personalization\.prelaunch_launch_subscription/u);
  assert.match(v1, /enable row level security/u);
  assert.match(v1, /revoke all on personalization\.prelaunch_launch_subscription from public, anon, authenticated/u);
  assert.doesNotMatch(v1, /grant\s+select[^;]+\b(?:anon|authenticated)\b/iu);

  assert.match(v2, /prelaunch-updates-2026-v1/u);
  assert.match(v2, /prelaunch_email_is_valid_v2/u);
  assert.match(v2, /security definer/u);
  assert.match(v2, /interval '24 months'/u);
  assert.match(v2, /Direct and Yandex-relay replays converge/u);
  assert.match(v2, /grant execute[^;]+to anon, authenticated, service_role/su);
  assert.doesNotMatch(v2, /execute\s+format\s*\(/iu);
});
