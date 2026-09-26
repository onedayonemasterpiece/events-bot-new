import assert from 'node:assert/strict';
import { mkdtempSync, mkdirSync, rmSync, writeFileSync } from 'node:fs';
import { readFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';

import { loadPreviewPublicConfig } from '../scripts/preview-public-env.mjs';

test('preview public config carries only the browser-safe Live Search collection URL', () => {
  const root = mkdtempSync(join(tmpdir(), 'ke-live-preview-env-'));
  const siteDir = join(root, 'site');
  mkdirSync(siteDir);
  const envFile = join(root, 'preview.env');
  writeFileSync(envFile, [
    'PERSONALIZATION_SUPABASE_URL=https://project.supabase.co',
    'PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY=sb_publishable_example',
    'STATIC_SITE_PUBLIC_LIVE_SEARCH_URL=https://events-bot-new-wngqia.fly.dev/api/live-search',
    'GOOGLE_API_KEY=must-never-be-forwarded',
    'GOOGLE_AI_LIMITER_SUPABASE_SERVICE_KEY=must-never-be-forwarded',
  ].join('\n'));
  try {
    const config = loadPreviewPublicConfig(siteDir, { STATIC_SITE_PREVIEW_ENV_FILE: envFile });
    assert.equal(
      config.values.PUBLIC_STATIC_SITE_LIVE_SEARCH_URL,
      'https://events-bot-new-wngqia.fly.dev/api/live-search',
    );
    assert.equal('GOOGLE_API_KEY' in config.values, false);
    assert.equal('GOOGLE_AI_LIMITER_SUPABASE_SERVICE_KEY' in config.values, false);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test('daily Live Search canary is low-frequency and exercises the real Live function-call route', async () => {
  const workflow = await readFile(new URL('../../.github/workflows/live-search-daily-canary.yml', import.meta.url), 'utf8');
  const script = await readFile(new URL('../../.github/scripts/run-live-search-daily-canary.mjs', import.meta.url), 'utf8');
  assert.match(workflow, /cron:\s*['"]37 3 \* \* \*['"]/u);
  assert.doesNotMatch(workflow, /cron:[^\n]*(?:\*\/|,)/u);
  assert.match(workflow, /id-token:\s*write/u);
  assert.match(workflow, /run-live-search-daily-canary\.mjs/u);
  assert.match(script, /\/api\/live-search/u);
  assert.match(script, /search_events/u);
  assert.match(script, /search_results/u);
  assert.match(script, /session_released:\s*true/u);
  assert.doesNotMatch(script, /GOOGLE_API_KEY|LIVE_API_KEY/u);
});

test('Live search publication path is explicit and keeps the direct search adapter as compatibility-only', async () => {
  const component = await readFile(new URL('../src/components/AuthorizedEventSearch.astro', import.meta.url), 'utf8');
  const runner = await readFile(new URL('../../scripts/run_static_site_builder_kaggle.py', import.meta.url), 'utf8');
  const builder = await readFile(new URL('../../kaggle/StaticSiteBuilder/static_site_builder.py', import.meta.url), 'utf8');
  assert.match(component, /data-search-live-url/u);
  assert.match(component, /if \(liveEnabled\) await runLiveSearch/u);
  assert.match(component, /else await runSearch/u);
  assert.doesNotMatch(component, /catch[\s\S]{0,300}runSearch\(\{ append:/u);
  assert.match(runner, /public_static_site_live_search_url/u);
  assert.match(builder, /PUBLIC_STATIC_SITE_LIVE_SEARCH_URL/u);
});
