import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { test } from 'node:test';

test('immutable preview uses its Kaliningrad build clock instead of stale snapshot metadata', async () => {
  const [builder, events] = await Promise.all([
    readFile(new URL('../scripts/build-preview.mjs', import.meta.url), 'utf8'),
    readFile(new URL('../src/lib/events.ts', import.meta.url), 'utf8'),
  ]);

  assert.match(builder, /timeZone:\s*'Europe\/Kaliningrad'/u);
  assert.match(builder, /PUBLIC_STATIC_SITE_CURRENT_DATE:\s*effectiveCurrentDate/u);
  assert.match(builder, /PUBLIC_STATIC_SITE_REFERENCE_ISO:\s*effectiveReferenceIso/u);
  assert.match(events, /PUBLIC_STATIC_SITE_CURRENT_DATE/u);
  assert.match(events, /PUBLIC_STATIC_SITE_REFERENCE_ISO/u);
  assert.match(events, /return getPreviewBuild\(\)\.current_date/u);
});
