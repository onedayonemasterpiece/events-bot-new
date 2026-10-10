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

test('deterministic browser CI clock stays bound to the checked-in preview corpus', async () => {
  const [workflow, fixtureText] = await Promise.all([
    readFile(new URL('../../.github/workflows/ci.yaml', import.meta.url), 'utf8'),
    readFile(new URL('../src/data/preview-events.json', import.meta.url), 'utf8'),
  ]);
  const fixture = JSON.parse(fixtureText);
  const step = workflow.split('      - name: Build real static event pages\n')[1]?.split('\n      - name:')[0] || '';
  const date = /^\s+STATIC_SITE_CURRENT_DATE:\s*'([^']+)'\s*$/mu.exec(step)?.[1];
  const instant = /^\s+STATIC_SITE_CURRENT_DATETIME:\s*'([^']+)'\s*$/mu.exec(step)?.[1];
  assert.match(String(fixture.build.current_date), /^\d{4}-\d{2}-\d{2}$/u);
  assert.ok(Number.isFinite(Date.parse(fixture.build.generated_at)));
  assert.equal(date, fixture.build.current_date);
  assert.equal(instant, fixture.build.generated_at);
  assert.match(step, /run: npm run build:preview/u);
});
