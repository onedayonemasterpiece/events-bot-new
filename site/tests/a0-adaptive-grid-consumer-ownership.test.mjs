import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const read = (path) => readFile(new URL(`../${path}`, import.meta.url), 'utf8');
const diagnosticWrite = /\.dataset\.adaptiveGrid(?:Input|Source|Rendered|Remainder|Runtime|Base)[A-Za-z]*\s*=/u;

test('PersonalFeed observes Adaptive diagnostics without becoming a second writer', async () => {
  const source = await read('src/components/PersonalFeedSlot.astro');

  assert.match(source, /<AdaptiveEventCardGrid[\s\S]*\bruntimeManaged\b/u);
  assert.match(source, /runtimeSourcePolicy="mirror-rendered"/u);
  assert.match(source, /data-adaptive-grid-rendered-count/u);
  assert.match(source, /syncSectionState/u);
  assert.doesNotMatch(source, diagnosticWrite);
  assert.doesNotMatch(source, /const PERSONAL_FEED_ROW_SIZE|syncDiagnostics/u);
});

test('Home delegates visibility-aware count, order and remainder diagnostics', async () => {
  const source = await read('src/components/HomeColdStartFeed.astro');

  assert.match(source, /<AdaptiveEventCardGrid[\s\S]*\bruntimeManaged\b/u);
  assert.match(source, /\bruntimeVisibleOnly\b/u);
  assert.match(source, /runtimeSourcePolicy="all-direct"/u);
  assert.doesNotMatch(source, diagnosticWrite);
  assert.doesNotMatch(source, /adaptiveGridRenderedOrder|adaptiveGridRenderedCount|adaptiveGridRemainderCount/u);
});

test('Favorites delegates post-reconciliation diagnostics while retaining product state', async () => {
  const source = await read('src/components/FavoritesSurface.astro');

  assert.match(source, /<AdaptiveEventCardGrid[\s\S]*\bruntimeManaged\b/u);
  assert.match(source, /runtimeSourcePolicy="mirror-rendered"/u);
  assert.match(source, /let rendered = 0/u);
  assert.match(source, /rendered \+= 1/u);
  assert.doesNotMatch(source, diagnosticWrite);
  assert.doesNotMatch(source, /syncAdaptiveDiagnostics/u);
});
