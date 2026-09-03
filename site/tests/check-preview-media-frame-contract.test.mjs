import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const siteRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (relativePath) => readFile(path.join(siteRoot, relativePath), 'utf8');

test('preview gate verifies MediaFrame v1 diagnostics instead of retired rail variables', async () => {
  const gate = await read('scripts/check-preview.mjs');

  for (const marker of [
    'data-media-frame-fit="cover"',
    'data-media-frame-fit="contain"',
    'data-media-frame-crop-permission="reviewed"',
    'data-media-frame-crop-permission="forbidden"',
    'data-media-frame-focal-position="65% 35%"',
    'data-media-frame-style-owner="media-frame.css"',
  ]) assert.ok(gate.includes(marker), `preview gate misses MediaFrame assertion: ${marker}`);

  assert.doesNotMatch(gate, /--rail-media-fit:(?:cover|contain)/u);
  assert.doesNotMatch(gate, /--focus-[xy]:/u);
  assert.match(gate, /Current generated catalog has no visual-only 140x112 cover rail canary/u);
  assert.match(gate, /More vnutri 4211 OCR media must remain fail-closed/u);
});
