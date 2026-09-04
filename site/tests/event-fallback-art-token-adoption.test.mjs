import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const siteRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

test('EventFallbackArt consumes canonical event-detail fallback roles', async () => {
  const source = await readFile(path.join(siteRoot, 'src/components/EventFallbackArt.astro'), 'utf8');

  assert.match(source, /background:\s*var\(--ke-color-event-fallback-surface\)/u);
  assert.match(source, /min-height:\s*var\(--ke-event-fallback-mobile-min-height\)/u);
  assert.doesNotMatch(source, /background:\s*#181818/u);
  assert.doesNotMatch(source, /min-height:\s*clamp\(300px,\s*92vw,\s*560px\)/u);
});
