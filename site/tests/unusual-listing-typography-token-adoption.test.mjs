import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const siteRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

test('Unusual listing section headings consume the canonical section heading role', async () => {
  const source = await readFile(path.join(siteRoot, 'src/components/UnusualListingSurface.astro'), 'utf8');

  assert.match(source, /\.unusual-page__feed-head h2,\.unusual-page__empty h2\s*\{[^}]*font:var\(--ke-type-h2\)/u);
  assert.doesNotMatch(source, /\.unusual-page__feed-head h2,\.unusual-page__empty h2\s*\{[^}]*font-size:clamp\(1\.6rem,3\.5vw,2\.8rem\)/u);
});
