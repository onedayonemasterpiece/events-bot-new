import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const siteRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

test('For me page title consumes the canonical page-title role', async () => {
  const source = await readFile(path.join(siteRoot, 'src/pages/dlya-menya/index.astro'), 'utf8');

  assert.match(source, /\.personal-page__head h1\s*\{[^}]*font:\s*var\(--ke-type-h1\);[^}]*letter-spacing:\s*var\(--ke-type-h1-letter\)/u);
  assert.doesNotMatch(source, /\.personal-page__head h1\s*\{[^}]*font-size:\s*clamp\(/u);
});

test('For me account heading remains its compact functional role', async () => {
  const source = await readFile(path.join(siteRoot, 'src/pages/dlya-menya/index.astro'), 'utf8');

  assert.match(source, /\.personal-page__account h2\s*\{[^}]*font-size:\s*clamp\(1\.45rem,\s*3vw,\s*2rem\)/u);
  assert.doesNotMatch(source, /\.personal-page__account h2\s*\{[^}]*font:\s*var\(--ke-type-h2\)/u);
});
