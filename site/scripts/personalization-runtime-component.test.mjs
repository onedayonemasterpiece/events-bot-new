import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import test from 'node:test';
import { transform } from '@astrojs/compiler';

const siteRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const componentPath = join(siteRoot, 'src/components/personalization/PersonalizationRuntime.astro');

test('P13N-00 runtime component compiles and remains inert/non-networking', async () => {
  const source = await readFile(componentPath, 'utf8');
  const compiled = await transform(source, { filename: componentPath });
  assert.ok(compiled.code.length > 0);
  assert.match(source, /data-p13n-runtime-marker="p13n-runtime-v1"/u);
  assert.match(source, /<script is:inline>/u);
  assert.doesNotMatch(source, /legacy\/scorer-v1/u);
  assert.doesNotMatch(source, /\bfetch\s*\(|XMLHttpRequest|sendBeacon|localStorage|sessionStorage|indexedDB/iu);
  assert.doesNotMatch(source, /data-p13n-target-algorithm/u);
});

test('production off mode omits the serialized test API payload', async () => {
  const source = await readFile(componentPath, 'utf8');
  assert.match(source, /mode\.mode === 'off' \? undefined/u);
  assert.match(source, /marker\.dataset\.p13nMode !== 'off'/u);
});
