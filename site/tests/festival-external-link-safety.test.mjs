import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const source = await readFile(new URL('../src/pages/festivali/index.astro', import.meta.url), 'utf8');

test('festival external links bind both reverse-tabnabbing safety tokens', () => {
  assert.match(source, /target=\{item\.isExternal \? '_blank' : undefined\}/u);
  assert.match(source, /rel=\{item\.isExternal \? 'noopener noreferrer' : undefined\}/u);
  assert.doesNotMatch(source, /rel=\{item\.isExternal \? 'noreferrer' : undefined\}/u);
});
