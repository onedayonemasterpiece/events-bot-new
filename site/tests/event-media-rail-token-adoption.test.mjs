import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const siteRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (relativePath) => readFile(path.join(siteRoot, relativePath), 'utf8');

test('EventMediaRail consumes the existing gallery, resolved, poster and role tokens', async () => {
  const source = await read('src/components/EventMediaRail.astro');

  for (const token of [
    '--ke-color-media-rail-gallery-surface',
    '--ke-color-media-rail-resolved-surface',
    '--ke-color-media-rail-gallery-item-surface',
    '--ke-color-media-rail-resolved-item-surface',
    '--ke-color-media-rail-inverse-text',
    '--ke-color-media-rail-current-border',
    '--ke-color-media-rail-shell-border',
    '--ke-color-media-rail-item-border',
    '--ke-color-media-rail-control-border',
    '--ke-color-media-rail-role-surface',
    '--ke-color-media-rail-role-text',
    '--ke-color-media-rail-more-label',
    '--ke-elevation-media-rail-gallery',
    '--ke-media-rail-gallery-gap',
    '--ke-media-rail-gallery-padding',
    '--ke-media-rail-gallery-radius',
    '--ke-media-rail-gallery-item-height',
    '--ke-media-rail-gallery-item-radius',
    '--ke-media-rail-resolved-gap',
    '--ke-media-rail-resolved-padding',
    '--ke-media-rail-resolved-radius',
    '--ke-media-rail-resolved-item-radius',
    '--ke-media-rail-hero-item-min-width',
    '--ke-media-rail-hero-item-max-width',
    '--ke-media-rail-hero-item-height',
    '--ke-media-rail-hero-more-count-size',
    '--ke-media-rail-resolved-more-label-size',
    '--ke-media-rail-role-radius',
    '--ke-media-rail-role-size',
    '--ke-media-rail-poster-thumb-height',
    '--ke-media-rail-poster-shell-extra-height',
    '--ke-media-rail-poster-gap',
    '--ke-media-rail-poster-padding',
    '--ke-media-rail-poster-item-width',
    '--ke-media-rail-poster-item-min-width',
    '--ke-media-rail-poster-item-max-width',
    '--ke-media-rail-poster-more-width',
  ]) {
    assert.match(source, new RegExp(`var\\(${token.replace(/[.*+?^${}()|[\\]\\]/g, '\\$&')}\\)`), `${token} must be consumed`);
  }

  assert.doesNotMatch(source, /background:\s*rgba\(31,\s*27,\s*24,/u);
  assert.doesNotMatch(source, /background:\s*#(?:2d2926|2b2724)/u);
  assert.doesNotMatch(source, /border-radius:\s*(?:18|16|11|10|6)px/u);
  assert.doesNotMatch(source, /--split-rail-thumb-height:\s*clamp\(104px,\s*13svh,\s*148px\)/u);
});
