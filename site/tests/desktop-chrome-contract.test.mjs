import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const siteRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (relativePath, encoding = 'utf8') => readFile(path.join(siteRoot, relativePath), encoding);

test('desktop event breadcrumbs sit over the photograph while preserving semantic hierarchy', async () => {
  const page = await read('src/components/DesktopEventPage.astro');
  const breadcrumbs = await read('src/components/Breadcrumbs.astro');

  assert.match(page, /candidate === 'editorial'[\s\S]*className="desktop-prototype__breadcrumbs desktop-prototype__breadcrumbs--overlay"/u);
  assert.match(page, /--desktop-sheet-top:clamp\(430px,67svh,720px\)/u);
  assert.match(page, /top:calc\(var\(--desktop-sheet-top\) - 31px\)/u);
  assert.match(page, /background:radial-gradient\([\s\S]*filter:blur\(9px\)/u);
  assert.match(page, /border-radius:28px 28px 0 0/u);
  assert.match(breadcrumbs, /<nav[\s\S]*aria-label="Хлебные крошки"/u);
  assert.match(breadcrumbs, /<ol>/u);
  assert.match(breadcrumbs, /<li aria-current="page">/u);
});

test('desktop leather tag has a deterministic WebP and an immediate solid-colour fallback', async () => {
  const layout = await read('src/layouts/EventLayout.astro');
  const metadata = JSON.parse(await read('public/assets/ui/desktop-head-leather-r5.metadata.json'));
  const asset = await read('public/assets/ui/desktop-head-leather-r5.webp', null);
  const hash = createHash('sha256').update(asset).digest('hex');

  assert.deepEqual(metadata.output_dimensions_px, [960, 352]);
  assert.equal(metadata.output_aspect_ratio, '30:11');
  assert.equal(metadata.chosen_source, 'docs/features/static-site-pages/references/head-skin-desctop (2).png');
  assert.equal(hash, metadata.output_sha256);
  assert.match(metadata.cleanup_policy, /stitched outer edge[\s\S]*offset dark backing/u);
  assert.match(layout, /--desktop-brand-tag-skin:url\('\$\{withBase\('\/assets\/ui\/desktop-head-leather-r5\.webp'\)\}'\)/u);
  assert.match(layout, /\.site-header__brand-tag,\s*\.hero-gallery__brand\s*\{[\s\S]*background-color:#98401f;[\s\S]*background-image:/u);
  assert.match(layout, /border:1px solid transparent/u);
  assert.match(layout, /@media \(min-width:\s*1024px\)/u);
});
