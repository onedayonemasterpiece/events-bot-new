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
  assert.match(page, /top:calc\(var\(--desktop-sheet-top\) - 48px\)/u);
  assert.match(page, /background:radial-gradient\([\s\S]*filter:blur\(9px\)/u);
  assert.match(page, /border-radius:28px 28px 0 0/u);
  assert.match(breadcrumbs, /<nav[\s\S]*aria-label="Хлебные крошки"/u);
  assert.match(breadcrumbs, /<ol>/u);
  assert.match(breadcrumbs, /<li aria-current="page">/u);
});

test('desktop leather tag has a deterministic WebP and an immediate solid-colour fallback', async () => {
  const layout = await read('src/layouts/EventLayout.astro');
  const metadata = JSON.parse(await read('public/assets/ui/desktop-head-leather.metadata.json'));
  const asset = await read('public/assets/ui/desktop-head-leather.webp', null);
  const hash = createHash('sha256').update(asset).digest('hex');

  assert.deepEqual(metadata.output_dimensions_px, [960, 352]);
  assert.equal(metadata.output_aspect_ratio, '30:11');
  assert.deepEqual(metadata.crop_box_px, [180, 620, 1236, 1007]);
  assert.equal(metadata.source_sha256, '31e96796af745b6c080b9159f267f804c03e943f4ae22cb5e099e4b69d94d907');
  assert.equal(hash, metadata.output_sha256);
  assert.match(metadata.crop_policy, /excludes the stitched panel border/u);
  assert.match(layout, /--desktop-brand-tag-skin:url\('\$\{withBase\('\/assets\/ui\/desktop-head-leather\.webp'\)\}'\)/u);
  assert.match(layout, /\.site-header__brand-tag,\s*\.hero-gallery__brand\s*\{[\s\S]*background-color:\s*var\(--ke-color-brand-tag\);[\s\S]*background-image:/u);
  assert.match(layout, /@media \(min-width:\s*1024px\)/u);
});
