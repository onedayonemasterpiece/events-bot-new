import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const siteRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (relativePath, encoding = 'utf8') => readFile(path.join(siteRoot, relativePath), encoding);

test('full mobile event shell keeps the accepted stitched leather tag and an immediate terracotta fallback', async () => {
  const route = await read('src/pages/sobytiya/[slug].astro');
  const menu = await read('src/components/Reference4MobileMenu.astro');
  const metadata = JSON.parse(await read('public/assets/ui/mobile-head-skinny-leather-3x.metadata.json'));
  const asset = await read('public/assets/ui/mobile-head-skinny-leather-3x.webp', null);
  const hash = createHash('sha256').update(asset).digest('hex');

  assert.match(route, /<EventLayout[\s\S]*heroChrome="immersive"/u);
  assert.match(route, /data-mobile-event-production/u);
  assert.equal(metadata.source, 'docs/features/static-site-pages/references/mobile-head-skinny.png');
  assert.deepEqual(metadata.output_size, [360, 252]);
  assert.equal(hash, 'a57f81e60f438ccf003f0a7570468e67fd31d29d5f7063599f7d1bb235d52946');
  assert.match(menu, /--reference4-tag:url\('\$\{withBase\('\/assets\/ui\/mobile-head-skinny-leather-3x\.webp'\)\}'\)/u);
  assert.match(menu, /mobile-discovery-menu--reference4 > \.mobile-discovery-menu__summary \{[\s\S]*background-color:#98401f!important;[\s\S]*background-image:var\(--reference4-tag\)!important;/u);
  assert.match(menu, /background-position:center!important;[\s\S]*background-size:100% 100%!important;[\s\S]*background-repeat:no-repeat!important;/u);
  assert.doesNotMatch(menu, /background:transparent var\(--reference4-tag\)/u);
});

test('mobile event-detail medallions share one bounded identity scale without changing Main and Secondary semantics', async () => {
  const styles = await read('src/components/MobileEventProductionStyles.astro');
  const medallions = await read('src/components/EventTokenMedallions.astro');
  const route = await read('src/pages/sobytiya/[slug].astro');

  assert.match(route, /mobile-event-production__medallions"><EventTokenMedallions event=\{event\} \/>/u);
  assert.match(styles, /\.mobile-event-production__medallions \.event-token:not\(\.event-token--pill\) \{[\s\S]*--token-size: clamp\(84px, 23vw, 92px\);/u);
  assert.match(styles, /\.mobile-event-production__medallions \.event-token-row \{[\s\S]*max-width: 100%;[\s\S]*gap: clamp\(0\.55rem, 2\.4vw, 0\.7rem\);/u);
  assert.match(styles, /\.event-token__circle img,[\s\S]*\.event-token__pushkin-composite \{[\s\S]*max-width: 100%;/u);

  assert.match(medallions, /data-medallion-role=\{token\.layoutRole \|\| 'secondary'\}/u);
  assert.match(medallions, /const layoutRole = identityLayout\.main\?\.item\.slug === organizer\.slug \? 'main' : 'secondary'/u);
  assert.match(medallions, /key: 'free-admission'[\s\S]*layoutRole:'secondary'/u);
  assert.match(medallions, /imageUrl: '\/assets\/badges\/free-listing-medallion\.svg'/u);
});

test('mobile OCR and unknown poster heroes stay fully readable without crop parallax', async () => {
  const [styles, hero, route] = await Promise.all([
    read('src/components/MobileEventProductionStyles.astro'),
    read('src/components/EventHero.astro'),
    read('src/pages/sobytiya/[slug].astro'),
  ]);

  assert.match(hero, /primaryImageTextMode === 'visual_only'[\s\S]*fit:'cover'[\s\S]*fit:'contain'/u);
  assert.match(hero, /data-hero-image-text-mode=\{primaryImageTextMode\}/u);
  assert.match(styles, /\.event-hero--poster-stage \.event-hero__visual \{[\s\S]*overflow: visible;/u);
  assert.match(styles, /\.event-hero--poster-stage \.event-hero__image \{[\s\S]*margin-top: 0;[\s\S]*margin-bottom: 0;[\s\S]*transform: none;/u);
  assert.match(styles, /\[data-mobile-parallax-profile="photo-continuous-crop"\] \.event-hero--poster-stage \.event-hero__image \{\s*margin-top: 0;/u);
  assert.doesNotMatch(styles, /\.event-hero--poster-stage \.event-hero__image \{[\s\S]{0,180}var\(--hero-poster-parallax-y/u);
  assert.match(route, /data-mobile-parallax-profile="photo-continuous-crop"/u);
});
