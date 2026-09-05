import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const siteRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

async function read(relativePath) {
  return readFile(path.join(siteRoot, relativePath), 'utf8');
}

test('shared gallery keeps interactive pointer activation separate from swipe capture', async () => {
  const layout = await read('src/layouts/EventLayout.astro');

  assert.match(layout, /const isInteractiveGalleryTarget = \(target\).*target\.closest\('a,button,input,select,textarea,[^']*\.hero-gallery__slide--cta[^']*\[data-gallery-keep-open\]'/su);
  assert.match(layout, /if \(isInteractiveGalleryTarget\(event\.target\)\) \{\s*resetSwipe\(\);\s*return;\s*\}\s*swipeStart\(event\.clientX, event\.clientY\)/su);
  assert.match(layout, /if \(isInteractiveGalleryTarget\(event\.target\)\) \{\s*resetSwipe\(\);\s*return;\s*\}\s*const touch/su);
  assert.match(layout, /if \(tracking && event\.cancelable\) event\.preventDefault\(\)/u);
  assert.match(layout, /swipeSurface\.setPointerCapture/u);
  assert.match(layout, /gallery\.dataset\.desktopGalleryDismiss !== 'true'/u);
  assert.match(layout, /target\?\.closest\('a,button,\.hero-gallery__slide--cta,\[data-gallery-keep-open\]'\)/u);
});

test('gallery reduced motion, inactive controls and dialog focus are hardened', async () => {
  const layout = await read('src/layouts/EventLayout.astro');
  const mobileHero = await read('src/components/EventHero.astro');
  const desktop = await read('src/components/DesktopEventPage.astro');

  assert.match(layout, /const reducedMotion = window\.matchMedia\('\(prefers-reduced-motion: reduce\)'\)/u);
  assert.match(layout, /if \(reducedMotion\.matches\) return;\s*const mode = direction/su);
  assert.match(layout, /gallery\.dataset\.autoAdvance = reducedMotion\.matches \? 'false' : 'true'/u);
  assert.match(layout, /reducedMotion\.addEventListener\?\.\('change'.*clearGalleryPanTimer\(\).*activeGallery\.dataset\.autoAdvance = 'false'/su);
  assert.match(layout, /const syncSlideTabbability/u);
  assert.match(layout, /control\.setAttribute\('tabindex', '-1'\)/u);
  assert.match(layout, /syncSlideTabbability\(slide, current\)/u);
  assert.match(layout, /event\.key === 'Tab'.*activeGallery\.querySelectorAll\('a\[href\],button:not\(:disabled\)/su);
  assert.match(layout, /event\.shiftKey && current === first/u);
  assert.match(mobileHero, /data-hero-gallery-counter role="status" aria-live="polite" aria-atomic="true"/u);
  assert.match(desktop, /data-hero-gallery-counter role="status" aria-live="polite" aria-atomic="true"/u);
});

test('app-owned media viewers declare persistent lower-surface semantics', async () => {
  const mobileHero = await read('src/components/EventHero.astro');
  const desktop = await read('src/components/DesktopEventPage.astro');
  const exhibitions = await read('src/components/ExhibitionsPersonalSurface.astro');

  for (const source of [mobileHero, desktop, exhibitions]) {
    assert.match(source, /data-app-lower-surface="media"\s+data-app-lower-lifecycle="persistent"/u);
  }
  assert.match(desktop, /\.desktop-portrait-viewer__topbar\s*\{[\s\S]*background:var\(--ke-color-background-surface\);[\s\S]*border-bottom:var\(--ke-shape-border-hairline\) solid var\(--ke-color-border-default\)/u);
  assert.match(exhibitions, /\.ex-gallery\s*\{[^}]*background:var\(--ke-color-exhibitions-gallery-surface\);[^}]*color:var\(--ke-color-text-primary\)/u);
  assert.match(await read('src/components/design-system/product-theme-foundations.css'), /--ke-color-exhibitions-gallery-surface:\s*var\(--ke-color-background-surface\)/u);
  assert.match(exhibitions, /\.ex-gallery__stage img\s*\{[^}]*object-fit:contain/u);
});

test('closed desktop hero owns unmodified arrows only while hovered or focused', async () => {
  const desktop = await read('src/components/DesktopEventPage.astro');
  const canaries = JSON.parse(await read('tests/fixtures/event-gallery-interaction-canaries.json'));

  assert.match(desktop, /data-closed-hero-gallery=\{photoCount > 1 \? galleryId : undefined\}/u);
  assert.match(desktop, /aria-keyshortcuts=\{photoCount > 1 \? 'ArrowLeft ArrowRight' : undefined\}/u);
  assert.match(desktop, /data-closed-hero-status role="status" aria-live="polite" aria-atomic="true"/u);
  assert.match(desktop, /event\.altKey \|\| event\.ctrlKey \|\| event\.metaKey \|\| event\.shiftKey/u);
  assert.match(desktop, /target\?\.closest\('input,textarea,select,\[contenteditable="true"\],\[role="textbox"\]'\)/u);
  assert.match(desktop, /if \(!closedHeroHovered && !heroFocused\) return;\s*event\.preventDefault\(\)/su);
  assert.match(desktop, /document\.body\.classList\.contains\('has-hero-gallery-open'\)/u);
  assert.match(desktop, /if \(efficientViewer && !efficientViewer\.hidden\) return/u);
  assert.match(desktop, /selectClosedHero\(current \+ \(event\.key === 'ArrowRight' \? 1 : -1\)\)/u);
  assert.match(desktop, /closedHeroStatus\.textContent = `Фото \$\{next \+ 1\} из \$\{closedHeroSlides\.length\}`/u);

  for (const eventId of [5755, 6408, 4783]) {
    const event = canaries.events.find((candidate) => candidate.id === eventId);
    assert.ok(event, `frozen gallery canary ${eventId} is required`);
    assert.ok(event.image_asset_count > 1, `frozen gallery canary ${eventId} must remain a multi-image interaction gate`);
  }
});

test('Playwright gate covers the three pinned gallery interaction fixtures', async () => {
  const gate = await read('tests/event-gallery-interactions.playwright.js');
  assert.match(gate, /5755/u);
  assert.match(gate, /6408/u);
  assert.match(gate, /4783/u);
  assert.match(gate, /Control\+ArrowRight/u);
  assert.match(gate, /data-gallery-slide-kind="cta"/u);
  assert.match(gate, /desktop image click must close the gallery/u);
  assert.match(gate, /mobile CTA must navigate/u);
});
