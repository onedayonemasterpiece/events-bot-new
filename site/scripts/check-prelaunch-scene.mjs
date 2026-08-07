import { mkdirSync, writeFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { chromium } from 'playwright';

function option(name, fallback = '') {
  const index = process.argv.indexOf(name);
  return index >= 0 ? String(process.argv[index + 1] || '') : fallback;
}

function check(condition, message, failures) {
  if (!condition) failures.push(message);
}

function boundedClip(box, viewport) {
  const x = Math.max(0, Math.floor(box.x));
  const y = Math.max(0, Math.floor(box.y));
  const right = Math.min(viewport.width, Math.ceil(box.x + box.width));
  const bottom = Math.min(viewport.height, Math.ceil(box.y + box.height));
  if (right - x < 2 || bottom - y < 2) return null;
  return { x, y, width: right - x, height: bottom - y };
}

const url = option('--url');
const artifactDir = resolve(option('--artifact-dir', 'artifacts/prelaunch-browser'));
if (!url || !/^https?:\/\//u.test(url)) {
  throw new Error('Usage: check-prelaunch-scene.mjs --url <http(s) URL> [--artifact-dir <path>]');
}
mkdirSync(artifactDir, { recursive: true });

const viewports = [
  { name: 'reference-square', width: 1200, height: 1200 },
  { name: 'visual-wide', width: 1728, height: 900 },
  { name: 'visual-desktop', width: 1440, height: 900 },
  { name: 'mobile', width: 390, height: 844 },
  { name: 'mobile-small', width: 320, height: 568 },
  { name: 'mobile-landscape', width: 844, height: 390 },
];

const executablePath = String(process.env.PRELAUNCH_CHROMIUM_EXECUTABLE_PATH || '').trim() || undefined;
const browser = await chromium.launch({ headless: true, executablePath });
const failures = [];
const evidence = [];

try {
  for (const viewport of viewports) {
    const localFailures = [];
    const context = await browser.newContext({
      viewport: { width: viewport.width, height: viewport.height },
      reducedMotion: 'reduce',
      colorScheme: 'dark',
      deviceScaleFactor: 1,
    });
    try {
      const page = await context.newPage();
      const response = await page.goto(url, { waitUntil: 'networkidle' });
      check(response?.ok(), `${viewport.name}: HTTP ${response?.status() || 'unknown'}`, localFailures);
      await page.locator('[data-prelaunch-page]').waitFor({ state: 'visible' });
      await page.waitForFunction(() => {
        const root = document.querySelector('[data-prelaunch-page]');
        const image = document.querySelector('[data-prelaunch-artwork-image]');
        return root?.getAttribute('data-scene-ready') === 'true'
          && image instanceof HTMLImageElement
          && image.complete
          && image.naturalWidth > 0;
      }, undefined, { timeout: 12_000 });
      await page.waitForTimeout(250);

      const scene = await page.evaluate(() => {
        const rect = (node) => {
          if (!(node instanceof Element)) return null;
          const value = node.getBoundingClientRect();
          return {
            x: value.x,
            y: value.y,
            top: value.top,
            right: value.right,
            bottom: value.bottom,
            left: value.left,
            width: value.width,
            height: value.height,
          };
        };
        const root = document.querySelector('[data-prelaunch-page]');
        const artwork = document.querySelector('[data-prelaunch-artwork]');
        const artworkImage = document.querySelector('[data-prelaunch-artwork-image]');
        const seams = document.querySelector('[data-prelaunch-seams]');
        const seamPath = document.querySelector('[data-prelaunch-seam-path]');
        const mosaic = document.querySelector('[data-prelaunch-mosaic]');
        const tiles = [...document.querySelectorAll('[data-prelaunch-tile]')].filter((tile) => !tile.hidden);
        const clearTiles = tiles.filter((tile) => tile.getAttribute('data-depth') === 'clear');
        const sample = clearTiles.find((tile) => {
          const box = tile.getBoundingClientRect();
          return box.left >= 0 && box.top >= 0 && box.right <= innerWidth && box.bottom <= innerHeight;
        }) || tiles[0];
        const sampleStyle = sample ? getComputedStyle(sample) : null;
        const artworkStyle = artwork ? getComputedStyle(artwork) : null;
        const imageStyle = artworkImage ? getComputedStyle(artworkImage) : null;
        const rootStyle = root ? getComputedStyle(root) : null;
        const seamStyle = seams ? getComputedStyle(seams) : null;
        const mosaicStyle = mosaic ? getComputedStyle(mosaic) : null;
        const scrolling = document.scrollingElement || document.documentElement;
        return {
          viewport: { width: innerWidth, height: innerHeight },
          rootPosition: rootStyle?.position || '',
          rootOverflow: rootStyle?.overflow || '',
          seamModel: root?.getAttribute('data-seam-model'),
          artworkModel: root?.getAttribute('data-artwork-model'),
          columns: Number(root?.getAttribute('data-grid-columns') || 0),
          rows: Number(root?.getAttribute('data-grid-rows') || 0),
          visibleTileCount: Number(root?.getAttribute('data-visible-tile-count') || 0),
          clearTileCount: Number(root?.getAttribute('data-clear-tile-count') || 0),
          artwork: {
            rect: rect(artwork),
            overflow: artworkStyle?.overflow || '',
            borderRadius: artworkStyle?.borderRadius || '',
            src: artworkImage instanceof HTMLImageElement ? artworkImage.currentSrc || artworkImage.src : '',
            naturalWidth: artworkImage instanceof HTMLImageElement ? artworkImage.naturalWidth : 0,
            naturalHeight: artworkImage instanceof HTMLImageElement ? artworkImage.naturalHeight : 0,
            imageWidth: imageStyle?.width || '',
            imageLeft: imageStyle?.left || '',
            imageTop: imageStyle?.top || '',
          },
          seams: {
            rect: rect(seams),
            zIndex: seamStyle?.zIndex || '',
            pathLength: seamPath?.getAttribute('d')?.length || 0,
            fillRule: seamPath?.getAttribute('fill-rule') || seamPath?.getAttribute('fillRule') || '',
          },
          mosaic: {
            rect: rect(mosaic),
            zIndex: mosaicStyle?.zIndex || '',
            gap: mosaicStyle?.gap || '',
          },
          sampleTile: {
            rect: rect(sample),
            borderRadius: sampleStyle?.borderRadius || '',
            backgroundColor: sampleStyle?.backgroundColor || '',
            backdropFilter: sampleStyle?.backdropFilter || sampleStyle?.webkitBackdropFilter || '',
            boxShadow: sampleStyle?.boxShadow || '',
          },
          verticalOverflow: Math.max(scrolling.scrollHeight, document.documentElement.scrollHeight, document.body?.scrollHeight || 0) - innerHeight,
          horizontalOverflow: Math.max(scrolling.scrollWidth, document.documentElement.scrollWidth, document.body?.scrollWidth || 0) - innerWidth,
          domTileCount: document.querySelectorAll('[data-prelaunch-tile]').length,
          text: {
            heading: document.querySelector('#prelaunch-title')?.textContent?.trim() || '',
            promise: document.querySelector('.prelaunch-form__promise')?.textContent?.trim() || '',
            consent: document.querySelector('.prelaunch-form__consent span')?.textContent?.trim() || '',
          },
        };
      });

      check(scene.rootPosition === 'fixed', `${viewport.name}: root position=${scene.rootPosition}`, localFailures);
      check(scene.rootOverflow === 'hidden', `${viewport.name}: root overflow=${scene.rootOverflow}`, localFailures);
      check(scene.seamModel === 'inverse-svg-rounded-holes', `${viewport.name}: seam model=${scene.seamModel}`, localFailures);
      check(scene.artworkModel === 'source-asset-rounded-crop', `${viewport.name}: artwork model=${scene.artworkModel}`, localFailures);
      check(scene.seams.pathLength > 500, `${viewport.name}: seam path length=${scene.seams.pathLength}`, localFailures);
      check(scene.seams.fillRule === 'evenodd', `${viewport.name}: seam fill-rule=${scene.seams.fillRule}`, localFailures);
      check(scene.domTileCount === 112, `${viewport.name}: DOM tile count=${scene.domTileCount}`, localFailures);
      check(scene.visibleTileCount > 0 && scene.visibleTileCount <= 112, `${viewport.name}: visible tile count=${scene.visibleTileCount}`, localFailures);
      check(scene.clearTileCount >= 3, `${viewport.name}: clear tile count=${scene.clearTileCount}`, localFailures);
      check(scene.artwork.naturalWidth >= 1200 && scene.artwork.naturalHeight >= 1200, `${viewport.name}: source artwork did not decode`, localFailures);
      check(scene.artwork.src.includes('/assets/prelaunch/PWA-icon.webp'), `${viewport.name}: unexpected artwork source`, localFailures);
      check(scene.artwork.overflow === 'hidden', `${viewport.name}: artwork crop is not clipped`, localFailures);
      check(scene.artwork.borderRadius !== '0px', `${viewport.name}: artwork crop lost radius`, localFailures);
      check(scene.sampleTile.borderRadius !== '0px', `${viewport.name}: sample tile is square`, localFailures);
      check(scene.sampleTile.backdropFilter.includes('blur'), `${viewport.name}: sample tile has no glass blur`, localFailures);
      check(scene.verticalOverflow <= 1, `${viewport.name}: vertical overflow=${scene.verticalOverflow}`, localFailures);
      check(scene.horizontalOverflow <= 1, `${viewport.name}: horizontal overflow=${scene.horizontalOverflow}`, localFailures);
      check(scene.text.heading.includes('Запуск') && scene.text.heading.includes('1 сентября'), `${viewport.name}: heading missing`, localFailures);
      check(scene.text.promise.includes('приятный сюрприз'), `${viewport.name}: surprise copy missing`, localFailures);
      check(scene.text.consent.includes('Отписаться можно'), `${viewport.name}: unsubscribe copy missing`, localFailures);
      check(!scene.text.consent.includes('одно письмо'), `${viewport.name}: obsolete one-letter consent remains`, localFailures);

      const screenshotPath = resolve(artifactDir, `prelaunch-${viewport.name}-${viewport.width}x${viewport.height}.png`);
      const domPath = resolve(artifactDir, `prelaunch-${viewport.name}-${viewport.width}x${viewport.height}-dom.html`);
      const scenePath = resolve(artifactDir, `prelaunch-${viewport.name}-${viewport.width}x${viewport.height}-scene.json`);
      await page.screenshot({ path: screenshotPath, fullPage: false, animations: 'disabled' });
      writeFileSync(domPath, await page.content());

      const artworkClip = scene.artwork.rect ? boundedClip(scene.artwork.rect, viewport) : null;
      if (artworkClip) {
        await page.screenshot({
          path: resolve(artifactDir, `prelaunch-${viewport.name}-artwork-crop.png`),
          clip: artworkClip,
          animations: 'disabled',
        });
      }

      const lightClip = boundedClip({
        x: viewport.width * .58,
        y: 0,
        width: viewport.width * .42,
        height: viewport.height * .58,
      }, viewport);
      if (lightClip) {
        await page.screenshot({
          path: resolve(artifactDir, `prelaunch-${viewport.name}-light-crop.png`),
          clip: lightClip,
          animations: 'disabled',
        });
      }

      const sampleRect = scene.sampleTile.rect;
      if (sampleRect) {
        const seamClip = boundedClip({
          x: sampleRect.x - sampleRect.width * .18,
          y: sampleRect.y - sampleRect.height * .18,
          width: sampleRect.width * 2.45,
          height: sampleRect.height * 2.45,
        }, viewport);
        if (seamClip) {
          await page.screenshot({
            path: resolve(artifactDir, `prelaunch-${viewport.name}-glass-seam-crop.png`),
            clip: seamClip,
            animations: 'disabled',
          });
        }
      }

      const result = { viewport, scene, failures: localFailures, screenshotPath, domPath, scenePath };
      writeFileSync(scenePath, `${JSON.stringify(result, null, 2)}\n`);
      evidence.push(result);
    } catch (error) {
      const message = `${viewport.name}: ${String(error?.stack || error)}`;
      localFailures.push(message);
      writeFileSync(resolve(artifactDir, `prelaunch-${viewport.name}-capture-error.txt`), `${message}\n`);
    } finally {
      failures.push(...localFailures);
      await context.close();
    }
  }
} finally {
  await browser.close();
}

const summary = {
  schema_version: 'prelaunch_scene_evidence_v2',
  ok: failures.length === 0,
  url,
  viewports,
  evidence,
  failures,
  manual_review_required: true,
  manual_review_axes: [
    'composition',
    'continuous_artwork_through_glass',
    'opaque_seams_and_rounded_corners',
    'glass_material',
    'single_upper_right_light',
    'desktop_mobile_equivalence',
  ],
};
writeFileSync(resolve(artifactDir, 'prelaunch-scene-summary.json'), `${JSON.stringify(summary, null, 2)}\n`);
console.log(JSON.stringify(summary, null, 2));
if (failures.length) throw new Error(failures.join('\n'));
