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

const url = option('--url');
const artifactDir = resolve(option('--artifact-dir', 'artifacts/prelaunch-browser'));
if (!url || !/^https?:\/\//u.test(url)) {
  throw new Error('Usage: check-prelaunch-visual-regressions.mjs --url <http(s) URL> [--artifact-dir <path>]');
}
mkdirSync(artifactDir, { recursive: true });

const viewports = [
  { name: 'visual-wide', width: 1728, height: 900 },
  { name: 'visual-desktop', width: 1440, height: 900 },
  { name: 'visual-mobile', width: 390, height: 844 },
  { name: 'visual-mobile-small', width: 320, height: 568 },
];

const executablePath = String(process.env.PRELAUNCH_CHROMIUM_EXECUTABLE_PATH || '').trim() || undefined;
const browser = await chromium.launch({ headless: true, executablePath });
const failures = [];
const evidence = [];

async function sampleScreenshot(page, png, scene) {
  const src = `data:image/png;base64,${png.toString('base64')}`;
  return page.evaluate(async ({ imageSrc, tile, gap, projection }) => {
    const image = new Image();
    image.src = imageSrc;
    await new Promise((resolve, reject) => {
      image.onload = resolve;
      image.onerror = reject;
    });
    const canvas = document.createElement('canvas');
    canvas.width = image.naturalWidth;
    canvas.height = image.naturalHeight;
    const context = canvas.getContext('2d', { willReadFrequently: true });
    if (!context) throw new Error('2d canvas unavailable');
    context.drawImage(image, 0, 0);

    const average = (x, y, radius = 1) => {
      const left = Math.max(0, Math.round(x) - radius);
      const top = Math.max(0, Math.round(y) - radius);
      const width = Math.min(canvas.width - left, radius * 2 + 1);
      const height = Math.min(canvas.height - top, radius * 2 + 1);
      const data = context.getImageData(left, top, width, height).data;
      const sum = [0, 0, 0, 0];
      for (let index = 0; index < data.length; index += 4) {
        sum[0] += data[index];
        sum[1] += data[index + 1];
        sum[2] += data[index + 2];
        sum[3] += data[index + 3];
      }
      const count = Math.max(1, data.length / 4);
      return sum.map((value) => value / count);
    };
    const distance = (left, right) => Math.hypot(
      left[0] - right[0],
      left[1] - right[1],
      left[2] - right[2],
    );
    const luminance = (value) => value[0] * .2126 + value[1] * .7152 + value[2] * .0722;

    const corner = average(tile.left + 2, tile.top + 2, 1);
    const side = average(tile.left + 5, tile.top + tile.height / 2, 1);
    const centre = average(tile.left + tile.width / 2, tile.top + tile.height / 2, 1);
    const seam = average(tile.right + Math.max(2, gap / 2), tile.top + tile.height / 2, 1);

    const region = {
      left: Math.max(0, Math.floor(projection.left + projection.width * .86)),
      top: Math.max(0, Math.floor(projection.top + projection.height * .25)),
      right: Math.min(canvas.width, Math.ceil(projection.right - projection.width * .02)),
      bottom: Math.min(canvas.height, Math.ceil(projection.top + projection.height * .78)),
    };
    let neutralBright = 0;
    let sampled = 0;
    if (region.right > region.left && region.bottom > region.top) {
      const pixels = context.getImageData(
        region.left,
        region.top,
        region.right - region.left,
        region.bottom - region.top,
      ).data;
      for (let index = 0; index < pixels.length; index += 16) {
        const r = pixels[index];
        const g = pixels[index + 1];
        const b = pixels[index + 2];
        const max = Math.max(r, g, b);
        const min = Math.min(r, g, b);
        if ((r + g + b) / 3 > 92 && max - min < 12) neutralBright += 1;
        sampled += 1;
      }
    }

    return {
      corner,
      side,
      centre,
      seam,
      cornerToSeam: distance(corner, seam),
      sideToSeam: distance(side, seam),
      centreToSeam: distance(centre, seam),
      cornerLuminance: luminance(corner),
      seamLuminance: luminance(seam),
      neutralBrightRatio: sampled ? neutralBright / sampled : 0,
      neutralBrightRegion: region,
    };
  }, {
    imageSrc: src,
    tile: scene.sampleTile,
    gap: scene.gap,
    projection: scene.projection,
  });
}

try {
  for (const viewport of viewports) {
    const localFailures = [];
    const context = await browser.newContext({
      viewport: { width: viewport.width, height: viewport.height },
      reducedMotion: 'reduce',
      colorScheme: 'dark',
    });
    try {
      const page = await context.newPage();
      const response = await page.goto(url, { waitUntil: 'networkidle' });
      check(response?.ok(), `${viewport.name}: HTTP ${response?.status() || 'unknown'}`, localFailures);
      await page.locator('[data-prelaunch-page]').waitFor({ state: 'visible' });
      await page.locator('[data-prelaunch-page][data-artwork-reveal-aligned]').waitFor({ state: 'attached' });
      await page.waitForTimeout(300);

      const scene = await page.evaluate(() => {
        const rect = (node) => {
          if (!(node instanceof Element)) return null;
          const value = node.getBoundingClientRect();
          return {
            top: value.top,
            right: value.right,
            bottom: value.bottom,
            left: value.left,
            width: value.width,
            height: value.height,
          };
        };
        const root = document.querySelector('[data-prelaunch-page]');
        const background = document.querySelector('.prelaunch__background');
        const projection = document.querySelector('.prelaunch__projection');
        const mosaic = document.querySelector('[data-prelaunch-mosaic]');
        const tiles = [...document.querySelectorAll('[data-prelaunch-tile]')];
        const heading = document.querySelector('#prelaunch-title');
        const consent = document.querySelector('.prelaunch-form__consent span');
        const promise = document.querySelector('.prelaunch-form__promise');
        const mosaicStyle = mosaic ? getComputedStyle(mosaic) : null;
        const seamLayer = mosaic ? getComputedStyle(mosaic, '::before') : null;
        const artworkLayer = background ? getComputedStyle(background, '::after') : null;
        const projectionRect = rect(projection);
        const visibleTiles = tiles
          .map((tile) => ({
            tile,
            rect: tile.getBoundingClientRect(),
            edge: tile.getAttribute('data-edge'),
          }))
          .filter(({ rect }) => (
            rect.top >= 0
            && rect.bottom <= window.innerHeight
            && rect.left >= 0
            && rect.right <= window.innerWidth
          ));
        const preferred = visibleTiles.find(({ rect: box, edge }) => (
          edge === 'ambient'
          && box.left > window.innerWidth * .58
          && box.top > window.innerHeight * .38
          && box.bottom < window.innerHeight * .78
        )) || visibleTiles.at(-1);
        const sampleTile = preferred ? {
          top: preferred.rect.top,
          right: preferred.rect.right,
          bottom: preferred.rect.bottom,
          left: preferred.rect.left,
          width: preferred.rect.width,
          height: preferred.rect.height,
        } : null;
        const first = tiles[0]?.getBoundingClientRect();
        const second = tiles[1]?.getBoundingClientRect();
        const sampleGlass = preferred ? getComputedStyle(preferred.tile, '::before') : null;
        const sampleBase = preferred ? getComputedStyle(preferred.tile) : null;
        return {
          viewport: { width: window.innerWidth, height: window.innerHeight },
          artworkRevealAligned: root?.getAttribute('data-artwork-reveal-aligned'),
          projection: projectionRect,
          artworkMixBlendMode: artworkLayer?.mixBlendMode || '',
          artworkDisplay: artworkLayer?.display || '',
          mosaicBackground: mosaicStyle?.backgroundColor || '',
          mosaicBeforeDisplay: seamLayer?.display || '',
          mosaicBeforeContent: seamLayer?.content || '',
          sampleTile,
          sampleTileRadius: sampleBase?.borderRadius || '',
          sampleTileOverflow: sampleBase?.overflow || '',
          sampleGlassRadius: sampleGlass?.borderRadius || '',
          sampleGlassShadow: sampleGlass?.boxShadow || '',
          gap: first && second ? second.left - first.right : -1,
          heading: rect(heading),
          headingFontSize: heading ? Number.parseFloat(getComputedStyle(heading).fontSize) : 0,
          consentCopy: consent?.textContent?.trim() || '',
          promiseCopy: promise?.textContent?.trim() || '',
          oldOneLetterCopyPresent: document.body.textContent?.includes('одно письмо') || false,
        };
      });

      check(scene.artworkRevealAligned === 'true', `${viewport.name}: reveal map is not aligned to artwork`, localFailures);
      check(scene.projection && scene.projection.width > 0, `${viewport.name}: artwork proxy has no geometry`, localFailures);
      check(scene.artworkMixBlendMode === 'multiply', `${viewport.name}: artwork white-field blend=${scene.artworkMixBlendMode}`, localFailures);
      check(scene.artworkDisplay !== 'none', `${viewport.name}: artwork layer is hidden`, localFailures);
      check(scene.mosaicBeforeDisplay === 'none' || scene.mosaicBeforeContent === 'none', `${viewport.name}: obsolete square seam overlay is still painted`, localFailures);
      check(scene.mosaicBackground.includes('7, 9, 13') || scene.mosaicBackground.includes('7, 9, 12'), `${viewport.name}: grid does not own the opaque seam`, localFailures);
      check(scene.sampleTile, `${viewport.name}: no visible sample tile`, localFailures);
      check(scene.sampleTileRadius !== '0px', `${viewport.name}: tile parent is still square`, localFailures);
      check(scene.sampleTileOverflow === 'hidden', `${viewport.name}: tile overflow=${scene.sampleTileOverflow}`, localFailures);
      check(scene.sampleGlassRadius !== '0px', `${viewport.name}: glass surface lost radius`, localFailures);
      check(!scene.sampleGlassShadow.includes('rgb(7, 9, 13)'), `${viewport.name}: spread-shadow square mask is still present`, localFailures);
      check(scene.consentCopy.includes('важных обновлениях'), `${viewport.name}: broad update consent copy missing`, localFailures);
      check(scene.consentCopy.includes('Отписаться можно'), `${viewport.name}: unsubscribe promise missing`, localFailures);
      check(scene.promiseCopy.includes('приятный сюрприз'), `${viewport.name}: subscriber surprise copy missing`, localFailures);
      check(!scene.oldOneLetterCopyPresent, `${viewport.name}: obsolete one-letter copy remains visible`, localFailures);

      if (scene.projection) {
        const widthRatio = scene.projection.width / viewport.width;
        const topRatio = scene.projection.top / viewport.height;
        if (viewport.width <= 599) {
          check(widthRatio >= .86 && widthRatio <= 1, `${viewport.name}: artwork width ratio=${widthRatio.toFixed(3)}`, localFailures);
          check(topRatio >= .2 && topRatio <= .4, `${viewport.name}: artwork top ratio=${topRatio.toFixed(3)}`, localFailures);
        } else {
          check(widthRatio >= .52 && widthRatio <= .74, `${viewport.name}: artwork width ratio=${widthRatio.toFixed(3)}`, localFailures);
          check(topRatio >= .1 && topRatio <= .28, `${viewport.name}: artwork top ratio=${topRatio.toFixed(3)}`, localFailures);
          check(scene.headingFontSize <= 94, `${viewport.name}: heading font=${scene.headingFontSize}px`, localFailures);
          check(scene.heading && scene.heading.top <= viewport.height * .34, `${viewport.name}: heading top=${scene.heading?.top}`, localFailures);
        }
      }

      const screenshotPath = resolve(artifactDir, `prelaunch-${viewport.name}-${viewport.width}x${viewport.height}.png`);
      const png = await page.screenshot({ path: screenshotPath, fullPage: false, animations: 'disabled' });
      let pixelEvidence = null;
      if (scene.sampleTile && scene.projection) {
        pixelEvidence = await sampleScreenshot(page, png, scene);
        check(pixelEvidence.cornerToSeam <= 30, `${viewport.name}: rounded corner differs from seam by ${pixelEvidence.cornerToSeam.toFixed(1)}`, localFailures);
        check(pixelEvidence.sideToSeam >= 8, `${viewport.name}: pane side still reads as a square seam (${pixelEvidence.sideToSeam.toFixed(1)})`, localFailures);
        check(pixelEvidence.centreToSeam >= 10, `${viewport.name}: pane centre indistinguishable from seam`, localFailures);
        check(pixelEvidence.neutralBrightRatio <= .025, `${viewport.name}: pale artwork field ratio=${pixelEvidence.neutralBrightRatio.toFixed(4)}`, localFailures);
      }

      const result = { viewport, scene, pixelEvidence, failures: localFailures };
      writeFileSync(
        resolve(artifactDir, `prelaunch-${viewport.name}-${viewport.width}x${viewport.height}-visual-regression.json`),
        `${JSON.stringify(result, null, 2)}\n`,
      );
      evidence.push(result);
    } catch (error) {
      localFailures.push(`${viewport.name}: visual regression capture failed: ${String(error?.stack || error)}`);
    } finally {
      failures.push(...localFailures);
      await context.close();
    }
  }
} finally {
  await browser.close();
}

const summary = {
  schema_version: 'prelaunch_visual_regression_v1',
  ok: failures.length === 0,
  url,
  evidence,
  failures,
};
writeFileSync(resolve(artifactDir, 'prelaunch-visual-regression-summary.json'), `${JSON.stringify(summary, null, 2)}\n`);
console.log(JSON.stringify(summary, null, 2));
if (failures.length > 0) {
  throw new Error(`Prelaunch visual regression gate failed after preserving evidence:\n- ${failures.join('\n- ')}`);
}
