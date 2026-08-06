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
if (!url || !/^https?:\/\//u.test(url)) throw new Error('Missing --url');
mkdirSync(artifactDir, { recursive: true });

const viewports = [
  { name: 'visual-wide', width: 1728, height: 900 },
  { name: 'visual-desktop', width: 1440, height: 900 },
  { name: 'visual-mobile', width: 390, height: 844 },
  { name: 'visual-mobile-small', width: 320, height: 568 },
  { name: 'visual-landscape', width: 844, height: 390 },
];

const executablePath = String(process.env.PRELAUNCH_CHROMIUM_EXECUTABLE_PATH || '').trim() || undefined;
const browser = await chromium.launch({ headless: true, executablePath });
const failures = [];
const evidence = [];

function overlaps(left, right, padding = 0) {
  return !(
    left.right + padding <= right.left
    || right.right + padding <= left.left
    || left.bottom + padding <= right.top
    || right.bottom + padding <= left.top
  );
}

async function pixelEvidence(page, png, scene) {
  const imageSrc = `data:image/png;base64,${png.toString('base64')}`;
  return page.evaluate(async ({ imageSrc: src, tiles, gap, projection }) => {
    const image = new Image();
    image.src = src;
    await new Promise((resolve, reject) => {
      image.onload = resolve;
      image.onerror = reject;
    });
    const canvas = document.createElement('canvas');
    canvas.width = image.naturalWidth;
    canvas.height = image.naturalHeight;
    const context = canvas.getContext('2d', { willReadFrequently: true });
    if (!context) throw new Error('Canvas unavailable');
    context.drawImage(image, 0, 0);

    const average = (x, y, radius = 1) => {
      const left = Math.max(0, Math.min(canvas.width - 1, Math.round(x) - radius));
      const top = Math.max(0, Math.min(canvas.height - 1, Math.round(y) - radius));
      const width = Math.max(1, Math.min(canvas.width - left, radius * 2 + 1));
      const height = Math.max(1, Math.min(canvas.height - top, radius * 2 + 1));
      const data = context.getImageData(left, top, width, height).data;
      const sum = [0, 0, 0];
      for (let index = 0; index < data.length; index += 4) {
        sum[0] += data[index];
        sum[1] += data[index + 1];
        sum[2] += data[index + 2];
      }
      const count = Math.max(1, data.length / 4);
      return sum.map((value) => value / count);
    };
    const distance = (left, right) => Math.hypot(
      left[0] - right[0],
      left[1] - right[1],
      left[2] - right[2],
    );

    const samples = tiles.map((tile) => {
      const seamX = Math.min(canvas.width - 2, tile.right + Math.max(2, gap / 2));
      const seam = average(seamX, tile.top + tile.height / 2, 1);
      const corner = average(tile.left + 2, tile.top + 2, 1);
      const side = average(tile.left + 8, tile.top + tile.height / 2, 1);
      const centre = average(tile.left + tile.width / 2, tile.top + tile.height / 2, 1);
      return {
        index: tile.index,
        cornerToSeam: distance(corner, seam),
        sideToSeam: distance(side, seam),
        centreToSeam: distance(centre, seam),
        corner,
        side,
        centre,
        seam,
      };
    });

    const region = {
      left: Math.max(0, Math.floor(projection.left)),
      top: Math.max(0, Math.floor(projection.top)),
      right: Math.min(canvas.width, Math.ceil(projection.right)),
      bottom: Math.min(canvas.height, Math.ceil(projection.bottom)),
    };
    let neutralBright = 0;
    let warm = 0;
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
        const maximum = Math.max(r, g, b);
        const minimum = Math.min(r, g, b);
        const luminance = r * .2126 + g * .7152 + b * .0722;
        if (luminance > 112 && maximum - minimum < 12) neutralBright += 1;
        if (luminance > 28 && r > g * 1.07 && r > b * 1.15) warm += 1;
        sampled += 1;
      }
    }
    return {
      samples,
      strongestCentre: samples.toSorted((a, b) => b.centreToSeam - a.centreToSeam)[0] || null,
      cleanestCorner: samples.toSorted((a, b) => a.cornerToSeam - b.cornerToSeam)[0] || null,
      neutralBrightRatio: sampled ? neutralBright / sampled : 0,
      warmArtworkRatio: sampled ? warm / sampled : 0,
      artworkRegion: region,
    };
  }, { imageSrc, tiles: scene.safeWindowTiles, gap: scene.gap, projection: scene.projection });
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
      await page.waitForTimeout(350);

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
        const intersects = (left, right, padding = 0) => !(
          left.right + padding <= right.left
          || right.right + padding <= left.left
          || left.bottom + padding <= right.top
          || right.bottom + padding <= left.top
        );
        const root = document.querySelector('[data-prelaunch-page]');
        const background = document.querySelector('.prelaunch__background');
        const projection = document.querySelector('.prelaunch__projection');
        const mosaic = document.querySelector('[data-prelaunch-mosaic]');
        const tiles = [...document.querySelectorAll('[data-prelaunch-tile]')];
        const heading = document.querySelector('#prelaunch-title');
        const consent = document.querySelector('.prelaunch-form__consent span');
        const promise = document.querySelector('.prelaunch-form__promise');
        const projectionRect = rect(projection);
        const occupied = [
          rect(document.querySelector('.prelaunch__brand')),
          rect(document.querySelector('.prelaunch__copy')),
          rect(document.querySelector('.prelaunch__notify')),
        ].filter(Boolean);
        const visibleWindows = tiles
          .map((tile, index) => ({ index, tile, box: rect(tile) }))
          .filter(({ tile, box }) => (
            box
            && tile.getAttribute('data-window') === 'true'
            && tile.getAttribute('data-state') === 'revealed'
            && box.top >= 0
            && box.left >= 0
            && box.bottom <= innerHeight
            && box.right <= innerWidth
          ));
        const safeWindowTiles = visibleWindows
          .filter(({ box }) => occupied.every((occupiedBox) => !intersects(box, occupiedBox, 3)))
          .map(({ index, box }) => ({ index, ...box }));
        const first = tiles[0]?.getBoundingClientRect();
        const second = tiles[1]?.getBoundingClientRect();
        const representative = visibleWindows[0]?.tile || tiles[0];
        const base = representative ? getComputedStyle(representative) : null;
        const glass = representative ? getComputedStyle(representative, '::before') : null;
        const artwork = background ? getComputedStyle(background, '::after') : null;
        const seam = mosaic ? getComputedStyle(mosaic, '::before') : null;
        const wordmarkBand = projectionRect ? {
          top: projectionRect.top + projectionRect.height * .45,
          bottom: projectionRect.top + projectionRect.height * .78,
        } : null;
        const wordmarkWindowCount = wordmarkBand
          ? visibleWindows.filter(({ box }) => box.bottom >= wordmarkBand.top && box.top <= wordmarkBand.bottom).length
          : 0;
        return {
          viewport: { width: innerWidth, height: innerHeight },
          artworkRevealAligned: root?.getAttribute('data-artwork-reveal-aligned'),
          projection: projectionRect,
          artworkDisplay: artwork?.display || '',
          artworkBackgroundImage: artwork?.backgroundImage || '',
          artworkBackgroundSize: artwork?.backgroundSize || '',
          mosaicBackground: mosaic ? getComputedStyle(mosaic).backgroundColor : '',
          mosaicBeforeDisplay: seam?.display || '',
          mosaicBeforeContent: seam?.content || '',
          tileRadius: base?.borderRadius || '',
          tileOverflow: base?.overflow || '',
          glassRadius: glass?.borderRadius || '',
          glassShadow: glass?.boxShadow || '',
          gap: first && second ? second.left - first.right : -1,
          visibleWindowCount: visibleWindows.length,
          safeWindowTiles,
          wordmarkWindowCount,
          heading: rect(heading),
          headingFontSize: heading ? Number.parseFloat(getComputedStyle(heading).fontSize) : 0,
          consentCopy: consent?.textContent?.trim() || '',
          promiseCopy: promise?.textContent?.trim() || '',
          oldOneLetterCopyPresent: document.body.textContent?.includes('одно письмо') || false,
        };
      });

      check(scene.artworkRevealAligned === 'true', `${viewport.name}: reveal map is not aligned`, localFailures);
      check(scene.projection?.width > 0, `${viewport.name}: artwork proxy missing`, localFailures);
      check(scene.artworkDisplay !== 'none', `${viewport.name}: artwork hidden`, localFailures);
      check(scene.artworkBackgroundImage.includes('_astro') || scene.artworkBackgroundImage.includes('announcements'), `${viewport.name}: artwork URL unresolved`, localFailures);
      check(scene.artworkBackgroundSize === 'contain', `${viewport.name}: artwork background-size=${scene.artworkBackgroundSize}`, localFailures);
      check(scene.mosaicBeforeDisplay === 'none' || scene.mosaicBeforeContent === 'none', `${viewport.name}: obsolete seam overlay visible`, localFailures);
      check(scene.mosaicBackground.includes('7, 9, 13') || scene.mosaicBackground.includes('7, 9, 12'), `${viewport.name}: opaque seam backing missing`, localFailures);
      check(scene.tileRadius !== '0px' && scene.glassRadius !== '0px', `${viewport.name}: rounded mask missing`, localFailures);
      check(scene.tileOverflow === 'hidden', `${viewport.name}: tile overflow=${scene.tileOverflow}`, localFailures);
      check(!scene.glassShadow.includes('rgb(7, 9, 13)'), `${viewport.name}: square spread shadow remains`, localFailures);
      check(scene.visibleWindowCount >= (viewport.height <= 500 ? 1 : 4), `${viewport.name}: visible windows=${scene.visibleWindowCount}`, localFailures);
      check(scene.wordmarkWindowCount >= (viewport.height <= 500 ? 1 : 2), `${viewport.name}: wordmark windows=${scene.wordmarkWindowCount}`, localFailures);
      check(scene.consentCopy.includes('важных обновлениях') && scene.consentCopy.includes('Отписаться можно'), `${viewport.name}: consent copy incomplete`, localFailures);
      check(scene.promiseCopy.includes('приятный сюрприз'), `${viewport.name}: surprise copy missing`, localFailures);
      check(!scene.oldOneLetterCopyPresent, `${viewport.name}: obsolete one-letter copy visible`, localFailures);

      if (scene.projection) {
        const widthRatio = scene.projection.width / viewport.width;
        const topRatio = scene.projection.top / viewport.height;
        const wordmarkMid = scene.projection.top + scene.projection.height * .615;
        check(wordmarkMid <= viewport.height * .96, `${viewport.name}: wordmark below viewport`, localFailures);
        if (viewport.width <= 599) {
          check(widthRatio >= .86 && widthRatio <= 1, `${viewport.name}: artwork width ratio=${widthRatio.toFixed(3)}`, localFailures);
          check(topRatio >= .29 && topRatio <= .46, `${viewport.name}: artwork top ratio=${topRatio.toFixed(3)}`, localFailures);
        } else if (viewport.height > 500) {
          check(widthRatio >= .5 && widthRatio <= .74, `${viewport.name}: artwork width ratio=${widthRatio.toFixed(3)}`, localFailures);
          check(topRatio >= 0 && topRatio <= .12, `${viewport.name}: artwork top ratio=${topRatio.toFixed(3)}`, localFailures);
          check(scene.headingFontSize <= 94, `${viewport.name}: heading font=${scene.headingFontSize}`, localFailures);
        }
      }

      const screenshotPath = resolve(artifactDir, `prelaunch-${viewport.name}-${viewport.width}x${viewport.height}.png`);
      const png = await page.screenshot({ path: screenshotPath, fullPage: false, animations: 'disabled' });
      let pixels = null;
      if (scene.projection && scene.safeWindowTiles.length) {
        pixels = await pixelEvidence(page, png, scene);
        check(pixels.cleanestCorner?.cornerToSeam <= 30, `${viewport.name}: no clean rounded-corner sample`, localFailures);
        check(pixels.strongestCentre?.centreToSeam >= 12, `${viewport.name}: artwork not visible through safe pane`, localFailures);
        check(pixels.neutralBrightRatio <= .02, `${viewport.name}: pale-field ratio=${pixels.neutralBrightRatio.toFixed(4)}`, localFailures);
        check(pixels.warmArtworkRatio >= .008, `${viewport.name}: warm-artwork ratio=${pixels.warmArtworkRatio.toFixed(4)}`, localFailures);
      } else if (viewport.height > 500) {
        localFailures.push(`${viewport.name}: no safe window available for pixel inspection`);
      }

      const result = { viewport, scene, pixels, failures: localFailures };
      writeFileSync(resolve(artifactDir, `prelaunch-${viewport.name}-${viewport.width}x${viewport.height}-visual-regression-v3.json`), `${JSON.stringify(result, null, 2)}\n`);
      evidence.push(result);
    } catch (error) {
      localFailures.push(`${viewport.name}: capture failed: ${String(error?.stack || error)}`);
    } finally {
      failures.push(...localFailures);
      await context.close();
    }
  }
} finally {
  await browser.close();
}

const summary = { schema_version: 'prelaunch_visual_regression_v3', ok: failures.length === 0, url, evidence, failures };
writeFileSync(resolve(artifactDir, 'prelaunch-visual-regression-v3-summary.json'), `${JSON.stringify(summary, null, 2)}\n`);
console.log(JSON.stringify(summary, null, 2));
if (failures.length) throw new Error(`Prelaunch visual regression v3 failed:\n- ${failures.join('\n- ')}`);
