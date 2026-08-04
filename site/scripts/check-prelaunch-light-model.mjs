import { mkdirSync, writeFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { chromium } from 'playwright';

function option(name, fallback = '') {
  const index = process.argv.indexOf(name);
  return index >= 0 ? String(process.argv[index + 1] || '') : fallback;
}

const url = option('--url');
const artifactDir = resolve(option('--artifact-dir', 'artifacts/prelaunch-browser'));
if (!url || !/^https?:\/\//u.test(url)) {
  throw new Error('Usage: check-prelaunch-light-model.mjs --url <http(s) URL> [--artifact-dir <path>]');
}
mkdirSync(artifactDir, { recursive: true });

const viewports = [
  { name: 'light-desktop', width: 1440, height: 900 },
  { name: 'light-mobile', width: 390, height: 844 },
  { name: 'light-mobile-small', width: 320, height: 568 },
];

const executablePath = String(process.env.PRELAUNCH_CHROMIUM_EXECUTABLE_PATH || '').trim() || undefined;
const browser = await chromium.launch({ headless: true, executablePath });
const failures = [];
const evidence = [];

function check(condition, message, localFailures) {
  if (!condition) localFailures.push(message);
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

      const scene = await page.evaluate(() => {
        const background = document.querySelector('.prelaunch__background');
        const atmosphere = document.querySelector('.prelaunch__atmosphere');
        const mosaic = document.querySelector('[data-prelaunch-mosaic]');
        const tiles = [...document.querySelectorAll('[data-prelaunch-tile]')];
        const backgroundImage = background ? getComputedStyle(background, '::after') : null;
        const source = atmosphere ? getComputedStyle(atmosphere, '::before') : null;
        const atmosphereStyle = atmosphere ? getComputedStyle(atmosphere) : null;
        const mosaicStyle = mosaic ? getComputedStyle(mosaic) : null;
        const tileSurfaces = tiles.map((tile) => {
          const surface = getComputedStyle(tile, '::before');
          return {
            index: Number(tile.getAttribute('data-index')),
            glint: tile.getAttribute('data-glint') === 'true',
            backgroundImage: surface.backgroundImage,
            backgroundAttachment: surface.backgroundAttachment,
            backgroundSize: surface.backgroundSize,
            backgroundPosition: surface.backgroundPosition,
            backgroundColor: surface.backgroundColor,
            boxShadow: surface.boxShadow,
          };
        });
        const number = (value) => Number.parseFloat(String(value || '0')) || 0;
        const lightFields = tileSurfaces.map((tile) => [
          tile.backgroundImage,
          tile.backgroundAttachment,
          tile.backgroundSize,
          tile.backgroundPosition,
        ].join('|'));
        return {
          viewport: { width: window.innerWidth, height: window.innerHeight },
          layerZ: {
            atmosphere: number(atmosphereStyle?.zIndex),
            mosaic: number(mosaicStyle?.zIndex),
          },
          atmosphereBackground: atmosphereStyle?.backgroundImage || '',
          sourceBackground: source?.backgroundImage || '',
          sourceBorderWidth: number(source?.borderTopWidth),
          sourceWidth: number(source?.width),
          sourceRight: number(source?.right),
          sourceTop: number(source?.top),
          artworkWidth: number(backgroundImage?.width),
          artworkTop: number(backgroundImage?.top),
          artworkLeft: number(backgroundImage?.left),
          tileSurfaces,
          paneRadialCount: tileSurfaces.filter((tile) => tile.backgroundImage.includes('radial-gradient')).length,
          fixedPaneCount: tileSurfaces.filter((tile) => tile.backgroundAttachment.split(',')[0]?.trim() === 'fixed').length,
          uniquePaneLightFieldCount: new Set(lightFields).size,
          localGlintCount: tileSurfaces.filter((tile) => tile.glint).length,
        };
      });

      check(
        scene.layerZ.atmosphere < scene.layerZ.mosaic,
        `${viewport.name}: atmospheric source must sit behind glass (${scene.layerZ.atmosphere}/${scene.layerZ.mosaic})`,
        localFailures,
      );
      check(
        scene.atmosphereBackground.includes('radial-gradient'),
        `${viewport.name}: shared atmospheric radial source is missing`,
        localFailures,
      );
      check(
        scene.sourceBackground.includes('radial-gradient') && scene.sourceBorderWidth === 0,
        `${viewport.name}: source must be a broad radial emitter, not a ring`,
        localFailures,
      );
      check(
        scene.sourceWidth >= viewport.width * .65,
        `${viewport.name}: shared emitter is too small (${scene.sourceWidth}px)`,
        localFailures,
      );
      check(
        scene.sourceRight < 0 && scene.sourceTop < 0,
        `${viewport.name}: source is not outside the upper-right edge`,
        localFailures,
      );
      check(
        scene.paneRadialCount === 72 && scene.fixedPaneCount === 72,
        `${viewport.name}: pane light is not one viewport-anchored field (radial=${scene.paneRadialCount}, fixed=${scene.fixedPaneCount})`,
        localFailures,
      );
      check(
        scene.uniquePaneLightFieldCount === 1,
        `${viewport.name}: panes use ${scene.uniquePaneLightFieldCount} different light coordinate fields`,
        localFailures,
      );
      check(scene.localGlintCount === 3, `${viewport.name}: deterministic glint markers changed`, localFailures);

      if (viewport.width <= 599) {
        const widthRatio = scene.artworkWidth / viewport.width;
        const topRatio = scene.artworkTop / viewport.height;
        check(
          widthRatio >= 1.07 && widthRatio <= 1.17,
          `${viewport.name}: phone artwork width ratio ${widthRatio.toFixed(3)} is outside 1.07–1.17`,
          localFailures,
        );
        check(
          topRatio >= .16 && topRatio <= .27,
          `${viewport.name}: phone artwork top ratio ${topRatio.toFixed(3)} is outside .16–.27`,
          localFailures,
        );
      }

      const scenePath = resolve(artifactDir, `prelaunch-${viewport.name}-${viewport.width}x${viewport.height}-light-model.json`);
      writeFileSync(scenePath, `${JSON.stringify({ ...scene, failures: localFailures }, null, 2)}\n`);
      evidence.push({ viewport, scenePath, failures: localFailures });
    } catch (error) {
      localFailures.push(`${viewport.name}: light-model exception: ${String(error?.stack || error)}`);
    } finally {
      failures.push(...localFailures);
      await context.close();
    }
  }
} finally {
  await browser.close();
}

const summary = { ok: failures.length === 0, url, viewports, evidence, failures };
writeFileSync(resolve(artifactDir, 'prelaunch-light-model-summary.json'), `${JSON.stringify(summary, null, 2)}\n`);
console.log(JSON.stringify(summary, null, 2));
if (failures.length > 0) {
  throw new Error(`Prelaunch light-model gate failed after preserving evidence:\n- ${failures.join('\n- ')}`);
}
