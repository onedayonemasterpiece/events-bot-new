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

function alphaFromColor(value) {
  if (value === 'transparent') return 0;
  const match = /rgba\([^)]*[,\s]([\d.]+)\s*\)$/u.exec(value);
  return match ? Number(match[1]) : 1;
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
      await page.waitForTimeout(250);

      const scene = await page.evaluate(() => {
        const root = document.querySelector('[data-prelaunch-page]');
        const background = document.querySelector('.prelaunch__background');
        const atmosphere = document.querySelector('.prelaunch__atmosphere');
        const mosaic = document.querySelector('[data-prelaunch-mosaic]');
        const tiles = [...document.querySelectorAll('[data-prelaunch-tile]')];
        const rootBefore = root ? getComputedStyle(root, '::before') : null;
        const rootAfter = root ? getComputedStyle(root, '::after') : null;
        const backgroundImage = background ? getComputedStyle(background, '::after') : null;
        const atmosphereStyle = atmosphere ? getComputedStyle(atmosphere) : null;
        const mosaicStyle = mosaic ? getComputedStyle(mosaic) : null;
        const number = (value) => Number.parseFloat(String(value || '0')) || 0;
        const gridColumns = String(mosaicStyle?.gridTemplateColumns || '')
          .split(/\s+/u)
          .filter(Boolean);
        const tileSurfaces = tiles.map((tile) => {
          const base = getComputedStyle(tile);
          const surface = getComputedStyle(tile, '::before');
          return {
            index: Number(tile.getAttribute('data-index')),
            state: tile.getAttribute('data-state'),
            edge: tile.getAttribute('data-edge'),
            accent: tile.getAttribute('data-accent') === 'true',
            window: tile.getAttribute('data-window') === 'true',
            baseOverflow: base.overflow,
            baseRadius: base.borderRadius,
            backgroundImage: surface.backgroundImage,
            backgroundAttachment: surface.backgroundAttachment,
            backgroundSize: surface.backgroundSize,
            backgroundPosition: surface.backgroundPosition,
            backgroundColor: surface.backgroundColor,
            backdropFilter: surface.backdropFilter || surface.webkitBackdropFilter || '',
            borderRadius: surface.borderRadius,
            boxShadow: surface.boxShadow,
            transitionProperty: surface.transitionProperty,
            willChange: surface.willChange,
          };
        });
        const edgeBands = [...new Set(tileSurfaces.map((tile) => tile.edge).filter(Boolean))];
        return {
          viewport: { width: window.innerWidth, height: window.innerHeight },
          layerZ: {
            sharedSource: number(rootBefore?.zIndex),
            mosaic: number(mosaicStyle?.zIndex),
            atmosphere: number(atmosphereStyle?.zIndex),
          },
          sharedSourceBackground: rootBefore?.backgroundImage || '',
          sharedSourceBlend: rootBefore?.mixBlendMode || '',
          dustBackground: rootAfter?.backgroundImage || '',
          dustBlend: rootAfter?.mixBlendMode || '',
          atmosphereBackground: atmosphereStyle?.backgroundImage || '',
          artworkWidth: number(backgroundImage?.width),
          artworkTop: number(backgroundImage?.top),
          artworkLeft: number(backgroundImage?.left),
          gridColumnCount: gridColumns.length,
          tileSurfaces,
          paneRadialCount: tileSurfaces.filter((tile) => tile.backgroundImage.includes('radial-gradient')).length,
          fixedPaneCount: tileSurfaces.filter((tile) => tile.backgroundAttachment.split(',')[0]?.trim() === 'fixed').length,
          paneBackdropCount: tileSurfaces.filter((tile) => tile.backdropFilter && tile.backdropFilter !== 'none').length,
          windowCount: tileSurfaces.filter((tile) => tile.window).length,
          accentCount: tileSurfaces.filter((tile) => tile.accent).length,
          edgeBands,
          uniqueEdgeShadowCount: new Set(tileSurfaces.map((tile) => tile.boxShadow)).size,
          cornerMaskCount: tileSurfaces.filter((tile) => (
            tile.baseOverflow === 'hidden'
            && tile.baseRadius === '0px'
            && tile.borderRadius !== '0px'
            && tile.boxShadow.includes('rgb(7, 9, 13)')
          )).length,
          clearWindowCount: tileSurfaces.filter((tile) => alphaFromColor(tile.backgroundColor) <= .4).length,
          transitionFilterCount: tileSurfaces.filter((tile) => tile.transitionProperty.includes('backdrop-filter')).length,
          paneWillChangeCount: tileSurfaces.filter((tile) => tile.willChange && tile.willChange !== 'auto').length,
        };
      });

      check(
        scene.layerZ.sharedSource < scene.layerZ.mosaic && scene.layerZ.mosaic < scene.layerZ.atmosphere,
        `${viewport.name}: expected shared source < mosaic < accents (${JSON.stringify(scene.layerZ)})`,
        localFailures,
      );
      check(
        scene.sharedSourceBackground.includes('radial-gradient'),
        `${viewport.name}: shared upper-right radial emitter is missing`,
        localFailures,
      );
      check(
        scene.sharedSourceBlend === 'screen',
        `${viewport.name}: shared emitter blend=${scene.sharedSourceBlend}`,
        localFailures,
      );
      check(
        (scene.dustBackground.match(/radial-gradient/gu) || []).length >= 12 && scene.dustBlend === 'screen',
        `${viewport.name}: golden powder overlay is missing or too sparse`,
        localFailures,
      );
      check(
        !scene.atmosphereBackground || scene.atmosphereBackground === 'none',
        `${viewport.name}: legacy atmosphere still paints a second full-scene wash`,
        localFailures,
      );
      check(
        scene.paneRadialCount === 0 && scene.fixedPaneCount === 0,
        `${viewport.name}: a pane paints its own spotlight (radial=${scene.paneRadialCount}, fixed=${scene.fixedPaneCount})`,
        localFailures,
      );
      check(
        scene.paneBackdropCount === 72,
        `${viewport.name}: all panes must transmit the shared source (${scene.paneBackdropCount}/72)`,
        localFailures,
      );
      check(scene.cornerMaskCount === 72, `${viewport.name}: rounded corner masks ${scene.cornerMaskCount}/72`, localFailures);
      check(scene.windowCount === 8, `${viewport.name}: coherent reveal windows=${scene.windowCount}`, localFailures);
      check(scene.accentCount === 3, `${viewport.name}: edge accents=${scene.accentCount}`, localFailures);
      check(
        ['ambient', 'soft', 'warm', 'hot'].every((edge) => scene.edgeBands.includes(edge)),
        `${viewport.name}: incomplete edge-response bands ${scene.edgeBands.join(',')}`,
        localFailures,
      );
      check(
        scene.uniqueEdgeShadowCount >= 4,
        `${viewport.name}: edge lighting lacks variation (${scene.uniqueEdgeShadowCount} shadow treatments)`,
        localFailures,
      );
      check(
        scene.clearWindowCount >= 6 && scene.clearWindowCount <= 10,
        `${viewport.name}: genuinely clear windows=${scene.clearWindowCount}`,
        localFailures,
      );
      check(scene.transitionFilterCount === 0, `${viewport.name}: blur is animated on ${scene.transitionFilterCount} panes`, localFailures);
      check(scene.paneWillChangeCount === 0, `${viewport.name}: ${scene.paneWillChangeCount} panes reserve compositor layers`, localFailures);

      if (viewport.width <= 599) {
        const widthRatio = scene.artworkWidth / viewport.width;
        const topRatio = scene.artworkTop / viewport.height;
        check(scene.gridColumnCount === 6, `${viewport.name}: mobile grid columns=${scene.gridColumnCount}`, localFailures);
        check(
          widthRatio >= .82 && widthRatio <= .96,
          `${viewport.name}: mobile artwork width ratio ${widthRatio.toFixed(3)} is outside .82–.96`,
          localFailures,
        );
        check(
          topRatio >= .31 && topRatio <= .46,
          `${viewport.name}: mobile artwork top ratio ${topRatio.toFixed(3)} is outside .31–.46`,
          localFailures,
        );
      } else {
        check(scene.gridColumnCount === 9, `${viewport.name}: desktop grid columns=${scene.gridColumnCount}`, localFailures);
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
