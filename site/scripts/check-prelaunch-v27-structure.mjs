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
const artifactDir = resolve(option('--artifact-dir', 'artifacts/prelaunch-v27'));
if (!url || !/^https?:\/\//u.test(url)) throw new Error('Missing --url');
mkdirSync(artifactDir, { recursive: true });

const viewports = [
  { name: 'reference-square', width: 1200, height: 1200, columns: 9 },
  { name: 'visual-wide', width: 1728, height: 900, columns: 14 },
  { name: 'mobile', width: 390, height: 844, columns: 7 },
  { name: 'mobile-small', width: 320, height: 568, columns: 7 },
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
    });
    try {
      const page = await context.newPage();
      const response = await page.goto(url, { waitUntil: 'networkidle' });
      check(response?.ok(), `${viewport.name}: HTTP ${response?.status()}`, localFailures);
      await page.waitForFunction(() => {
        const root = document.querySelector('[data-prelaunch-page]');
        return root?.getAttribute('data-artwork-ready') === 'true'
          && root?.getAttribute('data-tile-pool-count') === '98'
          && Number(root?.getAttribute('data-window-map-count') || 0) >= 8;
      }, undefined, { timeout: 10_000 });
      await page.waitForTimeout(250);

      const scene = await page.evaluate(() => {
        const root = document.querySelector('[data-prelaunch-page]');
        const artwork = document.querySelector('.prelaunch__artwork');
        const mosaic = document.querySelector('[data-prelaunch-mosaic]');
        const tiles = [...document.querySelectorAll('[data-prelaunch-tile]')];
        const first = tiles[0];
        const firstRect = first?.getBoundingClientRect();
        const artworkRect = artwork?.getBoundingClientRect();
        const mosaicStyle = mosaic ? getComputedStyle(mosaic) : null;
        const seamStyle = mosaic ? getComputedStyle(mosaic, '::before') : null;
        const tileStyle = first ? getComputedStyle(first) : null;
        const glassStyle = first ? getComputedStyle(first, '::before') : null;
        const scrolling = document.scrollingElement || document.documentElement;
        const gridColumns = String(mosaicStyle?.gridTemplateColumns || '')
          .split(/\s+/u)
          .filter(Boolean).length;
        const clipRect = artwork?.querySelector('clipPath rect');
        return {
          viewport: { width: innerWidth, height: innerHeight },
          artworkTag: artwork?.tagName || '',
          artworkBound: root?.getAttribute('data-transparent-artwork-bound'),
          artworkRect: artworkRect ? {
            left: artworkRect.left,
            top: artworkRect.top,
            width: artworkRect.width,
            height: artworkRect.height,
            bottom: artworkRect.bottom,
          } : null,
          clipRect: clipRect ? {
            x: clipRect.getAttribute('x'),
            y: clipRect.getAttribute('y'),
            width: clipRect.getAttribute('width'),
            height: clipRect.getAttribute('height'),
            rx: clipRect.getAttribute('rx'),
          } : null,
          tileCount: tiles.length,
          gridColumns,
          tileWidth: firstRect?.width || 0,
          tileHeight: firstRect?.height || 0,
          tileOverflow: tileStyle?.overflow || '',
          tileBorder: tileStyle?.border || '',
          tileOutline: tileStyle?.outlineStyle || '',
          tileBackgroundImage: tileStyle?.backgroundImage || '',
          glassRadius: glassStyle?.borderRadius || '',
          glassShadow: glassStyle?.boxShadow || '',
          seamFilter: seamStyle?.filter || '',
          seamShadow: seamStyle?.boxShadow || '',
          verticalOverflow: Math.max(scrolling.scrollHeight, document.documentElement.scrollHeight) - innerHeight,
          horizontalOverflow: Math.max(scrolling.scrollWidth, document.documentElement.scrollWidth) - innerWidth,
        };
      });

      check(scene.artworkTag.toLowerCase() === 'svg', `${viewport.name}: artwork tag ${scene.artworkTag}`, localFailures);
      check(scene.artworkBound === 'svg-rounded-clip', `${viewport.name}: artwork pipeline ${scene.artworkBound}`, localFailures);
      check(scene.clipRect?.rx === '175', `${viewport.name}: rounded artwork mask missing`, localFailures);
      check(scene.tileCount === 98, `${viewport.name}: tile pool ${scene.tileCount}`, localFailures);
      check(scene.gridColumns === viewport.columns, `${viewport.name}: columns ${scene.gridColumns}`, localFailures);
      check(Math.abs(scene.tileWidth - scene.tileHeight) <= 1, `${viewport.name}: non-square tile`, localFailures);
      check(scene.artworkRect && scene.tileWidth / scene.artworkRect.width >= .155 && scene.tileWidth / scene.artworkRect.width <= .19,
        `${viewport.name}: tile/artwork ratio ${(scene.tileWidth / Math.max(1, scene.artworkRect?.width || 1)).toFixed(3)}`, localFailures);
      check(scene.tileOverflow === 'hidden', `${viewport.name}: tile overflow ${scene.tileOverflow}`, localFailures);
      check(scene.tileOutline === 'none', `${viewport.name}: square outline ${scene.tileOutline}`, localFailures);
      check((scene.tileBackgroundImage.match(/radial-gradient/gu) || []).length === 4,
        `${viewport.name}: corner masks are not exactly four`, localFailures);
      check(scene.glassRadius !== '0px', `${viewport.name}: glass radius missing`, localFailures);
      check(!scene.glassShadow.includes('rgb(7, 9, 13) 0px 0px 0px'), `${viewport.name}: spread seam shadow remains`, localFailures);
      check(scene.seamFilter === 'none', `${viewport.name}: square gap drop-shadow ${scene.seamFilter}`, localFailures);
      check(scene.seamShadow === 'none', `${viewport.name}: square gap box-shadow ${scene.seamShadow}`, localFailures);
      check(scene.verticalOverflow <= 1, `${viewport.name}: vertical overflow ${scene.verticalOverflow}`, localFailures);
      check(scene.horizontalOverflow <= 1, `${viewport.name}: horizontal overflow ${scene.horizontalOverflow}`, localFailures);

      const screenshotPath = resolve(artifactDir, `prelaunch-${viewport.name}-${viewport.width}x${viewport.height}.png`);
      await page.screenshot({ path: screenshotPath, fullPage: false, animations: 'disabled' });
      const result = { viewport, scene, failures: localFailures, screenshotPath };
      evidence.push(result);
      writeFileSync(resolve(artifactDir, `prelaunch-${viewport.name}-${viewport.width}x${viewport.height}.json`), `${JSON.stringify(result, null, 2)}\n`);
    } catch (error) {
      localFailures.push(`${viewport.name}: ${String(error?.stack || error)}`);
    } finally {
      failures.push(...localFailures);
      await context.close();
    }
  }
} finally {
  await browser.close();
}

const summary = {
  schema_version: 'prelaunch_v27_structure_v1',
  ok: failures.length === 0,
  url,
  evidence,
  failures,
};
writeFileSync(resolve(artifactDir, 'prelaunch-v27-structure-summary.json'), `${JSON.stringify(summary, null, 2)}\n`);
console.log(JSON.stringify(summary, null, 2));
if (failures.length) throw new Error(failures.join('\n'));
