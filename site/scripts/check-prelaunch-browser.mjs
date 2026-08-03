import { mkdirSync } from 'node:fs';
import { resolve } from 'node:path';
import { chromium } from 'playwright';

function option(name, fallback = '') {
  const index = process.argv.indexOf(name);
  return index >= 0 ? String(process.argv[index + 1] || '') : fallback;
}

const url = option('--url');
const artifactDir = resolve(option('--artifact-dir', 'artifacts/prelaunch-browser'));
if (!url || !/^https?:\/\//u.test(url)) {
  throw new Error('Usage: check-prelaunch-browser.mjs --url <http(s) URL> [--artifact-dir <path>]');
}
mkdirSync(artifactDir, { recursive: true });

const viewports = [
  { name: 'desktop', width: 1440, height: 900 },
  { name: 'mobile', width: 390, height: 844 },
];
const executablePath = String(process.env.PRELAUNCH_CHROMIUM_EXECUTABLE_PATH || '').trim() || undefined;
const browser = await chromium.launch({ headless: true, executablePath });

try {
  for (const viewport of viewports) {
    const context = await browser.newContext({
      viewport: { width: viewport.width, height: viewport.height },
      reducedMotion: 'reduce',
      colorScheme: 'dark',
    });
    const page = await context.newPage();
    const response = await page.goto(url, { waitUntil: 'networkidle' });
    if (!response?.ok()) throw new Error(`${viewport.name}: HTTP ${response?.status() || 'unknown'}`);
    await page.locator('[data-prelaunch-page]').waitFor({ state: 'visible' });

    const result = await page.evaluate(() => {
      const root = document.querySelector('[data-prelaunch-page]');
      const projection = document.querySelector('.prelaunch__projection');
      const mosaic = document.querySelector('[data-prelaunch-mosaic]');
      const atmosphere = document.querySelector('.prelaunch__atmosphere');
      const foreground = document.querySelector('.prelaunch__foreground');
      const tiles = [...document.querySelectorAll('[data-prelaunch-tile]')];
      const states = new Set(tiles.map((tile) => tile.getAttribute('data-state')));
      const email = document.querySelector('input[type="email"]');
      const button = document.querySelector('[data-prelaunch-submit]');
      const image = projection instanceof HTMLImageElement
        ? { complete: projection.complete, naturalWidth: projection.naturalWidth }
        : { complete: false, naturalWidth: 0 };
      const opacityBefore = tiles.map((tile) => getComputedStyle(tile).opacity);
      return {
        layers: [root, projection, mosaic, atmosphere, foreground].every(Boolean),
        tileCount: tiles.length,
        states: [...states].sort(),
        image,
        overflow: document.documentElement.scrollWidth - document.documentElement.clientWidth,
        inputWidth: email?.getBoundingClientRect().width || 0,
        buttonWidth: button?.getBoundingClientRect().width || 0,
        opacityBefore,
      };
    });

    if (!result.layers) throw new Error(`${viewport.name}: one or more scene layers are missing`);
    if (result.tileCount !== 72) throw new Error(`${viewport.name}: expected 72 tiles, got ${result.tileCount}`);
    if (result.states.join(',') !== 'dim,revealed,sealed') {
      throw new Error(`${viewport.name}: expected three tile states, got ${result.states.join(',')}`);
    }
    if (!result.image.complete || result.image.naturalWidth < 512) throw new Error(`${viewport.name}: projection did not decode`);
    if (result.overflow > 1) throw new Error(`${viewport.name}: horizontal overflow ${result.overflow}px`);
    if (result.inputWidth < 180 || result.buttonWidth < 150) throw new Error(`${viewport.name}: form control geometry collapsed`);

    await page.waitForTimeout(4200);
    const opacityAfter = await page.locator('[data-prelaunch-tile]').evaluateAll((nodes) => (
      nodes.map((node) => getComputedStyle(node).opacity)
    ));
    if (JSON.stringify(opacityAfter) !== JSON.stringify(result.opacityBefore)) {
      throw new Error(`${viewport.name}: reduced-motion tile state changed`);
    }

    await page.screenshot({
      path: resolve(artifactDir, `prelaunch-${viewport.name}-${viewport.width}x${viewport.height}.png`),
      fullPage: true,
    });
    await context.close();
  }
} finally {
  await browser.close();
}

console.log(JSON.stringify({ ok: true, url, viewports: viewports.map(({ name, width, height }) => ({ name, width, height })), artifactDir }, null, 2));
