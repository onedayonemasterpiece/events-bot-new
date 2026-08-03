import { mkdirSync, writeFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { chromium } from 'playwright';

function option(name, fallback = '') {
  const index = process.argv.indexOf(name);
  return index >= 0 ? String(process.argv[index + 1] || '') : fallback;
}

function invariant(condition, message) {
  if (!condition) throw new Error(message);
}

const url = option('--url');
const artifactDir = resolve(option('--artifact-dir', 'artifacts/prelaunch-browser'));
if (!url || !/^https?:\/\//u.test(url)) {
  throw new Error('Usage: check-prelaunch-browser.mjs --url <http(s) URL> [--artifact-dir <path>]');
}
mkdirSync(artifactDir, { recursive: true });

const viewports = [
  { name: 'reference-square', width: 1200, height: 1200 },
  { name: 'desktop', width: 1440, height: 900 },
  { name: 'mobile', width: 390, height: 844 },
];
const executablePath = String(process.env.PRELAUNCH_CHROMIUM_EXECUTABLE_PATH || '').trim() || undefined;
const browser = await chromium.launch({ headless: true, executablePath });
const reducedScenes = [];

try {
  for (const viewport of viewports) {
    const context = await browser.newContext({
      viewport: { width: viewport.width, height: viewport.height },
      reducedMotion: 'reduce',
      colorScheme: 'dark',
    });
    const page = await context.newPage();
    const response = await page.goto(url, { waitUntil: 'networkidle' });
    invariant(response?.ok(), `${viewport.name}: HTTP ${response?.status() || 'unknown'}`);
    await page.locator('[data-prelaunch-page]').waitFor({ state: 'visible' });

    const scene = await page.evaluate(() => {
      const root = document.querySelector('[data-prelaunch-page]');
      const projection = document.querySelector('.prelaunch__projection');
      const mosaic = document.querySelector('[data-prelaunch-mosaic]');
      const atmosphere = document.querySelector('.prelaunch__atmosphere');
      const foreground = document.querySelector('.prelaunch__foreground');
      const heading = document.querySelector('#prelaunch-title');
      const tiles = [...document.querySelectorAll('[data-prelaunch-tile]')];
      const email = document.querySelector('input[type="email"]');
      const button = document.querySelector('[data-prelaunch-submit]');
      const counts = tiles.reduce((out, tile) => {
        const state = tile.getAttribute('data-state') || 'missing';
        out[state] = (out[state] || 0) + 1;
        return out;
      }, {});
      const alpha = (value) => {
        const match = /rgba?\([^/]*?(?:,|\s)\s*([\d.]+)\s*\)$/u.exec(value);
        if (!match) return value === 'transparent' ? 0 : 1;
        return Number(match[1]);
      };
      const stateSurface = Object.fromEntries(['sealed', 'dim', 'revealed'].map((state) => {
        const tile = tiles.find((candidate) => candidate.getAttribute('data-state') === state);
        if (!(tile instanceof HTMLElement)) return [state, null];
        const base = getComputedStyle(tile);
        const glass = getComputedStyle(tile, '::before');
        const matte = getComputedStyle(tile, '::after');
        return [state, {
          tileOpacity: Number(base.opacity),
          tileBackground: base.backgroundColor,
          glassBackground: glass.backgroundColor,
          glassAlpha: alpha(glass.backgroundColor),
          backdropFilter: glass.backdropFilter || glass.webkitBackdropFilter || '',
          borderColor: glass.borderColor,
          boxShadow: glass.boxShadow,
          matteOpacity: Number(matte.opacity),
        }];
      }));
      const first = tiles[0]?.getBoundingClientRect();
      const second = tiles[1]?.getBoundingClientRect();
      const nextRow = tiles[9]?.getBoundingClientRect();
      const gap = first && second ? second.left - first.right : -1;
      const verticalGap = first && nextRow ? nextRow.top - first.bottom : -1;
      const tileAspect = first && first.width > 0 ? first.height / first.width : 0;
      const radii = [...new Set(tiles.map((tile) => getComputedStyle(tile).borderRadius))];
      const image = projection instanceof HTMLImageElement
        ? { complete: projection.complete, naturalWidth: projection.naturalWidth }
        : { complete: false, naturalWidth: 0 };
      const headingRect = heading?.getBoundingClientRect();
      const mosaicSeam = mosaic ? getComputedStyle(mosaic, '::before').backgroundImage : '';
      return {
        layers: [root, projection, mosaic, atmosphere, foreground].every(Boolean),
        tileCount: tiles.length,
        counts,
        glintCount: tiles.filter((tile) => tile.getAttribute('data-glint') === 'true').length,
        zoneCount: new Set(tiles.map((tile) => tile.getAttribute('data-zone'))).size,
        radiiCount: radii.length,
        image,
        overflow: document.documentElement.scrollWidth - document.documentElement.clientWidth,
        inputWidth: email?.getBoundingClientRect().width || 0,
        buttonWidth: button?.getBoundingClientRect().width || 0,
        heading: headingRect ? {
          top: headingRect.top,
          left: headingRect.left,
          right: headingRect.right,
          bottom: headingRect.bottom,
        } : null,
        projection: projection?.getBoundingClientRect ? (() => {
          const rect = projection.getBoundingClientRect();
          return { top: rect.top, left: rect.left, width: rect.width, height: rect.height };
        })() : null,
        firstTile: first ? { top: first.top, left: first.left, right: first.right, bottom: first.bottom, width: first.width, height: first.height } : null,
        gap,
        verticalGap,
        tileAspect,
        mosaicSeam,
        stateSurface,
        stateSnapshot: tiles.map((tile) => tile.getAttribute('data-state')),
      };
    });

    invariant(scene.layers, `${viewport.name}: one or more scene layers are missing`);
    invariant(scene.tileCount === 72, `${viewport.name}: expected 72 tiles, got ${scene.tileCount}`);
    invariant(
      scene.counts.sealed === 30 && scene.counts.dim === 23 && scene.counts.revealed === 19,
      `${viewport.name}: deterministic glass scene changed ${JSON.stringify(scene.counts)}`,
    );
    invariant(scene.glintCount === 3, `${viewport.name}: expected three directed-light glints, got ${scene.glintCount}`);
    invariant(scene.zoneCount === 3, `${viewport.name}: expected quiet/light/leather motion zones`);
    invariant(scene.radiiCount >= 3, `${viewport.name}: tile radii lost deliberate micro-variation`);
    invariant(scene.image.complete && scene.image.naturalWidth >= 512, `${viewport.name}: projection did not decode`);
    invariant(scene.overflow <= 1, `${viewport.name}: horizontal overflow ${scene.overflow}px`);
    invariant(scene.inputWidth >= 180 && scene.buttonWidth >= 150, `${viewport.name}: form control geometry collapsed`);
    invariant(scene.heading && scene.heading.left >= 0 && scene.heading.top >= 0, `${viewport.name}: heading escaped viewport`);
    invariant(scene.gap >= 6 && scene.gap <= 16, `${viewport.name}: horizontal inter-tile seam is ${scene.gap}px`);
    invariant(scene.verticalGap >= 6 && scene.verticalGap <= 16, `${viewport.name}: vertical inter-tile seam is ${scene.verticalGap}px`);
    invariant(scene.tileAspect >= 1.04 && scene.tileAspect <= 1.14, `${viewport.name}: tile aspect ${scene.tileAspect} drifted from the reference`);
    invariant(
      scene.mosaicSeam.includes('repeating-linear-gradient'),
      `${viewport.name}: mosaic no longer paints an opaque seam above the projection`,
    );

    const sealed = scene.stateSurface.sealed;
    const dim = scene.stateSurface.dim;
    const revealed = scene.stateSurface.revealed;
    invariant(sealed && dim && revealed, `${viewport.name}: missing one glass surface state`);
    for (const [state, surface] of Object.entries(scene.stateSurface)) {
      invariant(surface.tileOpacity === 1, `${viewport.name}: ${state} fades the whole tile and exposes the seam`);
      invariant(
        surface.tileBackground === 'rgba(0, 0, 0, 0)' || surface.tileBackground === 'transparent',
        `${viewport.name}: ${state} tile base must remain transparent for backdrop glass`,
      );
      invariant(
        surface.backdropFilter && surface.backdropFilter !== 'none',
        `${viewport.name}: ${state} has no backdrop-filter`,
      );
      invariant(surface.boxShadow && surface.boxShadow !== 'none', `${viewport.name}: ${state} has no bevel/depth`);
    }
    invariant(
      sealed.glassAlpha > dim.glassAlpha && dim.glassAlpha > revealed.glassAlpha,
      `${viewport.name}: glass alpha order is not sealed > dim > revealed`,
    );
    invariant(
      new Set([sealed.backdropFilter, dim.backdropFilter, revealed.backdropFilter]).size === 3,
      `${viewport.name}: glass states lost distinct matte/blur treatments`,
    );

    if (viewport.name === 'reference-square') {
      invariant(scene.firstTile && scene.firstTile.right >= 160 && scene.firstTile.right <= 180, `reference-square: first seam x=${scene.firstTile?.right}`);
      invariant(scene.firstTile && scene.firstTile.bottom >= 100 && scene.firstTile.bottom <= 130, `reference-square: first seam y=${scene.firstTile?.bottom}`);
      invariant(scene.heading.left >= 110 && scene.heading.left <= 140, `reference-square: heading left=${scene.heading.left}`);
      invariant(scene.heading.top >= 300 && scene.heading.top <= 335, `reference-square: heading top=${scene.heading.top}`);
      invariant(scene.projection && scene.projection.width >= 1180 && scene.projection.width <= 1220, `reference-square: projection width=${scene.projection?.width}`);
      invariant(scene.projection && Math.abs(scene.projection.top) <= 2, `reference-square: projection top=${scene.projection?.top}`);
    }

    await page.waitForTimeout(4200);
    const stateAfter = await page.locator('[data-prelaunch-tile]').evaluateAll((nodes) => (
      nodes.map((node) => node.getAttribute('data-state'))
    ));
    invariant(
      JSON.stringify(stateAfter) === JSON.stringify(scene.stateSnapshot),
      `${viewport.name}: reduced-motion tile state changed`,
    );

    const screenshotPath = resolve(artifactDir, `prelaunch-${viewport.name}-${viewport.width}x${viewport.height}.png`);
    const domPath = resolve(artifactDir, `prelaunch-${viewport.name}-${viewport.width}x${viewport.height}-dom.html`);
    const scenePath = resolve(artifactDir, `prelaunch-${viewport.name}-${viewport.width}x${viewport.height}-scene.json`);
    await page.screenshot({ path: screenshotPath, fullPage: true, animations: 'disabled' });
    writeFileSync(domPath, await page.content());
    writeFileSync(scenePath, `${JSON.stringify({ viewport, ...scene }, null, 2)}\n`);
    reducedScenes.push({ viewport, screenshotPath, domPath, scenePath });
    await context.close();
  }

  const motionContext = await browser.newContext({
    viewport: { width: 1200, height: 1200 },
    reducedMotion: 'no-preference',
    colorScheme: 'dark',
  });
  const motionPage = await motionContext.newPage();
  const motionResponse = await motionPage.goto(url, { waitUntil: 'networkidle' });
  invariant(motionResponse?.ok(), `motion: HTTP ${motionResponse?.status() || 'unknown'}`);
  await motionPage.locator('[data-prelaunch-page]').waitFor({ state: 'visible' });
  const before = await motionPage.locator('[data-prelaunch-tile]').evaluateAll((nodes) => (
    nodes.map((node) => node.getAttribute('data-state'))
  ));
  await motionPage.waitForFunction((initial) => {
    const current = [...document.querySelectorAll('[data-prelaunch-tile]')]
      .map((node) => node.getAttribute('data-state'));
    return current.some((state, index) => state !== initial[index]);
  }, before, { timeout: 8000 });
  const motionEvidence = await motionPage.locator('[data-prelaunch-tile]').evaluateAll((nodes, initial) => {
    const current = nodes.map((node) => node.getAttribute('data-state'));
    return {
      changedIndices: current.flatMap((state, index) => state !== initial[index] ? [index] : []),
      tileOpacity: nodes.map((node) => Number(getComputedStyle(node).opacity)),
      seamBackground: getComputedStyle(document.querySelector('[data-prelaunch-mosaic]'), '::before').backgroundImage,
      current,
    };
  }, before);
  invariant(
    motionEvidence.changedIndices.length >= 2 && motionEvidence.changedIndices.length <= 5,
    `motion: expected one sparse group of 2–5 tiles, changed ${motionEvidence.changedIndices.length}`,
  );
  invariant(motionEvidence.tileOpacity.every((value) => value === 1), 'motion: whole-tile opacity changed');
  invariant(
    motionEvidence.seamBackground.includes('repeating-linear-gradient'),
    'motion: opaque seam disappeared during state transition',
  );
  writeFileSync(resolve(artifactDir, 'prelaunch-motion-scene.json'), `${JSON.stringify({ before, ...motionEvidence }, null, 2)}\n`);
  await motionContext.close();
} finally {
  await browser.close();
}

console.log(JSON.stringify({
  ok: true,
  url,
  viewports: viewports.map(({ name, width, height }) => ({ name, width, height })),
  artifactDir,
  artifactFilesPerViewport: ['screenshot', 'dom', 'scene-json'],
  reducedScenes,
}, null, 2));
