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
const failures = [];
const reducedScenes = [];

function check(condition, message, bucket = failures) {
  if (!condition) bucket.push(message);
}

function alphaFromColor(value) {
  if (value === 'transparent') return 0;
  const match = /rgba\([^)]*[,\s]([\d.]+)\s*\)$/u.exec(value);
  return match ? Number(match[1]) : 1;
}

async function readScene(page) {
  return page.evaluate(() => {
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
    const rect = projection?.getBoundingClientRect?.();
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
      projection: rect ? { top: rect.top, left: rect.left, width: rect.width, height: rect.height } : null,
      firstTile: first ? {
        top: first.top,
        left: first.left,
        right: first.right,
        bottom: first.bottom,
        width: first.width,
        height: first.height,
      } : null,
      gap,
      verticalGap,
      tileAspect,
      mosaicSeam,
      stateSurface,
      stateSnapshot: tiles.map((tile) => tile.getAttribute('data-state')),
    };
  });
}

function validateScene(scene, viewport, localFailures) {
  const prefix = viewport.name;
  check(scene.layers, `${prefix}: one or more scene layers are missing`, localFailures);
  check(scene.tileCount === 72, `${prefix}: expected 72 tiles, got ${scene.tileCount}`, localFailures);
  check(
    scene.counts.sealed === 30 && scene.counts.dim === 23 && scene.counts.revealed === 19,
    `${prefix}: deterministic glass scene changed ${JSON.stringify(scene.counts)}`,
    localFailures,
  );
  check(scene.glintCount === 3, `${prefix}: expected three directed-light glints, got ${scene.glintCount}`, localFailures);
  check(scene.zoneCount === 3, `${prefix}: expected quiet/light/leather motion zones`, localFailures);
  check(scene.radiiCount >= 3, `${prefix}: tile radii lost deliberate micro-variation`, localFailures);
  check(scene.image.complete && scene.image.naturalWidth >= 512, `${prefix}: projection did not decode`, localFailures);
  check(scene.overflow <= 1, `${prefix}: horizontal overflow ${scene.overflow}px`, localFailures);
  check(scene.inputWidth >= 180 && scene.buttonWidth >= 150, `${prefix}: form control geometry collapsed`, localFailures);
  check(scene.heading && scene.heading.left >= 0 && scene.heading.top >= 0, `${prefix}: heading escaped viewport`, localFailures);
  check(scene.gap >= 6 && scene.gap <= 16, `${prefix}: horizontal inter-tile seam is ${scene.gap}px`, localFailures);
  check(scene.verticalGap >= 6 && scene.verticalGap <= 16, `${prefix}: vertical inter-tile seam is ${scene.verticalGap}px`, localFailures);
  check(scene.tileAspect >= 1.04 && scene.tileAspect <= 1.14, `${prefix}: tile aspect ${scene.tileAspect} drifted from the reference`, localFailures);
  check(
    scene.mosaicSeam.includes('repeating-linear-gradient'),
    `${prefix}: mosaic no longer paints an opaque seam above the projection`,
    localFailures,
  );

  const sealed = scene.stateSurface.sealed;
  const dim = scene.stateSurface.dim;
  const revealed = scene.stateSurface.revealed;
  check(sealed && dim && revealed, `${prefix}: missing one glass surface state`, localFailures);
  for (const [state, surface] of Object.entries(scene.stateSurface)) {
    if (!surface) continue;
    surface.glassAlpha = alphaFromColor(surface.glassBackground);
    check(surface.tileOpacity === 1, `${prefix}: ${state} fades the whole tile and exposes the seam`, localFailures);
    check(
      surface.tileBackground === 'rgba(0, 0, 0, 0)' || surface.tileBackground === 'transparent',
      `${prefix}: ${state} tile base must remain transparent for backdrop glass`,
      localFailures,
    );
    check(surface.backdropFilter && surface.backdropFilter !== 'none', `${prefix}: ${state} has no backdrop-filter`, localFailures);
    check(surface.boxShadow && surface.boxShadow !== 'none', `${prefix}: ${state} has no bevel/depth`, localFailures);
  }
  if (sealed && dim && revealed) {
    check(
      sealed.glassAlpha > dim.glassAlpha && dim.glassAlpha > revealed.glassAlpha,
      `${prefix}: glass alpha order is not sealed > dim > revealed`,
      localFailures,
    );
    check(
      new Set([sealed.backdropFilter, dim.backdropFilter, revealed.backdropFilter]).size === 3,
      `${prefix}: glass states lost distinct matte/blur treatments`,
      localFailures,
    );
  }

  if (viewport.name === 'reference-square') {
    check(scene.firstTile && scene.firstTile.right >= 160 && scene.firstTile.right <= 180, `reference-square: first seam x=${scene.firstTile?.right}`, localFailures);
    check(scene.firstTile && scene.firstTile.bottom >= 100 && scene.firstTile.bottom <= 130, `reference-square: first seam y=${scene.firstTile?.bottom}`, localFailures);
    check(scene.heading && scene.heading.left >= 110 && scene.heading.left <= 140, `reference-square: heading left=${scene.heading?.left}`, localFailures);
    check(scene.heading && scene.heading.top >= 300 && scene.heading.top <= 335, `reference-square: heading top=${scene.heading?.top}`, localFailures);
    check(scene.projection && scene.projection.width >= 1180 && scene.projection.width <= 1220, `reference-square: projection width=${scene.projection?.width}`, localFailures);
    check(scene.projection && Math.abs(scene.projection.top) <= 2, `reference-square: projection top=${scene.projection?.top}`, localFailures);
  }
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
      const scene = await readScene(page);

      await page.waitForTimeout(4200);
      const stateAfter = await page.locator('[data-prelaunch-tile]').evaluateAll((nodes) => (
        nodes.map((node) => node.getAttribute('data-state'))
      ));
      check(
        JSON.stringify(stateAfter) === JSON.stringify(scene.stateSnapshot),
        `${viewport.name}: reduced-motion tile state changed`,
        localFailures,
      );

      validateScene(scene, viewport, localFailures);
      const screenshotPath = resolve(artifactDir, `prelaunch-${viewport.name}-${viewport.width}x${viewport.height}.png`);
      const domPath = resolve(artifactDir, `prelaunch-${viewport.name}-${viewport.width}x${viewport.height}-dom.html`);
      const scenePath = resolve(artifactDir, `prelaunch-${viewport.name}-${viewport.width}x${viewport.height}-scene.json`);
      await page.screenshot({ path: screenshotPath, fullPage: true, animations: 'disabled' });
      writeFileSync(domPath, await page.content());
      writeFileSync(scenePath, `${JSON.stringify({ viewport, failures: localFailures, ...scene }, null, 2)}\n`);
      reducedScenes.push({ viewport, screenshotPath, domPath, scenePath, failures: localFailures });
    } catch (error) {
      localFailures.push(`${viewport.name}: evidence capture exception: ${String(error?.stack || error)}`);
      writeFileSync(
        resolve(artifactDir, `prelaunch-${viewport.name}-${viewport.width}x${viewport.height}-capture-error.txt`),
        `${localFailures.join('\n\n')}\n`,
      );
    } finally {
      failures.push(...localFailures);
      await context.close();
    }
  }

  const motionFailures = [];
  const motionContext = await browser.newContext({
    viewport: { width: 1200, height: 1200 },
    reducedMotion: 'no-preference',
    colorScheme: 'dark',
  });
  try {
    const motionPage = await motionContext.newPage();
    const motionResponse = await motionPage.goto(url, { waitUntil: 'networkidle' });
    check(motionResponse?.ok(), `motion: HTTP ${motionResponse?.status() || 'unknown'}`, motionFailures);
    await motionPage.locator('[data-prelaunch-page]').waitFor({ state: 'visible' });
    const before = await motionPage.locator('[data-prelaunch-tile]').evaluateAll((nodes) => (
      nodes.map((node) => node.getAttribute('data-state'))
    ));
    try {
      await motionPage.waitForFunction((initial) => {
        const current = [...document.querySelectorAll('[data-prelaunch-tile]')]
          .map((node) => node.getAttribute('data-state'));
        return current.some((state, index) => state !== initial[index]);
      }, before, { timeout: 8000 });
    } catch {
      motionFailures.push('motion: no tile state changed within 8 seconds');
    }
    const motionEvidence = await motionPage.locator('[data-prelaunch-tile]').evaluateAll((nodes, initial) => {
      const current = nodes.map((node) => node.getAttribute('data-state'));
      return {
        changedIndices: current.flatMap((state, index) => state !== initial[index] ? [index] : []),
        tileOpacity: nodes.map((node) => Number(getComputedStyle(node).opacity)),
        seamBackground: getComputedStyle(document.querySelector('[data-prelaunch-mosaic]'), '::before').backgroundImage,
        current,
      };
    }, before);
    check(
      motionEvidence.changedIndices.length >= 2 && motionEvidence.changedIndices.length <= 5,
      `motion: expected one sparse group of 2–5 tiles, changed ${motionEvidence.changedIndices.length}`,
      motionFailures,
    );
    check(motionEvidence.tileOpacity.every((value) => value === 1), 'motion: whole-tile opacity changed', motionFailures);
    check(
      motionEvidence.seamBackground.includes('repeating-linear-gradient'),
      'motion: opaque seam disappeared during state transition',
      motionFailures,
    );
    await motionPage.screenshot({
      path: resolve(artifactDir, 'prelaunch-motion-1200x1200.png'),
      fullPage: true,
      animations: 'disabled',
    });
    writeFileSync(
      resolve(artifactDir, 'prelaunch-motion-scene.json'),
      `${JSON.stringify({ failures: motionFailures, before, ...motionEvidence }, null, 2)}\n`,
    );
  } catch (error) {
    motionFailures.push(`motion: evidence capture exception: ${String(error?.stack || error)}`);
    writeFileSync(resolve(artifactDir, 'prelaunch-motion-capture-error.txt'), `${motionFailures.join('\n\n')}\n`);
  } finally {
    failures.push(...motionFailures);
    await motionContext.close();
  }
} finally {
  await browser.close();
}

const summary = {
  ok: failures.length === 0,
  url,
  viewports: viewports.map(({ name, width, height }) => ({ name, width, height })),
  artifactDir,
  artifactFilesPerViewport: ['screenshot', 'dom', 'scene-json'],
  reducedScenes,
  failures,
};
writeFileSync(resolve(artifactDir, 'prelaunch-browser-summary.json'), `${JSON.stringify(summary, null, 2)}\n`);
console.log(JSON.stringify(summary, null, 2));
if (failures.length > 0) {
  throw new Error(`Prelaunch browser gate failed after preserving evidence:\n- ${failures.join('\n- ')}`);
}
