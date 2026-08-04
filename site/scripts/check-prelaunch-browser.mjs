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

// The landing is a single-screen product surface. This matrix deliberately
// includes short desktop and small-phone viewports where accidental scroll is
// most likely to appear.
const viewports = [
  { name: 'reference-square', width: 1200, height: 1200 },
  { name: 'desktop-xl', width: 1920, height: 1080 },
  { name: 'desktop', width: 1440, height: 900 },
  { name: 'desktop-short', width: 1366, height: 768 },
  { name: 'desktop-compact', width: 1280, height: 720 },
  { name: 'tablet', width: 1024, height: 768 },
  { name: 'mobile-large', width: 430, height: 932 },
  { name: 'mobile', width: 390, height: 844 },
  { name: 'mobile-compact', width: 375, height: 667 },
  { name: 'mobile-small', width: 320, height: 568 },
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
    const rectOf = (node) => {
      if (!(node instanceof Element)) return null;
      const rect = node.getBoundingClientRect();
      return {
        top: rect.top,
        right: rect.right,
        bottom: rect.bottom,
        left: rect.left,
        width: rect.width,
        height: rect.height,
      };
    };
    const alpha = (value) => {
      if (value === 'transparent') return 0;
      const match = /rgba\([^)]*[,\s]([\d.]+)\s*\)$/u.exec(value);
      return match ? Number(match[1]) : 1;
    };

    const root = document.querySelector('[data-prelaunch-page]');
    const projection = document.querySelector('.prelaunch__projection');
    const mosaic = document.querySelector('[data-prelaunch-mosaic]');
    const atmosphere = document.querySelector('.prelaunch__atmosphere');
    const foreground = document.querySelector('.prelaunch__foreground');
    const brand = document.querySelector('.prelaunch__brand');
    const copy = document.querySelector('.prelaunch__copy');
    const heading = document.querySelector('#prelaunch-title');
    const description = document.querySelector('.prelaunch__copy p');
    const notify = document.querySelector('.prelaunch__notify');
    const consent = document.querySelector('.prelaunch-form__consent');
    const tiles = [...document.querySelectorAll('[data-prelaunch-tile]')];
    const email = document.querySelector('input[type="email"]');
    const button = document.querySelector('[data-prelaunch-submit]');

    const counts = tiles.reduce((out, tile) => {
      const state = tile.getAttribute('data-state') || 'missing';
      out[state] = (out[state] || 0) + 1;
      return out;
    }, {});

    const tileSurfaces = tiles.map((tile) => {
      const base = getComputedStyle(tile);
      const glass = getComputedStyle(tile, '::before');
      return {
        index: Number(tile.getAttribute('data-index')),
        state: tile.getAttribute('data-state'),
        zone: tile.getAttribute('data-zone'),
        tileOpacity: Number(base.opacity),
        glassBackground: glass.backgroundColor,
        glassAlpha: alpha(glass.backgroundColor),
        backdropFilter: glass.backdropFilter || glass.webkitBackdropFilter || '',
      };
    });

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
    const mosaicSeam = mosaic ? getComputedStyle(mosaic, '::before').backgroundImage : '';
    const scrolling = document.scrollingElement || document.documentElement;
    const documentHeight = Math.max(
      scrolling.scrollHeight,
      document.documentElement.scrollHeight,
      document.body?.scrollHeight || 0,
    );

    return {
      layers: [root, projection, mosaic, atmosphere, foreground].every(Boolean),
      tileCount: tiles.length,
      counts,
      glintCount: tiles.filter((tile) => tile.getAttribute('data-glint') === 'true').length,
      zoneCount: new Set(tiles.map((tile) => tile.getAttribute('data-zone'))).size,
      radiiCount: radii.length,
      image,
      viewport: { width: window.innerWidth, height: window.innerHeight },
      documentHeight,
      verticalOverflow: documentHeight - window.innerHeight,
      horizontalOverflow: scrolling.scrollWidth - window.innerWidth,
      scrollTop: scrolling.scrollTop,
      inputWidth: email?.getBoundingClientRect().width || 0,
      buttonWidth: button?.getBoundingClientRect().width || 0,
      root: rectOf(root),
      brand: rectOf(brand),
      copy: rectOf(copy),
      heading: rectOf(heading),
      description: rectOf(description),
      notify: rectOf(notify),
      consent: rectOf(consent),
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
      tileSurfaces,
      effectiveClearCount: tileSurfaces.filter((surface) => surface.glassAlpha <= .4).length,
      effectiveMostlyClosedCount: tileSurfaces.filter((surface) => surface.glassAlpha >= .65).length,
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
    `${prefix}: deterministic DOM scene changed ${JSON.stringify(scene.counts)}`,
    localFailures,
  );
  check(scene.glintCount === 3, `${prefix}: expected three directed-light glints, got ${scene.glintCount}`, localFailures);
  check(scene.zoneCount === 3, `${prefix}: expected quiet/light/leather motion zones`, localFailures);
  check(scene.radiiCount >= 3, `${prefix}: tile radii lost deliberate micro-variation`, localFailures);
  check(scene.image.complete && scene.image.naturalWidth >= 512, `${prefix}: projection fallback did not decode`, localFailures);
  check(scene.horizontalOverflow <= 1, `${prefix}: horizontal overflow ${scene.horizontalOverflow}px`, localFailures);
  check(scene.verticalOverflow <= 1, `${prefix}: page scrolls by ${scene.verticalOverflow}px`, localFailures);
  check(scene.scrollTop === 0, `${prefix}: page opened at scrollTop=${scene.scrollTop}`, localFailures);
  check(scene.root && Math.abs(scene.root.height - scene.viewport.height) <= 1, `${prefix}: root height ${scene.root?.height} != viewport ${scene.viewport.height}`, localFailures);
  check(scene.inputWidth >= 170 && scene.buttonWidth >= 150, `${prefix}: form control geometry collapsed`, localFailures);
  check(scene.heading && scene.heading.left >= 0 && scene.heading.top >= 0, `${prefix}: heading escaped viewport`, localFailures);
  check(scene.brand && scene.brand.top >= -1, `${prefix}: brand escaped viewport`, localFailures);
  check(scene.notify && scene.notify.bottom <= scene.viewport.height + 1, `${prefix}: form bottom ${scene.notify?.bottom} exceeds viewport ${scene.viewport.height}`, localFailures);
  check(scene.consent && scene.consent.bottom <= scene.viewport.height + 1, `${prefix}: consent bottom ${scene.consent?.bottom} exceeds viewport`, localFailures);
  check(
    scene.description && scene.notify && scene.description.bottom + 8 <= scene.notify.top,
    `${prefix}: description overlaps form (${scene.description?.bottom} / ${scene.notify?.top})`,
    localFailures,
  );
  check(scene.gap >= 5 && scene.gap <= 12, `${prefix}: horizontal inter-tile seam is ${scene.gap}px`, localFailures);
  check(scene.verticalGap >= 5 && scene.verticalGap <= 12, `${prefix}: vertical inter-tile seam is ${scene.verticalGap}px`, localFailures);
  check(scene.tileAspect >= .97 && scene.tileAspect <= 1.03, `${prefix}: tile aspect ${scene.tileAspect} is not square`, localFailures);
  check(
    scene.mosaicSeam.includes('repeating-linear-gradient'),
    `${prefix}: mosaic no longer paints an opaque seam above the projection`,
    localFailures,
  );
  check(
    scene.effectiveClearCount >= 4 && scene.effectiveClearCount <= 10,
    `${prefix}: expected 4–10 genuinely clear windows, got ${scene.effectiveClearCount}`,
    localFailures,
  );
  check(
    scene.effectiveMostlyClosedCount >= 58,
    `${prefix}: product is insufficiently hidden; mostly-closed tiles=${scene.effectiveMostlyClosedCount}`,
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

  if (viewport.width <= 599) {
    check(
      scene.heading && scene.heading.top <= viewport.height * .48,
      `${prefix}: mobile launch date appears too late at y=${scene.heading?.top}`,
      localFailures,
    );
  }

  if (viewport.name === 'reference-square') {
    check(scene.firstTile && scene.firstTile.right >= 155 && scene.firstTile.right <= 180, `reference-square: first seam x=${scene.firstTile?.right}`, localFailures);
    check(scene.firstTile && scene.firstTile.bottom >= 75 && scene.firstTile.bottom <= 110, `reference-square: first seam y=${scene.firstTile?.bottom}`, localFailures);
    check(scene.heading && scene.heading.left >= 105 && scene.heading.left <= 145, `reference-square: heading left=${scene.heading?.left}`, localFailures);
    check(scene.heading && scene.heading.top >= 245 && scene.heading.top <= 360, `reference-square: heading top=${scene.heading?.top}`, localFailures);
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

      await page.waitForTimeout(300);
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
