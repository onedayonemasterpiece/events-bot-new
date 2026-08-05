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
  throw new Error('Usage: check-prelaunch-browser.mjs --url <http(s) URL> [--artifact-dir <path>]');
}
mkdirSync(artifactDir, { recursive: true });

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
    const tracks = (value) => String(value || '').split(/\s+/u).filter(Boolean);

    const root = document.querySelector('[data-prelaunch-page]');
    const projection = document.querySelector('.prelaunch__projection');
    const background = document.querySelector('.prelaunch__background');
    const mosaic = document.querySelector('[data-prelaunch-mosaic]');
    const atmosphere = document.querySelector('.prelaunch__atmosphere');
    const foreground = document.querySelector('.prelaunch__foreground');
    const heading = document.querySelector('#prelaunch-title');
    const description = document.querySelector('.prelaunch__copy p');
    const notify = document.querySelector('.prelaunch__notify');
    const consent = document.querySelector('.prelaunch-form__consent');
    const tiles = [...document.querySelectorAll('[data-prelaunch-tile]')];
    const mosaicStyle = mosaic ? getComputedStyle(mosaic) : null;
    const gridColumnCount = tracks(mosaicStyle?.gridTemplateColumns).length;
    const counts = tiles.reduce((out, tile) => {
      const state = tile.getAttribute('data-state') || 'missing';
      out[state] = (out[state] || 0) + 1;
      return out;
    }, {});
    const surfaces = tiles.map((tile) => {
      const base = getComputedStyle(tile);
      const glass = getComputedStyle(tile, '::before');
      const matte = getComputedStyle(tile, '::after');
      return {
        index: Number(tile.getAttribute('data-index')),
        state: tile.getAttribute('data-state'),
        edge: tile.getAttribute('data-edge'),
        window: tile.getAttribute('data-window') === 'true',
        accent: tile.getAttribute('data-accent') === 'true',
        tileOpacity: Number(base.opacity),
        baseOverflow: base.overflow,
        baseRadius: base.borderRadius,
        glassBackground: glass.backgroundColor,
        glassAlpha: alpha(glass.backgroundColor),
        backdropFilter: glass.backdropFilter || glass.webkitBackdropFilter || '',
        glassRadius: glass.borderRadius,
        boxShadow: glass.boxShadow,
        matteOpacity: Number(matte.opacity),
      };
    });
    const stateSurface = Object.fromEntries(['sealed', 'dim', 'revealed'].map((state) => {
      const surface = surfaces.find((candidate) => candidate.state === state && candidate.edge === 'ambient')
        || surfaces.find((candidate) => candidate.state === state);
      return [state, surface || null];
    }));
    const first = tiles[0]?.getBoundingClientRect();
    const second = tiles[1]?.getBoundingClientRect();
    const nextRow = tiles[gridColumnCount]?.getBoundingClientRect();
    const scrolling = document.scrollingElement || document.documentElement;
    const documentHeight = Math.max(
      scrolling.scrollHeight,
      document.documentElement.scrollHeight,
      document.body?.scrollHeight || 0,
    );
    const documentWidth = Math.max(
      scrolling.scrollWidth,
      document.documentElement.scrollWidth,
      document.body?.scrollWidth || 0,
    );
    const image = projection instanceof HTMLImageElement
      ? { complete: projection.complete, naturalWidth: projection.naturalWidth }
      : { complete: false, naturalWidth: 0 };
    const rootBefore = root ? getComputedStyle(root, '::before') : null;
    const backgroundImage = background ? getComputedStyle(background, '::after') : null;
    return {
      layers: [root, projection, background, mosaic, atmosphere, foreground].every(Boolean),
      experienceReady: root?.getAttribute('data-experience-ready'),
      viewport: { width: window.innerWidth, height: window.innerHeight },
      documentHeight,
      documentWidth,
      verticalOverflow: documentHeight - window.innerHeight,
      horizontalOverflow: documentWidth - window.innerWidth,
      scrollTop: scrolling.scrollTop,
      tileCount: tiles.length,
      gridColumnCount,
      counts,
      windowCount: surfaces.filter((surface) => surface.window).length,
      accentCount: surfaces.filter((surface) => surface.accent).length,
      edgeBands: [...new Set(surfaces.map((surface) => surface.edge).filter(Boolean))],
      clearWindowCount: surfaces.filter((surface) => surface.glassAlpha <= .4).length,
      mostlyClosedCount: surfaces.filter((surface) => surface.glassAlpha >= .65).length,
      cornerMaskCount: surfaces.filter((surface) => (
        surface.baseOverflow === 'hidden'
        && surface.baseRadius === '0px'
        && surface.glassRadius !== '0px'
        && surface.boxShadow.includes('rgb(7, 9, 13)')
      )).length,
      backdropCount: surfaces.filter((surface) => surface.backdropFilter && surface.backdropFilter !== 'none').length,
      wholeTileOpacityStable: surfaces.every((surface) => surface.tileOpacity === 1),
      image,
      sharedLight: rootBefore?.backgroundImage || '',
      artwork: {
        width: Number.parseFloat(backgroundImage?.width || '0') || 0,
        top: Number.parseFloat(backgroundImage?.top || '0') || 0,
        left: Number.parseFloat(backgroundImage?.left || '0') || 0,
      },
      heading: rectOf(heading),
      description: rectOf(description),
      notify: rectOf(notify),
      consent: rectOf(consent),
      root: rectOf(root),
      firstTile: first ? {
        top: first.top,
        left: first.left,
        right: first.right,
        bottom: first.bottom,
        width: first.width,
        height: first.height,
      } : null,
      gap: first && second ? second.left - first.right : -1,
      verticalGap: first && nextRow ? nextRow.top - first.bottom : -1,
      tileAspect: first && first.width > 0 ? first.height / first.width : 0,
      stateSurface,
      stateSnapshot: tiles.map((tile) => tile.getAttribute('data-state')),
    };
  });
}

function validateScene(scene, viewport, localFailures) {
  const prefix = viewport.name;
  check(scene.layers, `${prefix}: one or more scene layers are missing`, localFailures);
  check(scene.experienceReady === 'true', `${prefix}: enhancement module is not ready`, localFailures);
  check(scene.tileCount === 72, `${prefix}: expected 72 panes, got ${scene.tileCount}`, localFailures);
  check(
    scene.counts.sealed === 30 && scene.counts.dim === 23 && scene.counts.revealed === 19,
    `${prefix}: deterministic DOM states changed ${JSON.stringify(scene.counts)}`,
    localFailures,
  );
  check(scene.windowCount === 8, `${prefix}: coherent reveal windows=${scene.windowCount}`, localFailures);
  check(scene.accentCount === 3, `${prefix}: edge accents=${scene.accentCount}`, localFailures);
  check(
    ['ambient', 'soft', 'warm', 'hot'].every((edge) => scene.edgeBands.includes(edge)),
    `${prefix}: incomplete edge bands ${scene.edgeBands.join(',')}`,
    localFailures,
  );
  check(scene.image.complete && scene.image.naturalWidth >= 512, `${prefix}: production projection did not decode`, localFailures);
  check(scene.sharedLight.includes('radial-gradient'), `${prefix}: shared light source is missing`, localFailures);
  check(scene.verticalOverflow <= 1, `${prefix}: page scrolls by ${scene.verticalOverflow}px`, localFailures);
  check(scene.horizontalOverflow <= 1, `${prefix}: horizontal overflow ${scene.horizontalOverflow}px`, localFailures);
  check(scene.scrollTop === 0, `${prefix}: initial scrollTop=${scene.scrollTop}`, localFailures);
  check(scene.root && Math.abs(scene.root.height - viewport.height) <= 1, `${prefix}: root height ${scene.root?.height}`, localFailures);
  check(scene.heading && scene.heading.top >= -1 && scene.heading.left >= -1, `${prefix}: heading escaped viewport`, localFailures);
  check(scene.notify && scene.notify.bottom <= viewport.height + 1, `${prefix}: form bottom ${scene.notify?.bottom}`, localFailures);
  check(scene.consent && scene.consent.bottom <= viewport.height + 1, `${prefix}: consent bottom ${scene.consent?.bottom}`, localFailures);
  check(
    scene.description && scene.notify && scene.description.bottom + 6 <= scene.notify.top,
    `${prefix}: description/form overlap ${scene.description?.bottom}/${scene.notify?.top}`,
    localFailures,
  );
  check(scene.gap >= 4 && scene.gap <= 12, `${prefix}: horizontal seam ${scene.gap}px`, localFailures);
  check(scene.verticalGap >= 4 && scene.verticalGap <= 12, `${prefix}: vertical seam ${scene.verticalGap}px`, localFailures);
  check(scene.tileAspect >= .97 && scene.tileAspect <= 1.03, `${prefix}: pane aspect ${scene.tileAspect}`, localFailures);
  check(scene.cornerMaskCount === 72, `${prefix}: corner masks ${scene.cornerMaskCount}/72`, localFailures);
  check(scene.backdropCount === 72, `${prefix}: backdrop panes ${scene.backdropCount}/72`, localFailures);
  check(scene.wholeTileOpacityStable, `${prefix}: whole-pane opacity changed`, localFailures);
  check(scene.clearWindowCount >= 6 && scene.clearWindowCount <= 10, `${prefix}: clear windows=${scene.clearWindowCount}`, localFailures);
  check(scene.mostlyClosedCount >= 56, `${prefix}: mostly-closed panes=${scene.mostlyClosedCount}`, localFailures);

  const { sealed, dim, revealed } = scene.stateSurface;
  check(sealed && dim && revealed, `${prefix}: missing state surface`, localFailures);
  if (sealed && dim && revealed) {
    check(
      sealed.glassAlpha > dim.glassAlpha && dim.glassAlpha > revealed.glassAlpha,
      `${prefix}: state alpha order ${sealed.glassAlpha}/${dim.glassAlpha}/${revealed.glassAlpha}`,
      localFailures,
    );
    check(
      new Set([sealed.backdropFilter, dim.backdropFilter, revealed.backdropFilter]).size === 3,
      `${prefix}: state material treatments collapsed`,
      localFailures,
    );
  }

  if (viewport.width <= 599) {
    check(scene.gridColumnCount === 6, `${prefix}: mobile columns=${scene.gridColumnCount}`, localFailures);
    check(scene.heading && scene.heading.top <= viewport.height * .48, `${prefix}: mobile headline too late at ${scene.heading?.top}`, localFailures);
    const widthRatio = scene.artwork.width / viewport.width;
    check(widthRatio >= .82 && widthRatio <= .96, `${prefix}: artwork width ratio ${widthRatio.toFixed(3)}`, localFailures);
  } else {
    check(scene.gridColumnCount === 9, `${prefix}: desktop columns=${scene.gridColumnCount}`, localFailures);
  }

  if (viewport.name === 'reference-square') {
    check(scene.heading && scene.heading.left >= 100 && scene.heading.left <= 160, `reference-square: heading left=${scene.heading?.left}`, localFailures);
    check(scene.heading && scene.heading.top >= 250 && scene.heading.top <= 390, `reference-square: heading top=${scene.heading?.top}`, localFailures);
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
      await page.locator('[data-prelaunch-tile][data-edge]').first().waitFor({ state: 'attached' });
      await page.waitForTimeout(250);
      const scene = await readScene(page);
      validateScene(scene, viewport, localFailures);

      await page.waitForTimeout(350);
      const stateAfter = await page.locator('[data-prelaunch-tile]').evaluateAll((nodes) => (
        nodes.map((node) => node.getAttribute('data-state'))
      ));
      check(
        JSON.stringify(stateAfter) === JSON.stringify(scene.stateSnapshot),
        `${viewport.name}: reduced-motion state changed`,
        localFailures,
      );

      const screenshotPath = resolve(artifactDir, `prelaunch-${viewport.name}-${viewport.width}x${viewport.height}.png`);
      const domPath = resolve(artifactDir, `prelaunch-${viewport.name}-${viewport.width}x${viewport.height}-dom.html`);
      const scenePath = resolve(artifactDir, `prelaunch-${viewport.name}-${viewport.width}x${viewport.height}-scene.json`);
      await page.screenshot({ path: screenshotPath, fullPage: true, animations: 'disabled' });
      writeFileSync(domPath, await page.content());
      writeFileSync(scenePath, `${JSON.stringify({ viewport, failures: localFailures, ...scene }, null, 2)}\n`);
      reducedScenes.push({ viewport, screenshotPath, domPath, scenePath, failures: localFailures });
    } catch (error) {
      localFailures.push(`${viewport.name}: capture exception: ${String(error?.stack || error)}`);
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
    const page = await motionContext.newPage();
    const response = await page.goto(url, { waitUntil: 'networkidle' });
    check(response?.ok(), `motion: HTTP ${response?.status() || 'unknown'}`, motionFailures);
    await page.locator('[data-prelaunch-page]').waitFor({ state: 'visible' });
    const before = await page.locator('[data-prelaunch-tile]').evaluateAll((nodes) => (
      nodes.map((node) => node.getAttribute('data-state'))
    ));
    try {
      await page.waitForFunction((initial) => {
        const current = [...document.querySelectorAll('[data-prelaunch-tile]')]
          .map((node) => node.getAttribute('data-state'));
        return current.some((state, index) => state !== initial[index]);
      }, before, { timeout: 8000 });
    } catch {
      motionFailures.push('motion: no pane state changed within 8 seconds');
    }
    const motion = await page.locator('[data-prelaunch-tile]').evaluateAll((nodes, initial) => {
      const current = nodes.map((node) => node.getAttribute('data-state'));
      return {
        changedIndices: current.flatMap((state, index) => state !== initial[index] ? [index] : []),
        tileOpacity: nodes.map((node) => Number(getComputedStyle(node).opacity)),
        cornerMasks: nodes.filter((node) => {
          const base = getComputedStyle(node);
          const surface = getComputedStyle(node, '::before');
          return base.overflow === 'hidden' && base.borderRadius === '0px' && surface.boxShadow.includes('rgb(7, 9, 13)');
        }).length,
        current,
      };
    }, before);
    check(
      motion.changedIndices.length >= 2 && motion.changedIndices.length <= 5,
      `motion: expected 2–5 changed panes, got ${motion.changedIndices.length}`,
      motionFailures,
    );
    check(motion.tileOpacity.every((value) => value === 1), 'motion: whole-pane opacity changed', motionFailures);
    check(motion.cornerMasks === 72, `motion: corner masks ${motion.cornerMasks}/72`, motionFailures);
    await page.screenshot({
      path: resolve(artifactDir, 'prelaunch-motion-1200x1200.png'),
      fullPage: true,
      animations: 'disabled',
    });
    writeFileSync(
      resolve(artifactDir, 'prelaunch-motion-scene.json'),
      `${JSON.stringify({ failures: motionFailures, before, ...motion }, null, 2)}\n`,
    );
  } catch (error) {
    motionFailures.push(`motion: exception: ${String(error?.stack || error)}`);
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
  viewports,
  artifactDir,
  reducedScenes,
  failures,
};
writeFileSync(resolve(artifactDir, 'prelaunch-browser-summary.json'), `${JSON.stringify(summary, null, 2)}\n`);
console.log(JSON.stringify(summary, null, 2));
if (failures.length > 0) {
  throw new Error(`Prelaunch browser gate failed after preserving evidence:\n- ${failures.join('\n- ')}`);
}
