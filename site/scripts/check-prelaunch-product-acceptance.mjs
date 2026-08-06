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
  throw new Error('Usage: check-prelaunch-product-acceptance.mjs --url <http(s) URL> [--artifact-dir <path>]');
}
mkdirSync(artifactDir, { recursive: true });

const viewports = [
  { name: 'acceptance-square', width: 1200, height: 1200 },
  { name: 'acceptance-wide', width: 1728, height: 900 },
  { name: 'acceptance-mobile', width: 390, height: 844 },
  { name: 'acceptance-mobile-small', width: 320, height: 568 },
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
      check(response?.ok(), `${viewport.name}: HTTP ${response?.status() || 'unknown'}`, localFailures);
      await page.locator('[data-prelaunch-page]').waitFor({ state: 'visible' });
      await page.waitForFunction(() => {
        const root = document.querySelector('[data-prelaunch-page]');
        return root?.getAttribute('data-artwork-ready') === 'true'
          && root?.getAttribute('data-window-map-count') === '10'
          && document.querySelector('[data-prelaunch-form]')?.getAttribute('data-copy-calibrated') === 'true';
      }, undefined, { timeout: 8000 });
      await page.waitForTimeout(250);

      const scene = await page.evaluate(() => {
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
          if (!value || value === 'transparent') return 0;
          const match = /rgba\([^)]*[,\s]([\d.]+)\s*\)$/u.exec(value);
          return match ? Number(match[1]) : 1;
        };
        const root = document.querySelector('[data-prelaunch-page]');
        const artwork = document.querySelector('.prelaunch__artwork');
        const mosaic = document.querySelector('[data-prelaunch-mosaic]');
        const tiles = [...document.querySelectorAll('[data-prelaunch-tile]')];
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
        const first = tiles[0]?.getBoundingClientRect();
        const second = tiles[1]?.getBoundingClientRect();
        const columns = String(getComputedStyle(mosaic).gridTemplateColumns || '')
          .split(/\s+/u)
          .filter(Boolean).length;
        const nextRow = tiles[columns]?.getBoundingClientRect();
        const representative = tiles.find((tile) => tile.getAttribute('data-window') === 'true') || tiles[0];
        const representativeStyle = getComputedStyle(representative);
        const representativeGlass = getComputedStyle(representative, '::before');
        const mosaicSeams = getComputedStyle(mosaic, '::before');
        const rootLight = getComputedStyle(root, '::before');
        const surfaces = tiles.map((tile) => {
          const glass = getComputedStyle(tile, '::before');
          return {
            state: tile.getAttribute('data-state'),
            edge: tile.getAttribute('data-edge'),
            window: tile.getAttribute('data-window') === 'true',
            backgroundAlpha: alpha(glass.backgroundColor),
            borderColor: glass.borderColor,
            backgroundImage: glass.backgroundImage,
          };
        });

        let artworkPixels = null;
        if (artwork instanceof HTMLCanvasElement) {
          const context = artwork.getContext('2d', { willReadFrequently: true });
          if (context) {
            const sampleAlpha = (x, y) => context.getImageData(x, y, 1, 1).data[3];
            const width = artwork.width;
            const height = artwork.height;
            artworkPixels = {
              width,
              height,
              corners: [
                sampleAlpha(0, 0),
                sampleAlpha(width - 1, 0),
                sampleAlpha(0, height - 1),
                sampleAlpha(width - 1, height - 1),
              ],
              centre: sampleAlpha(Math.floor(width / 2), Math.floor(height / 2)),
            };
          }
        }

        return {
          viewport: { width: innerWidth, height: innerHeight },
          root: rectOf(root),
          heading: rectOf(document.querySelector('#prelaunch-title')),
          description: rectOf(document.querySelector('.prelaunch__copy p')),
          notify: rectOf(document.querySelector('.prelaunch__notify')),
          consent: rectOf(document.querySelector('.prelaunch-form__consent')),
          artwork: rectOf(artwork),
          artworkTag: artwork?.tagName || '',
          artworkReady: root?.getAttribute('data-artwork-ready'),
          artworkBound: root?.getAttribute('data-transparent-artwork-bound'),
          artworkPixels,
          tileCount: tiles.length,
          windowCount: surfaces.filter((surface) => surface.window).length,
          clearCount: surfaces.filter((surface) => surface.backgroundAlpha <= .45).length,
          mostlyClosedCount: surfaces.filter((surface) => surface.backgroundAlpha >= .62).length,
          hotCount: surfaces.filter((surface) => surface.edge === 'hot').length,
          warmCount: surfaces.filter((surface) => surface.edge === 'warm').length,
          localSpotlightCount: surfaces.filter((surface) => surface.backgroundImage.includes('radial-gradient')).length,
          tileBackground: representativeStyle.backgroundImage,
          tileOverflow: representativeStyle.overflow,
          tileRadius: representativeStyle.borderRadius,
          glassRadius: representativeGlass.borderRadius,
          seamBackground: mosaicSeams.backgroundImage,
          sharedLightBackground: rootLight.backgroundImage,
          gap: first && second ? second.left - first.right : -1,
          verticalGap: first && nextRow ? nextRow.top - first.bottom : -1,
          tileAspect: first && first.width ? first.height / first.width : 0,
          verticalOverflow: documentHeight - innerHeight,
          horizontalOverflow: documentWidth - innerWidth,
          promiseCopy: document.querySelector('.prelaunch-form__promise')?.textContent?.trim() || '',
          consentCopy: document.querySelector('.prelaunch-form__consent span')?.textContent?.trim() || '',
          submitLabel: document.querySelector('[data-prelaunch-submit-label]')?.textContent?.trim() || '',
        };
      });

      check(scene.root && Math.abs(scene.root.height - viewport.height) <= 1, `${viewport.name}: root does not fill viewport`, localFailures);
      check(scene.verticalOverflow <= 1, `${viewport.name}: vertical overflow ${scene.verticalOverflow}px`, localFailures);
      check(scene.horizontalOverflow <= 1, `${viewport.name}: horizontal overflow ${scene.horizontalOverflow}px`, localFailures);
      check(scene.tileCount === 72, `${viewport.name}: tile count ${scene.tileCount}`, localFailures);
      check(scene.windowCount === 10, `${viewport.name}: reveal windows ${scene.windowCount}`, localFailures);
      check(scene.clearCount >= 10 && scene.clearCount <= 20, `${viewport.name}: clear panes ${scene.clearCount}`, localFailures);
      check(scene.mostlyClosedCount >= 48, `${viewport.name}: mostly closed panes ${scene.mostlyClosedCount}`, localFailures);
      check(scene.hotCount > 0 && scene.warmCount > 0, `${viewport.name}: spatial edge bands missing`, localFailures);
      check(scene.localSpotlightCount === 0, `${viewport.name}: local per-pane radial spotlight returned`, localFailures);
      check(scene.seamBackground.includes('repeating-linear-gradient'), `${viewport.name}: opaque gap layer missing`, localFailures);
      check((scene.tileBackground.match(/radial-gradient/gu) || []).length >= 4, `${viewport.name}: rounded corner masks missing`, localFailures);
      check(scene.tileOverflow === 'visible', `${viewport.name}: tile overflow ${scene.tileOverflow}`, localFailures);
      check(scene.glassRadius !== '0px', `${viewport.name}: glass radius missing`, localFailures);
      check(scene.gap >= 4 && scene.gap <= 12, `${viewport.name}: horizontal gap ${scene.gap}px`, localFailures);
      check(scene.verticalGap >= 4 && scene.verticalGap <= 12, `${viewport.name}: vertical gap ${scene.verticalGap}px`, localFailures);
      check(scene.tileAspect >= .97 && scene.tileAspect <= 1.03, `${viewport.name}: tile aspect ${scene.tileAspect}`, localFailures);
      check(scene.sharedLightBackground.includes('radial-gradient'), `${viewport.name}: shared emitter missing`, localFailures);
      check(scene.artworkReady === 'true' && scene.artworkBound === 'canvas-reference', `${viewport.name}: artwork alpha pipeline not ready`, localFailures);
      check(scene.artworkTag === 'CANVAS', `${viewport.name}: artwork is not the processed canvas`, localFailures);
      check(scene.artwork && scene.artwork.width > 140 && scene.artwork.height > 140, `${viewport.name}: artwork geometry collapsed`, localFailures);
      check(scene.artworkPixels && scene.artworkPixels.corners.every((value) => value <= 8), `${viewport.name}: studio field remains in artwork corners`, localFailures);
      check(scene.artworkPixels && scene.artworkPixels.centre >= 220, `${viewport.name}: leather centre is transparent`, localFailures);
      check(scene.heading && scene.heading.top >= 0 && scene.heading.bottom <= viewport.height, `${viewport.name}: heading outside viewport`, localFailures);
      check(scene.notify && scene.notify.top >= 0 && scene.notify.bottom <= viewport.height + 1, `${viewport.name}: form outside viewport`, localFailures);
      check(scene.consent && scene.consent.bottom <= viewport.height + 1, `${viewport.name}: consent outside viewport`, localFailures);
      check(scene.promiseCopy.includes('приятный сюрприз'), `${viewport.name}: surprise promise missing`, localFailures);
      check(scene.consentCopy.includes('новостях сервиса') && scene.consentCopy.includes('Отписаться'), `${viewport.name}: consent copy incomplete`, localFailures);
      check(scene.submitLabel === 'Напомнить о запуске', `${viewport.name}: CTA label changed`, localFailures);

      const screenshotPath = resolve(artifactDir, `prelaunch-${viewport.name}-${viewport.width}x${viewport.height}.png`);
      await page.screenshot({ path: screenshotPath, fullPage: false, animations: 'disabled' });
      const result = { viewport, scene, failures: localFailures, screenshotPath };
      writeFileSync(
        resolve(artifactDir, `prelaunch-${viewport.name}-${viewport.width}x${viewport.height}-acceptance.json`),
        `${JSON.stringify(result, null, 2)}\n`,
      );
      evidence.push(result);
    } catch (error) {
      localFailures.push(`${viewport.name}: acceptance capture failed: ${String(error?.stack || error)}`);
    } finally {
      failures.push(...localFailures);
      await context.close();
    }
  }

  const registeredFailures = [];
  const registeredContext = await browser.newContext({
    viewport: { width: 390, height: 844 },
    reducedMotion: 'reduce',
    colorScheme: 'dark',
  });
  try {
    await registeredContext.addInitScript(() => {
      localStorage.setItem('ke_prelaunch_notification_v1', 'registered');
    });
    const page = await registeredContext.newPage();
    await page.goto(url, { waitUntil: 'networkidle' });
    await page.waitForFunction(() => (
      document.querySelector('[data-prelaunch-form]')?.getAttribute('data-experience-state') === 'registered'
    ));
    const state = await page.evaluate(() => ({
      state: document.querySelector('[data-prelaunch-form]')?.getAttribute('data-experience-state'),
      title: document.querySelector('.prelaunch-form__complete-copy strong')?.textContent?.trim() || '',
      body: document.querySelector('.prelaunch-form__complete-copy span')?.textContent?.trim() || '',
      completeVisible: !document.querySelector('.prelaunch-form__complete')?.hasAttribute('hidden'),
      rowHidden: document.querySelector('.prelaunch-form__row')?.hasAttribute('hidden') || false,
    }));
    check(state.state === 'registered', `registered: state ${state.state}`, registeredFailures);
    check(state.title === 'Вы уже записаны', `registered: title ${state.title}`, registeredFailures);
    check(state.body.includes('приятный сюрприз'), 'registered: surprise copy missing', registeredFailures);
    check(state.completeVisible && state.rowHidden, 'registered: completion surface not replacing fields', registeredFailures);
    await page.screenshot({
      path: resolve(artifactDir, 'prelaunch-acceptance-registered-390x844.png'),
      fullPage: false,
      animations: 'disabled',
    });
    evidence.push({ viewport: { name: 'registered', width: 390, height: 844 }, state, failures: registeredFailures });
  } catch (error) {
    registeredFailures.push(`registered: acceptance capture failed: ${String(error?.stack || error)}`);
  } finally {
    failures.push(...registeredFailures);
    await registeredContext.close();
  }
} finally {
  await browser.close();
}

const summary = {
  schema_version: 'prelaunch_product_acceptance_v1',
  ok: failures.length === 0,
  url,
  evidence,
  failures,
};
writeFileSync(resolve(artifactDir, 'prelaunch-product-acceptance-summary.json'), `${JSON.stringify(summary, null, 2)}\n`);
console.log(JSON.stringify(summary, null, 2));
if (failures.length > 0) {
  throw new Error(`Prelaunch product acceptance failed:\n- ${failures.join('\n- ')}`);
}
