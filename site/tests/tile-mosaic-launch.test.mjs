#!/usr/bin/env node

import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { existsSync, readdirSync, statfsSync } from 'node:fs';
import { mkdir, readFile, stat, writeFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { createRequire } from 'node:module';
import process from 'node:process';

const require = createRequire(import.meta.url);

const parseArgs = (argv) => {
  const options = {
    baseUrl: process.env.TILE_MOSAIC_BASE_URL || '',
    artifactDir: process.env.TILE_MOSAIC_ARTIFACT_DIR || '',
    photoPath: process.env.TILE_MOSAIC_PHOTO_PATH
      || '/assets/listing-media/67139633d0b0f304736438629a31e307818390f81a5f003c3dec1e7c989e9839-1080.webp',
  };
  for (let index = 0; index < argv.length; index += 1) {
    const value = argv[index];
    if (value === '--help' || value === '-h') options.help = true;
    else if (value === '--base-url') options.baseUrl = argv[++index] || '';
    else if (value === '--artifacts') options.artifactDir = argv[++index] || '';
    else if (value === '--photo-path') options.photoPath = argv[++index] || '';
    else throw new Error(`Unknown argument: ${value}`);
  }
  return options;
};

const usage = `Usage:
  node tests/tile-mosaic-launch.test.mjs --base-url <url> --artifacts <directory> [--photo-path <local asset path>]

Environment:
  TILE_MOSAIC_BASE_URL          Supplied preview/page URL (CLI flag wins)
  TILE_MOSAIC_ARTIFACT_DIR      Artifact directory (CLI flag wins)
  TILE_MOSAIC_PHOTO_PATH        Local generic photo path under the deployed base
  TILE_MOSAIC_LIVE_EMAIL        Optional explicit test address for real success + duplicate probes
  TILE_MOSAIC_TMPDIR            Optional Chromium profile temp directory
  PLAYWRIGHT_REQUIRE_PATH       Optional absolute Playwright package entry/path

This is Chromium L1 evidence. It does not emulate or claim native Android/iOS L2 coverage.`;

const loadPlaywright = () => {
  const candidates = [
    process.env.PLAYWRIGHT_REQUIRE_PATH,
    'playwright',
    '/usr/local/lib/node_modules/playwright',
    '/usr/lib/node_modules/playwright',
  ].filter(Boolean);
  const failures = [];
  for (const candidate of candidates) {
    try {
      return require(candidate);
    } catch (error) {
      failures.push(`${candidate}: ${error?.code || error?.message || 'load failed'}`);
    }
  }
  throw new Error(`Playwright is unavailable. Checked: ${failures.join('; ')}`);
};

const safeTarget = (value) => {
  const url = new URL(value);
  url.username = '';
  url.password = '';
  url.search = '';
  url.hash = '';
  return url.href;
};

const normalizedText = (value) => String(value || '').replace(/\s+/gu, ' ').trim();
const slug = (value) => value.toLowerCase().replace(/[^a-z0-9]+/gu, '-').replace(/^-|-$/gu, '');

const colorChannels = (value) => {
  const match = String(value).match(/rgba?\(\s*([\d.]+)[ ,]+([\d.]+)[ ,]+([\d.]+)(?:\s*[,/]\s*([\d.]+))?/iu);
  if (!match) return null;
  return {
    red: Number(match[1]),
    green: Number(match[2]),
    blue: Number(match[3]),
    alpha: match[4] === undefined ? 1 : Number(match[4]),
  };
};

const getPageUrl = (baseUrl, params = {}) => {
  const url = new URL(baseUrl);
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== null) url.searchParams.set(key, String(value));
  }
  return url.href;
};

const frameArtifacts = [];

const hasChromiumCache = (directory) => {
  if (!directory || !existsSync(directory)) return false;
  try {
    return readdirSync(directory).some((name) => /^chromium(?:_headless_shell)?-\d+$/u.test(name));
  } catch {
    return false;
  }
};

const freeBytes = (directory) => {
  try {
    const info = statfsSync(directory);
    return Number(info.bavail) * Number(info.bsize);
  } catch {
    return 0;
  }
};

const main = async () => {
  const options = parseArgs(process.argv.slice(2));
  if (options.help) {
    process.stdout.write(`${usage}\n`);
    return;
  }
  if (!options.baseUrl) throw new Error(`Missing --base-url.\n${usage}`);
  if (!options.artifactDir) throw new Error(`Missing --artifacts.\n${usage}`);
  const target = new URL(options.baseUrl);
  if (!['http:', 'https:'].includes(target.protocol)) throw new Error('Base URL must use HTTP(S).');

  const artifactDir = resolve(options.artifactDir);
  await mkdir(artifactDir, { recursive: true });
  const configuredTemp = process.env.TILE_MOSAIC_TMPDIR || process.env.TMPDIR || '/tmp';
  if (!process.env.TILE_MOSAIC_TMPDIR
      && freeBytes(configuredTemp) < 256 * 1024 * 1024
      && freeBytes('/dev/shm') > freeBytes(configuredTemp)) {
    process.env.TMPDIR = '/dev/shm';
  } else if (process.env.TILE_MOSAIC_TMPDIR) {
    process.env.TMPDIR = process.env.TILE_MOSAIC_TMPDIR;
  }
  if (freeBytes(process.env.TMPDIR || '/tmp') < 128 * 1024 * 1024) {
    throw new Error(`Insufficient Chromium temp space in ${process.env.TMPDIR || '/tmp'}; at least 128 MiB is required.`);
  }
  if (freeBytes(artifactDir) < 32 * 1024 * 1024) {
    throw new Error(`Insufficient artifact space in ${artifactDir}; at least 32 MiB is required.`);
  }
  if (!hasChromiumCache(process.env.PLAYWRIGHT_BROWSERS_PATH) && hasChromiumCache('/opt/ms-playwright')) {
    process.env.PLAYWRIGHT_BROWSERS_PATH = '/opt/ms-playwright';
  }
  const { chromium } = loadPlaywright();
  const launchBrowser = () => chromium.launch({
    headless: true,
    // Full Chromium is more stable than the headless-shell binary for this
    // backdrop-filter-heavy, high-resolution screenshot matrix.
    executablePath: chromium.executablePath(),
  });
  let browser = await launchBrowser();
  const startedAt = new Date().toISOString();
  const report = {
    schemaVersion: 1,
    runner: 'tile-mosaic-v2-l1-chromium',
    level: 'L1',
    nativeMobileEvidence: false,
    target: safeTarget(options.baseUrl),
    startedAt,
    browser: { name: 'chromium', version: browser.version() },
    requirements: Object.fromEntries(['R17', 'R18', 'R19', 'R20', 'R21', 'R22'].map((id) => [id, {
      status: 'pending', evidence: [],
    }])),
    scenarios: [],
    fixtures: { desktop: [], mobile: [] },
    form: [],
    animation: null,
    reducedMotion: null,
    projection: null,
    privacy: {
      syntheticAddressesOnlyByDefault: true,
      liveEmailRecorded: false,
      targetQueryRemovedFromReport: true,
      note: 'Optional live email is read from the environment and is never written to reports or screenshots.',
    },
    artifacts: [],
    failures: [],
  };

  const consoleFindings = [];

  const createPage = async ({ width, height, reducedMotion = 'no-preference' }, routeSetup) => {
    if (!browser.isConnected()) browser = await launchBrowser();
    const context = await browser.newContext({
      viewport: { width, height },
      reducedMotion,
      colorScheme: 'dark',
      locale: 'ru-RU',
      serviceWorkers: 'block',
    });
    const page = await context.newPage();
    page.on('pageerror', (error) => consoleFindings.push({ type: 'pageerror', message: String(error?.message || error) }));
    page.on('console', (message) => {
      if (message.type() === 'error') consoleFindings.push({ type: 'console', message: message.text().slice(0, 300) });
    });
    if (routeSetup) await routeSetup(page);
    return { context, page };
  };

  const ready = async (page, url = options.baseUrl) => {
    const response = await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 30_000 });
    assert(response, 'Navigation did not return a response.');
    assert(response.status() < 400, `Navigation returned HTTP ${response.status()}.`);
    await page.locator('[data-tile-mosaic-launch]').waitFor({ state: 'visible', timeout: 15_000 });
    await page.locator('.mosaic__tile').first().waitFor({ state: 'attached' });
    await page.evaluate(async () => {
      await document.fonts?.ready;
      const images = Array.from(document.images);
      await Promise.all(images.map((image) => image.complete
        ? Promise.resolve()
        : new Promise((resolveImage) => {
            image.addEventListener('load', resolveImage, { once: true });
            image.addEventListener('error', resolveImage, { once: true });
          })));
    });
  };

  const screenshot = async (page, name, metadata = {}) => {
    const filename = `${slug(name)}.png`;
    const absolute = resolve(artifactDir, filename);
    // Acceptance artifacts are viewport frames at the named dimensions. Avoid
    // Chromium's full-page compositor here: this scene is filter-heavy and the
    // handoff contract asks for viewport screenshots, not stitched documents.
    await page.screenshot({ path: absolute, fullPage: false, animations: 'allow' });
    frameArtifacts.push({ name, filename, ...metadata });
    return filename;
  };

  const scenario = async (name, requirementIds, callback) => {
    const item = { name, requirementIds, status: 'running', evidence: [] };
    report.scenarios.push(item);
    try {
      const evidence = await callback();
      item.status = 'passed';
      item.evidence = evidence ? (Array.isArray(evidence) ? evidence : [evidence]) : [];
      for (const id of requirementIds) report.requirements[id].evidence.push(name);
      return evidence;
    } catch (error) {
      item.status = 'failed';
      item.error = String(error?.message || error);
      report.failures.push({ scenario: name, message: item.error });
      return null;
    }
  };

  const desktopFixtures = [
    [1366, 768],
    [1440, 900],
    [1536, 864],
    [1672, 941],
    [1920, 1080],
  ];

  for (const [width, height] of desktopFixtures) {
    await scenario(`desktop-${width}x${height}`, ['R18', ...(width === 1672 || width === 1920 ? ['R21'] : [])], async () => {
      const { context, page } = await createPage({ width, height });
      try {
        await ready(page);
        const metrics = await page.evaluate(() => {
          const normalize = (value) => String(value || '').replace(/\s+/gu, ' ').trim();
          const rect = (selector) => {
            const element = document.querySelector(selector);
            if (!(element instanceof Element)) return null;
            const box = element.getBoundingClientRect();
            return { left: box.left, top: box.top, right: box.right, bottom: box.bottom, width: box.width, height: box.height };
          };
          const grid = document.querySelector('.mosaic__grid');
          const opaqueSeams = document.querySelector('[data-opaque-seams]');
          const tiles = Array.from(document.querySelectorAll('.mosaic__tile'));
          const first = tiles[0]?.getBoundingClientRect();
          const second = tiles[1]?.getBoundingClientRect();
          const gap = first && second ? second.left - first.right : null;
          const gapPoint = first && gap > 0
            ? { x: first.right + gap / 2, y: first.top + first.height / 2 }
            : null;
          const gapElement = gapPoint ? document.elementFromPoint(gapPoint.x, gapPoint.y) : null;
          const h1 = document.querySelector('.launch h1');
          const description = document.querySelector('.launch__description');
          const label = document.querySelector('label[for="launch-email"], .subscribe__label');
          const labelStyle = label ? getComputedStyle(label) : null;
          const brandImage = document.querySelector('.launch__brand img, [data-launch-brand] img');
          const status = document.querySelector('.launch__status');
          const date = document.querySelector('.launch__date');
          const input = document.querySelector('input[type="email"]');
          const button = document.querySelector('button[type="submit"]');
          const envelope = document.querySelector('.subscribe svg, [data-envelope-icon], .subscribe__envelope');
          return {
            tileCount: tiles.length,
            tile: first ? { width: first.width, height: first.height } : null,
            grid: rect('.mosaic__grid'),
            gridBackground: grid ? getComputedStyle(grid).backgroundColor : null,
            opaqueSeamsPresent: Boolean(opaqueSeams),
            opaqueSeamsBackground: opaqueSeams ? getComputedStyle(opaqueSeams).backgroundImage : null,
            gridGap: gap,
            gapHitClass: gapElement instanceof Element ? gapElement.className : null,
            root: rect('[data-tile-mosaic-launch]'),
            logo: rect('.launch__brand img, [data-launch-brand] img'),
            logoSrc: brandImage instanceof HTMLImageElement ? brandImage.currentSrc || brandImage.src : null,
            status: normalize(status?.textContent),
            date: normalize(date?.textContent),
            h1Lines: h1 instanceof HTMLElement ? h1.innerText.split(/\n+/u).map(normalize).filter(Boolean) : [],
            description: normalize(description?.textContent),
            descriptionLines: Array.from(description?.children || []).map((element) => normalize(element.textContent)).filter(Boolean),
            descriptionVisualLines: description ? Math.round(description.getBoundingClientRect().height / Number.parseFloat(getComputedStyle(description).lineHeight)) : 0,
            labelAccessible: label?.textContent ? normalize(label.textContent) : null,
            labelVisuallyHidden: labelStyle ? ['absolute', 'fixed'].includes(labelStyle.position)
              && (label?.getBoundingClientRect().width <= 2 || labelStyle.clipPath !== 'none') : false,
            input: rect('input[type="email"]'),
            button: rect('button[type="submit"]'),
            form: rect('[data-launch-subscribe-form]'),
            envelopePresent: Boolean(envelope),
            scrollHeight: Math.max(document.documentElement.scrollHeight, document.body.scrollHeight),
            viewport: { width: innerWidth, height: innerHeight },
          };
        });

        const image = await screenshot(page, `desktop-${width}x${height}`, {
          requirementIds: ['R18', ...(width === 1672 || width === 1920 ? ['R21'] : [])],
        });
        assert.equal(metrics.tileCount, 72, 'Expected exactly 72 mosaic tiles.');
        assert(metrics.tile, 'First tile is missing.');
        assert(Math.abs(metrics.tile.width - metrics.tile.height) <= 1.5, `Desktop tile is not square: ${metrics.tile.width}×${metrics.tile.height}.`);
        assert(metrics.gridGap >= 1, `Expected an opaque seam, measured gap ${metrics.gridGap}.`);
        const expectedTileSide = (height - 5 * metrics.gridGap) / 6;
        assert(Math.abs(metrics.tile.height - expectedTileSide) <= 1.5,
          `Tile side ${metrics.tile.height}px does not fit six one-sixth-height rows plus five seams (expected ${expectedTileSide}px).`);
        assert(metrics.grid && Math.abs(metrics.grid.top) <= 1.5, `Mosaic grid must start at viewport top, got ${metrics.grid?.top}.`);
        const leftRatio = metrics.grid.left / width;
        assert(leftRatio >= 0.34 && leftRatio <= 0.39, `Mosaic left ratio ${leftRatio.toFixed(4)} is outside the 34–39vw acceptance band.`);
        const seam = colorChannels(metrics.gridBackground);
        assert(seam && seam.alpha >= 0.95 && Math.max(seam.red, seam.green, seam.blue) <= 28,
          `Grid seam must be opaque and nearly black, got ${metrics.gridBackground}.`);
        assert(String(metrics.gapHitClass).includes('mosaic__grid'), `Gap hit ${metrics.gapHitClass}, expected opaque grid lattice.`);
        assert(metrics.opaqueSeamsPresent && metrics.opaqueSeamsBackground !== 'none', 'Expected the explicit opaque-seam overlay.');
        assert(metrics.scrollHeight <= height + 1, `Desktop ${width}×${height} scrolls vertically (${metrics.scrollHeight}px).`);
        assert.deepEqual(metrics.h1Lines, ['Полюбить', 'Калининград', 'Анонсы']);
        assert.equal(metrics.status.toLocaleUpperCase('ru-RU'), 'СКОРО ЗАПУСК • 1 СЕНТЯБРЯ');
        assert.equal(metrics.date.toLocaleUpperCase('ru-RU'), '1 СЕНТЯБРЯ');
        assert.deepEqual(metrics.descriptionLines, [
          'Персонализированный сервис анонсов',
          'и навигатор по культурным',
          'и просветительским событиям',
          'Калининградской области',
        ]);
        assert(metrics.descriptionVisualLines >= 4 && metrics.descriptionVisualLines <= 5,
          `Expected approximately four description lines, measured ${metrics.descriptionVisualLines}.`);
        assert(metrics.logo && Math.abs(metrics.logo.width - metrics.logo.height) <= 2, 'Brand PWA logo must be square.');
        assert(/PWA-icon\.png(?:$|\?)/iu.test(metrics.logoSrc || ''), `Unexpected logo source: ${metrics.logoSrc}`);
        assert(metrics.logo.right < metrics.grid.left, 'Desktop logo must remain in the left content zone.');
        assert(metrics.form && metrics.form.bottom <= height + 1, `Form is clipped below viewport at ${metrics.form?.bottom}.`);
        assert(metrics.input && metrics.button && metrics.input.right <= metrics.button.left + 1, 'Desktop form controls must be arranged in one row.');
        assert(metrics.input.width >= 315 && metrics.input.width <= 375,
          `Desktop input width ${metrics.input.width}px is outside the 320–368px reference band tolerance.`);
        assert(metrics.button.width >= 240 && metrics.button.width <= 270,
          `Desktop button width ${metrics.button.width}px is outside the 245–265px reference band tolerance.`);
        const formGap = metrics.button.left - metrics.input.right;
        assert(formGap >= 12 && formGap <= 20, `Desktop form gap ${formGap}px is not approximately 16px.`);
        assert(metrics.input.height >= 72 && metrics.input.height <= 84,
          `Desktop input height ${metrics.input.height}px is outside the 76–80px reference band tolerance.`);
        assert(metrics.button.height >= 72 && metrics.button.height <= 84,
          `Desktop button height ${metrics.button.height}px is outside the 76–80px reference band tolerance.`);
        assert.equal(metrics.labelAccessible, 'Email для напоминания о запуске');
        assert(metrics.labelVisuallyHidden, 'Email label should be visually hidden, not removed from accessibility semantics.');
        assert(metrics.envelopePresent, 'Expected an envelope icon in the subscription form.');

        const fixture = { width, height, image, leftRatio, metrics };
        report.fixtures.desktop.push(fixture);
        return { image, leftRatio, tileSide: metrics.tile.height };
      } finally {
        await context.close().catch(() => {});
      }
    });
  }

  const mobileFixtures = [[320, 700], [360, 800], [390, 844], [430, 932]];
  for (const [width, height] of mobileFixtures) {
    await scenario(`mobile-${width}x${height}`, ['R19', ...(width === 390 ? ['R21'] : [])], async () => {
      const { context, page } = await createPage({ width, height });
      try {
        await ready(page);
        const metrics = await page.evaluate(() => {
          const normalize = (value) => String(value || '').replace(/\s+/gu, ' ').trim();
          const rect = (selector) => {
            const element = document.querySelector(selector);
            if (!(element instanceof Element)) return null;
            const box = element.getBoundingClientRect();
            return { left: box.left, top: box.top, right: box.right, bottom: box.bottom, width: box.width, height: box.height };
          };
          const h1 = document.querySelector('.launch h1');
          const statusElement = document.querySelector('.launch__status');
          const dateElement = document.querySelector('.launch__date');
          const description = document.querySelector('.launch__description');
          const brandImage = document.querySelector('.launch__brand img, [data-launch-brand] img');
          const tiles = document.querySelectorAll('.mosaic__tile');
          const gridStyle = getComputedStyle(document.querySelector('.mosaic__grid'));
          return {
            tileCount: tiles.length,
            gridColumns: gridStyle.gridTemplateColumns.split(/\s+/u).filter(Boolean).length,
            scrollWidth: Math.max(document.documentElement.scrollWidth, document.body.scrollWidth),
            viewportWidth: innerWidth,
            header: rect('.launch__header'),
            mosaic: rect('.mosaic'),
            copy: rect('.launch__copy'),
            h1: rect('.launch h1'),
            h1Lines: h1 instanceof HTMLElement ? h1.innerText.split(/\n+/u).map(normalize).filter(Boolean) : [],
            statusText: normalize(statusElement?.textContent),
            dateText: normalize(dateElement?.textContent),
            descriptionLines: Array.from(description?.children || []).map((element) => normalize(element.textContent)).filter(Boolean),
            form: rect('[data-launch-subscribe-form]'),
            input: rect('input[type="email"]'),
            button: rect('button[type="submit"]'),
            logo: rect('.launch__brand img, [data-launch-brand] img'),
            logoSrc: brandImage instanceof HTMLImageElement ? brandImage.currentSrc || brandImage.src : null,
            status: rect('.launch__status'),
          };
        });
        const image = await screenshot(page, `mobile-${width}x${height}`, {
          requirementIds: ['R19', ...(width === 390 ? ['R21'] : [])],
        });
        assert.equal(metrics.tileCount, 72, 'Expected 72 tiles on mobile.');
        assert.equal(metrics.gridColumns, 6, `Expected six mobile grid columns, got ${metrics.gridColumns}.`);
        assert(metrics.scrollWidth <= width + 1, `Mobile has horizontal overflow: ${metrics.scrollWidth}px for ${width}px viewport.`);
        assert.deepEqual(metrics.h1Lines, ['Полюбить', 'Калининград', 'Анонсы']);
        assert.equal(metrics.statusText.toLocaleUpperCase('ru-RU'), 'СКОРО ЗАПУСК • 1 СЕНТЯБРЯ');
        assert.equal(metrics.dateText.toLocaleUpperCase('ru-RU'), '1 СЕНТЯБРЯ');
        assert.deepEqual(metrics.descriptionLines, [
          'Персонализированный сервис анонсов',
          'и навигатор по культурным',
          'и просветительским событиям',
          'Калининградской области',
        ]);
        assert(metrics.header && metrics.mosaic && metrics.copy, 'Mobile header/mosaic/copy geometry missing.');
        assert(metrics.mosaic.top >= metrics.header.bottom - 2, 'Mobile mosaic must follow the header.');
        assert(metrics.copy.top >= metrics.mosaic.bottom - 2, 'Mobile copy must follow the mosaic without overlap.');
        assert(metrics.h1.left >= -1 && metrics.h1.right <= width + 1, 'Mobile headline overflows the viewport.');
        assert(metrics.form.left >= -1 && metrics.form.right <= width + 1, 'Mobile form overflows the viewport.');
        assert(metrics.input && metrics.button && metrics.button.top >= metrics.input.bottom - 1, 'Mobile form must stack the button below the input.');
        assert(metrics.input.width >= width * 0.82 && metrics.button.width >= width * 0.82, 'Mobile form controls should use the available width.');
        assert(metrics.logo && metrics.status && metrics.logo.right <= metrics.status.left + 2, 'Mobile logo and launch status overlap.');
        assert(Math.abs(metrics.logo.width - metrics.logo.height) <= 2, 'Mobile brand PWA logo must be square.');
        assert(/PWA-icon\.png(?:$|\?)/iu.test(metrics.logoSrc || ''), `Unexpected mobile logo source: ${metrics.logoSrc}`);
        const fixture = { width, height, image, metrics };
        report.fixtures.mobile.push(fixture);
        return { image, scrollWidth: metrics.scrollWidth };
      } finally {
        await context.close().catch(() => {});
      }
    });
  }

  report.animation = await scenario('animation-0-5-10-seconds', ['R20'], async () => {
    const { context, page } = await createPage({ width: 1672, height: 941 });
    try {
      await ready(page);
      const captureState = () => page.locator('.mosaic__tile').evaluateAll((tiles) => tiles.map((tile) => tile.getAttribute('data-state')));
      const started = Date.now();
      const states0 = await captureState();
      const frame0 = await screenshot(page, 'animation-00s-1672x941', { requirementIds: ['R20'], elapsedSeconds: 0 });
      await page.waitForTimeout(Math.max(0, 5_000 - (Date.now() - started)));
      const states5 = await captureState();
      const frame5 = await screenshot(page, 'animation-05s-1672x941', { requirementIds: ['R20'], elapsedSeconds: 5 });
      await page.waitForTimeout(Math.max(0, 10_000 - (Date.now() - started)));
      const states10 = await captureState();
      const frame10 = await screenshot(page, 'animation-10s-1672x941', { requirementIds: ['R20'], elapsedSeconds: 10 });
      assert(states0.join('|') !== states5.join('|') || states5.join('|') !== states10.join('|'), 'Normal-motion tile states did not change across 0/5/10 seconds.');
      return { frames: [frame0, frame5, frame10], changedAt5: states0.join('|') !== states5.join('|'), changedAt10: states5.join('|') !== states10.join('|') };
    } finally {
      await context.close().catch(() => {});
    }
  });

  report.reducedMotion = await scenario('reduced-motion-static', ['R20'], async () => {
    const { context, page } = await createPage({ width: 1366, height: 768, reducedMotion: 'reduce' });
    try {
      await ready(page);
      const state0 = await page.locator('.mosaic__tile').evaluateAll((tiles) => tiles.map((tile) => tile.getAttribute('data-state')));
      const light0 = await page.locator('.mosaic').evaluate((element) => [element.style.getPropertyValue('--light-x'), element.style.getPropertyValue('--light-y')]);
      await page.waitForTimeout(5_200);
      const state5 = await page.locator('.mosaic__tile').evaluateAll((tiles) => tiles.map((tile) => tile.getAttribute('data-state')));
      const light5 = await page.locator('.mosaic').evaluate((element) => [element.style.getPropertyValue('--light-x'), element.style.getPropertyValue('--light-y')]);
      assert.deepEqual(state5, state0, 'Reduced-motion tile state changed.');
      assert.deepEqual(light5, light0, 'Reduced-motion light position changed.');
      const image = await screenshot(page, 'reduced-motion-static-1366x768', { requirementIds: ['R20'] });
      return { image, stateStable: true, lightStable: true };
    } finally {
      await context.close().catch(() => {});
    }
  });

  report.projection = await scenario('projection-pwa-and-generic-runtime', ['R22'], async () => {
    const { context, page } = await createPage({ width: 1672, height: 941 });
    try {
      await ready(page);
      const defaultProjection = await page.locator('[data-mosaic-image]').evaluate((image) => ({
        src: image.currentSrc || image.src,
        naturalWidth: image.naturalWidth,
        naturalHeight: image.naturalHeight,
        objectPosition: getComputedStyle(image).objectPosition,
        mode: image.closest('[data-image-mode]')?.getAttribute('data-image-mode')
          || document.querySelector('[data-tile-mosaic-launch]')?.getAttribute('data-image-mode')
          || image.getAttribute('data-image-mode'),
      }));
      assert(/PWA-icon\.png(?:$|\?)/iu.test(defaultProjection.src), `Default projection is not PWA-icon.png: ${defaultProjection.src}`);
      assert(defaultProjection.naturalWidth > 0 && defaultProjection.naturalHeight > 0, 'Default PWA projection failed to load.');
      assert.equal(defaultProjection.mode, 'brand', `Default projection mode must be brand, got ${defaultProjection.mode}.`);
      const pwaImage = await screenshot(page, 'projection-pwa-1672x941', { requirementIds: ['R22'], projectionMode: 'brand' });

      const photoUrl = (() => {
        if (/^https?:\/\//iu.test(options.photoPath)) return options.photoPath;
        const marker = '/assets/';
        const markerIndex = defaultProjection.src.indexOf(marker);
        assert(markerIndex >= 0, `Cannot infer deployed asset base from ${defaultProjection.src}`);
        return `${defaultProjection.src.slice(0, markerIndex)}${options.photoPath.startsWith('/') ? '' : '/'}${options.photoPath}`;
      })();
      await page.evaluate(({ src }) => {
        window.dispatchEvent(new CustomEvent('tile-mosaic:set-image', {
          detail: { src, focalX: 0.23, focalY: 0.71, mode: 'cover', projectionMode: 'generic' },
        }));
      }, { src: photoUrl });
      await page.waitForFunction((src) => {
        const image = document.querySelector('[data-mosaic-image]');
        return image instanceof HTMLImageElement && image.src === src && image.complete && image.naturalWidth > 0;
      }, photoUrl);
      const generic = await page.locator('[data-mosaic-image]').evaluate((image) => ({
        src: image.currentSrc || image.src,
        naturalWidth: image.naturalWidth,
        objectPosition: getComputedStyle(image).objectPosition,
        mode: image.closest('[data-image-mode]')?.getAttribute('data-image-mode')
          || document.querySelector('[data-tile-mosaic-launch]')?.getAttribute('data-image-mode')
          || image.getAttribute('data-image-mode'),
      }));
      assert.equal(generic.src, photoUrl, 'Runtime event did not switch to the requested local photo.');
      assert.equal(generic.objectPosition, '23% 71%', `Runtime focal point mismatch: ${generic.objectPosition}`);
      assert.equal(generic.mode, 'cover', `Runtime projection mode must be cover, got ${generic.mode}.`);
      const genericImage = await screenshot(page, 'projection-generic-photo-event-1672x941', { requirementIds: ['R22'], projectionMode: 'cover' });

      const queryUrl = getPageUrl(options.baseUrl, {
        mosaicImage: photoUrl,
        focalX: '0.77',
        focalY: '0.31',
        mosaicMode: 'cover',
        projectionMode: 'generic',
      });
      await ready(page, queryUrl);
      const queryProjection = await page.locator('[data-mosaic-image]').evaluate((image) => ({
        src: image.currentSrc || image.src,
        objectPosition: getComputedStyle(image).objectPosition,
        mode: image.closest('[data-image-mode]')?.getAttribute('data-image-mode')
          || document.querySelector('[data-tile-mosaic-launch]')?.getAttribute('data-image-mode')
          || image.getAttribute('data-image-mode'),
      }));
      assert.equal(queryProjection.src, photoUrl, 'Query contract did not select the local photo.');
      assert.equal(queryProjection.objectPosition, '77% 31%', `Query focal point mismatch: ${queryProjection.objectPosition}`);
      assert.equal(queryProjection.mode, 'cover', `Query projection mode must be cover, got ${queryProjection.mode}.`);
      return {
        pwaImage,
        genericImage,
        genericAssetPath: options.photoPath,
        runtimeFocalPoint: [0.23, 0.71],
        queryFocalPoint: [0.77, 0.31],
        sameImageElement: true,
      };
    } finally {
      await context.close().catch(() => {});
    }
  });

  const formScenario = async (name, mode, { duplicate = false, liveEmail = null } = {}) => scenario(name, ['R17'], async () => {
    let rpcRequests = 0;
    const routeSetup = mode === 'mock-success' || mode === 'network-error'
      ? async (page) => {
          await page.route('**/*', async (route) => {
            const requestUrl = route.request().url();
            if (requestUrl.includes('/rpc/transport_probe_v1')) {
              const payload = route.request().postDataJSON();
              return route.fulfill({
                status: 200,
                contentType: 'application/json',
                body: JSON.stringify({ nonce: payload?.p_nonce, schema: 1 }),
              });
            }
            if (!requestUrl.includes('/rpc/subscribe_site_launch_v1')) return route.continue();
            rpcRequests += 1;
            if (mode === 'network-error') {
              return route.fulfill({ status: 503, contentType: 'application/json', body: '{"message":"synthetic outage"}' });
            }
            return route.fulfill({ status: 200, contentType: 'application/json', body: '[{"accepted":true,"status":"subscribed"}]' });
          });
        }
      : null;
    const { context, page } = await createPage({ width: 1366, height: 768 }, routeSetup);
    try {
      await ready(page);
      const form = page.locator('[data-launch-subscribe-form]');
      const email = page.locator('input[name="email"]');
      const status = page.locator('[data-launch-subscribe-form] [role="status"]');
      const testEmail = liveEmail || 'qa.tile.mosaic@example.invalid';

      if (mode === 'invalid') {
        await email.fill('invalid-email');
        await page.locator('button[type="submit"]').click();
        assert.equal(await email.evaluate((input) => input.validity.valid), false, 'Invalid email unexpectedly passed native validity.');
        assert.notEqual(await form.getAttribute('data-state'), 'submitting');
        assert.equal(rpcRequests, 0);
      } else {
        await email.fill(testEmail);
        if (mode === 'honeypot') {
          await page.locator('input[name="company"]').evaluate((input) => { input.value = 'bot'; });
        }
        await page.locator('button[type="submit"]').click();
        await page.waitForFunction(() => ['success', 'error'].includes(document.querySelector('[data-launch-subscribe-form]')?.getAttribute('data-state')), null, { timeout: 20_000 });

        const expectedState = mode === 'network-error' ? 'error' : 'success';
        assert.equal(await form.getAttribute('data-state'), expectedState);
        if (mode === 'network-error') {
          assert(rpcRequests >= 1, 'Network-error test did not intercept an RPC request; preview may lack public Supabase configuration.');
          assert.equal(await email.inputValue(), testEmail, 'Network error must preserve the entered email.');
          assert.match(normalizedText(await status.textContent()), /не удалось|недоступ/iu);
        } else if (mode === 'honeypot') {
          assert.equal(rpcRequests, 0, 'Honeypot submission must not call the RPC.');
          assert.match(normalizedText(await status.textContent()), /спасибо|сообщим/iu);
        } else {
          assert(rpcRequests >= 1 || liveEmail, 'Mock success did not reach the RPC route.');
          assert.equal(await email.inputValue(), '', 'Successful submission should clear the email field.');
          assert.match(normalizedText(await status.textContent()), /готово|напомним/iu);
        }

        if (duplicate) {
          await email.fill(testEmail);
          await page.locator('button[type="submit"]').click();
          await page.waitForFunction(() => document.querySelector('[data-launch-subscribe-form]')?.getAttribute('data-state') === 'success', null, { timeout: 20_000 });
          assert.equal(await email.inputValue(), '', 'Duplicate success should clear the field.');
          if (!liveEmail) assert(rpcRequests >= 2, 'Duplicate test expected two RPC requests.');
        }
      }
      const evidence = {
        state: await form.getAttribute('data-state') || 'native-invalid',
        rpcRequests: liveEmail ? 'not-recorded' : rpcRequests,
        duplicate,
        emailRedacted: true,
      };
      report.form.push({ name, status: 'passed', ...evidence });
      return evidence;
    } finally {
      await context.close().catch(() => {});
    }
  });

  await formScenario('form-invalid-native', 'invalid');
  await formScenario('form-honeypot', 'honeypot');
  await formScenario('form-success-mocked', 'mock-success');
  await formScenario('form-duplicate-mocked', 'mock-success', { duplicate: true });
  await formScenario('form-network-error', 'network-error');

  const liveEmail = process.env.TILE_MOSAIC_LIVE_EMAIL?.trim() || '';
  if (liveEmail) {
    await formScenario('form-live-success-and-duplicate', 'live', { duplicate: true, liveEmail });
    report.form.push({ name: 'live-hook', status: 'executed', emailRedacted: true });
  } else {
    report.scenarios.push({
      name: 'form-live-success-and-duplicate',
      requirementIds: ['R17'],
      status: 'skipped',
      reason: 'Set TILE_MOSAIC_LIVE_EMAIL explicitly to exercise the configured backend twice. Mocked success and duplicate remain mandatory.',
    });
    report.form.push({ name: 'live-hook', status: 'skipped', emailRedacted: true });
  }

  for (const [id, requirement] of Object.entries(report.requirements)) {
    const associated = report.scenarios.filter((item) => item.requirementIds.includes(id) && item.status !== 'skipped');
    requirement.status = associated.length > 0 && associated.every((item) => item.status === 'passed') ? 'passed' : 'failed';
  }

  report.consoleFindings = consoleFindings;
  report.finishedAt = new Date().toISOString();
  report.durationMs = Date.parse(report.finishedAt) - Date.parse(startedAt);
  report.summary = {
    status: report.failures.length === 0 ? 'passed' : 'failed',
    passedScenarios: report.scenarios.filter((item) => item.status === 'passed').length,
    failedScenarios: report.scenarios.filter((item) => item.status === 'failed').length,
    skippedScenarios: report.scenarios.filter((item) => item.status === 'skipped').length,
    note: 'Viewport emulation and headless Chromium are L1 browser evidence only; native mobile registry scenarios remain L2.',
  };

  for (const artifact of frameArtifacts) {
    const absolute = resolve(artifactDir, artifact.filename);
    const contents = await readFile(absolute);
    const info = await stat(absolute);
    const pngWidth = contents.subarray(1, 4).toString('ascii') === 'PNG' ? contents.readUInt32BE(16) : null;
    const pngHeight = contents.subarray(1, 4).toString('ascii') === 'PNG' ? contents.readUInt32BE(20) : null;
    report.artifacts.push({
      ...artifact,
      bytes: info.size,
      pixelWidth: pngWidth,
      pixelHeight: pngHeight,
      sha256: createHash('sha256').update(contents).digest('hex'),
    });
  }
  const reportPath = resolve(artifactDir, 'report.json');
  await writeFile(reportPath, `${JSON.stringify(report, null, 2)}\n`, 'utf8');
  process.stdout.write(`${JSON.stringify({
    status: report.summary.status,
    report: reportPath,
    artifactDir,
    requirements: Object.fromEntries(Object.entries(report.requirements).map(([id, value]) => [id, value.status])),
    failures: report.failures,
  }, null, 2)}\n`);

  if (browser.isConnected()) await browser.close();
  if (report.failures.length > 0) process.exitCode = 1;
};

main().catch((error) => {
  process.stderr.write(`${error?.stack || error}\n`);
  process.exitCode = 1;
});
