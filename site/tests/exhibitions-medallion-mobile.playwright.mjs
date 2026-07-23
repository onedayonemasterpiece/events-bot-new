import assert from 'node:assert/strict';
import { mkdir, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { chromium } from 'playwright';

const url = process.env.EXHIBITIONS_URL;
assert.ok(url, 'Set EXHIBITIONS_URL to a built /vystavki/ or /lab/exhibitions-personal/ URL');

const outputDir = path.resolve(process.env.EXHIBITIONS_ARTIFACT_DIR || 'artifacts/exhibitions-medallion-mobile');
const executablePath = process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH || undefined;
const viewportWidth = Number(process.env.EXHIBITIONS_VIEWPORT_WIDTH || 390);
const viewportHeight = Number(process.env.EXHIBITIONS_VIEWPORT_HEIGHT || 844);
assert.ok([320, 390, 430].includes(viewportWidth), 'EXHIBITIONS_VIEWPORT_WIDTH must be 320, 390, or 430');
await mkdir(outputDir, { recursive: true });

const browser = await chromium.launch({
  headless: true,
  ...(executablePath ? { executablePath } : {}),
});

try {
  const page = await browser.newPage({ viewport: { width: viewportWidth, height: viewportHeight } });
  const consoleErrors = [];
  page.on('console', (message) => {
    if (message.type() === 'error') consoleErrors.push(message.text());
  });
  page.on('pageerror', (error) => consoleErrors.push(error.message));

  const response = await page.goto(url, { waitUntil: 'networkidle', timeout: 120_000 });
  assert.equal(response?.status(), 200);

  const seal = page.locator('[data-exhibition-medallion]:visible').first();
  await seal.scrollIntoViewIfNeeded();
  await seal.locator('img').evaluate(async (image) => {
    if (!image.complete) {
      await new Promise((resolve) => image.addEventListener('load', resolve, { once: true }));
    }
  });

  const measurements = await seal.evaluate((element) => {
    const box = element.getBoundingClientRect();
    const deck = element.closest('[data-deck]')?.getBoundingClientRect();
    const row = element.closest('[data-exhibition-row]')?.getBoundingClientRect();
    const counterElement = element.closest('[data-deck]')?.querySelector('[data-deck-count]:not([hidden])');
    const counter = counterElement?.getBoundingClientRect();
    const image = element.querySelector('img');
    const overlapsCounter = counter
      ? !(box.right <= counter.left || box.left >= counter.right || box.bottom <= counter.top || box.top >= counter.bottom)
      : false;
    return {
      viewport: { width: innerWidth, height: innerHeight },
      document: {
        clientWidth: document.documentElement.clientWidth,
        scrollWidth: document.documentElement.scrollWidth,
      },
      seal: {
        width: box.width,
        height: box.height,
        x: box.x,
        y: box.y,
        imageComplete: image?.complete || false,
        imageNaturalWidth: image?.naturalWidth || 0,
        imageNaturalHeight: image?.naturalHeight || 0,
      },
      deck: deck && { x: deck.x, y: deck.y, width: deck.width, height: deck.height },
      row: row && { x: row.x, y: row.y, width: row.width, height: row.height },
      overlapsCounter,
    };
  });

  assert.deepEqual(measurements.viewport, { width: viewportWidth, height: viewportHeight });
  assert.equal(measurements.seal.width, 44);
  assert.equal(measurements.seal.height, 44);
  assert.ok(measurements.seal.imageComplete);
  assert.ok(measurements.seal.imageNaturalWidth > 0);
  assert.ok(measurements.seal.imageNaturalHeight > 0);
  assert.equal(measurements.document.scrollWidth, measurements.document.clientWidth);
  assert.ok(measurements.deck);
  assert.ok(measurements.row);
  assert.ok(measurements.seal.x >= measurements.deck.x);
  assert.ok(measurements.seal.y >= measurements.deck.y);
  assert.ok(measurements.seal.x + measurements.seal.width <= measurements.deck.x + measurements.deck.width);
  assert.ok(measurements.seal.y + measurements.seal.height <= measurements.deck.y + measurements.deck.height);
  assert.equal(measurements.overlapsCounter, false);
  assert.deepEqual(consoleErrors, []);

  const screenshot = path.join(outputDir, `exhibitions-medallion-${viewportWidth}.png`);
  await page.screenshot({ path: screenshot });
  const report = {
    url,
    status: response?.status(),
    screenshot,
    consoleErrors,
    measurements,
  };
  await writeFile(
    path.join(outputDir, `exhibitions-medallion-${viewportWidth}.json`),
    `${JSON.stringify(report, null, 2)}\n`,
  );
  process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
} finally {
  await browser.close();
}
