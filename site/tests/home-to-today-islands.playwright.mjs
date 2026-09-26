import assert from 'node:assert/strict';
import { chromium } from 'playwright';

const base = process.env.CHECK_BASE?.replace(/\/$/u, '');
if (!base) throw new Error('CHECK_BASE is required');
const browser = await chromium.launch({
  executablePath: process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH || undefined,
});

try {
  for (const width of [390, 1440]) {
    const page = await browser.newPage({ viewport: { width, height: 900 } });
    const errors = [];
    page.on('pageerror', (error) => errors.push(error.message));
    await page.goto(`${base}/`, { waitUntil: 'load' });
    assert.equal(await page.locator('body').getAttribute('data-shell-composition'), 'home-navigation-only');
    await page.locator('[data-home-quick-nav] a[href$="/segodnya/"]').first().click();
    await page.waitForURL('**/segodnya/');
    await page.waitForFunction(() => document.body.dataset.fiMotion === 'ready');

    const atEntry = await page.evaluate(() => ({
      shell: document.body.dataset.shellComposition,
      islands: document.body.dataset.floatingIslands,
      headerBackground: getComputedStyle(document.querySelector('.site-header')).backgroundColor,
      topBand: Boolean(document.querySelector('[data-floating-top-band]')),
    }));
    assert.equal(atEntry.shell, 'contextual');
    assert.equal(atEntry.islands, 'site');
    assert.equal(atEntry.headerBackground, 'rgba(0, 0, 0, 0)');
    assert.equal(atEntry.topBand, true);

    await page.evaluate(() => window.scrollTo(0, 700));
    await page.waitForTimeout(700);
    if (width < 760) {
      const city = page.locator('[data-mobile-listing-rails] [data-listing-controls]:visible').first();
      assert.equal(await city.getAttribute('data-fi-docked'), 'true');
      const box = await city.boundingBox();
      assert.ok(box && box.y >= 12 && box.y <= 28, `mobile city island drifted: ${JSON.stringify(box)}`);
      assert.equal(await page.locator('body').getAttribute('data-fi-single-day'), '');
    } else {
      assert.equal(await page.locator('.fi-desktop-layer').count(), 1);
      assert.equal(await page.locator('.fi-desktop-layer .site-header__context:visible').count(), 1);
    }
    assert.deepEqual(errors, []);
    await page.close();
    console.log(`PASS home → Today shared islands at ${width}px`);
  }
} finally {
  await browser.close();
}
