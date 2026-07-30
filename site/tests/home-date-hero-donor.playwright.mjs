import assert from 'node:assert/strict';
import { chromium } from 'playwright';

const base = String(process.env.HERO_DONOR_BASE_URL || '').replace(/\/+$/u, '');
if (!base) throw new Error('HERO_DONOR_BASE_URL is required');
const datePath = process.env.HERO_DONOR_DATE_PATH || '/date-2026-08-08/';
const browser = await chromium.launch({ headless:true });

try {
  const desktop = await browser.newPage({ viewport:{ width:1440, height:900 } });
  await desktop.goto(`${base}/`, { waitUntil:'networkidle' });
  assert.equal(await desktop.locator('a[data-home-hero-scene]').count(), 0);
  assert.ok(await desktop.locator('[data-home-hero-fragment][href]').count() > 0);
  const narrativeContract = await desktop.evaluate(() => {
    const scenes = [...document.querySelectorAll('[data-home-hero-scene]')];
    const active = document.querySelector('[data-home-hero-scene].is-active');
    return {
      sceneCount:scenes.length,
      eventCount:new Set(scenes.map((scene) => scene.getAttribute('data-event-id')).filter(Boolean)).size,
      greeting:document.querySelectorAll('[data-editorial-id="greeting-day"]').length,
      localVoice:document.querySelectorAll('[data-editorial-id="local-keska"]').length,
      brandO:document.querySelectorAll('[data-home-hero-brand-o]').length,
      cursors:document.querySelectorAll('[data-home-hero-cursor]').length,
      activeCursors:active?.querySelectorAll('[data-home-hero-cursor]').length || 0,
    };
  });
  assert.ok(narrativeContract.sceneCount >= 20);
  assert.ok(narrativeContract.eventCount >= 16);
  assert.equal(narrativeContract.greeting, 1);
  assert.equal(narrativeContract.localVoice, 1);
  assert.equal(narrativeContract.brandO, 1);
  assert.equal(narrativeContract.cursors, narrativeContract.sceneCount);
  assert.equal(narrativeContract.activeCursors, 1);
  await desktop.waitForFunction(() => (
    document.querySelector('[data-home-hero-scene].is-active[data-mode="photo-mosaic"] [data-home-hero-mosaic]')
      ?.getAttribute('data-ready') === 'true'
  ), { timeout:26_000 });
  await desktop.waitForTimeout(1_500);
  const home = await desktop.evaluate(() => {
    const hero = document.querySelector('[data-home-hero-talk]');
    const scene = document.querySelector('[data-home-hero-scene].is-active');
    const media = scene.querySelector('[data-home-hero-media]');
    const mosaic = scene.querySelector('[data-home-hero-mosaic]');
    const preload = scene.querySelector('[data-home-hero-image]');
    const tiles = [...scene.querySelectorAll('[data-home-hero-tile]:not([hidden])')];
    const opacity = tiles.map((tile) => Number(getComputedStyle(tile).opacity));
    const rect = (node) => {
      const value = node.getBoundingClientRect();
      return { x:value.x, width:value.width, right:value.right, height:value.height };
    };
    return {
      hero:rect(hero),
      media:rect(media),
      preload:rect(preload),
      columns:Number(mosaic.dataset.columns),
      rows:Number(mosaic.dataset.rows),
      tileCount:tiles.length,
      partialCount:opacity.filter((value) => value < .98).length,
      fullCount:opacity.filter((value) => value >= .999).length,
      rowGap:tiles[Number(mosaic.dataset.columns)].getBoundingClientRect().y
        - tiles[0].getBoundingClientRect().bottom,
      overflow:document.documentElement.scrollWidth - document.documentElement.clientWidth,
    };
  });
  assert.equal(home.hero.x, 0);
  assert.equal(Math.round(home.hero.width), 1440);
  assert.ok(Math.abs(home.media.right - 1440) <= 1);
  assert.equal(home.columns, 16);
  assert.equal(home.rows, 5);
  assert.equal(home.tileCount, 80);
  assert.ok(home.partialCount > home.tileCount * .7);
  assert.ok(home.fullCount < home.tileCount * .2);
  assert.ok(Math.abs(home.rowGap) < .2);
  assert.equal(home.preload.width, 1);
  assert.equal(home.overflow, 0);
  for (const [width, columns] of [[1536, 18], [1920, 20]]) {
    await desktop.setViewportSize({ width, height:900 });
    await desktop.waitForTimeout(250);
    const responsive = await desktop.evaluate(() => {
      const scene = document.querySelector('[data-home-hero-scene].is-active');
      const media = scene.querySelector('[data-home-hero-media]').getBoundingClientRect();
      const mosaic = scene.querySelector('[data-home-hero-mosaic]');
      return {
        right:media.right,
        columns:Number(mosaic.dataset.columns),
        tiles:scene.querySelectorAll('[data-home-hero-tile]:not([hidden])').length,
        overflow:document.documentElement.scrollWidth - document.documentElement.clientWidth,
      };
    });
    assert.ok(Math.abs(responsive.right - width) <= 1);
    assert.equal(responsive.columns, columns);
    assert.equal(responsive.tiles, columns * 5);
    assert.equal(responsive.overflow, 0);
  }
  await desktop.close();

  const mobile = await browser.newPage({ viewport:{ width:390, height:844 } });
  await mobile.goto(`${base}${datePath}`, { waitUntil:'domcontentloaded' });
  const initial = await mobile.evaluate(() => {
    const hero = document.querySelector('[data-date-listing-hero]');
    const image = hero?.querySelector('img');
    return {
      y:hero?.getBoundingClientRect().y,
      right:hero?.getBoundingClientRect().right,
      imageWidth:image?.getBoundingClientRect().width,
      tiles:hero?.querySelectorAll('[data-date-listing-hero-tile]').length,
    };
  });
  assert.equal(initial.y, 0);
  assert.equal(initial.right, 390);
  assert.equal(initial.imageWidth, 1);
  assert.equal(initial.tiles, 66);
  await mobile.waitForSelector('[data-date-listing-hero][data-tiles-ready]');
  await mobile.waitForTimeout(1_500);
  const measure = () => mobile.evaluate(() => {
    const hero = document.querySelector('[data-date-listing-hero]');
    const tiles = [...hero.querySelectorAll('[data-date-listing-hero-tile]')];
    const opacity = tiles.map((tile) => Number(getComputedStyle(tile).opacity));
    const mean = (values) => values.reduce((sum, value) => sum + value, 0) / values.length;
    return {
      seed:hero.dataset.loadSeed,
      opacity,
      maximum:Math.max(...opacity),
      low:opacity.filter((value) => value <= .061).length,
      topRight:mean(tiles.filter((tile) => +tile.dataset.row <= 1 && +tile.dataset.col >= 9).map((tile) => Number(getComputedStyle(tile).opacity))),
      bottomLeft:mean(tiles.filter((tile) => +tile.dataset.row >= 4 && +tile.dataset.col <= 4).map((tile) => Number(getComputedStyle(tile).opacity))),
      overflow:document.documentElement.scrollWidth - document.documentElement.clientWidth,
    };
  });
  const first = await measure();
  assert.ok(first.low >= 20);
  assert.ok(first.maximum <= .92);
  assert.ok(first.topRight > first.bottomLeft * 6);
  assert.equal(first.overflow, 0);
  await mobile.setViewportSize({ width:390, height:760 });
  await mobile.waitForTimeout(200);
  const resized = await measure();
  assert.equal(resized.seed, first.seed);
  assert.deepEqual(resized.opacity, first.opacity);
  await mobile.reload({ waitUntil:'networkidle' });
  await mobile.waitForSelector('[data-date-listing-hero][data-tiles-ready]');
  const reloaded = await measure();
  assert.notEqual(reloaded.seed, first.seed);
  assert.ok(reloaded.topRight > reloaded.bottomLeft * 6);
  await mobile.close();
} finally {
  await browser.close();
}
