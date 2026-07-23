import { mkdirSync, readFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { expect, test } from '../../site/node_modules/playwright/test.mjs';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../..');
const buildId = process.env.PREVIEW_BUILD_ID || 'preview-l02-medallion-audit-20260723';
const baseURL = process.env.MEDALLION_PREVIEW_URL || `http://127.0.0.1:4321/${buildId}`;
const evidenceDir = path.join(root, 'artifacts/codex/L02-medallion-audit/playwright');
const previewEvents = JSON.parse(readFileSync(path.join(root, 'site/src/data/preview-events.json'), 'utf8')).events;
const eventById = new Map(previewEvents.map((event) => [event.id, event]));

const expectedOrganizerSlugs = [
  'world-ocean-museum', 'history-art-museum', 'kaliningrad-philharmonic',
  'kant-island', 'dom-kitoboya', 'tretyakovka-kaliningrad', 'konb',
  'act-opus', 'znanie-russia', 'kgd80', 'kantata-festival', 'kaup',
  'dramteatr39', 'yantar-hall', 'muzteatr39', 'dom-iskusstv',
  'city-jazz-club', 'rostec-arena', 'bar-bastion', 'signal', 'mumod',
  'kldzoo', 'locostandup', 'kaliningrad-art-museum', 'brachert',
  'ruin-keepers', 'greza-khutor',
];
const expectedFestivalSlugs = [
  'kgd80-80-stories', 'kaliningrad-city-jazz', 'kaliningrad-street-food',
  'grozd-festival', 'koroche', 'ostrova', 'more-vnutri',
  'simfoniya-vetra', 'bahosluzhenie', 'tolkin-fest',
];

mkdirSync(evidenceDir, { recursive:true });

for (const viewport of [
  { name:'desktop-1440', width:1440, height:1100 },
  { name:'mobile-390', width:390, height:844 },
]) {
  test(`${viewport.name}: exact catalog, loaded images and bounded layout`, async ({ page }) => {
    await page.setViewportSize({ width:viewport.width, height:viewport.height });
    await page.goto(`${baseURL}/lab/medallions/`, { waitUntil:'networkidle' });

    const organizers = page.locator('[data-medallion-catalog="organizers"] > [data-medallion-slug]');
    const festivals = page.locator('[data-medallion-catalog="festivals"] > [data-medallion-slug]');
    const venueBrands = page.locator('[data-medallion-catalog="venue-brands"] > [data-medallion-slug]');
    await expect(organizers).toHaveCount(27);
    await expect(festivals).toHaveCount(10);
    await expect(venueBrands).toHaveCount(1);
    expect(await organizers.evaluateAll((nodes) => nodes.map((node) => node.dataset.medallionSlug))).toEqual(expectedOrganizerSlugs);
    expect(await festivals.evaluateAll((nodes) => nodes.map((node) => node.dataset.medallionSlug))).toEqual(expectedFestivalSlugs);
    expect(await venueBrands.evaluateAll((nodes) => nodes.map((node) => node.dataset.medallionSlug))).toEqual(['kaup']);

    for (const slug of ['mumod', 'greza-khutor']) {
      const item = page.locator(`[data-medallion-catalog="organizers"] > [data-medallion-slug="${slug}"]`);
      await expect(item).toBeVisible();
      await expect(item.locator('img')).toHaveCount(1);
    }

    const images = page.locator('main img');
    for (let index = 0; index < await images.count(); index += 1) {
      await images.nth(index).scrollIntoViewIfNeeded();
    }
    await page.waitForFunction(() => Array.from(document.querySelectorAll('main img'))
      .every((image) => image.complete));
    const imageFailures = await page.locator('main img').evaluateAll((images) => images
      .filter((image) => !image.complete || image.naturalWidth === 0 || image.naturalHeight === 0)
      .map((image) => image.currentSrc || image.getAttribute('src')));
    expect(imageFailures).toEqual([]);

    const overflow = await page.evaluate(() => ({
      document:[document.documentElement.scrollWidth, document.documentElement.clientWidth],
      tokens:Array.from(document.querySelectorAll('[data-medallion-slug]'))
        .filter((node) => node.scrollWidth > node.clientWidth + 1)
        .map((node) => ({
          slug:node.getAttribute('data-medallion-slug'),
          scrollWidth:node.scrollWidth,
          clientWidth:node.clientWidth,
        })),
    }));
    expect(overflow.document[0]).toBeLessThanOrEqual(overflow.document[1] + 1);
    expect(overflow.tokens).toEqual([]);

    await page.screenshot({
      path:path.join(evidenceDir, `${viewport.name}-full.png`),
      fullPage:true,
    });
  });

  test(`${viewport.name}: real event pages resolve curated marks`, async ({ page }) => {
    await page.setViewportSize({ width:viewport.width, height:viewport.height });
    for (const [eventId, expectedAsset] of [
      [4211, '/assets/festivals/more-vnutri.svg'],
      [6153, '/assets/festivals/bahosluzhenie.webp'],
      [6529, '/assets/organizers/mumod.svg'],
      [5756, '/assets/organizers/dramteatr39.svg'],
      [6796, '/assets/festivals/kaup.svg'],
      [698, '/assets/badges/pushkin-card-medallion.webp'],
    ]) {
      const event = eventById.get(eventId);
      expect(event, `preview event ${eventId}`).toBeTruthy();
      await page.goto(`${baseURL}/sobytiya/${event.slug}/`, { waitUntil:'networkidle' });
      const matchingImages = page.locator(`main img[src$="${expectedAsset}"], main source[srcset$="${expectedAsset}"]`);
      expect(await matchingImages.count(), `${eventId} should render ${expectedAsset}`).toBeGreaterThan(0);
      const tokenImages = page.locator('main .event-token-section:visible img');
      for (let index = 0; index < await tokenImages.count(); index += 1) {
        await tokenImages.nth(index).scrollIntoViewIfNeeded();
      }
      await tokenImages.evaluateAll(async (images) => {
        await Promise.all(images.map(async (image) => {
          image.loading = 'eager';
          if (!image.complete) {
            await new Promise((resolve) => {
              image.addEventListener('load', resolve, { once:true });
              image.addEventListener('error', resolve, { once:true });
            });
          }
        }));
      });
      const failures = await tokenImages.evaluateAll((images) => images
        .filter((image) => !image.complete || image.naturalWidth === 0 || image.naturalHeight === 0)
        .map((image) => image.currentSrc || image.getAttribute('src')));
      expect(failures, `broken images on ${eventId}`).toEqual([]);
      const width = await page.evaluate(() => [document.documentElement.scrollWidth, document.documentElement.clientWidth]);
      expect(width[0], `horizontal overflow on ${eventId}`).toBeLessThanOrEqual(width[1] + 1);
    }

    const meow = eventById.get(6911);
    await page.goto(`${baseURL}/sobytiya/${meow.slug}/`, { waitUntil:'networkidle' });
    await expect(page.locator('.event-token--meow-afisha')).toHaveCount(0);
  });
}
