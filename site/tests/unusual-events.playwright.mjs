import assert from 'node:assert/strict';
import { chromium } from 'playwright';

const rawBase = process.env.UNUSUAL_EVENTS_BASE_URL;
if (!rawBase) throw new Error('UNUSUAL_EVENTS_BASE_URL is required');
const base = rawBase.replace(/\/+$/u, '');
const expectedApproved = process.env.UNUSUAL_EXPECT_APPROVED === '1';
const route = (path) => {
  const clean = path.replace(/^\/+|\/+$/gu, '');
  return clean ? `${base}/${clean}/` : `${base}/`;
};

const browser = await chromium.launch({ headless: true });
try {
  const context = await browser.newContext({ viewport: { width: 390, height: 844 }, reducedMotion: 'reduce' });
  const page = await context.newPage();
  const providerRequests = [];
  page.on('request', (request) => {
    if (/generativelanguage|googleapis.*embed|gemini|bge-m3|unusual.*(?:score|embed)/iu.test(request.url())) {
      providerRequests.push(`${request.method()} ${request.url()}`);
    }
  });

  await page.goto(route('/'), { waitUntil: 'domcontentloaded' });
  assert.equal(await page.locator('[data-home-page]').count(), 1);
  assert.equal(await page.locator('[data-home-hero-talk]').count(), 1);
  assert.equal(await page.locator('[data-home-quick-nav]').count(), 1);
  const homeItems = page.locator('[data-home-feed-item]');
  assert.ok(await homeItems.count() > 0);
  assert.ok(await homeItems.count() <= 30);
  assert.equal(await page.locator('[data-home-feed-grid]').getAttribute('data-home-feed-limit'), '30');

  const menu = page.locator('[data-mobile-discovery-menu]');
  await menu.locator('summary').click();
  await menu.locator('[data-reference4-collections-open]').click();
  const collectionPlane = menu.locator('[data-reference4-collections]');
  for (const label of ['Детям', 'Необычное', 'Бесплатно', 'Клубы']) {
    assert.equal(await collectionPlane.getByRole('link', { name: new RegExp(label, 'u') }).count(), 1, label);
  }
  assert.ok(await menu.locator('a').filter({ hasText: /^Бесплатно$/u }).count() >= 2);

  await page.goto(route('/podborki/besplatnye-sobytiya/'), { waitUntil: 'domcontentloaded' });
  assert.equal(await page.locator('[data-free-collection-medallion="large"]').count(), 1);
  assert.equal(await page.locator('[data-free-collection-medallion="compact"]').count(), 1);
  await page.locator('[data-free-collection-shelf]').scrollIntoViewIfNeeded();
  assert.equal(await page.locator('[data-free-collection-medallion="compact"]').isVisible(), true);

  await page.goto(route('/date-2026-07-30/'), { waitUntil: 'domcontentloaded' });
  const pianissimo = page.locator('[data-event-id="5297"] [data-rail-media-reason="single_safe_visual_landscape_5x4"]').first();
  assert.equal(await pianissimo.count(), 1);
  const crop = await pianissimo.evaluate((node) => {
    const image = node.querySelector('img');
    const box = node.getBoundingClientRect();
    return { ratio: box.width / box.height, fit: image ? getComputedStyle(image).objectFit : '' };
  });
  assert.ok(Math.abs(crop.ratio - 1.25) < 0.02, JSON.stringify(crop));
  assert.equal(crop.fit, 'cover');

  await page.goto(route('/segodnya/'), { waitUntil: 'domcontentloaded' });
  const accessory = page.locator('[data-mobile-date-accessory]');
  assert.match(await accessory.getAttribute('data-calendar-horizon'), /^\d{4}-\d{2}-\d{2}$/u);
  assert.match(await accessory.getAttribute('data-calendar-furthest-event-date'), /^\d{4}-\d{2}-\d{2}$/u);
  await page.locator('[data-calendar-open]').click();
  const disabledDate = page.locator('[data-calendar-date][aria-disabled="true"]').first();
  assert.ok(await disabledDate.count() > 0);
  const beforeDisabledClick = page.url();
  await disabledDate.click({ force: true });
  assert.equal(page.url(), beforeDisabledClick);
  const nextMonth = page.locator('[data-calendar-month-next]');
  while (await nextMonth.isEnabled()) await nextMonth.click();
  assert.equal(await nextMonth.isDisabled(), true);

  await page.goto(route('/izbrannoe/'), { waitUntil: 'domcontentloaded' });
  assert.equal(await page.locator('meta[name="robots"]').getAttribute('content').then((value) => /noindex/u.test(value || '')), true);
  assert.equal(await page.locator('[data-favorites-page] [data-favorites-surface]').count(), 1);
  const favoritesState = page.locator('[data-favorites-skeleton]:visible,[data-favorites-auth-required]:visible,[data-favorites-empty]:visible,[data-favorites-grid]:visible');
  assert.ok(await favoritesState.count() >= 1);

  await page.goto(route('/neobychnoe/'), { waitUntil: 'domcontentloaded' });
  assert.equal(await page.locator('meta[name="robots"]').getAttribute('content').then((value) => /noindex/u.test(value || '')), true);
  const cards = page.locator('[data-unusual-card]');
  if (expectedApproved) {
    assert.ok(await cards.count() > 0, 'approved canary must contain unusual concepts');
    const concepts = await cards.evaluateAll((nodes) => nodes.map((node) => node.getAttribute('data-unusual-concept-id')));
    assert.equal(new Set(concepts).size, concepts.length);
  } else if (await page.locator('[data-unusual-feed-empty]').count()) {
    assert.equal(await cards.count(), 0, 'fail-closed empty state must not mix ordinary cards');
  }
  const markSeen = page.locator('[data-unusual-mark-seen]');
  if (await markSeen.isVisible()) {
    await markSeen.click();
    assert.equal(await page.locator('[data-unusual-nav-dot]:visible').count(), 0);
    await page.reload({ waitUntil: 'domcontentloaded' });
    assert.equal(await page.locator('[data-unusual-nav-dot]:visible').count(), 0);
  }

  assert.deepEqual(providerRequests, [], `ordinary views called providers: ${providerRequests.join(', ')}`);
  await context.close();
} finally {
  await browser.close();
}
