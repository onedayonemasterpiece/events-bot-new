import assert from 'node:assert/strict';
import test from 'node:test';
import { chromium } from 'playwright';

const baseUrl = (process.env.MOBILE_STATE_BASE_URL || 'http://127.0.0.1:4321').replace(/\/+$/u, '');
const eventRoute = '/sobytiya/festival-elektronnoy-muzyki-laguna-beach-baltiysk-5833/';
const futureFavoriteId = '7008';
const transparentPoster = Buffer.from(
  '<svg xmlns="http://www.w3.org/2000/svg" width="400" height="500"><rect width="400" height="500" fill="#e8ded0"/></svg>',
);

function compatibleProfile(likedEventIds = []) {
  return {
    consent_ok: true,
    profile_version: 'anon-profile-v1',
    feature_schema_version: 'event-detail-related-v1',
    taxonomy_version: 'event-taxonomy-v1',
    anon_id: '11111111-1111-4111-8111-111111111111',
    session_id: '22222222-2222-4222-8222-222222222222',
    positive_tags: { music: 1 },
    negative_interest_tags: {},
    liked_event_ids: likedEventIds,
    not_interested_event_ids: [],
    hidden_event_ids: [],
    share_counts: {},
  };
}

async function stabilizeRemoteImages(page) {
  await page.route('https://static.kenigevents.ru/**', (route) => route.fulfill({
    status: 200,
    contentType: 'image/svg+xml',
    body: transparentPoster,
  }));
}

test('#780 liking the first newly paginated mobile card preserves the viewport', async () => {
  const browser = await chromium.launch({ headless: true });
  try {
    const context = await browser.newContext({ viewport: { width: 390, height: 844 } });
    const page = await context.newPage();
    await stabilizeRemoteImages(page);
    // Astro dev exposes this static JSON route with a trailing slash while the
    // production build transparently redirects the canonical fetch target.
    await page.route('**/data/discovery/5833.json', async (route) => {
      const response = await route.fetch({ url: `${baseUrl}/data/discovery/5833.json/` });
      await route.fulfill({ response });
    });
    await page.addInitScript((profile) => {
      window.localStorage.setItem('ke_personalization_profile', JSON.stringify(profile));
    }, compatibleProfile());

    await page.goto(`${baseUrl}${eventRoute}`, { waitUntil: 'domcontentloaded' });
    const feed = page.locator('[data-discovery-feed]:visible');
    const feedCards = feed.locator(':scope > [data-event-card]');
    const loadMore = page.locator('[data-discovery-load-more]:visible');
    await loadMore.waitFor({ state: 'visible' });
    const initialCount = await feedCards.count();
    assert.ok(initialCount >= 6, `expected an initial related page, received ${initialCount} cards`);

    await loadMore.click();
    await page.waitForFunction(
      (before) => Array.from(document.querySelectorAll('[data-discovery-feed]'))
        .find((candidate) => candidate.getClientRects().length > 0)
        ?.querySelectorAll(':scope > [data-event-card]').length > before,
      initialCount,
    );
    const paginatedCard = feedCards.nth(initialCount);
    const likeButton = paginatedCard.locator('[data-feedback-action="like"]');
    // Measure application-induced movement, not Playwright's own pre-click
    // scrolling: the actual tap target must already be inside the viewport.
    await likeButton.scrollIntoViewIfNeeded();
    await page.waitForTimeout(50);

    const eventId = await paginatedCard.getAttribute('data-event-id');
    assert.ok(eventId, 'the first newly paginated card must have an event id');
    const before = await page.evaluate(() => window.scrollY);
    const topBefore = await paginatedCard.evaluate((node) => node.getBoundingClientRect().top);
    await likeButton.click();
    await page.waitForFunction(
      (id) => Array.from(document.querySelectorAll('[data-discovery-feed]'))
        .find((candidate) => candidate.getClientRects().length > 0)
        ?.querySelector(
          `:scope > [data-event-card][data-event-id="${id}"] [data-feedback-action="like"]`,
        )?.getAttribute('aria-pressed') === 'true',
      eventId,
    );
    await page.waitForTimeout(50);

    const currentCard = feed.locator(`:scope > [data-event-card][data-event-id="${eventId}"]`);
    const after = await page.evaluate(() => window.scrollY);
    const topAfter = await currentCard.evaluate((node) => node.getBoundingClientRect().top);
    assert.ok(Math.abs(after - before) <= 24, `scrollY moved ${after - before}px (${before} → ${after})`);
    assert.ok(Math.abs(topAfter - topBefore) <= 24, `clicked card moved ${topAfter - topBefore}px in the viewport`);
    console.log(`#780 scroll evidence: scrollY ${before}→${after} (Δ${after - before}px), card top Δ${Math.round(topAfter - topBefore)}px`);
  } finally {
    await browser.close();
  }
});

test('#787 local future favorites expose an honest, measured ready state promptly', async () => {
  const browser = await chromium.launch({ headless: true });
  try {
    const context = await browser.newContext({ viewport: { width: 390, height: 844 } });
    const page = await context.newPage();
    await stabilizeRemoteImages(page);
    await page.route('**/data/personal-feed.json', async (route) => {
      await new Promise((resolve) => setTimeout(resolve, 250));
      await route.continue();
    });
    await page.addInitScript((profile) => {
      window.localStorage.setItem('ke_personalization_profile', JSON.stringify(profile));
    }, compatibleProfile([Number(futureFavoriteId)]));

    await page.goto(`${baseUrl}/izbrannoe/`, { waitUntil: 'domcontentloaded' });
    const surface = page.locator('[data-favorites-surface]');
    const skeleton = page.locator('[data-favorites-skeleton]');
    assert.equal(await surface.getAttribute('data-favorites-state'), 'loading');
    assert.equal(await skeleton.isVisible(), true);
    assert.equal(await skeleton.getAttribute('aria-busy'), 'true');

    await page.waitForFunction(() => (
      document.querySelector('[data-favorites-surface]')?.getAttribute('data-favorites-state') === 'ready'
    ));
    const readyMs = Number(await surface.getAttribute('data-favorites-local-ready-ms'));
    assert.ok(readyMs >= 200, `skeleton ended before the delayed catalog resolved (${readyMs}ms)`);
    assert.ok(readyMs <= 1_000, `local favorite hydration was not prompt (${readyMs}ms)`);
    assert.equal(await skeleton.isHidden(), true);
    assert.equal(await skeleton.getAttribute('aria-busy'), 'false');
    assert.equal(
      await page.locator(`[data-favorites-grid] > [data-event-card][data-event-id="${futureFavoriteId}"]`).count(),
      1,
    );
    console.log(`#787 hydration evidence: local future favorite ready in ${readyMs}ms after a 250ms catalog delay`);
  } finally {
    await browser.close();
  }
});
