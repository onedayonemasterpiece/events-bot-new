import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { existsSync } from 'node:fs';
import { chromium } from 'playwright';

const port = 43183;
const origin = `http://127.0.0.1:${port}`;
const server = spawn('python3', ['-m', 'http.server', String(port), '--bind', '127.0.0.1', '--directory', 'dist'], { stdio:'ignore' });
await new Promise((resolve) => setTimeout(resolve, 450));
const executablePath = [
  process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE,
  '/home/dev/.cache/ms-playwright/chromium-1228/chrome-linux64/chrome',
  '/opt/ms-playwright/chromium-1223/chrome-linux64/chrome',
].find((candidate) => candidate && existsSync(candidate));
const browser = await chromium.launch({ headless:true, executablePath });

const profile = {
  consent_ok:true,
  profile_version:'anon-profile-v1',
  feature_schema_version:'event-detail-related-v1',
  taxonomy_version:'event-taxonomy-v1',
  anon_id:'11111111-1111-4111-8111-111111111111',
  session_id:'22222222-2222-4222-8222-222222222222',
  positive_tags:{}, negative_interest_tags:{}, liked_event_ids:[],
  not_interested_event_ids:[], hidden_event_ids:[], seen_event_ids:[], seen_venue_ids:[],
  price_preferences:{ prefer_free:false }, share_counts:{}, updated_at:'2026-08-03T00:00:00.000Z',
};

async function preparedPage(context) {
  const page = await context.newPage();
  await page.addInitScript((seed) => localStorage.setItem('ke_personalization_profile', JSON.stringify(seed)), profile);
  await page.goto(origin, { waitUntil:'domcontentloaded' });
  await page.waitForSelector('[data-event-card][data-card-href]:not([hidden])');
  return page;
}

try {
  const desktop = await browser.newContext({ viewport:{ width:1280, height:900 } });

  const single = await preparedPage(desktop);
  const firstHref = await single.locator('[data-event-card][data-card-href]').first().getAttribute('data-card-href');
  let navigations = 0;
  single.on('framenavigated', (frame) => { if (frame === single.mainFrame()) navigations += 1; });
  await single.locator('[data-event-card][data-card-href] .event-card__meta-row').first().click();
  await single.waitForURL((url) => url.pathname === new URL(firstHref, origin).pathname, { timeout:3000 });
  assert.equal(navigations, 1, 'single non-interactive body click navigates once');
  await single.close();

  const double = await preparedPage(desktop);
  await double.evaluate(() => {
    window.__doubleLikeCount = 0;
    window.addEventListener('kenigevents:event-card-double-like', () => { window.__doubleLikeCount += 1; });
  });
  const card = double.locator('[data-event-card][data-card-href]').first();
  const eventId = await card.getAttribute('data-event-id');
  const stableCard = double.locator(`[data-event-card][data-event-id="${eventId}"]`).first();
  const initialUrl = double.url();
  await card.locator('.event-card__meta-row').click({ clickCount:2, delay:70 });
  await double.waitForTimeout(420);
  const doubleDiagnostics = await double.evaluate((id) => ({
    count:window.__doubleLikeCount,
    profile:JSON.parse(localStorage.getItem('ke_personalization_profile') || 'null'),
    log:JSON.parse(localStorage.getItem('ke_event_feedback_log_v2') || 'null'),
    cards:Array.from(document.querySelectorAll(`[data-event-card][data-event-id="${id}"]`)).map((node) => ({
      gesture:node.dataset.cardGestureResult,
      pressed:Array.from(node.querySelectorAll('[data-feedback-action="like"]')).map((button) => button.getAttribute('aria-pressed')),
    })),
  }), eventId);
  assert.equal(double.url(), initialUrl, 'double click must cancel navigation');
  assert.equal(await stableCard.locator('[data-feedback-action="like"]').getAttribute('aria-pressed'), 'true', JSON.stringify(doubleDiagnostics));
  assert.equal(await double.evaluate(() => window.__doubleLikeCount), 1, 'double click emits one like');
  assert.deepEqual(
    await double.evaluate(() => JSON.parse(localStorage.getItem('ke_personalization_profile')).liked_event_ids),
    [String(eventId)],
  );

  // Interactive descendants retain their own semantics.  They are visible to
  // the delegated document listeners, but never enter the card-body gesture
  // arbitration or synthesize another double-gesture like.
  await double.evaluate(() => {
    document.body.insertAdjacentHTML('beforeend', `
      <article data-event-card data-event-id="999992" data-event-title="Control specimen" data-card-href="#card-navigation-must-not-run" data-test-control-card
        style="position:fixed;z-index:10000;left:20px;bottom:20px;display:flex;gap:8px;padding:12px;background:white">
        <button type="button" data-feedback-action="like" data-event-id="999992" data-event-title="Control specimen" aria-pressed="false">Like</button>
        <button type="button" data-native-share data-share-event-id="999992" data-share-event-title="Control specimen" data-share-title="Control specimen" data-share-url="https://example.test/">Share</button>
        <a href="#calendar-control" data-calendar-action data-calendar-event-id="gesture-calendar" data-calendar-expiry-day="invalid"><span data-calendar-label>Calendar</span></a>
        <a href="#nested-link" data-test-nested-link>Link</a>
        <button type="button" data-test-nested-control>Control</button>
        <input data-test-nested-input aria-label="Input control">
        <span role="button" tabindex="0" data-test-role-control>Role control</span>
      </article>
    `);
    const node = document.querySelector('[data-test-control-card]');
    node.querySelector('[data-calendar-action]')?.addEventListener('click', (event) => event.preventDefault());
    node.querySelector('[data-test-nested-link]')?.addEventListener('click', (event) => event.preventDefault());
  });
  const controlCard = double.locator('[data-test-control-card]');
  await double.evaluate(() => {
    Object.defineProperty(navigator, 'share', { configurable:true, value:async () => undefined });
  });
  const gestureCountBeforeControls = await double.evaluate(() => window.__doubleLikeCount);
  const profileBeforeControls = await double.evaluate(() => JSON.parse(localStorage.getItem('ke_personalization_profile')).liked_event_ids);
  const controlUrl = double.url();
  const excludedControls = [
    '[data-feedback-action="like"]',
    '[data-native-share]',
    '[data-calendar-action]',
    '[data-test-nested-link]',
    '[data-test-nested-control]',
    '[data-test-nested-input]',
    '[data-test-role-control]',
  ];
  for (const selector of excludedControls) {
    const control = controlCard.locator(selector).first();
    await control.click({ clickCount:2, delay:45 });
    await double.waitForTimeout(310);
    assert.equal(double.url(), controlUrl, `${selector} does not trigger card navigation`);
    assert.equal(await double.evaluate(() => window.__doubleLikeCount), gestureCountBeforeControls, `${selector} does not synthesize an extra card like`);
  }
  assert.deepEqual(
    await double.evaluate(() => JSON.parse(localStorage.getItem('ke_personalization_profile')).liked_event_ids),
    profileBeforeControls,
    'two direct like-control clicks may toggle normally but do not leave an extra liked state',
  );

  const template = double.locator('template[data-event-card-template="split-actions"]');
  await template.evaluate((node) => {
    const clone = node.content.firstElementChild.cloneNode(true);
    clone.dataset.eventId = '999991';
    clone.dataset.eventTitle = 'Dynamic card';
    clone.dataset.cardHref = '/sobytiya/dynamic-card-999991/';
    clone.querySelectorAll('[data-feedback-action="like"]').forEach((button) => {
      button.dataset.eventId = '999991';
      button.dataset.eventTitle = 'Dynamic card';
      button.setAttribute('aria-pressed', 'false');
    });
    document.body.appendChild(clone);
  });
  const dynamic = double.locator('[data-event-card][data-event-id="999991"]');
  await dynamic.locator('.event-card__meta-row').click({ clickCount:2, delay:70 });
  await double.waitForTimeout(350);
  assert.equal(await dynamic.locator('[data-feedback-action="like"]').getAttribute('aria-pressed'), 'true');
  assert.equal(double.url(), initialUrl, 'dynamic cloned card uses the same delegated arbitration');
  await double.close();

  const drag = await preparedPage(desktop);
  const dragTarget = drag.locator('[data-event-card][data-card-href] .event-card__meta-row').first();
  const box = await dragTarget.boundingBox();
  assert.ok(box);
  const dragUrl = drag.url();
  await drag.mouse.move(box.x + 8, box.y + 8);
  await drag.mouse.down();
  await drag.mouse.move(box.x + 40, box.y + 34, { steps:4 });
  await drag.mouse.up();
  await drag.waitForTimeout(380);
  assert.equal(drag.url(), dragUrl, 'drag does not navigate');
  assert.equal(await drag.locator('[data-event-card][data-card-href]').first().locator('[data-feedback-action="like"]').getAttribute('aria-pressed'), 'false');
  await drag.close();

  const keyboard = await preparedPage(desktop);
  await keyboard.evaluate(() => {
    window.__keyboardDoubleLikes = 0;
    window.addEventListener('kenigevents:event-card-double-like', () => { window.__keyboardDoubleLikes += 1; });
  });
  const keyboardCard = keyboard.locator('[data-event-card][data-card-href]').first();
  await keyboardCard.evaluate((node) => { node.dataset.cardHref = '#keyboard-target'; });
  const keyboardBody = keyboardCard.locator('.event-card__meta-row');
  await keyboardBody.click();
  await keyboard.waitForTimeout(70);
  await keyboardCard.focus();
  const enterStarted = Date.now();
  await keyboardCard.press('Enter');
  await keyboard.waitForURL((url) => url.hash === '#keyboard-target', { timeout:220 });
  assert.ok(Date.now() - enterStarted < 220, 'keyboard Enter navigation is immediate, not delayed by the 280ms pointer window');
  await keyboard.waitForTimeout(340);
  assert.equal(await keyboard.evaluate(() => window.__keyboardDoubleLikes), 0, 'keyboard Enter clears a pending pointer tap without joining a double gesture');
  assert.equal(await keyboardCard.locator('[data-feedback-action="like"]').getAttribute('aria-pressed'), 'false');
  await keyboard.close();

  const touch = await browser.newContext({ viewport:{ width:390, height:844 }, isMobile:true, hasTouch:true });
  const touchPage = await preparedPage(touch);
  const touchCard = touchPage.locator('[data-event-card][data-card-href]').first();
  const touchEventId = await touchCard.getAttribute('data-event-id');
  const stableTouchCard = touchPage.locator(`[data-event-card][data-event-id="${touchEventId}"]`).first();
  const touchUrl = touchPage.url();
  await touchCard.locator('.event-card__meta-row').tap();
  await touchPage.waitForTimeout(80);
  await touchCard.locator('.event-card__meta-row').tap();
  await touchPage.waitForTimeout(420);
  assert.equal(touchPage.url(), touchUrl, 'double tap cancels navigation');
  assert.equal(await stableTouchCard.locator('[data-feedback-action="like"]').getAttribute('aria-pressed'), 'true');
  await touchPage.evaluate(() => {
    window.__touchControlDoubleLikes = 0;
    window.addEventListener('kenigevents:event-card-double-like', () => { window.__touchControlDoubleLikes += 1; });
    Object.defineProperty(navigator, 'share', { configurable:true, value:async () => undefined });
  });
  await touchPage.evaluate(() => {
    document.body.insertAdjacentHTML('beforeend', `
      <article data-event-card data-event-id="999993" data-event-title="Touch control specimen" data-card-href="#touch-card-navigation-must-not-run" data-test-touch-control-card
        style="position:fixed;z-index:10000;left:8px;right:8px;bottom:8px;display:flex;flex-wrap:wrap;gap:8px;padding:12px;background:white">
        <button type="button" data-feedback-action="like" data-event-id="999993" data-event-title="Touch control specimen" aria-pressed="false">Like</button>
        <button type="button" data-native-share data-share-event-id="999993" data-share-event-title="Touch control specimen" data-share-title="Touch control specimen" data-share-url="https://example.test/">Share</button>
        <a href="#touch-calendar" data-calendar-action data-calendar-event-id="touch-calendar" data-calendar-expiry-day="invalid"><span data-calendar-label>Calendar</span></a>
        <a href="#touch-link" data-test-touch-link>Link</a>
        <button type="button" data-test-touch-control>Control</button>
      </article>
    `);
    const node = document.querySelector('[data-test-touch-control-card]');
    node.querySelector('[data-calendar-action]')?.addEventListener('click', (event) => event.preventDefault());
    node.querySelector('[data-test-touch-link]')?.addEventListener('click', (event) => event.preventDefault());
  });
  const touchControlCard = touchPage.locator('[data-test-touch-control-card]');
  const touchControlUrl = touchPage.url();
  const touchProfileBeforeControls = await touchPage.evaluate(() => JSON.parse(localStorage.getItem('ke_personalization_profile')).liked_event_ids);
  for (const selector of ['[data-feedback-action="like"]', '[data-native-share]', '[data-calendar-action]', '[data-test-touch-link]', '[data-test-touch-control]']) {
    const control = touchControlCard.locator(selector).first();
    await control.tap();
    await touchPage.waitForTimeout(60);
    await control.tap();
    await touchPage.waitForTimeout(320);
    assert.equal(touchPage.url(), touchControlUrl, `${selector} double tap does not trigger card navigation`);
    assert.equal(await touchPage.evaluate(() => window.__touchControlDoubleLikes), 0, `${selector} double tap does not synthesize a card like`);
  }
  assert.deepEqual(
    await touchPage.evaluate(() => JSON.parse(localStorage.getItem('ke_personalization_profile')).liked_event_ids),
    touchProfileBeforeControls,
    'two direct touch like-control taps do not leave an extra liked state',
  );
  await touch.close();
  await desktop.close();
  process.stdout.write('event-card double-like browser acceptance: PASS\n');
} finally {
  await browser.close();
  server.kill('SIGTERM');
  await Promise.race([once(server, 'exit'), new Promise((resolve) => setTimeout(resolve, 1000))]);
}
