import assert from 'node:assert/strict';
import { chromium } from 'playwright';

const baseUrl = process.env.ARTIFACT_BASE_URL;
if (!baseUrl) throw new Error('ARTIFACT_BASE_URL is required');

const browser = await chromium.launch({ headless: true });
try {
  const context = await browser.newContext({
    viewport: { width: 390, height: 844 },
    reducedMotion: 'reduce',
  });
  const page = await context.newPage();
  const persistenceRequests = [];
  page.on('request', (request) => {
    const url = request.url();
    if (
      request.method() !== 'GET'
      || /(?:\/api\/|https:\/\/[^/]*supabase\.co\/|\/artifact-(?:collect|progress)(?:\/|\?|$)|\/telemetry(?:\/|\?|$))/iu.test(url)
    ) persistenceRequests.push(`${request.method()} ${url}`);
  });

  await page.goto(`${baseUrl}/vyhodnye/`, { waitUntil: 'domcontentloaded' });
  const artifact = page.locator('[data-amber-artifact]');
  assert.equal(await artifact.count(), 1);
  const row = artifact.locator('xpath=ancestor::*[@data-mobile-listing-row]');
  await row.locator('.rail-window').evaluate((node) => { node.scrollLeft = node.scrollWidth; });
  const order = await row.locator('.event-track').evaluate((node) =>
    [...node.children]
      .filter((child) => child.matches('.event-like-cta,[data-amber-artifact]'))
      .map((child) => child.matches('.event-like-cta') ? 'like' : 'artifact'));
  assert.deepEqual(order.slice(-2), ['like', 'artifact']);
  const box = await artifact.boundingBox();
  assert.ok(box);
  assert.ok(Math.abs(box.width - 94) < 1);
  assert.ok(Math.abs(box.height - 112) < 1);
  assert.equal(await artifact.locator('.amber-artifact__visual').evaluate((node) => getComputedStyle(node).animationName), 'none');

  await artifact.click();
  assert.equal(await artifact.getAttribute('aria-pressed'), 'true');
  const stored = await page.evaluate(() => JSON.parse(localStorage.getItem('ke_artifact_collection_v1') || 'null'));
  assert.equal(stored?.schemaVersion, 1);
  assert.equal(stored?.artifacts?.amber_cosmonaut?.status, 'found');

  await page.reload({ waitUntil: 'domcontentloaded' });
  const foundArtifact = page.locator('[data-amber-artifact]');
  assert.match(await foundArtifact.getAttribute('aria-label'), /Открыть историю/u);
  await foundArtifact.locator('xpath=ancestor::*[@data-mobile-listing-row]').locator('.rail-window')
    .evaluate((node) => { node.scrollLeft = node.scrollWidth; });
  await foundArtifact.click();
  await page.waitForURL(/\/artefakty\/#amber_cosmonaut$/u);
  const dialog = page.locator('[data-artifact-dialog="amber_cosmonaut"]');
  await dialog.waitFor({ state: 'visible' });
  assert.equal(await page.locator('[data-artifact-found-count]').textContent(), '1');
  assert.equal(await page.locator('[data-artifact-state="found"]').count(), 1);
  assert.equal(await page.locator('[data-artifact-state="empty"]').count(), 6);
  assert.equal(await page.locator('[data-artifact-slot]').count(), 7);
  assert.equal(persistenceRequests.length, 0, `unexpected persistence requests: ${persistenceRequests.join(', ')}`);

  await page.keyboard.press('Escape');
  assert.equal(await dialog.isVisible(), false);
  const amberOpen = page.locator('[data-artifact-open][data-artifact-id="amber_cosmonaut"]');
  await amberOpen.focus();
  await page.keyboard.press('Enter');
  assert.equal(await dialog.isVisible(), true);
  await page.getByRole('button', { name: 'Закрыть историю артефакта' }).click();
  assert.equal(await amberOpen.evaluate((node) => document.activeElement === node), true);
  await context.close();

  const legacyContext = await browser.newContext({ viewport: { width: 390, height: 844 } });
  const legacyPage = await legacyContext.newPage();
  await legacyPage.addInitScript(() => localStorage.setItem('ke_amber_artifact_prototype_v1:tail', 'found'));
  await legacyPage.goto(`${baseUrl}/artefakty/`, { waitUntil: 'domcontentloaded' });
  assert.equal(await legacyPage.locator('[data-artifact-found-count]').textContent(), '1');
  assert.ok(await legacyPage.evaluate(() => localStorage.getItem('ke_artifact_collection_v1')));
  await legacyContext.close();

  const completeContext = await browser.newContext({ viewport: { width: 1280, height: 960 } });
  const completePage = await completeContext.newPage();
  await completePage.addInitScript(() => {
    const artifactIds = ['amber_cosmonaut', 'baltic_light', 'luise_queen_bridge', 'marzipan_heart', 'sedov_bell', 'cosmonaut', 'old_brick'];
    localStorage.setItem('ke_artifact_collection_v1', JSON.stringify({
      schemaVersion: 1,
      collectionId: 'kaliningrad_artifacts_v1',
      artifacts: Object.fromEntries(artifactIds.map((artifactId) => [artifactId, {
        status: 'found', foundAt: '2026-07-28T00:00:00.000Z', eventId: null, placement: 'test.all',
      }])),
    }));
  });
  await completePage.goto(`${baseUrl}/artefakty/`, { waitUntil: 'domcontentloaded' });
  assert.equal(await completePage.locator('[data-artifact-found-count]').textContent(), '7');
  assert.equal(await completePage.locator('[data-artifact-state="found"]').count(), 7);
  await completePage.locator('[data-artifact-open][data-artifact-id="old_brick"]').focus();
  await completePage.keyboard.press('Enter');
  assert.equal(await completePage.locator('[data-artifact-dialog="old_brick"]').isVisible(), true);
  await completeContext.close();
} finally {
  await browser.close();
}
