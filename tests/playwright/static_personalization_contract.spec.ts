import { test, expect } from '@playwright/test';
import fs from 'node:fs';
import path from 'node:path';

const demoHtml = fs.readFileSync(path.resolve(process.cwd(), 'static_site/personalization/demo.html'), 'utf8');
const personalizationJs = fs.readFileSync(path.resolve(process.cwd(), 'static_site/personalization/personalization.js'), 'utf8');

async function openFixture(page: any, viewport: { width: number; height: number }, options: { backendAvailable?: boolean; emptyRelated?: boolean } = {}) {
  await page.setViewportSize(viewport);
  await page.route('https://kenigevents.test/**', async (route: any) => {
    const url = route.request().url();
    if (url.endsWith('/personalization/personalization.js')) {
      await route.fulfill({
        status: 200,
        contentType: 'application/javascript; charset=utf-8',
        body: personalizationJs,
      });
      return;
    }
    let body = options.backendAvailable === false
      ? demoHtml.replace('<script src="/personalization/personalization.js"></script>', '<script>window.__backendAvailable = false;</script><script src="/personalization/personalization.js"></script>')
      : demoHtml;
    if (options.emptyRelated) {
      body = body.replace('window.__controller = window.KenigEventsPersonalization.createController({', 'window.__relatedStatic = []; window.__controller = window.KenigEventsPersonalization.createController({');
    }
    await route.fulfill({
      status: 200,
      contentType: 'text/html; charset=utf-8',
      body,
    });
  });
  await page.goto('https://kenigevents.test/sobytiya/kamernyy-dzhaz-u-morya/');
}

test.describe('event_detail_related MVP-0 personalization contract', () => {
  test('no consent renders static related fallback and sends no telemetry', async ({ page }) => {
    await openFixture(page, { width: 375, height: 812 });

    await expect(page.locator('#related')).toHaveAttribute('data-surface', 'event_detail_related');
    await expect(page.locator('#related')).toHaveAttribute('data-layout-mode', 'module');
    await expect(page.locator('#related')).toHaveAttribute('data-presentation', 'mobile_related');
    await expect(page.locator('#related')).toHaveAttribute('data-algorithm-id', 'static_related_v1');
    await expect(page.locator('#status')).toHaveText('static related fallback');
    await expect(page.locator('.related-card').first()).toContainText('Балтийский джазовый вечер');
    await expect(page.locator('[data-event-id="100"]')).toHaveCount(0);
    await expect(page.locator('[data-event-id="107"]')).toHaveCount(0);
    await expect(page.locator('[data-event-id="108"]')).toHaveCount(0);
    await expect.poll(() => page.evaluate(() => window.localStorage.getItem('ke_personalization_profile'))).toBeNull();
    await expect.poll(() => page.evaluate(() => (window as any).__telemetry.length)).toBe(0);
  });

  test('mobile local rerank after consent keeps current-event similarity dominant and logs compact served list', async ({ page }) => {
    await openFixture(page, { width: 375, height: 812 });
    await page.evaluate(() => {
      (window as any).__seedProfile = {
        consent_ok: true,
        anon_id: 'anon-test',
        session_id: 'session-test',
        profile_version: 'anon-profile-v1',
        taxonomy_version: 'event-taxonomy-v1',
        feature_schema_version: 'event-features-v1',
        positive_tags: { theatre: 1.0, jazz: 0.3, evening: 0.2 },
        negative_interest_tags: { kids: 1.0, workshop: 0.5 },
        city_affinity: { 'Калининград': 1 },
        hidden_event_ids: [],
        recent_event_ids: []
      };
    });
    await page.locator('#ok').click();

    await expect(page.locator('#related')).toHaveAttribute('data-presentation', 'mobile_related');
    await expect(page.locator('#related')).toHaveAttribute('data-layout-mode', 'module');
    await expect(page.locator('#related')).toHaveAttribute('data-algorithm-id', 'local_related_rerank_v1');
    await expect(page.locator('#status')).toHaveText('local personalized related');
    // Even with a theatre-heavy profile, a jazz/current-event-similar card remains first.
    await expect(page.locator('.related-card').first()).toContainText('Балтийский джазовый вечер');
    // Negative interests are downranked out of the initial top module.
    await expect(page.locator('[data-event-id="104"]')).toHaveCount(0);

    await page.locator('.related-card').first().getByRole('button', { name: 'Подробнее' }).click();
    const telemetry = await page.evaluate(() => (window as any).__telemetry);
    expect(telemetry.some((e: any) => e.event_kind === 'personalization_served_list_summary'
      && e.summary.surface === 'event_detail_related'
      && e.summary.layout_mode === 'module'
      && e.summary.current_event_id === 100
      && e.summary.algorithm_id === 'local_related_rerank_v1'
      && e.summary.shown[0].event_id === 101)).toBeTruthy();
    expect(telemetry.some((e: any) => e.event_kind === 'related_card_click'
      && e.viewport_class === 'mobile'
      && e.layout_mode === 'module'
      && e.surface === 'event_detail_related'
      && e.algorithm_id === 'local_related_rerank_v1')).toBeTruthy();
  });

  test('hide/not interested hard-filters the event from following renders', async ({ page }) => {
    await openFixture(page, { width: 375, height: 812 });
    await page.locator('#ok').click();
    await expect(page.locator('[data-event-id="101"]')).toHaveCount(1);
    await page.locator('[data-event-id="101"]').getByRole('button', { name: 'Не интересно' }).click();
    await expect(page.locator('[data-event-id="101"]')).toHaveCount(0);
    const profile = await page.evaluate(() => JSON.parse(window.localStorage.getItem('ke_personalization_profile') || '{}'));
    expect(profile.hidden_event_ids).toContain('101');
  });

  test('desktop uses related module/grid behavior with right rail, not mobile feed', async ({ page }) => {
    await openFixture(page, { width: 1440, height: 900 });
    await page.locator('#ok').click();

    await expect(page.locator('#related')).toHaveAttribute('data-presentation', 'desktop_related');
    await expect(page.locator('#related')).toHaveAttribute('data-layout-mode', 'module');
    await expect(page.locator('.desktop-rail')).toBeVisible();
    await expect(page.locator('#related')).not.toHaveClass(/feed/);
    const telemetry = await page.evaluate(() => (window as any).__telemetry);
    expect(telemetry.some((e: any) => e.event_kind === 'personalization_served_list_summary'
      && e.summary.viewport_class === 'desktop'
      && e.summary.presentation === 'desktop_related')).toBeTruthy();
  });

  test('backend/Supabase unavailable keeps local related fallback and CTA usable', async ({ page }) => {
    await openFixture(page, { width: 375, height: 812 }, { backendAvailable: false });
    await page.locator('#ok').click();

    await expect(page.locator('#status')).toHaveText('local rerank: backend unavailable');
    await expect(page.locator('#related')).toHaveAttribute('data-algorithm-id', 'local_related_rerank_v1');
    await expect(page.getByRole('button', { name: 'Билеты / регистрация' })).toBeEnabled();
    const telemetry = await page.evaluate(() => (window as any).__telemetry);
    expect(telemetry.some((e: any) => e.event_kind === 'recommendation_fallback_used'
      && e.surface === 'event_detail_related')).toBeTruthy();
  });

  test('missing related manifest data keeps an empty static module and CTA usable', async ({ page }) => {
    await openFixture(page, { width: 375, height: 812 }, { emptyRelated: true });

    await expect(page.locator('#related')).toHaveAttribute('data-algorithm-id', 'static_related_v1');
    await expect(page.locator('.related-card')).toHaveCount(0);
    await expect(page.locator('.related-empty')).toHaveText('Похожих событий пока нет.');
    await expect(page.getByRole('button', { name: 'Билеты / регистрация' })).toBeEnabled();
    await expect.poll(() => page.evaluate(() => (window as any).__telemetry.length)).toBe(0);
  });
});
