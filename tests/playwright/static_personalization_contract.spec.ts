import { test, expect } from '@playwright/test';
import fs from 'node:fs';
import path from 'node:path';

const demoHtml = fs.readFileSync(path.resolve(process.cwd(), 'static_site/personalization/demo.html'), 'utf8');
const personalizationJs = fs.readFileSync(path.resolve(process.cwd(), 'static_site/personalization/personalization.js'), 'utf8');

async function openFixture(page: any, viewport: { width: number; height: number }, options: { backendAvailable?: boolean; seedProfile?: Record<string, unknown> } = {}) {
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
    let body = demoHtml;
    const injected: string[] = [];
    if (options.backendAvailable === false) injected.push('window.__backendAvailable = false;');
    if (options.seedProfile) injected.push(`window.__seedProfile = ${JSON.stringify(options.seedProfile)};`);
    if (injected.length) {
      body = body.replace('<script src="/personalization/personalization.js"></script>', `<script>${injected.join('\n')}</script><script src="/personalization/personalization.js"></script>`);
    }
    await route.fulfill({
      status: 200,
      contentType: 'text/html; charset=utf-8',
      body,
    });
  });
  await page.goto('https://kenigevents.test/sobytiya/kamerny-jazz/');
}

test.describe('event_detail_related MVP-0 personalization contract', () => {
  test('no consent renders static related fallback without profile or telemetry', async ({ page }) => {
    await openFixture(page, { width: 375, height: 812 });

    await expect(page.locator('#related')).toHaveAttribute('data-surface', 'event_detail_related');
    await expect(page.locator('#related')).toHaveAttribute('data-algorithm-id', 'static_related_v1');
    await expect(page.locator('#related')).toHaveAttribute('data-presentation-mode', 'vertical_related');
    await expect(page.locator('#status')).toHaveText('static related fallback');
    await expect(page.locator('.related-card').first()).toContainText('Детский музыкальный спектакль');
    await expect(page.locator('[data-event-id="101"]')).toHaveCount(0);
    await expect(page.locator('[data-event-id="209"]')).toHaveCount(0);
    await expect.poll(() => page.evaluate(() => window.localStorage.getItem('ke_personalization_profile'))).toBeNull();
    await expect.poll(() => page.evaluate(() => (window as any).__telemetry.length)).toBe(0);
  });

  test('mobile consent reranks locally, downranks negative interests and emits compact served-list telemetry', async ({ page }) => {
    await openFixture(page, { width: 375, height: 812 }, {
      seedProfile: {
        consent_ok: true,
        profile_version: 'anon-profile-v1',
        feature_schema_version: 'event-detail-related-v1',
        anon_id: 'anon-test',
        session_id: 'session-test',
        positive_tags: { jazz: 1.0, live_music: 0.6, evening: 0.2 },
        negative_interest_tags: { kids: 1.0 },
        hidden_event_ids: ['208'],
      },
    });
    await page.locator('#ok').click();

    await expect(page.locator('#related')).toHaveAttribute('data-algorithm-id', 'local_related_rerank_v1');
    await expect(page.locator('#related')).toHaveAttribute('data-layout-mode', 'module');
    await expect(page.locator('#related')).toHaveAttribute('data-presentation-mode', 'vertical_related');
    await expect(page.locator('#status')).toHaveText('personalized related');
    await expect(page.locator('.related-card').first()).toContainText('Камерный джаз на крыше');
    await expect(page.locator('[data-event-id="208"]')).toHaveCount(0);

    const topIds = await page.locator('.related-card').evaluateAll((nodes: Element[]) => nodes.slice(0, 3).map((node) => node.getAttribute('data-event-id')));
    expect(topIds).not.toContain('203');

    const telemetry = await page.evaluate(() => (window as any).__telemetry);
    const served = telemetry.find((e: any) => e.event_kind === 'served_list_summary');
    expect(served).toBeTruthy();
    expect(served.surface).toBe('event_detail_related');
    expect(served.layout_mode).toBe('module');
    expect(served.presentation_mode).toBe('vertical_related');
    expect(served.algorithm_id).toBe('local_related_rerank_v1');
    expect(served.current_event_id).toBe(101);
    expect(served.shown.some((item: any) => item.event_id === 101 || item.event_id === 209)).toBeFalsy();

    await page.locator('[data-event-id="203"]').getByRole('button', { name: 'Не интересно' }).click();
    await expect(page.locator('[data-event-id="203"]')).toHaveCount(0);
    const afterHide = await page.evaluate(() => ({ telemetry: (window as any).__telemetry, profile: JSON.parse(window.localStorage.getItem('ke_personalization_profile') || '{}') }));
    expect(afterHide.profile.hidden_event_ids).toContain('203');
    expect(afterHide.telemetry.some((e: any) => e.event_kind === 'hide_event' && e.event_id === 203)).toBeTruthy();
  });

  test('desktop uses related module/grid behavior, not a mobile infinite feed', async ({ page }) => {
    await openFixture(page, { width: 1440, height: 900 }, {
      seedProfile: {
        consent_ok: true,
        profile_version: 'anon-profile-v1',
        feature_schema_version: 'event-detail-related-v1',
        anon_id: 'anon-desktop',
        session_id: 'session-desktop',
        positive_tags: { theatre: 1.0, drama: 0.8 },
        negative_interest_tags: { kids: 0.9 },
        hidden_event_ids: [],
      },
    });
    await page.locator('#ok').click();

    await expect(page.locator('#related')).toHaveAttribute('data-layout-mode', 'module');
    await expect(page.locator('#related')).toHaveAttribute('data-presentation-mode', 'grid_related');
    await expect(page.locator('#related')).not.toHaveClass(/feed/);
    await expect(page.locator('.related-card')).toHaveCount(4);
    // Page context dominates: a theatre-preferring profile must not turn a jazz page into a theatre-only block.
    await expect(page.locator('.related-card').first()).toContainText('Камерный джаз на крыше');

    const telemetry = await page.evaluate(() => (window as any).__telemetry);
    expect(telemetry.some((e: any) => e.event_kind === 'served_list_summary' && e.viewport_class === 'desktop' && e.presentation_mode === 'grid_related')).toBeTruthy();
  });

  test('telemetry endpoint timeout keeps local fallback and CTA/buttons usable', async ({ page }) => {
    await openFixture(page, { width: 375, height: 812 }, { backendAvailable: false });
    await page.locator('#ok').click();

    await expect(page.locator('#status')).toContainText('personalized local fallback');
    await expect(page.locator('#related')).toHaveAttribute('data-algorithm-id', 'local_related_rerank_v1_fallback');
    await expect(page.getByRole('button', { name: 'Подробнее' }).first()).toBeEnabled();
    await page.getByRole('button', { name: 'Подробнее' }).first().click();
    const telemetry = await page.evaluate(() => (window as any).__telemetry);
    expect(telemetry.some((e: any) => e.event_kind === 'recommendation_fallback_used')).toBeTruthy();
    expect(telemetry.some((e: any) => e.event_kind === 'related_card_click')).toBeTruthy();
  });
});
