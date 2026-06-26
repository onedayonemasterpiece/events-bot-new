import { test, expect } from '@playwright/test';
import fs from 'node:fs';
import path from 'node:path';

const demoHtml = fs.readFileSync(path.resolve(process.cwd(), 'static_site/personalization/demo.html'), 'utf8');
const personalizationJs = fs.readFileSync(path.resolve(process.cwd(), 'static_site/personalization/personalization.js'), 'utf8');

type FixtureOptions = {
  backendAvailable?: boolean;
  seedProfile?: Record<string, unknown>;
  preloadedProfile?: Record<string, unknown>;
  disableDemoSeed?: boolean;
  breakStorage?: boolean;
};

async function openFixture(page: any, viewport: { width: number; height: number }, options: FixtureOptions = {}) {
  await page.setViewportSize(viewport);
  if (options.preloadedProfile) {
    await page.addInitScript((profile: Record<string, unknown>) => {
      window.localStorage.setItem('ke_personalization_profile', JSON.stringify(profile));
    }, options.preloadedProfile);
  }
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
    if (options.breakStorage) injected.push("Object.defineProperty(window, 'localStorage', { configurable: true, get: function () { throw new Error('blocked storage'); } });");
    if (options.disableDemoSeed) injected.push('window.__disableDemoSeed = true;');
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

  test('mobile consent reranks locally, keeps event exclusions separate from negative interests, and dedupes served-list telemetry', async ({ page }) => {
    await openFixture(page, { width: 375, height: 812 }, {
      seedProfile: {
        consent_ok: true,
        profile_version: 'anon-profile-v1',
        feature_schema_version: 'event-detail-related-v1',
        taxonomy_version: 'event-taxonomy-v1',
        anon_id: '11111111-1111-4111-8111-111111111111',
        session_id: '22222222-2222-4222-8222-222222222222',
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
    expect(topIds.slice(0, 2)).toEqual(['201', '210']);
    expect(topIds).not.toContain('203');

    let telemetry = await page.evaluate(() => (window as any).__telemetry);
    const served = telemetry.find((e: any) => e.event_kind === 'served_list_summary');
    expect(served).toBeTruthy();
    expect(served.surface).toBe('event_detail_related');
    expect(served.layout_mode).toBe('module');
    expect(served.presentation_mode).toBe('vertical_related');
    expect(served.algorithm_id).toBe('local_related_rerank_v1');
    expect(served.current_event_id).toBe(101);
    expect(served.served_list_id).toBeTruthy();
    expect(served.served_list_hash).toBeTruthy();
    expect(served.shown.some((item: any) => item.event_id === 101 || item.event_id === 209)).toBeFalsy();
    const adultJazz = served.shown.find((item: any) => item.event_id === 210);
    expect(adultJazz).toBeTruthy();
    expect(adultJazz.reason_codes).not.toContain('profile:negative_interest_penalty');

    await page.setViewportSize({ width: 390, height: 812 });
    await expect.poll(() => page.evaluate(() => (window as any).__telemetry.filter((e: any) => e.event_kind === 'served_list_summary').length)).toBe(1);

    await page.locator('.related-card').first().getByRole('button', { name: 'Подробнее' }).click();
    telemetry = await page.evaluate(() => (window as any).__telemetry);
    const click = telemetry.find((e: any) => e.event_kind === 'related_card_click');
    expect(click).toBeTruthy();
    expect(click.served_list_id).toBe(served.served_list_id);
    expect(click.served_list_hash).toBe(served.served_list_hash);

    await page.locator('[data-event-id="203"]').getByRole('button', { name: 'Не интересно' }).click();
    await expect(page.locator('[data-event-id="203"]')).toHaveCount(0);
    const afterHide = await page.evaluate(() => ({ telemetry: (window as any).__telemetry, profile: JSON.parse(window.localStorage.getItem('ke_personalization_profile') || '{}') }));
    expect(afterHide.profile.hidden_event_ids).toContain('203');
    expect(afterHide.telemetry.some((e: any) => e.event_kind === 'hide_event' && e.event_id === 203)).toBeTruthy();
  });

  test('demo seed is outside core: no-seed consent creates an empty compatible profile', async ({ page }) => {
    await openFixture(page, { width: 375, height: 812 }, { disableDemoSeed: true });
    await page.locator('#ok').click();

    const profile = await page.evaluate(() => JSON.parse(window.localStorage.getItem('ke_personalization_profile') || '{}'));
    expect(profile.consent_ok).toBe(true);
    expect(profile.profile_version).toBe('anon-profile-v1');
    expect(profile.feature_schema_version).toBe('event-detail-related-v1');
    expect(profile.taxonomy_version).toBe('event-taxonomy-v1');
    expect(profile.anon_id).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i);
    expect(profile.session_id).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i);
    expect(profile.positive_tags).toEqual({});
    expect(profile.negative_interest_tags).toEqual({});
    await expect(page.locator('#related')).toHaveAttribute('data-algorithm-id', 'local_related_rerank_v1');
    await expect(page.locator('.related-card').first()).toContainText('Детский музыкальный спектакль');
  });

  test('legacy profile without strict feature schema is rejected instead of silently scoring negative_tags', async ({ page }) => {
    await openFixture(page, { width: 375, height: 812 }, {
      preloadedProfile: {
        consent_ok: true,
        profile_version: 'anon-profile-v1',
        anon_id: 'anon-legacy',
        session_id: 'session-legacy',
        positive_tags: { jazz: 1 },
        negative_tags: { kids: 1 },
        hidden_event_ids: ['208'],
      },
    });

    await expect(page.locator('#status')).toHaveText('static related fallback');
    await expect(page.locator('#related')).toHaveAttribute('data-algorithm-id', 'static_related_v1');
    await expect(page.locator('.related-card').first()).toContainText('Детский музыкальный спектакль');
    await expect.poll(() => page.evaluate(() => (window as any).__telemetry.length)).toBe(0);
  });

  test('profile with matching feature schema but missing taxonomy_version is rejected', async ({ page }) => {
    await openFixture(page, { width: 375, height: 812 }, {
      preloadedProfile: {
        consent_ok: true,
        profile_version: 'anon-profile-v1',
        feature_schema_version: 'event-detail-related-v1',
        anon_id: 'anon-no-taxonomy',
        session_id: 'session-no-taxonomy',
        positive_tags: { jazz: 1 },
        negative_interest_tags: { kids: 1 },
        hidden_event_ids: ['208'],
      },
    });

    await expect(page.locator('#status')).toHaveText('static related fallback');
    await expect(page.locator('#related')).toHaveAttribute('data-algorithm-id', 'static_related_v1');
    await expect(page.locator('.related-card').first()).toContainText('Детский музыкальный спектакль');
    await expect.poll(() => page.evaluate(() => (window as any).__telemetry.length)).toBe(0);
  });

  test('profile with prefixed non-UUID ids is rejected before DB-compatible telemetry', async ({ page }) => {
    await openFixture(page, { width: 375, height: 812 }, {
      preloadedProfile: {
        consent_ok: true,
        profile_version: 'anon-profile-v1',
        feature_schema_version: 'event-detail-related-v1',
        taxonomy_version: 'event-taxonomy-v1',
        anon_id: 'anon-prefixed-id',
        session_id: 'session-prefixed-id',
        positive_tags: { jazz: 1 },
        negative_interest_tags: { kids: 1 },
        hidden_event_ids: ['208'],
      },
    });

    await expect(page.locator('#status')).toHaveText('static related fallback');
    await expect(page.locator('#related')).toHaveAttribute('data-algorithm-id', 'static_related_v1');
    await expect(page.locator('.related-card').first()).toContainText('Детский музыкальный спектакль');
    await expect.poll(() => page.evaluate(() => (window as any).__telemetry.length)).toBe(0);
  });

  test('blocked localStorage does not break the page or enable trusted personalization', async ({ page }) => {
    await openFixture(page, { width: 375, height: 812 }, { breakStorage: true });
    await expect(page.locator('#status')).toHaveText('static related fallback');
    await page.locator('#ok').click();
    await expect(page.locator('#status')).toHaveText('static related fallback');
    await expect(page.locator('#related')).toHaveAttribute('data-algorithm-id', 'static_related_v1');
    await expect(page.locator('.related-card').first()).toContainText('Детский музыкальный спектакль');
    await expect.poll(() => page.evaluate(() => (window as any).__telemetry.length)).toBe(0);
  });

  test('desktop uses related module/grid behavior, not a mobile infinite feed', async ({ page }) => {
    await openFixture(page, { width: 1440, height: 900 }, {
      seedProfile: {
        consent_ok: true,
        profile_version: 'anon-profile-v1',
        feature_schema_version: 'event-detail-related-v1',
        taxonomy_version: 'event-taxonomy-v1',
        anon_id: '33333333-3333-4333-8333-333333333333',
        session_id: '44444444-4444-4444-8444-444444444444',
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
    expect(telemetry.some((e: any) => e.event_kind === 'recommendation_fallback_used' && e.trusted_remote === false)).toBeTruthy();
    expect(telemetry.some((e: any) => e.event_kind === 'related_card_click' && e.trusted_remote === false)).toBeTruthy();
  });
});
