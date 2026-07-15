import { test, expect, chromium, type BrowserContext, type Page } from '@playwright/test';
import fs from 'node:fs';
import path from 'node:path';

function briefingLabUrl(variant = 'static') {
  const dist = path.resolve(process.cwd(), 'site/dist');
  const buildId = fs.readdirSync(dist).find((name) => name.startsWith('preview-'));
  if (!buildId) throw new Error('Run site build:preview before this spec');
  return `file://${path.join(dist, buildId, 'lab/briefing/index.html')}?variant=${variant}`;
}

async function openLab(page: Page, variant: 'control' | 'static' | 'reveal') {
  await page.goto(briefingLabUrl(variant));
  await expect(page.locator('html')).toHaveAttribute('data-briefing-variant', variant);
}

test.describe('isolated static briefing lab', () => {
  test('no-JS fallback is a complete static briefing with the first event visible on 320x568', async ({ browser }) => {
    const context = await browser.newContext({ javaScriptEnabled: false, viewport: { width: 320, height: 568 } });
    const page = await context.newPage();
    await page.goto(briefingLabUrl('reveal'));

    await expect(page.locator('[data-briefing]')).toContainText('Сегодня — события на любой план');
    await expect(page.locator('[data-first-event-title]')).toBeVisible();
    const briefingBox = await page.locator('[data-briefing-slot]').boundingBox();
    const titleBox = await page.locator('[data-first-event-title]').boundingBox();
    const decisionBox = await page.locator('[data-first-event-decision]').boundingBox();
    expect(briefingBox).not.toBeNull();
    expect(titleBox).not.toBeNull();
    expect(titleBox!.y).toBeLessThan(568);
    expect(decisionBox).not.toBeNull();
    expect(Math.min(decisionBox!.y + decisionBox!.height, 568) - decisionBox!.y).toBeGreaterThanOrEqual(70);
    await expect(page.locator('[data-briefing-categories]')).toBeVisible();
    await expect(page.locator('[data-briefing-event-id]')).toHaveCount(3);
    await context.close();
  });

  test('A/B/C are isolated and reveal completes on user priority input', async ({ page }) => {
    await page.setViewportSize({ width: 390, height: 844 });
    await openLab(page, 'control');
    await expect(page.locator('[data-briefing-slot]')).toBeHidden();
    await expect(page.locator('[data-first-event-title]')).toBeVisible();

    await page.getByRole('tab', { name: 'B · Статичный' }).click();
    await expect(page.locator('[data-briefing-slot]')).toBeVisible();
    await expect(page.locator('[data-briefing]')).toHaveAttribute('data-motion', 'complete');
    await expect.poll(() => page.evaluate(() => (window as Window & { __briefingTelemetry?: Array<{ event_kind?: string }> }).__briefingTelemetry?.filter((item) => item.event_kind === 'briefing_impression').length || 0)).toBe(1);
    const staticKinds = await page.evaluate(() => (window as Window & { __briefingTelemetry?: Array<{ event_kind?: string }> }).__briefingTelemetry?.map((item) => item.event_kind) || []);
    expect(staticKinds.indexOf('briefing_impression')).toBeLessThan(staticKinds.indexOf('briefing_complete'));

    await page.getByRole('tab', { name: 'C · Reveal ≤ 1200 мс' }).click();
    await expect(page.locator('[data-briefing]')).toHaveAttribute('data-motion', /ready|running/);
    await page.evaluate(() => document.dispatchEvent(new PointerEvent('pointerdown', { bubbles: true })));
    await expect(page.locator('[data-briefing]')).toHaveAttribute('data-motion', 'complete');
    const events = await page.evaluate(() => (window as Window & { __briefingTelemetry?: Array<Record<string, unknown>> }).__briefingTelemetry || []);
    expect(events.some((item) => item.event_kind === 'briefing_interrupt' && item.interrupt_reason === 'pointerdown')).toBeTruthy();
    expect(events.some((item) => item.event_kind === 'briefing_complete' && item.completion_reason === 'interrupt')).toBeTruthy();
    expect(events.some((item) => item.event_kind === 'event_detail_open')).toBeFalsy();
    expect(events.some((item) => item.event_kind === 'eligible_session' && item.assignment_source === 'qa_query')).toBeTruthy();
  });

  test('reduced motion never starts the reveal', async () => {
    const browser = await chromium.launch();
    const context: BrowserContext = await browser.newContext({ reducedMotion: 'reduce', viewport: { width: 375, height: 812 } });
    const page = await context.newPage();
    await openLab(page, 'reveal');
    await expect(page.locator('[data-briefing]')).toHaveAttribute('data-motion', 'complete');
    const reasons = await page.evaluate(() => ((window as Window & { __briefingTelemetry?: Array<{ completion_reason?: string }> }).__briefingTelemetry || []).map((item) => item.completion_reason));
    expect(reasons).toContain('reduced_motion');
    await browser.close();
  });
});
