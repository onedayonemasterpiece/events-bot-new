import assert from 'node:assert/strict';
import { chromium } from 'playwright';

const base = String(process.env.RELATED_CONTRAST_BASE_URL || '').replace(/\/+$/u, '');
if (!base) throw new Error('RELATED_CONTRAST_BASE_URL is required');
const eventPath = process.env.RELATED_CONTRAST_EVENT_PATH
  || '/sobytiya/dekorativnoe-mini-panno-tkanye-uzory-zelenogradsk-6529/';
const executablePath = process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH || undefined;
const browser = await chromium.launch({ headless:true, ...(executablePath ? { executablePath } : {}) });

try {
  const page = await browser.newPage({ viewport:{ width:1440, height:1000 } });
  await page.goto(`${base}${eventPath}`, { waitUntil:'domcontentloaded', timeout:120_000 });
  const share = page.locator(
    '.desktop-clean-related__grid[data-surface="event_detail_related"] '
      + '.event-card--split-actions .event-card__feedback--under .feedback-button--share',
  ).first();
  await share.waitFor({ state:'visible' });
  const result = await share.evaluate((element) => {
    const style = getComputedStyle(element);
    const [red, green, blue] = style.color.match(/\d+(?:\.\d+)?/gu)?.slice(0, 3).map(Number) || [];
    const luminance = (0.2126 * red + 0.7152 * green + 0.0722 * blue) / 255;
    return {
      color:style.color,
      luminance,
      iconColor:getComputedStyle(element.querySelector('.icon')).color,
    };
  });
  assert.ok(result.luminance >= .82, `share action is not light enough: ${result.color}`);
  assert.equal(result.iconColor, result.color);
  await share.scrollIntoViewIfNeeded();
  if (process.env.RELATED_CONTRAST_SCREENSHOT) {
    await page.screenshot({ path:process.env.RELATED_CONTRAST_SCREENSHOT });
  }
} finally {
  await browser.close();
}
