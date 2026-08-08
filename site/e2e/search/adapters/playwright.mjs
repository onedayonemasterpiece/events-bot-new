import { chromium, firefox, webkit } from 'playwright';

import { installSearchRuntimeProbe, snapshotSearchRuntimeProbe } from './runtime-probe.mjs';

const cardsSelector = '[data-search-results] [data-event-card][data-event-id], [data-search-results] [data-search-vector-card][data-event-id]';

export async function runRealWheelScroll({
  readScrollY, lastCardVisible, wheel, wait, step, maxAttempts = 40,
}) {
  for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
    if (await readScrollY() === 0) break;
    await wheel(-step);
    await wait();
  }
  const before = await readScrollY();
  for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
    if (await lastCardVisible()) break;
    await wheel(step);
    await wait();
  }
  const after = await readScrollY();
  return { performed: true, delta_y: after - before, card_visible_after: await lastCardVisible() };
}

function snapshotResultsInPage() {
  const results = document.querySelector('[data-search-results]');
  const status = document.querySelector('[data-search-status]');
  const submit = document.querySelector('[data-search-submit]');
  const cards = Array.from(document.querySelectorAll('[data-search-results] [data-event-card][data-event-id], [data-search-results] [data-search-vector-card][data-event-id]'));
  const visible = (node) => {
    const style = getComputedStyle(node); const rect = node.getBoundingClientRect();
    return style.visibility !== 'hidden' && style.display !== 'none' && rect.width > 0 && rect.height > 0
      && rect.bottom > 0 && rect.top < innerHeight;
  };
  const ids = cards.map((node) => String(node.getAttribute('data-event-id') || '')).filter(Boolean);
  const families = cards.map((node) => {
    const members = String(node.getAttribute('data-occurrence-member-ids') || '').split(',').filter(Boolean).sort();
    return members.length > 1 ? `family:${members.join(',')}` : `event:${node.getAttribute('data-event-id') || ''}`;
  });
  const error = status?.getAttribute('role') === 'alert';
  const busy = submit?.getAttribute('aria-busy') === 'true';
  return {
    terminal: !error && !busy && Boolean(results && !results.hidden && cards.length > 0),
    error, cards_visible: cards.some(visible), visible_card_count: cards.filter(visible).length,
    rendered_ids: ids, rendered_families: families,
    card_renderer_unavailable: Boolean(results?.querySelector('[data-search-card-render-unavailable]')),
  };
}

export async function createPlaywrightSearchAdapter(options = {}) {
  const timeoutMs = Number(options.timeoutMs || 60_000);
  let browser = options.browser || null;
  let context = options.context || null;
  let page = options.page || null;
  let ownsRuntime = false;
  if (!page) {
    const engine = { chromium, firefox, webkit }[options.browserName || 'chromium'];
    if (!engine) throw new Error(`search_browser_unknown:${options.browserName}`);
    browser = await engine.launch({ headless: options.headless !== false });
    context = await browser.newContext({ storageState: options.storageStatePath || undefined });
    page = await context.newPage();
    ownsRuntime = true;
  }
  let configuredPolicy = {};

  const adapter = {
    async bootstrapSession(actionLink, returnTarget) {
      await page.goto(actionLink, { waitUntil: 'domcontentloaded', timeout: timeoutMs });
      await page.waitForURL((url) => url.origin === new URL(returnTarget).origin, { timeout: timeoutMs });
    },
    async open(targetUrl) {
      await page.goto(targetUrl, { waitUntil: 'domcontentloaded', timeout: timeoutMs });
      if (new URL(page.url()).origin !== new URL(targetUrl).origin) throw new Error('search_target_origin_changed');
    },
    async inspectSurface() {
      const root = page.locator('[data-authorized-search]');
      await root.waitFor({ state: 'attached', timeout: timeoutMs });
      await page.waitForFunction(() => document.querySelector('[data-authorized-search]')?.classList.contains('is-authorized'), null, { timeout: timeoutMs });
      await page.evaluate(installSearchRuntimeProbe, configuredPolicy);
      return root.evaluate((node) => {
        const input = node.querySelector('[data-search-input]');
        return {
          enabled: node.dataset.searchEnabled === 'true',
          authorized: node.classList.contains('is-authorized'),
          transport: String(node.dataset.searchTransport || ''),
          input_tag: String(input?.tagName || '').toLowerCase(),
          enter_key_hint: String(input?.enterKeyHint || input?.getAttribute('enterkeyhint') || ''),
        };
      });
    },
    async configureRequestPolicy(policy) {
      configuredPolicy = { ...policy };
      await page.evaluate(installSearchRuntimeProbe, configuredPolicy);
    },
    async activity() { return page.evaluate(snapshotSearchRuntimeProbe); },
    async typeQuery(value) {
      const input = page.locator('[data-search-input]');
      await input.focus();
      await input.fill(value);
    },
    async clearQuery() { await page.locator('[data-search-input]').fill(''); },
    async readQueryState() { return page.locator('[data-search-input]').evaluate((node) => ({ length: node.value.length })); },
    async submitWithSearchIntent() { await page.locator('[data-search-input]').press('Enter'); },
    async waitForTerminal({ minimumResponseCount = 1, minimumCardCount = 1 } = {}) {
      await page.waitForFunction(({ responseCount, cardCount }) => {
        const probe = globalThis.__KENIGEVENTS_SEARCH_HARNESS_V1__;
        const status = document.querySelector('[data-search-status]');
        const submit = document.querySelector('[data-search-submit]');
        const more = document.querySelector('[data-search-more]');
        const cards = document.querySelectorAll('[data-search-results] [data-event-card][data-event-id], [data-search-results] [data-search-vector-card][data-event-id]');
        if (status?.getAttribute('role') === 'alert') return true;
        return (probe?.responses?.length || 0) >= responseCount && cards.length >= cardCount
          && submit?.getAttribute('aria-busy') !== 'true' && more?.getAttribute('aria-busy') !== 'true';
      }, { responseCount: minimumResponseCount, cardCount: minimumCardCount }, { timeout: timeoutMs });
      const state = await adapter.snapshotResults();
      if (state.error) throw new Error('search_ui_terminal_error');
      return state;
    },
    async snapshotResults() { return page.evaluate(snapshotResultsInPage); },
    async realScrollResults() {
      const last = page.locator(cardsSelector).last();
      await last.waitFor({ state: 'attached', timeout: timeoutMs });
      const viewport = page.viewportSize();
      const step = Math.max(500, Math.floor((viewport?.height || 720) * 0.8));
      // The query field deliberately keeps focus after Enter, so keyboard Home
      // moves the textarea caret rather than the document. Use only physical
      // wheel input here: first establish the top boundary, then keep scrolling
      // until the final rendered card actually intersects the viewport. Five
      // fixed steps were insufficient for eight real production cards.
      return runRealWheelScroll({
        readScrollY: () => page.evaluate(() => scrollY),
        lastCardVisible: () => last.evaluate((node) => { const rect = node.getBoundingClientRect(); return rect.top < innerHeight && rect.bottom > 0; }),
        wheel: (delta) => page.mouse.wheel(0, delta),
        wait: () => page.waitForTimeout(80),
        step,
      });
    },
    async showMoreState() {
      const more = page.locator('[data-search-more]');
      return more.evaluate((node) => ({ visible: !node.hidden && getComputedStyle(node).display !== 'none', enabled: !node.disabled }));
    },
    async activateShowMore() { await page.locator('[data-search-more]').click(); },
    async waitForValidation() {
      await page.waitForFunction(() => {
        const status = document.querySelector('[data-search-status]');
        const submit = document.querySelector('[data-search-submit]');
        return status?.getAttribute('role') === 'alert' && submit?.getAttribute('aria-busy') !== 'true';
      }, null, { timeout: Math.min(timeoutMs, 10_000) });
      return { visible: true, kind: 'error' };
    },
    async close() {
      if (ownsRuntime) await context?.close();
      if (ownsRuntime) await browser?.close();
    },
  };
  return adapter;
}
