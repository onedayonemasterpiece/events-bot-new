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

export function snapshotResultsInPage() {
  const results = document.querySelector('[data-search-results]');
  const status = document.querySelector('[data-search-status]');
  const submit = document.querySelector('[data-search-submit]');
  const cards = Array.from(document.querySelectorAll('[data-search-results] [data-event-card][data-event-id], [data-search-results] [data-search-vector-card][data-event-id]'));
  const candidateCards = Array.from(document.querySelectorAll('[data-search-results] [data-event-card], [data-search-results] [data-search-vector-card]'));
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
    skeleton_count: document.querySelectorAll('[data-search-skeletons]:not([hidden]) .authorized-search__skeleton-card, [data-search-results] [data-skeleton]').length,
    placeholder_count: candidateCards.filter((node) => !node.getAttribute('data-event-id')).length,
    card_renderer_unavailable: Boolean(results?.querySelector('[data-search-card-render-unavailable]')),
  };
}

export async function createPlaywrightSearchAdapter(options = {}) {
  const timeoutMs = Number(options.timeoutMs || 60_000);
  let browser = options.browser || null;
  let context = options.context || null;
  let page = options.page || null;
  let ownsRuntime = false;
  let postBoundarySearchPostCount = null;
  let postBoundaryRequestObserver = null;
  let postBoundaryObserverPage = null;
  const diagnostics = { console_errors: 0, failed_requests: 0, error_responses: 0, storage_requests: 0 };
  const bindPage = (value) => {
    value.on('console', (message) => { if (message.type() === 'error') diagnostics.console_errors += 1; });
    value.on('requestfailed', () => { diagnostics.failed_requests += 1; });
    value.on('response', (response) => {
      let path = '';
      try { path = new URL(response.url()).pathname; } catch { /* ignored */ }
      if (response.status() >= 400 && /^\/(?:auth|functions|rest)\/v1(?:\/|$)/u.test(path)) diagnostics.error_responses += 1;
      if (/^\/storage\/v1(?:\/|$)/u.test(path)) diagnostics.storage_requests += 1;
    });
    return value;
  };
  let configuredPolicy = options.productionHealth === true ? { production_health: true } : {};
  const stopPostBoundaryObservation = () => {
    if (postBoundaryRequestObserver && postBoundaryObserverPage) {
      postBoundaryObserverPage.off('request', postBoundaryRequestObserver);
    }
    postBoundaryRequestObserver = null;
    postBoundaryObserverPage = null;
  };
  const prepareContext = async (storageState) => {
    const value = await browser.newContext({ storageState: storageState || undefined });
    if (options.productionHealth === true) {
      await value.addInitScript(installSearchRuntimeProbe, configuredPolicy);
    }
    return value;
  };
  if (!page) {
    const engine = { chromium, firefox, webkit }[options.browserName || 'chromium'];
    if (!engine) throw new Error(`search_browser_unknown:${options.browserName}`);
    browser = await engine.launch({ headless: options.headless !== false });
    context = await prepareContext(options.storageStatePath || undefined);
    page = bindPage(await context.newPage());
    ownsRuntime = true;
  } else {
    bindPage(page);
  }

  const adapter = {
    async preflight() {
      const viewport = page.viewportSize();
      const noSideEffects = page.url() === 'about:blank'
        && diagnostics.console_errors === 0 && diagnostics.failed_requests === 0 && diagnostics.error_responses === 0;
      return {
        schema_version: 'search_browser_preflight_v1', platform: 'browser', side_effect_free: noSideEffects,
        browser_ready: true, transport_ready: true, viewport_ready: Number(viewport?.width) > 0 && Number(viewport?.height) > 0,
        auth_requests: 0, search_posts: 0, otp_requests: 0, supabase_requests: 0,
      };
    },
    async restoreSessionState(storageStatePath) {
      if (!ownsRuntime || !storageStatePath) throw new Error('search_browser_storage_state_missing');
      await context.close();
      context = await prepareContext(storageStatePath);
      page = bindPage(await context.newPage());
    },
    async bootstrapSession(actionLink, returnTarget) {
      await page.goto(actionLink, { waitUntil: 'domcontentloaded', timeout: timeoutMs });
      await page.waitForURL((url) => url.origin === new URL(returnTarget).origin, { timeout: timeoutMs });
    },
    async open(targetUrl) {
      const response = await page.goto(targetUrl, { waitUntil: 'domcontentloaded', timeout: timeoutMs });
      const expected = new URL(targetUrl);
      const observed = new URL(page.url());
      if (observed.origin !== expected.origin) throw new Error('search_target_origin_changed');
      if (observed.href !== expected.href) throw new Error('search_target_redirected');
      if (response?.request?.()?.redirectedFrom?.()) throw new Error('search_target_redirected');
      if (!response || response.status() < 200 || response.status() >= 300) throw new Error('search_target_http_invalid');
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
    async healthDiagnostics() { return { ...diagnostics }; },
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
        return (probe?.responses?.length || 0) >= responseCount && (probe?.meter?.pending || 0) === 0 && cards.length >= cardCount
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
    async openFirstResult() {
      const first = page.locator(cardsSelector).first();
      await first.waitFor({ state: 'visible', timeout: timeoutMs });
      const link = first.locator('a[href]').first();
      if (await link.count() !== 1) throw new Error('search_first_card_link_missing');
      const beforeOrigin = new URL(page.url()).origin;
      stopPostBoundaryObservation();
      postBoundarySearchPostCount = 0;
      const observeRequest = (request) => {
        try {
          if (request.method() === 'POST'
            && new URL(request.url()).pathname === '/functions/v1/event-search') {
            postBoundarySearchPostCount += 1;
          }
        } catch { /* malformed request metadata fails via the authoritative probe */ }
      };
      page.on('request', observeRequest);
      postBoundaryRequestObserver = observeRequest;
      postBoundaryObserverPage = page;
      const searchPageActivity = await page.evaluate(snapshotSearchRuntimeProbe);
      const [navigation] = await Promise.all([
        page.waitForNavigation({ waitUntil: 'domcontentloaded', timeout: timeoutMs }),
        link.click(),
      ]);
      const afterOrigin = new URL(page.url()).origin;
      const httpStatus = Number(navigation?.status() || 0);
      let request = navigation?.request?.() || null;
      while (request) {
        if (new URL(request.url()).origin !== beforeOrigin) throw new Error('search_card_route_cross_origin');
        request = request.redirectedFrom?.() || null;
      }
      return {
        schema_version: 'browser-card-open-v1', same_origin: beforeOrigin === afterOrigin,
        http_status: httpStatus, destination_class: 'event_detail', network_source: 'playwright_navigation_response',
        search_page_activity_before_navigation: searchPageActivity,
      };
    },
    async postNavigationSearchPostCount() {
      if (!Number.isSafeInteger(postBoundarySearchPostCount)) {
        throw new Error('search_post_navigation_observation_missing');
      }
      const count = postBoundarySearchPostCount;
      stopPostBoundaryObservation();
      return count;
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
      stopPostBoundaryObservation();
      if (ownsRuntime) await context?.close();
      if (ownsRuntime) await browser?.close();
    },
  };
  return adapter;
}
