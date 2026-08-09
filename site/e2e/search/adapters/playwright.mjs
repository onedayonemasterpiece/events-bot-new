import { chromium, firefox, webkit } from 'playwright';

import { installSearchRuntimeProbe, snapshotSearchRuntimeProbe } from './runtime-probe.mjs';
import { assertCanonicalCandidateEventDestination } from '../../mobile-web/appium-network-receipt.mjs';
import {
  classifySupabaseClientUrl,
  SupabaseClientObservedByteMeter,
  SUPABASE_CLIENT_BYTE_CLASSES,
} from '../production-health-meter.mjs';

const cardsSelector = '[data-search-results] [data-event-card][data-event-id], [data-search-results] [data-search-vector-card][data-event-id]';

export async function runRealWheelScroll({
  readScrollY, lastCardVisible, wheel, wait, step, maxAttempts = 40,
}) {
  let inputCount = 0;
  const dispatchWheel = async (delta) => {
    await wheel(delta);
    inputCount += 1;
  };
  for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
    if (await readScrollY() === 0) break;
    await dispatchWheel(-step);
    await wait();
  }
  const before = await readScrollY();
  for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
    if (attempt > 0 && await lastCardVisible()) break;
    await dispatchWheel(step);
    await wait();
  }
  const after = await readScrollY();
  return { performed: inputCount > 0, delta_y: after - before,
    card_visible_after: await lastCardVisible(), gesture_count: inputCount,
    input_kind: 'wheel', input_count: inputCount };
}

export function snapshotResultsInPage() {
  const root = document.querySelector('[data-authorized-search]');
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
    terminal: root?.dataset?.searchTerminal === 'true' && !busy,
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
  let postBoundaryResponseObserver = null;
  let postBoundaryObserverPage = null;
  let postBoundaryMeter = null;
  let postBoundaryMeterSnapshot = null;
  let postBoundaryMeterError = null;
  const postBoundaryResponseTasks = new Set();
  const postBoundaryActiveRequests = new Set();
  let postBoundaryRequestFinishedObserver = null;
  let postBoundaryRequestFailedObserver = null;
  let postBoundaryLastActivityAt = 0;
  let preNavigationSearchActivity = null;
  let preNavigationResultState = null;
  let wholeCellOrigins = [...new Set((options.supabaseOrigins || []).map((value) => new URL(value).origin))];
  const wholeCellRequests = [];
  const wholeCellActiveRequests = new Map();
  const wholeCellResponses = [];
  let wholeCellLastActivityAt = Date.now();
  const diagnostics = { console_errors: 0, failed_requests: 0, error_responses: 0, storage_requests: 0,
    failed_document_requests: 0, failed_auth_requests: 0, failed_edge_requests: 0,
    failed_rest_requests: 0, failed_rpc_requests: 0 };
  const criticalFailedRequestClass = (request) => {
    try {
      if (request.resourceType?.() === 'document') return 'document';
      const category = classifySupabaseClientUrl(request.url(), { supabaseOrigins: wholeCellOrigins });
      return ({
        [SUPABASE_CLIENT_BYTE_CLASSES.AUTH]: 'auth',
        [SUPABASE_CLIENT_BYTE_CLASSES.EDGE]: 'edge',
        [SUPABASE_CLIENT_BYTE_CLASSES.DIRECT_REST]: 'rest',
        [SUPABASE_CLIENT_BYTE_CLASSES.DIRECT_RPC]: 'rpc',
      })[category] || null;
    } catch {
      // Unknown third-party/subresource metadata is not Search product-health
      // evidence. The authoritative document and Supabase paths fail closed.
      return null;
    }
  };
  const bindPage = (value) => {
    value.on('console', (message) => { if (message.type() === 'error') diagnostics.console_errors += 1; });
    value.on('request', (request) => {
      try {
        const url = new URL(request.url());
        const record = { origin: url.origin, pathname: url.pathname,
          method: String(request.method() || 'GET').toUpperCase() };
        wholeCellRequests.push(record);
        wholeCellActiveRequests.set(request, record);
        wholeCellLastActivityAt = Date.now();
      } catch { /* malformed browser metadata is excluded */ }
    });
    const finish = (request) => {
      if (wholeCellActiveRequests.delete(request)) wholeCellLastActivityAt = Date.now();
    };
    value.on('requestfinished', finish);
    value.on('requestfailed', (request) => {
      const category = criticalFailedRequestClass(request);
      if (category) {
        diagnostics.failed_requests += 1;
        diagnostics[`failed_${category}_requests`] += 1;
      }
      finish(request);
    });
    value.on('response', (response) => {
      let path = '';
      try { path = new URL(response.url()).pathname; } catch { /* ignored */ }
      if (response.status() >= 400 && /^\/(?:auth|functions|rest)\/v1(?:\/|$)/u.test(path)) diagnostics.error_responses += 1;
      if (/^\/storage\/v1(?:\/|$)/u.test(path)) diagnostics.storage_requests += 1;
      try {
        const url = new URL(response.url());
        wholeCellResponses.push({ origin: url.origin, pathname: url.pathname, response, measured: null });
        wholeCellLastActivityAt = Date.now();
      } catch { /* malformed browser metadata is excluded */ }
    });
    return value;
  };
  let configuredPolicy = options.productionHealth === true ? { production_health: true } : {};
  const stopPostBoundaryObservation = () => {
    if (postBoundaryRequestObserver && postBoundaryObserverPage) {
      postBoundaryObserverPage.off('request', postBoundaryRequestObserver);
    }
    if (postBoundaryResponseObserver && postBoundaryObserverPage) {
      postBoundaryObserverPage.off('response', postBoundaryResponseObserver);
    }
    if (postBoundaryRequestFinishedObserver && postBoundaryObserverPage) {
      postBoundaryObserverPage.off('requestfinished', postBoundaryRequestFinishedObserver);
    }
    if (postBoundaryRequestFailedObserver && postBoundaryObserverPage) {
      postBoundaryObserverPage.off('requestfailed', postBoundaryRequestFailedObserver);
    }
    postBoundaryRequestObserver = null;
    postBoundaryResponseObserver = null;
    postBoundaryRequestFinishedObserver = null;
    postBoundaryRequestFailedObserver = null;
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
      const inspected = await root.evaluate((node) => {
        const input = node.querySelector('[data-search-input]');
        return {
          enabled: node.dataset.searchEnabled === 'true',
          authorized: node.classList.contains('is-authorized'),
          transport: String(node.dataset.searchTransport || ''),
          input_tag: String(input?.tagName || '').toLowerCase(),
          enter_key_hint: String(input?.enterKeyHint || input?.getAttribute('enterkeyhint') || ''),
          observer_origins: [node.dataset.supabaseUrl, node.dataset.supabaseRelayUrl]
            .filter(Boolean).map((value) => new URL(value, location.href).origin),
        };
      });
      wholeCellOrigins = [...new Set([...wholeCellOrigins, ...(inspected.observer_origins || [])])];
      delete inspected.observer_origins;
      return inspected;
    },
    async configureRequestPolicy(policy) {
      configuredPolicy = { ...policy };
      await page.evaluate(installSearchRuntimeProbe, configuredPolicy);
    },
    async activity() { return page.evaluate(snapshotSearchRuntimeProbe); },
    async awaitPhysicalIdle() {
      const quietMs = Math.max(25, Number(options.physicalQuietMs || 200));
      const deadline = Date.now() + timeoutMs;
      while (true) {
        const relevantActive = [...wholeCellActiveRequests.values()]
          .some((item) => wholeCellOrigins.includes(item.origin));
        if (!relevantActive && Date.now() - wholeCellLastActivityAt >= quietMs) return;
        if (Date.now() >= deadline) throw new Error('search_physical_observation_missing');
        await page.waitForTimeout(Math.min(25, quietMs));
      }
    },
    async physicalActivity() {
      if (wholeCellOrigins.length < 1) throw new Error('search_physical_observation_missing');
      const meter = new SupabaseClientObservedByteMeter({ supabaseOrigins: wholeCellOrigins });
      for (const item of wholeCellResponses) {
        if (!wholeCellOrigins.includes(item.origin)) continue;
        if (!item.measured) {
          const headers = await item.response.allHeaders?.() || item.response.headers?.() || {};
          const hasLength = Object.keys(headers).some((key) => key.toLowerCase() === 'content-length');
          const body = hasLength ? null : await item.response.body();
          item.measured = { headers, body };
        }
        meter.recordResponse({ url: `${item.origin}${item.pathname}`, ...item.measured });
      }
      const requests = wholeCellRequests.filter((item) => wholeCellOrigins.includes(item.origin));
      return Object.freeze({
        search_posts: requests.filter((item) => item.method === 'POST'
          && item.pathname === '/functions/v1/event-search').length,
        storage_requests: requests.filter((item) => item.pathname === '/storage/v1'
          || item.pathname.startsWith('/storage/v1/')).length,
        receipt_rpc_requests: requests.filter((item) => /^\/rest\/v1\/rpc\/get_event_search_receipt(?:_|$)/u.test(item.pathname)).length,
        meter: meter.snapshot(),
      });
    },
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
        const root = document.querySelector('[data-authorized-search]');
        const status = document.querySelector('[data-search-status]');
        const submit = document.querySelector('[data-search-submit]');
        const more = document.querySelector('[data-search-more]');
        const cards = document.querySelectorAll('[data-search-results] [data-event-card][data-event-id], [data-search-results] [data-search-vector-card][data-event-id]');
        return (probe?.responses?.length || 0) >= responseCount && (probe?.meter?.pending || 0) === 0
          && root?.dataset?.searchTerminal === 'true' && cards.length >= cardCount
          && submit?.getAttribute('aria-busy') !== 'true' && more?.getAttribute('aria-busy') !== 'true';
      }, { responseCount: minimumResponseCount, cardCount: minimumCardCount }, { timeout: timeoutMs });
      const state = await adapter.snapshotResults();
      if (state.error && !state.card_renderer_unavailable) throw new Error('search_ui_terminal_error');
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
      const beforeUrl = new URL(page.url());
      const rawHref = await link.getAttribute('href');
      const expectedUrl = rawHref ? new URL(rawHref, beforeUrl) : null;
      const destination = assertCanonicalCandidateEventDestination({
        searchUrl: beforeUrl.href, eventUrl: expectedUrl?.href,
      });
      const beforeOrigin = beforeUrl.origin;
      stopPostBoundaryObservation();
      postBoundarySearchPostCount = 0;
      postBoundaryMeter = null;
      postBoundaryMeterSnapshot = null;
      postBoundaryMeterError = null;
      postBoundaryActiveRequests.clear();
      postBoundaryLastActivityAt = Date.now();
      const originValues = await page.evaluate(() => {
        const root = document.querySelector('[data-authorized-search]');
        return [root?.dataset?.supabaseUrl, root?.dataset?.supabaseRelayUrl]
          .filter(Boolean).map((value) => new URL(value, location.href).origin);
      }).catch(() => []);
      const supabaseOrigins = Array.isArray(originValues)
        ? [...new Set(originValues.filter((value) => typeof value === 'string'))] : [];
      if (supabaseOrigins.length > 0) {
        postBoundaryMeter = new SupabaseClientObservedByteMeter({ supabaseOrigins });
      }
      const observeRequest = (request) => {
        try {
          const requestClass = classifySupabaseClientUrl(request.url(), { supabaseOrigins });
          if (requestClass !== SUPABASE_CLIENT_BYTE_CLASSES.EXCLUDED) {
            postBoundaryActiveRequests.add(request);
            postBoundaryLastActivityAt = Date.now();
          }
          if (request.method() === 'POST'
            && new URL(request.url()).pathname === '/functions/v1/event-search') {
            postBoundarySearchPostCount += 1;
          }
        } catch { /* malformed request metadata fails via the authoritative probe */ }
      };
      const observeResponse = (observedResponse) => {
        if (!postBoundaryMeter) return;
        postBoundaryLastActivityAt = Date.now();
        const task = (async () => {
          const url = observedResponse.url();
          if (classifySupabaseClientUrl(url, { supabaseOrigins })
            === SUPABASE_CLIENT_BYTE_CLASSES.EXCLUDED) return;
          const headers = await observedResponse.allHeaders?.() || observedResponse.headers?.() || {};
          const hasContentLength = Object.entries(headers)
            .some(([key, value]) => key.toLowerCase() === 'content-length' && String(value) !== '');
          const body = hasContentLength ? null : await observedResponse.body();
          postBoundaryMeter.recordResponse({ url, headers, body });
        })().catch((error) => { postBoundaryMeterError = error || new Error('search_post_navigation_meter_failed'); });
        postBoundaryResponseTasks.add(task);
        task.finally(() => postBoundaryResponseTasks.delete(task));
      };
      const finishRequest = (request) => {
        if (postBoundaryActiveRequests.delete(request)) postBoundaryLastActivityAt = Date.now();
      };
      page.on('request', observeRequest);
      page.on('response', observeResponse);
      page.on('requestfinished', finishRequest);
      page.on('requestfailed', finishRequest);
      postBoundaryRequestObserver = observeRequest;
      postBoundaryResponseObserver = observeResponse;
      postBoundaryRequestFinishedObserver = finishRequest;
      postBoundaryRequestFailedObserver = finishRequest;
      postBoundaryObserverPage = page;
      const searchPageActivity = await page.evaluate(snapshotSearchRuntimeProbe);
      preNavigationSearchActivity = searchPageActivity;
      preNavigationResultState = await page.evaluate(snapshotResultsInPage);
      const [navigation] = await Promise.all([
        page.waitForNavigation({ waitUntil: 'domcontentloaded', timeout: timeoutMs }),
        link.click(),
      ]);
      const afterOrigin = new URL(page.url()).origin;
      const httpStatus = Number(navigation?.status() || 0);
      let request = navigation?.request?.() || null;
      if (!request || request.redirectedFrom?.()) throw new Error('search_card_route_redirected');
      if (request.url() !== expectedUrl.href) throw new Error('search_card_route_changed');
      while (request) {
        if (new URL(request.url()).origin !== beforeOrigin) throw new Error('search_card_route_cross_origin');
        request = request.redirectedFrom?.() || null;
      }
      assertCanonicalCandidateEventDestination({
        searchUrl: beforeUrl.href, eventUrl: expectedUrl.href, finalUrl: page.url(),
      });
      if (httpStatus !== 200) throw new Error('search_card_http_200_missing');
      return {
        schema_version: 'browser-card-open-v1', same_origin: beforeOrigin === afterOrigin,
        http_status: httpStatus, destination_class: destination.destination_class,
        network_source: 'playwright_navigation_response',
        search_page_activity_before_navigation: searchPageActivity,
      };
    },
    async postNavigationSearchPostCount() {
      if (!Number.isSafeInteger(postBoundarySearchPostCount)) {
        throw new Error('search_post_navigation_observation_missing');
      }
      const quietMs = Math.max(25, Number(options.postNavigationQuietMs || 200));
      const deadline = Date.now() + timeoutMs;
      while (true) {
        await Promise.all([...postBoundaryResponseTasks]);
        if (postBoundaryActiveRequests.size === 0 && postBoundaryResponseTasks.size === 0
          && Date.now() - postBoundaryLastActivityAt >= quietMs) break;
        if (Date.now() >= deadline) throw new Error('search_post_navigation_meter_failed');
        await page.waitForTimeout(Math.min(25, quietMs));
      }
      if (postBoundaryMeterError) throw new Error('search_post_navigation_meter_failed', { cause: postBoundaryMeterError });
      if (!postBoundaryMeter) throw new Error('search_post_navigation_meter_origin_missing');
      postBoundaryMeterSnapshot = postBoundaryMeter.snapshot();
      const count = postBoundarySearchPostCount;
      stopPostBoundaryObservation();
      return count;
    },
    async postNavigationMeterSnapshot() {
      if (!postBoundaryMeterSnapshot) throw new Error('search_post_navigation_meter_missing');
      return structuredClone(postBoundaryMeterSnapshot);
    },
    async failedJourneyEvidence() {
      if (!preNavigationSearchActivity) return null;
      const count = Number(postBoundarySearchPostCount || 0);
      stopPostBoundaryObservation();
      return Object.freeze({
        activity: structuredClone(preNavigationSearchActivity),
        results: structuredClone(preNavigationResultState),
        post_navigation_search_post_count: count,
        ...(postBoundaryMeterSnapshot
          ? { post_navigation_meter: structuredClone(postBoundaryMeterSnapshot) } : {}),
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
      stopPostBoundaryObservation();
      if (ownsRuntime) await context?.close();
      if (ownsRuntime) await browser?.close();
    },
  };
  return adapter;
}
