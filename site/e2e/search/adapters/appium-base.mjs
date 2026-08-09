import { installSearchRuntimeProbe, snapshotSearchRuntimeProbe,
  verifyAuthenticatedOwnerRuntimeProbe } from './runtime-probe.mjs';
import { buildAppiumSessionFailureReceipt } from '../../mobile-web/appium-startup-receipt.mjs';
import { buildExactTargetNavigationReceipt, buildSameOriginNavigationReceipt,
  extractSanitizedNavigationResponses } from '../../mobile-web/appium-network-receipt.mjs';
import { buildMobilePreflightFailureReceipt,
  runAppiumTransportPreflight } from '../../mobile-web/appium-preflight.mjs';
import { dismissNativeKeyboard, focusIosSafariWebInput, observeNativeKeyboard,
  performNativeDocumentSwipe, prepareIosSafariWebContext,
  withNativeAppContext } from '../../mobile-web/appium-browser.mjs';

const IOS_SEARCH_INPUT_LABELS = Object.freeze([
  'Что хочется сделать?',
  'Например: послушать хор или сходить с детьми бесплатно',
  'Например: джаз на выходных',
]);
const IOS_SEARCH_KEYBOARD_DISMISS_LABELS = Object.freeze([
  'Найти событие',
  'Найти событие по описанию',
]);

function pageResultSnapshot() {
  const results = document.querySelector('[data-search-results]');
  const status = document.querySelector('[data-search-status]');
  const submit = document.querySelector('[data-search-submit]');
  const allCards = Array.from(document.querySelectorAll('[data-search-results] [data-event-card], [data-search-results] [data-search-vector-card]'));
  const cards = allCards.filter((node) => String(node.getAttribute('data-event-id') || ''));
  const skeletons = new Set(document.querySelectorAll(
    '[data-search-skeletons]:not([hidden]) .authorized-search__skeleton-card, [data-search-results] .authorized-search__skeleton-card, [data-search-results] [data-skeleton]',
  ));
  const isVisible = (node) => { const rect = node.getBoundingClientRect(); const style = getComputedStyle(node);
    return style.display !== 'none' && style.visibility !== 'hidden' && rect.width > 0 && rect.height > 0 && rect.top < innerHeight && rect.bottom > 0; };
  const renderedIds = cards.map((node) => String(node.getAttribute('data-event-id') || '')).filter(Boolean);
  const renderedFamilies = cards.map((node) => {
    const members = String(node.getAttribute('data-occurrence-member-ids') || '').split(',').filter(Boolean).sort();
    return members.length > 1 ? `family:${members.join(',')}` : `event:${node.getAttribute('data-event-id') || ''}`;
  });
  const error = status?.getAttribute('role') === 'alert';
  return { terminal: !error && submit?.getAttribute('aria-busy') !== 'true' && Boolean(results && !results.hidden && cards.length),
    error, cards_visible: cards.some(isVisible), visible_card_count: cards.filter(isVisible).length,
    rendered_ids: renderedIds, rendered_families: renderedFamilies,
    skeleton_count: skeletons.size,
    placeholder_count: allCards.filter((node) => !String(node.getAttribute('data-event-id') || '')).length,
    card_renderer_unavailable: Boolean(results?.querySelector('[data-search-card-render-unavailable]')) };
}

const closedLogLevelError = (value) => ['SEVERE', 'ERROR'].includes(String(value || '').toUpperCase());

function accumulateClosedDriverDiagnostics(logs, diagnostics, seen) {
  const visited = new WeakSet();
  const record = (kind, identity) => {
    const key = `${kind}:${String(identity || '')}`;
    if (!seen.has(key)) {
      seen.add(key);
      diagnostics[kind] += 1;
    }
  };
  const visit = (value, depth = 0) => {
    if (depth > 10 || value == null) return;
    if (typeof value === 'string') {
      const text = value.trim();
      if (!text.startsWith('{') && !text.startsWith('[')) return;
      try { visit(JSON.parse(text), depth + 1); } catch { /* non-protocol log */ }
      return;
    }
    if (typeof value !== 'object' || visited.has(value)) return;
    visited.add(value);
    if (Array.isArray(value)) { value.forEach((item) => visit(item, depth + 1)); return; }
    const method = String(value.method || '');
    const params = value.params && typeof value.params === 'object' ? value.params : {};
    const requestId = String(params.requestId || params.timestamp || value.timestamp || 'unidentified');
    if (method === 'Network.loadingFailed') record('failed_requests', requestId);
    if (method === 'Network.requestWillBeSent' || method === 'Network.responseReceived') {
      const rawUrl = method === 'Network.requestWillBeSent'
        ? params.request?.url : params.response?.url;
      let path = '';
      try { path = new URL(String(rawUrl)).pathname; } catch { /* malformed URL is ignored */ }
      if (/^\/storage\/v1(?:\/|$)/u.test(path)) record('storage_requests', requestId);
      const status = Number(params.response?.status || 0);
      if (method === 'Network.responseReceived' && status >= 400
        && /^\/(?:auth|functions|rest)\/v1(?:\/|$)/u.test(path)) {
        record('error_responses', requestId);
      }
    }
    if ((method === 'Runtime.consoleAPICalled' && String(params.type).toLowerCase() === 'error')
      || (method === 'Log.entryAdded' && closedLogLevelError(params.entry?.level))) {
      record('console_errors', requestId);
    }
    if (!method && closedLogLevelError(value.level)) {
      record('console_errors', `${value.timestamp || 'untimestamped'}:${String(value.level).toUpperCase()}`);
    }
    Object.values(value).forEach((child) => visit(child, depth + 1));
  };
  visit(logs);
}

export async function runRealTouchScroll({ readScrollY, lastCardVisible, gesture, wait, maxGestures = 40 }) {
  const before = Number(await readScrollY());
  let cardVisible = false;
  let gestures = 0;
  let lastGesture = null;
  while (gestures < maxGestures && !cardVisible) {
    const receipt = await gesture();
    if (receipt && typeof receipt === 'object') lastGesture = receipt;
    gestures += 1;
    await wait();
    cardVisible = await lastCardVisible();
  }
  const after = Number(await readScrollY());
  return { performed: gestures > 0, delta_y: after - before,
    card_visible_after: cardVisible === true, gesture_count: gestures,
    ...(lastGesture ? { last_gesture: lastGesture } : {}) };
}

export async function createAppiumSearchAdapter(options = {}) {
  const platform = options.platform;
  if (!['android', 'ios'].includes(platform)) throw new Error(`search_mobile_platform:${platform}`);
  const timeoutMs = Number(options.timeoutMs || 60_000);
  let driver = options.driver || null;
  if (!driver) {
    const { remote } = await import('webdriverio');
    const startedAt = Date.now();
    try {
      driver = await remote({
        hostname: options.hostname || '127.0.0.1', port: Number(options.port || 4723), path: options.path || '/',
        logLevel: options.logLevel || 'error',
        connectionRetryTimeout: Number(options.connectionRetryTimeout || 300_000),
        connectionRetryCount: 0,
        capabilities: options.capabilities,
      });
    } catch (error) {
      const receipt = await buildAppiumSessionFailureReceipt({
        error, platform, startedAt,
        logPath: options.appiumLogPath || process.env.E2E_APPIUM_LOG_PATH,
        appiumServerReady: process.env.E2E_APPIUM_STATUS_READY === '1',
        startupAttempt: process.env.E2E_APPIUM_STARTUP_ATTEMPT,
      });
      const failure = error instanceof Error ? error : new Error('webdriver_session_error');
      failure.searchReceipt = receipt;
      throw failure;
    }
  }
  const lifecycle = {
    failure_stage: 'webdriver_session_created',
    auth_callback_started: false, auth_callback_authorized: false,
    webdriver_client_session_created: true, native_safari_stable: platform !== 'ios',
    webview_attached: platform !== 'ios', transport_preflight_passed: false,
    search_surface_ready: false,
    startup_attempt: [1, 2].includes(Number(process.env.E2E_APPIUM_STARTUP_ATTEMPT))
      ? Number(process.env.E2E_APPIUM_STARTUP_ATTEMPT) : 1,
  };
  let configuredPolicy = {};
  let nativeKeyboardObserved = false;
  let closed = false;
  let callbackAuthObservedBytes = 0;
  const driverDiagnostics = {
    console_errors: 0, failed_requests: 0, error_responses: 0, storage_requests: 0,
  };
  const driverDiagnosticIds = new Set();

  const syncClosedDriverDiagnostics = async () => {
    const networkType = platform === 'android' ? 'performance' : 'safariNetwork';
    const consoleType = platform === 'android' ? 'browser' : 'safariConsole';
    const networkLogs = await driver.getLogs?.(networkType).catch(() => null);
    const consoleLogs = await driver.getLogs?.(consoleType).catch(() => null);
    if (!Array.isArray(networkLogs) || !Array.isArray(consoleLogs)) {
      throw new Error('mobile_health_diagnostics_unavailable');
    }
    accumulateClosedDriverDiagnostics(networkLogs, driverDiagnostics, driverDiagnosticIds);
    accumulateClosedDriverDiagnostics(consoleLogs, driverDiagnostics, driverDiagnosticIds);
  };

  const meterWithCallbackAuthBytes = (meter = {}) => {
    const categories = { auth: 0, edge: 0, direct_rest: 0, direct_rpc: 0, ...(meter.categories || {}) };
    categories.auth = Number(categories.auth || 0) + callbackAuthObservedBytes;
    const total = Object.values(categories).reduce((sum, value) => sum + Number(value || 0), 0);
    const target = Number(meter.target_bytes || 48 * 1024);
    const hard = Number(meter.hard_limit_bytes || 96 * 1024);
    return {
      ...meter, categories, total_bytes: total, target_bytes: target, hard_limit_bytes: hard,
      budget_status: total <= target ? 'within_target' : total <= hard ? 'above_target' : 'hard_limit_exceeded',
      target_met: total <= target, cost_guard_passed: total <= hard, hard_limit_exceeded: total > hard,
    };
  };

  const deleteDriverSession = async () => {
    if (closed) return true;
    await driver.deleteSession();
    closed = true;
    lifecycle.webdriver_client_session_deleted = true;
    return true;
  };

  const purgeLocalAuthState = async () => {
    const originalContext = await driver.getContext().catch(() => null);
    const contexts = await driver.getContexts().catch(() => []);
    const webContext = contexts.find((value) => {
      const normalized = String(value).toUpperCase();
      return normalized.includes('WEBVIEW') || normalized.includes('CHROMIUM');
    });
    if (!webContext) return lifecycle.auth_callback_started !== true;
    if (String(originalContext).toUpperCase() !== String(webContext).toUpperCase()) {
      await driver.switchContext(webContext);
    }
    try {
      const purged = await driver.execute(() => {
        try {
          localStorage.clear();
          sessionStorage.clear();
          return localStorage.length === 0 && sessionStorage.length === 0;
        } catch { return false; }
      });
      await driver.deleteCookies?.().catch(() => undefined);
      return purged === true;
    } finally {
      if (originalContext && String(originalContext).toUpperCase() !== String(webContext).toUpperCase()) {
        await driver.switchContext(originalContext).catch(() => undefined);
      }
    }
  };

  const adapter = {
    async diagnostics() { return structuredClone(lifecycle); },
    async preflight() {
      if (lifecycle.transport_preflight_passed && lifecycle.transport_preflight_receipt) {
        return structuredClone(lifecycle.transport_preflight_receipt);
      }
      lifecycle.failure_stage = platform === 'ios'
        ? 'native_safari_webview_prepare' : 'mobile_transport_preflight';
      try {
        const receipt = await runAppiumTransportPreflight(driver, {
          platform,
          expectedCapabilities: options.capabilities,
          startupAttempt: lifecycle.startup_attempt,
          env: options.env || process.env,
          iosPrepare: platform === 'ios'
            ? (options.iosPrepare || prepareIosSafariWebContext) : null,
        });
        lifecycle.native_safari_stable = platform === 'ios' ? true : lifecycle.native_safari_stable;
        lifecycle.webview_attached = true;
        lifecycle.transport_preflight_passed = true;
        lifecycle.transport_preflight_receipt = receipt;
        lifecycle.failure_stage = 'auth_callback_not_started';
        return structuredClone(receipt);
      } catch (error) {
        let deleted = false;
        try { deleted = await deleteDriverSession(); } catch { deleted = false; }
        const receipt = buildMobilePreflightFailureReceipt({
          platform, error, attempt: lifecycle.startup_attempt,
          driverSessionCreated: true, driverSessionDeleted: deleted,
        });
        const failure = error instanceof Error ? error : new Error('mobile_preflight_failed');
        failure.searchReceipt = receipt;
        throw failure;
      }
    },
    async bootstrapSession(actionLink, returnTarget) {
      lifecycle.failure_stage = 'auth_callback';
      lifecycle.auth_callback_started = true;
      const networkType = platform === 'android' ? 'performance' : 'safariNetwork';
      const initialLogs = await driver.getLogs(networkType).catch(() => null);
      if (!Array.isArray(initialLogs)) throw new Error('mobile_auth_network_log_unavailable');
      await driver.url(actionLink);
      await driver.waitUntil(async () => new URL(await driver.getUrl()).origin === new URL(returnTarget).origin,
        { timeout: timeoutMs, interval: 250, timeoutMsg: 'search_auth_callback_timeout' });
      await driver.waitUntil(async () => driver.execute(() => document.querySelector('[data-authorized-search]')
        ?.classList.contains('is-authorized') === true),
      { timeout: timeoutMs, interval: 250, timeoutMsg: 'search_auth_callback_session_not_restored' });
      const callbackLogs = await driver.getLogs(networkType).catch(() => null);
      if (!Array.isArray(callbackLogs)) throw new Error('mobile_auth_network_log_unavailable');
      callbackAuthObservedBytes += extractSanitizedNavigationResponses(callbackLogs)
        .filter((item) => item.pathname === '/auth/v1' || item.pathname.startsWith('/auth/v1/'))
        .reduce((sum, item) => sum + Number(item.encoded_bytes || 0), 0);
      lifecycle.auth_callback_authorized = true;
      lifecycle.failure_stage = 'search_surface';
    },
    async open(targetUrl) {
      const networkType = platform === 'android' ? 'performance' : 'safariNetwork';
      const initialLogs = await driver.getLogs(networkType).catch(() => null);
      if (!Array.isArray(initialLogs)) throw new Error('mobile_target_network_log_unavailable');
      await driver.url(targetUrl);
      await driver.waitUntil(async () => (await driver.getUrl()) === targetUrl,
        { timeout: timeoutMs, interval: 250, timeoutMsg: 'search_target_redirected' });
      const responses = [];
      await driver.waitUntil(async () => {
        const logs = await driver.getLogs(networkType).catch(() => null);
        if (!Array.isArray(logs)) return false;
        responses.push(...extractSanitizedNavigationResponses(logs));
        try {
          lifecycle.target_navigation_receipt = buildExactTargetNavigationReceipt({
            expectedUrl: targetUrl, finalUrl: await driver.getUrl(), responses, networkSource: networkType,
          });
          return true;
        } catch (error) {
          if (String(error?.message) === 'search_target_redirected') throw error;
          return false;
        }
      }, { timeout: timeoutMs, interval: 200, timeoutMsg: 'search_target_http_invalid' });
    },
    async inspectSurface() {
      await driver.waitUntil(async () => driver.execute(() => Boolean(document.querySelector('[data-authorized-search]'))),
        { timeout: timeoutMs, interval: 250, timeoutMsg: 'search_surface_missing' });
      await driver.waitUntil(async () => driver.execute(() => document.querySelector('[data-authorized-search]')?.classList.contains('is-authorized') === true),
        { timeout: timeoutMs, interval: 250, timeoutMsg: 'search_session_not_restored' });
      await driver.execute(installSearchRuntimeProbe, configuredPolicy);
      lifecycle.search_surface_ready = true;
      lifecycle.failure_stage = 'search_journey';
      return driver.execute(() => {
        const root = document.querySelector('[data-authorized-search]'); const input = root?.querySelector('[data-search-input]');
        return { enabled: root?.dataset.searchEnabled === 'true', authorized: root?.classList.contains('is-authorized') === true,
          transport: String(root?.dataset.searchTransport || ''), input_tag: String(input?.tagName || '').toLowerCase(),
          enter_key_hint: String(input?.enterKeyHint || input?.getAttribute('enterkeyhint') || '') };
      });
    },
    async configureRequestPolicy(policy) {
      configuredPolicy = { ...policy };
      await driver.execute(installSearchRuntimeProbe, configuredPolicy);
    },
    async activity() { return driver.execute(snapshotSearchRuntimeProbe); },
    async healthDiagnostics() {
      await syncClosedDriverDiagnostics();
      const runtime = await driver.execute(snapshotSearchRuntimeProbe);
      return {
        console_errors: driverDiagnostics.console_errors,
        failed_requests: Math.max(driverDiagnostics.failed_requests,
          Number(runtime?.network?.failed_requests || 0)),
        error_responses: driverDiagnostics.error_responses,
        storage_requests: Math.max(driverDiagnostics.storage_requests,
          Number(runtime?.network?.storage_requests || 0)),
      };
    },
    async verifyAuthenticatedOwner() {
      await driver.execute(installSearchRuntimeProbe, { ...configuredPolicy, production_health: true });
      const receipt = await driver.execute(verifyAuthenticatedOwnerRuntimeProbe);
      const snapshot = await driver.execute(snapshotSearchRuntimeProbe);
      return { receipt, meter: meterWithCallbackAuthBytes(snapshot.meter) };
    },
    async typeQuery(value) {
      lifecycle.failure_stage = 'search_input_focus';
      const input = await driver.$('[data-search-input]');
      let keyboardShown = false;
      if (platform === 'ios') {
        await driver.execute(() => document.querySelector('[data-search-input]')?.scrollIntoView({ block: 'center', inline: 'center' }));
        await driver.pause(200);
        const attempt = await focusIosSafariWebInput(driver, { labels: IOS_SEARCH_INPUT_LABELS });
        keyboardShown = attempt.keyboard_shown;
      } else {
        await input.click();
        keyboardShown = await withNativeAppContext(driver, () => observeNativeKeyboard(driver));
      }
      if (!keyboardShown) throw new Error(`search_native_keyboard_missing:${platform}`);
      nativeKeyboardObserved = true;
      lifecycle.failure_stage = 'search_input_type';
      await input.setValue(value);
      lifecycle.failure_stage = 'search_input_typed';
    },
    async clearQuery() {
      lifecycle.failure_stage = 'search_input_clear';
      await (await driver.$('[data-search-input]')).clearValue();
    },
    async readQueryState() { return driver.execute(() => ({ length: document.querySelector('[data-search-input]')?.value?.length || 0 })); },
    async submitWithSearchIntent() {
      lifecycle.failure_stage = 'search_submit';
      if (!nativeKeyboardObserved) throw new Error(`search_native_keyboard_hidden:${platform}`);
      await driver.keys('\uE007');
      nativeKeyboardObserved = false;
    },
    async waitForTerminal({ minimumResponseCount = 1, minimumCardCount = 1 } = {}) {
      lifecycle.failure_stage = 'search_terminal';
      let state = null;
      await driver.waitUntil(async () => {
        state = await driver.execute((responseCount, cardCount) => {
          const probe = globalThis.__KENIGEVENTS_SEARCH_HARNESS_V1__; const status = document.querySelector('[data-search-status]');
          const submit = document.querySelector('[data-search-submit]'); const more = document.querySelector('[data-search-more]');
          const cards = document.querySelectorAll('[data-search-results] [data-event-card][data-event-id], [data-search-results] [data-search-vector-card][data-event-id]');
          return { done: (probe?.responses?.length || 0) >= responseCount
            && (probe?.meter?.pending || 0) === 0 && cards.length >= cardCount
            && submit?.getAttribute('aria-busy') !== 'true' && more?.getAttribute('aria-busy') !== 'true',
          error: status?.getAttribute('role') === 'alert' };
        }, minimumResponseCount, minimumCardCount);
        return state.error || state.done;
      }, { timeout: timeoutMs, interval: 250, timeoutMsg: 'search_terminal_timeout' });
      if (state?.error) throw new Error('search_ui_terminal_error');
      return adapter.snapshotResults();
    },
    async snapshotResults() { return driver.execute(pageResultSnapshot); },
    async realScrollResults() {
      lifecycle.failure_stage = 'search_scroll_keyboard_dismiss';
      await dismissNativeKeyboard(driver, { allowUnsupported: platform === 'ios',
        fallbackTapLabels: IOS_SEARCH_KEYBOARD_DISMISS_LABELS,
        onFallbackTapProbe: (probe) => { lifecycle.keyboard_dismiss_target_probe = probe; } });
      lifecycle.failure_stage = 'search_scroll';
      return runRealTouchScroll({
        readScrollY: () => driver.execute(() => scrollY),
        gesture: () => performNativeDocumentSwipe(driver, { platform,
          startXRatio: 0.5, startYRatio: 0.72,
          endXRatio: 0.5, endYRatio: 0.28, duration: 450,
        }),
        wait: () => driver.pause(150),
        lastCardVisible: () => driver.execute(() => {
          const cards = document.querySelectorAll('[data-search-results] [data-event-card][data-event-id], [data-search-results] [data-search-vector-card][data-event-id]');
          const card = cards[cards.length - 1]; if (!card) return false; const rect = card.getBoundingClientRect();
          return rect.top < innerHeight && rect.bottom > 0;
        }),
      });
    },
    async openFirstResult() {
      lifecycle.failure_stage = 'search_first_card_capture';
      const captured = await driver.execute(() => {
        const selector = '[data-search-results] [data-event-card][data-event-id], [data-search-results] [data-search-vector-card][data-event-id]';
        const card = document.querySelector(selector);
        const rawHref = card?.getAttribute('data-card-href')
          || card?.querySelector('a[href]')?.getAttribute('href');
        if (!card || !rawHref) return null;
        try {
          const target = new URL(rawHref, location.href);
          return { href: target.href, same_origin: target.origin === location.origin };
        } catch { return null; }
      });
      if (!captured?.href || captured.same_origin !== true) throw new Error('mobile_first_card_route_invalid');
      const beforeUrl = await driver.getUrl();
      const expectedUrl = new URL(captured.href, beforeUrl);
      if (expectedUrl.origin !== new URL(beforeUrl).origin) throw new Error('mobile_card_route_cross_origin');
      const logType = platform === 'android' ? 'performance' : 'safariNetwork';
      const initialLogs = await driver.getLogs(logType).catch(() => null);
      if (!Array.isArray(initialLogs)) throw new Error('mobile_navigation_network_log_unavailable');
      accumulateClosedDriverDiagnostics(initialLogs, driverDiagnostics, driverDiagnosticIds);
      lifecycle.first_card_captured = true;
      lifecycle.failure_stage = 'search_first_card_open';
      await (await driver.$('[data-search-results] [data-event-card][data-event-id], [data-search-results] [data-search-vector-card][data-event-id]')).click();
      await driver.waitUntil(async () => {
        const current = new URL(await driver.getUrl());
        return current.origin === expectedUrl.origin && current.pathname === expectedUrl.pathname;
      }, { timeout: timeoutMs, interval: 200, timeoutMsg: 'mobile_first_card_navigation_timeout' });
      const responses = [];
      await driver.waitUntil(async () => {
        const logs = await driver.getLogs(logType).catch(() => null);
        if (!Array.isArray(logs)) return false;
        accumulateClosedDriverDiagnostics(logs, driverDiagnostics, driverDiagnosticIds);
        responses.push(...extractSanitizedNavigationResponses(logs));
        return responses.some((item) => item.origin === expectedUrl.origin
          && item.pathname === expectedUrl.pathname && item.status === 200
          && (!item.resource_type || item.resource_type === 'document'));
      }, { timeout: timeoutMs, interval: 200, timeoutMsg: 'mobile_first_card_http_200_timeout' });
      const receipt = buildSameOriginNavigationReceipt({
        beforeUrl, expectedUrl: expectedUrl.href, finalUrl: await driver.getUrl(),
        responses, networkSource: logType,
      });
      lifecycle.first_card_opened = true;
      lifecycle.failure_stage = 'search_first_card_opened';
      return receipt;
    },
    async showMoreState() {
      return driver.execute(() => { const node = document.querySelector('[data-search-more]');
        return { visible: Boolean(node && !node.hidden && getComputedStyle(node).display !== 'none'), enabled: Boolean(node && !node.disabled) }; });
    },
    async activateShowMore() { await (await driver.$('[data-search-more]')).click(); },
    async waitForValidation() {
      lifecycle.failure_stage = 'search_validation';
      await driver.waitUntil(async () => driver.execute(() => {
        const status = document.querySelector('[data-search-status]'); const submit = document.querySelector('[data-search-submit]');
        return status?.getAttribute('role') === 'alert' && submit?.getAttribute('aria-busy') !== 'true';
      }), { timeout: Math.min(timeoutMs, 10_000), interval: 100, timeoutMsg: 'search_validation_timeout' });
      return { visible: true, kind: 'error' };
    },
    async close() {
      if (closed) return { auth_local_purge_confirmed: true, webdriver_session_deleted: true };
      lifecycle.failure_stage = 'auth_local_purge';
      let purged = false;
      try { purged = await purgeLocalAuthState(); } catch { purged = false; }
      let deleted = false;
      try { deleted = await deleteDriverSession(); } finally {
        lifecycle.auth_local_purge_confirmed = purged;
        lifecycle.webdriver_client_session_deleted = deleted;
        lifecycle.failure_stage = 'closed';
      }
      if (!purged) throw new Error('mobile_auth_local_purge_unconfirmed');
      return { auth_local_purge_confirmed: true, webdriver_session_deleted: deleted };
    },
  };
  return adapter;
}
