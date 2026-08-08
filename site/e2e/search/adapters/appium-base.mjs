import { installSearchRuntimeProbe, snapshotSearchRuntimeProbe } from './runtime-probe.mjs';
import { buildAppiumSessionFailureReceipt } from '../../mobile-web/appium-startup-receipt.mjs';
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
  const cards = Array.from(document.querySelectorAll('[data-search-results] [data-event-card][data-event-id], [data-search-results] [data-search-vector-card][data-event-id]'));
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
    card_renderer_unavailable: Boolean(results?.querySelector('[data-search-card-render-unavailable]')) };
}

export async function runRealTouchScroll({ readScrollY, lastCardVisible, gesture, wait, maxGestures = 24 }) {
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
  let ownsDriver = false;
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
    ownsDriver = true;
  }
  const lifecycle = {
    failure_stage: 'webdriver_session_created',
    auth_callback_started: false, auth_callback_authorized: false,
    webdriver_client_session_created: true, native_safari_stable: platform !== 'ios',
    webview_attached: platform !== 'ios', search_surface_ready: false,
    startup_attempt: [1, 2].includes(Number(process.env.E2E_APPIUM_STARTUP_ATTEMPT))
      ? Number(process.env.E2E_APPIUM_STARTUP_ATTEMPT) : 1,
  };
  if (platform === 'ios') {
    lifecycle.failure_stage = 'native_safari_webview_prepare';
    await prepareIosSafariWebContext(driver);
    lifecycle.native_safari_stable = true;
    lifecycle.webview_attached = true;
    lifecycle.failure_stage = 'auth_callback_not_started';
  }
  let configuredPolicy = {};
  let nativeKeyboardObserved = false;

  const adapter = {
    async diagnostics() { return structuredClone(lifecycle); },
    async bootstrapSession(actionLink, returnTarget) {
      lifecycle.failure_stage = 'auth_callback';
      lifecycle.auth_callback_started = true;
      await driver.url(actionLink);
      await driver.waitUntil(async () => new URL(await driver.getUrl()).origin === new URL(returnTarget).origin,
        { timeout: timeoutMs, interval: 250, timeoutMsg: 'search_auth_callback_timeout' });
      await driver.waitUntil(async () => driver.execute(() => document.querySelector('[data-authorized-search]')
        ?.classList.contains('is-authorized') === true),
      { timeout: timeoutMs, interval: 250, timeoutMsg: 'search_auth_callback_session_not_restored' });
      lifecycle.auth_callback_authorized = true;
      lifecycle.failure_stage = 'search_surface';
    },
    async open(targetUrl) {
      await driver.url(targetUrl);
      await driver.waitUntil(async () => new URL(await driver.getUrl()).origin === new URL(targetUrl).origin,
        { timeout: timeoutMs, interval: 250, timeoutMsg: 'search_target_origin_changed' });
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
          return { done: (probe?.responses?.length || 0) >= responseCount && cards.length >= cardCount
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
        fallbackTapLabels: IOS_SEARCH_KEYBOARD_DISMISS_LABELS });
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
    async close() { if (ownsDriver) await driver.deleteSession(); },
  };
  return adapter;
}
