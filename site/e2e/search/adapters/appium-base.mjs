import { installSearchRuntimeProbe, snapshotSearchRuntimeProbe } from './runtime-probe.mjs';
import { prepareIosSafariWebContext } from '../../mobile-web/appium-browser.mjs';

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
  while (gestures < maxGestures && !cardVisible) {
    await gesture();
    gestures += 1;
    await wait();
    cardVisible = await lastCardVisible();
  }
  const after = Number(await readScrollY());
  return { performed: gestures > 0, delta_y: after - before, card_visible_after: cardVisible === true };
}

export async function createAppiumSearchAdapter(options = {}) {
  const platform = options.platform;
  if (!['android', 'ios'].includes(platform)) throw new Error(`search_mobile_platform:${platform}`);
  const timeoutMs = Number(options.timeoutMs || 60_000);
  let driver = options.driver || null;
  let ownsDriver = false;
  if (!driver) {
    const { remote } = await import('webdriverio');
    driver = await remote({
      hostname: options.hostname || '127.0.0.1', port: Number(options.port || 4723), path: options.path || '/',
      logLevel: options.logLevel || 'error',
      connectionRetryTimeout: Number(options.connectionRetryTimeout || 300_000),
      connectionRetryCount: 0,
      capabilities: options.capabilities,
    });
    ownsDriver = true;
  }
  if (platform === 'ios') await prepareIosSafariWebContext(driver);
  let configuredPolicy = {};

  const adapter = {
    async bootstrapSession(actionLink, returnTarget) {
      await driver.url(actionLink);
      await driver.waitUntil(async () => new URL(await driver.getUrl()).origin === new URL(returnTarget).origin,
        { timeout: timeoutMs, interval: 250, timeoutMsg: 'search_auth_callback_timeout' });
      await driver.waitUntil(async () => driver.execute(() => document.querySelector('[data-authorized-search]')
        ?.classList.contains('is-authorized') === true),
      { timeout: timeoutMs, interval: 250, timeoutMsg: 'search_auth_callback_session_not_restored' });
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
      const input = await driver.$('[data-search-input]');
      await input.click();
      const keyboardShown = await driver.isKeyboardShown().catch(() => false);
      if (!keyboardShown) throw new Error(`search_native_keyboard_missing:${platform}`);
      await input.setValue(value);
    },
    async clearQuery() { await (await driver.$('[data-search-input]')).clearValue(); },
    async readQueryState() { return driver.execute(() => ({ length: document.querySelector('[data-search-input]')?.value?.length || 0 })); },
    async submitWithSearchIntent() {
      if (!await driver.isKeyboardShown().catch(() => false)) throw new Error(`search_native_keyboard_hidden:${platform}`);
      await driver.keys('\uE007');
    },
    async waitForTerminal({ minimumResponseCount = 1, minimumCardCount = 1 } = {}) {
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
      const size = await driver.getWindowSize();
      return runRealTouchScroll({
        readScrollY: () => driver.execute(() => scrollY),
        gesture: () => platform === 'android'
          ? driver.execute('mobile: scrollGesture', { left: 0, top: Math.floor(size.height * 0.15), width: size.width,
            height: Math.floor(size.height * 0.7), direction: 'down', percent: 0.8 })
          : driver.execute('mobile: scroll', { direction: 'down' }),
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
