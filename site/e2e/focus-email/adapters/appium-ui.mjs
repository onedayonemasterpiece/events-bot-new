import { writeFile } from 'node:fs/promises';
import { join } from 'node:path';

import { observedRepoSha } from '../helpers/release-identity.mjs';
import { classifyKeyboardAcceptance, validateMobileConfig } from '../helpers/platform.mjs';

const SELECTORS = Object.freeze({
  install: '[data-intake-stage="install"]:not([hidden])', skip: '[data-focus-install-skip]',
  emailOpen: '[data-focus-email-open]', email: '#focus-email', send: '[data-focus-email-send]',
  code: '[data-focus-email-code-step]:not([hidden])', otp: '#focus-email-otp', done: '[data-focus-done-title]',
});

export function extractDriverNetworkEvents(logs) {
  const events = [];
  const fingerprints = new Set();
  const visited = new WeakSet();
  const emit = (type, requestId, rawUrl, method, status) => {
    try {
      const url = new URL(String(rawUrl));
      const event = { type, request_id: String(requestId || ''), method: String(method || 'GET').toUpperCase(),
        hostname: url.hostname, path: url.pathname, status: status != null && Number.isFinite(Number(status)) ? Number(status) : null };
      const fingerprint = JSON.stringify(event);
      if (!fingerprints.has(fingerprint)) { fingerprints.add(fingerprint); events.push(event); }
    } catch { /* malformed and relative driver log URLs are ignored */ }
  };
  const visit = (value, depth = 0) => {
    if (depth > 10 || value == null) return;
    if (typeof value === 'string') {
      const text = value.trim();
      if (!text.startsWith('{') && !text.startsWith('[')) return;
      try { visit(JSON.parse(text), depth + 1); } catch { /* not a serialized protocol event */ }
      return;
    }
    if (typeof value !== 'object' || visited.has(value)) return;
    visited.add(value);
    if (Array.isArray(value)) { value.forEach((item) => visit(item, depth + 1)); return; }
    const eventMethod = String(value.method || '');
    const params = value.params && typeof value.params === 'object' ? value.params : {};
    if (eventMethod === 'Network.requestWillBeSent') {
      emit('request', params.requestId, params.request?.url, params.request?.method, null);
    } else if (eventMethod === 'Network.responseReceived') {
      emit('response', params.requestId, params.response?.url, null, params.response?.status);
    }
    Object.values(value).forEach((item) => visit(item, depth + 1));
  };
  visit(logs);
  return events;
}

function networkBootstrap(directHost, relayHost) {
  if (window.__keE2eNetwork) return;
  const entries = [];
  let sequence = 0;
  const classify = (hostname) => hostname === directHost ? 'supabase_direct'
    : hostname === relayHost ? 'relay' : hostname === 'kenigevents.ru' ? 'kenigevents' : 'other';
  const record = (method, rawUrl, status, started, failureClass = null) => {
    try {
      const url = new URL(rawUrl, location.href);
      entries.push({ sequence: ++sequence, method: String(method || 'GET').toUpperCase(), host_class: classify(url.hostname),
        path: url.pathname, status: Number.isFinite(Number(status)) ? Number(status) : null,
        duration_ms: Math.max(0, Date.now() - started), failure_class: failureClass });
    } catch { /* sanitized probe ignores invalid URLs */ }
  };
  const originalFetch = window.fetch.bind(window);
  window.fetch = async (input, init = {}) => {
    const started = Date.now(); const url = typeof input === 'string' ? input : input.url; const method = init.method || input?.method || 'GET';
    try { const response = await originalFetch(input, init); record(method, url, response.status, started); return response; }
    catch (error) { record(method, url, null, started, String(error?.name || 'request_failed').slice(0, 40)); throw error; }
  };
  const open = XMLHttpRequest.prototype.open;
  const send = XMLHttpRequest.prototype.send;
  XMLHttpRequest.prototype.open = function patchedOpen(method, url, ...rest) { this.__ke = { method, url }; return open.call(this, method, url, ...rest); };
  XMLHttpRequest.prototype.send = function patchedSend(...args) {
    const started = Date.now();
    this.addEventListener('loadend', () => record(this.__ke?.method, this.__ke?.url, this.status, started,
      this.status === 0 ? 'request_failed' : null), { once: true });
    return send.apply(this, args);
  };
  window.__keE2eNetwork = entries;
}

async function waitText(driver, pattern, timeout = 30_000) {
  await driver.waitUntil(async () => pattern.test(await (await driver.$(SELECTORS.done)).getText()), {
    timeout, timeoutMsg: 'membership_state_timeout', interval: 350,
  });
}

export async function createAppiumUi({ platform, target, expectedRepoSha, evidenceRoot, directHost, relayHost, env = process.env }) {
  const config = validateMobileConfig(platform, env);
  const { remote } = await import('webdriverio');
  const caps = platform === 'android' ? {
    platformName: 'Android', browserName: 'Chrome',
    'wdio:enforceWebDriverClassic': true,
    'goog:loggingPrefs': { performance: 'ALL' },
    'appium:automationName': 'UiAutomator2', 'appium:deviceName': config.deviceName,
    'appium:platformVersion': config.platformVersion, 'appium:newCommandTimeout': 180,
    'appium:language': 'ru', 'appium:locale': 'RU',
  } : {
    platformName: 'iOS', browserName: 'Safari',
    'wdio:enforceWebDriverClassic': true,
    'appium:automationName': 'XCUITest', 'appium:deviceName': config.deviceName,
    'appium:platformVersion': config.platformVersion, 'appium:udid': config.udid,
    'appium:newCommandTimeout': 180, 'appium:language': 'ru', 'appium:locale': 'ru_RU',
    'appium:safariInitialUrl': 'about:blank', 'appium:includeSafariInWebviews': true,
    'appium:showSafariNetworkLog': true,
    // Keep ordinary navigation in WebKit: blanket nativeWebTap can miss
    // off-screen controls. Critical input focus is routed through an exact
    // native accessibility locator in focusWithNativeTap below.
    'appium:connectHardwareKeyboard': false,
    'appium:forceSimulatorSoftwareKeyboardPresence': true,
    ...(env.E2E_PREBUILT_WDA_PATH ? {
      'appium:usePreinstalledWDA': true,
      'appium:prebuiltWDAPath': env.E2E_PREBUILT_WDA_PATH,
    } : {}),
  };
  let driver;
  try {
    driver = await remote({ hostname: config.hostname, port: config.port, path: config.path, logLevel: 'warn',
      connectionRetryTimeout: platform === 'ios' ? 180_000 : 120_000, connectionRetryCount: 0, capabilities: caps });
  } catch (error) {
    throw new Error(`simulator_appium_startup:${String(error?.message || error).slice(0, 240)}`);
  }
  const recorder = {
    entries: [],
    count(method, suffix) { return this.entries.filter((x) => x.method === method && x.path.endsWith(suffix) && x.failure_class == null).length; },
    statuses(suffix) { return this.entries.filter((x) => x.path.endsWith(suffix)).map((x) => x.status); },
  };
  let pageEntriesSeen = 0;
  let networkSequence = 0;
  let verifiedRepoSha = null;
  const keyboard = {};
  const actualCaps = driver.capabilities || {};
  const device = { platform, device_name: config.deviceName, expected_platform_version: config.platformVersion,
    platform_version: String(actualCaps.platformVersion || config.platformVersion), browser_version: String(actualCaps.browserVersion || actualCaps.version || 'unreported'),
    os_version: String(env.E2E_HOST_OS_VERSION || 'unreported'), xcode_version: platform === 'ios' ? String(env.E2E_XCODE_VERSION || 'unreported') : null,
    udid_hash_recorded: Boolean(config.udid), automation_name: caps['appium:automationName'], browser_name: caps.browserName, appium_server: '3.6.0',
    driver_version: platform === 'android' ? '8.2.2' : '12.1.4', timezone: 'UTC', locale: platform === 'android' ? 'ru-RU' : 'ru_RU' };
  const driverRequestIndexes = new Map();
  const driverRequestIds = new Set();

  const classifyHost = (hostname) => hostname === directHost ? 'supabase_direct'
    : hostname === relayHost ? 'relay' : hostname === 'kenigevents.ru' ? 'kenigevents' : 'other';

  async function syncDriverNetwork() {
    const logType = platform === 'android' ? 'performance' : 'safariNetwork';
    const logs = await driver.getLogs(logType).catch(() => null);
    if (!Array.isArray(logs)) return false;
    for (const event of extractDriverNetworkEvents(logs)) {
      if (event.type === 'request') {
        const identity = event.request_id || `${event.method}:${event.hostname}${event.path}:${networkSequence + 1}`;
        if (driverRequestIds.has(identity)) continue;
        driverRequestIds.add(identity);
        const entry = { sequence: ++networkSequence, method: event.method, host_class: classifyHost(event.hostname),
          path: event.path, status: null, duration_ms: null, failure_class: null };
        driverRequestIndexes.set(identity, recorder.entries.length);
        recorder.entries.push(entry);
      } else if (event.request_id && driverRequestIndexes.has(event.request_id)) {
        recorder.entries[driverRequestIndexes.get(event.request_id)].status = event.status;
      }
    }
    return true;
  }

  async function syncNetwork() {
    if (await syncDriverNetwork()) return;
    const values = await driver.execute(() => Array.isArray(window.__keE2eNetwork) ? window.__keE2eNetwork : []);
    for (const entry of values.slice(pageEntriesSeen)) recorder.entries.push({ ...entry, sequence: ++networkSequence });
    pageEntriesSeen = values.length;
  }

  async function keyboardEvidence(kind, selector) {
    const dom = await driver.execute((css) => {
      const input = document.querySelector(css); const rect = input?.getBoundingClientRect();
      return { active: document.activeElement === input, visible: Boolean(rect && rect.width > 0 && rect.height > 0),
        inputMode: input?.inputMode || input?.getAttribute('inputmode') || input?.type || '',
        viewport: { innerHeight: (window.visualViewport?.offsetTop || 0) + (window.visualViewport?.height || window.innerHeight), elementBottom: rect?.bottom || 0 } };
    }, selector);
    const originalContext = await driver.getContext();
    const contexts = await driver.getContexts();
    const nativeContext = contexts.find((value) => String(value).toUpperCase() === 'NATIVE_APP');
    if (!nativeContext) throw new Error(`fail_browser_context:native_context_missing:${kind}`);
    await driver.switchContext(nativeContext);
    const shown = await driver.isKeyboardShown().catch(() => false);
    await driver.switchContext(originalContext);
    const accepted = { ...classifyKeyboardAcceptance({ shown, ...dom }), native_context_observed: true };
    keyboard[kind] = accepted;
    await writeFile(join(evidenceRoot, 'native-ui', `${kind}-keyboard.json`), `${JSON.stringify({ platform, ...accepted }, null, 2)}\n`);
    if (!accepted.passed) {
      const domain = accepted.shown ? 'FAIL_MOBILE_VIEWPORT' : 'FAIL_MOBILE_KEYBOARD';
      throw new Error(`${domain.toLowerCase()}:${kind}`);
    }
    return accepted;
  }

  async function focusWithNativeTap(selector) {
    await driver.execute((css) => document.querySelector(css)?.scrollIntoView({ block: 'center', inline: 'center' }), selector);
    await driver.pause(200);
    const element = await driver.$(selector);
    if (platform !== 'ios') {
      await element.click();
      return;
    }
    // Safari's WebKit click only changes DOM focus and does not reliably open
    // the software keyboard. Tap the labelled HTML input as the corresponding
    // XCTest text field instead. This avoids fragile Safari coordinate mapping
    // and never reads the native hierarchy or the input value.
    const labels = Object.freeze({
      [SELECTORS.email]: 'Электронная почта',
      [SELECTORS.otp]: 'Код из письма',
    });
    const label = labels[selector];
    if (!label) throw new Error(`fail_browser_context:unknown_native_input:${selector}`);
    const originalContext = await driver.getContext();
    const contexts = await driver.getContexts();
    const nativeContext = contexts.find((value) => String(value).toUpperCase() === 'NATIVE_APP');
    if (!nativeContext) throw new Error(`fail_browser_context:native_context_missing:${selector}`);
    await driver.switchContext(nativeContext);
    try {
      const escapedLabel = label.replaceAll("'", "\\'");
      const predicate = `type == 'XCUIElementTypeTextField' AND visible == 1 AND (name == '${escapedLabel}' OR label == '${escapedLabel}')`;
      const matches = await driver.findElements('-ios predicate string', predicate);
      if (matches.length !== 1) throw new Error(`fail_browser_context:native_input_match_count:${selector}:${matches.length}`);
      const elementId = matches[0]['element-6066-11e4-a52e-4f735466cecf'] || matches[0].ELEMENT;
      if (!elementId) throw new Error(`fail_browser_context:native_input_id_missing:${selector}`);
      await driver.elementClick(elementId);
    } finally {
      await driver.switchContext(originalContext);
    }
  }

  async function maskDom() {
    await driver.execute(() => {
      document.querySelectorAll('input[type="email"], input[autocomplete="one-time-code"]').forEach((node) => { node.value = ''; node.setAttribute('data-e2e-masked', 'true'); });
      document.querySelectorAll('[data-focus-email-destination], [data-static-auth-name]').forEach((node) => { node.textContent = 'f***@k***'; });
      document.querySelectorAll('[data-focus-otp-digit]').forEach((node) => { if (node.textContent) node.textContent = '•'; });
    });
  }

  return {
    kind: platform, recorder, consoles: [], keyboard,
    get observedRepoSha() { return verifiedRepoSha; },
    device,
    async openInvite() {
      await driver.url(target.href); await driver.waitUntil(async () => (await driver.getUrl()).startsWith(target.origin), { timeout: 30_000 });
      const userAgent = await driver.execute(() => navigator.userAgent);
      const version = platform === 'android' ? String(userAgent).match(/Chrome\/([^\s]+)/u)?.[1] : String(userAgent).match(/Version\/([^\s]+)/u)?.[1];
      if (version) device.browser_version = version;
      await driver.execute(networkBootstrap, directHost, relayHost);
      await syncNetwork();
    },
    async verifyReleaseIdentity() {
      verifiedRepoSha = await observedRepoSha(target, expectedRepoSha);
      return verifiedRepoSha;
    },
    async waitForInstallStage() { await (await driver.$(SELECTORS.install)).waitForDisplayed({ timeout: 20_000 }); },
    async skipInstall() { await (await driver.$(SELECTORS.skip)).click(); },
    async openEmailStep() { await (await driver.$(SELECTORS.emailOpen)).click(); },
    async focusEmailInput() { await focusWithNativeTap(SELECTORS.email); return keyboardEvidence('email', SELECTORS.email); },
    async enterEmail(value) { await (await driver.$(SELECTORS.email)).setValue(value); },
    async requestOtpWithCompetingGestures() {
      const send = await driver.$(SELECTORS.send); const email = await driver.$(SELECTORS.email);
      await email.click();
      const location = await send.getLocation(); const size = await send.getSize();
      await driver.performActions([
        { type: 'pointer', id: 'e2e-finger', parameters: { pointerType: 'touch' }, actions: [
          { type: 'pointerMove', duration: 0, origin: 'viewport', x: Math.round(location.x + size.width / 2), y: Math.round(location.y + size.height / 2) },
          { type: 'pointerDown', button: 0 }, { type: 'pointerUp', button: 0 },
        ] },
        { type: 'key', id: 'e2e-keyboard', actions: [
          { type: 'keyDown', value: '\uE007' }, { type: 'keyUp', value: '\uE007' }, { type: 'pause', duration: 0 },
        ] },
      ]);
      await driver.releaseActions();
    },
    async waitForCodeStep() { await (await driver.$(SELECTORS.code)).waitForDisplayed({ timeout: 25_000 }); },
    async focusOtpInput() { await focusWithNativeTap(SELECTORS.otp); return keyboardEvidence('otp', SELECTORS.otp); },
    async enterOtpDigitByDigit(value) { const el = await driver.$(SELECTORS.otp); for (const digit of value) { await el.addValue(digit); await driver.pause(45); } },
    async waitForMembershipConfirmed() { await waitText(driver, /Участие подтверждено/u); },
    async requestCounts() {
      await syncNetwork();
      return { issue: recorder.count('POST', '/auth/v1/otp'), verify: recorder.count('POST', '/auth/v1/verify'),
        registration: recorder.count('POST', '/rpc/register_focus_group_participant_v1'),
        registrationStatus: recorder.statuses('/rpc/register_focus_group_participant_v1').at(-1) ?? null };
    },
    async reloadOrReopen() { await syncNetwork(); await driver.refresh(); pageEntriesSeen = 0; await driver.pause(500); await driver.execute(networkBootstrap, directHost, relayHost); },
    async waitForReturningMember() { await waitText(driver, /Вы уже в фокус-группе|Участие подтверждено/u, 20_000); },
    async captureMaskedEvidence(name) { await driver.hideKeyboard().catch(() => undefined); await maskDom(); await driver.saveScreenshot(join(evidenceRoot, 'screenshots', `${name}.png`)); },
    async close() { await driver?.deleteSession().catch(() => undefined); },
  };
}
