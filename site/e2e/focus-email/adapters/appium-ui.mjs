import { writeFile } from 'node:fs/promises';
import { join } from 'node:path';

import { observedRepoSha } from '../helpers/release-identity.mjs';
import { classifyKeyboardAcceptance, keyboardFailureClass, validateMobileConfig } from '../helpers/platform.mjs';
import { sanitizedFailureClass } from '../helpers/runtime-diagnostics.mjs';
import { buildAppiumCapabilities, classifyActiveIosApp, inspectSafariNativeUiProtocol,
  prepareIosSafariWebContext, summarizeKnownSafariNativeSource } from '../../mobile-web/appium-browser.mjs';

const SELECTORS = Object.freeze({
  install: '[data-intake-stage="install"]:not([hidden])', skip: '[data-focus-install-skip]',
  emailOpen: '[data-focus-email-open]', email: '#focus-email', send: '[data-focus-email-send]',
  code: '[data-focus-email-code-step]:not([hidden])', otp: '#focus-email-otp', done: '[data-focus-done-title]',
});

export function extractDriverNetworkEvents(logs) {
  const events = [];
  const fingerprints = new Set();
  const visited = new WeakSet();
  const emit = (type, requestId, rawUrl, method, status, failureClass = null) => {
    try {
      const url = new URL(String(rawUrl));
      const event = { type, request_id: String(requestId || ''), method: String(method || 'GET').toUpperCase(),
        hostname: url.hostname, path: url.pathname, status: status != null && Number.isFinite(Number(status)) ? Number(status) : null,
        ...(failureClass ? { failure_class: failureClass } : {}) };
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
    } else if (eventMethod === 'Network.loadingFailed') {
      const event = { type: 'failure', request_id: String(params.requestId || ''), method: null,
        hostname: null, path: null, status: null,
        failure_class: sanitizedFailureClass({ canceled: params.canceled, errorText: params.errorText }) };
      const fingerprint = JSON.stringify(event);
      if (!fingerprints.has(fingerprint)) { fingerprints.add(fingerprint); events.push(event); }
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
    catch (error) { record(method, url, null, started, error?.name === 'AbortError' ? 'request_cancelled' : 'network_failure'); throw error; }
  };
  const open = XMLHttpRequest.prototype.open;
  const send = XMLHttpRequest.prototype.send;
  XMLHttpRequest.prototype.open = function patchedOpen(method, url, ...rest) { this.__ke = { method, url }; return open.call(this, method, url, ...rest); };
  XMLHttpRequest.prototype.send = function patchedSend(...args) {
    const started = Date.now();
    this.addEventListener('loadend', () => record(this.__ke?.method, this.__ke?.url, this.status, started,
      this.status === 0 ? 'network_failure' : null), { once: true });
    return send.apply(this, args);
  };
  window.__keE2eNetwork = entries;
}

async function waitText(driver, pattern, timeout = 30_000) {
  await driver.waitUntil(async () => pattern.test(await (await driver.$(SELECTORS.done)).getText()), {
    timeout, timeoutMsg: 'membership_state_timeout', interval: 350,
  });
}

export async function observeNativeKeyboard(driver, { timeout = 3_000, interval = 200 } = {}) {
  let shown = false;
  await driver.waitUntil(async () => {
    shown = await driver.isKeyboardShown().catch(() => false);
    return shown;
  }, { timeout, interval }).catch(() => undefined);
  return shown;
}

export async function createAppiumUi({ platform, target, expectedRepoSha, expectedFaultProfile = 'normal', evidenceRoot, directHost, relayHost, secrets = [], env = process.env }) {
  const config = validateMobileConfig(platform, env);
  const { remote } = await import('webdriverio');
  const caps = buildAppiumCapabilities(platform, config, env);
  let driver;
  try {
    driver = await remote({ hostname: config.hostname, port: config.port, path: config.path, logLevel: 'warn',
      connectionRetryTimeout: platform === 'ios' ? 180_000 : 120_000, connectionRetryCount: 0, capabilities: caps });
  } catch (error) {
    throw new Error(`simulator_appium_startup:${String(error?.message || error).slice(0, 240)}`);
  }
  if (platform === 'ios') {
    await driver.updateSettings({ respectSystemAlerts: true });
  }
  const actualSettings = platform === 'ios' ? await driver.getSettings().catch(() => ({})) : {};
  const recorder = {
    entries: [],
    count(method, suffix) { return this.entries.filter((x) => x.method === method && x.path.endsWith(suffix) && x.failure_class == null).length; },
    statuses(suffix) { return this.entries.filter((x) => x.path.endsWith(suffix)).map((x) => x.status); },
  };
  let pageEntriesSeen = 0;
  let networkSequence = 0;
  let verifiedRepoSha = null;
  const faultEvents = [];
  const transportEvents = [];
  let faultReceipt = null;
  let faultPageEventsSeen = 0;
  let transportPageEventsSeen = 0;
  const transportOutcomes = [];
  const transportOutcomeIds = new Set();
  const keyboard = {};
  const keyboardPreflight = { status: 'not_run', controls: {}, product: {} };
  let safariStartup = { status: platform === 'ios' ? 'pending' : 'not_applicable' };
  let startupActiveAppInfoPromise = null;
  let startupNativeSourceSummaryPromise = null;
  const nativeKeyboardAtTap = {};
  const actualCaps = driver.capabilities || {};
  const device = { platform, device_name: config.deviceName, expected_platform_version: config.platformVersion,
    platform_version: String(actualCaps.platformVersion || config.platformVersion), browser_version: String(actualCaps.browserVersion || actualCaps.version || 'unreported'),
    os_version: String(env.E2E_HOST_OS_VERSION || 'unreported'), xcode_version: platform === 'ios' ? String(env.E2E_XCODE_VERSION || 'unreported') : null,
    udid_hash_recorded: Boolean(config.udid), automation_name: caps['appium:automationName'], browser_name: platform === 'ios' ? 'Safari' : caps.browserName,
    appium_server: String(env.E2E_APPIUM_VERSION || 'unreported'),
    driver_version: String(env.E2E_APPIUM_DRIVER_VERSION || 'unreported'), wda_sha: platform === 'ios' ? String(env.E2E_WDA_SHA || 'unreported') : null,
    simulator_owner: platform === 'ios' ? 'appium_launched' : null,
    requested_connect_hardware_keyboard: platform === 'ios' ? false : null,
    requested_force_software_keyboard: platform === 'ios' ? true : null,
    reported_connect_hardware_keyboard: platform === 'ios' ? (actualCaps['appium:connectHardwareKeyboard'] ?? actualCaps.connectHardwareKeyboard ?? null) : null,
    reported_force_software_keyboard: platform === 'ios' ? (actualCaps['appium:forceSimulatorSoftwareKeyboardPresence'] ?? actualCaps.forceSimulatorSoftwareKeyboardPresence ?? null) : null,
    reported_respect_system_alerts: platform === 'ios' ? actualSettings.respectSystemAlerts === true : null,
    timezone: 'UTC', locale: platform === 'android' ? 'ru-RU' : 'ru_RU' };
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
        const entry = recorder.entries[driverRequestIndexes.get(event.request_id)];
        if (event.type === 'response') entry.status = event.status;
        if (event.type === 'failure') entry.failure_class = event.failure_class;
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

  async function syncTransportFaultEvidence() {
    const current = await driver.execute(() => ({
      receipt: globalThis['KENIGEVENTS_E2E_TRANSPORT_FAULT_INJECTOR_V1:receipt'] || null,
      faults: globalThis['KENIGEVENTS_E2E_TRANSPORT_FAULT_INJECTOR_V1:events'] || [],
      transport: globalThis['KENIGEVENTS_E2E_TRANSPORT_FAULT_INJECTOR_V1:transport-events'] || [],
    })).catch(() => ({ receipt: null, faults: [], transport: [] }));
    faultReceipt ||= current.receipt;
    faultEvents.push(...current.faults.slice(faultPageEventsSeen));
    transportEvents.push(...current.transport.slice(transportPageEventsSeen));
    faultPageEventsSeen = current.faults.length;
    transportPageEventsSeen = current.transport.length;
    const outcomes = await driver.execute(() => {
      const clients = globalThis.__KENIGEVENTS_RESILIENT_DATA_CLIENTS_V1__;
      if (!(clients instanceof Map)) return [];
      return [...clients.values()].flatMap((client) => client?.transport?.outcomeHistory?.() || []);
    }).catch(() => []);
    for (const outcome of outcomes) {
      if (!outcome?.operationId || transportOutcomeIds.has(outcome.operationId)) continue;
      transportOutcomeIds.add(outcome.operationId);
      transportOutcomes.push(outcome);
    }
    return { receipt: faultReceipt, fault_events: faultEvents, transport_events: transportEvents, transport_outcomes: transportOutcomes };
  }

  async function viewportSnapshot(selector) {
    return driver.execute((css) => {
      const input = document.querySelector(css); const rect = input?.getBoundingClientRect();
      const viewport = window.visualViewport;
      return {
        height: Number(viewport?.height || window.innerHeight || 0), width: Number(viewport?.width || window.innerWidth || 0),
        offset_top: Number(viewport?.offsetTop || 0), offset_left: Number(viewport?.offsetLeft || 0),
        element_bottom: Number(rect?.bottom || 0), element_top: Number(rect?.top || 0),
        element_left: Number(rect?.left || 0), element_right: Number(rect?.right || 0),
        element_width: Number(rect?.width || 0), element_height: Number(rect?.height || 0),
      };
    }, selector);
  }

  async function inspectSafariNativeUi() {
    if (platform !== 'ios') return classifySafariInspection();
    return inspectSafariNativeUiProtocol({
      findElements: (using, value) => driver.findElements(using, value),
      getAlertText: () => driver.getAlertText(),
      getAlertButtons: () => driver.executeScript('mobile: alert', [{ action: 'getButtons' }]),
      getActiveAppInfo: () => {
        startupActiveAppInfoPromise ??= driver.executeScript('mobile: activeAppInfo', []);
        return startupActiveAppInfoPromise;
      },
      // This runs before candidate navigation and before any identity/OTP
      // input. Retain the complete clean-simulator tree for the bounded system
      // UI diagnosis requested by the operator, then also derive stable counts.
      getNativeSourceSummary: () => {
        startupNativeSourceSummaryPromise ??= (async () => {
          const source = await driver.getPageSource();
          await writeFile(join(evidenceRoot, 'native-ui', 'ios-startup.raw.xml'), source);
          return summarizeKnownSafariNativeSource(source);
        })();
        return startupNativeSourceSummaryPromise;
      },
    });
  }

  async function withNativeContext(fn) {
    const originalContext = await driver.getContext();
    const contexts = await driver.getContexts();
    const nativeContext = contexts.find((value) => String(value).toUpperCase() === 'NATIVE_APP');
    if (!nativeContext) throw new Error('fail_browser_context:native_context_missing');
    await driver.switchContext(nativeContext);
    try { return await fn(); } finally { await driver.switchContext(originalContext); }
  }

  async function ensureSafariSystemUiStable() {
    if (platform !== 'ios') return;
    try {
      const detail = await prepareIosSafariWebContext(driver, { inspect: inspectSafariNativeUi });
      safariStartup = { status: 'passed', ...detail };
    } catch (error) {
      safariStartup = { status: 'blocked', ...(error?.evidence || {}), failure: String(error?.message || error).split(':').slice(0, 2).join(':') };
      await driver.saveScreenshot(join(evidenceRoot, 'screenshots', '00-safari-system-ui-blocked.png')).catch(() => undefined);
      throw error;
    } finally {
      await writeFile(join(evidenceRoot, 'native-ui', 'ios-startup.json'), `${JSON.stringify(safariStartup, null, 2)}\n`);
    }
  }

  async function assertNoBlockingSafariModal() {
    if (platform !== 'ios') return;
    const state = await withNativeContext(inspectSafariNativeUi);
    if (Number(state.blocking_dialog_count) !== 0) {
      safariStartup = { ...safariStartup, status: 'blocked', obstruction_free: false,
        failure: 'safari_first_run_ui:blocking_modal_before_input', late_blocking_dialog_count: Number(state.blocking_dialog_count) };
      await writeFile(join(evidenceRoot, 'native-ui', 'ios-startup.json'), `${JSON.stringify(safariStartup, null, 2)}\n`);
      await driver.saveScreenshot(join(evidenceRoot, 'screenshots', '00-safari-system-ui-late-blocked.png')).catch(() => undefined);
      throw new Error('safari_first_run_ui:blocking_modal_before_input');
    }
  }

  async function keyboardEvidence(kind, selector, baseline) {
    const dom = await driver.execute((css) => {
      const input = document.querySelector(css); const rect = input?.getBoundingClientRect();
      return { active: document.activeElement === input, visible: Boolean(rect && rect.width > 0 && rect.height > 0),
        inputMode: input?.inputMode || input?.getAttribute('inputmode') || input?.type || '' };
    }, selector);
    const focused = await viewportSnapshot(selector);
    const shown = nativeKeyboardAtTap[kind] ?? await withNativeContext(() => driver.isKeyboardShown().catch(() => false));
    const viewport = { baseline, focused, height_delta: Number(baseline?.height || 0) - Number(focused?.height || 0) };
    const accepted = { ...classifyKeyboardAcceptance({ shown, ...dom, viewport }), native_context_observed: true,
      activation_attempts: keyboard[kind]?.activation_attempts || [] };
    keyboard[kind] = accepted;
    await writeFile(join(evidenceRoot, 'native-ui', `${kind}-keyboard.json`), `${JSON.stringify({ platform, ...accepted }, null, 2)}\n`);
    if (!accepted.passed) {
      const domain = keyboardFailureClass(accepted);
      throw new Error(`${domain}:${kind}`);
    }
    return accepted;
  }

  async function focusWithNativeTap(selector, label, kind) {
    await assertNoBlockingSafariModal();
    await driver.execute((css) => document.querySelector(css)?.scrollIntoView({ block: 'center', inline: 'center' }), selector);
    await driver.pause(200);
    const baseline = await viewportSnapshot(selector);
    const attempts = [];
    if (platform !== 'ios') {
      attempts.push({ route: 'webdriver_element_click', outcome: 'dispatched' });
      await (await driver.$(selector)).click();
      const shown = await withNativeContext(() => observeNativeKeyboard(driver));
      nativeKeyboardAtTap[kind] = shown;
      attempts.at(-1).keyboard_shown = shown;
    } else {
      await withNativeContext(async () => {
        const escapedLabel = label.replaceAll("'", "\\'");
        const predicate = `type == 'XCUIElementTypeTextField' AND visible == 1 AND (name == '${escapedLabel}' OR label == '${escapedLabel}')`;
        const matches = await driver.findElements('-ios predicate string', predicate);
        attempts.push({ route: 'xcuitest_exact_accessibility_field', match_count: matches.length });
        if (matches.length !== 1) throw new Error(`fail_browser_context:native_input_match_count:${kind}:${matches.length}`);
        const elementId = matches[0]['element-6066-11e4-a52e-4f735466cecf'] || matches[0].ELEMENT;
        if (!elementId) throw new Error(`fail_browser_context:native_input_id_missing:${kind}`);
        const rect = await driver.getElementRect(elementId);
        if (!(rect.width > 0 && rect.height > 0)) throw new Error(`fail_browser_context:native_input_rect_invalid:${kind}`);
        await driver.executeScript('mobile: tap', [{ x: Math.round(rect.x + rect.width / 2), y: Math.round(rect.y + rect.height / 2) }]);
        attempts.at(-1).outcome = 'tap_dispatched';
        const shown = await observeNativeKeyboard(driver);
        nativeKeyboardAtTap[kind] = shown;
        attempts.at(-1).keyboard_shown = shown;
      });
    }
    keyboard[kind] = { activation_attempts: attempts };
    return keyboardEvidence(kind, selector, baseline);
  }

  async function preflightMobileKeyboards() {
    if (platform !== 'ios') return null;
    keyboardPreflight.status = 'running';
    await driver.execute(() => {
      const host = document.createElement('section'); host.id = 'ke-e2e-keyboard-preflight';
      host.style.cssText = 'position:fixed;inset:80px 12px auto 12px;z-index:2147483647;background:white;padding:16px';
      host.innerHTML = '<label for="ke-control-email">E2E control email</label><input id="ke-control-email" type="email" inputmode="email" aria-label="E2E control email"><label for="ke-control-numeric">E2E control numeric</label><input id="ke-control-numeric" inputmode="numeric" aria-label="E2E control numeric">';
      document.body.append(host);
    });
    try {
      for (const [kind, selector, label] of [['control_email', '#ke-control-email', 'E2E control email'], ['control_numeric', '#ke-control-numeric', 'E2E control numeric']]) {
        const accepted = await focusWithNativeTap(selector, label, kind);
        keyboardPreflight.controls[kind] = accepted;
        await driver.saveScreenshot(join(evidenceRoot, 'screenshots', `00-${kind}-empty-keyboard.png`));
        await driver.hideKeyboard().catch(() => undefined);
      }
      keyboardPreflight.status = 'passed'; return keyboardPreflight;
    } catch (error) {
      keyboardPreflight.status = 'blocked'; keyboardPreflight.failure = 'control_keyboard_unavailable';
      if (/safari_first_run_ui/iu.test(String(error?.message || error))) throw error;
      throw new Error(`ios_simulator_keyboard:${String(error?.message || error).split(':').at(-1)}`);
    } finally {
      await driver.execute(() => document.querySelector('#ke-e2e-keyboard-preflight')?.remove()).catch(() => undefined);
      await writeFile(join(evidenceRoot, 'native-ui', 'keyboard-preflight.json'), `${JSON.stringify(keyboardPreflight, null, 2)}\n`);
    }
  }

  async function maskDom() {
    await driver.execute(() => {
      const sensitiveInputs = Array.from(document.querySelectorAll('input[type="email"], input[autocomplete="one-time-code"]'));
      if (sensitiveInputs.includes(document.activeElement)) document.activeElement.blur();
      sensitiveInputs.forEach((node) => { node.value = ''; node.setAttribute('data-e2e-masked', 'true'); });
      document.querySelectorAll('[data-focus-email-destination], [data-static-auth-name]').forEach((node) => { node.textContent = 'f***@k***'; });
      document.querySelectorAll('[data-focus-otp-digit]').forEach((node) => { if (node.textContent) node.textContent = '•'; });
    });
  }

  return {
    kind: platform, recorder, consoles: [], keyboard,
    get observedRepoSha() { return verifiedRepoSha; },
    get safariStartup() { return safariStartup; },
    get keyboardPreflight() { return keyboardPreflight; },
    device,
    async openInvite() {
      if (platform === 'ios') {
        // The session starts in native Safari so WebKit attachment cannot be
        // blocked by SpringBoard's first-run sheet.
        await driver.saveScreenshot(join(evidenceRoot, 'screenshots', '00-safari-launch.png'));
        await ensureSafariSystemUiStable();
        await driver.saveScreenshot(join(evidenceRoot, 'screenshots', '00-safari-system-ui-cleared.png'));
      }
      await driver.url(target.href);
      try {
        await driver.waitUntil(async () => (await driver.getUrl()).startsWith(target.origin), { timeout: 30_000 });
      } catch {
        // Fresh hosted Safari can occasionally acknowledge the navigation
        // command while remaining at about:blank. This is a pre-side-effect
        // simulator/browser startup failure, not a product assertion failure.
        throw new Error('simulator_safari_navigation:target_origin_not_reached');
      }
      if (platform !== 'ios') await ensureSafariSystemUiStable();
      const userAgent = await driver.execute(() => navigator.userAgent);
      const version = platform === 'android' ? String(userAgent).match(/Chrome\/([^\s]+)/u)?.[1] : String(userAgent).match(/Version\/([^\s]+)/u)?.[1];
      if (version) device.browser_version = version;
      await driver.execute(networkBootstrap, directHost, relayHost);
      await syncNetwork();
    },
    async verifyReleaseIdentity() {
      verifiedRepoSha = await observedRepoSha(target, expectedRepoSha, fetch, expectedFaultProfile);
      return verifiedRepoSha;
    },
    async waitForInstallStage() { await (await driver.$(SELECTORS.install)).waitForDisplayed({ timeout: 20_000 }); },
    async preflightMobileKeyboards() { return preflightMobileKeyboards(); },
    async skipInstall() { await (await driver.$(SELECTORS.skip)).click(); },
    async openEmailStep() { await (await driver.$(SELECTORS.emailOpen)).click(); },
    async focusEmailInput() {
      const accepted = await focusWithNativeTap(SELECTORS.email, 'Электронная почта', 'email');
      keyboardPreflight.product.email = accepted;
      await writeFile(join(evidenceRoot, 'native-ui', 'keyboard-preflight.json'), `${JSON.stringify(keyboardPreflight, null, 2)}\n`);
      return accepted;
    },
    async enterEmail(value) { await assertNoBlockingSafariModal(); await (await driver.$(SELECTORS.email)).setValue(value); },
    async requestOtpWithCompetingGestures() {
      await assertNoBlockingSafariModal();
      const send = await driver.$(SELECTORS.send); const email = await driver.$(SELECTORS.email);
      await email.click();
      if (platform === 'ios') {
        // XCUITest accepts both commands as ordinary Safari user input, while
        // its web-context W3C touch source can acknowledge performActions
        // without dispatching the pointer event to WebKit. Start the button
        // tap and the focused field's Return gesture as one competing batch;
        // the product's selected-once guard must collapse them to one issue.
        await Promise.all([
          send.click(),
          driver.keys('\uE007'),
        ]);
        return;
      }
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
    async focusOtpInput() {
      const accepted = await focusWithNativeTap(SELECTORS.otp, 'Код из письма', 'otp');
      keyboardPreflight.product.otp = accepted;
      await writeFile(join(evidenceRoot, 'native-ui', 'keyboard-preflight.json'), `${JSON.stringify(keyboardPreflight, null, 2)}\n`);
      return accepted;
    },
    setOtpSecret(value) { if (value && !secrets.includes(value)) secrets.push(value); },
    async captureEmptyKeyboardEvidence(name) {
      await driver.saveScreenshot(join(evidenceRoot, 'screenshots', `${name}.png`));
    },
    async enterOtpDigitByDigit(value) {
      await assertNoBlockingSafariModal();
      const el = await driver.$(SELECTORS.otp);
      for (const digit of value) { await el.addValue(digit); await driver.pause(45); }
    },
    async waitForMembershipConfirmed() { await waitText(driver, /Участие подтверждено/u); },
    async requestCounts() {
      await syncNetwork(); const internal = await syncTransportFaultEvidence();
      const countOutcome = (operation) => internal.transport_outcomes.filter((item) => item.operation === operation).length;
      const outcomeStatus = (operation) => internal.transport_outcomes.filter((item) => item.operation === operation).at(-1)?.status ?? null;
      const issue = countOutcome('auth.otp'); const verify = countOutcome('auth.verify');
      const registration = countOutcome('rpc.register_focus_group_participant_v1');
      return {
        issue: issue || recorder.count('POST', '/auth/v1/otp'),
        verify: verify || recorder.count('POST', '/auth/v1/verify'),
        registration: registration || recorder.count('POST', '/rpc/register_focus_group_participant_v1'),
        registrationStatus: outcomeStatus('rpc.register_focus_group_participant_v1')
          ?? recorder.statuses('/rpc/register_focus_group_participant_v1').at(-1) ?? null,
      };
    },
    async reloadOrReopen() {
      await syncNetwork(); await syncTransportFaultEvidence(); await driver.refresh();
      pageEntriesSeen = 0; faultPageEventsSeen = 0; transportPageEventsSeen = 0;
      await driver.pause(500); await driver.execute(networkBootstrap, directHost, relayHost);
    },
    async transportFaultEvidence() { return syncTransportFaultEvidence(); },
    async waitForReturningMember() { await waitText(driver, /Вы уже в фокус-группе|Участие подтверждено/u, 20_000); },
    async captureMaskedEvidence(name) {
      await driver.hideKeyboard().catch(() => undefined);
      await maskDom();
      // Mobile Safari paints the native text-field overlay one frame after the
      // DOM value changes. Waiting here prevents the prior value from leaking
      // into the screenshot even though the DOM was already masked.
      if (platform === 'ios') await driver.pause(200);
      await driver.saveScreenshot(join(evidenceRoot, 'screenshots', `${name}.png`));
    },
    async close() { await driver?.deleteSession().catch(() => undefined); },
  };
}
