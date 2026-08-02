import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';
import { parse } from 'yaml';

import { buildAppiumCapabilities, classifyActiveIosApp, extractDriverNetworkEvents,
  inspectSafariNativeUiProtocol, summarizeKnownSafariNativeSource } from '../../e2e/focus-email/adapters/appium-ui.mjs';

test('Appium capability builder pins real mobile browsers and iOS keyboard ownership', () => {
  const ios = buildAppiumCapabilities('ios', { deviceName: 'iPhone 16', platformVersion: '18.5', udid: 'opaque' },
    { E2E_PREBUILT_WDA_PATH: '/safe/wda.app' });
  assert.deepEqual({ platformName: ios.platformName, browserName: ios.browserName, bundleId: ios['appium:bundleId'],
    automationName: ios['appium:automationName'], hardware: ios['appium:connectHardwareKeyboard'],
    software: ios['appium:forceSimulatorSoftwareKeyboardPresence'], udid: ios['appium:udid'] },
  { platformName: 'iOS', browserName: undefined, bundleId: 'com.apple.mobilesafari', automationName: 'XCUITest', hardware: false, software: true, udid: 'opaque' });
  assert.equal(ios['appium:usePreinstalledWDA'], true);
  assert.equal(ios['appium:prebuiltWDAPath'], '/safe/wda.app');
  assert.equal(ios['appium:settings[respectSystemAlerts]'], true);
  assert.equal(ios['appium:webviewConnectTimeout'], 60_000);
  assert.equal(ios['appium:webviewConnectRetries'], 120);

  const android = buildAppiumCapabilities('android', { deviceName: 'Pixel 7', platformVersion: '15' }, {});
  assert.equal(android.browserName, 'Chrome');
  assert.equal(android['appium:automationName'], 'UiAutomator2');
});

test('adapter confines diagnostic native source capture before candidate navigation and sensitive input', async () => {
  const source = await readFile(new URL('../../e2e/focus-email/adapters/appium-ui.mjs', import.meta.url), 'utf8');
  assert.doesNotMatch(source, /input\.value\s*=\s*value|osascript|Cmd-K|Toggle Software Keyboard/u);
  assert.match(source, /const source = await driver\.getPageSource\(\)/u);
  assert.match(source, /ios-startup\.raw\.xml/u);
  assert.match(source, /00-safari-launch\.png/u);
  const openInvite = source.slice(source.indexOf('async openInvite()'), source.indexOf('async verifyReleaseIdentity()'));
  assert.ok(openInvite.indexOf('ensureSafariSystemUiStable()') < openInvite.indexOf('switchToSafariWebContext()'));
  assert.ok(openInvite.indexOf('switchToSafariWebContext()') < openInvite.indexOf('driver.url(target.href)'));
});

test('transient native source reduces to known element/container counters and owner enum', () => {
  const source = '<XCUIElementTypeApplication name="SpringBoard"><XCUIElementTypeSheet><XCUIElementTypeStaticText name="Выбор поисковой системы"/><XCUIElementTypeButton label="Настройки"/><XCUIElementTypeButton label="Продолжить"/></XCUIElementTypeSheet></XCUIElementTypeApplication>';
  assert.deepEqual(summarizeKnownSafariNativeSource(source), {
    source_inspected: true,
    application_container_count: 1,
    alert_container_count: 0,
    sheet_container_count: 1,
    title_match_count: 1,
    continue_match_count: 1,
    settings_match_count: 1,
    matched_static_text_count: 1,
    matched_button_count: 2,
    matched_other_type_count: 0,
  });
  assert.equal(classifyActiveIosApp({ bundleId: 'com.apple.springboard' }), 'springboard');
  assert.equal(classifyActiveIosApp({ bundleId: 'com.apple.mobilesafari' }), 'safari');
  assert.equal(classifyActiveIosApp({ bundleId: 'example.other' }), 'other');
  assert.equal(classifyActiveIosApp(null), 'unknown');
});

test('Safari native inspection uses exact predicate title and same-alert button contract', async () => {
  const calls = [];
  const state = await inspectSafariNativeUiProtocol({
    findElements: async (using, predicate) => {
      calls.push({ using, predicate });
      return [{ ELEMENT: 'exact-title' }];
    },
    // WDA descendant enumeration does not promise title-first ordering.
    getAlertText: async () => 'Safe body text\nВыбор поисковой системы\nMore safe body text',
    getAlertButtons: async () => ['Настройки', 'Продолжить'],
  });
  assert.equal(calls.length, 4);
  assert.ok(calls.every((call) => call.using === '-ios predicate string'));
  assert.match(calls[0].predicate, /name == 'Выбор поисковой системы'/u);
  assert.match(calls[0].predicate, /visible == 1/u);
  assert.doesNotMatch(calls[1].predicate, /visible == 1/u);
  assert.match(calls[2].predicate, / CONTAINS /u);
  assert.deepEqual(state, { known_dialog_count: 1, continue_button_count: 1,
    blocking_dialog_count: 1, unknown_blocking_dialog_count: 0,
    action_token: 'Продолжить', contract_probe: {
      exact_visible_static_text_count: 1,
      exact_static_text_count: 1,
      containing_static_text_count: 1,
      exact_any_element_count: 1,
      current_alert_present: true,
      alert_text_length: 58,
      alert_text_line_count: 3,
      exact_title_line_count: 1,
      title_substring_count: 1,
      alert_button_count: 2,
      exact_continue_button_count: 1,
      exact_settings_button_count: 1,
    } });
});

test('Safari native inspection emits safe contract probes when the visible exact predicate disagrees', async () => {
  const state = await inspectSafariNativeUiProtocol({
    findElements: async (_using, predicate) => {
      if (/visible == 1/u.test(predicate)) return [];
      if (/type == 'XCUIElementTypeStaticText'/u.test(predicate) && / CONTAINS /u.test(predicate)) return [{ ELEMENT: 'contains' }];
      if (/type == 'XCUIElementTypeStaticText'/u.test(predicate)) return [];
      return [{ ELEMENT: 'any-type' }];
    },
    getAlertText: async () => 'Выбор поисковой системы Safari body',
    getAlertButtons: async () => ['Настройки', 'Продолжить'],
  });
  assert.deepEqual(state.contract_probe, {
    exact_visible_static_text_count: 0,
    exact_static_text_count: 0,
    containing_static_text_count: 1,
    exact_any_element_count: 1,
    current_alert_present: true,
    alert_text_length: 35,
    alert_text_line_count: 1,
    exact_title_line_count: 0,
    title_substring_count: 1,
    alert_button_count: 2,
    exact_continue_button_count: 1,
    exact_settings_button_count: 1,
  });
  assert.equal(state.unknown_blocking_dialog_count, 1);
  assert.equal(state.action_token, null);
});

test('Safari native inspection captures the startup owner and source even when no alert is present', async () => {
  const state = await inspectSafariNativeUiProtocol({
    findElements: async () => [],
    getAlertText: async () => { throw new Error('no alert'); },
    getAlertButtons: async () => [],
    getActiveAppInfo: async () => ({ bundleId: 'com.apple.mobilesafari' }),
    getNativeSourceSummary: async () => ({ source_inspected: true, application_container_count: 1 }),
  });
  assert.equal(state.contract_probe.current_alert_present, false);
  assert.equal(state.contract_probe.active_app_owner, 'safari');
  assert.equal(state.contract_probe.native_source.source_inspected, true);
  assert.equal(state.contract_probe.native_source.application_container_count, 1);
});

test('same-named action in a different current alert remains unknown and non-actionable', async () => {
  const state = await inspectSafariNativeUiProtocol({
    findElements: async () => [{ ELEMENT: 'exact-title-behind-alert' }],
    getAlertText: async () => 'Неизвестное системное окно\nBody',
    getAlertButtons: async () => ['Продолжить'],
  });
  assert.equal(state.known_dialog_count, 1);
  assert.equal(state.continue_button_count, 0);
  assert.equal(state.unknown_blocking_dialog_count, 1);
  assert.equal(state.action_token, null);
});

test('Safari native inspection fails closed when a driver violates the element-array contract', async () => {
  await assert.rejects(() => inspectSafariNativeUiProtocol({
    findElements: async () => ({ ELEMENT: 'not-an-array' }), getAlertText: async () => '', getAlertButtons: async () => [],
  }),
    /safari_native_find_elements_non_array/u);
  await assert.rejects(() => inspectSafariNativeUiProtocol({
    findElements: async () => [{ ELEMENT: 'title' }],
    getAlertText: async () => 'Выбор поисковой системы', getAlertButtons: async () => ({ not: 'an array' }),
  }), /safari_native_alert_buttons_non_array/u);
});

test('immutable preview metadata records the full repository SHA used by the E2E gate', async () => {
  const source = await readFile(new URL('../../scripts/build-preview.mjs', import.meta.url), 'utf8');
  assert.match(source, /repo_sha: gitFullSha\(\)/u);
});

test('Appium network logs retain sanitized request identity and failure class', () => {
  const logs = [
    { message: JSON.stringify({ message: { method: 'Network.requestWillBeSent', params: { requestId: 'otp-1',
      request: { method: 'POST', url: 'https://example.supabase.co/auth/v1/otp?email=secret@example.test' } } } }) },
    { message: JSON.stringify({ method: 'Network.responseReceived', params: { requestId: 'otp-1',
      response: { status: 200, url: 'https://example.supabase.co/auth/v1/otp?email=secret@example.test' } } }) },
    { message: JSON.stringify({ method: 'Network.loadingFailed', params: { requestId: 'health-2', canceled: true, errorText: 'raw details' } }) },
  ];
  const events = extractDriverNetworkEvents(logs);
  assert.deepEqual(events.at(-1), { type: 'failure', request_id: 'health-2', method: null,
    hostname: null, path: null, status: null, failure_class: 'request_cancelled' });
  assert.doesNotMatch(JSON.stringify(events), /secret|@|raw details/u);
});

test('protected workflow serializes real-mail jobs and never reruns a full journey as fallback', async () => {
  const source = await readFile(new URL('../../../.github/workflows/external-focus-email-otp.yml', import.meta.url), 'utf8');
  const workflow = parse(source);
  assert.deepEqual(workflow.jobs.android.needs, ['validate', 'browser']);
  assert.deepEqual(workflow.jobs.ios.needs, ['validate', 'android']);
  assert.equal(workflow.jobs.ios['runs-on'], 'macos-15');
  assert.doesNotMatch(source, /simctl boot |open -a Simulator|defaults write com\.apple\.iphonesimulator|tail -n|\.redaction-ok" \|\| npm run e2e/u);
  assert.match(source, /write-static-otp-blocked-evidence\.mjs/u);
});

test('QA gateway downloads redaction-gated summaries and forwards the closed scenario', async () => {
  const source = await readFile(new URL('../../../.github/workflows/static-site-qa-command.yml', import.meta.url), 'utf8');
  const workflow = parse(source);
  assert.equal(workflow.jobs.run.secrets, 'inherit');
  assert.equal(workflow.jobs.run.with.scenario, '${{ needs.prepare.outputs.scenario }}');
  assert.match(source, /actions\/download-artifact@d3f86a106a0bac45b974a628896c90dbdf5c8093/u);
  assert.match(source, /static-site-qa-terminal\.mjs/u);
});
