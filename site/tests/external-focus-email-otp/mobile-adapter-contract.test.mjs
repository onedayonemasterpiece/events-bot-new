import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';
import { parse } from 'yaml';

import { extractDriverNetworkEvents } from '../../e2e/focus-email/adapters/appium-ui.mjs';
import { buildAppiumCapabilities, classifyActiveIosApp, inspectSafariNativeUiProtocol,
  dismissNativeKeyboard, focusIosSafariWebInput, isUnsupportedIosKeyboardDismissError,
  observeNativeKeyboard, performNativeDocumentSwipe, performNativeTouchSwipe,
  prepareIosSafariWebContext, summarizeKnownSafariNativeSource } from '../../e2e/mobile-web/appium-browser.mjs';

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

test('Search and OTP consume one neutral Appium browser profile', async () => {
  const [searchAndroid, searchIos, focus] = await Promise.all([
    readFile(new URL('../../e2e/search/adapters/appium-android.mjs', import.meta.url), 'utf8'),
    readFile(new URL('../../e2e/search/adapters/appium-ios.mjs', import.meta.url), 'utf8'),
    readFile(new URL('../../e2e/focus-email/adapters/appium-ui.mjs', import.meta.url), 'utf8'),
  ]);
  for (const source of [searchAndroid, searchIos, focus]) assert.match(source, /mobile-web\/appium-browser\.mjs/u);
  assert.match(searchIos, /buildAppiumCapabilities\('ios'/u);
  assert.match(searchAndroid, /buildAppiumCapabilities\('android'/u);
  assert.match(focus, /buildAppiumCapabilities\(platform/u);
});

test('shared iOS startup clears the exact dialog before attaching Safari WebKit', async () => {
  const calls = [];
  let dismissed = false;
  const driver = {
    updateSettings: async (settings) => calls.push(['settings', settings]),
    getContext: async () => 'NATIVE_APP',
    getContexts: async () => ['NATIVE_APP', 'WEBVIEW_1'],
    switchContext: async (context) => calls.push(['context', context]),
    executeScript: async (command, args) => {
      calls.push(['execute', command, args]);
      if (command === 'mobile: alert' && args?.[0]?.action === 'accept') dismissed = true;
    },
    waitUntil: async (predicate) => {
      if (!await predicate()) throw new Error('unexpected_webview_timeout');
    },
  };
  const receipt = await prepareIosSafariWebContext(driver, {
    inspect: async () => dismissed
      ? { blocking_dialog_count: 0, known_dialog_count: 0, continue_button_count: 0 }
      : { blocking_dialog_count: 1, known_dialog_count: 1, continue_button_count: 1, action_token: 'opaque' },
  });
  assert.equal(receipt.dismissed, true);
  assert.deepEqual(calls[0], ['settings', { respectSystemAlerts: true }]);
  assert.ok(calls.some((item) => item[0] === 'execute' && item[2]?.[0]?.action === 'accept'));
  assert.deepEqual(calls.at(-1), ['context', 'WEBVIEW_1']);
});

test('native keyboard observation waits through a delayed Android IME report', async () => {
  let observations = 0;
  const driver = {
    isKeyboardShown: async () => ++observations >= 2,
    waitUntil: async (predicate) => {
      if (await predicate()) return true;
      if (await predicate()) return true;
      throw new Error('timeout');
    },
  };
  assert.equal(await observeNativeKeyboard(driver), true);
  assert.equal(observations, 2);
});

test('shared iOS input focus uses one exact native Safari accessibility match and observes its keyboard', async () => {
  const calls = [];
  let context = 'WEBVIEW_7';
  const driver = {
    getContext: async () => context,
    getContexts: async () => ['NATIVE_APP', 'WEBVIEW_7'],
    switchContext: async (next) => { context = next; calls.push(['context', next]); },
    findElements: async (using, predicate) => {
      calls.push(['find', using, predicate]);
      return [{ 'element-6066-11e4-a52e-4f735466cecf': 'field-1' }];
    },
    getElementRect: async (id) => {
      calls.push(['rect', id]);
      return { x: 20, y: 300, width: 320, height: 80 };
    },
    executeScript: async (command, args) => calls.push(['execute', command, args]),
    isKeyboardShown: async () => true,
    waitUntil: async (predicate) => {
      if (!await predicate()) throw new Error('unexpected_keyboard_timeout');
    },
  };
  const receipt = await focusIosSafariWebInput(driver, {
    labels: ['Поиск событий', 'Например: джаз на выходных'],
  });
  assert.deepEqual(receipt, { route: 'xcuitest_exact_accessibility_field', match_count: 1,
    outcome: 'tap_dispatched', keyboard_shown: true });
  assert.deepEqual(calls[0], ['context', 'NATIVE_APP']);
  assert.match(calls[1][2], /XCUIElementTypeTextField/u);
  assert.match(calls[1][2], /XCUIElementTypeTextView/u);
  assert.match(calls[1][2], /placeholderValue == 'Например: джаз на выходных'/u);
  assert.deepEqual(calls.at(-1), ['context', 'WEBVIEW_7']);
});

test('shared native touch swipe runs on device coordinates and restores the web context', async () => {
  const calls = [];
  let context = 'WEBVIEW_chrome';
  const driver = {
    getContext: async () => context,
    getContexts: async () => ['NATIVE_APP', 'WEBVIEW_chrome'],
    switchContext: async (next) => { context = next; calls.push(['context', next]); },
    getWindowSize: async () => { calls.push(['size']); return { width: 1080, height: 2400 }; },
    performActions: async (actions) => calls.push(['actions', actions]),
    releaseActions: async () => calls.push(['release']),
  };
  const receipt = await performNativeTouchSwipe(driver, {
    startXRatio: 0.5, startYRatio: 0.72, endXRatio: 0.5, endYRatio: 0.28, duration: 450,
  });
  assert.deepEqual(receipt, { route: 'w3c_native_touch', native_viewport_width: 1080,
    native_viewport_height: 2400, start_x: 540, start_y: 1728, end_x: 540,
    end_y: 672, delta_x: 0, delta_y: -1056, duration_ms: 450 });
  assert.deepEqual(calls.map((item) => item[0]), ['context', 'size', 'actions', 'release', 'context']);
  const pointer = calls[2][1][0];
  assert.equal(pointer.parameters.pointerType, 'touch');
  assert.deepEqual(pointer.actions.map((item) => item.type),
    ['pointerMove', 'pointerDown', 'pause', 'pointerMove', 'pointerUp']);
  assert.deepEqual(calls.at(-1), ['context', 'WEBVIEW_chrome']);
});

test('shared document swipe routes iOS Safari through the XCUITest native swipe command', async () => {
  const calls = [];
  let context = 'WEBVIEW_safari';
  const driver = {
    getContext: async () => context,
    getContexts: async () => ['NATIVE_APP', 'WEBVIEW_safari'],
    switchContext: async (next) => { context = next; calls.push(['context', next]); },
    getWindowSize: async () => { calls.push(['size']); return { width: 393, height: 852 }; },
    executeScript: async (...args) => calls.push(['executeScript', ...args]),
  };
  const receipt = await performNativeDocumentSwipe(driver, { platform: 'ios' });
  assert.deepEqual(receipt, { route: 'xcuitest_native_swipe', native_viewport_width: 393,
    native_viewport_height: 852, direction: 'up' });
  assert.deepEqual(calls, [
    ['context', 'NATIVE_APP'], ['size'],
    ['executeScript', 'mobile: swipe', [{ direction: 'up' }]],
    ['context', 'WEBVIEW_safari'],
  ]);
});

test('shared keyboard dismissal checks and hides the IME only in native context', async () => {
  const calls = [];
  let context = 'WEBVIEW_chrome';
  let keyboardShown = true;
  const driver = {
    getContext: async () => context,
    getContexts: async () => ['NATIVE_APP', 'WEBVIEW_chrome'],
    switchContext: async (next) => { context = next; calls.push(['context', next]); },
    isKeyboardShown: async () => { calls.push(['shown']); return keyboardShown; },
    hideKeyboard: async () => { calls.push(['hide']); keyboardShown = false; },
    waitUntil: async (probe) => probe(),
  };
  assert.equal(await dismissNativeKeyboard(driver), true);
  assert.deepEqual(calls.map((item) => item[0]), ['context', 'shown', 'hide', 'shown', 'context']);
});

test('shared iOS keyboard dismissal recovers exact unsupported hide with a verified native swipe', async () => {
  const calls = [];
  let context = 'WEBVIEW_safari';
  let keyboardShown = true;
  const driver = {
    getContext: async () => context,
    getContexts: async () => ['NATIVE_APP', 'WEBVIEW_safari'],
    switchContext: async (next) => { context = next; calls.push(['context', next]); },
    isKeyboardShown: async () => keyboardShown,
    hideKeyboard: async () => { calls.push(['hide']); throw new Error('Did not know how to dismiss the keyboard'); },
    executeScript: async (...args) => { calls.push(['executeScript', ...args]); keyboardShown = false; },
    waitUntil: async (probe) => probe(),
  };
  await assert.rejects(() => dismissNativeKeyboard(driver), /Did not know how/u);
  assert.equal(context, 'WEBVIEW_safari');
  assert.equal(await dismissNativeKeyboard(driver, { allowUnsupported: true }), true);
  assert.equal(context, 'WEBVIEW_safari');
  assert.equal(calls.filter((item) => item[0] === 'hide').length, 2);
  assert.deepEqual(calls.find((item) => item[0] === 'executeScript'),
    ['executeScript', 'mobile: swipe', [{ direction: 'down' }]]);
  for (const message of [
    'session corrupted: Did not know how to dismiss the keyboard',
    'Did not know how to dismiss the keyboard; transport closed',
    'did not know how to dismiss the keyboard',
  ]) {
    assert.equal(isUnsupportedIosKeyboardDismissError(new Error(message)), false);
  }
  assert.equal(isUnsupportedIosKeyboardDismissError(new Error('Did not know how to dismiss the keyboard')), true);
  const description = 'Did not know how to dismiss the keyboard. Try to dismiss it in the way supported by your application under test.';
  assert.equal(isUnsupportedIosKeyboardDismissError(new Error(
    `WebDriverError: Error Domain=com.facebook.WebDriverAgent Code=1 "${description}" UserInfo={NSLocalizedDescription=${description}} when running "appium/device/hide_keyboard" with method "POST"`,
  )), true);
});

test('shared iOS keyboard dismissal taps one exact safe native label when declared', async () => {
  const calls = [];
  let context = 'WEBVIEW_safari';
  let keyboardShown = true;
  const driver = {
    getContext: async () => context,
    getContexts: async () => ['NATIVE_APP', 'WEBVIEW_safari'],
    switchContext: async (next) => { context = next; calls.push(['context', next]); },
    isKeyboardShown: async () => keyboardShown,
    hideKeyboard: async () => { throw new Error('Did not know how to dismiss the keyboard'); },
    findElements: async (...args) => { calls.push(['findElements', ...args]);
      return [{ 'element-6066-11e4-a52e-4f735466cecf': 'safe-heading' }]; },
    getElementRect: async (id) => { calls.push(['rect', id]);
      return { x: 20, y: 100, width: 200, height: 40 }; },
    executeScript: async (...args) => { calls.push(['executeScript', ...args]); keyboardShown = false; },
    waitUntil: async (probe) => probe(),
  };
  assert.equal(await dismissNativeKeyboard(driver, {
    allowUnsupported: true, fallbackTapLabels: ['Найти событие'],
  }), true);
  assert.match(calls.find((item) => item[0] === 'findElements')[2],
    /type == 'XCUIElementTypeStaticText'.*name == 'Найти событие'/u);
  assert.deepEqual(calls.find((item) => item[0] === 'executeScript'),
    ['executeScript', 'mobile: tap', [{ x: 120, y: 120 }]]);
  assert.equal(context, 'WEBVIEW_safari');
});

test('shared iOS keyboard dismissal rejects missing or ambiguous safe tap targets', async () => {
  for (const matches of [[], [{ ELEMENT: 'one' }, { ELEMENT: 'two' }]]) {
    let context = 'WEBVIEW_safari';
    let tapCalls = 0;
    const driver = {
      getContext: async () => context,
      getContexts: async () => ['NATIVE_APP', 'WEBVIEW_safari'],
      switchContext: async (next) => { context = next; },
      isKeyboardShown: async () => true,
      hideKeyboard: async () => { throw new Error('Did not know how to dismiss the keyboard'); },
      findElements: async () => matches,
      executeScript: async () => { tapCalls += 1; },
    };
    await assert.rejects(() => dismissNativeKeyboard(driver, {
      allowUnsupported: true, fallbackTapLabels: ['Найти событие'],
    }), new RegExp(`mobile_keyboard_dismiss_target_count:${matches.length}`, 'u'));
    assert.equal(tapCalls, 0);
    assert.equal(context, 'WEBVIEW_safari');
  }
});

test('shared iOS keyboard dismissal reports only closed exact-label type counts before rejecting a target', async () => {
  let context = 'WEBVIEW_safari';
  let probe = null;
  const driver = {
    getContext: async () => context,
    getContexts: async () => ['NATIVE_APP', 'WEBVIEW_safari'],
    switchContext: async (next) => { context = next; },
    isKeyboardShown: async () => true,
    hideKeyboard: async () => { throw new Error('Did not know how to dismiss the keyboard'); },
    findElements: async (_using, predicate) => {
      if (predicate.includes("type == 'XCUIElementTypeOther'")) {
        return predicate.includes('visible == 1') ? [] : [{ ELEMENT: 'offscreen-other' }];
      }
      if (predicate.includes("type == 'XCUIElementTypeButton'")) return [{ ELEMENT: 'visible-button' }];
      return [];
    },
    executeScript: async () => { throw new Error('tap must not run for a non-StaticText target'); },
  };
  await assert.rejects(() => dismissNativeKeyboard(driver, {
    allowUnsupported: true,
    fallbackTapLabels: ['Найти событие'],
    onFallbackTapProbe: (value) => { probe = value; },
  }), /mobile_keyboard_dismiss_target_count:0/u);
  assert.deepEqual(probe, {
    schema_version: 'ios-keyboard-dismiss-target-v1',
    static_text: { total_count: 0, visible_count: 0 },
    other: { total_count: 1, visible_count: 0 },
    button: { total_count: 1, visible_count: 1 },
    link: { total_count: 0, visible_count: 0 },
  });
  assert.equal(context, 'WEBVIEW_safari');
});

test('shared iOS keyboard dismissal fails closed when fallback swipe leaves the IME visible', async () => {
  let context = 'WEBVIEW_safari';
  const driver = {
    getContext: async () => context,
    getContexts: async () => ['NATIVE_APP', 'WEBVIEW_safari'],
    switchContext: async (next) => { context = next; },
    isKeyboardShown: async () => true,
    hideKeyboard: async () => { throw new Error('Did not know how to dismiss the keyboard'); },
    executeScript: async () => undefined,
    waitUntil: async (probe) => { await probe(); throw new Error('timeout'); },
  };
  await assert.rejects(() => dismissNativeKeyboard(driver, { allowUnsupported: true }),
    /mobile_keyboard_dismiss_unconfirmed/u);
  assert.equal(context, 'WEBVIEW_safari');
});

test('shared keyboard dismissal fails closed when initial IME state is unobservable', async () => {
  let context = 'WEBVIEW_safari';
  let hideCalls = 0;
  const driver = {
    getContext: async () => context,
    getContexts: async () => ['NATIVE_APP', 'WEBVIEW_safari'],
    switchContext: async (next) => { context = next; },
    isKeyboardShown: async () => { throw new Error('transport details are not evidence'); },
    hideKeyboard: async () => { hideCalls += 1; },
  };
  await assert.rejects(() => dismissNativeKeyboard(driver), /mobile_keyboard_state_unavailable/u);
  assert.equal(hideCalls, 0);
  assert.equal(context, 'WEBVIEW_safari');
});

test('adapter confines diagnostic native source capture before candidate navigation and sensitive input', async () => {
  const source = await readFile(new URL('../../e2e/focus-email/adapters/appium-ui.mjs', import.meta.url), 'utf8');
  assert.doesNotMatch(source, /input\.value\s*=\s*value|osascript|Cmd-K|Toggle Software Keyboard/u);
  assert.match(source, /const source = await driver\.getPageSource\(\)/u);
  assert.match(source, /ios-startup\.raw\.xml/u);
  assert.match(source, /00-safari-launch\.png/u);
  const openInvite = source.slice(source.indexOf('async openInvite()'), source.indexOf('async verifyReleaseIdentity()'));
  assert.ok(openInvite.indexOf('ensureSafariSystemUiStable()') < openInvite.indexOf('driver.url(target.href)'));
  const shared = await readFile(new URL('../../e2e/mobile-web/appium-browser.mjs', import.meta.url), 'utf8');
  assert.ok(shared.indexOf('stabilizeSafariSystemUi') < shared.indexOf('driver.switchContext(selected)'));
  assert.match(source, /dismissNativeKeyboard/u);
  assert.doesNotMatch(source, /driver\.hideKeyboard\(\)\.catch/u);
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
  assert.equal(calls.length, 5);
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

test('Safari native inspection recognizes one exact first-run sheet without treating it as a WDA alert', async () => {
  const source = '<XCUIElementTypeApplication name="Safari"><XCUIElementTypeSheet><XCUIElementTypeStaticText name="Выбор поисковой системы"/><XCUIElementTypeButton label="Настройки"/><XCUIElementTypeButton label="Продолжить"/></XCUIElementTypeSheet></XCUIElementTypeApplication>';
  const state = await inspectSafariNativeUiProtocol({
    findElements: async (_using, predicate) => {
      if (/XCUIElementTypeButton/u.test(predicate)) return [{ ELEMENT: 'continue-button' }];
      return [{ ELEMENT: 'exact-title' }];
    },
    getAlertText: async () => { throw new Error('no WDA alert'); },
    getAlertButtons: async () => [],
    getActiveAppInfo: async () => ({ bundleId: 'com.apple.mobilesafari' }),
    getNativeSourceSummary: async () => summarizeKnownSafariNativeSource(source),
  });
  assert.deepEqual(state.action_token, { kind: 'native_exact_button', label: 'Продолжить' });
  assert.equal(state.known_dialog_count, 1);
  assert.equal(state.continue_button_count, 1);
  assert.equal(state.unknown_blocking_dialog_count, 0);
  assert.equal(state.contract_probe.current_alert_present, false);
  assert.equal(state.contract_probe.native_source.sheet_container_count, 1);
});

test('Safari native inspection fails closed for an ambiguous first-run sheet', async () => {
  const source = '<XCUIElementTypeApplication name="Safari"><XCUIElementTypeSheet><XCUIElementTypeStaticText name="Выбор поисковой системы"/><XCUIElementTypeButton label="Продолжить"/><XCUIElementTypeButton label="Продолжить"/></XCUIElementTypeSheet></XCUIElementTypeApplication>';
  const state = await inspectSafariNativeUiProtocol({
    findElements: async (_using, predicate) => /XCUIElementTypeButton/u.test(predicate)
      ? [{ ELEMENT: 'continue-1' }, { ELEMENT: 'continue-2' }]
      : [{ ELEMENT: 'exact-title' }],
    getAlertText: async () => { throw new Error('no WDA alert'); },
    getAlertButtons: async () => [],
    getActiveAppInfo: async () => ({ bundleId: 'com.apple.mobilesafari' }),
    getNativeSourceSummary: async () => summarizeKnownSafariNativeSource(source),
  });
  assert.equal(state.action_token, null);
  assert.ok(state.unknown_blocking_dialog_count > 0 || state.continue_button_count > 1);
});

test('Safari inspection ignores a retained pre-dismissal source summary after live exact elements disappear', async () => {
  const staleSource = summarizeKnownSafariNativeSource('<XCUIElementTypeApplication name="Safari"><XCUIElementTypeSheet><XCUIElementTypeStaticText name="Выбор поисковой системы"/><XCUIElementTypeButton label="Продолжить"/></XCUIElementTypeSheet></XCUIElementTypeApplication>');
  const state = await inspectSafariNativeUiProtocol({
    findElements: async () => [],
    getAlertText: async () => { throw new Error('no WDA alert'); },
    getAlertButtons: async () => [],
    getActiveAppInfo: async () => ({ bundleId: 'com.apple.mobilesafari' }),
    getNativeSourceSummary: async () => staleSource,
  });
  assert.equal(state.known_dialog_count, 0);
  assert.equal(state.blocking_dialog_count, 0);
  assert.equal(state.unknown_blocking_dialog_count, 0);
});

test('shared iOS startup clicks only the exact native first-run sheet button and never persists its source', async () => {
  const calls = [];
  let dismissed = false;
  const source = '<XCUIElementTypeApplication name="Safari"><XCUIElementTypeSheet><XCUIElementTypeStaticText name="Выбор поисковой системы"/><XCUIElementTypeButton label="Настройки"/><XCUIElementTypeButton label="Продолжить"/></XCUIElementTypeSheet></XCUIElementTypeApplication>';
  const driver = {
    updateSettings: async (settings) => calls.push(['settings', settings]),
    getContext: async () => 'NATIVE_APP',
    getContexts: async () => ['NATIVE_APP', 'WEBVIEW_1'],
    switchContext: async (context) => calls.push(['context', context]),
    findElements: async (_using, predicate) => {
      if (dismissed) return [];
      if (/XCUIElementTypeButton/u.test(predicate)) return [{ ELEMENT: 'continue-button' }];
      return [{ ELEMENT: 'exact-title' }];
    },
    getAlertText: async () => { throw new Error('no WDA alert'); },
    executeScript: async (command) => {
      if (command === 'mobile: activeAppInfo') return { bundleId: 'com.apple.mobilesafari' };
      if (command === 'mobile: alert') throw new Error('no WDA alert');
      throw new Error('unexpected_execute');
    },
    getPageSource: async () => dismissed ? '<XCUIElementTypeApplication name="Safari"/>' : source,
    elementClick: async (elementId) => { calls.push(['click', elementId]); dismissed = true; },
    waitUntil: async (predicate) => {
      if (!await predicate()) throw new Error('unexpected_webview_timeout');
    },
  };
  const receipt = await prepareIosSafariWebContext(driver);
  assert.equal(receipt.dismissed, true);
  assert.deepEqual(calls.filter(([kind]) => kind === 'click'), [['click', 'continue-button']]);
  assert.doesNotMatch(JSON.stringify(receipt), /Выбор|Продолжить|Настройки|XCUIElementType/u);
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
