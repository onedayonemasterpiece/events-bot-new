import { classifySafariInspection, stabilizeSafariSystemUi } from './safari-system-ui.mjs';

const IOS_WEB_INPUT_TYPES = Object.freeze([
  'XCUIElementTypeTextField',
  'XCUIElementTypeTextView',
  'XCUIElementTypeSearchField',
]);

function iosPredicateLiteral(value) {
  return String(value).replaceAll('\\', '\\\\').replaceAll("'", "\\'");
}

export async function withNativeAppContext(driver, fn) {
  if (!driver || typeof fn !== 'function') throw new TypeError('mobile_native_context_adapter_missing');
  const originalContext = await driver.getContext();
  const contexts = await driver.getContexts();
  const nativeContext = contexts.find((value) => String(value).toUpperCase() === 'NATIVE_APP');
  if (!nativeContext) throw new Error('fail_browser_context:native_context_missing');
  if (String(originalContext).toUpperCase() !== 'NATIVE_APP') await driver.switchContext(nativeContext);
  try {
    return await fn();
  } finally {
    if (String(originalContext).toUpperCase() !== 'NATIVE_APP') await driver.switchContext(originalContext);
  }
}

export async function observeNativeKeyboard(driver, { timeout = 3_000, interval = 200 } = {}) {
  let shown = false;
  await driver.waitUntil(async () => {
    shown = await driver.isKeyboardShown().catch(() => false);
    return shown;
  }, { timeout, interval }).catch(() => undefined);
  return shown;
}

const IOS_KEYBOARD_DISMISS_SHORT = 'Did not know how to dismiss the keyboard';
const IOS_KEYBOARD_DISMISS_DESCRIPTION = `${IOS_KEYBOARD_DISMISS_SHORT}. Try to dismiss it in the way supported by your application under test.`;

export function isUnsupportedIosKeyboardDismissError(error) {
  const value = String(error?.message || error);
  if (value === IOS_KEYBOARD_DISMISS_SHORT || value === IOS_KEYBOARD_DISMISS_DESCRIPTION) return true;
  const body = `Error Domain=com.facebook.WebDriverAgent Code=1 "${IOS_KEYBOARD_DISMISS_DESCRIPTION}" UserInfo={NSLocalizedDescription=${IOS_KEYBOARD_DISMISS_DESCRIPTION}} when running "appium/device/hide_keyboard" with method "POST"`;
  return value === body || value === `WebDriverError: ${body}`;
}

export async function dismissNativeKeyboard(driver, {
  allowUnsupported = false, fallbackTapLabels = [], onFallbackTapProbe = null,
} = {}) {
  const safeTapLabels = [...new Set((fallbackTapLabels || [])
    .map((value) => String(value).trim()).filter(Boolean))];
  return withNativeAppContext(driver, async () => {
    let shown = false;
    try {
      shown = await driver.isKeyboardShown();
    } catch {
      throw new Error('mobile_keyboard_state_unavailable');
    }
    if (!shown) return false;
    try {
      await driver.hideKeyboard();
    } catch (error) {
      const unsupported = isUnsupportedIosKeyboardDismissError(error);
      if (!allowUnsupported || !unsupported) throw error;
      // XCUITest documents that iPhone keyboards without a dismiss button must
      // be closed with the same gesture a user would make. Prefer a caller-
      // declared, exact non-actionable static label outside the field; unlike a
      // generic coordinate, it cannot silently land on submit/resend UI.
      if (safeTapLabels.length > 0) {
        const labelPredicate = safeTapLabels.map((value) => {
          const literal = iosPredicateLiteral(value);
          return `(name == '${literal}' OR label == '${literal}')`;
        }).join(' OR ');
        const matches = await driver.findElements('-ios predicate string',
          `visible == 1 AND type == 'XCUIElementTypeStaticText' AND (${labelPredicate})`);
        if (!Array.isArray(matches) || matches.length !== 1) {
          if (typeof onFallbackTapProbe === 'function') {
            const typeEntries = [
              ['static_text', 'XCUIElementTypeStaticText'],
              ['other', 'XCUIElementTypeOther'],
              ['button', 'XCUIElementTypeButton'],
              ['link', 'XCUIElementTypeLink'],
            ];
            const probe = { schema_version: 'ios-keyboard-dismiss-target-v1' };
            for (const [key, type] of typeEntries) {
              const typed = `type == '${type}' AND (${labelPredicate})`;
              const all = await driver.findElements('-ios predicate string', typed);
              const visible = await driver.findElements('-ios predicate string', `visible == 1 AND ${typed}`);
              probe[key] = {
                total_count: Array.isArray(all) ? all.length : -1,
                visible_count: Array.isArray(visible) ? visible.length : -1,
              };
            }
            onFallbackTapProbe(probe);
          }
          throw new Error(`mobile_keyboard_dismiss_target_count:${Array.isArray(matches) ? matches.length : 'invalid'}`);
        }
        const elementId = matches[0]['element-6066-11e4-a52e-4f735466cecf'] || matches[0].ELEMENT;
        if (!elementId) throw new Error('mobile_keyboard_dismiss_target_id_missing');
        const rect = await driver.getElementRect(elementId);
        if (!(rect?.width > 0 && rect?.height > 0)) {
          throw new Error('mobile_keyboard_dismiss_target_rect_invalid');
        }
        await driver.executeScript('mobile: tap', [{
          x: Math.round(rect.x + rect.width / 2),
          y: Math.round(rect.y + rect.height / 2),
        }]);
      } else {
        await driver.executeScript('mobile: swipe', [{ direction: 'down' }]);
      }
    }
    let hidden = false;
    await driver.waitUntil(async () => {
      hidden = !(await driver.isKeyboardShown().catch(() => true));
      return hidden;
    }, { timeout: 3_000, interval: 200,
      timeoutMsg: 'mobile_keyboard_dismiss_unconfirmed' }).catch(() => undefined);
    if (!hidden) throw new Error('mobile_keyboard_dismiss_unconfirmed');
    return true;
  });
}

/**
 * Focus a Safari web input through the exact native accessibility bridge.
 * WebKit click acknowledgement alone does not prove that the simulator opened
 * its software keyboard. Candidate names are closed, caller-owned UI strings;
 * no page source, input value or surrounding hierarchy is inspected.
 */
export async function focusIosSafariWebInput(driver, { labels, types = IOS_WEB_INPUT_TYPES } = {}) {
  const allowedLabels = [...new Set((labels || []).map((value) => String(value).trim()).filter(Boolean))];
  const allowedTypes = [...new Set((types || []).map((value) => String(value).trim()).filter(Boolean))];
  if (allowedLabels.length === 0 || allowedTypes.length === 0) throw new TypeError('ios_web_input_allowlist_missing');
  return withNativeAppContext(driver, async () => {
    const typePredicate = allowedTypes.map((value) => `type == '${iosPredicateLiteral(value)}'`).join(' OR ');
    const labelPredicate = allowedLabels.map((value) => {
      const literal = iosPredicateLiteral(value);
      return `(name == '${literal}' OR label == '${literal}' OR placeholderValue == '${literal}')`;
    }).join(' OR ');
    const predicate = `visible == 1 AND (${typePredicate}) AND (${labelPredicate})`;
    const matches = await driver.findElements('-ios predicate string', predicate);
    if (!Array.isArray(matches) || matches.length !== 1) {
      throw new Error(`fail_browser_context:native_input_match_count:${Array.isArray(matches) ? matches.length : 'invalid'}`);
    }
    const elementId = matches[0]['element-6066-11e4-a52e-4f735466cecf'] || matches[0].ELEMENT;
    if (!elementId) throw new Error('fail_browser_context:native_input_id_missing');
    const rect = await driver.getElementRect(elementId);
    if (!(rect?.width > 0 && rect?.height > 0)) throw new Error('fail_browser_context:native_input_rect_invalid');
    await driver.executeScript('mobile: tap', [{
      x: Math.round(rect.x + rect.width / 2),
      y: Math.round(rect.y + rect.height / 2),
    }]);
    const shown = await observeNativeKeyboard(driver);
    return { route: 'xcuitest_exact_accessibility_field', match_count: 1,
      outcome: 'tap_dispatched', keyboard_shown: shown };
  });
}

/** Resolve viewport ratios and execute the finger swipe wholly in NATIVE_APP. */
export async function performNativeTouchSwipe(driver, {
  startXRatio, startYRatio, endXRatio, endYRatio, duration = 450,
} = {}) {
  const ratios = [startXRatio, startYRatio, endXRatio, endYRatio].map(Number);
  const durationMs = Number(duration);
  if (ratios.some((value) => !Number.isFinite(value) || value <= 0 || value >= 1)
    || !Number.isFinite(durationMs) || durationMs < 1) {
    throw new TypeError('mobile_touch_swipe_geometry_invalid');
  }
  let receipt = null;
  await withNativeAppContext(driver, async () => {
    const viewport = await driver.getWindowSize();
    const width = Number(viewport?.width);
    const height = Number(viewport?.height);
    if (!Number.isFinite(width) || width < 1 || !Number.isFinite(height) || height < 1) {
      throw new TypeError('mobile_native_viewport_invalid');
    }
    const [fromX, fromY, toX, toY] = [
      width * ratios[0], height * ratios[1], width * ratios[2], height * ratios[3],
    ].map(Math.round);
    try {
      await driver.performActions([{
        type: 'pointer', id: 'mobile-web-finger', parameters: { pointerType: 'touch' }, actions: [
          { type: 'pointerMove', duration: 0, origin: 'viewport', x: fromX, y: fromY },
          { type: 'pointerDown', button: 0 },
          { type: 'pause', duration: 100 },
          { type: 'pointerMove', duration: Math.round(durationMs), origin: 'viewport', x: toX, y: toY },
          { type: 'pointerUp', button: 0 },
        ],
      }]);
    } finally {
      await driver.releaseActions().catch(() => undefined);
    }
    receipt = { route: 'w3c_native_touch', native_viewport_width: Math.round(width),
      native_viewport_height: Math.round(height), start_x: fromX, start_y: fromY,
      end_x: toX, end_y: toY, delta_x: toX - fromX, delta_y: toY - fromY,
      duration_ms: Math.round(durationMs) };
  });
  return receipt;
}

/** Dispatch one real browser-document swipe through the platform's accepted native route. */
export async function performNativeDocumentSwipe(driver, {
  platform, startXRatio, startYRatio, endXRatio, endYRatio, duration = 450,
} = {}) {
  if (platform === 'android') {
    return performNativeTouchSwipe(driver, {
      startXRatio, startYRatio, endXRatio, endYRatio, duration,
    });
  }
  if (platform !== 'ios') throw new TypeError('mobile_document_swipe_platform_invalid');
  let receipt = null;
  await withNativeAppContext(driver, async () => {
    const viewport = await driver.getWindowSize();
    const width = Number(viewport?.width);
    const height = Number(viewport?.height);
    if (!Number.isFinite(width) || width < 1 || !Number.isFinite(height) || height < 1) {
      throw new TypeError('mobile_native_viewport_invalid');
    }
    // XCUITest's application-level swipe dispatches a native finger gesture to
    // Safari. Its table-oriented `mobile: scroll` and W3C pointer source can
    // both acknowledge without moving the WebKit document.
    await driver.executeScript('mobile: swipe', [{ direction: 'up' }]);
    receipt = { route: 'xcuitest_native_swipe', native_viewport_width: Math.round(width),
      native_viewport_height: Math.round(height), direction: 'up' };
  });
  return receipt;
}

export function buildAppiumCapabilities(platform, config, env = process.env) {
  return platform === 'android' ? {
    platformName: 'Android', browserName: 'Chrome',
    'wdio:enforceWebDriverClassic': true,
    'goog:loggingPrefs': { performance: 'ALL' },
    'appium:automationName': 'UiAutomator2', 'appium:deviceName': config.deviceName,
    'appium:platformVersion': config.platformVersion, 'appium:newCommandTimeout': 180,
    'appium:language': 'ru', 'appium:locale': 'RU',
  } : {
    // Start Safari as a native application first. A browserName=safari session
    // tries to attach WebKit during session creation, which cannot complete
    // reliably while an iOS first-run system sheet blocks Safari.
    platformName: 'iOS',
    'wdio:enforceWebDriverClassic': true,
    'appium:automationName': 'XCUITest', 'appium:deviceName': config.deviceName,
    'appium:platformVersion': config.platformVersion, 'appium:udid': config.udid,
    'appium:bundleId': 'com.apple.mobilesafari',
    'appium:newCommandTimeout': 180, 'appium:language': 'ru', 'appium:locale': 'ru_RU',
    'appium:includeSafariInWebviews': true,
    'appium:webviewConnectTimeout': 60_000,
    'appium:webviewConnectRetries': 120,
    // Official XCUITest setting: select SpringBoard as active while it owns a
    // system alert, then return to Safari after the alert disappears.
    'appium:settings[respectSystemAlerts]': true,
    // XCUITest >=7.22 collects no Safari console bucket unless this is set.
    // false keeps raw events out of Appium stdout while enabling getLogs.
    'appium:showSafariConsoleLog': false,
    // `false` still collects fully serialized events in the safariNetwork
    // bucket, but (unlike `true`) never mirrors private target/callback URLs to
    // the Appium server log.
    'appium:showSafariNetworkLog': false,
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
}

export function classifyActiveIosApp(info) {
  const bundleId = String(info?.bundleId || '').toLowerCase();
  if (bundleId === 'com.apple.springboard') return 'springboard';
  if (bundleId === 'com.apple.mobilesafari') return 'safari';
  return bundleId ? 'other' : 'unknown';
}

/**
 * Reduce a transient, pre-input native source to allowlisted counters. The raw
 * source never leaves memory and must never be written to evidence or logs.
 */
export function summarizeKnownSafariNativeSource(source) {
  if (typeof source !== 'string' || !source) return { source_inspected: false };
  const tags = source.match(/<XCUIElementType[^>]*>/gu) || [];
  const title = 'Выбор поисковой системы';
  const action = 'Продолжить';
  const settings = 'Настройки';
  const matchingTags = tags.filter((tag) => tag.includes(title) || tag.includes(action) || tag.includes(settings));
  const countTags = (token) => matchingTags.filter((tag) => tag.includes(token)).length;
  return {
    source_inspected: true,
    application_container_count: tags.filter((tag) => tag.startsWith('<XCUIElementTypeApplication')).length,
    alert_container_count: tags.filter((tag) => tag.startsWith('<XCUIElementTypeAlert')).length,
    sheet_container_count: tags.filter((tag) => tag.startsWith('<XCUIElementTypeSheet')).length,
    title_match_count: countTags(title),
    continue_match_count: countTags(action),
    settings_match_count: countTags(settings),
    matched_static_text_count: matchingTags.filter((tag) => tag.startsWith('<XCUIElementTypeStaticText')).length,
    matched_button_count: matchingTags.filter((tag) => tag.startsWith('<XCUIElementTypeButton')).length,
    matched_other_type_count: matchingTags.filter((tag) => !tag.startsWith('<XCUIElementTypeStaticText') && !tag.startsWith('<XCUIElementTypeButton')).length,
  };
}

/**
 * Inspect only the current bounded Safari first-run alert through native
 * XCTest predicate lookup plus WDA's alert API. Exactly one alert-text line
 * must equal the expected title, and the requested button must occur
 * exactly once in that same current alert. No XPath hierarchy snapshot or
 * unscoped same-named button is accepted.
 */
export async function inspectSafariNativeUiProtocol({ findElements, getAlertText, getAlertButtons,
  getActiveAppInfo, getNativeSourceSummary }) {
  if (typeof findElements !== 'function' || typeof getAlertText !== 'function' || typeof getAlertButtons !== 'function') {
    throw new TypeError('safari_native_alert_adapter_missing');
  }
  const title = 'Выбор поисковой системы';
  const action = 'Продолжить';
  const settings = 'Настройки';
  const predicates = {
    exactVisibleStaticText: `type == 'XCUIElementTypeStaticText' AND visible == 1 AND (name == '${title}' OR label == '${title}')`,
    exactStaticText: `type == 'XCUIElementTypeStaticText' AND (name == '${title}' OR label == '${title}')`,
    containingStaticText: `type == 'XCUIElementTypeStaticText' AND (name CONTAINS '${title}' OR label CONTAINS '${title}')`,
    exactAnyElement: `(name == '${title}' OR label == '${title}')`,
  };
  const query = async (predicate) => {
    const result = await findElements('-ios predicate string', predicate);
    if (!Array.isArray(result)) throw new TypeError('safari_native_find_elements_non_array');
    return result;
  };
  // Keep WebDriver commands serial: one WDA session is the evidence source and
  // concurrent snapshots would make cross-query counts incomparable.
  const titles = await query(predicates.exactVisibleStaticText);
  const exactStaticTexts = await query(predicates.exactStaticText);
  const containingStaticTexts = await query(predicates.containingStaticText);
  const exactAnyElements = await query(predicates.exactAnyElement);
  let alertText = null;
  let buttons = [];
  try {
    alertText = String(await getAlertText());
    const result = await getAlertButtons();
    if (!Array.isArray(result)) throw new TypeError('safari_native_alert_buttons_non_array');
    buttons = result.map(String);
  } catch (error) {
    if (/safari_native_alert_buttons_non_array/iu.test(String(error?.message || error))) throw error;
    // No current WDA alert is distinct from an exact title with no actionable
    // alert ancestor; the classifier below blocks the latter as action-missing.
    alertText = null;
    buttons = [];
  }
  // WDA builds alert text by enumerating all StaticText descendants. Their
  // order is not a title-first API contract, so bind by an exact full line
  // rather than by position or substring.
  const lines = alertText?.split(/\r?\n/u).map((line) => line.trim()).filter(Boolean) || [];
  const exactTitleLineCount = lines.filter((line) => line === title).length;
  const titleSubstringCount = alertText == null ? 0 : alertText.split(title).length - 1;
  const currentAlertCount = alertText == null ? 0 : 1;
  let activeAppOwner = null;
  let nativeSource = null;
  if (typeof getActiveAppInfo === 'function') {
    activeAppOwner = classifyActiveIosApp(await getActiveAppInfo().catch(() => null));
  }
  if (typeof getNativeSourceSummary === 'function') {
    nativeSource = await getNativeSourceSummary().catch(() => ({ source_inspected: false }));
  }
  const titleBelongsToCurrentAlert = titles.length === 1 && exactTitleLineCount === 1;
  const scopedActions = titleBelongsToCurrentAlert ? buttons.filter((label) => label === action) : [];
  return {
    ...classifySafariInspection({ titleCount: titles.length, scopedActions,
      topLevelCount: currentAlertCount, knownTopLevelCount: titleBelongsToCurrentAlert ? 1 : 0 }),
    contract_probe: {
      exact_visible_static_text_count: titles.length,
      exact_static_text_count: exactStaticTexts.length,
      containing_static_text_count: containingStaticTexts.length,
      exact_any_element_count: exactAnyElements.length,
      current_alert_present: currentAlertCount === 1,
      alert_text_length: alertText?.length || 0,
      alert_text_line_count: lines.length,
      exact_title_line_count: exactTitleLineCount,
      title_substring_count: titleSubstringCount,
      alert_button_count: buttons.length,
      exact_continue_button_count: buttons.filter((label) => label === action).length,
      exact_settings_button_count: buttons.filter((label) => label === settings).length,
      ...(activeAppOwner == null ? {} : { active_app_owner: activeAppOwner }),
      ...(nativeSource == null ? {} : { native_source: nativeSource }),
    },
  };
}

export async function prepareIosSafariWebContext(driver, { inspect } = {}) {
  if (!driver) throw new TypeError('ios_safari_driver_missing');
  await driver.updateSettings({ respectSystemAlerts: true });
  const currentContext = await driver.getContext();
  const contexts = await driver.getContexts();
  const nativeContext = contexts.find((value) => String(value).toUpperCase() === 'NATIVE_APP');
  if (!nativeContext) throw new Error('fail_browser_context:native_context_missing');
  if (String(currentContext).toUpperCase() !== 'NATIVE_APP') await driver.switchContext(nativeContext);
  const inspectNative = inspect || (() => inspectSafariNativeUiProtocol({
    findElements: (using, value) => driver.findElements(using, value),
    getAlertText: () => driver.getAlertText(),
    getAlertButtons: () => driver.executeScript('mobile: alert', [{ action: 'getButtons' }]),
    getActiveAppInfo: () => driver.executeScript('mobile: activeAppInfo', []),
  }));
  const startup = await stabilizeSafariSystemUi({
    inspect: inspectNative,
    dismissKnownDialog: async (buttonLabel) => driver.executeScript('mobile: alert', [{ action: 'accept', buttonLabel }]),
  });
  let selected = null;
  await driver.waitUntil(async () => {
    const available = await driver.getContexts().catch(() => []);
    selected = available.find((value) => String(value).toUpperCase().includes('WEBVIEW')) || null;
    return Boolean(selected);
  }, { timeout: 60_000, interval: 500, timeoutMsg: 'safari_web_context_timeout' });
  await driver.switchContext(selected);
  return startup;
}
