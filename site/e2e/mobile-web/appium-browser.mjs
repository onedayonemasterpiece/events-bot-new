import { classifySafariInspection, stabilizeSafariSystemUi } from './safari-system-ui.mjs';

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
