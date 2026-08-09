const SAFE_VERSION = /^[a-z0-9][a-z0-9._+-]{0,63}$/iu;

const safeVersion = (value) => {
  const normalized = String(value || '').trim();
  return SAFE_VERSION.test(normalized) ? normalized : 'unreported';
};

const capability = (caps, ...keys) => {
  for (const key of keys) {
    if (caps && caps[key] != null && String(caps[key]).trim()) return caps[key];
  }
  return '';
};

const normalizedContextClass = (value) => {
  const context = String(value || '').toUpperCase();
  if (context === 'NATIVE_APP') return 'native';
  if (context.includes('WEBVIEW') || context.includes('CHROMIUM')) return 'webview';
  return 'other';
};

async function waitForNativeAndWebContexts(driver, {
  timeoutMs = 20_000,
  intervalMs = 500,
  now = () => Date.now(),
  sleep = (milliseconds) => new Promise((resolve) => setTimeout(resolve, milliseconds)),
} = {}) {
  const startedAt = now();
  let contexts = [];
  do {
    const observed = await driver.getContexts();
    contexts = Array.isArray(observed) ? observed : [];
    const classes = contexts.map(normalizedContextClass);
    if (classes.includes('native') && classes.includes('webview')) return contexts;
    if (now() - startedAt >= timeoutMs) break;
    await sleep(intervalMs);
  } while (true);
  return contexts;
}

const closedFailureClass = (error) => {
  const message = String(error?.message || error);
  const safari = message.match(/^safari_first_run_ui:([a-z][a-z0-9_]{2,63})$/u);
  if (safari) return `safari_first_run_ui_${safari[1]}`;
  if (/native_context_missing/iu.test(message)) return 'native_context_missing';
  if (/web_context_missing|webview|chromium/iu.test(message)) return 'web_context_missing';
  if (/viewport/iu.test(message)) return 'native_viewport_invalid';
  if (/capability|platform|automation|browser/iu.test(message)) return 'capability_mismatch';
  return 'mobile_preflight_failed';
};

const closedSafariUiEvidence = (error) => {
  if (!/^safari_first_run_ui:[a-z][a-z0-9_]{2,63}$/u.test(String(error?.message || error))) return null;
  const inspection = error?.evidence?.last_inspection;
  const probe = inspection?.contract_probe;
  const source = probe?.native_source;
  const count = (value) => Number.isSafeInteger(Number(value)) && Number(value) >= 0
    ? Number(value) : 0;
  return Object.freeze({
    known_dialog_count: count(inspection?.known_dialog_count),
    continue_button_count: count(inspection?.continue_button_count),
    active_app_owner: ['springboard', 'safari', 'other', 'unknown'].includes(probe?.active_app_owner)
      ? probe.active_app_owner : 'unknown',
    source_inspected: source?.source_inspected === true,
    application_container_count: count(source?.application_container_count),
    alert_container_count: count(source?.alert_container_count),
    sheet_container_count: count(source?.sheet_container_count),
    title_match_count: count(source?.title_match_count),
    continue_match_count: count(source?.continue_match_count),
    settings_match_count: count(source?.settings_match_count),
    matched_static_text_count: count(source?.matched_static_text_count),
    matched_button_count: count(source?.matched_button_count),
    matched_other_type_count: count(source?.matched_other_type_count),
  });
};

export function buildZeroSideEffectReceipt({ attempt = 1, driverSessionCreated = false,
  driverSessionDeleted = false } = {}) {
  return Object.freeze({
    schema_version: 'mobile-preflight-side-effects-v1',
    startup_attempt: [1, 2].includes(Number(attempt)) ? Number(attempt) : 0,
    broker_session_issued: false,
    auth_callback_started: false,
    navigation_count: 0,
    fetch_count: 0,
    search_post_count: 0,
    webdriver_client_session_created: driverSessionCreated === true,
    webdriver_client_session_deleted: driverSessionDeleted === true,
  });
}

export function isSafeMobilePreflightRetryReceipt(receipt) {
  return receipt?.schema_version === 'mobile-preflight-failure-v1'
    && receipt?.startup_attempt === 1
    && receipt?.cleanup_confirmed === true
    && receipt?.side_effects?.schema_version === 'mobile-preflight-side-effects-v1'
    && receipt.side_effects.broker_session_issued === false
    && receipt.side_effects.auth_callback_started === false
    && receipt.side_effects.navigation_count === 0
    && receipt.side_effects.fetch_count === 0
    && receipt.side_effects.search_post_count === 0
    && (receipt.side_effects.webdriver_client_session_created === false
      || receipt.side_effects.webdriver_client_session_deleted === true);
}

export function buildMobilePreflightFailureReceipt({ platform, error, attempt = 1,
  driverSessionCreated = false, driverSessionDeleted = false } = {}) {
  const cleanupConfirmed = driverSessionCreated !== true || driverSessionDeleted === true;
  const retrySafe = Number(attempt) === 1 && cleanupConfirmed === true;
  const safariUi = closedSafariUiEvidence(error);
  return Object.freeze({
    schema_version: 'mobile-preflight-failure-v1',
    platform: ['android', 'ios'].includes(platform) ? platform : 'unknown',
    startup_attempt: [1, 2].includes(Number(attempt)) ? Number(attempt) : 0,
    failure_class: closedFailureClass(error),
    cleanup_confirmed: cleanupConfirmed,
    retry_safe: retrySafe,
    ...(safariUi ? { safari_ui: safariUi } : {}),
    side_effects: buildZeroSideEffectReceipt({ attempt, driverSessionCreated, driverSessionDeleted }),
  });
}

/**
 * Prove the real native/browser transport without navigating or issuing any
 * identity or Search operation. Raw capability values and context identifiers
 * never enter the receipt. The caller must continue with this exact driver in
 * the same process; a WebDriver session id is intentionally not serializable.
 */
export async function runAppiumTransportPreflight(driver, {
  platform, expectedCapabilities = {}, startupAttempt = 1, iosPrepare = null,
  env = process.env, contextWait = {},
} = {}) {
  if (!driver || !['android', 'ios'].includes(platform)) {
    throw new TypeError('mobile_preflight_adapter_missing');
  }
  if (platform === 'ios' && typeof iosPrepare === 'function') await iosPrepare(driver);

  const caps = driver.capabilities || {};
  const actualPlatform = String(capability(caps, 'platformName', 'platform')).toLowerCase();
  const actualAutomation = String(capability(caps, 'appium:automationName', 'automationName'));
  const actualBrowser = String(capability(caps, 'browserName'));
  const actualBundle = String(capability(caps, 'appium:bundleId', 'bundleId'));
  const expectedPlatform = platform === 'android' ? 'android' : 'ios';
  const expectedAutomation = platform === 'android' ? 'UiAutomator2' : 'XCUITest';
  if (actualPlatform !== expectedPlatform) throw new Error('mobile_preflight_platform_capability_mismatch');
  if (actualAutomation.toLowerCase() !== expectedAutomation.toLowerCase()) {
    throw new Error('mobile_preflight_automation_capability_mismatch');
  }
  if (platform === 'android' && actualBrowser.toLowerCase() !== 'chrome') {
    throw new Error('mobile_preflight_browser_capability_mismatch');
  }
  if (platform === 'ios' && actualBrowser.toLowerCase() !== 'safari'
    && actualBundle.toLowerCase() !== 'com.apple.mobilesafari') {
    throw new Error('mobile_preflight_browser_capability_mismatch');
  }

  const originalContext = await driver.getContext();
  // Chrome 115+ may expose its DevTools socket shortly after the WebDriver
  // session itself is ready. A single getContexts() is therefore not a valid
  // transport preflight; poll the same, still side-effect-free session for a
  // bounded interval before classifying infrastructure unavailable.
  const contexts = await waitForNativeAndWebContexts(driver, contextWait);
  const contextClasses = [...new Set((Array.isArray(contexts) ? contexts : [])
    .map(normalizedContextClass))].filter((value) => value !== 'other').sort();
  const nativeContext = (Array.isArray(contexts) ? contexts : [])
    .find((value) => normalizedContextClass(value) === 'native');
  const webContext = (Array.isArray(contexts) ? contexts : [])
    .find((value) => normalizedContextClass(value) === 'webview');
  if (!nativeContext) throw new Error('mobile_preflight_native_context_missing');
  if (!webContext) throw new Error('mobile_preflight_web_context_missing');

  let viewport;
  try {
    if (normalizedContextClass(originalContext) !== 'native') await driver.switchContext(nativeContext);
    viewport = await driver.getWindowSize();
  } finally {
    if (normalizedContextClass(originalContext) !== 'native') await driver.switchContext(originalContext);
  }
  const width = Number(viewport?.width);
  const height = Number(viewport?.height);
  if (!Number.isInteger(width) || width < 1 || !Number.isInteger(height) || height < 1) {
    throw new Error('mobile_preflight_native_viewport_invalid');
  }

  const expectedPlatformVersion = safeVersion(capability(expectedCapabilities,
    'platformVersion', 'appium:platformVersion'));
  const reportedPlatformVersion = safeVersion(capability(caps, 'platformVersion', 'appium:platformVersion'));
  if (expectedPlatformVersion !== 'unreported' && reportedPlatformVersion !== 'unreported'
    && expectedPlatformVersion !== reportedPlatformVersion) {
    throw new Error('mobile_preflight_platform_version_mismatch');
  }

  return Object.freeze({
    schema_version: 'mobile-transport-preflight-v1',
    status: 'PASS',
    platform,
    side_effect_free: true,
    browser_ready: true,
    transport_ready: true,
    viewport_ready: true,
    auth_requests: 0,
    search_posts: 0,
    otp_requests: 0,
    supabase_requests: 0,
    transport: platform === 'android' ? 'real_android_chrome' : 'real_ios_mobile_safari',
    automation_name: expectedAutomation,
    browser_name: platform === 'android' ? 'Chrome' : 'Mobile Safari',
    platform_version: reportedPlatformVersion,
    browser_version: safeVersion(capability(caps, 'browserVersion', 'version')),
    appium_version: safeVersion(env.E2E_APPIUM_VERSION),
    driver_version: safeVersion(env.E2E_APPIUM_DRIVER_VERSION),
    ...(platform === 'ios' ? {
      xcode_version: safeVersion(env.E2E_XCODE_VERSION),
      wda_version: safeVersion(env.E2E_WDA_VERSION || env.E2E_WDA_SHA),
      wda_session_proven: true,
    } : {}),
    native_viewport: Object.freeze({ width, height }),
    context_classes: Object.freeze(contextClasses),
    same_session_continuation: true,
    continuation_handle: 'in_process_adapter',
    session_identifier_serialized: false,
    side_effects: buildZeroSideEffectReceipt({ attempt: startupAttempt, driverSessionCreated: true }),
  });
}
