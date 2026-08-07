import { createAppiumSearchAdapter } from './appium-base.mjs';

export function createIosSearchAdapter(options = {}) {
  const udid = options.udid || process.env.E2E_DEVICE_UDID;
  const prebuiltWdaPath = options.prebuiltWdaPath || process.env.E2E_PREBUILT_WDA_PATH;
  const capabilities = options.capabilities || {
    platformName: 'iOS',
    browserName: 'Safari',
    'appium:automationName': 'XCUITest',
    'appium:deviceName': options.deviceName || process.env.E2E_DEVICE_NAME || 'iPhone Simulator',
    ...(options.platformVersion || process.env.E2E_PLATFORM_VERSION
      ? { 'appium:platformVersion': options.platformVersion || process.env.E2E_PLATFORM_VERSION } : {}),
    ...(udid ? { 'appium:udid': udid } : {}),
    ...(prebuiltWdaPath ? {
      'appium:usePreinstalledWDA': true,
      'appium:prebuiltWDAPath': prebuiltWdaPath,
    } : {}),
    'appium:connectHardwareKeyboard': false,
    'appium:forceSimulatorSoftwareKeyboardPresence': true,
    'appium:settings[respectSystemAlerts]': true,
    'appium:newCommandTimeout': 180,
  };
  return createAppiumSearchAdapter({ ...options, platform: 'ios', capabilities });
}
