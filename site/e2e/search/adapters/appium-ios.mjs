import { createAppiumSearchAdapter } from './appium-base.mjs';

export function createIosSearchAdapter(options = {}) {
  const capabilities = options.capabilities || {
    platformName: 'iOS',
    browserName: 'Safari',
    'appium:automationName': 'XCUITest',
    'appium:deviceName': options.deviceName || process.env.E2E_DEVICE_NAME || 'iPhone Simulator',
    ...(options.platformVersion || process.env.E2E_PLATFORM_VERSION
      ? { 'appium:platformVersion': options.platformVersion || process.env.E2E_PLATFORM_VERSION } : {}),
    'appium:newCommandTimeout': 180,
  };
  return createAppiumSearchAdapter({ ...options, platform: 'ios', capabilities });
}
