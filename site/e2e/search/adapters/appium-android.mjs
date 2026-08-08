import { createAppiumSearchAdapter } from './appium-base.mjs';

export function createAndroidSearchAdapter(options = {}) {
  const capabilities = options.capabilities || {
    platformName: 'Android',
    browserName: 'Chrome',
    'wdio:enforceWebDriverClassic': true,
    'appium:automationName': 'UiAutomator2',
    'appium:deviceName': options.deviceName || process.env.E2E_DEVICE_NAME || 'Android Emulator',
    ...(options.platformVersion || process.env.E2E_PLATFORM_VERSION
      ? { 'appium:platformVersion': options.platformVersion || process.env.E2E_PLATFORM_VERSION } : {}),
    'appium:newCommandTimeout': 180,
  };
  return createAppiumSearchAdapter({ ...options, platform: 'android', capabilities });
}
