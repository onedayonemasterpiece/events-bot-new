import { createAppiumSearchAdapter } from './appium-base.mjs';
import { buildAppiumCapabilities } from '../../mobile-web/appium-browser.mjs';

export function createAndroidSearchAdapter(options = {}) {
  const capabilities = options.capabilities || buildAppiumCapabilities('android', {
    deviceName: options.deviceName || process.env.E2E_DEVICE_NAME || 'Android Emulator',
    platformVersion: options.platformVersion || process.env.E2E_PLATFORM_VERSION,
  });
  return createAppiumSearchAdapter({ ...options, platform: 'android', capabilities });
}
