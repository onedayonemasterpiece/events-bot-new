import { createAppiumSearchAdapter } from './appium-base.mjs';
import { buildAppiumCapabilities } from '../../mobile-web/appium-browser.mjs';

export function createIosSearchAdapter(options = {}) {
  const udid = options.udid || process.env.E2E_DEVICE_UDID;
  const env = { ...process.env,
    E2E_PREBUILT_WDA_PATH: options.prebuiltWdaPath || process.env.E2E_PREBUILT_WDA_PATH };
  const capabilities = options.capabilities || buildAppiumCapabilities('ios', {
    deviceName: options.deviceName || process.env.E2E_DEVICE_NAME || 'iPhone Simulator',
    platformVersion: options.platformVersion || process.env.E2E_PLATFORM_VERSION,
    udid,
  }, env);
  return createAppiumSearchAdapter({ ...options, platform: 'ios', capabilities });
}
