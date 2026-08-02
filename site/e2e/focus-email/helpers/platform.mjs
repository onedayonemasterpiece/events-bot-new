export const PLATFORMS = Object.freeze(['browser', 'android', 'ios']);

export function validatePlatform(value) {
  const platform = String(value || 'browser').trim().toLowerCase();
  if (!PLATFORMS.includes(platform)) throw new Error(`platform_invalid:${platform}`);
  return platform;
}

export function selectedPlatforms(value) {
  const selected = String(value || 'browser').trim().toLowerCase();
  if (selected === 'all') return [...PLATFORMS];
  return [validatePlatform(selected)];
}

export function assertSequentialMailboxPolicy({ maxParallel = 1, platforms = PLATFORMS } = {}) {
  if (Number(maxParallel) !== 1 || !Array.isArray(platforms)) throw new Error('mailbox_concurrency_policy_invalid');
  return true;
}

export function validateMobileConfig(platform, env = process.env) {
  if (!['android', 'ios'].includes(platform)) throw new Error('mobile_platform_invalid');
  const port = Number(env.APPIUM_PORT || 4723);
  if (!Number.isInteger(port) || port < 1 || port > 65535) throw new Error('appium_configuration_invalid');
  const deviceName = String(env.E2E_DEVICE_NAME || '').trim();
  const platformVersion = String(env.E2E_PLATFORM_VERSION || '').trim();
  if (!deviceName || !platformVersion) throw new Error('simulator_configuration_missing');
  if (platform === 'ios' && !String(env.E2E_DEVICE_UDID || '').trim()) throw new Error('simulator_runtime_missing');
  return {
    hostname: String(env.APPIUM_HOST || '127.0.0.1'), port,
    path: String(env.APPIUM_BASE_PATH || '/wd/hub'), deviceName, platformVersion,
    udid: String(env.E2E_DEVICE_UDID || '').trim() || undefined,
  };
}

export function classifyKeyboardAcceptance({ shown, active, visible, inputMode, viewport }) {
  const geometry = viewport && Number(viewport.innerHeight) > 0 && Number(viewport.elementBottom) <= Number(viewport.innerHeight) + 2;
  const inputPath = ['email', 'numeric', 'decimal', 'tel'].includes(String(inputMode || '').toLowerCase());
  return {
    passed: shown === true && active === true && visible === true && geometry && inputPath,
    shown: shown === true, active: active === true, visible: visible === true,
    input_mode: String(inputMode || ''), viewport_geometry_ok: Boolean(geometry),
  };
}
