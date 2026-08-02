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
  const viewportHeight = Number(viewport?.focused?.height ?? viewport?.height ?? viewport?.innerHeight);
  const elementBottom = Number(viewport?.focused?.element_bottom ?? viewport?.elementBottom);
  const geometry = viewportHeight > 0 && elementBottom <= viewportHeight + Number(viewport?.focused?.offset_top || 0) + 2;
  const inputPath = ['email', 'numeric', 'decimal', 'tel'].includes(String(inputMode || '').toLowerCase());
  return {
    passed: shown === true && active === true && visible === true && geometry && inputPath,
    shown: shown === true, active: active === true, visible: visible === true,
    input_mode: String(inputMode || ''), input_visible_in_visual_viewport: Boolean(geometry),
    visual_viewport: viewport || null,
  };
}

export function keyboardFailureClass(acceptance) {
  if (acceptance?.passed) return null;
  return acceptance?.shown && !acceptance?.input_visible_in_visual_viewport
    ? 'fail_mobile_viewport' : 'fail_mobile_keyboard';
}
