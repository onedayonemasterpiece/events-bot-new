import { open } from 'node:fs/promises';
import { buildZeroSideEffectReceipt } from './appium-preflight.mjs';

const MAX_LOG_CHARS = 2_000_000;

const has = (source, pattern) => pattern.test(source);

/**
 * Collapse a raw local Appium/XCUITest log into a closed, secret-free phase
 * receipt. The raw source must never be returned, persisted or uploaded.
 */
export function summarizeAppiumSessionStartup(value, { truncated = false } = {}) {
  const source = typeof value === 'string' ? value.slice(-MAX_LOG_CHARS) : '';
  return {
    log_inspected: source.length > 0,
    log_truncated: truncated === true || (typeof value === 'string' && value.length > MAX_LOG_CHARS),
    simulator_started: has(source, /Event 'simStarted'|Simulator.+(?:booted|started)/iu),
    wda_install_attempted: has(source, /Installing prebuilt WDA at/iu),
    wda_start_attempted: has(source, /Event 'wdaStartAttempted'|Trying to start WebDriverAgent/iu),
    wda_start_failed: has(source, /Event 'wdaStartFailed'|Unable to launch WebDriverAgent/iu),
    wda_session_attempted: has(source, /Event 'wdaSessionAttempted'|Sending createSession command to WDA/iu),
    wda_session_started: has(source, /Event 'wdaSessionStarted'/iu),
    wda_session_failed: has(source, /Event 'wdaSessionFailed'|Unable to start WebDriverAgent session/iu),
    wda_started: has(source, /Event 'wdaStarted'|WDA session startup took \d+ms/iu),
  };
}

export function summarizeAndroidAppiumSessionStartup(value, { truncated = false } = {}) {
  const source = typeof value === 'string' ? value.slice(-MAX_LOG_CHARS) : '';
  return {
    log_inspected: source.length > 0,
    log_truncated: truncated === true || (typeof value === 'string' && value.length > MAX_LOG_CHARS),
    chromedriver_discovery_attempted: has(source, /chromedriver/iu),
    chromedriver_missing: has(source, /No Chromedriver found|Could not find a Chromedriver|chromedriver.+not found/iu),
    chromedriver_download_failed: has(source, /chromedriver.+(?:download|storage).+(?:fail|error)|(?:fail|error).+download.+chromedriver/iu),
    chrome_session_failed: has(source, /Unable to (?:automate|start).+Chrome|Chrome session.+fail|unknown error:.+Chrome/iu),
    web_context_failed: has(source, /No webviews found|web ?view not found|failed to connect to socket.+chrome_devtools/iu),
    uiautomator_server_failed: has(source, /UiAutomator2.+(?:server|instrumentation).+(?:fail|error)|socket hang up/iu),
  };
}

async function readBoundedLogTail(path) {
  const handle = await open(path, 'r').catch(() => null);
  if (!handle) return { source: '', truncated: false };
  try {
    const size = Number((await handle.stat()).size || 0);
    const length = Math.min(MAX_LOG_CHARS, Math.max(0, size));
    if (length === 0) return { source: '', truncated: false };
    const buffer = Buffer.alloc(length);
    const { bytesRead } = await handle.read(buffer, 0, length, Math.max(0, size - length));
    return { source: buffer.subarray(0, bytesRead).toString('utf8'), truncated: size > MAX_LOG_CHARS };
  } finally {
    await handle.close().catch(() => undefined);
  }
}

function closedErrorClass(error) {
  const message = String(error?.message || '');
  if (/aborted due to timeout|timeout.*POST|POST.*timeout/iu.test(message)) return 'webdriver_client_request_timeout';
  if (/ECONNREFUSED|connection refused/iu.test(message)) return 'appium_connection_refused';
  return 'webdriver_session_error';
}

export async function buildAppiumSessionFailureReceipt({ error, platform, startedAt,
  endedAt = Date.now(), logPath, appiumServerReady = false, startupAttempt = 1 } = {}) {
  let log = { source: '', truncated: false };
  if (logPath) log = await readBoundedLogTail(logPath);
  const attempt = [1, 2].includes(Number(startupAttempt)) ? Number(startupAttempt) : 0;
  return {
    schema_version: 'mobile-preflight-failure-v1',
    platform: ['android', 'ios'].includes(platform) ? platform : 'unknown',
    failure_stage: 'webdriver_session_create',
    auth_callback_started: false,
    webdriver_client_session_created: false,
    appium_server_ready: appiumServerReady === true,
    startup_attempt: attempt,
    elapsed_ms: Math.max(0, Math.round(Number(endedAt) - Number(startedAt))),
    error_class: closedErrorClass(error),
    cleanup_confirmed: true,
    retry_safe: attempt === 1,
    side_effects: buildZeroSideEffectReceipt({ attempt, driverSessionCreated: false,
      driverSessionDeleted: false }),
    ...(platform === 'android'
      ? summarizeAndroidAppiumSessionStartup(log.source, { truncated: log.truncated })
      : summarizeAppiumSessionStartup(log.source, { truncated: log.truncated })),
  };
}
