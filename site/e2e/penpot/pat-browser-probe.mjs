import process from 'node:process';
import { execFile } from 'node:child_process';
import { mkdir, writeFile } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import { promisify } from 'node:util';

import WebSocket from 'ws';

const execFileAsync = promisify(execFile);
const PROFILE_PATH = '/api/rpc/command/get-profile';

const required = (name) => {
  const value = String(process.env[name] || '').trim();
  if (!value) throw new Error(`missing_configuration:${name}`);
  return value;
};

const token = required('PENPOT_INTEGRATION_TOKEN');
const baseUrl = String(process.env.PENPOT_BASE_URL || 'https://design.penpot.app').replace(/\/$/u, '');
const endpointUrl = `${baseUrl}${PROFILE_PATH}`;
const evidencePath = resolve(process.env.PENPOT_ANDROID_EVIDENCE_PATH || 'artifacts/penpot-android-browser/result.json');
const startedAt = Date.now();

if (process.env.GITHUB_ACTIONS === 'true') process.stdout.write(`::add-mask::${token}\n`);

const sleep = (ms) => new Promise((resolvePromise) => setTimeout(resolvePromise, ms));
const safeError = (error) => String(error?.message || error || 'unknown_error')
  .replaceAll(token, '[REDACTED]')
  .slice(0, 500);

class ScopedHeaderInjector {
  constructor(webSocketUrl, targetOrigin) {
    this.webSocketUrl = webSocketUrl;
    this.targetOrigin = targetOrigin;
    this.authorization = null;
    this.socket = null;
    this.sequence = 0;
    this.pending = new Map();
    this.interceptedCount = 0;
  }

  async connect() {
    this.socket = new WebSocket(this.webSocketUrl);
    await new Promise((resolvePromise, rejectPromise) => {
      const timer = setTimeout(() => rejectPromise(new Error('cdp_websocket_connect_timeout')), 15_000);
      this.socket.once('open', () => {
        clearTimeout(timer);
        resolvePromise();
      });
      this.socket.once('error', (error) => {
        clearTimeout(timer);
        rejectPromise(error);
      });
    });

    this.socket.on('message', (raw) => {
      let message;
      try {
        message = JSON.parse(String(raw));
      } catch {
        return;
      }

      if (message.id && this.pending.has(message.id)) {
        const pending = this.pending.get(message.id);
        this.pending.delete(message.id);
        if (message.error) pending.reject(new Error(`cdp_error:${message.error.message || 'unknown'}`));
        else pending.resolve(message.result || {});
        return;
      }

      if (message.method === 'Fetch.requestPaused') {
        void this.continuePausedRequest(message.params).catch(() => undefined);
      }
    });

    await this.send('Network.enable');
    await this.send('Network.setCacheDisabled', { cacheDisabled: true });
    await this.send('Fetch.enable', {
      patterns: [{
        urlPattern: `${this.targetOrigin}${PROFILE_PATH}*`,
        requestStage: 'Request',
      }],
    });
  }

  setAuthorization(value) {
    this.authorization = value || null;
  }

  async continuePausedRequest(params) {
    const requestUrl = new URL(params.request.url);
    const exactTarget = requestUrl.origin === this.targetOrigin && requestUrl.pathname === PROFILE_PATH;
    const headers = Object.entries(params.request.headers || {})
      .filter(([name]) => !['authorization', 'accept', 'cache-control'].includes(name.toLowerCase()))
      .map(([name, value]) => ({ name, value: String(value) }));

    headers.push({ name: 'Accept', value: '*/*' });
    headers.push({ name: 'Cache-Control', value: 'no-cache' });
    if (exactTarget && this.authorization) {
      headers.push({ name: 'Authorization', value: `Token ${this.authorization}` });
    }
    if (exactTarget) this.interceptedCount += 1;

    await this.send('Fetch.continueRequest', {
      requestId: params.requestId,
      headers,
    });
  }

  send(method, params = {}) {
    const id = ++this.sequence;
    return new Promise((resolvePromise, rejectPromise) => {
      const timer = setTimeout(() => {
        this.pending.delete(id);
        rejectPromise(new Error(`cdp_command_timeout:${method}`));
      }, 15_000);
      this.pending.set(id, {
        resolve: (value) => {
          clearTimeout(timer);
          resolvePromise(value);
        },
        reject: (error) => {
          clearTimeout(timer);
          rejectPromise(error);
        },
      });
      this.socket.send(JSON.stringify({ id, method, params }));
    });
  }

  async close() {
    if (!this.socket) return;
    await this.send('Fetch.disable').catch(() => undefined);
    this.socket.close();
  }
}

async function findChromePageTarget() {
  await execFileAsync('adb', ['forward', 'tcp:9222', 'localabstract:chrome_devtools_remote']);
  const deadline = Date.now() + 30_000;
  let lastTargets = [];

  while (Date.now() < deadline) {
    try {
      const response = await fetch('http://127.0.0.1:9222/json/list', { cache: 'no-store' });
      if (response.ok) {
        lastTargets = await response.json();
        const exact = lastTargets.find((target) => target.type === 'page'
          && String(target.url || '').startsWith(baseUrl));
        const page = exact || lastTargets.find((target) => target.type === 'page');
        if (page?.webSocketDebuggerUrl) return page;
      }
    } catch {
      // Chrome may not have exposed its DevTools socket yet.
    }
    await sleep(500);
  }

  throw new Error(`chrome_cdp_target_unavailable:targets=${lastTargets.length}`);
}

async function readPageState(driver) {
  return driver.execute(() => {
    const html = document.documentElement?.innerHTML || '';
    const text = document.body?.innerText || document.documentElement?.innerText || '';
    const title = document.title || '';
    const challenge = /just a moment/i.test(title)
      || /challenges\.cloudflare\.com/iu.test(html)
      || /cf-chl-/iu.test(html);
    const profileDetected = !challenge && (
      text.includes('~:default-team-id')
      || text.includes('~:auth-backend')
      || (text.includes('~:email') && text.includes('~:fullname'))
    );
    const navigation = performance.getEntriesByType('navigation').at(-1);
    const responseStatus = Number(navigation?.responseStatus || 0);
    return {
      href: location.href,
      title: title.slice(0, 160),
      readyState: document.readyState,
      contentType: document.contentType || '',
      challenge,
      profileDetected,
      bodyLength: text.length,
      responseStatus: responseStatus > 0 ? responseStatus : null,
    };
  });
}

async function waitForSettledPage(driver, timeoutMs = 75_000) {
  const deadline = Date.now() + timeoutMs;
  let challengeSeen = false;
  let stableReads = 0;
  let state = null;

  while (Date.now() < deadline) {
    state = await readPageState(driver);
    if (state.challenge) {
      challengeSeen = true;
      stableReads = 0;
    } else if (state.readyState === 'complete') {
      stableReads += 1;
      if (stableReads >= 2) break;
    } else {
      stableReads = 0;
    }
    await sleep(1_500);
  }

  return {
    challengeSeen,
    challengeCleared: Boolean(state && !state.challenge && stableReads >= 2),
    final: state,
  };
}

const result = {
  schemaVersion: 2,
  status: 'FAIL',
  platform: 'android-chrome-appium',
  targetOrigin: new URL(baseUrl).origin,
  startedAt: new Date(startedAt).toISOString(),
  browser: null,
  root: null,
  navigationProbes: null,
  cdp: null,
  failure: null,
};

let driver;
let injector;
try {
  const { remote } = await import('webdriverio');
  driver = await remote({
    hostname: '127.0.0.1',
    port: 4723,
    path: '/wd/hub',
    logLevel: 'error',
    connectionRetryTimeout: 120_000,
    connectionRetryCount: 0,
    capabilities: {
      platformName: 'Android',
      browserName: 'Chrome',
      pageLoadStrategy: 'none',
      'wdio:enforceWebDriverClassic': true,
      'appium:automationName': 'UiAutomator2',
      'appium:deviceName': String(process.env.PENPOT_ANDROID_DEVICE_NAME || 'Pixel 7'),
      'appium:platformVersion': String(process.env.PENPOT_ANDROID_PLATFORM_VERSION || '15'),
      'appium:newCommandTimeout': 180,
      'appium:language': 'en',
      'appium:locale': 'US',
      'goog:chromeOptions': {
        args: ['--no-first-run', '--disable-fre', '--disable-notifications'],
      },
    },
  });

  const capabilities = driver.capabilities || {};
  result.browser = {
    browserName: String(capabilities.browserName || 'Chrome'),
    browserVersion: String(capabilities.browserVersion || capabilities.version || 'unreported'),
    platformName: String(capabilities.platformName || 'Android'),
    platformVersion: String(capabilities.platformVersion || 'unreported'),
    deviceName: String(capabilities.deviceName || process.env.PENPOT_ANDROID_DEVICE_NAME || 'Pixel 7'),
  };

  await driver.url(`${baseUrl}/`);
  result.root = await waitForSettledPage(driver, 45_000);
  if (!result.root.challengeCleared) throw new Error('penpot_root_did_not_settle');

  const target = await findChromePageTarget();
  injector = new ScopedHeaderInjector(target.webSocketDebuggerUrl, new URL(baseUrl).origin);
  await injector.connect();
  result.cdp = {
    targetType: String(target.type || 'unknown'),
    scopedPath: PROFILE_PATH,
    interceptedRequestCount: 0,
  };

  const navigateProbe = async (label, authorization) => {
    injector.setAuthorization(authorization);
    await driver.url(endpointUrl);
    const settled = await waitForSettledPage(driver, 75_000);
    return {
      label,
      challengeSeen: settled.challengeSeen,
      challengeCleared: settled.challengeCleared,
      final: settled.final,
    };
  };

  const noAuthorization = await navigateProbe('no-authorization', null);
  const invalidToken = await navigateProbe('invalid-token', 'deliberately-invalid-token');
  const real = await navigateProbe('real-token', token);
  result.navigationProbes = { noAuthorization, invalidToken, real };
  result.cdp.interceptedRequestCount = injector.interceptedCount;

  const controlClean = noAuthorization.challengeCleared
    && !noAuthorization.final?.profileDetected
    && invalidToken.challengeCleared
    && !invalidToken.final?.profileDetected;
  const realWorks = real.challengeCleared && real.final?.profileDetected;

  if (!controlClean) throw new Error('navigation_control_probes_not_clean');
  if (!realWorks) throw new Error('real_pat_navigation_did_not_return_penpot_profile');

  result.status = 'PASS';
} catch (error) {
  result.failure = safeError(error);
} finally {
  if (injector) await injector.close().catch(() => undefined);
  if (driver) await driver.deleteSession().catch(() => undefined);
  await execFileAsync('adb', ['forward', '--remove', 'tcp:9222']).catch(() => undefined);
  result.finishedAt = new Date().toISOString();
  result.durationMs = Date.now() - startedAt;
  await mkdir(dirname(evidencePath), { recursive: true });
  await writeFile(evidencePath, `${JSON.stringify(result, null, 2)}\n`, 'utf8');
}

const summary = {
  status: result.status,
  rootChallengeSeen: result.root?.challengeSeen ?? null,
  noAuthorizationChallengeCleared: result.navigationProbes?.noAuthorization?.challengeCleared ?? null,
  invalidChallengeCleared: result.navigationProbes?.invalidToken?.challengeCleared ?? null,
  realChallengeCleared: result.navigationProbes?.real?.challengeCleared ?? null,
  realProfileDetected: result.navigationProbes?.real?.final?.profileDetected ?? false,
  interceptedRequestCount: result.cdp?.interceptedRequestCount ?? 0,
  failure: result.failure,
  evidencePath,
};
process.stdout.write(`${JSON.stringify(summary, null, 2)}\n`);

if (result.status !== 'PASS') process.exitCode = 1;
