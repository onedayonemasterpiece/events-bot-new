import process from 'node:process';
import { mkdir, writeFile } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';

const required = (name) => {
  const value = String(process.env[name] || '').trim();
  if (!value) throw new Error(`missing_configuration:${name}`);
  return value;
};

const token = required('PENPOT_INTEGRATION_TOKEN');
const baseUrl = String(process.env.PENPOT_BASE_URL || 'https://design.penpot.app').replace(/\/$/u, '');
const evidencePath = resolve(process.env.PENPOT_ANDROID_EVIDENCE_PATH || 'artifacts/penpot-android-browser/result.json');
const startedAt = Date.now();

if (process.env.GITHUB_ACTIONS === 'true') {
  process.stdout.write(`::add-mask::${token}\n`);
}

const sleep = (ms) => new Promise((resolvePromise) => setTimeout(resolvePromise, ms));
const safeError = (error) => String(error?.message || error || 'unknown_error')
  .replaceAll(token, '[REDACTED]')
  .slice(0, 500);

const result = {
  schemaVersion: 1,
  status: 'FAIL',
  platform: 'android-chrome-appium',
  targetOrigin: new URL(baseUrl).origin,
  startedAt: new Date(startedAt).toISOString(),
  challenge: {
    seen: false,
    passed: false,
  },
  browser: null,
  probes: null,
  failure: null,
};

let driver;
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

  let stableNonChallengeReads = 0;
  let lastState = null;
  const deadline = Date.now() + 90_000;

  while (Date.now() < deadline) {
    lastState = await driver.execute(() => {
      const html = document.documentElement?.innerHTML || '';
      const title = document.title || '';
      const challenge = /just a moment/i.test(title)
        || /challenges\.cloudflare\.com/iu.test(html)
        || /cf-chl-/iu.test(html);
      return {
        href: location.href,
        title: title.slice(0, 160),
        readyState: document.readyState,
        challenge,
      };
    });

    if (lastState?.challenge) {
      result.challenge.seen = true;
      stableNonChallengeReads = 0;
    } else if (lastState?.readyState === 'complete'
      && new URL(lastState.href).origin === new URL(baseUrl).origin) {
      stableNonChallengeReads += 1;
      if (stableNonChallengeReads >= 2) break;
    }

    await sleep(2_000);
  }

  result.challenge.finalPage = {
    url: lastState?.href || null,
    title: lastState?.title || null,
    readyState: lastState?.readyState || null,
    challengeDetected: Boolean(lastState?.challenge),
  };
  result.challenge.passed = Boolean(lastState && !lastState.challenge
    && new URL(lastState.href).origin === new URL(baseUrl).origin
    && stableNonChallengeReads >= 2);

  if (!result.challenge.passed) {
    throw new Error('cloudflare_challenge_not_cleared_in_android_chrome');
  }

  const probes = await driver.executeAsync((realToken, done) => {
    const endpoint = '/api/rpc/command/get-profile';

    const classify = async (label, authorization) => {
      const headers = { Accept: '*/*' };
      if (authorization) headers.Authorization = `Token ${authorization}`;

      const response = await fetch(endpoint, {
        method: 'GET',
        headers,
        credentials: 'include',
        cache: 'no-store',
      });
      const body = await response.text();
      const contentType = response.headers.get('content-type') || '';
      const html = /text\/html/iu.test(contentType)
        || /^\s*<!doctype html/iu.test(body)
        || /^\s*<html/iu.test(body);
      const cloudflareChallenge = response.headers.get('cf-mitigated') === 'challenge'
        || /<title>\s*Just a moment\.\.\.<\/title>/iu.test(body)
        || /challenges\.cloudflare\.com/iu.test(body);
      const profileDetected = !html && (
        body.includes('~:default-team-id')
        || body.includes('~:auth-backend')
        || (body.includes('~:email') && body.includes('~:fullname'))
      );

      return {
        label,
        status: response.status,
        ok: response.ok,
        contentType,
        html,
        cloudflareChallenge,
        profileDetected,
        bodyLength: body.length,
      };
    };

    (async () => {
      const noAuthorization = await classify('no-authorization', null);
      const invalidToken = await classify('invalid-token', 'deliberately-invalid-token');
      const real = await classify('real-token', realToken);
      done({ noAuthorization, invalidToken, real });
    })().catch((error) => done({
      executionError: String(error?.message || error || 'browser_fetch_failed').slice(0, 300),
    }));
  }, token);

  if (probes?.executionError) {
    throw new Error(`browser_fetch_execution_failed:${probes.executionError}`);
  }

  result.probes = probes;

  const noAuthIsClean = probes.noAuthorization
    && !probes.noAuthorization.profileDetected
    && !probes.noAuthorization.cloudflareChallenge;
  const invalidIsClean = probes.invalidToken
    && !probes.invalidToken.profileDetected
    && !probes.invalidToken.cloudflareChallenge;
  const realWorks = probes.real
    && probes.real.status === 200
    && probes.real.profileDetected
    && !probes.real.html
    && !probes.real.cloudflareChallenge;

  if (!noAuthIsClean) throw new Error('control_probe_no_authorization_is_not_clean');
  if (!invalidIsClean) throw new Error('control_probe_invalid_token_is_not_clean');
  if (!realWorks) throw new Error('real_pat_did_not_return_penpot_profile');

  result.status = 'PASS';
} catch (error) {
  result.failure = safeError(error);
} finally {
  if (driver) await driver.deleteSession().catch(() => undefined);
  result.finishedAt = new Date().toISOString();
  result.durationMs = Date.now() - startedAt;
  await mkdir(dirname(evidencePath), { recursive: true });
  await writeFile(evidencePath, `${JSON.stringify(result, null, 2)}\n`, 'utf8');
}

const summary = {
  status: result.status,
  challengeSeen: result.challenge.seen,
  challengePassed: result.challenge.passed,
  noAuthorizationStatus: result.probes?.noAuthorization?.status ?? null,
  invalidTokenStatus: result.probes?.invalidToken?.status ?? null,
  realTokenStatus: result.probes?.real?.status ?? null,
  realProfileDetected: result.probes?.real?.profileDetected ?? false,
  failure: result.failure,
  evidencePath,
};
process.stdout.write(`${JSON.stringify(summary, null, 2)}\n`);

if (result.status !== 'PASS') process.exitCode = 1;
