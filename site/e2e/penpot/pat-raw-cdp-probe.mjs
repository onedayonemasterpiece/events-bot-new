import process from 'node:process';
import { execFile } from 'node:child_process';
import { mkdir, writeFile } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import { promisify } from 'node:util';

import WebSocket from 'ws';

const execFileAsync = promisify(execFile);
const PROFILE_PATH = '/api/rpc/command/get-profile';
const token = String(process.env.PENPOT_INTEGRATION_TOKEN || '').trim();
if (!token) throw new Error('missing_configuration:PENPOT_INTEGRATION_TOKEN');

const baseUrl = String(process.env.PENPOT_BASE_URL || 'https://design.penpot.app').replace(/\/$/u, '');
const targetOrigin = new URL(baseUrl).origin;
const endpointUrl = `${baseUrl}${PROFILE_PATH}`;
const evidencePath = resolve(process.env.PENPOT_ANDROID_RAW_EVIDENCE_PATH || 'artifacts/penpot-android-raw/result.json');
const startedAt = Date.now();

if (process.env.GITHUB_ACTIONS === 'true') process.stdout.write(`::add-mask::${token}\n`);

const sleep = (ms) => new Promise((resolvePromise) => setTimeout(resolvePromise, ms));
const safeError = (error) => String(error?.message || error || 'unknown_error')
  .replaceAll(token, '[REDACTED]')
  .slice(0, 500);

class CdpClient {
  constructor(webSocketUrl) {
    this.webSocketUrl = webSocketUrl;
    this.socket = null;
    this.sequence = 0;
    this.pending = new Map();
    this.authorization = null;
    this.interceptedRequestCount = 0;
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

    await this.send('Page.enable');
    await this.send('Runtime.enable');
    await this.send('Network.enable');
    await this.send('Network.setCacheDisabled', { cacheDisabled: true });
    await this.send('Fetch.enable', {
      patterns: [{
        urlPattern: `${targetOrigin}${PROFILE_PATH}*`,
        requestStage: 'Request',
      }],
    });
  }

  setAuthorization(value) {
    this.authorization = value || null;
  }

  async continuePausedRequest(params) {
    const requestUrl = new URL(params.request.url);
    const exactTarget = requestUrl.origin === targetOrigin && requestUrl.pathname === PROFILE_PATH;
    const headers = Object.entries(params.request.headers || {})
      .filter(([name]) => !['authorization', 'accept', 'cache-control'].includes(name.toLowerCase()))
      .map(([name, value]) => ({ name, value: String(value) }));

    headers.push({ name: 'Accept', value: '*/*' });
    headers.push({ name: 'Cache-Control', value: 'no-cache' });
    if (exactTarget && this.authorization) {
      headers.push({ name: 'Authorization', value: `Token ${this.authorization}` });
    }
    if (exactTarget) this.interceptedRequestCount += 1;

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

  async evaluate(expression) {
    const response = await this.send('Runtime.evaluate', {
      expression,
      returnByValue: true,
      awaitPromise: true,
    });
    if (response.exceptionDetails) throw new Error('runtime_evaluate_exception');
    return response.result?.value;
  }

  async navigate(url) {
    return this.send('Page.navigate', { url, transitionType: 'typed' });
  }

  async close() {
    if (!this.socket) return;
    await this.send('Fetch.disable').catch(() => undefined);
    this.socket.close();
  }
}

async function startChrome() {
  const commandLine = 'chrome --no-first-run --disable-fre --disable-default-apps --disable-notifications';
  await execFileAsync('adb', ['shell', 'sh', '-c', `echo '${commandLine}' '>' /data/local/tmp/chrome-command-line`])
    .catch(() => undefined);
  await execFileAsync('adb', ['shell', 'am', 'force-stop', 'com.android.chrome']).catch(() => undefined);
  await execFileAsync('adb', [
    'shell', 'am', 'start',
    '-a', 'android.intent.action.VIEW',
    '-d', `${baseUrl}/`,
    '-p', 'com.android.chrome',
  ]);
  await execFileAsync('adb', ['forward', 'tcp:9222', 'localabstract:chrome_devtools_remote']);
}

async function findPageTarget() {
  const deadline = Date.now() + 45_000;
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
      // Chrome may still be starting.
    }
    await sleep(500);
  }
  throw new Error(`raw_chrome_cdp_target_unavailable:targets=${lastTargets.length}`);
}

const STATE_EXPRESSION = `(() => {
  const html = document.documentElement?.innerHTML || '';
  const text = document.body?.innerText || document.documentElement?.innerText || '';
  const title = document.title || '';
  const challenge = /just a moment/i.test(title)
    || /challenges\\.cloudflare\\.com/iu.test(html)
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
    navigatorWebdriver: navigator.webdriver === true,
  };
})()`;

async function waitForSettledPage(client, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  let state = null;
  let challengeSeen = false;
  let stableReads = 0;

  while (Date.now() < deadline) {
    try {
      state = await client.evaluate(STATE_EXPRESSION);
    } catch {
      await sleep(500);
      continue;
    }

    if (state?.challenge) {
      challengeSeen = true;
      stableReads = 0;
    } else if (state?.readyState === 'complete') {
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
  schemaVersion: 1,
  status: 'FAIL',
  platform: 'android-chrome-raw-cdp',
  targetOrigin,
  startedAt: new Date(startedAt).toISOString(),
  root: null,
  navigationProbes: null,
  cdp: null,
  failure: null,
};

let client;
try {
  await startChrome();
  const target = await findPageTarget();
  client = new CdpClient(target.webSocketDebuggerUrl);
  await client.connect();

  result.cdp = {
    targetType: String(target.type || 'unknown'),
    initialTargetUrl: String(target.url || '').slice(0, 300),
    scopedPath: PROFILE_PATH,
    interceptedRequestCount: 0,
  };

  result.root = await waitForSettledPage(client, 45_000);
  if (!result.root.challengeCleared) throw new Error('raw_chrome_root_did_not_settle');

  const navigateProbe = async (label, authorization) => {
    client.setAuthorization(authorization);
    await client.navigate(endpointUrl);
    const settled = await waitForSettledPage(client, 75_000);
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
  result.cdp.interceptedRequestCount = client.interceptedRequestCount;

  const controlsClean = noAuthorization.challengeCleared
    && !noAuthorization.final?.profileDetected
    && invalidToken.challengeCleared
    && !invalidToken.final?.profileDetected;
  const realWorks = real.challengeCleared && real.final?.profileDetected;

  if (!controlsClean) throw new Error('raw_navigation_control_probes_not_clean');
  if (!realWorks) throw new Error('raw_real_pat_navigation_did_not_return_penpot_profile');
  result.status = 'PASS';
} catch (error) {
  result.failure = safeError(error);
} finally {
  if (client) await client.close().catch(() => undefined);
  await execFileAsync('adb', ['forward', '--remove', 'tcp:9222']).catch(() => undefined);
  await execFileAsync('adb', ['shell', 'am', 'force-stop', 'com.android.chrome']).catch(() => undefined);
  result.finishedAt = new Date().toISOString();
  result.durationMs = Date.now() - startedAt;
  await mkdir(dirname(evidencePath), { recursive: true });
  await writeFile(evidencePath, `${JSON.stringify(result, null, 2)}\n`, 'utf8');
}

process.stdout.write(`${JSON.stringify({
  status: result.status,
  rootNavigatorWebdriver: result.root?.final?.navigatorWebdriver ?? null,
  realChallengeCleared: result.navigationProbes?.real?.challengeCleared ?? null,
  realProfileDetected: result.navigationProbes?.real?.final?.profileDetected ?? false,
  interceptedRequestCount: result.cdp?.interceptedRequestCount ?? 0,
  failure: result.failure,
  evidencePath,
}, null, 2)}\n`);

if (result.status !== 'PASS') process.exitCode = 1;
