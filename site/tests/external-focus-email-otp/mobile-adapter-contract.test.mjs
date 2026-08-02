import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';
import { parse } from 'yaml';

import { buildAppiumCapabilities, extractDriverNetworkEvents } from '../../e2e/focus-email/adapters/appium-ui.mjs';

test('Appium capability builder pins real mobile browsers and iOS keyboard ownership', () => {
  const ios = buildAppiumCapabilities('ios', { deviceName: 'iPhone 16', platformVersion: '18.5', udid: 'opaque' },
    { E2E_PREBUILT_WDA_PATH: '/safe/wda.app' });
  assert.deepEqual({ platformName: ios.platformName, browserName: ios.browserName,
    automationName: ios['appium:automationName'], hardware: ios['appium:connectHardwareKeyboard'],
    software: ios['appium:forceSimulatorSoftwareKeyboardPresence'], udid: ios['appium:udid'] },
  { platformName: 'iOS', browserName: 'Safari', automationName: 'XCUITest', hardware: false, software: true, udid: 'opaque' });
  assert.equal(ios['appium:usePreinstalledWDA'], true);
  assert.equal(ios['appium:prebuiltWDAPath'], '/safe/wda.app');

  const android = buildAppiumCapabilities('android', { deviceName: 'Pixel 7', platformVersion: '15' }, {});
  assert.equal(android.browserName, 'Chrome');
  assert.equal(android['appium:automationName'], 'UiAutomator2');
});

test('adapter excludes unsafe hierarchy reads, JS value injection and desktop keyboard rescue', async () => {
  const source = await readFile(new URL('../../e2e/focus-email/adapters/appium-ui.mjs', import.meta.url), 'utf8');
  assert.doesNotMatch(source, /getPageSource|pageSource|input\.value\s*=\s*value|osascript|Cmd-K|Toggle Software Keyboard/u);
});

test('immutable preview metadata records the full repository SHA used by the E2E gate', async () => {
  const source = await readFile(new URL('../../scripts/build-preview.mjs', import.meta.url), 'utf8');
  assert.match(source, /repo_sha: gitFullSha\(\)/u);
});

test('Appium network logs retain sanitized request identity and failure class', () => {
  const logs = [
    { message: JSON.stringify({ message: { method: 'Network.requestWillBeSent', params: { requestId: 'otp-1',
      request: { method: 'POST', url: 'https://example.supabase.co/auth/v1/otp?email=secret@example.test' } } } }) },
    { message: JSON.stringify({ method: 'Network.responseReceived', params: { requestId: 'otp-1',
      response: { status: 200, url: 'https://example.supabase.co/auth/v1/otp?email=secret@example.test' } } }) },
    { message: JSON.stringify({ method: 'Network.loadingFailed', params: { requestId: 'health-2', canceled: true, errorText: 'raw details' } }) },
  ];
  const events = extractDriverNetworkEvents(logs);
  assert.deepEqual(events.at(-1), { type: 'failure', request_id: 'health-2', method: null,
    hostname: null, path: null, status: null, failure_class: 'request_cancelled' });
  assert.doesNotMatch(JSON.stringify(events), /secret|@|raw details/u);
});

test('protected workflow serializes real-mail jobs and never reruns a full journey as fallback', async () => {
  const source = await readFile(new URL('../../../.github/workflows/external-focus-email-otp.yml', import.meta.url), 'utf8');
  const workflow = parse(source);
  assert.deepEqual(workflow.jobs.android.needs, ['validate', 'browser']);
  assert.deepEqual(workflow.jobs.ios.needs, ['validate', 'android']);
  assert.equal(workflow.jobs.ios['runs-on'], 'macos-15');
  assert.doesNotMatch(source, /simctl boot |open -a Simulator|defaults write com\.apple\.iphonesimulator|tail -n|\.redaction-ok" \|\| npm run e2e/u);
  assert.match(source, /write-static-otp-blocked-evidence\.mjs/u);
});

test('QA gateway downloads redaction-gated summaries and forwards the closed scenario', async () => {
  const source = await readFile(new URL('../../../.github/workflows/static-site-qa-command.yml', import.meta.url), 'utf8');
  const workflow = parse(source);
  assert.equal(workflow.jobs.run.secrets, 'inherit');
  assert.equal(workflow.jobs.run.with.scenario, '${{ needs.prepare.outputs.scenario }}');
  assert.match(source, /actions\/download-artifact@d3f86a106a0bac45b974a628896c90dbdf5c8093/u);
  assert.match(source, /static-site-qa-terminal\.mjs/u);
});
