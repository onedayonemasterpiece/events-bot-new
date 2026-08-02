import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

import { extractDriverNetworkEvents } from '../../e2e/focus-email/adapters/appium-ui.mjs';

test('Appium adapter uses real browser capabilities and ordinary digit input without raw native hierarchy', async () => {
  const source = await readFile(new URL('../../e2e/focus-email/adapters/appium-ui.mjs', import.meta.url), 'utf8');
  assert.match(source, /browserName: 'Chrome'/u);
  assert.match(source, /browserName: 'Safari'/u);
  assert.match(source, /UiAutomator2/u);
  assert.match(source, /XCUITest/u);
  assert.equal((source.match(/'wdio:enforceWebDriverClassic': true/gu) || []).length, 2);
  assert.match(source, /connectionRetryTimeout: platform === 'ios' \? 360_000 : 120_000/u);
  assert.match(source, /connectionRetryCount: 0/u);
  assert.match(source, /for \(const digit of value\).*addValue\(digit\)/su);
  assert.doesNotMatch(source, /getPageSource|pageSource|input\.value\s*=\s*value/u);
});

test('immutable preview metadata records the full repository SHA used by the E2E gate', async () => {
  const source = await readFile(new URL('../../scripts/build-preview.mjs', import.meta.url), 'utf8');
  assert.match(source, /repo_sha: gitFullSha\(\)/u);
  assert.match(source, /git.*rev-parse.*HEAD/su);
});

test('Appium network logs retain only request identity, host, path and status', () => {
  const logs = [
    { message: JSON.stringify({ message: { method: 'Network.requestWillBeSent', params: { requestId: 'otp-1',
      request: { method: 'POST', url: 'https://example.supabase.co/auth/v1/otp?email=secret@example.test' } } } }) },
    { message: JSON.stringify({ method: 'Network.responseReceived', params: { requestId: 'otp-1',
      response: { status: 200, url: 'https://example.supabase.co/auth/v1/otp?email=secret@example.test' } } }) },
  ];
  assert.deepEqual(extractDriverNetworkEvents(logs), [
    { type: 'request', request_id: 'otp-1', method: 'POST', hostname: 'example.supabase.co', path: '/auth/v1/otp', status: null },
    { type: 'response', request_id: 'otp-1', method: 'GET', hostname: 'example.supabase.co', path: '/auth/v1/otp', status: 200 },
  ]);
  assert.doesNotMatch(JSON.stringify(extractDriverNetworkEvents(logs)), /secret|@/u);
});

test('protected workflow keeps the recipient secret and gates all platforms strictly in sequence', async () => {
  const source = await readFile(new URL('../../../.github/workflows/external-focus-email-otp.yml', import.meta.url), 'utf8');
  assert.equal((source.match(/E2E_RECIPIENT_TEMPLATE: \$\{\{ secrets\.E2E_RECIPIENT_TEMPLATE \}\}/gu) || []).length, 3);
  assert.doesNotMatch(source, /vars\.E2E_RECIPIENT_TEMPLATE/u);
  assert.match(source, /inputs\.platform == 'all' && needs\.browser\.result == 'success'/u);
  assert.match(source, /inputs\.platform == 'all' && needs\.android\.result == 'success'/u);
  assert.match(source, /sudo chmod 0666 \/dev\/kvm/u);
  assert.match(source, /test -r \/dev\/kvm && test -w \/dev\/kvm/u);
  assert.equal((source.match(/node-version: '22\.22\.0'/gu) || []).length, 3);
  assert.match(source, /working-directory: site/u);
  assert.match(source, /--allow-insecure uiautomator2:chromedriver_autodownload/u);
});
