import assert from 'node:assert/strict';
import { mkdtemp, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';

import { buildAppiumSessionFailureReceipt,
  summarizeAppiumSessionStartup } from '../e2e/mobile-web/appium-startup-receipt.mjs';
import { isSafeMobilePreflightRetryReceipt } from '../e2e/mobile-web/appium-preflight.mjs';
import { closedMobileStartupRetryReceipt,
  isRetryableMobileStartupResult,
  readPriorMobileStartupReceipt } from '../e2e/search/mobile-startup-retry.mjs';

test('Appium startup receipt reduces raw XCUITest log to closed phase booleans', () => {
  const raw = [
    "secret-bearing capability marker must-not-survive",
    "[XCUITestDriver] Event 'simStarted' logged at 1",
    "[XCUITestDriver] Installing prebuilt WDA at '/private/path/WebDriverAgentRunner.app'. Bundle identifier: hidden",
    "[XCUITestDriver] Event 'wdaStartAttempted' logged at 2",
    "[XCUITestDriver] Event 'wdaSessionAttempted' logged at 3",
    "[XCUITestDriver] Event 'wdaSessionStarted' logged at 4",
    "[XCUITestDriver] Event 'wdaStarted' logged at 5",
  ].join('\n');
  const receipt = summarizeAppiumSessionStartup(raw);
  assert.deepEqual(receipt, {
    log_inspected: true,
    log_truncated: false,
    simulator_started: true,
    wda_install_attempted: true,
    wda_start_attempted: true,
    wda_start_failed: false,
    wda_session_attempted: true,
    wda_session_started: true,
    wda_session_failed: false,
    wda_started: true,
  });
  assert.equal(JSON.stringify(receipt).includes('must-not-survive'), false);
  assert.equal(JSON.stringify(receipt).includes('/private/path'), false);
  const truncated = summarizeAppiumSessionStartup(
    "Event 'wdaSessionStarted'\nEvent 'wdaStarted'\n" + 'x'.repeat(2_000_001),
  );
  assert.equal(truncated.log_truncated, true);
  assert.equal(truncated.wda_session_started, false);
  assert.equal(truncated.wda_started, false);
});

test('session-create failure receipt exposes only retry-safe bounded metadata', async () => {
  const receipt = await buildAppiumSessionFailureReceipt({
    error: new Error('The operation was aborted due to timeout while posting a secret URL'),
    platform: 'ios', startedAt: 1_000, endedAt: 301_000,
    appiumServerReady: true, startupAttempt: 2,
  });
  assert.equal(receipt.failure_stage, 'webdriver_session_create');
  assert.equal(receipt.auth_callback_started, false);
  assert.equal(receipt.webdriver_client_session_created, false);
  assert.equal(receipt.appium_server_ready, true);
  assert.equal(receipt.elapsed_ms, 300_000);
  assert.equal(receipt.startup_attempt, 2);
  assert.equal(receipt.error_class, 'webdriver_client_request_timeout');
  assert.equal(receipt.schema_version, 'mobile-preflight-failure-v1');
  assert.equal(receipt.cleanup_confirmed, true);
  assert.equal(receipt.retry_safe, false);
  assert.equal(receipt.side_effects.broker_session_issued, false);
  assert.equal(JSON.stringify(receipt).includes('secret URL'), false);
});

test('attempt-one session creation failure has the explicit zero-side-effect retry proof', async () => {
  const receipt = await buildAppiumSessionFailureReceipt({
    error: new Error('connection refused raw endpoint'), platform: 'android',
    startedAt: 1_000, endedAt: 1_500, appiumServerReady: true, startupAttempt: 1,
  });
  assert.equal(receipt.retry_safe, true);
  assert.equal(receipt.side_effects.webdriver_client_session_created, false);
  assert.equal(isSafeMobilePreflightRetryReceipt(receipt), true);
  assert.doesNotMatch(JSON.stringify(receipt), /raw endpoint/u);
});

test('only an iOS session-creation failure before callback and Search traffic is retryable', () => {
  const base = {
    status: 'FAIL', platform: 'ios', error_code: 'WebDriverError', counters: {}, query_cases: [],
    failure_receipt: {
      failure_stage: 'webdriver_session_create', auth_callback_started: false,
      webdriver_client_session_created: false, appium_server_ready: true,
      error_class: 'webdriver_client_request_timeout', startup_attempt: 1,
      elapsed_ms: 300_000, log_inspected: true, log_truncated: false, simulator_started: true,
      wda_install_attempted: true, wda_start_attempted: true,
      wda_start_failed: false, wda_session_attempted: false,
      wda_session_started: false, wda_session_failed: false, wda_started: false,
    },
  };
  assert.equal(isRetryableMobileStartupResult(base), true);
  assert.equal(isRetryableMobileStartupResult({ ...base, platform: 'android' }), false);
  assert.equal(isRetryableMobileStartupResult({ ...base, query_cases: [{ query_id: 'already_started' }] }), false);
  assert.equal(isRetryableMobileStartupResult({ ...base, counters: { requests: 1 } }), false);
  assert.equal(isRetryableMobileStartupResult({ ...base,
    failure_receipt: { ...base.failure_receipt, auth_callback_started: true } }), false);
  assert.equal(isRetryableMobileStartupResult({ ...base,
    failure_receipt: { ...base.failure_receipt, failure_stage: 'search_surface' } }), false);
  assert.equal(isRetryableMobileStartupResult({ ...base,
    failure_receipt: { ...base.failure_receipt, startup_attempt: 2 } }), false);
  assert.equal(isRetryableMobileStartupResult({ ...base,
    failure_receipt: { ...base.failure_receipt, error_class: 'webdriver_session_error' } }), false);
  assert.equal(isRetryableMobileStartupResult({ ...base,
    failure_receipt: { ...base.failure_receipt, log_truncated: true } }), false);
  const retained = closedMobileStartupRetryReceipt(base);
  assert.equal(retained.schema_version, 'appium-startup-retry-v1');
  assert.equal(retained.startup_attempt, 1);
  assert.equal(JSON.stringify(retained).includes('target_'), false);
});

test('closed first-attempt receipt survives evidence cleanup for terminal attempt', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'search-ios-retry-'));
  const path = join(dir, 'prior.json');
  const value = {
    schema_version: 'appium-startup-retry-v1', failure_stage: 'webdriver_session_create',
    auth_callback_started: false, webdriver_client_session_created: false,
    appium_server_ready: true, elapsed_ms: 300_000, startup_attempt: 1,
    error_class: 'webdriver_client_request_timeout', log_inspected: true, log_truncated: false,
    simulator_started: true, wda_install_attempted: true, wda_start_attempted: true,
    wda_start_failed: false, wda_session_attempted: false,
    wda_session_started: false, wda_session_failed: false, wda_started: false,
  };
  await writeFile(path, JSON.stringify(value));
  assert.deepEqual(await readPriorMobileStartupReceipt(path), value);
  await writeFile(path, JSON.stringify({ ...value, error_class: 'secret-bearing-unclosed-value' }));
  assert.equal(await readPriorMobileStartupReceipt(path), null);
});
