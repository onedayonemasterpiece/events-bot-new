import assert from 'node:assert/strict';
import { test } from 'node:test';

import { buildExactTargetNavigationReceipt, buildSameOriginNavigationReceipt,
  countEventSearchPostRequests,
  extractSanitizedNavigationResponses } from '../e2e/mobile-web/appium-network-receipt.mjs';
import { buildMobilePreflightFailureReceipt, isSafeMobilePreflightRetryReceipt,
  runAppiumTransportPreflight } from '../e2e/mobile-web/appium-preflight.mjs';
import { createAppiumSearchAdapter } from '../e2e/search/adapters/appium-base.mjs';
import { buildAppiumCapabilities } from '../e2e/mobile-web/appium-browser.mjs';

function preflightDriver(platform = 'android') {
  const events = [];
  let current = platform === 'android' ? 'CHROMIUM' : 'WEBVIEW_1';
  return {
    events,
    capabilities: platform === 'android' ? {
      platformName: 'Android', browserName: 'Chrome', browserVersion: '140.0.1',
      platformVersion: '16', automationName: 'UiAutomator2',
    } : {
      platformName: 'iOS', browserName: 'Safari', browserVersion: '18.5',
      platformVersion: '18.5', automationName: 'XCUITest', bundleId: 'com.apple.mobilesafari',
    },
    async getContext() { events.push('getContext'); return current; },
    async getContexts() { events.push('getContexts'); return ['NATIVE_APP', platform === 'android' ? 'CHROMIUM' : 'WEBVIEW_1']; },
    async switchContext(value) { events.push(`switch:${value}`); current = value; },
    async getWindowSize() { events.push('viewport'); return { width: 390, height: 844 }; },
    async execute() { events.push('storage-purge'); return true; },
    async deleteCookies() { events.push('cookies-purge'); },
    async deleteSession() { events.push('deleteSession'); },
  };
}

test('Android preflight proves real Chrome/UiAutomator2 without navigation or network activity', async () => {
  const driver = preflightDriver('android');
  driver.url = async () => { throw new Error('navigation_forbidden'); };
  driver.getLogs = async () => { throw new Error('network_probe_forbidden'); };
  const receipt = await runAppiumTransportPreflight(driver, {
    platform: 'android', expectedCapabilities: { platformVersion: '16' },
    env: { E2E_APPIUM_VERSION: '3.0.2', E2E_APPIUM_DRIVER_VERSION: '4.2.1' },
  });
  assert.deepEqual(receipt.native_viewport, { width: 390, height: 844 });
  assert.deepEqual(receipt.context_classes, ['native', 'webview']);
  assert.equal(receipt.transport, 'real_android_chrome');
  assert.equal(receipt.automation_name, 'UiAutomator2');
  assert.equal(receipt.side_effect_free, true);
  assert.equal(receipt.browser_ready, true);
  assert.equal(receipt.transport_ready, true);
  assert.equal(receipt.viewport_ready, true);
  assert.equal(receipt.auth_requests, 0);
  assert.equal(receipt.search_posts, 0);
  assert.equal(receipt.otp_requests, 0);
  assert.equal(receipt.supabase_requests, 0);
  assert.equal(receipt.side_effects.navigation_count, 0);
  assert.equal(receipt.side_effects.fetch_count, 0);
  assert.equal(receipt.side_effects.search_post_count, 0);
  assert.equal(receipt.continuation_handle, 'in_process_adapter');
  assert.equal(receipt.session_identifier_serialized, false);
  assert.doesNotMatch(JSON.stringify(receipt), /CHROMIUM|sessionId|deviceName/iu);
});

test('Appium health diagnostics expose only cumulative closed runtime and driver counts', async () => {
  const driver = preflightDriver('android');
  let networkRead = 0;
  driver.execute = async (fn) => {
    if (fn?.name === 'snapshotSearchRuntimeProbe') return {
      network: { failed_requests: 1, storage_requests: 1 },
    };
    return true;
  };
  driver.getLogs = async (type) => {
    if (type === 'browser') return [{ level: 'SEVERE', message: 'raw console secret' }];
    if (type !== 'performance' || networkRead++ > 0) return [];
    return [
      { message: JSON.stringify({ method: 'Network.requestWillBeSent', params: {
        requestId: 'storage-1', request: { url: 'https://example.test/storage/v1/object/private/raw', method: 'GET' },
      } }) },
      { message: JSON.stringify({ method: 'Network.responseReceived', params: {
        requestId: 'fn-1', response: { url: 'https://example.test/functions/v1/event-search?raw=yes', status: 503 },
      } }) },
      { message: JSON.stringify({ method: 'Network.loadingFailed', params: {
        requestId: 'failed-1', errorText: 'raw network secret',
      } }) },
    ];
  };
  const adapter = await createAppiumSearchAdapter({ platform: 'android', driver });
  const first = await adapter.healthDiagnostics();
  assert.deepEqual(first, {
    console_errors: 1, failed_requests: 1, error_responses: 1, storage_requests: 1,
  });
  assert.doesNotMatch(JSON.stringify(first), /raw|secret|event-search/u);
  assert.deepEqual(await adapter.healthDiagnostics(), first);
});

test('Appium result snapshot counts actual skeleton cards and id-less placeholders', async () => {
  const eventCard = {
    getAttribute(name) { return name === 'data-event-id' ? '42' : ''; },
    getBoundingClientRect() { return { width: 10, height: 10, top: 0, bottom: 10 }; },
  };
  const placeholder = {
    getAttribute() { return ''; },
    getBoundingClientRect() { return { width: 10, height: 10, top: 0, bottom: 10 }; },
  };
  const results = { hidden: false, querySelector() { return null; } };
  const driver = {
    capabilities: {},
    async execute(fn) {
      const prior = { document: globalThis.document, getComputedStyle: globalThis.getComputedStyle,
        innerHeight: globalThis.innerHeight };
      globalThis.document = {
        querySelector(selector) {
          if (selector === '[data-search-results]') return results;
          if (selector === '[data-search-status]') return { getAttribute() { return null; } };
          if (selector === '[data-search-submit]') return { getAttribute() { return 'false'; } };
          return null;
        },
        querySelectorAll(selector) {
          if (selector.includes('.authorized-search__skeleton-card')) return [{}];
          if (selector.includes('[data-event-id]')) return [eventCard];
          if (selector.includes('[data-event-card]')) return [eventCard, placeholder];
          return [];
        },
      };
      globalThis.getComputedStyle = () => ({ display: 'block', visibility: 'visible' });
      globalThis.innerHeight = 800;
      try { return fn(); } finally {
        globalThis.document = prior.document;
        globalThis.getComputedStyle = prior.getComputedStyle;
        globalThis.innerHeight = prior.innerHeight;
      }
    },
  };
  const adapter = await createAppiumSearchAdapter({ platform: 'android', driver });
  const snapshot = await adapter.snapshotResults();
  assert.equal(snapshot.skeleton_count, 1);
  assert.equal(snapshot.placeholder_count, 1);
});

test('iOS preflight uses the Safari preparation hook then proves Mobile Safari/XCUITest/WDA', async () => {
  const caps = buildAppiumCapabilities('ios', {
    deviceName: 'iPhone 16', platformVersion: '18.5', udid: 'test-udid',
  }, {});
  assert.equal(caps['appium:showSafariConsoleLog'], false);
  assert.equal(caps['appium:showSafariNetworkLog'], true);
  const driver = preflightDriver('ios');
  let prepared = 0;
  const receipt = await runAppiumTransportPreflight(driver, {
    platform: 'ios', expectedCapabilities: { 'appium:platformVersion': '18.5' },
    iosPrepare: async () => { prepared += 1; },
    env: { E2E_XCODE_VERSION: '16.4', E2E_WDA_SHA: 'abc123' },
  });
  assert.equal(prepared, 1);
  assert.equal(receipt.transport, 'real_ios_mobile_safari');
  assert.equal(receipt.automation_name, 'XCUITest');
  assert.equal(receipt.wda_session_proven, true);
  assert.equal(receipt.xcode_version, '16.4');
  assert.equal(receipt.wda_version, 'abc123');
});

test('retry requires attempt one, a deleted Appium session and an explicit zero-side-effect receipt', () => {
  const safe = buildMobilePreflightFailureReceipt({
    platform: 'ios', error: new Error('mobile_preflight_web_context_missing'),
    attempt: 1, driverSessionCreated: true, driverSessionDeleted: true,
  });
  assert.equal(safe.retry_safe, true);
  assert.equal(isSafeMobilePreflightRetryReceipt(safe), true);
  assert.equal(isSafeMobilePreflightRetryReceipt({ ...safe, startup_attempt: 2 }), false);
  assert.equal(isSafeMobilePreflightRetryReceipt({ ...safe,
    side_effects: { ...safe.side_effects, broker_session_issued: true } }), false);
  const leaked = buildMobilePreflightFailureReceipt({
    platform: 'ios', error: new Error('fail'), attempt: 1,
    driverSessionCreated: true, driverSessionDeleted: false,
  });
  assert.equal(leaked.retry_safe, false);
  assert.equal(isSafeMobilePreflightRetryReceipt(leaked), false);
});

test('adapter deletes an iOS session when Safari preparation fails before returning a receipt', async () => {
  const driver = preflightDriver('ios');
  const adapter = await createAppiumSearchAdapter({
    platform: 'ios', driver,
    capabilities: { platformName: 'iOS', 'appium:automationName': 'XCUITest' },
    iosPrepare: async () => { throw new Error('safari_web_context_timeout raw target'); },
  });
  await assert.rejects(() => adapter.preflight(), (error) => {
    assert.equal(error.searchReceipt.schema_version, 'mobile-preflight-failure-v1');
    assert.equal(error.searchReceipt.cleanup_confirmed, true);
    assert.equal(error.searchReceipt.retry_safe, true);
    assert.doesNotMatch(JSON.stringify(error.searchReceipt), /raw target/u);
    return true;
  });
  assert.equal(driver.events.at(-1), 'deleteSession');
});

test('adapter continues the same successful session and purges local auth before deletion', async () => {
  const driver = preflightDriver('android');
  const adapter = await createAppiumSearchAdapter({
    platform: 'android', driver,
    capabilities: { platformName: 'Android', browserName: 'Chrome',
      'appium:automationName': 'UiAutomator2', platformVersion: '16' },
  });
  const receipt = await adapter.preflight();
  const diagnostics = await adapter.diagnostics();
  assert.equal(receipt.same_session_continuation, true);
  assert.equal(diagnostics.transport_preflight_passed, true);
  const cleanup = await adapter.close();
  assert.deepEqual(cleanup, { auth_local_purge_confirmed: true, webdriver_session_deleted: true });
  assert.ok(driver.events.indexOf('storage-purge') < driver.events.indexOf('deleteSession'));
  assert.ok(driver.events.indexOf('cookies-purge') < driver.events.indexOf('deleteSession'));
});

test('mobile owner proof returns only closed Auth/RLS receipt and waits for zero pending bytes', async () => {
  const driver = preflightDriver('android');
  driver.execute = async (fn) => {
    if (fn?.name === 'verifyAuthenticatedOwnerRuntimeProbe') return {
      get_user_verified: true, protected_probe_verified: true, protected_probe_request_count: 1,
      product_otp_issue_count: 0, external_mail_send_count: 0, external_mail_receipt_count: 0,
      real_mail_fallback: 'forbidden',
    };
    if (fn?.name === 'snapshotSearchRuntimeProbe') return {
      meter: { schema_version: 'supabase_client_observed_bytes_v1', pending_measurements: 0, total_bytes: 123 },
    };
    return true;
  };
  const adapter = await createAppiumSearchAdapter({ platform: 'android', driver });
  const verified = await adapter.verifyAuthenticatedOwner();
  assert.equal(verified.receipt.get_user_verified, true);
  assert.equal(verified.receipt.protected_probe_request_count, 1);
  assert.equal(verified.meter.pending_measurements, 0);
  assert.doesNotMatch(JSON.stringify(verified), /access_token|email|user_id|authorization/iu);
});

test('first-card navigation receipt proves same-origin HTTP 200 without retaining URL or query', async () => {
  const rawLogs = [{ message: JSON.stringify({ message: {
    method: 'Network.responseReceived', params: { type: 'Document', response: {
      status: 200, url: 'https://kenigevents.ru/events/example?token=must-not-survive',
    } },
  } }) }];
  const responses = extractSanitizedNavigationResponses(rawLogs);
  assert.deepEqual(responses, [{ origin: 'https://kenigevents.ru', pathname: '/events/example',
    status: 200, resource_type: 'document' }]);
  const receipt = buildSameOriginNavigationReceipt({
    beforeUrl: 'https://kenigevents.ru/search',
    expectedUrl: 'https://kenigevents.ru/events/example?private=yes',
    finalUrl: 'https://kenigevents.ru/events/example',
    responses, networkSource: 'safariNetwork',
  });
  assert.equal(receipt.same_origin, true);
  assert.equal(receipt.http_status, 200);
  assert.equal(receipt.network_source, 'safariNetwork');
  assert.doesNotMatch(JSON.stringify(receipt), /example|private|token|kenigevents/iu);
});

test('mobile protocol receipt counts unique event-search POSTs without retaining request data', () => {
  const seen = new Set();
  const request = { message: JSON.stringify({ message: {
    method: 'Network.requestWillBeSent', params: { requestId: 'search-1', request: {
      method: 'POST', url: 'https://project.supabase.co/functions/v1/event-search?secret=no',
      postData: '{"query":"must not survive"}',
    } },
  } }) };
  assert.equal(countEventSearchPostRequests([request], seen), 1);
  assert.equal(countEventSearchPostRequests([request], seen), 0);
  assert.equal(countEventSearchPostRequests([{ message: JSON.stringify({ message: {
    method: 'Network.requestWillBeSent', params: { requestId: 'other-1', request: {
      method: 'GET', url: 'https://project.supabase.co/functions/v1/event-search',
    } },
  } }) }], seen), 0);
  assert.deepEqual([...seen], ['search-1']);
});

test('adapter opens the captured first result and binds the browser navigation to driver network logs', async () => {
  let currentUrl = 'https://kenigevents.ru/search';
  let logReads = 0;
  const driver = {
    capabilities: {},
    async execute() { return { href: 'https://kenigevents.ru/events/42?secret=yes', same_origin: true }; },
    async getUrl() { return currentUrl; },
    async getLogs(type) {
      if (type === 'browser') return [];
      assert.equal(type, 'performance');
      logReads += 1;
      if (logReads === 1) return [];
      if (logReads === 2) return [
        { message: JSON.stringify({ method: 'Network.responseReceived', params: {
          type: 'Document', requestId: 'document-42',
          response: { status: 200, url: 'https://kenigevents.ru/events/42?secret=yes' },
        } }) },
        { message: JSON.stringify({ method: 'Network.requestWillBeSent', params: {
          requestId: 'storage-after-open', request: { method: 'GET', url: 'https://assets.example/storage/v1/object/42' },
        } }) },
      ];
      if (logReads === 3) return [
        { message: JSON.stringify({ method: 'Network.requestWillBeSent', params: {
          requestId: 'late-search', request: {
            method: 'POST', url: 'https://project.supabase.co/functions/v1/event-search?late=yes',
          },
        } }) },
      ];
      if (logReads === 4) return [
        { message: JSON.stringify({ method: 'Network.requestWillBeSent', params: {
          requestId: 'final-boundary-search', request: {
            method: 'POST', url: 'https://project.supabase.co/functions/v1/event-search?final=yes',
          },
        } }) },
      ];
      return [];
    },
    async $(selector) {
      assert.match(selector, /data-search-results/u);
      return { click: async () => { currentUrl = 'https://kenigevents.ru/events/42'; } };
    },
    async waitUntil(fn) { if (!await fn()) throw new Error('wait_failed'); },
  };
  const adapter = await createAppiumSearchAdapter({ platform: 'android', driver });
  const receipt = await adapter.openFirstResult();
  assert.equal(receipt.schema_version, 'mobile-card-open-v1');
  assert.equal(receipt.same_origin, true);
  assert.equal(receipt.http_status, 200);
  assert.equal(receipt.destination_class, 'event_detail');
  assert.equal(receipt.network_source, 'performance');
  assert.equal(receipt.raw_url_retained, false);
  assert.ok(receipt.search_page_activity_before_navigation);
  assert.equal((await adapter.healthDiagnostics()).storage_requests, 1);
  assert.equal(await adapter.postNavigationSearchPostCount(), 2);
});

test('navigation receipt rejects cross-origin and non-200 document evidence', () => {
  assert.throws(() => buildSameOriginNavigationReceipt({
    beforeUrl: 'https://kenigevents.ru/search', expectedUrl: 'https://evil.example/event',
    finalUrl: 'https://evil.example/event', responses: [],
  }), /cross_origin/u);
  assert.throws(() => buildSameOriginNavigationReceipt({
    beforeUrl: 'https://kenigevents.ru/search', expectedUrl: 'https://kenigevents.ru/events/1',
    finalUrl: 'https://kenigevents.ru/events/1',
    responses: [{ origin: 'https://kenigevents.ru', pathname: '/events/1', status: 404,
      resource_type: 'document' }],
  }), /http_200_missing/u);
  assert.throws(() => buildSameOriginNavigationReceipt({
    beforeUrl: 'https://kenigevents.ru/search', expectedUrl: 'https://kenigevents.ru/events/1',
    finalUrl: 'https://kenigevents.ru/events/1',
    responses: [
      { origin: 'https://evil.example', pathname: '/bounce', status: 302, resource_type: 'document' },
      { origin: 'https://kenigevents.ru', pathname: '/events/1', status: 200, resource_type: 'document' },
    ],
  }), /redirected|cross_origin/u);
});


test('mobile pinned target receipt rejects redirect and requires exact 2xx document', () => {
  const target = 'https://kenigevents.ru/_review/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA/poisk/';
  assert.equal(buildExactTargetNavigationReceipt({
    expectedUrl: target, finalUrl: target, networkSource: 'performance',
    responses: [{ origin: 'https://kenigevents.ru', pathname: '/_review/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA/poisk/', status: 200, resource_type: 'document' }],
  }).redirect_count, 0);
  assert.throws(() => buildExactTargetNavigationReceipt({
    expectedUrl: target, finalUrl: target,
    responses: [{ origin: 'https://kenigevents.ru', pathname: '/other', status: 302, resource_type: 'document' }],
  }), /search_target_redirected/u);
  assert.equal(buildExactTargetNavigationReceipt({
    expectedUrl: target, finalUrl: target,
    responses: [
      { origin: 'https://cdn.example', pathname: '/font.woff2', status: 302, resource_type: 'font' },
      { origin: 'https://kenigevents.ru', pathname: '/_review/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA/poisk/', status: 200, resource_type: 'document' },
    ],
  }).redirect_count, 0);
  assert.throws(() => buildExactTargetNavigationReceipt({
    expectedUrl: target, finalUrl: target,
    responses: [
      { origin: 'https://evil.example', pathname: '/poisk/', status: 200, resource_type: 'document' },
      { origin: 'https://kenigevents.ru', pathname: '/_review/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA/poisk/', status: 200, resource_type: 'document' },
    ],
  }), /cross_origin/u);
});

test('mobile Auth callback response bytes join explicit getUser/RLS meter without retaining URL', async () => {
  const target = 'https://kenigevents.ru/_review/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA/poisk/';
  let logs = 0;
  const driver = {
    capabilities: {},
    async getLogs() {
      logs += 1;
      if (logs !== 2) return [];
      return [{ message: JSON.stringify({ method: 'Network.responseReceived', params: {
        type: 'Document', response: { status: 303,
          url: 'https://project.supabase.co/auth/v1/verify?token=secret', encodedDataLength: 2048 },
      } }) }];
    },
    async url() {}, async getUrl() { return target; },
    async waitUntil(predicate) { if (!await predicate()) throw new Error('wait_failed'); },
    async execute(fn) {
      if (fn?.name === 'verifyAuthenticatedOwnerRuntimeProbe') return {
        get_user_verified: true, protected_probe_verified: true, protected_probe_request_count: 1,
        product_otp_issue_count: 0, external_mail_send_count: 0, external_mail_receipt_count: 0,
        real_mail_fallback: 'forbidden',
      };
      if (fn?.name === 'snapshotSearchRuntimeProbe') return {
        meter: { total_bytes: 100, target_bytes: 48 * 1024, hard_limit_bytes: 96 * 1024,
          categories: { auth: 100, edge: 0, direct_rest: 0, direct_rpc: 0 } },
      };
      return true;
    },
  };
  const adapter = await createAppiumSearchAdapter({ platform: 'android', driver });
  await adapter.bootstrapSession('https://project.supabase.co/auth/v1/verify?token=secret', target);
  const verified = await adapter.verifyAuthenticatedOwner();
  assert.equal(verified.meter.categories.auth, 2148);
  assert.equal(verified.meter.total_bytes, 2148);
  assert.doesNotMatch(JSON.stringify(verified), /project|verify|token|secret/u);
});
