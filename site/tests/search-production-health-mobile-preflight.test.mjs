import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { test } from 'node:test';
import vm from 'node:vm';

import { buildExactTargetNavigationReceipt, buildSameOriginNavigationReceipt,
  countEventSearchPostRequests, createSanitizedNavigationResponseTracker,
  extractSanitizedNavigationResponses } from '../e2e/mobile-web/appium-network-receipt.mjs';
import { buildMobilePreflightFailureReceipt, isSafeMobilePreflightRetryReceipt,
  runAppiumTransportPreflight } from '../e2e/mobile-web/appium-preflight.mjs';
import { createAppiumSearchAdapter,
  installAndroidAuthByteProbe,
  installAppiumClassicLogCommands } from '../e2e/search/adapters/appium-base.mjs';
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

test('Android preflight bounded-waits for the Chrome web context on a fresh emulator', async () => {
  const driver = preflightDriver('android');
  let reads = 0;
  driver.getContexts = async () => {
    driver.events.push('getContexts');
    reads += 1;
    return reads < 3 ? ['NATIVE_APP'] : ['NATIVE_APP', 'CHROMIUM'];
  };
  const receipt = await runAppiumTransportPreflight(driver, {
    platform: 'android', expectedCapabilities: { platformVersion: '16' },
    contextWait: { timeoutMs: 2_000, intervalMs: 1, now: () => reads * 10,
      sleep: async () => {} },
  });
  assert.equal(reads, 3);
  assert.deepEqual(receipt.context_classes, ['native', 'webview']);
  assert.equal(receipt.side_effect_free, true);
});

test('standalone WebdriverIO 9 Appium session receives the exact Classic getLogs command', () => {
  const driver = {
    addCommand(name, implementation) { this[name] = implementation; },
  };
  assert.equal(driver.getLogs, undefined);
  assert.equal(installAppiumClassicLogCommands(driver), driver);
  assert.equal(typeof driver.getLogs, 'function');
  assert.equal(typeof driver.executeCdp, 'function');
  const installed = driver.getLogs;
  const installedCdp = driver.executeCdp;
  installAppiumClassicLogCommands(driver);
  assert.equal(driver.getLogs, installed);
  assert.equal(driver.executeCdp, installedCdp);
  const source = readFileSync(new URL('../e2e/search/adapters/appium-base.mjs', import.meta.url), 'utf8');
  assert.match(source, /\/session\/:sessionId\/goog\/cdp\/execute/u);
  assert.doesNotMatch(source, /chromium\/send_command_and_get_result/u);
});

test('pre-document Android Auth observer measures received body or declared bytes and exports counters only', async () => {
  const responses = [
    new Response('received-body'),
    new Response('', { headers: { 'content-length': '2048' } }),
    new Response('excluded'),
  ];
  const context = vm.createContext({
    URL, Request, Response,
    location: { href: 'https://kenigevents.ru/poisk/' },
    fetch: async () => responses.shift(),
  });
  vm.runInContext(
    `(${installAndroidAuthByteProbe.toString()})(${JSON.stringify({
      allowed_origins: ['https://project.supabase.co'],
    })})`, context,
  );
  await vm.runInContext("fetch('https://project.supabase.co/auth/v1/verify')", context);
  await vm.runInContext("fetch('https://project.supabase.co/auth/v1/user')", context);
  await vm.runInContext("fetch('https://project.supabase.co/rest/v1/ignored')", context);
  for (let attempt = 0; attempt < 20; attempt += 1) {
    const pending = vm.runInContext(
      'globalThis.__KENIGEVENTS_ANDROID_AUTH_BYTES_V1__.pending_count', context,
    );
    if (pending === 0) break;
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
  const snapshot = JSON.parse(vm.runInContext(
    'JSON.stringify(globalThis.__KENIGEVENTS_ANDROID_AUTH_BYTES_V1__)', context,
  ));
  assert.deepEqual(snapshot, {
    schema_version: 'android_auth_bytes_v1', request_count: 2, closed_count: 2,
    pending_count: 0, failed_count: 0, total_bytes: 2061,
  });
  assert.doesNotMatch(JSON.stringify(snapshot), /project|verify|user|received-body|token/u);
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

test('Appium target-open log drains remain in cumulative closed diagnostics', async () => {
  const target = 'https://kenigevents.ru/_review/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA/poisk/';
  let reads = 0;
  const driver = {
    capabilities: {},
    async getLogs(type) {
      if (type === 'browser') return [];
      reads += 1;
      if (reads === 1) return [{ message: JSON.stringify({ method: 'Network.loadingFailed', params: {
        requestId: 'target-failed', errorText: 'must not survive',
      } }) }];
      if (reads === 2) return [
        { message: JSON.stringify({ method: 'Network.responseReceived', params: {
          requestId: 'target-doc', type: 'Document', response: { url: target, status: 200,
            headers: { 'content-length': '1024' } },
        } }) },
        { message: JSON.stringify({ method: 'Network.responseReceived', params: {
          requestId: 'target-edge-503', type: 'Fetch', response: {
            url: 'https://project.supabase.co/functions/v1/transport-probe', status: 503,
          },
        } }) },
      ];
      return [];
    },
    async url() {}, async getUrl() { return target; },
    async waitUntil(fn) { if (!await fn()) throw new Error('wait_failed'); },
    async execute(fn) {
      if (fn?.name === 'snapshotSearchRuntimeProbe') return { network: {} };
      return true;
    },
  };
  const adapter = await createAppiumSearchAdapter({ platform: 'android', driver });
  await adapter.open(target);
  const diagnostics = await adapter.healthDiagnostics();
  assert.equal(diagnostics.failed_requests, 1);
  assert.equal(diagnostics.error_responses, 1);
  assert.doesNotMatch(JSON.stringify(diagnostics), /target|probe|survive/u);
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
  assert.equal(caps['appium:showSafariNetworkLog'], false);
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
      status: 200, url: 'https://kenigevents.ru/sobytiya/example/?token=must-not-survive',
    } },
  } }) }];
  const responses = extractSanitizedNavigationResponses(rawLogs);
  assert.deepEqual(responses, [{ origin: 'https://kenigevents.ru', pathname: '/sobytiya/example/',
    status: 200, resource_type: 'document' }]);
  const receipt = buildSameOriginNavigationReceipt({
    beforeUrl: 'https://kenigevents.ru/poisk/',
    expectedUrl: 'https://kenigevents.ru/sobytiya/example/',
    finalUrl: 'https://kenigevents.ru/sobytiya/example/',
    responses, networkSource: 'safariNetwork',
  });
  assert.equal(receipt.same_origin, true);
  assert.equal(receipt.http_status, 200);
  assert.equal(receipt.network_source, 'safariNetwork');
  assert.doesNotMatch(JSON.stringify(receipt), /example|private|token|kenigevents/iu);
});

test('CDP redirectResponse exposes a document redirect that both target and card receipts reject', () => {
  const target = 'https://kenigevents.ru/_review/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA/poisk/';
  const rawLogs = [
    { message: JSON.stringify({ message: {
      method: 'Network.requestWillBeSent', params: {
        requestId: 'document-chain', type: 'Document',
        request: { method: 'GET', url: target },
        redirectResponse: { status: 302, url: 'https://evil.example/bounce?secret=no' },
      },
    } }) },
    { message: JSON.stringify({ message: {
      method: 'Network.responseReceived', params: {
        requestId: 'document-chain', type: 'Document',
        response: { status: 200, url: target },
      },
    } }) },
  ];
  const responses = extractSanitizedNavigationResponses(rawLogs);
  assert.deepEqual(responses, [
    { origin: 'https://evil.example', pathname: '/bounce', status: 302, resource_type: 'document' },
    { origin: 'https://kenigevents.ru', pathname: '/_review/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA/poisk/', status: 200, resource_type: 'document' },
  ]);
  assert.throws(() => buildExactTargetNavigationReceipt({
    expectedUrl: target, finalUrl: target, responses,
  }), /redirected|cross_origin/u);
  const sameOriginRedirectResponses = extractSanitizedNavigationResponses([
    { message: JSON.stringify({ message: {
      method: 'Network.requestWillBeSent', params: {
        requestId: 'same-origin-document-chain', type: 'Document',
        request: { method: 'GET', url: target },
        redirectResponse: { status: 302, url: 'https://kenigevents.ru/old-search?private=no' },
      },
    } }) },
    rawLogs[1],
  ]);
  assert.throws(() => buildExactTargetNavigationReceipt({
    expectedUrl: target, finalUrl: target, responses: sameOriginRedirectResponses,
  }), /redirected/u);
  const cardTarget = 'https://kenigevents.ru/_review/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA/sobytiya/example/';
  const cardResponses = extractSanitizedNavigationResponses([
    rawLogs[0],
    { message: JSON.stringify({ message: {
      method: 'Network.responseReceived', params: {
        requestId: 'document-chain', type: 'Document',
        response: { status: 200, url: cardTarget },
      },
    } }) },
  ]);
  assert.throws(() => buildSameOriginNavigationReceipt({
    beforeUrl: target, expectedUrl: cardTarget,
    finalUrl: cardTarget, responses: cardResponses,
  }), /redirected|cross_origin/u);
  assert.doesNotMatch(JSON.stringify(responses), /secret/u);
});

test('CDP subresource redirectResponse remains harmless to exact target and card receipts', () => {
  const target = 'https://kenigevents.ru/sobytiya/42/';
  const rawLogs = [
    { message: JSON.stringify({ message: {
      method: 'Network.requestWillBeSent', params: {
        requestId: 'image-chain', type: 'Image',
        request: { method: 'GET', url: 'https://cdn.example/final.webp' },
        redirectResponse: { status: 302, url: 'https://cdn.example/old.webp?secret=no' },
      },
    } }) },
    { message: JSON.stringify({ message: {
      method: 'Network.responseReceived', params: {
        requestId: 'document-final', type: 'Document',
        response: { status: 200, url: target },
      },
    } }) },
  ];
  const responses = extractSanitizedNavigationResponses(rawLogs);
  assert.equal(responses[0].resource_type, 'image');
  assert.equal(responses[0].status, 302);
  assert.equal(buildExactTargetNavigationReceipt({
    expectedUrl: target, finalUrl: target, responses,
  }).redirect_count, 0);
  assert.equal(buildSameOriginNavigationReceipt({
    beforeUrl: 'https://kenigevents.ru/poisk/', expectedUrl: target,
    finalUrl: target, responses,
  }).http_status, 200);
});

test('CDP loadingFinished supplies terminal bytes while Content-Length keeps precedence', () => {
  const rawLogs = [
    { message: JSON.stringify({ message: {
      method: 'Network.responseReceived', params: {
        requestId: 'auth-terminal', type: 'Fetch', response: {
          status: 200, url: 'https://project.supabase.co/auth/v1/user',
          headers: {}, encodedDataLength: 64,
        },
      },
    } }) },
    { message: JSON.stringify({ message: {
      method: 'Network.loadingFinished', params: {
        requestId: 'auth-terminal', encodedDataLength: 4096,
      },
    } }) },
    { message: JSON.stringify({ message: {
      method: 'Network.responseReceived', params: {
        requestId: 'auth-declared', type: 'Fetch', response: {
          status: 200, url: 'https://project.supabase.co/auth/v1/token',
          headers: { 'content-length': '128' }, encodedDataLength: 32,
        },
      },
    } }) },
    { message: JSON.stringify({ message: {
      method: 'Network.loadingFinished', params: {
        requestId: 'auth-declared', encodedDataLength: 8192,
      },
    } }) },
  ];
  const responses = extractSanitizedNavigationResponses(rawLogs);
  assert.deepEqual(responses.map((item) => item.encoded_bytes), [4096, 128]);
  assert.doesNotMatch(JSON.stringify(responses), /auth-terminal|auth-declared/u);
});

test('CDP request start remains pending across drains until response terminal bytes arrive', () => {
  const tracker = createSanitizedNavigationResponseTracker();
  tracker.consume([{ message: JSON.stringify({ method: 'Network.requestWillBeSent', params: {
    requestId: 'auth-cross-drain', type: 'Fetch', request: {
      method: 'GET', url: 'https://project.supabase.co/auth/v1/verify?secret=no',
    },
  } }) }]);
  assert.equal(tracker.pendingTerminalCount({
    origin: 'https://project.supabase.co', pathPrefix: '/auth/v1',
  }), 1);
  assert.deepEqual(tracker.responses(), []);

  tracker.consume([{ message: JSON.stringify({ method: 'Network.responseReceived', params: {
    requestId: 'auth-cross-drain', type: 'Fetch', response: {
      status: 200, url: 'https://project.supabase.co/auth/v1/verify?secret=no',
      headers: {}, encodedDataLength: 64,
    },
  } }) }]);
  assert.equal(tracker.pendingTerminalCount({ pathPrefix: '/auth/v1' }), 1);
  assert.deepEqual(tracker.pendingTerminalSummary({ pathPrefix: '/auth/v1' }), {
    total: 1, request_only: 0, response_seen: 1, received_data: 0,
    verify: 1, user: 0, token: 0, other: 0,
  });

  tracker.consume([{ message: JSON.stringify({ method: 'Network.loadingFinished', params: {
    requestId: 'auth-cross-drain', encodedDataLength: 2048,
  } }) }]);
  assert.equal(tracker.pendingTerminalCount({ pathPrefix: '/auth/v1' }), 0);
  assert.equal(tracker.responses()[0].encoded_bytes, 2048);
  assert.doesNotMatch(JSON.stringify(tracker.responses()), /cross-drain|secret/u);
});

test('CDP loadingFailed closes an observed response with only actually received data bytes', () => {
  const tracker = createSanitizedNavigationResponseTracker();
  tracker.consume([{ message: JSON.stringify({ method: 'Network.requestWillBeSent', params: {
    requestId: 'auth-cancelled', type: 'Fetch', request: {
      method: 'GET', url: 'https://project.supabase.co/auth/v1/user?secret=no',
    },
  } }) }, { message: JSON.stringify({ method: 'Network.responseReceived', params: {
    requestId: 'auth-cancelled', type: 'Fetch', response: {
      status: 200, url: 'https://project.supabase.co/auth/v1/user?secret=no',
      headers: {}, encodedDataLength: 64,
    },
  } }) }, { message: JSON.stringify({ method: 'Network.dataReceived', params: {
    requestId: 'auth-cancelled', encodedDataLength: 1536,
  } }) }]);
  assert.equal(tracker.pendingTerminalCount({ pathPrefix: '/auth/v1' }), 1);

  tracker.consume([{ message: JSON.stringify({ method: 'Network.loadingFailed', params: {
    requestId: 'auth-cancelled', type: 'Fetch', errorText: 'net::ERR_ABORTED', canceled: true,
  } }) }]);
  assert.equal(tracker.pendingTerminalCount({ pathPrefix: '/auth/v1' }), 0);
  assert.equal(tracker.responses()[0].encoded_bytes, 1536);
  assert.doesNotMatch(JSON.stringify(tracker.responses()), /cancelled|secret|ERR_ABORTED/u);
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

test('mobile protocol receipt counts each physical POST dispatch in one CDP redirect chain', () => {
  const seen = new Set();
  const initial = { message: JSON.stringify({ method: 'Network.requestWillBeSent', params: {
    requestId: 'redirected-search', timestamp: 101.25, request: {
      method: 'POST', url: 'https://project.supabase.co/functions/v1/event-search?first=redacted',
    },
  } }) };
  const redirected = { message: JSON.stringify({ method: 'Network.requestWillBeSent', params: {
    requestId: 'redirected-search', timestamp: 101.5,
    redirectResponse: {
      status: 307, url: 'https://project.supabase.co/functions/v1/event-search?first=redacted',
    },
    request: {
      method: 'POST', url: 'https://relay.example/functions/v1/event-search?second=redacted',
    },
  } }) };

  assert.equal(countEventSearchPostRequests([initial, redirected], seen), 2);
  assert.equal(countEventSearchPostRequests([initial, redirected], seen), 0);
  assert.equal(JSON.stringify([...seen]).includes('first=redacted'), false);
  assert.equal(JSON.stringify([...seen]).includes('second=redacted'), false);
});

test('adapter opens the captured first result and binds the browser navigation to driver network logs', async () => {
  let currentUrl = 'https://kenigevents.ru/poisk/';
  let logReads = 0;
  const driver = {
    capabilities: {},
    async execute() { return { href: 'https://kenigevents.ru/sobytiya/42/', same_origin: true,
      supabase_origins: ['https://project.supabase.co'] }; },
    async getUrl() { return currentUrl; },
    async getLogs(type) {
      if (type === 'browser') return [];
      assert.equal(type, 'performance');
      logReads += 1;
      if (logReads === 1) return [];
      if (logReads === 2) return [
        { message: JSON.stringify({ method: 'Network.responseReceived', params: {
          type: 'Document', requestId: 'document-42',
          response: { status: 200, url: 'https://kenigevents.ru/sobytiya/42/?secret=yes' },
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
        { message: JSON.stringify({ method: 'Network.responseReceived', params: {
          requestId: 'late-search', type: 'Fetch', response: {
            status: 200, url: 'https://project.supabase.co/functions/v1/event-search?late=yes',
            headers: { 'content-length': '512' },
          },
        } }) },
      ];
      if (logReads === 4) return [
        { message: JSON.stringify({ method: 'Network.requestWillBeSent', params: {
          requestId: 'final-boundary-search', request: {
            method: 'POST', url: 'https://project.supabase.co/functions/v1/event-search?final=yes',
          },
        } }) },
        { message: JSON.stringify({ method: 'Network.responseReceived', params: {
          requestId: 'final-boundary-search', type: 'Fetch', response: {
            status: 200, url: 'https://project.supabase.co/functions/v1/event-search?final=yes',
            headers: { 'content-length': '256' },
          },
        } }) },
        { message: JSON.stringify({ method: 'Network.responseReceived', params: {
          requestId: 'late-rest', type: 'Fetch', response: {
            status: 200, url: 'https://project.supabase.co/rest/v1/user_saved_event',
            headers: {}, encodedDataLength: 64,
          },
        } }) },
      ];
      if (logReads === 5) return [
        { message: JSON.stringify({ method: 'Network.loadingFinished', params: {
          requestId: 'late-rest', encodedDataLength: 2048,
        } }) },
      ];
      return [];
    },
    async $(selector) {
      assert.match(selector, /data-search-results/u);
      return { click: async () => { currentUrl = 'https://kenigevents.ru/sobytiya/42/'; } };
    },
    async waitUntil(fn) { if (!await fn()) throw new Error('wait_failed'); },
  };
  const adapter = await createAppiumSearchAdapter({ platform: 'android', driver,
    supabaseOrigins: ['https://project.supabase.co'], physicalQuietMs: 25 });
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
  const postNavigationMeter = await adapter.postNavigationMeterSnapshot();
  assert.equal(postNavigationMeter.total_bytes, 2816);
  assert.equal(postNavigationMeter.categories.edge, 768);
  assert.equal(postNavigationMeter.categories.direct_rest, 2048);
  await adapter.awaitPhysicalIdle();
  const physical = await adapter.physicalActivity();
  assert.equal(physical.search_posts, 2);
  assert.equal(physical.meter.total_bytes, 2816);
});

test('Appium failed evidence preserves the pre-navigation snapshot when final logs disappear', async () => {
  let currentUrl = 'https://kenigevents.ru/poisk/';
  let reads = 0;
  let unavailable = false;
  const activity = { requests: [{ method: 'POST', path: '/functions/v1/event-search' }],
    responses: [{ request_id: 'mobile-before-log-loss' }], routes: [], meter: { total_bytes: 2048 } };
  const driver = {
    capabilities: {},
    async execute(fn) {
      if (fn?.name === 'snapshotSearchRuntimeProbe') return structuredClone(activity);
      if (fn?.name === 'pageResultSnapshot') return { terminal: true, rendered_ids: ['42'] };
      return { href: 'https://kenigevents.ru/sobytiya/42/', same_origin: true,
        supabase_origins: ['https://project.supabase.co'] };
    },
    async getUrl() { return currentUrl; },
    async getLogs(type) {
      if (type === 'browser') return [];
      if (unavailable) return null;
      reads += 1;
      if (reads === 1) return [];
      return [{ message: JSON.stringify({ method: 'Network.responseReceived', params: {
        requestId: 'document-42', type: 'Document', response: {
          status: 200, url: 'https://kenigevents.ru/sobytiya/42/', headers: { 'content-length': '1024' },
        },
      } }) }];
    },
    async $(selector) { assert.match(selector, /data-search-results/u); return { click: async () => { currentUrl = 'https://kenigevents.ru/sobytiya/42/'; } }; },
    async waitUntil(fn) { if (!await fn()) throw new Error('wait_failed'); },
  };
  const adapter = await createAppiumSearchAdapter({ platform: 'android', driver, postNavigationQuietMs: 25 });
  await adapter.openFirstResult();
  unavailable = true;
  const retained = await adapter.failedJourneyEvidence();
  assert.equal(retained.activity.responses[0].request_id, 'mobile-before-log-loss');
  assert.equal(retained.post_navigation_search_post_count, 0);
  assert.equal(retained.post_navigation_meter, undefined);
});

test('navigation receipt rejects cross-origin and non-200 document evidence', () => {
  assert.throws(() => buildSameOriginNavigationReceipt({
    beforeUrl: 'https://kenigevents.ru/poisk/', expectedUrl: 'https://evil.example/sobytiya/event/',
    finalUrl: 'https://evil.example/sobytiya/event/', responses: [],
  }), /cross_origin/u);
  assert.throws(() => buildSameOriginNavigationReceipt({
    beforeUrl: 'https://kenigevents.ru/poisk/', expectedUrl: 'https://kenigevents.ru/sobytiya/1/',
    finalUrl: 'https://kenigevents.ru/sobytiya/1/',
    responses: [{ origin: 'https://kenigevents.ru', pathname: '/sobytiya/1/', status: 404,
      resource_type: 'document' }],
  }), /http_200_missing/u);
  assert.throws(() => buildSameOriginNavigationReceipt({
    beforeUrl: 'https://kenigevents.ru/poisk/', expectedUrl: 'https://kenigevents.ru/sobytiya/1/',
    finalUrl: 'https://kenigevents.ru/sobytiya/1/',
    responses: [
      { origin: 'https://evil.example', pathname: '/bounce', status: 302, resource_type: 'document' },
      { origin: 'https://kenigevents.ru', pathname: '/sobytiya/1/', status: 200, resource_type: 'document' },
    ],
  }), /redirected|cross_origin/u);
  for (const badPath of ['/', '/mesta/example/']) {
    assert.throws(() => buildSameOriginNavigationReceipt({
      beforeUrl: 'https://kenigevents.ru/poisk/',
      expectedUrl: `https://kenigevents.ru${badPath}`,
      finalUrl: `https://kenigevents.ru${badPath}`,
      responses: [{ origin: 'https://kenigevents.ru', pathname: badPath, status: 200,
        resource_type: 'document' }],
    }), /event_path|candidate_prefix/u);
  }
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

test('mobile Auth callback rejects responseReceived partial bytes without terminal loadingFinished', async () => {
  const target = 'https://kenigevents.ru/_review/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA/poisk/';
  let logs = 0;
  const driver = {
    capabilities: {},
    async getLogs() {
      logs += 1;
      if (logs !== 2) return [];
      return [{ message: JSON.stringify({ method: 'Network.responseReceived', params: {
        requestId: 'auth-partial', type: 'Fetch', response: { status: 200,
          url: 'https://project.supabase.co/auth/v1/verify?token=secret', encodedDataLength: 2048 },
      } }) }];
    },
    async url() {}, async getUrl() { return target; },
    async waitUntil(predicate, options = {}) {
      if (!await predicate()) throw new Error(options.timeoutMsg || 'wait_failed');
    },
    async execute() { return true; },
  };
  const adapter = await createAppiumSearchAdapter({ platform: 'android', driver });
  await assert.rejects(
    () => adapter.bootstrapSession('https://project.supabase.co/auth/v1/verify?token=secret', target),
    /mobile_auth_terminal_bytes_timeout_verify_response_seen/u,
  );
});

test('Android Auth callback uses a pre-document byte probe when performance logs contain request starts only', async () => {
  const target = 'https://kenigevents.ru/_review/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA/poisk/';
  const commands = [];
  let logs = 0;
  const driver = {
    capabilities: {},
    async getLogs() {
      logs += 1;
      if (logs !== 2) return [];
      return ['verify', 'user'].map((kind) => ({ message: JSON.stringify({
        method: 'Network.requestWillBeSent', params: {
          requestId: `auth-${kind}`, type: 'Fetch', request: { method: 'GET',
            url: `https://project.supabase.co/auth/v1/${kind}?secret=no` },
        },
      }) }));
    },
    async executeCdp(command, params) {
      commands.push([command, params]);
      return command === 'Page.addScriptToEvaluateOnNewDocument' ? { identifier: 'script-1' } : {};
    },
    async url() {}, async getUrl() { return target; },
    async waitUntil(predicate, options = {}) {
      if (!await predicate()) throw new Error(options.timeoutMsg || 'wait_failed');
    },
    async execute(fn) {
      if (fn?.name === 'snapshotAndroidAuthByteProbe') return {
        schema_version: 'android_auth_bytes_v1', request_count: 2, closed_count: 2,
        pending_count: 0, failed_count: 0, total_bytes: 3072,
      };
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
  const adapter = await createAppiumSearchAdapter({ platform: 'android', driver,
    supabaseOrigins: ['https://project.supabase.co'] });
  await adapter.bootstrapSession(`${target}?token_hash=bridge-secret&type=magiclink`, target);
  const verified = await adapter.verifyAuthenticatedOwner();
  assert.equal(verified.meter.categories.auth, 3172);
  assert.deepEqual(commands.map(([name]) => name), [
    'Page.addScriptToEvaluateOnNewDocument', 'Page.removeScriptToEvaluateOnNewDocument',
  ]);
  assert.match(commands[0][1].source, /android_auth_bytes_v1/u);
  assert.doesNotMatch(JSON.stringify(verified), /project|verify|token|secret/u);
});

test('mobile Auth callback waits across log drains for terminal bytes before owner proof', async () => {
  const target = 'https://kenigevents.ru/_review/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA/poisk/';
  let logs = 0;
  const driver = {
    capabilities: {},
    async getLogs() {
      logs += 1;
      if (logs === 2) return [{ message: JSON.stringify({ method: 'Network.requestWillBeSent', params: {
        requestId: 'auth-late-terminal', type: 'Fetch', request: { method: 'GET',
          url: 'https://project.supabase.co/auth/v1/verify?token=secret' },
      } }) }];
      if (logs === 3) return [{ message: JSON.stringify({ method: 'Network.responseReceived', params: {
        requestId: 'auth-late-terminal', type: 'Fetch', response: { status: 200,
          url: 'https://project.supabase.co/auth/v1/verify?token=secret', encodedDataLength: 64 },
      } }) }];
      if (logs === 4) return [{ message: JSON.stringify({ method: 'Network.loadingFinished', params: {
        requestId: 'auth-late-terminal', encodedDataLength: 2048,
      } }) }];
      return [];
    },
    async url() {}, async getUrl() { return target; },
    async waitUntil(predicate, options = {}) {
      for (let attempt = 0; attempt < 5; attempt += 1) {
        if (await predicate()) return;
      }
      throw new Error(options.timeoutMsg || 'wait_failed');
    },
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
  await adapter.bootstrapSession(`${target}?token_hash=bridge-secret&type=magiclink`, target);
  const verified = await adapter.verifyAuthenticatedOwner();
  assert.equal(verified.meter.categories.auth, 2148);
  assert.equal(verified.meter.total_bytes, 2148);
  assert.doesNotMatch(JSON.stringify(verified), /project|verify|token|secret/u);
});

test('mobile Auth callback closes an authorised background Auth cancellation without timing out', async () => {
  const target = 'https://kenigevents.ru/_review/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA/poisk/';
  let logs = 0;
  const driver = {
    capabilities: {},
    async getLogs() {
      logs += 1;
      if (logs === 2) return [
        { message: JSON.stringify({ method: 'Network.responseReceived', params: {
          requestId: 'auth-verify', type: 'Fetch', response: { status: 200,
            url: 'https://project.supabase.co/auth/v1/verify?token=secret',
            headers: { 'content-length': '2048' } },
        } }) },
        { message: JSON.stringify({ method: 'Network.requestWillBeSent', params: {
          requestId: 'auth-background', type: 'Fetch', request: { method: 'GET',
            url: 'https://project.supabase.co/auth/v1/user?secret=no' },
        } }) },
        { message: JSON.stringify({ method: 'Network.responseReceived', params: {
          requestId: 'auth-background', type: 'Fetch', response: { status: 200,
            url: 'https://project.supabase.co/auth/v1/user?secret=no', encodedDataLength: 64 },
        } }) },
        { message: JSON.stringify({ method: 'Network.dataReceived', params: {
          requestId: 'auth-background', encodedDataLength: 512,
        } }) },
      ];
      if (logs === 3) return [{ message: JSON.stringify({ method: 'Network.loadingFailed', params: {
        requestId: 'auth-background', type: 'Fetch', errorText: 'net::ERR_ABORTED', canceled: true,
      } }) }];
      return [];
    },
    async url() {}, async getUrl() { return target; },
    async waitUntil(predicate, options = {}) {
      for (let attempt = 0; attempt < 5; attempt += 1) {
        if (await predicate()) return;
      }
      throw new Error(options.timeoutMsg || 'wait_failed');
    },
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
  await adapter.bootstrapSession(`${target}?token_hash=bridge-secret&type=magiclink`, target);
  const verified = await adapter.verifyAuthenticatedOwner();
  assert.equal(verified.meter.categories.auth, 2660);
  assert.equal(verified.meter.total_bytes, 2660);
  assert.doesNotMatch(JSON.stringify(verified), /background|project|verify|token|secret|ERR_ABORTED/u);
});
