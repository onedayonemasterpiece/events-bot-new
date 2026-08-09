import { installSearchRuntimeProbe, snapshotSearchRuntimeProbe,
  verifyAuthenticatedOwnerRuntimeProbe } from './runtime-probe.mjs';
import { buildAppiumSessionFailureReceipt } from '../../mobile-web/appium-startup-receipt.mjs';
import { assertCanonicalCandidateEventDestination, buildExactTargetNavigationReceipt, buildSameOriginNavigationReceipt,
  countEventSearchPostRequests, createSanitizedNavigationResponseTracker,
  extractSanitizedNavigationResponses } from '../../mobile-web/appium-network-receipt.mjs';
import { buildMobilePreflightFailureReceipt,
  runAppiumTransportPreflight } from '../../mobile-web/appium-preflight.mjs';
import { dismissNativeKeyboard, focusIosSafariWebInput, observeNativeKeyboard,
  performNativeDocumentSwipe, prepareIosSafariWebContext,
  withNativeAppContext } from '../../mobile-web/appium-browser.mjs';
import { SupabaseClientObservedByteMeter } from '../production-health-meter.mjs';
import { command as webdriverCommand } from 'webdriver';

const IOS_SEARCH_INPUT_LABELS = Object.freeze([
  'Что хочется сделать?',
  'Например: послушать хор или сходить с детьми бесплатно',
  'Например: джаз на выходных',
]);
const IOS_SEARCH_KEYBOARD_DISMISS_LABELS = Object.freeze([
  'Найти событие',
  'Найти событие по описанию',
]);

function pageResultSnapshot() {
  const root = document.querySelector('[data-authorized-search]');
  const results = document.querySelector('[data-search-results]');
  const status = document.querySelector('[data-search-status]');
  const submit = document.querySelector('[data-search-submit]');
  const allCards = Array.from(document.querySelectorAll('[data-search-results] [data-event-card], [data-search-results] [data-search-vector-card]'));
  const cards = allCards.filter((node) => String(node.getAttribute('data-event-id') || ''));
  const skeletons = new Set(document.querySelectorAll(
    '[data-search-skeletons]:not([hidden]) .authorized-search__skeleton-card, [data-search-results] .authorized-search__skeleton-card, [data-search-results] [data-skeleton]',
  ));
  const isVisible = (node) => { const rect = node.getBoundingClientRect(); const style = getComputedStyle(node);
    return style.display !== 'none' && style.visibility !== 'hidden' && rect.width > 0 && rect.height > 0 && rect.top < innerHeight && rect.bottom > 0; };
  const renderedIds = cards.map((node) => String(node.getAttribute('data-event-id') || '')).filter(Boolean);
  const renderedFamilies = cards.map((node) => {
    const members = String(node.getAttribute('data-occurrence-member-ids') || '').split(',').filter(Boolean).sort();
    return members.length > 1 ? `family:${members.join(',')}` : `event:${node.getAttribute('data-event-id') || ''}`;
  });
  const error = status?.getAttribute('role') === 'alert';
  return { terminal: root?.dataset?.searchTerminal === 'true' && submit?.getAttribute('aria-busy') !== 'true',
    error, cards_visible: cards.some(isVisible), visible_card_count: cards.filter(isVisible).length,
    rendered_ids: renderedIds, rendered_families: renderedFamilies,
    skeleton_count: skeletons.size,
    placeholder_count: allCards.filter((node) => !String(node.getAttribute('data-event-id') || '')).length,
    card_renderer_unavailable: Boolean(results?.querySelector('[data-search-card-render-unavailable]')) };
}

const closedLogLevelError = (value) => ['SEVERE', 'ERROR'].includes(String(value || '').toUpperCase());

function accumulateClosedDriverDiagnostics(logs, diagnostics, seen,
  requestMetadata = new Map(), criticalOrigins = []) {
  const allowedOrigins = new Set(criticalOrigins);
  const disposablePath = (path) => path === '/auth/v1/health'
    || path === '/rest/v1/rpc/transport_probe_v1'
    || path === '/functions/v1/transport-probe';
  const criticalRequest = (metadata) => {
    if (metadata?.resource_type === 'document') return true;
    return allowedOrigins.has(metadata?.origin)
      && /^\/(?:auth|functions|rest)\/v1(?:\/|$)/u.test(String(metadata?.pathname || ''))
      && !disposablePath(metadata.pathname);
  };
  const visited = new WeakSet();
  const record = (kind, identity) => {
    const key = `${kind}:${String(identity || '')}`;
    if (!seen.has(key)) {
      seen.add(key);
      diagnostics[kind] += 1;
    }
  };
  const visit = (value, depth = 0) => {
    if (depth > 10 || value == null) return;
    if (typeof value === 'string') {
      const text = value.trim();
      if (!text.startsWith('{') && !text.startsWith('[')) return;
      try { visit(JSON.parse(text), depth + 1); } catch { /* non-protocol log */ }
      return;
    }
    if (typeof value !== 'object' || visited.has(value)) return;
    visited.add(value);
    if (Array.isArray(value)) { value.forEach((item) => visit(item, depth + 1)); return; }
    const method = String(value.method || '');
    const params = value.params && typeof value.params === 'object' ? value.params : {};
    const requestId = String(params.requestId || params.timestamp || value.timestamp || 'unidentified');
    if (method === 'Network.requestWillBeSent' || method === 'Network.responseReceived') {
      const rawUrl = method === 'Network.requestWillBeSent'
        ? params.request?.url : params.response?.url;
      let path = '';
      let origin = '';
      try { const parsed = new URL(String(rawUrl)); path = parsed.pathname; origin = parsed.origin; } catch { /* malformed URL is ignored */ }
      if (method === 'Network.requestWillBeSent') {
        requestMetadata.set(requestId, { origin, pathname: path,
          resource_type: String(params.type || '').toLowerCase() });
      }
      if (/^\/storage\/v1(?:\/|$)/u.test(path)) record('storage_requests', requestId);
      const status = Number(params.response?.status || 0);
      const responseMetadata = { origin, pathname: path,
        resource_type: String(params.type || '').toLowerCase() };
      if (method === 'Network.responseReceived' && status >= 400
        && criticalRequest(responseMetadata)) {
        record('error_responses', requestId);
      }
    }
    if (method === 'Network.loadingFailed'
      && criticalRequest(requestMetadata.get(requestId))) {
      record('failed_requests', requestId);
    }
    if ((method === 'Runtime.consoleAPICalled' && String(params.type).toLowerCase() === 'error')
      || (method === 'Log.entryAdded' && closedLogLevelError(params.entry?.level))) {
      record('console_errors', requestId);
    }
    if (!method && closedLogLevelError(value.level)) {
      record('console_errors', `${value.timestamp || 'untimestamped'}:${String(value.level).toUpperCase()}`);
    }
    Object.values(value).forEach((child) => visit(child, depth + 1));
  };
  visit(logs);
}

export async function runRealTouchScroll({ readScrollY, lastCardVisible, gesture, wait, maxGestures = 40 }) {
  const before = Number(await readScrollY());
  let cardVisible = false;
  let gestures = 0;
  let lastGesture = null;
  while (gestures < maxGestures && !cardVisible) {
    const receipt = await gesture();
    if (receipt && typeof receipt === 'object') lastGesture = receipt;
    gestures += 1;
    await wait();
    cardVisible = await lastCardVisible();
  }
  const after = Number(await readScrollY());
  return { performed: gestures > 0, delta_y: after - before,
    card_visible_after: cardVisible === true, gesture_count: gestures,
    ...(lastGesture ? { last_gesture: lastGesture } : {}) };
}

export function installAppiumClassicLogCommands(driver) {
  if (!driver || typeof driver.addCommand !== 'function') {
    throw new TypeError('mobile_classic_log_adapter_missing');
  }
  if (typeof driver.getLogs !== 'function') {
    driver.addCommand('getLogs', webdriverCommand('POST', '/session/:sessionId/log', {
      command: 'getLogs',
      parameters: [{ name: 'type', type: 'string', required: true }],
    }));
  }
  if (typeof driver.executeCdp !== 'function') {
    driver.addCommand('executeCdp', webdriverCommand(
      'POST', '/session/:sessionId/goog/cdp/execute', {
        command: 'executeCdp',
        parameters: [
          { name: 'cmd', type: 'string', required: true },
          { name: 'params', type: 'object', required: true },
        ],
      },
    ));
  }
  return driver;
}

/**
 * Runs before product scripts in every Android Chrome document. It is the
 * authoritative Android physical boundary because ChromeDriver's performance
 * log can legally expose a request start without the matching response events.
 * Only closed counters and byte totals leave the page.
 */
export function installAndroidAuthByteProbe(config = {}) {
  const KEY = '__KENIGEVENTS_ANDROID_AUTH_BYTES_V1__';
  if (globalThis[KEY]?.schema_version === 'android_physical_observer_v2') return;
  const origins = new Set(Array.isArray(config.allowed_origins) ? config.allowed_origins : []);
  const state = {
    schema_version: 'android_physical_observer_v2',
    document_generation: `${Date.now()}-${Math.random()}`,
    request_count: 0, closed_count: 0, pending_count: 0, failed_count: 0,
    total_bytes: 0, search_posts: 0, storage_requests: 0,
    receipt_rpc_requests: 0,
    categories: { auth: 0, edge: 0, direct_rest: 0, direct_rpc: 0 },
  };
  const nativeFetch = globalThis.fetch.bind(globalThis);
  const trackedRequest = (input, init) => {
    try {
      const raw = typeof input === 'string' || input instanceof URL ? input : input?.url;
      const url = new URL(String(raw), globalThis.location?.href);
      if (!origins.has(url.origin)) return null;
      const path = url.pathname;
      const method = String(init?.method || input?.method || 'GET').toUpperCase();
      const disposable = path === '/auth/v1/health'
        || path === '/rest/v1/rpc/transport_probe_v1'
        || path === '/functions/v1/transport-probe';
      if (path === '/auth/v1' || path.startsWith('/auth/v1/')) {
        return { category: 'auth', path, method, disposable };
      }
      if (path === '/functions/v1' || path.startsWith('/functions/v1/')) {
        return { category: 'edge', path, method, disposable };
      }
      if (path === '/rest/v1/rpc' || path.startsWith('/rest/v1/rpc/')) {
        return { category: 'direct_rpc', path, method, disposable };
      }
      if (path === '/rest/v1' || path.startsWith('/rest/v1/')) {
        return { category: 'direct_rest', path, method, disposable };
      }
      if (path === '/storage/v1' || path.startsWith('/storage/v1/')) {
        return { category: null, path, method, disposable: false };
      }
      return null;
    } catch { return null; }
  };
  const responseBytes = async (response) => {
    const rawDeclared = response?.headers?.get?.('content-length');
    const declared = Number(rawDeclared);
    if (rawDeclared != null && String(rawDeclared).trim() !== ''
      && Number.isSafeInteger(declared) && declared >= 0) return declared;
    return (await response.clone().arrayBuffer()).byteLength;
  };
  globalThis.fetch = async (...args) => {
    const tracked = trackedRequest(args[0], args[1]);
    if (tracked) {
      state.request_count += 1;
      state.pending_count += 1;
      if (tracked.method === 'POST' && tracked.path === '/functions/v1/event-search') {
        state.search_posts += 1;
      }
      if (tracked.path === '/storage/v1' || tracked.path.startsWith('/storage/v1/')) {
        state.storage_requests += 1;
      }
      if (/^\/rest\/v1\/rpc\/get_event_search_receipt(?:_|$)/u.test(tracked.path)) {
        state.receipt_rpc_requests += 1;
      }
    }
    try {
      const response = await nativeFetch(...args);
      if (tracked) {
        void responseBytes(response).then((bytes) => {
          const value = Number(bytes || 0);
          state.total_bytes += value;
          if (tracked.category) state.categories[tracked.category] += value;
          state.closed_count += 1;
        }).catch(() => { state.failed_count += 1; })
          .finally(() => { state.pending_count = Math.max(0, state.pending_count - 1); });
      }
      return response;
    } catch (error) {
      if (tracked) {
        if (tracked.disposable) state.closed_count += 1;
        else state.failed_count += 1;
        state.pending_count = Math.max(0, state.pending_count - 1);
      }
      throw error;
    }
  };
  Object.defineProperty(globalThis, KEY, { value: state, configurable: false });
}

export function snapshotAndroidAuthByteProbe() {
  const state = globalThis.__KENIGEVENTS_ANDROID_AUTH_BYTES_V1__;
  if (state?.schema_version !== 'android_physical_observer_v2') return null;
  return {
    schema_version: state.schema_version,
    document_generation: String(state.document_generation || ''),
    request_count: Number(state.request_count || 0),
    closed_count: Number(state.closed_count || 0),
    pending_count: Number(state.pending_count || 0),
    failed_count: Number(state.failed_count || 0),
    total_bytes: Number(state.total_bytes || 0),
    search_posts: Number(state.search_posts || 0),
    storage_requests: Number(state.storage_requests || 0),
    receipt_rpc_requests: Number(state.receipt_rpc_requests || 0),
    categories: {
      auth: Number(state.categories?.auth || 0),
      edge: Number(state.categories?.edge || 0),
      direct_rest: Number(state.categories?.direct_rest || 0),
      direct_rpc: Number(state.categories?.direct_rpc || 0),
    },
  };
}

export async function createAppiumSearchAdapter(options = {}) {
  const platform = options.platform;
  if (!['android', 'ios'].includes(platform)) throw new Error(`search_mobile_platform:${platform}`);
  const timeoutMs = Number(options.timeoutMs || 60_000);
  let driver = options.driver || null;
  if (!driver) {
    const { remote } = await import('webdriverio');
    const startedAt = Date.now();
    try {
      driver = await remote({
        hostname: options.hostname || '127.0.0.1', port: Number(options.port || 4723), path: options.path || '/',
        logLevel: options.logLevel || 'error',
        connectionRetryTimeout: Number(options.connectionRetryTimeout || 300_000),
        connectionRetryCount: 0,
        capabilities: options.capabilities,
      });
      // WebdriverIO 9 removed deprecated JSONWP log commands even when the
      // session is intentionally WebDriver Classic. Appium still exposes the
      // exact endpoint used for Chrome performance and Safari log buckets.
      installAppiumClassicLogCommands(driver);
    } catch (error) {
      const receipt = await buildAppiumSessionFailureReceipt({
        error, platform, startedAt,
        logPath: options.appiumLogPath || process.env.E2E_APPIUM_LOG_PATH,
        appiumServerReady: process.env.E2E_APPIUM_STATUS_READY === '1',
        startupAttempt: process.env.E2E_APPIUM_STARTUP_ATTEMPT,
      });
      const failure = error instanceof Error ? error : new Error('webdriver_session_error');
      failure.searchReceipt = receipt;
      throw failure;
    }
  }
  const lifecycle = {
    failure_stage: 'webdriver_session_created',
    auth_callback_started: false, auth_callback_authorized: false,
    webdriver_client_session_created: true, native_safari_stable: platform !== 'ios',
    webview_attached: platform !== 'ios', transport_preflight_passed: false,
    search_surface_ready: false,
    startup_attempt: [1, 2].includes(Number(process.env.E2E_APPIUM_STARTUP_ATTEMPT))
      ? Number(process.env.E2E_APPIUM_STARTUP_ATTEMPT) : 1,
  };
  let configuredPolicy = {};
  let nativeKeyboardObserved = false;
  let closed = false;
  let callbackAuthObservedBytes = 0;
  const driverDiagnostics = {
    console_errors: 0, failed_requests: 0, error_responses: 0, storage_requests: 0,
  };
  const driverDiagnosticIds = new Set();
  const driverDiagnosticRequests = new Map();
  let postBoundarySearchPostCount = null;
  let postBoundarySearchObservationActive = false;
  const postBoundarySearchRequestIds = new Set();
  let postBoundaryResponseTracker = null;
  let postBoundarySupabaseOrigins = [];
  let postBoundaryMeterSnapshot = null;
  let preNavigationSearchActivity = null;
  let preNavigationResultState = null;
  const wholeCellResponseTracker = createSanitizedNavigationResponseTracker();
  const wholeCellSearchRequestIds = new Set();
  let wholeCellSearchPostCount = 0;
  let wholeCellOrigins = [...new Set((options.supabaseOrigins || []).map((value) => new URL(value).origin))];
  let androidPhysicalScriptIdentifier = '';
  let androidPhysicalLast = null;
  const androidPhysicalTotals = {
    request_count: 0, closed_count: 0, failed_count: 0, total_bytes: 0,
    search_posts: 0, storage_requests: 0, receipt_rpc_requests: 0,
    categories: { auth: 0, edge: 0, direct_rest: 0, direct_rpc: 0 },
  };
  let androidPostBoundaryStart = null;

  const addAndroidPhysicalSnapshot = (snapshot) => {
    if (platform !== 'android' || !snapshot
      || snapshot.schema_version !== 'android_physical_observer_v2'
      || !snapshot.document_generation) return false;
    const sameDocument = androidPhysicalLast?.document_generation === snapshot.document_generation;
    const previous = sameDocument ? androidPhysicalLast : {
      request_count: 0, closed_count: 0, failed_count: 0, total_bytes: 0,
      search_posts: 0, storage_requests: 0, receipt_rpc_requests: 0,
      categories: { auth: 0, edge: 0, direct_rest: 0, direct_rpc: 0 },
    };
    for (const key of ['request_count', 'closed_count', 'failed_count', 'total_bytes',
      'search_posts', 'storage_requests', 'receipt_rpc_requests']) {
      androidPhysicalTotals[key] += Math.max(0,
        Number(snapshot[key] || 0) - Number(previous[key] || 0));
    }
    for (const key of ['auth', 'edge', 'direct_rest', 'direct_rpc']) {
      androidPhysicalTotals.categories[key] += Math.max(0,
        Number(snapshot.categories?.[key] || 0) - Number(previous.categories?.[key] || 0));
    }
    androidPhysicalLast = structuredClone(snapshot);
    return true;
  };

  const snapshotAndroidPhysical = async ({ required = false } = {}) => {
    if (platform !== 'android') return null;
    const snapshot = await driver.execute(snapshotAndroidAuthByteProbe).catch(() => null);
    if (!addAndroidPhysicalSnapshot(snapshot) && required) {
      throw new Error('mobile_android_physical_observer_missing');
    }
    return snapshot;
  };

  const androidMeterSnapshot = (totals = androidPhysicalTotals) => {
    const meter = new SupabaseClientObservedByteMeter({ supabaseOrigins: wholeCellOrigins });
    const origin = wholeCellOrigins[0];
    if (!origin) throw new Error('search_physical_observation_missing');
    const paths = {
      auth: '/auth/v1/user', edge: '/functions/v1/event-search',
      direct_rest: '/rest/v1/user_saved_event', direct_rpc: '/rest/v1/rpc/health',
    };
    for (const [category, pathname] of Object.entries(paths)) {
      const bytes = Number(totals.categories?.[category] || 0);
      if (bytes > 0) meter.recordResponse({ url: `${origin}${pathname}`,
        headers: { 'content-length': String(bytes) }, body: null });
    }
    return meter.snapshot();
  };

  const subtractAndroidTotals = (after, before) => ({
    search_posts: Math.max(0, Number(after.search_posts || 0) - Number(before?.search_posts || 0)),
    storage_requests: Math.max(0, Number(after.storage_requests || 0) - Number(before?.storage_requests || 0)),
    receipt_rpc_requests: Math.max(0, Number(after.receipt_rpc_requests || 0) - Number(before?.receipt_rpc_requests || 0)),
    categories: Object.fromEntries(['auth', 'edge', 'direct_rest', 'direct_rpc'].map((key) => [
      key, Math.max(0, Number(after.categories?.[key] || 0) - Number(before?.categories?.[key] || 0)),
    ])),
  });

  const observePostBoundarySearch = (logs) => {
    wholeCellResponseTracker.consume(logs);
    wholeCellSearchPostCount += countEventSearchPostRequests(logs, wholeCellSearchRequestIds);
    if (!postBoundarySearchObservationActive) return;
    postBoundarySearchPostCount += countEventSearchPostRequests(
      logs, postBoundarySearchRequestIds,
    );
    postBoundaryResponseTracker?.consume(logs);
  };

  const syncClosedDriverDiagnostics = async () => {
    const networkType = platform === 'android' ? 'performance' : 'safariNetwork';
    const consoleType = platform === 'android' ? 'browser' : 'safariConsole';
    const networkLogs = await driver.getLogs?.(networkType).catch(() => null);
    const consoleLogs = await driver.getLogs?.(consoleType).catch(() => null);
    if (!Array.isArray(networkLogs) || !Array.isArray(consoleLogs)) {
      throw new Error('mobile_health_diagnostics_unavailable');
    }
    observePostBoundarySearch(networkLogs);
    accumulateClosedDriverDiagnostics(networkLogs, driverDiagnostics, driverDiagnosticIds,
      driverDiagnosticRequests, wholeCellOrigins);
    accumulateClosedDriverDiagnostics(consoleLogs, driverDiagnostics, driverDiagnosticIds,
      driverDiagnosticRequests, wholeCellOrigins);
  };

  const meterWithCallbackAuthBytes = (meter = {}) => {
    const categories = { auth: 0, edge: 0, direct_rest: 0, direct_rpc: 0, ...(meter.categories || {}) };
    categories.auth = Number(categories.auth || 0) + callbackAuthObservedBytes;
    const total = Object.values(categories).reduce((sum, value) => sum + Number(value || 0), 0);
    const target = Number(meter.target_bytes || 48 * 1024);
    const hard = Number(meter.hard_limit_bytes || 96 * 1024);
    return {
      ...meter, categories, total_bytes: total, target_bytes: target, hard_limit_bytes: hard,
      budget_status: total <= target ? 'within_target' : total <= hard ? 'above_target' : 'hard_limit_exceeded',
      target_met: total <= target, cost_guard_passed: total <= hard, hard_limit_exceeded: total > hard,
    };
  };

  const deleteDriverSession = async () => {
    if (closed) return true;
    await driver.deleteSession();
    closed = true;
    lifecycle.webdriver_client_session_deleted = true;
    return true;
  };

  const purgeLocalAuthState = async () => {
    const originalContext = await driver.getContext().catch(() => null);
    const contexts = await driver.getContexts().catch(() => []);
    const webContext = contexts.find((value) => {
      const normalized = String(value).toUpperCase();
      return normalized.includes('WEBVIEW') || normalized.includes('CHROMIUM');
    });
    if (!webContext) return lifecycle.auth_callback_started !== true;
    if (String(originalContext).toUpperCase() !== String(webContext).toUpperCase()) {
      await driver.switchContext(webContext);
    }
    try {
      const purged = await driver.execute(() => {
        try {
          localStorage.clear();
          sessionStorage.clear();
          return localStorage.length === 0 && sessionStorage.length === 0;
        } catch { return false; }
      });
      await driver.deleteCookies?.().catch(() => undefined);
      return purged === true;
    } finally {
      if (originalContext && String(originalContext).toUpperCase() !== String(webContext).toUpperCase()) {
        await driver.switchContext(originalContext).catch(() => undefined);
      }
    }
  };

  const adapter = {
    async diagnostics() { return structuredClone(lifecycle); },
    async preflight() {
      if (lifecycle.transport_preflight_passed && lifecycle.transport_preflight_receipt) {
        return structuredClone(lifecycle.transport_preflight_receipt);
      }
      lifecycle.failure_stage = platform === 'ios'
        ? 'native_safari_webview_prepare' : 'mobile_transport_preflight';
      try {
        const receipt = await runAppiumTransportPreflight(driver, {
          platform,
          expectedCapabilities: options.capabilities,
          startupAttempt: lifecycle.startup_attempt,
          env: options.env || process.env,
          iosPrepare: platform === 'ios'
            ? (options.iosPrepare || prepareIosSafariWebContext) : null,
        });
        lifecycle.native_safari_stable = platform === 'ios' ? true : lifecycle.native_safari_stable;
        lifecycle.webview_attached = true;
        lifecycle.transport_preflight_passed = true;
        lifecycle.transport_preflight_receipt = receipt;
        lifecycle.failure_stage = 'auth_callback_not_started';
        return structuredClone(receipt);
      } catch (error) {
        let deleted = false;
        try { deleted = await deleteDriverSession(); } catch { deleted = false; }
        const receipt = buildMobilePreflightFailureReceipt({
          platform, error, attempt: lifecycle.startup_attempt,
          driverSessionCreated: true, driverSessionDeleted: deleted,
        });
        const failure = error instanceof Error ? error : new Error('mobile_preflight_failed');
        failure.searchReceipt = receipt;
        throw failure;
      }
    },
    async bootstrapSession(actionLink, returnTarget) {
      lifecycle.failure_stage = 'auth_callback';
      lifecycle.auth_callback_started = true;
      const networkType = platform === 'android' ? 'performance' : 'safariNetwork';
      const initialLogs = await driver.getLogs(networkType).catch(() => null);
      if (!Array.isArray(initialLogs)) throw new Error('mobile_auth_network_log_unavailable');
      observePostBoundarySearch(initialLogs);
      if (platform === 'android' && typeof driver.executeCdp === 'function'
        && !androidPhysicalScriptIdentifier) {
        const allowedOrigins = [...new Set((options.supabaseOrigins || [])
          .map((value) => new URL(value).origin))];
        let installed;
        try {
          installed = await driver.executeCdp(
            'Page.addScriptToEvaluateOnNewDocument', {
              source: `(${installAndroidAuthByteProbe.toString()})(${JSON.stringify({ allowed_origins: allowedOrigins })})`,
            },
          );
        } catch {
          throw new Error('mobile_android_cdp_route_unavailable');
        }
        androidPhysicalScriptIdentifier = String(installed?.identifier || '');
        if (!androidPhysicalScriptIdentifier) throw new Error('mobile_android_cdp_script_receipt_missing');
      }
      await driver.url(actionLink);
      await driver.waitUntil(async () => new URL(await driver.getUrl()).origin === new URL(returnTarget).origin,
        { timeout: timeoutMs, interval: 250, timeoutMsg: 'search_auth_callback_timeout' });
      await driver.waitUntil(async () => driver.execute(() => document.querySelector('[data-authorized-search]')
        ?.classList.contains('is-authorized') === true),
      { timeout: timeoutMs, interval: 250, timeoutMsg: 'search_auth_callback_session_not_restored' });
      const callbackLogs = await driver.getLogs(networkType).catch(() => null);
      if (!Array.isArray(callbackLogs)) throw new Error('mobile_auth_network_log_unavailable');
      observePostBoundarySearch(callbackLogs);
      const callbackTracker = createSanitizedNavigationResponseTracker();
      callbackTracker.consume(callbackLogs);
      let androidAuthBytes = null;
      if (platform === 'android' && androidPhysicalScriptIdentifier) {
        await driver.waitUntil(async () => {
          const snapshot = await snapshotAndroidPhysical({ required: true });
          if (snapshot.failed_count > 0) throw new Error('mobile_auth_page_meter_failed');
          if (snapshot.request_count < 1 || snapshot.pending_count > 0
            || snapshot.closed_count !== snapshot.request_count) return false;
          androidAuthBytes = snapshot.categories.auth;
          return true;
        }, { timeout: timeoutMs, interval: 50,
          timeoutMsg: 'mobile_auth_page_meter_timeout' });
        callbackTracker.closePendingFromExternalMeasurement({ pathPrefix: '/auth/v1' });
        wholeCellResponseTracker.closePendingFromExternalMeasurement({ pathPrefix: '/auth/v1' });
      }
      // The issued callback can be a same-origin token_hash bridge while the
      // actual verify request is sent to Supabase. Correlate the closed Auth
      // category, never the callback link's unrelated origin.
      if (callbackTracker.pendingTerminalCount({ pathPrefix: '/auth/v1' }) > 0) {
        try {
          await driver.waitUntil(async () => {
            const logs = await driver.getLogs(networkType).catch(() => null);
            if (!Array.isArray(logs)) return false;
            observePostBoundarySearch(logs);
            callbackTracker.consume(logs);
            return callbackTracker.pendingTerminalCount({ pathPrefix: '/auth/v1' }) === 0;
          }, { timeout: timeoutMs, interval: 100,
            timeoutMsg: 'mobile_auth_terminal_bytes_timeout' });
        } catch {
          const pending = callbackTracker.pendingTerminalSummary({ pathPrefix: '/auth/v1' });
          const pathClass = ['verify', 'user', 'token'].find((key) => pending[key] === pending.total)
            || 'mixed';
          const state = pending.response_seen > 0
            ? (pending.received_data > 0 ? 'response_data_seen' : 'response_seen') : 'request_only';
          throw new Error(`mobile_auth_terminal_bytes_timeout_${pathClass}_${state}`);
        }
      }
      callbackAuthObservedBytes += androidAuthBytes == null
        ? callbackTracker.responses()
          .filter((item) => item.pathname === '/auth/v1' || item.pathname.startsWith('/auth/v1/'))
          .reduce((sum, item) => sum + Number(item.encoded_bytes || 0), 0)
        : Number(androidAuthBytes || 0);
      lifecycle.auth_callback_authorized = true;
      lifecycle.failure_stage = 'search_surface';
    },
    async open(targetUrl) {
      const networkType = platform === 'android' ? 'performance' : 'safariNetwork';
      const initialLogs = await driver.getLogs(networkType).catch(() => null);
      if (!Array.isArray(initialLogs)) throw new Error('mobile_target_network_log_unavailable');
      observePostBoundarySearch(initialLogs);
      accumulateClosedDriverDiagnostics(initialLogs, driverDiagnostics, driverDiagnosticIds,
        driverDiagnosticRequests, wholeCellOrigins);
      await driver.url(targetUrl);
      await driver.waitUntil(async () => (await driver.getUrl()) === targetUrl,
        { timeout: timeoutMs, interval: 250, timeoutMsg: 'search_target_redirected' });
      const responses = [];
      await driver.waitUntil(async () => {
        const logs = await driver.getLogs(networkType).catch(() => null);
        if (!Array.isArray(logs)) return false;
        observePostBoundarySearch(logs);
        accumulateClosedDriverDiagnostics(logs, driverDiagnostics, driverDiagnosticIds,
          driverDiagnosticRequests, wholeCellOrigins);
        responses.push(...extractSanitizedNavigationResponses(logs));
        try {
          lifecycle.target_navigation_receipt = buildExactTargetNavigationReceipt({
            expectedUrl: targetUrl, finalUrl: await driver.getUrl(), responses, networkSource: networkType,
          });
          return true;
        } catch (error) {
          if (String(error?.message) === 'search_target_redirected') throw error;
          return false;
        }
      }, { timeout: timeoutMs, interval: 200, timeoutMsg: 'search_target_http_invalid' });
    },
    async inspectSurface() {
      await driver.waitUntil(async () => driver.execute(() => Boolean(document.querySelector('[data-authorized-search]'))),
        { timeout: timeoutMs, interval: 250, timeoutMsg: 'search_surface_missing' });
      await driver.waitUntil(async () => driver.execute(() => document.querySelector('[data-authorized-search]')?.classList.contains('is-authorized') === true),
        { timeout: timeoutMs, interval: 250, timeoutMsg: 'search_session_not_restored' });
      await driver.execute(installSearchRuntimeProbe, configuredPolicy);
      lifecycle.search_surface_ready = true;
      lifecycle.failure_stage = 'search_journey';
      const inspected = await driver.execute(() => {
        const root = document.querySelector('[data-authorized-search]'); const input = root?.querySelector('[data-search-input]');
        return { enabled: root?.dataset.searchEnabled === 'true', authorized: root?.classList.contains('is-authorized') === true,
          transport: String(root?.dataset.searchTransport || ''), input_tag: String(input?.tagName || '').toLowerCase(),
          enter_key_hint: String(input?.enterKeyHint || input?.getAttribute('enterkeyhint') || ''),
          observer_origins: [root?.dataset.supabaseUrl, root?.dataset.supabaseRelayUrl]
            .filter(Boolean).map((value) => new URL(value, location.href).origin) };
      });
      wholeCellOrigins = [...new Set([...wholeCellOrigins, ...(inspected.observer_origins || [])])];
      delete inspected.observer_origins;
      return inspected;
    },
    async configureRequestPolicy(policy) {
      configuredPolicy = { ...policy };
      await driver.execute(installSearchRuntimeProbe, configuredPolicy);
    },
    async activity() { return driver.execute(snapshotSearchRuntimeProbe); },
    async awaitPhysicalIdle() {
      if (wholeCellOrigins.length < 1) throw new Error('search_physical_observation_missing');
      const networkType = platform === 'android' ? 'performance' : 'safariNetwork';
      const quietMs = Math.max(25, Number(options.physicalQuietMs || 200));
      const deadline = Date.now() + timeoutMs;
      let quietSince = null;
      let previousAndroidClosed = -1;
      const androidObserverActive = platform === 'android' && Boolean(androidPhysicalScriptIdentifier);
      const pending = () => wholeCellOrigins.reduce((sum, origin) => (
        sum + ['/auth/v1', '/functions/v1', '/rest/v1'].reduce((value, prefix) => (
          value + wholeCellResponseTracker.pendingTerminalCount({ origin, pathPrefix: prefix })
        ), 0)
      ), 0);
      while ((androidObserverActive ? true : pending() > 0)
        || quietSince === null || Date.now() - quietSince < quietMs) {
        if (Date.now() >= deadline) throw new Error('search_physical_observation_missing');
        if (typeof driver.pause === 'function') await driver.pause(Math.min(50, quietMs));
        else await new Promise((resolve) => setTimeout(resolve, Math.min(50, quietMs)));
        const logs = await driver.getLogs?.(networkType).catch(() => null);
        if (!Array.isArray(logs)) throw new Error('search_physical_observation_missing');
        observePostBoundarySearch(logs);
        accumulateClosedDriverDiagnostics(logs, driverDiagnostics, driverDiagnosticIds,
          driverDiagnosticRequests, wholeCellOrigins);
        if (androidObserverActive) {
          const snapshot = await snapshotAndroidPhysical({ required: true });
          if (snapshot.failed_count > 0) throw new Error('mobile_android_physical_observer_failed');
          const stable = snapshot.pending_count === 0
            && snapshot.closed_count === snapshot.request_count
            && snapshot.closed_count === previousAndroidClosed;
          previousAndroidClosed = snapshot.closed_count;
          if (!stable) quietSince = null;
          else if (quietSince === null) quietSince = Date.now();
          if (stable && Date.now() - quietSince >= quietMs) break;
        } else if (logs.length > 0) quietSince = null;
        else if (quietSince === null) quietSince = Date.now();
      }
    },
    async physicalActivity() {
      if (wholeCellOrigins.length < 1) throw new Error('search_physical_observation_missing');
      if (platform === 'android' && androidPhysicalScriptIdentifier) {
        await snapshotAndroidPhysical({ required: true });
        const requests = wholeCellResponseTracker.requests()
          .filter((item) => wholeCellOrigins.includes(item.origin));
        return Object.freeze({
          search_posts: Math.max(androidPhysicalTotals.search_posts, wholeCellSearchPostCount),
          storage_requests: Math.max(androidPhysicalTotals.storage_requests,
            requests.filter((item) => item.pathname === '/storage/v1'
              || item.pathname.startsWith('/storage/v1/')).length),
          receipt_rpc_requests: Math.max(androidPhysicalTotals.receipt_rpc_requests,
            requests.filter((item) => /^\/rest\/v1\/rpc\/get_event_search_receipt(?:_|$)/u.test(item.pathname)).length),
          meter: androidMeterSnapshot(),
        });
      }
      const meter = new SupabaseClientObservedByteMeter({ supabaseOrigins: wholeCellOrigins });
      for (const response of wholeCellResponseTracker.responses()) {
        if (!wholeCellOrigins.includes(response.origin)) continue;
        meter.recordResponse({ url: `${response.origin}${response.pathname}`,
          headers: { 'content-length': String(Number(response.encoded_bytes || 0)) }, body: null });
      }
      const requests = wholeCellResponseTracker.requests()
        .filter((item) => wholeCellOrigins.includes(item.origin));
      return Object.freeze({
        search_posts: wholeCellSearchPostCount,
        storage_requests: requests.filter((item) => item.pathname === '/storage/v1'
          || item.pathname.startsWith('/storage/v1/')).length,
        receipt_rpc_requests: requests.filter((item) => /^\/rest\/v1\/rpc\/get_event_search_receipt(?:_|$)/u.test(item.pathname)).length,
        meter: meter.snapshot(),
      });
    },
    async healthDiagnostics() {
      await syncClosedDriverDiagnostics();
      const runtime = await driver.execute(snapshotSearchRuntimeProbe);
      return {
        console_errors: driverDiagnostics.console_errors,
        failed_requests: Math.max(driverDiagnostics.failed_requests,
          Number(runtime?.network?.failed_requests || 0)),
        error_responses: driverDiagnostics.error_responses,
        storage_requests: Math.max(driverDiagnostics.storage_requests,
          Number(runtime?.network?.storage_requests || 0)),
      };
    },
    async verifyAuthenticatedOwner() {
      await driver.execute(installSearchRuntimeProbe, { ...configuredPolicy, production_health: true });
      if (typeof driver.executeAsync !== 'function') {
        throw new Error('mobile_auth_async_script_unavailable');
      }
      // XCUITest/Safari can JSON-clone a pending Promise from Execute Script as
      // an empty object even though Chromium awaits it. Use the WebDriver
      // callback command for this two-request Auth/RLS probe; OTP and Search
      // still share the same browser session and neutral mobile transport.
      const outcome = await driver.executeAsync(verifyAuthenticatedOwnerRuntimeProbe);
      if (outcome?.status === 'failed') throw new Error(outcome.failure_code);
      if (outcome?.status !== 'pass' || !outcome.receipt) {
        throw new Error('mobile_auth_async_script_result_invalid');
      }
      const receipt = outcome.receipt;
      const snapshot = await driver.execute(snapshotSearchRuntimeProbe);
      return { receipt, meter: meterWithCallbackAuthBytes(snapshot.meter) };
    },
    async typeQuery(value) {
      lifecycle.failure_stage = 'search_input_focus';
      const input = await driver.$('[data-search-input]');
      let keyboardShown = false;
      if (platform === 'ios') {
        await driver.execute(() => document.querySelector('[data-search-input]')?.scrollIntoView({ block: 'center', inline: 'center' }));
        await driver.pause(200);
        const attempt = await focusIosSafariWebInput(driver, { labels: IOS_SEARCH_INPUT_LABELS });
        keyboardShown = attempt.keyboard_shown;
      } else {
        await input.click();
        keyboardShown = await withNativeAppContext(driver, () => observeNativeKeyboard(driver));
      }
      if (!keyboardShown) throw new Error(`search_native_keyboard_missing:${platform}`);
      nativeKeyboardObserved = true;
      lifecycle.failure_stage = 'search_input_type';
      await input.setValue(value);
      lifecycle.failure_stage = 'search_input_typed';
    },
    async clearQuery() {
      lifecycle.failure_stage = 'search_input_clear';
      await (await driver.$('[data-search-input]')).clearValue();
    },
    async readQueryState() { return driver.execute(() => ({ length: document.querySelector('[data-search-input]')?.value?.length || 0 })); },
    async submitWithSearchIntent() {
      lifecycle.failure_stage = 'search_submit';
      if (!nativeKeyboardObserved) throw new Error(`search_native_keyboard_hidden:${platform}`);
      await driver.keys('\uE007');
      nativeKeyboardObserved = false;
    },
    async waitForTerminal({ minimumResponseCount = 1, minimumCardCount = 1 } = {}) {
      lifecycle.failure_stage = 'search_terminal';
      let state = null;
      await driver.waitUntil(async () => {
        state = await driver.execute((responseCount, cardCount) => {
          const probe = globalThis.__KENIGEVENTS_SEARCH_HARNESS_V1__; const root = document.querySelector('[data-authorized-search]');
          const status = document.querySelector('[data-search-status]');
          const submit = document.querySelector('[data-search-submit]'); const more = document.querySelector('[data-search-more]');
          const cards = document.querySelectorAll('[data-search-results] [data-event-card][data-event-id], [data-search-results] [data-search-vector-card][data-event-id]');
          return { done: (probe?.responses?.length || 0) >= responseCount
            && (probe?.meter?.pending || 0) === 0 && root?.dataset?.searchTerminal === 'true'
            && cards.length >= cardCount
            && submit?.getAttribute('aria-busy') !== 'true' && more?.getAttribute('aria-busy') !== 'true',
          error: status?.getAttribute('role') === 'alert' };
        }, minimumResponseCount, minimumCardCount);
        return state.done;
      }, { timeout: timeoutMs, interval: 250, timeoutMsg: 'search_terminal_timeout' });
      const snapshot = await adapter.snapshotResults();
      if (state?.error && !snapshot.card_renderer_unavailable) throw new Error('search_ui_terminal_error');
      return snapshot;
    },
    async snapshotResults() { return driver.execute(pageResultSnapshot); },
    async realScrollResults() {
      lifecycle.failure_stage = 'search_scroll_keyboard_dismiss';
      await dismissNativeKeyboard(driver, { allowUnsupported: platform === 'ios',
        fallbackTapLabels: IOS_SEARCH_KEYBOARD_DISMISS_LABELS,
        onFallbackTapProbe: (probe) => { lifecycle.keyboard_dismiss_target_probe = probe; } });
      lifecycle.failure_stage = 'search_scroll';
      return runRealTouchScroll({
        readScrollY: () => driver.execute(() => scrollY),
        gesture: () => performNativeDocumentSwipe(driver, { platform,
          startXRatio: 0.5, startYRatio: 0.72,
          endXRatio: 0.5, endYRatio: 0.28, duration: 450,
        }),
        wait: () => driver.pause(150),
        lastCardVisible: () => driver.execute(() => {
          const cards = document.querySelectorAll('[data-search-results] [data-event-card][data-event-id], [data-search-results] [data-search-vector-card][data-event-id]');
          const card = cards[cards.length - 1]; if (!card) return false; const rect = card.getBoundingClientRect();
          return rect.top < innerHeight && rect.bottom > 0;
        }),
      });
    },
    async openFirstResult() {
      lifecycle.failure_stage = 'search_first_card_capture';
      if (platform === 'android' && androidPhysicalScriptIdentifier) {
        await snapshotAndroidPhysical({ required: true });
        androidPostBoundaryStart = structuredClone(androidPhysicalTotals);
      }
      const captured = await driver.execute(() => {
        const selector = '[data-search-results] [data-event-card][data-event-id], [data-search-results] [data-search-vector-card][data-event-id]';
        const card = document.querySelector(selector);
        const rawHref = card?.getAttribute('data-card-href')
          || card?.querySelector('a[href]')?.getAttribute('href');
        if (!card || !rawHref) return null;
        try {
          const target = new URL(rawHref, location.href);
          const origins = [document.querySelector('[data-authorized-search]')?.dataset?.supabaseUrl,
            document.querySelector('[data-authorized-search]')?.dataset?.supabaseRelayUrl]
            .filter(Boolean).map((value) => new URL(value, location.href).origin);
          return { href: target.href, same_origin: target.origin === location.origin,
            supabase_origins: [...new Set(origins)] };
        } catch { return null; }
      });
      if (!captured?.href || captured.same_origin !== true) throw new Error('mobile_first_card_route_invalid');
      const beforeUrl = await driver.getUrl();
      const expectedUrl = new URL(captured.href, beforeUrl);
      if (expectedUrl.origin !== new URL(beforeUrl).origin) throw new Error('mobile_card_route_cross_origin');
      assertCanonicalCandidateEventDestination({ searchUrl: beforeUrl, eventUrl: expectedUrl.href });
      const logType = platform === 'android' ? 'performance' : 'safariNetwork';
      const initialLogs = await driver.getLogs(logType).catch(() => null);
      if (!Array.isArray(initialLogs)) throw new Error('mobile_navigation_network_log_unavailable');
      accumulateClosedDriverDiagnostics(initialLogs, driverDiagnostics, driverDiagnosticIds,
        driverDiagnosticRequests, wholeCellOrigins);
      const searchPageActivity = await driver.execute(snapshotSearchRuntimeProbe);
      preNavigationSearchActivity = searchPageActivity;
      preNavigationResultState = await driver.execute(pageResultSnapshot);
      postBoundarySearchRequestIds.clear();
      postBoundarySearchPostCount = 0;
      postBoundarySearchObservationActive = true;
      postBoundaryResponseTracker = createSanitizedNavigationResponseTracker();
      postBoundarySupabaseOrigins = Array.isArray(captured.supabase_origins)
        ? captured.supabase_origins.filter((value) => typeof value === 'string') : [];
      postBoundaryMeterSnapshot = null;
      lifecycle.first_card_captured = true;
      lifecycle.failure_stage = 'search_first_card_open';
      await (await driver.$('[data-search-results] [data-event-card][data-event-id], [data-search-results] [data-search-vector-card][data-event-id]')).click();
      await driver.waitUntil(async () => {
        const current = new URL(await driver.getUrl());
        return current.origin === expectedUrl.origin && current.pathname === expectedUrl.pathname;
      }, { timeout: timeoutMs, interval: 200, timeoutMsg: 'mobile_first_card_navigation_timeout' });
      const responses = [];
      await driver.waitUntil(async () => {
        const logs = await driver.getLogs(logType).catch(() => null);
        if (!Array.isArray(logs)) return false;
        accumulateClosedDriverDiagnostics(logs, driverDiagnostics, driverDiagnosticIds,
          driverDiagnosticRequests, wholeCellOrigins);
        observePostBoundarySearch(logs);
        responses.push(...extractSanitizedNavigationResponses(logs));
        return responses.some((item) => item.origin === expectedUrl.origin
          && item.pathname === expectedUrl.pathname && item.status === 200
          && (!item.resource_type || item.resource_type === 'document'));
      }, { timeout: timeoutMs, interval: 200, timeoutMsg: 'mobile_first_card_http_200_timeout' });
      const receipt = buildSameOriginNavigationReceipt({
        beforeUrl, expectedUrl: expectedUrl.href, finalUrl: await driver.getUrl(),
        responses, networkSource: logType,
      });
      lifecycle.first_card_opened = true;
      lifecycle.failure_stage = 'search_first_card_opened';
      return Object.freeze({
        ...receipt,
        search_page_activity_before_navigation: searchPageActivity,
      });
    },
    async postNavigationSearchPostCount() {
      if (!postBoundarySearchObservationActive
        || !Number.isSafeInteger(postBoundarySearchPostCount)) {
        throw new Error('search_post_navigation_observation_missing');
      }
      const networkType = platform === 'android' ? 'performance' : 'safariNetwork';
      const finalNetworkLogs = await driver.getLogs?.(networkType).catch(() => null);
      if (!Array.isArray(finalNetworkLogs)) {
        throw new Error('mobile_post_navigation_network_log_unavailable');
      }
      observePostBoundarySearch(finalNetworkLogs);
      if (platform === 'android' && androidPhysicalScriptIdentifier) {
        const quietMs = Math.max(25, Number(options.postNavigationQuietMs || 200));
        const deadline = Date.now() + timeoutMs;
        let quietSince = null;
        let previousClosed = -1;
        while (quietSince === null || Date.now() - quietSince < quietMs) {
          if (Date.now() >= deadline) throw new Error('mobile_post_navigation_terminal_bytes_missing');
          const snapshot = await snapshotAndroidPhysical({ required: true });
          if (snapshot.failed_count > 0) throw new Error('mobile_post_navigation_meter_failed');
          const stable = snapshot.pending_count === 0
            && snapshot.closed_count === snapshot.request_count
            && snapshot.closed_count === previousClosed;
          previousClosed = snapshot.closed_count;
          if (!stable) quietSince = null;
          else if (quietSince === null) quietSince = Date.now();
          if (typeof driver.pause === 'function') await driver.pause(Math.min(50, quietMs));
          else await new Promise((resolve) => setTimeout(resolve, Math.min(50, quietMs)));
        }
        const delta = subtractAndroidTotals(androidPhysicalTotals, androidPostBoundaryStart || {});
        postBoundarySearchPostCount = Math.max(postBoundarySearchPostCount, delta.search_posts);
        postBoundaryMeterSnapshot = androidMeterSnapshot(delta);
        postBoundarySearchObservationActive = false;
        return postBoundarySearchPostCount;
      }
      const pendingRelevantResponses = () => postBoundarySupabaseOrigins.reduce(
        (sum, origin) => sum + ['/auth/v1', '/functions/v1', '/rest/v1'].reduce(
          (originSum, prefix) => originSum
            + postBoundaryResponseTracker.pendingTerminalCount({ origin, pathPrefix: prefix }), 0,
        ), 0,
      );
      const quietMs = Math.max(25, Number(options.postNavigationQuietMs || 200));
      const pollMs = Math.min(50, quietMs);
      const deadline = Date.now() + timeoutMs;
      let quietSince = finalNetworkLogs.length === 0 ? Date.now() : null;
      while (pendingRelevantResponses() > 0 || quietSince === null
        || Date.now() - quietSince < quietMs) {
        if (Date.now() >= deadline) throw new Error('mobile_post_navigation_terminal_bytes_missing');
        if (typeof driver.pause === 'function') await driver.pause(pollMs);
        else await new Promise((resolve) => setTimeout(resolve, pollMs));
        const logs = await driver.getLogs?.(networkType).catch(() => null);
        if (!Array.isArray(logs)) throw new Error('mobile_post_navigation_network_log_unavailable');
        observePostBoundarySearch(logs);
        if (logs.length > 0) quietSince = null;
        else if (quietSince === null) quietSince = Date.now();
      }
      for (const origin of postBoundarySupabaseOrigins) {
        for (const prefix of ['/auth/v1', '/functions/v1', '/rest/v1']) {
          if (postBoundaryResponseTracker.pendingTerminalCount({ origin, pathPrefix: prefix }) > 0) {
            throw new Error('mobile_post_navigation_terminal_bytes_missing');
          }
        }
      }
      if (postBoundarySupabaseOrigins.length < 1) {
        throw new Error('mobile_post_navigation_meter_origin_missing');
      }
      const meter = new SupabaseClientObservedByteMeter({ supabaseOrigins: postBoundarySupabaseOrigins });
      for (const response of postBoundaryResponseTracker.responses()) {
        if (!postBoundarySupabaseOrigins.includes(response.origin)) continue;
        meter.recordResponse({
          url: `${response.origin}${response.pathname}`,
          headers: { 'content-length': String(Number(response.encoded_bytes || 0)) },
          body: null,
        });
      }
      postBoundaryMeterSnapshot = meter.snapshot();
      postBoundarySearchObservationActive = false;
      return postBoundarySearchPostCount;
    },
    async postNavigationMeterSnapshot() {
      if (!postBoundaryMeterSnapshot) throw new Error('mobile_post_navigation_meter_missing');
      return structuredClone(postBoundaryMeterSnapshot);
    },
    async failedJourneyEvidence() {
      if (!preNavigationSearchActivity) return null;
      let count = Number(postBoundarySearchPostCount || 0);
      if (postBoundarySearchObservationActive) {
        const networkType = platform === 'android' ? 'performance' : 'safariNetwork';
        const logs = await driver.getLogs?.(networkType).catch(() => null);
        if (Array.isArray(logs)) observePostBoundarySearch(logs);
        count = Number(postBoundarySearchPostCount || 0);
        postBoundarySearchObservationActive = false;
      }
      return Object.freeze({
        activity: structuredClone(preNavigationSearchActivity),
        results: structuredClone(preNavigationResultState),
        post_navigation_search_post_count: count,
        ...(postBoundaryMeterSnapshot
          ? { post_navigation_meter: structuredClone(postBoundaryMeterSnapshot) } : {}),
      });
    },
    async showMoreState() {
      return driver.execute(() => { const node = document.querySelector('[data-search-more]');
        return { visible: Boolean(node && !node.hidden && getComputedStyle(node).display !== 'none'), enabled: Boolean(node && !node.disabled) }; });
    },
    async activateShowMore() { await (await driver.$('[data-search-more]')).click(); },
    async waitForValidation() {
      lifecycle.failure_stage = 'search_validation';
      await driver.waitUntil(async () => driver.execute(() => {
        const status = document.querySelector('[data-search-status]'); const submit = document.querySelector('[data-search-submit]');
        return status?.getAttribute('role') === 'alert' && submit?.getAttribute('aria-busy') !== 'true';
      }), { timeout: Math.min(timeoutMs, 10_000), interval: 100, timeoutMsg: 'search_validation_timeout' });
      return { visible: true, kind: 'error' };
    },
    async close() {
      if (closed) return { auth_local_purge_confirmed: true, webdriver_session_deleted: true };
      lifecycle.failure_stage = 'auth_local_purge';
      let purged = false;
      try { purged = await purgeLocalAuthState(); } catch { purged = false; }
      if (platform === 'android' && androidPhysicalScriptIdentifier) {
        try {
          await driver.executeCdp?.('Page.removeScriptToEvaluateOnNewDocument', {
            identifier: androidPhysicalScriptIdentifier,
          });
        } catch { /* session teardown is authoritative */ }
        androidPhysicalScriptIdentifier = '';
      }
      let deleted = false;
      try { deleted = await deleteDriverSession(); } finally {
        lifecycle.auth_local_purge_confirmed = purged;
        lifecycle.webdriver_client_session_deleted = deleted;
        lifecycle.failure_stage = 'closed';
      }
      if (!purged) throw new Error('mobile_auth_local_purge_unconfirmed');
      return { auth_local_purge_confirmed: true, webdriver_session_deleted: deleted };
    },
  };
  return adapter;
}
