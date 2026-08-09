#!/usr/bin/env node
import { execFile as execFileCallback } from 'node:child_process';
import { appendFile } from 'node:fs/promises';
import { promisify } from 'node:util';
import { pathToFileURL } from 'node:url';
import { resolve } from 'node:path';

import {
  PRODUCTION_HEALTH_RESULTS,
  classifyProductionHealthOutcome,
} from './production-health-contract.mjs';
import {
  SupabaseClientObservedByteMeter,
  createSupabaseMeteredFetch,
  mergeSupabaseClientByteSnapshots,
} from './production-health-meter.mjs';
import {
  acceptedTargetImmutableEvidence,
  createAcceptedTargetRun,
  currentReviewCliResultToAcceptedTargetInput,
  normalizeAcceptedTargetResolverResult,
} from './production-health-target.mjs';
import {
  productionHealthEvidenceRecord,
  writeProductionHealthEvidence,
} from './evidence.mjs';
import { runProductionHealthJourney } from './production-health-journey.mjs';
import { waitForActiveSearchBackend } from './search-backend-release-probe.mjs';

const execFile = promisify(execFileCallback);
const platforms = new Set(['browser', 'android', 'ios']);
const SAFE_DEPLOYMENT_RUN_ID = /^[A-Za-z0-9][A-Za-z0-9._:-]{2,127}$/u;

export function githubMaskCommand(value) {
  const secret = String(value || '');
  if (!secret || /[\r\n]/u.test(secret)) throw new Error('search_health_mask_value_invalid');
  return `::add-mask::${secret}`;
}

function maskGitHubSecret(value, env = process.env) {
  if (String(env.GITHUB_ACTIONS || '').toLowerCase() !== 'true') return;
  process.stdout.write(`${githubMaskCommand(value)}\n`);
}

async function emitFailureClassOutput(env, failureClass) {
  const path = String(env.GITHUB_OUTPUT || '').trim();
  if (!path) return;
  const value = String(failureClass || '');
  if (value && !PRODUCTION_HEALTH_RESULTS[value]) throw new Error('search_health_output_failure_class_invalid');
  await appendFile(path, `failure_class=${value}\n`, { encoding: 'utf8' });
}

const emptyMeter = () => mergeSupabaseClientByteSnapshots();
const required = (env, name) => {
  const value = String(env[name] || '').trim();
  if (!value) throw new Error(`missing_${name.toLowerCase()}`);
  return value;
};

function assertPreflight(receipt, platform) {
  if (!receipt || receipt.side_effect_free !== true
    || receipt.transport_ready !== true || receipt.viewport_ready !== true
    || (platform === 'browser' && receipt.browser_ready !== true)
    || Number(receipt.auth_requests || 0) !== 0
    || Number(receipt.search_posts || 0) !== 0
    || Number(receipt.otp_requests || 0) !== 0
    || Number(receipt.supabase_requests || 0) !== 0) {
    throw new Error('search_health_preflight_not_side_effect_free');
  }
}

export function assertProductionHealthAuthReceipt(receipt) {
  if (!receipt || receipt.get_user_verified !== true || receipt.protected_probe_verified !== true
    || Number(receipt.protected_probe_request_count) !== 1
    || Number(receipt.product_otp_issue_count) !== 0
    || Number(receipt.external_mail_send_count) !== 0
    || Number(receipt.external_mail_receipt_count) !== 0
    || receipt.real_mail_fallback !== 'forbidden') {
    throw new Error('search_health_auth_receipt_invalid');
  }
}

async function attachIssuedSession(adapter, issued, targetUrl) {
  if (typeof issued?.attach === 'function') {
    await issued.attach(adapter, targetUrl);
    return;
  }
  if (issued?.storageStatePath && typeof adapter?.restoreSessionState === 'function') {
    await adapter.restoreSessionState(issued.storageStatePath);
    return;
  }
  if (issued?.actionLink && typeof adapter?.bootstrapSession === 'function') {
    await adapter.bootstrapSession(issued.actionLink, targetUrl);
    return;
  }
  throw new Error('search_health_session_attach_missing');
}

const infraFailure = (platform) => ({
  browser: PRODUCTION_HEALTH_RESULTS.UNKNOWN_RUNNER_BROWSER,
  android: PRODUCTION_HEALTH_RESULTS.UNKNOWN_ANDROID_INFRA,
  ios: PRODUCTION_HEALTH_RESULTS.UNKNOWN_IOS_INFRA,
})[platform];

const combinedObservedMeter = (issued, journey) => {
  const authMeter = typeof issued?.meterSnapshot === 'function' ? issued.meterSnapshot() : issued?.meter;
  return Promise.resolve(authMeter).then((value) => (
    mergeSupabaseClientByteSnapshots(value || emptyMeter(), journey?.meter || emptyMeter())
  ));
};

const preferMoreCompleteMeter = (current, candidate) => {
  const currentBytes = Number(current?.total_bytes || 0);
  const candidateBytes = Number(candidate?.total_bytes || 0);
  return candidateBytes >= currentBytes ? candidate : current;
};

const searchRequestsFromRuntime = (runtime) => (Array.isArray(runtime?.requests) ? runtime.requests : [])
  .filter((item) => item?.method === 'POST' && item?.path === '/functions/v1/event-search');

async function retainFailedJourneyEvidence(adapter, runtime = {}) {
  let persistent = null;
  let physical = null;
  try { persistent = await adapter?.failedJourneyEvidence?.() || null; } catch { /* closed fallback below */ }
  try { physical = await adapter?.physicalActivity?.() || null; } catch { /* best-known closed counters below */ }
  const authoritativeRuntime = persistent?.activity || runtime;
  const requests = searchRequestsFromRuntime(authoritativeRuntime);
  const responses = Array.isArray(authoritativeRuntime?.responses) ? authoritativeRuntime.responses : [];
  const response = responses.at(-1) || {};
  let state = persistent?.results || {};
  let diagnostics = {};
  if (!persistent?.results) {
    try { state = await adapter?.snapshotResults?.() || {}; } catch { /* closed counters only */ }
  }
  try { diagnostics = await adapter?.healthDiagnostics?.() || {}; } catch { /* closed counters only */ }
  const renderedIds = Array.isArray(state.rendered_ids) ? state.rendered_ids.map(String) : [];
  const responseIds = Array.isArray(response.response_ids) ? response.response_ids.map(String) : [];
  const postNavigationSearchPostCount = Number(persistent?.post_navigation_search_post_count || 0);
  const fallbackPhysicalSearchPostCount = requests.length
    + (Number.isSafeInteger(postNavigationSearchPostCount) && postNavigationSearchPostCount >= 0
      ? postNavigationSearchPostCount : 0);
  const observedPhysicalSearchPostCount = Number(physical?.search_posts);
  const physicalSearchPostCount = Number.isSafeInteger(observedPhysicalSearchPostCount)
    && observedPhysicalSearchPostCount >= 0
    ? observedPhysicalSearchPostCount : fallbackPhysicalSearchPostCount;
  let retainedMeter = authoritativeRuntime.meter || emptyMeter();
  if (persistent?.post_navigation_meter) {
    try {
      retainedMeter = mergeSupabaseClientByteSnapshots(
        retainedMeter, persistent.post_navigation_meter,
      );
    } catch { /* retain the last independently valid Search-page meter */ }
  }
  return {
    schema_version: 'search_production_health_failed_journey_v1',
    search_post_count: requests.length,
    physical_search_post_count: physicalSearchPostCount,
    request_contract: requests[0]?.body_contract || {},
    cache_state: response.result_cache_status,
    response_telemetry: {
      request_id: response.request_id, http_status: response.http_status, route: response.route,
      search_contract_version: response.search_contract_version,
      catalog_revision: response.catalog_revision, corpus_revision: response.corpus_revision,
      search_document_revision: response.search_document_revision,
    },
    provider_attempts: response.provider_attempts || {},
    response_ids: responseIds, rendered_ids: renderedIds,
    card_count: renderedIds.length, terminal_ui: state.terminal === true,
    forbidden_activity: {
      llm_calls: Number(response.provider_attempts?.llm || 0),
      pagination_requests: Math.max(0, requests.length - 1),
      receipt_rpc_calls: Number(physical?.receipt_rpc_requests
        ?? authoritativeRuntime.network?.receipt_rpc_requests ?? 0),
      storage_image_requests: Number(physical?.storage_requests
        ?? authoritativeRuntime.network?.storage_requests ?? 0),
    },
    diagnostics: {
      console_errors: Number(diagnostics.console_errors || 0),
      failed_requests: Number(diagnostics.failed_requests || authoritativeRuntime.network?.failed_requests || 0),
      error_responses: Number(diagnostics.error_responses || 0),
    },
    meter: retainedMeter,
  };
}

function journeyFailure(error) {
  const code = String(error?.message || '').split(':')[0];
  if (/surface|input|enter_key/u.test(code)) return PRODUCTION_HEALTH_RESULTS.BROKEN_SEARCH_SURFACE;
  if (/auth_receipt|not_authorized|authenticated_owner/u.test(code)) return PRODUCTION_HEALTH_RESULTS.BROKEN_AUTH_INTEGRATION;
  if (/no_results/u.test(code)) return PRODUCTION_HEALTH_RESULTS.BROKEN_NO_RESULTS;
  if (/render|cards|response_rendered|duplicate/u.test(code)) return PRODUCTION_HEALTH_RESULTS.BROKEN_RESULT_RENDER;
  if (/event_route|real_scroll|route_/u.test(code)) return PRODUCTION_HEALTH_RESULTS.BROKEN_RESULT_ROUTE;
  if (/hard_limit/u.test(code)) return PRODUCTION_HEALTH_RESULTS.COST_GUARD_FAILED;
  return PRODUCTION_HEALTH_RESULTS.BROKEN_SEARCH_REQUEST;
}

function isAdapterInfrastructureFailure(error) {
  const message = String(error?.message || '');
  return /^(?:search_browser_(?:crashed|session_lost)(?:_[a-z0-9]+)*|mobile_[a-z0-9_]*(?:network_log_unavailable|diagnostics_unavailable|session_lost)|webdriver_session_error)$/iu.test(message)
    || /(?:mobile_auth_terminal_bytes_timeout|mobile_post_navigation_(?:terminal_bytes_missing|meter_(?:origin_)?missing)|search_(?:post_navigation_meter_(?:failed|origin_missing|missing)|post_navigation_observation_missing|physical_observation_missing))/iu.test(message)
    || /(?:invalid session id|no such window|target page, context or browser has been closed|browser has been closed|web view not found)/iu.test(message);
}

/**
 * Callable platform-cell orchestration. Adapter construction and its
 * side-effect-free preflight happen before issuance; the same adapter then
 * consumes the issued session and performs exactly one UI Search.
 */
export async function runProductionHealthCell(options = {}) {
  const platform = String(options.platform || '').toLowerCase();
  if (!platforms.has(platform)) throw new Error('search_health_platform_unknown');
  if (typeof options.targetRun?.pin !== 'function' || typeof options.targetRun?.observeSupersession !== 'function') {
    throw new Error('search_health_target_run_missing');
  }
  if (typeof options.createAdapter !== 'function' || typeof options.issueSession !== 'function') {
    throw new Error('search_health_orchestration_hook_missing');
  }

  const target = await options.targetRun.pin();
  const testedAt = new Date(typeof options.now === 'function' ? options.now() : Date.now()).toISOString();
  const targetImmutable = acceptedTargetImmutableEvidence(target);
  const releaseActive = typeof options.releaseGate === 'function'
    ? await options.releaseGate(target) : true;
  let adapter = null;
  let issued = null;
  let preflight = {};
  let auth = {};
  let journey = {};
  let meter = emptyMeter();
  let targetSuperseded = false;
  let pointerRereadAttempted = false;
  let failureClass = releaseActive === true ? null : PRODUCTION_HEALTH_RESULTS.BLOCKED_RELEASE_NOT_ACTIVE;
  let phase = releaseActive === true ? 'preflight' : 'release_gate';
  let cleanupStatus = 'PENDING';

  try {
    if (!failureClass) {
      adapter = await options.createAdapter({ platform, target });
      if (typeof adapter?.preflight !== 'function') throw new Error('search_health_preflight_missing');
      preflight = await adapter.preflight();
      assertPreflight(preflight, platform);
      phase = 'issuance';
      issued = await options.issueSession({ platform, target, adapter });
      // From this point onward failures are product Auth/session-integration
      // evidence, not broker admission failures.
      phase = 'auth';
      await attachIssuedSession(adapter, issued, target.navigationUrl());
      if (typeof issued?.verifyAuth === 'function') {
        const verified = await issued.verifyAuth(adapter);
        auth = verified?.receipt || verified || {};
        if (verified?.meter) issued.verifiedMeter = verified.meter;
      } else {
        auth = issued?.authReceipt || issued?.receipt || {};
      }
      assertProductionHealthAuthReceipt(auth);
      const preSearchMeter = typeof issued?.meterSnapshot === 'function'
        ? await issued.meterSnapshot() : issued?.meter;
      if (preSearchMeter?.hard_limit_exceeded === true) {
        throw new Error('search_health_supabase_hard_limit_exceeded');
      }
      phase = 'journey';
      journey = await runProductionHealthJourney({ adapter, targetUrl: target.navigationUrl() });
      meter = preferMoreCompleteMeter(meter, await combinedObservedMeter(issued, journey));
      if (meter.hard_limit_exceeded) failureClass = PRODUCTION_HEALTH_RESULTS.COST_GUARD_FAILED;
      phase = 'pointer_reread';
      pointerRereadAttempted = true;
      const supersession = await options.targetRun.observeSupersession();
      targetSuperseded = supersession.target_superseded === true;
    }
  } catch (error) {
    if (isAdapterInfrastructureFailure(error)) failureClass = infraFailure(platform);
    else if (phase === 'preflight') failureClass = infraFailure(platform);
    else if (phase === 'issuance') failureClass = PRODUCTION_HEALTH_RESULTS.UNKNOWN_AUTH_BROKER;
    else if (phase === 'auth') failureClass = PRODUCTION_HEALTH_RESULTS.BROKEN_AUTH_INTEGRATION;
    else if (phase === 'pointer_reread') failureClass = PRODUCTION_HEALTH_RESULTS.BLOCKED_RELEASE_NOT_ACTIVE;
    else failureClass = journeyFailure(error);
    try {
      let runtime = {};
      try { runtime = await adapter?.activity?.() || {}; } catch { /* persistent receipt may survive */ }
      const retained = await retainFailedJourneyEvidence(adapter, runtime);
      if (retained.search_post_count > 0 || runtime?.meter) journey = retained;
      const observed = await combinedObservedMeter(issued, retained);
      meter = preferMoreCompleteMeter(meter, observed);
      if (meter.hard_limit_exceeded) failureClass = PRODUCTION_HEALTH_RESULTS.COST_GUARD_FAILED;
    } catch { /* the closed failure class is sufficient */ }
  } finally {
    if (releaseActive === true && !pointerRereadAttempted) {
      pointerRereadAttempted = true;
      try {
        const supersession = await options.targetRun.observeSupersession();
        targetSuperseded = supersession.target_superseded === true;
      } catch {
        if (!failureClass || String(failureClass).startsWith('BROKEN_')) {
          failureClass = PRODUCTION_HEALTH_RESULTS.BLOCKED_RELEASE_NOT_ACTIVE;
        }
      }
    }
    try { await adapter?.close?.(); } catch { cleanupStatus = 'FAIL'; }
    try {
      if (typeof issued?.cleanup === 'function') await issued.cleanup();
    } catch { cleanupStatus = 'FAIL'; }
    if (cleanupStatus !== 'FAIL') cleanupStatus = 'PASS';
    try {
      const observed = await combinedObservedMeter(issued, journey);
      meter = preferMoreCompleteMeter(meter, observed);
      if (meter.hard_limit_exceeded) failureClass = PRODUCTION_HEALTH_RESULTS.COST_GUARD_FAILED;
    } catch { /* keep the earlier closed meter/failure result */ }
    if (cleanupStatus === 'FAIL' && (!failureClass || String(failureClass).startsWith('BROKEN_'))) {
      failureClass = infraFailure(platform);
    }
  }
  return finalize();

  async function finalize() {
    const outcome = classifyProductionHealthOutcome({ failureClass, platform });
    const value = {
      platform, ...outcome, target_immutable: targetImmutable, target_superseded: targetSuperseded,
      expected_search_backend_revision: options.expectedSearchBackendRevision || null,
      preflight, auth, journey, meter, cleanup_status: cleanupStatus,
      workflow_run_id: options.workflowRunId,
      tested_at: testedAt,
    };
    if (options.evidenceDirectory) {
      const written = await writeProductionHealthEvidence(options.evidenceDirectory, value);
      return written.record;
    }
    return productionHealthEvidenceRecord(value);
  }
}

export function hasActiveSearchReleaseReceipt({
  siteActive, expectedSearchBackendRevision = '', deploymentRunId = '', backendActive = false,
} = {}) {
  if (siteActive !== true) return false;
  const expected = String(expectedSearchBackendRevision || '').trim();
  if (!expected) return true;
  return /^[A-Za-z0-9][A-Za-z0-9._:-]{0,95}$/u.test(expected)
    && SAFE_DEPLOYMENT_RUN_ID.test(String(deploymentRunId || '').trim())
    && backendActive === true;
}

export async function resolveCurrentAcceptedTargetFromFly(env = process.env) {
  const flyctl = String(env.FLYCTL_BIN || 'flyctl');
  const app = required(env, 'FLY_APP_NAME');
  const command = 'python3 scripts/request_static_site_build.py --db /data/db.sqlite --show-current-review';
  const { stdout } = await execFile(flyctl, [
    'ssh', 'console', '--app', app, '--pty=false', '--command', command,
  ], { env, maxBuffer: 1024 * 1024, timeout: 30_000 });
  const rows = String(stdout).split('\n').map((line) => line.trim()).filter((line) => line.startsWith('{'));
  let current = null;
  for (const row of rows) {
    try { current = JSON.parse(row); } catch { /* ignore non-JSON flyctl framing */ }
  }
  if (!current) throw new Error('search_health_current_review_resolver_invalid');
  return normalizeAcceptedTargetResolverResult(currentReviewCliResultToAcceptedTargetInput(current));
}

/** Bounded pre-Auth/Search wait for an explicitly deployed site runtime. */
export async function resolveExpectedAcceptedTarget({
  resolver,
  expectedSiteSha = '',
  maxAttempts = 6,
  delayMs = 10_000,
  sleep = (milliseconds) => new Promise((resolveSleep) => setTimeout(resolveSleep, milliseconds)),
} = {}) {
  if (typeof resolver !== 'function') throw new Error('search_health_target_resolver_missing');
  if (!Number.isSafeInteger(maxAttempts) || maxAttempts < 1 || maxAttempts > 12) {
    throw new Error('search_health_release_wait_attempts_invalid');
  }
  if (!Number.isSafeInteger(delayMs) || delayMs < 0 || delayMs > 30_000) {
    throw new Error('search_health_release_wait_delay_invalid');
  }
  let target;
  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    target = await resolver();
    if (!expectedSiteSha || target.target_repo_sha === expectedSiteSha) {
      return Object.freeze({ target, active: true, attempts: attempt });
    }
    if (attempt < maxAttempts) await sleep(delayMs);
  }
  return Object.freeze({ target, active: false, attempts: maxAttempts });
}

const acceptedTargetInput = (normalized) => ({
  source: normalized.source,
  target_url: normalized.navigationUrl(),
  target_repo_sha: normalized.target_repo_sha,
  ...normalized.immutable_identity,
});

async function githubOidcToken(env, fetchImpl) {
  const endpoint = new URL(required(env, 'ACTIONS_ID_TOKEN_REQUEST_URL'));
  endpoint.searchParams.set('audience', required(env, 'AUTH_SESSION_BROKER_OIDC_AUDIENCE'));
  const response = await fetchImpl(endpoint, {
    redirect: 'error', headers: { authorization: `Bearer ${required(env, 'ACTIONS_ID_TOKEN_REQUEST_TOKEN')}` },
  });
  if (!response.ok) throw new Error(`github_oidc_rejected_${response.status}`);
  const token = String((await response.json())?.value || '').trim();
  if (!token || token.includes('\n')) throw new Error('github_oidc_invalid');
  return token;
}

async function protectedOwnerProbe({ fetchImpl, userId, supabaseUrl, accessToken, publishableKey }) {
  const endpoint = new URL('/rest/v1/user_saved_event', supabaseUrl);
  endpoint.searchParams.set('select', 'user_id');
  endpoint.searchParams.set('user_id', `eq.${userId}`);
  endpoint.searchParams.set('limit', '1');
  const response = await fetchImpl(endpoint, {
    method: 'GET', headers: { accept: 'application/json', apikey: publishableKey, authorization: `Bearer ${accessToken}` },
  });
  if (!response.ok) return false;
  const rows = await response.json();
  return Array.isArray(rows) && rows.every((row) => String(row?.user_id || '') === userId);
}

export async function createBuiltInBrowserHooks(env, meter, dependencies = {}) {
  const [adapterModule, fixtureModule] = await Promise.all([
    dependencies.adapterModule || import('./adapters/playwright.mjs'),
    dependencies.fixtureModule || import('../auth-session-fixture/session-fixture.mjs'),
  ]);
  const fetchImpl = dependencies.fetchImpl || globalThis.fetch.bind(globalThis);
  const supabaseUrl = required(env, 'PERSONALIZATION_SUPABASE_URL');
  const publishableKey = required(env, 'PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY');
  const meteredFetch = createSupabaseMeteredFetch(fetchImpl, meter);
  return {
    async createAdapter() {
      return adapterModule.createPlaywrightSearchAdapter({
        browserName: env.E2E_SEARCH_BROWSER || 'chromium', headless: env.E2E_HEADLESS !== '0',
        timeoutMs: env.E2E_SEARCH_TIMEOUT_MS, productionHealth: true,
        supabaseOrigins: [supabaseUrl, env.PERSONALIZATION_SUPABASE_RELAY_URL].filter(Boolean),
      });
    },
    async issueSession({ platform, target }) {
      const purpose = required(env, 'E2E_SEARCH_HEALTH_MODE');
      if (!['production_health', 'release_qualification'].includes(purpose)) {
        throw new Error('search_health_session_purpose_invalid');
      }
      const oidc = dependencies.oidcToken || await githubOidcToken(env, fetchImpl);
      const issuer = fixtureModule.createAuthSessionBrokerIssuer({
        endpoint: required(env, 'AUTH_SESSION_BROKER_URL'), oidcToken: oidc,
      });
      const releaseQualification = purpose === 'release_qualification';
      const personaId = releaseQualification ? 'search-cold-browser' : 'search-cached-browser';
      const personaEmail = releaseQualification
        ? required(env, 'SEARCH_E2E_PERSONA_EMAIL_COLD_BROWSER')
        : required(env, 'SEARCH_E2E_PERSONA_EMAIL_CACHED_BROWSER');
      const fixture = await fixtureModule.createAuthSessionFixture({
        authMode: 'session_fixture', realMailFallback: false, issuer, purpose, platform,
        supabaseUrl, publishableKey, fetchImpl: meteredFetch,
        targetUrl: target.navigationUrl(), allowedOrigins: ['https://kenigevents.ru'],
        personaId, personas: { [personaId]: { email: personaEmail } },
        scopeKind: 'job', scopeId: `search-health-browser-${required(env, 'GITHUB_RUN_ID')}-${env.GITHUB_RUN_ATTEMPT || '1'}`,
        runId: required(env, 'GITHUB_RUN_ID'), protectedProbe: protectedOwnerProbe,
      });
      return {
        authReceipt: fixture.receipt, storageStatePath: fixture.storageStatePath,
        meterSnapshot: () => meter.snapshot(), cleanup: fixture.cleanup,
      };
    },
  };
}

async function externalPlatformHooks(env, platform, meter) {
  const modulePath = required(env, 'E2E_SEARCH_PLATFORM_HOOK_MODULE');
  const imported = await import(pathToFileURL(resolve(modulePath)).href);
  if (typeof imported.createProductionHealthPlatformHooks !== 'function') {
    throw new Error('search_health_platform_hook_invalid');
  }
  return imported.createProductionHealthPlatformHooks({ env, platform, meter });
}

export async function createBuiltInMobileHooks(env, platform, dependencies = {}) {
  if (!['android', 'ios'].includes(platform)) throw new Error('search_health_mobile_platform_invalid');
  const [adapterModule, fixtureModule] = await Promise.all([
    dependencies.adapterModule || import(platform === 'android' ? './adapters/appium-android.mjs' : './adapters/appium-ios.mjs'),
    dependencies.fixtureModule || import('../auth-session-fixture/session-fixture.mjs'),
  ]);
  const fetchImpl = dependencies.fetchImpl || globalThis.fetch.bind(globalThis);
  const oidc = async () => dependencies.oidcToken || githubOidcToken(env, fetchImpl);
  const common = {
    hostname: env.E2E_APPIUM_HOST || '127.0.0.1',
    port: Number(env.E2E_APPIUM_PORT || 4723), path: env.E2E_APPIUM_PATH || '/wd/hub',
    deviceName: env.E2E_DEVICE_NAME,
    platformVersion: env.E2E_PLATFORM_VERSION, timeoutMs: env.E2E_SEARCH_TIMEOUT_MS,
    appiumLogPath: env.E2E_APPIUM_LOG_PATH,
    supabaseOrigins: [env.PERSONALIZATION_SUPABASE_URL,
      env.PERSONALIZATION_SUPABASE_RELAY_URL].filter(Boolean),
    env,
  };
  return {
    async createAdapter() {
      return platform === 'android'
        ? adapterModule.createAndroidSearchAdapter(common)
        : adapterModule.createIosSearchAdapter({
          ...common, udid: env.E2E_DEVICE_UDID, prebuiltWdaPath: env.E2E_PREBUILT_WDA_PATH,
        });
    },
    async issueSession({ target }) {
      const issuer = fixtureModule.createAuthSessionBrokerIssuer({
        endpoint: required(env, 'AUTH_SESSION_BROKER_URL'), oidcToken: await oidc(),
      });
      const credential = await issuer.issue({
        purpose: 'production_health', personaId: `search-cached-${platform}`, platform,
        redirectTo: target.navigationUrl(), runId: required(env, 'GITHUB_RUN_ID'),
      });
      let callback = fixtureModule.createBrowserVerificationCallback({
        actionLink: credential.actionLink, redirectTo: target.navigationUrl(),
      });
      // Mask both values before the first WebDriver navigation. The raw broker
      // link never leaves this process; only the callback is sent to Appium.
      maskGitHubSecret(credential.actionLink, env);
      maskGitHubSecret(callback, env);
      credential.emailOtp = '';
      credential.actionLink = '';
      const issued = {
        actionLink: callback,
        async verifyAuth(adapter) {
          if (typeof adapter?.verifyAuthenticatedOwner !== 'function') {
            throw new Error('search_health_authenticated_owner_probe_missing');
          }
          const verified = await adapter.verifyAuthenticatedOwner();
          issued.verifiedMeter = verified?.meter || null;
          return verified;
        },
        meterSnapshot: () => issued.verifiedMeter || emptyMeter(),
        async cleanup() { callback = ''; issued.actionLink = ''; },
      };
      return issued;
    },
  };
}

export async function runProductionHealthCli(env = process.env) {
  const platform = String(env.E2E_SEARCH_PLATFORM || 'browser').toLowerCase();
  if (!platforms.has(platform)) throw new Error('search_health_platform_unknown');
  const expectedSiteSha = String(env.E2E_EXPECTED_SITE_RUNTIME_SHA || '').trim().toLowerCase();
  if (expectedSiteSha && !/^[0-9a-f]{40}$/u.test(expectedSiteSha)) {
    throw new Error('search_health_expected_site_runtime_sha_invalid');
  }
  const expectedBackendRevision = String(env.E2E_EXPECTED_SEARCH_BACKEND_REVISION || '').trim();
  const deploymentRunId = String(env.E2E_DEPLOYMENT_RUN_ID || '').trim();
  if (expectedBackendRevision && !/^[A-Za-z0-9][A-Za-z0-9._:-]{0,95}$/u.test(expectedBackendRevision)) {
    throw new Error('search_health_expected_backend_revision_invalid');
  }
  if (deploymentRunId && !SAFE_DEPLOYMENT_RUN_ID.test(deploymentRunId)) {
    throw new Error('search_health_deployment_run_id_invalid');
  }
  const initialResolution = await resolveExpectedAcceptedTarget({
    resolver: () => resolveCurrentAcceptedTargetFromFly(env),
    expectedSiteSha,
  });
  // The accepted review URL is private. Register it with the runner before an
  // adapter or Appium command can observe it.
  maskGitHubSecret(initialResolution.target.navigationUrl(), env);
  const backendReceipt = expectedBackendRevision
    ? await waitForActiveSearchBackend({
      supabaseUrl: required(env, 'PERSONALIZATION_SUPABASE_URL'),
      publishableKey: required(env, 'PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY'),
      expectedRevision: expectedBackendRevision,
    })
    : { active: true, product_search_posts: 0, auth_requests: 0 };
  let firstRead = true;
  const targetRun = createAcceptedTargetRun(async () => {
    if (firstRead) {
      firstRead = false;
      return acceptedTargetInput(initialResolution.target);
    }
    return acceptedTargetInput(await resolveCurrentAcceptedTargetFromFly(env));
  });
  const meter = new SupabaseClientObservedByteMeter({ supabaseOrigins: [required(env, 'PERSONALIZATION_SUPABASE_URL')] });
  const hooks = platform === 'browser'
    ? await createBuiltInBrowserHooks(env, meter)
    : env.E2E_SEARCH_PLATFORM_HOOK_MODULE
      ? await externalPlatformHooks(env, platform, meter)
      : await createBuiltInMobileHooks(env, platform);
  const result = await runProductionHealthCell({
    platform, targetRun, createAdapter: hooks.createAdapter, issueSession: hooks.issueSession,
    releaseGate: () => hasActiveSearchReleaseReceipt({
      siteActive: initialResolution.active, expectedSearchBackendRevision: expectedBackendRevision,
      deploymentRunId, backendActive: backendReceipt.active,
    }),
    expectedSearchBackendRevision: expectedBackendRevision || null,
    evidenceDirectory: required(env, 'E2E_EVIDENCE_DIR'),
    workflowRunId: required(env, 'GITHUB_RUN_ID'),
  });
  await emitFailureClassOutput(env, result.failure_class);
  process.stdout.write(`${JSON.stringify({
    schema_version: 'search_production_health_cli_v1', platform: result.platform,
    product_health: result.product_health, execution_status: result.execution_status,
    failure_class: result.failure_class, target_repo_sha: result.target.target_repo_sha,
    target_superseded: result.target.target_superseded,
    physical_post_count: result.search.physical_post_count,
    observed_supabase_bytes: result.supabase_observed_bytes.total_bytes,
    redaction_status: result.redaction.status,
  })}\n`);
  return result.execution_status === 'PASS' ? 0 : 1;
}

const isMain = process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href;
if (isMain) {
  runProductionHealthCli().then((code) => { process.exitCode = code; }).catch(async (error) => {
    const code = String(error?.message || 'search_health_runner_failed').split(':')[0];
    const platform = String(process.env.E2E_SEARCH_PLATFORM || 'browser').toLowerCase();
    const failureClass = code.startsWith('search_evidence_')
      ? PRODUCTION_HEALTH_RESULTS.EVIDENCE_REDACTION_FAILED
      : infraFailure(platform) || PRODUCTION_HEALTH_RESULTS.UNKNOWN_RUNNER_BROWSER;
    await emitFailureClassOutput(process.env, failureClass).catch(() => {});
    process.stderr.write(`${/^[a-z0-9_.-]{3,96}$/iu.test(code) ? code : 'search_health_runner_failed'}\n`);
    process.exitCode = 1;
  });
}
