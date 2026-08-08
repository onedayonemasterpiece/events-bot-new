#!/usr/bin/env node
import { unlink } from 'node:fs/promises';

import { resolveCanaryVariant } from './canary-manifest.mjs';
import { assertSearchRevisionPolicy } from './acceptance.mjs';
import { writeSearchEvidence, sanitizedTargetPath } from './evidence.mjs';
import { runSearchJourney } from './journey.mjs';
import { readPriorMobileStartupReceipt } from './mobile-startup-retry.mjs';

const closedModes = new Set(['cached_vector', 'cold_vector', 'cold_vector_llm', 'degraded_vector_fallback']);
const defaultQueries = [
  { id: 'family_nature_v1', value: 'На природу с детьми', paginate: true },
  { id: 'seaside_art_v1', value: 'искусство у моря', paginate: false },
  { id: 'friday_free_v1', value: 'в пятницу бесплатно', paginate: false },
];

function targetUrl(value) {
  let url;
  try { url = new URL(String(value || '')); } catch { throw new Error('search_target_url_invalid'); }
  if (url.protocol !== 'https:' || url.username || url.password || url.search || url.hash) throw new Error('search_target_url_unsafe');
  const allowed = new Set(String(process.env.E2E_SEARCH_ALLOWED_ORIGINS || 'https://kenigevents.ru,https://static.kenigevents.ru')
    .split(',').map((item) => item.trim()).filter(Boolean));
  if (!allowed.has(url.origin)) throw new Error('search_target_origin_not_allowed');
  if (!/^(?:\/poisk\/|\/_review\/[A-Za-z0-9_-]{43}\/poisk\/)$/u.test(url.pathname)) throw new Error('search_target_path_not_allowed');
  return url;
}

function queryCases(value) {
  if (!value) return defaultQueries;
  let parsed;
  try { parsed = JSON.parse(value); } catch { throw new Error('search_queries_json_invalid'); }
  if (!Array.isArray(parsed)) throw new Error('search_queries_json_not_array');
  return parsed;
}

async function assertTargetSha(url, expectedValue) {
  const expected = String(expectedValue || '').trim().toLowerCase();
  if (!/^[0-9a-f]{40}$/u.test(expected)) throw new Error('search_expected_repo_sha_invalid');
  const parts = url.pathname.split('/').filter(Boolean);
  const secretCandidate = parts[0] === '_review';
  const metadataUrl = new URL(secretCandidate ? `/_review/${parts[1]}/candidate-build.json` : '/static-release-manifest.json', url.origin);
  const response = await fetch(metadataUrl, { signal: AbortSignal.timeout(15_000), headers: { Accept: 'application/json' } });
  if (!response.ok) throw new Error(`search_target_metadata_status:${response.status}`);
  const body = await response.json().catch(() => null);
  const expectedSchema = secretCandidate ? 'static_secret_candidate_build_v1' : 'static_release_manifest_v1';
  if (body?.schema_version !== expectedSchema) throw new Error('search_target_identity_schema_mismatch');
  if (secretCandidate && body?.base_path !== `/_review/${parts[1]}`) throw new Error('search_target_identity_base_path_mismatch');
  const observed = String(body?.repo_sha || '').trim().toLowerCase();
  if (!/^[0-9a-f]{40}$/u.test(observed)) throw new Error('search_target_repo_sha_missing');
  if (observed !== expected) throw new Error('search_target_repo_sha_mismatch');
  const revisions = body?.search_revisions;
  const catalogRevision = String(revisions?.catalog_revision || '');
  const corpusRevision = String(revisions?.corpus_revision || '');
  const searchDocumentRevision = String(revisions?.search_document_revision || '');
  if (![catalogRevision, corpusRevision, searchDocumentRevision].every((value) => /^[0-9a-f]{64}$/u.test(value))
    || revisions?.coverage_status !== 'complete') throw new Error('search_target_revisions_missing');
  return { repoSha: observed, catalogRevision, corpusRevision, searchDocumentRevision };
}

function errorCode(error) {
  const value = String(error?.message || error?.name || 'search_acceptance_failed').split(':')[0];
  return /^[a-z0-9_.-]{3,80}$/iu.test(value) ? value : 'search_acceptance_failed';
}

async function adapterFor(platform, authStatePath) {
  if (platform === 'browser') {
    const { createPlaywrightSearchAdapter } = await import('./adapters/playwright.mjs');
    return createPlaywrightSearchAdapter({
      browserName: process.env.E2E_SEARCH_BROWSER || 'chromium', storageStatePath: authStatePath || undefined,
      headless: process.env.E2E_HEADLESS !== '0', timeoutMs: process.env.E2E_SEARCH_TIMEOUT_MS,
    });
  }
  const common = {
    hostname: process.env.E2E_APPIUM_HOST || '127.0.0.1', port: process.env.E2E_APPIUM_PORT || 4723,
    path: process.env.E2E_APPIUM_PATH || '/', deviceName: process.env.E2E_DEVICE_NAME,
    platformVersion: process.env.E2E_PLATFORM_VERSION, timeoutMs: process.env.E2E_SEARCH_TIMEOUT_MS,
  };
  if (platform === 'android') {
    const { createAndroidSearchAdapter } = await import('./adapters/appium-android.mjs');
    return createAndroidSearchAdapter(common);
  }
  const { createIosSearchAdapter } = await import('./adapters/appium-ios.mjs');
  return createIosSearchAdapter(common);
}

const platform = String(process.env.E2E_SEARCH_PLATFORM || 'browser').toLowerCase();
const mode = String(process.env.E2E_SEARCH_VARIANT || 'cold_vector').toLowerCase();
const evidenceDirectory = process.env.E2E_EVIDENCE_DIR || `artifacts/search-live-${platform}`;
const authStatePath = String(process.env.E2E_AUTH_STATE_PATH || '');
const revisionPolicy = String(process.env.E2E_SEARCH_REVISION_POLICY || 'release_exact');
let safeTarget = null;
let adapter = null;
let targetRepoSha = null;
let lastJourney = null;
let exitCode = 0;
const priorStartupReceipt = await readPriorMobileStartupReceipt(
  process.env.E2E_APPIUM_PRIOR_RECEIPT_PATH,
).catch(() => null);
try {
  if (!closedModes.has(mode)) throw new Error(`search_variant_unknown:${mode}`);
  if (!['release_exact', 'live_consistent'].includes(revisionPolicy)) throw new Error('search_revision_policy_invalid');
  safeTarget = targetUrl(process.env.E2E_SEARCH_TARGET_URL);
  const targetIdentity = await assertTargetSha(safeTarget, process.env.E2E_EXPECTED_REPO_SHA);
  targetRepoSha = targetIdentity.repoSha;
  const variant = resolveCanaryVariant(mode, platform);
  const actionLink = String(process.env.E2E_AUTH_ACTION_LINK || '');
  if (!authStatePath && !actionLink) throw new Error('search_auth_bootstrap_missing');
  if (platform !== 'browser' && !actionLink) throw new Error('search_mobile_broker_action_link_missing');
  adapter = await adapterFor(platform, authStatePath);
  if (actionLink) await adapter.bootstrapSession(actionLink, safeTarget.href);
  const journeyTarget = new URL(safeTarget.href);
  if (journeyTarget.pathname.startsWith('/_review/')) journeyTarget.searchParams.set('search_variant', mode);
  const journey = await runSearchJourney({
    adapter,
    targetUrl: journeyTarget.href,
    variant,
    queryCases: queryCases(process.env.E2E_SEARCH_QUERIES_JSON),
    cacheBootstrap: revisionPolicy === 'live_consistent' && mode === 'cached_vector',
  });
  lastJourney = journey;
  const revisionReceipt = assertSearchRevisionPolicy(journey, targetIdentity, revisionPolicy);
  const deviceReceipt = await adapter?.diagnostics?.().catch(() => null);
  const result = { ...journey, platform, execution_mode: mode, target_repo_sha: targetRepoSha,
    revision_receipt: revisionReceipt, ...(deviceReceipt ? { device_receipt: deviceReceipt } : {}),
    ...(priorStartupReceipt ? { prior_startup_receipt: priorStartupReceipt } : {}) };
  await writeSearchEvidence(evidenceDirectory, result);
  process.stdout.write(`search-live PASS platform=${platform} execution_mode=${mode} evidence=${evidenceDirectory}\n`);
} catch (error) {
  exitCode = 1;
  const failureReceipt = error?.searchReceipt || await adapter?.diagnostics?.().catch(() => null);
  const result = {
    ...(lastJourney || {}), status: 'FAIL', platform, execution_mode: mode,
    target_origin: safeTarget?.origin || null,
    target_path: safeTarget ? sanitizedTargetPath(safeTarget.pathname) : null,
    target_repo_sha: targetRepoSha,
    counters: lastJourney?.counters || {}, query_cases: lastJourney?.query_cases || [], error_code: errorCode(error),
    ...(failureReceipt ? { failure_receipt: failureReceipt } : {}),
    ...(priorStartupReceipt ? { prior_startup_receipt: priorStartupReceipt } : {}),
  };
  await writeSearchEvidence(evidenceDirectory, result).catch(() => undefined);
  process.stderr.write(`search-live FAIL code=${result.error_code}\n`);
} finally {
  await adapter?.close?.().catch(() => undefined);
  if (authStatePath) await unlink(authStatePath).catch(() => undefined);
}
process.exitCode = exitCode;
