import { spawn } from 'node:child_process';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { dirname, join, resolve } from 'node:path';

const siteRequire = createRequire(new URL('../../site/package.json', import.meta.url));
const { chromium } = siteRequire('playwright');

import { runExactTargetBrowserAcceptance } from '../../site/e2e/auth-session-fixture/acceptance.mjs';
import {
  createAuthSessionBrokerIssuer,
  createAuthSessionFixture,
} from '../../site/e2e/auth-session-fixture/session-fixture.mjs';
import { assertSanitizedSearchEvidence } from '../../site/e2e/search/evidence.mjs';

function required(name) {
  const value = String(process.env[name] || '').trim();
  if (!value) throw new Error(`missing_${name.toLowerCase()}`);
  return value;
}

function persona(variant) {
  if (variant === 'cached_vector') {
    return { id: 'search-cached-browser', email: required('SEARCH_E2E_PERSONA_EMAIL_CACHED_BROWSER') };
  }
  if (variant === 'degraded_vector_fallback') {
    return { id: 'search-degraded-browser', email: required('SEARCH_E2E_PERSONA_EMAIL_DEGRADED_BROWSER') };
  }
  return { id: 'search-cold-browser', email: required('SEARCH_E2E_PERSONA_EMAIL_COLD_BROWSER') };
}

async function oidcToken() {
  const url = new URL(required('ACTIONS_ID_TOKEN_REQUEST_URL'));
  url.searchParams.set('audience', required('AUTH_SESSION_BROKER_OIDC_AUDIENCE'));
  const response = await fetch(url, {
    redirect: 'error',
    headers: { authorization: `Bearer ${required('ACTIONS_ID_TOKEN_REQUEST_TOKEN')}` },
  });
  if (!response.ok) throw new Error(`github_oidc_rejected_${response.status}`);
  const payload = await response.json();
  const token = String(payload?.value || '').trim();
  if (!token || token.includes('\n')) throw new Error('github_oidc_invalid');
  return token;
}

async function protectedOwnerProbe({ fetchImpl, userId, supabaseUrl, accessToken, publishableKey }) {
  const url = new URL('/rest/v1/user_saved_event', supabaseUrl);
  url.searchParams.set('select', 'user_id');
  url.searchParams.set('user_id', `eq.${userId}`);
  url.searchParams.set('limit', '1');
  const response = await fetchImpl(url, {
    method: 'GET',
    headers: {
      accept: 'application/json',
      apikey: publishableKey,
      authorization: `Bearer ${accessToken}`,
    },
  });
  if (!response.ok) return false;
  const rows = await response.json();
  return Array.isArray(rows) && rows.every((row) => String(row?.user_id || '') === userId);
}

async function acceptanceAdapter(storageStatePath, targetUrl) {
  const browser = await chromium.launch({ headless: process.env.E2E_HEADLESS !== '0' });
  const context = await browser.newContext({ storageState: storageStatePath });
  const counters = { productOtpPosts: 0, externalMailSends: 0, externalMailReceipts: 0 };
  context.on('request', (request) => {
    const url = new URL(request.url());
    if (request.method() === 'POST' && url.pathname === '/auth/v1/otp') counters.productOtpPosts += 1;
  });
  const page = await context.newPage();
  return {
    async fetchReleaseIdentity(path) {
      const expected = new URL(path, targetUrl).href;
      const response = await context.request.get(expected, { failOnStatusCode: false });
      const body = await response.json().catch(() => null);
      return {
        status: response.status(), finalUrl: response.url(), repoSha: body?.repo_sha,
        siteMode: path.endsWith('/candidate-build.json') ? 'secret_candidate' : 'production',
        basePath: path.endsWith('/candidate-build.json') ? dirname(path) : '',
      };
    },
    async openExact(url) {
      const response = await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 60_000 });
      return { status: response?.status() || 0, finalUrl: page.url() };
    },
    async restoredSession() {
      await page.locator('[data-authorized-search]').waitFor({ state: 'attached', timeout: 30_000 });
      await page.waitForFunction(() => document.querySelector('[data-authorized-search]')?.classList.contains('is-authorized'));
      return { restored: true, authenticated: true, pathname: new URL(page.url()).pathname };
    },
    async requestCounters() { return { ...counters }; },
    async close() { await context.close(); await browser.close(); },
  };
}

function runSearchChild(statePath) {
  return new Promise((resolveChild, rejectChild) => {
    const child = spawn(process.execPath, ['site/e2e/search/run.mjs'], {
      cwd: resolve('.'), stdio: 'inherit',
      env: { ...process.env, E2E_AUTH_STATE_PATH: statePath, E2E_SEARCH_PLATFORM: 'browser' },
    });
    child.once('error', rejectChild);
    child.once('exit', (code, signal) => resolveChild(code ?? (signal ? 1 : 0)));
  });
}

async function accessTokenFromState(statePath) {
  const state = JSON.parse(await readFile(statePath, 'utf8'));
  for (const origin of state?.origins || []) {
    for (const entry of origin?.localStorage || []) {
      if (!String(entry?.name || '').startsWith('sb-')) continue;
      const session = JSON.parse(String(entry?.value || '{}'));
      if (session?.access_token) return String(session.access_token);
    }
  }
  throw new Error('fixture_state_session_missing');
}

function resultResponses(result) {
  const responses = [];
  for (const queryCase of result?.query_cases || []) {
    for (const page of queryCase?.pages || []) if (page?.response) responses.push(page.response);
    if (queryCase?.cache_warm?.response) responses.push(queryCase.cache_warm.response);
    if (queryCase?.cache_repeat?.response) responses.push(queryCase.cache_repeat.response);
  }
  return responses;
}

async function verifyOwnerReceipts({ evidenceDir, accessToken, supabaseUrl, publishableKey }) {
  const result = JSON.parse(await readFile(join(evidenceDir, 'result.json'), 'utf8'));
  const responses = resultResponses(result);
  if (!responses.length) throw new Error('search_server_receipts_missing');
  for (const response of responses) {
    if (!response?.request_id || !response?.receipt_id) throw new Error('search_server_receipt_identity_missing');
    const rpc = new URL('/rest/v1/rpc/get_event_search_receipt_v1', supabaseUrl);
    const received = await fetch(rpc, {
      method: 'POST',
      headers: {
        accept: 'application/json', 'content-type': 'application/json', apikey: publishableKey,
        authorization: `Bearer ${accessToken}`,
      },
      body: JSON.stringify({ p_request_id: response.request_id }),
    });
    if (!received.ok) throw new Error(`search_server_receipt_rpc_${received.status}`);
    const rows = await received.json();
    if (!Array.isArray(rows) || rows.length !== 1) throw new Error('search_server_receipt_owner_scope');
    const row = rows[0];
    const expectedIds = [...new Set(response.response_ids.map(String))].sort();
    const receiptIds = [...new Set((row.response_event_ids || []).map(String))].sort();
    if (String(row.receipt_id) !== response.receipt_id
      || String(row.requested_execution_mode) !== response.requested_execution_mode
      || String(row.actual_execution_mode) !== response.actual_execution_mode
      || String(row.catalog_revision) !== response.catalog_revision
      || String(row.corpus_revision) !== response.corpus_revision
      || expectedIds.join(',') !== receiptIds.join(',')) {
      throw new Error('search_server_receipt_mismatch');
    }
  }
  return responses.length;
}

async function main() {
  const variant = required('E2E_SEARCH_VARIANT');
  const targetUrl = required('E2E_SEARCH_TARGET_URL');
  const selectedPersona = persona(variant);
  const issuer = createAuthSessionBrokerIssuer({
    endpoint: required('AUTH_SESSION_BROKER_URL'),
    oidcToken: await oidcToken(),
  });
  let fixture;
  let adapter;
  let childCode = 1;
  let authReceipt;
  let accessToken = '';
  let ownerReceiptCount = 0;
  try {
    fixture = await createAuthSessionFixture({
      authMode: 'session_fixture', realMailFallback: false, issuer,
      supabaseUrl: required('PERSONALIZATION_SUPABASE_URL'),
      publishableKey: required('PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY'),
      targetUrl, allowedOrigins: ['https://kenigevents.ru'],
      personaId: selectedPersona.id, personas: { [selectedPersona.id]: { email: selectedPersona.email } },
      scopeKind: 'job', scopeId: `search-${variant}`, runId: required('GITHUB_RUN_ID'),
      protectedProbe: protectedOwnerProbe,
    });
    adapter = await acceptanceAdapter(fixture.storageStatePath, targetUrl);
    authReceipt = await runExactTargetBrowserAcceptance({
      targetUrl, expectedRepoSha: required('E2E_EXPECTED_REPO_SHA'), allowedOrigins: ['https://kenigevents.ru'],
      fixtureReceipt: fixture.receipt, adapter, receiptSalt: required('GITHUB_RUN_ID'),
    });
    await adapter.close();
    adapter = null;
    accessToken = await accessTokenFromState(fixture.storageStatePath);
    childCode = await runSearchChild(fixture.storageStatePath);
    if (childCode === 0) {
      ownerReceiptCount = await verifyOwnerReceipts({
        evidenceDir: resolve(required('E2E_EVIDENCE_DIR')), accessToken,
        supabaseUrl: required('PERSONALIZATION_SUPABASE_URL'),
        publishableKey: required('PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY'),
      });
    }
  } finally {
    await adapter?.close?.().catch(() => undefined);
    await fixture?.cleanup?.();
    if (authReceipt) {
      const evidenceDir = resolve(required('E2E_EVIDENCE_DIR'));
      await mkdir(evidenceDir, { recursive: true, mode: 0o700 });
      const sanitizedAuthReceipt = {
        ...authReceipt, cleanup_status: fixture?.receipt?.cleanup_status || 'PASS',
        owner_search_receipt_rpc_count: ownerReceiptCount,
      };
      assertSanitizedSearchEvidence(sanitizedAuthReceipt);
      await writeFile(join(evidenceDir, 'auth-acceptance.json'), `${JSON.stringify(sanitizedAuthReceipt, null, 2)}\n`, { mode: 0o600 });
    }
    accessToken = '';
  }
  if (childCode !== 0) process.exitCode = childCode;
}

main().catch((error) => {
  process.stderr.write(`${String(error?.message || 'browser_search_failed').replace(/https?:\/\/\S+/gu, '<redacted-url>')}\n`);
  process.exitCode = 1;
});
