import assert from 'node:assert/strict';
import { access, mkdtemp, readFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';

import {
  AuthSessionFixtureBlockedError,
  STATIC_SITE_AUTH_MODES,
  allowlistedPersonasFromEnv,
  createAuthSessionFixture,
  resetAuthSessionFixtureScopesForTests,
} from '../e2e/auth-session-fixture/session-fixture.mjs';

const TOKEN = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ1c2VyLTEiLCJyb2xlIjoiYXV0aGVudGljYXRlZCJ9.fixture-signature';
const REFRESH = 'refresh.secret.value';

function fakeClientFactory({ role, fetchImpl }) {
  if (role === 'issuer') {
    return {
      auth: {
        admin: {
          async generateLink() {
            await fetchImpl('https://project.supabase.co/auth/v1/admin/generate_link', { method: 'POST' });
            return {
              data: { properties: { email_otp: '456789', action_link: 'https://project.supabase.co/auth/v1/verify?token=secret' } },
              error: null,
            };
          },
        },
      },
    };
  }
  return {
    auth: {
      async verifyOtp() {
        await fetchImpl('https://project.supabase.co/auth/v1/verify', { method: 'POST' });
        return { data: { session: { access_token: TOKEN, refresh_token: REFRESH, expires_in: 3600 } }, error: null };
      },
      async getUser(accessToken) {
        assert.equal(accessToken, TOKEN);
        return { data: { user: { id: 'user-1', email: 'search-cached@example.invalid' } }, error: null };
      },
      async signOut(options) {
        assert.deepEqual(options, { scope: 'local' });
      },
    },
  };
}

const fetchImpl = async (input) => {
  const url = new URL(input instanceof Request ? input.url : String(input));
  const body = url.pathname.startsWith('/rest/v1/') ? [{ owner_id: 'user-1' }] : {};
  return Response.json(body);
};

async function verifiedRlsProbe({ accessToken, publishableKey, supabaseUrl, fetchImpl: probeFetch, userId }) {
  const response = await probeFetch(`${supabaseUrl}/rest/v1/auth_fixture_rls_probe?select=owner_id&limit=1`, {
    method: 'GET',
    headers: {
      apikey: publishableKey,
      Authorization: `Bearer ${accessToken}`,
      accept: 'application/json',
    },
  });
  if (!response.ok) return false;
  const rows = await response.json();
  return Array.isArray(rows) && rows.length === 1 && rows[0]?.owner_id === userId;
}

test.beforeEach(() => resetAuthSessionFixtureScopesForTests());

test('auth mode vocabulary is closed and includes anonymous-first focus sessions', () => {
  assert.deepEqual(STATIC_SITE_AUTH_MODES, [
    'anonymous', 'anonymous_session', 'mocked_ui', 'session_fixture',
    'admin_otp_ui', 'real_mail_otp', 'yandex_oauth',
  ]);
});

test('creates a real-shaped per-worker session state with zero OTP/mail side effects and sanitized receipt', async () => {
  const root = await mkdtemp(join(tmpdir(), 'ke-auth-fixture-'));
  const fixture = await createAuthSessionFixture({
    supabaseUrl: 'https://project.supabase.co',
    publishableKey: 'publishable',
    secretKey: 'server-secret',
    targetUrl: 'https://kenigevents.ru/_review/super-secret-preview-token/poisk/',
    allowedOrigins: ['https://kenigevents.ru'],
    personas: { 'search-cached': { email: 'search-cached@example.invalid' } },
    personaId: 'search-cached',
    scopeKind: 'worker',
    scopeId: 'chromium-0',
    runId: 'run-42',
    receiptSalt: 'receipt-salt',
    tempRoot: root,
    clientFactory: fakeClientFactory,
    fetchImpl,
    protectedProbe: verifiedRlsProbe,
  });

  assert.equal(fixture.receipt.outcome, 'PASS');
  assert.equal(fixture.receipt.auth_mode, 'session_fixture');
  assert.equal(fixture.receipt.admin_credential_count, 1);
  assert.equal(fixture.receipt.auth_verify_count, 1);
  assert.equal(fixture.receipt.product_otp_issue_count, 0);
  assert.equal(fixture.receipt.external_mail_send_count, 0);
  assert.equal(fixture.receipt.external_mail_receipt_count, 0);
  assert.equal(fixture.receipt.get_user_verified, true);
  assert.equal(fixture.receipt.protected_probe_verified, true);
  assert.equal(fixture.receipt.protected_probe_request_count, 1);
  assert.equal(fixture.receipt.real_mail_fallback, 'forbidden');
  assert.equal(fixture.receipt.cleanup_status, 'PENDING');
  assert.equal(fixture.receipt.target_path_class, 'immutable_preview');
  assert.match(fixture.receipt.target_path_hash, /^[a-f0-9]{20}$/u);
  const serializedReceipt = JSON.stringify(fixture.receipt);
  assert.doesNotMatch(serializedReceipt, /example\.invalid|eyJhbGci|refresh\.secret|456789|token=|super-secret-preview-token/u);

  const state = JSON.parse(await readFile(fixture.storageStatePath, 'utf8'));
  assert.equal(state.origins[0].origin, 'https://kenigevents.ru');
  assert.match(state.origins[0].localStorage[0].name, /^sb-project-auth-token$/u);
  assert.match(state.origins[0].localStorage[0].value, /eyJhbGciOiJIUzI1Ni/u);

  assert.deepEqual(await fixture.cleanup(), { cleanup_status: 'PASS' });
  assert.equal(fixture.receipt.cleanup_status, 'PASS');
  await assert.rejects(access(fixture.storageStatePath));
});

test('fails closed for unallowlisted persona, target and real-mail fallback', async () => {
  const base = {
    supabaseUrl: 'https://project.supabase.co', publishableKey: 'publishable', secretKey: 'secret',
    targetUrl: 'https://kenigevents.ru/poisk/', personas: {}, personaId: 'unknown', fetchImpl, clientFactory: fakeClientFactory,
  };
  await assert.rejects(createAuthSessionFixture(base), (error) => {
    assert.ok(error instanceof AuthSessionFixtureBlockedError);
    assert.equal(error.code, 'BLOCKED_AUTH_FIXTURE');
    assert.equal(error.reason, 'PERSONA_NOT_ALLOWLISTED');
    return true;
  });
  await assert.rejects(createAuthSessionFixture({
    ...base, personas: { unknown: { email: 'search-cached@example.invalid' } }, targetUrl: 'https://attacker.invalid/',
  }), /BLOCKED_AUTH_FIXTURE:TARGET_NOT_ALLOWLISTED/u);
  await assert.rejects(createAuthSessionFixture({
    ...base, personas: { unknown: { email: 'search-cached@example.invalid' } }, realMailFallback: true,
  }), /BLOCKED_AUTH_FIXTURE:REAL_MAIL_FALLBACK_FORBIDDEN/u);
  await assert.rejects(createAuthSessionFixture({
    ...base, personas: { unknown: { email: 'search-cached@example.invalid' } }, scopeKind: 'shared-global',
  }), /BLOCKED_AUTH_FIXTURE:SESSION_SCOPE_KIND_INVALID/u);
});

test('fails closed when protected RLS probe is omitted, bypassed or unsuccessful', async () => {
  const base = {
    supabaseUrl: 'https://project.supabase.co', publishableKey: 'publishable', secretKey: 'secret',
    targetUrl: 'https://kenigevents.ru/poisk/',
    personas: { p: { email: 'search-cached@example.invalid' } }, personaId: 'p',
    scopeKind: 'worker', runId: 'probe-negative',
    tempRoot: await mkdtemp(join(tmpdir(), 'ke-auth-fixture-')),
    fetchImpl, clientFactory: fakeClientFactory,
  };

  await assert.rejects(createAuthSessionFixture({ ...base, scopeId: 'omitted' }), (error) => {
    assert.equal(error.code, 'BLOCKED_AUTH_FIXTURE');
    assert.equal(error.reason, 'PROTECTED_PROBE_REQUIRED');
    return true;
  });

  await assert.rejects(createAuthSessionFixture({
    ...base,
    scopeId: 'bypassed',
    protectedProbe: async () => true,
  }), (error) => {
    assert.equal(error.code, 'BLOCKED_AUTH_FIXTURE');
    assert.equal(error.reason, 'PROTECTED_PROBE_FAILED');
    return true;
  });

  await assert.rejects(createAuthSessionFixture({
    ...base,
    scopeId: 'not-bound-to-session',
    protectedProbe: async ({ supabaseUrl, fetchImpl: probeFetch }) => {
      const response = await probeFetch(`${supabaseUrl}/rest/v1/auth_fixture_rls_probe?select=owner_id&limit=1`);
      return response.ok;
    },
  }), (error) => {
    assert.equal(error.code, 'BLOCKED_AUTH_FIXTURE');
    assert.equal(error.reason, 'PROTECTED_PROBE_SESSION_HEADERS_INVALID');
    return true;
  });

  const deniedFetch = async (input) => {
    const url = new URL(input instanceof Request ? input.url : String(input));
    if (url.pathname.startsWith('/rest/v1/')) return Response.json({ message: 'RLS denied' }, { status: 403 });
    return Response.json({});
  };
  await assert.rejects(createAuthSessionFixture({
    ...base,
    scopeId: 'denied',
    fetchImpl: deniedFetch,
    protectedProbe: verifiedRlsProbe,
  }), (error) => {
    assert.equal(error.code, 'BLOCKED_AUTH_FIXTURE');
    assert.equal(error.reason, 'PROTECTED_PROBE_FAILED');
    return true;
  });
});

test('one active scope cannot be shared and unexpected product OTP blocks without fallback', async () => {
  let releaseVerify;
  const waitingFactory = ({ role, fetchImpl: instrumented }) => role === 'issuer'
    ? fakeClientFactory({ role, fetchImpl: instrumented })
    : {
        auth: {
          async verifyOtp() {
            await new Promise((resolve) => { releaseVerify = resolve; });
            return fakeClientFactory({ role, fetchImpl: instrumented }).auth.verifyOtp();
          },
          getUser: (...args) => fakeClientFactory({ role, fetchImpl: instrumented }).auth.getUser(...args),
          signOut: async () => {},
        },
      };
  const options = {
    supabaseUrl: 'https://project.supabase.co', publishableKey: 'publishable', secretKey: 'secret',
    targetUrl: 'https://kenigevents.ru/poisk/', personas: { p: { email: 'search-cached@example.invalid' } }, personaId: 'p',
    scopeKind: 'worker', scopeId: 'shared', runId: 'run', tempRoot: await mkdtemp(join(tmpdir(), 'ke-auth-fixture-')),
    fetchImpl, clientFactory: waitingFactory, protectedProbe: verifiedRlsProbe,
  };
  const first = createAuthSessionFixture(options);
  while (!releaseVerify) await new Promise((resolve) => setTimeout(resolve, 0));
  await assert.rejects(createAuthSessionFixture(options), /BLOCKED_AUTH_FIXTURE:SESSION_SCOPE_ALREADY_ACTIVE/u);
  releaseVerify();
  const fixture = await first;
  await fixture.cleanup();

  const otpFactory = ({ role, fetchImpl: instrumented }) => {
    const client = fakeClientFactory({ role, fetchImpl: instrumented });
    if (role === 'session') {
      client.auth.verifyOtp = async () => {
        await instrumented('https://project.supabase.co/auth/v1/otp', { method: 'POST' });
        return { data: { session: { access_token: TOKEN, refresh_token: REFRESH } }, error: null };
      };
    }
    return client;
  };
  await assert.rejects(createAuthSessionFixture({ ...options, scopeId: 'otp', clientFactory: otpFactory }), (error) => {
    assert.equal(error.code, 'BLOCKED_AUTH_FIXTURE');
    assert.equal(error.reason, 'UNEXPECTED_PRODUCT_OTP');
    return true;
  });
});

test('persona allowlist is built only from named environment variables', () => {
  assert.deepEqual(allowlistedPersonasFromEnv({
    STATIC_SITE_AUTH_FIXTURE_EMAIL_SEARCH_CACHED: 'cached@example.invalid',
    ARBITRARY_EMAIL: 'ignored@example.invalid',
  }), { 'search-cached': { email: 'cached@example.invalid' } });
});
