import { createHash, randomUUID } from 'node:crypto';
import { mkdir, rm, writeFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import process from 'node:process';

import { createClient } from '@supabase/supabase-js';

export const STATIC_SITE_AUTH_MODES = Object.freeze([
  'anonymous',
  'anonymous_session',
  'mocked_ui',
  'session_fixture',
  'admin_otp_ui',
  'real_mail_otp',
  'yandex_oauth',
]);

export const DEFAULT_PERSONA_ENV = Object.freeze({
  'auth-readonly': 'STATIC_SITE_AUTH_FIXTURE_EMAIL_AUTH_READONLY',
  'search-cached': 'STATIC_SITE_AUTH_FIXTURE_EMAIL_SEARCH_CACHED',
  'search-cold': 'STATIC_SITE_AUTH_FIXTURE_EMAIL_SEARCH_COLD',
  personalization: 'STATIC_SITE_AUTH_FIXTURE_EMAIL_PERSONALIZATION',
  'focus-member': 'STATIC_SITE_AUTH_FIXTURE_EMAIL_FOCUS_MEMBER',
});

const activeScopes = new Set();
const ALLOWED_SCOPE_KINDS = new Set(['test', 'worker', 'job', 'device']);

export class AuthSessionFixtureBlockedError extends Error {
  constructor(reason, cause) {
    super(`BLOCKED_AUTH_FIXTURE:${reason}`);
    this.name = 'AuthSessionFixtureBlockedError';
    this.code = 'BLOCKED_AUTH_FIXTURE';
    this.reason = reason;
    if (cause !== undefined) this.cause = cause;
  }
}

function blocked(reason, cause) {
  return cause instanceof AuthSessionFixtureBlockedError
    ? cause
    : new AuthSessionFixtureBlockedError(reason, cause);
}

function trimmedOrigin(value) {
  return new URL(String(value || '')).origin;
}

function supabaseAuthStorageKey(supabaseUrl) {
  const projectRef = new URL(supabaseUrl).hostname.split('.')[0];
  return `sb-${projectRef}-auth-token`;
}

function safeSegment(value) {
  const segment = String(value || '').trim().replace(/[^a-z0-9_.-]+/giu, '-').replace(/^-+|-+$/gu, '');
  if (!segment) throw blocked('INVALID_SESSION_SCOPE');
  return segment.slice(0, 96);
}

function identityHash(personaId, salt) {
  return createHash('sha256').update(`${salt}:${personaId}`).digest('hex').slice(0, 20);
}

function targetPathClass(pathname) {
  const path = String(pathname || '/');
  const firstSegment = path.split('/').filter(Boolean)[0] || '';
  if (firstSegment === '_review' || firstSegment.startsWith('preview-')) return 'immutable_preview';
  if (path === '/') return 'root';
  return 'public_page';
}

function resolvePersona(personaId, personas) {
  const id = String(personaId || '').trim();
  const persona = personas instanceof Map ? personas.get(id) : personas?.[id];
  if (!persona || typeof persona !== 'object') throw blocked('PERSONA_NOT_ALLOWLISTED');
  const email = String(persona.email || '').trim().toLowerCase();
  if (!email || !email.includes('@')) throw blocked('PERSONA_EMAIL_MISSING');
  return { id, email };
}

function assertTarget(targetUrl, allowedOrigins) {
  let target;
  try {
    target = new URL(String(targetUrl || ''));
  } catch (error) {
    throw blocked('TARGET_INVALID', error);
  }
  if (target.protocol !== 'https:') throw blocked('TARGET_NOT_HTTPS');
  const allowed = new Set((allowedOrigins || ['https://kenigevents.ru']).map(trimmedOrigin));
  if (!allowed.has(target.origin)) throw blocked('TARGET_NOT_ALLOWLISTED');
  return target;
}

function instrumentedFetch(fetchImpl, counters) {
  return async (input, init) => {
    const request = input instanceof Request ? input : null;
    const method = String(init?.method || request?.method || 'GET').toUpperCase();
    const url = new URL(request?.url || String(input));
    if (method === 'POST' && url.pathname === '/auth/v1/otp') counters.productOtpIssues += 1;
    return fetchImpl(input, init);
  };
}

function protectedRlsProbeFetch(fetchImpl, { accessToken, publishableKey, supabaseUrl }, counters) {
  const expectedOrigin = new URL(supabaseUrl).origin;
  return async (input, init) => {
    const request = input instanceof Request ? input : null;
    const method = String(init?.method || request?.method || 'GET').toUpperCase();
    const url = new URL(request?.url || String(input));
    const headers = new Headers(request?.headers);
    new Headers(init?.headers).forEach((value, name) => headers.set(name, value));
    if (url.origin !== expectedOrigin || !url.pathname.startsWith('/rest/v1/')) {
      throw blocked('PROTECTED_PROBE_TARGET_INVALID');
    }
    if (method !== 'GET') throw blocked('PROTECTED_PROBE_NOT_READ_ONLY');
    if (headers.get('authorization') !== `Bearer ${accessToken}`
      || headers.get('apikey') !== publishableKey) {
      throw blocked('PROTECTED_PROBE_SESSION_HEADERS_INVALID');
    }
    counters.protectedProbeRequests += 1;
    const response = await fetchImpl(input, init);
    if (response?.ok) counters.protectedProbeSuccesses += 1;
    return response;
  };
}

function clientFactoryDefault({ supabaseUrl, key, fetchImpl, role }) {
  return createClient(supabaseUrl, key, {
    auth: {
      autoRefreshToken: false,
      detectSessionInUrl: false,
      persistSession: false,
    },
    global: { fetch: fetchImpl },
    // This marker is ignored by supabase-js but is available to injected test
    // factories without ever exposing the server key to browser code.
    kenigEventsFixtureRole: role,
  });
}

export function allowlistedPersonasFromEnv(env = process.env, mapping = DEFAULT_PERSONA_ENV) {
  const result = {};
  for (const [personaId, variable] of Object.entries(mapping)) {
    const email = String(env[variable] || '').trim();
    if (email) result[personaId] = { email };
  }
  return result;
}

export function resetAuthSessionFixtureScopesForTests() {
  activeScopes.clear();
}

/**
 * Create one real Supabase user session without invoking the product OTP/mail
 * path. The returned Playwright storage state is sensitive ephemeral material:
 * callers must use it only inside the same worker/device job and call cleanup.
 */
export async function createAuthSessionFixture(options = {}) {
  if (options.authMode && options.authMode !== 'session_fixture') throw blocked('AUTH_MODE_MISMATCH');
  if (options.realMailFallback !== undefined && options.realMailFallback !== false) {
    throw blocked('REAL_MAIL_FALLBACK_FORBIDDEN');
  }

  const supabaseUrl = String(options.supabaseUrl || '').replace(/\/+$/u, '');
  const publishableKey = String(options.publishableKey || '');
  const secretKey = String(options.secretKey || '');
  if (!supabaseUrl || !publishableKey || !secretKey) throw blocked('CONFIG_MISSING');

  const target = assertTarget(options.targetUrl, options.allowedOrigins);
  const persona = resolvePersona(options.personaId, options.personas || {});
  const scopeKind = safeSegment(options.scopeKind || 'worker');
  if (!ALLOWED_SCOPE_KINDS.has(scopeKind)) throw blocked('SESSION_SCOPE_KIND_INVALID');
  if (typeof options.protectedProbe !== 'function') throw blocked('PROTECTED_PROBE_REQUIRED');
  const scopeId = safeSegment(options.scopeId || randomUUID());
  const scopeKey = `${scopeKind}:${scopeId}`;
  if (activeScopes.has(scopeKey)) throw blocked('SESSION_SCOPE_ALREADY_ACTIVE');
  activeScopes.add(scopeKey);

  const counters = {
    adminCredentials: 0,
    authVerifies: 0,
    productOtpIssues: 0,
    externalMailSends: 0,
    externalMailReceipts: 0,
    protectedProbeRequests: 0,
    protectedProbeSuccesses: 0,
  };
  const rawFetch = options.fetchImpl || globalThis.fetch?.bind(globalThis);
  if (typeof rawFetch !== 'function') {
    activeScopes.delete(scopeKey);
    throw blocked('FETCH_UNAVAILABLE');
  }
  const fetchImpl = instrumentedFetch(rawFetch, counters);
  const clientFactory = options.clientFactory || clientFactoryDefault;
  const tempRoot = String(options.tempRoot || process.env.RUNNER_TEMP || process.env.TMPDIR || '/tmp');
  const statePath = join(tempRoot, 'kenigevents-auth', safeSegment(options.runId || 'local'), `${scopeKind}-${scopeId}.json`);
  const salt = String(options.receiptSalt || options.runId || 'local-fixture');
  let credential = null;
  let userClient = null;
  let receipt = null;
  let cleaned = false;

  const cleanup = async () => {
    if (cleaned) return { cleanup_status: receipt?.cleanup_status || 'PASS' };
    cleaned = true;
    let cleanupError = null;
    try {
      await userClient?.auth?.signOut?.({ scope: 'local' });
    } catch {
      // Local state removal below is authoritative; cleanup evidence records
      // filesystem removal without turning a server outage into blind retry.
    }
    try {
      await rm(statePath, { force: true });
    } catch (error) {
      cleanupError = error;
    } finally {
      activeScopes.delete(scopeKey);
      if (credential) {
        credential.emailOtp = '';
        credential.actionLink = '';
      }
      if (receipt) receipt.cleanup_status = cleanupError ? 'FAIL' : 'PASS';
    }
    if (cleanupError) throw blocked('CLEANUP_FAILED', cleanupError);
    return { cleanup_status: 'PASS' };
  };

  try {
    const adminClient = clientFactory({
      role: 'issuer', supabaseUrl, key: secretKey, fetchImpl,
    });
    const issued = await adminClient.auth.admin.generateLink({
      type: 'magiclink',
      email: persona.email,
      options: { redirectTo: target.href },
    });
    if (issued?.error) throw issued.error;
    counters.adminCredentials += 1;
    credential = {
      emailOtp: String(issued?.data?.properties?.email_otp || ''),
      actionLink: String(issued?.data?.properties?.action_link || ''),
    };
    if (!/^\d{6,10}$/u.test(credential.emailOtp) || !credential.actionLink.startsWith('https://')) {
      throw blocked('ISSUER_RESPONSE_INVALID');
    }

    userClient = clientFactory({
      role: 'session', supabaseUrl, key: publishableKey, fetchImpl,
    });
    const verified = await userClient.auth.verifyOtp({
      email: persona.email,
      token: credential.emailOtp,
      type: 'email',
    });
    if (verified?.error) throw verified.error;
    counters.authVerifies += 1;
    const session = verified?.data?.session;
    if (!session?.access_token || !session?.refresh_token) throw blocked('VERIFY_WITHOUT_SESSION');

    const identity = await userClient.auth.getUser(session.access_token);
    if (identity?.error) throw identity.error;
    const user = identity?.data?.user;
    if (!user?.id || String(user.email || '').toLowerCase() !== persona.email) {
      throw blocked('IDENTITY_MISMATCH');
    }

    let protectedProbe = false;
    try {
      const probeFetch = protectedRlsProbeFetch(fetchImpl, {
        accessToken: session.access_token,
        publishableKey,
        supabaseUrl,
      }, counters);
      protectedProbe = await options.protectedProbe({
        accessToken: session.access_token,
        publishableKey,
        supabaseUrl,
        fetchImpl: probeFetch,
        userId: user.id,
      });
    } catch (error) {
      throw blocked('PROTECTED_PROBE_FAILED', error);
    }
    if (protectedProbe !== true
      || counters.protectedProbeRequests !== 1
      || counters.protectedProbeSuccesses !== 1) {
      throw blocked('PROTECTED_PROBE_FAILED');
    }
    if (counters.productOtpIssues !== 0) throw blocked('UNEXPECTED_PRODUCT_OTP');

    const storageState = {
      cookies: [],
      origins: [{
        origin: target.origin,
        localStorage: [{
          name: supabaseAuthStorageKey(supabaseUrl),
          value: JSON.stringify(session),
        }],
      }],
    };
    await mkdir(dirname(statePath), { recursive: true, mode: 0o700 });
    await writeFile(statePath, `${JSON.stringify(storageState)}\n`, { mode: 0o600 });

    receipt = Object.seal({
      schema: 'static_site_auth_session_fixture_receipt.v1',
      outcome: 'PASS',
      auth_mode: 'session_fixture',
      session_scope: scopeKind,
      scope_hash: identityHash(scopeKey, salt),
      persona_role: persona.id,
      persona_hash: identityHash(persona.id, salt),
      target_origin: target.origin,
      target_path_class: targetPathClass(target.pathname),
      target_path_hash: identityHash(target.pathname, salt),
      project_ref_hash: identityHash(new URL(supabaseUrl).hostname.split('.')[0], salt),
      bootstrap_method: 'admin_generate_link_then_verify_otp',
      admin_credential_count: counters.adminCredentials,
      auth_verify_count: counters.authVerifies,
      product_otp_issue_count: counters.productOtpIssues,
      external_mail_send_count: counters.externalMailSends,
      external_mail_receipt_count: counters.externalMailReceipts,
      get_user_verified: true,
      protected_probe_verified: true,
      protected_probe_request_count: counters.protectedProbeRequests,
      storage_state_ephemeral: true,
      real_mail_fallback: 'forbidden',
      cleanup_status: 'PENDING',
      redaction_status: 'PASS',
    });

    credential.emailOtp = '';
    credential.actionLink = '';
    return { receipt, storageStatePath: statePath, cleanup };
  } catch (error) {
    await cleanup();
    throw blocked(error?.reason || 'SETUP_FAILED', error);
  }
}
