import { readFile, writeFile, mkdir } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';

import {
  createAuthSessionBrokerIssuer,
  createAuthSessionFixture,
} from '../../site/e2e/auth-session-fixture/session-fixture.mjs';

function required(name) {
  const value = String(process.env[name] || '').trim();
  if (!value) throw new Error(`missing_${name.toLowerCase()}`);
  return value;
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

async function accessTokenFromState(path) {
  const state = JSON.parse(await readFile(path, 'utf8'));
  for (const origin of state?.origins || []) {
    for (const entry of origin?.localStorage || []) {
      if (!String(entry?.name || '').startsWith('sb-')) continue;
      const session = JSON.parse(String(entry?.value || '{}'));
      if (session?.access_token) return String(session.access_token);
    }
  }
  throw new Error('fixture_state_session_missing');
}

function liveBase() {
  const explicit = String(process.env.LIVE_SEARCH_API_URL || '').trim().replace(/\/+$/u, '');
  if (explicit) return explicit;
  const app = required('FLY_APP_NAME');
  return `https://${app}.fly.dev/api/live-search`;
}

async function liveJson(url, token, options = {}) {
  const response = await fetch(url, {
    cache: 'no-store',
    ...options,
    headers: {
      origin: 'https://kenigevents.ru',
      authorization: `Bearer ${token}`,
      accept: 'application/json',
      ...(options.body ? { 'content-type': 'application/json' } : {}),
      ...(options.headers || {}),
    },
    signal: AbortSignal.timeout(35_000),
  });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(`live_http_${response.status}_${String(payload?.error || 'unknown').slice(0,80)}`);
  return payload;
}

async function main() {
  const targetUrl = 'https://kenigevents.ru/poisk/';
  const issuer = createAuthSessionBrokerIssuer({
    endpoint: required('AUTH_SESSION_BROKER_URL'),
    oidcToken: await oidcToken(),
  });
  let fixture;
  let sessionId = '';
  let stopped = false;
  const startedAt = Date.now();
  try {
    fixture = await createAuthSessionFixture({
      authMode: 'session_fixture',
      realMailFallback: false,
      issuer,
      supabaseUrl: required('PERSONALIZATION_SUPABASE_URL'),
      publishableKey: required('PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY'),
      targetUrl,
      allowedOrigins: ['https://kenigevents.ru'],
      personaId: 'search-cached-browser',
      personas: {
        'search-cached-browser': { email: required('SEARCH_E2E_PERSONA_EMAIL_CACHED_BROWSER') },
      },
      purpose: 'production_health',
      platform: 'browser',
      scopeKind: 'job',
      scopeId: `live-search-daily-${required('GITHUB_RUN_ID')}`,
      runId: required('GITHUB_RUN_ID'),
      protectedProbe: protectedOwnerProbe,
    });

    const token = await accessTokenFromState(fixture.storageStatePath);
    const base = liveBase();
    const started = await liveJson(base, token, { method: 'POST', body: '{}' });
    sessionId = String(started?.session_id || '');
    if (!/^live_[a-f0-9]{32}$/u.test(sessionId)) throw new Error('live_session_id_invalid');
    if (String(started?.model || '') !== 'gemini-3.8-live') throw new Error('live_model_unexpected');

    await liveJson(`${base}/${encodeURIComponent(sessionId)}/input`, token, {
      method: 'POST',
      body: JSON.stringify({ text: 'Покажи несколько интересных событий на ближайшие дни' }),
    });

    let cursor = 0;
    let resultEvent = null;
    let sawSearchTool = false;
    const deadline = Date.now() + 55_000;
    while (Date.now() < deadline && !resultEvent) {
      const page = await liveJson(`${base}/${encodeURIComponent(sessionId)}/events?after=${cursor}`, token);
      for (const event of page?.events || []) {
        cursor = Math.max(cursor, Number(event?.seq) || 0);
        if (event?.type === 'tool_call' && (event.calls || []).some((call) => call?.name === 'search_events')) {
          sawSearchTool = true;
        }
        if (event?.type === 'search_results') resultEvent = event;
        if (event?.type === 'error') throw new Error(`live_provider_${String(event.code || 'error').slice(0,80)}`);
      }
      cursor = Math.max(cursor, Number(page?.cursor) || 0);
      if (!resultEvent) await new Promise((resolve) => setTimeout(resolve, page?.has_more ? 10 : 250));
    }
    if (!sawSearchTool) throw new Error('live_search_function_call_missing');
    if (!resultEvent) throw new Error('live_search_results_timeout');
    const items = Array.isArray(resultEvent?.data?.items) ? resultEvent.data.items : [];
    const fallback = Array.isArray(resultEvent?.data?.fallback_items) ? resultEvent.data.fallback_items : [];
    if (items.length + fallback.length < 1) throw new Error('live_search_cards_empty');

    await liveJson(`${base}/${encodeURIComponent(sessionId)}/stop`, token, { method: 'POST', body: '{}' });
    stopped = true;

    const receipt = {
      schema_version: 'kenigevents_live_search_daily_canary_v1',
      outcome: 'PASS',
      model: 'gemini-3.8-live',
      functional_call: 'search_events',
      cards_observed: items.length + fallback.length,
      has_more: Boolean(resultEvent?.data?.has_more),
      elapsed_ms: Date.now() - startedAt,
      session_released: true,
    };
    const output = resolve(process.env.LIVE_SEARCH_CANARY_RECEIPT || 'artifacts/live-search-daily/receipt.json');
    await mkdir(dirname(output), { recursive: true });
    await writeFile(output, JSON.stringify(receipt, null, 2) + '\n', { mode: 0o600 });
    process.stdout.write(JSON.stringify(receipt) + '\n');
  } finally {
    if (fixture && sessionId && !stopped) {
      const token = await accessTokenFromState(fixture.storageStatePath).catch(() => '');
      if (token) await liveJson(`${liveBase()}/${encodeURIComponent(sessionId)}/stop`, token, { method: 'POST', body: '{}' }).catch(() => {});
    }
    await fixture?.cleanup?.().catch(() => undefined);
  }
}

main().catch((error) => {
  const message = String(error?.message || 'live_search_daily_failed')
    .replace(/https?:\/\/\S+/gu, '<redacted-url>')
    .replace(/Bearer\s+\S+/giu, 'Bearer <redacted>');
  process.stderr.write(message + '\n');
  process.exitCode = 1;
});
