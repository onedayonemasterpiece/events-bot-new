// This function is serialized into the product page by the mechanics adapters.
// It is deliberately self-contained and retains only closed, non-content
// counters/receipts. Never add query text, URLs, headers, bodies or raw errors.
export function installSearchRuntimeProbe(nextPolicy = {}) {
  const key = '__KENIGEVENTS_SEARCH_HARNESS_V1__';
  const searchPath = '/functions/v1/event-search';
  const targetBytes = 48 * 1024;
  const hardBytes = 96 * 1024;
  const count = (value) => Number.isSafeInteger(Number(value)) && Number(value) >= 0 ? Number(value) : 0;
  const state = globalThis[key] || {
    policy: {}, requests: [], responses: [], routes: [], route_ids: [], sequence: 0,
    wrapped: 'none', measure_global: true, transport_active: false,
    network: { storage_requests: 0, receipt_rpc_requests: 0, failed_requests: 0 },
    meter: {
      categories: { auth: 0, edge: 0, direct_rest: 0, direct_rpc: 0 },
      sources: { content_length: 0, received_body: 0 }, excluded_requests: 0, pending: 0,
    },
  };
  globalThis[key] = state;
  state.policy = { ...nextPolicy };

  const absoluteUrl = (input) => {
    try { return new URL(typeof input === 'string' || input instanceof URL ? input : input.url, location.href); }
    catch { return null; }
  };
  const classify = (url) => {
    const root = document.querySelector('[data-authorized-search]');
    const origins = new Set();
    for (const value of [root?.dataset.supabaseUrl, root?.dataset.supabaseRelayUrl]) {
      try { if (value) origins.add(new URL(value, location.href).origin); } catch { /* excluded */ }
    }
    if (!url || !origins.has(url.origin)) return 'excluded';
    if (url.pathname === '/auth/v1' || url.pathname.startsWith('/auth/v1/')) return 'auth';
    if (url.pathname === '/functions/v1' || url.pathname.startsWith('/functions/v1/')) return 'edge';
    if (url.pathname === '/rest/v1/rpc' || url.pathname.startsWith('/rest/v1/rpc/')) return 'direct_rpc';
    if (url.pathname === '/rest/v1' || url.pathname.startsWith('/rest/v1/')) return 'direct_rest';
    return 'excluded';
  };
  const receivedBytes = async (response) => {
    const raw = response?.headers?.get?.('content-length');
    const parsed = raw == null || raw === '' ? null : Number(raw);
    if (Number.isSafeInteger(parsed) && parsed >= 0) return { bytes: parsed, source: 'content_length' };
    const body = await response.clone().arrayBuffer();
    return { bytes: body.byteLength, source: 'received_body' };
  };
  const meterSnapshot = () => {
    const categories = { ...state.meter.categories };
    const total = Object.values(categories).reduce((sum, value) => sum + count(value), 0);
    return {
      schema_version: 'supabase_client_observed_bytes_v1',
      measurement_basis: 'client_observed_response_bytes',
      total_bytes: total, target_bytes: targetBytes, hard_limit_bytes: hardBytes,
      budget_status: total <= targetBytes ? 'within_target' : total <= hardBytes ? 'above_target' : 'hard_limit_exceeded',
      target_met: total <= targetBytes, cost_guard_passed: total <= hardBytes,
      hard_limit_exceeded: total > hardBytes, categories,
      sources: { ...state.meter.sources }, excluded_requests: count(state.meter.excluded_requests),
    };
  };
  const observeRequest = (url) => {
    if (!url) return;
    const root = document.querySelector('[data-authorized-search]');
    let supabaseOrigin = '';
    try { supabaseOrigin = new URL(root?.dataset.supabaseUrl || '', location.href).origin; } catch { /* none */ }
    if (url.origin !== supabaseOrigin) return;
    if (url.pathname === '/storage/v1' || url.pathname.startsWith('/storage/v1/')) state.network.storage_requests += 1;
    if (/^\/rest\/v1\/rpc\/get_event_search_receipt(?:_|$)/u.test(url.pathname)) state.network.receipt_rpc_requests += 1;
  };
  const measure = async (url, response) => {
    observeRequest(url);
    const category = classify(url);
    if (category === 'excluded') {
      state.meter.excluded_requests += 1;
      return;
    }
    const measured = await receivedBytes(response);
    state.meter.categories[category] += measured.bytes;
    state.meter.sources[measured.source] += measured.bytes;
    if (meterSnapshot().hard_limit_exceeded) throw new Error('search_health_supabase_hard_limit_exceeded');
  };
  const scheduleMeasure = (url, response) => {
    state.meter.pending = count(state.meter.pending) + 1;
    void measure(url, response).catch(() => {
      // The closed meter state, not raw error material, is the evidence. A hard
      // cap is checked before the journey continues beyond terminal Search.
    }).finally(() => { state.meter.pending = Math.max(0, count(state.meter.pending) - 1); });
  };

  const id = (value) => {
    const text = String(value ?? '').slice(0, 80);
    return /^[a-z0-9][a-z0-9:_-]{0,79}$/iu.test(text) ? text : null;
  };
  const eventId = (item) => id(item?.event_id ?? item?.id ?? item?.display?.event_id ?? item?.display?.id);
  const members = (item) => Array.from(new Set((item?.occurrence_member_ids ?? item?.display?.occurrence_member_ids ?? [])
    .map(id).filter(Boolean))).sort();
  const family = (item) => {
    const ids = members(item);
    return ids.length > 1 ? `family:${ids.join(',')}` : `event:${eventId(item) || ''}`;
  };
  const visible = (payload) => {
    const seen = new Set();
    const dedupe = (items) => (Array.isArray(items) ? items : []).filter((item) => {
      const familyKey = family(item);
      if (!eventId(item) || seen.has(familyKey)) return false;
      seen.add(familyKey);
      return true;
    });
    const exact = dedupe(payload?.items);
    const fallback = dedupe(payload?.fallback_items);
    return payload?.has_more === true && exact.length > 0 ? exact : [...exact, ...fallback];
  };
  const providerCounters = (payload) => {
    const actual = payload?.request_counters && typeof payload.request_counters === 'object'
      ? payload.request_counters : null;
    const legacy = payload?.provider_attempt_counters ?? payload?.provider_attempts ?? {};
    const values = actual || legacy;
    const has = (name) => Object.prototype.hasOwnProperty.call(values || {}, name);
    return {
      embedding: count(actual ? values.embedding_provider_attempts : values.embedding ?? values.embedding_provider ?? payload?.embedding_provider_attempts),
      vector: count(actual ? values.vector_rpc_attempts : values.vector ?? values.vector_rpc ?? payload?.vector_attempts),
      llm: count(actual ? values.llm_provider_attempts : values.llm ?? values.verifier ?? payload?.llm_provider_attempts),
      present: actual
        ? ['embedding_provider_attempts', 'vector_rpc_attempts', 'llm_provider_attempts'].every(has)
        : Boolean(Object.keys(values || {}).length || payload?.embedding_provider_attempts != null
          || payload?.vector_attempts != null || payload?.llm_provider_attempts != null),
      source: actual ? 'request_counters' : 'legacy_alias',
    };
  };
  const summarize = (payload, httpStatus, route) => {
    const shown = visible(payload);
    const attempts = providerCounters(payload);
    const llm = payload?.llm_verifier && typeof payload.llm_verifier === 'object' ? payload.llm_verifier : {};
    return {
      schema_version: String(payload?.schema_version ?? payload?.search_contract_version ?? '').slice(0, 80),
      search_contract_version: String(payload?.search_contract_version ?? payload?.schema_version ?? '').slice(0, 80),
      request_id: id(payload?.request_id), receipt_id: id(payload?.receipt_id),
      response_ids: shown.map(eventId), response_families: shown.map(family),
      item_count: Array.isArray(payload?.items) ? payload.items.length : 0,
      fallback_count: Array.isArray(payload?.fallback_items) ? payload.fallback_items.length : 0,
      has_more: payload?.has_more === true, next_offset: count(payload?.next_offset),
      result_cache_status: String(payload?.result_cache_status ?? payload?.cache_status ?? '').slice(0, 32).toLowerCase(),
      served_from_cache: payload?.served_from_cache === true,
      requested_execution_mode: String(payload?.requested_execution_mode ?? payload?.requested_mode ?? '').slice(0, 48),
      actual_execution_mode: String(payload?.actual_execution_mode ?? payload?.execution_mode ?? '').slice(0, 48),
      catalog_revision: String(payload?.catalog_revision ?? '').slice(0, 96),
      corpus_revision: String(payload?.corpus_revision ?? payload?.embedding_corpus_revision ?? '').slice(0, 96),
      search_document_revision: String(payload?.search_document_revision ?? '').slice(0, 96),
      policy_versions: payload?.policy_versions && typeof payload.policy_versions === 'object'
        ? Object.fromEntries(Object.entries(payload.policy_versions)
          .filter(([name, value]) => /^[a-z0-9_.-]+$/iu.test(name) && typeof value === 'string')
          .map(([name, value]) => [name, value.slice(0, 96)])) : {},
      provider_attempts: { embedding: attempts.embedding, vector: attempts.vector, llm: attempts.llm },
      provider_attempts_present: attempts.present, provider_attempts_source: attempts.source,
      llm: { requested: llm.requested === true, used: llm.used === true, status: String(llm.status ?? '').slice(0, 48) },
      http_status: count(httpStatus), route: String(route ?? '').slice(0, 24),
    };
  };
  const responsePayload = async (response) => {
    const text = await response.clone().text();
    if (text.length > 2_000_000) throw new Error('response_too_large');
    const contentType = response.headers?.get?.('content-type') || '';
    if (!contentType.includes('ndjson')) return JSON.parse(text || '{}');
    let final = null;
    for (const line of text.split('\n')) {
      if (!line.trim()) continue;
      const event = JSON.parse(line);
      if (event?.type === 'result') final = event.data;
    }
    return final || {};
  };
  const routeFromUrl = (url) => {
    try {
      const root = document.querySelector('[data-authorized-search]');
      const relay = root?.dataset.supabaseRelayUrl ? new URL(root.dataset.supabaseRelayUrl, location.href).host : '';
      return relay && url.host === relay ? 'relay' : 'direct';
    } catch { return 'unknown'; }
  };
  const patchBody = (init) => {
    if (typeof init?.body !== 'string') return init;
    try {
      const body = JSON.parse(init.body);
      const clientRequestId = typeof body.client_request_id === 'string' && /^[0-9a-f-]{36}$/iu.test(body.client_request_id)
        ? body.client_request_id : crypto.randomUUID();
      if (state.policy?.production_health === true) {
        const next = {
          ...body, limit: 5, offset: 0, use_llm_verifier: false,
          allow_llm_fallback: false, client_request_id: clientRequestId,
        };
        delete next.execution_mode;
        return { ...init, body: JSON.stringify(next) };
      }
      const executionMode = String(state.policy?.execution_mode || '');
      return executionMode
        ? { ...init, body: JSON.stringify({ ...body, client_request_id: clientRequestId, execution_mode: executionMode }) }
        : init;
    } catch { return init; }
  };
  const requestContract = (init) => {
    try {
      const body = JSON.parse(typeof init?.body === 'string' ? init.body : '{}');
      return {
        limit: count(body.limit), offset: count(body.offset),
        use_llm_verifier: body.use_llm_verifier === true,
        allow_llm_fallback: body.allow_llm_fallback === true,
        execution_mode_present: Object.prototype.hasOwnProperty.call(body, 'execution_mode'),
      };
    } catch {
      return { limit: 0, offset: 0, use_llm_verifier: true, allow_llm_fallback: true, execution_mode_present: true };
    }
  };
  const wrap = (owner, property, label, transport = false) => {
    const original = owner?.[property];
    if (typeof original !== 'function' || original.__keSearchHarnessWrapped) return false;
    const wrapped = async function searchHealthTransport(input, init = {}) {
      // A client may capture the init-time global wrapper as its raw fetch
      // before its resilient transport is registered. Once that transport is
      // instrumented it is the sole physical-request boundary; the captured
      // global wrapper becomes a transparent fallback instead of recording the
      // same Search a second time.
      if (!transport && state.transport_active === true) {
        return original.call(this, input, init);
      }
      const url = absoluteUrl(input);
      const method = String(init?.method || input?.method || 'GET').toUpperCase();
      const isSearch = Boolean(url?.pathname.endsWith(searchPath) && method === 'POST');
      const nextInit = isSearch ? patchBody(init) : init;
      let sequence = 0;
      let route = 'unknown';
      if (isSearch) {
        sequence = ++state.sequence;
        route = routeFromUrl(url);
        state.requests.push({ sequence, method: 'POST', path: searchPath, route, body_contract: requestContract(nextInit) });
      }
      let response;
      try {
        response = await original.call(this, input, nextInit);
        if (transport || state.measure_global) scheduleMeasure(url, response);
      } catch (error) {
        state.network.failed_requests += 1;
        throw error;
      }
      if (isSearch) {
        const retain = (payload) => {
          state.responses.push({ sequence, ...summarize(payload, response.status, route) });
        };
        const retainUnreadable = () => {
          state.responses.push({ sequence, schema_version: '', search_contract_version: '', request_id: null,
            receipt_id: null, response_ids: [], response_families: [], item_count: 0, fallback_count: 0,
            has_more: false, next_offset: 0, result_cache_status: 'unreadable', served_from_cache: false,
            requested_execution_mode: '', actual_execution_mode: '', catalog_revision: '', corpus_revision: '',
            search_document_revision: '', policy_versions: {}, provider_attempts: { embedding: 0, vector: 0, llm: 0 },
            provider_attempts_present: false, provider_attempts_source: 'missing',
            llm: { requested: false, used: false, status: '' }, http_status: count(response.status), route });
        };
        const parsed = responsePayload(response);
        if (state.policy?.production_health === true) void parsed.then(retain).catch(retainUnreadable);
        else await parsed.then(retain).catch(retainUnreadable);
      }
      return response;
    };
    wrapped.__keSearchHarnessWrapped = true;
    owner[property] = wrapped;
    state.wrapped = label;
    return true;
  };

  if (!globalThis.fetch?.__keSearchHarnessWrapped) wrap(globalThis, 'fetch', 'window_fetch', false);
  const clients = globalThis.__KENIGEVENTS_RESILIENT_DATA_CLIENTS_V1__;
  let wrappedTransport = false;
  if (clients instanceof Map) {
    for (const client of clients.values()) {
      const property = typeof client?.transport?.request === 'function' ? 'request' : 'fetch';
      wrappedTransport = wrap(client?.transport, property, property === 'request'
        ? 'resilient_transport_request' : 'resilient_transport_legacy_fetch', true) || wrappedTransport;
    }
    if (wrappedTransport || [...clients.values()].some((client) => (
      client?.transport?.request?.__keSearchHarnessWrapped || client?.transport?.fetch?.__keSearchHarnessWrapped
    ))) {
      state.transport_active = true;
      state.measure_global = false;
    }
  }
  return { installed: true, wrapped: state.wrapped };
}

export function snapshotSearchRuntimeProbe() {
  const state = globalThis.__KENIGEVENTS_SEARCH_HARNESS_V1__;
  if (!state) return { requests: [], responses: [], routes: [], wrapped: 'none', network: {}, meter: null };
  const clients = globalThis.__KENIGEVENTS_RESILIENT_DATA_CLIENTS_V1__;
  if (clients instanceof Map) {
    for (const client of clients.values()) {
      const outcomes = client?.transport?.outcomeHistory?.() || [];
      for (const outcome of outcomes) {
        if (outcome?.operation !== 'functions.event-search') continue;
        const routeId = String(outcome.operationId || `${outcome.startedAt}:${outcome.finalRoute}:${outcome.status}`);
        if (state.route_ids.includes(routeId)) continue;
        state.route_ids.push(routeId);
        state.routes.push({
          operation_id: /^[a-z0-9:_-]{1,96}$/iu.test(routeId) ? routeId : null,
          policy: String(outcome.policy || '').slice(0, 32), route: String(outcome.finalRoute || '').slice(0, 16),
          initial_route: String(outcome.initialRoute || '').slice(0, 16), kind: String(outcome.kind || '').slice(0, 32),
          status: Number(outcome.status || 0),
        });
      }
    }
  }
  const copy = (value) => JSON.parse(JSON.stringify(value));
  const categories = copy(state.meter.categories);
  const total = Object.values(categories).reduce((sum, value) => sum + Number(value || 0), 0);
  const meter = {
    schema_version: 'supabase_client_observed_bytes_v1', measurement_basis: 'client_observed_response_bytes',
    total_bytes: total, target_bytes: 48 * 1024, hard_limit_bytes: 96 * 1024,
    budget_status: total <= 48 * 1024 ? 'within_target' : total <= 96 * 1024 ? 'above_target' : 'hard_limit_exceeded',
    target_met: total <= 48 * 1024, cost_guard_passed: total <= 96 * 1024,
    hard_limit_exceeded: total > 96 * 1024, categories, sources: copy(state.meter.sources),
    excluded_requests: Number(state.meter.excluded_requests || 0),
  };
  meter.pending_measurements = Number(state.meter.pending || 0);
  return {
    requests: copy(state.requests), responses: copy([...state.responses].sort((a, b) => a.sequence - b.sequence)),
    routes: copy(state.routes), wrapped: state.wrapped, network: copy(state.network), meter,
  };
}

/**
 * Serialized into an already-authorized page after the runtime probe is
 * installed. It performs the existing authenticated user endpoint check and
 * exactly one owner-scoped, read-only RLS request through the registered
 * resilient data client. Credentials never leave the page realm.
 */
export async function verifyAuthenticatedOwnerRuntimeProbe() {
  const root = document.querySelector('[data-authorized-search]');
  const supabaseUrl = String(root?.dataset.supabaseUrl || '').replace(/\/+$/u, '');
  const publishableKey = String(root?.dataset.supabaseKey || '');
  if (!supabaseUrl || !publishableKey) throw new Error('search_health_authenticated_owner_config_missing');
  const projectRef = new URL(supabaseUrl).hostname.split('.')[0];
  let stored;
  try { stored = JSON.parse(localStorage.getItem(`sb-${projectRef}-auth-token`) || '{}'); }
  catch { throw new Error('search_health_authenticated_owner_session_invalid'); }
  const accessToken = String(stored?.access_token || '');
  if (!accessToken) throw new Error('search_health_authenticated_owner_session_missing');
  const clients = globalThis.__KENIGEVENTS_RESILIENT_DATA_CLIENTS_V1__;
  const client = clients instanceof Map
    ? [...clients.values()].find((item) => typeof item?.request === 'function' && String(item?.key || '').startsWith(`${supabaseUrl}|`))
    : null;
  if (!client) throw new Error('search_health_authenticated_owner_transport_missing');
  const headers = {
    accept: 'application/json', apikey: publishableKey, authorization: `Bearer ${accessToken}`,
  };
  const identityResponse = await client.request(`${supabaseUrl}/auth/v1/user`, { method: 'GET', headers });
  if (!identityResponse.ok) throw new Error('search_health_authenticated_owner_get_user_failed');
  const identity = await identityResponse.json();
  const userId = String(identity?.id || '');
  if (!/^[0-9a-f-]{36}$/iu.test(userId)) throw new Error('search_health_authenticated_owner_identity_invalid');
  const owner = new URL('/rest/v1/user_saved_event', supabaseUrl);
  owner.searchParams.set('select', 'user_id');
  owner.searchParams.set('user_id', `eq.${userId}`);
  owner.searchParams.set('limit', '1');
  const ownerResponse = await client.request(owner, { method: 'GET', headers });
  if (!ownerResponse.ok) throw new Error('search_health_authenticated_owner_rls_failed');
  const rows = await ownerResponse.json();
  if (!Array.isArray(rows) || !rows.every((row) => String(row?.user_id || '') === userId)) {
    throw new Error('search_health_authenticated_owner_rls_scope_invalid');
  }
  const state = globalThis.__KENIGEVENTS_SEARCH_HARNESS_V1__;
  for (let attempt = 0; attempt < 100 && Number(state?.meter?.pending || 0) > 0; attempt += 1) {
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  if (Number(state?.meter?.pending || 0) > 0) throw new Error('search_health_authenticated_owner_meter_pending');
  return {
    get_user_verified: true, protected_probe_verified: true, protected_probe_request_count: 1,
    product_otp_issue_count: 0, external_mail_send_count: 0, external_mail_receipt_count: 0,
    real_mail_fallback: 'forbidden',
  };
}
