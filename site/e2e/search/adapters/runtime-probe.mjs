// This function is serialized into the product page by the mechanics adapters.
// Keep it self-contained and retain only the allowlisted receipt fields below.
export function installSearchRuntimeProbe(nextPolicy = {}) {
  const key = '__KENIGEVENTS_SEARCH_HARNESS_V1__';
  const existing = globalThis[key];
  if (existing) {
    existing.policy = { ...nextPolicy };
    return { installed: true, wrapped: existing.wrapped };
  }
  const state = {
    policy: { ...nextPolicy }, requests: [], responses: [], routes: [], route_ids: [],
    sequence: 0, wrapped: 'none',
  };
  globalThis[key] = state;
  const path = '/functions/v1/event-search';
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
  const count = (value) => Number.isFinite(Number(value)) && Number(value) >= 0 ? Number(value) : 0;
  const summarize = (payload, httpStatus, route) => {
    const shown = visible(payload);
    const attempts = payload?.provider_attempt_counters ?? payload?.provider_attempts ?? {};
    const llm = payload?.llm_verifier && typeof payload.llm_verifier === 'object' ? payload.llm_verifier : {};
    return {
      schema_version: String(payload?.schema_version ?? payload?.search_contract_version ?? '').slice(0, 80),
      search_contract_version: String(payload?.search_contract_version ?? payload?.schema_version ?? '').slice(0, 80),
      request_id: id(payload?.request_id),
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
      policy_versions: payload?.policy_versions && typeof payload.policy_versions === 'object'
        ? Object.fromEntries(Object.entries(payload.policy_versions).filter(([name, value]) => /^[a-z0-9_.-]+$/iu.test(name) && typeof value === 'string').map(([name, value]) => [name, value.slice(0, 96)])) : {},
      provider_attempts: {
        embedding: count(attempts.embedding ?? attempts.embedding_provider ?? payload?.embedding_provider_attempts),
        vector: count(attempts.vector ?? attempts.vector_rpc ?? payload?.vector_attempts),
        llm: count(attempts.llm ?? attempts.verifier ?? payload?.llm_provider_attempts),
      },
      provider_attempts_present: Boolean((attempts && typeof attempts === 'object' && Object.keys(attempts).length > 0)
        || payload?.embedding_provider_attempts != null || payload?.vector_attempts != null || payload?.llm_provider_attempts != null),
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
  const routeFromUrl = (raw) => {
    try {
      const url = new URL(String(raw), location.href);
      const root = document.querySelector('[data-authorized-search]');
      const relay = root?.dataset.supabaseRelayUrl ? new URL(root.dataset.supabaseRelayUrl, location.href).host : '';
      return relay && url.host === relay ? 'relay' : 'direct';
    } catch { return 'unknown'; }
  };
  const patchBody = (init) => {
    const executionMode = String(state.policy?.execution_mode || '');
    if (!executionMode || typeof init?.body !== 'string') return init;
    try {
      const body = JSON.parse(init.body);
      const clientRequestId = typeof body.client_request_id === 'string' && /^[0-9a-f-]{36}$/iu.test(body.client_request_id)
        ? body.client_request_id : crypto.randomUUID();
      return { ...init, body: JSON.stringify({ ...body, client_request_id: clientRequestId, execution_mode: executionMode }) };
    } catch { return init; }
  };
  const wrap = (owner, property, label) => {
    const original = owner?.[property];
    if (typeof original !== 'function' || original.__keSearchHarnessWrapped) return false;
    const wrapped = async function searchHarnessFetch(input, init = {}) {
      let url;
      try { url = new URL(typeof input === 'string' || input instanceof URL ? input : input.url, location.href); }
      catch { return original.call(this, input, init); }
      if (!url.pathname.endsWith(path) || String(init?.method || input?.method || 'GET').toUpperCase() !== 'POST') {
        return original.call(this, input, init);
      }
      const sequence = ++state.sequence;
      const route = routeFromUrl(url.href);
      state.requests.push({ sequence, method: 'POST', path, route });
      const response = await original.call(this, input, patchBody(init));
      try {
        const payload = await responsePayload(response);
        state.responses.push({ sequence, ...summarize(payload, response.status, route) });
      } catch {
        state.responses.push({ sequence, schema_version: '', search_contract_version: '', request_id: null,
          response_ids: [], response_families: [], item_count: 0, fallback_count: 0, has_more: false,
          next_offset: 0, result_cache_status: 'unreadable', served_from_cache: false,
          requested_execution_mode: '', actual_execution_mode: '', catalog_revision: '', corpus_revision: '',
          policy_versions: {}, provider_attempts: { embedding: 0, vector: 0, llm: 0 },
          provider_attempts_present: false,
          llm: { requested: false, used: false, status: '' }, http_status: count(response.status), route });
      }
      return response;
    };
    wrapped.__keSearchHarnessWrapped = true;
    owner[property] = wrapped;
    state.wrapped = label;
    return true;
  };
  const clients = globalThis.__KENIGEVENTS_RESILIENT_DATA_CLIENTS_V1__;
  let wrappedTransport = false;
  if (clients instanceof Map) {
    for (const client of clients.values()) wrappedTransport = wrap(client?.transport, 'fetch', 'resilient_transport') || wrappedTransport;
  }
  if (!wrappedTransport) wrap(globalThis, 'fetch', 'window_fetch');
  return { installed: true, wrapped: state.wrapped };
}

export function snapshotSearchRuntimeProbe() {
  const state = globalThis.__KENIGEVENTS_SEARCH_HARNESS_V1__;
  if (!state) return { requests: [], responses: [], routes: [], wrapped: 'none' };
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
          policy: String(outcome.policy || '').slice(0, 32),
          route: String(outcome.finalRoute || '').slice(0, 16),
          initial_route: String(outcome.initialRoute || '').slice(0, 16),
          kind: String(outcome.kind || '').slice(0, 32),
          status: Number(outcome.status || 0),
        });
      }
    }
  }
  const copy = (items) => items.map((item) => JSON.parse(JSON.stringify(item)));
  return { requests: copy(state.requests), responses: copy(state.responses.sort((a, b) => a.sequence - b.sequence)),
    routes: copy(state.routes), wrapped: state.wrapped };
}
