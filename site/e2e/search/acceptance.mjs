const SEARCH_PATH = '/functions/v1/event-search';

const boundedString = (value, max = 96) => String(value ?? '').slice(0, max);
const asCount = (value) => Number.isFinite(Number(value)) && Number(value) >= 0 ? Number(value) : 0;
const stableId = (value) => {
  const id = boundedString(value, 80);
  return /^[a-z0-9][a-z0-9:_-]{0,79}$/iu.test(id) ? id : null;
};
const eventId = (item) => stableId(item?.event_id ?? item?.id ?? item?.display?.event_id ?? item?.display?.id);
const memberIds = (item) => Array.from(new Set((item?.occurrence_member_ids ?? item?.display?.occurrence_member_ids ?? [])
  .map((value) => stableId(value)).filter(Boolean))).sort();
const familyKey = (item) => {
  const members = memberIds(item);
  const id = eventId(item);
  return members.length > 1 ? `family:${members.join(',')}` : `event:${id || ''}`;
};

function dedupeFamilies(items, seen = new Set()) {
  const kept = [];
  for (const item of items) {
    const key = familyKey(item);
    if (!eventId(item) || seen.has(key)) continue;
    seen.add(key);
    kept.push(item);
  }
  return kept;
}

function visibleItems(payload) {
  const seen = new Set();
  const exact = dedupeFamilies(Array.isArray(payload?.items) ? payload.items : [], seen);
  const fallback = dedupeFamilies(Array.isArray(payload?.fallback_items) ? payload.fallback_items : [], seen);
  if (payload?.has_more === true && exact.length > 0) return exact;
  return [...exact, ...fallback];
}

export function summarizeSearchPayload(payload, extras = {}) {
  if (!payload || typeof payload !== 'object') return null;
  const visible = visibleItems(payload);
  const attempts = payload.provider_attempt_counters ?? payload.provider_attempts ?? {};
  const llm = payload.llm_verifier && typeof payload.llm_verifier === 'object' ? payload.llm_verifier : {};
  const requestedMode = payload.requested_execution_mode ?? payload.requested_mode ?? '';
  const actualMode = payload.actual_execution_mode ?? payload.execution_mode ?? '';
  return {
    schema_version: boundedString(payload.schema_version ?? payload.search_contract_version, 80),
    search_contract_version: boundedString(payload.search_contract_version ?? payload.schema_version, 80),
    request_id: stableId(payload.request_id),
    receipt_id: stableId(payload.receipt_id),
    response_ids: visible.map(eventId),
    response_families: visible.map(familyKey),
    item_count: Array.isArray(payload.items) ? payload.items.length : 0,
    fallback_count: Array.isArray(payload.fallback_items) ? payload.fallback_items.length : 0,
    has_more: payload.has_more === true,
    next_offset: asCount(payload.next_offset),
    result_cache_status: boundedString(payload.result_cache_status ?? payload.cache_status, 32).toLowerCase(),
    served_from_cache: payload.served_from_cache === true,
    requested_execution_mode: boundedString(requestedMode, 48),
    actual_execution_mode: boundedString(actualMode, 48),
    catalog_revision: boundedString(payload.catalog_revision, 96),
    corpus_revision: boundedString(payload.corpus_revision ?? payload.embedding_corpus_revision, 96),
    policy_versions: payload.policy_versions && typeof payload.policy_versions === 'object'
      ? Object.fromEntries(Object.entries(payload.policy_versions).filter(([key, value]) => /^[a-z0-9_.-]+$/iu.test(key) && typeof value === 'string').map(([key, value]) => [key, boundedString(value, 96)]))
      : {},
    provider_attempts: {
      embedding: asCount(attempts.embedding ?? attempts.embedding_provider ?? payload.embedding_provider_attempts),
      vector: asCount(attempts.vector ?? attempts.vector_rpc ?? payload.vector_attempts),
      llm: asCount(attempts.llm ?? attempts.verifier ?? payload.llm_provider_attempts),
    },
    provider_attempts_present: Boolean(
      (attempts && typeof attempts === 'object' && Object.keys(attempts).length > 0)
      || payload.embedding_provider_attempts != null || payload.vector_attempts != null || payload.llm_provider_attempts != null,
    ),
    llm: {
      requested: llm.requested === true,
      used: llm.used === true,
      status: boundedString(llm.status, 48),
    },
    http_status: asCount(extras.http_status),
    route: boundedString(extras.route, 24),
  };
}

export function activityDelta(before, after) {
  const slice = (key) => (Array.isArray(after?.[key]) ? after[key] : []).slice(Array.isArray(before?.[key]) ? before[key].length : 0);
  return { requests: slice('requests'), responses: slice('responses'), routes: slice('routes') };
}

export function assertOneSubmitOnePost(delta, label = 'submit') {
  const posts = delta.requests.filter((entry) => entry.method === 'POST' && entry.path === SEARCH_PATH);
  if (posts.length !== 1) throw new Error(`search_duplicate_post:${label}:${posts.length}`);
  if (delta.responses.length !== 1) throw new Error(`search_response_count:${label}:${delta.responses.length}`);
  return { request_count: posts.length, response_count: delta.responses.length };
}

export function assertTerminalState(state, label = 'result') {
  if (!state?.terminal || state.error) throw new Error(`search_terminal_missing:${label}`);
  if (!state.cards_visible || asCount(state.visible_card_count) < 1) throw new Error(`search_cards_not_visible:${label}`);
  if (state.card_renderer_unavailable) throw new Error(`search_card_renderer_unavailable:${label}`);
}

export function assertUniqueCards(state, label = 'result') {
  const ids = Array.isArray(state?.rendered_ids) ? state.rendered_ids : [];
  const families = Array.isArray(state?.rendered_families) ? state.rendered_families : [];
  if (ids.length !== new Set(ids).size) throw new Error(`search_duplicate_ids:${label}`);
  if (families.length !== new Set(families).size) throw new Error(`search_duplicate_families:${label}`);
}

export function assertResponseRenderedIds(response, state, { prefixIds = [] } = {}) {
  const expected = [...prefixIds, ...(response?.response_ids || [])];
  const actual = state?.rendered_ids || [];
  if (expected.length !== actual.length || expected.some((id, index) => id !== actual[index])) {
    throw new Error(`search_response_rendered_ids_mismatch:${expected.length}:${actual.length}`);
  }
}

export function assertRoute(delta, variant, label = 'submit') {
  if (delta.routes.length !== 1) throw new Error(`search_route_count:${label}:${delta.routes.length}`);
  const receipt = delta.routes[0];
  if (receipt.policy !== 'selected-once') throw new Error(`search_route_policy:${label}:${receipt.policy || 'missing'}`);
  if (!['direct', 'relay'].includes(receipt.route)) throw new Error(`search_route_missing:${label}`);
  if (receipt.status < 200 || receipt.status >= 300) throw new Error(`search_route_status:${label}:${receipt.status}`);
  if (variant?.request_policy?.selected_once !== true) throw new Error('search_variant_not_selected_once');
  return receipt.route;
}

export function assertCacheState(response, expected, label = 'initial') {
  const cacheState = boundedString(response?.result_cache_status, 32).toLowerCase();
  const accepted = Array.isArray(expected) ? expected : [expected];
  if (!accepted.includes(cacheState) && !(accepted.includes('hit') && response?.served_from_cache === true)) {
    throw new Error(`search_cache_state:${label}:${cacheState || 'missing'}`);
  }
}

export function assertProviderAttempts(response, limits) {
  if (response?.provider_attempts_present !== true) throw new Error('search_provider_attempt_counters_missing');
  for (const key of ['embedding', 'vector', 'llm']) {
    const actual = asCount(response?.provider_attempts?.[key]);
    const maximum = asCount(limits?.[key]);
    if (actual > maximum) throw new Error(`search_provider_attempts:${key}:${actual}:${maximum}`);
  }
}

export function assertExecutionReceipt(response, requestedMode, label = 'result') {
  const closed = new Set(['cached_vector', 'cold_vector', 'cold_vector_llm', 'degraded_vector_fallback']);
  const requested = boundedString(response?.requested_execution_mode, 48);
  const actual = boundedString(response?.actual_execution_mode, 48);
  if (requested !== requestedMode || !closed.has(actual)) {
    throw new Error(`search_execution_receipt:${label}:${requested || 'missing'}:${actual || 'missing'}`);
  }
  const allowedActual = {
    cached_vector: new Set(['cached_vector', 'cold_vector']),
    cold_vector: new Set(['cold_vector']),
    cold_vector_llm: new Set(['cold_vector_llm', 'degraded_vector_fallback']),
    degraded_vector_fallback: new Set(['degraded_vector_fallback']),
  }[requestedMode];
  if (!allowedActual?.has(actual)) throw new Error(`search_actual_execution_mode:${label}:${actual}`);
}

export function assertSearchRevisionPolicy(journey, identity, policy = 'release_exact') {
  if (!['release_exact', 'live_consistent'].includes(policy)) throw new Error('search_revision_policy_invalid');
  const revisions = [];
  for (const queryCase of journey?.query_cases || []) {
    const responses = [
      ...(queryCase.pages || []).map((page) => page.response),
      queryCase.cache_warm?.response,
      queryCase.cache_repeat?.response,
    ].filter(Boolean);
    for (const response of responses) {
      const catalog = String(response.catalog_revision || '');
      const corpus = String(response.corpus_revision || '');
      if (!/^[0-9a-f]{64}$/u.test(catalog) || !/^[0-9a-f]{64}$/u.test(corpus)) {
        throw new Error('search_response_revision_missing');
      }
      if (policy === 'release_exact'
        && (catalog !== identity.catalogRevision || corpus !== identity.corpusRevision)) {
        throw new Error('search_target_response_revision_mismatch');
      }
      revisions.push(`${catalog}:${corpus}`);
    }
  }
  if (revisions.length < 1) throw new Error('search_response_revision_missing');
  const unique = new Set(revisions);
  if (unique.size !== 1) throw new Error('search_response_revision_changed_during_journey');
  const [catalog_revision, corpus_revision] = revisions[0].split(':');
  return { policy, catalog_revision, corpus_revision, response_count: revisions.length };
}

export function assertRealScroll(receipt) {
  if (!receipt?.performed || asCount(Math.abs(receipt.delta_y)) < 1 || !receipt.card_visible_after) {
    const error = new Error('search_real_scroll_missing');
    const gesture = receipt?.last_gesture && typeof receipt.last_gesture === 'object' ? receipt.last_gesture : {};
    const numeric = (value) => Number.isFinite(Number(value)) ? Number(value) : 0;
    error.searchReceipt = {
      performed: receipt?.performed === true,
      delta_y: numeric(receipt?.delta_y),
      card_visible_after: receipt?.card_visible_after === true,
      gesture_count: asCount(receipt?.gesture_count),
      route: ['w3c_native_touch', 'xcuitest_native_swipe'].includes(gesture.route)
        ? gesture.route : 'unreported',
      native_viewport_width: numeric(gesture.native_viewport_width),
      native_viewport_height: numeric(gesture.native_viewport_height),
      start_x: numeric(gesture.start_x), start_y: numeric(gesture.start_y),
      end_x: numeric(gesture.end_x), end_y: numeric(gesture.end_y),
      duration_ms: numeric(gesture.duration_ms),
    };
    throw error;
  }
}

export function assertZeroPost(before, after, label) {
  const delta = activityDelta(before, after);
  const count = delta.requests.filter((entry) => entry.method === 'POST' && entry.path === SEARCH_PATH).length;
  if (count !== 0 || delta.responses.length !== 0) throw new Error(`search_validation_post:${label}:${count}`);
}

export const SEARCH_ENDPOINT_PATH = SEARCH_PATH;
