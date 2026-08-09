import {
  activityDelta,
  assertCacheState,
  assertOneSubmitOnePost,
  assertRealScroll,
  assertResponseRenderedIds,
  assertRoute,
  assertTerminalState,
  assertUniqueCards,
} from './acceptance.mjs';
export const PRODUCTION_HEALTH_UI_QUERY = 'куда сходить в Калининграде';
export const PRODUCTION_HEALTH_CACHE_STATES = Object.freeze(['hit', 'miss', 'stored']);
export const PRODUCTION_HEALTH_REQUEST_POLICY = Object.freeze({
  production_health: true,
  selected_once: true,
});

const requiredMethods = [
  'open', 'inspectSurface', 'configureRequestPolicy', 'activity', 'healthDiagnostics',
  'typeQuery', 'submitWithSearchIntent', 'waitForTerminal', 'snapshotResults',
  'realScrollResults', 'openFirstResult',
];

const finiteDelta = (after, before, key) => {
  const value = Number(after?.[key] || 0) - Number(before?.[key] || 0);
  return Number.isSafeInteger(value) && value >= 0 ? value : -1;
};

function assertAdapter(adapter) {
  for (const method of requiredMethods) {
    if (typeof adapter?.[method] !== 'function') throw new Error(`search_health_adapter_method_missing:${method}`);
  }
}

function assertSurface(surface) {
  if (!surface?.enabled || !surface?.authorized) throw new Error('search_health_surface_not_authorized');
  if (!['input', 'textarea'].includes(surface.input_tag)) throw new Error('search_health_surface_input_invalid');
  if (surface.enter_key_hint !== 'search') throw new Error('search_health_surface_enter_key_invalid');
}

function assertNormalVectorRequest(request) {
  const body = request?.body_contract;
  if (
    body?.limit !== 5
    || body?.offset !== 0
    || body?.use_llm_verifier !== false
    || body?.allow_llm_fallback !== false
    || body?.execution_mode_present !== false
  ) throw new Error('search_health_request_contract_invalid');
}

function assertVectorResponse(response) {
  if (!response || response.http_status < 200 || response.http_status >= 300) {
    throw new Error('search_health_response_http_invalid');
  }
  assertCacheState(response, PRODUCTION_HEALTH_CACHE_STATES, 'production_health');
  if (response.provider_attempts_present !== true || response.provider_attempts_source !== 'request_counters') {
    throw new Error('search_health_request_counters_missing');
  }
  if (Number(response.provider_attempts?.llm) !== 0 || response.llm?.requested || response.llm?.used) {
    throw new Error('search_health_llm_activity_forbidden');
  }
  if (response.requested_execution_mode !== 'cold_vector' || response.actual_execution_mode !== 'cold_vector') {
    throw new Error('search_health_not_normal_vector');
  }
}

function assertCards(response, state) {
  assertTerminalState(state, 'production_health');
  assertUniqueCards(state, 'production_health');
  const count = state?.rendered_ids?.length || 0;
  if (count < 1) throw new Error('search_health_no_results');
  if (count > 5 || state.skeleton_count !== 0 || state.placeholder_count !== 0) {
    throw new Error('search_health_result_render_invalid');
  }
  if (!state.rendered_ids.every((id) => /^[1-9][0-9]*$/u.test(String(id)))) {
    throw new Error('search_health_result_id_invalid');
  }
  assertResponseRenderedIds(response, state);
  if (response.response_ids.length !== count || response.response_ids.length > 5) {
    throw new Error('search_health_response_card_count_invalid');
  }
}

function assertNoSearchErrors(before, after) {
  for (const key of ['console_errors', 'failed_requests', 'error_responses']) {
    if (finiteDelta(after, before, key) !== 0) throw new Error('search_health_console_network_error');
  }
}

/**
 * One bounded product-health UI journey.  The mechanics adapter owns browser
 * or native input/scroll/navigation; this sequencer never imports either.
 */
export async function runProductionHealthJourney({ adapter, targetUrl, now = () => performance.now() }) {
  assertAdapter(adapter);
  await adapter.configureRequestPolicy(PRODUCTION_HEALTH_REQUEST_POLICY);
  await adapter.open(targetUrl);
  const surface = await adapter.inspectSurface();
  assertSurface(surface);

  // Snapshot after auth/surface initialization. Only the following UI intent
  // may contribute a Search POST to this journey.
  const before = await adapter.activity();
  const diagnosticsBefore = await adapter.healthDiagnostics();
  await adapter.typeQuery(PRODUCTION_HEALTH_UI_QUERY);
  const searchStartedAt = Number(now());
  await adapter.submitWithSearchIntent();
  await adapter.waitForTerminal({ minimumResponseCount: before.responses.length + 1, minimumCardCount: 1 });
  const searchLatencyMs = Math.max(0, Math.round(Number(now()) - searchStartedAt));
  const after = await adapter.activity();
  const delta = activityDelta(before, after);
  assertOneSubmitOnePost(delta, 'production_health');
  const searchRequests = delta.requests.filter((item) => item.method === 'POST' && item.path === '/functions/v1/event-search');
  assertNormalVectorRequest(searchRequests[0]);
  const response = delta.responses[0];
  assertVectorResponse(response);
  const state = await adapter.snapshotResults();
  assertCards(response, state);
  const route = assertRoute(delta, { request_policy: PRODUCTION_HEALTH_REQUEST_POLICY }, 'production_health');

  const network = after.network || {};
  if (Number(network.receipt_rpc_requests || 0) !== Number(before.network?.receipt_rpc_requests || 0)) {
    throw new Error('search_health_receipt_rpc_forbidden');
  }
  if (Number(network.storage_requests || 0) !== Number(before.network?.storage_requests || 0)) {
    throw new Error('search_health_storage_forbidden');
  }
  if (after.meter?.hard_limit_exceeded === true) throw new Error('search_health_supabase_hard_limit_exceeded');

  const scroll = await adapter.realScrollResults();
  assertRealScroll(scroll);
  const diagnosticsAfter = await adapter.healthDiagnostics();
  assertNoSearchErrors(diagnosticsBefore, diagnosticsAfter);
  if (finiteDelta(diagnosticsAfter, diagnosticsBefore, 'storage_requests') !== 0) {
    throw new Error('search_health_storage_forbidden');
  }
  const eventRoute = await adapter.openFirstResult();
  if (eventRoute?.same_origin !== true || Number(eventRoute?.http_status) !== 200) {
    throw new Error('search_health_event_route_invalid');
  }
  const diagnosticsFinal = await adapter.healthDiagnostics();
  assertNoSearchErrors(diagnosticsBefore, diagnosticsFinal);
  if (finiteDelta(diagnosticsFinal, diagnosticsBefore, 'storage_requests') !== 0) {
    throw new Error('search_health_storage_forbidden');
  }

  return Object.freeze({
    schema_version: 'search_production_health_journey_v1',
    status: 'PASS',
    latency_ms: searchLatencyMs,
    search_post_count: 1,
    request_contract: Object.freeze({
      limit: 5, offset: 0, use_llm_verifier: false,
      allow_llm_fallback: false, explicit_execution_mode: false,
    }),
    cache_state: response.result_cache_status,
    response_telemetry: Object.freeze({
      request_id: response.request_id,
      http_status: response.http_status,
      route: response.route,
      search_contract_version: response.search_contract_version,
      catalog_revision: response.catalog_revision,
      corpus_revision: response.corpus_revision,
      search_document_revision: response.search_document_revision,
    }),
    provider_attempts: Object.freeze({ ...response.provider_attempts }),
    response_ids: Object.freeze([...response.response_ids]),
    rendered_ids: Object.freeze([...state.rendered_ids]),
    rendered_family_count: state.rendered_families.length,
    card_count: state.rendered_ids.length,
    terminal_ui: true,
    real_scroll: Object.freeze({
      performed: scroll.performed === true,
      card_visible_after: scroll.card_visible_after === true,
      gesture_count: Number(scroll.gesture_count || 0),
      delta_y: Number(scroll.delta_y || 0),
    }),
    event_route: Object.freeze({
      same_origin: true, http_status: 200,
      destination_class: String(eventRoute.destination_class || 'event_detail').slice(0, 32),
      network_source: String(eventRoute.network_source || 'adapter').slice(0, 48),
    }),
    search_route: route,
    forbidden_activity: Object.freeze({
      llm_calls: 0, pagination_requests: 0, receipt_rpc_calls: 0, storage_image_requests: 0,
    }),
    diagnostics: Object.freeze({
      console_errors: 0, failed_requests: 0, error_responses: 0,
    }),
    meter: after.meter,
  });
}
