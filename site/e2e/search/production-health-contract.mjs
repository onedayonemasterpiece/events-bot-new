const RESULT_VALUES = [
  'HEALTHY',
  'DEGRADED',
  'BROKEN_SEARCH_SURFACE',
  'BROKEN_AUTH_INTEGRATION',
  'BROKEN_SEARCH_REQUEST',
  'BROKEN_NO_RESULTS',
  'BROKEN_RESULT_RENDER',
  'BROKEN_RESULT_ROUTE',
  'UNKNOWN_AUTH_BROKER',
  'UNKNOWN_RUNNER_BROWSER',
  'BLOCKED_RELEASE_NOT_ACTIVE',
  'COST_GUARD_FAILED',
  'EVIDENCE_REDACTION_FAILED',
];

export const PRODUCTION_HEALTH_RESULTS = Object.freeze(Object.fromEntries(
  RESULT_VALUES.map((value) => [value, value]),
));

export const PRODUCTION_HEALTH_RESULT_VALUES = Object.freeze([...RESULT_VALUES]);

export const ACCEPTED_HEALTH_CACHE_STATES = Object.freeze(['hit', 'miss', 'stored', 'bypass']);

const frozenHealthPlan = {
  schema_version: 'search_production_health_plan_v1',
  plane: 'production_health',
  query: {
    count: 1,
    dispatch: 'ui',
    execution: 'vector_only',
    search_post_count: 1,
    limit: 5,
  },
  results: {
    card_count: { minimum: 1, maximum: 5 },
    real_scroll_count: 1,
    card_http_200_count: 1,
  },
  accepted_variability: {
    cache_states: ACCEPTED_HEALTH_CACHE_STATES,
    content_drift: true,
    index_drift: true,
  },
  forbidden_activity: {
    llm_calls: 0,
    pagination_requests: 0,
    receipt_rpc_calls: 0,
    storage_image_requests: 0,
  },
};

const deepFreeze = (value) => {
  if (!value || typeof value !== 'object' || Object.isFrozen(value)) return value;
  for (const child of Object.values(value)) deepFreeze(child);
  return Object.freeze(value);
};

/**
 * This is a future live contract. Stage 1 consumers may plan and inspect it,
 * but must not execute it.
 */
export const FUTURE_PRODUCTION_HEALTH_PLAN = deepFreeze(frozenHealthPlan);

export function isProductionHealthResult(value) {
  return RESULT_VALUES.includes(value);
}

export function isProductFailureResult(value) {
  return isProductionHealthResult(value) && value.startsWith('BROKEN_');
}

export function classifyProductionHealthResult(value) {
  if (!isProductionHealthResult(value)) throw new Error('search_health_result_unknown');
  const productFailure = isProductFailureResult(value);
  return Object.freeze({
    result: value,
    product_failure: productFailure,
    product_incident: productFailure,
  });
}

/**
 * A retry is possible only before Search dispatch and only when the caller has
 * positive, fail-closed evidence that no side effect occurred. Missing or
 * ambiguous evidence is a denial.
 */
export function decideProductionHealthRetry({ search_dispatched = false, zero_side_effects_proven = false } = {}) {
  if (search_dispatched === true) {
    return Object.freeze({ retry_allowed: false, reason: 'search_already_dispatched' });
  }
  if (zero_side_effects_proven !== true) {
    return Object.freeze({ retry_allowed: false, reason: 'zero_side_effects_not_proven' });
  }
  return Object.freeze({ retry_allowed: true, reason: 'pre_dispatch_zero_side_effects_proven' });
}

const finiteCount = (value) => (
  Number.isInteger(Number(value)) && Number(value) >= 0 ? Number(value) : null
);

/**
 * Pure Stage-1 acceptance evaluator for a future observation. It deliberately
 * ignores content/catalog/index generation drift and accepts all four bounded
 * cache outcomes in the contract.
 */
export function evaluateProductionHealthObservation(observation = {}) {
  if (observation.evidence_redaction_passed !== true) {
    return classifyProductionHealthResult(PRODUCTION_HEALTH_RESULTS.EVIDENCE_REDACTION_FAILED);
  }
  if (observation.cost_guard_passed !== true) {
    return classifyProductionHealthResult(PRODUCTION_HEALTH_RESULTS.COST_GUARD_FAILED);
  }
  if (observation.release_active !== true) {
    return classifyProductionHealthResult(PRODUCTION_HEALTH_RESULTS.BLOCKED_RELEASE_NOT_ACTIVE);
  }
  if (observation.auth_broker_known !== true) {
    return classifyProductionHealthResult(PRODUCTION_HEALTH_RESULTS.UNKNOWN_AUTH_BROKER);
  }
  if (observation.runner_browser_known !== true) {
    return classifyProductionHealthResult(PRODUCTION_HEALTH_RESULTS.UNKNOWN_RUNNER_BROWSER);
  }
  if (observation.search_surface_ready !== true) {
    return classifyProductionHealthResult(PRODUCTION_HEALTH_RESULTS.BROKEN_SEARCH_SURFACE);
  }
  if (observation.auth_integration_ready !== true) {
    return classifyProductionHealthResult(PRODUCTION_HEALTH_RESULTS.BROKEN_AUTH_INTEGRATION);
  }

  const queryCount = finiteCount(observation.query_count);
  const postCount = finiteCount(observation.search_post_count);
  const limit = finiteCount(observation.limit);
  const llmCalls = finiteCount(observation.llm_calls);
  const paginationRequests = finiteCount(observation.pagination_requests);
  const receiptRpcCalls = finiteCount(observation.receipt_rpc_calls);
  const storageImageRequests = finiteCount(observation.storage_image_requests);
  if (
    queryCount !== 1
    || observation.query_dispatch !== 'ui'
    || observation.query_execution !== 'vector_only'
    || postCount !== 1
    || limit === null || limit < 1 || limit > 5
    || llmCalls !== 0
    || paginationRequests !== 0
    || receiptRpcCalls !== 0
    || storageImageRequests !== 0
    || !ACCEPTED_HEALTH_CACHE_STATES.includes(String(observation.cache_state || '').toLowerCase())
  ) {
    return classifyProductionHealthResult(PRODUCTION_HEALTH_RESULTS.BROKEN_SEARCH_REQUEST);
  }

  const cardCount = finiteCount(observation.card_count);
  if (cardCount === 0) {
    return classifyProductionHealthResult(PRODUCTION_HEALTH_RESULTS.BROKEN_NO_RESULTS);
  }
  if (cardCount === null || cardCount < 1 || cardCount > 5 || observation.cards_rendered !== true) {
    return classifyProductionHealthResult(PRODUCTION_HEALTH_RESULTS.BROKEN_RESULT_RENDER);
  }
  if (
    finiteCount(observation.real_scroll_count) !== 1
    || observation.real_scroll_performed !== true
    || finiteCount(observation.card_http_200_count) !== 1
  ) {
    return classifyProductionHealthResult(PRODUCTION_HEALTH_RESULTS.BROKEN_RESULT_ROUTE);
  }
  if (observation.degraded === true) {
    return classifyProductionHealthResult(PRODUCTION_HEALTH_RESULTS.DEGRADED);
  }
  return classifyProductionHealthResult(PRODUCTION_HEALTH_RESULTS.HEALTHY);
}
