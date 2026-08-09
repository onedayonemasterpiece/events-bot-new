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
  'UNKNOWN_ANDROID_INFRA',
  'UNKNOWN_IOS_INFRA',
  'BLOCKED_RELEASE_NOT_ACTIVE',
  'COST_GUARD_FAILED',
  'EVIDENCE_REDACTION_FAILED',
];

export const PRODUCTION_HEALTH_RESULTS = Object.freeze(Object.fromEntries(
  RESULT_VALUES.map((value) => [value, value]),
));

export const PRODUCTION_HEALTH_RESULT_VALUES = Object.freeze([...RESULT_VALUES]);

export const PRODUCTION_HEALTH_PRODUCT_STATES = Object.freeze({
  HEALTHY: 'HEALTHY',
  BROKEN: 'BROKEN',
  UNCONFIRMED: 'UNCONFIRMED',
});

export const PRODUCTION_HEALTH_EXECUTION_STATUSES = Object.freeze({
  PASS: 'PASS',
  FAILED: 'FAILED',
  BLOCKED: 'BLOCKED',
});

export const PRODUCTION_HEALTH_PLATFORMS = Object.freeze(['browser', 'android', 'ios']);

export const PRODUCTION_HEALTH_FAILURE_CLASSES = Object.freeze(
  RESULT_VALUES.filter((value) => !['HEALTHY', 'DEGRADED'].includes(value)),
);

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

const validatePlatform = (platform) => {
  const normalized = String(platform || '').toLowerCase();
  if (!PRODUCTION_HEALTH_PLATFORMS.includes(normalized)) {
    throw new Error('search_health_platform_unknown');
  }
  return normalized;
};

/**
 * Maps execution evidence to independent product and execution dimensions.
 * Only a typed BROKEN_* failure proves a platform-specific product incident.
 */
export function classifyProductionHealthOutcome({ failureClass = null, platform = 'browser' } = {}) {
  const normalizedPlatform = validatePlatform(platform);
  if (failureClass !== null && !PRODUCTION_HEALTH_FAILURE_CLASSES.includes(failureClass)) {
    throw new Error('search_health_failure_class_unknown');
  }

  const productIncident = typeof failureClass === 'string' && failureClass.startsWith('BROKEN_');
  const releaseBlocked = failureClass === PRODUCTION_HEALTH_RESULTS.BLOCKED_RELEASE_NOT_ACTIVE;
  const productHealth = productIncident
    ? PRODUCTION_HEALTH_PRODUCT_STATES.BROKEN
    : failureClass === null
      ? PRODUCTION_HEALTH_PRODUCT_STATES.HEALTHY
      : PRODUCTION_HEALTH_PRODUCT_STATES.UNCONFIRMED;
  const executionStatus = failureClass === null
    ? PRODUCTION_HEALTH_EXECUTION_STATUSES.PASS
    : releaseBlocked
      ? PRODUCTION_HEALTH_EXECUTION_STATUSES.BLOCKED
      : PRODUCTION_HEALTH_EXECUTION_STATUSES.FAILED;

  return Object.freeze({
    product_health: productHealth,
    execution_status: executionStatus,
    failure_class: failureClass,
    product_incident: productIncident,
    incident_scope: productIncident
      ? `search-product:${normalizedPlatform}:${failureClass}`
      : null,
  });
}

/**
 * Backward-compatible adapter for Stage-1 callers that still use the old
 * single-result vocabulary. DEGRADED remains legacy-only and cannot create a
 * scheduled-health product incident.
 */
export function classifyProductionHealthResult(value, { platform = 'browser' } = {}) {
  if (!isProductionHealthResult(value)) throw new Error('search_health_result_unknown');
  if (value === PRODUCTION_HEALTH_RESULTS.DEGRADED) {
    validatePlatform(platform);
    return Object.freeze({
      result: value,
      product_health: PRODUCTION_HEALTH_PRODUCT_STATES.UNCONFIRMED,
      execution_status: PRODUCTION_HEALTH_EXECUTION_STATUSES.PASS,
      failure_class: null,
      product_failure: false,
      product_incident: false,
      incident_scope: null,
    });
  }
  const outcome = classifyProductionHealthOutcome({
    failureClass: value === PRODUCTION_HEALTH_RESULTS.HEALTHY ? null : value,
    platform,
  });
  return Object.freeze({
    result: value,
    ...outcome,
    product_failure: outcome.product_incident,
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
  const platform = validatePlatform(observation.platform || 'browser');
  const classified = (result) => classifyProductionHealthResult(result, { platform });
  if (observation.evidence_redaction_passed !== true) {
    return classified(PRODUCTION_HEALTH_RESULTS.EVIDENCE_REDACTION_FAILED);
  }
  if (observation.cost_guard_passed !== true) {
    return classified(PRODUCTION_HEALTH_RESULTS.COST_GUARD_FAILED);
  }
  if (observation.release_active !== true) {
    return classified(PRODUCTION_HEALTH_RESULTS.BLOCKED_RELEASE_NOT_ACTIVE);
  }
  if (platform === 'browser' && observation.runner_browser_known !== true) {
    return classified(PRODUCTION_HEALTH_RESULTS.UNKNOWN_RUNNER_BROWSER);
  }
  if (platform === 'android' && observation.android_infra_known !== true) {
    return classified(PRODUCTION_HEALTH_RESULTS.UNKNOWN_ANDROID_INFRA);
  }
  if (platform === 'ios' && observation.ios_infra_known !== true) {
    return classified(PRODUCTION_HEALTH_RESULTS.UNKNOWN_IOS_INFRA);
  }
  if (observation.auth_broker_known !== true) {
    return classified(PRODUCTION_HEALTH_RESULTS.UNKNOWN_AUTH_BROKER);
  }
  if (observation.search_surface_ready !== true) {
    return classified(PRODUCTION_HEALTH_RESULTS.BROKEN_SEARCH_SURFACE);
  }
  if (observation.auth_integration_ready !== true) {
    return classified(PRODUCTION_HEALTH_RESULTS.BROKEN_AUTH_INTEGRATION);
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
    return classified(PRODUCTION_HEALTH_RESULTS.BROKEN_SEARCH_REQUEST);
  }

  const cardCount = finiteCount(observation.card_count);
  if (cardCount === 0) {
    return classified(PRODUCTION_HEALTH_RESULTS.BROKEN_NO_RESULTS);
  }
  if (cardCount === null || cardCount < 1 || cardCount > 5 || observation.cards_rendered !== true) {
    return classified(PRODUCTION_HEALTH_RESULTS.BROKEN_RESULT_RENDER);
  }
  if (
    finiteCount(observation.real_scroll_count) !== 1
    || observation.real_scroll_performed !== true
    || finiteCount(observation.card_http_200_count) !== 1
  ) {
    return classified(PRODUCTION_HEALTH_RESULTS.BROKEN_RESULT_ROUTE);
  }
  if (observation.degraded === true) {
    return classified(PRODUCTION_HEALTH_RESULTS.DEGRADED);
  }
  return classified(PRODUCTION_HEALTH_RESULTS.HEALTHY);
}
