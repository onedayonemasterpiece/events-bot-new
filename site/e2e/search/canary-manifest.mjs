const evidenceRequirements = Object.freeze([
  'request_response_route_counters',
  'terminal_ui_state',
  'response_rendered_id_correspondence',
  'card_visibility_and_real_scroll',
  'pagination_without_duplicate_ids_or_families',
  'cache_receipt',
  'validation_zero_post',
  'redaction_pass',
]);

const variant = (value) => Object.freeze({
  ...value,
  request_policy: Object.freeze({ ...value.request_policy }),
  expected_cache_state: Object.freeze([...value.expected_cache_state]),
  allowed_provider_attempts: Object.freeze({ ...value.allowed_provider_attempts }),
  platforms: Object.freeze([...value.platforms]),
  evidence_requirements: evidenceRequirements,
});

export const SEARCH_CANARY_VARIANTS = Object.freeze({
  cached_vector: variant({
    request_policy: { execution_mode: 'cached_vector', cache: 'prefer', llm: 'forbid', selected_once: true },
    expected_cache_state: ['hit'],
    allowed_provider_attempts: { embedding: 0, vector: 0, llm: 0 },
    auth_persona: 'search_cached_platform_scoped',
    platforms: ['browser', 'android', 'ios'],
    blocking_policy: 'blocking_after_stability_threshold',
  }),
  cold_vector: variant({
    request_policy: { execution_mode: 'cold_vector', cache: 'bypass_read', llm: 'forbid', selected_once: true },
    expected_cache_state: ['miss', 'stored', 'bypass', 'skipped'],
    allowed_provider_attempts: { embedding: 1, vector: 1, llm: 0 },
    auth_persona: 'search_cold_browser',
    platforms: ['browser'],
    blocking_policy: 'post_deploy_blocking',
  }),
  cold_vector_llm: variant({
    request_policy: { execution_mode: 'cold_vector_llm', cache: 'bypass_read', llm: 'bounded', selected_once: true },
    expected_cache_state: ['miss', 'stored', 'bypass', 'skipped'],
    allowed_provider_attempts: { embedding: 1, vector: 1, llm: 1 },
    auth_persona: 'search_cold_browser',
    platforms: ['browser'],
    blocking_policy: 'budget_enforced',
  }),
  degraded_vector_fallback: variant({
    request_policy: { execution_mode: 'degraded_vector_fallback', cache: 'bypass_read', llm: 'deterministic_failure', selected_once: true },
    expected_cache_state: ['miss', 'stored', 'bypass', 'skipped', 'degraded'],
    allowed_provider_attempts: { embedding: 1, vector: 1, llm: 0 },
    auth_persona: 'search_degraded_browser',
    platforms: ['browser'],
    blocking_policy: 'post_deploy_blocking',
  }),
});

export function resolveCanaryVariant(name, platform = 'browser') {
  const key = String(name || '').trim();
  const selected = SEARCH_CANARY_VARIANTS[key];
  if (!selected) throw new Error(`search_variant_unknown:${key || 'empty'}`);
  if (!['browser', 'android', 'ios'].includes(platform)) throw new Error(`search_platform_unknown:${platform}`);
  // Mobile is a separate L2 execution stage. It reuses the semantic variant
  // contract while the caller explicitly selects the device platform.
  if (platform !== 'browser' && key !== 'cached_vector') {
    throw new Error(`search_variant_platform_not_allowed:${key}:${platform}`);
  }
  return selected;
}

export function publicCanaryManifest() {
  return Object.fromEntries(Object.entries(SEARCH_CANARY_VARIANTS).map(([name, config]) => [name, {
    ...config,
    request_policy: { ...config.request_policy },
    expected_cache_state: [...config.expected_cache_state],
    allowed_provider_attempts: { ...config.allowed_provider_attempts },
    platforms: [...config.platforms],
    evidence_requirements: [...config.evidence_requirements],
  }]));
}
