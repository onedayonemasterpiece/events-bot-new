import { FUTURE_PRODUCTION_HEALTH_PLAN } from './production-health-contract.mjs';

export const PRODUCTION_HEALTH_PLANES = Object.freeze({
  CONTRACT_CI: 'contract_ci',
  PRODUCTION_HEALTH: 'production_health',
  RELEASE_QUALIFICATION: 'release_qualification',
});

export const PRODUCTION_HEALTH_TRIGGERS = Object.freeze({
  PULL_REQUEST: 'pull_request',
  MANUAL: 'workflow_dispatch',
  TWICE_DAILY: 'twice_daily',
  SEARCH_RUNTIME_DEPLOY: 'search_runtime_deploy',
  SMART_UPDATE: 'smart_update',
  SNAPSHOT_GENERATION: 'snapshot_generation',
  DATA_GENERATION: 'data_generation',
  CORPUS_MOVEMENT: 'corpus_movement',
  INDEX_MOVEMENT: 'index_movement',
});

const PLANE_VALUES = new Set(Object.values(PRODUCTION_HEALTH_PLANES));
const TRIGGER_VALUES = new Set(Object.values(PRODUCTION_HEALTH_TRIGGERS));
const HEALTH_TRIGGERS = new Set([
  PRODUCTION_HEALTH_TRIGGERS.MANUAL,
  PRODUCTION_HEALTH_TRIGGERS.TWICE_DAILY,
  PRODUCTION_HEALTH_TRIGGERS.SEARCH_RUNTIME_DEPLOY,
]);
const GENERATION_TRIGGERS = new Set([
  PRODUCTION_HEALTH_TRIGGERS.SMART_UPDATE,
  PRODUCTION_HEALTH_TRIGGERS.SNAPSHOT_GENERATION,
  PRODUCTION_HEALTH_TRIGGERS.DATA_GENERATION,
  PRODUCTION_HEALTH_TRIGGERS.CORPUS_MOVEMENT,
  PRODUCTION_HEALTH_TRIGGERS.INDEX_MOVEMENT,
]);

const RELEVANT_PATHS = [
  /^site\/e2e\/search\//u,
  /^site\/tests\/search-(?:e2e|production-health)/u,
  /^site\/src\/components\/(?:AuthorizedEventSearch|MobileSearchBottomNav)\.astro$/u,
  /^site\/src\/lib\/(?:staticSiteAuth|resilientSupabaseTransport)/u,
  /^supabase\/functions\/event-search\//u,
  /^supabase\/migrations\/[^/]*event_search[^/]*\.sql$/u,
  /^\.github\/(?:workflows|scripts)\/[^/]*static-search[^/]*$/u,
  /^docs\/features\/unsigned-personalization\/authorized-event-search\.md$/u,
  /^docs\/operations\/static-site-autotest-strategy\.md$/u,
];

export function isProductionHealthContractPath(path) {
  const normalized = String(path || '').replaceAll('\\', '/').replace(/^\.\//u, '');
  return RELEVANT_PATHS.some((pattern) => pattern.test(normalized));
}

const zeroLiveCalls = () => ({
  target_resolver: 0,
  browser: 0,
  search_post: 0,
  supabase: 0,
});

/**
 * Stage 1 is intentionally a dry planner. `eligible` describes trigger policy,
 * never permission to execute production traffic in this implementation.
 */
export function planProductionHealthRun({ plane, trigger, changedPaths = [] } = {}) {
  if (!PLANE_VALUES.has(plane)) throw new Error(`search_health_plane_invalid:${String(plane || 'empty')}`);
  if (!TRIGGER_VALUES.has(trigger)) throw new Error(`search_health_trigger_invalid:${String(trigger || 'empty')}`);
  if (!Array.isArray(changedPaths) || changedPaths.some((path) => typeof path !== 'string')) {
    throw new Error('search_health_changed_paths_invalid');
  }

  const relevantPaths = [...new Set(changedPaths.filter(isProductionHealthContractPath))].sort();
  let eligible = false;
  let reason = 'trigger_not_allowed_for_plane';
  let selection = 'none';

  if (GENERATION_TRIGGERS.has(trigger)) {
    reason = 'data_or_index_movement_never_triggers_health';
  } else if (plane === PRODUCTION_HEALTH_PLANES.CONTRACT_CI) {
    eligible = trigger === PRODUCTION_HEALTH_TRIGGERS.PULL_REQUEST && relevantPaths.length > 0;
    reason = eligible ? 'relevant_contract_path' : 'no_relevant_contract_path';
    selection = eligible ? 'deterministic_pr_contract_ci' : 'none';
  } else if (plane === PRODUCTION_HEALTH_PLANES.PRODUCTION_HEALTH) {
    eligible = HEALTH_TRIGGERS.has(trigger);
    reason = eligible ? 'future_health_trigger_accepted' : reason;
    selection = eligible ? 'single_bounded_health_journey' : 'none';
  } else if (plane === PRODUCTION_HEALTH_PLANES.RELEASE_QUALIFICATION) {
    eligible = trigger === PRODUCTION_HEALTH_TRIGGERS.MANUAL;
    reason = eligible ? 'manual_selective_release_qualification' : reason;
    selection = eligible ? 'manual_selective' : 'none';
  }

  return Object.freeze({
    schema_version: 'search_production_health_stage1_plan_v1',
    plane,
    trigger,
    stage: 'stage_1_contract_only',
    dry_run: true,
    zero_live: true,
    eligible,
    reason,
    selection,
    relevant_paths: Object.freeze(relevantPaths),
    live_calls: Object.freeze(zeroLiveCalls()),
    future_health_contract: plane === PRODUCTION_HEALTH_PLANES.PRODUCTION_HEALTH
      ? FUTURE_PRODUCTION_HEALTH_PLAN
      : null,
  });
}
