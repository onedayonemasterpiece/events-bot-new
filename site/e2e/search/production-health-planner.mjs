import { FUTURE_PRODUCTION_HEALTH_PLAN } from './production-health-contract.mjs';

export const PRODUCTION_HEALTH_PLANES = Object.freeze({
  CONTRACT_CI: 'contract_ci',
  PRODUCTION_HEALTH: 'production_health',
  RELEASE_QUALIFICATION: 'release_qualification',
});

export const PRODUCTION_HEALTH_TRIGGERS = Object.freeze({
  PULL_REQUEST: 'pull_request',
  MANUAL: 'workflow_dispatch',
  SCHEDULE_MORNING: 'schedule_morning',
  SCHEDULE_EVENING: 'schedule_evening',
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
  PRODUCTION_HEALTH_TRIGGERS.SCHEDULE_MORNING,
  PRODUCTION_HEALTH_TRIGGERS.SCHEDULE_EVENING,
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
  /^site\/e2e\/(?:auth-session-fixture|mobile-web)\//u,
  /^site\/tests\/search-(?:e2e|production-health)/u,
  /^site\/src\/components\/(?:AuthorizedEventSearch|MobileSearchBottomNav)\.astro$/u,
  /^site\/src\/lib\/(?:staticSiteAuth|resilientSupabaseTransport)/u,
  /^supabase\/functions\/event-search\//u,
  /^supabase\/migrations\/[^/]*event_search[^/]*\.sql$/u,
  /^\.github\/workflows\/(?:static-site-search-canary|search-production-health|search-release-qualification|ci)\.ya?ml$/u,
  /^\.github\/scripts\/(?:[^/]*static-search[^/]*|resolve-static-search-target)\.[^/]+$/u,
  /^scripts\/request_static_site_build\.py$/u,
  /^site\/package\.json$/u,
  /^docs\/features\/unsigned-personalization\/authorized-event-search\.md$/u,
  /^docs\/features\/static-site-pages\/smart-vector-search\//u,
  /^docs\/testing\/static-site-autotest-scenarios\.v1\.yml$/u,
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

export const PRODUCTION_HEALTH_MANUAL_PROFILES = Object.freeze({
  BROWSER: 'browser',
  BROWSER_ANDROID: 'browser_android',
  BROWSER_IOS: 'browser_ios',
  ALL: 'all',
});

export const SEARCH_VALIDATION_PROFILES = Object.freeze({
  NONE: 'none',
  STANDARD: 'standard',
  FULL: 'full',
});

const PLATFORM_SELECTIONS = Object.freeze({
  browser: Object.freeze(['browser']),
  browser_android: Object.freeze(['browser', 'android']),
  browser_ios: Object.freeze(['browser', 'ios']),
  all: Object.freeze(['browser', 'android', 'ios']),
});

const noPlatforms = () => Object.freeze([]);

/**
 * Stage 1 is intentionally a dry planner. `eligible` describes trigger policy,
 * never permission to execute production traffic in this implementation.
 */
export function planProductionHealthRun({
  plane,
  trigger,
  profile,
  validationProfile,
  changedPaths = [],
} = {}) {
  if (!PLANE_VALUES.has(plane)) throw new Error(`search_health_plane_invalid:${String(plane || 'empty')}`);
  if (!TRIGGER_VALUES.has(trigger)) throw new Error(`search_health_trigger_invalid:${String(trigger || 'empty')}`);
  if (!Array.isArray(changedPaths) || changedPaths.some((path) => typeof path !== 'string')) {
    throw new Error('search_health_changed_paths_invalid');
  }

  const relevantPaths = [...new Set(changedPaths.filter(isProductionHealthContractPath))].sort();
  let eligible = false;
  let reason = 'trigger_not_allowed_for_plane';
  let selection = 'none';
  let selectedPlatforms = noPlatforms();
  let releaseQualificationRequested = false;

  if (GENERATION_TRIGGERS.has(trigger)) {
    reason = 'data_or_index_movement_never_triggers_health';
  } else if (plane === PRODUCTION_HEALTH_PLANES.CONTRACT_CI) {
    eligible = trigger === PRODUCTION_HEALTH_TRIGGERS.PULL_REQUEST && relevantPaths.length > 0;
    reason = eligible ? 'relevant_contract_path' : 'no_relevant_contract_path';
    selection = eligible ? 'deterministic_pr_contract_ci' : 'none';
  } else if (plane === PRODUCTION_HEALTH_PLANES.PRODUCTION_HEALTH) {
    if (trigger === PRODUCTION_HEALTH_TRIGGERS.TWICE_DAILY) {
      reason = 'ambiguous_schedule_forbidden';
    } else if (trigger === PRODUCTION_HEALTH_TRIGGERS.SCHEDULE_MORNING) {
      eligible = true;
      reason = 'deterministic_morning_profile';
      selection = 'scheduled_morning';
      selectedPlatforms = PLATFORM_SELECTIONS.browser_android;
    } else if (trigger === PRODUCTION_HEALTH_TRIGGERS.SCHEDULE_EVENING) {
      eligible = true;
      reason = 'deterministic_evening_profile';
      selection = 'scheduled_evening';
      selectedPlatforms = PLATFORM_SELECTIONS.browser_ios;
    } else if (trigger === PRODUCTION_HEALTH_TRIGGERS.MANUAL) {
      const manualProfile = profile || PRODUCTION_HEALTH_MANUAL_PROFILES.BROWSER;
      if (Object.hasOwn(PLATFORM_SELECTIONS, manualProfile)) {
        eligible = true;
        reason = 'manual_profile_accepted';
        selection = `manual_${manualProfile}`;
        selectedPlatforms = PLATFORM_SELECTIONS[manualProfile];
      } else {
        reason = 'manual_profile_invalid';
      }
    } else if (trigger === PRODUCTION_HEALTH_TRIGGERS.SEARCH_RUNTIME_DEPLOY) {
      if (
        validationProfile === SEARCH_VALIDATION_PROFILES.STANDARD
        || validationProfile === SEARCH_VALIDATION_PROFILES.FULL
      ) {
        eligible = true;
        reason = 'explicit_release_validation_marker';
        selection = `release_${validationProfile}`;
        selectedPlatforms = PLATFORM_SELECTIONS.all;
        releaseQualificationRequested = validationProfile === SEARCH_VALIDATION_PROFILES.FULL;
      } else {
        reason = validationProfile === SEARCH_VALIDATION_PROFILES.NONE || validationProfile == null
          ? 'release_validation_not_requested'
          : 'release_validation_profile_invalid';
      }
    } else if (HEALTH_TRIGGERS.has(trigger)) {
      // The branches above exhaust all health triggers. Keep this fail-closed
      // guard so adding a trigger requires an explicit deterministic mapping.
      reason = 'health_trigger_mapping_missing';
    }
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
    selected_platforms: selectedPlatforms,
    release_qualification_requested: releaseQualificationRequested,
    relevant_paths: Object.freeze(relevantPaths),
    live_calls: Object.freeze(zeroLiveCalls()),
    future_health_contract: plane === PRODUCTION_HEALTH_PLANES.PRODUCTION_HEALTH
      ? FUTURE_PRODUCTION_HEALTH_PLAN
      : null,
  });
}
