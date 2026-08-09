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

export const SEARCH_RUNTIME_DEPLOY_PAYLOAD_KEYS = Object.freeze([
  'changed_surfaces',
  'deployment_run_id',
  'search_backend_revision',
  'site_runtime_sha',
  'validation_profile',
]);

const PLANE_VALUES = new Set(Object.values(PRODUCTION_HEALTH_PLANES));
const TRIGGER_VALUES = new Set(Object.values(PRODUCTION_HEALTH_TRIGGERS));
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
  /^scripts\/(?:request_static_site_build\.py|search-runtime-deploy-dispatch\.mjs|deploy_fly_main\.sh)$/u,
  /^site\/package\.json$/u,
  /^docs\/features\/unsigned-personalization\/authorized-event-search\.md$/u,
  /^docs\/features\/static-site-pages\/smart-vector-search\//u,
  /^docs\/testing\/static-site-autotest-scenarios\.v1\.yml$/u,
  /^docs\/operations\/static-site-autotest-strategy\.md$/u,
];

const PLATFORM_SELECTIONS = Object.freeze({
  browser: Object.freeze(['browser']),
  browser_android: Object.freeze(['browser', 'android']),
  browser_ios: Object.freeze(['browser', 'ios']),
  all: Object.freeze(['browser', 'android', 'ios']),
});

const SAFE_REVISION = /^[A-Za-z0-9][A-Za-z0-9._:-]{6,127}$/u;
const SAFE_DEPLOYMENT_RUN = /^[A-Za-z0-9][A-Za-z0-9._:-]{2,127}$/u;
const SAFE_SURFACE = /^[a-z][a-z0-9_-]{0,47}$/u;
const SEVERITY_WORDS = new Set(['critical', 'high', 'medium', 'low', 'severity']);

export function isProductionHealthContractPath(path) {
  const normalized = String(path || '').replaceAll('\\', '/').replace(/^\.\//u, '');
  return RELEVANT_PATHS.some((pattern) => pattern.test(normalized));
}

function validateSafeScalar(value, pattern, errorCode) {
  const normalized = String(value || '').trim();
  if (!pattern.test(normalized) || /https?:\/\/|@|[\r\n]/iu.test(normalized)) throw new Error(errorCode);
  return normalized;
}

export function validateSearchRuntimeDeployPayload(payload) {
  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) {
    throw new Error('search_runtime_deploy_payload_invalid');
  }
  const keys = Object.keys(payload).sort();
  if (JSON.stringify(keys) !== JSON.stringify(SEARCH_RUNTIME_DEPLOY_PAYLOAD_KEYS)) {
    throw new Error('search_runtime_deploy_payload_keys_invalid');
  }
  const siteRuntimeSha = String(payload.site_runtime_sha || '').toLowerCase();
  if (!/^[0-9a-f]{40}$/u.test(siteRuntimeSha)) throw new Error('search_runtime_site_sha_invalid');
  const searchBackendRevision = validateSafeScalar(
    payload.search_backend_revision, SAFE_REVISION, 'search_runtime_backend_revision_invalid',
  );
  const validationProfile = String(payload.validation_profile || '').trim().toLowerCase();
  if (![SEARCH_VALIDATION_PROFILES.STANDARD, SEARCH_VALIDATION_PROFILES.FULL].includes(validationProfile)) {
    throw new Error('search_runtime_validation_profile_invalid');
  }
  if (!Array.isArray(payload.changed_surfaces) || payload.changed_surfaces.length < 1
    || payload.changed_surfaces.length > 8) {
    throw new Error('search_runtime_changed_surfaces_invalid');
  }
  const changedSurfaces = [...new Set(payload.changed_surfaces.map((value) => String(value).trim().toLowerCase()))].sort();
  if (changedSurfaces.length !== payload.changed_surfaces.length
    || changedSurfaces.some((value) => !SAFE_SURFACE.test(value) || SEVERITY_WORDS.has(value))) {
    throw new Error('search_runtime_changed_surfaces_invalid');
  }
  const deploymentRunId = validateSafeScalar(
    payload.deployment_run_id, SAFE_DEPLOYMENT_RUN, 'search_runtime_deployment_run_id_invalid',
  );
  return Object.freeze({
    site_runtime_sha: siteRuntimeSha,
    search_backend_revision: searchBackendRevision,
    validation_profile: validationProfile,
    changed_surfaces: Object.freeze(changedSurfaces),
    deployment_run_id: deploymentRunId,
  });
}

const emptyMarker = () => Object.freeze({
  site_runtime_sha: null,
  search_backend_revision: null,
  validation_profile: null,
  changed_surfaces: Object.freeze([]),
  deployment_run_id: null,
});

const plannerSideEffects = () => Object.freeze({
  target_resolver: 0,
  browser: 0,
  search_post: 0,
  supabase: 0,
});

export function planProductionHealthRun({
  plane,
  trigger,
  profile,
  deploymentMarker,
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
  let selectedPlatforms = Object.freeze([]);
  let releaseQualificationRequested = false;
  let marker = emptyMarker();

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
      marker = validateSearchRuntimeDeployPayload(deploymentMarker);
      eligible = true;
      reason = 'explicit_release_validation_marker';
      selection = `release_${marker.validation_profile}`;
      selectedPlatforms = PLATFORM_SELECTIONS.all;
      releaseQualificationRequested = marker.validation_profile === SEARCH_VALIDATION_PROFILES.FULL;
    }
  } else if (plane === PRODUCTION_HEALTH_PLANES.RELEASE_QUALIFICATION) {
    if (trigger === PRODUCTION_HEALTH_TRIGGERS.MANUAL) {
      const manualProfile = profile || PRODUCTION_HEALTH_MANUAL_PROFILES.BROWSER;
      if (Object.hasOwn(PLATFORM_SELECTIONS, manualProfile)) {
        eligible = true;
        reason = 'manual_selective_release_qualification';
        selection = `manual_selective_${manualProfile}`;
        selectedPlatforms = PLATFORM_SELECTIONS[manualProfile];
      } else {
        reason = 'manual_profile_invalid';
      }
    }
  }

  const activeExecution = eligible && plane !== PRODUCTION_HEALTH_PLANES.CONTRACT_CI;
  return Object.freeze({
    schema_version: 'search_production_health_stage2_plan_v1',
    plane,
    trigger,
    stage: 'stage_2_active',
    dry_run: !activeExecution,
    zero_live: !activeExecution,
    eligible,
    reason,
    selection,
    selected_platforms: selectedPlatforms,
    release_qualification_requested: releaseQualificationRequested,
    deployment_marker: marker,
    relevant_paths: Object.freeze(relevantPaths),
    planner_side_effects: plannerSideEffects(),
    expected_live_calls: Object.freeze({
      platform_sessions: activeExecution ? selectedPlatforms.length : 0,
      search_post: activeExecution ? selectedPlatforms.length : 0,
    }),
    health_contract: plane === PRODUCTION_HEALTH_PLANES.PRODUCTION_HEALTH
      ? FUTURE_PRODUCTION_HEALTH_PLAN
      : null,
  });
}
