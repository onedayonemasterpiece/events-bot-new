import {
  PRODUCTION_HEALTH_EXECUTION_STATUSES,
  PRODUCTION_HEALTH_FAILURE_CLASSES,
  PRODUCTION_HEALTH_PLATFORMS,
  PRODUCTION_HEALTH_PRODUCT_STATES,
} from '../production-health-contract.mjs';

export const SEARCH_HEALTH_SUMMARY_SCHEMA = 'search_production_health_summary_v1';

export const SEARCH_HEALTH_SUMMARY_FIELDS = Object.freeze([
  'schema_version',
  'platform',
  'product_health',
  'execution_status',
  'failure_class',
  'target_fingerprint',
  'runtime_fingerprint',
  'run_id',
  'run_url',
]);

const SUMMARY_FIELD_SET = new Set(SEARCH_HEALTH_SUMMARY_FIELDS);
const PRODUCT_HEALTH_SET = new Set(Object.values(PRODUCTION_HEALTH_PRODUCT_STATES));
const EXECUTION_STATUS_SET = new Set(Object.values(PRODUCTION_HEALTH_EXECUTION_STATUSES));
const FAILURE_CLASS_SET = new Set(PRODUCTION_HEALTH_FAILURE_CLASSES);
const PLATFORM_SET = new Set(PRODUCTION_HEALTH_PLATFORMS);
const FINGERPRINT_PATTERN = /^[a-f0-9]{64}$/u;
const RUN_ID_PATTERN = /^[1-9][0-9]{0,19}$/u;

const fail = (reason) => {
  throw new Error(`search_health_summary_invalid:${reason}`);
};

const assertPlainRecord = (value, reason) => {
  if (!value || typeof value !== 'object' || Array.isArray(value)) fail(reason);
  if (Object.getPrototypeOf(value) !== Object.prototype) fail(reason);
};

const assertExactFields = (summary) => {
  const fields = Object.keys(summary);
  const unknown = fields.filter((field) => !SUMMARY_FIELD_SET.has(field));
  const missing = SEARCH_HEALTH_SUMMARY_FIELDS.filter((field) => !Object.hasOwn(summary, field));
  if (unknown.length > 0) fail(`field_not_allowlisted:${unknown.sort()[0]}`);
  if (missing.length > 0) fail(`field_missing:${missing[0]}`);
};

const validateRunUrl = (value, runId) => {
  let parsed;
  try {
    parsed = new URL(value);
  } catch {
    fail('run_url');
  }
  if (
    parsed.protocol !== 'https:'
    || parsed.hostname !== 'github.com'
    || parsed.username
    || parsed.password
    || parsed.search
    || parsed.hash
  ) fail('run_url');
  const segments = parsed.pathname.split('/').filter(Boolean);
  if (
    segments.length !== 5
    || segments[2] !== 'actions'
    || segments[3] !== 'runs'
    || segments[4] !== runId
    || !/^[A-Za-z0-9_.-]+$/u.test(segments[0])
    || !/^[A-Za-z0-9_.-]+$/u.test(segments[1])
  ) fail('run_url');
  return parsed.toString();
};

const assertConsistentOutcome = ({ product_health: productHealth, execution_status: executionStatus, failure_class: failureClass }) => {
  if (productHealth === 'HEALTHY') {
    if (executionStatus !== 'PASS' || failureClass !== null) fail('healthy_outcome_inconsistent');
    return;
  }
  if (productHealth === 'BROKEN') {
    if (executionStatus !== 'FAILED' || !failureClass?.startsWith('BROKEN_')) {
      fail('broken_outcome_inconsistent');
    }
    return;
  }
  if (executionStatus === 'BLOCKED') {
    if (failureClass !== 'BLOCKED_RELEASE_NOT_ACTIVE') fail('blocked_outcome_inconsistent');
    return;
  }
  if (executionStatus === 'FAILED') {
    if (
      !failureClass
      || failureClass.startsWith('BROKEN_')
      || failureClass === 'BLOCKED_RELEASE_NOT_ACTIVE'
    ) fail('unconfirmed_failed_outcome_inconsistent');
    return;
  }
  if (executionStatus !== 'PASS' || failureClass !== null) fail('unconfirmed_pass_outcome_inconsistent');
};

const assertPlatformFailureCompatibility = (platform, failureClass) => {
  if (failureClass === 'UNKNOWN_RUNNER_BROWSER' && platform !== 'browser') {
    fail('browser_failure_platform_mismatch');
  }
  if (failureClass === 'UNKNOWN_ANDROID_INFRA' && platform !== 'android') {
    fail('android_failure_platform_mismatch');
  }
  if (failureClass === 'UNKNOWN_IOS_INFRA' && platform !== 'ios') {
    fail('ios_failure_platform_mismatch');
  }
};

/**
 * Accepts only the intentionally public, sanitized interface emitted by a
 * platform health cell. Extra fields fail closed rather than leaking into an
 * issue body or report plan.
 */
export function normalizeSearchHealthSummary(summary) {
  assertPlainRecord(summary, 'record');
  assertExactFields(summary);
  if (summary.schema_version !== SEARCH_HEALTH_SUMMARY_SCHEMA) fail('schema_version');
  if (!PLATFORM_SET.has(summary.platform)) fail('platform');
  if (!PRODUCT_HEALTH_SET.has(summary.product_health)) fail('product_health');
  if (!EXECUTION_STATUS_SET.has(summary.execution_status)) fail('execution_status');
  if (summary.failure_class !== null && !FAILURE_CLASS_SET.has(summary.failure_class)) {
    fail('failure_class');
  }
  if (!FINGERPRINT_PATTERN.test(summary.target_fingerprint)) fail('target_fingerprint');
  if (!FINGERPRINT_PATTERN.test(summary.runtime_fingerprint)) fail('runtime_fingerprint');
  if (typeof summary.run_id !== 'string' || !RUN_ID_PATTERN.test(summary.run_id)) fail('run_id');
  const runUrl = validateRunUrl(summary.run_url, summary.run_id);
  assertConsistentOutcome(summary);
  assertPlatformFailureCompatibility(summary.platform, summary.failure_class);

  return Object.freeze({
    schema_version: SEARCH_HEALTH_SUMMARY_SCHEMA,
    platform: summary.platform,
    product_health: summary.product_health,
    execution_status: summary.execution_status,
    failure_class: summary.failure_class,
    target_fingerprint: summary.target_fingerprint,
    runtime_fingerprint: summary.runtime_fingerprint,
    run_id: summary.run_id,
    run_url: runUrl,
  });
}

export function normalizeSearchHealthHistory(history = []) {
  if (!Array.isArray(history)) fail('history');
  return Object.freeze(history.map(normalizeSearchHealthSummary));
}
