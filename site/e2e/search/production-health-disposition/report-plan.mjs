import {
  normalizeSearchHealthHistory,
  normalizeSearchHealthSummary,
} from './summary.mjs';

export const SEARCH_HEALTH_REPORT_PLAN_SCHEMA = 'search_production_health_report_plan_v1';

export const SEARCH_HEALTH_REPORT_ACTIONS = Object.freeze({
  OPEN_OR_UPDATE: 'open_or_update',
  CLOSE_MATCHING: 'close_matching',
  NONE: 'none',
});

const UNKNOWN_FAILURE_CLASSES = new Set([
  'UNKNOWN_AUTH_BROKER',
  'UNKNOWN_RUNNER_BROWSER',
  'UNKNOWN_ANDROID_INFRA',
  'UNKNOWN_IOS_INFRA',
]);

const ISSUE_KIND = Object.freeze({
  PRODUCT: 'product',
  INFRASTRUCTURE: 'infrastructure',
  COST: 'cost',
  SECURITY_EVIDENCE: 'security_evidence',
});

const titlePlatform = Object.freeze({
  browser: 'Browser',
  android: 'Android',
  ios: 'iOS',
});

const issueTitle = (kind, summary) => {
  const platform = titlePlatform[summary.platform];
  if (kind === ISSUE_KIND.PRODUCT) return `[Search health][${platform}] ${summary.failure_class}`;
  if (kind === ISSUE_KIND.INFRASTRUCTURE) return `[Search health infra][${platform}] ${summary.failure_class}`;
  if (kind === ISSUE_KIND.COST) return `[Search health cost][${platform}] response budget failed`;
  return `[Search health evidence][${platform}] redaction failed`;
};

const labelsFor = (kind, platform) => Object.freeze([
  'search-production-health',
  `search-health:${kind}`,
  `search-platform:${platform}`,
]);

const issueBody = ({ kind, fingerprint, summary, identicalTerminalRuns }) => {
  const lines = [
    `<!-- search-health-fingerprint:${fingerprint} -->`,
    '## Search production health disposition',
    '',
    `- Incident kind: \`${kind}\``,
    `- Platform: \`${summary.platform}\``,
    `- Product health: \`${summary.product_health}\``,
    `- Execution status: \`${summary.execution_status}\``,
    `- Failure class: \`${summary.failure_class}\``,
    `- Target fingerprint: \`${summary.target_fingerprint}\``,
    `- Runtime fingerprint: \`${summary.runtime_fingerprint}\``,
    `- Workflow run: [${summary.run_id}](${summary.run_url})`,
  ];
  if (kind === ISSUE_KIND.INFRASTRUCTURE) {
    lines.push(`- Consecutive identical terminal runs: \`${identicalTerminalRuns}\``);
  }
  lines.push('', 'This body is generated from the fixed sanitized-summary allowlist.');
  if (kind === ISSUE_KIND.SECURITY_EVIDENCE) {
    lines.push('Evidence artifact upload is forbidden for this disposition.');
  }
  return `${lines.join('\n')}\n`;
};

const openOperation = ({ kind, fingerprint, summary, identicalTerminalRuns = 1 }) => Object.freeze({
  action: SEARCH_HEALTH_REPORT_ACTIONS.OPEN_OR_UPDATE,
  issue_kind: kind,
  platform: summary.platform,
  fingerprint,
  title: issueTitle(kind, summary),
  body: issueBody({ kind, fingerprint, summary, identicalTerminalRuns }),
  labels: labelsFor(kind, summary.platform),
  artifact_policy: kind === ISSUE_KIND.SECURITY_EVIDENCE ? 'forbidden' : 'sanitized_summary_only',
});

const closeProductOperation = (summary) => Object.freeze({
  action: SEARCH_HEALTH_REPORT_ACTIONS.CLOSE_MATCHING,
  issue_kind: ISSUE_KIND.PRODUCT,
  platform: summary.platform,
  fingerprint_prefix: `search-product:${summary.platform}:`,
  close_comment: [
    'Platform product health is fully proven by a terminal healthy run.',
    `Workflow run: [${summary.run_id}](${summary.run_url})`,
    `Target fingerprint: \`${summary.target_fingerprint}\``,
    `Runtime fingerprint: \`${summary.runtime_fingerprint}\``,
  ].join('\n'),
  artifact_policy: 'sanitized_summary_only',
});

const noOperation = (summary, reason, identicalTerminalRuns = 0) => Object.freeze({
  action: SEARCH_HEALTH_REPORT_ACTIONS.NONE,
  issue_kind: null,
  platform: summary.platform,
  fingerprint: null,
  reason,
  identical_terminal_runs: identicalTerminalRuns,
});

const identicalUnknownTerminalRunCount = (history, current) => {
  let count = 1;
  for (let index = history.length - 1; index >= 0; index -= 1) {
    const previous = history[index];
    // Each platform owns an independent incident/streak lane. A browser cell
    // between two mobile cells must never reset or suppress mobile evidence.
    if (previous.platform !== current.platform) continue;
    if (
      previous.failure_class !== current.failure_class
      || previous.execution_status !== 'FAILED'
      || previous.product_health !== 'UNCONFIRMED'
    ) break;
    count += 1;
  }
  return count;
};

/**
 * Builds an issue-mutation plan without reading or mutating GitHub. History is
 * chronological and excludes the current summary.
 */
export function buildSearchHealthReportPlan({ summary, history = [] } = {}) {
  const current = normalizeSearchHealthSummary(summary);
  const prior = normalizeSearchHealthHistory(history);
  let operation;

  if (current.product_health === 'HEALTHY') {
    operation = closeProductOperation(current);
  } else if (current.failure_class?.startsWith('BROKEN_')) {
    operation = openOperation({
      kind: ISSUE_KIND.PRODUCT,
      fingerprint: `search-product:${current.platform}:${current.failure_class}`,
      summary: current,
    });
  } else if (UNKNOWN_FAILURE_CLASSES.has(current.failure_class)) {
    const count = identicalUnknownTerminalRunCount(prior, current);
    operation = count >= 3
      ? openOperation({
        kind: ISSUE_KIND.INFRASTRUCTURE,
        fingerprint: `search-infra:${current.platform}:${current.failure_class}`,
        summary: current,
        identicalTerminalRuns: count,
      })
      : noOperation(current, 'infra_streak_below_threshold', count);
  } else if (current.failure_class === 'COST_GUARD_FAILED') {
    operation = openOperation({
      kind: ISSUE_KIND.COST,
      fingerprint: `search-cost:${current.platform}`,
      summary: current,
    });
  } else if (current.failure_class === 'EVIDENCE_REDACTION_FAILED') {
    operation = openOperation({
      kind: ISSUE_KIND.SECURITY_EVIDENCE,
      fingerprint: `search-evidence:${current.platform}`,
      summary: current,
    });
  } else if (current.execution_status === 'BLOCKED') {
    operation = noOperation(current, 'release_not_active');
  } else {
    operation = noOperation(current, 'no_incident_disposition');
  }

  return Object.freeze({
    schema_version: SEARCH_HEALTH_REPORT_PLAN_SCHEMA,
    summary: current,
    operation,
  });
}
