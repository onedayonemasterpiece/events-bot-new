import { mkdir, rm, writeFile } from 'node:fs/promises';
import { resolve } from 'node:path';

import { junitXml, publicResult, qaSummary } from '../e2e/focus-email/helpers/evidence.mjs';

const root = resolve(process.env.E2E_EVIDENCE_DIR || 'artifacts/external-focus-email-otp-static-blocked');
const platform = String(process.env.E2E_PLATFORM || 'unknown');
const scenario = String(process.env.E2E_SCENARIO_ID || 'focus.otp.browser_tab');
const runId = String(process.env.E2E_RUN_ID || 'unknown').replace(/[^a-zA-Z0-9_.-]/gu, '-');
const artifactName = `static-site-qa-${scenario.replaceAll('.', '-')}-${platform}-${runId}`;
const provenance = {
  harness_repo_sha: String(process.env.E2E_HARNESS_REPO_SHA || process.env.GITHUB_SHA || 'unreported'),
  tested_repo_sha: String(process.env.E2E_EXPECTED_REPO_SHA || '') || null,
  observed_preview_sha: null,
  workflow_name: String(process.env.GITHUB_WORKFLOW || 'unreported'),
  workflow_ref: String(process.env.GITHUB_WORKFLOW_REF || 'unreported'),
  workflow_source_ref: String(process.env.GITHUB_REF || 'unreported'),
  workflow_run_id: String(process.env.GITHUB_RUN_ID || 'local'),
  workflow_run_attempt: String(process.env.GITHUB_RUN_ATTEMPT || '1'),
  runner_name: String(process.env.RUNNER_NAME || 'unreported'),
  runner_image_os: String(process.env.ImageOS || 'unreported'),
  runner_image_version: String(process.env.ImageVersion || 'unreported'),
  artifact_name: artifactName,
};
const result = publicResult({ scenario_id: scenario, platform, status: 'BLOCKED', failure_domain: 'BLOCKED_INFRASTRUCTURE',
  coverage: 'harness_safe_receipt_unavailable', target_origin: null, target_path: null,
  expected_repo_sha: provenance.tested_repo_sha, observed_repo_sha: null,
  otp_issue_request_count: null, otp_verify_request_count: null, participant_registration_request_count: null,
  participant_registration_status: null, redaction_audit_passed: true,
  first_failed_step: 'harness_safe_receipt_unavailable',
  failures: ['harness_did_not_produce_a_redaction_gated_receipt;side_effect_counts_unknown;automatic_rerun_forbidden'] });

await rm(root, { recursive: true, force: true });
await mkdir(root, { recursive: true });
const json = (name, value) => writeFile(resolve(root, name), `${JSON.stringify(value, null, 2)}\n`, 'utf8');
await json('manifest.json', { schema_version: 3, artifact: artifactName, scenario_id: scenario, provenance,
  files_are_sanitized: true, static_safe_fallback: true, forbidden_artifacts: ['har', 'trace', 'video', 'native-hierarchy', 'raw-appium-log'] });
await json('result.json', result);
await json('qa-summary.json', qaSummary(result, provenance));
await json('steps.json', [{ sequence: 1, name: 'harness_safe_receipt_unavailable', status: 'blocked' }]);
await writeFile(resolve(root, 'scenarios.jsonl'), `${JSON.stringify({ scenario_id: scenario, platform, status: 'BLOCKED', failure_domain: 'BLOCKED_INFRASTRUCTURE' })}\n`);
await writeFile(resolve(root, 'network.sanitized.jsonl'), '');
await writeFile(resolve(root, 'console.sanitized.jsonl'), '');
await json('mail-delivery.sanitized.json', { matching_message_count: null, status: 'unknown' });
await json('device.json', { platform, status: 'unavailable' });
await writeFile(resolve(root, 'junit.xml'), junitXml(result, 0), 'utf8');
await json('redaction-audit.json', { passed: true, mode: 'static_safe_fallback_after_incomplete_harness_output_was_deleted' });
await writeFile(resolve(root, 'README.md'), '# Static safe BLOCKED receipt\n\nThe harness produced no redaction-gated result. Incomplete output was deleted; side-effect counts are unknown and automatic rerun is forbidden.\n', 'utf8');
await writeFile(resolve(root, '.redaction-ok'), 'safe-static-fallback\n', 'utf8');
