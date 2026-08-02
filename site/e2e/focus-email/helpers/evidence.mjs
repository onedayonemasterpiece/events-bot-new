import { mkdir, writeFile } from 'node:fs/promises';
import { join } from 'node:path';

import { auditEvidenceDirectory, redactText } from './redaction.mjs';

export async function createEvidenceWriter({ root, secrets = [] }) {
  await mkdir(join(root, 'screenshots'), { recursive: true });
  await mkdir(join(root, 'native-ui'), { recursive: true });
  const writeJson = (name, value) => writeFile(join(root, name), `${JSON.stringify(value, null, 2)}\n`, 'utf8');
  const writeJsonl = (name, values) => writeFile(
    join(root, name),
    `${values.map((value) => JSON.stringify(value)).join('\n')}\n`,
    'utf8',
  );
  return {
    root,
    writeJson,
    writeJsonl,
    async writeText(name, value) { await writeFile(join(root, name), redactText(value, secrets), 'utf8'); },
    async audit() {
      const result = await auditEvidenceDirectory(root, secrets);
      await writeJson('redaction-audit.json', result);
      return result;
    },
  };
}

export function publicResult(input) {
  return {
    schema_version: 2,
    scenario_id: 'focus.otp.browser_tab',
    platform: input.platform || 'browser',
    status: input.status,
    failure_domain: input.failure_domain || null,
    coverage: input.coverage,
    target_origin: input.target_origin,
    target_path: input.target_path,
    expected_repo_sha: input.expected_repo_sha || null,
    observed_repo_sha: input.observed_repo_sha || null,
    browser: input.browser || null,
    device: input.device || null,
    keyboard_acceptance: input.keyboard_acceptance || null,
    otp_issue_request_count: input.otp_issue_request_count,
    otp_verify_request_count: input.otp_verify_request_count,
    participant_registration_request_count: input.participant_registration_request_count,
    participant_registration_status: input.participant_registration_status ?? null,
    mail: input.mail || null,
    final_ui_state: input.final_ui_state || null,
    reload_state: input.reload_state || null,
    console_error_count: input.console_error_count || 0,
    essential_failed_request_count: input.essential_failed_request_count || 0,
    redaction_audit_passed: input.redaction_audit_passed === true,
    failures: input.failures || [],
  };
}

export function qaSummary(result) {
  return {
    schema_version: 1,
    scenario_id: result.scenario_id,
    platform: result.platform,
    status: result.status,
    failure_domain: result.failure_domain,
    first_failed_step: result.failures?.[0] || null,
    expected_repo_sha: result.expected_repo_sha,
    observed_repo_sha: result.observed_repo_sha,
    target_origin: result.target_origin,
    target_path: result.target_path,
    counts: {
      issue: result.otp_issue_request_count,
      verify: result.otp_verify_request_count,
      registration: result.participant_registration_request_count,
    },
    keyboard_acceptance: result.keyboard_acceptance,
    device: result.device,
    redaction_status: result.redaction_audit_passed ? 'passed' : 'pending_or_failed',
    evidence: {
      result: 'result.json', steps: 'steps.json', scenarios: 'scenarios.jsonl', junit: 'junit.xml',
      device: 'device.json', network: 'network.sanitized.jsonl', console: 'console.sanitized.jsonl',
      mail: 'mail-delivery.sanitized.json', screenshots: 'screenshots/', native_ui: 'native-ui/',
      redaction: 'redaction-audit.json',
    },
  };
}

export function junitXml(result, elapsedSeconds = 0) {
  const escape = (value) => String(value ?? '').replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('"', '&quot;');
  const failed = result.status !== 'PASS';
  const body = failed
    ? `<failure type="${escape(result.failure_domain || result.status)}" message="${escape(result.failures?.[0] || result.status)}"/>`
    : '';
  return `<?xml version="1.0" encoding="UTF-8"?>\n<testsuite name="static-site-qa" tests="1" failures="${failed ? 1 : 0}" time="${Number(elapsedSeconds).toFixed(3)}"><testcase classname="${escape(result.platform)}" name="focus.otp.browser_tab" time="${Number(elapsedSeconds).toFixed(3)}">${body}</testcase></testsuite>\n`;
}
