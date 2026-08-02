import { mkdir, writeFile } from 'node:fs/promises';
import { join } from 'node:path';

import { auditEvidenceDirectory, redactText } from './redaction.mjs';

export async function createEvidenceWriter({ root, secrets = [] }) {
  await mkdir(join(root, 'screenshots'), { recursive: true });
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
    schema_version: 1,
    status: input.status,
    failure_domain: input.failure_domain || null,
    coverage: input.coverage,
    target_origin: input.target_origin,
    target_path: input.target_path,
    expected_repo_sha: input.expected_repo_sha || null,
    observed_repo_sha: input.observed_repo_sha || null,
    browser: input.browser,
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
