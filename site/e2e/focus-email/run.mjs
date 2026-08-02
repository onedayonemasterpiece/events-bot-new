import process from 'node:process';
import { writeFile } from 'node:fs/promises';
import { join, resolve } from 'node:path';

import { createAppiumUi } from './adapters/appium-ui.mjs';
import { createPlaywrightUi } from './adapters/playwright-ui.mjs';
import { createEvidenceWriter, junitXml, publicResult, qaSummary } from './helpers/evidence.mjs';
import { createMailbox } from './helpers/mailbox.mjs';
import { validatePlatform } from './helpers/platform.mjs';
import { redactText } from './helpers/redaction.mjs';
import { recipientForRun, sanitizedRunId, targetEvidence, validateFocusE2eTarget } from './helpers/target-url.mjs';
import { runFocusOtpBrowserTab } from './journey.mjs';

const startedAt = Date.now();
const required = (name) => { const value = String(process.env[name] || '').trim(); if (!value) throw new Error(`missing_configuration:${name}`); return value; };

function failureDomain(error) {
  const message = String(error?.message || error);
  if (/missing_configuration|configuration_invalid|configuration_missing|simulator_(?:runtime|appium|configuration)|target_url|websocket_connect/iu.test(message)) return 'BLOCKED_INFRASTRUCTURE';
  if (/release_evidence/iu.test(message)) return 'FAIL_RELEASE_EVIDENCE';
  if (/mail_/iu.test(message)) return 'FAIL_DELIVERY';
  if (/fail_mobile_keyboard/iu.test(message)) return 'FAIL_MOBILE_KEYBOARD';
  if (/fail_mobile_viewport/iu.test(message)) return 'FAIL_MOBILE_VIEWPORT';
  if (/browser_context/iu.test(message)) return 'FAIL_BROWSER_CONTEXT';
  return 'FAIL_PRODUCT';
}

function expectedRepoSha() {
  const value = String(process.env.E2E_EXPECTED_REPO_SHA || '').trim().toLowerCase();
  if (value && !/^[0-9a-f]{40}$/u.test(value)) throw new Error('release_evidence_expected_sha_invalid');
  return value || null;
}

const runId = sanitizedRunId(process.env.E2E_RUN_ID || `local-${Date.now()}`);
const platform = validatePlatform(process.env.E2E_PLATFORM || 'browser');
const timeoutMs = Math.min(180_000, Math.max(30_000, Number(process.env.E2E_MAIL_TIMEOUT_MS || 120_000)));
const evidenceRoot = resolve(process.env.E2E_EVIDENCE_DIR || `artifacts/external-focus-email-otp-evidence-${platform}-${runId}`);
const secrets = [process.env.E2E_IMAP_USERNAME, process.env.E2E_IMAP_PASSWORD, process.env.E2E_YANDEX_MAIL_WS_URL].filter(Boolean);
const evidence = await createEvidenceWriter({ root: evidenceRoot, secrets });
const steps = [];
const step = (name, status = 'passed') => steps.push({ sequence: steps.length + 1, name, status });
let target; let mailbox; let ui; let recipient = ''; let otp = ''; let coverage = 'configuration_unverified';
const emptyRecorder = { entries: [], count: () => 0, statuses: () => [] };
let recorder = emptyRecorder;
let result = publicResult({ platform, status: 'FAIL', coverage, target_origin: null, target_path: null,
  expected_repo_sha: null, observed_repo_sha: null, otp_issue_request_count: 0, otp_verify_request_count: 0,
  participant_registration_request_count: 0, failures: [] });

try {
  target = validateFocusE2eTarget(required('E2E_TARGET_URL'));
  ({ recipient, coverage } = recipientForRun(required('E2E_RECIPIENT_TEMPLATE'), runId));
  secrets.push(recipient);
  const expected = expectedRepoSha();
  result = publicResult({ ...result, coverage, ...targetEvidence(target), expected_repo_sha: expected });
  mailbox = createMailbox(process.env);
  const uiOptions = { platform, target, expectedRepoSha: expected, evidenceRoot, secrets,
    directHost: String(process.env.E2E_SUPABASE_HOST || '').trim(), relayHost: String(process.env.E2E_RELAY_HOST || '').trim() };
  ui = platform === 'browser' ? await createPlaywrightUi(uiOptions) : await createAppiumUi(uiOptions);
  recorder = ui.recorder;
  const journey = await runFocusOtpBrowserTab({ ui, mailbox, recipient, timeoutMs, step });
  otp = journey.mail.otp;
  secrets.push(otp);
  if (process.env.GITHUB_ACTIONS === 'true') process.stdout.write(`::add-mask::${otp}\n`);
  result = publicResult({ ...result, status: 'PASS', failure_domain: null, observed_repo_sha: journey.observedRepoSha,
    browser: platform === 'browser' ? ui.device : null, device: ui.device, keyboard_acceptance: platform === 'browser' ? null : journey.keyboardAcceptance,
    otp_issue_request_count: journey.counts.issue, otp_verify_request_count: journey.counts.verify,
    participant_registration_request_count: journey.counts.registration,
    participant_registration_status: journey.counts.registrationStatus,
    mail: { matching_message_count: journey.mail.matchingMessageCount, folder: process.env.E2E_MAIL_ADAPTER === 'yandex-websocket' ? 'mail-trigger' : 'inbox',
      delivery_latency_ms: journey.mail.deliveryLatencyMs, otp_length: otp.length, message_id_hash: journey.mail.messageIdHash },
    final_ui_state: 'membership_confirmed', reload_state: 'returning_member',
    console_error_count: ui.consoles.filter((item) => item.type === 'error').length,
    essential_failed_request_count: recorder.entries.filter((item) => item.failure_class && /auth\/v1|register_focus/u.test(item.path)).length,
    failures: [] });
} catch (error) {
  const domain = failureDomain(error);
  const message = redactText(String(error?.message || error), [...secrets, otp]);
  result = publicResult({ ...result, platform, status: domain === 'BLOCKED_INFRASTRUCTURE' ? 'BLOCKED' : 'FAIL', failure_domain: domain,
    observed_repo_sha: ui?.observedRepoSha || result.observed_repo_sha || null,
    browser: platform === 'browser' ? ui?.device || null : null, device: ui?.device || null,
    keyboard_acceptance: platform === 'browser' ? null : ui?.keyboard || null,
    otp_issue_request_count: recorder.count('POST', '/auth/v1/otp'), otp_verify_request_count: recorder.count('POST', '/auth/v1/verify'),
    participant_registration_request_count: recorder.count('POST', '/rpc/register_focus_group_participant_v1'),
    participant_registration_status: recorder.statuses('/rpc/register_focus_group_participant_v1').at(-1) ?? null,
    mail: mailbox?.safeDiagnostics?.({ recipient }) || result.mail || null,
    failures: [message] });
  step('journey_failed', 'failed');
} finally {
  await mailbox?.close().catch(() => undefined);
  await ui?.close().catch(() => undefined);
}

await evidence.writeJson('manifest.json', { schema_version: 2, artifact: `static-site-qa-focus-otp-${platform}-${runId}`, files_are_sanitized: true,
  forbidden_artifacts: ['har', 'trace', 'video', 'native-hierarchy'] });
await evidence.writeJson('result.json', result);
await evidence.writeJson('qa-summary.json', qaSummary(result));
await evidence.writeJson('steps.json', steps);
await evidence.writeJsonl('scenarios.jsonl', [{ scenario_id: 'focus.otp.browser_tab', platform, status: result.status, failure_domain: result.failure_domain }]);
await evidence.writeText('junit.xml', junitXml(result, (Date.now() - startedAt) / 1_000));
await evidence.writeJson('device.json', result.device || { platform, status: 'unavailable' });
await evidence.writeJsonl('network.sanitized.jsonl', recorder.entries);
await evidence.writeJsonl('console.sanitized.jsonl', ui?.consoles || []);
await evidence.writeJson('mail-delivery.sanitized.json', result.mail || { matching_message_count: 0 });
await evidence.writeText('README.md', '# Static-site focus OTP evidence\n\nOpen `qa-summary.json` first. No mailbox body, OTP, email, cookie, token, HAR, trace, video or native hierarchy is retained.\n');
let audit = await evidence.audit();
result.redaction_audit_passed = audit.passed;
await evidence.writeJson('result.json', result);
await evidence.writeJson('qa-summary.json', qaSummary(result));
audit = await evidence.audit();
if (!audit.passed) {
  result.status = 'FAIL'; result.failure_domain = 'FAIL_REDACTION'; result.redaction_audit_passed = false;
  await evidence.writeJson('result.json', result); await evidence.writeJson('qa-summary.json', qaSummary(result));
  process.stderr.write('Evidence redaction gate failed.\n'); process.exitCode = 2;
} else {
  await writeFile(join(evidenceRoot, '.redaction-ok'), 'safe\n', 'utf8');
  if (result.status !== 'PASS') { process.stderr.write(`Focus email OTP E2E ${result.status}: ${result.failure_domain}\n`); process.exitCode = 1; }
  else process.stdout.write(`Focus email OTP E2E PASS; platform=${platform}; evidence=${evidenceRoot}\n`);
}
