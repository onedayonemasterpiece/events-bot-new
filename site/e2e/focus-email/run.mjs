import process from 'node:process';
import { writeFile } from 'node:fs/promises';
import { join, resolve } from 'node:path';

import { createAppiumUi } from './adapters/appium-ui.mjs';
import { createPlaywrightUi } from './adapters/playwright-ui.mjs';
import { createEvidenceWriter, junitXml, publicResult, qaSummary } from './helpers/evidence.mjs';
import { focusOtpFailureDomain } from './helpers/failure-domain.mjs';
import { createMailbox } from './helpers/mailbox.mjs';
import { validatePlatform } from './helpers/platform.mjs';
import { redactText } from './helpers/redaction.mjs';
import { summarizeRuntimeDiagnostics } from './helpers/runtime-diagnostics.mjs';
import { recipientForRun, sanitizedRunId, targetEvidence, validateFocusE2eTarget } from './helpers/target-url.mjs';
import { runFocusOtpBrowserTab, runIosKeyboardPreflight } from './journey.mjs';

const FULL_SCENARIO = 'focus.otp.browser_tab';
const PREFLIGHT_SCENARIO = 'focus.otp.ios_keyboard_preflight';
const startedAt = Date.now();
const required = (name) => { const value = String(process.env[name] || '').trim(); if (!value) throw new Error(`missing_configuration:${name}`); return value; };
const addMask = (value) => { if (value && process.env.GITHUB_ACTIONS === 'true') process.stdout.write(`::add-mask::${value}\n`); };

function expectedRepoSha() {
  const value = String(process.env.E2E_EXPECTED_REPO_SHA || '').trim().toLowerCase();
  if (value && !/^[0-9a-f]{40}$/u.test(value)) throw new Error('release_evidence_expected_sha_invalid');
  return value || null;
}

const runId = sanitizedRunId(process.env.E2E_RUN_ID || `local-${Date.now()}`);
const platform = validatePlatform(process.env.E2E_PLATFORM || 'browser');
const scenario = String(process.env.E2E_SCENARIO_ID || FULL_SCENARIO).trim();
if (![FULL_SCENARIO, PREFLIGHT_SCENARIO].includes(scenario)) throw new Error(`scenario_invalid:${scenario}`);
if (scenario === PREFLIGHT_SCENARIO && platform !== 'ios') throw new Error('scenario_platform_invalid:ios_keyboard_preflight');
const timeoutMs = Math.min(180_000, Math.max(30_000, Number(process.env.E2E_MAIL_TIMEOUT_MS || 120_000)));
const evidenceRoot = resolve(process.env.E2E_EVIDENCE_DIR || `artifacts/external-focus-email-otp-evidence-${platform}-${runId}`);
const secrets = [process.env.E2E_IMAP_USERNAME, process.env.E2E_IMAP_PASSWORD, process.env.E2E_YANDEX_MAIL_WS_URL].filter(Boolean);
const evidence = await createEvidenceWriter({ root: evidenceRoot, secrets });
const steps = [];
const step = (name, status = 'passed') => steps.push({ sequence: steps.length + 1, name, status });
let target; let mailbox; let ui; let recipient = ''; let otp = ''; let coverage = scenario === PREFLIGHT_SCENARIO ? 'side_effect_free_keyboard_preflight' : 'configuration_unverified';
const emptyRecorder = { entries: [], count: () => 0, statuses: () => [] };
let recorder = emptyRecorder;
let diagnostics = summarizeRuntimeDiagnostics([], []);
let result = publicResult({ scenario_id: scenario, platform, status: 'FAIL', coverage, target_origin: null, target_path: null,
  expected_repo_sha: null, observed_repo_sha: null, otp_issue_request_count: 0, otp_verify_request_count: 0,
  participant_registration_request_count: 0, failures: [] });

try {
  target = validateFocusE2eTarget(required('E2E_TARGET_URL'));
  const expected = expectedRepoSha();
  result = publicResult({ ...result, coverage, ...targetEvidence(target), expected_repo_sha: expected });
  if (scenario === FULL_SCENARIO) {
    ({ recipient, coverage } = recipientForRun(required('E2E_RECIPIENT_TEMPLATE'), runId));
    addMask(recipient);
    secrets.push(recipient);
    mailbox = createMailbox(process.env);
  }
  const uiOptions = { platform, target, expectedRepoSha: expected, evidenceRoot, secrets,
    directHost: String(process.env.E2E_SUPABASE_HOST || '').trim(), relayHost: String(process.env.E2E_RELAY_HOST || '').trim() };
  ui = platform === 'browser' ? await createPlaywrightUi(uiOptions) : await createAppiumUi(uiOptions);
  recorder = ui.recorder;
  const onSecret = (value) => { otp = value; addMask(value); if (value && !secrets.includes(value)) secrets.push(value); };
  const journey = scenario === PREFLIGHT_SCENARIO
    ? await runIosKeyboardPreflight({ ui, step })
    : await runFocusOtpBrowserTab({ ui, mailbox, recipient, timeoutMs, step, onSecret });
  diagnostics = summarizeRuntimeDiagnostics(recorder.entries, ui.consoles);
  if (diagnostics.blocking_failure_count > 0) throw new Error(`runtime_diagnostics:blocking:${diagnostics.blocking_failure_count}`);
  result = publicResult({ ...result, coverage, status: 'PASS', failure_domain: null, observed_repo_sha: journey.observedRepoSha,
    browser: platform === 'browser' ? ui.device : null, device: ui.device,
    keyboard_acceptance: platform === 'browser' ? null : journey.keyboardAcceptance,
    keyboard_preflight: ui.keyboardPreflight || null, safari_startup: ui.safariStartup || null,
    otp_issue_request_count: journey.counts.issue, otp_verify_request_count: journey.counts.verify,
    participant_registration_request_count: journey.counts.registration,
    participant_registration_status: journey.counts.registrationStatus,
    mail: journey.mail ? { matching_message_count: journey.mail.matchingMessageCount,
      folder: process.env.E2E_MAIL_ADAPTER === 'yandex-websocket' ? 'mail-trigger' : 'inbox',
      delivery_latency_ms: journey.mail.deliveryLatencyMs, otp_length: otp.length, message_id_hash: journey.mail.messageIdHash } : null,
    final_ui_state: scenario === PREFLIGHT_SCENARIO ? 'product_email_keyboard_visible' : 'membership_confirmed',
    reload_state: scenario === PREFLIGHT_SCENARIO ? null : 'returning_member',
    console_error_count: ui.consoles.filter((item) => item.type === 'error').length,
    essential_failed_request_count: diagnostics.blocking_failure_count, diagnostics, failures: [] });
} catch (error) {
  await ui?.requestCounts?.().catch(() => undefined);
  diagnostics = summarizeRuntimeDiagnostics(recorder.entries, ui?.consoles || []);
  const domain = focusOtpFailureDomain(error);
  const message = redactText(String(error?.message || error), [...secrets, otp]);
  const failedStep = steps.length ? `after_${steps.at(-1).name}` : 'startup';
  result = publicResult({ ...result, scenario_id: scenario, platform, status: domain.startsWith('BLOCKED_') ? 'BLOCKED' : 'FAIL', failure_domain: domain,
    observed_repo_sha: ui?.observedRepoSha || result.observed_repo_sha || null,
    browser: platform === 'browser' ? ui?.device || null : null, device: ui?.device || null,
    keyboard_acceptance: platform === 'browser' ? null : ui?.keyboard || null,
    keyboard_preflight: ui?.keyboardPreflight || null, safari_startup: ui?.safariStartup || null,
    otp_issue_request_count: recorder.count('POST', '/auth/v1/otp'), otp_verify_request_count: recorder.count('POST', '/auth/v1/verify'),
    participant_registration_request_count: recorder.count('POST', '/rpc/register_focus_group_participant_v1'),
    participant_registration_status: recorder.statuses('/rpc/register_focus_group_participant_v1').at(-1) ?? null,
    mail: mailbox?.safeDiagnostics?.({ recipient }) || result.mail || null, diagnostics,
    first_failed_step: failedStep, failures: [message] });
  step('journey_failed', 'failed');
} finally {
  await mailbox?.close().catch(() => undefined);
  await ui?.close().catch(() => undefined);
}

const artifactName = `static-site-qa-${scenario.replaceAll('.', '-')}-${platform}-${runId}`;
const provenance = {
  harness_repo_sha: String(process.env.E2E_HARNESS_REPO_SHA || process.env.GITHUB_SHA || 'unreported'),
  tested_repo_sha: result.expected_repo_sha,
  observed_preview_sha: result.observed_repo_sha,
  workflow_name: String(process.env.GITHUB_WORKFLOW || 'local'),
  workflow_ref: String(process.env.GITHUB_WORKFLOW_REF || 'local'),
  workflow_source_ref: String(process.env.GITHUB_REF || 'local'),
  workflow_sha: String(process.env.GITHUB_SHA || 'unreported'),
  workflow_run_id: String(process.env.GITHUB_RUN_ID || 'local'),
  workflow_run_attempt: String(process.env.GITHUB_RUN_ATTEMPT || '1'), runner_name: String(process.env.RUNNER_NAME || 'local'),
  runner_os: String(process.env.RUNNER_OS || process.platform), runner_arch: String(process.env.RUNNER_ARCH || process.arch),
  runner_image_os: String(process.env.ImageOS || 'unreported'), runner_image_version: String(process.env.ImageVersion || 'unreported'),
  host_os_version: result.device?.os_version || null, platform_runtime_version: result.device?.platform_version || null,
  device_name: result.device?.device_name || null,
  appium_version: result.device?.appium_server || null, appium_driver_version: result.device?.driver_version || null,
  wda_version: String(process.env.E2E_WDA_VERSION || 'unreported'), wda_sha: result.device?.wda_sha || null,
  xcode_version: result.device?.xcode_version || null,
  artifact_name: artifactName,
};
await evidence.writeJson('manifest.json', { schema_version: 3, artifact: artifactName,
  scenario_id: scenario, provenance, files_are_sanitized: true, forbidden_artifacts: ['har', 'trace', 'video', 'native-hierarchy', 'raw-appium-log'] });
await evidence.writeJson('result.json', result);
await evidence.writeJson('qa-summary.json', qaSummary(result, provenance));
await evidence.writeJson('steps.json', steps);
await evidence.writeJsonl('scenarios.jsonl', [{ scenario_id: scenario, platform, status: result.status, failure_domain: result.failure_domain }]);
await evidence.writeText('junit.xml', junitXml(result, (Date.now() - startedAt) / 1_000));
await evidence.writeJson('device.json', result.device || { platform, status: 'unavailable' });
await evidence.writeJsonl('network.sanitized.jsonl', recorder.entries);
await evidence.writeJsonl('console.sanitized.jsonl', ui?.consoles || []);
await evidence.writeJson('runtime-diagnostics.json', diagnostics);
await evidence.writeJson('mail-delivery.sanitized.json', result.mail || { matching_message_count: 0 });
await evidence.writeText('README.md', '# Static-site focus OTP evidence\n\nOpen `qa-summary.json` first. No mailbox body, OTP, email, cookie, token, HAR, trace, video, raw Appium log or native hierarchy is retained.\n');
let audit = await evidence.audit();
result.redaction_audit_passed = audit.passed;
await evidence.writeJson('result.json', result);
await evidence.writeJson('qa-summary.json', qaSummary(result, provenance));
audit = await evidence.audit();
if (!audit.passed) {
  result.status = 'FAIL'; result.failure_domain = 'FAIL_REDACTION'; result.redaction_audit_passed = false;
  await evidence.writeJson('result.json', result); await evidence.writeJson('qa-summary.json', qaSummary(result, provenance));
  process.stderr.write('Evidence redaction gate failed.\n'); process.exitCode = 2;
} else {
  await writeFile(join(evidenceRoot, '.redaction-ok'), 'safe\n', 'utf8');
  if (result.status !== 'PASS') { process.stderr.write(`Focus email OTP E2E ${result.status}: ${result.failure_domain}\n`); process.exitCode = 1; }
  else process.stdout.write(`Focus email OTP E2E PASS; scenario=${scenario}; platform=${platform}; evidence=${evidenceRoot}\n`);
}
