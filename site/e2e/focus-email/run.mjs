import process from 'node:process';
import { mkdir, writeFile } from 'node:fs/promises';
import { join, resolve } from 'node:path';

import { chromium } from 'playwright';

import { createEvidenceWriter, publicResult } from './helpers/evidence.mjs';
import { FocusOtpMailbox } from './helpers/imap-mailbox.mjs';
import { redactText } from './helpers/redaction.mjs';
import { createSanitizedNetworkRecorder } from './helpers/sanitized-network-recorder.mjs';
import { recipientForRun, sanitizedRunId, targetEvidence, validateFocusE2eTarget } from './helpers/target-url.mjs';

function required(name) {
  const value = String(process.env[name] || '').trim();
  if (!value) throw new Error(`missing_configuration:${name}`);
  return value;
}

function failureDomain(error) {
  const message = String(error?.message || error);
  if (/missing_configuration|imap_configuration|target_url/iu.test(message)) return 'BLOCKED_INFRASTRUCTURE';
  if (/release_evidence/iu.test(message)) return 'FAIL_RELEASE_EVIDENCE';
  if (/mail_/iu.test(message)) return 'FAIL_DELIVERY';
  return 'FAIL_PRODUCT';
}

function expectedRepoSha() {
  const value = String(process.env.E2E_EXPECTED_REPO_SHA || '').trim().toLowerCase();
  if (value && !/^[0-9a-f]{40}$/u.test(value)) throw new Error('release_evidence_expected_sha_invalid');
  return value || null;
}

function previewBuildUrl(target) {
  const [prefix] = target.pathname.split('/').filter(Boolean);
  return new URL(prefix?.startsWith('preview-') ? `/${prefix}/preview-build.json` : '/preview-build.json', target.origin);
}

async function observedRepoSha(page, target, expected) {
  const response = await page.request.get(previewBuildUrl(target).href, { failOnStatusCode: false, timeout: 15_000 });
  if (!response.ok()) {
    if (expected) throw new Error(`release_evidence_metadata_status:${response.status()}`);
    return null;
  }
  let body;
  try { body = await response.json(); } catch { throw new Error('release_evidence_metadata_not_json'); }
  const value = String(body?.repo_sha || '').trim().toLowerCase();
  if (!/^[0-9a-f]{40}$/u.test(value)) {
    if (expected) throw new Error('release_evidence_repo_sha_missing');
    return null;
  }
  if (expected && value !== expected) throw new Error('release_evidence_repo_sha_mismatch');
  return value;
}

async function maskPage(page) {
  await page.evaluate(() => {
    document.querySelectorAll('input[type="email"], input[autocomplete="one-time-code"]').forEach((node) => {
      node.value = '';
      node.setAttribute('data-e2e-masked', 'true');
    });
    document.querySelectorAll('[data-focus-email-destination], [data-static-auth-name]').forEach((node) => {
      node.textContent = 'f***@k***';
    });
    document.querySelectorAll('[data-focus-otp-digit]').forEach((node) => {
      if (node.textContent) node.textContent = '•';
    });
  });
}

const runId = sanitizedRunId(process.env.E2E_RUN_ID || `local-${Date.now()}`);
const timeoutMs = Math.min(180_000, Math.max(30_000, Number(process.env.E2E_MAIL_TIMEOUT_MS || 120_000)));
const evidenceRoot = resolve(process.env.E2E_EVIDENCE_DIR || `artifacts/external-focus-email-otp-evidence-${runId}`);
let target;
let recipient = '';
let coverage = 'configuration_unverified';
const secrets = [process.env.E2E_IMAP_USERNAME, process.env.E2E_IMAP_PASSWORD].filter(Boolean);
const evidence = await createEvidenceWriter({ root: evidenceRoot, secrets });
const steps = [];
const consoles = [];
let mailbox;
let browser;
let context;
let page;
let recorder = { entries: [], count: () => 0, statuses: () => [] };
let otp = '';
let result = publicResult({
  status: 'FAIL',
  coverage,
  target_origin: null,
  target_path: null,
  expected_repo_sha: null,
  observed_repo_sha: null,
  browser: { name: 'chromium', version: null, viewport: { width: 390, height: 844 } },
  otp_issue_request_count: 0,
  otp_verify_request_count: 0,
  participant_registration_request_count: 0,
  failures: [],
});

const step = (name, status = 'passed') => steps.push({ sequence: steps.length + 1, name, status });

try {
  target = validateFocusE2eTarget(required('E2E_TARGET_URL'));
  ({ recipient, coverage } = recipientForRun(required('E2E_RECIPIENT_TEMPLATE'), runId));
  secrets.push(recipient);
  result = publicResult({
    ...result,
    coverage,
    ...targetEvidence(target),
    expected_repo_sha: expectedRepoSha(),
  });
  mailbox = new FocusOtpMailbox({
    host: required('E2E_IMAP_HOST'),
    port: Number(process.env.E2E_IMAP_PORT || 993),
    secure: String(process.env.E2E_IMAP_SECURE || 'true').toLowerCase() === 'true',
    username: required('E2E_IMAP_USERNAME'),
    password: required('E2E_IMAP_PASSWORD'),
    expectedFrom: required('E2E_EXPECTED_FROM_PATTERN'),
    expectedSubject: required('E2E_EXPECTED_SUBJECT_PATTERN'),
  });
  await mailbox.connect();
  const checkpoint = await mailbox.checkpoint();
  step('mailbox_checkpoint');

  browser = await chromium.launch({ headless: true });
  result.browser.version = browser.version();
  context = await browser.newContext({ viewport: result.browser.viewport });
  page = await context.newPage();
  page.on('console', (message) => {
    const type = message.type();
    if (['error', 'warning'].includes(type)) consoles.push({ type, text: redactText(message.text(), [...secrets, otp]) });
  });
  const directHost = String(process.env.E2E_SUPABASE_HOST || '').trim();
  const relayHost = String(process.env.E2E_RELAY_HOST || '').trim();
  recorder = createSanitizedNetworkRecorder(page, { directHost, relayHost });

  await page.goto(target.href, { waitUntil: 'domcontentloaded', timeout: 30_000 });
  if (new URL(page.url()).origin !== target.origin) throw new Error('navigation_left_allowed_origin');
  result.observed_repo_sha = await observedRepoSha(page, target, result.expected_repo_sha);
  step('release_identity_checked');
  await page.locator('[data-intake-stage="install"]:not([hidden])').waitFor({ timeout: 20_000 });
  await maskPage(page);
  await page.screenshot({ path: join(evidenceRoot, 'screenshots/01-invite-accepted.png'), fullPage: true });
  step('invite_accepted');

  await page.locator('[data-focus-install-skip]').click();
  await page.locator('[data-focus-email-open]').click();
  const email = page.locator('#focus-email');
  await email.fill(recipient);
  await maskPage(page);
  await page.screenshot({ path: join(evidenceRoot, 'screenshots/02-email-step.png'), fullPage: true });
  await email.fill(recipient);
  step('email_entered');

  const send = page.locator('[data-focus-email-send]');
  // Two ordinary user gestures race in the same UI. The product single-flight
  // guard must reduce them to one /auth/v1/otp request.
  await Promise.allSettled([send.click({ noWaitAfter: true }), email.press('Enter')]);
  await page.locator('[data-focus-email-code-step]:not([hidden])').waitFor({ timeout: 25_000 });
  await maskPage(page);
  await page.screenshot({ path: join(evidenceRoot, 'screenshots/03-mail-accepted-ui.png'), fullPage: true });
  step('mail_request_accepted_ui');

  const mail = await mailbox.waitForSingleOtp({ checkpoint, recipient, timeoutMs });
  otp = mail.otp;
  if (process.env.GITHUB_ACTIONS === 'true') process.stdout.write(`::add-mask::${otp}\n`);
  step('single_inbox_message_received');

  const otpInput = page.locator('#focus-email-otp');
  await otpInput.focus();
  await otpInput.pressSequentially(otp, { delay: 45 });
  await page.locator('[data-focus-done-title]').filter({ hasText: 'Участие подтверждено' }).waitFor({ timeout: 30_000 });
  step('otp_autosubmit_confirmed');
  await maskPage(page);
  await page.screenshot({ path: join(evidenceRoot, 'screenshots/04-membership-confirmed.png'), fullPage: true });

  const issueCount = recorder.count('POST', '/auth/v1/otp');
  const verifyCount = recorder.count('POST', '/auth/v1/verify');
  const registrationCount = recorder.count('POST', '/rpc/register_focus_group_participant_v1');
  if (issueCount !== 1) throw new Error(`otp_issue_count:${issueCount}`);
  if (verifyCount !== 1) throw new Error(`otp_verify_count:${verifyCount}`);
  if (registrationCount !== 1) throw new Error(`participant_registration_count:${registrationCount}`);
  const registrationStatus = recorder.statuses('/rpc/register_focus_group_participant_v1').at(-1);
  if (![200, 204, 409].includes(registrationStatus)) throw new Error(`participant_registration_status:${registrationStatus}`);

  await page.reload({ waitUntil: 'domcontentloaded', timeout: 30_000 });
  await page.locator('[data-focus-done-title]').filter({ hasText: /Вы уже в фокус-группе|Участие подтверждено/u }).waitFor({ timeout: 20_000 });
  if (recorder.count('POST', '/auth/v1/otp') !== 1) throw new Error('otp_reissued_after_reload');
  step('returning_state_persisted');
  await maskPage(page);
  await page.screenshot({ path: join(evidenceRoot, 'screenshots/05-returning-state.png'), fullPage: true });

  result = publicResult({
    ...result,
    status: 'PASS',
    failure_domain: null,
    otp_issue_request_count: issueCount,
    otp_verify_request_count: verifyCount,
    participant_registration_request_count: registrationCount,
    participant_registration_status: registrationStatus,
    mail: {
      matching_message_count: mail.matchingMessageCount,
      folder: 'inbox',
      delivery_latency_ms: mail.deliveryLatencyMs,
      otp_length: otp.length,
      message_id_hash: mail.messageIdHash,
    },
    final_ui_state: 'membership_confirmed',
    reload_state: 'returning_member',
    console_error_count: consoles.filter((item) => item.type === 'error').length,
    essential_failed_request_count: recorder.entries.filter((item) => item.failure_class && /auth\/v1|register_focus/u.test(item.path)).length,
    failures: [],
  });
} catch (error) {
  const message = redactText(String(error?.message || error), [...secrets, otp]);
  result = publicResult({
    ...result,
    status: /^BLOCKED_/u.test(failureDomain(error)) ? 'BLOCKED' : 'FAIL',
    failure_domain: failureDomain(error),
    otp_issue_request_count: recorder.count('POST', '/auth/v1/otp'),
    otp_verify_request_count: recorder.count('POST', '/auth/v1/verify'),
    participant_registration_request_count: recorder.count('POST', '/rpc/register_focus_group_participant_v1'),
    participant_registration_status: recorder.statuses('/rpc/register_focus_group_participant_v1').at(-1) ?? null,
    failures: [message],
  });
  step('journey_failed', 'failed');
} finally {
  await mailbox?.close();
  await context?.close();
  await browser?.close();
}

await mkdir(evidenceRoot, { recursive: true });
await evidence.writeJson('manifest.json', { schema_version: 1, artifact: `external-focus-email-otp-evidence-${runId}`, files_are_sanitized: true });
await evidence.writeJson('result.json', result);
await evidence.writeJson('steps.json', steps);
await evidence.writeJsonl('network.sanitized.jsonl', recorder.entries);
await evidence.writeJsonl('console.sanitized.jsonl', consoles);
await evidence.writeJson('mail-delivery.sanitized.json', result.mail || { matching_message_count: 0 });
await evidence.writeText('README.md', '# External focus email OTP evidence\n\nOpen `result.json` first. No mailbox body, OTP, email, cookie, token, HAR, trace or video is retained.\n');
let audit = await evidence.audit();
result.redaction_audit_passed = audit.passed;
await evidence.writeJson('result.json', result);
audit = await evidence.audit();
if (!audit.passed) {
  process.stderr.write('Evidence redaction gate failed.\n');
  process.exitCode = 2;
} else if (result.status !== 'PASS') {
  await writeFile(join(evidenceRoot, '.redaction-ok'), 'safe\n', 'utf8');
  process.stderr.write(`Focus email OTP E2E ${result.status}: ${result.failure_domain}\n`);
  process.exitCode = 1;
} else {
  await writeFile(join(evidenceRoot, '.redaction-ok'), 'safe\n', 'utf8');
  process.stdout.write(`Focus email OTP E2E PASS; evidence=${evidenceRoot}\n`);
}
