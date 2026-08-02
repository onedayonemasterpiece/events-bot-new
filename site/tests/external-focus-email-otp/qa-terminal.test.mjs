import assert from 'node:assert/strict';
import test from 'node:test';

import { formatTerminalReceipt } from '../../scripts/static-site-qa-terminal.mjs';

test('terminal receipt reports qa-summary truth rather than caller workflow success', () => {
  const body = formatTerminalReceipt({ scenario: 'focus.otp.ios_keyboard_preflight', platform: 'ios', runUrl: 'https://github.com/o/r/actions/runs/42', summaries: [{
    platform: 'ios', status: 'BLOCKED', failure_domain: 'BLOCKED_SAFARI_FIRST_RUN_UI',
    counts: { issue: 0, verify: 0, registration: 0 }, keyboard_acceptance: null,
    warnings: [{ code: 'BEST_EFFORT_AUTH_TELEMETRY_403' }], redaction_status: 'passed',
    provenance: { harness_repo_sha: 'a'.repeat(40), tested_repo_sha: 'b'.repeat(40), observed_preview_sha: 'c'.repeat(40) },
  }] });
  assert.match(body, /^TERMINAL · focus\.otp\.ios_keyboard_preflight · ios ·/u);
  assert.match(body, /BLOCKED\/BLOCKED_SAFARI_FIRST_RUN_UI/u);
  assert.match(body, /counts=0\/0\/0/u);
  assert.match(body, /BEST_EFFORT_AUTH_TELEMETRY_403/u);
  assert.doesNotMatch(body, /workflow=success/u);
});

test('terminal receipt fails closed when no sanitized summary exists', () => {
  const body = formatTerminalReceipt({ scenario: 'focus.otp.browser_tab', platform: 'ios', summaries: [], runUrl: 'https://github.com/o/r/actions/runs/1' });
  assert.match(body, /FAIL_CONTROL_PLANE_NO_QA_SUMMARY/u);
});

test('all-platform receipt fails closed when any platform summary is missing', () => {
  const body = formatTerminalReceipt({ scenario: 'focus.otp.browser_tab', platform: 'all', runUrl: 'https://github.com/o/r/actions/runs/2', summaries: [{
    platform: 'browser', status: 'PASS', failure_domain: null, counts: { issue: 1, verify: 1, registration: 1 }, redaction_status: 'passed',
  }] });
  assert.match(body, /FAIL_CONTROL_PLANE_MISSING_SUMMARY/u);
  assert.match(body, /missing=android,ios/u);
});
