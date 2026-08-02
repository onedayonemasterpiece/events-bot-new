import assert from 'node:assert/strict';
import test from 'node:test';

import { runFocusOtpBrowserTab } from '../../e2e/focus-email/journey.mjs';

test('shared journey calls semantic operations in business order and keeps side effects singular', async () => {
  const calls = [];
  const method = (name, value) => async () => { calls.push(name); return value; };
  const ui = {
    openInvite: method('openInvite'), verifyReleaseIdentity: method('verifyReleaseIdentity', 'a'.repeat(40)),
    waitForInstallStage: method('waitForInstallStage'), captureMaskedEvidence: async (name) => calls.push(`capture:${name}`),
    skipInstall: method('skipInstall'), openEmailStep: method('openEmailStep'), focusEmailInput: method('focusEmailInput', { passed: true }),
    enterEmail: method('enterEmail'), requestOtpWithCompetingGestures: method('requestOtpWithCompetingGestures'),
    waitForCodeStep: method('waitForCodeStep'), setOtpSecret: () => calls.push('setOtpSecret'),
    focusOtpInput: method('focusOtpInput', { passed: true }), enterOtpDigitByDigit: method('enterOtpDigitByDigit'),
    waitForMembershipConfirmed: method('waitForMembershipConfirmed'),
    requestCounts: method('requestCounts', { issue: 1, verify: 1, registration: 1, registrationStatus: 200 }),
    reloadOrReopen: method('reloadOrReopen'), waitForReturningMember: method('waitForReturningMember'),
  };
  const mailbox = { connect: method('mailbox.connect'), checkpoint: method('mailbox.checkpoint', 5),
    waitForSingleOtp: method('mailbox.waitForSingleOtp', { otp: '123456', matchingMessageCount: 1 }) };
  const result = await runFocusOtpBrowserTab({ ui, mailbox, recipient: 'masked-at-runtime', timeoutMs: 30_000, step: () => {} });
  assert.equal(result.counts.issue, 1);
  assert.equal(calls.filter((value) => value === 'requestOtpWithCompetingGestures').length, 1);
  assert.ok(calls.indexOf('mailbox.checkpoint') < calls.indexOf('requestOtpWithCompetingGestures'));
  assert.ok(calls.indexOf('requestOtpWithCompetingGestures') < calls.indexOf('mailbox.waitForSingleOtp'));
  assert.ok(calls.indexOf('enterOtpDigitByDigit') < calls.indexOf('reloadOrReopen'));
});

test('shared journey rejects counts other than exactly one without a resend', async () => {
  const ui = new Proxy({}, { get: (_target, name) => name === 'requestCounts'
    ? async () => ({ issue: 2, verify: 1, registration: 1, registrationStatus: 200 })
    : name === 'verifyReleaseIdentity' ? async () => 'a'.repeat(40)
      : name === 'focusEmailInput' || name === 'focusOtpInput' ? async () => null
        : name === 'setOtpSecret' ? () => {} : async () => {} });
  const mailbox = { connect: async () => {}, checkpoint: async () => 1,
    waitForSingleOtp: async () => ({ otp: '123456', matchingMessageCount: 1 }) };
  await assert.rejects(() => runFocusOtpBrowserTab({ ui, mailbox, recipient: 'masked', timeoutMs: 30_000, step: () => {} }), /otp_issue_count:2/u);
});
