import assert from 'node:assert/strict';
import test from 'node:test';

import { parseQaRunCommand, validateIssueEvent } from '../../scripts/static-site-qa-command.mjs';

const command = '/qa run scenario=focus.otp.browser_tab platform=all target_url=https://kenigevents.ru/preview-safe-target/fokus-gruppa/priglashenie/ expected_repo_sha=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa mode=blocking';

test('QA command parser accepts only the exact OTP registry command', () => {
  const parsed = parseQaRunCommand(command);
  assert.equal(parsed.platform, 'all');
  assert.equal(parsed.expected_repo_sha, 'a'.repeat(40));
  for (const unsafe of [`${command}\nanything`, command.replace('mode=blocking', 'mode=advisory'), command.replace('platform=all', 'platform=desktop'), command.replace('kenigevents.ru', 'evil.example')]) {
    assert.throws(() => parseQaRunCommand(unsafe));
  }
  const preflight = command.replace('focus.otp.browser_tab', 'focus.otp.ios_keyboard_preflight').replace('platform=all', 'platform=ios');
  assert.equal(parseQaRunCommand(preflight).scenario, 'focus.otp.ios_keyboard_preflight');
  assert.throws(() => parseQaRunCommand(preflight.replace('platform=ios', 'platform=browser')), /scenario_platform_invalid/u);
});

test('QA command requires the canonical labelled issue', async () => {
  const event = { issue: { number: 42, labels: [{ name: 'static-site-qa-control' }] }, comment: { body: command } };
  assert.equal((await validateIssueEvent(event, { controlIssueNumber: 42 })).scenario, 'focus.otp.browser_tab');
  await assert.rejects(() => validateIssueEvent(event, { controlIssueNumber: 41 }), /mismatch/u);
  await assert.rejects(() => validateIssueEvent({ ...event, issue: { number: 42, labels: [] } }, { controlIssueNumber: 42 }), /label/u);
});
