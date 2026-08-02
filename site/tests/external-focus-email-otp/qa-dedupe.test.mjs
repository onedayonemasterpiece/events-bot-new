import assert from 'node:assert/strict';
import test from 'node:test';

import { classifyDuplicateActiveRequest } from '../../scripts/static-site-qa-dedupe.mjs';

const command = '/qa run scenario=focus.otp.browser_tab platform=browser target_url=https://kenigevents.ru/fokus-gruppa/priglashenie/ expected_repo_sha=0123456789012345678901234567890123456789 mode=blocking';
const comment = (id, created_at, body, type = 'User') => ({ id, created_at, body, user: { type } });

test('deduplicates an identical command posted while its prior run was active', () => {
  const prior = comment(1, '2026-08-02T10:00:00Z', command);
  const current = comment(2, '2026-08-02T10:01:00Z', command);
  const accepted = comment(3, '2026-08-02T10:00:30Z', 'ACCEPTED · focus.otp.browser_tab · browser · sha · target · run https://github.com/o/r/actions/runs/42', 'Bot');
  const terminal = comment(4, '2026-08-02T10:03:00Z', 'TERMINAL · focus.otp.browser_tab · browser · workflow=success · sanitized artifacts: https://github.com/o/r/actions/runs/42', 'Bot');
  assert.deepEqual(classifyDuplicateActiveRequest({ current, comments: [prior, current, accepted, terminal] }), {
    duplicate: true,
    runId: '42',
  });
});

test('allows an intentional identical rerun posted after the prior terminal receipt', () => {
  const prior = comment(1, '2026-08-02T10:00:00Z', command);
  const accepted = comment(2, '2026-08-02T10:00:30Z', 'ACCEPTED · focus.otp.browser_tab · browser · sha · target · run https://github.com/o/r/actions/runs/42', 'Bot');
  const terminal = comment(3, '2026-08-02T10:03:00Z', 'TERMINAL · focus.otp.browser_tab · browser · workflow=success · sanitized artifacts: https://github.com/o/r/actions/runs/42', 'Bot');
  const current = comment(4, '2026-08-02T10:04:00Z', command);
  assert.equal(classifyDuplicateActiveRequest({ current, comments: [prior, accepted, terminal, current] }).duplicate, false);
});

test('does not trust a user-authored fake receipt', () => {
  const prior = comment(1, '2026-08-02T10:00:00Z', command);
  const current = comment(2, '2026-08-02T10:01:00Z', command);
  const accepted = comment(3, '2026-08-02T10:00:30Z', 'ACCEPTED · focus.otp.browser_tab · run https://github.com/o/r/actions/runs/42');
  const terminal = comment(4, '2026-08-02T10:03:00Z', 'TERMINAL · focus.otp.browser_tab · https://github.com/o/r/actions/runs/42');
  assert.equal(classifyDuplicateActiveRequest({ current, comments: [prior, current, accepted, terminal] }).duplicate, false);
});
