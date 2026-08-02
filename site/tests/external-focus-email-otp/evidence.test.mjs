import assert from 'node:assert/strict';
import test from 'node:test';

import { publicResult, qaSummary } from '../../e2e/focus-email/helpers/evidence.mjs';

test('evidence result exposes the stable PII-free schema only', () => {
  const result = publicResult({
    status: 'PASS', coverage: 'returning_test_identity', target_origin: 'https://kenigevents.ru',
    target_path: '/preview-x/fokus-gruppa/priglashenie/', browser: { name: 'chromium' },
    otp_issue_request_count: 1, otp_verify_request_count: 1, participant_registration_request_count: 1,
    mail: { matching_message_count: 1, otp_length: 6, message_id_hash: 'abcd' },
    redaction_audit_passed: true,
  });
  assert.equal(result.schema_version, 2);
  assert.equal(result.otp_issue_request_count, 1);
  assert.equal('email' in result, false);
  assert.equal('otp' in result, false);
  assert.equal('headers' in result, false);
  const summary = qaSummary(result);
  assert.equal(summary.scenario_id, 'focus.otp.browser_tab');
  assert.equal(summary.evidence.native_ui, 'native-ui/');
});
