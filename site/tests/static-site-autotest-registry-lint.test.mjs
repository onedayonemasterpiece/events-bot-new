import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

import { lintStaticSiteAutotestRegistry } from '../e2e/auth-session-fixture/registry-lint.mjs';

const registryUrl = new URL('../../docs/testing/static-site-autotest-scenarios.v1.yml', import.meta.url);
const focusRegistryUrl = new URL('../../docs/testing/focus-group-release-scenarios.v1.yml', import.meta.url);

test('canonical registries preserve closed auth modes, no-mail policies and anonymous-first focus v5', async () => {
  const [registry, focus] = await Promise.all([
    readFile(registryUrl, 'utf8'),
    readFile(focusRegistryUrl, 'utf8'),
  ]);
  assert.deepEqual(lintStaticSiteAutotestRegistry(registry, focus), []);
});

test('lint catches mail fallback, missing dependency and focus identity regression', async () => {
  const [registry, focus] = await Promise.all([
    readFile(registryUrl, 'utf8'),
    readFile(focusRegistryUrl, 'utf8'),
  ]);
  const brokenRegistry = registry
    .replace('    depends_on: [auth.session_fixture]\n', '')
    .replaceAll('session_fixture_real_mail_fallback: forbidden', 'session_fixture_real_mail_fallback: allowed');
  const brokenFocus = focus.replace('    anonymous_can_submit_feedback: true', '    anonymous_can_submit_feedback: false');
  const errors = lintStaticSiteAutotestRegistry(brokenRegistry, brokenFocus);
  assert.ok(errors.some((item) => item.startsWith('missing_session_fixture_dependency:')));
  assert.ok(errors.includes('registry_policy_missing:session_fixture_real_mail_fallback: forbidden'));
  assert.ok(errors.includes('focus_v5_invariant_missing:anonymous_can_submit_feedback: true'));
});
