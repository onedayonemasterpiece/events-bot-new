import { appendFile } from 'node:fs/promises';

import { createAuthSessionBrokerIssuer,
  createBrowserVerificationCallback } from '../../site/e2e/auth-session-fixture/session-fixture.mjs';

function required(name) {
  const value = String(process.env[name] || '').trim();
  if (!value) throw new Error(`missing_${name.toLowerCase()}`);
  return value;
}

const PLATFORMS = new Set(['browser', 'android', 'ios']);

async function githubOidcToken() {
  const requestUrl = new URL(required('ACTIONS_ID_TOKEN_REQUEST_URL'));
  requestUrl.searchParams.set('audience', required('AUTH_SESSION_BROKER_OIDC_AUDIENCE'));
  const response = await fetch(requestUrl, {
    redirect: 'error',
    headers: { authorization: `Bearer ${required('ACTIONS_ID_TOKEN_REQUEST_TOKEN')}` },
  });
  if (!response.ok) throw new Error(`github_oidc_rejected_${response.status}`);
  const payload = await response.json();
  const token = String(payload?.value || '').trim();
  if (!token || token.includes('\n')) throw new Error('github_oidc_invalid');
  return token;
}

async function main() {
  const targetUrl = required('E2E_SEARCH_TARGET_URL');
  const platform = required('E2E_SEARCH_PLATFORM');
  if (!PLATFORMS.has(platform)) throw new Error('search_platform_invalid');
  const oidcToken = await githubOidcToken();
  const issuer = createAuthSessionBrokerIssuer({
    endpoint: required('AUTH_SESSION_BROKER_URL'),
    oidcToken,
  });
  const credential = await issuer.issue({
    personaId: `search-cached-${platform}`,
    platform,
    redirectTo: targetUrl,
  });
  let actionLink = String(credential.actionLink || '');
  if (!actionLink.startsWith('https://') || /[\r\n]/u.test(actionLink)) {
    throw new Error('broker_action_link_invalid');
  }
  let browserCallback = createBrowserVerificationCallback({ actionLink, redirectTo: targetUrl });
  // Mask before writing to the step environment. The OTP is deliberately not
  // exported: browser/device bootstrap follows only the one-time callback.
  process.stdout.write(`::add-mask::${actionLink}\n`);
  process.stdout.write(`::add-mask::${browserCallback}\n`);
  await appendFile(required('GITHUB_ENV'), `E2E_AUTH_ACTION_LINK=${browserCallback}\n`, { mode: 0o600 });
  credential.emailOtp = '';
  credential.actionLink = '';
  actionLink = '';
  browserCallback = '';
  process.stdout.write('Search one-shot session issued (OTP/mail 0/0).\n');
}

main().catch((error) => {
  process.stderr.write(`${String(error?.message || 'session_issue_failed').replace(/https?:\/\/\S+/gu, '<redacted-url>')}\n`);
  process.exitCode = 1;
});
