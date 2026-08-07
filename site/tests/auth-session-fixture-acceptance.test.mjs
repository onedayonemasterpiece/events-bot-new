import assert from 'node:assert/strict';
import test from 'node:test';

import {
  AuthSessionFixtureBlockedError,
  runExactTargetBrowserAcceptance,
  validateExactSearchTarget,
} from '../e2e/auth-session-fixture/acceptance.mjs';

const SHA = '0123456789abcdef0123456789abcdef01234567';
const TOKEN = 'A'.repeat(43);

test('secret candidate exact target identity is bound to its bearer prefix and /poisk/', () => {
  const target = validateExactSearchTarget({
    targetUrl: `https://kenigevents.ru/_review/${TOKEN}/poisk/`,
    allowedOrigins: ['https://kenigevents.ru'], expectedRepoSha: SHA,
  });
  assert.deepEqual({ mode: target.mode, basePath: target.basePath, manifestPath: target.manifestPath,
    searchPath: target.searchPath, expectedRepoSha: target.expectedRepoSha }, {
    mode: 'secret_candidate', basePath: `/_review/${TOKEN}`,
    manifestPath: `/_review/${TOKEN}/candidate-build.json`,
    searchPath: `/_review/${TOKEN}/poisk/`, expectedRepoSha: SHA,
  });
  assert.throws(() => validateExactSearchTarget({
    targetUrl: `https://kenigevents.ru/_review/${TOKEN}/`, allowedOrigins: ['https://kenigevents.ru'], expectedRepoSha: SHA,
  }), /TARGET_NOT_SEARCH/u);
});

test('terminal browser acceptance proves exact SHA, restored /poisk session and zero OTP/mail', async () => {
  const targetUrl = `https://kenigevents.ru/_review/${TOKEN}/poisk/`;
  const calls = [];
  const receipt = await runExactTargetBrowserAcceptance({
    targetUrl, allowedOrigins: ['https://kenigevents.ru'], expectedRepoSha: SHA,
    fixtureReceipt: { product_otp_issue_count: 0, external_mail_send_count: 0,
      external_mail_receipt_count: 0, get_user_verified: true, protected_probe_verified: true },
    adapter: {
      async fetchReleaseIdentity(path) { calls.push(['identity', path]); return {
        status: 200, finalUrl: `https://kenigevents.ru${path}`, repoSha: SHA,
        siteMode: 'secret_candidate', basePath: `/_review/${TOKEN}`,
      }; },
      async openExact(url) { calls.push(['open', url]); return { status: 200, finalUrl: url }; },
      async restoredSession() { calls.push(['session']); return {
        restored: true, pathname: `/_review/${TOKEN}/poisk/`, authenticated: true,
      }; },
      async requestCounters() { return { productOtpPosts: 0, externalMailSends: 0, externalMailReceipts: 0 }; },
    },
    receiptSalt: 'safe-evidence',
  });
  assert.equal(receipt.outcome, 'PASS');
  assert.equal(receipt.terminal, true);
  assert.equal(receipt.observed_repo_sha, SHA);
  assert.equal(receipt.session_restored, true);
  assert.equal(receipt.product_otp_issue_count, 0);
  assert.doesNotMatch(JSON.stringify(receipt), new RegExp(TOKEN, 'u'));
  assert.deepEqual(calls[0], ['identity', `/_review/${TOKEN}/candidate-build.json`]);
});

test('browser acceptance fails closed on redirect, SHA mismatch or unverified fixture', async () => {
  const targetUrl = 'https://kenigevents.ru/poisk/';
  const base = {
    targetUrl, allowedOrigins: ['https://kenigevents.ru'], expectedRepoSha: SHA,
    fixtureReceipt: { product_otp_issue_count: 0, external_mail_send_count: 0,
      external_mail_receipt_count: 0, get_user_verified: true, protected_probe_verified: true },
    adapter: {
      fetchReleaseIdentity: async () => ({ status: 200, finalUrl: 'https://kenigevents.ru/static-release-manifest.json',
        repoSha: SHA, siteMode: 'production', basePath: '' }),
      openExact: async () => ({ status: 200, finalUrl: targetUrl }),
      restoredSession: async () => ({ restored: true, authenticated: true, pathname: '/poisk/' }),
      requestCounters: async () => ({ productOtpPosts: 0, externalMailSends: 0, externalMailReceipts: 0 }),
    },
  };
  await assert.rejects(runExactTargetBrowserAcceptance({ ...base,
    adapter: { ...base.adapter, openExact: async () => ({ status: 200, finalUrl: 'https://kenigevents.ru/' }) },
  }), (error) => error instanceof AuthSessionFixtureBlockedError && error.reason === 'TARGET_REDIRECTED');
  await assert.rejects(runExactTargetBrowserAcceptance({ ...base,
    adapter: { ...base.adapter, fetchReleaseIdentity: async () => ({ status: 200,
      finalUrl: 'https://kenigevents.ru/static-release-manifest.json', repoSha: 'f'.repeat(40), siteMode: 'production', basePath: '' }) },
  }), /TARGET_SHA_MISMATCH/u);
  await assert.rejects(runExactTargetBrowserAcceptance({ ...base,
    fixtureReceipt: { ...base.fixtureReceipt, protected_probe_verified: false },
  }), /FIXTURE_RECEIPT_INVALID/u);
});
