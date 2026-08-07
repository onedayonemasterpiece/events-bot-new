import { createHash } from 'node:crypto';

import { AuthSessionFixtureBlockedError } from './session-fixture.mjs';

export { AuthSessionFixtureBlockedError };

function blocked(reason, cause) {
  return new AuthSessionFixtureBlockedError(reason, cause);
}

function hash(value, salt) {
  return createHash('sha256').update(`${salt}:${value}`).digest('hex').slice(0, 20);
}

function exactUrl(value, reason) {
  try {
    return new URL(String(value || ''));
  } catch (error) {
    throw blocked(reason, error);
  }
}

export function validateExactSearchTarget(options = {}) {
  const target = exactUrl(options.targetUrl, 'TARGET_INVALID');
  if (target.protocol !== 'https:' || target.username || target.password || target.search || target.hash) {
    throw blocked('TARGET_INVALID');
  }
  const origins = new Set((options.allowedOrigins || ['https://kenigevents.ru'])
    .map((value) => exactUrl(value, 'TARGET_ALLOWLIST_INVALID').origin));
  if (!origins.has(target.origin)) throw blocked('TARGET_NOT_ALLOWLISTED');
  const expectedRepoSha = String(options.expectedRepoSha || '').trim().toLowerCase();
  if (!/^[0-9a-f]{40}$/u.test(expectedRepoSha)) throw blocked('TARGET_SHA_INVALID');

  const candidate = target.pathname.match(/^\/_review\/([A-Za-z0-9_-]{43})\/poisk\/$/u);
  if (candidate) {
    const basePath = `/_review/${candidate[1]}`;
    return Object.freeze({
      url: target, mode: 'secret_candidate', basePath,
      searchPath: `${basePath}/poisk/`, manifestPath: `${basePath}/candidate-build.json`, expectedRepoSha,
    });
  }
  if (target.pathname !== '/poisk/') throw blocked('TARGET_NOT_SEARCH');
  return Object.freeze({
    url: target, mode: 'production', basePath: '', searchPath: '/poisk/',
    manifestPath: '/static-release-manifest.json', expectedRepoSha,
  });
}

function assertFixtureReceipt(receipt) {
  if (!receipt || receipt.get_user_verified !== true || receipt.protected_probe_verified !== true
    || receipt.product_otp_issue_count !== 0 || receipt.external_mail_send_count !== 0
    || receipt.external_mail_receipt_count !== 0) {
    throw blocked('FIXTURE_RECEIPT_INVALID');
  }
}

function exactZeroCounters(value) {
  if (!value || value.productOtpPosts !== 0 || value.externalMailSends !== 0 || value.externalMailReceipts !== 0) {
    throw blocked('BROWSER_SIDE_EFFECT_COUNTERS_INVALID');
  }
}

export async function runExactTargetBrowserAcceptance(options = {}) {
  const target = validateExactSearchTarget(options);
  assertFixtureReceipt(options.fixtureReceipt);
  const adapter = options.adapter;
  for (const method of ['fetchReleaseIdentity', 'openExact', 'restoredSession', 'requestCounters']) {
    if (typeof adapter?.[method] !== 'function') throw blocked('BROWSER_ADAPTER_INVALID');
  }

  const manifestUrl = new URL(target.manifestPath, target.url.origin).href;
  const identity = await adapter.fetchReleaseIdentity(target.manifestPath);
  if (!identity || identity.status < 200 || identity.status >= 300
    || identity.finalUrl !== manifestUrl || String(identity.repoSha || '').toLowerCase() !== target.expectedRepoSha
    || identity.siteMode !== target.mode || String(identity.basePath || '') !== target.basePath) {
    const reason = identity?.repoSha && String(identity.repoSha).toLowerCase() !== target.expectedRepoSha
      ? 'TARGET_SHA_MISMATCH' : 'TARGET_IDENTITY_INVALID';
    throw blocked(reason);
  }

  const navigation = await adapter.openExact(target.url.href);
  if (!navigation || navigation.status < 200 || navigation.status >= 300) throw blocked('TARGET_HTTP_INVALID');
  if (navigation.finalUrl !== target.url.href) throw blocked('TARGET_REDIRECTED');
  const session = await adapter.restoredSession();
  if (!session || session.restored !== true || session.authenticated !== true || session.pathname !== target.searchPath) {
    throw blocked('SESSION_NOT_RESTORED');
  }
  const counters = await adapter.requestCounters();
  exactZeroCounters(counters);

  const salt = String(options.receiptSalt || target.expectedRepoSha);
  return Object.freeze({
    schema: 'static_site_auth_browser_acceptance.v1', outcome: 'PASS', terminal: true,
    target_mode: target.mode, target_origin: target.url.origin,
    target_path_hash: hash(target.searchPath, salt), candidate_identity_hash: hash(target.basePath || 'production', salt),
    expected_repo_sha: target.expectedRepoSha, observed_repo_sha: String(identity.repoSha).toLowerCase(),
    session_restored: true, get_user_verified: true, protected_probe_verified: true,
    product_otp_issue_count: 0, external_mail_send_count: 0, external_mail_receipt_count: 0,
    redaction_status: 'PASS',
  });
}
