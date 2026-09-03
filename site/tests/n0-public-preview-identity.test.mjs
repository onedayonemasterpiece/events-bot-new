import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';
import {
  PUBLIC_PREVIEW_IDENTITY_FIELDS,
  checkPublicPreviewIdentity,
  comparePublicPreviewManifest,
  normalizePublicBaseUrl,
  publicPreviewUrls,
  requireFullRepoSha,
  requireSafeBuildId,
  validateLocalPreviewManifest,
} from '../scripts/check-public-preview-identity.mjs';

const BUILD_ID = 'preview-ui-normalized-d5ff87bc-fresh-20260903-v1';
const REPO_SHA = 'd5ff87bcb7a2b2051ad956ef9e7e2733a1ae62c3';
const manifest = Object.freeze({
  buildId: BUILD_ID,
  repo_sha: REPO_SHA,
  generatedAt: '2026-09-03T08:42:38.000Z',
  basePath: `/${BUILD_ID}`,
  astroAssetBaseUrl: null,
  authorizedSearchConfigured: true,
  currentDate: '2026-09-03',
  referenceIso: '2026-09-03T08:42:38.000Z',
  transportFaultProfile: 'none',
  transportFaultRegistryDigest: 'f'.repeat(64),
});

const response = ({ status = 200, contentType, json }) => ({
  ok: status >= 200 && status < 300,
  status,
  statusText: status === 200 ? 'OK' : 'Failure',
  headers: { get: (name) => name.toLowerCase() === 'content-type' ? contentType : null },
  json: async () => json,
});

test('N0 acceptance requires an exact safe build id and full commit SHA', () => {
  assert.equal(requireSafeBuildId(BUILD_ID), BUILD_ID);
  assert.equal(requireFullRepoSha(REPO_SHA.toUpperCase()), REPO_SHA);
  for (const invalid of ['', 'ui-normalized', 'preview-a/b', '../preview-a', 'preview-']) {
    assert.throws(() => requireSafeBuildId(invalid), /exact safe preview id/u);
  }
  for (const invalid of ['', 'd5ff87bc', 'g'.repeat(40)]) {
    assert.throws(() => requireFullRepoSha(invalid), /full lowercase commit SHA/u);
  }
});

test('public URL construction is immutable-prefix-only', () => {
  assert.equal(normalizePublicBaseUrl('https://kenigevents.ru///'), 'https://kenigevents.ru');
  assert.deepEqual(publicPreviewUrls('https://kenigevents.ru/', BUILD_ID), {
    ownerUrl: `https://kenigevents.ru/${BUILD_ID}/__preview/`,
    manifestUrl: `https://kenigevents.ru/${BUILD_ID}/preview-build.json`,
  });
  assert.throws(() => normalizePublicBaseUrl('s3://bucket'), /HTTP\(S\)/u);
  assert.throws(() => normalizePublicBaseUrl('https://user:secret@example.test'), /credentials/u);
});

test('local preview identity must contain the complete acceptance field set', () => {
  assert.deepEqual(validateLocalPreviewManifest(manifest, {
    buildId: BUILD_ID,
    repoSha: REPO_SHA,
    currentDate: '2026-09-03',
    referenceIso: '2026-09-03T08:42:38.000Z',
  }), { buildId: BUILD_ID, repoSha: REPO_SHA });
  assert.deepEqual(
    PUBLIC_PREVIEW_IDENTITY_FIELDS.filter((field) => !(field in manifest)),
    [],
  );
});

test('local preview identity rejects SHA, build, base-path and reference drift', () => {
  assert.throws(
    () => validateLocalPreviewManifest({ ...manifest, repo_sha: 'a'.repeat(40) }, { buildId: BUILD_ID, repoSha: REPO_SHA }),
    /repo_sha mismatch/u,
  );
  assert.throws(
    () => validateLocalPreviewManifest({ ...manifest, basePath: '/preview-other' }, { buildId: BUILD_ID, repoSha: REPO_SHA }),
    /basePath mismatch/u,
  );
  assert.throws(
    () => validateLocalPreviewManifest(manifest, { buildId: BUILD_ID, repoSha: REPO_SHA, currentDate: '2026-09-04' }),
    /currentDate mismatch/u,
  );
  assert.throws(
    () => validateLocalPreviewManifest(manifest, { buildId: BUILD_ID, repoSha: REPO_SHA, referenceIso: '2026-09-03T09:00:00.000Z' }),
    /referenceIso mismatch/u,
  );
});

test('public preview-build.json must be byte-equivalent by identity fields', () => {
  assert.equal(comparePublicPreviewManifest(manifest, structuredClone(manifest)), true);
  assert.throws(
    () => comparePublicPreviewManifest(manifest, { ...manifest, authorizedSearchConfigured: false }),
    /authorizedSearchConfigured/u,
  );
  assert.throws(
    () => comparePublicPreviewManifest(manifest, { ...manifest, transportFaultRegistryDigest: '0'.repeat(64) }),
    /transportFaultRegistryDigest/u,
  );
});

test('network gate checks owner HTML before GET-verifying the public manifest', async () => {
  const calls = [];
  const result = await checkPublicPreviewIdentity({
    buildId: BUILD_ID,
    repoSha: REPO_SHA,
    publicBaseUrl: 'https://kenigevents.ru',
    localManifest: manifest,
    signal: {},
    fetchImpl: async (url, init) => {
      calls.push({ url, method: init.method, cache: init.cache });
      if (url.endsWith('/__preview/')) {
        return response({ contentType: 'text/html; charset=utf-8' });
      }
      return response({ contentType: 'application/json; charset=utf-8', json: structuredClone(manifest) });
    },
  });
  assert.equal(result.ok, true);
  assert.equal(result.buildId, BUILD_ID);
  assert.equal(result.repoSha, REPO_SHA);
  assert.deepEqual(calls, [
    {
      url: `https://kenigevents.ru/${BUILD_ID}/__preview/`,
      method: 'HEAD',
      cache: 'no-store',
    },
    {
      url: `https://kenigevents.ru/${BUILD_ID}/preview-build.json`,
      method: 'GET',
      cache: 'no-store',
    },
  ]);
});

test('network gate fails closed on unreachable HTML, MIME drift or public identity drift', async () => {
  await assert.rejects(
    checkPublicPreviewIdentity({
      buildId: BUILD_ID,
      repoSha: REPO_SHA,
      localManifest: manifest,
      signal: {},
      fetchImpl: async () => response({ status: 404, contentType: 'text/html' }),
    }),
    /returned 404/u,
  );
  await assert.rejects(
    checkPublicPreviewIdentity({
      buildId: BUILD_ID,
      repoSha: REPO_SHA,
      localManifest: manifest,
      signal: {},
      fetchImpl: async (url) => url.endsWith('/__preview/')
        ? response({ contentType: 'text/plain' })
        : response({ contentType: 'application/json', json: manifest }),
    }),
    /expected Content-Type text\/html/u,
  );
  await assert.rejects(
    checkPublicPreviewIdentity({
      buildId: BUILD_ID,
      repoSha: REPO_SHA,
      localManifest: manifest,
      signal: {},
      fetchImpl: async (url) => url.endsWith('/__preview/')
        ? response({ contentType: 'text/html' })
        : response({ contentType: 'application/json', json: { ...manifest, repo_sha: 'a'.repeat(40) } }),
    }),
    /Public preview identity mismatch/u,
  );
});

test('build identity remains full-SHA and N0 publication policy is Kaggle-only', async () => {
  const [buildSource, packageSource, acceptanceSource] = await Promise.all([
    readFile(new URL('../scripts/build-preview.mjs', import.meta.url), 'utf8'),
    readFile(new URL('../package.json', import.meta.url), 'utf8'),
    readFile(new URL('../scripts/n0-successor-acceptance.v1.json', import.meta.url), 'utf8'),
  ]);
  assert.match(buildSource, /STATIC_SITE_REPO_SHA/u);
  assert.match(buildSource, /repo_sha:\s*gitFullSha\(\)/u);
  assert.match(buildSource, /preview-build\.json/u);
  assert.match(buildSource, /SITE_BASE_PATH:\s*`\/\$\{buildId\}`/u);

  const packageJson = JSON.parse(packageSource);
  assert.equal(packageJson.scripts['deploy:preview'], undefined);
  assert.equal(packageJson.scripts['deploy:golden-preview'], undefined);

  const acceptance = JSON.parse(acceptanceSource);
  assert.ok(acceptance.next_real_gate.required.includes('the canonical events-bot-new Kaggle StaticSiteBuilder'));
  assert.ok(acceptance.next_real_gate.required.includes('--preview-data-mode real'));
  assert.ok(acceptance.next_real_gate.required.includes('--page-class all'));
  assert.ok(acceptance.prohibitions.includes('do not use deploy:preview or deploy:golden-preview as a launch path'));
});
