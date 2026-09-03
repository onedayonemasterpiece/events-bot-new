import { readFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const SAFE_BUILD_ID = /^preview-[a-z0-9][a-z0-9._-]*$/u;
const FULL_SHA = /^[0-9a-f]{40}$/u;

export const PUBLIC_PREVIEW_IDENTITY_FIELDS = Object.freeze([
  'buildId',
  'repo_sha',
  'basePath',
  'astroAssetBaseUrl',
  'authorizedSearchConfigured',
  'currentDate',
  'referenceIso',
  'transportFaultProfile',
  'transportFaultRegistryDigest',
]);

function nonEmpty(value) {
  return String(value ?? '').trim();
}

export function requireSafeBuildId(value) {
  const buildId = nonEmpty(value);
  if (!SAFE_BUILD_ID.test(buildId) || buildId.includes('/')) {
    throw new Error(`PREVIEW_BUILD_ID must be an exact safe preview id, got ${buildId || '(empty)'}`);
  }
  return buildId;
}

export function requireFullRepoSha(value) {
  const repoSha = nonEmpty(value).toLowerCase();
  if (!FULL_SHA.test(repoSha)) {
    throw new Error(`STATIC_SITE_REPO_SHA must be a full lowercase commit SHA, got ${repoSha || '(empty)'}`);
  }
  return repoSha;
}

export function normalizePublicBaseUrl(value) {
  const raw = nonEmpty(value) || 'https://kenigevents.ru';
  const url = new URL(raw);
  if (!['http:', 'https:'].includes(url.protocol)) {
    throw new Error(`Public preview base must use HTTP(S), got ${url.protocol}`);
  }
  if (url.username || url.password || url.search || url.hash) {
    throw new Error('Public preview base must not contain credentials, query or fragment');
  }
  url.pathname = url.pathname.replace(/\/+$/u, '');
  return url.href.replace(/\/+$/u, '');
}

export function publicPreviewUrls(publicBaseUrl, buildIdValue) {
  const buildId = requireSafeBuildId(buildIdValue);
  const base = normalizePublicBaseUrl(publicBaseUrl);
  return {
    ownerUrl: `${base}/${buildId}/__preview/`,
    manifestUrl: `${base}/${buildId}/preview-build.json`,
  };
}

function assertIsoDate(value, field) {
  const text = nonEmpty(value);
  if (!text || Number.isNaN(Date.parse(text))) {
    throw new Error(`${field} must be a valid ISO date/time`);
  }
  return text;
}

function assertCurrentDate(value) {
  const text = nonEmpty(value);
  if (!/^\d{4}-\d{2}-\d{2}$/u.test(text)) {
    throw new Error('currentDate must be YYYY-MM-DD');
  }
  return text;
}

export function validateLocalPreviewManifest(manifest, expected = {}) {
  if (!manifest || typeof manifest !== 'object' || Array.isArray(manifest)) {
    throw new Error('Local preview-build.json must contain an object');
  }
  const buildId = requireSafeBuildId(expected.buildId ?? manifest.buildId);
  const repoSha = requireFullRepoSha(expected.repoSha ?? manifest.repo_sha);
  if (manifest.buildId !== buildId) {
    throw new Error(`Local manifest buildId mismatch: expected ${buildId}, got ${manifest.buildId}`);
  }
  if (String(manifest.repo_sha || '').toLowerCase() !== repoSha) {
    throw new Error(`Local manifest repo_sha mismatch: expected ${repoSha}, got ${manifest.repo_sha || '(empty)'}`);
  }
  if (manifest.basePath !== `/${buildId}`) {
    throw new Error(`Local manifest basePath mismatch: expected /${buildId}, got ${manifest.basePath || '(empty)'}`);
  }
  assertIsoDate(manifest.generatedAt, 'generatedAt');
  assertCurrentDate(manifest.currentDate);
  assertIsoDate(manifest.referenceIso, 'referenceIso');
  if (expected.currentDate && manifest.currentDate !== expected.currentDate) {
    throw new Error(`Local manifest currentDate mismatch: expected ${expected.currentDate}, got ${manifest.currentDate}`);
  }
  if (expected.referenceIso && manifest.referenceIso !== expected.referenceIso) {
    throw new Error(`Local manifest referenceIso mismatch: expected ${expected.referenceIso}, got ${manifest.referenceIso}`);
  }
  for (const field of PUBLIC_PREVIEW_IDENTITY_FIELDS) {
    if (!(field in manifest)) throw new Error(`Local manifest misses identity field ${field}`);
  }
  return { buildId, repoSha };
}

function sameJsonValue(left, right) {
  return JSON.stringify(left) === JSON.stringify(right);
}

export function comparePublicPreviewManifest(localManifest, publicManifest) {
  if (!publicManifest || typeof publicManifest !== 'object' || Array.isArray(publicManifest)) {
    throw new Error('Public preview-build.json must contain an object');
  }
  const mismatches = [];
  for (const field of PUBLIC_PREVIEW_IDENTITY_FIELDS) {
    if (!sameJsonValue(publicManifest[field], localManifest[field])) {
      mismatches.push(`${field}: local=${JSON.stringify(localManifest[field])} public=${JSON.stringify(publicManifest[field])}`);
    }
  }
  if (mismatches.length) {
    throw new Error(`Public preview identity mismatch:\n- ${mismatches.join('\n- ')}`);
  }
  return true;
}

function assertResponse(response, label, expectedContentType) {
  if (!response || typeof response.ok !== 'boolean') {
    throw new Error(`${label} did not return a Response-like value`);
  }
  if (!response.ok) {
    throw new Error(`${label} returned ${response.status || 0} ${response.statusText || ''}`.trim());
  }
  const contentType = String(response.headers?.get?.('content-type') || '').toLowerCase();
  if (expectedContentType && !contentType.startsWith(expectedContentType)) {
    throw new Error(`${label} expected Content-Type ${expectedContentType}, got ${contentType || '(missing)'}`);
  }
}

export async function checkPublicPreviewIdentity(options = {}) {
  const env = options.env || process.env;
  const buildId = requireSafeBuildId(options.buildId ?? env.PREVIEW_BUILD_ID);
  const repoSha = requireFullRepoSha(options.repoSha ?? env.STATIC_SITE_REPO_SHA);
  const publicBaseUrl = normalizePublicBaseUrl(
    options.publicBaseUrl ?? env.KENIGEVENTS_SITE_PUBLIC_BASE_URL ?? 'https://kenigevents.ru',
  );
  const localManifestPath = resolve(
    options.localManifestPath
      ?? env.LOCAL_PREVIEW_BUILD_JSON
      ?? resolve(fileURLToPath(new URL('..', import.meta.url)), 'dist', buildId, 'preview-build.json'),
  );
  const localManifest = options.localManifest
    ?? JSON.parse(await readFile(localManifestPath, 'utf8'));
  validateLocalPreviewManifest(localManifest, {
    buildId,
    repoSha,
    currentDate: options.currentDate ?? env.EXPECTED_PREVIEW_CURRENT_DATE,
    referenceIso: options.referenceIso ?? env.EXPECTED_PREVIEW_REFERENCE_ISO,
  });

  const fetchImpl = options.fetchImpl || globalThis.fetch;
  if (typeof fetchImpl !== 'function') throw new Error('Global fetch is unavailable');
  const { ownerUrl, manifestUrl } = publicPreviewUrls(publicBaseUrl, buildId);
  const signal = options.signal || AbortSignal.timeout(Number(options.timeoutMs || env.PUBLIC_PREVIEW_VERIFY_TIMEOUT_MS || 20_000));

  const ownerResponse = await fetchImpl(ownerUrl, {
    method: 'HEAD',
    redirect: 'follow',
    cache: 'no-store',
    signal,
  });
  assertResponse(ownerResponse, 'Public owner preview', 'text/html');

  const manifestResponse = await fetchImpl(manifestUrl, {
    method: 'GET',
    redirect: 'follow',
    cache: 'no-store',
    headers: { accept: 'application/json' },
    signal,
  });
  assertResponse(manifestResponse, 'Public preview-build.json', 'application/json');
  const publicManifest = await manifestResponse.json();
  comparePublicPreviewManifest(localManifest, publicManifest);

  return {
    ok: true,
    ownerUrl,
    manifestUrl,
    buildId,
    repoSha,
    currentDate: localManifest.currentDate,
    referenceIso: localManifest.referenceIso,
    generatedAt: localManifest.generatedAt,
  };
}

async function main() {
  const result = await checkPublicPreviewIdentity();
  process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
}

const invokedDirectly = process.argv[1]
  && pathToFileURL(resolve(process.argv[1])).href === import.meta.url;
if (invokedDirectly) {
  main().catch((error) => {
    console.error(error?.stack || error);
    process.exitCode = 1;
  });
}
