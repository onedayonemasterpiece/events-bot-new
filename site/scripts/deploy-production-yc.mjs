import { createHash } from 'node:crypto';
import { spawnSync } from 'node:child_process';
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const MANIFEST_SCHEMA = 'static_release_manifest_v1';
const POINTER_SCHEMA = 'static_release_pointer_v1';
const MANIFEST_FILE = 'static-release-manifest.json';
const RELEASES_PREFIX = '_static/releases';
const CURRENT_POINTER_KEY = `${RELEASES_PREFIX}/current.json`;
const PREVIOUS_POINTER_KEY = `${RELEASES_PREFIX}/previous.json`;
const siteDir = dirname(dirname(fileURLToPath(import.meta.url)));
const repoRoot = resolve(siteDir, '..');
const distDir = join(siteDir, 'dist');

function loadDotEnv(path) {
  if (!existsSync(path)) return {};
  const parsed = {};
  for (const line of readFileSync(path, 'utf8').split(/\r?\n/u)) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#') || !trimmed.includes('=')) continue;
    const index = trimmed.indexOf('=');
    const key = trimmed.slice(0, index).trim();
    let value = trimmed.slice(index + 1).trim();
    if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) value = value.slice(1, -1);
    parsed[key] = value;
  }
  return parsed;
}

function parseArgs(argv) {
  const command = argv[0] || '';
  if (!['plan', 'publish', 'rollback'].includes(command)) throw new Error('Usage: deploy-production-yc.mjs plan|publish|rollback [--release-id production-...]');
  let releaseId = '';
  for (let index = 1; index < argv.length; index += 1) {
    if (argv[index] === '--release-id') releaseId = argv[++index] || '';
    else throw new Error(`Unknown argument: ${argv[index]}`);
  }
  return { command, releaseId };
}

function safeReleaseId(value) {
  if (!/^production-[a-zA-Z0-9][a-zA-Z0-9._-]*$/u.test(value || '') || value.includes('..') || value.includes('/')) throw new Error(`Invalid production release id: ${value || '(empty)'}`);
  return value;
}

function assertSafeManagedKey(key) {
  if (typeof key !== 'string' || !key || key.startsWith('/') || key.includes('\\') || key.split('/').some((part) => !part || part === '.' || part === '..')) throw new Error(`Unsafe managed root key: ${JSON.stringify(key)}`);
  if (/^(?:ics|__preview|lab)(?:\/|$)/u.test(key) || key.startsWith('_static/') || /^preview-[^/]+\//u.test(key)) throw new Error(`Protected key cannot be managed: ${key}`);
}

function validateManifest(manifest, expectedReleaseId = '', { allowDirty = false } = {}) {
  if (!manifest || manifest.schema_version !== MANIFEST_SCHEMA || manifest.site_mode !== 'production' || manifest.base_path !== '/') throw new Error(`Release manifest must use ${MANIFEST_SCHEMA} in production root mode`);
  safeReleaseId(manifest.build_id);
  if (expectedReleaseId && manifest.build_id !== expectedReleaseId) throw new Error(`Manifest build id ${manifest.build_id} does not match ${expectedReleaseId}`);
  if ((!allowDirty && manifest.git_dirty) || !/^[0-9a-f]{40}$/u.test(manifest.git_sha || '')) throw new Error('Production publisher refuses dirty or unversioned release metadata');
  if (!Array.isArray(manifest.files) || !Array.isArray(manifest.managed_root_keys) || !manifest.files.length) throw new Error('Manifest file/key lists are missing');
  const byKey = new Map();
  for (const file of manifest.files) {
    assertSafeManagedKey(file?.key);
    if (byKey.has(file.key) || !/^[0-9a-f]{64}$/u.test(file.sha256 || '') || !Number.isSafeInteger(file.size) || file.size < 0) throw new Error(`Invalid or duplicate file manifest entry: ${file?.key}`);
    byKey.set(file.key, file);
  }
  if (manifest.managed_root_keys.length !== byKey.size || new Set(manifest.managed_root_keys).size !== byKey.size) throw new Error('managed_root_keys must exactly match files[]');
  for (const key of manifest.managed_root_keys) {
    assertSafeManagedKey(key);
    if (!byKey.has(key)) throw new Error(`Managed key has no file metadata: ${key}`);
  }
  if (!Array.isArray(manifest.stable_ics)) throw new Error('stable_ics must be present');
  for (const item of manifest.stable_ics) {
    const source = byKey.get(item?.source_key);
    if (!source || source.sha256 !== item.sha256 || item.target_key !== `ics/${item.event_id}.ics`) throw new Error(`Invalid stable ICS mapping: ${JSON.stringify(item)}`);
  }
  return { manifest, byKey };
}

function encodeObjectPath(key) {
  return key.split('/').map((part) => encodeURIComponent(part)).join('/');
}

function sha256(bytes) {
  return createHash('sha256').update(bytes).digest('hex');
}

const args = parseArgs(process.argv.slice(2));
const fileEnv = loadDotEnv(join(repoRoot, '.env'));
const env = { ...fileEnv, ...process.env };

function checkLocalArtifact() {
  const check = spawnSync(process.execPath, [join(siteDir, 'scripts', 'check-production.mjs')], {
    cwd: siteDir,
    env,
    stdio: 'inherit',
  });
  if (check.status !== 0) throw new Error('Production artifact check failed; publishing is blocked');
  const manifest = JSON.parse(readFileSync(join(distDir, MANIFEST_FILE), 'utf8'));
  const allowDirtyPlan = args.command === 'plan' && ['1', 'true', 'yes', 'on'].includes(String(env.PRODUCTION_ALLOW_DIRTY || '').toLowerCase());
  return validateManifest(manifest, '', { allowDirty: allowDirtyPlan }).manifest;
}

const localManifest = args.command === 'rollback' ? null : checkLocalArtifact();
if (args.command === 'plan') {
  const counts = localManifest.files.reduce((out, file) => ({ ...out, [file.promotion_class]: (out[file.promotion_class] || 0) + 1 }), {});
  console.log(JSON.stringify({
    ok: true,
    command: 'plan',
    release_id: localManifest.build_id,
    git_sha: localManifest.git_sha,
    stage_prefix: `${RELEASES_PREFIX}/${localManifest.build_id}/root/`,
    release_manifest_key: `${RELEASES_PREFIX}/${localManifest.build_id}/${MANIFEST_FILE}`,
    managed_root_files: localManifest.files.length,
    stable_ics_updates: localManifest.stable_ics.length,
    promotion_order: ['immutable_asset', 'supporting', 'html', 'root_html'],
    deletion_policy: 'only stale keys from the prior manifest; never preview prefixes, _static/releases, /ics, or old _astro assets',
    counts,
  }, null, 2));
  process.exit(0);
}

const bucket = env.KENIGEVENTS_SITE_YC_BUCKET;
const endpoint = env.KENIGEVENTS_SITE_YC_ENDPOINT || 'https://storage.yandexcloud.net';
const region = env.KENIGEVENTS_SITE_YC_REGION || 'ru-central1';
const accessKey = env.KENIGEVENTS_SITE_YC_ACCESS_KEY_ID;
const secretKey = env.KENIGEVENTS_SITE_YC_SECRET_ACCESS_KEY;
if (!bucket || !accessKey || !secretKey) throw new Error('Missing KENIGEVENTS_SITE_YC_BUCKET / KENIGEVENTS_SITE_YC_ACCESS_KEY_ID / KENIGEVENTS_SITE_YC_SECRET_ACCESS_KEY');

const awsEnv = {
  ...process.env,
  AWS_ACCESS_KEY_ID: accessKey,
  AWS_SECRET_ACCESS_KEY: secretKey,
  AWS_DEFAULT_REGION: region,
};
const awsVersion = spawnSync('aws', ['--version'], { env: awsEnv, encoding: 'utf8' });
if (awsVersion.status !== 0) throw new Error('aws CLI is required for production publishing');

function runAws(argv, { capture = false, allowMissing = false } = {}) {
  const result = spawnSync('aws', ['--endpoint-url', endpoint, ...argv], {
    env: awsEnv,
    encoding: capture || allowMissing ? 'utf8' : undefined,
    stdio: capture || allowMissing ? 'pipe' : 'inherit',
    maxBuffer: 64 * 1024 * 1024,
  });
  if (result.status !== 0) {
    const output = `${result.stdout || ''}\n${result.stderr || ''}`;
    if (allowMissing && /(?:404|Not Found|NoSuchKey)/iu.test(output)) return null;
    throw new Error(`aws ${argv.join(' ')} failed (${result.status}): ${output.trim()}`);
  }
  return capture ? result.stdout : '';
}

function s3Uri(key) {
  return `s3://${bucket}/${key}`;
}

function readJsonObject(key, { missingOk = false } = {}) {
  const output = runAws(['s3', 'cp', s3Uri(key), '-', '--no-progress'], { capture: true, allowMissing: missingOk });
  if (output === null) return null;
  try {
    return JSON.parse(output);
  } catch (error) {
    throw new Error(`Object ${key} is not valid JSON: ${error.message}`);
  }
}

function objectExists(key) {
  const result = runAws(['s3api', 'head-object', '--bucket', bucket, '--key', key], { capture: true, allowMissing: true });
  return result !== null;
}

function releasePaths(releaseId) {
  return {
    rootPrefix: `${RELEASES_PREFIX}/${releaseId}/root`,
    manifestKey: `${RELEASES_PREFIX}/${releaseId}/${MANIFEST_FILE}`,
  };
}

function publicUrl(base, key) {
  return `${base.replace(/\/+$/u, '')}/${encodeObjectPath(key)}`;
}

async function verifyPublicFiles(baseUrl, prefix, manifest, label) {
  const queue = [...manifest.files];
  const concurrency = Math.max(1, Math.min(24, Number(env.KENIGEVENTS_SITE_VERIFY_CONCURRENCY || 8)));
  let checked = 0;
  async function worker() {
    while (queue.length) {
      const file = queue.shift();
      const key = prefix ? `${prefix}/${file.key}` : file.key;
      const url = publicUrl(baseUrl, key);
      const response = await fetch(url, { redirect: 'follow', headers: { 'cache-control': 'no-cache' } });
      if (!response.ok) throw new Error(`${label} public HTTP ${response.status} for ${url}`);
      const bytes = Buffer.from(await response.arrayBuffer());
      if (bytes.length !== file.size || sha256(bytes) !== file.sha256) throw new Error(`${label} public HTTP hash mismatch for ${url}`);
      checked += 1;
    }
  }
  await Promise.all(Array.from({ length: Math.min(concurrency, queue.length || 1) }, () => worker()));
  console.log(`${label} public HTTP verification: ${checked} files ok`);
}

async function verifyPublicObject(baseUrl, key, expectedBytes, label) {
  const url = publicUrl(baseUrl, key);
  const response = await fetch(url, { redirect: 'follow', headers: { 'cache-control': 'no-cache' } });
  if (!response.ok) throw new Error(`${label} public HTTP ${response.status} for ${url}`);
  const bytes = Buffer.from(await response.arrayBuffer());
  if (bytes.length !== expectedBytes.length || sha256(bytes) !== sha256(expectedBytes)) throw new Error(`${label} public HTTP hash mismatch for ${url}`);
  console.log(`${label} public HTTP verification: ok`);
}

function uploadLocalFile(localPath, key, file) {
  const argv = ['s3', 'cp', localPath, s3Uri(key), '--content-type', file.content_type, '--cache-control', file.cache_control, '--no-progress'];
  if (file.content_type.startsWith('text/calendar')) argv.push('--content-disposition', 'inline; filename="event.ics"');
  runAws(argv);
}

function copyObject(sourceKey, targetKey, file, replaceCalendarMetadata = false) {
  const argv = ['s3', 'cp', s3Uri(sourceKey), s3Uri(targetKey), '--no-progress'];
  if (replaceCalendarMetadata) {
    const eventId = /^(?:ics\/)(\d+)\.ics$/u.exec(targetKey)?.[1] || 'event';
    argv.push('--metadata-directive', 'REPLACE', '--content-type', 'text/calendar; charset=utf-8', '--content-disposition', `inline; filename="event-${eventId}.ics"`, '--cache-control', 'public, max-age=300, must-revalidate');
  } else {
    argv.push('--metadata-directive', 'COPY');
  }
  runAws(argv);
}

function promotionOrder(manifest) {
  const rank = { immutable_asset: 0, supporting: 1, html: 2, root_html: 3 };
  return [...manifest.files].sort((left, right) => (rank[left.promotion_class] - rank[right.promotion_class]) || left.key.localeCompare(right.key));
}

function promoteRelease(manifest) {
  const paths = releasePaths(manifest.build_id);
  for (const file of promotionOrder(manifest)) copyObject(`${paths.rootPrefix}/${file.key}`, file.key, file);
  for (const item of manifest.stable_ics) {
    const file = manifest.files.find((candidate) => candidate.key === item.source_key);
    copyObject(`${paths.rootPrefix}/${item.source_key}`, item.target_key, file, true);
  }
}

function deleteStaleManagedKeys(currentManifest, targetManifest) {
  if (!currentManifest) return 0;
  const nextKeys = new Set(targetManifest.managed_root_keys);
  let deleted = 0;
  for (const key of currentManifest.managed_root_keys) {
    assertSafeManagedKey(key);
    if (nextKeys.has(key) || key.startsWith('_astro/')) continue;
    runAws(['s3', 'rm', s3Uri(key), '--no-progress']);
    deleted += 1;
  }
  console.log(`Stale managed-root cleanup: ${deleted} keys; old _astro assets and all stable /ics keys retained`);
  return deleted;
}

function validPointer(pointer, label) {
  if (!pointer) return null;
  if (pointer.schema_version !== POINTER_SCHEMA || !pointer.release_id || !pointer.manifest_key) throw new Error(`${label} pointer is malformed`);
  safeReleaseId(pointer.release_id);
  return pointer;
}

function writePointer(key, pointer) {
  const dir = mkdtempSync(join(tmpdir(), 'kenigevents-release-pointer-'));
  try {
    const path = join(dir, 'pointer.json');
    writeFileSync(path, `${JSON.stringify(pointer, null, 2)}\n`);
    runAws(['s3', 'cp', path, s3Uri(key), '--content-type', 'application/json; charset=utf-8', '--cache-control', 'no-store', '--no-progress']);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
}

const websiteBase = (env.KENIGEVENTS_SITE_ORIGIN_PUBLIC_BASE_URL || `http://${bucket}.website.yandexcloud.net`).replace(/\/+$/u, '');
const stagePublicBase = (env.KENIGEVENTS_SITE_STAGE_PUBLIC_BASE_URL || websiteBase).replace(/\/+$/u, '');
const rootPublicBase = (env.KENIGEVENTS_SITE_ROOT_PUBLIC_BASE_URL || websiteBase).replace(/\/+$/u, '');
const currentPointer = validPointer(readJsonObject(CURRENT_POINTER_KEY, { missingOk: true }), 'current');
const currentManifest = currentPointer ? validateManifest(readJsonObject(currentPointer.manifest_key), currentPointer.release_id).manifest : null;

async function publish() {
  const manifest = localManifest;
  const confirm = `publish:${manifest.build_id}`;
  if (env.KENIGEVENTS_SITE_PRODUCTION_CONFIRM !== confirm) throw new Error(`Fail-closed confirmation missing: set KENIGEVENTS_SITE_PRODUCTION_CONFIRM=${confirm}`);
  const paths = releasePaths(manifest.build_id);
  if (objectExists(paths.manifestKey)) throw new Error(`Immutable release already exists: ${paths.manifestKey}`);

  console.log(`Staging immutable release ${manifest.build_id} to ${s3Uri(`${paths.rootPrefix}/`)}`);
  for (const file of manifest.files) uploadLocalFile(join(distDir, file.key), `${paths.rootPrefix}/${file.key}`, file);
  uploadLocalFile(join(distDir, MANIFEST_FILE), paths.manifestKey, {
    content_type: 'application/json; charset=utf-8',
    cache_control: 'public, max-age=31536000, immutable',
  });
  await verifyPublicObject(stagePublicBase, paths.manifestKey, readFileSync(join(distDir, MANIFEST_FILE)), 'Staged release manifest');
  await verifyPublicFiles(stagePublicBase, paths.rootPrefix, manifest, 'Staged release');

  promoteRelease(manifest);
  await verifyPublicFiles(rootPublicBase, '', manifest, 'Promoted root');
  deleteStaleManagedKeys(currentManifest, manifest);

  const publishedAt = new Date().toISOString();
  const previous = currentPointer || { schema_version: POINTER_SCHEMA, release_id: null, manifest_key: null, recorded_at: publishedAt };
  writePointer(PREVIOUS_POINTER_KEY, previous);
  writePointer(CURRENT_POINTER_KEY, {
    schema_version: POINTER_SCHEMA,
    release_id: manifest.build_id,
    manifest_key: paths.manifestKey,
    git_sha: manifest.git_sha,
    published_at: publishedAt,
    operation: 'publish',
    previous_release_id: currentPointer?.release_id || null,
  });
  console.log(`Production release published: ${manifest.build_id}`);
}

async function rollback() {
  const releaseId = safeReleaseId(args.releaseId || env.PRODUCTION_ROLLBACK_RELEASE_ID || '');
  const confirm = `rollback:${releaseId}`;
  if (env.KENIGEVENTS_SITE_PRODUCTION_CONFIRM !== confirm) throw new Error(`Fail-closed confirmation missing: set KENIGEVENTS_SITE_PRODUCTION_CONFIRM=${confirm}`);
  if (!currentPointer) throw new Error('Cannot rollback without a current release pointer');
  if (currentPointer.release_id === releaseId) throw new Error(`${releaseId} is already current`);
  const paths = releasePaths(releaseId);
  const targetManifest = validateManifest(readJsonObject(paths.manifestKey), releaseId).manifest;
  await verifyPublicFiles(stagePublicBase, paths.rootPrefix, targetManifest, 'Rollback staged release');
  promoteRelease(targetManifest);
  await verifyPublicFiles(rootPublicBase, '', targetManifest, 'Rolled-back root');
  deleteStaleManagedKeys(currentManifest, targetManifest);

  const rolledBackAt = new Date().toISOString();
  writePointer(PREVIOUS_POINTER_KEY, currentPointer);
  writePointer(CURRENT_POINTER_KEY, {
    schema_version: POINTER_SCHEMA,
    release_id: targetManifest.build_id,
    manifest_key: paths.manifestKey,
    git_sha: targetManifest.git_sha,
    published_at: rolledBackAt,
    operation: 'rollback',
    previous_release_id: currentPointer.release_id,
  });
  console.log(`Production rollback complete: ${currentPointer.release_id} -> ${releaseId}`);
}

if (args.command === 'publish') await publish();
else await rollback();
