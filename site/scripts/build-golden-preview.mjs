import { spawnSync } from 'node:child_process';
import {
  copyFileSync,
  existsSync,
  mkdirSync,
  openSync,
  closeSync,
  readFileSync,
  renameSync,
  rmSync,
  writeFileSync,
} from 'node:fs';
import { join, resolve } from 'node:path';
import {
  GOLDEN_CORPUS_PATH,
  loadGoldenCorpus,
  materializeGoldenPreviewData,
  sha256,
} from './golden-review-corpus.mjs';
import {
  applyGoldenActionFixtures,
  goldenActionContract,
} from './golden-review-actions.mjs';

const siteDir = resolve(new URL('..', import.meta.url).pathname);
const eventsPath = join(siteDir, 'src', 'data', 'preview-events.json');
const lockPath = join(siteDir, '.golden-preview-data.lock');

function gitFullSha() {
  const configured = String(process.env.STATIC_SITE_REPO_SHA || '').trim().toLowerCase();
  if (configured) {
    if (!/^[0-9a-f]{40}$/u.test(configured)) throw new Error('STATIC_SITE_REPO_SHA must be a full commit SHA');
    return configured;
  }
  const result = spawnSync('git', ['rev-parse', 'HEAD'], { cwd:siteDir, encoding:'utf8' });
  const value = result.status === 0 ? result.stdout.trim().toLowerCase() : '';
  if (!/^[0-9a-f]{40}$/u.test(value)) throw new Error('Cannot resolve the Golden preview source SHA');
  return value;
}

function safeGoldenBuildId(value) {
  if (!/^preview-golden-[a-zA-Z0-9._-]+$/u.test(value) || value.includes('/')) {
    throw new Error(`Invalid Golden preview build id: ${value || '(empty)'}`);
  }
  return value;
}

function atomicWrite(path, data) {
  const temporaryPath = `${path}.golden-${process.pid}.tmp`;
  try {
    writeFileSync(temporaryPath, data);
    renameSync(temporaryPath, path);
  } finally {
    rmSync(temporaryPath, { force:true });
  }
}

const { corpus, digest:corpusDigest } = loadGoldenCorpus();
const repoSha = gitFullSha();
const corpusVersion = String(corpus.corpus_id).match(/-v\d+$/u)?.[0]?.slice(1) || 'v1';
const buildId = safeGoldenBuildId(
  process.env.PREVIEW_BUILD_ID
    || `preview-golden-${repoSha.slice(0, 8)}-${corpus.frozen_clock.friday.replaceAll('-', '')}-${corpusVersion}`,
);

if (existsSync(lockPath)) {
  throw new Error(`Golden preview data lock already exists: ${lockPath}. Refusing concurrent source-data mutation.`);
}
const lockFd = openSync(lockPath, 'wx', 0o600);
closeSync(lockFd);

const originalRaw = readFileSync(eventsPath);
const originalDigest = sha256(originalRaw);
let basePreviewData;
try {
  basePreviewData = JSON.parse(originalRaw.toString('utf8'));
} catch (error) {
  rmSync(lockPath, { force:true });
  throw new Error(`Cannot parse the real preview data before Golden materialization: ${error?.message || error}`);
}
const materialized = applyGoldenActionFixtures(
  materializeGoldenPreviewData(corpus, basePreviewData),
  corpus,
);
const materializedRaw = Buffer.from(`${JSON.stringify(materialized, null, 2)}\n`, 'utf8');
const materializedDigest = sha256(materializedRaw);

let buildStatus = 1;
let buildError = null;
let restoreError = null;
try {
  atomicWrite(eventsPath, materializedRaw);
  const result = spawnSync(process.execPath, ['scripts/build-preview.mjs'], {
    cwd:siteDir,
    env:{
      ...process.env,
      PREVIEW_BUILD_ID:buildId,
      STATIC_SITE_REPO_SHA:repoSha,
      STATIC_SITE_CURRENT_DATE:corpus.frozen_clock.current_date,
      STATIC_SITE_CURRENT_DATETIME:corpus.frozen_clock.reference_iso,
      PUBLIC_SEARCH_COLLECTION_REFERENCE_DATE:corpus.frozen_clock.current_date,
      PREVIEW_DATA_MODE:'golden',
      GOLDEN_CORPUS_ID:corpus.corpus_id,
      GOLDEN_CORPUS_DIGEST:corpusDigest,
    },
    stdio:'inherit',
  });
  buildStatus = Number.isInteger(result.status) ? result.status : 1;
  buildError = result.error || null;
} finally {
  try {
    atomicWrite(eventsPath, originalRaw);
  } catch (error) {
    restoreError = error;
  } finally {
    rmSync(lockPath, { force:true });
  }
}

if (restoreError) {
  throw new Error(`Golden preview could not restore the real source data: ${restoreError?.message || restoreError}`);
}
const restoredDigest = sha256(readFileSync(eventsPath));
if (restoredDigest !== originalDigest) {
  throw new Error(`Golden preview source restoration failed: expected ${originalDigest}, received ${restoredDigest}`);
}
if (buildError) throw buildError;
if (buildStatus !== 0) {
  console.error(`Golden preview build failed with exit ${buildStatus}; real preview data restored byte-for-byte.`);
  process.exit(buildStatus);
}

const outputRoot = join(siteDir, 'dist', buildId);
if (!existsSync(outputRoot)) throw new Error(`Golden preview build did not create ${outputRoot}`);
copyFileSync(GOLDEN_CORPUS_PATH, join(outputRoot, 'golden-review-corpus.v1.json'));
mkdirSync(join(outputRoot, 'data', 'golden'), { recursive:true });
writeFileSync(join(outputRoot, 'data', 'golden', 'evidence.json'), JSON.stringify({
  schema_version:'kenigevents.golden-preview-evidence.v1',
  build_id:buildId,
  repo_sha:repoSha,
  corpus_id:corpus.corpus_id,
  corpus_sha256:corpusDigest,
  frozen_clock:corpus.frozen_clock,
  route_contract:corpus.route_contract,
  action_contract:goldenActionContract(corpus),
  pinned_assets:corpus.pinned_assets.map((asset) => ({
    id:asset.id,
    path:asset.path,
    kind:asset.kind,
    git_blob_sha:asset.git_blob_sha,
    ...(asset.sha256 ? { sha256:asset.sha256 } : {}),
  })),
  source_data_restore:{
    real_preview_events_sha256_before:originalDigest,
    real_preview_events_sha256_after:restoredDigest,
    materialized_golden_preview_events_sha256:materializedDigest,
    retained_real_canaries:materialized.events.length - corpus.events.length,
    golden_events:corpus.events.length,
  },
}, null, 2));

console.log(`Golden preview ready: dist/${buildId}/`);
console.log(`Golden preview URL: https://kenigevents.ru/${buildId}/__preview/`);
console.log(`Golden corpus: ${corpus.corpus_id} sha256=${corpusDigest}`);
console.log(`Real preview data restored: ${restoredDigest}`);
