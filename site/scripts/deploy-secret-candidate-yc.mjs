import { spawnSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  assertCandidateObjectKey, candidateBasePath, contentType, safeCandidateToken, sha256,
} from './release-contract.mjs';

const siteDir = dirname(dirname(fileURLToPath(import.meta.url)));

export function assertAnonymousListDisabled({ status, body = '' }) {
  if (status >= 200 && status < 300) throw new Error('Refusing secret-link publish: bucket anonymous ListObjects is enabled');
  if (![401, 403].includes(status) && !/AccessDenied|Forbidden/iu.test(body)) throw new Error(`Cannot prove anonymous bucket listing is disabled (HTTP ${status})`);
  return true;
}

export function publicationObjects(candidateRoot, manifest, token) {
  if (!manifest || manifest.site_mode !== 'secret_candidate' || manifest.publication_mode !== 'secret_link') throw new Error('Secret candidate manifest is invalid');
  for (const key of ['candidate_contract','catalog_parity','noindex','no_referrer','prefix_containment','root_isolation']) {
    if (manifest.checks?.[key] !== 'ok') throw new Error(`Refusing unchecked secret candidate: ${key}`);
  }
  const out = [
    ...manifest.files,
    { key: 'secret-candidate-manifest.json', content_type: 'application/json; charset=utf-8', cache_control: 'private, no-store, max-age=0' },
  ].map((file) => {
    const objectKey = `${candidateBasePath(token).slice(1)}/${file.key}`;
    assertCandidateObjectKey(objectKey, token);
    return {
      ...file,
      object_key: objectKey,
      local_path: join(candidateRoot, file.key),
      content_type: file.content_type || contentType(file.key),
      cache_control: 'private, no-store, max-age=0',
    };
  });
  if (new Set(out.map((item) => item.object_key)).size !== out.length) throw new Error('Duplicate candidate object keys');
  return out;
}

function parseArgs(argv) {
  const command = argv[0] || 'plan';
  if (!['plan', 'publish'].includes(command)) throw new Error('Usage: deploy-secret-candidate-yc.mjs plan|publish');
  return { command };
}

async function main() {
  const { command } = parseArgs(process.argv.slice(2));
  const token = safeCandidateToken(process.env.SECRET_CANDIDATE_TOKEN || '');
  const basePath = candidateBasePath(token);
  const candidateRoot = join(siteDir, 'dist', basePath.slice(1));
  const manifest = JSON.parse(readFileSync(join(candidateRoot, 'secret-candidate-manifest.json'), 'utf8'));
  if (manifest.token_sha256 !== sha256(token) || manifest.base_path !== basePath) throw new Error('Candidate token does not match checked artifact');
  const objects = publicationObjects(candidateRoot, manifest, token);
  const bucket = process.env.KENIGEVENTS_SITE_YC_BUCKET;
  const endpoint = process.env.KENIGEVENTS_SITE_YC_ENDPOINT || 'https://storage.yandexcloud.net';
  const publicBase = (process.env.KENIGEVENTS_SITE_PUBLIC_BASE_URL || 'https://kenigevents.ru').replace(/\/+$/u, '');
  if (!bucket) throw new Error('KENIGEVENTS_SITE_YC_BUCKET is required');

  const listProbeUrl = `${endpoint.replace(/\/+$/u, '')}/${encodeURIComponent(bucket)}?list-type=2&max-keys=1`;
  const probe = await fetch(listProbeUrl, { redirect: 'manual', headers: { 'cache-control': 'no-cache' } });
  const probeBody = await probe.text();
  assertAnonymousListDisabled({ status: probe.status, body: probeBody });
  const plan = {
    ok: true, command, publication_mode: 'secret_link', build_id: manifest.build_id,
    token_sha256: manifest.token_sha256, object_prefix: basePath.slice(1), object_count: objects.length,
    public_url: `${publicBase}${basePath}/`, anonymous_list_preflight: 'disabled',
    root_mutation: false, stable_ics_mutation: false, overwrite_allowed: false,
  };
  if (command === 'plan') { console.log(JSON.stringify(plan, null, 2)); return; }
  if (process.env.SECRET_CANDIDATE_CONFIRM !== `publish-secret:${manifest.build_id}:${manifest.token_sha256}`) throw new Error('Fail-closed secret candidate confirmation is missing');
  const accessKey = process.env.KENIGEVENTS_SITE_YC_ACCESS_KEY_ID;
  const secretKey = process.env.KENIGEVENTS_SITE_YC_SECRET_ACCESS_KEY;
  if (!accessKey || !secretKey) throw new Error('YC Object Storage credentials are required');
  const awsEnv = { ...process.env, AWS_ACCESS_KEY_ID: accessKey, AWS_SECRET_ACCESS_KEY: secretKey, AWS_DEFAULT_REGION: process.env.KENIGEVENTS_SITE_YC_REGION || 'ru-central1' };
  function aws(args, capture = false) {
    const result = spawnSync('aws', ['--endpoint-url', endpoint, ...args], { env: awsEnv, encoding: capture ? 'utf8' : undefined, stdio: capture ? 'pipe' : 'inherit', maxBuffer: 64 * 1024 * 1024 });
    if (result.status !== 0) throw new Error(`aws ${args[0]} failed: ${String(result.stderr || '').trim()}`);
    return result.stdout || '';
  }
  for (const item of objects) {
    const args = ['s3api','put-object','--bucket',bucket,'--key',item.object_key,'--body',item.local_path,'--if-none-match','*','--content-type',item.content_type,'--cache-control',item.cache_control];
    if (item.key.endsWith('.ics')) args.push('--content-disposition','inline; filename="event.ics"');
    aws(args);
  }
  for (const item of objects) {
    const url = `${publicBase}/${item.object_key}`;
    const response = await fetch(url, { redirect: 'follow', headers: { 'cache-control': 'no-cache' } });
    if (!response.ok) throw new Error(`Public candidate verification HTTP ${response.status}: ${url}`);
    const bytes = Buffer.from(await response.arrayBuffer());
    const expected = readFileSync(item.local_path);
    if (bytes.length !== expected.length || sha256(bytes) !== sha256(expected)) throw new Error(`Public candidate hash mismatch: ${item.key}`);
    const actualType = String(response.headers.get('content-type') || '').toLowerCase().replace(/\s+/gu, ' ').trim();
    if (actualType !== item.content_type) throw new Error(`Public candidate MIME mismatch ${item.key}: ${actualType}`);
  }
  console.log(JSON.stringify({ ...plan, command: 'publish', public_verification: 'ok' }, null, 2));
}

const invoked = process.argv[1] ? resolve(process.argv[1]) : '';
if (invoked === fileURLToPath(import.meta.url)) await main();
